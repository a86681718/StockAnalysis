#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raw-only broker microstructure research.

Inputs are restricted to:
- data/ohlc/*.csv
- data/bs_report/parquet_twse/*.parquet
- data/bs_report/parquet_tpex/*.parquet

The script intentionally does not read data/_derived or prior strategy outputs.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_OHLC_DIR = PROJECT_ROOT / "data" / "ohlc"
RAW_BS_DIRS = [
    PROJECT_ROOT / "data" / "bs_report" / "parquet_twse",
    PROJECT_ROOT / "data" / "bs_report" / "parquet_tpex",
]
OUT_DIR = PROJECT_ROOT / "outputs" / "analysis" / "raw_broker_microstructure"

FAMILY_NOTES = {
    "diffuse_buyer_accumulation": {
        "hypothesis": "Broad branch buying with no dominant single buyer is more durable than a one-branch spike.",
        "signal": "High buyer breadth, high positive buy/sell balance, capped top-buyer share, and non-overheated price/volume.",
        "trade": "Enter next open after the signal; hold by the selected hold_days with cooldown by symbol.",
    },
    "role_reversal": {
        "hypothesis": "Branches that were previously net sellers but abruptly turn into net buyers can mark informed accumulation.",
        "signal": "Broker-level rolling net buying turns strongly positive versus its prior 40-day net-selling baseline.",
        "trade": "Enter next open after a high cross-sectional role-reversal score; hold by the selected hold_days with cooldown by symbol.",
    },
    "sell_pressure_absorption": {
        "hypothesis": "Heavy sell pressure absorbed by broad buyers while price stays firm can precede rebound or continuation.",
        "signal": "High sell-pressure percentile, broad buyer participation, positive buy/sell balance, and non-negative short-term price action.",
        "trade": "Enter next open after the absorption signal; hold by the selected hold_days with cooldown by symbol.",
    },
}

LEGACY_BASELINE_PARAMS = {
    "diffuse_buyer_accumulation": {
        "hold_days": 40,
        "stop_loss": None,
        "cooldown_days": 10,
        "params": {
            "balance_pct": 0.98,
            "buyer_count_pct": 0.98,
            "ret5_cap": 0.15,
            "sell_pressure_cap": 1.0,
            "top_buyer_share_cap": 0.25,
            "volume_cap": 1.5,
        },
    },
    "role_reversal": {
        "hold_days": 40,
        "stop_loss": None,
        "cooldown_days": 10,
        "params": {
            "min_count": 3,
            "ret5_cap": 10.0,
            "score_pct": 0.99,
            "volume_cap": 3.0,
        },
    },
    "sell_pressure_absorption": {
        "hold_days": 40,
        "stop_loss": None,
        "cooldown_days": 10,
        "params": {
            "balance_min": 0.6,
            "buyer_count_pct": 0.9,
            "ret3_floor": 0.0,
            "sell_pressure_pct": 0.98,
            "volume_cap": 3.0,
        },
    },
}


@dataclass(frozen=True)
class CostConfig:
    fee_rate: float = 0.001425
    fee_discount: float = 0.28
    tax_rate_sell: float = 0.003
    slippage: float = 0.0005


def _clean_number(value: Any) -> float:
    if pd.isna(value):
        return np.nan
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if value in {"", "--", "----"}:
            return np.nan
    return float(value)


def _date_from_ohlc_path(path: Path) -> pd.Timestamp:
    # File names are twse-YYYYMMDD.csv / tpex-YYYYMMDD.csv.
    return pd.Timestamp(path.stem.split("-", 1)[1])


def latest_raw_ohlc_date() -> str:
    dates = [_date_from_ohlc_path(path) for path in RAW_OHLC_DIR.glob("*.csv") if "-" in path.stem]
    if not dates:
        raise RuntimeError("no raw OHLC files found")
    return max(dates).strftime("%Y-%m-%d")


def load_raw_ohlc(start: str, end: str, exclude_etf: bool) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    for path in sorted(RAW_OHLC_DIR.glob("*.csv")):
        date = _date_from_ohlc_path(path)
        if date < start_ts or date > end_ts:
            continue
        market = path.stem.split("-", 1)[0]
        df = pd.read_csv(path, dtype=str)
        if market == "twse":
            rename = {
                "證券代號": "symbol",
                "開盤價": "open",
                "最高價": "high",
                "最低價": "low",
                "收盤價": "close",
                "成交股數": "volume",
            }
        else:
            rename = {
                "代號": "symbol",
                "開盤": "open",
                "最高": "high",
                "最低": "low",
                "收盤": "close",
                "成交股數": "volume",
            }
        if not set(rename).issubset(df.columns):
            continue
        df = df[list(rename)].rename(columns=rename)
        df["date"] = date
        df["market"] = market
        rows.append(df)
    if not rows:
        raise RuntimeError("no raw OHLC rows loaded")
    out = pd.concat(rows, ignore_index=True)
    out["symbol"] = out["symbol"].astype(str).str.strip()
    out = out[out["symbol"].str.len() == 4].copy()
    if exclude_etf:
        out = out[~out["symbol"].str.startswith("00")].copy()
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = out[col].map(_clean_number)
    out = out.dropna(subset=["open", "high", "low", "close"])
    out["volume"] = out["volume"].fillna(0).astype(float)
    return out.sort_values(["symbol", "date"]).reset_index(drop=True)


def build_ohlc_maps(ohlc: pd.DataFrame) -> dict[str, dict[str, Any]]:
    maps: dict[str, dict[str, Any]] = {}
    for symbol, sub in ohlc.groupby("symbol"):
        sub = sub.sort_values("date").reset_index(drop=True)
        dates = pd.DatetimeIndex(sub["date"])
        maps[str(symbol)] = {
            "dates": dates.to_numpy(dtype="datetime64[ns]"),
            "open": sub["open"].to_numpy(float),
            "high": sub["high"].to_numpy(float),
            "low": sub["low"].to_numpy(float),
            "close": sub["close"].to_numpy(float),
            "date_to_idx": {pd.Timestamp(d): int(i) for i, d in enumerate(dates)},
        }
    return maps


def _bs_files(symbols: set[str] | None, max_symbols: int) -> list[Path]:
    files: list[Path] = []
    for directory in RAW_BS_DIRS:
        for path in sorted(directory.glob("????.parquet")):
            if symbols is not None and path.stem not in symbols:
                continue
            files.append(path)
    if max_symbols > 0:
        files = files[:max_symbols]
    return files


def _rolling_pct_by_date(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        df[f"{col}_pct_cs"] = df.groupby("date")[col].rank(pct=True)
    return df


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    return (num / den.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


def build_symbol_micro_features(path: Path, ohlc_symbol: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    symbol = path.stem
    raw = pd.read_parquet(path, columns=["日期", "券商", "買進股數", "賣出股數"])
    if raw.empty:
        return pd.DataFrame()
    raw = raw.rename(columns={"日期": "date", "券商": "broker", "買進股數": "buy", "賣出股數": "sell"})
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw = raw[(raw["date"] >= pd.Timestamp(start)) & (raw["date"] <= pd.Timestamp(end))].copy()
    raw["broker"] = raw["broker"].astype(str).str.strip()
    raw = raw[(raw["broker"] != "") & raw["date"].notna()].copy()
    if raw.empty:
        return pd.DataFrame()
    raw["buy"] = pd.to_numeric(raw["buy"], errors="coerce").fillna(0.0)
    raw["sell"] = pd.to_numeric(raw["sell"], errors="coerce").fillna(0.0)
    broker_day = raw.groupby(["date", "broker"], as_index=False).agg(buy=("buy", "sum"), sell=("sell", "sum"))
    broker_day["net"] = broker_day["buy"] - broker_day["sell"]

    broker_day = broker_day.sort_values(["broker", "date"]).reset_index(drop=True)
    g = broker_day.groupby("broker", group_keys=False)
    broker_day["net_3"] = g["net"].transform(lambda s: s.rolling(3, min_periods=1).sum())
    broker_day["net_5"] = g["net"].transform(lambda s: s.rolling(5, min_periods=2).sum())
    broker_day["prior_mean_40"] = g["net"].transform(lambda s: s.shift(1).rolling(40, min_periods=8).mean())
    broker_day["prior_std_40"] = g["net"].transform(lambda s: s.shift(1).rolling(40, min_periods=8).std())
    broker_day["z_3"] = (broker_day["net_3"] - broker_day["prior_mean_40"] * 3.0) / (
        broker_day["prior_std_40"].replace(0, np.nan) * math.sqrt(3.0)
    )

    pos = broker_day[broker_day["net"] > 0].copy()
    neg = broker_day[broker_day["net"] < 0].copy()

    daily = broker_day.groupby("date", as_index=False).agg(
        total_buy=("buy", "sum"),
        total_sell=("sell", "sum"),
        net_total=("net", "sum"),
        broker_count=("broker", "nunique"),
    )
    pos_agg = pos.groupby("date", as_index=False).agg(
        posnet_total=("net", "sum"),
        buyer_count=("broker", "nunique"),
        top_buyer_net=("net", "max"),
    )
    neg_agg = neg.assign(abs_net=-neg["net"]).groupby("date", as_index=False).agg(
        negnet_total=("abs_net", "sum"),
        seller_count=("broker", "nunique"),
        top_seller_abs=("abs_net", "max"),
    )

    role = broker_day[
        (broker_day["net_5"] > 0)
        & (broker_day["prior_mean_40"] < 0)
        & (broker_day["net_5"] > broker_day["prior_mean_40"].abs() * 5.0)
    ].copy()
    role_agg = role.groupby("date", as_index=False).agg(
        role_reversal_score=("net_5", "sum"),
        role_reversal_count=("broker", "nunique"),
    )

    coord = broker_day[(broker_day["net_3"] > 0) & (broker_day["z_3"] >= 2.0)].copy()
    coord_agg = coord.groupby("date", as_index=False).agg(
        coordinated_score=("z_3", "sum"),
        coordinated_count=("broker", "nunique"),
        coordinated_net=("net_3", "sum"),
        coordinated_top_net=("net_3", "max"),
    )

    out = daily.merge(pos_agg, on="date", how="left").merge(neg_agg, on="date", how="left")
    out = out.merge(role_agg, on="date", how="left").merge(coord_agg, on="date", how="left")
    fill_zero = [
        "posnet_total",
        "buyer_count",
        "top_buyer_net",
        "negnet_total",
        "seller_count",
        "top_seller_abs",
        "role_reversal_score",
        "role_reversal_count",
        "coordinated_score",
        "coordinated_count",
        "coordinated_net",
        "coordinated_top_net",
    ]
    out[fill_zero] = out[fill_zero].fillna(0.0)
    out["symbol"] = symbol
    out["top_buyer_share"] = _safe_div(out["top_buyer_net"], out["posnet_total"]).fillna(0.0)
    out["top_seller_share"] = _safe_div(out["top_seller_abs"], out["negnet_total"]).fillna(0.0)
    out["pos_neg_balance"] = _safe_div(out["posnet_total"], out["negnet_total"]).fillna(0.0)
    out["coord_top_share"] = _safe_div(out["coordinated_top_net"], out["coordinated_net"]).fillna(0.0)

    price = ohlc_symbol[["symbol", "date", "open", "high", "low", "close", "volume"]].copy()
    price = price.sort_values("date")
    price["ret_1d"] = price["close"].pct_change()
    price["ret_3d"] = price["close"].pct_change(3)
    price["ret_5d"] = price["close"].pct_change(5)
    price["prior_high_20"] = price["high"].shift(1).rolling(20, min_periods=10).max()
    price["prior_low_20"] = price["low"].shift(1).rolling(20, min_periods=10).min()
    price["breakout_gap_20"] = price["close"] / price["prior_high_20"] - 1.0
    price["price_pos_20"] = _safe_div(price["close"] - price["prior_low_20"], price["prior_high_20"] - price["prior_low_20"])
    price["volume_ma20"] = price["volume"].rolling(20, min_periods=10).mean()
    price["volume_ratio_20"] = _safe_div(price["volume"], price["volume_ma20"])

    out = price.merge(out, on=["symbol", "date"], how="left")
    numeric_cols = out.select_dtypes(include=[np.number]).columns
    out[numeric_cols] = out[numeric_cols].fillna(0.0)
    return out


def build_micro_features(ohlc: pd.DataFrame, start: str, end: str, max_symbols: int) -> pd.DataFrame:
    symbols = set(ohlc["symbol"].astype(str).unique())
    files = _bs_files(symbols, max_symbols)
    parts: list[pd.DataFrame] = []
    ohlc_groups = {symbol: sub.copy() for symbol, sub in ohlc.groupby("symbol")}
    for i, path in enumerate(files, start=1):
        sub = ohlc_groups.get(path.stem)
        if sub is None:
            continue
        feat = build_symbol_micro_features(path, sub, start, end)
        if not feat.empty:
            parts.append(feat)
        if i % 100 == 0:
            print(f"processed {i}/{len(files)} raw broker files, feature_parts={len(parts)}", flush=True)
    if not parts:
        raise RuntimeError("no microstructure features built")
    feat_all = pd.concat(parts, ignore_index=True)
    return _rolling_pct_by_date(
        feat_all,
        [
            "role_reversal_score",
            "role_reversal_count",
            "coordinated_score",
            "coordinated_count",
            "negnet_total",
            "buyer_count",
            "pos_neg_balance",
        ],
    )


def _dedup_cooldown(signals: pd.DataFrame, cooldown_days: int) -> pd.DataFrame:
    if signals.empty:
        return signals
    rows: list[pd.Series] = []
    for _, sub in signals.sort_values(["symbol", "date", "score"], ascending=[True, True, False]).groupby("symbol"):
        last_date: pd.Timestamp | None = None
        for _, row in sub.iterrows():
            date = pd.Timestamp(row["date"])
            if last_date is not None and (date - last_date).days < cooldown_days:
                continue
            rows.append(row)
            last_date = date
    return pd.DataFrame(rows).sort_values(["date", "score"], ascending=[True, False]).reset_index(drop=True)


def simulate_trades(signals: pd.DataFrame, ohlc_maps: dict[str, dict[str, Any]], hold_days: int, stop_loss: float | None) -> pd.DataFrame:
    cost = CostConfig()
    eff_fee = cost.fee_rate * cost.fee_discount
    rows: list[dict[str, Any]] = []
    for _, signal in signals.iterrows():
        symbol = str(signal["symbol"])
        data = ohlc_maps.get(symbol)
        if data is None:
            continue
        i_signal = data["date_to_idx"].get(pd.Timestamp(signal["date"]))
        if i_signal is None:
            continue
        i_entry = int(i_signal) + 1
        i_exit = i_entry + hold_days
        if i_entry >= len(data["open"]) or i_exit >= len(data["close"]):
            continue
        entry_raw = float(data["open"][i_entry])
        exit_raw = float(data["close"][i_exit])
        if not np.isfinite(entry_raw) or entry_raw <= 0 or not np.isfinite(exit_raw):
            continue
        exit_reason = "TIME"
        if stop_loss is not None:
            stop_raw = entry_raw * (1.0 + stop_loss)
            for j in range(i_entry + 1, i_exit + 1):
                low_j = float(data["low"][j])
                if np.isfinite(low_j) and low_j <= stop_raw:
                    exit_raw = stop_raw
                    i_exit = j
                    exit_reason = "SL"
                    break
        buy_px = entry_raw * (1.0 + cost.slippage)
        sell_px = exit_raw * (1.0 - cost.slippage)
        net_ret = (sell_px * (1.0 - eff_fee - cost.tax_rate_sell)) / (buy_px * (1.0 + eff_fee)) - 1.0
        out = signal.to_dict()
        out.update(
            {
                "signal_date": pd.Timestamp(signal["date"]),
                "entry_date": pd.Timestamp(data["dates"][i_entry]),
                "exit_date": pd.Timestamp(data["dates"][i_exit]),
                "entry_raw": entry_raw,
                "exit_raw": float(exit_raw),
                "net_ret": float(net_ret),
                "exit_reason": exit_reason,
            }
        )
        rows.append(out)
    return pd.DataFrame(rows)


def trade_metrics(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {"trades": 0, "win_rate": np.nan, "avg_net_ret": np.nan, "median_net_ret": np.nan, "max_loss": np.nan}
    ret = pd.to_numeric(trades["net_ret"], errors="coerce").dropna()
    return {
        "trades": int(len(ret)),
        "win_rate": float((ret > 0).mean()),
        "avg_net_ret": float(ret.mean()),
        "median_net_ret": float(ret.median()),
        "max_loss": float(ret.min()),
    }


def _make_signals(feat: pd.DataFrame, family: str, params: dict[str, Any]) -> pd.DataFrame:
    f = feat.copy()
    if family == "role_reversal":
        cond = (
            (f["role_reversal_score_pct_cs"] >= params["score_pct"])
            & (f["role_reversal_count"] >= params["min_count"])
            & (f["volume_ratio_20"] <= params["volume_cap"])
            & (f["ret_5d"] <= params["ret5_cap"])
        )
        score = f["role_reversal_score_pct_cs"] * f["role_reversal_count"].clip(lower=1)
    elif family == "coordinated_accumulation":
        cond = (
            (f["coordinated_score_pct_cs"] >= params["score_pct"])
            & (f["coordinated_count"] >= params["min_count"])
            & (f["coord_top_share"] <= params["top_share_cap"])
            & (f["breakout_gap_20"] <= params["breakout_cap"])
        )
        score = f["coordinated_score_pct_cs"] * f["coordinated_count"].clip(lower=1)
    elif family == "diffuse_buyer_accumulation":
        cond = (
            (f["buyer_count_pct_cs"] >= params["buyer_count_pct"])
            & (f["pos_neg_balance_pct_cs"] >= params["balance_pct"])
            & (f["top_buyer_share"] <= params["top_buyer_share_cap"])
            & (f["ret_5d"] <= params["ret5_cap"])
            & (f["volume_ratio_20"] <= params["volume_cap"])
            & (f["negnet_total_pct_cs"] <= params["sell_pressure_cap"])
        )
        score = f["buyer_count_pct_cs"] + f["pos_neg_balance_pct_cs"] - f["top_buyer_share"]
    elif family == "sell_pressure_absorption":
        cond = (
            (f["negnet_total_pct_cs"] >= params["sell_pressure_pct"])
            & (f["buyer_count_pct_cs"] >= params["buyer_count_pct"])
            & (f["pos_neg_balance"] >= params["balance_min"])
            & (f["ret_3d"] >= params["ret3_floor"])
            & (f["volume_ratio_20"] <= params["volume_cap"])
        )
        score = f["negnet_total_pct_cs"] + f["buyer_count_pct_cs"] + f["pos_neg_balance"].clip(upper=3)
    else:
        raise ValueError(f"unknown family: {family}")
    if "price_pos_min" in params:
        cond &= f["price_pos_20"] >= params["price_pos_min"]
    if "price_pos_max" in params:
        cond &= f["price_pos_20"] <= params["price_pos_max"]
    if "breakout_min" in params:
        cond &= f["breakout_gap_20"] >= params["breakout_min"]
    if "breakout_max" in params:
        cond &= f["breakout_gap_20"] <= params["breakout_max"]
    if "ret3_min" in params:
        cond &= f["ret_3d"] >= params["ret3_min"]
    if "ret5_min" in params:
        cond &= f["ret_5d"] >= params["ret5_min"]
    out = f.loc[cond].copy()
    out["event_family"] = family
    out["score"] = score.loc[out.index]
    out["params_json"] = json.dumps(params, sort_keys=True)
    keep = [
        "symbol",
        "date",
        "event_family",
        "score",
        "params_json",
        "close",
        "ret_3d",
        "ret_5d",
        "breakout_gap_20",
        "price_pos_20",
        "volume_ratio_20",
        "role_reversal_score",
        "role_reversal_count",
        "coordinated_score",
        "coordinated_count",
        "coord_top_share",
        "negnet_total",
        "buyer_count",
        "top_buyer_share",
        "pos_neg_balance",
    ]
    return out[[c for c in keep if c in out.columns]].sort_values(["date", "score"], ascending=[True, False])


def iter_family_params() -> dict[str, list[dict[str, Any]]]:
    params = {
        "role_reversal": [
            {
                "score_pct": score_pct,
                "min_count": min_count,
                "volume_cap": volume_cap,
                "ret5_cap": ret5_cap,
            }
            for score_pct, min_count, volume_cap, ret5_cap in itertools.product(
                [0.95, 0.97, 0.99], [1, 2, 3], [1.5, 2.0, 3.0], [0.05, 0.15, 10.0]
            )
        ],
        "diffuse_buyer_accumulation": [
            {
                "buyer_count_pct": buyer_count_pct,
                "balance_pct": balance_pct,
                "top_buyer_share_cap": top_buyer_share_cap,
                "ret5_cap": ret5_cap,
                "volume_cap": volume_cap,
                "sell_pressure_cap": sell_pressure_cap,
            }
            for buyer_count_pct, balance_pct, top_buyer_share_cap, ret5_cap, volume_cap, sell_pressure_cap in itertools.product(
                [0.98, 0.99], [0.95, 0.98], [0.25, 0.35, 0.45], [0.05, 0.15, 10.0], [1.5, 2.0], [0.85, 1.0]
            )
        ],
        "sell_pressure_absorption": [
            {
                "sell_pressure_pct": sell_pressure_pct,
                "buyer_count_pct": buyer_count_pct,
                "balance_min": balance_min,
                "ret3_floor": ret3_floor,
                "volume_cap": volume_cap,
            }
            for sell_pressure_pct, buyer_count_pct, balance_min, ret3_floor, volume_cap in itertools.product(
                [0.90, 0.95, 0.98], [0.70, 0.80, 0.90], [0.6, 0.8, 1.0], [-0.03, -0.01, 0.0], [1.5, 2.0, 3.0]
            )
        ],
    }
    params["role_reversal"].append(
        {
            "score_pct": 0.99,
            "min_count": 3,
            "volume_cap": 1.5,
            "ret5_cap": 10.0,
            "price_pos_min": 0.4,
            "ret5_min": 0.05,
        }
    )
    params["sell_pressure_absorption"].append(
        {
            "sell_pressure_pct": 0.98,
            "buyer_count_pct": 0.9,
            "balance_min": 0.6,
            "ret3_floor": 0.03,
            "volume_cap": 2.0,
            "price_pos_min": 0.4,
            "breakout_max": 0.1,
            "ret5_min": 0.05,
        }
    )
    return params


def run_search(feat: pd.DataFrame, ohlc_maps: dict[str, dict[str, Any]], min_trades: int) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    rows: list[dict[str, Any]] = []
    best_signals: dict[str, pd.DataFrame] = {}
    best_trades: dict[str, pd.DataFrame] = {}
    for family, params_list in iter_family_params().items():
        best_score: tuple[Any, ...] | None = None
        for param_i, params in enumerate(params_list, start=1):
            signals0 = _make_signals(feat, family, params)
            for hold_days, stop_loss, cooldown_days in itertools.product([10, 20, 40, 60], [None, -0.10, -0.15], [10, 20]):
                signals = _dedup_cooldown(signals0, cooldown_days)
                trades = simulate_trades(signals, ohlc_maps, hold_days, stop_loss)
                metrics = trade_metrics(trades)
                target_pass = bool(
                    metrics["trades"] >= min_trades
                    and metrics["win_rate"] > 0.60
                    and metrics["avg_net_ret"] > 0.10
                )
                row = {
                    "event_family": family,
                    "target_pass": target_pass,
                    "hold_days": hold_days,
                    "stop_loss": stop_loss,
                    "cooldown_days": cooldown_days,
                    "signals": int(len(signals)),
                    **metrics,
                    "params_json": json.dumps(params, sort_keys=True),
                }
                rows.append(row)
                score = (
                    target_pass,
                    float(metrics["avg_net_ret"]) if np.isfinite(metrics["avg_net_ret"]) else -np.inf,
                    float(metrics["win_rate"]) if np.isfinite(metrics["win_rate"]) else -np.inf,
                    int(metrics["trades"]),
                )
                if best_score is None or score > best_score:
                    best_score = score
                    best_signals[family] = signals
                    best_trades[family] = trades
            if param_i % 25 == 0 or param_i == len(params_list):
                print(f"searched {family} params {param_i}/{len(params_list)}: best={best_score}", flush=True)
        print(f"searched {family}: best={best_score}", flush=True)
    leaderboard = pd.DataFrame(rows).sort_values(
        ["target_pass", "avg_net_ret", "win_rate", "trades"], ascending=[False, False, False, False]
    )
    return leaderboard, best_signals, best_trades


def build_improvement_comparison(leaderboard: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family, baseline in LEGACY_BASELINE_PARAMS.items():
        params_json = json.dumps(baseline["params"], sort_keys=True)
        stop_loss = baseline["stop_loss"]
        base_mask = (
            (leaderboard["event_family"] == family)
            & (leaderboard["hold_days"] == baseline["hold_days"])
            & (leaderboard["cooldown_days"] == baseline["cooldown_days"])
            & (leaderboard["params_json"] == params_json)
        )
        if stop_loss is None:
            base_mask &= leaderboard["stop_loss"].isna()
        else:
            base_mask &= leaderboard["stop_loss"] == stop_loss
        baseline_rows = leaderboard.loc[base_mask]
        best_rows = leaderboard.loc[leaderboard["event_family"] == family]
        if baseline_rows.empty or best_rows.empty:
            continue
        base = baseline_rows.iloc[0]
        best = best_rows.iloc[0]
        avg_improvement = float(best["avg_net_ret"] / base["avg_net_ret"] - 1.0)
        win_improvement = float(best["win_rate"] / base["win_rate"] - 1.0)
        rows.append(
            {
                "event_family": family,
                "baseline_trades": int(base["trades"]),
                "baseline_win_rate": float(base["win_rate"]),
                "baseline_avg_net_ret": float(base["avg_net_ret"]),
                "optimized_trades": int(best["trades"]),
                "optimized_win_rate": float(best["win_rate"]),
                "optimized_avg_net_ret": float(best["avg_net_ret"]),
                "avg_net_ret_improvement": avg_improvement,
                "win_rate_improvement": win_improvement,
                "optimized_hold_days": int(best["hold_days"]),
                "optimized_stop_loss": best["stop_loss"],
                "optimized_cooldown_days": int(best["cooldown_days"]),
                "optimized_params_json": best["params_json"],
            }
        )
    return pd.DataFrame(rows)


def build_report(leaderboard: pd.DataFrame, best_trades: dict[str, pd.DataFrame], comparison: pd.DataFrame) -> str:
    lines = [
        "# Raw Broker Microstructure Research",
        "",
        "Inputs: raw `data/ohlc/*.csv` and raw broker parquet under `data/bs_report/parquet_*` only.",
        "",
        "Target gate: `trades >= min_trades`, `win_rate > 60%`, `avg_net_ret > 10%`.",
        "",
        "Improvement is measured against the prior best row for the same family using average net return.",
        "",
        "## Research Directions",
        "",
    ]
    for family, note in FAMILY_NOTES.items():
        lines.extend(
            [
                f"### {family}",
                "",
                f"- hypothesis: {note['hypothesis']}",
                f"- signal: {note['signal']}",
                f"- trade rule: {note['trade']}",
                "",
            ]
        )
    lines.extend(
        [
        "## Best Result By Family",
        "",
        ]
    )
    for family, sub in leaderboard.groupby("event_family", sort=False):
        best = sub.iloc[0]
        lines.extend(
            [
                f"### {family}",
                "",
                f"- target pass: `{bool(best['target_pass'])}`",
                f"- trades: `{int(best['trades'])}`",
                f"- win rate: `{best['win_rate']:.4f}`",
                f"- average net return: `{best['avg_net_ret']:.4f}`",
                f"- median net return: `{best['median_net_ret']:.4f}`",
                f"- max loss: `{best['max_loss']:.4f}`",
                f"- hold_days: `{int(best['hold_days'])}`",
                f"- stop_loss: `{best['stop_loss']}`",
                f"- cooldown_days: `{int(best['cooldown_days'])}`",
                f"- params: `{best['params_json']}`",
                "",
            ]
        )
        trades = best_trades.get(family, pd.DataFrame())
        if not trades.empty:
            lines.append("Top trades:")
            for _, row in trades.sort_values("net_ret", ascending=False).head(5).iterrows():
                lines.append(
                    f"- `{pd.Timestamp(row['signal_date']).date()} {row['symbol']}`: "
                    f"`net_ret={row['net_ret']:.4f}`, `entry={pd.Timestamp(row['entry_date']).date()}`, "
                    f"`exit={pd.Timestamp(row['exit_date']).date()}`"
                )
            lines.append("")
    lines.extend(["## Improvement vs Prior Best", ""])
    if comparison.empty:
        lines.append("No baseline comparison rows were available.")
    else:
        for _, row in comparison.iterrows():
            lines.append(
                f"- `{row['event_family']}`: avg `{row['baseline_avg_net_ret']:.4f}` -> "
                f"`{row['optimized_avg_net_ret']:.4f}` "
                f"(`{row['avg_net_ret_improvement']:.2%}`), trades "
                f"`{int(row['baseline_trades'])}` -> `{int(row['optimized_trades'])}`, "
                f"win `{row['baseline_win_rate']:.4f}` -> `{row['optimized_win_rate']:.4f}`"
            )
        lines.append("")
    passing = leaderboard[leaderboard["target_pass"]]
    lines.extend(["## Passing Rows", ""])
    if passing.empty:
        lines.append("No family met the target gate in this run.")
    else:
        for _, row in passing.head(30).iterrows():
            lines.append(
                f"- `{row['event_family']}`: trades `{int(row['trades'])}`, "
                f"win `{row['win_rate']:.4f}`, avg `{row['avg_net_ret']:.4f}`, params `{row['params_json']}`"
            )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Raw-only broker microstructure research")
    ap.add_argument("--start", default="2025-04-14")
    ap.add_argument("--end", default="", help="YYYY-MM-DD; defaults to latest raw OHLC file date")
    ap.add_argument("--max-symbols", type=int, default=0)
    ap.add_argument("--min-trades", type=int, default=20)
    ap.add_argument("--include-etf", action="store_true")
    ap.add_argument("--output-dir", type=Path, default=OUT_DIR)
    args = ap.parse_args()
    if not args.end:
        args.end = latest_raw_ohlc_date()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in list(out_dir.glob("*_best_signals.csv")) + list(out_dir.glob("*_best_trades.csv")):
        stale.unlink()
    ohlc = load_raw_ohlc(args.start, args.end, exclude_etf=not args.include_etf)
    ohlc_maps = build_ohlc_maps(ohlc)
    feat = build_micro_features(ohlc, args.start, args.end, args.max_symbols)
    feat.to_parquet(out_dir / "raw_micro_features.parquet", index=False)
    leaderboard, best_signals, best_trades = run_search(feat, ohlc_maps, args.min_trades)
    comparison = build_improvement_comparison(leaderboard)
    leaderboard.to_csv(out_dir / "raw_microstructure_leaderboard.csv", index=False)
    comparison.to_csv(out_dir / "raw_microstructure_improvement_comparison.csv", index=False)
    for family, signals in best_signals.items():
        signals.to_csv(out_dir / f"{family}_best_signals.csv", index=False)
    for family, trades in best_trades.items():
        trades.to_csv(out_dir / f"{family}_best_trades.csv", index=False)
    summary = {
        "raw_inputs": ["data/ohlc/*.csv", "data/bs_report/parquet_twse/*.parquet", "data/bs_report/parquet_tpex/*.parquet"],
        "start": args.start,
        "end": args.end,
        "feature_rows": int(len(feat)),
        "symbols": int(feat["symbol"].nunique()),
        "target_gate": {"min_trades": args.min_trades, "win_rate_gt": 0.60, "avg_net_ret_gt": 0.10},
        "passing_families": sorted(leaderboard.loc[leaderboard["target_pass"], "event_family"].unique().tolist()),
    }
    (out_dir / "raw_microstructure_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "raw_microstructure_report.md").write_text(build_report(leaderboard, best_trades, comparison), encoding="utf-8")
    print(out_dir / "raw_microstructure_report.md")


if __name__ == "__main__":
    main()
