#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a first-pass rare-event research pipeline on top of local derived data.

The pipeline focuses on low-frequency, high-conviction event families driven by:
- stock broker accumulation,
- warrant activity confirmation or divergence,
- price compression and low prior price reaction.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stockanalysis.config import ensure_dir

STOCK_STRONG_POSNET_PCT = 0.95
WARRANT_STRONG_POSNET_PCT = 0.80


@dataclass(frozen=True)
class CostConfig:
    fee_rate: float = 0.001425
    fee_discount: float = 0.28
    fee_min: float = 20.0
    tax_rate_sell: float = 0.003
    slippage: float = 0.0005


def _pct_rank_by_date(df: pd.DataFrame, value_col: str, out_col: str) -> None:
    s = pd.to_numeric(df[value_col], errors="coerce")
    df[out_col] = s.groupby(df["date"]).rank(method="average", pct=True)


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    out = a / b.replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan)


def load_base_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ohlc = pd.read_parquet(
        PROJECT_ROOT / "data/_derived/ohlc.parquet",
        columns=["symbol", "date", "open", "high", "low", "close", "volume"],
    )
    stock = pd.read_parquet(PROJECT_ROOT / "data/_derived/flow_stock_daily.parquet")
    warrant = pd.read_parquet(PROJECT_ROOT / "data/_derived/flow_warrant_daily_by_underlying.parquet")

    for df in (ohlc, stock, warrant):
        df["symbol"] = df["symbol"].astype(str)
        df["date"] = pd.to_datetime(df["date"])

    ohlc = ohlc[ohlc["symbol"].str.len() == 4].copy()
    stock = stock[stock["symbol"].str.len() == 4].copy()
    warrant = warrant[warrant["symbol"].str.len() == 4].copy()
    return ohlc, stock, warrant


def build_features() -> pd.DataFrame:
    ohlc, stock, warrant = load_base_frames()
    ohlc = ohlc.sort_values(["symbol", "date"]).reset_index(drop=True)

    g = ohlc.groupby("symbol", group_keys=False)
    daily_ret_1 = g["close"].pct_change()
    ohlc["ret_5d"] = g["close"].pct_change(5)
    ohlc["ret_10d"] = g["close"].pct_change(10)
    ohlc["ret_20d"] = g["close"].pct_change(20)
    ohlc["ret_40d"] = g["close"].pct_change(40)
    ohlc["prior_abs_ret_10d"] = ohlc["ret_10d"].abs()
    ohlc["prior_abs_ret_20d"] = ohlc["ret_20d"].abs()
    ohlc["vol_10d"] = daily_ret_1.groupby(ohlc["symbol"]).rolling(10, min_periods=5).std().reset_index(level=0, drop=True)
    ohlc["vol_20d"] = daily_ret_1.groupby(ohlc["symbol"]).rolling(20, min_periods=10).std().reset_index(level=0, drop=True)
    ohlc["volume_ma_5"] = g["volume"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    ohlc["volume_ma_20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    ohlc["volume_ratio_5_20"] = _safe_div(ohlc["volume_ma_5"], ohlc["volume_ma_20"])
    ohlc["range_ma_10"] = ((ohlc["high"] - ohlc["low"]) / ohlc["close"]).groupby(ohlc["symbol"]).rolling(10, min_periods=5).mean().reset_index(level=0, drop=True)
    ohlc["range_ma_20"] = ((ohlc["high"] - ohlc["low"]) / ohlc["close"]).groupby(ohlc["symbol"]).rolling(20, min_periods=10).mean().reset_index(level=0, drop=True)

    prev_high_20 = g["high"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).max())
    prev_low_20 = g["low"].transform(lambda s: s.shift(1).rolling(20, min_periods=10).min())
    prev_high_60 = g["high"].transform(lambda s: s.shift(1).rolling(60, min_periods=20).max())
    prev_low_60 = g["low"].transform(lambda s: s.shift(1).rolling(60, min_periods=20).min())
    ohlc["breakout_gap_20"] = ohlc["close"] / prev_high_20 - 1.0
    ohlc["breakout_gap_60"] = ohlc["close"] / prev_high_60 - 1.0
    ohlc["drawdown_20"] = ohlc["close"] / prev_high_20 - 1.0
    ohlc["compression_20"] = (prev_high_20 - prev_low_20) / ohlc["close"]
    ohlc["compression_60"] = (prev_high_60 - prev_low_60) / ohlc["close"]
    ohlc["price_pos_20"] = _safe_div(ohlc["close"] - prev_low_20, prev_high_20 - prev_low_20)
    ohlc["price_pos_60"] = _safe_div(ohlc["close"] - prev_low_60, prev_high_60 - prev_low_60)

    stock = stock.sort_values(["symbol", "date"]).reset_index(drop=True)
    stock["stock_hhi_posnet_10"] = stock.groupby("symbol")["hhi_posnet"].transform(lambda s: s.rolling(10, min_periods=4).mean())
    stock["stock_hhi_posnet_20"] = stock.groupby("symbol")["hhi_posnet"].transform(lambda s: s.rolling(20, min_periods=6).mean())
    stock["stock_hhi_posnet_40"] = stock.groupby("symbol")["hhi_posnet"].transform(lambda s: s.rolling(40, min_periods=10).mean())
    stock["stock_top_posnet_ratio_20"] = stock.groupby("symbol")["top_posnet_ratio"].transform(
        lambda s: s.rolling(20, min_periods=6).mean()
    )
    stock["stock_dyn_k_20"] = stock.groupby("symbol")["dyn_k"].transform(lambda s: s.rolling(20, min_periods=6).mean())

    warrant = warrant.sort_values(["symbol", "date"]).reset_index(drop=True)
    warrant["warrant_hhi_posnet_10"] = warrant.groupby("symbol")["hhi_posnet"].transform(lambda s: s.rolling(10, min_periods=4).mean())
    warrant["warrant_hhi_posnet_20"] = warrant.groupby("symbol")["hhi_posnet"].transform(lambda s: s.rolling(20, min_periods=6).mean())
    warrant["warrant_hhi_posnet_40"] = warrant.groupby("symbol")["hhi_posnet"].transform(lambda s: s.rolling(40, min_periods=10).mean())
    warrant["warrant_top_posnet_ratio_20"] = warrant.groupby("symbol")["top_posnet_ratio"].transform(
        lambda s: s.rolling(20, min_periods=6).mean()
    )
    warrant["warrant_dyn_k_20"] = warrant.groupby("symbol")["dyn_k"].transform(lambda s: s.rolling(20, min_periods=6).mean())

    stock_cols = [
        "symbol",
        "date",
        "net_total",
        "posnet_total",
        "hhi_posnet",
        "top_posnet_ratio",
        "dyn_k",
        "stock_hhi_posnet_10",
        "stock_hhi_posnet_20",
        "stock_hhi_posnet_40",
        "stock_top_posnet_ratio_20",
        "stock_dyn_k_20",
    ]
    stock = stock[stock_cols].rename(
        columns={
            "net_total": "stock_net_total",
            "posnet_total": "stock_posnet_total",
            "hhi_posnet": "stock_hhi_posnet",
            "top_posnet_ratio": "stock_top_posnet_ratio",
            "dyn_k": "stock_dyn_k",
        }
    )
    warrant_cols = [
        "symbol",
        "date",
        "net_total",
        "posnet_total",
        "hhi_posnet",
        "top_posnet_ratio",
        "dyn_k",
        "warrant_hhi_posnet_10",
        "warrant_hhi_posnet_20",
        "warrant_hhi_posnet_40",
        "warrant_top_posnet_ratio_20",
        "warrant_dyn_k_20",
    ]
    warrant = warrant[warrant_cols].rename(
        columns={
            "net_total": "warrant_net_total",
            "posnet_total": "warrant_posnet_total",
            "hhi_posnet": "warrant_hhi_posnet",
            "top_posnet_ratio": "warrant_top_posnet_ratio",
            "dyn_k": "warrant_dyn_k",
        }
    )

    feat = ohlc.merge(stock, on=["symbol", "date"], how="left").merge(warrant, on=["symbol", "date"], how="left")
    numeric_cols = [c for c in feat.columns if c not in {"symbol", "date"}]
    feat[numeric_cols] = feat[numeric_cols].apply(pd.to_numeric, errors="coerce")

    percentile_specs = [
        ("stock_posnet_total", "stock_posnet_pct_cs"),
        ("stock_hhi_posnet", "stock_hhi_pct_cs"),
        ("stock_dyn_k", "stock_dyn_k_pct_cs"),
        ("warrant_posnet_total", "warrant_posnet_pct_cs"),
        ("warrant_hhi_posnet", "warrant_hhi_pct_cs"),
        ("warrant_dyn_k", "warrant_dyn_k_pct_cs"),
    ]
    for src, out in percentile_specs:
        _pct_rank_by_date(feat, src, out)

    _pct_rank_by_date(feat, "compression_20", "compression_20_pct_cs")
    _pct_rank_by_date(feat, "prior_abs_ret_20d", "prior_abs_ret_20d_pct_cs")
    _pct_rank_by_date(feat, "volume_ratio_5_20", "volume_ratio_5_20_pct_cs")

    feat = feat.sort_values(["symbol", "date"]).reset_index(drop=True)
    feat["stock_posnet_strong_day"] = (feat["stock_posnet_pct_cs"] >= STOCK_STRONG_POSNET_PCT).astype(float)
    feat["stock_posnet_strong_days_10"] = feat.groupby("symbol")["stock_posnet_strong_day"].transform(
        lambda s: s.rolling(10, min_periods=4).sum()
    )
    feat["stock_posnet_strong_days_20"] = feat.groupby("symbol")["stock_posnet_strong_day"].transform(
        lambda s: s.rolling(20, min_periods=6).sum()
    )
    feat["stock_posnet_strong_days_40"] = feat.groupby("symbol")["stock_posnet_strong_day"].transform(
        lambda s: s.rolling(40, min_periods=10).sum()
    )
    feat["warrant_posnet_strong_day"] = (feat["warrant_posnet_pct_cs"] >= WARRANT_STRONG_POSNET_PCT).astype(float)
    feat["warrant_posnet_strong_days_10"] = feat.groupby("symbol")["warrant_posnet_strong_day"].transform(
        lambda s: s.rolling(10, min_periods=4).sum()
    )
    feat["warrant_posnet_strong_days_20"] = feat.groupby("symbol")["warrant_posnet_strong_day"].transform(
        lambda s: s.rolling(20, min_periods=6).sum()
    )
    feat["warrant_posnet_strong_days_40"] = feat.groupby("symbol")["warrant_posnet_strong_day"].transform(
        lambda s: s.rolling(40, min_periods=10).sum()
    )

    feat["stock_warrant_posnet_gap"] = feat["stock_posnet_pct_cs"] - feat["warrant_posnet_pct_cs"]
    feat["stock_warrant_dynk_gap"] = feat["stock_dyn_k_pct_cs"] - feat["warrant_dyn_k_pct_cs"]
    feat["warrant_confirms_stock"] = (
        (feat["stock_posnet_pct_cs"] >= 0.9) & (feat["warrant_posnet_pct_cs"] >= 0.7)
    ).astype(int)
    feat["stealth_stock_accum"] = (
        (feat["stock_posnet_pct_cs"] >= 0.9) & (feat["warrant_posnet_pct_cs"] <= 0.4)
    ).astype(int)

    feat = feat.sort_values(["date", "symbol"]).reset_index(drop=True)
    return feat


def build_ohlc_map(ohlc: pd.DataFrame) -> dict[str, dict[str, np.ndarray | dict]]:
    out: dict[str, dict[str, np.ndarray | dict]] = {}
    for sym, sub in ohlc.groupby("symbol"):
        sub = sub.sort_values("date").reset_index(drop=True)
        dates = sub["date"].to_numpy(dtype="datetime64[ns]")
        out[sym] = {
            "dates": dates,
            "open": sub["open"].to_numpy(float),
            "high": sub["high"].to_numpy(float),
            "low": sub["low"].to_numpy(float),
            "close": sub["close"].to_numpy(float),
            "date_to_idx": {pd.Timestamp(d): int(i) for i, d in enumerate(dates)},
        }
    return out


def _iter_event_specs() -> list[dict[str, object]]:
    specs: list[dict[str, object]] = []

    def add_family(
        name: str,
        hold_days_list: list[int],
        cooldown_list: list[int],
        stop_loss_list: list[float | None] | None = None,
        **grid: list[object],
    ) -> None:
        keys = list(grid.keys())
        sl_values = stop_loss_list if stop_loss_list is not None else [None]
        for values in itertools.product(*(grid[k] for k in keys)):
            params = dict(zip(keys, values))
            for hold_days, cooldown_days, stop_loss in itertools.product(hold_days_list, cooldown_list, sl_values):
                spec = {
                    "family": name,
                    "hold_days": hold_days,
                    "cooldown_days": cooldown_days,
                    "stop_loss": stop_loss,
                }
                spec.update(params)
                specs.append(spec)

    add_family(
        "extreme_accum_compression",
        hold_days_list=[20],
        cooldown_list=[15],
        stock_posnet_pct_cs=[0.98, 0.99],
        stock_hhi_pct_cs=[0.85],
        stock_posnet_strong_days_20=[3, 5],
        compression_20=[0.18],
        prior_abs_ret_20d=[0.08],
        warrant_posnet_floor=[0.50, 0.70],
    )
    add_family(
        "stealth_accumulation",
        hold_days_list=[20],
        cooldown_list=[15],
        stock_posnet_pct_cs=[0.98, 0.99],
        stock_posnet_strong_days_20=[3, 5],
        compression_20=[0.20],
        prior_abs_ret_20d=[0.08],
        warrant_posnet_cap=[0.40, 0.55],
    )
    add_family(
        "stock_then_warrant_confirmation",
        hold_days_list=[20, 30],
        cooldown_list=[15],
        stock_posnet_pct_cs=[0.95, 0.98],
        stock_posnet_strong_days_20=[3, 5],
        warrant_posnet_floor=[0.70],
        warrant_posnet_strong_days_20=[1, 3],
        prior_abs_ret_20d=[0.08],
    )
    add_family(
        "accumulation_during_compression",
        hold_days_list=[20, 30],
        cooldown_list=[20],
        stock_posnet_pct_cs=[0.95, 0.98],
        stock_hhi_pct_cs=[0.85],
        compression_20=[0.15, 0.22],
        price_pos_20_cap=[0.75],
        prior_abs_ret_20d=[0.10],
    )
    add_family(
        "warrant_leads_stock",
        hold_days_list=[30, 40],
        cooldown_list=[15, 20],
        stop_loss_list=[None, -0.12, -0.10],
        warrant_posnet_floor=[0.98, 0.99],
        warrant_posnet_strong_days_20=[1, 3, 5],
        stock_posnet_floor=[0.70],
        stock_posnet_cap=[0.90],
        prior_abs_ret_20d=[0.08, 0.12],
    )
    add_family(
        "accumulation_pre_breakout",
        hold_days_list=[20, 30],
        cooldown_list=[15],
        stop_loss_list=[None, -0.10],
        stock_posnet_pct_cs=[0.95, 0.98],
        stock_posnet_strong_days_20=[3, 5],
        breakout_gap_20_cap=[0.02, 0.05],
        volume_ratio_5_20_cap=[1.50, 1.80],
        warrant_posnet_floor=[0.50, 0.70],
    )
    return specs


def _fmt_token(value: object) -> str:
    if isinstance(value, float):
        return str(value).replace(".", "p")
    return str(value)


def _event_name(spec: dict[str, object]) -> str:
    parts = [str(spec["family"]), f"h{spec['hold_days']}", f"cd{spec['cooldown_days']}"]
    for k in sorted(spec.keys()):
        if k in {"family", "hold_days", "cooldown_days"}:
            continue
        parts.append(f"{k}-{_fmt_token(spec[k])}")
    return "__".join(parts)


def _apply_event_spec(feat: pd.DataFrame, spec: dict[str, object]) -> pd.Series:
    family = str(spec["family"])
    cond = feat["symbol"].notna()
    if family == "extreme_accum_compression":
        cond &= feat["stock_posnet_pct_cs"] >= float(spec["stock_posnet_pct_cs"])
        cond &= feat["stock_hhi_pct_cs"] >= float(spec["stock_hhi_pct_cs"])
        cond &= feat["stock_posnet_strong_days_20"] >= float(spec["stock_posnet_strong_days_20"])
        cond &= feat["compression_20"] <= float(spec["compression_20"])
        cond &= feat["prior_abs_ret_20d"] <= float(spec["prior_abs_ret_20d"])
        cond &= feat["warrant_posnet_pct_cs"].fillna(0.0) >= float(spec["warrant_posnet_floor"])
    elif family == "stealth_accumulation":
        cond &= feat["stock_posnet_pct_cs"] >= float(spec["stock_posnet_pct_cs"])
        cond &= feat["stock_posnet_strong_days_20"] >= float(spec["stock_posnet_strong_days_20"])
        cond &= feat["compression_20"] <= float(spec["compression_20"])
        cond &= feat["prior_abs_ret_20d"] <= float(spec["prior_abs_ret_20d"])
        cond &= feat["warrant_posnet_pct_cs"].fillna(0.0) <= float(spec["warrant_posnet_cap"])
    elif family == "stock_then_warrant_confirmation":
        cond &= feat["stock_posnet_pct_cs"] >= float(spec["stock_posnet_pct_cs"])
        cond &= feat["stock_posnet_strong_days_20"] >= float(spec["stock_posnet_strong_days_20"])
        cond &= feat["warrant_posnet_pct_cs"].fillna(0.0) >= float(spec["warrant_posnet_floor"])
        cond &= feat["warrant_posnet_strong_days_20"].fillna(0.0) >= float(spec["warrant_posnet_strong_days_20"])
        cond &= feat["prior_abs_ret_20d"] <= float(spec["prior_abs_ret_20d"])
    elif family == "accumulation_during_compression":
        cond &= feat["stock_posnet_pct_cs"] >= float(spec["stock_posnet_pct_cs"])
        cond &= feat["stock_hhi_pct_cs"] >= float(spec["stock_hhi_pct_cs"])
        cond &= feat["compression_20"] <= float(spec["compression_20"])
        cond &= feat["price_pos_20"].fillna(1.0) <= float(spec["price_pos_20_cap"])
        cond &= feat["prior_abs_ret_20d"] <= float(spec["prior_abs_ret_20d"])
    elif family == "warrant_leads_stock":
        cond &= feat["warrant_posnet_pct_cs"].fillna(0.0) >= float(spec["warrant_posnet_floor"])
        cond &= feat["warrant_posnet_strong_days_20"].fillna(0.0) >= float(spec["warrant_posnet_strong_days_20"])
        cond &= feat["stock_posnet_pct_cs"] >= float(spec["stock_posnet_floor"])
        cond &= feat["stock_posnet_pct_cs"] <= float(spec["stock_posnet_cap"])
        cond &= feat["prior_abs_ret_20d"] <= float(spec["prior_abs_ret_20d"])
    elif family == "accumulation_pre_breakout":
        cond &= feat["stock_posnet_pct_cs"] >= float(spec["stock_posnet_pct_cs"])
        cond &= feat["stock_posnet_strong_days_20"] >= float(spec["stock_posnet_strong_days_20"])
        cond &= feat["breakout_gap_20"].fillna(1.0) <= float(spec["breakout_gap_20_cap"])
        cond &= feat["breakout_gap_20"].fillna(-1.0) >= -0.08
        cond &= feat["volume_ratio_5_20"].fillna(np.inf) <= float(spec["volume_ratio_5_20_cap"])
        cond &= feat["warrant_posnet_pct_cs"].fillna(0.0) >= float(spec["warrant_posnet_floor"])
    else:
        raise ValueError(f"unknown family: {family}")
    cond &= feat["open"].notna() & feat["close"].notna() & feat["high"].notna() & feat["low"].notna()
    return cond.fillna(False)


def _dedup_cooldown(signals: pd.DataFrame, cooldown_days: int) -> pd.DataFrame:
    kept = []
    for _, sub in signals.sort_values(["symbol", "date"]).groupby("symbol"):
        last_date: pd.Timestamp | None = None
        for _, row in sub.iterrows():
            d = pd.Timestamp(row["date"])
            if last_date is None or (d - last_date).days >= cooldown_days:
                kept.append(row)
                last_date = d
    if not kept:
        return signals.iloc[0:0].copy()
    return pd.DataFrame(kept).reset_index(drop=True)


def _trade_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0,
            "avg_net_ret": np.nan,
            "median_net_ret": np.nan,
            "win_rate": np.nan,
            "profit_factor": np.nan,
            "payoff_ratio": np.nan,
            "avg_mfe_20d": np.nan,
            "avg_mfe_40d": np.nan,
            "avg_mae_20d": np.nan,
            "mfe_20d_hit_10": np.nan,
            "mfe_40d_hit_20": np.nan,
            "mfe_mae_ratio": np.nan,
            "max_loss": np.nan,
        }

    pos = trades.loc[trades["net_ret"] > 0, "net_ret"]
    neg = trades.loc[trades["net_ret"] < 0, "net_ret"]
    gross_profit = float(pos.sum()) if not pos.empty else 0.0
    gross_loss = float((-neg).sum()) if not neg.empty else 0.0
    avg_win = float(pos.mean()) if not pos.empty else np.nan
    avg_loss = float((-neg).mean()) if not neg.empty else np.nan
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else math.inf
    payoff_ratio = avg_win / avg_loss if np.isfinite(avg_win) and np.isfinite(avg_loss) and avg_loss > 0 else math.inf
    avg_mae_20d = float(trades["mae_20d"].mean()) if "mae_20d" in trades else np.nan
    mfe_mae_ratio = (
        float(trades["mfe_20d"].mean()) / abs(avg_mae_20d)
        if np.isfinite(avg_mae_20d) and avg_mae_20d != 0
        else math.inf
    )
    return {
        "trades": int(len(trades)),
        "avg_net_ret": float(trades["net_ret"].mean()),
        "median_net_ret": float(trades["net_ret"].median()),
        "win_rate": float((trades["net_ret"] > 0).mean()),
        "profit_factor": float(profit_factor),
        "payoff_ratio": float(payoff_ratio),
        "avg_mfe_20d": float(trades["mfe_20d"].mean()),
        "avg_mfe_40d": float(trades["mfe_40d"].mean()),
        "avg_mae_20d": float(avg_mae_20d),
        "mfe_20d_hit_10": float((trades["mfe_20d"] >= 0.10).mean()),
        "mfe_40d_hit_20": float((trades["mfe_40d"] >= 0.20).mean()),
        "mfe_mae_ratio": float(mfe_mae_ratio),
        "max_loss": float(trades["net_ret"].min()),
    }


def _criteria_full(metrics: dict[str, float]) -> tuple[bool, list[str]]:
    fails: list[str] = []
    if metrics["trades"] < 20:
        fails.append("full_trades<20")
    if not (metrics["avg_net_ret"] > 0.05):
        fails.append("avg_net_ret<=5%")
    if not (metrics["median_net_ret"] > 0.01):
        fails.append("median_net_ret<=1%")
    if not (metrics["avg_mfe_20d"] >= 0.08):
        fails.append("avg_mfe_20d<8%")
    if not (metrics["avg_mfe_40d"] >= 0.12):
        fails.append("avg_mfe_40d<12%")
    if not (metrics["mfe_20d_hit_10"] >= 0.20):
        fails.append("mfe20_hit10<20%")
    if not (metrics["mfe_40d_hit_20"] >= 0.10):
        fails.append("mfe40_hit20<10%")
    if not (metrics["profit_factor"] >= 1.8):
        fails.append("profit_factor<1.8")
    if not (metrics["payoff_ratio"] >= 2.0):
        fails.append("payoff_ratio<2.0")
    if not (metrics["mfe_mae_ratio"] >= 1.5):
        fails.append("mfe_mae_ratio<1.5")
    if not (metrics["max_loss"] > -0.15):
        fails.append("max_loss<=-15%")
    return len(fails) == 0, fails


def _criteria_test(metrics: dict[str, float]) -> tuple[bool, list[str]]:
    fails: list[str] = []
    if metrics["trades"] < 5:
        fails.append("test_trades<5")
    if not (metrics["avg_net_ret"] > 0.03):
        fails.append("test_avg_net_ret<=3%")
    if not (metrics["avg_mfe_20d"] >= 0.06):
        fails.append("test_avg_mfe_20d<6%")
    if not (metrics["profit_factor"] >= 1.3):
        fails.append("test_profit_factor<1.3")
    if not (metrics["payoff_ratio"] >= 1.5):
        fails.append("test_payoff_ratio<1.5")
    if not (metrics["max_loss"] > -0.15):
        fails.append("test_max_loss<=-15%")
    return len(fails) == 0, fails


def simulate_trades(
    signals: pd.DataFrame,
    ohlc_map: dict[str, dict[str, np.ndarray | dict]],
    hold_days: int,
    cost: CostConfig,
    stop_loss: float | None = None,
) -> pd.DataFrame:
    rows = []
    eff_fee = cost.fee_rate * cost.fee_discount
    for _, row in signals.iterrows():
        sym = str(row["symbol"])
        d = pd.Timestamp(row["date"])
        data = ohlc_map.get(sym)
        if data is None:
            continue
        idx_map = data["date_to_idx"]
        i_signal = idx_map.get(d)
        if i_signal is None:
            continue
        i_entry = i_signal + 1
        i_exit = i_entry + hold_days
        i_mfe20 = i_entry + 20
        i_mfe40 = i_entry + 40
        if i_entry >= len(data["open"]) or i_exit >= len(data["close"]) or i_mfe40 >= len(data["close"]):
            continue

        entry_px_raw = float(data["open"][i_entry])
        exit_px_raw = float(data["close"][i_exit])
        exit_reason = "TIME"
        if not np.isfinite(entry_px_raw) or not np.isfinite(exit_px_raw) or entry_px_raw <= 0:
            continue

        if stop_loss is not None:
            stop_px_raw = entry_px_raw * (1.0 + stop_loss)
            for j in range(i_entry + 1, i_exit + 1):
                low_j = float(data["low"][j])
                if np.isfinite(low_j) and low_j <= stop_px_raw:
                    exit_px_raw = stop_px_raw
                    i_exit = j
                    exit_reason = "SL"
                    break

        buy_px = entry_px_raw * (1.0 + cost.slippage)
        sell_px = exit_px_raw * (1.0 - cost.slippage)
        buy_fee = eff_fee
        sell_fee = eff_fee + cost.tax_rate_sell
        net_ret = (sell_px * (1.0 - sell_fee)) / (buy_px * (1.0 + buy_fee)) - 1.0

        high20 = float(np.nanmax(data["high"][i_entry : i_mfe20 + 1]))
        low20 = float(np.nanmin(data["low"][i_entry : i_mfe20 + 1]))
        high40 = float(np.nanmax(data["high"][i_entry : i_mfe40 + 1]))
        mfe20 = high20 / buy_px - 1.0
        mfe40 = high40 / buy_px - 1.0
        mae20 = low20 / buy_px - 1.0

        out = row.to_dict()
        out.update(
            {
                "signal_date": d,
                "entry_date": pd.Timestamp(data["dates"][i_entry]),
                "exit_date": pd.Timestamp(data["dates"][i_exit]),
                "entry_px_raw": entry_px_raw,
                "entry_px_net": buy_px,
                "exit_px_raw": exit_px_raw,
                "exit_px_net": sell_px,
                "net_ret": float(net_ret),
                "mfe_20d": float(mfe20),
                "mfe_40d": float(mfe40),
                "mae_20d": float(mae20),
                "exit_reason": exit_reason,
            }
        )
        rows.append(out)
    return pd.DataFrame(rows)


def summarize_candidate(signals: pd.DataFrame, trades: pd.DataFrame, spec: dict[str, object]) -> dict[str, object]:
    # Keep test inside the observable 40-day horizon; later signals would lose MFE_40d coverage.
    train_end = pd.Timestamp("2025-09-30")
    valid_end = pd.Timestamp("2025-11-15")
    test_end = pd.Timestamp("2025-12-15")

    if trades.empty or "signal_date" not in trades.columns:
        train = pd.DataFrame()
        valid = pd.DataFrame()
        test = pd.DataFrame()
    else:
        train = trades[trades["signal_date"] <= train_end].copy()
        valid = trades[(trades["signal_date"] > train_end) & (trades["signal_date"] <= valid_end)].copy()
        test = trades[(trades["signal_date"] > valid_end) & (trades["signal_date"] <= test_end)].copy()

    full_metrics = _trade_metrics(trades)
    train_metrics = _trade_metrics(train)
    valid_metrics = _trade_metrics(valid)
    test_metrics = _trade_metrics(test)
    full_pass, full_fails = _criteria_full(full_metrics)
    test_pass, test_fails = _criteria_test(test_metrics)

    robustness_score = 0.0
    if np.isfinite(full_metrics["avg_net_ret"]):
        robustness_score += 2.0 * full_metrics["avg_net_ret"]
    if np.isfinite(full_metrics["median_net_ret"]):
        robustness_score += full_metrics["median_net_ret"]
    if np.isfinite(full_metrics["avg_mfe_20d"]):
        robustness_score += full_metrics["avg_mfe_20d"]
    if np.isfinite(full_metrics["avg_mfe_40d"]):
        robustness_score += 2.0 * full_metrics["avg_mfe_40d"]
    if np.isfinite(full_metrics["mfe_20d_hit_10"]):
        robustness_score += full_metrics["mfe_20d_hit_10"]
    if np.isfinite(full_metrics["mfe_40d_hit_20"]):
        robustness_score += full_metrics["mfe_40d_hit_20"]
    if np.isfinite(full_metrics["payoff_ratio"]) and not math.isinf(full_metrics["payoff_ratio"]):
        robustness_score += 0.35 * full_metrics["payoff_ratio"]
    if np.isfinite(full_metrics["profit_factor"]) and not math.isinf(full_metrics["profit_factor"]):
        robustness_score += 0.10 * full_metrics["profit_factor"]
    if np.isfinite(test_metrics["avg_net_ret"]):
        robustness_score += test_metrics["avg_net_ret"]
    if np.isfinite(test_metrics["avg_mfe_20d"]):
        robustness_score += test_metrics["avg_mfe_20d"]

    rejected_reason = []
    if not full_pass:
        rejected_reason.extend(full_fails)
    if not test_pass:
        rejected_reason.extend(test_fails)

    base = {
        "event_name": _event_name(spec),
        "family": spec["family"],
        "hold_days": spec["hold_days"],
        "cooldown_days": spec["cooldown_days"],
        "stop_loss": spec.get("stop_loss"),
        "signal_count": int(len(signals)),
        "full_pass": full_pass,
        "test_pass": test_pass,
        "all_pass": bool(full_pass and test_pass),
        "robustness_score": float(robustness_score),
        "rejected_reason": "|".join(rejected_reason),
        "params_json": json.dumps(spec, ensure_ascii=False, sort_keys=True),
    }

    for prefix, metrics in [("train", train_metrics), ("valid", valid_metrics), ("test", test_metrics), ("full", full_metrics)]:
        for k, v in metrics.items():
            base[f"{prefix}_{k}"] = v
    return base


def build_report(
    best_row: pd.Series | None,
    all_rows: pd.DataFrame,
    rejected: pd.DataFrame,
    best_trades: pd.DataFrame,
) -> str:
    family_explanations = {
        "accumulation_pre_breakout": [
            "extreme stock-chip accumulation with muted prior price reaction can indicate inventory absorption before a cleaner breakout",
            "warrant confirmation helps separate real sponsorship from shallow one-day bursts",
        ],
        "warrant_leads_stock": [
            "warrant-side activity can move earlier than the underlying when informed or anticipatory flow reaches leverage instruments first",
            "keeping stock-chip strength in a mid-high but not fully crowded zone helps avoid already overreacted names",
        ],
        "stock_then_warrant_confirmation": [
            "persistent stock-chip accumulation followed by warrant participation may mark a transition from quiet inventory build to broader recognition",
            "the warrant leg acts as delayed confirmation rather than initial hype",
        ],
        "stealth_accumulation": [
            "stock-chip strength without hot warrant participation may capture quieter accumulation before public attention expands",
            "compression and low prior reaction keep the setup focused on under-reacted names",
        ],
        "extreme_accum_compression": [
            "highly concentrated broker accumulation during compression can signal tight supply before repricing",
            "warrant participation adds a secondary confirmation layer without requiring a full breakout first",
        ],
        "accumulation_during_compression": [
            "strong broker sponsorship while price remains compressed can indicate inventory transfer before expansion",
            "capping price position helps avoid late entries after the move has already started",
        ],
    }
    lines = ["# Rare Event Strategy Report", ""]
    if best_row is None:
        lines.extend(
            [
                "No event pattern passed all criteria in this iteration.",
                "",
                "## Best Partial Candidates",
            ]
        )
        top = all_rows.sort_values(["robustness_score", "full_avg_mfe_40d"], ascending=False).head(5)
        for _, row in top.iterrows():
            lines.extend(
                [
                    f"### {row['event_name']}",
                    f"- family: `{row['family']}`",
                    f"- params: `{row['params_json']}`",
                    f"- full trades: `{int(row['full_trades'])}`",
                    f"- avg net return: `{row['full_avg_net_ret']:.4f}`",
                    f"- median net return: `{row['full_median_net_ret']:.4f}`",
                    f"- win rate: `{row['full_win_rate']:.4f}`",
                    f"- avg MFE_20d: `{row['full_avg_mfe_20d']:.4f}`",
                    f"- avg MFE_40d: `{row['full_avg_mfe_40d']:.4f}`",
                    f"- payoff ratio: `{row['full_payoff_ratio']:.4f}`" if np.isfinite(row["full_payoff_ratio"]) else "- payoff ratio: `inf`",
                    f"- test trades: `{int(row['test_trades'])}`",
                    f"- test avg net return: `{row['test_avg_net_ret']:.4f}`",
                    f"- failed criteria: `{row['rejected_reason']}`",
                    "",
                ]
            )
        lines.extend(["## Rejected Event Families"])
        for _, row in rejected.head(10).iterrows():
            lines.append(f"- `{row['event_name']}`: `{row['rejected_reason']}`")
        return "\n".join(lines)

    top_best = best_trades.sort_values("net_ret", ascending=False).head(5)
    worst_trades = best_trades.sort_values("net_ret", ascending=True).head(5)
    sym_counts = best_trades["symbol"].astype(str).value_counts().head(5)
    month_counts = (
        best_trades.assign(month=best_trades["signal_date"].dt.to_period("M").astype(str))["month"]
        .value_counts()
        .sort_index()
    )

    explanation_lines = family_explanations.get(
        str(best_row["family"]),
        [
            "broker and warrant flow can reveal sponsorship before the price move is fully visible in OHLC alone",
            "strict event definitions aim to keep only asymmetric setups with controllable downside",
        ],
    )

    lines.extend(
        [
            f"## Best Event: {best_row['event_name']}",
            "",
            f"- profit factor: `{'inf' if not np.isfinite(best_row['full_profit_factor']) else format(best_row['full_profit_factor'], '.4f')}`",
            f"- payoff ratio: `{'inf' if not np.isfinite(best_row['full_payoff_ratio']) else format(best_row['full_payoff_ratio'], '.4f')}`",
            f"- event name: `{best_row['event_name']}`",
            "- split definition: `train <= 2025-09-30`, `valid = 2025-10-01 ~ 2025-11-15`, `test = 2025-11-16 ~ 2025-12-15`",
            f"- entry rule: next open after signal; strict event filter defined by `{best_row['params_json']}`",
            f"- exit rule: `stop_loss={best_row['stop_loss']}` if hit, otherwise close after `{int(best_row['hold_days'])}` trading days",
            f"- number of trades: `{int(best_row['full_trades'])}`",
            f"- average net return: `{best_row['full_avg_net_ret']:.4f}`",
            f"- median net return: `{best_row['full_median_net_ret']:.4f}`",
            f"- win rate: `{best_row['full_win_rate']:.4f}`",
            f"- average MFE and MAE: `mfe20={best_row['full_avg_mfe_20d']:.4f}`, `mfe40={best_row['full_avg_mfe_40d']:.4f}`, `mae20={best_row['full_avg_mae_20d']:.4f}`",
            f"- MFE >= 10% hit rate: `{best_row['full_mfe_20d_hit_10']:.4f}`",
            f"- MFE >= 20% hit rate: `{best_row['full_mfe_40d_hit_20']:.4f}`",
            f"- train/validation/test performance: `train={best_row['train_avg_net_ret']:.4f}`, `valid={best_row['valid_avg_net_ret']:.4f}`, `test={best_row['test_avg_net_ret']:.4f}`",
            f"- parameter sensitivity: neighboring-family robustness not yet quantified; use leaderboard rows around `{best_row['family']}` as first-pass sensitivity check",
            "- examples of the best historical events:",
        ]
    )
    for _, row in top_best.iterrows():
        lines.append(
            f"  - `{row['signal_date'].date()} {row['symbol']}`: `net_ret={row['net_ret']:.4f}`, `mfe20={row['mfe_20d']:.4f}`, `mfe40={row['mfe_40d']:.4f}`, `mae20={row['mae_20d']:.4f}`"
        )
    lines.append("- examples of failed events:")
    for _, row in worst_trades.iterrows():
        lines.append(
            f"  - `{row['signal_date'].date()} {row['symbol']}`: `net_ret={row['net_ret']:.4f}`, `mfe20={row['mfe_20d']:.4f}`, `mae20={row['mae_20d']:.4f}`"
        )
    lines.extend(
        [
            "- explanation of why the event may work:",
            *[f"  - {line}" for line in explanation_lines],
            "- risks and failure modes:",
            "  - low sample count can still overstate asymmetry",
            "  - broker behavior may drift when market structure changes",
            "  - some families may depend on a short post-2025 regime",
            "- concentration snapshot:",
        ]
    )
    for sym, cnt in sym_counts.items():
        lines.append(f"  - symbol `{sym}`: `{cnt}` trades")
    for month, cnt in month_counts.items():
        lines.append(f"  - month `{month}`: `{cnt}` trades")
    lines.extend(["", "## Nearby Passing Candidates"])
    same_family = all_rows[(all_rows["family"] == best_row["family"]) & (all_rows["all_pass"])].head(5)
    for _, row in same_family.iterrows():
        lines.append(
            f"- `{row['event_name']}`: `full_avg_net={row['full_avg_net_ret']:.4f}`, `full_mfe40={row['full_avg_mfe_40d']:.4f}`, `test_avg_net={row['test_avg_net_ret']:.4f}`"
        )
    lines.extend(["", "## Rejected Event Families"])
    for _, row in rejected.head(10).iterrows():
        lines.append(f"- `{row['event_name']}`: `{row['rejected_reason']}`")
    return "\n".join(lines)


def build_family_summary(leaderboard: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for family, sub in leaderboard.groupby("family"):
        rows.append(
            {
                "family": family,
                "candidates": int(len(sub)),
                "pass_count": int(sub["all_pass"].fillna(False).sum()),
                "best_full_avg_net_ret": float(sub["full_avg_net_ret"].max()),
                "best_full_avg_mfe_40d": float(sub["full_avg_mfe_40d"].max()),
                "best_test_avg_net_ret": float(sub["test_avg_net_ret"].max()),
                "mean_full_avg_net_ret": float(sub["full_avg_net_ret"].mean()),
                "mean_full_avg_mfe_40d": float(sub["full_avg_mfe_40d"].mean()),
                "mean_test_avg_net_ret": float(sub["test_avg_net_ret"].mean()),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["pass_count", "best_full_avg_net_ret", "best_full_avg_mfe_40d"],
        ascending=[False, False, False],
    )


def build_best_event_concentration(best_trades: pd.DataFrame) -> pd.DataFrame:
    if best_trades.empty:
        return pd.DataFrame(columns=["dimension", "bucket", "trades", "mean_net_ret", "total_net_ret"])

    rows: list[dict[str, object]] = []
    by_symbol = (
        best_trades.groupby("symbol")
        .agg(trades=("symbol", "size"), mean_net_ret=("net_ret", "mean"), total_net_ret=("net_ret", "sum"))
        .reset_index()
    )
    for _, row in by_symbol.iterrows():
        rows.append(
            {
                "dimension": "symbol",
                "bucket": str(row["symbol"]),
                "trades": int(row["trades"]),
                "mean_net_ret": float(row["mean_net_ret"]),
                "total_net_ret": float(row["total_net_ret"]),
            }
        )

    by_month = (
        best_trades.assign(month=best_trades["signal_date"].dt.to_period("M").astype(str))
        .groupby("month")
        .agg(trades=("month", "size"), mean_net_ret=("net_ret", "mean"), total_net_ret=("net_ret", "sum"))
        .reset_index()
    )
    for _, row in by_month.iterrows():
        rows.append(
            {
                "dimension": "month",
                "bucket": str(row["month"]),
                "trades": int(row["trades"]),
                "mean_net_ret": float(row["mean_net_ret"]),
                "total_net_ret": float(row["total_net_ret"]),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Rare-event strategy discovery pipeline")
    ap.add_argument("--output-dir", type=str, default=str(PROJECT_ROOT / "output"))
    args = ap.parse_args()

    out_dir = ensure_dir(Path(args.output_dir))
    feat = build_features()
    feat = feat.dropna(subset=["open", "high", "low", "close"]).copy()
    feat.to_parquet(out_dir / "features_stock_daily.parquet", index=False)

    ohlc_map = build_ohlc_map(feat[["symbol", "date", "open", "high", "low", "close"]].copy())
    specs = _iter_event_specs()

    leaderboard_rows: list[dict[str, object]] = []
    best_signals = pd.DataFrame()
    best_trades = pd.DataFrame()
    best_row: pd.Series | None = None

    for spec in specs:
        mask = _apply_event_spec(feat, spec)
        signals = feat.loc[mask].copy()
        if signals.empty:
            row = summarize_candidate(signals, pd.DataFrame(), spec)
            row["rejected_reason"] = "no_signals"
            leaderboard_rows.append(row)
            continue

        signals = _dedup_cooldown(signals, int(spec["cooldown_days"]))
        signals["event_name"] = _event_name(spec)
        signals["family"] = spec["family"]
        trades = simulate_trades(signals, ohlc_map, int(spec["hold_days"]), CostConfig(), spec.get("stop_loss"))
        row = summarize_candidate(signals, trades, spec)
        leaderboard_rows.append(row)

        if trades.empty:
            continue
        if best_row is None:
            best_row = pd.Series(row)
            best_signals = signals.copy()
            best_trades = trades.copy()
            continue

        current_score = (
            bool(row["all_pass"]),
            float(row["robustness_score"]),
            float(row["full_avg_net_ret"]) if pd.notna(row["full_avg_net_ret"]) else -np.inf,
            float(row["full_avg_mfe_40d"]) if pd.notna(row["full_avg_mfe_40d"]) else -np.inf,
        )
        best_score = (
            bool(best_row["all_pass"]),
            float(best_row["robustness_score"]),
            float(best_row["full_avg_net_ret"]) if pd.notna(best_row["full_avg_net_ret"]) else -np.inf,
            float(best_row["full_avg_mfe_40d"]) if pd.notna(best_row["full_avg_mfe_40d"]) else -np.inf,
        )
        if current_score > best_score:
            best_row = pd.Series(row)
            best_signals = signals.copy()
            best_trades = trades.copy()

    leaderboard = pd.DataFrame(leaderboard_rows).sort_values(
        ["all_pass", "robustness_score", "full_avg_net_ret", "full_avg_mfe_40d", "full_payoff_ratio"],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)
    leaderboard.to_csv(out_dir / "rare_event_leaderboard.csv", index=False)
    build_family_summary(leaderboard).to_csv(out_dir / "rare_event_family_summary.csv", index=False)

    rejected = leaderboard[~leaderboard["all_pass"]].copy()
    rejected.to_csv(out_dir / "rejected_rare_events.csv", index=False)

    if best_row is None:
        best_signals = feat.iloc[0:0].copy()
        best_trades = pd.DataFrame()
    else:
        best_signals = best_signals.sort_values(["date", "symbol"]).reset_index(drop=True)
        best_trades = best_trades.sort_values(["signal_date", "symbol"]).reset_index(drop=True)

    best_signals.to_parquet(out_dir / "rare_event_signals.parquet", index=False)
    best_trades.to_parquet(out_dir / "trades.parquet", index=False)
    build_best_event_concentration(best_trades).to_csv(out_dir / "best_event_concentration.csv", index=False)
    report = build_report(best_row, leaderboard, rejected, best_trades)
    (out_dir / "best_rare_event_report.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
