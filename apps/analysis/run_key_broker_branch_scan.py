#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scan key-broker branch accumulation rules against future stock returns.

This is a focused research tool for the "key broker branch" strategy line:
detect branch-level abnormal net buying in per-stock broker reports, then
backtest next-open entries with transaction costs.
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

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.stockanalysis.config import ensure_dir, resolve_data, resolve_output


@dataclass(frozen=True)
class CostConfig:
    fee_rate: float = 0.001425
    fee_discount: float = 0.28
    tax_rate_sell: float = 0.003
    slippage: float = 0.0005


def parse_csv_ints(text: str) -> list[int]:
    return [int(x.strip()) for x in text.split(",") if x.strip()]


def parse_csv_floats(text: str) -> list[float]:
    return [float(x.strip()) for x in text.split(",") if x.strip()]


def parse_stop_losses(text: str) -> list[float | None]:
    out: list[float | None] = []
    for token in text.split(","):
        token = token.strip().lower()
        if not token:
            continue
        out.append(None if token in {"none", "null", "na"} else float(token))
    return out


def parse_symbols(text: str | None) -> set[str] | None:
    if not text:
        return None
    values = {x.strip() for x in text.replace("\n", ",").split(",") if x.strip()}
    return values or None


def load_broker_lookup(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    code_col = next((c for c in ("證券商代號", "代號", "broker_id", "code") if c in df.columns), None)
    name_col = next((c for c in ("證券商名稱", "名稱", "broker_name", "name") if c in df.columns), None)
    if not code_col or not name_col:
        return {}
    df[code_col] = df[code_col].astype(str).str.strip()
    df[name_col] = df[name_col].astype(str).str.strip()
    return dict(zip(df[code_col], df[name_col]))


def is_branch_name(name: str, min_suffix_len: int) -> bool:
    name = str(name or "").strip()
    if "-" not in name and "－" not in name:
        return False
    suffix = name.replace("－", "-").rsplit("-", 1)[-1].strip()
    return len(suffix) >= min_suffix_len


def iter_stock_parquets(dirs: list[Path], symbols: set[str] | None, exclude_etf: bool) -> list[Path]:
    files: list[Path] = []
    for directory in dirs:
        for path in sorted(directory.glob("????.parquet")):
            symbol = path.stem
            if symbols is not None and symbol not in symbols:
                continue
            if exclude_etf and symbol.startswith("00"):
                continue
            files.append(path)
    return files


def load_ohlc(ohlc_path: Path, history_start: pd.Timestamp, max_exit_date: pd.Timestamp) -> pd.DataFrame:
    ohlc = pd.read_parquet(
        ohlc_path,
        columns=["symbol", "date", "open", "high", "low", "close", "volume"],
    )
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    ohlc = ohlc[
        (ohlc["symbol"].str.len() == 4)
        & (ohlc["date"] >= history_start)
        & (ohlc["date"] <= max_exit_date)
    ].copy()
    return ohlc.sort_values(["symbol", "date"]).reset_index(drop=True)


def build_ohlc_maps(ohlc: pd.DataFrame) -> tuple[dict[str, pd.DatetimeIndex], dict[str, dict[str, object]]]:
    date_map: dict[str, pd.DatetimeIndex] = {}
    trade_map: dict[str, dict[str, object]] = {}
    for symbol, sub in ohlc.groupby("symbol"):
        sub = sub.sort_values("date").reset_index(drop=True)
        dates = pd.DatetimeIndex(sub["date"])
        date_map[str(symbol)] = dates
        trade_map[str(symbol)] = {
            "dates": dates.to_numpy(dtype="datetime64[ns]"),
            "open": sub["open"].to_numpy(float),
            "high": sub["high"].to_numpy(float),
            "low": sub["low"].to_numpy(float),
            "close": sub["close"].to_numpy(float),
            "date_to_idx": {pd.Timestamp(d): int(i) for i, d in enumerate(dates)},
        }
    return date_map, trade_map


def _read_broker_parquet(path: Path, history_start: pd.Timestamp, signal_end: pd.Timestamp) -> pd.DataFrame:
    try:
        df = pd.read_parquet(
            path,
            columns=["價格", "券商", "日期", "買進股數", "賣出股數"],
            filters=[("日期", ">=", history_start), ("日期", "<=", signal_end)],
        )
    except Exception:
        df = pd.read_parquet(path, columns=["價格", "券商", "日期", "買進股數", "賣出股數"])
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df = df[(df["日期"] >= history_start) & (df["日期"] <= signal_end)].copy()

    if df.empty:
        return df
    df = df.rename(columns=str.strip)
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df["券商"] = df["券商"].astype(str).str.strip()
    df = df[df["日期"].notna() & (df["券商"] != "")].copy()
    for col in ("買進股數", "賣出股數"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def build_symbol_features(
    path: Path,
    trading_dates: pd.DatetimeIndex,
    broker_lookup: dict[str, str],
    args: argparse.Namespace,
    window_days_list: list[int],
) -> pd.DataFrame:
    symbol = path.stem
    raw = _read_broker_parquet(path, args.history_start_ts, args.signal_end_ts)
    if raw.empty:
        return pd.DataFrame()

    daily_all = (
        raw.groupby(["日期", "券商"], as_index=False)
        .agg(buy_shares=("買進股數", "sum"), sell_shares=("賣出股數", "sum"))
        .rename(columns={"日期": "date", "券商": "broker"})
    )
    daily_all["net_shares"] = daily_all["buy_shares"] - daily_all["sell_shares"]
    daily_all["positive_net"] = daily_all["net_shares"].clip(lower=0.0)

    stock_daily = (
        daily_all.groupby("date", as_index=False)
        .agg(stock_buy_shares=("buy_shares", "sum"), stock_positive_net=("positive_net", "sum"))
        .set_index("date")
        .reindex(trading_dates)
        .fillna(0.0)
        .rename_axis("date")
        .reset_index()
    )

    daily_all["broker_name"] = daily_all["broker"].map(broker_lookup).fillna("")
    daily_all["is_branch"] = daily_all["broker_name"].map(lambda x: is_branch_name(x, args.min_branch_suffix_len))
    if args.include_unknown_brokers:
        daily = daily_all[daily_all["is_branch"] | (daily_all["broker_name"] == "")].copy()
    else:
        daily = daily_all[daily_all["is_branch"]].copy()
    if daily.empty:
        return pd.DataFrame()

    brokers = sorted(daily["broker"].dropna().unique())
    full_idx = pd.MultiIndex.from_product([brokers, trading_dates], names=["broker", "date"])
    daily = (
        daily.set_index(["broker", "date"])[["buy_shares", "sell_shares", "net_shares", "positive_net", "broker_name"]]
        .reindex(full_idx)
        .reset_index()
    )
    for col in ("buy_shares", "sell_shares", "net_shares", "positive_net"):
        daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0.0)
    daily["broker_name"] = daily["broker"].map(broker_lookup).fillna("")
    daily["buy_flag"] = (daily["buy_shares"] > 0).astype(float)
    daily["positive_flag"] = (daily["net_shares"] > 0).astype(float)
    daily = daily.sort_values(["broker", "date"]).reset_index(drop=True)

    rows: list[pd.DataFrame] = []
    grouped = daily.groupby("broker", group_keys=False)
    prior_trading_days = grouped.cumcount()
    prior_buy_days = grouped["buy_flag"].cumsum() - daily["buy_flag"]
    prior_positive_sum = grouped["positive_net"].cumsum() - daily["positive_net"]
    history_pos_mean_day = prior_positive_sum / prior_trading_days.replace(0, np.nan)

    for window_days in window_days_list:
        work = daily.copy()
        work["window_days"] = window_days
        work["prior_trading_days"] = prior_trading_days
        work["prior_buy_days"] = prior_buy_days
        work["history_pos_mean_day"] = history_pos_mean_day
        work["expected_window_net"] = history_pos_mean_day * window_days
        work["window_net_sum"] = (
            grouped["positive_net"].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
        )
        work["window_buy_sum"] = (
            grouped["buy_shares"].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
        )
        work["window_positive_days"] = (
            grouped["positive_flag"].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
        )
        work["window_net_buy_ratio"] = np.where(
            work["window_buy_sum"] > 0,
            work["window_net_sum"] / work["window_buy_sum"],
            np.nan,
        )
        work["window_net_ratio"] = np.where(
            work["expected_window_net"] > 0,
            work["window_net_sum"] / work["expected_window_net"],
            np.nan,
        )

        stock = stock_daily.copy()
        stock["stock_buy_window"] = stock["stock_buy_shares"].rolling(window_days, min_periods=window_days).sum()
        stock["stock_positive_net_window"] = stock["stock_positive_net"].rolling(window_days, min_periods=window_days).sum()
        work = work.merge(
            stock[["date", "stock_buy_window", "stock_positive_net_window"]],
            on="date",
            how="left",
        )
        work["branch_buy_share"] = np.where(
            work["stock_buy_window"] > 0,
            work["window_buy_sum"] / work["stock_buy_window"],
            np.nan,
        )
        work["branch_posnet_share"] = np.where(
            work["stock_positive_net_window"] > 0,
            work["window_net_sum"] / work["stock_positive_net_window"],
            np.nan,
        )
        work["score"] = (
            work["window_net_ratio"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            * work["branch_buy_share"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            * np.sqrt(work["window_net_sum"].clip(lower=0.0))
        )
        broad = work[
            (work["date"] >= args.signal_start_ts)
            & (work["date"] <= args.signal_end_ts)
            & (work["prior_buy_days"] >= args.min_history_buy_days)
            & (work["window_net_sum"] >= args.min_window_net)
            & (work["window_positive_days"] >= args.min_positive_days)
            & (work["window_buy_sum"] > 0)
            & (work["score"] > 0)
        ].copy()
        if broad.empty:
            continue
        if args.max_feature_rows_per_symbol_window and args.max_feature_rows_per_symbol_window > 0:
            broad = broad.sort_values("score", ascending=False).head(args.max_feature_rows_per_symbol_window).copy()
        broad.insert(0, "symbol", symbol)
        rows.append(broad)

    if not rows:
        return pd.DataFrame()
    keep_cols = [
        "symbol",
        "date",
        "broker",
        "broker_name",
        "window_days",
        "prior_buy_days",
        "window_net_sum",
        "window_buy_sum",
        "window_positive_days",
        "window_net_buy_ratio",
        "expected_window_net",
        "window_net_ratio",
        "branch_buy_share",
        "branch_posnet_share",
        "score",
    ]
    return pd.concat(rows, ignore_index=True)[keep_cols]


def dedup_and_cooldown(signals: pd.DataFrame, cooldown_days: int) -> pd.DataFrame:
    if signals.empty:
        return signals.copy()
    best_daily = (
        signals.sort_values(["date", "symbol", "score"], ascending=[True, True, False])
        .drop_duplicates(["symbol", "date"], keep="first")
        .sort_values(["symbol", "date", "score"], ascending=[True, True, False])
    )
    kept = []
    for _, sub in best_daily.groupby("symbol", sort=False):
        last_date: pd.Timestamp | None = None
        for _, row in sub.sort_values("date").iterrows():
            date = pd.Timestamp(row["date"])
            if last_date is None or (date - last_date).days >= cooldown_days:
                kept.append(row)
                last_date = date
    if not kept:
        return best_daily.iloc[0:0].copy()
    return pd.DataFrame(kept).reset_index(drop=True)


def simulate_trades(
    signals: pd.DataFrame,
    ohlc_map: dict[str, dict[str, object]],
    hold_days: int,
    stop_loss: float | None,
    cost: CostConfig,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    eff_fee = cost.fee_rate * cost.fee_discount
    for _, signal in signals.iterrows():
        symbol = str(signal["symbol"])
        data = ohlc_map.get(symbol)
        if data is None:
            continue
        idx_map = data["date_to_idx"]
        signal_date = pd.Timestamp(signal["date"])
        i_signal = idx_map.get(signal_date)
        if i_signal is None:
            continue
        i_entry = i_signal + 1
        i_exit = i_entry + hold_days
        i_mfe = i_entry + min(20, hold_days)
        if i_entry >= len(data["open"]) or i_exit >= len(data["close"]):
            continue
        entry_raw = float(data["open"][i_entry])
        exit_raw = float(data["close"][i_exit])
        if not np.isfinite(entry_raw) or not np.isfinite(exit_raw) or entry_raw <= 0:
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
        high = float(np.nanmax(data["high"][i_entry : i_mfe + 1]))
        low = float(np.nanmin(data["low"][i_entry : i_mfe + 1]))
        out = signal.to_dict()
        out.update(
            {
                "signal_date": signal_date,
                "entry_date": pd.Timestamp(data["dates"][i_entry]),
                "exit_date": pd.Timestamp(data["dates"][i_exit]),
                "entry_px_raw": entry_raw,
                "exit_px_raw": exit_raw,
                "net_ret": float(net_ret),
                "mfe": float(high / buy_px - 1.0),
                "mae": float(low / buy_px - 1.0),
                "exit_reason": exit_reason,
            }
        )
        rows.append(out)
    return pd.DataFrame(rows)


def trade_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0,
            "win_rate": np.nan,
            "avg_net_ret": np.nan,
            "median_net_ret": np.nan,
            "profit_factor": np.nan,
            "payoff_ratio": np.nan,
            "avg_mfe": np.nan,
            "avg_mae": np.nan,
            "max_loss": np.nan,
        }
    pos = trades.loc[trades["net_ret"] > 0, "net_ret"]
    neg = trades.loc[trades["net_ret"] < 0, "net_ret"]
    gross_profit = float(pos.sum()) if not pos.empty else 0.0
    gross_loss = float((-neg).sum()) if not neg.empty else 0.0
    avg_win = float(pos.mean()) if not pos.empty else np.nan
    avg_loss = float((-neg).mean()) if not neg.empty else np.nan
    return {
        "trades": int(len(trades)),
        "win_rate": float((trades["net_ret"] > 0).mean()),
        "avg_net_ret": float(trades["net_ret"].mean()),
        "median_net_ret": float(trades["net_ret"].median()),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else math.inf,
        "payoff_ratio": avg_win / avg_loss if np.isfinite(avg_win) and np.isfinite(avg_loss) and avg_loss > 0 else math.inf,
        "avg_mfe": float(trades["mfe"].mean()),
        "avg_mae": float(trades["mae"].mean()),
        "max_loss": float(trades["net_ret"].min()),
    }


def evaluate_spec(features: pd.DataFrame, spec: dict[str, object], ohlc_map: dict[str, dict[str, object]], args: argparse.Namespace) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    mask = (
        (features["window_days"] == spec["window_days"])
        & (features["window_net_ratio"] >= spec["window_net_ratio"])
        & (features["branch_buy_share"] >= spec["branch_buy_share"])
        & (features["window_net_buy_ratio"] >= spec["window_net_buy_ratio"])
        & (features["branch_posnet_share"] >= spec["branch_posnet_share"])
    )
    signals = features.loc[mask].copy()
    signals = dedup_and_cooldown(signals, int(spec["cooldown_days"]))
    trades = simulate_trades(
        signals,
        ohlc_map=ohlc_map,
        hold_days=int(spec["hold_days"]),
        stop_loss=spec["stop_loss"],
        cost=CostConfig(),
    )
    m = trade_metrics(trades)
    target_pass = bool(
        m["trades"] >= args.min_trades
        and m["win_rate"] > 0.50
        and m["avg_net_ret"] > 0.10
    )
    row = {
        "target_pass": target_pass,
        "signal_count": int(len(signals)),
        "params_json": json.dumps(spec, sort_keys=True),
        **spec,
        **m,
    }
    return row, signals, trades


def build_report(best: pd.Series | None, leaderboard: pd.DataFrame, trades: pd.DataFrame, args: argparse.Namespace) -> str:
    lines = [
        "# Key Broker Branch Strategy Scan",
        "",
        f"- signal window: `{args.signal_start}` to `{args.signal_end}`",
        f"- history start: `{args.history_start}`",
        f"- feature rows evaluated: `{args.feature_rows_count}`",
        f"- per symbol/window feature cap: `{args.max_feature_rows_per_symbol_window}`",
        f"- min target gate: `trades >= {args.min_trades}`, `win_rate > 0.50`, `avg_net_ret > 0.10`",
        f"- scanned candidates: `{len(leaderboard)}`",
        f"- passing candidates: `{int(leaderboard['target_pass'].sum()) if not leaderboard.empty else 0}`",
        "",
    ]
    if best is None:
        lines.append("No branch-level candidate produced trades in this run.")
        return "\n".join(lines)

    lines.extend(
        [
            "## Best Candidate",
            "",
            f"- target pass: `{bool(best['target_pass'])}`",
            f"- params: `{best['params_json']}`",
            f"- signals: `{int(best['signal_count'])}`",
            f"- trades: `{int(best['trades'])}`",
            f"- win rate: `{best['win_rate']:.4f}`",
            f"- average net return: `{best['avg_net_ret']:.4f}`",
            f"- median net return: `{best['median_net_ret']:.4f}`",
            f"- profit factor: `{'inf' if not np.isfinite(best['profit_factor']) else format(best['profit_factor'], '.4f')}`",
            f"- payoff ratio: `{'inf' if not np.isfinite(best['payoff_ratio']) else format(best['payoff_ratio'], '.4f')}`",
            f"- average MFE / MAE: `{best['avg_mfe']:.4f}` / `{best['avg_mae']:.4f}`",
            f"- max loss: `{best['max_loss']:.4f}`",
            "",
        ]
    )

    if not trades.empty:
        top = trades.sort_values("net_ret", ascending=False).head(5)
        worst = trades.sort_values("net_ret").head(5)
        sym = trades["symbol"].astype(str).value_counts().head(8)
        broker = trades["broker"].astype(str).value_counts().head(8)
        lines.append("## Best Trades")
        for _, row in top.iterrows():
            lines.append(
                f"- `{row['signal_date'].date()} {row['symbol']} {row['broker_name'] or row['broker']}`: "
                f"`net_ret={row['net_ret']:.4f}`, `mfe={row['mfe']:.4f}`, `mae={row['mae']:.4f}`"
            )
        lines.extend(["", "## Worst Trades"])
        for _, row in worst.iterrows():
            lines.append(
                f"- `{row['signal_date'].date()} {row['symbol']} {row['broker_name'] or row['broker']}`: "
                f"`net_ret={row['net_ret']:.4f}`, `mfe={row['mfe']:.4f}`, `mae={row['mae']:.4f}`"
            )
        lines.extend(["", "## Concentration"])
        for key, value in sym.items():
            lines.append(f"- symbol `{key}`: `{value}` trades")
        for key, value in broker.items():
            lines.append(f"- broker `{key}`: `{value}` trades")

    lines.extend(["", "## Top Leaderboard Rows"])
    for _, row in leaderboard.head(10).iterrows():
        lines.append(
            f"- `{row['params_json']}`: `pass={row['target_pass']}`, "
            f"`trades={int(row['trades'])}`, `win={row['win_rate']:.4f}`, `avg={row['avg_net_ret']:.4f}`"
        )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Scan key-broker branch accumulation strategy rules.")
    ap.add_argument("--broker-dirs", nargs="+", type=Path, default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")])
    ap.add_argument("--ohlc", type=Path, default=resolve_data("_derived", "ohlc.parquet"))
    ap.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    ap.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "key_broker_branch"))
    ap.add_argument("--history-start", default="2025-07-01")
    ap.add_argument("--signal-start", default="2025-10-01")
    ap.add_argument("--signal-end", default="2026-02-03")
    ap.add_argument("--symbols", default=None, help="Comma-separated symbol allowlist.")
    ap.add_argument("--max-symbols", type=int, default=0)
    ap.add_argument("--exclude-etf", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--include-unknown-brokers", action="store_true")
    ap.add_argument("--min-branch-suffix-len", type=int, default=2)
    ap.add_argument("--min-history-buy-days", type=int, default=8)
    ap.add_argument("--min-positive-days", type=float, default=2)
    ap.add_argument("--min-window-net", type=float, default=0.0)
    ap.add_argument("--min-trades", type=int, default=10)
    ap.add_argument(
        "--max-feature-rows-per-symbol-window",
        type=int,
        default=200,
        help="Keep only the strongest anomaly rows per symbol/window before grid evaluation; set 0 to keep all.",
    )
    ap.add_argument("--save-features", action="store_true", help="Persist intermediate feature rows to parquet.")
    ap.add_argument("--cooldown-days-list", default="10,15")
    ap.add_argument("--window-days-list", default="3,5")
    ap.add_argument("--hold-days-list", default="10,16,20")
    ap.add_argument("--stop-loss-list", default="none,-0.10")
    ap.add_argument("--window-net-ratio-list", default="2.0,3.0,5.0")
    ap.add_argument("--branch-buy-share-list", default="0.08,0.12,0.16")
    ap.add_argument("--window-net-buy-ratio-list", default="0.40,0.60")
    ap.add_argument("--branch-posnet-share-list", default="0.10,0.20")
    args = ap.parse_args()

    args.history_start_ts = pd.Timestamp(args.history_start)
    args.signal_start_ts = pd.Timestamp(args.signal_start)
    args.signal_end_ts = pd.Timestamp(args.signal_end)

    window_days_list = parse_csv_ints(args.window_days_list)
    hold_days_list = parse_csv_ints(args.hold_days_list)
    cooldown_days_list = parse_csv_ints(args.cooldown_days_list)
    stop_loss_list = parse_stop_losses(args.stop_loss_list)
    max_hold = max(hold_days_list)
    max_exit = args.signal_end_ts + pd.Timedelta(days=max_hold * 3)

    out_dir = ensure_dir(args.output_dir)
    broker_lookup = load_broker_lookup(args.broker_list)
    ohlc = load_ohlc(args.ohlc, args.history_start_ts, max_exit)
    date_map, ohlc_map = build_ohlc_maps(ohlc)

    symbols = parse_symbols(args.symbols)
    files = iter_stock_parquets(args.broker_dirs, symbols, args.exclude_etf)
    files = [p for p in files if p.stem in date_map]
    if args.max_symbols and args.max_symbols > 0:
        files = files[: args.max_symbols]

    feature_parts: list[pd.DataFrame] = []
    for i, path in enumerate(files, start=1):
        feat = build_symbol_features(path, date_map[path.stem], broker_lookup, args, window_days_list)
        if not feat.empty:
            feature_parts.append(feat)
        if i % 100 == 0:
            print(f"processed {i}/{len(files)} files, feature_parts={len(feature_parts)}", flush=True)

    features = pd.concat(feature_parts, ignore_index=True) if feature_parts else pd.DataFrame()
    args.feature_rows_count = int(len(features))
    if args.save_features:
        features.to_parquet(out_dir / "key_broker_branch_features.parquet", index=False)

    specs: list[dict[str, object]] = []
    for values in itertools.product(
        window_days_list,
        hold_days_list,
        cooldown_days_list,
        stop_loss_list,
        parse_csv_floats(args.window_net_ratio_list),
        parse_csv_floats(args.branch_buy_share_list),
        parse_csv_floats(args.window_net_buy_ratio_list),
        parse_csv_floats(args.branch_posnet_share_list),
    ):
        specs.append(
            {
                "window_days": values[0],
                "hold_days": values[1],
                "cooldown_days": values[2],
                "stop_loss": values[3],
                "window_net_ratio": values[4],
                "branch_buy_share": values[5],
                "window_net_buy_ratio": values[6],
                "branch_posnet_share": values[7],
            }
        )

    rows: list[dict[str, object]] = []
    best_row: pd.Series | None = None
    best_signals = pd.DataFrame()
    best_trades = pd.DataFrame()
    for spec in specs:
        row, signals, trades = evaluate_spec(features, spec, ohlc_map, args)
        rows.append(row)
        score = (
            bool(row["target_pass"]),
            float(row["avg_net_ret"]) if pd.notna(row["avg_net_ret"]) else -np.inf,
            float(row["win_rate"]) if pd.notna(row["win_rate"]) else -np.inf,
            int(row["trades"]),
        )
        if best_row is None:
            best_row = pd.Series(row)
            best_signals = signals
            best_trades = trades
            continue
        best_score = (
            bool(best_row["target_pass"]),
            float(best_row["avg_net_ret"]) if pd.notna(best_row["avg_net_ret"]) else -np.inf,
            float(best_row["win_rate"]) if pd.notna(best_row["win_rate"]) else -np.inf,
            int(best_row["trades"]),
        )
        if score > best_score:
            best_row = pd.Series(row)
            best_signals = signals
            best_trades = trades

    leaderboard = pd.DataFrame(rows)
    if not leaderboard.empty:
        leaderboard = leaderboard.sort_values(
            ["target_pass", "avg_net_ret", "win_rate", "trades", "profit_factor"],
            ascending=[False, False, False, False, False],
        ).reset_index(drop=True)
    leaderboard.to_csv(out_dir / "key_broker_branch_leaderboard.csv", index=False)
    best_signals.to_parquet(out_dir / "best_key_broker_branch_signals.parquet", index=False)
    best_trades.to_csv(out_dir / "best_key_broker_branch_trades.csv", index=False)
    report = build_report(best_row, leaderboard, best_trades, args)
    (out_dir / "key_broker_branch_report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
