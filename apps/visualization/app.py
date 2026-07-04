from __future__ import annotations

import os
from pathlib import Path
import re

import pandas as pd

from dash import Dash, dcc, html, Input, Output, State, dash_table, no_update, callback_context
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from stockanalysis.config import resolve_data, resolve_output


# -----------------------------
# Symbol name mapping (latest OHLC csv)
# -----------------------------
_SYMBOL_NAME_MAP: dict[str, str] | None = None
_BROKER_NAME_MAP: dict[str, str] | None = None
_WARRANT_NAME_MAP: dict[str, str] | None = None
GENERAL_BROKER_FLOW_DIR = resolve_output("analysis", "general_broker_flow_anomaly")
BAR_WIDTH_DAYS = 0.7
BAR_WIDTH_MS = int(24 * 60 * 60 * 1000 * BAR_WIDTH_DAYS)


def load_symbol_name_map() -> dict[str, str]:
    global _SYMBOL_NAME_MAP
    if _SYMBOL_NAME_MAP is not None:
        return _SYMBOL_NAME_MAP

    ohlc_dir = resolve_data("ohlc")
    patt = re.compile(r"^(twse|tpex)-(\d{8})\.csv$")
    latest: dict[str, tuple[str, Path]] = {}
    for p in ohlc_dir.glob("*.csv"):
        m = patt.match(p.name)
        if not m:
            continue
        market, ymd = m.group(1), m.group(2)
        if (market not in latest) or (ymd > latest[market][0]):
            latest[market] = (ymd, p)

    mapping: dict[str, str] = {}
    if "twse" in latest:
        _, p = latest["twse"]
        df = pd.read_csv(p, dtype=str)
        if "證券代號" in df.columns and "證券名稱" in df.columns:
            for _, r in df[["證券代號", "證券名稱"]].dropna().iterrows():
                mapping[str(r["證券代號"]).strip()] = str(r["證券名稱"]).strip()

    if "tpex" in latest:
        _, p = latest["tpex"]
        df = pd.read_csv(p, dtype=str)
        if "代號" in df.columns and "名稱" in df.columns:
            for _, r in df[["代號", "名稱"]].dropna().iterrows():
                mapping[str(r["代號"]).strip()] = str(r["名稱"]).strip()

    _SYMBOL_NAME_MAP = mapping
    return mapping


def load_broker_name_map() -> dict[str, str]:
    global _BROKER_NAME_MAP
    if _BROKER_NAME_MAP is not None:
        return _BROKER_NAME_MAP

    broker_list_path = resolve_data("broker_list.csv")
    mapping: dict[str, str] = {}
    if broker_list_path.exists():
        broker_map = pd.read_csv(broker_list_path, dtype=str, encoding="utf-8-sig")[["證券商代號", "證券商名稱"]].dropna()
        broker_map["證券商代號"] = broker_map["證券商代號"].map(normalize_broker_code)
        mapping = dict(zip(broker_map["證券商代號"], broker_map["證券商名稱"].str.strip()))
    _BROKER_NAME_MAP = mapping
    return mapping


def normalize_broker_code(value: object) -> str:
    return str(value or "").strip().upper()


def load_warrant_name_map() -> dict[str, str]:
    global _WARRANT_NAME_MAP
    if _WARRANT_NAME_MAP is not None:
        return _WARRANT_NAME_MAP

    path = resolve_data("warrant", "warrant_list_dedup.csv")
    mapping: dict[str, str] = {}
    if path.exists():
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", usecols=lambda c: c in {"權證代號", "權證簡稱"}).fillna("")
        mapping = dict(zip(df["權證代號"].str.strip(), df["權證簡稱"].str.strip()))
    _WARRANT_NAME_MAP = mapping
    return mapping


def key_branch_dirs() -> list[Path]:
    env_dir = os.getenv("BROKER_ACCUMULATION_OUTPUT_DIR")
    dirs = []
    if env_dir:
        dirs.append(Path(env_dir).expanduser())
    dirs.append(GENERAL_BROKER_FLOW_DIR)
    return dirs


def find_key_branch_dir() -> Path | None:
    for directory in key_branch_dirs():
        if (
            (directory / "general_broker_flow_anomaly_review_cases.parquet").exists()
            and (directory / "general_broker_flow_anomaly_daily_triggers.parquet").exists()
        ):
            return directory
    return None


def _latest_matching_file(directory: Path, pattern: str) -> Path | None:
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _split_tokens(text: object) -> list[str]:
    out: list[str] = []
    for token in str(text or "").split(","):
        token = token.strip()
        if not token or token.lower() == "nan" or token.startswith("+"):
            continue
        out.append(token)
    return out


def _safe_float(value: object) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _fmt_int(value: object) -> str:
    num = _safe_float(value)
    if num is None:
        return "—"
    return f"{num:,.0f}"


def _fmt_pct(value: object) -> str:
    num = _safe_float(value)
    if num is None:
        return "—"
    return f"{num:.2%}"


def _fmt_score(value: object) -> str:
    num = _safe_float(value)
    if num is None:
        return "—"
    return f"{num:,.2f}"


def remove_isolated_price_outliers(ohlcv: pd.DataFrame) -> pd.DataFrame:
    if ohlcv.empty:
        return ohlcv
    close = ohlcv["close"]
    prev_close = close.shift(1)
    next_close = close.shift(-1)
    low_spike = (close < prev_close * 0.7) & (close < next_close * 0.7)
    high_spike = (close > prev_close * 1.3) & (close > next_close * 1.3)
    outliers = (low_spike | high_spike).fillna(False)
    if not outliers.any():
        return ohlcv
    return ohlcv.loc[~outliers].copy()


def load_key_branch_events() -> pd.DataFrame:
    directory = find_key_branch_dir()
    if directory is None:
        return pd.DataFrame()
    path = directory / "general_broker_flow_anomaly_daily_triggers.parquet"
    try:
        events = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if events.empty:
        return events
    events = events.copy()
    events["symbol"] = events["symbol"].astype(str).str.strip()
    events["underlying_stock_id"] = events["symbol"]
    events["date"] = pd.to_datetime(events["date"], errors="coerce")
    events = events.dropna(subset=["date", "symbol"]).copy()
    events["broker_name"] = events["top_buyer"].fillna("").astype(str)
    events["event_type"] = events["pattern"].fillna("").astype(str)
    events["anomaly_score"] = pd.to_numeric(events["pressure_multiple"], errors="coerce").fillna(0.0)
    events["stock_window_net_buy"] = pd.to_numeric(events["recent_net_buy"], errors="coerce").fillna(0.0)
    events["stock_window_branch_share"] = pd.to_numeric(events["recent_mean_top_share"], errors="coerce")
    events["pressure_days"] = pd.to_numeric(events["recent_pressure_days"], errors="coerce")
    events["single_day_share"] = pd.to_numeric(events["max_single_day_pressure_share"], errors="coerce")
    events["price_position"] = pd.to_numeric(events["recent_price_position"], errors="coerce")
    return events.sort_values(["anomaly_score", "date"], ascending=[False, False]).reset_index(drop=True)


def derive_key_branch_cases(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    for (symbol, broker, broker_name), sub in events.groupby(["underlying_stock_id", "broker", "broker_name"], dropna=False):
        sub = sub.sort_values("date")
        top = sub.loc[sub["anomaly_score"].idxmax()]
        rows.append(
            {
                "symbol": str(symbol),
                "broker": str(broker),
                "broker_name": str(broker_name),
                "case_start": sub["date"].min(),
                "case_end": sub["date"].max(),
                "n_events": int(len(sub)),
                "event_types": ",".join(sorted(sub["event_type"].dropna().astype(str).unique())),
                "max_score": top.get("anomaly_score"),
                "top_date": top.get("date"),
                "top_event_type": top.get("event_type"),
                "stock_trigger_any": bool(sub["stock_trigger"].any()) if "stock_trigger" in sub else False,
                "warrant_trigger_any": bool(sub["warrant_trigger"].any()) if "warrant_trigger" in sub else False,
                "stock_window_net_buy_max": sub.get("stock_window_net_buy", pd.Series(dtype=float)).max(),
                "stock_window_branch_share_max": sub.get("stock_window_branch_share", pd.Series(dtype=float)).max(),
                "stock_window_net_ratio_max": sub.get("stock_window_net_ratio", pd.Series(dtype=float)).max(),
                "warrant_window_net_buy_max": sub.get("warrant_window_net_buy", pd.Series(dtype=float)).max(),
                "warrant_window_branch_share_max": sub.get("warrant_window_branch_share", pd.Series(dtype=float)).max(),
                "warrant_window_net_ratio_max": sub.get("warrant_window_net_ratio", pd.Series(dtype=float)).max(),
                "warrant_window_count_max": sub.get("warrant_window_count", pd.Series(dtype=float)).max(),
                "warrant_window_ids_top": top.get("warrant_window_ids", ""),
                "explanation_top": top.get("explanation", ""),
            }
        )
    return pd.DataFrame(rows).sort_values(["max_score", "n_events"], ascending=[False, False]).reset_index(drop=True)


def load_key_branch_cases() -> pd.DataFrame:
    directory = find_key_branch_dir()
    if directory is None:
        return pd.DataFrame()
    path = directory / "general_broker_flow_anomaly_review_cases.parquet"
    try:
        cases = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if cases.empty:
        return cases
    cases = cases.copy()
    cases["symbol"] = cases["symbol"].astype(str).str.strip()
    cases["broker_name"] = cases["primary_buyer"].fillna("").astype(str)
    cases["case_start"] = pd.to_datetime(cases["episode_start"], errors="coerce")
    cases["case_end"] = pd.to_datetime(cases["last_qualifying_date"], errors="coerce")
    cases["top_date"] = cases["case_end"]
    cases["n_events"] = pd.to_numeric(cases["qualifying_dates"], errors="coerce").fillna(0).astype(int)
    cases["top_event_type"] = cases["dominant_pattern"].fillna("").astype(str)
    cases["max_score"] = pd.to_numeric(cases["severity_score"], errors="coerce").fillna(0.0)
    cases["stock_window_net_buy_max"] = pd.to_numeric(cases["max_recent_net_buy"], errors="coerce").fillna(0.0)
    cases["stock_window_branch_share_max"] = pd.to_numeric(cases["primary_buyer_share"], errors="coerce")
    cases["pressure_multiple_max"] = pd.to_numeric(cases["max_pressure_multiple"], errors="coerce")
    cases["single_day_share_max"] = pd.to_numeric(cases["max_single_day_pressure_share"], errors="coerce")
    cases["price_position_median"] = pd.to_numeric(cases["median_price_position"], errors="coerce")
    cases["buyer_names_top"] = cases["buyer_names"].fillna("").astype(str)
    return cases.sort_values(["case_end", "max_score"], ascending=[False, False]).reset_index(drop=True)


def key_branch_source_label() -> str:
    directory = find_key_branch_dir()
    if directory is None:
        return "尚未找到關鍵分點輸出"
    return str(directory)


def key_branch_enabled(value: object) -> bool:
    return isinstance(value, list) and "enabled" in value


def default_stock_id() -> str:
    return "2330"


def format_case_table(cases: pd.DataFrame, limit: int = 80) -> list[dict[str, object]]:
    if cases.empty:
        return []
    out = cases.head(limit).copy()
    for col in ("case_start", "case_end", "top_date"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    for col in ("max_score", "pressure_multiple_max"):
        if col in out.columns:
            out[col] = out[col].map(_fmt_score)
    for col in ("stock_window_net_buy_max",):
        if col in out.columns:
            out[col] = out[col].map(_fmt_int)
    for col in ("stock_window_branch_share_max", "single_day_share_max", "price_position_median"):
        if col in out.columns:
            out[col] = out[col].map(_fmt_pct)
    return out.to_dict("records")


def format_event_table(events: pd.DataFrame, limit: int = 80) -> list[dict[str, object]]:
    if events.empty:
        return []
    out = events.sort_values(["date", "anomaly_score"], ascending=[False, False]).head(limit).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    for col in ("anomaly_score",):
        out[col] = out[col].map(_fmt_score)
    for col in ("stock_window_net_buy",):
        if col in out.columns:
            out[col] = out[col].map(_fmt_int)
    for col in ("stock_window_branch_share", "single_day_share", "price_position"):
        if col in out.columns:
            out[col] = out[col].map(_fmt_pct)
    return out.to_dict("records")


KEY_CASE_STYLE_CELL_CONDITIONAL = [
    {"if": {"column_id": "symbol"}, "minWidth": "64px", "width": "72px"},
    {"if": {"column_id": "n_events"}, "minWidth": "88px", "width": "96px", "textAlign": "right"},
    {"if": {"column_id": "case_start"}, "minWidth": "92px", "width": "100px"},
    {"if": {"column_id": "case_end"}, "minWidth": "92px", "width": "100px"},
]


# -----------------------------
# Real data loader
# -----------------------------
def _resolve_bs_parquet(symbol: str) -> Path | None:
    for p in [resolve_data("bs_report", "parquet_twse", f"{symbol}.parquet"), resolve_data("bs_report", "parquet_tpex", f"{symbol}.parquet")]:
        if p.exists():
            return p
    return None


def load_warrant_broker_data(key_events: pd.DataFrame, min_date: pd.Timestamp | None, max_date: pd.Timestamp | None) -> pd.DataFrame:
    if key_events.empty:
        return pd.DataFrame(columns=["date", "broker", "warrant_id", "warrant_name", "buy", "sell", "net"])

    warrant_ids: set[str] = set()
    for col in ("warrant_window_ids", "warrant_ids"):
        if col in key_events.columns:
            for value in key_events[col].dropna():
                warrant_ids.update(_split_tokens(value))
    if not warrant_ids:
        return pd.DataFrame(columns=["date", "broker", "warrant_id", "warrant_name", "buy", "sell", "net"])

    broker_map = load_broker_name_map()
    warrant_name_map = load_warrant_name_map()
    frames: list[pd.DataFrame] = []
    for warrant_id in sorted(warrant_ids):
        path = _resolve_bs_parquet(warrant_id)
        if path is None:
            continue
        try:
            raw = pd.read_parquet(path, columns=["日期", "券商", "買進股數", "賣出股數"])
        except Exception:
            continue
        if raw.empty:
            continue
        raw = raw.rename(columns={"日期": "date", "券商": "broker", "買進股數": "buy", "賣出股數": "sell"})
        raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
        raw = raw.dropna(subset=["date"])
        if min_date is not None:
            raw = raw[raw["date"] >= min_date]
        if max_date is not None:
            raw = raw[raw["date"] <= max_date]
        if raw.empty:
            continue
        raw_broker = raw["broker"].astype(str).str.strip()
        raw["broker"] = raw_broker.map(normalize_broker_code).map(broker_map).fillna(raw_broker)
        raw["buy"] = pd.to_numeric(raw["buy"], errors="coerce").fillna(0.0)
        raw["sell"] = pd.to_numeric(raw["sell"], errors="coerce").fillna(0.0)
        raw["warrant_id"] = warrant_id
        raw["warrant_name"] = warrant_name_map.get(warrant_id, "")
        frames.append(raw[["date", "broker", "warrant_id", "warrant_name", "buy", "sell"]])

    if not frames:
        return pd.DataFrame(columns=["date", "broker", "warrant_id", "warrant_name", "buy", "sell", "net"])

    out = pd.concat(frames, ignore_index=True)
    out = (
        out.groupby(["date", "broker", "warrant_id", "warrant_name"], as_index=False)[["buy", "sell"]]
        .sum()
        .sort_values(["date", "broker", "warrant_id"])
    )
    out["net"] = out["buy"] - out["sell"]
    return out


def load_data(
    stock_id: str,
    days: int = 240,
    include_key_branch: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    """
    Returns:
      ohlcv_df: columns [date, open, high, low, close, volume]
      broker_df: columns [date, broker, buy, sell, net]
      brokers: list of broker names
    """
    stock_id = str(stock_id).strip()
    ohlc_path = resolve_data("_derived", "ohlc.parquet")
    if not ohlc_path.exists():
        raise FileNotFoundError("Missing data/_derived/ohlc.parquet. Please run Analysis_BsReport_v3.py first.")

    ohlcv = pd.read_parquet(ohlc_path, columns=["symbol", "date", "open", "high", "low", "close", "volume"])
    ohlcv = ohlcv[ohlcv["symbol"] == stock_id].copy()
    if ohlcv.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], []

    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    for c in ["open", "high", "low", "close", "volume"]:
        ohlcv[c] = pd.to_numeric(ohlcv[c], errors="coerce")
    ohlcv["volume"] = ohlcv["volume"].fillna(0)
    ohlcv = ohlcv.sort_values("date")
    ohlcv = remove_isolated_price_outliers(ohlcv)
    if days and days > 0:
        ohlcv = ohlcv.tail(days)

    # Broker data
    broker_path = _resolve_bs_parquet(stock_id)

    if broker_path is None:
        broker_df = pd.DataFrame(columns=["date", "broker", "buy", "sell", "buy_amt", "sell_amt", "net"])
    else:
        broker_raw = pd.read_parquet(broker_path, columns=["日期", "券商", "買進股數", "賣出股數", "價格"])
        broker_raw = broker_raw.rename(columns={"日期": "date", "券商": "broker", "買進股數": "buy", "賣出股數": "sell", "價格": "price"})
        broker_raw["date"] = pd.to_datetime(broker_raw["date"])
        broker_raw["buy"] = pd.to_numeric(broker_raw["buy"], errors="coerce").fillna(0.0)
        broker_raw["sell"] = pd.to_numeric(broker_raw["sell"], errors="coerce").fillna(0.0)
        broker_raw["price"] = pd.to_numeric(broker_raw["price"], errors="coerce").fillna(0.0)
        broker_raw["broker"] = broker_raw["broker"].astype(str).str.strip()
        broker_raw = broker_raw[(broker_raw["broker"] != "") & (broker_raw["broker"] != "nan")]
        broker_raw = broker_raw.dropna(subset=["broker", "date"])

        # Filter broker data to match OHLC range
        min_date = ohlcv["date"].min()
        max_date = ohlcv["date"].max()
        broker_raw = broker_raw[(broker_raw["date"] >= min_date) & (broker_raw["date"] <= max_date)]

        broker_raw["buy_amt"] = broker_raw["price"] * broker_raw["buy"]
        broker_raw["sell_amt"] = broker_raw["price"] * broker_raw["sell"]
        broker_df = (
            broker_raw.groupby(["date", "broker"], as_index=False)[["buy", "sell", "buy_amt", "sell_amt"]]
            .sum()
        )
        broker_df["net"] = broker_df["buy"] - broker_df["sell"]

        broker_map = load_broker_name_map()
        broker_code = broker_df["broker"].astype(str).str.strip().map(normalize_broker_code)
        broker_df["broker"] = broker_code.map(broker_map).fillna(broker_df["broker"])

        broker_df = (
            broker_df.groupby(["date", "broker"], as_index=False)[["buy", "sell", "buy_amt", "sell_amt"]]
            .sum()
        )
        broker_df["net"] = broker_df["buy"] - broker_df["sell"]

    brokers = set(broker_df["broker"].dropna().unique().tolist())

    if include_key_branch:
        all_key_events = load_key_branch_events()
        key_events = all_key_events[all_key_events["underlying_stock_id"].astype(str) == stock_id].copy() if not all_key_events.empty else pd.DataFrame()
        all_key_cases = load_key_branch_cases()
        key_cases = all_key_cases[all_key_cases["symbol"].astype(str) == stock_id].copy() if not all_key_cases.empty else pd.DataFrame()
        warrant_broker_df = load_warrant_broker_data(key_events, ohlcv["date"].min(), ohlcv["date"].max())
        if not warrant_broker_df.empty:
            brokers.update(warrant_broker_df["broker"].dropna().unique().tolist())
        if not key_cases.empty:
            brokers.update(key_cases["broker_name"].dropna().astype(str).tolist())
    else:
        warrant_broker_df = pd.DataFrame(columns=["date", "broker", "warrant_id", "warrant_name", "buy", "sell", "net"])
        key_events = pd.DataFrame()
        key_cases = pd.DataFrame()

    brokers = sorted(b for b in brokers if b)

    # Events for this symbol
    events_path = resolve_data("_derived", "scored.parquet")
    if events_path.exists():
        ev = pd.read_parquet(events_path, columns=["symbol", "date", "is_event"])
        ev = ev[(ev["symbol"] == stock_id) & (ev["is_event"] == 1)].copy()
        event_dates = pd.to_datetime(ev["date"]).dt.date.astype(str).unique().tolist()
    else:
        event_dates = []

    if not key_events.empty:
        event_dates = sorted(set(event_dates) | set(pd.to_datetime(key_events["date"]).dt.date.astype(str).unique().tolist()))

    return ohlcv, broker_df, warrant_broker_df, key_events, key_cases, brokers, event_dates


# -----------------------------
# Helpers
# -----------------------------
def parse_xrange(relayout_data: dict | None) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """
    Plotly relayoutData can be:
      - {"xaxis.range[0]": "...", "xaxis.range[1]": "..."}
      - {"xaxis.range": ["...", "..."]}
      - {"xaxis.autorange": True}
    Return (start, end) timestamps if found, else (None, None).
    """
    if not relayout_data:
        return None, None

    if relayout_data.get("xaxis.autorange") is True:
        return None, None

    if "xaxis.range[0]" in relayout_data and "xaxis.range[1]" in relayout_data:
        try:
            return pd.to_datetime(relayout_data["xaxis.range[0]"]), pd.to_datetime(relayout_data["xaxis.range[1]"])
        except Exception:
            return None, None

    if "xaxis.range" in relayout_data and isinstance(relayout_data["xaxis.range"], (list, tuple)) and len(relayout_data["xaxis.range"]) == 2:
        try:
            return pd.to_datetime(relayout_data["xaxis.range"][0]), pd.to_datetime(relayout_data["xaxis.range"][1])
        except Exception:
            return None, None

    return None, None


def parse_xrange_with_dates(
    relayout_data: dict | None,
    dates: pd.Series,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if relayout_data is None or dates.empty:
        return None, None

    if relayout_data.get("xaxis.autorange") is True:
        return None, None

    # Try category index range
    if "xaxis.range[0]" in relayout_data and "xaxis.range[1]" in relayout_data:
        v0, v1 = relayout_data["xaxis.range[0]"], relayout_data["xaxis.range[1]"]
    elif "xaxis.range" in relayout_data and isinstance(relayout_data["xaxis.range"], (list, tuple)) and len(relayout_data["xaxis.range"]) == 2:
        v0, v1 = relayout_data["xaxis.range"][0], relayout_data["xaxis.range"][1]
    else:
        return None, None

    def _is_numberish(x) -> bool:
        try:
            float(x)
            return True
        except Exception:
            return False

    # Date string range
    if isinstance(v0, str) and not _is_numberish(v0):
        try:
            return pd.to_datetime(v0), pd.to_datetime(v1)
        except Exception:
            return None, None

    # Numeric values: treat large numbers as epoch time
    if _is_numberish(v0) and _is_numberish(v1):
        f0 = float(v0)
        f1 = float(v1)
        # Epoch in ms or s
        if abs(f0) > 1e11 or abs(f1) > 1e11:
            try:
                return pd.to_datetime(f0, unit="ms"), pd.to_datetime(f1, unit="ms")
            except Exception:
                return None, None
        if abs(f0) > 1e9 or abs(f1) > 1e9:
            try:
                return pd.to_datetime(f0, unit="s"), pd.to_datetime(f1, unit="s")
            except Exception:
                return None, None

    # Index range (category axis)
    try:
        i0 = int(round(float(v0)))
        i1 = int(round(float(v1)))
    except Exception:
        try:
            return pd.to_datetime(v0), pd.to_datetime(v1)
        except Exception:
            return None, None

    date_list = sorted(pd.to_datetime(dates.dropna().unique()).tolist())
    if not date_list:
        return None, None

    i0 = max(0, min(i0, len(date_list) - 1))
    i1 = max(0, min(i1, len(date_list) - 1))
    if i0 > i1:
        i0, i1 = i1, i0
    return date_list[i0], date_list[i1]


def filter_by_range(df: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None, date_col: str = "date") -> pd.DataFrame:
    if start is None or end is None or df.empty or date_col not in df.columns:
        return df
    return df[(df[date_col] >= start) & (df[date_col] <= end)]


def add_bollinger_bands(ohlcv: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    if ohlcv.empty:
        return ohlcv
    out = ohlcv.copy()
    bb_mid = out["close"].rolling(window=window, min_periods=window).mean()
    bb_std = out["close"].rolling(window=window, min_periods=window).std()
    out["bb_upper"] = bb_mid + 2 * bb_std
    out["bb_mid"] = bb_mid
    out["bb_lower"] = bb_mid - 2 * bb_std
    return out


def _plot_dates(df: pd.DataFrame, date_col: str = "date") -> list[str]:
    if df.empty or date_col not in df.columns:
        return []
    return pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d").tolist()


def _axis_ticks(dates: list[str], max_ticks: int = 8) -> tuple[list[str], list[str]]:
    if not dates:
        return [], []
    if len(dates) <= max_ticks:
        tick_values = dates
    else:
        indexes = {round(i * (len(dates) - 1) / (max_ticks - 1)) for i in range(max_ticks)}
        indexes.update({0, len(dates) - 1})
        tick_values = [dates[i] for i in sorted(indexes)]
    tick_text = [pd.to_datetime(value).strftime("%m-%d") for value in tick_values]
    return tick_values, tick_text


def _plot_values(series: pd.Series) -> list[object]:
    return series.where(pd.notna(series), None).tolist()


def _net_bar_colors(series: pd.Series) -> list[str]:
    return ["#d24d57" if float(value) >= 0 else "#2e8b57" for value in series.fillna(0)]


def build_figure(
    ohlcv: pd.DataFrame,
    broker_df: pd.DataFrame,
    warrant_broker_df: pd.DataFrame,
    key_events: pd.DataFrame,
    selected_broker: str,
    topn_buy_daily: pd.DataFrame,
    topn_sell_daily: pd.DataFrame,
    topn_label: str,
    event_dates: list[pd.Timestamp],
    show_key_branch: bool,
    broker_net_charts: list[str] | None = None,
) -> go.Figure:
    bb_upper = ohlcv["bb_upper"] if "bb_upper" in ohlcv.columns else pd.Series(index=ohlcv.index, dtype=float)
    bb_mid = ohlcv["bb_mid"] if "bb_mid" in ohlcv.columns else pd.Series(index=ohlcv.index, dtype=float)
    bb_lower = ohlcv["bb_lower"] if "bb_lower" in ohlcv.columns else pd.Series(index=ohlcv.index, dtype=float)
    bar_width_ms = BAR_WIDTH_MS
    ohlcv_x = _plot_dates(ohlcv)
    broker_net_charts = broker_net_charts or []

    row_count = 6 if show_key_branch else 5
    row_heights = [0.40, 0.12, 0.16, 0.16, 0.08, 0.08] if show_key_branch else [0.44, 0.14, 0.18, 0.12, 0.12]
    specs = [[{"type": "candlestick"}]] + [[{"type": "bar"}] for _ in range(row_count - 1)]

    fig = make_subplots(
        rows=row_count,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        specs=specs,
    )

    # Row 1: Candlestick
    pct_change = ohlcv["close"].pct_change()
    pct_display = pct_change.map(lambda x: "" if pd.isna(x) else f"{x:+.2%}").tolist()
    fig.add_trace(
        go.Candlestick(
            x=ohlcv_x,
            open=_plot_values(ohlcv["open"]),
            high=_plot_values(ohlcv["high"]),
            low=_plot_values(ohlcv["low"]),
            close=_plot_values(ohlcv["close"]),
            customdata=pct_display,
            hovertemplate=(
                "開=%{open:.2f}<br>"
                "高=%{high:.2f}<br>"
                "低=%{low:.2f}<br>"
                "收=%{close:.2f}<br>"
                "漲跌%=%{customdata}<extra></extra>"
            ),
            name="K線",
            increasing_line_color="#d24d57",
            decreasing_line_color="#2e8b57",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=ohlcv_x,
            y=_plot_values(bb_upper),
            mode="lines",
            name="BB Upper",
            line=dict(color="#7a7a7a", width=1),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=ohlcv_x,
            y=_plot_values(bb_mid),
            mode="lines",
            name="BB Mid",
            line=dict(color="#aaaaaa", width=1, dash="dot"),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=ohlcv_x,
            y=_plot_values(bb_lower),
            mode="lines",
            name="BB Lower",
            line=dict(color="#7a7a7a", width=1),
        ),
        row=1,
        col=1,
    )

    if show_key_branch and key_events is not None and not key_events.empty:
        ev_summary = (
            key_events.assign(date=pd.to_datetime(key_events["date"], errors="coerce"))
            .dropna(subset=["date"])
            .groupby("date", as_index=False)
            .agg(
                anomaly_score=("anomaly_score", "max"),
                event_type=("event_type", lambda s: ",".join(sorted(set(s.astype(str))))),
                broker_name=("broker_name", lambda s: ",".join(sorted(set(s.astype(str)))[:3])),
            )
        )
        ev = ohlcv.merge(ev_summary, on="date", how="inner")
        if not ev.empty:
            fig.add_trace(
                go.Scatter(
                    x=_plot_dates(ev),
                    y=_plot_values(ev["close"]),
                    mode="markers",
                    name="關鍵分點事件",
                    customdata=ev[["broker_name", "event_type", "anomaly_score"]].values.tolist(),
                    hovertemplate=(
                        "Top買方=%{customdata[0]}<br>"
                        "型態=%{customdata[1]}<br>"
                        "壓力倍數=%{customdata[2]:.2f}<extra></extra>"
                    ),
                    marker=dict(color="#ffb000", size=13, symbol="star", line=dict(color="#7a3f00", width=1)),
                ),
                row=1,
                col=1,
            )
    elif event_dates is not None and len(event_dates) > 0:
        ev = ohlcv[ohlcv["date"].isin(event_dates)].copy()
        if not ev.empty:
            fig.add_trace(
                go.Scatter(
                    x=_plot_dates(ev),
                    y=_plot_values(ev["close"]),
                    mode="markers",
                    name="Event",
                    marker=dict(color="#f8f000", size=12, symbol="star", line=dict(color="#999900", width=1)),
                ),
                row=1,
                col=1,
            )

    # Row 2: Volume
    fig.add_trace(
        go.Bar(
            x=ohlcv_x,
            y=_plot_values(ohlcv["volume"]),
            name="成交量",
            marker_color="#218BE1",
            width=bar_width_ms,
        ),
        row=2,
        col=1,
    )

    # Row 3: Selected broker buy/sell
    bd = broker_df[broker_df["broker"] == selected_broker].sort_values("date")
    if "selected" in broker_net_charts:
        fig.add_trace(
            go.Bar(
                x=_plot_dates(bd),
                y=_plot_values(bd["net"]),
                name=f"{selected_broker} 淨買賣超",
                marker_color=_net_bar_colors(bd["net"]),
                width=bar_width_ms,
                showlegend=False,
            ),
            row=3,
            col=1,
        )
    else:
        fig.add_trace(
            go.Bar(
                x=_plot_dates(bd),
                y=_plot_values(bd["buy"]),
                name=f"{selected_broker} 買入",
                marker_color="#d24d57",
                width=bar_width_ms,
                showlegend=False,
            ),
            row=3,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                x=_plot_dates(bd),
                y=_plot_values(-bd["sell"]),
                name=f"{selected_broker} 賣出",
                marker_color="#2e8b57",
                width=bar_width_ms,
                showlegend=False,
            ),
            row=3,
            col=1,
        )

    top_buy_row = 5
    top_sell_row = 6

    # Row 4: New general broker-flow anomaly pressure.
    if show_key_branch:
        if key_events is not None and not key_events.empty:
            wd = (
                key_events.groupby("date", as_index=False)[["stock_window_net_buy"]]
                .sum()
                .rename(columns={"stock_window_net_buy": "net"})
                .sort_values("date")
            )
        else:
            wd = pd.DataFrame(columns=["date", "net"])
    else:
        wd = pd.DataFrame(columns=["date", "net"])
        top_buy_row = 4
        top_sell_row = 5

    if show_key_branch and not wd.empty:
        fig.add_trace(
            go.Bar(
                x=_plot_dates(wd),
                y=_plot_values(wd["net"]),
                name="新版關鍵買壓",
                marker_color="#b42318",
                width=bar_width_ms,
                showlegend=False,
            ),
            row=4,
            col=1,
        )

    # Top N buy brokers aggregated buy/sell
    if not topn_buy_daily.empty:
        if "top_buy" in broker_net_charts:
            net = topn_buy_daily["buy"] - topn_buy_daily["sell"]
            fig.add_trace(
                go.Bar(
                    x=_plot_dates(topn_buy_daily),
                    y=_plot_values(net),
                    name=f"{topn_label} 淨買賣超",
                    marker_color=_net_bar_colors(net),
                    width=bar_width_ms,
                    showlegend=False,
                ),
                row=top_buy_row,
                col=1,
            )
        else:
            fig.add_trace(
                go.Bar(
                    x=_plot_dates(topn_buy_daily),
                    y=_plot_values(topn_buy_daily["buy"]),
                    name=f"{topn_label} 買入",
                    marker_color="#d24d57",
                    width=bar_width_ms,
                    showlegend=False,
                ),
                row=top_buy_row,
                col=1,
            )
            fig.add_trace(
                go.Bar(
                    x=_plot_dates(topn_buy_daily),
                    y=_plot_values(-topn_buy_daily["sell"]),
                    name=f"{topn_label} 賣出",
                    marker_color="#2e8b57",
                    width=bar_width_ms,
                    showlegend=False,
                ),
                row=top_buy_row,
                col=1,
            )

    # Top N sell brokers aggregated buy/sell
    if not topn_sell_daily.empty:
        if "top_sell" in broker_net_charts:
            net = topn_sell_daily["buy"] - topn_sell_daily["sell"]
            fig.add_trace(
                go.Bar(
                    x=_plot_dates(topn_sell_daily),
                    y=_plot_values(net),
                    name=f"{topn_label} 賣超 淨買賣超",
                    marker_color=_net_bar_colors(net),
                    width=bar_width_ms,
                    showlegend=False,
                ),
                row=top_sell_row,
                col=1,
            )
        else:
            fig.add_trace(
                go.Bar(
                    x=_plot_dates(topn_sell_daily),
                    y=_plot_values(topn_sell_daily["buy"]),
                    name=f"{topn_label} 賣超 買入",
                    marker_color="#d24d57",
                    width=bar_width_ms,
                    showlegend=False,
                ),
                row=top_sell_row,
                col=1,
            )
            fig.add_trace(
                go.Bar(
                    x=_plot_dates(topn_sell_daily),
                    y=_plot_values(-topn_sell_daily["sell"]),
                    name=f"{topn_label} 賣超 賣出",
                    marker_color="#2e8b57",
                    width=bar_width_ms,
                    showlegend=False,
                ),
                row=top_sell_row,
                col=1,
            )
    fig.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        height=1040,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        hoverdistance=100,
        barmode="relative",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(
        matches="x",
        showspikes=False,
        tickformat="%m-%d",
        hoverformat="%Y-%m-%d",
        unifiedhovertitle=dict(text="%{x|%Y-%m-%d}"),
    )
    tick_values, tick_text = _axis_ticks(ohlcv_x)
    if tick_values:
        fig.update_xaxes(tickmode="array", tickvals=tick_values, ticktext=tick_text)

    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_yaxes(title_text="股票分點", row=3, col=1)
    if show_key_branch:
        fig.update_yaxes(title_text="權證分點", row=4, col=1)
    fig.update_yaxes(title_text="Top Buyers", row=top_buy_row, col=1)
    fig.update_yaxes(title_text="Top Sellers", row=top_sell_row, col=1)
    return fig


def build_summary_panel(ohlcv_view: pd.DataFrame, broker_view: pd.DataFrame) -> dict:
    """
    Returns a dict for summary values to display.
    """
    if ohlcv_view.empty:
        return {
            "range": "—",
            "ret": "—",
            "vol_sum": "—",
            "price_last": "—",
        }

    start = ohlcv_view["date"].min().date()
    end = ohlcv_view["date"].max().date()

    first_close = float(ohlcv_view["close"].iloc[0])
    last_close = float(ohlcv_view["close"].iloc[-1])
    ret = (last_close / first_close - 1.0) * 100.0

    vol_sum = int(ohlcv_view["volume"].sum())

    return {
        "range": f"{start} ~ {end}",
        "ret": f"{ret:+.2f}%",
        "vol_sum": f"{vol_sum:,}",
        "price_last": f"{last_close:.2f}",
    }


def top10_tables(broker_view: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aggregate net buy/sell per broker within view range.
    Return (top_buy_df, top_sell_df) with columns [rank, broker, buy, sell, net].
    """
    if broker_view.empty:
        cols = ["rank", "broker", "buy", "sell", "net", "avg_buy", "avg_sell"]
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols)

    agg = (
        broker_view.groupby("broker", as_index=False)[["buy", "sell", "net", "buy_amt", "sell_amt"]]
        .sum()
    )
    agg["avg_buy"] = agg["buy_amt"] / agg["buy"].replace(0, pd.NA)
    agg["avg_sell"] = agg["sell_amt"] / agg["sell"].replace(0, pd.NA)
    agg = agg[["broker", "buy", "sell", "net", "buy_amt", "sell_amt", "avg_buy", "avg_sell"]]
    agg = agg.sort_values("net", ascending=False)

    top_buy = agg.head(10).copy()
    top_buy.insert(0, "rank", range(1, len(top_buy) + 1))
    buy_total = pd.DataFrame(
        [{
            "rank": "",
            "broker": "合計",
            "buy": top_buy["buy"].sum(),
            "sell": top_buy["sell"].sum(),
            "net": top_buy["net"].sum(),
            "avg_buy": top_buy["buy_amt"].sum() / top_buy["buy"].sum() if top_buy["buy"].sum() else pd.NA,
            "avg_sell": top_buy["sell_amt"].sum() / top_buy["sell"].sum() if top_buy["sell"].sum() else pd.NA,
        }]
    )
    top_buy = pd.concat([top_buy, buy_total], ignore_index=True)
    top_buy = top_buy[["rank", "broker", "buy", "sell", "net", "avg_buy", "avg_sell"]]

    top_sell = agg.sort_values("net", ascending=True).head(10).copy()
    top_sell.insert(0, "rank", range(1, len(top_sell) + 1))
    sell_total = pd.DataFrame(
        [{
            "rank": "",
            "broker": "合計",
            "buy": top_sell["buy"].sum(),
            "sell": top_sell["sell"].sum(),
            "net": top_sell["net"].sum(),
            "avg_buy": top_sell["buy_amt"].sum() / top_sell["buy"].sum() if top_sell["buy"].sum() else pd.NA,
            "avg_sell": top_sell["sell_amt"].sum() / top_sell["sell"].sum() if top_sell["sell"].sum() else pd.NA,
        }]
    )
    top_sell = pd.concat([top_sell, sell_total], ignore_index=True)
    top_sell = top_sell[["rank", "broker", "buy", "sell", "net", "avg_buy", "avg_sell"]]

    return top_buy, top_sell


def topn_daily_sum(broker_view: pd.DataFrame, n: int, side: str) -> pd.DataFrame:
    if broker_view.empty:
        return pd.DataFrame(columns=["date", "buy", "sell"])
    n = int(n) if n is not None else 10
    n = max(1, n)
    agg = (
        broker_view.groupby("broker", as_index=False)[["buy", "sell", "net"]]
        .sum()
    )
    if side == "sell":
        agg = agg.sort_values("net", ascending=True)
    else:
        agg = agg.sort_values("net", ascending=False)
    top_brokers = agg.head(n)["broker"].tolist()
    if not top_brokers:
        return pd.DataFrame(columns=["date", "buy", "sell"])
    daily = (
        broker_view[broker_view["broker"].isin(top_brokers)]
        .groupby("date", as_index=False)[["buy", "sell"]]
        .sum()
        .sort_values("date")
    )
    return daily


def build_key_relation_panel(key_cases: pd.DataFrame, key_events: pd.DataFrame, warrant_broker_view: pd.DataFrame, selected_broker: str) -> html.Div:
    if key_cases.empty and key_events.empty:
        return html.Div("這檔股票目前沒有關鍵分點事件資料。", style={"opacity": 0.7, "fontSize": "13px"})

    selected_events = key_events[key_events["broker_name"] == selected_broker].copy() if not key_events.empty else pd.DataFrame()
    event_count = len(selected_events)
    selected_net = selected_events["stock_window_net_buy"].sum() if not selected_events.empty else 0
    max_pressure_multiple = selected_events["anomaly_score"].max() if not selected_events.empty else None

    top_case = key_cases.iloc[0] if not key_cases.empty else None
    top_case_label = "—"
    if top_case is not None:
        top_case_label = (
            f"{top_case.get('broker_name', '')} / {top_case.get('top_event_type', '')} / "
            f"score {_fmt_score(top_case.get('max_score'))}"
        )

    items = [
        html.Div([html.Span("本股最高分 case：", style={"opacity": 0.7}), html.Span(top_case_label)]),
        html.Div([html.Span("目前選定分點：", style={"opacity": 0.7}), html.Span(selected_broker or "—")]),
        html.Div([html.Span("區間內選定分點事件數：", style={"opacity": 0.7}), html.Span(f"{event_count}")]),
        html.Div([html.Span("區間內新版買壓：", style={"opacity": 0.7}), html.Span(_fmt_int(selected_net))]),
        html.Div([html.Span("最高壓力倍數：", style={"opacity": 0.7}), html.Span(_fmt_score(max_pressure_multiple))]),
    ]
    return html.Div(style={"display": "grid", "gap": "6px", "fontSize": "13px"}, children=items)


KEY_CASE_COLUMNS = [
    {"name": "股票", "id": "symbol"},
    {"name": "主要買方", "id": "broker_name"},
    {"name": "開始", "id": "case_start"},
    {"name": "結束", "id": "case_end"},
    {"name": "觸發天數", "id": "n_events"},
    {"name": "型態", "id": "top_event_type"},
    {"name": "Score", "id": "max_score"},
    {"name": "壓力倍數", "id": "pressure_multiple_max"},
    {"name": "買壓", "id": "stock_window_net_buy_max"},
    {"name": "主要占比", "id": "stock_window_branch_share_max"},
    {"name": "單日占比", "id": "single_day_share_max"},
    {"name": "價格位置", "id": "price_position_median"},
]

KEY_EVENT_COLUMNS = [
    {"name": "日期", "id": "date"},
    {"name": "Top 買方", "id": "broker_name"},
    {"name": "型態", "id": "event_type"},
    {"name": "壓力倍數", "id": "anomaly_score"},
    {"name": "買壓", "id": "stock_window_net_buy"},
    {"name": "Top 占比", "id": "stock_window_branch_share"},
    {"name": "單日占比", "id": "single_day_share"},
    {"name": "價格位置", "id": "price_position"},
]


# -----------------------------
# Dash App
# -----------------------------
app = Dash(__name__)
app.title = "籌碼K線"

app.layout = html.Div(
    style={"fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial", "padding": "12px"},
    children=[
        # Header controls
        html.Div(
            style={"display": "flex", "gap": "12px", "alignItems": "center", "marginBottom": "10px"},
            children=[
                html.Div("股票代號：", style={"fontWeight": 600}),
                dcc.Input(
                    id="stock-input",
                    type="text",
                    value=default_stock_id(),
                    debounce=True,
                    style={"width": "110px", "padding": "6px 8px"},
                ),
                html.Div("券商：", style={"fontWeight": 600, "marginLeft": "6px"}),
                dcc.Dropdown(
                    id="broker-dropdown",
                    options=[],
                    value=None,
                    clearable=False,
                    style={"width": "220px"},
                ),
                html.Div("開始：", style={"fontWeight": 600, "marginLeft": "6px"}),
                dcc.DatePickerSingle(
                    id="date-start",
                    date=None,
                    min_date_allowed=None,
                    max_date_allowed=None,
                    display_format="YYYY-MM-DD",
                ),
                html.Div("結束：", style={"fontWeight": 600}),
                dcc.DatePickerSingle(
                    id="date-end",
                    date=None,
                    min_date_allowed=None,
                    max_date_allowed=None,
                    display_format="YYYY-MM-DD",
                ),
                html.Div("TopN：", style={"fontWeight": 600}),
                dcc.Input(
                    id="topn-input",
                    type="number",
                    value=10,
                    min=1,
                    max=200,
                    step=1,
                    style={"width": "80px", "padding": "6px 8px"},
                ),
                dcc.Checklist(
                    id="key-branch-toggle",
                    options=[{"label": "關鍵分點附加功能", "value": "enabled"}],
                    value=[],
                    inputStyle={"marginRight": "6px"},
                    labelStyle={"fontWeight": 600, "whiteSpace": "nowrap"},
                    style={"marginLeft": "6px", "fontSize": "13px"},
                ),
                html.Div(id="hint", style={"marginLeft": "auto", "opacity": 0.75, "fontSize": "12px"}),
            ],
        ),

        html.Div(
            id="key-branch-overview-panel",
            style={"display": "none", "border": "1px solid #dbeafe", "borderRadius": "10px", "padding": "10px", "marginBottom": "12px", "background": "#f8fbff"},
            children=[
                html.Div(
                    style={"display": "flex", "justifyContent": "space-between", "gap": "12px", "alignItems": "baseline", "marginBottom": "8px"},
                    children=[
                        html.Div("新版關鍵分點總覽（點股票列可載入）", style={"fontWeight": 700}),
                        html.Div(f"資料來源：{key_branch_source_label()}", style={"fontSize": "12px", "opacity": 0.65}),
                    ],
                ),
                dash_table.DataTable(
                    id="key-case-overview-table",
                    columns=KEY_CASE_COLUMNS,
                    data=format_case_table(load_key_branch_cases(), limit=50),
                    style_table={"overflowX": "auto", "maxHeight": "260px", "overflowY": "auto"},
                    style_cell={"fontSize": "12px", "padding": "6px", "whiteSpace": "nowrap"},
                    style_cell_conditional=KEY_CASE_STYLE_CELL_CONDITIONAL,
                    style_header={"fontWeight": 700, "background": "#eaf2ff"},
                    sort_action="native",
                    sort_mode="multi",
                    sort_by=[{"column_id": "case_end", "direction": "desc"}],
                    page_action="none",
                    fixed_rows={"headers": True},
                ),
            ],
        ),

        # Main split layout
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "67% 33%", "gap": "12px"},
            children=[
                # Left: chart
                html.Div(
                    style={"border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "8px"},
                    children=[
                        html.Div(
                            style={"display": "flex", "justifyContent": "flex-end", "alignItems": "center", "gap": "10px", "marginBottom": "4px"},
                            children=[
                                html.Div("淨額顯示：", style={"fontWeight": 600, "fontSize": "13px"}),
                                dcc.Checklist(
                                    id="broker-net-chart-options",
                                    options=[
                                        {"label": "股票分點", "value": "selected"},
                                        {"label": "Top Buyers", "value": "top_buy"},
                                        {"label": "Top Sellers", "value": "top_sell"},
                                    ],
                                    value=[],
                                    inputStyle={"marginRight": "4px"},
                                    labelStyle={"display": "inline-block", "marginRight": "10px", "fontSize": "12px"},
                                    style={"whiteSpace": "nowrap"},
                                ),
                            ],
                        ),
                        dcc.Graph(
                            id="main-chart",
                            config={
                                "displayModeBar": True,
                                "scrollZoom": True,
                            },
                        )
                    ],
                ),

                # Right: panel
                html.Div(
                    style={"display": "flex", "flexDirection": "column", "gap": "10px"},
                    children=[
                        html.Div(
                            style={"border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "10px"},
                            children=[
                                html.Div("股票重點資訊", style={"fontWeight": 700, "marginBottom": "8px"}),
                                html.Div(id="summary-box"),
                            ],
                        ),
                        html.Div(
                            id="key-relation-panel",
                            style={"display": "none", "border": "1px solid #fed7aa", "borderRadius": "10px", "padding": "10px", "background": "#fffaf5"},
                            children=[
                                html.Div("新版關鍵分點摘要", style={"fontWeight": 700, "marginBottom": "8px"}),
                                html.Div(id="key-relation-box"),
                            ],
                        ),
                        html.Div(
                            id="key-case-panel",
                            style={"display": "none", "border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "10px"},
                            children=[
                                html.Div("本股關鍵分點 Case", style={"fontWeight": 700, "marginBottom": "8px"}),
                                dash_table.DataTable(
                                    id="key-case-table",
                                    columns=KEY_CASE_COLUMNS,
                                    data=[],
                                    style_table={"overflowX": "auto", "maxHeight": "220px", "overflowY": "auto"},
                                    style_cell={"fontSize": "12px", "padding": "6px", "whiteSpace": "nowrap"},
                                    style_cell_conditional=KEY_CASE_STYLE_CELL_CONDITIONAL,
                                    style_header={"fontWeight": 700},
                                    sort_action="native",
                                    sort_mode="multi",
                                    sort_by=[{"column_id": "case_end", "direction": "desc"}],
                                    page_action="none",
                                ),
                            ],
                        ),
                        html.Div(
                            id="key-event-panel",
                            style={"display": "none", "border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "10px"},
                            children=[
                                html.Div("本股關鍵分點事件", style={"fontWeight": 700, "marginBottom": "8px"}),
                                dash_table.DataTable(
                                    id="key-event-table",
                                    columns=KEY_EVENT_COLUMNS,
                                    data=[],
                                    style_table={"overflowX": "auto", "maxHeight": "260px", "overflowY": "auto"},
                                    style_cell={"fontSize": "12px", "padding": "6px", "whiteSpace": "nowrap"},
                                    style_header={"fontWeight": 700},
                                    page_action="none",
                                ),
                            ],
                        ),
                        html.Div(
                            style={"border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "10px"},
                            children=[
                                html.Div("買超前10大券商", style={"fontWeight": 700, "marginBottom": "8px"}),
                                dash_table.DataTable(
                                    id="top-buy-table",
                                    columns=[
                                        {"name": "排名", "id": "rank"},
                                        {"name": "券商", "id": "broker"},
                                        {"name": "買進", "id": "buy", "type": "numeric", "format": {"specifier": ","}},
                                        {"name": "賣出", "id": "sell", "type": "numeric", "format": {"specifier": ","}},
                                        {"name": "淨買", "id": "net", "type": "numeric", "format": {"specifier": ","}},
                                        {"name": "買進均價", "id": "avg_buy", "type": "numeric", "format": {"specifier": ".2f"}},
                                    ],
                                    data=[],
                                    style_table={"overflowX": "auto"},
                                    style_cell={"fontSize": "12px", "padding": "6px"},
                                    style_header={"fontWeight": 700},
                                    page_action="none",
                                ),
                            ],
                        ),
                        html.Div(
                            style={"border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "10px"},
                            children=[
                                html.Div("賣超前10大券商", style={"fontWeight": 700, "marginBottom": "8px"}),
                                dash_table.DataTable(
                                    id="top-sell-table",
                                    columns=[
                                        {"name": "排名", "id": "rank"},
                                        {"name": "券商", "id": "broker"},
                                        {"name": "買進", "id": "buy", "type": "numeric", "format": {"specifier": ","}},
                                        {"name": "賣出", "id": "sell", "type": "numeric", "format": {"specifier": ","}},
                                        {"name": "淨買", "id": "net", "type": "numeric", "format": {"specifier": ","}},
                                        {"name": "賣出均價", "id": "avg_sell", "type": "numeric", "format": {"specifier": ".2f"}},
                                    ],
                                    data=[],
                                    style_table={"overflowX": "auto"},
                                    style_cell={"fontSize": "12px", "padding": "6px"},
                                    style_header={"fontWeight": 700},
                                    page_action="none",
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),

        # Store current loaded data in browser memory
        dcc.Store(id="store-ohlcv"),
        dcc.Store(id="store-broker"),
        dcc.Store(id="store-warrant-broker"),
        dcc.Store(id="store-key-events"),
        dcc.Store(id="store-key-cases"),
        dcc.Store(id="store-pending-broker"),
        dcc.Store(id="store-brokers"),
        dcc.Store(id="store-events"),
    ],
)


@app.callback(
    Output("key-branch-overview-panel", "style"),
    Output("key-relation-panel", "style"),
    Output("key-case-panel", "style"),
    Output("key-event-panel", "style"),
    Input("key-branch-toggle", "value"),
)
def toggle_key_branch_panels(key_branch_toggle):
    display = "block" if key_branch_enabled(key_branch_toggle) else "none"
    return (
        {"display": display, "border": "1px solid #dbeafe", "borderRadius": "10px", "padding": "10px", "marginBottom": "12px", "background": "#f8fbff"},
        {"display": display, "border": "1px solid #fed7aa", "borderRadius": "10px", "padding": "10px", "background": "#fffaf5"},
        {"display": display, "border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "10px"},
        {"display": display, "border": "1px solid #e5e7eb", "borderRadius": "10px", "padding": "10px"},
    )


@app.callback(
    Output("store-ohlcv", "data"),
    Output("store-broker", "data"),
    Output("store-warrant-broker", "data"),
    Output("store-key-events", "data"),
    Output("store-key-cases", "data"),
    Output("store-brokers", "data"),
    Output("store-events", "data"),
    Output("broker-dropdown", "options"),
    Output("broker-dropdown", "value"),
    Output("date-start", "date"),
    Output("date-end", "date"),
    Output("date-start", "min_date_allowed"),
    Output("date-start", "max_date_allowed"),
    Output("date-end", "min_date_allowed"),
    Output("date-end", "max_date_allowed"),
    Output("hint", "children"),
    Output("store-pending-broker", "data"),
    Input("stock-input", "value"),
    Input("key-branch-toggle", "value"),
    State("store-pending-broker", "data"),
)
def on_stock_change(stock_id: str, key_branch_toggle, pending_broker: str | None):
    stock_id = (stock_id or "").strip()
    if not stock_id:
        return (
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            no_update,
            "請輸入股票代號",
            None,
        )

    include_key_branch = key_branch_enabled(key_branch_toggle)
    ohlcv, broker_df, warrant_broker_df, key_events, key_cases, brokers, event_dates = load_data(
        stock_id,
        include_key_branch=include_key_branch,
    )

    options = [{"label": b, "value": b} for b in brokers]
    pending_broker = str(pending_broker or "").strip()
    if pending_broker and pending_broker in brokers:
        default_broker = pending_broker
    elif not key_cases.empty:
        preferred = str(key_cases.iloc[0].get("broker_name", "") or "")
        default_broker = preferred if preferred in brokers else (brokers[0] if brokers else None)
    else:
        default_broker = brokers[0] if brokers else None

    if not ohlcv.empty:
        min_date = ohlcv["date"].min().date().isoformat()
        max_date = ohlcv["date"].max().date().isoformat()
        end_date = max_date
        start_dt = pd.to_datetime(end_date) - pd.DateOffset(months=3)
        start_date = start_dt.date().isoformat()
        if start_date < min_date:
            start_date = min_date
    else:
        min_date = None
        max_date = None
        start_date = None
        end_date = None

    hint = f"資料筆數：K線 {len(ohlcv)} 天、股票分點 {len(broker_df):,} 筆"
    if include_key_branch:
        hint += f"、新版關鍵事件 {len(key_events):,} 筆"
    return (
        ohlcv.to_dict("records"),
        broker_df.to_dict("records"),
        warrant_broker_df.to_dict("records"),
        key_events.to_dict("records"),
        key_cases.to_dict("records"),
        brokers,
        event_dates,
        options,
        default_broker,
        start_date,
        end_date,
        min_date,
        max_date,
        min_date,
        max_date,
        hint,
        None,
    )


@app.callback(
    Output("main-chart", "figure"),
    Output("summary-box", "children"),
    Output("key-relation-box", "children"),
    Output("key-case-table", "data"),
    Output("key-event-table", "data"),
    Output("top-buy-table", "data"),
    Output("top-sell-table", "data"),
    Input("store-ohlcv", "data"),
    Input("store-broker", "data"),
    Input("store-warrant-broker", "data"),
    Input("store-key-events", "data"),
    Input("store-key-cases", "data"),
    Input("store-events", "data"),
    Input("broker-dropdown", "value"),
    Input("date-start", "date"),
    Input("date-end", "date"),
    Input("topn-input", "value"),
    Input("broker-net-chart-options", "value"),
    Input("key-branch-toggle", "value"),
    Input("main-chart", "relayoutData"),
    State("stock-input", "value"),
)
def render_all(
    ohlcv_data,
    broker_data,
    warrant_broker_data,
    key_events_data,
    key_cases_data,
    event_dates,
    selected_broker,
    start_date,
    end_date,
    topn_value,
    broker_net_charts,
    key_branch_toggle,
    relayout_data,
    stock_id,
):
    if not ohlcv_data or not selected_broker:
        fig = go.Figure()
        fig.update_layout(height=1040, margin=dict(l=10, r=10, t=30, b=10))
        return fig, "—", "—", [], [], [], []

    ohlcv = pd.DataFrame(ohlcv_data)
    broker_df = pd.DataFrame(broker_data or [], columns=["date", "broker", "buy", "sell", "buy_amt", "sell_amt", "net"])
    warrant_broker_df = pd.DataFrame(warrant_broker_data or [], columns=["date", "broker", "warrant_id", "warrant_name", "buy", "sell", "net"])
    key_events = pd.DataFrame(key_events_data or [])
    key_cases = pd.DataFrame(key_cases_data or [])
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    if not broker_df.empty:
        broker_df["date"] = pd.to_datetime(broker_df["date"])
    if not warrant_broker_df.empty:
        warrant_broker_df["date"] = pd.to_datetime(warrant_broker_df["date"])
    if not key_events.empty:
        key_events["date"] = pd.to_datetime(key_events["date"])
    for col in ("case_start", "case_end", "top_date"):
        if not key_cases.empty and col in key_cases.columns:
            key_cases[col] = pd.to_datetime(key_cases[col], errors="coerce")
    event_dt = pd.to_datetime(event_dates) if event_dates else []
    ohlcv = add_bollinger_bands(ohlcv)

    # Filter by date picker range first
    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None
    ohlcv_base = filter_by_range(ohlcv, start_dt, end_dt, "date")
    broker_base = filter_by_range(broker_df, start_dt, end_dt, "date")
    warrant_base = filter_by_range(warrant_broker_df, start_dt, end_dt, "date")
    key_events_base = filter_by_range(key_events, start_dt, end_dt, "date")
    ohlcv_base = ohlcv_base.copy()
    broker_base = broker_base.copy()
    warrant_base = warrant_base.copy()
    key_events_base = key_events_base.copy()

    # Keep broker dates only where OHLC exists
    if not ohlcv_base.empty and not broker_base.empty:
        valid_dates = set(ohlcv_base["date"])
        broker_base = broker_base[broker_base["date"].isin(valid_dates)]
        if not warrant_base.empty:
            warrant_base = warrant_base[warrant_base["date"].isin(valid_dates)]
        if not key_events_base.empty:
            key_events_base = key_events_base[key_events_base["date"].isin(valid_dates)]

    # Further filter by current x-range (zoom/pan)
    start, end = parse_xrange_with_dates(relayout_data, ohlcv_base["date"])
    if not ohlcv_base.empty:
        base_min = ohlcv_base["date"].min()
        base_max = ohlcv_base["date"].max()
        if start is None or start < base_min:
            start = base_min
        if end is None or end > base_max:
            end = base_max
    ohlcv_view = filter_by_range(ohlcv_base, start, end, "date")
    broker_view = filter_by_range(broker_base, start, end, "date")
    warrant_view = filter_by_range(warrant_base, start, end, "date")
    key_events_view = filter_by_range(key_events_base, start, end, "date")

    topn_buy_daily = topn_daily_sum(broker_view, topn_value, "buy")
    topn_sell_daily = topn_daily_sum(broker_view, topn_value, "sell")
    topn_label = f"Top{int(topn_value) if topn_value else 10}"
    show_key_branch = key_branch_enabled(key_branch_toggle)

    # Build chart from the visible window instead of drawing all rows and
    # clipping with an x-axis range. Plotly candlesticks can be clipped badly
    # when the window is a single trading day.
    fig = build_figure(
        ohlcv_view,
        broker_view,
        warrant_view if show_key_branch else pd.DataFrame(),
        key_events_view if show_key_branch else pd.DataFrame(),
        selected_broker,
        topn_buy_daily,
        topn_sell_daily,
        topn_label,
        event_dt,
        show_key_branch,
        broker_net_charts or [],
    )

    # Remove missing dates based on OHLC dates
    if not ohlcv_view.empty:
        all_days = pd.date_range(ohlcv_view["date"].min(), ohlcv_view["date"].max(), freq="D")
        missing = all_days.difference(pd.to_datetime(ohlcv_view["date"].unique()))
        if len(missing) > 0:
            fig.update_xaxes(rangebreaks=[{"values": missing.to_pydatetime().tolist()}])

    # Category x-axis already skips missing dates (union across traces)

    summary = build_summary_panel(ohlcv_view, broker_view)
    name_map = load_symbol_name_map()
    stock_name = name_map.get(str(stock_id), "")
    name_label = f"{stock_id} {stock_name}".strip()
    summary_box = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "6px", "fontSize": "13px"},
        children=[
            html.Div([html.Span("代號：", style={"opacity": 0.7}), html.Span(name_label)]),
            html.Div([html.Span("區間：", style={"opacity": 0.7}), html.Span(summary["range"])]),
            html.Div([html.Span("區間報酬：", style={"opacity": 0.7}), html.Span(summary["ret"])]),
            html.Div([html.Span("收盤(最新)：", style={"opacity": 0.7}), html.Span(summary["price_last"])]),
            html.Div([html.Span("成交量總和：", style={"opacity": 0.7}), html.Span(summary["vol_sum"])]),
        ],
    )

    top_buy, top_sell = top10_tables(broker_view)
    if show_key_branch:
        relation_box = build_key_relation_panel(key_cases, key_events_view, warrant_view, selected_broker)
        key_case_rows = format_case_table(key_cases, limit=30)
        key_event_rows = format_event_table(key_events_view, limit=80)
    else:
        relation_box = "—"
        key_case_rows = []
        key_event_rows = []
    return (
        fig,
        summary_box,
        relation_box,
        key_case_rows,
        key_event_rows,
        top_buy.to_dict("records"),
        top_sell.to_dict("records"),
    )


@app.callback(
    Output("broker-dropdown", "value", allow_duplicate=True),
    Input("top-buy-table", "active_cell"),
    Input("top-sell-table", "active_cell"),
    Input("key-case-table", "active_cell"),
    State("top-buy-table", "data"),
    State("top-sell-table", "data"),
    State("key-case-table", "data"),
    State("key-case-table", "derived_virtual_data"),
    prevent_initial_call=True,
)
def on_table_click(buy_cell, sell_cell, case_cell, buy_data, sell_data, case_data, visible_case_data):
    if not callback_context.triggered:
        return no_update

    trigger = callback_context.triggered[0]["prop_id"].split(".")[0]
    if trigger == "top-buy-table":
        cell = buy_cell
        data = buy_data or []
        broker_col = "broker"
    elif trigger == "top-sell-table":
        cell = sell_cell
        data = sell_data or []
        broker_col = "broker"
    else:
        cell = case_cell
        data = visible_case_data or case_data or []
        broker_col = "broker_name"

    if not cell:
        return no_update

    row = cell.get("row")
    if row is None or row >= len(data):
        return no_update

    broker = data[row].get(broker_col)
    if not broker or broker == "合計":
        return no_update
    return broker


@app.callback(
    Output("stock-input", "value", allow_duplicate=True),
    Output("store-pending-broker", "data", allow_duplicate=True),
    Output("date-start", "date", allow_duplicate=True),
    Output("date-end", "date", allow_duplicate=True),
    Input("key-case-overview-table", "active_cell"),
    State("key-case-overview-table", "data"),
    State("key-case-overview-table", "derived_virtual_data"),
    prevent_initial_call=True,
)
def on_key_case_overview_click(active_cell, data, visible_data):
    if not active_cell or not data:
        return no_update, no_update, no_update, no_update
    rows = visible_data or data
    row = active_cell.get("row")
    if row is None or row >= len(rows):
        return no_update, no_update, no_update, no_update
    selected = rows[row]
    symbol = str(selected.get("symbol", "")).strip()
    broker = str(selected.get("broker_name", "")).strip()
    start = pd.to_datetime(selected.get("case_start"), errors="coerce")
    end = pd.to_datetime(selected.get("case_end"), errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return symbol or no_update, broker or None, no_update, no_update
    date_start = (start - pd.DateOffset(months=1)).date().isoformat()
    date_end = (end + pd.DateOffset(months=1)).date().isoformat()
    return symbol or no_update, broker or None, date_start, date_end


@app.callback(
    Output("date-start", "date", allow_duplicate=True),
    Output("date-end", "date", allow_duplicate=True),
    Input("main-chart", "relayoutData"),
    State("store-ohlcv", "data"),
    prevent_initial_call=True,
)
def sync_date_from_zoom(relayout_data, ohlcv_data):
    if not relayout_data or not ohlcv_data:
        return no_update, no_update
    ohlcv = pd.DataFrame(ohlcv_data)
    if "date" not in ohlcv.columns:
        return no_update, no_update
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    start, end = parse_xrange_with_dates(relayout_data, ohlcv["date"])
    if start is None or end is None:
        return no_update, no_update
    return start.date().isoformat(), end.date().isoformat()


if __name__ == "__main__":
    app.run(debug=True, port=8050)
