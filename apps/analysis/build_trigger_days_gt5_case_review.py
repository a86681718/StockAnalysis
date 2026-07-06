from __future__ import annotations

import argparse
from email.utils import parsedate_to_datetime
import html
import json
import math
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests

from stockanalysis.analysis.broker_branch_accumulation import (
    build_parquet_index,
    load_broker_lookup,
    read_broker_parquet,
)
from stockanalysis.analysis.regional_branch_accumulation import extract_city, load_company_profiles
from stockanalysis.config import ensure_dir, resolve_data, resolve_output


@dataclass(frozen=True)
class CaseReviewConfig:
    episodes_path: Path
    daily_triggers_path: Path
    general_cases_path: Path
    ohlc_path: Path
    broker_list_path: Path
    company_profile_paths: tuple[Path, ...]
    broker_dirs: tuple[Path, ...]
    output_dir: Path
    min_trigger_days: int
    max_cases: int
    detail_case_limit: int
    fetch_news: bool
    max_news_items: int
    news_timeout_seconds: float


def _num(value: object, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_int(value: object) -> str:
    return f"{_num(value):,.0f}"


def _fmt_float(value: object, digits: int = 2) -> str:
    number = _num(value, math.nan)
    if math.isnan(number):
        return "n/a"
    return f"{number:.{digits}f}"


def _fmt_pct(value: object, digits: int = 1) -> str:
    number = _num(value, math.nan)
    if math.isnan(number):
        return "n/a"
    return f"{number * 100:.{digits}f}%"


def _date(value: object) -> str:
    if pd.isna(value):
        return ""
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _risk_label(score: float) -> str:
    if score >= 4.0:
        return "可列入買進觀察"
    if score >= 2.0:
        return "觀察但需等確認"
    return "暫不追價"


def _detail_path(rank: object, symbol: str, broker: str, episode_start: object) -> str:
    return f"all_cases/{int(_num(rank)):04d}_{symbol}_{broker}_{_date(episode_start)}.html"


def _load_stock_names(paths: tuple[Path, ...]) -> dict[str, str]:
    profiles = load_company_profiles(paths)
    if profiles.empty:
        return {}
    return dict(zip(profiles["symbol"].astype(str), profiles["company_name"].astype(str)))


def _load_broker_cities(path: Path) -> dict[str, str]:
    raw = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    code_col = next((col for col in ("證券商代號", "代號", "broker_id", "code") if col in raw.columns), None)
    address_col = next((col for col in ("地址", "address") if col in raw.columns), None)
    if not code_col or not address_col:
        return {}
    return dict(zip(raw[code_col].astype(str).str.strip(), raw[address_col].map(extract_city)))


def _load_ohlc_context(path: Path, symbols: set[str]) -> pd.DataFrame:
    ohlc = pd.read_parquet(path, columns=["symbol", "date", "open", "high", "low", "close", "volume"])
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc = ohlc[ohlc["symbol"].isin(symbols)].copy()
    ohlc["date"] = pd.to_datetime(ohlc["date"], errors="coerce").dt.normalize()
    ohlc = ohlc.sort_values(["symbol", "date"]).reset_index(drop=True)
    grouped = ohlc.groupby("symbol", group_keys=False)
    for window in (5, 20, 60):
        ohlc[f"ma{window}"] = grouped["close"].transform(lambda s: s.rolling(window, min_periods=min(5, window)).mean())
        ohlc[f"ret{window}"] = grouped["close"].transform(lambda s: s / s.shift(window) - 1.0)
    low60 = grouped["low"].transform(lambda s: s.rolling(60, min_periods=20).min())
    high60 = grouped["high"].transform(lambda s: s.rolling(60, min_periods=20).max())
    ohlc["price_position_60"] = (ohlc["close"] - low60) / (high60 - low60).replace(0, np.nan)
    return ohlc


def _trend_snapshot(ohlc: pd.DataFrame, symbol: str, as_of: pd.Timestamp) -> dict[str, object]:
    sub = ohlc[(ohlc["symbol"] == symbol) & (ohlc["date"] <= as_of)].tail(80)
    if sub.empty:
        return {
            "latest_close": np.nan,
            "short_trend": "n/a",
            "mid_trend": "n/a",
            "long_trend": "n/a",
            "ret5": np.nan,
            "ret20": np.nan,
            "ret60": np.nan,
            "price_position_60": np.nan,
        }
    latest = sub.iloc[-1]
    close = _num(latest["close"], math.nan)
    ma5 = _num(latest.get("ma5"), math.nan)
    ma20 = _num(latest.get("ma20"), math.nan)
    ma60 = _num(latest.get("ma60"), math.nan)
    return {
        "latest_close": close,
        "short_trend": "多" if close >= ma5 >= ma20 else "弱" if close < ma5 < ma20 else "盤整",
        "mid_trend": "多" if close >= ma20 else "弱",
        "long_trend": "多" if close >= ma60 else "弱",
        "ret5": _num(latest.get("ret5"), math.nan),
        "ret20": _num(latest.get("ret20"), math.nan),
        "ret60": _num(latest.get("ret60"), math.nan),
        "price_position_60": _num(latest.get("price_position_60"), math.nan),
    }


def _ohlc_chart_rows(ohlc: pd.DataFrame, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> list[dict[str, object]]:
    symbol_ohlc = ohlc[ohlc["symbol"] == symbol].copy()
    before = symbol_ohlc[symbol_ohlc["date"] < start].tail(60)
    during = symbol_ohlc[(symbol_ohlc["date"] >= start) & (symbol_ohlc["date"] <= end)]
    after = symbol_ohlc[symbol_ohlc["date"] > end].head(10)
    window = pd.concat([before, during, after], ignore_index=True)
    rows: list[dict[str, object]] = []
    for row in window.itertuples(index=False):
        date = pd.Timestamp(row.date)
        rows.append(
            {
                "date": _date(date),
                "open": _num(row.open, math.nan),
                "high": _num(row.high, math.nan),
                "low": _num(row.low, math.nan),
                "close": _num(row.close, math.nan),
                "volume": _num(row.volume, math.nan),
                "ma5": _num(getattr(row, "ma5", np.nan), math.nan),
                "ma20": _num(getattr(row, "ma20", np.nan), math.nan),
                "ma60": _num(getattr(row, "ma60", np.nan), math.nan),
                "in_episode": bool(start <= date <= end),
            }
        )
    return rows


def _broker_daily_rows(raw: pd.DataFrame, target_broker: str) -> list[dict[str, object]]:
    if raw.empty:
        return []
    work = raw.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["net_buy"] = work["buy_volume"] - work["sell_volume"]
    target = work[work["broker"] == target_broker].groupby("date", as_index=False).agg(
        target_buy=("buy_volume", "sum"),
        target_sell=("sell_volume", "sum"),
        target_net=("net_buy", "sum"),
        target_buy_amount=("buy_amount", "sum"),
    )
    total = work.groupby("date", as_index=False).agg(
        market_buy=("buy_volume", "sum"),
        market_sell=("sell_volume", "sum"),
        market_net=("net_buy", "sum"),
    )
    daily = total.merge(target, on="date", how="left").fillna(0).sort_values("date")
    rows: list[dict[str, object]] = []
    for row in daily.itertuples(index=False):
        avg_price = row.target_buy_amount / row.target_buy if row.target_buy > 0 else np.nan
        rows.append(
            {
                "date": _date(row.date),
                "target_buy": float(row.target_buy),
                "target_sell": float(row.target_sell),
                "target_net": float(row.target_net),
                "target_avg_buy_price": float(avg_price) if not pd.isna(avg_price) else None,
                "market_buy": float(row.market_buy),
                "market_sell": float(row.market_sell),
                "market_net": float(row.market_net),
            }
        )
    return rows


def _broker_window_stats(
    raw: pd.DataFrame,
    broker_lookup: dict[str, str],
    broker_cities: dict[str, str],
    target_broker: str,
) -> dict[str, object]:
    if raw.empty:
        return {
            "dominant_city": "",
            "dominant_city_share": np.nan,
            "top_brokers": [],
            "target_avg_buy_price": np.nan,
            "target_net_buy": np.nan,
            "same_broker_cross_ratio": np.nan,
            "left_right_flag": False,
        }
    work = raw.copy()
    work["net_buy"] = work["buy_volume"] - work["sell_volume"]
    work["broker_name"] = work["broker"].map(broker_lookup).fillna(work["broker"])
    work["broker_city"] = work["broker"].map(broker_cities).fillna("")
    positive = work[work["net_buy"] > 0].copy()
    top_brokers: list[dict[str, object]] = []
    if not positive.empty:
        broker_totals = positive.groupby(["broker", "broker_name", "broker_city"], as_index=False).agg(
            buy_volume=("buy_volume", "sum"),
            sell_volume=("sell_volume", "sum"),
            net_buy=("net_buy", "sum"),
            buy_amount=("buy_amount", "sum"),
        )
        broker_totals["avg_buy_price"] = broker_totals["buy_amount"] / broker_totals["buy_volume"].replace(0, np.nan)
        broker_totals = broker_totals.sort_values("net_buy", ascending=False)
        for row in broker_totals.head(10).itertuples(index=False):
            top_brokers.append(
                {
                    "broker": row.broker,
                    "broker_name": row.broker_name,
                    "broker_city": row.broker_city,
                    "net_buy": float(row.net_buy),
                    "avg_buy_price": float(row.avg_buy_price) if not pd.isna(row.avg_buy_price) else None,
                }
            )
        total_net = float(broker_totals["net_buy"].sum())
        by_city = broker_totals[broker_totals["broker_city"].ne("")].groupby("broker_city")["net_buy"].sum()
        if not by_city.empty and total_net > 0:
            dominant_city = str(by_city.idxmax())
            dominant_city_share = float(by_city.max() / total_net)
        else:
            dominant_city = ""
            dominant_city_share = np.nan
    else:
        dominant_city = ""
        dominant_city_share = np.nan

    target = work[work["broker"] == target_broker]
    target_buy = float(target["buy_volume"].sum()) if not target.empty else 0.0
    target_sell = float(target["sell_volume"].sum()) if not target.empty else 0.0
    target_avg = float(target["buy_amount"].sum() / target_buy) if target_buy > 0 else np.nan
    target_net = target_buy - target_sell
    paired = np.minimum(work["buy_volume"], work["sell_volume"]).sum()
    gross = (work["buy_volume"] + work["sell_volume"]).sum()
    cross_ratio = float((2 * paired) / gross) if gross > 0 else np.nan
    left_right_flag = bool((not math.isnan(cross_ratio) and cross_ratio >= 0.55) and abs(float(work["net_buy"].sum())) < float(work["buy_volume"].sum()) * 0.25)
    return {
        "dominant_city": dominant_city,
        "dominant_city_share": dominant_city_share,
        "top_brokers": top_brokers,
        "target_avg_buy_price": target_avg,
        "target_net_buy": target_net,
        "same_broker_cross_ratio": cross_ratio,
        "left_right_flag": left_right_flag,
    }


def _score_case(row: pd.Series, trend: dict[str, object], broker_stats: dict[str, object], general_hit: bool) -> tuple[float, list[str], list[str]]:
    positives: list[str] = []
    risks: list[str] = []
    score = 0.0
    if _num(row["qualifying_dates"]) >= 20:
        score += 1.2
        positives.append("觸發天數達 20 日以上，持續性強")
    elif _num(row["qualifying_dates"]) >= 10:
        score += 0.8
        positives.append("觸發天數達 10 日以上")
    else:
        score += 0.4
    if _num(row["cumulative_purity"]) >= 0.95:
        score += 0.8
        positives.append("累積買超純度高")
    if _num(row["cumulative_buy_share"]) >= 0.15:
        score += 0.7
        positives.append("分點買盤占全股買量比例高")
    if str(row.get("status", "")) == "accelerating":
        score += 0.8
        positives.append("最近狀態仍在加速")
    elif str(row.get("status", "")) == "unwinding":
        score -= 1.0
        risks.append("狀態顯示已有退潮")
    if trend["short_trend"] == "多":
        score += 0.6
        positives.append("短線站上均線結構")
    elif trend["short_trend"] == "弱":
        score -= 0.6
        risks.append("短線趨勢偏弱")
    if trend["mid_trend"] == "多":
        score += 0.4
    if _num(trend["price_position_60"], math.nan) >= 0.85:
        score -= 0.5
        risks.append("60 日位置偏高，追價風險較高")
    if _num(broker_stats["dominant_city_share"], math.nan) >= 0.55:
        score += 0.4
        positives.append("買方券商有地區群聚")
    if broker_stats["left_right_flag"]:
        score -= 0.9
        risks.append("買賣同時放大，疑似左手換右手需人工查核")
    if general_hit:
        score += 0.6
        positives.append("也出現在一般主力流異常清單")
    avg_price = _num(broker_stats["target_avg_buy_price"], math.nan)
    latest_close = _num(trend["latest_close"], math.nan)
    if not math.isnan(avg_price) and not math.isnan(latest_close):
        premium = latest_close / avg_price - 1.0
        if premium <= 0.08:
            score += 0.5
            positives.append("現價接近主力估計均價")
        elif premium >= 0.25:
            score -= 0.8
            risks.append("現價已大幅高於主力估計均價")
    return score, positives, risks


def _news_links(symbol: str, name: str) -> list[dict[str, str]]:
    query = f"{symbol} {name} 最新 新聞"
    encoded = quote_plus(query)
    return [
        {"label": "Google News", "url": f"https://news.google.com/search?q={encoded}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"},
        {"label": "Yahoo 股市新聞", "url": f"https://tw.stock.yahoo.com/quote/{symbol}.TW/news"},
        {"label": "公開資訊觀測站", "url": f"https://mops.twse.com.tw/mops/web/t05st10_ifrs"},
    ]


def _load_news_cache(path: Path) -> dict[str, list[dict[str, str]]]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): value for key, value in raw.items() if isinstance(value, list)}


def _write_news_cache(path: Path, cache: dict[str, list[dict[str, str]]]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _news_cache_key(symbol: str, name: str) -> str:
    return f"{symbol}:{name}"


def _parse_rss_date(value: str) -> str:
    if not value:
        return ""
    try:
        return parsedate_to_datetime(value).date().isoformat()
    except (TypeError, ValueError, IndexError, AttributeError):
        return ""


def _fetch_google_news_items(symbol: str, name: str, max_items: int, timeout: float) -> list[dict[str, str]]:
    query = f"{symbol} {name} 最新 新聞"
    url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    response = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    root = ET.fromstring(response.content)
    items: list[dict[str, str]] = []
    for item in root.findall("./channel/item"):
        source_node = item.find("source")
        title = html.unescape((item.findtext("title") or "").strip())
        source = html.unescape((source_node.text or "").strip()) if source_node is not None else ""
        link = (item.findtext("link") or "").strip()
        published = _parse_rss_date((item.findtext("pubDate") or "").strip())
        if not title:
            continue
        items.append({"title": title, "source": source, "published": published, "url": link})
        if len(items) >= max_items:
            break
    return items


def _news_items_for_case(
    symbol: str,
    name: str,
    cfg: CaseReviewConfig,
    cache: dict[str, list[dict[str, str]]],
) -> list[dict[str, str]]:
    if not cfg.fetch_news:
        return []
    key = _news_cache_key(symbol, name)
    if key in cache:
        return cache[key]
    try:
        items = _fetch_google_news_items(symbol, name, cfg.max_news_items, cfg.news_timeout_seconds)
    except (requests.RequestException, ET.ParseError):
        items = []
    cache[key] = items
    return items


def _json_default(value: object) -> object:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if math.isnan(float(value)):
            return None
        return float(value)
    if isinstance(value, (pd.Timestamp,)):
        return _date(value)
    return str(value)


def _slim_cases(cases: list[dict[str, object]]) -> list[dict[str, object]]:
    heavy_keys = {"ohlc_chart", "broker_daily"}
    return [{key: value for key, value in case.items() if key not in heavy_keys} for case in cases]


def _build_universe_index(trigger_gt5: pd.DataFrame, selected: pd.DataFrame) -> pd.DataFrame:
    selected_keys = set(
        zip(
            selected["symbol"].astype(str),
            selected["broker"].astype(str),
            pd.to_datetime(selected["episode_start"]).dt.strftime("%Y-%m-%d"),
        )
    )
    out = trigger_gt5.copy()
    out["symbol"] = out["symbol"].astype(str)
    out["broker"] = out["broker"].astype(str)
    out["episode_start_text"] = pd.to_datetime(out["episode_start"]).dt.strftime("%Y-%m-%d")
    out["selected_for_detail"] = [
        (symbol, broker, start) in selected_keys
        for symbol, broker, start in zip(out["symbol"], out["broker"], out["episode_start_text"])
    ]
    out["scope_note"] = np.select(
        [
            out["selected_for_detail"],
            out["review_eligible"],
            out["ongoing"],
        ],
        [
            "detailed_review_case",
            "review_eligible_beyond_limit",
            "ongoing_but_not_review_eligible",
        ],
        default="historical_or_ended_episode",
    )
    out["detail_path"] = [
        _detail_path(rank, symbol, broker, start)
        for rank, symbol, broker, start in zip(out["rank"], out["symbol"], out["broker"], out["episode_start"])
    ]
    keep = [
        "rank",
        "symbol",
        "broker",
        "broker_name",
        "episode_start",
        "last_qualifying_date",
        "status_as_of",
        "status",
        "ongoing",
        "confidence",
        "qualifying_dates",
        "observed_sessions",
        "positive_net_sessions",
        "cumulative_net_buy",
        "cumulative_purity",
        "cumulative_buy_share",
        "review_eligible",
        "selected_for_detail",
        "scope_note",
        "detail_path",
    ]
    return out[keep].sort_values(["selected_for_detail", "rank"], ascending=[False, True]).reset_index(drop=True)


def build_cases(cfg: CaseReviewConfig) -> tuple[list[dict[str, object]], dict[str, object], pd.DataFrame]:
    episodes = pd.read_parquet(cfg.episodes_path)
    episodes["episode_start"] = pd.to_datetime(episodes["episode_start"]).dt.normalize()
    episodes["last_qualifying_date"] = pd.to_datetime(episodes["last_qualifying_date"]).dt.normalize()
    trigger_gt5 = episodes[episodes["qualifying_dates"] > cfg.min_trigger_days].copy()
    eligible_gt5 = trigger_gt5[trigger_gt5["review_eligible"]].copy()
    selected = eligible_gt5.sort_values(["rank"]).head(cfg.max_cases).reset_index(drop=True)
    universe = _build_universe_index(trigger_gt5, selected)
    universe_by_key = {
        (str(row.symbol), str(row.broker), _date(row.episode_start)): row
        for row in universe.itertuples(index=False)
    }
    review_rows = trigger_gt5.sort_values(["review_eligible", "ongoing", "rank"], ascending=[False, False, True]).reset_index(drop=True)
    symbols = set(review_rows["symbol"].astype(str))
    stock_names = _load_stock_names(cfg.company_profile_paths)
    broker_lookup = load_broker_lookup(cfg.broker_list_path)
    broker_cities = _load_broker_cities(cfg.broker_list_path)
    ohlc = _load_ohlc_context(cfg.ohlc_path, symbols)
    parquet_index = build_parquet_index(cfg.broker_dirs)
    general = pd.read_parquet(cfg.general_cases_path)
    general_hits = set(general.loc[general["qualifying_dates"] > cfg.min_trigger_days, "symbol"].astype(str))
    news_cache_path = cfg.output_dir / "news_cache.json"
    news_cache = _load_news_cache(news_cache_path)

    cases: list[dict[str, object]] = []
    for row in review_rows.itertuples(index=False):
        symbol = str(row.symbol)
        start = pd.Timestamp(row.episode_start)
        end = pd.Timestamp(row.last_qualifying_date)
        key = (symbol, str(row.broker), _date(start))
        universe_row = universe_by_key[key]
        frames = [read_broker_parquet(path, start, end) for path in parquet_index.get(symbol, [])]
        raw = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True) if frames else pd.DataFrame()
        trend = _trend_snapshot(ohlc, symbol, end)
        chart_rows = _ohlc_chart_rows(ohlc, symbol, start, end)
        broker_daily = _broker_daily_rows(raw, str(row.broker))
        broker_stats = _broker_window_stats(raw, broker_lookup, broker_cities, str(row.broker))
        name = stock_names.get(symbol, "")
        general_hit = symbol in general_hits
        news_items = _news_items_for_case(symbol, name, cfg, news_cache)
        row_series = pd.Series(row._asdict())
        score, positives, risks = _score_case(row_series, trend, broker_stats, general_hit)
        if not bool(row.ongoing):
            score = min(score, 1.5)
            risks.insert(0, "episode 已結束，僅供回顧，不作為當前買進候選")
        elif not bool(row.review_eligible):
            score = min(score, 1.9)
            risks.insert(0, "未通過 review_eligible gate，需等訊號延續或品質改善")
        case = {
            "rank": int(row.rank),
            "symbol": symbol,
            "name": name,
            "broker": str(row.broker),
            "broker_name": str(row.broker_name),
            "scope_note": str(universe_row.scope_note),
            "selected_for_detail": bool(universe_row.selected_for_detail),
            "review_eligible": bool(row.review_eligible),
            "ongoing": bool(row.ongoing),
            "episode_start": _date(start),
            "last_qualifying_date": _date(end),
            "status_as_of": _date(row.status_as_of),
            "status": str(row.status),
            "confidence": str(row.confidence),
            "trigger_days": int(row.qualifying_dates),
            "observed_sessions": int(row.observed_sessions),
            "positive_net_sessions": int(row.positive_net_sessions),
            "cumulative_net_buy": float(row.cumulative_net_buy),
            "cumulative_purity": float(row.cumulative_purity),
            "cumulative_buy_share": float(row.cumulative_buy_share),
            "retention_ratio": float(row.retention_ratio),
            "trend": trend,
            "ohlc_chart": chart_rows,
            "broker_daily": broker_daily,
            "broker_stats": broker_stats,
            "general_flow_hit": general_hit,
            "score": round(score, 2),
            "decision": _risk_label(score),
            "positives": positives,
            "risks": risks,
            "news_items": news_items,
            "news_links": _news_links(symbol, name),
            "detail_path": str(universe_row.detail_path),
        }
        cases.append(case)
    if cfg.fetch_news:
        _write_news_cache(news_cache_path, news_cache)

    summary = {
        "source": str(cfg.episodes_path),
        "min_trigger_days": cfg.min_trigger_days,
        "total_trigger_gt5_episodes": int(len(trigger_gt5)),
        "review_eligible_trigger_gt5_episodes": int(len(eligible_gt5)),
        "selected_actionable_cases": int(len(selected)),
        "case_count": len(cases),
        "latest_signal_date": max((case["last_qualifying_date"] for case in cases), default=""),
        "decision_counts": dict(pd.Series([case["decision"] for case in cases]).value_counts()) if cases else {},
        "news_status": "google_news_rss" if cfg.fetch_news else "links_only_fetch_disabled",
        "cases_with_news": sum(1 for case in cases if case["news_items"]),
        "news_cache_path": str(news_cache_path) if cfg.fetch_news else "",
        "all_trigger_gt5_cases_path": str(cfg.output_dir / "all_trigger_gt5_cases.csv"),
    }
    return cases, summary, universe


def _escape(value: object) -> str:
    return html.escape("" if value is None else str(value))


def _metric(label: str, value: str, subtext: str = "") -> str:
    sub = f"<span>{_escape(subtext)}</span>" if subtext else ""
    return f"<div class=\"metric\"><label>{_escape(label)}</label><b>{_escape(value)}</b>{sub}</div>"


def _html_json(data: object) -> str:
    raw = json.dumps(data, ensure_ascii=False, default=_json_default)
    return raw.replace("</", "<\\/")


def _candlestick_svg(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "<div class=\"empty\">沒有可繪製的 OHLC 資料。</div>"
    width = 1120
    height = 420
    left = 56
    right = 18
    top = 20
    price_bottom = 285
    volume_top = 320
    bottom = 392
    chart_width = width - left - right
    price_values = [
        _num(row[field], math.nan)
        for row in rows
        for field in ("open", "high", "low", "close", "ma5", "ma20", "ma60")
        if not math.isnan(_num(row[field], math.nan))
    ]
    if not price_values:
        return "<div class=\"empty\">沒有可繪製的價格資料。</div>"
    low = min(price_values)
    high = max(price_values)
    pad = max((high - low) * 0.08, 0.01)
    low -= pad
    high += pad
    max_volume = max((_num(row["volume"], 0.0) for row in rows), default=0.0) or 1.0
    step = chart_width / max(len(rows), 1)
    candle_width = max(2.0, min(8.0, step * 0.58))

    def x_pos(index: int) -> float:
        return left + index * step + step / 2

    def y_price(value: object) -> float:
        number = _num(value, math.nan)
        if math.isnan(number):
            return math.nan
        return top + (high - number) / (high - low) * (price_bottom - top)

    def y_volume(value: object) -> float:
        return bottom - (_num(value, 0.0) / max_volume) * (bottom - volume_top)

    grid_lines: list[str] = []
    for ratio in (0, 0.25, 0.5, 0.75, 1.0):
        y = top + ratio * (price_bottom - top)
        price = high - ratio * (high - low)
        grid_lines.append(
            f"<line x1=\"{left}\" y1=\"{y:.1f}\" x2=\"{width - right}\" y2=\"{y:.1f}\" class=\"grid-line\"/>"
            f"<text x=\"8\" y=\"{y + 4:.1f}\" class=\"axis-label\">{price:.2f}</text>"
        )

    in_episode_indexes = [idx for idx, row in enumerate(rows) if row.get("in_episode")]
    episode_rect = ""
    if in_episode_indexes:
        start_x = left + min(in_episode_indexes) * step
        end_x = left + (max(in_episode_indexes) + 1) * step
        episode_rect = f"<rect x=\"{start_x:.1f}\" y=\"{top}\" width=\"{max(end_x - start_x, step):.1f}\" height=\"{price_bottom - top}\" class=\"episode-band\"/>"

    candles: list[str] = []
    volumes: list[str] = []
    for idx, row in enumerate(rows):
        x = x_pos(idx)
        open_y = y_price(row["open"])
        high_y = y_price(row["high"])
        low_y = y_price(row["low"])
        close_y = y_price(row["close"])
        if any(math.isnan(value) for value in (open_y, high_y, low_y, close_y)):
            continue
        up = _num(row["close"]) >= _num(row["open"])
        klass = "up" if up else "down"
        body_y = min(open_y, close_y)
        body_h = max(abs(close_y - open_y), 1.2)
        title = _escape(
            f"{row['date']} O:{_fmt_float(row['open'])} H:{_fmt_float(row['high'])} "
            f"L:{_fmt_float(row['low'])} C:{_fmt_float(row['close'])}"
        )
        candles.append(
            f"<g><title>{title}</title><line x1=\"{x:.1f}\" y1=\"{high_y:.1f}\" x2=\"{x:.1f}\" y2=\"{low_y:.1f}\" class=\"wick {klass}\"/>"
            f"<rect x=\"{x - candle_width / 2:.1f}\" y=\"{body_y:.1f}\" width=\"{candle_width:.1f}\" height=\"{body_h:.1f}\" class=\"candle {klass}\"/></g>"
        )
        vol_y = y_volume(row["volume"])
        volumes.append(
            f"<rect x=\"{x - candle_width / 2:.1f}\" y=\"{vol_y:.1f}\" width=\"{candle_width:.1f}\" height=\"{bottom - vol_y:.1f}\" class=\"volume {klass}\"/>"
        )

    def ma_polyline(field: str, klass: str) -> str:
        points = []
        for idx, row in enumerate(rows):
            y = y_price(row.get(field))
            if not math.isnan(y):
                points.append(f"{x_pos(idx):.1f},{y:.1f}")
        if len(points) < 2:
            return ""
        return f"<polyline points=\"{' '.join(points)}\" class=\"ma {klass}\"/>"

    first_label = _escape(rows[0]["date"])
    last_label = _escape(rows[-1]["date"])
    return (
        f"<svg class=\"kchart\" viewBox=\"0 0 {width} {height}\" role=\"img\" aria-label=\"OHLC candlestick chart\">"
        f"<rect x=\"0\" y=\"0\" width=\"{width}\" height=\"{height}\" class=\"chart-bg\"/>"
        f"{episode_rect}{''.join(grid_lines)}"
        f"<line x1=\"{left}\" y1=\"{volume_top}\" x2=\"{width - right}\" y2=\"{volume_top}\" class=\"volume-line\"/>"
        f"{''.join(volumes)}{''.join(candles)}"
        f"{ma_polyline('ma5', 'ma5')}{ma_polyline('ma20', 'ma20')}{ma_polyline('ma60', 'ma60')}"
        f"<text x=\"{left}\" y=\"412\" class=\"axis-label\">{first_label}</text>"
        f"<text x=\"{width - right - 82}\" y=\"412\" class=\"axis-label\">{last_label}</text>"
        "</svg>"
    )


def _interactive_chart_html(case: dict[str, object]) -> str:
    payload = {
        "ohlc": case.get("ohlc_chart", []),
        "brokerDaily": case.get("broker_daily", []),
        "episodeStart": case["episode_start"],
        "episodeEnd": case["last_qualifying_date"],
        "brokerName": case["broker_name"],
        "broker": case["broker"],
    }
    return f"""
          <div class="chart-toolbar">
            <button type="button" class="range-btn active" data-range="all">全部</button>
            <button type="button" class="range-btn" data-range="episode">Episode</button>
            <button type="button" class="range-btn" data-range="120">近 120 根</button>
            <button type="button" class="range-btn" data-range="60">近 60 根</button>
            <label class="toggle"><input type="checkbox" id="showBrokerBars" checked> 關鍵分點買賣</label>
          </div>
          <div class="interactive-chart" id="interactiveChart">
            <svg id="interactiveKChart" viewBox="0 0 1120 520" role="img" aria-label="interactive OHLC and broker flow chart"></svg>
            <div class="chart-tooltip" id="chartTooltip"></div>
          </div>
          <div class="point-detail" id="pointDetail">
            <b>移到 K 線上查看單日資料</b>
            <span>會同步顯示 OHLC、成交量、主分點買賣、淨買賣與估計均價。</span>
          </div>
          <script type="application/json" id="chartData">{_html_json(payload)}</script>
          <script>
            document.addEventListener('DOMContentLoaded', () => {{
              const payload = JSON.parse(document.getElementById('chartData').textContent);
              const svg = document.getElementById('interactiveKChart');
              const tooltip = document.getElementById('chartTooltip');
              const detail = document.getElementById('pointDetail');
              const showBrokerBars = document.getElementById('showBrokerBars');
              const brokerByDate = new Map(payload.brokerDaily.map(row => [row.date, row]));
              const ohlc = payload.ohlc.map(row => ({{ ...row, broker: brokerByDate.get(row.date) || null }}));
              let currentRange = 'all';
              let selectedDate = '';

              const fmt = (value, digits = 2) => {{
                const number = Number(value);
                return Number.isFinite(number) ? number.toLocaleString('en-US', {{ maximumFractionDigits: digits, minimumFractionDigits: digits }}) : 'n/a';
              }};
              const fmtInt = value => {{
                const number = Number(value);
                return Number.isFinite(number) ? Math.round(number).toLocaleString('en-US') : 'n/a';
              }};
              const svgEl = (name, attrs = {{}}, text = '') => {{
                const node = document.createElementNS('http://www.w3.org/2000/svg', name);
                Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, String(value)));
                if (text) node.textContent = text;
                return node;
              }};
              const setDetail = row => {{
                const broker = row.broker || {{}};
                detail.innerHTML = `
                  <b>${{row.date}}</b>
                  <span>O ${{fmt(row.open)}} / H ${{fmt(row.high)}} / L ${{fmt(row.low)}} / C ${{fmt(row.close)}} · volume ${{fmtInt(row.volume)}}</span>
                  <span>主分點 ${{payload.brokerName}} 買 ${{fmtInt(broker.target_buy || 0)}} / 賣 ${{fmtInt(broker.target_sell || 0)}} / 淨 ${{fmtInt(broker.target_net || 0)}} · 均價 ${{fmt(broker.target_avg_buy_price)}}</span>
                  <span>全分點淨買 ${{fmtInt(broker.market_net || 0)}} · ${{row.in_episode ? 'episode 內' : 'episode 外'}}</span>
                `;
                document.querySelectorAll('tr[data-date]').forEach(tr => tr.classList.toggle('selected-row', tr.dataset.date === row.date));
              }};
              const visibleRows = () => {{
                if (currentRange === 'all') return ohlc;
                if (currentRange === 'episode') {{
                  const indexes = ohlc.map((row, idx) => row.in_episode ? idx : -1).filter(idx => idx >= 0);
                  if (!indexes.length) return ohlc;
                  const start = Math.max(Math.min(...indexes) - 20, 0);
                  const end = Math.min(Math.max(...indexes) + 11, ohlc.length);
                  return ohlc.slice(start, end);
                }}
                return ohlc.slice(Math.max(ohlc.length - Number(currentRange), 0));
              }};
              const render = () => {{
                const rows = visibleRows();
                svg.replaceChildren();
                if (!rows.length) return;
                const width = 1120, height = 520;
                const left = 58, right = 18, top = 20, priceBottom = 292, volumeTop = 322, volumeBottom = 390, brokerTop = 420, brokerBottom = 492;
                const chartW = width - left - right;
                const step = chartW / rows.length;
                const candleW = Math.max(2, Math.min(9, step * 0.58));
                const priceValues = rows.flatMap(row => [row.open, row.high, row.low, row.close, row.ma5, row.ma20, row.ma60].map(Number)).filter(Number.isFinite);
                let low = Math.min(...priceValues), high = Math.max(...priceValues);
                const pad = Math.max((high - low) * 0.08, 0.01);
                low -= pad; high += pad;
                const maxVolume = Math.max(...rows.map(row => Number(row.volume) || 0), 1);
                const maxAbsBroker = Math.max(...rows.map(row => Math.abs(Number(row.broker?.target_net) || 0)), 1);
                const yPrice = value => top + (high - Number(value)) / (high - low) * (priceBottom - top);
                const yVolume = value => volumeBottom - (Number(value) || 0) / maxVolume * (volumeBottom - volumeTop);
                const yBroker = value => {{
                  const mid = (brokerTop + brokerBottom) / 2;
                  return mid - (Number(value) || 0) / maxAbsBroker * ((brokerBottom - brokerTop) / 2);
                }};
                const xPos = idx => left + idx * step + step / 2;

                svg.appendChild(svgEl('rect', {{ x: 0, y: 0, width, height, class: 'chart-bg' }}));
                [0, .25, .5, .75, 1].forEach(ratio => {{
                  const y = top + ratio * (priceBottom - top);
                  const price = high - ratio * (high - low);
                  svg.appendChild(svgEl('line', {{ x1: left, y1: y.toFixed(1), x2: width - right, y2: y.toFixed(1), class: 'grid-line' }}));
                  svg.appendChild(svgEl('text', {{ x: 8, y: (y + 4).toFixed(1), class: 'axis-label' }}, price.toFixed(2)));
                }});
                const episodeIndexes = rows.map((row, idx) => row.in_episode ? idx : -1).filter(idx => idx >= 0);
                if (episodeIndexes.length) {{
                  const x = left + Math.min(...episodeIndexes) * step;
                  const w = (Math.max(...episodeIndexes) - Math.min(...episodeIndexes) + 1) * step;
                  svg.appendChild(svgEl('rect', {{ x: x.toFixed(1), y: top, width: Math.max(w, step).toFixed(1), height: priceBottom - top, class: 'episode-band' }}));
                }}
                svg.appendChild(svgEl('line', {{ x1: left, y1: volumeTop, x2: width - right, y2: volumeTop, class: 'volume-line' }}));
                svg.appendChild(svgEl('line', {{ x1: left, y1: (brokerTop + brokerBottom) / 2, x2: width - right, y2: (brokerTop + brokerBottom) / 2, class: 'broker-zero' }}));

                const drawPolyline = (field, klass) => {{
                  const points = rows.map((row, idx) => Number.isFinite(Number(row[field])) ? `${{xPos(idx).toFixed(1)}},${{yPrice(row[field]).toFixed(1)}}` : '').filter(Boolean).join(' ');
                  if (points) svg.appendChild(svgEl('polyline', {{ points, class: `ma ${{klass}}` }}));
                }};

                rows.forEach((row, idx) => {{
                  const x = xPos(idx);
                  const up = Number(row.close) >= Number(row.open);
                  const klass = up ? 'up' : 'down';
                  const volY = yVolume(row.volume);
                  svg.appendChild(svgEl('rect', {{ x: (x - candleW / 2).toFixed(1), y: volY.toFixed(1), width: candleW.toFixed(1), height: (volumeBottom - volY).toFixed(1), class: `volume ${{klass}}` }}));
                  if (showBrokerBars.checked && row.broker) {{
                    const net = Number(row.broker.target_net) || 0;
                    const mid = (brokerTop + brokerBottom) / 2;
                    const y = yBroker(net);
                    svg.appendChild(svgEl('rect', {{
                      x: (x - candleW / 2).toFixed(1),
                      y: Math.min(y, mid).toFixed(1),
                      width: candleW.toFixed(1),
                      height: Math.max(Math.abs(mid - y), 1).toFixed(1),
                      class: `broker-bar ${{net >= 0 ? 'buy' : 'sell'}}`
                    }}));
                    if (Number(row.broker.target_avg_buy_price) > 0) {{
                      svg.appendChild(svgEl('circle', {{ cx: x.toFixed(1), cy: yPrice(row.broker.target_avg_buy_price).toFixed(1), r: 2.6, class: 'avg-dot' }}));
                    }}
                  }}
                  const highY = yPrice(row.high), lowY = yPrice(row.low), openY = yPrice(row.open), closeY = yPrice(row.close);
                  svg.appendChild(svgEl('line', {{ x1: x.toFixed(1), y1: highY.toFixed(1), x2: x.toFixed(1), y2: lowY.toFixed(1), class: `wick ${{klass}}` }}));
                  svg.appendChild(svgEl('rect', {{
                    x: (x - candleW / 2).toFixed(1),
                    y: Math.min(openY, closeY).toFixed(1),
                    width: candleW.toFixed(1),
                    height: Math.max(Math.abs(closeY - openY), 1.2).toFixed(1),
                    class: `candle ${{klass}}`
                  }}));
                  const hit = svgEl('rect', {{ x: (left + idx * step).toFixed(1), y: top, width: Math.max(step, 2).toFixed(1), height: brokerBottom - top, class: 'hit-zone', 'data-date': row.date }});
                  hit.addEventListener('mouseenter', event => {{
                    tooltip.style.display = 'block';
                    tooltip.innerHTML = `<b>${{row.date}}</b><br>C ${{fmt(row.close)}} · 主分點淨 ${{fmtInt(row.broker?.target_net || 0)}}`;
                    setDetail(row);
                  }});
                  hit.addEventListener('mousemove', event => {{
                    const bounds = svg.getBoundingClientRect();
                    tooltip.style.left = `${{event.clientX - bounds.left + 14}}px`;
                    tooltip.style.top = `${{event.clientY - bounds.top + 12}}px`;
                  }});
                  hit.addEventListener('mouseleave', () => tooltip.style.display = 'none');
                  hit.addEventListener('click', () => {{
                    selectedDate = row.date;
                    setDetail(row);
                    render();
                  }});
                  svg.appendChild(hit);
                  if (row.date === selectedDate) {{
                    svg.appendChild(svgEl('line', {{ x1: x.toFixed(1), y1: top, x2: x.toFixed(1), y2: brokerBottom, class: 'selected-date-line' }}));
                  }}
                }});
                drawPolyline('ma5', 'ma5');
                drawPolyline('ma20', 'ma20');
                drawPolyline('ma60', 'ma60');
                svg.appendChild(svgEl('text', {{ x: left, y: 512, class: 'axis-label' }}, rows[0].date));
                svg.appendChild(svgEl('text', {{ x: width - right - 82, y: 512, class: 'axis-label' }}, rows[rows.length - 1].date));
              }};
              document.querySelectorAll('.range-btn').forEach(button => {{
                button.addEventListener('click', () => {{
                  document.querySelectorAll('.range-btn').forEach(item => item.classList.remove('active'));
                  button.classList.add('active');
                  currentRange = button.dataset.range;
                  render();
                }});
              }});
              showBrokerBars.addEventListener('change', render);
              document.querySelectorAll('tr[data-date]').forEach(row => {{
                row.addEventListener('mouseenter', () => {{
                  const item = ohlc.find(point => point.date === row.dataset.date);
                  if (item) setDetail(item);
                }});
              }});
              render();
            }});
          </script>
"""


def _case_detail_html(case: dict[str, object]) -> str:
    trend = case["trend"]
    broker_stats = case["broker_stats"]
    chart = _interactive_chart_html(case)
    news_items = case.get("news_items") or []
    news_html = "\n".join(
        f"<li><a href=\"{_escape(item.get('url', ''))}\">{_escape(item.get('title', ''))}</a>"
        f"<span>{_escape(item.get('source', ''))} {_escape(item.get('published', ''))}</span></li>"
        for item in news_items
    ) or "<li>未擷取到 Google News RSS 標題；請使用新聞入口人工查核。</li>"
    news_links = "\n".join(
        f"<a href=\"{_escape(link['url'])}\">{_escape(link['label'])}</a>"
        for link in case.get("news_links", [])
    )
    top_brokers = "\n".join(
        "<tr>"
        f"<td>{_escape(broker['broker_name'])}</td>"
        f"<td>{_escape(broker.get('broker_city') or 'n/a')}</td>"
        f"<td class=\"num\">{_fmt_int(broker['net_buy'])}</td>"
        f"<td class=\"num\">{_fmt_float(broker.get('avg_buy_price'))}</td>"
        "</tr>"
        for broker in broker_stats["top_brokers"]
    ) or "<tr><td colspan=\"4\">n/a</td></tr>"
    broker_daily = "\n".join(
        f"<tr data-date=\"{_escape(row['date'])}\">"
        f"<td>{_escape(row['date'])}</td>"
        f"<td class=\"num\">{_fmt_int(row['target_buy'])}</td>"
        f"<td class=\"num\">{_fmt_int(row['target_sell'])}</td>"
        f"<td class=\"num {'pos' if _num(row['target_net']) >= 0 else 'neg'}\">{_fmt_int(row['target_net'])}</td>"
        f"<td class=\"num\">{_fmt_float(row['target_avg_buy_price'])}</td>"
        f"<td class=\"num {'pos' if _num(row['market_net']) >= 0 else 'neg'}\">{_fmt_int(row['market_net'])}</td>"
        "</tr>"
        for row in case.get("broker_daily", [])
    ) or "<tr><td colspan=\"6\">n/a</td></tr>"
    ohlc_rows = "\n".join(
        f"<tr data-date=\"{_escape(row['date'])}\">"
        f"<td>{_escape(row['date'])}</td>"
        f"<td>{'yes' if row.get('in_episode') else ''}</td>"
        f"<td class=\"num\">{_fmt_float(row['open'])}</td>"
        f"<td class=\"num\">{_fmt_float(row['high'])}</td>"
        f"<td class=\"num\">{_fmt_float(row['low'])}</td>"
        f"<td class=\"num\">{_fmt_float(row['close'])}</td>"
        f"<td class=\"num\">{_fmt_int(row['volume'])}</td>"
        f"<td class=\"num\">{_fmt_float(row['ma5'])}</td>"
        f"<td class=\"num\">{_fmt_float(row['ma20'])}</td>"
        f"<td class=\"num\">{_fmt_float(row['ma60'])}</td>"
        "</tr>"
        for row in case.get("ohlc_chart", [])
    ) or "<tr><td colspan=\"10\">n/a</td></tr>"
    positives = "\n".join(f"<li>{_escape(item)}</li>" for item in case["positives"]) or "<li>無明顯正面加分。</li>"
    risks = "\n".join(f"<li>{_escape(item)}</li>" for item in case["risks"]) or "<li>未觸發主要量化風險。</li>"
    return f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_escape(case['symbol'])} {_escape(case['name'])} 觸發天數案例分析</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #172033; background: #f4f6f8; }}
    header {{ background: #111827; color: white; padding: 22px 28px; }}
    main {{ padding: 20px 28px 40px; }}
    h1 {{ margin: 0 0 8px; font-size: 26px; }}
    h2 {{ margin: 0 0 12px; font-size: 18px; }}
    a {{ color: #0b5cad; text-decoration: none; }}
    .back {{ color: #b7c7df; display: inline-block; margin-bottom: 14px; }}
    .meta {{ color: #c9d3e4; line-height: 1.5; }}
    .layout {{ display: grid; grid-template-columns: minmax(0, 1fr) 340px; gap: 18px; align-items: start; }}
    section {{ background: white; border: 1px solid #dfe5ee; border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
    .metrics {{ display: grid; grid-template-columns: repeat(4, minmax(130px, 1fr)); gap: 10px; margin-bottom: 16px; }}
    .metric {{ border: 1px solid #dfe5ee; border-radius: 8px; padding: 12px; background: #fbfcfe; min-height: 74px; }}
    .metric label {{ display: block; font-size: 12px; color: #637083; }}
    .metric b {{ display: block; font-size: 21px; margin-top: 5px; }}
    .metric span {{ display: block; font-size: 12px; color: #637083; margin-top: 4px; }}
    .decision {{ display: inline-flex; align-items: center; padding: 5px 9px; border-radius: 999px; background: #e8f3ff; color: #064f8f; font-weight: 700; }}
    .chart-wrap {{ overflow-x: auto; }}
    .chart-toolbar {{ display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-bottom: 10px; }}
    .chart-toolbar button {{ border: 1px solid #cfd8e6; background: #fbfcfe; color: #172033; border-radius: 6px; padding: 7px 10px; cursor: pointer; }}
    .chart-toolbar button.active {{ background: #172033; color: white; border-color: #172033; }}
    .toggle {{ display: inline-flex; align-items: center; gap: 6px; color: #526173; font-size: 13px; }}
    .interactive-chart {{ position: relative; overflow-x: auto; border: 1px solid #e5eaf1; background: #fbfcfe; }}
    .interactive-chart svg {{ min-width: 900px; width: 100%; height: auto; display: block; }}
    .chart-tooltip {{ display: none; position: absolute; pointer-events: none; z-index: 3; background: rgba(17, 24, 39, .92); color: white; border-radius: 6px; padding: 7px 9px; font-size: 12px; line-height: 1.45; box-shadow: 0 8px 20px rgba(15, 23, 42, .22); }}
    .point-detail {{ margin-top: 10px; border: 1px solid #dfe5ee; border-radius: 8px; background: #fbfcfe; padding: 11px 12px; display: grid; gap: 4px; color: #334155; }}
    .point-detail b {{ color: #172033; }}
    .point-detail span {{ font-size: 13px; }}
    .kchart {{ min-width: 900px; width: 100%; height: auto; display: block; }}
    .chart-bg {{ fill: #fbfcfe; }}
    .grid-line {{ stroke: #e3e8ef; stroke-width: 1; }}
    .volume-line {{ stroke: #cbd5e1; stroke-width: 1; }}
    .episode-band {{ fill: #fff4c2; opacity: 0.75; }}
    .wick.up, .candle.up {{ stroke: #c2410c; fill: #ef4444; }}
    .wick.down, .candle.down {{ stroke: #047857; fill: #10b981; }}
    .volume.up {{ fill: #fecaca; }}
    .volume.down {{ fill: #bbf7d0; }}
    .broker-zero {{ stroke: #94a3b8; stroke-width: 1; stroke-dasharray: 3 4; }}
    .broker-bar.buy {{ fill: #dc2626; opacity: .72; }}
    .broker-bar.sell {{ fill: #059669; opacity: .72; }}
    .avg-dot {{ fill: #111827; stroke: #fbbf24; stroke-width: 1.5; }}
    .hit-zone {{ fill: transparent; cursor: crosshair; }}
    .selected-date-line {{ stroke: #0f172a; stroke-width: 1.3; stroke-dasharray: 4 4; pointer-events: none; }}
    .ma {{ fill: none; stroke-width: 1.8; opacity: 0.95; }}
    .ma5 {{ stroke: #2563eb; }}
    .ma20 {{ stroke: #7c3aed; }}
    .ma60 {{ stroke: #f59e0b; }}
    .axis-label {{ fill: #64748b; font-size: 12px; }}
    .legend {{ display: flex; gap: 14px; flex-wrap: wrap; color: #526173; font-size: 13px; margin-top: 8px; }}
    .legend i {{ display: inline-block; width: 18px; height: 3px; vertical-align: middle; margin-right: 5px; }}
    .table-wrap {{ max-height: 440px; overflow: auto; border: 1px solid #e5eaf1; }}
    table {{ width: 100%; border-collapse: collapse; background: white; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #edf1f6; font-size: 13px; text-align: left; white-space: nowrap; }}
    th {{ position: sticky; top: 0; background: #eef2f7; z-index: 1; }}
    tr.selected-row td {{ background: #fff7d6; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .pos {{ color: #b42318; }}
    .neg {{ color: #047857; }}
    .split {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
    ul {{ margin: 0; padding-left: 18px; }}
    li {{ margin: 6px 0; }}
    .news li span {{ display: block; color: #64748b; font-size: 12px; margin-top: 2px; }}
    .links {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }}
    .links a {{ border: 1px solid #cfd8e6; border-radius: 6px; padding: 7px 9px; background: #fbfcfe; }}
    .empty {{ color: #64748b; padding: 20px; border: 1px dashed #cbd5e1; border-radius: 8px; }}
    @media (max-width: 980px) {{
      main {{ padding: 14px; }}
      .layout, .split {{ grid-template-columns: 1fr; }}
      .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
  </style>
</head>
<body>
  <header>
    <a class="back" href="../index.html">回到入口頁</a>
    <h1>{_escape(case['symbol'])} {_escape(case['name'])} - 觸發天數 &gt; 5 案例分析</h1>
    <div class="meta">
      <span class="decision">{_escape(case['decision'])}</span>
      score {_escape(case['score'])} · rank {_escape(case['rank'])} · {_escape(case['scope_note'])}<br>
      episode {_escape(case['episode_start'])} ~ {_escape(case['last_qualifying_date'])} · 主分點 {_escape(case['broker_name'])} ({_escape(case['broker'])})
    </div>
  </header>
  <main>
    <div class="metrics">
      {_metric("觸發天數", str(case["trigger_days"]), f"{case['positive_net_sessions']}/{case['observed_sessions']} 正買超天")}
      {_metric("累積買超", f"{_fmt_int(case['cumulative_net_buy'])} 股", f"純度 {_fmt_pct(case['cumulative_purity'])}")}
      {_metric("買量占比", _fmt_pct(case["cumulative_buy_share"]), f"留倉率 {_fmt_pct(case['retention_ratio'])}")}
      {_metric("主分點均價", _fmt_float(broker_stats["target_avg_buy_price"]), f"收盤 {_fmt_float(trend['latest_close'])}")}
      {_metric("短/中/長趨勢", f"{trend['short_trend']} / {trend['mid_trend']} / {trend['long_trend']}", f"5/20/60D {_fmt_pct(trend['ret5'])} / {_fmt_pct(trend['ret20'])} / {_fmt_pct(trend['ret60'])}")}
      {_metric("城市群聚", broker_stats["dominant_city"] or "n/a", _fmt_pct(broker_stats["dominant_city_share"]))}
      {_metric("左手換右手", "yes" if broker_stats["left_right_flag"] else "no", f"cross ratio {_fmt_pct(broker_stats['same_broker_cross_ratio'])}")}
      {_metric("一般主力流交叉", "yes" if case["general_flow_hit"] else "no", f"狀態 {case['status']}")}
    </div>
    <div class="layout">
      <div>
        <section>
          <h2>K 線與 episode 區間</h2>
          <div class="chart-wrap">{chart}</div>
          <div class="legend">
            <span><i style="background:#2563eb"></i>MA5</span>
            <span><i style="background:#7c3aed"></i>MA20</span>
            <span><i style="background:#f59e0b"></i>MA60</span>
            <span>黃色底色為 episode 對應時間</span>
          </div>
        </section>
        <section>
          <h2>主分點逐日買賣超</h2>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Date</th><th>主分點買</th><th>主分點賣</th><th>主分點淨買</th><th>估計買均價</th><th>全分點淨買</th></tr></thead>
              <tbody>{broker_daily}</tbody>
            </table>
          </div>
        </section>
        <section>
          <h2>OHLC / MA 明細</h2>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Date</th><th>Episode</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th><th>MA5</th><th>MA20</th><th>MA60</th></tr></thead>
              <tbody>{ohlc_rows}</tbody>
            </table>
          </div>
        </section>
      </div>
      <aside>
        <section>
          <h2>買進可能性摘要</h2>
          <div class="split">
            <div><b>正面依據</b><ul>{positives}</ul></div>
            <div><b>風險與待查</b><ul>{risks}</ul></div>
          </div>
        </section>
        <section>
          <h2>Top 買超分點</h2>
          <div class="table-wrap">
            <table>
              <thead><tr><th>分點</th><th>城市</th><th>淨買</th><th>均價</th></tr></thead>
              <tbody>{top_brokers}</tbody>
            </table>
          </div>
        </section>
        <section>
          <h2>最近新聞線索</h2>
          <ul class="news">{news_html}</ul>
          <div class="links">{news_links}</div>
        </section>
      </aside>
    </div>
  </main>
</body>
</html>
"""


def write_detail_reports(cases: list[dict[str, object]], out_dir: Path, limit: int) -> None:
    case_dir = ensure_dir(out_dir / "all_cases")
    selected_cases = cases if limit <= 0 else cases[:limit]
    for case in selected_cases:
        (case_dir / Path(str(case["detail_path"])).name).write_text(_case_detail_html(case), encoding="utf-8")


def write_review_report(cases: list[dict[str, object]], summary: dict[str, object], out_dir: Path) -> None:
    lines = [
        "# Trigger Days > 5 Case Review",
        "",
        "## Scope",
        "",
        f"- Source episodes: `{summary['source']}`",
        f"- Trigger threshold: `qualifying_dates > {summary['min_trigger_days']}`",
        f"- Total trigger-days cases in source: `{summary['total_trigger_gt5_episodes']}`",
        f"- Review-eligible trigger-days cases: `{summary['review_eligible_trigger_gt5_episodes']}`",
        f"- Selected actionable/current cases: `{summary['selected_actionable_cases']}`",
        f"- Cases written here: `{summary['case_count']}`",
        f"- Latest signal date: `{summary['latest_signal_date']}`",
        f"- News status: `{summary['news_status']}`",
        f"- Cases with RSS news items: `{summary['cases_with_news']}`",
        f"- News cache: `{summary['news_cache_path'] or 'n/a'}`",
        f"- Full trigger-days index: `{summary['all_trigger_gt5_cases_path']}`",
        "",
        "## Evaluation Facets",
        "",
        "- News: per-case Google News RSS headlines plus Google News, Yahoo stock news, and MOPS links. Headlines are treated as leads, not confirmed bullish or bearish conclusions.",
        "- Main-force strength: cumulative net buy, purity, buy share, retention, positive sessions, and cross-hit with the general broker-flow anomaly list.",
        "- Trend: 5/20/60-session return, moving-average posture, and 60-session price position.",
        "- Broker clustering: positive-net-buy broker city concentration from broker address data.",
        "- Estimated cost basis: branch buy amount divided by branch buy volume during the episode.",
        "- Wash-trade warning: same-broker buy and sell cross ratio during the episode.",
        "",
        "## Decision Counts",
        "",
    ]
    for decision, count in summary["decision_counts"].items():
        lines.append(f"- `{decision}`: `{count}`")
    lines.extend(["", "## Top Buy-Watch Cases", ""])
    buy_watch = [case for case in cases if case["decision"] == "可列入買進觀察"]
    if not buy_watch:
        lines.append("- none")
    for case in buy_watch[:50]:
        lines.append(
            f"- `{case['rank']}` `{case['symbol']}` `{case['name']}` `{case['broker_name']}` "
            f"score=`{case['score']}` trigger_days=`{case['trigger_days']}` "
            f"trend=`{case['trend']['short_trend']}/{case['trend']['mid_trend']}/{case['trend']['long_trend']}` "
            f"detail=`{case['detail_path']}`"
        )
    (out_dir / "review_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _html_page(cases: list[dict[str, object]], summary: dict[str, object]) -> str:
    payload = json.dumps({"summary": summary, "cases": _slim_cases(cases)}, ensure_ascii=False, default=_json_default)
    def news_cell(case: dict[str, object]) -> str:
        items = case.get("news_items") or []
        if not items:
            return "n/a"
        first = items[0]
        title = html.escape(str(first.get("title", "")))
        url = html.escape(str(first.get("url", "")))
        published = html.escape(str(first.get("published", "")))
        suffix = f"<br><span>{published}</span>" if published else ""
        return f"<a href=\"{url}\">{title}</a>{suffix}"

    rows = "\n".join(
        f"<tr data-decision=\"{html.escape(case['decision'])}\" data-scope=\"{html.escape(case['scope_note'])}\">"
        f"<td>{case['rank']}</td><td><a href=\"{html.escape(case['detail_path'])}\">{case['symbol']} {html.escape(case['name'])}</a></td>"
        f"<td>{html.escape(case['broker_name'])}</td><td>{html.escape(case['scope_note'])}</td><td>{case['trigger_days']}</td><td>{case['score']}</td>"
        f"<td>{html.escape(case['decision'])}</td><td>{_fmt_pct(case['cumulative_purity'])}</td>"
        f"<td>{_fmt_pct(case['cumulative_buy_share'])}</td><td>{case['trend']['short_trend']} / {case['trend']['mid_trend']} / {case['trend']['long_trend']}</td>"
        f"<td>{html.escape(case['broker_stats']['dominant_city'] or '')} {_fmt_pct(case['broker_stats']['dominant_city_share'])}</td>"
        f"<td>{'yes' if case['broker_stats']['left_right_flag'] else 'no'}</td><td>{news_cell(case)}</td></tr>"
        for case in cases
    )
    return f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>觸發天數 &gt; 5 案例買進可能性評估</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #172033; background: #f6f7f9; }}
    header {{ padding: 28px 32px; background: #172033; color: white; }}
    main {{ padding: 24px 32px 40px; }}
    h1 {{ margin: 0 0 10px; font-size: 28px; }}
    .meta {{ color: #c9d3e4; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(140px, 1fr)); gap: 12px; margin: 20px 0; }}
    .metric {{ background: white; border: 1px solid #e1e5ec; border-radius: 8px; padding: 14px; }}
    .metric b {{ display: block; font-size: 24px; margin-top: 4px; }}
    .toolbar {{ display: flex; gap: 10px; align-items: center; margin: 18px 0; flex-wrap: wrap; }}
    input, select {{ padding: 9px 10px; border: 1px solid #cfd6e1; border-radius: 6px; background: white; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border: 1px solid #e1e5ec; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid #edf0f4; text-align: left; font-size: 13px; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #eef2f7; z-index: 1; }}
    a {{ color: #0b5cad; text-decoration: none; }}
    .note {{ max-width: 1100px; line-height: 1.55; color: #445064; }}
  </style>
</head>
<body>
  <header>
    <h1>觸發天數 &gt; 5 案例買進可能性評估</h1>
    <div class="meta">資料來源: single_branch_persistent_accumulation + OHLC + broker branch parquet。最新訊號日: {html.escape(str(summary.get("latest_signal_date", "")))}</div>
  </header>
  <main>
    <p class="note">入口頁先用可重現的本地量化欄位排序，每個案例可點入 HTML 細報，細報包含 K 線、MA、episode 區間、逐日分點買賣超與新聞線索。最近新聞欄位來自 Google News RSS，只作為人工確認線索。完整 {summary.get("total_trigger_gt5_episodes", 0)} 筆觸發天數案例另存於 <a href="all_trigger_gt5_cases.csv">all_trigger_gt5_cases.csv</a>。</p>
    <section class="grid">
      <div class="metric">案例數<b>{summary.get("case_count", 0)}</b></div>
      <div class="metric">當前可審<b>{summary.get("selected_actionable_cases", 0)}</b></div>
      <div class="metric">觸發門檻<b>&gt; {summary.get("min_trigger_days", "")}</b></div>
      <div class="metric">可列入觀察<b>{summary.get("decision_counts", {}).get("可列入買進觀察", 0)}</b></div>
      <div class="metric">有新聞線索<b>{summary.get("cases_with_news", 0)}</b></div>
    </section>
    <div class="toolbar">
      <input id="q" placeholder="搜尋股票/分點/城市">
      <select id="decision">
        <option value="">全部結論</option>
        <option>可列入買進觀察</option>
        <option>觀察但需等確認</option>
        <option>暫不追價</option>
      </select>
      <select id="scope">
        <option value="">全部分類</option>
        <option>detailed_review_case</option>
        <option>review_eligible_beyond_limit</option>
        <option>ongoing_but_not_review_eligible</option>
        <option>historical_or_ended_episode</option>
      </select>
    </div>
    <table id="cases">
      <thead><tr><th>Rank</th><th>案例</th><th>主分點</th><th>分類</th><th>觸發</th><th>Score</th><th>結論</th><th>純度</th><th>買量占比</th><th>短/中/長趨勢</th><th>城市群聚</th><th>左手警示</th><th>最近新聞</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </main>
  <script id="payload" type="application/json">{html.escape(payload)}</script>
  <script>
    const q = document.getElementById('q');
    const decision = document.getElementById('decision');
    const scope = document.getElementById('scope');
    const rows = [...document.querySelectorAll('#cases tbody tr')];
    function filter() {{
      const term = q.value.trim().toLowerCase();
      const dec = decision.value;
      const scopeValue = scope.value;
      rows.forEach(row => {{
        const text = row.textContent.toLowerCase();
        row.style.display = (!term || text.includes(term)) && (!dec || row.dataset.decision === dec) && (!scopeValue || row.dataset.scope === scopeValue) ? '' : 'none';
      }});
    }}
    q.addEventListener('input', filter);
    decision.addEventListener('change', filter);
    scope.addEventListener('change', filter);
  </script>
</body>
</html>
"""


def write_outputs(
    cases: list[dict[str, object]],
    summary: dict[str, object],
    universe: pd.DataFrame,
    cfg: CaseReviewConfig,
) -> None:
    out_dir = ensure_dir(cfg.output_dir)
    slim_cases = _slim_cases(cases)
    (out_dir / "case_reviews.json").write_text(
        json.dumps({"summary": summary, "cases": slim_cases}, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    pd.DataFrame(slim_cases).to_csv(out_dir / "case_reviews.csv", index=False, encoding="utf-8-sig")
    universe.to_csv(out_dir / "all_trigger_gt5_cases.csv", index=False, encoding="utf-8-sig")
    write_detail_reports(cases, out_dir, cfg.detail_case_limit)
    write_review_report(cases, summary, out_dir)
    (out_dir / "index.html").write_text(_html_page(cases, summary), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build trigger-days > 5 case review portal.")
    parser.add_argument("--episodes", type=Path, default=resolve_output("analysis", "single_branch_persistent_accumulation", "single_branch_persistent_episodes.parquet"))
    parser.add_argument("--daily-triggers", type=Path, default=resolve_output("analysis", "single_branch_persistent_accumulation", "single_branch_persistent_daily_triggers.parquet"))
    parser.add_argument("--general-cases", type=Path, default=resolve_output("analysis", "general_broker_flow_anomaly", "general_broker_flow_anomaly_review_cases.parquet"))
    parser.add_argument("--ohlc", type=Path, default=resolve_data("_derived", "ohlc.parquet"))
    parser.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    parser.add_argument("--company-profiles", nargs="+", type=Path, default=[resolve_data("reference", "company_profiles_twse.json"), resolve_data("reference", "company_profiles_tpex.json")])
    parser.add_argument("--broker-dirs", nargs="+", type=Path, default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")])
    parser.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "trigger_days_gt5_case_review"))
    parser.add_argument("--min-trigger-days", type=int, default=5)
    parser.add_argument("--max-cases", type=int, default=274)
    parser.add_argument("--detail-case-limit", type=int, default=0, help="Maximum detail reports to write; 0 writes every trigger-days case.")
    parser.add_argument("--skip-news", action="store_true", help="Do not fetch Google News RSS headlines.")
    parser.add_argument("--max-news-items", type=int, default=3)
    parser.add_argument("--news-timeout-seconds", type=float, default=8.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = CaseReviewConfig(
        episodes_path=args.episodes,
        daily_triggers_path=args.daily_triggers,
        general_cases_path=args.general_cases,
        ohlc_path=args.ohlc,
        broker_list_path=args.broker_list,
        company_profile_paths=tuple(args.company_profiles),
        broker_dirs=tuple(args.broker_dirs),
        output_dir=args.output_dir,
        min_trigger_days=args.min_trigger_days,
        max_cases=args.max_cases,
        detail_case_limit=args.detail_case_limit,
        fetch_news=not args.skip_news,
        max_news_items=max(0, int(args.max_news_items)),
        news_timeout_seconds=max(1.0, float(args.news_timeout_seconds)),
    )
    cases, summary, universe = build_cases(cfg)
    write_outputs(cases, summary, universe, cfg)
    print(f"Wrote {len(cases)} case reviews to {cfg.output_dir}")


if __name__ == "__main__":
    main()
