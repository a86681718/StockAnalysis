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
    return f"all_cases/{int(_num(rank)):04d}_{symbol}_{broker}_{_date(episode_start)}.md"


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


def write_detail_reports(cases: list[dict[str, object]], out_dir: Path, limit: int) -> None:
    case_dir = ensure_dir(out_dir / "all_cases")
    selected_cases = cases if limit <= 0 else cases[:limit]
    for case in selected_cases:
        lines = [
            f"# {case['symbol']} {case['name']} - 觸發天數 > 5 案例分析",
            "",
            f"- 評估結論: **{case['decision']}** (score `{case['score']}`)",
            f"- 案例分類: `{case['scope_note']}`; review eligible `{'yes' if case['review_eligible'] else 'no'}`; ongoing `{'yes' if case['ongoing'] else 'no'}`",
            f"- 案例期間: `{case['episode_start']}` ~ `{case['last_qualifying_date']}`; trigger days `{case['trigger_days']}`",
            f"- 狀態基準日: `{case['status_as_of']}`; episode status `{case['status']}`",
            f"- 主分點: `{case['broker_name']}` (`{case['broker']}`)",
            "",
            "## 分析面向",
            "",
            "1. 新聞: 擷取 Google News RSS 最近標題作為新聞線索；利多利空仍需點進來源人工確認。",
            "2. 主力買賣超力道: 看累積買超、純度、買量占比、正買超天數與是否同時出現在一般主力流異常。",
            "3. 趨勢: 分短線 5/20 日、中線 20 日、長線 60 日與 60 日價格位置評估追價風險。",
            "4. 券商群聚: 以買超券商地址城市估算是否集中同一縣市。",
            "5. 均價: 用分點買進金額除以買進股數估算主分點均價，與最新收盤價比較。",
            "6. 左手換右手: 用同分點買賣同時放大的 cross ratio 當作警示，不直接判定作假。",
            "",
            "## 量化摘要",
            "",
            f"- 累積買超: `{_fmt_int(case['cumulative_net_buy'])}` 股",
            f"- 買超純度: `{_fmt_pct(case['cumulative_purity'])}`; 買量占比 `{_fmt_pct(case['cumulative_buy_share'])}`; 留倉率 `{_fmt_pct(case['retention_ratio'])}`",
            f"- 正買超天數: `{case['positive_net_sessions']}/{case['observed_sessions']}`",
            f"- 一般主力流異常交叉命中: `{'yes' if case['general_flow_hit'] else 'no'}`",
            "",
            "## 趨勢",
            "",
            f"- 最新收盤: `{_fmt_float(case['trend']['latest_close'])}`",
            f"- 短線: `{case['trend']['short_trend']}`; 中線: `{case['trend']['mid_trend']}`; 長線: `{case['trend']['long_trend']}`",
            f"- 5/20/60 日報酬: `{_fmt_pct(case['trend']['ret5'])}` / `{_fmt_pct(case['trend']['ret20'])}` / `{_fmt_pct(case['trend']['ret60'])}`",
            f"- 60 日價格位置: `{_fmt_pct(case['trend']['price_position_60'])}`",
            "",
            "## 券商與均價",
            "",
            f"- 主分點估計買進均價: `{_fmt_float(case['broker_stats']['target_avg_buy_price'])}`",
            f"- 主分點 episode 淨買超: `{_fmt_int(case['broker_stats']['target_net_buy'])}` 股",
            f"- 買方最大城市: `{case['broker_stats']['dominant_city'] or 'n/a'}`; 城市占比 `{_fmt_pct(case['broker_stats']['dominant_city_share'])}`",
            f"- 同分點買賣交叉比: `{_fmt_pct(case['broker_stats']['same_broker_cross_ratio'])}`; 左手換右手警示 `{'yes' if case['broker_stats']['left_right_flag'] else 'no'}`",
            "",
            "## Top 買超分點",
            "",
        ]
        for broker in case["broker_stats"]["top_brokers"]:
            lines.append(
                f"- `{broker['broker_name']}` `{broker['broker_city'] or 'n/a'}` "
                f"net=`{_fmt_int(broker['net_buy'])}` avg=`{_fmt_float(broker['avg_buy_price'])}`"
            )
        lines.extend(["", "## 正面依據", ""])
        lines.extend([f"- {item}" for item in case["positives"]] or ["- 無明顯正面加分。"])
        lines.extend(["", "## 風險與待查", ""])
        lines.extend([f"- {item}" for item in case["risks"]] or ["- 未觸發主要量化風險。"])
        lines.extend(["", "## 最近新聞線索", ""])
        if case["news_items"]:
            for item in case["news_items"]:
                suffix = f" ({item['published']})" if item.get("published") else ""
                source = f" - {item['source']}" if item.get("source") else ""
                lines.append(f"- [{item['title']}]({item['url']}){source}{suffix}")
        else:
            lines.append("- 未擷取到 Google News RSS 標題；請使用下方入口人工查核。")
        lines.extend(["", "## 新聞入口", ""])
        for link in case["news_links"]:
            lines.append(f"- [{link['label']}]({link['url']})")
        (case_dir / Path(str(case["detail_path"])).name).write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    payload = json.dumps({"summary": summary, "cases": cases}, ensure_ascii=False, default=_json_default)
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
    <p class="note">入口頁先用可重現的本地量化欄位排序，每個案例可點入 markdown 細報。最近新聞欄位來自 Google News RSS，只作為人工確認線索。完整 {summary.get("total_trigger_gt5_episodes", 0)} 筆觸發天數案例另存於 <a href="all_trigger_gt5_cases.csv">all_trigger_gt5_cases.csv</a>。</p>
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
    (out_dir / "case_reviews.json").write_text(
        json.dumps({"summary": summary, "cases": cases}, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    pd.DataFrame(cases).to_csv(out_dir / "case_reviews.csv", index=False, encoding="utf-8-sig")
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
