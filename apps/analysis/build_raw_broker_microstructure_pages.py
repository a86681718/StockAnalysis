#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stockanalysis.config import ensure_dir, resolve_output


RAW_OHLC_DIR = PROJECT_ROOT / "data" / "ohlc"
OUT_DIR = resolve_output("analysis", "raw_broker_microstructure")
SUMMARY_PATH = OUT_DIR / "raw_microstructure_summary.json"

STRATEGIES: dict[str, dict[str, Any]] = {
    "diffuse_buyer_accumulation": {
        "title": "Diffuse Buyer Accumulation",
        "short": "分散式買盤累積",
        "html": "diffuse_buyer_accumulation_case_explorer.html",
        "theme": "#0b6b57",
        "idea": "找很多分點同步買進、但沒有單一分點過度主導的早期累積型態。",
        "why": "這個策略假設真正可延續的買盤不一定來自單一大戶，而是多個分點同時轉向，代表市場共識正在擴散。",
        "rules": [
            "buyer_count_pct >= 0.99：買方分點廣度排在同日全市場前段。",
            "balance_pct >= 0.95：正向買賣平衡仍需維持高分位。",
            "top_buyer_share <= 0.25：避免單一分點主導。",
            "ret_5d <= 5%、volume_ratio_20 <= 1.5：避免追高與爆量過熱。",
            "訊號隔日開盤進場，持有 60 個交易日，cooldown 20 天。",
        ],
    },
    "role_reversal": {
        "title": "Role Reversal",
        "short": "分點角色反轉",
        "html": "role_reversal_case_explorer.html",
        "theme": "#315f8c",
        "idea": "找先前偏賣的分點突然轉為強買，形成角色反轉的累積訊號。",
        "why": "如果過去賣方分點開始反手買進，可能代表籌碼壓力結束或資訊面態度改變。",
        "rules": [
            "score_pct >= 0.99：角色反轉分數位於同日高分位。",
            "min_count >= 3：至少多個分點同時出現反轉。",
            "price_pos_20 >= 0.4、ret_5d >= 5%：價格已經有基本轉強確認。",
            "volume_ratio_20 <= 1.5：排除過熱爆量。",
            "訊號隔日開盤進場，持有 60 個交易日，cooldown 10 天。",
        ],
    },
    "sell_pressure_absorption": {
        "title": "Sell Pressure Absorption",
        "short": "賣壓吸收",
        "html": "sell_pressure_absorption_case_explorer.html",
        "theme": "#b7791f",
        "idea": "找賣壓很大但價格不弱、且有廣泛買盤吸收的型態。",
        "why": "大量賣壓沒有把價格打下去，代表承接力可能足以吸收籌碼並推動後續行情。",
        "rules": [
            "sell_pressure_pct >= 0.98：賣壓位於同日全市場高分位。",
            "buyer_count_pct >= 0.9：需要足夠買方分點承接。",
            "ret_3d >= 3%、ret_5d >= 5%：價格必須已經轉強。",
            "breakout_gap_20 <= 10%、volume_ratio_20 <= 2.0：避免過度延伸。",
            "訊號隔日開盤進場，持有 60 個交易日，cooldown 10 天。",
        ],
    },
}


def _clean_number(value: Any) -> float:
    if pd.isna(value):
        return math.nan
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if value in {"", "--", "----"}:
            return math.nan
    return float(value)


def _date_from_ohlc_path(path: Path) -> pd.Timestamp:
    return pd.Timestamp(path.stem.split("-", 1)[1])


def _load_raw_ohlc(start: pd.Timestamp, end: pd.Timestamp, symbols: set[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(RAW_OHLC_DIR.glob("*.csv")):
        date = _date_from_ohlc_path(path)
        if date < start or date > end:
            continue
        market = path.stem.split("-", 1)[0]
        raw = pd.read_csv(path, dtype=str)
        rename = {
            "twse": {"證券代號": "symbol", "開盤價": "open", "最高價": "high", "最低價": "low", "收盤價": "close", "成交股數": "volume"},
            "tpex": {"代號": "symbol", "開盤": "open", "最高": "high", "最低": "low", "收盤": "close", "成交股數": "volume"},
        }[market]
        if not set(rename).issubset(raw.columns):
            continue
        df = raw[list(rename)].rename(columns=rename)
        df["symbol"] = df["symbol"].astype(str).str.strip()
        df = df[df["symbol"].isin(symbols)].copy()
        if df.empty:
            continue
        df["date"] = date
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].map(_clean_number)
        frames.append(df)
    if not frames:
        raise RuntimeError("no raw OHLC rows loaded for case explorer")
    return pd.concat(frames, ignore_index=True).dropna(subset=["open", "high", "low", "close"]).sort_values(["symbol", "date"]).reset_index(drop=True)


def _safe_float(value: Any, digits: int = 4) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _safe_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _safe_millions(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value) / 1_000_000, 2)


def _trade_summary(trades: pd.DataFrame, signals: pd.DataFrame) -> dict[str, Any]:
    wins = trades.loc[trades["net_ret"] > 0, "net_ret"]
    losses = trades.loc[trades["net_ret"] < 0, "net_ret"]
    gross_loss = float((-losses).sum()) if not losses.empty else 0.0
    profit_factor = float(wins.sum()) / gross_loss if gross_loss > 0 else math.inf
    return {
        "signals": int(len(signals)),
        "completedTrades": int(len(trades)),
        "pendingSignals": int(len(signals) - len(trades)),
        "winRate": float((trades["net_ret"] > 0).mean()),
        "avgNetRet": float(trades["net_ret"].mean()),
        "medianNetRet": float(trades["net_ret"].median()),
        "maxLoss": float(trades["net_ret"].min()),
        "profitFactor": profit_factor,
        "firstSignal": pd.to_datetime(signals["date"]).min().strftime("%Y-%m-%d"),
        "lastSignal": pd.to_datetime(signals["date"]).max().strftime("%Y-%m-%d"),
    }


def _extract_window(ohlc: pd.DataFrame, row: pd.Series, pre_bars: int = 35, post_bars: int = 10) -> dict[str, Any]:
    sub = ohlc[ohlc["symbol"] == str(row["symbol"])].reset_index(drop=True)
    signal_date = pd.Timestamp(row["signal_date"])
    entry_date = pd.Timestamp(row["entry_date"])
    exit_date = pd.Timestamp(row["exit_date"])
    signal_idx = sub.index[sub["date"] == signal_date]
    exit_idx = sub.index[sub["date"] == exit_date]
    if len(signal_idx) == 0 or len(exit_idx) == 0:
        raise ValueError(f"missing raw OHLC rows for {row['symbol']} {signal_date:%Y-%m-%d}")
    start = max(int(signal_idx[0]) - pre_bars, 0)
    end = min(int(exit_idx[0]) + post_bars, len(sub) - 1)
    window = sub.iloc[start : end + 1].copy()
    dates = window["date"].dt.strftime("%Y-%m-%d").tolist()
    entry_raw = float(row["entry_raw"])
    candles = [[round(float(p.open), 4), round(float(p.close), 4), round(float(p.low), 4), round(float(p.high), 4)] for p in window.itertuples(index=False)]
    volumes = [int(p.volume) if pd.notna(p.volume) else 0 for p in window.itertuples(index=False)]
    ret_path = [round(float(p.close) / entry_raw - 1.0, 4) if entry_raw > 0 else None for p in window.itertuples(index=False)]
    return {"dates": dates, "candles": candles, "volumes": volumes, "retPath": ret_path}


def _case_bucket(row: pd.Series, top_keys: set[tuple[str, str]], worst_keys: set[tuple[str, str]]) -> str:
    key = (str(row["symbol"]), pd.Timestamp(row["signal_date"]).strftime("%Y-%m-%d"))
    if key in top_keys:
        return "top"
    if key in worst_keys:
        return "worst"
    return "win" if float(row["net_ret"]) > 0 else "loss"


def _load_strategy_payload(family: str) -> dict[str, Any]:
    meta = STRATEGIES[family]
    signals = pd.read_csv(OUT_DIR / f"{family}_best_signals.csv")
    trades = pd.read_csv(OUT_DIR / f"{family}_best_trades.csv")
    for frame in (signals, trades):
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(4)
        frame["date"] = pd.to_datetime(frame["date"])
    for col in ["signal_date", "entry_date", "exit_date"]:
        trades[col] = pd.to_datetime(trades[col])
    symbols = set(trades["symbol"].astype(str))
    ohlc = _load_raw_ohlc(trades["signal_date"].min() - pd.Timedelta(days=80), trades["exit_date"].max() + pd.Timedelta(days=25), symbols)
    top_keys = {(str(r.symbol), pd.Timestamp(r.signal_date).strftime("%Y-%m-%d")) for r in trades.sort_values("net_ret", ascending=False).head(8).itertuples(index=False)}
    worst_keys = {(str(r.symbol), pd.Timestamp(r.signal_date).strftime("%Y-%m-%d")) for r in trades.sort_values("net_ret", ascending=True).head(8).itertuples(index=False)}
    cases: list[dict[str, Any]] = []
    for idx, row in trades.sort_values(["signal_date", "symbol"]).reset_index(drop=True).iterrows():
        signal_date = pd.Timestamp(row["signal_date"]).strftime("%Y-%m-%d")
        net_ret = float(row["net_ret"])
        cases.append(
            {
                "id": int(idx),
                "symbol": str(row["symbol"]),
                "signalDate": signal_date,
                "entryDate": pd.Timestamp(row["entry_date"]).strftime("%Y-%m-%d"),
                "exitDate": pd.Timestamp(row["exit_date"]).strftime("%Y-%m-%d"),
                "month": pd.Timestamp(row["signal_date"]).strftime("%Y-%m"),
                "bucket": _case_bucket(row, top_keys, worst_keys),
                "netRet": round(net_ret, 4),
                "exitReason": str(row["exit_reason"]),
                "score": _safe_float(row.get("score")),
                "entryRaw": _safe_float(row.get("entry_raw")),
                "exitRaw": _safe_float(row.get("exit_raw")),
                "ret3d": _safe_float(row.get("ret_3d")),
                "ret5d": _safe_float(row.get("ret_5d")),
                "breakoutGap20": _safe_float(row.get("breakout_gap_20")),
                "pricePos20": _safe_float(row.get("price_pos_20")),
                "volumeRatio20": _safe_float(row.get("volume_ratio_20")),
                "roleReversalScore": _safe_millions(row.get("role_reversal_score")),
                "roleReversalCount": _safe_int(row.get("role_reversal_count")),
                "coordinatedCount": _safe_int(row.get("coordinated_count")),
                "negnetTotalM": _safe_millions(row.get("negnet_total")),
                "buyerCount": _safe_int(row.get("buyer_count")),
                "topBuyerShare": _safe_float(row.get("top_buyer_share")),
                "posNegBalance": _safe_float(row.get("pos_neg_balance")),
                **_extract_window(ohlc, row),
            }
        )
    return {
        "family": family,
        "meta": meta,
        "generatedAt": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "params": json.loads(str(trades.iloc[0]["params_json"])) if not trades.empty else {},
        "summary": _trade_summary(trades, signals),
        "cases": cases,
    }


def _strategy_page_html(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return STRATEGY_TEMPLATE.replace("__DATA__", data)


def _load_optional_csv(name: str) -> pd.DataFrame:
    path = OUT_DIR / "robustness" / name
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _overview_payload(strategy_payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    improvement = pd.read_csv(OUT_DIR / "raw_microstructure_improvement_comparison.csv")
    robustness = _load_optional_csv("robustness_by_family.csv")
    market = _load_optional_csv("market_adjusted_summary.csv")
    walk = _load_optional_csv("raw_microstructure_walk_forward_aggregate.csv")
    cards = []
    for family, payload in strategy_payloads.items():
        imp = improvement[improvement["event_family"] == family].iloc[0].to_dict()
        rob = robustness[robustness["event_family"] == family].iloc[0].to_dict() if not robustness.empty else {}
        mar = market[market["event_family"] == family].iloc[0].to_dict() if not market.empty else {}
        wf = walk[walk["event_family"] == family].iloc[0].to_dict() if not walk.empty else {}
        cards.append({"family": family, "meta": STRATEGIES[family], "summary": payload["summary"], "improvement": imp, "robustness": rob, "market": mar, "walkForward": wf})
    return {"generatedAt": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"), "cards": cards}


def _overview_html(payload: dict[str, Any]) -> str:
    return OVERVIEW_TEMPLATE.replace("__DATA__", json.dumps(payload, ensure_ascii=False))


STRATEGY_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Raw Broker Strategy Case Explorer</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    :root { --bg:#f5f0e4; --panel:#fffaf0; --ink:#1b1914; --muted:#6f675c; --line:#d8cbb8; --green:#0b6b57; --red:#b23a2f; --amber:#b7791f; --blue:#315f8c; }
    * { box-sizing:border-box; } body { margin:0; color:var(--ink); font-family:"Avenir Next","Helvetica Neue",sans-serif; background:radial-gradient(circle at 10% 4%, rgba(11,107,87,.13), transparent 25%), radial-gradient(circle at 90% 0%, rgba(183,121,31,.18), transparent 26%), linear-gradient(180deg,#fbf7ed,var(--bg)); }
    .page { max-width:1500px; margin:0 auto; padding:26px; } .panel { background:rgba(255,250,240,.92); border:1px solid var(--line); border-radius:24px; box-shadow:0 18px 44px rgba(32,25,15,.10); padding:22px; }
    .back { color:var(--muted); text-decoration:none; font-weight:800; } .hero { display:grid; grid-template-columns:1.1fr .9fr; gap:18px; margin:16px 0 18px; }
    h1 { margin:0 0 10px; font-family:Georgia,"Times New Roman",serif; font-size:clamp(2.2rem,5vw,4.8rem); line-height:.92; letter-spacing:-.045em; } h2 { margin:0 0 14px; font-size:1.05rem; } p { margin:0; } .lede { color:var(--muted); line-height:1.65; }
    .kpis,.metric-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-top:18px; } .kpi,.metric { border:1px solid var(--line); border-radius:16px; padding:13px; background:rgba(255,255,255,.55); } .kpi span,.metric span { display:block; color:var(--muted); font-size:.76rem; } .kpi b,.metric b { display:block; font-size:1.3rem; margin-top:4px; }
    .rules { display:grid; gap:8px; color:var(--muted); line-height:1.45; } .rule { border-left:4px solid var(--accent); padding-left:12px; }
    .layout { display:grid; grid-template-columns:420px 1fr; gap:18px; align-items:start; } .filters { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:14px; } input,select { width:100%; border:1px solid var(--line); border-radius:14px; padding:10px 12px; background:#fffdf8; color:var(--ink); }
    .case-list { max-height:760px; overflow:auto; display:grid; gap:8px; padding-right:4px; } .case { border:1px solid var(--line); border-radius:16px; padding:12px; background:rgba(255,255,255,.58); cursor:pointer; display:grid; grid-template-columns:1fr auto; gap:8px; } .case.active { outline:3px solid color-mix(in srgb,var(--accent) 24%, transparent); border-color:var(--accent); }
    .case-title { font-weight:900; } .case-meta { color:var(--muted); font-size:.82rem; margin-top:4px; } .pill { border-radius:999px; padding:4px 8px; font-size:.78rem; font-weight:900; align-self:start; } .pill.win,.pill.top { color:var(--green); background:rgba(11,107,87,.12); } .pill.loss,.pill.worst { color:var(--red); background:rgba(178,58,47,.12); }
    .detail-head { display:flex; justify-content:space-between; gap:16px; align-items:flex-start; margin-bottom:14px; } .symbol { font-family:Georgia,"Times New Roman",serif; font-size:3rem; line-height:.9; } .return { font-size:1.5rem; font-weight:900; } .gain { color:var(--green); } .loss { color:var(--red); }
    .charts { display:grid; grid-template-columns:1.25fr .75fr; gap:14px; } .chart { height:430px; border:1px solid var(--line); border-radius:20px; background:#fffdf8; } .chart.small { height:210px; } .note { margin-top:14px; color:var(--muted); font-size:.88rem; line-height:1.55; }
    @media (max-width:980px) { .hero,.layout,.charts { grid-template-columns:1fr; } .kpis,.metric-grid { grid-template-columns:repeat(2,1fr); } .case-list { max-height:420px; } }
  </style>
</head>
<body><div class="page">
  <a class="back" href="raw_microstructure_strategy_overview.html">← 回策略總覽</a>
  <section class="hero">
    <div class="panel"><h1 id="title"></h1><p class="lede" id="idea"></p><div class="kpis"><div class="kpi"><span>完成交易</span><b id="kTrades">-</b></div><div class="kpi"><span>勝率</span><b id="kWin">-</b></div><div class="kpi"><span>平均報酬</span><b id="kAvg">-</b></div><div class="kpi"><span>訊號期間</span><b id="kRange">-</b></div></div></div>
    <div class="panel rules"><h2>設計理念與規則</h2><p class="lede" id="why"></p><div id="rules"></div></div>
  </section>
  <section class="layout"><aside class="panel"><h2>案例清單</h2><div class="filters"><input id="search" placeholder="搜尋股票代號" /><select id="bucket"><option value="all">全部案例</option><option value="top">Top winners</option><option value="worst">Worst losses</option><option value="win">獲利</option><option value="loss">虧損</option></select><select id="month"></select><select id="sort"><option value="date_desc">日期新到舊</option><option value="ret_desc">報酬高到低</option><option value="ret_asc">報酬低到高</option><option value="score_desc">score 高到低</option></select></div><div class="case-list" id="caseList"></div></aside>
    <main class="panel"><div class="detail-head"><div><div class="symbol" id="symbol">-</div><p class="lede" id="subtitle">-</p></div><div class="return" id="netRet">-</div></div><div class="charts"><div id="priceChart" class="chart"></div><div><div id="returnChart" class="chart small"></div><div id="featureChart" class="chart small" style="margin-top:10px"></div></div></div><div class="metric-grid" id="metrics"></div><p class="note">垂直線標記 Signal、Entry、Exit。特徵值皆為訊號當日 raw broker + raw OHLC 計算結果。</p></main>
  </section>
</div>
<script>
const DATA = __DATA__;
document.documentElement.style.setProperty('--accent', DATA.meta.theme);
const fmtPct=(v,d=1)=>v==null||Number.isNaN(v)?'-':`${(v*100).toFixed(d)}%`;
const fmtNum=(v,d=2)=>v==null||Number.isNaN(v)?'-':Number(v).toLocaleString(undefined,{maximumFractionDigits:d});
let activeId=DATA.cases[0]?.id??null;
const priceChart=echarts.init(document.getElementById('priceChart')); const returnChart=echarts.init(document.getElementById('returnChart')); const featureChart=echarts.init(document.getElementById('featureChart'));
function init(){ const s=DATA.summary; title.textContent=DATA.meta.title; idea.textContent=DATA.meta.idea; why.textContent=DATA.meta.why; rules.innerHTML=DATA.meta.rules.map(r=>`<div class="rule">${r}</div>`).join(''); kTrades.textContent=s.completedTrades; kWin.textContent=fmtPct(s.winRate); kAvg.textContent=fmtPct(s.avgNetRet); kRange.textContent=`${s.firstSignal} ~ ${s.lastSignal}`; const months=[...new Set(DATA.cases.map(c=>c.month))].sort().reverse(); month.innerHTML='<option value="all">全部月份</option>'+months.map(m=>`<option value="${m}">${m}</option>`).join(''); }
function filtered(){ const q=search.value.trim(); const b=bucket.value; const m=month.value; const so=sort.value; let rows=DATA.cases.filter(c=>(!q||c.symbol.includes(q))&&(b==='all'||c.bucket===b||(b==='win'&&c.netRet>0)||(b==='loss'&&c.netRet<=0))&&(m==='all'||c.month===m)); rows=[...rows].sort((a,b)=>{ if(so==='ret_desc')return b.netRet-a.netRet; if(so==='ret_asc')return a.netRet-b.netRet; if(so==='score_desc')return (b.score??0)-(a.score??0); return b.signalDate.localeCompare(a.signalDate); }); return rows; }
function renderList(){ const rows=filtered(); caseList.innerHTML=rows.map(c=>`<div class="case ${c.id===activeId?'active':''}" onclick="selectCase(${c.id})"><div><div class="case-title">${c.symbol} · ${c.signalDate}</div><div class="case-meta">score ${fmtNum(c.score,3)} · buyers ${fmtNum(c.buyerCount,0)} · balance ${fmtNum(c.posNegBalance,3)}</div></div><div class="pill ${c.netRet>0?'win':'loss'}">${fmtPct(c.netRet)}</div></div>`).join('')||'<p class="lede">沒有符合篩選的案例</p>'; if(!rows.some(c=>c.id===activeId)&&rows[0]){ activeId=rows[0].id; renderDetail(); } }
window.selectCase=(id)=>{ activeId=id; renderList(); renderDetail(); };
function markLines(c){ return [{xAxis:c.signalDate,name:'Signal',lineStyle:{color:'#b7791f',type:'solid'}},{xAxis:c.entryDate,name:'Entry',lineStyle:{color:'#0b6b57',type:'dashed'}},{xAxis:c.exitDate,name:'Exit',lineStyle:{color:'#b23a2f',type:'dashed'}}]; }
function renderDetail(){ const c=DATA.cases.find(x=>x.id===activeId)||DATA.cases[0]; if(!c)return; symbol.textContent=c.symbol; subtitle.textContent=`Signal ${c.signalDate} · Entry ${c.entryDate} @ ${fmtNum(c.entryRaw)} · Exit ${c.exitDate} @ ${fmtNum(c.exitRaw)} · ${c.exitReason}`; netRet.textContent=fmtPct(c.netRet,2); netRet.className=`return ${c.netRet>=0?'gain':'loss'}`;
 priceChart.setOption({animation:false,tooltip:{trigger:'axis'},grid:[{left:54,right:20,top:32,height:260},{left:54,right:20,top:320,height:70}],xAxis:[{type:'category',data:c.dates,boundaryGap:false},{type:'category',data:c.dates,gridIndex:1,boundaryGap:false}],yAxis:[{scale:true},{gridIndex:1}],dataZoom:[{type:'inside',xAxisIndex:[0,1]},{type:'slider',xAxisIndex:[0,1],bottom:0}],series:[{name:'OHLC',type:'candlestick',data:c.candles,itemStyle:{color:'#b23a2f',color0:'#0b6b57',borderColor:'#b23a2f',borderColor0:'#0b6b57'},markLine:{symbol:'none',data:markLines(c),label:{formatter:'{b}'}}},{name:'Volume',type:'bar',xAxisIndex:1,yAxisIndex:1,data:c.volumes,itemStyle:{color:'rgba(49,95,140,.35)'}}]});
 returnChart.setOption({animation:false,tooltip:{trigger:'axis',valueFormatter:v=>fmtPct(v,2)},grid:{left:48,right:14,top:28,bottom:34},xAxis:{type:'category',data:c.dates},yAxis:{axisLabel:{formatter:v=>`${(v*100).toFixed(0)}%`}},series:[{name:'Return from entry',type:'line',data:c.retPath,smooth:true,areaStyle:{color:'rgba(11,107,87,.12)'},lineStyle:{color:DATA.meta.theme},markLine:{symbol:'none',data:markLines(c)}}]});
 const feats=[['買方分點數',c.buyerCount],['買賣平衡',c.posNegBalance],['最大買方占比',c.topBuyerShare],['價格位置20日',c.pricePos20],['量能比20日',c.volumeRatio20],['5日報酬',c.ret5d]]; featureChart.setOption({animation:false,tooltip:{trigger:'axis'},grid:{left:110,right:18,top:20,bottom:24},xAxis:{type:'value'},yAxis:{type:'category',data:feats.map(x=>x[0])},series:[{type:'bar',data:feats.map(x=>x[1]),itemStyle:{color:DATA.meta.theme},label:{show:true,position:'right',formatter:p=>fmtNum(p.value,3)}}]});
 const metrics=[['score',fmtNum(c.score,4)],['買方分點數',fmtNum(c.buyerCount,0)],['最大買方占比',fmtPct(c.topBuyerShare,2)],['買賣平衡',fmtNum(c.posNegBalance,3)],['賣方淨額(百萬)',fmtNum(c.negnetTotalM,2)],['角色反轉分數(百萬)',fmtNum(c.roleReversalScore,2)],['3日報酬',fmtPct(c.ret3d,2)],['5日報酬',fmtPct(c.ret5d,2)],['20日突破距離',fmtPct(c.breakoutGap20,2)],['20日價格位置',fmtNum(c.pricePos20,3)],['20日量能比',fmtNum(c.volumeRatio20,3)],['角色反轉分點數',fmtNum(c.roleReversalCount,0)]]; metricsEl=document.getElementById('metrics'); metricsEl.innerHTML=metrics.map(([k,v])=>`<div class="metric"><span>${k}</span><b>${v}</b></div>`).join(''); }
['search','bucket','month','sort'].forEach(id=>document.getElementById(id).addEventListener('input',renderList)); window.addEventListener('resize',()=>{priceChart.resize();returnChart.resize();featureChart.resize();}); init(); renderList(); renderDetail();
</script></body></html>"""


OVERVIEW_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-Hant"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" /><title>Raw Broker Microstructure Strategies</title>
<style>
:root{--bg:#f1eadc;--panel:#fffaf0;--ink:#181611;--muted:#70685e;--line:#d7c9b5;--green:#0b6b57;--red:#b23a2f;--amber:#b7791f;--blue:#315f8c}*{box-sizing:border-box}body{margin:0;color:var(--ink);font-family:"Avenir Next","Helvetica Neue",sans-serif;background:radial-gradient(circle at 15% 8%,rgba(11,107,87,.18),transparent 27%),radial-gradient(circle at 82% 0%,rgba(183,121,31,.20),transparent 25%),linear-gradient(180deg,#fbf7ed,var(--bg))}.page{max-width:1480px;margin:0 auto;padding:30px}.hero{display:grid;grid-template-columns:1.2fr .8fr;gap:18px;margin-bottom:18px}.panel,.card{background:rgba(255,250,240,.92);border:1px solid var(--line);border-radius:26px;box-shadow:0 20px 50px rgba(32,25,15,.10);padding:24px}h1{font-family:Georgia,"Times New Roman",serif;font-size:clamp(2.8rem,6vw,6rem);line-height:.88;letter-spacing:-.055em;margin:0 0 14px}h2{margin:0 0 12px}.lede{color:var(--muted);line-height:1.7;margin:0}.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:18px}.card{display:flex;flex-direction:column;gap:16px}.tag{display:inline-flex;align-self:flex-start;border-radius:999px;padding:6px 10px;font-size:.78rem;font-weight:900;color:#fff;background:var(--accent)}.kpis,.checks{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.kpi{border:1px solid var(--line);border-radius:16px;background:rgba(255,255,255,.55);padding:12px}.kpi span{display:block;color:var(--muted);font-size:.74rem}.kpi b{display:block;font-size:1.25rem;margin-top:4px}.rules{display:grid;gap:8px;color:var(--muted);line-height:1.45}.rule{border-left:4px solid var(--accent);padding-left:10px}.link{display:block;text-align:center;text-decoration:none;color:white;background:var(--accent);font-weight:900;border-radius:16px;padding:13px;margin-top:auto}.warn{color:var(--red);font-weight:900}.ok{color:var(--green);font-weight:900}@media(max-width:1050px){.hero,.cards{grid-template-columns:1fr}.page{padding:18px}}
</style></head><body><div class="page"><section class="hero"><div class="panel"><h1>Raw Broker Microstructure Strategies</h1><p class="lede">三個只使用 raw OHLC 與券商分點買賣資料建立的交易策略。這個頁面整理策略設計理念、最佳化後績效、robustness、walk-forward 與市場調整結果；每張卡片可以點進去看個別案例 K 線、報酬路徑與分點特徵。</p></div><div class="panel"><h2>目前驗證摘要</h2><p class="lede">三個策略的 rolling walk-forward aggregate out-of-sample 都通過；扣除全市場等權 benchmark 後也都仍通過。不過資料期間仍短，且部分策略有獲利集中度 watch。</p></div></section><section class="cards" id="cards"></section></div>
<script>
const DATA=__DATA__; const fmtPct=(v,d=1)=>v==null||Number.isNaN(v)?'-':`${(v*100).toFixed(d)}%`; const fmtNum=(v,d=2)=>v==null||Number.isNaN(v)?'-':Number(v).toLocaleString(undefined,{maximumFractionDigits:d});
function card(c){const m=c.meta,s=c.summary,i=c.improvement,r=c.robustness||{},ma=c.market||{},wf=c.walkForward||{};const accent=m.theme;return `<article class="card" style="--accent:${accent}"><span class="tag">${m.short}</span><h2>${m.title}</h2><p class="lede">${m.idea}</p><div class="kpis"><div class="kpi"><span>交易數</span><b>${s.completedTrades}</b></div><div class="kpi"><span>勝率</span><b>${fmtPct(s.winRate)}</b></div><div class="kpi"><span>平均報酬</span><b>${fmtPct(s.avgNetRet)}</b></div><div class="kpi"><span>改善幅度</span><b>${fmtPct(i.avg_net_ret_improvement)}</b></div><div class="kpi"><span>Robustness</span><b class="${r.verdict==='robust'?'ok':'warn'}">${r.verdict??'-'}</b></div><div class="kpi"><span>Walk-forward OOS</span><b class="${wf.aggregate_test_target_pass?'ok':'warn'}">${wf.aggregate_test_target_pass?'PASS':'-'}</b></div><div class="kpi"><span>Excess Avg</span><b>${fmtPct(ma.excess_avg_net_ret)}</b></div><div class="kpi"><span>Excess Win</span><b>${fmtPct(ma.excess_win_rate)}</b></div></div><div><h2>設計理念</h2><p class="lede">${m.why}</p></div><div class="rules">${m.rules.map(x=>`<div class="rule">${x}</div>`).join('')}</div><a class="link" href="${m.html}">深入看案例分析</a></article>`}
cards.innerHTML=DATA.cards.map(card).join('');
</script></body></html>"""


def main() -> None:
    ensure_dir(OUT_DIR)
    payloads = {family: _load_strategy_payload(family) for family in STRATEGIES}
    for family, payload in payloads.items():
        path = OUT_DIR / STRATEGIES[family]["html"]
        path.write_text(_strategy_page_html(payload), encoding="utf-8")
        print(path)
    overview = OUT_DIR / "raw_microstructure_strategy_overview.html"
    overview.write_text(_overview_html(_overview_payload(payloads)), encoding="utf-8")
    print(overview)


if __name__ == "__main__":
    main()
