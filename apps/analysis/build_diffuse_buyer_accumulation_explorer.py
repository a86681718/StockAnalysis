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
SIGNALS_PATH = OUT_DIR / "diffuse_buyer_accumulation_best_signals.csv"
TRADES_PATH = OUT_DIR / "diffuse_buyer_accumulation_best_trades.csv"
SUMMARY_PATH = OUT_DIR / "raw_microstructure_summary.json"
OUTPUT_HTML = OUT_DIR / "diffuse_buyer_accumulation_case_explorer.html"


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
        raise RuntimeError("no raw OHLC rows loaded for diffuse explorer")
    out = pd.concat(frames, ignore_index=True)
    return out.dropna(subset=["open", "high", "low", "close"]).sort_values(["symbol", "date"]).reset_index(drop=True)


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


def _summary(trades: pd.DataFrame, signals: pd.DataFrame) -> dict[str, Any]:
    wins = trades.loc[trades["net_ret"] > 0, "net_ret"]
    losses = trades.loc[trades["net_ret"] < 0, "net_ret"]
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float((-losses).sum()) if not losses.empty else 0.0
    avg_win = float(wins.mean()) if not wins.empty else math.nan
    avg_loss = float((-losses).mean()) if not losses.empty else math.nan
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else math.inf
    payoff_ratio = avg_win / avg_loss if math.isfinite(avg_win) and math.isfinite(avg_loss) and avg_loss > 0 else math.inf
    return {
        "signals": int(len(signals)),
        "completedTrades": int(len(trades)),
        "pendingSignals": int(len(signals) - len(trades)),
        "winRate": float((trades["net_ret"] > 0).mean()),
        "avgNetRet": float(trades["net_ret"].mean()),
        "medianNetRet": float(trades["net_ret"].median()),
        "maxLoss": float(trades["net_ret"].min()),
        "profitFactor": float(profit_factor),
        "payoffRatio": float(payoff_ratio),
        "firstSignal": pd.to_datetime(signals["date"]).min().strftime("%Y-%m-%d"),
        "lastSignal": pd.to_datetime(signals["date"]).max().strftime("%Y-%m-%d"),
    }


def _case_bucket(row: pd.Series, top_keys: set[tuple[str, str]], worst_keys: set[tuple[str, str]]) -> str:
    key = (str(row["symbol"]), pd.Timestamp(row["signal_date"]).strftime("%Y-%m-%d"))
    if key in top_keys:
        return "top"
    if key in worst_keys:
        return "worst"
    return "win" if float(row["net_ret"]) > 0 else "loss"


def _extract_window(ohlc: pd.DataFrame, row: pd.Series, pre_bars: int = 35, post_bars: int = 10) -> dict[str, Any]:
    sub = ohlc[ohlc["symbol"] == str(row["symbol"])].reset_index(drop=True)
    signal_date = pd.Timestamp(row["signal_date"])
    entry_date = pd.Timestamp(row["entry_date"])
    exit_date = pd.Timestamp(row["exit_date"])
    signal_idx = sub.index[sub["date"] == signal_date]
    exit_idx = sub.index[sub["date"] == exit_date]
    if len(signal_idx) == 0 or len(exit_idx) == 0:
        raise ValueError(f"missing raw OHLC rows for {row['symbol']} {signal_date:%Y-%m-%d}")
    signal_idx_i = int(signal_idx[0])
    exit_idx_i = int(exit_idx[0])
    start = max(signal_idx_i - pre_bars, 0)
    end = min(exit_idx_i + post_bars, len(sub) - 1)
    window = sub.iloc[start : end + 1].copy()

    dates = window["date"].dt.strftime("%Y-%m-%d").tolist()
    entry_raw = float(row["entry_raw"])
    candles: list[list[float]] = []
    volumes: list[int] = []
    ret_path: list[float | None] = []
    for _, price in window.iterrows():
        candles.append(
            [
                round(float(price["open"]), 4),
                round(float(price["close"]), 4),
                round(float(price["low"]), 4),
                round(float(price["high"]), 4),
            ]
        )
        volumes.append(int(price["volume"]) if pd.notna(price["volume"]) else 0)
        ret_path.append(round(float(price["close"]) / entry_raw - 1.0, 4) if entry_raw > 0 else None)
    return {
        "dates": dates,
        "candles": candles,
        "volumes": volumes,
        "retPath": ret_path,
        "signalPos": dates.index(signal_date.strftime("%Y-%m-%d")),
        "entryPos": dates.index(entry_date.strftime("%Y-%m-%d")),
        "exitPos": dates.index(exit_date.strftime("%Y-%m-%d")),
    }


def _load_payload() -> dict[str, Any]:
    signals = pd.read_csv(SIGNALS_PATH)
    trades = pd.read_csv(TRADES_PATH)
    for frame in (signals, trades):
        frame["symbol"] = frame["symbol"].astype(str).str.zfill(4)
        frame["date"] = pd.to_datetime(frame["date"])
    for col in ["signal_date", "entry_date", "exit_date"]:
        trades[col] = pd.to_datetime(trades[col])

    summary_json = json.loads(SUMMARY_PATH.read_text(encoding="utf-8")) if SUMMARY_PATH.exists() else {}
    symbols = set(trades["symbol"].astype(str))
    ohlc_start = trades["signal_date"].min() - pd.Timedelta(days=80)
    ohlc_end = trades["exit_date"].max() + pd.Timedelta(days=25)
    ohlc = _load_raw_ohlc(ohlc_start, ohlc_end, symbols)

    top_keys = {
        (str(r["symbol"]), pd.Timestamp(r["signal_date"]).strftime("%Y-%m-%d"))
        for _, r in trades.sort_values("net_ret", ascending=False).head(8).iterrows()
    }
    worst_keys = {
        (str(r["symbol"]), pd.Timestamp(r["signal_date"]).strftime("%Y-%m-%d"))
        for _, r in trades.sort_values("net_ret", ascending=True).head(8).iterrows()
    }

    cases: list[dict[str, Any]] = []
    for idx, row in trades.sort_values(["signal_date", "symbol"]).reset_index(drop=True).iterrows():
        window = _extract_window(ohlc, row)
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
                "label": f"{signal_date} {row['symbol']} {net_ret:+.2%}",
                "netRet": round(net_ret, 4),
                "exitReason": str(row["exit_reason"]),
                "score": _safe_float(row.get("score")),
                "close": _safe_float(row.get("close")),
                "entryRaw": _safe_float(row.get("entry_raw")),
                "exitRaw": _safe_float(row.get("exit_raw")),
                "ret3d": _safe_float(row.get("ret_3d")),
                "ret5d": _safe_float(row.get("ret_5d")),
                "breakoutGap20": _safe_float(row.get("breakout_gap_20")),
                "volumeRatio20": _safe_float(row.get("volume_ratio_20")),
                "negnetTotalM": _safe_millions(row.get("negnet_total")),
                "buyerCount": _safe_int(row.get("buyer_count")),
                "topBuyerShare": _safe_float(row.get("top_buyer_share")),
                "posNegBalance": _safe_float(row.get("pos_neg_balance")),
                **window,
            }
        )

    params = json.loads(str(trades.iloc[0]["params_json"])) if not trades.empty else {}
    return {
        "generatedAt": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": {
            "signals": str(SIGNALS_PATH.relative_to(PROJECT_ROOT)),
            "trades": str(TRADES_PATH.relative_to(PROJECT_ROOT)),
            "ohlc": "data/ohlc/*.csv",
        },
        "rawSummary": summary_json,
        "params": params,
        "summary": _summary(trades, signals),
        "cases": cases,
    }


def _render_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Diffuse Buyer Accumulation Case Explorer</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    :root {{
      --bg: #f3efe4;
      --panel: #fffaf0;
      --ink: #1b1914;
      --muted: #71695f;
      --line: #d8cbb8;
      --green: #0b6b57;
      --red: #b23a2f;
      --amber: #b7791f;
      --blue: #315f8c;
      --shadow: rgba(32, 25, 15, 0.10);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      background:
        radial-gradient(circle at 12% 10%, rgba(11, 107, 87, .15), transparent 28%),
        radial-gradient(circle at 82% 3%, rgba(183, 121, 31, .16), transparent 25%),
        linear-gradient(180deg, #faf6ed 0%, var(--bg) 78%);
    }}
    .page {{ max-width: 1500px; margin: 0 auto; padding: 26px; }}
    .hero {{
      display: grid;
      grid-template-columns: 1.1fr .9fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    .panel {{
      background: color-mix(in srgb, var(--panel) 92%, white 8%);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: 0 18px 44px var(--shadow);
      padding: 22px;
    }}
    h1 {{ margin: 0 0 10px; font-family: Georgia, "Times New Roman", serif; font-size: clamp(2rem, 4vw, 4rem); line-height: .95; letter-spacing: -.04em; }}
    h2 {{ margin: 0 0 14px; font-size: 1.05rem; }}
    p {{ margin: 0; }}
    .lede {{ color: var(--muted); line-height: 1.65; max-width: 820px; }}
    .kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 18px; }}
    .kpi {{ border: 1px solid var(--line); border-radius: 18px; padding: 14px; background: rgba(255,255,255,.45); }}
    .kpi span {{ display:block; color: var(--muted); font-size:.78rem; }}
    .kpi b {{ display:block; font-size:1.45rem; margin-top:4px; }}
    .rules {{ display:grid; gap: 8px; color: var(--muted); line-height:1.45; }}
    .rule {{ border-left: 4px solid var(--green); padding-left: 12px; }}
    .layout {{ display:grid; grid-template-columns: 420px 1fr; gap: 18px; align-items:start; }}
    .filters {{ display:grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 14px; }}
    input, select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 10px 12px;
      background: #fffdf8;
      color: var(--ink);
    }}
    .case-list {{ max-height: 760px; overflow:auto; display:grid; gap: 8px; padding-right: 4px; }}
    .case {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      background: rgba(255,255,255,.58);
      cursor: pointer;
      display:grid;
      grid-template-columns: 1fr auto;
      gap: 8px;
    }}
    .case.active {{ outline: 3px solid rgba(11,107,87,.22); border-color: var(--green); }}
    .case-title {{ font-weight: 800; }}
    .case-meta {{ color: var(--muted); font-size: .82rem; margin-top: 4px; }}
    .pill {{ border-radius: 999px; padding: 4px 8px; font-size: .78rem; font-weight: 800; align-self:start; }}
    .pill.win, .pill.top {{ color: var(--green); background: rgba(11,107,87,.12); }}
    .pill.loss, .pill.worst {{ color: var(--red); background: rgba(178,58,47,.12); }}
    .detail-head {{ display:flex; justify-content:space-between; gap: 16px; align-items:flex-start; margin-bottom: 14px; }}
    .symbol {{ font-family: Georgia, "Times New Roman", serif; font-size: 3rem; line-height: .9; }}
    .return {{ font-size: 1.5rem; font-weight: 900; }}
    .gain {{ color: var(--green); }}
    .loss {{ color: var(--red); }}
    .charts {{ display:grid; grid-template-columns: 1.25fr .75fr; gap: 14px; }}
    .chart {{ height: 430px; border: 1px solid var(--line); border-radius: 20px; background: #fffdf8; }}
    .chart.small {{ height: 210px; }}
    .metric-grid {{ display:grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 14px; }}
    .metric {{ border: 1px solid var(--line); border-radius: 16px; padding: 12px; background: rgba(255,255,255,.52); }}
    .metric span {{ display:block; color: var(--muted); font-size: .76rem; }}
    .metric b {{ display:block; margin-top: 5px; font-size: 1.05rem; }}
    .note {{ margin-top: 14px; color: var(--muted); font-size: .88rem; line-height:1.55; }}
    @media (max-width: 980px) {{
      .hero, .layout, .charts {{ grid-template-columns: 1fr; }}
      .kpis, .metric-grid {{ grid-template-columns: repeat(2, 1fr); }}
      .case-list {{ max-height: 420px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="panel">
        <h1>Diffuse Buyer Accumulation</h1>
        <p class="lede">追蹤「分散式買盤累積」案例：買方分點數量高、正向買賣平衡高、最大買方占比受控，避免單一分點爆量造成誤判。所有案例來自 raw OHLC CSV 與 raw broker parquet 產生的策略輸出。</p>
        <div class="kpis">
          <div class="kpi"><span>完成交易</span><b id="kTrades">-</b></div>
          <div class="kpi"><span>勝率</span><b id="kWin">-</b></div>
          <div class="kpi"><span>平均報酬</span><b id="kAvg">-</b></div>
          <div class="kpi"><span>訊號期間</span><b id="kRange">-</b></div>
        </div>
      </div>
      <div class="panel rules">
        <h2>策略規則</h2>
        <div class="rule">買方分點廣度位於橫截面高分位：<b>buyer_count_pct ≥ 0.98</b></div>
        <div class="rule">正向買賣平衡位於橫截面高分位：<b>balance_pct ≥ 0.98</b></div>
        <div class="rule">最大買方占比受控：<b>top_buyer_share ≤ 0.25</b></div>
        <div class="rule">避免過熱追價：<b>ret_5d ≤ 15%、volume_ratio_20 ≤ 1.5</b></div>
        <div class="rule">交易：訊號隔日開盤進場，持有 <b>40</b> 個交易日，同股 cooldown <b>10</b> 天。</div>
      </div>
    </section>

    <section class="layout">
      <aside class="panel">
        <h2>案例清單</h2>
        <div class="filters">
          <input id="search" placeholder="搜尋股票代號" />
          <select id="bucket">
            <option value="all">全部案例</option>
            <option value="top">Top winners</option>
            <option value="worst">Worst losses</option>
            <option value="win">獲利</option>
            <option value="loss">虧損</option>
          </select>
          <select id="month"></select>
          <select id="sort">
            <option value="date_desc">日期新到舊</option>
            <option value="ret_desc">報酬高到低</option>
            <option value="ret_asc">報酬低到高</option>
            <option value="score_desc">score 高到低</option>
          </select>
        </div>
        <div class="case-list" id="caseList"></div>
      </aside>
      <main class="panel">
        <div class="detail-head">
          <div>
            <div class="symbol" id="symbol">-</div>
            <p class="lede" id="subtitle">-</p>
          </div>
          <div class="return" id="netRet">-</div>
        </div>
        <div class="charts">
          <div id="priceChart" class="chart"></div>
          <div>
            <div id="returnChart" class="chart small"></div>
            <div id="featureChart" class="chart small" style="margin-top:10px"></div>
          </div>
        </div>
        <div class="metric-grid" id="metrics"></div>
        <p class="note">垂直線標記訊號日、進場日、出場日。特徵值是訊號當日的 raw 分點結構：買方廣度、最大買方占比、買賣平衡、賣方淨額、近期價格與量能。</p>
      </main>
    </section>
  </div>

  <script>
    const DATA = {payload_json};
    const fmtPct = (v, digits=1) => v == null || Number.isNaN(v) ? '-' : `${{(v*100).toFixed(digits)}}%`;
    const fmtNum = (v, digits=2) => v == null || Number.isNaN(v) ? '-' : Number(v).toLocaleString(undefined, {{maximumFractionDigits: digits}});
    let activeId = DATA.cases[0]?.id ?? null;
    const priceChart = echarts.init(document.getElementById('priceChart'));
    const returnChart = echarts.init(document.getElementById('returnChart'));
    const featureChart = echarts.init(document.getElementById('featureChart'));

    function initSummary() {{
      const s = DATA.summary;
      document.getElementById('kTrades').textContent = s.completedTrades;
      document.getElementById('kWin').textContent = fmtPct(s.winRate);
      document.getElementById('kAvg').textContent = fmtPct(s.avgNetRet);
      document.getElementById('kRange').textContent = `${{s.firstSignal}} ~ ${{s.lastSignal}}`;
      const months = [...new Set(DATA.cases.map(c => c.month))].sort().reverse();
      document.getElementById('month').innerHTML = '<option value="all">全部月份</option>' + months.map(m => `<option value="${{m}}">${{m}}</option>`).join('');
    }}

    function filteredCases() {{
      const q = document.getElementById('search').value.trim();
      const bucket = document.getElementById('bucket').value;
      const month = document.getElementById('month').value;
      const sort = document.getElementById('sort').value;
      let rows = DATA.cases.filter(c => (!q || c.symbol.includes(q)) && (bucket === 'all' || c.bucket === bucket || (bucket === 'win' && c.netRet > 0) || (bucket === 'loss' && c.netRet <= 0)) && (month === 'all' || c.month === month));
      rows = [...rows].sort((a,b) => {{
        if (sort === 'ret_desc') return b.netRet - a.netRet;
        if (sort === 'ret_asc') return a.netRet - b.netRet;
        if (sort === 'score_desc') return (b.score ?? 0) - (a.score ?? 0);
        return b.signalDate.localeCompare(a.signalDate);
      }});
      return rows;
    }}

    function renderList() {{
      const rows = filteredCases();
      const list = document.getElementById('caseList');
      list.innerHTML = rows.map(c => `
        <div class="case ${{c.id === activeId ? 'active' : ''}}" onclick="selectCase(${{c.id}})">
          <div>
            <div class="case-title">${{c.symbol}} · ${{c.signalDate}}</div>
            <div class="case-meta">score ${{fmtNum(c.score, 3)}} · buyers ${{fmtNum(c.buyerCount, 0)}} · top share ${{fmtPct(c.topBuyerShare)}}</div>
          </div>
          <div class="pill ${{c.netRet > 0 ? 'win' : 'loss'}}">${{fmtPct(c.netRet)}}</div>
        </div>
      `).join('') || '<p class="lede">沒有符合篩選的案例</p>';
      if (!rows.some(c => c.id === activeId) && rows[0]) {{
        activeId = rows[0].id;
        renderDetail();
      }}
    }}

    window.selectCase = (id) => {{
      activeId = id;
      renderList();
      renderDetail();
    }};

    function markLines(c) {{
      return [
        {{xAxis: c.signalDate, name: 'Signal', lineStyle: {{color: '#b7791f', type: 'solid'}}}},
        {{xAxis: c.entryDate, name: 'Entry', lineStyle: {{color: '#0b6b57', type: 'dashed'}}}},
        {{xAxis: c.exitDate, name: 'Exit', lineStyle: {{color: '#b23a2f', type: 'dashed'}}}},
      ];
    }}

    function renderDetail() {{
      const c = DATA.cases.find(x => x.id === activeId) || DATA.cases[0];
      if (!c) return;
      document.getElementById('symbol').textContent = c.symbol;
      document.getElementById('subtitle').textContent = `Signal ${{c.signalDate}} · Entry ${{c.entryDate}} @ ${{fmtNum(c.entryRaw)}} · Exit ${{c.exitDate}} @ ${{fmtNum(c.exitRaw)}} · ${{c.exitReason}}`;
      const ret = document.getElementById('netRet');
      ret.textContent = fmtPct(c.netRet, 2);
      ret.className = `return ${{c.netRet >= 0 ? 'gain' : 'loss'}}`;

      priceChart.setOption({{
        animation: false,
        tooltip: {{trigger: 'axis'}},
        grid: [{{left: 54, right: 20, top: 32, height: 260}}, {{left: 54, right: 20, top: 320, height: 70}}],
        xAxis: [{{type:'category', data:c.dates, boundaryGap:false}}, {{type:'category', data:c.dates, gridIndex:1, boundaryGap:false}}],
        yAxis: [{{scale:true}}, {{gridIndex:1}}],
        dataZoom: [{{type:'inside', xAxisIndex:[0,1]}}, {{type:'slider', xAxisIndex:[0,1], bottom: 0}}],
        series: [
          {{name:'OHLC', type:'candlestick', data:c.candles, itemStyle:{{color:'#b23a2f', color0:'#0b6b57', borderColor:'#b23a2f', borderColor0:'#0b6b57'}}, markLine:{{symbol:'none', data:markLines(c), label:{{formatter:'{{b}}'}}}}}},
          {{name:'Volume', type:'bar', xAxisIndex:1, yAxisIndex:1, data:c.volumes, itemStyle:{{color:'rgba(49,95,140,.35)'}}}},
        ]
      }});

      returnChart.setOption({{
        animation:false,
        tooltip:{{trigger:'axis', valueFormatter:v => fmtPct(v, 2)}},
        grid:{{left:48,right:14,top:28,bottom:34}},
        xAxis:{{type:'category', data:c.dates}},
        yAxis:{{axisLabel:{{formatter:v => `${{(v*100).toFixed(0)}}%`}}}},
        series:[{{name:'Return from entry', type:'line', data:c.retPath, smooth:true, areaStyle:{{color:'rgba(11,107,87,.12)'}}, lineStyle:{{color:'#0b6b57'}}, markLine:{{symbol:'none', data:markLines(c)}}}}]
      }});

      featureChart.setOption({{
        animation:false,
        tooltip:{{trigger:'axis'}},
        grid:{{left:110,right:18,top:20,bottom:24}},
        xAxis:{{type:'value'}},
        yAxis:{{type:'category', data:['買方廣度','買賣平衡','最大買方占比','量能比','5日報酬']}},
        series:[{{type:'bar', data:[c.buyerCount, c.posNegBalance, c.topBuyerShare, c.volumeRatio20, c.ret5d], itemStyle:{{color:'#b7791f'}}, label:{{show:true, position:'right', formatter:p => fmtNum(p.value, 3)}}}}]
      }});

      const metrics = [
        ['score', fmtNum(c.score, 4)],
        ['買方分點數', fmtNum(c.buyerCount, 0)],
        ['最大買方占比', fmtPct(c.topBuyerShare, 2)],
        ['買賣平衡', fmtNum(c.posNegBalance, 3)],
        ['賣方淨額(百萬)', fmtNum(c.negnetTotalM, 2)],
        ['5日報酬', fmtPct(c.ret5d, 2)],
        ['20日突破距離', fmtPct(c.breakoutGap20, 2)],
        ['20日量能比', fmtNum(c.volumeRatio20, 3)],
      ];
      document.getElementById('metrics').innerHTML = metrics.map(([k,v]) => `<div class="metric"><span>${{k}}</span><b>${{v}}</b></div>`).join('');
    }}

    ['search','bucket','month','sort'].forEach(id => document.getElementById(id).addEventListener('input', renderList));
    window.addEventListener('resize', () => {{ priceChart.resize(); returnChart.resize(); featureChart.resize(); }});
    initSummary();
    renderList();
    renderDetail();
  </script>
</body>
</html>
"""


def main() -> None:
    payload = _load_payload()
    ensure_dir(OUTPUT_HTML.parent)
    OUTPUT_HTML.write_text(_render_html(payload), encoding="utf-8")
    print(OUTPUT_HTML)


if __name__ == "__main__":
    main()
