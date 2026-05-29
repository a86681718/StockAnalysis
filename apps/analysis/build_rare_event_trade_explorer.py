#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stockanalysis.config import ensure_dir, resolve_output


DOCS_DIR = PROJECT_ROOT / "docs"
RARE_EVENT_DIR = resolve_output("analysis", "rare_event")
TRADES_PATH = RARE_EVENT_DIR / "trades.parquet"
REPORT_PATH = RARE_EVENT_DIR / "best_rare_event_report.md"
OHLC_PATH = PROJECT_ROOT / "data" / "_derived" / "ohlc.parquet"
OUTPUT_HTML = DOCS_DIR / "rare-event-trade-explorer.html"


def _load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    trades = pd.read_parquet(TRADES_PATH).copy()
    ohlc = pd.read_parquet(
        OHLC_PATH,
        columns=["symbol", "date", "open", "high", "low", "close", "volume"],
    ).copy()

    for df in (trades, ohlc):
        df["symbol"] = df["symbol"].astype(str)
        df["date"] = pd.to_datetime(df["date"])

    trades["signal_date"] = pd.to_datetime(trades["signal_date"])
    trades["entry_date"] = pd.to_datetime(trades["entry_date"])
    trades["exit_date"] = pd.to_datetime(trades["exit_date"])
    ohlc = ohlc[ohlc["symbol"].str.len() == 4].sort_values(["symbol", "date"]).reset_index(drop=True)
    trades = trades.sort_values(["signal_date", "symbol"]).reset_index(drop=True)
    return trades, ohlc


def _summary(trades: pd.DataFrame) -> dict[str, object]:
    wins = trades[trades["net_ret"] > 0]["net_ret"]
    losses = trades[trades["net_ret"] < 0]["net_ret"]
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float((-losses).sum()) if not losses.empty else 0.0
    avg_loss = float((-losses).mean()) if not losses.empty else math.nan
    avg_win = float(wins.mean()) if not wins.empty else math.nan
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else math.inf
    payoff_ratio = avg_win / avg_loss if math.isfinite(avg_win) and math.isfinite(avg_loss) and avg_loss > 0 else math.inf
    return {
        "trades": int(len(trades)),
        "avg_net_ret": float(trades["net_ret"].mean()),
        "median_net_ret": float(trades["net_ret"].median()),
        "win_rate": float((trades["net_ret"] > 0).mean()),
        "avg_mfe_20d": float(trades["mfe_20d"].mean()),
        "avg_mfe_40d": float(trades["mfe_40d"].mean()),
        "avg_mae_20d": float(trades["mae_20d"].mean()),
        "profit_factor": float(profit_factor),
        "payoff_ratio": float(payoff_ratio),
        "max_loss": float(trades["net_ret"].min()),
    }


def _extract_window(ohlc: pd.DataFrame, trade: pd.Series, pre_bars: int = 25, post_bars: int = 15) -> dict[str, object]:
    sub = ohlc[ohlc["symbol"] == trade["symbol"]].reset_index(drop=True)
    signal_idx = sub.index[sub["date"] == trade["signal_date"]]
    exit_idx = sub.index[sub["date"] == trade["exit_date"]]
    if len(signal_idx) == 0 or len(exit_idx) == 0:
        raise ValueError(f"missing price rows for {trade['symbol']} {trade['signal_date']}")

    signal_idx = int(signal_idx[0])
    exit_idx = int(exit_idx[0])
    start = max(signal_idx - pre_bars, 0)
    end = min(exit_idx + post_bars, len(sub) - 1)
    window = sub.iloc[start : end + 1].copy()

    dates = window["date"].dt.strftime("%Y-%m-%d").tolist()
    candles = []
    volumes = []
    close_path = []
    entry_px_net = float(trade["entry_px_net"])
    for _, row in window.iterrows():
        candles.append(
            [
                round(float(row["open"]), 4),
                round(float(row["close"]), 4),
                round(float(row["low"]), 4),
                round(float(row["high"]), 4),
            ]
        )
        volumes.append(int(row["volume"]) if pd.notna(row["volume"]) else 0)
        close_px = float(row["close"])
        close_path.append(round(close_px / entry_px_net - 1.0, 4) if entry_px_net > 0 else None)

    signal_date = trade["signal_date"].strftime("%Y-%m-%d")
    entry_date = trade["entry_date"].strftime("%Y-%m-%d")
    exit_date = trade["exit_date"].strftime("%Y-%m-%d")
    signal_pos = dates.index(signal_date)
    entry_pos = dates.index(entry_date)
    exit_pos = dates.index(exit_date)

    stop_loss = round(float(trade["entry_px_raw"]) * 0.90, 4)
    return {
        "symbol": str(trade["symbol"]),
        "signalDate": signal_date,
        "entryDate": entry_date,
        "exitDate": exit_date,
        "dates": dates,
        "candles": candles,
        "volumes": volumes,
        "closePath": close_path,
        "signalPos": signal_pos,
        "entryPos": entry_pos,
        "exitPos": exit_pos,
        "entryPxRaw": round(float(trade["entry_px_raw"]), 4),
        "entryPxNet": round(float(trade["entry_px_net"]), 4),
        "exitPxRaw": round(float(trade["exit_px_raw"]), 4),
        "exitPxNet": round(float(trade["exit_px_net"]), 4),
        "stopPxApprox": stop_loss,
        "netRet": round(float(trade["net_ret"]), 4),
        "mfe20": round(float(trade["mfe_20d"]), 4),
        "mfe40": round(float(trade["mfe_40d"]), 4),
        "mae20": round(float(trade["mae_20d"]), 4),
        "exitReason": str(trade["exit_reason"]),
        "stockPosnetPct": round(float(trade["stock_posnet_pct_cs"]), 4) if pd.notna(trade["stock_posnet_pct_cs"]) else None,
        "stockPosnetStrongDays20": int(trade["stock_posnet_strong_days_20"]) if pd.notna(trade["stock_posnet_strong_days_20"]) else None,
        "warrantPosnetPct": round(float(trade["warrant_posnet_pct_cs"]), 4) if pd.notna(trade["warrant_posnet_pct_cs"]) else None,
        "warrantPosnetStrongDays20": int(trade["warrant_posnet_strong_days_20"]) if pd.notna(trade["warrant_posnet_strong_days_20"]) else None,
        "priorAbsRet20d": round(float(trade["prior_abs_ret_20d"]), 4) if pd.notna(trade["prior_abs_ret_20d"]) else None,
        "eventName": str(trade["event_name"]),
    }


def _build_payload(trades: pd.DataFrame, ohlc: pd.DataFrame) -> dict[str, object]:
    top = trades.sort_values("net_ret", ascending=False).head(5)
    worst = trades.sort_values("net_ret", ascending=True).head(5)
    sample = pd.concat([top, worst, trades.head(5)]).drop_duplicates(subset=["signal_date", "symbol"]).reset_index(drop=True)

    entries = []
    for idx, row in sample.iterrows():
        item = _extract_window(ohlc, row)
        item["id"] = idx
        item["label"] = f"{item['signalDate']} {item['symbol']} {item['netRet']:+.2%}"
        item["bucket"] = "best" if idx < len(top) else "worst" if idx < len(top) + len(worst) else "mixed"
        entries.append(item)

    return {
        "summary": _summary(trades),
        "eventName": str(trades["event_name"].iloc[0]),
        "entries": entries,
    }


def _render_html(payload: dict[str, object], report_text: str) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    report_excerpt = json.dumps(report_text, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Rare Event Trade Explorer</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: #fffaf2;
      --ink: #1c1814;
      --muted: #6d6358;
      --line: #d8ccbc;
      --accent: #0f766e;
      --accent-2: #a16207;
      --danger: #b91c1c;
      --gain: #0f766e;
      --loss: #c2410c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.08), transparent 25%),
        radial-gradient(circle at top right, rgba(161,98,7,0.12), transparent 20%),
        linear-gradient(180deg, #f8f3ea 0%, var(--bg) 100%);
    }}
    .page {{
      max-width: 1480px;
      margin: 0 auto;
      padding: 28px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    .panel {{
      background: color-mix(in srgb, var(--panel) 90%, white 10%);
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: 0 18px 50px rgba(38, 28, 18, 0.08);
      padding: 22px;
    }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{
      font-size: clamp(2rem, 4vw, 3.6rem);
      line-height: 0.98;
      letter-spacing: -0.03em;
      margin-bottom: 12px;
    }}
    .lede {{
      color: var(--muted);
      line-height: 1.6;
      font-size: 1rem;
    }}
    .chips {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }}
    .chip {{
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--muted);
      background: rgba(255,255,255,0.66);
      font-size: 0.92rem;
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }}
    .stat {{
      padding: 14px;
      background: rgba(255,255,255,0.78);
      border-radius: 16px;
      border: 1px solid var(--line);
    }}
    .stat .label {{
      display: block;
      color: var(--muted);
      font-size: 0.86rem;
      margin-bottom: 6px;
    }}
    .stat .value {{
      font-size: 1.48rem;
      font-weight: 700;
      letter-spacing: -0.03em;
    }}
    .workspace {{
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 18px;
    }}
    .sidebar {{
      display: flex;
      flex-direction: column;
      gap: 14px;
    }}
    .trade-list {{
      display: flex;
      flex-direction: column;
      gap: 8px;
      max-height: 620px;
      overflow: auto;
      padding-right: 6px;
    }}
    .trade-btn {{
      width: 100%;
      text-align: left;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.75);
      color: var(--ink);
      padding: 12px 12px;
      cursor: pointer;
      transition: 160ms ease;
    }}
    .trade-btn:hover, .trade-btn.active {{
      transform: translateY(-1px);
      border-color: color-mix(in srgb, var(--accent) 40%, var(--line) 60%);
      background: #fff;
      box-shadow: 0 10px 24px rgba(15, 118, 110, 0.08);
    }}
    .trade-btn strong {{
      display: block;
      margin-bottom: 4px;
    }}
    .trade-btn .meta {{
      font-size: 0.84rem;
      color: var(--muted);
    }}
    .chart-panel {{
      display: grid;
      grid-template-rows: auto auto 1fr auto;
      gap: 14px;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }}
    .toolbar-group {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
    }}
    select, button {{
      font: inherit;
    }}
    select, .tool-btn {{
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.8);
      padding: 10px 12px;
      color: var(--ink);
    }}
    .tool-btn {{
      cursor: pointer;
    }}
    .details {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }}
    .detail {{
      padding: 12px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.72);
    }}
    .detail .label {{
      color: var(--muted);
      font-size: 0.82rem;
      display: block;
      margin-bottom: 4px;
    }}
    .detail .value {{
      font-size: 1.05rem;
      font-weight: 700;
    }}
    .chart-wrap {{
      min-height: 680px;
    }}
    #chart {{
      width: 100%;
      height: 680px;
    }}
    .footnote {{
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.55;
      white-space: pre-wrap;
    }}
    .gain {{ color: var(--gain); }}
    .loss {{ color: var(--loss); }}
    @media (max-width: 1100px) {{
      .hero, .workspace {{ grid-template-columns: 1fr; }}
      .details, .stats {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      #chart {{ height: 560px; }}
    }}
    @media (max-width: 700px) {{
      .page {{ padding: 16px; }}
      .details, .stats {{ grid-template-columns: 1fr; }}
      #chart {{ height: 480px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="panel">
        <h1>Rare Event Trade Explorer</h1>
        <p class="lede">這個頁面只用目前最佳策略的真實交易。左邊選案例，右邊看 K 線、成交量、進出場、停損與實際回測指標。圖表可滑鼠拖曳縮放、滾輪 zoom，或用下方 data zoom slider 精細檢查。</p>
        <div class="chips">
          <span class="chip" id="event-name-chip"></span>
          <span class="chip">資料來源: `outputs/analysis/rare_event/trades.parquet` + `data/_derived/ohlc.parquet`</span>
          <span class="chip">圖上區間: 訊號前 25 根 + 出場後 15 根</span>
        </div>
      </div>
      <div class="panel">
        <div class="stats" id="summary-stats"></div>
      </div>
    </section>

    <section class="workspace">
      <aside class="sidebar">
        <div class="panel">
          <h3 style="margin-bottom:10px;">案例選擇</h3>
          <div class="toolbar-group" style="margin-bottom:10px;">
            <select id="bucket-filter">
              <option value="all">全部代表案例</option>
              <option value="best">只看高報酬例子</option>
              <option value="worst">只看失敗例子</option>
              <option value="mixed">只看混合對照</option>
            </select>
          </div>
          <div class="trade-list" id="trade-list"></div>
        </div>
      </aside>

      <main class="chart-panel panel">
        <div class="toolbar">
          <div class="toolbar-group">
            <button class="tool-btn" id="prev-btn">上一個</button>
            <button class="tool-btn" id="next-btn">下一個</button>
            <select id="trade-select"></select>
          </div>
          <div class="toolbar-group">
            <button class="tool-btn" id="focus-trade-btn">聚焦進出場</button>
            <button class="tool-btn" id="reset-zoom-btn">重設縮放</button>
          </div>
        </div>

        <div class="details" id="trade-details"></div>
        <div class="chart-wrap"><div id="chart"></div></div>
        <div class="footnote" id="report-excerpt"></div>
      </main>
    </section>
  </div>

  <script>
    const payload = {payload_json};
    const reportExcerpt = {report_excerpt};
    const entries = payload.entries;
    let filteredEntries = entries.slice();
    let currentIndex = 0;
    const chart = echarts.init(document.getElementById('chart'));

    const formatPct = (v) => {{
      if (v === null || v === undefined || Number.isNaN(v)) return 'n/a';
      return `${{(v * 100).toFixed(2)}}%`;
    }};
    const formatNum = (v) => {{
      if (v === null || v === undefined || Number.isNaN(v)) return 'n/a';
      return Number(v).toFixed(4);
    }};

    function renderSummary() {{
      document.getElementById('event-name-chip').textContent = payload.eventName;
      const s = payload.summary;
      const stats = [
        ['Trades', s.trades],
        ['Avg Net', formatPct(s.avg_net_ret)],
        ['Median Net', formatPct(s.median_net_ret)],
        ['Win Rate', formatPct(s.win_rate)],
        ['Avg MFE 20D', formatPct(s.avg_mfe_20d)],
        ['Avg MFE 40D', formatPct(s.avg_mfe_40d)],
        ['Profit Factor', formatNum(s.profit_factor)],
        ['Payoff Ratio', formatNum(s.payoff_ratio)],
        ['Max Loss', formatPct(s.max_loss)],
      ];
      const html = stats.map(([label, value]) => `
        <div class="stat">
          <span class="label">${{label}}</span>
          <span class="value">${{value}}</span>
        </div>
      `).join('');
      document.getElementById('summary-stats').innerHTML = html;
      document.getElementById('report-excerpt').textContent =
        '策略摘要\\n' +
        '- 這些都是目前最佳策略的真實交易\\n' +
        '- 綠線是進場價，紅線是約略停損線，陰影區是持有期間\\n' +
        '- 下方 data zoom 可以自己 zoom-in / zoom-out\\n\\n' +
        reportExcerpt;
    }}

    function applyFilter() {{
      const bucket = document.getElementById('bucket-filter').value;
      filteredEntries = bucket === 'all' ? entries.slice() : entries.filter((x) => x.bucket === bucket);
      currentIndex = 0;
      renderTradeSelectors();
      renderCurrentTrade();
    }}

    function renderTradeSelectors() {{
      const listEl = document.getElementById('trade-list');
      const selectEl = document.getElementById('trade-select');
      listEl.innerHTML = '';
      selectEl.innerHTML = '';
      filteredEntries.forEach((entry, idx) => {{
        const btn = document.createElement('button');
        btn.className = 'trade-btn' + (idx === currentIndex ? ' active' : '');
        btn.innerHTML = `<strong>${{entry.symbol}} · ${{entry.signalDate}}</strong><div class="meta">${{entry.label}} · ${{entry.exitReason}}</div>`;
        btn.addEventListener('click', () => {{
          currentIndex = idx;
          renderTradeSelectors();
          renderCurrentTrade();
        }});
        listEl.appendChild(btn);

        const opt = document.createElement('option');
        opt.value = String(idx);
        opt.textContent = entry.label;
        if (idx === currentIndex) opt.selected = true;
        selectEl.appendChild(opt);
      }});
    }}

    function renderDetails(entry) {{
      const detailRows = [
        ['Signal', `${{entry.signalDate}} · ${{entry.symbol}}`],
        ['Entry / Exit', `${{entry.entryDate}} → ${{entry.exitDate}}`],
        ['Net Return', formatPct(entry.netRet)],
        ['Exit Reason', entry.exitReason],
        ['MFE 20D', formatPct(entry.mfe20)],
        ['MFE 40D', formatPct(entry.mfe40)],
        ['MAE 20D', formatPct(entry.mae20)],
        ['Entry / Exit Px', `${{formatNum(entry.entryPxRaw)}} → ${{formatNum(entry.exitPxRaw)}}`],
        ['Stock Posnet %', formatPct(entry.stockPosnetPct)],
        ['Stock Strong Days 20', entry.stockPosnetStrongDays20],
        ['Warrant Posnet %', formatPct(entry.warrantPosnetPct)],
        ['Warrant Strong Days 20', entry.warrantPosnetStrongDays20],
      ];
      document.getElementById('trade-details').innerHTML = detailRows.map(([label, value]) => `
        <div class="detail">
          <span class="label">${{label}}</span>
          <span class="value ${{
            label === 'Net Return' ? (entry.netRet >= 0 ? 'gain' : 'loss') : ''
          }}">${{value}}</span>
        </div>
      `).join('');
    }}

    function tradeOption(entry) {{
      return {{
        animation: false,
        backgroundColor: 'transparent',
        legend: {{ top: 6, textStyle: {{ color: '#6d6358' }} }},
        tooltip: {{
          trigger: 'axis',
          axisPointer: {{ type: 'cross' }},
          backgroundColor: 'rgba(28,24,20,0.92)',
          borderWidth: 0,
          textStyle: {{ color: '#fff' }},
        }},
        axisPointer: {{ link: [{{ xAxisIndex: 'all' }}] }},
        grid: [
          {{ left: 58, right: 32, top: 50, height: 380 }},
          {{ left: 58, right: 32, top: 460, height: 90 }},
          {{ left: 58, right: 32, top: 572, height: 82 }},
        ],
        xAxis: [
          {{
            type: 'category',
            data: entry.dates,
            boundaryGap: false,
            axisLine: {{ lineStyle: {{ color: '#bfae99' }} }},
            axisLabel: {{ color: '#6d6358' }},
            min: 'dataMin',
            max: 'dataMax',
          }},
          {{
            type: 'category',
            gridIndex: 1,
            data: entry.dates,
            boundaryGap: false,
            axisLine: {{ lineStyle: {{ color: '#bfae99' }} }},
            axisLabel: {{ show: false }},
            axisTick: {{ show: false }},
          }},
          {{
            type: 'category',
            gridIndex: 2,
            data: entry.dates,
            boundaryGap: false,
            axisLine: {{ lineStyle: {{ color: '#bfae99' }} }},
            axisLabel: {{ color: '#6d6358' }},
          }},
        ],
        yAxis: [
          {{
            scale: true,
            axisLine: {{ show: false }},
            splitLine: {{ lineStyle: {{ color: 'rgba(191,174,153,0.35)' }} }},
            axisLabel: {{ color: '#6d6358' }},
          }},
          {{
            gridIndex: 1,
            scale: true,
            axisLine: {{ show: false }},
            splitLine: {{ show: false }},
            axisLabel: {{ color: '#6d6358' }},
          }},
          {{
            gridIndex: 2,
            scale: true,
            axisLine: {{ show: false }},
            splitLine: {{ lineStyle: {{ color: 'rgba(191,174,153,0.25)' }} }},
            axisLabel: {{
              color: '#6d6358',
              formatter: (v) => `${{(v * 100).toFixed(0)}}%`,
            }},
          }},
        ],
        dataZoom: [
          {{ type: 'inside', xAxisIndex: [0, 1, 2], startValue: Math.max(entry.signalPos - 8, 0), endValue: Math.min(entry.exitPos + 8, entry.dates.length - 1) }},
          {{ type: 'slider', xAxisIndex: [0, 1, 2], bottom: 6, height: 22 }},
        ],
        series: [
          {{
            name: 'K線',
            type: 'candlestick',
            data: entry.candles,
            itemStyle: {{
              color: '#0f766e',
              color0: '#c2410c',
              borderColor: '#0f766e',
              borderColor0: '#c2410c',
            }},
            markPoint: {{
              symbolSize: 42,
              data: [
                {{ name: 'Signal', coord: [entry.signalPos, entry.candles[entry.signalPos][3]], value: '訊號', itemStyle: {{ color: '#a16207' }} }},
                {{ name: 'Entry', coord: [entry.entryPos, entry.candles[entry.entryPos][3]], value: '進場', itemStyle: {{ color: '#0f766e' }} }},
                {{ name: 'Exit', coord: [entry.exitPos, entry.candles[entry.exitPos][3]], value: '出場', itemStyle: {{ color: '#b91c1c' }} }},
              ],
            }},
            markLine: {{
              symbol: 'none',
              label: {{ color: '#6d6358' }},
              lineStyle: {{ type: 'dashed' }},
              data: [
                {{ yAxis: entry.entryPxRaw, lineStyle: {{ color: '#0f766e' }}, label: {{ formatter: '進場價' }} }},
                {{ yAxis: entry.stopPxApprox, lineStyle: {{ color: '#b91c1c' }}, label: {{ formatter: '約略停損價' }} }},
              ],
            }},
            markArea: {{
              itemStyle: {{ color: 'rgba(15, 118, 110, 0.08)' }},
              data: [[{{ xAxis: entry.entryPos }}, {{ xAxis: entry.exitPos }}]],
            }},
          }},
          {{
            name: 'Volume',
            type: 'bar',
            xAxisIndex: 1,
            yAxisIndex: 1,
            data: entry.volumes,
            itemStyle: {{
              color: (params) => {{
                const k = entry.candles[params.dataIndex];
                return k[1] >= k[0] ? '#7fb8b2' : '#e2a17b';
              }},
            }},
          }},
          {{
            name: 'Close vs Entry',
            type: 'line',
            xAxisIndex: 2,
            yAxisIndex: 2,
            smooth: false,
            symbol: 'none',
            data: entry.closePath,
            lineStyle: {{ color: '#7c3aed', width: 2 }},
            areaStyle: {{ color: 'rgba(124,58,237,0.08)' }},
            markLine: {{
              symbol: 'none',
              lineStyle: {{ color: '#6d6358', type: 'dotted' }},
              data: [{{ yAxis: 0 }}],
            }},
          }},
        ],
      }};
    }}

    function renderCurrentTrade() {{
      if (!filteredEntries.length) return;
      const entry = filteredEntries[currentIndex];
      document.getElementById('trade-select').value = String(currentIndex);
      renderDetails(entry);
      chart.setOption(tradeOption(entry), true);
      renderTradeSelectors();
    }}

    document.getElementById('trade-select').addEventListener('change', (e) => {{
      currentIndex = Number(e.target.value);
      renderCurrentTrade();
    }});
    document.getElementById('bucket-filter').addEventListener('change', applyFilter);
    document.getElementById('prev-btn').addEventListener('click', () => {{
      currentIndex = (currentIndex - 1 + filteredEntries.length) % filteredEntries.length;
      renderCurrentTrade();
    }});
    document.getElementById('next-btn').addEventListener('click', () => {{
      currentIndex = (currentIndex + 1) % filteredEntries.length;
      renderCurrentTrade();
    }});
    document.getElementById('focus-trade-btn').addEventListener('click', () => {{
      const entry = filteredEntries[currentIndex];
      chart.dispatchAction({{
        type: 'dataZoom',
        startValue: Math.max(entry.signalPos - 4, 0),
        endValue: Math.min(entry.exitPos + 4, entry.dates.length - 1),
      }});
    }});
    document.getElementById('reset-zoom-btn').addEventListener('click', () => {{
      const entry = filteredEntries[currentIndex];
      chart.dispatchAction({{
        type: 'dataZoom',
        startValue: 0,
        endValue: entry.dates.length - 1,
      }});
    }});
    window.addEventListener('resize', () => chart.resize());

    renderSummary();
    applyFilter();
  </script>
</body>
</html>
"""


def main() -> None:
    trades, ohlc = _load_data()
    payload = _build_payload(trades, ohlc)
    report_text = REPORT_PATH.read_text(encoding="utf-8")
    html = _render_html(payload, report_text)
    ensure_dir(DOCS_DIR)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(OUTPUT_HTML)


if __name__ == "__main__":
    main()
