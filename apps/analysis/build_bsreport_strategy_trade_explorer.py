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

from src.stockanalysis.config import ensure_dir


DOCS_DIR = PROJECT_ROOT / "docs"
TRADES_PATH = PROJECT_ROOT / "data" / "_derived" / "ml_runs" / "repair_branch_topratio0610_daybuy20ge4_trades.csv"
SUMMARY_PATH = PROJECT_ROOT / "data" / "_derived" / "ml_runs" / "repair_branch_topratio0610_daybuy20ge4_summary.json"
ROLLING_PATH = PROJECT_ROOT / "data" / "_derived" / "ml_runs" / "repair_branch_topratio0610_daybuy20ge4_rolling_windows.csv"
OHLC_PATH = PROJECT_ROOT / "data" / "_derived" / "ohlc.parquet"
OUTPUT_HTML = DOCS_DIR / "bsreport-strategy-trade-explorer.html"


def _load_data() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object], pd.DataFrame]:
    trades = pd.read_csv(TRADES_PATH).copy()
    ohlc = pd.read_parquet(
        OHLC_PATH,
        columns=["symbol", "date", "open", "high", "low", "close", "volume"],
    ).copy()
    rolling = pd.read_csv(ROLLING_PATH).copy()
    summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))

    trades["symbol"] = trades["symbol"].astype(str)
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        trades[col] = pd.to_datetime(trades[col])
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    ohlc = ohlc[ohlc["symbol"].str.len() == 4].sort_values(["symbol", "date"]).reset_index(drop=True)
    trades = trades.sort_values(["signal_date", "symbol"]).reset_index(drop=True)
    return trades, ohlc, summary, rolling


def _summary_metrics(trades: pd.DataFrame) -> dict[str, object]:
    wins = trades[trades["net_ret"] > 0]["net_ret"]
    losses = trades[trades["net_ret"] < 0]["net_ret"]
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float((-losses).sum()) if not losses.empty else 0.0
    avg_win = float(wins.mean()) if not wins.empty else math.nan
    avg_loss = float((-losses).mean()) if not losses.empty else math.nan
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else math.inf
    payoff_ratio = avg_win / avg_loss if math.isfinite(avg_win) and math.isfinite(avg_loss) and avg_loss > 0 else math.inf
    return {
        "trades": int(len(trades)),
        "avg_net_ret": float(trades["net_ret"].mean()),
        "median_net_ret": float(trades["net_ret"].median()),
        "win_rate": float((trades["net_ret"] > 0).mean()),
        "avg_pnl": float(trades["pnl"].mean()),
        "profit_factor": float(profit_factor),
        "payoff_ratio": float(payoff_ratio),
        "max_net_ret": float(trades["net_ret"].max()),
        "min_net_ret": float(trades["net_ret"].min()),
    }


def _window(ohlc: pd.DataFrame, trade: pd.Series, pre_bars: int = 20, post_bars: int = 10) -> dict[str, object]:
    sub = ohlc[ohlc["symbol"] == trade["symbol"]].reset_index(drop=True)
    signal_idx = sub.index[sub["date"] == trade["signal_date"]]
    exit_idx = sub.index[sub["date"] == trade["exit_date"]]
    if len(signal_idx) == 0 or len(exit_idx) == 0:
        raise ValueError(f"missing ohlc rows for {trade['symbol']} {trade['signal_date']}")

    signal_idx = int(signal_idx[0])
    exit_idx = int(exit_idx[0])
    start = max(signal_idx - pre_bars, 0)
    end = min(exit_idx + post_bars, len(sub) - 1)
    window = sub.iloc[start : end + 1].copy()

    dates = window["date"].dt.strftime("%Y-%m-%d").tolist()
    candles = []
    volumes = []
    ret_from_entry = []
    entry_px = float(trade["entry_px_raw"])
    for _, row in window.iterrows():
        candles.append([
            round(float(row["open"]), 4),
            round(float(row["close"]), 4),
            round(float(row["low"]), 4),
            round(float(row["high"]), 4),
        ])
        volumes.append(int(row["volume"]) if pd.notna(row["volume"]) else 0)
        ret_from_entry.append(round(float(row["close"]) / entry_px - 1.0, 4) if entry_px > 0 else None)

    signal_date = trade["signal_date"].strftime("%Y-%m-%d")
    entry_date = trade["entry_date"].strftime("%Y-%m-%d")
    exit_date = trade["exit_date"].strftime("%Y-%m-%d")
    return {
        "symbol": str(trade["symbol"]),
        "signalDate": signal_date,
        "entryDate": entry_date,
        "exitDate": exit_date,
        "signalPos": dates.index(signal_date),
        "entryPos": dates.index(entry_date),
        "exitPos": dates.index(exit_date),
        "dates": dates,
        "candles": candles,
        "volumes": volumes,
        "retFromEntry": ret_from_entry,
        "entryPxRaw": round(float(trade["entry_px_raw"]), 4),
        "exitPxRaw": round(float(trade["exit_px_raw"]), 4),
        "netRet": round(float(trade["net_ret"]), 4),
        "pnl": round(float(trade["pnl"]), 2),
        "mfe20": None,
        "mfe40": None,
        "mae20": None,
        "pred": round(float(trade["pred"]), 4) if pd.notna(trade["pred"]) else None,
        "label": round(float(trade["label"]), 4) if pd.notna(trade["label"]) else None,
        "exitReason": str(trade["exit_reason"]),
    }


def _payload(trades: pd.DataFrame, ohlc: pd.DataFrame, summary: dict[str, object], rolling: pd.DataFrame) -> dict[str, object]:
    entries = []
    for idx, row in trades.iterrows():
        item = _window(ohlc, row)
        item["id"] = idx
        item["labelText"] = f"{item['signalDate']} {item['symbol']} {item['netRet']:+.2%}"
        entries.append(item)

    return {
        "entries": entries,
        "summaryMetrics": _summary_metrics(trades),
        "config": summary["config"],
        "window": summary["window"],
        "summary": summary["summary"],
        "rolling": rolling.to_dict(orient="records"),
        "universe": {
            "repair_branch": [
                "stock_net_buy_days_20 >= 1",
                "warrant_hhi_posnet_20 <= 0.80",
            ],
            "refined_universe": "repair branch + top_posnet_ratio <= 0.610",
            "day_filter": "mean_buy20 >= 4",
        },
    }


def _render(payload: dict[str, object]) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BsReport Strategy Trade Explorer</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    :root {{
      --bg: #eef3f1;
      --panel: #fbfdfc;
      --ink: #17201d;
      --muted: #5c6c66;
      --line: #cfd9d5;
      --accent: #0f766e;
      --accent2: #1d4ed8;
      --gain: #047857;
      --loss: #c2410c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "PingFang TC", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.08), transparent 22%),
        radial-gradient(circle at top right, rgba(29,78,216,0.08), transparent 18%),
        linear-gradient(180deg, #f7fbf9 0%, var(--bg) 100%);
    }}
    .page {{ max-width: 1520px; margin: 0 auto; padding: 24px; }}
    .hero, .workspace {{ display: grid; gap: 18px; }}
    .hero {{ grid-template-columns: 1.2fr 0.8fr; margin-bottom: 18px; }}
    .workspace {{ grid-template-columns: 320px 1fr; }}
    .panel {{
      background: rgba(251,253,252,0.9);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 22px;
      box-shadow: 0 20px 48px rgba(23,32,29,0.08);
    }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{ font-size: clamp(2rem, 4vw, 3.4rem); line-height: 0.98; letter-spacing: -0.03em; margin-bottom: 12px; }}
    .lede {{ color: var(--muted); line-height: 1.65; }}
    .chips {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 16px; }}
    .chip {{ border: 1px solid var(--line); border-radius: 999px; padding: 8px 12px; font-size: 0.9rem; color: var(--muted); background: rgba(255,255,255,0.7); }}
    .stats {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }}
    .stat, .detail {{
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.76);
      border-radius: 16px;
      padding: 14px;
    }}
    .label {{ display: block; font-size: 0.82rem; color: var(--muted); margin-bottom: 4px; }}
    .value {{ font-size: 1.25rem; font-weight: 700; letter-spacing: -0.03em; }}
    .trade-list {{ display: flex; flex-direction: column; gap: 8px; max-height: 710px; overflow: auto; padding-right: 4px; }}
    .trade-btn {{
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px;
      background: rgba(255,255,255,0.8);
      cursor: pointer;
      text-align: left;
      transition: 160ms ease;
    }}
    .trade-btn:hover, .trade-btn.active {{
      border-color: color-mix(in srgb, var(--accent) 40%, var(--line) 60%);
      box-shadow: 0 10px 24px rgba(15,118,110,0.08);
      transform: translateY(-1px);
    }}
    .trade-btn strong {{ display: block; margin-bottom: 4px; }}
    .meta {{ color: var(--muted); font-size: 0.84rem; }}
    .toolbar {{ display: flex; justify-content: space-between; gap: 10px; flex-wrap: wrap; }}
    .toolbar-group {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .tool-btn, select {{
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255,255,255,0.85);
      padding: 10px 12px;
      font: inherit;
      color: var(--ink);
    }}
    .tool-btn {{ cursor: pointer; }}
    .details {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }}
    #chart {{ width: 100%; height: 700px; }}
    .foot {{ color: var(--muted); line-height: 1.6; white-space: pre-wrap; font-size: 0.92rem; }}
    .gain {{ color: var(--gain); }}
    .loss {{ color: var(--loss); }}
    @media (max-width: 1100px) {{
      .hero, .workspace {{ grid-template-columns: 1fr; }}
      .stats, .details {{ grid-template-columns: repeat(2, 1fr); }}
      #chart {{ height: 560px; }}
    }}
    @media (max-width: 720px) {{
      .page {{ padding: 16px; }}
      .stats, .details {{ grid-template-columns: 1fr; }}
      #chart {{ height: 480px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="panel">
        <h1>BsReport Strategy Explorer</h1>
        <p class="lede">這個頁面展示 `bsreport_strategy_discovery_log` 最終策略的全部 12 筆真實交易。你可以切換案例、看進出場 K 線、比較 rolling windows，並用滑鼠滾輪或 data zoom 自己放大縮小。</p>
        <div class="chips">
          <span class="chip">交易來源: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_trades.csv`</span>
          <span class="chip">價格來源: `data/_derived/ohlc.parquet`</span>
          <span class="chip">全數案例，不是抽樣</span>
        </div>
      </div>
      <div class="panel">
        <div class="stats" id="summary-stats"></div>
      </div>
    </section>

    <section class="workspace">
      <aside class="panel">
        <h3 style="margin-bottom: 10px;">12 筆交易</h3>
        <div class="trade-list" id="trade-list"></div>
      </aside>
      <main class="panel">
        <div class="toolbar">
          <div class="toolbar-group">
            <button class="tool-btn" id="prev-btn">上一筆</button>
            <button class="tool-btn" id="next-btn">下一筆</button>
            <select id="trade-select"></select>
          </div>
          <div class="toolbar-group">
            <button class="tool-btn" id="focus-btn">聚焦持有期</button>
            <button class="tool-btn" id="reset-btn">重設縮放</button>
          </div>
        </div>
        <div class="details" id="details" style="margin: 14px 0;"></div>
        <div id="chart"></div>
        <div class="foot" id="foot"></div>
      </main>
    </section>
  </div>

  <script>
    const payload = {data_json};
    const chart = echarts.init(document.getElementById('chart'));
    let currentIndex = 0;

    function pct(v) {{
      if (v === null || v === undefined || Number.isNaN(v)) return 'n/a';
      return `${{(v * 100).toFixed(2)}}%`;
    }}
    function num(v, digits = 4) {{
      if (v === null || v === undefined || Number.isNaN(v)) return 'n/a';
      return Number(v).toFixed(digits);
    }}

    function renderSummary() {{
      const s = payload.summaryMetrics;
      const extra = payload.summary;
      const rows = [
        ['Trades', s.trades],
        ['Avg Net', pct(s.avg_net_ret)],
        ['Median Net', pct(s.median_net_ret)],
        ['Win Rate', pct(s.win_rate)],
        ['Avg PnL', num(s.avg_pnl, 0)],
        ['Profit Factor', num(s.profit_factor, 2)],
        ['Payoff Ratio', num(s.payoff_ratio, 2)],
        ['Signals', extra.signals],
        ['Avg Picks / Signal Day', num(extra.avg_picks_signal_day, 2)],
      ];
      document.getElementById('summary-stats').innerHTML = rows.map(([label, value]) => `
        <div class="stat">
          <span class="label">${{label}}</span>
          <span class="value">${{value}}</span>
        </div>
      `).join('');
    }}

    function renderList() {{
      const list = document.getElementById('trade-list');
      const select = document.getElementById('trade-select');
      list.innerHTML = '';
      select.innerHTML = '';
      payload.entries.forEach((entry, idx) => {{
        const btn = document.createElement('button');
        btn.className = 'trade-btn' + (idx === currentIndex ? ' active' : '');
        btn.innerHTML = `<strong>${{entry.symbol}} · ${{entry.signalDate}}</strong><div class="meta">${{entry.labelText}} · ${{entry.exitReason}}</div>`;
        btn.addEventListener('click', () => {{
          currentIndex = idx;
          renderCurrent();
        }});
        list.appendChild(btn);

        const opt = document.createElement('option');
        opt.value = String(idx);
        opt.textContent = entry.labelText;
        if (idx === currentIndex) opt.selected = true;
        select.appendChild(opt);
      }});
    }}

    function renderDetails(entry) {{
      const rows = [
        ['Signal / Symbol', `${{entry.signalDate}} · ${{entry.symbol}}`],
        ['Entry / Exit', `${{entry.entryDate}} → ${{entry.exitDate}}`],
        ['Net Return', pct(entry.netRet)],
        ['PnL', num(entry.pnl, 0)],
        ['Entry / Exit Px', `${{num(entry.entryPxRaw)}} → ${{num(entry.exitPxRaw)}}`],
        ['Exit Reason', entry.exitReason],
        ['Pred', num(entry.pred)],
        ['Label', num(entry.label)],
      ];
      document.getElementById('details').innerHTML = rows.map(([label, value]) => `
        <div class="detail">
          <span class="label">${{label}}</span>
          <span class="value ${{
            label === 'Net Return' ? (entry.netRet >= 0 ? 'gain' : 'loss') : ''
          }}">${{value}}</span>
        </div>
      `).join('');
    }}

    function option(entry) {{
      return {{
        animation: false,
        backgroundColor: 'transparent',
        tooltip: {{
          trigger: 'axis',
          axisPointer: {{ type: 'cross' }},
          backgroundColor: 'rgba(23,32,29,0.92)',
          borderWidth: 0,
          textStyle: {{ color: '#fff' }},
        }},
        axisPointer: {{ link: [{{ xAxisIndex: 'all' }}] }},
        legend: {{ top: 6, textStyle: {{ color: '#5c6c66' }} }},
        grid: [
          {{ left: 58, right: 30, top: 48, height: 390 }},
          {{ left: 58, right: 30, top: 468, height: 92 }},
          {{ left: 58, right: 30, top: 580, height: 84 }},
        ],
        xAxis: [
          {{ type: 'category', data: entry.dates, boundaryGap: false, axisLine: {{ lineStyle: {{ color: '#bfd0c9' }} }}, axisLabel: {{ color: '#5c6c66' }} }},
          {{ type: 'category', gridIndex: 1, data: entry.dates, boundaryGap: false, axisLine: {{ lineStyle: {{ color: '#bfd0c9' }} }}, axisLabel: {{ show: false }}, axisTick: {{ show: false }} }},
          {{ type: 'category', gridIndex: 2, data: entry.dates, boundaryGap: false, axisLine: {{ lineStyle: {{ color: '#bfd0c9' }} }}, axisLabel: {{ color: '#5c6c66' }} }},
        ],
        yAxis: [
          {{ scale: true, axisLine: {{ show: false }}, splitLine: {{ lineStyle: {{ color: 'rgba(207,217,213,0.45)' }} }}, axisLabel: {{ color: '#5c6c66' }} }},
          {{ gridIndex: 1, scale: true, axisLine: {{ show: false }}, splitLine: {{ show: false }}, axisLabel: {{ color: '#5c6c66' }} }},
          {{ gridIndex: 2, scale: true, axisLine: {{ show: false }}, splitLine: {{ lineStyle: {{ color: 'rgba(207,217,213,0.35)' }} }}, axisLabel: {{ color: '#5c6c66', formatter: (v) => `${{(v*100).toFixed(0)}}%` }} }},
        ],
        dataZoom: [
          {{ type: 'inside', xAxisIndex: [0,1,2], startValue: Math.max(entry.signalPos - 6, 0), endValue: Math.min(entry.exitPos + 5, entry.dates.length - 1) }},
          {{ type: 'slider', xAxisIndex: [0,1,2], bottom: 6, height: 22 }},
        ],
        series: [
          {{
            name: 'K線',
            type: 'candlestick',
            data: entry.candles,
            itemStyle: {{ color: '#0f766e', color0: '#c2410c', borderColor: '#0f766e', borderColor0: '#c2410c' }},
            markPoint: {{
              symbolSize: 42,
              data: [
                {{ name: 'Signal', coord: [entry.signalPos, entry.candles[entry.signalPos][3]], value: '訊號', itemStyle: {{ color: '#1d4ed8' }} }},
                {{ name: 'Entry', coord: [entry.entryPos, entry.candles[entry.entryPos][3]], value: '進場', itemStyle: {{ color: '#0f766e' }} }},
                {{ name: 'Exit', coord: [entry.exitPos, entry.candles[entry.exitPos][3]], value: '出場', itemStyle: {{ color: '#c2410c' }} }},
              ],
            }},
            markLine: {{
              symbol: 'none',
              lineStyle: {{ type: 'dashed' }},
              label: {{ color: '#5c6c66' }},
              data: [
                {{ yAxis: entry.entryPxRaw, lineStyle: {{ color: '#0f766e' }}, label: {{ formatter: '進場價' }} }},
              ],
            }},
            markArea: {{
              itemStyle: {{ color: 'rgba(15,118,110,0.08)' }},
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
                return k[1] >= k[0] ? '#92c5bf' : '#efb08e';
              }},
            }},
          }},
          {{
            name: 'Close vs Entry',
            type: 'line',
            xAxisIndex: 2,
            yAxisIndex: 2,
            symbol: 'none',
            data: entry.retFromEntry,
            lineStyle: {{ color: '#1d4ed8', width: 2 }},
            areaStyle: {{ color: 'rgba(29,78,216,0.08)' }},
            markLine: {{ symbol: 'none', data: [{{ yAxis: 0 }}], lineStyle: {{ type: 'dotted', color: '#5c6c66' }} }},
          }},
        ],
      }};
    }}

    function renderFoot() {{
      const cfg = payload.config;
      const rolling = payload.rolling.map((r) => `${{r.window}}: ${{r.trades}} trades, win=${{pct(r.win)}}, mean=${{pct(r.mean_net)}}`).join('\\n');
      document.getElementById('foot').textContent =
        '最終策略條件\\n' +
        '- repair branch: ' + payload.universe.repair_branch.join(' ; ') + '\\n' +
        '- refined universe: ' + payload.universe.refined_universe + '\\n' +
        '- day filter: ' + payload.universe.day_filter + '\\n' +
        '- select_mode=' + cfg.select_mode + ', top_pct=' + cfg.top_pct + ', topk cap=' + cfg.topk + '\\n' +
        '- entry_mode=' + cfg.entry_mode + ', gap_th=' + cfg.gap_th + ', hold_days=' + cfg.hold_days + ', max_positions=' + cfg.max_positions + '\\n\\n' +
        'Rolling windows\\n' + rolling;
    }}

    function renderCurrent() {{
      const entry = payload.entries[currentIndex];
      document.getElementById('trade-select').value = String(currentIndex);
      renderList();
      renderDetails(entry);
      renderFoot();
      chart.setOption(option(entry), true);
    }}

    document.getElementById('trade-select').addEventListener('change', (e) => {{
      currentIndex = Number(e.target.value);
      renderCurrent();
    }});
    document.getElementById('prev-btn').addEventListener('click', () => {{
      currentIndex = (currentIndex - 1 + payload.entries.length) % payload.entries.length;
      renderCurrent();
    }});
    document.getElementById('next-btn').addEventListener('click', () => {{
      currentIndex = (currentIndex + 1) % payload.entries.length;
      renderCurrent();
    }});
    document.getElementById('focus-btn').addEventListener('click', () => {{
      const entry = payload.entries[currentIndex];
      chart.dispatchAction({{
        type: 'dataZoom',
        startValue: Math.max(entry.signalPos - 3, 0),
        endValue: Math.min(entry.exitPos + 3, entry.dates.length - 1),
      }});
    }});
    document.getElementById('reset-btn').addEventListener('click', () => {{
      const entry = payload.entries[currentIndex];
      chart.dispatchAction({{
        type: 'dataZoom',
        startValue: 0,
        endValue: entry.dates.length - 1,
      }});
    }});
    window.addEventListener('resize', () => chart.resize());

    renderSummary();
    renderCurrent();
  </script>
</body>
</html>
"""


def main() -> None:
    trades, ohlc, summary, rolling = _load_data()
    ensure_dir(DOCS_DIR)
    OUTPUT_HTML.write_text(_render(_payload(trades, ohlc, summary, rolling)), encoding="utf-8")
    print(OUTPUT_HTML)


if __name__ == "__main__":
    main()
