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


RARE_EVENT_DIR = resolve_output("analysis", "rare_event")
TRADES_PATH = RARE_EVENT_DIR / "warrant_leads_stock_refine_trades.parquet"
SIGNALS_PATH = RARE_EVENT_DIR / "warrant_leads_stock_refine_signals.parquet"
REPORT_PATH = RARE_EVENT_DIR / "warrant_leads_stock_refine_report.md"
OHLC_PATH = PROJECT_ROOT / "data" / "_derived" / "ohlc.parquet"
OUTPUT_HTML = RARE_EVENT_DIR / "warrant_leads_stock_case_explorer.html"


def _load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    trades = pd.read_parquet(TRADES_PATH).copy()
    signals = pd.read_parquet(SIGNALS_PATH).copy()
    ohlc = pd.read_parquet(
        OHLC_PATH,
        columns=["symbol", "date", "open", "high", "low", "close", "volume"],
    ).copy()

    for df in (trades, signals, ohlc):
        df["symbol"] = df["symbol"].astype(str)
        df["date"] = pd.to_datetime(df["date"])

    for col in ("signal_date", "entry_date", "exit_date"):
        trades[col] = pd.to_datetime(trades[col])

    ohlc = ohlc[ohlc["symbol"].str.len() == 4].sort_values(["symbol", "date"]).reset_index(drop=True)
    trades = trades.sort_values(["signal_date", "symbol"]).reset_index(drop=True)
    signals = signals.sort_values(["date", "symbol"]).reset_index(drop=True)
    return trades, signals, ohlc


def _safe_float(value: object, digits: int = 4) -> float | None:
    if pd.isna(value):
        return None
    return round(float(value), digits)


def _safe_int(value: object) -> int | None:
    if pd.isna(value):
        return None
    return int(value)


def _summary(trades: pd.DataFrame, signals: pd.DataFrame) -> dict[str, object]:
    wins = trades.loc[trades["net_ret"] > 0, "net_ret"]
    losses = trades.loc[trades["net_ret"] < 0, "net_ret"]
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float((-losses).sum()) if not losses.empty else 0.0
    avg_loss = float((-losses).mean()) if not losses.empty else math.nan
    avg_win = float(wins.mean()) if not wins.empty else math.nan
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else math.inf
    payoff_ratio = avg_win / avg_loss if math.isfinite(avg_win) and math.isfinite(avg_loss) and avg_loss > 0 else math.inf

    return {
        "signals": int(len(signals)),
        "completedTrades": int(len(trades)),
        "pendingSignals": int(len(signals) - len(trades)),
        "avgNetRet": float(trades["net_ret"].mean()),
        "medianNetRet": float(trades["net_ret"].median()),
        "winRate": float((trades["net_ret"] > 0).mean()),
        "profitFactor": float(profit_factor),
        "payoffRatio": float(payoff_ratio),
        "maxLoss": float(trades["net_ret"].min()),
        "stopLossTrades": int((trades["exit_reason"] == "SL").sum()),
        "timeExitTrades": int((trades["exit_reason"] == "TIME").sum()),
        "firstSignal": trades["signal_date"].min().strftime("%Y-%m-%d"),
        "lastExit": trades["exit_date"].max().strftime("%Y-%m-%d"),
    }


def _report_excerpt() -> str:
    if not REPORT_PATH.exists():
        return ""
    lines = REPORT_PATH.read_text(encoding="utf-8").splitlines()
    keep: list[str] = []
    for line in lines:
        if line.startswith("#") or line.startswith("- "):
            keep.append(line)
        if len(keep) >= 18:
            break
    return "\n".join(keep)


def _bucket_for(row: pd.Series, top_keys: set[tuple[str, str]], worst_keys: set[tuple[str, str]]) -> str:
    key = (str(row["symbol"]), row["signal_date"].strftime("%Y-%m-%d"))
    if key in top_keys:
        return "top"
    if key in worst_keys:
        return "worst"
    if row["exit_reason"] == "SL":
        return "stop"
    if float(row["net_ret"]) > 0:
        return "positive"
    return "negative"


def _extract_window(ohlc: pd.DataFrame, trade: pd.Series, pre_bars: int = 30, post_bars: int = 12) -> dict[str, object]:
    sub = ohlc.loc[ohlc["symbol"] == trade["symbol"]].reset_index(drop=True)
    signal_idx = sub.index[sub["date"] == trade["signal_date"]]
    entry_idx = sub.index[sub["date"] == trade["entry_date"]]
    exit_idx = sub.index[sub["date"] == trade["exit_date"]]
    if len(signal_idx) == 0 or len(entry_idx) == 0 or len(exit_idx) == 0:
        raise ValueError(f"missing OHLC rows for {trade['symbol']} {trade['signal_date']:%Y-%m-%d}")

    signal_idx = int(signal_idx[0])
    entry_idx = int(entry_idx[0])
    exit_idx = int(exit_idx[0])
    start = max(signal_idx - pre_bars, 0)
    end = min(exit_idx + post_bars, len(sub) - 1)
    window = sub.iloc[start : end + 1].copy()

    dates = window["date"].dt.strftime("%Y-%m-%d").tolist()
    entry_px_raw = float(trade["entry_px_raw"])
    stop_px = entry_px_raw * 0.90
    candles: list[list[float]] = []
    volumes: list[int] = []
    ret_path: list[float | None] = []

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
        ret_path.append(round(close_px / entry_px_raw - 1.0, 4) if entry_px_raw > 0 else None)

    return {
        "dates": dates,
        "candles": candles,
        "volumes": volumes,
        "retPath": ret_path,
        "signalPos": dates.index(trade["signal_date"].strftime("%Y-%m-%d")),
        "entryPos": dates.index(trade["entry_date"].strftime("%Y-%m-%d")),
        "exitPos": dates.index(trade["exit_date"].strftime("%Y-%m-%d")),
        "stopPx": round(stop_px, 4),
    }


def _trade_item(ohlc: pd.DataFrame, row: pd.Series, idx: int, bucket: str) -> dict[str, object]:
    signal_date = row["signal_date"].strftime("%Y-%m-%d")
    entry_date = row["entry_date"].strftime("%Y-%m-%d")
    exit_date = row["exit_date"].strftime("%Y-%m-%d")
    net_ret = float(row["net_ret"])
    window = _extract_window(ohlc, row)

    return {
        "id": idx,
        "bucket": bucket,
        "symbol": str(row["symbol"]),
        "signalDate": signal_date,
        "entryDate": entry_date,
        "exitDate": exit_date,
        "label": f"{signal_date} {row['symbol']} {net_ret:+.2%} {row['exit_reason']}",
        "netRet": round(net_ret, 4),
        "exitReason": str(row["exit_reason"]),
        "entryPxRaw": _safe_float(row["entry_px_raw"]),
        "entryPxNet": _safe_float(row["entry_px_net"]),
        "exitPxRaw": _safe_float(row["exit_px_raw"]),
        "exitPxNet": _safe_float(row["exit_px_net"]),
        "mfe20": _safe_float(row["mfe_20d"]),
        "mfe40": _safe_float(row["mfe_40d"]),
        "mae20": _safe_float(row["mae_20d"]),
        "priorAbsRet20d": _safe_float(row["prior_abs_ret_20d"]),
        "ret5d": _safe_float(row["ret_5d"]),
        "ret20d": _safe_float(row["ret_20d"]),
        "volumeRatio520": _safe_float(row["volume_ratio_5_20"]),
        "pricePos20": _safe_float(row["price_pos_20"]),
        "stockPosnetPct": _safe_float(row["stock_posnet_pct_cs"]),
        "stockDynKPct": _safe_float(row["stock_dyn_k_pct_cs"]),
        "stockStrongDays20": _safe_int(row["stock_posnet_strong_days_20"]),
        "warrantPosnetPct": _safe_float(row["warrant_posnet_pct_cs"]),
        "warrantDynKPct": _safe_float(row["warrant_dyn_k_pct_cs"]),
        "warrantStrongDays20": _safe_int(row["warrant_posnet_strong_days_20"]),
        "stockWarrantPosnetGap": _safe_float(row["stock_warrant_posnet_gap"]),
        "stockWarrantDynKGap": _safe_float(row["stock_warrant_dynk_gap"]),
        **window,
    }


def _build_payload(trades: pd.DataFrame, signals: pd.DataFrame, ohlc: pd.DataFrame) -> dict[str, object]:
    top_keys = {
        (str(row["symbol"]), row["signal_date"].strftime("%Y-%m-%d"))
        for _, row in trades.sort_values("net_ret", ascending=False).head(5).iterrows()
    }
    worst_keys = {
        (str(row["symbol"]), row["signal_date"].strftime("%Y-%m-%d"))
        for _, row in trades.sort_values("net_ret", ascending=True).head(5).iterrows()
    }

    entries = []
    ranked = trades.sort_values(["signal_date", "symbol"]).reset_index(drop=True)
    for idx, row in ranked.iterrows():
        bucket = _bucket_for(row, top_keys, worst_keys)
        entries.append(_trade_item(ohlc, row, idx, bucket))

    return {
        "summary": _summary(trades, signals),
        "eventName": str(trades["event_name"].iloc[0]),
        "rule": {
            "warrantPosnetPct": ">= 98%",
            "warrantDynKPct": ">= 90%",
            "warrantStrongDays20": ">= 3",
            "stockPosnetPct": "80% to 95%",
            "priorAbsRet20d": "<= 8%",
            "holdDays": "40 trading days",
            "cooldownDays": "15 trading days",
            "stopLoss": "-10%",
        },
        "reportExcerpt": _report_excerpt(),
        "entries": entries,
    }


def _render_html(payload: dict[str, object]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    template = """<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Warrant Leads Stock Case Explorer</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    :root {
      --bg: #f7f8fb;
      --surface: #ffffff;
      --surface-alt: #eef2f7;
      --ink: #111827;
      --muted: #5b6472;
      --line: #d8dee8;
      --blue: #2563eb;
      --green: #047857;
      --red: #b91c1c;
      --amber: #b45309;
      --shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans TC", sans-serif;
      background: var(--bg);
      color: var(--ink);
    }
    .page {
      max-width: 1500px;
      margin: 0 auto;
      padding: 24px;
    }
    .header {
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(360px, 0.75fr);
      gap: 16px;
      margin-bottom: 16px;
    }
    .panel {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .intro {
      padding: 22px;
    }
    h1, h2, h3, p {
      margin: 0;
    }
    h1 {
      font-size: 34px;
      line-height: 1.12;
      letter-spacing: 0;
      margin-bottom: 10px;
    }
    .lede {
      color: var(--muted);
      line-height: 1.7;
      max-width: 960px;
    }
    .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 16px;
    }
    .chip {
      border: 1px solid var(--line);
      border-radius: 999px;
      color: var(--muted);
      background: var(--surface-alt);
      padding: 7px 10px;
      font-size: 13px;
      white-space: nowrap;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 1px;
      overflow: hidden;
    }
    .stat {
      background: var(--surface);
      padding: 15px;
      min-height: 78px;
    }
    .label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
    }
    .value {
      display: block;
      font-weight: 700;
      font-size: 20px;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }
    .layout {
      display: grid;
      grid-template-columns: 360px minmax(0, 1fr);
      gap: 16px;
      align-items: start;
    }
    .sidebar {
      padding: 14px;
      position: sticky;
      top: 12px;
      max-height: calc(100vh - 24px);
      overflow: hidden;
      display: grid;
      grid-template-rows: auto auto 1fr;
      gap: 12px;
    }
    .filters {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }
    select, button {
      font: inherit;
    }
    select, .tool-btn, .trade-btn {
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      border-radius: 8px;
    }
    select, .tool-btn {
      min-height: 40px;
      padding: 8px 10px;
    }
    .tool-btn {
      cursor: pointer;
    }
    .trade-list {
      overflow: auto;
      display: flex;
      flex-direction: column;
      gap: 7px;
      padding-right: 4px;
    }
    .trade-btn {
      width: 100%;
      cursor: pointer;
      text-align: left;
      padding: 10px;
    }
    .trade-btn:hover,
    .trade-btn.active {
      border-color: var(--blue);
      box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.12);
    }
    .trade-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 4px;
    }
    .trade-meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .badge {
      border-radius: 999px;
      padding: 3px 7px;
      font-size: 12px;
      font-weight: 700;
      background: var(--surface-alt);
      color: var(--muted);
      white-space: nowrap;
    }
    .gain {
      color: var(--green);
    }
    .loss {
      color: var(--red);
    }
    .case {
      padding: 14px;
      display: grid;
      gap: 12px;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }
    .toolbar-group {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
    }
    .details {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    .detail {
      padding: 11px;
      border-right: 1px solid var(--line);
      border-bottom: 1px solid var(--line);
      min-height: 68px;
    }
    .detail:nth-child(6n) {
      border-right: 0;
    }
    .chart {
      width: 100%;
      height: 680px;
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .notes {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 12px;
    }
    .note {
      padding: 14px;
      background: var(--surface-alt);
      border: 1px solid var(--line);
      border-radius: 8px;
      line-height: 1.65;
      color: var(--muted);
      white-space: pre-wrap;
    }
    .note strong {
      color: var(--ink);
    }
    @media (max-width: 1180px) {
      .header, .layout, .notes {
        grid-template-columns: 1fr;
      }
      .sidebar {
        position: static;
        max-height: none;
      }
      .trade-list {
        max-height: 360px;
      }
      .details {
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }
      .detail:nth-child(6n) {
        border-right: 1px solid var(--line);
      }
      .detail:nth-child(3n) {
        border-right: 0;
      }
    }
    @media (max-width: 720px) {
      .page {
        padding: 12px;
      }
      h1 {
        font-size: 26px;
      }
      .stats, .details {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .detail:nth-child(3n) {
        border-right: 1px solid var(--line);
      }
      .detail:nth-child(2n) {
        border-right: 0;
      }
      .chart {
        height: 520px;
      }
      .chip {
        white-space: normal;
      }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="header">
      <div class="panel intro">
        <h1>Warrant Leads Stock Case Explorer</h1>
        <p class="lede">方向二的實際案例檢視。這頁只使用已完成出場的真實交易，左側可切換全部 52 筆、前五大獲利、前五大虧損、停損或獲利案例；右側顯示訊號日條件、進出場、停損線、持有區間與可縮放 K 線。</p>
        <div class="chips">
          <span class="chip">資料: outputs/analysis/rare_event/warrant_leads_stock_refine_trades.parquet</span>
          <span class="chip">價格: data/_derived/ohlc.parquet</span>
          <span class="chip">視窗: 訊號前 30 根到出場後 12 根</span>
        </div>
      </div>
      <div class="panel stats" id="summary-stats"></div>
    </section>

    <section class="layout">
      <aside class="panel sidebar">
        <h2 style="font-size:18px;">案例清單</h2>
        <div class="filters">
          <select id="bucket-filter" aria-label="case filter">
            <option value="all">全部完成交易</option>
            <option value="top">前五大獲利</option>
            <option value="worst">前五大虧損</option>
            <option value="stop">停損出場</option>
            <option value="positive">其他獲利</option>
            <option value="negative">其他虧損</option>
          </select>
          <select id="sort-select" aria-label="case sort">
            <option value="date">依訊號日期</option>
            <option value="returnDesc">依報酬高到低</option>
            <option value="returnAsc">依報酬低到高</option>
          </select>
        </div>
        <div class="trade-list" id="trade-list"></div>
      </aside>

      <main class="panel case">
        <div class="toolbar">
          <div class="toolbar-group">
            <button class="tool-btn" id="prev-btn">上一筆</button>
            <button class="tool-btn" id="next-btn">下一筆</button>
            <select id="trade-select" aria-label="case selector"></select>
          </div>
          <div class="toolbar-group">
            <button class="tool-btn" id="focus-btn">聚焦持有期間</button>
            <button class="tool-btn" id="reset-btn">重設縮放</button>
          </div>
        </div>
        <div class="details" id="details"></div>
        <div class="chart" id="chart"></div>
        <div class="notes">
          <div class="note" id="case-note"></div>
          <div class="note" id="rule-note"></div>
        </div>
      </main>
    </section>
  </div>

  <script>
    const payload = __PAYLOAD__;
    const entries = payload.entries;
    const chart = echarts.init(document.getElementById('chart'));
    let filteredEntries = entries.slice();
    let currentIndex = 0;

    const fmtPct = (value) => {
      if (value === null || value === undefined || Number.isNaN(value)) return 'n/a';
      return `${(value * 100).toFixed(2)}%`;
    };
    const fmtNum = (value) => {
      if (value === null || value === undefined || Number.isNaN(value)) return 'n/a';
      return Number(value).toFixed(4);
    };
    const signedClass = (value) => Number(value) >= 0 ? 'gain' : 'loss';

    function renderSummary() {
      const s = payload.summary;
      const rows = [
        ['完成交易', s.completedTrades],
        ['總訊號', s.signals],
        ['未滿持有期訊號', s.pendingSignals],
        ['平均淨報酬', fmtPct(s.avgNetRet)],
        ['勝率', fmtPct(s.winRate)],
        ['Profit Factor', fmtNum(s.profitFactor)],
        ['停損筆數', s.stopLossTrades],
        ['時間出場筆數', s.timeExitTrades],
        ['最大單筆虧損', fmtPct(s.maxLoss)],
      ];
      document.getElementById('summary-stats').innerHTML = rows.map(([label, value]) => `
        <div class="stat">
          <span class="label">${label}</span>
          <span class="value">${value}</span>
        </div>
      `).join('');
    }

    function sorted(list) {
      const mode = document.getElementById('sort-select').value;
      const copy = list.slice();
      if (mode === 'returnDesc') copy.sort((a, b) => b.netRet - a.netRet);
      if (mode === 'returnAsc') copy.sort((a, b) => a.netRet - b.netRet);
      if (mode === 'date') copy.sort((a, b) => a.signalDate.localeCompare(b.signalDate) || a.symbol.localeCompare(b.symbol));
      return copy;
    }

    function matchesFilter(entry, bucket) {
      if (bucket === 'all') return true;
      if (bucket === 'top') return entry.bucket === 'top';
      if (bucket === 'worst') return entry.bucket === 'worst';
      if (bucket === 'stop') return entry.exitReason === 'SL';
      if (bucket === 'positive') return entry.netRet > 0 && entry.bucket !== 'top';
      if (bucket === 'negative') return entry.netRet < 0 && entry.bucket !== 'worst';
      return true;
    }

    function applyFilters() {
      const bucket = document.getElementById('bucket-filter').value;
      const selected = entries.filter((entry) => matchesFilter(entry, bucket));
      filteredEntries = sorted(selected);
      currentIndex = 0;
      renderSelectors();
      renderCurrent();
    }

    function renderSelectors() {
      const list = document.getElementById('trade-list');
      const select = document.getElementById('trade-select');
      list.innerHTML = '';
      select.innerHTML = '';
      filteredEntries.forEach((entry, idx) => {
        const retClass = signedClass(entry.netRet);
        const button = document.createElement('button');
        button.className = `trade-btn${idx === currentIndex ? ' active' : ''}`;
        button.innerHTML = `
          <div class="trade-top">
            <strong>${entry.symbol} · ${entry.signalDate}</strong>
            <span class="badge ${retClass}">${fmtPct(entry.netRet)}</span>
          </div>
          <div class="trade-meta">${entry.entryDate} -> ${entry.exitDate} · ${entry.exitReason} · 權證分點 ${fmtPct(entry.warrantPosnetPct)}</div>
        `;
        button.addEventListener('click', () => {
          currentIndex = idx;
          renderSelectors();
          renderCurrent();
        });
        list.appendChild(button);

        const option = document.createElement('option');
        option.value = String(idx);
        option.textContent = entry.label;
        option.selected = idx === currentIndex;
        select.appendChild(option);
      });
    }

    function detail(label, value, extraClass = '') {
      return `
        <div class="detail">
          <span class="label">${label}</span>
          <span class="value ${extraClass}">${value}</span>
        </div>
      `;
    }

    function renderDetails(entry) {
      document.getElementById('details').innerHTML = [
        detail('股票 / 訊號日', `${entry.symbol} · ${entry.signalDate}`),
        detail('進場 / 出場', `${entry.entryDate} -> ${entry.exitDate}`),
        detail('淨報酬', fmtPct(entry.netRet), signedClass(entry.netRet)),
        detail('出場原因', entry.exitReason),
        detail('MFE 20D / 40D', `${fmtPct(entry.mfe20)} / ${fmtPct(entry.mfe40)}`),
        detail('MAE 20D', fmtPct(entry.mae20), 'loss'),
        detail('權證淨買百分位', fmtPct(entry.warrantPosnetPct)),
        detail('權證動態家數百分位', fmtPct(entry.warrantDynKPct)),
        detail('權證強勢天數 20D', entry.warrantStrongDays20),
        detail('現股淨買百分位', fmtPct(entry.stockPosnetPct)),
        detail('20D 前波振幅', fmtPct(entry.priorAbsRet20d)),
        detail('5/20 量比', fmtNum(entry.volumeRatio520)),
      ].join('');
    }

    function renderNotes(entry) {
      const winLoss = entry.netRet >= 0 ? '成功案例' : '失敗或風險案例';
      document.getElementById('case-note').innerHTML =
        `<strong>${winLoss}</strong>\n` +
        `訊號在 ${entry.signalDate} 出現，隔日以 ${fmtNum(entry.entryPxRaw)} 進場，${entry.exitDate} 以 ${fmtNum(entry.exitPxRaw)} 出場。\n` +
        `這筆的最大有利移動 MFE 40D 是 ${fmtPct(entry.mfe40)}，20D 內最大不利移動 MAE 是 ${fmtPct(entry.mae20)}。\n` +
        `判讀重點是：權證端分點集中度已經非常極端，但現股端仍在中高段，代表槓桿資金可能比現股分點更早或更集中。`;

      const rule = payload.rule;
      document.getElementById('rule-note').innerHTML =
        `<strong>方向二條件</strong>\n` +
        `權證淨買百分位 ${rule.warrantPosnetPct}，本筆 ${fmtPct(entry.warrantPosnetPct)}\n` +
        `權證動態家數百分位 ${rule.warrantDynKPct}，本筆 ${fmtPct(entry.warrantDynKPct)}\n` +
        `權證強勢天數 ${rule.warrantStrongDays20}，本筆 ${entry.warrantStrongDays20}\n` +
        `現股淨買百分位 ${rule.stockPosnetPct}，本筆 ${fmtPct(entry.stockPosnetPct)}\n` +
        `前 20 日振幅 ${rule.priorAbsRet20d}，本筆 ${fmtPct(entry.priorAbsRet20d)}\n` +
        `持有 ${rule.holdDays}，冷卻 ${rule.cooldownDays}，停損 ${rule.stopLoss}`;
    }

    function chartOption(entry) {
      const markLines = [
        { name: 'Signal', xAxis: entry.signalPos, lineStyle: { color: '#2563eb', type: 'dashed', width: 1.5 } },
        { name: 'Entry', xAxis: entry.entryPos, lineStyle: { color: '#047857', type: 'solid', width: 1.5 } },
        { name: 'Exit', xAxis: entry.exitPos, lineStyle: { color: '#b91c1c', type: 'solid', width: 1.5 } },
      ];
      return {
        animation: false,
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'cross' },
          valueFormatter: (value) => Array.isArray(value) ? value.join(', ') : value,
        },
        legend: {
          top: 8,
          data: ['K線', '成交量', '收盤相對進場報酬'],
        },
        grid: [
          { left: 64, right: 28, top: 52, height: 390 },
          { left: 64, right: 28, top: 478, height: 80 },
          { left: 64, right: 28, top: 592, height: 54 },
        ],
        xAxis: [
          { type: 'category', data: entry.dates, boundaryGap: true, axisLine: { onZero: false } },
          { type: 'category', data: entry.dates, gridIndex: 1, boundaryGap: true, axisLabel: { show: false } },
          { type: 'category', data: entry.dates, gridIndex: 2, boundaryGap: true, axisLabel: { show: false } },
        ],
        yAxis: [
          { scale: true, splitArea: { show: true }, axisLabel: { formatter: (v) => Number(v).toFixed(0) } },
          { scale: true, gridIndex: 1, splitNumber: 2, axisLabel: { show: false } },
          { scale: true, gridIndex: 2, axisLabel: { formatter: (v) => `${(v * 100).toFixed(0)}%` } },
        ],
        dataZoom: [
          { type: 'inside', xAxisIndex: [0, 1, 2], start: 0, end: 100 },
          { type: 'slider', xAxisIndex: [0, 1, 2], bottom: 8, start: 0, end: 100 },
        ],
        series: [
          {
            name: 'K線',
            type: 'candlestick',
            data: entry.candles,
            itemStyle: {
              color: '#dc2626',
              color0: '#059669',
              borderColor: '#dc2626',
              borderColor0: '#059669',
            },
            markLine: {
              symbol: 'none',
              label: { formatter: '{b}' },
              data: [
                ...markLines,
                { name: 'Entry Px', yAxis: entry.entryPxRaw, lineStyle: { color: '#047857', type: 'dotted' } },
                { name: 'Stop -10%', yAxis: entry.stopPx, lineStyle: { color: '#b91c1c', type: 'dotted' } },
              ],
            },
            markArea: {
              itemStyle: { color: 'rgba(37, 99, 235, 0.08)' },
              data: [[{ xAxis: entry.entryPos, name: '持有期間' }, { xAxis: entry.exitPos }]],
            },
          },
          {
            name: '成交量',
            type: 'bar',
            xAxisIndex: 1,
            yAxisIndex: 1,
            data: entry.volumes,
            itemStyle: { color: '#94a3b8' },
          },
          {
            name: '收盤相對進場報酬',
            type: 'line',
            xAxisIndex: 2,
            yAxisIndex: 2,
            data: entry.retPath,
            showSymbol: false,
            lineStyle: { width: 2, color: '#f59e0b' },
            markLine: {
              symbol: 'none',
              label: { show: false },
              data: [{ yAxis: 0, lineStyle: { color: '#64748b', type: 'dashed' } }],
            },
          },
        ],
      };
    }

    function renderCurrent() {
      if (filteredEntries.length === 0) {
        document.getElementById('details').innerHTML = '';
        document.getElementById('case-note').textContent = '沒有符合篩選條件的案例。';
        document.getElementById('rule-note').textContent = '';
        chart.clear();
        return;
      }
      const entry = filteredEntries[currentIndex];
      document.getElementById('trade-select').value = String(currentIndex);
      renderDetails(entry);
      renderNotes(entry);
      chart.setOption(chartOption(entry), true);
    }

    document.getElementById('bucket-filter').addEventListener('change', applyFilters);
    document.getElementById('sort-select').addEventListener('change', applyFilters);
    document.getElementById('trade-select').addEventListener('change', (event) => {
      currentIndex = Number(event.target.value);
      renderSelectors();
      renderCurrent();
    });
    document.getElementById('prev-btn').addEventListener('click', () => {
      currentIndex = (currentIndex - 1 + filteredEntries.length) % filteredEntries.length;
      renderSelectors();
      renderCurrent();
    });
    document.getElementById('next-btn').addEventListener('click', () => {
      currentIndex = (currentIndex + 1) % filteredEntries.length;
      renderSelectors();
      renderCurrent();
    });
    document.getElementById('reset-btn').addEventListener('click', () => {
      chart.dispatchAction({ type: 'dataZoom', start: 0, end: 100 });
    });
    document.getElementById('focus-btn').addEventListener('click', () => {
      const entry = filteredEntries[currentIndex];
      const total = Math.max(entry.dates.length - 1, 1);
      const start = Math.max(0, (entry.entryPos - 3) / total * 100);
      const end = Math.min(100, (entry.exitPos + 3) / total * 100);
      chart.dispatchAction({ type: 'dataZoom', start, end });
    });
    window.addEventListener('resize', () => chart.resize());

    renderSummary();
    applyFilters();
  </script>
</body>
</html>
"""
    return template.replace("__PAYLOAD__", payload_json)


def main() -> None:
    trades, signals, ohlc = _load_data()
    payload = _build_payload(trades, signals, ohlc)
    ensure_dir(OUTPUT_HTML.parent)
    OUTPUT_HTML.write_text(_render_html(payload), encoding="utf-8")
    print(f"Wrote {OUTPUT_HTML.relative_to(PROJECT_ROOT)}")
    print(f"Embedded {len(payload['entries'])} completed trades")


if __name__ == "__main__":
    main()
