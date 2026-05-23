from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from dash import Dash, dcc, html, Input, Output, State, dash_table, no_update, callback_context
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# -----------------------------
# Symbol name mapping (latest OHLC csv)
# -----------------------------
_SYMBOL_NAME_MAP: dict[str, str] | None = None


def load_symbol_name_map() -> dict[str, str]:
    global _SYMBOL_NAME_MAP
    if _SYMBOL_NAME_MAP is not None:
        return _SYMBOL_NAME_MAP

    ohlc_dir = Path("data/ohlc")
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


# -----------------------------
# Real data loader
# -----------------------------
BAR_WIDTH_DAYS = 0.7
BAR_WIDTH_MS = int(24 * 60 * 60 * 1000 * BAR_WIDTH_DAYS)
def load_data(stock_id: str, days: int = 240) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    """
    Returns:
      ohlcv_df: columns [date, open, high, low, close, volume]
      broker_df: columns [date, broker, buy, sell, net]
      brokers: list of broker names
    """
    stock_id = str(stock_id).strip()
    ohlc_path = Path("data/_derived/ohlc.parquet")
    if not ohlc_path.exists():
        raise FileNotFoundError("Missing data/_derived/ohlc.parquet. Please run Analysis_BsReport_v3.py first.")

    ohlcv = pd.read_parquet(ohlc_path, columns=["symbol", "date", "open", "high", "low", "close", "volume"])
    ohlcv = ohlcv[ohlcv["symbol"] == stock_id].copy()
    if ohlcv.empty:
        return pd.DataFrame(), pd.DataFrame(), [], []

    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    for c in ["open", "high", "low", "close", "volume"]:
        ohlcv[c] = pd.to_numeric(ohlcv[c], errors="coerce")
    ohlcv["volume"] = ohlcv["volume"].fillna(0)
    ohlcv = ohlcv.sort_values("date")
    if days and days > 0:
        ohlcv = ohlcv.tail(days)

    # Broker data
    broker_path = None
    for p in [Path(f"data/bs_report/parquet_twse/{stock_id}.parquet"), Path(f"data/bs_report/parquet_tpex/{stock_id}.parquet")]:
        if p.exists():
            broker_path = p
            break

    if broker_path is None:
        return ohlcv, pd.DataFrame(), [], []

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

    # Broker name mapping
    broker_list_path = Path("data/broker_list.csv")
    if broker_list_path.exists():
        broker_map = pd.read_csv(broker_list_path, dtype=str)[["證券商代號", "證券商名稱"]].dropna()
        broker_map = dict(zip(broker_map["證券商代號"].str.strip(), broker_map["證券商名稱"].str.strip()))
        broker_df["broker"] = broker_df["broker"].astype(str).str.strip().map(broker_map).fillna(broker_df["broker"])

    broker_df = (
        broker_df.groupby(["date", "broker"], as_index=False)[["buy", "sell", "buy_amt", "sell_amt"]]
        .sum()
    )
    broker_df["net"] = broker_df["buy"] - broker_df["sell"]

    brokers = sorted(broker_df["broker"].dropna().unique().tolist())

    # Events for this symbol
    events_path = Path("data/_derived/scored.parquet")
    if events_path.exists():
        ev = pd.read_parquet(events_path, columns=["symbol", "date", "is_event"])
        ev = ev[(ev["symbol"] == stock_id) & (ev["is_event"] == 1)].copy()
        event_dates = pd.to_datetime(ev["date"]).dt.date.astype(str).unique().tolist()
    else:
        event_dates = []

    return ohlcv, broker_df, brokers, event_dates


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
    if start is None or end is None:
        return df
    return df[(df[date_col] >= start) & (df[date_col] <= end)]


def build_figure(
    ohlcv: pd.DataFrame,
    broker_df: pd.DataFrame,
    selected_broker: str,
    topn_buy_daily: pd.DataFrame,
    topn_sell_daily: pd.DataFrame,
    topn_label: str,
    event_dates: list[pd.Timestamp],
) -> go.Figure:
    bb_window = 20
    bb_mid = ohlcv["close"].rolling(window=bb_window, min_periods=bb_window).mean()
    bb_std = ohlcv["close"].rolling(window=bb_window, min_periods=bb_window).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bar_width_ms = BAR_WIDTH_MS

    fig = make_subplots(
        rows=5,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.46, 0.14, 0.18, 0.11, 0.11],
        specs=[
            [{"type": "candlestick"}],
            [{"type": "bar"}],
            [{"type": "bar"}],
            [{"type": "bar"}],
            [{"type": "bar"}],
        ],
    )

    # Row 1: Candlestick
    pct_change = ohlcv["close"].pct_change()
    pct_display = pct_change.map(lambda x: "" if pd.isna(x) else f"{x:+.2%}").to_numpy()
    fig.add_trace(
        go.Candlestick(
            x=ohlcv["date"],
            open=ohlcv["open"],
            high=ohlcv["high"],
            low=ohlcv["low"],
            close=ohlcv["close"],
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
            x=ohlcv["date"],
            y=bb_upper,
            mode="lines",
            name="BB Upper",
            line=dict(color="#7a7a7a", width=1),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=ohlcv["date"],
            y=bb_mid,
            mode="lines",
            name="BB Mid",
            line=dict(color="#aaaaaa", width=1, dash="dot"),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=ohlcv["date"],
            y=bb_lower,
            mode="lines",
            name="BB Lower",
            line=dict(color="#7a7a7a", width=1),
        ),
        row=1,
        col=1,
    )

    if event_dates is not None and len(event_dates) > 0:
        ev = ohlcv[ohlcv["date"].isin(event_dates)].copy()
        if not ev.empty:
            fig.add_trace(
                go.Scatter(
                    x=ev["date"],
                    y=ev["close"],
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
            x=ohlcv["date"],
            y=ohlcv["volume"],
            name="成交量",
            marker_color="#218BE1",
            width=bar_width_ms,
        ),
        row=2,
        col=1,
    )

    # Row 3: Selected broker buy/sell
    bd = broker_df[broker_df["broker"] == selected_broker].sort_values("date")
    fig.add_trace(
        go.Bar(
            x=bd["date"],
            y=bd["buy"],
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
            x=bd["date"],
            y=-bd["sell"],
            name=f"{selected_broker} 賣出",
            marker_color="#2e8b57",
            width=bar_width_ms,
            showlegend=False,
        ),
        row=3,
        col=1,
    )

    # Row 4: Top N buy brokers aggregated buy/sell
    if not topn_buy_daily.empty:
        fig.add_trace(
            go.Bar(
                x=topn_buy_daily["date"],
                y=topn_buy_daily["buy"],
                name=f"{topn_label} 買入",
                marker_color="#d24d57",
                width=bar_width_ms,
                showlegend=False,
            ),
            row=4,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                x=topn_buy_daily["date"],
                y=-topn_buy_daily["sell"],
                name=f"{topn_label} 賣出",
                marker_color="#2e8b57",
                width=bar_width_ms,
                showlegend=False,
            ),
            row=4,
            col=1,
        )

    # Row 5: Top N sell brokers aggregated buy/sell
    if not topn_sell_daily.empty:
        fig.add_trace(
            go.Bar(
                x=topn_sell_daily["date"],
                y=topn_sell_daily["buy"],
                name=f"{topn_label} 賣超 買入",
                marker_color="#d24d57",
                width=bar_width_ms,
                showlegend=False,
            ),
            row=5,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                x=topn_sell_daily["date"],
                y=-topn_sell_daily["sell"],
                name=f"{topn_label} 賣超 賣出",
                marker_color="#2e8b57",
                width=bar_width_ms,
                showlegend=False,
            ),
            row=5,
            col=1,
        )
    fig.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        height=1040,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        barmode="relative",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(
        matches="x",
        showspikes=True,
        spikemode="across",
        spikedash="dash",
        spikecolor="#999999",
        spikethickness=1,
        spikesnap="cursor",
    )

    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_yaxes(title_text="Selected Broker", row=3, col=1)
    fig.update_yaxes(title_text="Top Buyers", row=4, col=1)
    fig.update_yaxes(title_text="Top Sellers", row=5, col=1)
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
    agg = agg[["broker", "buy", "sell", "net", "avg_buy", "avg_sell"]]
    agg = agg.sort_values("net", ascending=False)

    top_buy = agg.head(10).copy()
    top_buy.insert(0, "rank", range(1, len(top_buy) + 1))
    buy_total = pd.DataFrame(
        [{
            "rank": "",
            "broker": "合計",
            "buy": pd.NA,
            "sell": pd.NA,
            "net": top_buy["net"].sum(),
            "avg_buy": pd.NA,
            "avg_sell": pd.NA,
        }]
    )
    top_buy = pd.concat([top_buy, buy_total], ignore_index=True)

    top_sell = agg.sort_values("net", ascending=True).head(10).copy()
    top_sell.insert(0, "rank", range(1, len(top_sell) + 1))
    sell_total = pd.DataFrame(
        [{
            "rank": "",
            "broker": "合計",
            "buy": pd.NA,
            "sell": pd.NA,
            "net": top_sell["net"].sum(),
            "avg_buy": pd.NA,
            "avg_sell": pd.NA,
        }]
    )
    top_sell = pd.concat([top_sell, sell_total], ignore_index=True)

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


# -----------------------------
# Dash App
# -----------------------------
app = Dash(__name__)
app.title = "籌碼K線 MVP"

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
                    value="2330",
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
                html.Div(id="hint", style={"marginLeft": "auto", "opacity": 0.75, "fontSize": "12px"}),
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
        dcc.Store(id="store-brokers"),
        dcc.Store(id="store-events"),
    ],
)


@app.callback(
    Output("store-ohlcv", "data"),
    Output("store-broker", "data"),
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
    Input("stock-input", "value"),
)
def on_stock_change(stock_id: str):
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
            "請輸入股票代號",
        )

    ohlcv, broker_df, brokers, event_dates = load_data(stock_id)

    options = [{"label": b, "value": b} for b in brokers]
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

    hint = f"資料筆數：K線 {len(ohlcv)} 天、券商交易 {len(broker_df):,} 筆、事件 {len(event_dates)} 筆"
    return (
        ohlcv.to_dict("records"),
        broker_df.to_dict("records"),
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
    )


@app.callback(
    Output("main-chart", "figure"),
    Output("summary-box", "children"),
    Output("top-buy-table", "data"),
    Output("top-sell-table", "data"),
    Input("store-ohlcv", "data"),
    Input("store-broker", "data"),
    Input("store-events", "data"),
    Input("broker-dropdown", "value"),
    Input("date-start", "date"),
    Input("date-end", "date"),
    Input("topn-input", "value"),
    Input("main-chart", "relayoutData"),
    State("stock-input", "value"),
)
def render_all(ohlcv_data, broker_data, event_dates, selected_broker, start_date, end_date, topn_value, relayout_data, stock_id):
    if not ohlcv_data or not broker_data or not selected_broker:
        fig = go.Figure()
        fig.update_layout(height=1040, margin=dict(l=10, r=10, t=30, b=10))
        return fig, "—", [], []

    ohlcv = pd.DataFrame(ohlcv_data)
    broker_df = pd.DataFrame(broker_data)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    broker_df["date"] = pd.to_datetime(broker_df["date"])
    event_dt = pd.to_datetime(event_dates) if event_dates else []

    # Filter by date picker range first
    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None
    ohlcv_base = filter_by_range(ohlcv, start_dt, end_dt, "date")
    broker_base = filter_by_range(broker_df, start_dt, end_dt, "date")
    ohlcv_base = ohlcv_base.copy()
    broker_base = broker_base.copy()

    # Keep broker dates only where OHLC exists
    if not ohlcv_base.empty and not broker_base.empty:
        valid_dates = set(ohlcv_base["date"])
        broker_base = broker_base[broker_base["date"].isin(valid_dates)]

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

    topn_buy_daily = topn_daily_sum(broker_view, topn_value, "buy")
    topn_sell_daily = topn_daily_sum(broker_view, topn_value, "sell")
    topn_label = f"Top{int(topn_value) if topn_value else 10}"

    # Build chart (use date-filtered data)
    fig = build_figure(
        ohlcv_base,
        broker_base,
        selected_broker,
        topn_buy_daily,
        topn_sell_daily,
        topn_label,
        event_dt,
    )

    # Remove missing dates based on OHLC dates
    if not ohlcv_base.empty:
        all_days = pd.date_range(ohlcv_base["date"].min(), ohlcv_base["date"].max(), freq="D")
        missing = all_days.difference(pd.to_datetime(ohlcv_base["date"].unique()))
        if len(missing) > 0:
            fig.update_xaxes(rangebreaks=[{"values": missing.to_pydatetime().tolist()}])

    # Category x-axis already skips missing dates (union across traces)

    # If range present, lock xaxis range for all subplots
    if start is not None and end is not None:
        pad = pd.Timedelta(milliseconds=BAR_WIDTH_MS / 2)
        fig.update_xaxes(range=[start - pad, end + pad])

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
    return fig, summary_box, top_buy.to_dict("records"), top_sell.to_dict("records")


@app.callback(
    Output("broker-dropdown", "value", allow_duplicate=True),
    Input("top-buy-table", "active_cell"),
    Input("top-sell-table", "active_cell"),
    State("top-buy-table", "data"),
    State("top-sell-table", "data"),
    prevent_initial_call=True,
)
def on_table_click(buy_cell, sell_cell, buy_data, sell_data):
    if not callback_context.triggered:
        return no_update

    trigger = callback_context.triggered[0]["prop_id"].split(".")[0]
    if trigger == "top-buy-table":
        cell = buy_cell
        data = buy_data or []
    else:
        cell = sell_cell
        data = sell_data or []

    if not cell or cell.get("column_id") != "broker":
        return no_update

    row = cell.get("row")
    if row is None or row >= len(data):
        return no_update

    return data[row].get("broker", no_update)


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
