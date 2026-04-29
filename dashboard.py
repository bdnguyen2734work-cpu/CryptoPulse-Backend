"""
dashboard.py – Streamlit analytics dashboard cho CryptoPulse.

Cải tiến:
  • Dùng config.py, không hardcode
  • Cache connection pool (st.cache_resource)
  • Tính RSI + MACD → khớp với MarketFragment / AnalysisFragment trong app
  • Layout 2 cột giống app Android
  • Auto-rerun 5s (thay vì 2s để giảm tải DB)
"""

import time
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import mysql.connector

from config import DB_CONFIG, SYMBOLS, APP_COIN_MAP

# ─────────────────────────────────────────
# Cấu hình trang
# ─────────────────────────────────────────
st.set_page_config(
    page_title="CryptoPulse Pro",
    layout="wide",
    page_icon="🛡️",
)

# ─────────────────────────────────────────
# Connection (cache toàn session)
# ─────────────────────────────────────────
@st.cache_resource
def get_pool():
    """Tạo 1 connection duy nhất, tái sử dụng qua các rerun."""
    return mysql.connector.connect(**DB_CONFIG)


def query_df(sql: str) -> pd.DataFrame:
    try:
        conn = get_pool()
        if not conn.is_connected():
            conn.reconnect(attempts=3, delay=1)
        return pd.read_sql(sql, conn)
    except Exception as exc:
        st.error(f"DB Error: {exc}")
        return pd.DataFrame()


# ─────────────────────────────────────────
# Tính chỉ số kỹ thuật (khớp MarketFragment)
# ─────────────────────────────────────────
def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta  = series.diff()
    gain   = delta.clip(lower=0).rolling(period).mean()
    loss   = (-delta.clip(upper=0)).rolling(period).mean()
    rs     = gain / loss.replace(0, 1e-9)
    return 100 - (100 / (1 + rs))


def calc_macd(series: pd.Series):
    ema12  = series.ewm(span=12, adjust=False).mean()
    ema26  = series.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal


# ─────────────────────────────────────────
# Load dữ liệu lịch sử
# ─────────────────────────────────────────
def get_history(symbol: str, tf: str, limit: int = 500) -> pd.DataFrame:
    df = query_df(f"""
        SELECT symbol, open_time, open_price, high_price,
               low_price, close_price, volume
        FROM   kline_{tf}
        WHERE  symbol = '{symbol}'
        ORDER  BY open_time DESC
        LIMIT  {limit}
    """)
    if df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], unit="s")
    return df.sort_values("open_time").reset_index(drop=True)


# ─────────────────────────────────────────
# UI
# ─────────────────────────────────────────
st.markdown(
    "<h1 style='text-align:center;color:#A9FFAC;'>🛡️ CryptoPulse Professional</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "<p style='text-align:center;color:#767576;'>Big Data · Real-time Streaming · Obsidian Terminal</p>",
    unsafe_allow_html=True,
)

# Sidebar
with st.sidebar:
    st.header("⚙️ Điều khiển")
    selected_symbol = st.selectbox("🔍 Coin:", SYMBOLS,
                                   index=SYMBOLS.index("BTCUSDT"))
    selected_tf     = st.radio("⏰ Timeframe:",
                               ["1m", "5m", "15m", "1h", "1d", "1w"],
                               index=3, horizontal=True)
    st.markdown("---")
    st.caption("App Android: CryptoPulse Obsidian")

# Display name
coin_info = APP_COIN_MAP.get(selected_symbol, {"name": selected_symbol, "symbol": selected_symbol})

# ─────────────────────────────────────────
# Main content
# ─────────────────────────────────────────
df = get_history(selected_symbol, selected_tf)

if df.empty:
    st.warning(f"⏳ Chưa có dữ liệu `{selected_symbol}` [{selected_tf}]. "
               "Chạy backfill hoặc đợi Spark xử lý.")
    time.sleep(5)
    st.rerun()

latest = df.iloc[-1]
prev   = df.iloc[-2] if len(df) > 1 else latest
change_pct = (latest["close_price"] - prev["close_price"]) / prev["close_price"] * 100

# ── Metrics row (khớp HomeFragment stats) ──
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("💰 Giá",        f"${latest['close_price']:,.4f}",  f"{change_pct:+.2f}%")
c2.metric("📈 24H High",   f"${df['high_price'].max():,.4f}")
c3.metric("📉 24H Low",    f"${df['low_price'].min():,.4f}")
c4.metric("📊 Volume",     f"{latest['volume']:,.0f}")
c5.metric("🕯️ Nến",       f"{len(df):,}")

# ── Candlestick + Volume chart ──
fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.02,
    row_heights=[0.65, 0.15, 0.20],
    subplot_titles=("", "Volume", "RSI(14)"),
)

fig.add_trace(go.Candlestick(
    x=df["open_time"],
    open=df["open_price"], high=df["high_price"],
    low=df["low_price"],   close=df["close_price"],
    name="Giá",
    increasing_line_color="#A9FFAC",
    decreasing_line_color="#FF7162",
), row=1, col=1)

vol_colors = [
    "#A9FFAC" if c >= o else "#FF7162"
    for c, o in zip(df["close_price"], df["open_price"])
]
fig.add_trace(go.Bar(
    x=df["open_time"], y=df["volume"],
    marker_color=vol_colors, opacity=0.7, name="Volume",
), row=2, col=1)

# RSI (khớp MarketFragment indicators)
rsi = calc_rsi(df["close_price"])
fig.add_trace(go.Scatter(
    x=df["open_time"], y=rsi,
    line=dict(color="#A68CFF", width=1.5), name="RSI",
), row=3, col=1)
fig.add_hline(y=70, line_dash="dot", line_color="#FF7162", row=3, col=1)
fig.add_hline(y=30, line_dash="dot", line_color="#A9FFAC", row=3, col=1)

fig.update_layout(
    template="plotly_dark",
    plot_bgcolor="#0E0E0F",
    paper_bgcolor="#0E0E0F",
    xaxis_rangeslider_visible=False,
    height=700,
    margin=dict(l=10, r=10, t=30, b=10),
    showlegend=False,
    font=dict(color="#ADAAAB"),
)
fig.update_yaxes(gridcolor="#1A191B")

st.plotly_chart(fig, use_container_width=True,
                key=f"chart_{selected_symbol}_{selected_tf}_{int(time.time()//5)}")

# ── MACD + Indicators (khớp AnalysisFragment) ──
st.markdown("---")
col_macd, col_gauge = st.columns([2, 1])

with col_macd:
    st.subheader("📡 MACD Signal")
    macd_line, signal_line = calc_macd(df["close_price"])
    histogram = macd_line - signal_line

    fig_macd = go.Figure()
    fig_macd.add_trace(go.Bar(
        x=df["open_time"], y=histogram,
        marker_color=["#A9FFAC" if v >= 0 else "#FF7162" for v in histogram],
        name="Histogram",
    ))
    fig_macd.add_trace(go.Scatter(
        x=df["open_time"], y=macd_line,
        line=dict(color="#A9FFAC", width=1.5), name="MACD",
    ))
    fig_macd.add_trace(go.Scatter(
        x=df["open_time"], y=signal_line,
        line=dict(color="#FF7162", width=1.5), name="Signal",
    ))
    fig_macd.update_layout(
        template="plotly_dark",
        plot_bgcolor="#0E0E0F", paper_bgcolor="#0E0E0F",
        height=250, margin=dict(l=5, r=5, t=10, b=5),
        showlegend=True,
        font=dict(color="#ADAAAB"),
    )
    st.plotly_chart(fig_macd, use_container_width=True,
                    key=f"macd_{selected_symbol}_{selected_tf}")

with col_gauge:
    st.subheader("🎯 Indicators")
    rsi_val = rsi.iloc[-1] if not rsi.isna().all() else 50
    macd_val = macd_line.iloc[-1]
    sig_val  = signal_line.iloc[-1]

    if rsi_val > 70:
        rsi_label = "🔴 Overbought"
    elif rsi_val < 30:
        rsi_label = "🟢 Oversold"
    else:
        rsi_label = "🟡 Neutral"

    macd_label = "🟢 Golden Cross" if macd_val > sig_val else "🔴 Death Cross"

    st.metric("RSI (14)",    f"{rsi_val:.1f}", rsi_label)
    st.metric("MACD Signal", macd_label)
    st.metric("Support",     f"${df['low_price'].tail(20).min():,.2f}")
    st.metric("Resistance",  f"${df['high_price'].tail(20).max():,.2f}")

# Auto-refresh 5s
time.sleep(5)
st.rerun()
