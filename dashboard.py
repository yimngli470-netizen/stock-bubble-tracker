import os

import pandas as pd
import requests
import streamlit as st

API_BASE = os.getenv("API_BASE", "http://localhost:8080")

st.set_page_config(page_title="Market Risk Dashboard", layout="wide")
st.title("Market Risk Dashboard")


def load_data(metric_name: str) -> pd.DataFrame:
    response = requests.get(f"{API_BASE}/metrics/{metric_name}", timeout=20)
    response.raise_for_status()
    df = pd.DataFrame(response.json())
    if "date" in df.columns:
        df = df.sort_values("date")
    return df


if st.button("Refresh Data"):
    st.rerun()

col1, col2 = st.columns(2)

with col1:
    st.subheader("1. Price Deviation (Nasdaq 100)")
    df_dev = load_data("deviation")
    if not df_dev.empty:
        latest = df_dev.iloc[-1]
        st.metric("Current Deviation", f"{latest['deviation_pct']:.2f}%")
        st.line_chart(df_dev, x="date", y="deviation_pct")
    else:
        st.warning("No deviation data yet")

with col2:
    st.subheader("2. Market Sentiment (Fear & Greed)")
    df_sent = load_data("sentiment")
    if not df_sent.empty:
        latest = df_sent.iloc[-1]
        st.metric("Fear & Greed Index", f"{latest['fear_greed_score']:.0f}/100", delta=latest["rating"])
        st.line_chart(df_sent, x="date", y="fear_greed_score")
    else:
        st.warning("No sentiment data yet")

st.divider()
col3, col4 = st.columns(2)

with col3:
    st.subheader("3. Fed Liquidity")
    df_liq = load_data("liquidity")
    if not df_liq.empty:
        latest = df_liq.iloc[-1]
        st.metric("Reverse Repo", f"${latest['rrp_billions']:.0f}B")
        st.area_chart(df_liq, x="date", y=["rrp_billions", "tga_billions"])
    else:
        st.warning("No liquidity data yet")

with col4:
    st.subheader("4. IPO Heat")
    df_ipo = load_data("ipo_heat")
    if not df_ipo.empty:
        latest = df_ipo.iloc[-1]
        st.metric("IPO Volume Ratio", f"{latest['vol_heat_ratio']:.2f}x")
        st.bar_chart(df_ipo, x="date", y="vol_heat_ratio")
    else:
        st.warning("No IPO heat data yet")
