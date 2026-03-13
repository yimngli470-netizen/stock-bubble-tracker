from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas_datareader.data as web
import requests
import yfinance as yf

from app.db import get_conn, init_tables

LOGGER = logging.getLogger(__name__)


def run_deviation(run_date: datetime | None = None) -> None:
    df = yf.Ticker("^NDX").history(period="1y")
    curr = float(df["Close"].iloc[-1])
    sma = float(df["Close"].rolling(window=200).mean().iloc[-1])
    dev = ((curr - sma) / sma) * 100

    date_value = (run_date or datetime.now()).date()
    query = """
        INSERT INTO track_deviation (date, price, sma_200, deviation_pct)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET price = EXCLUDED.price,
            sma_200 = EXCLUDED.sma_200,
            deviation_pct = EXCLUDED.deviation_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, curr, sma, dev))
        conn.commit()


def run_liquidity(run_date: datetime | None = None) -> None:
    start = datetime.now() - timedelta(days=5)
    rrp = float(web.DataReader("RRPONTSYD", "fred", start).iloc[-1].item())
    tga = float(web.DataReader("WTREGEN", "fred", start).iloc[-1].item())

    date_value = (run_date or datetime.now()).date()
    query = """
        INSERT INTO track_liquidity (date, rrp_billions, tga_billions)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET rrp_billions = EXCLUDED.rrp_billions,
            tga_billions = EXCLUDED.tga_billions
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, rrp, tga))
        conn.commit()


def run_sentiment(run_date: datetime | None = None) -> None:
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    response.raise_for_status()

    data = response.json()
    score = float(data["fear_and_greed"]["score"])
    rating = str(data["fear_and_greed"]["rating"])

    date_value = (run_date or datetime.now()).date()
    query = """
        INSERT INTO track_sentiment (date, fear_greed_score, rating)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET fear_greed_score = EXCLUDED.fear_greed_score,
            rating = EXCLUDED.rating
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, score, rating))
        conn.commit()


def run_ipo_heat(run_date: datetime | None = None) -> None:
    hist = yf.Ticker("IPO").history(period="5d")
    curr = float(hist["Close"].iloc[-1])
    vol_ratio = float(hist["Volume"].iloc[-1] / hist["Volume"].mean())

    date_value = (run_date or datetime.now()).date()
    query = """
        INSERT INTO track_ipo_heat (date, ipo_etf_price, vol_heat_ratio)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET ipo_etf_price = EXCLUDED.ipo_etf_price,
            vol_heat_ratio = EXCLUDED.vol_heat_ratio
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, curr, vol_ratio))
        conn.commit()


def run_valuation(run_date: datetime | None = None) -> None:
    """Track S&P 500 and NASDAQ PE ratios"""
    spy = yf.Ticker("SPY")
    spy_info = spy.info
    spy_pe = spy_info.get("trailingPE", None)

    qqq = yf.Ticker("QQQ")
    qqq_info = qqq.info
    qqq_pe = qqq_info.get("trailingPE", None)

    # Historical average PE for S&P 500 is ~16
    spy_pe_deviation = ((spy_pe - 16) / 16) * 100 if spy_pe else None

    date_value = (run_date or datetime.now()).date()
    query = """
        INSERT INTO track_valuation (date, spy_pe, qqq_pe, spy_pe_deviation_pct)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET spy_pe = EXCLUDED.spy_pe,
            qqq_pe = EXCLUDED.qqq_pe,
            spy_pe_deviation_pct = EXCLUDED.spy_pe_deviation_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, spy_pe, qqq_pe, spy_pe_deviation))
        conn.commit()


def run_volatility(run_date: datetime | None = None) -> None:
    """Track VIX (fear index) and its moving average"""
    vix = yf.Ticker("^VIX").history(period="1mo")
    current_vix = float(vix["Close"].iloc[-1])
    vix_sma_20 = float(vix["Close"].rolling(window=20).mean().iloc[-1])

    date_value = (run_date or datetime.now()).date()
    query = """
        INSERT INTO track_volatility (date, vix_level, vix_sma_20)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET vix_level = EXCLUDED.vix_level,
            vix_sma_20 = EXCLUDED.vix_sma_20
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, current_vix, vix_sma_20))
        conn.commit()


def run_all() -> None:
    init_tables()
    jobs = [
        ("deviation", run_deviation),
        ("liquidity", run_liquidity),
        ("sentiment", run_sentiment),
        ("ipo_heat", run_ipo_heat),
        ("valuation", run_valuation),
        ("volatility", run_volatility),
    ]

    for name, job in jobs:
        try:
            job()
            LOGGER.info("Saved %s data", name)
        except Exception:
            LOGGER.exception("Failed saving %s data", name)
