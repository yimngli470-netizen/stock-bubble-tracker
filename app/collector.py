from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas_datareader.data as web
import requests
import yfinance as yf

from app.db import get_conn, init_tables

LOGGER = logging.getLogger(__name__)


def _latest_market_row(df, target_date):
    if df.empty:
        return None, None

    eligible = df[df.index.date <= target_date]
    if eligible.empty:
        return None, None

    source_date = eligible.index[-1].date()
    return source_date, eligible.iloc[-1]


def _is_missing_target_date(source_date, target_date, metric_name: str) -> bool:
    if source_date == target_date:
        return False

    LOGGER.info(
        "Skipping %s for %s; latest source observation is %s",
        metric_name,
        target_date,
        source_date,
    )
    return True


def _delete_stale_rows(cur, table_name: str, source_date, target_date) -> None:
    if source_date >= target_date:
        return

    cur.execute(
        f"DELETE FROM {table_name} WHERE date > %s AND date <= %s",
        (source_date, target_date),
    )


def run_deviation(run_date: datetime | None = None) -> None:
    target_date = (run_date or datetime.now()).date()

    if run_date:
        # Fetch enough history to compute 200-day SMA ending on date_value
        df = yf.Ticker("^NDX").history(
            start=target_date - timedelta(days=300),
            end=target_date + timedelta(days=1),
        )
    else:
        df = yf.Ticker("^NDX").history(period="1y")

    if df.empty:
        LOGGER.warning("No NDX data available for %s", target_date)
        return

    date_value, latest = _latest_market_row(df, target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "deviation")):
        return

    df = df[df.index.date <= date_value]
    curr = float(latest["Close"])
    sma = float(df["Close"].rolling(window=200).mean().iloc[-1])
    dev = ((curr - sma) / sma) * 100
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
            if not run_date:
                _delete_stale_rows(cur, "track_deviation", date_value, target_date)
        conn.commit()


def run_liquidity(run_date: datetime | None = None) -> None:
    target_date = (run_date or datetime.now()).date()

    try:
        if run_date:
            # Bound the end date so we only get data up to the target date
            start = target_date - timedelta(days=14)
            end = target_date
            rrp_data = web.DataReader("RRPONTSYD", "fred", start, end)
            tga_data = web.DataReader("WTREGEN", "fred", start, end)
        else:
            start = target_date - timedelta(days=14)
            rrp_data = web.DataReader("RRPONTSYD", "fred", start, target_date)
            tga_data = web.DataReader("WTREGEN", "fred", start - timedelta(days=14), target_date)

        if len(rrp_data) == 0 or len(tga_data) == 0:
            LOGGER.warning("No FRED data available for %s (likely weekend/holiday)", target_date)
            return

        rrp_data = rrp_data.dropna()
        tga_data = tga_data.dropna()
        if len(rrp_data) == 0 or len(tga_data) == 0:
            LOGGER.warning("No populated FRED data available for %s", target_date)
            return

        date_value = rrp_data.index[-1].date()
        if run_date and _is_missing_target_date(date_value, target_date, "liquidity"):
            return

        tga_data = tga_data[tga_data.index.date <= date_value]
        if len(tga_data) == 0:
            LOGGER.warning("No TGA data available on or before %s", date_value)
            return

        rrp = float(rrp_data.iloc[-1].item())
        tga = float(tga_data.iloc[-1].item())
    except (IndexError, KeyError):
        LOGGER.warning("FRED data not available for %s (likely weekend/holiday)", target_date)
        return

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
            if not run_date:
                _delete_stale_rows(cur, "track_liquidity", date_value, target_date)
        conn.commit()


def run_sentiment(run_date: datetime | None = None) -> None:
    date_value = (run_date or datetime.now()).date()
    today = datetime.now().date()
    
    # CNN Fear & Greed API only provides current data, not historical
    # Skip if trying to backfill a past date
    if date_value < today:
        LOGGER.info("Skipping sentiment for %s (Fear & Greed API only provides current data)", date_value)
        return
    
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://edition.cnn.com/",
    }
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()

    data = response.json()
    score = float(data["fear_and_greed"]["score"])
    rating = str(data["fear_and_greed"]["rating"])

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
    target_date = (run_date or datetime.now()).date()

    if run_date:
        hist = yf.Ticker("IPO").history(
            start=target_date - timedelta(days=14),
            end=target_date + timedelta(days=1),
        )
    else:
        hist = yf.Ticker("IPO").history(period="5d")

    if hist.empty:
        LOGGER.warning("No IPO data available for %s", target_date)
        return

    date_value, latest = _latest_market_row(hist, target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "ipo_heat")):
        return

    hist = hist[hist.index.date <= date_value]
    curr = float(latest["Close"])
    vol_ratio = float(latest["Volume"] / hist["Volume"].mean())
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
            if not run_date:
                _delete_stale_rows(cur, "track_ipo_heat", date_value, target_date)
        conn.commit()


def run_valuation(run_date: datetime | None = None) -> None:
    """Track S&P 500 and NASDAQ PE ratios"""
    date_value = (run_date or datetime.now()).date()
    today = datetime.now().date()
    
    # yfinance .info only provides current PE ratios, not historical
    # Skip if trying to backfill a past date
    if date_value < today:
        LOGGER.info("Skipping valuation for %s (yfinance PE ratios only provide current data)", date_value)
        return
    
    spy = yf.Ticker("SPY")
    spy_info = spy.info
    spy_pe = spy_info.get("trailingPE", None)

    qqq = yf.Ticker("QQQ")
    qqq_info = qqq.info
    qqq_pe = qqq_info.get("trailingPE", None)

    # Historical average PE for S&P 500 is ~16
    spy_pe_deviation = ((spy_pe - 16) / 16) * 100 if spy_pe else None

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
    target_date = (run_date or datetime.now()).date()

    if run_date:
        vix = yf.Ticker("^VIX").history(
            start=target_date - timedelta(days=35),
            end=target_date + timedelta(days=1),
        )
    else:
        vix = yf.Ticker("^VIX").history(period="1mo")

    if vix.empty:
        LOGGER.warning("No VIX data available for %s", target_date)
        return

    date_value, latest = _latest_market_row(vix, target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "volatility")):
        return

    vix = vix[vix.index.date <= date_value]
    current_vix = float(latest["Close"])
    vix_sma_20 = float(vix["Close"].rolling(window=20).mean().iloc[-1])
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
            if not run_date:
                _delete_stale_rows(cur, "track_volatility", date_value, target_date)
        conn.commit()


def run_all(run_date: datetime | None = None) -> None:
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
            job(run_date)
            LOGGER.info("Saved %s data", name)
        except Exception:
            LOGGER.exception("Failed saving %s data", name)
