from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pandas_datareader.data as web
import yfinance as yf

from app.db import get_conn, init_tables


def backfill_deviation() -> int:
    df = yf.Ticker("^NDX").history(period="2y")
    df["sma_200"] = df["Close"].rolling(window=200).mean()
    df["deviation"] = ((df["Close"] - df["sma_200"]) / df["sma_200"]) * 100
    last_year = df.iloc[-365:].copy()

    query = """
        INSERT INTO track_deviation (date, price, sma_200, deviation_pct)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET price = EXCLUDED.price,
            sma_200 = EXCLUDED.sma_200,
            deviation_pct = EXCLUDED.deviation_pct
    """

    count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for date, row in last_year.iterrows():
                if pd.notna(row["deviation"]):
                    cur.execute(
                        query,
                        (
                            date.date(),
                            float(row["Close"]),
                            float(row["sma_200"]),
                            float(row["deviation"]),
                        ),
                    )
                    count += 1
        conn.commit()

    return count


def backfill_liquidity() -> int:
    start_date = datetime.now() - timedelta(days=365)
    rrp = web.DataReader("RRPONTSYD", "fred", start_date)
    tga = web.DataReader("WTREGEN", "fred", start_date)
    df = pd.DataFrame({"rrp": rrp["RRPONTSYD"], "tga": tga["WTREGEN"]}).dropna()

    query = """
        INSERT INTO track_liquidity (date, rrp_billions, tga_billions)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET rrp_billions = EXCLUDED.rrp_billions,
            tga_billions = EXCLUDED.tga_billions
    """

    count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for date, row in df.iterrows():
                cur.execute(query, (date.date(), float(row["rrp"]), float(row["tga"])))
                count += 1
        conn.commit()

    return count


def backfill_ipo() -> int:
    df = yf.Ticker("IPO").history(period="1y")
    df["avg_vol"] = df["Volume"].rolling(window=20).mean()
    df["heat_ratio"] = df["Volume"] / df["avg_vol"]

    query = """
        INSERT INTO track_ipo_heat (date, ipo_etf_price, vol_heat_ratio)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET ipo_etf_price = EXCLUDED.ipo_etf_price,
            vol_heat_ratio = EXCLUDED.vol_heat_ratio
    """

    count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for date, row in df.iterrows():
                if pd.notna(row["heat_ratio"]):
                    cur.execute(
                        query,
                        (date.date(), float(row["Close"]), float(row["heat_ratio"])),
                    )
                    count += 1
        conn.commit()

    return count


def backfill_valuation() -> int:
    """Backfill valuation data (PE ratios)"""
    # Note: yfinance doesn't provide historical PE ratios easily
    # For demo purposes, we'll just set today's value
    # In production, you'd use a financial data API with historical PE data
    spy = yf.Ticker("SPY")
    spy_info = spy.info
    spy_pe = spy_info.get("trailingPE", None)

    qqq = yf.Ticker("QQQ")
    qqq_info = qqq.info
    qqq_pe = qqq_info.get("trailingPE", None)

    if not spy_pe:
        return 0

    spy_pe_deviation = ((spy_pe - 16) / 16) * 100

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
            cur.execute(query, (datetime.now().date(), spy_pe, qqq_pe, spy_pe_deviation))
        conn.commit()

    return 1


def backfill_volatility() -> int:
    """Backfill VIX data for last year"""
    vix = yf.Ticker("^VIX").history(period="1y")
    vix["sma_20"] = vix["Close"].rolling(window=20).mean()

    query = """
        INSERT INTO track_volatility (date, vix_level, vix_sma_20)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET vix_level = EXCLUDED.vix_level,
            vix_sma_20 = EXCLUDED.vix_sma_20
    """

    count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for date, row in vix.iterrows():
                vix_sma = row["sma_20"] if pd.notna(row["sma_20"]) else None
                cur.execute(
                    query,
                    (date.date(), float(row["Close"]), vix_sma),
                )
                count += 1
        conn.commit()

    return count


def run_backfill() -> dict[str, int]:
    init_tables()
    return {
        "deviation_days": backfill_deviation(),
        "liquidity_days": backfill_liquidity(),
        "ipo_days": backfill_ipo(),
        "valuation_days": backfill_valuation(),
        "volatility_days": backfill_volatility(),
    }
