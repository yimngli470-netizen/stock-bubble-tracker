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


def run_backfill() -> dict[str, int]:
    init_tables()
    return {
        "deviation_days": backfill_deviation(),
        "liquidity_days": backfill_liquidity(),
        "ipo_days": backfill_ipo(),
    }
