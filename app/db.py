import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://bubble:bubble@localhost:5432/bubble_tracker")


@contextmanager
def get_conn():
    conn = psycopg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def init_tables():
    statements = [
        """
        CREATE TABLE IF NOT EXISTS track_deviation (
            date DATE PRIMARY KEY,
            price DOUBLE PRECISION,
            sma_200 DOUBLE PRECISION,
            deviation_pct DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_liquidity (
            date DATE PRIMARY KEY,
            rrp_billions DOUBLE PRECISION,
            tga_billions DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_sentiment (
            date DATE PRIMARY KEY,
            fear_greed_score DOUBLE PRECISION,
            rating TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_ipo_heat (
            date DATE PRIMARY KEY,
            ipo_etf_price DOUBLE PRECISION,
            vol_heat_ratio DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_valuation (
            date DATE PRIMARY KEY,
            spy_pe DOUBLE PRECISION,
            qqq_pe DOUBLE PRECISION,
            spy_pe_deviation_pct DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_volatility (
            date DATE PRIMARY KEY,
            vix_level DOUBLE PRECISION,
            vix_sma_20 DOUBLE PRECISION
        )
        """,
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
        conn.commit()


def fetch_all(query: str, params: tuple = ()) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchall()


def fetch_one(query: str, params: tuple = ()) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchone()
