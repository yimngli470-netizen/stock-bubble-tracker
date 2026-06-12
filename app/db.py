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
        """
        CREATE TABLE IF NOT EXISTS track_credit (
            date DATE PRIMARY KEY,
            hy_spread_pct DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_concentration (
            date DATE PRIMARY KEY,
            smh_spy_ratio DOUBLE PRECISION,
            smh_spy_dev_pct DOUBLE PRECISION,
            qqq_qqqe_ratio DOUBLE PRECISION,
            qqq_qqqe_dev_pct DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_term_structure (
            date DATE PRIMARY KEY,
            vix_1m DOUBLE PRECISION,
            vix_3m DOUBLE PRECISION,
            vix_ratio DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_margin_debt (
            date DATE PRIMARY KEY,
            debit_balances_billions DOUBLE PRECISION,
            yoy_growth_pct DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_put_call (
            date DATE PRIMARY KEY,
            total_pc_ratio DOUBLE PRECISION,
            equity_pc_ratio DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS track_hot_sector (
            date DATE PRIMARY KEY,
            sector TEXT,
            dev_pct DOUBLE PRECISION
        )
        """,
        """
        ALTER TABLE track_ipo_heat ADD COLUMN IF NOT EXISTS ipo_rel_dev_pct DOUBLE PRECISION
        """,
        """
        CREATE TABLE IF NOT EXISTS track_fundamentals (
            date DATE PRIMARY KEY,
            erp_pct DOUBLE PRECISION,
            multiple_expansion_pct DOUBLE PRECISION,
            cape DOUBLE PRECISION,
            cape_percentile DOUBLE PRECISION,
            margins_pct DOUBLE PRECISION,
            credit_gap_pct DOUBLE PRECISION
        )
        """,
        # Native-cadence (monthly/quarterly) history for the slow fundamental
        # metrics, so their charts show real steps instead of one flat daily line.
        """
        CREATE TABLE IF NOT EXISTS track_metric_history (
            metric TEXT,
            date DATE,
            value DOUBLE PRECISION,
            PRIMARY KEY (metric, date)
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
