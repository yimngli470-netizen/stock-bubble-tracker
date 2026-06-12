import logging
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import yfinance as yf
from apscheduler.schedulers.blocking import BlockingScheduler

from app.collector import run_all
from app.db import fetch_all, fetch_one, init_tables

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)

SCHEDULE_TZ = os.getenv("SCHEDULE_TZ", "America/Los_Angeles")
SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "13"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))
CATCHUP_DAYS = int(os.getenv("CATCHUP_DAYS", os.getenv("BACKFILL_DAYS", "7")))

BACKFILL_TABLES = {
    "deviation": "track_deviation",
    "liquidity": "track_liquidity",
    "ipo_heat": "track_ipo_heat",
    "volatility": "track_volatility",
    "credit": "track_credit",
    "concentration": "track_concentration",
    "hot_sector": "track_hot_sector",
    "term_structure": "track_term_structure",
    "put_call": "track_put_call",
}


def has_data_for_date(date_str: str) -> bool:
    row = fetch_one("SELECT 1 AS ok FROM track_deviation WHERE date = %s", (date_str,))
    return row is not None


def get_latest_table_dates() -> dict[str, date | None]:
    rows = fetch_all(
        """
        SELECT 'deviation' AS metric, max(date) AS max_date FROM track_deviation
        UNION ALL
        SELECT 'liquidity', max(date) FROM track_liquidity
        UNION ALL
        SELECT 'ipo_heat', max(date) FROM track_ipo_heat
        UNION ALL
        SELECT 'volatility', max(date) FROM track_volatility
        UNION ALL
        SELECT 'credit', max(date) FROM track_credit
        UNION ALL
        SELECT 'concentration', max(date) FROM track_concentration
        UNION ALL
        SELECT 'hot_sector', max(date) FROM track_hot_sector
        UNION ALL
        SELECT 'term_structure', max(date) FROM track_term_structure
        UNION ALL
        SELECT 'put_call', max(date) FROM track_put_call
        """
    )
    return {row["metric"]: row["max_date"] for row in rows}


def get_expected_market_dates(start_date: date, end_date: date) -> list[date]:
    """Use NDX history as the trading-day calendar for backfillable market metrics."""
    if start_date > end_date:
        return []

    history = yf.Ticker("^NDX").history(
        start=start_date,
        end=end_date + timedelta(days=1),
    )
    if history.empty:
        return []

    return sorted({dt.date() for dt in history.index if start_date <= dt.date() <= end_date})


def get_missing_dates(lookback_days: int) -> list[str]:
    """Find trading dates missing from any backfillable table."""
    tz = ZoneInfo(SCHEDULE_TZ)
    today = datetime.now(tz).date()
    earliest_allowed = today - timedelta(days=lookback_days)
    latest_dates = get_latest_table_dates()
    populated_dates = [value for value in latest_dates.values() if value is not None]

    if populated_dates:
        start_date = max(earliest_allowed, min(populated_dates) - timedelta(days=3))
    else:
        start_date = earliest_allowed

    expected_dates = get_expected_market_dates(start_date, today)
    if not expected_dates:
        LOGGER.warning("No expected market dates found from %s to %s", start_date, today)
        return []

    existing_by_table = {}
    for metric, table_name in BACKFILL_TABLES.items():
        rows = fetch_all(
            f"SELECT date FROM {table_name} WHERE date >= %s AND date <= %s",
            (start_date, today),
        )
        existing_by_table[metric] = {row["date"] for row in rows}

    missing = []
    for expected_date in expected_dates:
        if any(expected_date not in dates for dates in existing_by_table.values()):
            missing.append(expected_date.isoformat())

    return missing


def collect_job() -> None:
    """Collect the latest available data.

    Collectors write market/FRED values under their source observation date, so
    this is safe to run repeatedly while waiting for data sources to update.
    """
    tz = ZoneInfo(SCHEDULE_TZ)
    today = datetime.now(tz).date().isoformat()
    LOGGER.info("Collecting latest available data as of %s", today)
    try:
        run_all()
        LOGGER.info("Latest data collection completed")
    except Exception:
        LOGGER.exception("Latest data collection failed")


def backfill_missing_data() -> None:
    """Check for and backfill any missing dates in the lookback window"""
    try:
        missing = get_missing_dates(CATCHUP_DAYS)
        if not missing:
            LOGGER.info("No missing dates found in the past %d days", CATCHUP_DAYS)
            return
        
        LOGGER.info("Found %d missing dates: %s", len(missing), missing[:5])
        for date_str in missing:
            try:
                LOGGER.info("Backfilling data for %s", date_str)
                # Parse the date string and pass it to run_all
                from datetime import datetime
                backfill_date = datetime.fromisoformat(date_str)
                run_all(backfill_date)
                LOGGER.info("Backfilled %s (Fear&Greed and Valuation skipped - no historical data)", date_str)
            except Exception:
                LOGGER.exception("Failed to backfill %s", date_str)
    except Exception:
        LOGGER.exception("Backfill process failed")


def run_scheduler() -> None:
    tz = ZoneInfo(SCHEDULE_TZ)
    
    init_tables()
    
    # Check for and backfill missing dates on startup
    LOGGER.info("Checking for missing data in the past %d days", CATCHUP_DAYS)
    backfill_missing_data()
    
    # Also refresh the latest available observation on startup.
    collect_job()

    check_interval = int(os.getenv("CHECK_INTERVAL_MINUTES", "30"))

    scheduler = BlockingScheduler(timezone=tz)
    scheduler.add_job(
        collect_job,
        "interval",
        minutes=check_interval,
        id="periodic_collection",
        replace_existing=True,
    )

    LOGGER.info(
        "Scheduler started: checking every %d minutes (%s)",
        check_interval,
        SCHEDULE_TZ,
    )
    scheduler.start()


if __name__ == "__main__":
    run_scheduler()
