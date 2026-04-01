import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from app.collector import run_all
from app.db import fetch_all, fetch_one, init_tables

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)

SCHEDULE_TZ = os.getenv("SCHEDULE_TZ", "America/Los_Angeles")
SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "13"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "7"))


def has_data_for_date(date_str: str) -> bool:
    row = fetch_one("SELECT 1 AS ok FROM track_deviation WHERE date = %s", (date_str,))
    return row is not None


def get_missing_dates(lookback_days: int) -> list[str]:
    """Find dates in the past N days that don't have data (excluding weekends)"""
    tz = ZoneInfo(SCHEDULE_TZ)
    today = datetime.now(tz).date()
    
    # Get all dates from DB in the lookback window
    start_date = today - timedelta(days=lookback_days)
    rows = fetch_all(
        "SELECT DISTINCT date FROM track_deviation WHERE date >= %s ORDER BY date",
        (start_date,)
    )
    existing_dates = {row["date"] for row in rows}
    
    # Generate expected dates (weekdays only)
    missing = []
    current = start_date
    while current <= today:
        # Skip weekends (5=Saturday, 6=Sunday)
        if current.weekday() < 5 and current not in existing_dates:
            missing.append(current.isoformat())
        current += timedelta(days=1)
    
    return missing


def collect_job() -> None:
    LOGGER.info("Running daily collection job")
    try:
        run_all()
        LOGGER.info("Collection completed")
    except Exception:
        LOGGER.exception("Collection failed")


def backfill_missing_data() -> None:
    """Check for and backfill any missing dates in the lookback window"""
    try:
        missing = get_missing_dates(BACKFILL_DAYS)
        if not missing:
            LOGGER.info("No missing dates found in the past %d days", BACKFILL_DAYS)
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
    LOGGER.info("Checking for missing data in the past %d days", BACKFILL_DAYS)
    backfill_missing_data()
    
    # Also check today specifically
    today = datetime.now(tz).date().isoformat()
    if not has_data_for_date(today):
        LOGGER.info("No data found for today (%s), running collection", today)
        collect_job()
    else:
        LOGGER.info("Data already exists for today (%s)", today)

    scheduler = BlockingScheduler(timezone=tz)
    scheduler.add_job(
        collect_job,
        CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, timezone=tz),
        id="daily_collection",
        replace_existing=True,
    )

    LOGGER.info(
        "Scheduler started: daily at %02d:%02d (%s)",
        SCHEDULE_HOUR,
        SCHEDULE_MINUTE,
        SCHEDULE_TZ,
    )
    scheduler.start()


if __name__ == "__main__":
    run_scheduler()
