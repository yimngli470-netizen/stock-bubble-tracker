import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from app.collector import run_all
from app.db import fetch_one, init_tables

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)

SCHEDULE_TZ = os.getenv("SCHEDULE_TZ", "America/Los_Angeles")
SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "13"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))


def has_today_data(today_str: str) -> bool:
    row = fetch_one("SELECT 1 AS ok FROM track_deviation WHERE date = %s", (today_str,))
    return row is not None


def collect_job() -> None:
    LOGGER.info("Running daily collection job")
    try:
        run_all()
        LOGGER.info("Collection completed")
    except Exception:
        LOGGER.exception("Collection failed")


def run_scheduler() -> None:
    tz = ZoneInfo(SCHEDULE_TZ)
    today = datetime.now(tz).date().isoformat()

    init_tables()

    if not has_today_data(today):
        LOGGER.info("No data found for %s, running catch-up collection", today)
        collect_job()
    else:
        LOGGER.info("Data already exists for %s, skipping catch-up", today)

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
