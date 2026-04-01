# Stock Bubble Tracker — Project Context for Claude

## What This Is

A containerized market monitoring system that tracks 6 financial indicators daily to detect potential stock market bubbles. Runs in Docker Compose. Dashboard served at `http://localhost:8080`.

## Architecture

3 Docker services defined in `docker-compose.yml`:

| Service | Container | Purpose |
|---|---|---|
| `db` | `bubble_db` | PostgreSQL 16, stores all time-series data |
| `api` | `bubble_api` | FastAPI on port 8080, serves REST API + dashboard |
| `worker` | `bubble_worker` | APScheduler, runs daily collection at 13:00 PT |

All Python code lives under `app/`. Root-level `collector.py`, `backfill.py`, `dashboard.py` are old/unused files — the active code is under `app/`.

## Key Files

- `app/collector.py` — data collection functions for all 6 metrics
- `app/worker.py` — scheduler + startup backfill logic
- `app/api.py` — FastAPI endpoints (`/`, `/health`, `/latest`, `/metrics/{metric}`)
- `app/db.py` — DB connection, table init, `fetch_all` / `fetch_one` helpers
- `app/static/index.html` — dashboard UI (Chart.js, vanilla JS, auto-refreshes every 30s)
- `docker-compose.yml` — service definitions

## 6 Tracked Metrics

| Metric | Table | Key Fields | Data Source |
|---|---|---|---|
| Deviation | `track_deviation` | `price`, `sma_200`, `deviation_pct` | yfinance `^NDX` |
| Liquidity | `track_liquidity` | `rrp_billions`, `tga_billions` | FRED (`RRPONTSYD`, `WTREGEN`) |
| Sentiment | `track_sentiment` | `fear_greed_score`, `rating` | CNN Fear & Greed API |
| IPO Heat | `track_ipo_heat` | `ipo_etf_price`, `vol_heat_ratio` | yfinance `IPO` ETF |
| Valuation | `track_valuation` | `spy_pe`, `qqq_pe`, `spy_pe_deviation_pct` | yfinance SPY/QQQ `.info` |
| Volatility | `track_volatility` | `vix_level`, `vix_sma_20` | yfinance `^VIX` |

All tables use `date` as primary key (upsert on conflict).

## Important Behavioral Notes

### Collectors that can backfill historical dates
- `run_deviation`, `run_ipo_heat`, `run_volatility`, `run_liquidity` — accept `run_date: datetime` param; when provided, they fetch yfinance/FRED data bounded to that date via explicit `start`/`end` params.
- **Critical**: always use `start`/`end` when fetching historical data. Using `period=` or omitting `end` causes `iloc[-1]` to return today's data written to a historical date row — this is the bug that corrupted 2025-03-17 through 2025-03-30 data.

### Collectors that cannot backfill
- `run_sentiment` — CNN API only returns current data; skips any `date_value < today`
- `run_valuation` — yfinance `.info` only returns current PE ratios; skips any `date_value < today`

### Worker startup behavior
On container start, `worker.py`:
1. Calls `init_tables()` to ensure schema exists
2. Runs `backfill_missing_data()` — fills missing weekdays in past `BACKFILL_DAYS` (default: 7)
3. Checks if today has data; if not, runs collection immediately
4. Then starts the blocking scheduler for the daily 13:00 PT cron job

`BACKFILL_DAYS` env var controls lookback window (default 7, not 30 — was reduced to avoid backfilling too far with stale data).

Missing date detection uses `track_deviation` as the reference table.

## DB Connection

```
postgresql://bubble:bubble@localhost:5432/bubble_tracker
```

From inside Docker network: `postgresql://bubble:bubble@db:5432/bubble_tracker`

## Common Commands

```bash
# Start everything
docker compose up -d

# Rebuild api after Python/static changes (static files also hot-reload via volume mount)
docker compose up -d --build api

# Rebuild worker after Python changes
docker compose up -d --build worker

# View worker logs (shows collection status)
docker compose logs -f worker

# Connect to DB
docker compose exec db psql -U bubble -d bubble_tracker

# Run SQL file against DB
docker compose exec db psql -U bubble -d bubble_tracker < some_file.sql
```

## Static File Hot-Reload

`app/static/` is volume-mounted into the `api` container:
```yaml
volumes:
  - ./app/static:/app/app/static
```
Edits to `index.html` are live immediately — just hard-refresh the browser (`Cmd+Shift+R`). No rebuild needed for frontend changes.

## Known History / Past Issues

- **2025-03-17 to 2025-03-30 data corruption**: The 30-day backfill on startup ran the broken collectors (which used `iloc[-1]` without an `end` date), writing 3/31's prices into all missing historical date rows. Fixed by: (1) bounding all historical fetches with explicit `end` dates, (2) reducing `BACKFILL_DAYS` default to 7, (3) running `cleanup_bad_backfill.sql` to delete the corrupted rows.
- `cleanup_bad_backfill.sql` exists in the repo root as a reference for that one-time cleanup.
