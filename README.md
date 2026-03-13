# Bubble Tracker (Container First)

This project runs a local bubble-tracker stack with:

- `api` (FastAPI) on `http://localhost:8080`
- `db` (PostgreSQL)
- `worker` (daily async data collection at 1:00 PM America/Los_Angeles)

The worker also does a startup catch-up run if today's data is missing.

## 1. Install Docker on macOS

1. Install Docker Desktop for Mac: [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
2. Open Docker Desktop and wait until it shows "Engine running".
3. Verify from terminal:

```bash
docker --version
docker compose version
```

## 2. Start the stack

From this repo root:

```bash
docker compose up -d --build
```

Check status:

```bash
docker compose ps
docker compose logs -f worker
```

Open the website:

- [http://localhost:8080](http://localhost:8080)

Health check:

```bash
curl http://localhost:8080/health
```

## 3. Daily schedule

- The worker runs every day at **13:00 America/Los_Angeles**.
- Config lives in `docker-compose.yml` env vars:
  - `SCHEDULE_TZ`
  - `SCHEDULE_HOUR`
  - `SCHEDULE_MINUTE`

## 4. Manual data jobs

Run one-time collection inside the worker container:

```bash
docker compose exec worker python collector.py
```

Run one-time backfill:

```bash
docker compose exec worker python backfill.py
```

## 5. Stop / restart

```bash
docker compose stop
docker compose start
```

Reset DB volume:

```bash
docker compose down -v
```

## API quick reference

- `GET /health`
- `GET /latest`
- `GET /metrics`
- `GET /metrics/deviation` - NASDAQ-100 deviation from 200-day SMA
- `GET /metrics/liquidity` - Fed liquidity (RRP + TGA)
- `GET /metrics/sentiment` - CNN Fear & Greed Index
- `GET /metrics/ipo_heat` - IPO ETF activity
- `GET /metrics/valuation` - S&P 500 and NASDAQ PE ratios
- `GET /metrics/volatility` - VIX fear index
