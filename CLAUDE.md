# Stock Bubble Tracker — Project Context for Claude

## What This Is

A containerized market monitoring system ("Market Mood Tracker") that tracks 12 financial indicators daily and blends them into a 0-100% **Euphoria-Panic Index** of crowd sentiment/positioning. Deliberately framed as a sentiment gauge, NOT a fair-value/bubble verdict (real earnings can justify optimism; the gauge cannot tell). Runs in Docker Compose. Dashboard served at `http://localhost:8080`.

## Architecture

3 Docker services defined in `docker-compose.yml`:

| Service | Container | Purpose |
|---|---|---|
| `db` | `bubble_db` | PostgreSQL 16, stores all time-series data |
| `api` | `bubble_api` | FastAPI on port 8080, serves REST API + dashboard |
| `worker` | `bubble_worker` | APScheduler, runs daily collection at 13:00 PT |

All Python code lives under `app/`. Root-level `collector.py`, `backfill.py`, `dashboard.py` are old/unused files — the active code is under `app/`.

## Key Files

- `app/collector.py` — data collection functions for all 12 metrics
- `app/composite.py` — composite bubble score: signal weights, calm/extreme anchors, historical episode data
- `app/worker.py` — scheduler + startup backfill logic
- `app/api.py` — FastAPI endpoints (`/`, `/health`, `/latest`, `/metrics/{metric}`, `/composite`, `/composite/history`)
- `app/db.py` — DB connection, table init, `fetch_all` / `fetch_one` helpers
- `app/static/index.html` — dashboard UI (Chart.js, vanilla JS, auto-refreshes every 30s). Two-page SPA via hash routing: `#euphoria` (default — hero score, episode lines, 12 cards in 4 sections) and `#disconnect` (disconnect score, visual 2x2 quadrant with positioned dot, 5 cards); page tabs under the header, cross-links in each hero
- `docker-compose.yml` — service definitions

## 12 Tracked Metrics

| Metric | Table | Key Fields | Data Source |
|---|---|---|---|
| Deviation | `track_deviation` | `price`, `sma_200`, `deviation_pct` | yfinance `^NDX` |
| Liquidity | `track_liquidity` | `rrp_billions`, `tga_billions` | FRED (`RRPONTSYD`, `WTREGEN`) |
| Sentiment | `track_sentiment` | `fear_greed_score`, `rating` | CNN Fear & Greed API |
| IPO Heat | `track_ipo_heat` | `ipo_etf_price`, `vol_heat_ratio`, `ipo_rel_dev_pct` | yfinance `IPO` ETF (+SPY for relative strength) |
| Valuation | `track_valuation` | `spy_pe`, `qqq_pe`, `spy_pe_deviation_pct` | yfinance SPY/QQQ `.info` |
| Volatility | `track_volatility` | `vix_level`, `vix_sma_20` | yfinance `^VIX` |
| Credit | `track_credit` | `hy_spread_pct` | FRED (`BAMLH0A0HYM2`, HY OAS) |
| Concentration | `track_concentration` | `smh_spy_ratio`, `smh_spy_dev_pct`, `qqq_qqqe_ratio`, `qqq_qqqe_dev_pct` | yfinance SMH/SPY/QQQ/QQQE |
| Term Structure | `track_term_structure` | `vix_1m`, `vix_3m`, `vix_ratio` | yfinance `^VIX`, `^VIX3M` |
| Margin Debt | `track_margin_debt` | `debit_balances_billions`, `yoy_growth_pct` | FINRA margin statistics xlsx (monthly) |
| Put/Call | `track_put_call` | `total_pc_ratio`, `equity_pc_ratio` | CBOE daily market statistics JSON |
| Hot Sector | `track_hot_sector` | `sector`, `dev_pct` | yfinance SPDR sector ETFs vs SPY; defensives XLP/XLU/XLV excluded (they lead in selloffs = fear, not euphoria) |
| Fundamentals | `track_fundamentals` | `erp_pct`, `multiple_expansion_pct`, `cape`, `cape_percentile`, `margins_pct`, `credit_gap_pct` | FRED (DGS10/CP/GDP/CRDQUSAPABIS), multpl.com CAPE scrape, yfinance SPY `.info`/`^GSPC`/`^TNX` |
| Metric history | `track_metric_history` | `metric`, `date`, `value` | native-cadence (monthly/quarterly) history for the 5 slow fundamentals, full-upserted each `run_fundamentals` run; charts read it via `/metric_history/{metric}` |

All tables use `date` as primary key (upsert on conflict).

Dashboard warning thresholds (red dashed = overheated/complacency, blue dashed = panic/stress): credit `<3%` / `>5%`, hottest-sector dev `>15%`, QQQ/QQQE dev `>5%`, VIX ratio `<0.85` / `>1.0`, margin debt YoY `>40%`, equity put/call `<0.5` / `>1.0`.

## Fundamental Disconnect Index (second axis)

`DISCONNECT_SIGNALS` in composite.py — measures whether price is backed by earnings (the "real bubble" axis; the euphoria index alone cannot distinguish justified optimism from mania). Weights: ERP 30 (earnings yield − 10Y, calm +3 → extreme −3; 2000 hit −3%), multiple expansion 25 (12m change in Shiller trailing P/E, 0 → 30), CAPE 30y-percentile 20 (identity 0-100), profit margins CP/GDP 15 (9 → 13), credit-to-GDP gap 10 (0 → +10pp, simplified BIS gap = ratio minus 10y average). Exposed via `/composite` response key `disconnect`, including a 2x2 `quadrant` string (euphoria >= 60 x disconnect >= 60 -> "Bubble conditions" / "Hot but earning it" / "Expensive but unloved" / "Healthy"). Hero card shows it next to the euphoria score. Dashboard section "Fundamental Disconnect" holds the 5 cards (chips show weight within the disconnect index, not the euphoria index).

## Composite Score (Euphoria-Panic Index)

`app/composite.py` blends all 12 signals into a 0–100% score (displayed as "Euphoria-Panic Index"). Each signal ramps linearly from a `calm` anchor (sub-score 0) to an `extreme` anchor (sub-score 100), clamped; inverted scales just have calm > extreme. Composite = weight-averaged sub-scores; missing signals are dropped and weights renormalized.

Dashboard groups cards into 4 sections (= signal weights, ranked within section):
1. **Price & Valuation 37%** — deviation 12, P/E 12, hottest sector 8, QQQ/QQQE 5 (fragility gauge, not a mania timer — deliberately the smallest weight in its section)
2. **Speculation & Sentiment 38%** — margin debt 16, put/call 10, fear&greed 7, IPO appetite 5 (positioning signals upweighted: they best separated the 2000/2007/2021 euphoria peaks from calm periods)
3. **Credit & Liquidity 15%** — HY spread 12, RRP 3
4. **Volatility & Complacency 10%** — VIX term structure 6, VIX 4

IPO is scored on `ipo_rel_dev_pct` (IPO ETF vs SPY, 200-day deviation — Feb 2021 mania +28.5%, Oct 2022 bust −14.6%), NOT the legacy `vol_heat_ratio`, which oscillates around 1.0 by construction and never discriminates. Both are backwards-looking (the ETF only holds already-listed names); a forward-looking pipeline signal would be SEC EDGAR S-1 filing counts (free full-text search API) — not yet implemented.

Historical episodes in `EPISODES` (scored through the same pipeline; values marked actual/proxy/estimated): Dot-com 2000-03-10 → ~80%, Pre-GFC euphoria 2007-06-01 → ~64%, Post-COVID froth 2021-02-12 → ~77%. The GFC reference is deliberately June 2007 (record margin growth, record-low HY spread, VIX ~12), NOT the October 2007 market top: by October the gauge read ~50% because speculation/credit had already rolled over, so that reading offered no warning and was removed as a reference line — this gauge peaks at the speculative-conditions peak, months before the price top. Hero card on dashboard shows the live score with these as dashed reference lines (color-coded, identified in the legend below — no on-chart text labels). The 2008 GFC panic trough was intentionally removed: a crash bottom scores ~0% (every signal pegged to the fear extreme), which is correct but confusing as a "bubble" reference. The "Hottest Sector" signal (max 200-day deviation of any SPDR sector ETF vs SPY) deliberately replaces a hardcoded semis signal so the gauge isn't biased toward the current boom's narrative; the sector ETFs exist since Dec 1998, covering all episodes with actual data. Per-metric card titles carry an info icon (ⓘ) whose hover tooltip holds the plain-English description. `/composite/history` recomputes the score per trading day from stored rows (margin debt forward-filled up to 62 days, daily signals 7 days, ≥6 signals required).

**Panic zone** (`PANIC_ZONE` in composite.py, blue dashed line at 22% on the hero chart): panic lows scored through the same pipeline — GFC 2008-11-20 → 8.6%, Christmas Eve 2018-12-24 → 13.7%, COVID 2020-03-23 → 20.7%, bear low 2022-10-13 → 21.1%. All were followed by +36–86% NDX in 1y; readings <22% have historically been strong long-term entry zones (coincident, not bottom-tick: 2008 reading came 4 months before the final low). Status bands: <15 Capitulation, <30 Fearful, <50 Neutral, <65 Heating up, <80 Euphoric, ≥80 Manic extreme.

## Important Behavioral Notes

### Collectors that can backfill historical dates
- `run_deviation`, `run_ipo_heat`, `run_volatility`, `run_liquidity`, `run_credit`, `run_concentration`, `run_hot_sector`, `run_term_structure`, `run_put_call` — accept `run_date: datetime` param; when provided, they fetch yfinance/FRED data bounded to that date via explicit `start`/`end` params (put/call uses CBOE's per-date URL: `cdn.cboe.com/data/us/options/market_statistics/daily/{date}_daily_options`, 403 on non-trading days).
- **Critical**: always use `start`/`end` when fetching historical data. Using `period=` or omitting `end` causes `iloc[-1]` to return today's data written to a historical date row — this is the bug that corrupted 2025-03-17 through 2025-03-30 data.

### Collectors that cannot backfill
- `run_sentiment` — CNN API only returns current data; skips any `date_value < today`
- `run_valuation` — yfinance `.info` only returns current PE ratios; skips any `date_value < today`
- `run_margin_debt` — skips backfill runs entirely; each live run downloads the FINRA xlsx and upserts the full monthly history (dates stored as month-end), so it never needs per-date backfill
- `run_fundamentals` — live-only (mixed monthly/quarterly sources with publication lags); each of its 5 components fails independently; 10Y yield falls back to yfinance `^TNX` when FRED `DGS10` times out (the container hits intermittent FRED read-timeouts — same issue that breaks `run_liquidity`). **Do NOT use Shiller's ie_data.xls — it stopped updating in 2023** (earnings end 2023-06); CAPE and S&P PE come from multpl.com monthly tables (current, back to 1871, regex scrape in `_multpl_series`). **Multiple expansion uses S&P price (`^GSPC`) ÷ corporate profits (FRED `CP`, trailing-4Q-avg to damp NIPA noise), 12-month change** — ~1-quarter lag, far fresher than multpl's ~9-month-lagged GAAP S&P PE (do NOT use the `s-p-500-pe-ratio` multpl series for this; it's stale). CAPE still comes from multpl's `shiller-pe` (current). `run_fundamentals` also upserts native-cadence history (from 2010) for cape/cape_percentile (monthly) and multiple_expansion/margins/credit_gap (quarterly) into `track_metric_history`. The 5 disconnect cards chart that history as **stepped lines** (Chart.js `stepped:'before'`) over a fixed window (CAPE 72 months, quarterly metrics 28) — ERP stays a daily line. Margin debt is also stepped. This replaced flat single-value daily charts that looked broken.

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
