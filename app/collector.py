from __future__ import annotations

import io
import logging
import math
import re
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pandas_datareader.data as web
import requests
import yfinance as yf

from app.db import get_conn, init_tables

LOGGER = logging.getLogger(__name__)


def _latest_market_row(df, target_date):
    if df.empty:
        return None, None

    eligible = df[df.index.date <= target_date]
    if eligible.empty:
        return None, None

    source_date = eligible.index[-1].date()
    return source_date, eligible.iloc[-1]


def _drop_missing_market_values(df, metric_name: str, required_columns: list[str]):
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        LOGGER.warning("Missing %s data columns for %s", ", ".join(missing_columns), metric_name)
        return df.iloc[0:0]

    cleaned = df.dropna(subset=required_columns)
    dropped_count = len(df) - len(cleaned)
    if dropped_count:
        LOGGER.info("Ignoring %s %s rows with missing market values", dropped_count, metric_name)
    return cleaned


def _has_finite_values(values) -> bool:
    return all(math.isfinite(float(value)) for value in values)


def _is_missing_target_date(source_date, target_date, metric_name: str) -> bool:
    if source_date == target_date:
        return False

    LOGGER.info(
        "Skipping %s for %s; latest source observation is %s",
        metric_name,
        target_date,
        source_date,
    )
    return True


def _delete_stale_rows(cur, table_name: str, source_date, target_date) -> None:
    if source_date >= target_date:
        return

    cur.execute(
        f"DELETE FROM {table_name} WHERE date > %s AND date <= %s",
        (source_date, target_date),
    )


def run_deviation(run_date: datetime | None = None) -> None:
    target_date = (run_date or datetime.now()).date()

    if run_date:
        # Fetch enough history to compute 200-day SMA ending on date_value
        df = yf.Ticker("^NDX").history(
            start=target_date - timedelta(days=300),
            end=target_date + timedelta(days=1),
        )
    else:
        df = yf.Ticker("^NDX").history(period="1y")

    if df.empty:
        LOGGER.warning("No NDX data available for %s", target_date)
        return

    df = _drop_missing_market_values(df, "deviation", ["Close"])
    if df.empty:
        LOGGER.warning("No populated NDX data available for %s", target_date)
        return

    date_value, latest = _latest_market_row(df, target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "deviation")):
        return

    df = df[df.index.date <= date_value]
    curr = float(latest["Close"])
    sma = float(df["Close"].rolling(window=200).mean().iloc[-1])
    dev = ((curr - sma) / sma) * 100
    if not _has_finite_values([curr, sma, dev]):
        LOGGER.warning("Skipping deviation for %s due to incomplete NDX values", date_value)
        return

    query = """
        INSERT INTO track_deviation (date, price, sma_200, deviation_pct)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET price = EXCLUDED.price,
            sma_200 = EXCLUDED.sma_200,
            deviation_pct = EXCLUDED.deviation_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, curr, sma, dev))
            if not run_date:
                _delete_stale_rows(cur, "track_deviation", date_value, target_date)
        conn.commit()


def run_liquidity(run_date: datetime | None = None) -> None:
    target_date = (run_date or datetime.now()).date()

    try:
        if run_date:
            # Bound the end date so we only get data up to the target date
            start = target_date - timedelta(days=14)
            end = target_date
            rrp_data = web.DataReader("RRPONTSYD", "fred", start, end)
            tga_data = web.DataReader("WTREGEN", "fred", start, end)
        else:
            start = target_date - timedelta(days=14)
            rrp_data = web.DataReader("RRPONTSYD", "fred", start, target_date)
            tga_data = web.DataReader("WTREGEN", "fred", start - timedelta(days=14), target_date)

        if len(rrp_data) == 0 or len(tga_data) == 0:
            LOGGER.warning("No FRED data available for %s (likely weekend/holiday)", target_date)
            return

        rrp_data = rrp_data.dropna()
        tga_data = tga_data.dropna()
        if len(rrp_data) == 0 or len(tga_data) == 0:
            LOGGER.warning("No populated FRED data available for %s", target_date)
            return

        date_value = rrp_data.index[-1].date()
        if run_date and _is_missing_target_date(date_value, target_date, "liquidity"):
            return

        tga_data = tga_data[tga_data.index.date <= date_value]
        if len(tga_data) == 0:
            LOGGER.warning("No TGA data available on or before %s", date_value)
            return

        rrp = float(rrp_data.iloc[-1].item())
        tga = float(tga_data.iloc[-1].item())
    except (IndexError, KeyError):
        LOGGER.warning("FRED data not available for %s (likely weekend/holiday)", target_date)
        return

    query = """
        INSERT INTO track_liquidity (date, rrp_billions, tga_billions)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET rrp_billions = EXCLUDED.rrp_billions,
            tga_billions = EXCLUDED.tga_billions
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, rrp, tga))
            if not run_date:
                _delete_stale_rows(cur, "track_liquidity", date_value, target_date)
        conn.commit()


def run_sentiment(run_date: datetime | None = None) -> None:
    date_value = (run_date or datetime.now()).date()
    today = datetime.now().date()
    
    # CNN Fear & Greed API only provides current data, not historical
    # Skip if trying to backfill a past date
    if date_value < today:
        LOGGER.info("Skipping sentiment for %s (Fear & Greed API only provides current data)", date_value)
        return
    
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://edition.cnn.com/",
    }
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()

    data = response.json()
    score = float(data["fear_and_greed"]["score"])
    rating = str(data["fear_and_greed"]["rating"])

    query = """
        INSERT INTO track_sentiment (date, fear_greed_score, rating)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET fear_greed_score = EXCLUDED.fear_greed_score,
            rating = EXCLUDED.rating
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, score, rating))
        conn.commit()


def run_ipo_heat(run_date: datetime | None = None) -> None:
    """Track appetite for recent IPOs: IPO ETF vs SPY relative strength + volume churn"""
    target_date = (run_date or datetime.now()).date()

    hist = _close_history("IPO", target_date, run_date, 460, "2y")
    if hist.empty:
        LOGGER.warning("No IPO data available for %s", target_date)
        return

    hist = _drop_missing_market_values(hist, "ipo_heat", ["Close", "Volume"])
    if hist.empty:
        LOGGER.warning("No populated IPO data available for %s", target_date)
        return

    date_value, latest = _latest_market_row(hist, target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "ipo_heat")):
        return

    hist = hist[hist.index.date <= date_value]
    curr = float(latest["Close"])
    vol_ratio = float(latest["Volume"] / hist["Volume"].iloc[-5:].mean())

    # Relative strength of recent IPOs vs the market: the market paying up for
    # new issues is the euphoria read; the volume ratio is just churn noise.
    ipo_rel_dev = None
    spy_hist = _close_history("SPY", target_date, run_date, 460, "2y")
    if not spy_hist.empty:
        spy_hist = _drop_missing_market_values(spy_hist, "ipo_heat", ["Close"])
    if not spy_hist.empty:
        ratio = (_naive_daily_close(hist) / _naive_daily_close(spy_hist)).dropna()
        ratio = ratio[ratio.index.date <= date_value]
        if len(ratio) >= 200:
            sma = float(ratio.rolling(window=200).mean().iloc[-1])
            dev = ((float(ratio.iloc[-1]) - sma) / sma) * 100
            if _has_finite_values([dev]):
                ipo_rel_dev = dev

    if not _has_finite_values([curr, vol_ratio]):
        LOGGER.warning("Skipping IPO heat for %s due to incomplete market values", date_value)
        return

    query = """
        INSERT INTO track_ipo_heat (date, ipo_etf_price, vol_heat_ratio, ipo_rel_dev_pct)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET ipo_etf_price = EXCLUDED.ipo_etf_price,
            vol_heat_ratio = EXCLUDED.vol_heat_ratio,
            ipo_rel_dev_pct = EXCLUDED.ipo_rel_dev_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, curr, vol_ratio, ipo_rel_dev))
            if not run_date:
                _delete_stale_rows(cur, "track_ipo_heat", date_value, target_date)
        conn.commit()


def run_valuation(run_date: datetime | None = None) -> None:
    """Track S&P 500 and NASDAQ PE ratios"""
    date_value = (run_date or datetime.now()).date()
    today = datetime.now().date()
    
    # yfinance .info only provides current PE ratios, not historical
    # Skip if trying to backfill a past date
    if date_value < today:
        LOGGER.info("Skipping valuation for %s (yfinance PE ratios only provide current data)", date_value)
        return
    
    spy = yf.Ticker("SPY")
    spy_info = spy.info
    spy_pe = spy_info.get("trailingPE", None)

    qqq = yf.Ticker("QQQ")
    qqq_info = qqq.info
    qqq_pe = qqq_info.get("trailingPE", None)

    # Historical average PE for S&P 500 is ~16
    spy_pe_deviation = ((spy_pe - 16) / 16) * 100 if spy_pe else None

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
            cur.execute(query, (date_value, spy_pe, qqq_pe, spy_pe_deviation))
        conn.commit()


def run_volatility(run_date: datetime | None = None) -> None:
    """Track VIX (fear index) and its moving average"""
    target_date = (run_date or datetime.now()).date()

    if run_date:
        vix = yf.Ticker("^VIX").history(
            start=target_date - timedelta(days=35),
            end=target_date + timedelta(days=1),
        )
    else:
        vix = yf.Ticker("^VIX").history(period="1mo")

    if vix.empty:
        LOGGER.warning("No VIX data available for %s", target_date)
        return

    vix = _drop_missing_market_values(vix, "volatility", ["Close"])
    if vix.empty:
        LOGGER.warning("No populated VIX data available for %s", target_date)
        return

    date_value, latest = _latest_market_row(vix, target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "volatility")):
        return

    vix = vix[vix.index.date <= date_value]
    current_vix = float(latest["Close"])
    vix_sma_20 = float(vix["Close"].rolling(window=20).mean().iloc[-1])
    if not _has_finite_values([current_vix, vix_sma_20]):
        LOGGER.warning("Skipping volatility for %s due to incomplete VIX values", date_value)
        return

    query = """
        INSERT INTO track_volatility (date, vix_level, vix_sma_20)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET vix_level = EXCLUDED.vix_level,
            vix_sma_20 = EXCLUDED.vix_sma_20
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, current_vix, vix_sma_20))
            if not run_date:
                _delete_stale_rows(cur, "track_volatility", date_value, target_date)
        conn.commit()


def _close_history(symbol: str, target_date, run_date: datetime | None, lookback_days: int, period: str):
    if run_date:
        return yf.Ticker(symbol).history(
            start=target_date - timedelta(days=lookback_days),
            end=target_date + timedelta(days=1),
        )
    return yf.Ticker(symbol).history(period=period)


def _naive_daily_close(hist):
    """Strip timezone from a history's index so series from different exchanges align by date.

    yfinance indexes each ticker in its exchange's timezone (e.g. ^VIX in
    America/Chicago, ^VIX3M in America/New_York), so tz-aware midnights never match.
    """
    close = hist["Close"].copy()
    close.index = close.index.tz_localize(None).normalize()
    return close


def run_credit(run_date: datetime | None = None) -> None:
    """Track ICE BofA US High Yield option-adjusted spread (credit complacency)"""
    target_date = (run_date or datetime.now()).date()

    try:
        start = target_date - timedelta(days=14)
        spread_data = web.DataReader("BAMLH0A0HYM2", "fred", start, target_date)

        if len(spread_data) == 0:
            LOGGER.warning("No FRED HY spread data available for %s (likely weekend/holiday)", target_date)
            return

        spread_data = spread_data.dropna()
        if len(spread_data) == 0:
            LOGGER.warning("No populated FRED HY spread data available for %s", target_date)
            return

        date_value = spread_data.index[-1].date()
        if run_date and _is_missing_target_date(date_value, target_date, "credit"):
            return

        hy_spread = float(spread_data.iloc[-1].item())
    except (IndexError, KeyError):
        LOGGER.warning("FRED HY spread data not available for %s (likely weekend/holiday)", target_date)
        return

    if not _has_finite_values([hy_spread]):
        LOGGER.warning("Skipping credit for %s due to incomplete spread values", date_value)
        return

    query = """
        INSERT INTO track_credit (date, hy_spread_pct)
        VALUES (%s, %s)
        ON CONFLICT (date) DO UPDATE
        SET hy_spread_pct = EXCLUDED.hy_spread_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, hy_spread))
            if not run_date:
                _delete_stale_rows(cur, "track_credit", date_value, target_date)
        conn.commit()


def run_concentration(run_date: datetime | None = None) -> None:
    """Track market concentration: SMH/SPY (semis vs market) and QQQ/QQQE (cap vs equal weight)"""
    target_date = (run_date or datetime.now()).date()

    closes = {}
    for symbol in ("SMH", "SPY", "QQQ", "QQQE"):
        # 365 calendar days gives ~250 trading days, enough for the 200-day SMA
        hist = _close_history(symbol, target_date, run_date, 365, "1y")
        if hist.empty:
            LOGGER.warning("No %s data available for %s", symbol, target_date)
            return
        hist = _drop_missing_market_values(hist, "concentration", ["Close"])
        if hist.empty:
            LOGGER.warning("No populated %s data available for %s", symbol, target_date)
            return
        closes[symbol] = _naive_daily_close(hist)

    df = pd.DataFrame({
        "smh_spy": closes["SMH"] / closes["SPY"],
        "qqq_qqqe": closes["QQQ"] / closes["QQQE"],
    }).dropna()
    if df.empty:
        LOGGER.warning("No overlapping concentration data available for %s", target_date)
        return

    date_value, latest = _latest_market_row(df, target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "concentration")):
        return

    df = df[df.index.date <= date_value]
    smh_spy = float(latest["smh_spy"])
    qqq_qqqe = float(latest["qqq_qqqe"])
    smh_spy_sma = float(df["smh_spy"].rolling(window=200).mean().iloc[-1])
    qqq_qqqe_sma = float(df["qqq_qqqe"].rolling(window=200).mean().iloc[-1])
    smh_spy_dev = ((smh_spy - smh_spy_sma) / smh_spy_sma) * 100
    qqq_qqqe_dev = ((qqq_qqqe - qqq_qqqe_sma) / qqq_qqqe_sma) * 100
    if not _has_finite_values([smh_spy, smh_spy_dev, qqq_qqqe, qqq_qqqe_dev]):
        LOGGER.warning("Skipping concentration for %s due to incomplete market values", date_value)
        return

    query = """
        INSERT INTO track_concentration (date, smh_spy_ratio, smh_spy_dev_pct, qqq_qqqe_ratio, qqq_qqqe_dev_pct)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET smh_spy_ratio = EXCLUDED.smh_spy_ratio,
            smh_spy_dev_pct = EXCLUDED.smh_spy_dev_pct,
            qqq_qqqe_ratio = EXCLUDED.qqq_qqqe_ratio,
            qqq_qqqe_dev_pct = EXCLUDED.qqq_qqqe_dev_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, smh_spy, smh_spy_dev, qqq_qqqe, qqq_qqqe_dev))
            if not run_date:
                _delete_stale_rows(cur, "track_concentration", date_value, target_date)
        conn.commit()


def run_term_structure(run_date: datetime | None = None) -> None:
    """Track VIX term structure: VIX (1M) vs VIX3M; ratio > 1 means inverted (panic)"""
    target_date = (run_date or datetime.now()).date()

    vix = _close_history("^VIX", target_date, run_date, 14, "5d")
    vix3m = _close_history("^VIX3M", target_date, run_date, 14, "5d")
    if vix.empty or vix3m.empty:
        LOGGER.warning("No VIX term structure data available for %s", target_date)
        return

    vix = _drop_missing_market_values(vix, "term_structure", ["Close"])
    vix3m = _drop_missing_market_values(vix3m, "term_structure", ["Close"])
    if vix.empty or vix3m.empty:
        LOGGER.warning("No populated VIX term structure data available for %s", target_date)
        return

    df = pd.DataFrame({"vix_1m": _naive_daily_close(vix), "vix_3m": _naive_daily_close(vix3m)}).dropna()
    if df.empty:
        LOGGER.warning("No overlapping VIX/VIX3M data available for %s", target_date)
        return

    date_value, latest = _latest_market_row(df, target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "term_structure")):
        return

    vix_1m = float(latest["vix_1m"])
    vix_3m = float(latest["vix_3m"])
    if not _has_finite_values([vix_1m, vix_3m]) or vix_3m == 0:
        LOGGER.warning("Skipping term structure for %s due to incomplete VIX values", date_value)
        return
    vix_ratio = vix_1m / vix_3m

    query = """
        INSERT INTO track_term_structure (date, vix_1m, vix_3m, vix_ratio)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET vix_1m = EXCLUDED.vix_1m,
            vix_3m = EXCLUDED.vix_3m,
            vix_ratio = EXCLUDED.vix_ratio
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, vix_1m, vix_3m, vix_ratio))
            if not run_date:
                _delete_stale_rows(cur, "track_term_structure", date_value, target_date)
        conn.commit()


MARGIN_STATS_URL = "https://www.finra.org/sites/default/files/2021-03/margin-statistics.xlsx"


def run_margin_debt(run_date: datetime | None = None) -> None:
    """Track FINRA monthly margin debt; the full monthly history is upserted on each live run"""
    # FINRA publishes one Excel file with the complete history (~3 week lag),
    # so historical backfill is unnecessary — every live run refreshes all months.
    if run_date:
        LOGGER.info("Skipping margin debt for %s (full history refreshed on live runs)", run_date.date())
        return

    # FINRA's CDN rejects spoofed browser user agents from non-browser clients; the
    # default python-requests UA is allowed.
    response = requests.get(MARGIN_STATS_URL, timeout=30)
    response.raise_for_status()

    df = pd.read_excel(io.BytesIO(response.content))
    df = df.rename(columns={df.columns[0]: "month", df.columns[1]: "debit_millions"})
    df["month"] = df["month"].astype(str).str.strip()
    df = df[df["month"].str.match(r"^\d{4}-\d{2}$")]
    df["debit_millions"] = pd.to_numeric(df["debit_millions"], errors="coerce")
    df = df.dropna(subset=["debit_millions"]).sort_values("month").reset_index(drop=True)
    if df.empty:
        LOGGER.warning("No parseable FINRA margin statistics rows")
        return

    df["debit_billions"] = df["debit_millions"] / 1000.0
    df["yoy_pct"] = df["debit_billions"].pct_change(12) * 100
    # Store under the month-end date so monthly points sort naturally with daily series
    df["date"] = [p.end_time.date() for p in pd.PeriodIndex(df["month"], freq="M")]

    rows = [
        (
            row.date,
            float(row.debit_billions),
            float(row.yoy_pct) if math.isfinite(row.yoy_pct) else None,
        )
        for row in df.itertuples()
    ]

    query = """
        INSERT INTO track_margin_debt (date, debit_balances_billions, yoy_growth_pct)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET debit_balances_billions = EXCLUDED.debit_balances_billions,
            yoy_growth_pct = EXCLUDED.yoy_growth_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, rows)
        conn.commit()
    LOGGER.info("Upserted %d FINRA margin debt months (latest: %s)", len(rows), rows[-1][0])


SECTOR_ETFS = {
    "XLK": "Technology",
    "XLF": "Financials",
    "XLE": "Energy",
    "XLV": "Health Care",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLI": "Industrials",
    "XLB": "Materials",
    "XLU": "Utilities",
    "XLRE": "Real Estate",
    "XLC": "Communication Services",
}

# Defensive sectors outperform during selloffs (they fall less), so leadership
# there signals fear, not euphoria — they'd inflate the gauge at panic bottoms
# (XLP led +32% at the 2008 low). Excluded from the hottest-sector scan.
DEFENSIVE_SECTORS = {"XLP", "XLU", "XLV"}


def run_hot_sector(run_date: datetime | None = None) -> None:
    """Track the hottest cyclical/growth S&P sector vs the whole market: max 200-day deviation of sector/SPY"""
    target_date = (run_date or datetime.now()).date()

    spy_hist = _close_history("SPY", target_date, run_date, 460, "2y")
    if spy_hist.empty:
        LOGGER.warning("No SPY data available for %s", target_date)
        return
    spy_hist = _drop_missing_market_values(spy_hist, "hot_sector", ["Close"])
    if spy_hist.empty:
        LOGGER.warning("No populated SPY data available for %s", target_date)
        return
    spy = _naive_daily_close(spy_hist)

    date_value, _ = _latest_market_row(pd.DataFrame({"spy": spy}), target_date)
    if not date_value or (run_date and _is_missing_target_date(date_value, target_date, "hot_sector")):
        return

    best_symbol = None
    best_dev = None
    for symbol in SECTOR_ETFS:
        if symbol in DEFENSIVE_SECTORS:
            continue
        hist = _close_history(symbol, target_date, run_date, 460, "2y")
        if hist.empty:
            continue
        hist = _drop_missing_market_values(hist, "hot_sector", ["Close"])
        if hist.empty:
            continue
        ratio = (_naive_daily_close(hist) / spy).dropna()
        ratio = ratio[ratio.index.date <= date_value]
        if len(ratio) < 200 or ratio.index[-1].date() != date_value:
            continue
        sma = float(ratio.rolling(window=200).mean().iloc[-1])
        dev = ((float(ratio.iloc[-1]) - sma) / sma) * 100
        if not _has_finite_values([dev]):
            continue
        if best_dev is None or dev > best_dev:
            best_symbol, best_dev = symbol, dev

    if best_symbol is None:
        LOGGER.warning("No sector with enough history for hot sector on %s", date_value)
        return

    query = """
        INSERT INTO track_hot_sector (date, sector, dev_pct)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET sector = EXCLUDED.sector,
            dev_pct = EXCLUDED.dev_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, best_symbol, best_dev))
            if not run_date:
                _delete_stale_rows(cur, "track_hot_sector", date_value, target_date)
        conn.commit()


PUT_CALL_URL = "https://cdn.cboe.com/data/us/options/market_statistics/daily/{date}_daily_options"


def _fetch_put_call_ratios(date_value) -> tuple[float, float] | None:
    response = requests.get(PUT_CALL_URL.format(date=date_value.isoformat()), timeout=20)
    if response.status_code != 200:
        return None
    try:
        ratios = {entry["name"]: entry["value"] for entry in response.json().get("ratios", [])}
        total = float(ratios["TOTAL PUT/CALL RATIO"])
        equity = float(ratios["EQUITY PUT/CALL RATIO"])
    except (ValueError, KeyError, TypeError):
        LOGGER.warning("Unexpected CBOE put/call payload for %s", date_value)
        return None
    if total <= 0 or equity <= 0:
        return None
    return total, equity


def run_put_call(run_date: datetime | None = None) -> None:
    """Track CBOE total and equity put/call ratios (speculative positioning)"""
    target_date = (run_date or datetime.now()).date()

    if run_date:
        result = _fetch_put_call_ratios(target_date)
        if result is None:
            LOGGER.info("No CBOE put/call data for %s (likely weekend/holiday)", target_date)
            return
        date_value = target_date
    else:
        # Today's file may not be published yet; walk back to the latest trading day
        result = None
        date_value = None
        for offset in range(7):
            candidate = target_date - timedelta(days=offset)
            result = _fetch_put_call_ratios(candidate)
            if result is not None:
                date_value = candidate
                break
        if result is None:
            LOGGER.warning("No CBOE put/call data found in the week before %s", target_date)
            return

    total_pc, equity_pc = result
    if not _has_finite_values([total_pc, equity_pc]):
        LOGGER.warning("Skipping put/call for %s due to incomplete values", date_value)
        return

    query = """
        INSERT INTO track_put_call (date, total_pc_ratio, equity_pc_ratio)
        VALUES (%s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET total_pc_ratio = EXCLUDED.total_pc_ratio,
            equity_pc_ratio = EXCLUDED.equity_pc_ratio
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_value, total_pc, equity_pc))
            if not run_date:
                _delete_stale_rows(cur, "track_put_call", date_value, target_date)
        conn.commit()


# How far back to keep native-cadence history for the stepped fundamental charts.
_HISTORY_START = date(2010, 1, 1)

# Shiller's Yale spreadsheet (ie_data.xls) stopped updating in 2023; multpl.com
# publishes the same series current to today, back to 1871.
MULTPL_URL = "https://www.multpl.com/{slug}/table/by-month"


def _multpl_series(slug: str) -> pd.Series:
    """Monthly series scraped from multpl.com, one value per month, ascending."""
    response = requests.get(
        MULTPL_URL.format(slug=slug), headers={"User-Agent": "Mozilla/5.0"}, timeout=30
    )
    response.raise_for_status()
    rows = re.findall(
        r"<td>([A-Z][a-z]{2} \d{1,2}, \d{4})</td>\s*<td>\s*(?:&#x2002;)?\s*([\d.]+)",
        response.text,
    )
    if not rows:
        raise ValueError(f"no rows parsed from multpl {slug}")
    dates = [datetime.strptime(d, "%b %d, %Y") for d, _ in rows]
    series = pd.Series([float(v) for _, v in rows], index=pd.to_datetime(dates)).sort_index()
    # collapse intramonth rows (e.g. both "Jun 1" and "Jun 11") to the latest
    series = series.groupby(series.index.to_period("M")).last()
    series.index = series.index.to_timestamp()
    return series


def run_fundamentals(run_date: datetime | None = None) -> None:
    """Fundamental Disconnect inputs: ERP, multiple expansion, CAPE, margins, credit gap.

    Live-only (mixed monthly/quarterly sources with publication lags can't be
    cleanly backfilled per-date); each component fails independently.
    """
    if run_date:
        LOGGER.info("Skipping fundamentals for %s (live-only collector)", run_date.date())
        return
    target_date = datetime.now().date()

    erp = None
    try:
        spy_pe = yf.Ticker("SPY").info.get("trailingPE")
        yield_10y = None
        try:
            dgs10 = web.DataReader("DGS10", "fred", target_date - timedelta(days=14), target_date).dropna()
            if len(dgs10):
                yield_10y = float(dgs10.iloc[-1].item())
        except Exception:
            LOGGER.warning("FRED DGS10 unavailable, falling back to ^TNX")
        if yield_10y is None:
            tnx = yf.Ticker("^TNX").history(period="5d")["Close"].dropna()
            if len(tnx):
                yield_10y = float(tnx.iloc[-1])
        if spy_pe and yield_10y is not None:
            erp = (100.0 / float(spy_pe)) - yield_10y
    except Exception:
        LOGGER.exception("Failed computing equity risk premium")

    # Native-cadence series for the slow metrics; latest point feeds the daily
    # row + index, the whole series feeds the stepped history charts.
    history = {}

    cape = cape_percentile = None
    try:
        capes = _multpl_series("shiller-pe")  # monthly CAPE back to 1871
        cape = float(capes.iloc[-1])
        # Rolling percentile within the trailing 30 years (360 months)
        cape_pct_series = capes.rolling(360).apply(
            lambda w: (w <= w.iloc[-1]).mean() * 100, raw=False
        ).dropna()
        if len(cape_pct_series):
            cape_percentile = float(cape_pct_series.iloc[-1])
        history["cape"] = capes
        history["cape_percentile"] = cape_pct_series
    except Exception:
        LOGGER.exception("Failed computing CAPE from multpl")

    # Shared quarterly macro series (FRED) for multiple expansion + margins
    cp = gdp = None
    try:
        macro_start = _HISTORY_START - timedelta(days=2200)
        cp = web.DataReader("CP", "fred", macro_start, target_date).dropna().iloc[:, 0]
        gdp = web.DataReader("GDP", "fred", macro_start, target_date).dropna().iloc[:, 0]
    except Exception:
        LOGGER.exception("Failed fetching FRED CP/GDP")

    multiple_expansion = None
    try:
        # Price vs earnings: 12-month (4-quarter) change in S&P 500 price relative
        # to corporate profits. Profits use a trailing-4Q average to damp NIPA
        # noise. ~1-quarter lag vs ~9 months for GAAP trailing-EPS sources.
        if cp is not None:
            px = yf.Ticker("^GSPC").history(
                start=_HISTORY_START - timedelta(days=900), end=target_date + timedelta(days=1)
            )["Close"]
            px.index = px.index.tz_localize(None)
            pxq = px.resample("QS").last()
            dfm = pd.DataFrame({"px": pxq, "cp": cp}).dropna()
            ratio = dfm["px"] / dfm["cp"].rolling(4).mean()
            multexp_series = ((ratio / ratio.shift(4) - 1) * 100).dropna()
            if len(multexp_series):
                multiple_expansion = float(multexp_series.iloc[-1])
                history["multiple_expansion"] = multexp_series
    except Exception:
        LOGGER.exception("Failed computing price-to-profits multiple expansion")

    margins = None
    try:
        if cp is not None and gdp is not None:
            margins_series = (pd.DataFrame({"cp": cp, "gdp": gdp}).dropna()
                              .eval("cp / gdp * 100"))
            if len(margins_series):
                margins = float(margins_series.iloc[-1])
                history["margins"] = margins_series
    except Exception:
        LOGGER.exception("Failed computing profit margins")

    credit_gap = None
    try:
        # Simplified BIS-style gap: total private non-financial credit / GDP minus
        # its trailing 10-year (40-quarter) average (BIS proper uses a one-sided
        # HP-filter trend). Sourced from the Fed Z.1 accounts — households
        # (CMDEBT) + nonfinancial business (BCNSDODNS), both in $ millions — rather
        # than the BIS series CRDQUSAPABIS, which lags ~a quarter longer. Z.1
        # publishes ~10 weeks after quarter-end and keeps the household/mortgage
        # leverage channel that defined 2007. Scale differs from BIS (2007 gap
        # peaks ~+18 vs ~+24) but both saturate past the extreme=10 anchor.
        cm = web.DataReader(
            "CMDEBT", "fred", _HISTORY_START - timedelta(days=20 * 366), target_date
        ).dropna().iloc[:, 0]
        biz = web.DataReader(
            "BCNSDODNS", "fred", _HISTORY_START - timedelta(days=20 * 366), target_date
        ).dropna().iloc[:, 0]
        credit = (cm + biz) / 1000.0  # $ millions -> $ billions, to match GDP units
        if gdp is not None:
            ratio = (pd.DataFrame({"credit": credit, "gdp": gdp}).dropna().eval("credit / gdp * 100"))
            gap_series = (ratio - ratio.rolling(40, min_periods=40).mean()).dropna()
            if len(gap_series):
                credit_gap = float(gap_series.iloc[-1])
                history["credit_gap"] = gap_series
    except Exception:
        LOGGER.exception("Failed computing credit-to-GDP gap")

    values = [erp, multiple_expansion, cape, cape_percentile, margins, credit_gap]
    if all(v is None for v in values):
        LOGGER.warning("No fundamentals components available for %s", target_date)
        return

    query = """
        INSERT INTO track_fundamentals
            (date, erp_pct, multiple_expansion_pct, cape, cape_percentile, margins_pct, credit_gap_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET erp_pct = EXCLUDED.erp_pct,
            multiple_expansion_pct = EXCLUDED.multiple_expansion_pct,
            cape = EXCLUDED.cape,
            cape_percentile = EXCLUDED.cape_percentile,
            margins_pct = EXCLUDED.margins_pct,
            credit_gap_pct = EXCLUDED.credit_gap_pct
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (target_date, *values))
            for metric, series in history.items():
                rows = [
                    (metric, idx.date() if hasattr(idx, "date") else idx, float(val))
                    for idx, val in series.items()
                    if pd.notna(val) and (idx.date() if hasattr(idx, "date") else idx) >= _HISTORY_START
                ]
                if rows:
                    cur.executemany(
                        """INSERT INTO track_metric_history (metric, date, value)
                           VALUES (%s, %s, %s)
                           ON CONFLICT (metric, date) DO UPDATE SET value = EXCLUDED.value""",
                        rows,
                    )
        conn.commit()
    LOGGER.info(
        "Saved fundamentals: erp=%s multexp=%s cape=%s(pct %s) margins=%s gap=%s (+%d history series)",
        *[round(v, 2) if v is not None else None for v in values],
        len(history),
    )



# --- Crypto regime (BTC/ETH MA120 trend filter + vol) --------------------------
# Strategy source: crypto-ta-lab walk-forward validation (2026-07-07): hold while
# daily close > MA120, exit on cross below; optional sizing min(1, 50%/vol20).
# Full-upsert style like run_margin_debt: every live run recomputes and upserts
# ~13 months of daily rows (crypto trades 7 days/week — its calendar is NOT the
# NDX trading calendar, so it stays out of the per-date backfill machinery).
CRYPTO_ASSETS = ("BTC-USD", "ETH-USD")
CRYPTO_MA_N = 120
CRYPTO_VOL_N = 20
CRYPTO_TARGET_VOL = 0.50


def run_crypto(run_date: datetime | None = None) -> None:
    if run_date:
        return  # full-history upsert on every live run; per-date backfill unnecessary

    today = datetime.now().date()
    start = today - timedelta(days=560)   # 120d MA warmup + ~13 months of chart rows
    query = """
        INSERT INTO track_crypto (date, asset, close, ma_120, deviation_pct,
                                  vol_20_pct, regime, overlay_weight_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, asset) DO UPDATE
        SET close = EXCLUDED.close,
            ma_120 = EXCLUDED.ma_120,
            deviation_pct = EXCLUDED.deviation_pct,
            vol_20_pct = EXCLUDED.vol_20_pct,
            regime = EXCLUDED.regime,
            overlay_weight_pct = EXCLUDED.overlay_weight_pct
    """

    for asset in CRYPTO_ASSETS:
        df = yf.Ticker(asset).history(start=start, end=today + timedelta(days=1))
        if df.empty:
            LOGGER.warning("No %s data available", asset)
            continue
        df = _drop_missing_market_values(df, "crypto", ["Close"])
        # Crypto's UTC day is still in progress when the 13:00 PT run fires — that
        # bar's "close" is just the current price and can flash a false MA cross.
        # Judge on completed closes only (self-heals next run regardless).
        utc_today = datetime.now(timezone.utc).date()
        df = df[[d.date() < utc_today for d in df.index]]
        if df.empty:
            continue
        closes = df["Close"].astype(float)
        ma = closes.rolling(window=CRYPTO_MA_N).mean()
        vol = closes.pct_change().rolling(window=CRYPTO_VOL_N).std() * (365 ** 0.5)
        rows = []
        for idx in range(len(df)):
            if not _has_finite_values([ma.iloc[idx]]):
                continue                     # MA warmup window
            c, m = float(closes.iloc[idx]), float(ma.iloc[idx])
            v = float(vol.iloc[idx]) if _has_finite_values([vol.iloc[idx]]) else None
            regime = "LONG" if c > m else "CASH"
            weight = round(min(1.0, CRYPTO_TARGET_VOL / v) * 100, 1) if v else None
            rows.append((df.index[idx].date(), asset, c, m,
                         round((c - m) / m * 100, 2),
                         round(v * 100, 1) if v is not None else None,
                         regime, weight))
        if not rows:
            LOGGER.warning("No computable %s rows (insufficient history?)", asset)
            continue
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()
        LOGGER.info("Upserted %d crypto rows for %s (latest: %s %s)",
                    len(rows), asset, rows[-1][0], rows[-1][6])


def run_all(run_date: datetime | None = None) -> None:
    init_tables()
    jobs = [
        ("deviation", run_deviation),
        ("liquidity", run_liquidity),
        ("sentiment", run_sentiment),
        ("ipo_heat", run_ipo_heat),
        ("valuation", run_valuation),
        ("volatility", run_volatility),
        ("credit", run_credit),
        ("concentration", run_concentration),
        ("hot_sector", run_hot_sector),
        ("term_structure", run_term_structure),
        ("margin_debt", run_margin_debt),
        ("put_call", run_put_call),
        ("fundamentals", run_fundamentals),
        ("crypto", run_crypto),
    ]

    for name, job in jobs:
        try:
            job(run_date)
            LOGGER.info("Saved %s data", name)
        except Exception:
            LOGGER.exception("Failed saving %s data", name)
