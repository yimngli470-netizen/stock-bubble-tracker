from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from app.composite import (
    DISCONNECT_SIGNALS,
    PANIC_ZONE,
    compute,
    episode_scores,
    history,
    latest_values,
    quadrant,
)
from app.db import fetch_all, init_tables

app = FastAPI(title="Bubble Tracker API")

TABLES = {
    "deviation": "track_deviation",
    "liquidity": "track_liquidity",
    "sentiment": "track_sentiment",
    "ipo_heat": "track_ipo_heat",
    "valuation": "track_valuation",
    "volatility": "track_volatility",
    "credit": "track_credit",
    "concentration": "track_concentration",
    "hot_sector": "track_hot_sector",
    "term_structure": "track_term_structure",
    "margin_debt": "track_margin_debt",
    "put_call": "track_put_call",
    "fundamentals": "track_fundamentals",
}


@app.on_event("startup")
def startup_event() -> None:
    init_tables()


@app.get("/")
def home() -> FileResponse:
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/metrics")
def list_metrics() -> list[str]:
    return list(TABLES.keys())


@app.get("/metrics/{metric}")
def metric_series(metric: str) -> list[dict]:
    table_name = TABLES.get(metric)
    if not table_name:
        raise HTTPException(status_code=404, detail="Unknown metric")

    rows = fetch_all(f"SELECT * FROM {table_name} ORDER BY date ASC")
    return rows


METRIC_HISTORY_KEYS = {"cape", "cape_percentile", "multiple_expansion", "margins", "credit_gap"}


@app.get("/metric_history/{metric}")
def metric_history(metric: str) -> list[dict]:
    if metric not in METRIC_HISTORY_KEYS:
        raise HTTPException(status_code=404, detail="Unknown metric")
    return fetch_all(
        "SELECT date, value FROM track_metric_history WHERE metric = %s ORDER BY date ASC",
        (metric,),
    )


@app.get("/composite")
def composite() -> dict:
    values, dates = latest_values()
    result = compute(values)
    for signal in result["signals"]:
        signal["date"] = dates.get(signal["key"])
    result["episodes"] = episode_scores()
    result["panic_zone"] = PANIC_ZONE

    d_values, d_dates = latest_values(DISCONNECT_SIGNALS)
    disconnect = compute(d_values, DISCONNECT_SIGNALS)
    for signal in disconnect["signals"]:
        signal["date"] = d_dates.get(signal["key"])
    disconnect["quadrant"] = quadrant(result["score"], disconnect["score"])
    result["disconnect"] = disconnect
    return result


@app.get("/composite/history")
def composite_history() -> list[dict]:
    return history()


CRYPTO_ASSETS = {"BTC-USD", "ETH-USD"}


@app.get("/crypto")
def crypto_latest() -> dict:
    out = {}
    for asset in sorted(CRYPTO_ASSETS):
        rows = fetch_all(
            "SELECT * FROM track_crypto WHERE asset = %s ORDER BY date DESC LIMIT 1", (asset,))
        out[asset] = rows[0] if rows else None
    return out


@app.get("/crypto/history/{asset}")
def crypto_history(asset: str) -> list[dict]:
    if asset not in CRYPTO_ASSETS:
        raise HTTPException(status_code=404, detail="Unknown asset")
    return fetch_all(
        "SELECT * FROM track_crypto WHERE asset = %s ORDER BY date ASC", (asset,))


@app.get("/latest")
def latest() -> dict:
    out = {}
    for key, table in TABLES.items():
        rows = fetch_all(f"SELECT * FROM {table} ORDER BY date DESC LIMIT 1")
        out[key] = rows[0] if rows else None
    return out
