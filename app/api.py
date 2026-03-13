from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from app.db import fetch_all, init_tables

app = FastAPI(title="Bubble Tracker API")

TABLES = {
    "deviation": "track_deviation",
    "liquidity": "track_liquidity",
    "sentiment": "track_sentiment",
    "ipo_heat": "track_ipo_heat",
    "valuation": "track_valuation",
    "volatility": "track_volatility",
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


@app.get("/latest")
def latest() -> dict:
    out = {}
    for key, table in TABLES.items():
        rows = fetch_all(f"SELECT * FROM {table} ORDER BY date DESC LIMIT 1")
        out[key] = rows[0] if rows else None
    return out
