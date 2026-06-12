"""Composite bubble score: weighted 0-100 blend of all tracked signals.

Each signal maps its stored metric onto a 0-100 "bubble heat" sub-score via a
linear ramp between a `calm` anchor and an `extreme` anchor, clamped to
[0, 100]. Anchors with calm > extreme invert the scale (lower raw value =
hotter, e.g. credit spreads). The composite is the weight-averaged sub-score;
signals with no value are dropped and the remaining weights renormalized —
this is also how missing data in historical episodes is handled.
"""
from __future__ import annotations

from bisect import bisect_right

from app.db import fetch_all

SIGNALS = [
    # --- Price & Valuation (37%): how stretched are prices? ---
    dict(key="deviation", label="Nasdaq Deviation", section="Price & Valuation",
         table="track_deviation", field="deviation_pct", weight=12, calm=0.0, extreme=25.0),
    dict(key="valuation", label="Valuation (P/E)", section="Price & Valuation",
         table="track_valuation", field="spy_pe_deviation_pct", weight=12, calm=0.0, extreme=100.0),
    # Defensive sectors (XLP/XLU/XLV) are excluded from the scan — see collector.
    dict(key="hot_sector", label="Hottest Sector", section="Price & Valuation",
         table="track_hot_sector", field="dev_pct", weight=8, calm=0.0, extreme=25.0),
    # Concentration is a fragility (drawdown-amplification) measure, not a mania
    # timer — it read low at the actual 2000/2021 euphoria peaks — so it carries
    # the smallest price-section weight.
    dict(key="qqq_qqqe", label="Mega-cap Concentration", section="Price & Valuation",
         table="track_concentration", field="qqq_qqqe_dev_pct", weight=5, calm=0.0, extreme=10.0),
    # --- Speculation & Sentiment (38%): how euphoric is positioning? ---
    dict(key="margin_debt", label="Margin Debt YoY", section="Speculation & Sentiment",
         table="track_margin_debt", field="yoy_growth_pct", weight=16, calm=0.0, extreme=60.0),
    dict(key="put_call", label="Equity Put/Call", section="Speculation & Sentiment",
         table="track_put_call", field="equity_pc_ratio", weight=10, calm=0.90, extreme=0.45),
    dict(key="fear_greed", label="Fear & Greed", section="Speculation & Sentiment",
         table="track_sentiment", field="fear_greed_score", weight=7, calm=0.0, extreme=100.0),
    # Scored on recent-IPO relative strength vs SPY (Feb 2021 mania: +28.5%,
    # Oct 2022 bust: -14.6%), not the old volume-churn ratio which hovers
    # around 1.0 by construction and never discriminates.
    dict(key="ipo_heat", label="IPO Appetite", section="Speculation & Sentiment",
         table="track_ipo_heat", field="ipo_rel_dev_pct", weight=5, calm=0.0, extreme=25.0),
    # --- Credit & Liquidity (15%): what is fueling it? ---
    dict(key="credit", label="HY Credit Spread", section="Credit & Liquidity",
         table="track_credit", field="hy_spread_pct", weight=12, calm=7.0, extreme=2.5),
    dict(key="liquidity", label="Fed Liquidity (RRP)", section="Credit & Liquidity",
         table="track_liquidity", field="rrp_billions", weight=3, calm=2000.0, extreme=0.0),
    # --- Volatility & Complacency (10%): is risk being priced? ---
    dict(key="term_structure", label="VIX Term Structure", section="Volatility & Complacency",
         table="track_term_structure", field="vix_ratio", weight=6, calm=1.0, extreme=0.82),
    dict(key="vix", label="VIX Level", section="Volatility & Complacency",
         table="track_volatility", field="vix_level", weight=4, calm=28.0, extreme=11.0),
]

# Monthly margin data lags ~7 weeks behind today (month end + ~3 week
# publication lag); daily signals go stale after a week.
STALENESS_DAYS = {"margin_debt": 62}
DEFAULT_STALENESS_DAYS = 7
MIN_SIGNALS_FOR_SCORE = 6

# Raw metric values at historical bubble dates, scored through the same
# pipeline as live data. Provenance per value:
#   actual    — computed from the original source (yfinance/FRED/FINRA/CBOE)
#   proxy     — equivalent instrument (^SOX/SPY before SMH existed)
#   estimated — documented contemporary level, no machine-readable source left
EPISODES = [
    {
        "name": "Dot-com peak",
        "date": "2000-03-10",
        "values": {
            "deviation": 58.7,        # actual: ^NDX vs 200-day SMA
            "valuation": 83.8,        # estimated: S&P trailing P/E ~29.4 (multpl) vs 16
            "hot_sector": 28.0,       # actual: XLK/SPY 200-day dev (hottest of the SPDR sectors)
            "qqq_qqqe": 4.0,          # estimated: QQQE launched 2012; late-90s gains were broad within tech
            "margin_debt": 80.5,      # actual: FINRA margin stats, 2000-03 YoY
            "put_call": 0.40,         # estimated: CBOE archive starts 2003; contemporary reports ~0.4 lows
            "fear_greed": 90.0,       # estimated: index launched 2012; proxied as extreme greed
            "ipo_heat": 60.0,         # estimated: recent-IPO basket massively outperforming (record count, ~71% avg first-day pop)
            "credit": 5.0,            # estimated: HY OAS ~5% (credit already cracking while equities peaked)
            "liquidity": None,        # n/a: ON RRP facility did not exist
            "term_structure": 0.97,   # estimated: VIX3M launched 2007; spot vol was elevated, curve near flat
            "vix": 21.2,              # actual
        },
        "provenance": {"actual": ["deviation", "hot_sector", "margin_debt", "vix"], "proxy": [],
                       "estimated": ["valuation", "qqq_qqqe", "put_call", "fear_greed", "ipo_heat", "credit", "term_structure"]},
    },
    {
        # The speculative conditions peak before the GFC: margin growth at its
        # all-time high, HY spreads at their all-time low, VIX near 12. The
        # market top came four months later with the composite already falling
        # — the warning was here, not at the price peak.
        "name": "Pre-GFC euphoria",
        "date": "2007-06-01",
        "values": {
            "deviation": 9.5,         # actual
            "valuation": 12.5,        # estimated: S&P trailing P/E ~18 vs 16
            "hot_sector": 7.8,        # actual: XLE/SPY 200-day dev
            "qqq_qqqe": 0.5,          # estimated: mild concentration
            "margin_debt": 62.4,      # actual: the all-time YoY growth peak
            "put_call": 0.50,         # actual: CBOE archive
            "fear_greed": 75.0,       # estimated
            "ipo_heat": 12.0,         # estimated: LBO/IPO boom in H1 2007, recent issues outperforming moderately
            "credit": 2.41,           # estimated: documented all-time record low HY OAS, 2007-06-01
            "liquidity": None,        # n/a
            "term_structure": 0.86,   # estimated: VIX3M launched Dec 2007; futures curve was in steep contango
            "vix": 12.8,              # actual
        },
        "provenance": {"actual": ["deviation", "hot_sector", "margin_debt", "put_call", "vix"],
                       "proxy": [], "estimated": ["valuation", "qqq_qqqe", "fear_greed", "ipo_heat", "credit", "term_structure"]},
    },
    # Note: the October 2007 market top is deliberately NOT an episode. It scored
    # ~50% — by then margin growth and credit had already rolled over, so the
    # reading offered an investor no warning. The informative GFC reference is
    # the June 2007 euphoria peak above; the lesson is that this gauge peaks at
    # the speculative-conditions peak, months before the price top.
    {
        "name": "Post-COVID froth peak",
        "date": "2021-02-12",
        "values": {
            "deviation": 21.3,        # actual
            "valuation": 145.6,       # estimated: trailing P/E ~39 (COVID earnings hole) vs 16
            "hot_sector": 6.2,        # actual: XLF/SPY 200-day dev (broad melt-up, no single runaway sector)
            "qqq_qqqe": -1.5,         # actual: equal weight was outperforming (broad melt-up)
            "margin_debt": 49.3,      # actual
            "put_call": 0.45,         # actual: CBOE daily JSON
            "fear_greed": 76.0,       # estimated: extreme greed readings that month
            "ipo_heat": 28.5,         # actual: IPO/SPY 200-day dev at the SPAC mania peak
            "credit": 3.3,            # estimated: HY OAS ~3.3%
            "liquidity": 0.001,       # actual: RRP ~zero, all liquidity deployed
            "term_structure": 0.754,  # actual: extreme contango/complacency
            "vix": 20.0,              # actual
        },
        "provenance": {"actual": ["deviation", "hot_sector", "qqq_qqqe", "margin_debt", "put_call", "liquidity", "term_structure", "vix", "ipo_heat"],
                       "proxy": [], "estimated": ["valuation", "fear_greed", "credit"]},
    },
]


# --- Fundamental Disconnect Index ---
# The second axis: is the price backed by earnings? A true bubble = high
# euphoria AND high disconnect; euphoria with earnings behind it (semis 2026
# thesis) reads high on the first axis only. All fields live in
# track_fundamentals (live-only collector, mixed monthly/quarterly sources).
DISCONNECT_SIGNALS = [
    # Daily, direct price-vs-alternatives test; 2000 hit -3% (the all-time signal)
    dict(key="erp", label="Equity Risk Premium", section="Fundamental Disconnect",
         table="track_fundamentals", field="erp_pct", weight=30, calm=3.0, extreme=-3.0),
    # Is price outrunning earnings? 12-month change in S&P price vs corporate
    # profits (FRED CP, 4Q-smoothed; ~1-quarter lag, far fresher than GAAP EPS)
    dict(key="multiple_expansion", label="Multiple Expansion (12m)", section="Fundamental Disconnect",
         table="track_fundamentals", field="multiple_expansion_pct", weight=25, calm=0.0, extreme=30.0),
    # Percentile of CAPE vs its trailing 30 years; already 0-100
    dict(key="cape", label="CAPE Percentile (30y)", section="Fundamental Disconnect",
         table="track_fundamentals", field="cape_percentile", weight=20, calm=0.0, extreme=100.0),
    # Corporate profits / GDP; record margins make trailing P/E deceptively cheap
    dict(key="margins", label="Profit Margins (CP/GDP)", section="Fundamental Disconnect",
         table="track_fundamentals", field="margins_pct", weight=15, calm=9.0, extreme=13.0),
    # Simplified BIS credit-to-GDP gap; the debt-bubble detector (2007, Japan 1990)
    dict(key="credit_gap", label="Credit-to-GDP Gap", section="Fundamental Disconnect",
         table="track_fundamentals", field="credit_gap_pct", weight=10, calm=0.0, extreme=10.0),
]


def quadrant(euphoria_score, disconnect_score) -> str:
    if euphoria_score is None or disconnect_score is None:
        return "Insufficient data"
    e_high, d_high = euphoria_score >= 60, disconnect_score >= 60
    if e_high and d_high:
        return "Bubble conditions — euphoric AND detached from earnings"
    if e_high:
        return "Hot but earning it — euphoric, still backed by earnings"
    if d_high:
        return "Expensive but unloved — stretched price without euphoria"
    return "Healthy — neither euphoric nor overpriced"


# Composite readings at historical panic lows, scored through this same
# pipeline. All four were followed by +36% to +86% NDX over the following year
# (+82% to +176% over three). Below ~22 has historically marked strong
# long-term entry zones — US sample, n=4, coincident not predictive: the 2008
# reading came four months before the final price low.
PANIC_ZONE = {
    "threshold": 22.0,
    "references": [
        {"name": "GFC capitulation", "date": "2008-11-20", "score": 8.6},
        {"name": "Christmas Eve crash", "date": "2018-12-24", "score": 13.7},
        {"name": "COVID bottom", "date": "2020-03-23", "score": 20.7},
        {"name": "2022 bear low", "date": "2022-10-13", "score": 21.1},
    ],
}


def subscore(value: float, calm: float, extreme: float) -> float:
    frac = (value - calm) / (extreme - calm)
    return round(max(0.0, min(1.0, frac)) * 100, 1)


def compute(values: dict, signal_defs: list | None = None) -> dict:
    acc = 0.0
    total_weight = 0.0
    signals = []
    for sig in signal_defs or SIGNALS:
        value = values.get(sig["key"])
        entry = {k: sig[k] for k in ("key", "label", "section", "weight")}
        entry["value"] = value
        if value is None:
            entry["subscore"] = None
        else:
            s = subscore(float(value), sig["calm"], sig["extreme"])
            entry["subscore"] = s
            acc += s * sig["weight"]
            total_weight += sig["weight"]
        signals.append(entry)
    score = round(acc / total_weight, 1) if total_weight else None
    return {"score": score, "weight_used": total_weight, "signals": signals}


def latest_values(signal_defs: list | None = None) -> tuple[dict, dict]:
    values, dates = {}, {}
    for sig in signal_defs or SIGNALS:
        rows = fetch_all(
            f"SELECT date, {sig['field']} AS v FROM {sig['table']} "
            f"WHERE {sig['field']} IS NOT NULL ORDER BY date DESC LIMIT 1"
        )
        if rows:
            values[sig["key"]] = float(rows[0]["v"])
            dates[sig["key"]] = rows[0]["date"].isoformat()
    return values, dates


def episode_scores() -> list[dict]:
    out = []
    for ep in EPISODES:
        result = compute(ep["values"])
        out.append({
            "name": ep["name"],
            "date": ep["date"],
            "score": result["score"],
            "signals": result["signals"],
            "provenance": ep["provenance"],
        })
    return out


def history() -> list[dict]:
    per_signal = {}
    for sig in SIGNALS:
        rows = fetch_all(
            f"SELECT date, {sig['field']} AS v FROM {sig['table']} "
            f"WHERE {sig['field']} IS NOT NULL ORDER BY date ASC"
        )
        per_signal[sig["key"]] = ([r["date"] for r in rows], [float(r["v"]) for r in rows])

    calendar = [r["date"] for r in fetch_all("SELECT date FROM track_deviation ORDER BY date ASC")]
    out = []
    for day in calendar:
        values = {}
        for sig in SIGNALS:
            dates, vals = per_signal[sig["key"]]
            i = bisect_right(dates, day) - 1
            if i < 0:
                continue
            staleness = STALENESS_DAYS.get(sig["key"], DEFAULT_STALENESS_DAYS)
            if (day - dates[i]).days > staleness:
                continue
            values[sig["key"]] = vals[i]
        if len(values) >= MIN_SIGNALS_FOR_SCORE:
            out.append({"date": day.isoformat(), "score": compute(values)["score"]})
    return out
