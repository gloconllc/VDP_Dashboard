# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
fetch_demand_signals.py
-----------------------
Computes a unified DEMAND SIGNAL INDEX for Dana Point using statistical
correlation and weighting across all available data sources.

This is a computed/derived table — no external API calls.
It reads from existing DB tables and produces actionable forward signals.

Outputs:
  demand_signal_weekly  — weekly demand score, components, and narrative
  correlation_matrix    — Pearson correlation between key metrics

Why this matters:
  - Synthesizes 8+ data sources into one actionable number (0-100 scale)
  - Identifies leading vs lagging indicators with measured lag times
  - Quantifies which signals are most predictive of STR occupancy
  - Provides the AI chatbot with correlation-backed answers

Demand Signal Index components (weights):
  35% — Google Trends search interest (2-4 week lead)
  20% — Weather score (temp, precipitation, swell quality)
  15% — Wikipedia awareness (1-2 week lead)
  15% — Gas prices (drive-market affordability, inverse)
  10% — Forward events (30-60 day calendar)
  5%  — TSA throughput (regional air travel)

Table: demand_signal_weekly
  week_date TEXT, demand_score REAL, trend_component REAL,
  weather_component REAL, awareness_component REAL,
  gas_component REAL, events_component REAL, tsa_component REAL,
  signal_direction TEXT ('rising'|'stable'|'declining'),
  narrative TEXT, updated_at TEXT

Table: data_correlation_matrix
  metric_a TEXT, metric_b TEXT, pearson_r REAL, lag_weeks INTEGER,
  sample_size INTEGER, interpretation TEXT, updated_at TEXT
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

INIT_SQL = """
CREATE TABLE IF NOT EXISTS demand_signal_weekly (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    week_date            TEXT NOT NULL,
    demand_score         REAL,
    trend_component      REAL,
    weather_component    REAL,
    awareness_component  REAL,
    gas_component        REAL,
    events_component     REAL,
    tsa_component        REAL,
    signal_direction     TEXT,
    score_change_wow     REAL,
    narrative            TEXT,
    updated_at           TEXT DEFAULT (datetime('now')),
    UNIQUE(week_date) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS data_correlation_matrix (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_a        TEXT NOT NULL,
    metric_b        TEXT NOT NULL,
    pearson_r       REAL,
    lag_weeks       INTEGER DEFAULT 0,
    sample_size     INTEGER,
    p_value_approx  REAL,
    is_significant  INTEGER,
    interpretation  TEXT,
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(metric_a, metric_b, lag_weeks) ON CONFLICT REPLACE
);
"""

# Today's date string
TODAY = datetime.today().strftime("%Y-%m-%d")


def _safe_pearson(a: pd.Series, b: pd.Series) -> tuple[float, float, int]:
    """Return (r, p_approx, n) for valid paired values."""
    df = pd.DataFrame({"a": a, "b": b}).dropna()
    n = len(df)
    if n < 5:
        return 0.0, 1.0, n
    r = df["a"].corr(df["b"])
    if pd.isna(r):
        return 0.0, 1.0, n
    # Approximate p-value using t-distribution
    if abs(r) >= 1.0:
        return float(r), 0.0, n
    t_stat = r * np.sqrt(n - 2) / np.sqrt(1 - r**2)
    # Two-tailed approximate p (rough, no scipy needed)
    from math import erfc, sqrt
    t_abs = abs(float(t_stat))
    p_approx = float(erfc(t_abs / sqrt(2)))
    return float(r), p_approx, n


def _interp_r(r: float) -> str:
    """Human interpretation of Pearson r."""
    a = abs(r)
    direction = "positive" if r > 0 else "inverse"
    if a >= 0.7:
        strength = "strong"
    elif a >= 0.4:
        strength = "moderate"
    elif a >= 0.2:
        strength = "weak"
    else:
        return "no meaningful correlation"
    return f"{strength} {direction} correlation"


def _normalize_series(s: pd.Series, invert: bool = False) -> pd.Series:
    """Normalize to 0-100 scale. invert=True for metrics where lower = better demand."""
    mn, mx = s.min(), s.max()
    if mx == mn:
        return pd.Series([50.0] * len(s), index=s.index)
    norm = (s - mn) / (mx - mn) * 100
    return (100 - norm) if invert else norm


def load_google_trends(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            """SELECT week_date, AVG(interest_idx) as avg_interest
               FROM google_trends_weekly
               WHERE category = 'primary'
               GROUP BY week_date
               ORDER BY week_date""",
            conn,
        )
        df["week_date"] = pd.to_datetime(df["week_date"])
        return df
    except Exception:
        return pd.DataFrame()


def load_kpi_weekly(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            """SELECT as_of_date, occ_pct, adr, revpar
               FROM kpi_daily_summary
               ORDER BY as_of_date""",
            conn,
        )
        df["as_of_date"] = pd.to_datetime(df["as_of_date"])
        df["week"] = df["as_of_date"].dt.to_period("W").dt.start_time
        return df.groupby("week")[["occ_pct", "adr", "revpar"]].mean().reset_index()
    except Exception:
        return pd.DataFrame()


def load_wikipedia(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            """SELECT as_of_date, SUM(views) as total_views
               FROM wikipedia_pageviews_daily
               GROUP BY as_of_date
               ORDER BY as_of_date""",
            conn,
        )
        df["as_of_date"] = pd.to_datetime(df["as_of_date"])
        df["week"] = df["as_of_date"].dt.to_period("W").dt.start_time
        return df.groupby("week")["total_views"].sum().reset_index()
    except Exception:
        return pd.DataFrame()


def load_gas_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            """SELECT week_end_date, AVG(price_per_gallon) as avg_gas
               FROM eia_gas_prices
               GROUP BY week_end_date
               ORDER BY week_end_date""",
            conn,
        )
        df["week_end_date"] = pd.to_datetime(df["week_end_date"])
        return df
    except Exception:
        return pd.DataFrame()


def load_weather(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            """SELECT month, avg_temp_f, precip_inches
               FROM weather_monthly
               ORDER BY month""",
            conn,
        )
        return df
    except Exception:
        return pd.DataFrame()


def load_events_forward(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        cutoff = TODAY
        df = pd.read_sql_query(
            f"""SELECT event_date, is_major
                FROM vdp_events
                WHERE event_date >= '{cutoff}'
                ORDER BY event_date""",
            conn,
        )
        return df
    except Exception:
        return pd.DataFrame()


def load_tsa(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        df = pd.read_sql_query(
            """SELECT travel_date, travelers_count
               FROM tsa_checkpoint_daily
               ORDER BY travel_date""",
            conn,
        )
        df["travel_date"] = pd.to_datetime(df["travel_date"])
        df["week"] = df["travel_date"].dt.to_period("W").dt.start_time
        return df.groupby("week")["travelers_count"].mean().reset_index()
    except Exception:
        return pd.DataFrame()


def compute_correlations(conn: sqlite3.Connection) -> list[tuple]:
    """Compute Pearson correlations between key metrics across data sources."""
    kpi    = load_kpi_weekly(conn)
    trends = load_google_trends(conn)
    wiki   = load_wikipedia(conn)
    gas    = load_gas_prices(conn)

    results: list[tuple] = []

    # Merge trends → kpi (test 0, 1, 2, 3 week leads)
    if not kpi.empty and not trends.empty:
        kpi_w = kpi.copy()
        kpi_w["week"] = pd.to_datetime(kpi_w["week"])
        trends_w = trends.copy()
        trends_w["week"] = pd.to_datetime(trends_w["week_date"])

        for lag in [0, 1, 2, 3, 4]:
            trends_lagged = trends_w.copy()
            trends_lagged["week"] = trends_lagged["week"] + pd.Timedelta(weeks=lag)
            merged = kpi_w.merge(trends_lagged, on="week", how="inner")
            if len(merged) >= 5:
                r, p, n = _safe_pearson(merged["avg_interest"], merged["occ_pct"])
                results.append((
                    "google_trends_primary", "kpi_occ_pct", float(r), lag, n,
                    float(p), int(abs(r) > 0.2 and p < 0.1), _interp_r(r),
                ))

    # Merge gas → kpi (inverse relationship expected)
    if not kpi.empty and not gas.empty:
        kpi_w = kpi.copy()
        kpi_w["week"] = pd.to_datetime(kpi_w["week"])
        gas_w = gas.copy()
        gas_w["week"] = pd.to_datetime(gas_w["week_end_date"])
        for lag in [0, 1, 2]:
            gas_lagged = gas_w.copy()
            gas_lagged["week"] = gas_lagged["week"] + pd.Timedelta(weeks=lag)
            merged = kpi_w.merge(gas_lagged, on="week", how="inner")
            if len(merged) >= 5:
                r, p, n = _safe_pearson(merged["avg_gas"], merged["occ_pct"])
                results.append((
                    "eia_gas_price", "kpi_occ_pct", float(r), lag, n,
                    float(p), int(abs(r) > 0.2 and p < 0.1), _interp_r(r),
                ))

    # Wikipedia → kpi
    if not kpi.empty and not wiki.empty:
        kpi_w = kpi.copy()
        kpi_w["week"] = pd.to_datetime(kpi_w["week"])
        wiki_w = wiki.copy()
        wiki_w["week"] = pd.to_datetime(wiki_w["week"])
        for lag in [0, 1, 2]:
            wiki_lagged = wiki_w.copy()
            wiki_lagged["week"] = wiki_lagged["week"] + pd.Timedelta(weeks=lag)
            merged = kpi_w.merge(wiki_lagged, on="week", how="inner")
            if len(merged) >= 5:
                r, p, n = _safe_pearson(merged["total_views"], merged["occ_pct"])
                results.append((
                    "wikipedia_pageviews", "kpi_occ_pct", float(r), lag, n,
                    float(p), int(abs(r) > 0.2 and p < 0.1), _interp_r(r),
                ))

    # Google Trends → ADR
    if not kpi.empty and not trends.empty:
        kpi_w = kpi.copy()
        kpi_w["week"] = pd.to_datetime(kpi_w["week"])
        trends_w = trends.copy()
        trends_w["week"] = pd.to_datetime(trends_w["week_date"])
        for lag in [0, 2, 4]:
            trends_lagged = trends_w.copy()
            trends_lagged["week"] = trends_lagged["week"] + pd.Timedelta(weeks=lag)
            merged = kpi_w.merge(trends_lagged, on="week", how="inner")
            if len(merged) >= 5:
                r, p, n = _safe_pearson(merged["avg_interest"], merged["adr"])
                results.append((
                    "google_trends_primary", "kpi_adr", float(r), lag, n,
                    float(p), int(abs(r) > 0.2 and p < 0.1), _interp_r(r),
                ))

    return results


def compute_demand_signal_weekly(conn: sqlite3.Connection) -> list[dict]:
    """Compute weekly demand signal index (0-100) combining all sources."""
    trends = load_google_trends(conn)
    wiki   = load_wikipedia(conn)
    gas    = load_gas_prices(conn)
    tsa    = load_tsa(conn)

    if trends.empty:
        return []

    # Use Google Trends as the anchor time series (most data)
    base = trends.rename(columns={"week_date": "week", "avg_interest": "trend_raw"}).copy()
    base["week"] = pd.to_datetime(base["week"])
    base = base.sort_values("week").reset_index(drop=True)

    # Normalize trend (0-100)
    base["trend_norm"] = _normalize_series(base["trend_raw"])

    # Wikipedia awareness (weekly sum normalized)
    if not wiki.empty:
        wiki["week"] = pd.to_datetime(wiki["week"])
        base = base.merge(wiki.rename(columns={"total_views": "wiki_raw"}), on="week", how="left")
        base["awareness_norm"] = _normalize_series(base["wiki_raw"].fillna(base["wiki_raw"].median()))
    else:
        base["awareness_norm"] = 50.0

    # Gas prices (inverse — higher gas = lower demand for drive markets)
    if not gas.empty:
        gas["week"] = pd.to_datetime(gas["week_end_date"])
        gas_weekly = gas.set_index("week")["avg_gas"].resample("W-MON").mean().reset_index()
        base = base.merge(gas_weekly.rename(columns={"week": "week", "avg_gas": "gas_raw"}), on="week", how="left")
        base["gas_raw"] = base["gas_raw"].ffill().fillna(base["gas_raw"].median() if not base["gas_raw"].isna().all() else 4.5)
        base["gas_norm"] = _normalize_series(base["gas_raw"], invert=True)
    else:
        base["gas_norm"] = 50.0

    # TSA throughput (air travel demand proxy)
    if not tsa.empty:
        tsa["week"] = pd.to_datetime(tsa["week"])
        base = base.merge(tsa.rename(columns={"travelers_count": "tsa_raw"}), on="week", how="left")
        base["tsa_norm"] = _normalize_series(base["tsa_raw"].ffill().fillna(50))
    else:
        base["tsa_norm"] = 50.0

    # Weather score: month-based lookup (seasonal heuristic)
    # Dana Point historical beach weather quality by month (0-100)
    BEACH_QUALITY = {1: 55, 2: 58, 3: 65, 4: 72, 5: 78, 6: 85,
                     7: 92, 8: 90, 9: 86, 10: 76, 11: 63, 12: 54}
    base["weather_norm"] = base["week"].dt.month.map(BEACH_QUALITY).astype(float)

    # Events bonus (next 30 days from this week)
    events_df = load_events_forward(conn)
    def _events_score(week_dt: pd.Timestamp) -> float:
        if events_df.empty:
            return 0.0
        w_end = week_dt + pd.Timedelta(days=30)
        future = events_df[
            (pd.to_datetime(events_df["event_date"]) >= week_dt) &
            (pd.to_datetime(events_df["event_date"]) <= w_end)
        ]
        n_major = future[future["is_major"] == 1].shape[0] if "is_major" in future.columns else 0
        n_other = future.shape[0] - n_major
        return min(100, n_major * 20 + n_other * 5)

    base["events_norm"] = base["week"].apply(_events_score)

    # Composite score (weighted average)
    W_TREND   = 0.35
    W_WEATHER = 0.20
    W_AWARE   = 0.15
    W_GAS     = 0.15
    W_EVENTS  = 0.10
    W_TSA     = 0.05

    # Ensure all components are numeric with fallback to 50 (neutral)
    for col in ["trend_norm", "weather_norm", "awareness_norm", "gas_norm", "events_norm", "tsa_norm"]:
        base[col] = pd.to_numeric(base[col], errors="coerce").fillna(50.0)

    base["demand_score"] = (
        base["trend_norm"]    * W_TREND   +
        base["weather_norm"]  * W_WEATHER +
        base["awareness_norm"] * W_AWARE  +
        base["gas_norm"]      * W_GAS     +
        base["events_norm"]   * W_EVENTS  +
        base["tsa_norm"]      * W_TSA
    ).round(1)

    # Direction
    base["score_change_wow"] = base["demand_score"].diff().round(2)
    def _direction(d: float | None) -> str:
        if d is None or pd.isna(d):
            return "stable"
        if d > 3:
            return "rising"
        if d < -3:
            return "declining"
        return "stable"
    base["signal_direction"] = base["score_change_wow"].apply(_direction)

    # Narrative
    def _narrative(row: pd.Series) -> str:
        score = row["demand_score"]
        direction = row["signal_direction"]
        week = row["week"].strftime("%b %-d")
        trend = row["trend_norm"]
        weather = row["weather_norm"]
        gas = row.get("gas_raw", 4.5)
        tier = "high" if score >= 70 else ("moderate" if score >= 50 else "low")
        parts = [
            f"Week of {week}: demand score {score:.0f}/100 ({tier}, {direction}). "
            f"Search interest at {trend:.0f}/100"
        ]
        if not pd.isna(weather):
            parts.append(f"beach conditions {weather:.0f}/100")
        if isinstance(gas, (int, float)) and not pd.isna(gas):
            parts.append(f"CA gas ${gas:.2f}/gal")
        return "; ".join(parts) + "."

    base["narrative"] = base.apply(_narrative, axis=1)

    records = []
    for _, row in base.iterrows():
        records.append({
            "week_date":           row["week"].strftime("%Y-%m-%d"),
            "demand_score":        row["demand_score"],
            "trend_component":     round(row["trend_norm"], 1),
            "weather_component":   round(row["weather_norm"], 1),
            "awareness_component": round(row["awareness_norm"], 1),
            "gas_component":       round(row["gas_norm"], 1),
            "events_component":    round(row["events_norm"], 1),
            "tsa_component":       round(row["tsa_norm"], 1),
            "signal_direction":    row["signal_direction"],
            "score_change_wow":    row["score_change_wow"] if not pd.isna(row["score_change_wow"]) else None,
            "narrative":           row["narrative"],
        })
    return records


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.executescript(INIT_SQL)
    conn.commit()

    # Compute correlations
    print("[INFO] Computing cross-source correlations ...")
    corr_rows = compute_correlations(conn)
    if corr_rows:
        conn.executemany(
            """INSERT OR REPLACE INTO data_correlation_matrix
               (metric_a, metric_b, pearson_r, lag_weeks, sample_size,
                p_value_approx, is_significant, interpretation)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            corr_rows,
        )
        conn.commit()
        print(f"[OK]   Correlation matrix: {len(corr_rows)} metric pairs computed")
    else:
        print("[WARN] Correlation matrix: insufficient data")

    # Compute demand signal
    print("[INFO] Computing weekly demand signal index ...")
    demand_rows = compute_demand_signal_weekly(conn)
    if demand_rows:
        rows_tuples = [
            (r["week_date"], r["demand_score"], r["trend_component"],
             r["weather_component"], r["awareness_component"], r["gas_component"],
             r["events_component"], r["tsa_component"], r["signal_direction"],
             r["score_change_wow"], r["narrative"])
            for r in demand_rows
        ]
        conn.executemany(
            """INSERT OR REPLACE INTO demand_signal_weekly
               (week_date, demand_score, trend_component, weather_component,
                awareness_component, gas_component, events_component, tsa_component,
                signal_direction, score_change_wow, narrative)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows_tuples,
        )
        conn.commit()
        latest = demand_rows[-1]
        print(
            f"[OK]   Demand signal weekly: {len(demand_rows)} weeks computed | "
            f"Latest: {latest['week_date']} score={latest['demand_score']:.0f} "
            f"({latest['signal_direction']})"
        )
    else:
        print("[WARN] Demand signal: insufficient source data")

    total = len(corr_rows) + len(demand_rows)
    conn.close()
    print(f"[DONE] fetch_demand_signals: {total} total records inserted/updated")
    return total


if __name__ == "__main__":
    main()
