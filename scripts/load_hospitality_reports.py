"""
load_hospitality_reports.py
============================
Parses Market Hospitality Report CSVs and loads structured data into
data/analytics.sqlite.

Source files (data/downloads/):
  hosp_market_kpis.csv              → hosp_market_kpis
  hosp_market_pipeline.csv          → hosp_market_pipeline
  hosp_market_segments.csv          → hosp_market_segments
  hosp_market_competitive_index.csv → hosp_market_competitive_index
  hosp_market_property_class.csv    → hosp_market_property_class
  hosp_market_forecast.csv          → hosp_market_forecast
  hosp_market_tbid_revenue.csv      → hosp_market_tbid_revenue

Run from project root:
    python scripts/load_hospitality_reports.py
"""

import os
import csv
import sqlite3
import re
from datetime import datetime
from pathlib import Path

# ─── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
DOWNLOADS    = PROJECT_ROOT / "data" / "downloads"
LOG_PATH     = PROJECT_ROOT / "logs" / "pipeline.log"

NOW = datetime.utcnow().isoformat()


# ─── Helpers ───────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    line = f"{_now_str()} | {step:<28} | {status:<4} | {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


def drop_and_create(cur: sqlite3.Cursor, table_sql: str) -> None:
    """Drop the table if it exists, then create fresh."""
    m = re.search(r'CREATE TABLE IF NOT EXISTS\s+(\w+)', table_sql, re.IGNORECASE)
    if m:
        cur.execute(f"DROP TABLE IF EXISTS {m.group(1)}")
    cur.executescript(table_sql)


def _float(val: str) -> float | None:
    """Parse float or return None for empty strings."""
    v = str(val).strip()
    if v in ("", "None", "null", "NULL"):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _int(val: str) -> int | None:
    v = str(val).strip()
    if v in ("", "None", "null", "NULL"):
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def read_csv(filename: str) -> list[dict]:
    path = DOWNLOADS / filename
    if not path.exists():
        log("hosp_loader", "WARN", f"{filename} not found in {DOWNLOADS} — skipping")
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 1 — hosp_market_kpis
# Monthly hotel market performance (occupancy, ADR, RevPAR, supply/demand)
# ══════════════════════════════════════════════════════════════════════════════

def load_market_kpis(cur: sqlite3.Cursor) -> int:
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS hosp_market_kpis (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        year                 INTEGER NOT NULL,
        month                INTEGER NOT NULL,
        period_label         TEXT,
        occ_pct              REAL,
        adr_usd              REAL,
        revpar_usd           REAL,
        supply_room_nights   INTEGER,
        demand_room_nights   INTEGER,
        room_revenue_usd     REAL,
        yoy_occ_pp           REAL,
        yoy_adr_pct          REAL,
        yoy_revpar_pct       REAL,
        report_source        TEXT,
        loaded_at            TEXT DEFAULT (datetime('now'))
    );
    """)

    rows = read_csv("hosp_market_kpis.csv")
    count = 0
    for r in rows:
        cur.execute("""
        INSERT INTO hosp_market_kpis
          (year, month, period_label, occ_pct, adr_usd, revpar_usd,
           supply_room_nights, demand_room_nights, room_revenue_usd,
           yoy_occ_pp, yoy_adr_pct, yoy_revpar_pct, report_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            _int(r["year"]), _int(r["month"]), r["period_label"],
            _float(r["occ_pct"]), _float(r["adr_usd"]), _float(r["revpar_usd"]),
            _int(r["supply_room_nights"]), _int(r["demand_room_nights"]),
            _float(r["room_revenue_usd"]),
            _float(r["yoy_occ_pp"]), _float(r["yoy_adr_pct"]),
            _float(r["yoy_revpar_pct"]), r["report_source"],
        ))
        count += 1
    log("hosp_market_kpis", "OK  ", f"{count} rows inserted")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 2 — hosp_market_pipeline
# Hotel development pipeline: new supply entering the market
# ══════════════════════════════════════════════════════════════════════════════

def load_pipeline(cur: sqlite3.Cursor) -> int:
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS hosp_market_pipeline (
        id                              INTEGER PRIMARY KEY AUTOINCREMENT,
        property_name                   TEXT,
        location                        TEXT,
        rooms                           INTEGER,
        property_class                  TEXT,
        status                          TEXT,
        expected_open_date              TEXT,
        developer                       TEXT,
        brand_affiliation               TEXT,
        segment_focus                   TEXT,
        estimated_annual_room_revenue_usd REAL,
        notes                           TEXT,
        report_source                   TEXT,
        loaded_at                       TEXT DEFAULT (datetime('now'))
    );
    """)

    rows = read_csv("hosp_market_pipeline.csv")
    count = 0
    for r in rows:
        cur.execute("""
        INSERT INTO hosp_market_pipeline
          (property_name, location, rooms, property_class, status,
           expected_open_date, developer, brand_affiliation, segment_focus,
           estimated_annual_room_revenue_usd, notes, report_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            r["property_name"], r["location"], _int(r["rooms"]),
            r["property_class"], r["status"], r["expected_open_date"],
            r["developer"], r["brand_affiliation"], r["segment_focus"],
            _float(r["estimated_annual_room_revenue_usd"]),
            r["notes"], r["report_source"],
        ))
        count += 1
    log("hosp_market_pipeline", "OK  ", f"{count} rows inserted")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 3 — hosp_market_segments
# Quarterly demand segmentation: transient leisure, business, group, contract
# ══════════════════════════════════════════════════════════════════════════════

def load_segments(cur: sqlite3.Cursor) -> int:
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS hosp_market_segments (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        year                        INTEGER NOT NULL,
        quarter                     TEXT NOT NULL,
        segment                     TEXT NOT NULL,
        room_nights_sold            INTEGER,
        occ_pct                     REAL,
        adr_usd                     REAL,
        revpar_usd                  REAL,
        share_of_total_demand_pct   REAL,
        yoy_demand_chg_pct          REAL,
        yoy_adr_chg_pct             REAL,
        report_source               TEXT,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    rows = read_csv("hosp_market_segments.csv")
    count = 0
    for r in rows:
        cur.execute("""
        INSERT INTO hosp_market_segments
          (year, quarter, segment, room_nights_sold, occ_pct,
           adr_usd, revpar_usd, share_of_total_demand_pct,
           yoy_demand_chg_pct, yoy_adr_chg_pct, report_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            _int(r["year"]), r["quarter"], r["segment"],
            _int(r["room_nights_sold"]), _float(r["occ_pct"]),
            _float(r["adr_usd"]), _float(r["revpar_usd"]),
            _float(r["share_of_total_demand_pct"]),
            _float(r["yoy_demand_chg_pct"]), _float(r["yoy_adr_chg_pct"]),
            r["report_source"],
        ))
        count += 1
    log("hosp_market_segments", "OK  ", f"{count} rows inserted")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 4 — hosp_market_competitive_index
# Monthly MPI / ARI / RGI vs. Anaheim comp set
# ══════════════════════════════════════════════════════════════════════════════

def load_competitive_index(cur: sqlite3.Cursor) -> int:
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS hosp_market_competitive_index (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        year                 INTEGER NOT NULL,
        month                INTEGER NOT NULL,
        period_label         TEXT,
        market_occ_pct       REAL,
        compset_occ_pct      REAL,
        mpi                  REAL,
        market_adr_usd       REAL,
        compset_adr_usd      REAL,
        ari                  REAL,
        market_revpar_usd    REAL,
        compset_revpar_usd   REAL,
        rgi                  REAL,
        compset_description  TEXT,
        report_source        TEXT,
        loaded_at            TEXT DEFAULT (datetime('now'))
    );
    """)

    rows = read_csv("hosp_market_competitive_index.csv")
    count = 0
    for r in rows:
        cur.execute("""
        INSERT INTO hosp_market_competitive_index
          (year, month, period_label,
           market_occ_pct, compset_occ_pct, mpi,
           market_adr_usd, compset_adr_usd, ari,
           market_revpar_usd, compset_revpar_usd, rgi,
           compset_description, report_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            _int(r["year"]), _int(r["month"]), r["period_label"],
            _float(r["market_occ_pct"]), _float(r["compset_occ_pct"]), _float(r["mpi"]),
            _float(r["market_adr_usd"]), _float(r["compset_adr_usd"]), _float(r["ari"]),
            _float(r["market_revpar_usd"]), _float(r["compset_revpar_usd"]), _float(r["rgi"]),
            r["compset_description"], r["report_source"],
        ))
        count += 1
    log("hosp_market_competitive_index", "OK  ", f"{count} rows inserted")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 5 — hosp_market_property_class
# Annual performance broken out by property class (Luxury → Economy)
# ══════════════════════════════════════════════════════════════════════════════

def load_property_class(cur: sqlite3.Cursor) -> int:
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS hosp_market_property_class (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        year                        INTEGER NOT NULL,
        property_class              TEXT NOT NULL,
        property_count              INTEGER,
        total_rooms                 INTEGER,
        occ_pct                     REAL,
        adr_usd                     REAL,
        revpar_usd                  REAL,
        room_revenue_usd            REAL,
        supply_room_nights          INTEGER,
        demand_room_nights          INTEGER,
        share_of_market_revenue_pct REAL,
        report_source               TEXT,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    rows = read_csv("hosp_market_property_class.csv")
    count = 0
    for r in rows:
        cur.execute("""
        INSERT INTO hosp_market_property_class
          (year, property_class, property_count, total_rooms,
           occ_pct, adr_usd, revpar_usd, room_revenue_usd,
           supply_room_nights, demand_room_nights,
           share_of_market_revenue_pct, report_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            _int(r["year"]), r["property_class"],
            _int(r["property_count"]), _int(r["total_rooms"]),
            _float(r["occ_pct"]), _float(r["adr_usd"]),
            _float(r["revpar_usd"]), _float(r["room_revenue_usd"]),
            _int(r["supply_room_nights"]), _int(r["demand_room_nights"]),
            _float(r["share_of_market_revenue_pct"]), r["report_source"],
        ))
        count += 1
    log("hosp_market_property_class", "OK  ", f"{count} rows inserted")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 6 — hosp_market_forecast
# Forward-looking quarterly & annual forecasts (12-month horizon)
# ══════════════════════════════════════════════════════════════════════════════

def load_forecast(cur: sqlite3.Cursor) -> int:
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS hosp_market_forecast (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        forecast_as_of_date         TEXT,
        forecast_period_start       TEXT,
        forecast_period_end         TEXT,
        period_label                TEXT,
        forecast_occ_pct            REAL,
        forecast_adr_usd            REAL,
        forecast_revpar_usd         REAL,
        forecast_room_revenue_usd   REAL,
        prior_year_occ_pct          REAL,
        prior_year_adr_usd          REAL,
        prior_year_revpar_usd       REAL,
        prior_year_room_revenue_usd REAL,
        yoy_occ_pp_chg              REAL,
        yoy_adr_pct_chg             REAL,
        yoy_revpar_pct_chg          REAL,
        confidence_level            TEXT,
        report_source               TEXT,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    rows = read_csv("hosp_market_forecast.csv")
    count = 0
    for r in rows:
        cur.execute("""
        INSERT INTO hosp_market_forecast
          (forecast_as_of_date, forecast_period_start, forecast_period_end,
           period_label, forecast_occ_pct, forecast_adr_usd, forecast_revpar_usd,
           forecast_room_revenue_usd, prior_year_occ_pct, prior_year_adr_usd,
           prior_year_revpar_usd, prior_year_room_revenue_usd,
           yoy_occ_pp_chg, yoy_adr_pct_chg, yoy_revpar_pct_chg,
           confidence_level, report_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            r["forecast_as_of_date"], r["forecast_period_start"], r["forecast_period_end"],
            r["period_label"],
            _float(r["forecast_occ_pct"]), _float(r["forecast_adr_usd"]),
            _float(r["forecast_revpar_usd"]), _float(r["forecast_room_revenue_usd"]),
            _float(r["prior_year_occ_pct"]), _float(r["prior_year_adr_usd"]),
            _float(r["prior_year_revpar_usd"]), _float(r["prior_year_room_revenue_usd"]),
            _float(r["yoy_occ_pp_chg"]), _float(r["yoy_adr_pct_chg"]),
            _float(r["yoy_revpar_pct_chg"]),
            r["confidence_level"], r["report_source"],
        ))
        count += 1
    log("hosp_market_forecast", "OK  ", f"{count} rows inserted")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 7 — hosp_market_tbid_revenue
# Quarterly TBID assessment + TOT revenue actuals and projections
# ══════════════════════════════════════════════════════════════════════════════

def load_tbid_revenue(cur: sqlite3.Cursor) -> int:
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS hosp_market_tbid_revenue (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        year                 INTEGER NOT NULL,
        quarter              TEXT NOT NULL,
        period_label         TEXT,
        room_revenue_usd     REAL,
        tbid_revenue_usd     REAL,
        tbid_rate_blended_pct REAL,
        tot_revenue_usd      REAL,
        tot_rate_pct         REAL,
        budget_tbid_usd      REAL,
        vs_budget_pct        REAL,
        prior_year_tbid_usd  REAL,
        yoy_tbid_pct         REAL,
        notes                TEXT,
        report_source        TEXT,
        loaded_at            TEXT DEFAULT (datetime('now'))
    );
    """)

    rows = read_csv("hosp_market_tbid_revenue.csv")
    count = 0
    for r in rows:
        cur.execute("""
        INSERT INTO hosp_market_tbid_revenue
          (year, quarter, period_label, room_revenue_usd,
           tbid_revenue_usd, tbid_rate_blended_pct,
           tot_revenue_usd, tot_rate_pct,
           budget_tbid_usd, vs_budget_pct,
           prior_year_tbid_usd, yoy_tbid_pct,
           notes, report_source)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            _int(r["year"]), r["quarter"], r["period_label"],
            _float(r["room_revenue_usd"]),
            _float(r["tbid_revenue_usd"]), _float(r["tbid_rate_blended_pct"]),
            _float(r["tot_revenue_usd"]), _float(r["tot_rate_pct"]),
            _float(r["budget_tbid_usd"]), _float(r["vs_budget_pct"]),
            _float(r.get("prior_year_tbid_usd", "")),
            _float(r.get("yoy_tbid_pct", "")),
            r["notes"], r["report_source"],
        ))
        count += 1
    log("hosp_market_tbid_revenue", "OK  ", f"{count} rows inserted")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log("hosp_loader", "START", "Loading Market Hospitality Reports into analytics.sqlite")

    conn = get_conn()
    cur  = conn.cursor()
    total = 0

    loaders = [
        ("hosp_market_kpis",             load_market_kpis),
        ("hosp_market_pipeline",          load_pipeline),
        ("hosp_market_segments",          load_segments),
        ("hosp_market_competitive_index", load_competitive_index),
        ("hosp_market_property_class",    load_property_class),
        ("hosp_market_forecast",          load_forecast),
        ("hosp_market_tbid_revenue",      load_tbid_revenue),
    ]

    for table_name, fn in loaders:
        try:
            n = fn(cur)
            total += n
            conn.commit()
        except Exception as exc:
            conn.rollback()
            log(table_name, "FAIL", str(exc))
            raise

    # Log to load_log (matches pipeline audit pattern)
    cur.execute("""
        INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, ("hospitality_reports", "mixed", "hosp_market_*.csv", total))
    conn.commit()
    conn.close()

    log("hosp_loader", "DONE", f"All 7 tables loaded. Total rows: {total}")


if __name__ == "__main__":
    main()
