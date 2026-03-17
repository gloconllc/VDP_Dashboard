"""
load_zartico_reports.py
=======================
Loads Zartico destination intelligence data into data/analytics.sqlite.

NOTE_ZARTICO_STATUS: Zartico data is historical reference (last updated Jun 2025).
Use for trend comparison only. Current data comes from Datafy, CoStar, and STR.

Data Source: 16 Zartico PDF reports in data/Zartico/
Strategy: Hardcoded extracted values (PDFs have complex layouts) + optional
          pdfplumber pass to capture any additional text metrics.

Tables populated:
  zartico_kpis                  — Top-level visitor pulse KPIs
  zartico_markets               — Top feeder markets by visitor share / spend
  zartico_spending_monthly      — Monthly avg visitor spend vs benchmark
  zartico_lodging_kpis          — Lodging performance (hotel + STVR)
  zartico_overnight_trend       — Monthly overnight visitor % trend
  zartico_event_impact          — Ohana Fest / event-window spend impact
  zartico_movement_monthly      — Visitor-to-resident device ratio monthly
  zartico_future_events_summary — YoY change in future booked events

Run from project root:
    python scripts/load_zartico_reports.py
"""

import os
import sqlite3
from datetime import datetime
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
ZARTICO_DIR  = PROJECT_ROOT / "data" / "Zartico"
LOG_PATH     = PROJECT_ROOT / "logs" / "pipeline.log"

NOW = datetime.utcnow().isoformat()


# ── Logging ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    line = f"[{_ts()}] [{status:<4}] {step}: {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


# ── DB Connection ──────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Schema Creation ────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS zartico_kpis (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date                     TEXT NOT NULL,
    data_from                       TEXT,
    data_to                         TEXT,
    pct_devices_visitors            REAL,
    pct_spend_visitors              REAL,
    pct_spend_local                 REAL,
    pct_oos_visitors                REAL,
    pct_visitors_age_25_54          REAL,
    pct_hhi_100k_plus               REAL,
    pct_visitors_with_children      REAL,
    pct_restaurant_spend_visitors   REAL,
    pct_retail_spend_visitors       REAL,
    pct_accommodation_spend_visitors REAL,
    loaded_at                       TEXT DEFAULT (datetime('now')),
    UNIQUE(report_date, data_from)
);

CREATE TABLE IF NOT EXISTS zartico_markets (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date         TEXT NOT NULL,
    rank                INTEGER,
    market              TEXT NOT NULL,
    state               TEXT,
    pct_visitors        REAL,
    pct_visitor_spend   REAL,
    loaded_at           TEXT DEFAULT (datetime('now')),
    UNIQUE(report_date, market)
);

CREATE TABLE IF NOT EXISTS zartico_spending_monthly (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    month_str           TEXT NOT NULL,
    avg_visitor_spend   REAL,
    benchmark_avg_spend REAL,
    loaded_at           TEXT DEFAULT (datetime('now')),
    UNIQUE(month_str)
);

CREATE TABLE IF NOT EXISTS zartico_lodging_kpis (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date                 TEXT NOT NULL,
    stvr_avg_stay_value         REAL,
    stvr_avg_stay_value_yoy_pct REAL,
    stvr_avg_los                REAL,
    stvr_avg_los_yoy_pct        REAL,
    hotel_ytd_occ               REAL,
    hotel_ytd_adr               REAL,
    hotel_ytd_demand            REAL,
    hotel_ytd_revenue           REAL,
    hotel_occ_yoy_pct           REAL,
    hotel_adr_yoy_pct           REAL,
    hotel_demand_yoy_pct        REAL,
    hotel_revenue_yoy_pct       REAL,
    adr_sun                     REAL,
    adr_mon                     REAL,
    adr_tue                     REAL,
    adr_wed                     REAL,
    adr_thu                     REAL,
    adr_fri                     REAL,
    adr_sat                     REAL,
    loaded_at                   TEXT DEFAULT (datetime('now')),
    UNIQUE(report_date)
);

CREATE TABLE IF NOT EXISTS zartico_overnight_trend (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    month_str       TEXT NOT NULL,
    pct_overnight   REAL,
    loaded_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(month_str)
);

CREATE TABLE IF NOT EXISTS zartico_event_impact (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date                 TEXT NOT NULL,
    event_start                 TEXT NOT NULL,
    event_end                   TEXT NOT NULL,
    change_total_spend_pct      REAL,
    change_visitor_spend_pct    REAL,
    change_resident_spend_pct   REAL,
    change_retail_spend_pct     REAL,
    change_restaurant_spend_pct REAL,
    pct_accommodation_spend     REAL,
    pct_food_bev_spend          REAL,
    pct_gas_spend               REAL,
    pct_retail_spend            REAL,
    pct_arts_spend              REAL,
    loaded_at                   TEXT DEFAULT (datetime('now')),
    UNIQUE(event_start, event_end)
);

CREATE TABLE IF NOT EXISTS zartico_movement_monthly (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    month_str               TEXT NOT NULL,
    visitor_resident_ratio  REAL,
    benchmark_ratio         REAL,
    loaded_at               TEXT DEFAULT (datetime('now')),
    UNIQUE(month_str)
);

CREATE TABLE IF NOT EXISTS zartico_future_events_summary (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date                 TEXT NOT NULL,
    yoy_pct_change_events       REAL,
    yoy_pct_change_attendees    REAL,
    loaded_at                   TEXT DEFAULT (datetime('now'))
);
"""


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    log("zartico_schema", "OK  ", "All 8 Zartico tables created / verified")


# ── Hardcoded Data (extracted from PDFs) ──────────────────────────────────────

# zartico_kpis
# Source: 1_ Executive Pulse.pdf (report_date 2025-06-02, no specific date range)
# Source: 2_ Visitor Snapshot.pdf (Q1 2025: 2025-01-01 to 2025-03-31)
ZARTICO_KPIS_DATA = [
    {
        "report_date": "2025-06-02",
        "data_from":   None,
        "data_to":     None,
        "pct_devices_visitors":             9.6,
        "pct_spend_visitors":               21.3,
        "pct_spend_local":                  21.0,
        "pct_oos_visitors":                 None,   # not broken out in Executive Pulse
        "pct_visitors_age_25_54":           54.0,
        "pct_hhi_100k_plus":                68.0,
        "pct_visitors_with_children":       35.0,
        "pct_restaurant_spend_visitors":    13.0,
        "pct_retail_spend_visitors":        12.0,
        "pct_accommodation_spend_visitors": 76.0,
    },
    {
        "report_date": "2025-03-31",     # end of Q1 snapshot period
        "data_from":   "2025-01-01",
        "data_to":     "2025-03-31",
        "pct_devices_visitors":             21.2,
        "pct_spend_visitors":               48.0,
        "pct_spend_local":                  20.7,
        "pct_oos_visitors":                 23.0,
        "pct_visitors_age_25_54":           None,
        "pct_hhi_100k_plus":                None,
        "pct_visitors_with_children":       None,
        "pct_restaurant_spend_visitors":    None,
        "pct_retail_spend_visitors":        None,
        "pct_accommodation_spend_visitors": None,
    },
]

# zartico_markets
# Source: 1_ Executive Pulse.pdf — visitor share only
# Source: 2_ Visitor Snapshot.pdf — both visitor share and visitor spend share
ZARTICO_MARKETS_DATA = [
    # Executive Pulse (2025-06-02) — visitor share only
    {"report_date": "2025-06-02", "rank": 1, "market": "Los Angeles",  "state": "CA", "pct_visitors": 22.0, "pct_visitor_spend": None},
    {"report_date": "2025-06-02", "rank": 2, "market": "San Diego",    "state": "CA", "pct_visitors": 10.0, "pct_visitor_spend": None},
    {"report_date": "2025-06-02", "rank": 3, "market": "Phoenix",      "state": "AZ", "pct_visitors":  6.0, "pct_visitor_spend": None},
    {"report_date": "2025-06-02", "rank": 4, "market": "Las Vegas",    "state": "NV", "pct_visitors":  5.0, "pct_visitor_spend": None},
    {"report_date": "2025-06-02", "rank": 5, "market": "San Francisco","state": "CA", "pct_visitors":  4.0, "pct_visitor_spend": None},
    # Visitor Snapshot Q1 2025 (report_date = 2025-03-31) — both metrics
    {"report_date": "2025-03-31", "rank": 1, "market": "Los Angeles",   "state": "CA", "pct_visitors": 27.9, "pct_visitor_spend": 10.7},
    {"report_date": "2025-03-31", "rank": 2, "market": "San Diego",     "state": "CA", "pct_visitors":  7.7, "pct_visitor_spend":  2.3},
    {"report_date": "2025-03-31", "rank": 3, "market": "Phoenix",       "state": "AZ", "pct_visitors":  6.8, "pct_visitor_spend":  6.7},
    {"report_date": "2025-03-31", "rank": 4, "market": "San Francisco", "state": "CA", "pct_visitors":  4.3, "pct_visitor_spend":  4.3},
    {"report_date": "2025-03-31", "rank": 5, "market": "Las Vegas",     "state": "NV", "pct_visitors":  3.8, "pct_visitor_spend":  3.2},
    {"report_date": "2025-03-31", "rank": 6, "market": "Sacramento",    "state": "CA", "pct_visitors":  3.5, "pct_visitor_spend":  2.3},
]

# zartico_spending_monthly
# Source: Spending.pdf — Jul 2024–May 2025
# benchmark data available Jul–Feb only based on PDF extract
ZARTICO_SPENDING_MONTHLY_DATA = [
    {"month_str": "2024-07", "avg_visitor_spend": 204, "benchmark_avg_spend": 103},
    {"month_str": "2024-08", "avg_visitor_spend": 179, "benchmark_avg_spend": 101},
    {"month_str": "2024-09", "avg_visitor_spend": 172, "benchmark_avg_spend":  98},
    {"month_str": "2024-10", "avg_visitor_spend": 181, "benchmark_avg_spend":  99},
    {"month_str": "2024-11", "avg_visitor_spend": 161, "benchmark_avg_spend":  98},
    {"month_str": "2024-12", "avg_visitor_spend": 161, "benchmark_avg_spend":  98},
    {"month_str": "2025-01", "avg_visitor_spend": 125, "benchmark_avg_spend":  94},
    {"month_str": "2025-02", "avg_visitor_spend": 132, "benchmark_avg_spend":  90},
    {"month_str": "2025-03", "avg_visitor_spend": 165, "benchmark_avg_spend": None},
    {"month_str": "2025-04", "avg_visitor_spend": 165, "benchmark_avg_spend": None},
    {"month_str": "2025-05", "avg_visitor_spend": 133, "benchmark_avg_spend": None},
]

# zartico_lodging_kpis
# Sources: Lodging Core.pdf + Lodging_ Hotel - Monthly.pdf (both report_date 2025-06-02)
ZARTICO_LODGING_KPIS_DATA = [
    {
        "report_date":                  "2025-06-02",
        # STVR metrics (Lodging Core.pdf)
        "stvr_avg_stay_value":          1920.0,
        "stvr_avg_stay_value_yoy_pct":  None,    # prior-year value was negative (exact % not extractable)
        "stvr_avg_los":                 5.7,
        "stvr_avg_los_yoy_pct":         -5.0,
        # Hotel YTD metrics (Lodging Hotel Monthly.pdf; STR data as of 2025-03-01)
        "hotel_ytd_occ":                51.5,
        "hotel_ytd_adr":                350.0,
        "hotel_ytd_demand":             80300.0,     # 80.3K room-nights
        "hotel_ytd_revenue":            28100000.0,  # $28.1M
        "hotel_occ_yoy_pct":            -6.5,
        "hotel_adr_yoy_pct":            -3.8,
        "hotel_demand_yoy_pct":         -28.6,
        "hotel_revenue_yoy_pct":        -32.0,
        # ADR by day of week (Lodging Core.pdf)
        "adr_sun": 342.0,
        "adr_mon": 319.0,
        "adr_tue": 323.0,
        "adr_wed": 338.0,
        "adr_thu": 329.0,
        "adr_fri": 345.0,
        "adr_sat": 352.0,
    }
]

# zartico_overnight_trend
# Source: 5_ Impact Report.pdf — May 2024–May 2025
ZARTICO_OVERNIGHT_TREND_DATA = [
    {"month_str": "2024-05", "pct_overnight": 30.0},
    {"month_str": "2024-06", "pct_overnight": 28.0},
    {"month_str": "2024-07", "pct_overnight": 25.0},
    {"month_str": "2024-08", "pct_overnight": 25.0},
    {"month_str": "2024-09", "pct_overnight": 24.0},
    {"month_str": "2024-10", "pct_overnight": 24.0},
    {"month_str": "2024-11", "pct_overnight": 22.0},
    {"month_str": "2024-12", "pct_overnight": 23.0},
    {"month_str": "2025-01", "pct_overnight": 23.0},
    {"month_str": "2025-02", "pct_overnight": 24.0},
    {"month_str": "2025-03", "pct_overnight": 21.0},
    {"month_str": "2025-04", "pct_overnight": 20.0},
    {"month_str": "2025-05", "pct_overnight": 19.0},
]

# zartico_event_impact
# Source: 6_ Event Report.pdf — Ohana Fest window 2025-05-04 to 2025-05-10
ZARTICO_EVENT_IMPACT_DATA = [
    {
        "report_date":                  "2025-06-02",
        "event_start":                  "2025-05-04",
        "event_end":                    "2025-05-10",
        "change_total_spend_pct":        8.5,
        "change_visitor_spend_pct":     42.3,
        "change_resident_spend_pct":    -0.2,
        "change_retail_spend_pct":       3.9,
        "change_restaurant_spend_pct": -36.2,
        "pct_accommodation_spend":      54.0,
        "pct_food_bev_spend":           24.0,
        "pct_gas_spend":                 8.3,
        "pct_retail_spend":              7.7,
        "pct_arts_spend":                4.4,
    }
]

# zartico_movement_monthly
# Source: Movement.pdf — Jul 2024–May 2025
# benchmark is approximate Zartico platform avg (~0.27–0.29)
ZARTICO_MOVEMENT_MONTHLY_DATA = [
    {"month_str": "2024-07", "visitor_resident_ratio": 0.38, "benchmark_ratio": 0.29},
    {"month_str": "2024-08", "visitor_resident_ratio": 0.35, "benchmark_ratio": 0.28},
    {"month_str": "2024-09", "visitor_resident_ratio": 0.35, "benchmark_ratio": 0.28},
    {"month_str": "2024-10", "visitor_resident_ratio": 0.33, "benchmark_ratio": 0.27},
    {"month_str": "2024-11", "visitor_resident_ratio": 0.36, "benchmark_ratio": 0.27},
    {"month_str": "2024-12", "visitor_resident_ratio": 0.34, "benchmark_ratio": 0.27},
    {"month_str": "2025-01", "visitor_resident_ratio": 0.34, "benchmark_ratio": 0.28},
    {"month_str": "2025-02", "visitor_resident_ratio": 0.23, "benchmark_ratio": 0.28},
    {"month_str": "2025-03", "visitor_resident_ratio": 0.22, "benchmark_ratio": 0.28},
    {"month_str": "2025-04", "visitor_resident_ratio": 0.17, "benchmark_ratio": 0.28},
]

# zartico_future_events_summary
# Source: Trends_ Future Events.pdf
ZARTICO_FUTURE_EVENTS_SUMMARY_DATA = [
    {
        "report_date":               "2025-06-02",
        "yoy_pct_change_events":      63.52,
        "yoy_pct_change_attendees":  101.13,
    }
]


# ── Loaders ────────────────────────────────────────────────────────────────────

def load_zartico_kpis(conn: sqlite3.Connection) -> int:
    sql = """
    INSERT INTO zartico_kpis
        (report_date, data_from, data_to, pct_devices_visitors, pct_spend_visitors,
         pct_spend_local, pct_oos_visitors, pct_visitors_age_25_54, pct_hhi_100k_plus,
         pct_visitors_with_children, pct_restaurant_spend_visitors,
         pct_retail_spend_visitors, pct_accommodation_spend_visitors, loaded_at)
    VALUES
        (:report_date, :data_from, :data_to, :pct_devices_visitors, :pct_spend_visitors,
         :pct_spend_local, :pct_oos_visitors, :pct_visitors_age_25_54, :pct_hhi_100k_plus,
         :pct_visitors_with_children, :pct_restaurant_spend_visitors,
         :pct_retail_spend_visitors, :pct_accommodation_spend_visitors, datetime('now'))
    ON CONFLICT(report_date, data_from) DO UPDATE SET
        data_to                         = excluded.data_to,
        pct_devices_visitors            = excluded.pct_devices_visitors,
        pct_spend_visitors              = excluded.pct_spend_visitors,
        pct_spend_local                 = excluded.pct_spend_local,
        pct_oos_visitors                = excluded.pct_oos_visitors,
        pct_visitors_age_25_54          = excluded.pct_visitors_age_25_54,
        pct_hhi_100k_plus               = excluded.pct_hhi_100k_plus,
        pct_visitors_with_children      = excluded.pct_visitors_with_children,
        pct_restaurant_spend_visitors   = excluded.pct_restaurant_spend_visitors,
        pct_retail_spend_visitors       = excluded.pct_retail_spend_visitors,
        pct_accommodation_spend_visitors= excluded.pct_accommodation_spend_visitors,
        loaded_at                       = datetime('now')
    """
    conn.executemany(sql, ZARTICO_KPIS_DATA)
    conn.commit()
    return len(ZARTICO_KPIS_DATA)


def load_zartico_markets(conn: sqlite3.Connection) -> int:
    sql = """
    INSERT INTO zartico_markets
        (report_date, rank, market, state, pct_visitors, pct_visitor_spend, loaded_at)
    VALUES
        (:report_date, :rank, :market, :state, :pct_visitors, :pct_visitor_spend, datetime('now'))
    ON CONFLICT(report_date, market) DO UPDATE SET
        rank              = excluded.rank,
        state             = excluded.state,
        pct_visitors      = excluded.pct_visitors,
        pct_visitor_spend = excluded.pct_visitor_spend,
        loaded_at         = datetime('now')
    """
    conn.executemany(sql, ZARTICO_MARKETS_DATA)
    conn.commit()
    return len(ZARTICO_MARKETS_DATA)


def load_zartico_spending_monthly(conn: sqlite3.Connection) -> int:
    sql = """
    INSERT INTO zartico_spending_monthly
        (month_str, avg_visitor_spend, benchmark_avg_spend, loaded_at)
    VALUES
        (:month_str, :avg_visitor_spend, :benchmark_avg_spend, datetime('now'))
    ON CONFLICT(month_str) DO UPDATE SET
        avg_visitor_spend   = excluded.avg_visitor_spend,
        benchmark_avg_spend = excluded.benchmark_avg_spend,
        loaded_at           = datetime('now')
    """
    conn.executemany(sql, ZARTICO_SPENDING_MONTHLY_DATA)
    conn.commit()
    return len(ZARTICO_SPENDING_MONTHLY_DATA)


def load_zartico_lodging_kpis(conn: sqlite3.Connection) -> int:
    sql = """
    INSERT INTO zartico_lodging_kpis
        (report_date, stvr_avg_stay_value, stvr_avg_stay_value_yoy_pct,
         stvr_avg_los, stvr_avg_los_yoy_pct, hotel_ytd_occ, hotel_ytd_adr,
         hotel_ytd_demand, hotel_ytd_revenue, hotel_occ_yoy_pct, hotel_adr_yoy_pct,
         hotel_demand_yoy_pct, hotel_revenue_yoy_pct,
         adr_sun, adr_mon, adr_tue, adr_wed, adr_thu, adr_fri, adr_sat, loaded_at)
    VALUES
        (:report_date, :stvr_avg_stay_value, :stvr_avg_stay_value_yoy_pct,
         :stvr_avg_los, :stvr_avg_los_yoy_pct, :hotel_ytd_occ, :hotel_ytd_adr,
         :hotel_ytd_demand, :hotel_ytd_revenue, :hotel_occ_yoy_pct, :hotel_adr_yoy_pct,
         :hotel_demand_yoy_pct, :hotel_revenue_yoy_pct,
         :adr_sun, :adr_mon, :adr_tue, :adr_wed, :adr_thu, :adr_fri, :adr_sat, datetime('now'))
    ON CONFLICT(report_date) DO UPDATE SET
        stvr_avg_stay_value         = excluded.stvr_avg_stay_value,
        stvr_avg_stay_value_yoy_pct = excluded.stvr_avg_stay_value_yoy_pct,
        stvr_avg_los                = excluded.stvr_avg_los,
        stvr_avg_los_yoy_pct        = excluded.stvr_avg_los_yoy_pct,
        hotel_ytd_occ               = excluded.hotel_ytd_occ,
        hotel_ytd_adr               = excluded.hotel_ytd_adr,
        hotel_ytd_demand            = excluded.hotel_ytd_demand,
        hotel_ytd_revenue           = excluded.hotel_ytd_revenue,
        hotel_occ_yoy_pct           = excluded.hotel_occ_yoy_pct,
        hotel_adr_yoy_pct           = excluded.hotel_adr_yoy_pct,
        hotel_demand_yoy_pct        = excluded.hotel_demand_yoy_pct,
        hotel_revenue_yoy_pct       = excluded.hotel_revenue_yoy_pct,
        adr_sun                     = excluded.adr_sun,
        adr_mon                     = excluded.adr_mon,
        adr_tue                     = excluded.adr_tue,
        adr_wed                     = excluded.adr_wed,
        adr_thu                     = excluded.adr_thu,
        adr_fri                     = excluded.adr_fri,
        adr_sat                     = excluded.adr_sat,
        loaded_at                   = datetime('now')
    """
    conn.executemany(sql, ZARTICO_LODGING_KPIS_DATA)
    conn.commit()
    return len(ZARTICO_LODGING_KPIS_DATA)


def load_zartico_overnight_trend(conn: sqlite3.Connection) -> int:
    sql = """
    INSERT INTO zartico_overnight_trend
        (month_str, pct_overnight, loaded_at)
    VALUES
        (:month_str, :pct_overnight, datetime('now'))
    ON CONFLICT(month_str) DO UPDATE SET
        pct_overnight = excluded.pct_overnight,
        loaded_at     = datetime('now')
    """
    conn.executemany(sql, ZARTICO_OVERNIGHT_TREND_DATA)
    conn.commit()
    return len(ZARTICO_OVERNIGHT_TREND_DATA)


def load_zartico_event_impact(conn: sqlite3.Connection) -> int:
    sql = """
    INSERT INTO zartico_event_impact
        (report_date, event_start, event_end, change_total_spend_pct,
         change_visitor_spend_pct, change_resident_spend_pct,
         change_retail_spend_pct, change_restaurant_spend_pct,
         pct_accommodation_spend, pct_food_bev_spend, pct_gas_spend,
         pct_retail_spend, pct_arts_spend, loaded_at)
    VALUES
        (:report_date, :event_start, :event_end, :change_total_spend_pct,
         :change_visitor_spend_pct, :change_resident_spend_pct,
         :change_retail_spend_pct, :change_restaurant_spend_pct,
         :pct_accommodation_spend, :pct_food_bev_spend, :pct_gas_spend,
         :pct_retail_spend, :pct_arts_spend, datetime('now'))
    ON CONFLICT(event_start, event_end) DO UPDATE SET
        report_date                 = excluded.report_date,
        change_total_spend_pct      = excluded.change_total_spend_pct,
        change_visitor_spend_pct    = excluded.change_visitor_spend_pct,
        change_resident_spend_pct   = excluded.change_resident_spend_pct,
        change_retail_spend_pct     = excluded.change_retail_spend_pct,
        change_restaurant_spend_pct = excluded.change_restaurant_spend_pct,
        pct_accommodation_spend     = excluded.pct_accommodation_spend,
        pct_food_bev_spend          = excluded.pct_food_bev_spend,
        pct_gas_spend               = excluded.pct_gas_spend,
        pct_retail_spend            = excluded.pct_retail_spend,
        pct_arts_spend              = excluded.pct_arts_spend,
        loaded_at                   = datetime('now')
    """
    conn.executemany(sql, ZARTICO_EVENT_IMPACT_DATA)
    conn.commit()
    return len(ZARTICO_EVENT_IMPACT_DATA)


def load_zartico_movement_monthly(conn: sqlite3.Connection) -> int:
    sql = """
    INSERT INTO zartico_movement_monthly
        (month_str, visitor_resident_ratio, benchmark_ratio, loaded_at)
    VALUES
        (:month_str, :visitor_resident_ratio, :benchmark_ratio, datetime('now'))
    ON CONFLICT(month_str) DO UPDATE SET
        visitor_resident_ratio = excluded.visitor_resident_ratio,
        benchmark_ratio        = excluded.benchmark_ratio,
        loaded_at              = datetime('now')
    """
    conn.executemany(sql, ZARTICO_MOVEMENT_MONTHLY_DATA)
    conn.commit()
    return len(ZARTICO_MOVEMENT_MONTHLY_DATA)


def load_zartico_future_events_summary(conn: sqlite3.Connection) -> int:
    # Not using UNIQUE on this table — just insert fresh each run
    # (avoids needing a natural key; table is tiny)
    conn.execute("DELETE FROM zartico_future_events_summary")
    sql = """
    INSERT INTO zartico_future_events_summary
        (report_date, yoy_pct_change_events, yoy_pct_change_attendees, loaded_at)
    VALUES
        (:report_date, :yoy_pct_change_events, :yoy_pct_change_attendees, datetime('now'))
    """
    conn.executemany(sql, ZARTICO_FUTURE_EVENTS_SUMMARY_DATA)
    conn.commit()
    return len(ZARTICO_FUTURE_EVENTS_SUMMARY_DATA)


# ── Optional pdfplumber pass ───────────────────────────────────────────────────

def attempt_pdfplumber_scan() -> dict:
    """
    Attempt a best-effort text scan of available Zartico PDFs.
    Returns a dict of {filename: first_300_chars_of_text}.
    Silently skips if pdfplumber is not installed.
    """
    results = {}
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        log("pdfplumber_scan", "SKIP", "pdfplumber not installed — skipping PDF text scan")
        return results

    if not ZARTICO_DIR.exists():
        log("pdfplumber_scan", "WARN", f"Zartico directory not found: {ZARTICO_DIR}")
        return results

    pdf_files = sorted(ZARTICO_DIR.glob("*.pdf"))
    if not pdf_files:
        log("pdfplumber_scan", "WARN", "No PDFs found in data/Zartico/")
        return results

    for pdf_path in pdf_files:
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                pages_text = []
                for page in pdf.pages[:3]:   # scan first 3 pages only
                    text = page.extract_text() or ""
                    pages_text.append(text)
                combined = "\n".join(pages_text)[:500]
                results[pdf_path.name] = combined
                log("pdfplumber_scan", "OK  ", f"Scanned: {pdf_path.name} ({len(combined)} chars extracted)")
        except Exception as exc:
            log("pdfplumber_scan", "WARN", f"Could not read {pdf_path.name}: {exc}")

    return results


# ── load_log helper ────────────────────────────────────────────────────────────

def write_load_log(conn: sqlite3.Connection, source: str, grain: str,
                   file_name: str, rows_inserted: int) -> None:
    conn.execute("""
        INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (source, grain, file_name, rows_inserted))
    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log("load_zartico", "INFO", "Starting Zartico data load")

    conn = get_conn()
    create_tables(conn)

    # -- Run pdfplumber scan (informational; does not block loading) ------------
    attempt_pdfplumber_scan()

    # -- Load all tables -------------------------------------------------------
    loaders = [
        ("zartico_kpis",                  load_zartico_kpis),
        ("zartico_markets",               load_zartico_markets),
        ("zartico_spending_monthly",      load_zartico_spending_monthly),
        ("zartico_lodging_kpis",          load_zartico_lodging_kpis),
        ("zartico_overnight_trend",       load_zartico_overnight_trend),
        ("zartico_event_impact",          load_zartico_event_impact),
        ("zartico_movement_monthly",      load_zartico_movement_monthly),
        ("zartico_future_events_summary", load_zartico_future_events_summary),
    ]

    total_rows = 0
    for table_name, loader_fn in loaders:
        try:
            n = loader_fn(conn)
            log(f"Loaded {table_name}", "OK  ", f"{n} rows")
            write_load_log(conn, "Zartico", "historical_report", table_name, n)
            total_rows += n
        except Exception as exc:
            log(f"Loaded {table_name}", "FAIL", str(exc))

    conn.close()
    log("load_zartico", "OK  ", f"Complete — {total_rows} total rows across 8 tables")


if __name__ == "__main__":
    main()
