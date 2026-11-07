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
Shared DDL and helper functions for VDP Analytics tests.
Import from here in test files; conftest.py provides pytest fixtures.
"""

import sqlite3


FACT_STR_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS fact_str_metrics (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    grain         TEXT NOT NULL,
    property_name TEXT NOT NULL,
    market        TEXT NOT NULL,
    submarket     TEXT,
    as_of_date    TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    metric_value  REAL,
    unit          TEXT,
    UNIQUE (source, grain, property_name, market, as_of_date, metric_name)
);
"""

KPI_DAILY_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS kpi_daily_summary (
    as_of_date  TEXT PRIMARY KEY,
    occ_pct     REAL,
    adr         REAL,
    revpar      REAL,
    occ_yoy     REAL,
    adr_yoy     REAL,
    revpar_yoy  REAL,
    is_occ_80   INTEGER,
    is_occ_90   INTEGER,
    created_at  TEXT DEFAULT (datetime('now'))
);
"""

KPI_COMPRESSION_QUARTERLY_DDL = """
CREATE TABLE IF NOT EXISTS kpi_compression_quarterly (
    quarter           TEXT PRIMARY KEY,
    days_above_80_occ INTEGER,
    days_above_90_occ INTEGER,
    created_at        TEXT DEFAULT (datetime('now'))
);
"""

LOAD_LOG_DDL = """
CREATE TABLE IF NOT EXISTS load_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT,
    grain         TEXT,
    file_name     TEXT,
    rows_inserted INTEGER,
    run_at        TEXT DEFAULT (datetime('now'))
);
"""


def insert_daily_str(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    """Insert rows into fact_str_metrics using OR IGNORE dedup."""
    conn.executemany(
        "INSERT OR IGNORE INTO fact_str_metrics "
        "(source, grain, property_name, market, as_of_date, metric_name, metric_value, unit) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def make_mem_conn() -> sqlite3.Connection:
    """Return a fresh in-memory SQLite with the core schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute(FACT_STR_METRICS_DDL)
    conn.execute(KPI_DAILY_SUMMARY_DDL)
    conn.execute(KPI_COMPRESSION_QUARTERLY_DDL)
    conn.execute(LOAD_LOG_DDL)
    conn.commit()
    return conn
