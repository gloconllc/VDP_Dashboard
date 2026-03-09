"""
compute_kpis.py
---------------
Builds two KPI tables from fact_str_metrics (grain='daily', source='STR'):

  kpi_daily_summary
      One row per as_of_date.
      Columns: occ_pct, adr, revpar  +  yoy deltas for each.
      YOY method (all three are % change vs same calendar date prior year):
        occ_yoy      percent change            (e.g. +5.6 %)
        adr_yoy      percent change            (e.g. +6.0 %)
        revpar_yoy   percent change            (e.g. +9.5 %)
      Compression flags (derived inline in the INSERT):
        is_occ_80    1 if occ_pct >= 80, else 0
        is_occ_90    1 if occ_pct >= 90, else 0
      Rows where no prior-year date exists get NULL in the YOY columns.

  kpi_compression_quarterly
      One row per calendar quarter (YYYY-Qn).
      Counts how many days in each quarter exceeded 80% and 90% occupancy.

Both tables are fully rebuilt on every run (DELETE + INSERT).

Run:
    python3 scripts/compute_kpis.py
"""

import os
import sqlite3

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_KPI_DAILY_SUMMARY = """
CREATE TABLE IF NOT EXISTS kpi_daily_summary (
    as_of_date  TEXT PRIMARY KEY,
    occ_pct     REAL,     -- percent, e.g. 68.8 (not decimal 0.688)
    adr         REAL,     -- USD
    revpar      REAL,     -- USD
    occ_yoy     REAL,     -- percent change vs same calendar date prior year
    adr_yoy     REAL,     -- percent change vs same calendar date prior year
    revpar_yoy  REAL,     -- percent change vs same calendar date prior year
    is_occ_80   INTEGER,  -- 1 if occ_pct >= 80, else 0
    is_occ_90   INTEGER,  -- 1 if occ_pct >= 90, else 0
    created_at  TEXT DEFAULT (datetime('now'))
);
"""

# Columns added in this revision — applied via ALTER TABLE when upgrading
# an existing deployment that pre-dates this version of the script.
MIGRATION_COLUMNS = [
    ("occ_yoy",   "REAL"),
    ("adr_yoy",   "REAL"),
    ("revpar_yoy","REAL"),
    ("is_occ_80", "INTEGER"),
    ("is_occ_90", "INTEGER"),
]

DDL_KPI_COMPRESSION_QUARTERLY = """
CREATE TABLE IF NOT EXISTS kpi_compression_quarterly (
    quarter           TEXT PRIMARY KEY,  -- e.g. '2025-Q3'
    days_above_80_occ INTEGER,           -- days where occ_pct > 80
    days_above_90_occ INTEGER,           -- days where occ_pct > 90
    created_at        TEXT DEFAULT (datetime('now'))
);
"""


# ---------------------------------------------------------------------------
# DML — kpi_daily_summary
# ---------------------------------------------------------------------------

DELETE_KPI_DAILY_SUMMARY = "DELETE FROM kpi_daily_summary;"

# Single INSERT using a CTE:
#   base  — pivots fact_str_metrics long→wide for each as_of_date
#   The CTE is self-joined on date(as_of_date, '-1 year') to obtain prior-year
#   values for YOY % calculations.  LEFT JOIN means dates with no prior-year
#   row silently produce NULL in the three _yoy columns.
#   Division-by-zero guard: each CASE checks ly.metric > 0 before dividing;
#   a NULL ly row makes the condition NULL (falsy), also yielding NULL.
INSERT_KPI_DAILY_SUMMARY = """
INSERT INTO kpi_daily_summary (
    as_of_date, occ_pct, adr, revpar,
    occ_yoy, adr_yoy, revpar_yoy,
    is_occ_80, is_occ_90
)
WITH base AS (
    SELECT
        as_of_date,
        MAX(CASE WHEN metric_name = 'occ'    THEN metric_value * 100 END) AS occ_pct,
        MAX(CASE WHEN metric_name = 'adr'    THEN metric_value END)        AS adr,
        MAX(CASE WHEN metric_name = 'revpar' THEN metric_value END)        AS revpar
    FROM  fact_str_metrics
    WHERE grain  = 'daily'
      AND source = 'STR'
    GROUP BY as_of_date
)
SELECT
    b.as_of_date,
    b.occ_pct,
    b.adr,
    b.revpar,
    CASE WHEN ly.occ_pct > 0
         THEN ROUND((b.occ_pct - ly.occ_pct) / ly.occ_pct * 100, 2)
         END                                         AS occ_yoy,
    CASE WHEN ly.adr > 0
         THEN ROUND((b.adr    - ly.adr)    / ly.adr    * 100, 2)
         END                                         AS adr_yoy,
    CASE WHEN ly.revpar > 0
         THEN ROUND((b.revpar - ly.revpar) / ly.revpar * 100, 2)
         END                                         AS revpar_yoy,
    CASE WHEN b.occ_pct >= 80 THEN 1 ELSE 0 END     AS is_occ_80,
    CASE WHEN b.occ_pct >= 90 THEN 1 ELSE 0 END     AS is_occ_90
FROM  base b
LEFT  JOIN base ly ON ly.as_of_date = date(b.as_of_date, '-1 year')
ORDER BY b.as_of_date;
"""


# ---------------------------------------------------------------------------
# DML — kpi_compression_quarterly
# ---------------------------------------------------------------------------

DELETE_KPI_COMPRESSION_QUARTERLY = "DELETE FROM kpi_compression_quarterly;"

# Quarter label: YYYY-Q1 … YYYY-Q4
# SQLite integer division: (month + 2) / 3  →  1,1,1,2,2,2,3,3,3,4,4,4
INSERT_KPI_COMPRESSION_QUARTERLY = """
INSERT INTO kpi_compression_quarterly (quarter, days_above_80_occ, days_above_90_occ)
SELECT
    strftime('%Y', as_of_date) || '-Q' ||
        CAST((CAST(strftime('%m', as_of_date) AS INTEGER) + 2) / 3 AS TEXT) AS quarter,
    SUM(CASE WHEN occ_pct > 80 THEN 1 ELSE 0 END) AS days_above_80_occ,
    SUM(CASE WHEN occ_pct > 90 THEN 1 ELSE 0 END) AS days_above_90_occ
FROM  kpi_daily_summary
WHERE occ_pct IS NOT NULL
GROUP BY quarter
ORDER BY quarter;
"""


# ---------------------------------------------------------------------------
# Schema migration (safe to run on existing deployments)
# ---------------------------------------------------------------------------

def _migrate(cur: sqlite3.Cursor) -> None:
    """Add any new columns to kpi_daily_summary that don't yet exist."""
    cur.execute("PRAGMA table_info(kpi_daily_summary)")
    existing = {row[1] for row in cur.fetchall()}   # row[1] = column name
    for col, col_type in MIGRATION_COLUMNS:
        if col not in existing:
            cur.execute(f"ALTER TABLE kpi_daily_summary ADD COLUMN {col} {col_type};")
            print(f"    migrated: added column {col} ({col_type})")


# ---------------------------------------------------------------------------
# Core compute functions
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def build_kpi_daily_summary(cur: sqlite3.Cursor) -> int:
    """
    Rebuild kpi_daily_summary from scratch in a single CTE INSERT.
    YOY deltas and compression flags are computed inline — no post-UPDATE needed.
    Returns rows_inserted.
    """
    cur.execute(DDL_KPI_DAILY_SUMMARY)
    _migrate(cur)

    cur.execute(DELETE_KPI_DAILY_SUMMARY)
    print(f"  [daily 1/2] Deleted {cur.rowcount} existing row(s).")

    cur.execute(INSERT_KPI_DAILY_SUMMARY)
    inserted = cur.rowcount
    print(f"  [daily 2/2] Inserted {inserted} row(s) with YOY deltas and compression flags.")

    return inserted


def build_kpi_compression_quarterly(cur: sqlite3.Cursor) -> int:
    """
    Rebuild kpi_compression_quarterly from kpi_daily_summary.
    Must be called after build_kpi_daily_summary.
    Returns rows_inserted.
    """
    cur.execute(DDL_KPI_COMPRESSION_QUARTERLY)

    cur.execute(DELETE_KPI_COMPRESSION_QUARTERLY)
    print(f"  [compression 1/2] Deleted {cur.rowcount} existing row(s).")

    cur.execute(INSERT_KPI_COMPRESSION_QUARTERLY)
    inserted = cur.rowcount
    print(f"  [compression 2/2] Inserted {inserted} quarterly row(s).")

    return inserted


# ---------------------------------------------------------------------------
# Preview helpers
# ---------------------------------------------------------------------------

def print_daily_preview(conn: sqlite3.Connection, n: int = 5) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT as_of_date, occ_pct, adr, revpar,
               occ_yoy, adr_yoy, revpar_yoy,
               is_occ_80, is_occ_90
        FROM   kpi_daily_summary
        ORDER  BY as_of_date DESC
        LIMIT  ?
        """,
        (n,),
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no rows in kpi_daily_summary)")
        return

    def fmt_pct(v, decimals=1):
        return f"{v:+.{decimals}f}%" if v is not None else "  NULL"

    def fmt_usd(v):
        return f"${v:.2f}" if v is not None else "NULL"

    hdr = (f"  {'date':<12}  {'occ':>7}  {'adr':>8}  {'revpar':>8}"
           f"  {'occ_yoy':>8}  {'adr_yoy%':>9}  {'rvp_yoy%':>9}"
           f"  {'80+':>4}  {'90+':>4}")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for date, occ, adr, rvp, occ_yoy, adr_yoy, rvp_yoy, i80, i90 in rows:
        print(
            f"  {date:<12}  {fmt_pct(occ):>7}  {fmt_usd(adr):>8}  {fmt_usd(rvp):>8}"
            f"  {fmt_pct(occ_yoy, 1):>8}  {fmt_pct(adr_yoy):>9}  {fmt_pct(rvp_yoy):>9}"
            f"  {i80 or 0:>4}  {i90 or 0:>4}"
        )


def print_compression_preview(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT quarter, days_above_80_occ, days_above_90_occ
        FROM   kpi_compression_quarterly
        ORDER  BY quarter DESC
        LIMIT  8
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no rows in kpi_compression_quarterly)")
        return

    hdr = f"  {'quarter':<10}  {'days >80%':>10}  {'days >90%':>10}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for quarter, d80, d90 in rows:
        print(f"  {quarter:<10}  {d80 or 0:>10}  {d90 or 0:>10}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== compute_kpis.py ===\n")
    conn = get_connection()
    try:
        cur = conn.cursor()

        print("── kpi_daily_summary ──────────────────────────────")
        daily_rows = build_kpi_daily_summary(cur)

        print("\n── kpi_compression_quarterly ──────────────────────")
        comp_rows = build_kpi_compression_quarterly(cur)

        conn.commit()

        print(f"\nDone.  daily={daily_rows} rows  compression={comp_rows} quarters\n")

        print("Daily preview (most recent 5 rows):")
        print_daily_preview(conn)

        print("\nCompression preview (most recent 8 quarters):")
        print_compression_preview(conn)

    finally:
        conn.close()
    print()


if __name__ == "__main__":
    main()
