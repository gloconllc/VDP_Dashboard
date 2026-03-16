"""
load_datafy_reports.py
======================
Reads Datafy & GA4 CSV files from data/datafy/ and upserts into
data/analytics.sqlite.  All hardcoded values have been extracted to CSV
files so new monthly data can be added without touching this script.

Monthly workflow
----------------
  1. Export the new Datafy report → save CSV to
       data/datafy/<report_type>/<table>_<period_label>.csv
  2. python scripts/run_pipeline.py  (or python scripts/load_datafy_reports.py)

Folder / file → DB table mapping
---------------------------------
  overview/kpis_*.csv                       → datafy_overview_kpis
  overview/dma_*.csv                        → datafy_overview_dma
  overview/demographics_*.csv               → datafy_overview_demographics
  overview/clusters_*.csv                   → datafy_overview_cluster_visitation
  overview/spending_*.csv                   → datafy_overview_category_spending
  overview/airports_*.csv                   → datafy_overview_airports
  attribution_website/kpis_*.csv            → datafy_attribution_website_kpis
  attribution_website/top_markets_*.csv     → datafy_attribution_website_top_markets
  attribution_website/dma_*.csv             → datafy_attribution_website_dma
  attribution_website/channels_*.csv        → datafy_attribution_website_channels
  attribution_website/clusters_*.csv        → datafy_attribution_website_clusters
  attribution_website/demographics_*.csv    → datafy_attribution_website_demographics
  attribution_media/kpis_*.csv              → datafy_attribution_media_kpis
  attribution_media/top_markets_*.csv       → datafy_attribution_media_top_markets
  social/traffic_*.csv                      → datafy_social_traffic_sources
  social/audience_*.csv                     → datafy_social_audience_overview
  social/pages_*.csv                        → datafy_social_top_pages

Upsert strategy
---------------
  Before inserting each CSV file's rows, delete any existing rows for the
  same (report_period_start, report_period_end) pair.  Re-running the
  pipeline for the same period is therefore safe — it replaces, not
  duplicates.  Running with a new period label adds a new slice of history.
"""

import csv
import glob
import os
import sqlite3
from datetime import datetime

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))   # …/scripts/
PROJECT_ROOT = os.path.dirname(BASE_DIR)                    # project root
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
DATAFY_DIR   = os.path.join(PROJECT_ROOT, "data", "datafy")

NOW = datetime.utcnow().isoformat()

# ─── File prefix → (DB table, subfolder) ─────────────────────────────────────
FILE_TABLE_MAP = {
    "kpis":         {
        "overview":             "datafy_overview_kpis",
        "attribution_website":  "datafy_attribution_website_kpis",
        "attribution_media":    "datafy_attribution_media_kpis",
    },
    "dma":          {
        "overview":             "datafy_overview_dma",
        "attribution_website":  "datafy_attribution_website_dma",
    },
    "demographics": {
        "overview":             "datafy_overview_demographics",
        "attribution_website":  "datafy_attribution_website_demographics",
    },
    "clusters":     {
        "overview":             "datafy_overview_cluster_visitation",
        "attribution_website":  "datafy_attribution_website_clusters",
    },
    "spending":     {"overview":    "datafy_overview_category_spending"},
    "airports":     {"overview":    "datafy_overview_airports"},
    "top_markets":  {
        "attribution_website":  "datafy_attribution_website_top_markets",
        "attribution_media":    "datafy_attribution_media_top_markets",
    },
    "channels":     {"attribution_website": "datafy_attribution_website_channels"},
    "traffic":      {"social":      "datafy_social_traffic_sources"},
    "audience":     {"social":      "datafy_social_audience_overview"},
    "pages":        {"social":      "datafy_social_top_pages"},
}


# ─── Schema DDL ───────────────────────────────────────────────────────────────

SCHEMAS = {

    "datafy_overview_kpis": """
        CREATE TABLE IF NOT EXISTS datafy_overview_kpis (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start             TEXT,
            report_period_end               TEXT,
            compare_period_start            TEXT,
            compare_period_end              TEXT,
            report_title                    TEXT,
            data_source                     TEXT,
            total_trips                     INTEGER,
            avg_length_of_stay_days         REAL,
            avg_los_vs_compare_days         REAL,
            day_trips_pct                   REAL,
            day_trips_vs_compare_pct        REAL,
            overnight_trips_pct             REAL,
            overnight_vs_compare_pct        REAL,
            one_time_visitors_pct           REAL,
            repeat_visitors_pct             REAL,
            in_state_visitor_days_pct       REAL,
            in_state_vd_vs_compare_pct      REAL,
            out_of_state_vd_pct             REAL,
            out_of_state_vd_vs_compare_pct  REAL,
            in_state_spending_pct           REAL,
            out_of_state_spending_pct       REAL,
            locals_pct                      REAL,
            locals_vs_compare_pct           REAL,
            visitors_pct                    REAL,
            visitors_vs_compare_pct         REAL,
            local_spending_pct              REAL,
            visitor_spending_pct            REAL,
            total_trips_vs_compare_pct      REAL,
            loaded_at                       TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_dma": """
        CREATE TABLE IF NOT EXISTS datafy_overview_dma (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start         TEXT,
            report_period_end           TEXT,
            dma                         TEXT,
            visitor_days_share_pct      REAL,
            visitor_days_vs_compare_pct REAL,
            spending_share_pct          REAL,
            avg_spend_usd               REAL,
            avg_length_of_stay_days     REAL,
            trips_share_pct             REAL,
            loaded_at                   TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_demographics": """
        CREATE TABLE IF NOT EXISTS datafy_overview_demographics (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            dimension           TEXT,
            segment             TEXT,
            share_pct           REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_cluster_visitation": """
        CREATE TABLE IF NOT EXISTS datafy_overview_cluster_visitation (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start     TEXT,
            report_period_end       TEXT,
            cluster                 TEXT,
            visitor_days_share_pct  REAL,
            vs_compare_pct          REAL,
            loaded_at               TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_category_spending": """
        CREATE TABLE IF NOT EXISTS datafy_overview_category_spending (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start      TEXT,
            report_period_end        TEXT,
            category                 TEXT,
            spend_share_pct          REAL,
            spending_correlation_pct REAL,
            loaded_at                TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_airports": """
        CREATE TABLE IF NOT EXISTS datafy_overview_airports (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start  TEXT,
            report_period_end    TEXT,
            airport_name         TEXT,
            airport_code         TEXT,
            passengers_share_pct REAL,
            loaded_at            TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_kpis": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_kpis (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start     TEXT,
            report_period_end       TEXT,
            visitation_window_start TEXT,
            visitation_window_end   TEXT,
            report_title            TEXT,
            market_radius_miles     TEXT,
            website_url             TEXT,
            cohort_spend_per_visitor REAL,
            manual_adr              REAL,
            attributable_trips      INTEGER,
            unique_reach            INTEGER,
            est_impact_usd          REAL,
            total_website_sessions  INTEGER,
            website_pageviews       INTEGER,
            avg_time_on_site_sec    INTEGER,
            avg_engagement_rate_pct REAL,
            loaded_at               TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_top_markets": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_top_markets (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start         TEXT,
            report_period_end           TEXT,
            cluster_type                TEXT,
            total_trips                 INTEGER,
            visitor_days_observed       INTEGER,
            est_room_nights             INTEGER,
            est_avg_length_of_stay_days REAL,
            est_impact_usd              REAL,
            top_dma                     TEXT,
            dma_share_of_impact_pct     REAL,
            dma_est_impact_usd          REAL,
            loaded_at                   TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_dma": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_dma (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start         TEXT,
            report_period_end           TEXT,
            dma                         TEXT,
            total_trips                 INTEGER,
            avg_los_destination_days    REAL,
            vs_overall_destination_days REAL,
            loaded_at                   TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_channels": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_channels (
            id                         INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start        TEXT,
            report_period_end          TEXT,
            acquisition_channel        TEXT,
            attribution_rate_pct       REAL,
            sessions                   INTEGER,
            avg_time_on_site_mmss      TEXT,
            engagement_rate_pct        REAL,
            attributable_trips_dest    INTEGER,
            attributable_trips_hotels  INTEGER,
            attributable_trips_resorts INTEGER,
            loaded_at                  TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_clusters": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_clusters (
            id                             INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start            TEXT,
            report_period_end              TEXT,
            area                           TEXT,
            pct_of_total_destination_trips REAL,
            area_type                      TEXT,
            loaded_at                      TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_demographics": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_demographics (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            cluster_type        TEXT,
            dimension           TEXT,
            segment             TEXT,
            share_pct           REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_media_kpis": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_media_kpis (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start      TEXT,
            report_period_end        TEXT,
            visitation_window_start  TEXT,
            visitation_window_end    TEXT,
            report_title             TEXT,
            campaign_name            TEXT,
            market_radius_miles      TEXT,
            program_type             TEXT,
            cohort_spend_per_visitor REAL,
            manual_adr               REAL,
            total_impressions        INTEGER,
            unique_reach             INTEGER,
            attributable_trips       INTEGER,
            est_campaign_impact_usd  REAL,
            roas_description         TEXT,
            total_impact_usd         REAL,
            total_investment_usd     REAL,
            loaded_at                TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_media_top_markets": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_media_top_markets (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start         TEXT,
            report_period_end           TEXT,
            cluster_type                TEXT,
            total_trips                 INTEGER,
            visitor_days_observed       INTEGER,
            est_room_nights             INTEGER,
            est_avg_length_of_stay_days REAL,
            est_impact_usd              REAL,
            top_dma                     TEXT,
            dma_share_of_impact_pct     REAL,
            dma_est_impact_usd          REAL,
            loaded_at                   TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_traffic_sources": """
        CREATE TABLE IF NOT EXISTS datafy_social_traffic_sources (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start       TEXT,
            report_period_end         TEXT,
            source                    TEXT,
            sessions                  INTEGER,
            screen_page_views         INTEGER,
            avg_session_duration_mmss TEXT,
            engagement_rate_pct       REAL,
            loaded_at                 TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_audience_overview": """
        CREATE TABLE IF NOT EXISTS datafy_social_audience_overview (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start       TEXT,
            report_period_end         TEXT,
            audience_name             TEXT,
            sessions                  INTEGER,
            screen_page_views         INTEGER,
            avg_session_duration_mmss TEXT,
            engagement_rate_pct       REAL,
            conversions               INTEGER,
            loaded_at                 TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_top_pages": """
        CREATE TABLE IF NOT EXISTS datafy_social_top_pages (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            page_title          TEXT,
            page_views          INTEGER,
            page_path           TEXT,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _coerce(val):
    """Return None for empty / whitespace strings, otherwise the raw string.
    The DB layer will store as TEXT; SQLite will coerce REAL/INTEGER columns."""
    if val is None:
        return None
    val = str(val).strip()
    return None if val == "" else val


def _get_table_columns(cur, table):
    """Return list of column names for a table (excluding id and loaded_at)."""
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()
            if row[1] not in ("id", "loaded_at")]
    return cols


def load_csv_into_table(cur, csv_path, table):
    """
    Read a CSV file and upsert its rows into `table`.

    Period-level replace strategy:
      1. Read the first data row to get report_period_start/end.
      2. Delete all existing rows for that period pair.
      3. Insert all rows from the CSV.
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print(f"  ⚠  {os.path.basename(csv_path)}: empty file, skipping")
        return 0

    period_start = rows[0].get("report_period_start", "").strip()
    period_end   = rows[0].get("report_period_end",   "").strip()

    if not period_start or not period_end:
        print(f"  ⚠  {os.path.basename(csv_path)}: missing period columns, skipping")
        return 0

    # Remove rows for this period so re-runs are idempotent
    cur.execute(
        f"DELETE FROM {table} WHERE report_period_start=? AND report_period_end=?",
        (period_start, period_end),
    )

    db_cols = _get_table_columns(cur, table)
    placeholders = ",".join("?" * len(db_cols))
    sql = f"INSERT INTO {table} ({','.join(db_cols)}) VALUES ({placeholders})"

    inserted = 0
    for row in rows:
        values = [_coerce(row.get(col)) for col in db_cols]
        cur.execute(sql, values)
        inserted += 1

    return inserted


def ensure_schemas(cur):
    """Create all Datafy tables if they don't already exist.
    Social tables may predate period columns — migrate them if needed."""
    # Migrate social tables that lack report_period_start
    social_tables = [
        "datafy_social_traffic_sources",
        "datafy_social_audience_overview",
        "datafy_social_top_pages",
    ]
    for tbl in social_tables:
        cur.execute(f"PRAGMA table_info({tbl})")
        cols = [r[1] for r in cur.fetchall()]
        if cols and "report_period_start" not in cols:
            cur.execute(f"DROP TABLE {tbl}")

    for ddl in SCHEMAS.values():
        cur.executescript(ddl)


def resolve_table(subfolder, file_prefix):
    """Map (subfolder, file_prefix) → DB table name, or None if unknown."""
    sub_map = FILE_TABLE_MAP.get(file_prefix)
    if sub_map is None:
        return None
    return sub_map.get(subfolder)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    ensure_schemas(cur)
    conn.commit()

    total_files   = 0
    total_rows    = 0
    skipped_files = 0

    # Walk all subfolders under data/datafy/
    for subfolder in sorted(os.listdir(DATAFY_DIR)):
        subfolder_path = os.path.join(DATAFY_DIR, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        csv_files = sorted(glob.glob(os.path.join(subfolder_path, "*.csv")))
        if not csv_files:
            continue

        print(f"Loading {subfolder}/…")

        for csv_path in csv_files:
            filename   = os.path.basename(csv_path)            # e.g. kpis_2025-annual.csv
            name_stem  = filename.rsplit(".", 1)[0]             # e.g. kpis_2025-annual
            # File prefix = everything before the first "_<year>" or "_<period>" token
            # Strategy: split on "_" and take the first segment(s) that form a known prefix
            parts      = name_stem.split("_")
            table      = None
            for i in range(len(parts), 0, -1):
                prefix = "_".join(parts[:i])
                table  = resolve_table(subfolder, prefix)
                if table:
                    break

            if table is None:
                print(f"  ⚠  {filename}: no table mapping found, skipping")
                skipped_files += 1
                continue

            try:
                n = load_csv_into_table(cur, csv_path, table)
                print(f"  ✓  {filename} → {table} ({n} rows)")
                total_files += 1
                total_rows  += n
            except Exception as exc:
                print(f"  ✗  {filename}: {exc}")
                skipped_files += 1

    conn.commit()

    # Log to load_log
    cur.execute("""
        INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at)
        VALUES (?,?,?,?,?)
    """, ("datafy", "mixed", "data/datafy/*/*.csv", total_rows, NOW))
    conn.commit()
    conn.close()

    print(f"\nDatafy load complete: {total_files} files, {total_rows} rows inserted"
          + (f", {skipped_files} skipped" if skipped_files else ""))


if __name__ == "__main__":
    main()
