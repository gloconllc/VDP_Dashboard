"""
load_datafy_reports.py
======================
Reads Datafy & GA4 CSV files from data/datafy/ and upserts into
data/analytics.sqlite.

Two loader paths:
  1. LEGACY — old-style CSVs that include report_period_start / report_period_end
     columns directly in the file (e.g. kpis_2025-annual.csv).  Handled by
     load_csv_into_table() as before.

  2. NEW-FORMAT — April-2026 Datafy export style where period is NOT in the file.
     Period is injected using FOLDER_DEFAULT_PERIODS.  Each file type has its own
     parser function registered in NEW_FILE_HANDLERS.

Re-run safety: every path uses a DELETE + INSERT cycle keyed on
(report_period_start, report_period_end), so running the pipeline twice is safe.

Folder / file → DB table mapping (legacy prefix-based)
--------------------------------------------------------
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

New-format file → DB table mapping (NEW_FILE_HANDLERS)
-------------------------------------------------------
  OverallNumbers_Export*                    → datafy_overview_total_kpis
  TopMarkets_Export* / marketAnalysis-*     → datafy_overview_top_markets
  TopPOIs_Export*                           → datafy_overview_top_pois
  Enhanced Spending Insights-Market*        → datafy_overview_spending_by_market
  Enhanced Spending Insights-Top Markets*   → datafy_overview_spending_by_category
  TopDemographics_Export*                   → datafy_overview_demographics  (new format)
  Attribution Insights Top Polygons*        → datafy_attribution_polygons
  Attribution Insights Website Attribution Groups* → datafy_attribution_website_groups
  Attribution Insights Website Overview Top Markets* → datafy_attribution_website_market_performance
  Attribution Insights Website Top Markets* → datafy_attribution_website_market_performance (same)
  AttributionInsightsOverviewMediaBreakdown* → datafy_attribution_website_media_breakdown
  AttributionInsightsVisitorWebsiteAttributionGroups* → datafy_attribution_website_visitor_markets
  AttributionPeakVisitation*                → datafy_attribution_peak_visitation
  Attribution Insights Media Attribution Groups* → datafy_attribution_media_groups
  Attribution Insights Media Overview Totals* → datafy_attribution_media_kpis (new format)
  Attribution Insights Media Overview Top Markets* → datafy_attribution_media_top_markets (new)
  GoogleAnalytics-DeviceBreakdown*          → datafy_social_device_breakdown
  GoogleAnalytics-NewVsReturning*           → datafy_social_new_vs_returning
  GoogleAnalytics_TopSearches* / GoogleAnalytics-TopSearches* → datafy_social_top_searches
  GoogleAnalytics-Audiences-at-a-Glance*   → datafy_social_ga_overview (skip if Total Users < 100)
  GoogleAnalytics-Acquisition-Channels*    → datafy_social_ga_channels (skip if < 3 rows)
  GoogleAnalytics-GeographicBreakdown*      → datafy_social_geo_breakdown
  GoogleAnalytics-MostPopularPages*         → datafy_social_top_pages  (new format)

SKIP stems (explicitly ignored)
---------------------------------
  AttributionInsightsVisitorWebsiteAttributionGroups_Export (1)
  AttributionInsightsVisitorWebsiteAttributionGroups_Export (2)
  AttributionInsightsOverviewMediaBreakdown_Export (1)
  Attribution Insights Media Overview Totals_Export (1)
"""

from __future__ import annotations

import csv
import glob
import os
import re
import sqlite3
from datetime import datetime
from typing import Optional

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))   # …/scripts/
PROJECT_ROOT = os.path.dirname(BASE_DIR)                    # project root
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
DATAFY_DIR   = os.path.join(PROJECT_ROOT, "data", "datafy")

NOW = datetime.utcnow().isoformat()

# ─── Period injection for new-format files (no period columns in CSV) ─────────
FOLDER_DEFAULT_PERIODS: dict[str, tuple[str, str]] = {
    "overview":             ("2025-01-01", "2025-12-31"),
    "attribution_website":  ("2025-07-01", "2025-09-30"),
    "attribution_media":    ("2025-06-01", "2026-03-31"),
    "social":               ("2025-01-01", "2025-12-31"),
}

# ─── Stems to skip entirely ───────────────────────────────────────────────────
# Use the full file stem (filename without .csv extension), case-insensitive match.
SKIP_STEMS: set[str] = {
    "attributioninsightsvisitorwebsiteattributiongroups_export (1)",
    "attributioninsightsvisitorwebsiteattributiongroups_export (2)",
    "attributioninsightsoverviewmediabreakdown_export (1)",
    "attribution insights media overview totals_export (1)",
}

# ─── Legacy file prefix → (DB table, subfolder) ───────────────────────────────
FILE_TABLE_MAP = {
    "kpis": {
        "overview":             "datafy_overview_kpis",
        "attribution_website":  "datafy_attribution_website_kpis",
        "attribution_media":    "datafy_attribution_media_kpis",
    },
    "dma": {
        "overview":             "datafy_overview_dma",
        "attribution_website":  "datafy_attribution_website_dma",
    },
    "demographics": {
        "overview":             "datafy_overview_demographics",
        "attribution_website":  "datafy_attribution_website_demographics",
    },
    "clusters": {
        "overview":             "datafy_overview_cluster_visitation",
        "attribution_website":  "datafy_attribution_website_clusters",
    },
    "spending":    {"overview":           "datafy_overview_category_spending"},
    "airports":    {"overview":           "datafy_overview_airports"},
    "top_markets": {
        "attribution_website":  "datafy_attribution_website_top_markets",
        "attribution_media":    "datafy_attribution_media_top_markets",
    },
    "channels":    {"attribution_website": "datafy_attribution_website_channels"},
    "traffic":     {"social":             "datafy_social_traffic_sources"},
    "audience":    {"social":             "datafy_social_audience_overview"},
    "pages":       {"social":             "datafy_social_top_pages"},
}


# ─── Schema DDL ───────────────────────────────────────────────────────────────

SCHEMAS: dict[str, str] = {

    # ── Legacy tables ──────────────────────────────────────────────────────────

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
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start      TEXT,
            report_period_end        TEXT,
            visitation_window_start  TEXT,
            visitation_window_end    TEXT,
            report_title             TEXT,
            market_radius_miles      TEXT,
            website_url              TEXT,
            cohort_spend_per_visitor REAL,
            manual_adr               REAL,
            attributable_trips       INTEGER,
            unique_reach             INTEGER,
            est_impact_usd           REAL,
            total_website_sessions   INTEGER,
            website_pageviews        INTEGER,
            avg_time_on_site_sec     INTEGER,
            avg_engagement_rate_pct  REAL,
            loaded_at                TEXT DEFAULT (datetime('now'))
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

    # ── New tables (April 2026 Datafy export format) ──────────────────────────

    "datafy_overview_total_kpis": """
        CREATE TABLE IF NOT EXISTS datafy_overview_total_kpis (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            total_trips         REAL,
            visitor_days        REAL,
            avg_los_days        REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_top_markets": """
        CREATE TABLE IF NOT EXISTS datafy_overview_top_markets (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            dma                 TEXT,
            trips_share_pct     REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_top_pois": """
        CREATE TABLE IF NOT EXISTS datafy_overview_top_pois (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start    TEXT,
            report_period_end      TEXT,
            cluster                TEXT,
            visitor_days_share_pct REAL,
            loaded_at              TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_spending_by_market": """
        CREATE TABLE IF NOT EXISTS datafy_overview_spending_by_market (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            dma                 TEXT,
            spend_share_pct     REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_overview_spending_by_category": """
        CREATE TABLE IF NOT EXISTS datafy_overview_spending_by_category (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            category            TEXT,
            spend_share_pct     REAL,
            avg_spend_usd       REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_polygons": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_polygons (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start             TEXT,
            report_period_end               TEXT,
            cluster                         TEXT,
            share_of_destination_trips_pct  REAL,
            loaded_at                       TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_groups": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_groups (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            attribution_group   TEXT,
            trips               INTEGER,
            visitor_days        INTEGER,
            avg_los_days        REAL,
            est_impact_usd      REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_visitor_markets": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_visitor_markets (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start  TEXT,
            report_period_end    TEXT,
            market               TEXT,
            share_of_impact      REAL,
            avg_spend_per_visitor REAL,
            avg_spend_per_day    REAL,
            est_impact_usd       REAL,
            share_of_visitor_days REAL,
            share_of_trips       REAL,
            visitor_days         REAL,
            trips                REAL,
            rank                 INTEGER,
            loaded_at            TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_market_performance": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_market_performance (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            dma                 TEXT,
            trips               REAL,
            visitor_days        REAL,
            avg_spend_per_visitor REAL,
            avg_spend_per_day   REAL,
            est_impact_usd      REAL,
            avg_los_days        REAL,
            rank                INTEGER,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_peak_visitation": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_peak_visitation (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start    TEXT,
            report_period_end      TEXT,
            day_of_week            TEXT,
            month                  TEXT,
            vs_max_visitor_days_pct REAL,
            loaded_at              TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_media_groups": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_media_groups (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            attribution_group   TEXT,
            trips               INTEGER,
            visitor_days        INTEGER,
            avg_los_days        REAL,
            est_impact_usd      REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_attribution_website_media_breakdown": """
        CREATE TABLE IF NOT EXISTS datafy_attribution_website_media_breakdown (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start       TEXT,
            report_period_end         TEXT,
            channel                   TEXT,
            est_destination_impact_usd REAL,
            loaded_at                 TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_device_breakdown": """
        CREATE TABLE IF NOT EXISTS datafy_social_device_breakdown (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            device              TEXT,
            users               INTEGER,
            new_users           INTEGER,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_new_vs_returning": """
        CREATE TABLE IF NOT EXISTS datafy_social_new_vs_returning (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            user_type           TEXT,
            share_pct           REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_top_searches": """
        CREATE TABLE IF NOT EXISTS datafy_social_top_searches (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            search_term         TEXT,
            total_users         INTEGER,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_geo_breakdown": """
        CREATE TABLE IF NOT EXISTS datafy_social_geo_breakdown (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            location            TEXT,
            users               INTEGER,
            new_users           INTEGER,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_ga_overview": """
        CREATE TABLE IF NOT EXISTS datafy_social_ga_overview (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start          TEXT,
            report_period_end            TEXT,
            total_users                  INTEGER,
            new_users                    INTEGER,
            sessions                     INTEGER,
            screen_page_views            INTEGER,
            sessions_per_user            REAL,
            screen_page_views_per_session REAL,
            avg_session_duration_mmss    TEXT,
            engagement_rate_pct          REAL,
            loaded_at                    TEXT DEFAULT (datetime('now'))
        );""",

    "datafy_social_ga_channels": """
        CREATE TABLE IF NOT EXISTS datafy_social_ga_channels (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period_start TEXT,
            report_period_end   TEXT,
            channel             TEXT,
            share_of_users_pct  REAL,
            loaded_at           TEXT DEFAULT (datetime('now'))
        );""",
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _coerce(val) -> Optional[str]:
    """Return None for empty / whitespace strings, otherwise the raw string."""
    if val is None:
        return None
    val = str(val).strip()
    return None if val == "" else val


def _clean_money(val) -> Optional[float]:
    """Strip $, commas → float.  Returns None on failure."""
    if val is None:
        return None
    s = str(val).strip().replace("$", "").replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _clean_pct(val) -> Optional[float]:
    """Strip % → float (value is stored as-is, e.g. 22.15 for 22.15%)."""
    if val is None:
        return None
    s = str(val).strip().rstrip("%").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _clean_days(val) -> Optional[float]:
    """Strip ' Days' suffix → float.  Returns None on failure."""
    if val is None:
        return None
    s = re.sub(r"[Dd]ays?", "", str(val)).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _clean_num(val) -> Optional[float]:
    """Strip commas and whitespace → float.  Returns None on failure."""
    if val is None:
        return None
    s = str(val).strip().replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _get_table_columns(cur, table: str) -> list:
    """Return column names for a table excluding id and loaded_at."""
    cur.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()
            if row[1] not in ("id", "loaded_at")]


def _extract_period(filename: str, subfolder: str) -> tuple:
    """
    Try to parse DD-MM-YYYY_to_DD-MM-YYYY from filename.
    Falls back to FOLDER_DEFAULT_PERIODS[subfolder].
    """
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})_to_(\d{2})-(\d{2})-(\d{4})", filename)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        start = f"{y1}-{m1}-{d1}"
        end   = f"{y2}-{m2}-{d2}"
        return start, end
    return FOLDER_DEFAULT_PERIODS.get(subfolder, ("2025-01-01", "2025-12-31"))


def _delete_period(cur, table: str, period_start: str, period_end: str) -> None:
    cur.execute(
        f"DELETE FROM {table} WHERE report_period_start=? AND report_period_end=?",
        (period_start, period_end),
    )


def _insert_rows(cur, table: str, rows: list) -> None:
    """Insert a list of dicts into table (keys must match column names)."""
    if not rows:
        return
    cols = list(rows[0].keys())
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    for row in rows:
        cur.execute(sql, list(row.values()))


def load_csv_into_table(cur, csv_path: str, table: str) -> int:
    """
    LEGACY loader: CSV must contain report_period_start / report_period_end columns.
    Performs DELETE + INSERT for the period found in the CSV.
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print(f"  WARN  {os.path.basename(csv_path)}: empty file, skipping")
        return 0

    period_start = rows[0].get("report_period_start", "").strip()
    period_end   = rows[0].get("report_period_end",   "").strip()

    if not period_start or not period_end:
        print(f"  WARN  {os.path.basename(csv_path)}: missing period columns, skipping")
        return 0

    _delete_period(cur, table, period_start, period_end)

    db_cols = _get_table_columns(cur, table)
    placeholders = ",".join("?" * len(db_cols))
    sql = f"INSERT INTO {table} ({','.join(db_cols)}) VALUES ({placeholders})"

    inserted = 0
    for row in rows:
        values = [_coerce(row.get(col)) for col in db_cols]
        cur.execute(sql, values)
        inserted += 1

    return inserted


def ensure_schemas(cur) -> None:
    """Create all Datafy tables; migrate legacy social tables that lack period cols."""
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


def resolve_table(subfolder: str, file_prefix: str) -> str | None:
    """Map (subfolder, file_prefix) → DB table name, or None if unknown."""
    sub_map = FILE_TABLE_MAP.get(file_prefix)
    if sub_map is None:
        return None
    return sub_map.get(subfolder)


# ─── New-format parser functions ──────────────────────────────────────────────

def parse_overall_numbers(csv_path: str, cur, ps: str, pe: str) -> int:
    """OverallNumbers_Export*.csv → datafy_overview_total_kpis"""
    table = "datafy_overview_total_kpis"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data: dict[str, float | None] = {}
        for row in reader:
            metric = str(row.get("Metric", "")).strip()
            raw    = str(row.get("value", "")).strip()
            # Strip trailing units like "Trips" / "Days"
            val_str = re.sub(r"[A-Za-z]+$", "", raw).strip()
            try:
                val = float(val_str)
            except ValueError:
                val = None
            key = metric.lower().replace(" ", "_")
            if "total_trips" in key or key == "total_trips":
                data["total_trips"] = val
            elif "visitor_days" in key:
                data["visitor_days"] = val
            elif "avg_length" in key or "avg_los" in key or "length_of_stay" in key:
                data["avg_los_days"] = val

    rows = [{
        "report_period_start": ps,
        "report_period_end":   pe,
        "total_trips":         data.get("total_trips"),
        "visitor_days":        data.get("visitor_days"),
        "avg_los_days":        data.get("avg_los_days"),
    }]
    _insert_rows(cur, table, rows)
    return len(rows)


def parse_top_markets(csv_path: str, cur, ps: str, pe: str) -> int:
    """TopMarkets_Export*.csv / marketAnalysis-*.csv → datafy_overview_top_markets"""
    table = "datafy_overview_top_markets"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    # Both files share the same columns: DMA, Trips  OR  DMA, Share of Total Trips
    rows_out = []
    for row in rows_in:
        dma = str(row.get("DMA", "")).strip()
        if not dma or dma.lower() == "all others":
            continue
        # Column name varies across files
        raw_pct = row.get("Trips") or row.get("Share of Total Trips") or ""
        pct = _clean_pct(str(raw_pct).strip())
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "dma":                 dma,
            "trips_share_pct":     pct,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_top_pois(csv_path: str, cur, ps: str, pe: str) -> int:
    """TopPOIs_Export*.csv → datafy_overview_top_pois"""
    table = "datafy_overview_top_pois"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        cluster = str(row.get("Cluster", "")).strip()
        if not cluster:
            continue
        share_raw = row.get("Share of Visitor Days", "")
        try:
            share = float(str(share_raw).strip())
        except ValueError:
            share = None
        rows_out.append({
            "report_period_start":    ps,
            "report_period_end":      pe,
            "cluster":                cluster,
            "visitor_days_share_pct": share,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_spending_by_market(csv_path: str, cur, ps: str, pe: str) -> int:
    """Enhanced Spending Insights-Market Visitation*.csv → datafy_overview_spending_by_market"""
    table = "datafy_overview_spending_by_market"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        dma = str(row.get("DMA", "")).strip()
        if not dma:
            continue
        raw_share = row.get("Share of Spend %", "")
        try:
            share = float(str(raw_share).strip())
        except ValueError:
            share = None
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "dma":                 dma,
            "spend_share_pct":     share,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_spending_by_category(csv_path: str, cur, ps: str, pe: str) -> int:
    """Enhanced Spending Insights-Top Markets*.csv → datafy_overview_spending_by_category"""
    table = "datafy_overview_spending_by_category"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        cat = str(row.get("Category", "")).strip()
        if not cat:
            continue
        raw_share = row.get("Share of Spend %", "")
        raw_avg   = row.get("Avg. Spend", "")
        try:
            share = float(str(raw_share).strip())
        except ValueError:
            share = None
        avg_spend = _clean_money(raw_avg)
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "category":            cat,
            "spend_share_pct":     share,
            "avg_spend_usd":       avg_spend,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_top_demographics(csv_path: str, cur, ps: str, pe: str) -> int:
    """TopDemographics_Export*.csv → datafy_overview_demographics (new format)"""
    table = "datafy_overview_demographics"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        dimension = str(row.get("Demographic", "")).strip()
        segment   = str(row.get("Group/Level", "")).strip()
        if not dimension and not segment:
            continue
        share = _clean_pct(row.get("Share of Demographic", ""))
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "dimension":           dimension,
            "segment":             segment,
            "share_pct":           share,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_attribution_polygons(csv_path: str, cur, ps: str, pe: str) -> int:
    """Attribution Insights Top Polygons*.csv → datafy_attribution_polygons"""
    table = "datafy_attribution_polygons"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        cluster = str(row.get("Cluster", "")).strip()
        if not cluster:
            continue
        share = _clean_pct(row.get("Share of Total Destination Trips", ""))
        rows_out.append({
            "report_period_start":            ps,
            "report_period_end":              pe,
            "cluster":                        cluster,
            "share_of_destination_trips_pct": share,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_website_attribution_groups(csv_path: str, cur, ps: str, pe: str) -> int:
    """Attribution Insights Website Attribution Groups*.csv → datafy_attribution_website_groups"""
    table = "datafy_attribution_website_groups"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        group = str(row.get("Attribution Group", "")).strip()
        if not group:
            continue
        trips   = _clean_num(row.get("Trips", ""))
        vis_days = _clean_num(row.get("Visitor Days", ""))
        los     = _clean_days(row.get("Est. Average Length of Stay", ""))
        impact  = _clean_money(row.get("Estimated Impact", ""))
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "attribution_group":   group,
            "trips":               int(trips) if trips is not None else None,
            "visitor_days":        int(vis_days) if vis_days is not None else None,
            "avg_los_days":        los,
            "est_impact_usd":      impact,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_website_visitor_markets(csv_path: str, cur, ps: str, pe: str) -> int:
    """AttributionInsightsVisitorWebsiteAttributionGroups_Export.csv (base only)
       → datafy_attribution_website_visitor_markets"""
    table = "datafy_attribution_website_visitor_markets"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        market = str(row.get("market", "")).strip()
        if not market:
            continue
        rank_raw = row.get("originalRank", "")
        try:
            rank = int(float(str(rank_raw).strip()))
        except (ValueError, TypeError):
            rank = None
        rows_out.append({
            "report_period_start":   ps,
            "report_period_end":     pe,
            "market":                market,
            "share_of_impact":       _clean_num(row.get("shareOfImpact", "")),
            "avg_spend_per_visitor": _clean_num(row.get("avg_spend_per_visitor", "")),
            "avg_spend_per_day":     _clean_num(row.get("avg_spend_per_day", "")),
            "est_impact_usd":        _clean_num(row.get("estImpact", "")),
            "share_of_visitor_days": _clean_num(row.get("shareOfVisitorDays", "")),
            "share_of_trips":        _clean_num(row.get("shareOfTrips", "")),
            "visitor_days":          _clean_num(row.get("visitorDays", "")),
            "trips":                 _clean_num(row.get("trips", "")),
            "rank":                  rank,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_website_market_performance(csv_path: str, cur, ps: str, pe: str) -> int:
    """
    Attribution Insights Website Overview Top Markets*.csv
    Attribution Insights Website Top Markets*.csv
    → datafy_attribution_website_market_performance
    Handles both column layouts (market/DMA column names differ).
    """
    table = "datafy_attribution_website_market_performance"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        # DMA column is "market" in the rich file, "market" in overview file too
        dma = str(row.get("market", row.get("DMA", ""))).strip()
        if not dma:
            continue
        rank_raw = row.get("originalRank", "")
        try:
            rank = int(float(str(rank_raw).strip()))
        except (ValueError, TypeError):
            rank = None
        # avg_trip_length / avg_length_of_stay both appear across files
        los_raw = row.get("avg_length_of_stay", row.get("avg_trip_length", ""))
        los = _clean_days(los_raw)

        impact_raw = row.get("est_impact", row.get("est_impact_[Destination]", ""))
        impact = _clean_money(impact_raw)

        rows_out.append({
            "report_period_start":   ps,
            "report_period_end":     pe,
            "dma":                   dma,
            "trips":                 _clean_num(row.get("trips", row.get("total_trips_[Destination]", ""))),
            "visitor_days":          _clean_num(row.get("visitor_days", row.get("visitor_days_[Destination]", ""))),
            "avg_spend_per_visitor": _clean_money(row.get("avg_spend_per_visitor", "")),
            "avg_spend_per_day":     _clean_money(row.get("avg_spend_per_day", "")),
            "est_impact_usd":        impact,
            "avg_los_days":          los,
            "rank":                  rank,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_media_breakdown(csv_path: str, cur, ps: str, pe: str) -> int:
    """AttributionInsightsOverviewMediaBreakdown_Export.csv (base)
       → datafy_attribution_website_media_breakdown"""
    table = "datafy_attribution_website_media_breakdown"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        channel = str(row.get("Acquisition/Channel", "")).strip()
        if not channel:
            continue
        impact = _clean_money(row.get("Est. Destination Impact", ""))
        rows_out.append({
            "report_period_start":         ps,
            "report_period_end":           pe,
            "channel":                     channel,
            "est_destination_impact_usd":  impact,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_peak_visitation(csv_path: str, cur, ps: str, pe: str) -> int:
    """AttributionPeakVisitation_Export*.csv → datafy_attribution_peak_visitation"""
    table = "datafy_attribution_peak_visitation"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        day   = str(row.get("Day", "")).strip()
        month = str(row.get("Month", "")).strip()
        if not day and not month:
            continue
        vs_max = _clean_pct(row.get("Compared to Max Visitor Days Value", ""))
        rows_out.append({
            "report_period_start":    ps,
            "report_period_end":      pe,
            "day_of_week":            day,
            "month":                  month,
            "vs_max_visitor_days_pct": vs_max,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_media_attribution_groups(csv_path: str, cur, ps: str, pe: str) -> int:
    """Attribution Insights Media Attribution Groups*.csv → datafy_attribution_media_groups"""
    table = "datafy_attribution_media_groups"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        group = str(row.get("Attribution Group", "")).strip()
        if not group:
            continue
        trips    = _clean_num(row.get("Trips", ""))
        vis_days = _clean_num(row.get("Visitor Days", ""))
        los      = _clean_days(row.get("Est. Average Length of Stay", ""))
        impact   = _clean_money(row.get("Est. Impact", ""))
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "attribution_group":   group,
            "trips":               int(trips) if trips is not None else None,
            "visitor_days":        int(vis_days) if vis_days is not None else None,
            "avg_los_days":        los,
            "est_impact_usd":      impact,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_media_kpis_new(csv_path: str, cur, ps: str, pe: str) -> int:
    """Attribution Insights Media Overview Totals*.csv (base only)
       → datafy_attribution_media_kpis (new-format insert for impressions/reach/trips/impact)"""
    table = "datafy_attribution_media_kpis"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    if not rows_in:
        return 0

    row = rows_in[0]
    impressions = _clean_num(row.get("Total Impressions", ""))
    reach       = _clean_num(row.get("Unique Reach", ""))
    attr_trips  = _clean_num(row.get("Attributable Trips", ""))
    impact      = _clean_money(row.get("Est. Campaign Impact", ""))

    db_row = {
        "report_period_start":   ps,
        "report_period_end":     pe,
        "visitation_window_start": None,
        "visitation_window_end":   None,
        "report_title":            None,
        "campaign_name":           None,
        "market_radius_miles":     None,
        "program_type":            None,
        "cohort_spend_per_visitor": None,
        "manual_adr":              None,
        "total_impressions":       int(impressions) if impressions is not None else None,
        "unique_reach":            int(reach) if reach is not None else None,
        "attributable_trips":      int(attr_trips) if attr_trips is not None else None,
        "est_campaign_impact_usd": impact,
        "roas_description":        None,
        "total_impact_usd":        impact,
        "total_investment_usd":    None,
    }
    _insert_rows(cur, table, [db_row])
    return 1


def parse_media_top_markets_new(csv_path: str, cur, ps: str, pe: str) -> int:
    """Attribution Insights Media Overview Top Markets*.csv
       → datafy_attribution_media_top_markets (new rich multi-cluster format)"""
    table = "datafy_attribution_media_top_markets"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        # Column is "market" in newer exports, "DMA" in some older variants
        market = str(row.get("market", row.get("DMA", ""))).strip()
        if not market:
            continue
        # Use Destination cluster as primary; fill legacy fields with None
        trips_raw = _clean_num(row.get("total_trips_[Destination]", row.get("trips", "")))
        vdays_raw = _clean_num(row.get("visitor_days_[Destination]", row.get("visitor_days", "")))
        impact_val = _clean_money(row.get("est_impact_[Destination]", row.get("est_impact", "")))
        rows_out.append({
            "report_period_start":       ps,
            "report_period_end":         pe,
            "cluster_type":              "Destination",
            "total_trips":               int(float(trips_raw)) if trips_raw is not None else None,
            "visitor_days_observed":     int(float(vdays_raw)) if vdays_raw is not None else None,
            "est_room_nights":           None,
            "est_avg_length_of_stay_days": _clean_days(row.get("avg_trip_length", row.get("avg_length_of_stay", ""))),
            "est_impact_usd":            impact_val,
            "top_dma":                   market,
            "dma_share_of_impact_pct":   _clean_num(row.get("share_of_visitor_days", "")),
            "dma_est_impact_usd":        impact_val,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_device_breakdown(csv_path: str, cur, ps: str, pe: str) -> int:
    """GoogleAnalytics-DeviceBreakdown*.csv → datafy_social_device_breakdown"""
    table = "datafy_social_device_breakdown"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        device = str(row.get("Device", "")).strip()
        if not device:
            continue
        users     = _clean_num(row.get("Users", ""))
        new_users = _clean_num(row.get("New Users", ""))
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "device":              device,
            "users":               int(users) if users is not None else None,
            "new_users":           int(new_users) if new_users is not None else None,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_new_vs_returning(csv_path: str, cur, ps: str, pe: str) -> int:
    """GoogleAnalytics-NewVsReturning*.csv → datafy_social_new_vs_returning"""
    table = "datafy_social_new_vs_returning"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        user_type = str(row.get("User Type", "")).strip()
        if not user_type:
            continue
        share = _clean_pct(row.get("Share of Users", ""))
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "user_type":           user_type,
            "share_pct":           share,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_top_searches(csv_path: str, cur, ps: str, pe: str) -> int:
    """GoogleAnalytics_TopSearches*.csv → datafy_social_top_searches"""
    table = "datafy_social_top_searches"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        term = str(row.get("Search Term", "")).strip()
        if not term:
            continue
        users = _clean_num(row.get("Total Users", ""))
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "search_term":         term,
            "total_users":         int(users) if users is not None else None,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_ga_overview(csv_path: str, cur, ps: str, pe: str) -> int:
    """GoogleAnalytics-Audiences-at-a-Glance*.csv → datafy_social_ga_overview
       Skips files where Total Users < 100 (test data guard)."""
    table = "datafy_social_ga_overview"

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    # Build pivot: Metric → Value
    pivot: dict[str, str] = {}
    for row in rows_in:
        metric = str(row.get("Metric", "")).strip()
        value  = str(row.get("Value", "")).strip()
        if metric:
            pivot[metric] = value

    # Guard: skip tiny test exports
    total_users_raw = pivot.get("Total Users", "0")
    total_users_num = _clean_num(total_users_raw)
    if total_users_num is None or total_users_num < 100:
        print(f"  SKIP  {os.path.basename(csv_path)}: Total Users={total_users_raw} < 100 (test data)")
        return 0

    _delete_period(cur, table, ps, pe)

    def _parse_engagement_rate(val: str) -> float | None:
        """Handles '51.81%' format."""
        return _clean_pct(val)

    sessions_per_user = _clean_num(pivot.get("Sessions Per User", ""))
    spv = _clean_num(pivot.get("Screen Page Views Per Session", ""))

    db_row = {
        "report_period_start":           ps,
        "report_period_end":             pe,
        "total_users":                   int(total_users_num),
        "new_users":                     int(v) if (v := _clean_num(pivot.get("New Users", ""))) is not None else None,
        "sessions":                      int(v) if (v := _clean_num(pivot.get("Sessions", ""))) is not None else None,
        "screen_page_views":             int(v) if (v := _clean_num(pivot.get("Screen Page Views", ""))) is not None else None,
        "sessions_per_user":             sessions_per_user,
        "screen_page_views_per_session": spv,
        "avg_session_duration_mmss":     pivot.get("Average Session Duration", None),
        "engagement_rate_pct":           _parse_engagement_rate(pivot.get("Engagement Rate", "")),
    }
    _insert_rows(cur, table, [db_row])
    return 1


def parse_ga_channels(csv_path: str, cur, ps: str, pe: str) -> int:
    """GoogleAnalytics-Acquisition-Channels*.csv → datafy_social_ga_channels
       Skips files with fewer than 3 data rows (incomplete exports)."""
    table = "datafy_social_ga_channels"

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    if len(rows_in) < 3:
        print(f"  SKIP  {os.path.basename(csv_path)}: only {len(rows_in)} rows (incomplete export)")
        return 0

    _delete_period(cur, table, ps, pe)
    rows_out = []
    for row in rows_in:
        channel = str(row.get("Acquisition Channel", "")).strip()
        if not channel:
            continue
        share = _clean_pct(row.get("Share of Users", ""))
        rows_out.append({
            "report_period_start":  ps,
            "report_period_end":    pe,
            "channel":              channel,
            "share_of_users_pct":   share,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_geo_breakdown(csv_path: str, cur, ps: str, pe: str) -> int:
    """GoogleAnalytics-GeographicBreakdown*.csv → datafy_social_geo_breakdown"""
    table = "datafy_social_geo_breakdown"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        location = str(row.get("Location", "")).strip()
        if not location:
            continue
        users     = _clean_num(row.get("Users", ""))
        new_users = _clean_num(row.get("New Users", ""))
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "location":            location,
            "users":               int(users) if users is not None else None,
            "new_users":           int(new_users) if new_users is not None else None,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


def parse_popular_pages_new(csv_path: str, cur, ps: str, pe: str) -> int:
    """GoogleAnalytics-MostPopularPages*.csv → datafy_social_top_pages (new format)"""
    table = "datafy_social_top_pages"
    _delete_period(cur, table, ps, pe)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_in = list(reader)

    rows_out = []
    for row in rows_in:
        title = str(row.get("Page Title", "")).strip()
        if not title:
            continue
        views = _clean_num(row.get("Page Views", ""))
        path  = str(row.get("Page Path", "")).strip() or None
        rows_out.append({
            "report_period_start": ps,
            "report_period_end":   pe,
            "page_title":          title,
            "page_views":          int(views) if views is not None else None,
            "page_path":           path,
        })

    _insert_rows(cur, table, rows_out)
    return len(rows_out)


# ─── New-file handler registry ────────────────────────────────────────────────
# Format: (stem_fragment_lower, table_name, parser_func)
# Matched via  stem_fragment in filename_lower  (case-insensitive).
# More-specific patterns listed FIRST to prevent shorter ones from matching first.

NEW_FILE_HANDLERS: list[tuple[str, str, object]] = [
    # ── Overview ──────────────────────────────────────────────────────────────
    ("overallnumbers_export",                                  "datafy_overview_total_kpis",                     parse_overall_numbers),
    ("enhanced spending insights-top markets",                 "datafy_overview_spending_by_category",           parse_spending_by_category),
    ("enhanced spending insights-market visitation",           "datafy_overview_spending_by_market",             parse_spending_by_market),
    ("topdemographics_export",                                 "datafy_overview_demographics",                   parse_top_demographics),
    ("topmarkets_export",                                      "datafy_overview_top_markets",                    parse_top_markets),
    ("marketanalysis-marketanalysistopmarkets_export",         "datafy_overview_top_markets",                    parse_top_markets),
    ("toppois_export",                                         "datafy_overview_top_pois",                       parse_top_pois),

    # ── Attribution Website ───────────────────────────────────────────────────
    ("attribution insights top polygons",                      "datafy_attribution_polygons",                    parse_attribution_polygons),
    ("attribution insights website attribution groups",        "datafy_attribution_website_groups",              parse_website_attribution_groups),
    ("attribution insights website overview top markets",      "datafy_attribution_website_market_performance",  parse_website_market_performance),
    ("attribution insights website top markets",               "datafy_attribution_website_market_performance",  parse_website_market_performance),
    ("attributioninsightsoverviewmediabreakdown_export",       "datafy_attribution_website_media_breakdown",     parse_media_breakdown),
    ("attributioninsightsvisitorwebsiteattributiongroups_export", "datafy_attribution_website_visitor_markets",  parse_website_visitor_markets),
    ("attributionpeakvisitation_export",                       "datafy_attribution_peak_visitation",             parse_peak_visitation),

    # ── Attribution Media ─────────────────────────────────────────────────────
    ("attribution insights media attribution groups",          "datafy_attribution_media_groups",                parse_media_attribution_groups),
    ("attribution insights media overview totals",             "datafy_attribution_media_kpis",                  parse_media_kpis_new),
    ("attribution insights media overview top markets",        "datafy_attribution_media_top_markets",           parse_media_top_markets_new),
    ("attribution insights media top markets",                 "datafy_attribution_media_top_markets",           parse_media_top_markets_new),

    # ── Social ────────────────────────────────────────────────────────────────
    ("googleanalytics-devicebreakdown",                        "datafy_social_device_breakdown",                 parse_device_breakdown),
    ("googleanalytics-newvsreturning",                         "datafy_social_new_vs_returning",                 parse_new_vs_returning),
    ("googleanalytics_topsearches",                            "datafy_social_top_searches",                     parse_top_searches),
    ("googleanalytics-audiences-at-a-glance",                  "datafy_social_ga_overview",                      parse_ga_overview),
    ("googleanalytics-acquisition-channels",                   "datafy_social_ga_channels",                      parse_ga_channels),
    ("googleanalytics-geographicbreakdown",                    "datafy_social_geo_breakdown",                    parse_geo_breakdown),
    ("googleanalytics-mostpopularpages",                       "datafy_social_top_pages",                        parse_popular_pages_new),
]


def find_new_handler(filename_lower: str):
    """Return (table, parser_func) for filename, or (None, None) if no match."""
    for stem_frag, table, func in NEW_FILE_HANDLERS:
        if stem_frag in filename_lower:
            return table, func
    return None, None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    ensure_schemas(cur)
    conn.commit()

    total_files   = 0
    total_rows    = 0
    skipped_files = 0

    for subfolder in sorted(os.listdir(DATAFY_DIR)):
        subfolder_path = os.path.join(DATAFY_DIR, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        csv_files = sorted(glob.glob(os.path.join(subfolder_path, "*.csv")))
        if not csv_files:
            continue

        print(f"Loading {subfolder}/…")

        for csv_path in csv_files:
            filename      = os.path.basename(csv_path)
            name_stem     = filename.rsplit(".", 1)[0]
            name_stem_lc  = name_stem.lower()

            # ── 1. Explicit skip list ─────────────────────────────────────────
            if name_stem_lc in SKIP_STEMS:
                print(f"  SKIP  {filename} (in skip list)")
                skipped_files += 1
                continue

            # ── 2. Try new-format handlers ────────────────────────────────────
            table, parser = find_new_handler(name_stem_lc)
            if table is not None:
                ps, pe = _extract_period(filename, subfolder)
                try:
                    n = parser(csv_path, cur, ps, pe)
                    if n > 0:
                        print(f"  OK    {filename} → {table} ({n} rows)")
                        total_files += 1
                        total_rows  += n
                    else:
                        # Parser already printed its own SKIP/WARN; just count as skipped
                        skipped_files += 1
                except Exception as exc:
                    print(f"  ERR   {filename}: {exc}")
                    skipped_files += 1
                continue

            # ── 3. Legacy FILE_TABLE_MAP (period columns in CSV) ──────────────
            parts  = name_stem.split("_")
            legacy_table = None
            for i in range(len(parts), 0, -1):
                prefix = "_".join(parts[:i])
                legacy_table = resolve_table(subfolder, prefix)
                if legacy_table:
                    break

            if legacy_table is None:
                print(f"  WARN  {filename}: no table mapping found, skipping")
                skipped_files += 1
                continue

            try:
                n = load_csv_into_table(cur, csv_path, legacy_table)
                print(f"  OK    {filename} → {legacy_table} ({n} rows)")
                total_files += 1
                total_rows  += n
            except Exception as exc:
                print(f"  ERR   {filename}: {exc}")
                skipped_files += 1

    conn.commit()

    # Audit trail
    cur.execute("""
        INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at)
        VALUES (?,?,?,?,?)
    """, ("datafy", "mixed", "data/datafy/*/*.csv", total_rows, NOW))
    conn.commit()
    conn.close()

    print(
        f"\nDatafy load complete: {total_files} files, {total_rows} rows inserted"
        + (f", {skipped_files} skipped" if skipped_files else "")
    )


if __name__ == "__main__":
    main()
