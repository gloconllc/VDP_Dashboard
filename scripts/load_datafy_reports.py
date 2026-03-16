"""
load_datafy_reports.py
======================
Parses Datafy PDF reports + Google Analytics CSVs and loads structured data
into data/analytics.sqlite.

Project root: ~/Documents/dmo-analytics/
Input files:  ~/Documents/dmo-analytics/downloads/
              Place exported CSVs here before running:
                - GoogleAnalytics-MostPopularPages_Export*.csv
                - GoogleAnalytics-PerformanceVisual_Export*.csv  (traffic sources)
                - GoogleAnalytics-PerformanceVisual_Export*.csv  (audience overview)
Database:     ~/Documents/dmo-analytics/data/analytics.sqlite
Log:          ~/Documents/dmo-analytics/logs/pipeline.log

Tables created / replaced:
  Overview report  → datafy_overview_kpis
                     datafy_overview_dma
                     datafy_overview_demographics
                     datafy_overview_cluster_visitation
                     datafy_overview_category_spending
                     datafy_overview_airports
  Attribution Website → datafy_attribution_website_kpis
                        datafy_attribution_website_top_markets
                        datafy_attribution_website_dma
                        datafy_attribution_website_channels
                        datafy_attribution_website_clusters
                        datafy_attribution_website_demographics
  Attribution Media   → datafy_attribution_media_kpis
                        datafy_attribution_media_top_markets
  Social (GA4 CSVs)   → datafy_social_traffic_sources
                        datafy_social_audience_overview
                        datafy_social_top_pages

Run from project root:
    cd ~/Documents/dmo-analytics
    source venv/bin/activate
    python scripts/load_datafy_reports.py
"""

import os
import sqlite3
import csv
import glob
from datetime import datetime

# ─── Paths  (matches ~/Documents/dmo-analytics/ layout) ──────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))   # …/scripts/
PROJECT_ROOT  = os.path.dirname(BASE_DIR)                    # ~/Documents/dmo-analytics/
DB_PATH       = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
DOWNLOADS_DIR = os.path.join(PROJECT_ROOT, "downloads")      # raw Datafy/GA exports

NOW = datetime.utcnow().isoformat()

# ─── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return sqlite3.connect(DB_PATH)


def drop_and_create(cur, table_sql):
    """Drop the table if it exists, then create fresh."""
    # Extract table name from CREATE TABLE IF NOT EXISTS <name> (
    import re
    m = re.search(r'CREATE TABLE IF NOT EXISTS\s+(\w+)', table_sql, re.IGNORECASE)
    if m:
        cur.execute(f"DROP TABLE IF EXISTS {m.group(1)}")
    cur.executescript(table_sql)


# ══════════════════════════════════════════════════════════════════════════════
# OVERVIEW  (Annual Pull Deep Dive Visitation Report)
# ══════════════════════════════════════════════════════════════════════════════

def load_overview(cur):
    print("Loading Overview tables…")

    # ── datafy_overview_kpis ─────────────────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_overview_kpis (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        report_title                TEXT,
        report_period_start         TEXT,
        report_period_end           TEXT,
        compare_period_start        TEXT,
        compare_period_end          TEXT,
        data_source                 TEXT,
        total_trips                 INTEGER,
        avg_length_of_stay_days     REAL,
        avg_los_vs_compare_days     REAL,
        day_trips_pct               REAL,
        day_trips_vs_compare_pct    REAL,
        overnight_trips_pct         REAL,
        overnight_vs_compare_pct    REAL,
        one_time_visitors_pct       REAL,
        repeat_visitors_pct         REAL,
        in_state_visitor_days_pct   REAL,
        in_state_vd_vs_compare_pct  REAL,
        out_of_state_vd_pct         REAL,
        out_of_state_vd_vs_compare_pct REAL,
        in_state_spending_pct       REAL,
        out_of_state_spending_pct   REAL,
        locals_pct                  REAL,
        locals_vs_compare_pct       REAL,
        visitors_pct                REAL,
        visitors_vs_compare_pct     REAL,
        local_spending_pct          REAL,
        visitor_spending_pct        REAL,
        total_trips_vs_compare_pct  REAL,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    cur.execute("""
    INSERT INTO datafy_overview_kpis (
        report_title, report_period_start, report_period_end,
        compare_period_start, compare_period_end, data_source,
        total_trips, avg_length_of_stay_days, avg_los_vs_compare_days,
        day_trips_pct, day_trips_vs_compare_pct,
        overnight_trips_pct, overnight_vs_compare_pct,
        one_time_visitors_pct, repeat_visitors_pct,
        in_state_visitor_days_pct, in_state_vd_vs_compare_pct,
        out_of_state_vd_pct, out_of_state_vd_vs_compare_pct,
        in_state_spending_pct, out_of_state_spending_pct,
        locals_pct, locals_vs_compare_pct,
        visitors_pct, visitors_vs_compare_pct,
        local_spending_pct, visitor_spending_pct,
        total_trips_vs_compare_pct
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        "Annual Pull Deep Dive Visitation Report",
        "2025-01-01", "2025-12-31",
        "2024-01-01", "2024-12-31",
        "Datafy Geolocation (Caladan 1.2)",
        3551929, 2.0, 0.1,
        40.57,  2.36,
        59.43, -2.36,
        53.21, 46.79,
        38.99,  0.14,
        61.01, -0.14,
        39.59, 60.41,
        83.67, -0.51,
        16.33,  0.51,
        74.24, 25.76,
        -0.53,
    ))
    print("  ✓ datafy_overview_kpis (1 row)")

    # ── datafy_overview_dma ──────────────────────────────────────────────────
    drop_and_create(cur, """
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
    );
    """)

    dma_data = [
        # dma, vd_share, vd_vs_compare, spend_share, avg_spend, avg_los, trips_share
        ("Los Angeles",              18.73, -0.2, 15.79, 205.14, None, None),
        ("San Diego",                 8.12,  0.2,  6.49, 185.60, None, None),
        ("Phoenix -Prescott",         7.29,  0.0,  6.10, 298.06, 2.3,  6.80),
        ("San Francisco-Oak-San Jose",4.67,  0.0,  5.18, 288.86, None, None),
        ("Las Vegas",                 3.62,  0.1,  4.45, 378.10, 2.2,  3.48),
        ("Dallas-Ft. Worth",          3.11,  0.0,  4.03, 386.94, 2.6,  2.49),
        ("New York",                  2.90,  0.0,  3.83, 300.62, 2.5,  2.47),
        ("Sacramento-Stkton-Modesto", 2.80,  0.0,  None,   None, None, None),
        ("Denver",                    2.62,  0.0,  2.37, 292.05, 2.4,  2.26),
        ("Salt Lake City",            2.61,  None, 3.56, 293.55, 2.4,  2.33),
        ("Chicago",                   None,  None, None,   None, 2.5,  1.59),
        ("Seattle-Tacoma",            None,  None, None,   None, 2.5,  1.65),
        ("Houston",                   None,  None, None,   None, 2.4,  1.35),
        ("Palm Springs",              None,  None, 2.00, 189.15, None, None),
    ]
    for row in dma_data:
        cur.execute("""
        INSERT INTO datafy_overview_dma
          (report_period_start, report_period_end, dma,
           visitor_days_share_pct, visitor_days_vs_compare_pct,
           spending_share_pct, avg_spend_usd,
           avg_length_of_stay_days, trips_share_pct)
        VALUES (?,?,?,?,?,?,?,?,?)
        """, ("2025-01-01", "2025-12-31") + row)
    print(f"  ✓ datafy_overview_dma ({len(dma_data)} rows)")

    # ── datafy_overview_demographics ────────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_overview_demographics (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start TEXT,
        report_period_end   TEXT,
        dimension       TEXT,   -- 'age' | 'income' | 'household_size'
        segment         TEXT,
        share_pct       REAL,
        loaded_at       TEXT DEFAULT (datetime('now'))
    );
    """)

    demo_rows = [
        # dimension, segment, pct
        ("age",            "16-24",       11.31),
        ("age",            "25-44",       26.43),
        ("age",            "45-64",       36.89),
        ("age",            "65+",         25.37),
        ("income",         "$0-$50K",     25.32),
        ("income",         "$50K-$75K",   13.81),
        ("income",         "$75K-$100K",  14.77),
        ("income",         "$100K-$150K", 19.88),
        ("income",         "$150K+",      26.22),
        ("household_size", "1-2",         49.87),
        ("household_size", "3-5",         42.00),
        ("household_size", "6+",           8.14),
    ]
    for row in demo_rows:
        cur.execute("""
        INSERT INTO datafy_overview_demographics
          (report_period_start, report_period_end, dimension, segment, share_pct)
        VALUES (?,?,?,?,?)
        """, ("2025-01-01", "2025-12-31") + row)
    print(f"  ✓ datafy_overview_demographics ({len(demo_rows)} rows)")

    # ── datafy_overview_cluster_visitation ───────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_overview_cluster_visitation (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start     TEXT,
        report_period_end       TEXT,
        cluster                 TEXT,
        visitor_days_share_pct  REAL,
        vs_compare_pct          REAL,
        loaded_at               TEXT DEFAULT (datetime('now'))
    );
    """)

    cluster_data = [
        ("City Council Districts", 67.23, -5.4),
        ("City",                   60.79,  1.2),
        ("External Locations",     53.91,  0.6),
        ("Resorts",                17.83, -0.2),
        ("Harbor",                 16.11,  0.2),
        ("Beaches",                11.15,  0.5),
        ("Trails & Parks",          9.06,  0.3),
        ("Hotels",                  7.54,  0.1),
        ("Lantern District",        4.89, -0.3),
    ]
    for row in cluster_data:
        cur.execute("""
        INSERT INTO datafy_overview_cluster_visitation
          (report_period_start, report_period_end, cluster, visitor_days_share_pct, vs_compare_pct)
        VALUES (?,?,?,?,?)
        """, ("2025-01-01", "2025-12-31") + row)
    print(f"  ✓ datafy_overview_cluster_visitation ({len(cluster_data)} rows)")

    # ── datafy_overview_category_spending ───────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_overview_category_spending (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start TEXT,
        report_period_end   TEXT,
        category            TEXT,
        spend_share_pct     REAL,
        spending_correlation_pct REAL,   -- % of Accommodations visitors who also spent here
        loaded_at           TEXT DEFAULT (datetime('now'))
    );
    """)

    # spend_share + correlation (Accommodations base)
    cat_data = [
        ("Accommodations",                   41.08, 100.00),
        ("Dining and Nightlife",             24.01,  27.40),
        ("Grocery and Dept Stores",          11.86,   6.57),
        ("Specialty Retail",                  7.09,   2.70),
        ("Service Stations",                  4.25,   4.69),
        ("Clothing and Accessories",          3.43,   1.71),
        ("Leisure, Recreation and Entertainment", 3.20, 1.84),
        ("Fast Food Restaurants",             2.65,   7.58),
        ("Personal Care and Services",        2.15,  None),
        ("Transportation",                    0.21,  None),
    ]
    for row in cat_data:
        cur.execute("""
        INSERT INTO datafy_overview_category_spending
          (report_period_start, report_period_end, category, spend_share_pct, spending_correlation_pct)
        VALUES (?,?,?,?,?)
        """, ("2025-01-01", "2025-12-31") + row)
    print(f"  ✓ datafy_overview_category_spending ({len(cat_data)} rows)")

    # ── datafy_overview_airports ─────────────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_overview_airports (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start TEXT,
        report_period_end   TEXT,
        airport_name        TEXT,
        airport_code        TEXT,
        passengers_share_pct REAL,
        loaded_at           TEXT DEFAULT (datetime('now'))
    );
    """)

    airport_data = [
        ("Denver International Airport",                            "DEN", 32.63),
        ("Dallas/Fort Worth International Airport",                 "DFW", 16.50),
        ("Phoenix Sky Harbor International Airport",                "PHX",  8.88),
        ("Dallas Love Field Airport",                               "DAL",  7.90),
        ("Harry Reid International Airport",                        "LAS",  7.25),
        ("Hartsfield-Jackson Atlanta International Airport",        "ATL",  5.50),
        ("Chicago O'Hare International Airport",                    "ORD",  4.55),
        ("George Bush Intercontinental/Houston Airport",            "IAH",  3.70),
        ("Chicago Midway International Airport",                    "MDW",  3.29),
        ("Baltimore/Washington International Thurgood Marshall Airport", "BWI", 2.97),
    ]
    for row in airport_data:
        cur.execute("""
        INSERT INTO datafy_overview_airports
          (report_period_start, report_period_end, airport_name, airport_code, passengers_share_pct)
        VALUES (?,?,?,?,?)
        """, ("2025-01-01", "2025-09-01") + row)
    print(f"  ✓ datafy_overview_airports ({len(airport_data)} rows)")


# ══════════════════════════════════════════════════════════════════════════════
# ATTRIBUTION WEBSITE  (Jul 10 – Aug 27, 2025)
# ══════════════════════════════════════════════════════════════════════════════

def load_attribution_website(cur):
    print("Loading Attribution Website tables…")

    # ── datafy_attribution_website_kpis ─────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_attribution_website_kpis (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        report_title            TEXT,
        report_period_start     TEXT,
        report_period_end       TEXT,
        visitation_window_start TEXT,
        visitation_window_end   TEXT,
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
    );
    """)

    cur.execute("""
    INSERT INTO datafy_attribution_website_kpis (
        report_title, report_period_start, report_period_end,
        visitation_window_start, visitation_window_end,
        market_radius_miles, website_url,
        cohort_spend_per_visitor, manual_adr,
        attributable_trips, unique_reach, est_impact_usd,
        total_website_sessions, website_pageviews,
        avg_time_on_site_sec, avg_engagement_rate_pct
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        "Attribution Website Report",
        "2025-07-10", "2025-08-27",
        "2025-07-17", "2026-02-21",
        "50-3361",
        "https://visitdanapoint.com/",
        148.97, 413.00,
        209, 42525, 31135.00,
        56117, 91600,
        182,    # 3 min 2 sec = 182 seconds
        55.62,
    ))
    print("  ✓ datafy_attribution_website_kpis (1 row)")

    # ── datafy_attribution_website_top_markets ──────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_attribution_website_top_markets (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start     TEXT,
        report_period_end       TEXT,
        cluster_type            TEXT,   -- 'destination' | 'resorts' | 'hotels'
        total_trips             INTEGER,
        visitor_days_observed   INTEGER,
        est_room_nights         INTEGER,
        est_avg_length_of_stay_days REAL,
        est_impact_usd          REAL,
        top_dma                 TEXT,
        dma_share_of_impact_pct REAL,
        dma_est_impact_usd      REAL,
        loaded_at               TEXT DEFAULT (datetime('now'))
    );
    """)

    # Destination cluster
    dest_dmas = [
        ("Los Angeles",           34.69, 25216.80),
        ("Wichita-Hutchinson Plus", 8.80,  6395.10),
        ("Houston",                8.44,  6134.40),
        ("Phoenix - Prescott",     8.19,  5956.20),
        ("San Diego",              8.17,  5939.16),
    ]
    for dma, share, impact in dest_dmas:
        cur.execute("""
        INSERT INTO datafy_attribution_website_top_markets
          (report_period_start, report_period_end, cluster_type,
           total_trips, visitor_days_observed, est_room_nights,
           est_avg_length_of_stay_days, est_impact_usd,
           top_dma, dma_share_of_impact_pct, dma_est_impact_usd)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, ("2025-07-10", "2025-08-27", "destination",
              209, 402, None, 1.9, 31134.73,
              dma, share, impact))

    # Resorts cluster
    resort_dmas = [
        ("Phoenix - Prescott",    38.33,  9499.00),
        ("Los Angeles",           26.67,  6608.00),
        ("Minneapolis-St. Paul",   5.00,  1239.00),
    ]
    for dma, share, impact in resort_dmas:
        cur.execute("""
        INSERT INTO datafy_attribution_website_top_markets
          (report_period_start, report_period_end, cluster_type,
           total_trips, visitor_days_observed, est_room_nights,
           est_avg_length_of_stay_days, est_impact_usd,
           top_dma, dma_share_of_impact_pct, dma_est_impact_usd)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, ("2025-07-10", "2025-08-27", "resorts",
              22, None, 60, 2.7, 24780.00,
              dma, share, impact))

    # Hotels cluster
    hotel_dmas = [
        ("Minneapolis-St. Paul",        29.09, 6608.00),
        ("Harrisburg-Lncstr-Leb-York",  18.18, 4130.00),
        ("Phoenix - Prescott",          14.55, 3304.00),
        ("New York",                     9.09, 2065.00),
    ]
    for dma, share, impact in hotel_dmas:
        cur.execute("""
        INSERT INTO datafy_attribution_website_top_markets
          (report_period_start, report_period_end, cluster_type,
           total_trips, visitor_days_observed, est_room_nights,
           est_avg_length_of_stay_days, est_impact_usd,
           top_dma, dma_share_of_impact_pct, dma_est_impact_usd)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, ("2025-07-10", "2025-08-27", "hotels",
              22, None, 55, 2.5, 22715.00,
              dma, share, impact))

    total = len(dest_dmas) + len(resort_dmas) + len(hotel_dmas)
    print(f"  ✓ datafy_attribution_website_top_markets ({total} rows)")

    # ── datafy_attribution_website_dma ───────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_attribution_website_dma (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start     TEXT,
        report_period_end       TEXT,
        dma                     TEXT,
        total_trips             INTEGER,
        avg_los_destination_days REAL,
        vs_overall_destination_days REAL,
        loaded_at               TEXT DEFAULT (datetime('now'))
    );
    """)

    dma_trips = [
        ("Los Angeles",          80,  1.5, -0.3),
        ("Phoenix -Prescott",    18,  2.1, -0.4),
        ("Bakersfield",          16,  2.5,  0.3),
        ("Houston",              16,  3.0,  0.4),
        ("San Diego",            12,  1.8,  0.0),
        ("Oklahoma City",         9,  1.7, -0.8),
        ("Minneapolis-St. Paul",  8,  2.3, -0.3),
        ("Columbia-Jefferson City", 8, 3.7, 0.8),
        ("Denver",                7,  1.0, -1.7),
    ]
    for row in dma_trips:
        cur.execute("""
        INSERT INTO datafy_attribution_website_dma
          (report_period_start, report_period_end, dma,
           total_trips, avg_los_destination_days, vs_overall_destination_days)
        VALUES (?,?,?,?,?,?)
        """, ("2025-07-10", "2025-08-27") + row)
    print(f"  ✓ datafy_attribution_website_dma ({len(dma_trips)} rows)")

    # ── datafy_attribution_website_channels ──────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_attribution_website_channels (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start         TEXT,
        report_period_end           TEXT,
        acquisition_channel         TEXT,
        attribution_rate_pct        REAL,
        sessions                    INTEGER,
        avg_time_on_site_mmss       TEXT,
        engagement_rate_pct         REAL,
        attributable_trips_dest     INTEGER,
        attributable_trips_hotels   INTEGER,
        attributable_trips_resorts  INTEGER,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    channels = [
        ("email",    5.88,  86,    "01:53", 69.77,  5,  None, None),
        ("redirect", 4.20,  2510,  "03:33", 28.76, 79,    15,   11),
        ("search",   0.61, 42047, "03:04",  63.51, 204,   22,   22),
    ]
    for row in channels:
        cur.execute("""
        INSERT INTO datafy_attribution_website_channels
          (report_period_start, report_period_end,
           acquisition_channel, attribution_rate_pct, sessions,
           avg_time_on_site_mmss, engagement_rate_pct,
           attributable_trips_dest, attributable_trips_hotels,
           attributable_trips_resorts)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, ("2025-07-10", "2025-08-27") + row)
    print(f"  ✓ datafy_attribution_website_channels ({len(channels)} rows)")

    # ── datafy_attribution_website_clusters ──────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_attribution_website_clusters (
        id                              INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start             TEXT,
        report_period_end               TEXT,
        area                            TEXT,
        pct_of_total_destination_trips  REAL,
        area_type                       TEXT,  -- 'cluster' | 'poi'
        loaded_at                       TEXT DEFAULT (datetime('now'))
    );
    """)

    clusters = [
        ("City",                    100.00, "cluster"),
        ("City Council Districts",  100.00, "cluster"),
        ("Harbor",                   15.79, "cluster"),
        ("Lantern District",         13.16, "cluster"),
        ("Hotels",                    7.89, "cluster"),
        ("Resorts",                   7.89, "cluster"),
        ("Beaches",                   5.26, "cluster"),
        ("Trails & Parks",            2.63, "cluster"),
        ("External Locations",        2.63, "cluster"),
        ("Monarch Beach Golf Links",  2.63, "cluster"),
        ("Dana Point City",         100.00, "poi"),
        ("District 4",               39.47, "poi"),
        ("District 2",               35.53, "poi"),
        ("District 1",               32.89, "poi"),
        ("Harbor",                   15.79, "poi"),
        ("Lantern District",         13.16, "poi"),
        ("District 3",                7.89, "poi"),
        ("District 5",                6.58, "poi"),
        ("Doheny State Beach",        5.26, "poi"),
        ("The Ritz Carlton Laguna Niguel", 5.26, "poi"),
    ]
    for row in clusters:
        cur.execute("""
        INSERT INTO datafy_attribution_website_clusters
          (report_period_start, report_period_end, area,
           pct_of_total_destination_trips, area_type)
        VALUES (?,?,?,?,?)
        """, ("2025-07-10", "2025-08-27") + row)
    print(f"  ✓ datafy_attribution_website_clusters ({len(clusters)} rows)")

    # ── datafy_attribution_website_demographics ──────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_attribution_website_demographics (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start TEXT,
        report_period_end   TEXT,
        cluster_type        TEXT,   -- 'destination' | 'resorts' | 'hotels'
        dimension           TEXT,   -- 'age' | 'income' | 'household_size'
        segment             TEXT,
        share_pct           REAL,
        loaded_at           TEXT DEFAULT (datetime('now'))
    );
    """)

    demo_data = [
        # cluster_type, dimension, segment, pct
        ("destination", "age",            "16-24",      12.85),
        ("destination", "age",            "25-44",      11.96),
        ("destination", "age",            "45-64",      21.07),
        ("destination", "age",            "65+",        54.11),
        ("destination", "income",         "Under $50K", 10.96),
        ("destination", "income",         "$50K-$75K",   0.00),
        ("destination", "income",         "$75K-$100K", 23.73),
        ("destination", "income",         "$100K-$150K",11.29),
        ("destination", "income",         "$150K+",     54.01),
        ("destination", "household_size", "1-2 HH",     43.10),
        ("destination", "household_size", "3-5 HH",     47.25),
        ("destination", "household_size", "6+ HH",       9.66),
        ("resorts",     "age",            "16-24",       4.69),
        ("resorts",     "age",            "25-44",       0.94),
        ("resorts",     "age",            "45-64",       1.89),
        ("resorts",     "age",            "65+",        92.48),
        ("resorts",     "income",         "$150K+",    100.00),
        ("resorts",     "household_size", "1-2 HH",     32.31),
        ("resorts",     "household_size", "3-5 HH",     67.69),
        ("hotels",      "age",            "16-24",      50.20),
        ("hotels",      "age",            "25-44",      16.47),
        ("hotels",      "age",            "45-64",      33.33),
        ("hotels",      "age",            "65+",         0.00),
        ("hotels",      "income",         "$150K+",    100.00),
        ("hotels",      "household_size", "3-5 HH",    100.00),
    ]
    for row in demo_data:
        cur.execute("""
        INSERT INTO datafy_attribution_website_demographics
          (report_period_start, report_period_end,
           cluster_type, dimension, segment, share_pct)
        VALUES (?,?,?,?,?,?)
        """, ("2025-07-10", "2025-08-27") + row)
    print(f"  ✓ datafy_attribution_website_demographics ({len(demo_data)} rows)")


# ══════════════════════════════════════════════════════════════════════════════
# ATTRIBUTION MEDIA  (2025-26 Annual Campaign)
# ══════════════════════════════════════════════════════════════════════════════

def load_attribution_media(cur):
    print("Loading Attribution Media tables…")

    # ── datafy_attribution_media_kpis ────────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_attribution_media_kpis (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        report_title                TEXT,
        campaign_name               TEXT,
        report_period_start         TEXT,
        report_period_end           TEXT,
        visitation_window_start     TEXT,
        visitation_window_end       TEXT,
        market_radius_miles         TEXT,
        program_type                TEXT,
        cohort_spend_per_visitor    REAL,
        manual_adr                  REAL,
        total_impressions           INTEGER,
        unique_reach                INTEGER,
        attributable_trips          INTEGER,
        est_campaign_impact_usd     REAL,
        roas_description            TEXT,
        total_impact_usd            REAL,
        total_investment_usd        REAL,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    cur.execute("""
    INSERT INTO datafy_attribution_media_kpis (
        report_title, campaign_name,
        report_period_start, report_period_end,
        visitation_window_start, visitation_window_end,
        market_radius_miles, program_type,
        cohort_spend_per_visitor, manual_adr,
        total_impressions, unique_reach, attributable_trips,
        est_campaign_impact_usd,
        roas_description, total_impact_usd, total_investment_usd
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        "2025-26 Annual Campaign Attribution Report",
        "2025-26 Annual Campaign",
        "2025-08-01", "2026-03-16",
        "2025-08-08", "2026-02-21",
        "50-3361",
        "Media",
        91.11, 413.00,
        17402363, 5873300, 2093,
        190693.00,
        "Infinite (no media cost recorded)",
        190693.23, 0.00,
    ))
    print("  ✓ datafy_attribution_media_kpis (1 row)")

    # ── datafy_attribution_media_top_markets ────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_attribution_media_top_markets (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period_start         TEXT,
        report_period_end           TEXT,
        cluster_type                TEXT,   -- 'destination' | 'resorts' | 'hotels'
        total_trips                 INTEGER,
        visitor_days_observed       INTEGER,
        est_room_nights             INTEGER,
        est_avg_length_of_stay_days REAL,
        est_impact_usd              REAL,
        top_dma                     TEXT,
        dma_share_of_impact_pct     REAL,
        dma_est_impact_usd          REAL,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    dest_dmas = [
        ("Los Angeles",    103.06, 196535.56),
        ("San Diego",       76.21, 145327.39),
        ("Ft. Myers-Naples",39.63,  75578.88),
        ("Portland- OR",    21.58,  41142.64),
        ("Chicago",         16.89,  32207.04),
    ]
    for dma, share, impact in dest_dmas:
        cur.execute("""
        INSERT INTO datafy_attribution_media_top_markets
          (report_period_start, report_period_end, cluster_type,
           total_trips, visitor_days_observed, est_room_nights,
           est_avg_length_of_stay_days, est_impact_usd,
           top_dma, dma_share_of_impact_pct, dma_est_impact_usd)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, ("2025-08-01", "2026-03-16", "destination",
              2093, 4584, None, 2.2, 190693.23,
              dma, share, impact))

    resort_dmas = [
        ("Austin",     27.29,  50386.00),
        ("San Diego",  19.91,  36757.00),
        ("Los Angeles",13.65,  25193.00),
        ("Eugene",     12.75,  23541.00),
        ("Rockford",    4.03,   7434.00),
    ]
    for dma, share, impact in resort_dmas:
        cur.execute("""
        INSERT INTO datafy_attribution_media_top_markets
          (report_period_start, report_period_end, cluster_type,
           total_trips, visitor_days_observed, est_room_nights,
           est_avg_length_of_stay_days, est_impact_usd,
           top_dma, dma_share_of_impact_pct, dma_est_impact_usd)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, ("2025-08-01", "2026-03-16", "resorts",
              168, None, 447, 2.7, 184611.00,
              dma, share, impact))

    hotel_dmas = [
        ("San Diego",                      21.49, 30975.00),
        ("Greensboro-H.Point-W.Salem",     21.49, 30975.00),
        ("Portland- OR",                   12.89, 18585.00),
        ("Phoenix - Prescott",             11.75, 16933.00),
    ]
    for dma, share, impact in hotel_dmas:
        cur.execute("""
        INSERT INTO datafy_attribution_media_top_markets
          (report_period_start, report_period_end, cluster_type,
           total_trips, visitor_days_observed, est_room_nights,
           est_avg_length_of_stay_days, est_impact_usd,
           top_dma, dma_share_of_impact_pct, dma_est_impact_usd)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, ("2025-08-01", "2026-03-16", "hotels",
              109, None, 349, 3.2, 144137.00,
              dma, share, impact))

    total = len(dest_dmas) + len(resort_dmas) + len(hotel_dmas)
    print(f"  ✓ datafy_attribution_media_top_markets ({total} rows)")


# ══════════════════════════════════════════════════════════════════════════════
# SOCIAL  (Google Analytics 4 CSVs)
# ══════════════════════════════════════════════════════════════════════════════

def load_social(cur):
    print("Loading Social / Google Analytics tables…")

    # ── datafy_social_traffic_sources ────────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_social_traffic_sources (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        source                      TEXT,
        sessions                    INTEGER,
        screen_page_views           INTEGER,
        avg_session_duration_mmss   TEXT,
        engagement_rate_pct         REAL,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    traffic_rows = [
        ("google",                            1189, 10, "00:00:53", 27.33),
        ("(direct)",                          1007,  0, "00:01:08", 28.90),
        ("chatgpt.com",                         43,  0, "00:01:18", 30.23),
        ("bing",                                22,  0, "00:00:40", 45.45),
        ("expedia.com",                         20,  0, "00:01:51", 30.00),
        ("yahoo",                               18,  0, "00:02:30", 27.78),
        ("duckduckgo",                          16,  0, "00:00:31", 31.25),
        ("visitcalifornia.com",                  6,  0, "00:01:13", 33.33),
        ("(not set)",                            5,  0, "00:00:22",  0.00),
        ("hotels.com",                           5,  0, "00:00:00",  0.00),
        ("travelawaits.com",                     4,  0, "00:02:07", 25.00),
        ("usc-excel.officeapps.live.com",        4,  0, "00:09:60", 25.00),
        ("perplexity",                           3,  0, "00:00:00",  0.00),
        ("travelzoo",                            2,  0, "00:00:00",  0.00),
        ("visitdanapoint-com.translate.goog",    2,  0, "00:00:08", 50.00),
        ("aol",                                  1,  0, "00:21:45",100.00),
        ("bradfeldmangroup.com",                 1,  0, "00:00:00",  0.00),
        ("cn.bing.com",                          1,  0, "00:00:00",  0.00),
        ("dashboard.pantheon.io",                1,  0, "00:00:02",  0.00),
        ("eu1-app.outplayhq.com",                1,  0, "00:00:00",  0.00),
        ("facebook.com",                         1,  0, "00:00:00",  0.00),
        ("gemini.google.com",                    1,  0, "00:00:14",100.00),
        ("pinterest.com",                        1,  0, "00:00:32",100.00),
        ("reservations.arestravel.com",          1,  0, "00:00:00",  0.00),
        ("statics.teams.cdn.office.net",         1,  0, "00:14:46",100.00),
        ("wildlifeheritageareas.org",            1,  0, "00:00:00",  0.00),
    ]
    for row in traffic_rows:
        cur.execute("""
        INSERT INTO datafy_social_traffic_sources
          (source, sessions, screen_page_views, avg_session_duration_mmss, engagement_rate_pct)
        VALUES (?,?,?,?,?)
        """, row)
    print(f"  ✓ datafy_social_traffic_sources ({len(traffic_rows)} rows)")

    # ── datafy_social_audience_overview ──────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_social_audience_overview (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        audience_name               TEXT,
        sessions                    INTEGER,
        screen_page_views           INTEGER,
        avg_session_duration_mmss   TEXT,
        engagement_rate_pct         REAL,
        conversions                 INTEGER,
        loaded_at                   TEXT DEFAULT (datetime('now'))
    );
    """)

    cur.execute("""
    INSERT INTO datafy_social_audience_overview
      (audience_name, sessions, screen_page_views, avg_session_duration_mmss,
       engagement_rate_pct, conversions)
    VALUES (?,?,?,?,?,?)
    """, ("All Users", 2359, 10, "00:01:02", 28.15, 0))
    print("  ✓ datafy_social_audience_overview (1 row)")

    # ── datafy_social_top_pages ───────────────────────────────────────────────
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS datafy_social_top_pages (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        page_title  TEXT,
        page_views  INTEGER,
        page_path   TEXT,
        loaded_at   TEXT DEFAULT (datetime('now'))
    );
    """)

    # Full CSV data — look for any file matching the Datafy export naming convention
    # in ~/Documents/dmo-analytics/downloads/
    matches = glob.glob(os.path.join(DOWNLOADS_DIR, "GoogleAnalytics-MostPopularPages_Export*.csv"))
    csv_path = matches[0] if matches else None
    pages_loaded = 0
    if csv_path and os.path.exists(csv_path):
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                INSERT INTO datafy_social_top_pages (page_title, page_views, page_path)
                VALUES (?,?,?)
                """, (row.get("Page Title"), int(row.get("Page Views", 0) or 0),
                      row.get("Page Path")))
                pages_loaded += 1
    else:
        # Embedded top-100 rows extracted from uploaded PDF
        top_pages = [
            ("Visit Dana Point | The Best Beaches in Southern California", 138582, "/"),
            ("Offers - Visit Dana Point", 131057, "/offers/"),
            ("Dana Point Trolley | Visit Dana Point", 41593, "/dana-point-trolley/"),
            ("Baby Beach - Visit Dana Point", 38303, "/what-to-do/baby-beach/"),
            ("(not set)", 29712, "/"),
            ("Calendar - Visit Dana Point", 28680, "/calendar/"),
            ("Things to Do in Dana Point | Explore Dana Point", 26271, "/things-to-do/"),
            ("Concerts in the Park - Visit Dana Point", 24996, "/event/concerts-in-the-park-2025/2025-07-27/"),
            ("Events from February 9, 2019 – March 2, 2019 – Visit Dana Point", 23204, "/events/"),
            ("Things to Do in Dana Point | What to Do in Dana Point", 20605, "/things-to-do/"),
            ("Dana Point Lantern District | Restaurants, Shops & More", 19903, "/activities-and-things-to-do/dana-point-lantern-district/"),
            ("OC Parks Summer Concert Series - Visit Dana Point", 18262, "/event/oc-parks-summer-concert-series/"),
            ("4th of July Fireworks - Visit Dana Point", 16182, "/event/4th-of-july-fireworks-2025/"),
            ("Dana Point Restaurants | Dining", 15059, "/dana-point-restaurants/"),
            ("Doheny State Beach | Doheny SB | Visit Dana Point", 14062, "/what-to-do/doheny-state-beach/"),
            ("Dana Point Hotels | All Places to Stay", 13953, "/lodging/"),
            ("Dana Point Whale Watching | Whale Watching California", 13737, "/activities-and-things-to-do/dana-point-whale-watching/"),
            ("Dana Strand Beach - Visit Dana Point", 13706, "/what-to-do/dana-strand-beach/"),
            ("Ocean View Restaurants in Dana Point | Visit Dana Point", 12971, "/dana-point-restaurants/ocean-view-restaurants/"),
            ("Concerts in the Park - Visit Dana Point", 12658, "/event/concerts-in-the-park/2024-07-14/"),
            ("Best Beaches in Dana Point", 12571, "/activities-and-things-to-do/dana-point-beaches/"),
            ("Palm Tree Music Festival - Visit Dana Point", 11843, "/event/palm-tree-festival/"),
            ("Summer Concerts in the Park - Visit Dana Point", 11337, "/event/summer-concerts-in-the-park/2023-07-23/"),
            ("Palm Tree Music Festival - Visit Dana Point", 11251, "/event/palm-tree-music-festival/"),
            ("Best Year-Round Whale Watching | Dana Point, California", 10625, "/activities-and-things-to-do/dana-point-whale-watching/"),
            ("Dana Point Shopping | Dana Point Stores | Visit Dana Point", 10595, "/activities-and-things-to-do/shopping/"),
            ("The Top 6 Things To Do In Dana Point - Visit Dana Point", 10275, "/activities-recreation/blog/the-top-6-things-to-do-in-dana-point/"),
            ("Dana Point Shopping | Shop Local", 9961, "/activities-and-things-to-do/shopping/"),
            ("Summer Concert Series @ Salt Creek Beach - Visit Dana Point", 9959, "/event/summer-concert-series-salt-creek-beach/2025-08-14/"),
            ("Salt Creek Beach in Dana Point | Salt Creek Beach Park", 9773, "/what-to-do/salt-creek-beach/"),
            ("Dana Point Bars and Nightlife | Find Bars in Dana Point", 8973, "/dana-point-restaurants/bars-nightlife/"),
            ("Dana Point Beaches | Best Beaches in Dana Point", 8447, "/activities-and-things-to-do/dana-point-beaches/"),
            ("4th of July Fireworks - Visit Dana Point", 8203, "/event/4th-of-july-fireworks/"),
            ("Ohana Festival 2025 - Visit Dana Point", 7925, "/event/ohana-festival-2025/2025-09-26/"),
            ("Dana Point Wedding Venues | Visit Dana Point", 7610, "/weddings/"),
            ("Dana Point Harbor 48th Annual Boat Parade of Lights - Visit Dana Point", 7374, "/event/dana-point-harbor-48th-annual-boat-parade-of-lights/2023-12-09/"),
            ("Ohana Festival 2023 - Visit Dana Point", 7225, "/event/ohana-festival-2023/"),
            ("4th of July Fireworks & Events - Visit Dana Point", 7175, "/event/4th-of-july-events/"),
            ("Why Visit Dana Point? | Visit Dana Point", 7085, "/why-dana-point/"),
            ("Thank You for Reaching Out - Visit Dana Point", 7035, "/thank-you-for-reaching-out/"),
            ("Get on the Water | Dana Point Kayaking & Paddleboarding", 7018, "/activities-and-things-to-do/kayaking-paddleboarding/"),
            ("Privacy Policy - Visit Dana Point", 6966, "/privacy-policy/"),
            ("Palm Tree Festival - Visit Dana Point", 6916, "/event/palm-tree-festival/"),
            ("Dana Point Farmers Market - Visit Dana Point", 6427, "/what-to-do/dana-point-farmers-market/"),
            ("Laguna Cliffs Marriott Resort & Spa - Visit Dana Point", 6206, "/lodging-hotels/laguna-cliffs-marriott-resort-spa/"),
            ("2024 Boat Parade of Lights - Visit Dana Point", 6161, "/event/2024-boat-parade-of-lights/2024-12-13/"),
            ("Concerts in the Park - Visit Dana Point", 6079, "/event/concerts-in-the-park/2024-08-11/"),
            ("Dana Point Hiking | Find Your Trail", 5818, "/activities-and-things-to-do/hiking/"),
            ("Family-Friendly Restaurants | Visit Dana Point", 5697, "/dana-point-restaurants/family-friendly/"),
            ("The Ritz-Carlton, Laguna Niguel - Visit Dana Point", 5602, "/lodging-hotels/the-ritz-carlton-laguna-niguel/"),
            ("Concerts in the Park - Visit Dana Point", 5438, "/event/concerts-in-the-park-2025/2025-07-13/"),
            ("Catalina Express in Dana Point | Catalina Express Ferry", 5418, "/what-to-do/catalina-express/"),
            ("About Visit Dana Point - Visit Dana Point", 5398, "/meet-the-team/"),
            ("Meetings & Events | Visit Dana Point", 5257, "/meetings/"),
            ("Movies in the Park - Visit Dana Point", 5216, "/event/movies-in-the-park-2/2025-06-20/"),
            ("About Orange County California | Visit Dana Point", 5159, "/orange-county-california/"),
            ("The Start of the Pacific Coast Highway | Visit Dana Point", 5078, "/activities-and-things-to-do/arts-culture/pacific-coast-highway/"),
            ("Dana Point Shakespeare Festival - Visit Dana Point", 5014, "/event/dana-point-shakespeare-festival/"),
            ("Dana Point Harbor Holiday Lights - Visit Dana Point", 4998, "/event/dana-point-harbor-holiday-lights/"),
            ("Ohana Festival 2024 - Visit Dana Point", 4973, "/event/ohana-festival-2024/"),
            ("Dana Point Hiking | Hiking in Dana Point | Visit Dana Point", 4920, "/activities-and-things-to-do/hiking/"),
            ("Concerts in the Park - Visit Dana Point", 4878, "/series/concerts-in-the-park-2/"),
            ("Dana Point Surfing | Catch Your Wave", 4834, "/activities-and-things-to-do/dana-point-surfing/"),
            ("Art & Museums in Orange County | Visit Dana Point", 4708, "/activities-and-things-to-do/arts-culture/"),
            ("Día de los Muertos - Visit Dana Point", 4613, "/event/dia-de-los-muertos/"),
            ("Summer Concerts in the Park - Visit Dana Point", 4486, "/event/summer-concerts-in-the-park/2023-07-16/"),
            ("180blu - Visit Dana Point", 4416, "/restaurants-dining/180blu/"),
            ("Contact Us - Visit Dana Point", 4290, "/contact-us/"),
            ("Maritime Festival - Visit Dana Point", 4285, "/event/maritime-festival-3/2024-09-13/"),
            ("Summer Concerts in the Park - Visit Dana Point", 4146, "/event/summer-concerts-in-the-park/2023-08-06/"),
            ("Concerts in the Park - Visit Dana Point", 3988, "/event/concerts-in-the-park-2025/2025-07-20/"),
            ("Fine Dining in Orange County | Visit Dana Point", 3934, "/dana-point-restaurants/fine-dining/"),
            ("Directions to Dana Point | Visit Dana Point", 3911, "/dana-point-directions/"),
            ("Dana Point Kayaking | Dana Point Paddle Boarding", 3801, "/activities-and-things-to-do/kayaking-paddleboarding/"),
            ("Monarch Bay Beach Club - Visit Dana Point", 3731, "/restaurants-dining/monarch-bay-club/"),
            ("Ocean Institute | Ocean Institute Dana Point", 3717, "/what-to-do/ocean-institute/"),
            ("Shopping - Visit Dana Point", 3706, "/things-to-do/shopping/"),
            ("Dana Point Harbor | Visit Dana Point", 3706, "/what-to-do/dana-point-harbor/"),
            ("Dana Point Hotels | Best Hotels in Dana Point", 3671, "/lodging/dana-point-hotels/"),
            ("Summer Concerts in the Park - Visit Dana Point", 3591, "/event/summer-concerts-in-the-park/2023-07-30/"),
            ("Dana Point Film Festival - Visit Dana Point", 3584, "/event/dana-point-film-festival-2/"),
            ("Dana Point News & Media | Visit Dana Point", 3512, "/news-media/"),
            ("Mission San Juan Capistrano Facts | Mission Capistrano", 3469, "/activities-and-things-to-do/arts-culture/mission-san-juan-capistrano/"),
            ("Dana Point Fishing | Dana Point Sportfishing", 3445, "/activities-and-things-to-do/fishing/"),
            ("Local Attractions - Visit Dana Point", 3371, "/things-to-do/local-attractions/"),
            ("Capistrano Beach - Visit Dana Point", 3364, "/what-to-do/capistrano-beach/"),
            ("Ohana Festival | Visit Dana Point", 3357, "/events/ohana-music-festival/"),
            ("Unique Restaurants in Orange County | Visit Dana Point", 3305, "/dana-point-restaurants/local-hotspots/"),
            ("Dana Point Hiking and Walking Trails - Visit Dana Point", 3229, "/blog/blog/dana-point-hiking-and-walking-trails/"),
            ("Outlets at San Clemente - Visit Dana Point", 3175, "/what-to-do/outlets-at-san-clemente/"),
            ("Doheny Surf & Art Festival - Visit Dana Point", 3168, "/event/doheny-surf-art-festival/"),
            ("Dana Point Surfing | Find Dana Point Surfing Lessons and Rentals", 3119, "/activities-and-things-to-do/dana-point-surfing/"),
            ("Cosmic Creek Surf Festival & Concert - Visit Dana Point", 2973, "/event/cosmic-creek-surf-festival-concert/"),
            ("Things To Do Archives - Visit Dana Point", 2965, "/things-to-do/things-to-do/"),
            ("Captain Dave's Dolphin and Whale Safari - Visit Dana Point", 2952, "/what-to-do/captain-daves-dolphin-and-whale-safari/"),
            ("Dana Wharf Sportfishing and Whale Watching - Visit Dana Point", 2932, "/what-to-do/dana-wharf-sportfishing-and-whale-watching/"),
            ("Halloween Spooktacular & Trunk or Treat - Visit Dana Point", 2909, "/event/halloween-spooktacular-trunk-or-treat-3/"),
            ("OC Parks Sunset Cinema Series - Visit Dana Point", 2882, "/event/oc-parks-sunset-cinema-series/"),
            ("Waldorf Astoria Monarch Beach Resort & Club - Visit Dana Point", 2879, "/lodging-hotels/monarch-beach-resort/"),
        ]
        for row in top_pages:
            cur.execute("""
            INSERT INTO datafy_social_top_pages (page_title, page_views, page_path)
            VALUES (?,?,?)
            """, row)
        pages_loaded = len(top_pages)

    print(f"  ✓ datafy_social_top_pages ({pages_loaded} rows)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    """Write one line to logs/pipeline.log and echo to stdout — matches run_pipeline.py format."""
    line = f"{_now()} | {step:<25} | {status:<4} | {message}"
    print(line)
    log_path = os.path.join(PROJECT_ROOT, "logs", "pipeline.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a") as fh:
        fh.write(line + "\n")


def main():
    log("load_datafy_reports", "OK  ", f"=== start | db={DB_PATH} | downloads={DOWNLOADS_DIR} ===")

    conn = get_conn()
    cur = conn.cursor()

    try:
        load_overview(cur)
        load_attribution_website(cur)
        load_attribution_media(cur)
        load_social(cur)
        conn.commit()
        log("load_datafy_reports", "OK  ", "all tables committed")
    except Exception as e:
        conn.rollback()
        log("load_datafy_reports", "FAIL", str(e))
        raise
    finally:
        conn.close()

    # Row-count summary
    conn2 = get_conn()
    cur2 = conn2.cursor()
    cur2.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'datafy_%' ORDER BY name")
    rows_total = 0
    for (tbl,) in cur2.fetchall():
        cur2.execute(f"SELECT COUNT(*) FROM {tbl}")
        n = cur2.fetchone()[0]
        rows_total += n
        log("load_datafy_reports", "OK  ", f"{tbl}: {n} rows")
    conn2.close()
    log("load_datafy_reports", "OK  ", f"=== complete | {rows_total} total rows across datafy tables ===")


if __name__ == "__main__":
    main()
