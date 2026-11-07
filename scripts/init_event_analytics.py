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
init_event_analytics.py
=======================
Initialize event analytics tables for comprehensive event impact analysis.

Tables created:
  - events_metrics: attendance factors, web interest, promotion channels
  - events_economic_impact: STR correlation, revenue estimates
  - events_promotion_analysis: social buzz, Google Trends, web traffic
  - events_visitor_mix: demographics from Datafy for each event
"""

import sqlite3
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH = PROJECT_ROOT / "data" / "analytics.sqlite"
LOG_PATH = PROJECT_ROOT / "logs" / "pipeline.log"


def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step, status, message):
    line = f"[{_ts()}] [{status:<4}] {step}: {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create_tables(conn):
    """Create all event analytics tables."""

    # ── Events Metrics ────────────────────────────────────────────────────────
    # Attendance factors, promotion channels, web interest
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_metrics (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id              INTEGER NOT NULL,
            event_date            TEXT NOT NULL,
            event_name            TEXT,
            estimated_attendance  INTEGER,
            promotion_channels    TEXT,
            google_trend_peak     REAL,
            google_trend_interest INTEGER,
            web_search_volume     INTEGER,
            social_post_count     INTEGER,
            social_engagement     INTEGER,
            media_coverage        INTEGER,
            days_until_event      INTEGER,
            season                TEXT,
            is_recurring          INTEGER DEFAULT 0,
            last_year_attendance  INTEGER,
            growth_yoy            REAL,
            calculated_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(event_id, event_date),
            FOREIGN KEY(event_id) REFERENCES vdp_events(id)
        )
    """)

    # ── Events Economic Impact ────────────────────────────────────────────────
    # STR correlation, revenue estimates, ROI
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_economic_impact (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id                  INTEGER NOT NULL,
            event_date                TEXT NOT NULL,
            event_name                TEXT,
            baseline_occupancy        REAL,
            event_occupancy           REAL,
            occ_lift_pct              REAL,
            baseline_adr              REAL,
            event_adr                 REAL,
            adr_lift_pct              REAL,
            baseline_revpar           REAL,
            event_revpar              REAL,
            revpar_lift_pct           REAL,
            estimated_rooms_sold      INTEGER,
            estimated_room_revenue    REAL,
            estimated_total_spend     REAL,
            visitor_days_generated    INTEGER,
            daytrip_conversions       INTEGER,
            overnight_conversion_pct  REAL,
            estimated_tbid_revenue    REAL,
            estimated_tot_revenue     REAL,
            event_marketing_cost      REAL,
            estimated_roi             REAL,
            revenue_per_attendee      REAL,
            calculated_at             TEXT DEFAULT (datetime('now')),
            UNIQUE(event_id, event_date),
            FOREIGN KEY(event_id) REFERENCES vdp_events(id)
        )
    """)

    # ── Events Promotion Analysis ────────────────────────────────────────────
    # Social media buzz, Google Trends, web traffic correlation
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_promotion_analysis (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id              INTEGER NOT NULL,
            event_date            TEXT NOT NULL,
            event_name            TEXT,
            instagram_posts       INTEGER,
            instagram_engagement  REAL,
            facebook_posts        INTEGER,
            facebook_engagement   REAL,
            tiktok_posts          INTEGER,
            tiktok_engagement     REAL,
            total_social_reach    INTEGER,
            hashtag_volume        INTEGER,
            sentiment_score       REAL,
            top_hashtags          TEXT,
            news_articles         INTEGER,
            news_sentiment        TEXT,
            influencer_mentions   INTEGER,
            organic_web_traffic   INTEGER,
            paid_web_traffic      INTEGER,
            event_page_views      INTEGER,
            ticket_page_ctr       REAL,
            booking_uplift_pct    REAL,
            promotion_effectiveness_score REAL,
            recommended_spend_next_year   REAL,
            calculated_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(event_id, event_date),
            FOREIGN KEY(event_id) REFERENCES vdp_events(id)
        )
    """)

    # ── Events Visitor Mix ────────────────────────────────────────────────────
    # Demographics from Datafy for each event period
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_visitor_mix (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id                INTEGER NOT NULL,
            event_date              TEXT NOT NULL,
            event_name              TEXT,
            total_visitors          INTEGER,
            overnight_visitors      INTEGER,
            daytrip_visitors        INTEGER,
            overnight_pct           REAL,
            avg_length_of_stay      REAL,
            out_of_state_pct        REAL,
            international_pct       REAL,
            primary_origin_market   TEXT,
            primary_origin_state    TEXT,
            primary_origin_share    REAL,
            top_5_markets           TEXT,
            age_group_primary       TEXT,
            household_income_median REAL,
            avg_party_size          REAL,
            repeat_visitor_pct      REAL,
            first_time_visitor_pct  REAL,
            business_traveler_pct   REAL,
            leisure_traveler_pct    REAL,
            group_travel_pct        REAL,
            family_group_pct        REAL,
            avg_spending_per_trip   REAL,
            top_spending_category   TEXT,
            accommodation_spend_pct REAL,
            dining_spend_pct        REAL,
            retail_spend_pct        REAL,
            calculated_at           TEXT DEFAULT (datetime('now')),
            UNIQUE(event_id, event_date),
            FOREIGN KEY(event_id) REFERENCES vdp_events(id)
        )
    """)

    # ── Events Insights Cache ────────────────────────────────────────────────
    # Pre-computed event insights for dashboard use
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_insights (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id              INTEGER NOT NULL,
            event_date            TEXT NOT NULL,
            event_name            TEXT,
            insight_type          TEXT NOT NULL,
            headline              TEXT,
            body                  TEXT,
            metric_value          REAL,
            metric_label          TEXT,
            recommendation        TEXT,
            confidence_score      REAL,
            priority              INTEGER,
            calculated_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(event_id, event_date, insight_type),
            FOREIGN KEY(event_id) REFERENCES vdp_events(id)
        )
    """)

    conn.commit()
    log("init_event_analytics", "OK  ", "Created 5 event analytics tables")


def main():
    log("init_event_analytics", "INFO", "Starting event analytics schema initialization")

    conn = get_conn()
    try:
        create_tables(conn)
        log("init_event_analytics", "OK  ", "Event analytics schema ready")
    except Exception as e:
        log("init_event_analytics", "FAIL", f"Schema creation failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
