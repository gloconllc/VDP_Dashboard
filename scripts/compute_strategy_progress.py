"""
compute_strategy_progress.py
─────────────────────────────
Initializes the strategy_goals table and auto-refreshes current_value
for each active goal by querying live data from analytics.sqlite.

Goals are created via the dashboard UI (admin mode) and stored here.
This script updates progress on every pipeline run so the tracker
always reflects the latest data.

Run automatically as step 6.5 in run_pipeline.py (after compute_insights).
Safe to run standalone: python3 scripts/compute_strategy_progress.py
"""

import os
import sqlite3
from datetime import datetime, date

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_table(cur):
    """Create strategy_goals table if it doesn't exist."""
    cur.execute("""
    CREATE TABLE IF NOT EXISTS strategy_goals (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        title          TEXT NOT NULL,
        description    TEXT,
        category       TEXT NOT NULL DEFAULT 'custom',
        metric_name    TEXT NOT NULL,
        metric_unit    TEXT DEFAULT '',
        target_value   REAL NOT NULL,
        current_value  REAL DEFAULT 0,
        baseline_value REAL DEFAULT 0,
        start_date     TEXT NOT NULL,
        target_date    TEXT NOT NULL,
        status         TEXT DEFAULT 'active',
        priority       INTEGER DEFAULT 2,
        notes          TEXT,
        auto_compute   INTEGER DEFAULT 1,
        compute_query  TEXT,
        created_at     TEXT DEFAULT (datetime('now')),
        updated_at     TEXT DEFAULT (datetime('now'))
    )
    """)

    # Seed default goals only if table is empty
    cur.execute("SELECT COUNT(*) FROM strategy_goals")
    if cur.fetchone()[0] == 0:
        _seed_default_goals(cur)


def _seed_default_goals(cur):
    """Seed example goals that map to live STR + Datafy data."""
    today = date.today().isoformat()
    goals = [
        (
            "RevPAR Growth Target",
            "Grow RevPAR to $180 by end of Q2 2026 — baseline from trailing-90-day average.",
            "revenue",
            "revpar_30d_avg",
            "USD",
            180.0, 0.0, 0.0,
            today, "2026-06-30",
            "active", 1,
            "Target aligns with peak-season rate strategy. Push rate discipline on compression days.",
            1,
            """SELECT ROUND(AVG(revpar),2) FROM kpi_daily_summary
               WHERE as_of_date >= date('now','-90 days') AND revpar IS NOT NULL"""
        ),
        (
            "ADR Premium $250 Milestone",
            "Achieve $250 Average Daily Rate across the portfolio — requires disciplined rate floors on weekends.",
            "revenue",
            "adr_30d_avg",
            "USD",
            250.0, 0.0, 0.0,
            today, "2026-09-01",
            "active", 1,
            "ADR is the highest-margin lever. Focus on weekend rate minimums and compression day premiums.",
            1,
            """SELECT ROUND(AVG(adr),2) FROM kpi_daily_summary
               WHERE as_of_date >= date('now','-30 days') AND adr IS NOT NULL"""
        ),
        (
            "Q3 Compression Days Target",
            "Achieve 40+ days above 80% occupancy in Q3 2026 (up from 36 in Q3 2025).",
            "occupancy",
            "compression_days_q3",
            "days",
            40.0, 0.0, 36.0,
            "2026-07-01", "2026-09-30",
            "active", 1,
            "Compression days are the clearest evidence of market strength for the TBID board.",
            1,
            """SELECT COALESCE(days_above_80_occ, 0) FROM kpi_compression_quarterly
               WHERE quarter = '2026-Q3' LIMIT 1"""
        ),
        (
            "Annual Visitor Trips — 500K Goal",
            "Grow total annual visitor trips to Dana Point to 500,000 (up from reported base).",
            "visitors",
            "total_trips_annual",
            "trips",
            500000.0, 0.0, 0.0,
            today, "2026-12-31",
            "active", 2,
            "Aligned with Datafy methodology. Track overnight + day-trip split separately.",
            1,
            """SELECT COALESCE(total_trips, 0) FROM datafy_overview_kpis
               ORDER BY id DESC LIMIT 1"""
        ),
        (
            "TBID Annual Revenue Target",
            "Project $2.5M in TBID assessments for FY2026 based on room revenue trajectory.",
            "tbid",
            "tbid_annual_est_usd",
            "USD",
            2500000.0, 0.0, 0.0,
            today, "2026-12-31",
            "active", 1,
            "Formula: trailing 365-day room revenue × 0.0125. Board-ready KPI.",
            1,
            """SELECT ROUND(
                 (SELECT COALESCE(SUM(metric_value),0) FROM fact_str_metrics
                  WHERE metric_name='revenue'
                  AND as_of_date >= date('now','-365 days')
                  AND grain='daily') * 0.0125, 2)"""
        ),
        (
            "Out-of-State Visitor Share — 70%",
            "Grow out-of-state visitor share from 68% to 70%+ to maximize ADR premium and economic impact.",
            "visitors",
            "oos_visitor_pct",
            "%",
            70.0, 0.0, 68.0,
            today, "2026-12-31",
            "active", 2,
            "OOS visitors spend 1.3–1.4x more per trip than in-state drive-market visitors.",
            1,
            """SELECT COALESCE(out_of_state_vd_pct, 0) FROM datafy_overview_kpis
               ORDER BY id DESC LIMIT 1"""
        ),
        (
            "Occupancy 80%+ Days — Annual",
            "Achieve 120+ days above 80% occupancy in calendar year 2026 across all quarters.",
            "occupancy",
            "annual_80pct_days",
            "days",
            120.0, 0.0, 0.0,
            "2026-01-01", "2026-12-31",
            "active", 2,
            "Sum of all quarterly compression days. Key board metric for rate strategy validation.",
            1,
            """SELECT COALESCE(SUM(days_above_80_occ),0) FROM kpi_compression_quarterly
               WHERE quarter LIKE '2026-%'"""
        ),
        (
            "Media Campaign ROAS — 5x",
            "Achieve 5x return on ad spend from Datafy media attribution — up from current ROAS.",
            "marketing",
            "media_roas",
            "x",
            5.0, 0.0, 0.0,
            today, "2026-12-31",
            "active", 2,
            "Datafy media ROAS is the most defensible number in the TBID board report.",
            1,
            """SELECT COALESCE(
                 CASE WHEN total_investment_usd > 0
                 THEN ROUND(total_impact_usd / total_investment_usd, 2)
                 ELSE 0 END, 0)
               FROM datafy_attribution_media_kpis
               ORDER BY id DESC LIMIT 1"""
        ),
    ]

    cur.executemany("""
    INSERT INTO strategy_goals
      (title, description, category, metric_name, metric_unit,
       target_value, current_value, baseline_value,
       start_date, target_date, status, priority, notes,
       auto_compute, compute_query)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, goals)
    print(f"  Seeded {len(goals)} default strategy goals.")


def refresh_current_values(conn, cur):
    """Run each goal's compute_query against live data and update current_value."""
    cur.execute("""
        SELECT id, title, compute_query
        FROM strategy_goals
        WHERE auto_compute = 1
          AND status IN ('active','achieved')
          AND compute_query IS NOT NULL
          AND compute_query != ''
    """)
    rows = cur.fetchall()
    updated = 0
    for gid, title, query in rows:
        try:
            result = cur.execute(query).fetchone()
            val = float(result[0]) if result and result[0] is not None else 0.0
            cur.execute("""
                UPDATE strategy_goals
                SET current_value = ?,
                    updated_at    = datetime('now')
                WHERE id = ?
            """, (val, gid))
            updated += 1
        except Exception as e:
            print(f"  WARN: goal '{title}' (id={gid}) compute failed — {e}")

    # Auto-mark achieved goals
    cur.execute("""
        UPDATE strategy_goals
        SET status = 'achieved', updated_at = datetime('now')
        WHERE status = 'active'
          AND current_value >= target_value
    """)
    # Re-activate goals that regressed (current < target after being achieved)
    cur.execute("""
        UPDATE strategy_goals
        SET status = 'active', updated_at = datetime('now')
        WHERE status = 'achieved'
          AND current_value < target_value * 0.98
    """)
    return updated


def main():
    conn = get_conn()
    cur  = conn.cursor()
    try:
        init_table(cur)
        conn.commit()

        n = refresh_current_values(conn, cur)
        conn.commit()
        print(f"[compute_strategy_progress] Updated {n} goal(s) with live data.")

        # Log to load_log
        cur.execute("""
            INSERT INTO load_log (source, grain, file_name, rows_inserted)
            VALUES ('strategy_goals', 'daily', 'compute_strategy_progress.py', ?)
        """, (n,))
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
