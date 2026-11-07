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
optimize_db.py
--------------
Keep analytics.sqlite fast after every pipeline run.

This is the "always-on" performance step: it runs late in run_pipeline.py so
that every refresh leaves the database tuned for the dashboard's read path.

What it does (all idempotent, safe to run repeatedly):
  1. Sets durable PRAGMAs (WAL journal, NORMAL sync) — persisted in the DB header.
  2. Creates hot-path indexes the dashboard filters on (IF NOT EXISTS).
  3. Runs ANALYZE so the query planner has fresh row statistics.
  4. Runs PRAGMA optimize to refresh internal stats.
  5. Checkpoints the WAL so the committed DB file is up to date for git.

Run:
    python3 scripts/optimize_db.py
"""

import os
import sqlite3
import time
from datetime import datetime

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")

# Indexes that back the dashboard's most frequent filters. Each is guarded by a
# guard query so we only build it when the underlying table exists — a fresh or
# partial DB never errors out.
INDEXES = [
    ("fact_str_metrics",   "idx_str_src_grain_date",
     "CREATE INDEX IF NOT EXISTS idx_str_src_grain_date "
     "ON fact_str_metrics (source, grain, as_of_date)"),
    ("fact_str_metrics",   "idx_str_metric",
     "CREATE INDEX IF NOT EXISTS idx_str_metric "
     "ON fact_str_metrics (metric_name)"),
    ("fact_str_group_metrics", "idx_str_group_market_date",
     "CREATE INDEX IF NOT EXISTS idx_str_group_market_date "
     "ON fact_str_group_metrics (market, as_of_date)"),
    ("kpi_daily_summary",  "idx_kpi_daily_date",
     "CREATE INDEX IF NOT EXISTS idx_kpi_daily_date "
     "ON kpi_daily_summary (as_of_date)"),
    ("insights_daily",     "idx_insights_aud_date",
     "CREATE INDEX IF NOT EXISTS idx_insights_aud_date "
     "ON insights_daily (as_of_date, audience, category)"),
    ("load_log",           "idx_load_log_run",
     "CREATE INDEX IF NOT EXISTS idx_load_log_run "
     "ON load_log (run_at)"),
]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def optimize() -> None:
    if not os.path.exists(DB_PATH):
        print(f"optimize_db: database not found at {DB_PATH} — nothing to do")
        return

    t0 = time.time()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        # 1. Durable performance PRAGMAs (WAL is persisted in the file header)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        # 2. Hot-path indexes
        created = 0
        for table, name, ddl in INDEXES:
            if _table_exists(conn, table):
                conn.execute(ddl)
                created += 1
        conn.commit()

        # 3. Fresh planner statistics
        conn.execute("ANALYZE")
        conn.commit()

        # 4. Refresh internal stats
        conn.execute("PRAGMA optimize")

        # 5. Fold the WAL back into the main DB file so the committed
        #    analytics.sqlite reflects the latest data for git.
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.commit()
    finally:
        conn.close()

    dt = time.time() - t0
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"optimize_db: {created} indexes ensured, ANALYZE done, "
          f"WAL checkpointed in {dt:.2f}s @ {ts}")


if __name__ == "__main__":
    optimize()
