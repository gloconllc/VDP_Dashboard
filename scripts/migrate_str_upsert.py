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
migrate_str_upsert.py

One-time migration: adds UNIQUE constraint to fact_str_metrics so the
loaders can UPSERT (newer files update existing rows rather than skipping).

Safe to run multiple times — checks if constraint already exists first.
"""

import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")


def main():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    cur = conn.cursor()

    # Check if UNIQUE constraint already exists
    schema = cur.execute(
        "SELECT sql FROM sqlite_master WHERE name='fact_str_metrics'"
    ).fetchone()[0]

    if "UNIQUE" in schema.upper():
        print("fact_str_metrics already has UNIQUE constraint — no migration needed.")
        conn.close()
        return

    print("Migrating fact_str_metrics to add UNIQUE constraint...")

    row_count = cur.execute("SELECT COUNT(*) FROM fact_str_metrics").fetchone()[0]
    print(f"  Existing rows: {row_count:,}")

    cur.executescript("""
        BEGIN;

        CREATE TABLE fact_str_metrics_new (
            source        TEXT NOT NULL,
            grain         TEXT NOT NULL,
            property_name TEXT NOT NULL,
            market        TEXT NOT NULL,
            submarket     TEXT,
            as_of_date    TEXT NOT NULL,
            metric_name   TEXT NOT NULL,
            metric_value  REAL,
            unit          TEXT NOT NULL,
            UNIQUE(source, grain, property_name, market, as_of_date, metric_name)
                ON CONFLICT REPLACE
        );

        INSERT OR REPLACE INTO fact_str_metrics_new
            SELECT source, grain, property_name, market, submarket,
                   as_of_date, metric_name, metric_value, unit
            FROM fact_str_metrics;

        DROP TABLE fact_str_metrics;
        ALTER TABLE fact_str_metrics_new RENAME TO fact_str_metrics;

        COMMIT;
    """)

    new_count = cur.execute("SELECT COUNT(*) FROM fact_str_metrics").fetchone()[0]
    print(f"  Rows after migration: {new_count:,}")
    print("  Done. fact_str_metrics now has UNIQUE(source,grain,property_name,market,as_of_date,metric_name).")
    conn.close()


if __name__ == "__main__":
    main()
