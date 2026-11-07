# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
# scripts/fix_load_log_monthly.py

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "analytics.sqlite"


def recompute_monthly_rows_inserted():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Count current STR monthly rows in fact_str_metrics
    cur.execute(
        """
        SELECT COUNT(*)
        FROM fact_str_metrics
        WHERE source = 'STR'
          AND grain = 'monthly';
        """
    )
    row = cur.fetchone()
    monthly_count = row[0] if row else 0

    # Update the most recent STR monthly log row to that count
    cur.execute(
        """
        UPDATE load_log
        SET rows_inserted = ?
        WHERE id = (
            SELECT id
            FROM load_log
            WHERE source = 'STR'
              AND grain = 'monthly'
            ORDER BY run_at DESC
            LIMIT 1
        );
        """,
        (monthly_count,),
    )

    conn.commit()
    conn.close()
    print(f"Updated latest STR monthly load_log entry to {monthly_count} rows.")


if __name__ == "__main__":
    recompute_monthly_rows_inserted()

