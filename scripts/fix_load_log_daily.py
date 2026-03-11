# scripts/fix_load_log_daily.py

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "analytics.sqlite"


def recompute_daily_rows_inserted():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Count current STR daily rows in fact_str_metrics
    cur.execute(
        """
        SELECT COUNT(*)
        FROM fact_str_metrics
        WHERE source = 'STR'
          AND grain = 'daily';
        """
    )
    row = cur.fetchone()
    daily_count = row[0] if row else 0

    # Update the most recent STR daily log row to that count
    cur.execute(
        """
        UPDATE load_log
        SET rows_inserted = ?
        WHERE id = (
            SELECT id
            FROM load_log
            WHERE source = 'STR'
              AND grain = 'daily'
            ORDER BY run_at DESC
            LIMIT 1
        );
        """,
        (daily_count,),
    )

    conn.commit()
    conn.close()
    print(f"Updated latest STR daily load_log entry to {daily_count} rows.")


if __name__ == "__main__":
    recompute_daily_rows_inserted()

