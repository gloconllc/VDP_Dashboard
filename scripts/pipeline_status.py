# scripts/pipeline_status.py

import sqlite3
from pathlib import Path
from typing import Dict

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "analytics.sqlite"


def get_str_row_counts() -> Dict[str, int]:
    """
    Returns a dict like {'daily': 4392, 'monthly': 2345}.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT grain, COUNT(*)
        FROM fact_str_metrics
        WHERE source = 'STR'
        GROUP BY grain;
        """
    )
    counts = {grain: cnt for (grain, cnt) in cur.fetchall()}
    conn.close()
    return counts


if __name__ == "__main__":
    counts = get_str_row_counts()
    daily = counts.get("daily", 0)
    monthly = counts.get("monthly", 0)
    print(f"STR daily rows:   {daily}")
    print(f"STR monthly rows: {monthly}")

