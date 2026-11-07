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
load_visit_ca_sentiment.py — California Resident Sentiment loader

Parses "California Resident Sentiment 2025 (N).xlsx" — Visit California's
ongoing study of resident attitudes toward travel and tourism (4 annual
waves: 2022-2025; National/State/Region/County/City breakdowns) — into
visit_ca_resident_sentiment.

Layer 2 context data (per CLAUDE.md hierarchy): resident sentiment informs
community-relations messaging, never overrides Layer 1 STR/Datafy truth.
"""

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "analytics.sqlite"
DATA_DIR = ROOT / "data" / "Visit_California"

SENTIMENT_FILE = DATA_DIR / "California Resident Sentiment 2025  (2).xlsx"

DDL = """
CREATE TABLE IF NOT EXISTS visit_ca_resident_sentiment (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    area_type    TEXT NOT NULL,
    area         TEXT NOT NULL,
    question     TEXT NOT NULL,
    scale        TEXT,
    response     TEXT NOT NULL,
    count_2022   REAL,
    score_2022   REAL,
    count_2023   REAL,
    score_2023   REAL,
    count_2024   REAL,
    score_2024   REAL,
    count_2025   REAL,
    score_2025   REAL,
    loaded_at    TEXT DEFAULT (datetime('now')),
    UNIQUE(area_type, area, question, response) ON CONFLICT REPLACE
);
"""


def ts() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def _f(val):
    try:
        f = float(val)
        return None if f != f else f  # NaN check
    except (TypeError, ValueError):
        return None


def load_sentiment(conn: sqlite3.Connection) -> int:
    if not SENTIMENT_FILE.exists():
        print(f"{ts()} [SKIP] {SENTIMENT_FILE} not found")
        return 0

    df = pd.read_excel(SENTIMENT_FILE, sheet_name="Resident Sentiment", header=None)

    # Row 0 = year headers, row 1 = column labels. Data starts row 2.
    df = df.iloc[2:].reset_index(drop=True)

    # Area Type / Area / Question / Scale are only populated on the first row
    # of each group in the source export — forward-fill to associate every
    # Response/Mean row with its parent group.
    for col in (0, 1, 2, 3):
        df[col] = df[col].ffill()

    cur = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        area_type = str(row[0]).strip() if pd.notna(row[0]) else None
        area = str(row[1]).strip() if pd.notna(row[1]) else None
        question = str(row[2]).strip() if pd.notna(row[2]) else None
        scale = str(row[3]).strip() if pd.notna(row[3]) else None
        response = str(row[4]).strip() if pd.notna(row[4]) else None
        if not area_type or not area or not question or not response:
            continue

        cur.execute(
            """
            INSERT INTO visit_ca_resident_sentiment (
                area_type, area, question, scale, response,
                count_2022, score_2022, count_2023, score_2023,
                count_2024, score_2024, count_2025, score_2025
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                area_type, area, question, scale, response,
                _f(row[5]), _f(row[6]), _f(row[7]), _f(row[8]),
                _f(row[9]), _f(row[10]), _f(row[11]), _f(row[12]),
            ),
        )
        count += 1

    conn.commit()
    return count


def log_load(conn, rows):
    conn.execute(
        "INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at) "
        "VALUES ('Visit_CA_Sentiment', 'annual', ?, ?, datetime('now'))",
        (SENTIMENT_FILE.name, rows),
    )
    conn.commit()


def seed_table_relationships(conn):
    cur = conn.cursor()
    existing = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='table_relationships'"
    ).fetchone()
    if not existing:
        return
    rels = [
        ("visit_ca_resident_sentiment", "kpi_daily_summary", "context", "area",
         "CA resident sentiment on tourism (Orange County / Orange County region) contextualizes community support alongside VDP performance"),
        ("visit_ca_resident_sentiment", "datafy_overview_kpis", "context", "report_period",
         "Resident sentiment on tourism benefits/costs informs destination-management narrative alongside visitor economy KPIs"),
    ]
    for (a, b, rel, key, desc) in rels:
        cur.execute(
            """
            INSERT INTO table_relationships (table_a, table_b, relationship_type, join_key, description)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            (a, b, rel, key, desc),
        )
    conn.commit()


def main():
    print(f"{ts()} [START] load_visit_ca_sentiment.py")
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript(DDL)
    conn.commit()

    n = load_sentiment(conn)
    print(f"{ts()} [{'OK  ' if n else 'WARN'}] visit_ca_resident_sentiment: {n} rows")
    log_load(conn, n)
    seed_table_relationships(conn)

    conn.close()
    print(f"{ts()} [DONE] load_visit_ca_sentiment.py complete")


if __name__ == "__main__":
    main()
