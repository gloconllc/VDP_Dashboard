"""
load_resident_sentiment.py — California Resident Sentiment on Travel & Tourism
Parses 'California Resident Sentiment 2025 (2).xlsx' into visit_ca_resident_sentiment.

Source: Visit California annual survey (4 waves: 2022, 2023, 2024, 2025)
Coverage: US, California, 12 tourism regions, 27 counties, 22 cities
Sheet: 'Resident Sentiment'
Columns: Area Type, Area, Question, Scale, Response, Count/Score × 4 years

Table: visit_ca_resident_sentiment
Run from project root: python scripts/load_resident_sentiment.py
"""

import sqlite3
from datetime import datetime
from pathlib import Path

import openpyxl
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "analytics.sqlite"
DATA_DIR = ROOT / "data" / "Visit_California"
FILE = DATA_DIR / "California Resident Sentiment 2025 (2).xlsx"

YEARS = [2022, 2023, 2024, 2025]

DDL = """
CREATE TABLE IF NOT EXISTS visit_ca_resident_sentiment (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    area_type   TEXT NOT NULL,
    area        TEXT NOT NULL,
    question    TEXT NOT NULL,
    scale       TEXT,
    response    TEXT NOT NULL,
    survey_year INTEGER NOT NULL,
    count       REAL,
    score       REAL,
    updated_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(area_type, area, question, response, survey_year) ON CONFLICT REPLACE
);
"""


def ts():
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def load():
    if not FILE.exists():
        print(f"{ts()} [SKIP] {FILE.name} not found")
        return 0

    wb = openpyxl.load_workbook(str(FILE), read_only=True, data_only=True)
    ws = wb["Resident Sentiment"]

    # Row 0 (index): year headers — (None, None, None, None, None, 2022, None, 2023, ...)
    # Row 1 (index): column headers — Area Type, Area, Question, Scale, Response, Count, Score, ...
    all_rows = list(ws.iter_rows(values_only=True))
    wb.close()

    # Build column index: year -> (count_col, score_col)
    year_row = all_rows[0]
    year_cols = {}
    current_year = None
    for i, v in enumerate(year_row):
        if v in YEARS:
            current_year = int(v)
            year_cols[current_year] = [i, i + 1]  # count, score

    records = []
    prev_area_type, prev_area, prev_question, prev_scale = None, None, None, None

    for row in all_rows[2:]:  # skip year header + column header rows
        area_type = row[0] or prev_area_type
        area      = row[1] or prev_area
        question  = row[2] or prev_question
        scale     = row[3] or prev_scale
        response  = row[4]

        if not response:
            continue

        prev_area_type = area_type
        prev_area      = area
        prev_question  = question
        prev_scale     = scale

        for year, (ci, si) in year_cols.items():
            count = row[ci] if ci < len(row) else None
            score = row[si] if si < len(row) else None
            if count is None and score is None:
                continue
            records.append({
                "area_type":   area_type,
                "area":        area,
                "question":    question,
                "scale":       scale,
                "response":    str(response),
                "survey_year": year,
                "count":       float(count) if count is not None else None,
                "score":       float(score) if score is not None else None,
            })

    if not records:
        print(f"{ts()} [WARN] No records parsed from {FILE.name}")
        return 0

    conn = sqlite3.connect(str(DB), timeout=10)
    conn.executescript(DDL)

    rows = [(
        r["area_type"], r["area"], r["question"], r["scale"],
        r["response"], r["survey_year"], r["count"], r["score"]
    ) for r in records]

    conn.executemany(
        """INSERT OR REPLACE INTO visit_ca_resident_sentiment
           (area_type, area, question, response, scale, survey_year, count, score)
           VALUES (?,?,?,?,?,?,?,?)""",
        [(r["area_type"], r["area"], r["question"], r["response"],
          r["scale"], r["survey_year"], r["count"], r["score"]) for r in records]
    )
    conn.commit()

    try:
        conn.execute(
            "INSERT INTO load_log (source, grain, file_name, rows_inserted) VALUES (?,?,?,?)",
            ("VisitCA", "annual", FILE.name, len(records))
        )
        conn.commit()
    except Exception:
        pass

    conn.close()
    print(f"{ts()} [OK  ] visit_ca_resident_sentiment: {len(records)} rows from {FILE.name}")
    return len(records)


if __name__ == "__main__":
    print(f"{ts()} [START] load_resident_sentiment.py")
    n = load()
    print(f"{ts()} [DONE] {n} rows upserted")
