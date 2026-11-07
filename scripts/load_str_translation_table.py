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
load_str_translation_table.py

Parses the 'Translation Table - 2025' sheet from weekly STR files.
Extracts:
  1. Holiday dates (TY vs LY) — shows holiday shift for YOY context
  2. Weekday/weekend day counts per reporting period

Output table: str_holiday_calendar
  (as_of_date, year_label, holiday_name, holiday_date, weekdays, weekend_days)
"""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger("load_str_translation_table")

BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
WEEKLY_DIR   = PROJECT_ROOT / "data" / "str" / "weekly"

SHEET_NAME = "Translation Table - 2025"

WEEK_RE = re.compile(r"For the Week of\s+(\w+ \d+,?\s*\d{4})", re.IGNORECASE)

# Parse "Monday, Dec 15th" or "Thursday, Apr 2nd" → date
DAY_RE = re.compile(r"(\w+day),\s+(\w+)\s+(\d+)\w*", re.IGNORECASE)


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS str_holiday_calendar (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date    TEXT NOT NULL,       -- week-ending date (ISO)
            year_label    TEXT NOT NULL,       -- 'this_year' or 'last_year'
            holiday_name  TEXT NOT NULL,
            holiday_date  TEXT,                -- ISO date if parseable
            holiday_raw   TEXT,                -- raw string from sheet
            weekdays      INTEGER,
            weekend_days  INTEGER,
            source_file   TEXT,
            UNIQUE(as_of_date, year_label, holiday_name)
        )
    """)
    conn.commit()


def _parse_week_start(cell_text: str) -> str | None:
    m = WEEK_RE.search(str(cell_text) if pd.notna(cell_text) else "")
    if m:
        raw = m.group(1).replace(",", "").strip()
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
    return None


def _parse_holiday_date(raw: str, as_of_date: str) -> str | None:
    """Parse 'Monday, Dec 15th' → ISO date, inferring year from as_of_date."""
    m = DAY_RE.match(raw.strip())
    if not m:
        return None
    month_str = m.group(2)
    day_str   = m.group(3)
    year = int(as_of_date[:4]) if as_of_date else datetime.now().year
    for fmt in ("%b %d %Y", "%B %d %Y"):
        for y in [year, year - 1, year + 1]:
            try:
                return datetime.strptime(f"{month_str} {day_str} {y}", fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
    return None


def parse_translation_sheet(df: pd.DataFrame, as_of_date: str, source_file: str) -> list[tuple]:
    """
    Structure:
      Row 17: headers ('This Year' at col 3, 'Last Year' at col 15)
      Rows 18-28: holiday rows — col 2 = TY date string, col 7 = TY holiday name,
                                  col 14 = LY date string, col 19 = LY holiday name
      Row 29: weekdays count (col 7 = TY count, col 19 = LY count)
      Row 30: weekend days count
    """
    rows: list[tuple] = []

    if len(df) < 20:
        return rows

    # Weekday/weekend counts
    weekdays_ty = weekdays_ly = weekend_ty = weekend_ly = None
    try:
        weekdays_ty  = int(float(df.iloc[29, 7]))  if pd.notna(df.iloc[29, 7])  else None
        weekdays_ly  = int(float(df.iloc[29, 19])) if pd.notna(df.iloc[29, 19]) else None
        weekend_ty   = int(float(df.iloc[30, 7]))  if pd.notna(df.iloc[30, 7])  else None
        weekend_ly   = int(float(df.iloc[30, 19])) if pd.notna(df.iloc[30, 19]) else None
    except Exception:
        pass

    # Holiday rows (18–28)
    for row_idx in range(18, min(29, len(df))):
        row = df.iloc[row_idx]

        # This Year
        ty_date_raw = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ""
        ty_name     = str(row.iloc[7]).strip()  if pd.notna(row.iloc[7])  else ""
        # strip leading " - "
        ty_name = ty_name.lstrip(" -").strip()

        if ty_date_raw and ty_name:
            holiday_date = _parse_holiday_date(ty_date_raw, as_of_date)
            rows.append((
                as_of_date, "this_year", ty_name, holiday_date, ty_date_raw,
                weekdays_ty, weekend_ty, source_file,
            ))

        # Last Year
        ly_date_raw = str(row.iloc[14]).strip() if pd.notna(row.iloc[14]) else ""
        ly_name     = str(row.iloc[19]).strip()  if pd.notna(row.iloc[19]) else ""
        ly_name = ly_name.lstrip(" -").strip()

        if ly_date_raw and ly_name:
            holiday_date = _parse_holiday_date(ly_date_raw, as_of_date)
            rows.append((
                as_of_date, "last_year", ly_name, holiday_date, ly_date_raw,
                weekdays_ly, weekend_ly, source_file,
            ))

    return rows


def load_translation_tables() -> int:
    conn = get_connection()
    ensure_table(conn)

    total = 0
    files = sorted(WEEKLY_DIR.glob("*.xls*"))
    _logger.info("Processing %d weekly files for Translation Table", len(files))

    for fpath in files:
        try:
            xl = pd.ExcelFile(fpath)
        except Exception as e:
            _logger.warning("Cannot open %s: %s", fpath.name, e)
            continue

        if SHEET_NAME not in xl.sheet_names:
            _logger.debug("  No '%s' in %s — skipping", SHEET_NAME, fpath.name)
            continue

        try:
            df = pd.read_excel(xl, sheet_name=SHEET_NAME, header=None)
        except Exception as e:
            _logger.warning("  Cannot read sheet in %s: %s", fpath.name, e)
            continue

        as_of_date = _parse_week_start(df.iloc[1, 1] if df.shape[0] > 1 else "")
        if not as_of_date:
            _logger.warning("  Cannot parse date from %s", fpath.name)
            continue

        parsed = parse_translation_sheet(df, as_of_date, fpath.name)
        if parsed:
            conn.executemany(
                """INSERT OR REPLACE INTO str_holiday_calendar
                   (as_of_date, year_label, holiday_name, holiday_date, holiday_raw,
                    weekdays, weekend_days, source_file)
                   VALUES (?,?,?,?,?,?,?,?)""",
                parsed,
            )
            conn.commit()
            total += len(parsed)
            _logger.info("  %s → %d holiday rows", fpath.name, len(parsed))

    try:
        conn.execute(
            "INSERT INTO load_log (source, grain, file_name, rows_inserted) VALUES (?,?,?,?)",
            ("STR", "weekly", "translation_table_weekly", total),
        )
        conn.commit()
    except Exception:
        pass

    _logger.info("Total holiday rows inserted: %d", total)
    conn.close()
    print(f"[load_str_translation_table] Done — {total:,} rows into str_holiday_calendar")
    return total


if __name__ == "__main__":
    load_translation_tables()
