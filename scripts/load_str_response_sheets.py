"""
load_str_response_sheets.py
----------------------------
Parses all 6 "Response" sheets from every weekly and monthly STR Excel file
and loads property roster data into fact_str_response_markets.

Sheet structure (row-based, 0-indexed):
  Row 0: Title  ("Tab 4 - Response Dana Point, CA+: Visit Dana Point")
  Row 2: Date range ("For the Week of April 19, 2026 to April 25, 2026"
                     OR "For the Month of January 2026")
  Row 6 (weekly) / Row 5 (monthly): Column headers
           [NaN, STR Code, Name of Establishment, City & State, Zip Code,
            Aff Date, Open Date, Rooms, Chg in Rms, <day/month markers...>]
  Row 7+ (weekly) / Row 6+ (monthly): Property rows
  Last rows: Totals / Legend rows (detected by non-numeric STR Code)

NOTE: The Response sheets contain property membership/participation rosters,
      NOT performance metrics.  supply/demand/revenue/occ_pct/adr/revpar
      are always stored as NULL.  The useful data is the property roster
      (STR code, name, rooms, city/state, zip) and reporting status.

Output table: fact_str_response_markets
"""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger("load_str_response_sheets")

BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent

import sys as _sys
_sys.path.insert(0, str(BASE_DIR))
try:
    import file_dates as FD          # order files by DATE ADDED (oldest first), never by name
except Exception:                    # pragma: no cover
    FD = None


def _oldest_first(paths):
    paths = list(paths)
    if FD is None:
        return sorted(paths)
    ordered = FD.sort_oldest_first([str(p) for p in paths], str(PROJECT_ROOT))
    return [Path(p) for p in ordered]

DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
WEEKLY_DIR   = PROJECT_ROOT / "data" / "str" / "weekly"
MONTHLY_DIR  = PROJECT_ROOT / "data" / "str" / "monthly"

# Sheet name → clean market name
SHEET_MARKET_MAP: dict[str, str] = {
    "Response Dana Point, CA+":    "Dana Point",
    "Response Newport Beach, CA+": "Newport Beach",
    "Response La Jolla, CA+":      "La Jolla",
    "Response Santa Barbara, CA+": "Santa Barbara",
    "Resp Monterey-Carmel, CA+":   "Monterey-Carmel",
    "Response Hunting Beach, CA+": "Huntington Beach",
}

RESPONSE_SHEETS = list(SHEET_MARKET_MAP.keys())


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_str_response_markets (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            grain         TEXT NOT NULL,
            as_of_date    TEXT NOT NULL,
            market        TEXT NOT NULL,
            property_name TEXT,
            city_state    TEXT,
            zip_code      TEXT,
            str_code      TEXT,
            rooms         INTEGER,
            supply        REAL,
            demand        REAL,
            revenue       REAL,
            occ_pct       REAL,
            adr           REAL,
            revpar        REAL,
            source_file   TEXT,
            UNIQUE(grain, as_of_date, market, property_name)
        )
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

_WEEK_RE  = re.compile(
    r"For the Week of\s+(\w+ \d+,?\s*\d{4})", re.IGNORECASE
)
_MONTH_RE = re.compile(
    r"For the Month of\s+(\w+ \d{4})", re.IGNORECASE
)


def _parse_date(date_str: str, grain: str) -> str | None:
    """Return ISO date string from the row-2 header text."""
    if not isinstance(date_str, str):
        return None
    if grain == "weekly":
        m = _WEEK_RE.search(date_str)
        if m:
            raw = m.group(1).replace(",", "").strip()
            for fmt in ("%B %d %Y", "%b %d %Y"):
                try:
                    return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    pass
    else:
        m = _MONTH_RE.search(date_str)
        if m:
            raw = m.group(1).strip()
            for fmt in ("%B %Y", "%b %Y"):
                try:
                    return datetime.strptime(raw, fmt).strftime("%Y-%m-01")
                except ValueError:
                    pass
    return None


# ---------------------------------------------------------------------------
# Sheet parser
# ---------------------------------------------------------------------------

def _parse_response_sheet(
    xl: pd.ExcelFile,
    sheet: str,
    grain: str,
    source_file: str,
) -> list[dict]:
    """
    Parse one Response sheet.  Returns a list of row dicts ready for DB insert.
    Column layout (0-indexed):
        Col 0: (blank / index)
        Col 1: STR Code
        Col 2: Name of Establishment
        Col 3: City & State
        Col 4: Zip Code
        Col 5: Aff Date
        Col 6: Open Date
        Col 7: Rooms
        Col 8: Chg in Rms
        Col 9+: Date/status markers (B/L/T/blank)
    Header row: 6 for weekly, 5 for monthly.
    """
    try:
        df = pd.read_excel(xl, sheet_name=sheet, header=None)
    except Exception as exc:
        _logger.warning("  Could not read sheet %s from %s: %s", sheet, source_file, exc)
        return []

    market = SHEET_MARKET_MAP.get(sheet, sheet)

    # --- date from row 2 ---
    date_cell = df.iloc[2, 1] if df.shape[0] > 2 else None
    as_of_date = _parse_date(str(date_cell) if pd.notna(date_cell) else "", grain)
    if not as_of_date:
        _logger.warning("  Could not parse date from '%s' in %s / %s", date_cell, source_file, sheet)
        return []

    # --- determine first data row ---
    # Weekly: headers at row 6, data from row 7
    # Monthly: headers at row 5, data from row 6
    data_start = 7 if grain == "weekly" else 6

    rows: list[dict] = []
    for i in range(data_start, len(df)):
        row = df.iloc[i]
        str_code_raw = row.iloc[1]

        # Stop at legend / total rows (STR Code is not numeric)
        if pd.isna(str_code_raw):
            continue
        try:
            str_code = str(int(float(str_code_raw)))
        except (ValueError, TypeError):
            # Totals row or legend text — skip
            continue

        name     = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
        city_st  = str(row.iloc[3]).strip() if pd.notna(row.iloc[3]) else None
        zip_code = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else None
        rooms_raw = row.iloc[7]
        try:
            rooms = int(float(rooms_raw)) if pd.notna(rooms_raw) else None
        except (ValueError, TypeError):
            rooms = None

        rows.append({
            "grain":         grain,
            "as_of_date":    as_of_date,
            "market":        market,
            "property_name": name,
            "city_state":    city_st,
            "zip_code":      zip_code,
            "str_code":      str_code,
            "rooms":         rooms,
            "supply":        None,
            "demand":        None,
            "revenue":       None,
            "occ_pct":       None,
            "adr":           None,
            "revpar":        None,
            "source_file":   source_file,
        })

    return rows


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def load_response_sheets() -> int:
    conn = get_connection()
    ensure_table(conn)

    total_inserted = 0

    file_sets: list[tuple[str, Path]] = [
        ("weekly",  WEEKLY_DIR),
        ("monthly", MONTHLY_DIR),
    ]

    for grain, directory in file_sets:
        if not directory.exists():
            _logger.warning("Directory not found: %s", directory)
            continue

        files = _oldest_first(directory.glob("*.xls*"))
        _logger.info("Processing %d %s files from %s", len(files), grain, directory)

        for fpath in files:
            try:
                xl = pd.ExcelFile(fpath)
            except Exception as exc:
                _logger.warning("  Cannot open %s: %s", fpath.name, exc)
                continue

            file_rows: list[tuple] = []
            available_sheets = xl.sheet_names

            for sheet in RESPONSE_SHEETS:
                if sheet not in available_sheets:
                    _logger.debug("  Sheet '%s' not in %s — skipping", sheet, fpath.name)
                    continue

                parsed = _parse_response_sheet(xl, sheet, grain, fpath.name)
                for r in parsed:
                    file_rows.append((
                        r["grain"], r["as_of_date"], r["market"],
                        r["property_name"], r["city_state"], r["zip_code"],
                        r["str_code"], r["rooms"],
                        r["supply"], r["demand"], r["revenue"],
                        r["occ_pct"], r["adr"], r["revpar"],
                        r["source_file"],
                    ))

            if file_rows:
                conn.executemany(
                    """INSERT OR REPLACE INTO fact_str_response_markets
                       (grain, as_of_date, market, property_name, city_state,
                        zip_code, str_code, rooms, supply, demand, revenue,
                        occ_pct, adr, revpar, source_file)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    file_rows,
                )
                conn.commit()
                total_inserted += len(file_rows)
                _logger.info("  %s: inserted/replaced %d rows across response sheets",
                             fpath.name, len(file_rows))
            else:
                _logger.info("  %s: no response rows found", fpath.name)

    conn.close()
    _logger.info("Total rows inserted/replaced: %d", total_inserted)
    return total_inserted


if __name__ == "__main__":
    n = load_response_sheets()
    print(f"[load_str_response_sheets] Done — {n} rows loaded into fact_str_response_markets")
