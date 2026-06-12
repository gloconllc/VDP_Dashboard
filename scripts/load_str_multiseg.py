"""
load_str_multiseg.py

Loads multi-segment STR data from weekly and monthly STR Excel files.

Parses two sheets per file:
  1. "Multi Seg Seg"     → OCC%, ADR, RevPAR by segment (Trans/Grp/Cont/Total)
                           for each of the 6 comp-set markets, current week + % change vs LY
  2. "Multi-Seg Seg Raw" → Supply, Demand, Revenue by segment, TY and LY

All data goes into fact_str_group_metrics:
  (source, grain, as_of_date, market, segment, metric_name, metric_value, unit, data_period)

Markets: Dana Point, Newport Beach, La Jolla, Santa Barbara, Monterey-Carmel, Huntington Beach
"""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger("load_str_multiseg")

BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
WEEKLY_DIR   = PROJECT_ROOT / "data" / "str" / "weekly"
MONTHLY_DIR  = PROJECT_ROOT / "data" / "str" / "monthly"

# Normalize market names from sheet row labels
MARKET_NORM = {
    "Dana Point, CA+":       "Dana Point",
    "Newport Beach, CA+":    "Newport Beach",
    "La Jolla, CA+":         "La Jolla",
    "Santa Barbara, CA+":    "Santa Barbara",
    "Monterey-Carmel, CA+":  "Monterey-Carmel",
    "Huntington Beach, CA+": "Huntington Beach",
}

WEEK_RE  = re.compile(r"For the Week of\s+(\w+ \d+,?\s*\d{4})", re.IGNORECASE)
MONTH_RE = re.compile(r"For the Month of\s+(\w+ \d{4})", re.IGNORECASE)


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_str_group_metrics (
            source       TEXT NOT NULL,
            grain        TEXT NOT NULL,
            as_of_date   TEXT NOT NULL,
            market       TEXT NOT NULL,
            segment      TEXT NOT NULL,
            metric_name  TEXT NOT NULL,
            metric_value REAL,
            unit         TEXT NOT NULL,
            data_period  TEXT NOT NULL DEFAULT 'current',
            UNIQUE(source, grain, as_of_date, market, segment, metric_name, data_period)
                ON CONFLICT REPLACE
        )
    """)
    # Legacy column rename: property → market (no-op if already correct)
    try:
        conn.execute("ALTER TABLE fact_str_group_metrics ADD COLUMN data_period TEXT NOT NULL DEFAULT 'current'")
    except Exception:
        pass
    conn.commit()


def _parse_date(cell_text: str, grain: str) -> str | None:
    s = str(cell_text) if pd.notna(cell_text) else ""
    if grain == "weekly":
        m = WEEK_RE.search(s)
        if m:
            raw = m.group(1).replace(",", "").strip()
            for fmt in ("%B %d %Y", "%b %d %Y"):
                try:
                    return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    pass
    else:
        m = MONTH_RE.search(s)
        if m:
            raw = m.group(1).strip()
            for fmt in ("%B %Y", "%b %Y"):
                try:
                    return datetime.strptime(raw, fmt).strftime("%Y-%m-01")
                except ValueError:
                    pass
    return None


def _float(val) -> float | None:
    try:
        v = float(val)
        return None if v < 0 else v
    except (TypeError, ValueError):
        return None


def parse_seg_sheet(df: pd.DataFrame, grain: str) -> list[tuple]:
    """
    Parse 'Multi Seg Seg' sheet (header=None).

    Row 2  col 1: date string
    Row 8  col 1: "Segment", cols 2-5: Trans/Grp/Cont/Total  (repeated for each metric)
    Rows 9-14: market rows (Dana Point, Newport Beach, ...)

    Current Week columns:
      Occ%:   cols 2-5  (Trans/Grp/Cont/Total)
      ADR:    cols 6-9
      RevPAR: cols 10-13

    Percent Change columns (vs LY):
      Occ%:   cols 15-18
      ADR:    cols 19-22
      RevPAR: cols 23-26
    """
    rows: list[tuple] = []

    if len(df) < 10:
        return rows

    as_of_date = _parse_date(df.iloc[2, 1], grain)
    if not as_of_date:
        _logger.warning("  Seg sheet: could not parse date from '%s'", df.iloc[2, 1])
        return rows

    # Segment names from row 8 cols 2-5
    segs = [str(df.iloc[8, c]).strip() for c in range(2, 6) if pd.notna(df.iloc[8, c])]
    if len(segs) < 4:
        segs = ["Trans.", "Grp.", "Cont.", "Total"]

    metric_blocks = [
        ("occ_pct",  2,  "%",   "current"),
        ("adr",      6,  "USD", "current"),
        ("revpar",  10,  "USD", "current"),
        ("occ_pct", 15,  "%",   "pct_change"),
        ("adr",     19,  "USD", "pct_change"),
        ("revpar",  23,  "USD", "pct_change"),
    ]

    for row_idx in range(9, min(len(df), 20)):
        raw_market = df.iloc[row_idx, 1]
        if not isinstance(raw_market, str) or not raw_market.strip():
            continue
        market = MARKET_NORM.get(raw_market.strip(), raw_market.strip())

        for metric_name, col_start, unit, period in metric_blocks:
            for i, seg in enumerate(segs):
                val = _float(df.iloc[row_idx, col_start + i] if col_start + i < df.shape[1] else None)
                rows.append(("STR", grain, as_of_date, market, seg, metric_name, val, unit, period))

    return rows


def parse_raw_sheet(df: pd.DataFrame, grain: str) -> list[tuple]:
    """
    Parse 'Multi-Seg Seg Raw' sheet (header=None).

    Row 8 segments: col1=Segment, col2=Total, col3=Trans, col4=Grp, col5=Con,
                    col6=Total(Demand), col7=Trans, col8=Grp, col9=Con, col10=Total(Revenue)
    Metric layout (This Year):
      Supply  col 2  (Total only)
      Demand  cols 3-6  (Trans/Grp/Con/Total)
      Revenue cols 7-10 (Trans/Grp/Con/Total)
    Last Year same layout offset by 10 (cols 12-21)
    """
    rows: list[tuple] = []

    if len(df) < 10:
        return rows

    as_of_date = _parse_date(df.iloc[2, 1], grain)
    if not as_of_date:
        return rows

    demand_segs  = ["Trans.", "Grp.", "Con.", "Total"]
    revenue_segs = ["Trans.", "Grp.", "Con.", "Total"]

    for row_idx in range(9, min(len(df), 20)):
        raw_market = df.iloc[row_idx, 1]
        if not isinstance(raw_market, str) or not raw_market.strip():
            continue
        market = MARKET_NORM.get(raw_market.strip(), raw_market.strip())

        # Supply (Total only) — TY and LY
        supply_ty = _float(df.iloc[row_idx, 2]  if df.shape[1] > 2  else None)
        supply_ly = _float(df.iloc[row_idx, 12] if df.shape[1] > 12 else None)
        rows.append(("STR", grain, as_of_date, market, "Total", "supply", supply_ty, "room-nights", "current"))
        rows.append(("STR", grain, as_of_date, market, "Total", "supply", supply_ly, "room-nights", "prior_year"))

        # Demand (Trans/Grp/Con/Total) — TY cols 3-6, LY cols 13-16
        for i, seg in enumerate(demand_segs):
            ty = _float(df.iloc[row_idx, 3 + i]  if df.shape[1] > 3 + i  else None)
            ly = _float(df.iloc[row_idx, 13 + i] if df.shape[1] > 13 + i else None)
            rows.append(("STR", grain, as_of_date, market, seg, "demand", ty, "room-nights", "current"))
            rows.append(("STR", grain, as_of_date, market, seg, "demand", ly, "room-nights", "prior_year"))

        # Revenue (Trans/Grp/Con/Total) — TY cols 7-10, LY cols 17-20
        for i, seg in enumerate(revenue_segs):
            ty = _float(df.iloc[row_idx, 7 + i]  if df.shape[1] > 7 + i  else None)
            ly = _float(df.iloc[row_idx, 17 + i] if df.shape[1] > 17 + i else None)
            rows.append(("STR", grain, as_of_date, market, seg, "revenue", ty, "USD", "current"))
            rows.append(("STR", grain, as_of_date, market, seg, "revenue", ly, "USD", "prior_year"))

    return rows


def load_file(fpath: Path, grain: str, conn: sqlite3.Connection) -> int:
    try:
        xl = pd.ExcelFile(fpath)
    except Exception as e:
        _logger.warning("Cannot open %s: %s", fpath.name, e)
        return 0

    all_rows: list[tuple] = []

    if "Multi Seg Seg" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Multi Seg Seg", header=None)
        all_rows.extend(parse_seg_sheet(df, grain))

    if "Multi-Seg Seg Raw" in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name="Multi-Seg Seg Raw", header=None)
        all_rows.extend(parse_raw_sheet(df, grain))

    if all_rows:
        # Insert with property defaulting to 'All Properties' (multiseg data is market-level)
        rows_with_property = [(r[0], r[1], r[2], "All Properties", r[3], r[4], r[5], r[6], r[7], r[8]) for r in all_rows]
        conn.executemany(
            """INSERT OR REPLACE INTO fact_str_group_metrics
               (source, grain, as_of_date, property, market, segment, metric_name, metric_value, unit, data_period)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            rows_with_property,
        )
        conn.commit()
        _logger.info("  %s → %d rows", fpath.name, len(all_rows))

    return len(all_rows)


def main() -> None:
    conn = get_connection()
    ensure_table(conn)

    # Clear stale data with wrong market names
    conn.execute("DELETE FROM fact_str_group_metrics WHERE market NOT IN (?,?,?,?,?,?)",
                 ("Dana Point", "Newport Beach", "La Jolla", "Santa Barbara",
                  "Monterey-Carmel", "Huntington Beach"))
    conn.commit()

    total = 0
    for grain, directory in [("weekly", WEEKLY_DIR), ("monthly", MONTHLY_DIR)]:
        if not directory.exists():
            continue
        files = sorted(directory.glob("*.xls*"))
        _logger.info("Processing %d %s files", len(files), grain)
        for fpath in files:
            total += load_file(fpath, grain, conn)

    try:
        conn.execute(
            "INSERT INTO load_log (source, grain, file_name, rows_inserted) VALUES (?,?,?,?)",
            ("STR", "multiseg", "multiseg_weekly+monthly", total),
        )
        conn.commit()
    except Exception:
        pass

    _logger.info("Total rows inserted: %d", total)
    conn.close()
    print(f"[load_str_multiseg] Done — {total:,} rows into fact_str_group_metrics")


if __name__ == "__main__":
    main()
