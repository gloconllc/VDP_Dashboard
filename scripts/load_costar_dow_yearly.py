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
load_costar_dow_yearly.py: two CoStar exports first delivered 2026-11-19.

  DOW_MM_DD_YY.xlsx     -> costar_dow_monthly
      Each month has eight rows: "Oct 2026" (all nights) and "Sat Oct 2026" .. "Sun Oct 2026".
      Same Newport Beach/Dana Point submarket as costar_market_monthly (the all-nights rows
      match it). 24 months.
  Yearly_MM_DD_YY.xlsx  -> costar_yearly
      One row per calendar year: supply, demand, revenue, occupancy, ADR, RevPAR and their
      year-over-year change, plus 12-month sales volume, delivered rooms and inventory growth.
      CoStar prints "-" for a missing value; it is stored as NULL. Years with no performance
      values are kept only when a capital-market value exists.

Units follow costar_weekpart_monthly: occupancy and every *_yoy_pct column in percent
(64.1, -1.1), dollars as dollars. The source holds fractions (0.641, -0.011).

Skip-safe: exits 0 when a file is missing. Newest file by date added (scripts/file_dates.py).
"""
import os
import re
import sqlite3
import sys
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
COSTAR_DIR = os.path.join(PROJECT_ROOT, "data", "costar")
sys.path.insert(0, BASE_DIR)
try:
    import file_dates as FD
except Exception:  # pragma: no cover
    FD = None

DOW_NAME_RE = re.compile(r"^(dow|day_of_week)_.*\.xlsx$", re.IGNORECASE)
YEARLY_NAME_RE = re.compile(r"^(yearly|annual)_.*\.xlsx$", re.IGNORECASE)
DOW_PERIOD_RE = re.compile(r"^(?:(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+)?([A-Za-z]{3})\s+(\d{4})$")
DOW_ORDER = {"mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6, "sun": 7}

DDL_DOW = """
CREATE TABLE IF NOT EXISTS costar_dow_monthly (
    report_period   TEXT NOT NULL,      -- YYYY-MM
    day_of_week     TEXT NOT NULL,      -- all | mon .. sun
    dow_order       INTEGER,            -- 0 for all, 1 Monday .. 7 Sunday
    supply          REAL, demand REAL, revenue_usd REAL,
    occupancy_pct   REAL, occupancy_yoy_pct REAL,
    adr_usd         REAL, adr_yoy_pct REAL,
    revpar_usd      REAL, revpar_yoy_pct REAL,
    source_file     TEXT,
    loaded_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(report_period, day_of_week) ON CONFLICT REPLACE
)
"""

DDL_YEARLY = """
CREATE TABLE IF NOT EXISTS costar_yearly (
    year                     INTEGER NOT NULL,
    supply                   REAL, supply_yoy_pct REAL,
    demand                   REAL, demand_yoy_pct REAL,
    revenue_usd              REAL, revenue_yoy_pct REAL,
    occupancy_pct            REAL, occupancy_yoy_pct REAL,
    adr_usd                  REAL, adr_yoy_pct REAL,
    revpar_usd               REAL, revpar_yoy_pct REAL,
    sales_volume_12mo_usd    REAL,
    delivered_rooms_12mo     REAL,
    inventory_growth_12mo_pct REAL,
    source_file              TEXT,
    loaded_at                TEXT DEFAULT (datetime('now')),
    UNIQUE(year) ON CONFLICT REPLACE
)
"""


def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _latest(rx):
    if not os.path.isdir(COSTAR_DIR):
        return None
    c = [os.path.join(COSTAR_DIR, f) for f in os.listdir(COSTAR_DIR) if rx.match(f)]
    if not c:
        return None
    if FD is not None:
        return FD.newest(c, PROJECT_ROOT)
    return max(c, key=os.path.getmtime)


def _f(v, scale=1.0):
    """Number or None. CoStar's '-' and blanks become None, never 0."""
    v = pd.to_numeric(v, errors="coerce")
    return round(float(v) * scale, 4) if pd.notna(v) else None


def _col(df, prefix):
    """First column whose header starts with prefix (CoStar truncates long headers with '...')."""
    for c in df.columns:
        if str(c).strip().lower().startswith(prefix.lower()):
            return c
    return None


def _log(conn, grain, path, n):
    try:
        conn.execute("INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at) VALUES (?,?,?,?,?)",
                     ("CoStar", grain, os.path.basename(path), n, ts()))
    except sqlite3.Error:
        pass


def load_dow(conn):
    path = _latest(DOW_NAME_RE)
    if not path:
        print(f"[{ts()}] [SKIP] no DOW_ export in data/costar/")
        return 0
    df = pd.read_excel(path)
    rows = []
    for _, r in df.iterrows():
        m = DOW_PERIOD_RE.match(str(r.get("Period", "")).strip())
        if not m:
            continue
        dow = (m.group(1) or "all").lower()
        period = datetime.strptime(f"{m.group(2)} {m.group(3)}", "%b %Y").strftime("%Y-%m")
        occ = _f(r.get("Occupancy"), 100)
        if occ is None and _f(r.get("ADR")) is None:
            continue
        rows.append((period, dow, DOW_ORDER.get(dow, 0), _f(r.get("Supply")), _f(r.get("Demand")),
                     _f(r.get("Revenue")), occ, _f(r.get("Occupancy Chg (YOY)"), 100),
                     _f(r.get("ADR")), _f(r.get("ADR Chg (YOY)"), 100),
                     _f(r.get("RevPAR")), _f(r.get("RevPAR Chg (YOY)"), 100), os.path.basename(path)))
    conn.execute(DDL_DOW)
    conn.executemany("INSERT INTO costar_dow_monthly (report_period, day_of_week, dow_order, supply, demand, "
                     "revenue_usd, occupancy_pct, occupancy_yoy_pct, adr_usd, adr_yoy_pct, revpar_usd, "
                     "revpar_yoy_pct, source_file) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    _log(conn, "dow_monthly", path, len(rows))
    print(f"[{ts()}] [{'OK  ' if rows else 'WARN'}] costar_dow_monthly: {len(rows)} rows from {os.path.basename(path)}")
    return len(rows)


def load_yearly(conn):
    path = _latest(YEARLY_NAME_RE)
    if not path:
        print(f"[{ts()}] [SKIP] no Yearly_ export in data/costar/")
        return 0
    df = pd.read_excel(path)
    c_sv, c_del, c_inv = _col(df, "12 Mo Sales Volume"), _col(df, "12 Mo Delivered"), _col(df, "12 Mo Inventory")
    rows = []
    for _, r in df.iterrows():
        y = pd.to_numeric(r.get("Period"), errors="coerce")
        if pd.isna(y):
            continue
        perf = [_f(r.get("Supply")), _f(r.get("Demand")), _f(r.get("Revenue")), _f(r.get("Occupancy"), 100),
                _f(r.get("ADR")), _f(r.get("RevPAR"))]
        sv = _f(r.get(c_sv)) if c_sv else None
        dl = _f(r.get(c_del)) if c_del else None
        inv = _f(r.get(c_inv), 100) if c_inv else None
        # A blank performance year with only "0 delivered rooms" carries no information.
        if all(v is None for v in perf) and sv is None and inv is None and not dl:
            continue
        rows.append((int(y), perf[0], _f(r.get("Supply Chg (YOY)"), 100), perf[1], _f(r.get("Demand Chg (YOY)"), 100),
                     perf[2], _f(r.get("Revenue Chg (YOY)"), 100), perf[3], _f(r.get("Occupancy Chg (YOY)"), 100),
                     perf[4], _f(r.get("ADR Chg (YOY)"), 100), perf[5], _f(r.get("RevPAR Chg (YOY)"), 100),
                     sv, dl, inv, os.path.basename(path)))
    conn.execute(DDL_YEARLY)
    conn.executemany("INSERT INTO costar_yearly (year, supply, supply_yoy_pct, demand, demand_yoy_pct, revenue_usd, "
                     "revenue_yoy_pct, occupancy_pct, occupancy_yoy_pct, adr_usd, adr_yoy_pct, revpar_usd, "
                     "revpar_yoy_pct, sales_volume_12mo_usd, delivered_rooms_12mo, inventory_growth_12mo_pct, "
                     "source_file) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    _log(conn, "yearly", path, len(rows))
    print(f"[{ts()}] [{'OK  ' if rows else 'WARN'}] costar_yearly: {len(rows)} rows from {os.path.basename(path)}")
    return len(rows)


def main():
    print(f"[{ts()}] [START] load_costar_dow_yearly.py")
    conn = sqlite3.connect(DB_PATH)
    load_dow(conn)
    load_yearly(conn)
    conn.commit()
    conn.close()
    print(f"[{ts()}] [DONE] load_costar_dow_yearly.py complete")


if __name__ == "__main__":
    main()
