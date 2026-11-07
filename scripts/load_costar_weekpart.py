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
load_costar_weekpart.py — CoStar weekday vs. weekend monthly export → costar_weekpart_monthly

Reads the newest CoStar "Weekend_MM_DD_YY.xlsx" / "weekday_weekend_monthly_MM_DD_YY.xlsx"
export in data/costar/ (newest by DATE ADDED, scripts/file_dates.py). Each month has three
rows in the source: "Sep 2026" (all nights), "Weekend Sep 2026" and "Weekday Sep 2026".
Same CoStar submarket as costar_market_monthly (the "all nights" rows match it exactly).

Added 2026-11-05. Skip-safe: exits 0 when no file is found.
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

NAME_RE = re.compile(r"^(weekend|weekday_weekend).*\.xlsx$", re.IGNORECASE)
PERIOD_RE = re.compile(r"^(?:(Weekend|Weekday)\s+)?([A-Za-z]{3})\s+(\d{4})$")

DDL = """
CREATE TABLE IF NOT EXISTS costar_weekpart_monthly (
    report_period   TEXT NOT NULL,      -- YYYY-MM
    day_part        TEXT NOT NULL,      -- all | weekend | weekday
    supply          REAL, demand REAL, revenue_usd REAL,
    occupancy_pct   REAL, occupancy_yoy_pct REAL,
    adr_usd         REAL, adr_yoy_pct REAL,
    revpar_usd      REAL, revpar_yoy_pct REAL,
    source_file     TEXT,
    loaded_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(report_period, day_part) ON CONFLICT REPLACE
)
"""


def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def latest_file():
    if not os.path.isdir(COSTAR_DIR):
        return None
    c = [os.path.join(COSTAR_DIR, f) for f in os.listdir(COSTAR_DIR) if NAME_RE.match(f)]
    if not c:
        return None
    if FD is not None:
        return FD.newest(c, PROJECT_ROOT)
    return max(c, key=os.path.getmtime)


def _f(v, scale=1.0):
    v = pd.to_numeric(v, errors="coerce")
    return float(v) * scale if pd.notna(v) else None


def main():
    print(f"[{ts()}] [START] load_costar_weekpart.py")
    path = latest_file()
    if not path:
        print(f"[{ts()}] [SKIP] no Weekend_/weekday_weekend_ export in data/costar/")
        return
    df = pd.read_excel(path)
    rows = []
    for _, r in df.iterrows():
        m = PERIOD_RE.match(str(r.get("Period", "")).strip())
        if not m:
            continue
        part = (m.group(1) or "all").lower()
        period = datetime.strptime(f"{m.group(2)} {m.group(3)}", "%b %Y").strftime("%Y-%m")
        rows.append((period, part, _f(r.get("Supply")), _f(r.get("Demand")), _f(r.get("Revenue")),
                     _f(r.get("Occupancy"), 100), _f(r.get("Occupancy Chg (YOY)"), 100),
                     _f(r.get("ADR")), _f(r.get("ADR Chg (YOY)"), 100),
                     _f(r.get("RevPAR")), _f(r.get("RevPAR Chg (YOY)"), 100), os.path.basename(path)))
    conn = sqlite3.connect(DB_PATH)
    conn.execute(DDL)
    conn.executemany("INSERT INTO costar_weekpart_monthly (report_period, day_part, supply, demand, revenue_usd, "
                     "occupancy_pct, occupancy_yoy_pct, adr_usd, adr_yoy_pct, revpar_usd, revpar_yoy_pct, source_file) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    try:
        conn.execute("INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at) VALUES (?,?,?,?,?)",
                     ("CoStar", "weekpart_monthly", os.path.basename(path), len(rows), ts()))
    except sqlite3.Error:
        pass
    conn.commit()
    conn.close()
    print(f"[{ts()}] [{'OK  ' if rows else 'WARN'}] costar_weekpart_monthly: {len(rows)} rows from {os.path.basename(path)}")
    print(f"[{ts()}] [DONE] load_costar_weekpart.py complete")


if __name__ == "__main__":
    main()
