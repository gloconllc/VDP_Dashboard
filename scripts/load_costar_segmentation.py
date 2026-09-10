"""
load_costar_segmentation.py — CoStar submarket segment (Transient/Group/Contract)
daily/monthly HospitalityDataGrid loader, plus the accompanying property
Participation roster.

CoStar ships these as a companion batch alongside the plain daily/monthly
export handled by load_costar_market_daily.py:

  data/costar/Daily_Seg_MM_DD_YY.xlsx    (or daily_seg_MM_DD_YY.xlsx)
      → same HospitalityDataGrid shape as the plain daily export, but broken
        out by Transient / Group / Contract business mix, including the
        trailing 28-day rollups.
  data/costar/Monthly_Seg_MM_DD_YY.xlsx  (or monthly_seg_MM_DD_YY.xlsx)
      → same idea, monthly grain, with 12-month (TTM) rollups.
  data/costar/Particpation_MM_DD_YY.xlsx (CoStar's own spelling varies —
      also seen as "particp_MM_DD_YY.xlsx")
      → roster of properties in the submarket comp set, with a rolling
        monthly participation marker (e.g. "●") per property.

Loads into:
  costar_market_daily_segment    (as_of_date, segment) unique
  costar_market_monthly_segment  (report_period, segment) unique
  costar_participation           (snapshot_date, building_name, period_month) unique

Distinct from load_costar_market_daily.py (aggregate, unsegmented daily/monthly)
and load_costar_reports.py (PDF-parsed historical performance). Skip-safe:
any file that isn't found or fails to parse is logged and skipped rather than
raising, so a missing/late CoStar drop never blocks the rest of the pipeline.
"""

import os
import re
import sqlite3
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
COSTAR_DIR = os.path.join(PROJECT_ROOT, "data", "costar")

SEGMENTS = ["Transient", "Group", "Contract"]

MONTH_COL_RE = re.compile(r"^[A-Za-z]{3}\s+\d{4}$")  # e.g. "Jul 2024"


def _find_latest(regex_pattern: str):
    """Return the newest file in COSTAR_DIR whose name matches regex_pattern
    (case-insensitive), newest by the MM_DD_YY date embedded in the filename,
    falling back to file mtime. Returns None if nothing matches.

    Regex-based (not glob) so CoStar's inconsistent capitalization —
    "Daily_Seg_..." one month, "daily_seg_..." the next — is handled without
    renaming files by hand, and so it never has to be reconciled against the
    glob patterns used by load_costar_market_daily.py for the plain (non-seg)
    export.
    """
    if not os.path.isdir(COSTAR_DIR):
        return None
    rx = re.compile(regex_pattern, re.IGNORECASE)
    candidates = [
        os.path.join(COSTAR_DIR, f) for f in os.listdir(COSTAR_DIR) if rx.match(f)
    ]
    if not candidates:
        return None

    def _sort_key(path):
        m = re.search(r"(\d{2})_(\d{2})_(\d{2})", os.path.basename(path))
        if m:
            mm, dd, yy = (int(x) for x in m.groups())
            try:
                return datetime(2000 + yy, mm, dd)
            except ValueError:
                pass
        return datetime.fromtimestamp(os.path.getmtime(path))

    return max(candidates, key=_sort_key)


# Digit-anchored via the MM_DD_YY date so these can never accidentally match
# each other or the plain Daily_/Monthly_ export from load_costar_market_daily.py.
DAILY_SEG_FILE = _find_latest(r"daily.?seg.*\.xlsx$")
MONTHLY_SEG_FILE = _find_latest(r"monthly.?seg.*\.xlsx$")
# CoStar has spelled this "Particpation_MM_DD_YY.xlsx" and "particp_MM_DD_YY.xlsx"
# in different months — match on the shared "partic" stem, case-insensitive.
PARTICIPATION_FILE = _find_latest(r"partic.*\.xlsx$")

DDL = """
CREATE TABLE IF NOT EXISTS costar_market_daily_segment (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of_date              TEXT NOT NULL,
    day_of_week             TEXT,
    segment                 TEXT NOT NULL,   -- 'Transient' | 'Group' | 'Contract'
    demand                  INTEGER,
    demand_yoy_pct          REAL,
    revenue_usd             REAL,
    revenue_yoy_pct         REAL,
    occupancy_pct           REAL,
    occupancy_yoy_pp        REAL,
    adr_usd                 REAL,
    adr_yoy_pct             REAL,
    revpar_usd              REAL,
    revpar_yoy_pct          REAL,
    demand_28day            INTEGER,
    demand_28day_chg_pct    REAL,
    revenue_28day_usd       REAL,
    revenue_28day_chg_pct   REAL,
    occupancy_28day_pct     REAL,
    occupancy_28day_chg_pp  REAL,
    adr_28day_usd           REAL,
    adr_28day_chg_pct       REAL,
    revpar_28day_usd        REAL,
    revpar_28day_chg_pct    REAL,
    loaded_at               TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of_date, segment) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS costar_market_monthly_segment (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    report_period           TEXT NOT NULL,
    segment                 TEXT NOT NULL,
    demand                  INTEGER,
    demand_yoy_pct          REAL,
    revenue_usd             REAL,
    revenue_yoy_pct         REAL,
    occupancy_pct           REAL,
    occupancy_yoy_pp        REAL,
    adr_usd                 REAL,
    adr_yoy_pct             REAL,
    revpar_usd              REAL,
    revpar_yoy_pct          REAL,
    ttm_demand              INTEGER,
    ttm_demand_yoy_pct      REAL,
    ttm_revenue_usd         REAL,
    ttm_revenue_yoy_pct     REAL,
    ttm_occupancy_pct       REAL,
    ttm_occupancy_yoy_pp    REAL,
    ttm_adr_usd             REAL,
    ttm_adr_yoy_pct         REAL,
    ttm_revpar_usd          REAL,
    ttm_revpar_yoy_pct      REAL,
    loaded_at               TEXT DEFAULT (datetime('now')),
    UNIQUE(report_period, segment) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS costar_participation (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date      TEXT NOT NULL,      -- date of the export batch (from filename)
    building_name      TEXT NOT NULL,
    property_id        TEXT,
    city               TEXT,
    state              TEXT,
    zip_code           TEXT,
    property_class     TEXT,
    hotel_open_date    TEXT,
    rooms              INTEGER,
    room_change_flag   TEXT,
    room_change_1      TEXT,
    room_change_2      TEXT,
    room_change_3      TEXT,
    period_month       TEXT NOT NULL,      -- e.g. '2026-07'
    participating      INTEGER NOT NULL,   -- 1 if marked (e.g. '●'), else 0
    loaded_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, building_name, period_month) ON CONFLICT REPLACE
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


def _pct(val):
    f = _f(val)
    return None if f is None else round(f * 100, 4)


def _i(val):
    f = _f(val)
    return None if f is None else int(f)


def _clean_str(val):
    """Return a stripped string, or None for NaN/empty. Collapses pandas'
    float-ified whole numbers (e.g. ZIP '92624.0', Property ID '4170838.0')
    back down to plain integer strings."""
    try:
        if val is None or pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None
    if s.endswith(".0"):
        try:
            return str(int(float(s)))
        except ValueError:
            pass
    return s


def _seg_metrics(row, seg: str, grain: str) -> dict:
    """Pull one segment's (Transient/Group/Contract) metrics out of a
    HospitalityDataGrid row. Column lookup is exact-name, not positional, so
    it's independent of whatever column order CoStar happens to export."""
    d = {
        "segment": seg,
        "demand": _i(row.get(f"{seg} Demand")),
        "demand_yoy_pct": _pct(row.get(f"{seg} Demand Chg (YOY)")),
        "revenue_usd": _f(row.get(f"{seg} Revenue")),
        "revenue_yoy_pct": _pct(row.get(f"{seg} Revenue Chg (YOY)")),
        "occupancy_pct": _pct(row.get(f"{seg} Occupancy")),
        "occupancy_yoy_pp": _pct(row.get(f"{seg} Occupancy Chg (YOY)")),
        "adr_usd": _f(row.get(f"{seg} ADR")),
        "adr_yoy_pct": _pct(row.get(f"{seg} ADR Chg (YOY)")),
        "revpar_usd": _f(row.get(f"{seg} RevPAR")),
        "revpar_yoy_pct": _pct(row.get(f"{seg} RevPAR Chg (YOY)")),
    }
    if grain == "daily":
        d.update({
            "demand_28day": _i(row.get(f"28 Day {seg} Demand")),
            "demand_28day_chg_pct": _pct(row.get(f"28 Day {seg} Demand Chg")),
            "revenue_28day_usd": _f(row.get(f"28 Day {seg} Revenue")),
            "revenue_28day_chg_pct": _pct(row.get(f"28 Day {seg} Revenue Chg")),
            "occupancy_28day_pct": _pct(row.get(f"28 Day {seg} Occupancy")),
            "occupancy_28day_chg_pp": _pct(row.get(f"28 Day {seg} Occupancy Chg")),
            "adr_28day_usd": _f(row.get(f"28 Day {seg} ADR")),
            "adr_28day_chg_pct": _pct(row.get(f"28 Day {seg} ADR Chg")),
            "revpar_28day_usd": _f(row.get(f"28 Day {seg} RevPAR")),
            "revpar_28day_chg_pct": _pct(row.get(f"28 Day {seg} RevPAR Chg")),
        })
    else:
        d.update({
            "ttm_demand": _i(row.get(f"12 Mo {seg} Demand")),
            "ttm_demand_yoy_pct": _pct(row.get(f"12 Mo {seg} Demand Chg")),
            "ttm_revenue_usd": _f(row.get(f"12 Mo {seg} Revenue")),
            "ttm_revenue_yoy_pct": _pct(row.get(f"12 Mo {seg} Revenue Chg")),
            "ttm_occupancy_pct": _pct(row.get(f"12 Mo {seg} Occupancy")),
            "ttm_occupancy_yoy_pp": _pct(row.get(f"12 Mo {seg} Occupancy Chg")),
            "ttm_adr_usd": _f(row.get(f"12 Mo {seg} ADR")),
            "ttm_adr_yoy_pct": _pct(row.get(f"12 Mo {seg} ADR Chg")),
            "ttm_revpar_usd": _f(row.get(f"12 Mo {seg} RevPAR")),
            "ttm_revpar_yoy_pct": _pct(row.get(f"12 Mo {seg} RevPAR Chg")),
        })
    return d


def load_daily_segment(conn: sqlite3.Connection) -> int:
    if not DAILY_SEG_FILE or not os.path.exists(DAILY_SEG_FILE):
        print(f"{ts()} [SKIP] no Daily Seg export found in {COSTAR_DIR}")
        return 0

    df = pd.read_excel(DAILY_SEG_FILE, sheet_name="HospitalityDataGrid")
    cur = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        as_of = pd.to_datetime(row.get("Period"), errors="coerce")
        if pd.isna(as_of):
            continue
        as_of_str = as_of.strftime("%Y-%m-%d")
        dow = str(row.get("Day of Week", "")).strip() or None

        for seg in SEGMENTS:
            m = _seg_metrics(row, seg, "daily")
            cur.execute(
                """
                INSERT INTO costar_market_daily_segment (
                    as_of_date, day_of_week, segment, demand, demand_yoy_pct,
                    revenue_usd, revenue_yoy_pct, occupancy_pct, occupancy_yoy_pp,
                    adr_usd, adr_yoy_pct, revpar_usd, revpar_yoy_pct,
                    demand_28day, demand_28day_chg_pct, revenue_28day_usd, revenue_28day_chg_pct,
                    occupancy_28day_pct, occupancy_28day_chg_pp, adr_28day_usd, adr_28day_chg_pct,
                    revpar_28day_usd, revpar_28day_chg_pct
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    as_of_str, dow, m["segment"], m["demand"], m["demand_yoy_pct"],
                    m["revenue_usd"], m["revenue_yoy_pct"], m["occupancy_pct"], m["occupancy_yoy_pp"],
                    m["adr_usd"], m["adr_yoy_pct"], m["revpar_usd"], m["revpar_yoy_pct"],
                    m["demand_28day"], m["demand_28day_chg_pct"], m["revenue_28day_usd"], m["revenue_28day_chg_pct"],
                    m["occupancy_28day_pct"], m["occupancy_28day_chg_pp"], m["adr_28day_usd"], m["adr_28day_chg_pct"],
                    m["revpar_28day_usd"], m["revpar_28day_chg_pct"],
                ),
            )
            count += 1
    conn.commit()
    return count


def load_monthly_segment(conn: sqlite3.Connection) -> int:
    if not MONTHLY_SEG_FILE or not os.path.exists(MONTHLY_SEG_FILE):
        print(f"{ts()} [SKIP] no Monthly Seg export found in {COSTAR_DIR}")
        return 0

    df = pd.read_excel(MONTHLY_SEG_FILE, sheet_name="HospitalityDataGrid")
    cur = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        period_dt = pd.to_datetime(row.get("Period"), errors="coerce")
        if pd.isna(period_dt):
            continue
        report_period = period_dt.strftime("%Y-%m")

        for seg in SEGMENTS:
            m = _seg_metrics(row, seg, "monthly")
            cur.execute(
                """
                INSERT INTO costar_market_monthly_segment (
                    report_period, segment, demand, demand_yoy_pct,
                    revenue_usd, revenue_yoy_pct, occupancy_pct, occupancy_yoy_pp,
                    adr_usd, adr_yoy_pct, revpar_usd, revpar_yoy_pct,
                    ttm_demand, ttm_demand_yoy_pct, ttm_revenue_usd, ttm_revenue_yoy_pct,
                    ttm_occupancy_pct, ttm_occupancy_yoy_pp, ttm_adr_usd, ttm_adr_yoy_pct,
                    ttm_revpar_usd, ttm_revpar_yoy_pct
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    report_period, m["segment"], m["demand"], m["demand_yoy_pct"],
                    m["revenue_usd"], m["revenue_yoy_pct"], m["occupancy_pct"], m["occupancy_yoy_pp"],
                    m["adr_usd"], m["adr_yoy_pct"], m["revpar_usd"], m["revpar_yoy_pct"],
                    m["ttm_demand"], m["ttm_demand_yoy_pct"], m["ttm_revenue_usd"], m["ttm_revenue_yoy_pct"],
                    m["ttm_occupancy_pct"], m["ttm_occupancy_yoy_pp"], m["ttm_adr_usd"], m["ttm_adr_yoy_pct"],
                    m["ttm_revpar_usd"], m["ttm_revpar_yoy_pct"],
                ),
            )
            count += 1
    conn.commit()
    return count


def _snapshot_date_from_filename(path: str) -> str:
    m = re.search(r"(\d{2})_(\d{2})_(\d{2})", os.path.basename(path))
    if m:
        mm, dd, yy = (int(x) for x in m.groups())
        try:
            return datetime(2000 + yy, mm, dd).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d")


def load_participation(conn: sqlite3.Connection) -> int:
    if not PARTICIPATION_FILE or not os.path.exists(PARTICIPATION_FILE):
        print(f"{ts()} [SKIP] no Participation export found in {COSTAR_DIR}")
        return 0

    df = pd.read_excel(PARTICIPATION_FILE, sheet_name="Participation")
    snapshot_date = _snapshot_date_from_filename(PARTICIPATION_FILE)
    month_cols = [c for c in df.columns if MONTH_COL_RE.match(str(c).strip())]

    cur = conn.cursor()
    # Replace this snapshot wholesale so a re-run doesn't leave stale rows if
    # a property drops out of (or into) the roster between exports.
    cur.execute("DELETE FROM costar_participation WHERE snapshot_date = ?", (snapshot_date,))

    count = 0
    for _, row in df.iterrows():
        name = _clean_str(row.get("Building Name"))
        if not name:
            continue
        rooms = _i(row.get("Rooms"))

        for month_col in month_cols:
            period_month = datetime.strptime(month_col.strip(), "%b %Y").strftime("%Y-%m")
            mark = row.get(month_col)
            participating = 1 if _clean_str(mark) else 0
            cur.execute(
                """
                INSERT INTO costar_participation (
                    snapshot_date, building_name, property_id, city, state, zip_code,
                    property_class, hotel_open_date, rooms, room_change_flag,
                    room_change_1, room_change_2, room_change_3, period_month, participating
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    snapshot_date, name, _clean_str(row.get("Property ID")),
                    _clean_str(row.get("City")), _clean_str(row.get("State")),
                    _clean_str(row.get("ZIP Code")), _clean_str(row.get("Class")),
                    _clean_str(row.get("Hotel Open Date")), rooms,
                    _clean_str(row.get("Room Change?")), _clean_str(row.get("Room Change 1")),
                    _clean_str(row.get("Room Change 2")), _clean_str(row.get("Room Change 3")),
                    period_month, participating,
                ),
            )
            count += 1
    conn.commit()
    return count


def log_load(conn, grain, file_name, rows):
    conn.execute(
        "INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at) "
        "VALUES ('CoStar', ?, ?, ?, datetime('now'))",
        (grain, file_name, rows),
    )
    conn.commit()


def main():
    print(f"{ts()} [START] load_costar_segmentation.py")
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript(DDL)
    conn.commit()

    n_daily = load_daily_segment(conn)
    print(f"{ts()} [{'OK  ' if n_daily else 'WARN'}] costar_market_daily_segment: {n_daily} rows")
    log_load(conn, "daily_segment", os.path.basename(DAILY_SEG_FILE) if DAILY_SEG_FILE else "(none found)", n_daily)

    n_monthly = load_monthly_segment(conn)
    print(f"{ts()} [{'OK  ' if n_monthly else 'WARN'}] costar_market_monthly_segment: {n_monthly} rows")
    log_load(conn, "monthly_segment", os.path.basename(MONTHLY_SEG_FILE) if MONTHLY_SEG_FILE else "(none found)", n_monthly)

    n_particip = load_participation(conn)
    print(f"{ts()} [{'OK  ' if n_particip else 'WARN'}] costar_participation: {n_particip} rows")
    log_load(conn, "participation", os.path.basename(PARTICIPATION_FILE) if PARTICIPATION_FILE else "(none found)", n_particip)

    conn.close()
    print(f"{ts()} [DONE] load_costar_segmentation.py complete")


if __name__ == "__main__":
    main()
