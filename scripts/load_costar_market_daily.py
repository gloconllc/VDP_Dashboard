"""
load_costar_market_daily.py — CoStar submarket daily/monthly HospitalityDataGrid loader

Parses data/costar/daily_costar.xlsx and data/costar/monhtly_costar.xlsx —
CoStar's raw submarket performance export (same "HospitalityDataGrid" shape
as the STR exports) — into costar_market_daily / costar_market_monthly.

Distinct from costar_monthly_performance (parsed from CoStar PDF reports by
load_costar_reports.py): this is the current, higher-frequency CoStar export
straight from the source tool, including trailing 28-day / 12-month rollups
and (monthly only) capital-markets fields.
"""

import os
import re
import glob
import sqlite3
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
COSTAR_DIR = os.path.join(PROJECT_ROOT, "data", "costar")


def _latest_costar_export(dated_patterns, legacy_name):
    """CoStar's export naming drifted from the fixed 'daily_costar.xlsx' /
    'monhtly_costar.xlsx' to dated drops like 'Daily_08_26_26.xlsx'. Pick
    whichever candidate is actually newest by the date embedded in its
    filename (MM_DD_YY), falling back to file mtime, so a fresh dated
    export is picked up automatically without renaming it by hand first.
    Falls back to the legacy fixed filename if no dated file is present.
    """
    candidates = []
    for pat in dated_patterns:
        candidates += glob.glob(os.path.join(COSTAR_DIR, pat))
    legacy_path = os.path.join(COSTAR_DIR, legacy_name)
    if os.path.exists(legacy_path):
        candidates.append(legacy_path)
    if not candidates:
        return legacy_path  # let the loader report [SKIP] not found

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


DAILY_FILE = _latest_costar_export(["Daily_*.xlsx", "Daily_*.XLSX"], "daily_costar.xlsx")
MONTHLY_FILE = _latest_costar_export(["Monthly_*.xlsx", "Monthly_*.XLSX"], "monhtly_costar.xlsx")

DDL = """
CREATE TABLE IF NOT EXISTS costar_market_daily (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of_date              TEXT NOT NULL,
    day_of_week             TEXT,
    supply                  INTEGER,
    supply_yoy_pct          REAL,
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
    supply_28day            INTEGER,
    supply_28day_chg_pct    REAL,
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
    UNIQUE(as_of_date) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS costar_market_monthly (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    report_period               TEXT NOT NULL,
    supply                      INTEGER,
    supply_yoy_pct               REAL,
    demand                      INTEGER,
    demand_yoy_pct               REAL,
    revenue_usd                  REAL,
    revenue_yoy_pct              REAL,
    occupancy_pct                REAL,
    occupancy_yoy_pp             REAL,
    adr_usd                      REAL,
    adr_yoy_pct                  REAL,
    revpar_usd                   REAL,
    revpar_yoy_pct                REAL,
    ttm_supply                   INTEGER,
    ttm_supply_yoy_pct           REAL,
    ttm_demand                   INTEGER,
    ttm_demand_yoy_pct           REAL,
    ttm_revenue_usd              REAL,
    ttm_revenue_yoy_pct          REAL,
    ttm_occupancy_pct            REAL,
    ttm_occupancy_yoy_pp         REAL,
    ttm_adr_usd                  REAL,
    ttm_adr_yoy_pct              REAL,
    ttm_revpar_usd               REAL,
    ttm_revpar_yoy_pct           REAL,
    market_sale_price_per_room   REAL,
    ttm_sales_volume_usd         REAL,
    market_cap_rate_pct          REAL,
    loaded_at                    TEXT DEFAULT (datetime('now')),
    UNIQUE(report_period) ON CONFLICT REPLACE
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


def load_daily(conn: sqlite3.Connection) -> int:
    if not os.path.exists(DAILY_FILE):
        print(f"{ts()} [SKIP] {DAILY_FILE} not found")
        return 0

    df = pd.read_excel(DAILY_FILE, sheet_name="HospitalityDataGrid")
    cur = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        as_of = pd.to_datetime(row.get("Period"), errors="coerce")
        if pd.isna(as_of):
            continue
        cur.execute(
            """
            INSERT INTO costar_market_daily (
                as_of_date, day_of_week, supply, supply_yoy_pct, demand, demand_yoy_pct,
                revenue_usd, revenue_yoy_pct, occupancy_pct, occupancy_yoy_pp,
                adr_usd, adr_yoy_pct, revpar_usd, revpar_yoy_pct,
                supply_28day, supply_28day_chg_pct, demand_28day, demand_28day_chg_pct,
                revenue_28day_usd, revenue_28day_chg_pct, occupancy_28day_pct, occupancy_28day_chg_pp,
                adr_28day_usd, adr_28day_chg_pct, revpar_28day_usd, revpar_28day_chg_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                as_of.strftime("%Y-%m-%d"),
                str(row.get("Day of Week", "")).strip() or None,
                _i(row.get("Supply")), _pct(row.get("Supply Chg (YOY)")),
                _i(row.get("Demand")), _pct(row.get("Demand Chg (YOY)")),
                _f(row.get("Revenue")), _pct(row.get("Revenue Chg (YOY)")),
                _pct(row.get("Occupancy")), _pct(row.get("Occupancy Chg (YOY)")),
                _f(row.get("ADR")), _pct(row.get("ADR Chg (YOY)")),
                _f(row.get("RevPAR")), _pct(row.get("RevPAR Chg (YOY)")),
                _i(row.get("28 Day Supply")), _pct(row.get("28 Day Supply Chg")),
                _i(row.get("28 Day Demand")), _pct(row.get("28 Day Demand Chg")),
                _f(row.get("28 Day Revenue")), _pct(row.get("28 Day Revenue Chg")),
                _pct(row.get("28 Day Occupancy")), _pct(row.get("28 Day Occupancy Chg")),
                _f(row.get("28 Day ADR")), _pct(row.get("28 Day ADR Chg")),
                _f(row.get("28 Day RevPAR")), _pct(row.get("28 Day RevPAR Chg")),
            ),
        )
        count += 1
    conn.commit()
    return count


def load_monthly(conn: sqlite3.Connection) -> int:
    if not os.path.exists(MONTHLY_FILE):
        print(f"{ts()} [SKIP] {MONTHLY_FILE} not found")
        return 0

    df = pd.read_excel(MONTHLY_FILE, sheet_name="HospitalityDataGrid")
    cur = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        period_raw = row.get("Period")
        period_dt = pd.to_datetime(period_raw, errors="coerce")
        if pd.isna(period_dt):
            continue
        report_period = period_dt.strftime("%Y-%m")
        cur.execute(
            """
            INSERT INTO costar_market_monthly (
                report_period, supply, supply_yoy_pct, demand, demand_yoy_pct,
                revenue_usd, revenue_yoy_pct, occupancy_pct, occupancy_yoy_pp,
                adr_usd, adr_yoy_pct, revpar_usd, revpar_yoy_pct,
                ttm_supply, ttm_supply_yoy_pct, ttm_demand, ttm_demand_yoy_pct,
                ttm_revenue_usd, ttm_revenue_yoy_pct, ttm_occupancy_pct, ttm_occupancy_yoy_pp,
                ttm_adr_usd, ttm_adr_yoy_pct, ttm_revpar_usd, ttm_revpar_yoy_pct,
                market_sale_price_per_room, ttm_sales_volume_usd, market_cap_rate_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                report_period,
                _i(row.get("Supply")), _pct(row.get("Supply Chg (YOY)")),
                _i(row.get("Demand")), _pct(row.get("Demand Chg (YOY)")),
                _f(row.get("Revenue")), _pct(row.get("Revenue Chg (YOY)")),
                _pct(row.get("Occupancy")), _pct(row.get("Occupancy Chg (YOY)")),
                _f(row.get("ADR")), _pct(row.get("ADR Chg (YOY)")),
                _f(row.get("RevPAR")), _pct(row.get("RevPAR Chg (YOY)")),
                _i(row.get("12 Mo Supply")), _pct(row.get("12 Mo Supply Chg")),
                _i(row.get("12 Mo Demand")), _pct(row.get("12 Mo Demand Chg")),
                _f(row.get("12 Mo Revenue")), _pct(row.get("12 Mo Revenue Chg")),
                _pct(row.get("12 Mo Occupancy")), _pct(row.get("12 Mo Occupancy Chg")),
                _f(row.get("12 Mo ADR")), _pct(row.get("12 Mo ADR Chg")),
                _f(row.get("12 Mo RevPAR")), _pct(row.get("12 Mo RevPAR Chg")),
                _f(row.get("Market Sale Price/Room")), _f(row.get("12 Mo Sales Volume")),
                _pct(row.get("Market Cap Rate")),
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
    print(f"{ts()} [START] load_costar_market_daily.py")
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript(DDL)
    conn.commit()

    n_daily = load_daily(conn)
    print(f"{ts()} [{'OK  ' if n_daily else 'WARN'}] costar_market_daily: {n_daily} rows")
    log_load(conn, "daily", os.path.basename(DAILY_FILE), n_daily)

    n_monthly = load_monthly(conn)
    print(f"{ts()} [{'OK  ' if n_monthly else 'WARN'}] costar_market_monthly: {n_monthly} rows")
    log_load(conn, "monthly", os.path.basename(MONTHLY_FILE), n_monthly)

    conn.close()
    print(f"{ts()} [DONE] load_costar_market_daily.py complete")


if __name__ == "__main__":
    main()
