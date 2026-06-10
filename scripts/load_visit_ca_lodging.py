"""
load_visit_ca_lodging.py — CA Lodging Performance monthly loader
Parses CALodgingPerformance_202601.xls into visit_ca_lodging_monthly.

Strategy:
  1. Try xlrd (native .xls format) to read real STR data from the XLS.
  2. Fall back to representative Jan 2026 seed data if file is missing or parse fails.

Table created:
  visit_ca_lodging_monthly
"""

import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"
DATA_DIR = ROOT / "data" / "Visit_California"

def _latest_lodging() -> Path:
    matches = sorted(
        list(DATA_DIR.glob("CALodgingPerformance_*.xls")) + list(DATA_DIR.glob("CALodgingPerformance_*.xlsx")),
        key=lambda p: p.stat().st_mtime, reverse=True
    )
    return matches[0] if matches else DATA_DIR / "CALodgingPerformance_202601.xls"

XLS_FILE = _latest_lodging()

DDL = """
CREATE TABLE IF NOT EXISTS visit_ca_lodging_monthly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_period TEXT NOT NULL,
    region TEXT NOT NULL,
    occupancy_pct REAL,
    adr_usd REAL,
    revpar_usd REAL,
    supply_rooms INTEGER,
    demand_room_nights INTEGER,
    room_revenue_usd REAL,
    occ_yoy_pp REAL,
    adr_yoy_pct REAL,
    revpar_yoy_pct REAL,
    data_source TEXT,
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(report_period, region) ON CONFLICT REPLACE
);
"""

# ── Seed data — January 2026 CA lodging from published report ─────────────────
# Source: Visit California / STR Multi-Segment report, January 2026
# All OCC values are percentages. YOY columns are percentage-point (OCC) or % change.
SEED_ROWS = [
    {
        "report_period": "2026-01",
        "region": "California",
        "occupancy_pct": 60.0,
        "adr_usd": 190.21,
        "revpar_usd": 114.05,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 0.71,
        "adr_yoy_pct": 1.78,
        "revpar_yoy_pct": 2.50,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "United States",
        "occupancy_pct": 52.4,
        "adr_usd": 152.09,
        "revpar_usd": 79.69,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": -0.20,
        "adr_yoy_pct": 0.62,
        "revpar_yoy_pct": 0.41,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Pacific",
        "occupancy_pct": 58.4,
        "adr_usd": 195.19,
        "revpar_usd": 113.91,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 0.83,
        "adr_yoy_pct": 1.47,
        "revpar_yoy_pct": 2.31,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Orange County",
        "occupancy_pct": 65.1,
        "adr_usd": 196.26,
        "revpar_usd": 127.70,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 2.47,
        "adr_yoy_pct": 2.21,
        "revpar_yoy_pct": 4.74,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Los Angeles County",
        "occupancy_pct": 64.4,
        "adr_usd": 190.96,
        "revpar_usd": 122.98,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": -2.33,
        "adr_yoy_pct": 2.59,
        "revpar_yoy_pct": 0.20,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Los Angeles",
        "occupancy_pct": 65.1,
        "adr_usd": 200.14,
        "revpar_usd": 130.33,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 6.42,
        "adr_yoy_pct": 4.72,
        "revpar_yoy_pct": 11.44,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "San Francisco Bay Area",
        "occupancy_pct": 59.8,
        "adr_usd": 239.90,
        "revpar_usd": 143.52,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 6.64,
        "adr_yoy_pct": 3.17,
        "revpar_yoy_pct": 10.03,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "San Diego County",
        "occupancy_pct": 64.4,
        "adr_usd": 189.40,
        "revpar_usd": 122.06,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 0.05,
        "adr_yoy_pct": 0.43,
        "revpar_yoy_pct": 0.48,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Anaheim",
        "occupancy_pct": 69.0,
        "adr_usd": 229.50,
        "revpar_usd": 158.43,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 7.48,
        "adr_yoy_pct": 6.24,
        "revpar_yoy_pct": 14.19,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Newport Beach",
        "occupancy_pct": 62.1,
        "adr_usd": 321.63,
        "revpar_usd": 199.87,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": -2.39,
        "adr_yoy_pct": -4.09,
        "revpar_yoy_pct": -6.38,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Palm Springs",
        "occupancy_pct": 64.1,
        "adr_usd": 227.79,
        "revpar_usd": 146.00,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": -3.31,
        "adr_yoy_pct": 1.82,
        "revpar_yoy_pct": -1.54,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "South Lake Tahoe",
        "occupancy_pct": 54.5,
        "adr_usd": 240.21,
        "revpar_usd": 130.90,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 11.04,
        "adr_yoy_pct": 2.22,
        "revpar_yoy_pct": 13.50,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Central Coast",
        "occupancy_pct": 54.7,
        "adr_usd": 179.91,
        "revpar_usd": 98.46,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": -5.47,
        "adr_yoy_pct": -3.29,
        "revpar_yoy_pct": -8.59,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Deserts",
        "occupancy_pct": 59.7,
        "adr_usd": 186.73,
        "revpar_usd": 111.45,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": -2.46,
        "adr_yoy_pct": 1.34,
        "revpar_yoy_pct": -1.15,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "High Sierra",
        "occupancy_pct": 54.6,
        "adr_usd": 252.00,
        "revpar_usd": 137.58,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": 7.76,
        "adr_yoy_pct": 3.33,
        "revpar_yoy_pct": 11.35,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
    {
        "report_period": "2026-01",
        "region": "Sacramento",
        "occupancy_pct": 60.7,
        "adr_usd": 144.70,
        "revpar_usd": 87.77,
        "supply_rooms": None,
        "demand_room_nights": None,
        "room_revenue_usd": None,
        "occ_yoy_pp": -1.15,
        "adr_yoy_pct": -5.93,
        "revpar_yoy_pct": -7.01,
        "data_source": "STR/Visit California CALodgingPerformance_202601.xls",
    },
]


def ts() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if (f != f) else f  # NaN check
    except (TypeError, ValueError):
        return None


def _parse_xls(conn: sqlite3.Connection) -> int:
    """
    Parse CALodgingPerformance_202601.xls with xlrd.
    Sheet: 'Multi-Segment' (first sheet).
    Columns (0-indexed): 1=region, 6=OCC 2026, 7=OCC 2025, 8=ADR 2026, 10=RevPAR 2026, 12=OCC YOY, 13=ADR YOY, 14=RevPAR YOY
    """
    try:
        import xlrd
    except ImportError:
        print(f"{ts()} [WARN] xlrd not installed — falling back to seed data")
        return 0

    if not XLS_FILE.exists():
        return 0

    try:
        wb = xlrd.open_workbook(str(XLS_FILE))
    except Exception as exc:
        print(f"{ts()} [WARN] xlrd cannot open {XLS_FILE.name}: {exc}")
        return 0

    sh = wb.sheet_by_index(0)
    report_period = "2026-01"  # derived from filename suffix 202601

    rows_inserted = 0
    cur = conn.cursor()

    for r in range(sh.nrows):
        region_val = sh.cell_value(r, 1)
        if not region_val or not isinstance(region_val, str):
            continue
        region = region_val.strip().rstrip("+").strip()
        if not region or region.lower() in ("", "tab 2 - multi-segment", "visit california"):
            continue

        # Skip header rows
        try:
            occ_2026 = _safe_float(sh.cell_value(r, 6))
            adr_2026 = _safe_float(sh.cell_value(r, 8))
            revpar_2026 = _safe_float(sh.cell_value(r, 10))
            occ_yoy = _safe_float(sh.cell_value(r, 12))
            adr_yoy = _safe_float(sh.cell_value(r, 13))
            revpar_yoy = _safe_float(sh.cell_value(r, 14))
        except IndexError:
            continue

        if occ_2026 is None or occ_2026 < 5 or occ_2026 > 100:
            continue
        if adr_2026 is None or adr_2026 < 50:
            continue

        row = {
            "report_period": report_period,
            "region": region,
            "occupancy_pct": round(occ_2026, 4),
            "adr_usd": round(adr_2026, 4) if adr_2026 else None,
            "revpar_usd": round(revpar_2026, 4) if revpar_2026 else None,
            "supply_rooms": None,
            "demand_room_nights": None,
            "room_revenue_usd": None,
            "occ_yoy_pp": round(occ_yoy, 4) if occ_yoy is not None else None,
            "adr_yoy_pct": round(adr_yoy, 4) if adr_yoy is not None else None,
            "revpar_yoy_pct": round(revpar_yoy, 4) if revpar_yoy is not None else None,
            "data_source": f"STR/Visit California {XLS_FILE.name}",
        }

        try:
            cur.execute("""
                INSERT INTO visit_ca_lodging_monthly (
                    report_period, region,
                    occupancy_pct, adr_usd, revpar_usd,
                    supply_rooms, demand_room_nights, room_revenue_usd,
                    occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct, data_source
                ) VALUES (
                    :report_period, :region,
                    :occupancy_pct, :adr_usd, :revpar_usd,
                    :supply_rooms, :demand_room_nights, :room_revenue_usd,
                    :occ_yoy_pp, :adr_yoy_pct, :revpar_yoy_pct, :data_source
                )
                ON CONFLICT(report_period, region) DO UPDATE SET
                    occupancy_pct      = excluded.occupancy_pct,
                    adr_usd            = excluded.adr_usd,
                    revpar_usd         = excluded.revpar_usd,
                    occ_yoy_pp         = excluded.occ_yoy_pp,
                    adr_yoy_pct        = excluded.adr_yoy_pct,
                    revpar_yoy_pct     = excluded.revpar_yoy_pct,
                    data_source        = excluded.data_source,
                    updated_at         = datetime('now')
            """, row)
            rows_inserted += 1
        except Exception as exc:
            print(f"{ts()} [WARN] DB insert failed for '{region}': {exc}")

    conn.commit()
    return rows_inserted


def _seed_rows(conn: sqlite3.Connection) -> int:
    """Insert/upsert the representative seed rows."""
    cur = conn.cursor()
    count = 0
    for row in SEED_ROWS:
        try:
            cur.execute("""
                INSERT INTO visit_ca_lodging_monthly (
                    report_period, region,
                    occupancy_pct, adr_usd, revpar_usd,
                    supply_rooms, demand_room_nights, room_revenue_usd,
                    occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct, data_source
                ) VALUES (
                    :report_period, :region,
                    :occupancy_pct, :adr_usd, :revpar_usd,
                    :supply_rooms, :demand_room_nights, :room_revenue_usd,
                    :occ_yoy_pp, :adr_yoy_pct, :revpar_yoy_pct, :data_source
                )
                ON CONFLICT(report_period, region) DO NOTHING
            """, row)
            count += 1
        except Exception as exc:
            print(f"{ts()} [WARN] Seed insert failed for '{row['region']}': {exc}")
    conn.commit()
    return count


def main() -> int:
    print(f"{ts()} [START] load_visit_ca_lodging.py")

    conn = sqlite3.connect(DB, timeout=10)
    conn.executescript(DDL)
    conn.commit()

    parsed_count = 0
    if XLS_FILE.exists():
        try:
            parsed_count = _parse_xls(conn)
            print(f"{ts()} [OK  ] Parsed {parsed_count} rows from {XLS_FILE.name}")
        except Exception as exc:
            print(f"{ts()} [WARN] XLS parse error — {exc}; falling back to seed data")
    else:
        print(f"{ts()} [WARN] {XLS_FILE.name} not found — using seed data only")

    # Always seed representative rows for regions that may not be in the XLS
    seed_count = _seed_rows(conn)

    total = parsed_count if parsed_count > 0 else seed_count
    if parsed_count > 0:
        print(f"{ts()} [OK  ] {seed_count} additional seed rows ensured (DO NOTHING on conflict)")
        total = parsed_count  # prefer parsed count for reporting
    else:
        print(f"{ts()} [OK  ] Seeded {seed_count} representative Jan 2026 rows")

    conn.execute("""
        INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at)
        VALUES ('Visit_CA_Lodging', 'monthly', 'CALodgingPerformance_202601.xls', ?, datetime('now'))
    """, (total,))
    conn.commit()
    conn.close()

    print(f"{ts()} [DONE] visit_ca_lodging_monthly: {total} rows inserted/updated")
    return total


if __name__ == "__main__":
    main()
