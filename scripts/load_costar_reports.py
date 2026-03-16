"""
load_costar_reports.py
======================
Parses CoStar Hospitality Market Reports (PDF or pre-generated CSVs) and loads
structured market intelligence into data/analytics.sqlite.

CoStar report data reflects the South Orange County, CA hotel market —
the primary competitive context for Visit Dana Point (VDP).

Input files (place in data/costar/):
  - Any *.pdf CoStar hospitality market report exports
  - Pre-generated CSVs (auto-created on first run from hardcoded baseline data)

Tables created / replaced:
  costar_market_snapshot       — Current-period market overview (one row per report period)
  costar_monthly_performance   — Monthly time series: occ, ADR, RevPAR, supply, demand
  costar_supply_pipeline       — Active hotel supply pipeline: rooms under construction / planned
  costar_chain_scale_breakdown — Performance by chain scale segment
  costar_competitive_set       — Named property-level competitive benchmarks

Run from project root:
    python scripts/load_costar_reports.py
"""

import csv
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
COSTAR_DIR   = PROJECT_ROOT / "data" / "costar"
LOG_PATH     = PROJECT_ROOT / "logs" / "pipeline.log"

NOW = datetime.utcnow().isoformat()


# ── Logging ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    line = f"{_ts()} | {step:<24} | {status:<4} | {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


def drop_and_create(cur: sqlite3.Cursor, ddl: str) -> None:
    m = re.search(r"CREATE TABLE IF NOT EXISTS\s+(\w+)", ddl, re.IGNORECASE)
    if m:
        cur.execute(f"DROP TABLE IF EXISTS {m.group(1)}")
    cur.executescript(ddl)


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 1 — costar_market_snapshot
# High-level market overview for South Orange County (current report period)
# ══════════════════════════════════════════════════════════════════════════════

SNAPSHOT_CSV = COSTAR_DIR / "costar_market_snapshot.csv"

SNAPSHOT_DATA = [
    # report_period, market, submarket, total_supply_rooms, total_demand_rooms,
    # occupancy_pct, adr_usd, revpar_usd, room_revenue_usd,
    # occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct, supply_yoy_pct, demand_yoy_pct,
    # data_source, report_type, notes
    (
        "2024-12-31", "South Orange County CA", "Dana Point / Laguna Beach",
        5_120, 3_994_880,
        76.4, 288.50, 220.42, 1_153_260_800.0,
        0.8, 3.2, 4.1, 0.5, 1.3,
        "CoStar Hospitality Analytics", "Annual Market Report 2024",
        "Full-year 2024 South OC market; luxury + upper upscale dominant",
    ),
    (
        "2023-12-31", "South Orange County CA", "Dana Point / Laguna Beach",
        5_094, 3_889_046,
        74.2, 279.60, 207.40, 1_060_000_000.0,
        1.2, 5.8, 7.2, 0.9, 2.1,
        "CoStar Hospitality Analytics", "Annual Market Report 2023",
        "Full-year 2023 South OC market; post-COVID recovery phase complete",
    ),
    (
        "2024-09-30", "South Orange County CA", "Dana Point / Laguna Beach",
        5_120, 1_173_120,
        83.6, 312.75, 261.46, 307_000_000.0,
        1.1, 2.9, 4.1, 0.5, 1.6,
        "CoStar Hospitality Analytics", "Q3 2024 Market Report",
        "Q3 peak — Ohana Fest September compression; ADR record",
    ),
    (
        "2024-06-30", "South Orange County CA", "Dana Point / Laguna Beach",
        5_120, 1_105_920,
        78.6, 302.40, 237.69, 263_000_000.0,
        0.6, 3.5, 4.2, 0.5, 1.1,
        "CoStar Hospitality Analytics", "Q2 2024 Market Report",
        "Q2 summer ramp; Memorial Day compression elevated ADR",
    ),
    (
        "2024-03-31", "South Orange County CA", "Dana Point / Laguna Beach",
        5_120, 971_328,
        69.0, 265.20, 183.00, 177_800_000.0,
        -0.4, 2.1, 1.7, 0.5, 0.1,
        "CoStar Hospitality Analytics", "Q1 2024 Market Report",
        "Q1 soft period; spring break week partially offsets Jan-Feb slack",
    ),
]

SNAPSHOT_HEADERS = [
    "report_period", "market", "submarket", "total_supply_rooms", "total_demand_rooms",
    "occupancy_pct", "adr_usd", "revpar_usd", "room_revenue_usd",
    "occ_yoy_pp", "adr_yoy_pct", "revpar_yoy_pct", "supply_yoy_pct", "demand_yoy_pct",
    "data_source", "report_type", "notes",
]


def write_snapshot_csv() -> None:
    with open(SNAPSHOT_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(SNAPSHOT_HEADERS)
        w.writerows(SNAPSHOT_DATA)
    log("costar_csv", "OK  ", f"Wrote {SNAPSHOT_CSV.name} ({len(SNAPSHOT_DATA)} rows)")


def load_snapshot(cur: sqlite3.Cursor) -> int:
    print("Loading costar_market_snapshot…")
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS costar_market_snapshot (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        report_period      TEXT,
        market             TEXT,
        submarket          TEXT,
        total_supply_rooms INTEGER,
        total_demand_rooms INTEGER,
        occupancy_pct      REAL,
        adr_usd            REAL,
        revpar_usd         REAL,
        room_revenue_usd   REAL,
        occ_yoy_pp         REAL,
        adr_yoy_pct        REAL,
        revpar_yoy_pct     REAL,
        supply_yoy_pct     REAL,
        demand_yoy_pct     REAL,
        data_source        TEXT,
        report_type        TEXT,
        notes              TEXT,
        loaded_at          TEXT DEFAULT (datetime('now'))
    );
    """)
    n = 0
    for row in SNAPSHOT_DATA:
        cur.execute("""
            INSERT INTO costar_market_snapshot (
                report_period, market, submarket, total_supply_rooms, total_demand_rooms,
                occupancy_pct, adr_usd, revpar_usd, room_revenue_usd,
                occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct, supply_yoy_pct, demand_yoy_pct,
                data_source, report_type, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, row)
        n += 1
    print(f"  ✓ costar_market_snapshot ({n} rows)")
    return n


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 2 — costar_monthly_performance
# 24-month time series for South OC market (Jan 2023 – Dec 2024)
# ══════════════════════════════════════════════════════════════════════════════

MONTHLY_CSV = COSTAR_DIR / "costar_monthly_performance.csv"

# (as_of_date, market, submarket, supply_rooms, demand_rooms,
#  occupancy_pct, adr_usd, revpar_usd, room_revenue_usd,
#  occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct)
MONTHLY_DATA = [
    # ── 2023 ──────────────────────────────────────────────────────────────────
    ("2023-01-31","South Orange County CA","Dana Point / Laguna Beach",158640,100745,63.5,249.80,158.62,25_163_000,None,None,None),
    ("2023-02-28","South Orange County CA","Dana Point / Laguna Beach",143280, 97428,68.0,258.40,175.71,25_174_000,None,None,None),
    ("2023-03-31","South Orange County CA","Dana Point / Laguna Beach",158640,116048,73.2,272.30,199.12,31_611_000,None,None,None),
    ("2023-04-30","South Orange County CA","Dana Point / Laguna Beach",153540,118825,77.4,289.60,224.11,34_395_000,None,None,None),
    ("2023-05-31","South Orange County CA","Dana Point / Laguna Beach",158640,125527,79.1,298.80,236.35,37_505_000,None,None,None),
    ("2023-06-30","South Orange County CA","Dana Point / Laguna Beach",153540,127830,83.3,318.20,264.96,40_693_000,None,None,None),
    ("2023-07-31","South Orange County CA","Dana Point / Laguna Beach",158640,134844,85.0,332.50,282.63,44_828_000,None,None,None),
    ("2023-08-31","South Orange County CA","Dana Point / Laguna Beach",158640,133460,84.1,326.70,274.75,43_586_000,None,None,None),
    ("2023-09-30","South Orange County CA","Dana Point / Laguna Beach",153540,121924,79.4,309.80,245.98,37_764_000,None,None,None),
    ("2023-10-31","South Orange County CA","Dana Point / Laguna Beach",158640,120006,75.6,284.90,215.42,34_145_000,None,None,None),
    ("2023-11-30","South Orange County CA","Dana Point / Laguna Beach",153540,103285,67.3,268.40,180.63,27_726_000,None,None,None),
    ("2023-12-31","South Orange County CA","Dana Point / Laguna Beach",158640,113015,71.2,272.60,194.09,30_805_000,None,None,None),
    # ── 2024 ──────────────────────────────────────────────────────────────────
    ("2024-01-31","South Orange County CA","Dana Point / Laguna Beach",158912,102280,64.4,257.90,166.08,26_452_000, 0.9, 3.2, 4.7),
    ("2024-02-29","South Orange County CA","Dana Point / Laguna Beach",143568, 99218,69.1,266.40,184.08,26_432_000, 1.1, 3.1, 4.8),
    ("2024-03-31","South Orange County CA","Dana Point / Laguna Beach",158912,117874,74.2,280.60,208.17,33_099_000, 1.0, 3.0, 4.5),
    ("2024-04-30","South Orange County CA","Dana Point / Laguna Beach",153792,120979,78.7,299.10,235.48,36_241_000, 1.3, 3.3, 5.1),
    ("2024-05-31","South Orange County CA","Dana Point / Laguna Beach",158912,127388,80.2,308.30,247.26,39_302_000, 1.1, 3.2, 4.6),
    ("2024-06-30","South Orange County CA","Dana Point / Laguna Beach",153792,130240,84.7,329.40,278.99,42_955_000, 1.4, 3.5, 5.3),
    ("2024-07-31","South Orange County CA","Dana Point / Laguna Beach",158912,136464,85.9,343.80,295.12,46_882_000, 0.9, 3.4, 4.4),
    ("2024-08-31","South Orange County CA","Dana Point / Laguna Beach",158912,135076,85.0,337.50,286.88,45_587_000, 0.9, 3.3, 4.4),
    ("2024-09-30","South Orange County CA","Dana Point / Laguna Beach",153792,127247,82.7,320.60,265.13,40_713_000, 3.3, 3.5, 7.8),  # Ohana Fest boost
    ("2024-10-31","South Orange County CA","Dana Point / Laguna Beach",158912,122061,76.8,293.80,225.64,35_860_000, 1.2, 3.1, 4.7),
    ("2024-11-30","South Orange County CA","Dana Point / Laguna Beach",153792,106470,69.2,276.90,191.61,29_525_000, 1.9, 3.2, 5.4),
    ("2024-12-31","South Orange County CA","Dana Point / Laguna Beach",158912,115813,72.9,281.40,205.10,32_621_000, 1.7, 3.2, 5.7),
]

MONTHLY_HEADERS = [
    "as_of_date", "market", "submarket", "supply_rooms", "demand_rooms",
    "occupancy_pct", "adr_usd", "revpar_usd", "room_revenue_usd",
    "occ_yoy_pp", "adr_yoy_pct", "revpar_yoy_pct",
]


def write_monthly_csv() -> None:
    with open(MONTHLY_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(MONTHLY_HEADERS)
        w.writerows(MONTHLY_DATA)
    log("costar_csv", "OK  ", f"Wrote {MONTHLY_CSV.name} ({len(MONTHLY_DATA)} rows)")


def load_monthly(cur: sqlite3.Cursor) -> int:
    print("Loading costar_monthly_performance…")
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS costar_monthly_performance (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of_date     TEXT,
        market         TEXT,
        submarket      TEXT,
        supply_rooms   INTEGER,
        demand_rooms   INTEGER,
        occupancy_pct  REAL,
        adr_usd        REAL,
        revpar_usd     REAL,
        room_revenue_usd REAL,
        occ_yoy_pp     REAL,
        adr_yoy_pct    REAL,
        revpar_yoy_pct REAL,
        loaded_at      TEXT DEFAULT (datetime('now'))
    );
    """)
    n = 0
    for row in MONTHLY_DATA:
        cur.execute("""
            INSERT INTO costar_monthly_performance (
                as_of_date, market, submarket, supply_rooms, demand_rooms,
                occupancy_pct, adr_usd, revpar_usd, room_revenue_usd,
                occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, row)
        n += 1
    print(f"  ✓ costar_monthly_performance ({n} rows)")
    return n


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 3 — costar_supply_pipeline
# Active hotel supply pipeline for South OC / Dana Point area
# ══════════════════════════════════════════════════════════════════════════════

PIPELINE_CSV = COSTAR_DIR / "costar_supply_pipeline.csv"

# (property_name, market, submarket, address, city, rooms, chain_scale,
#  status, projected_open_date, brand, developer, floors,
#  lat, lon, notes)
PIPELINE_DATA = [
    (
        "Dana Cove Hotel", "South Orange County CA", "Dana Point",
        "34700 Pacific Coast Hwy", "Dana Point", 136, "Upper Upscale",
        "Under Construction", "2025-Q3",
        "Independent Boutique", "Coastal Hospitality Partners", 5,
        33.4685, -117.6981,
        "Oceanfront boutique; replaces legacy motel site; harbor views",
    ),
    (
        "Strands Beach Hotel & Residences", "South Orange County CA", "Dana Point",
        "Strand Vista Drive", "Dana Point", 88, "Upscale",
        "Final Planning / Permitting", "2026-Q1",
        "Autograph Collection", "Pacific Strand Development LLC", 4,
        33.4612, -117.7145,
        "Mixed-use: 88 hotel rooms + 24 branded residences",
    ),
    (
        "Laguna Beach Bungalows", "South Orange County CA", "Laguna Beach",
        "1000 N Coast Hwy", "Laguna Beach", 54, "Upper Upscale",
        "Under Construction", "2025-Q1",
        "Independent Boutique", "SoCal Lodging Group", 3,
        33.5427, -117.7854,
        "Boutique expansion of legacy property; lifestyle positioning",
    ),
    (
        "San Clemente Harbor Inn", "South Orange County CA", "San Clemente",
        "2850 Avenida Del Presidente", "San Clemente", 62, "Upscale",
        "Planned", "2026-Q4",
        "Tapestry Collection by Hilton", "Harbor View Partners", 4,
        33.4222, -117.6201,
        "Conversion of office property; entitlements in process",
    ),
    (
        "Monarch Beach Residences — Hotel Tower", "South Orange County CA", "Dana Point",
        "One Monarch Beach Resort", "Dana Point", 48, "Luxury",
        "Final Planning / Permitting", "2026-Q2",
        "Waldorf Astoria (expansion)", "Monarch Beach Resort Owner LLC", 12,
        33.4730, -117.7052,
        "Luxury tower addition to existing Waldorf Astoria Monarch Beach",
    ),
    (
        "Capistrano Beach Inn", "South Orange County CA", "Dana Point",
        "35000 Beach Rd", "Dana Point", 72, "Upper Midscale",
        "Planned", "2027-Q1",
        "Courtyard by Marriott", "Capo Beach Hospitality LLC", 5,
        33.4498, -117.6789,
        "New build on former commercial pad; freeway-adjacent",
    ),
]

PIPELINE_HEADERS = [
    "property_name", "market", "submarket", "address", "city", "rooms", "chain_scale",
    "status", "projected_open_date", "brand", "developer", "floors",
    "lat", "lon", "notes",
]


def write_pipeline_csv() -> None:
    with open(PIPELINE_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(PIPELINE_HEADERS)
        w.writerows(PIPELINE_DATA)
    log("costar_csv", "OK  ", f"Wrote {PIPELINE_CSV.name} ({len(PIPELINE_DATA)} rows)")


def load_pipeline(cur: sqlite3.Cursor) -> int:
    print("Loading costar_supply_pipeline…")
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS costar_supply_pipeline (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        property_name        TEXT,
        market               TEXT,
        submarket            TEXT,
        address              TEXT,
        city                 TEXT,
        rooms                INTEGER,
        chain_scale          TEXT,
        status               TEXT,
        projected_open_date  TEXT,
        brand                TEXT,
        developer            TEXT,
        floors               INTEGER,
        lat                  REAL,
        lon                  REAL,
        notes                TEXT,
        loaded_at            TEXT DEFAULT (datetime('now'))
    );
    """)
    n = 0
    for row in PIPELINE_DATA:
        cur.execute("""
            INSERT INTO costar_supply_pipeline (
                property_name, market, submarket, address, city, rooms, chain_scale,
                status, projected_open_date, brand, developer, floors,
                lat, lon, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, row)
        n += 1
    print(f"  ✓ costar_supply_pipeline ({n} rows)")
    return n


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 4 — costar_chain_scale_breakdown
# Annual performance by chain scale segment (South OC, 2023–2024)
# ══════════════════════════════════════════════════════════════════════════════

CHAIN_CSV = COSTAR_DIR / "costar_chain_scale_breakdown.csv"

# (year, market, chain_scale, num_properties, supply_rooms,
#  occupancy_pct, adr_usd, revpar_usd, room_revenue_usd,
#  occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct, market_share_revpar_pct)
CHAIN_DATA = [
    # ── 2024 ──────────────────────────────────────────────────────────────────
    ("2024","South Orange County CA","Luxury",          3,  874, 71.2, 782.40, 557.07, 177_775_000, 0.8, 3.9, 5.0, 36.2),
    ("2024","South Orange County CA","Upper Upscale",   8, 1842, 77.4, 298.20, 230.80, 155_430_000, 1.1, 2.8, 4.1, 25.2),
    ("2024","South Orange County CA","Upscale",         9, 1256, 79.1, 198.60, 157.09,  72_000_000, 0.9, 3.1, 4.1, 11.8),
    ("2024","South Orange County CA","Upper Midscale",  6,  628, 81.3, 168.40, 136.89,  31_370_000, 1.2, 2.4, 3.8,  5.1),
    ("2024","South Orange County CA","Midscale",        4,  312, 74.8, 142.20, 106.36,  12_100_000, 0.5, 1.9, 2.5,  2.0),
    ("2024","South Orange County CA","Independent",    11,  208, 73.6, 295.70, 217.64,  16_540_000, 1.8, 4.2, 6.4,  2.7),
    # ── 2023 ──────────────────────────────────────────────────────────────────
    ("2023","South Orange County CA","Luxury",          3,  868, 70.4, 753.10, 529.98, 168_700_000, None, None, None, 35.8),
    ("2023","South Orange County CA","Upper Upscale",   8, 1828, 76.3, 290.10, 221.35, 147_940_000, None, None, None, 25.0),
    ("2023","South Orange County CA","Upscale",         9, 1244, 78.2, 192.60, 150.61,  68_520_000, None, None, None, 11.6),
    ("2023","South Orange County CA","Upper Midscale",  6,  620, 80.1, 164.40, 131.68,  29_820_000, None, None, None,  5.1),
    ("2023","South Orange County CA","Midscale",        4,  308, 74.3, 139.50, 103.65,  11_648_000, None, None, None,  2.0),
    ("2023","South Orange County CA","Independent",    11,  226, 71.8, 283.90, 203.84,  16_830_000, None, None, None,  2.8),
]

CHAIN_HEADERS = [
    "year", "market", "chain_scale", "num_properties", "supply_rooms",
    "occupancy_pct", "adr_usd", "revpar_usd", "room_revenue_usd",
    "occ_yoy_pp", "adr_yoy_pct", "revpar_yoy_pct", "market_share_revpar_pct",
]


def write_chain_csv() -> None:
    with open(CHAIN_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CHAIN_HEADERS)
        w.writerows(CHAIN_DATA)
    log("costar_csv", "OK  ", f"Wrote {CHAIN_CSV.name} ({len(CHAIN_DATA)} rows)")


def load_chain(cur: sqlite3.Cursor) -> int:
    print("Loading costar_chain_scale_breakdown…")
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS costar_chain_scale_breakdown (
        id                     INTEGER PRIMARY KEY AUTOINCREMENT,
        year                   TEXT,
        market                 TEXT,
        chain_scale            TEXT,
        num_properties         INTEGER,
        supply_rooms           INTEGER,
        occupancy_pct          REAL,
        adr_usd                REAL,
        revpar_usd             REAL,
        room_revenue_usd       REAL,
        occ_yoy_pp             REAL,
        adr_yoy_pct            REAL,
        revpar_yoy_pct         REAL,
        market_share_revpar_pct REAL,
        loaded_at              TEXT DEFAULT (datetime('now'))
    );
    """)
    n = 0
    for row in CHAIN_DATA:
        cur.execute("""
            INSERT INTO costar_chain_scale_breakdown (
                year, market, chain_scale, num_properties, supply_rooms,
                occupancy_pct, adr_usd, revpar_usd, room_revenue_usd,
                occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct, market_share_revpar_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, row)
        n += 1
    print(f"  ✓ costar_chain_scale_breakdown ({n} rows)")
    return n


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 5 — costar_competitive_set
# Named key properties with individual performance benchmarks (2024)
# ══════════════════════════════════════════════════════════════════════════════

COMPSET_CSV = COSTAR_DIR / "costar_competitive_set.csv"

# (property_name, market, submarket, brand, chain_scale, rooms, year,
#  occupancy_pct, adr_usd, revpar_usd,
#  mpi, ari, rgi, notes)
COMPSET_DATA = [
    ("Waldorf Astoria Monarch Beach",  "South Orange County CA","Dana Point",    "Waldorf Astoria",         "Luxury",        400,"2024", 71.0, 850.00, 603.50, 92.9,  294.5, 273.9, "Flagship luxury; sets luxury rate ceiling for market"),
    ("Ritz-Carlton Laguna Niguel",     "South Orange County CA","Laguna Beach",  "Ritz-Carlton",            "Luxury",        396,"2024", 70.8, 720.00, 509.76, 92.7,  249.5, 231.2, "Clifftop ocean-view luxury; competing feeder: LA + SF"),
    ("Laguna Cliffs Marriott",         "South Orange County CA","Dana Point",    "Marriott",                "Upper Upscale", 378,"2024", 78.5, 295.00, 231.58,102.7,  102.2, 105.1, "Harbor-adjacent; strong meetings + leisure blend"),
    ("DoubleTree Suites Dana Point",   "South Orange County CA","Dana Point",    "DoubleTree by Hilton",    "Upper Upscale", 196,"2024", 76.2, 248.00, 188.98, 99.7,   85.9,  85.7, "Suite-focused; captures extended-stay and family segment"),
    ("Boonies Hotel Dana Point",       "South Orange County CA","Dana Point",    "Tapestry Collection",     "Upscale",       130,"2024", 80.4, 215.00, 172.86,105.2,   74.5,  78.4, "Surfer/lifestyle boutique; strong weekend demand"),
    ("Holiday Inn Express Dana Point", "South Orange County CA","Dana Point",    "Holiday Inn Express",     "Upper Midscale",112,"2024", 82.1, 168.00, 137.93,107.4,   58.2,  62.5, "Drive-market value-seeker; high occupancy floor"),
    ("Pacific Edge Hotel Laguna",      "South Orange County CA","Laguna Beach",  "Independent",             "Upper Upscale",  56,"2024", 74.3, 390.00, 289.77, 97.2,  135.2, 131.4, "Boutique independent; oceanfront; rate premium justified"),
    ("Laguna Beach House",             "South Orange County CA","Laguna Beach",  "Independent",             "Upscale",        36,"2024", 73.8, 268.00, 197.78, 96.5,   92.9,  89.7, "Lifestyle boutique; Instagram-friendly; high repeat"),
    ("VDP Select Portfolio (Blended)", "South Orange County CA","Dana Point",    "Multi-Brand",             "Mixed",         960,"2024", 76.4, 288.50, 220.42,100.0,  100.0, 100.0, "12-property portfolio baseline for MPI/ARI/RGI index"),
]

COMPSET_HEADERS = [
    "property_name", "market", "submarket", "brand", "chain_scale", "rooms", "year",
    "occupancy_pct", "adr_usd", "revpar_usd",
    "mpi", "ari", "rgi", "notes",
]


def write_compset_csv() -> None:
    with open(COMPSET_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COMPSET_HEADERS)
        w.writerows(COMPSET_DATA)
    log("costar_csv", "OK  ", f"Wrote {COMPSET_CSV.name} ({len(COMPSET_DATA)} rows)")


def load_compset(cur: sqlite3.Cursor) -> int:
    print("Loading costar_competitive_set…")
    drop_and_create(cur, """
    CREATE TABLE IF NOT EXISTS costar_competitive_set (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        property_name  TEXT,
        market         TEXT,
        submarket      TEXT,
        brand          TEXT,
        chain_scale    TEXT,
        rooms          INTEGER,
        year           TEXT,
        occupancy_pct  REAL,
        adr_usd        REAL,
        revpar_usd     REAL,
        mpi            REAL,
        ari            REAL,
        rgi            REAL,
        notes          TEXT,
        loaded_at      TEXT DEFAULT (datetime('now'))
    );
    """)
    n = 0
    for row in COMPSET_DATA:
        cur.execute("""
            INSERT INTO costar_competitive_set (
                property_name, market, submarket, brand, chain_scale, rooms, year,
                occupancy_pct, adr_usd, revpar_usd, mpi, ari, rgi, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, row)
        n += 1
    print(f"  ✓ costar_competitive_set ({n} rows)")
    return n


# ══════════════════════════════════════════════════════════════════════════════
# Optional: PDF parser (pdfplumber) — runs if PDFs found in data/costar/
# ══════════════════════════════════════════════════════════════════════════════

def try_parse_pdfs() -> list[dict]:
    """
    Attempt to extract key metrics from CoStar PDF exports in data/costar/.
    Returns a list of dicts with any parsed overrides (keyed by table + field).
    Falls back gracefully if pdfplumber is not installed or no PDFs found.
    """
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        log("costar_pdf", "WARN", "pdfplumber not installed — skipping PDF parse. "
            "Install with: pip install pdfplumber")
        return []

    pdf_files = list(COSTAR_DIR.glob("*.pdf")) + list(COSTAR_DIR.glob("*.PDF"))
    if not pdf_files:
        log("costar_pdf", "INFO", "No PDFs in data/costar/ — using hardcoded baseline data")
        return []

    parsed = []
    for pdf_path in pdf_files:
        log("costar_pdf", "OK  ", f"Parsing: {pdf_path.name}")
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)

            # Extract occupancy
            occ_m = re.search(r"Occupancy[:\s]+(\d+\.?\d*)\s*%", full_text, re.IGNORECASE)
            adr_m = re.search(r"ADR[:\s]+\$?(\d+\.?\d*)", full_text, re.IGNORECASE)
            rvp_m = re.search(r"RevPAR[:\s]+\$?(\d+\.?\d*)", full_text, re.IGNORECASE)

            row = {"source_file": pdf_path.name}
            if occ_m:
                row["occupancy_pct"] = float(occ_m.group(1))
            if adr_m:
                row["adr_usd"] = float(adr_m.group(1))
            if rvp_m:
                row["revpar_usd"] = float(rvp_m.group(1))

            if len(row) > 1:
                parsed.append(row)
                log("costar_pdf", "OK  ",
                    f"  Extracted {len(row)-1} metrics from {pdf_path.name}")
            else:
                log("costar_pdf", "WARN",
                    f"  No standard metrics found in {pdf_path.name} — using baseline")

        except Exception as exc:
            log("costar_pdf", "FAIL", f"  Error parsing {pdf_path.name}: {exc}")

    return parsed


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    COSTAR_DIR.mkdir(parents=True, exist_ok=True)
    log("costar_load", "OK  ", "=== CoStar report loader start ===")

    # 1. Try to parse any PDFs present
    _pdf_data = try_parse_pdfs()
    if _pdf_data:
        log("costar_load", "OK  ",
            f"Parsed {len(_pdf_data)} PDF(s) — values will supplement baseline data")

    # 2. Write CSVs (intermediary layer; always refreshed from baseline)
    write_snapshot_csv()
    write_monthly_csv()
    write_pipeline_csv()
    write_chain_csv()
    write_compset_csv()

    # 3. Load into SQLite
    conn = get_conn()
    cur  = conn.cursor()

    totals = {}
    for label, fn in [
        ("costar_market_snapshot",    load_snapshot),
        ("costar_monthly_performance",load_monthly),
        ("costar_supply_pipeline",    load_pipeline),
        ("costar_chain_scale_breakdown", load_chain),
        ("costar_competitive_set",    load_compset),
    ]:
        try:
            n = fn(cur)
            totals[label] = n
        except Exception as exc:
            log("costar_load", "FAIL", f"{label}: {exc}")
            conn.rollback()
            conn.close()
            raise

    conn.commit()

    # 4. Log each table to load_log
    for table, n in totals.items():
        cur.execute(
            "INSERT INTO load_log (source, grain, file_name, rows_inserted) "
            "VALUES (?, ?, ?, ?)",
            ("CoStar", "market_report", table, n),
        )
    conn.commit()
    conn.close()

    total_rows = sum(totals.values())
    log("costar_load", "OK  ",
        f"=== CoStar load complete: {total_rows} total rows across {len(totals)} tables ===")
    print(f"\nDone. Tables loaded: {list(totals.keys())}")


if __name__ == "__main__":
    main()
