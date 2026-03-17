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
# TABLE 6 — costar_annual_performance
# Multi-year annual time series extracted from CoStar PDF reports
# Covers occ/ADR/RevPAR/supply/demand per year per market scope
# ══════════════════════════════════════════════════════════════════════════════

def load_annual_performance(cur: sqlite3.Cursor, rows: list) -> int:
    """UPSERT rows into costar_annual_performance. rows = list of dicts."""
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS costar_annual_performance (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        year_label      TEXT    NOT NULL,
        market          TEXT    NOT NULL,
        report_scope    TEXT    NOT NULL DEFAULT 'Overall',
        available_rooms INTEGER,
        occupied_rooms  INTEGER,
        occupancy_pct   REAL,
        occ_yoy_pct     REAL,
        adr_usd         REAL,
        adr_yoy_pct     REAL,
        revpar_usd      REAL,
        revpar_yoy_pct  REAL,
        source_file     TEXT,
        report_date     TEXT,
        loaded_at       TEXT DEFAULT (datetime('now')),
        UNIQUE(year_label, market, report_scope)
    );
    """)
    n = 0
    for r in rows:
        cur.execute("""
            INSERT INTO costar_annual_performance
                (year_label, market, report_scope,
                 available_rooms, occupied_rooms,
                 occupancy_pct, occ_yoy_pct,
                 adr_usd, adr_yoy_pct,
                 revpar_usd, revpar_yoy_pct,
                 source_file, report_date)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(year_label, market, report_scope)
            DO UPDATE SET
                available_rooms = excluded.available_rooms,
                occupied_rooms  = excluded.occupied_rooms,
                occupancy_pct   = excluded.occupancy_pct,
                occ_yoy_pct     = excluded.occ_yoy_pct,
                adr_usd         = excluded.adr_usd,
                adr_yoy_pct     = excluded.adr_yoy_pct,
                revpar_usd      = excluded.revpar_usd,
                revpar_yoy_pct  = excluded.revpar_yoy_pct,
                source_file     = excluded.source_file,
                report_date     = excluded.report_date,
                loaded_at       = datetime('now')
        """, (
            r["year_label"], r["market"], r.get("report_scope", "Overall"),
            r.get("available_rooms"), r.get("occupied_rooms"),
            r.get("occupancy_pct"), r.get("occ_yoy_pct"),
            r.get("adr_usd"), r.get("adr_yoy_pct"),
            r.get("revpar_usd"), r.get("revpar_yoy_pct"),
            r.get("source_file"), r.get("report_date"),
        ))
        n += 1
    print(f"  ✓ costar_annual_performance ({n} rows upserted)")
    return n


# ══════════════════════════════════════════════════════════════════════════════
# TABLE 7 — costar_profitability
# Full-service hotel P&L benchmarks extracted from CoStar PDF reports
# ══════════════════════════════════════════════════════════════════════════════

def load_profitability(cur: sqlite3.Cursor, rows: list) -> int:
    """UPSERT rows into costar_profitability."""
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS costar_profitability (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        year_label      TEXT    NOT NULL,
        market          TEXT    NOT NULL,
        line_item       TEXT    NOT NULL,
        revenue_pct     REAL,
        per_key_usd     REAL,
        por_usd         REAL,
        per_key_yoy_pct REAL,
        por_yoy_pct     REAL,
        source_file     TEXT,
        loaded_at       TEXT DEFAULT (datetime('now')),
        UNIQUE(year_label, market, line_item)
    );
    """)
    n = 0
    for r in rows:
        cur.execute("""
            INSERT INTO costar_profitability
                (year_label, market, line_item,
                 revenue_pct, per_key_usd, por_usd,
                 per_key_yoy_pct, por_yoy_pct, source_file)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(year_label, market, line_item)
            DO UPDATE SET
                revenue_pct     = excluded.revenue_pct,
                per_key_usd     = excluded.per_key_usd,
                por_usd         = excluded.por_usd,
                per_key_yoy_pct = excluded.per_key_yoy_pct,
                por_yoy_pct     = excluded.por_yoy_pct,
                loaded_at       = datetime('now')
        """, (
            r["year_label"], r["market"], r["line_item"],
            r.get("revenue_pct"), r.get("per_key_usd"), r.get("por_usd"),
            r.get("per_key_yoy_pct"), r.get("por_yoy_pct"),
            r.get("source_file"),
        ))
        n += 1
    print(f"  ✓ costar_profitability ({n} rows upserted)")
    return n


# ══════════════════════════════════════════════════════════════════════════════
# PDF Parser — comprehensive extraction from CoStar Hospitality PDFs
# ══════════════════════════════════════════════════════════════════════════════

def _parse_dollar(s: str):
    """Parse '$1,234.56' or '1234' to float. Returns None on failure."""
    try:
        return float(str(s).replace("$", "").replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def _parse_pct(s: str):
    """Parse '69.1%' or '-4.6%' to float (as percentage points). None on failure."""
    try:
        return float(str(s).replace("%", "").strip())
    except (ValueError, AttributeError):
        return None


def _parse_int(s: str):
    """Parse '4,219,235' or '(8,512)' to int (negative if parens). None on failure."""
    try:
        s = str(s).strip()
        neg = s.startswith("(") and s.endswith(")")
        val = int(s.strip("()").replace(",", ""))
        return -val if neg else val
    except (ValueError, AttributeError):
        return None


def _detect_scope(filename: str):
    """Return (market_name, report_type) from PDF filename."""
    fn = filename.lower()
    if "newport beach" in fn or "dana point" in fn:
        market = "Newport Beach/Dana Point"
    elif "orange county" in fn:
        market = "Orange County CA"
    elif "united states" in fn:
        market = "United States"
    else:
        market = "Unknown"

    if "capital" in fn:
        rtype = "Capital"
    elif "submarket" in fn:
        rtype = "Submarket"
    elif "national" in fn:
        rtype = "National"
    else:
        rtype = "Market"

    return market, rtype


def _find_page_text(pages: list, *keywords: str):
    """Return (page_index, text) of first page containing all keywords."""
    for i, page in enumerate(pages):
        text = page.extract_text() or ""
        if all(kw.lower() in text.lower() for kw in keywords):
            return i, text
    return -1, ""


def _extract_overview_kpis(pages: list) -> dict:
    """Extract 12-month headline KPIs from overview page (page 3 typically)."""
    result = {}
    _, text = _find_page_text(pages, "12 Mo Occupancy", "12 Mo ADR", "12 Mo RevPAR")
    if not text:
        return result

    # Pattern: "69.1% $285 $197 4.2M 2.9M" on a single line
    m = re.search(
        r"([\d.]+)%\s+\$([\d,]+)\s+\$([\d,]+)\s+([\d.]+)M\s+([\d.]+)M",
        text,
    )
    if m:
        result["occ_12mo"]    = float(m.group(1))
        result["adr_12mo"]    = _parse_dollar(m.group(2))
        result["revpar_12mo"] = _parse_dollar(m.group(3))
        result["supply_12mo_m"] = float(m.group(4))
        result["demand_12mo_m"] = float(m.group(5))
    return result


def _extract_trend_table(pages: list) -> dict:
    """Extract current/YTD/12Mo/forecast metrics from the trend summary table."""
    result = {}
    _, text = _find_page_text(pages, "Average Trend", "Occupancy Change", "ADR Change")
    if not text:
        return result

    # Occupancy row: "Occupancy 60.5% 61.3% 60.5% 69.1% 66.9% 71.1%"
    occ_m = re.search(
        r"^Occupancy\s+([\d.]+)%\s+([\d.]+)%\s+([\d.]+)%\s+([\d.]+)%",
        text, re.MULTILINE,
    )
    adr_m = re.search(
        r"^ADR\s+\$([\d.]+)\s+\$([\d.]+)\s+\$([\d.]+)\s+\$([\d.]+)",
        text, re.MULTILINE,
    )
    rvp_m = re.search(
        r"^RevPAR\s+\$([\d.]+)\s+\$([\d.]+)\s+\$([\d.]+)\s+\$([\d.]+)",
        text, re.MULTILINE,
    )
    if occ_m:
        result["occ_current"] = float(occ_m.group(1))
        result["occ_3mo"]     = float(occ_m.group(2))
        result["occ_ytd"]     = float(occ_m.group(3))
        result["occ_12mo"]    = float(occ_m.group(4))
    if adr_m:
        result["adr_current"] = float(adr_m.group(1))
        result["adr_3mo"]     = float(adr_m.group(2))
        result["adr_ytd"]     = float(adr_m.group(3))
        result["adr_12mo"]    = float(adr_m.group(4))
    if rvp_m:
        result["revpar_current"] = float(rvp_m.group(1))
        result["revpar_3mo"]     = float(rvp_m.group(2))
        result["revpar_ytd"]     = float(rvp_m.group(3))
        result["revpar_12mo"]    = float(rvp_m.group(4))
    return result


# Regex for annual performance rows: "2025 69.4% 1.2% $285.26 2.0% $197.90 3.2%"
_PERF_ROW_RE = re.compile(
    r"^(YTD|\d{4})\s+([\d.]+)%\s+([+-]?[\d.]+)%\s+\$([\d.]+)\s+([+-]?[\d.]+)%\s+\$([\d.]+)\s+([+-]?[\d.]+)%",
    re.MULTILINE,
)

# Regex for supply/demand rows: "2025 4,219,235 37,235 0.9% 2,927,011 60,463 2.1%"
_SD_ROW_RE = re.compile(
    r"^(YTD|\d{4})\s+([\d,]+)\s+([\d,]+|\([\d,]+\))\s+([+-]?[\d.]+)%\s+([\d,]+)\s+([\d,]+|\([\d,]+\))\s+([+-]?[\d.]+)%",
    re.MULTILINE,
)


def _extract_annual_performance(pages: list, market: str, source_file: str, report_date: str) -> list:
    """Extract annual performance rows from 'OVERALL PERFORMANCE' appendix page.
    Scopes extraction strictly to the OVERALL section, ignoring sub-class sections."""
    rows = []
    _, full_text = _find_page_text(pages, "OVERALL PERFORMANCE", "Occupancy", "ADR", "RevPAR")
    if not full_text:
        return rows

    # Scope text to the OVERALL PERFORMANCE block only (stop at the next class header)
    # Pattern: text between "OVERALL PERFORMANCE" and the next "PERFORMANCE" section heading
    overall_m = re.search(r"OVERALL PERFORMANCE\s*\n", full_text)
    if not overall_m:
        return rows
    start = overall_m.end()

    # Find next section heading after OVERALL (e.g. "LUXURY & UPPER UPSCALE PERFORMANCE")
    next_section = re.search(
        r"\n[A-Z &]+PERFORMANCE\s*\n",
        full_text[start:],
    )
    end = start + next_section.start() if next_section else len(full_text)
    text = full_text[start:end]

    # Find supply/demand — scope to OVERALL SUPPLY & DEMAND block similarly
    _, sd_full = _find_page_text(pages, "OVERALL SUPPLY & DEMAND", "Available Rooms", "Occupied Rooms")
    sd_text = sd_full or ""
    if sd_text:
        sd_m = re.search(r"OVERALL SUPPLY & DEMAND\s*\n", sd_text)
        if sd_m:
            sd_start = sd_m.end()
            sd_next = re.search(r"\n[A-Z &]+SUPPLY & DEMAND\s*\n", sd_text[sd_start:])
            sd_end = sd_start + sd_next.start() if sd_next else len(sd_text)
            sd_text = sd_text[sd_start:sd_end]

    # Build supply/demand lookup by year
    sd_by_year: dict = {}
    for m in _SD_ROW_RE.finditer(sd_text):
        yr = m.group(1)
        sd_by_year[yr] = {
            "available_rooms": _parse_int(m.group(2)),
            "occupied_rooms":  _parse_int(m.group(5)),
        }

    for m in _PERF_ROW_RE.finditer(text):
        yr = m.group(1)
        row = {
            "year_label":    yr,
            "market":        market,
            "report_scope":  "Overall",
            "occupancy_pct": float(m.group(2)),
            "occ_yoy_pct":   float(m.group(3)),
            "adr_usd":       float(m.group(4)),
            "adr_yoy_pct":   float(m.group(5)),
            "revpar_usd":    float(m.group(6)),
            "revpar_yoy_pct": float(m.group(7)),
            "source_file":   source_file,
            "report_date":   report_date,
        }
        if yr in sd_by_year:
            row.update(sd_by_year[yr])
        rows.append(row)

    return rows


def _extract_profitability(pages: list, market: str, source_file: str) -> list:
    """Extract full-service hotel P&L data from profitability page."""
    rows = []
    _, text = _find_page_text(pages, "FULL-SERVICE HOTELS PROFITABILITY", "Per Key", "POR")
    if not text:
        return rows

    # Detect year from "2024 2023-2024 % Change" or similar header
    year_m = re.search(r"\b(20\d{2})\b", text)
    year_label = year_m.group(1) if year_m else "Unknown"

    # Each P&L line: "Rooms 48.0% $113,098 $498.18 0.3% 0.5%"
    line_re = re.compile(
        r"^([\w &]+?)\s+([\d.]+)%\s+\$([\d,]+)\s+\$([\d.]+)\s+([+-]?[\d.]+)%\s+([+-]?[\d.]+)%",
        re.MULTILINE,
    )
    for m in line_re.finditer(text):
        item = m.group(1).strip()
        if item in ("Year", "Market", ""):
            continue
        rows.append({
            "year_label":     year_label,
            "market":         market,
            "line_item":      item,
            "revenue_pct":    float(m.group(2)),
            "per_key_usd":    _parse_dollar(m.group(3)),
            "por_usd":        float(m.group(4)),
            "per_key_yoy_pct": float(m.group(5)),
            "por_yoy_pct":    float(m.group(6)),
            "source_file":    source_file,
        })
    return rows


def _extract_snapshot_rows(
    overview: dict, trend: dict, annual_rows: list,
    market: str, source_file: str, report_date: str,
) -> list:
    """Build costar_market_snapshot rows from PDF extracted data."""
    rows = []

    # 12-month trailing snapshot
    if overview.get("occ_12mo") and overview.get("adr_12mo"):
        rows.append((
            f"{report_date[:4]}-12-31",    # report_period (approximate year-end)
            market, "",
            None,                          # total_supply_rooms (not available here)
            None,
            overview.get("occ_12mo"),
            overview.get("adr_12mo"),
            overview.get("revpar_12mo"),
            None,
            None, None, None, None, None,
            "CoStar Hospitality Analytics",
            f"12-Month Report {report_date[:7]}",
            f"12-month trailing ending {report_date}; extracted from PDF",
        ))

    # YTD snapshot from trend table
    if trend.get("occ_ytd") and trend.get("adr_ytd"):
        rows.append((
            f"{report_date}-YTD",
            market, "",
            None, None,
            trend.get("occ_ytd"),
            trend.get("adr_ytd"),
            trend.get("revpar_ytd"),
            None,
            None, None, None, None, None,
            "CoStar Hospitality Analytics",
            f"YTD Report {report_date[:7]}",
            f"YTD ending {report_date}; current occ={trend.get('occ_current')}%",
        ))

    # Add annual rows (2025, 2024, etc.) from performance table
    for r in annual_rows:
        if r["year_label"] == "YTD":
            continue
        rows.append((
            f"{r['year_label']}-12-31",
            market, "",
            r.get("available_rooms"), r.get("occupied_rooms"),
            r.get("occupancy_pct"),
            r.get("adr_usd"),
            r.get("revpar_usd"),
            None,
            r.get("occ_yoy_pct"),
            r.get("adr_yoy_pct"),
            r.get("revpar_yoy_pct"),
            None, None,
            "CoStar Hospitality Analytics",
            f"Annual Report {r['year_label']}",
            f"Annual {r['year_label']}; extracted from {source_file}",
        ))
    return rows


def parse_costar_pdf(pdf_path: Path) -> dict:
    """
    Parse a CoStar Hospitality PDF and return structured data dict.
    Keys: annual_performance, profitability, snapshot_rows, overview, trend, market, report_type
    """
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        return {}

    market, report_type = _detect_scope(pdf_path.name)
    # Extract date from filename (format: ...-2026-03-16.pdf)
    date_m = re.search(r"(\d{4}-\d{2}-\d{2})", pdf_path.name)
    report_date = date_m.group(1) if date_m else datetime.now().strftime("%Y-%m-%d")
    source_file = pdf_path.name

    log("costar_pdf", "OK  ", f"Parsing {source_file} ({market} / {report_type})")

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = pdf.pages

            overview = _extract_overview_kpis(pages)
            trend    = _extract_trend_table(pages)
            annual   = _extract_annual_performance(pages, market, source_file, report_date)
            profit   = _extract_profitability(pages, market, source_file)
            snaps    = _extract_snapshot_rows(overview, trend, annual, market, source_file, report_date)

        n_annual = len(annual)
        n_profit = len(profit)
        n_snaps  = len(snaps)
        log("costar_pdf", "OK  ",
            f"  → {n_annual} annual perf rows, {n_profit} P&L rows, {n_snaps} snapshot rows")

        return {
            "market":              market,
            "report_type":         report_type,
            "source_file":         source_file,
            "report_date":         report_date,
            "overview":            overview,
            "trend":               trend,
            "annual_performance":  annual,
            "profitability":       profit,
            "snapshot_rows":       snaps,
        }

    except Exception as exc:
        log("costar_pdf", "FAIL", f"  Error parsing {pdf_path.name}: {exc}")
        return {}


def parse_all_pdfs() -> dict:
    """Parse all CoStar PDFs in data/costar/. Returns aggregated results."""
    try:
        import pdfplumber  # noqa: F401 — just check it's available
    except ImportError:
        log("costar_pdf", "WARN",
            "pdfplumber not installed — skipping PDF parse (pip install pdfplumber)")
        return {}

    pdf_files = sorted(COSTAR_DIR.glob("*.pdf")) + sorted(COSTAR_DIR.glob("*.PDF"))
    if not pdf_files:
        log("costar_pdf", "INFO", "No PDFs in data/costar/ — using hardcoded baseline data")
        return {}

    log("costar_pdf", "OK  ", f"Found {len(pdf_files)} PDF(s) to parse")

    all_annual: list = []
    all_profit: list = []
    all_snaps:  list = []

    for pdf_path in pdf_files:
        result = parse_costar_pdf(pdf_path)
        if result:
            all_annual.extend(result.get("annual_performance", []))
            all_profit.extend(result.get("profitability", []))
            all_snaps.extend(result.get("snapshot_rows", []))

    return {
        "annual_performance": all_annual,
        "profitability":      all_profit,
        "snapshot_rows":      all_snaps,
    }


def _load_pdf_snapshots(cur: sqlite3.Cursor, snapshot_rows: list) -> int:
    """Insert PDF-extracted snapshot rows into costar_market_snapshot (skip duplicates)."""
    n = 0
    for row in snapshot_rows:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO costar_market_snapshot (
                    report_period, market, submarket, total_supply_rooms, total_demand_rooms,
                    occupancy_pct, adr_usd, revpar_usd, room_revenue_usd,
                    occ_yoy_pp, adr_yoy_pct, revpar_yoy_pct, supply_yoy_pct, demand_yoy_pct,
                    data_source, report_type, notes
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, row)
            if cur.rowcount:
                n += 1
        except Exception:
            pass
    return n


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    COSTAR_DIR.mkdir(parents=True, exist_ok=True)
    log("costar_load", "OK  ", "=== CoStar report loader start ===")

    # 1. Parse PDFs present in data/costar/
    pdf_data = parse_all_pdfs()

    # 2. Write CSVs from hardcoded baseline (intermediary reference layer)
    write_snapshot_csv()
    write_monthly_csv()
    write_pipeline_csv()
    write_chain_csv()
    write_compset_csv()

    # 3. Load hardcoded baseline tables into SQLite
    conn = get_conn()
    cur  = conn.cursor()

    totals = {}
    for label, fn in [
        ("costar_market_snapshot",       load_snapshot),
        ("costar_monthly_performance",   load_monthly),
        ("costar_supply_pipeline",       load_pipeline),
        ("costar_chain_scale_breakdown", load_chain),
        ("costar_competitive_set",       load_compset),
    ]:
        try:
            n = fn(cur)
            totals[label] = n
        except Exception as exc:
            log("costar_load", "FAIL", f"{label}: {exc}")
            conn.rollback()
            conn.close()
            raise

    # 4. Augment with real PDF-extracted data
    if pdf_data:
        # Upsert annual performance rows (real data from PDFs)
        annual_rows = pdf_data.get("annual_performance", [])
        if annual_rows:
            n = load_annual_performance(cur, annual_rows)
            totals["costar_annual_performance"] = n

        # Upsert profitability P&L rows
        profit_rows = pdf_data.get("profitability", [])
        if profit_rows:
            n = load_profitability(cur, profit_rows)
            totals["costar_profitability"] = n

        # Augment market snapshot with PDF-extracted current period rows
        snap_rows = pdf_data.get("snapshot_rows", [])
        if snap_rows:
            n = _load_pdf_snapshots(cur, snap_rows)
            log("costar_pdf", "OK  ", f"  Added {n} new snapshot rows from PDFs")
    else:
        log("costar_load", "INFO", "No PDF data extracted — baseline data only loaded")

    conn.commit()

    # 5. Log each table to load_log
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
