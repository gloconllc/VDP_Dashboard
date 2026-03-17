"""
load_visit_ca.py — Visit California data loader
Loads 4 tables from Visit CA Excel exports into data/analytics.sqlite
Tables: visit_ca_travel_forecast, visit_ca_lodging_forecast,
        visit_ca_airport_traffic, visit_ca_intl_arrivals
"""

import sqlite3
import pandas as pd
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

DB_PATH = Path(__file__).parent.parent / "data" / "analytics.sqlite"
DATA_DIR = Path(__file__).parent.parent / "data" / "Visit_California"

TRAVEL_FILE = DATA_DIR / "CATravelForecast_Feb2026_data.xlsx"
LODGING_FILE = DATA_DIR / "State and Regional lodging Forecast -February 2026.xlsx"
AIRPORT_FILE = DATA_DIR / "CAAirportPassengerTraffic -December 2025 .xlsx"
INTL_FILE = DATA_DIR / "CAInternationalArrivals_Jan 2026.xlsx"

# Forecast threshold: years >= this are forecast
FORECAST_FROM = 2025


def ts():
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS visit_ca_travel_forecast (
    id INTEGER PRIMARY KEY,
    year INTEGER,
    total_visits_m REAL,
    domestic_visits_m REAL,
    intl_visits_m REAL,
    leisure_visits_m REAL,
    business_visits_m REAL,
    total_yoy_pct REAL,
    domestic_yoy_pct REAL,
    intl_yoy_pct REAL,
    is_forecast INTEGER DEFAULT 0,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(year)
);

CREATE TABLE IF NOT EXISTS visit_ca_lodging_forecast (
    id INTEGER PRIMARY KEY,
    region TEXT,
    year INTEGER,
    supply_daily REAL,
    demand_daily REAL,
    occupancy_pct REAL,
    adr_usd REAL,
    revpar_usd REAL,
    room_revenue_b REAL,
    is_forecast INTEGER DEFAULT 0,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(region, year)
);

CREATE TABLE IF NOT EXISTS visit_ca_airport_traffic (
    id INTEGER PRIMARY KEY,
    airport TEXT,
    year INTEGER,
    month INTEGER,
    total_pax INTEGER,
    domestic_pax INTEGER,
    intl_pax INTEGER,
    total_yoy_pct REAL,
    domestic_yoy_pct REAL,
    intl_yoy_pct REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(airport, year, month)
);

CREATE TABLE IF NOT EXISTS visit_ca_intl_arrivals (
    id INTEGER PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    total_intl INTEGER,
    total_overseas INTEGER,
    priority_markets INTEGER,
    top_country TEXT,
    top_country_arrivals INTEGER,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(year, month)
);
"""

TABLE_RELATIONSHIPS = [
    ("visit_ca_travel_forecast", "kpi_daily_summary", "context", "year", "CA statewide visit forecast provides macro demand context for hotel KPIs"),
    ("visit_ca_lodging_forecast", "kpi_daily_summary", "context", "year", "CA/OC lodging forecast benchmarks occupancy and ADR against local STR data"),
    ("visit_ca_lodging_forecast", "fact_str_metrics", "context", "year", "Statewide lodging forecast vs local STR actuals"),
    ("visit_ca_airport_traffic", "datafy_overview_airports", "cross_ref", "airport", "CA airport pax trends validate feeder market reach"),
    ("visit_ca_intl_arrivals", "datafy_overview_dma", "context", "year", "International arrivals inform out-of-state visitor origin analysis"),
]


def _safe_float(val):
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _safe_int(val):
    f = _safe_float(val)
    return int(f) if f is not None else None


# ---------------------------------------------------------------------------
# 1. Travel Forecast
# ---------------------------------------------------------------------------

def load_travel_forecast(conn):
    """Parse CATravelForecast_Feb2026_data.xlsx into visit_ca_travel_forecast."""
    if not TRAVEL_FILE.exists():
        print(f"{ts()} [SKIP] travel forecast file not found: {TRAVEL_FILE}")
        return 0

    df = pd.read_excel(TRAVEL_FILE, header=None)

    # Row 5 contains year values starting at col 2
    years_row = df.iloc[5]
    year_cols = {}
    for col_idx, val in years_row.items():
        try:
            yr = int(float(val))
            if 2009 <= yr <= 2030 and col_idx not in year_cols.values():
                # Only take first occurrence of each year (cols 2-23 are actuals/forecast)
                if yr not in year_cols:
                    year_cols[yr] = col_idx
        except (TypeError, ValueError):
            pass

    # Rows of interest by label in column 1
    label_map = {
        "Total visits": "total",
        "Business": "business",
        "Leisure": "leisure",
    }
    # For domestic/intl we look at the sub-section rows
    # Row 12 = Domestic Total, Row 29+ = International Total
    # Simpler: find rows by exact label
    metric_data = {}
    for row_idx in range(len(df)):
        label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        if label == "Total visits":
            metric_data["total"] = row_idx
        elif label == "Business" and "business" not in metric_data:
            metric_data["business"] = row_idx
        elif label == "Leisure" and "leisure" not in metric_data:
            metric_data["leisure"] = row_idx

    # For domestic and intl totals: find section headers
    # Domestic total is under "Domestic" section header — label = "Total" after "Domestic"
    # Intl total is under "International" section header — label = "Total" after "International"
    in_domestic = False
    in_intl = False
    for row_idx in range(len(df)):
        label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        if label == "Domestic":
            in_domestic = True
            in_intl = False
        elif label == "International":
            in_intl = True
            in_domestic = False
        elif label == "Total":
            if in_domestic and "domestic_total" not in metric_data:
                metric_data["domestic_total"] = row_idx
            elif in_intl and "intl_total" not in metric_data:
                metric_data["intl_total"] = row_idx

    rows = []
    sorted_years = sorted(year_cols.keys())
    for i, yr in enumerate(sorted_years):
        col = year_cols[yr]
        total = _safe_float(df.iloc[metric_data.get("total", 0), col]) if "total" in metric_data else None
        business = _safe_float(df.iloc[metric_data.get("business", 0), col]) if "business" in metric_data else None
        leisure = _safe_float(df.iloc[metric_data.get("leisure", 0), col]) if "leisure" in metric_data else None
        domestic = _safe_float(df.iloc[metric_data.get("domestic_total", 0), col]) if "domestic_total" in metric_data else None
        intl = _safe_float(df.iloc[metric_data.get("intl_total", 0), col]) if "intl_total" in metric_data else None

        # YOY: compare to previous year if available
        def yoy(curr, prev_yr):
            if prev_yr not in year_cols or curr is None:
                return None
            prev_col = year_cols[prev_yr]
            prev_val = _safe_float(df.iloc[metric_data.get("total", 0), prev_col])
            if prev_val and prev_val != 0:
                return round((curr - prev_val) / prev_val * 100, 2)
            return None

        prev_yr = sorted_years[i - 1] if i > 0 else None
        total_yoy = None
        domestic_yoy = None
        intl_yoy = None
        if prev_yr:
            prev_col = year_cols[prev_yr]
            if total is not None and "total" in metric_data:
                prev_total = _safe_float(df.iloc[metric_data["total"], prev_col])
                if prev_total and prev_total != 0:
                    total_yoy = round((total - prev_total) / prev_total * 100, 2)
            if domestic is not None and "domestic_total" in metric_data:
                prev_dom = _safe_float(df.iloc[metric_data["domestic_total"], prev_col])
                if prev_dom and prev_dom != 0:
                    domestic_yoy = round((domestic - prev_dom) / prev_dom * 100, 2)
            if intl is not None and "intl_total" in metric_data:
                prev_intl = _safe_float(df.iloc[metric_data["intl_total"], prev_col])
                if prev_intl and prev_intl != 0:
                    intl_yoy = round((intl - prev_intl) / prev_intl * 100, 2)

        rows.append({
            "year": yr,
            "total_visits_m": round(total, 3) if total else None,
            "domestic_visits_m": round(domestic, 3) if domestic else None,
            "intl_visits_m": round(intl, 3) if intl else None,
            "leisure_visits_m": round(leisure, 3) if leisure else None,
            "business_visits_m": round(business, 3) if business else None,
            "total_yoy_pct": total_yoy,
            "domestic_yoy_pct": domestic_yoy,
            "intl_yoy_pct": intl_yoy,
            "is_forecast": 1 if yr >= FORECAST_FROM else 0,
        })

    count = 0
    cur = conn.cursor()
    for r in rows:
        if r["total_visits_m"] is None:
            continue
        cur.execute("""
            INSERT INTO visit_ca_travel_forecast
                (year, total_visits_m, domestic_visits_m, intl_visits_m,
                 leisure_visits_m, business_visits_m, total_yoy_pct,
                 domestic_yoy_pct, intl_yoy_pct, is_forecast)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(year) DO UPDATE SET
                total_visits_m=excluded.total_visits_m,
                domestic_visits_m=excluded.domestic_visits_m,
                intl_visits_m=excluded.intl_visits_m,
                leisure_visits_m=excluded.leisure_visits_m,
                business_visits_m=excluded.business_visits_m,
                total_yoy_pct=excluded.total_yoy_pct,
                domestic_yoy_pct=excluded.domestic_yoy_pct,
                intl_yoy_pct=excluded.intl_yoy_pct,
                is_forecast=excluded.is_forecast,
                loaded_at=datetime('now')
        """, (
            r["year"], r["total_visits_m"], r["domestic_visits_m"], r["intl_visits_m"],
            r["leisure_visits_m"], r["business_visits_m"], r["total_yoy_pct"],
            r["domestic_yoy_pct"], r["intl_yoy_pct"], r["is_forecast"]
        ))
        count += 1
    conn.commit()
    return count


# ---------------------------------------------------------------------------
# 2. Lodging Forecast
# ---------------------------------------------------------------------------

# Hardcoded seed values — always inserted regardless of parse success
OC_LODGING_SEED = [
    {"region": "Orange County", "year": 2025, "supply_daily": 61089, "demand_daily": 44610,
     "occupancy_pct": 73.0, "adr_usd": 209.53, "revpar_usd": 153.01, "room_revenue_b": 3.41, "is_forecast": 1},
    {"region": "Orange County", "year": 2026, "supply_daily": 60951, "demand_daily": 45258,
     "occupancy_pct": 74.3, "adr_usd": 212.49, "revpar_usd": 157.78, "room_revenue_b": 3.51, "is_forecast": 1},
    {"region": "California", "year": 2025, "supply_daily": 574798, "demand_daily": 386931,
     "occupancy_pct": 67.3, "adr_usd": 189.85, "revpar_usd": 127.80, "room_revenue_b": 26.8, "is_forecast": 1},
    {"region": "California", "year": 2026, "supply_daily": 581904, "demand_daily": 392251,
     "occupancy_pct": 67.4, "adr_usd": 194.64, "revpar_usd": 131.20, "room_revenue_b": 27.9, "is_forecast": 1},
]


def _parse_lodging_sheet(df, region_name):
    """
    Parse a lodging forecast sheet. Columns: supply(2), demand(3), occ(4), ADR(5), RevPAR(6), revenue(7).
    Year labels are in col 1 starting at 'Levels' section.
    Returns list of dicts.
    """
    rows = []
    # Find rows where col 1 is an integer year (2019-2030)
    for row_idx in range(len(df)):
        val = df.iloc[row_idx, 1]
        try:
            yr = int(float(val))
        except (TypeError, ValueError):
            continue
        if yr < 2015 or yr > 2030:
            continue

        supply = _safe_float(df.iloc[row_idx, 2])
        demand = _safe_float(df.iloc[row_idx, 3])
        occ = _safe_float(df.iloc[row_idx, 4])
        adr = _safe_float(df.iloc[row_idx, 5])
        revpar = _safe_float(df.iloc[row_idx, 6])
        revenue_raw = _safe_float(df.iloc[row_idx, 7])

        # Revenue is stored in raw dollars; convert to billions
        room_rev_b = round(revenue_raw / 1e9, 4) if revenue_raw else None
        # Convert occupancy from decimal (0.73) to percent
        occ_pct = round(occ * 100, 2) if occ and occ < 2 else (round(occ, 2) if occ else None)

        # Skip growth section duplicates (same years appear twice)
        # We only want the first occurrence per year for this region
        if any(r["region"] == region_name and r["year"] == yr for r in rows):
            continue

        rows.append({
            "region": region_name,
            "year": yr,
            "supply_daily": round(supply, 2) if supply else None,
            "demand_daily": round(demand, 2) if demand else None,
            "occupancy_pct": occ_pct,
            "adr_usd": round(adr, 4) if adr else None,
            "revpar_usd": round(revpar, 4) if revpar else None,
            "room_revenue_b": room_rev_b,
            "is_forecast": 1 if yr >= FORECAST_FROM else 0,
        })

    return rows


def load_lodging_forecast(conn):
    """Parse lodging forecast file into visit_ca_lodging_forecast."""
    cur = conn.cursor()
    count = 0

    # First seed hardcoded values
    for r in OC_LODGING_SEED:
        cur.execute("""
            INSERT INTO visit_ca_lodging_forecast
                (region, year, supply_daily, demand_daily, occupancy_pct,
                 adr_usd, revpar_usd, room_revenue_b, is_forecast)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(region, year) DO UPDATE SET
                supply_daily=excluded.supply_daily,
                demand_daily=excluded.demand_daily,
                occupancy_pct=excluded.occupancy_pct,
                adr_usd=excluded.adr_usd,
                revpar_usd=excluded.revpar_usd,
                room_revenue_b=excluded.room_revenue_b,
                is_forecast=excluded.is_forecast,
                loaded_at=datetime('now')
        """, (r["region"], r["year"], r["supply_daily"], r["demand_daily"],
              r["occupancy_pct"], r["adr_usd"], r["revpar_usd"], r["room_revenue_b"], r["is_forecast"]))
        count += 1
    conn.commit()

    if not LODGING_FILE.exists():
        print(f"{ts()} [WARN] lodging forecast file not found — seeded hardcoded values only")
        return count

    # Parse Excel sheets
    xl = pd.ExcelFile(LODGING_FILE)
    target_sheets = [s for s in xl.sheet_names if s.strip() not in ("Summary", "Gateways", "All Other Regions")]

    for sheet_name in target_sheets:
        region = sheet_name.strip()
        try:
            df_sheet = xl.parse(sheet_name, header=None)
            rows = _parse_lodging_sheet(df_sheet, region)
            for r in rows:
                if r["supply_daily"] is None:
                    continue
                cur.execute("""
                    INSERT INTO visit_ca_lodging_forecast
                        (region, year, supply_daily, demand_daily, occupancy_pct,
                         adr_usd, revpar_usd, room_revenue_b, is_forecast)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(region, year) DO UPDATE SET
                        supply_daily=excluded.supply_daily,
                        demand_daily=excluded.demand_daily,
                        occupancy_pct=excluded.occupancy_pct,
                        adr_usd=excluded.adr_usd,
                        revpar_usd=excluded.revpar_usd,
                        room_revenue_b=excluded.room_revenue_b,
                        is_forecast=excluded.is_forecast,
                        loaded_at=datetime('now')
                """, (r["region"], r["year"], r["supply_daily"], r["demand_daily"],
                      r["occupancy_pct"], r["adr_usd"], r["revpar_usd"], r["room_revenue_b"], r["is_forecast"]))
                count += 1
        except Exception as e:
            print(f"{ts()} [WARN] lodging sheet '{sheet_name}' parse error: {e}")
            continue

    conn.commit()
    return count


# ---------------------------------------------------------------------------
# 3. Airport Traffic
# ---------------------------------------------------------------------------

MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


def _parse_airport_sheet(df, airport_name):
    """
    Parse a single airport sheet. Structure:
      Row 3: headers — 2025, 2024, YOY% | 2025, 2024, YOY% | 2025, 2024, YOY%
      Rows 5-16: month rows (Total | Domestic | International data)
    Columns: 1=total_2025, 2=total_2024, 3=total_yoy | 5=dom_2025, 6=dom_2024, 7=dom_yoy | 9=intl_2025, 10=intl_2024, 11=intl_yoy
    """
    rows = []
    for row_idx in range(5, min(len(df), 20)):
        month_label = str(df.iloc[row_idx, 0]).strip() if pd.notna(df.iloc[row_idx, 0]) else ""
        if month_label not in MONTH_MAP:
            continue
        month_num = MONTH_MAP[month_label]

        total_pax = _safe_int(df.iloc[row_idx, 1])
        total_yoy = _safe_float(df.iloc[row_idx, 3])
        dom_pax = _safe_int(df.iloc[row_idx, 5])
        dom_yoy = _safe_float(df.iloc[row_idx, 7])

        # International: col 9 for 2025 value
        intl_pax = None
        intl_yoy = None
        ncols = len(df.columns)
        if ncols > 9:
            intl_pax = _safe_int(df.iloc[row_idx, 9])
        if ncols > 11:
            intl_yoy = _safe_float(df.iloc[row_idx, 11])

        if total_pax is None:
            continue

        rows.append({
            "airport": airport_name,
            "year": 2025,
            "month": month_num,
            "total_pax": total_pax,
            "domestic_pax": dom_pax,
            "intl_pax": intl_pax,
            "total_yoy_pct": round(total_yoy * 100, 4) if total_yoy is not None else None,
            "domestic_yoy_pct": round(dom_yoy * 100, 4) if dom_yoy is not None else None,
            "intl_yoy_pct": round(intl_yoy * 100, 4) if intl_yoy is not None else None,
        })
    return rows


def load_airport_traffic(conn):
    """Parse airport traffic file into visit_ca_airport_traffic."""
    if not AIRPORT_FILE.exists():
        print(f"{ts()} [SKIP] airport traffic file not found: {AIRPORT_FILE}")
        return 0

    xl = pd.ExcelFile(AIRPORT_FILE)
    skip_sheets = {"CALIFORNIA", "High Low stats"}
    cur = conn.cursor()
    count = 0

    for sheet_name in xl.sheet_names:
        if sheet_name in skip_sheets:
            continue
        try:
            df_sheet = xl.parse(sheet_name, header=None)
            # Airport name from cell A0
            airport_label = str(df_sheet.iloc[0, 0]).strip() if pd.notna(df_sheet.iloc[0, 0]) else sheet_name
            rows = _parse_airport_sheet(df_sheet, airport_label)
            for r in rows:
                cur.execute("""
                    INSERT INTO visit_ca_airport_traffic
                        (airport, year, month, total_pax, domestic_pax, intl_pax,
                         total_yoy_pct, domestic_yoy_pct, intl_yoy_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(airport, year, month) DO UPDATE SET
                        total_pax=excluded.total_pax,
                        domestic_pax=excluded.domestic_pax,
                        intl_pax=excluded.intl_pax,
                        total_yoy_pct=excluded.total_yoy_pct,
                        domestic_yoy_pct=excluded.domestic_yoy_pct,
                        intl_yoy_pct=excluded.intl_yoy_pct,
                        loaded_at=datetime('now')
                """, (r["airport"], r["year"], r["month"], r["total_pax"], r["domestic_pax"],
                      r["intl_pax"], r["total_yoy_pct"], r["domestic_yoy_pct"], r["intl_yoy_pct"]))
                count += 1
        except Exception as e:
            print(f"{ts()} [WARN] airport sheet '{sheet_name}': {e}")
            continue

    conn.commit()
    return count


# ---------------------------------------------------------------------------
# 4. International Arrivals
# ---------------------------------------------------------------------------

def _parse_intl_sheet(df, year):
    """Parse one year's international arrivals sheet. Returns list of monthly dicts."""
    rows = []

    # Row 2 = headers with datetime objects for each month (cols 1-12), col 13 = annual total
    # Row 3 = Total International
    # Row 4 = Total Overseas
    # Row 5 = Total Priority Markets
    # Find top-country row: first row after row 5 with largest value in col 1 (Jan)
    try:
        total_row = df.iloc[3]
        overseas_row = df.iloc[4]
        priority_row = df.iloc[5]
    except IndexError:
        return rows

    # Find top country by highest Jan arrivals among country rows (rows 6+)
    top_country = None
    top_arrivals_jan = 0
    for row_idx in range(6, min(len(df), 50)):
        country_label = str(df.iloc[row_idx, 0]).strip() if pd.notna(df.iloc[row_idx, 0]) else ""
        if not country_label or country_label.startswith("Total") or country_label.startswith("Other"):
            continue
        val = _safe_int(df.iloc[row_idx, 1])
        if val and val > top_arrivals_jan:
            top_arrivals_jan = val
            top_country = country_label

    for month_col in range(1, 13):
        try:
            month_num = month_col  # col 1 = Jan, col 12 = Dec
            total_intl = _safe_int(total_row.iloc[month_col])
            total_overseas = _safe_int(overseas_row.iloc[month_col])
            priority = _safe_int(priority_row.iloc[month_col])

            # Top country for this month
            top_arrivals_month = None
            for row_idx in range(6, min(len(df), 50)):
                country_label = str(df.iloc[row_idx, 0]).strip() if pd.notna(df.iloc[row_idx, 0]) else ""
                if country_label == top_country:
                    top_arrivals_month = _safe_int(df.iloc[row_idx, month_col])
                    break

            if total_intl is None:
                continue

            rows.append({
                "year": year,
                "month": month_num,
                "total_intl": total_intl,
                "total_overseas": total_overseas,
                "priority_markets": priority,
                "top_country": top_country,
                "top_country_arrivals": top_arrivals_month,
            })
        except (IndexError, Exception):
            continue

    return rows


def load_intl_arrivals(conn):
    """Parse international arrivals file into visit_ca_intl_arrivals."""
    if not INTL_FILE.exists():
        print(f"{ts()} [SKIP] intl arrivals file not found: {INTL_FILE}")
        return 0

    xl = pd.ExcelFile(INTL_FILE)
    skip_sheets = {"Read Me", "Data "}
    cur = conn.cursor()
    count = 0

    for sheet_name in xl.sheet_names:
        if sheet_name in skip_sheets:
            continue
        try:
            year_str = sheet_name.strip()
            year = int(year_str)
        except ValueError:
            continue

        try:
            df_sheet = xl.parse(sheet_name, header=None)
            rows = _parse_intl_sheet(df_sheet, year)
            for r in rows:
                cur.execute("""
                    INSERT INTO visit_ca_intl_arrivals
                        (year, month, total_intl, total_overseas, priority_markets,
                         top_country, top_country_arrivals)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(year, month) DO UPDATE SET
                        total_intl=excluded.total_intl,
                        total_overseas=excluded.total_overseas,
                        priority_markets=excluded.priority_markets,
                        top_country=excluded.top_country,
                        top_country_arrivals=excluded.top_country_arrivals,
                        loaded_at=datetime('now')
                """, (r["year"], r["month"], r["total_intl"], r["total_overseas"],
                      r["priority_markets"], r["top_country"], r["top_country_arrivals"]))
                count += 1
        except Exception as e:
            print(f"{ts()} [WARN] intl arrivals sheet '{sheet_name}': {e}")
            continue

    conn.commit()
    return count


# ---------------------------------------------------------------------------
# load_log + table_relationships
# ---------------------------------------------------------------------------

def log_load(conn, table, rows):
    conn.execute("""
        INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, ("Visit_CA", "annual", table, rows))
    conn.commit()


def seed_table_relationships(conn):
    """Upsert Visit CA relationships into table_relationships."""
    cur = conn.cursor()
    # Check if table exists
    existing = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='table_relationships'").fetchone()
    if not existing:
        return
    for (a, b, rel, key, desc) in TABLE_RELATIONSHIPS:
        cur.execute("""
            INSERT INTO table_relationships (table_a, table_b, relationship_type, join_key, description)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
        """, (a, b, rel, key, desc))
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"{ts()} [START] load_visit_ca.py")

    conn = sqlite3.connect(DB_PATH)
    conn.executescript(DDL)
    conn.commit()

    # 1. Travel forecast
    n = load_travel_forecast(conn)
    print(f"{ts()} [{'OK  ' if n > 0 else 'WARN'}] visit_ca_travel_forecast: {n} rows")
    log_load(conn, "visit_ca_travel_forecast", n)

    # 2. Lodging forecast
    n = load_lodging_forecast(conn)
    print(f"{ts()} [{'OK  ' if n > 0 else 'WARN'}] visit_ca_lodging_forecast: {n} rows")
    log_load(conn, "visit_ca_lodging_forecast", n)

    # 3. Airport traffic
    n = load_airport_traffic(conn)
    print(f"{ts()} [{'OK  ' if n > 0 else 'WARN'}] visit_ca_airport_traffic: {n} rows")
    log_load(conn, "visit_ca_airport_traffic", n)

    # 4. International arrivals
    n = load_intl_arrivals(conn)
    print(f"{ts()} [{'OK  ' if n > 0 else 'WARN'}] visit_ca_intl_arrivals: {n} rows")
    log_load(conn, "visit_ca_intl_arrivals", n)

    # Seed table relationships
    seed_table_relationships(conn)

    conn.close()
    print(f"{ts()} [DONE] load_visit_ca.py complete")


if __name__ == "__main__":
    main()
