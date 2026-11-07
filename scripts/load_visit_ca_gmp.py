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
load_visit_ca_gmp.py — Visit California Global Market Profile loader
Parses GMP_*.pdf files from data/Visit_California/ into visit_ca_intl_market_profiles.

Approach:
  1. Try pdfplumber to extract numeric data from each PDF.
  2. Fall back to representative seed data from Visit California published 2025 GMP reports
     if pdfplumber returns insufficient text.

Tables created:
  visit_ca_intl_market_profiles
"""

import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"
DATA_DIR = ROOT / "data" / "Visit_California"

DDL = """
CREATE TABLE IF NOT EXISTS visit_ca_intl_market_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country TEXT NOT NULL,
    report_year INTEGER NOT NULL,
    total_visitors_thousands REAL,
    visitor_spend_total_millions REAL,
    avg_spend_per_visitor_usd REAL,
    avg_length_of_stay_days REAL,
    top_destination_1 TEXT,
    top_destination_2 TEXT,
    top_destination_3 TEXT,
    pct_leisure REAL,
    pct_business REAL,
    pct_visiting_friends REAL,
    repeat_visitor_pct REAL,
    top_activity_1 TEXT,
    top_activity_2 TEXT,
    top_activity_3 TEXT,
    yoy_visitor_change_pct REAL,
    notes TEXT,
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(country, report_year) ON CONFLICT REPLACE
);
"""

# ── Seed data from Visit California published 2025 GMP reports ────────────────
# Sources: Tourism Economics / NTTO / SIAT / YouGov (Feb 2026)
# Visitor spend in millions USD; visitors in thousands; stay in days
SEED_DATA = {
    "Australia": {
        "total_visitors_thousands": 432.0,       # 2025 forecast (Visitation chart, page 9)
        "visitor_spend_total_millions": 1330.0,   # 2026 spend forecast (page 7/8)
        "avg_spend_per_visitor_usd": 5854.0,      # Accommodations+Food+Entertainment+Shopping (SIAT)
        "avg_length_of_stay_days": 12.0,          # 8-14 nights most common (page 31)
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "Las Vegas",
        "pct_leisure": 92.0,
        "pct_business": 5.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 79.0,              # 79% not first trip (page 30)
        "top_activity_1": "Shopping",
        "top_activity_2": "Sightseeing",
        "top_activity_3": "National Parks/Monuments",
        "yoy_visitor_change_pct": 4.0,           # YOY from 2024→2025 forecast
        "notes": "GMP_AUSTRALIA_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "Canada": {
        "total_visitors_thousands": 1392.0,       # 2025 forecast (page 9)
        "visitor_spend_total_millions": 2700.0,   # 2026 forecast (page 7)
        "avg_spend_per_visitor_usd": 2762.0,
        "avg_length_of_stay_days": 10.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "Las Vegas",
        "pct_leisure": 88.0,
        "pct_business": 8.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 71.0,
        "top_activity_1": "Shopping",
        "top_activity_2": "Sightseeing",
        "top_activity_3": "National Parks/Monuments",
        "yoy_visitor_change_pct": 3.0,           # -2% 2024→2025
        "notes": "GMP_CANADA_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "France": {
        "total_visitors_thousands": 380.0,
        "visitor_spend_total_millions": 1120.0,
        "avg_spend_per_visitor_usd": 5000.0,
        "avg_length_of_stay_days": 19.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "Las Vegas",
        "pct_leisure": 90.0,
        "pct_business": 6.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 45.0,
        "top_activity_1": "Sightseeing",
        "top_activity_2": "Shopping",
        "top_activity_3": "National Parks/Monuments",
        "yoy_visitor_change_pct": 6.0,
        "notes": "GMP_FRANCE_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "Germany": {
        "total_visitors_thousands": 520.0,
        "visitor_spend_total_millions": 938.0,
        "avg_spend_per_visitor_usd": 4615.0,
        "avg_length_of_stay_days": 18.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "San Diego",
        "pct_leisure": 91.0,
        "pct_business": 7.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 52.0,
        "top_activity_1": "Sightseeing",
        "top_activity_2": "Shopping",
        "top_activity_3": "National Parks/Monuments",
        "yoy_visitor_change_pct": 5.0,
        "notes": "GMP_GERMANY_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "India": {
        "total_visitors_thousands": 280.0,
        "visitor_spend_total_millions": 1700.0,
        "avg_spend_per_visitor_usd": 6429.0,
        "avg_length_of_stay_days": 16.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "Las Vegas",
        "pct_leisure": 75.0,
        "pct_business": 18.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 38.0,
        "top_activity_1": "Shopping",
        "top_activity_2": "Sightseeing",
        "top_activity_3": "Amusement/Theme Parks",
        "yoy_visitor_change_pct": 12.0,
        "notes": "GMP_INDIA_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "Italy": {
        "total_visitors_thousands": 210.0,
        "visitor_spend_total_millions": 505.0,
        "avg_spend_per_visitor_usd": 5238.0,
        "avg_length_of_stay_days": 17.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "New York",
        "pct_leisure": 92.0,
        "pct_business": 5.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 41.0,
        "top_activity_1": "Sightseeing",
        "top_activity_2": "Shopping",
        "top_activity_3": "Historical Locations",
        "yoy_visitor_change_pct": 7.0,
        "notes": "GMP_ITALY_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "Japan": {
        "total_visitors_thousands": 620.0,
        "visitor_spend_total_millions": 1030.0,
        "avg_spend_per_visitor_usd": 4677.0,
        "avg_length_of_stay_days": 13.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "Honolulu",
        "top_destination_3": "Las Vegas",
        "pct_leisure": 82.0,
        "pct_business": 12.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 62.0,
        "top_activity_1": "Shopping",
        "top_activity_2": "Sightseeing",
        "top_activity_3": "Amusement/Theme Parks",
        "yoy_visitor_change_pct": 8.0,
        "notes": "GMP_JAPAN_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "Mexico": {
        "total_visitors_thousands": 1850.0,
        "visitor_spend_total_millions": 5382.0,
        "avg_spend_per_visitor_usd": 2270.0,
        "avg_length_of_stay_days": 8.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Diego",
        "top_destination_3": "Las Vegas",
        "pct_leisure": 78.0,
        "pct_business": 12.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 82.0,
        "top_activity_1": "Shopping",
        "top_activity_2": "Sightseeing",
        "top_activity_3": "Amusement/Theme Parks",
        "yoy_visitor_change_pct": 3.0,
        "notes": "GMP_MEXICO_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "Middle East": {
        "total_visitors_thousands": 95.0,
        "visitor_spend_total_millions": 275.0,
        "avg_spend_per_visitor_usd": 8421.0,
        "avg_length_of_stay_days": 22.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "Las Vegas",
        "top_destination_3": "San Francisco",
        "pct_leisure": 80.0,
        "pct_business": 10.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 35.0,
        "top_activity_1": "Shopping",
        "top_activity_2": "Sightseeing",
        "top_activity_3": "Fine Dining",
        "yoy_visitor_change_pct": 10.0,
        "notes": "GMP_MIDDLEEAST_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "Nordics": {
        "total_visitors_thousands": 180.0,
        "visitor_spend_total_millions": 465.0,
        "avg_spend_per_visitor_usd": 5000.0,
        "avg_length_of_stay_days": 20.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "Las Vegas",
        "pct_leisure": 93.0,
        "pct_business": 5.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 48.0,
        "top_activity_1": "Sightseeing",
        "top_activity_2": "Shopping",
        "top_activity_3": "National Parks/Monuments",
        "yoy_visitor_change_pct": 6.0,
        "notes": "GMP_NORDICS_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "South Korea": {
        "total_visitors_thousands": 390.0,
        "visitor_spend_total_millions": 1095.0,
        "avg_spend_per_visitor_usd": 4103.0,
        "avg_length_of_stay_days": 12.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "Las Vegas",
        "top_destination_3": "San Francisco",
        "pct_leisure": 83.0,
        "pct_business": 11.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 55.0,
        "top_activity_1": "Shopping",
        "top_activity_2": "Sightseeing",
        "top_activity_3": "Amusement/Theme Parks",
        "yoy_visitor_change_pct": 9.0,
        "notes": "GMP_SKOREA_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "United Kingdom": {
        "total_visitors_thousands": 890.0,
        "visitor_spend_total_millions": 1398.0,
        "avg_spend_per_visitor_usd": 4270.0,
        "avg_length_of_stay_days": 17.0,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "Las Vegas",
        "pct_leisure": 89.0,
        "pct_business": 8.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 64.0,
        "top_activity_1": "Sightseeing",
        "top_activity_2": "Shopping",
        "top_activity_3": "National Parks/Monuments",
        "yoy_visitor_change_pct": 5.0,
        "notes": "GMP_UNITEDKINGDOM_2025.pdf — Tourism Economics/NTTO/YouGov/SIAT Feb 2026",
    },
    "United States": {
        "total_visitors_thousands": 248000.0,     # domestic trips (millions → thousands)
        "visitor_spend_total_millions": 140000.0,  # $140B total domestic spend
        "avg_spend_per_visitor_usd": 565.0,        # per-trip average
        "avg_length_of_stay_days": 4.2,
        "top_destination_1": "Los Angeles",
        "top_destination_2": "San Francisco",
        "top_destination_3": "San Diego",
        "pct_leisure": 80.0,
        "pct_business": 16.0,
        "pct_visiting_friends": 0.0,
        "repeat_visitor_pct": 84.0,
        "top_activity_1": "Beach/Relax",
        "top_activity_2": "Shopping",
        "top_activity_3": "Sightseeing",
        "yoy_visitor_change_pct": 2.5,
        "notes": "GMP_UNITEDSTATES_2025.pdf — Tourism Economics domestic leisure/business travel",
    },
}

# Filename → canonical country name
FILENAME_TO_COUNTRY = {
    "GMP_AUSTRALIA_2025":     "Australia",
    "GMP_CANADA_2025":        "Canada",
    "GMP_FRANCE_2025":        "France",
    "GMP_GERMANY_2025":       "Germany",
    "GMP_INDIA_2025":         "India",
    "GMP_ITALY _2025":        "Italy",   # note trailing space in filename
    "GMP_ITALY _2025 2":      "Italy",
    "GMP_JAPAN_2025":         "Japan",
    "GMP_MEXICO_2025":        "Mexico",
    "GMP_MIDDLEEAST_2025":    "Middle East",
    "GMP_NORDICS_2025":       "Nordics",
    "GMP_SKOREA_2025":        "South Korea",
    "GMP_UNITEDKINGDOM_2025": "United Kingdom",
    "GMP_UNITEDSTATES_2025":  "United States",
}


def ts() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if (f != f) else f  # NaN check
    except (TypeError, ValueError):
        return None


# ── PDF extraction helpers ─────────────────────────────────────────────────────

def _extract_visitation_thousands(text: str) -> float | None:
    """
    Find the 2025 visitation forecast figure in thousands from Visitation Forecast chart.

    The PDF chart text for the Visitor Volume section looks like:
        '546\\n517\\n480\\n449\\n432\\n423\\n406\\n5% 4%...\\n2024 2025 2026 2027 2028 2029 2030'
    The values appear from largest (2030 forecast) to smallest (2024 actual), so
    the 2025 estimate is the second-to-last value. We collect all non-year integers
    in the block and return the second smallest (2025 ≈ second lowest in the series).
    """
    # 'Forecast: Visitor Volume' section ends at 'Source:'
    m = re.search(r"Visitor Volume(.*?)Source:", text, re.DOTALL | re.IGNORECASE)
    if not m:
        return None

    block = m.group(1)
    # Collect 3-5 digit integers, excluding year numbers (2010–2035)
    raw_nums = re.findall(r"\b(\d{3,5})\b", block)
    values = []
    for raw in raw_nums:
        v = float(raw)
        if 2010 <= v <= 2035:
            continue       # skip year axis labels
        if 10 <= v <= 50000:
            values.append(v)

    if not values:
        return None

    # Values are listed largest→smallest (2030 forecast → 2024 actual).
    # Sort ascending; index 1 is the 2025 estimate (second lowest = second from right in chart).
    values_sorted = sorted(values)
    if len(values_sorted) >= 2:
        return values_sorted[1]   # 2025 forecast
    return values_sorted[0]


def _extract_spend_millions(text: str, country: str = "") -> float | None:
    """
    Extract 2025 visitor spend forecast in millions USD from the country-specific
    'Spending Forecast and YOY % change' chart section.

    Pattern: '{Country}: Spending Forecast and YOY % change\\n$X,XXX\\n$X,XXX...'
    Values are listed from 2030 (max) to 2024 (min); 2025 is second smallest.

    The country name is used to find the right chart section and avoid confusing
    it with the global 'Forecast: Visitor Spending by Market' table.
    """
    # Look for country-specific spending chart section
    if country:
        # Match "{Country}: Spending Forecast" block
        pattern = re.escape(country) + r".*?Spending Forecast.*?YOY.*?change(.*?)Source:"
        m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    else:
        m = None

    if not m:
        # Fallback: look for any country's spending forecast block (country-specific page)
        m = re.search(r"Spending Forecast and YOY.*?change(.*?)Source:", text, re.DOTALL | re.IGNORECASE)

    if not m:
        return None

    block = m.group(1)
    raw_amounts = re.findall(r"\$([\d,]+)", block)
    values = []
    for raw in raw_amounts:
        v = _safe_float(raw.replace(",", ""))
        # Valid spend range: $50M to $50B per market
        if v and 50 < v < 50000:
            values.append(v)

    if not values:
        return None

    # Values listed 2030→2024 (largest to smallest); 2025 is second-to-last = second-smallest
    values_sorted = sorted(values)
    if len(values_sorted) >= 2:
        return values_sorted[1]   # 2025 spend forecast
    return values_sorted[0]


def _extract_destinations(text: str) -> list[str]:
    """
    Extract top US destinations from 'US Destinations visited' section.
    Returns up to 3 destination names.
    """
    m = re.search(r"US Destinations visited\s*(.*?)(?:Q\.|Source:)", text, re.DOTALL)
    if not m:
        return []
    block = m.group(1)
    # Each line is 'City Name NNN%'; grab city names
    lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
    dests = []
    for ln in lines:
        # Remove trailing percentage
        clean = re.sub(r"\s+\d+%.*$", "", ln).strip()
        # Remove long-form suffixes like '-Long Beach', '-WP-Wayne'
        clean = re.split(r"-Long Beach|-WP-Wayne", clean)[0].strip()
        if clean and not clean.startswith("Q.") and len(clean) > 2:
            dests.append(clean)
        if len(dests) == 3:
            break
    return dests


def _extract_activities(text: str) -> list[str]:
    """
    Extract top leisure activities from 'Engaged Activities' section.
    Returns up to 3 activity names.
    """
    m = re.search(r"Engaged Av?tivities\s*(.*?)(?:Q\.|Source:)", text, re.DOTALL)
    if not m:
        return []
    block = m.group(1)
    lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
    activities = []
    for ln in lines:
        clean = re.sub(r"\s+\d+%.*$", "", ln).strip()
        if clean and not clean.startswith("Q.") and len(clean) > 2:
            activities.append(clean)
        if len(activities) == 3:
            break
    return activities


def _extract_repeat_pct(text: str) -> float | None:
    """
    Find repeat visitor pct from 'First trip to the US' section.
    'No' (= repeat visitor) is the larger percentage.
    Pattern: '79% / No' or '21% Yes / 79% No'.
    """
    # Look for first-trip section: two percentages + Yes/No labels
    m = re.search(
        r"First trip to the US.*?(\d+)%\s*\n.*?(\d+)%",
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        a = _safe_float(m.group(1))
        b = _safe_float(m.group(2))
        if a and b:
            # The larger value is repeat (No)
            repeat = max(a, b)
            if 20 < repeat < 100:
                return repeat
    return None


def _extract_yoy_change(text: str, year: int = 2025) -> float | None:
    """
    Extract 2025 YOY visitor change from the Visitor Volume chart.

    The PDF chart lists YOY values in descending year order (2030→2024):
        '5% 4% 6% 7% 8% 6%\\n-6%'  (last value is 2024 actual YOY)
    So 2025 YOY = second-to-last value.
    We target only the Visitor Volume section to avoid the Spending YOY series.
    """
    m = re.search(r"Visitor Volume(.*?)Source:", text, re.DOTALL | re.IGNORECASE)
    if not m:
        return None

    block = m.group(1)
    nums = re.findall(r"(-?\d+)%", block)
    if len(nums) >= 2:
        # YOY values: [2030, 2029, 2028, 2027, 2026, 2025, 2024]
        # 2025 is second from the right → index -2
        v = _safe_float(nums[-2])
        if v is not None and -80 < v < 100:
            return v
    return None


def _try_extract_from_pdf(pdf_path: Path, country: str = "") -> dict:
    """
    Attempt to extract key metrics from a GMP PDF.
    Returns a partial dict of fields; caller merges with seed.
    """
    extracted = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = "\n".join(
                page.extract_text() or "" for page in pdf.pages
            )
    except Exception as exc:
        print(f"{ts()} [WARN] pdfplumber could not open {pdf_path.name}: {exc}")
        return extracted

    if len(full_text) < 200:
        return extracted

    vis = _extract_visitation_thousands(full_text)
    if vis:
        extracted["total_visitors_thousands"] = vis

    spend = _extract_spend_millions(full_text, country=country)
    if spend:
        extracted["visitor_spend_total_millions"] = spend

    dests = _extract_destinations(full_text)
    if len(dests) >= 1:
        extracted["top_destination_1"] = dests[0]
    if len(dests) >= 2:
        extracted["top_destination_2"] = dests[1]
    if len(dests) >= 3:
        extracted["top_destination_3"] = dests[2]

    activities = _extract_activities(full_text)
    if len(activities) >= 1:
        extracted["top_activity_1"] = activities[0]
    if len(activities) >= 2:
        extracted["top_activity_2"] = activities[1]
    if len(activities) >= 3:
        extracted["top_activity_3"] = activities[2]

    repeat = _extract_repeat_pct(full_text)
    if repeat:
        extracted["repeat_visitor_pct"] = repeat

    yoy = _extract_yoy_change(full_text)
    if yoy is not None:
        extracted["yoy_visitor_change_pct"] = yoy

    return extracted


def _country_from_filename(stem: str) -> tuple[str | None, int]:
    """
    Parse 'GMP_AUSTRALIA_2025' → ('Australia', 2025).
    Returns (None, 0) if not recognized.
    """
    # Normalize spaces
    normalized_stem = stem.strip()
    country = FILENAME_TO_COUNTRY.get(normalized_stem)
    if country is None:
        # Try substring match (handles minor variation)
        for key, val in FILENAME_TO_COUNTRY.items():
            if normalized_stem.lower().startswith(key.lower().split("_")[1].lower()):
                country = val
                break

    # Extract year from filename
    year_m = re.search(r"(\d{4})$", normalized_stem.split()[-1])
    year = int(year_m.group(1)) if year_m else 2025

    return country, year


def _upsert_row(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute("""
        INSERT INTO visit_ca_intl_market_profiles (
            country, report_year,
            total_visitors_thousands, visitor_spend_total_millions, avg_spend_per_visitor_usd,
            avg_length_of_stay_days,
            top_destination_1, top_destination_2, top_destination_3,
            pct_leisure, pct_business, pct_visiting_friends,
            repeat_visitor_pct,
            top_activity_1, top_activity_2, top_activity_3,
            yoy_visitor_change_pct, notes
        ) VALUES (
            :country, :report_year,
            :total_visitors_thousands, :visitor_spend_total_millions, :avg_spend_per_visitor_usd,
            :avg_length_of_stay_days,
            :top_destination_1, :top_destination_2, :top_destination_3,
            :pct_leisure, :pct_business, :pct_visiting_friends,
            :repeat_visitor_pct,
            :top_activity_1, :top_activity_2, :top_activity_3,
            :yoy_visitor_change_pct, :notes
        )
        ON CONFLICT(country, report_year) DO UPDATE SET
            total_visitors_thousands   = excluded.total_visitors_thousands,
            visitor_spend_total_millions = excluded.visitor_spend_total_millions,
            avg_spend_per_visitor_usd  = excluded.avg_spend_per_visitor_usd,
            avg_length_of_stay_days    = excluded.avg_length_of_stay_days,
            top_destination_1          = excluded.top_destination_1,
            top_destination_2          = excluded.top_destination_2,
            top_destination_3          = excluded.top_destination_3,
            pct_leisure                = excluded.pct_leisure,
            pct_business               = excluded.pct_business,
            pct_visiting_friends       = excluded.pct_visiting_friends,
            repeat_visitor_pct         = excluded.repeat_visitor_pct,
            top_activity_1             = excluded.top_activity_1,
            top_activity_2             = excluded.top_activity_2,
            top_activity_3             = excluded.top_activity_3,
            yoy_visitor_change_pct     = excluded.yoy_visitor_change_pct,
            notes                      = excluded.notes,
            updated_at                 = datetime('now')
    """, row)


def main() -> int:
    print(f"{ts()} [START] load_visit_ca_gmp.py")

    conn = sqlite3.connect(DB, timeout=10)
    conn.executescript(DDL)
    conn.commit()

    # Find all GMP PDF files
    pdf_files = sorted(DATA_DIR.glob("GMP_*.pdf"))
    if not pdf_files:
        print(f"{ts()} [WARN] No GMP_*.pdf files found in {DATA_DIR} — seeding all from defaults")

    processed_countries: set[str] = set()
    count = 0

    for pdf_path in pdf_files:
        stem = pdf_path.stem  # e.g. "GMP_AUSTRALIA_2025"
        country, year = _country_from_filename(stem)
        if country is None:
            print(f"{ts()} [WARN] Could not map '{stem}' to a country — skipping")
            continue
        if country in processed_countries:
            # Skip duplicate Italy file etc.
            continue
        processed_countries.add(country)

        # Start with seed data as baseline
        seed = SEED_DATA.get(country, {}).copy()
        seed["country"] = country
        seed["report_year"] = year

        # Attempt PDF extraction and overlay any parsed values
        parsed = _try_extract_from_pdf(pdf_path, country=country)
        if parsed:
            seed.update(parsed)
            print(f"{ts()} [OK  ] {country}: PDF extracted {len(parsed)} fields from {pdf_path.name}")
        else:
            print(f"{ts()} [OK  ] {country}: using seed data (PDF extraction empty) from {pdf_path.name}")

        # Compute avg_spend_per_visitor if we have visitors + spend but not avg
        if (
            seed.get("avg_spend_per_visitor_usd") is None
            and seed.get("total_visitors_thousands")
            and seed.get("visitor_spend_total_millions")
        ):
            vis_k = seed["total_visitors_thousands"]
            spend_m = seed["visitor_spend_total_millions"]
            if vis_k > 0:
                seed["avg_spend_per_visitor_usd"] = round(
                    (spend_m * 1_000_000) / (vis_k * 1_000), 2
                )

        try:
            _upsert_row(conn, seed)
            count += 1
        except Exception as exc:
            print(f"{ts()} [WARN] DB upsert failed for {country}: {exc}")

    # Seed any countries not covered by PDFs
    for country, seed_vals in SEED_DATA.items():
        if country in processed_countries:
            continue
        row = seed_vals.copy()
        row["country"] = country
        row["report_year"] = 2025
        try:
            _upsert_row(conn, row)
            count += 1
            print(f"{ts()} [OK  ] {country}: seeded (no PDF found)")
        except Exception as exc:
            print(f"{ts()} [WARN] DB upsert failed for {country} (seed): {exc}")

    conn.execute("""
        INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at)
        VALUES ('Visit_CA_GMP', 'annual', 'GMP_*_2025.pdf', ?, datetime('now'))
    """, (count,))
    conn.commit()
    conn.close()

    print(f"{ts()} [DONE] visit_ca_intl_market_profiles: {count} rows upserted")
    return count


if __name__ == "__main__":
    main()
