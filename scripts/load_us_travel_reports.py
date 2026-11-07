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
load_us_travel_reports.py
─────────────────────────
Loads U.S. Travel Association national benchmark data into analytics.sqlite.

Data sources:
  - PDF reports in data/us_travel/ (State of Group Travel, Business Travel,
    Travel Recovery Insights Dashboard — downloaded monthly from ustravel.org)
  - Hardcoded 2024 benchmark data seeded on every run (from published reports)

Tables populated:
  us_travel_group_segments     — 4 group travel segments with $ spend + jobs (annual)
  us_travel_business_travel    — Business + transient travel totals & recovery rates
  us_travel_traveler_types     — 10 traveler type benchmarks (booking window, LOS, priorities)
  us_travel_national_kpis      — National travel KPIs for year-over-year context

How it works:
  1. Always seeds/upserts 2024 hardcoded benchmark values (from published reports)
  2. Scans data/us_travel/*.pdf — extracts text with pdftotext (poppler-utils)
  3. Regex patterns extract updated dollar figures, percentages, recovery rates
  4. UPSERTs extracted values into tables (keyed on report_year + segment/category)

Run monthly after downloading new US Travel PDFs.
Non-fatal in pipeline — logs WARN and continues if no PDFs found.

Setup:
  - Install poppler-utils: apt-get install poppler-utils  (already in Streamlit Cloud)
  - Download PDFs monthly from ustravel.org/state-of-group-travel-report
    and ustravel.org/research/travel-recovery-insights-dashboard
  - Save to data/us_travel/ and run pipeline

DB path: data/analytics.sqlite
"""

import os
import re
import json
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
US_TRAVEL_DIR = PROJECT_ROOT / "data" / "us_travel"
LOG_PATH     = PROJECT_ROOT / "logs" / "pipeline.log"

NOW = datetime.utcnow().isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    line = f"[{_ts()}] [{status:<4}] {step}: {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# DB Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS us_travel_group_segments (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    report_year             INTEGER NOT NULL,
    segment                 TEXT NOT NULL,           -- meetings_events, participatory_sports, live_spectator, leisure_group, total_group
    spend_billion_usd       REAL,                    -- annual economic impact in $B
    jobs_supported          INTEGER,                 -- direct + indirect jobs
    pct_recovery_vs_2019    REAL,                    -- % recovery vs 2019 baseline
    notes                   TEXT,
    source_file             TEXT,
    loaded_at               TEXT DEFAULT (datetime('now')),
    UNIQUE(report_year, segment)
);

CREATE TABLE IF NOT EXISTS us_travel_business_travel (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    report_year             INTEGER NOT NULL,
    category                TEXT NOT NULL,           -- total_business, transient_business, meetings_events
    spend_billion_usd       REAL,
    pct_recovery_vs_2019    REAL,
    pct_total_lodging_rev   REAL,                    -- % of total lodging/air revenue (business travelers = 20% volume, 60% revenue)
    pct_total_air_rev       REAL,
    notes                   TEXT,
    source_file             TEXT,
    loaded_at               TEXT DEFAULT (datetime('now')),
    UNIQUE(report_year, category)
);

CREATE TABLE IF NOT EXISTS us_travel_traveler_types (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    report_year             INTEGER NOT NULL,
    traveler_type           TEXT NOT NULL,           -- solo, family, business, leisure, adventure, group_smerf, budget, luxury, foodie, pet
    primary_motivation      TEXT,
    booking_window_weeks_low  INTEGER,               -- typical booking window in weeks (low end)
    booking_window_weeks_high INTEGER,
    typical_los_nights_low  REAL,                    -- typical length of stay (low)
    typical_los_nights_high REAL,
    top_priority_1          TEXT,
    top_priority_2          TEXT,
    top_priority_3          TEXT,
    revenue_contribution    TEXT,                    -- qualitative: low/medium/high/highest
    seasonal_pattern        TEXT,                    -- peak/shoulder/year-round
    booking_lead_days       INTEGER,                 -- median booking lead time in days
    notes                   TEXT,
    source_file             TEXT,
    loaded_at               TEXT DEFAULT (datetime('now')),
    UNIQUE(report_year, traveler_type)
);

CREATE TABLE IF NOT EXISTS us_travel_national_kpis (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    report_year             INTEGER NOT NULL,
    metric_name             TEXT NOT NULL,
    metric_value            REAL,
    metric_unit             TEXT,                    -- 'billion_usd', 'million_jobs', 'pct', 'index'
    vs_prior_year_pct       REAL,                    -- YOY change %
    vs_2019_pct             REAL,                    -- recovery vs 2019
    notes                   TEXT,
    source_file             TEXT,
    loaded_at               TEXT DEFAULT (datetime('now')),
    UNIQUE(report_year, metric_name)
);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Hardcoded 2024 Benchmarks
# Published: U.S. Travel Association State of Group Travel + Business Travel 2024
# Source: https://www.ustravel.org/business-and-group-travel
# ─────────────────────────────────────────────────────────────────────────────

HARDCODED_GROUP_SEGMENTS_2024 = [
    # (segment, spend_B, jobs, recovery_pct, notes)
    ("meetings_events",    126.0, None, 82.0,
     "Conventions, B2B tradeshows, corporate meetings, incentive travel. "
     "Projected to grow faster than transient business travel through 2025."),
    ("participatory_sports", 52.0, None, None,
     "Youth and amateur sporting events. Often recession-resistant. "
     "Leading driver of hotel room demand in many secondary markets."),
    ("live_spectator",    102.0, None, None,
     "Concerts, festivals, professional sports. Surpassed pre-pandemic levels. "
     "Pent-up demand for live in-person experiences fueled resurgence."),
    ("leisure_group",      39.0, None, None,
     "Motorcoach tours, vacation packages, organized sightseeing, large group experiences. "
     "Vital for small towns and rural communities."),
    ("total_group",        319.0, 3_000_000, None,
     "Total group travel annual economic impact. Supports 3M+ jobs across the U.S. "
     "Spans both business and leisure categories including some international inbound."),
]

HARDCODED_BUSINESS_TRAVEL_2024 = [
    # (category, spend_B, recovery_pct, pct_lodging_rev, pct_air_rev, notes)
    ("total_business",   312.0, 85.0, 60.0, 60.0,
     "Business travelers = ~20% of total travel volume but 60% of lodging and air revenue. "
     "2024 = 85% recovery from 2019 levels."),
    ("transient_business", 186.0, 87.0, None, None,
     "Individual work-related trips. Growth slowed slightly as companies maintain cost constraints."),
    ("meetings_events",  126.0, 82.0, None, None,
     "Conventions, B2B tradeshows, corporate meetings, incentive travel. "
     "Projected to grow faster than transient through 2025."),
]

HARDCODED_TRAVELER_TYPES_2024 = [
    # (type, motivation, bk_wk_low, bk_wk_high, los_low, los_high, p1, p2, p3, revenue, seasonal, lead_days, notes)
    ("solo",          "Self-discovery, work, wellness",     1,  4,  1.0, 3.0,  "Safety",        "Wi-Fi",          "Walkability",       "medium",  "year_round",  14,
     "43% of travelers took a solo trip in 2024 (Skyscanner). Fast booking decisions."),
    ("family",        "Shared holiday, convenience",        8, 24,  3.0, 7.0,  "Room size",     "Kid amenities",  "Location",          "high",    "peak",        45,
     "Book early, stay longer, occupy more rooms. High-value, underestimated segment."),
    ("business",      "Work obligation",                    1,  2,  1.0, 3.0,  "Fast check-in", "Wi-Fi",          "Quiet",             "highest", "year_round",   7,
     "20% of volume, 60% of lodging/air revenue. Often extended into bleisure."),
    ("leisure",       "Rest and enjoyment",                 4, 12,  2.0, 5.0,  "Comfort",       "Experience",     "Value",             "high",    "peak",        30,
     "Price-sensitive but value-driven. Research more, compare more."),
    ("adventure",     "Activity and exploration",           1,  4,  2.0, 4.0,  "Gear storage",  "Early breakfast","Local knowledge",   "medium",  "shoulder",    14,
     "Prioritize experience over luxury. Book closer to travel dates."),
    ("group_smerf",   "Shared purpose, events",             8, 48,  2.0, 5.0,  "Room blocks",   "Meeting space",  "Coordination",      "high",    "shoulder",    90,
     "SMERF = Social, Military, Educational, Religious, Fraternal. Book 2-12 months ahead. "
     "Fill shoulder seasons. Prefer named contact and clear logistics."),
    ("budget",        "Cost efficiency",                    1,  2,  1.0, 4.0,  "Transparency",  "Value",          "Clean basics",      "low",     "year_round",   7,
     "Includes students, retirees. Flexible dates, strict on honesty. Book late at night from mobile."),
    ("luxury",        "Premium experience",                 8, 24,  2.0, 7.0,  "Personalization","Service",       "Consistency",       "highest", "year_round",  60,
     "Price rarely the deciding factor. High expectations for consistency. Virtuoso: comparing value."),
    ("foodie",        "Culinary discovery",                 4, 12,  2.0, 4.0,  "On-site dining","Local food",    "Staff recs",        "medium",  "year_round",  30,
     "Plan trip around restaurants, markets, local specialties. Research before booking."),
    ("pet",           "Travel with animal",                 2,  6,  2.0, 5.0,  "Clear pet policy","Outdoor access","Pet amenities",   "medium",  "year_round",  21,
     "Book based on clear pet policies. Repeat bookings + social media content for pet-friendly properties."),
]

HARDCODED_NATIONAL_KPIS_2024 = [
    # (metric_name, value, unit, vs_prior_pct, vs_2019_pct, notes)
    ("total_travel_spending",         2_400.0, "billion_usd",   None, None,
     "Total US travel spending 2024 estimate (business + leisure)"),
    ("business_travel_spending",        312.0, "billion_usd",   None, 85.0,
     "Business travel 2024. 85% recovery from 2019."),
    ("group_travel_economic_impact",    319.0, "billion_usd",   None, None,
     "Total group travel economic impact 2024 (4 segments combined)"),
    ("group_travel_jobs_supported",       3.0, "million_jobs",  None, None,
     "Jobs supported by group travel across the US"),
    ("business_traveler_lodging_share",  60.0, "pct",          None, None,
     "Business travelers = 20% of volume but 60% of lodging revenue"),
    ("business_traveler_air_share",      60.0, "pct",          None, None,
     "Business travelers = 20% of volume but 60% of air revenue"),
    ("meetings_events_recovery",         82.0, "pct",          None, None,
     "Meetings & business events vs 2019 baseline. Projected to grow faster than transient."),
    ("transient_business_recovery",      87.0, "pct",          None, None,
     "Transient business travel vs 2019. Growth slowed slightly under cost constraints."),
    ("total_business_recovery",          85.0, "pct",          None, None,
     "Total business travel vs 2019 baseline."),
    ("smerf_shoulder_season_fill",      None,  "qualitative",  None, None,
     "SMERF groups often land in shoulder seasons — protects occupancy and ADR when demand is softer."),
]


def seed_2024_benchmarks(conn: sqlite3.Connection) -> int:
    """Upsert hardcoded 2024 U.S. Travel Association benchmarks."""
    cur = conn.cursor()
    count = 0

    for segment, spend, jobs, recovery, notes in HARDCODED_GROUP_SEGMENTS_2024:
        cur.execute("""
            INSERT INTO us_travel_group_segments
                (report_year, segment, spend_billion_usd, jobs_supported, pct_recovery_vs_2019, notes, source_file)
            VALUES (2024, ?, ?, ?, ?, ?, 'ustravel.org/business-and-group-travel')
            ON CONFLICT(report_year, segment) DO UPDATE SET
                spend_billion_usd    = excluded.spend_billion_usd,
                jobs_supported       = excluded.jobs_supported,
                pct_recovery_vs_2019 = excluded.pct_recovery_vs_2019,
                notes                = excluded.notes,
                loaded_at            = datetime('now')
        """, (segment, spend, jobs, recovery, notes))
        count += 1

    for category, spend, recovery, pct_lodg, pct_air, notes in HARDCODED_BUSINESS_TRAVEL_2024:
        cur.execute("""
            INSERT INTO us_travel_business_travel
                (report_year, category, spend_billion_usd, pct_recovery_vs_2019,
                 pct_total_lodging_rev, pct_total_air_rev, notes, source_file)
            VALUES (2024, ?, ?, ?, ?, ?, ?, 'ustravel.org/business-and-group-travel')
            ON CONFLICT(report_year, category) DO UPDATE SET
                spend_billion_usd    = excluded.spend_billion_usd,
                pct_recovery_vs_2019 = excluded.pct_recovery_vs_2019,
                pct_total_lodging_rev= excluded.pct_total_lodging_rev,
                pct_total_air_rev    = excluded.pct_total_air_rev,
                notes                = excluded.notes,
                loaded_at            = datetime('now')
        """, (category, spend, recovery, pct_lodg, pct_air, notes))
        count += 1

    for (ttype, motivation, bk_low, bk_high, los_low, los_high,
         p1, p2, p3, rev, seasonal, lead_days, notes) in HARDCODED_TRAVELER_TYPES_2024:
        cur.execute("""
            INSERT INTO us_travel_traveler_types
                (report_year, traveler_type, primary_motivation,
                 booking_window_weeks_low, booking_window_weeks_high,
                 typical_los_nights_low, typical_los_nights_high,
                 top_priority_1, top_priority_2, top_priority_3,
                 revenue_contribution, seasonal_pattern, booking_lead_days, notes, source_file)
            VALUES (2024, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'lighthouse.com/blog/traveler-types')
            ON CONFLICT(report_year, traveler_type) DO UPDATE SET
                primary_motivation         = excluded.primary_motivation,
                booking_window_weeks_low   = excluded.booking_window_weeks_low,
                booking_window_weeks_high  = excluded.booking_window_weeks_high,
                typical_los_nights_low     = excluded.typical_los_nights_low,
                typical_los_nights_high    = excluded.typical_los_nights_high,
                top_priority_1             = excluded.top_priority_1,
                top_priority_2             = excluded.top_priority_2,
                top_priority_3             = excluded.top_priority_3,
                revenue_contribution       = excluded.revenue_contribution,
                seasonal_pattern           = excluded.seasonal_pattern,
                booking_lead_days          = excluded.booking_lead_days,
                notes                      = excluded.notes,
                loaded_at                  = datetime('now')
        """, (ttype, motivation, bk_low, bk_high, los_low, los_high,
              p1, p2, p3, rev, seasonal, lead_days, notes))
        count += 1

    for metric, value, unit, vs_prior, vs_2019, notes in HARDCODED_NATIONAL_KPIS_2024:
        cur.execute("""
            INSERT INTO us_travel_national_kpis
                (report_year, metric_name, metric_value, metric_unit,
                 vs_prior_year_pct, vs_2019_pct, notes, source_file)
            VALUES (2024, ?, ?, ?, ?, ?, ?, 'ustravel.org/business-and-group-travel')
            ON CONFLICT(report_year, metric_name) DO UPDATE SET
                metric_value      = excluded.metric_value,
                metric_unit       = excluded.metric_unit,
                vs_prior_year_pct = excluded.vs_prior_year_pct,
                vs_2019_pct       = excluded.vs_2019_pct,
                notes             = excluded.notes,
                loaded_at         = datetime('now')
        """, (metric, value, unit, vs_prior, vs_2019, notes))
        count += 1

    conn.commit()
    return count


# ─────────────────────────────────────────────────────────────────────────────
# PDF Text Extraction (pdftotext / poppler-utils)
# ─────────────────────────────────────────────────────────────────────────────

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract all text from a PDF using pdftotext (poppler-utils)."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return result.stdout
        return ""
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# PDF Data Extraction — regex patterns for US Travel report formats
# ─────────────────────────────────────────────────────────────────────────────

def _extract_dollar_billion(text: str, label_pattern: str) -> float | None:
    """Find a dollar amount in billions near a label. Returns float or None."""
    pattern = rf"{label_pattern}[^$\d]*\$?([\d,\.]+)\s*[Bb]illion"
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if m:
        val = m.group(1).replace(",", "")
        try:
            return float(val)
        except ValueError:
            pass
    return None


def _extract_pct(text: str, label_pattern: str) -> float | None:
    """Find a percentage near a label."""
    pattern = rf"{label_pattern}[^%\d]*?([\d\.]+)\s*%"
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _detect_report_year(text: str, filename: str) -> int:
    """Best-guess report year from text and filename."""
    # Look for explicit year mentions near "report", "data", "spending"
    for pattern in [r"(\d{4})\s+(?:Annual|Report|Data|Spending)", r"(?:through|in)\s+(\d{4})"]:
        m = re.search(pattern, text[:2000], re.IGNORECASE)
        if m:
            year = int(m.group(1))
            if 2019 <= year <= 2030:
                return year
    # Fallback: look for year in filename
    m = re.search(r"(20\d{2})", filename)
    if m:
        return int(m.group(1))
    return datetime.now().year


def parse_group_segments_from_text(text: str, report_year: int, source: str) -> list[dict]:
    """Extract group segment spend figures from US Travel report text."""
    rows = []

    segment_patterns = {
        "meetings_events":     r"meetings?\s*(?:&|and)\s*(?:business\s*)?events?",
        "participatory_sports": r"participatory\s+sports?",
        "live_spectator":      r"live\s+spectator",
        "leisure_group":       r"leisure\s+group",
        "total_group":         r"(?:total\s+)?group\s+travel",
    }

    for segment, label_pat in segment_patterns.items():
        spend = _extract_dollar_billion(text, label_pat)
        if spend:
            rows.append({
                "segment": segment,
                "spend_billion_usd": spend,
                "report_year": report_year,
                "source_file": source,
            })

    return rows


def parse_business_travel_from_text(text: str, report_year: int, source: str) -> list[dict]:
    """Extract business travel spend figures from US Travel report text."""
    rows = []

    cat_patterns = {
        "total_business":    r"(?:total\s+)?business\s+travel",
        "transient_business": r"transient\s+(?:business\s+)?travel",
        "meetings_events":   r"meetings?\s*(?:&|and)\s*(?:business\s*)?events?",
    }

    for category, label_pat in cat_patterns.items():
        spend = _extract_dollar_billion(text, label_pat)
        recovery = _extract_pct(text, label_pat + r"[^.]*?recovery")
        if spend:
            rows.append({
                "category": category,
                "spend_billion_usd": spend,
                "pct_recovery_vs_2019": recovery,
                "report_year": report_year,
                "source_file": source,
            })

    return rows


def parse_national_kpis_from_text(text: str, report_year: int, source: str) -> list[dict]:
    """Extract headline national KPI figures."""
    rows = []
    kpi_patterns = [
        ("total_travel_spending",       r"total\s+(?:U\.S\.\s+)?travel\s+(?:spending|economic\s+impact)"),
        ("group_travel_economic_impact", r"group\s+travel[^$\d]*economic\s+impact"),
        ("business_travel_spending",     r"business\s+travel[^$\d]*spending"),
    ]
    for metric_name, label_pat in kpi_patterns:
        val = _extract_dollar_billion(text, label_pat)
        if val:
            rows.append({
                "metric_name": metric_name,
                "metric_value": val,
                "metric_unit": "billion_usd",
                "report_year": report_year,
                "source_file": source,
            })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# PDF Scan and Load
# ─────────────────────────────────────────────────────────────────────────────

def scan_and_load_pdfs(conn: sqlite3.Connection) -> int:
    """Scan data/us_travel/*.pdf and load any extracted data. Returns row count."""
    if not US_TRAVEL_DIR.exists():
        log("scan_pdfs", "SKIP", f"Directory not found: {US_TRAVEL_DIR}")
        return 0

    pdfs = sorted(US_TRAVEL_DIR.glob("*.pdf"))
    if not pdfs:
        log("scan_pdfs", "INFO", "No PDFs found in data/us_travel/ — using hardcoded benchmarks only")
        return 0

    log("scan_pdfs", "INFO", f"Found {len(pdfs)} PDF(s) in data/us_travel/")
    total_rows = 0
    cur = conn.cursor()

    for pdf_path in pdfs:
        log("scan_pdfs", "INFO", f"Parsing {pdf_path.name} …")
        text = extract_pdf_text(pdf_path)
        if not text.strip():
            log("scan_pdfs", "WARN", f"No text extracted from {pdf_path.name} — skipping")
            continue

        report_year = _detect_report_year(text, pdf_path.name)
        source = pdf_path.name

        # Group segments
        for row in parse_group_segments_from_text(text, report_year, source):
            cur.execute("""
                INSERT INTO us_travel_group_segments
                    (report_year, segment, spend_billion_usd, source_file)
                VALUES (:report_year, :segment, :spend_billion_usd, :source_file)
                ON CONFLICT(report_year, segment) DO UPDATE SET
                    spend_billion_usd = excluded.spend_billion_usd,
                    source_file       = excluded.source_file,
                    loaded_at         = datetime('now')
            """, row)
            total_rows += 1

        # Business travel
        for row in parse_business_travel_from_text(text, report_year, source):
            cur.execute("""
                INSERT INTO us_travel_business_travel
                    (report_year, category, spend_billion_usd, pct_recovery_vs_2019, source_file)
                VALUES (:report_year, :category, :spend_billion_usd, :pct_recovery_vs_2019, :source_file)
                ON CONFLICT(report_year, category) DO UPDATE SET
                    spend_billion_usd    = excluded.spend_billion_usd,
                    pct_recovery_vs_2019 = excluded.pct_recovery_vs_2019,
                    source_file          = excluded.source_file,
                    loaded_at            = datetime('now')
            """, row)
            total_rows += 1

        # National KPIs
        for row in parse_national_kpis_from_text(text, report_year, source):
            cur.execute("""
                INSERT INTO us_travel_national_kpis
                    (report_year, metric_name, metric_value, metric_unit, source_file)
                VALUES (:report_year, :metric_name, :metric_value, :metric_unit, :source_file)
                ON CONFLICT(report_year, metric_name) DO UPDATE SET
                    metric_value = excluded.metric_value,
                    metric_unit  = excluded.metric_unit,
                    source_file  = excluded.source_file,
                    loaded_at    = datetime('now')
            """, row)
            total_rows += 1

        log("scan_pdfs", "OK  ", f"{pdf_path.name} — {total_rows} rows extracted")

    conn.commit()
    return total_rows


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=== load_us_travel_reports.py ===\n")
    conn = get_conn()

    try:
        log("schema", "INFO", "Ensuring US Travel tables …")
        ensure_schema(conn)

        log("seed", "INFO", "Seeding 2024 hardcoded benchmarks …")
        seeded = seed_2024_benchmarks(conn)
        log("seed", "OK  ", f"{seeded} benchmark rows upserted")

        log("scan", "INFO", "Scanning data/us_travel/ for PDFs …")
        pdf_rows = scan_and_load_pdfs(conn)
        if pdf_rows:
            log("scan", "OK  ", f"{pdf_rows} additional rows from PDF extraction")

        # Summary
        for tbl in ["us_travel_group_segments", "us_travel_business_travel",
                    "us_travel_traveler_types", "us_travel_national_kpis"]:
            n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log("summary", "INFO", f"  {tbl}: {n} rows")

        # Log to load_log
        total = seeded + pdf_rows
        conn.execute(
            "INSERT INTO load_log (source, grain, file_name, rows_inserted) VALUES (?,?,?,?)",
            ("US_TRAVEL", "annual", "load_us_travel_reports.py", total),
        )
        conn.commit()
        log("done", "OK  ", f"Total rows: {total}")
        print("\nDone.\n")

    except Exception as exc:
        log("error", "FAIL", str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
