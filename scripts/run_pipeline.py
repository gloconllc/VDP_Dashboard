"""
run_pipeline.py
---------------
Orchestrates the full VDP analytics pipeline in order.

STANDARD PROCESS — follow every time new data or logic is added:
  1. Drop raw files into data/<source_name>/  (CSV, Excel, PDF)
  2. Write or update scripts/load_<source>.py to parse → analytics.sqlite table(s)
  3. Add new relationship entries to scripts/build_table_relationships.py
  4. Add the loader to the STEPS list below
  5. Run: python scripts/run_pipeline.py
  6. Step 20 (build_relationships) always runs last — auto-refreshes all relationships
  7. Commit: git add data/analytics.sqlite data/<source>/ scripts/ && git commit

Pipeline steps:
   1. load_str_daily_sqlite.py    — ingest STR daily export     → fact_str_metrics
   2. load_str_monthly_sqlite.py  — ingest STR monthly export   → fact_str_metrics
   3. compute_kpis.py             — pivot STR                   → kpi_daily_summary, kpi_compression_quarterly
   4. load_datafy_reports.py      — Datafy visitor economy CSVs → 17 datafy_* tables (skip-safe)
   5. load_costar_reports.py      — CoStar market data          → 7 costar_* tables (skip-safe)
   6. compute_insights.py         — AI insights engine          → insights_daily (FAIL-FAST)
   7. load_zartico_reports.py     — Zartico historical PDFs     → 8 zartico_* tables (skip-safe)
   8. fetch_vdp_events.py         — VDP event calendar scraper  → vdp_events (skip-safe)
   9. load_visit_ca.py            — Visit California Excel      → 5 visit_ca_* tables incl. travel_indicators (skip-safe)
  9a. load_visit_ca_gmp.py       — Visit CA Global Market Profile PDFs → visit_ca_intl_market_profiles (skip-safe)
  9b. load_visit_ca_lodging.py   — CALodgingPerformance_202601.xls → visit_ca_lodging_monthly (skip-safe)
  10. load_later_reports.py       — Later.com social CSVs       → 14 later_* tables (skip-safe)
  11. fetch_event_analytics.py    — Event impact analysis       → 5 events_* tables (skip-safe)
  12. audit_data.py               — data-quality audit; stdout summary (skip-safe)
  12. fetch_fred_data.py          — FRED macro indicators       → fred_economic_indicators (skip-safe, needs FRED_API_KEY)
  13. fetch_google_trends.py      — Google search demand        → google_trends_weekly (skip-safe)
  14. fetch_weather_data.py       — Open-Meteo coastal weather  → weather_monthly (skip-safe)
  15. fetch_bls_data.py           — BLS OC employment           → bls_employment_monthly (skip-safe)
  16. fetch_eia_gas.py            — EIA CA gas prices           → eia_gas_prices (skip-safe)
  17. fetch_tsa_data.py           — TSA checkpoint throughput   → tsa_checkpoint_daily (skip-safe)
  18. fetch_noaa_marine.py        — NOAA ocean buoy data        → noaa_marine_monthly (skip-safe)
  19. fetch_census_acs.py         — US Census ACS demographics  → census_demographics (skip-safe)
  20. fetch_ticketmaster_events.py — Ticketmaster Discovery API → ticketmaster_events (skip-safe, demo seeds without key)
  21. fetch_wikipedia_pageviews.py — Wikipedia awareness signal → wikipedia_pageviews_daily (skip-safe, no key)
  22. fetch_noaa_tides.py         — NOAA tide predictions       → noaa_tides_daily (skip-safe, no key)
  23. fetch_airnow_aqi.py         — EPA AirNow AQI              → airnow_aqi_daily (skip-safe, demo seeds without key)
  24. load_visit_ca_gmp.py        — Visit California intl market profiles (GMP PDFs) → visit_ca_intl_market_profiles (skip-safe)
  25. load_visit_ca_lodging.py    — CA lodging performance XLS → visit_ca_lodging_monthly (skip-safe)
  26. fetch_airbnb_market.py      — InsideAirbnb STVR listings → airbnb_market_data, airbnb_market_summary (skip-safe)
  27. fetch_beach_water_quality.py — Heal the Bay beach grades → beach_water_quality_weekly (skip-safe)
  28. fetch_whale_watching.py      — Whale watching charter activity index → whale_watching_activity (skip-safe)
  29. fetch_godly_design.py       — Godly.website design inspiration → data/design/godly_inspiration.json (skip-safe, no DB writes)
  30. fetch_nws_weather.py        — NWS weather.gov forecast + observations → weather_forecast, weather_hourly, weather_observations (skip-safe, no key)
  31. build_table_relationships.py — ALWAYS LAST: rebuild ALL table relationships → table_relationships (skip-safe)

# NOTE: All skip-safe steps use try/except and log WARN rather than raising — this prevents
# a single broken data source from crashing the dashboard.

Steps 1, 2, 3, 6 are FAIL-FAST (abort on failure). All others are skip-safe.
Each step is logged to logs/pipeline.log:
  YYYY-MM-DD HH:MM:SS | STEP                 | OK/FAIL | message

Raw data directories:
  data/str/           — STR Excel exports (str_daily.xlsx, str_monthly.xlsx)
  data/datafy/        — Datafy CSV exports (attribution_media/, attribution_website/, social/, overview/)
  data/costar/        — CoStar PDFs + CSVs
  data/Zartico/       — Zartico PDF reports
  data/Visit_California/ — Visit California Excel files
  data/later/         — Later.com CSV exports (IG/, FB/, TikTok/)
  downloads/          — Staging area for new raw files before moving to data/<source>/

Run:
    python3 scripts/run_pipeline.py
"""

import json
import os
import subprocess
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
LOG_PATH      = os.path.join(PROJECT_ROOT, "logs", "pipeline.log")
LOG_JSON_PATH = os.path.join(PROJECT_ROOT, "logs", "pipeline.jsonl")

# Steps: (step_name, script_path, fail_fast)
# fail_fast=True  → pipeline aborts if step fails (core STR/KPI/Insights steps)
# fail_fast=False → pipeline warns and continues (optional enrichment steps)
STEPS = [
    # Step 0: Dropbox sync — downloads latest STR weekly+monthly exports BEFORE loaders run.
    # Skip-safe: if DROPBOX_ACCESS_TOKEN is not set, this step logs a warning and continues.
    ("fetch_str_dropbox", os.path.join(BASE_DIR, "fetch_str_dropbox.py"),       False),
    ("load_str_daily",    os.path.join(BASE_DIR, "load_str_daily_sqlite.py"),   True),
    ("load_str_monthly",  os.path.join(BASE_DIR, "load_str_monthly_sqlite.py"), True),
    # Group/segment multi-tab loader — must run after daily/monthly so fact_str_group_metrics
    # is populated before compute_insights and seed_group_benchmarks read it.
    ("load_str_multiseg",   os.path.join(BASE_DIR, "load_str_multiseg.py"),          False),
    # Response sheet loader — property roster + competitive set membership (6 markets)
    ("load_str_response",   os.path.join(BASE_DIR, "load_str_response_sheets.py"),   False),
    # Translation Table — holiday calendar TY vs LY (explains YOY variance)
    ("load_str_translation", os.path.join(BASE_DIR, "load_str_translation_table.py"), False),
    ("compute_kpis",      os.path.join(BASE_DIR, "compute_kpis.py"),            True),
    ("load_datafy",       os.path.join(BASE_DIR, "load_datafy_reports.py"),     False),
    ("load_costar",           os.path.join(BASE_DIR, "load_costar_reports.py"),     False),
    # U.S. Travel Association national benchmarks (group + business travel + traveler types)
    # Seeds hardcoded 2024 benchmarks + parses any PDFs in data/us_travel/.
    # Download monthly from ustravel.org/state-of-group-travel-report and save to data/us_travel/
    ("load_us_travel",        os.path.join(BASE_DIR, "load_us_travel_reports.py"),  False),
    # Group benchmarks must run before compute_insights so group_intelligence data is available
    ("seed_group_benchmarks", os.path.join(BASE_DIR, "seed_group_benchmarks.py"), False),
    ("compute_insights",      os.path.join(BASE_DIR, "compute_insights.py"),      True),
    ("load_zartico",      os.path.join(BASE_DIR, "load_zartico_reports.py"),    False),
    ("fetch_vdp_events",  os.path.join(BASE_DIR, "fetch_vdp_events.py"),        False),
    ("load_visit_ca",     os.path.join(BASE_DIR, "load_visit_ca.py"),           False),
    ("load_later",        os.path.join(BASE_DIR, "load_later_reports.py"),      False),
    ("fetch_event_analytics", os.path.join(BASE_DIR, "fetch_event_analytics.py"), False),
    ("audit_data",        os.path.join(BASE_DIR, "audit_data.py"),              False),
    # External live data — skip-safe, run last so core pipeline is never blocked
    ("fetch_fred",        os.path.join(BASE_DIR, "fetch_fred_data.py"),         False),
    ("fetch_trends",      os.path.join(BASE_DIR, "fetch_google_trends.py"),     False),
    ("fetch_weather",     os.path.join(BASE_DIR, "fetch_weather_data.py"),      False),
    ("fetch_bls",         os.path.join(BASE_DIR, "fetch_bls_data.py"),          False),
    ("fetch_eia_gas",     os.path.join(BASE_DIR, "fetch_eia_gas.py"),           False),
    ("fetch_tsa",         os.path.join(BASE_DIR, "fetch_tsa_data.py"),          False),
    ("fetch_noaa_marine", os.path.join(BASE_DIR, "fetch_noaa_marine.py"),         False),
    ("fetch_census_acs",  os.path.join(BASE_DIR, "fetch_census_acs.py"),         False),
    # New event + visitor-impact sources (added 2026-05-05)
    # Events are the #1 forward demand signal; awareness, tides, and AQI all
    # directly affect visitor experience and event-day capacity.
    ("fetch_ticketmaster",   os.path.join(BASE_DIR, "fetch_ticketmaster_events.py"), False),
    ("fetch_wiki_pageviews", os.path.join(BASE_DIR, "fetch_wikipedia_pageviews.py"), False),
    ("fetch_noaa_tides",     os.path.join(BASE_DIR, "fetch_noaa_tides.py"),          False),
    ("fetch_airnow_aqi",     os.path.join(BASE_DIR, "fetch_airnow_aqi.py"),          False),
    # Strategy goal progress — refresh current_value for all active goals from live data
    ("strategy_progress", os.path.join(BASE_DIR, "compute_strategy_progress.py"), False),
    # 2026-05-22: New coastal + demand intelligence sources
    # Surf conditions are a key beach-tourism driver (water temp, wave quality)
    ("fetch_surf_conditions", os.path.join(BASE_DIR, "fetch_surf_conditions_daily.py"), False),
    # CA State Parks Doheny/Crystal Cove/San Clemente visitation (day-use invisible in STR)
    ("fetch_ca_state_parks",  os.path.join(BASE_DIR, "fetch_ca_state_parks.py"),       False),
    # Cross-source demand signal index + statistical correlation matrix
    ("fetch_demand_signals",  os.path.join(BASE_DIR, "fetch_demand_signals.py"),        False),
    # Visit California deep-parse: GMP international market profiles + lodging XLS (added 2026-05-22)
    # GMP PDFs cover 13 international origin markets — key for intl visitor intelligence
    ("load_visit_ca_gmp",     os.path.join(BASE_DIR, "load_visit_ca_gmp.py"),     False),
    # CA statewide lodging performance monthly data (XLS format)
    ("load_visit_ca_lodging", os.path.join(BASE_DIR, "load_visit_ca_lodging.py"), False),
    # CA Resident Sentiment on travel & tourism (4-wave annual survey 2022-2025)
    ("load_resident_sentiment", os.path.join(BASE_DIR, "load_resident_sentiment.py"), False),
    # InsideAirbnb STVR market data — competitive landscape for hotel market
    ("fetch_airbnb_market",   os.path.join(BASE_DIR, "fetch_airbnb_market.py"),   False),
    # Beach + marine visitor-experience data (added 2026-05-22)
    ("fetch_beach_water_quality", os.path.join(BASE_DIR, "fetch_beach_water_quality.py"), False),
    # Whale watching activity index — top-5 visitor reason; shoulder-season demand driver
    ("fetch_whale_watching",      os.path.join(BASE_DIR, "fetch_whale_watching.py"),      False),
    # Design inspiration — no DB writes; saves to data/design/godly_inspiration.json
    ("fetch_godly_design", os.path.join(BASE_DIR, "fetch_godly_design.py"),      False),
    # NWS weather.gov — free, no API key; 7-day forecast + 48h hourly + 30-day observations
    ("fetch_nws_weather",  os.path.join(BASE_DIR, "fetch_nws_weather.py"),       False),
    # BTS T-100 Domestic Segment — SoCal airport route passenger volumes for feeder market analysis
    ("fetch_bts_routes",      os.path.join(BASE_DIR, "fetch_bts_routes.py"),      False),
    # InsideAirbnb STVR — Dana Point short-term rental market summary (hotel vs. STVR comparison)
    ("fetch_inside_airbnb",   os.path.join(BASE_DIR, "fetch_inside_airbnb.py"),   False),
    # SoCal gas prices — LA Basin weekly retail price (drive-market demand signal)
    ("fetch_socal_gas",       os.path.join(BASE_DIR, "fetch_socal_gas.py"),       False),
    # ALWAYS LAST — rebuilds all table relationships after every pipeline run
    # Add new relationship entries to build_table_relationships.py when adding new data sources
    ("build_relationships", os.path.join(BASE_DIR, "build_table_relationships.py"), False),
    # POST-PIPELINE AUDIT — runs after all steps; non-fatal, logs to logs/audit_report.json
    ("audit_app",           os.path.join(BASE_DIR, "audit_app.py"),                False),
]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    """Append one human-readable line to pipeline.log and one JSON line to pipeline.jsonl."""
    ts = _now()
    line = f"{ts} | {step:<22} | {status:<4} | {message}"
    print(line)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")
    # Machine-readable JSON Lines format for dashboards and alerting
    record = {"ts": ts, "step": step, "status": status.strip(), "message": message}
    with open(LOG_JSON_PATH, "a") as fh:
        fh.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_step(step_name: str, script_path: str) -> bool:
    """
    Execute `python3 <script_path>` as a subprocess.

    Returns True on success (exit code 0), False on any failure.
    Captured stdout/stderr are included in the log message.
    If the script file does not exist, logs SKIP and returns True (non-blocking).
    """
    if not os.path.exists(script_path):
        log(step_name, "SKIP", f"Script not found (non-fatal): {script_path}")
        return True   # allow pipeline to continue

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
    except Exception as exc:
        log(step_name, "FAIL", f"subprocess error: {exc}")
        return False

    # Condense stdout/stderr into a single one-line summary for the log
    output_lines = (result.stdout + result.stderr).strip().splitlines()
    summary = " | ".join(line.strip() for line in output_lines if line.strip()) or "(no output)"
    if len(summary) > 300:
        summary = summary[:297] + "..."

    if result.returncode == 0:
        log(step_name, "OK  ", summary)
        return True
    else:
        log(step_name, "FAIL", f"exit={result.returncode} | {summary}")
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log("pipeline", "OK  ", "=== pipeline start ===")

    for step_name, script_path, fail_fast in STEPS:
        success = run_step(step_name, script_path)
        if not success:
            if fail_fast:
                log("pipeline", "FAIL", f"pipeline aborted at step '{step_name}'")
                sys.exit(1)
            else:
                log("pipeline", "WARN",
                    f"step '{step_name}' failed — non-critical, continuing")

    log("pipeline", "OK  ", "=== pipeline complete ===")


if __name__ == "__main__":
    main()
