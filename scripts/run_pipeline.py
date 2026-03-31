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
   9. load_visit_ca.py            — Visit California Excel      → 4 visit_ca_* tables (skip-safe)
  10. load_later_reports.py       — Later.com social CSVs       → 14 later_* tables (skip-safe)
  11. audit_data.py               — data-quality audit; stdout summary (skip-safe)
  12. fetch_fred_data.py          — FRED macro indicators       → fred_economic_indicators (skip-safe, needs FRED_API_KEY)
  13. fetch_google_trends.py      — Google search demand        → google_trends_weekly (skip-safe)
  14. fetch_weather_data.py       — Open-Meteo coastal weather  → weather_monthly (skip-safe)
  15. fetch_bls_data.py           — BLS OC employment           → bls_employment_monthly (skip-safe)
  16. fetch_eia_gas.py            — EIA CA gas prices           → eia_gas_prices (skip-safe)
  17. fetch_tsa_data.py           — TSA checkpoint throughput   → tsa_checkpoint_daily (skip-safe)
  18. fetch_noaa_marine.py        — NOAA ocean buoy data        → noaa_marine_monthly (skip-safe)
  19. fetch_census_acs.py         — US Census ACS demographics  → census_demographics (skip-safe)
  20. build_table_relationships.py — ALWAYS LAST: rebuild ALL table relationships → table_relationships (skip-safe)

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

import os
import subprocess
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
LOG_PATH     = os.path.join(PROJECT_ROOT, "logs", "pipeline.log")

# Steps: (step_name, script_path, fail_fast)
# fail_fast=True  → pipeline aborts if step fails (core STR/KPI/Insights steps)
# fail_fast=False → pipeline warns and continues (optional enrichment steps)
STEPS = [
    ("load_str_daily",    os.path.join(BASE_DIR, "load_str_daily_sqlite.py"),   True),
    ("load_str_monthly",  os.path.join(BASE_DIR, "load_str_monthly_sqlite.py"), True),
    ("compute_kpis",      os.path.join(BASE_DIR, "compute_kpis.py"),            True),
    ("load_datafy",       os.path.join(BASE_DIR, "load_datafy_reports.py"),     False),
    ("load_costar",       os.path.join(BASE_DIR, "load_costar_reports.py"),     False),
    ("compute_insights",  os.path.join(BASE_DIR, "compute_insights.py"),        True),
    ("load_zartico",      os.path.join(BASE_DIR, "load_zartico_reports.py"),    False),
    ("fetch_vdp_events",  os.path.join(BASE_DIR, "fetch_vdp_events.py"),        False),
    ("load_visit_ca",     os.path.join(BASE_DIR, "load_visit_ca.py"),           False),
    ("load_later",        os.path.join(BASE_DIR, "load_later_reports.py"),      False),
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
    # ALWAYS LAST — rebuilds all table relationships after every pipeline run
    # Add new relationship entries to build_table_relationships.py when adding new data sources
    ("build_relationships", os.path.join(BASE_DIR, "build_table_relationships.py"), False),
]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    """Append one line to logs/pipeline.log and echo it to stdout."""
    line = f"{_now()} | {step:<22} | {status:<4} | {message}"
    print(line)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


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
