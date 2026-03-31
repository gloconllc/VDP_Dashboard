"""
run_pipeline.py
---------------
Orchestrates the full VDP analytics pipeline in order:

  1. load_str_daily_sqlite.py    — ingest STR daily export into fact_str_metrics
  2. load_str_monthly_sqlite.py  — ingest STR monthly export into fact_str_metrics
  3. compute_kpis.py             — pivot fact_str_metrics into kpi_daily_summary
  4. load_datafy_reports.py      — load Datafy visitor economy CSVs (skip-safe)
  5. load_costar_reports.py      — load CoStar hospitality market data (skip-safe)
  6. compute_insights.py         — generate forward-looking insights for all audiences
  7. load_zartico_reports.py     — load Zartico historical reference data (skip-safe)
  8. fetch_vdp_events.py         — scrape VDP event calendar (skip-safe)
  9. load_visit_ca.py            — load Visit California state context data (skip-safe)
 10. load_later_reports.py       — load Later.com social media data (IG/FB/TikTok) (skip-safe)
 11. audit_data.py               — data-quality audit; prints summary to stdout (skip-safe)
 12. fetch_fred_data.py          — FRED economic indicators (hotel CPI, disposable income) (skip-safe, needs FRED_API_KEY)
 13. fetch_google_trends.py      — Google Trends search demand signals (skip-safe)
 14. fetch_weather_data.py       — Open-Meteo coastal weather + beach day score (skip-safe)
 15. fetch_bls_data.py           — BLS OC hospitality employment data (skip-safe)
 16. fetch_eia_gas.py            — EIA California weekly gas prices (drive-market demand signal, skip-safe)
 17. fetch_tsa_data.py           — TSA checkpoint throughput (national air travel demand, skip-safe)

Steps 4–15 are SKIP-SAFE: if input files are absent, API keys are missing, or the
script fails, the step logs a warning and continues (exit code 0). Steps 1, 2, 3, 6
are FAIL-FAST: any failure aborts.

Each step is logged to logs/pipeline.log as:
  YYYY-MM-DD HH:MM:SS | STEP                 | OK/FAIL | message

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
