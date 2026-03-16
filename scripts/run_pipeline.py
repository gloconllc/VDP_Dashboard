"""
run_pipeline.py
---------------
Orchestrates the full analytics pipeline in order:

  1. load_str_daily_sqlite.py   — ingest STR daily export into fact_str_metrics
  2. load_str_monthly_sqlite.py — ingest STR monthly export into fact_str_metrics
  3. load_datafy_reports.py     — ingest Datafy visitor economy data (non-fatal if missing)
  4. compute_kpis.py            — pivot fact_str_metrics into kpi_daily_summary
  5. compute_insights.py        — generate forward-looking insights for all 4 audiences

Each step is logged to logs/pipeline.log as:
  YYYY-MM-DD HH:MM:SS | STEP | OK/FAIL | message

FATAL steps (1, 2, 4, 5): abort pipeline on failure.
NON-FATAL steps (3): log failure and continue.

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

# (step_name, script_path, fatal_on_failure)
STEPS = [
    ("load_str_daily",    os.path.join(BASE_DIR, "load_str_daily_sqlite.py"),  True),
    ("load_str_monthly",  os.path.join(BASE_DIR, "load_str_monthly_sqlite.py"), True),
    ("load_datafy",       os.path.join(BASE_DIR, "load_datafy_reports.py"),    False),
    ("compute_kpis",      os.path.join(BASE_DIR, "compute_kpis.py"),           True),
    ("compute_insights",  os.path.join(BASE_DIR, "compute_insights.py"),       True),
]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    """Append one line to logs/pipeline.log and echo it to stdout."""
    line = f"{_now()} | {step:<20} | {status:<4} | {message}"
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
    # Truncate so the log line stays readable
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

    for step_name, script_path, fatal in STEPS:
        success = run_step(step_name, script_path)
        if not success:
            if fatal:
                log("pipeline", "FAIL", f"pipeline aborted at step '{step_name}'")
                sys.exit(1)
            else:
                log("pipeline", "WARN", f"non-fatal failure at '{step_name}' — continuing")

    log("pipeline", "OK  ", "=== pipeline complete ===")


if __name__ == "__main__":
    main()
