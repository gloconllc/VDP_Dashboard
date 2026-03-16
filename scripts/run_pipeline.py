"""
run_pipeline.py
---------------
Orchestrates the full VDP analytics pipeline in order:

  1. load_str_daily_sqlite.py    — ingest STR daily export into fact_str_metrics
  2. load_str_monthly_sqlite.py  — ingest STR monthly export into fact_str_metrics
  3. compute_kpis.py             — pivot fact_str_metrics into kpi_daily_summary
  4. load_datafy_reports.py      — load Datafy visitor economy data (skip-safe)
  5. load_costar_reports.py      — load CoStar hospitality market data (skip-safe)

Steps 4 and 5 are SKIP-SAFE: if input files are absent, the step logs a warning
and continues (exit code 0). Steps 1–3 are FAIL-FAST: any failure aborts the run.

Each step is logged to logs/pipeline.log as:
  YYYY-MM-DD HH:MM:SS | STEP                 | OK/FAIL | message

Exit code is 0 only if every step succeeds; nonzero on first failure.

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
# fail_fast=True  → pipeline aborts if step fails (core STR/KPI steps)
# fail_fast=False → pipeline warns and continues (optional enrichment steps)
STEPS = [
    ("load_str_daily",    os.path.join(BASE_DIR, "load_str_daily_sqlite.py"),   True),
    ("load_str_monthly",  os.path.join(BASE_DIR, "load_str_monthly_sqlite.py"), True),
    ("compute_kpis",      os.path.join(BASE_DIR, "compute_kpis.py"),            True),
    ("load_datafy",       os.path.join(BASE_DIR, "load_datafy_reports.py"),     False),
    ("load_costar",       os.path.join(BASE_DIR, "load_costar_reports.py"),     False),
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
    """
    if not os.path.exists(script_path):
        log(step_name, "FAIL", f"Script not found: {script_path}")
        return False

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
