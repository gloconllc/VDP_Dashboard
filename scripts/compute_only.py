"""
compute_only.py
---------------
Re-computes all derived tables and refreshes everything the dashboard displays.
Triggered by the "🔄 Run Pipeline" admin button.

Does NOT re-ingest source data — use fetch_external_all.py (Fetch All Sources)
to pull new data from every source first.

Steps:
  1. compute_kpis.py      — rebuild kpi_daily_summary + kpi_compression_quarterly
  2. compute_insights.py  — regenerate insights_daily for all 4 audiences + cross

Run:
    python3 scripts/compute_only.py
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
LOG_PATH     = PROJECT_ROOT / "logs" / "pipeline.log"

STEPS = [
    ("compute_kpis",     "compute_kpis.py",     True),
    ("compute_insights", "compute_insights.py", True),
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    line = f"{_now()} | {step:<22} | {status:<4} | {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_step(step_name: str, script_name: str) -> bool:
    script_path = BASE_DIR / script_name

    if not script_path.exists():
        log(step_name, "FAIL", f"Required script not found: {script_name}")
        return False

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
    except Exception as exc:
        log(step_name, "FAIL", f"subprocess error: {exc}")
        return False

    output_lines = (result.stdout + result.stderr).strip().splitlines()
    summary = " | ".join(ln.strip() for ln in output_lines if ln.strip()) or "(no output)"
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
    log("compute_only", "OK  ", "=== compute pipeline start ===")

    for step_name, script_name, _fatal in STEPS:
        ok = run_step(step_name, script_name)
        if not ok:
            log("compute_only", "FAIL", f"pipeline aborted at step '{step_name}'")
            sys.exit(1)

    log("compute_only", "OK  ", "=== compute pipeline complete — dashboard data refreshed ===")


if __name__ == "__main__":
    main()
