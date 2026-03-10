"""
fetch_external_all.py
----------------------
Orchestrates all external / Layer-2 data fetches in order:

  1. fetch_costar_data.py   — CoStar Excel exports from downloads/
  2. fetch_fred_data.py     — FRED hotel pricing index  (runs if file exists)
  3. fetch_ca_tot.py        — CA State TOT data         (runs if file exists)
  4. fetch_jwa_stats.py     — JWA passenger counts      (runs if file exists)

Per CLAUDE.md data hierarchy:
  Layer 1 (STR / Datafy) = Truth — run_pipeline.py handles that
  Layer 2 (FRED / CA TOT / JWA / CoStar) = Context — this script handles that

Each step is logged to logs/pipeline.log.
Fail-fast: first FAIL aborts the run (WARN is non-fatal).

Run:
    python3 scripts/fetch_external_all.py
"""

import os
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

# Steps: (log_name, script_path, fatal_on_missing)
STEPS = [
    ("fetch_costar",  BASE_DIR / "fetch_costar_data.py",   True),
    ("fetch_fred",    BASE_DIR / "fetch_fred_data.py",      False),
    ("fetch_ca_tot",  BASE_DIR / "fetch_ca_tot.py",         False),
    ("fetch_jwa",     BASE_DIR / "fetch_jwa_stats.py",      False),
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(step: str, status: str, message: str) -> None:
    line = f"{_now()} | {step:<20} | {status:<4} | {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_step(step_name: str, script_path: Path, fatal_on_missing: bool) -> bool:
    """
    Execute the script as a subprocess using the current Python interpreter.
    Returns True on success or non-fatal skip. Returns False on hard failure.
    """
    if not script_path.exists():
        if fatal_on_missing:
            log(step_name, "FAIL", f"Script not found: {script_path}")
            return False
        else:
            log(step_name, "SKIP", f"Script not yet implemented: {script_path.name}")
            return True   # non-fatal skip

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
    log("fetch_external", "OK  ", "=== external fetch start ===")

    for step_name, script_path, fatal_on_missing in STEPS:
        ok = run_step(step_name, script_path, fatal_on_missing)
        if not ok:
            log("fetch_external", "FAIL",
                f"external fetch aborted at step '{step_name}'")
            sys.exit(1)

    log("fetch_external", "OK  ", "=== external fetch complete ===")


if __name__ == "__main__":
    main()
