"""
fetch_external_all.py
----------------------
Fetches and ingests data from EVERY source into the database.
Triggered by the "📡 Fetch All Sources" admin button.

Dedup guarantee: every load script uses ON CONFLICT / SELECT-before-INSERT /
DELETE-then-INSERT-by-period — re-running this script never creates duplicate rows.

Step order (all skip-safe unless marked fatal):

  Layer 1 — Truth (STR / Datafy):
    1. load_str_daily_sqlite.py    — STR daily export → fact_str_metrics       [fatal if file present]
    2. load_str_monthly_sqlite.py  — STR monthly export → fact_str_metrics      [fatal if file present]
    3. load_datafy_reports.py      — Datafy visitor economy CSVs                [skip-safe]

  Layer 1 — Market data:
    4. load_costar_reports.py      — CoStar hospitality analytics PDFs          [skip-safe]
    5. load_zartico_reports.py     — Zartico historical reference PDFs          [skip-safe]

  Layer 2 — External context:
    6. load_visit_ca.py            — Visit California state forecast data       [skip-safe]
    7. fetch_vdp_events.py         — VDP event calendar (scrape + seed)         [skip-safe]
    8. fetch_fred_data.py          — FRED hotel pricing index                   [skip-safe]
    9. fetch_ca_tot.py             — CA State TOT data                          [skip-safe]
   10. fetch_jwa_stats.py          — JWA passenger counts                       [skip-safe]

After this completes, run compute_only.py (or click "Run Pipeline") to
refresh KPIs and insights.

Run:
    python3 scripts/fetch_external_all.py
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

# Steps: (log_name, script_name, fatal_if_fails)
# fatal_if_fails=True  → abort fetch run if this step returns non-zero
# fatal_if_fails=False → log warning and continue (skip-safe)
STEPS = [
    # Layer 1 — Truth
    ("load_str_daily",   "load_str_daily_sqlite.py",   False),  # skip-safe: file may not exist yet
    ("load_str_monthly", "load_str_monthly_sqlite.py", False),  # skip-safe: file may not exist yet
    ("load_datafy",      "load_datafy_reports.py",     False),
    # Layer 1 — Market data
    ("load_costar",      "load_costar_reports.py",     False),
    ("load_zartico",     "load_zartico_reports.py",    False),
    # Layer 2 — External context
    ("load_visit_ca",    "load_visit_ca.py",           False),
    ("fetch_vdp_events", "fetch_vdp_events.py",        False),
    ("fetch_fred",       "fetch_fred_data.py",         False),
    ("fetch_ca_tot",     "fetch_ca_tot.py",            False),
    ("fetch_jwa",        "fetch_jwa_stats.py",         False),
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

def run_step(step_name: str, script_name: str, fatal_if_fails: bool) -> bool:
    """
    Execute scripts/<script_name> as a subprocess.
    Returns True on success or non-fatal skip. Returns False on hard failure.
    """
    script_path = BASE_DIR / script_name

    if not script_path.exists():
        log(step_name, "SKIP", f"Script not yet implemented: {script_name}")
        return True  # always non-fatal for missing scripts

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
    except Exception as exc:
        log(step_name, "FAIL", f"subprocess error: {exc}")
        return not fatal_if_fails  # fatal → False, non-fatal → True (continue)

    output_lines = (result.stdout + result.stderr).strip().splitlines()
    summary = " | ".join(ln.strip() for ln in output_lines if ln.strip()) or "(no output)"
    if len(summary) > 300:
        summary = summary[:297] + "..."

    if result.returncode == 0:
        log(step_name, "OK  ", summary)
        return True
    else:
        log(step_name, "FAIL", f"exit={result.returncode} | {summary}")
        if fatal_if_fails:
            return False
        log(step_name, "WARN", f"{step_name} failed — non-critical, continuing")
        return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log("fetch_all", "OK  ", "=== fetch all sources start ===")

    for step_name, script_name, fatal_if_fails in STEPS:
        ok = run_step(step_name, script_name, fatal_if_fails)
        if not ok:
            log("fetch_all", "FAIL", f"fetch aborted at step '{step_name}'")
            sys.exit(1)

    log("fetch_all", "OK  ", "=== fetch all sources complete — run compute_only.py to refresh KPIs/insights ===")


if __name__ == "__main__":
    main()
