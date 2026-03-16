"""
run_pipeline.py
---------------
Orchestrates the full VDP analytics pipeline in order:

  1. load_str_daily_sqlite.py    — scan data/str/ for new daily xlsx → fact_str_metrics
  2. load_str_monthly_sqlite.py  — scan data/str/ for new monthly xlsx → fact_str_metrics
  3. compute_kpis.py             — pivot fact_str_metrics → kpi_daily_summary
  4. load_datafy_reports.py      — scan downloads/ for new Datafy files (skip-safe)
  5. load_costar_reports.py      — scan data/costar/ for new PDFs/xlsx (skip-safe)
  6. git_auto_push               — commit data/analytics.sqlite + csvs → push to remote

Steps 4–5 are SKIP-SAFE: absent files log a warning and continue.
Step 6 only commits+pushes if the pipeline produced new data (rows_inserted > 0).

Each step logged to logs/pipeline.log:
  YYYY-MM-DD HH:MM:SS | STEP                  | OK/FAIL | message

Run:
    python scripts/run_pipeline.py
    python scripts/run_pipeline.py --no-push   # skip git push
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
STEPS = [
    ("load_str_daily",   os.path.join(BASE_DIR, "load_str_daily_sqlite.py"),   True),
    ("load_str_monthly", os.path.join(BASE_DIR, "load_str_monthly_sqlite.py"), True),
    ("compute_kpis",     os.path.join(BASE_DIR, "compute_kpis.py"),            True),
    ("load_datafy",      os.path.join(BASE_DIR, "load_datafy_reports.py"),     False),
    ("load_costar",      os.path.join(BASE_DIR, "load_costar_reports.py"),     False),
]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    line = f"{_now()} | {step:<22} | {status:<4} | {message}"
    print(line)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


# ---------------------------------------------------------------------------
# Step runner
# ---------------------------------------------------------------------------

def run_step(step_name: str, script_path: str) -> tuple[bool, str]:
    """
    Run `python <script_path>` as a subprocess.
    Returns (success: bool, output: str).
    """
    if not os.path.exists(script_path):
        log(step_name, "FAIL", f"Script not found: {script_path}")
        return False, ""

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
    except Exception as exc:
        log(step_name, "FAIL", f"subprocess error: {exc}")
        return False, ""

    output_lines = (result.stdout + result.stderr).strip().splitlines()
    summary = " | ".join(l.strip() for l in output_lines if l.strip()) or "(no output)"
    if len(summary) > 300:
        summary = summary[:297] + "..."

    if result.returncode == 0:
        log(step_name, "OK  ", summary)
        return True, result.stdout + result.stderr
    else:
        log(step_name, "FAIL", f"exit={result.returncode} | {summary}")
        return False, result.stdout + result.stderr


# ---------------------------------------------------------------------------
# Git auto-push
# ---------------------------------------------------------------------------

def _rows_inserted(output: str) -> int:
    """Parse total rows inserted from combined step output."""
    import re
    total = 0
    for m in re.finditer(r"(\d+)\s+(?:new\s+)?rows?\s+(?:inserted|across)", output, re.IGNORECASE):
        try:
            total += int(m.group(1))
        except ValueError:
            pass
    return total


def git_auto_push(all_output: str, no_push: bool = False) -> None:
    """
    Stage data/analytics.sqlite + data/costar CSVs + data/str, commit with
    a timestamped message, and push to the current remote branch.
    Runs only when the pipeline produced new rows.
    """
    step = "git_auto_push"

    if no_push:
        log(step, "SKIP", "--no-push flag set — skipping git commit+push")
        return

    # Check for any changes to commit
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    changed = result.stdout.strip()
    if not changed:
        log(step, "SKIP", "No changes to commit — database and CSVs unchanged")
        return

    log(step, "OK  ", f"Changes detected:\n{changed}")

    # Stage data files only (never commit .env or venv)
    stage_paths = [
        os.path.join(PROJECT_ROOT, "data", "analytics.sqlite"),
        os.path.join(PROJECT_ROOT, "data", "costar"),
        os.path.join(PROJECT_ROOT, "data", "str"),
        os.path.join(PROJECT_ROOT, "logs", "pipeline.log"),
    ]
    for path in stage_paths:
        if os.path.exists(path):
            subprocess.run(["git", "add", path], cwd=PROJECT_ROOT, capture_output=True)

    # Commit
    msg = (
        f"Pipeline auto-update {_now()}\n\n"
        f"Data refreshed via run_pipeline.py\n\n"
        f"https://claude.ai/code/session_018PCe4ynoUBhAJY9ZQbTyNz"
    )
    commit_result = subprocess.run(
        ["git", "commit", "-m", msg],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if commit_result.returncode != 0:
        out = (commit_result.stdout + commit_result.stderr).strip()
        if "nothing to commit" in out:
            log(step, "SKIP", "Nothing new to commit after staging")
            return
        log(step, "FAIL", f"git commit failed: {out[:200]}")
        return

    log(step, "OK  ", "Committed data changes")

    # Get current branch
    branch_result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    branch = branch_result.stdout.strip() or "main"

    # Push with retry (up to 4 attempts, exponential backoff)
    import time
    for attempt, wait in enumerate([0, 2, 4, 8], start=1):
        if wait:
            log(step, "WARN", f"Push attempt {attempt} — waiting {wait}s after failure")
            time.sleep(wait)
        push_result = subprocess.run(
            ["git", "push", "-u", "origin", branch],
            capture_output=True, text=True, cwd=PROJECT_ROOT,
        )
        if push_result.returncode == 0:
            log(step, "OK  ", f"Pushed to origin/{branch} — Streamlit will auto-redeploy")
            return
        err = (push_result.stdout + push_result.stderr).strip()
        log(step, "WARN" if attempt < 4 else "FAIL",
            f"Push attempt {attempt} failed: {err[:150]}")

    log(step, "FAIL", "All push attempts failed — check network and git credentials")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    no_push = "--no-push" in sys.argv

    log("pipeline", "OK  ", "=== pipeline start ===")

    all_output   = ""
    rows_new     = 0

    for step_name, script_path, fail_fast in STEPS:
        success, output = run_step(step_name, script_path)
        all_output += output
        rows_new   += _rows_inserted(output)

        if not success:
            if fail_fast:
                log("pipeline", "FAIL", f"Aborted at '{step_name}'")
                sys.exit(1)
            else:
                log("pipeline", "WARN", f"'{step_name}' failed — non-critical, continuing")

    log("pipeline", "OK  ", f"=== pipeline complete | {rows_new} new rows total ===")

    # Auto-commit + push if we have anything new
    git_auto_push(all_output, no_push=no_push)


if __name__ == "__main__":
    main()
