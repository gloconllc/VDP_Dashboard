"""
ingest_and_deploy.py
====================
Full VDP data ingestion, processing, and deployment pipeline.

What it does:
  1. Scans data/str/    for new XLSX/CSV STR export files  → parse + load to DB
  2. Scans data/datafy/ for new CSV Datafy report files    → parse + load to DB
  3. Scans data/costar/ for new PDF/CSV CoStar files       → parse + load to DB
  4. compute_kpis.py   — rebuild kpi_daily_summary
  5. compute_insights.py — regenerate all forward-looking insights
  6. git add data/analytics.sqlite + commit with datestamp
  7. git push origin main → triggers Streamlit Cloud auto-redeploy

Usage:
    # Full run — ingest all sources, rebuild brain, deploy
    python scripts/ingest_and_deploy.py

    # Dry-run (no git push)
    python scripts/ingest_and_deploy.py --dry-run

    # Skip git operations
    python scripts/ingest_and_deploy.py --no-push

    # Force re-ingest even if no new files detected
    python scripts/ingest_and_deploy.py --force

File placement rules (MUST be followed for auto-detection):
  STR daily exports     → data/str/str_daily*.xlsx  or  data/str/str_daily*.csv
  STR monthly exports   → data/str/str_monthly*.xlsx or data/str/str_monthly*.csv
  Datafy overview CSVs  → data/datafy/overview/*.csv
  Datafy attribution    → data/datafy/attribution_media/*.csv
                          data/datafy/attribution_website/*.csv
  Datafy social/GA4     → data/datafy/social/*.csv
  CoStar PDF reports    → data/costar/*.pdf
  CoStar pre-built CSVs → data/costar/costar_*.csv  (auto-generated on first run)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = Path(__file__).parent          # scripts/
PROJECT_ROOT = BASE_DIR.parent                # project root
LOG_PATH     = PROJECT_ROOT / "logs" / "pipeline.log"

DATA_STR    = PROJECT_ROOT / "data" / "str"
DATA_DATAFY = PROJECT_ROOT / "data" / "datafy"
DATA_COSTAR = PROJECT_ROOT / "data" / "costar"
DB_PATH     = PROJECT_ROOT / "data" / "analytics.sqlite"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    line = f"{_now()} | {step:<26} | {status:<4} | {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


# ---------------------------------------------------------------------------
# File detection helpers
# ---------------------------------------------------------------------------

def _has_new_str_files() -> bool:
    """True if any STR XLSX/CSV files exist in data/str/."""
    if not DATA_STR.exists():
        return False
    return any(
        f.suffix.lower() in (".xlsx", ".xls", ".csv")
        for f in DATA_STR.iterdir()
        if f.is_file() and not f.name.startswith(".")
    )


def _has_new_datafy_files() -> bool:
    """True if any CSV files exist in any data/datafy/ subfolder."""
    if not DATA_DATAFY.exists():
        return False
    for sub in DATA_DATAFY.iterdir():
        if sub.is_dir():
            if any(f.suffix.lower() == ".csv" for f in sub.iterdir() if f.is_file()):
                return True
    return False


def _has_new_costar_files() -> bool:
    """True if any PDF or CSV files exist in data/costar/."""
    if not DATA_COSTAR.exists():
        return False
    return any(
        f.suffix.lower() in (".pdf", ".csv")
        for f in DATA_COSTAR.iterdir()
        if f.is_file() and not f.name.startswith(".")
    )


def _list_source_files() -> dict[str, list[str]]:
    """Return a dict of source category → list of file names found."""
    result: dict[str, list[str]] = {"str": [], "datafy": [], "costar": []}
    if DATA_STR.exists():
        result["str"] = [
            f.name for f in sorted(DATA_STR.iterdir())
            if f.is_file() and f.suffix.lower() in (".xlsx", ".xls", ".csv")
        ]
    if DATA_DATAFY.exists():
        for sub in sorted(DATA_DATAFY.iterdir()):
            if sub.is_dir():
                result["datafy"].extend(
                    f"{sub.name}/{f.name}"
                    for f in sorted(sub.iterdir())
                    if f.is_file() and f.suffix.lower() == ".csv"
                )
    if DATA_COSTAR.exists():
        result["costar"] = [
            f.name for f in sorted(DATA_COSTAR.iterdir())
            if f.is_file() and f.suffix.lower() in (".pdf", ".csv")
        ]
    return result


# ---------------------------------------------------------------------------
# Script runner
# ---------------------------------------------------------------------------

def run_script(step: str, script: Path, fatal: bool = True) -> bool:
    """Execute a Python script as subprocess. Returns True on success."""
    if not script.exists():
        log(step, "SKIP", f"script not found: {script.name}")
        return True  # non-fatal skip

    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
    except Exception as exc:
        log(step, "FAIL", f"subprocess error: {exc}")
        return False

    output_lines = (result.stdout + result.stderr).strip().splitlines()
    summary = " | ".join(l.strip() for l in output_lines if l.strip()) or "(no output)"
    if len(summary) > 400:
        summary = summary[:397] + "..."

    if result.returncode == 0:
        log(step, "OK  ", summary)
        return True
    else:
        log(step, "FAIL", f"exit={result.returncode} | {summary}")
        return False


# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------

def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(PROJECT_ROOT)] + list(args),
        capture_output=True,
        text=True,
        check=check,
    )


def git_commit_and_push(dry_run: bool = False, no_push: bool = False) -> bool:
    """Stage analytics.sqlite, commit with datestamp, push to origin/main."""
    log("git", "INFO", "Staging data/analytics.sqlite …")
    try:
        # Stage the database
        _git("add", "data/analytics.sqlite")

        # Check if anything to commit
        status = _git("status", "--porcelain")
        if not status.stdout.strip():
            log("git", "SKIP", "Nothing to commit — database unchanged since last push")
            return True

        commit_msg = f"Auto-ingest: pipeline run {datetime.now().strftime('%Y-%m-%d %H:%M')} [skip ci]"
        if dry_run:
            log("git", "DRUN", f"DRY RUN — would commit: {commit_msg}")
            return True

        _git("commit", "-m", commit_msg)
        log("git", "OK  ", f"Committed: {commit_msg}")

        if no_push:
            log("git", "SKIP", "Skipping push (--no-push flag)")
            return True

        push_result = _git("push", "origin", "main")
        if push_result.returncode == 0:
            log("git", "OK  ", "Pushed to origin/main → Streamlit Cloud will redeploy automatically")
            return True
        else:
            log("git", "FAIL", f"Push failed: {push_result.stderr.strip()}")
            return False

    except subprocess.CalledProcessError as exc:
        log("git", "FAIL", f"Git error: {exc.stderr.strip() if exc.stderr else str(exc)}")
        return False


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="VDP ingestion + deployment pipeline")
    parser.add_argument("--dry-run",  action="store_true", help="Print what would happen; no writes")
    parser.add_argument("--no-push",  action="store_true", help="Run pipeline but skip git push")
    parser.add_argument("--force",    action="store_true", help="Run all steps even if no new files")
    parser.add_argument("--skip-git", action="store_true", help="Skip all git operations")
    args = parser.parse_args()

    log("ingest_and_deploy", "OK  ", "=== VDP ingestion + deploy start ===")

    # ── Report what files are available ──────────────────────────────────────
    source_files = _list_source_files()
    log("scan", "INFO", f"STR files found: {len(source_files['str'])} — {source_files['str']}")
    log("scan", "INFO", f"Datafy files found: {len(source_files['datafy'])} — {source_files['datafy'][:5]}")
    log("scan", "INFO", f"CoStar files found: {len(source_files['costar'])} — {source_files['costar']}")

    if not args.force:
        if not any([source_files["str"], source_files["datafy"], source_files["costar"]]):
            log("scan", "WARN", "No source files found in data/str/, data/datafy/, or data/costar/. "
                "Drop new files there and re-run, or use --force to re-process existing data.")

    # ── Step 1: STR daily ─────────────────────────────────────────────────────
    ok = run_script("load_str_daily", BASE_DIR / "load_str_daily_sqlite.py", fatal=True)
    if not ok:
        log("ingest_and_deploy", "FAIL", "Aborted at load_str_daily")
        sys.exit(1)

    # ── Step 2: STR monthly ───────────────────────────────────────────────────
    ok = run_script("load_str_monthly", BASE_DIR / "load_str_monthly_sqlite.py", fatal=True)
    if not ok:
        log("ingest_and_deploy", "FAIL", "Aborted at load_str_monthly")
        sys.exit(1)

    # ── Step 3: Datafy ────────────────────────────────────────────────────────
    ok = run_script("load_datafy", BASE_DIR / "load_datafy_reports.py", fatal=False)
    if not ok:
        log("ingest_and_deploy", "WARN", "Datafy load failed — continuing with existing data")

    # ── Step 4: CoStar ────────────────────────────────────────────────────────
    ok = run_script("load_costar", BASE_DIR / "load_costar_reports.py", fatal=False)
    if not ok:
        log("ingest_and_deploy", "WARN", "CoStar load failed — continuing with existing data")

    # ── Step 5: Compute KPIs ──────────────────────────────────────────────────
    ok = run_script("compute_kpis", BASE_DIR / "compute_kpis.py", fatal=True)
    if not ok:
        log("ingest_and_deploy", "FAIL", "Aborted at compute_kpis")
        sys.exit(1)

    # ── Step 6: Compute insights ──────────────────────────────────────────────
    ok = run_script("compute_insights", BASE_DIR / "compute_insights.py", fatal=True)
    if not ok:
        log("ingest_and_deploy", "FAIL", "Aborted at compute_insights")
        sys.exit(1)

    log("ingest_and_deploy", "OK  ", "=== All pipeline steps complete ===")

    # ── Step 7: Git commit + push ─────────────────────────────────────────────
    if args.skip_git:
        log("git", "SKIP", "--skip-git flag set; skipping git operations")
    else:
        git_commit_and_push(dry_run=args.dry_run, no_push=args.no_push)

    log("ingest_and_deploy", "OK  ", "=== ingest_and_deploy complete — Streamlit will redeploy automatically ===")


if __name__ == "__main__":
    main()
