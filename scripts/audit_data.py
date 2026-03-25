"""
audit_data.py
-------------
Daily data-quality audit for the VDP Analytics (Dana Point PULSE) platform.

Checks every tracked SQLite table for:
  - Row count (FAIL if 0 rows)
  - Freshness (WARN if stale beyond threshold)
  - Required columns present (FAIL if missing)
  - NULLs in critical fields (WARN if any found)
  - Recent ETL runs via load_log
  - Insights currency (WARN if insights_daily > 7 days stale)

Outputs:
  logs/audit_YYYY-MM-DD.json  — dated report
  logs/audit_latest.json      — always-current copy
  logs/pending_changes.json   — proposed fixes (if issues found)
  stdout                      — human-readable summary

Flags:
  --check-only  (default) Report only; no scripts are run.
  --apply       Run all proposed fixes from pending_changes.json.

Usage:
    python3 scripts/audit_data.py
    python3 scripts/audit_data.py --check-only
    python3 scripts/audit_data.py --apply
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPTS_DIR  = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
LOGS_DIR     = PROJECT_ROOT / "logs"
AUDIT_DATED  = LOGS_DIR / f"audit_{date.today().isoformat()}.json"
AUDIT_LATEST = LOGS_DIR / "audit_latest.json"
PENDING_FILE = LOGS_DIR / "pending_changes.json"

TODAY = date.today()

# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------
# Each entry: (table_name, required_cols, critical_null_cols, freshness_days, date_col, optional)
#
# freshness_days: warn if max(date_col) is older than this many days
#   30  → daily-updated tables
#   90  → monthly tables
#   180 → slow-moving reference tables
#   None → skip freshness check (no date column)
#
# optional=True  → table may not exist yet; skip instead of FAIL

TABLE_SPECS: list[dict[str, Any]] = [
    {
        "table": "fact_str_metrics",
        "required_cols": ["grain", "as_of_date", "metric_name", "metric_value"],
        "critical_null_cols": ["metric_value"],
        "freshness_days": 30,
        "date_col": "as_of_date",
        "optional": False,
    },
    {
        "table": "kpi_daily_summary",
        "required_cols": ["as_of_date", "occ_pct", "adr", "revpar"],
        "critical_null_cols": ["occ_pct", "adr", "revpar"],
        "freshness_days": 30,
        "date_col": "as_of_date",
        "optional": False,
    },
    {
        "table": "kpi_compression_quarterly",
        "required_cols": ["quarter", "days_above_80_occ"],
        "critical_null_cols": ["days_above_80_occ"],
        "freshness_days": 90,
        "date_col": None,
        "optional": False,
    },
    {
        "table": "load_log",
        "required_cols": ["source", "run_at"],
        "critical_null_cols": [],
        "freshness_days": 30,
        "date_col": "run_at",
        "optional": False,
    },
    {
        "table": "insights_daily",
        "required_cols": ["as_of_date", "audience", "category"],
        "critical_null_cols": ["audience", "category"],
        "freshness_days": 7,
        "date_col": "as_of_date",
        "optional": False,
    },
    {
        "table": "datafy_overview_kpis",
        "required_cols": ["report_period_start", "report_period_end"],
        "critical_null_cols": [],
        "freshness_days": 90,
        "date_col": None,
        "optional": False,
    },
    {
        "table": "datafy_overview_dma",
        "required_cols": ["dma"],
        "critical_null_cols": [],
        "freshness_days": 90,
        "date_col": None,
        "optional": False,
    },
    {
        "table": "datafy_attribution_media_kpis",
        "required_cols": ["report_period_start", "report_period_end"],
        "critical_null_cols": [],
        "freshness_days": 90,
        "date_col": None,
        "optional": False,
    },
    {
        "table": "costar_snapshot",
        "required_cols": [],
        "critical_null_cols": [],
        "freshness_days": 30,
        "date_col": None,
        "optional": True,
    },
    {
        "table": "costar_pipeline",
        "required_cols": [],
        "critical_null_cols": [],
        "freshness_days": 30,
        "date_col": None,
        "optional": True,
    },
    {
        "table": "zartico_kpis",
        "required_cols": [],
        "critical_null_cols": [],
        "freshness_days": 180,
        "date_col": None,
        "optional": True,
    },
    {
        "table": "vdp_events",
        "required_cols": ["event_name", "event_date"],
        "critical_null_cols": [],
        "freshness_days": 90,
        "date_col": "event_date",
        "optional": True,
    },
]

# Scripts that fix specific issues.  Keys are fix_ids referenced in check results.
FIX_SCRIPTS: dict[str, str] = {
    "run_compute_kpis":      str(SCRIPTS_DIR / "compute_kpis.py"),
    "run_compute_insights":  str(SCRIPTS_DIR / "compute_insights.py"),
    "run_load_str_daily":    str(SCRIPTS_DIR / "load_str_daily_sqlite.py"),
    "run_load_str_monthly":  str(SCRIPTS_DIR / "load_str_monthly_sqlite.py"),
    "run_load_datafy":       str(SCRIPTS_DIR / "load_datafy_reports.py"),
    "run_load_costar":       str(SCRIPTS_DIR / "load_costar_reports.py"),
    "run_load_zartico":      str(SCRIPTS_DIR / "load_zartico_reports.py"),
    "run_fetch_vdp_events":  str(SCRIPTS_DIR / "fetch_vdp_events.py"),
    "run_full_pipeline":     str(SCRIPTS_DIR / "run_pipeline.py"),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _connect() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return cur.fetchone() is not None


def _row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _actual_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _max_date(conn: sqlite3.Connection, table: str, date_col: str) -> date | None:
    """Return max value of date_col as a date object, or None on any error."""
    try:
        val = conn.execute(f"SELECT MAX({date_col}) FROM {table}").fetchone()[0]
        if val is None:
            return None
        # SQLite stores dates as text; handle date-only and datetime strings
        if "T" in str(val) or " " in str(val):
            return datetime.fromisoformat(str(val).replace("T", " ").split(".")[0]).date()
        return date.fromisoformat(str(val)[:10])
    except Exception:
        return None


def _null_count(conn: sqlite3.Connection, table: str, col: str) -> int:
    try:
        return conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
        ).fetchone()[0]
    except Exception:
        return 0


def _staleness_label(days_old: int | None) -> str:
    if days_old is None:
        return "unknown"
    return f"{days_old}d ago"


# ---------------------------------------------------------------------------
# Individual checkers
# ---------------------------------------------------------------------------

def check_table(conn: sqlite3.Connection, spec: dict[str, Any]) -> dict[str, Any]:
    """
    Run all checks for one table and return a result dict:
      {table, status, checks: [{name, status, detail, fix_id?}]}
    """
    table    = spec["table"]
    optional = spec.get("optional", False)
    checks: list[dict[str, Any]] = []
    overall  = PASS

    def _add(name: str, status: str, detail: str, fix_id: str | None = None) -> None:
        nonlocal overall
        checks.append({"name": name, "status": status, "detail": detail,
                        **({"fix_id": fix_id} if fix_id else {})})
        if status == FAIL and overall != FAIL:
            overall = FAIL
        elif status == WARN and overall == PASS:
            overall = WARN

    # --- 1. Table existence ---
    if not _table_exists(conn, table):
        if optional:
            return {
                "table": table,
                "status": WARN,
                "optional": True,
                "checks": [{"name": "table_exists", "status": WARN,
                             "detail": "Table does not exist yet (optional — skip-safe)"}],
            }
        else:
            return {
                "table": table,
                "status": FAIL,
                "checks": [{"name": "table_exists", "status": FAIL,
                             "detail": "Table missing from database",
                             "fix_id": "run_full_pipeline"}],
            }

    # --- 2. Row count ---
    try:
        rows = _row_count(conn, table)
        if rows == 0:
            _add("row_count", FAIL, "Table is empty (0 rows)", "run_full_pipeline")
        else:
            _add("row_count", PASS, f"{rows:,} rows")
    except Exception as exc:
        _add("row_count", FAIL, f"Query error: {exc}")

    # --- 3. Required columns ---
    req_cols = spec.get("required_cols", [])
    if req_cols:
        try:
            actual = set(_actual_columns(conn, table))
            missing = [c for c in req_cols if c not in actual]
            if missing:
                _add("required_columns", FAIL,
                     f"Missing columns: {', '.join(missing)}")
            else:
                _add("required_columns", PASS,
                     f"All {len(req_cols)} required columns present")
        except Exception as exc:
            _add("required_columns", FAIL, f"PRAGMA error: {exc}")

    # --- 4. Freshness ---
    freshness_days = spec.get("freshness_days")
    date_col       = spec.get("date_col")
    if freshness_days and date_col:
        try:
            max_dt = _max_date(conn, table, date_col)
            if max_dt is None:
                _add("freshness", WARN,
                     f"Cannot determine latest date in column '{date_col}'")
            else:
                days_old = (TODAY - max_dt).days
                threshold = freshness_days
                label = _staleness_label(days_old)
                if days_old > threshold:
                    fix_hint = _freshness_fix(table)
                    _add("freshness", WARN,
                         f"Last record: {max_dt} ({label}) — stale by >{threshold}d threshold",
                         fix_hint)
                else:
                    _add("freshness", PASS,
                         f"Last record: {max_dt} ({label}) — within {threshold}d threshold")
        except Exception as exc:
            _add("freshness", WARN, f"Freshness check error: {exc}")

    # --- 5. NULLs in critical fields ---
    for col in spec.get("critical_null_cols", []):
        try:
            null_ct = _null_count(conn, table, col)
            if null_ct > 0:
                total = _row_count(conn, table)
                pct   = null_ct / total * 100 if total else 0
                _add(f"nulls_{col}", WARN,
                     f"{null_ct:,} NULLs in '{col}' ({pct:.1f}% of rows)")
            else:
                _add(f"nulls_{col}", PASS, f"No NULLs in '{col}'")
        except Exception as exc:
            _add(f"nulls_{col}", WARN, f"NULL check error for '{col}': {exc}")

    return {"table": table, "status": overall, "checks": checks}


def _freshness_fix(table: str) -> str | None:
    """Map a stale table to the fix script most likely to refresh it."""
    mapping = {
        "fact_str_metrics":          "run_load_str_daily",
        "kpi_daily_summary":         "run_compute_kpis",
        "kpi_compression_quarterly": "run_compute_kpis",
        "insights_daily":            "run_compute_insights",
        "load_log":                  "run_full_pipeline",
        "datafy_overview_kpis":      "run_load_datafy",
        "datafy_overview_dma":       "run_load_datafy",
        "datafy_attribution_media_kpis": "run_load_datafy",
        "costar_snapshot":           "run_load_costar",
        "costar_pipeline":           "run_load_costar",
        "zartico_kpis":              "run_load_zartico",
        "vdp_events":                "run_fetch_vdp_events",
    }
    return mapping.get(table)


# ---------------------------------------------------------------------------
# Specialised load_log checker
# ---------------------------------------------------------------------------

def check_load_log(conn: sqlite3.Connection) -> dict[str, Any]:
    """Extra check: surface last ETL run timestamp per source."""
    checks: list[dict[str, Any]] = []
    try:
        df = pd.read_sql_query(
            "SELECT source, MAX(run_at) AS last_run FROM load_log GROUP BY source ORDER BY last_run DESC",
            conn,
        )
        if df.empty:
            checks.append({"name": "etl_history", "status": WARN,
                            "detail": "No ETL history found in load_log",
                            "fix_id": "run_full_pipeline"})
        else:
            for _, row in df.iterrows():
                try:
                    run_dt = datetime.fromisoformat(
                        str(row["last_run"]).replace("T", " ").split(".")[0]
                    ).date()
                    days_old = (TODAY - run_dt).days
                    label    = _staleness_label(days_old)
                    status   = WARN if days_old > 30 else PASS
                    checks.append({
                        "name": f"etl_{row['source']}",
                        "status": status,
                        "detail": f"Last run: {run_dt} ({label})",
                        **({"fix_id": "run_full_pipeline"} if status == WARN else {}),
                    })
                except Exception:
                    checks.append({"name": f"etl_{row['source']}",
                                   "status": WARN, "detail": "Cannot parse run_at timestamp"})
    except Exception as exc:
        checks.append({"name": "etl_history", "status": FAIL,
                        "detail": f"Query error: {exc}", "fix_id": "run_full_pipeline"})

    overall = FAIL if any(c["status"] == FAIL for c in checks) else \
              (WARN if any(c["status"] == WARN for c in checks) else PASS)
    return {"table": "load_log_detail", "status": overall, "checks": checks}


# ---------------------------------------------------------------------------
# Build pending_changes
# ---------------------------------------------------------------------------

def build_pending_changes(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Collect unique fix_ids from all WARN/FAIL checks and return an ordered
    list of proposed actions.  Deduplicates by fix_id; orders most-impactful first.
    """
    seen: set[str] = set()
    pending: list[dict[str, Any]] = []

    # Collect all fix_ids in priority order (full pipeline last as catch-all)
    priority_order = [
        "run_load_str_daily",
        "run_load_str_monthly",
        "run_compute_kpis",
        "run_load_datafy",
        "run_load_costar",
        "run_load_zartico",
        "run_fetch_vdp_events",
        "run_compute_insights",
        "run_full_pipeline",
    ]
    fix_ids_found: set[str] = set()
    for result in results:
        for check in result.get("checks", []):
            if check["status"] in (WARN, FAIL):
                fid = check.get("fix_id")
                if fid:
                    fix_ids_found.add(fid)

    for fid in priority_order:
        if fid in fix_ids_found and fid not in seen:
            seen.add(fid)
            script = FIX_SCRIPTS.get(fid, "UNKNOWN")
            pending.append({
                "fix_id": fid,
                "script": script,
                "description": _fix_description(fid),
                "proposed_at": _now_str(),
                "status": "pending",
            })

    return pending


def _fix_description(fix_id: str) -> str:
    desc = {
        "run_load_str_daily":    "Reload STR daily data: python3 scripts/load_str_daily_sqlite.py",
        "run_load_str_monthly":  "Reload STR monthly data: python3 scripts/load_str_monthly_sqlite.py",
        "run_compute_kpis":      "Recompute KPIs: python3 scripts/compute_kpis.py",
        "run_load_datafy":       "Reload Datafy visitor economy reports: python3 scripts/load_datafy_reports.py",
        "run_load_costar":       "Reload CoStar market data: python3 scripts/load_costar_reports.py",
        "run_load_zartico":      "Reload Zartico historical reference data: python3 scripts/load_zartico_reports.py",
        "run_fetch_vdp_events":  "Refresh VDP events calendar: python3 scripts/fetch_vdp_events.py",
        "run_compute_insights":  "Regenerate daily insights: python3 scripts/compute_insights.py",
        "run_full_pipeline":     "Run full pipeline: python3 scripts/run_pipeline.py",
    }
    return desc.get(fix_id, fix_id)


# ---------------------------------------------------------------------------
# Apply pending changes
# ---------------------------------------------------------------------------

def apply_pending(pending: list[dict[str, Any]]) -> None:
    """Execute all pending fixes in order, skipping run_full_pipeline if individual steps cover it."""
    if not pending:
        print("  No pending fixes to apply.")
        return

    # If run_full_pipeline is the only or first substantive fix, just run it
    fix_ids = [p["fix_id"] for p in pending]
    if "run_full_pipeline" in fix_ids and len(fix_ids) == 1:
        _run_fix(pending[0])
        return

    for fix in pending:
        if fix["fix_id"] == "run_full_pipeline":
            continue  # skip catch-all when individual scripts are already queued
        _run_fix(fix)


def _run_fix(fix: dict[str, Any]) -> None:
    script = fix["script"]
    print(f"\n  Applying: {fix['description']}")
    if not Path(script).exists():
        print(f"  [SKIP] Script not found: {script}")
        fix["status"] = "skipped"
        return
    result = subprocess.run(
        [sys.executable, script],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    if result.returncode == 0:
        print(f"  [OK] {script}")
        fix["status"] = "applied"
    else:
        out = (result.stdout + result.stderr).strip()[:300]
        print(f"  [FAIL] exit={result.returncode} | {out}")
        fix["status"] = "failed"


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

STATUS_ICON = {PASS: "[PASS]", WARN: "[WARN]", FAIL: "[FAIL]"}


def print_summary(results: list[dict[str, Any]], pending: list[dict[str, Any]]) -> None:
    total  = len(results)
    passes = sum(1 for r in results if r["status"] == PASS)
    warns  = sum(1 for r in results if r["status"] == WARN)
    fails  = sum(1 for r in results if r["status"] == FAIL)

    print()
    print("=" * 65)
    print("  VDP Analytics — Data Quality Audit")
    print(f"  Run: {_now_str()}")
    print("=" * 65)

    for result in results:
        icon = STATUS_ICON[result["status"]]
        optional_tag = "  (optional)" if result.get("optional") else ""
        print(f"\n  {icon} {result['table']}{optional_tag}")
        for check in result.get("checks", []):
            c_icon = STATUS_ICON[check["status"]]
            fix    = f"  → Fix: {_fix_description(check['fix_id'])}" if check.get("fix_id") else ""
            print(f"         {c_icon} {check['name']}: {check['detail']}{fix}")

    print()
    print("-" * 65)
    print(f"  Summary: {total} tables | {passes} PASS | {warns} WARN | {fails} FAIL")

    if pending:
        print(f"\n  Proposed fixes ({len(pending)}):")
        for fix in pending:
            print(f"    - {fix['description']}")
        print(f"\n  Re-run with --apply to execute all fixes automatically.")
    else:
        print("\n  No fixes required.")
    print("=" * 65)
    print()


# ---------------------------------------------------------------------------
# Main audit runner
# ---------------------------------------------------------------------------

def run_audit() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Execute all checks and return (report_dict, pending_list)."""
    if not DB_PATH.exists():
        print(f"[FAIL] Database not found: {DB_PATH}")
        sys.exit(1)

    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    conn    = _connect()
    results = []

    for spec in TABLE_SPECS:
        try:
            result = check_table(conn, spec)
            results.append(result)
        except Exception as exc:
            results.append({
                "table": spec["table"],
                "status": FAIL,
                "checks": [{"name": "audit_error", "status": FAIL,
                             "detail": f"Unhandled audit exception: {exc}"}],
            })

    # Extra load_log detail check
    try:
        results.append(check_load_log(conn))
    except Exception as exc:
        results.append({
            "table": "load_log_detail",
            "status": FAIL,
            "checks": [{"name": "audit_error", "status": FAIL,
                         "detail": f"load_log detail check failed: {exc}"}],
        })

    conn.close()

    pending = build_pending_changes(results)

    report = {
        "generated_at": _now_str(),
        "db_path": str(DB_PATH),
        "summary": {
            "total_checks": len(results),
            "pass": sum(1 for r in results if r["status"] == PASS),
            "warn": sum(1 for r in results if r["status"] == WARN),
            "fail": sum(1 for r in results if r["status"] == FAIL),
            "issues_found": sum(1 for r in results if r["status"] in (WARN, FAIL)),
        },
        "results": results,
        "pending_fixes": pending,
    }

    return report, pending


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="VDP Analytics daily data-quality audit"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check-only",
        action="store_true",
        default=False,
        help="Report only; do not apply any fixes (default behavior)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Automatically apply all pending fixes from pending_changes.json",
    )
    args = parser.parse_args()

    # Run the audit
    report, pending = run_audit()

    # Persist reports
    for path in (AUDIT_DATED, AUDIT_LATEST):
        path.write_text(json.dumps(report, indent=2, default=str))

    # Persist pending changes (even if empty — clears stale list)
    PENDING_FILE.write_text(json.dumps(pending, indent=2, default=str))

    # Print human-readable summary
    print_summary(report["results"], pending)

    print(f"  Audit report saved to: {AUDIT_LATEST}")
    if pending:
        print(f"  Pending fixes saved to: {PENDING_FILE}")
    print()

    # Apply mode: run all pending fixes
    if args.apply:
        if not pending:
            print("  Nothing to apply — all checks clean.")
        else:
            print(f"  Applying {len(pending)} pending fix(es)...\n")
            apply_pending(pending)
            # Update pending file with applied/failed status
            PENDING_FILE.write_text(json.dumps(pending, indent=2, default=str))
            print("\n  Apply complete. Re-run audit to verify results.")

    # Exit with non-zero code if any FAILs exist (useful for CI/cron alerting)
    fail_count = report["summary"]["fail"]
    if fail_count > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()
