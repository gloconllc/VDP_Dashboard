"""
audit_data_quality.py

Runs data quality checks against analytics.sqlite and prints a PASS/FAIL report.
Saves output to logs/audit_YYYY-MM-DD.txt.

Usage:
    python scripts/audit_data_quality.py
"""

import sqlite3
from datetime import date
from pathlib import Path

BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
LOGS_DIR     = PROJECT_ROOT / "logs"

CHECKS = [
    # (label, sql, expect_zero_rows_to_pass, description)
    (
        "occ decimal range",
        "SELECT COUNT(*) FROM fact_str_metrics WHERE metric_name='occ' AND (metric_value > 1.5 OR metric_value < 0)",
        True,
        "STR occupancy should be stored as decimal 0.0–1.0 (not raw percentage). Values outside this range indicate a loader bug.",
    ),
    (
        "adr range",
        "SELECT COUNT(*) FROM fact_str_metrics WHERE metric_name='adr' AND (metric_value > 2000 OR metric_value < 50) AND metric_value IS NOT NULL",
        True,
        "ADR should be $50–$2,000. Out-of-range values suggest unit mismatch or data corruption.",
    ),
    (
        "revpar range",
        "SELECT COUNT(*) FROM fact_str_metrics WHERE metric_name='revpar' AND (metric_value > 1500 OR metric_value < 20) AND metric_value IS NOT NULL",
        True,
        "RevPAR should be $20–$1,500. Out-of-range values suggest unit mismatch or data corruption.",
    ),
    (
        "kpi occ_pct range",
        "SELECT COUNT(*) FROM kpi_daily_summary WHERE occ_pct > 100 OR occ_pct < 0",
        True,
        "kpi_daily_summary.occ_pct should be 0–100 (percentage form). Violations indicate a compute_kpis.py bug.",
    ),
    (
        "group occ range",
        """SELECT COUNT(*) FROM fact_str_group_metrics
           WHERE metric_name IN ('Occupancy (%)', 'occ_pct')
           AND metric_value > 1.5 AND metric_value IS NOT NULL""",
        True,
        "Group segment occupancy should be stored as decimal 0.0–1.0. Values > 1.5 are room-night counts stored in the wrong field.",
    ),
    (
        "stvr occ range",
        "SELECT COUNT(*) FROM stvr_market_summary WHERE occupancy_pct > 100 OR occupancy_pct < 0",
        True,
        "STVR occupancy_pct should be 0–100.",
    ),
    (
        "bts passengers",
        "SELECT COUNT(*) FROM bts_route_passengers WHERE passengers < 0",
        True,
        "BTS passenger counts should be non-negative.",
    ),
    (
        "insights count",
        "SELECT COUNT(*) FROM insights_daily WHERE body IS NULL OR body = ''",
        True,
        "All insights_daily rows must have a non-empty body.",
    ),
    (
        "insight duplicates",
        """SELECT COUNT(*) FROM (
               SELECT as_of_date, audience, category, COUNT(*) AS cnt
               FROM insights_daily
               GROUP BY as_of_date, audience, category
               HAVING cnt > 1
           )""",
        True,
        "insights_daily unique key is (as_of_date, audience, category). Duplicates indicate a broken UPSERT.",
    ),
    (
        "adr group range",
        """SELECT COUNT(*) FROM fact_str_group_metrics
           WHERE metric_name IN ('ADR', 'adr')
           AND metric_value IS NOT NULL
           AND (metric_value > 2000 OR metric_value < 0)""",
        True,
        "Group ADR should be $0–$2,000. Large values (millions) indicate revenue totals stored in the ADR field.",
    ),
    (
        "kpi daily rows",
        "SELECT COUNT(*) FROM kpi_daily_summary",
        False,  # expect > 0
        "kpi_daily_summary must have rows. Zero rows means compute_kpis.py has not been run.",
    ),
    (
        "insights rows",
        "SELECT COUNT(*) FROM insights_daily",
        False,  # expect > 0
        "insights_daily must have rows. Zero rows means compute_insights.py has not been run.",
    ),
]


def run_audit() -> list[dict]:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    results = []
    for label, sql, expect_zero, description in CHECKS:
        # Skip checks on tables that don't exist
        table_name = None
        for token in sql.split():
            if "." not in token and token not in ("SELECT", "FROM", "WHERE", "COUNT(*)", "AND", "OR", "IS", "NOT", "NULL", "(", ")", "GROUP", "BY", "HAVING", "cnt", ">", "<", "1"):
                if conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (token,)
                ).fetchone():
                    table_name = token
                    break

        try:
            count = conn.execute(sql).fetchone()[0]
        except Exception as exc:
            results.append({
                "label": label,
                "status": "SKIP",
                "count": None,
                "description": description,
                "error": str(exc),
            })
            continue

        if expect_zero:
            passed = count == 0
        else:
            passed = count > 0

        results.append({
            "label": label,
            "status": "PASS" if passed else "FAIL",
            "count": count,
            "description": description,
            "error": None,
        })

    conn.close()
    return results


def format_report(results: list[dict]) -> str:
    lines = [
        f"Data Quality Audit — {date.today().isoformat()}",
        f"Database: {DB_PATH}",
        "=" * 60,
        "",
    ]
    fail_count  = sum(1 for r in results if r["status"] == "FAIL")
    pass_count  = sum(1 for r in results if r["status"] == "PASS")
    skip_count  = sum(1 for r in results if r["status"] == "SKIP")

    for r in results:
        icon = {"PASS": "✓", "FAIL": "✗", "SKIP": "—"}.get(r["status"], "?")
        count_str = f" ({r['count']:,} bad rows)" if r["status"] == "FAIL" and r["count"] is not None else ""
        lines.append(f"[{r['status']}] {icon}  {r['label']}{count_str}")
        if r["status"] == "FAIL":
            lines.append(f"       → {r['description']}")
        if r["error"]:
            lines.append(f"       ERROR: {r['error']}")

    lines += [
        "",
        "=" * 60,
        f"Summary: {pass_count} PASS  |  {fail_count} FAIL  |  {skip_count} SKIP",
        "",
    ]
    if fail_count == 0:
        lines.append("All checks passed. Data quality looks good.")
    else:
        lines.append(f"{fail_count} check(s) failed. Review the items above and re-run the pipeline.")

    return "\n".join(lines)


def main() -> None:
    LOGS_DIR.mkdir(exist_ok=True)
    results  = run_audit()
    report   = format_report(results)
    log_path = LOGS_DIR / f"audit_{date.today().isoformat()}.txt"
    log_path.write_text(report)
    print(report)
    print(f"\nReport saved to {log_path}")


if __name__ == "__main__":
    main()
