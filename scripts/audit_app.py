"""
Dana Point PULSE — Post-Pipeline App Audit
Runs after run_pipeline.py to flag data errors, stale data, and broken insights.
Usage: python scripts/audit_app.py
Output: logs/audit_report.json  +  printed summary
"""

import sqlite3
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT  = Path(__file__).parent.parent
DB    = ROOT / "data" / "analytics.sqlite"
LOGS  = ROOT / "logs"
LOGS.mkdir(exist_ok=True)
OUT   = LOGS / "audit_report.json"

NOW   = datetime.now()
TODAY = NOW.strftime("%Y-%m-%d")

WARN  = "⚠️ "
ERR   = "❌ "
OK    = "✅ "
INFO  = "ℹ️  "

issues   = []
warnings = []
passes   = []


def conn():
    return sqlite3.connect(str(DB))


def flag(level: str, category: str, message: str, detail: str = ""):
    entry = {"level": level, "category": category, "message": message, "detail": detail, "ts": TODAY}
    if level == "error":
        issues.append(entry)
        print(f"  {ERR}[{category}] {message}" + (f" | {detail}" if detail else ""))
    elif level == "warning":
        warnings.append(entry)
        print(f"  {WARN}[{category}] {message}" + (f" | {detail}" if detail else ""))
    else:
        passes.append(entry)
        print(f"  {OK}[{category}] {message}")


# ─── 1. Core table existence + row counts ─────────────────────────────────────
print("\n╔══════════════════════════════════════════════════════╗")
print("║  Dana Point PULSE — App Audit                       ║")
print(f"║  Run: {TODAY}                                    ║")
print("╚══════════════════════════════════════════════════════╝\n")

print("── 1. Table Health ──────────────────────────────────────")
REQUIRED_TABLES = {
    "fact_str_metrics":        500,
    "kpi_daily_summary":       300,
    "kpi_compression_quarterly": 1,
    "insights_daily":          10,
    "datafy_overview_kpis":     1,
    "datafy_overview_dma":      3,
    "vdp_events":               5,
    "costar_annual_performance": 10,
    "costar_snapshot":           1,
    "table_relationships":       50,
    "load_log":                  5,
}
c = conn()
existing = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
for tbl, min_rows in REQUIRED_TABLES.items():
    if tbl not in existing:
        flag("error", "table", f"Missing table: {tbl}")
    else:
        rows = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        if rows < min_rows:
            flag("warning", "table", f"{tbl} has only {rows} rows (expected ≥{min_rows})", f"Run pipeline to populate")
        else:
            flag("ok", "table", f"{tbl}: {rows:,} rows")

# ─── 2. STR data freshness ────────────────────────────────────────────────────
print("\n── 2. STR Data Freshness ────────────────────────────────")
try:
    latest_str = c.execute(
        "SELECT MAX(as_of_date) FROM kpi_daily_summary"
    ).fetchone()[0]
    if latest_str:
        days_old = (NOW - datetime.strptime(latest_str, "%Y-%m-%d")).days
        if days_old > 14:
            flag("error",   "freshness", f"STR daily data is {days_old} days old (last: {latest_str})", "Drop new STR export and run pipeline")
        elif days_old > 7:
            flag("warning", "freshness", f"STR daily data is {days_old} days old (last: {latest_str})", "Consider refreshing")
        else:
            flag("ok", "freshness", f"STR daily data: {latest_str} ({days_old}d ago)")
    else:
        flag("error", "freshness", "kpi_daily_summary is empty — no STR data loaded")
except Exception as e:
    flag("error", "freshness", f"Could not check STR freshness: {e}")

# ─── 3. Insights freshness ────────────────────────────────────────────────────
print("\n── 3. Insights Freshness ────────────────────────────────")
try:
    latest_ins = c.execute("SELECT MAX(as_of_date) FROM insights_daily").fetchone()[0]
    if latest_ins:
        ins_age = (NOW - datetime.strptime(latest_ins, "%Y-%m-%d")).days
        if ins_age > 3:
            flag("warning", "insights", f"Insights are {ins_age} days old (last: {latest_ins})", "Run compute_insights.py")
        else:
            flag("ok", "insights", f"Insights current: {latest_ins} ({ins_age}d ago)")
    else:
        flag("error", "insights", "insights_daily is empty — run compute_insights.py")

    # Check all 5 audiences present
    for aud in ["dmo", "city", "visitor", "resident", "cross"]:
        ct = c.execute(
            "SELECT COUNT(*) FROM insights_daily WHERE audience=? AND as_of_date=?",
            (aud, latest_ins or TODAY)
        ).fetchone()[0]
        if ct == 0:
            flag("warning", "insights", f"No '{aud}' insights for latest date", "Re-run compute_insights.py")
        else:
            flag("ok", "insights", f"Audience '{aud}': {ct} insights")
except Exception as e:
    flag("error", "insights", f"Could not check insights: {e}")

# ─── 4. KPI sanity checks ─────────────────────────────────────────────────────
print("\n── 4. KPI Sanity Checks ─────────────────────────────────")
try:
    row = c.execute(
        "SELECT AVG(occ_pct), AVG(adr), AVG(revpar) FROM kpi_daily_summary "
        "WHERE as_of_date >= date('now','-90 days')"
    ).fetchone()
    avg_occ, avg_adr, avg_rvp = (row[0] or 0), (row[1] or 0), (row[2] or 0)
    if avg_occ < 20 or avg_occ > 100:
        flag("error", "kpi", f"Occupancy out of range: avg {avg_occ:.1f}%", "Check STR import format")
    else:
        flag("ok", "kpi", f"Avg occupancy (90d): {avg_occ:.1f}%")
    if avg_adr < 50 or avg_adr > 2000:
        flag("error", "kpi", f"ADR out of range: avg ${avg_adr:.0f}", "Check STR import format")
    else:
        flag("ok", "kpi", f"Avg ADR (90d): ${avg_adr:.0f}")
    if avg_rvp < 20 or avg_rvp > 1500:
        flag("error", "kpi", f"RevPAR out of range: avg ${avg_rvp:.0f}", "Check STR import format")
    else:
        flag("ok", "kpi", f"Avg RevPAR (90d): ${avg_rvp:.0f}")
except Exception as e:
    flag("error", "kpi", f"KPI sanity check failed: {e}")

# ─── 5. Duplicate detection ───────────────────────────────────────────────────
print("\n── 5. Duplicate Detection ───────────────────────────────")
try:
    dups = c.execute(
        "SELECT as_of_date, COUNT(*) as ct FROM kpi_daily_summary "
        "GROUP BY as_of_date HAVING ct > 1 LIMIT 5"
    ).fetchall()
    if dups:
        flag("warning", "duplicates", f"kpi_daily_summary has {len(dups)} duplicate dates", str(dups[:3]))
    else:
        flag("ok", "duplicates", "kpi_daily_summary: no duplicate dates")
except Exception as e:
    flag("error", "duplicates", f"Duplicate check failed: {e}")

try:
    # Only check for same-date duplicates (multiple dates are expected from repeated pipeline runs)
    ins_dups = c.execute(
        "SELECT audience, category, as_of_date, COUNT(*) as ct FROM insights_daily "
        "GROUP BY audience, category, as_of_date HAVING ct > 1 LIMIT 5"
    ).fetchall()
    if ins_dups:
        flag("warning", "duplicates", f"insights_daily has {len(ins_dups)} same-date duplicate audience/category pairs",
             "UPSERT may not have fired correctly")
    else:
        flag("ok", "duplicates", "insights_daily: no same-date duplicate audience/category pairs")
except Exception as e:
    flag("error", "duplicates", f"Insights duplicate check failed: {e}")

# ─── 6. Event data check ──────────────────────────────────────────────────────
print("\n── 6. Event Data ────────────────────────────────────────")
try:
    evt_ct = c.execute("SELECT COUNT(*) FROM vdp_events").fetchone()[0]
    major_ct = c.execute("SELECT COUNT(*) FROM vdp_events WHERE is_major=1").fetchone()[0]
    if evt_ct < 5:
        flag("warning", "events", f"Only {evt_ct} events in vdp_events", "Run fetch_vdp_events.py to seed")
    else:
        flag("ok", "events", f"vdp_events: {evt_ct} total, {major_ct} major events")
except Exception as e:
    flag("error", "events", f"Event check failed: {e}")

# ─── 7. CoStar data check ─────────────────────────────────────────────────────
print("\n── 7. CoStar Data ───────────────────────────────────────")
try:
    cs_snap = c.execute("SELECT COUNT(*) FROM costar_market_snapshot").fetchone()[0]
    cs_ann  = c.execute("SELECT COUNT(*) FROM costar_annual_performance").fetchone()[0]
    cs_pipe = c.execute("SELECT COUNT(*) FROM costar_supply_pipeline").fetchone()[0]
    if cs_snap < 1:
        flag("warning", "costar", "costar_market_snapshot is empty", "Run load_costar_reports.py")
    else:
        flag("ok", "costar", f"costar_market_snapshot: {cs_snap} rows")
    if cs_ann < 10:
        flag("warning", "costar", f"costar_annual_performance has only {cs_ann} rows", "Re-run CoStar PDF parser")
    else:
        flag("ok", "costar", f"costar_annual_performance: {cs_ann} rows (actuals + forecasts)")
    flag("ok", "costar", f"costar_supply_pipeline: {cs_pipe} rows")
except Exception as e:
    flag("error", "costar", f"CoStar check failed: {e}")

# ─── 8. Stale insight detection ───────────────────────────────────────────────
print("\n── 8. Stale Insight Content ─────────────────────────────")
try:
    # Flag insights referencing dates more than 180 days ago in headline/body
    old_ths = (NOW - timedelta(days=180)).strftime("%Y")
    rows = c.execute(
        "SELECT audience, category, headline FROM insights_daily "
        "WHERE as_of_date = (SELECT MAX(as_of_date) FROM insights_daily)"
    ).fetchall()
    stale_ct = 0
    for aud, cat, headline in rows:
        for yr in ["2023", "2022", "2021"]:
            if yr in (headline or ""):
                flag("warning", "stale_insight", f"[{aud}/{cat}] References old year {yr}", headline[:80])
                stale_ct += 1
                break
    if stale_ct == 0:
        flag("ok", "stale_insight", "No obviously stale year references in insights")
except Exception as e:
    flag("warning", "stale_insight", f"Could not scan for stale insights: {e}")

# ─── 9. Pipeline log check ────────────────────────────────────────────────────
print("\n── 9. Pipeline Log ──────────────────────────────────────")
try:
    last_run = c.execute("SELECT MAX(run_at) FROM load_log").fetchone()[0]
    if last_run:
        run_age = (NOW - datetime.fromisoformat(last_run[:19])).days
        if run_age > 7:
            flag("warning", "pipeline", f"Last pipeline run was {run_age} days ago ({last_run[:10]})", "Run pipeline to refresh data")
        else:
            flag("ok", "pipeline", f"Last pipeline run: {last_run[:10]} ({run_age}d ago)")
    else:
        flag("error", "pipeline", "No pipeline run recorded in load_log")
except Exception as e:
    flag("warning", "pipeline", f"Pipeline log check failed: {e}")

c.close()

# ─── Summary ──────────────────────────────────────────────────────────────────
print("\n══════════════════════════════════════════════════════════")
print(f"  Audit complete: {len(passes)} ✅  {len(warnings)} ⚠️   {len(issues)} ❌")
print("══════════════════════════════════════════════════════════\n")

report = {
    "run_at":   NOW.isoformat(),
    "summary":  {"passes": len(passes), "warnings": len(warnings), "errors": len(issues)},
    "passes":   passes,
    "warnings": warnings,
    "errors":   issues,
}
OUT.write_text(json.dumps(report, indent=2))
print(f"Report saved → {OUT}\n")

if issues:
    sys.exit(1)   # Non-zero exit so CI/cron can detect failures
