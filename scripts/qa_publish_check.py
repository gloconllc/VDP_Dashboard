# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created by John Picou, GloCon Solutions LLC, for Visit Dana Point.
#  Use, copying, sharing, or redistribution requires written permission.
#  Contact GloCon Solutions LLC (John Picou), https://gloconsolutions.com.
# =============================================================================
"""
qa_publish_check.py, the publish gate (last pipeline step, fatal).

Before any refresh of analytics.sqlite is committed and pushed (which is what
publishes the live PULSE site), this step rebuilds what readers will see and
checks it the way John reads it:

  1. No zero placeholders left in measures where zero is impossible
     (dashboard/quality_gate.py ZERO_IS_MISSING).
  2. Today's insights_daily rows: any headline or body carrying "n/a",
     "not available", "$0", or an unsigned "0%" is removed (insights are
     regenerated on every run, so nothing real is lost).
  3. The Insights report, rebuilt for Year to date and Last 12 months, after
     the quality gate: zero residual issues allowed. "Of every 100 trips: 0%
     and 0%" is exactly what this blocks.
  4. Board cards: checked and reported (warnings).

Exit code 1 (pipeline stops, do not commit or push) if check 1 or 3 fails.
Writes logs/qa_publish_report.json.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import warnings
from datetime import date, datetime

warnings.filterwarnings("ignore")
os.environ.setdefault("STREAMLIT_LOG_LEVEL", "error")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "dashboard"))
import quality_gate as qg  # noqa: E402

DB = os.environ.get("PULSE_DB") or os.path.join(ROOT, "data", "analytics.sqlite")


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] qa_publish: {msg}")


def check_zero_placeholders(conn) -> list[dict]:
    return qg.normalize_missing(conn, dry_run=True)


def clean_insights(conn) -> list[str]:
    try:
        last = conn.execute("SELECT MAX(as_of_date) FROM insights_daily").fetchone()[0]
    except Exception:  # noqa: BLE001
        return []
    if not last:
        return []
    rows = conn.execute("SELECT rowid, audience, category, headline, body FROM insights_daily WHERE as_of_date=?",
                        (last,)).fetchall()
    bad = [r for r in rows if qg.has_issue(f"{r[3] or ''} {r[4] or ''}")]
    for r in bad:
        conn.execute("DELETE FROM insights_daily WHERE rowid=?", (r[0],))
    conn.commit()
    return [f"{r[1]}/{r[2]}: {(r[3] or '')[:80]}" for r in bad]


def build_reports(conn) -> dict:
    import pulse_data as pdx
    import pulse_insights
    import pulse_story
    daily = pdx.hotel_daily(conn)
    if daily is None or daily.empty:
        return {}
    end = daily["date"].max()
    out = {}
    for label, months, start in (("Year to date", 12, f"{end.year}-01-01"), ("Last 12 months", 12, None)):
        model = pulse_story.build_model(conn, months, start, end.strftime("%Y-%m-%d") if start else None, label)
        if model is None:
            continue
        out[label] = (model, pulse_insights.build_report(conn, model, None, date.today()))
    return out


def board_payloads(model) -> dict:
    try:
        import pulse_board as pb
    except Exception:  # noqa: BLE001
        return {}
    out = {}
    for name in ("brief", "snapshot", "forward"):
        fn = getattr(pb, name, None)
        if fn:
            try:
                out[name] = fn(model)
            except Exception as exc:  # noqa: BLE001
                out[name] = {"error": str(exc)}
    return out


def main() -> int:
    conn = sqlite3.connect(DB, timeout=30)
    result = {"run_at": datetime.now().isoformat(timespec="seconds"), "status": "PASS",
              "zero_placeholders": [], "insights_removed": [], "reports": {}, "board_warnings": {}}
    try:
        zp = check_zero_placeholders(conn)
        result["zero_placeholders"] = zp
        if zp:
            _log(f"FAIL zero placeholders still present: {zp}")
            result["status"] = "FAIL"
        removed = clean_insights(conn)
        result["insights_removed"] = removed
        for r in removed:
            _log(f"removed insight with a missing value: {r}")
        reports = build_reports(conn)
        if not reports:
            _log("FAIL report could not be built")
            result["status"] = "FAIL"
        for label, (model, rep) in reports.items():
            issues = qg.scan(rep)
            result["reports"][label] = {"excluded": rep.get("_qa", []), "residual_issues": issues}
            _log(f"{label}: {len(rep.get('_qa', []))} item(s) excluded by the quality gate, {len(issues)} residual issue(s)")
            for i in issues[:25]:
                _log(f"  residual: {i}")
            if issues:
                result["status"] = "FAIL"
            warns = qg.scan(board_payloads(model))
            result["board_warnings"][label] = warns
            for w in warns[:10]:
                _log(f"  board warning: {w}")
    finally:
        conn.close()
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    with open(os.path.join(ROOT, "logs", "qa_publish_report.json"), "w") as fh:
        json.dump(result, fh, indent=2, default=str)
    _log(f"publish gate {result['status']}")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
