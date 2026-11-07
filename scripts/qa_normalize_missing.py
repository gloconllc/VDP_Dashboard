# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created by John Picou, GloCon Solutions LLC, for Visit Dana Point.
#  Use, copying, sharing, or redistribution requires written permission.
#  Contact GloCon Solutions LLC (John Picou), https://gloconsolutions.com.
# =============================================================================
"""
qa_normalize_missing.py, pipeline step: zero placeholders become NULL.

Runs after the loaders and before compute_kpis / compute_insights, so every
formula downstream (averages, totals, year-over-year, shares) skips a missing
number instead of counting it as zero. The list of measures where zero means
"missing" lives in dashboard/quality_gate.py (ZERO_IS_MISSING).

Usage:  python scripts/qa_normalize_missing.py [--dry-run]
Writes: logs/qa_normalize.json
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "dashboard"))
import quality_gate as qg  # noqa: E402

DB = os.environ.get("PULSE_DB") or os.path.join(ROOT, "data", "analytics.sqlite")


def main() -> int:
    dry = "--dry-run" in sys.argv
    conn = sqlite3.connect(DB, timeout=30)
    try:
        rows = qg.normalize_missing(conn, dry_run=dry)
    finally:
        conn.close()
    total = sum(r["zeros_excluded"] for r in rows)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for r in rows:
        print(f"[{stamp}] qa_normalize: {r['table']}.{r['column']}: {r['zeros_excluded']} zero placeholder(s) "
              f"{'would be' if dry else ''} set to NULL")
    print(f"[{stamp}] qa_normalize: {total} zero placeholder(s) {'found' if dry else 'excluded'} across {len(rows)} measure(s)")
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    with open(os.path.join(ROOT, "logs", "qa_normalize.json"), "w") as fh:
        json.dump({"run_at": stamp, "dry_run": dry, "excluded": rows, "total": total}, fh, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
