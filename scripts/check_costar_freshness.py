"""
check_costar_freshness.py — has a CoStar pull happened recently enough?

Written 2026-09-20, CORRECTED 2026-09-21 after getting it wrong the first time.

WHAT THIS MEASURES, AND WHY THE OBVIOUS VERSION IS WRONG
  The first version of this script compared each table's newest COVERED
  period (as_of_date / report_period) against today's calendar date, and
  duly reported the CoStar tables as 23 and 82 days stale. That was wrong,
  and it is the exact bug this repo has already found and fixed three times
  (audit_app.py, and compute_insights.py's load_kpi_recent and
  load_str_revenue): measuring data against the wall clock instead of
  against the data.

  CoStar publishes with a real, expected lag. The 2026-09-07 pull contained
  daily data only through 2026-08-29, nine days behind, and monthly data
  only through 2026-07, because July was the last closed month. None of
  that is staleness. That is simply what CoStar had.

  So the question worth asking is not "how old is the newest row" but
  "how long since anybody actually pulled from CoStar". That is the thing
  automation fixes, and the thing a missed Thursday run breaks.

TWO SIGNALS
  1. PULL RECENCY (the real check). How many days since the most recent
     CoStar pull, taken from the pull-date columns the loaders record
     (costar_annual_performance.report_date, costar_participation.snapshot_date)
     and from the newest MM_DD_YY stamp on a file in data/costar/.
     Weekly cadence plus grace, so anything past MAX_DAYS_SINCE_PULL means
     a run was missed.

  2. COVERAGE LAG (a sanity check, not a staleness check). How far each
     table's newest covered period sits behind THE PULL DATE, never behind
     today. A daily export whose newest row is far behind its own pull date
     is a malformed or truncated export. Normal is about 9 days.

Exit codes:
  0  a pull happened recently and coverage looks normal
  1  no recent pull, or a table is empty/missing, or coverage is abnormal
"""

from __future__ import annotations

import re
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "analytics.sqlite"
COSTAR_DIR = PROJECT_ROOT / "data" / "costar"

# Intended cadence is weekly (Thursday), matching str_weekly_sync.yml.
# 10 days means one missed run is caught before a second one goes by.
MAX_DAYS_SINCE_PULL = 10

# CoStar's observed daily lag is ~9 days behind the pull. Well past that
# means a truncated export, not a normal one.
MAX_DAILY_COVERAGE_LAG = 21
# Monthly closes a month in arrears; ~40 days behind the pull is normal.
MAX_MONTHLY_COVERAGE_LAG = 75

FILE_DATE_RE = re.compile(r"_(\d{2})_(\d{2})_(\d{2})\.xlsx$", re.IGNORECASE)
PDF_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})\.pdf$", re.IGNORECASE)

# (table, coverage column, granularity)
COVERAGE = [
    ("costar_market_daily",           "as_of_date",    "daily"),
    ("costar_market_daily_segment",   "as_of_date",    "daily"),
    ("costar_market_monthly",         "report_period", "monthly"),
    ("costar_market_monthly_segment", "report_period", "monthly"),
    ("costar_participation",          "period_month",  "monthly"),
]

# Columns that record WHEN a pull happened, as opposed to what it covers.
PULL_MARKERS = [
    ("costar_annual_performance", "report_date"),
    ("costar_segment_room_split", "report_date"),
    ("costar_participation",      "snapshot_date"),
]


def _parse(value, granularity: str):
    if not value:
        return None
    value = str(value).strip()
    fmts = ("%Y-%m-%d",) if granularity == "daily" else ("%Y-%m", "%Y-%m-%d")
    for fmt in fmts:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _latest_pull_from_files():
    """Newest pull date visible in data/costar/ filenames."""
    newest = None
    if not COSTAR_DIR.exists():
        return None
    for p in COSTAR_DIR.iterdir():
        d = None
        m = FILE_DATE_RE.search(p.name)
        if m:
            mm, dd, yy = m.groups()
            try:
                d = date(2000 + int(yy), int(mm), int(dd))
            except ValueError:
                d = None
        else:
            m = PDF_DATE_RE.search(p.name)
            if m:
                d = _parse(m.group(1), "daily")
        if d and (newest is None or d > newest):
            newest = d
    return newest


def main() -> int:
    if not DB_PATH.exists():
        print(f"FAIL: {DB_PATH} not found.")
        return 1

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)
    today = date.today()
    problems = []

    print(f"CoStar freshness check, {today.isoformat()}")
    print("=" * 72)

    # --- 1. pull recency -------------------------------------------------
    print("\n1. PULL RECENCY  (how long since anyone pulled from CoStar)")
    print("-" * 72)

    pull_dates = []
    for table, col in PULL_MARKERS:
        try:
            v = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()[0]  # noqa: S608
        except sqlite3.Error:
            continue
        d = _parse(v, "daily")
        if d:
            pull_dates.append(d)
            print(f"   {table}.{col}: {v}")

    file_pull = _latest_pull_from_files()
    if file_pull:
        pull_dates.append(file_pull)
        print(f"   newest dated file in data/costar/: {file_pull.isoformat()}")

    if not pull_dates:
        print("   FAIL: no pull date found anywhere.")
        problems.append("no pull date found")
        latest_pull = None
    else:
        latest_pull = max(pull_dates)
        age = (today - latest_pull).days
        verdict = "OK   " if age <= MAX_DAYS_SINCE_PULL else "STALE"
        print(f"\n   {verdict} last pull {latest_pull.isoformat()}, {age} days ago "
              f"(limit {MAX_DAYS_SINCE_PULL})")
        if age > MAX_DAYS_SINCE_PULL:
            problems.append(
                f"last CoStar pull was {latest_pull.isoformat()}, {age} days ago"
            )

    # --- 2. coverage lag, measured against the pull, not today -----------
    print("\n2. COVERAGE  (newest period each table holds, vs the pull date)")
    print("-" * 72)

    for table, col, granularity in COVERAGE:
        try:
            newest_raw, n_rows = conn.execute(
                f"SELECT MAX({col}), COUNT(*) FROM {table}"  # noqa: S608
            ).fetchone()
        except sqlite3.Error as exc:
            print(f"   FAIL  {table}: unreadable ({exc})")
            problems.append(f"{table} unreadable")
            continue

        if not n_rows:
            print(f"   FAIL  {table}: 0 rows")
            problems.append(f"{table} empty")
            continue

        newest = _parse(newest_raw, granularity)
        if newest is None:
            print(f"   FAIL  {table}: MAX({col}) = {newest_raw!r}, unparseable")
            problems.append(f"{table} unparseable date")
            continue

        if latest_pull is None:
            print(f"   ?     {table}: newest {newest_raw} ({n_rows} rows), "
                  f"no pull date to compare against")
            continue

        lag = (latest_pull - newest).days
        limit = (MAX_DAILY_COVERAGE_LAG if granularity == "daily"
                 else MAX_MONTHLY_COVERAGE_LAG)
        if lag > limit:
            print(f"   ODD   {table}: newest {newest_raw} is {lag} days behind "
                  f"the {latest_pull.isoformat()} pull (normal under {limit}). "
                  f"Possible truncated export.")
            problems.append(f"{table} coverage lag {lag}d behind its own pull")
        else:
            print(f"   OK    {table}: newest {newest_raw}, {lag} days behind the "
                  f"pull, which is normal for CoStar ({n_rows} rows)")

    conn.close()
    print("\n" + "=" * 72)

    if problems:
        print(f"FAIL: {len(problems)} problem(s):")
        for p in problems:
            print(f"   - {p}")
        print("\nNote: every CoStar loader is skip-safe, so run_pipeline.py may "
              "still have exited 0. That is what this check is for.")
        return 1

    print("OK: CoStar pulled recently, coverage normal.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
