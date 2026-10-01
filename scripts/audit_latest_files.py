"""
audit_latest_files.py
=====================
Pipeline audit step: "is the newest file the one the board is showing?"

The identifier for "latest" is the DATE A FILE WAS ADDED (see scripts/file_dates.py), never its
name. Names such as "TopMarkets_Export (6).csv" or "marketAnalysis-..." say almost nothing about
which export is newest. For every data source the audit answers four questions:

  1. What was added most recently, and when?               (date added, by git or file time)
  2. Did the loader read every file in that newest batch?  (datafy_file_ingest + load_log)
  3. Is the newest file's data what the board reads?       (period actually stored vs period shown)
  4. How far behind the newest files is each table?        (database freshness)

Output
  * stdout            first line is a one-line headline (it becomes the pipeline.log summary)
  * logs/file_audit.txt   the full readable report
  * logs/file_audit.json  the same findings, machine readable
  * file_audit table      findings of the latest run (replaced each run)

Severity
  ATTN  something newer on disk is not what the board shows, or was not read. Needs a person.
  NOTE  worth knowing, nothing is wrong.
  OK    confirmed.

Always exits 0 so it can never block the pipeline; pass --strict to exit 1 when ATTN findings exist.

Run:  python3 scripts/audit_latest_files.py
"""

from __future__ import annotations

import fnmatch
import glob
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
sys.path.insert(0, BASE_DIR)
import file_dates as FD  # noqa: E402

DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# Datafy publishes with a lag of about three weeks: a September 30 export carries September
# only through about the 9th. A month counts as complete once it ended at least this long
# before the file was added.
DATAFY_LAG_DAYS = 21

# Files this much newer than the data the board shows are worth flagging (absorbs clock skew).
TOLERANCE = timedelta(hours=2)

DDL = """
CREATE TABLE IF NOT EXISTS file_audit (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at     TEXT,
    source     TEXT,
    severity   TEXT,     -- ATTN | NOTE | OK
    code       TEXT,
    subject    TEXT,
    detail     TEXT
)
"""

findings: list[dict] = []


def add(source: str, severity: str, code: str, subject: str, detail: str) -> None:
    findings.append({"source": source, "severity": severity, "code": code,
                     "subject": subject, "detail": detail})


def fmt(dt) -> str:
    if not dt:
        return "unknown"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return dt
    return dt.strftime("%b %d, %Y %H:%M UTC")


def _short(dt) -> str:
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return dt
    return dt.strftime("%Y-%m-%d") if dt else "unknown"


def table_exists(cur, name: str) -> bool:
    return bool(cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def table_cols(cur, name: str) -> list[str]:
    return [r[1] for r in cur.execute(f"PRAGMA table_info({name})")]


def last_complete_month(added: datetime) -> tuple:
    """(year, month) of the newest month that had fully ended DATAFY_LAG_DAYS before `added`."""
    cut = added - timedelta(days=DATAFY_LAG_DAYS)
    first = datetime(cut.year, cut.month, 1)
    month_end = (first + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    if month_end.date() <= cut.date():
        return (cut.year, cut.month)
    prev = first - timedelta(days=1)
    return (prev.year, prev.month)


def board_tables() -> set[str]:
    """Datafy tables the dashboard reads, found by scanning its source (no list to maintain)."""
    found: set[str] = set()
    dash = os.path.join(PROJECT_ROOT, "dashboard")
    for path in glob.glob(os.path.join(dash, "*.py")):
        if "backup" in os.path.basename(path):
            continue
        try:
            text = open(path, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        found.update(re.findall(r"\bdatafy_[a-z0-9_]+", text))
    return found


# ─────────────────────────────────────────────────────────────────────────────
# Datafy
# ─────────────────────────────────────────────────────────────────────────────

def audit_datafy(cur) -> dict:
    out: dict = {}
    if not table_exists(cur, "datafy_file_ingest"):
        add("Datafy", "ATTN", "NO_INGEST_LOG", "datafy_file_ingest",
            "The Datafy loader has not written its file log yet. Run scripts/load_datafy_reports.py.")
        return out

    rows = cur.execute("""
        SELECT batch_no, file_path, file_name, family, date_added, date_added_src,
               target_table, period_start, period_end, period_source, rows_loaded, status, note
        FROM datafy_file_ingest ORDER BY date_added, id
    """).fetchall()
    cols = ["batch_no", "file_path", "file_name", "family", "date_added", "src",
            "table", "ps", "pe", "psrc", "rows", "status", "note"]
    files = [dict(zip(cols, r)) for r in rows]
    if not files:
        add("Datafy", "NOTE", "NO_FILES", "data/datafy", "No Datafy CSV files were found.")
        return out

    last_batch = max(f["batch_no"] or 0 for f in files)
    newest = [f for f in files if f["batch_no"] == last_batch]
    newest_when = max(f["date_added"] for f in newest)
    ok_new = sum(1 for f in newest if f["status"] == "ok")
    out["newest_batch"] = {"batch_no": last_batch, "added": newest_when, "files": len(newest), "loaded": ok_new}
    add("Datafy", "OK", "NEWEST_BATCH", f"batch {last_batch} of {len({f['batch_no'] for f in files})}",
        f"Newest upload added {fmt(newest_when)} ({newest[0]['src']}): {len(newest)} files, "
        f"{ok_new} read by load_datafy_reports.py (the advertising and extended loaders read the rest).")

    # Undated exports: where did the loader get their window?
    recorded = [f for f in newest if f["psrc"] == "date_filters"]
    if recorded:
        pairs = sorted({f"{f['ps']} to {f['pe']}" for f in recorded})
        add("Datafy", "NOTE", "WINDOW_FROM_RECORDED_FILTER", f"{len(recorded)} file(s) in the newest upload",
            f"These exports state no date range, so each is filed under the Datafy date filter written down in "
            f"data/datafy_date_filters.json ({'; '.join(pairs)}). Putting the range in the file name, for example "
            f"_01-01-2026_to_30-09-2026, overrides the record and is the permanent fix.")
    inferred = [f for f in newest if f["psrc"] == "batch_window"]
    if inferred:
        m = None
        for f in inferred:
            m = re.search(r"read from (.+?) in the same upload", f["note"] or "")
            if m:
                break
        add("Datafy", "NOTE", "WINDOW_READ_FROM_UPLOAD", f"{len(inferred)} file(s) in the newest upload",
            f"These exports state no date range and no filter was recorded, so the upload's window "
            f"({inferred[0]['ps']} to {inferred[0]['pe']}) was read from {m.group(1) if m else 'a dated companion file'} "
            f"in the same upload. Attribution and Advertising can use a narrower filter than the visitation reports, "
            f"so confirm it against the dashboard or put the range in the file name.")
    # A recorded filter that no file used points to a wrong 'added' date or pattern.
    try:
        import json
        with open(os.path.join(DATA_DIR, "datafy_date_filters.json"), encoding="utf-8") as fh:
            rec_filters = json.load(fh).get("filters", [])
    except (OSError, ValueError, AttributeError):
        rec_filters = []
    unused = [e for e in rec_filters
              if not any(f["psrc"] == "date_filters" and f["ps"] == e.get("start") and f["pe"] == e.get("end")
                         for f in files)]
    if unused:
        add("Datafy", "ATTN", "DATE_FILTER_UNUSED", f"{len(unused)} recorded filter(s)",
            "data/datafy_date_filters.json has entries that no file used (the 'added' date or the 'match' pattern "
            "does not fit the repository): " + "; ".join(f"{e.get('match')} added {e.get('added')}" for e in unused[:4]) + ".")

    # Files that other Datafy loaders (advertising, extended) own are named in load_log.
    owned: set[str] = set()
    if table_exists(cur, "load_log"):
        for (names,) in cur.execute("SELECT file_name FROM load_log WHERE LOWER(source) = 'datafy'"):
            for part in str(names or "").split(","):
                owned.add(part.strip().lower())

    # ── Newest batch: anything nobody read ──────────────────────────────────
    def _read(f: dict) -> bool:
        return f["status"] == "ok" or f["file_name"].lower() in owned

    def _rank(f: dict):
        return (f["date_added"], FD.copy_number(f["file_name"]), f["file_name"].lower())

    unread, superseded, other_measure = [], [], []
    for f in newest:
        if f["status"] in ("ok", "skip_list"):
            continue
        if f["file_name"].lower() in owned:
            continue
        if "different measure" in (f["note"] or ""):
            other_measure.append(f)
            continue
        # An older copy of a report type is not a gap when a newer copy of the same type was read.
        if f["family"] and any(g is not f and g["family"] == f["family"] and _read(g) and _rank(g) > _rank(f)
                               for g in files):
            superseded.append(f)
            continue
        unread.append(f)
    if other_measure:
        add("Datafy", "NOTE", "DIFFERENT_MEASURE", f"{len(other_measure)} file(s) in the newest upload",
            "Read, but each holds a different measure than the table stores (for example Share of Trips where "
            "Share of Visitor Days is expected), so the earlier rows were kept: "
            + "; ".join(f["file_name"] for f in other_measure[:6]) + ".")
    if superseded:
        add("Datafy", "NOTE", "OLDER_COPY_SKIPPED", f"{len(superseded)} file(s) in the newest upload",
            "A newer copy of the same report type was read, so these older copies were passed over: "
            + "; ".join(f["file_name"] for f in superseded[:6]) + ".")
    if unread:
        names = "; ".join(f["file_name"] for f in unread[:8]) + (f"; and {len(unread) - 8} more" if len(unread) > 8 else "")
        add("Datafy", "ATTN", "NOT_LOADED", f"{len(unread)} file(s) in the newest upload",
            f"Added {_short(newest_when)} but no loader read them: {names}. "
            f"Each needs a handler (NEW_FILE_HANDLERS in load_datafy_reports.py or load_datafy_extended.py) "
            f"or an entry in SKIP_STEMS if it is intentionally ignored.")
    skipped_list = [f for f in newest if f["status"] == "skip_list"]
    if skipped_list:
        add("Datafy", "NOTE", "ON_SKIP_LIST", f"{len(skipped_list)} file(s) in the newest upload",
            "Deliberately ignored (SKIP_STEMS): " + "; ".join(f["file_name"] for f in skipped_list[:6]))

    # ── Per table: is the newest file what the board reads? ─────────────────
    used_by_board = board_tables()
    by_table: dict[str, list[dict]] = defaultdict(list)
    for f in files:
        if f["table"] and f["status"] == "ok":
            by_table[f["table"]].append(f)

    shown_summary = []
    stale_board: list[tuple] = []     # (table, newest_file, newest_added, newest_period, shown_period, shown_added, gap_days)
    stale_other: list[str] = []
    for table, fl in sorted(by_table.items()):
        if not table_exists(cur, table) or "report_period_start" not in table_cols(cur, table):
            continue
        pairs = cur.execute(
            f"SELECT DISTINCT report_period_start, report_period_end FROM {table} "
            f"ORDER BY report_period_end DESC, report_period_start DESC").fetchall()
        if not pairs:
            continue
        shown = pairs[0]                                  # the pair the board's queries select
        supplier = [f for f in fl if (f["ps"], f["pe"]) == shown]
        shown_added = max((f["date_added"] for f in supplier), default=None)
        newest_file = max(fl, key=lambda f: (f["date_added"], FD.copy_number(f["file_name"]), f["file_name"].lower()))
        board = table in used_by_board
        shown_summary.append({"table": table, "board_reads": board, "shown_period": list(shown),
                              "shown_added": shown_added, "newest_file": newest_file["file_name"],
                              "newest_added": newest_file["date_added"],
                              "newest_period": [newest_file["ps"], newest_file["pe"]],
                              "newest_period_source": newest_file["psrc"]})
        if (newest_file["ps"], newest_file["pe"]) == shown or shown_added is None:
            continue
        gap = datetime.fromtimestamp(0) if False else (datetime.fromisoformat(newest_file["date_added"])
                                                       - datetime.fromisoformat(shown_added))
        if gap <= TOLERANCE:
            continue
        if board and newest_file["psrc"] == "folder_default":
            stale_board.append((table, newest_file["file_name"], newest_file["date_added"],
                                (newest_file["ps"], newest_file["pe"]), shown, shown_added, gap.days))
        else:
            stale_other.append(table)

    if stale_board:
        newest_day = max(x[2] for x in stale_board)[:10]
        lines = "; ".join(f"{t.replace('datafy_overview_', '')} ({fn})" for t, fn, *_ in stale_board)
        shown_periods = sorted({f"{x[4][0]} to {x[4][1]}" for x in stale_board})
        add("Datafy", "ATTN", "NEWER_FILES_NOT_SHOWN", f"{len(stale_board)} board table(s)",
            f"Files added {newest_day} are newer than the data the board shows ({', '.join(shown_periods)}, "
            f"files added {min(x[5] for x in stale_board)[:10]}), but they were filed under a fixed folder default "
            f"({stale_board[0][3][0]} to {stale_board[0][3][1]}) and the board does not use them: {lines}. "
            f"Add the Datafy date range to each file name, or record it in data/datafy_date_filters.json, for example "
            f"TopMarkets_Export (6)_01-01-2026_to_30-09-2026.csv, and re-run.")
    else:
        add("Datafy", "OK", "BOARD_USES_NEWEST", "board tables",
            "For every Datafy table the board reads, the newest file's period is the one shown.")
    if stale_other:
        add("Datafy", "NOTE", "NEWER_FILES_NOT_SHOWN_OTHER", f"{len(stale_other)} table(s) the board does not read",
            "Newer undated exports are stored under an assumed period: " + ", ".join(sorted(stale_other)) + ".")
    out["tables"] = shown_summary

    # ── Where name order and date added disagree ────────────────────────────
    fam: dict[str, list[dict]] = defaultdict(list)
    for f in files:
        if f["family"]:
            fam[f["family"]].append(f)
    disagree = []
    for key, fl in fam.items():
        if len(fl) < 2:
            continue
        by_date = max(fl, key=lambda f: (f["date_added"], FD.copy_number(f["file_name"]), f["file_name"].lower()))
        by_name = max(fl, key=lambda f: f["file_name"].lower())
        if by_date["file_name"] != by_name["file_name"]:
            disagree.append((key, by_date["file_name"], by_name["file_name"]))
    out["name_vs_date_disagreements"] = len(disagree)
    if disagree:
        sample = "; ".join(f"{d[1]} (by date) instead of {d[2]} (by name)" for d in disagree[:3])
        add("Datafy", "NOTE", "NAME_ORDER_WRONG", f"{len(disagree)} report type(s)",
            f"Sorting by name would have chosen an older file than date added does. Date added is used. "
            f"Examples: {sample}.")

    # ── The monthly trend the board draws ───────────────────────────────────
    series = {}
    for table, label in (("datafy_overview_spending_by_month", "spend"),
                         ("datafy_overview_visitation_by_month", "visitor days")):
        if not table_exists(cur, table):
            continue
        val = "spending_usd" if label == "spend" else "visitor_days"
        mo = cur.execute(f"SELECT year, month, {val} FROM {table} WHERE {val} > 0").fetchall()
        mnum = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun",
                                            "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
        pts = []
        for y, m, v in mo:
            try:
                mm = mnum.get(str(m).strip().lower()[:3]) or int(m)
                pts.append((int(y), mm))
            except (TypeError, ValueError):
                continue
        if pts:
            series[label] = max(pts)
    # date the series file was added: newest ok file feeding those tables
    feed = [f for f in files if f["status"] == "ok" and f["table"] in
            ("datafy_overview_spending_by_month", "datafy_overview_visitation_by_month")]
    if series and feed:
        f_new = max(feed, key=lambda f: f["date_added"])
        added = datetime.fromisoformat(f_new["date_added"])
        last_ym = max(series.values())
        last_complete = last_complete_month(added)
        out["series"] = {"last_month_with_data": last_ym, "last_complete_month": last_complete,
                         "file": f_new["file_name"], "added": f_new["date_added"]}
        partial = last_ym > last_complete
        add("Datafy", "NOTE" if partial else "OK", "TREND_THROUGH",
            "monthly spend and visitor days",
            f"Newest series file {f_new['file_name']} (added {_short(f_new['date_added'])}) has data through "
            f"{datetime(last_ym[0], last_ym[1], 1):%B %Y}"
            + (f"; Datafy lags about {DATAFY_LAG_DAYS} days, so {datetime(last_ym[0], last_ym[1], 1):%B} is partial and "
               f"{datetime(last_complete[0], last_complete[1], 1):%B %Y} is the last complete month the board trends."
               if partial else "."))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Other sources: newest file by date added, and is it in the load log?
# ─────────────────────────────────────────────────────────────────────────────

SKIP_DIRS = {"datafy", "design"}
SKIP_SUFFIX = (".gitkeep", ".ds_store", ".json")


def _logged_names(cur) -> list[str]:
    if not table_exists(cur, "load_log"):
        return []
    names: list[str] = []
    for (n,) in cur.execute("SELECT DISTINCT file_name FROM load_log"):
        for part in str(n or "").split(","):
            part = part.strip().lower()
            if part:
                names.append(part)
    return names


def audit_other_sources(cur) -> None:
    logged = _logged_names(cur)

    def is_logged(basename: str) -> bool:
        b = basename.lower()
        for n in logged:
            if b == n or (("*" in n or "?" in n) and fnmatch.fnmatch(b, n)):
                return True
        return False

    for folder in sorted(os.listdir(DATA_DIR)):
        fp = os.path.join(DATA_DIR, folder)
        if not os.path.isdir(fp) or folder.lower() in SKIP_DIRS:
            continue
        files = [p for p in glob.glob(os.path.join(fp, "**", "*"), recursive=True)
                 if os.path.isfile(p) and not os.path.basename(p).lower().endswith(SKIP_SUFFIX)
                 and not os.path.basename(p).startswith("~$")]
        if not files:
            continue
        ranked = FD.sort_oldest_first(files, PROJECT_ROOT)
        newest = ranked[-1]
        when, src = FD.date_added(newest, PROJECT_ROOT)
        batch = [p for p in ranked if (when - FD.date_added(p, PROJECT_ROOT)[0]) <= timedelta(hours=6)]
        rel = os.path.relpath(newest, fp).replace(os.sep, "/")
        add(folder, "OK", "NEWEST_FILE", f"data/{folder}",
            f"Newest file by date added: {rel} ({fmt(when)}, {src}); {len(batch)} file(s) in that upload."
            + (" Named in the load log." if is_logged(os.path.basename(newest)) else ""))


# ─────────────────────────────────────────────────────────────────────────────
# Database freshness next to the newest source files
# ─────────────────────────────────────────────────────────────────────────────

FRESHNESS = [
    # (label, folder whose newest file feeds it, SQL returning the latest date)
    ("Hotel daily (kpi_daily_summary)", "str", "SELECT MAX(as_of_date) FROM kpi_daily_summary"),
    ("STR weekly (fact_str_group_metrics)", "str",
     "SELECT MAX(as_of_date) FROM fact_str_group_metrics WHERE grain='weekly'"),
    ("CoStar market daily", "costar", "SELECT MAX(as_of_date) FROM costar_market_daily"),
    ("CoStar market monthly", "costar", "SELECT MAX(report_period) FROM costar_market_monthly"),
    ("Datafy advertising snapshot", "datafy", "SELECT MAX(snapshot_date) FROM datafy_advertising_kpis"),
    ("Insights", None, "SELECT MAX(as_of_date) FROM insights_daily"),
]


def audit_freshness(cur) -> list[dict]:
    rows = []
    for label, folder, sql in FRESHNESS:
        try:
            latest = cur.execute(sql).fetchone()[0]
        except Exception:
            continue
        newest_added = None
        if folder:
            fp = os.path.join(DATA_DIR, folder)
            files = [p for p in glob.glob(os.path.join(fp, "**", "*"), recursive=True) if os.path.isfile(p)]
            if files:
                newest_added = max(FD.date_added(p, PROJECT_ROOT)[0] for p in files)
        rows.append({"label": label, "database_through": latest, "newest_file_added": newest_added})
        add("Database", "OK", "FRESHNESS", label,
            f"Database runs through {latest}"
            + (f"; newest {folder} file was added {_short(newest_added)}." if newest_added else "."))
    # STR main files are replaced in place; call out when they lag the weekly drops.
    try:
        strdir = os.path.join(DATA_DIR, "str")
        main_files = [os.path.join(strdir, n) for n in ("str_daily.xlsx", "str_monthly.xlsx") if
                      os.path.exists(os.path.join(strdir, n))]
        weekly = [p for p in glob.glob(os.path.join(strdir, "weekly", "*")) if os.path.isfile(p)]
        if main_files and weekly:
            main_added = max(FD.date_added(p, PROJECT_ROOT)[0] for p in main_files)
            weekly_added = max(FD.date_added(p, PROJECT_ROOT)[0] for p in weekly)
            if weekly_added - main_added > timedelta(days=7):
                add("str", "ATTN", "STR_MAIN_FILES_OLDER", "str_daily.xlsx, str_monthly.xlsx",
                    f"These were last replaced {_short(main_added)}, {(weekly_added - main_added).days} days before the "
                    f"newest weekly file ({_short(weekly_added)}). The STR daily and monthly series stop where those "
                    f"two files stop; drop fresh exports under the same names to extend them.")
    except Exception:
        pass
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    strict = "--strict" in sys.argv
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript(DDL)

    detail = {}
    try:
        detail["datafy"] = audit_datafy(cur)
    except Exception as exc:                               # never let the audit break the pipeline
        add("Datafy", "NOTE", "AUDIT_ERROR", "datafy", f"Audit could not finish: {exc}")
    try:
        audit_other_sources(cur)
    except Exception as exc:
        add("Files", "NOTE", "AUDIT_ERROR", "other sources", f"Audit could not finish: {exc}")
    try:
        detail["freshness"] = audit_freshness(cur)
    except Exception as exc:
        add("Database", "NOTE", "AUDIT_ERROR", "freshness", f"Audit could not finish: {exc}")

    run_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("DELETE FROM file_audit")
    cur.executemany(
        "INSERT INTO file_audit (run_at, source, severity, code, subject, detail) VALUES (?,?,?,?,?,?)",
        [(run_at, f["source"], f["severity"], f["code"], f["subject"], f["detail"]) for f in findings])
    conn.commit()
    conn.close()

    attn = [f for f in findings if f["severity"] == "ATTN"]
    note = [f for f in findings if f["severity"] == "NOTE"]
    okc = [f for f in findings if f["severity"] == "OK"]
    headline = (f"FILE AUDIT (latest = date added): {len(attn)} need attention, {len(note)} notes, {len(okc)} confirmed"
                + (" | " + "; ".join(f"{f['code']} {f['subject']}" for f in attn[:3]) if attn else ""))

    lines = [headline, ""]
    for sev, bucket in (("NEEDS ATTENTION", attn), ("NOTES", note), ("CONFIRMED", okc)):
        if not bucket:
            continue
        lines.append(sev.title())
        for f in bucket:
            lines.append(f"  [{f['source']}] {f['code']}: {f['subject']}")
            lines.append(f"      {f['detail']}")
        lines.append("")
    report = "\n".join(lines).rstrip() + "\n"

    os.makedirs(LOG_DIR, exist_ok=True)
    with open(os.path.join(LOG_DIR, "file_audit.txt"), "w", encoding="utf-8") as fh:
        fh.write(f"Generated {run_at} UTC\n\n" + report)
    with open(os.path.join(LOG_DIR, "file_audit.json"), "w", encoding="utf-8") as fh:
        json.dump({"generated_utc": run_at, "attn": len(attn), "notes": len(note), "ok": len(okc),
                   "findings": findings, "detail": detail}, fh, indent=2, default=str)

    # First line only to the pipeline log (run_pipeline joins stdout lines); full text lives in logs/.
    print(headline)
    if "--verbose" in sys.argv:
        print(report)
    return 1 if (strict and attn) else 0


if __name__ == "__main__":
    sys.exit(main())
