"""
load_datafy_advertising.py — Datafy "Advertising" campaign performance reports.

This is a distinct Datafy export family from the paid-media/ad-campaign side
of the platform (impressions, clicks, spend, CTR, DMA-level trip attribution,
ROAS) rather than Datafy's visitor-economy geo-fencing reports handled by
load_datafy_reports.py. They land directly in data/datafy/ (top level), named:

  Advertising_Overview_TopMarketsVisual_Export.csv
      → DMA-level trip share / impact / spend-per-visitor
  Advertising_Overview_TopPerformersVisual_Export.csv
      → attribution rate by tactic (VIDEO / NATIVE / BANNER)
  Advertising_Shared_AttributionGroupsSummaryVisual_Export.csv
      → est. trips/visitor-days/impact by attribution group (Destination/Hotels/Resorts)
  Advertising_Shared_TraditionalKPIsVisual_Export.csv (sometimes duplicated as "(1)")
      → campaign-wide impressions/clicks/spend/reach/CTR/VCR
  Advertising_TraditionalAdvertisingMetrics_LineItemPerformanceVisual_Export.csv
      → per-line-item impressions/clicks/CTR/spend/VCR
  Overview Dashboard_Export.csv
      → campaign-level Est. Campaign Impact / Est. ROAS / Cost per Visitor Day

None of these carry a report_period in the data itself (no date columns, no
date embedded in the filename) — each is a point-in-time snapshot of the
campaign to date. Rows are keyed by snapshot_date, taken from the file's own
mtime since there's no other date signal available. A re-run replaces that
day's snapshot rather than appending duplicates, so re-exporting the same
day's numbers twice (as happened with TraditionalKPIsVisual in the first
batch this loader was written for) is harmless — same day, same content,
same row. A fresh git checkout resets every mtime, so the loader also
compares content with the newest snapshot already on record and reuses that
date when nothing changed (see _stable_snapshot); a new snapshot is only
created when the export's numbers actually differ.

Skip-safe: any file that's missing or fails to parse is logged and skipped
rather than raising.
"""

import os
import re
import sqlite3
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
DATAFY_DIR = os.path.join(PROJECT_ROOT, "data", "datafy")

DDL = """
CREATE TABLE IF NOT EXISTS datafy_advertising_top_markets (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date         TEXT NOT NULL,
    dma                   TEXT NOT NULL,
    trip_share_pct        REAL,
    trips                 INTEGER,
    visitor_days          INTEGER,
    avg_trip_length_days  REAL,
    est_impact_usd        REAL,
    spend_per_visitor_usd REAL,
    loaded_at             TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, dma) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS datafy_advertising_tactic_performance (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date         TEXT NOT NULL,
    tactic                TEXT NOT NULL,
    attribution_rate_pct  REAL,
    loaded_at             TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, tactic) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS datafy_advertising_attribution_groups (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date            TEXT NOT NULL,
    attribution_group        TEXT NOT NULL,
    est_trips                INTEGER,
    est_visitor_days         INTEGER,
    avg_length_of_stay_days  REAL,
    est_campaign_impact_usd  REAL,
    loaded_at                TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, attribution_group) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS datafy_advertising_kpis (
    id                            INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date                 TEXT NOT NULL,
    total_impressions             INTEGER,
    total_clicks                  INTEGER,
    total_spend_usd                REAL,
    unique_reach                  INTEGER,
    avg_display_ctr_pct            REAL,
    avg_native_ctr_pct             REAL,
    avg_vcr_acr_pct                 REAL,
    total_video_audio_completes    INTEGER,
    loaded_at                      TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS datafy_advertising_line_item_performance (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date    TEXT NOT NULL,
    line_item_name   TEXT NOT NULL,
    impressions      INTEGER,
    clicks           INTEGER,
    ctr_pct          REAL,
    total_spend_usd  REAL,
    vcr_acr_pct      REAL,
    loaded_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, line_item_name) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS datafy_advertising_overview (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date             TEXT NOT NULL,
    est_campaign_impact_usd   REAL,
    est_roas                  REAL,
    cost_per_visitor_day_usd  REAL,
    loaded_at                 TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date) ON CONFLICT REPLACE
);
"""


def ts() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def _num(val):
    """Strip $, comma, % and any trailing unit text (' Days', etc.), return
    the leading numeric value as a float, or None if nothing parses.
    Percent strings are returned as the printed percent number (e.g. '0.42%'
    -> 0.42), not divided down to a fraction — matches how these CSVs print
    their own rates elsewhere (Trip Share, Attribution Rate, CTR)."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None
    s = s.replace("$", "").replace(",", "").replace("%", "").strip()
    m = re.match(r"-?\d+(\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _int(val):
    f = _num(val)
    return None if f is None else int(round(f))


_FILENAME_DATE_RX = [
    (re.compile(r"(20\d{2})[-_](\d{2})[-_](\d{2})"), "ymd"),   # 2026-09-07
    (re.compile(r"(?<!\d)(\d{2})_(\d{2})_(\d{2})(?!\d)"), "mdy"),  # 09_07_26
]


def _snapshot_date(path: str) -> str:
    """Candidate snapshot date for a newly seen export: a date embedded in the
    filename when there is one, otherwise the file's mtime. The mtime is only
    trustworthy on the machine the file was saved to; a fresh git checkout
    (GitHub Actions, Railway, any clone) stamps every file with the checkout
    time. _stable_snapshot() below guards against that by reusing the date
    already on record whenever the content has not changed."""
    name = os.path.basename(path)
    for rx, order in _FILENAME_DATE_RX:
        m = rx.search(name)
        if not m:
            continue
        try:
            if order == "ymd":
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            else:
                mo, d, y = int(m.group(1)), int(m.group(2)), 2000 + int(m.group(3))
            return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d")


def _norm_row(row) -> tuple:
    out = []
    for v in row:
        if isinstance(v, float):
            out.append(round(v, 6))
        else:
            out.append(v)
    return tuple(out)


def _stable_snapshot(cur, table: str, value_cols: list[str], rows: list[tuple], candidate: str) -> str:
    """Return the snapshot date to write `rows` under.

    If the newest snapshot already in `table` holds exactly the same values,
    reuse that date, so re-running the loader on an unchanged export (every
    CI run checks the repo out fresh) never mints a duplicate snapshot dated
    to the run. Found 2026-09-25: the mtime-only version created a second,
    identical "2026-09-25" snapshot on a fresh clone, which doubled the rows
    and made the dashboard claim the campaign data was current as of the run
    date rather than the export date."""
    latest = cur.execute(f"SELECT MAX(snapshot_date) FROM {table}").fetchone()[0]
    if latest:
        existing = cur.execute(
            f"SELECT {', '.join(value_cols)} FROM {table} WHERE snapshot_date = ?", (latest,)
        ).fetchall()
        if sorted(_norm_row(r) for r in existing) == sorted(_norm_row(r) for r in rows):
            return latest
    return candidate


def _write_snapshot(cur, table: str, value_cols: list[str], rows: list[tuple], candidate: str) -> str:
    """Delete-then-insert one snapshot of `rows` (value columns only, without
    snapshot_date) under a stable snapshot date. Returns the date used."""
    snap = _stable_snapshot(cur, table, value_cols, rows, candidate)
    cur.execute(f"DELETE FROM {table} WHERE snapshot_date = ?", (snap,))
    placeholders = ",".join("?" for _ in range(len(value_cols) + 1))
    cur.executemany(
        f"INSERT INTO {table} (snapshot_date, {', '.join(value_cols)}) VALUES ({placeholders})",
        [(snap, *r) for r in rows],
    )
    return snap


def _find_file(regex_pattern: str):
    """Newest file in DATAFY_DIR (top level only) matching regex_pattern,
    case-insensitive, by mtime. Returns None if nothing matches."""
    if not os.path.isdir(DATAFY_DIR):
        return None
    rx = re.compile(regex_pattern, re.IGNORECASE)
    candidates = [
        os.path.join(DATAFY_DIR, f) for f in os.listdir(DATAFY_DIR)
        if os.path.isfile(os.path.join(DATAFY_DIR, f)) and rx.match(f)
    ]
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def _read_metric_value_csv(path: str) -> dict:
    """Parse a two-column 'Metric,Value' CSV into {metric: raw_value}."""
    df = pd.read_csv(path)
    out = {}
    for _, row in df.iterrows():
        key = str(row.iloc[0]).strip()
        out[key] = row.iloc[1]
    return out


def load_top_markets(conn, path) -> int:
    df = pd.read_csv(path)
    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        dma = str(row.get("DMA", "")).strip()
        if not dma or dma.lower() == "nan":
            continue
        rows.append((
            dma, _num(row.get("Trip Share")), _int(row.get("Trips")),
            _int(row.get("Visitor Days")), _num(row.get("Avg Trip Length")),
            _num(row.get("Est Impact")), _num(row.get("Spend Per Visitor")),
        ))
    _write_snapshot(
        cur, "datafy_advertising_top_markets",
        ["dma", "trip_share_pct", "trips", "visitor_days",
         "avg_trip_length_days", "est_impact_usd", "spend_per_visitor_usd"],
        rows, _snapshot_date(path),
    )
    conn.commit()
    return len(rows)


def load_tactic_performance(conn, path) -> int:
    df = pd.read_csv(path)
    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        tactic = str(row.get("Tactic", "")).strip()
        if not tactic or tactic.lower() == "nan":
            continue
        rows.append((tactic, _num(row.get("Attribution Rate"))))
    _write_snapshot(
        cur, "datafy_advertising_tactic_performance",
        ["tactic", "attribution_rate_pct"], rows, _snapshot_date(path),
    )
    conn.commit()
    return len(rows)


def load_attribution_groups(conn, path) -> int:
    df = pd.read_csv(path)
    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        group = str(row.get("Attribution Group", "")).strip()
        if not group or group.lower() == "nan":
            continue
        rows.append((
            group, _int(row.get("Est. Trips")), _int(row.get("Est. Visitor Days")),
            _num(row.get("Avg. Length of Stay")), _num(row.get("Est. Campaign Impact")),
        ))
    _write_snapshot(
        cur, "datafy_advertising_attribution_groups",
        ["attribution_group", "est_trips", "est_visitor_days",
         "avg_length_of_stay_days", "est_campaign_impact_usd"],
        rows, _snapshot_date(path),
    )
    conn.commit()
    return len(rows)


def load_kpis(conn, path) -> int:
    m = _read_metric_value_csv(path)
    cur = conn.cursor()
    _write_snapshot(
        cur, "datafy_advertising_kpis",
        ["total_impressions", "total_clicks", "total_spend_usd", "unique_reach",
         "avg_display_ctr_pct", "avg_native_ctr_pct", "avg_vcr_acr_pct",
         "total_video_audio_completes"],
        [(
            _int(m.get("Total Impressions")), _int(m.get("Total Clicks")),
            _num(m.get("Total Spend")), _int(m.get("Unique Reach")),
            _num(m.get("Avg Display CTR")), _num(m.get("Avg Native CTR")),
            _num(m.get("Avg VCR/ACR")), _int(m.get("Total Video/Audio Completes")),
        )],
        _snapshot_date(path),
    )
    conn.commit()
    return 1


def load_line_item_performance(conn, path) -> int:
    df = pd.read_csv(path)
    cur = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        name = str(row.get("Line Item Name", "")).strip()
        if not name or name.lower() == "nan":
            continue
        # CTR here is a bare decimal fraction (e.g. 0.00404...), unlike the
        # already-a-percent strings in the other Advertising files — scale
        # up to a percent for a consistent *_pct convention across this loader.
        ctr_frac = _num(row.get("CTR"))
        ctr_pct = round(ctr_frac * 100, 4) if ctr_frac is not None else None
        rows.append((
            name, _int(row.get("Impressions")), _int(row.get("Clicks")),
            ctr_pct, _num(row.get("Total Spend")), _num(row.get("VCR/ACR")),
        ))
    _write_snapshot(
        cur, "datafy_advertising_line_item_performance",
        ["line_item_name", "impressions", "clicks", "ctr_pct",
         "total_spend_usd", "vcr_acr_pct"],
        rows, _snapshot_date(path),
    )
    conn.commit()
    return len(rows)


def load_overview(conn, path) -> int:
    m = _read_metric_value_csv(path)
    cur = conn.cursor()

    roas_raw = str(m.get("Est. ROAS", "")).strip()
    # "$1.84: $1" → 1.84 (dollars of estimated impact per dollar spent)
    roas_match = re.match(r"\$?\s*([\d.]+)\s*:\s*\$?\s*1", roas_raw)
    roas = float(roas_match.group(1)) if roas_match else _num(roas_raw)

    _write_snapshot(
        cur, "datafy_advertising_overview",
        ["est_campaign_impact_usd", "est_roas", "cost_per_visitor_day_usd"],
        [(_num(m.get("Est. Campaign Impact")), roas, _num(m.get("Cost/Visitor Day")))],
        _snapshot_date(path),
    )
    conn.commit()
    return 1


def log_load(conn, grain, file_name, rows):
    conn.execute(
        "INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at) "
        "VALUES ('Datafy', ?, ?, ?, datetime('now'))",
        (grain, file_name, rows),
    )
    conn.commit()


def main():
    print(f"{ts()} [START] load_datafy_advertising.py")
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript(DDL)
    conn.commit()

    targets = [
        ("advertising_top_markets", r"advertising_overview_topmarketsvisual.*\.csv$", load_top_markets),
        ("advertising_tactic_performance", r"advertising_overview_topperformersvisual.*\.csv$", load_tactic_performance),
        ("advertising_attribution_groups", r"advertising_shared_attributiongroupssummaryvisual.*\.csv$", load_attribution_groups),
        ("advertising_kpis", r"advertising_shared_traditionalkpisvisual.*\.csv$", load_kpis),
        ("advertising_line_item_performance", r"advertising_traditionaladvertisingmetrics_lineitemperformancevisual.*\.csv$", load_line_item_performance),
        ("advertising_overview", r"overview dashboard_export.*\.csv$", load_overview),
    ]

    for label, pattern, loader in targets:
        path = _find_file(pattern)
        if not path:
            print(f"{ts()} [SKIP] no file found for {label} (pattern: {pattern})")
            continue
        try:
            n = loader(conn, path)
            print(f"{ts()} [{'OK  ' if n else 'WARN'}] {label}: {n} rows from {os.path.basename(path)}")
            log_load(conn, label, os.path.basename(path), n)
        except Exception as exc:
            print(f"{ts()} [ERR ] {label} ({os.path.basename(path)}): {exc}")

    conn.close()
    print(f"{ts()} [DONE] load_datafy_advertising.py complete")


if __name__ == "__main__":
    main()
