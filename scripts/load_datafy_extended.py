"""
load_datafy_extended.py — Datafy export families that had no table (added 2026-09-30).

The 2026-09-30 Datafy drop added report types that neither load_datafy_reports.py
(visitor-economy geo-fencing) nor load_datafy_advertising.py (campaign KPIs) maps.
They sit at the top level of data/datafy/ and were logged "no recognized report-type
match". This loader gives each family a table, snapshot-dated the same way as
load_datafy_advertising.py (date embedded in the filename, otherwise file mtime, with
_stable_snapshot reusing the date on record when the numbers have not changed).

  AdRevamp_MarketBreakdownTable_{Destination,Hotels,Resorts}_Export.csv
      -> datafy_adrevamp_market_impact
  Advertising_Destination_LengthOfStayBreakdownVisual_Export.csv  + "Attribution Insights_Export.csv"
      -> datafy_ext_length_of_stay          (source column tells them apart)
  Advertising_Destination_VisitorsByWeekdayVisual_Export.csv      + "Attribution Insights_Export (1).csv"
      -> datafy_ext_trips_by_weekday
  Advertising_Destination_MarketTableVisual_Export.csv
      -> datafy_advertising_destination_markets
  Advertising_Destination_TopPOIsVisual_Export.csv
      -> datafy_advertising_destination_pois
  Advertising_Visitor_PerformanceBreakdownTableVisual_Export.csv
      -> datafy_advertising_audience_performance
  Advertising_Visitor_DemographicVisual_Export.csv + Attribution Insights Media Visitor Demographics_Export.csv
      -> datafy_ext_visitor_demographics
  Advertising_Visitor_CostPerVisitorDayVisual / DynamicSpendVisual, Avg Spend Per Visitor-UserBoxes
      -> datafy_ext_visitor_metrics
  Attribution-Insights-Website-Performance-Table_Export.csv
      -> datafy_attribution_vendor_performance
  DailyVisitorsTrendDashboard Buckets_Export.csv
      -> datafy_visitor_days_by_year
  Enhanced Spending Insights-{Length of Stay, Repeat Spenders, Spend By Day}_Export.csv
      -> datafy_spend_by_length_of_stay, datafy_spend_repeat_split, datafy_spend_by_weekday
  Enhanced Spending Insights-Top Spending {category,market}_Export.csv (monthly history, upserted)
      -> datafy_spend_category_monthly, datafy_spend_market_monthly
  Geolocation-VisitationByMonthAndDay_Export.csv -> datafy_visitation_heatmap
  TrendsDensityMap_Export.csv                    -> datafy_trip_share_by_dma

Deliberately not loaded:
  AttributionInsightsVisitorMediaAttributionGroups_Export (n).csv — the export carries no
      attribution-group name, so its rows cannot be labelled without guessing.
  Enhanced Spend Correlation Insights_Export (n).csv — correlation columns are all "None".

Skip-safe: a missing or unparseable file is logged and skipped, never raised.
"""

import os
import re
import sqlite3
import sys
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from load_datafy_advertising import (  # noqa: E402  (shared parsing + snapshot helpers)
    DATAFY_DIR, DB_PATH, FD, PROJECT_ROOT, _int, _num, _snapshot_date, _write_snapshot, log_load, ts,
)

DDL = """
CREATE TABLE IF NOT EXISTS datafy_adrevamp_market_impact (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    segment TEXT NOT NULL, dma TEXT NOT NULL, est_impact_usd REAL, share_of_impact_pct REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, segment, dma) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_ext_length_of_stay (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    source TEXT NOT NULL, length_of_stay TEXT NOT NULL, share_of_trips_pct REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, source, length_of_stay) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_ext_trips_by_weekday (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    source TEXT NOT NULL, day TEXT NOT NULL, trips REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, source, day) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_advertising_destination_markets (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    dma TEXT NOT NULL, est_trips INTEGER, attribution_rate_pct REAL,
    avg_length_of_stay_days REAL, est_impact_usd REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, dma) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_advertising_destination_pois (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    area TEXT NOT NULL, polygon_ids TEXT NOT NULL, attribution_group TEXT NOT NULL,
    trips REAL, share_of_trips_pct REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, area, polygon_ids, attribution_group) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_advertising_audience_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    audience_segment TEXT NOT NULL, est_trips INTEGER, attribution_rate_pct REAL,
    avg_length_of_stay_days REAL, est_impact_usd REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, audience_segment) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_ext_visitor_demographics (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    source TEXT NOT NULL, category TEXT NOT NULL, bucket TEXT NOT NULL, share_pct REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, source, category, bucket) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_ext_visitor_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    metric TEXT NOT NULL, value REAL, source_file TEXT,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, metric) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_attribution_vendor_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    vendor TEXT NOT NULL, attribution_pct REAL, unique_reach INTEGER, impressions INTEGER,
    clicks INTEGER, ctr_pct REAL, click_conversion_rate_pct REAL, total_trips INTEGER,
    est_impact_usd REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, vendor) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_visitor_days_by_year (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    year INTEGER NOT NULL, visitor_days INTEGER, visitor_days_vs_prev_year_pct REAL,
    trips INTEGER, trips_vs_prev_year_pct REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, year) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_spend_by_length_of_stay (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    length_of_stay TEXT NOT NULL, pct_of_spend REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, length_of_stay) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_spend_repeat_split (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    repeat_pct REAL, one_time_pct REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_spend_by_weekday (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    day TEXT NOT NULL, spend_volume REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, day) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_visitation_heatmap (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    day_of_week TEXT NOT NULL, month TEXT NOT NULL, pct_of_max_visitor_days REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, day_of_week, month) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_trip_share_by_dma (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL,
    dma TEXT NOT NULL, share_of_trips_pct REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, dma) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_spend_category_monthly (
    name TEXT NOT NULL, month TEXT NOT NULL, spend_volume REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (name, month) ON CONFLICT REPLACE
);
CREATE TABLE IF NOT EXISTS datafy_spend_market_monthly (
    name TEXT NOT NULL, month TEXT NOT NULL, spend_volume REAL,
    loaded_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (name, month) ON CONFLICT REPLACE
);
"""


# ── file discovery ───────────────────────────────────────────────────────────

def _candidates(pattern: str, header_prefix: str | None = None):
    """Top-level CSVs matching `pattern` (case-insensitive), newest first by DATE ADDED.
    `header_prefix` additionally requires the first line to start with it, which
    separates same-named exports that carry different reports (Attribution Insights_Export)."""
    if not os.path.isdir(DATAFY_DIR):
        return []
    rx = re.compile(pattern, re.IGNORECASE)
    out = []
    for f in os.listdir(DATAFY_DIR):
        p = os.path.join(DATAFY_DIR, f)
        if not (os.path.isfile(p) and rx.match(f)):
            continue
        if header_prefix:
            try:
                with open(p, encoding="utf-8-sig") as fh:
                    first = fh.readline().strip().lower()
            except OSError:
                continue
            if not first.startswith(header_prefix.lower()):
                continue
        out.append(p)
    if FD is not None:
        return sorted(out, key=lambda p: FD.sort_key(p, PROJECT_ROOT), reverse=True)   # newest DATE ADDED first
    return sorted(out, key=os.path.getmtime, reverse=True)


def _newest(pattern: str, header_prefix: str | None = None):
    c = _candidates(pattern, header_prefix)
    return c[0] if c else None


def _read(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def _clean(v) -> str:
    return str(v).strip()


def _pct_value(v):
    """'11.33%' -> 11.33 ; bare fraction 0.1798 -> 17.98."""
    s = _clean(v)
    if not s:
        return None
    if s.endswith("%"):
        return _num(s)
    n = _num(s)
    return None if n is None else round(n * 100, 4)


def _write(conn, table, cols, rows, files):
    """Write one combined snapshot; candidate date is the newest source file's date."""
    if not rows:
        return 0
    cand = max(_snapshot_date(p) for p in files)
    _write_snapshot(conn.cursor(), table, cols, rows, cand)
    conn.commit()
    return len(rows)


# ── loaders (each returns (rows_written, source_file_names)) ─────────────────

def load_adrevamp(conn):
    rows, files = [], []
    for seg in ("Destination", "Hotels", "Resorts"):
        p = _newest(rf"adrevamp_marketbreakdowntable_{seg}_export.*\.csv$")
        if not p:
            continue
        files.append(p)
        for _, r in _read(p).iterrows():
            dma = _clean(r.get("DMA", ""))
            if dma:
                rows.append((seg, dma, _num(r.get("Est. Impact")), _num(r.get("Share of Impact"))))
    n = _write(conn, "datafy_adrevamp_market_impact",
               ["segment", "dma", "est_impact_usd", "share_of_impact_pct"], rows, files)
    return n, files


def load_length_of_stay(conn):
    rows, files = [], []
    for source, pat, hdr in (
        ("advertising_destination", r"advertising_destination_lengthofstaybreakdownvisual_export.*\.csv$", None),
        ("attribution_insights", r"attribution insights_export.*\.csv$", "Length of Stay"),
    ):
        p = _newest(pat, hdr)
        if p:
            files.append(p)
            for _, r in _read(p).iterrows():
                los = _clean(r.get("Length of Stay", ""))
                if los:
                    rows.append((source, los, _num(r.get("Share of Trips"))))
    n = _write(conn, "datafy_ext_length_of_stay",
               ["source", "length_of_stay", "share_of_trips_pct"], rows, files)
    return n, files


def load_trips_by_weekday(conn):
    rows, files = [], []
    for source, pat, hdr in (
        ("advertising_destination", r"advertising_destination_visitorsbyweekdayvisual_export.*\.csv$", None),
        ("attribution_insights", r"attribution insights_export.*\.csv$", "Day,Number of Trips"),
    ):
        p = _newest(pat, hdr)
        if p:
            files.append(p)
            for _, r in _read(p).iterrows():
                day = _clean(r.get("Day", ""))
                if day:
                    rows.append((source, day, _num(r.get("Number of Trips"))))
    n = _write(conn, "datafy_ext_trips_by_weekday", ["source", "day", "trips"], rows, files)
    return n, files


def load_destination_markets(conn):
    p = _newest(r"advertising_destination_markettablevisual_export.*\.csv$")
    if not p:
        return 0, []
    df = _read(p)
    rows = []
    for _, r in df.iterrows():
        dma = _clean(r.iloc[0])
        if dma:
            # last header cell is blank in the export; it holds the Destination Est. Impact dollars
            rows.append((dma, _int(r.iloc[1]), _num(r.iloc[2]), _num(r.iloc[3]), _num(r.iloc[4])))
    n = _write(conn, "datafy_advertising_destination_markets",
               ["dma", "est_trips", "attribution_rate_pct", "avg_length_of_stay_days", "est_impact_usd"], rows, [p])
    return n, [p]


def load_destination_pois(conn):
    p = _newest(r"advertising_destination_toppoisvisual_export.*\.csv$")
    if not p:
        return 0, []
    rows = []
    for _, r in _read(p).iterrows():
        area = _clean(r.get("Area", ""))
        if area:
            rows.append((area, _clean(r.get("Polygon ID", "")), _clean(r.get("Attribution Group", "")),
                         _num(r.get("Trips")), _num(r.get("Share of Trips"))))
    n = _write(conn, "datafy_advertising_destination_pois",
               ["area", "polygon_ids", "attribution_group", "trips", "share_of_trips_pct"], rows, [p])
    return n, [p]


def load_audience_performance(conn):
    p = _newest(r"advertising_visitor_performancebreakdowntablevisual_export.*\.csv$")
    if not p:
        return 0, []
    rows = []
    for _, r in _read(p).iterrows():
        seg = _clean(r.get("Audience Segment Name", ""))
        if seg:
            rows.append((seg, _int(r.get("Est. Trips Destination")), _num(r.get("Attribution Rate")),
                         _num(r.get("Avg. Length of Stay Destination")), _num(r.get("Destination Est. Impact"))))
    n = _write(conn, "datafy_advertising_audience_performance",
               ["audience_segment", "est_trips", "attribution_rate_pct", "avg_length_of_stay_days", "est_impact_usd"],
               rows, [p])
    return n, [p]


def load_visitor_demographics(conn):
    rows, files = [], []
    p = _newest(r"advertising_visitor_demographicvisual_export.*\.csv$")
    if p:
        files.append(p)
        for _, r in _read(p).iterrows():
            label = _clean(r.iloc[0])
            if " - " in label:
                cat, bucket = label.split(" - ", 1)
                rows.append(("advertising_visitor", cat.strip().title(), bucket.strip(), _pct_value(r.iloc[1])))
    p = _newest(r"attribution insights media visitor demographics_export.*\.csv$")
    if p:
        files.append(p)
        for _, r in _read(p).iterrows():
            cat, bucket = _clean(r.get("Demographic", "")), _clean(r.get("label", ""))
            if cat and bucket:
                rows.append(("media_attribution", cat.title(), bucket, _pct_value(r.get("share"))))
    n = _write(conn, "datafy_ext_visitor_demographics",
               ["source", "category", "bucket", "share_pct"], rows, files)
    return n, files


def load_visitor_metrics(conn):
    rows, files = [], []
    for metric, pat in (
        ("cost_per_visitor_day_usd", r"advertising_visitor_costpervisitordayvisual_export.*\.csv$"),
        ("advertising_visitor_spend_per_visitor_usd", r"advertising_visitor_dynamicspendvisual_export.*\.csv$"),
        ("avg_spend_per_visitor_usd", r"avg spend per visitor-userboxes_export.*\.csv$"),
    ):
        p = _newest(pat)
        if not p:
            continue
        df = _read(p)
        if len(df):
            files.append(p)
            rows.append((metric, _num(df.iloc[0, 0]), os.path.basename(p)))
    n = _write(conn, "datafy_ext_visitor_metrics", ["metric", "value", "source_file"], rows, files)
    return n, files


def load_vendor_performance(conn):
    p = _newest(r"attribution-insights-website-performance-table_export.*\.csv$")
    if not p:
        return 0, []
    rows = []
    for _, r in _read(p).iterrows():
        vendor = _clean(r.get("Vendor", ""))
        if vendor:
            rows.append((vendor, _num(r.get("attribution")), _int(r.get("unique_reach")),
                         _int(r.get("impressions")), _int(r.get("clicks")), _num(r.get("ctr")),
                         _num(r.get("click_conversion_rate")), _int(r.get("total_trips")),
                         _num(r.get("est_impact"))))
    n = _write(conn, "datafy_attribution_vendor_performance",
               ["vendor", "attribution_pct", "unique_reach", "impressions", "clicks", "ctr_pct",
                "click_conversion_rate_pct", "total_trips", "est_impact_usd"], rows, [p])
    return n, [p]


def load_visitor_days_by_year(conn):
    p = _newest(r"dailyvisitorstrenddashboard buckets_export.*\.csv$")
    if not p:
        return 0, []
    rows = []
    for _, r in _read(p).iterrows():
        yr = _int(r.get("Year"))
        if yr:
            rows.append((yr, _int(r.get("Visitor Days")), _num(r.get("Visitor Days Compared to Previous Year")),
                         _int(r.get("Trips")), _num(r.get("Trips Compared to Previous Year"))))
    n = _write(conn, "datafy_visitor_days_by_year",
               ["year", "visitor_days", "visitor_days_vs_prev_year_pct", "trips", "trips_vs_prev_year_pct"], rows, [p])
    return n, [p]


def load_spend_by_los(conn):
    p = _newest(r"enhanced spending insights-length of stay_export.*\.csv$")
    if not p:
        return 0, []
    rows = [(_clean(r.get("Length of Stay", "")), _pct_value(r.get("% of Spend")))
            for _, r in _read(p).iterrows() if _clean(r.get("Length of Stay", ""))]
    n = _write(conn, "datafy_spend_by_length_of_stay", ["length_of_stay", "pct_of_spend"], rows, [p])
    return n, [p]


def load_repeat_split(conn):
    p = _newest(r"enhanced spending insights-repeat spenders_export.*\.csv$")
    if not p:
        return 0, []
    df = _read(p)
    if not len(df):
        return 0, [p]
    r = df.iloc[0]
    n = _write(conn, "datafy_spend_repeat_split", ["repeat_pct", "one_time_pct"],
               [(_pct_value(r.get("Repeat")), _pct_value(r.get("One Time")))], [p])
    return n, [p]


def load_spend_by_weekday(conn):
    p = _newest(r"enhanced spending insights-spend by day_export.*\.csv$")
    if not p:
        return 0, []
    rows = [(_clean(r.get("Day", "")), _num(r.get("Spend Volume")))
            for _, r in _read(p).iterrows() if _clean(r.get("Day", ""))]
    n = _write(conn, "datafy_spend_by_weekday", ["day", "spend_volume"], rows, [p])
    return n, [p]


def load_heatmap(conn):
    p = _newest(r"geolocation-visitationbymonthandday_export.*\.csv$")
    if not p:
        return 0, []
    rows = [(_clean(r.get("Day", "")), _clean(r.get("Month", "")), _num(r.get("Compared to Max Visitor Days Value")))
            for _, r in _read(p).iterrows() if _clean(r.get("Day", "")) and _clean(r.get("Month", ""))]
    n = _write(conn, "datafy_visitation_heatmap",
               ["day_of_week", "month", "pct_of_max_visitor_days"], rows, [p])
    return n, [p]


def load_trip_share_dma(conn):
    p = _newest(r"trendsdensitymap_export.*\.csv$")
    if not p:
        return 0, []
    rows = [(_clean(r.get("DMA", "")), _num(r.get("Share of Trips")))
            for _, r in _read(p).iterrows() if _clean(r.get("DMA", ""))]
    n = _write(conn, "datafy_trip_share_by_dma", ["dma", "share_of_trips_pct"], rows, [p])
    return n, [p]


def _load_monthly(conn, pattern, table):
    """Monthly history: upsert on (name, month) so months absent from a later, shorter export survive."""
    p = _newest(pattern)
    if not p:
        return 0, []
    rows = [(_clean(r.get("Name", "")), _clean(r.get("Month", ""))[:10], _num(r.get("Spend Volume")))
            for _, r in _read(p).iterrows() if _clean(r.get("Name", "")) and _clean(r.get("Month", ""))]
    conn.executemany(f"INSERT OR REPLACE INTO {table} (name, month, spend_volume) VALUES (?,?,?)", rows)
    conn.commit()
    return len(rows), [p]


def load_category_monthly(conn):
    return _load_monthly(conn, r"enhanced spending insights-top spending category_export.*\.csv$",
                         "datafy_spend_category_monthly")


def load_market_monthly(conn):
    return _load_monthly(conn, r"enhanced spending insights-top spending market_export.*\.csv$",
                         "datafy_spend_market_monthly")


LOADERS = [
    ("adrevamp_market_impact", load_adrevamp),
    ("ext_length_of_stay", load_length_of_stay),
    ("ext_trips_by_weekday", load_trips_by_weekday),
    ("advertising_destination_markets", load_destination_markets),
    ("advertising_destination_pois", load_destination_pois),
    ("advertising_audience_performance", load_audience_performance),
    ("ext_visitor_demographics", load_visitor_demographics),
    ("ext_visitor_metrics", load_visitor_metrics),
    ("attribution_vendor_performance", load_vendor_performance),
    ("visitor_days_by_year", load_visitor_days_by_year),
    ("spend_by_length_of_stay", load_spend_by_los),
    ("spend_repeat_split", load_repeat_split),
    ("spend_by_weekday", load_spend_by_weekday),
    ("visitation_heatmap", load_heatmap),
    ("trip_share_by_dma", load_trip_share_dma),
    ("spend_category_monthly", load_category_monthly),
    ("spend_market_monthly", load_market_monthly),
]


def main():
    print(f"{ts()} [START] load_datafy_extended.py")
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript(DDL)
    conn.commit()
    for label, fn in LOADERS:
        try:
            n, files = fn(conn)
        except Exception as exc:  # skip-safe: one bad export must not stop the rest
            print(f"{ts()} [ERR ] {label}: {exc}")
            continue
        if not files:
            print(f"{ts()} [SKIP] {label}: no matching file in data/datafy/")
            continue
        names = ", ".join(os.path.basename(f) for f in files)
        print(f"{ts()} [{'OK  ' if n else 'WARN'}] {label}: {n} rows from {names}")
        try:
            log_load(conn, label, names[:250], n)
        except Exception:
            pass
    conn.close()
    print(f"{ts()} [DONE] load_datafy_extended.py complete")


if __name__ == "__main__":
    main()
