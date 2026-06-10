import os
import sqlite3

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
STR_DIR = os.path.join(PROJECT_ROOT, "data", "str")
# Primary file (legacy / manual drop)
DAILY_FILE = os.path.join(STR_DIR, "str_daily.xlsx")
# Dropbox-synced weekly exports land here (multiple files, any # of sheets)
WEEKLY_DIR = os.path.join(STR_DIR, "weekly")


def get_connection():
    return sqlite3.connect(DB_PATH, timeout=10)


def normalize_str_daily(df):
    """
    Normalize STR DAILY export with columns:
    ['Period', 'Day of Week', 'Supply', 'Supply Chg (YOY)', 'Demand',
     'Demand Chg (YOY)', 'Revenue', 'Revenue Chg (YOY)', 'Occupancy',
     'Occupancy Chg (YOY)', 'ADR', 'ADR Chg (YOY)', 'RevPAR',
     'RevPAR Chg (YOY)', ...]

    NOTE: Occupancy stored as decimal in fact_str_metrics (0.688 = 68.8%).
    kpi_daily_summary multiplies by 100 to get occ_pct for display.
    """
    date_col = "Period"

    metric_columns = {
        "Supply": ("supply", "rooms"),
        "Demand": ("demand", "rooms"),
        "Revenue": ("revenue", "USD"),
        "Occupancy": ("occ", "decimal"),  # stored as 0.0–1.0 decimal
        "ADR": ("adr", "USD"),
        "RevPAR": ("revpar", "USD"),
    }

    required = [date_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected STR columns: {missing}")

    df = df.copy()

    # Parse dates with coerce so malformed values become NaT instead of crashing
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    nat_count = df[date_col].isna().sum()
    if nat_count:
        print(f"  WARNING: {nat_count} rows have unparseable dates and will be dropped")
        df = df.dropna(subset=[date_col])
    df[date_col] = df[date_col].dt.strftime("%Y-%m-%d")

    long_frames = []
    for col_name, (metric_name, unit) in metric_columns.items():
        if col_name not in df.columns:
            continue

        tmp = df[[date_col, col_name]].copy()
        tmp.rename(columns={col_name: "metric_value"}, inplace=True)
        tmp["metric_name"] = metric_name
        tmp["unit"] = unit
        tmp["grain"] = "daily"
        tmp["source"] = "STR"
        tmp["property_name"] = "VDP Select Portfolio"
        tmp["market"] = "Anaheim Area"
        tmp["submarket"] = None
        tmp.rename(columns={date_col: "as_of_date"}, inplace=True)
        long_frames.append(tmp)

    if not long_frames:
        raise ValueError("No known metric columns found in STR daily export")

    long_df = pd.concat(long_frames, ignore_index=True)
    return long_df[
        [
            "source",
            "grain",
            "property_name",
            "market",
            "submarket",
            "as_of_date",
            "metric_name",
            "metric_value",
            "unit",
        ]
    ]


def _sorted_xlsx_files(directory: str) -> list[str]:
    """Return .xlsx/.xls files in a directory sorted oldest-first by filename.

    STR files are named with dates (e.g. VisitDanaPoint_20260426.xlsx) so
    alphabetical sort == chronological order. Oldest loads first so newer
    exports overwrite corrections for the same date.
    """
    if not os.path.isdir(directory):
        return []
    return sorted(f for f in os.listdir(directory) if f.lower().endswith((".xlsx", ".xls")))


def _read_all_sheets(fpath: str) -> list[tuple[str, str, pd.DataFrame]]:
    """Return (fpath, sheet_name, df) for every sheet that has a 'Period' column."""
    results = []
    fname = os.path.basename(fpath)
    engine = "xlrd" if fname.lower().endswith(".xls") else "openpyxl"
    try:
        xl = pd.ExcelFile(fpath, engine=engine)
        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet)
                if "Period" in df.columns:
                    results.append((fpath, sheet, df))
                else:
                    print(f"  SKIP sheet '{sheet}' in {fname} — no 'Period' column")
            except Exception as e:
                print(f"  WARN: could not parse sheet '{sheet}' in {fname}: {e}")
    except Exception as e:
        print(f"  WARN: could not open {fname}: {e}")
    return results


def _collect_daily_sources() -> list[tuple[str, str, pd.DataFrame]]:
    """Return all (fpath, sheet, df) for daily/weekly STR data, oldest file first.

    Load order:
      1. data/str/weekly/*.xlsx  — Dropbox exports, sorted oldest→newest by filename
      2. data/str/str_daily.xlsx — legacy single-file (appended last)

    Oldest-first ensures newer exports WIN on UPSERT when the same date appears
    in multiple files (STR sometimes revises prior-week values).
    """
    sources = []
    for fname in _sorted_xlsx_files(WEEKLY_DIR):
        sources.extend(_read_all_sheets(os.path.join(WEEKLY_DIR, fname)))
    # Primary daily file(s): str_daily.xlsx + any str_daily_*.xlsx dated exports
    daily_files = sorted(
        [f for f in os.listdir(STR_DIR) if f.lower().startswith("str_daily") and f.lower().endswith((".xlsx", ".xls"))]
    )
    for fname in daily_files:
        sources.extend(_read_all_sheets(os.path.join(STR_DIR, fname)))
    return sources


def _to_float(val):
    try:
        return float(val) if pd.notna(val) else None
    except (TypeError, ValueError):
        return None


def load_str_daily(conn):
    sources = _collect_daily_sources()
    if not sources:
        print(f"No STR daily data found in {WEEKLY_DIR}/ or {DAILY_FILE} — skipping")
        return 0

    file_names = sorted({os.path.basename(s[0]) for s in sources})
    print(f"Loading STR daily: {len(sources)} sheet(s) from {len(file_names)} file(s) (oldest→newest)")

    all_norm = []
    for fpath, sheet, df in sources:
        fname = os.path.basename(fpath)
        try:
            norm_df = normalize_str_daily(df)
            all_norm.append(norm_df)
            date_range = f"{norm_df['as_of_date'].min()} → {norm_df['as_of_date'].max()}"
            print(f"  {fname} [{sheet}] → {len(norm_df)} rows  ({date_range})")
        except Exception as e:
            print(f"  WARN: normalize failed for {fname} [{sheet}]: {e}")

    if not all_norm:
        print("  No normalizable data found — skipping")
        return 0

    # Concat oldest→newest; later rows win on duplicate key (desired UPSERT behavior)
    norm_df = pd.concat(all_norm, ignore_index=True)

    rows_to_upsert = []
    for _, row in norm_df.iterrows():
        val = _to_float(row["metric_value"])
        rows_to_upsert.append((
            row["source"], row["grain"], row["property_name"], row["market"],
            row["submarket"], row["as_of_date"], row["metric_name"], val, row["unit"],
        ))

    cursor = conn.cursor()
    if rows_to_upsert:
        # INSERT OR REPLACE: newer file data overwrites old values for same date.
        # Historical rows not present in new files are untouched (no DELETE).
        cursor.executemany(
            """
            INSERT OR REPLACE INTO fact_str_metrics
            (source, grain, property_name, market, submarket,
             as_of_date, metric_name, metric_value, unit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_upsert,
        )

    rows_upserted = len(rows_to_upsert)
    conn.commit()

    total = cursor.execute(
        "SELECT COUNT(*) FROM fact_str_metrics WHERE grain='daily'"
    ).fetchone()[0]

    file_label = f"weekly_dir({len(file_names)} files)"
    cursor.execute(
        "INSERT INTO load_log (source, grain, file_name, rows_inserted) VALUES (?,?,?,?)",
        ("STR", "daily", file_label, rows_upserted),
    )
    conn.commit()

    print(f"  Upserted {rows_upserted:,} rows — total daily rows in DB: {total:,}")
    return rows_upserted


def main():
    conn = get_connection()
    try:
        daily_rows = load_str_daily(conn)
        print(f"Done. Daily rows inserted: {daily_rows}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
