"""
load_str_daily_sqlite.py
------------------------
Scans data/str/ for ALL STR daily Excel exports and loads any new files
into fact_str_metrics (grain='daily').

File detection:
  - Scans data/str/*.xlsx for files with a "Day of Week" column (daily signature)
  - Skips files already recorded in load_log (by filename) to prevent re-loading
  - New files drop into data/str/ and are picked up automatically on next run

Run:
    python scripts/load_str_daily_sqlite.py
"""

import glob
import os
import sqlite3

import pandas as pd

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
STR_DIR      = os.path.join(PROJECT_ROOT, "data", "str")


def get_connection():
    return sqlite3.connect(DB_PATH)


def already_loaded(conn, filename: str) -> bool:
    """Return True if this filename was already loaded into load_log."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM load_log WHERE source='STR' AND grain='daily' AND file_name=?",
        (filename,),
    )
    return cur.fetchone()[0] > 0


def is_daily_format(df: pd.DataFrame) -> bool:
    """Detect daily STR format by presence of 'Day of Week' column."""
    return any("day of week" in str(c).lower() for c in df.columns)


def normalize_str_daily(df: pd.DataFrame) -> pd.DataFrame:
    date_col = "Period"
    metric_columns = {
        "Supply":    ("supply",  "rooms"),
        "Demand":    ("demand",  "rooms"),
        "Revenue":   ("revenue", "USD"),
        "Occupancy": ("occ",     "percent"),
        "ADR":       ("adr",     "USD"),
        "RevPAR":    ("revpar",  "USD"),
    }

    if date_col not in df.columns:
        raise ValueError(f"Missing expected column '{date_col}'")

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")

    long_frames = []
    for col_name, (metric_name, unit) in metric_columns.items():
        if col_name not in df.columns:
            continue
        tmp = df[[date_col, col_name]].copy()
        tmp[col_name] = pd.to_numeric(
            tmp[col_name].astype(str).str.strip().replace("-", None),
            errors="coerce",
        )
        tmp.rename(columns={col_name: "metric_value"}, inplace=True)
        tmp["metric_name"]   = metric_name
        tmp["unit"]          = unit
        tmp["grain"]         = "daily"
        tmp["source"]        = "STR"
        tmp["property_name"] = "VDP Select Portfolio"
        tmp["market"]        = "Anaheim Area"
        tmp["submarket"]     = None
        tmp.rename(columns={date_col: "as_of_date"}, inplace=True)
        long_frames.append(tmp)

    if not long_frames:
        raise ValueError("No known metric columns found in STR daily export")

    return pd.concat(long_frames, ignore_index=True)[
        ["source","grain","property_name","market","submarket",
         "as_of_date","metric_name","metric_value","unit"]
    ]


def load_str_daily(path: str, conn: sqlite3.Connection) -> int:
    filename = os.path.basename(path)
    print(f"Loading STR daily: {filename}")
    df = pd.read_excel(path, engine="openpyxl")

    if not is_daily_format(df):
        print(f"  ↳ Skipping {filename} — no 'Day of Week' column (not daily format)")
        return 0

    norm_df = normalize_str_daily(df)
    cursor  = conn.cursor()
    rows_inserted = 0

    for _, row in norm_df.iterrows():
        cursor.execute(
            """SELECT COUNT(*) FROM fact_str_metrics
               WHERE source=? AND grain=? AND property_name=? AND market=?
                 AND as_of_date=? AND metric_name=?""",
            (row["source"], row["grain"], row["property_name"],
             row["market"], row["as_of_date"], row["metric_name"]),
        )
        if cursor.fetchone()[0]:
            continue
        metric_val = row["metric_value"]
        cursor.execute(
            """INSERT INTO fact_str_metrics
               (source,grain,property_name,market,submarket,
                as_of_date,metric_name,metric_value,unit)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (row["source"], row["grain"], row["property_name"], row["market"],
             row["submarket"], row["as_of_date"], row["metric_name"],
             float(metric_val) if pd.notna(metric_val) else None,
             row["unit"]),
        )
        rows_inserted += 1

    conn.commit()
    cursor.execute(
        "INSERT INTO load_log (source,grain,file_name,rows_inserted) VALUES (?,?,?,?)",
        ("STR", "daily", filename, rows_inserted),
    )
    conn.commit()
    print(f"  ✓ {filename} → {rows_inserted} new rows")
    return rows_inserted


def main():
    os.makedirs(STR_DIR, exist_ok=True)
    files = sorted(glob.glob(os.path.join(STR_DIR, "*.xlsx")))

    if not files:
        print(f"No xlsx files in {STR_DIR} — place STR daily exports here and rerun.")
        return

    conn  = get_connection()
    total = 0

    for path in files:
        filename = os.path.basename(path)
        if already_loaded(conn, filename):
            print(f"  ↳ Skipping {filename} — already in load_log")
            continue
        try:
            total += load_str_daily(path, conn)
        except Exception as exc:
            print(f"  ✗ Error loading {filename}: {exc}")

    conn.close()
    print(f"Done. Total daily rows inserted: {total}")


if __name__ == "__main__":
    main()
