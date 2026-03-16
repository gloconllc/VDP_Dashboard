"""
load_str_monthly_sqlite.py
--------------------------
Scans data/str/ for ALL STR monthly Excel exports and loads any new files
into fact_str_metrics (grain='monthly').

File detection:
  - Scans data/str/*.xlsx for files WITHOUT a "Day of Week" column (monthly signature)
  - Skips files already recorded in load_log (by filename) to prevent re-loading
  - New files drop into data/str/ and are picked up automatically on next run

Run:
    python scripts/load_str_monthly_sqlite.py
"""

import glob
import os
import sqlite3

import pandas as pd

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
STR_DIR      = os.path.join(PROJECT_ROOT, "data", "str")


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def already_loaded(conn, filename: str) -> bool:
    """Return True if this filename was already loaded into load_log."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM load_log WHERE source='STR' AND grain='monthly' AND file_name=?",
        (filename,),
    )
    return cur.fetchone()[0] > 0


def is_monthly_format(df: pd.DataFrame) -> bool:
    """Monthly STR exports do NOT have a 'Day of Week' column."""
    has_period = any("period" in str(c).lower() for c in df.columns)
    has_dow    = any("day of week" in str(c).lower() for c in df.columns)
    return has_period and not has_dow


def normalize_str_monthly(df: pd.DataFrame) -> pd.DataFrame:
    date_col = "Period"
    metric_columns = {
        "Supply":  ("supply",  "rooms"),
        "Demand":  ("demand",  "rooms"),
        "Revenue": ("revenue", "USD"),
        "Occ":     ("occ",     "percent"),
        "Occ %":   ("occ",     "percent"),
        "ADR":     ("adr",     "USD"),
        "RevPAR":  ("revpar",  "USD"),
    }

    if date_col not in df.columns:
        raise ValueError(f"Missing expected column '{date_col}'")

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")

    for col_name in metric_columns:
        if col_name in df.columns:
            s = df[col_name].astype(str).str.strip().replace("-", None)
            df[col_name] = pd.to_numeric(s, errors="coerce")

    frames = []
    seen_metrics = set()  # avoid duplicating occ from both "Occ" and "Occ %"

    for col_name, (metric_name, unit) in metric_columns.items():
        if col_name not in df.columns:
            continue
        if metric_name in seen_metrics:
            continue
        seen_metrics.add(metric_name)
        tmp = df[[date_col, col_name]].copy()
        tmp.rename(columns={col_name: "metric_value"}, inplace=True)
        tmp["metric_name"]   = metric_name
        tmp["unit"]          = unit
        tmp["grain"]         = "monthly"
        tmp["source"]        = "STR"
        tmp["property_name"] = "VDP Select Portfolio"
        tmp["market"]        = "Anaheim Area"
        tmp["submarket"]     = None
        tmp.rename(columns={date_col: "as_of_date"}, inplace=True)
        frames.append(tmp)

    if not frames:
        raise ValueError("No known metric columns found in STR monthly export")

    return pd.concat(frames, ignore_index=True)[
        ["source","grain","property_name","market","submarket",
         "as_of_date","metric_name","metric_value","unit"]
    ]


def safe_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_str_monthly(path: str, conn: sqlite3.Connection) -> int:
    filename = os.path.basename(path)
    print(f"Loading STR monthly: {filename}")
    df = pd.read_excel(path, engine="openpyxl")

    if not is_monthly_format(df):
        print(f"  ↳ Skipping {filename} — 'Day of Week' detected (looks daily, not monthly)")
        return 0

    norm_df = normalize_str_monthly(df)
    cur     = conn.cursor()
    rows_inserted = 0

    for _, row in norm_df.iterrows():
        cur.execute(
            """SELECT COUNT(*) FROM fact_str_metrics
               WHERE source=? AND grain=? AND property_name=? AND market=?
                 AND as_of_date=? AND metric_name=?""",
            (row["source"], row["grain"], row["property_name"],
             row["market"], row["as_of_date"], row["metric_name"]),
        )
        if cur.fetchone()[0]:
            continue
        cur.execute(
            """INSERT INTO fact_str_metrics
               (source,grain,property_name,market,submarket,
                as_of_date,metric_name,metric_value,unit)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (row["source"], row["grain"], row["property_name"], row["market"],
             row["submarket"], row["as_of_date"], row["metric_name"],
             max(safe_float(row["metric_value"]) or 0.0, 0.0),
             row["unit"]),
        )
        rows_inserted += 1

    conn.commit()
    cur.execute(
        "INSERT INTO load_log (source,grain,file_name,rows_inserted) VALUES (?,?,?,?)",
        ("STR", "monthly", filename, rows_inserted),
    )
    conn.commit()
    print(f"  ✓ {filename} → {rows_inserted} new rows")
    return rows_inserted


def main() -> None:
    os.makedirs(STR_DIR, exist_ok=True)
    files = sorted(glob.glob(os.path.join(STR_DIR, "*.xlsx")))

    if not files:
        print(f"No xlsx files in {STR_DIR} — place STR monthly exports here and rerun.")
        return

    conn  = get_connection()
    total = 0

    for path in files:
        filename = os.path.basename(path)
        if already_loaded(conn, filename):
            print(f"  ↳ Skipping {filename} — already in load_log")
            continue
        try:
            total += load_str_monthly(path, conn)
        except Exception as exc:
            print(f"  ✗ Error loading {filename}: {exc}")

    conn.close()
    print(f"Done. Total monthly rows inserted: {total}")


if __name__ == "__main__":
    main()
