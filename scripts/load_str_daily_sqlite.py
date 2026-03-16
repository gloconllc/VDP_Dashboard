import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
# STR raw exports live in data/str/  (legacy path: downloads/str_daily.xlsx)
STR_DIR = os.path.join(PROJECT_ROOT, "data", "str")
DAILY_FILE = os.path.join(STR_DIR, "str_daily.xlsx")


def get_connection():
    return sqlite3.connect(DB_PATH)


def normalize_str_daily(df):
    """
    Normalize STR DAILY export with columns:
    ['Period', 'Day of Week', 'Supply', 'Supply Chg (YOY)', 'Demand',
     'Demand Chg (YOY)', 'Revenue', 'Revenue Chg (YOY)', 'Occupancy',
     'Occupancy Chg (YOY)', 'ADR', 'ADR Chg (YOY)', 'RevPAR',
     'RevPAR Chg (YOY)', ...]
    """
    date_col = "Period"

    metric_columns = {
        "Supply": ("supply", "rooms"),
        "Demand": ("demand", "rooms"),
        "Revenue": ("revenue", "USD"),
        "Occupancy": ("occ", "percent"),
        "ADR": ("adr", "USD"),
        "RevPAR": ("revpar", "USD"),
    }

    required = [date_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected STR columns: {missing}")

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")

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


def load_str_daily(conn):
    if not os.path.exists(DAILY_FILE):
        print(f"Daily file not found, skipping: {DAILY_FILE}")
        return 0

    print(f"Loading STR daily file: {DAILY_FILE}")
    df = pd.read_excel(DAILY_FILE, engine="openpyxl")
    norm_df = normalize_str_daily(df)

    cursor = conn.cursor()
    rows_inserted = 0

    for _, row in norm_df.iterrows():
        cursor.execute(
            """
            SELECT COUNT(*) FROM fact_str_metrics
            WHERE source = ?
              AND grain = ?
              AND property_name = ?
              AND market = ?
              AND as_of_date = ?
              AND metric_name = ?
            """,
            (
                row["source"],
                row["grain"],
                row["property_name"],
                row["market"],
                row["as_of_date"],
                row["metric_name"],
            ),
        )
        exists = cursor.fetchone()[0] > 0
        if exists:
            continue

        cursor.execute(
            """
            INSERT INTO fact_str_metrics
            (source, grain, property_name, market, submarket,
             as_of_date, metric_name, metric_value, unit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["source"],
                row["grain"],
                row["property_name"],
                row["market"],
                row["submarket"],
                row["as_of_date"],
                row["metric_name"],
                float(row["metric_value"])
                if row["metric_value"] is not None
                else None,
                row["unit"],
            ),
        )
        rows_inserted += 1

    conn.commit()

    cursor.execute(
        """
        INSERT INTO load_log (source, grain, file_name, rows_inserted)
        VALUES (?, ?, ?, ?)
        """,
        ("STR", "daily", os.path.basename(DAILY_FILE), rows_inserted),
    )
    conn.commit()

    print(f"Inserted {rows_inserted} new daily rows from {os.path.basename(DAILY_FILE)}")
    return rows_inserted


def main():
    conn = get_connection()
    try:
        daily_rows = load_str_daily(conn)
        print(f"Done. Daily rows inserted: {daily_rows}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

