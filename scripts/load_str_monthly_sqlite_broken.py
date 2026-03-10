import pandas as pd

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
DOWNLOADS_DIR = os.path.join(PROJECT_ROOT, "downloads")
MONTHLY_FILE = os.path.join(DOWNLOADS_DIR, "str_monthly.xlsx")


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def normalize_str_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert STR MONTHLY wide format to long format for fact_str_metrics.
    Assumptions (adjust if needed once we see real headers):
      - 'Period' is the date column (month-end or similar)
      - Metric columns include: Supply, Demand, Revenue, Occ, ADR, RevPAR
      - No property/market columns in file: treat as portfolio-level series
    """
    date_col = "Period"
    property_name_value = "VDP Select Portfolio"
    market_value = "Anaheim Area"

    # Map file columns -> (metric_name_in_db, unit)
    metric_columns = {
        "Supply": ("supply", "rooms"),
        "Demand": ("demand", "rooms"),
        "Revenue": ("revenue", "USD"),
        "Occ": ("occ", "percent"),      # Occ or Occ %
        "Occ %": ("occ", "percent"),
        "ADR": ("adr", "USD"),
        "RevPAR": ("revpar", "USD"),
    }

    if date_col not in df.columns:
        raise ValueError(f"Missing expected STR column '{date_col}'")

    df = df.copy()

    # Normalize date
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[date_col] = df[date_col].dt.strftime("%Y-%m-%d")

    # Clean metric columns: trim, treat "-" as missing, coerce to numeric
    for col_name, (metric_name, unit) in metric_columns.items():
        if col_name in df.columns:
            s = df[col_name]
            s = s.astype(str).str.strip().replace("-", None)
            df[col_name] = pd.to_numeric(s, errors="coerce")

    long_frames = []

    for col_name, (metric_name, unit) in metric_columns.items():
        if col_name not in df.columns:
            continue

        tmp = df[[date_col, col_name]].copy()
        tmp.rename(columns={col_name: "metric_value"}, inplace=True)
        tmp["metric_name"] = metric_name
        tmp["unit"] = unit
        tmp["grain"] = "monthly"
        tmp["source"] = "STR"
        tmp["property_name"] = property_name_value
        tmp["market"] = market_value
        tmp["submarket"] = None
        tmp.rename(columns={date_col: "as_of_date"}, inplace=True)

        long_frames.append(tmp)

    if not long_frames:
        raise ValueError("No known metric columns found in STR monthly export")

    long_df = pd.concat(long_frames, ignore_index=True)

    # Final column order expected by fact_str_metrics
    long_df = long_df[
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

    return long_df


def safe_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_str_monthly(path: str, conn: sqlite3.Connection) -> int:
    if not os.path.exists(path):
        print(f"File not found, skipping: {path}")
        return 0

    print(f"Loading STR monthly file: {path}")
    df = pd.read_excel(path, engine="openpyxl")
    norm_df = normalize_str_monthly(df)

    cursor = conn.cursor()
    rows_inserted = 0

    for _, row in norm_df.iterrows():
        # Dedup key includes grain so daily/monthly never collide
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
        exists = cursor.fetchone()[0]

        if exists:
            continue

        cursor.execute(
            """
            INSERT INTO fact_str_metrics (
                source, grain, property_name, market, submarket,
                as_of_date, metric_name, metric_value, unit
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["source"],
                row["grain"],
                row["property_name"],
                row["market"],
                row["submarket"],
                row["as_of_date"],
                row["metric_name"],
                safe_float(row["metric_value"]),
                row["unit"],
            ),
        )
        rows_inserted += 1

    conn.commit()

    cursor.execute(
        """
        INSERT INTO load_log (source, grain, filename, rows_inserted)
        VALUES (?, ?, ?, ?)
        """,
        ("STR", "monthly", os.path.basename(path), rows_inserted),
    )
    conn.commit()

    print(f"Inserted {rows_inserted} new monthly rows from {os.path.basename(path)}")
    return rows_inserted


    conn = get_connection()
    try:
        monthly_rows = load_str_monthly(MONTHLY_FILE, conn)
        print(f"Done. Monthly rows inserted: {monthly_rows}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
def main() -> None:
    conn = get_connection()
    try:
        monthly_rows = load_str_monthly(MONTHLY_FILE, conn)
        print(f"Done. Monthly rows inserted: {monthly_rows}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
        """,
        ("STR", "monthly", os.path.basename(path), rows_inserted),
    )
    conn.commit()

    print(f"Inserted {rows_inserted} new monthly rows from {os.path.basename(path)}")
    return rows_inserted

def main():
    conn = get_connection()
    try:
        monthly_rows = load_str_monthly(MONTHLY_FILE, conn)
        print(f"Done. Monthly rows inserted: {monthly_rows}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

