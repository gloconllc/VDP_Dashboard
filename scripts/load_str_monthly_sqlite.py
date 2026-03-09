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

