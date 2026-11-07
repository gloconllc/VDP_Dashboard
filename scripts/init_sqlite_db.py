# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS fact_str_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,        -- 'STR'
    grain TEXT NOT NULL,         -- 'daily' or 'monthly'
    property_name TEXT,
    market TEXT,
    submarket TEXT,
    as_of_date TEXT NOT NULL,    -- 'YYYY-MM-DD'
    metric_name TEXT NOT NULL,   -- 'demand', 'supply', 'adr', 'revpar', etc.
    metric_value REAL NOT NULL,
    unit TEXT,                   -- 'rooms', 'USD', 'index', etc.
    created_at TEXT DEFAULT (datetime('now'))
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS load_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,        -- 'STR'
    grain TEXT NOT NULL,         -- 'daily' or 'monthly'
    file_name TEXT NOT NULL,
    rows_inserted INTEGER NOT NULL,
    run_at TEXT DEFAULT (datetime('now'))
);
""")

conn.commit()
conn.close()

print(f"Initialized SQLite DB at {DB_PATH}")

