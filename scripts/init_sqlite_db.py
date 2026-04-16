import os
import sqlite3
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")

DB_SCHEMA_VERSION = 3  # Increment when schema changes occur

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

cur.execute("""
CREATE TABLE IF NOT EXISTS db_schema_version (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    migration_date TEXT DEFAULT (datetime('now')),
    description TEXT,
    UNIQUE(version)
);
""")

cur.execute("INSERT OR IGNORE INTO db_schema_version (version, description) VALUES (?, ?)",
    (DB_SCHEMA_VERSION, f"Schema initialized on {datetime.now().isoformat()}"))

conn.commit()
conn.close()

print(f"Initialized SQLite DB at {DB_PATH} (schema version {DB_SCHEMA_VERSION})")

