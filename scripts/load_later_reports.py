"""
load_later_reports.py — Later.com social media data loader
Parses Later.com CSV exports from data/later/ into analytics.sqlite

Directory structure:
  data/later/IG/      — files starting with IG_
  data/later/FB/      — files starting with FB_
  data/later/TikTok/  — files starting with TK_

Tables created (12 tables):
  later_ig_profile_growth, later_ig_posts, later_ig_reels,
  later_ig_audience_demographics, later_ig_audience_engagement, later_ig_location
  later_fb_profile_growth, later_fb_posts, later_fb_profile_interactions
  later_tk_profile_growth, later_tk_audience_demographics, later_tk_audience_engagement
"""

import sqlite3
import pandas as pd
import warnings
from datetime import datetime
from pathlib import Path
import glob as _glob
import re

warnings.filterwarnings("ignore")

DB_PATH      = Path(__file__).parent.parent / "data" / "analytics.sqlite"
LATER_DIR    = Path(__file__).parent.parent / "data" / "later"
IG_DIR       = LATER_DIR / "IG"
FB_DIR       = LATER_DIR / "FB"
TK_DIR       = LATER_DIR / "TikTok"


def ts():
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

DDL = """
-- Instagram

CREATE TABLE IF NOT EXISTS later_ig_profile_growth (
    id          INTEGER PRIMARY KEY,
    data_date   TEXT,
    followers   INTEGER,
    views       INTEGER,
    reach       INTEGER,
    loaded_at   TEXT DEFAULT (datetime('now')),
    UNIQUE(data_date)
);

CREATE TABLE IF NOT EXISTS later_ig_posts (
    id              INTEGER PRIMARY KEY,
    posted_at       TEXT,
    media_type      TEXT,
    engagement_rate REAL,
    engagements     INTEGER,
    followers       INTEGER,
    views           INTEGER,
    reach           INTEGER,
    likes           INTEGER,
    comments        INTEGER,
    saves           INTEGER,
    shares          INTEGER,
    instagram_url   TEXT,
    loaded_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(posted_at, instagram_url)
);

CREATE TABLE IF NOT EXISTS later_ig_reels (
    id              INTEGER PRIMARY KEY,
    posted_at       TEXT,
    media_type      TEXT,
    engagement_rate REAL,
    engagements     INTEGER,
    comments        INTEGER,
    likes           INTEGER,
    views           INTEGER,
    saves           INTEGER,
    reach           INTEGER,
    shares          INTEGER,
    instagram_url   TEXT,
    loaded_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(posted_at, instagram_url)
);

CREATE TABLE IF NOT EXISTS later_ig_audience_demographics (
    id          INTEGER PRIMARY KEY,
    gender      TEXT,
    total_pct   REAL,
    age_13_17   REAL,
    age_18_24   REAL,
    age_25_34   REAL,
    age_35_44   REAL,
    age_45_54   REAL,
    age_55_64   REAL,
    age_65_plus REAL,
    loaded_at   TEXT DEFAULT (datetime('now')),
    UNIQUE(gender)
);

CREATE TABLE IF NOT EXISTS later_ig_audience_engagement (
    id          INTEGER PRIMARY KEY,
    hour_24     INTEGER,
    day_label   TEXT,
    impressions INTEGER,
    loaded_at   TEXT DEFAULT (datetime('now')),
    UNIQUE(hour_24, day_label)
);

CREATE TABLE IF NOT EXISTS later_ig_location (
    id              INTEGER PRIMARY KEY,
    location_type   TEXT,
    name            TEXT,
    followers       INTEGER,
    loaded_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(location_type, name)
);

-- Facebook

CREATE TABLE IF NOT EXISTS later_fb_profile_growth (
    id                  INTEGER PRIMARY KEY,
    data_date           TEXT,
    page_followers      INTEGER,
    page_media_views    INTEGER,
    reach               INTEGER,
    page_views          INTEGER,
    loaded_at           TEXT DEFAULT (datetime('now')),
    UNIQUE(data_date)
);

CREATE TABLE IF NOT EXISTS later_fb_posts (
    id              INTEGER PRIMARY KEY,
    posted_at       TEXT,
    media_type      TEXT,
    engagements     INTEGER,
    views           INTEGER,
    reach           INTEGER,
    clicks          INTEGER,
    likes           INTEGER,
    shares          INTEGER,
    comments        INTEGER,
    loaded_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(posted_at)
);

CREATE TABLE IF NOT EXISTS later_fb_profile_interactions (
    id          INTEGER PRIMARY KEY,
    data_date   TEXT,
    reactions   INTEGER,
    engagement  INTEGER,
    loaded_at   TEXT DEFAULT (datetime('now')),
    UNIQUE(data_date)
);

-- TikTok

CREATE TABLE IF NOT EXISTS later_tk_profile_growth (
    id              INTEGER PRIMARY KEY,
    data_date       TEXT,
    followers       INTEGER,
    video_views     INTEGER,
    loaded_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(data_date)
);

CREATE TABLE IF NOT EXISTS later_tk_audience_demographics (
    id          INTEGER PRIMARY KEY,
    gender      TEXT,
    percentage  REAL,
    loaded_at   TEXT DEFAULT (datetime('now')),
    UNIQUE(gender)
);

CREATE TABLE IF NOT EXISTS later_tk_audience_engagement (
    id          INTEGER PRIMARY KEY,
    hour_24     INTEGER,
    day_label   TEXT,
    impressions REAL,
    loaded_at   TEXT DEFAULT (datetime('now')),
    UNIQUE(hour_24, day_label)
);
"""

TABLE_RELATIONSHIPS = [
    ("later_ig_profile_growth",  "later_fb_profile_growth",   "cross_platform", "data_date",   "Instagram vs Facebook follower growth comparison"),
    ("later_ig_profile_growth",  "later_tk_profile_growth",   "cross_platform", "data_date",   "Instagram vs TikTok follower growth comparison"),
    ("later_ig_posts",           "later_fb_posts",            "cross_platform", "posted_at",   "Cross-platform post performance (IG vs FB)"),
    ("later_ig_audience_demographics", "datafy_overview_demographics", "context", "—",        "Social audience age/gender vs visitor demographics"),
    ("later_ig_profile_growth",  "kpi_daily_summary",         "context",        "data_date",   "Instagram reach growth vs hotel demand seasonality"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(v):
    try:
        f = float(str(v).replace(",", "").strip())
        return int(f)
    except Exception:
        return None


def _safe_float(v):
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except Exception:
        return None


def _parse_date(v):
    """Normalize any Later.com date string to ISO YYYY-MM-DD."""
    try:
        return pd.to_datetime(str(v), errors="raise").strftime("%Y-%m-%d")
    except Exception:
        return None


def _parse_datetime(v):
    """Normalize any Later.com datetime string to ISO."""
    try:
        return pd.to_datetime(str(v), errors="raise").strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def _find_files(directory, prefix):
    """Return all CSV files (case-insensitive) in directory starting with prefix."""
    if not directory.exists():
        return []
    return [
        p for p in directory.iterdir()
        if p.suffix.lower() == ".csv" and p.name.upper().startswith(prefix.upper())
    ]


def _read_csv(path: Path) -> pd.DataFrame:
    """Read a Later.com CSV, stripping BOM and normalizing column names."""
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    except Exception:
        df = pd.read_csv(path, encoding="latin-1", low_memory=False)
    df.columns = [c.strip().strip('"') for c in df.columns]
    return df


def _hour_label_to_24(label):
    """Convert '01:00 AM, -07:00 UTC' → 1  or  '01:00 PM, ...' → 13."""
    m = re.match(r"(\d+):(\d+)\s*(AM|PM)", str(label), re.IGNORECASE)
    if not m:
        return None
    h, _, ampm = int(m.group(1)), int(m.group(2)), m.group(3).upper()
    if ampm == "AM":
        return 0 if h == 12 else h
    else:
        return 12 if h == 12 else h + 12


def log_load(conn, source: str, file_name: str, rows: int):
    conn.execute(
        "INSERT OR IGNORE INTO load_log(source, grain, file_name, rows_inserted, run_at) "
        "VALUES(?,?,?,?,datetime('now'))",
        (source, "social", file_name, rows),
    )


def seed_table_relationships(conn):
    for (ta, tb, rel, key, desc) in TABLE_RELATIONSHIPS:
        conn.execute(
            "INSERT OR IGNORE INTO table_relationships"
            "(table_a, table_b, relationship_type, join_key, description) "
            "VALUES(?,?,?,?,?)",
            (ta, tb, rel, key, desc),
        )


# ---------------------------------------------------------------------------
# IG Loaders
# ---------------------------------------------------------------------------

def load_ig_profile_growth(conn) -> int:
    files = _find_files(IG_DIR, "IG_Profile_Growth")
    if not files:
        print(f"{ts()} [IG Profile Growth] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        # Columns: Date, Followers, Views, Reach
        inserted = 0
        for _, row in df.iterrows():
            d = _parse_date(row.get("Date") or row.get("date"))
            if not d:
                continue
            followers   = _safe_int(row.get("Followers"))
            views       = _safe_int(row.get("Views"))
            reach       = _safe_int(row.get("Reach"))
            conn.execute(
                "INSERT OR REPLACE INTO later_ig_profile_growth"
                "(data_date, followers, views, reach) VALUES(?,?,?,?)",
                (d, followers, views, reach),
            )
            inserted += 1
        log_load(conn, "later_ig_profile_growth", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [IG Profile Growth] {f.name} → {inserted} rows")
    return rows_total


def load_ig_posts(conn) -> int:
    files = _find_files(IG_DIR, "IG_Detailed_Post_Performance")
    if not files:
        print(f"{ts()} [IG Posts] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        inserted = 0
        for _, row in df.iterrows():
            posted_at = _parse_datetime(row.get("Time Posted") or row.get("time posted"))
            if not posted_at:
                continue
            ig_url = str(row.get("Instagram Post") or "").strip() or None
            conn.execute(
                "INSERT OR REPLACE INTO later_ig_posts"
                "(posted_at, media_type, engagement_rate, engagements, followers,"
                " views, reach, likes, comments, saves, shares, instagram_url) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    posted_at,
                    str(row.get("Media Type (Image, Video, Carousel)") or "").strip() or None,
                    _safe_float(row.get("Engagement Rate")),
                    _safe_int(row.get("Engagements")),
                    _safe_int(row.get("Followers")),
                    _safe_int(row.get("Views")),
                    _safe_int(row.get("Reach")),
                    _safe_int(row.get("Likes")),
                    _safe_int(row.get("Comments")),
                    _safe_int(row.get("Saves")),
                    _safe_int(row.get("Shares")),
                    ig_url,
                ),
            )
            inserted += 1
        log_load(conn, "later_ig_posts", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [IG Posts] {f.name} → {inserted} rows")
    return rows_total


def load_ig_reels(conn) -> int:
    files = _find_files(IG_DIR, "IG_Detailed_Reel_Performance")
    if not files:
        print(f"{ts()} [IG Reels] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        inserted = 0
        for _, row in df.iterrows():
            posted_at = _parse_datetime(row.get("Time Posted") or row.get("time posted"))
            if not posted_at:
                continue
            ig_url = str(row.get("Instagram Post") or "").strip() or None
            conn.execute(
                "INSERT OR REPLACE INTO later_ig_reels"
                "(posted_at, media_type, engagement_rate, engagements, comments,"
                " likes, views, saves, reach, shares, instagram_url) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    posted_at,
                    str(row.get("Media Type (Image, Video, Carousel)") or "").strip() or None,
                    _safe_float(row.get("Engagement Rate")),
                    _safe_int(row.get("Engagements")),
                    _safe_int(row.get("Comments")),
                    _safe_int(row.get("Likes")),
                    _safe_int(row.get("Views")),
                    _safe_int(row.get("Saves")),
                    _safe_int(row.get("Reach")),
                    _safe_int(row.get("Shares")),
                    ig_url,
                ),
            )
            inserted += 1
        log_load(conn, "later_ig_reels", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [IG Reels] {f.name} → {inserted} rows")
    return rows_total


def load_ig_audience_demographics(conn) -> int:
    files = _find_files(IG_DIR, "IG_Audience_Demographics")
    if not files:
        print(f"{ts()} [IG Demographics] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        # Columns: Gender, Total, 13-17, 18-24, 25-34, 35-44, 45-54, 55-64, 65+
        inserted = 0
        for _, row in df.iterrows():
            gender = str(row.get("Gender") or row.get("gender") or "").strip()
            if not gender:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO later_ig_audience_demographics"
                "(gender, total_pct, age_13_17, age_18_24, age_25_34,"
                " age_35_44, age_45_54, age_55_64, age_65_plus) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    gender,
                    _safe_float(row.get("Total")),
                    _safe_float(row.get("13-17")),
                    _safe_float(row.get("18-24")),
                    _safe_float(row.get("25-34")),
                    _safe_float(row.get("35-44")),
                    _safe_float(row.get("45-54")),
                    _safe_float(row.get("55-64")),
                    _safe_float(row.get("65+")),
                ),
            )
            inserted += 1
        log_load(conn, "later_ig_audience_demographics", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [IG Demographics] {f.name} → {inserted} rows")
    return rows_total


def load_ig_audience_engagement(conn) -> int:
    """Parse hourly engagement grid (rows=hours, columns=days) into long format."""
    files = _find_files(IG_DIR, "IG_Audience_Engagement")
    if not files:
        print(f"{ts()} [IG Engagement] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        # First column = Time label, remaining columns = day labels
        time_col = df.columns[0]
        day_cols  = [c for c in df.columns[1:] if str(c).strip()]
        inserted  = 0
        for _, row in df.iterrows():
            hour_24 = _hour_label_to_24(str(row[time_col]))
            if hour_24 is None:
                continue
            for day in day_cols:
                val = _safe_int(row[day]) if pd.notna(row.get(day)) else None
                if val is None:
                    continue
                day_label = str(day).strip()
                conn.execute(
                    "INSERT OR REPLACE INTO later_ig_audience_engagement"
                    "(hour_24, day_label, impressions) VALUES(?,?,?)",
                    (hour_24, day_label, val),
                )
                inserted += 1
        log_load(conn, "later_ig_audience_engagement", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [IG Engagement] {f.name} → {inserted} rows")
    return rows_total


def load_ig_location(conn) -> int:
    files = _find_files(IG_DIR, "IG_Location_and_Language")
    if not files:
        print(f"{ts()} [IG Location] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        inserted = 0
        for _, row in df.iterrows():
            # Could be "Top Country" / "Top Language" etc.
            loc_type = None
            name     = None
            for col in df.columns:
                if "country" in col.lower() or "language" in col.lower() or "city" in col.lower():
                    loc_type = col.replace("Top ", "").strip()
                    name     = str(row[col]).strip()
                    break
            if not name or name in ("nan", ""):
                continue
            followers_col = [c for c in df.columns if "follower" in c.lower() or "percentage" in c.lower()]
            followers = _safe_int(row[followers_col[0]]) if followers_col else None
            conn.execute(
                "INSERT OR REPLACE INTO later_ig_location(location_type, name, followers) VALUES(?,?,?)",
                (loc_type or "unknown", name, followers),
            )
            inserted += 1
        log_load(conn, "later_ig_location", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [IG Location] {f.name} → {inserted} rows")
    return rows_total


# ---------------------------------------------------------------------------
# FB Loaders
# ---------------------------------------------------------------------------

def load_fb_profile_growth(conn) -> int:
    files = _find_files(FB_DIR, "FB_Profile_Growth")
    if not files:
        print(f"{ts()} [FB Profile Growth] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        # Columns: Date, Page Followers, Page Media Views, Reach, Page Views
        inserted = 0
        for _, row in df.iterrows():
            d = _parse_date(row.get("Date") or row.get("date"))
            if not d:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO later_fb_profile_growth"
                "(data_date, page_followers, page_media_views, reach, page_views) "
                "VALUES(?,?,?,?,?)",
                (
                    d,
                    _safe_int(row.get("Page Followers")),
                    _safe_int(row.get("Page Media Views")),
                    _safe_int(row.get("Reach")),
                    _safe_int(row.get("Page Views")),
                ),
            )
            inserted += 1
        log_load(conn, "later_fb_profile_growth", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [FB Profile Growth] {f.name} → {inserted} rows")
    return rows_total


def load_fb_posts(conn) -> int:
    files = _find_files(FB_DIR, "FB_Detailed_Post_Performance")
    if not files:
        print(f"{ts()} [FB Posts] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        inserted = 0
        for _, row in df.iterrows():
            posted_at = _parse_datetime(row.get("Time Posted") or row.get("time posted"))
            if not posted_at:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO later_fb_posts"
                "(posted_at, media_type, engagements, views, reach, clicks, likes, shares, comments) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    posted_at,
                    str(row.get("Media Type (Image, Video, Carousel)") or "").strip() or None,
                    _safe_int(row.get("Engagements")),
                    _safe_int(row.get("Views")),
                    _safe_int(row.get("Reach")),
                    _safe_int(row.get("Clicks")),
                    _safe_int(row.get("Likes")),
                    _safe_int(row.get("Shares")),
                    _safe_int(row.get("Comments")),
                ),
            )
            inserted += 1
        log_load(conn, "later_fb_posts", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [FB Posts] {f.name} → {inserted} rows")
    return rows_total


def load_fb_profile_interactions(conn) -> int:
    files = _find_files(FB_DIR, "FB_Profile_Interactions")
    if not files:
        print(f"{ts()} [FB Interactions] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        # Columns: Date, Reactions, Engagement
        inserted = 0
        for _, row in df.iterrows():
            d = _parse_date(row.get("Date") or row.get("date"))
            if not d:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO later_fb_profile_interactions"
                "(data_date, reactions, engagement) VALUES(?,?,?)",
                (
                    d,
                    _safe_int(row.get("Reactions")),
                    _safe_int(row.get("Engagement")),
                ),
            )
            inserted += 1
        log_load(conn, "later_fb_profile_interactions", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [FB Interactions] {f.name} → {inserted} rows")
    return rows_total


# ---------------------------------------------------------------------------
# TikTok Loaders
# ---------------------------------------------------------------------------

def load_tk_profile_growth(conn) -> int:
    files = _find_files(TK_DIR, "TK_Profile_Growth")
    if not files:
        print(f"{ts()} [TK Profile Growth] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        # Columns: Date, Followers, Video Views
        inserted = 0
        for _, row in df.iterrows():
            d = _parse_date(row.get("Date") or row.get("date"))
            if not d:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO later_tk_profile_growth"
                "(data_date, followers, video_views) VALUES(?,?,?)",
                (
                    d,
                    _safe_int(row.get("Followers")),
                    _safe_int(row.get("Video Views")),
                ),
            )
            inserted += 1
        log_load(conn, "later_tk_profile_growth", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [TK Profile Growth] {f.name} → {inserted} rows")
    return rows_total


def load_tk_audience_demographics(conn) -> int:
    files = _find_files(TK_DIR, "TK_Audience_Demographics")
    if not files:
        print(f"{ts()} [TK Demographics] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        # Columns: Gender, Percentage
        inserted = 0
        for _, row in df.iterrows():
            gender = str(row.get("Gender") or row.get("gender") or "").strip()
            if not gender or gender == "nan":
                continue
            conn.execute(
                "INSERT OR REPLACE INTO later_tk_audience_demographics(gender, percentage) VALUES(?,?)",
                (gender, _safe_float(row.get("Percentage"))),
            )
            inserted += 1
        log_load(conn, "later_tk_audience_demographics", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [TK Demographics] {f.name} → {inserted} rows")
    return rows_total


def load_tk_audience_engagement(conn) -> int:
    files = _find_files(TK_DIR, "TK_Audience_Engagement")
    if not files:
        print(f"{ts()} [TK Engagement] No files found — skipping")
        return 0
    rows_total = 0
    for f in files:
        df = _read_csv(f)
        time_col = df.columns[0]
        day_cols  = [c for c in df.columns[1:] if str(c).strip()]
        inserted  = 0
        for _, row in df.iterrows():
            hour_24 = _hour_label_to_24(str(row[time_col]))
            if hour_24 is None:
                continue
            for day in day_cols:
                val = _safe_float(row[day]) if pd.notna(row.get(day)) else None
                if val is None:
                    continue
                conn.execute(
                    "INSERT OR REPLACE INTO later_tk_audience_engagement"
                    "(hour_24, day_label, impressions) VALUES(?,?,?)",
                    (hour_24, str(day).strip(), val),
                )
                inserted += 1
        log_load(conn, "later_tk_audience_engagement", f.name, inserted)
        rows_total += inserted
        print(f"{ts()} [TK Engagement] {f.name} → {inserted} rows")
    return rows_total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    if not LATER_DIR.exists():
        print(f"{ts()} [Later] data/later/ directory not found — nothing to load")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    # Ensure schema exists
    conn.executescript(DDL)
    conn.commit()

    # Instagram
    ig_total = 0
    ig_total += load_ig_profile_growth(conn)
    ig_total += load_ig_posts(conn)
    ig_total += load_ig_reels(conn)
    ig_total += load_ig_audience_demographics(conn)
    ig_total += load_ig_audience_engagement(conn)
    ig_total += load_ig_location(conn)

    # Facebook
    fb_total = 0
    fb_total += load_fb_profile_growth(conn)
    fb_total += load_fb_posts(conn)
    fb_total += load_fb_profile_interactions(conn)

    # TikTok
    tk_total = 0
    tk_total += load_tk_profile_growth(conn)
    tk_total += load_tk_audience_demographics(conn)
    tk_total += load_tk_audience_engagement(conn)

    seed_table_relationships(conn)
    conn.commit()
    conn.close()

    total = ig_total + fb_total + tk_total
    print(
        f"{ts()} [Later] Complete — "
        f"IG: {ig_total} rows · FB: {fb_total} rows · TK: {tk_total} rows · "
        f"Total: {total} rows"
    )


if __name__ == "__main__":
    main()
