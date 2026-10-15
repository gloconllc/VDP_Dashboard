"""
fetch_vdp_events.py
===================
Scrapes upcoming events from https://visitdanapoint.com/events/ and loads them
into the vdp_events table in data/analytics.sqlite.

Events flagged is_major=1 when the name contains any of these keywords:
  ohana, fest, festival, marathon, race, tournament, concert, parade,
  show, championship

Run from project root:
    python scripts/fetch_vdp_events.py
"""

import re
import sqlite3
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
LOG_PATH     = PROJECT_ROOT / "logs" / "pipeline.log"

EVENTS_URL = "https://visitdanapoint.com/events/"

MAJOR_KEYWORDS = {
    "ohana", "fest", "festival", "marathon", "race", "tournament",
    "concert", "parade", "show", "championship",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Month name → zero-padded number
MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


# ── Logging ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(step: str, status: str, message: str) -> None:
    line = f"[{_ts()}] [{status:<4}] {step}: {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vdp_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name      TEXT NOT NULL,
            event_date      TEXT NOT NULL,
            event_end_date  TEXT,
            category        TEXT,
            venue           TEXT,
            description     TEXT,
            url             TEXT,
            is_major        INTEGER DEFAULT 0,
            scraped_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(event_name, event_date)
        )
    """)
    conn.commit()


def write_load_log(conn: sqlite3.Connection, rows_inserted: int) -> None:
    conn.execute("""
        INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at)
        VALUES ('VDP_Events', 'event_calendar', 'visitdanapoint.com/events/', ?, datetime('now'))
    """, (rows_inserted,))
    conn.commit()


# ── Date parsing ──────────────────────────────────────────────────────────────

def parse_date(raw: str):  # -> tuple[Optional[str], Optional[str]]:
    """
    Parse a raw date string into (start_iso, end_iso).
    Handles patterns like:
      - "May 10, 2025"
      - "May 10 - 11, 2025"
      - "May 10 - June 1, 2025"
      - "May 2025"        → "2025-05-01"
      - "2025-05-10"
    Returns (start_iso, end_iso). end_iso may be None.
    """
    if not raw:
        return None, None

    raw = raw.strip()

    # Already ISO
    iso_match = re.match(r"^(\d{4}-\d{2}-\d{2})(?:\s*[-–]\s*(\d{4}-\d{2}-\d{2}))?$", raw)
    if iso_match:
        return iso_match.group(1), iso_match.group(2)

    # "Month DD - DD, YYYY" or "Month DD - Month DD, YYYY"
    range_match = re.match(
        r"^([A-Za-z]+)\s+(\d{1,2})\s*[-–]\s*(?:([A-Za-z]+)\s+)?(\d{1,2}),?\s*(\d{4})$",
        raw,
    )
    if range_match:
        mon1 = range_match.group(1).lower()
        d1   = range_match.group(2).zfill(2)
        mon2 = (range_match.group(3) or range_match.group(1)).lower()
        d2   = range_match.group(4).zfill(2)
        yr   = range_match.group(5)
        m1   = MONTH_MAP.get(mon1)
        m2   = MONTH_MAP.get(mon2)
        if m1 and m2:
            return f"{yr}-{m1}-{d1}", f"{yr}-{m2}-{d2}"

    # "Month DD, YYYY" or "Month DD YYYY"
    single_match = re.match(
        r"^([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})$", raw
    )
    if single_match:
        mon = single_match.group(1).lower()
        d   = single_match.group(2).zfill(2)
        yr  = single_match.group(3)
        m   = MONTH_MAP.get(mon)
        if m:
            return f"{yr}-{m}-{d}", None

    # "Month YYYY" → first of month
    month_year = re.match(r"^([A-Za-z]+)\s+(\d{4})$", raw)
    if month_year:
        mon = month_year.group(1).lower()
        yr  = month_year.group(2)
        m   = MONTH_MAP.get(mon)
        if m:
            return f"{yr}-{m}-01", None

    # Fallback: try to grab any 4-digit year + day pattern
    fallback = re.search(r"(\d{4})", raw)
    if fallback:
        yr = fallback.group(1)
        day = re.search(r"\b(\d{1,2})\b", raw)
        mon_word = re.search(r"([A-Za-z]+)", raw)
        if day and mon_word:
            m = MONTH_MAP.get(mon_word.group(1).lower())
            if m:
                return f"{yr}-{m}-{day.group(1).zfill(2)}", None
        return f"{yr}-01-01", None

    return None, None


def is_major_event(name: str) -> int:
    lower = name.lower()
    for kw in MAJOR_KEYWORDS:
        if kw in lower:
            return 1
    return 0


# ── Scraper ───────────────────────────────────────────────────────────────────

def scrape_events() -> list[dict]:
    """
    Fetch the events page and attempt to parse event cards.
    Returns a list of event dicts. On failure returns empty list.
    """
    log("fetch_vdp_events", "INFO", f"Fetching {EVENTS_URL}")

    try:
        resp = requests.get(EVENTS_URL, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log("fetch_vdp_events", "FAIL", f"HTTP request failed: {exc}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    events: list[dict] = []

    # Strategy 1: Look for structured event schema (JSON-LD)
    for script_tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            import json
            data = json.loads(script_tag.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") in ("Event", "SocialEvent", "Festival"):
                    name = item.get("name", "").strip()
                    start_raw = item.get("startDate", "")
                    end_raw   = item.get("endDate", "")
                    location  = item.get("location", {})
                    venue_name = (
                        location.get("name", "") if isinstance(location, dict) else ""
                    )
                    description = (item.get("description") or "")[:500]
                    url_val = item.get("url") or item.get("@id") or ""

                    start_iso, _ = parse_date(start_raw[:10] if start_raw else "")
                    end_iso,   _ = parse_date(end_raw[:10] if end_raw else "")

                    if name and start_iso:
                        events.append({
                            "event_name":     name,
                            "event_date":     start_iso,
                            "event_end_date": end_iso,
                            "category":       item.get("eventStatus", None),
                            "venue":          venue_name or None,
                            "description":    description or None,
                            "url":            url_val or None,
                            "is_major":       is_major_event(name),
                        })
        except Exception:
            pass  # JSON-LD parse failure — fall through to HTML scraping

    if events:
        log("fetch_vdp_events", "OK  ", f"JSON-LD strategy found {len(events)} events")
        return events

    # Strategy 2: Common WordPress / event-plugin HTML patterns
    selectors = [
        # The Events Calendar plugin
        ("article.type-tribe_events",  ".tribe-event-url, a[href*='event']",
         ".tribe-events-single-event-title, h2, h1",
         ".tribe-event-date-start, .tribe-events-schedule, time",
         ".tribe-events-cat-list, .tribe-event-categories",
         ".tribe-venue, .tribe-events-venue",
         ".tribe-events-single-section--description, .tribe-event__body p"),
        # Generic event cards
        (".event-card, .event-item, .event, [class*='event-']", "a",
         "h2, h3, .event-title, .event-name, [class*='title']",
         "time, .date, [class*='date'], [class*='time']",
         ".category, [class*='category'], [class*='tag']",
         ".venue, [class*='venue'], [class*='location']",
         "p, .description, [class*='desc']"),
    ]

    for (container_sel, link_sel, name_sel, date_sel,
         cat_sel, venue_sel, desc_sel) in selectors:
        containers = soup.select(container_sel)
        if not containers:
            continue

        for card in containers:
            try:
                # Name
                name_el = card.select_one(name_sel)
                name = (name_el.get_text(strip=True) if name_el else "").strip()
                if not name or len(name) < 3:
                    continue

                # URL
                link_el = card.select_one(link_sel)
                href = ""
                if link_el:
                    href = link_el.get("href", "")
                    if href and not href.startswith("http"):
                        href = urllib.parse.urljoin(EVENTS_URL, href)

                # Date
                date_el = card.select_one(date_sel)
                date_raw = ""
                if date_el:
                    date_raw = (
                        date_el.get("datetime", "") or date_el.get_text(strip=True)
                    )
                start_iso, end_iso = parse_date(date_raw)
                if not start_iso:
                    continue

                # Category
                cat_el   = card.select_one(cat_sel)
                category = cat_el.get_text(strip=True) if cat_el else None

                # Venue
                venue_el = card.select_one(venue_sel)
                venue    = venue_el.get_text(strip=True) if venue_el else None

                # Description
                desc_el     = card.select_one(desc_sel)
                description = desc_el.get_text(strip=True)[:500] if desc_el else None

                events.append({
                    "event_name":     name,
                    "event_date":     start_iso,
                    "event_end_date": end_iso,
                    "category":       category or None,
                    "venue":          venue or None,
                    "description":    description or None,
                    "url":            href or None,
                    "is_major":       is_major_event(name),
                })
            except Exception as exc:
                log("fetch_vdp_events", "WARN", f"Card parse error: {exc}")
                continue

        if events:
            log("fetch_vdp_events", "OK  ", f"HTML selector '{container_sel}' found {len(events)} events")
            return events

    # Strategy 3: Broad fallback — any <a> tags whose href contains "/event"
    log("fetch_vdp_events", "WARN",
        "Structured selectors found no events — trying broad link fallback")
    seen: set[str] = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/event" not in href.lower():
            continue
        full_url = urllib.parse.urljoin(EVENTS_URL, href)
        name = a_tag.get_text(strip=True)
        if not name or len(name) < 4 or full_url in seen:
            continue
        seen.add(full_url)

        # Try to find a date near this link
        parent = a_tag.parent
        date_raw = ""
        for _ in range(4):
            if parent is None:
                break
            time_el = parent.find("time")
            if time_el:
                date_raw = time_el.get("datetime", "") or time_el.get_text(strip=True)
                break
            date_el = parent.find(class_=re.compile(r"date|time", re.I))
            if date_el:
                date_raw = date_el.get_text(strip=True)
                break
            parent = parent.parent

        start_iso, end_iso = parse_date(date_raw)
        # If still no date, use a placeholder so the row can at least be stored
        if not start_iso:
            continue

        events.append({
            "event_name":     name,
            "event_date":     start_iso,
            "event_end_date": end_iso,
            "category":       None,
            "venue":          None,
            "description":    None,
            "url":            full_url,
            "is_major":       is_major_event(name),
        })

    log("fetch_vdp_events", "INFO",
        f"Fallback link scan found {len(events)} candidate events")
    return events


# ── DB writer ─────────────────────────────────────────────────────────────────

def upsert_events(conn: sqlite3.Connection, events: list[dict]) -> int:
    sql = """
    INSERT INTO vdp_events
        (event_name, event_date, event_end_date, category, venue,
         description, url, is_major, scraped_at)
    VALUES
        (:event_name, :event_date, :event_end_date, :category, :venue,
         :description, :url, :is_major, datetime('now'))
    ON CONFLICT(event_name, event_date) DO UPDATE SET
        event_end_date = excluded.event_end_date,
        category       = excluded.category,
        venue          = excluded.venue,
        description    = excluded.description,
        url            = excluded.url,
        is_major       = excluded.is_major,
        scraped_at     = datetime('now')
    """
    conn.executemany(sql, events)
    conn.commit()
    return len(events)


# ── Known Dana Point Major Events (seed when live scrape returns 0) ──────────
# The VDP events calendar is JavaScript-rendered and requires Playwright/Chrome
# for live scraping. These are known recurring Dana Point annual events seeded
# as baseline reference data. Update dates each year as needed.
KNOWN_DANA_POINT_EVENTS = [
    # ── 2025 Historical Events ────────────────────────────────────────────────
    {"event_name": "OC Marathon", "event_date": "2025-05-04", "event_end_date": "2025-05-04",
     "category": "Race/Sport", "venue": "Dana Point Harbor",
     "description": "Annual marathon finishing at Dana Point Harbor. Major overnight demand driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "SoCal Wahine Surf Classic", "event_date": "2025-08-09", "event_end_date": "2025-08-10",
     "category": "Surf Tournament", "venue": "Doheny State Beach",
     "description": "Women's longboard surf championship. Summer peak demand support.",
     "url": "https://visitdanapoint.com/events/", "is_major": 0},
    {"event_name": "Doheny Days Music Festival", "event_date": "2025-09-13", "event_end_date": "2025-09-14",
     "category": "Festival/Concert", "venue": "Doheny State Beach",
     "description": "Annual rock music festival at Doheny Beach. Strong ADR compression event.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Golden Sails Car Show", "event_date": "2025-09-20", "event_end_date": "2025-09-21",
     "category": "Show", "venue": "Dana Point",
     "description": "Annual classic car show. Weekend demand driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Ohana Fest", "event_date": "2025-09-26", "event_end_date": "2025-09-28",
     "category": "Festival/Concert", "venue": "Doheny State Beach",
     "description": "Annual music and surf festival curated by Eddie Vedder. Signature demand compression event — ADR lift $139, 68% OOS visitors, $14.6M direct expenditure.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Tall Ships Festival", "event_date": "2025-10-03", "event_end_date": "2025-10-05",
     "category": "Festival", "venue": "Dana Point Harbor",
     "description": "Annual tall ships festival at Dana Point Harbor. Strong weekend occupancy driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Dana Point Turkey Trot", "event_date": "2025-11-27", "event_end_date": "2025-11-27",
     "category": "Race", "venue": "Dana Point",
     "description": "Thanksgiving Day 5K/10K race. Family event driving local and overnight visits.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Holiday Boat Parade", "event_date": "2025-12-13", "event_end_date": "2025-12-13",
     "category": "Parade", "venue": "Dana Point Harbor",
     "description": "Annual Holiday Boat Parade of Lights. High-demand shoulder season driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    # ── 2026 Events ───────────────────────────────────────────────────────────
    {"event_name": "Blessing of the Waves", "event_date": "2026-01-01", "event_end_date": "2026-01-01",
     "category": "Festival", "venue": "Dana Point Harbor",
     "description": "Annual New Year's Day paddle-out blessing ceremony.",
     "url": "https://visitdanapoint.com/events/", "is_major": 0},
    {"event_name": "Dana Point Whale Festival", "event_date": "2026-03-01", "event_end_date": "2026-03-02",
     "category": "Festival", "venue": "Dana Point Harbor",
     "description": "Annual festival celebrating gray whale migration season. Q1 shoulder season driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "OC Marathon", "event_date": "2026-05-03", "event_end_date": "2026-05-03",
     "category": "Race/Sport", "venue": "Dana Point Harbor",
     "description": "Annual marathon finishing at Dana Point Harbor. Major overnight demand driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "July 4th Holiday", "event_date": "2026-07-03", "event_end_date": "2026-07-05",
     "category": "Holiday", "venue": "Dana Point",
     "description": "Independence Day holiday weekend. Peak ADR compression period.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "SoCal Wahine Surf Classic", "event_date": "2026-08-08", "event_end_date": "2026-08-09",
     "category": "Surf Tournament", "venue": "Doheny State Beach",
     "description": "Women's longboard surf championship. Summer peak demand support.",
     "url": "https://visitdanapoint.com/events/", "is_major": 0},
    {"event_name": "Doheny Days Music Festival", "event_date": "2026-09-12", "event_end_date": "2026-09-13",
     "category": "Festival/Concert", "venue": "Doheny State Beach",
     "description": "Annual rock music festival at Doheny Beach. Strong ADR compression event.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Golden Sails Car Show", "event_date": "2026-09-19", "event_end_date": "2026-09-20",
     "category": "Show", "venue": "Dana Point",
     "description": "Annual classic car show. Weekend demand driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Ohana Fest", "event_date": "2026-09-25", "event_end_date": "2026-09-27",
     "category": "Festival/Concert", "venue": "Doheny State Beach",
     "description": "Annual music and surf festival curated by Eddie Vedder. Signature demand compression event.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Tall Ships Festival", "event_date": "2026-10-02", "event_end_date": "2026-10-04",
     "category": "Festival", "venue": "Dana Point Harbor",
     "description": "Annual tall ships festival at Dana Point Harbor. Strong weekend occupancy driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Dana Point Turkey Trot", "event_date": "2026-11-26", "event_end_date": "2026-11-26",
     "category": "Race", "venue": "Dana Point",
     "description": "Thanksgiving Day 5K/10K race. Family event driving local and overnight visits.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
    {"event_name": "Holiday Boat Parade", "event_date": "2026-12-12", "event_end_date": "2026-12-12",
     "category": "Parade", "venue": "Dana Point Harbor",
     "description": "Annual Holiday Boat Parade of Lights. Shoulder season demand driver.",
     "url": "https://visitdanapoint.com/events/", "is_major": 1},
]


# ── visitdanapoint.com events feed (master source, added 2026-10-14) ─────────
# The site runs on Simpleview. Its own events calendar is served by a JSON feed
# behind a short-lived public token, which is the same data the calendar page
# renders. This is the first-choice source; the HTML scraper and the seeded list
# below remain as fallbacks. A dated snapshot of the feed is kept in
# data/vdp_site/ (raw data rule) and can be loaded with --from-json.

SITE_ROOT = "https://visitdanapoint.com"
SITE_TOKEN_URL = SITE_ROOT + "/plugins/core/get_simple_token/"
SITE_FEED_URL = SITE_ROOT + "/includes/rest_v2/plugins_events_events_by_date/find/"
SITE_SNAPSHOT_DIR = PROJECT_ROOT / "data" / "vdp_site"
MAJOR_CATEGORIES = {"Signature Events", "Festivals"}


def fetch_site_feed(days_ahead: int = 365) -> list[dict]:
    """Raw event occurrences from the visitdanapoint.com feed, or [] on any failure."""
    import json
    from datetime import timedelta, timezone
    try:
        token = requests.get(SITE_TOKEN_URL, headers=HEADERS, timeout=20).text.strip()
        now = datetime.now(timezone.utc)
        query = {
            "filter": {"active": True, "date_range": {
                "start": {"$date": now.strftime("%Y-%m-%dT00:00:00.000Z")},
                "end": {"$date": (now + timedelta(days=days_ahead)).strftime("%Y-%m-%dT00:00:00.000Z")}}},
            "options": {"limit": 500, "skip": 0, "count": True, "castDocs": False,
                        "fields": {"_id": 1, "title": 1, "date": 1, "startDate": 1, "endDate": 1, "location": 1,
                                   "address1": 1, "city": 1, "url": 1, "categories": 1, "recid": 1, "featured": 1},
                        "sort": {"date": 1, "rank": 1, "title_sort": 1}},
        }
        resp = requests.get(SITE_FEED_URL, params={"json": json.dumps(query), "token": token},
                            headers={**HEADERS, "Accept": "application/json"}, timeout=30)
        resp.raise_for_status()
        docs = resp.json().get("docs", {}).get("docs", [])
        raw = [{"recid": d.get("recid"), "title": d.get("title"), "date": d.get("date"),
                "startDate": d.get("startDate"), "endDate": d.get("endDate"), "location": d.get("location"),
                "address1": d.get("address1"), "city": d.get("city"), "url": d.get("url"),
                "categories": [c.get("catName") for c in d.get("categories") or [] if c.get("catName")],
                "featured": bool(d.get("featured"))} for d in docs]
        if raw:
            SITE_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
            snap = SITE_SNAPSHOT_DIR / f"events_feed_{datetime.now().strftime('%Y-%m-%d')}.json"
            snap.write_text(json.dumps(raw, ensure_ascii=False))
        return raw
    except Exception as exc:  # noqa: BLE001
        log("fetch_vdp_events", "WARN", f"visitdanapoint.com feed unavailable: {exc}")
        return []


def parse_site_feed(raw: list[dict]) -> list[dict]:
    """Feed occurrences -> vdp_events rows (Pacific dates, one row per event).

    Recurring listings (no end date, or a start date months before the
    occurrence, such as weekly farmers markets and dining series) are left out:
    they repeat every week and would crowd the calendar of dated events.
    """
    from datetime import timezone
    from zoneinfo import ZoneInfo
    pt = ZoneInfo("America/Los_Angeles")

    def local(ts):
        if not ts:
            return None
        return datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).astimezone(pt).date()

    rows, seen = [], set()
    for d in raw:
        start, end = local(d.get("startDate")), local(d.get("endDate"))
        if not start or not end:
            continue                      # recurring listing without an end date
        occ = local(d.get("date"))
        if occ and (occ - start).days > 14:
            continue                      # recurring series running for months
        if (end - start).days > 30:
            end = start                   # open-ended listing; keep the opening day only
        key = (d.get("recid"), start)
        if key in seen:
            continue
        seen.add(key)
        cats = d.get("categories") or []
        name = (d.get("title") or "").strip()
        venue = (d.get("location") or d.get("address1") or d.get("city") or "Dana Point").strip()
        rows.append({
            "event_name": name,
            "event_date": start.isoformat(),
            "event_end_date": end.isoformat(),
            "category": ", ".join(cats) or None,
            "venue": venue,
            "description": "Listed on the visitdanapoint.com events calendar"
                           + (f" ({', '.join(cats)})." if cats else "."),
            "url": SITE_ROOT + (d.get("url") or "/events/"),
            "is_major": 1 if (MAJOR_CATEGORIES & set(cats) or d.get("featured") or is_major_event(name)) else 0,
        })
    return rows


def retire_seed_duplicates(conn: sqlite3.Connection, events: list[dict]) -> int:
    """Remove seeded placeholder rows that the site now lists on the same dates
    (within a day, sharing a key word), so one event does not appear twice.
    Only rows still pointing at the generic events page are touched."""
    stop = {"dana", "point", "the", "of", "and", "annual", "2026", "2027", "holiday", "festival", "51st"}
    removed = 0
    seeds = conn.execute(
        "SELECT id, event_name, event_date FROM vdp_events WHERE url = ? AND event_date >= date('now','localtime')",
        (EVENTS_URL,)).fetchall()
    for sid, sname, sdate in seeds:
        sw = {w for w in re.findall(r"[a-z]+", sname.lower()) if w not in stop and len(w) > 3}
        sd = datetime.strptime(sdate, "%Y-%m-%d").date()
        for e in events:
            ew = {w for w in re.findall(r"[a-z]+", e["event_name"].lower()) if w not in stop and len(w) > 3}
            ed = datetime.strptime(e["event_date"], "%Y-%m-%d").date()
            if abs((ed - sd).days) <= 1 and sw & ew:
                conn.execute("DELETE FROM vdp_events WHERE id = ?", (sid,))
                removed += 1
                break
    conn.commit()
    return removed


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    import json
    import sys
    log("fetch_vdp_events", "INFO", "Starting VDP events fetch")

    conn = get_conn()
    create_table(conn)

    # 1. visitdanapoint.com events feed (or a saved snapshot with --from-json PATH)
    if "--from-json" in sys.argv:
        raw = json.loads(Path(sys.argv[sys.argv.index("--from-json") + 1]).read_text())
    else:
        raw = fetch_site_feed()
    site_events = parse_site_feed(raw) if raw else []
    if site_events:
        retired = retire_seed_duplicates(conn, site_events)
        inserted = upsert_events(conn, site_events)
        write_load_log(conn, inserted)
        log("fetch_vdp_events", "OK  ",
            f"visitdanapoint.com feed: {inserted} dated events saved "
            f"({sum(e['is_major'] for e in site_events)} major); {retired} duplicate seed rows replaced")
        conn.close()
        return

    # 2. HTML scraper, then 3. seeded list (unchanged fallbacks)
    events = scrape_events()

    if not events:
        log("fetch_vdp_events", "WARN",
            "Live scrape returned 0 events (JS-rendered site) — seeding known major Dana Point events")
        events = KNOWN_DANA_POINT_EVENTS
        # Once the site's own listings are loaded, never re-seed future dates:
        # the site rows already cover them and seeds would duplicate events.
        has_site = conn.execute(
            "SELECT COUNT(*) FROM vdp_events WHERE url LIKE ?", (SITE_ROOT + "/event/%",)).fetchone()[0]
        if has_site:
            today = datetime.now().strftime("%Y-%m-%d")
            events = [e for e in events if e["event_date"] < today]
            log("fetch_vdp_events", "INFO", "Site events on file; seeded fallback limited to past dates")

    if not events:
        write_load_log(conn, 0)
        conn.close()
        return

    inserted = upsert_events(conn, events)
    write_load_log(conn, inserted)

    major_count = sum(1 for e in events if e["is_major"])
    log("fetch_vdp_events", "OK  ",
        f"Saved {inserted} events ({major_count} flagged major) to vdp_events")

    conn.close()


if __name__ == "__main__":
    main()
