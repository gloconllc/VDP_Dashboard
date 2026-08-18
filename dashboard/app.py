"""
Dana Point PULSE Report Viewer
-----------------------------------------
Generates the PULSE report PDF from live STR / CoStar / Datafy data (each on
its own native reporting window) and displays it in full, alongside three
interactive features built for Heather Johnston at Visit Dana Point: an
Intelligence Brief AI assistant that answers questions against the full STR,
CoStar, and Datafy history (not just the current PDF snapshot); an editable
notes layer for her own commentary on the data; and a browsable archive of
every past generated report.

Notes and the report archive are NOT stored in analytics.sqlite (see
NOTES_DB_PATH / REPORT_ARCHIVE_DIR below); on Railway, both need a mounted
persistent Volume or they will not survive the next auto-redeploy.
"""

import base64
import html
import io
import os
import sqlite3
import sys
from datetime import datetime

import pandas as pd
import streamlit as st

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
PDF_PATH = os.path.join(LOGS_DIR, "weekly_report_latest.pdf")
LOGO_PATH = os.path.join(BASE_DIR, "assets", "vdp_logo.svg")
LOGO_NAV_PATH = os.path.join(BASE_DIR, "assets", "vdp_logo_nav.svg")
HERO_PHOTO_PATH = os.path.join(BASE_DIR, "assets", "photos", "hero_coast.jpg")

# Notes/annotation storage and the report archive both live OUTSIDE
# analytics.sqlite and outside git entirely. analytics.sqlite is
# git-committed and gets replaced by a brand-new container on every Railway
# auto-redeploy, so anything written straight into that file would vanish
# the next time new data lands. Point NOTES_DB_PATH / REPORT_ARCHIVE_DIR at
# a Railway persistent Volume once one is configured; both fall back to a
# local path for development so the features still work before that is set
# up. See CLAUDE.md Lessons Learned, 2026-08-17.
NOTES_DB_PATH = os.environ.get(
    "NOTES_DB_PATH", os.path.join(PROJECT_ROOT, "data", "dashboard_notes.sqlite")
)
REPORT_ARCHIVE_DIR = os.environ.get(
    "REPORT_ARCHIVE_DIR", os.path.join(PROJECT_ROOT, "data", "report_archive")
)

# Notes/annotation controls are editable by anyone viewing the app, no
# special link required, per Heather Johnston's explicit request.
IS_EDITOR = True

st.set_page_config(
    page_title="Dana Point PULSE",
    page_icon="📄",
    layout="wide",
)


def _file_data_uri(path: str, mime: str) -> str:
    try:
        with open(path, "rb") as fh:
            b64 = base64.b64encode(fh.read()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except FileNotFoundError:
        return ""


def _logo_data_uri() -> str:
    return _file_data_uri(LOGO_PATH, "image/svg+xml")


def _logo_nav_data_uri() -> str:
    return _file_data_uri(LOGO_NAV_PATH, "image/svg+xml")


def _hero_photo_data_uri() -> str:
    return _file_data_uri(HERO_PHOTO_PATH, "image/jpeg")


PHOTOS_DIR = os.path.join(BASE_DIR, "assets", "photos")


def _photo_data_uri(filename: str) -> str:
    return _file_data_uri(os.path.join(PHOTOS_DIR, filename), "image/jpeg")

st.markdown(
    """
    <style>
      #MainMenu {visibility: hidden;}
      footer {visibility: hidden;}
      .block-container { padding-top: 3rem; max-width: 100%; padding-left: 3rem; padding-right: 3rem; }
      .pulse-header { display:flex; align-items:center; justify-content:space-between;
                      padding-top: 6px; padding-bottom: 14px;
                      border-bottom: 3px solid transparent;
                      border-image: linear-gradient(90deg, #123C4A 0%, #1D6E86 45%, #B4530980 100%) 1;
                      margin-bottom: 18px; overflow: visible; line-height: 1.3; }
      .pulse-header-left { display:flex; align-items:center; gap:16px; }
      .pulse-logo-badge { background:#123C4A; border-radius:10px; width:58px; height:50px;
                           display:flex; align-items:center; justify-content:center; flex-shrink:0;
                           box-shadow: 0 2px 6px rgba(18,60,74,0.35); }

      /* Destination hero band: real Dana Point photography */
      .pulse-hero { position:relative; height:180px; border-radius:14px; overflow:hidden;
        background-size:cover; background-position:center; margin-bottom:22px; }
      .pulse-hero-overlay { position:absolute; inset:0;
        background:linear-gradient(180deg, rgba(9,32,40,0.58) 0%, rgba(9,32,40,0.72) 100%); }
      .pulse-hero-content { position:absolute; inset:0; display:flex; flex-direction:column;
        align-items:center; justify-content:center; text-align:center; }
      .pulse-hero-logo { width:170px; height:auto; margin-bottom:10px;
        filter: drop-shadow(0 2px 10px rgba(0,0,0,0.65)) drop-shadow(0 0 2px rgba(0,0,0,0.5)); }
      .pulse-hero-tag { color:#FFFFFF; font-size:13px; font-weight:600; letter-spacing:.06em; text-transform:uppercase;
        text-shadow: 0 1px 6px rgba(0,0,0,0.55); }
      .pulse-logo-badge img { width:36px; height:auto; }
      .pulse-eyebrow { font-size: 11px; font-weight:700; letter-spacing:.12em; text-transform:uppercase;
        color:#1D6E86; margin-bottom:2px; }
      .pulse-title { font-family:Georgia,'DejaVu Serif',serif; font-style:italic; font-size: 28px;
        font-weight: 700; color: #0B2530; line-height:1.15; }
      .pulse-sub { font-size: 13.5px; color: #475569; margin-top:5px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
      .pulse-sub .pulse-sub-brand { color:#1D6E86; font-weight:600; }
      .pulse-sub .pulse-sub-dot { color:#CBD9DE; }
      .pulse-sub .pulse-status-pill { display:inline-flex; align-items:center; gap:6px;
        background:#F0F7F9; border:1px solid #CBE3EA; border-radius:20px; padding:3px 10px 3px 8px;
        color:#0E4B5C; font-weight:600; font-size:12.5px; }
      .pulse-sub .pulse-status-dot { width:7px; height:7px; border-radius:50%; background:#1D9E6F; flex-shrink:0; }

      /* Quick-take summary card, shown before the full report */
      .pulse-summary-card { background:#F0F7F9; border:1px solid #CBE3EA; border-left:4px solid #1D6E86;
        border-radius:10px; padding:16px 20px; margin:4px 0 22px 0; }
      .pulse-summary-label { font-size:11px; font-weight:700; letter-spacing:.1em; text-transform:uppercase;
        color:#1D6E86; margin-bottom:6px; }
      .pulse-summary-body { font-size:14.5px; line-height:1.55; color:#1E293B; }

      /* Help popover content */
      .pulse-help-title { font-size:15px; font-weight:700; color:#0B2530; margin-bottom:8px; }
      .pulse-help-item { font-size:13.5px; line-height:1.55; color:#334155; margin-bottom:10px; }
      .pulse-help-item b { color:#0E4B5C; }

      /* Regenerate button + filter row */
      div[data-testid="stButton"] > button {
        background:#1D6E86; color:#FFFFFF; border:1px solid #123C4A; border-radius:8px;
        font-weight:600; transition: background 0.15s ease;
      }
      div[data-testid="stButton"] > button:hover { background:#123C4A; color:#FFFFFF; border-color:#123C4A; }
      div[data-testid="stButton"] > button:active { background:#0E4B5C; color:#FFFFFF; }

      /* Main "Download PDF" CTA: bigger, centered, brand teal. Download
         buttons render under a different testid than regular buttons, so
         they need their own rule rather than inheriting stButton's. */
      div[data-testid="stDownloadButton"] > button {
        background:#1D6E86; color:#FFFFFF; border:1px solid #123C4A; border-radius:10px;
        font-weight:700; font-size:16px; padding:14px 0; transition: background 0.15s ease;
        box-shadow: 0 2px 8px rgba(18,60,74,0.25);
      }
      div[data-testid="stDownloadButton"] > button:hover { background:#123C4A; color:#FFFFFF; border-color:#123C4A; }
      div[data-testid="stDownloadButton"] > button:active { background:#0E4B5C; color:#FFFFFF; }
      .pulse-filter-row { display:flex; align-items:flex-end; gap:14px; margin-bottom:6px; }
      div[data-testid="stDateInput"] input { border-color:#CBD9DE; }
      div[data-testid="column"] { display:flex; align-items:center; }

      /* Whale-watching loading splash */
      .whale-splash {
        position:relative; height:340px; border-radius:14px; overflow:hidden;
        background: linear-gradient(180deg, #D2E0E9 0%, #75A9CC 35%, #255A6D 70%, #16333D 100%);
        display:flex; align-items:center; justify-content:center; flex-direction:column;
        margin: 10px 0 24px 0;
      }
      .whale-splash .sun { position:absolute; top:26px; right:60px; width:46px; height:46px;
        border-radius:50%; background:#FFE9A8; box-shadow:0 0 40px 10px rgba(255,233,168,0.5); }
      .whale-splash .wave-row { position:absolute; left:0; right:0; height:26px; opacity:.55; }
      .whale-splash .wave-row svg { width:200%; height:100%; animation: wave-drift 9s linear infinite; }
      .whale-splash .wave1 { bottom:64px; }
      .whale-splash .wave2 { bottom:34px; opacity:.4; }
      .whale-splash .wave2 svg { animation-duration: 13s; animation-direction: reverse; }
      @keyframes wave-drift { from { transform: translateX(0); } to { transform: translateX(-50%); } }

      .whale-splash .scene { position:relative; width:220px; height:170px; }
      .whale-splash .tail {
        position:absolute; bottom:18px; left:50%; width:150px; height:150px;
        transform-origin: bottom center; transform: translateX(-50%) rotate(0deg);
        animation: whale-dive 4.5s ease-in-out infinite;
      }
      @keyframes whale-dive {
        0%   { transform: translateX(-50%) translateY(20px) rotate(0deg); opacity:0; }
        12%  { opacity:1; }
        30%  { transform: translateX(-50%) translateY(-58px) rotate(-6deg); opacity:1; }
        45%  { transform: translateX(-50%) translateY(-64px) rotate(4deg); opacity:1; }
        60%  { transform: translateX(-50%) translateY(-40px) rotate(-2deg); opacity:1; }
        78%  { transform: translateX(-50%) translateY(30px) rotate(0deg); opacity:0.4; }
        100% { transform: translateX(-50%) translateY(30px) rotate(0deg); opacity:0; }
      }
      .whale-splash .splash-ring {
        position:absolute; bottom:18px; left:50%; width:120px; height:18px; margin-left:-60px;
        border-radius:50%; background:radial-gradient(ellipse at center, rgba(255,255,255,0.75) 0%, rgba(255,255,255,0) 72%);
        animation: splash-pulse 4.5s ease-in-out infinite;
      }
      @keyframes splash-pulse {
        0%, 68% { opacity:0; transform: translateX(-50%) scale(0.6); }
        74% { opacity:1; transform: translateX(-50%) scale(1); }
        90% { opacity:0; transform: translateX(-50%) scale(1.5); }
        100% { opacity:0; }
      }
      .whale-splash .boat { position:absolute; bottom:78px; left:26px; width:54px;
        animation: boat-bob 3.2s ease-in-out infinite; }
      @keyframes boat-bob { 0%,100% { transform: translateY(0px) rotate(-2deg); } 50% { transform: translateY(-6px) rotate(2deg); } }
      .whale-splash .caption { color:#F0FAFF; font-size:13px; font-weight:600; letter-spacing:.02em;
        margin-top:14px; text-shadow:0 1px 4px rgba(0,0,0,0.25); }
      .whale-splash .caption-sub { color:#CFEFF9; font-size:11px; margin-top:2px; }

      /* Intelligence Brief: AI synthesis banner + answer box */
      .brain-banner { position:relative; height:120px; border-radius:12px; overflow:hidden;
        background-size:cover; background-position:center; margin: 8px 0 16px 0; }
      .brain-banner-overlay { position:absolute; inset:0;
        background:linear-gradient(90deg, rgba(18,60,74,0.82) 0%, rgba(18,60,74,0.45) 100%); }
      .brain-banner-content { position:absolute; inset:0; display:flex; flex-direction:column;
        justify-content:center; padding: 0 24px; }
      .brain-eyebrow { color:#A5DDE9; font-size:11px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; }
      .brain-title { color:#FFFFFF; font-size:19px; font-weight:700; margin-top:4px; max-width:640px; }
      .ai-answer-box { background:#F0F7F9; border:1px solid #CBE3EA; border-left:4px solid #1D6E86;
        border-radius:10px; padding:16px 20px; margin:8px 0 8px 0; font-size:14.5px; line-height:1.6; color:#1E293B; }
      .notes-box { background:#FFFFFF; border:1px solid #E2E8F0; border-radius:10px; padding:14px 18px; margin: 8px 0; }
      .notes-box-title { font-size:12px; font-weight:700; letter-spacing:.05em; text-transform:uppercase;
        color:#1D6E86; margin-bottom:8px; }
      .note-item { font-size:13.5px; color:#334155; padding:6px 0; border-top:1px solid #F1F5F9; }
      .note-item:first-of-type { border-top:none; }
      .note-date { font-weight:700; color:#0E4B5C; margin-right:6px; }
    </style>
    """,
    unsafe_allow_html=True,
)

_WAVE_SVG = (
    '<svg viewBox="0 0 200 20" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">'
    '<path d="M0 10 Q 12.5 0, 25 10 T 50 10 T 75 10 T 100 10 T 125 10 T 150 10 T 175 10 T 200 10 V20 H0 Z" fill="#F0FAFF"/>'
    '</svg>'
)

WHALE_SPLASH_HTML = f"""
<div class="whale-splash">
  <div class="sun"></div>
  <div class="wave-row wave1">{_WAVE_SVG}{_WAVE_SVG}</div>
  <div class="wave-row wave2">{_WAVE_SVG}{_WAVE_SVG}</div>
  <div class="scene">
    <svg class="boat" viewBox="0 0 60 30" xmlns="http://www.w3.org/2000/svg">
      <path d="M4 22 L56 22 L48 29 L12 29 Z" fill="#0F172A"/>
      <rect x="27" y="4" width="2.4" height="18" fill="#0F172A"/>
      <path d="M29.4 6 L44 20 L29.4 20 Z" fill="#F8FAFB"/>
    </svg>
    <div class="splash-ring"></div>
    <svg class="tail" viewBox="0 0 150 150" xmlns="http://www.w3.org/2000/svg">
      <path d="M75 150 C 60 100, 40 90, 10 70 C 45 78, 60 65, 75 40 C 90 65, 105 78, 140 70 C 110 90, 90 100, 75 150 Z"
            fill="#255A6D" stroke="#16333D" stroke-width="2"/>
    </svg>
  </div>
  <div class="caption">Heading out for this week&rsquo;s numbers&hellip;</div>
  <div class="caption-sub">Building the Dana Point PULSE report from live STR, CoStar &amp; Datafy data</div>
</div>
"""


@st.cache_data(ttl=3600, show_spinner=False)
def _generate(_cache_key: str, range_start: str | None, range_end: str | None):
    from generate_weekly_report import build_report
    override = (range_start, range_end) if range_start and range_end else None
    return build_report(date_range=override)


# ---------------------------------------------------------------------------
# Data access for the Intelligence Brief AI assistant below. Read-only,
# defensive: a missing table or a stale connection returns an empty
# string/frame rather than crashing the page.
# ---------------------------------------------------------------------------

@st.cache_resource
def _open_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)


def get_connection():
    """Self-healing wrapper around the cached connection. analytics.sqlite
    can throw a disk I/O error if the underlying file gets replaced out from
    under an open handle. A dead handle doesn't fix itself; probe it on every
    call and rebuild if it has gone stale, instead of letting the page crash."""
    conn = _open_connection()
    try:
        conn.execute("SELECT 1")
    except Exception:
        _open_connection.clear()
        conn = _open_connection()
    return conn


@st.cache_data(ttl=1800, show_spinner=False)
def load_str_monthly_summary(
    months: float = 24, start_date: str | None = None, end_date: str | None = None
) -> str:
    try:
        conn = get_connection()
        if start_date and end_date:
            df = pd.read_sql_query(
                """
                SELECT strftime('%Y-%m', as_of_date) AS month,
                       ROUND(AVG(occ_pct), 1) AS avg_occ,
                       ROUND(AVG(adr), 0) AS avg_adr,
                       ROUND(AVG(revpar), 0) AS avg_revpar
                FROM kpi_daily_summary
                WHERE as_of_date >= ? AND as_of_date <= ?
                GROUP BY month ORDER BY month DESC
                """,
                conn, params=(start_date, end_date),
            )
        else:
            df = pd.read_sql_query(
                """
                SELECT strftime('%Y-%m', as_of_date) AS month,
                       ROUND(AVG(occ_pct), 1) AS avg_occ,
                       ROUND(AVG(adr), 0) AS avg_adr,
                       ROUND(AVG(revpar), 0) AS avg_revpar
                FROM kpi_daily_summary
                GROUP BY month ORDER BY month DESC LIMIT ?
                """,
                conn, params=(max(1, int(round(months))),),
            )
        if df.empty:
            return "No STR monthly history available."
        return "\n".join(
            f"{r.month}: Occupancy {r.avg_occ}%, ADR ${r.avg_adr:,.0f}, RevPAR ${r.avg_revpar:,.0f}"
            for r in df.itertuples()
        )
    except Exception:
        return "STR monthly history unavailable."


@st.cache_data(ttl=1800, show_spinner=False)
def load_costar_summary(
    months: float = 12, start_date: str | None = None, end_date: str | None = None
) -> str:
    """Pulls from costar_market_daily, the real day-by-day CoStar submarket
    feed, windowed to the same trailing period as everything else (or an
    explicit start_date/end_date for a custom range)."""
    try:
        conn = get_connection()
        if start_date and end_date:
            cutoff, latest_date = start_date, end_date
        else:
            latest_row = pd.read_sql_query("SELECT MAX(as_of_date) AS d FROM costar_market_daily", conn)
            latest_date = latest_row.iloc[0]["d"]
            if not latest_date:
                return "No CoStar submarket history available."
            cutoff = (pd.to_datetime(latest_date) - pd.Timedelta(days=30 * months)).strftime("%Y-%m-%d")
        df = pd.read_sql_query(
            """
            SELECT strftime('%Y-%m', as_of_date) AS month,
                   ROUND(AVG(occupancy_pct), 1) AS avg_occ,
                   ROUND(AVG(adr_usd), 0) AS avg_adr,
                   ROUND(AVG(revpar_usd), 0) AS avg_revpar
            FROM costar_market_daily
            WHERE as_of_date >= ? AND as_of_date <= ?
            GROUP BY month ORDER BY month DESC
            """,
            conn, params=(cutoff, latest_date),
        )
        if df.empty:
            return "No CoStar submarket history available."
        header = f"Real CoStar daily feed, most recent report date {latest_date}:\n"
        return header + "\n".join(
            f"{r.month}: Occupancy {r.avg_occ}%, ADR ${r.avg_adr:,.0f}, RevPAR ${r.avg_revpar:,.0f}"
            for r in df.itertuples()
        )
    except Exception:
        return "CoStar history unavailable."


@st.cache_data(ttl=1800, show_spinner=False)
def load_datafy_summary() -> str:
    try:
        conn = get_connection()
        markets = pd.read_sql_query(
            "SELECT dma, spend_share_pct FROM datafy_overview_spending_by_market "
            "ORDER BY report_period_start DESC, spend_share_pct DESC LIMIT 10",
            conn,
        )
        if not markets.empty:
            markets_metric = "% of visitor spend"
            markets_line = ", ".join(f"{r.dma} {r.spend_share_pct * 100:.1f}%" for r in markets.itertuples())
        else:
            markets = pd.read_sql_query(
                "SELECT dma, trips_share_pct FROM datafy_overview_top_markets "
                "ORDER BY report_period_start DESC, trips_share_pct DESC LIMIT 10",
                conn,
            )
            markets_metric = "% of trips"
            markets_line = ", ".join(f"{r.dma} {r.trips_share_pct:.1f}%" for r in markets.itertuples())
        spending = pd.read_sql_query(
            "SELECT category, spend_share_pct FROM datafy_overview_spending_by_category "
            "ORDER BY report_period_start DESC, spend_share_pct DESC LIMIT 8",
            conn,
        )
        parts = []
        if not markets.empty:
            parts.append(f"Top visitor origin markets ({markets_metric}): " + markets_line)
        if not spending.empty:
            parts.append(
                "Visitor spending by category (% share): "
                + ", ".join(f"{r.category} {r.spend_share_pct * 100:.1f}%" for r in spending.itertuples())
            )
        return "\n".join(parts) if parts else "No Datafy summary data available."
    except Exception:
        return "Datafy summary unavailable."


@st.cache_data(ttl=1800, show_spinner=False)
def load_recent_insights_summary() -> str:
    try:
        conn = get_connection()
        df = pd.read_sql_query(
            "SELECT as_of_date, headline, body FROM insights_daily "
            "WHERE audience IN ('dmo','cross') ORDER BY as_of_date DESC LIMIT 8",
            conn,
        )
        if df.empty:
            return "No recent insights available."
        return "\n".join(
            f"[{r.as_of_date}] {r.headline}: {(r.body or '')[:200]}" for r in df.itertuples()
        )
    except Exception:
        return "Insights unavailable."


def ask_hotel_partner_ai(
    question: str, months: float = 24, start_date: str | None = None, end_date: str | None = None
) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return (
            "The AI assistant is not configured yet. Contact Visit Dana Point "
            "to enable this feature."
        )
    try:
        import anthropic
    except ImportError:
        return "The AI assistant is temporarily unavailable."

    window_desc = f"{start_date} to {end_date}" if start_date and end_date else f"trailing {months} months"
    system_prompt = f"""You are a helpful tourism data assistant for Dana Point hotel partners \
and Visit Dana Point staff. Answer using ONLY the data provided below, which \
covers STR (hotel performance), CoStar (submarket benchmarking), and Datafy \
(visitor economy) history for Dana Point over the {window_desc}. If \
the data below does not answer the question, say so plainly rather than \
guessing or inventing figures. Keep answers concise, specific, and in plain, \
non-technical language.

=== STR Monthly History (Occupancy / ADR / RevPAR) ===
{load_str_monthly_summary(months)}

=== CoStar Submarket History (Newport Beach/Dana Point) ===
{load_costar_summary(months, start_date=start_date, end_date=end_date)}

=== Datafy Visitor Economy Summary ===
{load_datafy_summary()}

=== Recent Forward-Looking Insights ===
{load_recent_insights_summary()}
"""
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            system=system_prompt,
            messages=[{"role": "user", "content": question}],
        )
        return resp.content[0].text if resp.content else "No answer returned."
    except Exception as e:
        return f"The assistant could not answer right now ({type(e).__name__}). Please try again shortly."


# ---------------------------------------------------------------------------
# Notes / annotation layer, Heather Johnston's editable commentary. Writes
# go to NOTES_DB_PATH, never to analytics.sqlite (see note above).
# ---------------------------------------------------------------------------

@st.cache_resource
def get_notes_connection():
    os.makedirs(os.path.dirname(NOTES_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(NOTES_DB_PATH, check_same_thread=False, timeout=10)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dashboard_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section TEXT NOT NULL,
            note_text TEXT NOT NULL,
            author TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def load_notes(section: str) -> list[dict]:
    try:
        conn = get_notes_connection()
        cur = conn.execute(
            "SELECT id, note_text, author, created_at FROM dashboard_notes "
            "WHERE section = ? ORDER BY created_at DESC",
            (section,),
        )
        cols = ["id", "note_text", "author", "created_at"]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        return []


def save_note(section: str, text: str, author: str = "Heather Johnston") -> None:
    text = text.strip()
    if not text:
        return
    conn = get_notes_connection()
    conn.execute(
        "INSERT INTO dashboard_notes (section, note_text, author, created_at) "
        "VALUES (?, ?, ?, ?)",
        (section, text, author, datetime.now().strftime("%Y-%m-%d %H:%M")),
    )
    conn.commit()


def delete_note(note_id: int) -> None:
    conn = get_notes_connection()
    conn.execute("DELETE FROM dashboard_notes WHERE id = ?", (note_id,))
    conn.commit()


def render_notes_block(section: str, label: str) -> None:
    notes = load_notes(section)

    if IS_EDITOR:
        with st.expander(f"\U0001F4DD {label} — add or manage notes", expanded=False):
            new_text = st.text_area(
                "Add a note",
                key=f"note_input_{section}",
                placeholder="e.g. Occupancy dip in early July tracks with the marina closure, not a demand issue.",
            )
            if st.button("Save note", key=f"note_save_{section}"):
                if new_text.strip():
                    save_note(section, new_text)
                    st.success("Note saved.")
                    st.rerun()
                else:
                    st.warning("Write something before saving.")
            if notes:
                st.markdown("---")
                for n in notes:
                    c1, c2 = st.columns([6, 1])
                    with c1:
                        st.markdown(f"**{n['created_at']}** — {html.escape(n['note_text'])}")
                    with c2:
                        if st.button("Delete", key=f"note_del_{n['id']}"):
                            delete_note(n["id"])
                            st.rerun()
    elif notes:
        items_html = "".join(
            f'<div class="note-item"><span class="note-date">{html.escape(n["created_at"])}</span> '
            f'{html.escape(n["note_text"])}</div>'
            for n in notes
        )
        st.markdown(
            f'<div class="notes-box"><div class="notes-box-title">\U0001F4CC {html.escape(label)}</div>{items_html}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="notes-box"><div class="notes-box-title">\U0001F4CC {html.escape(label)}</div>'
            '<div class="note-item" style="font-style:italic; color:#94A3B8;">'
            "No notes have been added yet. Add commentary on the data here."
            "</div></div>",
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Report archive, dated copies of every generated PDF, browsable in-app.
# ---------------------------------------------------------------------------

def archive_report_copy(pdf_path: str) -> str | None:
    try:
        if not pdf_path or not os.path.exists(pdf_path):
            return None
        os.makedirs(REPORT_ARCHIVE_DIR, exist_ok=True)
        dated_name = f"dana_point_pulse_{datetime.now().strftime('%Y-%m-%d_%H%M')}.pdf"
        dest = os.path.join(REPORT_ARCHIVE_DIR, dated_name)
        with open(pdf_path, "rb") as src, open(dest, "wb") as dst:
            dst.write(src.read())
        return dest
    except Exception:
        return None


def list_archived_reports(limit: int = 20) -> list[dict]:
    try:
        if not os.path.isdir(REPORT_ARCHIVE_DIR):
            return []
        files = [f for f in os.listdir(REPORT_ARCHIVE_DIR) if f.lower().endswith(".pdf")]
        files.sort(reverse=True)
        return [{"name": f, "path": os.path.join(REPORT_ARCHIVE_DIR, f)} for f in files[:limit]]
    except Exception:
        return []


def render_report_archive() -> None:
    reports = list_archived_reports()
    with st.expander(f"\U0001F4C1 Report Repository — Past Issues ({len(reports)})", expanded=False):
        st.caption(
            "Every generated report is kept here. Moving to a synced SharePoint "
            "folder is next; this local archive is the repository until then."
        )
        if not reports:
            st.caption("No past reports archived yet. Click Regenerate to create the first one.")
            return
        for r in reports:
            try:
                with open(r["path"], "rb") as f:
                    data = f.read()
                st.download_button(
                    r["name"], data=data, file_name=r["name"], mime="application/pdf",
                    key=f"archive_{r['name']}", use_container_width=True,
                )
            except Exception:
                continue


# Section map: each entry is one poppable section a viewer can jump to.
# "page" is the 0-based index into the physical PDF (0 = cover, which is
# intentionally not offered as its own pop-out section).
SECTIONS = [
    {"icon": "📊", "title": "Executive Summary & Hotel Performance",
     "desc": "Headline KPIs, the occupancy trend, and the story behind this week's numbers.", "page": 1},
    {"icon": "🏨", "title": "Hotel Performance (ADR) & Visitor Origins",
     "desc": "Average daily rate by day of week and where visitors are traveling from.", "page": 2},
    {"icon": "🏢", "title": "Market Segments",
     "desc": "CoStar chain-scale occupancy and RevPAR by tier for the Newport Beach/Dana Point submarket.", "page": 3},
    {"icon": "🏬", "title": "Chain-Scale Segment Detail",
     "desc": "Segment-level performance and the TBID/TOT tax estimate.", "page": 4},
    {"icon": "🧳", "title": "Visitor Profile & Spend",
     "desc": "Datafy visitor demographics, category spend, and length of stay.", "page": 5},
    {"icon": "📈", "title": "Forward Outlook & Group Business",
     "desc": "What's ahead for compression, group bookings, and travel trends.", "page": 6},
]


# How-to reference for the "?" help icon. Written in John Picou's voice:
# formal, warm, and specific about what each control does and why it matters.
HELP_HTML = """
<div class="pulse-help-title">How to Use This Report</div>
<div class="pulse-help-item">
  This page brings live STR, CoStar, and Datafy data for Dana Point together in one place. Four
  tools make it easy to work with, and each is described below.
</div>
<div class="pulse-help-item">
  <b>Report Window.</b> Select a start and end date to rebuild the cover, executive summary, and
  hotel performance pages around that period. CoStar and Datafy sections keep their own freshest
  available window, since each source reports on a different schedule.
</div>
<div class="pulse-help-item">
  <b>Summarize.</b> Select this for a short, plain-language read of what the current numbers show.
  It draws directly from the report in front of you, so it always matches your selected window.
</div>
<div class="pulse-help-item">
  <b>Regenerate.</b> Rebuilds the report from the latest data in the pipeline. Use this after a
  fresh STR, CoStar, or Datafy load, or whenever you want to confirm you are looking at the most
  current figures available.
</div>
<div class="pulse-help-item">
  <b>Download PDF.</b> Saves the complete report to your computer, named for the day you
  downloaded it.
</div>
<div class="pulse-help-item">
  <b>View a Section.</b> Every major section, from the executive summary through forward outlook,
  can open on its own. Select "View section" on any card to see, download, or print that page by
  itself, then select "Close, return to full report" when you are finished.
</div>
<div class="pulse-help-item">
  Questions about a specific figure are welcome. The Data &amp; Downloads section at the end of
  the full report names the source behind every number.
</div>
"""


@st.cache_data(ttl=3600, show_spinner=False)
def _summarize_report(pdf_bytes: bytes) -> str:
    """Pull the cover and executive summary text from the generated report and
    have Claude write a short, plain-language take in John Picou's voice."""
    import pdfplumber

    text_chunks = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages[:2]:
            text_chunks.append(page.extract_text() or "")
    source_text = "\n\n".join(text_chunks).strip()[:6000]
    if not source_text:
        return ""

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""

    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    system_prompt = (
        "You are John Picou, Director of Business Intelligence at Visit Anaheim and founder of "
        "GloCon Solutions. Write in his voice: formal, warm, analytically precise, and "
        "solution-oriented. Use the Oxford comma. Never use em dashes, exclamation marks, or ALL "
        "CAPS. Spell out numbers one through nine; use numerals for 10 and above. In four to five "
        "sentences: state what the data shows, name the headline figure, note the trend "
        "direction, and close with what it means for the team this week."
    )
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=400,
            system=system_prompt,
            messages=[{
                "role": "user",
                "content": f"Summarize the following Dana Point PULSE report excerpt:\n\n{source_text}",
            }],
        )
        return resp.content[0].text.strip()
    except Exception:  # noqa: BLE001
        return ""


@st.cache_data(ttl=3600, show_spinner=False)
def _split_pdf_pages(pdf_bytes: bytes) -> list[bytes]:
    """Split the full report into single-page PDFs so a section can be
    viewed or printed on its own without the rest of the report."""
    from pypdf import PdfReader, PdfWriter

    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages: list[bytes] = []
    for page in reader.pages:
        writer = PdfWriter()
        writer.add_page(page)
        buf = io.BytesIO()
        writer.write(buf)
        pages.append(buf.getvalue())
    return pages


def _pdf_generated_at() -> str:
    if os.path.exists(PDF_PATH):
        return datetime.fromtimestamp(os.path.getmtime(PDF_PATH)).strftime("%b %d, %Y at %I:%M %p")
    return "not yet generated"


def _header_html(status_text: str, ok: bool = True) -> str:
    dot_color = "#1D9E6F" if ok else "#C2410C"
    return f"""
        <div class="pulse-header">
          <div class="pulse-header-left">
            <div class="pulse-logo-badge"><img src="{_logo_data_uri()}"></div>
            <div>
              <div class="pulse-eyebrow">Destination Intelligence Report</div>
              <div class="pulse-title">Dana Point PULSE</div>
              <div class="pulse-sub">
                <span class="pulse-sub-brand">Prepared by GloCon Solutions LLC for Visit Dana Point</span>
                <span class="pulse-sub-dot">&bull;</span>
                <span class="pulse-status-pill">
                  <span class="pulse-status-dot" style="background:{dot_color};"></span>{status_text}
                </span>
              </div>
            </div>
          </div>
        </div>
        """


col1, col2, col3, col4, col5 = st.columns([3.2, 2, 1, 1, 0.5])
with col1:
    header_slot = st.empty()
    # Shown immediately, before this run's report generation has actually
    # happened, so it can't yet reflect a real timestamp. Replaced below
    # with the true "Last generated" stamp once generation completes.
    header_slot.markdown(_header_html("Generating latest report&hellip;"), unsafe_allow_html=True)
with col2:
    date_range = st.date_input(
        "Report window",
        value=(),
        label_visibility="collapsed",
        help="Pick a start and end date to rebuild the hotel-performance window (cover, Executive Summary, Hotel Performance pages). Datafy and CoStar sections always show their own freshest available period.",
    )
with col3:
    summarize_clicked = st.button("Summarize", use_container_width=True)
with col4:
    regenerate = st.button("Regenerate", use_container_width=True)
with col5:
    with st.popover("❓", use_container_width=True):
        st.markdown(HELP_HTML, unsafe_allow_html=True)

range_start_iso = date_range[0].isoformat() if len(date_range) == 2 else None
range_end_iso = date_range[1].isoformat() if len(date_range) == 2 else None

st.markdown(
    f"""
    <div class="pulse-hero" style="background-image:url('{_hero_photo_data_uri()}');">
      <div class="pulse-hero-overlay"></div>
      <div class="pulse-hero-content">
        <img class="pulse-hero-logo" src="{_logo_nav_data_uri()}">
        <div class="pulse-hero-tag">Dana Point, California</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if regenerate:
    _generate.clear()

splash = st.empty()
splash.markdown(WHALE_SPLASH_HTML, unsafe_allow_html=True)

try:
    cache_key = datetime.now().strftime("%Y-%m-%d-%H") if not regenerate else datetime.now().isoformat()
    pdf_path = _generate(cache_key, range_start_iso, range_end_iso)
    if regenerate:
        archive_report_copy(pdf_path)
except Exception as exc:  # noqa: BLE001
    splash.empty()
    header_slot.markdown(_header_html("Last generated: generation failed, see logs", ok=False), unsafe_allow_html=True)
    st.error(f"Report generation failed: {exc}")
    st.stop()

splash.empty()

if not os.path.exists(pdf_path):
    header_slot.markdown(_header_html("Last generated: report file missing", ok=False), unsafe_allow_html=True)
    st.error("Report file was not created. Check logs for details.")
    st.stop()

header_slot.markdown(_header_html(f"Last generated: {_pdf_generated_at()}"), unsafe_allow_html=True)

with open(pdf_path, "rb") as f:
    pdf_bytes = f.read()

if "show_summary" not in st.session_state:
    st.session_state["show_summary"] = False
if summarize_clicked:
    st.session_state["show_summary"] = True

if st.session_state["show_summary"]:
    with st.spinner("Reading the numbers…"):
        summary_text = _summarize_report(pdf_bytes)
    if summary_text:
        st.markdown(
            f"""
            <div class="pulse-summary-card">
              <div class="pulse-summary-label">Quick Take, in John's Words</div>
              <div class="pulse-summary-body">{html.escape(summary_text).replace(chr(10), "<br>")}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.info("A summary is not available right now. Check that ANTHROPIC_API_KEY is configured.")

dl_left, dl_center, dl_right = st.columns([1, 2, 1])
with dl_center:
    st.download_button(
        "⬇ Download PDF",
        data=pdf_bytes,
        file_name=f"Visit Dana Point PULSE Report {datetime.now().strftime('%Y-%m-%d')}.pdf",
        mime="application/pdf",
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Intelligence Brief — an AI assistant answering questions against this
# week's live STR, CoStar, and Datafy data, plus a free-form follow-up
# question box. Distinct from the "Summarize" button above: Summarize reads
# only the current PDF's cover and executive summary; this reads the full
# monthly STR/CoStar history, Datafy visitor economy summary, and recent
# forward-looking insights, so it can answer trend and comparison questions
# the PDF snapshot alone cannot.
# ---------------------------------------------------------------------------

brain_months = (
    max((date_range[1] - date_range[0]).days, 1) / 30.0 if len(date_range) == 2 else 24.0
)

st.markdown(
    f"""
    <div class="brain-banner" style="background-image:url('{_photo_data_uri("drone_aerial.jpg")}');">
      <div class="brain-banner-overlay"></div>
      <div class="brain-banner-content">
        <div class="brain-eyebrow">Dana Point Intelligence Brief</div>
        <div class="brain-title">Ask a question about your STR, CoStar &amp; Datafy data</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

brain_col1, brain_col2 = st.columns([1, 1])
with brain_col1:
    latest_intel_clicked = st.button(
        "\U0001F9E0 Generate Latest Intelligence", use_container_width=True, type="primary",
    )
with brain_col2:
    st.caption("Synthesizes STR, CoStar, and Datafy history into one plain-language read.")

if latest_intel_clicked:
    with st.spinner("Correlating STR, CoStar, and Datafy data..."):
        brain_answer = ask_hotel_partner_ai(
            "In plain, non-technical language, synthesize the current story across "
            "STR hotel performance, CoStar submarket benchmarking, and Datafy visitor "
            "economy data. Cover: where occupancy/ADR/RevPAR stand and their trend, "
            "how Dana Point compares to its CoStar submarket, and the one or two most "
            "important visitor-economy patterns. Keep it to three or four short paragraphs.",
            months=brain_months, start_date=range_start_iso, end_date=range_end_iso,
        )
    st.markdown(f'<div class="ai-answer-box">{html.escape(brain_answer)}</div>', unsafe_allow_html=True)

with st.expander("\U0001F50D Ask a follow-up question about this data"):
    partner_question = st.text_input(
        "Your question",
        key="partner_question",
        placeholder="e.g. How has occupancy trended over the last 12 months?",
        label_visibility="collapsed",
    )
    if st.button("Ask", type="primary", key="ask_followup"):
        if partner_question.strip():
            with st.spinner("Reading STR, CoStar, and Datafy history..."):
                answer = ask_hotel_partner_ai(
                    partner_question.strip(), months=brain_months,
                    start_date=range_start_iso, end_date=range_end_iso,
                )
            st.markdown(f'<div class="ai-answer-box">{html.escape(answer)}</div>', unsafe_allow_html=True)
        else:
            st.warning("Type a question first.")

st.divider()

section_pages = _split_pdf_pages(pdf_bytes)
available_sections = [s for s in SECTIONS if s["page"] < len(section_pages)]

if "open_section" not in st.session_state:
    st.session_state["open_section"] = None

if available_sections:
    st.markdown("#### View a Section")
    st.caption("Click any section below to open, view, or print just that part of the report.")
    grid_cols = st.columns(3)
    for i, section in enumerate(available_sections):
        with grid_cols[i % 3]:
            with st.container(border=True):
                st.markdown(f"**{section['icon']} {section['title']}**")
                st.caption(section["desc"])
                if st.button("View section", key=f"open_section_{i}", use_container_width=True):
                    st.session_state["open_section"] = i
                    st.rerun()

open_idx = st.session_state.get("open_section")
if open_idx is not None and 0 <= open_idx < len(available_sections):
    open_section = available_sections[open_idx]

    @st.dialog(open_section["title"], width="large")
    def _section_dialog():
        st.caption(open_section["desc"])
        page_bytes = section_pages[open_section["page"]]
        page_b64 = base64.b64encode(page_bytes).decode("utf-8")
        st.markdown(
            f"""
            <iframe src="data:application/pdf;base64,{page_b64}"
                    width="100%" height="620px" style="border:1px solid #E2E8F0; border-radius:8px;">
            </iframe>
            """,
            unsafe_allow_html=True,
        )
        dl_col, close_col = st.columns(2)
        with dl_col:
            st.download_button(
                "⬇ Download this section",
                data=page_bytes,
                file_name=f"Visit Dana Point PULSE - {open_section['title']} - {datetime.now().strftime('%Y-%m-%d')}.pdf",
                mime="application/pdf",
                use_container_width=True,
                key="download_section_pdf",
            )
        with close_col:
            if st.button("Close, return to full report", use_container_width=True, key="close_section_dialog"):
                st.session_state["open_section"] = None
                st.rerun()

    _section_dialog()

st.markdown("#### Full Report")
b64 = base64.b64encode(pdf_bytes).decode("utf-8")
st.markdown(
    f"""
    <iframe src="data:application/pdf;base64,{b64}"
            width="100%" height="900px" style="border:1px solid #E2E8F0; border-radius:8px;">
    </iframe>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Notes & Report Repository — editable commentary on the data, and a
# browsable archive of every past generated PDF.
# ---------------------------------------------------------------------------

st.markdown("#### Notes & Report Repository")
render_notes_block("general", "General Notes")
render_report_archive()

# Admin-only: manual digest send, gated per the project's existing ?admin=true
# convention. Runs inside this app's own container, which has real outbound
# network access, unlike a local sandbox, so this is also how a send gets
# verified end to end before the scheduled morning send relies on it.
if st.query_params.get("admin", "").lower() == "true":
    st.markdown("---")
    st.markdown("#### Admin: Intelligence Brief Digest")
    missing_env = [
        k for k in ("DIGEST_EMAIL_FROM", "DIGEST_EMAIL_TO", "DIGEST_SMTP_HOST", "DIGEST_SMTP_USER", "DIGEST_SMTP_PASS")
        if not os.environ.get(k)
    ]
    if missing_env:
        st.warning(f"Not configured yet, missing: {', '.join(missing_env)}")
    else:
        st.caption(f"Will send from {os.environ['DIGEST_EMAIL_FROM']} to {os.environ['DIGEST_EMAIL_TO']}.")
        if st.button("Send Digest Email Now"):
            with st.spinner("Sending…"):
                try:
                    import send_weekly_digest as digest
                    kpis = digest.load_latest_kpis()
                    insights = digest.load_insights()
                    digest_html = digest.build_html(kpis, insights)
                    n = digest.send_email(digest_html)
                    st.success(f"Sent to {n} recipient(s).")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Send failed: {exc}")
