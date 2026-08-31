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
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

# Card-sized live figures for the "View a Section" cards, plus the tiled-basemap
# feeder-market map. Guarded so a missing module degrades to the previous
# PDF-thumbnail behavior rather than taking the whole page down.
try:
    import section_visuals
except Exception:  # pragma: no cover - defensive on deploy
    section_visuals = None

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

# Global responsive styles for entire app
st.markdown("""
<style>
    /* Desktop-first base styles */
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    .stContainer { max-width: 100%; }
    .element-container { overflow: visible; }

    /* Metric and value typography scales */
    .metric-large { font-size: 42px; font-weight: 800; line-height: 1.1; color: #0B2530; }
    .metric-medium { font-size: 32px; font-weight: 800; line-height: 1.1; color: #0B2530; }
    .metric-label { font-size: 13px; font-weight: 700; color: #6B7280; text-transform: uppercase; letter-spacing: 0.5px; }
    .metric-delta { font-size: 13px; font-weight: 600; color: #059669; }

    /* Chart and visualization responsive sizing */
    .plotly-graph-div { width: 100% !important; overflow-x: auto; }
    .js-plotly-plot { width: 100% !important; }

    /* Section titles responsive */
    .section-title { font-size: 18px; font-weight: 700; color: #0B2530; margin: 24px 0 12px 0; }

    /* Prevent horizontal scroll on all viewport widths */
    body, html { overflow-x: hidden; }
    main { overflow-x: hidden; }
    .stApp { overflow-x: hidden; }

    /* Tablet optimization (768px and below) */
    @media (max-width: 768px) {
        .metric-large { font-size: 32px; }
        .metric-medium { font-size: 26px; }
        .metric-label { font-size: 12px; letter-spacing: 0.3px; }
        .section-title { font-size: 16px; margin: 20px 0 10px 0; }
        .stColumn { padding: 0 4px; }

        /* Allow text to wrap freely on tablets */
        .element-container { word-wrap: break-word; word-break: break-word; }
        [data-testid="column"] { word-wrap: break-word; word-break: break-word; }
        .stMarkdown { word-wrap: break-word; word-break: break-word; }
    }

    /* Mobile optimization (480px and below) */
    @media (max-width: 480px) {
        .metric-large { font-size: 28px; }
        .metric-medium { font-size: 22px; }
        .metric-label { font-size: 11px; margin-bottom: 4px; }
        .metric-delta { font-size: 12px; }
        .section-title { font-size: 14px; margin: 16px 0 8px 0; }
        .stColumn { padding: 0 2px; }
        .stMarkdown > p { font-size: 14px; }

        /* Prevent horizontal overflow on mobile only: overflow-x hidden
           without affecting vertical. This allows text to wrap and containers
           to expand vertically without clipping. */
        .element-container {
            overflow-x: hidden;
            overflow-y: visible;
            word-wrap: break-word;
            word-break: break-word;
        }
        [data-testid="column"] {
            overflow-x: hidden;
            overflow-y: visible;
            word-wrap: break-word;
            word-break: break-word;
        }
    }
</style>
""", unsafe_allow_html=True)


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

      /* "Download PDF" CTA: grouped with Summarize/Regenerate in the header
         row, so it matches their height/padding but keeps a bit more
         weight (bolder text, subtle shadow) as the primary action of the
         three. Download buttons render under a different testid than
         regular buttons, so they need their own rule rather than
         inheriting stButton's. */
      div[data-testid="stDownloadButton"] > button {
        background:#1D6E86; color:#FFFFFF; border:1px solid #123C4A; border-radius:8px;
        font-weight:700; transition: background 0.15s ease;
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

      /* Jump-to-section nav strip */
      .pulse-jumpnav { display:flex; flex-wrap:wrap; gap:8px; margin: 4px 0 22px 0; }
      .pulse-jumpnav a { display:inline-flex; align-items:center; gap:6px;
        background:#F8FAFC; border:1px solid #E2E8F0; border-radius:20px;
        padding:7px 14px; font-size:12.5px; font-weight:600; color:#123C4A;
        text-decoration:none; transition: all 0.15s ease; scroll-margin-top: 16px; }
      .pulse-jumpnav a:hover { background:#F0F7F9; border-color:#1D6E86; color:#1D6E86; }
      .pulse-anchor { position:relative; top:-14px; visibility:hidden; }

      /* Responsive: mobile + narrow desktop windows. Streamlit's own wide
         layout defaults to a fixed-feeling multi-column grid that doesn't
         reflow well under ~640px; these overrides keep the header, hero,
         KPI tiles, and buttons legible on a phone instead of just shrinking
         everything proportionally. */
      @media (max-width: 640px) {
        .block-container { padding-left:0.8rem !important; padding-right:0.8rem !important; padding-top:1.5rem !important; }
        .pulse-title { font-size:20px; }
        .pulse-eyebrow { font-size:10px; }
        .pulse-header { flex-wrap:wrap; gap:10px; }
        .pulse-hero { height:120px; }
        .pulse-hero-logo { width:110px; }
        .pulse-hero-tag { font-size:11px; }
        .brain-banner { height:140px; }
        .brain-title { font-size:15px; }
        .pulse-summary-card, .ai-answer-box, .notes-box { padding:12px 14px; }
        div[data-testid="stDownloadButton"] > button { font-size:13px; padding:10px 0; }
        div[data-testid="column"] { min-width: 100% !important; flex: 1 1 100% !important; }
      }
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


def load_kpi_period_stats(
    months: float = 12, start_date: str | None = None, end_date: str | None = None
) -> dict | None:
    """Averages over the selected data window, not just the single latest
    day, so the KPI tiles actually move when the period filter changes.
    Pass start_date/end_date for an explicit custom range (e.g. from a
    date picker); otherwise falls back to a trailing window of `months`
    ending at the latest STR-reported day."""
    try:
        conn = get_connection()
        if start_date and end_date:
            cutoff, latest_date = start_date, end_date
        else:
            latest_row = pd.read_sql_query("SELECT MAX(as_of_date) AS d FROM kpi_daily_summary", conn)
            latest_date = latest_row.iloc[0]["d"]
            if not latest_date:
                return None
            cutoff = (pd.to_datetime(latest_date) - pd.Timedelta(days=30 * months)).strftime("%Y-%m-%d")
        df = pd.read_sql_query(
            "SELECT AVG(occ_pct) AS occ_pct, AVG(adr) AS adr, AVG(revpar) AS revpar, "
            "AVG(occ_yoy) AS occ_yoy, AVG(adr_yoy) AS adr_yoy, AVG(revpar_yoy) AS revpar_yoy, "
            "COUNT(*) AS n_days "
            "FROM kpi_daily_summary WHERE as_of_date >= ? AND as_of_date <= ?",
            conn, params=(cutoff, latest_date),
        )
        if df.empty or pd.isna(df.iloc[0]["occ_pct"]):
            return None
        row = df.iloc[0].to_dict()
        row["period_start"] = cutoff
        row["period_end"] = latest_date
        return row
    except Exception:
        return None


@st.cache_data(ttl=1800, show_spinner=False)
def load_kpi_trend_df(
    months: float = 12, start_date: str | None = None, end_date: str | None = None
) -> pd.DataFrame:
    """Windowed by an explicit start_date/end_date when given (custom
    range), otherwise a trailing window of `months` ending at the latest
    STR-reported day. Buckets by day for short windows (This Week and any
    custom range under ~45 days) since a single monthly average isn't a
    trend line, and by month for longer windows."""
    try:
        conn = get_connection()
        if start_date and end_date:
            cutoff, latest_date = start_date, end_date
        else:
            latest_row = pd.read_sql_query("SELECT MAX(as_of_date) AS d FROM kpi_daily_summary", conn)
            latest_date = latest_row.iloc[0]["d"]
            if not latest_date:
                return pd.DataFrame()
            cutoff = (pd.to_datetime(latest_date) - pd.Timedelta(days=30 * months)).strftime("%Y-%m-%d")

        window_days = (pd.to_datetime(latest_date) - pd.to_datetime(cutoff)).days
        bucket_expr = "as_of_date" if window_days <= 45 else "strftime('%Y-%m', as_of_date)"
        label_col = "day" if window_days <= 45 else "month"

        return pd.read_sql_query(
            f"""
            SELECT {bucket_expr} AS {label_col},
                   ROUND(AVG(occ_pct), 1) AS occ,
                   ROUND(AVG(adr), 0) AS adr,
                   ROUND(AVG(revpar), 0) AS revpar
            FROM kpi_daily_summary
            WHERE as_of_date >= ? AND as_of_date <= ?
            GROUP BY {label_col} ORDER BY {label_col}
            """,
            conn, params=(cutoff, latest_date),
        ).rename(columns={label_col: "month"})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1800, show_spinner=False)
def load_datafy_markets_df(limit: int = 8) -> pd.DataFrame:
    """Prefers datafy_overview_spending_by_market (DMA spend share, 2026 YTD,
    the freshest markets table on file) over the older
    datafy_overview_top_markets (trips share, stuck on a 2025 annual pull).
    Returns a normalized `share_pct` column (0-100 scale) plus a `metric`
    label so the chart/caption can say which one it's showing."""
    try:
        conn = get_connection()
        df = pd.read_sql_query(
            "SELECT dma, spend_share_pct FROM datafy_overview_spending_by_market "
            "WHERE report_period_start = (SELECT MAX(report_period_start) FROM datafy_overview_spending_by_market) "
            "ORDER BY spend_share_pct DESC LIMIT ?",
            conn, params=(limit,),
        )
        if not df.empty:
            df["share_pct"] = df["spend_share_pct"] * 100
            df["metric"] = "Share of visitor spend"
            return df[["dma", "share_pct", "metric"]]
        df = pd.read_sql_query(
            "SELECT dma, trips_share_pct FROM datafy_overview_top_markets "
            "WHERE report_period_start = (SELECT MAX(report_period_start) FROM datafy_overview_top_markets) "
            "ORDER BY trips_share_pct DESC LIMIT ?",
            conn, params=(limit,),
        )
        df["share_pct"] = df["trips_share_pct"]
        df["metric"] = "Share of trips"
        return df[["dma", "share_pct", "metric"]]
    except Exception:
        return pd.DataFrame()


# Approximate Nielsen DMA centroid coordinates (lat, lon) for every feeder
# market name that appears in datafy_overview_spending_by_market /
# datafy_overview_top_markets, so the visitor-origins section can plot a real
# geographic bubble map instead of only a horizontal bar chart. Any DMA name
# that shows up in the data but isn't in this dict is simply left off the
# map; the bar chart below still shows every market.
DMA_COORDS: dict[str, tuple[float, float]] = {
    "Los Angeles": (34.05, -118.24), "San Diego": (32.72, -117.16),
    "New York": (40.71, -74.01), "Phoenix -Prescott": (33.45, -112.07),
    "San Francisco-Oak-San Jose": (37.77, -122.42), "Dallas-Ft. Worth": (32.78, -96.80),
    "Las Vegas": (36.17, -115.14), "Portland- OR": (45.52, -122.68),
    "Salt Lake City": (40.76, -111.89), "Sacramnto-Stkton-Modesto": (38.58, -121.49),
    "Philadelphia": (39.95, -75.17), "Washington-DC -Hagrstwn": (38.90, -77.04),
    "Seattle-Tacoma": (47.61, -122.33), "Houston": (29.76, -95.37),
    "Chicago": (41.88, -87.63), "Boston -Manchester": (42.36, -71.06),
    "Nashville": (36.16, -86.78), "Denver": (39.74, -104.99),
    "Palm Springs": (33.83, -116.55), "Dayton": (39.76, -84.19),
    "Atlanta": (33.75, -84.39), "Minneapolis-St. Paul": (44.98, -93.27),
    "SantaBarbra-SanMar-SanLuob": (34.42, -119.70), "Boise": (43.62, -116.20),
    "Cincinnati": (39.10, -84.51), "Orlando-Daytona Bch-Melbrn": (28.54, -81.38),
    "Reno": (39.53, -119.81), "Cleveland-Akron -Canton": (41.50, -81.69),
    "Miami-Ft. Lauderdale": (25.76, -80.19), "Tampa-St. Pete -Sarasota": (27.95, -82.46),
    "Tucson -Sierra Vista": (32.22, -110.93), "Fresno-Visalia": (36.75, -119.77),
    "Hartford & New Haven": (41.76, -72.69), "Raleigh-Durham -Fayetvlle": (35.78, -78.64),
    "Bakersfield": (35.37, -119.02), "Baltimore": (39.29, -76.61),
    "Detroit": (42.33, -83.05), "Colorado Springs-Pueblo": (38.83, -104.82),
    "West Palm Beach-Ft. Pierce": (26.72, -80.05), "Charlotte": (35.23, -80.84),
    "Buffalo": (42.89, -78.88), "Honolulu": (21.31, -157.86),
    "Ft. Myers-Naples": (26.64, -81.87), "Eugene": (44.05, -123.09),
    "Grand Rapids-Kalmzoo-B.Crk": (42.96, -85.67), "Missoula": (46.87, -113.99),
    "Kansas City": (39.10, -94.58), "Indianapolis": (39.77, -86.16),
    "Monterey-Salinas": (36.60, -121.65), "Spokane": (47.66, -117.43),
    "Peoria-Bloomington": (40.69, -89.59), "Des Moines-Ames": (41.60, -93.61),
    "Eureka": (40.80, -124.16), "Austin": (30.27, -97.74),
    "Richmond-Petersburg": (37.54, -77.44), "Oklahoma City": (35.47, -97.52),
    "Norfolk-Portsmth-Newpt Nws": (36.85, -76.29), "St. Louis": (38.63, -90.20),
    "Albuquerque-Santa Fe": (35.08, -106.65), "Idaho Falls-Pocatllo-Jcksn": (43.49, -112.04),
    "Milwaukee": (43.04, -87.91), "Jacksonville": (30.33, -81.66),
    "Medford-Klamath Falls": (42.33, -122.87), "San Antonio": (29.42, -98.49),
    "Portland-Auburn": (43.66, -70.26), "Erie": (42.13, -80.09),
    "Huntsville-Decatur -Flor": (34.73, -86.59), "Memphis": (35.15, -90.05),
    "Youngstown": (41.10, -80.65), "Lincoln & Hastings-Krny": (40.81, -96.68),
    "Providence-New Bedford": (41.82, -71.41), "Ft. Smith-Fay-Sprngdl-Rgrs": (36.06, -94.16),
    "Columbus- OH": (39.96, -82.99), "Louisville": (38.25, -85.76),
    "Springfield- MO": (37.21, -93.29), "Greensboro-H.Point-W.Salem": (36.07, -79.79),
    "Omaha": (41.26, -95.93), "Flint-Saginaw-Bay City": (43.01, -83.69),
    "Wilkes Barre-Scranton-Hztn": (41.41, -75.66), "Birmingham -Ann and Tusc": (33.52, -86.80),
    "El Paso -Las Cruces": (31.76, -106.49), "Cedar Rapids-Wtrlo-Iwc&Dub": (42.01, -91.64),
    "Mobile-Pensacola -Ft Walt": (30.69, -88.04), "Tulsa": (36.15, -95.99),
    "Yuma-El Centro": (32.69, -114.62), "Paducah-Cape Girard-Harsbg": (37.08, -88.60),
    "Rochester- NY": (43.16, -77.61), "Little Rock-Pine Bluff": (34.75, -92.29),
    "Chico-Redding": (39.73, -121.84), "Pittsburgh": (40.44, -79.99),
    "Tyler-Longview-Lfkn&Ncgd": (32.35, -95.30), "Roanoke-Lynchburg": (37.27, -79.94),
    "Greenvll-Spart-Ashevll-And": (34.85, -82.40), "Albany-Schenectady-Troy": (42.65, -73.75),
    "Madison": (43.07, -89.40), "Ft. Wayne": (41.08, -85.14),
    "Traverse City-Cadillac": (44.76, -85.62), "New Orleans": (29.95, -90.07),
    "Biloxi-Gulfport": (30.40, -88.89),
}


def build_markets_map_figure(markets_df: pd.DataFrame) -> go.Figure | None:
    """A real geographic bubble map of feeder markets, sized by their share
    of visitor spend/trips, centered on Dana Point. Returns None if none of
    the current markets_df rows have a known coordinate, so callers can fall
    back to the bar chart alone rather than show an empty map."""
    if markets_df.empty:
        return None
    mapped = markets_df.assign(
        lat=markets_df["dma"].map(lambda d: DMA_COORDS.get(d, (None, None))[0]),
        lon=markets_df["dma"].map(lambda d: DMA_COORDS.get(d, (None, None))[1]),
    ).dropna(subset=["lat", "lon"])
    if mapped.empty:
        return None
    max_share = mapped["share_pct"].max() or 1.0
    fig = go.Figure()
    fig.add_trace(go.Scattergeo(
        lon=mapped["lon"], lat=mapped["lat"],
        text=mapped.apply(lambda r: f"{r['dma']}: {r['share_pct']:.1f}%", axis=1),
        hoverinfo="text",
        marker=dict(
            size=(mapped["share_pct"] / max_share) * 34 + 8,
            color="#1D6E86", opacity=0.75,
            line=dict(width=1, color="#123C4A"),
        ),
        mode="markers",
    ))
    fig.add_trace(go.Scattergeo(
        lon=[-117.698], lat=[33.467], text=["Dana Point"], hoverinfo="text",
        marker=dict(size=14, color="#B45309", symbol="star", line=dict(width=1, color="#7A3406")),
        mode="markers",
    ))
    fig.update_geos(
        scope="usa", showland=True, landcolor="#F1F5F9", showlakes=True,
        lakecolor="#E2E8F0", showsubunits=True, subunitcolor="#CBD5E1",
        bgcolor="rgba(0,0,0,0)",
    )
    fig.update_layout(
        # No in-plot title: the caller already renders "Where Dana Point's
        # Visitors Come From" as a markdown heading above this chart. A
        # second, plotly-native title here used to double up on it, and its
        # small fixed top margin (t=30) had no room for that title text to
        # wrap on narrow/portrait mobile widths -- the wrapped second line
        # clipped straight into the map. Markdown wraps naturally with the
        # container, so keeping the title there instead of in the figure
        # sidesteps the problem entirely.
        height=340, margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#0B2530", family="-apple-system, Segoe UI, sans-serif"),
        showlegend=False,
    )
    return fig


@st.cache_data(ttl=1800, show_spinner=False)
def load_datafy_spending_df(limit: int = 8) -> pd.DataFrame:
    try:
        conn = get_connection()
        return pd.read_sql_query(
            "SELECT category, spend_share_pct FROM datafy_overview_spending_by_category "
            "ORDER BY report_period_start DESC, spend_share_pct DESC LIMIT ?",
            conn,
            params=(limit,),
        )
    except Exception:
        return pd.DataFrame()


_MONTH_ABBR_TO_NUM = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


@st.cache_data(ttl=1800, show_spinner=False)
def load_datafy_spending_trend_df(
    months: float = 12, start_date: str | None = None, end_date: str | None = None
) -> pd.DataFrame:
    """datafy_overview_spending_by_month is the one Datafy table that carries
    real month-by-month rows, unlike top_markets and spending_by_category,
    which are one fixed annual snapshot. This is the genuinely fresh Datafy
    series, so it is the one that should respond to the data-window filter.

    The table's own `month` column is a 3-letter name ("Jan".."Dec"), not a
    number, so the month-name conversion and chronological sort happen in
    pandas instead of SQL. Trailing months that have not been reported yet
    come through as a literal 0.0 (a placeholder, not a real $0 of visitor
    spend) and are dropped rather than plotted as a cliff to zero."""
    try:
        conn = get_connection()
        df = pd.read_sql_query(
            "SELECT year, month, spending_usd FROM datafy_overview_spending_by_month", conn
        )
        if df.empty:
            return df
        df["month_num"] = df["month"].map(_MONTH_ABBR_TO_NUM)
        df = df.dropna(subset=["month_num"])
        df = df[df["spending_usd"] > 0]
        if df.empty:
            return df
        df["month_num"] = df["month_num"].astype(int)
        df["period"] = pd.to_datetime(dict(year=df["year"], month=df["month_num"], day=1))
        df = df.sort_values("period")
        if start_date and end_date:
            df = df[(df["period"] >= pd.to_datetime(start_date)) & (df["period"] <= pd.to_datetime(end_date))]
        else:
            latest_period = df["period"].max()
            cutoff_period = latest_period - pd.DateOffset(months=months)
            df = df[df["period"] >= cutoff_period]
        df = df.reset_index(drop=True)
        df["month_label"] = df["period"].dt.strftime("%Y-%m")
        return df[["year", "month", "spending_usd", "month_label"]]
    except Exception:
        return pd.DataFrame()


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
    if not reports:
        # Nothing archived yet -- an empty "Past Issues (0)" expander reads as
        # broken rather than simply new, so show nothing until there's a
        # first report to list.
        return
    with st.expander(f"\U0001F4C1 Report Repository — Past Issues ({len(reports)})", expanded=False):
        st.caption(
            "Every generated report is kept here. Moving to a synced SharePoint "
            "folder is next; this local archive is the repository until then."
        )
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
    {"icon": "📊", "title": "Executive Summary",
     "desc": "Headline KPIs, YoY change, and this week's top DMO insight.", "page": 1},
    {"icon": "🏨", "title": "Hotel Performance — Occupancy",
     "desc": "Occupancy by day of week and the 6-market RevPAR comp set.", "page": 2},
    {"icon": "💵", "title": "Hotel Performance — ADR & Compression",
     "desc": "Average daily rate by day of week and compression days by quarter.", "page": 3},
    {"icon": "🌎", "title": "Visitor Origins",
     "desc": "Top feeder markets to Dana Point and why visitors choose it.", "page": 4},
    {"icon": "🏢", "title": "Market Segments",
     "desc": "CoStar chain-scale occupancy and RevPAR by tier for the Newport Beach/Dana Point submarket.", "page": 5},
    {"icon": "🏬", "title": "Chain-Scale Segment Detail",
     "desc": "Segment-level performance and the TBID/TOT tax estimate.", "page": 6},
    {"icon": "🧳", "title": "Visitor Profile & Spend",
     "desc": "Datafy visitor demographics, category spend, and length of stay.", "page": 7},
    {"icon": "📈", "title": "Forward Outlook & Group Business",
     "desc": "What's ahead for compression, group bookings, and travel trends.", "page": 8},
    {"icon": "📝", "title": "Notes & Commentary",
     "desc": "Team commentary added live in the dashboard, carried into the report automatically.", "page": 9},
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
  <b>Notes.</b> Scroll to the Notes and Report Repository section near the bottom of the page to
  add commentary on the data, such as an explanation for a dip in occupancy or context behind a
  spending trend. Every note saved there carries automatically into page nine of the PDF, titled
  Notes and Commentary, the next time the report is generated or regenerated. No separate step is
  needed to get a note into the report.
</div>
<div class="pulse-help-item">
  <b>Report Repository.</b> Once a report has been generated more than once, this same section
  keeps a dated copy of each past PDF, so earlier versions stay available for comparison.
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


@st.cache_data(ttl=3600, show_spinner=False)
def _render_page_images(pdf_bytes: bytes) -> list[str]:
    """Rasterize every page to a base64 PNG data URI for the flipbook viewer."""
    import fitz  # PyMuPDF

    images: list[str] = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        for page in doc:
            pix = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0))
            b64 = base64.b64encode(pix.tobytes("png")).decode("ascii")
            images.append(f"data:image/png;base64,{b64}")
    finally:
        doc.close()
    return images


def _flipbook_html(page_images: list[str], height: int = 760) -> str:
    """An Issuu-style page-by-page viewer: one page at a time, prev/next,
    a page counter, and a thumbnail strip, built from pre-rendered page
    images so it never depends on the browser's native PDF plugin."""
    pages_json = "[" + ",".join(f'"{p}"' for p in page_images) + "]"
    thumbs_html = "".join(
        f'<img class="pf-thumb" data-i="{i}" src="{p}" onclick="pfGo({i})">'
        for i, p in enumerate(page_images)
    )
    return f"""
    <div class="pf-wrap">
      <div class="pf-stage">
        <button class="pf-arrow pf-prev" onclick="pfPrev()" aria-label="Previous page">&#10094;</button>
        <div class="pf-book">
          <img id="pf-page" class="pf-page" src="{page_images[0] if page_images else ''}">
          <div id="pf-shade" class="pf-shade"></div>
        </div>
        <button class="pf-arrow pf-next" onclick="pfNext()" aria-label="Next page">&#10095;</button>
      </div>
      <div class="pf-bar">
        <span id="pf-counter">Page 1 of {len(page_images)}</span>
      </div>
      <div class="pf-thumbs">{thumbs_html}</div>
    </div>
    <style>
      .pf-wrap {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
      .pf-stage {{ position:relative; display:flex; align-items:center; justify-content:center;
        background:#0B2530; border-radius:14px; padding:18px; min-height:{height}px;
        perspective:1800px; }}
      .pf-book {{ position:relative; max-width:92%; max-height:{height - 36}px; }}
      .pf-page {{ display:block; max-width:100%; max-height:{height - 36}px; width:auto; height:auto;
        box-shadow:0 12px 34px rgba(0,0,0,0.45); border-radius:4px; background:#fff;
        transform-style:preserve-3d; backface-visibility:hidden; }}
      .pf-shade {{ position:absolute; inset:0; border-radius:4px; opacity:0; pointer-events:none;
        background:linear-gradient(90deg, rgba(0,0,0,0.35) 0%, rgba(0,0,0,0) 35%); }}
      .pf-page.pf-turn-next {{ animation: pfTurnNext 0.46s ease-in-out; transform-origin:left center; }}
      .pf-page.pf-turn-prev {{ animation: pfTurnPrev 0.46s ease-in-out; transform-origin:right center; }}
      .pf-shade.pf-turn-next, .pf-shade.pf-turn-prev {{ animation: pfShade 0.46s ease-in-out; }}
      @keyframes pfTurnNext {{
        0%   {{ transform: rotateY(0deg); }}
        50%  {{ transform: rotateY(-92deg); }}
        50.1%{{ transform: rotateY(92deg); }}
        100% {{ transform: rotateY(0deg); }}
      }}
      @keyframes pfTurnPrev {{
        0%   {{ transform: rotateY(0deg); }}
        50%  {{ transform: rotateY(92deg); }}
        50.1%{{ transform: rotateY(-92deg); }}
        100% {{ transform: rotateY(0deg); }}
      }}
      @keyframes pfShade {{
        0% {{ opacity:0; }} 45% {{ opacity:0.9; }} 55% {{ opacity:0.9; }} 100% {{ opacity:0; }}
      }}
      .pf-arrow {{ background:rgba(255,255,255,0.12); color:#fff; border:1px solid rgba(255,255,255,0.25);
        border-radius:50%; width:44px; height:44px; font-size:16px; cursor:pointer;
        position:absolute; top:50%; transform:translateY(-50%); z-index:5; }}
      .pf-arrow:hover {{ background:rgba(255,255,255,0.28); }}
      .pf-arrow:disabled {{ opacity:0.35; cursor:default; }}
      .pf-prev {{ left:8px; }}
      .pf-next {{ right:8px; }}
      .pf-bar {{ text-align:center; padding:12px 0 6px 0; color:#0B2530; font-weight:700; font-size:13px; }}
      .pf-thumbs {{ display:flex; gap:8px; overflow-x:auto; padding:8px 4px 4px 4px; -webkit-overflow-scrolling:touch; }}
      .pf-thumb {{ height:64px; border-radius:5px; border:2px solid transparent; cursor:pointer;
        opacity:0.6; transition:opacity 0.15s ease, border-color 0.15s ease; flex-shrink:0; }}
      .pf-thumb:hover {{ opacity:0.9; }}
      .pf-thumb.pf-active {{ opacity:1; border-color:#1D6E86; }}
      @media (max-width: 768px) {{
        .pf-stage {{ min-height:{max(280, int(height * 0.50))}px; padding:8px; gap:0; }}
        .pf-book, .pf-page {{ max-height:{max(240, int((height - 36) * 0.50))}px; }}
        .pf-arrow {{ width:32px; height:32px; font-size:12px; left:4px !important; right:auto; }}
        .pf-next {{ right:4px !important; left:auto; }}
        .pf-bar {{ padding:10px 0 4px 0; font-size:12px; }}
        .pf-thumbs {{ padding:6px 2px 2px 2px; gap:6px; }}
        .pf-thumb {{ height:48px; }}
      }}
      @media (max-width: 480px) {{
        .pf-stage {{ min-height:{max(220, int(height * 0.42))}px; padding:6px; gap:0; }}
        .pf-book, .pf-page {{ max-height:{max(200, int((height - 36) * 0.42))}px; }}
        .pf-arrow {{ width:28px; height:28px; font-size:11px; left:2px !important; right:auto; }}
        .pf-next {{ right:2px !important; left:auto; }}
        .pf-bar {{ padding:8px 0 3px 0; font-size:11px; }}
        .pf-thumbs {{ padding:4px 2px 2px 2px; gap:4px; }}
        .pf-thumb {{ height:40px; }}
      }}
    </style>
    <script>
      const pfPages = {pages_json};
      let pfIdx = 0;
      let pfBusy = false;
      function pfSyncChrome() {{
        document.getElementById('pf-counter').innerText = 'Page ' + (pfIdx + 1) + ' of ' + pfPages.length;
        document.querySelectorAll('.pf-thumb').forEach((el, i) => {{
          el.classList.toggle('pf-active', i === pfIdx);
        }});
        document.querySelector('.pf-prev').disabled = pfIdx === 0;
        document.querySelector('.pf-next').disabled = pfIdx === pfPages.length - 1;
      }}
      function pfTurn(newIdx, direction) {{
        if (pfBusy || newIdx === pfIdx || newIdx < 0 || newIdx >= pfPages.length) return;
        pfBusy = true;
        const img = document.getElementById('pf-page');
        const shade = document.getElementById('pf-shade');
        const cls = direction > 0 ? 'pf-turn-next' : 'pf-turn-prev';
        img.classList.add(cls);
        shade.classList.add(cls);
        setTimeout(() => {{ img.src = pfPages[newIdx]; }}, 220);
        setTimeout(() => {{
          pfIdx = newIdx;
          pfSyncChrome();
          img.classList.remove(cls);
          shade.classList.remove(cls);
          pfBusy = false;
        }}, 460);
      }}
      function pfGo(i) {{ pfTurn(i, i > pfIdx ? 1 : -1); }}
      function pfPrev() {{ pfTurn(pfIdx - 1, -1); }}
      function pfNext() {{ pfTurn(pfIdx + 1, 1); }}
      document.addEventListener('keydown', (e) => {{
        if (e.key === 'ArrowLeft') pfPrev();
        if (e.key === 'ArrowRight') pfNext();
      }});
      pfSyncChrome();
    </script>
    """


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


col1, col2, col3, col4, col5 = st.columns([3.3, 1, 1, 1.2, 0.5])
with col1:
    header_slot = st.empty()
    # Shown immediately, before this run's report generation has actually
    # happened, so it can't yet reflect a real timestamp. Replaced below
    # with the true "Last generated" stamp once generation completes.
    header_slot.markdown(_header_html("Generating latest report&hellip;"), unsafe_allow_html=True)
with col2:
    summarize_clicked = st.button("Summarize", use_container_width=True)
with col3:
    regenerate = st.button("Regenerate", use_container_width=True)
with col4:
    # Reserved now, filled in below once pdf_bytes exists -- keeps Download
    # PDF grouped with Summarize/Regenerate instead of stranded lower on
    # the page, even though the file itself isn't ready until after this
    # row renders.
    download_slot = st.empty()
with col5:
    with st.popover("❓", use_container_width=True):
        st.markdown(HELP_HTML, unsafe_allow_html=True)

# Data window -- set here, before the report is built, so the KPI tiles,
# trend charts, Datafy visitor-origins section, and Intelligence Brief below
# all reflect the SAME selected period. Only "Custom range" overrides the
# PDF's own date_range (cover, Executive Summary, Hotel Performance pages);
# every preset window lets the PDF use its own freshest default while still
# driving every native Streamlit chart on this page.
PERIOD_OPTIONS = {
    "This week": 7 / 30,
    "Last 30 days": 1,
    "Last 3 months": 3,
    "Last 6 months": 6,
    "Last 12 months": 12,
    "Last 24 months": 24,
}
period_label = st.radio(
    "Data window (drives KPI tiles, charts, and the Intelligence Brief below)",
    list(PERIOD_OPTIONS.keys()) + ["Custom range"], index=4, horizontal=True, key="pulse_period",
)

range_start_iso, range_end_iso = None, None
if period_label == "Custom range":
    cust_c1, cust_c2 = st.columns(2)
    with cust_c1:
        custom_start_d = st.date_input(
            "Start date", value=date.today() - timedelta(days=90),
            max_value=date.today(), key="custom_start_date",
        )
    with cust_c2:
        custom_end_d = st.date_input(
            "End date", value=date.today(), max_value=date.today(), key="custom_end_date",
        )
    if custom_start_d > custom_end_d:
        custom_start_d, custom_end_d = custom_end_d, custom_start_d
        st.caption("Start date was after end date, swapped them.")
    range_start_iso = custom_start_d.isoformat()
    range_end_iso = custom_end_d.isoformat()
    window_months = max((custom_end_d - custom_start_d).days, 1) / 30.0
    period_label_display = f"{range_start_iso} to {range_end_iso}"
else:
    window_months = PERIOD_OPTIONS[period_label]
    period_label_display = period_label

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

download_slot.download_button(
    "⬇ Download PDF",
    data=pdf_bytes,
    file_name=f"Visit Dana Point PULSE Report {datetime.now().strftime('%Y-%m-%d')}.pdf",
    mime="application/pdf",
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Jump-to-section nav -- lets anyone see everything this page offers at a
# glance and go straight to it, instead of scrolling past every section.
# ---------------------------------------------------------------------------

st.markdown(
    """
    <div class="pulse-jumpnav">
      <a href="#pulse-snapshot">\U0001F4CA Performance Snapshot</a>
      <a href="#pulse-origins">\U0001F30E Visitor Origins &amp; Spend</a>
      <a href="#pulse-market">\U0001F3E2 Market Performance</a>
      <a href="#pulse-forward">\U0001F4C8 Forward Outlook &amp; Group Business</a>
      <a href="#pulse-brain">\U0001F9E0 Intelligence Brief</a>
      <a href="#pulse-fullreport">\U0001F4C4 Full Report</a>
      <a href="#pulse-notes">\U00002B07 Notes &amp; Downloads</a>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Performance Snapshot -- KPI tiles and a trend chart read straight from the
# database, not the static PDF, so the data window above drives a real
# chart, not just PDF page selection.
# ---------------------------------------------------------------------------

st.markdown('<div id="pulse-snapshot" class="pulse-anchor"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">\U0001F4CA Performance Snapshot</div>', unsafe_allow_html=True)
st.caption(f"Showing {period_label_display.lower()}. Change the data window above to update this section and the Intelligence Brief below.")

period_kpi = load_kpi_period_stats(window_months, start_date=range_start_iso, end_date=range_end_iso)
kpi_c1, kpi_c2, kpi_c3 = st.columns(3)
if period_kpi:
    # Performance Snapshot - Large, bold metrics
    occ_delta = f"{period_kpi['occ_yoy']:+.1f} pts YoY" if pd.notna(period_kpi.get("occ_yoy")) else None
    adr_delta = f"{period_kpi['adr_yoy']:+.1f}% YoY" if pd.notna(period_kpi.get("adr_yoy")) else None
    revpar_delta = f"{period_kpi['revpar_yoy']:+.1f}% YoY" if pd.notna(period_kpi.get("revpar_yoy")) else None

    with kpi_c1:
        st.markdown(
            f'<div style="text-align:center; padding:20px 12px;">'
            f'<div style="font-size:13px; color:#6B7280; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Occupancy</div>'
            f'<div style="font-size:42px; color:#0B2530; font-weight:800; line-height:1.1;">{period_kpi["occ_pct"]:.1f}%</div>'
            f'<div style="font-size:13px; color:#059669; margin-top:8px; font-weight:600;">'
            f'{"↑" if occ_delta and occ_delta.startswith("+") else "↓"} {occ_delta if occ_delta else "—"}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
    with kpi_c2:
        st.markdown(
            f'<div style="text-align:center; padding:20px 12px;">'
            f'<div style="font-size:13px; color:#6B7280; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">ADR</div>'
            f'<div style="font-size:42px; color:#0B2530; font-weight:800; line-height:1.1;">${period_kpi["adr"]:,.0f}</div>'
            f'<div style="font-size:13px; color:#059669; margin-top:8px; font-weight:600;">'
            f'{"↑" if adr_delta and adr_delta.startswith("+") else "↓"} {adr_delta if adr_delta else "—"}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
    with kpi_c3:
        st.markdown(
            f'<div style="text-align:center; padding:20px 12px;">'
            f'<div style="font-size:13px; color:#6B7280; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">RevPAR</div>'
            f'<div style="font-size:42px; color:#0B2530; font-weight:800; line-height:1.1;">${period_kpi["revpar"]:,.0f}</div>'
            f'<div style="font-size:13px; color:#059669; margin-top:8px; font-weight:600;">'
            f'{"↑" if revpar_delta and revpar_delta.startswith("+") else "↓"} {revpar_delta if revpar_delta else "—"}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
    st.caption(
        f"Averaged over {period_kpi['n_days']} STR-reported days, "
        f"{period_kpi['period_start']} to {period_kpi['period_end']}."
    )
else:
    st.info("STR KPI data is not available yet.")

trend_df = load_kpi_trend_df(window_months, start_date=range_start_iso, end_date=range_end_iso)
if not trend_df.empty:
    st.markdown(
        f'<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:2px;">'
        f"STR Occupancy &amp; ADR Trend &mdash; {html.escape(period_label_display)}</div>",
        unsafe_allow_html=True,
    )
    trend_fig = go.Figure()
    trend_fig.add_trace(go.Scatter(x=trend_df["month"], y=trend_df["occ"], name="Occupancy %",
                                    line=dict(color="#1D6E86", width=3), yaxis="y1"))
    trend_fig.add_trace(go.Bar(x=trend_df["month"], y=trend_df["adr"], name="ADR ($)",
                                marker=dict(color="#B45309"), opacity=0.55, yaxis="y2"))
    trend_fig.update_layout(
        # t=56 (up from 40): the 2-item horizontal legend above the plot
        # (y=1.02) can wrap to two rows on narrow/portrait mobile widths,
        # and the old margin only had room for one -- the second legend row
        # clipped into the chart. This gives it enough headroom to wrap
        # without overlapping.
        height=340, margin=dict(l=10, r=10, t=56, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#0B2530", family="-apple-system, Segoe UI, sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        yaxis=dict(title="Occupancy %", showgrid=True, gridcolor="#E2E8F0"),
        yaxis2=dict(title="ADR ($)", overlaying="y", side="right", showgrid=False),
    )
    st.plotly_chart(trend_fig, use_container_width=True, config={"displayModeBar": False})
else:
    st.info("STR monthly trend data is not available yet.")

st.divider()

# ---------------------------------------------------------------------------
# Visitor Origins & Spend -- Datafy's monthly spend trend (real, responds to
# the data window above), then the latest snapshot pull: a geographic bubble
# map of feeder markets alongside a bar chart, and spend by category.
# ---------------------------------------------------------------------------

st.markdown('<div id="pulse-origins" class="pulse-anchor"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">\U0001F30E Visitor Origins &amp; Spend</div>', unsafe_allow_html=True)

spend_trend_df = load_datafy_spending_trend_df(window_months, start_date=range_start_iso, end_date=range_end_iso)
if not spend_trend_df.empty:
    st.markdown(
        '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:4px;">'
        "Datafy Monthly Visitor Spend Trend</div>",
        unsafe_allow_html=True,
    )
    sc1, sc2 = st.columns(2)
    latest_month = spend_trend_df.iloc[-1]["month_label"]
    latest_spend = spend_trend_df.iloc[-1]["spending_usd"]
    avg_spend = spend_trend_df["spending_usd"].mean()

    with sc1:
        st.markdown(
            f'<div style="text-align:center; padding:16px 12px;">'
            f'<div style="font-size:12px; color:#6B7280; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px;">Latest Month</div>'
            f'<div style="font-size:32px; color:#0B2530; font-weight:800; line-height:1.1;">{latest_month}</div>'
            f'<div style="font-size:13px; color:#059669; margin-top:6px; font-weight:600;">✓ ${latest_spend:,.0f}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
    with sc2:
        st.markdown(
            f'<div style="text-align:center; padding:16px 12px;">'
            f'<div style="font-size:12px; color:#6B7280; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px;">{period_label_display} Avg.</div>'
            f'<div style="font-size:32px; color:#0B2530; font-weight:800; line-height:1.1;">${avg_spend:,.0f}</div>'
            f'<div style="font-size:12px; color:#6B7280; margin-top:6px; font-weight:500;">/month</div>'
            f'</div>',
            unsafe_allow_html=True
        )
    spend_trend_fig = go.Figure(go.Scatter(
        x=spend_trend_df["month_label"], y=spend_trend_df["spending_usd"],
        mode="lines+markers", line=dict(color="#1D6E86", width=3),
        marker=dict(size=6),
    ))
    spend_trend_fig.update_layout(
        height=260, margin=dict(l=10, r=10, t=20, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#0B2530", family="-apple-system, Segoe UI, sans-serif"),
        yaxis=dict(title="Visitor spend ($)", showgrid=True, gridcolor="#E2E8F0"),
    )
    st.plotly_chart(spend_trend_fig, use_container_width=True, config={"displayModeBar": False})
    st.caption(
        f"This one Datafy series is real month-by-month data and does respond to the {period_label_display.lower()} "
        "window above. The map and category breakdowns below use Datafy's latest available snapshot pull and do not."
    )
    st.divider()

try:
    datafy_periods = pd.read_sql_query(
        "SELECT report_period_start, report_period_end FROM datafy_overview_spending_by_market "
        "ORDER BY report_period_start DESC LIMIT 1",
        get_connection(),
    )
except Exception:
    datafy_periods = pd.DataFrame()
datafy_period_str = (
    f"{datafy_periods.iloc[0]['report_period_start']} to {datafy_periods.iloc[0]['report_period_end']}"
    if not datafy_periods.empty else "period unavailable"
)
st.markdown(
    f"""
    <div style="background:#FFFBEB; border:1px solid #FDE68A; border-left:4px solid #D97706;
                border-radius:10px; padding:10px 16px; margin-bottom:14px; font-size:12.5px;
                color:#78350F; line-height:1.5;">
      <b>\U0001F4CC Datafy Visitor Economy:</b> the map and charts below cover
      <b>{datafy_period_str}</b>, the most recent period Datafy has published. Datafy releases
      new visitor-origin and spending data periodically, not daily, so this section always shows
      its latest available pull no matter which date window is selected above. The STR chart and
      Performance Snapshot above it do follow the date window.
    </div>
    """,
    unsafe_allow_html=True,
)

markets_df = load_datafy_markets_df()
if not markets_df.empty:
    top_row = markets_df.iloc[0]
    mc1, mc2 = st.columns([1.3, 1])
    with mc1:
        st.markdown(
            f'<div style="border-left:4px solid #1D6E86; padding-left:12px;">'
            f'<div style="font-size:11px; color:#6B7280; font-weight:600; text-transform:uppercase; margin-bottom:4px;">Top Market</div>'
            f'<div style="font-size:14px; color:#0B2530; font-weight:700; line-height:1.4; word-wrap:break-word;">{top_row["dma"]}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
    mc2.metric(top_row["metric"], f"{top_row['share_pct']:.1f}%")
    # Tiled-basemap flow map (bubbles graduated and connected to Dana Point).
    # build_markets_map_figure below is the previous flat Scattergeo outline,
    # kept as the fallback for any environment where the newer map traces or
    # their basemap tiles are unavailable.
    map_fig = None
    if section_visuals is not None:
        try:
            map_fig = section_visuals.build_feeder_market_map(markets_df, DMA_COORDS)
        except Exception:
            map_fig = None
    if map_fig is None:
        map_fig = build_markets_map_figure(markets_df)
    if map_fig is not None:
        st.markdown(
            '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:4px;">'
            "Where Dana Point&rsquo;s Visitors Come From</div>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(map_fig, use_container_width=True, config={"displayModeBar": False})
        st.caption(
            "Dana Point is the amber marker on the coast. Every teal bubble is an origin "
            "market, sized and shaded by its share of visitor spend, and the weight of each "
            "connector carries the same value, so the heaviest lines are the markets sending "
            "the most spend into the destination. Hover any bubble for its exact share."
        )
    # markets_df is already ranked largest-share-first and the y-axis below
    # is reversed, so row order and top-to-bottom draw order match: index 0
    # (the leading market) gets the first palette color, no reversal needed.
    _mkt_colors = (
        [section_visuals.CATEGORY_COLORS[i % len(section_visuals.CATEGORY_COLORS)]
         for i in range(len(markets_df))]
        if section_visuals is not None else "#1D6E86"
    )
    st.markdown(
        '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:4px;">'
        "Top Visitor Origin Markets</div>",
        unsafe_allow_html=True,
    )
    mkt_fig = go.Figure(go.Bar(
        x=markets_df["share_pct"], y=markets_df["dma"], orientation="h",
        marker=dict(color=_mkt_colors),
    ))
    mkt_fig.update_layout(
        # Title moved to the markdown heading above -- a plotly-native title
        # here needed 2 lines on narrow/portrait widths and this chart's
        # small top margin clipped the wrap. Markdown text reflows with the
        # container instead of clipping.
        height=280, margin=dict(l=10, r=10, t=16, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#0B2530", family="-apple-system, Segoe UI, sans-serif"),
        xaxis=dict(title=f"{top_row['metric']} (%)", showgrid=True, gridcolor="#E2E8F0"),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(mkt_fig, use_container_width=True, config={"displayModeBar": False})
else:
    st.info("Datafy visitor origin data is not available yet.")

spend_df = load_datafy_spending_df()
if not spend_df.empty:
    top_row = spend_df.iloc[0]
    sc1, sc2 = st.columns([1.4, 1])
    with sc1:
        st.markdown(
            f'<div style="border-left:4px solid #1D6E86; padding-left:12px;">'
            f'<div style="font-size:11px; color:#6B7280; font-weight:600; text-transform:uppercase; margin-bottom:4px;">Top Category</div>'
            f'<div style="font-size:14px; color:#0B2530; font-weight:700; line-height:1.4; word-wrap:break-word;">{top_row["category"]}</div>'
            f'</div>',
            unsafe_allow_html=True
        )
    sc2.metric("Spend Share", f"{top_row['spend_share_pct'] * 100:.1f}%")
    st.markdown(
        '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:4px;">'
        "Visitor Spend by Category</div>",
        unsafe_allow_html=True,
    )
    spend_fig = go.Figure(go.Pie(
        labels=spend_df["category"], values=spend_df["spend_share_pct"], hole=0.55,
        marker=dict(colors=["#1D6E86", "#123C4A", "#B45309", "#1D9E6F", "#7FD6C4", "#475569", "#94A3B8", "#CBD9DE"]),
    ))
    spend_fig.update_layout(
        # Title moved to the markdown heading above -- see mkt_fig comment
        # a few lines up for why: same wrap-and-clip failure on portrait
        # mobile widths, same fix.
        height=280, margin=dict(l=10, r=10, t=16, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#0B2530", family="-apple-system, Segoe UI, sans-serif"),
    )
    st.plotly_chart(spend_fig, use_container_width=True, config={"displayModeBar": False})
else:
    st.info("Datafy spending data is not available yet.")

st.divider()

# ---------------------------------------------------------------------------
# Market Performance -- CoStar submarket KPIs and the tier-level RevPAR and
# room-inventory figures also used on the Market Segments / Chain-Scale
# Segment Detail section cards, shown here at full size with a multi-year
# trend for context. All from real, parsed CoStar submarket PDFs.
# ---------------------------------------------------------------------------

st.markdown('<div id="pulse-market" class="pulse-anchor"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">\U0001F3E2 Market Performance</div>', unsafe_allow_html=True)

if section_visuals is not None:
    _mkt_kpis = section_visuals.costar_overall_kpis(get_connection())
else:
    _mkt_kpis = None

if _mkt_kpis:
    mp_c1, mp_c2, mp_c3 = st.columns(3)
    mp_c1.metric("Submarket Occupancy (YTD)", f"{_mkt_kpis['occupancy_pct']:.1f}%",
                 delta=f"{_mkt_kpis['occ_yoy_pct']:+.1f} pts YoY" if pd.notna(_mkt_kpis.get('occ_yoy_pct')) else None)
    mp_c2.metric("Submarket ADR (YTD)", f"${_mkt_kpis['adr_usd']:,.0f}",
                 delta=f"{_mkt_kpis['adr_yoy_pct']:+.1f}% YoY" if pd.notna(_mkt_kpis.get('adr_yoy_pct')) else None)
    mp_c3.metric("Submarket RevPAR (YTD)", f"${_mkt_kpis['revpar_usd']:,.0f}",
                 delta=f"{_mkt_kpis['revpar_yoy_pct']:+.1f}% YoY" if pd.notna(_mkt_kpis.get('revpar_yoy_pct')) else None)
    st.caption(f"Newport Beach/Dana Point submarket, Overall scope. Source: CoStar, {_mkt_kpis['report_date']}.")

    mp_col1, mp_col2 = st.columns(2)
    with mp_col1:
        st.markdown(
            '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:2px;">'
            "RevPAR by Chain-Scale Tier</div>", unsafe_allow_html=True,
        )
        tier_result = section_visuals._fig_costar_tiers(get_connection(), height=300)
        if tier_result is not None:
            tier_fig, tier_caption = tier_result
            st.plotly_chart(tier_fig, use_container_width=True, config={"displayModeBar": False}, key="market_tier_fig")
            st.caption(tier_caption)
        else:
            st.info("CoStar chain-scale tier data is not available yet.")
    with mp_col2:
        st.markdown(
            '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:2px;">'
            "Room Inventory by Tier</div>", unsafe_allow_html=True,
        )
        room_result = section_visuals._fig_room_split(get_connection(), height=300)
        if room_result is not None:
            room_fig, room_caption = room_result
            st.plotly_chart(room_fig, use_container_width=True, config={"displayModeBar": False}, key="market_room_fig")
            st.caption(room_caption)
        else:
            st.info("CoStar room inventory data is not available yet.")

    trend_result = section_visuals.fig_costar_overall_trend(get_connection(), height=260)
    if trend_result is not None:
        trend_fig2, trend_caption2 = trend_result
        st.markdown(
            '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:2px;">'
            "Multi-Year RevPAR Trend</div>", unsafe_allow_html=True,
        )
        st.plotly_chart(trend_fig2, use_container_width=True, config={"displayModeBar": False}, key="market_trend_fig")
        st.caption(trend_caption2)
else:
    st.info("CoStar submarket data is not available yet.")

st.divider()

# ---------------------------------------------------------------------------
# Forward Outlook & Group Business -- built entirely from real, live sources:
# STR's own group-segment occupancy mix, the STR compression history, the
# seeded VDP events calendar, and the pipeline's own generated insights.
# Deliberately does not surface group_intelligence's dollar projections,
# since that table is derived from costar_chain_scale_breakdown /
# costar_competitive_set, both documented as hardcoded baseline data rather
# than a parsed CoStar export. See section_visuals.py and CLAUDE.md Lessons
# Learned for the full reasoning.
# ---------------------------------------------------------------------------

st.markdown('<div id="pulse-forward" class="pulse-anchor"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">\U0001F4C8 Forward Outlook &amp; Group Business</div>', unsafe_allow_html=True)

if section_visuals is not None:
    _fo_conn = get_connection()
    _fo_events = section_visuals.upcoming_events_df(_fo_conn, limit=1)
    _fo_compression = section_visuals._compression(_fo_conn, quarters=1)
    _fo_group = section_visuals._group_mix(_fo_conn)
else:
    _fo_events = pd.DataFrame()
    _fo_compression = pd.DataFrame()
    _fo_group = pd.DataFrame()

fo_c1, fo_c2, fo_c3 = st.columns([1.2, 1, 1])
if not _fo_events.empty:
    _ev = _fo_events.iloc[0]
    _days_out = (pd.to_datetime(_ev["event_date"]) - pd.Timestamp.now().normalize()).days
    with fo_c1:
        st.markdown(
            f'<div style="border-left:4px solid #1D6E86; padding-left:12px;">'
            f'<div style="font-size:11px; color:#6B7280; font-weight:600; text-transform:uppercase; margin-bottom:4px;">Next Major Event</div>'
            f'<div style="font-size:15px; color:#0B2530; font-weight:700; line-height:1.4; word-wrap:break-word;">{_ev["event_name"]}</div>'
            f'<div style="font-size:12px; color:#059669; margin-top:6px;">↑ in {_days_out} days</div>'
            f'</div>',
            unsafe_allow_html=True
        )
else:
    with fo_c1:
        st.markdown(
            f'<div style="border-left:4px solid #9CA3AF; padding-left:12px;">'
            f'<div style="font-size:11px; color:#6B7280; font-weight:600; text-transform:uppercase; margin-bottom:4px;">Next Major Event</div>'
            f'<div style="font-size:14px; color:#6B7280; font-weight:500;">None scheduled</div>'
            f'</div>',
            unsafe_allow_html=True
        )
if not _fo_compression.empty:
    _cq = _fo_compression.iloc[0]
    fo_c2.metric(f"{_cq['quarter']} Compression", f"{int(_cq['days_above_80_occ'])} days 80%+",
                 delta=f"{int(_cq['days_above_90_occ'])} days 90%+")
else:
    fo_c2.metric("Compression This Quarter", "N/A")
if not _fo_group.empty and _fo_group["occ_pct"].sum() > 0:
    _grp_row = _fo_group[_fo_group["segment"] == "Grp."]
    _grp_pct = (_grp_row["occ_pct"].iloc[0] / _fo_group["occ_pct"].sum() * 100) if not _grp_row.empty else None
    fo_c3.metric("Group Share of Occupancy", f"{_grp_pct:.0f}%" if _grp_pct is not None else "N/A")
else:
    fo_c3.metric("Group Share of Occupancy", "N/A")
st.caption("Event calendar and STR sources, current as of this page's most recent data load.")

fo_col1, fo_col2 = st.columns(2)
with fo_col1:
    st.markdown(
        '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:2px;">'
        "Occupancy Mix, Latest STR Week</div>", unsafe_allow_html=True,
    )
    gm_result = section_visuals._fig_group_mix(get_connection(), height=180) if section_visuals is not None else None
    if gm_result is not None:
        gm_fig, gm_caption = gm_result
        st.plotly_chart(gm_fig, use_container_width=True, config={"displayModeBar": False}, key="forward_group_fig")
        st.caption(gm_caption)
    else:
        st.info("STR group-segment data is not available yet.")
with fo_col2:
    st.markdown(
        '<div style="font-weight:700; font-size:14.5px; color:#0B2530; margin-bottom:2px;">'
        "Compression Days by Quarter</div>", unsafe_allow_html=True,
    )
    cq_result = section_visuals._fig_compression(get_connection(), quarters=8, height=280) if section_visuals is not None else None
    if cq_result is not None:
        cq_fig, cq_caption = cq_result
        st.plotly_chart(cq_fig, use_container_width=True, config={"displayModeBar": False}, key="forward_compression_fig")
        st.caption(cq_caption)
    else:
        st.info("STR compression data is not available yet.")

if section_visuals is not None:
    _top_insight = section_visuals.top_forward_insight(get_connection())
else:
    _top_insight = None
if _top_insight:
    st.markdown(
        f"""
        <div class="pulse-summary-card">
          <div class="pulse-summary-label">What's Ahead: {html.escape(_top_insight['category'].replace('_', ' ').title())}</div>
          <div class="pulse-summary-body">{html.escape(_top_insight['body'] or _top_insight['headline'])}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("Source: today's generated insight, drawn from live STR and Datafy data.")

st.divider()

st.divider()

# ---------------------------------------------------------------------------
# Intelligence Brief — an AI assistant answering questions against this
# week's live STR, CoStar, and Datafy data, plus a free-form follow-up
# question box. Distinct from the "Summarize" button above: Summarize reads
# only the current PDF's cover and executive summary; this reads the full
# monthly STR/CoStar history, Datafy visitor economy summary, and recent
# forward-looking insights, so it can answer trend and comparison questions
# the PDF snapshot alone cannot.
# ---------------------------------------------------------------------------

st.markdown('<div id="pulse-brain" class="pulse-anchor"></div>', unsafe_allow_html=True)
brain_months = window_months

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

st.markdown('<div id="pulse-fullreport" class="pulse-anchor"></div>', unsafe_allow_html=True)

section_pages = _split_pdf_pages(pdf_bytes)
available_sections = [s for s in SECTIONS if s["page"] < len(section_pages)]

# Rendered once (cached on pdf_bytes) and reused both for the section-card
# thumbnails below and for the flipbook further down the page -- so every
# section card shows an actual preview of that page's charts, not just text.
with st.spinner("Rendering page previews..."):
    _section_preview_images = _render_page_images(pdf_bytes)

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
                # A live figure read from analytics.sqlite beats a rasterized
                # picture of the PDF page: it carries the current number and
                # it can be hovered. The thumbnail stays as the fallback for
                # sections with no chart of their own (Executive Summary,
                # Notes) and for any section whose source table is empty.
                visual = (
                    section_visuals.build_section_visual(section["title"], get_connection())
                    if section_visuals is not None else None
                )
                if visual is not None:
                    sec_fig, sec_caption = visual
                    st.plotly_chart(
                        sec_fig, use_container_width=True,
                        config={"displayModeBar": False},
                        key=f"section_visual_{i}",
                    )
                    st.caption(sec_caption)
                elif section["page"] < len(_section_preview_images):
                    st.image(_section_preview_images[section["page"]], use_container_width=True)
                    st.caption(section["desc"])
                else:
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

st.markdown("#### Flip Through the Full Report")
st.caption(
    "Browse the report page by page, just like flipping through Issuu. Use the arrows, the "
    "thumbnail strip, or the left and right arrow keys. Choose which sections to include below; "
    "the flipbook and the export button both follow your choice, and the full report is included "
    "by default."
)

_all_section_titles = [s["title"] for s in available_sections]
selected_section_titles = st.multiselect(
    "Sections to include",
    options=_all_section_titles,
    default=_all_section_titles,
    key="flipbook_section_picker",
    label_visibility="collapsed",
)
if not selected_section_titles:
    st.caption("No sections selected, showing the full report instead.")
    selected_section_titles = _all_section_titles

# The cover page (index 0) is not one of the pop-out SECTIONS entries but is
# always kept, so a filtered flipbook or export still opens on a real cover
# rather than jumping straight to Executive Summary.
_selected_pages = sorted(
    {0} | {sec["page"] for sec in available_sections if sec["title"] in selected_section_titles}
)
_is_full_selection = len(selected_section_titles) == len(_all_section_titles)

page_images = [
    _section_preview_images[i] for i in _selected_pages if i < len(_section_preview_images)
]
if not _is_full_selection:
    st.caption(f"Showing {len(page_images)} of {len(_section_preview_images)} pages.")
components.html(_flipbook_html(page_images), height=800, scrolling=False)

if section_visuals is not None and not _is_full_selection and page_images:
    try:
        subset_pdf_bytes = section_visuals.build_subset_pdf(pdf_bytes, _selected_pages)
    except Exception:
        subset_pdf_bytes = None
    if subset_pdf_bytes:
        st.download_button(
            "⬇ Download Selected Sections as PDF",
            data=subset_pdf_bytes,
            file_name=f"Visit Dana Point PULSE - Selected Sections - {datetime.now().strftime('%Y-%m-%d')}.pdf",
            mime="application/pdf",
            key="download_selected_sections_pdf",
        )

with st.expander("View the full report as one continuous scroll instead"):
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

st.markdown('<div id="pulse-notes" class="pulse-anchor"></div>', unsafe_allow_html=True)
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

    # -------------------------------------------------------------------
    # Admin: on-demand STR sync. Dispatches the existing str_weekly_sync.yml
    # GitHub Actions workflow (Dropbox → loaders → commit → push) instead of
    # running the pipeline inside this container: the live Railway container
    # has no git push credentials and its own local sqlite would just get
    # overwritten on the next redeploy, so triggering the CI job is the
    # actual persistent path. Takes 1-2 minutes; Railway auto-redeploys once
    # the workflow pushes to main.
    #
    # STR only, on purpose (2026-08-19): this is a stopgap for as long as
    # Heather is on the Dropbox folder — once she moves to SharePoint this
    # needs a different fetch mechanism (see full_sync.yml's OPEN ITEM note).
    # CoStar and Datafy are staying manual (local folder, John will specify
    # the path later), so no button for those yet.
    # -------------------------------------------------------------------
    st.markdown("---")
    st.markdown("#### Admin: STR Data Sync")
    st.caption(
        "Triggers the STR Dropbox sync + reload workflow on GitHub Actions "
        "(str_weekly_sync.yml). Takes 1-2 minutes to run — the dashboard "
        "shows fresh numbers once it finishes and Railway redeploys. "
        "CoStar and Datafy stay manual for now (local folder upload), no "
        "button here yet."
    )
    _github_token = os.environ.get("GITHUB_TOKEN", "")
    _github_repo = os.environ.get("GITHUB_REPO", "gloconllc/VDP_Dashboard")
    if not _github_token:
        st.warning(
            "GITHUB_TOKEN is not configured on this deployment. Set a "
            "fine-grained GitHub PAT (actions:write scope on "
            f"{_github_repo}) as the GITHUB_TOKEN env var on Railway to "
            "enable this button."
        )
    else:
        st.caption(f"Will dispatch str_weekly_sync.yml on {_github_repo}@main.")
        if st.button("Sync STR from Dropbox Now"):
            with st.spinner("Triggering STR sync workflow…"):
                try:
                    import requests
                    _resp = requests.post(
                        f"https://api.github.com/repos/{_github_repo}/actions/workflows/str_weekly_sync.yml/dispatches",
                        headers={
                            "Authorization": f"Bearer {_github_token}",
                            "Accept": "application/vnd.github+json",
                        },
                        json={"ref": "main"},
                        timeout=15,
                    )
                    if _resp.status_code == 204:
                        st.success(
                            "STR sync triggered. It runs on GitHub Actions "
                            "and takes 1-2 minutes; the live dashboard will "
                            "update automatically once it finishes and "
                            "Railway redeploys."
                        )
                    else:
                        st.error(f"GitHub API returned {_resp.status_code}: {_resp.text[:300]}")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Trigger failed: {exc}")
