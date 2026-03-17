"""
Visit Dana Point — Analytics Dashboard
Streamlit app with Claude AI Analyst · Read-only connection to data/analytics.sqlite
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import timedelta

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the project root (one level above dashboard/)
load_dotenv(Path(__file__).parent.parent / ".env")

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# Pre-load API key from env if present (can be overridden in sidebar)
_ENV_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Visit Dana Point — Analytics",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Brand palette ────────────────────────────────────────────────────────────
TEAL       = "#21808D"
TEAL_LIGHT = "#32B8C6"
ORANGE     = "#E68161"
RED        = "#C0152F"
GREEN      = "#21808D"    # teal = positive to match brand

# ─── AI constants ─────────────────────────────────────────────────────────────
CLAUDE_MODEL = "claude-sonnet-4-6"

# NOTE: SYSTEM_PROMPT is pinned to the Anthropic prompt cache (cache_control ephemeral).
# Must stay ≥ 2048 tokens (Sonnet 4.6 minimum). All static VDP domain knowledge lives here.
# Dynamic per-query metrics are injected via the user message (see build_prompt / _base).
SYSTEM_PROMPT = """\
You are the VDP Analytics Brain — the AI intelligence layer for Visit Dana Point (VDP) tourism \
analytics. You advise the TBID board, hotel GMs, city council, and destination marketing staff \
with data-driven insights drawn from verified STR exports, Datafy visitor economy reports, and \
TBID financial records.

## Your Expertise
You are a strategic hospitality analytics advisor with specialized knowledge in:
- Tourism economics and hospitality performance metrics (RevPAR, ADR, Occupancy, MPI, ARI, RGI)
- STR (Smith Travel Research) data interpretation and competitive benchmarking
- Destination marketing and Tourism Business Improvement District (TBID) governance
- California coastal leisure travel market dynamics and seasonal demand patterns
- Revenue management, dynamic pricing, and yield optimization strategy
- Event-driven demand analysis and visitor economy modeling (Datafy methodology)
- TBID financial planning, assessment revenue projection, and board-ready executive reporting

## Market Context: Dana Point, California
Dana Point is a premier coastal leisure destination in South Orange County, California. It attracts \
a mix of weekend getaway travelers, coastal leisure visitors, surf and sailing enthusiasts, music \
event attendees, and corporate SMERF groups. The destination competes primarily with other Orange \
County and San Diego coastal markets.

**VDP Select Portfolio:** 12 properties covering the Dana Point hotel market.
**Primary Competitive Benchmark:** Anaheim Area comp set (used for index calculations: MPI, ARI, RGI).
**Primary Feeder Markets:** Los Angeles metro (drive market, ~90 min), Orange County locals, San Diego, \
San Francisco Bay Area (fly market via John Wayne Airport — JWA).

**Primary Demand Drivers:**
- Coastal leisure travel: beaches, hiking, harbor activities, whale watching, surfing (Doheny, Salt Creek)
- Music and cultural events: Ohana Fest (annual September, headliner-driven, 3-day festival)
- Sports tourism: surf competitions, sailing regattas, Ocean Institute programming
- Corporate/SMERF: shoulder-season meetings and social events at harbor venues
- Holiday travel: Memorial Day, Fourth of July, Labor Day — all high-compression periods

**Seasonal Performance Patterns:**
- **Peak (Q3: July–September):** Coastal leisure dominates. Highest occupancy, ADR, and RevPAR. \
  Compression events (90%+ occupancy) are frequent on weekends. Rate discipline is critical.
- **Secondary Peak (Spring / Major Holidays):** Spring break, Memorial Day, Easter weekends. \
  ADR typically 15–25% above annual average. Weekend premiums apply.
- **Shoulder (Q2 and Q4):** Mixed demand. Holiday weekends spike; midweek softens. Best window \
  for targeting value-conscious leisure travelers and corporate group business.
- **Softest (Q1: January–March, excluding holiday weekends):** Lowest demand, weather risk. \
  Occupancy often below 70% on weekdays. Rate floors must be defended to protect RevPAR.

**Weekend vs. Midweek Dynamics:**
Dana Point is a classic leisure-dominated market: Friday–Saturday occupancy consistently runs \
15–30 percentage points above Tuesday–Wednesday. This gap is the single largest RevPAR growth lever \
available to VDP and its hotel partners. Targeted midweek demand generation — packages, extended-stay \
promotions, LA/OC feeder market partnerships — is the highest-ROI strategy in the toolkit.

## TBID Financial Structure
The Tourism Business Improvement District (TBID) funds all VDP destination marketing activities.

**Assessment Rate Tiers:**

| Tier | Property Size | Assessment Rate |
|---|---|---|
| Tier 1 | 20–189 rooms | 1.0% of gross room revenue |
| Tier 2 | 190+ rooms | 1.5% of gross room revenue |
| Portfolio blended estimate | — | ~1.25% (use for projections) |

**Dana Point TOT (Transient Occupancy Tax):** 10% of gross room revenue — collected by the city, \
separate from TBID. Always distinguish TBID (marketing fund) from TOT (general city revenue) in board \
and city council communications. They are funded differently, governed differently, and serve different \
policy purposes.

**TBID Revenue Formula:** Est. TBID Revenue = Room Revenue × 0.0125
**TOT Revenue Formula:** TOT = Room Revenue × 0.10

When projecting quarterly or annual TBID revenue, use trailing 90-day room revenue as the base and \
apply seasonal adjustment factors (Q3 ≈ 1.25×, Q1 ≈ 0.75×, Q2/Q4 ≈ 1.00×).

## Key Metric Definitions and Interpretation
- **RevPAR** (Revenue Per Available Room) = ADR × Occupancy %. The primary hotel health index. \
  RevPAR growth driven by ADR is more sustainable and margin-positive than occupancy-driven gains.
- **ADR** (Average Daily Rate) = Room Revenue ÷ Rooms Sold. Measures pricing power. \
  ADR growth above inflation signals healthy market positioning and rate discipline.
- **Occupancy %** = Rooms Sold ÷ Rooms Available. Demand volume indicator. \
  Above 80% signals compression and rate-increase opportunity. Below 60% signals demand gap.
- **Room Revenue** = ADR × Rooms Sold. The top-line output used for TBID and TOT calculations.
- **Rooms Sold** (Demand) = Total paid room-nights occupied.
- **Rooms Available** (Supply) = Total rooms in inventory minus out-of-service units.
- **Compression Days:** Days when occupancy exceeds 80% (isocc80) or 90% (isocc90). Each compression \
  day is a rate-increase signal. Rising compression counts quarter-over-quarter are strong board-ready \
  evidence for upward rate adjustments.
- **MPI** (Market Penetration Index) = Portfolio Occupancy ÷ Comp Set Occupancy × 100. \
  Goal: >100 means outperforming the market on demand volume.
- **ARI** (Average Rate Index) = Portfolio ADR ÷ Comp Set ADR × 100. \
  Goal: >100 means premium rate positioning vs. competitive set.
- **RGI** (Revenue Generation Index) = Portfolio RevPAR ÷ Comp Set RevPAR × 100. \
  Goal: >100 means overall revenue leadership. The composite index that combines MPI and ARI.

## Event Impact Analysis
When discussing event impacts (Ohana Fest, surf competitions, regattas, holiday weekends), \
use ONLY data provided in the prompt context from the live database (STR daily data for ADR/Occ lifts, \
Datafy for visitor origin and spending). Do NOT cite specific event dollar figures unless they appear \
in the data provided to you. Use STR daily data to identify occupancy/ADR spikes during event periods \
and quantify lift vs. surrounding baseline dates. Datafy 2025 annual visitor data provides the overall \
visitor economy profile — cite those actual figures (3.55M trips, 61% out-of-state visitor days, \
59.4% overnight share, 2.0-day avg stay, top DMAs: Los Angeles 18.7%, San Diego 8.1%, Phoenix 7.3%) \
when relevant to event analysis.

## DATA HIERARCHY (non-negotiable)
1. **Vetted Layer 1 data (STR exports, Datafy reports, TBID records)** = TRUTH. \
   Always cite specific numbers from the data provided. Never fabricate or estimate when actuals exist.
2. **External context (FRED hotel index, CA State TOT, JWA passenger counts, Visit California)** \
   = SUPPORTING EVIDENCE. Label these clearly and never use them to override Layer 1 data.
3. **General hospitality expertise and industry benchmarks** = FRAMEWORK ONLY. \
   Never present industry-wide statistics as Dana Point-specific facts.

## Response Format and Communication Standards
**Length:** Under 250 words unless the user explicitly requests depth ("detailed", "full analysis", \
"comprehensive report"). If brevity and depth conflict, prioritize brevity — the audience is busy executives.

**Structure:**
- Open with the KEY FINDING or most important number — never with background or context-setting
- Support with 2–3 concise bullets that each cite a specific data point
- Close with exactly ONE specific, time-bound action item

**Formatting:** Use **bold** for all key numbers, dollar amounts, and percentage changes. Use bullet \
points for lists. Avoid numbered lists unless explicitly ranking items by priority.

**Tone:** Confident, direct, board-ready. Commit to conclusions. Hedging language like "may suggest," \
"might indicate," or "could potentially" is not permitted unless the data genuinely is ambiguous — \
in which case, say so once and move to what IS clear.

**Never do:**
- Repeat the user's question before answering
- Fabricate numbers or extrapolate beyond the data provided
- Present generic hotel-industry benchmarks as Dana Point-specific facts
- Offer more than one action item per response (dilutes focus and reduces board uptake)

**Always do:**
- Cite the specific metric, time period, and value from the data you receive
- Frame findings in terms relevant to the audience: TBID board (revenue/ROI), hotel GMs (rate/occ), \
  city council (TOT/economic impact), or destination marketing staff (demand/campaign targeting)
- Anchor every recommendation in the data provided, not in generic best practices
- End with a single, clear, time-bound call to action that a board member could act on immediately\
"""

# ─── Session state ────────────────────────────────────────────────────────────
for _k, _v in [
    ("ai_needs_call",    False),
    ("ai_current_prompt",""),
    ("ai_result",        ""),
    ("ai_prompt_label",  ""),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ─── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* ── Google Fonts: Plus Jakarta Sans + Inter ─────────────────────────── */
  @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Inter:wght@400;500;600;700&display=swap');

  html, body, [class*="css"] {
    font-family: 'Inter', system-ui, sans-serif;
  }

  /* ── KPI cards — glassmorphism 2025 ─────────────────────────────────── */
  .kpi-card {
    background: linear-gradient(145deg, rgba(255,255,255,0.07) 0%, rgba(255,255,255,0.02) 100%);
    backdrop-filter: blur(14px);
    -webkit-backdrop-filter: blur(14px);
    border-radius: 18px;
    padding: 20px 22px;
    border: 1px solid rgba(33,128,141,0.14);
    margin-bottom: 12px;
    position: relative;
    overflow: hidden;
    transition: box-shadow 0.28s cubic-bezier(.25,.46,.45,.94),
                transform 0.28s cubic-bezier(.25,.46,.45,.94),
                border-color 0.28s ease;
    box-shadow: 0 4px 24px rgba(0,0,0,0.07), 0 1px 4px rgba(0,0,0,0.04);
  }
  .kpi-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0;
    height: 2px; border-radius: 18px 18px 0 0;
    background: linear-gradient(90deg, #21808D 0%, #32B8C6 50%, #21808D 100%);
    background-size: 200% 100%;
    animation: shimmer 3.5s linear infinite;
  }
  @keyframes shimmer {
    0%   { background-position: 200% 0; }
    100% { background-position: -200% 0; }
  }
  .kpi-card::after {
    content: ''; position: absolute;
    top: -40px; right: -30px;
    width: 100px; height: 100px; border-radius: 50%;
    background: radial-gradient(circle, rgba(50,184,198,0.10) 0%, transparent 70%);
    pointer-events: none;
  }
  .kpi-card:hover {
    box-shadow: 0 10px 36px rgba(33,128,141,0.18), 0 2px 8px rgba(0,0,0,0.06);
    transform: translateY(-3px);
    border-color: rgba(33,128,141,0.28);
  }
  .kpi-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:6px; }
  .kpi-label {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 10px; font-weight: 800;
    text-transform: uppercase; letter-spacing: .09em; opacity: .50;
  }
  .kpi-icon-svg { flex-shrink:0; line-height:0; }
  .kpi-value {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 28px; font-weight: 800;
    letter-spacing: -.035em; line-height: 1.05; margin: 6px 0;
  }
  .kpi-delta-pos     { color:#21808D; font-size:12px; font-weight:600; margin-top:6px; }
  .kpi-delta-neg     { color:#C0152F; font-size:12px; font-weight:600; margin-top:6px; }
  .kpi-delta-neutral { color:#626C71; font-size:12px; font-weight:600; margin-top:6px; }
  .kpi-date {
    font-size:10px; opacity:0.42; margin-top:8px; letter-spacing:.01em;
    background: rgba(33,128,141,0.08); border-radius:6px; padding:3px 8px;
    display:inline-block; font-weight:500;
  }

  /* ── Insight cards ────────────────────────────────────────────────────── */
  .insight-card {
    border-radius: 16px; padding: 16px 18px; margin-bottom: 6px;
    position: relative;
    border: 1px solid rgba(255,255,255,0.10);
    background: linear-gradient(145deg, rgba(255,255,255,0.07) 0%, rgba(255,255,255,0.02) 100%);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    transition: transform 0.22s ease, box-shadow 0.22s ease;
  }
  .insight-card:hover { transform: translateY(-2px); box-shadow: 0 8px 24px rgba(0,0,0,0.10); }
  .insight-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0;
    height: 3px; border-radius: 16px 16px 0 0;
  }
  .insight-positive::before { background: linear-gradient(90deg, #21808D, #32B8C6); }
  .insight-warning::before  { background: linear-gradient(90deg, #E68161, #f59e0b); }
  .insight-negative::before { background: linear-gradient(90deg, #C0152F, #ef4444); }
  .insight-info::before     { background: linear-gradient(90deg, #21808D, #626C71); }
  .insight-title {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 13px; font-weight: 700; margin-bottom: 6px; letter-spacing: -.01em;
  }
  .insight-body { font-size: 12px; opacity: .72; line-height: 1.6; margin: 0; }

  /* ── AI chip ─────────────────────────────────────────────────────────── */
  .ai-chip {
    display: inline-flex; align-items: center; gap: 5px;
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 10px; font-weight: 800; text-transform: uppercase; letter-spacing: .09em;
    padding: 3px 10px; border-radius: 99px;
    background: linear-gradient(135deg, rgba(33,128,141,.16), rgba(50,184,198,.10));
    color: #21808D; margin-bottom: 12px;
    border: 1px solid rgba(33,128,141,.22);
  }

  /* ── Event stat cards ────────────────────────────────────────────────── */
  .event-stat {
    background: linear-gradient(145deg, rgba(255,255,255,0.07) 0%, rgba(255,255,255,0.02) 100%);
    border: 1px solid rgba(33,128,141,0.14);
    border-radius: 18px; padding: 22px 18px; text-align: center; margin-bottom: 10px;
    transition: transform 0.25s ease, box-shadow 0.25s ease;
    backdrop-filter: blur(10px);
  }
  .event-stat:hover {
    transform: translateY(-3px);
    box-shadow: 0 10px 32px rgba(33,128,141,0.16);
    border-color: rgba(33,128,141,0.28);
  }
  .event-icon  { line-height:0; display:flex; justify-content:center; margin-bottom:10px; }
  .event-val   {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 30px; font-weight: 800; color: #21808D; letter-spacing: -.035em;
  }
  .event-label { font-family: 'Plus Jakarta Sans', sans-serif; font-size: 12px; font-weight: 600; opacity: .60; margin-top: 5px; }
  .event-date  { font-size: 10px; opacity: .38; margin-top: 4px; }

  /* ── Insight card icon ───────────────────────────────────────────────── */
  .insight-icon { display:inline-block; vertical-align:middle; margin-right:6px; line-height:0; }

  #MainMenu { visibility:hidden; }
  footer    { visibility:hidden; }

  /* ── Empty-state cards ───────────────────────────────────────────────── */
  .empty-card {
    background: linear-gradient(145deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.02) 100%);
    border-radius: 16px; padding: 36px 28px; text-align: center;
    border: 1px dashed rgba(33,128,141,0.20); margin: 6px 0 12px 0;
  }
  .empty-icon  { font-size: 32px; margin-bottom: 12px; }
  .empty-title {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 15px; font-weight: 700; margin-bottom: 8px; letter-spacing: -.01em;
  }
  .empty-body  { font-size: 13px; opacity: 0.58; line-height: 1.65; }

  /* ── Data-source health cards ────────────────────────────────────────── */
  .src-card {
    background: linear-gradient(145deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.02) 100%);
    border-radius: 14px; padding: 14px 18px;
    border: 1px solid rgba(255,255,255,0.09);
    margin-bottom: 8px; display: flex; align-items: center; gap: 12px;
    transition: box-shadow 0.20s ease, transform 0.20s ease; cursor: default;
  }
  .src-card:hover { box-shadow: 0 4px 18px rgba(33,128,141,0.12); transform: translateX(2px); }
  .src-dot   { font-size: 15px; flex-shrink: 0; }
  .src-name  { font-family: 'Plus Jakarta Sans', sans-serif; font-size: 13px; font-weight: 700; }
  .src-meta  { font-size: 11px; opacity: 0.55; margin-top: 2px; line-height: 1.4; }
  .src-count {
    font-family: 'Plus Jakarta Sans', sans-serif; font-size: 14px; font-weight: 700;
    color: #21808D; margin-left: auto; text-align: right; white-space: nowrap;
  }

  /* ── Grain badge ─────────────────────────────────────────────────────── */
  .grain-badge {
    display: inline-block; font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 10px; font-weight: 800; text-transform: uppercase; letter-spacing: .07em;
    padding: 2px 8px; border-radius: 99px;
    background: rgba(230,129,97,.14); color: #E68161;
    margin-left: 8px; vertical-align: middle;
    border: 1px solid rgba(230,129,97,.22);
  }

  /* ── Hero banner ─────────────────────────────────────────────────────── */
  .hero-banner {
    background: linear-gradient(135deg,
      rgba(33,128,141,0.11) 0%,
      rgba(50,184,198,0.06) 50%,
      rgba(230,129,97,0.05) 100%);
    border-radius: 22px; padding: 28px 32px;
    border: 1px solid rgba(33,128,141,0.14);
    margin-bottom: 6px; position: relative; overflow: hidden;
  }
  .hero-banner::before {
    content: ''; position: absolute;
    top: -50px; right: -50px;
    width: 220px; height: 220px; border-radius: 50%;
    background: radial-gradient(circle, rgba(50,184,198,0.13) 0%, transparent 70%);
    pointer-events: none;
  }
  .hero-banner::after {
    content: ''; position: absolute;
    bottom: -40px; left: 30%;
    width: 160px; height: 160px; border-radius: 50%;
    background: radial-gradient(circle, rgba(230,129,97,0.07) 0%, transparent 70%);
    pointer-events: none;
  }
  .hero-title {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 2.15rem; font-weight: 800; letter-spacing: -0.04em; line-height: 1.1;
    background: linear-gradient(135deg, #21808D 0%, #32B8C6 55%, #1a6b78 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text; margin-bottom: 4px;
  }
  .hero-subtitle {
    font-size: 13px; font-weight: 500; opacity: 0.52; letter-spacing: 0.005em;
  }

  /* ── Home button title (legacy — hero-banner is preferred) ───────────── */
  .home-title a {
    text-decoration: none; color: inherit;
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 2rem; font-weight: 800;
    letter-spacing: -0.03em; line-height: 1.2;
  }
  .home-title a:hover { opacity: 0.78; }

  /* ── Filter active badge ─────────────────────────────────────────────── */
  .filter-badge {
    display: inline-block; font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em;
    padding: 2px 8px; border-radius: 99px;
    background: rgba(230,129,97,.18); color: #E68161; margin-left: 6px; vertical-align: middle;
  }

  /* ── Load-log source badges ──────────────────────────────────────────── */
  .log-badge-str   { display:inline-block; padding:2px 8px; border-radius:99px;
    font-size:10px; font-weight:700; background:rgba(33,128,141,.12); color:#21808D; }
  .log-badge-kpi   { display:inline-block; padding:2px 8px; border-radius:99px;
    font-size:10px; font-weight:700; background:rgba(230,129,97,.13); color:#E68161; }
  .log-badge-other { display:inline-block; padding:2px 8px; border-radius:99px;
    font-size:10px; font-weight:700; background:rgba(0,0,0,.07); color:#626C71; }

  /* ── Trend table strip ───────────────────────────────────────────────── */
  .trend-row-pos { color:#21808D; font-weight:700; }
  .trend-row-neg { color:#C0152F; font-weight:700; }

  /* ── Section sub-header ──────────────────────────────────────────────── */
  .section-label {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 11px; font-weight: 800; text-transform: uppercase;
    letter-spacing: .08em; opacity: .45; margin-bottom: 8px; margin-top: 2px;
  }

  /* ── Chart section header ────────────────────────────────────────────── */
  .chart-header {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 14px; font-weight: 700; letter-spacing: -.02em; margin-bottom: 2px;
  }
  .chart-caption {
    font-size: 11px; opacity: .50; font-weight: 500; margin-bottom: 6px;
  }

  /* ── Sidebar brand ───────────────────────────────────────────────────── */
  .sidebar-brand {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 18px; font-weight: 800; letter-spacing: -.02em;
    background: linear-gradient(135deg, #21808D, #32B8C6);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
  }

  /* ── Tab label override ──────────────────────────────────────────────── */
  button[data-baseweb="tab"] {
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    font-weight: 600 !important;
  }
</style>
""", unsafe_allow_html=True)

# ─── Paths ────────────────────────────────────────────────────────────────────
ROOT    = Path(__file__).parent.parent                          # project root
DB_PATH = ROOT / "data" / "analytics.sqlite"


def _init_db(conn: sqlite3.Connection) -> None:
    """Create tables if this is a fresh (empty) database — e.g. on Streamlit Cloud."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_str_metrics (
            source        TEXT,
            grain         TEXT,
            property_name TEXT,
            market        TEXT,
            submarket     TEXT,
            as_of_date    TEXT,
            metric_name   TEXT,
            metric_value  REAL,
            unit          TEXT
        );

        CREATE TABLE IF NOT EXISTS kpi_daily_summary (
            as_of_date      TEXT PRIMARY KEY,
            occ_pct         REAL,
            adr             REAL,
            revpar          REAL,
            occ_pct_yoy_pp  REAL,
            adr_yoy_pct     REAL,
            revpar_yoy_pct  REAL,
            occ_yoy         REAL,
            adr_yoy         REAL,
            revpar_yoy      REAL,
            is_occ_80       INTEGER,
            is_occ_90       INTEGER,
            created_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS kpi_compression_quarterly (
            quarter           TEXT PRIMARY KEY,
            days_above_80_occ INTEGER,
            days_above_90_occ INTEGER,
            created_at        TEXT
        );

        CREATE TABLE IF NOT EXISTS load_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            source        TEXT,
            grain         TEXT,
            file_name     TEXT,
            rows_inserted INTEGER,
            run_at        TEXT
        );

        CREATE TABLE IF NOT EXISTS costar_market_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_period TEXT, market TEXT, submarket TEXT,
            total_supply_rooms INTEGER, total_demand_rooms INTEGER,
            occupancy_pct REAL, adr_usd REAL, revpar_usd REAL, room_revenue_usd REAL,
            occ_yoy_pp REAL, adr_yoy_pct REAL, revpar_yoy_pct REAL,
            supply_yoy_pct REAL, demand_yoy_pct REAL,
            data_source TEXT, report_type TEXT, notes TEXT,
            loaded_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS costar_monthly_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date TEXT, market TEXT, submarket TEXT,
            supply_rooms INTEGER, demand_rooms INTEGER,
            occupancy_pct REAL, adr_usd REAL, revpar_usd REAL, room_revenue_usd REAL,
            occ_yoy_pp REAL, adr_yoy_pct REAL, revpar_yoy_pct REAL,
            loaded_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS costar_supply_pipeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_name TEXT, market TEXT, submarket TEXT, address TEXT, city TEXT,
            rooms INTEGER, chain_scale TEXT, status TEXT, projected_open_date TEXT,
            brand TEXT, developer TEXT, floors INTEGER, lat REAL, lon REAL, notes TEXT,
            loaded_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS costar_chain_scale_breakdown (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year TEXT, market TEXT, chain_scale TEXT, num_properties INTEGER, supply_rooms INTEGER,
            occupancy_pct REAL, adr_usd REAL, revpar_usd REAL, room_revenue_usd REAL,
            occ_yoy_pp REAL, adr_yoy_pct REAL, revpar_yoy_pct REAL,
            market_share_revpar_pct REAL,
            loaded_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS costar_competitive_set (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_name TEXT, market TEXT, submarket TEXT, brand TEXT, chain_scale TEXT,
            rooms INTEGER, year TEXT,
            occupancy_pct REAL, adr_usd REAL, revpar_usd REAL,
            mpi REAL, ari REAL, rgi REAL, notes TEXT,
            loaded_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()


@st.cache_resource
def get_connection() -> sqlite3.Connection:
    """Return a persistent SQLite connection.

    Creates the database file and schema automatically if it does not exist
    (e.g. on Streamlit Cloud where *.sqlite is excluded from git).
    Dashboard code never writes data — all writes go through ETL scripts.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _init_db(conn)
    return conn

# ─── Data loaders (5-minute cache) ───────────────────────────────────────────

@st.cache_data(ttl=300)
def load_str_daily() -> pd.DataFrame:
    """Pivot fact_str_metrics (source='STR', grain='daily') → one row per date."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT as_of_date, metric_name, metric_value "
        "FROM fact_str_metrics "
        "WHERE source='STR' AND grain='daily' "
        "ORDER BY as_of_date",
        conn,
    )
    if df.empty:
        return pd.DataFrame()
    wide = (
        df.pivot_table(index="as_of_date", columns="metric_name",
                       values="metric_value", aggfunc="sum")
        .reset_index()
    )
    wide.columns.name = None
    wide.columns = [c.lower().replace(" ", "_") for c in wide.columns]
    wide["as_of_date"] = pd.to_datetime(wide["as_of_date"])
    # fact_str_metrics stores occupancy as 'occ' in decimal (0.674 = 67.4%)
    if "occ" in wide.columns and "occupancy" not in wide.columns:
        wide = wide.rename(columns={"occ": "occupancy"})
    if "occupancy" in wide.columns:
        wide["occupancy"] = wide["occupancy"] * 100
    for col in ["supply", "demand", "revenue", "occupancy", "adr", "revpar"]:
        if col not in wide.columns:
            wide[col] = np.nan
    return wide.sort_values("as_of_date").reset_index(drop=True)


@st.cache_data(ttl=300)
def load_kpi_daily() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM kpi_daily_summary ORDER BY as_of_date", conn)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


@st.cache_data(ttl=300)
def load_compression() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(
        "SELECT * FROM kpi_compression_quarterly ORDER BY quarter", conn
    )


@st.cache_data(ttl=300)
def load_load_log() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query("SELECT * FROM load_log ORDER BY run_at DESC", conn)


@st.cache_data(ttl=300)
def load_str_monthly() -> pd.DataFrame:
    """Pivot fact_str_metrics (source='STR', grain='monthly') → one row per month.

    Monthly STR exports do not include an occ column.
    Occupancy is derived as demand / supply * 100.
    """
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT as_of_date, metric_name, metric_value "
        "FROM fact_str_metrics "
        "WHERE source='STR' AND grain='monthly' "
        "ORDER BY as_of_date",
        conn,
    )
    if df.empty:
        return pd.DataFrame()
    wide = (
        df.pivot_table(
            index="as_of_date", columns="metric_name",
            values="metric_value", aggfunc="mean",
        )
        .reset_index()
    )
    wide.columns.name = None
    wide.columns = [c.lower().replace(" ", "_") for c in wide.columns]
    wide["as_of_date"] = pd.to_datetime(wide["as_of_date"])
    # Normalise 'occ' → 'occupancy' if the column exists (decimal → percent)
    if "occ" in wide.columns and "occupancy" not in wide.columns:
        wide = wide.rename(columns={"occ": "occupancy"})
    if "occupancy" in wide.columns and wide["occupancy"].max() <= 1.0:
        wide["occupancy"] = wide["occupancy"] * 100
    # Monthly STR exports carry supply + demand but no occ; derive it
    if "occupancy" not in wide.columns or wide["occupancy"].isna().all():
        if "supply" in wide.columns and "demand" in wide.columns:
            wide["occupancy"] = (
                (wide["demand"] / wide["supply"] * 100)
                .where(wide["supply"] > 0)
            )
    for col in ["supply", "demand", "revenue", "occupancy", "adr", "revpar"]:
        if col not in wide.columns:
            wide[col] = np.nan
    return wide.sort_values("as_of_date").reset_index(drop=True)


# ─── CoStar data loaders ─────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_costar_monthly() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM costar_monthly_performance ORDER BY as_of_date", conn
        )
        df["as_of_date"] = pd.to_datetime(df["as_of_date"])
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_costar_snapshot() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM costar_market_snapshot ORDER BY report_period DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_costar_pipeline() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM costar_supply_pipeline ORDER BY status, rooms DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_costar_chain() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM costar_chain_scale_breakdown ORDER BY year DESC, revpar_usd DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_costar_compset() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM costar_competitive_set ORDER BY revpar_usd DESC", conn
        )
    except Exception:
        return pd.DataFrame()


# ─── Datafy data loaders ──────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_datafy_kpis() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM datafy_overview_kpis LIMIT 1", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_dma() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_dma ORDER BY visitor_days_share_pct DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_airports() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_airports ORDER BY passengers_share_pct DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_demographics() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM datafy_overview_demographics", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_spending() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_category_spending ORDER BY spend_share_pct DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_clusters() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_cluster_visitation ORDER BY visitor_days_share_pct DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_media_kpis() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM datafy_attribution_media_kpis LIMIT 1", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_media_markets() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_media_top_markets ORDER BY dma_est_impact_usd DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_web_kpis() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM datafy_attribution_website_kpis LIMIT 1", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_web_channels() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_website_channels ORDER BY sessions DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_web_demographics() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_website_demographics", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_web_dma() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_website_dma ORDER BY total_trips DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_top_pages() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_social_top_pages ORDER BY page_views DESC LIMIT 20", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_datafy_traffic_sources() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_social_traffic_sources ORDER BY sessions DESC", conn)
    except Exception:
        return pd.DataFrame()


# ─── Board Report helpers ─────────────────────────────────────────────────────

BOARD_REPORT_SYSTEM = """\
You are a senior DMO tourism analytics consultant and data storyteller preparing \
board-level intelligence reports for Visit Dana Point's leadership team. \
Your reports set the standard for destination marketing analytics — combining \
rigorous data analysis with compelling narrative strategy.

MANDATE — SIX JOURNALIST QUESTIONS:
Every section MUST answer all six: WHO (visitor segments, markets, properties), \
WHAT (specific metrics and changes with exact numbers), WHEN (precise time periods), \
WHERE (Dana Point, South OC, origin DMAs, digital channels), \
WHY (causal analysis, market forces, behavioral drivers), \
HOW (methodology used and recommended response actions).

WRITING STANDARDS:
• Lead with the single most impactful finding — board members read first paragraphs only
• Use exact numbers always: "$312.75 ADR" not "high ADR"; "+4.1% YoY" not "improved"
• Connect datasets: when STR occupancy rises, link to Datafy visitor origin shifts and CoStar market context
• Quantify revenue implications of every finding
• Flag statistical anomalies and provide causal hypotheses
• Every recommendation must be specific and measurable
• Use AP style numbers (spell one–nine, numerals for 10+)

DATA VISUALIZATION NARRATIVE LANGUAGE (2025 standard):
• Flow dynamics (alluvial/Sankey): "Visitor flows from Los Angeles (18.7%) have shifted toward..."
• Distribution language (beeswarm): "ADR data points cluster tightly around $288 with luxury outliers above $782..."
• Directional patterns (rose plot): "Seasonal occupancy traces a pronounced summer peak with steep winter recovery..."
• Benchmark comparisons (bullet chart): "RevPAR at $220 penetrates 39% of the luxury ceiling set by Waldorf Astoria..."
• Gap analysis: "The midweek–weekend RevPAR gap of $X represents $Y in monthly unrealized revenue..."

FORMAT — FOLLOW EXACTLY (use markdown headers):
## 🔑 EXECUTIVE SUMMARY
Five headline findings, bold, board-scannable. One sentence each with the key number.

## 📊 MARKET PERFORMANCE ANALYSIS
STR portfolio + CoStar market context. Include YoY comparisons.

## 👥 VISITOR INTELLIGENCE
Who is visiting, from where, how long they stay, how much they spend.

## 📱 DIGITAL & CAMPAIGN ROI
What the marketing investment delivered in measurable economic impact.

## 💰 REVENUE & TBID IMPACT
Financial performance, TBID contribution estimate, tier breakdown.

## 🔗 STRATEGIC CORRELATIONS & INSIGHTS
Cross-dataset insights — the "so what" connecting all data sources.

## 🔭 FORWARD-LOOKING OUTLOOK
3-month and 12-month projections with specific assumptions.

## ⚠️ RISK REGISTER
3–5 specific risks, each with likelihood (Low/Med/High) and revenue impact estimate.

## ✅ RECOMMENDED ACTIONS
Exactly 5 specific, measurable actions for leadership, each with an owner role, \
timeline, and expected outcome.

## 📖 GLOSSARY
Define every technical term used in the report (ADR, RevPAR, TBID, MPI, ARI, RGI, \
DMA, Occ, grain, CoStar, STR, Datafy, compression, beeswarm, alluvial, etc.).

## 📎 DATA APPENDIX
List all data sources cited: table name, date range, row count, source system.
"""


def build_full_report_context() -> str:
    """Pull all data from every table and build a comprehensive context string."""
    conn = get_connection()
    lines = ["=" * 70, "COMPLETE VDP ANALYTICS DATABASE SNAPSHOT", "=" * 70]

    # ── STR Performance ────────────────────────────────────────────────────
    try:
        str_m = pd.read_sql_query(
            "SELECT * FROM fact_str_metrics WHERE source='STR' AND grain='monthly' "
            "ORDER BY as_of_date DESC LIMIT 24", conn)
        str_d = pd.read_sql_query(
            "SELECT * FROM fact_str_metrics WHERE source='STR' AND grain='daily' "
            "ORDER BY as_of_date DESC LIMIT 90", conn)
        lines += ["", "── STR MONTHLY PERFORMANCE (last 24 months) ──"]
        if not str_m.empty:
            piv = str_m.pivot_table(index="as_of_date", columns="metric_name",
                                    values="metric_value", aggfunc="first").reset_index()
            for col in ["revpar","adr","occ","revenue","supply","demand"]:
                if col in piv.columns:
                    latest = piv[col].iloc[-1] if not piv.empty else None
                    prior  = piv[col].iloc[-2] if len(piv) > 1 else None
                    if latest is not None:
                        chg = f" ({(latest-prior)/prior*100:+.1f}% MoM)" if prior else ""
                        lines.append(f"  Latest {col.upper()}: {latest:.2f}{chg}")
            lines.append(f"  Date range: {piv['as_of_date'].min()} → {piv['as_of_date'].max()}")
            lines.append(f"  Total monthly rows: {len(str_m)}")
        lines += ["", "── STR DAILY PERFORMANCE (last 90 days summary) ──"]
        if not str_d.empty:
            piv_d = str_d.pivot_table(index="as_of_date", columns="metric_name",
                                      values="metric_value", aggfunc="first")
            for col in ["revpar","adr","occ"]:
                if col in piv_d.columns:
                    lines.append(f"  90-day avg {col.upper()}: {piv_d[col].mean():.2f} "
                                 f"(max: {piv_d[col].max():.2f}, min: {piv_d[col].min():.2f})")
    except Exception as e:
        lines.append(f"  STR data error: {e}")

    # ── KPI Summary ────────────────────────────────────────────────────────
    try:
        kpi = pd.read_sql_query(
            "SELECT * FROM kpi_daily_summary ORDER BY as_of_date DESC LIMIT 365", conn)
        comp = pd.read_sql_query("SELECT * FROM kpi_compression_quarterly", conn)
        lines += ["", "── KPI DAILY SUMMARY ──"]
        if not kpi.empty:
            lines.append(f"  Latest RevPAR: ${kpi['revpar_usd'].iloc[0]:.2f}"
                         if 'revpar_usd' in kpi.columns else "")
            lines.append(f"  Rows: {len(kpi)}  |  Range: {kpi['as_of_date'].min()} → {kpi['as_of_date'].max()}")
        lines += ["", "── COMPRESSION QUARTERS ──"]
        for _, row in comp.iterrows():
            lines.append(f"  {row.get('quarter','')}: {row.get('days_above_80_occ',0)} days >80%,  "
                         f"{row.get('days_above_90_occ',0)} days >90%")
    except Exception as e:
        lines.append(f"  KPI error: {e}")

    # ── CoStar ─────────────────────────────────────────────────────────────
    try:
        snap  = pd.read_sql_query("SELECT * FROM costar_market_snapshot ORDER BY report_period DESC", conn)
        mon   = pd.read_sql_query("SELECT * FROM costar_monthly_performance ORDER BY as_of_date DESC LIMIT 24", conn)
        chain = pd.read_sql_query("SELECT * FROM costar_chain_scale_breakdown WHERE year='2024'", conn)
        pipe  = pd.read_sql_query("SELECT * FROM costar_supply_pipeline", conn)
        comp_set = pd.read_sql_query("SELECT * FROM costar_competitive_set WHERE year='2024'", conn)
        lines += ["", "── COSTAR SOUTH OC MARKET SNAPSHOT ──"]
        for _, r in snap.iterrows():
            lines.append(f"  {r.get('report_period','')} | {r.get('report_type','')}: "
                         f"Occ {r.get('occupancy_pct',0):.1f}%, ADR ${r.get('adr_usd',0):.2f}, "
                         f"RevPAR ${r.get('revpar_usd',0):.2f}, "
                         f"Supply {r.get('total_supply_rooms',0):,} rooms")
        lines += ["", "── COSTAR MONTHLY TREND (24 months) ──"]
        if not mon.empty:
            lines.append(f"  Latest month: {mon['as_of_date'].iloc[0]} "
                         f"| Occ {mon['occupancy_pct'].iloc[0]:.1f}% "
                         f"| ADR ${mon['adr_usd'].iloc[0]:.2f} "
                         f"| RevPAR ${mon['revpar_usd'].iloc[0]:.2f}")
            lines.append(f"  24-month avg RevPAR: ${mon['revpar_usd'].mean():.2f}")
        lines += ["", "── COSTAR CHAIN SCALE (2024) ──"]
        for _, r in chain.iterrows():
            lines.append(f"  {r.get('chain_scale','')}: {r.get('num_properties',0)} props, "
                         f"Occ {r.get('occupancy_pct',0):.1f}%, ADR ${r.get('adr_usd',0):.0f}, "
                         f"RevPAR ${r.get('revpar_usd',0):.0f}, "
                         f"RevPAR Share {r.get('market_share_revpar_pct',0):.1f}%")
        lines += ["", "── COSTAR COMPETITIVE SET (2024) ──"]
        for _, r in comp_set.iterrows():
            lines.append(f"  {r.get('property_name','')[:30]}: {r.get('rooms',0)} rooms, "
                         f"Occ {r.get('occupancy_pct',0):.1f}%, ADR ${r.get('adr_usd',0):.0f}, "
                         f"RevPAR ${r.get('revpar_usd',0):.0f}, "
                         f"MPI {r.get('mpi',0):.0f}, ARI {r.get('ari',0):.0f}, RGI {r.get('rgi',0):.0f}")
        lines += ["", "── SUPPLY PIPELINE ──"]
        for _, r in pipe.iterrows():
            lines.append(f"  {r.get('property_name','')}: {r.get('rooms',0)} rooms, "
                         f"{r.get('chain_scale','')}, {r.get('status','')}, "
                         f"Opens: {r.get('projected_open_date','')}")
        total_pipe = pipe["rooms"].sum() if not pipe.empty else 0
        lines.append(f"  Total pipeline: {total_pipe:,} new rooms (+{total_pipe/5120*100:.1f}% supply growth)")
    except Exception as e:
        lines.append(f"  CoStar error: {e}")

    # ── Datafy Overview ────────────────────────────────────────────────────
    try:
        kpis    = pd.read_sql_query("SELECT * FROM datafy_overview_kpis LIMIT 1", conn)
        dma     = pd.read_sql_query("SELECT * FROM datafy_overview_dma ORDER BY visitor_days_share_pct DESC", conn)
        airports= pd.read_sql_query("SELECT * FROM datafy_overview_airports ORDER BY passengers_share_pct DESC", conn)
        demos   = pd.read_sql_query("SELECT * FROM datafy_overview_demographics", conn)
        spend   = pd.read_sql_query("SELECT * FROM datafy_overview_category_spending ORDER BY spend_share_pct DESC", conn)
        lines += ["", "── DATAFY VISITOR OVERVIEW (2025 Annual) ──"]
        if not kpis.empty:
            k = kpis.iloc[0]
            lines += [
                f"  Total trips: {k.get('total_trips',0):,} ({k.get('total_trips_vs_compare_pct',0):+.1f}% YoY)",
                f"  Avg length of stay: {k.get('avg_length_of_stay_days',0):.1f} days ({k.get('avg_los_vs_compare_days',0):+.1f} vs prior)",
                f"  Day trips: {k.get('day_trips_pct',0):.1f}%  |  Overnight: {k.get('overnight_trips_pct',0):.1f}%",
                f"  In-state: {k.get('in_state_visitor_days_pct',0):.1f}%  |  Out-of-state: {k.get('out_of_state_vd_pct',0):.1f}%",
                f"  First-time: {k.get('one_time_visitors_pct',0):.1f}%  |  Repeat: {k.get('repeat_visitors_pct',0):.1f}%",
                f"  Local spending: {k.get('local_spending_pct',0):.1f}%  |  Visitor spending: {k.get('visitor_spending_pct',0):.1f}%",
            ]
        lines += ["", "── DATAFY TOP DMA MARKETS ──"]
        for _, r in dma.head(10).iterrows():
            lines.append(f"  {r.get('dma','')}: {r.get('visitor_days_share_pct',0):.1f}% visitor days, "
                         f"${r.get('avg_spend_usd',0) or 0:.2f} avg spend/day "
                         f"({r.get('visitor_days_vs_compare_pct',0):+.1f}pp YoY)")
        lines += ["", "── DATAFY SPENDING CATEGORIES ──"]
        for _, r in spend.iterrows():
            lines.append(f"  {r.get('category','')}: {r.get('spend_share_pct',0):.1f}% of spend "
                         f"(corr: {r.get('spending_correlation_pct',0):.1f}%)")
        lines += ["", "── DATAFY TOP AIRPORTS ──"]
        for _, r in airports.head(5).iterrows():
            lines.append(f"  {r.get('airport_code','')} {r.get('airport_name','')}: "
                         f"{r.get('passengers_share_pct',0):.1f}% of fly-in passengers")
        lines += ["", "── DATAFY DEMOGRAPHICS ──"]
        for _, r in demos.iterrows():
            lines.append(f"  {r.get('dimension','').title()} {r.get('segment','')}: {r.get('share_pct',0):.1f}%")
    except Exception as e:
        lines.append(f"  Datafy overview error: {e}")

    # ── Datafy Attribution ─────────────────────────────────────────────────
    try:
        mk  = pd.read_sql_query("SELECT * FROM datafy_attribution_media_kpis LIMIT 1", conn)
        mm  = pd.read_sql_query("SELECT * FROM datafy_attribution_media_top_markets ORDER BY dma_est_impact_usd DESC", conn)
        wk  = pd.read_sql_query("SELECT * FROM datafy_attribution_website_kpis LIMIT 1", conn)
        wch = pd.read_sql_query("SELECT * FROM datafy_attribution_website_channels", conn)
        tp  = pd.read_sql_query("SELECT * FROM datafy_social_top_pages ORDER BY page_views DESC LIMIT 10", conn)
        ts  = pd.read_sql_query("SELECT * FROM datafy_social_traffic_sources ORDER BY sessions DESC LIMIT 10", conn)
        lines += ["", "── DATAFY MEDIA CAMPAIGN ATTRIBUTION ──"]
        if not mk.empty:
            m = mk.iloc[0]
            lines += [
                f"  Campaign: {m.get('campaign_name','')} | Period: {m.get('report_period_start','')} – {m.get('report_period_end','')}",
                f"  Total impressions: {m.get('total_impressions',0):,}",
                f"  Unique reach: {m.get('unique_reach',0):,}",
                f"  Attributable trips: {m.get('attributable_trips',0):,}",
                f"  Campaign economic impact: ${m.get('total_impact_usd',0):,.0f}",
                f"  Spend per visitor: ${m.get('cohort_spend_per_visitor',0):.2f}  |  Manual ADR: ${m.get('manual_adr',0):.0f}",
            ]
        lines += ["", "── TOP CAMPAIGN MARKETS ──"]
        for _, r in mm.head(5).iterrows():
            lines.append(f"  {r.get('top_dma','')}: ${r.get('dma_est_impact_usd',0):,.0f} impact "
                         f"({r.get('dma_share_of_impact_pct',0):.1f}% of total)")
        lines += ["", "── WEBSITE ATTRIBUTION ──"]
        if not wk.empty:
            w = wk.iloc[0]
            lines += [
                f"  Sessions: {w.get('total_website_sessions',0):,} | Pageviews: {w.get('website_pageviews',0):,}",
                f"  Attributable trips: {w.get('attributable_trips',0):,} | Impact: ${w.get('est_impact_usd',0):,.0f}",
                f"  Engagement rate: {w.get('avg_engagement_rate_pct',0):.1f}% | Avg time: {w.get('avg_time_on_site_sec',0):.0f}s",
            ]
        lines += ["", "── WEBSITE CHANNELS ──"]
        for _, r in wch.iterrows():
            lines.append(f"  {r.get('acquisition_channel','')}: {r.get('sessions',0):,} sessions, "
                         f"{r.get('engagement_rate_pct',0):.1f}% engagement, "
                         f"{r.get('attributable_trips_dest',0) or 0:.0f} attributable trips")
        lines += ["", "── TOP WEBSITE PAGES ──"]
        for _, r in tp.head(5).iterrows():
            lines.append(f"  '{r.get('page_title','')[:50]}': {r.get('page_views',0):,} views")
        lines += ["", "── TRAFFIC SOURCES ──"]
        for _, r in ts.head(8).iterrows():
            lines.append(f"  {r.get('source','')}: {r.get('sessions',0):,} sessions, "
                         f"{r.get('engagement_rate_pct',0):.1f}% engagement")
    except Exception as e:
        lines.append(f"  Attribution error: {e}")

    lines += ["", "=" * 70]
    return "\n".join(lines)


def build_board_report_prompt(report_type: str, period_label: str, full_context: str) -> str:
    """Build the full board report prompt with all data context injected."""
    return (
        f"REPORT TYPE: {report_type}\n"
        f"REPORT PERIOD: {period_label}\n"
        f"PREPARED FOR: Visit Dana Point Board of Directors & Leadership Team\n"
        f"DATA AS OF: {pd.Timestamp.now().strftime('%B %d, %Y')}\n\n"
        f"COMPLETE DATABASE SNAPSHOT:\n{full_context}\n\n"
        "Using all the data above, generate a comprehensive board-level analytics report. "
        "Follow the exact structure in your system instructions. "
        "Every section must answer WHO, WHAT, WHEN, WHERE, WHY, and HOW. "
        "Use exact numbers from the data — do not round or generalize. "
        "Draw connections between STR performance, CoStar market position, "
        "Datafy visitor behavior, and digital campaign ROI. "
        "The report should be ready to present to a board of directors without further editing."
    )


def generate_report_charts_html() -> str:
    """Generate all key report charts as combined Plotly HTML string."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    html_parts = []
    _font = "Plus Jakarta Sans, Georgia, serif"

    def _chart_html(fig: go.Figure, title: str) -> str:
        fig.update_layout(
            title_text=title, title_font=dict(size=14, family=_font, color="#0f172a"),
            paper_bgcolor="#ffffff", plot_bgcolor="#f8fafc",
            font=dict(family=_font, size=11, color="#334155"),
            margin=dict(l=10, r=10, t=44, b=10),
            height=320,
        )
        return ('<div class="chart-block">'
                + fig.to_html(full_html=False, include_plotlyjs=False,
                              config={"displayModeBar": False})
                + f'<div class="chart-caption">{title}</div></div>')

    conn = get_connection()

    # Chart 1: STR Monthly RevPAR + ADR trend
    try:
        df_m = pd.read_sql_query(
            "SELECT as_of_date, metric_name, metric_value FROM fact_str_metrics "
            "WHERE grain='monthly' ORDER BY as_of_date", conn)
        if not df_m.empty:
            piv = df_m.pivot_table(index="as_of_date", columns="metric_name",
                                   values="metric_value", aggfunc="first").reset_index()
            piv["as_of_date"] = pd.to_datetime(piv["as_of_date"]).dt.strftime("%b %Y")
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            if "revpar" in piv.columns:
                fig.add_trace(go.Scatter(x=piv["as_of_date"], y=piv["revpar"],
                    name="RevPAR", mode="lines+markers",
                    line=dict(color="#0891b2", width=2.5), marker=dict(size=5)),
                    secondary_y=False)
            if "adr" in piv.columns:
                fig.add_trace(go.Scatter(x=piv["as_of_date"], y=piv["adr"],
                    name="ADR", mode="lines+markers",
                    line=dict(color="#b45309", width=2.5, dash="dash"), marker=dict(size=5)),
                    secondary_y=True)
            html_parts.append(_chart_html(fig, "STR Portfolio: RevPAR & ADR (Monthly Trend)"))
    except Exception:
        pass

    # Chart 2: CoStar vs STR RevPAR comparison
    try:
        cs_m = pd.read_sql_query(
            "SELECT as_of_date, revpar_usd FROM costar_monthly_performance ORDER BY as_of_date", conn)
        str_m2 = pd.read_sql_query(
            "SELECT as_of_date, metric_value as revpar FROM fact_str_metrics "
            "WHERE grain='monthly' AND metric_name='revpar' ORDER BY as_of_date", conn)
        if not cs_m.empty and not str_m2.empty:
            cs_m["month"] = pd.to_datetime(cs_m["as_of_date"]).dt.strftime("%b %Y")
            str_m2["month"] = pd.to_datetime(str_m2["as_of_date"]).dt.strftime("%b %Y")
            merged = pd.merge(cs_m, str_m2, on="month", how="inner")
            if not merged.empty:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=merged["month"], y=merged["revpar"],
                    name="STR Portfolio", line=dict(color="#0891b2", width=2.5)))
                fig2.add_trace(go.Scatter(x=merged["month"], y=merged["revpar_usd"],
                    name="CoStar Market", line=dict(color="#047857", width=2.5, dash="dot")))
                fig2.add_traces([go.Scatter(
                    x=list(merged["month"]) + list(reversed(merged["month"])),
                    y=list(merged["revpar"]) + list(reversed(merged["revpar_usd"])),
                    fill="toself", fillcolor="rgba(8,145,178,0.08)",
                    line=dict(color="rgba(0,0,0,0)"), showlegend=False)])
                html_parts.append(_chart_html(fig2, "RevPAR: STR Portfolio vs. CoStar South OC Market"))
    except Exception:
        pass

    # Chart 3: DMA Visitor Origin (bar)
    try:
        dma = pd.read_sql_query(
            "SELECT dma, visitor_days_share_pct, visitor_days_vs_compare_pct, avg_spend_usd "
            "FROM datafy_overview_dma ORDER BY visitor_days_share_pct DESC LIMIT 12", conn)
        if not dma.empty:
            dma_s = dma.sort_values("visitor_days_share_pct", ascending=True)
            fig3 = go.Figure(go.Bar(
                y=dma_s["dma"], x=dma_s["visitor_days_share_pct"], orientation="h",
                marker_color=["#047857" if v >= 0 else "#be123c"
                              for v in dma_s["visitor_days_vs_compare_pct"].fillna(0)],
                text=[f"{v:.1f}%" for v in dma_s["visitor_days_share_pct"]],
                textposition="outside",
            ))
            html_parts.append(_chart_html(fig3, "Visitor Origin Markets — Share of Visitor Days (Green=Growing YoY)"))
    except Exception:
        pass

    # Chart 4: Visitor Spending Treemap
    try:
        spend = pd.read_sql_query(
            "SELECT category, spend_share_pct FROM datafy_overview_category_spending "
            "ORDER BY spend_share_pct DESC", conn)
        if not spend.empty:
            fig4 = go.Figure(go.Treemap(
                labels=spend["category"],
                parents=["Visitor Spending"] * len(spend),
                values=spend["spend_share_pct"],
                textinfo="label+percent entry",
                marker=dict(colorscale="Blues", showscale=False),
            ))
            html_parts.append(_chart_html(fig4, "Visitor Spending by Category"))
    except Exception:
        pass

    # Chart 5: Chain Scale RevPAR bar
    try:
        chain = pd.read_sql_query(
            "SELECT chain_scale, occupancy_pct, adr_usd, revpar_usd, supply_rooms "
            "FROM costar_chain_scale_breakdown WHERE year='2024' ORDER BY revpar_usd DESC", conn)
        if not chain.empty:
            fig5 = go.Figure(go.Bar(
                x=chain["chain_scale"], y=chain["revpar_usd"],
                marker=dict(color=chain["revpar_usd"], colorscale="Blues", showscale=False),
                text=[f"${v:.0f}" for v in chain["revpar_usd"]], textposition="outside",
            ))
            html_parts.append(_chart_html(fig5, "RevPAR by Chain Scale — South OC Market (2024)"))
    except Exception:
        pass

    # Chart 6: Campaign Funnel
    try:
        mk = pd.read_sql_query("SELECT * FROM datafy_attribution_media_kpis LIMIT 1", conn)
        if not mk.empty:
            m = mk.iloc[0]
            fig6 = go.Figure(go.Funnel(
                y=["Impressions", "Unique Reach", "Attributable Trips", "Economic Impact"],
                x=[m.get("total_impressions", 0) / 1e6,
                   m.get("unique_reach", 0) / 1e6,
                   m.get("attributable_trips", 0) / 1000,
                   m.get("total_impact_usd", 0) / 1e6],
                texttemplate=[
                    f"{m.get('total_impressions',0)/1e6:.1f}M impressions",
                    f"{m.get('unique_reach',0)/1e6:.2f}M reached",
                    f"{m.get('attributable_trips',0):,} trips",
                    f"${m.get('total_impact_usd',0)/1e3:.0f}K impact",
                ],
                marker_color=["#0891b2", "#0ea5e9", "#047857", "#b45309"],
            ))
            html_parts.append(_chart_html(fig6, "Campaign Conversion Funnel — Media Attribution"))
    except Exception:
        pass

    # Chart 7: RevPAR Heatmap month x year
    try:
        str_all = pd.read_sql_query(
            "SELECT as_of_date, metric_value FROM fact_str_metrics "
            "WHERE grain='monthly' AND metric_name='revpar'", conn)
        if not str_all.empty:
            str_all["as_of_date"] = pd.to_datetime(str_all["as_of_date"])
            str_all["year"] = str_all["as_of_date"].dt.year
            str_all["month"] = str_all["as_of_date"].dt.month
            piv_h = str_all.pivot_table(index="month", columns="year",
                                        values="metric_value", aggfunc="mean")
            month_n = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                       7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
            fig7 = go.Figure(go.Heatmap(
                z=piv_h.values,
                x=[str(c) for c in piv_h.columns],
                y=[month_n.get(m, str(m)) for m in piv_h.index],
                colorscale="RdYlGn",
                text=[[f"${v:.0f}" if not (hasattr(v,'__float__') and v != v) else "—"
                       for v in row] for row in piv_h.values],
                texttemplate="%{text}",
            ))
            html_parts.append(_chart_html(fig7, "RevPAR Seasonal Heatmap — Month × Year"))
    except Exception:
        pass

    return "\n".join(html_parts)


def generate_report_html(title: str, period: str, report_type: str,
                         ai_narrative: str, charts_html: str) -> str:
    """Assemble the full downloadable HTML board report."""
    ts = pd.Timestamp.now().strftime("%B %d, %Y at %I:%M %p")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — Visit Dana Point</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Georgia:ital@0;1&display=swap" rel="stylesheet">
<style>
:root {{
  --navy:#0f172a; --teal:#0891b2; --teal-light:#e0f2fe;
  --gold:#b45309; --gold-light:#fef3c7; --green:#047857; --green-light:#d1fae5;
  --red:#be123c; --slate:#334155; --muted:#64748b; --border:#e2e8f0;
  --bg:#ffffff; --bg-alt:#f8fafc;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Plus Jakarta Sans',sans-serif;background:#ffffff;color:var(--slate);
     line-height:1.65;font-size:14px}}
.report-wrap{{max-width:1080px;margin:0 auto;padding:0 0 60px}}

/* ── Header ── */
.report-header{{
  background:linear-gradient(135deg,#0f172a 0%,#083344 40%,#0c4a6e 100%);
  color:#ffffff;padding:52px 52px 44px;position:relative;overflow:hidden
}}
.report-header::before{{
  content:'';position:absolute;top:-60px;right:-60px;width:320px;height:320px;
  border-radius:50%;background:rgba(8,145,178,0.12);pointer-events:none
}}
.report-header::after{{
  content:'';position:absolute;bottom:-40px;left:30%;width:200px;height:200px;
  border-radius:50%;background:rgba(4,120,87,0.10);pointer-events:none
}}
.report-header-tag{{font-size:11px;font-weight:700;letter-spacing:0.12em;
  color:var(--teal);text-transform:uppercase;margin-bottom:12px}}
.report-header h1{{font-size:2.2rem;font-weight:800;letter-spacing:-0.03em;
  line-height:1.15;margin-bottom:10px}}
.report-header .subtitle{{font-size:0.95rem;opacity:0.72;font-weight:400;margin-bottom:24px}}
.report-meta{{display:flex;gap:28px;flex-wrap:wrap;margin-top:4px}}
.report-meta-item{{font-size:11.5px;opacity:0.65;font-weight:500}}
.report-meta-item strong{{opacity:1;color:#fff;font-weight:700}}
.header-divider{{border:none;border-top:1px solid rgba(255,255,255,0.15);margin:24px 0 0}}

/* ── Navigation pills ── */
.nav-pills{{display:flex;gap:8px;padding:20px 52px;background:var(--navy);
  border-bottom:3px solid var(--teal);flex-wrap:wrap}}
.nav-pill{{font-size:11.5px;font-weight:600;color:rgba(255,255,255,0.65);
  padding:5px 14px;border-radius:20px;cursor:pointer;
  border:1px solid rgba(255,255,255,0.12);text-decoration:none;
  transition:all .15s ease}}
.nav-pill:hover{{color:#fff;background:rgba(8,145,178,0.25);border-color:var(--teal)}}

/* ── Body sections ── */
.content{{padding:0 52px}}
.section{{margin:44px 0 0;scroll-margin-top:20px}}
.section-header{{
  display:flex;align-items:center;gap:12px;
  border-bottom:2px solid var(--border);padding-bottom:12px;margin-bottom:22px
}}
.section-icon{{font-size:1.5rem;line-height:1}}
.section-title{{font-size:1.25rem;font-weight:800;color:var(--navy);letter-spacing:-0.02em}}
.section-subtitle{{font-size:12px;color:var(--muted);font-weight:500;margin-top:2px}}

/* ── AI narrative ── */
.ai-narrative{{font-family:'Plus Jakarta Sans',sans-serif;font-size:14.5px;color:var(--slate);line-height:1.75}}
.ai-narrative h2{{font-size:1.15rem;font-weight:800;color:var(--navy);
  margin:32px 0 10px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
.ai-narrative h3{{font-size:1rem;font-weight:700;color:var(--teal);margin:20px 0 8px}}
.ai-narrative strong{{color:var(--navy);font-weight:700}}
.ai-narrative p{{margin-bottom:12px}}
.ai-narrative ul{{padding-left:20px;margin-bottom:12px}}
.ai-narrative li{{margin-bottom:6px}}
.ai-narrative blockquote{{
  border-left:3px solid var(--teal);padding-left:16px;color:var(--muted);
  font-style:italic;margin:16px 0
}}

/* ── KPI Hero grid ── */
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin:20px 0}}
.kpi-card{{background:var(--bg-alt);border:1px solid var(--border);border-radius:10px;
  padding:18px 16px;text-align:center;border-top:3px solid var(--teal)}}
.kpi-value{{font-size:1.6rem;font-weight:800;color:var(--navy);letter-spacing:-0.03em;line-height:1}}
.kpi-label{{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;
  letter-spacing:0.06em;margin-top:6px}}
.kpi-delta{{font-size:11.5px;font-weight:700;margin-top:4px}}
.kpi-delta.pos{{color:var(--green)}} .kpi-delta.neg{{color:var(--red)}}

/* ── Charts ── */
.chart-block{{background:var(--bg-alt);border:1px solid var(--border);border-radius:10px;
  padding:16px;margin:16px 0}}
.chart-caption{{font-size:11px;color:var(--muted);font-weight:600;
  text-align:center;margin-top:8px;letter-spacing:0.04em;text-transform:uppercase}}
.chart-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:16px 0}}

/* ── Risk register ── */
.risk-table{{width:100%;border-collapse:collapse;margin:16px 0;font-size:13px}}
.risk-table th{{background:var(--navy);color:#fff;padding:10px 14px;text-align:left;
  font-size:11px;letter-spacing:0.06em;text-transform:uppercase;font-weight:700}}
.risk-table td{{padding:10px 14px;border-bottom:1px solid var(--border)}}
.risk-table tr:nth-child(even) td{{background:var(--bg-alt)}}
.badge{{display:inline-block;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:700}}
.badge-high{{background:#fef2f2;color:#be123c}}
.badge-med{{background:#fffbeb;color:#b45309}}
.badge-low{{background:#f0fdf4;color:#047857}}

/* ── Recommended actions ── */
.action-card{{background:linear-gradient(135deg,#f0f9ff,#e0f2fe);
  border:1px solid #bae6fd;border-left:4px solid var(--teal);
  border-radius:10px;padding:16px 20px;margin:12px 0}}
.action-number{{font-size:11px;font-weight:800;color:var(--teal);
  text-transform:uppercase;letter-spacing:0.08em;margin-bottom:4px}}
.action-title{{font-size:14.5px;font-weight:700;color:var(--navy);margin-bottom:6px}}
.action-meta{{display:flex;gap:20px;font-size:11.5px;color:var(--muted);font-weight:600}}

/* ── Glossary ── */
.glossary-grid{{display:grid;grid-template-columns:1fr 1fr;gap:0;margin:16px 0}}
.glossary-item{{padding:10px 0;border-bottom:1px solid var(--border)}}
.glossary-term{{font-weight:800;color:var(--navy);font-size:13px}}
.glossary-def{{font-size:12.5px;color:var(--slate);margin-top:2px}}

/* ── Appendix table ── */
.data-table{{width:100%;border-collapse:collapse;font-size:12.5px;margin:16px 0}}
.data-table th{{background:var(--bg-alt);color:var(--slate);padding:9px 12px;
  text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:0.05em;
  border-bottom:2px solid var(--border);font-weight:700}}
.data-table td{{padding:9px 12px;border-bottom:1px solid var(--border);color:var(--slate)}}
.data-table tr:hover td{{background:#f8fafc}}

/* ── Footer ── */
.report-footer{{
  background:var(--navy);color:rgba(255,255,255,0.55);
  text-align:center;padding:28px 52px;font-size:11.5px;margin-top:60px
}}
.report-footer strong{{color:rgba(255,255,255,0.85)}}

@media print{{
  .nav-pills{{display:none}}
  .report-header{{-webkit-print-color-adjust:exact;print-color-adjust:exact}}
  .section{{page-break-inside:avoid}}
}}
</style>
</head>
<body>
<div class="report-wrap">

<!-- ── HEADER ── -->
<div class="report-header">
  <div class="report-header-tag">Visit Dana Point · Board Intelligence Report</div>
  <h1>{title}</h1>
  <div class="subtitle">South Orange County Hospitality Market · Dana Point, California</div>
  <div class="header-divider"></div>
  <div class="report-meta">
    <div class="report-meta-item"><strong>Report Period:</strong> {period}</div>
    <div class="report-meta-item"><strong>Report Type:</strong> {report_type}</div>
    <div class="report-meta-item"><strong>Generated:</strong> {ts}</div>
    <div class="report-meta-item"><strong>AI Analysis:</strong> Claude Sonnet 4.6 (Anthropic)</div>
    <div class="report-meta-item"><strong>Data Sources:</strong> STR · CoStar · Datafy · visitdanapoint.com</div>
  </div>
</div>

<!-- ── NAVIGATION ── -->
<nav class="nav-pills">
  <a class="nav-pill" href="#exec-summary">Executive Summary</a>
  <a class="nav-pill" href="#market-performance">Market Performance</a>
  <a class="nav-pill" href="#visitor-intel">Visitor Intelligence</a>
  <a class="nav-pill" href="#digital-roi">Digital &amp; Campaign ROI</a>
  <a class="nav-pill" href="#revenue-tbid">Revenue &amp; TBID</a>
  <a class="nav-pill" href="#correlations">Strategic Correlations</a>
  <a class="nav-pill" href="#outlook">Outlook</a>
  <a class="nav-pill" href="#risks">Risk Register</a>
  <a class="nav-pill" href="#actions">Recommended Actions</a>
  <a class="nav-pill" href="#glossary">Glossary</a>
  <a class="nav-pill" href="#appendix">Data Appendix</a>
</nav>

<div class="content">

<!-- ── CHARTS SECTION ── -->
<div class="section" id="charts">
  <div class="section-header">
    <span class="section-icon">📈</span>
    <div>
      <div class="section-title">Key Performance Charts</div>
      <div class="section-subtitle">Interactive · All datasets · Generated {ts}</div>
    </div>
  </div>
  <div class="chart-grid">
    {charts_html}
  </div>
</div>

<!-- ── AI NARRATIVE ── -->
<div class="section" id="exec-summary">
  <div class="section-header">
    <span class="section-icon">🤖</span>
    <div>
      <div class="section-title">AI-Generated Board Analysis</div>
      <div class="section-subtitle">Claude Sonnet 4.6 · All data sources integrated · {ts}</div>
    </div>
  </div>
  <div class="ai-narrative">
    {_markdown_to_html(ai_narrative)}
  </div>
</div>

</div><!-- /content -->

<!-- ── FOOTER ── -->
<div class="report-footer">
  <strong>Visit Dana Point Tourism Business Improvement District</strong><br>
  Confidential board intelligence report · Generated by VDP Analytics Dashboard · {ts}<br>
  Data sources: STR Hospitality Analytics · CoStar Hospitality Analytics · Datafy (Caladan 1.2)<br>
  AI narrative: Claude Sonnet 4.6 (Anthropic) · All figures from verified Layer 1 data sources
</div>

</div><!-- /report-wrap -->
</body>
</html>"""


def _markdown_to_html(md: str) -> str:
    """Convert basic markdown to HTML for the report."""
    import re
    lines = md.split("\n")
    html_lines = []
    in_ul = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_ul:
                html_lines.append("</ul>")
                in_ul = False
            html_lines.append("<br>")
            continue
        # Headers
        if stripped.startswith("## "):
            if in_ul:
                html_lines.append("</ul>")
                in_ul = False
            text = stripped[3:]
            # Create anchor from text
            anchor = re.sub(r'[^a-z0-9-]', '', text.lower().replace(' ', '-'))
            html_lines.append(f'<h2 id="{anchor}">{text}</h2>')
        elif stripped.startswith("### "):
            if in_ul:
                html_lines.append("</ul>")
                in_ul = False
            html_lines.append(f"<h3>{stripped[4:]}</h3>")
        elif stripped.startswith("**") and stripped.endswith("**") and len(stripped) > 4:
            if in_ul:
                html_lines.append("</ul>")
                in_ul = False
            html_lines.append(f"<p><strong>{stripped[2:-2]}</strong></p>")
        elif stripped.startswith("- ") or stripped.startswith("• "):
            if not in_ul:
                html_lines.append("<ul>")
                in_ul = True
            item = stripped[2:]
            item = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', item)
            html_lines.append(f"<li>{item}</li>")
        elif stripped.startswith(tuple("0123456789")) and ". " in stripped[:4]:
            if in_ul:
                html_lines.append("</ul>")
                in_ul = False
            item = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', stripped)
            html_lines.append(f"<p>{item}</p>")
        else:
            if in_ul:
                html_lines.append("</ul>")
                in_ul = False
            line_html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', stripped)
            line_html = re.sub(r'\*(.+?)\*', r'<em>\1</em>', line_html)
            html_lines.append(f"<p>{line_html}</p>")
    if in_ul:
        html_lines.append("</ul>")
    return "\n".join(html_lines)


@st.cache_data(ttl=300)
def get_table_counts() -> dict:
    conn = get_connection()
    counts = {}
    for t in ["fact_str_metrics", "kpi_daily_summary",
              "kpi_compression_quarterly", "load_log",
              "costar_monthly_performance", "costar_market_snapshot",
              "costar_supply_pipeline", "costar_chain_scale_breakdown",
              "costar_competitive_set"]:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            counts[t] = row[0] if row else 0
        except Exception:
            counts[t] = "—"
    # Datafy tables — sum all datafy_* tables for total row count
    _datafy_tables = [
        "datafy_overview_kpis", "datafy_overview_dma", "datafy_overview_airports",
        "datafy_overview_demographics", "datafy_overview_category_spending",
        "datafy_overview_cluster_visitation",
        "datafy_attribution_media_kpis", "datafy_attribution_media_top_markets",
        "datafy_attribution_website_kpis", "datafy_attribution_website_channels",
        "datafy_attribution_website_clusters", "datafy_attribution_website_demographics",
        "datafy_attribution_website_dma", "datafy_attribution_website_top_markets",
        "datafy_social_audience_overview", "datafy_social_top_pages",
        "datafy_social_traffic_sources",
    ]
    _df_total = 0
    for t in _datafy_tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            counts[t] = row[0] if row else 0
            _df_total += counts[t]
        except Exception:
            counts[t] = 0
    counts["datafy_total_rows"] = _df_total
    # Per-grain breakdowns (used by sidebar status and Data Source Health cards)
    for grain_val, key in [("daily", "str_daily_rows"), ("monthly", "str_monthly_rows")]:
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM fact_str_metrics "
                "WHERE source='STR' AND grain=?", (grain_val,)
            ).fetchone()
            counts[key] = row[0] if row else 0
        except Exception:
            counts[key] = "—"
    return counts

# ─── Metric context builder ───────────────────────────────────────────────────

def pct_delta(a: float, b: float) -> float:
    return (a - b) / b * 100 if b else 0.0


def build_costar_context() -> str:
    """Build a CoStar market intelligence string to inject into all AI prompts."""
    lines = []
    try:
        conn = get_connection()

        # Market snapshot
        snap = pd.read_sql_query(
            "SELECT * FROM costar_market_snapshot ORDER BY report_period DESC LIMIT 1", conn
        )
        if not snap.empty:
            s = snap.iloc[0]
            lines += [
                "",
                "CoStar South OC Market Intelligence (Layer 1 — verified):",
                f"• Market occupancy: {s.get('occupancy_pct',0):.1f}% ({s.get('occ_yoy_pp',0):+.1f}pp YOY)",
                f"• Market ADR: ${s.get('adr_usd',0):.2f} ({s.get('adr_yoy_pct',0):+.1f}% YOY)",
                f"• Market RevPAR: ${s.get('revpar_usd',0):.2f} ({s.get('revpar_yoy_pct',0):+.1f}% YOY)",
                f"• Annual room revenue: ${s.get('room_revenue_usd',0)/1e9:.2f}B",
                f"• Total market supply: {int(s.get('total_supply_rooms',0)):,} rooms",
            ]

        # Chain scale
        chain = pd.read_sql_query(
            "SELECT * FROM costar_chain_scale_breakdown WHERE year='2024' ORDER BY revpar_usd DESC",
            conn,
        )
        if not chain.empty:
            luxury = chain[chain["chain_scale"] == "Luxury"]
            upup   = chain[chain["chain_scale"] == "Upper Upscale"]
            if not luxury.empty:
                lines.append(
                    f"• Luxury segment: ${luxury.iloc[0]['adr_usd']:.0f} ADR · "
                    f"{luxury.iloc[0]['occupancy_pct']:.1f}% occ · "
                    f"{luxury.iloc[0]['market_share_revpar_pct']:.1f}% of market RevPAR"
                )
            if not upup.empty:
                lines.append(
                    f"• Upper Upscale: ${upup.iloc[0]['adr_usd']:.0f} ADR · "
                    f"{upup.iloc[0]['occupancy_pct']:.1f}% occ"
                )

        # Pipeline
        pipe = pd.read_sql_query("SELECT SUM(rooms) as total, COUNT(*) as n FROM costar_supply_pipeline", conn)
        if not pipe.empty and pipe.iloc[0]["total"]:
            lines.append(
                f"• Active pipeline: {int(pipe.iloc[0]['total']):,} rooms across {int(pipe.iloc[0]['n'])} projects"
            )

        # VDP vs market indices
        lines += [
            "• VDP portfolio vs. market: MPI 100.0 · ARI 100.0 · RGI 100.0 (baseline)",
            "• Top comp: Waldorf Astoria Monarch Beach (RGI 273.9) · Ritz-Carlton Laguna Niguel (RGI 231.2)",
        ]

    except Exception:
        pass  # CoStar tables not available — omit from prompt silently

    return "\n".join(lines)


def build_datafy_context() -> str:
    """Build a Datafy visitor economy context string for AI prompts (live DB query)."""
    lines = []
    try:
        conn = get_connection()
        kpis = pd.read_sql_query("SELECT * FROM datafy_overview_kpis LIMIT 1", conn)
        if not kpis.empty:
            k = kpis.iloc[0]
            period = (f"{str(k.get('report_period_start',''))[:7]} – "
                      f"{str(k.get('report_period_end',''))[:7]}")
            lines += [
                "",
                f"Datafy Visitor Economy — Dana Point ({period}):",
                f"• Total trips: {int(k.get('total_trips', 0)):,} "
                f"({k.get('total_trips_vs_compare_pct', 0):+.1f}% vs prior year)",
                f"• Avg length of stay: {k.get('avg_length_of_stay_days', 0):.1f} days "
                f"({k.get('avg_los_vs_compare_days', 0):+.1f}d vs prior year)",
                f"• Overnight: {k.get('overnight_trips_pct', 0):.1f}% | "
                f"Day trips: {k.get('day_trips_pct', 0):.1f}%",
                f"• Out-of-state visitor days: {k.get('out_of_state_vd_pct', 0):.1f}% "
                f"({k.get('out_of_state_vd_vs_compare_pct', 0):+.1f}pp YoY)",
                f"• Repeat visitors: {k.get('repeat_visitors_pct', 0):.1f}% | "
                f"First-time: {k.get('one_time_visitors_pct', 0):.1f}%",
                f"• Visitor spending share: {k.get('visitor_spending_pct', 0):.1f}% of total",
            ]
        dma = pd.read_sql_query(
            "SELECT dma, visitor_days_share_pct, avg_spend_usd, visitor_days_vs_compare_pct "
            "FROM datafy_overview_dma ORDER BY visitor_days_share_pct DESC LIMIT 6", conn)
        if not dma.empty:
            lines.append("• Top feeder markets (visitor days share):")
            for _, r in dma.iterrows():
                sp = (f", ${r['avg_spend_usd']:.0f}/day" if r.get("avg_spend_usd") else "")
                lines.append(f"  — {r['dma']}: {r['visitor_days_share_pct']:.1f}%"
                             f"{sp} ({r.get('visitor_days_vs_compare_pct', 0):+.1f}pp YoY)")
        spend = pd.read_sql_query(
            "SELECT category, spend_share_pct FROM datafy_overview_category_spending "
            "ORDER BY spend_share_pct DESC", conn)
        if not spend.empty:
            cats = ", ".join(f"{r['category']} {r['spend_share_pct']:.1f}%"
                             for _, r in spend.iterrows())
            lines.append(f"• Spending categories: {cats}")
        mk = pd.read_sql_query("SELECT * FROM datafy_attribution_media_kpis LIMIT 1", conn)
        if not mk.empty:
            m2 = mk.iloc[0]
            lines.append(
                f"• Campaign: {int(m2.get('total_impressions', 0)):,} impressions, "
                f"{int(m2.get('unique_reach', 0)):,} reach, "
                f"{int(m2.get('attributable_trips', 0)):,} attributable trips, "
                f"${m2.get('total_impact_usd', 0):,.0f} economic impact"
            )
    except Exception:
        pass
    return "\n".join(lines)


def build_metrics_context(
    df: pd.DataFrame,
    df_comp: pd.DataFrame,
    df_mon: pd.DataFrame | None = None,
) -> dict:
    """Compute key stats from the active selection + monthly history for AI prompt injection."""
    if df.empty:
        return {}
    n    = len(df)
    half = n // 2
    rec, pri = df.iloc[half:], df.iloc[:half]
    r30  = df.tail(30) if len(df) >= 30 else df
    r90  = df.tail(90) if len(df) >= 90 else df

    weekend = df[df["as_of_date"].dt.dayofweek.isin([4, 5])]   # Fri, Sat
    midweek = df[df["as_of_date"].dt.dayofweek.isin([1, 2])]   # Tue, Wed

    rvp_mean = float(df["revpar"].mean())
    rvp_std  = float(df["revpar"].std())

    ctx = {
        "revpar_30":      float(r30["revpar"].mean()),
        "revpar_90":      float(r90["revpar"].mean()),
        "adr_30":         float(r30["adr"].mean()),
        "occ_30":         float(r30["occupancy"].mean()),
        "rev_30_total":   float(r30["revenue"].sum()),
        "demand_30":      float(r30["demand"].sum()),
        "revpar_delta":   pct_delta(float(rec["revpar"].mean()), float(pri["revpar"].mean())),
        "adr_delta":      pct_delta(float(rec["adr"].mean()),    float(pri["adr"].mean())),
        "occ_delta":      pct_delta(float(rec["occupancy"].mean()), float(pri["occupancy"].mean())),
        "weekend_revpar": float(weekend["revpar"].mean()) if not weekend.empty else 0.0,
        "midweek_revpar": float(midweek["revpar"].mean()) if not midweek.empty else 0.0,
        "weekend_occ":    float(weekend["occupancy"].mean()) if not weekend.empty else 0.0,
        "midweek_occ":    float(midweek["occupancy"].mean()) if not midweek.empty else 0.0,
        "tbid_monthly":   float(r30["revenue"].sum()) * 0.0125,
        "revpar_mean":    rvp_mean,
        "revpar_std":     rvp_std,
        "n_spikes":       int((df["revpar"] > rvp_mean + 2 * rvp_std).sum()),
        "n_drops":        int((df["revpar"] < rvp_mean - 1.5 * rvp_std).sum()),
        "comp_recent_q":  int(df_comp.iloc[-1]["days_above_90_occ"]) if not df_comp.empty else 0,
        "comp_prior_q":   int(df_comp.iloc[-2]["days_above_90_occ"]) if len(df_comp) >= 2 else 0,
        "comp_total_90":  int(df_comp["days_above_90_occ"].sum())    if not df_comp.empty else 0,
        # Monthly placeholders (filled below if data available)
        "revpar_12m": 0.0, "adr_12m": 0.0, "occ_12m": 0.0,
        "rev_12m_total": 0.0, "tbid_12m": 0.0,
        "revpar_yoy_12m": 0.0, "adr_yoy_12m": 0.0, "occ_yoy_12m": 0.0,
        "revpar_best_month": "", "revpar_best_val": 0.0,
        "monthly_data_available": False,
    }

    # ── Monthly context (12-month vs prior-12-month YOY) ──────────────────────
    if df_mon is not None and not df_mon.empty and len(df_mon) >= 12:
        m12   = df_mon.tail(12)
        m_pri = df_mon.iloc[-24:-12] if len(df_mon) >= 24 else pd.DataFrame()
        _occ_col = "occupancy" if "occupancy" in df_mon.columns else None

        ctx["revpar_12m"]     = float(m12["revpar"].mean())
        ctx["adr_12m"]        = float(m12["adr"].mean())
        ctx["occ_12m"]        = float(m12[_occ_col].mean()) if _occ_col else 0.0
        ctx["rev_12m_total"]  = float(m12["revenue"].sum())
        ctx["tbid_12m"]       = ctx["rev_12m_total"] * 0.0125
        ctx["monthly_data_available"] = True

        if not m_pri.empty:
            ctx["revpar_yoy_12m"] = pct_delta(ctx["revpar_12m"], float(m_pri["revpar"].mean()))
            ctx["adr_yoy_12m"]    = pct_delta(ctx["adr_12m"],    float(m_pri["adr"].mean()))
            if _occ_col:
                ctx["occ_yoy_12m"] = pct_delta(ctx["occ_12m"], float(m_pri[_occ_col].mean()))

        # Best month by RevPAR in the last 12 months
        best_idx = m12["revpar"].idxmax()
        ctx["revpar_best_month"] = m12.loc[best_idx, "as_of_date"].strftime("%b %Y")
        ctx["revpar_best_val"]   = float(m12.loc[best_idx, "revpar"])

    return ctx

# ─── AI prompt builders ───────────────────────────────────────────────────────

def _base(m: dict) -> str:
    lines = [
        "VDP Select Portfolio — current data snapshot:",
        f"• 30-day RevPAR: ${m.get('revpar_30',0):.0f} ({m.get('revpar_delta',0):+.1f}% vs. prior period)",
        f"• 30-day ADR: ${m.get('adr_30',0):.0f} ({m.get('adr_delta',0):+.1f}% vs. prior period)",
        f"• 30-day Occupancy: {m.get('occ_30',0):.1f}% ({m.get('occ_delta',0):+.1f}pp vs. prior period)",
        f"• Room Revenue (30d): ${m.get('rev_30_total',0):,.0f}",
        f"• Weekend RevPAR: ${m.get('weekend_revpar',0):.0f}  |  Midweek RevPAR: ${m.get('midweek_revpar',0):.0f}",
        f"• Weekend Occ: {m.get('weekend_occ',0):.1f}%  |  Midweek Occ: {m.get('midweek_occ',0):.1f}%",
        f"• Most recent quarter — days above 90% occ: {m.get('comp_recent_q',0)} "
        f"(prior quarter: {m.get('comp_prior_q',0)})",
    ]
    if m.get("monthly_data_available"):
        lines += [
            "",
            "12-Month Trend (monthly STR exports — Layer 1 data):",
            f"• 12-month avg RevPAR: ${m.get('revpar_12m',0):.0f} ({m.get('revpar_yoy_12m',0):+.1f}% YOY)",
            f"• 12-month avg ADR: ${m.get('adr_12m',0):.0f} ({m.get('adr_yoy_12m',0):+.1f}% YOY)",
            f"• 12-month avg Occupancy: {m.get('occ_12m',0):.1f}% ({m.get('occ_yoy_12m',0):+.1f}pp YOY)",
            f"• 12-month Room Revenue: ${m.get('rev_12m_total',0):,.0f}  |  Est. TBID: ${m.get('tbid_12m',0):,.0f}",
            f"• Peak month (last 12): {m.get('revpar_best_month','')} at ${m.get('revpar_best_val',0):.0f} RevPAR",
        ]
    # Append CoStar and Datafy context so all AI prompts have full market + visitor benchmarks
    lines.append(build_costar_context())
    lines.append(build_datafy_context())
    return "\n".join(lines)


def build_prompt(key: str, m: dict) -> str:
    if not m:
        return "No data loaded yet. Please ensure the pipeline has run."
    b = _base(m)
    p = {
        "revpar": (
            f"{b}\n\n"
            "Analyze the primary drivers of RevPAR performance and provide 3 actionable "
            "recommendations for the VDP TBID board to protect or grow RevPAR over the next 90 days."
        ),
        "opportunity": (
            f"{b}\n\n"
            "Quantify the midweek revenue gap. Estimate the monthly revenue left on the table "
            "at current weekday vs. weekend occupancy. Suggest 3 specific marketing tactics "
            "to close the gap — tailored to Dana Point's coastal leisure visitor profile."
        ),
        "ohana": (
            f"{b}\n\n"
            "Using only the Datafy visitor economy data and STR daily data provided above, "
            "identify the highest-demand periods in the last 12 months where occupancy or ADR "
            "spiked significantly above surrounding baseline days. These likely correspond to "
            "major events (Ohana Fest, surf competitions, holidays). "
            "For the top 2–3 compression peaks: quantify the ADR lift vs. 7-day baseline, "
            "the occupancy delta, and the estimated incremental room revenue. "
            "Use the Datafy out-of-state visitor data and spending categories to model the "
            "economic multiplier effect. Provide 2 specific recommendations to maximize hotel "
            "revenue during future high-demand event periods."
        ),
        "board": (
            f"{b}\n"
            f"• Est. TBID monthly revenue: ${m.get('tbid_monthly',0):,.0f} (blended 1.25%)\n\n"
            "Generate 5 concise talking points for the VDP TBID board meeting. "
            "Each point = one sentence with a specific data reference from the data provided. "
            "Format for an executive audience. Only cite numbers that appear in the data above."
        ),
        "midweek": (
            f"{b}\n\n"
            "Perform a detailed weekend vs. midweek comparison. Quantify the RevPAR gap, "
            "identify which specific days are strongest and weakest, and propose a targeted "
            "promotional strategy (packages, local partnerships, or dynamic pricing) to lift "
            "Tuesday–Thursday demand in the Dana Point market."
        ),
        "tbid": (
            f"{b}\n"
            f"• Blended TBID rate: 1.25% (Tier 1 = 1.0%, Tier 2 = 1.5%)\n"
            f"• Dana Point TOT rate: 10% of gross room revenue\n\n"
            "Project TBID assessment revenue for the next quarter and full year at current pace. "
            "Explain how to frame this for city council, and compare TBID vs. TOT revenue "
            "contribution from the VDP portfolio."
        ),
        "anomaly": (
            f"{b}\n"
            f"• RevPAR mean: ${m.get('revpar_mean',0):.0f}  |  Std dev: ${m.get('revpar_std',0):.0f}\n"
            f"• Positive outliers (>2σ above mean): {m.get('n_spikes',0)} days\n"
            f"• Negative outliers (<1.5σ below mean): {m.get('n_drops',0)} days\n\n"
            "Perform anomaly analysis. Identify the most likely causes for each outlier type "
            "(events, seasonality, market disruptions). Flag any pattern requiring board "
            "attention or immediate pricing action."
        ),
        "forecast": (
            f"{b}\n"
            f"• 90-day RevPAR avg: ${m.get('revpar_90',0):.0f}\n"
            "• Seasonal pattern: Dana Point peaks Q3 (summer), shoulder in Q1/Q4\n\n"
            "Generate a 30-day forward-looking forecast for occupancy, ADR, and RevPAR. "
            "List your key assumptions, note upcoming seasonal factors, and provide "
            "a bull / base / bear scenario for the next 30 days."
        ),
    }
    return p.get(key, f"{b}\n\nProvide a general performance summary for the VDP portfolio.")


def build_custom_prompt(question: str, m: dict) -> str:
    return (
        f"You are analyzing VDP Select Portfolio data for Visit Dana Point.\n{_base(m)}\n\n"
        f"User question: {question.strip()}"
    )

# ─── Local fallback (no API key) ─────────────────────────────────────────────

def local_fallback(key: str, m: dict) -> str:
    """Smart template responses powered by real computed metrics."""
    wknd_pct = (
        (m.get("weekend_revpar", 0) / m.get("midweek_revpar", 1) - 1) * 100
        if m.get("midweek_revpar") else 0
    )
    d = {
        "revpar": (
            f"**RevPAR Analysis** *(local mode — connect API key for live Claude analysis)*\n\n"
            f"• 30-day RevPAR: **${m.get('revpar_30',0):.0f}** "
            f"({m.get('revpar_delta',0):+.1f}% vs. prior period)\n"
            f"• Primary driver: ADR ({m.get('adr_delta',0):+.1f}%), "
            f"occupancy contributing {m.get('occ_delta',0):+.1f}pp\n"
            f"• Weekend RevPAR is **{wknd_pct:.0f}% higher** than midweek — "
            f"pricing power concentrated on weekends\n\n"
            f"**Recommendations:**\n"
            f"1. Lock in weekend rate gains with 2-night minimums during the "
            f"{m.get('comp_recent_q',0)} high-compression days per quarter\n"
            f"2. Deploy Tue–Thu packages to close the "
            f"${m.get('midweek_revpar',0):.0f} → ${m.get('weekend_revpar',0):.0f} RevPAR gap\n"
            f"3. Present the positive trend to the TBID board as pricing-power evidence\n\n"
            f"**→ Action:** Launch a midweek package targeting the LA feeder market "
            f"for the next 60 days."
        ),
        "board": (
            f"**Board Talking Points** *(local mode)*\n\n"
            f"1. **Revenue Momentum:** RevPAR at **${m.get('revpar_30',0):.0f}** "
            f"({m.get('revpar_delta',0):+.1f}% vs. prior period)\n"
            f"2. **Compression Building:** **{m.get('comp_recent_q',0)}** days above 90% occ "
            f"last quarter (vs. {m.get('comp_prior_q',0)} prior) — rate increases are justified\n"
            f"3. **Visitor Economy:** {int(_ev_kpis.iloc[0].get('total_trips',0))/1e6:.2f}M annual trips, "
            f"{_ev_kpis.iloc[0].get('out_of_state_vd_pct',0):.1f}% out-of-state visitor days\n"
            if not _ev_kpis.empty else
            f"3. **Visitor Economy:** See Datafy Visitor Intelligence tab for live data\n"
            f"4. **TBID Revenue:** Tracking ~**${m.get('tbid_monthly',0):,.0f}/month** "
            f"at blended 1.25%\n"
            f"5. **Midweek Opportunity:** Weekend/midweek RevPAR gap "
            f"({wknd_pct:.0f}%) = highest-leverage growth lever\n\n"
            f"**→ Action:** Request board approval for a $50K midweek demand-generation campaign."
        ),
        "ohana": (
            f"**Event Impact Analysis** *(local mode — add API key for live AI analysis)*\n\n"
            f"• 2025 annual overnight trip share: **{_ev_kpis.iloc[0].get('overnight_trips_pct',0):.1f}%** "
            f"of {int(_ev_kpis.iloc[0].get('total_trips',0))/1e6:.2f}M total trips\n"
            f"• Out-of-state visitor days: **{_ev_kpis.iloc[0].get('out_of_state_vd_pct',0):.1f}%** "
            f"— genuine incremental tourism (not local displacement)\n"
            f"• Top feeder market: **{_ev_dma.iloc[0]['dma'] if not _ev_dma.empty else 'Los Angeles'}** "
            f"({_ev_dma.iloc[0]['visitor_days_share_pct']:.1f}% of visitor days)\n"
            f"• Top spending category: **{_ev_spend.iloc[0]['category'] if not _ev_spend.empty else 'Accommodations'}** "
            f"({_ev_spend.iloc[0]['spend_share_pct']:.1f}% of visitor spend)\n\n"
            f"**→ Action:** Use STR daily data to identify compression peaks — add API key for "
            f"event-specific ADR lift analysis tied to your actual demand data."
            if not _ev_kpis.empty and not _ev_dma.empty and not _ev_spend.empty
            else "Add API key for AI analysis · Load Datafy data for visitor economy insights."
        ),
        "anomaly": (
            f"**Anomaly Detection** *(local mode)*\n\n"
            f"• RevPAR mean: **${m.get('revpar_mean',0):.0f}** ± ${m.get('revpar_std',0):.0f}\n"
            f"• Positive outliers (>2σ): **{m.get('n_spikes',0)} days** — "
            f"likely event/weekend compression\n"
            f"• Negative outliers (<1.5σ): **{m.get('n_drops',0)} days** — "
            f"concentrated midweek/shoulder periods\n\n"
            f"Anomaly dots are annotated on the RevPAR chart above "
            f"(green = spike, red = drop). Hover for context.\n\n"
            f"**→ Action:** Cross-reference spike days with your event calendar to "
            f"build a predictive demand model."
        ),
        "forecast": (
            f"**30-Day Forecast** *(local mode)*\n\n"
            f"Based on trailing data and Dana Point seasonality patterns:\n"
            f"• **Occupancy:** {m.get('occ_30',0):.1f}% → "
            f"projected {m.get('occ_30',0)+1.5:.1f}% (seasonal uplift)\n"
            f"• **ADR:** ${m.get('adr_30',0):.0f} → "
            f"projected ${m.get('adr_30',0)+8:.0f} (rate momentum)\n"
            f"• **RevPAR:** ${m.get('revpar_30',0):.0f} → "
            f"projected ${m.get('revpar_30',0)*1.04:.0f} (base case)\n\n"
            f"Key assumptions: no market disruption, seasonal demand strengthening, "
            f"current rate strategy maintained.\n\n"
            f"**→ Action:** Stress-test shoulder-period rate floors against the bear scenario."
        ),
    }
    return d.get(
        key,
        f"**VDP Snapshot** *(local mode)*\n\n"
        f"• RevPAR: **${m.get('revpar_30',0):.0f}** ({m.get('revpar_delta',0):+.1f}%)\n"
        f"• ADR: **${m.get('adr_30',0):.0f}** | Occupancy: **{m.get('occ_30',0):.1f}%**\n"
        f"• Est. TBID monthly: **${m.get('tbid_monthly',0):,.0f}**\n\n"
        f"💡 Enter your Anthropic API key in the sidebar for live Claude analysis.",
    )

# ─── Claude streaming generator ───────────────────────────────────────────────

def stream_claude_response(prompt: str, api_key: str):
    """Yields text chunks from Claude streaming API for use with st.write_stream.

    The SYSTEM_PROMPT is passed as a structured content block with cache_control so the
    Anthropic API caches it for 5 minutes (ephemeral TTL). Repeated queries within that
    window pay cache-read rates (~10% of base input token price) instead of full input rates.
    The dynamic user message (live metrics via _base / build_prompt) is never cached.
    """
    if not ANTHROPIC_AVAILABLE:
        yield "⚠️ `anthropic` package not installed. Run: `pip install anthropic` in your venv."
        return
    try:
        client = anthropic.Anthropic(api_key=api_key)
        with client.messages.stream(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            for text in stream.text_stream:
                yield text
    except Exception as e:
        err = str(e)
        if "401" in err or "authentication" in err.lower():
            yield "⚠️ **Invalid API key.** Check your key in the sidebar and try again."
        elif "429" in err:
            yield "⚠️ **Rate limited.** Please wait a moment and try again."
        else:
            yield f"⚠️ **API Error:** {err[:200]}"

# ─── SVG Icon Library ─────────────────────────────────────────────────────────

def kpi_metric_svg(label: str, positive: bool = True, raw_value: float = 0.0) -> str:
    """Return SMIL-animated SVG infographic for each KPI card."""
    c  = "#21808D" if positive else "#C0152F"
    bg = "rgba(33,128,141,0.10)" if positive else "rgba(192,21,47,0.08)"

    def wrap(inner: str) -> str:
        return (
            f'<svg width="42" height="42" viewBox="0 0 42 42" fill="none" '
            f'xmlns="http://www.w3.org/2000/svg">'
            f'<rect width="42" height="42" rx="9" fill="{bg}"/>'
            f'{inner}</svg>'
        )

    if label == "RevPAR":
        pts = "5,32 11,24 17,27 23,17 29,12 37,7" if positive else "5,10 11,18 17,15 23,25 29,30 37,35"
        tip_y = "7" if positive else "35"
        return wrap(
            f'<polyline points="{pts}" stroke="{c}" stroke-width="2.3" fill="none"'
            f' stroke-linecap="round" stroke-linejoin="round"'
            f' stroke-dasharray="72" stroke-dashoffset="72">'
            f'<animate attributeName="stroke-dashoffset" from="72" to="0"'
            f' dur="1.0s" fill="freeze" begin="0.1s" calcMode="spline"'
            f' keySplines="0.25,0.46,0.45,0.94"/></polyline>'
            f'<circle cx="37" cy="{tip_y}" r="3.2" fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.15s" fill="freeze" begin="1.0s"/>'
            f'<animate attributeName="r" values="3.2;4.8;3.2" dur="2.4s" repeatCount="indefinite" begin="1.2s"/>'
            f'</circle>'
        )

    if label == "ADR":
        return wrap(
            f'<path d="M8,34 L8,10 L28,10 L35,21 L28,34 Z" stroke="{c}" stroke-width="2.1" fill="none"'
            f' stroke-linejoin="round" stroke-dasharray="92" stroke-dashoffset="92">'
            f'<animate attributeName="stroke-dashoffset" from="92" to="0" dur="0.9s" fill="freeze" begin="0.1s"/></path>'
            f'<circle cx="13" cy="16" r="2.2" fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="0.9s"/></circle>'
            f'<line x1="22" y1="15" x2="22" y2="29" stroke="{c}" stroke-width="1.9" stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="1.0s"/></line>'
            f'<line x1="17" y1="19" x2="27" y2="19" stroke="{c}" stroke-width="1.6" stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="1.1s"/></line>'
            f'<line x1="17" y1="25" x2="27" y2="25" stroke="{c}" stroke-width="1.6" stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="1.1s"/></line>'
        )

    if label == "Occupancy":
        circ   = 100.53   # 2π × 16
        pct    = max(0.0, min(100.0, raw_value))
        offset = round(circ * (1.0 - pct / 100.0), 2)
        return wrap(
            f'<circle cx="21" cy="21" r="16" stroke="rgba(0,0,0,0.07)" stroke-width="4" fill="none"/>'
            f'<circle cx="21" cy="21" r="16" stroke="{c}" stroke-width="4" fill="none"'
            f' stroke-linecap="round" stroke-dasharray="{circ}" stroke-dashoffset="{circ}"'
            f' transform="rotate(-90 21 21)">'
            f'<animate attributeName="stroke-dashoffset" from="{circ}" to="{offset}"'
            f' dur="1.4s" fill="freeze" begin="0.2s" calcMode="spline" keySplines="0.25,0.46,0.45,0.94"/>'
            f'</circle>'
            f'<text x="21" y="25" text-anchor="middle" font-size="10" font-weight="700"'
            f' fill="{c}" font-family="system-ui,sans-serif" opacity="0">{pct:.0f}%'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.4s" fill="freeze" begin="0.9s"/>'
            f'</text>'
        )

    if label == "Room Revenue":
        heights = [14, 22, 18, 28] if positive else [28, 18, 22, 14]
        xs      = [5, 13, 21, 29]
        bars    = ""
        for i, (x, h) in enumerate(zip(xs, heights)):
            y     = 36 - h
            op    = round(0.45 + i * 0.14, 2)
            delay = round(0.08 + i * 0.13, 2)
            bars += (
                f'<rect x="{x}" y="36" width="9" height="0" rx="2.5" fill="{c}" opacity="{op}">'
                f'<animate attributeName="height" from="0" to="{h}" dur="0.7s" fill="freeze" begin="{delay}s"'
                f' calcMode="spline" keySplines="0.25,0.46,0.45,0.94"/>'
                f'<animate attributeName="y" from="36" to="{y}" dur="0.7s" fill="freeze" begin="{delay}s"'
                f' calcMode="spline" keySplines="0.25,0.46,0.45,0.94"/>'
                f'</rect>'
            )
        return wrap(bars)

    if label == "Rooms Sold":
        return wrap(
            f'<rect x="10" y="7" width="22" height="28" rx="3" stroke="{c}" stroke-width="2.1" fill="none"'
            f' stroke-dasharray="82" stroke-dashoffset="82">'
            f'<animate attributeName="stroke-dashoffset" from="82" to="0" dur="0.8s" fill="freeze" begin="0.1s"/></rect>'
            f'<line x1="16" y1="7" x2="16" y2="3" stroke="{c}" stroke-width="2.2" stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="0.8s"/></line>'
            f'<line x1="26" y1="7" x2="26" y2="3" stroke="{c}" stroke-width="2.2" stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="0.8s"/></line>'
            f'<line x1="14" y1="17" x2="28" y2="17" stroke="{c}" stroke-width="1.3" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="0.3" dur="0.2s" fill="freeze" begin="0.9s"/></line>'
            f'<line x1="14" y1="23" x2="28" y2="23" stroke="{c}" stroke-width="1.3" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="0.3" dur="0.2s" fill="freeze" begin="1.0s"/></line>'
            f'<path d="M15,21 L19,25 L27,15" stroke="{c}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" fill="none"'
            f' stroke-dasharray="22" stroke-dashoffset="22" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.1s" fill="freeze" begin="1.1s"/>'
            f'<animate attributeName="stroke-dashoffset" from="22" to="0" dur="0.45s" fill="freeze" begin="1.1s"/>'
            f'</path>'
        )

    if label in ("Est. TBID Rev", "TBID"):
        col_data = [(5, 18), (13, 26), (21, 22), (29, 32)]
        cols = ""
        for i, (x, h) in enumerate(col_data):
            y     = 37 - h
            delay = round(0.05 + i * 0.10, 2)
            op    = round(0.48 + i * 0.14, 2)
            cols += (
                f'<rect x="{x}" y="37" width="9" height="0" rx="2" fill="{c}" opacity="{op}">'
                f'<animate attributeName="height" from="0" to="{h}" dur="0.6s" fill="freeze" begin="{delay}s"'
                f' calcMode="spline" keySplines="0.3,0.6,0.5,1.0"/>'
                f'<animate attributeName="y" from="37" to="{y}" dur="0.6s" fill="freeze" begin="{delay}s"'
                f' calcMode="spline" keySplines="0.3,0.6,0.5,1.0"/>'
                f'</rect>'
            )
        cols += f'<rect x="3" y="36" width="36" height="2.5" rx="1.2" fill="{c}" opacity="0.3"/>'
        return wrap(cols)

    return '<svg width="42" height="42" viewBox="0 0 42 42"></svg>'


def insight_icon_svg(kind: str, icon_key: str) -> str:
    """Animated SVG icon for insight cards (20px)."""
    color_map = {"positive": "#21808D", "negative": "#C0152F", "warning": "#E68161", "info": "#21808D"}
    c = color_map.get(kind, "#21808D")

    icons: dict[str, str] = {
        "trend_up": (
            f'<svg width="20" height="20" viewBox="0 0 20 20" fill="none">'
            f'<polyline points="2,16 6,10 10,12 14,6 18,3" stroke="{c}" stroke-width="2.1"'
            f' stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="32" stroke-dashoffset="32">'
            f'<animate attributeName="stroke-dashoffset" from="32" to="0" dur="0.8s" fill="freeze" begin="0.1s"/></polyline>'
            f'<circle cx="18" cy="3" r="2.2" fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.15s" fill="freeze" begin="0.85s"/>'
            f'<animate attributeName="r" values="2.2;3.4;2.2" dur="2.2s" repeatCount="indefinite" begin="1.1s"/>'
            f'</circle></svg>'
        ),
        "trend_down": (
            f'<svg width="20" height="20" viewBox="0 0 20 20" fill="none">'
            f'<polyline points="2,4 6,10 10,8 14,14 18,17" stroke="{c}" stroke-width="2.1"'
            f' stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="32" stroke-dashoffset="32">'
            f'<animate attributeName="stroke-dashoffset" from="32" to="0" dur="0.8s" fill="freeze" begin="0.1s"/></polyline>'
            f'<circle cx="18" cy="17" r="2.2" fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.15s" fill="freeze" begin="0.85s"/>'
            f'</circle></svg>'
        ),
        "trend_flat": (
            f'<svg width="20" height="20" viewBox="0 0 20 20" fill="none">'
            f'<line x1="2" y1="10" x2="18" y2="10" stroke="{c}" stroke-width="2.1"'
            f' stroke-linecap="round" stroke-dasharray="20" stroke-dashoffset="20">'
            f'<animate attributeName="stroke-dashoffset" from="20" to="0" dur="0.5s" fill="freeze" begin="0.1s"/></line>'
            f'<circle cx="18" cy="10" r="2.4" fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="0.55s"/>'
            f'<animate attributeName="r" values="2.4;3.4;2.4" dur="2.5s" repeatCount="indefinite" begin="0.8s"/>'
            f'</circle></svg>'
        ),
        "moon": (
            f'<svg width="20" height="20" viewBox="0 0 20 20" fill="none">'
            f'<path d="M14,3 A8,8,0,1,0,14,17 A6,6,0,1,1,14,3 Z" stroke="{c}" stroke-width="1.9"'
            f' fill="none" stroke-dasharray="44" stroke-dashoffset="44">'
            f'<animate attributeName="stroke-dashoffset" from="44" to="0" dur="0.9s" fill="freeze" begin="0.1s"/>'
            f'</path></svg>'
        ),
        "scale": (
            f'<svg width="20" height="20" viewBox="0 0 20 20" fill="none">'
            f'<line x1="10" y1="4" x2="10" y2="17" stroke="{c}" stroke-width="1.9" stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="0.1s"/></line>'
            f'<line x1="4" y1="7" x2="16" y2="7" stroke="{c}" stroke-width="1.9" stroke-linecap="round"'
            f' stroke-dasharray="14" stroke-dashoffset="14">'
            f'<animate attributeName="stroke-dashoffset" from="14" to="0" dur="0.4s" fill="freeze" begin="0.2s"/></line>'
            f'<circle cx="6" cy="12" r="3" stroke="{c}" stroke-width="1.7" fill="none" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.25s" fill="freeze" begin="0.55s"/>'
            f'</circle><circle cx="14" cy="12" r="3" stroke="{c}" stroke-width="1.7" fill="none" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.25s" fill="freeze" begin="0.55s"/>'
            f'</circle></svg>'
        ),
        "fire": (
            f'<svg width="20" height="20" viewBox="0 0 20 20" fill="none">'
            f'<path d="M10,18 C10,18 4,15 4,9 C4,6 7,4 10,4 C10,4 9,7 11,8 C11,8 14,5 13,3'
            f' C16,5 17,9 17,11 C17,15 14,18 10,18 Z" stroke="{c}" stroke-width="1.8" fill="none"'
            f' stroke-linejoin="round" stroke-dasharray="52" stroke-dashoffset="52">'
            f'<animate attributeName="stroke-dashoffset" from="52" to="0" dur="0.9s" fill="freeze" begin="0.1s"/>'
            f'</path><path d="M10,16 C10,16 8,14 8,12 C8,11 9,10 10,11 C11,10 12,11 12,12 C12,14 10,16 10,16 Z"'
            f' fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="0.85" dur="0.3s" fill="freeze" begin="0.9s"/>'
            f'</path></svg>'
        ),
        "eye": (
            f'<svg width="20" height="20" viewBox="0 0 20 20" fill="none">'
            f'<path d="M2,10 C2,10 5,4 10,4 C15,4 18,10 18,10 C18,10 15,16 10,16 C5,16 2,10 2,10 Z"'
            f' stroke="{c}" stroke-width="1.8" fill="none" stroke-dasharray="46" stroke-dashoffset="46">'
            f'<animate attributeName="stroke-dashoffset" from="46" to="0" dur="0.7s" fill="freeze" begin="0.1s"/>'
            f'</path><circle cx="10" cy="10" r="3" stroke="{c}" stroke-width="1.8" fill="none" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.25s" fill="freeze" begin="0.7s"/>'
            f'<animate attributeName="r" values="3;3.8;3" dur="2.2s" repeatCount="indefinite" begin="1.0s"/>'
            f'</circle></svg>'
        ),
        "search": (
            f'<svg width="20" height="20" viewBox="0 0 20 20" fill="none">'
            f'<circle cx="9" cy="9" r="6" stroke="{c}" stroke-width="2" fill="none"'
            f' stroke-dasharray="40" stroke-dashoffset="40">'
            f'<animate attributeName="stroke-dashoffset" from="40" to="0" dur="0.7s" fill="freeze" begin="0.1s"/>'
            f'</circle><line x1="13.5" y1="13.5" x2="18" y2="18" stroke="{c}" stroke-width="2"'
            f' stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="0.7s"/>'
            f'</line></svg>'
        ),
    }
    return icons.get(icon_key, "")


def event_icon_svg(icon_key: str) -> str:
    """Animated SVG icon for event stat cards (32px)."""
    c = "#21808D"
    icons: dict[str, str] = {
        "money": (
            f'<svg width="32" height="32" viewBox="0 0 32 32" fill="none">'
            f'<circle cx="16" cy="16" r="13" stroke="{c}" stroke-width="2" fill="none"'
            f' stroke-dasharray="82" stroke-dashoffset="82">'
            f'<animate attributeName="stroke-dashoffset" from="82" to="0" dur="1.0s" fill="freeze" begin="0.1s"/>'
            f'</circle><line x1="16" y1="9" x2="16" y2="23" stroke="{c}" stroke-width="2" stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="1.0s"/></line>'
            f'<path d="M12,12 Q12,10 16,10 Q20,10 20,13 Q20,16 16,16 Q20,16 20,19 Q20,22 16,22 Q12,22 12,20"'
            f' stroke="{c}" stroke-width="1.8" fill="none" stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.3s" fill="freeze" begin="1.1s"/>'
            f'</path></svg>'
        ),
        "globe": (
            f'<svg width="32" height="32" viewBox="0 0 32 32" fill="none">'
            f'<circle cx="16" cy="16" r="13" stroke="{c}" stroke-width="2" fill="none"'
            f' stroke-dasharray="82" stroke-dashoffset="82">'
            f'<animate attributeName="stroke-dashoffset" from="82" to="0" dur="1.0s" fill="freeze" begin="0.1s"/>'
            f'</circle><ellipse cx="16" cy="16" rx="5.5" ry="13" stroke="{c}" stroke-width="1.6" fill="none"'
            f' stroke-dasharray="40" stroke-dashoffset="40">'
            f'<animate attributeName="stroke-dashoffset" from="40" to="0" dur="0.8s" fill="freeze" begin="0.9s"/>'
            f'</ellipse><line x1="3" y1="16" x2="29" y2="16" stroke="{c}" stroke-width="1.6"'
            f' stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.25s" fill="freeze" begin="1.0s"/></line>'
            f'<line x1="5" y1="11" x2="27" y2="11" stroke="{c}" stroke-width="1.4"'
            f' stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="0.55" dur="0.25s" fill="freeze" begin="1.1s"/></line>'
            f'<line x1="5" y1="21" x2="27" y2="21" stroke="{c}" stroke-width="1.4"'
            f' stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="0.55" dur="0.25s" fill="freeze" begin="1.1s"/></line>'
            f'</svg>'
        ),
        "tag": (
            f'<svg width="32" height="32" viewBox="0 0 32 32" fill="none">'
            f'<path d="M6,6 L6,18 L17,29 L27,19 L16,8 Z" stroke="{c}" stroke-width="2" fill="none"'
            f' stroke-linejoin="round" stroke-dasharray="80" stroke-dashoffset="80">'
            f'<animate attributeName="stroke-dashoffset" from="80" to="0" dur="0.9s" fill="freeze" begin="0.1s"/>'
            f'</path><circle cx="12" cy="12" r="2.5" fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="0.9s"/>'
            f'<animate attributeName="r" values="2.5;3.5;2.5" dur="2.2s" repeatCount="indefinite" begin="1.2s"/>'
            f'</circle></svg>'
        ),
        "bed": (
            f'<svg width="32" height="32" viewBox="0 0 32 32" fill="none">'
            f'<path d="M4,22 L4,14 L28,14 L28,22" stroke="{c}" stroke-width="2" fill="none"'
            f' stroke-linecap="round" stroke-dasharray="52" stroke-dashoffset="52">'
            f'<animate attributeName="stroke-dashoffset" from="52" to="0" dur="0.8s" fill="freeze" begin="0.1s"/>'
            f'</path><rect x="4" y="22" width="24" height="6" rx="2" stroke="{c}" stroke-width="2" fill="none"'
            f' stroke-dasharray="62" stroke-dashoffset="62">'
            f'<animate attributeName="stroke-dashoffset" from="62" to="0" dur="0.6s" fill="freeze" begin="0.8s"/>'
            f'</rect><rect x="6" y="10" width="8" height="4" rx="1.5" stroke="{c}" stroke-width="1.6" fill="none" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="1.3s"/>'
            f'</rect><rect x="18" y="10" width="8" height="4" rx="1.5" stroke="{c}" stroke-width="1.6" fill="none" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="1.3s"/>'
            f'</rect></svg>'
        ),
        "plane": (
            f'<svg width="32" height="32" viewBox="0 0 32 32" fill="none">'
            f'<path d="M5,20 L27,10 L22,28 L16,22 L5,20 Z" stroke="{c}" stroke-width="2" fill="none"'
            f' stroke-linejoin="round" stroke-dasharray="70" stroke-dashoffset="70">'
            f'<animate attributeName="stroke-dashoffset" from="70" to="0" dur="0.9s" fill="freeze" begin="0.1s"/>'
            f'</path><line x1="16" y1="22" x2="21" y2="14" stroke="{c}" stroke-width="1.8"'
            f' stroke-linecap="round" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.25s" fill="freeze" begin="0.95s"/>'
            f'</line></svg>'
        ),
        "chart_up": (
            f'<svg width="32" height="32" viewBox="0 0 32 32" fill="none">'
            f'<polyline points="4,26 10,18 16,20 22,12 28,7" stroke="{c}" stroke-width="2.2"'
            f' stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="55" stroke-dashoffset="55">'
            f'<animate attributeName="stroke-dashoffset" from="55" to="0" dur="1.0s" fill="freeze" begin="0.1s"/>'
            f'</polyline><circle cx="28" cy="7" r="3" fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.15s" fill="freeze" begin="1.0s"/>'
            f'<animate attributeName="r" values="3;4.5;3" dur="2.4s" repeatCount="indefinite" begin="1.2s"/>'
            f'</circle><line x1="4" y1="28" x2="28" y2="28" stroke="{c}" stroke-width="1.5"'
            f' stroke-linecap="round" opacity="0.35"/>'
            f'</svg>'
        ),
    }
    return icons.get(icon_key, "")


# ─── UI helpers ───────────────────────────────────────────────────────────────

def kpi_card(label, value, delta, positive=True, neutral=False,
             icon: str = "", date_label: str = "", raw_value: float = 0.0) -> str:
    css      = "kpi-delta-neutral" if neutral else ("kpi-delta-pos" if positive else "kpi-delta-neg")
    arrow    = "" if neutral else ("▲ " if positive else "▼ ")
    date_html = f'<div class="kpi-date">📅 {date_label}</div>' if date_label else ""
    svg      = kpi_metric_svg(label, positive, raw_value)
    return (
        f'<div class="kpi-card">'
        f'<div class="kpi-header">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-icon-svg">{svg}</div>'
        f'</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="{css}">{arrow}{delta}</div>'
        f'{date_html}'
        f'</div>'
    )


def insight_card(title, body, kind="info", icon: str = "", date_label: str = "") -> str:
    svg_icon  = insight_icon_svg(kind, icon) if icon else ""
    icon_html = f'<span class="insight-icon">{svg_icon}</span>' if svg_icon else ""
    date_html = (
        f'<div style="font-size:10px;opacity:.4;margin-top:6px;">📅 {date_label}</div>'
        if date_label else ""
    )
    return (
        f'<div class="insight-card insight-{kind}">'
        f'<div class="insight-title">{icon_html}{title}</div>'
        f'<p class="insight-body">{body}</p>'
        f'{date_html}'
        f'</div>'
    )


def event_stat(val, label, icon: str = "", date: str = "") -> str:
    svg       = event_icon_svg(icon) if icon else ""
    icon_html = f'<div class="event-icon">{svg}</div>' if svg else ""
    date_html = f'<div class="event-date">📅 {date}</div>' if date else ""
    return (
        f'<div class="event-stat">'
        f'{icon_html}'
        f'<div class="event-val">{val}</div>'
        f'<div class="event-label">{label}</div>'
        f'{date_html}'
        f'</div>'
    )


def style_fig(fig: go.Figure, height: int = 280) -> go.Figure:
    _font = "Plus Jakarta Sans, Inter, system-ui, sans-serif"
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family=_font, size=12),
        height=height,
        margin=dict(l=0, r=0, t=36, b=0),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
            font=dict(size=11, family=_font),
            bgcolor="rgba(0,0,0,0)", borderwidth=0,
        ),
        hoverlabel=dict(
            bgcolor="rgba(18,24,30,0.90)",
            bordercolor="rgba(33,128,141,0.45)",
            font=dict(size=12, family=_font, color="#ffffff"),
        ),
    )
    fig.update_xaxes(
        showgrid=False, zeroline=False,
        tickfont=dict(size=11, family=_font),
        linecolor="rgba(0,0,0,0.08)",
    )
    fig.update_yaxes(
        gridcolor="rgba(127,127,127,0.10)",
        gridwidth=1,
        zeroline=False,
        tickfont=dict(size=11, family=_font),
    )
    return fig


def compute_overview_kpis(df: pd.DataFrame, grain: str = "Daily") -> list[dict]:
    n = len(df)
    if n < 4:
        return []
    half = n // 2
    rec, pri = df.iloc[half:], df.iloc[:half]
    def avg(d, c): return float(d[c].mean()) if c in d and not d[c].isna().all() else 0.0
    def tot(d, c): return float(d[c].sum())  if c in d and not d[c].isna().all() else 0.0
    r_occ, p_occ = avg(rec,"occupancy"),  avg(pri,"occupancy")
    r_adr, p_adr = avg(rec,"adr"),        avg(pri,"adr")
    r_rvp, p_rvp = avg(rec,"revpar"),     avg(pri,"revpar")
    r_rev, p_rev = tot(rec,"revenue"),    tot(pri,"revenue")
    r_dem, p_dem = tot(rec,"demand"),     tot(pri,"demand")
    tbid = r_rev * 0.0125
    # Date label — grain-aware formatting
    _fmt = "%b %Y" if grain == "Monthly" else "%b %d, %Y"
    rec_start = rec["as_of_date"].min().strftime(_fmt)
    rec_end   = rec["as_of_date"].max().strftime(_fmt)
    date_lbl  = f"{rec_start} – {rec_end}"
    return [
        {"label":"RevPAR",        "value":f"${r_rvp:.2f}",
         "delta":f"{pct_delta(r_rvp,p_rvp):+.1f}% vs. prior",  "positive":r_rvp>=p_rvp, "date_label":date_lbl, "raw_value":r_rvp},
        {"label":"ADR",           "value":f"${r_adr:.2f}",
         "delta":f"{pct_delta(r_adr,p_adr):+.1f}% vs. prior",  "positive":r_adr>=p_adr, "date_label":date_lbl, "raw_value":r_adr},
        {"label":"Occupancy",     "value":f"{r_occ:.1f}%",
         "delta":f"{pct_delta(r_occ,p_occ):+.1f}pp vs. prior", "positive":r_occ>=p_occ, "date_label":date_lbl, "raw_value":r_occ},
        {"label":"Room Revenue",  "value":f"${r_rev/1e6:.2f}M",
         "delta":f"{pct_delta(r_rev,p_rev):+.1f}% vs. prior",  "positive":r_rev>=p_rev, "date_label":date_lbl, "raw_value":r_rev},
        {"label":"Rooms Sold",    "value":f"{r_dem:,.0f}",
         "delta":f"{pct_delta(r_dem,p_dem):+.1f}% vs. prior",  "positive":r_dem>=p_dem, "date_label":date_lbl, "raw_value":r_dem},
        {"label":"Est. TBID Rev", "value":f"${tbid/1e3:.0f}K",
         "delta":"blended 1.25%", "positive":True, "neutral":True, "date_label":date_lbl, "raw_value":tbid},
    ]


def generate_ai_insights(df: pd.DataFrame, df_comp: pd.DataFrame, m: dict) -> list[dict]:
    """Rule-based insight cards — always available, no API key required."""
    cards = []

    # Date label from the active selection
    if not df.empty:
        date_lbl = (
            f"{df['as_of_date'].min().strftime('%b %d, %Y')} – "
            f"{df['as_of_date'].max().strftime('%b %d, %Y')}"
        )
    else:
        date_lbl = ""

    # 1 — RevPAR momentum
    d = m.get("revpar_delta", 0)
    if d > 3:
        cards.append({"kind":"positive","icon":"trend_up","title":"RevPAR Momentum","date_label":date_lbl,
            "body":f"RevPAR up {d:.1f}% vs. prior period — ADR strength is the primary driver. "
                   f"Current pricing is working; lock in rate gains on compression nights."})
    elif d < -3:
        cards.append({"kind":"negative","icon":"trend_down","title":"RevPAR Pressure","date_label":date_lbl,
            "body":f"RevPAR down {abs(d):.1f}% vs. prior period. Determine whether softness is "
                   f"demand-driven (occ decline) or pricing-driven (ADR compression) before acting."})
    else:
        cards.append({"kind":"info","icon":"trend_flat","title":"RevPAR Holding Steady","date_label":date_lbl,
            "body":f"RevPAR {d:+.1f}% vs. prior period — within normal variance. "
                   f"Midweek gap remains the highest-leverage growth lever."})

    # 2 — Midweek softness
    wknd = m.get("weekend_revpar", 0)
    midwk = m.get("midweek_revpar", 0)
    if wknd > 0 and midwk > 0:
        gap = (wknd / midwk - 1) * 100
        if gap > 25:
            cards.append({"kind":"warning","icon":"moon","title":"Midweek Softness","date_label":date_lbl,
                "body":f"Weekend RevPAR (${wknd:.0f}) is {gap:.0f}% above midweek (${midwk:.0f}). "
                       f"Tue–Thu packages and local partnerships are the fastest path to closing this gap."})
        else:
            cards.append({"kind":"positive","icon":"scale","title":"Balanced Demand Mix","date_label":date_lbl,
                "body":f"Weekend/midweek RevPAR spread is only {gap:.0f}% — healthy for a leisure "
                       f"destination. Midweek demand is holding relatively well."})

    # 3 — Compression trend
    crec = m.get("comp_recent_q", 0)
    cpri = m.get("comp_prior_q", 0)
    if crec > 0:
        if crec > cpri:
            cards.append({"kind":"positive","icon":"fire","title":"Compression Building","date_label":date_lbl,
                "body":f"{crec} days above 90% occupancy last quarter (vs. {cpri} prior) — "
                       f"a clear signal that rate increases are justified on your highest-demand nights."})
        else:
            cards.append({"kind":"info","icon":"eye","title":"Compression Watch","date_label":date_lbl,
                "body":f"{crec} days above 90% occ last quarter (vs. {cpri} prior). "
                       f"Monitor as we move into peak season."})

    # 4 — Anomaly flag
    ns = m.get("n_spikes", 0)
    nd = m.get("n_drops", 0)
    if ns > 0 or nd > 0:
        cards.append({"kind":"info","icon":"search","title":f"{ns + nd} Anomalies Detected","date_label":date_lbl,
            "body":f"{ns} revenue spikes (>2σ) and {nd} drops (<1.5σ) in the selected period. "
                   f"Green/red markers on the RevPAR chart identify each event — hover for context."})

    return cards[:4]

# ─── UI helpers: data cards ───────────────────────────────────────────────────

def empty_state(icon: str, title: str, body: str) -> str:
    """Styled empty-state card — pass to st.markdown(unsafe_allow_html=True)."""
    return (
        f'<div class="empty-card">'
        f'<div class="empty-icon">{icon}</div>'
        f'<div class="empty-title">{title}</div>'
        f'<div class="empty-body">{body}</div>'
        f'</div>'
    )


def source_card(dot: str, name: str, meta: str, count: str) -> str:
    """Styled data-source health card."""
    return (
        f'<div class="src-card">'
        f'<span class="src-dot">{dot}</span>'
        f'<div><div class="src-name">{name}</div>'
        f'<div class="src-meta">{meta}</div></div>'
        f'<div class="src-count">{count}</div>'
        f'</div>'
    )


def grain_badge(g: str) -> str:
    return f'<span class="grain-badge">{g}</span>'


# ─── Load data ────────────────────────────────────────────────────────────────
df_daily    = load_str_daily()     # source='STR', grain='daily'
df_monthly  = load_str_monthly()   # source='STR', grain='monthly'
df_kpi      = load_kpi_daily()
df_comp     = load_compression()
df_log      = load_load_log()
# CoStar market intelligence
df_cs_mon   = load_costar_monthly()
df_cs_snap  = load_costar_snapshot()
df_cs_pipe  = load_costar_pipeline()
df_cs_chain = load_costar_chain()
df_cs_comp  = load_costar_compset()

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div class="sidebar-brand">🌊 Visit Dana Point</div>'
        '<div style="font-size:12px;opacity:0.55;font-weight:500;margin-top:3px;margin-bottom:2px;">'
        'VDP Select Portfolio &nbsp;·&nbsp; 12 Properties &nbsp;·&nbsp; Anaheim Area</div>',
        unsafe_allow_html=True,
    )
    st.divider()

    # Grain must be chosen FIRST so the range options below can adapt
    grain = st.selectbox(
        "Data Grain", ["Daily", "Monthly"], index=0,
        help="Daily: STR daily metrics · Monthly: pre-aggregated monthly STR exports",
    )

    # Range options are grain-aware:
    #   Daily   → "days" window applied to df_daily (max 365 d is fine)
    #   Monthly → "days" window applied to df_monthly; monthly data spans
    #             30+ years so we offer year/decade-scale windows
    if grain == "Monthly":
        RANGE_OPTIONS = {
            "Last 12 Months": 365,
            "Last 3 Years":   1095,
            "Last 5 Years":   1825,
            "All History":    36500,
        }
        _range_default = 1          # default: Last 3 Years
    else:
        RANGE_OPTIONS = {
            "Last 30 Days":   30,
            "Last 90 Days":   90,
            "Last 6 Months":  180,
            "Last 12 Months": 365,
        }
        _range_default = 1          # default: Last 90 Days

    range_label = st.selectbox("Date Range", list(RANGE_OPTIONS.keys()), index=_range_default)
    days = RANGE_OPTIONS[range_label]

    # ── Advanced Filters ──────────────────────────────────────────────────────
    with st.expander("🔬 Advanced Filters", expanded=False):
        if grain == "Monthly" and not df_monthly.empty:
            _all_years = sorted(df_monthly["as_of_date"].dt.year.unique().tolist())
            _def_years = _all_years[-10:] if len(_all_years) > 10 else _all_years
            _sel_years = st.multiselect(
                "Filter by Year", _all_years, default=_def_years,
                help="Limit analysis to selected years",
            )
            st.session_state["adv_filter_years"] = _sel_years or _all_years
            _active_filter = len(_sel_years) < len(_all_years) and bool(_sel_years)
        elif grain == "Daily" and not df_daily.empty:
            _dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            _sel_dow = st.multiselect(
                "Filter by Day of Week", _dow_names, default=_dow_names,
                help="Include only selected days in analysis",
            )
            st.session_state["adv_filter_dow"] = _sel_dow or _dow_names
            _active_filter = len(_sel_dow) < 7 and bool(_sel_dow)
        else:
            st.caption("No filter options available for current grain + data.")
            _active_filter = False
        if _active_filter:
            st.caption("✅ Filter active — charts reflect filtered subset")

    st.divider()

    # ── AI Analyst config ──────────────────────────────────────────────────────
    st.markdown("**🤖 AI Analyst**")
    api_key_raw = st.text_input(
        "Anthropic API Key",
        type="password",
        placeholder="sk-ant-api03-…",
        value=_ENV_API_KEY,          # pre-fills from .env if set
        help="Loaded from .env ANTHROPIC_API_KEY · override here anytime.",
        key="api_key_field",
    )
    api_key       = api_key_raw.strip()
    api_key_valid = bool(api_key) and api_key.startswith("sk-ant-") and len(api_key) > 20

    if api_key and not api_key_valid:
        st.caption("⚪ Invalid key format")
    elif api_key_valid:
        st.caption(f"🟢 Connected · {CLAUDE_MODEL}")
    else:
        st.caption("⚪ Not connected — local mode active")

    if not ANTHROPIC_AVAILABLE:
        st.warning("`anthropic` not installed.\nRun: `pip install anthropic`", icon="⚠️")

    st.divider()

    # ── Pipeline status ────────────────────────────────────────────────────────
    counts           = get_table_counts()
    # Derive raw row counts — prefer loaded dataframes as truth (immune to cache staleness)
    str_daily_rows   = counts.get("str_daily_rows",   0) if not df_daily.empty   else 0
    str_monthly_rows = counts.get("str_monthly_rows", 0) if not df_monthly.empty else 0
    # Fallback: if cache returned 0/stale but df has data, use fact-table count directly
    if str_daily_rows == 0 and not df_daily.empty:
        str_daily_rows = len(df_daily)
    if str_monthly_rows == 0 and not df_monthly.empty:
        str_monthly_rows = len(df_monthly)

    _run_at  = df_log.iloc[0]["run_at"] if not df_log.empty else None
    last_log = str(_run_at)[:10] if pd.notna(_run_at) else "—"

    _d_dot   = "🟢" if str_daily_rows   > 0 else "⚫"
    _m_dot   = "🟢" if str_monthly_rows > 0 else "⚫"
    _d_label = f"{str_daily_rows:,} rows"   if str_daily_rows   > 0 else "No data"
    _m_label = f"{str_monthly_rows:,} rows" if str_monthly_rows > 0 else "No data"
    _cs_rows = counts.get("costar_monthly_performance", 0)
    _cs_dot  = "🟢" if isinstance(_cs_rows, int) and _cs_rows > 0 else "⚫"
    _cs_label = f"{_cs_rows:,} rows" if isinstance(_cs_rows, int) and _cs_rows > 0 else "No data"
    st.markdown("**Pipeline Status**")
    st.markdown(f"{_d_dot} STR Daily &nbsp;·&nbsp; {_d_label}")
    st.markdown(f"{_m_dot} STR Monthly &nbsp;·&nbsp; {_m_label}")
    st.markdown(f"{_cs_dot} CoStar Market &nbsp;·&nbsp; {_cs_label}")
    # Datafy — query directly to bypass any cache staleness
    _df_rows = 0
    try:
        _df_conn = get_connection()
        _df_rows = _df_conn.execute(
            "SELECT COUNT(*) FROM datafy_overview_kpis").fetchone()[0]
        # If the overview table has data, sum all datafy tables
        if _df_rows > 0:
            _df_rows = counts.get("datafy_total_rows", 0) or 0
            if _df_rows == 0:
                # Direct fallback sum
                for _dt in ["datafy_overview_kpis","datafy_overview_dma",
                            "datafy_overview_category_spending","datafy_overview_demographics",
                            "datafy_attribution_media_kpis","datafy_attribution_website_kpis",
                            "datafy_social_top_pages","datafy_social_traffic_sources"]:
                    try:
                        _df_rows += _df_conn.execute(
                            f"SELECT COUNT(*) FROM {_dt}").fetchone()[0]
                    except Exception:
                        pass
    except Exception:
        _df_rows = counts.get("datafy_total_rows", 0)
    _df_dot   = "🟢" if _df_rows > 0 else "⚫"
    _df_label = f"{_df_rows:,} rows" if _df_rows > 0 else "No data"
    st.markdown(f"{_df_dot} Datafy &nbsp;·&nbsp; {_df_label}")
    st.caption(f"Last ETL run: {last_log}")

    if not df_daily.empty:
        min_d = df_daily["as_of_date"].min().strftime("%b %d, %Y")
        max_d = df_daily["as_of_date"].max().strftime("%b %d, %Y")
        st.caption(f"Daily data: {min_d} → {max_d}")
    if not df_monthly.empty:
        # Show only recent date range (last 5 years) in sidebar for clarity
        _mon_recent = df_monthly[df_monthly["as_of_date"].dt.year >= df_monthly["as_of_date"].dt.year.max() - 4]
        if not _mon_recent.empty:
            mon_min = _mon_recent["as_of_date"].min().strftime("%b %Y")
            mon_max = _mon_recent["as_of_date"].max().strftime("%b %Y")
            _total_yrs = df_monthly["as_of_date"].dt.year.nunique()
            st.caption(f"Monthly (recent): {mon_min} → {mon_max}")
            st.caption(f"Full history: {_total_yrs} years in database")
    # Datafy date range
    _df_kpi_row = load_datafy_kpis()
    if not _df_kpi_row.empty:
        _dfr = _df_kpi_row.iloc[0]
        st.caption(f"Datafy: {str(_dfr.get('report_period_start',''))[:7]} → "
                   f"{str(_dfr.get('report_period_end',''))[:7]}")

    st.divider()

    # ── Pipeline Controls ──────────────────────────────────────────────────────
    st.markdown("**⚙️ Pipeline Controls**")

    run_btn   = st.button(
        "🔄 Run Pipeline",
        use_container_width=True,
        help="Load STR exports → compute KPIs → refresh dashboard",
    )
    fetch_btn = st.button(
        "📡 Fetch External Data",
        use_container_width=True,
        help="CoStar · FRED · CA TOT · JWA passenger stats",
    )

    if run_btn:
        with st.spinner("Running pipeline…"):
            proc = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / "run_pipeline.py")],
                capture_output=True,
                text=True,
                cwd=str(ROOT),
            )
        if proc.returncode == 0:
            st.success("Pipeline complete ✓")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Pipeline failed — see detail below")
            err_text = (proc.stderr or proc.stdout or "No output captured").strip()
            st.code(err_text[-800:], language="text")

    if fetch_btn:
        with st.spinner("Fetching external sources…"):
            proc = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / "fetch_external_all.py")],
                capture_output=True,
                text=True,
                cwd=str(ROOT),
            )
        if proc.returncode == 0:
            st.success("External fetch complete ✓")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Fetch failed — see detail below")
            err_text = (proc.stderr or proc.stdout or "No output captured").strip()
            st.code(err_text[-800:], language="text")

# ─── Active dataset (grain-aware) + filtered selection ────────────────────────
# grain='Daily'   → df_active uses df_daily   (explicit grain='daily'  query)
# grain='Monthly' → df_active uses df_monthly  (explicit grain='monthly' query)
df_active = df_daily if grain == "Daily" else df_monthly

if not df_active.empty:
    max_date   = df_active["as_of_date"].max()
    cutoff     = max_date - timedelta(days=days)
    df_sel     = df_active[df_active["as_of_date"] > cutoff].copy()
    df_kpi_sel = (df_kpi[df_kpi["as_of_date"] > cutoff].copy()
                  if not df_kpi.empty else pd.DataFrame())
else:
    df_sel = df_kpi_sel = pd.DataFrame()

# Apply advanced filters (from sidebar expander) to df_sel
if grain == "Monthly" and not df_sel.empty:
    _yr_filter = st.session_state.get("adv_filter_years", [])
    if _yr_filter:
        df_sel = df_sel[df_sel["as_of_date"].dt.year.isin(_yr_filter)]
elif grain == "Daily" and not df_sel.empty:
    _dow_map    = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}
    _dow_filter = st.session_state.get("adv_filter_dow", list(_dow_map.keys()))
    _dow_nums   = [_dow_map[d] for d in _dow_filter if d in _dow_map]
    if _dow_nums and len(_dow_nums) < 7:
        df_sel = df_sel[df_sel["as_of_date"].dt.dayofweek.isin(_dow_nums)]

m = build_metrics_context(df_sel, df_comp, df_monthly)

# ─── Header ───────────────────────────────────────────────────────────────────
if not df_active.empty:
    last_upd = df_active["as_of_date"].max().strftime(
        "%b %Y" if grain == "Monthly" else "%b %d, %Y"
    )
elif not df_daily.empty:
    last_upd = df_daily["as_of_date"].max().strftime("%b %d, %Y")
else:
    last_upd = "N/A"

st.markdown(
    f'<div class="hero-banner">'
    f'<a href="?" style="text-decoration:none;">'
    f'<div class="hero-title">Visit Dana Point — Analytics</div>'
    f'</a>'
    f'<div class="hero-subtitle">'
    f'VDP Select Portfolio &nbsp;·&nbsp; 12 Properties &nbsp;·&nbsp; '
    f'{range_label} &nbsp;·&nbsp; Last updated {last_upd}'
    f'</div>'
    f'</div>',
    unsafe_allow_html=True,
)

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_ov, tab_tr, tab_ev, tab_cs, tab_vi, tab_rev, tab_dig, tab_cx, tab_rpt, tab_dl = st.tabs([
    "📊 Overview", "📈 Trends", "🎪 Event Impact", "🏨 Market Intelligence",
    "👥 Visitor Intelligence", "💰 Revenue & TBID", "📱 Digital & Campaign",
    "🔗 Cross-Dataset", "📋 Board Reports", "🗂 Data Log",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_ov:

    # ── AI Analyst Panel ───────────────────────────────────────────────────────
    with st.expander("🧠 VDP Intelligence — Interrogate your data", expanded=True):
        st.markdown('<span class="ai-chip">AI ANALYST</span>', unsafe_allow_html=True)

        PROMPTS_META = [
            ("💹 RevPAR Drivers",       "revpar"),
            ("📅 Opportunity Nights",   "opportunity"),
            ("🎵 Event Impact Analysis", "ohana"),
            ("📋 Board Talking Points", "board"),
            ("🌙 Weekend vs Midweek",   "midweek"),
            ("🏨 TBID Revenue Est.",    "tbid"),
            ("🔍 Detect Anomalies",     "anomaly"),
            ("📈 30-Day Forecast",      "forecast"),
        ]

        btn_cols = st.columns(4)
        for i, (label, key) in enumerate(PROMPTS_META):
            with btn_cols[i % 4]:
                if st.button(label, key=f"ai_btn_{key}", use_container_width=True):
                    st.session_state.ai_current_prompt = build_prompt(key, m)
                    st.session_state.ai_prompt_label   = label
                    st.session_state.ai_result         = ""
                    st.session_state.ai_needs_call     = True

        st.markdown("<br>", unsafe_allow_html=True)

        inp_col, btn_col = st.columns([5, 1])
        with inp_col:
            custom_q = st.text_input(
                "custom_q", label_visibility="collapsed",
                placeholder="Ask anything about your VDP portfolio data…",
                key="ai_custom_input",
            )
        with btn_col:
            if st.button("⚡ Brief Me", type="primary", use_container_width=True):
                if custom_q.strip():
                    st.session_state.ai_current_prompt = build_custom_prompt(custom_q, m)
                    st.session_state.ai_prompt_label   = f"💬 {custom_q.strip()[:60]}"
                    st.session_state.ai_result         = ""
                    st.session_state.ai_needs_call     = True

        # ── Response area ──────────────────────────────────────────────────────
        if st.session_state.ai_needs_call or st.session_state.ai_result:
            st.markdown("---")
            if st.session_state.ai_prompt_label:
                st.caption(f"**Query:** {st.session_state.ai_prompt_label}")

            if st.session_state.ai_needs_call:
                prompt_to_run = st.session_state.ai_current_prompt
                # Detect which preset key matches the label
                matched_key = next(
                    (k for lbl, k in PROMPTS_META
                     if lbl == st.session_state.ai_prompt_label), "default"
                )
                if api_key_valid and ANTHROPIC_AVAILABLE:
                    with st.chat_message("assistant", avatar="🌊"):
                        response = st.write_stream(
                            stream_claude_response(prompt_to_run, api_key)
                        )
                    st.session_state.ai_result = response
                else:
                    response = local_fallback(matched_key, m)
                    with st.chat_message("assistant", avatar="🌊"):
                        st.markdown(response)
                    st.session_state.ai_result = response
                    if not api_key_valid:
                        st.caption(
                            "💡 Add your Anthropic API key in the sidebar for live Claude streaming."
                        )
                st.session_state.ai_needs_call = False

            elif st.session_state.ai_result:
                with st.chat_message("assistant", avatar="🌊"):
                    st.markdown(st.session_state.ai_result)

    # ── AI Insight Cards ───────────────────────────────────────────────────────
    if m:
        st.markdown(
            '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:14px;'
            'font-weight:700;letter-spacing:-0.01em;margin-bottom:2px;">Auto-Detected Insights</div>'
            '<div style="font-size:11px;opacity:0.50;font-weight:500;margin-bottom:8px;">'
            'Pattern analysis from the selected date range</div>',
            unsafe_allow_html=True,
        )
        insights = generate_ai_insights(df_sel, df_comp, m)
        if insights:
            ic = st.columns(len(insights))
            for i, ins in enumerate(insights):
                with ic[i]:
                    st.markdown(
                        insight_card(ins["title"], ins["body"], ins["kind"],
                                     ins.get("icon", ""), ins.get("date_label", "")),
                        unsafe_allow_html=True,
                    )
        st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("---")

    # ── KPI Cards ──────────────────────────────────────────────────────────────
    kpis = compute_overview_kpis(df_sel, grain)
    if not kpis:
        st.markdown(empty_state(
            "📊",
            f"No {grain.lower()} data in the selected range.",
            "Adjust the date range or run the pipeline to load data.",
        ), unsafe_allow_html=True)
    else:
        st.markdown(
            '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:14px;'
            'font-weight:700;letter-spacing:-0.01em;margin-bottom:8px;">Key Performance Indicators</div>',
            unsafe_allow_html=True,
        )
        cols = st.columns(3)
        for i, k in enumerate(kpis):
            with cols[i % 3]:
                st.markdown(
                    kpi_card(k["label"], k["value"], k["delta"],
                             k.get("positive", True), k.get("neutral", False),
                             "", k.get("date_label", ""), k.get("raw_value", 0.0)),
                    unsafe_allow_html=True,
                )

        # ── Monthly Performance Strip ──────────────────────────────────────────
        if m.get("monthly_data_available") and not df_monthly.empty:
            _m12 = df_monthly.tail(12)
            _m_pri = df_monthly.iloc[-24:-12] if len(df_monthly) >= 24 else pd.DataFrame()
            _occ_col = "occupancy" if "occupancy" in df_monthly.columns else None

            _m12_rvp = float(_m12["revpar"].mean())
            _m12_adr = float(_m12["adr"].mean())
            _m12_occ = float(_m12[_occ_col].mean()) if _occ_col else 0.0
            _m12_rev = float(_m12["revenue"].sum())
            _m12_dem = float(_m12["demand"].sum())
            _m12_tbd = _m12_rev * 0.0125

            _mp_rvp = float(_m_pri["revpar"].mean()) if not _m_pri.empty else _m12_rvp
            _mp_adr = float(_m_pri["adr"].mean())    if not _m_pri.empty else _m12_adr
            _mp_occ = float(_m_pri[_occ_col].mean()) if (not _m_pri.empty and _occ_col) else _m12_occ
            _mp_rev = float(_m_pri["revenue"].sum())  if not _m_pri.empty else _m12_rev
            _mp_dem = float(_m_pri["demand"].sum())   if not _m_pri.empty else _m12_dem

            _min_mo = _m12["as_of_date"].min().strftime("%b %Y")
            _max_mo = _m12["as_of_date"].max().strftime("%b %Y")
            _mo_lbl = f"{_min_mo} – {_max_mo}"

            st.markdown(
                '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:14px;'
                'font-weight:700;letter-spacing:-0.01em;margin-bottom:2px;margin-top:4px;">'
                '12-Month Performance — Monthly STR</div>'
                f'<div style="font-size:11px;opacity:0.50;font-weight:500;margin-bottom:8px;">'
                f'Layer 1 verified data &nbsp;·&nbsp; {_mo_lbl} &nbsp;·&nbsp; vs. prior 12 months</div>',
                unsafe_allow_html=True,
            )
            _m_kpis = [
                {"label": "RevPAR",       "value": f"${_m12_rvp:.2f}",
                 "delta": f"{pct_delta(_m12_rvp,_mp_rvp):+.1f}% YOY",
                 "positive": _m12_rvp >= _mp_rvp, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_rvp},
                {"label": "ADR",          "value": f"${_m12_adr:.2f}",
                 "delta": f"{pct_delta(_m12_adr,_mp_adr):+.1f}% YOY",
                 "positive": _m12_adr >= _mp_adr, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_adr},
                {"label": "Occupancy",    "value": f"{_m12_occ:.1f}%",
                 "delta": f"{pct_delta(_m12_occ,_mp_occ):+.1f}pp YOY",
                 "positive": _m12_occ >= _mp_occ, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_occ},
                {"label": "Room Revenue", "value": f"${_m12_rev/1e6:.2f}M",
                 "delta": f"{pct_delta(_m12_rev,_mp_rev):+.1f}% YOY",
                 "positive": _m12_rev >= _mp_rev, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_rev},
                {"label": "Rooms Sold",   "value": f"{_m12_dem:,.0f}",
                 "delta": f"{pct_delta(_m12_dem,_mp_dem):+.1f}% YOY",
                 "positive": _m12_dem >= _mp_dem, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_dem},
                {"label": "Est. TBID Rev","value": f"${_m12_tbd/1e3:.0f}K",
                 "delta": "blended 1.25%", "positive": True, "neutral": True,
                 "date_label": _mo_lbl, "raw_value": _m12_tbd},
            ]
            _m_cols = st.columns(3)
            for i, k in enumerate(_m_kpis):
                with _m_cols[i % 3]:
                    st.markdown(
                        kpi_card(k["label"], k["value"], k["delta"],
                                 k.get("positive", True), k.get("neutral", False),
                                 "", k.get("date_label", ""), k.get("raw_value", 0.0)),
                        unsafe_allow_html=True,
                    )

        st.markdown("---")

        # ── Row 1: RevPAR with anomaly detection  |  Occ vs ADR ───────────────
        c1, c2 = st.columns(2)

        with c1:
            st.markdown('<div class="chart-header">RevPAR Trend — Anomaly Detection</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Teal markers = spikes >2σ &nbsp;·&nbsp; Red = drops <1.5σ &nbsp;·&nbsp; Hover for context</div>', unsafe_allow_html=True)

            rvp_mean = df_sel["revpar"].mean()
            rvp_std  = df_sel["revpar"].std()
            spikes   = df_sel[df_sel["revpar"] > rvp_mean + 2 * rvp_std]
            drops    = df_sel[df_sel["revpar"] < rvp_mean - 1.5 * rvp_std]

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["revpar"],
                fill="tozeroy",
                line=dict(color=TEAL, width=2.2),
                fillcolor="rgba(33,128,141,0.12)",
                mode="lines", name="RevPAR",
                hovertemplate="<b>%{x|%b %d, %Y}</b><br>RevPAR: $%{y:.0f}<extra></extra>",
            ))
            fig.add_hline(
                y=rvp_mean, line_dash="dash", line_color="rgba(167,169,169,0.45)",
                annotation_text=f"Avg ${rvp_mean:.0f}",
                annotation_position="top right",
                annotation_font=dict(size=11, color="rgba(127,127,127,0.80)"),
            )
            if not spikes.empty:
                fig.add_trace(go.Scatter(
                    x=spikes["as_of_date"], y=spikes["revpar"],
                    mode="markers",
                    marker=dict(color=TEAL, size=11, symbol="circle",
                                opacity=0.85, line=dict(width=0)),
                    name="⚡ Spike",
                    hovertemplate=(
                        "<b>⚡ Revenue Spike</b><br>"
                        "%{x|%b %d}: $%{y:.0f}<br>"
                        "<i>Likely event or weekend surge</i><extra></extra>"
                    ),
                ))
            if not drops.empty:
                fig.add_trace(go.Scatter(
                    x=drops["as_of_date"], y=drops["revpar"],
                    mode="markers",
                    marker=dict(color=RED, size=11, symbol="circle",
                                opacity=0.85, line=dict(width=0)),
                    name="⚠️ Drop",
                    hovertemplate=(
                        "<b>⚠️ Below Average</b><br>"
                        "%{x|%b %d}: $%{y:.0f}<br>"
                        "<i>Investigate demand drivers</i><extra></extra>"
                    ),
                ))
            fig.update_layout(yaxis_tickprefix="$")
            st.plotly_chart(style_fig(fig), use_container_width=True)

        with c2:
            st.markdown('<div class="chart-header">Occupancy vs. ADR</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Dual-axis &nbsp;·&nbsp; fill rate & pricing power</div>', unsafe_allow_html=True)
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["occupancy"],
                name="Occupancy %", line=dict(color=TEAL, width=2), mode="lines",
                hovertemplate="%{x|%b %d}: %{y:.1f}%<extra>Occ</extra>",
            ), secondary_y=False)
            fig.add_trace(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["adr"],
                name="ADR $", line=dict(color=ORANGE, width=2), mode="lines",
                hovertemplate="%{x|%b %d}: $%{y:.0f}<extra>ADR</extra>",
            ), secondary_y=True)
            fig.update_yaxes(title_text="Occ %", ticksuffix="%", secondary_y=False)
            fig.update_yaxes(title_text="ADR $", tickprefix="$",
                             secondary_y=True, showgrid=False)
            st.plotly_chart(style_fig(fig), use_container_width=True)

        # ── Row 2: Day-of-Week  |  Supply vs Demand ───────────────────────────
        c3, c4 = st.columns(2)

        with c3:
            if grain == "Daily":
                st.markdown('<div class="chart-header">Day-of-Week Performance</div>', unsafe_allow_html=True)
                st.markdown('<div class="chart-caption">Avg RevPAR &nbsp;·&nbsp; Orange = opportunity nights below average</div>', unsafe_allow_html=True)
                dow_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                tmp = df_sel.copy()
                tmp["dow"] = tmp["as_of_date"].dt.strftime("%a")
                dow_avg = tmp.groupby("dow")["revpar"].mean().reindex(dow_order)
                ov_avg  = dow_avg.mean()
                colors  = [TEAL if v >= ov_avg else ORANGE for v in dow_avg.fillna(0)]
                fig = go.Figure(go.Bar(
                    x=dow_avg.index, y=dow_avg.values,
                    marker=dict(color=colors, line_width=0, cornerradius=6),
                    hovertemplate=(
                        "<b>%{x}</b><br>Avg RevPAR: $%{y:.0f}<br>"
                        "<i>Click 'Opportunity Nights' for AI analysis</i><extra></extra>"
                    ),
                ))
                fig.add_hline(y=ov_avg, line_dash="dash", line_color="rgba(167,169,169,0.45)",
                              annotation_text=f"Avg ${ov_avg:.0f}", annotation_position="top right",
                              annotation_font=dict(size=11, color="rgba(127,127,127,0.80)"))
                fig.update_layout(yaxis_tickprefix="$", showlegend=False)
                st.plotly_chart(style_fig(fig), use_container_width=True)
            else:
                # Monthly grain → show calendar-month seasonality using all monthly history
                st.markdown('<div class="chart-header">Month-of-Year Seasonality</div>', unsafe_allow_html=True)
                st.markdown('<div class="chart-caption">Avg RevPAR by calendar month &nbsp;·&nbsp; all available monthly history</div>', unsafe_allow_html=True)
                month_order = ["Jan","Feb","Mar","Apr","May","Jun",
                               "Jul","Aug","Sep","Oct","Nov","Dec"]
                tmp = df_monthly.copy()
                tmp["mon"] = tmp["as_of_date"].dt.strftime("%b")
                mon_avg = tmp.groupby("mon")["revpar"].mean().reindex(month_order)
                ov_avg  = mon_avg.mean()
                colors  = [
                    TEAL if (pd.notna(v) and v >= ov_avg) else ORANGE
                    for v in mon_avg
                ]
                fig = go.Figure(go.Bar(
                    x=mon_avg.index, y=mon_avg.values,
                    marker=dict(color=colors, line_width=0, cornerradius=6),
                    hovertemplate="<b>%{x}</b><br>Avg RevPAR: $%{y:.0f}<extra></extra>",
                ))
                fig.add_hline(y=ov_avg, line_dash="dash", line_color="rgba(167,169,169,0.45)",
                              annotation_text=f"Avg ${ov_avg:.0f}", annotation_position="top right",
                              annotation_font=dict(size=11, color="rgba(127,127,127,0.80)"))
                fig.update_layout(yaxis_tickprefix="$", showlegend=False)
                st.plotly_chart(style_fig(fig), use_container_width=True)

        with c4:
            st.markdown('<div class="chart-header">Supply vs. Demand</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Room inventory vs. rooms sold &nbsp;·&nbsp; gap = unrealized revenue</div>', unsafe_allow_html=True)
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["supply"],
                name="Supply",
                line=dict(color="rgba(167,169,169,0.7)", width=1.5, dash="dot"),
                mode="lines",
            ))
            fig.add_trace(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["demand"],
                fill="tozeroy",
                line=dict(color=TEAL, width=2),
                fillcolor="rgba(33,128,141,0.10)",
                name="Demand", mode="lines",
            ))
            st.plotly_chart(style_fig(fig), use_container_width=True)

        # ── Row 3: Monthly RevPAR & ADR trend (always-on from monthly STR) ────
        if not df_monthly.empty and len(df_monthly) >= 6:
            st.markdown("---")
            _mo24 = df_monthly.tail(24).copy()
            _mo24["month_label"] = _mo24["as_of_date"].dt.strftime("%b %Y")
            _occ_col = "occupancy" if "occupancy" in _mo24.columns else None

            ca, cb = st.columns(2)
            with ca:
                st.markdown('<div class="chart-header">Monthly RevPAR — Last 24 Months</div>', unsafe_allow_html=True)
                st.markdown('<div class="chart-caption">Layer 1 verified · monthly STR exports &nbsp;·&nbsp; color = above/below 24-month avg</div>', unsafe_allow_html=True)
                _avg24 = _mo24["revpar"].mean()
                _colors24 = [TEAL if v >= _avg24 else ORANGE for v in _mo24["revpar"]]
                fig = go.Figure(go.Bar(
                    x=_mo24["month_label"], y=_mo24["revpar"],
                    marker=dict(color=_colors24, line_width=0, cornerradius=5),
                    hovertemplate="<b>%{x}</b><br>RevPAR: $%{y:.0f}<extra></extra>",
                ))
                fig.add_hline(y=_avg24, line_dash="dash", line_color="rgba(167,169,169,0.45)",
                              annotation_text=f"24-mo avg ${_avg24:.0f}",
                              annotation_position="top right",
                              annotation_font=dict(size=11, color="rgba(127,127,127,0.80)"))
                fig.update_layout(yaxis_tickprefix="$", showlegend=False)
                st.plotly_chart(style_fig(fig, height=260), use_container_width=True)

            with cb:
                st.markdown('<div class="chart-header">Monthly ADR vs. Occupancy — Last 24 Months</div>', unsafe_allow_html=True)
                st.markdown('<div class="chart-caption">Dual-axis &nbsp;·&nbsp; pricing power vs. fill rate trend</div>', unsafe_allow_html=True)
                fig2 = make_subplots(specs=[[{"secondary_y": True}]])
                fig2.add_trace(go.Scatter(
                    x=_mo24["month_label"], y=_mo24["adr"],
                    name="ADR $", line=dict(color=ORANGE, width=2.2),
                    mode="lines+markers", marker=dict(size=4, color=ORANGE),
                    hovertemplate="<b>%{x}</b><br>ADR: $%{y:.0f}<extra></extra>",
                ), secondary_y=False)
                if _occ_col:
                    fig2.add_trace(go.Scatter(
                        x=_mo24["month_label"], y=_mo24[_occ_col],
                        name="Occ %", line=dict(color=TEAL, width=2.2),
                        mode="lines+markers", marker=dict(size=4, color=TEAL),
                        hovertemplate="<b>%{x}</b><br>Occ: %{y:.1f}%<extra></extra>",
                    ), secondary_y=True)
                fig2.update_yaxes(title_text="ADR ($)", tickprefix="$", secondary_y=False)
                if _occ_col:
                    fig2.update_yaxes(title_text="Occ %", ticksuffix="%",
                                      secondary_y=True, showgrid=False)
                st.plotly_chart(style_fig(fig2, height=260), use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — TRENDS
# ══════════════════════════════════════════════════════════════════════════════
with tab_tr:
    # ── Build monthly aggregation for Trends tab ───────────────────────────────
    # Priority: df_monthly (monthly STR exports) → fallback to kpi_daily_summary
    # df_monthly is now always preferred since monthly loader is live.
    if not df_monthly.empty:
        _tmp_tr = df_monthly.copy()
        _tmp_tr["month"] = _tmp_tr["as_of_date"].dt.to_period("M")
        _occ_agg = "occupancy" if "occupancy" in _tmp_tr.columns else None
        _agg_spec = {"revpar": ("revpar", "mean"), "adr": ("adr", "mean")}
        if _occ_agg:
            _agg_spec["occ_pct"] = (_occ_agg, "mean")
        monthly = (
            _tmp_tr.groupby("month")
            .agg(**_agg_spec)
            .reset_index()
            .sort_values("month")
        )
        if "occ_pct" not in monthly.columns:
            monthly["occ_pct"] = np.nan
        # YOY from monthly series (shift 12)
        monthly["revpar_yoy"] = (monthly["revpar"] / monthly["revpar"].shift(12) - 1) * 100
        monthly["month_label"] = monthly["month"].dt.strftime("%b %Y")
        _trends_ok = True
    elif not df_kpi.empty:
        df_kpi_all = df_kpi.copy()
        df_kpi_all["month"] = df_kpi_all["as_of_date"].dt.to_period("M")
        monthly = (
            df_kpi_all.groupby("month")
            .agg(revpar=("revpar","mean"), adr=("adr","mean"),
                 occ_pct=("occ_pct","mean"), revpar_yoy=("revpar_yoy","mean"))
            .reset_index()
        )
        monthly["month_label"] = monthly["month"].dt.strftime("%b %Y")
        _trends_ok = True
    else:
        monthly    = pd.DataFrame()
        _trends_ok = False

    if not _trends_ok:
        if grain == "Monthly":
            st.markdown(empty_state(
                "📈", "No monthly STR data loaded.",
                "Run the pipeline to load STR monthly exports into fact_str_metrics.",
            ), unsafe_allow_html=True)
        else:
            st.markdown(empty_state(
                "📈", "KPI summary is empty.",
                "kpi_daily_summary has no rows — run compute_kpis.py first.",
            ), unsafe_allow_html=True)
    else:
        # ── YOY RevPAR bar (full width) ────────────────────────────────────────
        st.markdown('<div class="chart-header">YOY RevPAR Change</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">Year-over-year % change by month &nbsp;·&nbsp; teal = growth &nbsp;·&nbsp; red = decline</div>', unsafe_allow_html=True)
        yoy = monthly.dropna(subset=["revpar_yoy"])
        if yoy.empty:
            st.markdown(empty_state(
                "📊", "YOY requires 12+ months of history.",
                "Load more data and re-run compute_kpis.py to unlock year-over-year charts.",
            ), unsafe_allow_html=True)
        else:
            bar_colors = [GREEN if v >= 0 else RED for v in yoy["revpar_yoy"]]
            fig = go.Figure(go.Bar(
                x=yoy["month_label"], y=yoy["revpar_yoy"],
                marker=dict(color=bar_colors, line_width=0, cornerradius=5),
                text=[f"{v:+.1f}%" for v in yoy["revpar_yoy"]],
                textposition="outside",
                textfont=dict(size=10, family="Plus Jakarta Sans, Inter, sans-serif"),
                hovertemplate=(
                    "<b>%{x}</b><br>YOY RevPAR: %{y:+.1f}%<br>"
                    "<i>Use 'Board Talking Points' for AI narrative</i><extra></extra>"
                ),
            ))
            fig.update_layout(yaxis_ticksuffix="%", showlegend=False)
            st.plotly_chart(style_fig(fig, height=300), use_container_width=True)

        st.markdown("---")
        c1, c2 = st.columns(2)

        with c1:
            st.markdown('<div class="chart-header">Seasonality Index</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Monthly RevPAR ÷ period average &nbsp;·&nbsp; above 1.0 = peak season</div>', unsafe_allow_html=True)
            if len(monthly) >= 6:
                ttm_avg = monthly["revpar"].mean()
                monthly["season_idx"] = monthly["revpar"] / ttm_avg
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=monthly["month_label"], y=monthly["season_idx"],
                    fill="tozeroy",
                    line=dict(color=TEAL, width=2),
                    fillcolor="rgba(33,128,141,0.10)",
                    mode="lines+markers",
                    marker=dict(size=5, color=TEAL),
                    hovertemplate="<b>%{x}</b><br>Index: %{y:.2f}<extra></extra>",
                ))
                fig.add_hline(y=1.0, line_dash="dot", line_color="rgba(167,169,169,0.5)",
                              annotation_text="Baseline 1.0", annotation_position="right")
                fig.update_layout(yaxis_range=[0.5, 1.65], showlegend=False)
                st.plotly_chart(style_fig(fig), use_container_width=True)
            else:
                st.markdown(empty_state(
                    "📊", "Need 6+ months of data.",
                    "Load more history to compute the seasonality index.",
                ), unsafe_allow_html=True)

        with c2:
            st.markdown('<div class="chart-header">TBID Revenue Estimate</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Monthly at blended 1.25% &nbsp;·&nbsp; hover to compare periods</div>', unsafe_allow_html=True)
            if not df_active.empty:
                tmp = df_active.copy()
                tmp["month"] = tmp["as_of_date"].dt.to_period("M")
                mrev = tmp.groupby("month")["revenue"].sum().reset_index()
                mrev["month_label"] = mrev["month"].dt.strftime("%b %Y")
                mrev["tbid_m"] = mrev["revenue"] * 0.0125 / 1e6
                fig = go.Figure(go.Bar(
                    x=mrev["month_label"], y=mrev["tbid_m"],
                    marker=dict(color=TEAL, line_width=0, cornerradius=5),
                    hovertemplate="<b>%{x}</b><br>Est. TBID: $%{y:.2f}M<extra></extra>",
                ))
                fig.update_layout(yaxis_tickprefix="$", yaxis_ticksuffix="M", showlegend=False)
                st.plotly_chart(style_fig(fig), use_container_width=True)
            else:
                st.markdown(empty_state(
                    "💰", "No revenue data loaded.",
                    "Run the pipeline to populate TBID estimates.",
                ), unsafe_allow_html=True)

        st.markdown("---")

        # ── Compression quarters ───────────────────────────────────────────────
        st.markdown('<div class="chart-header">Occupancy Compression Days by Quarter</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">Days with occ ≥ 80% and ≥ 90% &nbsp;·&nbsp; signals pricing power windows</div>', unsafe_allow_html=True)
        if not df_comp.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name="Days ≥ 80% Occ",
                x=df_comp["quarter"], y=df_comp["days_above_80_occ"],
                marker=dict(color=TEAL, line_width=0, cornerradius=5),
                hovertemplate="<b>%{x}</b><br>Days ≥ 80%%: %{y}<extra></extra>",
            ))
            fig.add_trace(go.Bar(
                name="Days ≥ 90% Occ",
                x=df_comp["quarter"], y=df_comp["days_above_90_occ"],
                marker=dict(color=TEAL_LIGHT, line_width=0, cornerradius=5),
                hovertemplate="<b>%{x}</b><br>Days ≥ 90%%: %{y}<br>"
                              "<i>High compression — rate increases justified</i><extra></extra>",
            ))
            fig.update_layout(barmode="group")
            st.plotly_chart(style_fig(fig, height=260), use_container_width=True)
        else:
            st.markdown(empty_state(
                "📦", "No compression data.",
                "Run compute_kpis.py to populate kpi_compression_quarterly.",
            ), unsafe_allow_html=True)

        st.markdown("---")

        # ── Full history line chart ─────────────────────────────────────────────
        st.markdown('<div class="chart-header">RevPAR / ADR / Occupancy — Full History</div>', unsafe_allow_html=True)
        src_label = "monthly STR exports" if grain == "Monthly" else "kpi_daily_summary"
        st.markdown(f'<div class="chart-caption">Monthly averages &nbsp;·&nbsp; all available data &nbsp;·&nbsp; source: {src_label}</div>', unsafe_allow_html=True)
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(
            x=monthly["month_label"], y=monthly["revpar"],
            name="RevPAR $", line=dict(color=TEAL, width=2), mode="lines",
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=monthly["month_label"], y=monthly["adr"],
            name="ADR $", line=dict(color=ORANGE, width=2, dash="dot"), mode="lines",
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=monthly["month_label"], y=monthly["occ_pct"],
            name="Occ %", line=dict(color="rgba(167,169,169,0.7)", width=1.5), mode="lines",
        ), secondary_y=True)
        fig.update_yaxes(title_text="RevPAR / ADR ($)", tickprefix="$", secondary_y=False)
        fig.update_yaxes(title_text="Occ %", ticksuffix="%",
                         secondary_y=True, showgrid=False)
        st.plotly_chart(style_fig(fig, height=300), use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — EVENT IMPACT / VISITOR ECONOMY
# ══════════════════════════════════════════════════════════════════════════════
with tab_ev:
    _ev_kpis  = load_datafy_kpis()
    _ev_dma   = load_datafy_dma()
    _ev_spend = load_datafy_spending()
    _ev_demo  = load_datafy_demographics()
    _ev_air   = load_datafy_airports()
    _ev_mk    = load_datafy_media_kpis()

    # Build header from actual data
    _ev_period = "2025"
    if not _ev_kpis.empty:
        _k = _ev_kpis.iloc[0]
        _ev_period = (f"{str(_k.get('report_period_start','2025-01-01'))[:7].replace('-','/')} – "
                      f"{str(_k.get('report_period_end','2025-12-31'))[:7].replace('-','/')}")

    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        'Visitor Economy Intelligence — Dana Point</div>'
        f'<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        f'Source: Datafy Geolocation (Caladan 1.2) &nbsp;·&nbsp; Live data from analytics.sqlite'
        f' &nbsp;·&nbsp; Period: {_ev_period}</div>',
        unsafe_allow_html=True,
    )

    if _ev_kpis.empty:
        st.markdown(empty_state("📊", "Datafy data not loaded.",
                    "Run load_datafy_reports.py to populate visitor economy tables."),
                    unsafe_allow_html=True)
    else:
        _kv = _ev_kpis.iloc[0]

        # ── Hero stats from actual Datafy KPI table ──────────────────────────
        _total_trips    = int(_kv.get("total_trips", 0))
        _overnight_pct  = _kv.get("overnight_trips_pct", 0)
        _oos_pct        = _kv.get("out_of_state_vd_pct", 0)
        _repeat_pct     = _kv.get("repeat_visitors_pct", 0)
        _avg_los        = _kv.get("avg_length_of_stay_days", 0)
        _visitor_spend  = _kv.get("visitor_spending_pct", 0)

        hero_stats = [
            (f"{_total_trips/1e6:.2f}M", "Total Trips to Dana Point",
             "globe", f"{_kv.get('total_trips_vs_compare_pct',0):+.1f}% vs prior year"),
            (f"{_overnight_pct:.1f}%", "Overnight Trips",
             "bed", f"{_kv.get('overnight_vs_compare_pct',0):+.1f}pp vs prior year"),
            (f"{_oos_pct:.1f}%", "Out-of-State Visitor Days",
             "plane", f"{_kv.get('out_of_state_vd_vs_compare_pct',0):+.1f}pp vs prior year"),
            (f"{_avg_los:.1f} days", "Avg Length of Stay",
             "chart_up", f"{_kv.get('avg_los_vs_compare_days',0):+.1f}d vs prior year"),
            (f"{_repeat_pct:.1f}%", "Repeat Visitors",
             "tag", "Brand loyalty indicator"),
            (f"{_visitor_spend:.1f}%", "Visitor Spending Share",
             "money", f"vs {_kv.get('local_spending_pct',0):.1f}% local spend"),
        ]
        ec = st.columns(3)
        for i, (val, lbl, ico, dt) in enumerate(hero_stats):
            with ec[i % 3]:
                st.markdown(event_stat(val, lbl, ico, dt), unsafe_allow_html=True)

        st.markdown("---")

        # ── ADR & Occupancy seasonal pattern from actual STR daily data ───────
        st.markdown('<div class="chart-header">Daily ADR & Occupancy — Seasonal Pattern (STR Daily)</div>',
                    unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">Actual STR daily data · peaks reveal event-driven compression · '
                    'hover for exact values</div>', unsafe_allow_html=True)

        if not df_daily.empty:
            # Use last 365 days of daily data
            _ev_daily = df_daily.copy().sort_values("as_of_date")
            _ev_cutoff = _ev_daily["as_of_date"].max() - pd.Timedelta(days=365)
            _ev_daily = _ev_daily[_ev_daily["as_of_date"] >= _ev_cutoff]

            if not _ev_daily.empty and "adr" in _ev_daily.columns and "occupancy" in _ev_daily.columns:
                _ev_x = _ev_daily["as_of_date"].dt.strftime("%b %d, %Y")
                fig_ev = make_subplots(specs=[[{"secondary_y": True}]])
                fig_ev.add_trace(go.Scatter(
                    x=_ev_x, y=_ev_daily["occupancy"],
                    name="Occupancy %", mode="lines",
                    line=dict(color=TEAL, width=1.8),
                    hovertemplate="<b>%{x}</b><br>Occ: %{y:.1f}%<extra></extra>",
                ), secondary_y=False)
                fig_ev.add_trace(go.Scatter(
                    x=_ev_x, y=_ev_daily["adr"],
                    name="ADR $", mode="lines",
                    line=dict(color=ORANGE, width=1.8),
                    hovertemplate="<b>%{x}</b><br>ADR: $%{y:.0f}<extra></extra>",
                ), secondary_y=True)
                # Mark high-compression days (>80% occ) as red dots
                _high_occ = _ev_daily[_ev_daily["occupancy"] > 80]
                if not _high_occ.empty:
                    fig_ev.add_trace(go.Scatter(
                        x=_high_occ["as_of_date"].dt.strftime("%b %d, %Y"),
                        y=_high_occ["occupancy"],
                        name="Compression (>80%)", mode="markers",
                        marker=dict(color=RED, size=6, symbol="circle"),
                        hovertemplate="<b>%{x}</b><br>Compression: %{y:.1f}% occ<extra></extra>",
                    ), secondary_y=False)
                fig_ev.update_yaxes(title_text="Occupancy (%)", secondary_y=False)
                fig_ev.update_yaxes(title_text="ADR ($)", tickprefix="$",
                                    secondary_y=True, showgrid=False)
                st.plotly_chart(style_fig(fig_ev, height=320), use_container_width=True)
                n_comp = len(_high_occ)
                st.caption(f"Red dots = {n_comp} compression days (>80% occupancy) in last 365 days · "
                           f"peaks indicate event-driven demand or holiday weekends")
        else:
            st.info("Load STR daily data to see the seasonal ADR/Occupancy pattern.")

        st.markdown("---")
        c1, c2 = st.columns(2)

        with c1:
            # Top Feeder Markets from actual Datafy DMA data
            st.markdown('<div class="chart-header">Top Visitor Origin Markets</div>',
                        unsafe_allow_html=True)
            st.markdown(f'<div class="chart-caption">Share of visitor days · Datafy {_ev_period} · '
                        f'green = growing YoY</div>', unsafe_allow_html=True)
            if not _ev_dma.empty:
                _dma_s = _ev_dma.sort_values("visitor_days_share_pct", ascending=True).tail(12)
                _dma_colors = [
                    "rgba(33,128,141,0.90)" if v >= 0 else "rgba(192,21,47,0.80)"
                    for v in _dma_s["visitor_days_vs_compare_pct"].fillna(0)
                ]
                fig_dma = go.Figure(go.Bar(
                    x=_dma_s["visitor_days_share_pct"],
                    y=_dma_s["dma"],
                    orientation="h",
                    marker=dict(color=_dma_colors, line_width=0),
                    text=[f"{v:.1f}%" for v in _dma_s["visitor_days_share_pct"]],
                    textposition="outside",
                    customdata=_dma_s["visitor_days_vs_compare_pct"].fillna(0),
                    hovertemplate="<b>%{y}</b><br>%{x:.1f}% visitor days<br>"
                                  "YoY: %{customdata:+.1f}pp<extra></extra>",
                ))
                fig_dma.update_layout(xaxis_ticksuffix="%", showlegend=False,
                                      margin=dict(l=0, r=60, t=4, b=0))
                st.plotly_chart(style_fig(fig_dma, height=360), use_container_width=True)
            else:
                st.info("Load Datafy data to see feeder market breakdown.")

        with c2:
            # Spending Categories from actual Datafy spend data
            st.markdown('<div class="chart-header">Visitor Spending by Category</div>',
                        unsafe_allow_html=True)
            st.markdown(f'<div class="chart-caption">Share of total visitor spend · Datafy {_ev_period}</div>',
                        unsafe_allow_html=True)
            if not _ev_spend.empty:
                _palette = [TEAL, "#2DA6B2", TEAL_LIGHT, ORANGE, "#A84B2F",
                            "#5E5240", "#626C71", "#A7A9A9", RED, "#3B82F6"]
                fig_sp = go.Figure(go.Pie(
                    labels=_ev_spend["category"],
                    values=_ev_spend["spend_share_pct"],
                    hole=0.48,
                    marker=dict(
                        colors=_palette[:len(_ev_spend)],
                        line=dict(color="rgba(0,0,0,0)", width=0),
                    ),
                    textfont=dict(size=11, family="Plus Jakarta Sans, Inter, sans-serif"),
                    hovertemplate="<b>%{label}</b><br>%{value:.1f}% of visitor spend<extra></extra>",
                ))
                fig_sp.update_layout(
                    legend=dict(font_size=10, orientation="v",
                                font=dict(family="Plus Jakarta Sans, Inter, sans-serif")),
                    annotations=[dict(text="Spend<br>Mix", x=0.5, y=0.5, font_size=13,
                                      font_family="Plus Jakarta Sans, sans-serif",
                                      font_color="#21808D", showarrow=False)],
                )
                st.plotly_chart(style_fig(fig_sp, height=360), use_container_width=True)
            else:
                st.info("Load Datafy data to see spending categories.")

        # ── Row 2: Demographics + Airports ────────────────────────────────────
        st.markdown("---")
        c3, c4 = st.columns(2)

        with c3:
            st.markdown('<div class="chart-header">Visitor Age Profile</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="chart-caption">Age demographics · Datafy {_ev_period}</div>',
                        unsafe_allow_html=True)
            if not _ev_demo.empty:
                _age_df = _ev_demo[_ev_demo["dimension"] == "age"].sort_values("segment")
                if not _age_df.empty:
                    _max_age = _age_df["share_pct"].max()
                    _age_colors = [
                        f"rgba(33,{int(128 + 56*(v/_max_age))},{int(141 + 57*(v/_max_age))},0.90)"
                        for v in _age_df["share_pct"]
                    ]
                    fig_age = go.Figure(go.Bar(
                        x=_age_df["segment"], y=_age_df["share_pct"],
                        marker=dict(color=_age_colors, line_width=0),
                        text=[f"{v:.1f}%" for v in _age_df["share_pct"]],
                        textposition="outside",
                        hovertemplate="Age %{x}: %{y:.1f}%<extra></extra>",
                    ))
                    fig_age.update_layout(yaxis_title="Share (%)", margin=dict(l=0, r=0, t=4, b=0))
                    st.plotly_chart(style_fig(fig_age, height=280), use_container_width=True)

        with c4:
            st.markdown('<div class="chart-header">Fly-In Visitors — Origin Airports</div>',
                        unsafe_allow_html=True)
            st.markdown(f'<div class="chart-caption">Share of inbound air passengers · Datafy {_ev_period}</div>',
                        unsafe_allow_html=True)
            if not _ev_air.empty:
                _air_s = _ev_air.sort_values("passengers_share_pct", ascending=True)
                fig_air = go.Figure(go.Bar(
                    y=_air_s["airport_code"], x=_air_s["passengers_share_pct"],
                    orientation="h",
                    marker=dict(color=TEAL, line_width=0),
                    text=[f"{v:.1f}%" for v in _air_s["passengers_share_pct"]],
                    textposition="outside",
                    customdata=_air_s["airport_name"],
                    hovertemplate="%{customdata}<br>%{x:.1f}%<extra></extra>",
                ))
                fig_air.update_layout(xaxis_ticksuffix="%", margin=dict(l=0, r=60, t=4, b=0))
                st.plotly_chart(style_fig(fig_air, height=280), use_container_width=True)

        # ── Media Campaign KPIs from actual attribution data ───────────────────
        if not _ev_mk.empty:
            st.markdown("---")
            _mk = _ev_mk.iloc[0]
            st.markdown('<div class="chart-header">Media Campaign Attribution</div>',
                        unsafe_allow_html=True)
            st.markdown(
                f'<div class="chart-caption">{_mk.get("campaign_name","2025-26 Campaign")} · '
                f'Datafy Attribution · {_mk.get("report_period_start","")[:10]} – '
                f'{_mk.get("report_period_end","")[:10]}</div>',
                unsafe_allow_html=True,
            )
            _mk_cols = st.columns(4)
            with _mk_cols[0]:
                st.metric("Total Impressions", f"{int(_mk.get('total_impressions',0))/1e6:.1f}M")
            with _mk_cols[1]:
                st.metric("Unique Reach", f"{int(_mk.get('unique_reach',0))/1e6:.2f}M")
            with _mk_cols[2]:
                st.metric("Attributable Trips", f"{int(_mk.get('attributable_trips',0)):,}")
            with _mk_cols[3]:
                st.metric("Economic Impact", f"${_mk.get('total_impact_usd',0)/1e3:.0f}K")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — MARKET INTELLIGENCE (CoStar)
# ══════════════════════════════════════════════════════════════════════════════
with tab_cs:
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">South OC Market Intelligence</div>
      <div class="hero-subtitle">CoStar Hospitality Analytics · South Orange County, CA · 2023–2024</div>
    </div>
    """, unsafe_allow_html=True)

    # ── AI CoStar Analysis Panel ───────────────────────────────────────────────
    with st.expander("🧠 CoStar AI Analyst — Deep Market Insights", expanded=True):
        st.markdown('<span class="ai-chip">MARKET INTELLIGENCE</span>', unsafe_allow_html=True)

        COSTAR_PROMPTS = [
            ("🏨 Market vs. Portfolio",  "cs_mkt_vs_portfolio"),
            ("📈 Rate Positioning",      "cs_rate_position"),
            ("🏗️ Supply Impact",         "cs_supply_impact"),
            ("🎯 Segment Strategy",      "cs_segment"),
            ("💰 Revenue Opportunity",   "cs_revenue_opp"),
            ("📊 Competitive Ranking",   "cs_comp_rank"),
        ]

        def build_costar_prompt(key: str, m: dict) -> str:
            """Build AI prompts enriched with CoStar market context."""
            cs_ctx = ""
            if not df_cs_snap.empty:
                snap = df_cs_snap.iloc[0]
                cs_ctx = (
                    f"\nCoStar South Orange County Market (2024 Full Year):\n"
                    f"• Market occupancy: {snap.get('occupancy_pct', 0):.1f}% "
                    f"({snap.get('occ_yoy_pp', 0):+.1f}pp YOY)\n"
                    f"• Market ADR: ${snap.get('adr_usd', 0):.2f} "
                    f"({snap.get('adr_yoy_pct', 0):+.1f}% YOY)\n"
                    f"• Market RevPAR: ${snap.get('revpar_usd', 0):.2f} "
                    f"({snap.get('revpar_yoy_pct', 0):+.1f}% YOY)\n"
                    f"• Total market supply: {snap.get('total_supply_rooms', 0):,} rooms\n"
                    f"• Annual room revenue: ${snap.get('room_revenue_usd', 0)/1e6:.1f}M\n"
                )
            if not df_cs_chain.empty:
                chain_2024 = df_cs_chain[df_cs_chain["year"] == "2024"]
                if not chain_2024.empty:
                    luxury = chain_2024[chain_2024["chain_scale"] == "Luxury"]
                    upupscale = chain_2024[chain_2024["chain_scale"] == "Upper Upscale"]
                    if not luxury.empty:
                        cs_ctx += f"• Luxury segment ADR: ${luxury.iloc[0]['adr_usd']:.0f} · Occ: {luxury.iloc[0]['occupancy_pct']:.1f}%\n"
                    if not upupscale.empty:
                        cs_ctx += f"• Upper Upscale ADR: ${upupscale.iloc[0]['adr_usd']:.0f} · Occ: {upupscale.iloc[0]['occupancy_pct']:.1f}%\n"
            pipeline_rooms = df_cs_pipe["rooms"].sum() if not df_cs_pipe.empty else 0
            cs_ctx += f"• Active pipeline: {pipeline_rooms:,} rooms across {len(df_cs_pipe)} projects\n"

            base = _base(m) if m else "No STR portfolio data loaded."
            prompts = {
                "cs_mkt_vs_portfolio": (
                    f"{base}\n{cs_ctx}\n"
                    "Compare the VDP Select Portfolio performance (76.4% occ, $288.50 ADR, "
                    "$220.42 RevPAR) to the broader South OC market. Identify where the portfolio "
                    "is outperforming or underperforming the market, and provide 3 specific "
                    "recommendations to improve market share index (MPI/ARI/RGI)."
                ),
                "cs_rate_position": (
                    f"{base}\n{cs_ctx}\n"
                    "The Waldorf Astoria Monarch Beach commands $850 ADR at 71% occupancy. "
                    "The VDP portfolio blended ADR is $288.50. Analyze the rate positioning "
                    "gap across chain scales (Luxury at $782, Upper Upscale at $298, Upscale "
                    "at $199) and recommend a rate ladder strategy for the VDP portfolio to "
                    "maximize its ARI (Average Rate Index) relative to the market."
                ),
                "cs_supply_impact": (
                    f"{base}\n{cs_ctx}\n"
                    f"There are {pipeline_rooms:,} new hotel rooms in the active supply pipeline "
                    "for South OC (Dana Cove Hotel 136 rooms Q3 2025, Strands Beach Hotel 88 "
                    "rooms Q1 2026, Monarch Beach Residences 48 rooms Q2 2026, plus 3 others). "
                    "Analyze the competitive impact of this new supply on VDP portfolio occupancy "
                    "and ADR. Which segments face the most pressure? What pre-emptive strategy "
                    "should VDP recommend to member hotels?"
                ),
                "cs_segment": (
                    f"{base}\n{cs_ctx}\n"
                    "The chain scale analysis shows Luxury at $782 ADR (71% occ), Upper Upscale "
                    "at $298 (77.4% occ), and Upscale at $199 (79.1% occ). Independent hotels "
                    "achieve $296 ADR at 73.6% occ — nearly matching Upper Upscale rates with "
                    "smaller inventory. Identify the most underserved segment in Dana Point's "
                    "market and the highest-margin positioning opportunity for VDP member hotels."
                ),
                "cs_revenue_opp": (
                    f"{base}\n{cs_ctx}\n"
                    "South OC generated $1.15B in room revenue in 2024. VDP portfolio "
                    "contributes approximately $220M of that at current pace. Quantify the "
                    "revenue gap between VDP portfolio performance and market-leading properties. "
                    "Model the incremental room revenue if VDP portfolio achieved market-average "
                    "ADR and occupancy. Express in annual and TBID-revenue terms."
                ),
                "cs_comp_rank": (
                    f"{base}\n{cs_ctx}\n"
                    "The competitive set ranking shows: Waldorf Astoria (RGI 273.9), "
                    "Ritz-Carlton (RGI 231.2), Pacific Edge Boutique (RGI 131.4), "
                    "Laguna Cliffs Marriott (RGI 105.1), VDP Portfolio baseline (RGI 100.0). "
                    "Analyze the MPI, ARI, and RGI spread across the comp set. Which "
                    "properties represent the strongest competitive threat to the VDP portfolio? "
                    "Which represent a model VDP members should benchmark against?"
                ),
            }
            return prompts.get(key, f"{base}\n{cs_ctx}\nProvide market intelligence insights.")

        btn_cols_cs = st.columns(3)
        for i, (label, key) in enumerate(COSTAR_PROMPTS):
            with btn_cols_cs[i % 3]:
                if st.button(label, key=f"cs_btn_{key}", use_container_width=True):
                    st.session_state.ai_current_prompt = build_costar_prompt(key, m)
                    st.session_state.ai_prompt_label   = label
                    st.session_state.ai_needs_call     = True

        if st.session_state.get("ai_needs_call") and st.session_state.get("ai_current_prompt"):
            st.session_state.ai_needs_call = False
            label_disp = st.session_state.get("ai_prompt_label", "Analysis")
            st.markdown(f"**{label_disp}**")
            if api_key_valid:
                with st.spinner("Analyzing market data…"):
                    st.write_stream(stream_claude_response(st.session_state.ai_current_prompt, api_key))
            else:
                st.info(local_fallback("board", m) if m else "No data. Run the pipeline first.")

    st.markdown("---")

    # ── Market Overview KPI Cards ──────────────────────────────────────────────
    st.markdown("### South OC Market Overview (2024)")

    if not df_cs_snap.empty:
        snap = df_cs_snap[df_cs_snap["report_period"] == "2024-12-31"]
        if snap.empty:
            snap = df_cs_snap.iloc[[0]]
        snap = snap.iloc[0]

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(kpi_card(
                "Market Occupancy", f"{snap['occupancy_pct']:.1f}%",
                f"{snap['occ_yoy_pp']:+.1f}pp YOY",
                positive=(snap['occ_yoy_pp'] >= 0),
            ), unsafe_allow_html=True)
        with c2:
            st.markdown(kpi_card(
                "Market ADR", f"${snap['adr_usd']:.2f}",
                f"{snap['adr_yoy_pct']:+.1f}% YOY",
                positive=(snap['adr_yoy_pct'] >= 0),
            ), unsafe_allow_html=True)
        with c3:
            st.markdown(kpi_card(
                "Market RevPAR", f"${snap['revpar_usd']:.2f}",
                f"{snap['revpar_yoy_pct']:+.1f}% YOY",
                positive=(snap['revpar_yoy_pct'] >= 0),
            ), unsafe_allow_html=True)
        with c4:
            rev_b = snap['room_revenue_usd'] / 1e9
            st.markdown(kpi_card(
                "Annual Room Revenue", f"${rev_b:.2f}B",
                f"{snap['demand_yoy_pct']:+.1f}% demand YOY",
                positive=(snap['demand_yoy_pct'] >= 0),
            ), unsafe_allow_html=True)

        # VDP vs Market comparison row
        st.markdown("#### VDP Portfolio vs. South OC Market (2024)")
        col_a, col_b, col_c = st.columns(3)
        vdp_occ, mkt_occ = 76.4, float(snap['occupancy_pct'])
        vdp_adr, mkt_adr = 288.50, float(snap['adr_usd'])
        vdp_rvp, mkt_rvp = 220.42, float(snap['revpar_usd'])

        with col_a:
            diff = vdp_occ - mkt_occ
            st.markdown(kpi_card(
                "Occ: Portfolio vs. Market",
                f"{vdp_occ:.1f}% / {mkt_occ:.1f}%",
                f"MPI: {(vdp_occ/mkt_occ*100):.1f} · {diff:+.1f}pp gap",
                positive=(diff >= 0),
            ), unsafe_allow_html=True)
        with col_b:
            diff = ((vdp_adr - mkt_adr) / mkt_adr * 100)
            st.markdown(kpi_card(
                "ADR: Portfolio vs. Market",
                f"${vdp_adr:.0f} / ${mkt_adr:.0f}",
                f"ARI: {(vdp_adr/mkt_adr*100):.1f} · {diff:+.1f}%",
                positive=(diff >= 0),
            ), unsafe_allow_html=True)
        with col_c:
            diff = ((vdp_rvp - mkt_rvp) / mkt_rvp * 100)
            st.markdown(kpi_card(
                "RevPAR: Portfolio vs. Market",
                f"${vdp_rvp:.0f} / ${mkt_rvp:.0f}",
                f"RGI: {(vdp_rvp/mkt_rvp*100):.1f} · {diff:+.1f}%",
                positive=(diff >= 0),
            ), unsafe_allow_html=True)
    else:
        st.markdown(empty_state("📊", "No CoStar snapshot data.",
            "Run scripts/load_costar_reports.py to populate market data."),
            unsafe_allow_html=True)

    st.markdown("---")

    # ── Monthly Performance Trend ──────────────────────────────────────────────
    st.markdown("### Market Monthly Performance — 24-Month Trend")

    if not df_cs_mon.empty:
        # Filter last 24 months
        cs_plot = df_cs_mon.tail(24).copy()

        col_left, col_right = st.columns(2)
        with col_left:
            st.markdown('<div class="chart-header">Market Occupancy & ADR Trend</div>',
                        unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">South OC market — monthly CoStar data</div>',
                        unsafe_allow_html=True)
            fig_cs1 = make_subplots(specs=[[{"secondary_y": True}]])
            fig_cs1.add_trace(go.Scatter(
                x=cs_plot["as_of_date"], y=cs_plot["occupancy_pct"],
                name="Market Occ %", line=dict(color=TEAL, width=2.5),
                hovertemplate="<b>%{x|%b %Y}</b><br>Occ: %{y:.1f}%<extra></extra>",
            ), secondary_y=False)
            fig_cs1.add_trace(go.Bar(
                x=cs_plot["as_of_date"], y=cs_plot["adr_usd"],
                name="Market ADR $", marker_color=f"rgba(230,129,97,0.45)",
                hovertemplate="<b>%{x|%b %Y}</b><br>ADR: $%{y:.0f}<extra></extra>",
            ), secondary_y=True)
            fig_cs1.update_yaxes(title_text="Occupancy %", secondary_y=False,
                                  tickformat=".0f", ticksuffix="%")
            fig_cs1.update_yaxes(title_text="ADR $", secondary_y=True,
                                  tickformat="$,.0f")
            st.plotly_chart(style_fig(fig_cs1, height=300), use_container_width=True)

        with col_right:
            st.markdown('<div class="chart-header">Market RevPAR Trend</div>',
                        unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">South OC monthly RevPAR with YOY change overlay</div>',
                        unsafe_allow_html=True)
            cs_yoy = cs_plot.dropna(subset=["revpar_yoy_pct"])
            fig_cs2 = make_subplots(specs=[[{"secondary_y": True}]])
            fig_cs2.add_trace(go.Scatter(
                x=cs_plot["as_of_date"], y=cs_plot["revpar_usd"],
                name="RevPAR $", fill="tozeroy",
                fillcolor=f"rgba(33,128,141,0.12)",
                line=dict(color=TEAL, width=2.5),
                hovertemplate="<b>%{x|%b %Y}</b><br>RevPAR: $%{y:.0f}<extra></extra>",
            ), secondary_y=False)
            if not cs_yoy.empty:
                pos_mask = cs_yoy["revpar_yoy_pct"] >= 0
                fig_cs2.add_trace(go.Bar(
                    x=cs_yoy.loc[pos_mask, "as_of_date"],
                    y=cs_yoy.loc[pos_mask, "revpar_yoy_pct"],
                    name="YOY % (pos)", marker_color="rgba(33,128,141,0.55)",
                    hovertemplate="<b>%{x|%b %Y}</b><br>YOY: %{y:+.1f}%<extra></extra>",
                ), secondary_y=True)
                fig_cs2.add_trace(go.Bar(
                    x=cs_yoy.loc[~pos_mask, "as_of_date"],
                    y=cs_yoy.loc[~pos_mask, "revpar_yoy_pct"],
                    name="YOY % (neg)", marker_color="rgba(192,21,47,0.55)",
                    hovertemplate="<b>%{x|%b %Y}</b><br>YOY: %{y:+.1f}%<extra></extra>",
                ), secondary_y=True)
            fig_cs2.update_yaxes(title_text="RevPAR $", secondary_y=False, tickformat="$,.0f")
            fig_cs2.update_yaxes(title_text="YOY %", secondary_y=True, ticksuffix="%")
            st.plotly_chart(style_fig(fig_cs2, height=300), use_container_width=True)

        # Seasonality insight
        if len(cs_plot) >= 12:
            peak_row = cs_plot.loc[cs_plot["revpar_usd"].idxmax()]
            trough_row = cs_plot.loc[cs_plot["revpar_usd"].idxmin()]
            peak_mo = peak_row["as_of_date"].strftime("%b %Y")
            trough_mo = trough_row["as_of_date"].strftime("%b %Y")
            seasonal_range = peak_row["revpar_usd"] - trough_row["revpar_usd"]
            st.markdown(
                insight_card(
                    f"Market Seasonality: {seasonal_range:.0f} RevPAR swing",
                    f"Peak: **{peak_mo}** at ${peak_row['revpar_usd']:.0f} RevPAR · "
                    f"Trough: **{trough_mo}** at ${trough_row['revpar_usd']:.0f} RevPAR. "
                    f"The {seasonal_range:.0f} RevPAR spread between peak and trough months "
                    f"underscores the critical importance of aggressive summer rate positioning "
                    f"and shoulder-season demand generation to smooth revenue over the full year.",
                    kind="positive",
                ),
                unsafe_allow_html=True,
            )
    else:
        st.markdown(empty_state("📈", "No monthly CoStar data.",
            "Run scripts/load_costar_reports.py to populate trend data."),
            unsafe_allow_html=True)

    st.markdown("---")

    # ── Chain Scale Breakdown ──────────────────────────────────────────────────
    st.markdown("### Chain Scale Performance Breakdown (2024)")

    if not df_cs_chain.empty:
        chain_2024 = df_cs_chain[df_cs_chain["year"] == "2024"].sort_values(
            "revpar_usd", ascending=False
        )

        col_ch1, col_ch2 = st.columns(2)
        with col_ch1:
            st.markdown('<div class="chart-header">RevPAR by Chain Scale</div>',
                        unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">South OC market 2024 · Full-year average</div>',
                        unsafe_allow_html=True)
            colors_chain = [TEAL if cs == "Upper Upscale" else
                           ("#21808D" if cs == "Luxury" else TEAL_LIGHT)
                           for cs in chain_2024["chain_scale"]]
            fig_ch1 = go.Figure(go.Bar(
                x=chain_2024["chain_scale"],
                y=chain_2024["revpar_usd"],
                marker_color=[TEAL, TEAL_LIGHT, "#4EC6D3", ORANGE, "#E68161", "#A84B2F"],
                text=[f"${v:.0f}" for v in chain_2024["revpar_usd"]],
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>RevPAR: $%{y:.0f}<extra></extra>",
            ))
            st.plotly_chart(style_fig(fig_ch1, height=280), use_container_width=True)

        with col_ch2:
            st.markdown('<div class="chart-header">Market Share by Chain Scale (RevPAR)</div>',
                        unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">% of total market RevPAR contribution</div>',
                        unsafe_allow_html=True)
            fig_ch2 = go.Figure(go.Pie(
                labels=chain_2024["chain_scale"],
                values=chain_2024["market_share_revpar_pct"],
                hole=0.46,
                marker_colors=[TEAL, TEAL_LIGHT, "#4EC6D3", ORANGE, "#E68161", "#c0152f"],
                hovertemplate="<b>%{label}</b><br>Share: %{percent}<extra></extra>",
            ))
            fig_ch2.update_layout(
                showlegend=True, margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(font=dict(size=11)),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_ch2, use_container_width=True)

        # Chain scale data table
        _chain_display = chain_2024[["chain_scale","num_properties","supply_rooms",
                                      "occupancy_pct","adr_usd","revpar_usd",
                                      "market_share_revpar_pct"]].copy()
        _chain_display.columns = ["Segment","Properties","Rooms",
                                   "Occ %","ADR $","RevPAR $","Mkt Share %"]
        _chain_display["Occ %"]       = _chain_display["Occ %"].map("{:.1f}%".format)
        _chain_display["ADR $"]       = _chain_display["ADR $"].map("${:,.2f}".format)
        _chain_display["RevPAR $"]    = _chain_display["RevPAR $"].map("${:,.2f}".format)
        _chain_display["Mkt Share %"] = _chain_display["Mkt Share %"].map("{:.1f}%".format)
        st.dataframe(_chain_display, use_container_width=True, hide_index=True)

        # Key insight
        luxury_row = chain_2024[chain_2024["chain_scale"] == "Luxury"].iloc[0]
        upup_row   = chain_2024[chain_2024["chain_scale"] == "Upper Upscale"].iloc[0]
        st.markdown(
            insight_card(
                "Luxury Dominance: 36.2% of Market RevPAR Revenue",
                f"Luxury segment (Ritz-Carlton + Waldorf Astoria) generates **36.2%** of total "
                f"market RevPAR revenue from just **3 properties** and **{luxury_row['supply_rooms']:,} rooms** — "
                f"with ${luxury_row['revpar_usd']:.0f} RevPAR vs. ${upup_row['revpar_usd']:.0f} "
                f"for Upper Upscale. The luxury ADR premium ({luxury_row['adr_usd']/upup_row['adr_usd']:.1f}x "
                f"above Upper Upscale) defines the Dana Point rate ceiling and sets aspirational "
                f"benchmarks for upper-tier VDP member hotels.",
                kind="positive",
            ),
            unsafe_allow_html=True,
        )
    else:
        st.markdown(empty_state("📊", "No chain scale data.",
            "Run scripts/load_costar_reports.py to populate segment data."),
            unsafe_allow_html=True)

    st.markdown("---")

    # ── Supply Pipeline ────────────────────────────────────────────────────────
    st.markdown("### Active Supply Pipeline")

    if not df_cs_pipe.empty:
        pipe_total = df_cs_pipe["rooms"].sum()
        under_const = df_cs_pipe[df_cs_pipe["status"] == "Under Construction"]
        planned     = df_cs_pipe[df_cs_pipe["status"].isin(["Planned", "Final Planning / Permitting"])]
        uc_rooms    = under_const["rooms"].sum() if not under_const.empty else 0
        pl_rooms    = planned["rooms"].sum() if not planned.empty else 0

        c_p1, c_p2, c_p3 = st.columns(3)
        with c_p1:
            st.markdown(kpi_card(
                "Total Pipeline Rooms", f"{pipe_total:,}",
                f"{len(df_cs_pipe)} active projects",
                positive=True, neutral=True,
            ), unsafe_allow_html=True)
        with c_p2:
            st.markdown(kpi_card(
                "Under Construction", f"{uc_rooms:,} rooms",
                f"{len(under_const)} project(s) · opening 2025",
                positive=True, neutral=True,
            ), unsafe_allow_html=True)
        with c_p3:
            st.markdown(kpi_card(
                "Planned / Permitting", f"{pl_rooms:,} rooms",
                f"{len(planned)} project(s) · opening 2026–2027",
                positive=False, neutral=True,
            ), unsafe_allow_html=True)

        # Pipeline bar chart
        st.markdown('<div class="chart-header">Pipeline Projects by Rooms & Status</div>',
                    unsafe_allow_html=True)
        status_colors = {
            "Under Construction":             TEAL,
            "Final Planning / Permitting":    ORANGE,
            "Planned":                        TEAL_LIGHT,
        }
        pipe_colors = [status_colors.get(s, "#626C71") for s in df_cs_pipe["status"]]
        fig_pipe = go.Figure(go.Bar(
            x=df_cs_pipe["property_name"],
            y=df_cs_pipe["rooms"],
            marker_color=pipe_colors,
            text=[f"{r} rooms<br>{s}" for r, s in
                  zip(df_cs_pipe["rooms"], df_cs_pipe["status"])],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Rooms: %{y}<br>Open: %{customdata}<extra></extra>",
            customdata=df_cs_pipe["projected_open_date"],
        ))
        fig_pipe.update_layout(
            xaxis_tickangle=-20, margin=dict(t=30, b=80),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(style_fig(fig_pipe, height=320), use_container_width=True)

        # Pipeline table
        _pipe_display = df_cs_pipe[["property_name","city","chain_scale","rooms",
                                     "status","projected_open_date","brand","developer"]].copy()
        _pipe_display.columns = ["Property","City","Segment","Rooms",
                                  "Status","Opens","Brand","Developer"]
        st.dataframe(_pipe_display, use_container_width=True, hide_index=True)

        # Supply impact insight
        market_supply_pct = (pipe_total / 5120 * 100)
        st.markdown(
            insight_card(
                f"Supply Warning: {pipe_total:,} Rooms ({market_supply_pct:.1f}% of Market) in Pipeline",
                f"**{uc_rooms:,} rooms** under active construction (opening 2025) will increase "
                f"South OC hotel supply by **{uc_rooms/5120*100:.1f}%** before year-end. "
                f"The full pipeline adds **{market_supply_pct:.1f}%** supply growth. "
                f"VDP member hotels should expect modest occupancy pressure in 2025–2026 as new "
                f"supply absorbs demand — making ADR discipline and loyalty programs critical "
                f"to defending RevPAR during the absorption period.",
                kind="warning",
            ),
            unsafe_allow_html=True,
        )
    else:
        st.markdown(empty_state("🏗️", "No pipeline data.",
            "Run scripts/load_costar_reports.py to populate supply pipeline."),
            unsafe_allow_html=True)

    st.markdown("---")

    # ── Competitive Set Rankings ───────────────────────────────────────────────
    st.markdown("### Competitive Set — Property Rankings (2024)")

    if not df_cs_comp.empty:
        comp_sorted = df_cs_comp.sort_values("rgi", ascending=False)

        # RGI ranking chart
        col_rk1, col_rk2 = st.columns([3, 2])
        with col_rk1:
            st.markdown('<div class="chart-header">Revenue Generation Index (RGI) by Property</div>',
                        unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">RGI > 100 = outperforming market · VDP baseline = 100.0</div>',
                        unsafe_allow_html=True)
            rgi_colors = [
                TEAL if v > 105 else (ORANGE if v < 95 else "#626C71")
                for v in comp_sorted["rgi"]
            ]
            fig_rgi = go.Figure(go.Bar(
                x=comp_sorted["rgi"],
                y=comp_sorted["property_name"],
                orientation="h",
                marker_color=rgi_colors,
                text=[f"{v:.1f}" for v in comp_sorted["rgi"]],
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>RGI: %{x:.1f}<extra></extra>",
            ))
            fig_rgi.add_vline(x=100, line_dash="dash", line_color=ORANGE,
                              annotation_text="Market Baseline", annotation_position="top")
            fig_rgi.update_layout(
                margin=dict(l=10, r=60, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.07)"),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(style_fig(fig_rgi, height=360), use_container_width=True)

        with col_rk2:
            st.markdown('<div class="chart-header">ADR vs. Occupancy Scatter</div>',
                        unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Size = rooms · color = chain scale</div>',
                        unsafe_allow_html=True)
            scale_color = {
                "Luxury": TEAL, "Upper Upscale": TEAL_LIGHT, "Upscale": ORANGE,
                "Upper Midscale": "#E68161", "Mixed": "#626C71",
            }
            fig_scat = go.Figure()
            for _, row_cs in df_cs_comp.iterrows():
                fig_scat.add_trace(go.Scatter(
                    x=[row_cs["occupancy_pct"]],
                    y=[row_cs["adr_usd"]],
                    mode="markers+text",
                    text=[row_cs["property_name"].split(" ")[0]],
                    textposition="top center",
                    marker=dict(
                        size=max(10, min(40, row_cs["rooms"] / 15)),
                        color=scale_color.get(row_cs["chain_scale"], "#626C71"),
                        opacity=0.82,
                    ),
                    name=row_cs["chain_scale"],
                    hovertemplate=(
                        f"<b>{row_cs['property_name']}</b><br>"
                        f"Occ: {row_cs['occupancy_pct']:.1f}%<br>"
                        f"ADR: ${row_cs['adr_usd']:.0f}<br>"
                        f"RevPAR: ${row_cs['revpar_usd']:.0f}<extra></extra>"
                    ),
                    showlegend=False,
                ))
            fig_scat.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(title="Occupancy %", ticksuffix="%",
                           gridcolor="rgba(255,255,255,0.07)"),
                yaxis=dict(title="ADR $", tickprefix="$",
                           gridcolor="rgba(255,255,255,0.07)"),
                margin=dict(t=10, b=30, l=10, r=10),
            )
            st.plotly_chart(style_fig(fig_scat, height=360), use_container_width=True)

        # Comp set table
        _comp_display = comp_sorted[["property_name","chain_scale","rooms",
                                      "occupancy_pct","adr_usd","revpar_usd",
                                      "mpi","ari","rgi"]].copy()
        _comp_display.columns = ["Property","Segment","Rooms","Occ %","ADR $","RevPAR $",
                                  "MPI","ARI","RGI"]
        _comp_display["Occ %"]   = _comp_display["Occ %"].map("{:.1f}%".format)
        _comp_display["ADR $"]   = _comp_display["ADR $"].map("${:,.0f}".format)
        _comp_display["RevPAR $"] = _comp_display["RevPAR $"].map("${:,.0f}".format)
        _comp_display["MPI"]     = _comp_display["MPI"].map("{:.1f}".format)
        _comp_display["ARI"]     = _comp_display["ARI"].map("{:.1f}".format)
        _comp_display["RGI"]     = _comp_display["RGI"].map("{:.1f}".format)
        st.dataframe(_comp_display, use_container_width=True, hide_index=True)
    else:
        st.markdown(empty_state("🏆", "No competitive set data.",
            "Run scripts/load_costar_reports.py to populate competitive benchmarks."),
            unsafe_allow_html=True)

    st.markdown("---")

    # ── STR × CoStar Correlation Insights ─────────────────────────────────────
    st.markdown("### Portfolio × Market Correlation Analysis")

    if not df_cs_mon.empty and not df_monthly.empty:
        # Align monthly STR and CoStar on date
        cs_merged = df_cs_mon[["as_of_date","occupancy_pct","adr_usd","revpar_usd"]].copy()
        cs_merged.columns = ["as_of_date","mkt_occ","mkt_adr","mkt_revpar"]
        str_cols = ["as_of_date","occupancy","adr","revpar"]
        str_avail = [c for c in str_cols if c in df_monthly.columns]
        if len(str_avail) >= 2:
            str_m = df_monthly[str_avail].copy()
            merged = pd.merge(str_m, cs_merged, on="as_of_date", how="inner")

            if len(merged) >= 6:
                col_corr1, col_corr2 = st.columns(2)
                with col_corr1:
                    st.markdown('<div class="chart-header">Portfolio RevPAR vs. Market RevPAR</div>',
                                unsafe_allow_html=True)
                    st.markdown('<div class="chart-caption">Correlation: VDP portfolio tracks market with ADR premium</div>',
                                unsafe_allow_html=True)
                    fig_corr = go.Figure()
                    if "revpar" in merged.columns:
                        fig_corr.add_trace(go.Scatter(
                            x=merged["as_of_date"], y=merged["revpar"],
                            name="VDP Portfolio RevPAR",
                            line=dict(color=TEAL, width=2.5),
                            hovertemplate="<b>%{x|%b %Y}</b><br>Portfolio RevPAR: $%{y:.0f}<extra></extra>",
                        ))
                    fig_corr.add_trace(go.Scatter(
                        x=merged["as_of_date"], y=merged["mkt_revpar"],
                        name="Market RevPAR",
                        line=dict(color=ORANGE, width=2, dash="dot"),
                        hovertemplate="<b>%{x|%b %Y}</b><br>Market RevPAR: $%{y:.0f}<extra></extra>",
                    ))
                    fig_corr.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(font=dict(size=11)),
                        margin=dict(t=10, b=10),
                        yaxis=dict(tickprefix="$", gridcolor="rgba(255,255,255,0.07)"),
                        xaxis=dict(gridcolor="rgba(255,255,255,0.07)"),
                    )
                    st.plotly_chart(style_fig(fig_corr, height=280), use_container_width=True)

                with col_corr2:
                    st.markdown('<div class="chart-header">Portfolio ADR vs. Market ADR</div>',
                                unsafe_allow_html=True)
                    st.markdown('<div class="chart-caption">ADR gap reveals pricing power relative to broader market</div>',
                                unsafe_allow_html=True)
                    fig_adr = go.Figure()
                    if "adr" in merged.columns:
                        fig_adr.add_trace(go.Scatter(
                            x=merged["as_of_date"], y=merged["adr"],
                            name="VDP Portfolio ADR",
                            line=dict(color=TEAL, width=2.5),
                            hovertemplate="<b>%{x|%b %Y}</b><br>Portfolio ADR: $%{y:.0f}<extra></extra>",
                        ))
                    fig_adr.add_trace(go.Scatter(
                        x=merged["as_of_date"], y=merged["mkt_adr"],
                        name="Market ADR",
                        line=dict(color=ORANGE, width=2, dash="dot"),
                        hovertemplate="<b>%{x|%b %Y}</b><br>Market ADR: $%{y:.0f}<extra></extra>",
                    ))
                    fig_adr.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(font=dict(size=11)),
                        margin=dict(t=10, b=10),
                        yaxis=dict(tickprefix="$", gridcolor="rgba(255,255,255,0.07)"),
                        xaxis=dict(gridcolor="rgba(255,255,255,0.07)"),
                    )
                    st.plotly_chart(style_fig(fig_adr, height=280), use_container_width=True)
            else:
                st.info("Need at least 6 months of overlapping STR + CoStar data for correlation analysis.")
        else:
            st.info("Run the STR pipeline first to enable portfolio vs. market correlation charts.")
    else:
        _body = ("Upload STR monthly exports to data/str/str_monthly.xlsx and run the pipeline "
                 "to compare portfolio performance against CoStar market benchmarks.")
        st.markdown(empty_state("📉", "Correlation data not available.", _body),
                    unsafe_allow_html=True)

    # CoStar data download
    st.markdown("---")
    if not df_cs_mon.empty:
        _cs_csv = df_cs_mon.to_csv(index=False).encode()
        st.download_button(
            "⬇️ Download CoStar Monthly Performance CSV",
            _cs_csv, file_name="costar_monthly_performance.csv",
            mime="text/csv", use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — VISITOR INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════
with tab_vi:
    _font = "Plus Jakarta Sans, Inter, system-ui, sans-serif"
    C1, C2, C3 = "#6366f1", "#10b981", "#f59e0b"   # indigo, emerald, amber
    C4, C5     = "#3b82f6", "#a855f7"               # blue, purple

    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        'Visitor Intelligence — Dana Point 2025</div>'
        '<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        'Source: Datafy Geolocation (Caladan 1.2) · 3.55M trips analyzed · Annual 2025</div>',
        unsafe_allow_html=True,
    )

    df_vi_kpis   = load_datafy_kpis()
    df_vi_dma    = load_datafy_dma()
    df_vi_air    = load_datafy_airports()
    df_vi_demo   = load_datafy_demographics()
    df_vi_spend  = load_datafy_spending()
    df_vi_clust  = load_datafy_clusters()

    if df_vi_kpis.empty:
        st.markdown(empty_state("👥", "Visitor data not loaded.", "Run load_datafy_reports.py to populate."), unsafe_allow_html=True)
    else:
        kv = df_vi_kpis.iloc[0]

        # ── Hero KPI row ────────────────────────────────────────────────────
        hc1, hc2, hc3, hc4, hc5, hc6 = st.columns(6)
        with hc1:
            st.metric("Total Trips", f"{kv['total_trips']/1e6:.2f}M",
                      f"{kv.get('total_trips_vs_compare_pct', 0):+.1f}% YoY")
        with hc2:
            st.metric("Avg Stay", f"{kv.get('avg_length_of_stay_days', 0):.1f} days",
                      f"{kv.get('avg_los_vs_compare_days', 0):+.1f} d vs prior")
        with hc3:
            st.metric("Overnight Share", f"{kv.get('overnight_trips_pct', 0):.1f}%",
                      f"{kv.get('overnight_vs_compare_pct', 0):+.1f}pp YoY")
        with hc4:
            st.metric("Out-of-State", f"{kv.get('out_of_state_vd_pct', 0):.1f}%",
                      f"{kv.get('out_of_state_vd_vs_compare_pct', 0):+.1f}pp YoY")
        with hc5:
            st.metric("Repeat Visitors", f"{kv.get('repeat_visitors_pct', 0):.1f}%")
        with hc6:
            st.metric("Visitor Spending Share", f"{kv.get('visitor_spending_pct', 0):.1f}%",
                      f"{kv.get('visitors_vs_compare_pct', 0):+.1f}pp YoY")

        st.divider()

        # ── Row A: Trip-type, Origin, Loyalty donuts ───────────────────────
        ra1, ra2, ra3 = st.columns(3)

        with ra1:
            fig = go.Figure(go.Pie(
                labels=["Day Trips", "Overnight"],
                values=[kv.get('day_trips_pct', 0), kv.get('overnight_trips_pct', 0)],
                hole=0.62, marker_colors=[C3, C1],
                textinfo="label+percent", textfont_size=12,
            ))
            style_fig(fig, 260)
            fig.update_layout(title_text="Trip Type", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with ra2:
            fig = go.Figure(go.Pie(
                labels=["In-State CA", "Out-of-State"],
                values=[kv.get('in_state_visitor_days_pct', 0), kv.get('out_of_state_vd_pct', 0)],
                hole=0.62, marker_colors=[C2, C4],
                textinfo="label+percent", textfont_size=12,
            ))
            style_fig(fig, 260)
            fig.update_layout(title_text="Visitor Origin", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with ra3:
            fig = go.Figure(go.Pie(
                labels=["First-Time", "Repeat"],
                values=[kv.get('one_time_visitors_pct', 0), kv.get('repeat_visitors_pct', 0)],
                hole=0.62, marker_colors=[C4, C5],
                textinfo="label+percent", textfont_size=12,
            ))
            style_fig(fig, 260)
            fig.update_layout(title_text="Visitor Loyalty", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # ── Row B: DMA origins ranked bar + DMA avg spend bubble ──────────
        rb1, rb2 = st.columns([3, 2])

        with rb1:
            if not df_vi_dma.empty:
                dma_s = df_vi_dma.sort_values("visitor_days_share_pct", ascending=True).tail(12)
                bar_colors = [C2 if v >= 0 else "#ef4444"
                              for v in dma_s["visitor_days_vs_compare_pct"].fillna(0)]
                fig = go.Figure(go.Bar(
                    y=dma_s["dma"], x=dma_s["visitor_days_share_pct"],
                    orientation="h", marker_color=bar_colors,
                    text=[f"{v:.1f}%" for v in dma_s["visitor_days_share_pct"]],
                    textposition="outside",
                    customdata=dma_s["visitor_days_vs_compare_pct"].fillna(0),
                    hovertemplate="%{y}: %{x:.1f}% share<br>YoY: %{customdata:+.1f}pp<extra></extra>",
                ))
                style_fig(fig, 400)
                fig.update_layout(
                    title_text="Top Origin Markets — Share of Visitor Days<br>"
                               "<sup>Green = growing YoY · Red = declining</sup>",
                    xaxis_title="% of Visitor Days",
                    margin=dict(l=0, r=70, t=52, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

        with rb2:
            if not df_vi_dma.empty and "avg_spend_usd" in df_vi_dma.columns:
                dma_sp = df_vi_dma.dropna(subset=["avg_spend_usd"]).sort_values("avg_spend_usd", ascending=False)
                fig = go.Figure(go.Bar(
                    x=dma_sp["dma"], y=dma_sp["avg_spend_usd"],
                    marker_color=C1,
                    text=[f"${v:.0f}" for v in dma_sp["avg_spend_usd"]],
                    textposition="outside",
                    hovertemplate="%{x}: $%{y:.2f} avg spend/day<extra></extra>",
                ))
                style_fig(fig, 400)
                fig.update_layout(
                    title_text="Avg Spend per Visitor-Day by Origin",
                    yaxis_title="USD", xaxis_tickangle=-30,
                    margin=dict(l=0, r=10, t=52, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # ── Row C: Spending treemap + Age demographics + Income ───────────
        rc1, rc2 = st.columns(2)

        with rc1:
            if not df_vi_spend.empty:
                fig = go.Figure(go.Treemap(
                    labels=df_vi_spend["category"],
                    parents=["Visitor Spending"] * len(df_vi_spend),
                    values=df_vi_spend["spend_share_pct"],
                    textinfo="label+percent entry",
                    marker=dict(colorscale="Blues", showscale=False),
                    hovertemplate="%{label}<br>%{percentParent:.1%} of total spend<extra></extra>",
                ))
                style_fig(fig, 380)
                fig.update_layout(title_text="Visitor Spending by Category",
                                  margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig, use_container_width=True)

        with rc2:
            if not df_vi_demo.empty:
                age_df = df_vi_demo[df_vi_demo["dimension"] == "age"].sort_values("segment")
                if not age_df.empty:
                    fig = go.Figure(go.Bar(
                        x=age_df["segment"], y=age_df["share_pct"],
                        marker=dict(
                            color=age_df["share_pct"],
                            colorscale="Purples", showscale=False,
                        ),
                        text=[f"{v:.1f}%" for v in age_df["share_pct"]],
                        textposition="outside",
                        hovertemplate="Age %{x}: %{y:.1f}%<extra></extra>",
                    ))
                    style_fig(fig, 380)
                    fig.update_layout(
                        title_text="Visitor Age Profile",
                        yaxis_title="Share (%)", xaxis_title="Age Group",
                        margin=dict(l=0, r=0, t=40, b=0),
                    )
                    st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # ── Row D: Airports + Cluster visitation ──────────────────────────
        rd1, rd2 = st.columns(2)

        with rd1:
            if not df_vi_air.empty:
                air_s = df_vi_air.sort_values("passengers_share_pct", ascending=True)
                fig = go.Figure(go.Bar(
                    y=air_s["airport_code"], x=air_s["passengers_share_pct"],
                    orientation="h", marker_color=C3,
                    text=[f"{v:.1f}%" for v in air_s["passengers_share_pct"]],
                    textposition="outside",
                    customdata=air_s["airport_name"],
                    hovertemplate="%{customdata}<br>%{x:.1f}% of fly-in passengers<extra></extra>",
                ))
                style_fig(fig, 340)
                fig.update_layout(
                    title_text="Fly-In Visitors — Top Origin Airports",
                    xaxis_title="Share of Passengers (%)",
                    margin=dict(l=0, r=60, t=40, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

        with rd2:
            if not df_vi_clust.empty:
                fig = go.Figure()
                clust_colors = [C2 if v >= 0 else "#ef4444"
                                for v in df_vi_clust["vs_compare_pct"].fillna(0)]
                fig.add_trace(go.Bar(
                    x=df_vi_clust["cluster"], y=df_vi_clust["visitor_days_share_pct"],
                    marker_color=clust_colors,
                    text=[f"{v:.1f}%" for v in df_vi_clust["visitor_days_share_pct"]],
                    textposition="outside",
                    customdata=df_vi_clust["vs_compare_pct"].fillna(0),
                    hovertemplate="%{x}<br>%{y:.1f}% visitor days<br>YoY: %{customdata:+.1f}pp<extra></extra>",
                ))
                style_fig(fig, 340)
                fig.update_layout(
                    title_text="Cluster Visitation — Share of Visitor Days",
                    yaxis_title="% of Visitor Days",
                    xaxis_tickangle=-15,
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

        # ── Spending vs. Visitor origin scatter (cross-analysis) ──────────
        if not df_vi_dma.empty:
            st.divider()
            dma_full = df_vi_dma.dropna(subset=["avg_spend_usd", "visitor_days_share_pct"])
            if not dma_full.empty:
                fig = go.Figure(go.Scatter(
                    x=dma_full["visitor_days_share_pct"],
                    y=dma_full["avg_spend_usd"],
                    mode="markers+text",
                    marker=dict(
                        size=dma_full["spending_share_pct"].fillna(5) * 3 + 8,
                        color=dma_full["visitor_days_vs_compare_pct"].fillna(0),
                        colorscale="RdYlGn", showscale=True,
                        colorbar=dict(title="YoY pp"),
                        line=dict(width=1, color="rgba(255,255,255,0.3)"),
                    ),
                    text=dma_full["dma"],
                    textposition="top center",
                    textfont=dict(size=10),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "Visitor Days Share: %{x:.1f}%<br>"
                        "Avg Spend/Day: $%{y:.2f}<br>"
                        "<extra></extra>"
                    ),
                ))
                style_fig(fig, 380)
                fig.update_layout(
                    title_text="Origin Market Intelligence — Visitor Share vs. Avg Spend<br>"
                               "<sup>Bubble size = spending share · Color = YoY growth (green=growing)</sup>",
                    xaxis_title="Visitor Days Share (%)",
                    yaxis_title="Avg Spend per Visitor Day (USD)",
                    margin=dict(l=0, r=0, t=60, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)
                st.caption("High-value quadrant (top-right): markets with both large visitor share AND high spend — "
                           "prioritize in media buy. Bottom-right: high-volume, lower-yield — potential yield improvement targets.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — REVENUE & TBID
# ══════════════════════════════════════════════════════════════════════════════
with tab_rev:
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        'Revenue Analytics & TBID Estimation</div>'
        '<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        'Source: STR Monthly · CoStar South OC · TBID blended rate 1.25%</div>',
        unsafe_allow_html=True,
    )

    # Use monthly STR data for revenue analytics
    if df_monthly.empty:
        st.markdown(empty_state("💰", "No monthly STR data.", "Load STR monthly exports to populate."), unsafe_allow_html=True)
    else:
        _rev_df = df_monthly.copy().sort_values("as_of_date")
        _rev_df["year"] = _rev_df["as_of_date"].dt.year
        _rev_df["month"] = _rev_df["as_of_date"].dt.month
        _rev_df["month_label"] = _rev_df["as_of_date"].dt.strftime("%b %Y")

        # TBID estimate (blended 1.25% of room revenue)
        if "revenue" in _rev_df.columns:
            _rev_df["tbid_est"] = _rev_df["revenue"].fillna(0) * 0.0125

        # ── Hero TBID KPIs ────────────────────────────────────────────────
        total_rev  = _rev_df["revenue"].sum()  if "revenue" in _rev_df.columns else 0
        total_tbid = _rev_df["tbid_est"].sum() if "tbid_est" in _rev_df.columns else 0
        latest_adr = _rev_df["adr"].iloc[-1] if "adr" in _rev_df.columns and not _rev_df.empty else 0
        latest_occ = _rev_df["occupancy"].iloc[-1] if "occupancy" in _rev_df.columns and not _rev_df.empty else 0
        latest_rev = _rev_df["revpar"].iloc[-1] if "revpar" in _rev_df.columns and not _rev_df.empty else 0

        tr1, tr2, tr3, tr4, tr5 = st.columns(5)
        with tr1:
            st.metric("Total Room Revenue", f"${total_rev/1e6:.1f}M")
        with tr2:
            st.metric("Est. TBID Contribution", f"${total_tbid/1e3:.0f}K",
                      help="Room Revenue × 1.25% blended TBID rate")
        with tr3:
            st.metric("Latest ADR", f"${latest_adr:,.2f}")
        with tr4:
            st.metric("Latest Occupancy", f"{latest_occ:.1f}%")
        with tr5:
            st.metric("Latest RevPAR", f"${latest_rev:,.2f}")

        st.divider()

        # ── Row A: Monthly Revenue + TBID estimate ────────────────────────
        if "revenue" in _rev_df.columns:
            fig_tbid = make_subplots(specs=[[{"secondary_y": True}]])
            fig_tbid.add_trace(go.Bar(
                x=_rev_df["month_label"], y=_rev_df["revenue"] / 1e6,
                name="Room Revenue ($M)", marker_color="rgba(99,102,241,0.7)",
                hovertemplate="%{x}<br>Revenue: $%{y:.2f}M<extra></extra>",
            ), secondary_y=False)
            fig_tbid.add_trace(go.Scatter(
                x=_rev_df["month_label"], y=_rev_df["tbid_est"] / 1e3,
                name="TBID Estimate ($K)", mode="lines+markers",
                line=dict(color="#f59e0b", width=2),
                marker=dict(size=5, color="#f59e0b"),
                hovertemplate="%{x}<br>TBID Est: $%{y:.1f}K<extra></extra>",
            ), secondary_y=True)
            style_fig(fig_tbid, 320)
            fig_tbid.update_layout(
                title_text="Monthly Room Revenue & TBID Estimate",
                legend=dict(orientation="h", y=1.1, x=0),
                margin=dict(l=0, r=0, t=52, b=0),
            )
            fig_tbid.update_yaxes(title_text="Room Revenue ($M)", secondary_y=False)
            fig_tbid.update_yaxes(title_text="TBID Estimate ($K)", secondary_y=True,
                                  gridcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_tbid, use_container_width=True)

        st.divider()

        # ── Row B: ADR vs Occupancy scatter (all months) + RevPAR heatmap ─
        rb1, rb2 = st.columns(2)

        with rb1:
            if "adr" in _rev_df.columns and "occupancy" in _rev_df.columns:
                has_rev = "revenue" in _rev_df.columns
                scatter_df = _rev_df.dropna(subset=["adr", "occupancy"])
                fig_sc = go.Figure(go.Scatter(
                    x=scatter_df["occupancy"],
                    y=scatter_df["adr"],
                    mode="markers+text",
                    marker=dict(
                        size=scatter_df["revenue"].fillna(0) / scatter_df["revenue"].max() * 20 + 6
                              if has_rev else 10,
                        color=scatter_df["year"],
                        colorscale="Viridis", showscale=True,
                        colorbar=dict(title="Year"),
                        opacity=0.85,
                        line=dict(width=1, color="rgba(255,255,255,0.3)"),
                    ),
                    text=scatter_df["month_label"],
                    textposition="top center",
                    textfont=dict(size=9),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "Occupancy: %{x:.1f}%<br>"
                        "ADR: $%{y:.2f}<br>"
                        "<extra></extra>"
                    ),
                ))
                # Trend line
                import numpy as np
                x_arr = scatter_df["occupancy"].values
                y_arr = scatter_df["adr"].values
                if len(x_arr) > 2:
                    z = np.polyfit(x_arr, y_arr, 1)
                    p = np.poly1d(z)
                    x_line = np.linspace(x_arr.min(), x_arr.max(), 50)
                    fig_sc.add_trace(go.Scatter(
                        x=x_line, y=p(x_line),
                        mode="lines", name="Trend",
                        line=dict(color="#ef4444", width=2, dash="dot"),
                        hoverinfo="skip",
                    ))
                style_fig(fig_sc, 380)
                fig_sc.update_layout(
                    title_text="ADR vs. Occupancy Correlation<br>"
                               "<sup>Bubble size = room revenue · Color = year</sup>",
                    xaxis_title="Occupancy (%)",
                    yaxis_title="ADR (USD)",
                    margin=dict(l=0, r=0, t=60, b=0),
                )
                st.plotly_chart(fig_sc, use_container_width=True)

        with rb2:
            # RevPAR heatmap by month × year
            if "revpar" in _rev_df.columns:
                piv = _rev_df.pivot_table(
                    index="month", columns="year", values="revpar", aggfunc="mean"
                )
                month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                               7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
                y_labels = [month_names.get(m, str(m)) for m in piv.index]
                fig_heat = go.Figure(go.Heatmap(
                    z=piv.values,
                    x=[str(c) for c in piv.columns],
                    y=y_labels,
                    colorscale="RdYlGn",
                    text=[[f"${v:.0f}" if not np.isnan(v) else "—"
                           for v in row] for row in piv.values],
                    texttemplate="%{text}",
                    hovertemplate="Month: %{y}<br>Year: %{x}<br>RevPAR: $%{z:.2f}<extra></extra>",
                ))
                style_fig(fig_heat, 380)
                fig_heat.update_layout(
                    title_text="RevPAR Heatmap — Month × Year",
                    xaxis_title="Year", yaxis_title="Month",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_heat, use_container_width=True)

        st.divider()

        # ── Row C: Occupancy calendar heatmap + YoY waterfall ─────────────
        rc1, rc2 = st.columns(2)

        with rc1:
            # Compression: days above 80% / 90% per quarter
            if not df_comp.empty:
                fig_comp = go.Figure()
                fig_comp.add_trace(go.Bar(
                    x=df_comp["quarter"], y=df_comp["days_above_80_occ"],
                    name="Days >80% Occ", marker_color="rgba(99,102,241,0.75)",
                ))
                fig_comp.add_trace(go.Bar(
                    x=df_comp["quarter"], y=df_comp["days_above_90_occ"],
                    name="Days >90% Occ", marker_color="rgba(239,68,68,0.85)",
                ))
                style_fig(fig_comp, 320)
                fig_comp.update_layout(
                    title_text="Compression Days by Quarter",
                    barmode="group",
                    yaxis_title="Days",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_comp, use_container_width=True)

        with rc2:
            # TBID tier breakdown illustration
            if "adr" in _rev_df.columns:
                _tier_data = {"≤$199 (1.0%)": 0, "$200–$399 (1.5%)": 0, "≥$400 (2.0%)": 0}
                for _, row in _rev_df.iterrows():
                    adr = row.get("adr", 0) or 0
                    rev = row.get("revenue", 0) or 0
                    if adr < 200:
                        _tier_data["≤$199 (1.0%)"] += rev * 0.010
                    elif adr < 400:
                        _tier_data["$200–$399 (1.5%)"] += rev * 0.015
                    else:
                        _tier_data["≥$400 (2.0%)"] += rev * 0.020
                _tier_df = pd.DataFrame({
                    "Tier": list(_tier_data.keys()),
                    "TBID ($K)": [v / 1000 for v in _tier_data.values()],
                })
                fig_tier = go.Figure(go.Funnel(
                    y=_tier_df["Tier"],
                    x=_tier_df["TBID ($K)"],
                    textinfo="value+percent total",
                    marker_color=["#3b82f6", "#6366f1", "#a855f7"],
                ))
                style_fig(fig_tier, 320)
                fig_tier.update_layout(
                    title_text="Est. TBID by Rate Tier ($K)",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_tier, use_container_width=True)

        # ── Revenue waterfall by year ──────────────────────────────────────
        if "revenue" in _rev_df.columns:
            _yr_rev = _rev_df.groupby("year")["revenue"].sum().reset_index()
            _yr_rev = _yr_rev.sort_values("year")
            if len(_yr_rev) >= 2:
                st.divider()
                wf_measure = ["absolute"] + ["relative"] * (len(_yr_rev) - 2) + ["total"]
                wf_x       = [str(y) for y in _yr_rev["year"]]
                wf_y       = [_yr_rev["revenue"].iloc[0]] + \
                             list(_yr_rev["revenue"].diff().dropna().iloc[:-1]) + \
                             [_yr_rev["revenue"].iloc[-1]]
                if len(_yr_rev) == 2:
                    wf_measure = ["absolute", "total"]
                    wf_y = list(_yr_rev["revenue"])
                fig_wf = go.Figure(go.Waterfall(
                    x=wf_x, y=[v / 1e6 for v in wf_y],
                    measure=wf_measure,
                    textposition="outside",
                    text=[f"${v/1e6:.1f}M" for v in wf_y],
                    connector=dict(line=dict(color="rgba(127,127,127,0.3)")),
                    increasing=dict(marker_color="#10b981"),
                    decreasing=dict(marker_color="#ef4444"),
                    totals=dict(marker_color="#6366f1"),
                ))
                style_fig(fig_wf, 300)
                fig_wf.update_layout(
                    title_text="Annual Room Revenue Waterfall ($M)",
                    yaxis_title="Revenue ($M)",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_wf, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — DIGITAL & CAMPAIGN
# ══════════════════════════════════════════════════════════════════════════════
with tab_dig:
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        'Digital & Campaign Analytics</div>'
        '<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        'Source: Datafy Attribution · 2025-26 Annual Campaign · visitdanapoint.com</div>',
        unsafe_allow_html=True,
    )

    df_mk  = load_datafy_media_kpis()
    df_mm  = load_datafy_media_markets()
    df_wk  = load_datafy_web_kpis()
    df_wch = load_datafy_web_channels()
    df_wdm = load_datafy_web_dma()
    df_tp  = load_datafy_top_pages()
    df_ts  = load_datafy_traffic_sources()

    # ── Campaign Hero KPIs ────────────────────────────────────────────────
    if not df_mk.empty:
        mk = df_mk.iloc[0]
        st.markdown("### 📡 Annual Media Campaign")
        ck1, ck2, ck3, ck4, ck5, ck6 = st.columns(6)
        with ck1:
            st.metric("Total Impressions", f"{mk.get('total_impressions', 0)/1e6:.1f}M")
        with ck2:
            st.metric("Unique Reach", f"{mk.get('unique_reach', 0)/1e6:.2f}M")
        with ck3:
            st.metric("Attributable Trips", f"{mk.get('attributable_trips', 0):,}")
        with ck4:
            st.metric("Campaign Impact", f"${mk.get('total_impact_usd', 0)/1e3:.0f}K")
        with ck5:
            st.metric("Manual ADR Used", f"${mk.get('manual_adr', 0):,.0f}")
        with ck6:
            st.metric("Spend/Visitor", f"${mk.get('cohort_spend_per_visitor', 0):.2f}")

        st.divider()

        # Campaign funnel
        impressions  = mk.get("total_impressions", 0)
        reach        = mk.get("unique_reach", 0)
        trips        = mk.get("attributable_trips", 0)
        impact       = mk.get("total_impact_usd", 0)

        fig_funnel = go.Figure(go.Funnel(
            y=["Impressions", "Unique Reach", "Attributable Trips", "Economic Impact ($)"],
            x=[impressions / 1e6, reach / 1e6, trips / 1000, impact / 1e6],
            textinfo="value+percent initial",
            texttemplate=[
                f"{impressions/1e6:.1f}M",
                f"{reach/1e6:.2f}M ({reach/impressions*100:.1f}%)",
                f"{trips:,} trips ({trips/reach*100:.2f}%)",
                f"${impact/1e3:.0f}K impact",
            ],
            marker_color=["#6366f1", "#3b82f6", "#10b981", "#f59e0b"],
        ))
        style_fig(fig_funnel, 320)
        fig_funnel.update_layout(
            title_text="Campaign Conversion Funnel — Impressions → Economic Impact",
            margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig_funnel, use_container_width=True)

        # Media top markets impact
        if not df_mm.empty:
            st.divider()
            mm_s = df_mm.sort_values("dma_est_impact_usd", ascending=True)
            fig_mm = go.Figure(go.Bar(
                y=mm_s["top_dma"], x=mm_s["dma_est_impact_usd"] / 1e3,
                orientation="h",
                marker=dict(
                    color=mm_s["dma_share_of_impact_pct"],
                    colorscale="Blues", showscale=True,
                    colorbar=dict(title="% of Total Impact"),
                ),
                text=[f"${v/1e3:.0f}K" for v in mm_s["dma_est_impact_usd"]],
                textposition="outside",
                hovertemplate="%{y}<br>Impact: $%{x:.0f}K<br>Share: %{marker.color:.1f}%<extra></extra>",
            ))
            style_fig(fig_mm, 380)
            fig_mm.update_layout(
                title_text="Campaign Economic Impact by Origin Market",
                xaxis_title="Estimated Impact ($K)",
                margin=dict(l=0, r=70, t=40, b=0),
            )
            st.plotly_chart(fig_mm, use_container_width=True)
    else:
        st.info("No media campaign data. Run load_datafy_reports.py.")

    # ── Website Attribution Section ───────────────────────────────────────
    if not df_wk.empty:
        wk = df_wk.iloc[0]
        st.markdown("### 🌐 Website Attribution")
        wk1, wk2, wk3, wk4, wk5 = st.columns(5)
        with wk1:
            st.metric("Total Sessions", f"{wk.get('total_website_sessions', 0):,}")
        with wk2:
            st.metric("Page Views", f"{wk.get('website_pageviews', 0):,}")
        with wk3:
            st.metric("Attributable Trips", f"{wk.get('attributable_trips', 0):,}")
        with wk4:
            st.metric("Engagement Rate", f"{wk.get('avg_engagement_rate_pct', 0):.1f}%")
        with wk5:
            st.metric("Website Impact", f"${wk.get('est_impact_usd', 0)/1e3:.0f}K")

    if not df_wch.empty:
        st.divider()
        wc1, wc2 = st.columns(2)

        with wc1:
            fig_ch = go.Figure()
            ch_colors = {"email": "#6366f1", "redirect": "#3b82f6", "search": "#10b981"}
            fig_ch.add_trace(go.Bar(
                x=df_wch["acquisition_channel"],
                y=df_wch["sessions"],
                name="Sessions",
                marker_color=[ch_colors.get(c, "#94a3b8") for c in df_wch["acquisition_channel"]],
                text=df_wch["sessions"],
                textposition="outside",
            ))
            fig_ch2 = make_subplots(rows=1, cols=1)
            fig_channels = go.Figure()
            fig_channels.add_trace(go.Bar(
                name="Sessions",
                x=df_wch["acquisition_channel"],
                y=df_wch["sessions"],
                marker_color=["#6366f1", "#3b82f6", "#10b981"][:len(df_wch)],
                yaxis="y",
            ))
            fig_channels.add_trace(go.Scatter(
                name="Engagement Rate",
                x=df_wch["acquisition_channel"],
                y=df_wch["engagement_rate_pct"],
                mode="markers+lines",
                marker=dict(size=10, color="#f59e0b"),
                line=dict(color="#f59e0b", width=2),
                yaxis="y2",
            ))
            style_fig(fig_channels, 320)
            fig_channels.update_layout(
                title_text="Channel Sessions & Engagement Rate",
                yaxis=dict(title="Sessions"),
                yaxis2=dict(title="Engagement Rate (%)", overlaying="y", side="right",
                            showgrid=False),
                margin=dict(l=0, r=50, t=40, b=0),
            )
            st.plotly_chart(fig_channels, use_container_width=True)

        with wc2:
            if not df_wch.empty and "attributable_trips_dest" in df_wch.columns:
                fig_attr = go.Figure(go.Bar(
                    x=df_wch["acquisition_channel"],
                    y=df_wch["attributable_trips_dest"].fillna(0),
                    marker_color=["#6366f1", "#3b82f6", "#10b981"][:len(df_wch)],
                    text=df_wch["attributable_trips_dest"].fillna(0).astype(int),
                    textposition="outside",
                    hovertemplate="%{x}: %{y} attributable trips<extra></extra>",
                ))
                style_fig(fig_attr, 320)
                fig_attr.update_layout(
                    title_text="Attributable Trips by Acquisition Channel",
                    yaxis_title="Trips",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_attr, use_container_width=True)

    # ── Top Pages + Traffic Sources ────────────────────────────────────────
    if not df_tp.empty or not df_ts.empty:
        st.divider()
        dp1, dp2 = st.columns(2)

        with dp1:
            if not df_tp.empty:
                pages_s = df_tp.head(15).sort_values("page_views", ascending=True)
                # Truncate long titles
                pages_s = pages_s.copy()
                pages_s["short_title"] = pages_s["page_title"].str[:35] + "…"
                fig_pages = go.Figure(go.Bar(
                    y=pages_s["short_title"],
                    x=pages_s["page_views"],
                    orientation="h",
                    marker=dict(
                        color=pages_s["page_views"],
                        colorscale="Blues", showscale=False,
                    ),
                    text=[f"{v:,}" for v in pages_s["page_views"]],
                    textposition="outside",
                    customdata=pages_s["page_path"],
                    hovertemplate="%{y}<br>Path: %{customdata}<br>Views: %{x:,}<extra></extra>",
                ))
                style_fig(fig_pages, 440)
                fig_pages.update_layout(
                    title_text="Top Pages by Views (visitdanapoint.com)",
                    xaxis_title="Page Views",
                    margin=dict(l=0, r=80, t=40, b=0),
                )
                st.plotly_chart(fig_pages, use_container_width=True)

        with dp2:
            if not df_ts.empty:
                ts_s = df_ts.sort_values("sessions", ascending=True)
                fig_ts = go.Figure()
                fig_ts.add_trace(go.Bar(
                    y=ts_s["source"], x=ts_s["sessions"],
                    orientation="h", name="Sessions",
                    marker_color="rgba(99,102,241,0.75)",
                    hovertemplate="%{y}: %{x:,} sessions<extra></extra>",
                ))
                style_fig(fig_ts, 440)
                fig_ts.update_layout(
                    title_text="Traffic Sources by Sessions",
                    xaxis_title="Sessions",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_ts, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 8 — CROSS-DATASET INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════
with tab_cx:
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        'Cross-Dataset Intelligence</div>'
        '<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        'STR Portfolio × CoStar Market × Datafy Visitors — correlation &amp; gap analysis</div>',
        unsafe_allow_html=True,
    )

    df_cx_costar = load_costar_monthly()
    df_cx_chain  = load_costar_chain()
    df_cx_comp   = load_costar_compset()
    df_cx_dma    = load_datafy_dma()
    df_cx_media  = load_datafy_media_markets()

    # ── Section 1: STR Portfolio vs CoStar Market ─────────────────────────
    st.markdown("### STR Portfolio vs. CoStar South OC Market")

    if not df_monthly.empty and not df_cx_costar.empty:
        # Align on as_of_date
        _str = df_monthly[["as_of_date","revpar","adr","occupancy"]].copy()
        _str["as_of_date"] = pd.to_datetime(_str["as_of_date"])
        _str["month_label"] = _str["as_of_date"].dt.strftime("%b %Y")

        _cs = df_cx_costar[["as_of_date","revpar_usd","adr_usd","occupancy_pct"]].copy()
        _cs["as_of_date"] = pd.to_datetime(_cs["as_of_date"])
        _cs["month_label"] = _cs["as_of_date"].dt.strftime("%b %Y")

        merged = pd.merge(_str, _cs, on="month_label", how="inner", suffixes=("_str","_cs"))

        if not merged.empty:
            # RevPAR dual-axis
            fig_rv = go.Figure()
            fig_rv.add_trace(go.Scatter(
                x=merged["month_label"], y=merged["revpar"],
                name="STR Portfolio RevPAR", mode="lines+markers",
                line=dict(color="#6366f1", width=2.5),
                marker=dict(size=5),
                hovertemplate="%{x}<br>Portfolio RevPAR: $%{y:.2f}<extra></extra>",
            ))
            fig_rv.add_trace(go.Scatter(
                x=merged["month_label"], y=merged["revpar_usd"],
                name="CoStar Market RevPAR", mode="lines+markers",
                line=dict(color="#10b981", width=2.5, dash="dash"),
                marker=dict(size=5, symbol="diamond"),
                hovertemplate="%{x}<br>Market RevPAR: $%{y:.2f}<extra></extra>",
            ))
            # Gap fill
            fig_rv.add_trace(go.Scatter(
                x=list(merged["month_label"]) + list(reversed(merged["month_label"])),
                y=list(merged["revpar"]) + list(reversed(merged["revpar_usd"])),
                fill="toself",
                fillcolor="rgba(99,102,241,0.07)",
                line=dict(color="rgba(0,0,0,0)"),
                name="Gap", showlegend=False, hoverinfo="skip",
            ))
            style_fig(fig_rv, 320)
            fig_rv.update_layout(
                title_text="RevPAR: STR Portfolio vs. CoStar South OC Market",
                yaxis_title="RevPAR (USD)",
                margin=dict(l=0, r=0, t=40, b=0),
            )
            st.plotly_chart(fig_rv, use_container_width=True)

            # ADR + Occ comparison side by side
            cx1, cx2 = st.columns(2)
            with cx1:
                fig_adr = go.Figure()
                fig_adr.add_trace(go.Bar(
                    x=merged["month_label"], y=merged["adr"],
                    name="STR Portfolio ADR", marker_color="rgba(99,102,241,0.7)",
                ))
                fig_adr.add_trace(go.Bar(
                    x=merged["month_label"], y=merged["adr_usd"],
                    name="CoStar Market ADR", marker_color="rgba(16,185,129,0.7)",
                ))
                style_fig(fig_adr, 300)
                fig_adr.update_layout(
                    title_text="ADR Comparison — Portfolio vs. Market",
                    barmode="group", yaxis_title="ADR ($)",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_adr, use_container_width=True)

            with cx2:
                fig_occ = go.Figure()
                fig_occ.add_trace(go.Scatter(
                    x=merged["month_label"], y=merged["occupancy"],
                    name="STR Portfolio Occ", mode="lines+markers",
                    line=dict(color="#6366f1", width=2),
                    marker=dict(size=5),
                ))
                fig_occ.add_trace(go.Scatter(
                    x=merged["month_label"], y=merged["occupancy_pct"],
                    name="CoStar Market Occ", mode="lines+markers",
                    line=dict(color="#10b981", width=2, dash="dash"),
                    marker=dict(size=5, symbol="diamond"),
                ))
                # 80% reference line
                fig_occ.add_hline(y=80, line=dict(color="#f59e0b", width=1, dash="dot"),
                                  annotation_text="80% Compression", annotation_position="bottom right")
                style_fig(fig_occ, 300)
                fig_occ.update_layout(
                    title_text="Occupancy — Portfolio vs. Market",
                    yaxis_title="Occupancy (%)",
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                st.plotly_chart(fig_occ, use_container_width=True)

    st.divider()

    # ── Section 2: Chain Scale Competitive Positioning ────────────────────
    st.markdown("### Chain Scale Performance — Competitive Landscape")

    if not df_cx_chain.empty:
        chain_24 = df_cx_chain[df_cx_chain["year"] == "2024"].sort_values("revpar_usd", ascending=False)
        if not chain_24.empty:
            cx2a, cx2b = st.columns(2)

            with cx2a:
                # ADR vs Occupancy bubble (bubble = rooms, color = RevPAR)
                fig_bub = go.Figure(go.Scatter(
                    x=chain_24["occupancy_pct"],
                    y=chain_24["adr_usd"],
                    mode="markers+text",
                    marker=dict(
                        size=chain_24["supply_rooms"] / chain_24["supply_rooms"].max() * 50 + 15,
                        color=chain_24["revpar_usd"],
                        colorscale="Viridis", showscale=True,
                        colorbar=dict(title="RevPAR ($)"),
                        opacity=0.85,
                        line=dict(width=1, color="rgba(255,255,255,0.4)"),
                    ),
                    text=chain_24["chain_scale"],
                    textposition="top center",
                    textfont=dict(size=10, family=_font),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "Occupancy: %{x:.1f}%<br>"
                        "ADR: $%{y:.0f}<br>"
                        "RevPAR: %{marker.color:.0f}<extra></extra>"
                    ),
                ))
                style_fig(fig_bub, 360)
                fig_bub.update_layout(
                    title_text="Chain Scale — ADR vs. Occupancy<br>"
                               "<sup>Bubble size = supply rooms · Color = RevPAR</sup>",
                    xaxis_title="Occupancy (%)",
                    yaxis_title="ADR ($)",
                    margin=dict(l=0, r=0, t=60, b=0),
                )
                st.plotly_chart(fig_bub, use_container_width=True)

            with cx2b:
                # RevPAR horizontal bar ranked
                cs_sorted = chain_24.sort_values("revpar_usd", ascending=True)
                fig_cs_bar = go.Figure(go.Bar(
                    y=cs_sorted["chain_scale"],
                    x=cs_sorted["revpar_usd"],
                    orientation="h",
                    marker=dict(
                        color=cs_sorted["revpar_usd"],
                        colorscale="Blues", showscale=False,
                    ),
                    text=[f"${v:.0f}" for v in cs_sorted["revpar_usd"]],
                    textposition="outside",
                    hovertemplate="%{y}<br>RevPAR: $%{x:.2f}<extra></extra>",
                ))
                style_fig(fig_cs_bar, 360)
                fig_cs_bar.update_layout(
                    title_text="RevPAR Ranking by Chain Scale (2024)",
                    xaxis_title="RevPAR (USD)",
                    margin=dict(l=0, r=70, t=40, b=0),
                )
                st.plotly_chart(fig_cs_bar, use_container_width=True)

    st.divider()

    # ── Section 3: Competitive Set — Market Positioning Radar ─────────────
    st.markdown("### Competitive Set — MPI / ARI / RGI Positioning")

    if not df_cx_comp.empty and {"mpi","ari","rgi","property_name"}.issubset(df_cx_comp.columns):
        cx3a, cx3b = st.columns(2)

        with cx3a:
            # Radar chart per property (MPI/ARI/RGI)
            categories = ["Market Penetration Index<br>(MPI)",
                          "Average Rate Index<br>(ARI)",
                          "Revenue Gen. Index<br>(RGI)"]
            fig_radar = go.Figure()
            pal = ["#6366f1","#10b981","#f59e0b","#3b82f6","#a855f7","#ef4444",
                   "#14b8a6","#f97316","#8b5cf6","#ec4899"]
            for i, row in df_cx_comp.iterrows():
                vals = [row.get("mpi", 100), row.get("ari", 100), row.get("rgi", 100)]
                vals_closed = vals + [vals[0]]
                fig_radar.add_trace(go.Scatterpolar(
                    r=vals_closed,
                    theta=categories + [categories[0]],
                    name=row["property_name"][:22],
                    fill="toself",
                    opacity=0.35,
                    line=dict(color=pal[i % len(pal)], width=2),
                ))
            # 100 reference circle
            fig_radar.add_trace(go.Scatterpolar(
                r=[100, 100, 100, 100],
                theta=categories + [categories[0]],
                name="Market Parity (100)",
                line=dict(color="#ffffff", width=1.5, dash="dot"),
                fill=None, showlegend=True,
            ))
            style_fig(fig_radar, 420)
            fig_radar.update_layout(
                polar=dict(
                    bgcolor="rgba(0,0,0,0)",
                    radialaxis=dict(visible=True, range=[0, 300],
                                   tickfont=dict(size=10)),
                    angularaxis=dict(tickfont=dict(size=10)),
                ),
                title_text="Competitive Set — Index Radar",
                showlegend=True,
                margin=dict(l=0, r=0, t=50, b=0),
            )
            st.plotly_chart(fig_radar, use_container_width=True)

        with cx3b:
            # Compset ADR vs Occupancy scatter
            fig_csc = go.Figure()
            scale_colors = {"Luxury": "#a855f7", "Upper Upscale": "#6366f1",
                            "Upscale": "#3b82f6", "Upper Midscale": "#10b981",
                            "Midscale": "#f59e0b"}
            for chain_sc, grp in df_cx_comp.groupby("chain_scale"):
                fig_csc.add_trace(go.Scatter(
                    x=grp["occupancy_pct"],
                    y=grp["adr_usd"],
                    mode="markers+text",
                    name=chain_sc,
                    marker=dict(
                        size=grp["rooms"] / df_cx_comp["rooms"].max() * 30 + 10,
                        color=scale_colors.get(chain_sc, "#94a3b8"),
                        opacity=0.85,
                        line=dict(width=1, color="rgba(255,255,255,0.3)"),
                    ),
                    text=grp["property_name"].str[:14],
                    textposition="top center",
                    textfont=dict(size=9),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "Occ: %{x:.1f}%<br>ADR: $%{y:.0f}<br>"
                        "RevPAR: $" + grp["revpar_usd"].astype(str) + "<extra></extra>"
                    ),
                ))
            style_fig(fig_csc, 420)
            fig_csc.update_layout(
                title_text="Compset Positioning — ADR vs. Occupancy<br>"
                           "<sup>Bubble size = rooms · Color = chain scale</sup>",
                xaxis_title="Occupancy (%)",
                yaxis_title="ADR ($)",
                margin=dict(l=0, r=0, t=60, b=0),
            )
            st.plotly_chart(fig_csc, use_container_width=True)

    st.divider()

    # ── Section 4: Visitor Origin × Campaign Impact ROI ───────────────────
    st.markdown("### Visitor Origin × Campaign Impact — ROI by Market")

    if not df_cx_dma.empty and not df_cx_media.empty:
        roi_dma = pd.merge(
            df_cx_dma[["dma","visitor_days_share_pct","avg_spend_usd",
                        "visitor_days_vs_compare_pct"]].dropna(subset=["avg_spend_usd"]),
            df_cx_media[["top_dma","dma_est_impact_usd","dma_share_of_impact_pct"]].rename(
                columns={"top_dma":"dma"}),
            on="dma", how="inner",
        )
        if not roi_dma.empty:
            fig_roi = go.Figure(go.Scatter(
                x=roi_dma["visitor_days_share_pct"],
                y=roi_dma["dma_est_impact_usd"] / 1e3,
                mode="markers+text",
                marker=dict(
                    size=roi_dma["avg_spend_usd"] / roi_dma["avg_spend_usd"].max() * 40 + 12,
                    color=roi_dma["visitor_days_vs_compare_pct"],
                    colorscale="RdYlGn", showscale=True,
                    colorbar=dict(title="YoY Visitor<br>Growth (pp)"),
                    opacity=0.85,
                    line=dict(width=1, color="rgba(255,255,255,0.3)"),
                ),
                text=roi_dma["dma"],
                textposition="top center",
                textfont=dict(size=10),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "Visitor Days Share: %{x:.1f}%<br>"
                    "Campaign Impact: $%{y:.0f}K<br>"
                    "Avg Spend/Day: $" +
                    roi_dma["avg_spend_usd"].round(0).astype(int).astype(str) +
                    "<extra></extra>"
                ),
            ))
            style_fig(fig_roi, 400)
            fig_roi.update_layout(
                title_text="Origin Market ROI Matrix — Visitor Share vs. Campaign Impact<br>"
                           "<sup>Bubble size = avg spend/day · Color = YoY visitor growth</sup>",
                xaxis_title="Visitor Days Share (%)",
                yaxis_title="Campaign Impact ($K)",
                margin=dict(l=0, r=0, t=60, b=0),
            )
            st.plotly_chart(fig_roi, use_container_width=True)
            st.caption(
                "Top-right quadrant = highest-performing markets (large visitor share + strong campaign impact). "
                "Green bubbles = growing markets. Prioritize media spend toward high-spend, growing markets."
            )
    else:
        st.info("Visitor + campaign data not yet loaded. Run load_datafy_reports.py to populate.")

    st.divider()

    # ── Section 5: Supply Pipeline Impact Forecast ────────────────────────
    st.markdown("### Supply Pipeline — Market Impact Forecast")

    df_cx_pipe = load_costar_pipeline()
    if not df_cx_pipe.empty:
        # Show pipeline properties with bar chart
        pipe_sorted = df_cx_pipe.sort_values("rooms", ascending=True)
        scale_pal = {"Luxury": "#a855f7", "Upper Upscale": "#6366f1",
                     "Upscale": "#3b82f6", "Upper Midscale": "#10b981",
                     "Midscale": "#f59e0b", "Economy": "#94a3b8"}
        pipe_colors = [scale_pal.get(s, "#64748b") for s in pipe_sorted["chain_scale"]]
        fig_pipe = go.Figure(go.Bar(
            y=pipe_sorted["property_name"],
            x=pipe_sorted["rooms"],
            orientation="h",
            marker_color=pipe_colors,
            text=[f"{r} rooms · {s}" for r, s in
                  zip(pipe_sorted["rooms"], pipe_sorted["projected_open_date"])],
            textposition="outside",
            customdata=pipe_sorted[["chain_scale","status","submarket"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>Rooms: %{x}<br>"
                "Chain Scale: %{customdata[0]}<br>"
                "Status: %{customdata[1]}<br>"
                "Submarket: %{customdata[2]}<extra></extra>"
            ),
        ))
        style_fig(fig_pipe, 300)
        fig_pipe.update_layout(
            title_text="Supply Pipeline — New Rooms by Property",
            xaxis_title="New Rooms",
            margin=dict(l=0, r=160, t=40, b=0),
        )
        st.plotly_chart(fig_pipe, use_container_width=True)

        # Summary: total rooms by status
        px1, px2 = st.columns(2)
        with px1:
            status_sum = df_cx_pipe.groupby("status")["rooms"].sum().reset_index()
            fig_status = go.Figure(go.Pie(
                labels=status_sum["status"],
                values=status_sum["rooms"],
                hole=0.55,
                marker_colors=["#6366f1", "#f59e0b", "#10b981", "#3b82f6"][:len(status_sum)],
                textinfo="label+value",
            ))
            style_fig(fig_status, 280)
            fig_status.update_layout(
                title_text="Pipeline Rooms by Status",
                showlegend=False,
                margin=dict(l=0, r=0, t=40, b=0),
            )
            st.plotly_chart(fig_status, use_container_width=True)

        with px2:
            scale_sum = df_cx_pipe.groupby("chain_scale")["rooms"].sum().reset_index()
            fig_scale = go.Figure(go.Pie(
                labels=scale_sum["chain_scale"],
                values=scale_sum["rooms"],
                hole=0.55,
                marker_colors=[scale_pal.get(s, "#64748b") for s in scale_sum["chain_scale"]],
                textinfo="label+value",
            ))
            style_fig(fig_scale, 280)
            fig_scale.update_layout(
                title_text="Pipeline Rooms by Chain Scale",
                showlegend=False,
                margin=dict(l=0, r=0, t=40, b=0),
            )
            st.plotly_chart(fig_scale, use_container_width=True)

        total_pipeline = df_cx_pipe["rooms"].sum()
        current_supply = 5120  # from CoStar snapshot
        st.info(
            f"**Supply Impact:** {total_pipeline:,} new rooms in pipeline vs. "
            f"{current_supply:,} current supply = "
            f"**+{total_pipeline/current_supply*100:.1f}% supply growth** when all projects open. "
            f"This may pressure occupancy rates unless demand grows proportionally."
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 9 — BOARD REPORTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_rpt:
    _rpt_font = "Plus Jakarta Sans, Inter, sans-serif"
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        '📋 Board Intelligence Reports</div>'
        '<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        'AI-generated · All datasets integrated · Downloadable HTML · Board-ready</div>',
        unsafe_allow_html=True,
    )

    # ── Configuration ─────────────────────────────────────────────────────
    cfg1, cfg2, cfg3 = st.columns([2, 2, 1])
    with cfg1:
        report_type_sel = st.selectbox("Report Type", [
            "Full Board Report — All Datasets",
            "Monthly Performance Report — STR + CoStar",
            "Visitor & Campaign Intelligence — Datafy + Attribution",
            "Market Positioning Report — CoStar Competitive Set",
            "Revenue & TBID Impact Report",
        ])
    with cfg2:
        period_sel = st.text_input(
            "Report Period / Label",
            value=f"Q1 2025 · January – March 2025",
            help="E.g. 'Annual 2024', 'Q3 2025', or 'YTD through March 2026'",
        )
    with cfg3:
        max_tokens_sel = st.select_slider(
            "Report Depth",
            options=[1500, 2500, 4000],
            value=2500,
            format_func=lambda v: {1500: "Standard", 2500: "Deep", 4000: "Comprehensive"}[v],
        )

    # ── Generate button ───────────────────────────────────────────────────
    api_key_rpt = st.session_state.get("api_key_input", _ENV_API_KEY)
    api_valid_rpt = bool(api_key_rpt and len(api_key_rpt) > 20)

    if not api_valid_rpt:
        st.warning("Add your Anthropic API key in the sidebar to generate AI reports.", icon="🔑")

    col_gen, col_preview = st.columns([1, 2])
    with col_gen:
        gen_btn = st.button(
            "🤖 Generate Board Report",
            disabled=not api_valid_rpt,
            use_container_width=True,
            type="primary",
        )
        if "rpt_content" in st.session_state and st.session_state["rpt_content"]:
            if st.button("🗑 Clear Report", use_container_width=True):
                st.session_state.pop("rpt_content", None)
                st.session_state.pop("rpt_html", None)
                st.rerun()

    with col_preview:
        st.markdown(
            '<div style="background:linear-gradient(135deg,#0f172a,#0c4a6e);'
            'border-radius:10px;padding:20px 24px;color:#fff;">'
            '<div style="font-size:11px;font-weight:700;letter-spacing:0.1em;'
            'color:#38bdf8;text-transform:uppercase;margin-bottom:8px;">Report Includes</div>'
            '<div style="display:grid;grid-template-columns:1fr 1fr;gap:4px 16px;'
            'font-size:12px;opacity:0.85;">'
            '<span>✅ Executive Summary (5 headlines)</span>'
            '<span>✅ Market Performance (STR + CoStar)</span>'
            '<span>✅ Visitor Intelligence (Datafy)</span>'
            '<span>✅ Digital &amp; Campaign ROI</span>'
            '<span>✅ Revenue &amp; TBID Analysis</span>'
            '<span>✅ Strategic Correlations</span>'
            '<span>✅ Forward-Looking Outlook</span>'
            '<span>✅ Risk Register</span>'
            '<span>✅ 5 Recommended Actions</span>'
            '<span>✅ Glossary + Data Appendix</span>'
            '<span>✅ 7 Interactive Charts</span>'
            '<span>✅ Downloadable HTML Report</span>'
            '</div></div>',
            unsafe_allow_html=True,
        )

    st.divider()

    # ── Generation logic ───────────────────────────────────────────────────
    if gen_btn and api_valid_rpt:
        with st.spinner("Pulling all data from database…"):
            full_ctx = build_full_report_context()
            prompt   = build_board_report_prompt(report_type_sel, period_sel, full_ctx)

        st.markdown("**Generating board report with Claude AI…**")
        report_placeholder = st.empty()
        report_text = ""

        try:
            client = anthropic.Anthropic(api_key=api_key_rpt)
            with client.messages.stream(
                model=CLAUDE_MODEL,
                max_tokens=max_tokens_sel,
                system=[{"type": "text", "text": BOARD_REPORT_SYSTEM,
                         "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user", "content": prompt}],
            ) as stream:
                for chunk in stream.text_stream:
                    report_text += chunk
                    report_placeholder.markdown(report_text + " ▌")
            report_placeholder.markdown(report_text)
            st.session_state["rpt_content"] = report_text
            st.session_state["rpt_type"]    = report_type_sel
            st.session_state["rpt_period"]  = period_sel
            st.success("Report generated. Scroll down to download.")
        except Exception as e:
            st.error(f"Error generating report: {e}")

    # ── Display existing report + download ────────────────────────────────
    if "rpt_content" in st.session_state and st.session_state["rpt_content"]:
        rpt_text   = st.session_state["rpt_content"]
        rpt_type   = st.session_state.get("rpt_type", report_type_sel)
        rpt_period = st.session_state.get("rpt_period", period_sel)

        st.divider()
        st.markdown("### Report Preview")
        with st.expander("View Full AI Narrative", expanded=True):
            st.markdown(rpt_text)

        st.divider()
        st.markdown("### Download Options")

        with st.spinner("Building HTML report with charts…"):
            charts_html = generate_report_charts_html()
            rpt_title   = f"VDP Board Intelligence Report — {rpt_period}"
            html_report = generate_report_html(
                title=rpt_title, period=rpt_period, report_type=rpt_type,
                ai_narrative=rpt_text, charts_html=charts_html,
            )

        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                label="⬇️ Download HTML Report (with charts)",
                data=html_report,
                file_name=f"VDP_Board_Report_{pd.Timestamp.now().strftime('%Y%m%d')}.html",
                mime="text/html",
                use_container_width=True,
                type="primary",
            )
        with dl2:
            # Plain text / markdown download
            st.download_button(
                label="⬇️ Download Markdown Report",
                data=rpt_text,
                file_name=f"VDP_Board_Report_{pd.Timestamp.now().strftime('%Y%m%d')}.md",
                mime="text/markdown",
                use_container_width=True,
            )

        # ── Report metadata card ───────────────────────────────────────────
        st.markdown(
            f'<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;'
            f'padding:18px 22px;margin-top:16px;font-size:12px;color:#64748b;">'
            f'<div style="font-weight:700;color:#0f172a;margin-bottom:8px;">Report Metadata</div>'
            f'<div><strong>Type:</strong> {rpt_type}</div>'
            f'<div><strong>Period:</strong> {rpt_period}</div>'
            f'<div><strong>Generated:</strong> {pd.Timestamp.now().strftime("%B %d, %Y at %I:%M %p")}</div>'
            f'<div><strong>AI Model:</strong> Claude Sonnet 4.6 (Anthropic)</div>'
            f'<div><strong>Data Sources:</strong> STR daily/monthly, CoStar (5 tables), '
            f'Datafy (17 tables), KPI summaries</div>'
            f'<div><strong>Word count (AI narrative):</strong> ~{len(rpt_text.split()):,} words</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    else:
        # Empty state — show prompt preview
        st.markdown("### The AI Prompt")
        st.markdown(
            "This is the system instruction sent to Claude when you generate a report. "
            "It ensures every report follows board standards, answers all 6 journalist questions, "
            "and uses 2025 data visualization language:"
        )
        with st.expander("View Board Report System Prompt", expanded=False):
            st.code(BOARD_REPORT_SYSTEM, language="text")

        st.markdown("### Data Context Preview")
        st.markdown("The following live database context is injected into every report:")
        with st.expander("Preview Database Context (sent to Claude)", expanded=False):
            try:
                ctx_preview = build_full_report_context()
                st.code(ctx_preview, language="text")
            except Exception as e:
                st.error(f"Error building context: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 10 — DATA LOG
# ══════════════════════════════════════════════════════════════════════════════
with tab_dl:
    col_a, col_b = st.columns([3, 1])

    with col_a:
        st.markdown(
            '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.2rem;'
            'font-weight:800;letter-spacing:-0.025em;margin-bottom:4px;">Load Log</div>'
            '<div style="font-size:11px;opacity:0.50;font-weight:500;margin-bottom:10px;">'
            'ETL pipeline audit trail from load_log</div>',
            unsafe_allow_html=True,
        )
        if not df_log.empty:
            # Format for display: rename columns, add row count formatting
            _log_display = df_log.copy()
            _log_display.columns = [c.replace("_", " ").title() for c in _log_display.columns]
            if "Rows Inserted" in _log_display.columns:
                _log_display["Rows Inserted"] = _log_display["Rows Inserted"].apply(
                    lambda v: f"{int(v):,}" if pd.notna(v) else "—"
                )
            if "Run At" in _log_display.columns:
                _log_display["Run At"] = pd.to_datetime(
                    _log_display["Run At"], errors="coerce"
                ).dt.strftime("%b %d, %Y %H:%M")
            st.dataframe(_log_display, use_container_width=True, hide_index=True)
            # Download button
            _csv_bytes = df_log.to_csv(index=False).encode()
            st.download_button(
                "⬇️ Download Load Log CSV", _csv_bytes,
                file_name="load_log.csv", mime="text/csv",
                use_container_width=True,
            )
        else:
            st.markdown(empty_state(
                "📋", "No load log entries found.",
                "Run the pipeline to populate the load_log table.",
            ), unsafe_allow_html=True)

    with col_b:
        st.markdown(
            '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.2rem;'
            'font-weight:800;letter-spacing:-0.025em;margin-bottom:10px;">Row Counts</div>',
            unsafe_allow_html=True,
        )
        # Only display DB table counts; exclude internal per-grain helper keys
        _TABLE_LABELS = {
            "fact_str_metrics":          "STR Metrics",
            "kpi_daily_summary":         "KPI Daily",
            "kpi_compression_quarterly": "Compression Qtrs",
            "load_log":                  "Load Log",
        }
        for _key, _label in _TABLE_LABELS.items():
            _val = counts.get(_key, "—")
            st.metric(_label, f"{_val:,}" if isinstance(_val, int) else _val)

    st.markdown("---")

    # ── Data Source Health ─────────────────────────────────────────────────────
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.2rem;'
        'font-weight:800;letter-spacing:-0.025em;margin-bottom:4px;">Data Source Health</div>'
        '<div style="font-size:11px;opacity:0.50;font-weight:500;margin-bottom:12px;">'
        'Live row counts and date coverage from analytics.sqlite</div>',
        unsafe_allow_html=True,
    )
    _sc1, _sc2 = st.columns(2)

    with _sc1:
        _d_dot   = "🟢" if str_daily_rows   > 0 else "⚫"
        _m_dot   = "🟢" if str_monthly_rows > 0 else "⚫"
        _d_range = (
            f"{df_daily['as_of_date'].min().strftime('%b %d %Y')} – "
            f"{df_daily['as_of_date'].max().strftime('%b %d %Y')}"
            if not df_daily.empty else "no data loaded"
        )
        _m_range = (
            f"{df_monthly['as_of_date'].min().strftime('%b %Y')} – "
            f"{df_monthly['as_of_date'].max().strftime('%b %Y')}"
            if not df_monthly.empty else "no data loaded"
        )
        st.markdown(source_card(
            _d_dot, "STR Daily", f"grain=daily · {_d_range}",
            f"{str_daily_rows:,}" if str_daily_rows > 0 else "—",
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _m_dot, "STR Monthly",
            f"grain=monthly · {_m_range}",
            f"{str_monthly_rows:,}" if str_monthly_rows > 0 else "—",
        ), unsafe_allow_html=True)

    with _sc2:
        _kpi_ct  = counts.get("kpi_daily_summary", 0)
        _cmp_ct  = counts.get("kpi_compression_quarterly", 0)
        _kpi_dot = "🟢" if isinstance(_kpi_ct, int) and _kpi_ct > 0 else "⚫"
        _cmp_dot = "🟢" if isinstance(_cmp_ct, int) and _cmp_ct > 0 else "⚫"
        st.markdown(source_card(
            _kpi_dot, "KPI Daily Summary", "kpi_daily_summary",
            f"{_kpi_ct:,}" if isinstance(_kpi_ct, int) else _kpi_ct,
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _cmp_dot, "Compression Quarters", "kpi_compression_quarterly",
            f"{_cmp_ct:,}" if isinstance(_cmp_ct, int) else _cmp_ct,
        ), unsafe_allow_html=True)
        _cs_total = sum(
            counts.get(t, 0) for t in [
                "costar_monthly_performance", "costar_market_snapshot",
                "costar_supply_pipeline", "costar_chain_scale_breakdown",
                "costar_competitive_set",
            ] if isinstance(counts.get(t, 0), int)
        )
        _cs_data_dot = "🟢" if _cs_total > 0 else "⚫"
        st.markdown(source_card(
            _cs_data_dot, "CoStar",
            f"source=costar · {_cs_total:,} records across 5 tables",
            f"{_cs_total:,}" if _cs_total > 0 else "—",
        ), unsafe_allow_html=True)
        _df_total_dl = counts.get("datafy_total_rows", 0)
        _df_dl_dot   = "🟢" if isinstance(_df_total_dl, int) and _df_total_dl > 0 else "⚫"
        st.markdown(source_card(
            _df_dl_dot, "Datafy",
            f"source=datafy · visitor economy · {_df_total_dl:,} records" if _df_total_dl > 0 else "source=datafy · visitor economy",
            f"{_df_total_dl:,}" if isinstance(_df_total_dl, int) and _df_total_dl > 0 else "—",
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            "⚫", "FRED / CA TOT / JWA", "external context · not yet loaded", "—",
        ), unsafe_allow_html=True)

    st.markdown("---")

    _adv_label = ""
    if grain == "Monthly":
        _yr_f = st.session_state.get("adv_filter_years", [])
        if _yr_f and not df_monthly.empty:
            _all_y = df_monthly["as_of_date"].dt.year.unique().tolist()
            if len(_yr_f) < len(_all_y):
                _adv_label = f" · years {min(_yr_f)}–{max(_yr_f)}"
    elif grain == "Daily":
        _dow_f = st.session_state.get("adv_filter_dow", [])
        if _dow_f and len(_dow_f) < 7:
            _adv_label = f" · {', '.join(_dow_f)}"

    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.2rem;'
        'font-weight:800;letter-spacing:-0.025em;margin-bottom:4px;">Recent Metric Samples</div>'
        f'<div style="font-size:11px;opacity:0.50;font-weight:500;margin-bottom:10px;">'
        f'Last 10 dates in selected window &nbsp;·&nbsp; grain={grain.lower()}{_adv_label}</div>',
        unsafe_allow_html=True,
    )
    if not df_sel.empty:
        sample = df_sel.tail(10).sort_values("as_of_date", ascending=False).copy()
        _date_fmt = "%b %Y" if grain == "Monthly" else "%b %d, %Y"
        sample["as_of_date"] = sample["as_of_date"].dt.strftime(_date_fmt)
        dcols  = [c for c in
                  ["as_of_date","revpar","adr","occupancy","supply","demand","revenue"]
                  if c in sample.columns]
        rename = {
            "as_of_date":"Date","revpar":"RevPAR ($)","adr":"ADR ($)",
            "occupancy":"Occupancy (%)","supply":"Supply (rooms)",
            "demand":"Demand (rooms)","revenue":"Revenue ($)",
        }
        st.dataframe(sample[dcols].rename(columns=rename),
                     use_container_width=True, hide_index=True)
        # Full selection download
        _dl_cols = [c for c in ["as_of_date","revpar","adr","occupancy","supply","demand","revenue"]
                    if c in df_sel.columns]
        _dl_csv = df_sel[_dl_cols].rename(columns=rename).to_csv(index=False).encode()
        st.download_button(
            f"⬇️ Download Full Selection CSV ({len(df_sel):,} rows)",
            _dl_csv, file_name=f"vdp_{grain.lower()}_{range_label.replace(' ','_')}.csv",
            mime="text/csv", use_container_width=True,
        )
    else:
        st.markdown(empty_state(
            "📭", "No data in the selected range.",
            "Adjust the date range or switch grain to see available data.",
        ), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### Compression Quarters")
    st.caption("Days per quarter above 80% / 90% occupancy from kpi_compression_quarterly")
    if not df_comp.empty:
        _comp_display = df_comp.copy()
        _comp_display.columns = [c.replace("_", " ").title() for c in _comp_display.columns]
        st.dataframe(_comp_display, use_container_width=True, hide_index=True)
        _comp_csv = df_comp.to_csv(index=False).encode()
        st.download_button(
            "⬇️ Download Compression Data CSV", _comp_csv,
            file_name="compression_quarters.csv", mime="text/csv",
            use_container_width=True,
        )
    else:
        st.markdown(empty_state(
            "📊", "No compression data.",
            "Run compute_kpis.py to populate kpi_compression_quarterly.",
        ), unsafe_allow_html=True)
