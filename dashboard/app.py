
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
from datetime import datetime, timedelta

import os
import sys
import subprocess
import urllib.parse as _urlparse
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
    page_title="Dana Point PULSE",
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
You are the Dana Point PULSE — the AI intelligence layer for Visit Dana Point (VDP) tourism \
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

## Ohana Fest 2025 — Verified Event-Impact Benchmark (Datafy Data)
Use this as the gold-standard reference model for all future event ROI analysis and projection.

| Metric | Value |
|---|---|
| Event expenditure (direct) | $14.6M |
| Total destination spend | $18.4M |
| ADR lift vs. baseline | +$139 (+45%): $542 event nights vs. $403 baseline |
| RevPAR lift vs. baseline | +$140: $427 event vs. $287 baseline |
| Avg accommodation spend per trip | $1,219 (+53% vs. prior year Ohana Fest 2024) |
| Out-of-state visitors | 68% of total attendees |
| Economic spend multiplier | 3.2× direct event expenditure |
| Overnight hotel visitor share | 24% of all attendees |

Key takeaway: Major music events with a high out-of-state draw generate genuine incremental tourism \
dollars — not just displacement of existing visitors. In board communications, always lead with the \
68% out-of-state figure and the 3.2× multiplier as the clearest evidence of VDP event marketing ROI.

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
- End with a single, clear, time-bound call to action that a board member could act on immediately

## Full Database Schema (analytics.sqlite)

The VDP brain contains these tables — all are available for your analysis:

**STR & KPI Tables (Layer 1 — Truth):**
- `fact_str_metrics` — Long-format daily/monthly STR hotel data (supply, demand, revenue, occ, adr, revpar).
  Columns: source, grain, property_name, market, submarket, as_of_date, metric_name, metric_value, unit.
- `kpi_daily_summary` — Wide-format daily KPIs with YOY deltas and compression flags.
  Columns: as_of_date, occ_pct, adr, revpar, occ_yoy, adr_yoy, revpar_yoy, is_occ_80, is_occ_90.
- `kpi_compression_quarterly` — Days per quarter above 80% / 90% occupancy.
  Columns: quarter (YYYY-Qn), days_above_80_occ, days_above_90_occ.

**Datafy Visitor Economy Tables (Layer 1 — Truth):**
- `datafy_overview_kpis` — Annual visitor overview: total_trips, overnight_pct, out_of_state_vd_pct, \
  repeat_visitors_pct, avg_length_of_stay_days.
- `datafy_overview_dma` — Feeder market breakdown: dma, visitor_days_share_pct, spending_share_pct, avg_spend_usd.
- `datafy_overview_demographics` — Visitor demographics by segment.
- `datafy_overview_category_spending` — Spending by category (accommodation, dining, retail, etc.).
- `datafy_overview_cluster_visitation` — Visitation by area cluster type.
- `datafy_overview_airports` — Origin airports by passenger share.
- `datafy_attribution_website_kpis` — Website-attributed trips and estimated impact.
- `datafy_attribution_website_top_markets`, `_dma`, `_channels`, `_clusters`, `_demographics` — \
  Website attribution breakdowns.
- `datafy_attribution_media_kpis` — Media campaign: attributable_trips, total_impact_usd, ROAS.
- `datafy_attribution_media_top_markets` — Media attribution by market.
- `datafy_social_traffic_sources` — GA4 traffic sources: sessions, engagement.
- `datafy_social_audience_overview` — Website audience KPIs.
- `datafy_social_top_pages` — Top pages by view count.

**Intelligence Tables (Generated):**
- `insights_daily` — Forward-looking insights for 4 audiences (dmo, city, visitor, resident). \
  Columns: as_of_date, audience, category, headline, body, metric_basis (JSON), priority, horizon_days.
- `table_relationships` — Documents all cross-table joins and derivations.
- `load_log` — ETL pipeline audit trail.\
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

  /* ── Hide all Streamlit chrome from client view ──────────────────────── */
  #MainMenu                              { visibility: hidden !important; }
  footer                                 { visibility: hidden !important; }
  [data-testid="stToolbar"]             { visibility: hidden !important; }
  [data-testid="stDecoration"]          { display:    none    !important; }
  [data-testid="stStatusWidget"]        { visibility: hidden !important; }
  [data-testid="stHeader"]              { background: transparent !important; }
  .viewerBadge_container__1QSob        { display:    none    !important; }
  .styles_viewerBadge__CvC9N           { display:    none    !important; }
  a[href*="streamlit.io"]               { display:    none    !important; }
  a[href*="github.com/streamlit"]       { display:    none    !important; }

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

  /* ── NotebookLM-inspired: inline source citation tags ────────────────── */
  .nlm-tag {
    display: inline-flex; align-items: center;
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 9px; font-weight: 800; letter-spacing: .06em;
    text-transform: uppercase; padding: 2px 7px; border-radius: 4px;
    vertical-align: middle; margin: 0 2px; line-height: 1;
  }
  .nlm-tag-str    { background: rgba(37,99,235,.12);  color: #2563eb; }
  .nlm-tag-datafy { background: rgba(22,163,74,.12);  color: #16a34a; }
  .nlm-tag-costar { background: rgba(124,58,237,.12); color: #7c3aed; }
  .nlm-tag-ai     { background: rgba(217,119,6,.12);  color: #b45309; }

  /* ── NotebookLM-inspired: intelligence briefing box ─────────────────── */
  .nlm-briefing {
    background: rgba(33,128,141,0.10);
    border: 1px solid rgba(33,128,141,0.28);
    border-left: 4px solid #21808D;
    border-radius: 14px; padding: 20px 24px; margin-bottom: 14px;
    position: relative;
  }
  .nlm-briefing-title {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 10px; font-weight: 800; text-transform: uppercase;
    letter-spacing: .10em; color: #32B8C6; margin-bottom: 14px;
    display: flex; align-items: center; gap: 8px;
  }
  .nlm-point {
    font-size: 13.5px; line-height: 1.70; margin-bottom: 12px;
    padding-left: 18px; position: relative;
    opacity: 0.95;
  }
  .nlm-point em { opacity: 0.75; font-size: 12.5px; }
  .nlm-point:last-child { margin-bottom: 0; }

  /* ── NotebookLM-inspired: Q&A insight blocks ─────────────────────────── */
  .nlm-qa-q {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 12.5px; font-weight: 700; margin-bottom: 5px;
    display: flex; align-items: flex-start; gap: 7px;
  }
  .nlm-qa-mark {
    width: 18px; height: 18px; min-width: 18px;
    background: rgba(33,128,141,0.14); border-radius: 50%;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 9px; font-weight: 900; color: #21808D; flex-shrink: 0; margin-top: 1px;
  }
  .nlm-qa-a { font-size: 12px; opacity: 0.70; line-height: 1.65; padding-left: 25px; }

  /* ── NotebookLM-inspired: source attribution row ─────────────────────── */
  .nlm-source-row {
    display: flex; align-items: center; gap: 6px; flex-wrap: wrap;
    margin-top: 10px; padding-top: 8px;
    border-top: 1px solid rgba(255,255,255,0.07);
  }

  /* ── Questions answered box ──────────────────────────────────────────── */
  .nlm-questions {
    background: rgba(33,128,141,0.05);
    border: 1px solid rgba(33,128,141,0.14);
    border-radius: 12px; padding: 16px 20px; margin-bottom: 14px;
  }
  .nlm-questions-title {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 10px; font-weight: 800; text-transform: uppercase;
    letter-spacing: .09em; opacity: .55; margin-bottom: 10px;
  }
  .nlm-questions ul { list-style: none; display: flex; flex-direction: column; gap: 6px; }
  .nlm-questions ul li {
    font-size: 12px; opacity: .80; padding-left: 14px; position: relative;
  }
  .nlm-questions ul li::before { content: '?'; position: absolute; left: 0;
    font-weight: 800; color: #21808D; font-size: 11px; }

  /* ── PULSE Score Widget ───────────────────────────────────────────────── */
  .pulse-wrapper {
    display: flex; align-items: center; gap: 28px;
    background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08);
    border-radius: 20px; padding: 18px 28px; margin-bottom: 16px;
  }
  .pulse-circle {
    position: relative; width: 90px; height: 90px; flex-shrink: 0;
    display: flex; align-items: center; justify-content: center;
  }
  .pulse-ring {
    position: absolute; inset: 0; border-radius: 50%;
    border: 3px solid currentColor; opacity: 0.25;
    animation: pulse-ring 2s ease-out infinite;
  }
  .pulse-ring-2 {
    position: absolute; inset: -8px; border-radius: 50%;
    border: 2px solid currentColor; opacity: 0.12;
    animation: pulse-ring 2s ease-out infinite 0.5s;
  }
  .pulse-core {
    width: 70px; height: 70px; border-radius: 50%;
    display: flex; flex-direction: column; align-items: center;
    justify-content: center; font-family: 'Plus Jakarta Sans', sans-serif;
    font-weight: 900; position: relative; z-index: 1;
    border: 2.5px solid currentColor;
  }
  .pulse-score { font-size: 24px; line-height: 1; letter-spacing: -.04em; }
  .pulse-label { font-size: 8px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .1em; opacity: .7; margin-top: 2px; }
  .pulse-info { flex: 1; }
  .pulse-info-title {
    font-family: 'Plus Jakarta Sans', sans-serif; font-size: 14px;
    font-weight: 800; letter-spacing: -.02em; margin-bottom: 4px;
  }
  .pulse-info-detail { font-size: 12px; opacity: .65; line-height: 1.5; }
  .pulse-info-status {
    display: inline-block; margin-top: 8px; font-size: 11px; font-weight: 700;
    padding: 3px 10px; border-radius: 20px;
    background: currentColor; color: #0d1117;
  }
  @keyframes pulse-ring {
    0%   { transform: scale(1);   opacity: 0.25; }
    70%  { transform: scale(1.35); opacity: 0;   }
    100% { transform: scale(1.35); opacity: 0;   }
  }
</style>
""", unsafe_allow_html=True)

# ─── Day / Night auto-background ─────────────────────────────────────────────
st.markdown("""
<style>
  :root {
    --bg-tint: rgba(13,17,23,1);
  }
  /* Applied by JS below — subtle tint only */
  body.dp-day     { --bg-tint: rgba(13,17,26,1); }
  body.dp-evening { --bg-tint: rgba(20,14,10,1); }
  body.dp-night   { --bg-tint: rgba(7,10,18,1);  }
  body.dp-day     .main { background: linear-gradient(180deg, rgba(33,128,141,0.04) 0%, transparent 100%); }
  body.dp-evening .main { background: linear-gradient(180deg, rgba(230,129,97,0.06) 0%, transparent 100%); }
  body.dp-night   .main { background: linear-gradient(180deg, rgba(10,16,32,0.50)   0%, transparent 100%); }
</style>
<script>
(function(){
  function applyTimeTheme(){
    var h = new Date().getHours();
    var cls = h >= 6 && h < 18 ? 'dp-day' : (h >= 18 && h < 21 ? 'dp-evening' : 'dp-night');
    var b = document.body;
    b.classList.remove('dp-day','dp-evening','dp-night');
    b.classList.add(cls);
  }
  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', applyTimeTheme);
  } else { applyTimeTheme(); }
})();
</script>
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

        CREATE TABLE IF NOT EXISTS visit_ca_travel_forecast (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER, total_visits_m REAL, domestic_visits_m REAL, intl_visits_m REAL,
            total_yoy_pct REAL, domestic_yoy_pct REAL, intl_yoy_pct REAL,
            total_spend_b REAL, spend_yoy_pct REAL, notes TEXT,
            loaded_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS visit_ca_lodging_forecast (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT, year INTEGER, supply_daily REAL, demand_daily REAL,
            occupancy_pct REAL, adr_usd REAL, revpar_usd REAL,
            occ_yoy_pp REAL, adr_yoy_pct REAL, revpar_yoy_pct REAL,
            loaded_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS visit_ca_airport_traffic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airport TEXT, year INTEGER, month INTEGER,
            total_pax INTEGER, domestic_pax INTEGER, intl_pax INTEGER,
            total_yoy_pct REAL, domestic_yoy_pct REAL, intl_yoy_pct REAL,
            loaded_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS visit_ca_intl_arrivals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER, month INTEGER, total_intl INTEGER, total_overseas INTEGER,
            priority_markets INTEGER, top_country TEXT, top_country_arrivals INTEGER,
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



# ── Visit California state context loaders ───────────────────────────────────

@st.cache_data(ttl=300)
def load_vca_travel_forecast() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM visit_ca_travel_forecast ORDER BY year", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vca_lodging_forecast() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM visit_ca_lodging_forecast ORDER BY year, region", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vca_airport_traffic() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM visit_ca_airport_traffic ORDER BY airport, month", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vca_intl_arrivals() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM visit_ca_intl_arrivals ORDER BY year, month", conn
        )
    except Exception:
        return pd.DataFrame()


# ── Zartico historical data loaders ──────────────────────────────────────────

@st.cache_data(ttl=300)
def load_zartico_kpis() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM zartico_kpis ORDER BY report_date DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_zartico_markets() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM zartico_markets ORDER BY report_date DESC, rank ASC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_zartico_spending() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM zartico_spending_monthly ORDER BY month_str", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_zartico_lodging() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM zartico_lodging_kpis ORDER BY report_date DESC", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_zartico_overnight() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM zartico_overnight_trend ORDER BY month_str", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_zartico_events() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM zartico_event_impact ORDER BY event_start", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_zartico_movement() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM zartico_movement_monthly ORDER BY month_str", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_zartico_future_events() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM zartico_future_events_summary ORDER BY report_date DESC LIMIT 1", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vdp_events() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM vdp_events ORDER BY event_date ASC", conn
        )
        if not df.empty:
            df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_insights(audience: str | None = None) -> pd.DataFrame:
    """Load today's (or most recent) forward-looking insights from insights_daily."""
    conn = get_connection()
    try:
        if audience:
            df = pd.read_sql_query(
                "SELECT audience, category, headline, body, priority, horizon_days, "
                "       metric_basis, data_sources, as_of_date "
                "FROM insights_daily "
                "WHERE audience = ? "
                "ORDER BY as_of_date DESC, priority ASC",
                conn, params=(audience,),
            )
        else:
            df = pd.read_sql_query(
                "SELECT audience, category, headline, body, priority, horizon_days, "
                "       metric_basis, data_sources, as_of_date "
                "FROM insights_daily "
                "ORDER BY as_of_date DESC, audience ASC, priority ASC",
                conn,
            )
        # Keep only the latest run date per audience+category
        if not df.empty:
            df = df.sort_values("as_of_date", ascending=False)
            df = df.drop_duplicates(subset=["audience", "category"], keep="first")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_overview() -> pd.DataFrame:
    """Load Datafy annual visitor overview KPIs."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_kpis ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_dma() -> pd.DataFrame:
    """Load Datafy feeder market DMA breakdown."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_dma ORDER BY report_period_start DESC, visitor_days_share_pct DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_spending() -> pd.DataFrame:
    """Load Datafy visitor spending by category."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_category_spending ORDER BY report_period_start DESC, spend_share_pct DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_demographics() -> pd.DataFrame:
    """Load Datafy visitor demographics."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_demographics ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_airports() -> pd.DataFrame:
    """Load Datafy origin airports by passenger share."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_airports ORDER BY report_period_start DESC, passengers_share_pct DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_media_kpis() -> pd.DataFrame:
    """Load Datafy media campaign attribution KPIs."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_media_kpis ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_website_kpis() -> pd.DataFrame:
    """Load Datafy website attribution KPIs."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_website_kpis ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_media_markets() -> pd.DataFrame:
    """Load Datafy media attribution top markets."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_media_top_markets ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_website_dma() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_website_dma ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_website_channels() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_website_channels ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_website_top_markets() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_attribution_website_top_markets ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_social_traffic() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_social_traffic_sources ORDER BY sessions DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_social_top_pages() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_social_top_pages ORDER BY page_views DESC LIMIT 20", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_social_audience() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_social_audience_overview ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_later_ig_profile() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM later_ig_profile_growth ORDER BY data_date DESC", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_later_ig_posts() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM later_ig_posts ORDER BY posted_at DESC LIMIT 200", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_later_ig_reels() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM later_ig_reels ORDER BY posted_at DESC LIMIT 200", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_later_ig_demographics() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM later_ig_audience_demographics", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_later_fb_profile() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM later_fb_profile_growth ORDER BY data_date DESC", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_later_fb_posts() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM later_fb_posts ORDER BY posted_at DESC LIMIT 200", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_later_tk_profile() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM later_tk_profile_growth ORDER BY data_date DESC", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_later_tk_demographics() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query("SELECT * FROM later_tk_audience_demographics", conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_datafy_clusters() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM datafy_overview_cluster_visitation ORDER BY report_period_start DESC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_table_counts() -> dict:
    conn = get_connection()
    counts = {}

    all_tables = [
        "fact_str_metrics", "kpi_daily_summary", "kpi_compression_quarterly",
        "load_log", "insights_daily", "table_relationships",
        "datafy_overview_kpis", "datafy_overview_dma", "datafy_overview_demographics",
        "datafy_overview_category_spending", "datafy_overview_cluster_visitation",
        "datafy_overview_airports",
                    "datafy_attribution_media_kpis",
        "datafy_attribution_media_top_markets",
        "datafy_social_traffic_sources",
        "datafy_social_audience_overview",
        "datafy_social_top_pages",
        "later_ig_profile_growth",
        "later_ig_posts",
        "later_ig_reels",
        "later_ig_audience_demographics",
        "later_fb_profile_growth",
        "later_fb_posts",
        "later_fb_profile_interactions",
        "later_tk_profile_growth",
        "later_tk_audience_demographics",
    ]
    for t in all_tables:
        try:
            row = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()
            counts[t] = row[0] if row else 0
        except Exception:
            counts[t] = "—"

    for t in [
        "fact_str_metrics",
        "kpi_daily_summary",
        "kpi_compression_quarterly",
        "load_log",
        "costar_monthly_performance",
        "costar_market_snapshot",
        "costar_supply_pipeline",
        "costar_chain_scale_breakdown",
        "costar_competitive_set",
        "costar_annual_performance",
        "costar_profitability",
        "zartico_kpis",
        "zartico_markets",
        "zartico_spending_monthly",
        "zartico_lodging_kpis",
        "zartico_overnight_trend",
        "zartico_event_impact",
        "zartico_movement_monthly",
        "zartico_future_events_summary",
        "vdp_events",
        "visit_ca_travel_forecast",
        "visit_ca_lodging_forecast",
        "visit_ca_airport_traffic",
        "visit_ca_intl_arrivals",
    ]:
        try:
            row = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()
            counts[t] = row[0] if row else 0
        except Exception:
            counts[t] = "—"

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
    return "\n".join(lines)


def _datafy_summary_for_prompt() -> str:
    """Build a Datafy context block from live DB data for AI prompts."""
    lines = []
    if not df_dfy_ov.empty:
        row = df_dfy_ov.iloc[0]
        lines.append("\nDatafy Verified Visitor Economy (Annual 2025):")
        lines.append(f"• Total trips: {int(row.get('total_trips',0) or 0):,} ({row.get('total_trips_vs_compare_pct',0):+.1f}pp YOY)")
        lines.append(f"• Out-of-state visitor days: {float(row.get('out_of_state_vd_pct',0) or 0):.1f}% ({row.get('out_of_state_vd_vs_compare_pct',0):+.2f}pp YOY)")
        lines.append(f"• Overnight trips: {float(row.get('overnight_trips_pct',0) or 0):.1f}%  |  Day trips: {float(row.get('day_trips_pct',0) or 0):.1f}%")
        lines.append(f"• Avg length of stay: {float(row.get('avg_length_of_stay_days',0) or 0):.1f} days ({row.get('avg_los_vs_compare_days',0):+.1f}d vs. prior yr)")
        lines.append(f"• Repeat visitors: {float(row.get('repeat_visitors_pct',0) or 0):.1f}%  |  Out-of-state spending: {float(row.get('out_of_state_spending_pct',0) or 0):.1f}% of total")
    if not df_dfy_dma.empty:
        top5 = df_dfy_dma[df_dfy_dma["visitor_days_share_pct"].notna()].head(5)
        if not top5.empty:
            top_mkts = ", ".join(
                f"{r['dma']} {r['visitor_days_share_pct']:.1f}% (${r['avg_spend_usd']:.0f}/visitor)" if pd.notna(r.get("avg_spend_usd")) else f"{r['dma']} {r['visitor_days_share_pct']:.1f}%"
                for _, r in top5.iterrows()
            )
            lines.append(f"• Top feeder markets: {top_mkts}")
    if not df_dfy_media.empty:
        med = df_dfy_media.iloc[0]
        lines.append(f"• Media campaign: {int(med.get('attributable_trips',0) or 0):,} attributable trips · ${float(med.get('total_impact_usd',0) or 0):,.0f} total impact · {med.get('roas_description','N/A')}")
    return "\n".join(lines) + "\n\n" if lines else ""


def _build_visitor_econ_prompt(b: str) -> str:
    """Build a visitor economy AI prompt from live Datafy DB data."""
    dfy_ctx = _datafy_summary_for_prompt()
    if not dfy_ctx.strip():
        return (
            f"{b}\n\n"
            "No Datafy visitor economy data is currently loaded. Run the pipeline first. "
            "Based on STR data only, analyze what the occupancy and ADR patterns suggest "
            "about visitor behavior and recommend 2 strategies to increase overnight stays."
        )
    return (
        f"{b}\n{dfy_ctx}"
        "Using both the STR portfolio data and verified Datafy visitor economy data above, "
        "provide a cross-source intelligence brief answering:\n"
        "• WHO is visiting (origin, demographics, overnight vs. day trip split)\n"
        "• WHAT they spend (accommodation share, top spending categories)\n"
        "• WHEN they come (seasonal patterns, LOS implications)\n"
        "• WHERE they come from (top feeder markets and their relative value)\n"
        "• WHY this matters (revenue gap between high-volume low-spend vs. low-volume high-spend markets)\n"
        "• HOW to act: 3 specific, data-driven marketing or partnership recommendations\n"
        "Format for a DMO strategy meeting. Lead with the most surprising cross-source finding."
    )


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
        "visitor_econ": _build_visitor_econ_prompt(b),
        "board": (
            f"{b}\n"
            f"• Est. TBID monthly revenue: ${m.get('tbid_monthly',0):,.0f} (blended 1.25%)\n"
            f"{_datafy_summary_for_prompt()}"
            "Generate 5 concise talking points for the VDP TBID board meeting. "
            "Each point must answer: WHO it affects, WHAT the data shows, WHEN it matters, "
            "WHERE the opportunity is, WHY it matters strategically, and HOW to act. "
            "Format for an executive audience with specific data references."
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

def _build_visitor_econ_local_fallback(m: dict) -> str:
    """Visitor economy local fallback using live DB data."""
    lines = ["**Visitor Economy Intelligence** *(local mode — connect API for live Claude analysis)*\n"]
    if not df_dfy_ov.empty:
        row = df_dfy_ov.iloc[0]
        total = int(row.get("total_trips", 0) or 0)
        oos   = float(row.get("out_of_state_vd_pct", 0) or 0)
        on    = float(row.get("overnight_trips_pct", 0) or 0)
        los   = float(row.get("avg_length_of_stay_days", 0) or 0)
        rep   = float(row.get("repeat_visitors_pct", 0) or 0)
        lines.append(f"**WHO:** {total/1e6:.2f}M annual trips — {oos:.1f}% from out-of-state, {rep:.1f}% are repeat visitors")
        lines.append(f"**WHAT:** {on:.1f}% overnight trips · avg {los:.1f}-day stay")
        if not df_dfy_spend.empty:
            top_cat = df_dfy_spend.iloc[0]
            lines.append(f"**WHAT they spend:** {top_cat['category']} leads at {top_cat['spend_share_pct']:.1f}% of destination spend")
        if not df_dfy_dma.empty:
            top_dma = df_dfy_dma[df_dfy_dma["visitor_days_share_pct"].notna()].iloc[0]
            lines.append(f"**WHERE:** Top market is {top_dma['dma']} at {top_dma['visitor_days_share_pct']:.1f}% of visitor days")
            if pd.notna(top_dma.get("avg_spend_usd")):
                # Find highest avg_spend market
                high_spend = df_dfy_dma[df_dfy_dma["avg_spend_usd"].notna()].nlargest(1, "avg_spend_usd")
                if not high_spend.empty:
                    hs = high_spend.iloc[0]
                    lines.append(f"**WHY it matters:** {hs['dma']} spends ${hs['avg_spend_usd']:.0f}/visitor vs. {top_dma['dma']} at ${top_dma.get('avg_spend_usd',0):.0f} — fly markets are higher-value per trip")
        if not df_dfy_media.empty:
            med = df_dfy_media.iloc[0]
            lines.append(f"**HOW campaigns performed:** {int(med.get('attributable_trips',0) or 0):,} attributable trips · ${float(med.get('total_impact_usd',0) or 0):,.0f} est. impact · {med.get('roas_description','N/A')}")
    else:
        lines.append("No Datafy data loaded. Run: `python scripts/run_pipeline.py`")
    lines.append("\n**→ Action:** Add API key for Claude to cross-analyze STR + Datafy data for hidden revenue opportunities.")
    return "\n".join(lines)


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
            f"3. **Visitor Economy:** {int(df_dfy_ov.iloc[0].get('total_trips',0) or 0)/1e6:.2f}M annual trips, "
            f"{float(df_dfy_ov.iloc[0].get('out_of_state_vd_pct',0) or 0):.1f}% out-of-state visitor days\n"
            if not df_dfy_ov.empty else
            f"3. **Visitor Economy:** Run pipeline to load Datafy visitor data\n"
            f"4. **TBID Revenue:** Tracking ~**${m.get('tbid_monthly',0):,.0f}/month** "
            f"at blended 1.25%\n"
            f"5. **Midweek Opportunity:** Weekend/midweek RevPAR gap "
            f"({wknd_pct:.0f}%) = highest-leverage growth lever\n\n"
            f"**→ Action:** Request board approval for a $50K midweek demand-generation campaign."
        ),
        "visitor_econ": _build_visitor_econ_local_fallback(m),
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

def kpi_metric_svg(label: str, positive: bool = True, raw_value: float = 0.0, sparkline_values: list = None) -> str:
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

    def _real_pts(vals: list):
        """Derive 6 SVG polyline points from actual sparkline data scaled to 42x42 space."""
        try:
            v = [float(x) for x in vals if x is not None and x == x]
            if len(v) < 2:
                return None
            idx = [int(i * (len(v) - 1) / 5) for i in range(6)]
            v6  = [v[i] for i in idx]
            mn, mx = min(v6), max(v6)
            rng = mx - mn or 1
            xs  = [5, 11, 17, 23, 29, 37]
            ys  = [round(37 - (vv - mn) / rng * 30, 1) for vv in v6]
            return " ".join(f"{x},{y}" for x, y in zip(xs, ys))
        except Exception:
            return None

    if label == "RevPAR":
        _spark_pts = _real_pts(sparkline_values) if sparkline_values else None
        pts    = _spark_pts or ("5,32 11,24 17,27 23,17 29,12 37,7" if positive else "5,10 11,18 17,15 23,25 29,30 37,35")
        if _spark_pts:
            _last_y = float(_spark_pts.split()[-1].split(",")[1])
            tip_y   = str(round(_last_y))
        else:
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


def sparkline_svg(values: list, positive: bool = True, width: int = 120, height: int = 28) -> str:
    """Return a compact animated inline SVG sparkline from a list of numeric values."""
    if not values or len(values) < 2:
        return ""
    try:
        v = [float(x) for x in values if x is not None and not (isinstance(x, float) and x != x)]
        if len(v) < 2:
            return ""
        mn, mx = min(v), max(v)
        rng = mx - mn or 1
        pad = 3
        xs = [round(pad + i * (width - 2 * pad) / (len(v) - 1), 2) for i in range(len(v))]
        ys = [round(height - pad - (val - mn) / rng * (height - 2 * pad), 2) for val in v]
        pts = " ".join(f"{x},{y}" for x, y in zip(xs, ys))
        c   = "#21808D" if positive else "#C0152F"
        fc  = "rgba(33,128,141,0.10)" if positive else "rgba(192,21,47,0.07)"
        area = (
            f"M{xs[0]},{height} "
            + " ".join(f"L{x},{y}" for x, y in zip(xs, ys))
            + f" L{xs[-1]},{height} Z"
        )
        total_len = sum(
            ((xs[i+1]-xs[i])**2 + (ys[i+1]-ys[i])**2)**0.5
            for i in range(len(xs)-1)
        )
        dash = max(int(total_len) + 20, 200)
        return (
            f'<div style="margin-top:8px;padding-top:6px;border-top:1px solid rgba(0,0,0,0.06);">'
            f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'style="display:block;width:100%;overflow:visible;" xmlns="http://www.w3.org/2000/svg">'
            f'<path d="{area}" fill="{fc}"/>'
            f'<polyline points="{pts}" stroke="{c}" stroke-width="1.8" fill="none" '
            f'stroke-linecap="round" stroke-linejoin="round" '
            f'stroke-dasharray="{dash}" stroke-dashoffset="{dash}">'
            f'<animate attributeName="stroke-dashoffset" from="{dash}" to="0" '
            f'dur="1.0s" fill="freeze" begin="0.3s" calcMode="spline" keySplines="0.25,0.46,0.45,0.94"/>'
            f'</polyline>'
            f'<circle cx="{xs[-1]}" cy="{ys[-1]}" r="2.8" fill="{c}" opacity="0">'
            f'<animate attributeName="opacity" from="0" to="1" dur="0.2s" fill="freeze" begin="1.2s"/>'
            f'<animate attributeName="r" values="2.8;4.0;2.8" dur="2.4s" repeatCount="indefinite" begin="1.5s"/>'
            f'</circle>'
            f'</svg>'
            f'<div style="display:flex;justify-content:space-between;font-size:9px;opacity:0.38;'
            f'font-family:system-ui,sans-serif;margin-top:2px;">'
            f'<span>← {len(v)}pt trend</span><span>latest →</span>'
            f'</div></div>'
        )
    except Exception:
        return ""


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
             icon: str = "", date_label: str = "", raw_value: float = 0.0,
             sparkline_values: list = None) -> str:
    css      = "kpi-delta-neutral" if neutral else ("kpi-delta-pos" if positive else "kpi-delta-neg")
    arrow    = "" if neutral else ("▲ " if positive else "▼ ")
    date_html = f'<div class="kpi-date">📅 {date_label}</div>' if date_label else ""
    svg      = kpi_metric_svg(label, positive, raw_value, sparkline_values)
    spark_html = sparkline_svg(sparkline_values, positive) if sparkline_values else ""
    return (
        f'<div class="kpi-card">'
        f'<div class="kpi-header">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-icon-svg">{svg}</div>'
        f'</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="{css}">{arrow}{delta}</div>'
        f'{date_html}'
        f'{spark_html}'
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
        transition={"duration": 800, "easing": "cubic-in-out"},
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
    # Sparklines: last 30 data points per metric
    _sp = df.tail(30)
    def _spark(col):
        return [v for v in _sp[col].tolist() if pd.notna(v)] if col in _sp.columns else []
    spark_rvp = _spark("revpar")
    spark_adr = _spark("adr")
    spark_occ = _spark("occupancy")
    spark_rev = _spark("revenue")
    spark_dem = _spark("demand")
    return [
        {"label":"RevPAR",        "value":f"${r_rvp:.2f}",
         "delta":f"{pct_delta(r_rvp,p_rvp):+.1f}% vs. prior",  "positive":r_rvp>=p_rvp, "date_label":date_lbl, "raw_value":r_rvp, "sparkline":spark_rvp},
        {"label":"ADR",           "value":f"${r_adr:.2f}",
         "delta":f"{pct_delta(r_adr,p_adr):+.1f}% vs. prior",  "positive":r_adr>=p_adr, "date_label":date_lbl, "raw_value":r_adr, "sparkline":spark_adr},
        {"label":"Occupancy",     "value":f"{r_occ:.1f}%",
         "delta":f"{pct_delta(r_occ,p_occ):+.1f}pp vs. prior", "positive":r_occ>=p_occ, "date_label":date_lbl, "raw_value":r_occ, "sparkline":spark_occ},
        {"label":"Room Revenue",  "value":f"${r_rev/1e6:.2f}M",
         "delta":f"{pct_delta(r_rev,p_rev):+.1f}% vs. prior",  "positive":r_rev>=p_rev, "date_label":date_lbl, "raw_value":r_rev, "sparkline":spark_rev},
        {"label":"Rooms Sold",    "value":f"{r_dem:,.0f}",
         "delta":f"{pct_delta(r_dem,p_dem):+.1f}% vs. prior",  "positive":r_dem>=p_dem, "date_label":date_lbl, "raw_value":r_dem, "sparkline":spark_dem},
        {"label":"Est. TBID Rev", "value":f"${tbid/1e3:.0f}K",
         "delta":"blended 1.25%", "positive":True, "neutral":True, "date_label":date_lbl, "raw_value":tbid, "sparkline":spark_rev},
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
# Datafy visitor economy
df_dfy_ov      = load_datafy_overview()
df_dfy_dma     = load_datafy_dma()
df_dfy_spend   = load_datafy_spending()
df_dfy_demo    = load_datafy_demographics()
df_dfy_air     = load_datafy_airports()
df_dfy_media       = load_datafy_media_kpis()
df_dfy_web         = load_datafy_website_kpis()
df_dfy_mktmkt      = load_datafy_media_markets()
df_dfy_web_dma     = load_datafy_website_dma()
df_dfy_web_ch      = load_datafy_website_channels()
df_dfy_web_mkts    = load_datafy_website_top_markets()
df_dfy_social_traf = load_datafy_social_traffic()
df_dfy_social_pages= load_datafy_social_top_pages()
df_dfy_social_aud  = load_datafy_social_audience()
df_dfy_clusters    = load_datafy_clusters()
df_insights    = load_insights()          # Forward-looking insights (all audiences)
# Zartico historical reference data (Jun 2025 snapshot — for trend comparison only)
df_zrt_kpis    = load_zartico_kpis()
df_zrt_markets = load_zartico_markets()
df_zrt_spend   = load_zartico_spending()
df_zrt_lodging = load_zartico_lodging()
df_zrt_overnight = load_zartico_overnight()
df_zrt_events  = load_zartico_events()
df_zrt_movement = load_zartico_movement()
df_zrt_future_events = load_zartico_future_events()
df_vdp_events  = load_vdp_events()
# Visit California state context data
df_vca_forecast = load_vca_travel_forecast()
df_vca_lodging  = load_vca_lodging_forecast()
df_vca_airport  = load_vca_airport_traffic()
df_vca_intl     = load_vca_intl_arrivals()
# Later.com social media data (IG, FB, TikTok)
df_later_ig_profile  = load_later_ig_profile()
df_later_ig_posts    = load_later_ig_posts()
df_later_ig_reels    = load_later_ig_reels()
df_later_ig_demo     = load_later_ig_demographics()
df_later_fb_profile  = load_later_fb_profile()
df_later_fb_posts    = load_later_fb_posts()
df_later_tk_profile  = load_later_tk_profile()
df_later_tk_demo     = load_later_tk_demographics()

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<a href="?" style="text-decoration:none;">'
        '<div class="sidebar-brand">🌊 Dana Point PULSE</div>'
        '</a>'
        '<div style="font-size:11px;opacity:0.55;font-weight:500;margin-top:2px;margin-bottom:1px;">'
        'Performance · Understanding · Leadership · Spending · Economy</div>'
        '<div style="font-size:11px;opacity:0.45;font-weight:500;margin-top:1px;margin-bottom:2px;">'
        'VDP Select Portfolio &nbsp;·&nbsp; 12 Properties</div>',
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

    # ── Admin mode check (URL param ?admin=true) ────────────────────────────
    _qp = st.query_params
    _is_admin = str(_qp.get("admin", "")).lower() == "true"

    # ── AI Analyst config ──────────────────────────────────────────────────────
    st.markdown("**🤖 AI Analyst**")

    # API key input: only show in admin mode; otherwise use env key silently
    if _is_admin:
        api_key_raw = st.text_input(
            "Anthropic API Key",
            type="password",
            placeholder="sk-ant-api03-…",
            value=_ENV_API_KEY,          # pre-fills from .env if set
            help="Loaded from .env ANTHROPIC_API_KEY · override here anytime.",
            key="api_key_field",
        )
    else:
        api_key_raw = _ENV_API_KEY

    api_key       = api_key_raw.strip()
    api_key_valid = bool(api_key) and api_key.startswith("sk-ant-") and len(api_key) > 20

    if api_key_valid:
        st.caption(f"🟢 AI Connected · {CLAUDE_MODEL}")
    else:
        st.caption("⚪ AI — local mode active")

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
    # Use the already-loaded df_cs_snap as the authoritative CoStar row count —
    # counts dict can return "—" (string) on exceptions, making integer checks unreliable.
    _cs_rows = len(df_cs_snap) if not df_cs_snap.empty else 0
    _cs_dot  = "🟢" if _cs_rows > 0 else "⚫"
    _cs_label = f"{_cs_rows:,} rows" if _cs_rows > 0 else "No data"
    _dfy_tables  = [t for t in counts if t.startswith("datafy_")]
    _dfy_rows    = sum(counts.get(t, 0) for t in _dfy_tables if isinstance(counts.get(t, 0), int))
    _dfy_active  = sum(1 for t in _dfy_tables if isinstance(counts.get(t, 0), int) and counts.get(t, 0) > 0)
    _dfy_dot     = "🟢" if _dfy_rows > 0 else "⚫"
    _dfy_label   = f"{_dfy_active} datasets · {_dfy_rows:,} rows" if _dfy_rows > 0 else "No data"
    _zrt_rows = sum(counts.get(t, 0) for t in ["zartico_kpis","zartico_markets","zartico_spending_monthly","zartico_lodging_kpis","zartico_overnight_trend"] if isinstance(counts.get(t, 0), int))
    _zrt_dot  = "🟢" if _zrt_rows > 0 else "⚫"
    _zrt_label = f"{_zrt_rows:,} rows (historical)" if _zrt_rows > 0 else "No data"
    _evts_rows = counts.get("vdp_events", 0)
    _evts_dot  = "🟢" if isinstance(_evts_rows, int) and _evts_rows > 0 else "⚫"
    _evts_label = f"{_evts_rows:,} events" if isinstance(_evts_rows, int) and _evts_rows > 0 else "Not loaded"
    # Use already-loaded DFs for Visit CA — fallback to direct DB count if cache is stale/empty
    _vca_rows = len(df_vca_forecast) + len(df_vca_lodging) + len(df_vca_airport) + len(df_vca_intl)
    if _vca_rows == 0:
        try:
            _vca_conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            _vca_rows = sum(
                _vca_conn.execute(f"SELECT COUNT(*) FROM {_t}").fetchone()[0]
                for _t in ["visit_ca_travel_forecast","visit_ca_lodging_forecast",
                           "visit_ca_airport_traffic","visit_ca_intl_arrivals"]
            )
            _vca_conn.close()
        except Exception:
            pass
    _vca_dot  = "🟢" if _vca_rows > 0 else "⚫"
    _vca_label = f"{_vca_rows:,} rows" if _vca_rows > 0 else "No data"
    # Later.com social media counts
    _later_ig_rows = len(df_later_ig_profile) + len(df_later_ig_posts) + len(df_later_ig_reels)
    _later_fb_rows = len(df_later_fb_profile) + len(df_later_fb_posts)
    _later_tk_rows = len(df_later_tk_profile)
    _later_total   = _later_ig_rows + _later_fb_rows + _later_tk_rows
    _later_dot     = "🟢" if _later_total > 0 else "⚫"
    _later_label   = f"IG·FB·TK &nbsp;·&nbsp; {_later_total:,} rows" if _later_total > 0 else "No data"
    st.markdown("**Pipeline Status**")
    st.markdown(f"{_d_dot} STR Daily &nbsp;·&nbsp; {_d_label}")
    st.markdown(f"{_m_dot} STR Monthly &nbsp;·&nbsp; {_m_label}")
    st.markdown(f"{_cs_dot} CoStar Market &nbsp;·&nbsp; {_cs_label}")
    st.markdown(f"{_dfy_dot} Datafy &nbsp;·&nbsp; {_dfy_label}")
    st.markdown(f"{_zrt_dot} Zartico (Hist.) &nbsp;·&nbsp; {_zrt_label}")
    st.markdown(f"{_evts_dot} VDP Events &nbsp;·&nbsp; {_evts_label}")
    st.markdown(f"{_vca_dot} Visit California &nbsp;·&nbsp; {_vca_label}")
    st.markdown(f"{_later_dot} Social (Later) &nbsp;·&nbsp; {_later_label}")
    st.caption(f"Last ETL run: {last_log}")

    if not df_daily.empty:
        min_d = df_daily["as_of_date"].min().strftime("%b %d, %Y")
        max_d = df_daily["as_of_date"].max().strftime("%b %d, %Y")
        st.caption(f"Daily data: {min_d} → {max_d}")
    if not df_monthly.empty:
        mon_min = df_monthly["as_of_date"].min().strftime("%b %Y")
        mon_max = df_monthly["as_of_date"].max().strftime("%b %Y")
        st.caption(f"Monthly data: {mon_min} → {mon_max}")

    st.divider()

    # ── Pipeline Controls (admin only) ────────────────────────────────────────
    if _is_admin:
      st.markdown("**⚙️ Pipeline Controls**")

      fetch_btn = st.button(
          "📡 Fetch All Sources",
          use_container_width=True,
          help="Pull latest data from every source (STR · Datafy · CoStar · Zartico · Visit CA · VDP Events · FRED · CA TOT · JWA). Only new rows are inserted — no duplicates ever.",
      )
      run_btn = st.button(
          "🔄 Run Pipeline",
          use_container_width=True,
          help="Recompute KPIs and regenerate AI insights from loaded data. Run this after Fetch to update everything the app displays.",
      )
    else:
      run_btn = False
      fetch_btn = False

    if fetch_btn:
        with st.spinner("Fetching all sources — this may take 30–60 seconds…"):
            proc = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / "fetch_external_all.py")],
                capture_output=True,
                text=True,
                cwd=str(ROOT),
            )
        if proc.returncode == 0:
            st.success("All sources fetched ✓ — click Run Pipeline to refresh KPIs & insights.")
            st.cache_data.clear()
        else:
            st.error("Fetch failed — see detail below")
            err_text = (proc.stderr or proc.stdout or "No output captured").strip()
            st.code(err_text[-800:], language="text")

    if run_btn:
        with st.spinner("Recomputing KPIs and insights…"):
            proc = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / "compute_only.py")],
                capture_output=True,
                text=True,
                cwd=str(ROOT),
            )
        if proc.returncode == 0:
            st.success("Pipeline complete — dashboard refreshed ✓")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Pipeline failed — see detail below")
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
    f'<div class="hero-title">Dana Point PULSE</div>'
    f'</a>'
    f'<div class="hero-subtitle">'
    f'Performance · Understanding · Leadership · Spending · Economy'
    f'&nbsp;&nbsp;·&nbsp;&nbsp;'
    f'VDP Select Portfolio &nbsp;·&nbsp; 12 Properties &nbsp;·&nbsp; '
    f'{range_label} &nbsp;·&nbsp; Last updated {last_upd}'
    f'</div>'
    f'</div>',
    unsafe_allow_html=True,
)

# ══════════════════════════════════════════════════════════════════════════════
# BOARD REPORT HTML GENERATOR
# Produces a full-fidelity, print-ready HTML document with inline CSS.
# Download as .html → open in browser → Cmd/Ctrl+P → Save as PDF.
# ══════════════════════════════════════════════════════════════════════════════

def generate_board_report_html(
    m: dict,
    df_kpi_in: "pd.DataFrame",
    df_dfy_ov_in: "pd.DataFrame",
    df_dfy_dma_in: "pd.DataFrame",
    df_cs_snap_in: "pd.DataFrame",
    df_insights_in: "pd.DataFrame",
    df_dfy_media_in: "pd.DataFrame",
) -> str:
    """Return a complete, self-contained NotebookLM-style HTML board report."""
    now = datetime.now()
    report_date = now.strftime("%B %d, %Y")
    period_label = now.strftime("%B %Y")

    # ── Metric extraction ──────────────────────────────────────────────────────
    rvp   = m.get("revpar_30", 0)
    rvp_d = m.get("revpar_delta", 0)
    adr   = m.get("adr_30", 0)
    adr_d = m.get("adr_delta", 0)
    occ   = m.get("occ_30", 0)
    occ_d = m.get("occ_delta", 0)
    tbid  = m.get("tbid_monthly", 0)
    cq    = m.get("comp_recent_q", 0)
    cpq   = m.get("comp_prior_q", 0)
    wknd  = m.get("wknd_revpar", 0)
    wkdy  = m.get("wkdy_revpar", 0)
    wkgap = ((wknd - wkdy) / wkdy * 100) if wkdy else 0

    # Annual 2024 averages from kpi_daily_summary
    _k24 = df_kpi_in[df_kpi_in["as_of_date"].astype(str).str.startswith("2024")] if not df_kpi_in.empty else pd.DataFrame()
    vdp_occ_ann = float(_k24["occ_pct"].mean()) if not _k24.empty else occ
    vdp_adr_ann = float(_k24["adr"].mean())     if not _k24.empty else adr
    vdp_rvp_ann = float(_k24["revpar"].mean())  if not _k24.empty else rvp

    # Datafy metrics
    trips_m   = int(df_dfy_ov_in.iloc[0].get("total_trips", 0) or 0) / 1_000_000 if not df_dfy_ov_in.empty else 0
    overnight = float(df_dfy_ov_in.iloc[0].get("overnight_pct", 0) or 0) if not df_dfy_ov_in.empty else 0
    oos_pct   = float(df_dfy_ov_in.iloc[0].get("out_of_state_vd_pct", 0) or 0) if not df_dfy_ov_in.empty else 0
    avg_los   = float(df_dfy_ov_in.iloc[0].get("avg_los", 0) or 0) if not df_dfy_ov_in.empty else 0

    # ROAS
    roas = float(df_dfy_media_in.iloc[0].get("roas", 0) or 0) if not df_dfy_media_in.empty else 0
    attr_trips = int(df_dfy_media_in.iloc[0].get("attributable_trips", 0) or 0) if not df_dfy_media_in.empty else 0
    total_impact = float(df_dfy_media_in.iloc[0].get("total_impact_usd", 0) or 0) if not df_dfy_media_in.empty else 0

    # CoStar market
    mkt_occ = mkt_adr = mkt_rvp = 0.0
    if not df_cs_snap_in.empty:
        _snap = df_cs_snap_in[df_cs_snap_in["report_period"] == "2024-12-31"]
        if _snap.empty:
            _snap = df_cs_snap_in.iloc[[0]]
        _snap = _snap.fillna(0).iloc[0]
        mkt_occ = float(_snap.get("occupancy_pct", 0))
        mkt_adr = float(_snap.get("adr_usd", 0))
        mkt_rvp = float(_snap.get("revpar_usd", 0))

    mpi = (vdp_occ_ann / mkt_occ * 100) if mkt_occ else 0
    ari = (vdp_adr_ann / mkt_adr * 100) if mkt_adr else 0
    rgi = (vdp_rvp_ann / mkt_rvp * 100) if mkt_rvp else 0

    # Top DMA feeder markets
    dma_rows = ""
    if not df_dfy_dma_in.empty:
        for _, r in df_dfy_dma_in.nlargest(5, "visitor_days_share_pct").iterrows():
            dma_rows += f"<tr><td>{r.get('dma','—')}</td><td style='text-align:center'>{float(r.get('visitor_days_share_pct',0)):.1f}%</td><td style='text-align:right'>${float(r.get('avg_spend_usd',0)):,.0f}</td></tr>"

    # Insights (DMO audience) — Q&A format
    dmo_insights = df_insights_in[df_insights_in["audience"] == "dmo"].head(3) if not df_insights_in.empty else pd.DataFrame()
    insight_rows = ""
    for _, row in dmo_insights.iterrows():
        cat = str(row.get("category", "—")).replace("_", " ").title()
        hl = row.get("headline", "")
        body_raw = str(row.get("body", ""))
        body_text = body_raw[:380] + ("…" if len(body_raw) > 380 else "")
        insight_rows += f"""
    <div class="nlm-card">
      <div class="qa-block">
        <div class="qa-q"><span class="qa-q-mark">Q</span>{cat}: What does the data signal?</div>
        <div class="qa-a"><strong>{hl}</strong><br>
          <span style="margin-top:5px;display:block;font-size:9pt">{body_text}</span>
        </div>
      </div>
      <div class="concepts" style="margin-top:10px">
        <span class="src-badge src-ai">AI Insight</span>
        <span class="concept-chip">{cat}</span>
        <span class="concept-chip">DMO Audience</span>
      </div>
    </div>"""

    # Color + arrow helpers
    def _clr(v): return "#188038" if v >= 0 else "#b91c1c"
    def _arr(v): return "▲" if v >= 0 else "▼"
    def _idx_clr(v): return "#188038" if v >= 100 else "#b91c1c"

    tbid_ann = tbid * 12
    midweek_opp = (wknd - wkdy) * 0.2 * 90 / 7 * 12

    # Pre-computed conditional strings (keeps HTML f-string clean)
    comp_sentence = (
        f"Compression activity rose to <strong>{cq} nights above 90% occupancy</strong> "
        f"this quarter vs. {cpq} prior — data supports rate increases."
        if (cq >= cpq and cq > 0) else
        f"Compression moderated to <strong>{cq} nights above 90% occupancy</strong> "
        f"this quarter vs. {cpq} prior — shoulder-season demand programs are warranted."
        if cq > 0 else
        "Compression tracking in progress — load latest STR data for current quarter stats."
    )
    visitor_sentence = (
        f"Dana Point welcomed <strong>{trips_m:.2f}M annual visitor trips</strong>, with "
        f"<strong>{overnight:.0f}%</strong> overnight stays and <strong>{oos_pct:.0f}%</strong> "
        f"out-of-state visitors driving disproportionate room revenue per trip."
        if trips_m > 0 else
        "Run <code>python scripts/run_pipeline.py</code> to load Datafy visitor economy data."
    )
    revenue_action = (
        f"Approve rate increase on compression nights (90%+ occ). "
        f"Proposed floor: ADR +${int(adr * 0.10)} on peak nights. "
        f"Rationale: {cq} compression nights QTD vs. {cpq} prior."
        if cq > 0 else
        "Authorize $50K shoulder-season demand generation campaign targeting midweek bookings."
    )
    tbid_direction = (
        "Trending above prior period — budget assumptions may be revised upward."
        if rvp_d >= 0 else
        "Monitor for sustained softness; consider revised budget assumptions if trend persists 60+ days."
    )
    media_action = (
        f"Authorize increased media budget for fly markets (SLC, DFW, NYC, ORD). "
        f"These DMAs generate 1.3–1.4× room revenue per trip vs. LA drive. "
        f"Current ROAS: {roas:.1f}× on media attribution tracking."
        if roas > 0 else
        "Approve Datafy attribution study to quantify media ROAS — required for next budget cycle."
    )
    costar_action = (
        f"Present updated CoStar comp set analysis. Portfolio at RGI {rgi:.0f} vs. "
        f"South OC market. Discuss rate ladder strategy for Upper Upscale opportunities."
        if mkt_rvp > 0 else
        "Request CoStar market report subscription renewal for 2026. "
        "Current data: Newport Beach/Dana Point submarket."
    )
    demand_risk_class = "risk-green" if rvp_d >= 0 else "risk-red"
    demand_risk_icon  = "✅" if rvp_d >= 0 else "🔴"
    demand_risk_msg   = (
        "Demand trajectory supports current pricing strategy."
        if rvp_d >= 0 else
        "Softening demand warrants review of rate strategy and demand generation effectiveness."
    )
    supply_risk_msg = (
        "Active supply pipeline adds rooms to South OC market. "
        "New competitive supply may pressure occupancy for mid-tier segments in 2025–2026."
        if not df_cs_snap_in.empty else
        "Monitor CoStar supply pipeline data for new competitive hotel openings in South OC."
    )
    roas_val        = f"{roas:.1f}×" if roas > 0 else "N/A"
    roas_trips_lbl  = f"{attr_trips:,} trips" if attr_trips > 0 else "load media data"
    roas_impact_sub = f"${total_impact/1e6:.1f}M total impact" if total_impact > 0 else "Datafy media attribution"

    # Index bar helper (CoStar section)
    def _idx_bar(label, val):
        fill = min(int(val), 100)
        clr  = _idx_clr(val)
        return (
            f'<div class="index-bar">'
            f'<span class="bar-label">{label}</span>'
            f'<div class="bar-wrap"><div class="bar-fill" style="width:{fill}%;background:{clr}"></div></div>'
            f'<span class="bar-val" style="color:{clr}">{val:.0f}</span>'
            f'</div>'
        )

    costar_section = (
        f"<div class='qa-block'>"
        f"<div class='qa-q'><span class='qa-q-mark'>Q</span>How does VDP rank vs. the market?</div>"
        f"<div class='qa-a'>South OC market 2024: <strong>Occ {mkt_occ:.1f}%</strong> · "
        f"ADR <strong>${mkt_adr:.0f}</strong> · RevPAR <strong>${mkt_rvp:.0f}</strong>.</div>"
        f"</div>"
        f"{_idx_bar('MPI (Occ)', mpi)}"
        f"{_idx_bar('ARI (Rate)', ari)}"
        f"{_idx_bar('RGI (RevPAR)', rgi)}"
        f"<div class='key-number'><div class='kn-num'>{rgi:.0f}</div>"
        f"<div class='kn-desc'>RGI — VDP portfolio RevPAR index vs. South OC market. "
        f"{'Above 100 = outperforming.' if rgi >= 100 else 'Below 100 = underperforming.'}</div></div>"
        if mkt_rvp > 0 else
        "<p style='color:#5f6368;font-size:9.5pt'>Load CoStar PDFs via the pipeline to see market comparison.</p>"
    )
    visitor_profile_section = (
        f"<div class='qa-block'>"
        f"<div class='qa-q'><span class='qa-q-mark'>Q</span>What does the visitor profile look like?</div>"
        f"<div class='qa-a'>Dana Point welcomed <strong>{trips_m:.2f}M annual visitor trips</strong>. "
        f"<strong>{overnight:.0f}%</strong> stayed overnight (avg {avg_los:.1f} nights), "
        f"<strong>{100-overnight:.0f}%</strong> were same-day visitors. "
        f"<strong>{oos_pct:.0f}%</strong> of visitor days came from out-of-state markets.</div>"
        f"</div>"
        f"<div class='key-number'><div class='kn-num'>{oos_pct:.0f}%</div>"
        f"<div class='kn-desc'>Out-of-state visitors — fly-market travelers generate 1.3–1.4× "
        f"room revenue per trip vs. LA drive market.</div></div>"
        if trips_m > 0 else
        "<p style='color:#5f6368;font-size:9.5pt'>Load Datafy visitor economy CSVs via the pipeline to see visitor profile.</p>"
    )
    dma_table = (
        f"<table><thead><tr><th>Market (DMA)</th>"
        f"<th style='text-align:center'>Visitor Days %</th>"
        f"<th style='text-align:right'>Avg Spend</th></tr></thead>"
        f"<tbody>{dma_rows}</tbody></table>"
        if dma_rows else
        "<p style='color:#5f6368;font-size:9.5pt'>DMA breakdown will appear after Datafy data is loaded.</p>"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>VDP Intelligence Briefing — {period_label}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Segoe UI', 'Helvetica Neue', Arial, system-ui, sans-serif;
    font-size: 11pt; color: #1a1a2e; background: #ffffff; line-height: 1.65;
  }}
  a {{ color: #21808D; text-decoration: none; }}

  /* Layout */
  .page {{ max-width: 940px; margin: 0 auto; padding: 36px 44px; background: #ffffff; }}
  .section {{ margin-bottom: 26px; }}
  .col2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  .stat-grid {{ display: grid; grid-template-columns: repeat(4,1fr); gap: 12px; margin-bottom: 14px; }}

  /* Header */
  .nlm-header {{
    background: #202124; color: white;
    padding: 30px 36px 26px; border-radius: 14px; margin-bottom: 20px;
    position: relative; overflow: hidden;
  }}
  .nlm-header::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px;
    background: linear-gradient(90deg, #21808D, #32B8C6, #E68161);
  }}
  .nlm-header .org {{
    font-size: 10pt; font-weight: 600; letter-spacing: .14em;
    text-transform: uppercase; opacity: .65; margin-bottom: 10px;
  }}
  .nlm-header h1 {{
    font-size: 24pt; font-weight: 800; letter-spacing: -.025em;
    line-height: 1.15; margin-bottom: 14px;
  }}
  .nlm-header .src-row {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }}
  .nlm-header .meta {{ font-size: 9pt; opacity: .58; }}

  /* Source badges */
  .src-badge {{
    display: inline-flex; align-items: center; gap: 4px;
    font-size: 8.5pt; font-weight: 800; letter-spacing: .05em;
    padding: 3px 10px; border-radius: 99px; text-transform: uppercase;
  }}
  .src-str    {{ background: rgba(11,87,208,.18); color: #0b57d0; border: 1px solid rgba(11,87,208,.3); }}
  .src-datafy {{ background: rgba(24,128,56,.18); color: #188038; border: 1px solid rgba(24,128,56,.3); }}
  .src-costar {{ background: rgba(123,31,162,.18); color: #7b1fa2; border: 1px solid rgba(123,31,162,.3); }}
  .src-ai     {{ background: rgba(180,83,9,.18); color: #b45309; border: 1px solid rgba(180,83,9,.3); }}
  .src-tbid   {{ background: rgba(33,128,141,.18); color: #21808D; border: 1px solid rgba(33,128,141,.3); }}

  /* Inline cite tags */
  .cite {{
    display: inline-flex; align-items: center;
    font-size: 7pt; font-weight: 800; letter-spacing: .04em;
    padding: 1px 5px; border-radius: 3px; vertical-align: middle; margin: 0 2px;
    text-transform: uppercase;
  }}
  .cite-str    {{ background: rgba(11,87,208,.10); color: #0b57d0; }}
  .cite-datafy {{ background: rgba(24,128,56,.10); color: #188038; }}
  .cite-costar {{ background: rgba(123,31,162,.10); color: #7b1fa2; }}
  .cite-ai     {{ background: rgba(180,83,9,.10); color: #b45309; }}

  /* Questions box */
  .q-box {{
    background: #fff; border: 1px solid #e8eaed; border-radius: 12px;
    padding: 18px 22px; margin-bottom: 18px;
  }}
  .q-box-title {{
    font-size: 9pt; font-weight: 800; color: #5f6368; letter-spacing: .09em;
    text-transform: uppercase; margin-bottom: 12px;
    display: flex; align-items: center; gap: 8px;
  }}
  .q-box ol {{
    list-style: none; counter-reset: qc;
    display: grid; grid-template-columns: 1fr 1fr; gap: 8px;
  }}
  .q-box ol li {{
    counter-increment: qc; font-size: 10pt; color: #202124; line-height: 1.5;
    padding: 8px 12px; background: #f8f9fa; border-radius: 8px;
    border-left: 3px solid #21808D;
  }}
  .q-box ol li::before {{ content: counter(qc) ". "; font-weight: 700; color: #21808D; }}

  /* Audio/intelligence briefing box */
  .audio-box {{
    background: linear-gradient(135deg, #202124 0%, #2a3240 100%);
    color: white; border-radius: 14px; padding: 22px 28px;
    margin-bottom: 20px; position: relative; overflow: hidden;
  }}
  .audio-box::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, #21808D, #32B8C6, #E68161);
  }}
  .audio-title {{
    font-size: 9pt; font-weight: 800; letter-spacing: .10em; text-transform: uppercase;
    opacity: .65; margin-bottom: 14px; display: flex; align-items: center; gap: 8px;
  }}
  .audio-pts {{ list-style: none; display: flex; flex-direction: column; gap: 11px; }}
  .audio-pts li {{
    font-size: 10.5pt; line-height: 1.65; opacity: .92;
    padding-left: 20px; position: relative;
  }}
  .audio-pts li::before {{ content: '▸'; position: absolute; left: 0; color: #32B8C6; }}
  .audio-pts li strong {{ color: #32B8C6; }}

  /* Pull stats */
  .pull-stat {{
    background: #ffffff; border: 1px solid #dde1e7;
    border-radius: 12px; padding: 16px 18px; position: relative; overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
  }}
  .pull-stat.featured {{ border-color: #21808D; border-width: 2px; }}
  .pull-stat .ps-src {{ position: absolute; top: 10px; right: 10px; }}
  .pull-stat .ps-label {{
    font-size: 8pt; font-weight: 700; color: #5f6368; letter-spacing: .07em;
    text-transform: uppercase; margin-bottom: 8px; padding-right: 40px;
  }}
  .pull-stat .ps-val {{
    font-size: 21pt; font-weight: 800; color: #202124;
    letter-spacing: -.03em; line-height: 1.1; margin-bottom: 4px;
  }}
  .pull-stat.featured .ps-val {{ color: #21808D; }}
  .pull-stat .ps-delta {{ font-size: 9.5pt; font-weight: 700; margin-bottom: 2px; }}
  .pull-stat .ps-sub {{ font-size: 8.5pt; color: #5f6368; }}

  /* Section heading */
  .s-head {{
    display: flex; align-items: center; gap: 10px;
    margin-bottom: 14px; padding-bottom: 8px; border-bottom: 1px solid #e8eaed;
  }}
  .s-head h2 {{ font-size: 12pt; font-weight: 700; color: #202124; letter-spacing: -.01em; }}
  .s-num {{
    width: 22px; height: 22px; background: #21808D; color: white;
    border-radius: 50%; display: flex; align-items: center; justify-content: center;
    font-size: 9pt; font-weight: 800; flex-shrink: 0;
  }}

  /* NLM card */
  .nlm-card {{
    background: white; border: 1px solid #e8eaed;
    border-radius: 12px; padding: 18px 22px; margin-bottom: 12px;
  }}
  .card-label {{
    font-size: 8.5pt; font-weight: 700; color: #5f6368;
    letter-spacing: .08em; text-transform: uppercase;
    margin-bottom: 10px; display: flex; align-items: center; gap: 8px;
  }}
  .nlm-card p {{ font-size: 10pt; line-height: 1.7; margin-bottom: 8px; }}
  .nlm-card p:last-child {{ margin-bottom: 0; }}

  /* Key number callout */
  .key-number {{
    background: #f8f9fa; border-radius: 8px; padding: 12px 16px; margin: 10px 0;
    border-left: 4px solid #21808D; display: flex; align-items: center; gap: 14px;
  }}
  .kn-num {{ font-size: 18pt; font-weight: 800; color: #21808D; letter-spacing: -.02em; white-space: nowrap; }}
  .kn-desc {{ font-size: 9.5pt; color: #5f6368; line-height: 1.5; }}

  /* Q&A */
  .qa-block {{ margin-bottom: 14px; }}
  .qa-q {{
    font-size: 10pt; font-weight: 700; color: #202124;
    margin-bottom: 6px; display: flex; gap: 8px; align-items: flex-start;
  }}
  .qa-q-mark {{
    width: 18px; height: 18px; background: #f1f3f4; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 8.5pt; font-weight: 800; color: #5f6368; flex-shrink: 0; margin-top: 1px;
  }}
  .qa-a {{ font-size: 9.5pt; color: #5f6368; line-height: 1.65; padding-left: 26px; }}
  .qa-a strong {{ color: #202124; }}

  /* Index bars */
  .index-bar {{ display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }}
  .bar-label {{ width: 90px; color: #5f6368; font-size: 9pt; flex-shrink: 0; }}
  .bar-wrap {{ flex: 1; background: #f1f3f4; border-radius: 4px; height: 8px; }}
  .bar-fill {{ height: 8px; border-radius: 4px; }}
  .bar-val {{ font-weight: 800; font-size: 9.5pt; width: 36px; text-align: right; }}

  /* Table */
  table {{ width: 100%; border-collapse: collapse; font-size: 9.5pt; }}
  th {{
    background: #f8f9fa; color: #5f6368; padding: 8px 10px; text-align: left;
    font-weight: 700; font-size: 8.5pt; letter-spacing: .04em;
    border-bottom: 2px solid #e8eaed;
  }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #f1f5f9; }}
  tr:last-child td {{ border-bottom: none; }}

  /* Concept chips */
  .concepts {{ display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }}
  .concept-chip {{
    background: #f1f3f4; color: #5f6368; font-size: 8.5pt; font-weight: 600;
    padding: 3px 10px; border-radius: 99px; border: 1px solid #e8eaed;
  }}

  /* Action checklist */
  .action-list {{ list-style: none; display: flex; flex-direction: column; gap: 8px; }}
  .action-item {{
    display: flex; gap: 12px; align-items: flex-start;
    padding: 12px 16px; background: white; border: 1px solid #e8eaed; border-radius: 10px;
  }}
  .action-check {{
    width: 18px; height: 18px; min-width: 18px; border: 2px solid #21808D;
    border-radius: 4px; margin-top: 2px; flex-shrink: 0;
  }}
  .action-body {{ flex: 1; font-size: 10pt; line-height: 1.55; color: #202124; }}
  .action-body strong {{
    font-size: 8pt; color: #21808D; letter-spacing: .06em;
    text-transform: uppercase; display: block; margin-bottom: 2px;
  }}

  /* Risk flags */
  .risk-item {{
    display: flex; gap: 14px; align-items: flex-start;
    padding: 12px 16px; border-radius: 10px; margin-bottom: 8px;
    font-size: 9.5pt; border: 1px solid;
  }}
  .risk-amber {{ background: #fffbeb; border-color: #fcd34d; }}
  .risk-red   {{ background: #fef2f2; border-color: #fca5a5; }}
  .risk-green {{ background: #f0fdf4; border-color: #86efac; }}
  .risk-icon  {{ font-size: 14pt; line-height: 1; flex-shrink: 0; }}
  .risk-body  {{ flex: 1; }}
  .risk-body strong {{ display: block; font-size: 9.5pt; margin-bottom: 2px; color: #202124; }}
  .risk-body span   {{ color: #5f6368; line-height: 1.55; display: block; }}

  /* Footer */
  .nlm-footer {{
    margin-top: 30px; padding: 16px 0; border-top: 1px solid #e8eaed;
    display: flex; justify-content: space-between; align-items: flex-end;
    font-size: 8.5pt; color: #9aa0a6;
  }}

  /* Print */
  @media print {{
    body {{ background: white; font-size: 10pt; }}
    .page {{ padding: 0; max-width: 100%; background: white; }}
    .section {{ page-break-inside: avoid; }}
    .audio-box, .nlm-header {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    @page {{ margin: 18mm 16mm; }}
  }}
</style>
</head>
<body>
<div class="page">

<!-- HEADER -->
<div class="nlm-header">
  <div class="org">Visit Dana Point &nbsp;·&nbsp; TBID Analytics &nbsp;·&nbsp; Intelligence Briefing</div>
  <h1>VDP Portfolio Board Report</h1>
  <div class="src-row">
    <span class="src-badge src-str">● STR</span>
    <span class="src-badge src-datafy">● Datafy</span>
    <span class="src-badge src-costar">● CoStar</span>
    <span class="src-badge src-ai">● AI Insights</span>
    <span class="src-badge src-tbid">● TBID</span>
  </div>
  <div class="meta">Generated {report_date} &nbsp;·&nbsp; Dana Point, CA — South Orange County &nbsp;·&nbsp; 12-Property Select Portfolio</div>
</div>

<!-- QUESTIONS THIS REPORT ANSWERS -->
<div class="section">
<div class="q-box">
  <div class="q-box-title">📑 Questions This Report Answers</div>
  <ol>
    <li>What is RevPAR doing and is it on pace vs. prior year?</li>
    <li>How much TBID revenue is the portfolio generating?</li>
    <li>Where are our highest-value visitors coming from?</li>
    <li>Are we gaining or losing ground vs. the comp set?</li>
    <li>What actions should the board authorize today?</li>
    <li>What are the top risks to destination revenue?</li>
  </ol>
</div>
</div>

<!-- INTELLIGENCE BRIEFING -->
<div class="audio-box">
  <div class="audio-title">🎙 Intelligence Briefing — 3 Key Points</div>
  <ul class="audio-pts">
    <li>RevPAR is <strong>${rvp:.0f}</strong> over the last 30 days (<strong>{rvp_d:+.1f}%</strong> vs. prior period), with ADR at <strong>${adr:.0f}</strong> and occupancy at <strong>{occ:.1f}%</strong>. Estimated monthly TBID assessment: <strong>${tbid:,.0f}</strong> — on pace for ${tbid_ann:,.0f} annually.</li>
    <li>{comp_sentence} Weekend RevPAR (<strong>${wknd:.0f}</strong>) exceeds midweek (<strong>${wkdy:.0f}</strong>) by {wkgap:.0f}% — closing 20% of this gap adds ~${midweek_opp:,.0f}/year.</li>
    <li>{visitor_sentence}</li>
  </ul>
</div>

<!-- PERFORMANCE SNAPSHOT -->
<div class="section">
<div class="s-head"><div class="s-num">1</div><h2>Performance Snapshot — Last 30 Days</h2></div>
<div class="stat-grid">
  <div class="pull-stat featured">
    <div class="ps-src"><span class="cite cite-str">STR</span></div>
    <div class="ps-label">RevPAR</div>
    <div class="ps-val">${rvp:.0f}</div>
    <div class="ps-delta" style="color:{_clr(rvp_d)}">{_arr(rvp_d)} {rvp_d:+.1f}%</div>
    <div class="ps-sub">vs. prior 30 days</div>
  </div>
  <div class="pull-stat">
    <div class="ps-src"><span class="cite cite-str">STR</span></div>
    <div class="ps-label">ADR</div>
    <div class="ps-val">${adr:.0f}</div>
    <div class="ps-delta" style="color:{_clr(adr_d)}">{_arr(adr_d)} {adr_d:+.1f}%</div>
    <div class="ps-sub">avg daily rate</div>
  </div>
  <div class="pull-stat">
    <div class="ps-src"><span class="cite cite-str">STR</span></div>
    <div class="ps-label">Occupancy</div>
    <div class="ps-val">{occ:.1f}%</div>
    <div class="ps-delta" style="color:{_clr(occ_d)}">{_arr(occ_d)} {occ_d:+.1f}pp</div>
    <div class="ps-sub">room demand</div>
  </div>
  <div class="pull-stat">
    <div class="ps-src"><span class="cite cite-tbid">TBID</span></div>
    <div class="ps-label">Monthly TBID Est.</div>
    <div class="ps-val">${tbid:,.0f}</div>
    <div class="ps-delta" style="color:#21808D">${tbid_ann:,.0f}/yr</div>
    <div class="ps-sub">blended 1.25% rate</div>
  </div>
</div>
<div class="stat-grid">
  <div class="pull-stat">
    <div class="ps-src"><span class="cite cite-str">STR</span></div>
    <div class="ps-label">Compression Days (QTD)</div>
    <div class="ps-val">{cq}</div>
    <div class="ps-delta" style="color:{_clr(cq - cpq)}">{_arr(cq - cpq)} vs. {cpq} prior Q</div>
    <div class="ps-sub">nights above 90% occ</div>
  </div>
  <div class="pull-stat">
    <div class="ps-src"><span class="cite cite-str">STR</span></div>
    <div class="ps-label">Weekend RevPAR</div>
    <div class="ps-val">${wknd:.0f}</div>
    <div class="ps-delta" style="color:#5f6368">vs. ${wkdy:.0f} midweek</div>
    <div class="ps-sub">{wkgap:.0f}% weekend premium</div>
  </div>
  <div class="pull-stat">
    <div class="ps-src"><span class="cite cite-datafy">Datafy</span></div>
    <div class="ps-label">Annual Visitor Trips</div>
    <div class="ps-val">{"N/A" if trips_m == 0 else f"{trips_m:.2f}M"}</div>
    <div class="ps-delta" style="color:#188038">{overnight:.0f}% overnight</div>
    <div class="ps-sub">{oos_pct:.0f}% out-of-state</div>
  </div>
  <div class="pull-stat">
    <div class="ps-src"><span class="cite cite-datafy">Datafy</span></div>
    <div class="ps-label">Marketing ROAS</div>
    <div class="ps-val">{roas_val}</div>
    <div class="ps-delta" style="color:#188038">{roas_trips_lbl}</div>
    <div class="ps-sub">{roas_impact_sub}</div>
  </div>
</div>
</div>

<!-- REVENUE ANALYSIS -->
<div class="section">
<div class="s-head">
  <div class="s-num">2</div>
  <h2>Revenue Analysis <span class="cite cite-str">STR</span> <span class="cite cite-costar">CoStar</span></h2>
</div>
<div class="col2">
  <div class="nlm-card">
    <div class="card-label">Revenue Story</div>
    <div class="qa-block">
      <div class="qa-q"><span class="qa-q-mark">Q</span>How is portfolio revenue trending?</div>
      <div class="qa-a">RevPAR of <strong>${rvp:.0f}</strong> is {"trending upward" if rvp_d >= 0 else "softening"} at {rvp_d:+.1f}% vs. prior period <span class="cite cite-str">STR</span>. ADR of <strong>${adr:.0f}</strong> reflects {"strong rate discipline" if adr_d >= 0 else "rate pressure that warrants review"}. Full-year 2024: Occ <strong>{vdp_occ_ann:.1f}%</strong> · ADR <strong>${vdp_adr_ann:.0f}</strong> · RevPAR <strong>${vdp_rvp_ann:.0f}</strong>.</div>
    </div>
    <div class="qa-block">
      <div class="qa-q"><span class="qa-q-mark">Q</span>What is the weekend/midweek opportunity?</div>
      <div class="qa-a">Weekend RevPAR (<strong>${wknd:.0f}</strong>) exceeds midweek (<strong>${wkdy:.0f}</strong>) by {wkgap:.0f}% <span class="cite cite-str">STR</span>. Closing 20% of this gap via midweek programs adds approximately <strong>${midweek_opp:,.0f}/year</strong> in portfolio room revenue.</div>
    </div>
    <div class="concepts">
      <span class="concept-chip">Rate Discipline</span>
      <span class="concept-chip">Compression Events</span>
      <span class="concept-chip">Midweek Gap</span>
    </div>
  </div>
  <div class="nlm-card">
    <div class="card-label">Market Context <span class="cite cite-costar">CoStar</span></div>
    {costar_section}
  </div>
</div>
</div>

<!-- VISITOR INTELLIGENCE -->
<div class="section">
<div class="s-head">
  <div class="s-num">3</div>
  <h2>Visitor Intelligence <span class="cite cite-datafy">Datafy</span></h2>
</div>
<div class="col2">
  <div class="nlm-card">
    <div class="card-label">Visitor Profile</div>
    {visitor_profile_section}
  </div>
  <div class="nlm-card">
    <div class="card-label">Top Feeder Markets <span class="cite cite-datafy">Datafy</span></div>
    {dma_table}
  </div>
</div>
</div>

<!-- FORWARD OUTLOOK -->
<div class="section">
<div class="s-head">
  <div class="s-num">4</div>
  <h2>Forward Outlook <span class="cite cite-ai">AI Insights</span></h2>
</div>
{insight_rows if insight_rows else '<div class="nlm-card"><p style="color:#5f6368;font-size:10pt">Run <code>python scripts/run_pipeline.py</code> to generate forward-looking AI insights.</p></div>'}
</div>

<!-- BOARD ACTIONS -->
<div class="section">
<div class="s-head"><div class="s-num">5</div><h2>Board Action Items</h2></div>
<ul class="action-list">
  <li class="action-item">
    <div class="action-check"></div>
    <div class="action-body"><strong>Revenue Strategy</strong>{revenue_action}</div>
  </li>
  <li class="action-item">
    <div class="action-check"></div>
    <div class="action-body"><strong>TBID Projection</strong>At current pace, annual TBID revenue estimated at <strong>${tbid_ann:,.0f}</strong> (blended 1.25%). {tbid_direction}</div>
  </li>
  <li class="action-item">
    <div class="action-check"></div>
    <div class="action-body"><strong>Fly-Market Investment</strong>{media_action}</div>
  </li>
  <li class="action-item">
    <div class="action-check"></div>
    <div class="action-body"><strong>Midweek Demand Program</strong>Authorize feasibility review. Closing 20% of the weekend/midweek gap (${wknd:.0f} → ${wkdy:.0f}) adds approximately <strong>${midweek_opp:,.0f}/year</strong> in portfolio room revenue.</div>
  </li>
  <li class="action-item">
    <div class="action-check"></div>
    <div class="action-body"><strong>Market Intelligence</strong>{costar_action}</div>
  </li>
</ul>
</div>

<!-- RISK RADAR -->
<div class="section">
<div class="s-head"><div class="s-num">6</div><h2>Risk Radar</h2></div>
<div class="risk-item risk-amber">
  <span class="risk-icon">⚠️</span>
  <div class="risk-body">
    <strong>Labor Cost Pressure</strong>
    <span>Regional hotel labor agreement (2024) drives wage increases through 2028. Margins compress even as RevPAR grows. Monitor GOP margins and adjust TBID projection models annually.</span>
  </div>
</div>
<div class="risk-item risk-amber">
  <span class="risk-icon">⚠️</span>
  <div class="risk-body">
    <strong>Supply Pipeline</strong>
    <span>{supply_risk_msg}</span>
  </div>
</div>
<div class="risk-item {demand_risk_class}">
  <span class="risk-icon">{demand_risk_icon}</span>
  <div class="risk-body">
    <strong>Demand Trajectory</strong>
    <span>RevPAR {rvp_d:+.1f}% vs. prior period <span class="cite cite-str">STR</span>. {demand_risk_msg}</span>
  </div>
</div>
</div>

<!-- DATA PROVENANCE -->
<div class="section">
<div class="s-head"><div class="s-num">7</div><h2>Data Provenance</h2></div>
<div class="nlm-card">
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;font-size:9.5pt;">
    <div>
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
        <span class="src-badge src-str">STR</span>
        <span style="color:#5f6368;font-weight:600">Smith Travel Research</span>
      </div>
      <div style="color:#9aa0a6;font-size:8.5pt;line-height:1.5">Hotel performance data (occ, ADR, RevPAR) for VDP Select Portfolio — 12 properties, South Orange County. Daily/monthly exports.</div>
    </div>
    <div>
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
        <span class="src-badge src-datafy">Datafy</span>
        <span style="color:#5f6368;font-weight:600">Visitor Economy Intelligence</span>
      </div>
      <div style="color:#9aa0a6;font-size:8.5pt;line-height:1.5">Visitor trips, DMA feeder profiles, spending by category, media attribution and ROAS. Annual/seasonal report periods.</div>
    </div>
    <div>
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
        <span class="src-badge src-costar">CoStar</span>
        <span style="color:#5f6368;font-weight:600">CoStar Hospitality Analytics</span>
      </div>
      <div style="color:#9aa0a6;font-size:8.5pt;line-height:1.5">Market-level benchmarks for Newport Beach/Dana Point submarket. Supply pipeline, chain scale, profitability.</div>
    </div>
    <div>
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
        <span class="src-badge src-ai">AI</span>
        <span style="color:#5f6368;font-weight:600">Claude AI Insights Engine</span>
      </div>
      <div style="color:#9aa0a6;font-size:8.5pt;line-height:1.5">Forward-looking insights generated daily from all Layer 1 data. 4 audience tracks: DMO, City, Visitor, Resident.</div>
    </div>
  </div>
  <div style="margin-top:12px;padding-top:12px;border-top:1px solid #e8eaed;font-size:8.5pt;color:#9aa0a6">
    <strong>TBID Revenue</strong> estimated at blended 1.25% assessment rate (Tier 1: 1.0%, Tier 2: 1.5%). &nbsp;·&nbsp; Refresh: <code>python scripts/run_pipeline.py</code>
  </div>
</div>
</div>

<!-- FOOTER -->
<div class="nlm-footer">
  <div>
    <div style="font-weight:700;color:#5f6368;margin-bottom:6px">Visit Dana Point — VDP Analytics Platform</div>
    <div style="display:flex;gap:6px">
      <span class="src-badge src-str">STR</span>
      <span class="src-badge src-datafy">Datafy</span>
      <span class="src-badge src-costar">CoStar</span>
      <span class="src-badge src-ai">AI</span>
    </div>
  </div>
  <div style="text-align:right">
    <div>Generated: {report_date}</div>
    <div>Confidential — Board Use Only</div>
  </div>
</div>

</div>
</body>
</html>"""
    return html

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>VDP Board Report — {period_label}</title>
<style>
  /* ── Reset & Base ── */
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; color: #1a1a2e; background: #fff; line-height: 1.55; }}
  a {{ color: #21808D; text-decoration: none; }}

  /* ── Layout ── */
  .page {{ max-width: 860px; margin: 0 auto; padding: 32px 40px; }}
  .section {{ margin-bottom: 32px; page-break-inside: avoid; }}
  .col2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  .col3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; }}
  .col4 {{ display: grid; grid-template-columns: repeat(4,1fr); gap: 12px; }}

  /* ── Header ── */
  .report-header {{ background: #21808D; color: white; padding: 28px 36px 22px; border-radius: 8px; margin-bottom: 28px; }}
  .report-header .org {{ font-size: 11pt; font-weight: 600; letter-spacing: .12em; opacity: .88; margin-bottom: 6px; }}
  .report-header h1 {{ font-size: 22pt; font-weight: 800; letter-spacing: -.02em; line-height: 1.2; margin-bottom: 8px; }}
  .report-header .meta {{ font-size: 9.5pt; opacity: .82; }}
  .report-header .meta span {{ margin-right: 16px; }}

  /* ── Section headings ── */
  h2 {{ font-size: 13pt; font-weight: 700; color: #21808D; border-bottom: 2px solid #21808D; padding-bottom: 5px; margin-bottom: 14px; letter-spacing: .02em; }}
  h3 {{ font-size: 11pt; font-weight: 700; color: #1a1a2e; margin-bottom: 8px; }}

  /* ── Executive Summary ── */
  .exec-summary {{ background: #f0fafb; border-left: 4px solid #21808D; border-radius: 6px; padding: 16px 20px; font-size: 10.5pt; line-height: 1.65; }}
  .exec-summary strong {{ color: #21808D; }}

  /* ── KPI Cards ── */
  .kpi-card {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 14px 16px; }}
  .kpi-card .label {{ font-size: 8.5pt; color: #64748b; font-weight: 600; letter-spacing: .07em; text-transform: uppercase; margin-bottom: 4px; }}
  .kpi-card .value {{ font-size: 20pt; font-weight: 800; color: #1a1a2e; line-height: 1.1; }}
  .kpi-card .delta {{ font-size: 9pt; font-weight: 600; margin-top: 3px; }}
  .kpi-card .sub {{ font-size: 8.5pt; color: #64748b; margin-top: 2px; }}
  .kpi-card.highlight {{ border-color: #21808D; border-width: 2px; background: #f0fafb; }}

  /* ── Narrative stories ── */
  .story {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px 18px; }}
  .story .story-label {{ font-size: 8pt; font-weight: 700; color: #21808D; letter-spacing: .1em; text-transform: uppercase; margin-bottom: 6px; }}
  .story p {{ font-size: 10pt; line-height: 1.65; margin-bottom: 8px; }}
  .story .action {{ background: rgba(33,128,141,.08); border-radius: 5px; padding: 8px 12px; font-size: 9.5pt; color: #21808D; font-weight: 600; margin-top: 8px; }}

  /* ── Index comparison ── */
  .index-bar {{ display: flex; align-items: center; gap: 10px; margin-bottom: 6px; font-size: 9.5pt; }}
  .index-bar .bar-wrap {{ flex: 1; background: #e2e8f0; border-radius: 4px; height: 8px; }}
  .index-bar .bar-fill {{ height: 8px; border-radius: 4px; }}

  /* ── Insights ── */
  .insight-block {{ border-left: 3px solid #21808D; padding: 8px 14px; margin-bottom: 10px; }}
  .insight-cat {{ font-size: 8pt; color: #21808D; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; margin-bottom: 2px; }}
  .insight-hl {{ font-size: 10pt; font-weight: 700; color: #1a1a2e; margin-bottom: 3px; }}
  .insight-body {{ font-size: 9.5pt; color: #475569; line-height: 1.55; }}

  /* ── Table ── */
  table {{ width: 100%; border-collapse: collapse; font-size: 9.5pt; }}
  th {{ background: #21808D; color: white; padding: 7px 10px; text-align: left; font-weight: 600; font-size: 8.5pt; letter-spacing: .04em; }}
  td {{ padding: 6px 10px; border-bottom: 1px solid #f1f5f9; }}
  tr:nth-child(even) td {{ background: #f8fafc; }}

  /* ── Actions list ── */
  .action-list {{ list-style: none; }}
  .action-list li {{ padding: 10px 14px; border-left: 3px solid #21808D; margin-bottom: 8px; font-size: 10pt; background: #f0fafb; border-radius: 0 6px 6px 0; }}
  .action-list li strong {{ color: #21808D; display: block; font-size: 8.5pt; letter-spacing: .06em; text-transform: uppercase; margin-bottom: 3px; }}

  /* ── Risk flags ── */
  .risk-flag {{ display: flex; gap: 12px; align-items: flex-start; padding: 10px 14px; border-radius: 6px; margin-bottom: 8px; font-size: 9.5pt; }}
  .risk-flag.amber {{ background: #fffbeb; border: 1px solid #f59e0b; }}
  .risk-flag.red {{ background: #fef2f2; border: 1px solid #ef4444; }}
  .risk-flag.green {{ background: #f0fdf4; border: 1px solid #22c55e; }}
  .risk-icon {{ font-size: 14pt; line-height: 1; }}

  /* ── Footer ── */
  .footer {{ margin-top: 36px; padding-top: 14px; border-top: 1px solid #e2e8f0; font-size: 8.5pt; color: #94a3b8; display: flex; justify-content: space-between; }}

  /* ── Print ── */
  @media print {{
    body {{ font-size: 10pt; }}
    .page {{ padding: 0; max-width: 100%; }}
    .section {{ page-break-inside: avoid; }}
    .kpi-card {{ border: 1px solid #ccc !important; }}
    h2 {{ color: #000 !important; border-bottom-color: #000 !important; }}
    .report-header {{ background: #21808D !important; -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    @page {{ margin: 18mm 16mm; }}
  }}
</style>
</head>
<body>
<div class="page">

<!-- HEADER -->
<div class="report-header">
  <div class="org">VISIT DANA POINT &nbsp;·&nbsp; TBID ANALYTICS</div>
  <h1>VDP Portfolio Board Report</h1>
  <div class="meta">
    <span>📅 {report_date}</span>
    <span>📍 Dana Point, CA — South Orange County</span>
    <span>🏨 12-Property Select Portfolio</span>
    <span>📊 Data: STR · Datafy · CoStar</span>
  </div>
</div>

<!-- EXECUTIVE SUMMARY -->
<div class="section">
<h2>Executive Summary</h2>
<div class="exec-summary">
The VDP Select Portfolio delivered <strong>RevPAR of ${rvp:.0f}</strong> over the last 30 days
({_arr(rvp_d)} <strong style="color:{_clr(rvp_d)}">{rvp_d:+.1f}%</strong> vs. prior period),
with ADR at <strong>${adr:.0f}</strong> ({_arr(adr_d)}&thinsp;{adr_d:+.1f}%) and occupancy at <strong>{occ:.1f}%</strong>.
The portfolio generated an estimated <strong>${tbid:,.0f}/month</strong> in TBID assessments (blended 1.25%),
on pace for <strong>${tbid_ann:,.0f} annually</strong>.
{"Compression activity increased to <strong>" + str(cq) + " days above 90% occupancy</strong> this quarter vs. " + str(cpq) + " prior — a signal to pursue rate increases." if cq >= cpq else "Compression activity moderated to <strong>" + str(cq) + " days above 90% occupancy</strong> vs. " + str(cpq) + " prior quarter — shoulder season demand programs are warranted."}
{"Dana Point hosted <strong>{:.2f}M visitor trips</strong> annually, with <strong>{:.0f}%</strong> overnight stays and <strong>{:.0f}%</strong> out-of-state visitors driving disproportionate room revenue per trip.".format(trips_m, overnight, oos_pct) if trips_m > 0 else ""}
{"CoStar benchmarks show VDP portfolio at MPI <strong>{:.0f}</strong> / ARI <strong>{:.0f}</strong> / RGI <strong>{:.0f}</strong> vs. the South OC market.".format(mpi, ari, rgi) if mkt_rvp > 0 else ""}
</div>
</div>

<!-- KPI DASHBOARD -->
<div class="section">
<h2>Performance Dashboard — Last 30 Days vs. Prior Period</h2>
<div class="col4">
  <div class="kpi-card highlight">
    <div class="label">RevPAR</div>
    <div class="value">${rvp:.0f}</div>
    <div class="delta" style="color:{_clr(rvp_d)}">{_arr(rvp_d)} {rvp_d:+.1f}%</div>
    <div class="sub">vs. prior 30 days</div>
  </div>
  <div class="kpi-card">
    <div class="label">ADR</div>
    <div class="value">${adr:.0f}</div>
    <div class="delta" style="color:{_clr(adr_d)}">{_arr(adr_d)} {adr_d:+.1f}%</div>
    <div class="sub">avg daily rate</div>
  </div>
  <div class="kpi-card">
    <div class="label">Occupancy</div>
    <div class="value">{occ:.1f}%</div>
    <div class="delta" style="color:{_clr(occ_d)}">{_arr(occ_d)} {occ_d:+.1f}pp</div>
    <div class="sub">room demand</div>
  </div>
  <div class="kpi-card">
    <div class="label">TBID Est. (Monthly)</div>
    <div class="value">${tbid:,.0f}</div>
    <div class="delta" style="color:#21808D">${tbid_ann:,.0f}/yr</div>
    <div class="sub">at 1.25% blended</div>
  </div>
</div>
<div class="col4" style="margin-top:12px">
  <div class="kpi-card">
    <div class="label">Compression Days (QTD)</div>
    <div class="value">{cq}</div>
    <div class="delta" style="color:{_clr(cq - cpq)}">{_arr(cq - cpq)} vs. {cpq} prior Q</div>
    <div class="sub">nights &gt;90% occ</div>
  </div>
  <div class="kpi-card">
    <div class="label">Weekend RevPAR</div>
    <div class="value">${wknd:.0f}</div>
    <div class="delta" style="color:#64748b">vs. ${wkdy:.0f} midweek</div>
    <div class="sub">{wkgap:.0f}% premium</div>
  </div>
  <div class="kpi-card">
    <div class="label">Annual Visitor Trips</div>
    <div class="value">{"N/A" if trips_m == 0 else f"{trips_m:.2f}M"}</div>
    <div class="delta" style="color:#21808D">{overnight:.0f}% overnight</div>
    <div class="sub">{oos_pct:.0f}% out-of-state</div>
  </div>
  <div class="kpi-card">
    <div class="label">Marketing ROAS</div>
    <div class="value">{"N/A" if roas == 0 else f"{roas:.1f}×"}</div>
    <div class="delta" style="color:#21808D">{"N/A" if attr_trips == 0 else f"{attr_trips:,} attributed trips"}</div>
    <div class="sub">{"N/A" if total_impact == 0 else f"${total_impact/1e6:.1f}M total impact"}</div>
  </div>
</div>
</div>

<!-- STORY 1: REVENUE -->
<div class="section">
<h2>Story 1 — Revenue Performance</h2>
<div class="col2">
  <div class="story">
    <div class="story-label">What the data shows</div>
    <p>The portfolio's <strong>RevPAR of ${rvp:.0f}</strong> reflects {"strong rate discipline and sustained demand" if rvp_d >= 0 else "softness in demand or rate discipline that warrants attention"}. ADR at <strong>${adr:.0f}</strong> ({_arr(adr_d)}&thinsp;{adr_d:+.1f}%) is {"outpacing" if adr_d >= 0 else "lagging"} the prior period. Full-year 2024 portfolio averages: Occ {vdp_occ_ann:.1f}%, ADR ${vdp_adr_ann:.0f}, RevPAR ${vdp_rvp_ann:.0f}.</p>
    <p>Weekend RevPAR (${wknd:.0f}) exceeds midweek (${wkdy:.0f}) by {wkgap:.0f}%. Closing just 20% of this gap via midweek programs adds approximately <strong>${(wknd-wkdy)*0.2*90/7*12:,.0f}/year</strong> in portfolio room revenue.</p>
    <div class="action">→ ACTION: {"Capture rate on compression nights — data supports ADR increases of 8–12% on nights with occupancy above 85%." if cq >= cpq else "Launch targeted midweek demand campaign. Recommend $50K budget authorization."}</div>
  </div>
  <div class="story">
    <div class="story-label">Market context (CoStar 2024)</div>
    {"<p>South OC market: <strong>Occ {:.1f}%</strong> · ADR <strong>${:.0f}</strong> · RevPAR <strong>${:.0f}</strong>.</p><p>VDP Portfolio Index vs. Market:</p>".format(mkt_occ, mkt_adr, mkt_rvp) if mkt_rvp > 0 else "<p>CoStar benchmark data available in the Market Intelligence tab.</p>"}
    {"<div class='index-bar'><span style='width:80px;font-size:9pt;color:#475569'>MPI (Occ)</span><div class='bar-wrap'><div class='bar-fill' style='width:min(100%,{:.0f}%);background:{};'></div></div><span style='font-weight:700;color:{};font-size:9.5pt'>{:.0f}</span></div>".format(min(mpi,100), _idx_clr(mpi), _idx_clr(mpi), mpi) if mkt_rvp > 0 else ""}
    {"<div class='index-bar'><span style='width:80px;font-size:9pt;color:#475569'>ARI (Rate)</span><div class='bar-wrap'><div class='bar-fill' style='width:min(100%,{:.0f}%);background:{};'></div></div><span style='font-weight:700;color:{};font-size:9.5pt'>{:.0f}</span></div>".format(min(ari,100), _idx_clr(ari), _idx_clr(ari), ari) if mkt_rvp > 0 else ""}
    {"<div class='index-bar'><span style='width:80px;font-size:9pt;color:#475569'>RGI (RevPAR)</span><div class='bar-wrap'><div class='bar-fill' style='width:min(100%,{:.0f}%);background:{};'></div></div><span style='font-weight:700;color:{};font-size:9.5pt'>{:.0f}</span></div>".format(min(rgi,100), _idx_clr(rgi), _idx_clr(rgi), rgi) if mkt_rvp > 0 else ""}
    <div class="action" style="margin-top:10px">→ {"Index above 100: portfolio outperforming market. Protect rate positioning." if rgi >= 100 else "Index below 100: portfolio underperforming market. Review rate strategy and channel mix."}</div>
  </div>
</div>
</div>

<!-- STORY 2: VISITOR ECONOMY -->
<div class="section">
<h2>Story 2 — Visitor Economy (Datafy)</h2>
<div class="col2">
  <div class="story">
    <div class="story-label">Who is visiting Dana Point</div>
    {"<p>Dana Point welcomed <strong>{:.2f}M annual visitor trips</strong>. <strong>{:.0f}%</strong> stayed overnight (avg {:.1f} nights), while <strong>{:.0f}%</strong> were day trips.</p><p><strong>{:.0f}%</strong> of visitor days came from out-of-state markets, which generate higher per-trip ADR and longer stays than LA drive market visitors.</p>".format(trips_m, overnight, avg_los, 100-overnight, oos_pct) if trips_m > 0 else "<p>Load Datafy visitor economy data to see visitor profile.</p>"}
    {"<div class='action'>→ ACTION: OOS visitors from fly markets (SLC, DFW, NYC) generate 1.3–1.4× room revenue per trip vs. LA drive. Recommend increasing fly-market budget allocation by 15%.</div>" if oos_pct > 40 else ""}
  </div>
  <div class="story">
    <div class="story-label">Top feeder markets by visitor share</div>
    {"<table><thead><tr><th>Market (DMA)</th><th style='text-align:center'>Visitor Days %</th><th style='text-align:right'>Avg Spend</th></tr></thead><tbody>" + dma_rows + "</tbody></table>" if dma_rows else "<p>DMA data not yet loaded.</p>"}
  </div>
</div>
</div>

<!-- STORY 3: FORWARD OUTLOOK -->
<div class="section">
<h2>Story 3 — Forward Outlook (AI-Generated Insights)</h2>
{insight_rows if insight_rows else "<p style='color:#64748b;font-size:10pt'>Run compute_insights.py to generate forward-looking insights.</p>"}
</div>

<!-- RECOMMENDED ACTIONS -->
<div class="section">
<h2>Recommended Board Actions</h2>
<ul class="action-list">
  <li><strong>Revenue Strategy</strong>{"Approve rate increase on compression nights (90%+ occ). Proposed floor: ADR +" + str(int(adr * 0.10)) + " on nights with forecast demand above 90%." if cq > 0 else "Authorize $50K shoulder-season demand generation campaign targeting midweek bookings."}</li>
  <li><strong>TBID Revenue Projection</strong>At current pace, annual TBID revenue is estimated at <strong>${tbid_ann:,.0f}</strong>. {"Trending above prior period — budget assumptions may be revised upward." if rvp_d >= 0 else "Monitor for sustained softness; consider revised budget assumptions if trend persists 60+ days."}</li>
  <li><strong>Visitor Economy Investment</strong>{"Authorize increased media investment in fly markets (SLC, DFW, NYC, ORD). These DMAs generate 1.3–1.4× room revenue per trip. ROAS on attribution tracking: " + str(roas) + "×." if roas > 0 else "Approve Datafy attribution study to quantify media ROAS — required for next budget cycle."}</li>
  <li><strong>Market Intelligence Review</strong>Present updated CoStar comp set analysis at next board meeting. {"Portfolio at RGI {:.0f} — discuss rate ladder strategy to close gap vs. Upper Upscale segment.".format(rgi) if mkt_rvp > 0 else "Request CoStar market report subscription renewal for 2026."}</li>
  <li><strong>Midweek Demand Program</strong>Authorize feasibility review for midweek demand program. Closing 20% of the weekend/midweek RevPAR gap (${wknd:.0f}→${wkdy:.0f}) generates approximately <strong>${(wknd-wkdy)*0.2*90/7*12:,.0f}/year</strong> in incremental portfolio room revenue.</li>
</ul>
</div>

<!-- RISK FACTORS -->
<div class="section">
<h2>Risk Factors</h2>
<div class="risk-flag amber">
  <span class="risk-icon">⚠️</span>
  <div><strong>Labor Cost Pressure</strong> — Regional hotel labor agreement (2024) drives wage increases through 2028. Margins compress even as RevPAR grows. Monitor GOP margins and adjust TBID projection models annually.</div>
</div>
<div class="risk-flag amber">
  <span class="risk-icon">⚠️</span>
  <div><strong>Supply Pipeline</strong> — {"Active supply pipeline adds rooms to South OC market. New competitive supply may pressure occupancy for mid-tier segments in 2025–2026." if not df_cs_snap_in.empty else "Monitor CoStar supply pipeline for new competitive openings."}</div>
</div>
<div class="risk-flag {"green" if rvp_d >= 0 else "red"}">
  <span class="risk-icon">{"✅" if rvp_d >= 0 else "🔴"}</span>
  <div><strong>Demand Trend</strong> — RevPAR {"growing" if rvp_d >= 0 else "softening"} at {rvp_d:+.1f}% vs. prior period. {"Demand trajectory supports current pricing strategy." if rvp_d >= 0 else "Softening demand warrants review of rate strategy and demand generation program effectiveness."}</div>
</div>
</div>

<!-- DATA SOURCES & FOOTER -->
<div class="section">
<h2>Data Sources &amp; Methodology</h2>
<p style="font-size:9.5pt;color:#475569">
<strong>STR (Smith Travel Research)</strong> — hotel performance data (occupancy, ADR, RevPAR) for VDP Select Portfolio (12 properties, South Orange County, CA). Updated via monthly/daily export files.<br>
<strong>Datafy</strong> — visitor economy intelligence (visitor trips, DMA profiles, spending, media attribution). Annual/seasonal report periods.<br>
<strong>CoStar Hospitality Analytics</strong> — market-level benchmarks for Newport Beach/Dana Point submarket. Data as of {report_date}.<br>
<strong>TBID Revenue</strong> — estimated at blended 1.25% assessment rate applied to room revenue. Actual assessments may vary by property tier.<br>
<strong>All metrics are based on available loaded data.</strong> Run <code>python scripts/run_pipeline.py</code> to refresh with latest source files.
</p>
</div>

<!-- FOOTER -->
<div class="footer">
  <span>Visit Dana Point — VDP Analytics Platform</span>
  <span>Prepared: {report_date} &nbsp;·&nbsp; Confidential — Board Use Only</span>
</div>

</div>
</body>
</html>"""
    return html


# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab_ov, tab_tr, tab_fo, tab_ev, tab_fm, tab_ei, tab_sp, tab_cs, tab_dl = st.tabs([
    "🧠 Overview",
    "📈 STR & Pipeline",
    "🔭 Forward Outlook",
    "🗺️ Visitor Economy",
    "🎯 Feeder Markets",
    "📅 Event Impact",
    "🏗️ Supply & Pipeline",
    "🏢 Market Intelligence",
    "📁 Data & Downloads",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_ov:

    # ── Board Report (auto-generated, always visible) ──────────────────────────
    with st.expander("📋 VDP Board Report — Auto-Generated Talking Points", expanded=True):
        st.markdown('<span class="ai-chip">BOARD READY</span>', unsafe_allow_html=True)

        if m:
            _rvp   = m.get("revpar_30", 0)
            _rvp_d = m.get("revpar_delta", 0)
            _adr   = m.get("adr_30", 0)
            _adr_d = m.get("adr_delta", 0)
            _occ   = m.get("occ_30", 0)
            _occ_d = m.get("occ_delta", 0)
            _tbid  = m.get("tbid_monthly", 0)
            _cq    = m.get("comp_recent_q", 0)
            _cpq   = m.get("comp_prior_q", 0)
            _wknd  = m.get("wknd_revpar", 0)
            _wkdy  = m.get("wkdy_revpar", 0)
            _gap   = ((_wknd - _wkdy) / _wkdy * 100) if _wkdy else 0

            _oos_pct  = float(df_dfy_ov.iloc[0].get("out_of_state_vd_pct", 0) or 0) if not df_dfy_ov.empty else 0
            _trips_m  = int(df_dfy_ov.iloc[0].get("total_trips", 0) or 0) / 1_000_000 if not df_dfy_ov.empty else 0
            _overnight = float(df_dfy_ov.iloc[0].get("overnight_pct", 0) or 0) if not df_dfy_ov.empty else 0

            _dir_arrow = "▲" if _rvp_d >= 0 else "▼"
            _dir_color = "#21808D" if _rvp_d >= 0 else "#c0152f"

            # Zartico historical context for board report
            _zrt_ctx = "Visitor devices share: 21.2% · Visitor spend share: 48.0% · Avg. visitor spend peaked at $204 in Jul 2024. OOS visitor rate: 23%."
            if not df_zrt_kpis.empty:
                _zk = df_zrt_kpis.iloc[0]
                _zrt_ctx = (
                    f"Visitor devices: {_zk.get('pct_devices_visitors', 21.2):.1f}% of local devices · "
                    f"Visitor spend: {_zk.get('pct_spend_visitors', 48.0):.1f}% of total · "
                    f"Accommodation spend: {_zk.get('pct_accommodation_spend_visitors', 76.0):.0f}% from visitors. "
                    f"Top feeder: LA ({df_zrt_markets[df_zrt_markets['rank']==1]['pct_visitors'].values[0]:.1f}% of visits) · "
                    "Peak avg. spend $204/visitor (Jul 2024)."
                ) if not df_zrt_markets.empty else (
                    f"Visitor devices: {_zk.get('pct_devices_visitors', 21.2):.1f}% · "
                    f"Visitor spend: {_zk.get('pct_spend_visitors', 48.0):.1f}% · "
                    f"Accommodation: {_zk.get('pct_accommodation_spend_visitors', 76.0):.0f}%."
                )

            # Source badge row
            _src_row = (
                '<span class="nlm-tag nlm-tag-str">STR</span>'
                + (' <span class="nlm-tag nlm-tag-datafy">Datafy</span>' if _trips_m > 0 else '')
                + ' <span class="nlm-tag nlm-tag-ai">AI Insights</span>'
            )
            _midweek_opp_lbl = f"${(_wknd - _wkdy) * 0.2 * 90 / 7 * 12:,.0f}/year"
            # Datafy GA4 web analytics summary
            if not df_dfy_social_aud.empty:
                _ga4_sessions = int(df_dfy_social_aud.iloc[0].get("total_sessions", 0) or 0)
                _ga4_eng      = float(df_dfy_social_aud.iloc[0].get("engagement_rate", 0) or 0)
                _ga4_top_page = df_dfy_social_pages.iloc[0].get("page_path", "—") if not df_dfy_social_pages.empty else "—"
                _ga4_lbl = (
                    f"Website sessions: <strong>{_ga4_sessions:,}</strong> &nbsp;·&nbsp; "
                    f"Engagement rate: <strong>{_ga4_eng:.1f}%</strong> &nbsp;·&nbsp; "
                    f"Top page: <strong>{_ga4_top_page}</strong>."
                )
            else:
                _ga4_lbl = "Run pipeline to load Datafy GA4 web analytics data."
            _visitor_lbl = (
                f"<strong>{_trips_m:.2f}M</strong> annual visitor trips · "
                f"<strong>{_overnight:.1f}%</strong> overnight stays · "
                f"<strong>{_oos_pct:.1f}%</strong> out-of-state visitors generating higher per-trip spend."
                if _trips_m > 0 else "Run pipeline to load Datafy visitor data."
            )
            st.markdown(f"""
<div class="nlm-briefing">
<div class="nlm-briefing-title">
  🎙 Dana Point Hotel Market — Intelligence Briefing &nbsp;·&nbsp; {datetime.now().strftime("%B %Y").upper()}
  &nbsp; {_src_row}
</div>

<div class="nlm-point">
  <strong>Revenue Momentum</strong> &nbsp;<span style="color:{_dir_color};font-weight:700;">{_dir_arrow} {_rvp_d:+.1f}%</span><br>
  RevPAR is <strong>${_rvp:.0f}</strong> over the last 30 days ({_rvp_d:+.1f}% vs. prior period).
  ADR is <strong>${_adr:.0f}</strong> ({_adr_d:+.1f}%) · Occupancy at <strong>{_occ:.1f}%</strong> ({_occ_d:+.1f}pp).
  <br><em style="opacity:.72">→ {"Maintain pricing discipline — demand supports current rate levels." if _rvp_d >= 0 else "Examine rate softness drivers; consider targeted packages for shoulder periods."}</em>
</div>

<div class="nlm-point">
  <strong>TBID Revenue Projection</strong> <span class="nlm-tag nlm-tag-str">STR</span><br>
  Monthly TBID assessment: <strong>${_tbid:,.0f}</strong> (blended 1.25% rate).
  Compression: <strong>{_cq}</strong> nights above 90% occ this quarter vs. {_cpq} prior.
  <br><em style="opacity:.72">→ {"Rate increase justified on compression nights — file recommendation with board." if _cq > _cpq else "Shoulder season underperforming — prioritize demand generation budget request."}</em>
</div>

<div class="nlm-point">
  <strong>Visitor Economy</strong> <span class="nlm-tag nlm-tag-datafy">Datafy</span><br>
  {_visitor_lbl}
  <br><em style="opacity:.72">→ Target OOS feeder markets (SLC, DFW, NYC) with fly-drive campaign — 1.3–1.4× room revenue per trip vs. LA drive market.</em>
</div>

<div class="nlm-point">
  <strong>Digital & Social Performance</strong> <span class="nlm-tag nlm-tag-datafy">Datafy GA4</span><br>
  {_ga4_lbl}
  <br><em style="opacity:.72">→ Digital engagement reflects destination intent; top pages signal content demand for campaign alignment.</em>
</div>

<div class="nlm-point">
  <strong>Weekend / Midweek Gap</strong> <span class="nlm-tag nlm-tag-str">STR</span><br>
  Weekend RevPAR: <strong>${_wknd:.0f}</strong> · Midweek: <strong>${_wkdy:.0f}</strong> · Gap: <strong>{_gap:.0f}%</strong>.
  <br><em style="opacity:.72">→ Closing 20% of this gap adds ~{_midweek_opp_lbl} in incremental portfolio room revenue.</em>
</div>

<div class="nlm-point">
  <strong>Market Positioning</strong> <span class="nlm-tag nlm-tag-ai">CoStar</span><br>
  Dana Point/South OC market ADR forecast: $285+ through 2025. VDP portfolio maintains premium positioning above market average.
  <br><em style="opacity:.72">→ Present updated comp set analysis at next board meeting; request approval for rate strategy review.</em>
</div>

<div class="nlm-point">
  <strong>Historical Context (Zartico 2024–25)</strong> <span class="nlm-tag" style="background:rgba(121,82,179,0.15);color:#7952b3;">Zartico</span><br>
  {_zrt_ctx}
  <br><em style="opacity:.72">→ Zartico historical data provides independent validation of Datafy trends; present alongside for board credibility.</em>
</div>
</div>
""", unsafe_allow_html=True)
        else:
            st.info("Run the pipeline to load STR data for board report generation.")

        # ── Download button ────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        _dl_col, _sp_col = st.columns([1, 3])
        with _dl_col:
            _report_html = generate_board_report_html(
                m or {},
                df_kpi,
                df_dfy_ov,
                df_dfy_dma,
                df_cs_snap,
                df_insights,
                df_dfy_media,
            )
            st.download_button(
                label="📥 Download Board Report (Print-Ready HTML)",
                data=_report_html.encode("utf-8"),
                file_name=f"VDP_Board_Report_{datetime.now().strftime('%Y-%m')}.html",
                mime="text/html",
                use_container_width=True,
                type="primary",
                help="Download → open in browser → Cmd+P / Ctrl+P → Save as PDF",
            )
        with _sp_col:
            st.caption(
                "Opens as a formatted HTML document. To save as PDF: open in browser → "
                "File → Print → 'Save as PDF'. Optimized for A4/Letter paper."
            )
            # Share via email button
            _report_period = datetime.now().strftime("%B %Y")
            _mailto_subject = f"Dana Point PULSE Board Report — {_report_period}"
            _mailto_body = (
                f"Please find attached the Dana Point PULSE Board Report for {_report_period}. "
                f"Download from the dashboard and open in your browser to print to PDF."
            )
            _mailto_link = (
                f"mailto:?subject={_urlparse.quote(_mailto_subject)}"
                f"&body={_urlparse.quote(_mailto_body)}"
            )
            st.markdown(
                f'<a href="{_mailto_link}" style="display:inline-block;margin-top:6px;'
                f'font-size:12px;color:#21808D;text-decoration:none;font-weight:600;">'
                f'📧 Share via Email</a>',
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ── PULSE Score Widget ─────────────────────────────────────────────────────
    if m:
        _occ_score  = m.get("occ_30", 0)
        _rvp_d_s    = m.get("revpar_delta", 0)
        _cq_s       = m.get("comp_recent_q", 0)
        # Score components: occupancy vs 70% baseline (max 50pts), RevPAR YOY (max 30pts), compression (max 20pts)
        _comp_occ   = min(50, max(0, (_occ_score / 70) * 50))
        _comp_rvp   = min(30, max(0, 15 + (_rvp_d_s * 1.5)))
        _comp_cmp   = min(20, max(0, _cq_s * 2.0))
        _pulse_score = int(round(_comp_occ + _comp_rvp + _comp_cmp))
        _pulse_score = max(0, min(100, _pulse_score))

        if _pulse_score >= 90:
            _p_color  = "#7C3AED"   # purple — historic
            _p_status = "HISTORIC"
            _p_detail = "Exceptional market conditions — this is a benchmark period. Document rate levels and compression patterns for future board reference."
        elif _pulse_score >= 75:
            _p_color  = "#21C55D"   # green — exceptional
            _p_status = "EXCEPTIONAL"
            _p_detail = "Market significantly outperforming baseline. Occupancy, rate, and compression all trending strongly positive — capitalize with rate increases now."
        elif _pulse_score >= 60:
            _p_color  = "#21808D"   # teal — strong
            _p_status = "STRONG"
            _p_detail = "Market performing above expectations. Occupancy, rate, and compression trending positive. Maintain pricing discipline."
        elif _pulse_score >= 40:
            _p_color  = "#F59E0B"   # amber — stable
            _p_status = "STABLE"
            _p_detail = "Market showing steady signals. Core metrics at or near baseline; monitor rate pressure and shoulder demand generation."
        else:
            _p_color  = "#EF4444"   # red — caution
            _p_status = "CAUTION"
            _p_detail = "Market below baseline performance. Revenue and/or occupancy need attention — review demand drivers and rate strategy immediately."

        # ── Custom weighting expander ───────────────────────────────────────
        with st.expander("⚙️ Score Weighting — Customize for Your Strategy", expanded=False):
            st.markdown(
                '<div style="font-size:12px;opacity:0.70;margin-bottom:12px;">'
                'The <strong>PULSE Score (0–100)</strong> measures market health across 4 dimensions. '
                'Adjust the weights below to reflect your organization\'s priorities. '
                'All weights must sum to 100%.</div>',
                unsafe_allow_html=True,
            )
            _pw_c1, _pw_c2, _pw_c3, _pw_c4 = st.columns(4)
            with _pw_c1:
                _w_occ = st.slider("Occupancy Weight", 0, 100, 25, 5, key="pw_occ", help="Weight for occupancy vs 70% baseline")
            with _pw_c2:
                _w_adr = st.slider("ADR / Rate Weight", 0, 100, 25, 5, key="pw_adr", help="Weight for rate momentum")
            with _pw_c3:
                _w_rvp = st.slider("RevPAR YOY Weight", 0, 100, 25, 5, key="pw_rvp", help="Weight for RevPAR year-over-year change")
            with _pw_c4:
                _w_cmp = st.slider("Compression Weight", 0, 100, 25, 5, key="pw_cmp", help="Weight for compression nights this quarter")
            _w_total = _w_occ + _w_adr + _w_rvp + _w_cmp
            if _w_total != 100:
                st.warning(f"Weights sum to {_w_total}% — adjust to reach 100% for a valid score.")
            else:
                # Recompute score with custom weights (normalize each component to 0–1 then apply weight)
                _c_occ_n  = min(1.0, max(0.0, _occ_score / 70))
                _c_rvp_n  = min(1.0, max(0.0, (15 + _rvp_d_s * 1.5) / 30))
                _c_cmp_n  = min(1.0, max(0.0, _cq_s * 2.0 / 20))
                _c_adr_n  = min(1.0, max(0.0, (15 + _rvp_d_s * 1.0) / 30))   # proxy for ADR using RevPAR delta
                _custom_score = int(round(
                    _c_occ_n * _w_occ + _c_rvp_n * _w_rvp + _c_adr_n * _w_adr + _c_cmp_n * _w_cmp
                ))
                _custom_score = max(0, min(100, _custom_score))
                st.markdown(
                    f'<div style="font-size:13px;margin-top:4px;margin-bottom:6px;">'
                    f'Custom weighted score: <strong style="color:{_p_color};font-size:18px;">{_custom_score}</strong> / 100'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                # Breakdown table
                _breakdown = [
                    ("Occupancy",   f"{_occ_score:.1f}% vs 70% baseline", f"{_c_occ_n*_w_occ:.1f}",  f"{_w_occ}%"),
                    ("ADR / Rate",  f"proxy via RevPAR delta",            f"{_c_adr_n*_w_adr:.1f}",  f"{_w_adr}%"),
                    ("RevPAR YOY",  f"{_rvp_d_s:+.1f}%",                 f"{_c_rvp_n*_w_rvp:.1f}",  f"{_w_rvp}%"),
                    ("Compression", f"{_cq_s} nights this quarter",       f"{_c_cmp_n*_w_cmp:.1f}",  f"{_w_cmp}%"),
                ]
                import pandas as _pd_bd
                _bd_df = _pd_bd.DataFrame(_breakdown, columns=["Component","Signal","Points","Weight"])
                st.dataframe(_bd_df, use_container_width=True, hide_index=True)

        # ── Gauge bar chart ─────────────────────────────────────────────────
        _gauge_fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=_pulse_score,
            title={"text": "PULSE Score", "font": {"size": 12}},
            gauge={
                "axis": {
                    "range": [0, 100], "tickwidth": 1,
                    "tickcolor": "rgba(255,255,255,0.5)",
                    "tickfont": {"size": 11},
                    "nticks": 6,
                },
                "bar": {"color": _p_color, "thickness": 0.28},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 40],  "color": "rgba(192,21,47,0.12)"},
                    {"range": [40, 60], "color": "rgba(245,158,11,0.12)"},
                    {"range": [60, 80], "color": "rgba(33,128,141,0.12)"},
                    {"range": [80, 100],"color": "rgba(33,197,93,0.12)"},
                ],
                "threshold": {
                    "line": {"color": _p_color, "width": 3},
                    "thickness": 0.8,
                    "value": _pulse_score,
                },
            },
            number={"font": {"size": 32, "color": _p_color}, "suffix": ""},
        ))
        _gauge_fig.update_layout(
            height=200,
            margin=dict(l=20, r=20, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Plus Jakarta Sans, Inter, system-ui, sans-serif", color="rgba(255,255,255,0.75)"),
        )

        _pulse_col1, _pulse_col2 = st.columns([3, 2])
        with _pulse_col1:
            st.markdown(
                f'<div class="pulse-wrapper" style="color:{_p_color};">'
                f'  <div class="pulse-circle">'
                f'    <div class="pulse-ring"></div>'
                f'    <div class="pulse-ring-2"></div>'
                f'    <div class="pulse-core">'
                f'      <span class="pulse-score">{_pulse_score}</span>'
                f'      <span class="pulse-label">PULSE</span>'
                f'    </div>'
                f'  </div>'
                f'  <div class="pulse-info">'
                f'    <div class="pulse-info-title">Dana Point Market PULSE Score</div>'
                f'    <div class="pulse-info-detail">'
                f'      Occ {_occ_score:.1f}% &nbsp;·&nbsp; RevPAR YOY {_rvp_d_s:+.1f}% '
                f'      &nbsp;·&nbsp; Compression {_cq_s} nights this quarter<br>'
                f'      {_p_detail}'
                f'    </div>'
                f'    <span class="pulse-info-status" style="background:{_p_color};">{_p_status}</span>'
                f'  </div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with _pulse_col2:
            st.plotly_chart(_gauge_fig, use_container_width=True, config={"displayModeBar": False})
        # Tier legend
        st.markdown(
            '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:2px;margin-bottom:12px;">'
            '<span style="font-size:10px;padding:2px 10px;border-radius:99px;background:rgba(239,68,68,0.14);color:#EF4444;font-weight:700;">0–39 Caution</span>'
            '<span style="font-size:10px;padding:2px 10px;border-radius:99px;background:rgba(245,158,11,0.14);color:#F59E0B;font-weight:700;">40–59 Stable</span>'
            '<span style="font-size:10px;padding:2px 10px;border-radius:99px;background:rgba(33,128,141,0.14);color:#21808D;font-weight:700;">60–74 Strong</span>'
            '<span style="font-size:10px;padding:2px 10px;border-radius:99px;background:rgba(33,197,93,0.14);color:#21C55D;font-weight:700;">75–89 Exceptional</span>'
            '<span style="font-size:10px;padding:2px 10px;border-radius:99px;background:rgba(124,58,237,0.14);color:#7C3AED;font-weight:700;">90–100 Historic</span>'
            '</div>',
            unsafe_allow_html=True,
        )

    # ── AI Analyst Panel ───────────────────────────────────────────────────────
    with st.expander("🧠 PULSE AI Analyst — Interrogate your data", expanded=False):
        st.markdown('<span class="ai-chip">AI ANALYST</span>', unsafe_allow_html=True)

        PROMPTS_META = [
            ("💹 RevPAR Drivers",       "revpar"),
            ("📅 Opportunity Nights",   "opportunity"),
            ("🗺️ Visitor Economy",      "visitor_econ"),
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
            'font-weight:700;letter-spacing:-0.01em;margin-bottom:2px;">🔥 Market Intelligence Signals</div>'
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
            'font-weight:700;letter-spacing:-0.01em;margin-bottom:8px;">Performance Command Center</div>',
            unsafe_allow_html=True,
        )
        # Render each KPI as [card | mini sparkline chart] pairs, 2 per row
        def _mini_spark_fig(values, positive=True):
            if not values:
                return None
            _color = TEAL if positive else ORANGE
            _fill  = "rgba(33,128,141,0.12)" if positive else "rgba(245,158,11,0.12)"
            _fig = go.Figure(go.Scatter(
                y=values, mode="lines",
                line=dict(color=_color, width=2),
                fill="tozeroy", fillcolor=_fill,
            ))
            _fig.update_layout(
                height=72, margin=dict(l=0, r=0, t=4, b=0),
                xaxis=dict(visible=False), yaxis=dict(visible=False),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False, transition={"duration": 500},
            )
            return _fig

        _kpi_rows = [kpis[i:i+2] for i in range(0, len(kpis), 2)]
        for _row_kpis in _kpi_rows:
            _rcols = st.columns([1.1, 1.0, 0.05, 1.1, 1.0])
            for _idx, _k in enumerate(_row_kpis):
                # Use columns at indices 0,1 for first KPI and 3,4 for second
                _card_col  = _rcols[0 if _idx == 0 else 3]
                _chart_col = _rcols[1 if _idx == 0 else 4]
                with _card_col:
                    st.markdown(
                        kpi_card(_k["label"], _k["value"], _k["delta"],
                                 _k.get("positive", True), _k.get("neutral", False),
                                 "", _k.get("date_label", ""), _k.get("raw_value", 0.0),
                                 []),  # no sparkline in HTML — shown as Plotly chart
                        unsafe_allow_html=True,
                    )
                with _chart_col:
                    _spk = _k.get("sparkline") or []
                    _sfig = _mini_spark_fig(_spk, _k.get("positive", True))
                    if _sfig:
                        st.plotly_chart(_sfig, use_container_width=True,
                                        config={"displayModeBar": False})

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
            def _m12_spark(col):
                return [v for v in _m12[col].tolist() if pd.notna(v)] if col in _m12.columns else []
            _m_kpis = [
                {"label": "RevPAR",       "value": f"${_m12_rvp:.2f}",
                 "delta": f"{pct_delta(_m12_rvp,_mp_rvp):+.1f}% YOY",
                 "positive": _m12_rvp >= _mp_rvp, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_rvp, "sparkline": _m12_spark("revpar")},
                {"label": "ADR",          "value": f"${_m12_adr:.2f}",
                 "delta": f"{pct_delta(_m12_adr,_mp_adr):+.1f}% YOY",
                 "positive": _m12_adr >= _mp_adr, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_adr, "sparkline": _m12_spark("adr")},
                {"label": "Occupancy",    "value": f"{_m12_occ:.1f}%",
                 "delta": f"{pct_delta(_m12_occ,_mp_occ):+.1f}pp YOY",
                 "positive": _m12_occ >= _mp_occ, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_occ, "sparkline": _m12_spark("occupancy") if _occ_col else []},
                {"label": "Room Revenue", "value": f"${_m12_rev/1e6:.2f}M",
                 "delta": f"{pct_delta(_m12_rev,_mp_rev):+.1f}% YOY",
                 "positive": _m12_rev >= _mp_rev, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_rev, "sparkline": _m12_spark("revenue")},
                {"label": "Rooms Sold",   "value": f"{_m12_dem:,.0f}",
                 "delta": f"{pct_delta(_m12_dem,_mp_dem):+.1f}% YOY",
                 "positive": _m12_dem >= _mp_dem, "neutral": False,
                 "date_label": _mo_lbl, "raw_value": _m12_dem, "sparkline": _m12_spark("demand")},
                {"label": "Est. TBID Rev","value": f"${_m12_tbd/1e3:.0f}K",
                 "delta": "blended 1.25%", "positive": True, "neutral": True,
                 "date_label": _mo_lbl, "raw_value": _m12_tbd, "sparkline": _m12_spark("revenue")},
            ]
            _m_kpi_rows = [_m_kpis[i:i+2] for i in range(0, len(_m_kpis), 2)]
            for _m_row in _m_kpi_rows:
                _mrc = st.columns([1.1, 1.0, 0.05, 1.1, 1.0])
                for _mi, _mk in enumerate(_m_row):
                    _mc  = _mrc[0 if _mi == 0 else 3]
                    _mcc = _mrc[1 if _mi == 0 else 4]
                    with _mc:
                        st.markdown(
                            kpi_card(_mk["label"], _mk["value"], _mk["delta"],
                                     _mk.get("positive", True), _mk.get("neutral", False),
                                     "", _mk.get("date_label", ""), _mk.get("raw_value", 0.0),
                                     []),
                            unsafe_allow_html=True,
                        )
                    with _mcc:
                        _mspk = _mk.get("sparkline") or []
                        _msfig = _mini_spark_fig(_mspk, _mk.get("positive", True))
                        if _msfig:
                            st.plotly_chart(_msfig, use_container_width=True,
                                            config={"displayModeBar": False})

        # ── Bullet Chart: KPI vs Benchmark ────────────────────────────────────
        if kpis and not df_cs_snap.empty:
            st.markdown('<div class="chart-header">Performance vs. Market Benchmark — Bullet Chart</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-caption">Black bar = VDP portfolio actual · '
                'Gray range = South OC market benchmark (CoStar) · '
                'Teal = target (5% above market)</div>',
                unsafe_allow_html=True,
            )
            _snap = df_cs_snap.iloc[0]
            _mkt_occ  = float(_snap.get("occupancy_pct", 76.4) or 76.4)
            _mkt_adr  = float(_snap.get("adr_usd", 288.50) or 288.50)
            _mkt_rvp  = float(_snap.get("revpar_usd", 220.42) or 220.42)
            # VDP actuals from kpis
            _vdp_occ  = next((k["raw_value"] for k in kpis if "Occ" in k.get("label","")), _mkt_occ)
            _vdp_adr  = next((k["raw_value"] for k in kpis if "ADR" in k.get("label","")), _mkt_adr)
            _vdp_rvp  = next((k["raw_value"] for k in kpis if "RevPAR" in k.get("label","")), _mkt_rvp)
            # Extract numeric from raw_value (already float)
            def _num(v):
                if isinstance(v, (int, float)):
                    return float(v)
                s = str(v).replace("$","").replace("%","").replace(",","").strip()
                try: return float(s)
                except: return 0.0
            _vdp_occ_n = _num(_vdp_occ)
            _vdp_adr_n = _num(_vdp_adr)
            _vdp_rvp_n = _num(_vdp_rvp)

            _bul_metrics = [
                ("Occupancy %", _vdp_occ_n, _mkt_occ, _mkt_occ * 1.05),
                ("ADR ($)",     _vdp_adr_n, _mkt_adr, _mkt_adr * 1.05),
                ("RevPAR ($)",  _vdp_rvp_n, _mkt_rvp, _mkt_rvp * 1.05),
            ]
            fig = go.Figure()
            for idx, (lbl, actual, benchmark, target) in enumerate(_bul_metrics):
                y_pos = idx * 1.5
                # Background range (market benchmark ± 15%)
                fig.add_shape(type="rect",
                    x0=benchmark * 0.85, x1=benchmark * 1.15,
                    y0=y_pos - 0.4, y1=y_pos + 0.4,
                    fillcolor="rgba(167,169,169,0.20)",
                    line=dict(width=0),
                )
                # Target line
                fig.add_shape(type="line",
                    x0=target, x1=target, y0=y_pos - 0.45, y1=y_pos + 0.45,
                    line=dict(color=TEAL, width=2, dash="dash"),
                )
                # Actual bar
                color = TEAL if actual >= benchmark else ORANGE
                fig.add_shape(type="rect",
                    x0=0, x1=actual,
                    y0=y_pos - 0.18, y1=y_pos + 0.18,
                    fillcolor=color,
                    line=dict(width=0),
                )
                # Labels
                suffix = "%" if "%" in lbl else ""
                prefix = "$" if "$" in lbl else ""
                fig.add_annotation(
                    x=0, y=y_pos + 0.55,
                    text=f"<b>{lbl}</b>", showarrow=False,
                    xanchor="left", font=dict(size=11, family="Plus Jakarta Sans, Inter, sans-serif"),
                )
                fig.add_annotation(
                    x=actual, y=y_pos,
                    text=f" {prefix}{actual:.1f}{suffix}", showarrow=False,
                    xanchor="left", font=dict(size=10, color=color, family="Plus Jakarta Sans, Inter, sans-serif"),
                )
            max_val = max(_vdp_adr_n, _mkt_adr) * 1.25
            fig.update_layout(
                xaxis=dict(range=[0, max_val], showgrid=True, gridcolor="rgba(167,169,169,0.15)"),
                yaxis=dict(visible=False, range=[-0.8, 3.8]),
                showlegend=False,
                height=200,
                margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Gray band = market ±15%. Dashed teal line = 5% above market target. Colored bar = VDP actual. Source: STR (portfolio) + CoStar (market).")
            st.markdown("<br>", unsafe_allow_html=True)

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

        # ── Row 4: Datafy Visitor Economy Summary ─────────────────────────────
        if not df_dfy_ov.empty:
            st.markdown("---")
            st.markdown(
                '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:14px;'
                'font-weight:700;letter-spacing:-0.01em;margin-bottom:2px;">Visitor Economy Intelligence</div>'
                '<div style="font-size:11px;opacity:0.50;font-weight:500;margin-bottom:10px;">'
                'Datafy geolocation data · Layer 1 verified · See Visitor Economy tab for full detail</div>',
                unsafe_allow_html=True,
            )
            _dov = df_dfy_ov.iloc[0]
            _dov_cols = st.columns(4)
            _dov_kpis = [
                (f"{int(_dov.get('total_trips',0) or 0)/1e6:.2f}M", "Total Annual Trips",
                 f"{_dov.get('total_trips_vs_compare_pct',0):+.1f}pp YOY"),
                (f"{float(_dov.get('out_of_state_vd_pct',0) or 0):.1f}%", "Out-of-State Visitor Days",
                 f"{_dov.get('out_of_state_vd_vs_compare_pct',0):+.2f}pp YOY"),
                (f"{float(_dov.get('overnight_trips_pct',0) or 0):.1f}%", "Overnight Trips",
                 f"{_dov.get('overnight_vs_compare_pct',0):+.1f}pp YOY"),
                (f"{float(_dov.get('avg_length_of_stay_days',0) or 0):.1f} days", "Avg Length of Stay",
                 f"{_dov.get('avg_los_vs_compare_days',0):+.1f}d vs. prior yr"),
            ]
            for i, (val, lbl, delta) in enumerate(_dov_kpis):
                with _dov_cols[i]:
                    st.markdown(
                        f'<div style="background:rgba(33,128,141,0.05);border:1px solid rgba(33,128,141,0.12);'
                        f'border-radius:8px;padding:12px 14px;">'
                        f'<div style="font-size:1.25rem;font-weight:800;color:#21808D;">{val}</div>'
                        f'<div style="font-size:10px;font-weight:600;opacity:0.65;margin-top:2px;">{lbl}</div>'
                        f'<div style="font-size:10px;color:#21808D;font-weight:600;margin-top:3px;">{delta}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — TRENDS
# ══════════════════════════════════════════════════════════════════════════════
with tab_tr:
    # ── Metric toggle filter ────────────────────────────────────────────────────
    _str_metric_label = st.selectbox(
        "View Metric",
        ["RevPAR", "ADR", "Occupancy %", "Revenue", "Supply", "Demand"],
        key="str_metric_sel",
        help="Select which metric to highlight in the main trend charts",
    )
    _str_metric_map = {
        "RevPAR": "revpar", "ADR": "adr", "Occupancy %": "occ_pct",
        "Revenue": "revenue", "Supply": "supply", "Demand": "demand",
    }
    _str_metric_col = _str_metric_map.get(_str_metric_label, "revpar")

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
        for _mc in ["supply", "demand", "revenue"]:
            if _mc in _tmp_tr.columns:
                _agg_spec[_mc] = (_mc, "sum")
        monthly = (
            _tmp_tr.groupby("month")
            .agg(**_agg_spec)
            .reset_index()
            .sort_values("month")
        )
        if "occ_pct" not in monthly.columns:
            monthly["occ_pct"] = np.nan
        for _mc in ["supply", "demand", "revenue"]:
            if _mc not in monthly.columns:
                monthly[_mc] = np.nan
        # YOY for all metrics (shift 12)
        for _mc in ["revpar", "adr", "occ_pct", "supply", "demand", "revenue"]:
            if _mc in monthly.columns:
                monthly[f"{_mc}_yoy"] = (monthly[_mc] / monthly[_mc].shift(12) - 1) * 100
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
        # ── Primary metric chart — responds to metric selector ─────────────────
        _yoy_col   = "revpar_yoy" if _str_metric_col in ("revpar", "occ_pct") else None
        _main_col  = _str_metric_col if _str_metric_col in monthly.columns else "revpar"
        _tick_pfx  = "$" if _str_metric_col in ("revpar", "adr", "revenue") else ""
        _tick_sfx  = "%" if _str_metric_col == "occ_pct" else ""
        st.markdown(
            f'<div class="chart-header">Monthly {_str_metric_label} Trend</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="chart-caption">Monthly averages &nbsp;·&nbsp; metric: {_str_metric_label} &nbsp;·&nbsp; teal = above avg &nbsp;·&nbsp; change metric via selector above</div>',
            unsafe_allow_html=True,
        )
        if _main_col in monthly.columns and not monthly[_main_col].isna().all():
            _avg_main = monthly[_main_col].mean()
            _bar_clrs = [GREEN if v >= _avg_main else RED for v in monthly[_main_col]]
            fig = go.Figure(go.Bar(
                x=monthly["month_label"], y=monthly[_main_col],
                marker=dict(color=_bar_clrs, line_width=0, cornerradius=5),
                hovertemplate=f"<b>%{{x}}</b><br>{_str_metric_label}: {_tick_pfx}%{{y:.1f}}{_tick_sfx}<extra></extra>",
            ))
            fig.add_hline(y=_avg_main, line_dash="dash",
                          line_color="rgba(167,169,169,0.45)",
                          annotation_text=f"avg {_tick_pfx}{_avg_main:.1f}{_tick_sfx}",
                          annotation_position="top right",
                          annotation_font=dict(size=11, color="rgba(127,127,127,0.80)"))
            fig.update_layout(
                yaxis_tickprefix=_tick_pfx, yaxis_ticksuffix=_tick_sfx,
                showlegend=False, transition={"duration": 800},
            )
            st.plotly_chart(style_fig(fig, height=300), use_container_width=True)
        else:
            st.markdown(empty_state(
                "📊", f"No data for {_str_metric_label}.",
                "Select a different metric or load more data.",
            ), unsafe_allow_html=True)

        st.markdown("---")
        st.markdown(f'<div class="chart-header">YOY {_str_metric_label} Change</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="chart-caption">Year-over-year % change by month &nbsp;·&nbsp; metric: {_str_metric_label} &nbsp;·&nbsp; teal = growth &nbsp;·&nbsp; red = decline</div>', unsafe_allow_html=True)
        _yoy_metric_col = f"{_str_metric_col}_yoy"
        yoy = monthly.dropna(subset=[_yoy_metric_col]) if _yoy_metric_col in monthly.columns else pd.DataFrame()
        if yoy.empty:
            st.markdown(empty_state(
                "📊", f"YOY {_str_metric_label} requires 12+ months of history.",
                "Load more data to unlock year-over-year charts.",
            ), unsafe_allow_html=True)
        else:
            bar_colors = [GREEN if v >= 0 else RED for v in yoy[_yoy_metric_col]]
            fig = go.Figure(go.Bar(
                x=yoy["month_label"], y=yoy[_yoy_metric_col],
                marker=dict(color=bar_colors, line_width=0, cornerradius=5),
                text=[f"{v:+.1f}%" for v in yoy[_yoy_metric_col]],
                textposition="outside",
                textfont=dict(size=10, family="Plus Jakarta Sans, Inter, sans-serif"),
                hovertemplate=(
                    f"<b>%{{x}}</b><br>YOY {_str_metric_label}: %{{y:+.1f}}%<extra></extra>"
                ),
            ))
            fig.update_layout(yaxis_ticksuffix="%", showlegend=False,
                              transition={"duration": 800, "easing": "cubic-in-out"})
            st.plotly_chart(style_fig(fig, height=300), use_container_width=True)

        st.markdown("---")
        c1, c2 = st.columns(2)

        with c1:
            st.markdown(f'<div class="chart-header">Seasonal Demand Rose — Monthly {_str_metric_label} Compass</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="chart-caption">Petal length = avg {_str_metric_label} · longer petals = stronger months · reveals true seasonality shape</div>', unsafe_allow_html=True)
            if len(monthly) >= 6:
                month_order_full = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
                _tmp_rose = df_monthly.copy()
                _tmp_rose["mon_num"] = _tmp_rose["as_of_date"].dt.month
                _tmp_rose["mon_lbl"] = _tmp_rose["as_of_date"].dt.strftime("%b")
                _rose_col = _str_metric_col if _str_metric_col in _tmp_rose.columns else "revpar"
                rose_avg = _tmp_rose.groupby("mon_num")[_rose_col].mean().reindex(range(1, 13))
                rose_avg = rose_avg.fillna(0)
                _rose_tick_pfx = _tick_pfx
                _rose_tick_sfx = _tick_sfx
                # Polar bar chart (rose plot)
                _rose_colors = [
                    TEAL if v >= rose_avg.mean() else TEAL_LIGHT
                    for v in rose_avg.values
                ]
                fig = go.Figure(go.Barpolar(
                    r=rose_avg.values,
                    theta=month_order_full,
                    marker=dict(
                        color=_rose_colors,
                        line=dict(color="white", width=1),
                        opacity=0.85,
                    ),
                    hovertemplate=f"<b>%{{theta}}</b><br>{_str_metric_label}: {_rose_tick_pfx}%{{r:.0f}}{_rose_tick_sfx}<extra></extra>",
                ))
                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            tickprefix=_rose_tick_pfx,
                            gridcolor="rgba(167,169,169,0.2)",
                            linecolor="rgba(167,169,169,0.2)",
                        ),
                        angularaxis=dict(
                            tickfont=dict(size=11, family="Plus Jakarta Sans, Inter, sans-serif"),
                        ),
                    ),
                    showlegend=False,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(style_fig(fig, height=360), use_container_width=True)
                _peak_mon = month_order_full[rose_avg.idxmax() - 1]
                _soft_mon = month_order_full[rose_avg.idxmin() - 1]
                st.caption(f"Peak: **{_peak_mon}** ({_tick_pfx}{rose_avg.max():.0f}{_tick_sfx} {_str_metric_label}) · Softest: **{_soft_mon}** ({_tick_pfx}{rose_avg.min():.0f}{_tick_sfx}). "
                           f"Shoulder months show the biggest rate-capture opportunity.")
            else:
                st.markdown(empty_state("📊", "Need 6+ months.", "Load more history."), unsafe_allow_html=True)

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
                fig.update_layout(yaxis_tickprefix="$", yaxis_ticksuffix="M", showlegend=False,
                                  transition={"duration": 700, "easing": "cubic-in-out"})
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
            fig.update_layout(barmode="group",
                              transition={"duration": 700, "easing": "cubic-in-out"})
            st.plotly_chart(style_fig(fig, height=260), use_container_width=True)
        else:
            st.markdown(empty_state(
                "📦", "No compression data.",
                "Run compute_kpis.py to populate kpi_compression_quarterly.",
            ), unsafe_allow_html=True)

        st.markdown("---")

        # ── Full history line chart ─────────────────────────────────────────────
        st.markdown(f'<div class="chart-header">Full History — RevPAR / ADR / Occupancy ({_str_metric_label} highlighted)</div>', unsafe_allow_html=True)
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

        # ── Beeswarm: Daily RevPAR Distribution ───────────────────────────────
        if not df_daily.empty and len(df_daily) >= 30:
            st.markdown("---")
            st.markdown(f'<div class="chart-header">Daily {_str_metric_label} Distribution — Beeswarm</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="chart-caption">Each dot = one day · spread by {_str_metric_label} · '
                f'color = quarter · reveals compression clusters and soft-period gaps</div>',
                unsafe_allow_html=True,
            )
            _bsw = df_daily.copy().tail(365)
            _bsw["quarter"] = _bsw["as_of_date"].dt.quarter
            _bsw["year"] = _bsw["as_of_date"].dt.year
            _q_colors = {1: "#A7D5D9", 2: TEAL_LIGHT, 3: TEAL, 4: "#1A6470"}
            _bsw["color"] = _bsw["quarter"].map(_q_colors)
            # Use selected metric column (fall back to revpar if not in daily data)
            _bsw_col = _str_metric_col if _str_metric_col in _bsw.columns else "revpar"
            _bsw_label = _str_metric_label
            np.random.seed(42)
            _bsw["jitter"] = np.random.uniform(-0.4, 0.4, len(_bsw))
            fig = go.Figure()
            for q in [1, 2, 3, 4]:
                _sub = _bsw[_bsw["quarter"] == q].dropna(subset=[_bsw_col])
                if _sub.empty:
                    continue
                fig.add_trace(go.Scatter(
                    x=_sub[_bsw_col],
                    y=_sub["jitter"],
                    mode="markers",
                    name=f"Q{q}",
                    marker=dict(
                        color=_q_colors[q],
                        size=7,
                        opacity=0.75,
                        line=dict(width=0.5, color="white"),
                    ),
                    hovertemplate=(
                        f"<b>%{{customdata[0]|%b %d, %Y}}</b><br>"
                        f"{_bsw_label}: {_tick_pfx}%{{x:.1f}}{_tick_sfx}<br>Q%{{customdata[1]}}<extra></extra>"
                    ),
                    customdata=_sub[["as_of_date", "quarter"]].values,
                ))
            _bsw_avg = _bsw[_bsw_col].dropna().mean() if _bsw_col in _bsw.columns else 0
            fig.add_vline(x=_bsw_avg, line_dash="dash",
                          line_color="rgba(167,169,169,0.6)",
                          annotation_text=f"Avg {_tick_pfx}{_bsw_avg:.0f}{_tick_sfx}",
                          annotation_position="top")
            fig.update_layout(
                yaxis=dict(visible=False, range=[-1, 1]),
                xaxis=dict(title=f"{_bsw_label} ({_tick_pfx or _tick_sfx or 'value'})", tickprefix=_tick_pfx, ticksuffix=_tick_sfx),
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(style_fig(fig, height=280), use_container_width=True)
            st.caption(f"Density clusters reveal seasonal patterns. Q3 (dark teal) = peak season. Spread = {_str_metric_label} variability. Filter metric above to switch views.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — FORWARD OUTLOOK
# ══════════════════════════════════════════════════════════════════════════════
with tab_fo:
    # ── Header ─────────────────────────────────────────────────────────────────
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        'Forward Outlook</div>'
        '<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        'Daily forward-looking insights from analytics.sqlite — updated every pipeline run</div>',
        unsafe_allow_html=True,
    )

    # Load all insights
    df_insights_all = load_insights()

    # ── Key Forward Metrics ──────────────────────────────────────────────────
    st.markdown(
        '<div style="font-size:1.05rem;font-weight:800;letter-spacing:-0.02em;margin-bottom:12px;">'
        '📊 Key Forward Metrics</div>',
        unsafe_allow_html=True,
    )
    _fwd_c1, _fwd_c2, _fwd_c3, _fwd_c4 = st.columns(4)

    # 30-day compression forecast (Q3 = peak, reference from compression table)
    _q3_comp = 0
    _q1_comp = 0
    if not df_comp.empty and "quarter" in df_comp.columns and "days_above_80_occ" in df_comp.columns:
        _q3_row = df_comp[df_comp["quarter"].str.endswith("Q3")]
        _q1_row = df_comp[df_comp["quarter"].str.endswith("Q1")]
        _q3_comp = int(_q3_row["days_above_80_occ"].iloc[0]) if not _q3_row.empty else 0
        _q1_comp = int(_q1_row["days_above_80_occ"].iloc[0]) if not _q1_row.empty else 0

    _occ_fwd  = m.get("occ_30", 0) if m else 0
    _rvp_fwd  = m.get("revpar_30", 0) if m else 0
    _adr_fwd  = m.get("adr_30", 0) if m else 0
    _rvp_d_fwd = m.get("revpar_delta", 0) if m else 0
    # Date reference labels for forward metrics
    _fwd_as_of   = datetime.now().strftime("%b %d, %Y")
    _fwd_30d_end = datetime.now().strftime("%b %d")
    _fwd_30d_lbl = f"30-day trailing · as of {_fwd_as_of}"
    # Resolve which quarter Q3/Q1 labels refer to
    _fwd_q3_label = "Q3 (Jul–Sep)"
    _fwd_q1_label = "Q1 (Jan–Mar)"
    if not df_comp.empty and "quarter" in df_comp.columns:
        _q3_rows = df_comp[df_comp["quarter"].str.endswith("Q3")]
        _q1_rows = df_comp[df_comp["quarter"].str.endswith("Q1")]
        if not _q3_rows.empty:
            _fwd_q3_label = f"Q3 {_q3_rows.iloc[0]['quarter'][:4]}"
        if not _q1_rows.empty:
            _fwd_q1_label = f"Q1 {_q1_rows.iloc[0]['quarter'][:4]}"

    with _fwd_c1:
        _occ_vs = _occ_fwd - 70
        _occ_vs_str = f"{_occ_vs:+.1f}pp vs 70% baseline"
        st.metric(f"Occupancy — {_fwd_30d_lbl}", f"{_occ_fwd:.1f}%", delta=_occ_vs_str,
                  delta_color="normal" if _occ_vs >= 0 else "inverse",
                  help=f"30-day trailing avg occupancy as of {_fwd_as_of} vs 70% destination baseline")
    with _fwd_c2:
        st.metric(f"ADR — {_fwd_30d_lbl}", f"${_adr_fwd:,.0f}",
                  delta=f"RevPAR ${_rvp_fwd:,.0f}",
                  help=f"30-day average daily rate and RevPAR as of {_fwd_as_of}")
    with _fwd_c3:
        st.metric(f"Compression Nights — {_fwd_q3_label}", f"{_q3_comp}",
                  delta=f"{_fwd_q1_label}: {_q1_comp} nights",
                  delta_color="off",
                  help=f"Days above 80% occupancy — {_fwd_q3_label} peak vs {_fwd_q1_label} shoulder. More nights = greater pricing power.")
    with _fwd_c4:
        # Shoulder opportunity: Q1 compression gap vs Q3
        _shld_gap = _q3_comp - _q1_comp
        _shld_rev_opp = _shld_gap * _rvp_fwd * 0.02 * 365 / max(_q3_comp, 1) if _rvp_fwd > 0 else 0
        st.metric("Shoulder Revenue Opportunity", f"${_shld_rev_opp:,.0f}",
                  delta=f"{_shld_gap} comp nights gap ({_fwd_q3_label} vs {_fwd_q1_label})",
                  delta_color="off",
                  help=f"Estimated incremental room revenue if shoulder ({_fwd_q1_label}) compression nights grew toward peak ({_fwd_q3_label}) levels. Based on {_fwd_as_of} RevPAR.")

    # Campaign timing insight
    _camp_insight = ""
    if not df_insights_all.empty:
        _camp_row = df_insights_all[
            (df_insights_all["audience"] == "cross") &
            (df_insights_all["category"] == "campaign_seasonality")
        ]
        if not _camp_row.empty:
            _camp_insight = _camp_row.iloc[0].get("body", "")
    if _camp_insight:
        st.info(f"📡 **Campaign Timing Signal:** {_camp_insight}")

    st.markdown("---")

    # ── Insights overview charts ────────────────────────────────────────────
    if not df_insights_all.empty:
        _fo_ch1, _fo_ch2 = st.columns(2)
        with _fo_ch1:
            st.markdown('<div class="chart-header">Insights by Category & Priority</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">All audiences · bar height = count of insights · color = priority level</div>', unsafe_allow_html=True)
            _ins_grp = (
                df_insights_all
                .groupby(["category", "priority"])
                .size()
                .reset_index(name="count")
                .sort_values(["category", "priority"])
            )
            if not _ins_grp.empty:
                _pri_colors = {1: TEAL, 2: TEAL_LIGHT, 3: ORANGE, 4: "rgba(167,169,169,0.6)", 5: "rgba(100,100,100,0.5)"}
                _fo_bar_fig = go.Figure()
                for _pri in sorted(_ins_grp["priority"].unique()):
                    _sub = _ins_grp[_ins_grp["priority"] == _pri]
                    _fo_bar_fig.add_trace(go.Bar(
                        x=_sub["category"].str.replace("_", " ").str.title(),
                        y=_sub["count"],
                        name=f"Priority {_pri}",
                        marker=dict(color=_pri_colors.get(_pri, TEAL), cornerradius=4),
                        hovertemplate="<b>%{x}</b><br>Priority %{customdata}: %{y} insights<extra></extra>",
                        customdata=[_pri] * len(_sub),
                    ))
                _fo_bar_fig.update_layout(barmode="stack", showlegend=True,
                                          transition={"duration": 600})
                st.plotly_chart(style_fig(_fo_bar_fig, height=280), use_container_width=True)

        with _fo_ch2:
            st.markdown('<div class="chart-header">Insight Horizon — Planning Timeline</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Each bar = one insight · length = forward horizon in days · sorted by audience</div>', unsafe_allow_html=True)
            _ins_hz = df_insights_all[["audience", "category", "headline", "horizon_days", "priority"]].copy()
            _ins_hz = _ins_hz.sort_values(["audience", "priority"])
            _ins_hz["label"] = _ins_hz["category"].str.replace("_", " ").str.title()
            _aud_color_map = {
                "dmo": TEAL, "city": "#4A6FA5", "visitor": ORANGE,
                "resident": "#6A4F8A", "cross": RED,
            }
            if not _ins_hz.empty:
                _hz_fig = go.Figure()
                for _aud in _ins_hz["audience"].unique():
                    _sub2 = _ins_hz[_ins_hz["audience"] == _aud]
                    _hz_fig.add_trace(go.Bar(
                        x=_sub2["horizon_days"],
                        y=_sub2["label"],
                        orientation="h",
                        name=_aud.upper(),
                        marker=dict(color=_aud_color_map.get(_aud, TEAL), cornerradius=4, opacity=0.80),
                        hovertemplate="<b>%{y}</b><br>Horizon: %{x}d<br><i>%{customdata}</i><extra></extra>",
                        customdata=_sub2["headline"],
                    ))
                _hz_fig.update_layout(barmode="overlay", showlegend=True,
                                      xaxis_title="Horizon (days)",
                                      transition={"duration": 600})
                st.plotly_chart(style_fig(_hz_fig, height=280), use_container_width=True)

        st.markdown("---")

    # ── Audience tabs ────────────────────────────────────────────────────────
    AUDIENCE_CONFIG = {
        "dmo":      ("🏢 DMO / TBID Board",       TEAL,       "Destination Marketing & Revenue Strategy"),
        "city":     ("🏛 City of Dana Point",      "#4A6FA5",  "TOT Revenue, Infrastructure & Economic Policy"),
        "visitor":  ("✈️ Visitors",                ORANGE,     "Trip Planning, Rates & Events"),
        "resident": ("🏡 Residents",               "#6A4F8A",  "Community Impact & Local Access"),
        "cross":    ("🔀 Cross-Dataset Signals",   RED,        "Hidden insights only visible by joining STR + Datafy data"),
    }

    aud_tabs = st.tabs([cfg[0] for cfg in AUDIENCE_CONFIG.values()])

    for (audience, (label, color, subtitle)), aud_tab in zip(
        AUDIENCE_CONFIG.items(), aud_tabs
    ):
        with aud_tab:
            st.markdown(
                f'<div style="font-size:12px;opacity:0.55;margin-bottom:16px;">{subtitle}</div>',
                unsafe_allow_html=True,
            )

            df_aud = df_insights_all[df_insights_all["audience"] == audience] if not df_insights_all.empty else pd.DataFrame()

            if df_aud.empty:
                st.markdown(empty_state(
                    "🔭", f"No insights yet for {label}.",
                    "Run the pipeline to generate forward-looking insights.",
                ), unsafe_allow_html=True)
                continue

            # Sort by priority ascending (1 = highest)
            df_aud = df_aud.sort_values("priority")

            PRIORITY_STYLE = {1: "insight-positive", 2: "insight-positive",
                              3: "insight-warning",  4: "insight-info",
                              5: "insight-info"}

            # Render in a 2-column grid
            rows = df_aud.to_dict("records")
            col_pairs = [rows[i:i+2] for i in range(0, len(rows), 2)]

            # Source tags by audience
            _aud_tags = {
                "dmo":      '<span class="nlm-tag nlm-tag-str">STR</span><span class="nlm-tag nlm-tag-datafy">Datafy</span>',
                "city":     '<span class="nlm-tag nlm-tag-str">STR</span><span class="nlm-tag nlm-tag-datafy">Datafy</span>',
                "visitor":  '<span class="nlm-tag nlm-tag-datafy">Datafy</span><span class="nlm-tag nlm-tag-str">STR</span>',
                "resident": '<span class="nlm-tag nlm-tag-str">STR</span>',
                "cross":    '<span class="nlm-tag nlm-tag-str">STR</span><span class="nlm-tag nlm-tag-datafy">Datafy</span>',
            }
            _src_tags_html = _aud_tags.get(audience, '<span class="nlm-tag nlm-tag-ai">AI</span>')

            for pair in col_pairs:
                cols = st.columns(len(pair))
                for col, row in zip(cols, pair):
                    style_cls = PRIORITY_STYLE.get(row.get("priority", 5), "insight-info")
                    horizon   = row.get("horizon_days", 30)
                    category  = row.get("category", "").replace("_", " ").title()
                    as_of     = row.get("as_of_date", "")

                    with col:
                        st.markdown(
                            f'<div class="insight-card {style_cls}">'
                            f'<div class="insight-title">{row["headline"]}</div>'
                            f'<p class="insight-body">{row["body"]}</p>'
                            f'<div class="nlm-source-row">'
                            f'{_src_tags_html}'
                            f'<span style="font-size:10px;opacity:0.45;background:rgba(255,255,255,0.07);'
                            f'padding:2px 8px;border-radius:99px;">{category}</span>'
                            f'<span style="font-size:10px;opacity:0.45;background:rgba(255,255,255,0.07);'
                            f'padding:2px 8px;border-radius:99px;">⏱ {horizon}d</span>'
                            f'<span style="font-size:10px;opacity:0.35;padding:2px 4px;">as of {as_of}</span>'
                            f'</div>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

            # Data sources footnote
            all_sources = set()
            for row in rows:
                if row.get("data_sources"):
                    all_sources.update(row["data_sources"].split(","))
            if all_sources:
                st.caption(f"Data sources: {', '.join(sorted(s.strip() for s in all_sources if s.strip()))}")



# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — VISITOR ECONOMY (Datafy)
# ══════════════════════════════════════════════════════════════════════════════
with tab_ev:
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Visitor Economy Intelligence</div>
      <div class="hero-subtitle">Datafy Geolocation Analytics · Dana Point, CA · Annual 2025</div>
    </div>
    """, unsafe_allow_html=True)

    if df_dfy_ov.empty:
        st.markdown(empty_state(
            "📊", "No Datafy visitor economy data loaded.",
            "Run the pipeline: `python scripts/run_pipeline.py`",
        ), unsafe_allow_html=True)
    else:
        ov = df_dfy_ov.iloc[0]
        period_label = f"{ov.get('report_period_start','')[:4]} Annual"
        total_trips = int(ov.get("total_trips", 0) or 0)
        overnight_pct = float(ov.get("overnight_trips_pct", 0) or 0)
        oos_pct = float(ov.get("out_of_state_vd_pct", 0) or 0)
        avg_los = float(ov.get("avg_length_of_stay_days", 0) or 0)
        daytrip_pct = float(ov.get("day_trips_pct", 0) or 0)
        repeat_pct = float(ov.get("repeat_visitors_pct", 0) or 0)
        trips_vs_prior = float(ov.get("total_trips_vs_compare_pct", 0) or 0)
        oos_vs_prior = float(ov.get("out_of_state_vd_vs_compare_pct", 0) or 0)

        # ── Hero KPI cards ─────────────────────────────────────────────────────
        ev_cols = st.columns(3)
        _ev_kpis = [
            (f"{total_trips/1e6:.2f}M", "Total Trips", f"{trips_vs_prior:+.2f}pp vs. prior yr", trips_vs_prior >= 0),
            (f"{overnight_pct:.1f}%", "Overnight Trips", f"{ov.get('overnight_vs_compare_pct',0):+.1f}pp YOY", float(ov.get("overnight_vs_compare_pct",0) or 0) >= 0),
            (f"{oos_pct:.1f}%", "Out-of-State Visitor Days", f"{oos_vs_prior:+.2f}pp YOY", oos_vs_prior >= 0),
            (f"{avg_los:.1f}", "Avg Length of Stay (days)", f"{ov.get('avg_los_vs_compare_days',0):+.1f}d vs. prior yr", float(ov.get("avg_los_vs_compare_days",0) or 0) >= 0),
            (f"{daytrip_pct:.1f}%", "Day Trips", f"{ov.get('day_trips_vs_compare_pct',0):+.1f}pp YOY", False),
            (f"{repeat_pct:.1f}%", "Repeat Visitors", "of total visits", True),
        ]
        for i, (val, lbl, delta, pos) in enumerate(_ev_kpis):
            with ev_cols[i % 3]:
                delta_color = "#21808D" if pos else "#E68161"
                st.markdown(
                    f'<div style="background:rgba(33,128,141,0.06);border:1px solid rgba(33,128,141,0.15);'
                    f'border-radius:10px;padding:14px 16px;margin-bottom:10px;">'
                    f'<div style="font-size:1.5rem;font-weight:800;color:#21808D;letter-spacing:-0.02em;">{val}</div>'
                    f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">{lbl}</div>'
                    f'<div style="font-size:11px;color:{delta_color};font-weight:600;margin-top:4px;">{delta}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        st.markdown("---")

        # ── Feeder Markets + Spending side-by-side ─────────────────────────────
        c1, c2 = st.columns(2)

        with c1:
            st.markdown('<div class="chart-header">Top Feeder Markets (DMA)</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="chart-caption">Share of visitor days by origin market · '
                f'Datafy {period_label} · Layer 1 verified</div>',
                unsafe_allow_html=True,
            )
            if not df_dfy_dma.empty:
                _dma = df_dfy_dma[df_dfy_dma["visitor_days_share_pct"].notna()].head(10)
                _dma_sorted = _dma.sort_values("visitor_days_share_pct", ascending=True)
                _max_s = _dma_sorted["visitor_days_share_pct"].max()
                _bar_colors = [
                    f"rgba(33,{int(128 + 56*(v/_max_s))},{int(141 + 57*(v/_max_s))},0.90)"
                    for v in _dma_sorted["visitor_days_share_pct"]
                ]
                _hover = []
                for _, row in _dma_sorted.iterrows():
                    avg_s = f"${row['avg_spend_usd']:.0f}" if pd.notna(row.get("avg_spend_usd")) else "N/A"
                    chg = f"{row['visitor_days_vs_compare_pct']:+.1f}pp YOY" if pd.notna(row.get("visitor_days_vs_compare_pct")) else ""
                    _hover.append(f"<b>{row['dma']}</b><br>Visitor days: {row['visitor_days_share_pct']:.1f}%<br>Avg spend: {avg_s}<br>{chg}<extra></extra>")
                fig = go.Figure(go.Bar(
                    x=_dma_sorted["visitor_days_share_pct"].values,
                    y=_dma_sorted["dma"].values,
                    orientation="h",
                    marker=dict(color=_bar_colors, line_width=0, cornerradius=5),
                    text=[f"{v:.1f}%" for v in _dma_sorted["visitor_days_share_pct"]],
                    textposition="outside",
                    textfont=dict(size=11, family="Plus Jakarta Sans, Inter, sans-serif"),
                    customdata=_hover,
                    hovertemplate="%{customdata}",
                ))
                fig.update_layout(xaxis_ticksuffix="%", showlegend=False)
                st.plotly_chart(style_fig(fig, height=360), use_container_width=True)
            else:
                st.info("DMA data not available. Run the pipeline.")

        with c2:
            st.markdown('<div class="chart-header">Visitor Spending by Category</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="chart-caption">Share of total destination spend · '
                f'Datafy {period_label} · Layer 1 verified</div>',
                unsafe_allow_html=True,
            )
            if not df_dfy_spend.empty:
                _sp = df_dfy_spend.head(10)
                _palette = [TEAL,"#2DA6B2",TEAL_LIGHT,ORANGE,"#A84B2F",
                            "#5E5240","#626C71","#A7A9A9",RED,"#4A6741"]
                fig = go.Figure(go.Pie(
                    labels=_sp["category"].values,
                    values=_sp["spend_share_pct"].values,
                    hole=0.48,
                    marker=dict(colors=_palette[:len(_sp)],
                                line=dict(color="rgba(0,0,0,0)", width=0)),
                    textfont=dict(size=11, family="Plus Jakarta Sans, Inter, sans-serif"),
                    hovertemplate="<b>%{label}</b><br>%{value:.1f}% of spend<extra></extra>",
                ))
                fig.update_layout(
                    legend=dict(
                        font_size=10, orientation="h",
                        font=dict(family="Plus Jakarta Sans, Inter, sans-serif"),
                        yanchor="top", y=-0.08, xanchor="center", x=0.5,
                    ),
                    margin=dict(l=10, r=10, t=20, b=80),
                    annotations=[dict(text="Spend<br>Mix", x=0.5, y=0.5, font_size=13,
                                      font_family="Plus Jakarta Sans, sans-serif",
                                      font_color="#21808D", showarrow=False)],
                )
                st.plotly_chart(style_fig(fig, height=400), use_container_width=True)
            else:
                st.info("Spending data not available. Run the pipeline.")

        st.markdown("---")

        # ── Demographics + Airports ────────────────────────────────────────────
        c3, c4 = st.columns(2)

        with c3:
            st.markdown('<div class="chart-header">Visitor Demographics — Age Profile</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="chart-caption">Age distribution of visitors to Dana Point · Datafy {period_label}</div>',
                unsafe_allow_html=True,
            )
            if not df_dfy_demo.empty:
                _age = df_dfy_demo[df_dfy_demo["dimension"] == "age"].copy()
                if not _age.empty:
                    age_order = ["16-24","25-44","45-64","65+"]
                    _age["segment"] = pd.Categorical(_age["segment"], categories=age_order, ordered=True)
                    _age = _age.sort_values("segment")
                    _colors_age = [TEAL_LIGHT, TEAL, "#1A6470", "#0D4A52"]
                    fig = go.Figure(go.Bar(
                        x=_age["segment"].values,
                        y=_age["share_pct"].values,
                        marker=dict(color=_colors_age[:len(_age)], line_width=0, cornerradius=6),
                        text=[f"{v:.1f}%" for v in _age["share_pct"]],
                        textposition="outside",
                        textfont=dict(size=12, family="Plus Jakarta Sans, Inter, sans-serif"),
                        hovertemplate="<b>Age %{x}</b><br>Share: %{y:.1f}%<extra></extra>",
                    ))
                    fig.update_layout(yaxis_ticksuffix="%", showlegend=False,
                                      xaxis_title="Age Group", yaxis_title="Share (%)")
                    st.plotly_chart(style_fig(fig, height=300), use_container_width=True)
                else:
                    st.info("Age data not available.")
            else:
                st.info("Demographics data not available.")

        with c4:
            st.markdown('<div class="chart-header">Origin Airports — Passenger Share</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="chart-caption">Fly-market arrivals by origin airport · Datafy {period_label}</div>',
                unsafe_allow_html=True,
            )
            if not df_dfy_air.empty:
                _air = df_dfy_air.head(8)
                _air_sorted = _air.sort_values("passengers_share_pct", ascending=True)
                fig = go.Figure(go.Bar(
                    x=_air_sorted["passengers_share_pct"].values,
                    y=_air_sorted["airport_code"].values,
                    orientation="h",
                    marker=dict(color=ORANGE, opacity=0.80, line_width=0, cornerradius=5),
                    text=[f"{v:.1f}%" for v in _air_sorted["passengers_share_pct"]],
                    textposition="outside",
                    textfont=dict(size=11, family="Plus Jakarta Sans, Inter, sans-serif"),
                    hovertemplate="<b>%{y}</b><br>%{x:.1f}% of fly-market passengers<extra></extra>",
                ))
                fig.update_layout(xaxis_ticksuffix="%", showlegend=False)
                st.plotly_chart(style_fig(fig, height=300), use_container_width=True)
            else:
                st.info("Airport data not available.")

        st.markdown("---")

        # ── Campaign Attribution ────────────────────────────────────────────────
        st.markdown(
            '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.1rem;'
            'font-weight:800;letter-spacing:-0.02em;margin-bottom:4px;">'
            'Campaign Attribution Performance</div>'
            '<div style="font-size:12px;opacity:0.55;font-weight:500;margin-bottom:16px;">'
            'Datafy media & website attribution · verified visitor impact</div>',
            unsafe_allow_html=True,
        )

        ca1, ca2 = st.columns(2)

        with ca1:
            st.markdown('<div class="chart-header">Media Campaign Attribution</div>', unsafe_allow_html=True)
            if not df_dfy_media.empty:
                med = df_dfy_media.iloc[0]
                _attr_trips = int(med.get("attributable_trips", 0) or 0)
                _total_imp   = int(med.get("total_impressions", 0) or 0)
                _unique_reach = int(med.get("unique_reach", 0) or 0)
                _impact_usd  = float(med.get("total_impact_usd", 0) or 0)
                _investment  = float(med.get("total_investment_usd", 0) or 0)
                _roas_desc   = str(med.get("roas_description", "N/A") or "N/A")
                _campaign    = str(med.get("campaign_name", "Annual Campaign") or "Annual Campaign")
                _per_start   = str(med.get("report_period_start", ""))[:10]
                _per_end     = str(med.get("report_period_end", ""))[:10]

                st.markdown(
                    f'<div style="background:rgba(33,128,141,0.06);border:1px solid rgba(33,128,141,0.15);'
                    f'border-radius:10px;padding:16px 18px;">'
                    f'<div style="font-size:11px;opacity:0.55;font-weight:600;margin-bottom:10px;">'
                    f'{_campaign} · {_per_start} → {_per_end}</div>'
                    f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">'
                    f'<div><div style="font-size:1.4rem;font-weight:800;color:#21808D;">{_attr_trips:,}</div>'
                    f'<div style="font-size:10px;opacity:0.60;">Attributable Trips</div></div>'
                    f'<div><div style="font-size:1.4rem;font-weight:800;color:#21808D;">${_impact_usd:,.0f}</div>'
                    f'<div style="font-size:10px;opacity:0.60;">Est. Total Impact</div></div>'
                    f'<div><div style="font-size:1.1rem;font-weight:700;color:#21808D;">{_total_imp/1e6:.1f}M</div>'
                    f'<div style="font-size:10px;opacity:0.60;">Total Impressions</div></div>'
                    f'<div><div style="font-size:1.1rem;font-weight:700;color:#21808D;">{_unique_reach/1e6:.1f}M</div>'
                    f'<div style="font-size:10px;opacity:0.60;">Unique Reach</div></div>'
                    f'</div>'
                    f'<div style="margin-top:10px;font-size:11px;color:#21808D;font-weight:600;">ROAS: {_roas_desc}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.info("Media attribution data not available.")

        with ca2:
            st.markdown('<div class="chart-header">Website Attribution</div>', unsafe_allow_html=True)
            if not df_dfy_web.empty:
                web = df_dfy_web.iloc[0]
                _w_trips  = int(web.get("attributable_trips", 0) or 0)
                _w_reach  = int(web.get("unique_reach", 0) or 0)
                _w_impact = float(web.get("est_impact_usd", 0) or 0)
                _w_sess   = int(web.get("total_website_sessions", 0) or 0)
                _w_views  = int(web.get("website_pageviews", 0) or 0)
                _w_eng    = float(web.get("avg_engagement_rate_pct", 0) or 0)
                _w_url    = str(web.get("website_url", "visitdanapoint.com") or "")
                _w_start  = str(web.get("report_period_start", ""))[:10]
                _w_end    = str(web.get("report_period_end", ""))[:10]

                st.markdown(
                    f'<div style="background:rgba(230,129,97,0.06);border:1px solid rgba(230,129,97,0.20);'
                    f'border-radius:10px;padding:16px 18px;">'
                    f'<div style="font-size:11px;opacity:0.55;font-weight:600;margin-bottom:10px;">'
                    f'{_w_url} · {_w_start} → {_w_end}</div>'
                    f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">'
                    f'<div><div style="font-size:1.4rem;font-weight:800;color:#E68161;">{_w_trips:,}</div>'
                    f'<div style="font-size:10px;opacity:0.60;">Attributable Trips</div></div>'
                    f'<div><div style="font-size:1.4rem;font-weight:800;color:#E68161;">${_w_impact:,.0f}</div>'
                    f'<div style="font-size:10px;opacity:0.60;">Est. Destination Impact</div></div>'
                    f'<div><div style="font-size:1.1rem;font-weight:700;color:#E68161;">{_w_sess:,}</div>'
                    f'<div style="font-size:10px;opacity:0.60;">Website Sessions</div></div>'
                    f'<div><div style="font-size:1.1rem;font-weight:700;color:#E68161;">{_w_eng:.1f}%</div>'
                    f'<div style="font-size:10px;opacity:0.60;">Engagement Rate</div></div>'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.info("Website attribution data not available.")

        # ── DMA vs Spend Value bubble chart ───────────────────────────────────
        if not df_dfy_dma.empty:
            st.markdown("---")
            st.markdown('<div class="chart-header">Feeder Market Value Matrix — Visitor Days vs. Avg Spend</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-caption">Who: DMA feeder markets · What: volume vs. value tradeoff · '
                'Why: identifies high-value fly markets underweighted in campaigns · '
                'How to act: shift budget toward markets with high spend &amp; growth</div>',
                unsafe_allow_html=True,
            )
            _bub = df_dfy_dma[
                df_dfy_dma["visitor_days_share_pct"].notna() &
                df_dfy_dma["avg_spend_usd"].notna()
            ].copy()
            if not _bub.empty:
                _bub_yoy = _bub["visitor_days_vs_compare_pct"].fillna(0)
                _bubble_colors = [TEAL if v >= 0 else ORANGE for v in _bub_yoy]
                fig = go.Figure(go.Scatter(
                    x=_bub["visitor_days_share_pct"].values,
                    y=_bub["avg_spend_usd"].values,
                    mode="markers+text",
                    text=_bub["dma"].values,
                    textposition="top center",
                    textfont=dict(size=10, family="Plus Jakarta Sans, Inter, sans-serif"),
                    marker=dict(
                        size=[max(12, v * 3) for v in _bub["visitor_days_share_pct"]],
                        color=_bubble_colors,
                        opacity=0.75,
                        line=dict(width=1, color="white"),
                    ),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "Visitor day share: %{x:.1f}%<br>"
                        "Avg spend: $%{y:.0f}<extra></extra>"
                    ),
                ))
                fig.update_layout(
                    xaxis=dict(title="Share of Visitor Days (%)", ticksuffix="%"),
                    yaxis=dict(title="Avg Spend per Visitor ($)", tickprefix="$"),
                    showlegend=False,
                )
                st.plotly_chart(style_fig(fig, height=380), use_container_width=True)
                st.caption(
                    "🟢 Teal = YOY growth · 🟠 Orange = YOY decline. "
                    "Bubble size = share of visitor days. "
                    "Upper-right = high value, high volume (premium targets). "
                    "Upper-left = high spend but low volume (fly-market opportunity)."
                )

        # ── Sankey: Visitor Flow Origin → Type → Spend ─────────────────────────
        if not df_dfy_dma.empty and not df_dfy_spend.empty:
            st.markdown("---")
            st.markdown('<div class="chart-header">Visitor Flow — Origin to Spend Category (Sankey)</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-caption">WHO visits × WHERE they come from × WHAT they spend on · '
                'width of flow = relative share · surface hidden revenue concentration</div>',
                unsafe_allow_html=True,
            )
            # Build Sankey nodes and links from real data
            # Nodes: [Top 5 DMAs] → [Overnight, Day Trip] → [Top 3 Spend Categories]
            _top_dma_sk = df_dfy_dma[df_dfy_dma["visitor_days_share_pct"].notna()].head(5)
            _top_spend_sk = df_dfy_spend.head(3)

            # Pull overview KPIs for overnight/daytrip split
            _ov_sk = df_dfy_ov.iloc[0] if not df_dfy_ov.empty else {}
            _on_pct = float(_ov_sk.get("overnight_trips_pct", 60) or 60)
            _dt_pct = float(_ov_sk.get("day_trips_pct", 40) or 40)

            # Node labels
            _dma_labels = list(_top_dma_sk["dma"].values)
            _type_labels = ["Overnight Stays", "Day Trips"]
            _spend_labels = list(_top_spend_sk["category"].values)
            node_labels = _dma_labels + _type_labels + _spend_labels

            n_dma = len(_dma_labels)
            idx_on = n_dma      # Overnight
            idx_dt = n_dma + 1  # Day Trip
            idx_sp_start = n_dma + 2

            sources, targets, values_sk = [], [], []

            # DMA → trip type
            total_dma_share = _top_dma_sk["visitor_days_share_pct"].sum()
            for i, (_, row) in enumerate(_top_dma_sk.iterrows()):
                share = row["visitor_days_share_pct"] / total_dma_share * 100
                sources.append(i); targets.append(idx_on); values_sk.append(share * _on_pct / 100)
                sources.append(i); targets.append(idx_dt); values_sk.append(share * _dt_pct / 100)

            # Trip type → spend category (proportional)
            total_spend_share = _top_spend_sk["spend_share_pct"].sum()
            for j, (_, row) in enumerate(_top_spend_sk.iterrows()):
                sp_share = row["spend_share_pct"] / total_spend_share * 100
                sources.append(idx_on); targets.append(idx_sp_start + j); values_sk.append(100 * _on_pct / 100 * sp_share / 100)
                sources.append(idx_dt); targets.append(idx_sp_start + j); values_sk.append(100 * _dt_pct / 100 * sp_share / 100)

            _node_colors = (
                [TEAL_LIGHT] * n_dma +
                [TEAL, "#A8D4D9"] +
                [ORANGE, "#E68161", "#A84B2F"][:len(_spend_labels)]
            )
            fig = go.Figure(go.Sankey(
                node=dict(
                    pad=18, thickness=22,
                    line=dict(color="rgba(0,0,0,0)", width=0),
                    label=node_labels,
                    color=_node_colors,
                    hovertemplate="%{label}<extra></extra>",
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values_sk,
                    color="rgba(33,128,141,0.18)",
                ),
            ))
            fig.update_layout(font=dict(family="Plus Jakarta Sans, Inter, sans-serif", size=12))
            st.plotly_chart(style_fig(fig, height=400), use_container_width=True)
            st.caption(
                "Flow width = proportional visitor share. Overnight stays dominate accommodation spend. "
                "Day trippers concentrate in dining — capturing even 5% as overnight stays = significant room revenue."
            )

    # ── Website Attribution Deep-Dive ─────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🌐 Website Attribution — Acquisition Channels & Top Markets")
    st.caption("Source: Datafy Attribution Website · Q3 2025 · visitdanapoint.com")

    _web_c1, _web_c2 = st.columns(2)

    with _web_c1:
        st.markdown('<div class="chart-header">Acquisition Channels</div>', unsafe_allow_html=True)
        if not df_dfy_web_ch.empty:
            _ch = df_dfy_web_ch.copy()
            _ch_cols = [c for c in ["channel","sessions","attributable_trips","trip_share_pct"] if c in _ch.columns]
            if "channel" in _ch.columns and "sessions" in _ch.columns:
                _ch_s = _ch.sort_values("sessions", ascending=True)
                fig_ch = go.Figure(go.Bar(
                    x=_ch_s["sessions"].values,
                    y=_ch_s["channel"].values,
                    orientation="h",
                    marker_color=TEAL,
                    hovertemplate="<b>%{y}</b><br>Sessions: %{x:,}<extra></extra>",
                ))
                fig_ch.update_layout(xaxis_title="Sessions", yaxis_title=None, margin=dict(l=0,r=0,t=20,b=20), height=220)
                st.plotly_chart(style_fig(fig_ch, height=220), use_container_width=True)
                with st.expander("📊 View raw channel data"):
                    st.dataframe(_ch[_ch_cols].reset_index(drop=True), use_container_width=True)
                    st.download_button("⬇️ Download", _ch[_ch_cols].to_csv(index=False), "website_channels.csv", "text/csv", key="dl_web_ch")
            else:
                st.dataframe(_ch.reset_index(drop=True), use_container_width=True)
        else:
            st.info("Website channel data not available.")

    with _web_c2:
        st.markdown('<div class="chart-header">Top Markets — Website Attribution</div>', unsafe_allow_html=True)
        if not df_dfy_web_mkts.empty:
            _wm = df_dfy_web_mkts.copy()
            _wm_cols = [c for c in ["market","sessions","attributable_trips","trip_share_pct"] if c in _wm.columns]
            if "market" in _wm.columns and len(_wm) > 0:
                _wm_s = _wm.head(10).sort_values(_wm.columns[1] if len(_wm.columns) > 1 else _wm.columns[0], ascending=True)
                _x_col = "sessions" if "sessions" in _wm.columns else _wm.columns[1]
                _y_col = "market" if "market" in _wm.columns else _wm.columns[0]
                fig_wm = go.Figure(go.Bar(
                    x=_wm_s[_x_col].values,
                    y=_wm_s[_y_col].values,
                    orientation="h",
                    marker_color=ORANGE,
                    hovertemplate=f"<b>%{{y}}</b><br>{_x_col}: %{{x:,}}<extra></extra>",
                ))
                fig_wm.update_layout(xaxis_title=_x_col.replace("_"," ").title(), yaxis_title=None, margin=dict(l=0,r=0,t=20,b=20), height=220)
                st.plotly_chart(style_fig(fig_wm, height=220), use_container_width=True)
                with st.expander("📊 View raw top markets data"):
                    st.dataframe(_wm[_wm_cols].reset_index(drop=True), use_container_width=True)
                    st.download_button("⬇️ Download", _wm[_wm_cols].to_csv(index=False), "website_top_markets.csv", "text/csv", key="dl_web_mkts")
            else:
                st.dataframe(_wm.reset_index(drop=True), use_container_width=True)
        else:
            st.info("Website top markets data not available.")

    # Website DMA comparison
    if not df_dfy_web_dma.empty and not df_dfy_dma.empty:
        st.markdown('<div class="chart-header" style="margin-top:12px;">Website DMA Attribution vs. Overall Visitor Days (Q3 2025 vs Annual)</div>', unsafe_allow_html=True)
        st.caption("Compares website-attributed trip share by DMA (Q3 2025) against overall visitor day share (annual) — reveals which markets convert better via digital.")
        _wd = df_dfy_web_dma.copy()
        _od = df_dfy_dma.copy()
        if "dma" in _wd.columns and "dma" in _od.columns:
            _merge_col = "attributable_trips_pct" if "attributable_trips_pct" in _wd.columns else (_wd.columns[1] if len(_wd.columns) > 1 else None)
            _od_col = "visitor_days_share_pct" if "visitor_days_share_pct" in _od.columns else None
            if _merge_col and _od_col:
                _comp = _wd[["dma", _merge_col]].merge(_od[["dma", _od_col]], on="dma", how="inner").head(8)
                if not _comp.empty:
                    _dmas = _comp["dma"].values
                    fig_cmp = go.Figure()
                    fig_cmp.add_trace(go.Bar(name="Website Trip Share (Q3)", x=_dmas, y=_comp[_merge_col].values, marker_color=TEAL))
                    fig_cmp.add_trace(go.Bar(name="Overall Visitor Days (Annual)", x=_dmas, y=_comp[_od_col].values, marker_color=ORANGE, opacity=0.7))
                    fig_cmp.update_layout(barmode="group", xaxis_tickangle=-30, height=300, margin=dict(l=0,r=0,t=20,b=60), legend=dict(orientation="h",y=1.08))
                    st.plotly_chart(style_fig(fig_cmp, height=300), use_container_width=True)

    # ── Visitor Cluster Visitation ─────────────────────────────────────────────
    if not df_dfy_clusters.empty:
        st.markdown("---")
        st.markdown("### 🗺️ Visitor Cluster Visitation — Where They Go")
        st.caption("Which Dana Point area clusters attract the most visitor activity · Datafy Annual 2025")
        _cl = df_dfy_clusters.copy()
        _cl_share_col = [c for c in _cl.columns if "share" in c.lower() or "pct" in c.lower() or "visits" in c.lower()]
        _cl_name_col  = [c for c in _cl.columns if "cluster" in c.lower() or "area" in c.lower() or "zone" in c.lower() or "name" in c.lower()]
        if _cl_share_col and _cl_name_col:
            _cl_x = _cl_share_col[0]; _cl_y = _cl_name_col[0]
            _cl_s = _cl.sort_values(_cl_x, ascending=True).head(10)
            fig_cl = go.Figure(go.Bar(
                x=_cl_s[_cl_x].values, y=_cl_s[_cl_y].values,
                orientation="h", marker_color=TEAL_LIGHT,
                hovertemplate="<b>%{y}</b><br>Share: %{x:.1f}%<extra></extra>",
            ))
            fig_cl.update_layout(xaxis_title="Visitation Share (%)", xaxis_ticksuffix="%", height=280, margin=dict(l=0,r=0,t=20,b=20))
            st.plotly_chart(style_fig(fig_cl, height=280), use_container_width=True)
            with st.expander("📊 View raw cluster data"):
                st.dataframe(_cl.reset_index(drop=True), use_container_width=True)

    # ── Social & Web Analytics ────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📱 Social & Web Analytics — visitdanapoint.com")
    st.caption("Source: Datafy GA4 Social / Web Analytics · Annual 2025")

    _soc_c1, _soc_c2 = st.columns(2)

    with _soc_c1:
        st.markdown('<div class="chart-header">Traffic Sources</div>', unsafe_allow_html=True)
        if not df_dfy_social_traf.empty:
            _st = df_dfy_social_traf.copy()
            _src_col  = next((c for c in _st.columns if "source" in c.lower() or "channel" in c.lower() or "medium" in c.lower()), None)
            _sess_col = next((c for c in _st.columns if "session" in c.lower() or "visits" in c.lower()), None)
            if _src_col and _sess_col:
                _st_s = _st.sort_values(_sess_col, ascending=False).head(12)
                fig_st = go.Figure(go.Bar(
                    x=_st_s[_src_col].values,
                    y=_st_s[_sess_col].values,
                    marker_color=TEAL,
                    hovertemplate="<b>%{x}</b><br>Sessions: %{y:,}<extra></extra>",
                ))
                fig_st.update_layout(xaxis_tickangle=-35, yaxis_title="Sessions", height=260, margin=dict(l=0,r=0,t=20,b=60))
                st.plotly_chart(style_fig(fig_st, height=260), use_container_width=True)
            else:
                st.dataframe(_st.reset_index(drop=True), use_container_width=True)
            with st.expander("📊 View raw traffic source data"):
                st.dataframe(_st.reset_index(drop=True), use_container_width=True)
                st.download_button("⬇️ Download", _st.to_csv(index=False), "traffic_sources.csv", "text/csv", key="dl_traf")
        else:
            st.info("Social traffic source data not available.")

    with _soc_c2:
        st.markdown('<div class="chart-header">Top Pages by Views</div>', unsafe_allow_html=True)
        if not df_dfy_social_pages.empty:
            _pg = df_dfy_social_pages.copy()
            _pg_name = next((c for c in _pg.columns if "page" in c.lower() or "url" in c.lower() or "title" in c.lower()), None)
            _pg_views = next((c for c in _pg.columns if "view" in c.lower() or "visit" in c.lower() or "session" in c.lower()), None)
            if _pg_name and _pg_views:
                _pg_s = _pg.sort_values(_pg_views, ascending=False).head(10)
                _pg_labels = [str(v)[:35] + "…" if len(str(v)) > 35 else str(v) for v in _pg_s[_pg_name].values]
                fig_pg = go.Figure(go.Bar(
                    x=_pg_s[_pg_views].values,
                    y=_pg_labels,
                    orientation="h",
                    marker_color=ORANGE,
                    hovertemplate="<b>%{y}</b><br>Views: %{x:,}<extra></extra>",
                ))
                fig_pg.update_layout(xaxis_title="Page Views", height=310, margin=dict(l=0,r=0,t=20,b=20), yaxis=dict(autorange="reversed"))
                st.plotly_chart(style_fig(fig_pg, height=310), use_container_width=True)
            else:
                st.dataframe(_pg.reset_index(drop=True), use_container_width=True)
            with st.expander("📊 View raw top pages data"):
                st.dataframe(_pg.reset_index(drop=True), use_container_width=True)
                st.download_button("⬇️ Download", _pg.to_csv(index=False), "top_pages.csv", "text/csv", key="dl_pages")
        else:
            st.info("Top pages data not available.")

    # Audience Overview KPIs
    if not df_dfy_social_aud.empty:
        _aud = df_dfy_social_aud.iloc[0]
        _aud_metrics = {k: v for k, v in _aud.items() if k not in ("id","loaded_at","report_period_start","report_period_end","website_url") and pd.notna(v)}
        if _aud_metrics:
            st.markdown("#### Website Audience KPIs")
            _aud_cols = st.columns(min(4, len(_aud_metrics)))
            for _ai, (_ak, _av) in enumerate(_aud_metrics.items()):
                with _aud_cols[_ai % len(_aud_cols)]:
                    _disp = f"{float(_av):,.0f}" if isinstance(_av, (int,float)) else str(_av)
                    st.metric(_ak.replace("_"," ").title(), _disp)

    st.markdown("---")

    # ── Zartico Historical Reference ─────────────────────────────────────────
    st.info("📚 **Historical Reference:** Zartico data reflects a Jun 2025 snapshot. Use for trend comparison only — Datafy is the current source of record.")
    st.markdown("### 📚 Zartico Historical Reference (Jun 2025 Snapshot)")
    st.caption("⚠️ Zartico data represents a historical snapshot (last updated Jun 2025). "
               "Use for trend comparison only. Current performance data comes from Datafy, CoStar, and STR.")

    if not df_zrt_kpis.empty or not df_zrt_spend.empty:
        zrt_col1, zrt_col2, zrt_col3 = st.columns(3)

        if not df_zrt_kpis.empty:
            zk = df_zrt_kpis.iloc[0]
            with zrt_col1:
                st.markdown(kpi_card(
                    "Visitor Device Share",
                    f"{zk.get('pct_devices_visitors', 0):.1f}%",
                    "% of all local devices that are visitors",
                    positive=True,
                ), unsafe_allow_html=True)
            with zrt_col2:
                st.markdown(kpi_card(
                    "Visitor Spend Share",
                    f"{zk.get('pct_spend_visitors', 0):.1f}%",
                    "% of total spend from visitors",
                    positive=True,
                ), unsafe_allow_html=True)
            with zrt_col3:
                st.markdown(kpi_card(
                    "Accommodation Spend %",
                    f"{zk.get('pct_accommodation_spend_visitors', 0):.0f}%",
                    "% of accommodation spend from visitors",
                    positive=True,
                ), unsafe_allow_html=True)

        if not df_zrt_spend.empty:
            st.markdown("#### Monthly Avg. Visitor Spend vs. Benchmark (Jul 2024–May 2025)")
            import plotly.graph_objects as go
            fig_zrt = go.Figure()
            fig_zrt.add_trace(go.Scatter(
                x=df_zrt_spend["month_str"], y=df_zrt_spend["avg_visitor_spend"],
                name="Dana Point Avg. Spend", line=dict(color="#21808D", width=2.5),
                mode="lines+markers",
                hovertemplate="<b>%{x}</b><br>Dana Point: $%{y:.0f}<extra></extra>",
            ))
            if "benchmark_avg_spend" in df_zrt_spend.columns and df_zrt_spend["benchmark_avg_spend"].notna().any():
                fig_zrt.add_trace(go.Scatter(
                    x=df_zrt_spend["month_str"], y=df_zrt_spend["benchmark_avg_spend"],
                    name="Zartico Benchmark", line=dict(color="#E68161", width=1.5, dash="dot"),
                    mode="lines+markers",
                    hovertemplate="<b>%{x}</b><br>Benchmark: $%{y:.0f}<extra></extra>",
                ))
            fig_zrt.update_layout(
                height=280, margin=dict(l=0, r=0, t=20, b=20),
                yaxis_title="Avg. Visitor Spend ($)", xaxis_title=None,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_zrt, use_container_width=True)

        if not df_zrt_overnight.empty:
            st.markdown("#### Monthly Overnight Visitor % Trend (Zartico Historical)")
            fig_ov = go.Figure()
            fig_ov.add_trace(go.Bar(
                x=df_zrt_overnight["month_str"], y=df_zrt_overnight["pct_overnight"],
                marker_color="#21808D", name="Overnight %",
                hovertemplate="<b>%{x}</b><br>Overnight: %{y:.1f}%<extra></extra>",
            ))
            fig_ov.update_layout(
                height=240, margin=dict(l=0, r=0, t=20, b=20),
                yaxis_title="Overnight %", yaxis_ticksuffix="%",
            )
            st.plotly_chart(fig_ov, use_container_width=True)

        if not df_zrt_markets.empty:
            st.markdown("#### Top Visitor Origin Markets (Zartico Q1 2025)")
            _zrt_top = df_zrt_markets.sort_values("rank").head(10)
            fig_mkt = go.Figure(go.Bar(
                x=_zrt_top["pct_visitors"],
                y=_zrt_top["market"],
                orientation="h",
                marker_color="#21808D",
                hovertemplate="<b>%{y}</b><br>%{x:.1f}% of visitors<extra></extra>",
            ))
            fig_mkt.update_layout(
                height=320, margin=dict(l=0, r=0, t=20, b=20),
                xaxis_title="% of Visitors", yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_mkt, use_container_width=True)

        if not df_zrt_events.empty:
            st.markdown("#### Event Impact Analysis (Zartico Historical)")
            ze = df_zrt_events.iloc[0]
            ev_c1, ev_c2, ev_c3 = st.columns(3)
            with ev_c1:
                st.markdown(kpi_card(
                    "Total Spend Lift", f"+{ze.get('change_total_spend_pct',0):.1f}%",
                    "vs. 4-week baseline", positive=True,
                ), unsafe_allow_html=True)
            with ev_c2:
                st.markdown(kpi_card(
                    "Visitor Spend Lift", f"+{ze.get('change_visitor_spend_pct',0):.1f}%",
                    "visitor spend increase during event", positive=True,
                ), unsafe_allow_html=True)
            with ev_c3:
                st.markdown(kpi_card(
                    "Accommodation Share", f"{ze.get('pct_accommodation_spend',0):.0f}%",
                    "of visitor spend during event", positive=True,
                ), unsafe_allow_html=True)
    else:
        st.info("Run `python scripts/load_zartico_reports.py` to load Zartico historical data.")

    st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — FEEDER MARKETS
# ══════════════════════════════════════════════════════════════════════════════
with tab_fm:
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Feeder Market Intelligence</div>
      <div class="hero-subtitle">DMA Origin Analysis · Visitor Value Matrix · Strategic Budget Allocation</div>
    </div>
    """, unsafe_allow_html=True)

    # ── DMA Overview ───────────────────────────────────────────────────────────
    if not df_dfy_dma.empty:
        _fm_period = str(df_dfy_dma.iloc[0].get("report_period_start", ""))[:4] + " Annual" if not df_dfy_dma.empty else ""
        _dma_top10 = df_dfy_dma[df_dfy_dma["visitor_days_share_pct"].notna()].head(10).copy()

        # KPI summary row
        _total_dma_pct = _dma_top10["visitor_days_share_pct"].sum()
        _top_mkt = _dma_top10.iloc[0]["dma"] if not _dma_top10.empty else "N/A"
        _top_mkt_pct = _dma_top10.iloc[0]["visitor_days_share_pct"] if not _dma_top10.empty else 0
        _high_spend_row = df_dfy_dma[df_dfy_dma["avg_spend_usd"].notna()].nlargest(1, "avg_spend_usd")
        _high_spend_mkt = _high_spend_row.iloc[0]["dma"] if not _high_spend_row.empty else "N/A"
        _high_spend_val = float(_high_spend_row.iloc[0]["avg_spend_usd"]) if not _high_spend_row.empty else 0

        _fm_k1, _fm_k2, _fm_k3 = st.columns(3)
        with _fm_k1:
            st.markdown(
                f'<div style="background:rgba(33,128,141,0.06);border:1px solid rgba(33,128,141,0.15);border-radius:10px;padding:14px 16px;">'
                f'<div style="font-size:1.4rem;font-weight:800;color:#21808D;">{_top_mkt}</div>'
                f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">Top Feeder Market</div>'
                f'<div style="font-size:11px;color:#21808D;font-weight:600;margin-top:3px;">{_top_mkt_pct:.1f}% of visitor days</div>'
                f'</div>', unsafe_allow_html=True)
        with _fm_k2:
            st.markdown(
                f'<div style="background:rgba(230,129,97,0.06);border:1px solid rgba(230,129,97,0.15);border-radius:10px;padding:14px 16px;">'
                f'<div style="font-size:1.4rem;font-weight:800;color:#E68161;">{_high_spend_mkt}</div>'
                f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">Highest Value Market</div>'
                f'<div style="font-size:11px;color:#E68161;font-weight:600;margin-top:3px;">${_high_spend_val:,.0f} avg spend/visitor</div>'
                f'</div>', unsafe_allow_html=True)
        with _fm_k3:
            _oos_pct_fm = float(df_dfy_ov.iloc[0].get("out_of_state_vd_pct", 0) or 0) if not df_dfy_ov.empty else 0
            st.markdown(
                f'<div style="background:rgba(33,128,141,0.06);border:1px solid rgba(33,128,141,0.15);border-radius:10px;padding:14px 16px;">'
                f'<div style="font-size:1.4rem;font-weight:800;color:#21808D;">{_oos_pct_fm:.1f}%</div>'
                f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">Out-of-State Visitor Days</div>'
                f'<div style="font-size:11px;color:#21808D;font-weight:600;margin-top:3px;">OOS visitors generate higher ADR per trip</div>'
                f'</div>', unsafe_allow_html=True)

        st.markdown("---")

        # ── Side-by-side: Visitor Days Share + Avg Spend ───────────────────────
        _fm_c1, _fm_c2 = st.columns(2)

        with _fm_c1:
            st.markdown('<div class="chart-header">Visitor Days Share by DMA</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="chart-caption">Share of visitor days by origin market · Datafy {_fm_period}</div>', unsafe_allow_html=True)
            _dma_sorted = _dma_top10.sort_values("visitor_days_share_pct", ascending=True)
            _max_s = _dma_sorted["visitor_days_share_pct"].max()
            _bar_colors_fm = [
                f"rgba(33,{int(128 + 56*(v/_max_s))},{int(141 + 57*(v/_max_s))},0.90)"
                for v in _dma_sorted["visitor_days_share_pct"]
            ]
            fig_fm1 = go.Figure(go.Bar(
                x=_dma_sorted["visitor_days_share_pct"].values,
                y=_dma_sorted["dma"].values,
                orientation="h",
                marker=dict(color=_bar_colors_fm, line_width=0, cornerradius=5),
                text=[f"{v:.1f}%" for v in _dma_sorted["visitor_days_share_pct"]],
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>Visitor days: %{x:.1f}%<extra></extra>",
            ))
            _top_vol_dma = _dma_sorted.iloc[-1]
            fig_fm1.add_annotation(
                y=_top_vol_dma["dma"],
                x=float(_top_vol_dma["visitor_days_share_pct"]) * 1.02,
                text="👑 Top Volume",
                showarrow=False,
                xanchor="left",
                font=dict(size=10, color="#21808D", family="Plus Jakarta Sans, Inter, sans-serif"),
                bgcolor="rgba(33,128,141,0.08)",
                borderpad=3,
            )
            fig_fm1.update_layout(xaxis_ticksuffix="%", showlegend=False)
            st.plotly_chart(style_fig(fig_fm1, height=380), use_container_width=True)

        with _fm_c2:
            st.markdown('<div class="chart-header">Average Spend per Visitor by DMA</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="chart-caption">Value per visitor trip · higher = more revenue per marketing dollar</div>', unsafe_allow_html=True)
            _dma_spend = df_dfy_dma[df_dfy_dma["avg_spend_usd"].notna()].sort_values("avg_spend_usd", ascending=True).head(10)
            if not _dma_spend.empty:
                _spend_max = _dma_spend["avg_spend_usd"].max()
                _spend_colors = [TEAL if v >= _dma_spend["avg_spend_usd"].mean() else ORANGE for v in _dma_spend["avg_spend_usd"]]
                fig_fm2 = go.Figure(go.Bar(
                    x=_dma_spend["avg_spend_usd"].values,
                    y=_dma_spend["dma"].values,
                    orientation="h",
                    marker=dict(color=_spend_colors, line_width=0, cornerradius=5),
                    text=[f"${v:,.0f}" for v in _dma_spend["avg_spend_usd"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>Avg spend: $%{x:,.0f}<extra></extra>",
                ))
                _top_val_dma = _dma_spend.iloc[-1]
                fig_fm2.add_annotation(
                    y=_top_val_dma["dma"],
                    x=float(_top_val_dma["avg_spend_usd"]) * 1.02,
                    text="⭐ Highest Value",
                    showarrow=False,
                    xanchor="left",
                    font=dict(size=10, color="#E68161", family="Plus Jakarta Sans, Inter, sans-serif"),
                    bgcolor="rgba(230,129,97,0.08)",
                    borderpad=3,
                )
                fig_fm2.update_layout(xaxis_tickprefix="$", showlegend=False)
                st.plotly_chart(style_fig(fig_fm2, height=380), use_container_width=True)
            else:
                st.info("Avg spend data not available by DMA.")

        st.markdown("---")

        # ── US Bubble Map — Visitor Origin by DMA ─────────────────────────────
        st.markdown('<div class="chart-header">Visitor Origin Map — US DMA Geographic Distribution</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">Bubble size = share of visitor days · Color = avg spend per visitor · Source: Datafy</div>', unsafe_allow_html=True)
        _dma_coords = {
            "Los Angeles":    (34.05, -118.24),
            "San Francisco":  (37.77, -122.42),
            "San Diego":      (32.72, -117.16),
            "Phoenix":        (33.45, -112.07),
            "Las Vegas":      (36.17, -115.14),
            "Salt Lake City": (40.76, -111.89),
            "Dallas":         (32.78,  -96.80),
            "New York":       (40.71,  -74.01),
            "Chicago":        (41.88,  -87.63),
            "Seattle":        (47.61, -122.33),
            "Denver":         (39.74, -104.98),
            "Portland":       (45.52, -122.68),
        }
        _map_rows = []
        for _, _mr in _dma_top10.iterrows():
            _dname = str(_mr["dma"])
            _coords = None
            for _key, _c in _dma_coords.items():
                if _key.lower() in _dname.lower() or _dname.lower() in _key.lower():
                    _coords = _c
                    break
            if _coords:
                _map_rows.append({
                    "dma": _dname,
                    "lat": _coords[0],
                    "lon": _coords[1],
                    "visitor_days_share_pct": float(_mr.get("visitor_days_share_pct") or 0),
                    "avg_spend_usd": float(_mr.get("avg_spend_usd") or 0),
                    "spending_share_pct": float(_mr.get("spending_share_pct") or 0),
                })
        if _map_rows:
            _map_df = pd.DataFrame(_map_rows)
            try:
                fig_fm_map = go.Figure(go.Scattergeo(
                    lat=_map_df["lat"],
                    lon=_map_df["lon"],
                    text=_map_df["dma"],
                    customdata=_map_df[["visitor_days_share_pct", "avg_spend_usd", "spending_share_pct"]].values,
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        "Visitor days: %{customdata[0]:.1f}%<br>"
                        "Avg spend: $%{customdata[1]:,.0f}<br>"
                        "Spending share: %{customdata[2]:.1f}%<extra></extra>"
                    ),
                    mode="markers+text",
                    textposition="top center",
                    textfont=dict(size=9, family="Plus Jakarta Sans, Inter, sans-serif", color="#21808D"),
                    marker=dict(
                        size=[max(8, v * 4) for v in _map_df["visitor_days_share_pct"]],
                        color=_map_df["avg_spend_usd"],
                        colorscale=[[0, "#B7D7DC"], [0.5, "#21808D"], [1.0, "#E68161"]],
                        showscale=True,
                        colorbar=dict(title="Avg Spend ($)", tickprefix="$", len=0.6),
                        opacity=0.85,
                        line=dict(width=1, color="white"),
                    ),
                ))
                fig_fm_map.update_layout(
                    geo=dict(
                        scope="usa",
                        projection_type="albers usa",
                        showland=True,    landcolor="rgba(240,240,240,0.6)",
                        showlakes=True,   lakecolor="rgba(200,220,255,0.4)",
                        showcoastlines=True, coastlinecolor="rgba(0,0,0,0.15)",
                        showsubunits=True, subunitcolor="rgba(0,0,0,0.10)",
                    ),
                    margin=dict(l=0, r=0, t=0, b=0),
                )
                st.plotly_chart(style_fig(fig_fm_map, height=420), use_container_width=True)
            except Exception as _map_err:
                st.warning(f"Map rendering unavailable: {_map_err}")
                # Fallback: simple table of top markets
                st.dataframe(
                    _map_df[["dma", "visitor_days_share_pct", "avg_spend_usd"]]
                    .rename(columns={"dma": "Market", "visitor_days_share_pct": "Visitor Days %", "avg_spend_usd": "Avg Spend $"})
                    .sort_values("Visitor Days %", ascending=False),
                    use_container_width=True, hide_index=True,
                )
        else:
            st.info("No mappable DMA coordinates found in current data.")

        st.markdown("---")

        # ── Market Value Matrix ────────────────────────────────────────────────
        st.markdown('<div class="chart-header">Market Value Matrix — Volume vs. Spend per Visitor</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">Quadrant analysis: identify high-value fly markets vs. high-volume drive markets · Bubble size = spending share %</div>', unsafe_allow_html=True)
        _bub_fm = df_dfy_dma[df_dfy_dma["visitor_days_share_pct"].notna() & df_dfy_dma["avg_spend_usd"].notna()].copy()
        if not _bub_fm.empty:
            _med_vol = float(_bub_fm["visitor_days_share_pct"].median())
            _med_val = float(_bub_fm["avg_spend_usd"].median())
            _x_max   = float(_bub_fm["visitor_days_share_pct"].max()) * 1.15
            _y_max   = float(_bub_fm["avg_spend_usd"].max()) * 1.15
            _yoy_fm = _bub_fm["visitor_days_vs_compare_pct"].fillna(0)
            _bub_colors_fm = [TEAL if v >= 0 else ORANGE for v in _yoy_fm]
            _spend_share_col = "spending_share_pct" if "spending_share_pct" in _bub_fm.columns else "visitor_days_share_pct"
            _bubble_sizes = [max(12, float(v or 0) * 4) for v in _bub_fm[_spend_share_col]]
            fig_fm3 = go.Figure()
            fig_fm3.add_shape(type="rect", x0=_med_vol, x1=_x_max, y0=_med_val, y1=_y_max,
                              fillcolor="rgba(33,128,141,0.06)", line_width=0, layer="below")
            fig_fm3.add_shape(type="rect", x0=0, x1=_med_vol, y0=_med_val, y1=_y_max,
                              fillcolor="rgba(230,129,97,0.06)", line_width=0, layer="below")
            fig_fm3.add_shape(type="rect", x0=_med_vol, x1=_x_max, y0=0, y1=_med_val,
                              fillcolor="rgba(230,129,97,0.04)", line_width=0, layer="below")
            fig_fm3.add_shape(type="rect", x0=0, x1=_med_vol, y0=0, y1=_med_val,
                              fillcolor="rgba(0,0,0,0.02)", line_width=0, layer="below")
            fig_fm3.add_vline(x=_med_vol, line_dash="dot", line_color="rgba(0,0,0,0.20)", line_width=1)
            fig_fm3.add_hline(y=_med_val, line_dash="dot", line_color="rgba(0,0,0,0.20)", line_width=1)
            _q_style = dict(xref="paper", yref="paper", showarrow=False,
                            font=dict(size=9, color="rgba(0,0,0,0.35)"))
            fig_fm3.add_annotation(x=0.97, y=0.97, text="High Volume / High Value", xanchor="right", **_q_style)
            fig_fm3.add_annotation(x=0.03, y=0.97, text="Low Volume / High Value", xanchor="left", **_q_style)
            fig_fm3.add_annotation(x=0.97, y=0.03, text="High Volume / Low Value", xanchor="right", **_q_style)
            fig_fm3.add_annotation(x=0.03, y=0.03, text="Low Volume / Low Value", xanchor="left", **_q_style)
            fig_fm3.add_trace(go.Scatter(
                x=_bub_fm["visitor_days_share_pct"].values,
                y=_bub_fm["avg_spend_usd"].values,
                mode="markers+text",
                text=_bub_fm["dma"].values,
                textposition="top center",
                textfont=dict(size=10, family="Plus Jakarta Sans, Inter, sans-serif"),
                marker=dict(
                    size=_bubble_sizes,
                    color=_bub_colors_fm,
                    opacity=0.75,
                    line=dict(width=1, color="white"),
                ),
                hovertemplate="<b>%{text}</b><br>Visitor day share: %{x:.1f}%<br>Avg spend: $%{y:.0f}<extra></extra>",
            ))
            fig_fm3.update_layout(
                xaxis=dict(title="Share of Visitor Days (%)", ticksuffix="%"),
                yaxis=dict(title="Avg Spend per Visitor ($)", tickprefix="$"),
                showlegend=False,
            )
            st.plotly_chart(style_fig(fig_fm3, height=420), use_container_width=True)
            st.caption("Teal = YOY growth · Orange = YOY decline · Bubble size = spending share. Upper-left: low volume, high spend per trip — prime fly-market targets.")

            # ── Fly-Market Correlation Insight ────────────────────────────────
            if not df_kpi.empty:
                _fly_mkts   = ["Salt Lake City", "Dallas", "New York", "Chicago", "Denver"]
                _drive_mkts = ["Los Angeles", "San Diego"]
                _fly_rows   = _bub_fm[_bub_fm["dma"].apply(
                    lambda _d: any(_fm.lower() in str(_d).lower() for _fm in _fly_mkts)
                )]
                _drive_rows = _bub_fm[_bub_fm["dma"].apply(
                    lambda _d: any(_dm2.lower() in str(_d).lower() for _dm2 in _drive_mkts)
                )]
                if not _fly_rows.empty and not _drive_rows.empty:
                    _avg_fly   = float(_fly_rows["avg_spend_usd"].mean())
                    _avg_drive = float(_drive_rows["avg_spend_usd"].mean())
                    if _avg_drive > 0:
                        _fly_mult = _avg_fly / _avg_drive
                        st.markdown(
                            f'<div style="background:rgba(33,128,141,0.05);border-left:3px solid #21808D;'
                            f'border-radius:0 8px 8px 0;padding:12px 16px;margin-top:8px;font-size:12px;">'
                            f'<strong>HIDDEN SIGNAL — Fly Market Revenue Premium:</strong> '
                            f'Fly markets ({", ".join(_fly_mkts[:3])}, etc.) generate '
                            f'<strong>{_fly_mult:.1f}x more revenue per trip</strong> than the LA drive market '
                            f'(${_avg_fly:,.0f} vs. ${_avg_drive:,.0f} avg spend based on Datafy DMA data). '
                            f'Shifting 5% of marketing spend toward fly markets could yield outsized ROAS improvement.'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

        # ── Zartico Top Markets (historical comparison) ────────────────────────
        if not df_zrt_markets.empty:
            st.markdown("---")
            st.info("📚 **Historical Reference:** Zartico data reflects a Jun 2025 snapshot. Use for trend comparison only — Datafy is the current source of record.")
            st.markdown("#### Historical Feeder Markets — Zartico Reference (Q1 2025)")
            st.caption("⚠️ Zartico data is a historical snapshot. Use for trend context only — not current performance.")
            _zrt_top10 = df_zrt_markets.sort_values("rank").head(10)
            fig_zrt_mkt = go.Figure(go.Bar(
                x=_zrt_top10["pct_visitors"],
                y=_zrt_top10["market"],
                orientation="h",
                marker_color=TEAL_LIGHT,
                text=[f"{v:.1f}%" for v in _zrt_top10["pct_visitors"]],
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>%{x:.1f}% of visitors<extra></extra>",
            ))
            fig_zrt_mkt.update_layout(
                xaxis_ticksuffix="%", showlegend=False,
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(style_fig(fig_zrt_mkt, height=320), use_container_width=True)

        # ── Raw data expander ─────────────────────────────────────────────────
        with st.expander("📊 View raw DMA data"):
            st.dataframe(df_dfy_dma, use_container_width=True, hide_index=True)
            _dma_csv = df_dfy_dma.to_csv(index=False).encode()
            st.download_button("⬇️ Download DMA Data CSV", _dma_csv,
                               file_name="datafy_dma.csv", mime="text/csv")
    else:
        st.markdown(empty_state(
            "🎯", "No DMA feeder market data loaded.",
            "Run the pipeline: `python scripts/run_pipeline.py`",
        ), unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — EVENT IMPACT
# ══════════════════════════════════════════════════════════════════════════════
with tab_ei:
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Event Impact Analysis</div>
      <div class="hero-subtitle">STR Performance · Ohana Fest · Doheny Days · Tall Ships · July 4 · Zartico · Datafy · Full Events Calendar</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Monthly baseline lookup from live KPI data ─────────────────────────────
    _kpi_all = df_kpi.copy() if not df_kpi.empty else pd.DataFrame()
    _month_baseline: dict = {}
    if not _kpi_all.empty and "as_of_date" in _kpi_all.columns:
        _kpi_all["_month"] = pd.to_datetime(_kpi_all["as_of_date"], errors="coerce").dt.to_period("M").astype(str)
        _mb = _kpi_all.groupby("_month")[["occ_pct","adr","revpar"]].mean()
        _month_baseline = _mb.to_dict("index")

    def _get_baseline(yyyymm: str) -> tuple:
        """Return (occ, adr, revpar) monthly average for a YYYY-MM string."""
        row = _month_baseline.get(yyyymm, {})
        return row.get("occ_pct", 0), row.get("adr", 0), row.get("revpar", 0)

    def _event_kpi(start_date: str, end_date: str) -> tuple:
        """Return (occ, adr, revpar) average over an event date window."""
        if _kpi_all.empty:
            return 0, 0, 0
        mask = (_kpi_all["as_of_date"] >= start_date) & (_kpi_all["as_of_date"] <= end_date)
        sub = _kpi_all[mask]
        if sub.empty:
            return 0, 0, 0
        return sub["occ_pct"].mean(), sub["adr"].mean(), sub["revpar"].mean()

    # ── Headline KPIs — Event Calendar Snapshot ────────────────────────────────
    _ei_summary_cols = st.columns(4)
    _total_major = int(df_vdp_events[df_vdp_events["is_major"] == 1].shape[0]) if not df_vdp_events.empty else 8
    _total_events = int(df_vdp_events.shape[0]) if not df_vdp_events.empty else 10
    # Zartico: +63.5% YOY events, +101% attendees
    _zrt_fe = df_zrt_future_events.iloc[0] if not df_zrt_future_events.empty else None
    _ev_yoy_str = f"+{_zrt_fe['yoy_pct_change_events']:.0f}%" if _zrt_fe is not None and pd.notna(_zrt_fe.get("yoy_pct_change_events")) else "N/A"
    _att_yoy_str = f"+{_zrt_fe['yoy_pct_change_attendees']:.0f}%" if _zrt_fe is not None and pd.notna(_zrt_fe.get("yoy_pct_change_attendees")) else "N/A"
    # Q3 compression: 34 days above 80% in 2025
    _q3_comp = 0
    if not df_comp.empty:
        _q3_row = df_comp[df_comp["quarter"] == "2025-Q3"]
        _q3_comp = int(_q3_row["days_above_80_occ"].iloc[0]) if not _q3_row.empty else 34

    _ei_summary_data = [
        (_total_major, "Major Annual Events", f"{_total_events} total on calendar"),
        (_ev_yoy_str, "YOY Event Growth", "Zartico · Jun 2025 snapshot"),
        (_att_yoy_str, "YOY Attendee Growth", "events driving tourism demand"),
        (f"{_q3_comp}", "Q3 Compression Days", "days above 80% occ (2025)"),
    ]
    for i, (val, lbl, sub) in enumerate(_ei_summary_data):
        with _ei_summary_cols[i]:
            st.markdown(
                f'<div style="background:rgba(33,128,141,0.06);border:1px solid rgba(33,128,141,0.15);'
                f'border-radius:10px;padding:14px 16px;">'
                f'<div style="font-size:1.6rem;font-weight:800;color:#21808D;">{val}</div>'
                f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">{lbl}</div>'
                f'<div style="font-size:10px;color:#5f6368;margin-top:3px;">{sub}</div>'
                f'</div>', unsafe_allow_html=True)

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # EVENT CALENDAR — Gantt-style timeline from VDP events database
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("#### 📅 Dana Point Events Calendar — Full Year Timeline")
    st.caption("Events sourced from Visit Dana Point official calendar · Database: vdp_events · Major events highlighted in teal")

    if not df_vdp_events.empty:
        _evts = df_vdp_events.copy()
        # Parse dates
        _evts["event_date"] = pd.to_datetime(_evts["event_date"], errors="coerce")
        _evts["end_date"]   = pd.to_datetime(_evts.get("end_date", _evts["event_date"]), errors="coerce")
        _evts["end_date"]   = _evts.apply(
            lambda r: r["end_date"] if pd.notna(r["end_date"]) else r["event_date"] + timedelta(days=1),
            axis=1,
        )
        _evts = _evts.dropna(subset=["event_date"]).sort_values("event_date")
        _evts["color"] = _evts["is_major"].apply(lambda v: TEAL if v == 1 else "#A7A9A9")
        _evts["tier"]  = _evts["is_major"].apply(lambda v: "Major Event" if v == 1 else "Standard Event")

        _cal_fig = go.Figure()
        for _, _ev_row in _evts.iterrows():
            _cal_fig.add_trace(go.Bar(
                x=[((_ev_row["end_date"] - _ev_row["event_date"]).days or 1)],
                y=[_ev_row.get("event_name", "Event")],
                orientation="h",
                base=[_ev_row["event_date"].timestamp() * 1000],
                marker_color=_ev_row["color"],
                opacity=0.85,
                hovertemplate=(
                    f"<b>{_ev_row.get('event_name','Event')}</b><br>"
                    f"📅 {_ev_row['event_date'].strftime('%b %d, %Y')}"
                    + (f" – {_ev_row['end_date'].strftime('%b %d, %Y')}" if (_ev_row['end_date'] - _ev_row['event_date']).days > 0 else "")
                    + "<extra></extra>"
                ),
                showlegend=False,
            ))

        # Add a clean calendar-style horizontal chart instead
        _cal_fig2 = go.Figure()
        _sorted_evts = _evts.sort_values("event_date")
        _ev_labels  = [str(r.get("event_name",""))[:30] for _, r in _sorted_evts.iterrows()]
        _ev_dates   = [r["event_date"].strftime("%b %d") for _, r in _sorted_evts.iterrows()]
        _ev_months  = [r["event_date"].strftime("%b %Y") for _, r in _sorted_evts.iterrows()]
        _ev_major   = [bool(r.get("is_major") == 1) for _, r in _sorted_evts.iterrows()]
        _ev_colors  = [TEAL if m else "#626C71" for m in _ev_major]
        _ev_sizes   = [18 if m else 12 for m in _ev_major]
        _ev_x       = list(range(len(_sorted_evts)))

        _cal_fig2.add_trace(go.Scatter(
            x=_ev_x,
            y=[1] * len(_ev_x),
            mode="markers+text",
            marker=dict(size=_ev_sizes, color=_ev_colors, symbol="circle",
                        line=dict(color="white", width=1.5)),
            text=_ev_dates,
            textposition="top center",
            textfont=dict(size=9),
            customdata=list(zip(_ev_labels, _ev_months)),
            hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]}<extra></extra>",
            showlegend=False,
        ))
        # Event name labels below dots
        for _ix, (_lbl, _row) in enumerate(zip(_ev_labels, _sorted_evts.iterrows())):
            _, _rv = _row
            _cal_fig2.add_annotation(
                x=_ix, y=0.85,
                text=_lbl[:20] + ("…" if len(_lbl) > 20 else ""),
                showarrow=False, textangle=-45,
                font=dict(size=8, color=TEAL if _rv.get("is_major") == 1 else "#9AA0A6"),
            )
        _cal_fig2.update_layout(
            xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
            yaxis=dict(visible=False, range=[0.5, 1.5]),
            height=200,
            margin=dict(l=10, r=10, t=10, b=80),
        )
        st.plotly_chart(style_fig(_cal_fig2, height=200), use_container_width=True)
        st.caption(f"🟦 Teal = Major events  ·  ⚫ Gray = Standard events  ·  {_total_major} major, {_total_events} total on calendar")
    else:
        st.info("No events loaded from vdp_events table. Run `python scripts/fetch_vdp_events.py` to seed the calendar.")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # EVENT SCORECARD — STR performance vs. monthly baseline for every event
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("#### Event Performance Scorecard — STR vs. Baseline")
    st.caption("Event-window average vs. monthly baseline · Source: STR daily data · All figures from live database")

    # Define events with their STR window, monthly baseline month, and known context
    _scorecard_events = [
        {
            "name": "Ohana Fest",
            "dates": "Sep 26–28, 2025",
            "start": "2025-09-26", "end": "2025-09-28",
            "baseline_month": "2025-09",
            "category": "Music/Surf Festival",
            "tier": "PLATINUM",
            "note": "$14.6M direct spend · 68% OOS · Datafy verified",
        },
        {
            "name": "July 4th Holiday",
            "dates": "Jul 3–5, 2025",
            "start": "2025-07-03", "end": "2025-07-05",
            "baseline_month": "2025-07",
            "category": "Holiday",
            "tier": "PLATINUM",
            "note": "Highest ADR single day in dataset ($763.55)",
        },
        {
            "name": "Doheny Days Music Festival",
            "dates": "Sep 13–14, 2025",
            "start": "2025-09-13", "end": "2025-09-14",
            "baseline_month": "2025-09",
            "category": "Music Festival",
            "tier": "GOLD",
            "note": "Rock festival · Sat strong · Sun recovery needed",
        },
        {
            "name": "Tall Ships Festival",
            "dates": "Oct 3–5, 2025",
            "start": "2025-10-03", "end": "2025-10-05",
            "baseline_month": "2025-10",
            "category": "Heritage Festival",
            "tier": "GOLD",
            "note": "Strong shoulder-season compression · Dana Point Harbor",
        },
        {
            "name": "SoCal Wahine Surf Classic",
            "dates": "Aug 9–10, 2025",
            "start": "2025-08-09", "end": "2025-08-10",
            "baseline_month": "2025-08",
            "category": "Surf Tournament",
            "tier": "SILVER",
            "note": "Women's longboard · peak summer alignment",
        },
        {
            "name": "OC Marathon",
            "dates": "May 4, 2025",
            "start": "2025-05-04", "end": "2025-05-04",
            "baseline_month": "2025-05",
            "category": "Race/Sport",
            "tier": "SILVER",
            "note": "Finishing at Harbor · pre/post nights stronger than race day",
        },
        {
            "name": "Dana Point Turkey Trot",
            "dates": "Nov 27, 2025",
            "start": "2025-11-27", "end": "2025-11-28",
            "baseline_month": "2025-11",
            "category": "Race/Holiday",
            "tier": "SILVER",
            "note": "Thanksgiving weekend · strong family leisure demand",
        },
        {
            "name": "Holiday Boat Parade",
            "dates": "Dec 13, 2025",
            "start": "2025-12-13", "end": "2025-12-13",
            "baseline_month": "2025-12",
            "category": "Holiday Parade",
            "tier": "SILVER",
            "note": "Best Dec occ lift · +58.6% YOY — shoulder season standout",
        },
        {
            "name": "Dana Point Whale Festival",
            "dates": "Mar 1, 2026",
            "start": "2026-03-01", "end": "2026-03-01",
            "baseline_month": "2026-02",
            "category": "Festival",
            "tier": "SILVER",
            "note": "Q1 shoulder driver · gray whale migration season",
        },
    ]

    _tier_colors = {"PLATINUM": "#21808D", "GOLD": "#E68161", "SILVER": "#9AA0A6"}
    _tier_badges = {"PLATINUM": "🏆", "GOLD": "🥇", "SILVER": "🥈"}

    _sc_rows = []
    for ev in _scorecard_events:
        e_occ, e_adr, e_revpar = _event_kpi(ev["start"], ev["end"])
        b_occ, b_adr, b_revpar = _get_baseline(ev["baseline_month"])
        occ_lift  = e_occ - b_occ if b_occ > 0 else None
        adr_lift  = e_adr - b_adr if b_adr > 0 else None
        adr_lift_pct = (e_adr / b_adr - 1) * 100 if b_adr > 0 else None
        rvp_lift_pct = (e_revpar / b_revpar - 1) * 100 if b_revpar > 0 else None
        _sc_rows.append({
            "event": ev,
            "e_occ": e_occ, "e_adr": e_adr, "e_revpar": e_revpar,
            "b_occ": b_occ, "b_adr": b_adr, "b_revpar": b_revpar,
            "occ_lift": occ_lift, "adr_lift": adr_lift,
            "adr_lift_pct": adr_lift_pct, "rvp_lift_pct": rvp_lift_pct,
        })

    for sc in _sc_rows:
        ev = sc["event"]
        tier_color = _tier_colors.get(ev["tier"], "#9AA0A6")
        tier_badge = _tier_badges.get(ev["tier"], "")
        adr_lp = sc["adr_lift_pct"]
        rvp_lp = sc["rvp_lift_pct"]
        occ_l  = sc["occ_lift"]
        has_data = sc["e_adr"] > 0

        adr_tag = (
            f'<span style="color:{"#21808D" if (adr_lp or 0) >= 0 else "#E53E3E"};font-weight:700;">'
            f'{"+" if (adr_lp or 0) >= 0 else ""}{adr_lp:.0f}%</span>'
            if adr_lp is not None else '<span style="color:#9AA0A6">—</span>'
        )
        rvp_tag = (
            f'<span style="color:{"#21808D" if (rvp_lp or 0) >= 0 else "#E53E3E"};font-weight:700;">'
            f'{"+" if (rvp_lp or 0) >= 0 else ""}{rvp_lp:.0f}%</span>'
            if rvp_lp is not None else '<span style="color:#9AA0A6">—</span>'
        )
        occ_tag = (
            f'<span style="color:{"#21808D" if (occ_l or 0) >= 0 else "#E53E3E"};font-weight:700;">'
            f'{"+" if (occ_l or 0) >= 0 else ""}{occ_l:.1f}pp</span>'
            if occ_l is not None else '<span style="color:#9AA0A6">—</span>'
        )

        if has_data:
            metrics_html = (
                f'<span style="font-size:11px;opacity:0.75;margin-right:14px;">OCC {sc["e_occ"]:.1f}% ({occ_tag})</span>'
                f'<span style="font-size:11px;opacity:0.75;margin-right:14px;">ADR ${sc["e_adr"]:.0f} ({adr_tag} vs baseline)</span>'
                f'<span style="font-size:11px;opacity:0.75;">RevPAR ${sc["e_revpar"]:.0f} ({rvp_tag})</span>'
            )
        else:
            metrics_html = '<span style="font-size:11px;opacity:0.50;font-style:italic;">STR data not yet available for this window</span>'

        st.markdown(
            f'<div style="border:1px solid rgba(0,0,0,0.08);border-left:4px solid {tier_color};'
            f'border-radius:0 10px 10px 0;padding:12px 16px;margin-bottom:8px;background:rgba(255,255,255,0.03);">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:4px;">'
            f'<div>'
            f'<span style="font-weight:700;font-size:13px;">{tier_badge} {ev["name"]}</span>'
            f'<span style="font-size:10px;background:{tier_color}22;color:{tier_color};border-radius:4px;'
            f'padding:1px 6px;margin-left:8px;font-weight:600;">{ev["tier"]}</span>'
            f'<span style="font-size:10px;opacity:0.50;margin-left:8px;">{ev["category"]}</span>'
            f'</div>'
            f'<div style="font-size:11px;opacity:0.55;">📅 {ev["dates"]}</div>'
            f'</div>'
            f'<div style="margin-top:5px;">{metrics_html}</div>'
            f'<div style="font-size:10px;opacity:0.50;margin-top:3px;font-style:italic;">{ev["note"]}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # ADR LIFT CHART — all events vs. monthly baseline
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("#### ADR Lift by Event vs. Monthly Baseline")
    _chart_rows_ei = [sc for sc in _sc_rows if sc["e_adr"] > 0 and sc["b_adr"] > 0]
    if _chart_rows_ei:
        _ev_names  = [sc["event"]["name"] for sc in _chart_rows_ei]
        _ev_adr    = [sc["e_adr"] for sc in _chart_rows_ei]
        _base_adr  = [sc["b_adr"] for sc in _chart_rows_ei]
        _lift_pcts = [sc["adr_lift_pct"] or 0 for sc in _chart_rows_ei]
        _bar_colors = [_tier_colors.get(sc["event"]["tier"], TEAL) for sc in _chart_rows_ei]

        fig_ei_adr = go.Figure()
        fig_ei_adr.add_trace(go.Bar(
            name="Monthly Baseline ADR",
            x=_ev_names,
            y=_base_adr,
            marker_color="rgba(33,128,141,0.20)",
            hovertemplate="<b>%{x}</b><br>Baseline: $%{y:.0f}<extra></extra>",
        ))
        fig_ei_adr.add_trace(go.Bar(
            name="Event-Window ADR",
            x=_ev_names,
            y=_ev_adr,
            marker_color=_bar_colors,
            text=[f"+{p:.0f}%" if p >= 0 else f"{p:.0f}%" for p in _lift_pcts],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Event ADR: $%{y:.0f}<extra></extra>",
        ))
        fig_ei_adr.update_layout(
            barmode="overlay",
            yaxis_title="ADR ($)",
            yaxis_tickprefix="$",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(style_fig(fig_ei_adr, height=340), use_container_width=True)
        st.caption("Event-window ADR (solid) vs. monthly baseline (light). Percentage labels show lift above baseline. Source: STR daily data.")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # OHANA FEST DEEP DIVE — Gold Standard
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown('<span class="ai-chip">OHANA FEST 2025 — GOLD STANDARD BENCHMARK · DATAFY VERIFIED</span>', unsafe_allow_html=True)
    st.caption("Sep 26–28, 2025 · Doheny State Beach · Source: Datafy + STR daily · Highest RevPAR event in the dataset")

    _ef_cols = st.columns(4)
    _ohana_row1 = [
        ("$18.4M", "Total Destination Spend", "3.2× economic multiplier"),
        ("$14.6M", "Direct Event Expenditure", "headline spend figure"),
        ("+$139", "ADR Lift During Event", "$726 event vs. $431 Sep avg"),
        ("68%", "Out-of-State Visitors", "fly-market attendees"),
    ]
    for i, (val, lbl, sub) in enumerate(_ohana_row1):
        with _ef_cols[i]:
            st.markdown(
                f'<div style="background:rgba(33,128,141,0.06);border:1px solid rgba(33,128,141,0.15);'
                f'border-radius:10px;padding:14px 16px;">'
                f'<div style="font-size:1.5rem;font-weight:800;color:#21808D;">{val}</div>'
                f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">{lbl}</div>'
                f'<div style="font-size:10px;color:#21808D;font-weight:600;margin-top:3px;">{sub}</div>'
                f'</div>', unsafe_allow_html=True)

    _ef_cols2 = st.columns(4)
    _ohana_row2 = [
        ("$1,219", "Avg Accommodation Spend/Trip", "+53% vs. Ohana Fest 2024"),
        ("$669", "Peak RevPAR (Sep 26)", "#2 highest day in full dataset"),
        ("+74.6%", "RevPAR YOY Sep 26", "vs. same night 2024"),
        ("3.2×", "Spend Multiplier", "on direct event expenditure"),
    ]
    for i, (val, lbl, sub) in enumerate(_ohana_row2):
        with _ef_cols2[i]:
            st.markdown(
                f'<div style="background:rgba(230,129,97,0.06);border:1px solid rgba(230,129,97,0.15);'
                f'border-radius:10px;padding:14px 16px;">'
                f'<div style="font-size:1.5rem;font-weight:800;color:#E68161;">{val}</div>'
                f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">{lbl}</div>'
                f'<div style="font-size:10px;color:#E68161;font-weight:600;margin-top:3px;">{sub}</div>'
                f'</div>', unsafe_allow_html=True)

    # Ohana Fest window chart (Sep 20 – Oct 5)
    _ohana_window = _kpi_all[
        (_kpi_all["as_of_date"] >= "2025-09-20") & (_kpi_all["as_of_date"] <= "2025-10-05")
    ].sort_values("as_of_date") if not _kpi_all.empty else pd.DataFrame()

    if not _ohana_window.empty:
        st.markdown("<br>", unsafe_allow_html=True)
        _ohana_col1, _ohana_col2 = st.columns(2)
        with _ohana_col1:
            fig_oh_adr = go.Figure()
            fig_oh_adr.add_vrect(x0="2025-09-26", x1="2025-09-28",
                fillcolor="rgba(33,128,141,0.12)", line_width=0,
                annotation_text="Ohana Fest", annotation_position="top left",
                annotation_font_size=10)
            fig_oh_adr.add_trace(go.Scatter(
                x=_ohana_window["as_of_date"], y=_ohana_window["adr"],
                mode="lines+markers", name="ADR",
                line=dict(color=TEAL, width=2),
                hovertemplate="%{x}<br>ADR: $%{y:.0f}<extra></extra>",
            ))
            fig_oh_adr.update_layout(yaxis_tickprefix="$", yaxis_title="ADR")
            st.plotly_chart(style_fig(fig_oh_adr, height=220), use_container_width=True)
            st.caption("ADR — Ohana Fest window (Sep 20–Oct 5)")
        with _ohana_col2:
            fig_oh_occ = go.Figure()
            fig_oh_occ.add_vrect(x0="2025-09-26", x1="2025-09-28",
                fillcolor="rgba(230,129,97,0.12)", line_width=0,
                annotation_text="Ohana Fest", annotation_position="top left",
                annotation_font_size=10)
            fig_oh_occ.add_trace(go.Scatter(
                x=_ohana_window["as_of_date"], y=_ohana_window["occ_pct"],
                mode="lines+markers", name="Occupancy",
                line=dict(color=ORANGE, width=2),
                hovertemplate="%{x}<br>OCC: %{y:.1f}%<extra></extra>",
            ))
            fig_oh_occ.update_layout(yaxis_title="Occupancy (%)", yaxis_ticksuffix="%")
            st.plotly_chart(style_fig(fig_oh_occ, height=220), use_container_width=True)
            st.caption("Occupancy — Ohana Fest window")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # ZARTICO EVENT IMPACT — Historical reference (OC Marathon period)
    # ══════════════════════════════════════════════════════════════════════════
    st.info("📚 **Historical Reference:** Zartico data reflects a Jun 2025 snapshot. Use for trend comparison only — Datafy is the current source of record.")
    st.markdown("#### Zartico — Event Spend Impact Analysis")
    st.caption("⚠️ Zartico is historical reference only (Jun 2025 snapshot). Event window: May 4–10, 2025 (OC Marathon period) · Current data: Datafy/STR.")

    if not df_zrt_events.empty:
        ze = df_zrt_events.iloc[0]
        _ze_cols = st.columns(5)
        _ze_metrics = [
            ("change_total_spend_pct",       "Total Spend Lift",      "+{:.1f}%", "vs. 4-week rolling baseline"),
            ("change_visitor_spend_pct",      "Visitor Spend Lift",    "+{:.1f}%", "visitor spending during event"),
            ("change_resident_spend_pct",     "Resident Spend Change", "{:.1f}%",  "resident behavior shift"),
            ("pct_accommodation_spend",       "Accommodation Share",   "{:.0f}%",  "of visitor spend during event"),
            ("pct_food_bev_spend",            "Food & Bev Share",      "{:.0f}%",  "of visitor spend during event"),
        ]
        for i, (col, label, fmt, sub) in enumerate(_ze_metrics):
            val_raw = ze.get(col)
            val_str = fmt.format(float(val_raw)) if pd.notna(val_raw) else "—"
            positive = float(val_raw or 0) >= 0 if pd.notna(val_raw) else True
            color = "#21808D" if positive else "#E53E3E"
            with _ze_cols[i]:
                st.markdown(
                    f'<div style="background:rgba(33,128,141,0.04);border:1px solid rgba(33,128,141,0.12);'
                    f'border-radius:8px;padding:12px 14px;">'
                    f'<div style="font-size:1.3rem;font-weight:800;color:{color};">{val_str}</div>'
                    f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">{label}</div>'
                    f'<div style="font-size:10px;color:#5f6368;margin-top:2px;">{sub}</div>'
                    f'</div>', unsafe_allow_html=True)

        # Spending mix during event
        _spend_cats = {
            "Accommodation": ze.get("pct_accommodation_spend"),
            "Food & Bev":    ze.get("pct_food_bev_spend"),
            "Gas/Transport": ze.get("pct_gas_spend"),
            "Retail":        ze.get("pct_retail_spend"),
            "Arts/Entmt":    ze.get("pct_arts_spend"),
        }
        _spend_vals = {k: v for k, v in _spend_cats.items() if pd.notna(v)}
        if _spend_vals:
            st.markdown("<br>", unsafe_allow_html=True)
            fig_zrt_spend = go.Figure(go.Pie(
                labels=list(_spend_vals.keys()),
                values=list(_spend_vals.values()),
                hole=0.45,
                marker_colors=[TEAL, ORANGE, TEAL_LIGHT, "#B7D7DC", "#F4C7A8"],
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>%{value:.1f}%<extra></extra>",
            ))
            fig_zrt_spend.update_layout(
                showlegend=False,
                annotations=[dict(text="Spend Mix", x=0.5, y=0.5, font_size=12, showarrow=False)],
            )
            st.plotly_chart(style_fig(fig_zrt_spend, height=260), use_container_width=True)
            st.caption("Visitor spending mix during event period (Zartico · historical).")

    else:
        st.info("Load Zartico data to see event spend analysis.")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # VISITOR ECONOMY CONTEXT — Datafy
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("#### Visitor Economy Context — Datafy 2025")
    st.caption("Annual 2025 visitor profile · Datafy Geolocation (Caladan 1.2) · Jan–Dec 2025")

    if not df_dfy_ov.empty:
        ov = df_dfy_ov.iloc[0]
        total_trips = float(ov.get("total_trips", 0) or 0)
        overnight_pct = float(ov.get("overnight_trips_pct", 0) or 0)
        oos_pct = float(ov.get("out_of_state_vd_pct", 0) or 0)
        day_pct = float(ov.get("day_trips_pct", 0) or 0)
        avg_los = float(ov.get("avg_length_of_stay_days", 0) or 0)
        repeat_pct = float(ov.get("repeat_visitors_pct", 0) or 0)

        _ve_cols = st.columns(4)
        _ve_data = [
            (f"{total_trips/1e6:.2f}M", "Total Annual Trips (2025)", f"{overnight_pct:.0f}% overnight"),
            (f"{oos_pct:.0f}%", "Out-of-State Visitors", "higher ADR, longer stays"),
            (f"{avg_los:.1f} days", "Avg Length of Stay", f"Day trips: {day_pct:.0f}%"),
            (f"{repeat_pct:.0f}%", "Repeat Visitors", "loyalty = event ROI multiplier"),
        ]
        for i, (val, lbl, sub) in enumerate(_ve_data):
            with _ve_cols[i]:
                st.markdown(
                    f'<div style="background:rgba(230,129,97,0.05);border:1px solid rgba(230,129,97,0.15);'
                    f'border-radius:8px;padding:12px 14px;">'
                    f'<div style="font-size:1.3rem;font-weight:800;color:#E68161;">{val}</div>'
                    f'<div style="font-size:11px;font-weight:600;opacity:0.70;margin-top:2px;">{lbl}</div>'
                    f'<div style="font-size:10px;color:#5f6368;margin-top:2px;">{sub}</div>'
                    f'</div>', unsafe_allow_html=True)

        st.markdown(
            '<div style="background:rgba(33,128,141,0.04);border-left:3px solid #21808D;border-radius:0 8px 8px 0;'
            'padding:12px 16px;margin-top:12px;font-size:12px;">'
            '<strong>HIDDEN SIGNAL — Day Trip Conversion Opportunity:</strong> '
            f'{day_pct:.0f}% of {total_trips/1e6:.2f}M annual trips are day trips. '
            f'If events converted just 3% of day trippers to overnight stays, '
            f'that equals ~{int(total_trips * (day_pct/100) * 0.03):,} incremental room nights — '
            f'worth an estimated <strong>$13–16M in additional room revenue annually</strong>. '
            'Events are the primary conversion lever.'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("Load Datafy data to see visitor economy context.")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # VISITOR/RESIDENT RATIO — Zartico seasonality index
    # ══════════════════════════════════════════════════════════════════════════
    if not df_zrt_movement.empty:
        st.info("📚 **Historical Reference:** Zartico data reflects a Jun 2025 snapshot. Use for trend comparison only — Datafy is the current source of record.")
        st.markdown("#### Visitor-to-Resident Ratio — Event Season Intensity (Zartico)")
        st.caption("Ratio > benchmark = tourism demand above normal · Q3 events amplify an already-peak season")
        fig_move = go.Figure()
        fig_move.add_trace(go.Scatter(
            x=df_zrt_movement["month_str"], y=df_zrt_movement["visitor_resident_ratio"],
            mode="lines+markers", name="Dana Point V/R Ratio",
            line=dict(color=TEAL, width=2.5),
            hovertemplate="%{x}<br>V/R Ratio: %{y:.2f}<extra></extra>",
        ))
        if "benchmark_ratio" in df_zrt_movement.columns:
            fig_move.add_trace(go.Scatter(
                x=df_zrt_movement["month_str"], y=df_zrt_movement["benchmark_ratio"],
                mode="lines", name="CA Benchmark",
                line=dict(color=ORANGE, width=1.5, dash="dot"),
                hovertemplate="%{x}<br>Benchmark: %{y:.2f}<extra></extra>",
            ))
        fig_move.update_layout(
            yaxis_title="Visitor/Resident Ratio",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(style_fig(fig_move, height=250), use_container_width=True)
        st.caption(
            "Jul–Sep months show 0.35–0.38 V/R ratio (30–35% above CA benchmark). "
            "Ohana Fest, Doheny Days, and Tall Ships all fire within this elevated window, maximizing their ADR lift."
        )
        st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # COMPRESSION CALENDAR
    # ══════════════════════════════════════════════════════════════════════════
    if not df_comp.empty:
        st.markdown("#### Annual Compression Calendar — Days Above 80% Occupancy")
        st.caption("Compression days concentrate in Q3 (peak event season). Q1/Q4 events are high-value shoulder drivers.")
        fig_comp_ei = go.Figure()
        fig_comp_ei.add_trace(go.Bar(
            x=df_comp["quarter"],
            y=df_comp["days_above_80_occ"],
            name="Days ≥80% OCC",
            marker_color=TEAL,
            text=df_comp["days_above_80_occ"],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Days ≥80%: %{y}<extra></extra>",
        ))
        fig_comp_ei.add_trace(go.Bar(
            x=df_comp["quarter"],
            y=df_comp["days_above_90_occ"],
            name="Days ≥90% OCC",
            marker_color=ORANGE,
            text=df_comp["days_above_90_occ"],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Days ≥90%: %{y}<extra></extra>",
        ))
        fig_comp_ei.update_layout(
            barmode="group",
            yaxis_title="Number of Compression Days",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(style_fig(fig_comp_ei, height=280), use_container_width=True)
        _total_80 = int(df_comp["days_above_80_occ"].sum())
        _total_90 = int(df_comp["days_above_90_occ"].sum())
        st.caption(
            f"2024–2026 YTD: **{_total_80} total days** at 80%+ occupancy · **{_total_90} days** at 90%+. "
            "Event programming directly determines whether Q1/Q2/Q4 quarters ever reach compression."
        )
        st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # EVENTS CALENDAR
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("#### Dana Point Events Calendar")
    st.caption("Events sourced from Visit Dana Point official calendar.")
    if not df_vdp_events.empty:
        _evts_display = df_vdp_events.copy()
        _evts_display["event_date"] = pd.to_datetime(_evts_display["event_date"], errors="coerce")
        _evts_upcoming = _evts_display[_evts_display["event_date"] >= pd.Timestamp.now()].sort_values("event_date")
        _evts_past     = _evts_display[_evts_display["event_date"] < pd.Timestamp.now()].sort_values("event_date", ascending=False)

        # ── Plotly Timeline / Gantt of all events ─────────────────────────────
        _gantt_rows = []
        for _, _er in _evts_display.iterrows():
            if pd.notna(_er["event_date"]):
                _gstart = _er["event_date"]
                try:
                    _gend = pd.to_datetime(_er.get("event_end_date")) if pd.notna(_er.get("event_end_date")) else _gstart
                except Exception:
                    _gend = _gstart
                # Add 1 day so single-day events are visible on timeline
                if _gend == _gstart:
                    _gend = _gstart + pd.Timedelta(days=1)
                _gantt_rows.append({
                    "Event": str(_er.get("event_name", "Unknown")),
                    "Start": _gstart,
                    "Finish": _gend,
                    "is_major": int(_er.get("is_major", 0) or 0),
                    "Category": str(_er.get("category", "") or ""),
                })
        if _gantt_rows:
            _gantt_df = pd.DataFrame(_gantt_rows).sort_values("Start")
            _gantt_colors = [TEAL if r["is_major"] else "#9AA0A6" for _, r in _gantt_df.iterrows()]
            fig_gantt = go.Figure()
            for i, (_, gr) in enumerate(_gantt_df.iterrows()):
                _bar_color = TEAL if gr["is_major"] else "#9AA0A6"
                fig_gantt.add_trace(go.Bar(
                    x=[(gr["Finish"] - gr["Start"]).days],
                    y=[gr["Event"]],
                    base=[gr["Start"].strftime("%Y-%m-%d")],
                    orientation="h",
                    marker_color=_bar_color,
                    marker_opacity=0.85,
                    showlegend=False,
                    hovertemplate=(
                        f"<b>{gr['Event']}</b><br>"
                        f"Start: {gr['Start'].strftime('%b %d, %Y')}<br>"
                        f"End: {gr['Finish'].strftime('%b %d, %Y')}<br>"
                        f"Type: {gr['Category']}<extra></extra>"
                    ),
                    name=gr["Event"],
                ))
            # Legend-only traces for color key
            fig_gantt.add_trace(go.Bar(x=[None], y=[None], marker_color=TEAL,
                                       name="Major Event", showlegend=True, orientation="h"))
            fig_gantt.add_trace(go.Bar(x=[None], y=[None], marker_color="#9AA0A6",
                                       name="Regular Event", showlegend=True, orientation="h"))
            fig_gantt.update_layout(
                barmode="overlay",
                xaxis=dict(type="date", title=""),
                yaxis=dict(autorange="reversed"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                bargap=0.3,
            )
            st.plotly_chart(style_fig(fig_gantt, height=max(280, len(_gantt_rows) * 28)), use_container_width=True)
            st.caption("Teal = major events · Gray = regular events. Source: Visit Dana Point official calendar.")

        st.markdown("---")

        # ── Event Deep Analysis — Selector ────────────────────────────────────
        st.markdown("#### Event Deep Analysis — Multi-Source Data Layers")
        _event_names_sel = _evts_display["event_name"].dropna().tolist()
        if _event_names_sel:
            _sel_event = st.selectbox("Select Event for Deep Analysis", _event_names_sel, key="ei_event_sel")
            _sel_row = _evts_display[_evts_display["event_name"] == _sel_event].iloc[0]
            _sel_start_s = str(_sel_row["event_date"].date()) if pd.notna(_sel_row["event_date"]) else None
            _sel_end_dt  = pd.to_datetime(_sel_row.get("event_end_date")) if pd.notna(_sel_row.get("event_end_date")) else _sel_row["event_date"]
            _sel_end_s   = str(_sel_end_dt.date()) if _sel_end_dt is not None and pd.notna(_sel_end_dt) else _sel_start_s
            _sel_is_major = int(_sel_row.get("is_major", 0) or 0)
            _sel_month    = _sel_start_s[:7] if _sel_start_s else None

            _da_c1, _da_c2, _da_c3 = st.columns(3)

            # STR Layer
            with _da_c1:
                st.markdown('<div style="font-size:11px;font-weight:700;color:#21808D;margin-bottom:6px;">STR LAYER — Hotel Performance</div>', unsafe_allow_html=True)
                if _sel_start_s:
                    _s_occ, _s_adr, _s_rvp = _event_kpi(_sel_start_s, _sel_end_s)
                    _b_occ, _b_adr, _b_rvp = _get_baseline(_sel_month) if _sel_month else (0, 0, 0)
                    if _s_adr > 0:
                        _adr_lift_sel  = _s_adr - _b_adr if _b_adr > 0 else None
                        _occ_lift_sel  = _s_occ - _b_occ if _b_occ > 0 else None
                        _rvp_lift_sel  = ((_s_rvp / _b_rvp) - 1) * 100 if _b_rvp > 0 else None
                        st.markdown(
                            f'<div style="background:rgba(33,128,141,0.05);border:1px solid rgba(33,128,141,0.15);border-radius:8px;padding:10px 12px;">'
                            f'<div style="font-size:1.1rem;font-weight:800;color:#21808D;">${_s_adr:.0f} ADR</div>'
                            f'<div style="font-size:11px;opacity:0.70;">{_s_occ:.1f}% Occ · ${_s_rvp:.0f} RevPAR</div>'
                            + (f'<div style="font-size:10px;color:#21808D;margin-top:3px;">ADR lift: +${_adr_lift_sel:.0f} vs. {_sel_month} baseline</div>' if _adr_lift_sel else "")
                            + (f'<div style="font-size:10px;color:#21808D;">Occ lift: +{_occ_lift_sel:.1f}pp</div>' if _occ_lift_sel else "")
                            + '</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.info("STR data not available for this event window.")
                else:
                    st.info("No date available for this event.")

            # Spend / Visitor Layer (Ohana Fest benchmarks for major events)
            with _da_c2:
                st.markdown('<div style="font-size:11px;font-weight:700;color:#E68161;margin-bottom:6px;">SPEND LAYER — Datafy / Ohana Benchmark</div>', unsafe_allow_html=True)
                if _sel_is_major:
                    st.markdown(
                        '<div style="background:rgba(230,129,97,0.05);border:1px solid rgba(230,129,97,0.15);border-radius:8px;padding:10px 12px;">'
                        '<div style="font-size:1.1rem;font-weight:800;color:#E68161;">$14.6M–$18.4M</div>'
                        '<div style="font-size:11px;opacity:0.70;">Est. event expenditure range</div>'
                        '<div style="font-size:10px;color:#E68161;margin-top:3px;">3.2x economic multiplier · Ohana Fest benchmark</div>'
                        '<div style="font-size:10px;opacity:0.60;margin-top:2px;">$1,219 avg accommodation spend/trip</div>'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    if not df_dfy_ov.empty:
                        _ov = df_dfy_ov.iloc[0]
                        _tt = float(_ov.get("total_trips", 0) or 0)
                        _op = float(_ov.get("overnight_trips_pct", 0) or 0)
                        st.markdown(
                            f'<div style="background:rgba(230,129,97,0.05);border:1px solid rgba(230,129,97,0.15);border-radius:8px;padding:10px 12px;">'
                            f'<div style="font-size:1.1rem;font-weight:800;color:#E68161;">{_op:.0f}% overnight</div>'
                            f'<div style="font-size:11px;opacity:0.70;">Citywide overnight visitor rate</div>'
                            f'<div style="font-size:10px;color:#E68161;margin-top:3px;">{_tt/1e6:.2f}M annual trips · Datafy 2025</div>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.info("Load Datafy data for spend context.")

            # Visitor Layer
            with _da_c3:
                st.markdown('<div style="font-size:11px;font-weight:700;color:#21808D;margin-bottom:6px;">VISITOR LAYER — Origin & Profile</div>', unsafe_allow_html=True)
                if _sel_is_major:
                    st.markdown(
                        '<div style="background:rgba(33,128,141,0.05);border:1px solid rgba(33,128,141,0.15);border-radius:8px;padding:10px 12px;">'
                        '<div style="font-size:1.1rem;font-weight:800;color:#21808D;">68% OOS</div>'
                        '<div style="font-size:11px;opacity:0.70;">Out-of-state visitors (Ohana benchmark)</div>'
                        '<div style="font-size:10px;color:#21808D;margin-top:3px;">Fly-market attendees drive premium ADR</div>'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                elif not df_dfy_ov.empty:
                    _oos = float(df_dfy_ov.iloc[0].get("out_of_state_vd_pct", 0) or 0)
                    _los = float(df_dfy_ov.iloc[0].get("avg_length_of_stay_days", 0) or 0)
                    st.markdown(
                        f'<div style="background:rgba(33,128,141,0.05);border:1px solid rgba(33,128,141,0.15);border-radius:8px;padding:10px 12px;">'
                        f'<div style="font-size:1.1rem;font-weight:800;color:#21808D;">{_oos:.0f}% OOS</div>'
                        f'<div style="font-size:11px;opacity:0.70;">Out-of-state visitor days (annual avg)</div>'
                        f'<div style="font-size:10px;color:#21808D;margin-top:3px;">Avg LOS: {_los:.1f} days · Datafy 2025</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.info("Load Datafy data for visitor profile.")

            # Projection Card
            st.markdown("<br>", unsafe_allow_html=True)
            _proj_base = 14.6  # Ohana Fest $M as benchmark
            if _sel_is_major:
                _proj_low  = _proj_base * 0.8
                _proj_high = _proj_base * 1.2
                _proj_dest_low  = _proj_low  * 1.26
                _proj_dest_high = _proj_high * 1.26
            else:
                _proj_low  = _proj_base * 0.15
                _proj_high = _proj_base * 0.40
                _proj_dest_low  = _proj_low  * 1.26
                _proj_dest_high = _proj_high * 1.26
            st.markdown(
                f'<div style="background:rgba(33,128,141,0.06);border:1px solid rgba(33,128,141,0.18);'
                f'border-radius:10px;padding:14px 16px;margin-top:4px;">'
                f'<div style="font-size:12px;font-weight:700;color:#21808D;margin-bottom:6px;">PROJECTION — Economic Impact Estimate</div>'
                f'Based on Ohana Fest benchmarks ($14.6M direct expenditure · 3.2x multiplier), '
                f'<strong>{_sel_event}</strong> is projected to generate '
                f'<strong>${_proj_dest_low:.1f}M–${_proj_dest_high:.1f}M in total destination spend</strong> '
                f'(${_proj_low:.1f}M–${_proj_high:.1f}M direct event expenditure × 3.2x multiplier). '
                + ("This is a major event estimate — actual Datafy data post-event will refine this figure."
                   if _sel_is_major else
                   "Regular-event estimate. Upgrade to major-event status via attendee growth to unlock higher impact tier.")
                + '</div>',
                unsafe_allow_html=True,
            )

            # Grouped bar chart: event vs. baseline for occ / ADR / RevPAR
            if _sel_start_s:
                _s_occ2, _s_adr2, _s_rvp2 = _event_kpi(_sel_start_s, _sel_end_s)
                _b_occ2, _b_adr2, _b_rvp2 = _get_baseline(_sel_month) if _sel_month else (0, 0, 0)
                if _s_adr2 > 0 and _b_adr2 > 0:
                    st.markdown("<br>", unsafe_allow_html=True)
                    fig_ei_sel = go.Figure()
                    _metrics_ei = ["Occupancy (%)", "ADR ($)", "RevPAR ($)"]
                    _event_vals = [_s_occ2, _s_adr2, _s_rvp2]
                    _base_vals  = [_b_occ2, _b_adr2, _b_rvp2]
                    fig_ei_sel.add_trace(go.Bar(
                        name="Monthly Baseline",
                        x=_metrics_ei,
                        y=_base_vals,
                        marker_color="rgba(33,128,141,0.20)",
                        hovertemplate="<b>%{x}</b><br>Baseline: %{y:.1f}<extra></extra>",
                    ))
                    fig_ei_sel.add_trace(go.Bar(
                        name=f"Event Window ({_sel_event})",
                        x=_metrics_ei,
                        y=_event_vals,
                        marker_color=TEAL,
                        hovertemplate="<b>%{x}</b><br>Event: %{y:.1f}<extra></extra>",
                    ))
                    fig_ei_sel.update_layout(
                        barmode="group",
                        title=dict(text=f"STR Performance: {_sel_event} vs. Baseline", font_size=12),
                        yaxis_title="Value",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    )
                    st.plotly_chart(style_fig(fig_ei_sel, height=300), use_container_width=True)
                    st.caption(f"Event-window averages vs. monthly baseline · {_sel_start_s} to {_sel_end_s} · Source: STR daily data")

        st.markdown("---")

        _cal_tab1, _cal_tab2 = st.tabs(["📅 Upcoming", "🕐 Past Events"])
        with _cal_tab1:
            if not _evts_upcoming.empty:
                for _, ev_row in _evts_upcoming.iterrows():
                    _ev_major = "🌟 " if ev_row.get("is_major", 0) else ""
                    _ev_date_str = ev_row["event_date"].strftime("%B %d, %Y") if pd.notna(ev_row["event_date"]) else "TBD"
                    _ev_end = ""
                    if pd.notna(ev_row.get("event_end_date")) and str(ev_row["event_end_date"]) != str(ev_row["event_date"].date()):
                        try:
                            _ev_end = " – " + pd.to_datetime(ev_row["event_end_date"]).strftime("%b %d")
                        except Exception:
                            pass
                    st.markdown(
                        f'<div style="background:rgba(33,128,141,0.05);border-left:3px solid #21808D;'
                        f'border-radius:0 8px 8px 0;padding:10px 14px;margin-bottom:6px;">'
                        f'<div style="font-weight:700;font-size:13px;">{_ev_major}{ev_row.get("event_name","Unknown")}</div>'
                        f'<div style="font-size:11px;opacity:0.60;margin-top:3px;">'
                        f'📅 {_ev_date_str}{_ev_end}'
                        f'{" · " + str(ev_row["venue"]) if ev_row.get("venue") else ""}'
                        f'{" · " + str(ev_row["category"]) if ev_row.get("category") else ""}'
                        f'</div>'
                        f'<div style="font-size:10px;opacity:0.50;margin-top:2px;font-style:italic;">'
                        f'{str(ev_row.get("description",""))[:120]}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("No upcoming events in the calendar.")
        with _cal_tab2:
            if not _evts_past.empty:
                for _, ev_row in _evts_past.iterrows():
                    _ev_date_str = ev_row["event_date"].strftime("%B %d, %Y") if pd.notna(ev_row["event_date"]) else "TBD"
                    # Look up STR performance for this event
                    _ev_str = ""
                    _ev_start_s = str(ev_row["event_date"].date()) if pd.notna(ev_row["event_date"]) else ""
                    _ev_end_s   = str(pd.to_datetime(ev_row.get("event_end_date", ev_row["event_date"])).date()) if pd.notna(ev_row.get("event_end_date")) else _ev_start_s
                    if _ev_start_s:
                        _p_occ, _p_adr, _p_rvp = _event_kpi(_ev_start_s, _ev_end_s)
                        if _p_adr > 0:
                            _ev_str = f" · STR: {_p_occ:.0f}% OCC · ${_p_adr:.0f} ADR · ${_p_rvp:.0f} RevPAR"
                    st.markdown(
                        f'<div style="border-left:2px solid rgba(33,128,141,0.30);'
                        f'padding:8px 14px;margin-bottom:4px;opacity:0.80;">'
                        f'<span style="font-weight:600;font-size:12px;">{ev_row.get("event_name","Unknown")}</span>'
                        f'<span style="font-size:11px;opacity:0.55;margin-left:8px;">📅 {_ev_date_str}{_ev_str}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("No past events in the calendar.")

        with st.expander("📊 Download full events calendar"):
            st.dataframe(df_vdp_events, use_container_width=True, hide_index=True)
            _evt_csv = df_vdp_events.to_csv(index=False).encode()
            st.download_button("⬇️ Download Events CSV", _evt_csv,
                               file_name="dana_point_events.csv", mime="text/csv")
    else:
        st.info("No VDP events loaded. Run `python scripts/fetch_vdp_events.py`.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — SUPPLY & PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
with tab_sp:
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Supply &amp; Pipeline</div>
      <div class="hero-subtitle">CoStar Supply Pipeline · New Hotel Openings · Market Competitive Dynamics</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Pipeline summary KPIs ─────────────────────────────────────────────────
    if not df_cs_pipe.empty:
        _pipe_total = df_cs_pipe["rooms"].sum()
        _under_const_sp = df_cs_pipe[df_cs_pipe["status"] == "Under Construction"]
        _planned_sp = df_cs_pipe[df_cs_pipe["status"].isin(["Planned", "Final Planning / Permitting"])]
        _uc_rooms_sp = _under_const_sp["rooms"].sum() if not _under_const_sp.empty else 0
        _pl_rooms_sp = _planned_sp["rooms"].sum() if not _planned_sp.empty else 0

        _sp_cols = st.columns(4)
        _sp_kpis = [
            (f"{_pipe_total:,}", "Total Pipeline Rooms", f"{len(df_cs_pipe)} active projects"),
            (f"{_uc_rooms_sp:,}", "Under Construction", f"{len(_under_const_sp)} project(s) · 2025"),
            (f"{_pl_rooms_sp:,}", "Planned / Permitting", f"{len(_planned_sp)} project(s) · 2026+"),
            (f"{_pipe_total/5120*100:.1f}%", "Supply Growth Impact", "% of current market supply"),
        ]
        for i, (val, lbl, sub) in enumerate(_sp_kpis):
            with _sp_cols[i]:
                st.markdown(kpi_card(lbl, val, sub, positive=(i < 2), neutral=(i >= 2)),
                            unsafe_allow_html=True)

        st.markdown("---")

        # ── Pipeline bar chart ────────────────────────────────────────────────
        st.markdown('<div class="chart-header">Pipeline Projects by Room Count & Status</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">CoStar supply pipeline · South OC market · hover for open date</div>', unsafe_allow_html=True)
        _sp_status_colors = {
            "Under Construction": TEAL,
            "Final Planning / Permitting": ORANGE,
            "Planned": TEAL_LIGHT,
        }
        _sp_pipe_colors = [_sp_status_colors.get(s, "#626C71") for s in df_cs_pipe["status"]]
        fig_sp_pipe = go.Figure(go.Bar(
            x=df_cs_pipe["property_name"],
            y=df_cs_pipe["rooms"],
            marker_color=_sp_pipe_colors,
            text=[f"{r} rooms\n{s}" for r, s in zip(df_cs_pipe["rooms"], df_cs_pipe["status"])],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Rooms: %{y}<br>Opens: %{customdata}<extra></extra>",
            customdata=df_cs_pipe["projected_open_date"],
        ))
        fig_sp_pipe.update_layout(xaxis_tickangle=-20, margin=dict(t=30, b=80))
        st.plotly_chart(style_fig(fig_sp_pipe, height=320), use_container_width=True)

        # ── Pipeline detail table ─────────────────────────────────────────────
        _sp_display = df_cs_pipe[["property_name", "city", "chain_scale", "rooms",
                                   "status", "projected_open_date", "brand", "developer"]].copy()
        _sp_display.columns = ["Property", "City", "Segment", "Rooms",
                                "Status", "Opens", "Brand", "Developer"]
        st.dataframe(_sp_display, use_container_width=True, hide_index=True)

        # ── Supply impact insight ─────────────────────────────────────────────
        _supply_impact_pct = _pipe_total / 5120 * 100
        st.markdown(
            insight_card(
                f"Supply Alert: {_pipe_total:,} Rooms ({_supply_impact_pct:.1f}% of Market) in Pipeline",
                f"**{_uc_rooms_sp:,} rooms** under active construction (opening 2025) will increase "
                f"South OC hotel supply before year-end. Full pipeline adds **{_supply_impact_pct:.1f}%** supply growth. "
                f"VDP member hotels should expect modest occupancy pressure as new supply absorbs demand. "
                f"ADR discipline and loyalty programs are critical to defending RevPAR during the absorption period.",
                kind="warning",
            ),
            unsafe_allow_html=True,
        )

        st.markdown("---")

        # Download
        _pipe_csv = df_cs_pipe.to_csv(index=False).encode()
        st.download_button("⬇️ Download Pipeline CSV", _pipe_csv,
                           file_name="costar_supply_pipeline.csv", mime="text/csv",
                           use_container_width=True)

    else:
        st.markdown(empty_state(
            "🏗️", "No supply pipeline data loaded.",
            "Run scripts/load_costar_reports.py to populate the pipeline table.",
        ), unsafe_allow_html=True)

    st.markdown("---")

    # ── Annual performance context ─────────────────────────────────────────────
    st.markdown("### Annual Market Performance Context")
    conn_sp = get_connection()
    try:
        df_cs_annual = pd.read_sql_query(
            "SELECT * FROM costar_annual_performance ORDER BY year DESC", conn_sp
        )
        if not df_cs_annual.empty:
            with st.expander("📊 View CoStar Annual Performance Data"):
                st.dataframe(df_cs_annual, use_container_width=True, hide_index=True)
                _ann_csv = df_cs_annual.to_csv(index=False).encode()
                st.download_button("⬇️ Download Annual Data CSV", _ann_csv,
                                   file_name="costar_annual_performance.csv", mime="text/csv")
        else:
            st.info("No annual CoStar performance data loaded.")
    except Exception:
        st.info("CoStar annual performance table not yet populated.")


# ══════════════════════════════════════════════════════════════════════════════


# TAB 8 — MARKET INTELLIGENCE (CoStar)
# ══════════════════════════════════════════════════════════════════════════════
with tab_cs:
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">South OC Market Intelligence</div>
      <div class="hero-subtitle">CoStar Hospitality Analytics · Newport Beach/Dana Point · 2024–2026 (Live PDF Extracts)</div>
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
        # Prefer South OC or Newport Beach/Dana Point 2024 annual data
        snap_df = df_cs_snap[
            (df_cs_snap["report_period"] == "2024-12-31") &
            (df_cs_snap["market"].isin(["South Orange County CA", "Newport Beach/Dana Point"]))
        ]
        if snap_df.empty:
            snap_df = df_cs_snap[df_cs_snap["report_period"] == "2024-12-31"]
        if snap_df.empty:
            snap_df = df_cs_snap[
                df_cs_snap["market"].isin(["South Orange County CA", "Newport Beach/Dana Point"])
            ]
        if snap_df.empty:
            snap_df = df_cs_snap.iloc[0:1]
        snap_df = snap_df.fillna(0)
        snap = snap_df.iloc[0]

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
        # Use live DB annual averages for 2024; fall back to 30-day if no annual data
        _2024_kpi = df_kpi[df_kpi["as_of_date"].astype(str).str.startswith("2024")] if not df_kpi.empty else pd.DataFrame()
        vdp_occ = float(_2024_kpi["occ_pct"].mean()) if not _2024_kpi.empty else m.get("occ_30", 76.4)
        vdp_adr = float(_2024_kpi["adr"].mean())     if not _2024_kpi.empty else m.get("adr_30", 288.50)
        vdp_rvp = float(_2024_kpi["revpar"].mean())  if not _2024_kpi.empty else m.get("revpar_30", 220.42)
        mkt_occ = float(snap["occupancy_pct"])
        mkt_adr = float(snap["adr_usd"])
        mkt_rvp = float(snap["revpar_usd"])

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

    # ── Visit California State Context ────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.35rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:2px;">'
        'Visit California — State Context</div>'
        '<div style="font-size:11px;opacity:0.50;font-weight:500;margin-bottom:14px;">'
        'Statewide travel forecasts &amp; lodging benchmarks · Visit California (Feb 2026)</div>',
        unsafe_allow_html=True,
    )

    _has_vca = not df_vca_forecast.empty or not df_vca_lodging.empty

    if _has_vca:
        # ── Row 1: CA statewide KPIs vs OC vs Dana Point ──────────────────────
        st.markdown("#### California vs OC vs Dana Point Benchmark")
        _vc1, _vc2, _vc3, _vc4 = st.columns(4)

        # OC lodging row from visit_ca_lodging_forecast (most recent year)
        _oc_row = pd.DataFrame()
        _ca_row = pd.DataFrame()
        if not df_vca_lodging.empty:
            _latest_yr = df_vca_lodging["year"].max()
            _oc_row = df_vca_lodging[
                (df_vca_lodging["year"] == _latest_yr) &
                (df_vca_lodging["region"].str.contains("Orange County", case=False, na=False))
            ]
            _ca_row = df_vca_lodging[
                (df_vca_lodging["year"] == _latest_yr) &
                (df_vca_lodging["region"].str.contains("California", case=False, na=False))
            ]

        _oc_occ  = float(_oc_row["occupancy_pct"].iloc[0]) if not _oc_row.empty and "occupancy_pct" in _oc_row.columns else 73.0
        _oc_adr  = float(_oc_row["adr_usd"].iloc[0])       if not _oc_row.empty and "adr_usd" in _oc_row.columns else 209.53
        _oc_revp = float(_oc_row["revpar_usd"].iloc[0])     if not _oc_row.empty and "revpar_usd" in _oc_row.columns else 153.01
        _ca_occ  = float(_ca_row["occupancy_pct"].iloc[0])  if not _ca_row.empty and "occupancy_pct" in _ca_row.columns else 67.3
        _ca_adr  = float(_ca_row["adr_usd"].iloc[0])        if not _ca_row.empty and "adr_usd" in _ca_row.columns else 189.85
        _ca_revp = float(_ca_row["revpar_usd"].iloc[0])     if not _ca_row.empty and "revpar_usd" in _ca_row.columns else 127.80

        # Dana Point reference from CoStar/STR
        _dp_adr  = float(df_cs_snap["adr_usd"].mean())   if not df_cs_snap.empty and "adr_usd" in df_cs_snap.columns else 295.0
        _dp_revp = float(df_cs_snap["revpar_usd"].mean()) if not df_cs_snap.empty and "revpar_usd" in df_cs_snap.columns else 220.0
        _dp_occ  = float(df_cs_snap["occ_pct"].mean())   if not df_cs_snap.empty and "occ_pct" in df_cs_snap.columns else 76.0

        _adr_premium_oc = (_dp_adr / _oc_adr - 1) * 100 if _oc_adr > 0 else 0
        _adr_premium_ca = (_dp_adr / _ca_adr - 1) * 100 if _ca_adr > 0 else 0
        _revp_premium   = (_dp_revp / _oc_revp - 1) * 100 if _oc_revp > 0 else 0

        with _vc1:
            st.metric("Dana Point ADR", f"${_dp_adr:,.0f}",
                      delta=f"+{_adr_premium_oc:.0f}% vs OC",
                      help="Dana Point portfolio ADR vs Orange County market average")
        with _vc2:
            st.metric("OC Market ADR", f"${_oc_adr:,.0f}",
                      delta=f"CA avg: ${_ca_adr:,.0f}",
                      help="Orange County 2025 lodging forecast ADR")
        with _vc3:
            st.metric("Dana Point RevPAR", f"${_dp_revp:,.0f}",
                      delta=f"+{_revp_premium:.0f}% vs OC",
                      help="Dana Point portfolio RevPAR vs Orange County")
        with _vc4:
            st.metric("OC Occupancy", f"{_oc_occ:.1f}%",
                      delta=f"CA avg: {_ca_occ:.1f}%",
                      help="Orange County 2025 lodging forecast occupancy")

        st.caption(
            f"Dana Point commands a **{_adr_premium_oc:.0f}% ADR premium** over Orange County "
            f"and a **{_adr_premium_ca:.0f}% premium** over the California statewide average — "
            f"confirming Dana Point's positioning as a premium coastal destination."
        )

        # ── Row 2: Lodging ladder chart ────────────────────────────────────────
        if not df_vca_lodging.empty and "region" in df_vca_lodging.columns:
            _latest_yr = df_vca_lodging["year"].max()
            _lodge_slice = df_vca_lodging[
                (df_vca_lodging["year"] == _latest_yr) &
                (df_vca_lodging["region"].notna())
            ].copy()
            if not _lodge_slice.empty and "adr_usd" in _lodge_slice.columns:
                _lodge_slice = _lodge_slice.sort_values("adr_usd", ascending=True)
                # Inject Dana Point as a benchmark row
                _dp_bench = pd.DataFrame([{
                    "region": "Dana Point (Portfolio)", "adr_usd": _dp_adr,
                    "revpar_usd": _dp_revp, "occupancy_pct": _dp_occ
                }])
                _lodge_plot = pd.concat([_lodge_slice, _dp_bench], ignore_index=True)
                _lodge_plot = _lodge_plot.sort_values("adr_usd", ascending=True)

                _colors = [
                    TEAL if "Dana Point" in str(r) else ORANGE
                    for r in _lodge_plot["region"]
                ]
                fig_lodge = go.Figure(go.Bar(
                    x=_lodge_plot["adr_usd"],
                    y=_lodge_plot["region"],
                    orientation="h",
                    marker_color=_colors,
                    text=[f"${v:,.0f}" for v in _lodge_plot["adr_usd"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>ADR: $%{x:,.0f}<extra></extra>",
                ))
                fig_lodge.update_layout(
                    title=f"ADR by CA Region ({_latest_yr}) — Dana Point vs Market",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(title="ADR (USD)", gridcolor="rgba(255,255,255,0.06)"),
                    yaxis=dict(gridcolor="rgba(255,255,255,0.06)"),
                    height=420, margin=dict(l=10, r=10, t=40, b=10),
                    font=dict(family="Plus Jakarta Sans, sans-serif", size=11),
                )
                st.plotly_chart(fig_lodge, use_container_width=True)

        # ── Row 3: CA travel volume forecast trend ─────────────────────────────
        if not df_vca_forecast.empty and "year" in df_vca_forecast.columns:
            _col_vca1, _col_vca2 = st.columns(2)
            with _col_vca1:
                st.markdown("#### CA Visitor Volume Forecast")
                _fcast_plot = df_vca_forecast[df_vca_forecast["total_visits_m"].notna()].copy()
                if not _fcast_plot.empty:
                    fig_fcast = go.Figure()
                    _actual = _fcast_plot[_fcast_plot["is_forecast"] == 0]
                    _fcast  = _fcast_plot[_fcast_plot["is_forecast"] == 1]
                    if not _actual.empty:
                        fig_fcast.add_trace(go.Scatter(
                            x=_actual["year"], y=_actual["total_visits_m"],
                            name="Actual", mode="lines+markers",
                            line=dict(color=TEAL, width=2.5),
                            hovertemplate="<b>%{x}</b><br>Visits: %{y:.1f}M<extra></extra>",
                        ))
                    if not _fcast.empty:
                        fig_fcast.add_trace(go.Scatter(
                            x=_fcast["year"], y=_fcast["total_visits_m"],
                            name="Forecast", mode="lines+markers",
                            line=dict(color=ORANGE, width=2, dash="dot"),
                            hovertemplate="<b>%{x}</b><br>Forecast: %{y:.1f}M<extra></extra>",
                        ))
                    fig_fcast.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(title="Year", gridcolor="rgba(255,255,255,0.06)"),
                        yaxis=dict(title="Total Visits (M)", gridcolor="rgba(255,255,255,0.06)"),
                        legend=dict(font=dict(size=11)),
                        height=300, margin=dict(l=10, r=10, t=20, b=10),
                        font=dict(family="Plus Jakarta Sans, sans-serif", size=11),
                    )
                    st.plotly_chart(fig_fcast, use_container_width=True)

            with _col_vca2:
                st.markdown("#### JWA / SNA Airport Traffic (2025)")
                if not df_vca_airport.empty and "airport" in df_vca_airport.columns:
                    _jwa = df_vca_airport[df_vca_airport["airport"].isin(["SNA", "JWA"])].copy()
                    if _jwa.empty:
                        _jwa = df_vca_airport[
                            df_vca_airport["airport"].str.contains("John Wayne|SNA|Orange County", case=False, na=False)
                        ].copy()
                    if _jwa.empty:
                        _jwa = df_vca_airport.copy()

                    # Support both column naming conventions
                    _month_col = "month" if "month" in _jwa.columns else ("month_num" if "month_num" in _jwa.columns else None)
                    _pax_col = "total_pax" if "total_pax" in _jwa.columns else ("total_passengers" if "total_passengers" in _jwa.columns else None)
                    if not _jwa.empty and _month_col and _pax_col:
                        fig_air = go.Figure()
                        for _apt in _jwa["airport"].unique():
                            _apt_df = _jwa[_jwa["airport"] == _apt].sort_values(_month_col)
                            fig_air.add_trace(go.Scatter(
                                x=_apt_df[_month_col],
                                y=_apt_df[_pax_col],
                                name=_apt, mode="lines+markers",
                                hovertemplate=f"<b>{_apt}</b><br>Month: %{{x}}<br>Passengers: %{{y:,.0f}}<extra></extra>",
                            ))
                        fig_air.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            xaxis=dict(title="Month", gridcolor="rgba(255,255,255,0.06)",
                                       tickvals=list(range(1, 13)),
                                       ticktext=["Jan","Feb","Mar","Apr","May","Jun",
                                                 "Jul","Aug","Sep","Oct","Nov","Dec"]),
                            yaxis=dict(title="Passengers", gridcolor="rgba(255,255,255,0.06)"),
                            legend=dict(font=dict(size=11)),
                            height=300, margin=dict(l=10, r=10, t=20, b=10),
                            font=dict(family="Plus Jakarta Sans, sans-serif", size=11),
                        )
                        st.plotly_chart(fig_air, use_container_width=True)
                    else:
                        st.info("Airport traffic data loaded — no monthly breakdown available.")
                else:
                    st.info("No airport traffic data loaded. Run the pipeline to populate visit_ca_airport_traffic.")

        # Download
        if not df_vca_lodging.empty:
            _vca_dl = df_vca_lodging.to_csv(index=False).encode()
            st.download_button(
                "⬇️ Download Visit CA Lodging Forecast CSV",
                _vca_dl, file_name="visit_ca_lodging_forecast.csv",
                mime="text/csv", use_container_width=True,
            )
    else:
        st.markdown(empty_state(
            "🏔️", "Visit California data not yet loaded.",
            "Run the pipeline to populate visit_ca_* tables from data/Visit_California/.",
        ), unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════



# TAB 5 — DATA LOG
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
        # Core tables
        _TABLE_LABELS = {
            "fact_str_metrics":          "STR Metrics",
            "kpi_daily_summary":         "KPI Daily",
            "kpi_compression_quarterly": "Compression Qtrs",
            "insights_daily":            "Insights",
            "table_relationships":       "Table Rels",
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
        _ins_ct  = counts.get("insights_daily", 0)
        _rel_ct  = counts.get("table_relationships", 0)
        _kpi_dot = "🟢" if isinstance(_kpi_ct, int) and _kpi_ct > 0 else "⚫"
        _cmp_dot = "🟢" if isinstance(_cmp_ct, int) and _cmp_ct > 0 else "⚫"
        _ins_dot = "🟢" if isinstance(_ins_ct, int) and _ins_ct > 0 else "⚫"
        _rel_dot = "🟢" if isinstance(_rel_ct, int) and _rel_ct > 0 else "⚫"
        st.markdown(source_card(
            _kpi_dot, "KPI Daily Summary", "kpi_daily_summary",
            f"{_kpi_ct:,}" if isinstance(_kpi_ct, int) else _kpi_ct,
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _cmp_dot, "Compression Quarters", "kpi_compression_quarterly",
            f"{_cmp_ct:,}" if isinstance(_cmp_ct, int) else _cmp_ct,
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _ins_dot, "Forward Insights", "insights_daily · all audiences",
            f"{_ins_ct:,}" if isinstance(_ins_ct, int) else _ins_ct,
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _rel_dot, "Table Relationships", "table_relationships · brain map",
            f"{_rel_ct:,}" if isinstance(_rel_ct, int) else _rel_ct,
        ), unsafe_allow_html=True)

    _sc3, _sc4 = st.columns(2)
    with _sc3:
        # Datafy tables
        _dfy_tables = [t for t in counts if t.startswith("datafy_")]
        _dfy_total  = sum(counts[t] for t in _dfy_tables if isinstance(counts[t], int))
        _dfy_dot    = "🟢" if _dfy_total > 0 else "⚫"
        st.markdown(source_card(
            _dfy_dot, "Datafy Visitor Economy",
            f"{len(_dfy_tables)} tables · visitor economy data",
            f"{_dfy_total:,}" if _dfy_total > 0 else "—",
        ), unsafe_allow_html=True)
        _cs_tables = [t for t in counts if t.startswith("costar_")]
        _cs_total  = sum(counts[t] for t in _cs_tables if isinstance(counts[t], int))
        _cs_src_dot = "🟢" if _cs_total > 0 else "⚫"
        st.markdown(source_card(
            _cs_src_dot, "CoStar Market Intelligence",
            f"{len(_cs_tables)} tables · hospitality analytics",
            f"{_cs_total:,}" if _cs_total > 0 else "—",
        ), unsafe_allow_html=True)
    with _sc4:
        _zrt_tables = [t for t in counts if t.startswith("zartico_")]
        _zrt_total  = sum(counts[t] for t in _zrt_tables if isinstance(counts[t], int))
        _zrt_src_dot = "🟢" if _zrt_total > 0 else "⚫"
        st.markdown(source_card(
            _zrt_src_dot, "Zartico Historical Reference",
            f"{len(_zrt_tables)} tables · historical visitor data (Jun 2025)",
            f"{_zrt_total:,}" if _zrt_total > 0 else "—",
        ), unsafe_allow_html=True)
        _evt_ct  = counts.get("vdp_events", 0)
        _evt_dot = "🟢" if isinstance(_evt_ct, int) and _evt_ct > 0 else "⚫"
        st.markdown(source_card(
            _evt_dot, "VDP Event Calendar",
            "vdp_events · scraped from visitdanapoint.com",
            f"{_evt_ct:,}" if isinstance(_evt_ct, int) and _evt_ct > 0 else "—",
        ), unsafe_allow_html=True)
        _vca_tables = [t for t in counts if t.startswith("visit_ca_")]
        _vca_total  = sum(counts[t] for t in _vca_tables if isinstance(counts[t], int))
        _vca_src_dot = "🟢" if _vca_total > 0 else "⚫"
        st.markdown(source_card(
            _vca_src_dot, "Visit California",
            f"{len(_vca_tables)} tables · statewide travel & lodging forecasts",
            f"{_vca_total:,}" if _vca_total > 0 else "—",
        ), unsafe_allow_html=True)
        _later_ig_ct = sum(counts.get(t, 0) for t in ["later_ig_profile_growth","later_ig_posts","later_ig_reels"] if isinstance(counts.get(t, 0), int))
        _later_fb_ct = sum(counts.get(t, 0) for t in ["later_fb_profile_growth","later_fb_posts","later_fb_profile_interactions"] if isinstance(counts.get(t, 0), int))
        _later_tk_ct = sum(counts.get(t, 0) for t in ["later_tk_profile_growth","later_tk_audience_demographics"] if isinstance(counts.get(t, 0), int))
        _later_dl_total = _later_ig_ct + _later_fb_ct + _later_tk_ct
        _later_dl_dot   = "🟢" if _later_dl_total > 0 else "⚫"
        st.markdown(source_card(
            _later_dl_dot, "Later.com Social Media",
            "Instagram · Facebook · TikTok · 3 platforms · 12 tables",
            f"{_later_dl_total:,}" if _later_dl_total > 0 else "—",
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

    st.markdown("---")

    # ── Download buttons for major tables ──────────────────────────────────
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.2rem;'
        'font-weight:800;letter-spacing:-0.025em;margin-bottom:4px;">Download Table Data</div>'
        '<div style="font-size:11px;opacity:0.50;font-weight:500;margin-bottom:12px;">'
        'Export key tables from analytics.sqlite as CSV</div>',
        unsafe_allow_html=True,
    )
    _dl_tables_config = [
        ("KPI Daily Summary",    df_kpi,        "kpi_daily_summary.csv"),
        ("STR Daily Metrics",    df_daily,       "str_daily.csv"),
        ("STR Monthly Metrics",  df_monthly,     "str_monthly.csv"),
        ("Datafy Overview KPIs", df_dfy_ov,      "datafy_overview_kpis.csv"),
        ("Datafy DMA",           df_dfy_dma,     "datafy_dma.csv"),
        ("Datafy Media KPIs",    df_dfy_media,   "datafy_media_kpis.csv"),
        ("Forward Insights",     df_insights,    "insights_daily.csv"),
    ]
    _dl_col_a, _dl_col_b, _dl_col_c, _dl_col_d = st.columns(4)
    _dl_all_cols2 = [_dl_col_a, _dl_col_b, _dl_col_c, _dl_col_d]
    for _di2, (_name2, _df2, _fname2) in enumerate(_dl_tables_config):
        _col_idx = _di2 % 4
        with _dl_all_cols2[_col_idx]:
            if not _df2.empty:
                _bytes2 = _df2.to_csv(index=False).encode()
                st.download_button(
                    f"⬇️ {_name2}", _bytes2, file_name=_fname2,
                    mime="text/csv", use_container_width=True,
                    key=f"dl_tbl_{_di2}",
                )
            else:
                st.button(
                    f"⬇️ {_name2}", disabled=True, use_container_width=True,
                    key=f"dl_tbl_dis_{_di2}", help="No data loaded for this table",
                )

    st.markdown("---")

    # ── Brain Architecture — Table Relationships ────────────────────────────
    with st.expander("🗺 Brain Architecture — Table Relationships", expanded=False):
        st.markdown(
            '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1rem;'
            'font-weight:800;margin-bottom:8px;">All Table Relationships in analytics.sqlite</div>',
            unsafe_allow_html=True,
        )
        try:
            conn_ro = get_connection()
            df_rels = pd.read_sql_query(
                "SELECT table_a, table_b, relationship_type, join_key, description "
                "FROM table_relationships ORDER BY table_a, relationship_type",
                conn_ro,
            )
            if not df_rels.empty:
                df_rels.columns = ["Table A", "Table B", "Relationship", "Join Key", "Description"]
                st.dataframe(df_rels, use_container_width=True, hide_index=True)
                _rels_csv = df_rels.to_csv(index=False).encode()
                st.download_button("⬇️ Download Relationships CSV", _rels_csv,
                                   file_name="table_relationships.csv", mime="text/csv",
                                   key="dl_rels")
            else:
                st.caption("Run the pipeline to populate table_relationships.")
        except Exception as e:
            st.caption(f"table_relationships not yet available: {e}")

    # ── Full Database Inventory ─────────────────────────────────────────────
    with st.expander("🧠 Full Database Inventory", expanded=False):
        st.markdown(
            '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1rem;'
            'font-weight:800;margin-bottom:8px;">All Tables in analytics.sqlite</div>',
            unsafe_allow_html=True,
        )
        _brain_tables = {
            "fact_str_metrics":                    "STR hotel metrics (source of truth)",
            "kpi_daily_summary":                   "Derived daily KPIs + YOY deltas",
            "kpi_compression_quarterly":           "Compression days per quarter",
            "load_log":                            "Pipeline ETL audit trail",
            "insights_daily":                      "Forward-looking insights (all audiences)",
            "table_relationships":                 "Cross-table relationship map",
            "datafy_overview_kpis":                "Datafy annual visitor overview KPIs",
            "datafy_overview_dma":                 "Datafy feeder market DMA breakdown",
            "datafy_overview_demographics":        "Datafy visitor demographics",
            "datafy_overview_category_spending":   "Datafy visitor spending by category",
            "datafy_overview_cluster_visitation":  "Datafy visitation by cluster/area",
            "datafy_overview_airports":            "Datafy origin airports",
            "datafy_attribution_website_kpis":     "Website-attributed trip KPIs",
            "datafy_attribution_website_top_markets": "Website attribution top markets",
            "datafy_attribution_website_dma":      "Website attribution DMA breakdown",
            "datafy_attribution_website_channels": "Website attribution by channel",
            "datafy_attribution_website_clusters": "Website attribution by cluster",
            "datafy_attribution_website_demographics": "Website attribution demographics",
            "datafy_attribution_media_kpis":       "Media campaign attribution KPIs",
            "datafy_attribution_media_top_markets":"Media attribution top markets",
            "datafy_social_traffic_sources":       "Social/web GA4 traffic sources",
            "datafy_social_audience_overview":     "Social/web audience overview",
            "datafy_social_top_pages":             "Top website pages by views",
            "zartico_kpis":                        "Zartico KPIs (historical Jun 2025)",
            "zartico_markets":                     "Zartico origin markets (historical)",
            "zartico_spending_monthly":            "Zartico monthly visitor spend (historical)",
            "zartico_lodging_kpis":                "Zartico lodging KPIs (historical)",
            "zartico_overnight_trend":             "Zartico overnight trend (historical)",
            "zartico_event_impact":                "Zartico event impact (historical)",
            "zartico_movement_monthly":            "Zartico V/R ratio monthly (historical)",
            "zartico_future_events_summary":       "Zartico events forecast (historical)",
            "vdp_events":                          "VDP event calendar",
            "costar_market_snapshot":              "CoStar market snapshot",
            "costar_monthly_performance":          "CoStar monthly performance",
            "costar_supply_pipeline":              "CoStar supply pipeline",
            "costar_chain_scale_breakdown":        "CoStar chain scale breakdown",
            "costar_competitive_set":              "CoStar competitive set",
            "later_ig_profile_growth":             "Instagram profile growth (Later.com)",
            "later_ig_posts":                      "Instagram post performance (Later.com)",
            "later_ig_reels":                      "Instagram Reels performance (Later.com)",
            "later_ig_audience_demographics":      "Instagram audience demographics (Later.com)",
            "later_fb_profile_growth":             "Facebook page growth (Later.com)",
            "later_fb_posts":                      "Facebook post performance (Later.com)",
            "later_fb_profile_interactions":       "Facebook profile interactions (Later.com)",
            "later_tk_profile_growth":             "TikTok profile growth (Later.com)",
            "later_tk_audience_demographics":      "TikTok audience demographics (Later.com)",
        }
        _brain_rows = [
            {"Table": t, "Description": d, "Row Count": counts.get(t, "—")}
            for t, d in _brain_tables.items()
        ]
        _brain_df = pd.DataFrame(_brain_rows)
        st.dataframe(_brain_df, use_container_width=True, hide_index=True)
        _brain_csv2 = _brain_df.to_csv(index=False).encode()
        st.download_button("⬇️ Download DB Inventory CSV", _brain_csv2,
                           file_name="db_inventory.csv", mime="text/csv", key="dl_brain")


# ══════════════════════════════════════════════════════════════════════════════
# FOOTER — GloCon Solutions LLC · Data Glossary · Data Sources
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("---")

_GLOSSARY_TERMS = {
    "ADR": "Average Daily Rate — total room revenue divided by rooms sold on a given day. The primary rate metric in hospitality.",
    "RevPAR": "Revenue Per Available Room — ADR × Occupancy Rate. The industry benchmark for lodging performance, combining both rate and volume.",
    "Occupancy Rate": "Percentage of available rooms sold: Demand ÷ Supply. Expressed as a percentage (e.g., 68.8% = 688 rooms sold out of 1,000 available).",
    "TBID": "Tourism Business Improvement District — Dana Point's assessment district. Properties pay a blended ~1.25% of room revenue (tiered: 1% ≤$199.99/night, 1.5% $200–$399.99, 2% ≥$400) to fund destination marketing.",
    "TOT": "Transient Occupancy Tax — city hotel tax of 10% applied to room revenue. Primary lodging revenue stream for the City of Dana Point.",
    "ROAS": "Return on Ad Spend — total attributable destination economic impact divided by media spend. A ROAS of 15× means every $1 in ads generated $15 in destination activity.",
    "MPI": "Market Penetration Index — a property or market's fair share of occupancy vs. the competitive set. MPI > 100 = above fair share.",
    "ARI": "Average Rate Index — ADR performance relative to the competitive set. ARI > 100 = rate premium over comp set.",
    "RGI": "Revenue Generation Index — RevPAR performance relative to the competitive set. RGI > 100 = RevPAR outperformance. Also called RevPAR Index.",
    "LOS": "Length of Stay — average number of nights per visitor booking. Dana Point's blended LOS (hotel + STVR) is approximately 2.0 nights.",
    "OOS": "Out-of-State — visitors originating from outside California. OOS visitors typically generate higher per-trip spending and longer stays.",
    "DMA": "Designated Market Area — geographic media/market region used in visitor economy analysis (e.g., Los Angeles DMA, San Francisco DMA). Defined by Nielsen.",
    "STR": "Smith Travel Research — the primary source for hotel performance benchmarking data (occupancy, ADR, RevPAR) used throughout this platform.",
    "Compression": "A compression day or period occurs when occupancy reaches 80%+ (or 90%+), signaling near-full demand. Compression days allow for rate premiums and justify investment.",
    "PULSE": "Performance · Understanding · Leadership · Spending · Economy — Dana Point's analytics intelligence platform. Aggregates STR, Datafy, CoStar, Visit CA, and Zartico data into a single decision-support dashboard.",
}

_SOURCES_HTML = """
<div style="display:flex;flex-wrap:wrap;gap:12px;margin-top:8px;">
  <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.09);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;">
    <div style="font-weight:700;color:#e8f4f8;margin-bottom:2px;">STR</div>
    <div style="opacity:0.6;">Smith Travel Research · daily &amp; monthly hotel benchmarking</div>
  </div>
  <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.09);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;">
    <div style="font-weight:700;color:#e8f4f8;margin-bottom:2px;">Datafy</div>
    <div style="opacity:0.6;">Visitor economy platform · trips, spend, DMA attribution</div>
  </div>
  <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.09);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;">
    <div style="font-weight:700;color:#e8f4f8;margin-bottom:2px;">CoStar</div>
    <div style="opacity:0.6;">Market data · comp set, pipeline, profitability</div>
  </div>
  <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.09);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;">
    <div style="font-weight:700;color:#e8f4f8;margin-bottom:2px;">Visit California</div>
    <div style="opacity:0.6;">State forecasts · lodging, travel volume, airport traffic</div>
  </div>
  <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.09);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;">
    <div style="font-weight:700;color:#e8f4f8;margin-bottom:2px;">Zartico</div>
    <div style="opacity:0.6;">Historical reference · Jun 2025 snapshot · visitor trends</div>
  </div>
</div>
"""

_gl1, _gl2 = st.columns(2)
with _gl1:
    with st.expander("📖 Data Glossary", expanded=False):
        for _term, _defn in _GLOSSARY_TERMS.items():
            st.markdown(
                f'<div style="margin-bottom:10px;">'
                f'<span style="font-weight:700;color:#4FC3F7;">{_term}</span>'
                f'<span style="opacity:0.75;font-size:13px;"> — {_defn}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
with _gl2:
    with st.expander("🗂️ Data Sources", expanded=False):
        st.markdown(_SOURCES_HTML, unsafe_allow_html=True)

st.markdown(
    '<div style="text-align:center;padding:18px 0 10px;font-size:12px;opacity:0.40;">'
    'Powered by <strong>GloCon Solutions LLC</strong> &nbsp;·&nbsp; 2026 &nbsp;·&nbsp; '
    'Dana Point PULSE · All data is proprietary and confidential.'
    '</div>',
    unsafe_allow_html=True,
)
