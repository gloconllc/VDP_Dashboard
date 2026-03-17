
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
    background: rgba(33,128,141,0.06);
    border: 1px solid rgba(33,128,141,0.18);
    border-left: 4px solid #21808D;
    border-radius: 14px; padding: 18px 22px; margin-bottom: 14px;
    position: relative;
  }
  .nlm-briefing-title {
    font-family: 'Plus Jakarta Sans', sans-serif;
    font-size: 10px; font-weight: 800; text-transform: uppercase;
    letter-spacing: .10em; color: #21808D; margin-bottom: 14px;
    display: flex; align-items: center; gap: 8px;
  }
  .nlm-point {
    font-size: 13px; line-height: 1.65; margin-bottom: 10px;
    padding-left: 16px; position: relative;
  }
  .nlm-point::before {
    content: '▸'; position: absolute; left: 0;
    color: #21808D; font-size: 11px; top: 2px;
  }
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
# Datafy visitor economy
df_dfy_ov      = load_datafy_overview()
df_dfy_dma     = load_datafy_dma()
df_dfy_spend   = load_datafy_spending()
df_dfy_demo    = load_datafy_demographics()
df_dfy_air     = load_datafy_airports()
df_dfy_media   = load_datafy_media_kpis()
df_dfy_web     = load_datafy_website_kpis()
df_dfy_mktmkt  = load_datafy_media_markets()
df_insights    = load_insights()          # Forward-looking insights (all audiences)

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
    # Use the already-loaded df_cs_snap as the authoritative CoStar row count —
    # counts dict can return "—" (string) on exceptions, making integer checks unreliable.
    _cs_rows = len(df_cs_snap) if not df_cs_snap.empty else 0
    _cs_dot  = "🟢" if _cs_rows > 0 else "⚫"
    _cs_label = f"{_cs_rows:,} rows" if _cs_rows > 0 else "No data"
    _dfy_rows = counts.get("datafy_overview_kpis", 0)
    _dfy_dot  = "🟢" if isinstance(_dfy_rows, int) and _dfy_rows > 0 else "⚫"
    _dfy_label = f"{_dfy_rows:,} report(s)" if isinstance(_dfy_rows, int) and _dfy_rows > 0 else "No data"
    st.markdown("**Pipeline Status**")
    st.markdown(f"{_d_dot} STR Daily &nbsp;·&nbsp; {_d_label}")
    st.markdown(f"{_m_dot} STR Monthly &nbsp;·&nbsp; {_m_label}")
    st.markdown(f"{_cs_dot} CoStar Market &nbsp;·&nbsp; {_cs_label}")
    st.markdown(f"{_dfy_dot} Datafy &nbsp;·&nbsp; {_dfy_label}")
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
    font-size: 11pt; color: #202124; background: #f8f9fa; line-height: 1.6;
  }}
  a {{ color: #21808D; text-decoration: none; }}

  /* Layout */
  .page {{ max-width: 920px; margin: 0 auto; padding: 32px 40px; }}
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
    background: white; border: 1px solid #e8eaed;
    border-radius: 12px; padding: 16px 18px; position: relative; overflow: hidden;
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

tab_ov, tab_tr, tab_fo, tab_ev, tab_cs, tab_dl = st.tabs(
    ["Overview Brain", "STR & Pipeline", "Forward Outlook", "Visitor Economy", "Market Intelligence", "Data & Downloads"]
)

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

            # Source badge row
            _src_row = (
                '<span class="nlm-tag nlm-tag-str">STR</span>'
                + (' <span class="nlm-tag nlm-tag-datafy">Datafy</span>' if _trips_m > 0 else '')
                + ' <span class="nlm-tag nlm-tag-ai">AI Insights</span>'
            )
            _midweek_opp_lbl = f"${(_wknd - _wkdy) * 0.2 * 90 / 7 * 12:,.0f}/year"
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
  <strong>Weekend / Midweek Gap</strong> <span class="nlm-tag nlm-tag-str">STR</span><br>
  Weekend RevPAR: <strong>${_wknd:.0f}</strong> · Midweek: <strong>${_wkdy:.0f}</strong> · Gap: <strong>{_gap:.0f}%</strong>.
  <br><em style="opacity:.72">→ Closing 20% of this gap adds ~{_midweek_opp_lbl} in incremental portfolio room revenue.</em>
</div>

<div class="nlm-point">
  <strong>Market Positioning</strong> <span class="nlm-tag nlm-tag-ai">CoStar</span><br>
  Dana Point/South OC market ADR forecast: $285+ through 2025. VDP portfolio maintains premium positioning above market average.
  <br><em style="opacity:.72">→ Present updated comp set analysis at next board meeting; request approval for rate strategy review.</em>
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

    st.markdown("---")

    # ── AI Analyst Panel ───────────────────────────────────────────────────────
    with st.expander("🧠 VDP Intelligence — Interrogate your data", expanded=False):
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
            st.markdown('<div class="chart-header">Seasonal Demand Rose — Monthly RevPAR Compass</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Petal length = avg RevPAR · longer petals = stronger months · reveals true seasonality shape</div>', unsafe_allow_html=True)
            if len(monthly) >= 6:
                month_order_full = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
                _tmp_rose = df_monthly.copy()
                _tmp_rose["mon_num"] = _tmp_rose["as_of_date"].dt.month
                _tmp_rose["mon_lbl"] = _tmp_rose["as_of_date"].dt.strftime("%b")
                rose_avg = _tmp_rose.groupby("mon_num")["revpar"].mean().reindex(range(1, 13))
                rose_avg = rose_avg.fillna(0)
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
                    hovertemplate="<b>%{theta}</b><br>Avg RevPAR: $%{r:.0f}<extra></extra>",
                ))
                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            tickprefix="$",
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
                st.caption(f"Peak: {_peak_mon} (${rose_avg.max():.0f} RevPAR) · Softest: {_soft_mon} (${rose_avg.min():.0f}). "
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

        # ── Beeswarm: Daily RevPAR Distribution ───────────────────────────────
        if not df_daily.empty and len(df_daily) >= 30:
            st.markdown("---")
            st.markdown('<div class="chart-header">Daily RevPAR Distribution — Beeswarm</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-caption">Each dot = one day · spread by RevPAR value · '
                'color = quarter · reveals compression clusters and soft-period gaps</div>',
                unsafe_allow_html=True,
            )
            _bsw = df_daily.copy().tail(365)
            _bsw["quarter"] = _bsw["as_of_date"].dt.quarter
            _bsw["year"] = _bsw["as_of_date"].dt.year
            _q_colors = {1: "#A7D5D9", 2: TEAL_LIGHT, 3: TEAL, 4: "#1A6470"}
            _bsw["color"] = _bsw["quarter"].map(_q_colors)
            # Jitter y-axis to create beeswarm effect
            np.random.seed(42)
            _bsw["jitter"] = np.random.uniform(-0.4, 0.4, len(_bsw))
            fig = go.Figure()
            for q in [1, 2, 3, 4]:
                _sub = _bsw[_bsw["quarter"] == q]
                if _sub.empty:
                    continue
                fig.add_trace(go.Scatter(
                    x=_sub["revpar"],
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
                        "<b>%{customdata[0]|%b %d, %Y}</b><br>"
                        "RevPAR: $%{x:.0f}<br>Q%{customdata[1]}<extra></extra>"
                    ),
                    customdata=_sub[["as_of_date", "quarter"]].values,
                ))
            _bsw_avg = _bsw["revpar"].mean()
            fig.add_vline(x=_bsw_avg, line_dash="dash",
                          line_color="rgba(167,169,169,0.6)",
                          annotation_text=f"Avg ${_bsw_avg:.0f}",
                          annotation_position="top")
            fig.update_layout(
                yaxis=dict(visible=False, range=[-1, 1]),
                xaxis=dict(title="RevPAR ($)", tickprefix="$"),
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(style_fig(fig, height=280), use_container_width=True)
            st.caption("Density clusters reveal seasonal compression. Q3 (dark teal) dots pushed right = peak pricing power. Spread = rate variability risk.")

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

    st.markdown("---")

    # ── All-table relationship map ───────────────────────────────────────────
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
            else:
                st.caption("Run the pipeline to populate table_relationships.")
        except Exception as e:
            st.caption(f"table_relationships not yet available: {e}")

    # ── DB brain snapshot ────────────────────────────────────────────────────
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
        }
        _brain_rows = [
            {"Table": t, "Description": d, "Row Count": counts.get(t, "—")}
            for t, d in _brain_tables.items()
        ]
        st.dataframe(pd.DataFrame(_brain_rows), use_container_width=True, hide_index=True)


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
                    legend=dict(font_size=10, orientation="v",
                                font=dict(family="Plus Jakarta Sans, Inter, sans-serif")),
                    annotations=[dict(text="Spend<br>Mix", x=0.5, y=0.5, font_size=13,
                                      font_family="Plus Jakarta Sans, sans-serif",
                                      font_color="#21808D", showarrow=False)],
                )
                st.plotly_chart(style_fig(fig, height=360), use_container_width=True)
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

# ══════════════════════════════════════════════════════════════════════════════


# TAB 4 — MARKET INTELLIGENCE (CoStar)
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
        snap_df = df_cs_snap[df_cs_snap["report_period"] == "2024-12-31"]
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
        st.markdown(source_card(
            "⚫", "CoStar", "source=costar · pending export", "—",
        ), unsafe_allow_html=True)
    with _sc4:
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
