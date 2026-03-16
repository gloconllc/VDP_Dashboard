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
def get_table_counts() -> dict:
    conn = get_connection()
    counts = {}
    all_tables = [
        "fact_str_metrics", "kpi_daily_summary", "kpi_compression_quarterly",
        "load_log", "insights_daily", "table_relationships",
        "datafy_overview_kpis", "datafy_overview_dma", "datafy_overview_demographics",
        "datafy_overview_category_spending", "datafy_overview_cluster_visitation",
        "datafy_overview_airports",
        "datafy_attribution_website_kpis", "datafy_attribution_website_top_markets",
        "datafy_attribution_website_dma", "datafy_attribution_website_channels",
        "datafy_attribution_website_clusters", "datafy_attribution_website_demographics",
        "datafy_attribution_media_kpis", "datafy_attribution_media_top_markets",
        "datafy_social_traffic_sources", "datafy_social_audience_overview",
        "datafy_social_top_pages",
    ]
    for t in all_tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM \"{t}\"").fetchone()
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
            "Ohana Fest 2025 — Datafy visitor economy data for Dana Point:\n"
            "• Event expenditure: $14.6M  |  Total destination spend: $18.4M\n"
            "• ADR lift: +$139 vs. baseline ($542 vs. $403)\n"
            "• RevPAR lift: +$140 ($427 vs. $287 baseline)\n"
            "• Overnight hotel visitors: 24% of total attendees\n"
            "• Avg accommodation spend/trip: $1,219 (+53% vs. 2024)\n"
            "• Out-of-state visitors: 68%  |  Spend multiplier: 3.2×\n\n"
            f"Current portfolio context: {b}\n\n"
            "Generate a concise board-ready ROI analysis of Ohana Fest and provide 2 specific "
            "recommendations to maximize hotel revenue impact for future events."
        ),
        "board": (
            f"{b}\n"
            f"• Est. TBID monthly revenue: ${m.get('tbid_monthly',0):,.0f} (blended 1.25%)\n"
            "• Ohana Fest destination spend: $18.4M\n\n"
            "Generate 5 concise talking points for the VDP TBID board meeting. "
            "Each point = one sentence with a specific data reference. Format for an executive audience."
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
            f"3. **Ohana Fest ROI:** $18.4M destination spend, 3.2× multiplier, "
            f"68% out-of-state visitors\n"
            f"4. **TBID Revenue:** Tracking ~**${m.get('tbid_monthly',0):,.0f}/month** "
            f"at blended 1.25%\n"
            f"5. **Midweek Opportunity:** Weekend/midweek RevPAR gap "
            f"({wknd_pct:.0f}%) = highest-leverage growth lever\n\n"
            f"**→ Action:** Request board approval for a $50K midweek demand-generation campaign."
        ),
        "ohana": (
            f"**Ohana Fest Impact** *(local mode)*\n\n"
            f"• Event expenditure: **$14.6M** | Destination spend: **$18.4M**\n"
            f"• ADR lift vs. baseline: **+$139** (+45%) on event nights\n"
            f"• RevPAR lift: **+$140** ($427 vs. $287 baseline)\n"
            f"• 68% out-of-state visitors = genuine incremental tourism dollars\n"
            f"• Avg accommodation spend: **$1,219/trip** (+53% vs. 2024)\n\n"
            f"**ROI:** 3.2× spend multiplier · 5-day post-event ADR halo: +$22 above baseline\n\n"
            f"**→ Action:** Negotiate multi-year Ohana Fest partnership with preferred "
            f"hotel rate agreements to capture accommodation spend upstream."
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
df_daily   = load_str_daily()    # source='STR', grain='daily'
df_monthly = load_str_monthly()  # source='STR', grain='monthly'
df_kpi     = load_kpi_daily()
df_comp    = load_compression()
df_log     = load_load_log()

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
    st.markdown("**Pipeline Status**")
    st.markdown(f"{_d_dot} STR Daily &nbsp;·&nbsp; {_d_label}")
    st.markdown(f"{_m_dot} STR Monthly &nbsp;·&nbsp; {_m_label}")
    st.markdown(f"⚫ Datafy &nbsp;·&nbsp; Not connected")
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

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_ov, tab_tr, tab_fo, tab_ev, tab_dl = st.tabs(
    ["📊 Overview", "📈 Trends", "🔭 Forward Outlook", "🎪 Event Impact", "🗂 Data Log"]
)

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
            ("🎵 Ohana Fest Impact",    "ohana"),
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
        "dmo":      ("🏢 DMO / TBID Board",    TEAL,       "Destination Marketing & Revenue Strategy"),
        "city":     ("🏛 City of Dana Point",   "#4A6FA5",  "TOT Revenue, Infrastructure & Economic Policy"),
        "visitor":  ("✈️ Visitors",              ORANGE,     "Trip Planning, Rates & Events"),
        "resident": ("🏡 Residents",             "#6A4F8A",  "Community Impact & Local Access"),
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

            for pair in col_pairs:
                cols = st.columns(len(pair))
                for col, row in zip(cols, pair):
                    style_cls = PRIORITY_STYLE.get(row.get("priority", 5), "insight-info")
                    horizon   = row.get("horizon_days", 30)
                    category  = row.get("category", "").replace("_", " ").title()
                    sources   = row.get("data_sources", "")
                    as_of     = row.get("as_of_date", "")

                    with col:
                        st.markdown(
                            f'<div class="insight-card {style_cls}">'
                            f'<div class="insight-title">{row["headline"]}</div>'
                            f'<p class="insight-body">{row["body"]}</p>'
                            f'<div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">'
                            f'<span style="font-size:10px;opacity:0.5;background:rgba(255,255,255,0.07);'
                            f'padding:2px 8px;border-radius:99px;">{category}</span>'
                            f'<span style="font-size:10px;opacity:0.5;background:rgba(255,255,255,0.07);'
                            f'padding:2px 8px;border-radius:99px;">⏱ {horizon}d outlook</span>'
                            f'<span style="font-size:10px;opacity:0.40;'
                            f'padding:2px 4px;">as of {as_of}</span>'
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
# TAB 4 — EVENT IMPACT
# ══════════════════════════════════════════════════════════════════════════════
with tab_ev:
    st.markdown(
        '<div style="font-family:\'Plus Jakarta Sans\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        'Ohana Fest 2025 — Dana Point, CA</div>'
        '<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        'Source: Datafy visitor economy report &nbsp;·&nbsp; Pending live DB integration</div>',
        unsafe_allow_html=True,
    )

    # Hero stats grid
    hero_stats = [
        ("$14.6M", "Event Expenditure",      "money",    "Sep 26–28, 2025"),
        ("$18.4M", "Destination Spend",      "globe",    "Sep 26–28, 2025"),
        ("+$139",  "ADR Lift vs. Baseline",  "tag",      "Event nights vs. baseline"),
        ("$1,219", "Avg Accom. Spend / Trip","bed",      "Per overnight visitor"),
        ("68%",    "Out-of-State Visitors",  "plane",    "Share of total attendees"),
        ("3.2×",   "Spend Multiplier",       "chart_up", "Economic impact ratio"),
    ]
    ec = st.columns(3)
    for i, (val, lbl, ico, dt) in enumerate(hero_stats):
        with ec[i % 3]:
            st.markdown(event_stat(val, lbl, ico, dt), unsafe_allow_html=True)

    st.markdown("---")

    # ── Event lift chart ───────────────────────────────────────────────────────
    st.markdown('<div class="chart-header">ADR & Occupancy Lift — Ohana Fest 2024</div>', unsafe_allow_html=True)
    st.markdown('<div class="chart-caption">Event period vs. surrounding baseline &nbsp;·&nbsp; shaded = event weekend</div>', unsafe_allow_html=True)
    event_days = [
        "Sep 15","Sep 16","Sep 17","Sep 18","Sep 19","Sep 20","Sep 21",
        "Sep 22","Sep 23","Sep 24","Sep 25","Sep 26","Sep 27","Sep 28","Sep 29",
    ]
    adr_vals = [403,410,415,420,436,430,425,418,412,430,460,510,542,530,403]
    occ_vals = [65.6,66,67,68,71.2,70,69,67,66,70,74,77,78.8,76,65.6]

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(
        x=event_days, y=occ_vals, name="Occupancy %",
        line=dict(color=TEAL, width=2),
        mode="lines+markers", marker=dict(size=5, color=TEAL),
        hovertemplate="<b>%{x}</b><br>Occ: %{y:.1f}%<extra></extra>",
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=event_days, y=adr_vals, name="ADR $",
        line=dict(color=ORANGE, width=2),
        mode="lines+markers", marker=dict(size=5, color=ORANGE),
        hovertemplate="<b>%{x}</b><br>ADR: $%{y:.0f}<extra></extra>",
    ), secondary_y=True)
    # Shade event weekend
    for shade_x in ["Sep 26", "Sep 27", "Sep 28"]:
        fig.add_vline(x=shade_x, line_width=1,
                      line_color="rgba(33,128,141,0.2)", line_dash="dot")
    fig.update_yaxes(title_text="Occ %", range=[60, 85], secondary_y=False)
    fig.update_yaxes(title_text="ADR $", tickprefix="$", range=[350, 580],
                     secondary_y=True, showgrid=False)
    st.plotly_chart(style_fig(fig, height=320), use_container_width=True)

    c1, c2 = st.columns(2)

    with c1:
        st.markdown('<div class="chart-header">Top Feeder Markets</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">Visitor origin by share of visitor days (Datafy 2025)</div>', unsafe_allow_html=True)
        cities = ["San Clemente","San Juan Cap.","Dana Point","Laguna Niguel",
                  "Ladera Ranch","Huntington Bch","San Diego","Mission Viejo",
                  "Los Angeles","Cap. Beach"]
        shares = [8.89,7.84,6.78,6.50,3.53,3.19,2.96,2.42,2.35,1.85]
        # Gradient color scale — deeper teal for larger shares
        _max_s = max(shares)
        _bar_colors = [
            f"rgba(33,{int(128 + 56*(v/_max_s))},{int(141 + 57*(v/_max_s))},0.90)"
            for v in shares
        ]
        fig = go.Figure(go.Bar(
            x=shares, y=cities, orientation="h",
            marker=dict(color=_bar_colors, line_width=0, cornerradius=5),
            text=[f"{v:.1f}%" for v in shares], textposition="outside",
            textfont=dict(size=11, family="Plus Jakarta Sans, Inter, sans-serif"),
            hovertemplate="<b>%{y}</b><br>Share: %{x:.2f}%<extra></extra>",
        ))
        fig.update_layout(yaxis=dict(autorange="reversed"),
                          xaxis_ticksuffix="%", showlegend=False)
        st.plotly_chart(style_fig(fig, height=340), use_container_width=True)

    with c2:
        st.markdown('<div class="chart-header">Spending by Category</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">Share of total destination spend during event weekend</div>', unsafe_allow_html=True)
        cats   = ["Dining & Nightlife","Accommodations","Grocery/Dept",
                  "Service Stations","Fast Food","Specialty Retail",
                  "Personal Care","Clothing","Leisure/Rec"]
        values = [30.2,23.6,17.4,7.71,6.73,6.46,4.08,2.09,1.33]
        palette = [TEAL,"#2DA6B2",TEAL_LIGHT,ORANGE,"#A84B2F",
                   "#5E5240","#626C71","#A7A9A9",RED]
        fig = go.Figure(go.Pie(
            labels=cats, values=values, hole=0.48,
            marker=dict(colors=palette, line=dict(color="rgba(0,0,0,0)", width=0)),
            textfont=dict(size=11, family="Plus Jakarta Sans, Inter, sans-serif"),
            hovertemplate="<b>%{label}</b><br>%{value:.1f}% of destination spend<extra></extra>",
        ))
        fig.update_layout(
            legend=dict(font_size=10, orientation="v",
                        font=dict(family="Plus Jakarta Sans, Inter, sans-serif")),
            annotations=[dict(text="Spend<br>Mix", x=0.5, y=0.5, font_size=13,
                              font_family="Plus Jakarta Sans, sans-serif",
                              font_color="#21808D", showarrow=False)],
        )
        st.plotly_chart(style_fig(fig, height=340), use_container_width=True)

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
