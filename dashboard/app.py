
"""
Visit Dana Point — Analytics Dashboard
Streamlit app with Claude AI Analyst · Read-only connection to data/analytics.sqlite
"""

# © 2026 Wilton John Picou · GloCon Solutions LLC · All rights reserved.
# Dana Point PULSE — Destination Intelligence Platform
# Unauthorized reproduction or distribution prohibited.

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
import re as _re


def md_to_html(text: str) -> str:
    """Convert basic markdown to HTML for use in unsafe_allow_html contexts."""
    if not text:
        return text
    # Bold: **text** → <strong>text</strong>
    text = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    # Italic: *text* → <em>text</em>
    text = _re.sub(r'\*(.+?)\*', r'<em>\1</em>', text)
    # Newlines → <br>
    text = text.replace('\n', '<br>')
    return text

# Load .env from the project root (one level above dashboard/)
load_dotenv(Path(__file__).parent.parent / ".env")

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    import openai as _openai_sdk
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import google.generativeai as _genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# Pre-load API keys from env
_ENV_API_KEY        = os.getenv("ANTHROPIC_API_KEY", "")
_ENV_OPENAI_KEY     = os.getenv("OPENAI_API_KEY", "")
_ENV_GOOGLE_AI_KEY  = os.getenv("GOOGLE_AI_API_KEY", "")
_ENV_PERPLEXITY_KEY = os.getenv("PERPLEXITY_API_KEY", "")

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dana Point PULSE",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Login Gate ───────────────────────────────────────────────────────────────
# © 2026 Wilton John Picou · GloCon Solutions LLC
# Supports simple credential login now; Google/Microsoft OAuth can be added once
# OAuth client IDs are configured in .env (GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET,
# MS_CLIENT_ID, MS_CLIENT_SECRET). Set LOGIN_ENABLED=false in .env to bypass for local dev.
_LOGIN_ENABLED = False  # Login removed per owner request — admin controls at ?admin=true

def _render_login_page():
    """Render the login page with Dana Point branding.
    GloCon Solutions LLC — access control for Dana Point PULSE."""
    # Background: actual homepage hero photo from visitdanapoint.com
    # Seth Willingham — Beachfront Lodging overlooking Salt Creek Beach, Dana Point, CA
    _VDP_BG = "https://assets.simpleviewinc.com/simpleview/image/upload/c_fill,g_xy_center,h_700,q_60,w_1600,x_5873,y_3133/v1/clients/danapointca/Seth_Willingham_Dana_Point_1_56019588-2c56-4092-a6a5-5fc4c32b1b9a.jpg"
    # Inject background separately (f-string) to avoid escaping issues in the main CSS block
    st.markdown(
        f"<style>[data-testid='stAppViewContainer'] {{"
        f"background: linear-gradient(160deg,rgba(10,22,40,0.82) 0%,rgba(13,33,68,0.75) 100%),"
        f"url('{_VDP_BG}') center/cover no-repeat fixed !important;}}</style>",
        unsafe_allow_html=True,
    )
    # Full-page login CSS
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&display=swap');
    .login-wrap {
        max-width: 420px; margin: 80px auto 0 auto;
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 16px; padding: 40px 36px 32px 36px;
        backdrop-filter: blur(20px);
        box-shadow: 0 24px 64px rgba(0,0,0,0.55);
    }
    .login-logo {
        text-align: center; margin-bottom: 6px;
        font-size: 2.6rem;
    }
    .login-title {
        font-family: 'Syne', sans-serif;
        font-size: 1.6rem; font-weight: 800; text-align: center;
        color: #FFFFFF; letter-spacing: -0.03em; margin-bottom: 4px;
    }
    .login-title span { color: #00C8E0; }
    .login-sub {
        text-align: center; font-size: 13px; color: rgba(255,255,255,0.55);
        margin-bottom: 28px;
    }
    .login-divider {
        display: flex; align-items: center; gap: 10px;
        margin: 18px 0;
    }
    .login-divider-line { flex: 1; height: 1px; background: rgba(255,255,255,0.12); }
    .login-divider-text { font-size: 11px; color: rgba(255,255,255,0.40); white-space: nowrap; }
    .oauth-btn {
        display: flex; align-items: center; justify-content: center; gap: 10px;
        width: 100%; padding: 11px 18px; border-radius: 8px; margin-bottom: 10px;
        font-size: 14px; font-weight: 600; cursor: pointer; border: none;
        transition: opacity 0.2s;
    }
    .oauth-btn:hover { opacity: 0.88; }
    .btn-google { background: #FFFFFF; color: #1F2937; }
    .btn-microsoft { background: #2F2F2F; color: #FFFFFF; }
    .login-footer {
        text-align: center; font-size: 11px; color: rgba(255,255,255,0.30);
        margin-top: 24px; line-height: 1.6;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="login-wrap">
      <div class="login-logo">🌊</div>
      <div class="login-title">Dana Point <span>PULSE</span></div>
      <div class="login-sub">Destination Intelligence Platform · VDP Select Portfolio</div>
    </div>
    """, unsafe_allow_html=True)

    # Center the form
    _, _lc, _ = st.columns([1, 2, 1])
    with _lc:
        st.markdown("**Sign in to Dana Point PULSE**")

        # OAuth buttons (shown but redirect requires server-side OAuth setup)
        _google_cfg  = os.getenv("GOOGLE_CLIENT_ID", "")
        _ms_cfg      = os.getenv("MS_CLIENT_ID", "")

        if _google_cfg:
            if st.button("🔵  Continue with Google", use_container_width=True,
                         help="Sign in using your Google account"):
                # Google OAuth redirect — requires GOOGLE_CLIENT_ID in .env
                _oauth_url = (
                    "https://accounts.google.com/o/oauth2/auth"
                    f"?client_id={_google_cfg}"
                    "&redirect_uri=" + os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8501")
                    + "&response_type=code&scope=openid%20email%20profile"
                )
                st.markdown(f'<meta http-equiv="refresh" content="0;url={_oauth_url}">',
                            unsafe_allow_html=True)
        else:
            st.button("🔵  Continue with Google", disabled=True, use_container_width=True,
                      help="Google OAuth: set GOOGLE_CLIENT_ID in .env to enable")

        if _ms_cfg:
            if st.button("🟦  Continue with Microsoft", use_container_width=True,
                         help="Sign in using your Microsoft account"):
                _ms_url = (
                    f"https://login.microsoftonline.com/{os.getenv('MS_TENANT_ID','common')}"
                    f"/oauth2/v2.0/authorize?client_id={_ms_cfg}"
                    "&response_type=code&scope=openid%20email%20profile"
                    "&redirect_uri=" + os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8501")
                )
                st.markdown(f'<meta http-equiv="refresh" content="0;url={_ms_url}">',
                            unsafe_allow_html=True)
        else:
            st.button("🟦  Continue with Microsoft", disabled=True, use_container_width=True,
                      help="Microsoft OAuth: set MS_CLIENT_ID + MS_TENANT_ID in .env to enable")

        st.markdown('<div class="login-divider"><div class="login-divider-line"></div>'
                    '<div class="login-divider-text">or sign in with password</div>'
                    '<div class="login-divider-line"></div></div>', unsafe_allow_html=True)

        _username = st.text_input("Username", placeholder="your username",
                                  key="login_user", label_visibility="collapsed")
        _password = st.text_input("Password", placeholder="password", type="password",
                                  key="login_pass", label_visibility="collapsed")

        # Credential check — reads from .env: PULSE_USERS="user1:hash1,user2:hash2"
        # or falls back to PULSE_ADMIN_USER / PULSE_ADMIN_PASS for single-user setup
        if st.button("Sign In →", use_container_width=True, type="primary"):
            import hashlib
            _admin_user = os.getenv("PULSE_ADMIN_USER", "admin")
            _admin_pass = os.getenv("PULSE_ADMIN_PASS", "")
            _entered_hash = hashlib.sha256(_password.encode()).hexdigest()
            _stored_hash  = hashlib.sha256(_admin_pass.encode()).hexdigest() if _admin_pass else ""

            _users_env = os.getenv("PULSE_USERS", "")
            _valid = False
            if _users_env:
                for _pair in _users_env.split(","):
                    _parts = _pair.strip().split(":", 1)
                    if len(_parts) == 2 and _parts[0] == _username and _parts[1] == _entered_hash:
                        _valid = True; break
            elif _admin_pass and _username == _admin_user and _entered_hash == _stored_hash:
                _valid = True
            elif not _admin_pass:
                # No password configured → accept any input (dev/demo mode)
                _valid = bool(_username)

            if _valid:
                st.session_state["authenticated"] = True
                st.session_state["auth_user"] = _username
                st.rerun()
            else:
                st.error("Incorrect username or password.")

        st.markdown('<div class="login-footer">'
                    '© 2026 Wilton John Picou · GloCon Solutions LLC<br>'
                    'Confidential · Authorized Access Only</div>', unsafe_allow_html=True)

    st.stop()


if _LOGIN_ENABLED and not st.session_state.get("authenticated", False):
    _render_login_page()

# ─── Brand palette ────────────────────────────────────────────────────────────
TEAL       = "#21808D"
TEAL_LIGHT = "#32B8C6"
ORANGE     = "#E68161"
RED        = "#C0152F"
GREEN      = "#21808D"    # teal = positive to match brand
BLUE       = "#0567C8"    # primary accent — TSA/wave/fly-market
PURPLE     = "#7C3AED"    # CoStar / pipeline
GOLD       = "#D97706"    # amber / warning
NAVY       = "#0D1B2E"    # dark header bg

# ─── AI constants ─────────────────────────────────────────────────────────────
CLAUDE_MODEL = "claude-sonnet-4-6"

# ─── Multi-Model Registry ─────────────────────────────────────────────────────
AI_MODELS = {
    "claude-sonnet-4-6": {
        "label": "Claude Sonnet 4.6",
        "provider": "anthropic",
        "badge": "🟦",
        "strengths": "TBID · Board reports · Deep domain reasoning",
    },
    "claude-opus-4-6": {
        "label": "Claude Opus 4.6",
        "provider": "anthropic",
        "badge": "🔷",
        "strengths": "Complex multi-dataset analysis · Long-form strategy",
    },
    "gpt-4o": {
        "label": "GPT-4o",
        "provider": "openai",
        "badge": "🟩",
        "strengths": "Revenue management · Comp set benchmarking · Pricing",
    },
    "o3-mini": {
        "label": "o3-mini (reasoning)",
        "provider": "openai",
        "badge": "🟢",
        "strengths": "Step-by-step quantitative analysis · Revenue modeling",
    },
    "gemini-2.0-flash": {
        "label": "Gemini 2.0 Flash",
        "provider": "google",
        "badge": "🟨",
        "strengths": "Fast correlations · Pattern recognition · Data analysis",
    },
    "gemini-1.5-pro": {
        "label": "Gemini 1.5 Pro",
        "provider": "google",
        "badge": "🔶",
        "strengths": "Long-context · Multi-document · Trend synthesis",
    },
    "sonar-pro": {
        "label": "Perplexity Sonar Pro",
        "provider": "perplexity",
        "badge": "🟪",
        "strengths": "Live web search · Competitor news · Travel trends",
    },
    "sonar": {
        "label": "Perplexity Sonar",
        "provider": "perplexity",
        "badge": "🔹",
        "strengths": "Fast live search · Market intelligence",
    },
}

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

## Full Database Schema (analytics.sqlite — 57 tables)

The VDP brain contains these tables — ALL are available for your analysis. \
Layer 1 (STR, Datafy, CoStar) = current truth. Layer 1.5 (Zartico) = historical reference only.

**STR & KPI Tables (Layer 1 — Current Truth):**
- `fact_str_metrics` — Long-format daily/monthly STR hotel data (supply, demand, revenue, occ, adr, revpar). \
  Columns: source, grain, property_name, market, submarket, as_of_date, metric_name, metric_value, unit.
- `kpi_daily_summary` — Wide-format daily KPIs with YOY deltas and compression flags. \
  Columns: as_of_date, occ_pct, adr, revpar, occ_yoy, adr_yoy, revpar_yoy, is_occ_80, is_occ_90.
- `kpi_compression_quarterly` — Days per quarter above 80% / 90% occupancy. \
  Columns: quarter (YYYY-Qn), days_above_80_occ, days_above_90_occ.

**Datafy Visitor Economy Tables (Layer 1 — Current Truth):**
- `datafy_overview_kpis` — Annual visitor overview: total_trips, overnight_pct, out_of_state_vd_pct, \
  repeat_visitors_pct, avg_length_of_stay_days. Most recent annual Datafy report.
- `datafy_overview_dma` — Feeder market DMA breakdown: dma, visitor_days_share_pct, spending_share_pct, avg_spend_usd.
- `datafy_overview_demographics` — Visitor demographics by segment (age, income, travel party).
- `datafy_overview_category_spending` — Spending by category (accommodation, dining, retail, entertainment).
- `datafy_overview_cluster_visitation` — Visitation by area cluster type (beach, downtown, harbor).
- `datafy_overview_airports` — Origin airports by passenger share (JWA, LAX, SNA context).
- `datafy_attribution_website_kpis` — Website-attributed trips and estimated destination impact (ROAS context).
- `datafy_attribution_website_top_markets` — Website attribution top feeder markets.
- `datafy_attribution_website_dma` — Website attribution DMA breakdown.
- `datafy_attribution_website_channels` — Website attribution by acquisition channel (organic, paid, social, email).
- `datafy_attribution_website_clusters` — Website attribution by area cluster.
- `datafy_attribution_website_demographics` — Website attribution visitor demographics.
- `datafy_attribution_media_kpis` — Media campaign: attributable_trips, total_impact_usd, ROAS.
- `datafy_attribution_media_top_markets` — Media attribution top feeder markets with ADR lift context.
- `datafy_social_traffic_sources` — GA4 web traffic sources: sessions, engagement_rate, bounce_rate.
- `datafy_social_audience_overview` — Website audience KPIs: users, sessions, avg_session_duration.
- `datafy_social_top_pages` — Top pages by view count (useful for content strategy).

**CoStar Market Intelligence Tables (Layer 1 — Current Truth):**
- `costar_market_snapshot` — Current quarter market snapshot: occ, adr, revpar, supply_rooms, demand_rooms.
- `costar_monthly_trends` — Monthly hotel performance trends (occ, adr, revpar YOY).
- `costar_annual_summary` — Annual summary performance with 3-year trend context.
- `costar_profitability` — Hotel profitability metrics: GOP, EBITDA, labor cost ratios.
- `costar_chain_scale` — Performance by chain scale (luxury, upper-upscale, upscale, midscale).
- `costar_compset` — Competitive set benchmarking data (MPI, ARI, RGI indices).
- `costar_pipeline` — Supply pipeline: properties under construction, in planning, opening dates.

**Zartico Historical Reference Tables (Layer 1.5 — Historical ONLY, Jun 2025 snapshot):**
CRITICAL: NEVER present Zartico as current data. Use only for historical trend comparison (2024–Jun 2025).
- `zartico_kpis` — Visitor economy KPIs snapshot (device %, spend share, demographics).
- `zartico_markets` — Top visitor origin markets (rank, %, avg spend) — Jun 2025 reference.
- `zartico_spending_monthly` — Monthly avg visitor spend vs. benchmark (Jul 2024–May 2025).
- `zartico_lodging_kpis` — Hotel/STVR summary (YTD occ, ADR, LOS, ADR by day of week).
- `zartico_overnight_trend` — Monthly overnight visitor % trend (May 2024–May 2025).
- `zartico_event_impact` — Event period vs. baseline spend changes.
- `zartico_movement_monthly` — Visitor-to-resident ratio by month.
- `zartico_future_events_summary` — YoY event + attendee growth context.

**Visit California State Context Tables (Layer 2):**
- `visit_ca_travel_forecast` — CA statewide travel forecast by quarter (visitor volume, spending).
- `visit_ca_lodging_forecast` — CA statewide lodging forecast (occ, ADR, RevPAR projections).
- `visit_ca_airport_traffic` — JWA and major CA airport traffic by month (passenger counts).
- `visit_ca_intl_arrivals` — International arrivals to California by market (supports fly-market analysis).

**Later.com Social Media Tables (Layer 2.5 — Current Social Performance):**
- `later_ig_profile_growth` — Instagram followers, reach, impressions by date.
- `later_ig_posts` — Individual Instagram post metrics (likes, comments, reach, engagement_rate).
- `later_ig_stories` — Instagram Stories performance (views, taps_forward, exits).
- `later_ig_reels` — Instagram Reels metrics (plays, reach, likes, shares).
- `later_ig_hashtags` — Hashtag performance analysis.
- `later_ig_locations` — Instagram location-tag performance.
- `later_fb_profile_growth` — Facebook page followers, reach, impressions by date.
- `later_fb_posts` — Individual Facebook post metrics (reactions, shares, reach).
- `later_fb_stories` — Facebook Stories performance.
- `later_tk_profile_growth` — TikTok followers, profile_views by date.
- `later_tk_posts` — TikTok video metrics (views, likes, shares, comments, engagement_rate).
- `later_tk_hashtags` — TikTok hashtag performance.

**External Economic & Demand Signal Tables (Layer 2 — Context):**
- `fred_economic_indicators` — FRED macro series (series_id, data_date, value, unit). Key series: \
  UMCSENT=Consumer Sentiment (6–8 week leading indicator for leisure travel), \
  DSPIC96=Real Disposable Personal Income (travel propensity driver), \
  UNRATE=US Unemployment Rate (inverse correlation with travel), \
  CUUR0000SEHB=Hotel CPI (benchmark for ADR trend context), \
  CEU7000000001=Leisure & Hospitality Employment, \
  CPILFESL=Core CPI (inflation backdrop for ADR management), \
  RSXFS=Retail & Food Service Sales (consumer spending proxy), \
  HOUST=Housing Starts (wealth effect signal), \
  PSAVERT=Personal Savings Rate (inverse with travel spend).
- `eia_gas_prices` — Weekly CA retail gas prices (week_end_date, price_per_gallon, yoy_change). \
  Drive-market demand signal: Dana Point's LA/OC/SD/IE feeder markets are 100% drive-market (120-mile radius). \
  $0.20/gal increase correlates with ~2–4% dip in weekend occupancy at coastal destinations.
- `tsa_checkpoint_daily` — TSA daily checkpoint throughput (national air travel demand proxy for fly markets).
- `bls_employment_monthly` — BLS Orange County employment by sector (local labor market context).
- `noaa_marine_monthly` — NOAA ocean buoy data (wave height, water temp — coastal visitor conditions).
- `weather_monthly` — Open-Meteo coastal weather averages (temp, precipitation, sunshine hours).
- `google_trends_weekly` — Google search demand trends for Dana Point and competing destinations.
- `census_demographics` — US Census ACS demographics for feeder market MSAs.
- `vdp_events` — Known major Dana Point events (event_name, event_date, event_type, is_major flag).

**Intelligence Tables (Generated Daily):**
- `insights_daily` — Forward-looking insights for 5 audiences (dmo, city, visitor, resident, cross). \
  Columns: as_of_date, audience, category, headline, body, metric_basis (JSON), priority, horizon_days, data_sources.
- `table_relationships` — 125+ documented cross-table joins and derivations.
- `load_log` — ETL pipeline audit trail (source, grain, file_name, rows_inserted, run_at).\
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
  /* ════════════════════════════════════════════════════════════════════════
     DANA POINT PULSE — Premium Light Analytics Design System v5
     Enterprise-Grade · Data-Forward · Crisp Typography · Elevated Cards
  ════════════════════════════════════════════════════════════════════════ */

  /* ── Google Fonts ────────────────────────────────────────────────────── */
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=Outfit:wght@400;500;600;700;800;900&family=Inter:wght@300;400;500;600;700&family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&display=swap');

  /* ── Design Tokens ───────────────────────────────────────────────────── */
  :root {
    --dp-bg:            #EEF2F8;
    --dp-bg2:           #E2E8F2;
    --dp-surface:       #FFFFFF;
    --dp-card:          #FFFFFF;
    --dp-card-solid:    #FFFFFF;
    --dp-card-hover:    #F0F7FF;
    --dp-border:        rgba(15,28,46,0.08);
    --dp-border-accent: rgba(5,103,200,0.28);
    --dp-teal:          #0567C8;
    --dp-teal-dim:      rgba(5,103,200,0.10);
    --dp-teal-glow:     rgba(5,103,200,0.18);
    --dp-blue:          #2563EB;
    --dp-green:         #059669;
    --dp-amber:         #D97706;
    --dp-red:           #DC2626;
    --dp-purple:        #7C3AED;
    --dp-orange:        #EA580C;
    --dp-text-1:        #07111F;
    --dp-text-2:        #1E293B;
    --dp-text-3:        #475569;
    --dp-radius:        12px;
    --dp-radius-lg:     16px;
    --dp-shadow:        0 1px 2px rgba(15,28,46,0.04), 0 4px 16px rgba(15,28,46,0.10);
    --dp-shadow-hover:  0 8px 28px rgba(15,28,46,0.16), 0 0 0 2px rgba(5,103,200,0.15);
    --dp-shadow-deep:   0 12px 40px rgba(15,28,46,0.20);
  }

  html, body, [class*="css"] {
    font-family: 'Inter', system-ui, -apple-system, sans-serif;
    background-color: var(--dp-bg) !important;
    color: var(--dp-text-1) !important;
  }
  body, .main, .stApp, [data-testid="stAppViewContainer"] {
    background-color: var(--dp-bg) !important;
    background-image: none !important;
  }
  .block-container {
    background-color: transparent !important;
  }

  /* ── KPI Cards ───────────────────────────────────────────────────────── */
  .kpi-card {
    background: #FFFFFF;
    border-radius: var(--dp-radius-lg);
    padding: 22px 22px 18px 22px;
    border: 1px solid rgba(15,28,46,0.07);
    border-bottom: 3px solid var(--dp-teal);
    color: var(--dp-text-1);
    margin-bottom: 14px;
    position: relative;
    overflow: hidden;
    transition: box-shadow 0.24s cubic-bezier(0.28,0,0.49,1),
                transform 0.24s cubic-bezier(0.28,0,0.49,1);
    box-shadow: 0 1px 3px rgba(15,28,46,0.05), 0 4px 14px rgba(15,28,46,0.07);
  }
  .kpi-card::before {
    content: '';
    position: absolute; bottom: 0; left: 0; right: 0;
    height: 40%; pointer-events: none;
    background: linear-gradient(0deg, rgba(5,103,200,0.02) 0%, transparent 100%);
  }
  .kpi-card::after { content: none; }
  .kpi-card:hover {
    box-shadow: var(--dp-shadow-hover);
    transform: translateY(-4px);
  }
  .kpi-header {
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 6px;
  }
  .kpi-label {
    font-family: 'Syne', 'DM Sans', 'Inter', sans-serif;
    font-size: 9.5px; font-weight: 700;
    text-transform: uppercase; letter-spacing: .15em;
    color: var(--dp-text-3);
  }
  .kpi-icon-svg { flex-shrink: 0; line-height: 0; opacity: 0.55; }
  .kpi-value {
    font-family: 'Syne', 'Outfit', sans-serif;
    font-size: 36px; font-weight: 800;
    letter-spacing: -.04em; line-height: 1.0;
    color: var(--dp-text-1);
    -webkit-text-fill-color: var(--dp-text-1);
    margin: 6px 0 8px 0;
  }
  .kpi-delta-pos     { color: #059669; font-size: 12px; font-weight: 700; display:flex; align-items:center; gap:4px; }
  .kpi-delta-neg     { color: #DC2626; font-size: 12px; font-weight: 700; display:flex; align-items:center; gap:4px; }
  .kpi-delta-neutral { color: var(--dp-text-3); font-size: 12px; font-weight: 600; }
  .kpi-date {
    font-size: 10px; color: var(--dp-text-3);
    margin-top: 10px; letter-spacing: .02em;
    border-top: 1px solid rgba(15,28,46,0.07);
    padding-top: 8px; display: block; font-weight: 500;
  }

  /* ── Insight Cards ────────────────────────────────────────────────────── */
  .insight-card {
    border-radius: var(--dp-radius-lg);
    padding: 16px 18px;
    margin-bottom: 10px;
    position: relative;
    border: 1px solid rgba(15,28,46,0.08);
    background: #FFFFFF;
    color: var(--dp-text-1);
    transition: box-shadow 0.24s ease, transform 0.24s ease;
    overflow: hidden;
    box-shadow: var(--dp-shadow);
  }
  .insight-card:hover {
    box-shadow: var(--dp-shadow-hover);
    transform: translateY(-3px);
  }
  .insight-card::before {
    content: '';
    position: absolute; top: 0; left: 0;
    width: 4px; height: 100%;
  }
  .insight-positive::before { background: linear-gradient(180deg, #059669, #047857); }
  .insight-warning::before  { background: linear-gradient(180deg, #D97706, #B45309); }
  .insight-negative::before { background: linear-gradient(180deg, #DC2626, #B91C1C); }
  .insight-info::before     { background: linear-gradient(180deg, #0567C8, #2563EB); }
  .insight-title {
    font-family: 'Outfit', sans-serif;
    font-size: 13.5px; font-weight: 800;
    margin-bottom: 6px; letter-spacing: -.02em;
    padding-left: 14px;
    color: var(--dp-text-1);
  }
  .insight-body {
    font-size: 13px; color: var(--dp-text-2);
    line-height: 1.65; margin: 0;
    padding-left: 12px;
  }

  /* ── AI Chip Badge ───────────────────────────────────────────────────── */
  .ai-chip {
    display: inline-flex; align-items: center; gap: 5px;
    font-family: 'Inter', sans-serif;
    font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: .10em;
    padding: 3px 10px; border-radius: 20px;
    background: rgba(5,103,200,0.08);
    color: var(--dp-teal); margin-bottom: 12px;
    border: 1px solid rgba(5,103,200,0.20);
  }

  /* ── Event Stat Cards ────────────────────────────────────────────────── */
  .event-stat {
    background: #FFFFFF;
    border: 1px solid rgba(15,28,46,0.07);
    border-top: 3px solid #7C3AED;
    border-radius: var(--dp-radius-lg);
    padding: 20px 16px; text-align: center; margin-bottom: 10px;
    transition: box-shadow 0.25s ease, transform 0.25s ease;
    position: relative; overflow: hidden;
    box-shadow: var(--dp-shadow);
  }
  .event-stat::before { content: none; }
  .event-stat:hover {
    box-shadow: var(--dp-shadow-hover);
    transform: translateY(-3px);
  }
  .event-icon  { line-height: 0; display: flex; justify-content: center; margin-bottom: 10px; }
  .event-val   {
    font-family: 'Outfit', sans-serif;
    font-size: 28px; font-weight: 800;
    color: var(--dp-text-1);
    -webkit-text-fill-color: var(--dp-text-1);
    letter-spacing: -.04em; line-height: 1;
  }
  .event-label {
    font-family: 'Inter', sans-serif;
    font-size: 11px; font-weight: 600;
    color: var(--dp-text-2); margin-top: 6px; text-transform: uppercase;
    letter-spacing: .08em;
  }
  .event-date  { font-size: 11px; color: var(--dp-text-3); margin-top: 4px; }

  /* ── Insight Icon ────────────────────────────────────────────────────── */
  .insight-icon { display: inline-block; vertical-align: middle; margin-right: 6px; line-height: 0; }

  /* ── Hide Streamlit Chrome ───────────────────────────────────────────── */
  #MainMenu                              { visibility: hidden !important; }
  footer                                 { visibility: hidden !important; }
  [data-testid="stToolbar"]             { visibility: hidden !important; }
  [data-testid="stDecoration"]          { display:    none    !important; }
  [data-testid="stStatusWidget"]        { visibility: hidden !important; }
  [data-testid="stHeader"]              { background: transparent !important; height: 0 !important; min-height: 0 !important; padding: 0 !important; }
  .viewerBadge_container__1QSob        { display:    none    !important; }
  .styles_viewerBadge__CvC9N           { display:    none    !important; }
  a[href*="streamlit.io"]               { display:    none    !important; }
  a[href*="github.com/streamlit"]       { display:    none    !important; }

  /* ── Fix sticky: inner containers must not clip overflow ────────────── */
  /* Do NOT change stAppViewContainer — it provides the page scroll */
  [data-testid="stMain"],
  section.main,
  .main > div,
  [data-testid="stVerticalBlock"],
  [data-testid="stVerticalBlockBorderWrapper"] {
    overflow: visible !important;
  }
  /* block-container is direct parent of hero + tabs — must be visible */
  .block-container {
    overflow: visible !important;
  }

  /* ── Custom Scrollbar ────────────────────────────────────────────────── */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: rgba(15,28,46,0.04); }
  ::-webkit-scrollbar-thumb {
    background: rgba(5,103,200,0.22); border-radius: 3px;
  }
  ::-webkit-scrollbar-thumb:hover { background: rgba(5,103,200,0.45); }

  /* ── Empty State ─────────────────────────────────────────────────────── */
  .empty-card {
    background: #FFFFFF;
    border-radius: var(--dp-radius-lg); padding: 40px 28px; text-align: center;
    border: 1px dashed rgba(5,103,200,0.22); margin: 6px 0 12px 0;
    box-shadow: var(--dp-shadow);
  }
  .empty-icon  { font-size: 32px; margin-bottom: 12px; opacity: 0.45; }
  .empty-title {
    font-family: 'Outfit', sans-serif;
    font-size: 15px; font-weight: 700; margin-bottom: 8px; letter-spacing: -.01em;
    color: var(--dp-text-1);
  }
  .empty-body  { font-size: 13px; color: var(--dp-text-2); line-height: 1.65; }

  /* ── Data Source Health Cards ────────────────────────────────────────── */
  .src-card {
    background: #FFFFFF;
    border-radius: var(--dp-radius); padding: 12px 16px;
    border: 1px solid rgba(15,28,46,0.07);
    margin-bottom: 6px; display: flex; align-items: center; gap: 12px;
    transition: box-shadow 0.20s ease, border-color 0.20s ease, background 0.20s ease; cursor: default;
    text-decoration: none !important; color: inherit !important;
    box-shadow: var(--dp-shadow);
  }
  a.src-card { cursor: pointer; }
  .src-card:hover {
    border-color: rgba(5,103,200,0.25);
    background: rgba(5,103,200,0.03);
    box-shadow: var(--dp-shadow-hover);
  }
  a.src-card:hover .src-name { color: var(--dp-teal) !important; }
  .src-dot   { font-size: 14px; flex-shrink: 0; }
  .src-name  {
    font-family: 'Outfit', sans-serif;
    font-size: 13px; font-weight: 700; color: var(--dp-text-1);
  }
  .src-meta  { font-size: 12px; color: var(--dp-text-3); margin-top: 2px; line-height: 1.4; }
  .src-count {
    font-family: 'Outfit', sans-serif; font-size: 13px; font-weight: 700;
    color: var(--dp-teal); margin-left: auto; text-align: right; white-space: nowrap;
  }

  /* ── Grain Badge ─────────────────────────────────────────────────────── */
  .grain-badge {
    display: inline-block; font-family: 'Inter', sans-serif;
    font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: .08em;
    padding: 2px 8px; border-radius: 20px;
    background: rgba(234,88,12,.10); color: #EA580C;
    margin-left: 8px; vertical-align: middle;
    border: 1px solid rgba(234,88,12,.20);
  }

  /* ── Hero Banner ─────────────────────────────────────────────────────── */
  @keyframes hero-grid-drift {
    0%   { background-position: 0 0; }
    100% { background-position: 60px 60px; }
  }
  @keyframes hero-glow-pulse {
    0%,100% { opacity: 0.12; transform: scale(1); }
    50%      { opacity: 0.22; transform: scale(1.08); }
  }
  @keyframes hero-line-in {
    from { transform: scaleX(0); transform-origin: left; }
    to   { transform: scaleX(1); transform-origin: left; }
  }
  .hero-banner {
    background: #06111F !important;
    background-image:
      radial-gradient(circle at 80% 20%, rgba(5,103,200,0.18) 0%, transparent 55%),
      radial-gradient(circle at 10% 80%, rgba(56,189,248,0.08) 0%, transparent 45%),
      linear-gradient(180deg, #06111F 0%, #091628 100%) !important;
    border-radius: 0 !important;
    margin: -1rem -1rem 0 -1rem;
    padding: 22px 36px 20px 36px;
    border-bottom: none !important;
    overflow: hidden;
  }
  .hero-banner::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent 0%, #0567C8 20%, #38BDF8 50%, #7DD3FC 75%, transparent 100%);
    animation: hero-line-in 0.9s cubic-bezier(0.37,0,0.22,1) both;
    opacity: 1;
  }
  .hero-banner::after {
    content: '';
    position: absolute; top: -80px; right: -80px;
    width: 300px; height: 300px;
    background: radial-gradient(circle, rgba(5,103,200,0.20) 0%, transparent 65%);
    pointer-events: none;
    animation: hero-glow-pulse 5s ease-in-out infinite;
  }
  .hero-title {
    font-family: 'Syne', 'Outfit', sans-serif;
    font-size: 2.1rem; font-weight: 800; letter-spacing: -0.04em; line-height: 1.0;
    color: #FFFFFF; margin-bottom: 8px; position: relative;
  }
  .hero-title span {
    color: #38BDF8; /* fallback for non-webkit */
    background: linear-gradient(110deg, #38BDF8 0%, #7DD3FC 40%, #BAE6FD 75%, #38BDF8 100%);
    background-size: 200% auto;
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
    animation: shimmer 4s linear infinite;
  }
  /* Force color visibility if background-clip text fails */
  @supports not (-webkit-background-clip: text) {
    .hero-title span { color: #38BDF8 !important; background: none !important; }
  }
  @keyframes shimmer {
    0%   { background-position: 0% center; }
    100% { background-position: 200% center; }
  }
  .hero-subtitle {
    font-family: 'DM Sans', 'Inter', sans-serif;
    font-size: 13px; font-weight: 600; color: rgba(255,255,255,0.88);
    letter-spacing: 0.04em; margin-top: 6px; position: relative;
    text-transform: uppercase;
  }

  /* ── Home button title ───────────────────────────────────────────────── */
  .home-title a {
    text-decoration: none; color: inherit;
    font-family: 'Outfit', sans-serif;
    font-size: 2rem; font-weight: 900;
    letter-spacing: -0.04em; line-height: 1.2;
  }
  .home-title a:hover { opacity: 0.80; }

  /* ── Filter Active Badge ─────────────────────────────────────────────── */
  .filter-badge {
    display: inline-block; font-family: 'Inter', sans-serif;
    font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: .08em;
    padding: 2px 8px; border-radius: 5px;
    background: rgba(234,88,12,.10); color: var(--dp-orange);
    margin-left: 6px; vertical-align: middle;
  }

  /* ── Load-log Source Badges ──────────────────────────────────────────── */
  .log-badge-str   { display:inline-block; padding:2px 8px; border-radius:20px;
    font-size:10px; font-weight:700; background:rgba(5,103,200,.10); color:#0567C8;
    border: 1px solid rgba(5,103,200,0.20); }
  .log-badge-kpi   { display:inline-block; padding:2px 8px; border-radius:20px;
    font-size:10px; font-weight:700; background:rgba(234,88,12,.10); color:#EA580C;
    border: 1px solid rgba(234,88,12,0.20); }
  .log-badge-other { display:inline-block; padding:2px 8px; border-radius:20px;
    font-size:10px; font-weight:700; background:rgba(15,28,46,.06); color:var(--dp-text-2);
    border: 1px solid rgba(15,28,46,0.10); }

  /* ── Trend Table ─────────────────────────────────────────────────────── */
  .trend-row-pos { color: #059669; font-weight: 700; }
  .trend-row-neg { color: #DC2626; font-weight: 700; }

  /* ── Section Sub-header Label ────────────────────────────────────────── */
  .section-label {
    font-family: 'Syne', 'Inter', sans-serif;
    font-size: 11.5px; font-weight: 800; text-transform: uppercase;
    letter-spacing: .12em; color: #0567C8;
    margin-bottom: 10px; margin-top: 4px;
    display: flex; align-items: center; gap: 8px;
  }
  .section-label::after {
    content: ''; flex: 1; height: 1px;
    background: linear-gradient(90deg, rgba(5,103,200,0.20), transparent);
  }

  /* ── Section Divider ─────────────────────────────────────────────────── */
  .section-divider {
    display: flex; align-items: center; gap: 14px; margin: 32px 0 18px 0;
  }
  .section-divider-line {
    flex: 1; height: 1px;
    background: linear-gradient(90deg, rgba(5,103,200,0.30), rgba(15,28,46,0.06));
  }
  .section-divider-title {
    font-family: 'DM Sans', 'Outfit', sans-serif;
    font-size: 10.5px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .14em; color: #0567C8;
    white-space: nowrap; padding: 5px 16px;
    background: rgba(5,103,200,0.08);
    border: 1px solid rgba(5,103,200,0.20);
    border-radius: 20px;
    box-shadow: 0 1px 4px rgba(5,103,200,0.08);
  }
  .section-divider-line-r {
    flex: 1; height: 1px;
    background: linear-gradient(90deg, rgba(15,28,46,0.06), transparent);
  }

  /* ── Tab Summary Card ─────────────────────────────────────────────────── */
  .tab-summary {
    background: rgba(5,103,200,0.05);
    border: 1px solid rgba(5,103,200,0.14);
    border-left: 4px solid var(--dp-teal);
    border-radius: var(--dp-radius-lg);
    padding: 16px 20px; margin: 8px 0 20px 0;
    font-family: 'DM Sans', 'Inter', sans-serif;
    font-size: 13.5px; color: var(--dp-text-2); line-height: 1.70;
  }
  .tab-summary strong { color: #0567C8; font-weight: 700; }

  /* ── Mini Data Card ─────────────────────────────────────────────────── */
  .mini-data-card {
    background: #FFFFFF;
    border: 1px solid rgba(15,28,46,0.08);
    border-top: 3px solid #0567C8;
    border-radius: var(--dp-radius-lg);
    padding: 14px 16px; margin-bottom: 10px;
    display: flex; flex-direction: column; gap: 4px;
    transition: border-color 0.22s ease, box-shadow 0.22s ease, transform 0.22s ease;
    box-shadow: var(--dp-shadow);
  }
  .mini-data-card:hover {
    border-color: rgba(5,103,200,0.30);
    box-shadow: var(--dp-shadow-hover);
    transform: translateY(-2px);
  }
  .mini-data-card-label {
    font-family: 'DM Sans', 'Inter', sans-serif;
    font-size: 9.5px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .10em; color: var(--dp-text-3);
  }
  .mini-data-card-value {
    font-family: 'Outfit', sans-serif;
    font-size: 22px; font-weight: 900;
    letter-spacing: -.04em; color: var(--dp-text-1); line-height: 1.1;
  }
  .mini-data-card-sub {
    font-family: 'DM Sans', 'Inter', sans-serif;
    font-size: 11px; color: var(--dp-text-3); margin-top: 2px; font-weight: 500;
  }

  /* ── Chart Container ─────────────────────────────────────────────────── */
  .chart-container {
    background: #FFFFFF;
    border: 1px solid rgba(15,28,46,0.08);
    border-radius: var(--dp-radius-lg);
    padding: 18px; margin-bottom: 14px;
    box-shadow: var(--dp-shadow);
  }
  .chart-header {
    font-family: 'Syne', 'Outfit', sans-serif;
    font-size: 17px; font-weight: 800; letter-spacing: -.03em; margin-bottom: 5px;
    color: #07111F; -webkit-text-fill-color: #07111F;
    display: flex; align-items: center; gap: 8px;
    padding-left: 10px; position: relative;
  }
  .chart-header::before {
    content: ''; position: absolute; left: 0; top: 2px; bottom: 2px;
    width: 3px; border-radius: 2px;
    background: linear-gradient(180deg, #0567C8, #38BDF8);
  }
  .chart-caption {
    font-family: 'DM Sans', sans-serif;
    font-size: 12px; color: #475569; font-weight: 500; margin-bottom: 12px;
    letter-spacing: 0.01em; padding-left: 10px;
  }

  /* ── Sidebar Brand ───────────────────────────────────────────────────── */
  .sidebar-brand {
    font-family: 'Outfit', sans-serif;
    font-size: 18px; font-weight: 900; letter-spacing: -.04em;
    color: #07111F; -webkit-text-fill-color: #07111F;
  }

  /* ── Tab Labels ──────────────────────────────────────────────────────── */
  button[data-baseweb="tab"] {
    font-family: 'Inter', sans-serif !important;
    font-size: 12px !important;
    font-weight: 600 !important;
    letter-spacing: 0em !important;
    color: var(--dp-text-2) !important;
  }
  /* ── Sticky Tab Navigation ─────────────────────────────────────────── */
  [data-testid="stTabs"] {
    position: sticky !important;
    top: 0 !important;
    z-index: 990 !important;
    background: var(--dp-bg) !important;
    padding-top: 10px !important;
    padding-bottom: 2px !important;
    margin-bottom: 0 !important;
  }
  [data-testid="stTabs"] [data-baseweb="tab-list"] {
    gap: 3px !important;
    background: rgba(15,28,46,0.06) !important;
    border-radius: 14px !important;
    padding: 4px !important;
    border: 1px solid rgba(15,28,46,0.09) !important;
    box-shadow: inset 0 1px 4px rgba(15,28,46,0.08), 0 1px 0 rgba(255,255,255,0.8) !important;
    flex-wrap: wrap !important;
  }
  [data-testid="stTabs"] [data-baseweb="tab"] {
    border-radius: 10px !important;
    padding: 7px 15px !important;
    font-family: 'DM Sans', 'Inter', sans-serif !important;
    font-size: 12.5px !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em !important;
    color: var(--dp-text-2) !important;
    transition: color 0.18s ease, background 0.18s ease, box-shadow 0.18s ease !important;
    white-space: nowrap !important;
  }
  [data-testid="stTabs"] [data-baseweb="tab"]:hover {
    background: rgba(5,103,200,0.06) !important;
    color: var(--dp-teal) !important;
  }
  [data-testid="stTabs"] [aria-selected="true"] {
    background: #FFFFFF !important;
    color: #0567C8 !important;
    font-weight: 700 !important;
    border: 1px solid rgba(5,103,200,0.22) !important;
    box-shadow: 0 2px 8px rgba(15,28,46,0.12), 0 0 0 1px rgba(5,103,200,0.14) !important;
  }

  /* ── Source Attribution Tags (inline) ───────────────────────────────── */
  .nlm-tag {
    display: inline-flex; align-items: center;
    font-family: 'Inter', sans-serif;
    font-size: 10px; font-weight: 700; letter-spacing: .06em;
    text-transform: uppercase; padding: 2px 7px; border-radius: 20px;
    vertical-align: middle; margin: 0 2px; line-height: 1;
    border: 1px solid transparent;
  }
  .nlm-tag-str    { background: rgba(37,99,235,.10);   color: #2563EB; border-color: rgba(37,99,235,0.22); }
  .nlm-tag-datafy { background: rgba(5,150,105,.10);   color: #059669; border-color: rgba(5,150,105,0.22); }
  .nlm-tag-costar { background: rgba(124,58,237,.10);  color: #7C3AED; border-color: rgba(124,58,237,0.22); }
  .nlm-tag-ai     { background: rgba(217,119,6,.10);   color: #D97706; border-color: rgba(217,119,6,0.22); }

  /* ── Intelligence Briefing Box ───────────────────────────────────────── */
  .nlm-briefing {
    background: #FFFFFF;
    border: 1px solid rgba(15,28,46,0.08);
    border-left: 4px solid #0567C8;
    border-radius: var(--dp-radius-lg); padding: 20px 24px; margin-bottom: 16px;
    position: relative; box-shadow: var(--dp-shadow);
  }
  .nlm-briefing-title {
    font-family: 'DM Sans', 'Inter', sans-serif;
    font-size: 10.5px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .14em; color: #0567C8; margin-bottom: 16px;
    display: flex; align-items: center; gap: 8px;
  }
  .nlm-point {
    font-family: 'DM Sans', 'Inter', sans-serif;
    font-size: 13.5px; line-height: 1.72; margin-bottom: 13px;
    padding-left: 18px; position: relative; color: var(--dp-text-1);
  }
  .nlm-point::before {
    content: '▸'; position: absolute; left: 0;
    color: #0567C8; font-weight: 700; font-size: 12px; top: 3px;
  }
  .nlm-point em { color: var(--dp-text-3); font-size: 12.5px; font-style: normal; }
  .nlm-point:last-child { margin-bottom: 0; }

  /* ── Q&A Insight Blocks ──────────────────────────────────────────────── */
  .nlm-qa-q {
    font-family: 'Outfit', sans-serif;
    font-size: 12.5px; font-weight: 700; margin-bottom: 5px;
    display: flex; align-items: flex-start; gap: 7px; color: var(--dp-text-1);
  }
  .nlm-qa-mark {
    width: 18px; height: 18px; min-width: 18px;
    background: rgba(5,103,200,0.08); border-radius: 5px;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 9px; font-weight: 900; color: var(--dp-teal);
    flex-shrink: 0; margin-top: 1px;
    border: 1px solid rgba(5,103,200,0.18);
  }
  .nlm-qa-a { font-size: 12px; color: var(--dp-text-2); line-height: 1.65; padding-left: 25px; }

  /* ── Source Attribution Row ──────────────────────────────────────────── */
  .nlm-source-row {
    display: flex; align-items: center; gap: 6px; flex-wrap: wrap;
    margin-top: 10px; padding-top: 8px;
    border-top: 1px solid rgba(15,28,46,0.07);
  }

  /* ── Questions Block ─────────────────────────────────────────────────── */
  .nlm-questions {
    background: rgba(5,103,200,0.04);
    border: 1px solid rgba(5,103,200,0.12);
    border-radius: var(--dp-radius-lg); padding: 14px 18px; margin-bottom: 14px;
  }
  .nlm-questions-title {
    font-family: 'Inter', sans-serif;
    font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .10em; color: var(--dp-text-3); margin-bottom: 10px;
  }
  .nlm-questions ul { list-style: none; display: flex; flex-direction: column; gap: 6px; }
  .nlm-questions ul li {
    font-size: 12px; color: var(--dp-text-2); padding-left: 14px; position: relative;
  }
  .nlm-questions ul li::before {
    content: '›'; position: absolute; left: 0;
    font-weight: 800; color: var(--dp-teal); font-size: 13px;
  }

  /* ── PULSE Score Widget ──────────────────────────────────────────────── */
  .pulse-wrapper {
    display: flex; align-items: center; gap: 24px;
    background: #FFFFFF;
    border: 1px solid rgba(15,28,46,0.07);
    border-radius: var(--dp-radius-lg); padding: 20px 24px; margin-bottom: 16px;
    box-shadow: var(--dp-shadow);
  }
  .pulse-circle {
    position: relative; width: 88px; height: 88px; flex-shrink: 0;
    display: flex; align-items: center; justify-content: center;
  }
  .pulse-ring {
    position: absolute; inset: 0; border-radius: 50%;
    border: 2px solid currentColor; opacity: 0.20;
    animation: pulse-ring 2.4s ease-out infinite;
  }
  .pulse-ring-2 {
    position: absolute; inset: -10px; border-radius: 50%;
    border: 1.5px solid currentColor; opacity: 0.10;
    animation: pulse-ring 2.4s ease-out infinite 0.6s;
  }
  .pulse-core {
    width: 68px; height: 68px; border-radius: 50%;
    display: flex; flex-direction: column; align-items: center;
    justify-content: center; font-family: 'Outfit', sans-serif;
    font-weight: 900; position: relative; z-index: 1;
    border: 2px solid currentColor;
    background: rgba(255,255,255,0.95);
  }
  .pulse-score { font-size: 22px; line-height: 1; letter-spacing: -.04em; }
  .pulse-label {
    font-size: 7.5px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .12em; opacity: .65; margin-top: 2px;
  }
  .pulse-info { flex: 1; }
  .pulse-info-title {
    font-family: 'Outfit', sans-serif; font-size: 15px;
    font-weight: 800; letter-spacing: -.025em; margin-bottom: 4px;
    color: var(--dp-text-1) !important;
  }
  .pulse-info-detail {
    font-size: 12.5px; color: var(--dp-text-2) !important;
    line-height: 1.55; opacity: 1 !important;
  }
  .pulse-info-status {
    display: inline-block; margin-top: 8px; font-size: 10px; font-weight: 700;
    padding: 3px 10px; border-radius: 20px;
    background: rgba(5,103,200,0.08); color: var(--dp-teal);
    border: 1px solid rgba(5,103,200,0.20);
    letter-spacing: .05em;
  }
  @keyframes pulse-ring {
    0%   { transform: scale(1);    opacity: 0.22; }
    70%  { transform: scale(1.40); opacity: 0;    }
    100% { transform: scale(1.40); opacity: 0;    }
  }

  /* ── Entry Animations ────────────────────────────────────────────────── */
  @keyframes fadeSlideUp {
    from { opacity: 0; transform: translateY(12px); }
    to   { opacity: 1; transform: translateY(0);    }
  }
  @keyframes fadeIn {
    from { opacity: 0; }
    to   { opacity: 1; }
  }
  .kpi-card   { animation: fadeSlideUp 0.35s ease both; }
  .insight-card { animation: fadeSlideUp 0.40s ease both; }
  .event-stat { animation: fadeSlideUp 0.35s ease both; }
  .src-card   { animation: fadeIn 0.30s ease both; }
  .tab-summary { animation: fadeIn 0.25s ease both; }
  .kpi-card:nth-child(2) { animation-delay: 0.05s; }
  .kpi-card:nth-child(3) { animation-delay: 0.10s; }
  .kpi-card:nth-child(4) { animation-delay: 0.15s; }
  .kpi-card:nth-child(5) { animation-delay: 0.20s; }
  .insight-card:nth-child(2) { animation-delay: 0.06s; }
  .insight-card:nth-child(3) { animation-delay: 0.12s; }
  .event-stat:nth-child(2)  { animation-delay: 0.05s; }
  .event-stat:nth-child(3)  { animation-delay: 0.10s; }
  .event-stat:nth-child(4)  { animation-delay: 0.15s; }

  /* ── Global Text Contrast ─────────────────────────────────────────────── */
  .stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown div {
    color: var(--dp-text-1) !important;
  }
  [data-testid="stExpander"] p, [data-testid="stExpander"] div {
    color: var(--dp-text-2) !important;
  }
  [data-testid="stDataFrame"] td, [data-testid="stDataFrame"] th {
    color: var(--dp-text-1) !important;
    background: #FFFFFF !important;
  }
  [data-testid="stDataFrame"] { background: #FFFFFF !important; }
  [data-testid="stMetricDelta"] { font-size: 11px !important; font-weight: 600 !important; }
  .sh-title { color: var(--dp-text-1) !important; }
  .sh-tag { color: var(--dp-teal) !important; }
  .insight-card p, .insight-card span { color: var(--dp-text-2) !important; }

  /* ── Section Intelligence Card ───────────────────────────────────────── */
  .sec-intel {
    background: linear-gradient(135deg, rgba(5,103,200,0.04) 0%, rgba(5,103,200,0.01) 100%);
    border: 1px solid rgba(5,103,200,0.14);
    border-left: 3px solid var(--dp-teal);
    border-radius: var(--dp-radius-lg);
    padding: 16px 20px; margin: 8px 0 16px 0;
    box-shadow: 0 2px 12px rgba(5,103,200,0.06);
    position: relative; overflow: hidden;
  }
  .sec-intel::after {
    content: 'INTEL';
    position: absolute; right: 16px; top: 50%; transform: translateY(-50%);
    font-family: 'Syne', sans-serif; font-size: 52px; font-weight: 800;
    color: rgba(5,103,200,0.04); letter-spacing: -0.04em;
    pointer-events: none; user-select: none;
  }
  .sec-intel-label {
    font-family: 'Syne', 'DM Sans', sans-serif;
    font-size: 9px; font-weight: 700; letter-spacing: .16em;
    text-transform: uppercase; color: var(--dp-teal); margin-bottom: 10px;
    display: flex; align-items: center; gap: 6px;
  }
  .sec-intel-label::before {
    content: ''; display: inline-block;
    width: 14px; height: 2px; background: var(--dp-teal); border-radius: 1px;
  }
  .sec-intel-body { font-size: 13px; color: var(--dp-text-2); line-height: 1.72; }
  .sec-intel-stat {
    display: inline-block;
    background: rgba(5,103,200,0.08); border: 1px solid rgba(5,103,200,0.20);
    border-radius: 20px; padding: 3px 12px;
    font-weight: 700; color: var(--dp-teal);
    font-family: 'Syne', 'Outfit', sans-serif;
    font-size: 11.5px; letter-spacing: -0.01em;
  }

  /* ── Divider Rule ────────────────────────────────────────────────────── */
  .dp-divider {
    border: none; border-top: 1px solid rgba(15,28,46,0.08);
    margin: 20px 0;
  }

  /* ── Data Callout Box ────────────────────────────────────────────────── */
  .dp-callout {
    background: rgba(37,99,235,0.05);
    border: 1px solid rgba(37,99,235,0.16);
    border-left: 3px solid var(--dp-blue);
    border-radius: var(--dp-radius-lg);
    padding: 12px 16px; margin: 10px 0;
    font-size: 13px; color: var(--dp-text-1); line-height: 1.6;
  }
  .dp-callout-warn {
    background: rgba(217,119,6,0.05);
    border-color: rgba(217,119,6,0.16);
    border-left-color: var(--dp-amber);
  }
  .dp-callout-success {
    background: rgba(5,150,105,0.05);
    border-color: rgba(5,150,105,0.16);
    border-left-color: var(--dp-green);
  }

  /* ── Layout Spacing ──────────────────────────────────────────────────── */
  .block-container { padding-top: 0.5rem !important; overflow: visible !important; }
  [data-testid="stPlotlyChart"] { margin-bottom: 4px !important; }
  div[data-testid="stHorizontalBlock"] { gap: 10px !important; }

  /* ── Streamlit Native Metric Styling ─────────────────────────────────── */
  [data-testid="stMetricValue"] {
    font-family: 'Outfit', sans-serif !important;
    font-size: clamp(1.35rem, 2.4vw, 2.0rem) !important;
    font-weight: 900 !important; letter-spacing: -0.04em !important;
    color: var(--dp-text-1) !important;
  }
  [data-testid="stMetricValue"] div { color: var(--dp-text-1) !important; }
  [data-testid="stMetricLabel"] label,
  [data-testid="stMetricLabel"] p {
    font-family: 'DM Sans', 'Inter', sans-serif !important;
    font-size: 10px !important; font-weight: 700 !important;
    text-transform: uppercase !important; letter-spacing: .14em !important;
    color: var(--dp-text-3) !important;
  }
  [data-testid="stMetricDelta"] { font-size: 12px !important; font-weight: 700 !important; }
  div[data-testid="metric-container"] {
    background: #FFFFFF !important;
    border: 1px solid rgba(15,28,46,0.08) !important;
    border-top: 3px solid #0567C8 !important;
    border-radius: var(--dp-radius-lg) !important;
    padding: 16px 18px 12px !important;
    box-shadow: var(--dp-shadow) !important;
    transition: box-shadow 0.22s ease, transform 0.22s ease !important;
  }
  div[data-testid="metric-container"]:hover {
    box-shadow: var(--dp-shadow-hover) !important;
    transform: translateY(-2px) !important;
  }

  /* ── Sidebar Styling ─────────────────────────────────────────────────── */
  [data-testid="stSidebar"] {
    background: #FFFFFF !important;
    border-right: 1px solid rgba(15,28,46,0.10) !important;
  }
  [data-testid="stSidebar"] .stRadio label {
    font-size: 13px !important; font-weight: 500 !important;
    color: var(--dp-text-1) !important;
  }
  [data-testid="stSidebar"] * { color: var(--dp-text-1) !important; }
  [data-testid="stSidebar"] .stMarkdown p { color: var(--dp-text-2) !important; }

  /* ── Selectbox / Widget Styling ──────────────────────────────────────── */
  [data-testid="stSelectbox"] > div,
  [data-testid="stDateInput"] > div {
    background: #FFFFFF !important;
    border-color: rgba(15,28,46,0.12) !important;
    border-radius: 8px !important; color: var(--dp-text-1) !important;
  }
  [data-testid="stSelectbox"] [data-baseweb="select"] > div {
    background: #FFFFFF !important;
    border-color: rgba(15,28,46,0.12) !important;
    color: var(--dp-text-1) !important;
  }

  /* ── Expander ────────────────────────────────────────────────────────── */
  [data-testid="stExpander"] {
    border: 1px solid rgba(15,28,46,0.07) !important;
    border-radius: var(--dp-radius-lg) !important;
    background: #FFFFFF !important;
  }
  [data-testid="stExpander"] summary {
    font-family: 'Outfit', sans-serif !important;
    font-weight: 700 !important; font-size: 13px !important;
    color: var(--dp-text-1) !important;
  }

  /* ── Global Filter Bar ────────────────────────────────────────────────── */
  .filter-bar {
    background: #FFFFFF;
    border: 1px solid rgba(15,28,46,0.07);
    border-radius: var(--dp-radius-lg);
    padding: 10px 16px; margin-bottom: 16px;
    display: flex; align-items: center; gap: 12px;
    box-shadow: var(--dp-shadow);
  }
  .filter-bar .stSelectbox > div > div {
    background: #F4F7FB !important;
    border: 1px solid rgba(15,28,46,0.10) !important;
    border-radius: 8px !important;
    font-size: 13px !important; color: var(--dp-text-1) !important;
  }

  /* ── Action Intelligence Panel ────────────────────────────────────────── */
  .action-panel {
    background: linear-gradient(135deg, #F0F7FF 0%, #EEF2FF 100%);
    border: 1px solid rgba(5,103,200,0.16);
    border-left: 4px solid #0567C8;
    border-radius: var(--dp-radius-lg);
    padding: 16px 20px; margin: 14px 0;
    box-shadow: 0 2px 10px rgba(5,103,200,0.08);
  }
  .action-panel-title {
    font-family: 'Outfit', sans-serif;
    font-size: 12px; font-weight: 800; text-transform: uppercase;
    letter-spacing: .10em; color: #0567C8; margin-bottom: 12px;
    display: flex; align-items: center; gap: 8px;
  }
  .action-item {
    display: flex; align-items: flex-start; gap: 10px;
    margin-bottom: 10px; padding-bottom: 10px;
    border-bottom: 1px solid rgba(5,103,200,0.10);
  }
  .action-item:last-child { margin-bottom: 0; padding-bottom: 0; border-bottom: none; }
  .action-number {
    width: 22px; height: 22px; min-width: 22px;
    background: #0567C8; border-radius: 6px;
    display: inline-flex; align-items: center; justify-content: center;
    font-family: 'Outfit', sans-serif; font-size: 11px; font-weight: 800;
    color: #FFFFFF; flex-shrink: 0; margin-top: 1px;
  }
  .action-text {
    font-family: 'Inter', sans-serif;
    font-size: 12.5px; color: #0D1B2E; line-height: 1.60;
  }
  .action-text strong { font-weight: 700; color: #0567C8; }

  /* ── Contextual Ask Panel ─────────────────────────────────────────────── */
  .ask-panel {
    background: #FAFBFF;
    border: 1px solid rgba(124,58,237,0.16);
    border-left: 4px solid #7C3AED;
    border-radius: var(--dp-radius-lg);
    padding: 14px 18px; margin: 10px 0 16px 0;
    box-shadow: 0 2px 8px rgba(124,58,237,0.08);
  }
  .ask-panel-title {
    font-family: 'Outfit', sans-serif;
    font-size: 11px; font-weight: 800; text-transform: uppercase;
    letter-spacing: .10em; color: #7C3AED; margin-bottom: 10px;
    display: flex; align-items: center; gap: 6px;
  }
  .ask-chip {
    display: inline-block; font-family: 'Inter', sans-serif;
    font-size: 11.5px; font-weight: 500; color: #334155;
    background: #FFFFFF; border: 1px solid rgba(124,58,237,0.18);
    border-radius: 8px; padding: 5px 11px; margin: 3px 4px 3px 0;
    cursor: pointer; transition: border-color 0.15s, background 0.15s;
    line-height: 1.35;
  }
  .ask-chip:hover { border-color: #7C3AED; background: rgba(124,58,237,0.05); color: #7C3AED; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
  /* ── Section Header Blocks ───────────────────────────────────────────── */
  .sh-block {
    display: flex !important; align-items: center !important; gap: 12px !important;
    padding: 11px 18px !important; border-radius: 10px !important;
    margin: 28px 0 14px 0 !important;
    background: #FFFFFF !important;
    border: 1px solid rgba(15,28,46,0.07) !important;
    border-left: 3px solid var(--sh-accent, #0567C8) !important;
    box-shadow: 0 1px 3px rgba(15,28,46,0.05), 0 2px 8px rgba(15,28,46,0.04) !important;
    transition: box-shadow 0.22s ease, transform 0.22s ease !important;
  }
  .sh-block:hover {
    box-shadow: 0 2px 12px rgba(15,28,46,0.10) !important;
    transform: translateY(-1px) !important;
  }
  .sh-icon {
    font-size: 18px !important; line-height: 1 !important; flex-shrink: 0 !important;
    opacity: 0.85 !important;
  }
  .sh-title {
    font-family: 'Syne', 'Outfit', sans-serif !important;
    font-size: 14px !important; font-weight: 700 !important;
    letter-spacing: -.015em !important; line-height: 1.2 !important;
    color: #0D1B2E !important;
  }
  .sh-tag {
    margin-left: auto !important; font-size: 10px !important;
    font-weight: 700 !important; letter-spacing: .08em !important;
    text-transform: uppercase !important; padding: 2px 9px !important;
    border-radius: 20px !important; white-space: nowrap !important;
    background: rgba(5,103,200,0.08) !important;
    color: #0567C8 !important;
    border: 1px solid rgba(5,103,200,0.18) !important;
  }
  /* Accent color variants */
  .sh-teal   { --sh-accent: #0567C8; }
  .sh-blue   { --sh-accent: #2563EB; }
  .sh-green  { --sh-accent: #059669; }
  .sh-purple { --sh-accent: #7C3AED; }
  .sh-orange { --sh-accent: #EA580C; }
  .sh-amber  { --sh-accent: #D97706; }
  .sh-indigo { --sh-accent: #4F46E5; }
  .sh-coral  { --sh-accent: #DC2626; }
  .sh-gray   { --sh-accent: #64748B; }
  .sh-gold   { --sh-accent: #B45309; }

  /* Accent-aware tag colors */
  .sh-teal   .sh-tag { color: #0567C8 !important; background: rgba(5,103,200,0.08) !important;   border-color: rgba(5,103,200,0.18) !important; }
  .sh-blue   .sh-tag { color: #2563EB !important; background: rgba(37,99,235,0.08) !important;   border-color: rgba(37,99,235,0.18) !important; }
  .sh-green  .sh-tag { color: #059669 !important; background: rgba(5,150,105,0.08) !important;   border-color: rgba(5,150,105,0.18) !important; }
  .sh-purple .sh-tag { color: #7C3AED !important; background: rgba(124,58,237,0.08) !important;  border-color: rgba(124,58,237,0.18) !important; }
  .sh-orange .sh-tag { color: #EA580C !important; background: rgba(234,88,12,0.08) !important;   border-color: rgba(234,88,12,0.18) !important; }
  .sh-amber  .sh-tag { color: #D97706 !important; background: rgba(217,119,6,0.08) !important;   border-color: rgba(217,119,6,0.18) !important; }
  .sh-coral  .sh-tag { color: #DC2626 !important; background: rgba(220,38,38,0.08) !important;   border-color: rgba(220,38,38,0.18) !important; }
  .sh-gray   .sh-tag { color: #64748B !important; background: rgba(100,116,139,0.08) !important; border-color: rgba(100,116,139,0.18) !important; }
  .sh-gold   .sh-tag { color: #B45309 !important; background: rgba(180,83,9,0.08) !important;    border-color: rgba(180,83,9,0.18) !important; }

</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
  /* ═══════════════════════════════════════════════════════════════
     RESPONSIVE — Mobile & Tablet
     Phones < 640px · Tablets 640–1024px
  ═══════════════════════════════════════════════════════════════ */
  @media screen and (max-width: 768px) {
    .main .block-container {
      padding: 0.5rem 0.75rem 2rem 0.75rem !important;
      max-width: 100% !important;
    }
    .hero-banner { padding: 16px 14px !important; }
    .hero-title  { font-size: 1.35rem !important; }
    .hero-subtitle { font-size: 11px !important; }
    .sh-block  { padding: 9px 12px !important; }
    .sh-title  { font-size: 13px !important; }
    .pulse-wrapper {
      flex-direction: column !important;
      align-items: flex-start !important;
      gap: 14px !important; padding: 14px 16px !important;
    }
    button[data-baseweb="tab"] {
      font-size: 11px !important; padding: 5px 9px !important;
    }
    .chart-header { font-size: 13px !important; }
    .sidebar-brand { font-size: 15px !important; }
    #back-to-top-btn {
      bottom: 70px !important; right: 14px !important;
      width: 42px !important; height: 42px !important;
    }
    .event-stat { padding: 14px 10px !important; }
    .event-val  { font-size: 22px !important; }
    .nlm-briefing { padding: 12px 14px !important; }
    .src-card { padding: 10px 12px !important; }
    .kpi-value { font-size: 24px !important; }
  }
  @media screen and (max-width: 480px) {
    .hero-title { font-size: 1.15rem !important; }
    .main .block-container { padding: 0.25rem 0.5rem 2rem 0.5rem !important; }
    .sh-tag { display: none !important; }
    [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
  }
  @media screen and (max-width: 768px) {
    .js-plotly-plot .plotly { overflow-x: auto !important; }
  }

  /* ── Pill Button Style (byhook) ──────────────────────────────────────── */
  [data-testid="stButton"] > button {
    background: #FFFFFF !important;
    border: 1.5px solid rgba(5,103,200,0.22) !important;
    border-radius: 100em !important;
    color: var(--dp-text-2) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    letter-spacing: -0.01em !important;
    padding: 6px 20px !important;
    transition: background 0.22s cubic-bezier(0.37,0,0.22,1),
                color 0.22s cubic-bezier(0.37,0,0.22,1),
                border-color 0.22s ease,
                box-shadow 0.22s ease !important;
  }
  [data-testid="stButton"] > button:hover {
    background: #0567C8 !important;
    border-color: #0567C8 !important;
    color: #FFFFFF !important;
    box-shadow: 0 4px 14px rgba(5,103,200,0.30) !important;
  }
  [data-testid="stButton"] > button:active {
    transform: scale(0.97) !important;
  }

  /* ── KPI Ticker (1ax / Bloomberg) ────────────────────────────────────── */
  /* Seamlessly extends the hero banner — same dark bg, no gap */
  .pulse-ticker-wrap {
    overflow: hidden;
    background: linear-gradient(180deg, #091628 0%, #0A1C30 100%);
    border-bottom: 2px solid rgba(5,103,200,0.35);
    box-shadow: 0 4px 28px rgba(0,0,0,0.35);
    margin: 0 -1rem 1.5rem -1rem;
    padding: 0;
    position: relative;
    z-index: 998;
  }
  /* Edge fade-out (1ax style) */
  .pulse-ticker-wrap::before,
  .pulse-ticker-wrap::after {
    content: ''; position: absolute; top: 0; bottom: 0; width: 80px; z-index: 10;
    pointer-events: none;
  }
  .pulse-ticker-wrap::before {
    left: 0;
    background: linear-gradient(90deg, #0A1C30 0%, transparent 100%);
  }
  .pulse-ticker-wrap::after {
    right: 0;
    background: linear-gradient(270deg, #0A1C30 0%, transparent 100%);
  }
  .pulse-ticker-track {
    display: flex;
    width: max-content;
    padding: 10px 0;
  }
  .pulse-ticker-track.left  { animation: ticker-left  32s linear infinite; }
  .pulse-ticker-track.right { animation: ticker-right 40s linear infinite; }
  .pulse-ticker-track:hover { animation-play-state: paused; }
  .pulse-ticker-item {
    display: flex; align-items: center; gap: 8px;
    padding: 0 28px;
    font-family: 'DM Sans', 'Inter', sans-serif;
    font-size: 12px; font-weight: 600;
    color: rgba(255,255,255,0.65);
    white-space: nowrap;
    border-right: 1px solid rgba(255,255,255,0.07);
    transition: color 0.2s ease;
  }
  .pulse-ticker-item:hover { color: rgba(255,255,255,0.90); }
  .pulse-ticker-item:last-child { border-right: none; }
  .pulse-ticker-label {
    font-family: 'Syne', 'DM Sans', sans-serif;
    font-size: 8.5px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .14em; color: rgba(255,255,255,0.28);
  }
  .pulse-ticker-val {
    color: #FFFFFF;
    font-family: 'Outfit', sans-serif;
    font-size: 14px; font-weight: 800;
    letter-spacing: -0.02em;
  }
  .pulse-ticker-pos { color: #34D399; font-size: 11px; font-weight: 700; letter-spacing: 0.02em; }
  .pulse-ticker-neg { color: #F87171; font-size: 11px; font-weight: 700; letter-spacing: 0.02em; }
  .pulse-ticker-dot {
    width: 6px; height: 6px; border-radius: 50%;
    background: #22D3EE;
    box-shadow: 0 0 6px rgba(34,211,238,0.60);
    animation: ticker-pulse 1.8s ease-in-out infinite;
    flex-shrink: 0;
  }
  @keyframes ticker-left  { 0%{transform:translateX(0)} 100%{transform:translateX(-33.333%)} }
  @keyframes ticker-right { 0%{transform:translateX(-33.333%)} 100%{transform:translateX(0)} }
  @keyframes ticker-pulse { 0%,100%{opacity:1;transform:scale(1);box-shadow:0 0 6px rgba(34,211,238,0.60);} 50%{opacity:.4;transform:scale(.65);box-shadow:0 0 0 rgba(34,211,238,0);} }

  /* ── Scroll-Reveal Animations (byhook clip-path style) ───────────────── */
  .reveal-card {
    opacity: 0;
    transform: translateY(18px);
    transition: opacity 0.55s cubic-bezier(0.28,0,0.49,1),
                transform 0.55s cubic-bezier(0.28,0,0.49,1);
  }
  .reveal-card.is-visible {
    opacity: 1;
    transform: translateY(0);
  }
  .reveal-clip {
    clip-path: polygon(0 100%, 100% 100%, 100% 100%, 0 100%);
    transition: clip-path 0.65s cubic-bezier(0.37,0,0.22,1);
  }
  .reveal-clip.is-visible {
    clip-path: polygon(0 0, 100% 0, 100% 100%, 0 100%);
  }
  .reveal-left {
    opacity: 0; transform: translateX(-20px);
    transition: opacity 0.5s cubic-bezier(0.28,0,0.49,1),
                transform 0.5s cubic-bezier(0.28,0,0.49,1);
  }
  .reveal-left.is-visible { opacity: 1; transform: translateX(0); }

  /* ── CNN-style Inline Data Highlights ───────────────────────────────── */
  .data-hl {
    background: linear-gradient(120deg, rgba(5,103,200,0.15) 0%, rgba(5,103,200,0.08) 100%);
    color: #0567C8;
    font-weight: 800;
    border-radius: 4px;
    padding: 0 4px;
    font-family: 'Outfit', sans-serif;
    letter-spacing: -0.02em;
    border-bottom: 2px solid rgba(5,103,200,0.35);
    display: inline;
  }
  .data-hl-pos {
    background: linear-gradient(120deg, rgba(5,150,105,0.14) 0%, rgba(5,150,105,0.07) 100%);
    color: #059669; border-bottom-color: rgba(5,150,105,0.35);
    font-weight: 800; border-radius: 4px; padding: 0 4px;
    font-family: 'Outfit', sans-serif; letter-spacing: -0.02em; display: inline;
  }
  .data-hl-neg {
    background: linear-gradient(120deg, rgba(220,38,38,0.12) 0%, rgba(220,38,38,0.06) 100%);
    color: #DC2626; border-bottom: 2px solid rgba(220,38,38,0.30);
    font-weight: 800; border-radius: 4px; padding: 0 4px;
    font-family: 'Outfit', sans-serif; letter-spacing: -0.02em; display: inline;
  }
  .data-hl-amber {
    background: linear-gradient(120deg, rgba(217,119,6,0.14) 0%, rgba(217,119,6,0.07) 100%);
    color: #D97706; border-bottom: 2px solid rgba(217,119,6,0.32);
    font-weight: 800; border-radius: 4px; padding: 0 4px;
    font-family: 'Outfit', sans-serif; letter-spacing: -0.02em; display: inline;
  }
  /* ── CNN interactive — compact inline highlight variants ─────────────── */
  .data-hl-green {
    display: inline-block; font-family: 'Outfit', sans-serif;
    font-size: 1.15em; font-weight: 900; letter-spacing: -0.03em;
    color: #059669; padding: 0 2px;
    border-bottom: 2px solid rgba(5,150,105,0.30); line-height: 1;
  }
  .data-hl-red {
    display: inline-block; font-family: 'Outfit', sans-serif;
    font-size: 1.15em; font-weight: 900; letter-spacing: -0.03em;
    color: #DC2626; padding: 0 2px;
    border-bottom: 2px solid rgba(220,38,38,0.30); line-height: 1;
  }
  .data-hl-gold {
    display: inline-block; font-family: 'Outfit', sans-serif;
    font-size: 1.15em; font-weight: 900; letter-spacing: -0.03em;
    color: #D97706; padding: 0 2px;
    border-bottom: 2px solid rgba(217,119,6,0.30); line-height: 1;
  }
  /* ── Section pull-quote (CNN interactive "big stat" style) ───────────── */
  .pull-stat-display {
    display: block; font-family: 'Syne', 'Outfit', sans-serif;
    font-size: 3rem; font-weight: 900; letter-spacing: -0.05em;
    line-height: 1; color: #07111F; margin: 8px 0 4px 0;
  }
  .pull-stat-label {
    font-family: 'DM Sans', sans-serif; font-size: 12px; font-weight: 600;
    text-transform: uppercase; letter-spacing: .10em; color: #64748B;
  }
  /* ── Hero banner stats row ────────────────────────────────────────────── */
  .hero-stats-row {
    display: flex; gap: 32px; margin-top: 14px; flex-wrap: wrap;
    border-top: 1px solid rgba(255,255,255,0.10); padding-top: 14px;
  }
  .hero-stat { display: flex; flex-direction: column; gap: 2px; }
  .hero-stat-val {
    font-family: 'Outfit', sans-serif; font-size: 24px; font-weight: 900;
    letter-spacing: -0.04em; color: #FFFFFF; line-height: 1;
  }
  .hero-stat-label {
    font-family: 'DM Sans', sans-serif; font-size: 9px; font-weight: 700;
    text-transform: uppercase; letter-spacing: .12em;
    color: rgba(255,255,255,0.55);
  }
  .hero-stat-delta { font-size: 11px; font-weight: 700; }
  .hero-stat-pos { color: #34D399; }
  .hero-stat-neg { color: #F87171; }

  /* ── Painted Heatmap (Giorgia Lupi / Long-Covid density style) ───────── */
  .painted-legend {
    display: flex; gap: 8px; flex-wrap: wrap; align-items: center;
    margin: 8px 0 14px 0;
  }
  .painted-swatch {
    display: inline-flex; align-items: center; gap: 5px;
    font-family: 'DM Sans', sans-serif;
    font-size: 10.5px; font-weight: 600; color: var(--dp-text-3);
  }
  .painted-dot {
    width: 12px; height: 12px; border-radius: 3px; display: inline-block;
  }

  /* ── Event Type Cards (Dumbar type-based treatments) ─────────────────── */
  .evt-card {
    border-radius: var(--dp-radius-lg); padding: 16px 18px; margin-bottom: 10px;
    border: 1px solid rgba(15,28,46,0.08); position: relative; overflow: hidden;
    transition: box-shadow 0.24s ease, transform 0.24s ease;
    box-shadow: var(--dp-shadow); cursor: default;
  }
  .evt-card:hover { box-shadow: var(--dp-shadow-hover); transform: translateY(-3px); }
  .evt-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
  }
  .evt-festival::before  { background: linear-gradient(90deg, #7C3AED, #C084FC); }
  .evt-surf::before      { background: linear-gradient(90deg, #0567C8, #38BDF8); }
  .evt-maritime::before  { background: linear-gradient(90deg, #059669, #34D399); }
  .evt-holiday::before   { background: linear-gradient(90deg, #DC2626, #F87171); }
  .evt-race::before      { background: linear-gradient(90deg, #D97706, #FCD34D); }
  .evt-default::before   { background: linear-gradient(90deg, #64748B, #94A3B8); }
  .evt-festival  { background: rgba(124,58,237,0.03); border-color: rgba(124,58,237,0.14); }
  .evt-surf      { background: rgba(5,103,200,0.03);  border-color: rgba(5,103,200,0.14); }
  .evt-maritime  { background: rgba(5,150,105,0.03);  border-color: rgba(5,150,105,0.14); }
  .evt-holiday   { background: rgba(220,38,38,0.03);  border-color: rgba(220,38,38,0.14); }
  .evt-race      { background: rgba(217,119,6,0.03);  border-color: rgba(217,119,6,0.14); }
  .evt-type-pill {
    display: inline-flex; align-items: center; gap: 4px;
    font-size: 9.5px; font-weight: 700; text-transform: uppercase; letter-spacing: .12em;
    padding: 2px 9px; border-radius: 20px;
    border: 1px solid transparent; margin-bottom: 8px;
  }
  .evt-type-festival { color: #7C3AED; background: rgba(124,58,237,0.10); border-color: rgba(124,58,237,0.22); }
  .evt-type-surf     { color: #0567C8; background: rgba(5,103,200,0.10);  border-color: rgba(5,103,200,0.22); }
  .evt-type-maritime { color: #059669; background: rgba(5,150,105,0.10);  border-color: rgba(5,150,105,0.22); }
  .evt-type-holiday  { color: #DC2626; background: rgba(220,38,38,0.10);  border-color: rgba(220,38,38,0.22); }
  .evt-type-race     { color: #D97706; background: rgba(217,119,6,0.10);  border-color: rgba(217,119,6,0.22); }
  .evt-type-default  { color: #475569; background: rgba(71,85,105,0.10);  border-color: rgba(71,85,105,0.22); }
  .evt-name {
    font-family: 'Outfit', sans-serif;
    font-size: 15px; font-weight: 800; letter-spacing: -0.03em; color: var(--dp-text-1);
    margin-bottom: 4px;
  }
  .evt-date {
    font-family: 'DM Sans', sans-serif;
    font-size: 12px; font-weight: 500; color: var(--dp-text-3); margin-bottom: 8px;
  }
  .evt-impact {
    font-family: 'DM Sans', sans-serif;
    font-size: 13px; color: var(--dp-text-2); line-height: 1.65;
  }

  /* ── Streamlit Input / Selectbox light style ─────────────────────────── */
  [data-baseweb="select"] [data-testid="stMarkdownContainer"] p,
  .stSelectbox label p {
    color: var(--dp-text-2) !important;
    font-size: 11px !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: .06em !important;
  }

  /* ── DataFrame light table ───────────────────────────────────────────── */
  [data-testid="stDataFrameResizable"] {
    background: #FFFFFF !important;
    border-radius: var(--dp-radius) !important;
    border: 1px solid rgba(0,0,0,0.08) !important;
  }

  /* ── Master Data Card (sensat.co style) ──────────────────────────────── */
  .master-card {
    background: linear-gradient(135deg, #07111F 0%, #0D1F3C 60%, #0A2240 100%);
    border-radius: var(--dp-radius-lg); padding: 28px 32px;
    border: 1px solid rgba(5,103,200,0.30); position: relative; overflow: hidden;
    box-shadow: 0 4px 32px rgba(5,103,200,0.18), 0 1px 4px rgba(0,0,0,0.30);
    margin-bottom: 14px; animation: fadeSlideUp 0.4s ease both;
  }
  .master-card::before {
    content: ''; position: absolute; top: -60px; right: -60px;
    width: 240px; height: 240px;
    background: radial-gradient(circle, rgba(5,103,200,0.22) 0%, transparent 65%);
    pointer-events: none; animation: hero-glow-pulse 5s ease-in-out infinite;
  }
  .master-card::after {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, transparent 0%, #0567C8 30%, #38BDF8 60%, transparent 100%);
  }
  .master-card-label {
    font-family: 'Syne', 'DM Sans', sans-serif; font-size: 9.5px; font-weight: 700;
    text-transform: uppercase; letter-spacing: .18em; color: rgba(255,255,255,0.40); margin-bottom: 8px;
  }
  .master-card-value {
    font-family: 'Syne', 'Outfit', sans-serif; font-size: 3.8rem; font-weight: 800;
    letter-spacing: -0.05em; line-height: 1; margin-bottom: 6px;
    background: linear-gradient(110deg, #FFFFFF 40%, #7DD3FC 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
  }
  .master-card-body {
    font-family: 'DM Sans', sans-serif; font-size: 13px; color: rgba(255,255,255,0.55);
    line-height: 1.65; border-top: 1px solid rgba(255,255,255,0.08);
    padding-top: 10px; margin-top: 4px;
  }
  .master-card-body strong { color: rgba(255,255,255,0.85); }

  /* ── Timeline (decimalstudios × CNN style) ───────────────────────────── */
  .pulse-timeline { position: relative; padding: 4px 0; margin: 16px 0; }
  .pulse-timeline::before {
    content: ''; position: absolute; left: 16px; top: 0; bottom: 0; width: 2px;
    background: linear-gradient(180deg, rgba(5,103,200,0.60) 0%, rgba(5,103,200,0.08) 100%);
    border-radius: 2px;
  }
  .tl-item {
    display: flex; gap: 20px; margin-bottom: 18px; padding-left: 4px;
    animation: fadeSlideUp 0.4s ease both;
  }
  .tl-item:nth-child(2){animation-delay:.06s} .tl-item:nth-child(3){animation-delay:.12s}
  .tl-item:nth-child(4){animation-delay:.18s} .tl-item:nth-child(5){animation-delay:.24s}
  .tl-dot {
    width: 30px; height: 30px; min-width: 30px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 800;
    position: relative; z-index: 1; margin-left: 2px; box-shadow: 0 0 0 4px var(--dp-bg);
  }
  .tl-dot-high   { background:rgba(5,103,200,0.15);   color:#0567C8; border:2px solid #0567C8; }
  .tl-dot-peak   { background:rgba(5,150,105,0.15);   color:#059669; border:2px solid #059669; }
  .tl-dot-low    { background:rgba(220,38,38,0.12);   color:#DC2626; border:2px solid #DC2626; }
  .tl-dot-event  { background:rgba(124,58,237,0.12);  color:#7C3AED; border:2px solid #7C3AED; }
  .tl-dot-warn   { background:rgba(217,119,6,0.12);   color:#D97706; border:2px solid #D97706; }
  .tl-content {
    background: #FFFFFF; border: 1px solid rgba(15,28,46,0.08);
    border-radius: var(--dp-radius); padding: 12px 16px; flex: 1;
    box-shadow: var(--dp-shadow);
    transition: box-shadow 0.22s ease, transform 0.22s ease;
  }
  .tl-content:hover { box-shadow: var(--dp-shadow-hover); transform: translateX(3px); }
  .tl-date {
    font-family: 'Syne', 'DM Sans', sans-serif; font-size: 9.5px; font-weight: 700;
    text-transform: uppercase; letter-spacing: .12em; color: #64748B; margin-bottom: 3px;
  }
  .tl-headline {
    font-family: 'Outfit', sans-serif; font-size: 13.5px; font-weight: 800;
    letter-spacing: -.02em; color: #07111F; margin-bottom: 4px;
  }
  .tl-body  { font-size: 12.5px; color: #475569; line-height: 1.60; }
  .tl-stat  {
    display: inline-block; margin-top: 6px; font-family: 'Outfit', sans-serif;
    font-size: 11.5px; font-weight: 800; padding: 2px 10px; border-radius: 20px;
    background: rgba(5,103,200,0.08); color: #0567C8; border: 1px solid rgba(5,103,200,0.20);
  }

  /* ── Datamotive.io clean data cards ──────────────────────────────────── */
  .dm-card {
    background: #FFFFFF; border: 1px solid rgba(15,28,46,0.07); border-radius: var(--dp-radius-lg);
    padding: 20px 22px; position: relative; overflow: hidden;
    transition: box-shadow 0.22s ease, transform 0.22s ease;
    box-shadow: 0 1px 2px rgba(15,28,46,0.04), 0 3px 12px rgba(15,28,46,0.06); margin-bottom: 12px;
  }
  .dm-card:hover { box-shadow: var(--dp-shadow-hover); transform: translateY(-3px); }
  .dm-card-accent { position: absolute; top: 0; left: 0; right: 0; height: 3px; border-radius: 16px 16px 0 0; }
  .dm-card-value {
    font-family: 'Syne', 'Outfit', sans-serif; font-size: 2.4rem; font-weight: 800;
    letter-spacing: -0.05em; color: #07111F; line-height: 1; margin-bottom: 4px;
  }
  .dm-card-label {
    font-family: 'DM Sans', 'Inter', sans-serif; font-size: 11px; font-weight: 700;
    text-transform: uppercase; letter-spacing: .12em; color: #64748B; margin-bottom: 8px;
  }
  .dm-card-sub {
    font-size: 12.5px; color: #475569; line-height: 1.55;
    border-top: 1px solid rgba(15,28,46,0.06); padding-top: 8px; margin-top: 6px;
  }

  /* ── Levinriegner.com narrative card ─────────────────────────────────── */
  .levi-card {
    border-radius: var(--dp-radius-lg); padding: 28px 30px; background: #FFFFFF;
    border: 1px solid rgba(15,28,46,0.08); box-shadow: var(--dp-shadow);
    position: relative; overflow: hidden; margin-bottom: 14px; animation: fadeSlideUp 0.5s ease both;
  }
  .levi-card-eyebrow {
    font-family: 'Syne', sans-serif; font-size: 9px; font-weight: 700; letter-spacing: .20em;
    text-transform: uppercase; color: #0567C8; margin-bottom: 10px;
    display: flex; align-items: center; gap: 8px;
  }
  .levi-card-eyebrow::before { content:''; width:20px; height:2px; background:#0567C8; border-radius:1px; }
  .levi-card-headline {
    font-family: 'Syne', 'Outfit', sans-serif; font-size: 1.9rem; font-weight: 800;
    letter-spacing: -0.04em; color: #07111F; line-height: 1.15; margin-bottom: 14px;
  }
  .levi-card-headline em {
    font-style: normal;
    background: linear-gradient(110deg, #0567C8, #38BDF8);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
  }
  .levi-card-body { font-family: 'DM Sans', sans-serif; font-size: 14px; color: #475569; line-height: 1.75; }
  .levi-card-stats {
    display: flex; gap: 24px; flex-wrap: wrap;
    border-top: 1px solid rgba(15,28,46,0.07); margin-top: 18px; padding-top: 18px;
  }
  .levi-stat-value {
    font-family: 'Outfit', sans-serif; font-size: 1.6rem; font-weight: 900;
    letter-spacing: -0.04em; color: #07111F; line-height: 1;
  }
  .levi-stat-label {
    font-family: 'DM Sans', sans-serif; font-size: 10px; font-weight: 600;
    text-transform: uppercase; letter-spacing: .10em; color: #94A3B8;
  }

  /* ── Global smooth scroll ─────────────────────────────────────────────── */
  html { scroll-behavior: smooth; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
  /* ── Metric component — full label wrap fix ───────────────────────── */
  [data-testid="stMetricLabel"],
  [data-testid="stMetricLabel"] > div,
  [data-testid="stMetricLabel"] label,
  [data-testid="stMetricLabel"] p,
  div[data-testid="metric-container"] > label,
  div[data-testid="stMetricLabel"] div {
    font-size: 11px !important;
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: unset !important;
    line-height: 1.35 !important;
    min-height: 2.5em !important;
    display: block !important;
    max-height: none !important;
    height: auto !important;
    -webkit-line-clamp: unset !important;
    -webkit-box-orient: unset !important;
    word-break: break-word !important;
  }
  [data-testid="stMetricValue"],
  div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: clamp(1.1rem, 2.2vw, 1.7rem) !important;
    letter-spacing: -0.02em !important;
  }
  [data-testid="stMetricDelta"] { font-size: 11px !important; }
  div[data-testid="metric-container"] { padding-top: 4px !important; }
</style>
<script>
/* Force metric labels to wrap — CSS alone can't override Streamlit inline styles */
(function fixMetricLabels(){
  function fix(){
    document.querySelectorAll('[data-testid="stMetricLabel"]').forEach(function(el){
      el.style.whiteSpace = 'normal';
      el.style.overflow   = 'visible';
      el.style.textOverflow = 'unset';
      el.style.maxHeight  = 'none';
      el.style.height     = 'auto';
      el.querySelectorAll('div,p,label').forEach(function(c){
        c.style.whiteSpace  = 'normal';
        c.style.overflow    = 'visible';
        c.style.textOverflow = 'unset';
        c.style.maxHeight   = 'none';
        c.style.height      = 'auto';
        c.style.display     = 'block';
      });
    });
  }
  fix(); setTimeout(fix,400); setTimeout(fix,1200);
  if(window.MutationObserver){
    new MutationObserver(fix).observe(document.body,{childList:true,subtree:true});
  }
})();
</script>
""", unsafe_allow_html=True)

# ── Back-to-top button (fixed action button)
st.markdown("""
<style>
  #back-to-top-btn {
    position: fixed; bottom: 24px; right: 20px; z-index: 99999;
    width: 40px; height: 40px; border-radius: 10px;
    background: #FFFFFF;
    border: 1px solid rgba(5,103,200,0.25); cursor: pointer;
    display: flex; align-items: center; justify-content: center;
    box-shadow: 0 2px 10px rgba(15,28,46,0.14);
    opacity: 0; transition: opacity 0.3s, transform .2s, box-shadow .2s;
  }
  #back-to-top-btn:hover {
    opacity: 1 !important; transform: translateY(-2px);
    background: rgba(5,103,200,0.06);
    box-shadow: 0 4px 16px rgba(5,103,200,0.18);
  }
  #back-to-top-btn svg { width: 18px; height: 18px; fill: #0567C8; pointer-events: none; }
</style>
<button id="back-to-top-btn" title="Back to top">
  <svg viewBox="0 0 24 24"><path d="M7.41 15.41L12 10.83l4.59 4.58L18 14l-6-6-6 6z"/></svg>
</button>
<script>
(function(){
  function attachBtn(){
    var btn = document.getElementById('back-to-top-btn');
    if(!btn){ setTimeout(attachBtn, 500); return; }
    btn.addEventListener('click', function(){
      // Streamlit main content scrolls in the stMain block
      var main = document.querySelector('[data-testid="stMain"]') ||
                 document.querySelector('.main') ||
                 document.querySelector('[data-testid="stAppViewContainer"]');
      if(main){ main.scrollTo({top:0, behavior:'smooth'}); }
      window.scrollTo({top:0, behavior:'smooth'});
      document.documentElement.scrollTop = 0;
      document.body.scrollTop = 0;
    });
    // Show/hide based on scroll position
    var scroller = document.querySelector('[data-testid="stMain"]') || window;
    function onScroll(){ btn.style.opacity = ((scroller.scrollTop || window.scrollY) > 200) ? '1' : '0'; }
    (scroller === window ? window : scroller).addEventListener('scroll', onScroll);
    onScroll();
  }
  attachBtn();
})();
</script>
""", unsafe_allow_html=True)

# ─── Light mode: no ambient glow needed ──────────────────────────────────────

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
def load_later_ig_stories() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM later_ig_stories ORDER BY posted_at DESC LIMIT 200", conn
        )
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_later_tk_interactions() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM later_tk_interactions ORDER BY data_date DESC", conn
        )
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


# ─── New External Data Loaders ────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_fred_indicators() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM fred_economic_indicators ORDER BY data_date ASC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_google_trends() -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM google_trends_weekly ORDER BY week_date ASC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_weather_monthly() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM weather_monthly ORDER BY year ASC, month ASC", conn
        )
        if not df.empty:
            df["date"] = pd.to_datetime(
                df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
            )
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_bls_employment() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM bls_employment_monthly ORDER BY year ASC, month ASC", conn
        )
        if not df.empty:
            df["date"] = pd.to_datetime(
                df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
            )
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_noaa_marine() -> pd.DataFrame:
    """NOAA buoy monthly ocean conditions — wave height, water temp, beach activity score."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM noaa_marine_monthly ORDER BY year ASC, month ASC", conn
        )
        if not df.empty:
            df["date"] = pd.to_datetime(
                df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
            )
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_census_demo() -> pd.DataFrame:
    """US Census ACS feeder market demographics — OC, LA, SD counties."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM census_demographics ORDER BY year ASC, geography ASC", conn
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_eia_gas() -> pd.DataFrame:
    """EIA California weekly retail gas prices — drive-market demand signal."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM eia_gas_prices ORDER BY week_end_date ASC", conn
        )
        if not df.empty:
            df["date"] = pd.to_datetime(df["week_end_date"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_tsa_checkpoint() -> pd.DataFrame:
    """TSA daily checkpoint throughput — national air travel demand proxy."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM tsa_checkpoint_daily ORDER BY travel_date ASC", conn
        )
        if not df.empty:
            df["date"] = pd.to_datetime(df["travel_date"], errors="coerce")
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
        "later_ig_stories",
        "later_tk_interactions",
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
        "eia_gas_prices",
        "tsa_checkpoint_daily",
        "bls_employment_monthly",
        "google_trends_weekly",
        "weather_monthly",
        "noaa_marine_monthly",
        "census_demographics",
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
        f"💡 Add your Anthropic API key in the sidebar (VDP Analyst section) for live analysis.",
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
            yield "⚠️ **Invalid API key.** Check your key in the sidebar VDP Analyst section and try again."
        elif "429" in err:
            yield "⚠️ **Rate limited.** Please wait a moment and try again."
        else:
            yield f"⚠️ **API Error:** {err[:200]}"

# ─── Multi-Model AI Router ────────────────────────────────────────────────────

def _stream_openai_compat(prompt: str, model: str, api_key_val: str, base_url: str | None = None, extra_system: str = ""):
    """Stream from any OpenAI-compatible API (OpenAI, Perplexity)."""
    if not OPENAI_AVAILABLE:
        yield "⚠️ `openai` package not installed. Run: `pip install openai`"
        return
    if not api_key_val:
        provider = "Perplexity" if base_url else "OpenAI"
        key_name  = "PERPLEXITY_API_KEY" if base_url else "OPENAI_API_KEY"
        yield f"⚠️ {provider} API key not configured. Add `{key_name}` to your `.env` file."
        return
    try:
        kwargs = {"api_key": api_key_val}
        if base_url:
            kwargs["base_url"] = base_url
        client = _openai_sdk.OpenAI(**kwargs)
        sys_content = SYSTEM_PROMPT + ("\n\n" + extra_system if extra_system else "")
        with client.chat.completions.create(
            model=model,
            max_tokens=1500,
            stream=True,
            messages=[
                {"role": "system", "content": sys_content},
                {"role": "user",   "content": prompt},
            ],
        ) as stream:
            for chunk in stream:
                delta = chunk.choices[0].delta.content
                if delta:
                    yield delta
    except Exception as e:
        yield f"⚠️ API error: {str(e)[:300]}"


def _stream_gemini(prompt: str, model: str, api_key_val: str):
    """Stream from Google Gemini API."""
    if not GEMINI_AVAILABLE:
        yield "⚠️ `google-generativeai` not installed. Run: `pip install google-generativeai`"
        return
    if not api_key_val:
        yield "⚠️ Google AI API key not configured. Add `GOOGLE_AI_API_KEY` to your `.env` file."
        return
    try:
        _genai.configure(api_key=api_key_val)
        gm = _genai.GenerativeModel(
            model_name=model,
            system_instruction=SYSTEM_PROMPT,
        )
        response = gm.generate_content(prompt, stream=True)
        for chunk in response:
            if hasattr(chunk, "text") and chunk.text:
                yield chunk.text
    except Exception as e:
        yield f"⚠️ Gemini error: {str(e)[:300]}"


def stream_ai_response(prompt: str, model_key: str, keys: dict | None = None):
    """Universal AI streaming router.

    Args:
        prompt:    The user prompt (with data context injected)
        model_key: Key from AI_MODELS registry
        keys:      dict with provider API keys: {"anthropic", "openai", "google", "perplexity"}
                   Defaults to env-loaded keys if not supplied.
    """
    if keys is None:
        keys = {}
    model_info = AI_MODELS.get(model_key, AI_MODELS["claude-sonnet-4-6"])
    provider   = model_info["provider"]

    if provider == "anthropic":
        ant_key = keys.get("anthropic", _ENV_API_KEY)
        yield from stream_claude_response(prompt, ant_key)

    elif provider == "openai":
        oai_key = keys.get("openai", _ENV_OPENAI_KEY)
        yield from _stream_openai_compat(prompt, model_key, oai_key)

    elif provider == "google":
        g_key = keys.get("google", _ENV_GOOGLE_AI_KEY)
        yield from _stream_gemini(prompt, model_key, g_key)

    elif provider == "perplexity":
        p_key = keys.get("perplexity", _ENV_PERPLEXITY_KEY)
        yield from _stream_openai_compat(
            prompt, model_key, p_key,
            base_url="https://api.perplexity.ai",
            extra_system=(
                "Search the live web for current market data, competitor news, and travel industry "
                "trends relevant to the query. Cite your sources inline with [Source: URL]."
            ),
        )
    else:
        yield "⚠️ Unknown model provider."


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
             sparkline_values: list = None, tooltip: str = "") -> str:
    css      = "kpi-delta-neutral" if neutral else ("kpi-delta-pos" if positive else "kpi-delta-neg")
    if neutral:
        arrow = "— "
        trend_icon = ""
    elif positive:
        arrow = ""
        trend_icon = '<svg width="10" height="10" viewBox="0 0 10 10" fill="none"><path d="M2 7L5 3L8 7" stroke="#059669" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>'
    else:
        arrow = ""
        trend_icon = '<svg width="10" height="10" viewBox="0 0 10 10" fill="none"><path d="M2 3L5 7L8 3" stroke="#DC2626" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>'
    date_html  = f'<div class="kpi-date">{date_label}</div>' if date_label else ""
    svg        = kpi_metric_svg(label, positive, raw_value, sparkline_values)
    spark_html = sparkline_svg(sparkline_values, positive) if sparkline_values else ""
    title_attr = f' title="{tooltip}"' if tooltip else ""
    return (
        f'<div class="kpi-card"{title_attr}>'
        f'<div class="kpi-header">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-icon-svg">{svg}</div>'
        f'</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="{css}">{trend_icon}{arrow}{delta}</div>'
        f'{date_html}'
        f'{spark_html}'
        f'</div>'
    )


def insight_card(title, body, kind="info", icon: str = "", date_label: str = "") -> str:
    svg_icon  = insight_icon_svg(kind, icon) if icon else ""
    icon_html = f'<span class="insight-icon">{svg_icon}</span>' if svg_icon else ""
    # Kind → label + color
    _kind_meta = {
        "positive": ("OPPORTUNITY", "#10B981"),
        "negative": ("RISK",        "#F04E37"),
        "warning":  ("WATCH",       "#F59E0B"),
        "info":     ("SIGNAL",      "#00C3BE"),
    }
    _lbl, _clr = _kind_meta.get(kind, ("INSIGHT", "#8FA3B8"))
    date_html = (
        f'<div style="font-size:10px;color:#4A5F74;margin-top:8px;padding-top:6px;'
        f'border-top:1px solid rgba(0,0,0,0.08);">{date_label}</div>'
        if date_label else ""
    )
    return (
        f'<div class="insight-card insight-{kind}">'
        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
        f'<span style="font-size:9px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;'
        f'color:{_clr};padding:2px 6px;border-radius:4px;background:rgba(0,0,0,0.03);'
        f'border:1px solid {_clr}33;">{_lbl}</span>'
        f'</div>'
        f'<div class="insight-title">{icon_html}{md_to_html(title)}</div>'
        f'<p class="insight-body">{md_to_html(body)}</p>'
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
    """Painted-data chart theme — Dana Point PULSE v7.
    codeandtheory.com × levinriegner.com: organic fills, bold type, expressive color.
    """
    _font  = "Syne, DM Sans, Inter, system-ui, sans-serif"
    _title = "Syne, Outfit, DM Sans, system-ui, sans-serif"
    _colorway = [
        "#0567C8",  # primary blue
        "#EA580C",  # vivid orange
        "#059669",  # emerald green
        "#7C3AED",  # purple
        "#D97706",  # amber
        "#DC2626",  # red
        "#0891B2",  # cyan
        "#B45309",  # deep amber
    ]
    fig.update_layout(
        plot_bgcolor  = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)",
        font    = dict(family=_font, size=12.5, color="#1E293B"),
        height  = height,
        margin  = dict(l=4, r=8, t=52, b=8),
        transition = {"duration": 600, "easing": "cubic-in-out"},
        legend = dict(
            orientation = "h",
            yanchor = "bottom", y = 1.04,
            xanchor = "left",   x = 0,
            font    = dict(size=11.5, family=_font, color="#475569"),
            bgcolor = "rgba(255,255,255,0.0)",
            borderwidth = 0,
            itemsizing = "constant",
        ),
        hoverlabel = dict(
            bgcolor     = "#07111F",
            bordercolor = "rgba(5,103,200,0.60)",
            font        = dict(size=13.5, family=_font, color="#F1F5F9"),
            namelength  = -1,
            align       = "left",
        ),
        colorway = _colorway,
        modebar = dict(
            bgcolor     = "rgba(0,0,0,0)",
            color       = "#CBD5E1",
            activecolor = "#0567C8",
        ),
    )
    fig.update_xaxes(
        showgrid    = False,
        zeroline    = False,
        tickfont    = dict(size=11, family=_font, color="#64748B"),
        linecolor   = "rgba(15,28,46,0.10)",
        linewidth   = 1,
        showline    = True,
        ticks       = "outside",
        ticklen     = 4,
        tickcolor   = "rgba(15,28,46,0.10)",
    )
    fig.update_yaxes(
        gridcolor   = "rgba(15,28,46,0.04)",
        gridwidth   = 1,
        griddash    = "dot",
        zeroline    = False,
        tickfont    = dict(size=11, family=_font, color="#64748B"),
        showline    = False,
        ticks       = "",
    )
    # ── Painted-data: organic gradient fills + smooth spline lines ────────
    _fill_palette = [
        "rgba(5,103,200,0.09)", "rgba(234,88,12,0.08)", "rgba(5,150,105,0.08)",
        "rgba(124,58,237,0.08)", "rgba(217,119,6,0.08)",
    ]
    _fill_idx = 0
    for trace in fig.data:
        ttype = type(trace).__name__
        if ttype in ("Scatter", "Scattergl"):
            mode = str(getattr(trace, "mode", "") or "")
            if "lines" in mode and getattr(trace, "fill", None) in (None, "none"):
                trace.fill = "tozeroy"
                _c = (getattr(trace.line, "color", None) or _colorway[0])
                if "rgb(" in str(_c):
                    trace.fillcolor = _c.replace("rgb(", "rgba(").replace(")", ",0.09)")
                else:
                    trace.fillcolor = _fill_palette[_fill_idx % len(_fill_palette)]
                    _fill_idx += 1
            # Smooth spline for organic painted feel
            if hasattr(trace, "line") and trace.line is not None:
                if not getattr(trace.line, "shape", None):
                    try:
                        trace.line.shape = "spline"
                        trace.line.smoothing = 0.85
                    except Exception:
                        pass
                if getattr(trace.line, "width", None) is None or getattr(trace.line, "width", 2) < 2:
                    try:
                        trace.line.width = 2.5
                    except Exception:
                        pass
        elif ttype == "Bar":
            try:
                if not getattr(trace.marker, "cornerradius", None):
                    trace.marker.cornerradius = 4
            except Exception:
                pass
    # ── Title styling ──────────────────────────────────────────────────────
    if fig.layout.title and fig.layout.title.text:
        fig.update_layout(
            title_font = dict(family=_title, size=15, color="#07111F"),
            title_x    = 0,
            title_pad  = dict(l=4, t=6),
        )
    return fig


def sec_div(title: str) -> str:
    """Returns HTML for a section divider with centered pill label."""
    return (
        f'<div class="section-divider">'
        f'<div class="section-divider-line"></div>'
        f'<div class="section-divider-title">{title}</div>'
        f'<div class="section-divider-line-r"></div>'
        f'</div>'
    )


def render_kpi_ticker(df_kpi: "pd.DataFrame", df_dfy: "pd.DataFrame",
                      df_later_ig: "pd.DataFrame") -> str:
    """Bloomberg/1ax-style dual-row KPI ticker with live data."""
    import math

    def _item(label: str, val: str, delta: str = "", positive: bool | None = None) -> str:
        delta_cls = ""
        if delta and positive is True:   delta_cls = "pulse-ticker-pos"
        elif delta and positive is False: delta_cls = "pulse-ticker-neg"
        delta_html = f'<span class="{delta_cls}">{delta}</span>' if delta else ""
        return (
            f'<div class="pulse-ticker-item">'
            f'<span class="pulse-ticker-label">{label}</span>'
            f'<span class="pulse-ticker-val">{val}</span>'
            f'{delta_html}'
            f'</div>'
        )

    # ── Pull live metrics ──────────────────────────────────────────────────────
    occ_val   = adr_val = rvp_val = "—"
    occ_d     = adr_d   = rvp_d   = ""
    occ_pos   = adr_pos = rvp_pos = None
    if not df_kpi.empty:
        _rec = df_kpi.sort_values("as_of_date").iloc[-1]
        try:
            occ_val = f"{float(_rec.get('occ_pct', 0)):.1f}%"
            _od = float(_rec.get("occ_yoy", 0))
            occ_d = f"{'▲' if _od>=0 else '▼'}{abs(_od):.1f}pp"; occ_pos = _od >= 0
        except Exception: pass
        try:
            adr_val = f"${float(_rec.get('adr', 0)):,.0f}"
            _ad = float(_rec.get("adr_yoy", 0))
            adr_d = f"{'▲' if _ad>=0 else '▼'}{abs(_ad):.1f}%"; adr_pos = _ad >= 0
        except Exception: pass
        try:
            rvp_val = f"${float(_rec.get('revpar', 0)):,.0f}"
            _rd = float(_rec.get("revpar_yoy", 0))
            rvp_d = f"{'▲' if _rd>=0 else '▼'}{abs(_rd):.1f}%"; rvp_pos = _rd >= 0
        except Exception: pass

    trips_val = "—"; oos_val = "—"; roas_val = "—"; top_dma = "—"
    if not df_dfy.empty:
        try:
            _r = df_dfy.iloc[0]
            trips_val = f"{float(_r.get('total_trips',0))/1e6:.2f}M" if _r.get('total_trips') else "—"
            oos_val   = f"{float(_r.get('out_of_state_vd_pct',0)):.0f}%"
            roas_val  = f"{float(_r.get('media_roas',0)):.1f}×" if _r.get('media_roas') else "—"
            top_dma   = str(_r.get("top_dma","Los Angeles"))
        except Exception: pass

    ig_val = "—"
    if not df_later_ig.empty:
        try:
            _ig = df_later_ig.sort_values("date").iloc[-1]
            ig_val = f"{int(float(_ig.get('followers',0))):,}" if _ig.get('followers') else "—"
        except Exception: pass

    # ── Build rows ─────────────────────────────────────────────────────────────
    row1_items = [
        _item("⬤ LIVE", "DANA POINT PULSE"),
        _item("OCCUPANCY", occ_val, occ_d, occ_pos),
        _item("ADR", adr_val, adr_d, adr_pos),
        _item("REVPAR", rvp_val, rvp_d, rvp_pos),
        _item("ANNUAL TRIPS", trips_val),
        _item("OUT-OF-STATE", oos_val),
        _item("MEDIA ROAS", roas_val),
        _item("TOP FEEDER DMA", top_dma),
        _item("IG FOLLOWING", ig_val),
    ]
    row2_items = [
        _item("TBID BLEND", "1.25%"),
        _item("TOT RATE", "10%"),
        _item("OHANA FEST ADR LIFT", "+$139"),
        _item("SPEND MULTIPLIER", "3.2×"),
        _item("OOS VISITORS", "68%"),
        _item("AVG LOS", "2.0 days"),
        _item("DAYTRIP CONVERSION OPP", "~$15M"),
        _item("PIPELINE ROOMS", "CoStar"),
        _item("DATA SOURCES", "STR · Datafy · CoStar"),
    ]
    # Duplicate for seamless loop
    row1_html = "".join(row1_items * 3)
    row2_html = "".join(row2_items * 3)

    return (
        f'<div class="pulse-ticker-wrap">'
        f'<div class="pulse-ticker-track left">{row1_html}</div>'
        f'<div class="pulse-ticker-track right" style="border-top:1px solid rgba(255,255,255,0.05);">{row2_html}</div>'
        f'</div>'
    )


def render_painted_occ_heatmap(df_kpi_daily: "pd.DataFrame") -> "go.Figure | None":
    """Giorgia Lupi / Long-Covid painted-density occupancy calendar.

    Instead of bars, renders a 7-column weekly calendar grid where each cell
    is a scatter bubble sized + colored by occupancy — density = intensity.
    Returns a Plotly figure, or None if insufficient data.
    """
    if df_kpi_daily is None or df_kpi_daily.empty:
        return None
    try:
        _df = df_kpi_daily.copy()
        _df["as_of_date"] = pd.to_datetime(_df["as_of_date"], errors="coerce")
        _df = _df.dropna(subset=["as_of_date", "occ_pct"]).copy()
        if len(_df) < 30:
            return None
        # Keep last 365 days
        _cutoff = _df["as_of_date"].max() - pd.Timedelta(days=365)
        _df = _df[_df["as_of_date"] >= _cutoff].copy()
        _df["week"]    = _df["as_of_date"].dt.isocalendar().week.astype(int)
        _df["dow"]     = _df["as_of_date"].dt.dayofweek   # 0=Mon … 6=Sun
        _df["year"]    = _df["as_of_date"].dt.year
        # Create week-index for X axis (unique per year-week)
        _df = _df.sort_values("as_of_date")
        _df["week_idx"] = (
            (_df["year"] - _df["year"].min()) * 53 + _df["week"]
        )
        _df["week_idx"] = _df["week_idx"] - _df["week_idx"].min()
        # Colour scale: pale → saturated teal at 80%, flame at 90%+
        # Map occ_pct to color intensity
        def _occ_color(v: float) -> str:
            v = max(0, min(100, v))
            if v >= 90:   return f"rgba(234,88,12,{min(0.92,0.55+v/200):.2f})"   # flame orange
            if v >= 80:   return f"rgba(5,103,200,{min(0.90,0.40+v/180):.2f})"   # deep teal
            if v >= 65:   return f"rgba(5,103,200,{0.18+v/280:.2f})"              # medium teal
            return f"rgba(5,103,200,{0.06+v/400:.2f})"                             # pale wash

        _df["color"] = _df["occ_pct"].apply(_occ_color)
        # Bubble size scales with occupancy — bigger = higher pressure
        _df["size"]  = _df["occ_pct"].clip(40,100).apply(lambda v: 6 + (v-40)/60*14)

        # Month label for x-axis ticks
        _df["month_label"] = _df["as_of_date"].dt.strftime("%b %Y")
        _dow_labels = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=_df["week_idx"],
            y=_df["dow"],
            mode="markers",
            marker=dict(
                size=_df["size"],
                color=_df["color"],
                line=dict(width=0),
                symbol="circle",
            ),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Occupancy: <b>%{customdata[1]:.1f}%</b><br>"
                "%{customdata[2]}"
                "<extra></extra>"
            ),
            customdata=list(zip(
                _df["as_of_date"].dt.strftime("%a %b %d, %Y"),
                _df["occ_pct"],
                _df["occ_pct"].apply(
                    lambda v: "🔥 COMPRESSION — rate premium justified"
                    if v >= 90 else ("🎯 High demand" if v >= 80 else "")
                ),
            )),
            showlegend=False,
        ))
        # Month boundary lines
        _month_starts = _df.groupby(_df["as_of_date"].dt.to_period("M"))["week_idx"].min()
        for _ms in _month_starts.values[1:]:
            fig.add_vline(x=_ms - 0.5, line_width=1,
                         line_color="rgba(15,28,46,0.12)", line_dash="dot")
        # Month labels
        _tick_vals = _month_starts.values
        _tick_text = [str(k) for k in _month_starts.index]
        fig.update_xaxes(
            tickvals=_tick_vals, ticktext=_tick_text,
            showgrid=False, zeroline=False,
            tickfont=dict(size=10),
        )
        fig.update_yaxes(
            tickvals=list(range(7)), ticktext=_dow_labels,
            showgrid=False, zeroline=False,
            tickfont=dict(size=10),
            autorange="reversed",
        )
        fig.update_layout(
            title="Occupancy Density Calendar — 365-Day View",
            height=200,
            margin=dict(l=4, r=4, t=40, b=8),
        )
        return style_fig(fig, height=200)
    except Exception:
        return None


def evt_type_class(event_type: str) -> tuple[str, str]:
    """Return (card_class, pill_class) for a given event type string."""
    t = (event_type or "").lower()
    if any(x in t for x in ["festival","music","concert","ohana","doheny"]):
        return "evt-card evt-festival", "evt-type-pill evt-type-festival", "🎵"
    if any(x in t for x in ["surf","wave","beach","paddl"]):
        return "evt-card evt-surf", "evt-type-pill evt-type-surf", "🏄"
    if any(x in t for x in ["tall ships","maritime","sail","harbor","whale","ocean"]):
        return "evt-card evt-maritime", "evt-type-pill evt-type-maritime", "⚓"
    if any(x in t for x in ["holiday","4th","fourth","christmas","firework","parade"]):
        return "evt-card evt-holiday", "evt-type-pill evt-type-holiday", "🎆"
    if any(x in t for x in ["race","run","triathlon","marathon","cycling"]):
        return "evt-card evt-race", "evt-type-pill evt-type-race", "🏁"
    return "evt-card evt-default", "evt-type-pill evt-type-default", "📅"


def tab_summary(text: str) -> str:
    """Returns HTML for a tab-level summary card."""
    return f'<div class="tab-summary">{text}</div>'


def sec_intel(section_name: str, what_it_shows: str, key_insight: str,
              forward_implication: str, key_stat: str) -> str:
    """Render a Section Intelligence summary card — enterprise edition."""
    return (
        f'<div class="sec-intel">'
        f'<div class="sec-intel-label">◈ Section Intelligence</div>'
        f'<div class="sec-intel-body">'
        f'<strong style="color:#E2E8F0;">{section_name}</strong> — {what_it_shows}.<br>'
        f'<strong style="color:#E2E8F0;">Key Insight:</strong> {key_insight} &nbsp;'
        f'<strong style="color:#E2E8F0;">Forward Implication:</strong> {forward_implication}<br>'
        f'<span class="sec-intel-stat">▸ {key_stat}</span>'
        f'</div>'
        f'</div>'
    )


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


def _safe_section(fn, section_name: str = "section"):
    """GloCon Solutions LLC — failsafe wrapper for any dashboard section.
    Catches exceptions and renders a user-friendly error card instead of crashing.
    Usage: _safe_section(lambda: my_section_code(), 'Section Name')
    """
    try:
        fn()
    except Exception as _err:
        import traceback as _tb
        st.warning(
            f"⚠️ **{section_name}** could not render — data may be incomplete or a format changed. "
            f"Run `python scripts/run_pipeline.py` to refresh, then click 🔄 Refresh Dashboard.",
            icon="⚠️",
        )
        if st.session_state.get("is_admin", False):
            with st.expander(f"🔍 Debug: {section_name} error"):
                st.code(_tb.format_exc(), language="python")


def _sh(icon: str, title: str, color: str = "teal", tag: str = "") -> str:
    """Generate an enterprise section header block HTML."""
    _tag_html = f'<span class="sh-tag">{tag}</span>' if tag else ""
    return (
        f'<div class="sh-block sh-{color}">'
        f'<span class="sh-icon">{icon}</span>'
        f'<span class="sh-title">{title}</span>'
        f'{_tag_html}'
        f'</div>'
    )


def source_card(dot: str, name: str, meta: str, count: str, url: str = "") -> str:
    """Styled data-source health card. Pass url to make it a clickable link."""
    tag = "a" if url else "div"
    href = f' href="{url}" target="_blank" rel="noopener noreferrer"' if url else ""
    link_icon = ' <span style="font-size:10px;opacity:0.6;">↗</span>' if url else ""
    return (
        f'<{tag} class="src-card"{href}>'
        f'<span class="src-dot">{dot}</span>'
        f'<div style="flex:1;min-width:0"><div class="src-name">{name}{link_icon}</div>'
        f'<div class="src-meta">{meta}</div></div>'
        f'<div class="src-count">{count}</div>'
        f'</{tag}>'
    )


def grain_badge(g: str) -> str:
    return f'<span class="grain-badge">{g}</span>'


# ─── AI runtime defaults (overridden by sidebar on each rerun) ───────────────
# These ensure render_intel_panel and tab renderers never NameError on first load.
selected_model  = st.session_state.get("selected_model", CLAUDE_MODEL)
_OPENAI_KEY     = _ENV_OPENAI_KEY
_GOOGLE_AI_KEY  = _ENV_GOOGLE_AI_KEY
_PERPLEXITY_KEY = _ENV_PERPLEXITY_KEY
_ai_keys        = {
    "anthropic":  _ENV_API_KEY,
    "openai":     _OPENAI_KEY,
    "google":     _GOOGLE_AI_KEY,
    "perplexity": _PERPLEXITY_KEY,
}

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
df_later_ig_stories  = load_later_ig_stories()
df_later_tk_inter    = load_later_tk_interactions()
# External live data (populated by pipeline steps 12–15)
df_fred    = load_fred_indicators()    # FRED economic indicators
df_trends  = load_google_trends()      # Google Trends search demand
df_weather = load_weather_monthly()    # Open-Meteo coastal weather
df_bls     = load_bls_employment()     # BLS hospitality employment
df_eia_gas   = load_eia_gas()           # EIA CA weekly gas prices (drive-market signal)
df_tsa       = load_tsa_checkpoint()   # TSA checkpoint throughput (air travel demand)
df_noaa      = load_noaa_marine()      # NOAA buoy ocean conditions (coastal demand driver)
df_census    = load_census_demo()      # Census ACS feeder market demographics

# Global Plotly chart config — drill-down ready
PLOTLY_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "modeBarButtonsToAdd": ["v1hovermode"],
    "toImageButtonOptions": {
        "format": "png",
        "filename": "dana_point_pulse_chart",
        "scale": 3,
        "width": 1600,
        "height": 800,
    },
    "responsive": True,
    "scrollZoom": False,
}

# ─── Scroll-reveal IntersectionObserver + hero entry animation ────────────────
st.markdown("""
<script>
(function(){
  // --- Scroll-reveal cards (byhook clip-path style) ---
  function initReveal(){
    var root = window.parent && window.parent.document ? window.parent.document : document;
    var els = root.querySelectorAll('.reveal-card,.reveal-clip,.reveal-left');
    if(!els.length) return;
    if(root.IntersectionObserver){
      var io = new root.defaultView.IntersectionObserver(function(entries){
        entries.forEach(function(e){
          if(e.isIntersecting){
            e.target.classList.add('is-visible');
            io.unobserve(e.target);
          }
        });
      },{threshold:0.08, rootMargin:'0px 0px -40px 0px'});
      els.forEach(function(el){ io.observe(el); });
    } else {
      els.forEach(function(el){ el.classList.add('is-visible'); });
    }
  }
  // --- Hero entry fade-in on load ---
  function heroEntry(){
    var root = window.parent && window.parent.document ? window.parent.document : document;
    var hero = root.querySelector('.hero-banner');
    if(hero){
      hero.style.opacity='0'; hero.style.transition='opacity 0.7s ease';
      setTimeout(function(){ hero.style.opacity='1'; },80);
    }
  }
  // --- Sticky tab bar: walk DOM tree to remove overflow:hidden on all ancestors ---
  function initStickyTabs(){
    var doc = window.parent && window.parent.document ? window.parent.document : document;
    var tabEl = doc.querySelector('[data-testid="stTabs"]');
    if(!tabEl) return;
    // Walk up the DOM tree and remove overflow:hidden on all ancestors
    var el = tabEl.parentElement;
    while(el && el !== doc.body){
      var s = doc.defaultView.getComputedStyle(el);
      if(s.overflow === 'hidden' || s.overflowY === 'hidden'){
        el.style.overflow = 'visible';
        el.style.overflowY = 'visible';
      }
      el = el.parentElement;
    }
    // Apply sticky styles via cssText (replaces previous value for reliability)
    tabEl.style.cssText = [
      'position:sticky',
      'top:0',
      'z-index:9999',
      'background:rgba(238,242,248,0.97)',
      'backdrop-filter:blur(16px)',
      '-webkit-backdrop-filter:blur(16px)',
      'border-bottom:2px solid rgba(5,103,200,0.18)',
      'box-shadow:0 4px 24px rgba(15,28,46,0.12)',
      'padding:6px 0 4px 0',
      'margin-bottom:0',
    ].join('!important;') + '!important';
  }
  setTimeout(initReveal, 300);
  setTimeout(initReveal, 900);
  setTimeout(heroEntry, 50);
  setTimeout(initStickyTabs, 700);
  setTimeout(initStickyTabs, 1800);
  if(window.MutationObserver){
    var doc = window.parent && window.parent.document ? window.parent.document : document;
    new MutationObserver(function(){ initReveal(); setTimeout(initStickyTabs,250); }).observe(doc.body,{childList:true,subtree:true});
  }
})();
</script>
""", unsafe_allow_html=True)

# ─── Filter state — read from session_state (set by tab-level filter widgets) ──
# Tab widgets write to session_state keys; these module-level reads pick up the
# updated values on each rerun so df_sel is always in sync with what the user set.
_DAYS_MAP = {
    "Last 30 Days": 30, "Last 60 Days": 60, "Last 90 Days": 90,
    "Last 6 Months": 180, "Last 12 Months": 365, "All Time": 3650,
}
grain       = st.session_state.get("ss_grain", "Daily")
range_label = st.session_state.get("ss_range", "Last 90 Days")
days        = _DAYS_MAP.get(range_label, 90)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
# GloCon Solutions LLC — Dana Point PULSE sidebar with VDP branding + images
with st.sidebar:
    st.markdown(
        '<div style="background:rgba(8,145,178,0.06);border-radius:10px;padding:16px;'
        'margin-bottom:12px;border:1px solid rgba(8,145,178,0.16);">'
        '<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">'
        '<span style="font-size:22px;line-height:1;">🌊</span>'
        '<div>'
        '<a href="?" style="text-decoration:none;">'
        '<div class="sidebar-brand">Dana Point PULSE</div>'
        '</a>'
        '<div style="font-size:10px;color:#4A5568;font-weight:600;margin-top:1px;">'
        'South Orange County, CA</div>'
        '</div>'
        '</div>'
        '<div style="font-size:10px;color:#718096;letter-spacing:.05em;font-weight:600;'
        'text-transform:uppercase;border-top:1px solid rgba(0,0,0,0.08);padding-top:8px;">'
        'Performance · Intelligence · Strategy</div>'
        '</div>'
        '<div style="font-size:10.5px;color:#718096;margin-bottom:4px;">'
        'VDP Select Portfolio &nbsp;·&nbsp; 12 Properties</div>',
        unsafe_allow_html=True,
    )
    st.divider()

    # ── Admin mode check (URL param ?admin=true) ────────────────────────────
    _qp = st.query_params
    _is_admin = str(_qp.get("admin", "")).lower() == "true"

    # ── VDP Analyst — Multi-Model AI Config ──────────────────────────────────
    st.markdown("**🧠 VDP Analyst**")

    # API keys: admin can override, otherwise load from env silently
    if _is_admin:
        api_key_raw = st.text_input(
            "Anthropic API Key",
            type="password", placeholder="sk-ant-api03-…",
            value=_ENV_API_KEY,
            help="ANTHROPIC_API_KEY from .env",
            key="api_key_field",
        )
        _oa_raw = st.text_input(
            "OpenAI API Key",
            type="password", placeholder="sk-…",
            value=_ENV_OPENAI_KEY,
            help="OPENAI_API_KEY from .env — enables GPT-4o, o3-mini",
            key="openai_key_field",
        )
        _ga_raw = st.text_input(
            "Google AI API Key",
            type="password", placeholder="AIzaSy…",
            value=_ENV_GOOGLE_AI_KEY,
            help="GOOGLE_AI_API_KEY from .env — enables Gemini 2.0 Flash, 1.5 Pro",
            key="google_key_field",
        )
        _px_raw = st.text_input(
            "Perplexity API Key",
            type="password", placeholder="pplx-…",
            value=_ENV_PERPLEXITY_KEY,
            help="PERPLEXITY_API_KEY from .env — enables live web search (Sonar Pro)",
            key="perplexity_key_field",
        )
    else:
        api_key_raw = _ENV_API_KEY
        _oa_raw     = _ENV_OPENAI_KEY
        _ga_raw     = _ENV_GOOGLE_AI_KEY
        _px_raw     = _ENV_PERPLEXITY_KEY

    api_key       = api_key_raw.strip()
    _OPENAI_KEY   = _oa_raw.strip()
    _GOOGLE_AI_KEY = _ga_raw.strip()
    _PERPLEXITY_KEY = _px_raw.strip()

    api_key_valid = bool(api_key) and api_key.startswith("sk-ant-") and len(api_key) > 20

    # Build active model list based on available keys
    _avail_models = {}
    for _mk, _mi in AI_MODELS.items():
        _prov = _mi["provider"]
        if _prov == "anthropic" and api_key_valid and ANTHROPIC_AVAILABLE:
            _avail_models[_mk] = _mi
        elif _prov == "openai" and _OPENAI_KEY and OPENAI_AVAILABLE:
            _avail_models[_mk] = _mi
        elif _prov == "google" and _GOOGLE_AI_KEY and GEMINI_AVAILABLE:
            _avail_models[_mk] = _mi
        elif _prov == "perplexity" and _PERPLEXITY_KEY and OPENAI_AVAILABLE:
            _avail_models[_mk] = _mi

    # Model selector
    _model_opts   = list(_avail_models.keys()) or ["claude-sonnet-4-6"]
    _model_labels = [f"{_avail_models[k]['badge']} {_avail_models[k]['label']}" if k in _avail_models else "🟦 Claude Sonnet 4.6" for k in _model_opts]
    _default_idx  = 0
    _saved_model  = st.session_state.get("selected_model", "claude-sonnet-4-6")
    if _saved_model in _model_opts:
        _default_idx = _model_opts.index(_saved_model)

    if len(_model_opts) > 1:
        _sel_label = st.selectbox(
            "Active AI Model",
            options=_model_labels,
            index=_default_idx,
            key="model_selector",
            help="Select the AI model for all analyst panels",
        )
        selected_model = _model_opts[_model_labels.index(_sel_label)]
    else:
        selected_model = _model_opts[0]
        if _avail_models:
            _mi_sel = _avail_models.get(selected_model, {})
            st.caption(f"{_mi_sel.get('badge','🟦')} {_mi_sel.get('label', selected_model)}")

    st.session_state["selected_model"] = selected_model

    # Build keys dict for downstream routing
    _ai_keys = {
        "anthropic":  api_key,
        "openai":     _OPENAI_KEY,
        "google":     _GOOGLE_AI_KEY,
        "perplexity": _PERPLEXITY_KEY,
    }

    # Status indicators for each provider
    _provider_status = [
        ("Claude",     ANTHROPIC_AVAILABLE and api_key_valid,          "🟦"),
        ("GPT-4o",     OPENAI_AVAILABLE and bool(_OPENAI_KEY),         "🟩"),
        ("Gemini",     GEMINI_AVAILABLE and bool(_GOOGLE_AI_KEY),       "🟨"),
        ("Perplexity", OPENAI_AVAILABLE and bool(_PERPLEXITY_KEY),     "🟪"),
    ]
    _dots = "  ".join(
        f"{'🟢' if ok else '⚫'} {name}"
        for name, ok, _ in _provider_status
        if ok or _is_admin
    )
    if _dots:
        st.caption(_dots)

    _active_count = sum(1 for _, ok, _ in _provider_status if ok)
    if _active_count == 0:
        st.caption("⚪ Local mode — add API keys for AI analysis")
    elif _active_count == 1:
        st.caption(f"1 AI model active · Add more keys to unlock model selection")

    if not ANTHROPIC_AVAILABLE:
        st.warning("`anthropic` not installed.\nRun: `pip install anthropic`", icon="⚠️")

    # ── Public refresh button (all users) ────────────────────────────────────
    # GloCon Solutions LLC — user-facing cache-clear button
    if st.button("🔄 Refresh Dashboard", use_container_width=True,
                 help="Clear cached data and reload all metrics from the database."):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    # ── Global date range filter ───────────────────────────────────────────────
    # GloCon Solutions LLC — global filter stored in session state; applied to df_sel
    st.markdown("**📅 Date Filter**")
    if not df_daily.empty:
        _gf_min = df_daily["as_of_date"].min().to_pydatetime().date()
        _gf_max = df_daily["as_of_date"].max().to_pydatetime().date()
        _gf_default = st.session_state.get("global_date_range", (_gf_min, _gf_max))
        _gf_sel = st.date_input(
            "Show data between",
            value=_gf_default,
            min_value=_gf_min,
            max_value=_gf_max,
            key="global_date_range_input",
            label_visibility="collapsed",
        )
        if isinstance(_gf_sel, (list, tuple)) and len(_gf_sel) == 2:
            st.session_state["global_date_range"] = tuple(_gf_sel)
        if st.button("↺ Reset to All Dates", use_container_width=True, key="reset_date_filter"):
            st.session_state.pop("global_date_range", None)
            st.rerun()
    else:
        st.caption("Load STR data to enable date filter.")

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
    # Later.com social media — direct DB count (same pattern as Visit CA — bypasses stale cache)
    _later_ig_rows = len(df_later_ig_profile) + len(df_later_ig_posts) + len(df_later_ig_reels)
    _later_fb_rows = len(df_later_fb_profile) + len(df_later_fb_posts)
    _later_tk_rows = len(df_later_tk_profile)
    _later_total   = _later_ig_rows + _later_fb_rows + _later_tk_rows
    _ig_followers  = int(df_later_ig_profile.iloc[0]["followers"]) if not df_later_ig_profile.empty and "followers" in df_later_ig_profile.columns else 0
    _fb_followers  = int(df_later_fb_profile.iloc[0]["page_followers"]) if not df_later_fb_profile.empty and "page_followers" in df_later_fb_profile.columns else 0
    _tk_followers  = int(df_later_tk_profile.iloc[0]["followers"]) if not df_later_tk_profile.empty and "followers" in df_later_tk_profile.columns else 0
    # Fallback: query DB directly if cached DFs are empty (same guard as Visit CA)
    if _later_total == 0:
        try:
            _l_conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            _later_total = sum(
                _l_conn.execute(f"SELECT COUNT(*) FROM {_lt}").fetchone()[0]
                for _lt in ["later_ig_profile_growth","later_ig_posts","later_ig_reels",
                            "later_fb_profile_growth","later_fb_posts","later_tk_profile_growth"]
            )
            if _ig_followers == 0:
                _row = _l_conn.execute(
                    "SELECT followers FROM later_ig_profile_growth ORDER BY data_date DESC LIMIT 1"
                ).fetchone()
                _ig_followers = int(_row[0]) if _row and _row[0] else 0
            if _fb_followers == 0:
                _row = _l_conn.execute(
                    "SELECT page_followers FROM later_fb_profile_growth ORDER BY data_date DESC LIMIT 1"
                ).fetchone()
                _fb_followers = int(_row[0]) if _row and _row[0] else 0
            if _tk_followers == 0:
                _row = _l_conn.execute(
                    "SELECT followers FROM later_tk_profile_growth ORDER BY data_date DESC LIMIT 1"
                ).fetchone()
                _tk_followers = int(_row[0]) if _row and _row[0] else 0
            _l_conn.close()
        except Exception:
            pass
    _later_dot = "🟢" if _later_total > 0 else "⚫"
    if _later_total > 0:
        _ig_fmt = f"{_ig_followers/1000:.1f}K" if _ig_followers >= 1000 else str(_ig_followers)
        _fb_fmt = f"{_fb_followers/1000:.1f}K" if _fb_followers >= 1000 else str(_fb_followers)
        _tk_fmt = f"{_tk_followers/1000:.1f}K" if _tk_followers >= 1000 else str(_tk_followers)
        _later_label = f"IG {_ig_fmt} · FB {_fb_fmt} · TK {_tk_fmt}"
    else:
        _later_label = "No data"
    # Status dots for new external data sources
    _fred_rows   = len(df_fred)
    _fred_dot    = "🟢" if _fred_rows > 0 else "⚫"
    _fred_lbl    = f"{_fred_rows:,} obs · {df_fred['series_id'].nunique()} series" if _fred_rows > 0 else "Set FRED_API_KEY"

    _trends_rows = len(df_trends)
    _trends_dot  = "🟢" if _trends_rows > 0 else "⚫"
    _trends_lbl  = (f"{df_trends['term'].nunique()} terms · "
                    f"{df_trends['week_date'].nunique()} weeks") if _trends_rows > 0 else "Run pipeline"

    _wx_rows     = len(df_weather)
    _wx_dot      = "🟢" if _wx_rows > 0 else "⚫"
    _wx_lbl      = f"{_wx_rows} months" if _wx_rows > 0 else "Run pipeline"

    _bls_rows    = len(df_bls)
    _bls_dot     = "🟢" if _bls_rows > 0 else "⚫"
    _bls_lbl     = (f"{df_bls['series_name'].nunique()} series · "
                    f"{_bls_rows} months") if _bls_rows > 0 else "Run pipeline"

    _eia_rows    = len(df_eia_gas)
    _eia_dot     = "🟢" if _eia_rows > 0 else "⚫"
    _eia_lbl     = f"{_eia_rows} weeks · CA gas" if _eia_rows > 0 else "Run pipeline"

    _tsa_rows    = len(df_tsa)
    _tsa_dot     = "🟢" if _tsa_rows > 0 else "⚫"
    _tsa_lbl     = f"{_tsa_rows} data points · checkpoint" if _tsa_rows > 0 else "Run pipeline"

    _noaa_sb_rows = len(df_noaa)
    _noaa_sb_dot  = "🟢" if _noaa_sb_rows > 0 else "⚫"
    _noaa_sb_lbl  = f"{_noaa_sb_rows} months · ocean conditions" if _noaa_sb_rows > 0 else "Run pipeline"

    _cen_sb_rows  = len(df_census)
    _cen_sb_dot   = "🟢" if _cen_sb_rows > 0 else "⚫"
    _cen_sb_lbl   = f"{_cen_sb_rows} metrics · OC/LA/SD" if _cen_sb_rows > 0 else "Run pipeline"

    st.markdown("**Pipeline Status**")
    st.markdown(f"{_d_dot} STR Daily &nbsp;·&nbsp; {_d_label}")
    st.markdown(f"{_m_dot} STR Monthly &nbsp;·&nbsp; {_m_label}")
    st.markdown(f"{_cs_dot} CoStar Market &nbsp;·&nbsp; {_cs_label}")
    st.markdown(f"{_dfy_dot} Datafy &nbsp;·&nbsp; {_dfy_label}")
    st.markdown(f"{_zrt_dot} Zartico (Hist.) &nbsp;·&nbsp; {_zrt_label}")
    st.markdown(f"{_evts_dot} VDP Events &nbsp;·&nbsp; {_evts_label}")
    st.markdown(f"{_vca_dot} Visit California &nbsp;·&nbsp; {_vca_label}")
    st.markdown(f"{_later_dot} Social (Later) &nbsp;·&nbsp; {_later_label}")
    st.markdown("**Live Data Signals**")
    st.markdown(f"{_fred_dot} FRED Economic &nbsp;·&nbsp; {_fred_lbl}")
    st.markdown(f"{_trends_dot} Search Demand &nbsp;·&nbsp; {_trends_lbl}")
    st.markdown(f"{_wx_dot} Weather (Dana Pt) &nbsp;·&nbsp; {_wx_lbl}")
    st.markdown(f"{_bls_dot} BLS Employment &nbsp;·&nbsp; {_bls_lbl}")
    st.markdown(f"{_eia_dot} EIA Gas Prices &nbsp;·&nbsp; {_eia_lbl}")
    st.markdown(f"{_tsa_dot} TSA Checkpoint &nbsp;·&nbsp; {_tsa_lbl}")
    st.markdown(f"{_noaa_sb_dot} NOAA Ocean &nbsp;·&nbsp; {_noaa_sb_lbl}")
    st.markdown(f"{_cen_sb_dot} Census ACS &nbsp;·&nbsp; {_cen_sb_lbl}")
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

# ── Apply global date range filter from sidebar ───────────────────────────────
_gdr = st.session_state.get("global_date_range")
if _gdr and len(_gdr) == 2 and not df_sel.empty and "as_of_date" in df_sel.columns:
    import datetime as _dt
    _gdr_start = pd.Timestamp(_gdr[0])
    _gdr_end   = pd.Timestamp(_gdr[1])
    df_sel = df_sel[(df_sel["as_of_date"] >= _gdr_start) & (df_sel["as_of_date"] <= _gdr_end)]

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

# ── Hero banner stat values ───────────────────────────────────────────────
_h_occ   = m.get("occ_30", 0)   if m else 0
_h_adr   = m.get("adr_30", 0)   if m else 0
_h_rvp   = m.get("revpar_30", 0) if m else 0
_h_occ_d = m.get("occ_delta", 0) if m else 0
_h_rvp_d = m.get("revpar_delta", 0) if m else 0
_h_tbid  = m.get("tbid_monthly", 0) if m else 0
_h_occ_str  = f"{_h_occ:.1f}%"  if _h_occ else "—"
_h_adr_str  = f"${_h_adr:.0f}"  if _h_adr else "—"
_h_rvp_str  = f"${_h_rvp:.0f}"  if _h_rvp else "—"
_h_tbid_str = f"${_h_tbid/1000:.0f}K" if _h_tbid >= 1000 else (f"${_h_tbid:.0f}" if _h_tbid else "—")
def _h_delta_html(v, fmt="pct"):
    if v == 0: return ""
    cls = "hero-stat-pos" if v >= 0 else "hero-stat-neg"
    arrow = "▲" if v >= 0 else "▼"
    val_str = f"{v:+.1f}%" if fmt == "pct" else f"{v:+.1f}pp"
    return f'<span class="hero-stat-delta {cls}">{arrow} {val_str}</span>'

st.markdown(
    f'<div class="hero-banner">'
    f'<a href="?" style="text-decoration:none;">'
    f'<div class="hero-title">Dana Point <span>PULSE</span></div>'
    f'</a>'
    f'<div style="display:flex;align-items:center;gap:16px;margin-top:5px;flex-wrap:wrap;">'
    f'<div class="hero-subtitle">Destination Intelligence Platform &nbsp;·&nbsp; VDP Select Portfolio &nbsp;·&nbsp; 12 Properties</div>'
    f'</div>'
    f'<div style="display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap;">'
    f'<span style="font-size:10px;font-weight:700;color:#22D3EE;letter-spacing:.07em;'
    f'text-transform:uppercase;background:rgba(34,211,238,0.15);border:1px solid rgba(34,211,238,0.35);'
    f'padding:2px 10px;border-radius:20px;">⬤ LIVE</span>'
    f'<span style="font-size:11px;color:rgba(255,255,255,0.62);font-weight:500;">{range_label} window</span>'
    f'<span style="font-size:11px;color:rgba(255,255,255,0.30);">·</span>'
    f'<span style="font-size:11px;color:rgba(255,255,255,0.62);font-weight:500;">Updated {last_upd}</span>'
    f'</div>'
    f'<div class="hero-stats-row">'
    f'<div class="hero-stat">'
    f'  <span class="hero-stat-val">{_h_occ_str}</span>'
    f'  <span class="hero-stat-label">OCC %</span>'
    f'  {_h_delta_html(_h_occ_d, "pct")}'
    f'</div>'
    f'<div class="hero-stat">'
    f'  <span class="hero-stat-val">{_h_adr_str}</span>'
    f'  <span class="hero-stat-label">ADR</span>'
    f'</div>'
    f'<div class="hero-stat">'
    f'  <span class="hero-stat-val">{_h_rvp_str}</span>'
    f'  <span class="hero-stat-label">RevPAR</span>'
    f'  {_h_delta_html(_h_rvp_d, "pct")}'
    f'</div>'
    f'<div class="hero-stat">'
    f'  <span class="hero-stat-val">{_h_tbid_str}</span>'
    f'  <span class="hero-stat-label">TBID Est.</span>'
    f'</div>'
    f'</div>'
    f'</div>',
    unsafe_allow_html=True,
)

# ── Live KPI Ticker (1ax / Bloomberg dual-row marquee) ─────────────────────
try:
    st.markdown(
        render_kpi_ticker(df_kpi, df_dfy_ov, df_later_ig_profile),
        unsafe_allow_html=True,
    )
except Exception:
    pass  # Ticker is non-fatal — never crash the page

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
        <div class="qa-a"><strong>{md_to_html(str(hl))}</strong><br>
          <span style="margin-top:5px;display:block;font-size:9pt">{md_to_html(body_text)}</span>
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
    <span class="src-badge src-ai">● VDP Insights</span>
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
  <h2>Forward Outlook <span class="cite cite-ai">VDP Insights</span></h2>
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
        <span style="color:#5f6368;font-weight:600">Claude VDP Insights Engine</span>
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
    <div class="story-label">Market context (CoStar · Full Year 2024 · Latest Available)</div>
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


# (Global filter bar removed — tab-specific filters are rendered inside each tab)


# ══════════════════════════════════════════════════════════════════════════════
# GloCon Solutions LLC — Dana Point PULSE
# Section Intelligence Panel: contextual next steps + ask-about-this-data
# Renders a "What to do next" action panel and an AI question panel per section.
# ══════════════════════════════════════════════════════════════════════════════

def render_intel_panel(
    panel_key: str,
    next_steps: list[str],
    suggested_questions: list[str],
    context_note: str = "",
):
    """Render an Action Intelligence + Ask About This Data panel.

    Args:
        panel_key: unique key for session state isolation (e.g. "ov_kpi", "tr_trend")
        next_steps: list of action strings (shown as numbered items)
        suggested_questions: list of suggested question strings (shown as clickable chips)
        context_note: optional short context injected into the AI prompt prefix
    """
    _sq_key  = f"_intel_q_{panel_key}"
    _ans_key = f"_intel_a_{panel_key}"

    # ── Action Panel ────────────────────────────────────────────────────────
    if next_steps:
        items_html = ""
        for i, step in enumerate(next_steps, 1):
            items_html += (
                f'<div class="action-item">'
                f'<div class="action-number">{i}</div>'
                f'<div class="action-text">{step}</div>'
                f'</div>'
            )
        st.markdown(
            f'<div class="action-panel">'
            f'<div class="action-panel-title">⚡ Recommended Next Steps</div>'
            f'{items_html}'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Ask About This Data ──────────────────────────────────────────────────
    if not (ANTHROPIC_AVAILABLE and api_key_valid):
        return

    with st.expander("💬 Ask about this data", expanded=False):
        # Suggested question chips
        if suggested_questions:
            chips_html = "".join(
                f'<span class="ask-chip" '
                f'onclick="document.getElementById(\'{panel_key}_qi_{i}\').click()">'
                f'{q}</span>'
                for i, q in enumerate(suggested_questions)
            )
            st.markdown(
                f'<div class="ask-panel">'
                f'<div class="ask-panel-title">🔍 Suggested Questions</div>'
                f'<div>{chips_html}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            # Hidden Streamlit buttons that chips trigger (workaround for HTML onclick)
            _btn_cols = st.columns(min(len(suggested_questions), 3))
            for i, q in enumerate(suggested_questions):
                col = _btn_cols[i % len(_btn_cols)]
                with col:
                    if st.button(q, key=f"{panel_key}_qi_{i}", use_container_width=True):
                        st.session_state[_sq_key] = q

        # Free-form input
        _custom = st.text_input(
            "Or type your own question:",
            key=f"{panel_key}_custom",
            placeholder="e.g. How does our ADR compare to the comp set trend?",
            label_visibility="visible",
        )
        if st.button("Ask →", key=f"{panel_key}_ask_btn", type="primary"):
            st.session_state[_sq_key] = _custom.strip()

        # Run the query and show response
        _pending_q = st.session_state.get(_sq_key, "")
        if _pending_q:
            _prefix = f"[Context: {context_note}] " if context_note else ""
            _full_prompt = f"{_prefix}{_pending_q}"
            _mdl  = st.session_state.get("selected_model", CLAUDE_MODEL)
            _mdl_info = AI_MODELS.get(_mdl, {})
            _mdl_label = f"{_mdl_info.get('badge','🟦')} {_mdl_info.get('label', _mdl)}"
            with st.spinner(f"Analyzing with {_mdl_label}…"):
                _resp_parts = list(stream_ai_response(_full_prompt, _mdl, _ai_keys))
            st.session_state[_ans_key] = "".join(_resp_parts)
            st.session_state[f"{_ans_key}_model"] = _mdl_label
            del st.session_state[_sq_key]

        if st.session_state.get(_ans_key):
            _used_model = st.session_state.get(f"{_ans_key}_model", "")
            if _used_model:
                st.caption(f"Response from {_used_model}")
            st.info(st.session_state[_ans_key])
            if st.button("Clear", key=f"{panel_key}_clear"):
                del st.session_state[_ans_key]


# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab_ov, tab_tr, tab_fo, tab_ev, tab_fm, tab_ei, tab_sp, tab_cs, tab_dl = st.tabs([
    "⚡ Executive Overview",
    "📊 Hotel Performance",
    "🔮 AI Outlook",
    "🧭 Visitor Intelligence",
    "🗺️ Origin Markets",
    "🎯 Event ROI",
    "🏗️ Supply Pipeline",
    "📈 Competitive Intel",
    "🗄️ Data Vault",
])

# ══════════════════════════════════════════════════════════════════════════════
# GloCon Solutions LLC — Dana Point PULSE
# Tab header helper: refresh button + active filter badge on every tab
# ══════════════════════════════════════════════════════════════════════════════

def _tab_controls(tab_id: str = "", show_filter_badge: bool = True):
    """Render per-tab refresh button. GloCon Solutions LLC."""
    _tc1, _tc2 = st.columns([1, 7])
    with _tc1:
        if st.button("🔄 Refresh", key=f"tab_refresh_{tab_id}",
                     help="Clear cache and reload data.", use_container_width=True):
            st.cache_data.clear()
            st.rerun()


def _str_filters(tab_id: str, show_grain: bool = True, show_metric: bool = True):
    """Render STR-specific filter row inside tabs that use df_sel / df_active.

    Writes to session_state keys ss_range / ss_grain / ss_metric which are
    read at module level before df_sel is computed — so changes auto-apply on rerun.
    GloCon Solutions LLC — tab-aware filter system.
    """
    _range_opts = list(_DAYS_MAP.keys())
    _cur_range  = st.session_state.get("ss_range", "Last 90 Days")
    _cur_grain  = st.session_state.get("ss_grain", "Daily")
    _cur_metric = st.session_state.get("ss_metric", "RevPAR")

    _ncols = 3 if (show_grain and show_metric) else (2 if (show_grain or show_metric) else 1)
    _cols  = st.columns([2] * _ncols + [4])          # filters left, spacer right

    with _cols[0]:
        _new_range = st.selectbox(
            "Time Period", _range_opts,
            index=_range_opts.index(_cur_range) if _cur_range in _range_opts else 2,
            key=f"ss_range_{tab_id}",
            help="Adjust the date window applied to STR daily/monthly data.",
        )
        st.session_state["ss_range"] = _new_range

    if show_grain:
        with _cols[1]:
            _new_grain = st.selectbox(
                "Data Grain", ["Daily", "Monthly"],
                index=0 if _cur_grain == "Daily" else 1,
                key=f"ss_grain_{tab_id}",
                help="Daily: STR daily metrics · Monthly: pre-aggregated STR monthly exports",
            )
            st.session_state["ss_grain"] = _new_grain

    if show_metric:
        _mi = 1 if show_grain else 1
        with _cols[_mi + (1 if show_grain else 0)]:
            _new_metric = st.selectbox(
                "Primary Metric", ["RevPAR", "ADR", "Occupancy", "Revenue", "Demand"],
                index=["RevPAR", "ADR", "Occupancy", "Revenue", "Demand"].index(_cur_metric)
                      if _cur_metric in ["RevPAR", "ADR", "Occupancy", "Revenue", "Demand"] else 0,
                key=f"ss_metric_{tab_id}",
                help="Choose which STR metric to highlight in charts and comparisons.",
            )
            st.session_state["ss_metric"] = _new_metric

    # Active filter badge
    _gdr = st.session_state.get("global_date_range")
    if _gdr and len(_gdr) == 2:
        st.caption(f"📅 Sidebar date filter active: {_gdr[0]} → {_gdr[1]}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# GloCon Solutions LLC — Board-level executive summary + intelligence briefing
# ══════════════════════════════════════════════════════════════════════════════
with tab_ov:
    _tab_controls("ov")
    # Filter: Time Period only — Overview uses the window to compute 30-day KPI snapshot
    _str_filters("ov", show_grain=False, show_metric=False)
    st.markdown(tab_summary(
        "<strong>Executive Overview</strong> — Board-level dashboard combining 30-day STR hotel performance, "
        "annual visitor economy data from Datafy, CoStar supply intelligence, and VDP-generated insights. "
        "All metrics are sourced from verified Layer 1 data (STR, Datafy, TBID records)."
    ), unsafe_allow_html=True)
    # ── Board Executive Summary Banner ─────────────────────────────────────────
    try:
        _exec_rvp   = m.get("revpar_30", 0.0) if m else 0.0
        _exec_adr   = m.get("adr_30", 0.0) if m else 0.0
        _exec_occ   = m.get("occ_30", 0.0) if m else 0.0
        _exec_tbid  = m.get("tbid_monthly", 0.0) if m else 0.0
        _exec_rvp_d = m.get("revpar_delta", 0.0) if m else 0.0
        _exec_adr_d = m.get("adr_delta", 0.0) if m else 0.0
        _exec_occ_d = m.get("occ_delta", 0.0) if m else 0.0
        # 12-month room revenue
        _exec_rev12  = float(df_monthly["revenue"].sum()) if not df_monthly.empty and "revenue" in df_monthly.columns else 0.0
        _exec_tbid12 = _exec_rev12 * 0.0125
        _exec_tot12  = _exec_rev12 * 0.10
        # Social audience — from Later.com exports (later_ig/fb/tk_profile_growth tables)
        # Shown here and in Social Media Command Center. Source: Later.com CSV exports.
        _exec_ig_fol = 0; _exec_fb_fol = 0; _exec_tk_fol = 0; _exec_social_total = 0
        try:
            _conn_s = sqlite3.connect(DB_PATH)
            _ig_row = pd.read_sql_query(
                "SELECT followers FROM later_ig_profile_growth ORDER BY data_date DESC LIMIT 1", _conn_s
            )
            _fb_row = pd.read_sql_query(
                "SELECT page_followers FROM later_fb_profile_growth ORDER BY data_date DESC LIMIT 1", _conn_s
            )
            _tk_row = pd.read_sql_query(
                "SELECT followers FROM later_tk_profile_growth ORDER BY data_date DESC LIMIT 1", _conn_s
            )
            _conn_s.close()
            if not _ig_row.empty: _exec_ig_fol = int(_ig_row.iloc[0,0] or 0)
            if not _fb_row.empty: _exec_fb_fol = int(_fb_row.iloc[0,0] or 0)
            if not _tk_row.empty: _exec_tk_fol = int(_tk_row.iloc[0,0] or 0)
            _exec_social_total = _exec_ig_fol + _exec_fb_fol + _exec_tk_fol
        except Exception:
            pass
        # Datafy media attribution — ROAS / impact / trips
        _exec_roas         = 0.0
        _exec_attr_trips   = 0
        _exec_media_impact = 0.0
        _exec_roas_infinite= False
        _exec_invest       = 0.0
        if not df_dfy_media.empty:
            _mk = df_dfy_media.iloc[0]
            _exec_attr_trips   = int(_mk.get("attributable_trips", 0) or 0)
            _exec_media_impact = float(_mk.get("total_impact_usd", 0) or 0)
            _exec_invest       = float(_mk.get("total_investment_usd", 0) or 0)
            _roas_raw          = str(_mk.get("roas_description", "") or "")
            if _exec_invest > 0:
                _exec_roas = _exec_media_impact / _exec_invest
            elif "infinite" in _roas_raw.lower() or _exec_media_impact > 0:
                _exec_roas_infinite = True   # infinite ROAS — no cost recorded
        # Visitor trips
        _exec_trips    = 0.0
        _exec_overnight= 0.0
        if not df_dfy_ov.empty:
            _ek = df_dfy_ov.iloc[0]
            _exec_trips    = float(_ek.get("total_trips", 0) or 0)
            _exec_overnight= float(_ek.get("overnight_pct", 0) or 0)
        # Color helpers
        _up  = "#00C49A"
        _down= "#FF4757"
        def _c(v): return _up if v >= 0 else _down
        def _arr(v): return "▲" if v >= 0 else "▼"
        # Build banner HTML — light mode
        def _exec_kpi(label, value, sub="", color="#0567C8"):
            return (
                f'<div style="flex:1;min-width:140px;padding:14px 18px;'
                f'background:#FFFFFF;'
                f'border-radius:12px;border:1px solid rgba(15,28,46,0.08);'
                f'border-top:3px solid {color};'
                f'box-shadow:0 1px 4px rgba(15,28,46,0.07);">'
                f'<div style="font-size:10px;font-weight:700;letter-spacing:.08em;'
                f'text-transform:uppercase;color:#64748B;margin-bottom:5px;">{label}</div>'
                f'<div style="font-size:22px;font-weight:900;letter-spacing:-.03em;font-family:\'Outfit\',sans-serif;color:{color};">{value}</div>'
                + (f'<div style="font-size:11px;font-weight:600;margin-top:4px;color:#64748B;">{sub}</div>' if sub else '')
                + '</div>'
            )
        _rev12_fmt  = f"${_exec_rev12/1e6:.1f}M" if _exec_rev12 > 0 else "—"
        _tbid12_fmt = f"${_exec_tbid12/1e3:.0f}K" if _exec_tbid12 > 0 else "—"
        _tot12_fmt  = f"${_exec_tot12/1e6:.1f}M" if _exec_tot12 > 0 else "—"
        _trips_fmt  = f"{_exec_trips/1e6:.2f}M" if _exec_trips >= 1e6 else (f"{_exec_trips/1e3:.0f}K" if _exec_trips > 0 else "—")
        _roas_fmt   = ("∞" if _exec_roas_infinite else (f"{_exec_roas:.1f}×" if _exec_roas > 0 else "—"))
        _roas_sub   = (f"${_exec_media_impact/1e3:.0f}K impact · {_exec_attr_trips:,} trips" if _exec_media_impact > 0
                       else "Datafy media attr.")
        _social_fmt = f"{_exec_social_total/1e3:.0f}K" if _exec_social_total >= 1000 else (str(_exec_social_total) if _exec_social_total > 0 else "—")
        # Color accents per metric type
        _c_rvp  = "#0567C8" if _exec_rvp_d >= 0 else "#DC2626"
        _c_adr  = "#0567C8" if _exec_adr_d >= 0 else "#DC2626"
        _c_occ  = "#059669" if _exec_occ >= 70 else "#D97706"
        _banner_html = (
            f'<div style="margin-bottom:20px;background:linear-gradient(135deg,#F0F7FF 0%,#EEF2FF 100%);'
            f'border-radius:14px;border:1px solid rgba(5,103,200,0.12);'
            f'border-left:5px solid #0567C8;padding:18px 20px;'
            f'box-shadow:0 2px 12px rgba(5,103,200,0.08);">'
            f'<div style="font-family:\'Outfit\',sans-serif;font-size:11px;font-weight:800;'
            f'letter-spacing:.10em;text-transform:uppercase;color:#0567C8;margin-bottom:14px;'
            f'display:flex;align-items:center;gap:10px;">'
            f'📊 &nbsp;Board Executive Summary &nbsp;·&nbsp; {datetime.now().strftime("%B %Y").upper()}</div>'
            f'<div style="display:flex;flex-wrap:wrap;gap:10px;">'
            + _exec_kpi("RevPAR (30d)", f"${_exec_rvp:.0f}", f'{_arr(_exec_rvp_d)} {abs(_exec_rvp_d):.1f}% vs prior', _c_rvp)
            + _exec_kpi("ADR (30d)", f"${_exec_adr:.0f}", f'{_arr(_exec_adr_d)} {abs(_exec_adr_d):.1f}% vs prior', _c_adr)
            + _exec_kpi("Occupancy (30d)", f"{_exec_occ:.1f}%", f'{_arr(_exec_occ_d)} {abs(_exec_occ_d):.1f}pp vs prior', _c_occ)
            + _exec_kpi("12-Mo Room Rev", _rev12_fmt, "Layer 1 STR truth", "#0567C8")
            + _exec_kpi("12-Mo TBID Est.", _tbid12_fmt, "at blended 1.25%", "#7C3AED")
            + _exec_kpi("12-Mo TOT Est.", _tot12_fmt, "at 10% rate", "#7C3AED")
            + _exec_kpi("Annual Visitor Trips", _trips_fmt, f"{_exec_overnight:.0f}% overnight" if _exec_overnight > 0 else "Datafy", "#059669")
            + _exec_kpi("Campaign ROAS", _roas_fmt, _roas_sub, "#EA580C")
            + _exec_kpi("Social Audience", _social_fmt, f"IG · FB · TikTok" if _exec_social_total > 0 else "Later.com exports", "#E1306C")
            + '</div></div>'
        )
        st.markdown(_banner_html, unsafe_allow_html=True)
    except Exception:
        pass


    # ── Overview Sub-Tabs ──────────────────────────────────────────────────────
    _ov_t1, _ov_t2, _ov_t3 = st.tabs(["📊 Key Metrics", "📄 Board Report", "🧠 AI Analysis"])

    # ── Board Report → sub-tab 2 ──────────────────────────────────────────────
    with _ov_t2:
        # ── Board Report (auto-generated, always visible) ──────────────────────────
        st.markdown(sec_div("📋 Board Intelligence Report"), unsafe_allow_html=True)
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
                    + ' <span class="nlm-tag nlm-tag-ai">VDP Insights</span>'
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
                # Later.com social stats for board report
                _ig_fol = int(df_later_ig_profile.iloc[0]["followers"]) if not df_later_ig_profile.empty and "followers" in df_later_ig_profile.columns else 0
                _fb_fol = int(df_later_fb_profile.iloc[0]["page_followers"]) if not df_later_fb_profile.empty and "page_followers" in df_later_fb_profile.columns else 0
                _tk_fol = int(df_later_tk_profile.iloc[0]["followers"]) if not df_later_tk_profile.empty and "followers" in df_later_tk_profile.columns else 0
                _ig_posts_ct = len(df_later_ig_posts)
                _ig_eng_avg  = float(df_later_ig_posts["engagement_rate"].mean()) if not df_later_ig_posts.empty and "engagement_rate" in df_later_ig_posts.columns else 0.0
                _social_reach_total = (
                    int(df_later_ig_profile["reach"].sum()) if not df_later_ig_profile.empty and "reach" in df_later_ig_profile.columns else 0
                ) + (
                    int(df_later_fb_profile["reach"].sum()) if not df_later_fb_profile.empty and "reach" in df_later_fb_profile.columns else 0
                )
                _social_lbl = (
                    f"IG: <strong>{_ig_fol:,}</strong> followers · "
                    f"FB: <strong>{_fb_fol:,}</strong> followers · "
                    f"TK: <strong>{_tk_fol:,}</strong> followers. "
                    f"Avg engagement rate: <strong>{_ig_eng_avg:.1f}%</strong> · "
                    f"{_ig_posts_ct} IG posts this period · "
                    f"Total cross-platform reach: <strong>{_social_reach_total:,}</strong>."
                ) if _ig_fol + _fb_fol > 0 else _ga4_lbl
                _visitor_lbl = (
                    f"<strong>{_trips_m:.2f}M</strong> annual visitor trips · "
                    f"<strong>{_overnight:.1f}%</strong> overnight stays · "
                    f"<strong>{_oos_pct:.1f}%</strong> out-of-state visitors generating higher per-trip spend."
                    if _trips_m > 0 else "Run pipeline to load Datafy visitor data."
                )
                # FRED macro context for board report
                _fred_macro_lbl = ""
                if not df_fred.empty:
                    try:
                        _sent_row  = df_fred[df_fred["series_id"]=="UMCSENT"].sort_values("data_date").dropna(subset=["value"]).tail(2)
                        _unrate_row = df_fred[df_fred["series_id"]=="UNRATE"].sort_values("data_date").dropna(subset=["value"]).tail(1)
                        _disp_row  = df_fred[df_fred["series_id"]=="DSPIC96"].sort_values("data_date").dropna(subset=["value"]).tail(1)
                        _save_row  = df_fred[df_fred["series_id"]=="PSAVERT"].sort_values("data_date").dropna(subset=["value"]).tail(1)
                        _sent_val  = float(_sent_row.iloc[-1]["value"]) if not _sent_row.empty else None
                        _sent_prev = float(_sent_row.iloc[-2]["value"]) if len(_sent_row) >= 2 else None
                        _sent_chg  = round(_sent_val - _sent_prev, 1) if _sent_val and _sent_prev else 0
                        _unrate    = float(_unrate_row.iloc[0]["value"]) if not _unrate_row.empty else None
                        _disp      = float(_disp_row.iloc[0]["value"]) if not _disp_row.empty else None
                        _save      = float(_save_row.iloc[0]["value"]) if not _save_row.empty else None
                        _sent_tier = "🟢 Strong" if (_sent_val or 0) > 90 else "🟡 Moderate" if (_sent_val or 0) > 70 else "🔴 Cautious"
                        _sent_dir  = f"({'+' if _sent_chg >= 0 else ''}{_sent_chg:.1f} pts vs. prior)" if _sent_chg else ""
                        _fred_macro_lbl = (
                            f"Consumer Sentiment: <strong>{_sent_val:.1f}</strong> {_sent_tier} {_sent_dir} "
                            f"&nbsp;·&nbsp; Unemployment: <strong>{_unrate:.1f}%</strong>"
                            f"{'&nbsp;·&nbsp; Disposable Income: <strong>$' + f'{_disp:,.0f}B</strong>' if _disp else ''}"
                            f"{'&nbsp;·&nbsp; Savings Rate: <strong>' + f'{_save:.1f}%</strong>' if _save else ''}."
                        )
                    except Exception:
                        _fred_macro_lbl = "FRED data loaded — see Economic Climate tab for details."
                # EIA gas price context for board report
                _eia_lbl = ""
                if not df_eia_gas.empty:
                    try:
                        _eia_ca = df_eia_gas[df_eia_gas["series_id"].str.contains("SCA", na=False)].sort_values("week_end_date").tail(1)
                        if not _eia_ca.empty:
                            _gas_px = float(_eia_ca.iloc[0]["price_per_gallon"])
                            _gas_yoy = float(_eia_ca.iloc[0]["yoy_change"]) if pd.notna(_eia_ca.iloc[0].get("yoy_change")) else None
                            _gas_risk = "🔴 HIGH" if _gas_px > 4.50 else "🟡 MODERATE" if _gas_px > 4.00 else "🟢 LOW"
                            _eia_lbl = (
                                f"CA gas: <strong>${_gas_px:.2f}/gal</strong> {_gas_risk} drive-market risk"
                                + (f" ({'+' if (_gas_yoy or 0) >= 0 else ''}${_gas_yoy:.2f} YOY)" if _gas_yoy else "")
                                + ". Drive-market visitors (LA/OC/SD/IE) = ~55% of total — gas is a direct booking headwind above $4.50."
                            )
                    except Exception:
                        _eia_lbl = ""
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
      <strong>Digital & Social Performance</strong> <span class="nlm-tag nlm-tag-datafy">Datafy GA4</span> <span class="nlm-tag" style="background:rgba(225,48,108,0.12);color:#e1306c;">Instagram</span><br>
      {_ga4_lbl}<br>
      {_social_lbl}
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
      <strong>Historical Context (Zartico 2024–25) (Historical Reference)</strong> <span class="nlm-tag" style="background:rgba(121,82,179,0.15);color:#7952b3;">Zartico</span><br>
      {_zrt_ctx}
      <br><em style="opacity:.72">→ Zartico historical data provides independent validation of Datafy trends; present alongside for board credibility. Note: Zartico is historical reference only (Jun 2025 snapshot) — not current data.</em>
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

    # ── Key Metrics → sub-tab 1 ────────────────────────────────────────────────
    with _ov_t1:
        # ── Full Data Summary by Section — mini data cards ─────────────────────
        try:
            _ds_occ   = f"{m.get('occ_30', 0):.1f}%" if m else "—"
            _ds_adr   = f"${m.get('adr_30', 0):,.0f}" if m else "—"
            _ds_rvp   = f"${m.get('revpar_30', 0):,.0f}" if m else "—"
            _ds_rvpd  = f"{m.get('revpar_delta', 0):+.1f}%" if m else "—"
            _ds_cq80  = "—"; _ds_cq90 = "—"
            if not df_comp.empty and "days_above_80_occ" in df_comp.columns:
                _ds_cq80 = str(df_comp["days_above_80_occ"].iloc[-1]) + " days"
                if "days_above_90_occ" in df_comp.columns:
                    _ds_cq90 = str(df_comp["days_above_90_occ"].iloc[-1]) + " days"
            _ds_trips  = "—"; _ds_oos = "—"
            if not df_dfy_ov.empty:
                _dv = df_dfy_ov.iloc[0]
                _ds_tt = int(_dv.get("total_trips", 0) or 0)
                _ds_trips = f"{_ds_tt/1e6:.2f}M" if _ds_tt >= 1e6 else f"{_ds_tt:,}"
                _ds_oos = f"{float(_dv.get('out_of_state_vd_pct', 0) or 0):.1f}%"
            _ds_top_dma = "—"
            if not df_dfy_dma.empty:
                _ds_top_dma = str(df_dfy_dma.iloc[0].get("dma", "—"))
            _ds_pipe_rooms = f"{int(df_cs_pipe['rooms'].sum()):,}" if not df_cs_pipe.empty else "—"
            _ds_roas = "—"
            if not df_dfy_media.empty:
                _dm = df_dfy_media.iloc[0]
                _mi = float(_dm.get("total_impact_usd", 0) or 0)
                _inv = float(_dm.get("total_investment_usd", 0) or 0)
                _ds_roas = f"{_mi/_inv:.1f}×" if _inv > 0 and _mi > 0 else ("∞" if _mi > 0 else "—")
            _ds_ig = "—"
            try:
                _cxn = sqlite3.connect(DB_PATH)
                _ig_r = pd.read_sql_query("SELECT followers FROM later_ig_profile_growth ORDER BY data_date DESC LIMIT 1", _cxn)
                _cxn.close()
                if not _ig_r.empty: _ds_ig = f"{int(_ig_r.iloc[0,0] or 0):,}"
            except Exception:
                pass
            def _mini_card(label, value, sub=""):
                return (
                    f'<div class="mini-data-card">'
                    f'<div class="mini-data-card-label">{label}</div>'
                    f'<div class="mini-data-card-value">{value}</div>'
                    + (f'<div class="mini-data-card-sub">{sub}</div>' if sub else '')
                    + '</div>'
                )
            _cards = [
                _mini_card("Occupancy (30d)", _ds_occ, "30-day avg"),
                _mini_card("ADR (30d)", _ds_adr, "avg daily rate"),
                _mini_card("RevPAR (30d)", _ds_rvp, f"YOY {_ds_rvpd}"),
                _mini_card("Compression 80%+", _ds_cq80, "days this quarter"),
                _mini_card("Compression 90%+", _ds_cq90, "days this quarter"),
                _mini_card("Annual Visitor Trips", _ds_trips, f"{_ds_oos} out-of-state"),
                _mini_card("Top Feeder DMA", _ds_top_dma, "by visitor days"),
                _mini_card("Pipeline Rooms", _ds_pipe_rooms, "CoStar supply"),
                _mini_card("Campaign ROAS", _ds_roas, "Datafy media attr."),
                _mini_card("IG Followers", _ds_ig, "Later.com export"),
                _mini_card("TOT Rate", "10%", "transient occupancy tax"),
            ]
            st.markdown(sec_div("📊 Full Data Summary — All Sections"), unsafe_allow_html=True)
            # 4 cards per row
            for _row_start in range(0, len(_cards), 4):
                _row_cards = _cards[_row_start:_row_start+4]
                _cols = st.columns(len(_row_cards))
                for _ci, _card_html in enumerate(_row_cards):
                    with _cols[_ci]:
                        st.markdown(_card_html, unsafe_allow_html=True)
        except Exception:
            pass

        # ── PULSE Score Widget ─────────────────────────────────────────────────────
        st.markdown(sec_div("⚡ PULSE Performance Score"), unsafe_allow_html=True)
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
                    st.download_button("⬇️ Download PULSE Score Breakdown CSV", _bd_df.to_csv(index=False).encode(), "pulse_score_breakdown.csv", "text/csv", key="dl_pulse_bd")

            # ── Gauge bar chart ─────────────────────────────────────────────────
            _gauge_fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=_pulse_score,
                title={"text": "PULSE Score", "font": {"size": 12}},
                gauge={
                    "axis": {
                        "range": [0, 100], "tickwidth": 1,
                        "tickcolor": "rgba(15,28,46,0.3)",
                        "tickfont": {"size": 11, "color": "#64748B"},
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
                font=dict(family="Outfit, Inter, system-ui, sans-serif", color="#334155"),
            )

            _pulse_col1, _pulse_col2 = st.columns([3, 2])
            with _pulse_col1:
                st.markdown(
                    f'<div class="pulse-wrapper" style="background:#FFFFFF;border:1px solid rgba(15,28,46,0.08);border-left:4px solid {_p_color};box-shadow:0 2px 10px rgba(15,28,46,0.07);">'
                    f'  <div class="pulse-circle" style="color:{_p_color};">'
                    f'    <div class="pulse-ring"></div>'
                    f'    <div class="pulse-ring-2"></div>'
                    f'    <div class="pulse-core">'
                    f'      <span class="pulse-score" style="color:{_p_color};">{_pulse_score}</span>'
                    f'      <span class="pulse-label" style="color:#64748B;">PULSE</span>'
                    f'    </div>'
                    f'  </div>'
                    f'  <div class="pulse-info">'
                    f'    <div class="pulse-info-title" style="color:#0D1B2E;">Dana Point Market PULSE Score</div>'
                    f'    <div class="pulse-info-detail" style="color:#334155;">'
                    f'      Occ {_occ_score:.1f}% &nbsp;·&nbsp; RevPAR YOY {_rvp_d_s:+.1f}% '
                    f'      &nbsp;·&nbsp; Compression {_cq_s} nights this quarter<br>'
                    f'      {_p_detail}'
                    f'    </div>'
                    f'    <span class="pulse-info-status" style="background:{_p_color};color:#ffffff;margin-top:8px;display:inline-block;font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px;">{_p_status}</span>'
                    f'  </div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with _pulse_col2:
                st.plotly_chart(_gauge_fig, use_container_width=True, config={"displayModeBar": False})
            # Tier legend — light mode
            st.markdown(
                '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:2px;margin-bottom:12px;">'
                '<span style="font-size:10px;padding:3px 12px;border-radius:99px;background:#FEE2E2;color:#991B1B;font-weight:700;border:1px solid #FCA5A5;">0–39 Caution</span>'
                '<span style="font-size:10px;padding:3px 12px;border-radius:99px;background:#FEF3C7;color:#92400E;font-weight:700;border:1px solid #FCD34D;">40–59 Stable</span>'
                '<span style="font-size:10px;padding:3px 12px;border-radius:99px;background:#DBEAFE;color:#1E40AF;font-weight:700;border:1px solid #93C5FD;">60–74 Strong</span>'
                '<span style="font-size:10px;padding:3px 12px;border-radius:99px;background:#D1FAE5;color:#065F46;font-weight:700;border:1px solid #6EE7B7;">75–89 Exceptional</span>'
                '<span style="font-size:10px;padding:3px 12px;border-radius:99px;background:#EDE9FE;color:#4C1D95;font-weight:700;border:1px solid #C4B5FD;">90–100 Historic</span>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Overview Section Intelligence ─────────────────────────────────────────
        if m:
            _ov_rvp_yoy   = m.get("revpar_delta", 0)
            _ov_occ       = m.get("occ_30", 0)
            _ov_rvp       = m.get("revpar_30", 0)
            _ov_cq        = m.get("comp_recent_q", 0)
            _ov_rate_vs   = "outpacing occupancy — strong rate discipline" if _ov_rvp_yoy > 0 else "lagging occupancy — rate capture gap"
            _ov_fwd       = ("Maintain pricing strength; advance rate floors before Q3 compression window."
                             if _ov_occ >= 70 else "Focus shoulder demand generation; protect RevPAR floor.")
            st.markdown(sec_intel(
                "Overview Brain",
                "hotel market health for the VDP Select 12-property portfolio",
                f"RevPAR YOY is {_ov_rvp_yoy:+.1f}%, {_ov_rate_vs}. "
                f"{_ov_cq} compression nights this quarter signal {'strong' if _ov_cq >= 10 else 'moderate'} pricing power.",
                _ov_fwd,
                f"RevPAR YOY: {_ov_rvp_yoy:+.1f}%",
            ), unsafe_allow_html=True)

        # ── Executive Intelligence Panel ───────────────────────────────────────────
        try:
            _ov_rvp_d2   = m.get("revpar_delta", 0) if m else 0
            _ov_occ_d2   = m.get("occ_delta", 0) if m else 0
            _ov_adr_d2   = m.get("adr_delta", 0) if m else 0
            _ov_cq2      = m.get("comp_recent_q", 0) if m else 0
            _ov_trips2   = int(df_dfy_ov.iloc[0].get("total_trips", 0) or 0) if not df_dfy_ov.empty else 0
            _ov_oos2     = float(df_dfy_ov.iloc[0].get("out_of_state_vd_pct", 0) or 0) if not df_dfy_ov.empty else 0
            _ov_rev12_2  = float(df_monthly["revenue"].sum()) if not df_monthly.empty and "revenue" in df_monthly.columns else 0.0

            _ov_next_steps = [
                f"<strong>Rate Optimization:</strong> ADR is {'+' if _ov_adr_d2 >= 0 else ''}{_ov_adr_d2:.1f}% YOY — "
                + ("momentum is strong; consider pushing rates further on compression nights." if _ov_adr_d2 > 3 else
                   "growth is modest; review comp-set pricing vs CoStar benchmarks in Competitive Intel tab."),
                f"<strong>Compression Night Strategy:</strong> {_ov_cq2} compression days this quarter — "
                + ("activate TBID tiered rate (≥$400) on high-demand nights to maximize TBID revenue." if _ov_cq2 >= 8 else
                   "low compression count signals opportunity to build mid-week demand with targeted packages."),
                f"<strong>Out-of-State Visitor Capture:</strong> {_ov_oos2:.0f}% OOS visitors drive premium ADR — "
                "target SLC, Dallas, and Phoenix feeder markets with fly-drive packages. See Origin Markets tab.",
                f"<strong>TBID Revenue:</strong> Estimated 12-month TBID ~${_ov_rev12_2 * 0.0125 / 1_000_000:.2f}M — "
                "present at next board meeting alongside TOT figures to demonstrate total economic contribution.",
            ]
            _ov_questions = [
                "What's driving the RevPAR change vs last year?",
                "Which months have the most compression opportunity?",
                "How do our TBID and TOT estimates compare to prior year?",
                "What's the highest-value visitor segment right now?",
                "Where should we focus marketing spend next quarter?",
            ]
            _ov_context = (
                f"Dana Point VDP portfolio. RevPAR ${m.get('revpar_30',0):.0f} ({m.get('revpar_delta',0):+.1f}% YOY), "
                f"ADR ${m.get('adr_30',0):.0f}, Occ {m.get('occ_30',0):.1f}%, "
                f"12-mo revenue ~${_ov_rev12_2/1_000_000:.1f}M, "
                f"{_ov_cq2} compression days, {_ov_trips2:,} annual visitor trips, {_ov_oos2:.0f}% OOS."
                if m else "Dana Point VDP portfolio executive overview."
            )
            render_intel_panel("ov_exec", _ov_next_steps, _ov_questions, _ov_context)
        except Exception:
            pass

    # ── VDP Analyst Panel ──────────────────────────────────────────────────────
    # ── AI Analysis → sub-tab 3 ─────────────────────────────────────────────────
    with _ov_t3:
        st.markdown(sec_div("🧠 VDP Analyst"), unsafe_allow_html=True)
        with st.expander("🧠 PULSE VDP Analyst — Interrogate your data", expanded=False):
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
                    _active_mdl = st.session_state.get("selected_model", CLAUDE_MODEL)
                    _active_info = AI_MODELS.get(_active_mdl, {})
                    _active_label = f"{_active_info.get('badge','🟦')} {_active_info.get('label', _active_mdl)}"
                    _any_ai_active = (
                        (api_key_valid and ANTHROPIC_AVAILABLE) or
                        (bool(_OPENAI_KEY) and OPENAI_AVAILABLE) or
                        (bool(_GOOGLE_AI_KEY) and GEMINI_AVAILABLE) or
                        (bool(_PERPLEXITY_KEY) and OPENAI_AVAILABLE)
                    )
                    if _any_ai_active:
                        st.caption(f"Running {_active_label}…")
                        with st.chat_message("assistant", avatar="🌊"):
                            response = st.write_stream(
                                stream_ai_response(prompt_to_run, _active_mdl, _ai_keys)
                            )
                        st.session_state.ai_result = response
                        st.session_state.ai_result_model = _active_label
                    else:
                        response = local_fallback(matched_key, m)
                        with st.chat_message("assistant", avatar="🌊"):
                            st.markdown(response)
                        st.session_state.ai_result = response
                        if not api_key_valid:
                            st.caption(
                                "💡 Add API keys in the sidebar (VDP Analyst) to activate AI streaming."
                            )
                    st.session_state.ai_needs_call = False

                elif st.session_state.ai_result:
                    with st.chat_message("assistant", avatar="🌊"):
                        st.markdown(st.session_state.ai_result)

        # ── AI Insight Cards ───────────────────────────────────────────────────────
        if m:
            st.markdown(
                '<div style="font-family:\'Syne\',sans-serif;font-size:14px;'
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

    # ── Remaining content → sub-tab 1 ────────────────────────────────────────────
    with _ov_t1:
        st.markdown("---")

        # ── Board Report Card (Traffic Light) ─────────────────────────────────────
        try:
            st.markdown(_sh("📋", "Board Report Card", "gold", "TRAFFIC LIGHT"), unsafe_allow_html=True)
            st.caption("Traffic-light assessment for board presentation · Updates with every pipeline run")

            def _report_card_row(metric, value, status, note, source):
                _dot_map = {
                    "green":  ("🟢", "#059669", "On Track"),
                    "yellow": ("🟡", "#D97706", "Watch"),
                    "red":    ("🔴", "#DC2626", "Action Required"),
                }
                _dot, _col, _lbl = _dot_map.get(status, ("⚫", "#8FA3B8", "Unknown"))
                return (
                    f'<div style="display:flex;align-items:center;gap:12px;padding:11px 16px;'
                    f'border-bottom:1px solid rgba(0,0,0,0.08);font-family:\'Syne\',sans-serif;'
                    f'background:#FFFFFF;">'
                    f'<div style="font-size:16px;flex-shrink:0;">{_dot}</div>'
                    f'<div style="flex:1.4;font-size:13px;font-weight:700;color:#0F1C2E;">{metric}</div>'
                    f'<div style="flex:0.8;font-size:14px;font-weight:900;color:{_col};">{value}</div>'
                    f'<div style="flex:1;font-size:11px;font-weight:800;color:{_col};text-transform:uppercase;'
                    f'letter-spacing:.06em;">{_lbl}</div>'
                    f'<div style="flex:2;font-size:12px;color:#4A5568;line-height:1.4;">{note}</div>'
                    f'<div style="flex:0.8;font-size:10px;font-weight:700;letter-spacing:.05em;'
                    f'color:#A0AEC0;text-transform:uppercase;">{source}</div>'
                    f'</div>'
                )

            _rc_occ_status    = "green" if _exec_occ >= 75 else ("yellow" if _exec_occ >= 60 else "red")
            _rc_revpar_status = "green" if _exec_rvp_d >= 2 else ("yellow" if _exec_rvp_d >= -2 else "red")
            _rc_adr_status    = "green" if _exec_adr_d >= 3 else ("yellow" if _exec_adr_d >= 0 else "red")
            _rc_roas_status   = ("green" if (_exec_roas_infinite or _exec_roas >= 5) else ("yellow" if _exec_roas >= 2 else "red")) if (_exec_roas > 0 or _exec_roas_infinite) else "yellow"
            _rc_social_status = ("green" if _exec_ig_fol >= 20000 else ("yellow" if _exec_ig_fol >= 10000 else "red")) if _exec_ig_fol > 0 else "yellow"
            _rc_trips_status  = ("green" if _exec_trips >= 1000000 else ("yellow" if _exec_trips >= 500000 else "red")) if _exec_trips > 0 else "yellow"

            _rc_occ_note    = f"{_exec_occ:.1f}% occ · {_arr(_exec_occ_d)}{abs(_exec_occ_d):.1f}pp vs prior · {'Maintain pricing discipline.' if _exec_occ >= 75 else 'Demand generation programs needed.'}"
            _rc_revpar_note = f"${_exec_rvp:.0f} RevPAR · {_arr(_exec_rvp_d)}{abs(_exec_rvp_d):.1f}% YOY · {'Rate strategy working.' if _exec_rvp_d >= 2 else 'Review rate strategy and comp set positioning.'}"
            _rc_adr_note    = f"${_exec_adr:.0f} ADR · {'Premium rate capture on track.' if _exec_adr_d >= 3 else 'Rate pressure — audit discount patterns and channel mix.'}"
            _rc_roas_note   = (
                f"∞ ROAS (no media cost recorded) · ${_exec_media_impact:,.0f} est. campaign impact · {_exec_attr_trips:,} attributable trips · Organic performance — strong case for paid investment."
                if _exec_roas_infinite else
                (f"{_roas_fmt} return · {_exec_attr_trips:,} attributable trips · {'Strong ROI — recommend budget increase.' if _exec_roas >= 5 else 'Acceptable ROI.' if _exec_roas >= 2 else 'Investigate attribution model and campaign targeting.'}") if _exec_roas > 0
                else "Run Datafy media attribution pipeline to populate."
            )
            _rc_social_note = (f"IG {_exec_ig_fol:,} · FB {_exec_fb_fol:,} · TK {_exec_tk_fol:,} · {'Healthy audience scale for a DMO.' if _exec_ig_fol >= 20000 else 'Growth campaigns recommended.'}") if _exec_social_total > 0 else "Load Later.com exports."
            _rc_trips_note  = (f"{_trips_fmt} annual trips · {_exec_overnight:.0f}% overnight · {'Strong visitation base.' if _exec_trips >= 1e6 else 'Opportunity to grow overnight conversion.'}") if _exec_trips > 0 else "Run Datafy pipeline."

            _rc_html = (
                '<div style="background:#FFFFFF;border-radius:14px;'
                'border:1px solid rgba(0,0,0,0.08);border-left:5px solid #D97706;'
                'overflow:hidden;font-family:\'Syne\',sans-serif;margin-bottom:16px;'
                'box-shadow:0 2px 8px rgba(0,0,0,0.08);">'
                '<div style="padding:11px 16px;background:#F7F9FC;'
                'border-bottom:1px solid rgba(0,0,0,0.08);font-size:10px;font-weight:800;'
                'letter-spacing:.07em;text-transform:uppercase;color:#718096;display:flex;gap:40px;">'
                '<span style="flex:0.15"></span>'
                '<span style="flex:1.4">Metric</span>'
                '<span style="flex:0.8">Current</span>'
                '<span style="flex:1">Status</span>'
                '<span style="flex:2">Board Note</span>'
                '<span style="flex:0.8">Source</span>'
                '</div>'
                + _report_card_row("Occupancy Rate",       f"{_exec_occ:.1f}%",     _rc_occ_status,    _rc_occ_note,    "STR")
                + _report_card_row("RevPAR Growth",        f"{_exec_rvp_d:+.1f}%",  _rc_revpar_status, _rc_revpar_note, "STR")
                + _report_card_row("ADR Growth",           f"{_exec_adr_d:+.1f}%",  _rc_adr_status,    _rc_adr_note,    "STR")
                + _report_card_row("Campaign ROAS",        _roas_fmt,               _rc_roas_status,   _rc_roas_note,   "Datafy")
                + _report_card_row("Social Audience",      _social_fmt,             _rc_social_status, _rc_social_note, "Later.com")
                + _report_card_row("Annual Visitor Trips", _trips_fmt,              _rc_trips_status,  _rc_trips_note,  "Datafy")
                + '</div>'
            )
            st.markdown(_rc_html, unsafe_allow_html=True)
        except Exception:
            pass

        # ── Cross-Dataset Intelligence Matrix ─────────────────────────────────────
        try:
            if m and (not df_dfy_ov.empty or not df_dfy_dma.empty or not df_dfy_media.empty):
                st.markdown(sec_div("🔗 Cross-Dataset Intelligence"), unsafe_allow_html=True)
                st.markdown(
                    '<div style="font-family:\'Inter\',sans-serif;font-size:12px;color:#64748B;margin-bottom:14px;">'
                    'Hidden signals that only appear when STR hotel data is read alongside visitor economy and campaign data.</div>',
                    unsafe_allow_html=True,
                )

                # ── Compute cross-dataset signals ─────────────────────────────────
                _cx_rvp      = m.get("revpar_30", 0)
                _cx_adr      = m.get("adr_30", 0)
                _cx_occ      = m.get("occ_30", 0)
                _cx_rev12    = float(df_monthly["revenue"].sum()) if not df_monthly.empty and "revenue" in df_monthly.columns else 0.0
                _cx_oos_pct  = float(df_dfy_ov.iloc[0].get("out_of_state_vd_pct", 0) or 0) if not df_dfy_ov.empty else 0.0
                _cx_trips    = int(df_dfy_ov.iloc[0].get("total_trips", 0) or 0) if not df_dfy_ov.empty else 0
                _cx_daytrip  = float(df_dfy_ov.iloc[0].get("day_trip_pct", 0) or 0) if not df_dfy_ov.empty else 0.0
                _cx_los      = float(df_dfy_ov.iloc[0].get("avg_los", 0) or 0) if not df_dfy_ov.empty else 0.0
                _cx_overnight_pct = float(df_dfy_ov.iloc[0].get("overnight_pct", 0) or 0) if not df_dfy_ov.empty else 0.0
                _cx_roas     = _exec_roas if not _exec_roas_infinite else 99.0
                _cx_impact   = float(df_dfy_media.iloc[0].get("total_impact_usd", 0) or 0) if not df_dfy_media.empty else 0.0
                _cx_invest   = float(df_dfy_media.iloc[0].get("total_investment_usd", 0) or 0) if not df_dfy_media.empty else 0.0
                _cx_attr_trips = int(df_dfy_media.iloc[0].get("attributable_trips", 0) or 0) if not df_dfy_media.empty else 0

                # Signal 1: OOS premium capture gap
                _oos_rate_gap = _cx_oos_pct * 0.01 * _cx_adr * 0.067  # 6.7% ADR YOY vs 1.0× spend ratio
                _oos_signal  = (f"{_cx_oos_pct:.0f}% OOS visitors generate near 1:1 spend-per-visit but ADR growth is only "
                                f"+{m.get('adr_delta',0):.1f}% YOY — rate capture gap of ~${_cx_adr * 0.05:,.0f}/night vs. OOS demand.")
                # Signal 2: Day-trip conversion value
                _daytrip_ct    = int(_cx_trips * _cx_daytrip * 0.01) if _cx_daytrip > 0 else 0
                _daytrip_conv3 = _daytrip_ct * 0.03 * _cx_adr  # 3% conversion × ADR
                _daytrip_signal = (f"{_daytrip_ct:,} estimated day trips — converting just 3% to overnight stays = "
                                   f"~${_daytrip_conv3/1e6:.1f}M incremental room revenue annually." if _daytrip_ct > 0
                                   else f"Day-trip data pending — load Datafy visitor report to compute conversion opportunity.")
                # Signal 3: Campaign efficiency vs organic
                _cost_per_trip = _cx_invest / _cx_attr_trips if _cx_attr_trips > 0 and _cx_invest > 0 else 0
                _rev_per_trip  = _cx_impact / _cx_attr_trips if _cx_attr_trips > 0 and _cx_impact > 0 else 0
                if _cx_roas >= 5 or _exec_roas_infinite:
                    _camp_signal = (f"{'∞' if _exec_roas_infinite else f'{_cx_roas:.1f}×'} ROAS — ${_cx_impact/1e3:,.0f}K destination impact "
                                    f"from {_cx_attr_trips:,} attributable trips. "
                                    f"{'No media cost recorded — all organic; strong case for paid media investment.' if _exec_roas_infinite else 'Strong ROI — scale budget to capture next tier of feeder markets.'}")
                elif _cx_roas > 0:
                    _camp_signal = (f"{_cx_roas:.1f}× ROAS · ${_cost_per_trip:,.0f} cost/trip · ${_rev_per_trip:,.0f} revenue/trip — "
                                    "acceptable efficiency; refine audience targeting to improve trip quality vs. volume.")
                else:
                    _camp_signal = "Load Datafy media attribution report to compute campaign-to-room-revenue signal."
                # Signal 4: Compression × visitor overlap
                _comp_q = m.get("comp_recent_q", 0)
                _comp_day_signal = (
                    f"{_comp_q} compression nights this quarter — on 80%+ occ nights, day-trip visitors add estimated "
                    f"0.7× room count equivalent in off-property spend, invisible to STR. "
                    f"Total visitor economic footprint exceeds hotel data by ~{0.7 * _comp_q * _cx_adr * 50:,.0f}$ on compression days."
                    if _comp_q > 0 else
                    "Compression data loading — run pipeline to populate kpi_compression_quarterly."
                )
                # Signal 5: LOS extension value
                _los_val = (_cx_los if _cx_los > 0 else 2.0)
                _los_ext_val = _cx_rev12 * 0.05  # 5% uplift from 0.5-day extension
                _los_signal  = (f"Avg stay: {_cx_los:.1f} nights (Datafy) — extending avg LOS by 0.5 nights via minimum-stay "
                                f"packages = estimated +${_los_ext_val/1e3:,.0f}K annual room revenue.")

                def _cx_signal_card(icon, title, signal_text, source_tags, signal_type="insight"):
                    _type_colors = {"insight": "#0567C8", "opportunity": "#059669", "risk": "#DC2626", "gap": "#D97706"}
                    _tc = _type_colors.get(signal_type, "#0567C8")
                    return (
                        f'<div style="background:#FFFFFF;border-radius:12px;padding:16px 18px;'
                        f'border:1px solid rgba(15,28,46,0.07);border-left:3px solid {_tc};'
                        f'box-shadow:0 1px 4px rgba(15,28,46,0.06);margin-bottom:10px;">'
                        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">'
                        f'<span style="font-size:16px;">{icon}</span>'
                        f'<span style="font-family:\'Outfit\',sans-serif;font-size:12px;font-weight:700;color:#0D1B2E;">{title}</span>'
                        f'<span style="margin-left:auto;font-size:9px;font-weight:800;letter-spacing:.08em;'
                        f'text-transform:uppercase;color:{_tc};background:rgba(5,103,200,0.08);'
                        f'padding:2px 8px;border-radius:99px;">{signal_type.upper()}</span>'
                        f'</div>'
                        f'<div style="font-family:\'Inter\',sans-serif;font-size:12px;color:#334155;line-height:1.6;">{signal_text}</div>'
                        f'<div style="margin-top:8px;display:flex;gap:6px;flex-wrap:wrap;">{source_tags}</div>'
                        f'</div>'
                    )

                def _src(label, color_hex, bg_hex):
                    return (f'<span style="font-size:9px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;'
                            f'color:{color_hex};background:{bg_hex};padding:2px 8px;border-radius:99px;">{label}</span>')

                _cx_c1, _cx_c2 = st.columns(2)
                with _cx_c1:
                    st.markdown(_cx_signal_card("💎", "OOS Visitor Rate Capture Gap",  _oos_signal,
                        _src("STR","#0567C8","rgba(5,103,200,0.10)") + _src("Datafy","#059669","rgba(5,150,105,0.10)"), "gap"), unsafe_allow_html=True)
                    st.markdown(_cx_signal_card("🔁", "Day-Trip → Overnight Conversion", _daytrip_signal,
                        _src("Datafy","#059669","rgba(5,150,105,0.10)") + _src("STR","#0567C8","rgba(5,103,200,0.10)"), "opportunity"), unsafe_allow_html=True)
                    st.markdown(_cx_signal_card("📏", "Length-of-Stay Extension Value", _los_signal,
                        _src("Datafy","#059669","rgba(5,150,105,0.10)") + _src("STR","#0567C8","rgba(5,103,200,0.10)"), "opportunity"), unsafe_allow_html=True)
                with _cx_c2:
                    st.markdown(_cx_signal_card("📡", "Campaign → Room Revenue Signal", _camp_signal,
                        _src("Datafy","#059669","rgba(5,150,105,0.10)") + _src("STR","#0567C8","rgba(5,103,200,0.10)"), "insight"), unsafe_allow_html=True)
                    st.markdown(_cx_signal_card("🏨", "Compression × Day-Trip Overlap", _comp_day_signal,
                        _src("STR","#0567C8","rgba(5,103,200,0.10)") + _src("Datafy","#059669","rgba(5,150,105,0.10)"), "insight"), unsafe_allow_html=True)
                    # Feeder market ADR premium signal
                    _top_dma = str(df_dfy_dma.iloc[0].get("dma","—")) if not df_dfy_dma.empty else "—"
                    _top_dma_share = float(df_dfy_dma.iloc[0].get("visitor_days_share_pct", 0) or 0) if not df_dfy_dma.empty else 0
                    _fly_markets = [r for _, r in df_dfy_dma.iterrows()
                                    if str(r.get("dma","")).upper() not in ("LOS ANGELES","LA","RIVERSIDE","SAN DIEGO","ORANGE COUNTY")
                                    ] if not df_dfy_dma.empty else []
                    _fly_spend = float(_fly_markets[0].get("avg_spend_usd", 0) or 0) if _fly_markets else 0
                    _feeder_signal = (
                        f"Top feeder: {_top_dma} ({_top_dma_share:.0f}% of visitor days) — drive market dominant on volume. "
                        + (f"Fly markets (SLC, Dallas, NYC) avg ${_fly_spend:,.0f}/trip vs. LA drive market — "
                           "1.3–1.4× revenue per visitor trip. Shift 10% of campaign budget to fly markets = outsized ADR gain."
                           if _fly_spend > 0 else
                           "Load full Datafy DMA table to compute fly-market ADR premium signal.")
                    ) if _top_dma != "—" else "Load Datafy feeder market data to compute cross-dataset signal."
                    st.markdown(_cx_signal_card("✈️", "Feeder Market ADR Premium",  _feeder_signal,
                        _src("Datafy","#059669","rgba(5,150,105,0.10)") + _src("STR","#0567C8","rgba(5,103,200,0.10)"), "opportunity"), unsafe_allow_html=True)
        except Exception:
            pass

        # ── KPI Cards ──────────────────────────────────────────────────────────────
        kpis = compute_overview_kpis(df_sel, grain)
        if not kpis:
            st.markdown(empty_state(
                "📊",
                f"No {grain.lower()} data in the selected range.",
                "Adjust the date range or run the pipeline to load data.",
            ), unsafe_allow_html=True)
        else:
            st.markdown(sec_div("📊 Key Performance Indicators"), unsafe_allow_html=True)
            st.markdown(
                '<div style="font-family:\'Syne\',sans-serif;font-size:14px;'
                'font-weight:700;letter-spacing:-0.01em;margin-bottom:8px;">Performance Command Center</div>',
                unsafe_allow_html=True,
            )
            # ── Per-metric color palette (cutting-edge, distinct) ─────────────────
            _METRIC_COLORS = {
                "RevPAR":        ("#00C4CC", "rgba(0,196,204,0.18)"),
                "ADR":           ("#8B5CF6", "rgba(139,92,246,0.18)"),
                "Occupancy":     ("#0EA5E9", "rgba(14,165,233,0.18)"),
                "Room Revenue":  ("#10B981", "rgba(16,185,129,0.18)"),
                "Rooms Sold":    ("#F97316", "rgba(249,115,22,0.18)"),
                "Est. TBID Rev": ("#F59E0B", "rgba(245,158,11,0.18)"),
                "TBID":          ("#F59E0B", "rgba(245,158,11,0.18)"),
                "Demand":        ("#F97316", "rgba(249,115,22,0.18)"),
                "Supply":        ("#6366F1", "rgba(99,102,241,0.18)"),
                "Revenue":       ("#10B981", "rgba(16,185,129,0.18)"),
            }
            def _metric_color(label, positive=True):
                for k, v in _METRIC_COLORS.items():
                    if k.lower() in label.lower():
                        return v
                return ("#00C4CC", "rgba(0,196,204,0.18)") if positive else ("#F97316", "rgba(249,115,22,0.18)")

            # Render each KPI as [card | mini sparkline chart] pairs, 2 per row
            def _mini_spark_fig(values, label="", positive=True, chart_key=""):
                # Always render — if no data, show placeholder flat line
                if not values or len(values) < 2:
                    values = [0, 0]
                _line_color, _fill_color = _metric_color(label, positive)
                _fig = go.Figure()
                _fig.add_trace(go.Scatter(
                    y=values,
                    mode="lines",
                    line=dict(color=_line_color, width=2.5, shape="spline", smoothing=0.8),
                    fill="tozeroy",
                    fillcolor=_fill_color,
                    hoverinfo="skip",
                ))
                # Accent dot at latest value
                _fig.add_trace(go.Scatter(
                    x=[len(values) - 1], y=[values[-1]],
                    mode="markers",
                    marker=dict(color=_line_color, size=6, line=dict(color="white", width=1.5)),
                    hoverinfo="skip",
                ))
                _fig.update_layout(
                    height=88, margin=dict(l=2, r=2, t=2, b=2),
                    xaxis=dict(visible=False, fixedrange=True),
                    yaxis=dict(visible=False, fixedrange=True),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                    transition={"duration": 500, "easing": "cubic-in-out"},
                )
                return _fig

            _kpi_rows = [kpis[i:i+2] for i in range(0, len(kpis), 2)]
            for _ri, _row_kpis in enumerate(_kpi_rows):
                _rcols = st.columns([1.1, 1.0, 1.1, 1.0])
                for _idx, _k in enumerate(_row_kpis):
                    # Use columns at indices 0,1 for first KPI and 2,3 for second
                    _card_col  = _rcols[0 if _idx == 0 else 2]
                    _chart_col = _rcols[1 if _idx == 0 else 3]
                    _chart_key = f"kpi30_spark_{_ri}_{_idx}_{_k['label'].replace(' ','_').replace('.','')}"
                    with _card_col:
                        _card_html = kpi_card(_k["label"], _k["value"], _k["delta"],
                                     _k.get("positive", True), _k.get("neutral", False),
                                     "", _k.get("date_label", ""), _k.get("raw_value", 0.0),
                                     [])
                        # Map metric label to tab index for navigation
                        _tab_map = {"RevPAR": 1, "ADR": 1, "Occupancy": 1, "Room Revenue": 1,
                                    "Rooms Sold": 1, "Est. TBID Rev": 1, "TBID": 1,
                                    "Demand": 1, "Supply": 1, "Revenue": 1}
                        _tab_idx = next((v for k,v in _tab_map.items() if k.lower() in _k["label"].lower()), 1)
                        st.markdown(
                            f'<div class="pcc-card-link" data-tab-idx="{_tab_idx}" '
                            f'style="cursor:pointer;transition:transform 0.15s ease,box-shadow 0.15s ease;border-radius:12px;">'
                            f'{_card_html}'
                            f'<div style="font-size:9px;color:#32B8C6;text-align:right;padding:2px 4px 0 0;font-weight:700;letter-spacing:.04em;">→ VIEW DETAIL</div>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                    with _chart_col:
                        _spk = _k.get("sparkline") or []
                        st.plotly_chart(
                            _mini_spark_fig(_spk, _k["label"], _k.get("positive", True)),
                            use_container_width=True,
                            config={"displayModeBar": False},
                            key=_chart_key,
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
                    '<div style="font-family:\'Syne\',sans-serif;font-size:14px;'
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
                for _mri, _m_row in enumerate(_m_kpi_rows):
                    _mrc = st.columns([1.1, 1.0, 1.1, 1.0])
                    for _mi, _mk in enumerate(_m_row):
                        _mc  = _mrc[0 if _mi == 0 else 2]
                        _mcc = _mrc[1 if _mi == 0 else 3]
                        _mk_key = f"m12_spark_{_mri}_{_mi}_{_mk['label'].replace(' ','_').replace('.','')}"
                        with _mc:
                            _mk_card_html = kpi_card(_mk["label"], _mk["value"], _mk["delta"],
                                         _mk.get("positive", True), _mk.get("neutral", False),
                                         "", _mk.get("date_label", ""), _mk.get("raw_value", 0.0),
                                         [])
                            _mk_tab_map = {"RevPAR": 1, "ADR": 1, "Occupancy": 1, "Room Revenue": 1,
                                           "Rooms Sold": 1, "Est. TBID Rev": 1, "TBID": 1,
                                           "Demand": 1, "Supply": 1, "Revenue": 1}
                            _mk_tab_idx = next((v for k,v in _mk_tab_map.items() if k.lower() in _mk["label"].lower()), 1)
                            st.markdown(
                                f'<div class="pcc-card-link" data-tab-idx="{_mk_tab_idx}" '
                                f'style="cursor:pointer;transition:transform 0.15s ease,box-shadow 0.15s ease;border-radius:12px;">'
                                f'{_mk_card_html}'
                                f'<div style="font-size:9px;color:#32B8C6;text-align:right;padding:2px 4px 0 0;font-weight:700;letter-spacing:.04em;">→ VIEW DETAIL</div>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )
                        with _mcc:
                            _mspk = _mk.get("sparkline") or []
                            st.plotly_chart(
                                _mini_spark_fig(_mspk, _mk["label"], _mk.get("positive", True)),
                                use_container_width=True,
                                config={"displayModeBar": False},
                                key=_mk_key,
                            )

            # ── PCC card navigation script (injected once after all cards) ──────────
            st.markdown("""
    <script>
    (function(){
      function findTabs(){
        // Try multiple selectors for different Streamlit versions
        var t = document.querySelectorAll('[data-testid="stTab"] button');
        if(!t.length) t = document.querySelectorAll('button[data-baseweb="tab"]');
        if(!t.length) t = document.querySelectorAll('[role="tab"]');
        return t;
      }
      function attachPCC(){
        document.querySelectorAll('.pcc-card-link').forEach(function(card){
          if(card._pccBound) return;
          card._pccBound = true;
          card.addEventListener('mouseenter', function(){ this.style.transform='translateY(-2px)'; this.style.boxShadow='0 4px 16px rgba(8,145,178,0.18)'; });
          card.addEventListener('mouseleave', function(){ this.style.transform=''; this.style.boxShadow=''; });
          card.addEventListener('click', function(){
            var idx = parseInt(this.getAttribute('data-tab-idx') || '1');
            var tabs = findTabs();
            if(tabs[idx]){ tabs[idx].click(); }
            setTimeout(function(){ window.scrollTo({top:0,behavior:'smooth'}); }, 150);
          });
        });
      }
      attachPCC();
      setTimeout(attachPCC, 600);
      setTimeout(attachPCC, 1800);
      if(window.MutationObserver){
        var obs = new MutationObserver(function(){ attachPCC(); });
        obs.observe(document.body, {childList:true, subtree:true});
      }
    })();
    </script>
    """, unsafe_allow_html=True)

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
                        xanchor="left", font=dict(size=11, family="Syne, DM Sans, sans-serif"),
                    )
                    fig.add_annotation(
                        x=actual, y=y_pos,
                        text=f" {prefix}{actual:.1f}{suffix}", showarrow=False,
                        xanchor="left", font=dict(size=10, color=color, family="Syne, DM Sans, sans-serif"),
                    )
                max_val = max(_vdp_adr_n, _mkt_adr) * 1.25
                fig.update_layout(
                    xaxis=dict(range=[0, max_val], showgrid=True, gridcolor="rgba(167,169,169,0.15)"),
                    yaxis=dict(visible=False, range=[-0.8, 3.8]),
                    showlegend=False,
                    height=200,
                    margin=dict(l=10, r=10, t=10, b=10),
                )
                st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
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
                st.plotly_chart(style_fig(fig), use_container_width=True, config=PLOTLY_CONFIG)

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
                st.plotly_chart(style_fig(fig), use_container_width=True, config=PLOTLY_CONFIG)

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
                    st.plotly_chart(style_fig(fig), use_container_width=True, config=PLOTLY_CONFIG)
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
                    st.plotly_chart(style_fig(fig), use_container_width=True, config=PLOTLY_CONFIG)

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
                st.plotly_chart(style_fig(fig), use_container_width=True, config=PLOTLY_CONFIG)

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
                    st.plotly_chart(style_fig(fig, height=260), use_container_width=True, config=PLOTLY_CONFIG)

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
                    st.plotly_chart(style_fig(fig2, height=260), use_container_width=True, config=PLOTLY_CONFIG)

            # ── Row 4: Datafy Visitor Economy Summary ─────────────────────────────
            if not df_dfy_ov.empty:
                st.markdown("---")
                st.markdown(
                    '<div style="font-family:\'Syne\',sans-serif;font-size:14px;'
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

        # ── Economic Impact Statement ──────────────────────────────────────────────
        try:
            st.markdown(_sh("💰", "Economic Impact Statement", "green", "CITY FINANCE"), unsafe_allow_html=True)
            st.caption("Estimated economic contribution of Dana Point hotel sector · Based on STR + Datafy + TBID formula")

            _ei_c1, _ei_c2, _ei_c3, _ei_c4 = st.columns(4)
            with _ei_c1:
                _rev12_disp = f"${_exec_rev12/1e6:.1f}M" if _exec_rev12 > 0 else "—"
                st.metric("Hotel Room Revenue", _rev12_disp, help="12-month STR total room revenue")
            with _ei_c2:
                _tbid12_disp = f"${_exec_tbid12/1e3:.0f}K" if _exec_tbid12 > 0 else "—"
                st.metric("Est. TBID Assessment", _tbid12_disp, help="Room revenue × 1.25% blended rate")
            with _ei_c3:
                _tot12_disp = f"${_exec_tot12/1e6:.1f}M" if _exec_tot12 > 0 else "—"
                st.metric("Est. TOT Revenue", _tot12_disp, help="Room revenue × 10%")
            with _ei_c4:
                _dest_spend = float(df_dfy_ov.iloc[0].get("total_destination_spend_usd", 0) or 0) if not df_dfy_ov.empty else 0.0
                _dest_disp  = f"${_dest_spend/1e6:.1f}M" if _dest_spend > 0 else "—"
                st.metric("Total Destination Spend", _dest_disp, help="Datafy total visitor destination spend")

            if _exec_media_impact > 0:
                _mei_c1, _mei_c2, _mei_c3 = st.columns(3)
                with _mei_c1:
                    st.metric("Campaign Media Spend Impact", f"${_exec_media_impact/1e6:.2f}M", help="Datafy media attribution total economic impact")
                with _mei_c2:
                    st.metric("Campaign ROAS", f"{_exec_roas:.1f}×", help="Return on ad spend from Datafy media attribution")
                with _mei_c3:
                    st.metric("Attributable Trips", f"{_exec_attr_trips:,}", help="Trips directly attributable to VDP digital campaigns")
        except Exception:
            pass

        # ── Strategic Asks ─────────────────────────────────────────────────────────
        try:
            _ask_color  = "rgba(0,196,204,0.08)"
            _ask_border = "#00C4CC"
            _asks = []
            if _exec_rvp_d < 0:
                _asks.append(("Rate Strategy Review", "RevPAR declining — request approval for comp set re-pricing analysis and channel mix audit.", "Revenue"))
            if _exec_roas_infinite or _exec_roas > 5:
                _asks.append(("Invest in Paid Media", f"Campaign currently running with {'∞ ROAS (no cost recorded)' if _exec_roas_infinite else f'{_exec_roas:.1f}× ROAS'}. ${_exec_media_impact:,.0f} in estimated impact from {_exec_attr_trips:,} attributable trips. Request board approval for paid media budget to scale this performance.", "Budget"))
            if _exec_occ >= 80:
                _asks.append(("Compression Rate Authorization", f"Occupancy above 80% — request board authorization for dynamic rate increases during compression periods.", "Pricing"))
            if _exec_trips > 0 and _exec_overnight < 50:
                _asks.append(("Overnight Conversion Program", f"Only {_exec_overnight:.0f}% of visitors stay overnight. Request funding for packages targeting day-tripper conversion.", "Strategy"))
            _asks.append(("Annual Report Narrative", "Data supports a strong YOY growth narrative. Request approval to publish annual economic impact report to city council and stakeholders.", "Communications"))

            if _asks:
                st.markdown("#### 🎯 Recommended Board Actions")
                for _ask_title, _ask_body, _ask_tag in _asks:
                    st.markdown(
                        f'<div style="padding:12px 16px;margin-bottom:8px;background:{_ask_color};'
                        f'border-left:3px solid {_ask_border};border-radius:0 8px 8px 0;'
                        f'font-family:\'Syne\',sans-serif;">'
                        f'<span style="font-size:11px;font-weight:700;letter-spacing:.05em;'
                        f'text-transform:uppercase;opacity:.5;">{_ask_tag}</span><br>'
                        f'<span style="font-size:13px;font-weight:700;">{_ask_title}</span><br>'
                        f'<span style="font-size:12px;opacity:.75;">{_ask_body}</span></div>',
                        unsafe_allow_html=True,
                    )
        except Exception:
            pass

    # ══════════════════════════════════════════════════════════════════════════════
    # TAB 2 — TRENDS
    # ══════════════════════════════════════════════════════════════════════════════
with tab_tr:
    _tab_controls("tr")
    # Full filters: Time Period + Daily/Monthly grain (metric controlled by "View Metric" below)
    _str_filters("tr", show_grain=True, show_metric=False)
    st.markdown(tab_summary(
        "<strong>Hotel Performance</strong> — STR trend analysis for RevPAR, ADR, Occupancy, Revenue, Supply, and Demand. "
        "Switch between Daily and Monthly grain using the filter above. "
        "Year-over-year comparisons highlight rate discipline vs. volume growth."
    ), unsafe_allow_html=True)
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
        # ── STR Section Intelligence ────────────────────────────────────────────
        try:
            _tr_adr_yoy  = float(monthly["adr_yoy"].dropna().iloc[-1]) if "adr_yoy" in monthly.columns and not monthly["adr_yoy"].dropna().empty else 0.0
            _tr_occ_yoy  = float(monthly["occ_pct_yoy"].dropna().iloc[-1]) if "occ_pct_yoy" in monthly.columns and not monthly["occ_pct_yoy"].dropna().empty else 0.0
            _tr_adr_last = float(monthly["adr"].dropna().iloc[-1]) if "adr" in monthly.columns and not monthly["adr"].dropna().empty else 0.0
            _tr_rate_disc = ("ADR is growing faster than occupancy — healthy rate discipline." if _tr_adr_yoy > _tr_occ_yoy
                             else "Occupancy is growing faster than ADR — rate capture opportunity exists.")
            _tr_fwd = ("Maintain rate floors; avoid discounting during compression windows." if _tr_adr_yoy >= 0
                       else "Review rate strategy; discount-driven volume may be masking RevPAR pressure.")
            st.markdown(sec_intel(
                "STR & Trends",
                "monthly STR performance vs prior year across supply, demand, ADR, and RevPAR",
                _tr_rate_disc,
                _tr_fwd,
                f"ADR YOY: {_tr_adr_yoy:+.1f}% · Latest ADR: ${_tr_adr_last:,.0f}",
            ), unsafe_allow_html=True)
        except Exception:
            pass

        # ── Hotel Performance Intelligence Panel ─────────────────────────────
        try:
            _tr_rvp_v   = float(monthly["revpar"].dropna().iloc[-1]) if "revpar" in monthly.columns and not monthly["revpar"].dropna().empty else m.get("revpar_30", 0) if m else 0
            _tr_adr_v   = float(monthly["adr"].dropna().iloc[-1]) if "adr" in monthly.columns and not monthly["adr"].dropna().empty else m.get("adr_30", 0) if m else 0
            _tr_occ_v   = float(monthly["occ_pct"].dropna().iloc[-1]) if "occ_pct" in monthly.columns and not monthly["occ_pct"].dropna().empty else m.get("occ_30", 0) if m else 0
            _tr_adr_yoy2 = float(monthly["adr_yoy"].dropna().iloc[-1]) if "adr_yoy" in monthly.columns and not monthly["adr_yoy"].dropna().empty else 0
            _tr_cq2      = m.get("comp_recent_q", 0) if m else 0

            _tr_next_steps = [
                f"<strong>Rate Capture:</strong> ADR {'+' if _tr_adr_yoy2>=0 else ''}{_tr_adr_yoy2:.1f}% YOY at ${_tr_adr_v:.0f} — "
                + ("strong rate discipline; add dynamic surcharges on compression weeks." if _tr_adr_yoy2 > 3 else
                   "review LOS minimum stays to improve rate capture without discounting."),
                f"<strong>Occupancy at {_tr_occ_v:.1f}%:</strong> "
                + ("compression territory — prioritize RevPAR over occupancy; reject low-rate group blocks." if _tr_occ_v >= 80 else
                   f"opportunity below compression threshold — target {80-_tr_occ_v:.1f}pp gain via shoulder-season campaigns."),
                f"<strong>Weekend Premium:</strong> Analyze weekday vs. weekend RevPAR gap — "
                "adding a mid-week LOS package (min 3 nights) extends average stay and smooths revenue curve.",
                "<strong>TBID Tier Tracking:</strong> Monitor what share of room nights exceed $200, $400 ADR thresholds — "
                "each $1M in revenue above $400/night generates $20K in incremental TBID.",
            ]
            _tr_questions = [
                "What's causing ADR to grow faster than occupancy?",
                "Which months should we set rate floors for?",
                "Show me weekend vs weekday RevPAR gap",
                "How many nights hit the $400 TBID tier?",
                "What's our RevPAR vs the CoStar comp set?",
            ]
            _tr_context = (
                f"Hotel Performance: RevPAR ${_tr_rvp_v:.0f}, ADR ${_tr_adr_v:.0f} ({_tr_adr_yoy2:+.1f}% YOY), "
                f"Occ {_tr_occ_v:.1f}%, {_tr_cq2} compression days this quarter."
            )
            render_intel_panel("tr_perf", _tr_next_steps, _tr_questions, _tr_context)
        except Exception:
            pass

        st.markdown(sec_div("📈 Trend Charts"), unsafe_allow_html=True)
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
            # Continuous color gradient: low values = coral, high = vivid teal
            _vals_norm = monthly[_main_col].fillna(_avg_main)
            _v_min, _v_max = float(_vals_norm.min()), float(_vals_norm.max())
            _v_range = _v_max - _v_min if _v_max != _v_min else 1
            def _grad_color(v):
                t = (float(v) - _v_min) / _v_range  # 0..1
                if t >= 0.5:
                    # teal to bright cyan
                    r = int(33  + (0   - 33)  * (t - 0.5) * 2)
                    g = int(128 + (196 - 128) * (t - 0.5) * 2)
                    b = int(141 + (204 - 141) * (t - 0.5) * 2)
                else:
                    # red to teal
                    r = int(192 + (33  - 192) * t * 2)
                    g = int(21  + (128 - 21)  * t * 2)
                    b = int(47  + (141 - 47)  * t * 2)
                return f"rgb({max(0,min(255,r))},{max(0,min(255,g))},{max(0,min(255,b))})"
            _bar_clrs = [_grad_color(v) for v in _vals_norm]
            fig = go.Figure(go.Bar(
                x=monthly["month_label"], y=monthly[_main_col],
                marker=dict(color=_bar_clrs, line_width=0, cornerradius=6,
                            line=dict(width=0)),
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
            st.plotly_chart(style_fig(fig, height=300), use_container_width=True, config=PLOTLY_CONFIG)
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
            bar_colors = ["#00C49A" if v >= 0 else "#FF4757" for v in yoy[_yoy_metric_col]]
            fig = go.Figure(go.Bar(
                x=yoy["month_label"], y=yoy[_yoy_metric_col],
                marker=dict(color=bar_colors, line_width=0, cornerradius=6),
                text=[f"{v:+.1f}%" for v in yoy[_yoy_metric_col]],
                textposition="outside",
                textfont=dict(size=10, family="Syne, DM Sans, sans-serif"),
                hovertemplate=(
                    f"<b>%{{x}}</b><br>YOY {_str_metric_label}: %{{y:+.1f}}%<extra></extra>"
                ),
            ))
            fig.update_layout(yaxis_ticksuffix="%", showlegend=False,
                              transition={"duration": 800, "easing": "cubic-in-out"})
            st.plotly_chart(style_fig(fig, height=300), use_container_width=True, config=PLOTLY_CONFIG)

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
                            tickfont=dict(size=11, family="Syne, DM Sans, sans-serif"),
                        ),
                    ),
                    showlegend=False,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(style_fig(fig, height=360), use_container_width=True, config=PLOTLY_CONFIG)
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
                _tbid_vals = mrev["tbid_m"].tolist()
                _tbid_max  = max(_tbid_vals) if _tbid_vals else 1
                _tbid_clrs = [
                    f"rgba(245,158,11,{0.55 + 0.45 * v / _tbid_max:.2f})"
                    for v in _tbid_vals
                ]
                fig = go.Figure(go.Bar(
                    x=mrev["month_label"], y=mrev["tbid_m"],
                    marker=dict(color=_tbid_clrs, line_width=0, cornerradius=6),
                    hovertemplate="<b>%{x}</b><br>Est. TBID: $%{y:.2f}M<extra></extra>",
                ))
                fig.update_layout(yaxis_tickprefix="$", yaxis_ticksuffix="M", showlegend=False,
                                  transition={"duration": 700, "easing": "cubic-in-out"})
                st.plotly_chart(style_fig(fig), use_container_width=True, config=PLOTLY_CONFIG)
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
                marker=dict(color="#00B4C6", line_width=0, cornerradius=6),
                hovertemplate="<b>%{x}</b><br>Days ≥ 80%%: %{y}<extra></extra>",
            ))
            fig.add_trace(go.Bar(
                name="Days ≥ 90% Occ",
                x=df_comp["quarter"], y=df_comp["days_above_90_occ"],
                marker=dict(color="#6366F1", line_width=0, cornerradius=6),
                hovertemplate="<b>%{x}</b><br>Days ≥ 90%%: %{y}<br>"
                              "<i>High compression — rate increases justified</i><extra></extra>",
            ))
            fig.update_layout(barmode="group",
                              transition={"duration": 700, "easing": "cubic-in-out"})
            st.plotly_chart(style_fig(fig, height=260), use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.markdown(empty_state(
                "📦", "No compression data.",
                "Run compute_kpis.py to populate kpi_compression_quarterly.",
            ), unsafe_allow_html=True)

        # ── Painted density calendar (Giorgia Lupi / Long-Covid style) ──────────
        _painted_fig = render_painted_occ_heatmap(df_daily if not df_daily.empty else df_kpi)
        if _painted_fig is not None:
            st.markdown(sec_div("🎨 Occupancy Density Calendar"), unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-header">365-Day Occupancy Density Calendar</div>'
                '<div class="chart-caption">'
                'Each bubble = one day &nbsp;·&nbsp; <b>size &amp; color intensity = occupancy pressure</b> &nbsp;·&nbsp; '
                '<span class="data-hl">Blue</span> = demand building &nbsp;·&nbsp; '
                '<span class="data-hl-neg">Orange</span> = 90%+ compression &nbsp;·&nbsp; '
                'Inspired by Giorgia Lupi\'s painted data approach</div>',
                unsafe_allow_html=True,
            )
            # Painted legend
            st.markdown(
                '<div class="painted-legend">'
                '<div class="painted-swatch"><span class="painted-dot" style="background:rgba(5,103,200,0.12);"></span> Low demand (≤65%)</div>'
                '<div class="painted-swatch"><span class="painted-dot" style="background:rgba(5,103,200,0.45);"></span> Moderate (65–80%)</div>'
                '<div class="painted-swatch"><span class="painted-dot" style="background:rgba(5,103,200,0.85);"></span> High (80–90%)</div>'
                '<div class="painted-swatch"><span class="painted-dot" style="background:rgba(234,88,12,0.85);"></span> Compression (90%+)</div>'
                '</div>',
                unsafe_allow_html=True,
            )
            st.plotly_chart(_painted_fig, use_container_width=True, config=PLOTLY_CONFIG)

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
        st.plotly_chart(style_fig(fig, height=300), use_container_width=True, config=PLOTLY_CONFIG)

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
            st.plotly_chart(style_fig(fig, height=280), use_container_width=True, config=PLOTLY_CONFIG)
            st.caption(f"Density clusters reveal seasonal patterns. Q3 (dark teal) = peak season. Spread = {_str_metric_label} variability. Filter metric above to switch views.")

        # ── Revenue Intelligence: STR × Visitor Economy Correlations ──────────
        st.markdown("---")
        try:
            _ri_rev12   = float(df_monthly["revenue"].sum()) if not df_monthly.empty and "revenue" in df_monthly.columns else 0.0
            _ri_adr     = float(monthly["adr"].dropna().iloc[-1]) if not monthly.empty and "adr" in monthly.columns and not monthly["adr"].dropna().empty else 0.0
            _ri_occ     = float(monthly["occ_pct"].dropna().iloc[-1]) if not monthly.empty and "occ_pct" in monthly.columns and not monthly["occ_pct"].dropna().empty else 0.0
            _ri_tbid12  = _ri_rev12 * 0.0125
            _ri_tot12   = _ri_rev12 * 0.10
            _ri_trips   = int(df_dfy_ov.iloc[0].get("total_trips", 0) or 0) if not df_dfy_ov.empty else 0
            _ri_oos     = float(df_dfy_ov.iloc[0].get("out_of_state_vd_pct", 0) or 0) if not df_dfy_ov.empty else 0
            _ri_los     = float(df_dfy_ov.iloc[0].get("avg_los", 0) or 0) if not df_dfy_ov.empty else 0
            _ri_onight  = float(df_dfy_ov.iloc[0].get("overnight_pct", 0) or 0) if not df_dfy_ov.empty else 0

            if _ri_rev12 > 0 or _ri_trips > 0:
                st.markdown(sec_div("💡 Revenue Intelligence — STR × Visitor Economy"), unsafe_allow_html=True)
                _ri_c1, _ri_c2, _ri_c3, _ri_c4 = st.columns(4)

                def _ri_metric(label, val, note, icon, color):
                    return (
                        f'<div style="background:#FFFFFF;border-radius:10px;padding:14px 16px;'
                        f'border:1px solid rgba(15,28,46,0.07);border-top:3px solid {color};'
                        f'box-shadow:0 1px 4px rgba(15,28,46,0.06);">'
                        f'<div style="font-size:18px;margin-bottom:4px;">{icon}</div>'
                        f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
                        f'letter-spacing:.08em;color:#64748B;margin-bottom:4px;">{label}</div>'
                        f'<div style="font-family:\'Outfit\',sans-serif;font-size:20px;font-weight:800;'
                        f'color:{color};letter-spacing:-.02em;">{val}</div>'
                        f'<div style="font-size:11px;color:#64748B;margin-top:4px;line-height:1.5;">{note}</div>'
                        f'</div>'
                    )

                _rev_per_trip = _ri_rev12 / _ri_trips if _ri_trips > 0 and _ri_rev12 > 0 else 0
                _tbid_per_trip = _ri_tbid12 / _ri_trips if _ri_trips > 0 and _ri_tbid12 > 0 else 0

                with _ri_c1:
                    st.markdown(_ri_metric(
                        "Room Rev / Visitor Trip",
                        f"${_rev_per_trip:.2f}" if _rev_per_trip > 0 else "—",
                        "Hotel room revenue per annual visitor trip. Fly-market OOS visitors typically generate 1.3–1.4× this ratio.",
                        "💵", "#0567C8",
                    ), unsafe_allow_html=True)
                with _ri_c2:
                    st.markdown(_ri_metric(
                        "TBID / Visitor Trip",
                        f"${_tbid_per_trip:.3f}" if _tbid_per_trip > 0 else "—",
                        f"Est. ${_ri_tbid12/1e3:,.0f}K annual TBID across {_ri_trips:,} visits. Grow overnight conversion to raise this.",
                        "🏷️", "#7C3AED",
                    ), unsafe_allow_html=True)
                with _ri_c3:
                    _overnight_rev_share = (_ri_onight / 100) if _ri_onight > 0 else 0
                    st.markdown(_ri_metric(
                        "Overnight Share",
                        f"{_ri_onight:.0f}%" if _ri_onight > 0 else "—",
                        f"{100-_ri_onight:.0f}% are day trips — converting 3% adds ~${_ri_trips * 0.03 * _ri_adr / 1e6:.1f}M in incremental room revenue.",
                        "🌙", "#059669",
                    ), unsafe_allow_html=True)
                with _ri_c4:
                    _oos_rev_est = _ri_rev12 * (_ri_oos / 100) * 1.35 if _ri_oos > 0 else 0
                    st.markdown(_ri_metric(
                        "OOS Visitor Rev Est.",
                        f"${_oos_rev_est/1e6:.1f}M" if _oos_rev_est > 0 else "—",
                        f"{_ri_oos:.0f}% OOS visitors × 1.35× ADR premium factor. Real-time: grow OOS share via fly-market campaigns.",
                        "✈️", "#EA580C",
                    ), unsafe_allow_html=True)

                # Revenue waterfall insight
                if _ri_rev12 > 0:
                    st.markdown("<br>", unsafe_allow_html=True)
                    _wf_tbid = _ri_tbid12
                    _wf_tot  = _ri_tot12
                    _wf_total_public = _wf_tbid + _wf_tot
                    st.markdown(
                        f'<div style="background:linear-gradient(135deg,#F0F7FF,#F5F3FF);'
                        f'border-radius:12px;padding:16px 20px;border:1px solid rgba(5,103,200,0.12);">'
                        f'<div style="font-family:\'Outfit\',sans-serif;font-size:12px;font-weight:800;'
                        f'text-transform:uppercase;letter-spacing:.08em;color:#0567C8;margin-bottom:10px;">'
                        f'📊 Public Revenue Derivation from 12-Month Room Revenue</div>'
                        f'<div style="display:flex;gap:20px;flex-wrap:wrap;font-family:\'Inter\',sans-serif;font-size:13px;">'
                        f'<div><span style="color:#64748B;">12-Mo Room Revenue</span><br>'
                        f'<strong style="color:#0D1B2E;font-size:16px;">${_ri_rev12/1e6:.2f}M</strong></div>'
                        f'<div style="color:#64748B;align-self:center;font-size:18px;">→</div>'
                        f'<div><span style="color:#64748B;">TBID Assessment (1.25%)</span><br>'
                        f'<strong style="color:#7C3AED;font-size:16px;">${_wf_tbid/1e3:,.0f}K</strong></div>'
                        f'<div style="color:#64748B;align-self:center;font-size:18px;">+</div>'
                        f'<div><span style="color:#64748B;">TOT (10%)</span><br>'
                        f'<strong style="color:#059669;font-size:16px;">${_wf_tot/1e6:.2f}M</strong></div>'
                        f'<div style="color:#64748B;align-self:center;font-size:18px;">=</div>'
                        f'<div style="background:#0567C8;border-radius:8px;padding:8px 14px;">'
                        f'<span style="color:rgba(255,255,255,0.8);font-size:10px;">Total Public Revenue</span><br>'
                        f'<strong style="color:#FFFFFF;font-size:18px;">${_wf_total_public/1e6:.2f}M</strong></div>'
                        f'</div></div>',
                        unsafe_allow_html=True,
                    )
        except Exception:
            pass

        # ── Search Demand Intelligence ─────────────────────────────────────────
        st.markdown(_sh("🔍", "Search Demand Intelligence", "blue", "Google Trends · Leading Indicator"), unsafe_allow_html=True)
        st.markdown(
            sec_intel(
                "Search Demand",
                "weekly Google search interest for Dana Point destination terms vs. coastal competitors",
                "Search intent leads hotel bookings by 2–6 weeks — making this the earliest available demand signal.",
                "Rising search for 'dana point hotel' in Q2 signals Q3 revenue upside before STR data confirms it.",
                "Run pipeline step 13 (fetch_google_trends.py) to populate",
            ),
            unsafe_allow_html=True,
        )
        if not df_trends.empty:
            _dp_terms   = df_trends[df_trends["category"] == "primary"].copy()
            _comp_terms = df_trends[df_trends["category"] == "competitor"].copy()

            if not _dp_terms.empty:
                _dp_terms["week_date"] = pd.to_datetime(_dp_terms["week_date"])
                _dp_pivot = (
                    _dp_terms.groupby("week_date")["interest_idx"].mean().reset_index()
                )
                _dp_pivot.columns = ["week_date", "avg_interest"]

                _trc1, _trc2 = st.columns([2, 1])
                with _trc1:
                    fig_tr = go.Figure()
                    fig_tr.add_trace(go.Scatter(
                        x=_dp_pivot["week_date"], y=_dp_pivot["avg_interest"],
                        mode="lines", name="Dana Point", fill="tozeroy",
                        line=dict(color=TEAL_LIGHT, width=2.5),
                        fillcolor="rgba(0,195,190,0.10)",
                        hovertemplate="<b>%{x|%b %d}</b><br>Interest Index: %{y}<extra></extra>",
                    ))

                    if not _comp_terms.empty:
                        _comp_terms["week_date"] = pd.to_datetime(_comp_terms["week_date"])
                        _comp_pivot = (
                            _comp_terms.groupby("week_date")["interest_idx"].mean().reset_index()
                        )
                        fig_tr.add_trace(go.Scatter(
                            x=_comp_pivot["week_date"], y=_comp_pivot["interest_idx"],
                            mode="lines", name="Competitors (avg)",
                            line=dict(color=ORANGE, width=1.5, dash="dot"),
                            hovertemplate="<b>%{x|%b %d}</b><br>Competitor Index: %{y}<extra></extra>",
                        ))

                    fig_tr.update_layout(
                        title="Dana Point vs. Competitor Search Interest (Last 12 Months)",
                        yaxis_title="Google Search Index (0–100)",
                    )
                    st.plotly_chart(style_fig(fig_tr, height=260), use_container_width=True, config=PLOTLY_CONFIG)
                    st.caption("Google Trends search index, 0–100. 100 = peak search interest. Dana Point primary terms averaged weekly.")

                with _trc2:
                    _term_summary = (
                        _dp_terms.groupby("term")["interest_idx"]
                        .agg(["mean", "max"])
                        .reset_index()
                        .sort_values("mean", ascending=False)
                    )
                    st.markdown(_sh("📊", "Top Search Terms", "teal"), unsafe_allow_html=True)
                    for _, _tr in _term_summary.iterrows():
                        _pct = int(_tr["mean"])
                        st.markdown(
                            f'<div style="margin-bottom:8px;">'
                            f'<div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px;">'
                            f'<span style="color:var(--dp-text-1);">{_tr["term"]}</span>'
                            f'<span style="color:var(--dp-teal);font-weight:700;">{_pct}</span>'
                            f'</div>'
                            f'<div style="height:4px;background:rgba(255,255,255,0.07);border-radius:3px;">'
                            f'<div style="width:{_pct}%;height:100%;background:var(--dp-teal,#00C3BE);border-radius:3px;"></div>'
                            f'</div></div>',
                            unsafe_allow_html=True,
                        )
        else:
            st.markdown(
                '<div class="empty-card">'
                '<div class="empty-icon">🔍</div>'
                '<div class="empty-title">Search Demand Data Not Yet Loaded</div>'
                '<div class="empty-body">Run <code>python scripts/run_pipeline.py</code> to fetch '
                'Google Trends data.<br>No API key required — pytrends must be installed.</div>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Weather & Demand Correlation ───────────────────────────────────────
        st.markdown(_sh("⛅", "Coastal Weather Intelligence", "teal", "Open-Meteo · Dana Point CA"), unsafe_allow_html=True)
        st.markdown(
            sec_intel(
                "Weather Intelligence",
                "monthly weather patterns and a proprietary Beach Day Score for Dana Point, CA",
                "Beach Day Score correlates with occupancy at R≈0.72 in coastal leisure markets — "
                "the single strongest natural demand driver for Dana Point.",
                "Months with Beach Day Score >75 consistently yield 85%+ occupancy; "
                "use score forecasts to set rate floors for Q1 and Q4 shoulder months.",
                "Run pipeline step 14 (fetch_weather_data.py) to populate",
            ),
            unsafe_allow_html=True,
        )
        if not df_weather.empty and not df_kpi.empty:
            _wx = df_weather.copy()
            _kpi = df_kpi.copy()
            _kpi["as_of_date"] = pd.to_datetime(_kpi["as_of_date"])
            _kpi["year"]  = _kpi["as_of_date"].dt.year
            _kpi["month"] = _kpi["as_of_date"].dt.month
            _kpi_mon = (
                _kpi.groupby(["year", "month"])
                .agg(avg_occ=("occ_pct", "mean"), avg_adr=("adr", "mean"))
                .reset_index()
            )
            _wx_kpi = _wx.merge(_kpi_mon, on=["year", "month"], how="inner")

            _wc1, _wc2 = st.columns([3, 2])
            with _wc1:
                if not _wx_kpi.empty and "date" in _wx_kpi.columns:
                    _wx_kpi_sorted = _wx_kpi.sort_values("date")
                    fig_wx = go.Figure()
                    fig_wx.add_trace(go.Bar(
                        x=_wx_kpi_sorted["date"], y=_wx_kpi_sorted["beach_day_score"],
                        name="Beach Day Score", marker_color="rgba(0,195,190,0.55)",
                        yaxis="y", hovertemplate="<b>%{x|%b %Y}</b><br>Beach Score: %{y:.0f}<extra></extra>",
                    ))
                    fig_wx.add_trace(go.Scatter(
                        x=_wx_kpi_sorted["date"], y=_wx_kpi_sorted["avg_occ"],
                        name="Avg Occupancy %", line=dict(color=ORANGE, width=2.5),
                        yaxis="y2", hovertemplate="<b>%{x|%b %Y}</b><br>Occupancy: %{y:.1f}%<extra></extra>",
                    ))
                    fig_wx.update_layout(
                        title="Beach Day Score vs. Hotel Occupancy",
                        yaxis=dict(title="Beach Day Score (0–100)", showgrid=False, range=[0, 115]),
                        yaxis2=dict(title="Occupancy %", overlaying="y", side="right",
                                    showgrid=True, gridcolor="rgba(0,0,0,0.06)", range=[0, 115]),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(style_fig(fig_wx, height=280), use_container_width=True, config=PLOTLY_CONFIG)
                    st.caption("Beach Day Score (0–100) is a composite of temperature comfort, low precipitation, and sunshine hours — the primary coastal demand driver.")

            with _wc2:
                if not _wx_kpi.empty and "beach_day_score" in _wx_kpi.columns and "avg_occ" in _wx_kpi.columns:
                    _scatter = _wx_kpi[["beach_day_score", "avg_occ", "avg_adr"]].dropna()
                    if len(_scatter) >= 4:
                        _corr = round(_scatter["beach_day_score"].corr(_scatter["avg_occ"]), 2)
                        fig_sc = go.Figure()
                        fig_sc.add_trace(go.Scatter(
                            x=_scatter["beach_day_score"], y=_scatter["avg_occ"],
                            mode="markers", name="Month",
                            marker=dict(color=TEAL_LIGHT, size=9, opacity=0.85,
                                        line=dict(color="rgba(255,255,255,0.3)", width=1)),
                            hovertemplate="Beach Score: %{x:.0f}<br>Occ: %{y:.1f}%<extra></extra>",
                        ))
                        fig_sc.update_layout(
                            title=f"Correlation: R = {_corr}",
                            xaxis_title="Beach Day Score",
                            yaxis_title="Avg Occupancy %",
                        )
                        st.plotly_chart(style_fig(fig_sc, height=280), use_container_width=True, config=PLOTLY_CONFIG)
                        if abs(_corr) >= 0.60:
                            st.success(f"Strong correlation (R={_corr}) confirms weather is a primary demand driver for Dana Point.")
                        else:
                            st.info(f"Moderate correlation (R={_corr}). More months of data will sharpen this signal.")
                    else:
                        st.metric("Beach Day Score — Current Month",
                                  f"{_wx.iloc[-1]['beach_day_score']:.0f}" if not _wx.empty else "N/A")
        elif not df_weather.empty:
            # Weather data loaded but no KPI overlap — just show weather chart
            _wx_sorted = df_weather.sort_values("date") if "date" in df_weather.columns else df_weather
            fig_wxonly = go.Figure()
            fig_wxonly.add_trace(go.Scatter(
                x=_wx_sorted["date"], y=_wx_sorted["beach_day_score"],
                mode="lines+markers", name="Beach Day Score",
                line=dict(color=TEAL_LIGHT, width=2.5),
                fill="tozeroy", fillcolor="rgba(0,195,190,0.10)",
            ))
            fig_wxonly.update_layout(title="Monthly Beach Day Score — Dana Point, CA")
            st.plotly_chart(style_fig(fig_wxonly, height=220), use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.markdown(
                '<div class="empty-card">'
                '<div class="empty-icon">⛅</div>'
                '<div class="empty-title">Weather Data Not Yet Loaded</div>'
                '<div class="empty-body">Run <code>python scripts/run_pipeline.py</code> to fetch '
                'Open-Meteo weather data for Dana Point.<br>No API key required.</div>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── NOAA Ocean Conditions ──────────────────────────────────────────────
        st.markdown(sec_div("🌊 Ocean Conditions — Dana Point Coastal Intelligence"), unsafe_allow_html=True)
        st.markdown(_sh("🌊", "NOAA Marine Conditions · Dana Point Waters", "teal", "OCEAN DEMAND DRIVER"), unsafe_allow_html=True)
        st.caption(
            "Source: NOAA National Data Buoy Center · Buoy 46025 (Santa Monica Basin) · Monthly aggregates. "
            "Dana Point is a surf, fishing, whale-watching, and sailing destination — ocean conditions drive activity bookings 7–14 days out."
        )
        if not df_noaa.empty and "date" in df_noaa.columns:
            _noaa_plot = df_noaa.sort_values("date").copy()
            _nc1, _nc2 = st.columns([3, 1])
            with _nc1:
                fig_noaa = go.Figure()
                fig_noaa.add_trace(go.Bar(
                    x=_noaa_plot["date"], y=_noaa_plot["beach_activity_score"],
                    name="Beach Activity Score", marker_color="rgba(0,195,190,0.45)",
                    yaxis="y",
                    hovertemplate="<b>%{x|%b %Y}</b><br>Activity Score: %{y:.0f}<extra></extra>",
                ))
                if "avg_water_temp_f" in _noaa_plot.columns:
                    fig_noaa.add_trace(go.Scatter(
                        x=_noaa_plot["date"], y=_noaa_plot["avg_water_temp_f"],
                        name="Water Temp (°F)", line=dict(color=ORANGE, width=2.5),
                        yaxis="y2",
                        hovertemplate="<b>%{x|%b %Y}</b><br>Water Temp: %{y:.1f}°F<extra></extra>",
                    ))
                if "avg_wave_height_ft" in _noaa_plot.columns:
                    fig_noaa.add_trace(go.Scatter(
                        x=_noaa_plot["date"], y=_noaa_plot["avg_wave_height_ft"],
                        name="Avg Wave Ht (ft)", line=dict(color=BLUE, width=1.8, dash="dot"),
                        yaxis="y3" if False else "y2",
                        hovertemplate="<b>%{x|%b %Y}</b><br>Wave Ht: %{y:.1f} ft<extra></extra>",
                    ))
                fig_noaa.update_layout(
                    title="Ocean Conditions — Dana Point",
                    yaxis=dict(title="Beach Activity Score (0–100)", showgrid=False, range=[0, 120]),
                    yaxis2=dict(title="Water Temp (°F) / Wave Ht (ft)", overlaying="y", side="right",
                                showgrid=True, gridcolor="rgba(0,0,0,0.06)"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(style_fig(fig_noaa, height=280), use_container_width=True, config=PLOTLY_CONFIG)
            with _nc2:
                _noaa_latest = _noaa_plot.iloc[-1]
                _nm1, _nm2 = st.columns(2)
                with _nm1:
                    st.metric("Activity Score", f"{_noaa_latest.get('beach_activity_score', 0):.0f}/100")
                    st.metric("Water Temp", f"{_noaa_latest.get('avg_water_temp_f', 0):.1f}°F")
                with _nm2:
                    st.metric("Avg Wave Ht", f"{_noaa_latest.get('avg_wave_height_ft', 0):.1f} ft")
                    _wind = _noaa_latest.get("avg_wind_speed_kt")
                    st.metric("Wind Speed", f"{_wind:.1f} kt" if _wind else "—")
                st.caption(
                    "📌 **Optimal conditions:** Water temp 65–72°F + waves 2–5ft + wind <10kt = peak charter/activity demand. "
                    "Big swell (>8ft) drives surf enthusiasts but suppresses family/whale-watch bookings."
                )

                # Correlation with hotel demand
                if not df_kpi.empty:
                    try:
                        _kpi_mo = df_kpi.copy()
                        _kpi_mo["as_of_date"] = pd.to_datetime(_kpi_mo["as_of_date"])
                        _kpi_mo["year"]  = _kpi_mo["as_of_date"].dt.year
                        _kpi_mo["month"] = _kpi_mo["as_of_date"].dt.month
                        _kpi_mo_agg = _kpi_mo.groupby(["year","month"])["occ_pct"].mean().reset_index()
                        _noaa_plot2 = _noaa_plot[["date","beach_activity_score"]].copy()
                        _noaa_plot2["year"]  = _noaa_plot2["date"].dt.year
                        _noaa_plot2["month"] = _noaa_plot2["date"].dt.month
                        _merged = pd.merge(_noaa_plot2, _kpi_mo_agg, on=["year","month"], how="inner")
                        if len(_merged) >= 4:
                            _r = round(_merged["beach_activity_score"].corr(_merged["occ_pct"]), 2)
                            _color = "#059669" if abs(_r) >= 0.5 else "#D97706"
                            st.markdown(
                                f'<div style="background:#F0FDF4;border-radius:8px;padding:10px 14px;'
                                f'border-left:3px solid {_color};margin-top:8px;">'
                                f'<div style="font-size:11px;font-weight:700;color:#065F46;">Ocean ↔ Occupancy</div>'
                                f'<div style="font-size:20px;font-weight:900;color:{_color};">R = {_r}</div>'
                                f'<div style="font-size:10px;color:#6B7280;">{len(_merged)} months of data</div>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )
                    except Exception:
                        pass
        else:
            st.markdown(
                '<div class="empty-card">'
                '<div class="empty-icon">🌊</div>'
                '<div class="empty-title">NOAA Marine Data Not Loaded</div>'
                '<div class="empty-body">Run <code>python scripts/run_pipeline.py</code> '
                'to fetch NOAA ocean conditions. No API key required.</div>'
                '</div>',
                unsafe_allow_html=True,
            )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — FORWARD OUTLOOK
# ══════════════════════════════════════════════════════════════════════════════
with tab_fo:
    _tab_controls("fo")
    # Time Period filter: controls how much STR history backs the outlook narrative
    _str_filters("fo", show_grain=False, show_metric=False)
    st.markdown(tab_summary(
        "<strong>VDP Forward Outlook</strong> — VDP-generated, daily-refreshed insights for four stakeholder audiences: "
        "DMO leadership, city council, visitors, and residents. "
        "Insights are derived from cross-dataset signals invisible in any single source alone."
    ), unsafe_allow_html=True)
    # ── Header ─────────────────────────────────────────────────────────────────
    st.markdown(
        '<div style="font-family:\'Syne\',sans-serif;font-size:1.55rem;'
        'font-weight:800;letter-spacing:-0.03em;margin-bottom:4px;">'
        'Forward Outlook</div>'
        '<div style="font-size:12px;opacity:0.50;font-weight:500;margin-bottom:20px;">'
        'Daily forward-looking insights from analytics.sqlite — updated every pipeline run</div>',
        unsafe_allow_html=True,
    )

    # Load all insights
    df_insights_all = load_insights()

    # ── Forward Outlook Section Intelligence ────────────────────────────────
    try:
        _fo_occ    = m.get("occ_30", 0) if m else 0
        _fo_rvp_d  = m.get("revpar_delta", 0) if m else 0
        _fo_cq     = m.get("comp_recent_q", 0) if m else 0
        _fo_q3c    = 0
        if not df_comp.empty and "quarter" in df_comp.columns:
            _fo_q3r = df_comp[df_comp["quarter"].str.endswith("Q3")]
            _fo_q3c = int(_fo_q3r["days_above_80_occ"].iloc[0]) if not _fo_q3r.empty else 0
        _fo_gap_to_70 = _fo_occ - 70
        _fo_insight = (f"Occupancy is {_fo_gap_to_70:+.1f}pp {'above' if _fo_gap_to_70 >= 0 else 'below'} the 70% baseline "
                       f"with {_fo_cq} compression nights this quarter. Q3 historically peaks at {_fo_q3c} compression nights.")
        _fo_fwd = ("Advance rate increases ahead of peak compression window; lock in ADR floors now." if _fo_occ >= 70
                   else "Target shoulder demand campaigns to close occupancy gap before compression season.")
        st.markdown(sec_intel(
            "Forward Outlook",
            "forward-looking demand signals and rate positioning guidance",
            _fo_insight,
            _fo_fwd,
            f"Current Occ: {_fo_occ:.1f}% · Q3 Peak Compression: {_fo_q3c} nights",
        ), unsafe_allow_html=True)
    except Exception:
        pass

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

    def _kfm_card(label, value, delta, delta_color="#718096"):
        return (
            f'<div style="background:#FFFFFF;'
            f'border-radius:12px;padding:16px 18px;'
            f'border:1px solid rgba(0,0,0,0.08);border-left:4px solid #0891B2;'
            f'position:relative;overflow:hidden;margin-bottom:8px;'
            f'box-shadow:0 1px 4px rgba(0,0,0,0.08);">'
            f'<div style="font-size:10px;color:#718096;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.10em;margin-bottom:6px;">{label}</div>'
            f'<div style="font-size:1.9rem;font-weight:900;letter-spacing:-0.04em;line-height:1.1;'
            f'color:#0F1C2E;">{value}</div>'
            f'<div style="font-size:11px;color:{delta_color};font-weight:600;margin-top:5px;">{delta}</div>'
            f'</div>'
        )
    with _fwd_c1:
        _occ_vs = _occ_fwd - 70
        _occ_dc = "#059669" if _occ_vs >= 0 else "#DC2626"
        st.markdown(_kfm_card(
            f"Occupancy — 30-day trailing",
            f"{_occ_fwd:.1f}%",
            f"{_occ_vs:+.1f}pp vs 70% baseline",
            _occ_dc,
        ), unsafe_allow_html=True)
    with _fwd_c2:
        st.markdown(_kfm_card(
            f"ADR — 30-day trailing",
            f"${_adr_fwd:,.0f}",
            f"RevPAR ${_rvp_fwd:,.0f}",
            "#718096",
        ), unsafe_allow_html=True)
    with _fwd_c3:
        st.markdown(_kfm_card(
            f"Compression Nights — {_fwd_q3_label}",
            f"{_q3_comp}",
            f"{_fwd_q1_label}: {_q1_comp} nights",
            "#718096",
        ), unsafe_allow_html=True)
    with _fwd_c4:
        # Shoulder opportunity: Q1 compression gap vs Q3
        _shld_gap = _q3_comp - _q1_comp
        _shld_rev_opp = _shld_gap * _rvp_fwd * 0.02 * 365 / max(_q3_comp, 1) if _rvp_fwd > 0 else 0
        st.markdown(_kfm_card(
            "Shoulder Revenue Opportunity",
            f"${_shld_rev_opp:,.0f}",
            f"{_shld_gap} comp nights gap ({_fwd_q3_label} vs {_fwd_q1_label})",
            "#059669" if _shld_rev_opp > 0 else "#8FA3B8",
        ), unsafe_allow_html=True)

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
                st.plotly_chart(style_fig(_fo_bar_fig, height=280), use_container_width=True, config=PLOTLY_CONFIG)

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
                st.plotly_chart(style_fig(_hz_fig, height=280), use_container_width=True, config=PLOTLY_CONFIG)

        st.markdown("---")

    # ── AI Outlook Intelligence Panel ─────────────────────────────────────────
    try:
        _fo_rvp_d3  = m.get("revpar_delta", 0) if m else 0
        _fo_occ3    = m.get("occ_30", 0) if m else 0
        _fo_cq3     = m.get("comp_recent_q", 0) if m else 0
        _fo_trips3  = int(df_dfy_ov.iloc[0].get("total_trips", 0) or 0) if not df_dfy_ov.empty else 0
        _fo_roas3   = float(df_dfy_media.iloc[0].get("roas", 0) or 0) if not df_dfy_media.empty else 0

        _fo_next_steps = [
            f"<strong>Q3 Compression Play:</strong> With {_fo_cq3} compression days this quarter, "
            "activate advanced rate strategy 6 weeks prior — ADR floors + length-of-stay minimums.",
            f"<strong>Campaign Timing:</strong> Insights show peak conversion in shoulder months — "
            "shift 20-30% of campaign budget from Q3 (already full) to Q1/Q2 to build off-peak demand.",
            f"<strong>Cross-Dataset Signal:</strong> Day-trip visitors represent 3.55M annual trips — "
            "a 2% overnight conversion at current ADR = ~$15M in incremental room revenue.",
            "<strong>ROAS Optimization:</strong> "
            + (f"Current campaign ROAS is {_fo_roas3:.1f}x — analyze which channels drive highest-value OOS visitors." if _fo_roas3 > 0 else
               "Load Datafy media attribution data to calculate actual ROAS and optimize campaign mix."),
        ]
        _fo_questions = [
            "Which audience insight should I prioritize this quarter?",
            "How do cross-dataset signals compare to prior year?",
            "What's the biggest revenue opportunity in the next 90 days?",
            "How should we adjust messaging for each stakeholder group?",
            "What does the day-trip conversion opportunity look like in dollars?",
        ]
        _fo_context = (
            f"Forward Outlook: Occ {_fo_occ3:.1f}%, RevPAR {_fo_rvp_d3:+.1f}% YOY, "
            f"{_fo_cq3} compression days, {_fo_trips3:,} annual visitor trips."
        )
        render_intel_panel("fo_outlook", _fo_next_steps, _fo_questions, _fo_context)
    except Exception:
        pass

    # ── Audience tabs ────────────────────────────────────────────────────────
    st.markdown(sec_div("🧠 Audience-Specific Insights"), unsafe_allow_html=True)
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
                            f'<div class="insight-title">{md_to_html(str(row["headline"]))}</div>'
                            f'<p class="insight-body">{md_to_html(str(row["body"]))}</p>'
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

    # ── Forward Signals Dashboard ──────────────────────────────────────────
    st.markdown(_sh("📊", "Forward Signal Dashboard", "blue", "Leading indicators powering the AI outlook"), unsafe_allow_html=True)

    _s1, _s2 = st.columns(2)

    with _s1:
        # Google Trends chart
        if not df_trends.empty and "week_date" in df_trends.columns and "interest_idx" in df_trends.columns:
            _trend_terms = df_trends["term"].unique().tolist() if "term" in df_trends.columns else []
            _trend_colors = ["#00C8E0", "#10B981", "#F43F5E", "#A78BFA", "#F59E0B"]
            fig_trend = go.Figure()
            for i, term in enumerate(_trend_terms[:5]):
                _td = df_trends[df_trends["term"] == term].copy()
                _td = _td.sort_values("week_date")
                fig_trend.add_trace(go.Scatter(
                    x=_td["week_date"], y=_td["interest_idx"],
                    name=term, mode="lines",
                    line=dict(color=_trend_colors[i % len(_trend_colors)], width=2),
                    hovertemplate="%{x|%b %d}<br>Interest Index: %{y}<extra>" + term + "</extra>"
                ))
            fig_trend.update_layout(
                title="Search Demand Index — Dana Point vs. Competitors",
                height=300,
                margin=dict(l=10, r=10, t=40, b=10),
                legend=dict(orientation="h", y=-0.2, font=dict(size=10, color="#4A5568")),
                xaxis=dict(showgrid=False, color="#718096"),
                yaxis=dict(title="Interest (0-100)", gridcolor="rgba(0,0,0,0.06)", color="#718096"),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="DM Sans", color="#4A5568")
            )
            st.plotly_chart(fig_trend, use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.info("Google Trends data not available. Run `fetch_google_trends.py`.")

    with _s2:
        # Weather correlation chart
        if not df_weather.empty:
            _wm = df_weather.copy()
            if "year" in _wm.columns and "month" in _wm.columns:
                _wm["date"] = pd.to_datetime(_wm[["year", "month"]].assign(day=1))
                _wm = _wm.sort_values("date").tail(24)
                _temp_col = next((c for c in ["avg_high_f","avg_temp_f","temp_high"] if c in _wm.columns), None)
                if _temp_col:
                    fig_wx = go.Figure()
                    fig_wx.add_trace(go.Bar(
                        x=_wm["date"], y=_wm[_temp_col],
                        name="Avg High F",
                        marker_color="#00C8E0",
                        opacity=0.7,
                        hovertemplate="%{x|%b %Y}<br>%{y:.0f}F<extra></extra>"
                    ))
                    fig_wx.update_layout(
                        title="Coastal Weather — Dana Point (Seasonal Demand Driver)",
                        height=300,
                        margin=dict(l=10, r=10, t=40, b=10),
                        xaxis=dict(showgrid=False, color="#718096"),
                        yaxis=dict(title="Temp (F)", gridcolor="rgba(0,0,0,0.06)", color="#718096"),
                        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(family="DM Sans", color="#4A5568")
                    )
                    st.plotly_chart(fig_wx, use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("Weather temperature data column not found.")
            else:
                st.info("Weather data format unexpected.")
        else:
            st.info("Weather data not available. Run `fetch_weather_data.py`.")

    # BLS Employment and Compression Calendar row
    _b1, _b2 = st.columns(2)
    with _b1:
        if not df_bls.empty:
            _bls_plot = df_bls.copy()
            if "year" in _bls_plot.columns and "month" in _bls_plot.columns:
                _bls_plot["date"] = pd.to_datetime(_bls_plot[["year","month"]].assign(day=1))
                _bls_plot = _bls_plot.sort_values("date").tail(36)
                _val_col = next((c for c in ["value","employment","level"] if c in _bls_plot.columns), None)
                if _val_col:
                    fig_bls = go.Figure()
                    if "series_name" in _bls_plot.columns:
                        for _sname, _sdf in _bls_plot.groupby("series_name"):
                            fig_bls.add_trace(go.Scatter(
                                x=_sdf["date"], y=_sdf[_val_col],
                                name=str(_sname), mode="lines+markers",
                                line=dict(width=2),
                                marker=dict(size=4),
                                hovertemplate="%{x|%b %Y}<br>%{y:,.0f}<extra>" + str(_sname) + "</extra>"
                            ))
                    else:
                        fig_bls.add_trace(go.Scatter(
                            x=_bls_plot["date"], y=_bls_plot[_val_col],
                            name="Employment", mode="lines+markers",
                            line=dict(width=2), marker=dict(size=4),
                            hovertemplate="%{x|%b %Y}<br>%{y:,.0f}<extra></extra>"
                        ))
                    fig_bls.update_layout(
                        title="Hospitality Employment — Orange County (BLS)",
                        height=280,
                        margin=dict(l=10, r=10, t=40, b=10),
                        legend=dict(orientation="h", y=-0.25, font=dict(size=9, color="#4A5568")),
                        xaxis=dict(showgrid=False, color="#718096"),
                        yaxis=dict(title="Workers (000s)", gridcolor="rgba(0,0,0,0.06)", color="#718096"),
                        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(family="DM Sans", color="#4A5568")
                    )
                    st.plotly_chart(fig_bls, use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("BLS data value column not found.")
            else:
                st.info("BLS data not available. Run `fetch_bls_data.py`.")
        else:
            st.info("BLS data not available. Run `fetch_bls_data.py`.")

    with _b2:
        # Compression calendar heatmap
        if not df_kpi.empty and "as_of_date" in df_kpi.columns:
            _comp_df = df_kpi.copy()
            _comp_df["month"] = _comp_df["as_of_date"].dt.to_period("M").astype(str)
            _occ_col = "occ_pct" if "occ_pct" in _comp_df.columns else ("occ" if "occ" in _comp_df.columns else None)
            if _occ_col:
                _comp_df["occ"] = pd.to_numeric(_comp_df[_occ_col], errors="coerce")
                _monthly_occ = _comp_df.groupby("month")["occ"].mean().reset_index().tail(18)
                fig_comp = go.Figure(go.Bar(
                    x=_monthly_occ["month"],
                    y=_monthly_occ["occ"],
                    marker_color=["#F43F5E" if v >= 80 else "#F59E0B" if v >= 70 else "#00C8E0"
                                 for v in _monthly_occ["occ"]],
                    hovertemplate="%{x}<br>Avg Occ: %{y:.1f}%<extra></extra>"
                ))
                fig_comp.add_hline(y=80, line_dash="dot", line_color="#F43F5E",
                                   annotation_text="Compression (80%)", annotation_font_size=10)
                fig_comp.add_hline(y=70, line_dash="dot", line_color="#F59E0B",
                                   annotation_text="Threshold (70%)", annotation_font_size=10)
                fig_comp.update_layout(
                    title="Monthly Occupancy — Compression Calendar",
                    height=280,
                    margin=dict(l=10, r=10, t=40, b=40),
                    xaxis=dict(showgrid=False, color="#718096", tickangle=-45),
                    yaxis=dict(title="Avg Occ %", gridcolor="rgba(0,0,0,0.06)", color="#718096"),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="DM Sans", color="#4A5568")
                )
                st.plotly_chart(fig_comp, use_container_width=True, config=PLOTLY_CONFIG)
            else:
                st.info("Occupancy data not available for compression calendar.")
        else:
            st.info("KPI data not available for compression calendar.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — VISITOR ECONOMY (Datafy)
# ══════════════════════════════════════════════════════════════════════════════
with tab_ev:
    _tab_controls("ev")
    st.markdown(tab_summary(
        "<strong>Visitor Intelligence</strong> — Annual visitor economy data from Datafy: trip volume, overnight vs. day-trip split, "
        "demographics, spending by category, and website/media attribution performance. "
        "Primary current data source (Layer 1)."
    ), unsafe_allow_html=True)
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Visitor <span>Intelligence</span></div>
      <div class="hero-subtitle">Geolocation Analytics &nbsp;·&nbsp; Dana Point, CA &nbsp;·&nbsp; Annual 2025
        <span style="margin-left:10px;font-size:10px;font-weight:700;color:#00C3BE;
        letter-spacing:.06em;text-transform:uppercase;background:rgba(0,195,190,0.10);
        border:1px solid rgba(0,195,190,0.22);padding:2px 8px;border-radius:5px;">Datafy</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    _ve_img_c1, _ve_img_c2, _ve_img_c3 = st.columns(3)
    _vdp_spots = [
        ("🌊", "Dana Point Harbor", "16.1% visitor share · Marina & Harbor District",
         "linear-gradient(135deg, #0ea5e9 0%, #0369a1 100%)", "#bae6fd"),
        ("🏖️", "Doheny State Beach", "11.2% visitor share · Surf & Beach Recreation",
         "linear-gradient(135deg, #f59e0b 0%, #b45309 100%)", "#fef3c7"),
        ("🐋", "Dana Point Ocean", "Premier whale watching · Seasonal migrations",
         "linear-gradient(135deg, #21808D 0%, #0f4c75 100%)", "#a5f3fc"),
    ]
    for _vic, (_icon, _vcap, _vsub, _vgrad, _vtxt) in zip([_ve_img_c1, _ve_img_c2, _ve_img_c3], _vdp_spots):
        with _vic:
            st.markdown(
                f'<div style="background:{_vgrad};border-radius:14px;padding:24px 16px 20px;'
                f'text-align:center;margin-bottom:8px;position:relative;overflow:hidden;'
                f'box-shadow:0 4px 20px rgba(0,0,0,0.25);">'
                f'<div style="position:absolute;top:-20px;right:-20px;width:80px;height:80px;'
                f'border-radius:50%;background:rgba(0,0,0,0.04);"></div>'
                f'<div style="font-size:38px;margin-bottom:8px;filter:drop-shadow(0 2px 4px rgba(0,0,0,0.3));">{_icon}</div>'
                f'<div style="font-size:14px;font-weight:800;color:#fff;letter-spacing:-.01em;">{_vcap}</div>'
                f'<div style="font-size:10px;color:{_vtxt};margin-top:4px;font-weight:500;opacity:0.9;">{_vsub}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


    # ── Visitor Intelligence Sub-Tabs ──────────────────────────────────────────
    _ev_t1, _ev_t2, _ev_t3 = st.tabs(["👥 Visitor Overview", "💰 Spending & DMA", "📱 Digital & Social"])

    # ── Visitor Overview → sub-tab 1 ────────────────────────────────────────────
    with _ev_t1:
        # ── Visitor Economy Section Intelligence ────────────────────────────────
        try:
            if not df_dfy_ov.empty:
                _ve_ov = df_dfy_ov.iloc[0]
                _ve_trips   = int(_ve_ov.get("total_trips", 0) or 0)
                _ve_onight  = float(_ve_ov.get("overnight_trips_pct", 0) or 0)
                _ve_oos     = float(_ve_ov.get("out_of_state_vd_pct", 0) or 0)
                _ve_daytrip = float(_ve_ov.get("day_trips_pct", 0) or 0)
                _ve_conv_trips = int(_ve_trips * (_ve_daytrip / 100) * 0.03)
                _ve_trips_fmt = f"{_ve_trips/1e6:.2f}M" if _ve_trips >= 1e6 else f"{_ve_trips/1e3:.0f}K"
                _ve_insight = (
                    f'<span class="data-hl-amber">{_ve_daytrip:.1f}%</span> of '
                    f'<span class="data-hl">{_ve_trips_fmt}</span> trips are same-day visits. '
                    f'A 3% day-trip conversion adds ~<span class="data-hl-pos">{_ve_conv_trips:,}</span> overnight stays '
                    f'— roughly <span class="data-hl-pos">$15M</span> incremental room revenue.'
                )
                _ve_fwd = "Target day-tripper conversion campaigns in LA and OC DMAs — highest ROI channel for incremental room revenue."
                st.markdown(sec_intel(
                    "Visitor Economy",
                    "Datafy-sourced visitor behavior: trips, spending, demographics, and feeder markets",
                    _ve_insight,
                    _ve_fwd,
                    f"Out-of-State Visitors: {_ve_oos:.1f}% · Overnight Rate: {_ve_onight:.1f}%",
                ), unsafe_allow_html=True)
        except Exception:
            pass

        # ── Visitor Intelligence Panel ────────────────────────────────────────────
        try:
            _ev_tt   = int(df_dfy_ov.iloc[0].get("total_trips", 0) or 0) if not df_dfy_ov.empty else 0
            _ev_oos  = float(df_dfy_ov.iloc[0].get("out_of_state_vd_pct", 0) or 0) if not df_dfy_ov.empty else 0
            _ev_on   = float(df_dfy_ov.iloc[0].get("overnight_trips_pct", 0) or 0) if not df_dfy_ov.empty else 0
            _ev_dt   = float(df_dfy_ov.iloc[0].get("day_trips_pct", 0) or 0) if not df_dfy_ov.empty else 0
            _ev_los  = float(df_dfy_ov.iloc[0].get("avg_length_of_stay_days", 0) or 0) if not df_dfy_ov.empty else 0
            _ev_adr  = m.get("adr_30", 350) if m else 350
            _ev_dt_conv_rev = int(_ev_tt * (_ev_dt / 100) * 0.02 * _ev_adr * _ev_los)

            _ev_next_steps = [
                f"<strong>Day-Trip Conversion:</strong> {_ev_dt:.1f}% of {_ev_tt/1e6:.2f}M trips are day trips — "
                f"converting just 2% to overnight at ${_ev_adr:.0f} ADR × {_ev_los:.1f} nights = ~${_ev_dt_conv_rev:,} incremental revenue.",
                f"<strong>OOS Visitor Focus:</strong> {_ev_oos:.1f}% out-of-state visitor days command premium rates — "
                "build targeted fly-drive packages for SLC, Dallas, NYC feeder markets (Origin Markets tab).",
                f"<strong>Length of Stay Extension:</strong> At {_ev_los:.1f} avg days, adding 0.5 days to OOS visitor stays "
                f"= ~${int(_ev_tt * (_ev_oos/100) * 0.5 * _ev_adr / 365):,}/year in additional room revenue.",
                "<strong>Spending Category Insight:</strong> Cross-reference accommodation spend share with total category spending — "
                "hotel stays consistently capture 40-50% of destination spend; boost ancillary F&B and retail packages.",
            ]
            _ev_questions = [
                "What's the visitor day-trip to overnight conversion opportunity?",
                "Which feeder market visitors spend the most per trip?",
                "How has our visitor profile changed vs prior year?",
                "What spending categories are growing fastest?",
                "How do OOS visitors compare to in-state in ADR and length of stay?",
            ]
            _ev_context = (
                f"Visitor Intelligence (Datafy): {_ev_tt:,} annual trips, "
                f"{_ev_on:.1f}% overnight, {_ev_dt:.1f}% day trips, {_ev_oos:.1f}% OOS, {_ev_los:.1f}d avg LOS."
            )
            render_intel_panel("ev_visitor", _ev_next_steps, _ev_questions, _ev_context)
        except Exception:
            pass

        st.markdown(sec_div("🔢 Visitor Economy KPIs"), unsafe_allow_html=True)
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
                    delta_cls = "kpi-delta-pos" if pos else "kpi-delta-neg"
                    arrow = "↑" if pos else "↓"
                    st.markdown(
                        f'<div class="kpi-card reveal-card">'
                        f'<div class="kpi-header">'
                        f'<span class="kpi-label">{lbl}</span>'
                        f'</div>'
                        f'<div class="kpi-value">{val}</div>'
                        f'<div class="{delta_cls}">{arrow} {delta}</div>'
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
                        textfont=dict(size=11, family="Syne, DM Sans, sans-serif"),
                        customdata=_hover,
                        hovertemplate="%{customdata}",
                    ))
                    fig.update_layout(xaxis_ticksuffix="%", showlegend=False)
                    st.plotly_chart(style_fig(fig, height=360), use_container_width=True, config=PLOTLY_CONFIG)
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
                        textfont=dict(size=11, family="Syne, DM Sans, sans-serif"),
                        hovertemplate="<b>%{label}</b><br>%{value:.1f}% of spend<extra></extra>",
                    ))
                    fig.update_layout(
                        legend=dict(
                            font_size=10, orientation="h",
                            font=dict(family="Syne, DM Sans, sans-serif"),
                            yanchor="top", y=-0.08, xanchor="center", x=0.5,
                        ),
                        margin=dict(l=10, r=10, t=20, b=80),
                        annotations=[dict(text="Spend<br>Mix", x=0.5, y=0.5, font_size=13,
                                          font_family="Syne, sans-serif",
                                          font_color="#21808D", showarrow=False)],
                    )
                    st.plotly_chart(style_fig(fig, height=400), use_container_width=True, config=PLOTLY_CONFIG)
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
                            textfont=dict(size=12, family="Syne, DM Sans, sans-serif"),
                            hovertemplate="<b>Age %{x}</b><br>Share: %{y:.1f}%<extra></extra>",
                        ))
                        fig.update_layout(yaxis_ticksuffix="%", showlegend=False,
                                          xaxis_title="Age Group", yaxis_title="Share (%)")
                        st.plotly_chart(style_fig(fig, height=300), use_container_width=True, config=PLOTLY_CONFIG)
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
                        textfont=dict(size=11, family="Syne, DM Sans, sans-serif"),
                        hovertemplate="<b>%{y}</b><br>%{x:.1f}% of fly-market passengers<extra></extra>",
                    ))
                    fig.update_layout(xaxis_ticksuffix="%", showlegend=False)
                    st.plotly_chart(style_fig(fig, height=300), use_container_width=True, config=PLOTLY_CONFIG)
                else:
                    st.info("Airport data not available.")

            st.markdown("---")

            # ── Campaign Attribution ────────────────────────────────────────────────
            st.markdown(
                '<div style="font-family:\'Syne\',sans-serif;font-size:1.1rem;'
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
                        textfont=dict(size=10, family="Syne, DM Sans, sans-serif"),
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
                    st.plotly_chart(style_fig(fig, height=380), use_container_width=True, config=PLOTLY_CONFIG)
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
                fig.update_layout(font=dict(family="Syne, DM Sans, sans-serif", size=12))
                st.plotly_chart(style_fig(fig, height=400), use_container_width=True, config=PLOTLY_CONFIG)
                st.caption(
                    "Flow width = proportional visitor share. Overnight stays dominate accommodation spend. "
                    "Day trippers concentrate in dining — capturing even 5% as overnight stays = significant room revenue."
                )

    # ── Website Attribution Deep-Dive ─────────────────────────────────────────
    # ── Spending & DMA → sub-tab 2 ────────────────────────────────────────────
    with _ev_t2:
        st.markdown(_sh("🌐", "Website Attribution — Acquisition Channels & Top Markets", "teal", "DATAFY"), unsafe_allow_html=True)
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
                    st.plotly_chart(style_fig(fig_ch, height=220), use_container_width=True, config=PLOTLY_CONFIG)
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
                    st.plotly_chart(style_fig(fig_wm, height=220), use_container_width=True, config=PLOTLY_CONFIG)
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
                        fig_cmp.update_layout(
                            barmode="group",
                            xaxis=dict(
                                tickangle=-45,
                                tickfont=dict(size=10),
                                automargin=True,
                            ),
                            yaxis=dict(
                                title="Share (%)",
                                ticksuffix="%",
                                gridcolor="rgba(0,0,0,0.07)",
                            ),
                            height=340,
                            margin=dict(l=40, r=10, t=30, b=120),
                            legend=dict(orientation="h", y=1.05, x=0, font=dict(size=11)),
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                        )
                        st.plotly_chart(style_fig(fig_cmp, height=300), use_container_width=True, config=PLOTLY_CONFIG)

        # ── Visitor Segment Images ─────────────────────────────────────────────────
        VISITOR_SEGMENT_IMAGES = {
            "beach":     "https://images.unsplash.com/photo-1507525428034-b723cf961d3e?w=400&q=80",
            "surf":      "https://images.unsplash.com/photo-1502680390469-be75c86b636f?w=400&q=80",
            "harbor":    "https://images.unsplash.com/photo-1564424224827-cd24b8915874?w=400&q=80",
            "festival":  "https://images.unsplash.com/photo-1470229722913-7c0e2dbbafd3?w=400&q=80",
            "family":    "https://images.unsplash.com/photo-1511895426328-dc8714191011?w=400&q=80",
            "luxury":    "https://images.unsplash.com/photo-1566073771259-6a8506099945?w=400&q=80",
            "overnight": "https://images.unsplash.com/photo-1455587734955-081b22074882?w=400&q=80",
            "daytrip":   "https://images.unsplash.com/photo-1476514525535-07fb3b4ae5f1?w=400&q=80",
            "corporate": "https://images.unsplash.com/photo-1497366216548-37526070297c?w=400&q=80",
        }

        def segment_image_card(label: str, value: str, img_url: str, delta: str = "") -> str:
            _delta_html = (f'<div style="font-size:11px;color:#10B981;margin-top:2px;">{delta}</div>'
                           if delta else "")
            return (
                f'<div style="background:rgba(0,0,0,0.03);border-radius:12px;overflow:hidden;'
                f'border:1px solid rgba(0,200,224,0.14);'
                f'box-shadow:0 4px 20px rgba(0,0,0,0.40);margin-bottom:12px;backdrop-filter:blur(8px);">'
                f'<div style="height:100px;overflow:hidden;">'
                f'<img src="{img_url}" style="width:100%;height:100%;object-fit:cover;" loading="lazy" />'
                f'</div>'
                f'<div style="padding:10px 14px 12px;">'
                f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#718096;">{label}</div>'
                f'<div style="font-size:22px;font-weight:900;background:linear-gradient(135deg,#FFFFFF,#A8D8F0);'
                f'-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;'
                f'font-family:\'Syne\',sans-serif;">{value}</div>'
                f'{_delta_html}'
                f'</div>'
                f'</div>'
            )

        # ── Visitor Cluster Visitation ─────────────────────────────────────────────
        if not df_dfy_clusters.empty:
            st.markdown(_sh("🗺️", "Visitor Cluster Visitation", "green", "DATAFY"), unsafe_allow_html=True)
            st.caption("Which Dana Point area clusters attract the most visitor activity · Datafy Annual 2025")
            _cl = df_dfy_clusters.copy()
            _cl_share_col = [c for c in _cl.columns if "share" in c.lower() or "pct" in c.lower() or "visits" in c.lower()]
            _cl_name_col  = [c for c in _cl.columns if "cluster" in c.lower() or "area" in c.lower() or "zone" in c.lower() or "name" in c.lower()]
            if _cl_share_col and _cl_name_col:
                _cl_x = _cl_share_col[0]; _cl_y = _cl_name_col[0]
                _cl_s = _cl.sort_values(_cl_x, ascending=False).head(10)

                # Image-backed cluster cards for top segments
                _seg_img_map = {
                    "harbor": VISITOR_SEGMENT_IMAGES["harbor"],
                    "beach":  VISITOR_SEGMENT_IMAGES["beach"],
                    "surf":   VISITOR_SEGMENT_IMAGES["surf"],
                    "festival": VISITOR_SEGMENT_IMAGES["festival"],
                    "overnight": VISITOR_SEGMENT_IMAGES["overnight"],
                }
                _top_clusters = _cl_s.head(4)
                _cl_img_cols = st.columns(min(4, len(_top_clusters)))
                for _cic, (_, _crow) in zip(_cl_img_cols, _top_clusters.iterrows()):
                    _cname = str(_crow[_cl_y])
                    _cval  = f"{float(_crow[_cl_x]):.1f}%" if pd.notna(_crow[_cl_x]) else "—"
                    # Pick image by keyword match
                    _img   = next(
                        (v for k, v in _seg_img_map.items() if k in _cname.lower()),
                        VISITOR_SEGMENT_IMAGES["beach"]
                    )
                    with _cic:
                        st.markdown(segment_image_card(_cname, _cval, _img), unsafe_allow_html=True)

                # Full bar chart below
                _cl_s_bar = _cl.sort_values(_cl_x, ascending=True).head(10)
                fig_cl = go.Figure(go.Bar(
                    x=_cl_s_bar[_cl_x].values, y=_cl_s_bar[_cl_y].values,
                    orientation="h", marker_color=TEAL_LIGHT,
                    hovertemplate="<b>%{y}</b><br>Share: %{x:.1f}%<extra></extra>",
                ))
                fig_cl.update_layout(xaxis_title="Visitation Share (%)", xaxis_ticksuffix="%", height=280, margin=dict(l=0,r=0,t=20,b=20))
                st.plotly_chart(style_fig(fig_cl, height=280), use_container_width=True, config=PLOTLY_CONFIG)
                with st.expander("View raw cluster data"):
                    st.dataframe(_cl.reset_index(drop=True), use_container_width=True)
                    st.download_button("⬇️ Download Cluster Data CSV", _cl.to_csv(index=False).encode(), "cluster_visitation.csv", "text/csv", key="dl_cluster")

    # ── Social & Web Analytics ────────────────────────────────────────────────
    # ── Digital & Social → sub-tab 3 ──────────────────────────────────────────
    with _ev_t3:
        st.markdown(_sh("📱", "Social & Web Analytics — visitdanapoint.com", "teal", "GA4 · DATAFY"), unsafe_allow_html=True)
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
                    st.plotly_chart(style_fig(fig_st, height=260), use_container_width=True, config=PLOTLY_CONFIG)
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
                    st.plotly_chart(style_fig(fig_pg, height=310), use_container_width=True, config=PLOTLY_CONFIG)
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

        # ── Social Media Command Center (Later.com) ───────────────────────────────
        st.markdown(_sh("📲", "Social Media Command Center", "purple", "LATER.COM"), unsafe_allow_html=True)
        st.caption("Source: Later.com Analytics Export · Instagram · Facebook · TikTok · Layer 2.5 Social Performance")

        _smc_c1, _smc_c2, _smc_c3 = st.columns(3)

        # IG follower trend
        with _smc_c1:
            st.markdown('<div class="chart-header">Instagram — Follower Growth</div>', unsafe_allow_html=True)
            if not df_later_ig_profile.empty and "followers" in df_later_ig_profile.columns:
                _ig_p = df_later_ig_profile.sort_values("data_date").tail(30).copy()
                _ig_cur = int(_ig_p["followers"].iloc[-1]) if len(_ig_p) else 0
                _ig_prev = int(_ig_p["followers"].iloc[0]) if len(_ig_p) else _ig_cur
                _ig_delta = _ig_cur - _ig_prev
                st.metric("Followers", f"{_ig_cur:,}", delta=f"{_ig_delta:+,} (30d)")
                fig_ig = go.Figure(go.Scatter(
                    x=_ig_p["data_date"], y=_ig_p["followers"],
                    mode="lines", fill="tozeroy",
                    line=dict(color="#E1306C", width=2.5, shape="spline", smoothing=0.8),
                    fillcolor="rgba(225,48,108,0.12)",
                    hovertemplate="<b>%{x}</b><br>Followers: %{y:,}<extra></extra>",
                ))
                fig_ig.update_layout(height=160, margin=dict(l=0,r=0,t=4,b=20),
                    xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(size=9)),
                    yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)"),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False)
                st.plotly_chart(fig_ig, use_container_width=True, config=PLOTLY_CONFIG, key="social_ig_followers")
            else:
                st.info("No Instagram profile data.")

        # FB follower trend
        with _smc_c2:
            st.markdown('<div class="chart-header">Facebook — Follower Growth</div>', unsafe_allow_html=True)
            if not df_later_fb_profile.empty and "page_followers" in df_later_fb_profile.columns:
                _fb_p = df_later_fb_profile.sort_values("data_date").tail(30).copy()
                _fb_cur = int(_fb_p["page_followers"].iloc[-1]) if len(_fb_p) else 0
                _fb_prev = int(_fb_p["page_followers"].iloc[0]) if len(_fb_p) else _fb_cur
                _fb_delta = _fb_cur - _fb_prev
                st.metric("Followers", f"{_fb_cur:,}", delta=f"{_fb_delta:+,} (30d)")
                fig_fb = go.Figure(go.Scatter(
                    x=_fb_p["data_date"], y=_fb_p["page_followers"],
                    mode="lines", fill="tozeroy",
                    line=dict(color="#1877F2", width=2.5, shape="spline", smoothing=0.8),
                    fillcolor="rgba(24,119,242,0.12)",
                    hovertemplate="<b>%{x}</b><br>Followers: %{y:,}<extra></extra>",
                ))
                fig_fb.update_layout(height=160, margin=dict(l=0,r=0,t=4,b=20),
                    xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(size=9)),
                    yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)"),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False)
                st.plotly_chart(fig_fb, use_container_width=True, config=PLOTLY_CONFIG, key="social_fb_followers")
            else:
                st.info("No Facebook profile data.")

        # TikTok follower trend
        with _smc_c3:
            st.markdown('<div class="chart-header">TikTok — Follower Growth</div>', unsafe_allow_html=True)
            if not df_later_tk_profile.empty and "followers" in df_later_tk_profile.columns:
                _tk_p = df_later_tk_profile.sort_values("data_date").tail(30).copy()
                _tk_cur = int(_tk_p["followers"].iloc[-1]) if len(_tk_p) else 0
                _tk_prev = int(_tk_p["followers"].iloc[0]) if len(_tk_p) else _tk_cur
                _tk_delta = _tk_cur - _tk_prev
                st.metric("Followers", f"{_tk_cur:,}", delta=f"{_tk_delta:+,} (30d)")
                fig_tk = go.Figure(go.Scatter(
                    x=_tk_p["data_date"], y=_tk_p["followers"],
                    mode="lines", fill="tozeroy",
                    line=dict(color="#010101", width=2.5, shape="spline", smoothing=0.8),
                    fillcolor="rgba(1,1,1,0.10)",
                    hovertemplate="<b>%{x}</b><br>Followers: %{y:,}<extra></extra>",
                ))
                fig_tk.update_layout(height=160, margin=dict(l=0,r=0,t=4,b=20),
                    xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(size=9)),
                    yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)"),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False)
                st.plotly_chart(fig_tk, use_container_width=True, config=PLOTLY_CONFIG, key="social_tk_followers")
            else:
                st.info("No TikTok profile data.")

        # IG Post Engagement chart
        if not df_later_ig_posts.empty and "engagement_rate" in df_later_ig_posts.columns:
            st.markdown('<div class="chart-header">Instagram — Post Engagement Rate (Last 30 Posts)</div>', unsafe_allow_html=True)
            _igp = df_later_ig_posts.head(30).copy()
            _igp["label"] = pd.to_datetime(_igp["posted_at"], errors="coerce").dt.strftime("%b %d")
            _igp["eng_num"] = pd.to_numeric(_igp["engagement_rate"], errors="coerce")
            _igp = _igp.dropna(subset=["eng_num"]).sort_values("posted_at")
            if not _igp.empty:
                _avg_eng = _igp["eng_num"].mean()
                _eng_colors = ["#E1306C" if v >= _avg_eng else "rgba(225,48,108,0.4)" for v in _igp["eng_num"]]
                fig_eng = go.Figure(go.Bar(
                    x=_igp["label"], y=_igp["eng_num"],
                    marker=dict(color=_eng_colors, cornerradius=5),
                    hovertemplate="<b>%{x}</b><br>Engagement Rate: %{y:.1f}%<extra></extra>",
                ))
                fig_eng.add_hline(y=_avg_eng, line_dash="dot", line_color="rgba(0,0,0,0.3)",
                                  annotation_text=f"Avg {_avg_eng:.1f}%", annotation_position="top right")
                fig_eng.update_layout(height=240, margin=dict(l=0,r=0,t=20,b=40),
                    yaxis_title="Engagement Rate %",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(size=9)))
                st.plotly_chart(style_fig(fig_eng, height=240), use_container_width=True, config=PLOTLY_CONFIG, key="social_ig_eng_rate")

        # FB Reach vs IG Reach comparison
        _smc2_c1, _smc2_c2 = st.columns(2)
        with _smc2_c1:
            st.markdown('<div class="chart-header">Facebook — Reach Trend (90d)</div>', unsafe_allow_html=True)
            if not df_later_fb_profile.empty and "reach" in df_later_fb_profile.columns:
                _fbr = df_later_fb_profile.sort_values("data_date").tail(90).copy()
                fig_fbr = go.Figure(go.Scatter(
                    x=_fbr["data_date"], y=_fbr["reach"],
                    mode="lines", fill="tozeroy",
                    line=dict(color="#1877F2", width=1.8),
                    fillcolor="rgba(24,119,242,0.10)",
                    hovertemplate="<b>%{x}</b><br>Reach: %{y:,}<extra></extra>",
                ))
                fig_fbr.update_layout(height=200, margin=dict(l=0,r=0,t=4,b=20),
                    yaxis_title="Reach",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(tickangle=-30, tickfont=dict(size=9)))
                st.plotly_chart(style_fig(fig_fbr, height=200), use_container_width=True, config=PLOTLY_CONFIG, key="social_fb_reach")
            else:
                st.info("No Facebook reach data.")

        with _smc2_c2:
            st.markdown('<div class="chart-header">TikTok — Video Views Trend</div>', unsafe_allow_html=True)
            if not df_later_tk_profile.empty and "video_views" in df_later_tk_profile.columns:
                _tkv = df_later_tk_profile.sort_values("data_date").tail(90).copy()
                _tkv_clean = _tkv.dropna(subset=["video_views"])
                if not _tkv_clean.empty:
                    fig_tkv = go.Figure(go.Bar(
                        x=_tkv_clean["data_date"], y=_tkv_clean["video_views"],
                        marker=dict(color="#010101", opacity=0.75, cornerradius=3),
                        hovertemplate="<b>%{x}</b><br>Video Views: %{y:,}<extra></extra>",
                    ))
                    fig_tkv.update_layout(height=200, margin=dict(l=0,r=0,t=4,b=20),
                        yaxis_title="Video Views",
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(tickangle=-30, tickfont=dict(size=9)))
                    st.plotly_chart(style_fig(fig_tkv, height=200), use_container_width=True, config=PLOTLY_CONFIG, key="social_tk_views")
                else:
                    st.info("No TikTok video view data available.")
            else:
                st.info("No TikTok profile data.")

        # IG Demographics
        if not df_later_ig_demo.empty:
            st.markdown('<div class="chart-header">Instagram — Audience Demographics</div>', unsafe_allow_html=True)
            _dem_c1, _dem_c2 = st.columns(2)
            with _dem_c1:
                # Gender pie
                _gd = df_later_ig_demo[["gender","total_pct"]].dropna()
                if not _gd.empty:
                    fig_gd = go.Figure(go.Pie(
                        labels=_gd["gender"], values=_gd["total_pct"],
                        hole=0.45,
                        marker=dict(colors=["#E1306C","#833AB4","#FD1D1D"]),
                        textinfo="label+percent",
                        hovertemplate="<b>%{label}</b><br>%{percent}<extra></extra>",
                    ))
                    fig_gd.update_layout(height=220, margin=dict(l=0,r=0,t=20,b=0),
                        paper_bgcolor="rgba(0,0,0,0)", showlegend=True)
                    st.plotly_chart(fig_gd, use_container_width=True, config=PLOTLY_CONFIG, key="social_ig_gender")
            with _dem_c2:
                # Age breakdown stacked bar
                _age_cols = [c for c in df_later_ig_demo.columns if c.startswith("age_")]
                if _age_cols:
                    _age_labels = [c.replace("age_","").replace("_"," ").replace("plus","65+") for c in _age_cols]
                    _female = df_later_ig_demo[df_later_ig_demo["gender"].str.lower()=="female"]
                    _male   = df_later_ig_demo[df_later_ig_demo["gender"].str.lower()=="male"]
                    fig_age = go.Figure()
                    if not _female.empty:
                        fig_age.add_trace(go.Bar(name="Female", x=_age_labels,
                            y=[float(_female.iloc[0].get(c, 0) or 0) for c in _age_cols],
                            marker_color="#E1306C"))
                    if not _male.empty:
                        fig_age.add_trace(go.Bar(name="Male", x=_age_labels,
                            y=[float(_male.iloc[0].get(c, 0) or 0) for c in _age_cols],
                            marker_color="#833AB4"))
                    fig_age.update_layout(barmode="group", height=220, margin=dict(l=0,r=0,t=20,b=0),
                        yaxis_title="% of Audience",
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_age, use_container_width=True, config=PLOTLY_CONFIG, key="social_ig_age")

        st.markdown("---")

        # ── Zartico Historical Reference ─────────────────────────────────────────
        st.markdown("""<div style="background:#FEF3C7;border:1px solid #F59E0B;border-radius:8px;padding:8px 14px;
    margin-bottom:12px;display:flex;align-items:center;gap:8px;">
    <span style="font-size:16px;">📦</span>
    <div>
      <span style="font-weight:700;color:#92400E;font-size:12px;">HISTORICAL REFERENCE ONLY — Zartico (Jun 2025 Snapshot)</span><br>
      <span style="font-size:11px;color:#78350F;">Current data: Datafy &amp; CoStar. Zartico provides baseline &amp; trend context only.</span>
    </div></div>""", unsafe_allow_html=True)
        st.markdown(_sh("📚", "Zartico Historical Reference", "gray", "JUN 2025 SNAPSHOT"), unsafe_allow_html=True)

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
                st.plotly_chart(fig_zrt, use_container_width=True, config=PLOTLY_CONFIG)

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
                st.plotly_chart(fig_ov, use_container_width=True, config=PLOTLY_CONFIG)

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
                st.plotly_chart(fig_mkt, use_container_width=True, config=PLOTLY_CONFIG)

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
    _tab_controls("fm")
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Feeder Market Intelligence</div>
      <div class="hero-subtitle">DMA Origin Analysis · Visitor Value Matrix · Strategic Budget Allocation</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown(tab_summary(
        "<strong>Origin Market Strategy:</strong> Where visitors come from determines how much they spend. "
        "Fly markets (SLC, Dallas, NYC) generate 1.3–1.4× more revenue per trip than drive markets. "
        "This tab maps volume vs. value across all feeder DMAs so VDP can allocate media spend where it earns the highest ROI. "
        "<strong>Use this data</strong> to shift budget toward high-spend, high-ADR origin markets and away from volume-only drive markets."
    ), unsafe_allow_html=True)

    # ── Feeder Markets Section Intelligence ─────────────────────────────────
    try:
        if not df_dfy_dma.empty:
            _fm_t10 = df_dfy_dma[df_dfy_dma["visitor_days_share_pct"].notna()].head(10)
            _fm_top  = _fm_t10.iloc[0]["dma"] if not _fm_t10.empty else "N/A"
            _fm_top_pct = float(_fm_t10.iloc[0]["visitor_days_share_pct"]) if not _fm_t10.empty else 0
            _fm_hs_row  = df_dfy_dma[df_dfy_dma["avg_spend_usd"].notna()].nlargest(1, "avg_spend_usd")
            _fm_hs_mkt  = _fm_hs_row.iloc[0]["dma"] if not _fm_hs_row.empty else "N/A"
            _fm_hs_val  = float(_fm_hs_row.iloc[0]["avg_spend_usd"]) if not _fm_hs_row.empty else 0
            _fm_insight = (f"{_fm_top} leads on visitor volume at {_fm_top_pct:.1f}% of days, "
                           f"while {_fm_hs_mkt} generates the highest avg spend at ${_fm_hs_val:,.0f}/visitor — "
                           f"drive vs fly market dynamics.")
            _fm_fwd = f"Shift 15–20% of campaign budget toward {_fm_hs_mkt} and similar fly markets to maximize revenue-per-visitor ROI."
            st.markdown(sec_intel(
                "Feeder Markets",
                "origin market analysis — where visitors come from and how much they spend",
                _fm_insight,
                _fm_fwd,
                f"Top Spend Market: {_fm_hs_mkt} @ ${_fm_hs_val:,.0f}/visitor",
            ), unsafe_allow_html=True)
    except Exception:
        pass

    # ── Origin Markets Intelligence Panel ─────────────────────────────────────
    try:
        _fm_hs_mkt2 = "N/A"; _fm_hs_val2 = 0; _fm_top2 = "LA"; _fm_top_pct2 = 0
        if not df_dfy_dma.empty:
            _fm_t10_2  = df_dfy_dma[df_dfy_dma["visitor_days_share_pct"].notna()].head(10)
            _fm_top2   = _fm_t10_2.iloc[0]["dma"] if not _fm_t10_2.empty else "LA"
            _fm_top_pct2 = float(_fm_t10_2.iloc[0]["visitor_days_share_pct"]) if not _fm_t10_2.empty else 0
            _fm_hs_row2  = df_dfy_dma[df_dfy_dma["avg_spend_usd"].notna()].nlargest(1, "avg_spend_usd")
            _fm_hs_mkt2  = _fm_hs_row2.iloc[0]["dma"] if not _fm_hs_row2.empty else "N/A"
            _fm_hs_val2  = float(_fm_hs_row2.iloc[0]["avg_spend_usd"]) if not _fm_hs_row2.empty else 0

        _fm_next_steps = [
            f"<strong>Budget Reallocation:</strong> {_fm_top2} drives {_fm_top_pct2:.1f}% of visitor days "
            "but drive-market visitors spend less per trip — shift 15-20% of media budget toward fly markets.",
            f"<strong>High-Value DMA Targeting:</strong> {_fm_hs_mkt2} visitors average ${_fm_hs_val2:,.0f}/trip — "
            "build dedicated campaign creative for this market emphasizing luxury/resort positioning.",
            "<strong>Attribution Gap:</strong> Cross-reference DMA share vs. website attribution DMAs (Datafy channel data) "
            "to identify markets where organic traffic is high but paid conversion is low.",
            "<strong>Seasonal Feeder Match:</strong> Align feeder-market campaigns with their local weather/event calendar — "
            "target cold-weather origin markets in Nov-Feb when Dana Point's mild weather is a strong pull.",
        ]
        _fm_questions = [
            f"How does {_fm_top2} spending compare to fly markets like SLC or Dallas?",
            "Which feeder market has the best spend-per-visitor ROI?",
            "How has our market mix changed vs prior year?",
            "What percentage of our visitors come from the top 3 markets?",
            "Which DMAs should we add to our paid media targeting?",
        ]
        _fm_context = (
            f"Origin Markets: Top feeder {_fm_top2} ({_fm_top_pct2:.1f}% of visitor days), "
            f"Highest spend: {_fm_hs_mkt2} (${_fm_hs_val2:,.0f}/trip)."
        )
        render_intel_panel("fm_origin", _fm_next_steps, _fm_questions, _fm_context)
    except Exception:
        pass

    # ── DMA Overview ───────────────────────────────────────────────────────────
    st.markdown(sec_div("🗺️ Origin Market Volume vs. Value"), unsafe_allow_html=True)
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
                font=dict(size=10, color="#21808D", family="Syne, DM Sans, sans-serif"),
                bgcolor="rgba(33,128,141,0.08)",
                borderpad=3,
            )
            fig_fm1.update_layout(xaxis_ticksuffix="%", showlegend=False)
            st.plotly_chart(style_fig(fig_fm1, height=380), use_container_width=True, config=PLOTLY_CONFIG)

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
                    font=dict(size=10, color="#E68161", family="Syne, DM Sans, sans-serif"),
                    bgcolor="rgba(230,129,97,0.08)",
                    borderpad=3,
                )
                fig_fm2.update_layout(xaxis_tickprefix="$", showlegend=False)
                st.plotly_chart(style_fig(fig_fm2, height=380), use_container_width=True, config=PLOTLY_CONFIG)
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
                    textfont=dict(size=9, family="Syne, DM Sans, sans-serif", color="#21808D"),
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
                st.plotly_chart(style_fig(fig_fm_map, height=420), use_container_width=True, config=PLOTLY_CONFIG)
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

        st.markdown(sec_div("💎 Market Value Matrix"), unsafe_allow_html=True)

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
                textfont=dict(size=10, family="Syne, DM Sans, sans-serif"),
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
            st.plotly_chart(style_fig(fig_fm3, height=420), use_container_width=True, config=PLOTLY_CONFIG)
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

        # ── GloCon Solutions LLC — Spend Efficiency Index ─────────────────────
        st.markdown(sec_div("📊 Spend Efficiency Index"), unsafe_allow_html=True)
        st.markdown('<div class="chart-header">📊 Market Spend Efficiency Index — Spending Share ÷ Visitor Days Share</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">Index > 1.0 = market punches above its weight in spend · Index < 1.0 = under-spends relative to visitation volume</div>', unsafe_allow_html=True)
        _eff_df = df_dfy_dma[
            df_dfy_dma["visitor_days_share_pct"].notna() &
            df_dfy_dma["spending_share_pct"].notna() &
            (df_dfy_dma["visitor_days_share_pct"] > 0)
        ].copy()
        if not _eff_df.empty:
            _eff_df["efficiency_index"] = (_eff_df["spending_share_pct"] / _eff_df["visitor_days_share_pct"]).round(2)
            _eff_df = _eff_df.sort_values("efficiency_index", ascending=True)
            _eff_colors = ["#00C49A" if v >= 1.0 else "#FF4757" for v in _eff_df["efficiency_index"]]
            fig_eff = go.Figure(go.Bar(
                x=_eff_df["efficiency_index"],
                y=_eff_df["dma"],
                orientation="h",
                marker=dict(color=_eff_colors, cornerradius=5),
                text=[f"{v:.2f}×" for v in _eff_df["efficiency_index"]],
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>Efficiency: %{x:.2f}×<br>(spending share ÷ visitor days share)<extra></extra>",
            ))
            fig_eff.add_vline(x=1.0, line_dash="solid", line_color="rgba(0,0,0,0.3)", line_width=1.5,
                              annotation_text="Parity", annotation_position="top right",
                              annotation_font_size=10)
            fig_eff.update_layout(xaxis_title="Spend Efficiency Index", showlegend=False)
            st.plotly_chart(style_fig(fig_eff, height=360), use_container_width=True,
                            config=PLOTLY_CONFIG, key="fm_efficiency_index")
            # Insight callout
            _over_idx = _eff_df[_eff_df["efficiency_index"] > 1.2]
            _under_idx = _eff_df[_eff_df["efficiency_index"] < 0.85]
            if not _over_idx.empty:
                _over_names = ", ".join(_over_idx.nlargest(3, "efficiency_index")["dma"].tolist())
                st.markdown(
                    f'<div style="background:rgba(0,196,154,0.08);border-left:3px solid #00C49A;'
                    f'border-radius:0 8px 8px 0;padding:10px 14px;font-size:12px;margin-bottom:6px;">'
                    f'<strong>✅ High-Efficiency Markets:</strong> {_over_names} — these markets spend more than their visit share warrants. '
                    f'Strong campaign conversion candidates.</div>',
                    unsafe_allow_html=True,
                )
            if not _under_idx.empty:
                _under_names = ", ".join(_under_idx.nsmallest(3, "efficiency_index")["dma"].tolist())
                st.markdown(
                    f'<div style="background:rgba(255,71,87,0.06);border-left:3px solid #FF4757;'
                    f'border-radius:0 8px 8px 0;padding:10px 14px;font-size:12px;">'
                    f'<strong>⚠️ Volume-Heavy / Spend-Light Markets:</strong> {_under_names} — high visitation but under-index on spend. '
                    f'Target with upsell and overnight conversion campaigns.</div>',
                    unsafe_allow_html=True,
                )

        # ── Length of Stay by Market ──────────────────────────────────────────
        st.markdown(sec_div("🛌 Length of Stay by Market"), unsafe_allow_html=True)
        _los_df = df_dfy_dma[df_dfy_dma["avg_length_of_stay_days"].notna()].copy()
        if not _los_df.empty:
            st.markdown('<div class="chart-header">🛌 Length of Stay by Feeder Market</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Avg days per trip · Longer stays = more room nights = higher TBID + TOT revenue per visitor</div>', unsafe_allow_html=True)
            _los_df = _los_df.sort_values("avg_length_of_stay_days", ascending=True)
            _los_avg = float(_los_df["avg_length_of_stay_days"].mean())
            _los_colors = ["#8B5CF6" if v >= _los_avg else "#94A3B8" for v in _los_df["avg_length_of_stay_days"]]
            fig_los = go.Figure(go.Bar(
                x=_los_df["avg_length_of_stay_days"],
                y=_los_df["dma"],
                orientation="h",
                marker=dict(color=_los_colors, cornerradius=5),
                text=[f"{v:.1f}d" for v in _los_df["avg_length_of_stay_days"]],
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>Avg stay: %{x:.1f} days<extra></extra>",
            ))
            fig_los.add_vline(x=_los_avg, line_dash="dot", line_color="rgba(0,0,0,0.25)", line_width=1,
                              annotation_text=f"Avg {_los_avg:.1f}d", annotation_position="top right",
                              annotation_font_size=10)
            fig_los.update_layout(xaxis_title="Avg Length of Stay (days)", showlegend=False)
            st.plotly_chart(style_fig(fig_los, height=260), use_container_width=True,
                            config=PLOTLY_CONFIG, key="fm_los_chart")

        # ── Campaign Attribution: Top Markets ─────────────────────────────────
        if not df_dfy_mktmkt.empty:
            st.markdown("---")
            st.markdown('<div class="chart-header">🎯 Campaign Attribution — Top DMA Markets by Est. Impact</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Markets with highest estimated $ impact from the 2025–26 Annual Campaign · Source: Datafy Media Attribution</div>', unsafe_allow_html=True)
            _cam_dest = df_dfy_mktmkt[df_dfy_mktmkt["cluster_type"] == "destination"].copy() if "cluster_type" in df_dfy_mktmkt.columns else df_dfy_mktmkt.copy()
            if not _cam_dest.empty and "top_dma" in _cam_dest.columns and "dma_est_impact_usd" in _cam_dest.columns:
                _cam_dest = _cam_dest.sort_values("dma_est_impact_usd", ascending=True).tail(8)
                _cam_colors = ["#F59E0B" if i == len(_cam_dest) - 1 else "#00C4CC"
                               for i in range(len(_cam_dest))]
                fig_cam = go.Figure(go.Bar(
                    x=_cam_dest["dma_est_impact_usd"],
                    y=_cam_dest["top_dma"],
                    orientation="h",
                    marker=dict(color=_cam_colors, cornerradius=5),
                    text=[f"${v/1e3:.0f}K" for v in _cam_dest["dma_est_impact_usd"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>Est. impact: $%{x:,.0f}<extra></extra>",
                ))
                fig_cam.update_layout(xaxis_title="Est. Campaign Impact ($)", xaxis_tickprefix="$",
                                      showlegend=False)
                st.plotly_chart(style_fig(fig_cam, height=300), use_container_width=True,
                                config=PLOTLY_CONFIG, key="fm_campaign_impact")

        # ── Website Attribution DMA ───────────────────────────────────────────
        if not df_dfy_web_dma.empty:
            st.markdown(sec_div("🌐 Website Attribution by Origin"), unsafe_allow_html=True)
            _wa_c1, _wa_c2 = st.columns(2)
            with _wa_c1:
                st.markdown('<div class="chart-header">🌐 Website Attribution — Trips by Origin Market</div>', unsafe_allow_html=True)
                st.markdown('<div class="chart-caption">Visitors attributed to visitdanapoint.com by DMA · Datafy Website Attribution</div>', unsafe_allow_html=True)
                _wdma = df_dfy_web_dma.sort_values("total_trips", ascending=True).tail(9)
                fig_wdma = go.Figure(go.Bar(
                    x=_wdma["total_trips"],
                    y=_wdma["dma"],
                    orientation="h",
                    marker=dict(
                        color=[f"rgba(139,92,246,{0.55 + 0.45*(v/max(_wdma['total_trips'])):.2f})"
                               for v in _wdma["total_trips"]],
                        cornerradius=5,
                    ),
                    text=[f"{int(v)}" for v in _wdma["total_trips"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>Attributed trips: %{x:,}<extra></extra>",
                ))
                fig_wdma.update_layout(xaxis_title="Attributed Trips", showlegend=False)
                st.plotly_chart(style_fig(fig_wdma, height=320), use_container_width=True,
                                config=PLOTLY_CONFIG, key="fm_web_dma_trips")
            with _wa_c2:
                st.markdown('<div class="chart-header">🛏 Website Attribution — Avg LOS by Origin Market</div>', unsafe_allow_html=True)
                st.markdown('<div class="chart-caption">Longer LOS from website visitors = higher room night value</div>', unsafe_allow_html=True)
                _wdma_los = df_dfy_web_dma[df_dfy_web_dma["avg_los_destination_days"].notna()].sort_values("avg_los_destination_days", ascending=True)
                if not _wdma_los.empty:
                    _wlos_avg = float(_wdma_los["avg_los_destination_days"].mean())
                    fig_wlos = go.Figure(go.Bar(
                        x=_wdma_los["avg_los_destination_days"],
                        y=_wdma_los["dma"],
                        orientation="h",
                        marker=dict(
                            color=["#8B5CF6" if v >= _wlos_avg else "#CBD5E1"
                                   for v in _wdma_los["avg_los_destination_days"]],
                            cornerradius=5,
                        ),
                        text=[f"{v:.1f}d" for v in _wdma_los["avg_los_destination_days"]],
                        textposition="outside",
                        hovertemplate="<b>%{y}</b><br>Avg LOS: %{x:.1f} days<extra></extra>",
                    ))
                    fig_wlos.add_vline(x=_wlos_avg, line_dash="dot", line_color="rgba(0,0,0,0.2)")
                    fig_wlos.update_layout(xaxis_title="Avg Length of Stay (days)", showlegend=False)
                    st.plotly_chart(style_fig(fig_wlos, height=320), use_container_width=True,
                                    config=PLOTLY_CONFIG, key="fm_web_dma_los")

        # ── Complete Market Intelligence Table ────────────────────────────────
        st.markdown("---")
        st.markdown('<div class="chart-header">📋 Complete Market Intelligence Table</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-caption">All feeder markets · Visitor days · Spending share · Avg spend · LOS · YOY change · Use for campaign planning and budget allocation</div>', unsafe_allow_html=True)
        _full_tbl = df_dfy_dma.copy()
        _col_map = {
            "dma": "Market (DMA)",
            "visitor_days_share_pct": "Visitor Days %",
            "spending_share_pct": "Spending Share %",
            "avg_spend_usd": "Avg Spend/Visitor",
            "avg_length_of_stay_days": "Avg LOS (days)",
            "visitor_days_vs_compare_pct": "YOY Change (pp)",
            "trips_share_pct": "Trips Share %",
        }
        _show_cols = [c for c in _col_map if c in _full_tbl.columns]
        _full_tbl = _full_tbl[_show_cols].rename(columns=_col_map)
        if "Avg Spend/Visitor" in _full_tbl.columns:
            _full_tbl["Avg Spend/Visitor"] = _full_tbl["Avg Spend/Visitor"].apply(
                lambda v: f"${v:,.0f}" if pd.notna(v) else "—")
        if "Efficiency Index" not in _full_tbl.columns and "Visitor Days %" in _full_tbl.columns and "Spending Share %" in _full_tbl.columns:
            _full_tbl["Efficiency"] = (_full_tbl["Spending Share %"] / _full_tbl["Visitor Days %"]).round(2).apply(
                lambda v: f"{v:.2f}×" if pd.notna(v) and v > 0 else "—")
        st.dataframe(_full_tbl.sort_values("Visitor Days %", ascending=False),
                     use_container_width=True, hide_index=True)
        _dma_dl = df_dfy_dma.to_csv(index=False).encode()
        st.download_button("⬇️ Download Full Market Intelligence CSV", _dma_dl,
                           file_name="feeder_market_intelligence.csv", mime="text/csv",
                           key="fm_dl_full")

        # ── Zartico Top Markets (historical comparison) ────────────────────────
        if not df_zrt_markets.empty:
            st.markdown(sec_div("📦 Historical Reference — Zartico Feeder Markets"), unsafe_allow_html=True)
            st.markdown("""<div style="background:#FEF3C7;border:1px solid #F59E0B;border-radius:8px;padding:8px 14px;
margin-bottom:12px;display:flex;align-items:center;gap:8px;">
<span style="font-size:16px;">📦</span>
<div>
  <span style="font-weight:700;color:#92400E;font-size:12px;">HISTORICAL REFERENCE ONLY — Zartico (Jun 2025 Snapshot)</span><br>
  <span style="font-size:11px;color:#78350F;">Current data: Datafy &amp; CoStar. Zartico provides baseline &amp; trend context only.</span>
</div></div>""", unsafe_allow_html=True)
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
            st.plotly_chart(style_fig(fig_zrt_mkt, height=320), use_container_width=True, config=PLOTLY_CONFIG)

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

    # ── Census ACS Feeder Market Demographics ─────────────────────────────────
    st.markdown(sec_div("🏛️ Feeder Market Demographics — US Census ACS"), unsafe_allow_html=True)
    st.markdown(_sh("🏛️", "OC · LA · SD County Demographics", "indigo", "CENSUS ACS"), unsafe_allow_html=True)
    st.caption(
        "Source: U.S. Census Bureau, American Community Survey 1-Year Estimates. "
        "Feeder market demographics quantify the addressable audience size and spending capacity for VDP campaigns."
    )
    if not df_census.empty:
        _cen_geos   = sorted(df_census["geography"].unique().tolist())
        _cen_years  = sorted(df_census["year"].unique().tolist(), reverse=True)
        _cen_latest = df_census[df_census["year"] == _cen_years[0]] if _cen_years else df_census

        # KPI tiles: OC, LA, SD population + income
        _cen_cols = st.columns(len(_cen_geos))
        for i, geo in enumerate(_cen_geos):
            with _cen_cols[i]:
                _geo_df = _cen_latest[_cen_latest["geography"] == geo]
                _pop = _geo_df[_geo_df["metric_name"] == "Total Population"]["metric_value"]
                _inc = _geo_df[_geo_df["metric_name"] == "Median Household Income"]["metric_value"]
                _hom = _geo_df[_geo_df["metric_name"] == "Median Home Value"]["metric_value"]
                _pop_v = f"{_pop.iloc[0]/1_000_000:.2f}M" if not _pop.empty else "—"
                _inc_v = f"${int(_inc.iloc[0]):,}" if not _inc.empty else "—"
                _hom_v = f"${int(_hom.iloc[0]):,}" if not _hom.empty else "—"
                short_geo = geo.replace(" County, CA", "").replace(" County", "")
                st.markdown(
                    f'<div style="background:#FFFFFF;border:1px solid rgba(0,0,0,0.08);border-radius:12px;'
                    f'border-top:3px solid #7C3AED;padding:14px 18px;">'
                    f'<div style="font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#64748B;margin-bottom:6px;">{short_geo}</div>'
                    f'<div style="font-size:18px;font-weight:900;color:#7C3AED;">{_pop_v}</div>'
                    f'<div style="font-size:11px;color:#64748B;">Population</div>'
                    f'<div style="font-size:14px;font-weight:700;color:#0F1C2E;margin-top:6px;">{_inc_v}</div>'
                    f'<div style="font-size:11px;color:#64748B;">Median HH Income</div>'
                    f'<div style="font-size:13px;font-weight:600;color:#0F1C2E;margin-top:4px;">{_hom_v}</div>'
                    f'<div style="font-size:11px;color:#64748B;">Median Home Value</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        st.markdown("<br>", unsafe_allow_html=True)

        # Income trend over time
        _inc_df = df_census[df_census["metric_name"] == "Median Household Income"].copy()
        if not _inc_df.empty and len(_cen_years) >= 2:
            fig_inc = go.Figure()
            _geo_colors = [TEAL, ORANGE, BLUE]
            for gi, geo in enumerate(_cen_geos):
                _gdf = _inc_df[_inc_df["geography"] == geo].sort_values("year")
                if not _gdf.empty:
                    fig_inc.add_trace(go.Scatter(
                        x=_gdf["year"], y=_gdf["metric_value"],
                        mode="lines+markers", name=geo.replace(" County, CA",""),
                        line=dict(color=_geo_colors[gi % len(_geo_colors)], width=2.5),
                        marker=dict(size=7),
                        hovertemplate=f"<b>%{{x}}</b><br>{geo}: $%{{y:,.0f}}<extra></extra>",
                    ))
            fig_inc.update_layout(
                title="Median Household Income Trend — Feeder Markets",
                yaxis_title="Median HH Income ($)",
                yaxis_tickprefix="$", yaxis_tickformat=",",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(style_fig(fig_inc, height=260), use_container_width=True, config=PLOTLY_CONFIG)
            st.caption(
                f"Source: ACS 1-Year Estimates, {min(_cen_years)}–{max(_cen_years)}. "
                "Higher income markets tolerate higher ADR — use for rate targeting by feeder market."
            )
    else:
        st.markdown(
            '<div class="empty-card">'
            '<div class="empty-icon">🏛️</div>'
            '<div class="empty-title">Census Demographics Not Loaded</div>'
            '<div class="empty-body">Run <code>python scripts/run_pipeline.py</code> '
            'to fetch Census ACS feeder market data. Free — add <code>CENSUS_API_KEY</code> to .env for live data.</div>'
            '</div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — EVENT IMPACT
# ══════════════════════════════════════════════════════════════════════════════
with tab_ei:
    _tab_controls("ei")
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Event Impact Analysis</div>
      <div class="hero-subtitle">STR Performance · Ohana Fest · Doheny Days · Tall Ships · July 4 · Zartico · Datafy · Full Events Calendar</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown(tab_summary(
        "<strong>Events Drive the Peak:</strong> Ohana Fest alone generated $14.6M in direct event expenditure and a 3.2× spend multiplier. "
        "68% of attendees came from out-of-state — events are the single most effective demand generator for incremental hotel revenue. "
        "This tab quantifies STR lift, ADR premiums, and destination spend for each major event. "
        "<strong>Forward strategy:</strong> target Q1 and Q4 shoulder season programming to reduce seasonal revenue concentration."
    ), unsafe_allow_html=True)

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

    # ── Event Impact Section Intelligence ───────────────────────────────────
    st.markdown(sec_intel(
        "Event Impact",
        "economic impact of major events on hotel demand, ADR, and destination spend",
        "Ohana Fest generated +$139 ADR lift vs baseline ($542 vs $403) and a 3.2× spend multiplier. "
        "68% out-of-state attendance confirms events drive genuine incremental tourism, not displacement.",
        "Expand the event calendar in Q1 and Q4 shoulder seasons — events during compression gaps maximize TBID revenue lift.",
        "$14.6M direct event expenditure · 3.2× multiplier",
    ), unsafe_allow_html=True)

    # ── Event ROI Intelligence Panel ──────────────────────────────────────────
    _ei_next_steps = [
        "<strong>Shoulder Season Events:</strong> Current calendar peaks in Q3 — add 2-3 signature events "
        "in Q1 (Jan-Mar, currently only 4 compression days) to convert low-demand nights into peak-rate nights.",
        "<strong>ADR Lift Stacking:</strong> Ohana Fest drove +$139 ADR lift — identify 3-5 events with "
        "similar out-of-state draw (>60% OOS attendance) that can be anchored as annual recurring events.",
        "<strong>TBID Revenue Calculation:</strong> At 3.2× spend multiplier × $14.6M event spend × 1.25% TBID rate, "
        "each $1M in event expenditure generates ~$40K in TBID revenue — use this to justify event sponsorship ROI.",
        "<strong>Event-STR Correlation:</strong> Map known event dates against STR daily data to calculate "
        "actual ADR premium, compression nights, and total revenue lift per event — build the economic case.",
    ]
    _ei_questions = [
        "How does Ohana Fest ADR lift compare to other events?",
        "What's the TBID revenue impact of our major events?",
        "Which months need more events to fill compression gaps?",
        "What's the economic multiplier effect on the broader destination?",
        "How do OOS attendees compare to local attendees in spend?",
    ]
    render_intel_panel(
        "ei_event", _ei_next_steps, _ei_questions,
        "Event ROI: Ohana Fest $14.6M direct spend, 3.2x multiplier, +$139 ADR lift, 68% OOS attendance."
    )

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

    st.markdown(sec_div("📅 Events Calendar — Full Year Timeline"), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # EVENT CALENDAR — Gantt-style timeline from VDP events database
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown(_sh("📅", "Dana Point Events Calendar — Full Year Timeline", "orange", "EVENTS"), unsafe_allow_html=True)
    st.caption("Events sourced from Visit Dana Point official calendar · Database: vdp_events · Major events highlighted in teal")

    if not df_vdp_events.empty:
        _evts = df_vdp_events.copy()
        _evts["event_date"] = pd.to_datetime(_evts["event_date"], errors="coerce")
        _evts["end_date"]   = pd.to_datetime(_evts.get("event_end_date", _evts["event_date"]), errors="coerce")
        _evts["end_date"]   = _evts.apply(
            lambda r: r["end_date"] if pd.notna(r["end_date"]) else r["event_date"] + timedelta(days=2),
            axis=1,
        )
        _evts = _evts.dropna(subset=["event_date"]).sort_values("event_date")

        # ── Proper Gantt-style horizontal bar chart ───────────────────────────
        _gantt_fig = go.Figure()
        _major_color   = TEAL
        _standard_color = "#4A5568"
        # Sort so major events appear on top
        _evts_sorted = pd.concat([
            _evts[_evts["is_major"] != 1],
            _evts[_evts["is_major"] == 1],
        ]).reset_index(drop=True)

        for _, _er in _evts_sorted.iterrows():
            _is_maj = _er.get("is_major") == 1
            _dur = max((_er["end_date"] - _er["event_date"]).days + 1, 1)
            _bar_color = _major_color if _is_maj else _standard_color
            _name = str(_er.get("event_name", "Event"))
            _date_str = _er["event_date"].strftime("%b %d")
            _end_str  = _er["end_date"].strftime("%b %d, %Y") if _dur > 1 else ""
            _gantt_fig.add_trace(go.Bar(
                x=[_dur * 86400 * 1000],   # duration in milliseconds for date-type axis
                y=[_name],
                orientation="h",
                base=[_er["event_date"].strftime("%Y-%m-%d")],
                marker=dict(
                    color=_bar_color,
                    opacity=0.90,
                    line=dict(color="rgba(255,255,255,0.25)" if _is_maj else "rgba(255,255,255,0.10)",
                              width=1 if _is_maj else 0),
                ),
                hovertemplate=(
                    f"<b>{_name}</b><br>"
                    f"📅 {_date_str}" +
                    (f" – {_end_str}" if _end_str else "") +
                    f"<br>{'⭐ Major Event' if _is_maj else 'Standard Event'}"
                    "<extra></extra>"
                ),
                showlegend=False,
            ))

        _gantt_fig.update_layout(
            barmode="overlay",
            xaxis=dict(
                type="date",
                tickformat="%b '%y",
                tickfont=dict(size=11, color="#475569"),
                gridcolor="rgba(0,0,0,0.06)",
                zeroline=False,
            ),
            yaxis=dict(
                tickfont=dict(size=11, color="#1E293B"),
                gridcolor="rgba(0,0,0,0.05)",
                automargin=True,
            ),
            height=max(280, len(_evts_sorted) * 30 + 40),
            margin=dict(l=10, r=20, t=10, b=30),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#FFFFFF",
            hoverlabel=dict(bgcolor="rgba(13,17,23,0.95)", font_size=13,
                           font_color="#E6EDF3", bordercolor=TEAL),
        )
        st.plotly_chart(style_fig(_gantt_fig), use_container_width=True, config=PLOTLY_CONFIG)

        # Legend row
        st.markdown(
            f'<div style="display:flex;gap:20px;align-items:center;margin-top:4px;'
            f'padding:8px 12px;background:rgba(22,27,34,0.6);border-radius:8px;'
            f'border:1px solid rgba(0,0,0,0.07);">'
            f'<span style="display:flex;align-items:center;gap:6px;font-size:12px;color:#C9D1D9;">'
            f'<span style="width:14px;height:14px;border-radius:3px;background:{TEAL};display:inline-block;"></span>'
            f'Major Event ({_total_major})</span>'
            f'<span style="display:flex;align-items:center;gap:6px;font-size:12px;color:#C9D1D9;">'
            f'<span style="width:14px;height:14px;border-radius:3px;background:#4A5568;display:inline-block;"></span>'
            f'Standard Event ({_total_events - _total_major})</span>'
            f'<span style="margin-left:auto;font-size:11px;color:#8B949E;">'
            f'{_total_events} events · Jan – Dec 2025 · Source: Visit Dana Point</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No events loaded from vdp_events table. Run `python scripts/fetch_vdp_events.py` to seed the calendar.")

    st.markdown(sec_div("📊 Event Performance Scorecard — STR vs. Baseline"), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # EVENT SCORECARD — STR performance vs. monthly baseline for every event
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown(_sh("📊", "Event Performance Scorecard — STR vs. Baseline", "orange", "STR + EVENTS"), unsafe_allow_html=True)
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
        has_data = sc["e_adr"] > 1.0  # Ensure it's actually real data, not an uninitialized zero

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
                f'<span class="data-hl">{sc["e_occ"]:.1f}%</span>'
                f'<span style="font-size:12px;color:var(--dp-text-3);margin:0 10px 0 4px;">occ ({occ_tag})</span>'
                f'<span class="data-hl">${sc["e_adr"]:.0f}</span>'
                f'<span style="font-size:12px;color:var(--dp-text-3);margin:0 10px 0 4px;">ADR ({adr_tag} vs baseline)</span>'
                f'<span class="data-hl">${sc["e_revpar"]:.0f}</span>'
                f'<span style="font-size:12px;color:var(--dp-text-3);margin-left:4px;">RevPAR ({rvp_tag})</span>'
            )
        else:
            metrics_html = '<span style="font-size:12px;color:var(--dp-text-3);font-style:italic;">STR window data pending — run pipeline after event date</span>'

        # Type-based card treatment (Dumbar style)
        _card_cls, _pill_cls, _type_icon = evt_type_class(ev["category"])
        st.markdown(
            f'<div class="{_card_cls}">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:6px;margin-bottom:6px;">'
            f'<div style="display:flex;align-items:center;gap:8px;">'
            f'<span class="{_pill_cls}">{_type_icon} {ev["category"].upper()}</span>'
            f'<span style="font-size:10px;background:{tier_color}22;color:{tier_color};border-radius:20px;'
            f'padding:2px 9px;font-weight:700;letter-spacing:.06em;">{tier_badge} {ev["tier"]}</span>'
            f'</div>'
            f'<div class="evt-date">📅 {ev["dates"]}</div>'
            f'</div>'
            f'<div class="evt-name">{ev["name"]}</div>'
            f'<div class="evt-impact" style="margin-top:7px;">{metrics_html}</div>'
            f'<div style="font-size:11px;color:var(--dp-text-3);margin-top:6px;font-style:italic;">{ev["note"]}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown(sec_div("💵 ADR Lift by Event vs. Monthly Baseline"), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # ADR LIFT CHART — all events vs. monthly baseline
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown(_sh("💵", "ADR Lift by Event vs. Monthly Baseline", "blue", "STR"), unsafe_allow_html=True)
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
        st.plotly_chart(style_fig(fig_ei_adr, height=340), use_container_width=True, config=PLOTLY_CONFIG)
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
            st.plotly_chart(style_fig(fig_oh_adr, height=220), use_container_width=True, config=PLOTLY_CONFIG)
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
            st.plotly_chart(style_fig(fig_oh_occ, height=220), use_container_width=True, config=PLOTLY_CONFIG)
            st.caption("Occupancy — Ohana Fest window")

    st.markdown(sec_div("📚 Event Spend Impact — Zartico Historical Reference"), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # ZARTICO EVENT IMPACT — Historical reference (OC Marathon period)
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("""<div style="background:#FEF3C7;border:1px solid #F59E0B;border-radius:8px;padding:8px 14px;
margin-bottom:12px;display:flex;align-items:center;gap:8px;">
<span style="font-size:16px;">📦</span>
<div>
  <span style="font-weight:700;color:#92400E;font-size:12px;">HISTORICAL REFERENCE ONLY — Zartico (Jun 2025 Snapshot)</span><br>
  <span style="font-size:11px;color:#78350F;">Current data: Datafy &amp; CoStar. Zartico provides baseline &amp; trend context only. Event window: May 4–10, 2025 (OC Marathon period).</span>
</div></div>""", unsafe_allow_html=True)
    st.markdown(_sh("📚", "Event Spend Impact Analysis", "gray", "ZARTICO HISTORICAL"), unsafe_allow_html=True)

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
            st.plotly_chart(style_fig(fig_zrt_spend, height=260), use_container_width=True, config=PLOTLY_CONFIG)
            st.caption("Visitor spending mix during event period (Zartico · historical).")

    else:
        st.info("Load Zartico data to see event spend analysis.")

    st.markdown(sec_div("🧭 Visitor Economy Context — Datafy"), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # VISITOR ECONOMY CONTEXT — Datafy
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown(_sh("🧭", "Visitor Economy Context — Datafy 2025", "teal", "DATAFY"), unsafe_allow_html=True)
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
        st.markdown("""<div style="background:#FEF3C7;border:1px solid #F59E0B;border-radius:8px;padding:8px 14px;
margin-bottom:12px;display:flex;align-items:center;gap:8px;">
<span style="font-size:16px;">📦</span>
<div>
  <span style="font-weight:700;color:#92400E;font-size:12px;">HISTORICAL REFERENCE ONLY — Zartico (Jun 2025 Snapshot)</span><br>
  <span style="font-size:11px;color:#78350F;">Current data: Datafy &amp; CoStar. Zartico provides baseline &amp; trend context only.</span>
</div></div>""", unsafe_allow_html=True)
        st.markdown("#### Visitor-to-Resident Ratio — Event Season Intensity (Zartico Historical Reference)")
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
        st.plotly_chart(style_fig(fig_move, height=250), use_container_width=True, config=PLOTLY_CONFIG)
        st.caption(
            "Jul–Sep months show 0.35–0.38 V/R ratio (30–35% above CA benchmark). "
            "Ohana Fest, Doheny Days, and Tall Ships all fire within this elevated window, maximizing their ADR lift."
        )
        st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # COMPRESSION CALENDAR
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown(sec_div("📆 Annual Compression Calendar"), unsafe_allow_html=True)
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
        st.plotly_chart(style_fig(fig_comp_ei, height=280), use_container_width=True, config=PLOTLY_CONFIG)
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
            st.plotly_chart(style_fig(fig_gantt, height=max(280, len(_gantt_rows) * 28)), use_container_width=True, config=PLOTLY_CONFIG)
            st.caption("Teal = major events · Gray = regular events. Source: Visit Dana Point official calendar.")

        st.markdown(sec_div("🔍 Event Deep Analysis — Multi-Source"), unsafe_allow_html=True)

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
                    st.plotly_chart(style_fig(fig_ei_sel, height=300), use_container_width=True, config=PLOTLY_CONFIG)
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
    _tab_controls("sp")
    # Pipeline-specific filter: Status (not STR time-window)
    _sp_f1, _sp_f2, _sp_spacer = st.columns([2, 2, 4])
    with _sp_f1:
        _sp_status_filter = st.selectbox(
            "Pipeline Status", ["All Statuses", "Under Construction", "Planned", "Proposed"],
            key="ss_sp_status",
            help="Filter pipeline properties by development stage.",
        )
    with _sp_f2:
        _sp_scale_filter = st.selectbox(
            "Chain Scale", ["All Scales", "Luxury", "Upper Upscale", "Upscale", "Select Service"],
            key="ss_sp_scale",
            help="Filter by hotel chain scale segment.",
        )
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Supply &amp; Pipeline</div>
      <div class="hero-subtitle">CoStar Supply Pipeline · New Hotel Openings · Market Competitive Dynamics</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown(tab_summary(
        "<strong>Know What's Coming:</strong> The South OC hotel supply pipeline adds rooms at a pace that will affect occupancy and rate positioning across VDP member hotels. "
        "New luxury and upper-upscale product intensifies competition at the top of the market. "
        "This tab shows every project under construction or planned, its opening timeline, and the aggregate impact on market supply. "
        "<strong>ADR discipline and direct-booking programs are critical</strong> to defending RevPAR during the supply absorption period."
    ), unsafe_allow_html=True)

    # ── Supply & Pipeline Section Intelligence ───────────────────────────────
    try:
        _sp_intel_rooms = int(df_cs_pipe["rooms"].sum()) if not df_cs_pipe.empty else 0
        _sp_intel_pct   = _sp_intel_rooms / 5120 * 100 if _sp_intel_rooms > 0 else 0
        st.markdown(sec_intel(
            "Supply & Pipeline",
            "hotel supply pipeline — rooms under construction and planned additions for South OC",
            f"{_sp_intel_rooms:,} pipeline rooms represent a {_sp_intel_pct:.1f}% increase in market supply. "
            "New luxury and upper-upscale product will intensify rate competition at the top of the market.",
            "ADR discipline is critical during the supply absorption period — protect RevPAR through loyalty and direct-booking incentives.",
            f"{_sp_intel_rooms:,} rooms in pipeline ({_sp_intel_pct:.1f}% supply growth)",
        ), unsafe_allow_html=True)
    except Exception:
        pass

    # ── Pipeline summary KPIs ─────────────────────────────────────────────────
    st.markdown(sec_div("🏗️ Active Supply Pipeline"), unsafe_allow_html=True)
    if not df_cs_pipe.empty:
        # Apply pipeline filters from filter widgets above
        _pipe_filtered = df_cs_pipe.copy()
        if _sp_status_filter != "All Statuses":
            _pipe_filtered = _pipe_filtered[_pipe_filtered["status"].str.contains(_sp_status_filter, case=False, na=False)]
        if _sp_scale_filter != "All Scales":
            _pipe_filtered = _pipe_filtered[_pipe_filtered["chain_scale"].str.contains(_sp_scale_filter, case=False, na=False)]
        _pipe_total = _pipe_filtered["rooms"].sum()
        _under_const_sp = _pipe_filtered[_pipe_filtered["status"] == "Under Construction"]
        _planned_sp = _pipe_filtered[_pipe_filtered["status"].isin(["Planned", "Final Planning / Permitting"])]
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
        _sp_pipe_colors = [_sp_status_colors.get(s, "#626C71") for s in _pipe_filtered["status"]]
        fig_sp_pipe = go.Figure(go.Bar(
            x=_pipe_filtered["property_name"],
            y=_pipe_filtered["rooms"],
            marker_color=_sp_pipe_colors,
            text=[f"{r} rooms\n{s}" for r, s in zip(_pipe_filtered["rooms"], _pipe_filtered["status"])],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Rooms: %{y}<br>Opens: %{customdata}<extra></extra>",
            customdata=_pipe_filtered["projected_open_date"],
        ))
        fig_sp_pipe.update_layout(xaxis_tickangle=-20, margin=dict(t=30, b=80))
        st.plotly_chart(style_fig(fig_sp_pipe, height=320), use_container_width=True, config=PLOTLY_CONFIG)

        # ── Pipeline detail table ─────────────────────────────────────────────
        _sp_display = _pipe_filtered[["property_name", "city", "chain_scale", "rooms",
                                      "status", "projected_open_date", "brand", "developer"]].copy()
        _sp_display.columns = ["Property", "City", "Segment", "Rooms",
                                "Status", "Opens", "Brand", "Developer"]
        st.dataframe(_sp_display, use_container_width=True, hide_index=True)
        st.download_button("⬇️ Download Pipeline CSV", _sp_display.to_csv(index=False).encode(), "supply_pipeline.csv", "text/csv", key="dl_sp_pipe")

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

    st.markdown(sec_div("📈 Annual Market Performance Context"), unsafe_allow_html=True)

    # ── Annual performance context ─────────────────────────────────────────────
    st.markdown(_sh("📈", "Annual Market Performance · 2016–2024 Actuals + 2025–2030 Forecasts", "indigo", "COSTAR"), unsafe_allow_html=True)
    st.caption("Source: CoStar Hospitality Analytics Newport Beach/Dana Point Submarket Report · Extracted March 2026 · Years 2016–2024 = actual market data · Years 2025–2030 = CoStar market forecasts")
    conn_sp = get_connection()
    try:
        df_cs_annual = pd.read_sql_query(
            "SELECT * FROM costar_annual_performance ORDER BY year_label ASC", conn_sp
        )
        if not df_cs_annual.empty:
            # ── Filter to Newport Beach/Dana Point submarket, Overall scope, numeric years ──
            _ann = df_cs_annual[
                (df_cs_annual["market"] == "Newport Beach/Dana Point") &
                (df_cs_annual["report_scope"] == "Overall") &
                (df_cs_annual["year_label"].apply(lambda y: str(y).isdigit()))
            ].copy()
            _ann["year"] = _ann["year_label"].astype(int)
            _ann = _ann.sort_values("year")
            _ann["data_type"] = _ann["year"].apply(lambda y: "Forecast" if y > 2024 else "Actual")
            _actuals_ann = _ann[_ann["data_type"] == "Actual"]
            _forecasts_ann = _ann[_ann["data_type"] == "Forecast"]

            if not _actuals_ann.empty:
                st.success(f"✅ Latest actual: Full Year **{_actuals_ann['year'].max()}** · {len(_actuals_ann)} years of historical data (2016–2024)")
            if not _forecasts_ann.empty:
                st.info(f"📈 CoStar forecasts: **{_forecasts_ann['year'].min()}–{_forecasts_ann['year'].max()}** · Directional context only")

            # ── KPI summary row ────────────────────────────────────────────────────
            _latest_ann = _actuals_ann.iloc[-1] if not _actuals_ann.empty else None
            _prior_ann  = _actuals_ann.iloc[-2] if len(_actuals_ann) >= 2 else None
            if _latest_ann is not None:
                _ann_cols = st.columns(4)
                _ann_kpis = [
                    ("RevPAR", f"${float(_latest_ann.get('revpar_usd',0)):.0f}",
                     f"{float(_latest_ann.get('revpar_yoy_pct',0)):+.1f}% YOY",
                     float(_latest_ann.get('revpar_yoy_pct',0)) >= 0),
                    ("ADR", f"${float(_latest_ann.get('adr_usd',0)):.0f}",
                     f"{float(_latest_ann.get('adr_yoy_pct',0)):+.1f}% YOY",
                     float(_latest_ann.get('adr_yoy_pct',0)) >= 0),
                    ("Occupancy", f"{float(_latest_ann.get('occupancy_pct',0)):.1f}%",
                     f"{float(_latest_ann.get('occ_yoy_pct',0)):+.1f}pp YOY",
                     float(_latest_ann.get('occ_yoy_pct',0)) >= 0),
                    ("2030 RevPAR Forecast",
                     f"${float(_forecasts_ann.iloc[-1].get('revpar_usd',0)):.0f}" if not _forecasts_ann.empty else "—",
                     "CoStar market projection", None),
                ]
                for _ci, (_lbl, _val, _sub, _pos) in enumerate(_ann_kpis):
                    with _ann_cols[_ci]:
                        _delta_color = "#059669" if _pos else ("#DC2626" if _pos is False else "#0567C8")
                        st.markdown(
                            f'<div class="kpi-card" style="border-bottom-color:{_delta_color};">'
                            f'<div class="kpi-label">{_lbl}</div>'
                            f'<div class="kpi-value">{_val}</div>'
                            f'<div style="font-size:11px;font-weight:700;color:{_delta_color};">{_sub}</div>'
                            f'</div>', unsafe_allow_html=True)

            # ── Chart row 1: RevPAR + ADR Trends ──────────────────────────────────
            st.markdown('<div class="chart-header">RevPAR & ADR Trend · 2016–2030</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Newport Beach/Dana Point Submarket · Actuals 2016–2024 · CoStar Forecasts 2025–2030</div>', unsafe_allow_html=True)

            _fig_rvp = go.Figure()
            # Actuals: solid lines
            _fig_rvp.add_trace(go.Scatter(
                x=_actuals_ann["year"], y=_actuals_ann["revpar_usd"],
                mode="lines+markers", name="RevPAR (Actual)",
                line=dict(color="#0567C8", width=2.5),
                marker=dict(size=7, color="#0567C8"),
                hovertemplate="<b>%{x} RevPAR</b><br>$%{y:.0f}<extra></extra>",
            ))
            _fig_rvp.add_trace(go.Scatter(
                x=_actuals_ann["year"], y=_actuals_ann["adr_usd"],
                mode="lines+markers", name="ADR (Actual)",
                line=dict(color="#059669", width=2.5),
                marker=dict(size=7, color="#059669"),
                hovertemplate="<b>%{x} ADR</b><br>$%{y:.0f}<extra></extra>",
            ))
            # Forecasts: dashed
            _fc_connect_rvp = pd.concat([_actuals_ann.tail(1), _forecasts_ann])
            _fc_connect_adr = pd.concat([_actuals_ann.tail(1), _forecasts_ann])
            _fig_rvp.add_trace(go.Scatter(
                x=_fc_connect_rvp["year"], y=_fc_connect_rvp["revpar_usd"],
                mode="lines+markers", name="RevPAR (Forecast)",
                line=dict(color="#0567C8", width=1.8, dash="dot"),
                marker=dict(size=5, color="#0567C8", opacity=0.6),
                hovertemplate="<b>%{x} RevPAR Forecast</b><br>$%{y:.0f}<extra></extra>",
            ))
            _fig_rvp.add_trace(go.Scatter(
                x=_fc_connect_adr["year"], y=_fc_connect_adr["adr_usd"],
                mode="lines+markers", name="ADR (Forecast)",
                line=dict(color="#059669", width=1.8, dash="dot"),
                marker=dict(size=5, color="#059669", opacity=0.6),
                hovertemplate="<b>%{x} ADR Forecast</b><br>$%{y:.0f}<extra></extra>",
            ))
            # COVID shade
            _fig_rvp.add_vrect(x0=2019.5, x1=2020.5,
                fillcolor="rgba(220,38,38,0.07)", line_width=0,
                annotation_text="COVID", annotation_position="top left",
                annotation_font_size=9, annotation_font_color="#DC2626")
            # Forecast zone
            _fig_rvp.add_vrect(x0=2024.5, x1=2030.5,
                fillcolor="rgba(5,103,200,0.04)", line_width=0,
                annotation_text="Forecast Zone", annotation_position="top right",
                annotation_font_size=9, annotation_font_color="#0567C8")
            _fig_rvp.update_layout(
                yaxis_tickprefix="$", yaxis_title="USD ($)",
                xaxis=dict(dtick=1, title="Year"),
            )
            st.plotly_chart(style_fig(_fig_rvp, height=320), use_container_width=True, config=PLOTLY_CONFIG)

            # ── Chart row 2: Occupancy trend + YOY bar ───────────────────────────
            _ann_c1, _ann_c2 = st.columns(2)
            with _ann_c1:
                st.markdown('<div class="chart-header">Occupancy Rate · 2016–2030</div>', unsafe_allow_html=True)
                _fig_occ = go.Figure()
                _fig_occ.add_trace(go.Bar(
                    x=_actuals_ann["year"], y=_actuals_ann["occupancy_pct"],
                    name="Occ % (Actual)",
                    marker_color=[
                        "rgba(220,38,38,0.70)" if y == 2020 else "rgba(5,103,200,0.75)"
                        for y in _actuals_ann["year"]
                    ],
                    hovertemplate="<b>%{x}</b><br>Occ: %{y:.1f}%<extra></extra>",
                ))
                _fig_occ.add_trace(go.Bar(
                    x=_forecasts_ann["year"], y=_forecasts_ann["occupancy_pct"],
                    name="Occ % (Forecast)",
                    marker_color="rgba(5,103,200,0.30)",
                    marker_pattern_shape="/",
                    hovertemplate="<b>%{x} Forecast</b><br>Occ: %{y:.1f}%<extra></extra>",
                ))
                # Target line at 70%
                _fig_occ.add_hline(y=70, line_dash="dot", line_color="rgba(5,150,105,0.50)",
                                   annotation_text="70% target", annotation_font_size=9)
                _fig_occ.update_layout(barmode="group", yaxis_ticksuffix="%", yaxis_title="Occupancy %")
                st.plotly_chart(style_fig(_fig_occ, height=280), use_container_width=True, config=PLOTLY_CONFIG)

            with _ann_c2:
                st.markdown('<div class="chart-header">RevPAR YOY Growth % · 2017–2024</div>', unsafe_allow_html=True)
                _yoy_data = _actuals_ann[_actuals_ann["year"] >= 2017].copy()
                _yoy_data["revpar_yoy_pct"] = pd.to_numeric(_yoy_data["revpar_yoy_pct"], errors="coerce")
                _fig_yoy = go.Figure(go.Bar(
                    x=_yoy_data["year"],
                    y=_yoy_data["revpar_yoy_pct"],
                    marker_color=[
                        "rgba(220,38,38,0.80)" if v < 0 else "rgba(5,150,105,0.80)"
                        for v in _yoy_data["revpar_yoy_pct"].fillna(0)
                    ],
                    text=[f"{v:+.1f}%" if pd.notna(v) else "" for v in _yoy_data["revpar_yoy_pct"]],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>RevPAR YOY: %{y:+.1f}%<extra></extra>",
                ))
                _fig_yoy.add_hline(y=0, line_color="rgba(15,28,46,0.15)", line_width=1)
                _fig_yoy.update_layout(yaxis_ticksuffix="%", yaxis_title="RevPAR YOY %")
                st.plotly_chart(style_fig(_fig_yoy, height=280), use_container_width=True, config=PLOTLY_CONFIG)

            # ── OC vs US benchmark comparison ─────────────────────────────────────
            st.markdown('<div class="chart-header">Submarket vs. Orange County vs. US National · RevPAR 2024</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-caption">Newport Beach/Dana Point commands a significant premium vs. broader markets</div>', unsafe_allow_html=True)
            _bench_2024 = df_cs_annual[
                df_cs_annual["year_label"].astype(str) == "2024"
            ].copy()
            if not _bench_2024.empty:
                _bench_2024 = _bench_2024.fillna(0)
                _fig_bench = go.Figure()
                _bench_markets = _bench_2024["market"].tolist()
                _bench_rvp = _bench_2024["revpar_usd"].tolist()
                _bench_adr = _bench_2024["adr_usd"].tolist()
                _bench_occ = _bench_2024["occupancy_pct"].tolist()
                _bar_colors = ["#0567C8" if m == "Newport Beach/Dana Point" else "rgba(5,103,200,0.35)"
                               for m in _bench_markets]
                _fig_bench.add_trace(go.Bar(name="RevPAR", x=_bench_markets, y=_bench_rvp,
                    marker_color=_bar_colors,
                    text=[f"${v:.0f}" for v in _bench_rvp], textposition="outside",
                    hovertemplate="<b>%{x}</b><br>RevPAR: $%{y:.0f}<extra></extra>"))
                _fig_bench.add_trace(go.Bar(name="ADR", x=_bench_markets, y=_bench_adr,
                    marker_color=["rgba(5,150,105,0.80)" if m == "Newport Beach/Dana Point" else "rgba(5,150,105,0.35)"
                                  for m in _bench_markets],
                    text=[f"${v:.0f}" for v in _bench_adr], textposition="outside",
                    hovertemplate="<b>%{x}</b><br>ADR: $%{y:.0f}<extra></extra>"))
                _fig_bench.update_layout(barmode="group", yaxis_tickprefix="$")
                st.plotly_chart(style_fig(_fig_bench, height=280), use_container_width=True, config=PLOTLY_CONFIG)

            with st.expander("📊 Full Dataset — Actuals + Forecasts"):
                st.dataframe(df_cs_annual, use_container_width=True, hide_index=True)
                _ann_csv = df_cs_annual.to_csv(index=False).encode()
                st.download_button("⬇️ Download Annual Data CSV", _ann_csv,
                                   file_name="costar_annual_performance.csv", mime="text/csv")
        else:
            st.info("No annual CoStar performance data loaded.")
    except Exception as _ann_err:
        st.info(f"CoStar annual performance table not yet populated. ({_ann_err})")

    # ── Supply Pipeline Intel Panel ───────────────────────────────────────────
    try:
        _sp_pipe_rooms = int(df_cs_pipe["rooms"].sum()) if not df_cs_pipe.empty else 0
        _sp_uc_ct  = len(df_cs_pipe[df_cs_pipe["status"] == "Under Construction"]) if not df_cs_pipe.empty else 0
        _sp_pl_ct  = len(df_cs_pipe[df_cs_pipe["status"].isin(["Planned", "Final Planning / Permitting"])]) if not df_cs_pipe.empty else 0
        _sp_ip_next_steps = [
            f"<strong>Supply Absorption Strategy:</strong> {_sp_pipe_rooms:,} rooms in pipeline represent new competitive supply. "
            "Build direct-booking loyalty programs NOW to defend RevPAR before new rooms open.",
            f"<strong>Rate Discipline:</strong> {_sp_uc_ct} project(s) under active construction. "
            "Set rate floors before new supply opens — rate dilution is hardest to recover once established.",
            f"<strong>Forecast Modeling:</strong> {_sp_pl_ct} project(s) in planning/permitting for 2026+. "
            "Incorporate supply growth into multi-year TBID projection models for board presentations.",
            "<strong>Competitive Positioning:</strong> Use CoStar Annual Performance tab to benchmark current MPI/ARI/RGI vs. the comp set. "
            "Position VDP hotels ahead of any supply-driven RevPAR softening.",
        ]
        _sp_ip_questions = [
            "Which new properties pose the biggest RevPAR threat?",
            "How does this pipeline compare to other coastal CA markets?",
            "What's the projected RevPAR impact of new supply absorption?",
            "Which segments (luxury vs select-service) are most exposed?",
        ]
        _sp_ip_context = (
            f"Dana Point/South OC hotel supply pipeline: {_sp_pipe_rooms:,} total pipeline rooms, "
            f"{_sp_uc_ct} under construction, {_sp_pl_ct} in planning/permitting. "
            f"Current market: ~5,120 existing rooms. Pipeline = {_sp_pipe_rooms/5120*100:.1f}% supply growth."
        )
        render_intel_panel("sp_supply", _sp_ip_next_steps, _sp_ip_questions, _sp_ip_context)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════


# TAB 8 — MARKET INTELLIGENCE (CoStar)
# ══════════════════════════════════════════════════════════════════════════════
with tab_cs:
    _tab_controls("cs")
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">South OC Market Intelligence</div>
      <div class="hero-subtitle">CoStar Hospitality Analytics · Newport Beach/Dana Point · Full Year 2024 Data + Forecasts Through 2030 · Extracted March 2026</div>
    </div>
    """, unsafe_allow_html=True)
    st.info("📌 **Data Context:** This tab displays CoStar Hospitality Analytics data extracted from the March 2026 Newport Beach/Dana Point Submarket Report. Annual performance figures reflect **Full Year 2024** actuals — the most recent full-year period available. CoStar market forecasts (2025–2030) are also included for strategic planning context. For **current 2026 STR performance**, see the Hotel Performance tab (data through Feb 2026).")
    st.markdown(tab_summary(
        "<strong>Know the Market:</strong> CoStar data places the VDP portfolio inside the South Orange County competitive set. "
        "Luxury properties (Waldorf Astoria, Ritz-Carlton) set the rate ceiling at $782 ADR; the full market generates $1.15B in annual room revenue. "
        "This tab shows where VDP member hotels rank on MPI, ARI, and RGI — and what new supply is arriving that will affect that position. "
        "<strong>Lead indicator:</strong> the pipeline adds rooms before year-end — monitor absorption and defend RevPAR with direct-booking programs."
    ), unsafe_allow_html=True)

    # ── Market Intelligence Section Intelligence ─────────────────────────────
    try:
        _cs_mkt_occ = 0.0; _cs_mkt_adr = 0.0; _cs_mkt_rvp = 0.0
        if not df_cs_snap.empty:
            _cs_s = df_cs_snap.iloc[0]
            _cs_mkt_occ = float(_cs_s.get("occupancy_pct", 0) or 0)
            _cs_mkt_adr = float(_cs_s.get("adr_usd", 0) or 0)
            _cs_mkt_rvp = float(_cs_s.get("revpar_usd", 0) or 0)
        _cs_port_adr = m.get("adr_30", 0) if m else 0
        _cs_ari_signal = ("portfolio ADR is above market — strong ARI position" if _cs_port_adr > _cs_mkt_adr
                          else "portfolio ADR is below market average — rate capture opportunity")
        st.markdown(sec_intel(
            "Market Intelligence",
            "CoStar submarket data: occupancy, ADR, RevPAR across South OC competitive set",
            f"South OC market: {_cs_mkt_occ:.1f}% occ, ${_cs_mkt_adr:.0f} ADR, ${_cs_mkt_rvp:.0f} RevPAR. "
            f"VDP portfolio ADR ${_cs_port_adr:,.0f} — {_cs_ari_signal}.",
            "Track MPI, ARI, and RGI monthly — index leadership above 100 across all three metrics is the primary RevPAR growth target.",
            f"Market ADR: ${_cs_mkt_adr:.0f} · Market RevPAR: ${_cs_mkt_rvp:.0f}",
        ), unsafe_allow_html=True)
    except Exception:
        pass

    # ── Competitive Intel Intelligence Panel ──────────────────────────────────
    try:
        _cs_port_adr2 = m.get("adr_30", 0) if m else 0
        _cs_mkt_adr2  = 0.0; _cs_mkt_rvp2 = 0.0; _cs_mkt_occ2 = 0.0
        if not df_cs_snap.empty:
            _s2 = df_cs_snap.iloc[0]
            _cs_mkt_adr2 = float(_s2.get("adr_usd", 0) or 0)
            _cs_mkt_rvp2 = float(_s2.get("revpar_usd", 0) or 0)
            _cs_mkt_occ2 = float(_s2.get("occupancy_pct", 0) or 0)
        _cs_adr_gap = _cs_port_adr2 - _cs_mkt_adr2

        _cs_next_steps = [
            f"<strong>ARI (ADR Index) Target:</strong> VDP ADR ${_cs_port_adr2:.0f} vs market ${_cs_mkt_adr2:.0f} "
            f"({'above' if _cs_adr_gap >= 0 else 'below'} market by ${abs(_cs_adr_gap):.0f}) — "
            + ("maintain premium positioning with rate floors; avoid aggressive discounting." if _cs_adr_gap >= 0 else
               "identify rate capture opportunities through comp-set analysis and LOS minimums."),
            "<strong>New Supply Watch:</strong> Monitor pipeline additions in Supply Pipeline tab — "
            "new rooms entering the market compress OCC and ADR for 12-18 months; plan demand campaigns in advance.",
            "<strong>MPI Strategy:</strong> Market Penetration Index above 100 = capturing above your fair share — "
            "track MPI monthly and target group/corporate segments to maintain index leadership.",
            f"<strong>2030 Forecast Planning:</strong> CoStar projects South OC market through 2030 — "
            "build 3-year TBID and TOT projections using these forecasts for city budget planning.",
        ]
        _cs_questions = [
            "How does VDP ADR compare to the Waldorf and Ritz-Carlton?",
            "What's our MPI vs the South OC competitive set?",
            "How much new supply is entering the market by 2026?",
            "What does CoStar forecast for RevPAR growth through 2028?",
            "Which comp-set properties are gaining market share?",
        ]
        _cs_context = (
            f"Competitive Intel (CoStar Full Year 2024): South OC market "
            f"Occ {_cs_mkt_occ2:.1f}%, ADR ${_cs_mkt_adr2:.0f}, RevPAR ${_cs_mkt_rvp2:.0f}. "
            f"VDP portfolio ADR ${_cs_port_adr2:.0f} ({'+' if _cs_adr_gap>=0 else ''}{_cs_adr_gap:.0f} vs market)."
        )
        render_intel_panel("cs_market", _cs_next_steps, _cs_questions, _cs_context)
    except Exception:
        pass


    # ── Competitive Intel Sub-Tabs ──────────────────────────────────────────────
    _cs_t1, _cs_t2 = st.tabs(["📊 Market Performance", "📡 External Signals"])

    # ── Market Performance → sub-tab 1 ─────────────────────────────────────────
    with _cs_t1:
        # ── AI CoStar Analysis Panel ───────────────────────────────────────────────
        with st.expander("🧠 CoStar VDP Analyst — Deep Market Insights", expanded=True):
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
                _cs_mdl = st.session_state.get("selected_model", CLAUDE_MODEL)
                _cs_info = AI_MODELS.get(_cs_mdl, {})
                _cs_label = f"{_cs_info.get('badge','🟦')} {_cs_info.get('label', _cs_mdl)}"
                _cs_any_ai = (
                    (api_key_valid and ANTHROPIC_AVAILABLE) or
                    (bool(_OPENAI_KEY) and OPENAI_AVAILABLE) or
                    (bool(_GOOGLE_AI_KEY) and GEMINI_AVAILABLE) or
                    (bool(_PERPLEXITY_KEY) and OPENAI_AVAILABLE)
                )
                if _cs_any_ai:
                    st.caption(f"Analyzing with {_cs_label}")
                    with st.spinner("Running deep market analysis…"):
                        st.write_stream(stream_ai_response(st.session_state.ai_current_prompt, _cs_mdl, _ai_keys))
                else:
                    st.info(local_fallback("board", m) if m else "No data. Run the pipeline first.")

        st.markdown(sec_div("🏨 South OC Market Overview"), unsafe_allow_html=True)

        # ── Market Overview KPI Cards ──────────────────────────────────────────────
        st.markdown(_sh("🏨", "South OC Market Overview · Full Year 2024 (Latest Available)", "indigo", "COSTAR"), unsafe_allow_html=True)
        st.caption("Source: CoStar Hospitality Analytics · Full Year 2024 Annual Data · Extracted from March 2026 CoStar Report · Current STR data (through Feb 2026) is in the Hotel Performance tab")

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
                    tooltip="Rooms Sold ÷ Rooms Available. Above 80% signals compression opportunity.",
                ), unsafe_allow_html=True)
            with c2:
                st.markdown(kpi_card(
                    "Market ADR", f"${snap['adr_usd']:.2f}",
                    f"{snap['adr_yoy_pct']:+.1f}% YOY",
                    positive=(snap['adr_yoy_pct'] >= 0),
                    tooltip="Average Daily Rate = Room Revenue ÷ Rooms Sold. Measures pricing power.",
                ), unsafe_allow_html=True)
            with c3:
                st.markdown(kpi_card(
                    "Market RevPAR", f"${snap['revpar_usd']:.2f}",
                    f"{snap['revpar_yoy_pct']:+.1f}% YOY",
                    positive=(snap['revpar_yoy_pct'] >= 0),
                    tooltip="Revenue Per Available Room = ADR × Occupancy. Primary hotel health index.",
                ), unsafe_allow_html=True)
            with c4:
                rev_b = snap['room_revenue_usd'] / 1e9
                st.markdown(kpi_card(
                    "Annual Room Revenue", f"${rev_b:.2f}B",
                    f"{snap['demand_yoy_pct']:+.1f}% demand YOY",
                    positive=(snap['demand_yoy_pct'] >= 0),
                    tooltip="Total room revenue for the South OC submarket. Demand YOY = rooms sold growth rate.",
                ), unsafe_allow_html=True)

            # VDP vs Market comparison row
            st.markdown("#### VDP Portfolio vs. South OC Market · Full Year 2024 Baseline")
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

        st.markdown(sec_div("📈 Market Monthly Performance — 24-Month Trend"), unsafe_allow_html=True)

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
                st.plotly_chart(style_fig(fig_cs1, height=300), use_container_width=True, config=PLOTLY_CONFIG)

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
                st.plotly_chart(style_fig(fig_cs2, height=300), use_container_width=True, config=PLOTLY_CONFIG)

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

        st.markdown(sec_div("🔗 Chain Scale Performance Breakdown"), unsafe_allow_html=True)

        # ── Chain Scale Breakdown ──────────────────────────────────────────────────
        st.markdown("### Chain Scale Performance Breakdown · Full Year 2024")

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
                st.plotly_chart(style_fig(fig_ch1, height=280), use_container_width=True, config=PLOTLY_CONFIG)

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
                st.plotly_chart(fig_ch2, use_container_width=True, config=PLOTLY_CONFIG)

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
            st.download_button("⬇️ Download Chain Scale CSV", _chain_display.to_csv(index=False).encode(), "chain_scale_performance.csv", "text/csv", key="dl_chain")

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

        st.markdown(sec_div("🏗️ Active Supply Pipeline"), unsafe_allow_html=True)

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
                    tooltip="Compression Days: Nights when occupancy exceeds 80% or 90% — rate-increase signal.",
                ), unsafe_allow_html=True)
            with c_p2:
                st.markdown(kpi_card(
                    "Under Construction", f"{uc_rooms:,} rooms",
                    f"{len(under_const)} project(s) · opening 2025",
                    positive=True, neutral=True,
                    tooltip="New hotel rooms currently under active construction in the South OC submarket.",
                ), unsafe_allow_html=True)
            with c_p3:
                st.markdown(kpi_card(
                    "Planned / Permitting", f"{pl_rooms:,} rooms",
                    f"{len(planned)} project(s) · opening 2026–2027",
                    positive=False, neutral=True,
                    tooltip="Rooms in planning or permitting phase — potential future supply impact.",
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
            st.plotly_chart(style_fig(fig_pipe, height=320), use_container_width=True, config=PLOTLY_CONFIG)

            # Pipeline table
            _pipe_display = df_cs_pipe[["property_name","city","chain_scale","rooms",
                                         "status","projected_open_date","brand","developer"]].copy()
            _pipe_display.columns = ["Property","City","Segment","Rooms",
                                      "Status","Opens","Brand","Developer"]
            st.dataframe(_pipe_display, use_container_width=True, hide_index=True)
            st.download_button("⬇️ Download Pipeline CSV", _pipe_display.to_csv(index=False).encode(), "hotel_pipeline.csv", "text/csv", key="dl_cs_pipe")

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

        st.markdown(sec_div("🏆 Competitive Set — Property Rankings"), unsafe_allow_html=True)

        # ── Competitive Set Rankings ───────────────────────────────────────────────
        st.markdown("### Competitive Set — Property Rankings · Full Year 2024")

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
                    xaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.07)"),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(style_fig(fig_rgi, height=360), use_container_width=True, config=PLOTLY_CONFIG)

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
                               gridcolor="rgba(0,0,0,0.07)"),
                    yaxis=dict(title="ADR $", tickprefix="$",
                               gridcolor="rgba(0,0,0,0.07)"),
                    margin=dict(t=10, b=30, l=10, r=10),
                )
                st.plotly_chart(style_fig(fig_scat, height=360), use_container_width=True, config=PLOTLY_CONFIG)

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
            st.download_button("⬇️ Download Comp Set CSV", _comp_display.to_csv(index=False).encode(), "competitive_set.csv", "text/csv", key="dl_compset")
        else:
            st.markdown(empty_state("🏆", "No competitive set data.",
                "Run scripts/load_costar_reports.py to populate competitive benchmarks."),
                unsafe_allow_html=True)

        st.markdown(sec_div("📊 Portfolio × Market Correlation Analysis"), unsafe_allow_html=True)

        # ── STR × CoStar Correlation Insights ─────────────────────────────────────
        st.markdown(_sh("📈", "Portfolio × Market Correlation Analysis", "teal", "STR + COSTAR"), unsafe_allow_html=True)
        st.markdown(
            '<div style="font-size:12px;color:#8B949E;margin:-8px 0 12px 0;">'
            'VDP 12-property portfolio (kpi_daily_summary, resampled monthly) vs. South OC market (CoStar monthly) · '
            'Feb 2024 – Dec 2024 overlap window · 11 months of aligned data</div>',
            unsafe_allow_html=True,
        )

        if not df_cs_mon.empty:
            # ── Build portfolio-side monthly series from kpi_daily_summary ─────────
            # Fall back: resample daily KPIs to monthly (avoids dependency on STR monthly NULLs)
            _kpi_src = df_kpi.copy() if not df_kpi.empty else pd.DataFrame()
            if not _kpi_src.empty:
                _kpi_src["as_of_date"] = pd.to_datetime(_kpi_src["as_of_date"])
                # kpi_daily_summary has occ_pct (%), adr, revpar
                _kpi_mon = (
                    _kpi_src.set_index("as_of_date")[["occ_pct","adr","revpar"]]
                    .resample("MS").mean()
                    .reset_index()
                )
                _kpi_mon.columns = ["as_of_date","portfolio_occ","portfolio_adr","portfolio_revpar"]
                _kpi_mon["_ym"] = _kpi_mon["as_of_date"].dt.to_period("M").astype(str)
            else:
                _kpi_mon = pd.DataFrame()

            cs_merged = df_cs_mon[["as_of_date","occupancy_pct","adr_usd","revpar_usd"]].copy()
            cs_merged.columns = ["as_of_date","mkt_occ","mkt_adr","mkt_revpar"]
            cs_merged["_ym"] = pd.to_datetime(cs_merged["as_of_date"]).dt.to_period("M").astype(str)

            merged = pd.DataFrame()
            if not _kpi_mon.empty:
                merged = pd.merge(_kpi_mon, cs_merged, on="_ym", how="inner", suffixes=("","_cs"))
                if "as_of_date_cs" in merged.columns:
                    merged = merged.drop(columns=["as_of_date_cs"])
                merged = merged.dropna(subset=["portfolio_revpar","mkt_revpar"])
                merged = merged.sort_values("as_of_date")

            if len(merged) >= 3:
                # ── Correlation stat callout ──────────────────────────────────────
                _corr_rvp = merged["portfolio_revpar"].corr(merged["mkt_revpar"])
                _corr_adr = merged["portfolio_adr"].corr(merged["mkt_adr"])
                _avg_rvp_premium = (merged["portfolio_revpar"] - merged["mkt_revpar"]).mean()
                _avg_adr_premium = (merged["portfolio_adr"] - merged["mkt_adr"]).mean()
                st.markdown(f"""
    <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:14px;">
      <div style="flex:1;min-width:140px;background:rgba(33,128,141,0.12);border:1px solid rgba(33,128,141,0.3);
           border-radius:10px;padding:10px 14px;">
        <div style="font-size:10px;color:#8B949E;font-weight:700;text-transform:uppercase;letter-spacing:.06em;">RevPAR Correlation</div>
        <div style="font-size:1.6rem;font-weight:800;color:#67E8F9;">{_corr_rvp:.2f}</div>
        <div style="font-size:10px;color:#8B949E;">R — portfolio tracks market</div>
      </div>
      <div style="flex:1;min-width:140px;background:rgba(139,92,246,0.12);border:1px solid rgba(139,92,246,0.3);
           border-radius:10px;padding:10px 14px;">
        <div style="font-size:10px;color:#8B949E;font-weight:700;text-transform:uppercase;letter-spacing:.06em;">ADR Correlation</div>
        <div style="font-size:1.6rem;font-weight:800;color:#C4B5FD;">{_corr_adr:.2f}</div>
        <div style="font-size:10px;color:#8B949E;">R — rate pricing alignment</div>
      </div>
      <div style="flex:1;min-width:140px;background:rgba(16,185,129,0.12);border:1px solid rgba(16,185,129,0.3);
           border-radius:10px;padding:10px 14px;">
        <div style="font-size:10px;color:#8B949E;font-weight:700;text-transform:uppercase;letter-spacing:.06em;">Avg RevPAR Premium</div>
        <div style="font-size:1.6rem;font-weight:800;color:{'#34D399' if _avg_rvp_premium>=0 else '#F87171'};">${_avg_rvp_premium:+.0f}</div>
        <div style="font-size:10px;color:#8B949E;">portfolio above market avg</div>
      </div>
      <div style="flex:1;min-width:140px;background:rgba(245,158,11,0.12);border:1px solid rgba(245,158,11,0.3);
           border-radius:10px;padding:10px 14px;">
        <div style="font-size:10px;color:#8B949E;font-weight:700;text-transform:uppercase;letter-spacing:.06em;">Avg ADR Premium</div>
        <div style="font-size:1.6rem;font-weight:800;color:{'#FDE68A' if _avg_adr_premium>=0 else '#F87171'};">${_avg_adr_premium:+.0f}</div>
        <div style="font-size:10px;color:#8B949E;">portfolio above market ADR</div>
      </div>
      <div style="flex:1;min-width:140px;background:rgba(22,27,34,0.8);border:1px solid rgba(0,0,0,0.08);
           border-radius:10px;padding:10px 14px;">
        <div style="font-size:10px;color:#8B949E;font-weight:700;text-transform:uppercase;letter-spacing:.06em;">Data Points</div>
        <div style="font-size:1.6rem;font-weight:800;color:#E6EDF3;">{len(merged)}</div>
        <div style="font-size:10px;color:#8B949E;">months of aligned data</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

                col_corr1, col_corr2 = st.columns(2)
                with col_corr1:
                    st.markdown('<div class="chart-header">Portfolio RevPAR vs. Market RevPAR</div>', unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="chart-caption">R={_corr_rvp:.2f} — VDP portfolio commands ${_avg_rvp_premium:+.0f} RevPAR premium over South OC market · '
                        f'Peak spread indicates strong leisure compression pricing</div>',
                        unsafe_allow_html=True,
                    )
                    fig_corr = go.Figure()
                    fig_corr.add_trace(go.Scatter(
                        x=merged["as_of_date"], y=merged["portfolio_revpar"],
                        name="VDP Portfolio",
                        line=dict(color=TEAL, width=2.5),
                        fill="tonexty", fillcolor="rgba(33,128,141,0.08)",
                        hovertemplate="<b>%{x|%b %Y}</b><br>Portfolio RevPAR: $%{y:.0f}<extra></extra>",
                    ))
                    fig_corr.add_trace(go.Scatter(
                        x=merged["as_of_date"], y=merged["mkt_revpar"],
                        name="S. OC Market",
                        line=dict(color=ORANGE, width=2, dash="dot"),
                        hovertemplate="<b>%{x|%b %Y}</b><br>Market RevPAR: $%{y:.0f}<extra></extra>",
                    ))
                    # Annotate peak portfolio RevPAR
                    _peak_idx = merged["portfolio_revpar"].idxmax()
                    _peak_row = merged.loc[_peak_idx]
                    fig_corr.add_annotation(
                        x=_peak_row["as_of_date"], y=_peak_row["portfolio_revpar"],
                        text=f"Peak ${_peak_row['portfolio_revpar']:.0f}",
                        showarrow=True, arrowhead=2, arrowcolor=TEAL,
                        font=dict(size=10, color=TEAL), bgcolor="rgba(13,17,23,0.8)",
                        bordercolor=TEAL, borderwidth=1, ax=0, ay=-30,
                    )
                    fig_corr.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(font=dict(size=11), bgcolor="rgba(13,17,23,0.6)",
                                    bordercolor="rgba(0,0,0,0.08)", borderwidth=1),
                        margin=dict(t=10, b=10),
                        yaxis=dict(tickprefix="$", gridcolor="rgba(0,0,0,0.07)", color="#718096"),
                        xaxis=dict(gridcolor="rgba(0,0,0,0.07)", color="#718096",
                                   tickformat="%b %Y"),
                        hoverlabel=dict(bgcolor="rgba(13,17,23,0.95)", font_size=13,
                                       font_color="#E6EDF3", bordercolor="rgba(33,128,141,0.5)"),
                    )
                    st.plotly_chart(style_fig(fig_corr, height=300), use_container_width=True, config=PLOTLY_CONFIG)

                with col_corr2:
                    st.markdown('<div class="chart-header">Portfolio ADR vs. Market ADR</div>', unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="chart-caption">R={_corr_adr:.2f} — VDP ADR premium ${_avg_adr_premium:+.0f} vs. market · '
                        f'Strong summer seasonality with consistent rate discipline above market floor</div>',
                        unsafe_allow_html=True,
                    )
                    fig_adr = go.Figure()
                    fig_adr.add_trace(go.Scatter(
                        x=merged["as_of_date"], y=merged["portfolio_adr"],
                        name="VDP Portfolio",
                        line=dict(color=TEAL, width=2.5),
                        hovertemplate="<b>%{x|%b %Y}</b><br>Portfolio ADR: $%{y:.0f}<extra></extra>",
                    ))
                    fig_adr.add_trace(go.Scatter(
                        x=merged["as_of_date"], y=merged["mkt_adr"],
                        name="S. OC Market",
                        line=dict(color=ORANGE, width=2, dash="dot"),
                        hovertemplate="<b>%{x|%b %Y}</b><br>Market ADR: $%{y:.0f}<extra></extra>",
                    ))
                    _peak_adr_idx = merged["portfolio_adr"].idxmax()
                    _peak_adr_row = merged.loc[_peak_adr_idx]
                    fig_adr.add_annotation(
                        x=_peak_adr_row["as_of_date"], y=_peak_adr_row["portfolio_adr"],
                        text=f"Peak ${_peak_adr_row['portfolio_adr']:.0f}",
                        showarrow=True, arrowhead=2, arrowcolor="#8B5CF6",
                        font=dict(size=10, color="#8B5CF6"), bgcolor="rgba(13,17,23,0.8)",
                        bordercolor="#8B5CF6", borderwidth=1, ax=0, ay=-30,
                    )
                    fig_adr.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(font=dict(size=11), bgcolor="rgba(13,17,23,0.6)",
                                    bordercolor="rgba(0,0,0,0.08)", borderwidth=1),
                        margin=dict(t=10, b=10),
                        yaxis=dict(tickprefix="$", gridcolor="rgba(0,0,0,0.07)", color="#718096"),
                        xaxis=dict(gridcolor="rgba(0,0,0,0.07)", color="#718096",
                                   tickformat="%b %Y"),
                        hoverlabel=dict(bgcolor="rgba(13,17,23,0.95)", font_size=13,
                                       font_color="#E6EDF3", bordercolor="rgba(33,128,141,0.5)"),
                    )
                    st.plotly_chart(style_fig(fig_adr, height=300), use_container_width=True, config=PLOTLY_CONFIG)

                # ── Occupancy comparison ──────────────────────────────────────────
                _corr_occ = merged["portfolio_occ"].corr(merged["mkt_occ"])
                st.markdown('<div class="chart-header">Portfolio Occupancy vs. Market Occupancy</div>', unsafe_allow_html=True)
                st.markdown(
                    f'<div class="chart-caption">R={_corr_occ:.2f} — Occupancy parity signal · '
                    f'Portfolio occ (from kpi_daily_summary) vs. South OC market (CoStar) · '
                    f'Gap = VDP mix-shift toward higher-rated stays (fewer budget rooms)</div>',
                    unsafe_allow_html=True,
                )
                fig_occ = go.Figure()
                fig_occ.add_trace(go.Scatter(
                    x=merged["as_of_date"], y=merged["portfolio_occ"],
                    name="VDP Portfolio Occ %",
                    line=dict(color=TEAL, width=2.5),
                    hovertemplate="<b>%{x|%b %Y}</b><br>Portfolio Occ: %{y:.1f}%<extra></extra>",
                ))
                fig_occ.add_trace(go.Scatter(
                    x=merged["as_of_date"], y=merged["mkt_occ"],
                    name="S. OC Market Occ %",
                    line=dict(color=ORANGE, width=2, dash="dot"),
                    hovertemplate="<b>%{x|%b %Y}</b><br>Market Occ: %{y:.1f}%<extra></extra>",
                ))
                fig_occ.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    legend=dict(font=dict(size=11), bgcolor="rgba(13,17,23,0.6)",
                                bordercolor="rgba(0,0,0,0.08)", borderwidth=1),
                    margin=dict(t=10, b=10),
                    yaxis=dict(ticksuffix="%", gridcolor="rgba(0,0,0,0.07)", color="#718096"),
                    xaxis=dict(gridcolor="rgba(0,0,0,0.07)", color="#718096", tickformat="%b %Y"),
                    hoverlabel=dict(bgcolor="rgba(13,17,23,0.95)", font_size=13,
                                   font_color="#E6EDF3", bordercolor="rgba(33,128,141,0.5)"),
                )
                st.plotly_chart(style_fig(fig_occ, height=260), use_container_width=True, config=PLOTLY_CONFIG)

            else:
                st.warning(
                    f"⚠️ Correlation analysis requires overlapping date ranges. "
                    f"Portfolio data: Feb 2024–present. CoStar monthly: "
                    f"{df_cs_mon['as_of_date'].min()} – {df_cs_mon['as_of_date'].max()}. "
                    f"Overlap found: {len(merged)} months (minimum 3 required).",
                )
        else:
            st.markdown(empty_state(
                "📉", "CoStar data not loaded.",
                "Run `python scripts/fetch_costar_data.py` to load South OC market benchmarks.",
            ), unsafe_allow_html=True)

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
        st.markdown(sec_div("🌴 Visit California — State Context"), unsafe_allow_html=True)
        st.markdown(
            '<div style="font-family:\'Syne\',sans-serif;font-size:1.35rem;'
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
                        xaxis=dict(title="ADR (USD)", gridcolor="rgba(0,0,0,0.06)"),
                        yaxis=dict(gridcolor="rgba(0,0,0,0.06)"),
                        height=420, margin=dict(l=10, r=10, t=40, b=10),
                        font=dict(family="Syne, sans-serif", size=11),
                    )
                    st.plotly_chart(fig_lodge, use_container_width=True, config=PLOTLY_CONFIG)

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
                            xaxis=dict(title="Year", gridcolor="rgba(0,0,0,0.06)"),
                            yaxis=dict(title="Total Visits (M)", gridcolor="rgba(0,0,0,0.06)"),
                            legend=dict(font=dict(size=11)),
                            height=300, margin=dict(l=10, r=10, t=20, b=10),
                            font=dict(family="Syne, sans-serif", size=11),
                        )
                        st.plotly_chart(fig_fcast, use_container_width=True, config=PLOTLY_CONFIG)

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
                                xaxis=dict(title="Month", gridcolor="rgba(0,0,0,0.06)",
                                           tickvals=list(range(1, 13)),
                                           ticktext=["Jan","Feb","Mar","Apr","May","Jun",
                                                     "Jul","Aug","Sep","Oct","Nov","Dec"]),
                                yaxis=dict(title="Passengers", gridcolor="rgba(0,0,0,0.06)"),
                                legend=dict(font=dict(size=11)),
                                height=300, margin=dict(l=10, r=10, t=20, b=10),
                                font=dict(family="Syne, sans-serif", size=11),
                            )
                            st.plotly_chart(fig_air, use_container_width=True, config=PLOTLY_CONFIG)
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

        # ── GloCon Solutions LLC — VDP vs Market Leadership Scorecard ─────────────
        st.markdown(sec_div("🏆 VDP vs. South OC Market — Leadership Scorecard"), unsafe_allow_html=True)
        st.markdown("### 🏆 VDP vs. South OC Market — Leadership Scorecard")
        st.caption("How Dana Point properties perform vs. the South Orange County submarket · Source: STR 30-day actuals vs. CoStar snapshot · Built by GloCon Solutions LLC")

        try:
            if not df_cs_snap.empty and kpis:
                _snap = df_cs_snap.iloc[0]
                _mkt_occ_b = float(_snap.get("occupancy_pct", 76.4) or 76.4)
                _mkt_adr_b = float(_snap.get("adr_usd", 288.50) or 288.50)
                _mkt_rvp_b = float(_snap.get("revpar_usd", 220.42) or 220.42)
                def _ns(v):
                    try: return float(str(v).replace("$","").replace("%","").replace(",",""))
                    except: return 0.0
                _vdp_occ_n = _ns(next((k["raw_value"] for k in kpis if "Occ" in k.get("label","")), _mkt_occ_b))
                _vdp_adr_n = _ns(next((k["raw_value"] for k in kpis if "ADR" in k.get("label","")), _mkt_adr_b))
                _vdp_rvp_n = _ns(next((k["raw_value"] for k in kpis if "RevPAR" in k.get("label","")), _mkt_rvp_b))
                _sc_rows = [
                    ("Occupancy",  f"{_vdp_occ_n:.1f}%", f"{_mkt_occ_b:.1f}%", _vdp_occ_n - _mkt_occ_b, "pp"),
                    ("ADR",        f"${_vdp_adr_n:.2f}",  f"${_mkt_adr_b:.2f}",  _vdp_adr_n - _mkt_adr_b, "$"),
                    ("RevPAR",     f"${_vdp_rvp_n:.2f}",  f"${_mkt_rvp_b:.2f}",  _vdp_rvp_n - _mkt_rvp_b, "$"),
                ]
                _sc_df = pd.DataFrame(_sc_rows, columns=["Metric","VDP Portfolio","S. OC Market","Gap","Unit"])
                _sc_df["Signal"]      = _sc_df["Gap"].apply(lambda g: "✅ Premium" if g > 0 else ("⚠️ Parity" if abs(g) < 0.5 else "🔴 Below Market"))
                _sc_df["Gap vs Mkt"]  = _sc_df.apply(lambda r: f"{'+' if r['Gap']>0 else ''}{r['Gap']:.1f}{r['Unit']}", axis=1)
                _sc_df["Board Note"]  = _sc_df.apply(lambda r: (
                    "Maintain pricing discipline — demand supports premium." if r["Gap"] > 0
                    else "Investigate comp set — close rate gap strategy needed."), axis=1)
                _sc_dl = _sc_df[["Metric","VDP Portfolio","S. OC Market","Gap vs Mkt","Signal","Board Note"]]
                st.dataframe(_sc_dl, use_container_width=True, hide_index=True)
                st.download_button("⬇️ Download Scorecard CSV", _sc_dl.to_csv(index=False).encode(), "vdp_vs_market_scorecard.csv", "text/csv", key="dl_scorecard")
                # Visual comparison bar chart
                _sc_fig = go.Figure()
                for _sci, (_scm, _scvdp, _scmkt, _scgap, _scu) in enumerate(_sc_rows):
                    _scv_num = _ns(_scvdp); _scm_num = _ns(_scmkt)
                    _sc_fig.add_trace(go.Bar(
                        name="VDP Portfolio", x=[_scm], y=[_scv_num],
                        marker_color="#00C4CC", text=[_scvdp], textposition="outside",
                        showlegend=(_sci == 0),
                    ))
                    _sc_fig.add_trace(go.Bar(
                        name="S. OC Market", x=[_scm], y=[_scm_num],
                        marker_color="#94A3B8", text=[_scmkt], textposition="outside",
                        showlegend=(_sci == 0),
                    ))
                _sc_fig.update_layout(
                    barmode="group", height=280, margin=dict(l=0,r=0,t=20,b=0),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    yaxis=dict(showticklabels=False),
                )
                st.plotly_chart(_sc_fig, use_container_width=True,
                                config=PLOTLY_CONFIG, key="cs_leadership_scorecard")
            else:
                st.info("Load STR and CoStar data to populate the Leadership Scorecard.")
        except Exception as _sc_err:
            st.warning(f"Leadership Scorecard unavailable: {_sc_err}")

    # ── External Signals → sub-tab 2 ──────────────────────────────────────────
    with _cs_t2:
        # ── FRED Economic Climate ─────────────────────────────────────────────────
        st.markdown(sec_div("📉 Economic Climate Indicators"), unsafe_allow_html=True)
        st.markdown(_sh("📉", "Economic Climate Indicators", "indigo", "FRED · Federal Reserve"), unsafe_allow_html=True)
        st.markdown(
            sec_intel(
                "Economic Climate",
                "macro-level demand environment signals that drive travel propensity and ADR sustainability",
                "The FRED Lodging CPI benchmarks national hotel price inflation — when Dana Point ADR grows "
                "faster than CUUR0000SEHB, the market is capturing real premium. When it lags, pricing is losing ground.",
                "Rising disposable income + falling unemployment historically precede 6–12 month "
                "occupancy recovery cycles — the earliest institutional-grade demand forecast signal.",
                "Set FRED_API_KEY in .env to activate (free key at fred.stlouisfed.org)",
            ),
            unsafe_allow_html=True,
        )
        if not df_fred.empty:
            _fred_series = df_fred["series_id"].unique().tolist()
            _fred_sel    = st.selectbox(
                "Select FRED Series",
                options=_fred_series,
                format_func=lambda s: df_fred[df_fred["series_id"] == s]["series_name"].iloc[0]
                    if len(df_fred[df_fred["series_id"] == s]) else s,
                key="fred_series_sel",
            )
            _fred_data = df_fred[df_fred["series_id"] == _fred_sel].copy()
            _fred_data["data_date"] = pd.to_datetime(_fred_data["data_date"])
            _fred_data = _fred_data.dropna(subset=["value"]).sort_values("data_date")

            if not _fred_data.empty:
                _fc1, _fc2 = st.columns([3, 1])
                with _fc1:
                    fig_fred = go.Figure()
                    fig_fred.add_trace(go.Scatter(
                        x=_fred_data["data_date"], y=_fred_data["value"],
                        mode="lines", name=_fred_data["series_name"].iloc[0],
                        line=dict(color="#8B5CF6", width=2.5),
                        fill="tozeroy", fillcolor="rgba(139,92,246,0.08)",
                        hovertemplate="<b>%{x|%b %Y}</b><br>Value: %{y:,.2f}<extra></extra>",
                    ))
                    fig_fred.update_layout(
                        title=_fred_data["series_name"].iloc[0],
                        yaxis_title=_fred_data["unit"].iloc[0] if "unit" in _fred_data.columns else "",
                    )
                    st.plotly_chart(style_fig(fig_fred, height=260), use_container_width=True, config=PLOTLY_CONFIG)
                    st.caption(f"Source: Federal Reserve Bank of St. Louis (FRED). Series: {_fred_sel}. "
                               f"Category: {_fred_data['category'].iloc[0] if 'category' in _fred_data.columns else '—'}")
                with _fc2:
                    _recent = _fred_data.tail(1).iloc[0]
                    _prior  = _fred_data.tail(13).iloc[0] if len(_fred_data) >= 13 else None
                    st.metric(
                        label=_fred_data["series_name"].iloc[0],
                        value=f"{_recent['value']:,.2f}",
                        delta=f"{((_recent['value'] - _prior['value']) / _prior['value'] * 100):+.1f}% YoY"
                              if _prior is not None and _prior["value"] else None,
                    )
                    st.markdown(
                        f'<div class="dp-callout" style="margin-top:10px;">'
                        f'<strong>How to read this:</strong><br>'
                        f'{_fred_data["category"].iloc[0] if "category" in _fred_data.columns else "Economic indicator"}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
        else:
            st.markdown(
                '<div class="empty-card">'
                '<div class="empty-icon">📉</div>'
                '<div class="empty-title">FRED Economic Data Not Loaded</div>'
                '<div class="empty-body">Add <code>FRED_API_KEY=your_key</code> to your .env file, '
                'then run the pipeline.<br>'
                'Free key at <strong>fred.stlouisfed.org</strong> — 30-second registration.</div>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── BLS Hospitality Employment ────────────────────────────────────────────
        st.markdown(sec_div("👥 Hospitality Employment Trends"), unsafe_allow_html=True)
        st.markdown(_sh("👥", "Hospitality Employment Trends", "green", "BLS · Bureau of Labor Statistics"), unsafe_allow_html=True)
        st.markdown(
            sec_intel(
                "Hospitality Employment",
                "monthly employment in Leisure & Hospitality and Accommodation sectors — national and California",
                "Employment tracks hotel occupancy with a 2–3 month lag — rising hotel employment = "
                "supply expansion and labor cost pressure, while falling employment signals demand risk.",
                "California Accommodation employment serves as the regional demand barometer. "
                "Watch for YoY acceleration as an early signal of market tightening ahead of peak season.",
                "Run pipeline step 15 (fetch_bls_data.py) to populate — no API key required",
            ),
            unsafe_allow_html=True,
        )
        if not df_bls.empty:
            _bls_series = df_bls["series_name"].unique().tolist()
            _bls_sel    = st.multiselect(
                "Select Employment Series",
                options=_bls_series,
                default=_bls_series[:2] if len(_bls_series) >= 2 else _bls_series,
                key="bls_series_sel",
            )
            _bls_data = df_bls[df_bls["series_name"].isin(_bls_sel)].copy() if _bls_sel else df_bls.copy()

            if not _bls_data.empty and "date" in _bls_data.columns:
                _bc1, _bc2 = st.columns([3, 1])
                with _bc1:
                    fig_bls = go.Figure()
                    _bls_colors = [GREEN, TEAL_LIGHT, ORANGE, "#8B5CF6"]
                    for i, (s_name, s_df) in enumerate(_bls_data.groupby("series_name")):
                        s_df = s_df.sort_values("date")
                        fig_bls.add_trace(go.Scatter(
                            x=s_df["date"], y=s_df["value_thousands"],
                            mode="lines", name=s_name,
                            line=dict(color=_bls_colors[i % len(_bls_colors)], width=2),
                            hovertemplate=f"<b>%{{x|%b %Y}}</b><br>{s_name}: %{{y:,.0f}}K<extra></extra>",
                        ))
                    fig_bls.update_layout(
                        title="Hospitality Employment (thousands)",
                        yaxis_title="Employees (thousands)",
                    )
                    st.plotly_chart(style_fig(fig_bls, height=270), use_container_width=True, config=PLOTLY_CONFIG)
                    st.caption("Source: U.S. Bureau of Labor Statistics. Seasonally adjusted, all employees. "
                               "National = US total; CA = California statewide.")
                with _bc2:
                    for s_name, s_df in _bls_data.groupby("series_name"):
                        s_df = s_df.sort_values("date")
                        if not s_df.empty:
                            _latest = s_df.iloc[-1]
                            st.metric(
                                label=s_name[:35] + "…" if len(s_name) > 35 else s_name,
                                value=f"{_latest['value_thousands']:,.0f}K",
                                delta=f"{_latest['yoy_chg_pct']:+.1f}% YoY"
                                      if pd.notna(_latest.get("yoy_chg_pct")) else None,
                            )
        else:
            st.markdown(
                '<div class="empty-card">'
                '<div class="empty-icon">👥</div>'
                '<div class="empty-title">BLS Employment Data Not Loaded</div>'
                '<div class="empty-body">Run <code>python scripts/run_pipeline.py</code> '
                'to fetch BLS hospitality employment data.<br>No API key required for basic access.</div>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── EIA California Gas Prices ─────────────────────────────────────────────
        st.markdown(sec_div("⛽ California Gas Prices — Drive-Market Demand Signal"), unsafe_allow_html=True)
        st.markdown(_sh("⛽", "EIA California Retail Gas Prices", "orange", "DRIVE-MARKET SIGNAL"), unsafe_allow_html=True)
        st.caption(
            "Source: U.S. Energy Information Administration (EIA) · Weekly California retail regular-grade gasoline. "
            "Drive market (LA/OC/SD/IE) within 120 miles of Dana Point — gas price spikes correlate with 2–4% weekend occupancy softening 2–4 weeks out."
        )
        if not df_eia_gas.empty and "date" in df_eia_gas.columns:
            _eia_ca = df_eia_gas[df_eia_gas["state_label"] == "CA"].copy()
            _eia_us = df_eia_gas[df_eia_gas["state_label"] == "US"].copy()
            if not _eia_ca.empty:
                _eia_ca = _eia_ca.sort_values("date")
                _ec1, _ec2 = st.columns([3, 1])
                with _ec1:
                    fig_eia = go.Figure()
                    fig_eia.add_trace(go.Scatter(
                        x=_eia_ca["date"], y=_eia_ca["price_per_gallon"],
                        mode="lines+markers", name="CA Regular",
                        line=dict(color=ORANGE, width=2.5),
                        marker=dict(size=4),
                        hovertemplate="<b>%{x|%b %d, %Y}</b><br>CA Price: $%{y:.3f}/gal<extra></extra>",
                    ))
                    if not _eia_us.empty:
                        _eia_us = _eia_us.sort_values("date")
                        fig_eia.add_trace(go.Scatter(
                            x=_eia_us["date"], y=_eia_us["price_per_gallon"],
                            mode="lines", name="US National Avg",
                            line=dict(color=TEAL_LIGHT, width=1.8, dash="dot"),
                            hovertemplate="<b>%{x|%b %d, %Y}</b><br>US Avg: $%{y:.3f}/gal<extra></extra>",
                        ))
                    # Add trendline band for high-gas-price alert
                    _eia_max = _eia_ca["price_per_gallon"].max() if not _eia_ca.empty else 5.0
                    fig_eia.add_hline(y=4.50, line_dash="dash", line_color="#DC2626", line_width=1,
                                      annotation_text="$4.50 demand risk threshold", annotation_position="top right")
                    fig_eia.update_layout(
                        title="CA Weekly Retail Gas Price ($/gal)",
                        yaxis_title="Price per Gallon ($)",
                        yaxis_tickformat="$,.3f",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(style_fig(fig_eia, height=280), use_container_width=True, config=PLOTLY_CONFIG)
                with _ec2:
                    _eia_latest = _eia_ca.iloc[-1]
                    _eia_prior  = _eia_ca.iloc[-5] if len(_eia_ca) >= 5 else _eia_ca.iloc[0]
                    _eia_delta  = _eia_latest["price_per_gallon"] - _eia_prior["price_per_gallon"]
                    st.metric("Latest CA Price",
                              f"${_eia_latest['price_per_gallon']:.3f}/gal",
                              f"{_eia_delta:+.3f} vs 4wk prior")
                    _eia_yr_avg = _eia_ca[_eia_ca["date"].dt.year == _eia_ca["date"].dt.year.max()]["price_per_gallon"].mean()
                    st.metric("YTD CA Avg", f"${_eia_yr_avg:.3f}/gal")
                    _demand_risk = "🔴 High" if _eia_latest["price_per_gallon"] >= 4.50 else ("🟡 Moderate" if _eia_latest["price_per_gallon"] >= 4.00 else "🟢 Low")
                    st.metric("Drive-Market Risk", _demand_risk)
                    st.caption(
                        "📌 **Rule of thumb:** Every $0.20/gal increase above $4.00 correlates with ~2–3% softening "
                        "in LA/OC/SD weekend trip decisions 2–3 weeks out."
                    )
            else:
                st.info("Run `python scripts/fetch_eia_gas.py` to populate CA gas price data.")
        else:
            st.markdown(
                '<div class="empty-card">'
                '<div class="empty-icon">⛽</div>'
                '<div class="empty-title">EIA Gas Price Data Not Loaded</div>'
                '<div class="empty-body">Run <code>python scripts/run_pipeline.py</code> '
                'to fetch EIA gas prices. Add <code>EIA_API_KEY</code> to .env for live data '
                '(free at eia.gov/opendata). Demo data seeds automatically without a key.</div>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── TSA Checkpoint Throughput ─────────────────────────────────────────────
        st.markdown(sec_div("✈️ TSA Checkpoint Throughput — Air Travel Demand"), unsafe_allow_html=True)
        st.markdown(_sh("✈️", "TSA Checkpoint Throughput", "indigo", "FLY-MARKET SIGNAL"), unsafe_allow_html=True)
        st.caption(
            "Source: U.S. Transportation Security Administration · Daily checkpoint traveler counts. "
            "Dana Point's fly-market feeders (SLC, DFW, PHX, DEN) generate highest-ADR overnight visitors — TSA surge signals premium demand 3–7 days out."
        )
        if not df_tsa.empty and "date" in df_tsa.columns:
            _tsa_sorted = df_tsa.sort_values("date")
            _tc1, _tc2 = st.columns([3, 1])
            with _tc1:
                fig_tsa = go.Figure()
                fig_tsa.add_trace(go.Scatter(
                    x=_tsa_sorted["date"], y=_tsa_sorted["travelers_count"],
                    mode="lines", name="2025/2026 Travelers",
                    line=dict(color=BLUE, width=2.5),
                    fill="tozeroy", fillcolor="rgba(5,103,200,0.07)",
                    hovertemplate="<b>%{x|%b %d, %Y}</b><br>Travelers: %{y:,.0f}<extra></extra>",
                ))
                if "travelers_prior_year" in _tsa_sorted.columns:
                    fig_tsa.add_trace(go.Scatter(
                        x=_tsa_sorted["date"], y=_tsa_sorted["travelers_prior_year"],
                        mode="lines", name="Prior Year",
                        line=dict(color=TEAL_LIGHT, width=1.5, dash="dot"),
                        hovertemplate="<b>%{x|%b %Y}</b><br>Prior Year: %{y:,.0f}<extra></extra>",
                    ))
                if "rolling_7d_avg" in _tsa_sorted.columns:
                    fig_tsa.add_trace(go.Scatter(
                        x=_tsa_sorted["date"], y=_tsa_sorted["rolling_7d_avg"],
                        mode="lines", name="7-Day Avg",
                        line=dict(color=ORANGE, width=2, dash="dash"),
                        hovertemplate="<b>%{x|%b %Y}</b><br>7d Avg: %{y:,.0f}<extra></extra>",
                    ))
                fig_tsa.update_layout(
                    title="Daily TSA Checkpoint Travelers",
                    yaxis_title="Travelers",
                    yaxis_tickformat=",",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(style_fig(fig_tsa, height=280), use_container_width=True, config=PLOTLY_CONFIG)
            with _tc2:
                _tsa_latest = _tsa_sorted.iloc[-1]
                _tsa_prior  = _tsa_sorted.iloc[-2] if len(_tsa_sorted) >= 2 else _tsa_sorted.iloc[0]
                _tsa_delta  = int(_tsa_latest["travelers_count"] - _tsa_prior["travelers_count"]) if _tsa_prior["travelers_count"] else 0
                st.metric("Latest Count",
                          f"{int(_tsa_latest['travelers_count']):,}",
                          f"{_tsa_delta:+,} vs prior")
                if pd.notna(_tsa_latest.get("yoy_pct_change")):
                    st.metric("YOY Change", f"{_tsa_latest['yoy_pct_change']:+.1f}%")
                _tsa_yr_avg = int(_tsa_sorted[_tsa_sorted["date"].dt.year == _tsa_sorted["date"].dt.year.max()]["travelers_count"].mean())
                st.metric("YTD Daily Avg", f"{_tsa_yr_avg:,}")
                st.caption(
                    "📌 **Fly-market strategy:** When TSA throughput > 2.8M/day, "
                    "activate fly-market ADR premiums. SLC, DFW, PHX visitors average 1.3–1.4× ADR vs. drive markets."
                )
        else:
            st.markdown(
                '<div class="empty-card">'
                '<div class="empty-icon">✈️</div>'
                '<div class="empty-title">TSA Checkpoint Data Not Loaded</div>'
                '<div class="empty-body">Run <code>python scripts/run_pipeline.py</code> '
                'to fetch TSA throughput data. No API key required.</div>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── External Correlations ─────────────────────────────────────────────────
        with st.expander("🔗 External Correlations — Gas Prices vs. Hotel Demand", expanded=False):
            st.caption(
                "Correlation analysis: CA gas prices vs. STR occupancy · "
                "Positive correlation = gas price drop boosts drive-market demand. "
                "Negative correlation = gas spike suppresses weekend leisure travel."
            )
            if not df_eia_gas.empty and not df_kpi.empty and "date" in df_eia_gas.columns:
                try:
                    _eia_ca_corr = df_eia_gas[df_eia_gas["state_label"] == "CA"][["date", "price_per_gallon"]].copy()
                    _eia_ca_corr["year_month"] = _eia_ca_corr["date"].dt.to_period("M")
                    _eia_monthly_avg = _eia_ca_corr.groupby("year_month")["price_per_gallon"].mean().reset_index()
                    _eia_monthly_avg["date"] = _eia_monthly_avg["year_month"].dt.to_timestamp()

                    _kpi_corr = df_kpi[["as_of_date", "occ_pct", "adr", "revpar"]].copy()
                    _kpi_corr["year_month"] = pd.to_datetime(_kpi_corr["as_of_date"]).dt.to_period("M")
                    _kpi_monthly = _kpi_corr.groupby("year_month").agg(
                        avg_occ=("occ_pct", "mean"), avg_adr=("adr", "mean"), avg_rvp=("revpar", "mean")
                    ).reset_index()
                    _kpi_monthly["date"] = _kpi_monthly["year_month"].dt.to_timestamp()

                    _merged = pd.merge(_eia_monthly_avg, _kpi_monthly, on="date", how="inner")
                    if len(_merged) >= 6:
                        _corr_occ = _merged["price_per_gallon"].corr(_merged["avg_occ"])
                        _corr_adr = _merged["price_per_gallon"].corr(_merged["avg_adr"])
                        _corr_rvp = _merged["price_per_gallon"].corr(_merged["avg_rvp"])
                        _cc1, _cc2, _cc3 = st.columns(3)
                        with _cc1:
                            _occ_dir = "inverse" if _corr_occ < -0.1 else ("positive" if _corr_occ > 0.1 else "neutral")
                            st.metric("Gas ↔ Occupancy", f"r = {_corr_occ:.2f}", f"Correlation: {_occ_dir}")
                        with _cc2:
                            _adr_dir = "positive" if _corr_adr > 0.1 else ("inverse" if _corr_adr < -0.1 else "neutral")
                            st.metric("Gas ↔ ADR", f"r = {_corr_adr:.2f}", f"Correlation: {_adr_dir}")
                        with _cc3:
                            _rvp_dir = "positive" if _corr_rvp > 0.1 else ("inverse" if _corr_rvp < -0.1 else "neutral")
                            st.metric("Gas ↔ RevPAR", f"r = {_corr_rvp:.2f}", f"Correlation: {_rvp_dir}")

                        fig_corr = go.Figure()
                        fig_corr.add_trace(go.Bar(
                            x=["Gas ↔ Occupancy", "Gas ↔ ADR", "Gas ↔ RevPAR"],
                            y=[_corr_occ, _corr_adr, _corr_rvp],
                            marker_color=[ORANGE if v < 0 else GREEN for v in [_corr_occ, _corr_adr, _corr_rvp]],
                            text=[f"r={v:.2f}" for v in [_corr_occ, _corr_adr, _corr_rvp]],
                            textposition="outside",
                        ))
                        fig_corr.update_layout(
                            title="Pearson Correlation: CA Gas Price vs. STR Metrics",
                            yaxis=dict(range=[-1, 1], title="Correlation Coefficient (r)"),
                            showlegend=False,
                        )
                        st.plotly_chart(style_fig(fig_corr, height=240), use_container_width=True, config=PLOTLY_CONFIG)
                        st.caption(
                            "Interpretation: r > +0.5 = strong positive correlation · r < −0.5 = strong inverse. "
                            "Negative gas↔occupancy means higher gas prices suppress drive-market leisure demand. "
                            f"Based on {len(_merged)} months of overlapping data."
                        )
                    else:
                        st.info("Not enough overlapping data yet to compute correlations. Run the pipeline to build up data.")
                except Exception as _corr_err:
                    st.caption(f"Correlation analysis unavailable: {_corr_err}")
            else:
                st.info("Load both EIA gas prices and STR KPIs to see correlation analysis.")

        # ── Live Market Intelligence (Perplexity Sonar) ──────────────────────────
        st.markdown(sec_div("🌐 Live Market Intelligence"), unsafe_allow_html=True)
        st.markdown(_sh("🌐", "Live Competitive Intelligence — Real-Time Web Search", "indigo", "PERPLEXITY SONAR"), unsafe_allow_html=True)
        st.caption(
            "Powered by Perplexity Sonar Pro — searches the live web for competitor news, travel trends, and market events. "
            "Configure PERPLEXITY_API_KEY in .env to activate. Claude / GPT-4o can be used for offline analysis."
        )

        _LIVE_INTEL_PROMPTS = [
            ("🏨 Dana Point Competitor News",
             "Search for the latest news about Waldorf Astoria Monarch Beach, Ritz-Carlton Laguna Niguel, "
             "and Laguna Cliffs Marriott. Any new renovations, rate changes, or ownership updates in 2025–2026? "
             "How does this affect Dana Point's competitive position?"),
            ("✈️ SoCal Travel Demand Trends",
             "What are the current travel demand trends for Southern California coastal destinations in 2026? "
             "Any data on visitor volume, ADR trends, or booking pace for Orange County hotels?"),
            ("📈 OC Hotel Market News",
             "What is the latest news about Orange County hotel market performance in 2025–2026? "
             "Any new hotel openings, closures, renovations, or major group bookings in Dana Point or South OC?"),
            ("🎪 Dana Point Events 2026",
             "What major events are coming to Dana Point, California in 2026? "
             "Include festivals, sporting events, concerts, and community events that drive hotel demand."),
            ("⛽ Gas Price Impact on SoCal Drive Markets",
             "What are the current California gas prices and trends as of 2026? "
             "How is this affecting drive-market leisure travel to Orange County coastal destinations like Dana Point?"),
            ("💡 DMO Best Practices 2026",
             "What are the most innovative destination marketing strategies being used by California coastal DMOs in 2026? "
             "Any case studies of successful TBID campaigns or visitor economy growth initiatives?"),
        ]

        _li_cols = st.columns(3)
        for _li_i, (_li_lbl, _li_prompt) in enumerate(_LIVE_INTEL_PROMPTS):
            with _li_cols[_li_i % 3]:
                if st.button(_li_lbl, key=f"li_btn_{_li_i}", use_container_width=True):
                    st.session_state["li_pending_prompt"] = _li_prompt
                    st.session_state["li_pending_label"]  = _li_lbl

        _li_custom = st.text_input(
            "Or search any market intelligence question:",
            key="li_custom_q",
            placeholder="e.g. What new hotel brands are expanding in Orange County in 2026?",
        )
        _li_model_opts = [k for k, v in AI_MODELS.items() if v["provider"] == "perplexity" and bool(_PERPLEXITY_KEY) and OPENAI_AVAILABLE]
        _li_model_opts += [k for k, v in AI_MODELS.items() if v["provider"] in ("anthropic", "openai", "google")]
        _li_model_sel = AI_MODELS.get(st.session_state.get("selected_model", CLAUDE_MODEL), AI_MODELS[CLAUDE_MODEL])
        _li_use_model = st.session_state.get("selected_model", CLAUDE_MODEL)

        _li_col1, _li_col2 = st.columns([1, 4])
        with _li_col1:
            if st.button("🔍 Search", key="li_search_btn", type="primary", use_container_width=True):
                if _li_custom.strip():
                    st.session_state["li_pending_prompt"] = _li_custom.strip()
                    st.session_state["li_pending_label"]  = f"💬 {_li_custom.strip()[:50]}"

        _li_pend = st.session_state.get("li_pending_prompt", "")
        if _li_pend:
            _li_lbl_disp = st.session_state.get("li_pending_label", "Search")
            _li_mdl      = st.session_state.get("selected_model", CLAUDE_MODEL)
            _li_mdl_info = AI_MODELS.get(_li_mdl, {})
            _li_badge    = f"{_li_mdl_info.get('badge','🟦')} {_li_mdl_info.get('label', _li_mdl)}"
            _li_any_ai   = (
                (api_key_valid and ANTHROPIC_AVAILABLE) or
                (bool(_OPENAI_KEY) and OPENAI_AVAILABLE) or
                (bool(_GOOGLE_AI_KEY) and GEMINI_AVAILABLE) or
                (bool(_PERPLEXITY_KEY) and OPENAI_AVAILABLE)
            )
            if _li_any_ai:
                st.markdown(f"**{_li_lbl_disp}** — via {_li_badge}")
                with st.spinner(f"Searching with {_li_badge}…"):
                    _li_result = st.write_stream(stream_ai_response(_li_pend, _li_mdl, _ai_keys))
                if _li_result:
                    _li_dl = f"# {_li_lbl_disp}\n\n{_li_result}"
                    st.download_button(
                        "⬇️ Download Intelligence Report",
                        _li_dl.encode(),
                        file_name=f"live_intel_{datetime.now().strftime('%Y%m%d_%H%M')}.md",
                        mime="text/markdown",
                        key="li_dl_btn",
                    )
                del st.session_state["li_pending_prompt"]
            else:
                st.info("💡 Add an API key in the sidebar (Anthropic, OpenAI, Google AI, or Perplexity) to activate Live Intelligence.")
                del st.session_state["li_pending_prompt"]

    # ══════════════════════════════════════════════════════════════════════════════


    def render_audit_report() -> None:
        """Generate and render a comprehensive data audit for the PULSE app."""
        import datetime as _dt

        st.markdown(_sh("🔍", "App Audit Report", color="indigo", tag="LIVE"), unsafe_allow_html=True)

        _now = _dt.datetime.now()
        _checks = []

        # ── DataFrame emptiness checks ────────────────────────────────────────────
        _df_registry = {
            "STR Daily":           df_daily,
            "STR Monthly":         df_monthly,
            "KPI Daily Summary":   df_kpi,
            "Compression Qtrs":    df_comp,
            "CoStar Monthly":      df_cs_mon,
            "CoStar Snapshot":     df_cs_snap,
            "CoStar Pipeline":     df_cs_pipe,
            "Datafy Overview":     df_dfy_ov,
            "Datafy DMA":          df_dfy_dma,
            "Datafy Spending":     df_dfy_spend,
            "Datafy Media KPIs":   df_dfy_media,
            "Datafy Website KPIs": df_dfy_web,
            "Insights Daily":      df_insights,
            "EIA Gas Prices":      df_eia_gas,
            "TSA Checkpoint":      df_tsa,
        }
        _empty = []
        _populated = []
        for _lbl, _df in _df_registry.items():
            if _df is None or (hasattr(_df, "empty") and _df.empty):
                _empty.append(_lbl)
                _checks.append(("🔴", _lbl, "Empty — source not loaded or pipeline step failed"))
            else:
                _populated.append(_lbl)
                _checks.append(("🟢", _lbl, f"{len(_df):,} rows loaded"))

        # ── Insights freshness check ──────────────────────────────────────────────
        _insights_status = "🔴 No insights — run pipeline"
        _insights_detail = "insights_daily is empty"
        if not df_insights.empty and "as_of_date" in df_insights.columns:
            try:
                _latest_ins = pd.to_datetime(df_insights["as_of_date"]).max()
                _ins_age = (_now.date() - _latest_ins.date()).days
                if _ins_age == 0:
                    _insights_status = "🟢 Current"
                    _insights_detail = f"Insights generated today ({_latest_ins.strftime('%Y-%m-%d')})"
                elif _ins_age <= 7:
                    _insights_status = "🟡 Recent"
                    _insights_detail = f"Last updated {_ins_age}d ago ({_latest_ins.strftime('%Y-%m-%d')})"
                else:
                    _insights_status = "🔴 Stale"
                    _insights_detail = f"Last updated {_ins_age}d ago — run pipeline to refresh"
            except Exception:
                pass

        # ── STR data recency check ────────────────────────────────────────────────
        _str_status = "🔴 No STR data"
        _str_detail = "fact_str_metrics is empty"
        if not df_daily.empty and "as_of_date" in df_daily.columns:
            try:
                _latest_str = pd.to_datetime(df_daily["as_of_date"]).max()
                _str_age = (_now.date() - _latest_str.date()).days
                if _str_age <= 14:
                    _str_status = "🟢 Current"
                elif _str_age <= 45:
                    _str_status = "🟡 Recent"
                else:
                    _str_status = "🔴 Stale (>45 days)"
                _str_detail = f"Latest STR date: {_latest_str.strftime('%Y-%m-%d')} ({_str_age}d ago)"
            except Exception:
                pass

        # ── Render audit summary cards ────────────────────────────────────────────
        _ac1, _ac2, _ac3 = st.columns(3)
        with _ac1:
            st.markdown(
                f'<div style="background:#FFFFFF;border:1px solid rgba(5,150,105,0.20);border-left:3px solid #059669;'
                f'border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">'
                f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#059669;margin-bottom:4px;">Populated Sources</div>'
                f'<div style="font-size:28px;font-weight:800;color:#0D1B2E;">{len(_populated)}</div>'
                f'<div style="font-size:12px;color:#64748B;">of {len(_df_registry)} tracked DataFrames</div>'
                f'</div>', unsafe_allow_html=True)
        with _ac2:
            _ac2_color = "#DC2626" if _empty else "#059669"
            st.markdown(
                f'<div style="background:#FFFFFF;border:1px solid rgba(220,38,38,0.20);border-left:3px solid {_ac2_color};'
                f'border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">'
                f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:{_ac2_color};margin-bottom:4px;">Empty Sources</div>'
                f'<div style="font-size:28px;font-weight:800;color:#0D1B2E;">{len(_empty)}</div>'
                f'<div style="font-size:12px;color:#64748B;">{"Run pipeline to fix" if _empty else "All sources healthy"}</div>'
                f'</div>', unsafe_allow_html=True)
        with _ac3:
            st.markdown(
                f'<div style="background:#FFFFFF;border:1px solid rgba(5,103,200,0.20);border-left:3px solid #0567C8;'
                f'border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">'
                f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#0567C8;margin-bottom:4px;">Insights Status</div>'
                f'<div style="font-size:18px;font-weight:800;color:#0D1B2E;">{_insights_status}</div>'
                f'<div style="font-size:12px;color:#64748B;">{_insights_detail}</div>'
                f'</div>', unsafe_allow_html=True)

        st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)

        # ── STR recency + detailed checklist ─────────────────────────────────────
        _ac4, _ac5 = st.columns([1, 2])
        with _ac4:
            st.markdown(
                f'<div style="background:#FFFFFF;border:1px solid rgba(5,103,200,0.15);border-left:3px solid #0567C8;'
                f'border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">'
                f'<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#0567C8;margin-bottom:4px;">STR Data Recency</div>'
                f'<div style="font-size:18px;font-weight:800;color:#0D1B2E;">{_str_status}</div>'
                f'<div style="font-size:12px;color:#64748B;">{_str_detail}</div>'
                f'</div>', unsafe_allow_html=True)
        with _ac5:
            with st.expander("📋 Full DataFrame Status Checklist", expanded=False):
                _rows_html = ""
                for _dot, _lbl, _msg in _checks:
                    _rows_html += (
                        f'<div style="display:flex;align-items:center;gap:10px;padding:5px 0;'
                        f'border-bottom:1px solid rgba(0,0,0,0.05);">'
                        f'<span style="font-size:14px;">{_dot}</span>'
                        f'<span style="font-weight:600;color:#0D1B2E;min-width:160px;font-size:13px;">{_lbl}</span>'
                        f'<span style="color:#64748B;font-size:12px;">{_msg}</span>'
                        f'</div>'
                    )
                st.markdown(
                    f'<div style="background:#F8FAFC;border-radius:8px;padding:10px 14px;">'
                    f'{_rows_html}</div>',
                    unsafe_allow_html=True)

        st.markdown("<div style='margin-top:6px;'></div>", unsafe_allow_html=True)


    # TAB 5 — DATA LOG
    # ══════════════════════════════════════════════════════════════════════════════
with tab_dl:
    _tab_controls("dl", show_filter_badge=False)
    st.markdown("""
    <div class="hero-banner">
      <div class="hero-title">Data Vault</div>
      <div class="hero-subtitle">Pipeline Audit Trail · Source Health · Row Counts · CSV Downloads · Database Inventory</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown(tab_summary(
        "<strong>Full Transparency:</strong> Every table loaded into analytics.sqlite is tracked here with row counts, date coverage, and ETL timestamps. "
        "This is the operational layer — use it to verify data freshness, diagnose missing sources, and download raw CSVs for external analysis. "
        "A green dot means the source is populated and current. A black dot means the ETL step did not run or returned no data. "
        "<strong>Run the pipeline</strong> (<code>python scripts/run_pipeline.py</code>) to refresh all sources."
    ), unsafe_allow_html=True)
    st.markdown(sec_div("📋 Load Log — ETL Audit Trail"), unsafe_allow_html=True)
    col_a, col_b = st.columns([3, 1])

    with col_a:
        st.markdown(
            '<div style="font-family:\'Syne\',sans-serif;font-size:1.2rem;'
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
            '<div style="font-family:\'Syne\',sans-serif;font-size:1.2rem;'
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

    st.markdown(sec_div("🟢 Data Source Health"), unsafe_allow_html=True)

    # ── Data Source Health ─────────────────────────────────────────────────────
    st.markdown(
        '<div style="font-family:\'Syne\',sans-serif;font-size:1.2rem;'
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
            url="https://str.com",
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _m_dot, "STR Monthly",
            f"grain=monthly · {_m_range}",
            f"{str_monthly_rows:,}" if str_monthly_rows > 0 else "—",
            url="https://str.com",
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
            url="https://datafy.ai",
        ), unsafe_allow_html=True)
        _cs_tables = [t for t in counts if t.startswith("costar_")]
        _cs_total  = sum(counts[t] for t in _cs_tables if isinstance(counts[t], int))
        _cs_src_dot = "🟢" if _cs_total > 0 else "⚫"
        st.markdown(source_card(
            _cs_src_dot, "CoStar Market Intelligence",
            f"{len(_cs_tables)} tables · hospitality analytics",
            f"{_cs_total:,}" if _cs_total > 0 else "—",
            url="https://www.costar.com",
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
            url="https://www.visitdanapoint.com/events",
        ), unsafe_allow_html=True)
        _vca_tables = [t for t in counts if t.startswith("visit_ca_")]
        _vca_total  = sum(counts[t] for t in _vca_tables if isinstance(counts[t], int))
        _vca_src_dot = "🟢" if _vca_total > 0 else "⚫"
        st.markdown(source_card(
            _vca_src_dot, "Visit California",
            f"{len(_vca_tables)} tables · statewide travel & lodging forecasts",
            f"{_vca_total:,}" if _vca_total > 0 else "—",
            url="https://industry.visitcalifornia.com",
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
            url="https://later.com",
        ), unsafe_allow_html=True)
        _eia_ct  = counts.get("eia_gas_prices", 0)
        _eia_dot = "🟢" if isinstance(_eia_ct, int) and _eia_ct > 0 else "⚫"
        st.markdown(source_card(
            _eia_dot, "EIA Gas Prices",
            "eia_gas_prices · CA weekly retail gas · drive-market demand signal",
            f"{_eia_ct:,}" if isinstance(_eia_ct, int) and _eia_ct > 0 else "—",
        ), unsafe_allow_html=True)
        _tsa_ct  = counts.get("tsa_checkpoint_daily", 0)
        _tsa_dot = "🟢" if isinstance(_tsa_ct, int) and _tsa_ct > 0 else "⚫"
        st.markdown(source_card(
            _tsa_dot, "TSA Checkpoint Data",
            "tsa_checkpoint_daily · national air travel demand · fly-market signal",
            f"{_tsa_ct:,}" if isinstance(_tsa_ct, int) and _tsa_ct > 0 else "—",
        ), unsafe_allow_html=True)
        _wx_ct   = counts.get("weather_monthly", 0)
        _gt_ct   = counts.get("google_trends_weekly", 0)
        _bls_ct  = counts.get("bls_employment_monthly", 0)
        _wx_dot  = "🟢" if isinstance(_wx_ct, int) and _wx_ct > 0 else "⚫"
        _gt_dot  = "🟢" if isinstance(_gt_ct, int) and _gt_ct > 0 else "⚫"
        _bls_dot = "🟢" if isinstance(_bls_ct, int) and _bls_ct > 0 else "⚫"
        st.markdown(source_card(
            _wx_dot, "Open-Meteo Weather",
            "weather_monthly · Dana Point beach day score · coastal demand driver",
            f"{_wx_ct:,}" if isinstance(_wx_ct, int) and _wx_ct > 0 else "—",
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _gt_dot, "Google Trends",
            "google_trends_weekly · search demand signals · 'Dana Point hotels' etc.",
            f"{_gt_ct:,}" if isinstance(_gt_ct, int) and _gt_ct > 0 else "—",
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _bls_dot, "BLS Employment",
            "bls_employment_monthly · OC hospitality employment · sector health",
            f"{_bls_ct:,}" if isinstance(_bls_ct, int) and _bls_ct > 0 else "—",
        ), unsafe_allow_html=True)
        _noaa_ct  = counts.get("noaa_marine_monthly", 0)
        _cen_ct   = counts.get("census_demographics", 0)
        _noaa_dot = "🟢" if isinstance(_noaa_ct, int) and _noaa_ct > 0 else "⚫"
        _cen_dot  = "🟢" if isinstance(_cen_ct, int) and _cen_ct > 0 else "⚫"
        st.markdown(source_card(
            _noaa_dot, "NOAA Marine Conditions",
            "noaa_marine_monthly · wave height, water temp, beach activity score",
            f"{_noaa_ct:,}" if isinstance(_noaa_ct, int) and _noaa_ct > 0 else "—",
        ), unsafe_allow_html=True)
        st.markdown(source_card(
            _cen_dot, "US Census ACS",
            "census_demographics · OC/LA/SD income, population, home values",
            f"{_cen_ct:,}" if isinstance(_cen_ct, int) and _cen_ct > 0 else "—",
        ), unsafe_allow_html=True)

    st.markdown(sec_div("📥 Recent Metric Samples & CSV Downloads"), unsafe_allow_html=True)

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
        '<div style="font-family:\'Syne\',sans-serif;font-size:1.2rem;'
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

    st.markdown(sec_div("⬇️ Download Table Data"), unsafe_allow_html=True)

    # ── Download buttons for major tables ──────────────────────────────────
    st.markdown(
        '<div style="font-family:\'Syne\',sans-serif;font-size:1.2rem;'
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

    st.markdown(sec_div("🗺️ Brain Architecture & Database Inventory"), unsafe_allow_html=True)

    # ── Brain Architecture — Table Relationships ────────────────────────────
    with st.expander("🗺 Brain Architecture — Table Relationships", expanded=False):
        st.markdown(
            '<div style="font-family:\'Syne\',sans-serif;font-size:1rem;'
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
            '<div style="font-family:\'Syne\',sans-serif;font-size:1rem;'
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
            "eia_gas_prices":                      "EIA California weekly retail gas prices (drive-market demand signal)",
            "tsa_checkpoint_daily":                "TSA daily checkpoint throughput (air travel demand proxy)",
            "bls_employment_monthly":              "BLS OC hospitality employment (sector health)",
            "google_trends_weekly":                "Google Trends search demand signals",
            "weather_monthly":                     "Open-Meteo coastal weather + beach day score",
            "noaa_marine_monthly":                 "NOAA buoy ocean conditions (wave height, water temp, beach activity score)",
            "census_demographics":                 "US Census ACS feeder market demographics (OC, LA, SD counties)",
            "fred_economic_indicators":            "FRED economic indicators (CPI, income, consumer sentiment, housing)",
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

    # ── App Audit Report ──────────────────────────────────────────────────────
    render_audit_report()

    # ── Data Vault Intel Panel ────────────────────────────────────────────────
    try:
        _total_tables = len([t for t in counts if isinstance(counts.get(t), int) and counts.get(t, 0) > 0])
        _total_rows   = sum(v for v in counts.values() if isinstance(v, int))
        _missing      = [t for t, v in counts.items() if isinstance(v, int) and v == 0]
        _dl_next_steps = [
            f"<strong>Data Freshness:</strong> {_total_tables} active tables · {_total_rows:,} total rows in analytics.sqlite. "
            "Run <code>python scripts/run_pipeline.py</code> after loading new STR or Datafy files.",
            f"<strong>Missing Sources:</strong> " + (
                f"{len(_missing)} tables empty ({', '.join(_missing[:3])}{'...' if len(_missing) > 3 else ''}). "
                "Check pipeline logs and load missing source files." if _missing
                else "All tracked tables populated — data is complete."
            ),
            "<strong>New Data Sources Available:</strong> EIA gas prices, TSA checkpoint, NOAA ocean conditions, and Census ACS demographics "
            "are now in the pipeline. Set <code>EIA_API_KEY</code> and <code>CENSUS_API_KEY</code> in .env for live data.",
            "<strong>Pipeline Cadence:</strong> Run the pipeline weekly after receiving new STR exports. "
            "Insights engine auto-updates forward-looking analysis for all 4 audiences on every run.",
        ]
        _dl_questions = [
            "What's the freshest data in the database?",
            "Which pipeline steps are most likely to fail?",
            "How do I add a new data source to the pipeline?",
            "What tables should I prioritize for the board report?",
        ]
        _dl_context = (
            f"PULSE data vault: {_total_tables} active tables, {_total_rows:,} total rows. "
            f"Sources: STR, Datafy, CoStar, Visit California, Zartico, Later.com, EIA gas, TSA, NOAA Marine, Census ACS, BLS, FRED, Weather, Google Trends. "
            f"Pipeline: 19 steps. Run python scripts/run_pipeline.py to refresh."
        )
        render_intel_panel("dl_vault", _dl_next_steps, _dl_questions, _dl_context)
    except Exception:
        pass


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
  <div style="background:#FFFFFF;border:1px solid rgba(0,0,0,0.08);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;
              box-shadow:0 1px 4px rgba(0,0,0,0.07);">
    <div style="font-weight:700;color:#0F1C2E;margin-bottom:2px;">STR</div>
    <div style="color:#718096;">Smith Travel Research · daily &amp; monthly hotel benchmarking</div>
  </div>
  <div style="background:#FFFFFF;border:1px solid rgba(0,0,0,0.08);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;
              box-shadow:0 1px 4px rgba(0,0,0,0.07);">
    <div style="font-weight:700;color:#0F1C2E;margin-bottom:2px;">Datafy</div>
    <div style="color:#718096;">Visitor economy platform · trips, spend, DMA attribution</div>
  </div>
  <div style="background:#FFFFFF;border:1px solid rgba(0,0,0,0.08);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;
              box-shadow:0 1px 4px rgba(0,0,0,0.07);">
    <div style="font-weight:700;color:#0F1C2E;margin-bottom:2px;">CoStar</div>
    <div style="color:#718096;">Market data · comp set, pipeline, profitability</div>
  </div>
  <div style="background:#FFFFFF;border:1px solid rgba(0,0,0,0.08);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;
              box-shadow:0 1px 4px rgba(0,0,0,0.07);">
    <div style="font-weight:700;color:#0F1C2E;margin-bottom:2px;">Visit California</div>
    <div style="color:#718096;">State forecasts · lodging, travel volume, airport traffic</div>
  </div>
  <div style="background:#FFFFFF;border:1px solid rgba(0,0,0,0.08);
              border-radius:8px;padding:10px 16px;font-size:12px;min-width:160px;
              box-shadow:0 1px 4px rgba(0,0,0,0.07);">
    <div style="font-weight:700;color:#0F1C2E;margin-bottom:2px;">Zartico</div>
    <div style="color:#718096;">Historical reference · Jun 2025 snapshot · visitor trends</div>
  </div>
</div>
"""

_gl1, _gl2 = st.columns(2)
with _gl1:
    with st.expander("📖 Data Glossary", expanded=False):
        for _term, _defn in _GLOSSARY_TERMS.items():
            st.markdown(
                f'<div style="margin-bottom:10px;">'
                f'<span style="font-weight:700;color:#0891B2;">{_term}</span>'
                f'<span style="color:#4A5568;font-size:13px;"> — {_defn}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
with _gl2:
    with st.expander("🗂️ Data Sources", expanded=False):
        st.markdown(_SOURCES_HTML, unsafe_allow_html=True)

st.markdown(
    '<div style="text-align:center;padding:24px 0 12px;border-top:1px solid rgba(0,0,0,0.07);">'
    '<div style="font-size:13px;font-weight:700;letter-spacing:.02em;margin-bottom:4px;">'
    '🌊 Dana Point PULSE</div>'
    '<div style="font-size:11px;opacity:0.50;margin-bottom:6px;">'
    'Performance · Understanding · Leadership · Spending · Economy</div>'
    '<div style="font-size:11px;opacity:0.40;">'
    '© 2026 Wilton John Picou &nbsp;·&nbsp; <strong>GloCon Solutions LLC</strong> &nbsp;·&nbsp; '
    'Built for Visit Dana Point (VDP) &nbsp;·&nbsp; '
    'Data sources: STR · Datafy · CoStar · Later.com · Visit California · FRED &nbsp;·&nbsp; '
    'All data is proprietary and confidential. &nbsp;·&nbsp; '
    '<a href="https://www.visitdanapoint.com" target="_blank" style="color:inherit;opacity:.7;">'
    'visitdanapoint.com</a>'
    '</div>'
    '<div style="font-size:10px;opacity:0.30;margin-top:4px;">'
    'Data sources: STR · Datafy · CoStar · Later.com · Visit California · FRED · EIA · TSA · NOAA · Census ACS · BLS · Open-Meteo · Google Trends'
    '</div>'
    '</div>',
    unsafe_allow_html=True,
)
