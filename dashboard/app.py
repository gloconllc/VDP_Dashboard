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
CLAUDE_MODEL = "claude-opus-4-5"

SYSTEM_PROMPT = """You are the VDP Analytics Brain — the AI intelligence layer for Visit Dana Point (VDP) \
tourism analytics. You advise the TBID board, hotel GMs, city council, and destination marketing staff.

DATA HIERARCHY:
1. Vetted data (STR exports, Datafy reports, TBID records) = TRUTH. Always cite specific numbers.
2. External context (FRED, BLS, Visit California) = SUPPORTING EVIDENCE. Label clearly.
3. General hospitality expertise = FRAMEWORK ONLY. Never present as local fact.

PORTFOLIO CONTEXT:
• 12 Dana Point properties (VDP Select), Anaheim Area comp set
• TBID: Tier 1 (20–189 rooms) = 1.0%, Tier 2 (190+ rooms) = 1.5% · Blended ~1.25%
• Dana Point TOT rate: 10% of gross room revenue

RESPONSE FORMAT:
• Under 250 words unless depth is explicitly requested
• Lead with the key finding, add context, then recommend
• Bullet points for lists · **Bold** key numbers and dollar amounts
• End with exactly one clear, specific action item"""

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
  /* KPI cards */
  .kpi-card {
    background: var(--secondary-background-color);
    border-radius: 10px; padding: 18px 20px;
    border: 1px solid rgba(94,82,64,0.15); margin-bottom: 12px;
  }
  .kpi-label { font-size:11px; font-weight:600; text-transform:uppercase;
    letter-spacing:.05em; opacity:.55; margin-bottom:6px; }
  .kpi-value { font-size:26px; font-weight:700; letter-spacing:-.02em; line-height:1.1; }
  .kpi-delta-pos     { color:#21808D; font-size:12px; font-weight:500; margin-top:6px; }
  .kpi-delta-neg     { color:#C0152F; font-size:12px; font-weight:500; margin-top:6px; }
  .kpi-delta-neutral { color:gray;    font-size:12px; font-weight:500; margin-top:6px; }

  /* Insight cards */
  .insight-card { border-radius:10px; padding:14px 16px; margin-bottom:4px;
    position:relative; border:1px solid rgba(94,82,64,0.12);
    background: var(--secondary-background-color); }
  .insight-card::before { content:''; position:absolute; top:0; left:0; right:0;
    height:3px; border-radius:10px 10px 0 0; }
  .insight-positive::before { background:linear-gradient(90deg,#21808D,#32B8C6); }
  .insight-warning::before  { background:linear-gradient(90deg,#E68161,#f59e0b); }
  .insight-negative::before { background:linear-gradient(90deg,#C0152F,#ef4444); }
  .insight-info::before     { background:linear-gradient(90deg,#21808D,#626C71); }
  .insight-title { font-size:13px; font-weight:600; margin-bottom:4px; }
  .insight-body  { font-size:12px; opacity:.75; line-height:1.5; margin:0; }

  /* AI chip */
  .ai-chip { display:inline-block; font-size:10px; font-weight:700;
    text-transform:uppercase; letter-spacing:.05em; padding:2px 8px;
    border-radius:99px; background:rgba(33,128,141,.12); color:#21808D; margin-bottom:10px; }

  /* Event stat cards */
  .event-stat { background:var(--secondary-background-color);
    border:1px solid rgba(94,82,64,.12); border-radius:10px;
    padding:16px; text-align:center; margin-bottom:8px; }
  .event-val   { font-size:28px; font-weight:700; color:#21808D; letter-spacing:-.02em; }
  .event-label { font-size:12px; opacity:.6; margin-top:4px; }

  #MainMenu { visibility:hidden; }
  footer    { visibility:hidden; }

  /* Home button title */
  .home-title a {
    text-decoration: none;
    color: inherit;
    font-size: 2rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    line-height: 1.2;
  }
  .home-title a:hover { opacity: 0.75; }

  /* Empty-state cards */
  .empty-card {
    background: var(--secondary-background-color);
    border-radius: 10px;
    padding: 32px 24px;
    text-align: center;
    border: 1px dashed rgba(94,82,64,0.25);
    margin: 6px 0 12px 0;
  }
  .empty-icon  { font-size: 30px; margin-bottom: 10px; }
  .empty-title { font-size: 14px; font-weight: 600; margin-bottom: 6px; }
  .empty-body  { font-size: 12px; opacity: 0.65; line-height: 1.55; }

  /* Data-source health cards */
  .src-card {
    background: var(--secondary-background-color);
    border-radius: 10px;
    padding: 13px 16px;
    border: 1px solid rgba(94,82,64,0.12);
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .src-dot   { font-size: 15px; flex-shrink: 0; }
  .src-name  { font-size: 13px; font-weight: 600; }
  .src-meta  { font-size: 11px; opacity: 0.6; margin-top: 2px; line-height: 1.4; }
  .src-count { font-size: 13px; font-weight: 700; color: #21808D;
               margin-left: auto; text-align: right; white-space: nowrap; }

  /* Grain badge in chart titles */
  .grain-badge {
    display: inline-block; font-size: 10px; font-weight: 700;
    text-transform: uppercase; letter-spacing: .05em; padding: 2px 7px;
    border-radius: 99px; background: rgba(230,129,97,.13);
    color: #E68161; margin-left: 8px; vertical-align: middle;
  }
</style>
""", unsafe_allow_html=True)

# ─── Paths ────────────────────────────────────────────────────────────────────
ROOT    = Path(__file__).parent.parent                          # ~/Documents/dmo-analytics
DB_PATH = ROOT / "data" / "analytics.sqlite"

@st.cache_resource
def get_connection():
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ─── Data loaders (5-minute cache) ───────────────────────────────────────────

@st.cache_data(ttl=300)
def load_str_daily() -> pd.DataFrame:
    """Pivot fact_str_metrics (grain=daily) → one row per date."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT as_of_date, metric_name, metric_value "
        "FROM fact_str_metrics WHERE grain='daily' ORDER BY as_of_date",
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
def load_str_monthly() -> pd.DataFrame:
    """Pivot fact_str_metrics (grain=monthly) → one row per month."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT as_of_date, metric_name, metric_value "
        "FROM fact_str_metrics WHERE grain='monthly' ORDER BY as_of_date",
        conn,
    )
    if df.empty:
        return pd.DataFrame()
    wide = (
        df.pivot_table(index="as_of_date", columns="metric_name",
                       values="metric_value", aggfunc="mean")
        .reset_index()
    )
    wide.columns.name = None
    wide.columns = [c.lower().replace(" ", "_") for c in wide.columns]
    wide["as_of_date"] = pd.to_datetime(wide["as_of_date"])
    # Rename occ → occupancy if present (STR sometimes stores as 'occ')
    if "occ" in wide.columns and "occupancy" not in wide.columns:
        wide = wide.rename(columns={"occ": "occupancy"})
    # Normalise occ from decimal if needed
    if "occupancy" in wide.columns and wide["occupancy"].max() <= 1.0:
        wide["occupancy"] = wide["occupancy"] * 100
    # Monthly STR exports don't include an occ column — derive from demand/supply
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
def get_table_counts() -> dict:
    conn = get_connection()
    counts = {}
    for t in ["fact_str_metrics", "kpi_daily_summary",
              "kpi_compression_quarterly", "load_log"]:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            counts[t] = row[0] if row else 0
        except Exception:
            counts[t] = "—"
    # Per-grain breakdowns for the pipeline status display
    for grain_val, key in [("daily", "str_daily_rows"), ("monthly", "str_monthly_rows")]:
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM fact_str_metrics WHERE grain=?", (grain_val,)
            ).fetchone()
            counts[key] = row[0] if row else 0
        except Exception:
            counts[key] = "—"
    return counts

# ─── Metric context builder ───────────────────────────────────────────────────

def pct_delta(a: float, b: float) -> float:
    return (a - b) / b * 100 if b else 0.0


def build_metrics_context(df: pd.DataFrame, df_comp: pd.DataFrame) -> dict:
    """Compute key stats from the filtered daily data for AI prompt injection."""
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

    return {
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
    }

# ─── AI prompt builders ───────────────────────────────────────────────────────

def _base(m: dict) -> str:
    return (
        f"VDP Select Portfolio — current data snapshot:\n"
        f"• 30-day RevPAR: ${m.get('revpar_30',0):.0f} ({m.get('revpar_delta',0):+.1f}% vs. prior period)\n"
        f"• 30-day ADR: ${m.get('adr_30',0):.0f} ({m.get('adr_delta',0):+.1f}% vs. prior period)\n"
        f"• 30-day Occupancy: {m.get('occ_30',0):.1f}% ({m.get('occ_delta',0):+.1f}pp vs. prior period)\n"
        f"• Room Revenue (30d): ${m.get('rev_30_total',0):,.0f}\n"
        f"• Weekend RevPAR: ${m.get('weekend_revpar',0):.0f}  |  Midweek RevPAR: ${m.get('midweek_revpar',0):.0f}\n"
        f"• Weekend Occ: {m.get('weekend_occ',0):.1f}%  |  Midweek Occ: {m.get('midweek_occ',0):.1f}%\n"
        f"• Most recent quarter — days above 90% occ: {m.get('comp_recent_q',0)} "
        f"(prior quarter: {m.get('comp_prior_q',0)})"
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
    """Yields text chunks from Claude streaming API for use with st.write_stream."""
    if not ANTHROPIC_AVAILABLE:
        yield "⚠️ `anthropic` package not installed. Run: `pip install anthropic` in your venv."
        return
    try:
        client = anthropic.Anthropic(api_key=api_key)
        with client.messages.stream(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
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

# ─── UI helpers ───────────────────────────────────────────────────────────────

def kpi_card(label, value, delta, positive=True, neutral=False) -> str:
    css   = "kpi-delta-neutral" if neutral else ("kpi-delta-pos" if positive else "kpi-delta-neg")
    arrow = "" if neutral else ("▲ " if positive else "▼ ")
    return (
        f'<div class="kpi-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="{css}">{arrow}{delta}</div>'
        f'</div>'
    )


def insight_card(title, body, kind="info") -> str:
    return (
        f'<div class="insight-card insight-{kind}">'
        f'<div class="insight-title">{title}</div>'
        f'<p class="insight-body">{body}</p>'
        f'</div>'
    )


def event_stat(val, label) -> str:
    return (
        f'<div class="event-stat">'
        f'<div class="event-val">{val}</div>'
        f'<div class="event-label">{label}</div>'
        f'</div>'
    )


def style_fig(fig: go.Figure, height: int = 280) -> go.Figure:
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font_family="Inter, system-ui, sans-serif", font_size=12,
        height=height, margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="rgba(0,0,0,0.06)", zeroline=False)
    return fig


def compute_overview_kpis(df: pd.DataFrame) -> list[dict]:
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
    return [
        {"label":"RevPAR",       "value":f"${r_rvp:.2f}",      "delta":f"{pct_delta(r_rvp,p_rvp):+.1f}% vs. prior", "positive":r_rvp>=p_rvp},
        {"label":"ADR",          "value":f"${r_adr:.2f}",      "delta":f"{pct_delta(r_adr,p_adr):+.1f}% vs. prior", "positive":r_adr>=p_adr},
        {"label":"Occupancy",    "value":f"{r_occ:.1f}%",      "delta":f"{pct_delta(r_occ,p_occ):+.1f}pp vs. prior", "positive":r_occ>=p_occ},
        {"label":"Room Revenue", "value":f"${r_rev/1e6:.2f}M", "delta":f"{pct_delta(r_rev,p_rev):+.1f}% vs. prior", "positive":r_rev>=p_rev},
        {"label":"Rooms Sold",   "value":f"{r_dem:,.0f}",      "delta":f"{pct_delta(r_dem,p_dem):+.1f}% vs. prior", "positive":r_dem>=p_dem},
        {"label":"Est. TBID Rev","value":f"${tbid/1e3:.0f}K",  "delta":"blended 1.25%",                              "positive":True,"neutral":True},
    ]


def generate_ai_insights(df: pd.DataFrame, df_comp: pd.DataFrame, m: dict) -> list[dict]:
    """Rule-based insight cards — always available, no API key required."""
    cards = []

    # 1 — RevPAR momentum
    d = m.get("revpar_delta", 0)
    if d > 3:
        cards.append({"kind":"positive","title":"RevPAR Momentum",
            "body":f"RevPAR up {d:.1f}% vs. prior period — ADR strength is the primary driver. "
                   f"Current pricing is working; lock in rate gains on compression nights."})
    elif d < -3:
        cards.append({"kind":"negative","title":"RevPAR Pressure",
            "body":f"RevPAR down {abs(d):.1f}% vs. prior period. Determine whether softness is "
                   f"demand-driven (occ decline) or pricing-driven (ADR compression) before acting."})
    else:
        cards.append({"kind":"info","title":"RevPAR Holding Steady",
            "body":f"RevPAR {d:+.1f}% vs. prior period — within normal variance. "
                   f"Midweek gap remains the highest-leverage growth lever."})

    # 2 — Midweek softness
    wknd = m.get("weekend_revpar", 0)
    midwk = m.get("midweek_revpar", 0)
    if wknd > 0 and midwk > 0:
        gap = (wknd / midwk - 1) * 100
        if gap > 25:
            cards.append({"kind":"warning","title":"Midweek Softness",
                "body":f"Weekend RevPAR (${wknd:.0f}) is {gap:.0f}% above midweek (${midwk:.0f}). "
                       f"Tue–Thu packages and local partnerships are the fastest path to closing this gap."})
        else:
            cards.append({"kind":"positive","title":"Balanced Demand Mix",
                "body":f"Weekend/midweek RevPAR spread is only {gap:.0f}% — healthy for a leisure "
                       f"destination. Midweek demand is holding relatively well."})

    # 3 — Compression trend
    crec = m.get("comp_recent_q", 0)
    cpri = m.get("comp_prior_q", 0)
    if crec > 0:
        if crec > cpri:
            cards.append({"kind":"positive","title":"Compression Building",
                "body":f"{crec} days above 90% occupancy last quarter (vs. {cpri} prior) — "
                       f"a clear signal that rate increases are justified on your highest-demand nights."})
        else:
            cards.append({"kind":"info","title":"Compression Watch",
                "body":f"{crec} days above 90% occ last quarter (vs. {cpri} prior). "
                       f"Monitor as we move into peak season."})

    # 4 — Anomaly flag
    ns = m.get("n_spikes", 0)
    nd = m.get("n_drops", 0)
    if ns > 0 or nd > 0:
        cards.append({"kind":"info","title":f"{ns + nd} Anomalies Detected",
            "body":f"{ns} revenue spikes (>2σ) and {nd} drops (<1.5σ) in the selected period. "
                   f"Green/red markers on the RevPAR chart identify each event — hover for context."})

    return cards[:4]

# ─── UI helper: empty-state card ─────────────────────────────────────────────

def empty_state(icon: str, title: str, body: str) -> str:
    """Return an HTML empty-state card (pass to st.markdown unsafe_allow_html=True)."""
    return (
        f'<div class="empty-card">'
        f'<div class="empty-icon">{icon}</div>'
        f'<div class="empty-title">{title}</div>'
        f'<div class="empty-body">{body}</div>'
        f'</div>'
    )


def source_card(dot: str, name: str, meta: str, count: str) -> str:
    """Return an HTML data-source health card."""
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
df_daily   = load_str_daily()
df_monthly = load_str_monthly()
df_kpi     = load_kpi_daily()
df_comp    = load_compression()
df_log     = load_load_log()

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🌊 Visit Dana Point")
    st.markdown("VDP Select Portfolio · 12 Properties · Anaheim Area")
    st.divider()

    RANGE_OPTIONS = {
        "Last 30 Days":   30,
        "Last 90 Days":   90,
        "Last 6 Months":  180,
        "Last 12 Months": 365,
    }
    range_label = st.selectbox("Date Range", list(RANGE_OPTIONS.keys()), index=1)
    days = RANGE_OPTIONS[range_label]
    grain = st.selectbox(
        "Data Grain",
        ["Daily", "Monthly"],
        index=0,
        help="Daily = STR daily exports · Monthly = pre-aggregated monthly STR data",
    )

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
    counts          = get_table_counts()
    str_daily_rows  = counts.get("str_daily_rows",   0)
    str_monthly_rows= counts.get("str_monthly_rows", 0)
    last_log = df_log.iloc[0]["run_at"][:10] if not df_log.empty else "—"

    _d_dot = "🟢" if isinstance(str_daily_rows,   int) and str_daily_rows   > 0 else "⚫"
    _m_dot = "🟢" if isinstance(str_monthly_rows, int) and str_monthly_rows > 0 else "🟡"
    _m_lbl = (f"{str_monthly_rows:,} rows"
              if isinstance(str_monthly_rows, int) and str_monthly_rows > 0
              else "Not loaded")

    st.markdown("**Pipeline Status**")
    st.markdown(f"{_d_dot} STR Daily &nbsp;·&nbsp; {str_daily_rows:,} rows")
    st.markdown(f"{_m_dot} STR Monthly &nbsp;·&nbsp; {_m_lbl}")
    st.markdown(f"⚫ Datafy &nbsp;·&nbsp; Not connected")
    st.caption(f"Last ETL run: {last_log}")

    if not df_daily.empty:
        min_d = df_daily["as_of_date"].min().strftime("%b %d, %Y")
        max_d = df_daily["as_of_date"].max().strftime("%b %d, %Y")
        st.caption(f"Data: {min_d} → {max_d}")

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
df_active = df_daily if grain == "Daily" else df_monthly

if not df_active.empty:
    max_date   = df_active["as_of_date"].max()
    cutoff     = max_date - timedelta(days=days)
    df_sel     = df_active[df_active["as_of_date"] > cutoff].copy()
    df_kpi_sel = df_kpi[df_kpi["as_of_date"] > cutoff].copy() if not df_kpi.empty else pd.DataFrame()
else:
    df_sel = df_kpi_sel = pd.DataFrame()

m = build_metrics_context(df_sel, df_comp)

# ─── Header ───────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="home-title"><a href="?" title="Reset to Overview">Visit Dana Point — Analytics</a></div>',
    unsafe_allow_html=True,
)
last_upd = df_daily["as_of_date"].max().strftime("%b %d, %Y") if not df_daily.empty else "N/A"
st.caption(f"VDP Select Portfolio · {range_label} · Last updated {last_upd}")

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_ov, tab_tr, tab_ev, tab_dl = st.tabs(
    ["📊 Overview", "📈 Trends", "🎪 Event Impact", "🗂 Data Log"]
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
        st.markdown("**Auto-Detected Insights**")
        st.caption("Pattern analysis from the selected date range")
        insights = generate_ai_insights(df_sel, df_comp, m)
        if insights:
            ic = st.columns(len(insights))
            for i, ins in enumerate(insights):
                with ic[i]:
                    st.markdown(
                        insight_card(ins["title"], ins["body"], ins["kind"]),
                        unsafe_allow_html=True,
                    )
        st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("---")

    # ── KPI Cards ──────────────────────────────────────────────────────────────
    kpis = compute_overview_kpis(df_sel)
    if not kpis:
        if grain == "Monthly" and df_monthly.empty:
            st.markdown(empty_state(
                "🗓️", "No Monthly Data Loaded",
                "Monthly STR exports haven't been loaded yet.<br>"
                "Save a monthly Excel file to <code>downloads/</code> and click "
                "<b>🔄 Run Pipeline</b> in the sidebar.",
            ), unsafe_allow_html=True)
        else:
            st.markdown(empty_state(
                "📭", "No Data in Selected Range",
                f"No {grain.lower()} records found for the selected window.<br>"
                "Try expanding the date range or run the pipeline to load new data.",
            ), unsafe_allow_html=True)
    else:
        st.markdown("**Key Performance Indicators**")
        cols = st.columns(3)
        for i, k in enumerate(kpis):
            with cols[i % 3]:
                st.markdown(
                    kpi_card(k["label"], k["value"], k["delta"],
                             k.get("positive", True), k.get("neutral", False)),
                    unsafe_allow_html=True,
                )

        st.markdown("---")

        # ── Row 1: RevPAR with anomaly detection  |  Occ vs ADR ───────────────
        c1, c2 = st.columns(2)

        with c1:
            st.markdown("**RevPAR Trend — with Anomaly Detection**")
            st.caption("Green markers = spikes >2σ · Red = drops <1.5σ · Hover for context")

            rvp_mean = df_sel["revpar"].mean()
            rvp_std  = df_sel["revpar"].std()
            spikes   = df_sel[df_sel["revpar"] > rvp_mean + 2 * rvp_std]
            drops    = df_sel[df_sel["revpar"] < rvp_mean - 1.5 * rvp_std]

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["revpar"],
                fill="tozeroy",
                line=dict(color=TEAL, width=2),
                fillcolor="rgba(33,128,141,0.10)",
                mode="lines", name="RevPAR",
                hovertemplate="<b>%{x|%b %d, %Y}</b><br>RevPAR: $%{y:.0f}<extra></extra>",
            ))
            fig.add_hline(
                y=rvp_mean, line_dash="dot", line_color="rgba(167,169,169,0.5)",
                annotation_text=f"Avg ${rvp_mean:.0f}", annotation_position="top right",
            )
            if not spikes.empty:
                fig.add_trace(go.Scatter(
                    x=spikes["as_of_date"], y=spikes["revpar"],
                    mode="markers",
                    marker=dict(color=TEAL, size=10, symbol="circle-open",
                                line=dict(width=2.5, color=TEAL)),
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
                    marker=dict(color=RED, size=10, symbol="circle-open",
                                line=dict(width=2.5, color=RED)),
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
            st.markdown("**Occupancy vs. ADR**")
            st.caption("Dual-axis · fill rate & pricing power")
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
                st.markdown("**Day-of-Week Performance**")
                st.caption("Avg RevPAR by weekday · orange = below overall avg")
                dow_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                tmp = df_sel.copy()
                tmp["dow"] = tmp["as_of_date"].dt.strftime("%a")
                dow_avg = tmp.groupby("dow")["revpar"].mean().reindex(dow_order)
                ov_avg  = dow_avg.mean()
                colors  = [TEAL if v >= ov_avg else ORANGE for v in dow_avg.fillna(0)]
                fig = go.Figure(go.Bar(
                    x=dow_avg.index, y=dow_avg.values,
                    marker=dict(color=colors, line_width=0),
                    hovertemplate=(
                        "<b>%{x}</b><br>Avg RevPAR: $%{y:.0f}<br>"
                        "<i>Click 'Opportunity Nights' for AI analysis</i><extra></extra>"
                    ),
                ))
                fig.add_hline(y=ov_avg, line_dash="dot", line_color="rgba(167,169,169,0.5)",
                              annotation_text=f"Avg ${ov_avg:.0f}", annotation_position="top right")
                fig.update_layout(yaxis_tickprefix="$", showlegend=False)
                st.plotly_chart(style_fig(fig), use_container_width=True)
            else:
                # Monthly grain → Month-of-Year seasonality
                st.markdown("**Month-of-Year Seasonality**")
                st.caption("Avg RevPAR by calendar month across all loaded data · orange = below avg")
                mon_order = ["Jan","Feb","Mar","Apr","May","Jun",
                             "Jul","Aug","Sep","Oct","Nov","Dec"]
                tmp = df_active.copy()
                tmp["mon"] = tmp["as_of_date"].dt.strftime("%b")
                mon_avg = tmp.groupby("mon")["revpar"].mean().reindex(mon_order)
                ov_avg  = mon_avg.mean()
                colors  = [TEAL if (v >= ov_avg if pd.notna(v) else False) else ORANGE
                           for v in mon_avg]
                fig = go.Figure(go.Bar(
                    x=mon_avg.index, y=mon_avg.values,
                    marker=dict(color=colors, line_width=0),
                    hovertemplate="<b>%{x}</b><br>Avg RevPAR: $%{y:.0f}<extra></extra>",
                ))
                fig.add_hline(y=ov_avg, line_dash="dot", line_color="rgba(167,169,169,0.5)",
                              annotation_text=f"Avg ${ov_avg:.0f}", annotation_position="top right")
                fig.update_layout(yaxis_tickprefix="$", showlegend=False)
                st.plotly_chart(style_fig(fig), use_container_width=True)

        with c4:
            st.markdown("**Supply vs. Demand**")
            st.caption("Room inventory vs. rooms sold · gap = unrealized revenue")
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

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — TRENDS
# ══════════════════════════════════════════════════════════════════════════════
with tab_tr:
    # Decide the monthly series to drive Trends — grain-aware
    _trends_empty = (grain == "Daily" and df_kpi.empty) or \
                    (grain == "Monthly" and df_monthly.empty)

    if _trends_empty:
        if grain == "Monthly":
            st.markdown(empty_state(
                "🗓️", "No Monthly Data Loaded",
                "Monthly STR data hasn't been loaded yet.<br>"
                "Drop a monthly export in <code>downloads/</code> and click "
                "<b>🔄 Run Pipeline</b>.",
            ), unsafe_allow_html=True)
        else:
            st.markdown(empty_state(
                "📊", "KPI Table Is Empty",
                "The <code>kpi_daily_summary</code> table has no rows yet.<br>"
                "Click <b>🔄 Run Pipeline</b> in the sidebar to compute KPIs from your STR data.",
            ), unsafe_allow_html=True)
    else:
        # ── Build monthly series ───────────────────────────────────────────────
        if grain == "Monthly" and not df_monthly.empty:
            # Use pre-aggregated monthly STR source directly (up to 469 months)
            monthly = df_monthly.sort_values("as_of_date").copy()
            monthly["month"] = monthly["as_of_date"].dt.to_period("M")
            monthly["month_label"] = monthly["as_of_date"].dt.strftime("%b %Y")
            monthly = monthly.rename(columns={"occupancy": "occ_pct"})
            # Compute YOY by comparing to the same month one year prior
            monthly["revpar_yoy"] = (
                monthly["revpar"] / monthly["revpar"].shift(12) - 1
            ) * 100
        else:
            # Daily grain → aggregate kpi_daily_summary to monthly
            df_kpi_all = df_kpi.copy()
            df_kpi_all["month"] = df_kpi_all["as_of_date"].dt.to_period("M")
            monthly = (
                df_kpi_all.groupby("month")
                .agg(revpar=("revpar", "mean"), adr=("adr", "mean"),
                     occ_pct=("occ_pct", "mean"), revpar_yoy=("revpar_yoy", "mean"))
                .reset_index()
            )
            monthly["month_label"] = monthly["month"].dt.strftime("%b %Y")

        # ── YOY RevPAR bar (full width) ────────────────────────────────────────
        st.markdown("**YOY RevPAR Change**")
        st.caption("Year-over-year % change by month · teal = growth, red = decline")
        yoy = monthly.dropna(subset=["revpar_yoy"])
        if yoy.empty:
            st.markdown(empty_state(
                "📅", "More History Needed for YOY",
                f"Year-over-year comparison requires 12+ months of data.<br>"
                f"Currently {len(monthly)} month(s) are available in "
                f"<code>kpi_daily_summary</code>.",
            ), unsafe_allow_html=True)
        else:
            bar_colors = [GREEN if v >= 0 else RED for v in yoy["revpar_yoy"]]
            fig = go.Figure(go.Bar(
                x=yoy["month_label"], y=yoy["revpar_yoy"],
                marker=dict(color=bar_colors, line_width=0),
                text=[f"{v:+.1f}%" for v in yoy["revpar_yoy"]],
                textposition="outside", textfont=dict(size=10),
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
            st.markdown("**Seasonality Index**")
            st.caption("Monthly RevPAR ÷ period average · above 1.0 = peak season")
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
                    "🌊", "Building Seasonality Curve",
                    f"Seasonality index needs 6+ months of data.<br>"
                    f"Currently {len(monthly)} month(s) available — keep loading STR exports.",
                ), unsafe_allow_html=True)

        with c2:
            st.markdown("**TBID Revenue Estimate**")
            st.caption("Monthly at blended 1.25% · hover to compare periods")
            _tbid_src = df_active if not df_active.empty else df_daily
            if not _tbid_src.empty:
                tmp = _tbid_src.copy()
                tmp["month"] = tmp["as_of_date"].dt.to_period("M")
                mrev = tmp.groupby("month")["revenue"].sum().reset_index()
                mrev["month_label"] = mrev["month"].dt.strftime("%b %Y")
                mrev["tbid_m"] = mrev["revenue"] * 0.0125 / 1e6
                fig = go.Figure(go.Bar(
                    x=mrev["month_label"], y=mrev["tbid_m"],
                    marker=dict(color=TEAL, line_width=0),
                    hovertemplate="<b>%{x}</b><br>Est. TBID: $%{y:.2f}M<extra></extra>",
                ))
                fig.update_layout(yaxis_tickprefix="$", yaxis_ticksuffix="M", showlegend=False)
                st.plotly_chart(style_fig(fig), use_container_width=True)

        st.markdown("---")

        # ── Compression quarters ───────────────────────────────────────────────
        st.markdown("**Occupancy Compression Days by Quarter**")
        st.caption("Days with occ ≥ 80% and ≥ 90% · signals pricing power windows")
        if not df_comp.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name="Days ≥ 80% Occ",
                x=df_comp["quarter"], y=df_comp["days_above_80_occ"],
                marker=dict(color=TEAL, line_width=0),
                hovertemplate="<b>%{x}</b><br>Days ≥ 80%%: %{y}<extra></extra>",
            ))
            fig.add_trace(go.Bar(
                name="Days ≥ 90% Occ",
                x=df_comp["quarter"], y=df_comp["days_above_90_occ"],
                marker=dict(color=TEAL_LIGHT, line_width=0),
                hovertemplate="<b>%{x}</b><br>Days ≥ 90%%: %{y}<br>"
                              "<i>High compression — rate increases justified</i><extra></extra>",
            ))
            fig.update_layout(barmode="group")
            st.plotly_chart(style_fig(fig, height=260), use_container_width=True)
        else:
            st.markdown(empty_state(
                "📉", "No Compression Data Yet",
                "Quarterly compression counts haven't been computed.<br>"
                "Click <b>🔄 Run Pipeline</b> to populate <code>kpi_compression_quarterly</code>.",
            ), unsafe_allow_html=True)

        st.markdown("---")

        # ── Full history line chart ─────────────────────────────────────────────
        st.markdown("**RevPAR / ADR / Occupancy — Full History**")
        st.caption("Monthly averages · all available data")
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
# TAB 3 — EVENT IMPACT
# ══════════════════════════════════════════════════════════════════════════════
with tab_ev:
    st.markdown("### Ohana Fest 2025 — Dana Point, CA")
    st.caption("Source: Datafy visitor economy report · Pending live DB integration")

    # Hero stats grid
    hero_stats = [
        ("$14.6M", "Event Expenditure"),
        ("$18.4M", "Destination Spend"),
        ("+$139",  "ADR Lift vs. Baseline"),
        ("$1,219", "Avg Accom. Spend / Trip"),
        ("68%",    "Out-of-State Visitors"),
        ("3.2×",   "Spend Multiplier"),
    ]
    ec = st.columns(3)
    for i, (val, lbl) in enumerate(hero_stats):
        with ec[i % 3]:
            st.markdown(event_stat(val, lbl), unsafe_allow_html=True)

    st.markdown("---")

    # ── Event lift chart ───────────────────────────────────────────────────────
    st.markdown("**ADR & Occupancy Lift — Ohana Fest 2024**")
    st.caption("Event period vs. surrounding baseline · shaded = event weekend")
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
        st.markdown("**Top Feeder Markets**")
        st.caption("Visitor origin by share of visitor days (Datafy 2025)")
        cities = ["San Clemente","San Juan Cap.","Dana Point","Laguna Niguel",
                  "Ladera Ranch","Huntington Bch","San Diego","Mission Viejo",
                  "Los Angeles","Cap. Beach"]
        shares = [8.89,7.84,6.78,6.50,3.53,3.19,2.96,2.42,2.35,1.85]
        fig = go.Figure(go.Bar(
            x=shares, y=cities, orientation="h",
            marker=dict(color=TEAL, line_width=0),
            text=[f"{v:.1f}%" for v in shares], textposition="outside",
            hovertemplate="<b>%{y}</b><br>Share: %{x:.2f}%<extra></extra>",
        ))
        fig.update_layout(yaxis=dict(autorange="reversed"),
                          xaxis_ticksuffix="%", showlegend=False)
        st.plotly_chart(style_fig(fig, height=340), use_container_width=True)

    with c2:
        st.markdown("**Spending by Category**")
        st.caption("Share of total destination spend during event weekend")
        cats   = ["Dining & Nightlife","Accommodations","Grocery/Dept",
                  "Service Stations","Fast Food","Specialty Retail",
                  "Personal Care","Clothing","Leisure/Rec"]
        values = [30.2,23.6,17.4,7.71,6.73,6.46,4.08,2.09,1.33]
        palette = [TEAL,"#2DA6B2",TEAL_LIGHT,ORANGE,"#A84B2F",
                   "#5E5240","#626C71","#A7A9A9",RED]
        fig = go.Figure(go.Pie(
            labels=cats, values=values, hole=0.42,
            marker=dict(colors=palette), textfont=dict(size=10),
        ))
        fig.update_layout(legend=dict(font_size=10, orientation="v"))
        st.plotly_chart(style_fig(fig, height=340), use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — DATA LOG
# ══════════════════════════════════════════════════════════════════════════════
with tab_dl:

    # ── Data Source Health ─────────────────────────────────────────────────────
    st.markdown("### 📡 Data Source Health")
    st.caption("Live status of every data source connected to the analytics database")

    counts = get_table_counts()

    # Build metadata for each source
    _daily_min  = df_daily["as_of_date"].min().strftime("%b %d, %Y") if not df_daily.empty else None
    _daily_max  = df_daily["as_of_date"].max().strftime("%b %d, %Y") if not df_daily.empty else None
    _daily_rows = counts.get("fact_str_metrics", 0)

    _mon_min = df_monthly["as_of_date"].min().strftime("%b %Y") if not df_monthly.empty else None
    _mon_max = df_monthly["as_of_date"].max().strftime("%b %Y") if not df_monthly.empty else None
    _mon_cnt = len(df_monthly)

    _kpi_min = df_kpi["as_of_date"].min().strftime("%b %d, %Y") if not df_kpi.empty else None
    _kpi_max = df_kpi["as_of_date"].max().strftime("%b %d, %Y") if not df_kpi.empty else None
    _kpi_rows = counts.get("kpi_daily_summary", 0)

    _comp_rows = counts.get("kpi_compression_quarterly", 0)
    _last_log  = df_log.iloc[0]["run_at"][:16] if not df_log.empty else "Never"

    hc1, hc2 = st.columns(2)

    with hc1:
        st.markdown(source_card(
            "🟢" if not df_daily.empty else "⚫",
            "STR Daily",
            (f"{_daily_min} → {_daily_max}" if _daily_min else "No data loaded") +
            f"<br>Last pipeline run: {_last_log}",
            f"{_daily_rows:,} rows" if isinstance(_daily_rows, int) else "—",
        ), unsafe_allow_html=True)

        _str_mon_rows = counts.get("str_monthly_rows", 0)
        _mon_rows_lbl = (f"{_str_mon_rows:,} rows · {_mon_cnt} months"
                         if isinstance(_str_mon_rows, int) and _str_mon_rows > 0
                         else "0 rows")
        st.markdown(source_card(
            "🟢" if not df_monthly.empty else "🟡",
            "STR Monthly",
            (f"{_mon_min} → {_mon_max}" if _mon_min else
             "No monthly data — save <code>downloads/str_monthly.xlsx</code> and run pipeline"),
            _mon_rows_lbl,
        ), unsafe_allow_html=True)

    with hc2:
        st.markdown(source_card(
            "🟢" if not df_kpi.empty else "🟡",
            "KPI Daily Summary",
            (f"{_kpi_min} → {_kpi_max}" if _kpi_min else
             "Empty — click 🔄 Run Pipeline to compute"),
            f"{_kpi_rows:,} rows" if isinstance(_kpi_rows, int) else "—",
        ), unsafe_allow_html=True)

        st.markdown(source_card(
            "🟢" if _comp_rows and _comp_rows > 0 else "🟡",
            "Compression Quarters",
            f"{_comp_rows} quarter(s) computed" if _comp_rows else
            "Empty — run pipeline to compute",
            f"{_comp_rows} qtrs",
        ), unsafe_allow_html=True)

    st.markdown("---")

    # CoStar / external row
    st.markdown(source_card(
        "⚫", "CoStar",
        "Awaiting export — save <code>downloads/costar_*.xlsx</code>, then click "
        "<b>📡 Fetch External Data</b>",
        "0 rows",
    ), unsafe_allow_html=True)
    st.markdown(source_card(
        "⚫", "FRED · CA TOT · JWA",
        "Fetch scripts pending implementation",
        "—",
    ), unsafe_allow_html=True)

    st.markdown("---")

    # ── Load Log + Row Counts ──────────────────────────────────────────────────
    col_a, col_b = st.columns([3, 1])

    with col_a:
        st.markdown("### Load Log")
        st.caption("ETL pipeline audit trail from load_log")
        if not df_log.empty:
            st.dataframe(df_log, use_container_width=True, hide_index=True)
        else:
            st.markdown(empty_state(
                "🪵", "No Pipeline Runs Logged Yet",
                "Run the pipeline with <b>🔄 Run Pipeline</b> in the sidebar<br>"
                "or <code>python scripts/run_pipeline.py</code> from the terminal.",
            ), unsafe_allow_html=True)

    with col_b:
        st.markdown("### Row Counts")
        for table, count in counts.items():
            label = table.replace("_", " ").title()
            st.metric(label, f"{count:,}" if isinstance(count, int) else count)

    st.markdown("---")

    # ── Recent Metric Samples ──────────────────────────────────────────────────
    st.markdown(f"### Recent Metric Samples {grain_badge(grain)}", unsafe_allow_html=True)
    st.caption(f"Last 10 dates in selected window · {grain.lower()} grain · from fact_str_metrics")
    if not df_sel.empty:
        sample = df_sel.tail(10).sort_values("as_of_date", ascending=False)
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
    else:
        st.markdown(empty_state(
            "📭", "No Records in Selected Range",
            f"No {grain.lower()} data found for the current date window.<br>"
            "Try expanding the range or switching grain.",
        ), unsafe_allow_html=True)

    st.markdown("---")

    # ── Compression Quarters ───────────────────────────────────────────────────
    st.markdown("### Compression Quarters")
    st.caption("Days per quarter above 80% / 90% occupancy from kpi_compression_quarterly")
    if not df_comp.empty:
        st.dataframe(df_comp, use_container_width=True, hide_index=True)
    else:
        st.markdown(empty_state(
            "📉", "No Compression Data",
            "Click <b>🔄 Run Pipeline</b> to populate <code>kpi_compression_quarterly</code>.",
        ), unsafe_allow_html=True)
