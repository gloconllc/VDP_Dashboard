"""
Visit Dana Point — Analytics Dashboard
Streamlit app | Read-only connection to data/analytics.sqlite
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import timedelta

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Visit Dana Point — Analytics",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Brand colors ─────────────────────────────────────────────────────────────
TEAL        = "#21808D"
TEAL_LIGHT  = "#32B8C6"
ORANGE      = "#E68161"
RED         = "#C0152F"
GREEN       = TEAL          # teal = positive/success to match brand palette

# ─── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
  .kpi-card {
    background: var(--secondary-background-color);
    border-radius: 10px;
    padding: 18px 20px;
    border: 1px solid rgba(94,82,64,0.15);
    margin-bottom: 12px;
  }
  .kpi-label {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-color);
    opacity: 0.55;
    margin-bottom: 6px;
  }
  .kpi-value {
    font-size: 26px;
    font-weight: 700;
    letter-spacing: -0.02em;
    line-height: 1.1;
    color: var(--text-color);
  }
  .kpi-delta-pos     { color: #21808D; font-size: 12px; font-weight: 500; margin-top: 6px; }
  .kpi-delta-neg     { color: #C0152F; font-size: 12px; font-weight: 500; margin-top: 6px; }
  .kpi-delta-neutral { color: gray;    font-size: 12px; font-weight: 500; margin-top: 6px; }
  /* hide Streamlit branding */
  #MainMenu { visibility: hidden; }
  footer    { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ─── DB connection (read-only, cached for session) ────────────────────────────
DB_PATH = Path(__file__).parent.parent / "data" / "analytics.sqlite"

@st.cache_resource
def get_connection():
    """Singleton read-only SQLite connection."""
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ─── Data loaders (cached 5 min) ──────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_str_daily() -> pd.DataFrame:
    """
    Pivots fact_str_metrics (grain='daily') from long → wide.
    Returns one row per date with columns: supply, demand, revenue,
    occupancy, adr, revpar.
    """
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT as_of_date, metric_name, metric_value
        FROM   fact_str_metrics
        WHERE  grain = 'daily'
        ORDER  BY as_of_date
        """,
        conn,
    )
    if df.empty:
        return pd.DataFrame()

    wide = (
        df.pivot_table(
            index="as_of_date",
            columns="metric_name",
            values="metric_value",
            aggfunc="sum",
        )
        .reset_index()
    )
    wide.columns.name = None
    wide.columns = [c.lower().replace(" ", "_") for c in wide.columns]
    wide["as_of_date"] = pd.to_datetime(wide["as_of_date"])

    # fact_str_metrics stores occupancy as 'occ' in decimal form (0.674 = 67.4%)
    # Convert to percentage and rename for clarity
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
    """Pre-computed daily KPIs with YOY deltas."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM kpi_daily_summary ORDER BY as_of_date",
        conn,
    )
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


@st.cache_data(ttl=300)
def load_compression() -> pd.DataFrame:
    """Quarterly occupancy compression days."""
    conn = get_connection()
    return pd.read_sql_query(
        "SELECT * FROM kpi_compression_quarterly ORDER BY quarter",
        conn,
    )


@st.cache_data(ttl=300)
def load_load_log() -> pd.DataFrame:
    """ETL audit log."""
    conn = get_connection()
    return pd.read_sql_query(
        "SELECT * FROM load_log ORDER BY run_at DESC",
        conn,
    )


@st.cache_data(ttl=300)
def get_table_counts() -> dict:
    conn = get_connection()
    tables = [
        "fact_str_metrics",
        "kpi_daily_summary",
        "kpi_compression_quarterly",
        "load_log",
    ]
    counts = {}
    for t in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            counts[t] = row[0] if row else 0
        except Exception:
            counts[t] = "—"
    return counts


# ─── Helpers ──────────────────────────────────────────────────────────────────

def kpi_card(label: str, value: str, delta: str, positive: bool = True, neutral: bool = False) -> str:
    css = "kpi-delta-neutral" if neutral else ("kpi-delta-pos" if positive else "kpi-delta-neg")
    arrow = "" if neutral else ("▲ " if positive else "▼ ")
    return f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      <div class="{css}">{arrow}{delta}</div>
    </div>
    """


def pct_delta(a, b) -> float:
    return (a - b) / b * 100 if b else 0.0


def compute_overview_kpis(df: pd.DataFrame) -> list[dict]:
    """Split df into recent/prior halves and compute KPI cards."""
    n = len(df)
    if n < 4:
        return []
    half = n // 2
    rec, pri = df.iloc[half:], df.iloc[:half]

    def avg(d, c): return d[c].mean() if c in d and not d[c].isna().all() else 0.0
    def tot(d, c): return d[c].sum()  if c in d and not d[c].isna().all() else 0.0

    r_occ, p_occ     = avg(rec, "occupancy"),  avg(pri, "occupancy")
    r_adr, p_adr     = avg(rec, "adr"),         avg(pri, "adr")
    r_rvp, p_rvp     = avg(rec, "revpar"),      avg(pri, "revpar")
    r_rev, p_rev     = tot(rec, "revenue"),     tot(pri, "revenue")
    r_dem, p_dem     = tot(rec, "demand"),      tot(pri, "demand")
    tbid             = r_rev * 0.0125

    return [
        {"label": "RevPAR",       "value": f"${r_rvp:.2f}",             "delta": f"{pct_delta(r_rvp, p_rvp):+.1f}% vs. prior period", "positive": r_rvp >= p_rvp},
        {"label": "ADR",          "value": f"${r_adr:.2f}",             "delta": f"{pct_delta(r_adr, p_adr):+.1f}% vs. prior period", "positive": r_adr >= p_adr},
        {"label": "Occupancy",    "value": f"{r_occ:.1f}%",             "delta": f"{pct_delta(r_occ, p_occ):+.1f}% vs. prior period", "positive": r_occ >= p_occ},
        {"label": "Room Revenue", "value": f"${r_rev/1e6:.2f}M",        "delta": f"{pct_delta(r_rev, p_rev):+.1f}% vs. prior period", "positive": r_rev >= p_rev},
        {"label": "Rooms Sold",   "value": f"{r_dem:,.0f}",             "delta": f"{pct_delta(r_dem, p_dem):+.1f}% vs. prior period", "positive": r_dem >= p_dem},
        {"label": "Est. TBID Rev","value": f"${tbid/1e3:.0f}K",         "delta": "blended 1.25%",                                     "positive": True, "neutral": True},
    ]


def style_fig(fig: go.Figure, height: int = 280) -> go.Figure:
    """Apply consistent chart styling."""
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_family="Inter, system-ui, sans-serif",
        font_size=12,
        height=height,
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="rgba(0,0,0,0.06)", zeroline=False)
    return fig


# ─── Load data ────────────────────────────────────────────────────────────────
df_daily = load_str_daily()
df_kpi   = load_kpi_daily()
df_comp  = load_compression()
df_log   = load_load_log()

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🌊 Visit Dana Point")
    st.markdown("VDP Select Portfolio · 12 Properties · Anaheim Area")
    st.divider()

    RANGE_OPTIONS = {
        "Last 30 Days":  30,
        "Last 90 Days":  90,
        "Last 6 Months": 180,
        "Last 12 Months":365,
    }
    range_label = st.selectbox("Date Range", list(RANGE_OPTIONS.keys()), index=1)
    days = RANGE_OPTIONS[range_label]

    st.selectbox("Data Grain", ["Daily", "Monthly"], index=0,
                 help="Monthly grain pending loader — daily data is active")

    st.divider()

    counts = get_table_counts()
    str_rows = counts.get("fact_str_metrics", 0)
    last_log = df_log.iloc[0]["run_at"] if not df_log.empty else "—"
    if last_log != "—":
        last_log = last_log[:10]   # trim to YYYY-MM-DD

    st.markdown("**Pipeline Status**")
    st.markdown(f"🟢 STR Daily &nbsp;·&nbsp; {str_rows:,} rows")
    st.markdown(f"🟡 STR Monthly &nbsp;·&nbsp; Pending loader")
    st.markdown(f"⚫ Datafy &nbsp;·&nbsp; Not connected")
    st.caption(f"Last ETL run: {last_log}")

    if not df_daily.empty:
        min_d = df_daily["as_of_date"].min().strftime("%b %d, %Y")
        max_d = df_daily["as_of_date"].max().strftime("%b %d, %Y")
        st.caption(f"Data range: {min_d} → {max_d}")

# ─── Filter data by selected date range ──────────────────────────────────────
if not df_daily.empty:
    max_date = df_daily["as_of_date"].max()
    cutoff   = max_date - timedelta(days=days)
    df_sel   = df_daily[df_daily["as_of_date"] > cutoff].copy()
    df_kpi_sel = df_kpi[df_kpi["as_of_date"] > cutoff].copy() if not df_kpi.empty else pd.DataFrame()
else:
    df_sel, df_kpi_sel = pd.DataFrame(), pd.DataFrame()

# ─── Header ───────────────────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.title("Visit Dana Point — Analytics")
    last_updated = df_daily["as_of_date"].max().strftime("%b %d, %Y") if not df_daily.empty else "N/A"
    st.caption(f"VDP Select Portfolio · {range_label} · Last updated {last_updated}")

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_ov, tab_tr, tab_ev, tab_dl = st.tabs(
    ["📊 Overview", "📈 Trends", "🎪 Event Impact", "🗂 Data Log"]
)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_ov:
    kpis = compute_overview_kpis(df_sel)

    if not kpis:
        st.warning("No daily data available for the selected date range.")
    else:
        # KPI grid — 3 columns × 2 rows
        cols = st.columns(3)
        for i, k in enumerate(kpis):
            with cols[i % 3]:
                st.markdown(
                    kpi_card(k["label"], k["value"], k["delta"],
                             k.get("positive", True), k.get("neutral", False)),
                    unsafe_allow_html=True,
                )

        st.markdown("---")

        # ── Row 1: RevPAR trend  |  Occ vs ADR ───────────────────────────────
        c1, c2 = st.columns(2)

        with c1:
            st.markdown("**RevPAR Trend**")
            st.caption("Rolling daily values for selected period")
            fig = go.Figure(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["revpar"],
                fill="tozeroy",
                line=dict(color=TEAL, width=2),
                fillcolor="rgba(33,128,141,0.10)",
                mode="lines", name="RevPAR",
            ))
            fig.update_layout(yaxis_tickprefix="$", showlegend=False)
            st.plotly_chart(style_fig(fig), use_container_width=True)

        with c2:
            st.markdown("**Occupancy vs. ADR**")
            st.caption("Dual-axis: fill rate & pricing power")
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["occupancy"],
                name="Occupancy %", line=dict(color=TEAL, width=2), mode="lines",
            ), secondary_y=False)
            fig.add_trace(go.Scatter(
                x=df_sel["as_of_date"], y=df_sel["adr"],
                name="ADR $", line=dict(color=ORANGE, width=2), mode="lines",
            ), secondary_y=True)
            fig.update_yaxes(title_text="Occ %", secondary_y=False)
            fig.update_yaxes(title_text="ADR $", tickprefix="$",
                             secondary_y=True, showgrid=False)
            st.plotly_chart(style_fig(fig), use_container_width=True)

        # ── Row 2: Day-of-Week  |  Supply vs Demand ───────────────────────────
        c3, c4 = st.columns(2)

        with c3:
            st.markdown("**Day-of-Week Pattern**")
            st.caption("Average RevPAR by day — identifies opportunity nights")
            dow_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            tmp = df_sel.copy()
            tmp["dow"] = tmp["as_of_date"].dt.strftime("%a")
            dow_avg = tmp.groupby("dow")["revpar"].mean().reindex(dow_order)
            overall_avg = dow_avg.mean()
            bar_colors = [TEAL if v >= overall_avg else ORANGE for v in dow_avg.fillna(0)]
            fig = go.Figure(go.Bar(
                x=dow_avg.index, y=dow_avg.values,
                marker=dict(color=bar_colors, line_width=0),
            ))
            fig.update_layout(yaxis_tickprefix="$", showlegend=False)
            st.plotly_chart(style_fig(fig), use_container_width=True)

        with c4:
            st.markdown("**Supply vs. Demand**")
            st.caption("Room inventory vs. rooms sold")
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
    if df_kpi.empty:
        st.warning("kpi_daily_summary is empty — run compute_kpis.py first.")
    else:
        # Monthly aggregation from the full KPI table (not just selected range)
        df_kpi_all = df_kpi.copy()
        df_kpi_all["month"] = df_kpi_all["as_of_date"].dt.to_period("M")
        monthly = (
            df_kpi_all.groupby("month")
            .agg(
                revpar=("revpar",    "mean"),
                adr=("adr",          "mean"),
                occ_pct=("occ_pct",  "mean"),
                revpar_yoy=("revpar_yoy", "mean"),
            )
            .reset_index()
        )
        monthly["month_label"] = monthly["month"].dt.strftime("%b %Y")

        # ── Full width: YOY RevPAR Change ──────────────────────────────────────
        st.markdown("**YOY RevPAR Change**")
        st.caption("Year-over-year percentage change by month — positive (teal) vs. negative (red)")
        yoy = monthly.dropna(subset=["revpar_yoy"])
        if yoy.empty:
            st.info("YOY data requires at least 12 months of history in kpi_daily_summary.")
        else:
            colors = [GREEN if v >= 0 else RED for v in yoy["revpar_yoy"]]
            fig = go.Figure(go.Bar(
                x=yoy["month_label"], y=yoy["revpar_yoy"],
                marker=dict(color=colors, line_width=0),
                text=[f"{v:+.1f}%" for v in yoy["revpar_yoy"]],
                textposition="outside",
                textfont=dict(size=10),
            ))
            fig.update_layout(yaxis_ticksuffix="%", showlegend=False)
            st.plotly_chart(style_fig(fig, height=300), use_container_width=True)

        st.markdown("---")
        c1, c2 = st.columns(2)

        with c1:
            st.markdown("**Seasonality Index**")
            st.caption("Monthly RevPAR ÷ 12-month average — above 1.0 = peak season")
            if len(monthly) >= 6:
                ttm_avg = monthly["revpar"].mean()
                monthly["season_idx"] = monthly["revpar"] / ttm_avg
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=monthly["month_label"], y=monthly["season_idx"],
                    fill="tozeroy",
                    line=dict(color=TEAL, width=2),
                    fillcolor="rgba(33,128,141,0.10)",
                    mode="lines+markers", marker=dict(size=5, color=TEAL),
                    name="Seasonality Index",
                ))
                fig.add_hline(y=1.0, line_dash="dot",
                              line_color="rgba(167,169,169,0.6)",
                              annotation_text="Baseline 1.0", annotation_position="right")
                fig.update_layout(yaxis_range=[0.5, 1.6], showlegend=False)
                st.plotly_chart(style_fig(fig), use_container_width=True)
            else:
                st.info("Need 6+ months of data for seasonality index.")

        with c2:
            st.markdown("**TBID Revenue Estimate**")
            st.caption("Estimated monthly assessment at blended 1.25% of room revenue")
            if not df_daily.empty:
                tmp = df_daily.copy()
                tmp["month"] = tmp["as_of_date"].dt.to_period("M")
                month_rev = tmp.groupby("month")["revenue"].sum().reset_index()
                month_rev["month_label"] = month_rev["month"].dt.strftime("%b %Y")
                month_rev["tbid_est_m"] = month_rev["revenue"] * 0.0125 / 1e6
                fig = go.Figure(go.Bar(
                    x=month_rev["month_label"], y=month_rev["tbid_est_m"],
                    marker=dict(color=TEAL, line_width=0),
                ))
                fig.update_layout(
                    yaxis_tickprefix="$", yaxis_ticksuffix="M", showlegend=False
                )
                st.plotly_chart(style_fig(fig), use_container_width=True)

        st.markdown("---")

        # ── Compression quarters ───────────────────────────────────────────────
        st.markdown("**Occupancy Compression Days by Quarter**")
        st.caption("Days with occupancy ≥ 80% and ≥ 90% — signals pricing power windows")
        if not df_comp.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name="Days ≥ 80% Occ",
                x=df_comp["quarter"], y=df_comp["days_above_80_occ"],
                marker=dict(color=TEAL, line_width=0),
            ))
            fig.add_trace(go.Bar(
                name="Days ≥ 90% Occ",
                x=df_comp["quarter"], y=df_comp["days_above_90_occ"],
                marker=dict(color=TEAL_LIGHT, line_width=0),
            ))
            fig.update_layout(barmode="group")
            st.plotly_chart(style_fig(fig, height=260), use_container_width=True)
        else:
            st.info("No compression data — run compute_kpis.py to populate kpi_compression_quarterly.")

        # ── RevPAR + ADR + Occ trend lines (full history) ─────────────────────
        st.markdown("---")
        st.markdown("**RevPAR / ADR / Occupancy — Full History**")
        st.caption("Monthly averages across all available data")
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
    st.markdown("### Ohana Fest 2025 — Event Impact")
    st.caption("Source: Datafy visitor economy report · Pending live DB integration")

    # Event KPI cards
    event_kpis = [
        {"label": "Est. Event Expenditure",    "value": "$14.6M",  "delta": "Ohana Fest 2025",      "positive": True, "neutral": True},
        {"label": "Est. Destination Spend",    "value": "$18.4M",  "delta": "+3.0% vs. 2024",       "positive": True},
        {"label": "ADR Lift (Event Night)",    "value": "+$139",   "delta": "$542 vs $403 baseline", "positive": True},
        {"label": "RevPAR Lift",               "value": "+$140",   "delta": "$427 vs $287 baseline", "positive": True},
        {"label": "Overnight Hotel Visitors",  "value": "24%",     "delta": "of total attendees",    "positive": True, "neutral": True},
        {"label": "Avg. Accom. Spend/Trip",    "value": "$1,219",  "delta": "+53% vs. 2024",         "positive": True},
    ]
    cols = st.columns(3)
    for i, k in enumerate(event_kpis):
        with cols[i % 3]:
            st.markdown(
                kpi_card(k["label"], k["value"], k["delta"],
                         k.get("positive", True), k.get("neutral", False)),
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # ── Event lift chart ───────────────────────────────────────────────────────
    st.markdown("**Hotel Performance Lift — Ohana Fest 2024**")
    st.caption("ADR and Occupancy during event period vs. surrounding baseline")
    event_days = [
        "Sep 15","Sep 16","Sep 17","Sep 18","Sep 19","Sep 20","Sep 21",
        "Sep 22","Sep 23","Sep 24","Sep 25","Sep 26","Sep 27","Sep 28","Sep 29",
    ]
    adr_vals = [403, 410, 415, 420, 436, 430, 425, 418, 412, 430, 460, 510, 542, 530, 403]
    occ_vals = [65.6, 66, 67, 68, 71.2, 70, 69, 67, 66, 70, 74, 77, 78.8, 76, 65.6]

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(
        x=event_days, y=occ_vals,
        name="Occupancy %", line=dict(color=TEAL, width=2),
        mode="lines+markers", marker=dict(size=5, color=TEAL),
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=event_days, y=adr_vals,
        name="ADR $", line=dict(color=ORANGE, width=2),
        mode="lines+markers", marker=dict(size=5, color=ORANGE),
    ), secondary_y=True)
    fig.update_yaxes(title_text="Occ %", range=[60, 85], secondary_y=False)
    fig.update_yaxes(title_text="ADR $", tickprefix="$", range=[350, 580],
                     secondary_y=True, showgrid=False)
    # Shade the event weekend (Sep 26-28)
    for shade_x in ["Sep 26", "Sep 27", "Sep 28"]:
        fig.add_vline(x=shade_x, line_width=1,
                      line_color="rgba(33,128,141,0.25)", line_dash="dot")
    st.plotly_chart(style_fig(fig, height=320), use_container_width=True)

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Visitor Origin — Ohana Fest 2025**")
        st.caption("Top feeder markets by share of visitor days (Datafy)")
        cities  = ["San Clemente","San Juan Cap.","Dana Point","Laguna Niguel",
                   "Ladera Ranch","Huntington Bch","San Diego","Mission Viejo",
                   "Los Angeles","Cap. Beach"]
        shares  = [8.89, 7.84, 6.78, 6.50, 3.53, 3.19, 2.96, 2.42, 2.35, 1.85]
        fig = go.Figure(go.Bar(
            x=shares, y=cities, orientation="h",
            marker=dict(color=TEAL, line_width=0),
            text=[f"{v:.1f}%" for v in shares], textposition="outside",
        ))
        fig.update_layout(yaxis=dict(autorange="reversed"), xaxis_ticksuffix="%",
                          showlegend=False)
        st.plotly_chart(style_fig(fig, height=340), use_container_width=True)

    with c2:
        st.markdown("**Spending by Category**")
        st.caption("Share of total destination spend during event weekend")
        cats   = ["Dining & Nightlife","Accommodations","Grocery/Dept",
                  "Service Stations","Fast Food","Specialty Retail",
                  "Personal Care","Clothing","Leisure/Rec"]
        values = [30.2, 23.6, 17.4, 7.71, 6.73, 6.46, 4.08, 2.09, 1.33]
        palette = [TEAL,"#2DA6B2",TEAL_LIGHT,ORANGE,"#A84B2F",
                   "#5E5240","#626C71","#A7A9A9",RED]
        fig = go.Figure(go.Pie(
            labels=cats, values=values, hole=0.42,
            marker=dict(colors=palette),
            textfont=dict(size=10),
        ))
        fig.update_layout(
            legend=dict(font_size=10, orientation="v"),
        )
        st.plotly_chart(style_fig(fig, height=340), use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — DATA LOG
# ══════════════════════════════════════════════════════════════════════════════
with tab_dl:
    col_a, col_b = st.columns([3, 1])

    with col_a:
        st.markdown("### Load Log")
        st.caption("ETL pipeline audit trail from load_log")
        if not df_log.empty:
            st.dataframe(df_log, use_container_width=True, hide_index=True)
        else:
            st.info("No load log entries found.")

    with col_b:
        st.markdown("### Row Counts")
        counts = get_table_counts()
        for table, count in counts.items():
            label = table.replace("_", " ").title()
            st.metric(label, f"{count:,}" if isinstance(count, int) else count)

    st.markdown("---")

    # ── Recent metric samples from filtered window ─────────────────────────────
    st.markdown("### Recent Metric Samples")
    st.caption("Last 10 dates in selected window from fact_str_metrics")
    if not df_sel.empty:
        sample = df_sel.tail(10).sort_values("as_of_date", ascending=False)
        display = [c for c in
                   ["as_of_date","revpar","adr","occupancy","supply","demand","revenue"]
                   if c in sample.columns]
        rename_map = {
            "as_of_date": "Date",
            "revpar":     "RevPAR ($)",
            "adr":        "ADR ($)",
            "occupancy":  "Occupancy (%)",
            "supply":     "Supply (rooms)",
            "demand":     "Demand (rooms)",
            "revenue":    "Revenue ($)",
        }
        st.dataframe(
            sample[display].rename(columns=rename_map),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No data in selected date range.")

    st.markdown("---")

    # ── Compression quarters ───────────────────────────────────────────────────
    st.markdown("### Compression Quarters")
    st.caption("Days per quarter with occupancy ≥ 80% / ≥ 90% from kpi_compression_quarterly")
    if not df_comp.empty:
        st.dataframe(df_comp, use_container_width=True, hide_index=True)
    else:
        st.info("No compression data — run compute_kpis.py.")
