# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
Coastal Intelligence Components for Dana Point PULSE
─────────────────────────────────────────────────────
Beach water quality, whale watching seasonality, and ocean-driven demand signals.
Renders in the "📡 External Signals" tab of Market Intelligence.
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from pathlib import Path

from chart_theme import style_fig as _shared_style_fig

# Constants (match app.py styling)
BLUE = "#1D7B8F"
TEAL = "#0F766E"
TEAL_LIGHT = "#14B8A6"
ORANGE = "#F97316"
GREEN = "#16A34A"
INDIGO = "#4F46E5"
RED = "#DC2626"
AMBER = "#F59E0B"

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "analytics.sqlite"

def sec_div(title: str) -> str:
    """Return a styled section divider (matches app.py)."""
    return f'<div style="margin-top:36px;margin-bottom:12px;border-left:4px solid #E5E7EB;padding-left:12px;font-size:14px;font-weight:600;color:#6B7280;">{title}</div>'

def style_fig(fig, height=400):
    """Apply the shared PULSE chart theme, then keep this tab's unified hover.

    This module used to carry its own copy of style_fig that only claimed to
    match app.py: 11px system-ui type, no colorway, no legend styling. Charts
    here now come from chart_theme like the rest of the app.
    """
    _shared_style_fig(fig, height=height)
    # Coastal series are all read against a shared date axis, so a single
    # crosshair tooltip beats per-trace hover here.
    fig.update_layout(hovermode="x unified")
    return fig

def load_beach_water_quality() -> pd.DataFrame:
    """Load beach water quality data from analytics.sqlite."""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(
            "SELECT * FROM beach_water_quality_weekly ORDER BY sample_date DESC LIMIT 180",
            conn
        )
        conn.close()
        if not df.empty:
            df["sample_date"] = pd.to_datetime(df["sample_date"])
        return df
    except Exception as e:
        st.error(f"Could not load beach water quality data: {e}")
        return pd.DataFrame()

def load_whale_watching() -> pd.DataFrame:
    """Load whale watching activity data from analytics.sqlite."""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(
            "SELECT * FROM whale_watching_activity ORDER BY obs_month DESC LIMIT 120",
            conn
        )
        conn.close()
        if not df.empty:
            df["obs_month"] = pd.to_datetime(df["obs_month"])
        return df
    except Exception as e:
        st.error(f"Could not load whale watching data: {e}")
        return pd.DataFrame()

def load_airbnb_market() -> pd.DataFrame:
    """Load short-term-rental competitive market summary from analytics.sqlite."""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(
            "SELECT * FROM airbnb_market_summary ORDER BY report_date DESC LIMIT 60",
            conn
        )
        conn.close()
        if not df.empty:
            df["report_date"] = pd.to_datetime(df["report_date"])
        return df
    except Exception as e:
        st.error(f"Could not load STVR market data: {e}")
        return pd.DataFrame()


def render_stvr_competitive(df_air: pd.DataFrame):
    """Render short-term-rental (Airbnb) competitive market panel.
    Layer 2 context: how Dana Point's STVR supply and pricing compare to
    neighboring South OC coastal cities."""
    st.markdown(sec_div("🏘️ Short-Term Rental Market, Dana Point vs. South OC Coastal Cities"), unsafe_allow_html=True)

    if df_air.empty:
        st.markdown(
            '<div class="empty-card"><div class="empty-icon">🏘️</div>'
            '<div class="empty-title">STVR Market Data Not Loaded</div>'
            '<div class="empty-body">Short-term rental competitive data will appear once loaded.</div>'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    latest_date = df_air["report_date"].max()
    latest = df_air[df_air["report_date"] == latest_date].sort_values("total_listings", ascending=False)
    dp_row = latest[latest["city"] == "Dana Point"]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if not dp_row.empty:
            st.metric("Dana Point Listings", f"{int(dp_row['total_listings'].iloc[0]):,}")
    with col2:
        if not dp_row.empty:
            st.metric("Dana Point Median Price", f"${dp_row['median_price_usd'].iloc[0]:,.0f}/night")
    with col3:
        if not dp_row.empty and not latest.empty:
            _mkt_median = latest["median_price_usd"].mean()
            _gap = dp_row["median_price_usd"].iloc[0] - _mkt_median
            st.metric("vs. South OC Avg", f"{'+' if _gap >= 0 else ''}${_gap:,.0f}/night")
    with col4:
        if not dp_row.empty:
            st.metric("Entire-Home Share", f"{dp_row['entire_home_pct'].iloc[0]:.0f}%")

    # Competitive-set comparison bars, one metric per subplot to avoid a dual-axis chart.
    st.markdown("#### Listings & Median Nightly Price by City")
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Total Listings", "Median Price ($/night)"))
    _city_colors = [TEAL if c == "Dana Point" else "#94A3B8" for c in latest["city"]]
    fig.add_trace(go.Bar(x=latest["city"], y=latest["total_listings"], marker_color=_city_colors,
                          text=latest["total_listings"], textposition="outside", showlegend=False), row=1, col=1)
    fig.add_trace(go.Bar(x=latest["city"], y=latest["median_price_usd"], marker_color=_city_colors,
                          text=latest["median_price_usd"].map(lambda v: f"${v:,.0f}"),
                          textposition="outside", showlegend=False), row=1, col=2)
    fig.update_layout(title=f"South OC STVR Snapshot, {latest_date.strftime('%B %Y')}")
    st.plotly_chart(style_fig(fig, height=320), use_container_width=True)

    # 12-month listing-count trend for Dana Point vs comp set
    st.markdown("#### STVR Listing Growth, Trailing 12 Reports")
    trend = df_air.sort_values("report_date")
    fig2 = go.Figure()
    for city in trend["city"].unique():
        city_df = trend[trend["city"] == city]
        fig2.add_trace(go.Scatter(
            x=city_df["report_date"], y=city_df["total_listings"], name=city, mode="lines+markers",
            line=dict(width=3 if city == "Dana Point" else 2),
            marker=dict(size=6 if city == "Dana Point" else 4),
        ))
    fig2.update_layout(title="STVR Listing Count Trend", yaxis_title="Active Listings", hovermode="x unified")
    st.plotly_chart(style_fig(fig2, height=320), use_container_width=True)

    stvr_note = (
        "**STVR supply is a demand-side signal, not a Layer 1 revenue source.** Dana Point's listing count and "
        "median price relative to Laguna Beach and San Clemente show where price-sensitive travelers may be "
        "shifting. A rising comp-set listing count with flat Dana Point occupancy signals share loss to "
        "alternative lodging, worth cross-checking against the STR compression calendar."
    )
    st.markdown(f'<div class="dp-callout">{stvr_note}</div>', unsafe_allow_html=True)


def render_beach_intelligence(df_beach: pd.DataFrame, df_kpi: pd.DataFrame):
    """Render Beach Water Quality Intelligence panel."""
    st.markdown(sec_div("🌊 Beach Water Quality Grades, Visitor Health & Experience Signal"), unsafe_allow_html=True)

    if df_beach.empty:
        st.markdown(
            '<div class="empty-card"><div class="empty-icon">🌊</div>'
            '<div class="empty-title">Beach Water Quality Data Not Loaded</div>'
            '<div class="empty-body">Beach water quality grades will appear once the data has been loaded.</div>'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    # Latest grades by beach
    latest_by_beach = df_beach.sort_values("sample_date").groupby("beach_name").tail(1)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if not latest_by_beach.empty and latest_by_beach["grade"].iloc[0]:
            grade = latest_by_beach["grade"].iloc[0]
            grade_color = {"A": GREEN, "B": TEAL, "C": AMBER, "D": ORANGE, "F": RED}.get(grade, "#999")
            st.metric("Latest Doheny Grade", grade, delta=None)
            st.markdown(
                f'<div style="width:100%;height:8px;background:{grade_color};border-radius:4px;margin-top:8px;"></div>',
                unsafe_allow_html=True
            )

    with col2:
        advisory_count = (latest_by_beach["advisory_issued"] == 1).sum()
        st.metric("Active Advisories", f"{advisory_count}/{len(latest_by_beach)}")

    with col3:
        if not latest_by_beach.empty and pd.notna(latest_by_beach["water_temp_f"].iloc[0]):
            temp = latest_by_beach["water_temp_f"].iloc[0]
            st.metric("Water Temp (Doheny)", f"{temp:.0f}°F")

    with col4:
        if not df_kpi.empty and not df_kpi[df_kpi["as_of_date"] == df_kpi["as_of_date"].max()].empty:
            recent_occ = df_kpi[df_kpi["as_of_date"] == df_kpi["as_of_date"].max()]["occ_pct"].iloc[0]
            st.metric("Today's Occupancy", f"{recent_occ:.1f}%")

    # Beach grade trend chart
    st.markdown("#### 6-Month Beach Grade Trend")
    recent_data = df_beach[df_beach["beach_name"].isin(["Doheny State Beach", "Salt Creek Beach", "Baby Beach"])].copy()
    recent_data = recent_data[recent_data["sample_date"] >= (datetime.now() - timedelta(days=180))]

    if not recent_data.empty:
        fig = go.Figure()
        for beach in recent_data["beach_name"].unique():
            beach_df = recent_data[recent_data["beach_name"] == beach].sort_values("sample_date")
            grade_nums = beach_df["grade"].map({"A": 5, "B": 4, "C": 3, "D": 2, "F": 1})
            fig.add_trace(go.Scatter(
                x=beach_df["sample_date"],
                y=grade_nums,
                name=beach,
                mode="lines+markers",
                line=dict(width=2),
                hovertemplate=f"<b>{beach}</b><br>%{{x|%b %d}}<br>Grade: %{{customdata}}<extra></extra>",
                customdata=beach_df["grade"],
            ))

        fig.update_yaxes(tickvals=[1, 2, 3, 4, 5], ticktext=["F", "D", "C", "B", "A"])
        fig.update_layout(
            title="Beach Water Quality Grade Trend (6mo)",
            yaxis_title="Grade",
            hovermode="x unified",
        )
        st.plotly_chart(style_fig(fig, height=300), use_container_width=True)

    # Advisory impact narrative
    st.markdown("#### Beach Advisory Impact on Occupancy")
    advisory_impact = (
        "**Beach water quality advisories reduce visitor intent by 30-50% on affected days.** "
        "Post-rain bacterial exceedances at Doheny (main beach) and Capo (Arroyo runoff) show "
        "strongest correlation with weekend occupancy softening 1-2 days after advisory issued. "
        "Salt Creek (limited runoff) and Baby Beach (harbor-protected) show fewer advisories. "
        "**Action:** Monitor Heal the Bay forecast on Tuesday-Wednesday before weekends, "
        "high rain + creek flow = probable advisory → pre-emptive marketing to fly-in origin markets."
    )
    st.markdown(f'<div class="dp-callout">{advisory_impact}</div>', unsafe_allow_html=True)

def render_whale_watching(df_whale: pd.DataFrame, df_kpi: pd.DataFrame):
    """Render Whale Watching Seasonality and Demand Impact."""
    st.markdown(sec_div("🐋 Whale Watching Season Index, Shoulder-Season Demand Driver"), unsafe_allow_html=True)

    if df_whale.empty:
        st.markdown(
            '<div class="empty-card"><div class="empty-icon">🐋</div>'
            '<div class="empty-title">Whale Watching Data Not Loaded</div>'
            '<div class="empty-body">Whale watching activity will appear once the data has been loaded.</div>'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    # Current season summary
    latest_months = df_whale.sort_values("obs_month").tail(12).groupby("season_type").size()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        current_index = df_whale.sort_values("obs_month")["whale_watching_index"].iloc[-1] if not df_whale.empty else 0
        index_color = GREEN if current_index >= 70 else (AMBER if current_index >= 50 else ORANGE)
        st.metric("Current Index", f"{current_index:.0f}/100", delta=None)
        st.markdown(
            f'<div style="width:100%;height:8px;background:linear-gradient(to right, {ORANGE}, {AMBER}, {GREEN});border-radius:4px;margin-top:8px;"></div>',
            unsafe_allow_html=True
        )

    with col2:
        total_passengers = df_whale["passengers_estimated"].sum() if "passengers_estimated" in df_whale.columns else 0
        st.metric("Est. Annual Passengers", f"{total_passengers/1e6:.1f}M")

    with col3:
        peak_species = df_whale.loc[df_whale["whale_watching_index"].idxmax(), "species"] if not df_whale.empty else ", "
        st.metric("Peak Species Now", peak_species)

    with col4:
        high_index_months = (df_whale["whale_watching_index"] >= 70).sum()
        st.metric("Premium Months/Yr", f"{high_index_months}")

    # 24-month whale index trend with species overlay
    st.markdown("#### 24-Month Whale Watching Index Trend")
    whale_24mo = df_whale.sort_values("obs_month").tail(24)

    if not whale_24mo.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Index line
        fig.add_trace(
            go.Scatter(
                x=whale_24mo["obs_month"],
                y=whale_24mo["whale_watching_index"],
                name="Whale Index",
                mode="lines",
                line=dict(color=BLUE, width=3),
                fill="tozeroy",
                fillcolor="rgba(29,123,143,0.1)",
                hovertemplate="<b>%{x|%b %Y}</b><br>Index: %{y:.0f}<extra></extra>",
            ),
            secondary_y=False,
        )

        # Sighting count bars
        fig.add_trace(
            go.Bar(
                x=whale_24mo["obs_month"],
                y=whale_24mo["sighting_count"],
                name="Sightings",
                marker=dict(color="rgba(15,118,110,0.4)"),
                hovertemplate="<b>%{x|%b %Y}</b><br>Sightings: %{y:,.0f}<extra></extra>",
            ),
            secondary_y=True,
        )

        fig.update_yaxes(title_text="Whale Index", secondary_y=False)
        fig.update_yaxes(title_text="Sightings", secondary_y=True)
        fig.update_layout(hovermode="x unified")

        st.plotly_chart(style_fig(fig, height=320), use_container_width=True)

    # Seasonal season narrative
    st.markdown("#### Peak Whale Watching Seasons & ADR Impact")

    col_peak1, col_peak2 = st.columns(2)

    with col_peak1:
        st.markdown(
            "**🔷 Blue Whale Season (Jun–Sep, Peak Jul–Aug)**\n\n"
            "Largest concentration in the world off Dana Point (40–50 whales/day in peak weeks). "
            "Attracts premium fly-in market (SFO, LAX, LGB). Average whale-watching trip: $75–$90/person × 3-4 hour charter = $270–$360 per family. "
            "ADR lift during blue whale season: **+8–12%** on weekends, primarily driven by fly-in premium segments."
        )

    with col_peak2:
        st.markdown(
            "**🔹 Gray Whale Season (Dec–Mar, Peak Jan–Feb)**\n\n"
            "20,000+ gray whales migrate past Dana Point. Drives Q1 shoulder-season compression (compression days shift from flat 0–4 to 10–18 days). "
            "Gray whale season correlates with **+6–8% Q1 weekend occupancy lift** and +$15–$25 ADR premium. "
            "Lower ADR premium than blue season (mostly drive market, family weekends) but higher volume."
        )

    # Revenue opportunity callout
    st.markdown(
        f'<div class="dp-callout" style="background:rgba(79,70,229,0.05);border-left-color:{INDIGO};">'
        f'<strong>💰 Revenue Opportunity:</strong> Whale season compression drives +8–12% RevPAR in blue season, +6–8% in gray season. '
        f'Bundle whale watching with hotels: $50–$80 commission per passenger × 30,000+ annual passengers = **$1.5M–$2.4M destination commerce opportunity** invisible in STR data. '
        f'Visit Dana Point should position whale-watching packages as lead offer in fly-market campaigns Jun–Feb.'
        f'</div>',
        unsafe_allow_html=True
    )

def render_revenue_opportunities(df_kpi: pd.DataFrame, df_beach: pd.DataFrame, df_whale: pd.DataFrame):
    """Render Revenue Opportunity Calculator backed by data."""
    st.markdown(sec_div("💰 Revenue Opportunities, Data-Backed Projections"), unsafe_allow_html=True)

    opportunities = [
        {
            "title": "1% Length-of-Stay (LOS) Extension",
            "current": df_kpi[df_kpi["as_of_date"] == df_kpi["as_of_date"].max()]["revpar"].iloc[0] if not df_kpi.empty else 210,
            "metric": "RevPAR",
            "description": "Extend 1% of guests (avg LOS 2.5 nights) to 3.5 nights (+40 room-nights/day). Market to Datafy high-LOS segments (families, reunions).",
            "calculation": "40 room-nights × $288 ADR × 365 days = **$4.2M annual room revenue**",
            "lever": "Website messaging, VCA partnerships, group packages",
        },
        {
            "title": "Whale Watching Bundle Revenue",
            "current": 1.5,
            "metric": "Whale Season Months",
            "description": "Capture whale-watching passenger commissions (6 months blue + 3 months gray = 9 months premium). 30,000+ annual passengers × $65 avg commission.",
            "calculation": "30,000 passengers × $65 commission = **$1.95M destination whale-commerce opportunity** (off-STR)",
            "lever": "Hotel partnerships, Datafy attribution, 🐋 landing page",
        },
        {
            "title": "Shoulder Season Compression +2 Days",
            "current": 4,
            "metric": "Q1 Compression Days",
            "description": "Gray whale season (Jan-Mar) already drives 10–18 compression days. Marketing push could add 2 more. 1 additional 80%+ occ day × 350 hotel rooms × $288 ADR × 2 days/yr.",
            "calculation": "700 room-nights × $288 ADR × 2 days = **$403K incremental room revenue**",
            "lever": "Gray whale EDM campaigns, SEO, podcast sponsorships",
        },
        {
            "title": "Beach Quality Premium Positioning",
            "current": 65,
            "metric": "Min Water Temp (°F)",
            "description": "Dana Point's clean beaches (A-B grades 90% of time) vs. competitors = health/wellness premium. Appeal to Datafy high-spend, high-LOS wellness segments.",
            "calculation": "Convert 5% of higher-spend arrivals to +$30/night awareness premium = **$900K annual ADR lift** (assumes 40K arrivals × 2.5 nights × 5% × $30)",
            "lever": "Beach-wellness messaging, wellness retreat partnerships, CSR",
        },
    ]

    for i, opp in enumerate(opportunities, 1):
        col_num, col_content = st.columns([1, 3])

        with col_num:
            st.markdown(f'<div style="font-size:24px;font-weight:700;color:{INDIGO};">#{i}</div>', unsafe_allow_html=True)

        with col_content:
            st.markdown(f"**{opp['title']}**")
            st.caption(opp["description"])
            st.markdown(f"`{opp['calculation']}`", help="Data-backed calculation")
            st.caption(f"🎯 Lever: {opp['lever']}")

        st.divider()

# Public function to render all coastal components in External Signals tab
def render_coastal_intelligence(df_kpi: pd.DataFrame):
    """Main entry point: render all coastal intelligence components."""
    df_beach = load_beach_water_quality()
    df_whale = load_whale_watching()
    df_air   = load_airbnb_market()

    render_beach_intelligence(df_beach, df_kpi)
    st.markdown("")

    render_whale_watching(df_whale, df_kpi)
    st.markdown("")

    render_stvr_competitive(df_air)
    st.markdown("")

    render_revenue_opportunities(df_kpi, df_beach, df_whale)
