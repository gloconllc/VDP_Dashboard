"""
components_group.py — Group & Traveler Intelligence tab for Dana Point PULSE.

Renders the dedicated 👥 Group & Travel tab with 4 sub-tabs:
  1. Group Strategy    — TBID opportunity, displacement heatmap, executive brief
  2. Traveler Types    — radar chart, booking windows, LOS benchmarks
  3. National Context  — $319B funnel, segment donut, recovery rates
  4. AI Analyst        — pre-loaded group travel prompts
"""

from __future__ import annotations

import sqlite3
import os
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ── DB path ──────────────────────────────────────────────────────────────────
_DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "analytics.sqlite")

# ── Thresholds (named constants) ─────────────────────────────────────────────
OCC_GROUP_SAFE_MAX  = 65.0   # below this: group blocks welcome, low displacement risk
OCC_GROUP_RISK_MIN  = 80.0   # above this: group blocks displace higher-rate transient
OCC_GROUP_WARN_MIN  = 70.0   # amber zone
TBID_RATE           = 0.0125
TOT_RATE            = 0.10
GROUP_SHARE_LOW     = 0.25
GROUP_SHARE_HIGH    = 0.32
GROUP_ADR_DISCOUNT  = 0.18   # group negotiated rate ~18% below blended ADR
SMERF_BOOKING_WINDOW_LOW  = 8   # weeks
SMERF_BOOKING_WINDOW_HIGH = 48
CACHE_TTL_GROUP     = 1800   # 30 min — semi-live group data
CACHE_TTL_NATIONAL  = 3600   # 1 hr  — historical benchmarks

# Light theme — these charts render on the app's WHITE page background.
# (Text must be dark to be legible; transparent plot bg lets the page show.)
_DARK_BG   = "rgba(0,0,0,0)"          # transparent — inherits white page
_GRID_CLR  = "rgba(15,23,42,0.07)"    # subtle dark grid on white
_FONT_CLR  = "#334155"                # slate-700 body text
_LABEL_CLR = "#64748B"                # slate-500 axis labels
_TITLE_CLR = "#0F172A"                # near-black chart titles

_COLORWAY = [
    "#0891B2",  # teal
    "#F5B940",  # gold
    "#10B981",  # green
    "#A78BFA",  # purple
    "#FB923C",  # orange
    "#EF4444",  # red
    "#38BDF8",  # sky
    "#6EE7B7",  # seafoam
]


# ── Small utilities ───────────────────────────────────────────────────────────

def _dark_fig(height: int = 320) -> go.Figure:
    """Base figure tuned for the app's LIGHT page (dark text, subtle grid)."""
    fig = go.Figure()
    fig.update_layout(
        plot_bgcolor  = _DARK_BG,
        paper_bgcolor = _DARK_BG,
        font          = dict(color=_FONT_CLR, size=11.5, family="'Outfit',sans-serif"),
        height        = height,
        # t=64 gives the horizontal legend at y=1.04 enough room above the plot area
        margin        = dict(l=14, r=20, t=64, b=36, autoexpand=True),
        colorway      = _COLORWAY,
        legend        = dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="left", x=0,
            font=dict(size=10.5, color=_FONT_CLR),
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="#E2E8F0", borderwidth=1,
            tracegroupgap=4,
        ),
        hoverlabel    = dict(
            bgcolor="rgba(15,23,42,0.96)",
            bordercolor="rgba(8,145,178,0.55)",
            font=dict(size=13, color="#F8FAFC"),
        ),
    )
    fig.update_xaxes(showgrid=False, color=_LABEL_CLR, title_font=dict(color=_FONT_CLR),
                     automargin=True)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_CLR, color=_LABEL_CLR,
                     title_font=dict(color=_FONT_CLR), automargin=True)
    return fig


def _metric_box(label: str, value: str, note: str = "", color: str = "#0891B2",
                action: str = "") -> str:
    """Light-theme stat card — white bg, dark text, accent top-border."""
    action_html = (
        f'<div style="margin-top:8px;padding:6px 10px;background:{color}12;'
        f'border-radius:5px;font-size:10.5px;color:#334155;line-height:1.5;">'
        f'<strong style="color:{color};">Action:</strong> {action}</div>'
    ) if action else ""
    return (
        f'<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-top:3px solid {color};'
        f'border-radius:9px;padding:14px 16px;text-align:center;'
        f'box-shadow:0 1px 3px rgba(15,23,42,0.06);">'
        f'<div style="color:{color};font-size:9.5px;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.08em;margin-bottom:4px;">{label}</div>'
        f'<div style="color:#0F172A;font-family:\'Outfit\',sans-serif;font-size:24px;'
        f'font-weight:900;letter-spacing:-0.02em;line-height:1.1;">{value}</div>'
        f'<div style="color:#64748B;font-size:10.5px;margin-top:4px;">{note}</div>'
        f'{action_html}'
        f'</div>'
    )


def _action_card(icon: str, headline: str, body: str, action: str,
                 accent: str = "#0891B2") -> str:
    """Light-theme action card — white bg, dark text, accent left bar + CTA."""
    return (
        f'<div style="background:linear-gradient(180deg,{accent}08 0%,#FFFFFF 60%);'
        f'border:1px solid #E2E8F0;border-left:4px solid {accent};'
        f'border-radius:10px;padding:14px 18px;margin-bottom:10px;'
        f'box-shadow:0 1px 3px rgba(15,23,42,0.06);">'
        f'<div style="display:flex;align-items:flex-start;gap:10px;">'
        f'<span style="font-size:22px;margin-top:2px;">{icon}</span>'
        f'<div style="flex:1;">'
        f'<div style="color:#0F172A;font-family:\'Outfit\',sans-serif;font-weight:800;'
        f'font-size:13.5px;margin-bottom:4px;">{headline}</div>'
        f'<div style="color:#475569;font-size:12px;line-height:1.6;margin-bottom:8px;">{body}</div>'
        f'<div style="background:{accent}12;border-radius:5px;padding:6px 10px;'
        f'font-size:11px;color:{accent};font-weight:700;">→ {action}</div>'
        f'</div></div></div>'
    )


# ── Data loaders (cached inside this module) ──────────────────────────────────

@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_group_intel() -> dict:
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM group_intelligence ORDER BY benchmark_date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        return {}


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_str_group_metrics() -> pd.DataFrame:
    """Load fact_str_group_metrics: (as_of_date, property, segment, metric_name, metric_value)."""
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        df = pd.read_sql_query(
            """
            SELECT as_of_date, property, segment, metric_name, metric_value, unit
            FROM fact_str_group_metrics
            WHERE as_of_date >= date('now','-180 days')
            ORDER BY as_of_date DESC, property, segment, metric_name
            """,
            conn,
        )
        conn.close()
        return df if not df.empty else pd.DataFrame()
    except Exception as exc:
        import logging; logging.getLogger("vdp_dashboard").debug("_load_str_group_metrics failed: %s", exc)
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_monthly_occ() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        df = pd.read_sql_query(
            """
            SELECT substr(as_of_date,1,7) AS month,
                   avg(occ_pct) AS avg_occ,
                   avg(adr)     AS avg_adr,
                   avg(revpar)  AS avg_revpar
            FROM kpi_daily_summary
            WHERE as_of_date >= date('now','-18 months')
            GROUP BY month
            ORDER BY month
            """,
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_NATIONAL)
def _load_ust_segments() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        df = pd.read_sql_query("SELECT * FROM us_travel_group_segments", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_NATIONAL)
def _load_ust_biz() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        df = pd.read_sql_query("SELECT * FROM us_travel_business_travel", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_NATIONAL)
def _load_traveler_types() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        df = pd.read_sql_query(
            "SELECT * FROM us_travel_traveler_types ORDER BY revenue_contribution DESC",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_group_insights() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        df = pd.read_sql_query(
            """
            SELECT audience, category, headline, body, priority
            FROM insights_daily
            WHERE category LIKE '%group%'
               OR category LIKE '%traveler%'
               OR category = 'group_event_synergy'
               OR category = 'traveler_mix_revenue_gap'
               OR category = 'supply_pipeline_group_risk'
               OR category = 'competitive_set_group_gap'
               OR category = 'channel_group_attribution'
            ORDER BY priority ASC
            LIMIT 12
            """,
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_social_summary() -> dict:
    """Latest IG + FB social metrics for group audience reach context."""
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        ig = conn.execute(
            "SELECT followers, reach, impressions FROM later_ig_profile_growth "
            "ORDER BY data_date DESC LIMIT 1"
        ).fetchone()
        fb = conn.execute(
            "SELECT page_followers, reach FROM later_fb_profile_growth "
            "ORDER BY data_date DESC LIMIT 1"
        ).fetchone()
        # IG recent engagement rate
        eng = conn.execute(
            "SELECT avg(engagement_rate) as avg_eng FROM later_ig_posts "
            "WHERE posted_at >= date('now','-30 days')"
        ).fetchone()
        conn.close()
        return {
            "ig_followers":  int(ig["followers"]  or 0) if ig and ig["followers"]  else 0,
            "fb_followers":  int(fb["page_followers"] or 0) if fb and fb["page_followers"] else 0,
            "ig_reach":      int(ig["reach"]       or 0) if ig and ig["reach"]       else 0,
            "ig_eng":        float(eng["avg_eng"]   or 0) if eng and eng["avg_eng"]   else 0.0,
        }
    except Exception:
        return {"ig_followers": 0, "fb_followers": 0, "ig_reach": 0, "ig_eng": 0.0}


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_costar_chain() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        df = pd.read_sql_query(
            """
            SELECT chain_scale, supply_rooms, occupancy_pct, adr_usd, revpar_usd,
                   room_revenue_usd, market_share_revpar_pct
            FROM costar_chain_scale_breakdown
            ORDER BY revpar_usd DESC
            """,
            conn,
        )
        conn.close()
        return df
    except Exception as exc:
        import logging; logging.getLogger("vdp_dashboard").debug("_load_costar_chain failed: %s", exc)
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_competitive_set() -> pd.DataFrame:
    """Load costar_competitive_set: ADR, Occ, MPI, ARI, RGI per property."""
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='costar_competitive_set'")
        if not cur.fetchone():
            conn.close()
            return pd.DataFrame()
        df = pd.read_sql_query(
            """
            SELECT property_name, chain_scale, rooms, occupancy_pct, adr_usd,
                   revpar_usd, mpi, ari, rgi, submarket
            FROM costar_competitive_set
            ORDER BY adr_usd DESC
            """,
            conn,
        )
        conn.close()
        return df
    except Exception as exc:
        import logging; logging.getLogger("vdp_dashboard").debug("_load_competitive_set failed: %s", exc)
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_supply_pipeline() -> pd.DataFrame:
    """Load costar_supply_pipeline: upcoming hotel openings."""
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='costar_supply_pipeline'")
        if not cur.fetchone():
            conn.close()
            return pd.DataFrame()
        df = pd.read_sql_query(
            """
            SELECT property_name, city, rooms, chain_scale, status,
                   projected_open_date, brand, notes
            FROM costar_supply_pipeline
            ORDER BY projected_open_date ASC
            """,
            conn,
        )
        conn.close()
        return df
    except Exception as exc:
        import logging; logging.getLogger("vdp_dashboard").debug("_load_supply_pipeline failed: %s", exc)
        return pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_attribution_groups() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load website + media group attribution data."""
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        web_df = pd.DataFrame()
        med_df = pd.DataFrame()
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='datafy_attribution_website_groups'")
        if cur.fetchone():
            web_df = pd.read_sql_query("SELECT * FROM datafy_attribution_website_groups ORDER BY report_period_start", conn)
        cur2 = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='datafy_attribution_media_groups'")
        if cur2.fetchone():
            med_df = pd.read_sql_query("SELECT * FROM datafy_attribution_media_groups ORDER BY report_period_start", conn)
        conn.close()
        return web_df, med_df
    except Exception as exc:
        import logging; logging.getLogger("vdp_dashboard").debug("_load_attribution_groups failed: %s", exc)
        return pd.DataFrame(), pd.DataFrame()


@st.cache_data(ttl=CACHE_TTL_GROUP)
def _load_costar_monthly() -> pd.DataFrame:
    """Load costar_monthly_performance for occupancy trend overlay."""
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='costar_monthly_performance'")
        if not cur.fetchone():
            conn.close()
            return pd.DataFrame()
        df = pd.read_sql_query(
            """
            SELECT as_of_date, occupancy_pct, adr_usd, revpar_usd
            FROM costar_monthly_performance
            WHERE as_of_date >= date('now','-24 months')
            ORDER BY as_of_date
            """,
            conn,
        )
        conn.close()
        return df
    except Exception as exc:
        import logging; logging.getLogger("vdp_dashboard").debug("_load_costar_monthly failed: %s", exc)
        return pd.DataFrame()


# ── Chart builders ────────────────────────────────────────────────────────────

def _chart_tbid_bar(g: dict) -> go.Figure:
    """Side-by-side TBID bar: current range vs group contribution vs +5pp uplift."""
    tbid_low   = g.get("estimated_group_tbid_rev_low",  3_603_940) / 1e6
    tbid_high  = g.get("estimated_group_tbid_rev_high", 4_613_043) / 1e6
    uplift     = g.get("tbid_uplift_per_5pp_shift",      720_788)  / 1e6
    tbid_mid   = (tbid_low + tbid_high) / 2

    categories = ["Group TBID<br><sup>Low Estimate</sup>", "Group TBID<br><sup>High Estimate</sup>", "+5pp Mix<br><sup>Incremental Uplift</sup>"]
    values     = [tbid_low, tbid_high, uplift]
    bar_colors = ["#0891B2", "#06B6D4", "#F5B940"]
    line_colors= ["#38BDF8", "#22D3EE", "#FCD34D"]
    # Inside label: dollar amount; outside annotation: % of mid
    pct_of_mid = [v / tbid_mid * 100 if tbid_mid else 0 for v in values]
    sub_labels = ["baseline range", "upside scenario", f"{pct_of_mid[2]:.0f}% of midpoint"]

    fig = _dark_fig(height=320)
    fig.add_trace(go.Bar(
        x=categories,
        y=values,
        marker=dict(
            color=bar_colors,
            line=dict(color=line_colors, width=2),
            opacity=0.92,
        ),
        text=[f"<b>${v:.2f}M</b>" for v in values],
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(size=14, color="#FFFFFF", family="'Outfit',sans-serif"),
        hovertemplate=(
            "<b>%{x}</b><br>"
            "<span style='color:#38BDF8'>$%{y:.3f}M</span> annual TBID<br>"
            "<extra></extra>"
        ),
        customdata=sub_labels,
    ))
    # Annotation above each bar with sub-label
    for i, (cat, val, sub) in enumerate(zip(categories, values, sub_labels)):
        fig.add_annotation(
            x=i, y=val,
            text=f"<i style='font-size:10px;color:#94A3B8'>{sub}</i>",
            showarrow=False,
            yshift=14,
            font=dict(size=10, color="#94A3B8"),
        )
    # Midpoint reference line
    fig.add_hline(
        y=tbid_mid,
        line_dash="dot", line_color="#38BDF8", line_width=1.5,
        annotation_text=f"  midpoint ${tbid_mid:.2f}M",
        annotation_font_size=10, annotation_font_color="#38BDF8",
        annotation_position="right",
    )
    fig.update_layout(
        title=dict(text="TBID Revenue Opportunity — Group Business", font=dict(size=13, color="#0F172A")),
        yaxis_title="$M / Year",
        yaxis_tickprefix="$",
        yaxis_ticksuffix="M",
        yaxis=dict(gridcolor="rgba(148,163,184,0.12)", zeroline=False),
        showlegend=False,
        margin=dict(l=14, r=80, t=52, b=14),
    )
    return fig


def _chart_occ_heatmap(df_monthly: pd.DataFrame) -> go.Figure:
    """Monthly occupancy with group-safe/risk color bands + ADR overlay line."""
    if df_monthly.empty:
        return _dark_fig()

    months = df_monthly["month"].tolist()
    occs   = df_monthly["avg_occ"].tolist()
    adrs   = df_monthly["avg_adr"].tolist() if "avg_adr" in df_monthly.columns else []

    # Shorten month labels: "2025-01" → "Jan '25"
    import calendar
    short_months = []
    for m in months:
        try:
            y, mo = m.split("-")
            short_months.append(f"{calendar.month_abbr[int(mo)]} '{y[2:]}")
        except Exception:
            short_months.append(m)

    bar_colors, border_colors, zone_labels = [], [], []
    for occ in occs:
        if occ >= OCC_GROUP_RISK_MIN:
            bar_colors.append("#EF4444")
            border_colors.append("#FCA5A5")
            zone_labels.append("Displace")
        elif occ >= OCC_GROUP_WARN_MIN:
            bar_colors.append("#F59E0B")
            border_colors.append("#FCD34D")
            zone_labels.append("Monitor")
        else:
            bar_colors.append("#10B981")
            border_colors.append("#6EE7B7")
            zone_labels.append("Safe")

    fig = _dark_fig(height=320)
    # Occupancy bars
    fig.add_trace(go.Bar(
        name="Avg Occupancy",
        x=short_months,
        y=occs,
        marker=dict(color=bar_colors, line=dict(color=border_colors, width=1.5), opacity=0.88),
        text=[f"<b>{v:.1f}%</b>" for v in occs],
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(size=11, color="#FFFFFF", family="'Outfit',sans-serif"),
        hovertemplate="<b>%{x}</b><br>Occ: <b>%{y:.1f}%</b><br>Zone: %{customdata}<extra></extra>",
        customdata=zone_labels,
        yaxis="y",
    ))
    # ADR overlay line (right axis)
    if adrs:
        fig.add_trace(go.Scatter(
            name="ADR ($)",
            x=short_months,
            y=adrs,
            mode="lines+markers+text",
            line=dict(color="#A78BFA", width=2.5, dash="solid"),
            marker=dict(size=6, color="#A78BFA", line=dict(color="#C4B5FD", width=1.5)),
            text=[f"${v:.0f}" for v in adrs],
            textposition="top center",
            textfont=dict(size=9, color="#C4B5FD"),
            hovertemplate="<b>%{x}</b><br>ADR: <b>$%{y:.0f}</b><extra></extra>",
            yaxis="y2",
        ))
    # Reference lines
    for level, color, label in [
        (OCC_GROUP_RISK_MIN, "#EF4444", "80% displace risk"),
        (OCC_GROUP_WARN_MIN, "#F59E0B", "70% monitor"),
        (OCC_GROUP_SAFE_MAX, "#10B981", "65% group safe"),
    ]:
        fig.add_hline(
            y=level, line_dash="dot", line_color=color, line_width=1.5,
            annotation_text=f"  {label}",
            annotation_font_size=9, annotation_font_color=color,
            annotation_position="right",
        )
    fig.update_layout(
        title=dict(
            text="Monthly Occupancy — Group Safety Calendar  🟢 Safe  🟡 Monitor  🔴 Displace",
            font=dict(size=12.5, color="#0F172A"),
        ),
        yaxis=dict(title="Avg Occupancy %", range=[0, 105], gridcolor="rgba(148,163,184,0.12)"),
        yaxis2=dict(title="ADR ($)", overlaying="y", side="right", showgrid=False,
                    tickprefix="$", color="#A78BFA") if adrs else {},
        showlegend=bool(adrs),
        legend=dict(
            orientation="h", yanchor="top", y=-0.14, xanchor="center", x=0.5,
            font=dict(size=10, color=_FONT_CLR),
            bgcolor="rgba(255,255,255,0.85)", bordercolor="#E2E8F0", borderwidth=1,
        ),
        margin=dict(l=14, r=90, t=52, b=50, autoexpand=True),
        barmode="group",
    )
    # Remove ADR text labels to avoid overlap — hover shows them instead
    if adrs:
        fig.update_traces(texttemplate=None, selector=dict(type="scatter"))
    return fig


def _chart_segment_donut(df_segs: pd.DataFrame) -> go.Figure:
    """National $319B group travel segment donut."""
    segs = df_segs[df_segs["segment"] != "total_group"].copy()
    if segs.empty:
        return _dark_fig()

    segs["label"] = segs["segment"].str.replace("_", " ").str.title()
    segs = segs.sort_values("spend_billion_usd", ascending=False)

    DONUT_COLORS = ["#0891B2", "#10B981", "#F5B940", "#A78BFA", "#FB923C", "#EF4444", "#38BDF8", "#6EE7B7"]
    fig = _dark_fig(height=360)
    fig.add_trace(go.Pie(
        labels=segs["label"].tolist(),
        values=segs["spend_billion_usd"].tolist(),
        hole=0.54,
        marker=dict(
            colors=DONUT_COLORS[:len(segs)],
            line=dict(color="rgba(15,23,42,0.8)", width=3),
        ),
        texttemplate="<b>%{label}</b><br>$%{value:.0f}B · %{percent}",
        textposition="outside",
        textfont=dict(size=11, color="#CBD5E1"),
        hovertemplate="<b>%{label}</b><br>$%{value:.0f}B&nbsp;&nbsp;(%{percent})<extra></extra>",
        pull=[0.06 if i == 0 else 0.02 for i in range(len(segs))],
        rotation=20,
    ))
    fig.update_layout(
        title=dict(text="U.S. Group Travel — $319B National Segments", font=dict(size=13, color="#0F172A")),
        annotations=[dict(
            text="<b>$319B</b><br><span style='font-size:9px;color:#94A3B8'>National<br>Group Travel</span>",
            x=0.5, y=0.5, font_size=16, showarrow=False,
            font=dict(color="#0F172A"),
        )],
        showlegend=True,
        legend=dict(orientation="v", x=1.02, y=0.5, font=dict(size=10)),
        margin=dict(l=10, r=150, t=52, b=10),
    )
    return fig


def _chart_revenue_funnel(g: dict) -> go.Figure:
    """Revenue funnel: $319B national → South OC market → Dana Point group → TBID."""
    national_group  = 319.0   # $B
    annual_rev      = g.get("estimated_annual_room_rev", 1_153_261_000) / 1e9
    group_rev_mid   = annual_rev * ((GROUP_SHARE_LOW + GROUP_SHARE_HIGH) / 2)
    tbid_est        = group_rev_mid * TBID_RATE

    labels = [
        "U.S. Group Travel",
        "South OC Room Revenue",
        "Dana Point Group Rev (est.)",
        "Group TBID Contribution",
    ]
    values    = [national_group, annual_rev, group_rev_mid, tbid_est]
    value_fmt = ["$319B national market", f"${annual_rev:.2f}B total rev",
                 f"${group_rev_mid*1e3:.0f}M est. group", f"${tbid_est*1e3:.1f}M TBID"]
    funnel_colors = ["#0891B2", "#06B6D4", "#F5B940", "#A78BFA"]
    border_colors = ["#38BDF8", "#22D3EE", "#FCD34D", "#C4B5FD"]

    fig = _dark_fig(height=340)
    fig.add_trace(go.Funnel(
        y=labels,
        x=values,
        textposition="inside",
        texttemplate="<b>%{customdata}</b>",
        customdata=value_fmt,
        textfont=dict(size=13, color="#FFFFFF", family="'Outfit',sans-serif"),
        marker=dict(
            color=funnel_colors,
            line=dict(width=2.5, color=border_colors),
            opacity=0.92,
        ),
        connector=dict(line=dict(color="rgba(148,163,184,0.25)", dash="dot", width=1.5)),
        hovertemplate="<b>%{y}</b><br>%{customdata}<extra></extra>",
    ))
    # Pct-of-national annotations on the right
    for i, (lbl, val) in enumerate(zip(labels, values)):
        pct = val / national_group * 100 if national_group else 0
        fig.add_annotation(
            x=val, y=lbl,
            text=f"  {pct:.2f}% of U.S." if i > 0 else "  100% baseline",
            xanchor="left", showarrow=False,
            font=dict(size=9.5, color="#94A3B8"),
        )
    fig.update_layout(
        title=dict(text="Group Revenue Funnel — National → Dana Point TBID", font=dict(size=13, color="#0F172A")),
        funnelmode="stack",
        margin=dict(l=14, r=200, t=52, b=14),
    )
    return fig


def _chart_traveler_radar(df_types: pd.DataFrame) -> go.Figure:
    """Radar/spider chart comparing 4 key traveler types on 5 dimensions."""
    if df_types.empty:
        return _dark_fig()

    rev_score = {"highest": 10, "high": 7.5, "medium": 5, "low": 2.5}
    sea_score = {"year_round": 10, "shoulder": 6, "peak": 4, "winter": 3}

    TARGET_TYPES = ["business", "smerf", "family", "leisure"]
    subset = df_types[df_types["traveler_type"].isin(TARGET_TYPES)].copy()
    if subset.empty:
        subset = df_types.head(4)

    dims = ["Revenue Tier", "Length of Stay", "Booking Window", "Seasonal Flex", "Group Fit"]

    def _scores(row) -> list[float]:
        rev  = rev_score.get(str(row.get("revenue_contribution", "medium")).lower(), 5)
        los  = min(float(row.get("typical_los_nights_high") or 3) / 7 * 10, 10)
        bkw  = min(float(row.get("booking_window_weeks_high") or 4) / 48 * 10, 10)
        sea  = sea_score.get(str(row.get("seasonal_pattern", "peak")).lower(), 5)
        grp_fit = 8 if row.get("traveler_type") in ("smerf", "business") else 5
        return [rev, los, bkw, sea, grp_fit]

    RADAR_COLORS = ["#0891B2", "#F5B940", "#10B981", "#A78BFA"]
    FILL_COLORS  = ["rgba(8,145,178,0.18)", "rgba(245,185,64,0.18)",
                    "rgba(16,185,129,0.18)", "rgba(167,139,250,0.18)"]
    fig = _dark_fig(height=400)

    for i, (_, row) in enumerate(subset.iterrows()):
        scores = _scores(row)
        scores_closed = scores + [scores[0]]
        type_label = str(row.get("traveler_type", "")).title()
        # peak score for label
        peak_dim = dims[scores.index(max(scores))]
        fig.add_trace(go.Scatterpolar(
            r=scores_closed,
            theta=dims + [dims[0]],
            fill="toself",
            fillcolor=FILL_COLORS[i % len(FILL_COLORS)],
            line=dict(color=RADAR_COLORS[i % len(RADAR_COLORS)], width=2.5),
            marker=dict(size=7, color=RADAR_COLORS[i % len(RADAR_COLORS)],
                        line=dict(color="#0F172A", width=1.5)),
            name=f"{type_label} (peak: {peak_dim})",
            hovertemplate=f"<b>{type_label}</b><br>%{{theta}}: <b>%{{r:.1f}}/10</b><extra></extra>",
        ))

    fig.update_layout(
        title=dict(text="Traveler Type Profile — 5-Dimension Comparison", font=dict(size=13, color="#0F172A")),
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(
                visible=True, range=[0, 10], color=_LABEL_CLR,
                gridcolor=_GRID_CLR, tickfont=dict(size=9),
                tickvals=[2.5, 5, 7.5, 10],
                ticktext=["Low", "Med", "High", "Best"],
            ),
            angularaxis=dict(
                color="#CBD5E1", gridcolor=_GRID_CLR,
                tickfont=dict(size=12, color="#CBD5E1"),
            ),
        ),
        showlegend=True,
        legend=dict(orientation="h", y=-0.12, x=0.5, xanchor="center",
                    font=dict(size=11)),
        margin=dict(l=40, r=40, t=52, b=60),
    )
    return fig


def _chart_competitive_scatter(df: pd.DataFrame) -> go.Figure:
    """ADR vs Occupancy bubble per property — size = RevPAR, color = chain scale."""
    if df.empty:
        return _dark_fig()

    scale_color = {
        "Luxury":         "#F5B940",
        "Upper Upscale":  "#0891B2",
        "Upscale":        "#10B981",
        "Upper Midscale": "#A78BFA",
        "Midscale":       "#FB923C",
        "Economy":        "#6EE7B7",
    }

    # Normalize RevPAR to bubble sizes 14–42
    rvp_vals = df["revpar_usd"].fillna(0)
    rvp_min, rvp_max = rvp_vals.min(), rvp_vals.max()
    rvp_range = rvp_max - rvp_min if rvp_max != rvp_min else 1
    bubble_sizes = [14 + (v - rvp_min) / rvp_range * 28 for v in rvp_vals]

    fig = _dark_fig(height=380)
    for scale in df["chain_scale"].unique():
        sub  = df[df["chain_scale"] == scale]
        idxs = sub.index.tolist()
        clr  = scale_color.get(str(scale), "#64748B")
        sizes = [bubble_sizes[df.index.get_loc(i)] for i in idxs]
        fig.add_trace(go.Scatter(
            x=sub["occupancy_pct"].tolist(),
            y=sub["adr_usd"].tolist(),
            mode="markers+text",
            marker=dict(
                color=clr, size=sizes,
                line=dict(width=2, color="rgba(255,255,255,0.35)"),
                opacity=0.88,
            ),
            text=sub["property_name"].str[:14].tolist(),
            textposition="top center",
            textfont=dict(size=9, color=_FONT_CLR),
            name=str(scale),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Occ: <b>%{x:.1f}%</b><br>"
                "ADR: <b>$%{y:.0f}</b><br>"
                "RevPAR: <b>$%{customdata:.0f}</b><extra></extra>"
            ),
            customdata=sub["revpar_usd"].tolist(),
        ))
    # Market average crosshairs
    avg_occ = df["occupancy_pct"].mean()
    avg_adr = df["adr_usd"].mean()
    fig.add_vline(x=avg_occ, line_dash="dot", line_color="rgba(148,163,184,0.4)", line_width=1)
    fig.add_hline(y=avg_adr, line_dash="dot", line_color="rgba(148,163,184,0.4)", line_width=1)
    fig.add_annotation(x=avg_occ, y=avg_adr,
                       text=f" Mkt avg ({avg_occ:.0f}% / ${avg_adr:.0f})",
                       showarrow=False, font=dict(size=9, color="#64748B"), xanchor="left")
    fig.update_layout(
        title=dict(
            text="Competitive Set — ADR vs Occupancy  ·  bubble size = RevPAR",
            font=dict(size=12.5, color="#0F172A"),
        ),
        xaxis=dict(title="Occupancy %", ticksuffix="%", gridcolor=_GRID_CLR),
        yaxis=dict(title="ADR ($)", tickprefix="$", gridcolor=_GRID_CLR),
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="top", y=-0.14, xanchor="center", x=0.5,
            font=dict(size=10, color=_FONT_CLR),
            bgcolor="rgba(255,255,255,0.85)", bordercolor="#E2E8F0", borderwidth=1,
        ),
        margin=dict(l=14, r=14, t=52, b=70, autoexpand=True),
    )
    return fig


def _chart_tbid_chain_waterfall(df_chain: pd.DataFrame, g: dict) -> go.Figure:
    """TBID contribution waterfall by chain scale — ranked bars with inside labels."""
    if df_chain.empty:
        return _dark_fig()

    _GROUP_SCALES = ["Luxury", "Upper Upscale", "Upscale"]
    subset = df_chain[df_chain["chain_scale"].isin(_GROUP_SCALES)].copy()
    if subset.empty:
        subset = df_chain.copy()

    group_share = (GROUP_SHARE_LOW + GROUP_SHARE_HIGH) / 2
    subset = subset.copy()
    subset["group_rev"] = (
        subset["supply_rooms"].fillna(0) *
        subset["adr_usd"].fillna(0) *
        subset["occupancy_pct"].fillna(0) / 100 *
        group_share * 365
    )
    subset["tbid_contribution"] = subset["group_rev"] * TBID_RATE
    subset = subset.sort_values("tbid_contribution", ascending=False)

    BAR_COLORS    = ["#F5B940", "#0891B2", "#10B981", "#A78BFA", "#FB923C"]
    BORDER_COLORS = ["#FCD34D", "#38BDF8", "#6EE7B7", "#C4B5FD", "#FDBA74"]
    tbid_vals = (subset["tbid_contribution"] / 1e6).tolist()
    total = sum(tbid_vals)
    pct_labels = [f"{v/total*100:.0f}% of mix" for v in tbid_vals]

    fig = _dark_fig(height=320)
    fig.add_trace(go.Bar(
        x=subset["chain_scale"].tolist(),
        y=tbid_vals,
        marker=dict(
            color=BAR_COLORS[:len(subset)],
            line=dict(color=BORDER_COLORS[:len(subset)], width=2),
            opacity=0.92,
        ),
        text=[f"<b>${v:.2f}M</b>" for v in tbid_vals],
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(size=13, color="#FFFFFF", family="'Outfit',sans-serif"),
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Est. Group TBID: <b>$%{y:.3f}M</b><br>"
            "%{customdata}<extra></extra>"
        ),
        customdata=pct_labels,
    ))
    # Outside pct label
    for i, (scale, val, pct) in enumerate(zip(subset["chain_scale"], tbid_vals, pct_labels)):
        fig.add_annotation(
            x=scale, y=val,
            text=f"<i style='color:#94A3B8'>{pct}</i>",
            showarrow=False, yshift=14,
            font=dict(size=9.5, color="#94A3B8"),
        )
    tbid_target = 4.1
    fig.add_hline(
        y=tbid_target, line_dash="dash", line_color="#F5B940", line_width=1.5,
        annotation_text=f"  ${tbid_target:.1f}M target",
        annotation_font_size=10, annotation_font_color="#F5B940",
        annotation_position="right",
    )
    fig.update_layout(
        title=dict(text="TBID Waterfall by Chain Scale — rooms × ADR × 28% group share × 1.25%",
                   font=dict(size=12, color="#0F172A")),
        yaxis=dict(title="Est. TBID $M/yr", tickprefix="$", ticksuffix="M",
                   gridcolor="rgba(148,163,184,0.12)"),
        showlegend=False,
        margin=dict(l=14, r=80, t=52, b=14),
    )
    return fig


def _chart_supply_pipeline(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar timeline of supply pipeline by chain scale."""
    if df.empty:
        return _dark_fig()

    scale_color  = {
        "Luxury":         "#F5B940",
        "Upper Upscale":  "#0891B2",
        "Upscale":        "#10B981",
        "Upper Midscale": "#A78BFA",
        "Midscale":       "#FB923C",
        "Economy":        "#6EE7B7",
    }
    scale_border = {
        "Luxury":         "#FCD34D",
        "Upper Upscale":  "#38BDF8",
        "Upscale":        "#6EE7B7",
        "Upper Midscale": "#C4B5FD",
        "Midscale":       "#FDBA74",
        "Economy":        "#A7F3D0",
    }
    df = df.sort_values("rooms", ascending=True)
    labels  = [f"{r['property_name']}  ({r.get('projected_open_date','TBD')})"
               for _, r in df.iterrows()]
    colors  = [scale_color.get(str(s), "#64748B") for s in df["chain_scale"]]
    borders = [scale_border.get(str(s), "#94A3B8") for s in df["chain_scale"]]

    fig = _dark_fig(height=max(280, len(df) * 60))
    fig.add_trace(go.Bar(
        y=labels,
        x=df["rooms"].tolist(),
        orientation="h",
        marker=dict(color=colors, line=dict(color=borders, width=1.5), opacity=0.90),
        text=[f"<b>{int(v):,} rooms</b>" for v in df["rooms"]],
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(size=11.5, color="#FFFFFF", family="'Outfit',sans-serif"),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Rooms: <b>%{x:,}</b><br>"
            "Scale: %{customdata}<extra></extra>"
        ),
        customdata=df["chain_scale"].tolist(),
    ))
    # Outside annotation: % of total pipeline
    total_rooms = df["rooms"].sum()
    for i, (lbl, val) in enumerate(zip(labels, df["rooms"])):
        fig.add_annotation(
            x=val, y=lbl,
            text=f"  {val/total_rooms*100:.0f}% of pipeline",
            xanchor="left", showarrow=False,
            font=dict(size=9, color="#94A3B8"),
        )
    fig.update_layout(
        title=dict(text="Supply Pipeline — Upcoming Dana Point Hotel Openings",
                   font=dict(size=12.5, color="#0F172A")),
        xaxis=dict(title="New Rooms", gridcolor="rgba(148,163,184,0.12)"),
        showlegend=False,
        margin=dict(l=260, r=160, t=52, b=14),
    )
    return fig


def _chart_attribution_channel(web_df: pd.DataFrame, med_df: pd.DataFrame) -> go.Figure:
    """Grouped bar: website vs media attributed group trips and impact — dual axis."""
    if web_df.empty and med_df.empty:
        return _dark_fig()

    TBID_TARGET_MID = 4_100_000

    channels, trips_vals, impact_vals = [], [], []
    if not web_df.empty:
        channels.append("Website")
        trips_vals.append(float(web_df["trips"].sum()))
        impact_vals.append(float(web_df["est_impact_usd"].sum()))
    if not med_df.empty:
        channels.append("Media")
        trips_vals.append(float(med_df["trips"].sum()))
        impact_vals.append(float(med_df["est_impact_usd"].sum()))

    fig = _dark_fig(height=340)
    fig.add_trace(go.Bar(
        name="Attributed Trips",
        x=channels,
        y=trips_vals,
        marker=dict(color="#0891B2", line=dict(color="#38BDF8", width=2), opacity=0.90),
        yaxis="y",
        text=[f"<b>{int(v):,}</b><br><span style='font-size:9px'>trips</span>" for v in trips_vals],
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(size=12, color="#FFFFFF", family="'Outfit',sans-serif"),
        hovertemplate="<b>%{x}</b><br>Trips: <b>%{y:,}</b><extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Est. Economic Impact",
        x=channels,
        y=[v / 1000 for v in impact_vals],
        marker=dict(color="#10B981", line=dict(color="#6EE7B7", width=2), opacity=0.90),
        yaxis="y2",
        text=[f"<b>${v/1000:.0f}K</b><br><span style='font-size:9px'>impact</span>" for v in impact_vals],
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(size=12, color="#FFFFFF", family="'Outfit',sans-serif"),
        hovertemplate="<b>%{x}</b><br>Impact: <b>$%{y:.0f}K</b><extra></extra>",
    ))
    fig.add_hline(
        y=TBID_TARGET_MID / 1000, line_dash="dash",
        line_color="#F5B940", line_width=1.5,
        annotation_text="  $4.1M TBID target",
        annotation_font_size=10, annotation_font_color="#F5B940",
        annotation_position="right",
        yref="y2",
    )
    fig.update_layout(
        title=dict(text="Group Attribution — Website vs Media Channels",
                   font=dict(size=12.5, color="#0F172A")),
        barmode="group",
        yaxis=dict(title="Attributed Trips", color="#38BDF8", gridcolor="rgba(148,163,184,0.12)"),
        yaxis2=dict(title="Est. Impact ($K)", overlaying="y", side="right",
                    color="#6EE7B7", tickprefix="$", ticksuffix="K", showgrid=False),
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.14, xanchor="center", x=0.5, font=dict(size=10, color=_FONT_CLR), bgcolor="rgba(255,255,255,0.85)", bordercolor="#E2E8F0", borderwidth=1),
        margin=dict(l=14, r=90, t=52, b=60, autoexpand=True),
    )
    return fig


def _chart_costar_occ_overlay(df_costar: pd.DataFrame, web_df: pd.DataFrame) -> go.Figure:
    """Dual-axis line: CoStar monthly occ vs Datafy group attribution — area fill + markers."""
    fig = _dark_fig(height=360)

    if not df_costar.empty and "as_of_date" in df_costar.columns:
        occ_vals = df_costar["occupancy_pct"].tolist()
        fig.add_trace(go.Scatter(
            x=df_costar["as_of_date"].tolist(),
            y=occ_vals,
            name="CoStar Market Occ %",
            mode="lines+markers",
            fill="tozeroy",
            fillcolor="rgba(8,145,178,0.12)",
            line=dict(color="#0891B2", width=2.5),
            marker=dict(size=7, color="#0891B2",
                        line=dict(color="#38BDF8", width=1.5)),
            yaxis="y",
            hovertemplate="<b>%{x}</b><br>CoStar Occ: <b>%{y:.1f}%</b><extra></extra>",
        ))
        # Threshold bands
        for lvl, clr, lbl in [
            (OCC_GROUP_RISK_MIN, "#EF4444", "80% — displace risk"),
            (OCC_GROUP_WARN_MIN, "#F59E0B", "70% — monitor"),
        ]:
            fig.add_hline(y=lvl, line_dash="dot", line_color=clr, line_width=1,
                          annotation_text=f"  {lbl}", annotation_font_size=9,
                          annotation_font_color=clr, annotation_position="right")

    if not web_df.empty and "est_impact_usd" in web_df.columns:
        q_totals = web_df.groupby("report_period_start")["est_impact_usd"].sum().reset_index()
        fig.add_trace(go.Scatter(
            x=q_totals["report_period_start"].tolist(),
            y=(q_totals["est_impact_usd"] / 1000).tolist(),
            name="Group Impact ($K)",
            mode="lines+markers+text",
            line=dict(color="#10B981", width=2.5, dash="dot"),
            marker=dict(size=10, symbol="diamond", color="#10B981",
                        line=dict(color="#6EE7B7", width=1.5)),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Impact: <b>$%{y:.0f}K</b><extra></extra>",
        ))

    fig.update_layout(
        title=dict(text="CoStar Monthly Occupancy vs Datafy Group Attribution",
                   font=dict(size=12.5, color="#0F172A")),
        yaxis=dict(title="Occupancy %", ticksuffix="%", color="#38BDF8",
                   gridcolor="rgba(148,163,184,0.12)"),
        yaxis2=dict(title="Group Impact ($K)", overlaying="y", side="right",
                    color="#6EE7B7", tickprefix="$", ticksuffix="K", showgrid=False),
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.14, xanchor="center", x=0.5, font=dict(size=10, color=_FONT_CLR), bgcolor="rgba(255,255,255,0.85)", bordercolor="#E2E8F0", borderwidth=1),
        margin=dict(l=14, r=90, t=52, b=60, autoexpand=True),
    )
    return fig


def _render_shoulder_alignment(df_types: pd.DataFrame, comp: pd.DataFrame) -> None:
    """Heatmap table: traveler types × quarters — Safe/Caution/Risk by occ threshold."""
    if df_types.empty or comp.empty:
        st.info("Traveler type or compression data not loaded.")
        return

    quarters = ["Q1", "Q2", "Q3", "Q4"]
    # Map compression days to occ risk level per quarter
    q_risk: dict[str, str] = {}
    for _, row in comp.iterrows():
        q = str(row.get("quarter", "")).split("-")[-1] if "-" in str(row.get("quarter", "")) else str(row.get("quarter", ""))
        days_high = int(row.get("days_above_80_occ") or 0)
        if days_high >= 20:
            q_risk[q] = "Risk"
        elif days_high >= 8:
            q_risk[q] = "Caution"
        else:
            q_risk[q] = "Safe"
    for q in quarters:
        q_risk.setdefault(q, "Safe")

    # Select traveler types to show
    _SHOW_TYPES = ["business", "smerf", "family", "leisure", "solo", "bleisure"]
    subset = df_types[df_types["traveler_type"].isin(_SHOW_TYPES)].copy()
    if subset.empty:
        subset = df_types.head(5)

    # Build seasonal affinity: which quarters does each type prefer?
    _SEASONAL_MAP = {
        "peak":       {"Q3": "Caution", "Q2": "Caution", "Q1": "Safe", "Q4": "Safe"},
        "year_round": {"Q1": "Safe",    "Q2": "Safe",    "Q3": "Safe", "Q4": "Safe"},
        "shoulder":   {"Q1": "Safe",    "Q4": "Safe",    "Q2": "Caution", "Q3": "Risk"},
        "winter":     {"Q1": "Safe",    "Q4": "Caution", "Q2": "Risk",  "Q3": "Risk"},
    }
    # Light-theme risk cells: soft tinted bg + dark readable text
    _CELL_COLOR = {"Safe": "#DCFCE7", "Caution": "#FEF3C7", "Risk": "#FEE2E2"}
    _TEXT_COLOR = {"Safe": "#15803D", "Caution": "#B45309", "Risk": "#B91C1C"}

    rows_html = ""
    for _, trow in subset.iterrows():
        ttype = str(trow.get("traveler_type", "")).title()
        seasonal = str(trow.get("seasonal_pattern", "year_round")).lower()
        affinity = _SEASONAL_MAP.get(seasonal, _SEASONAL_MAP["year_round"])
        cells = ""
        for q in quarters:
            type_pref = affinity.get(q, "Safe")
            mkt_risk  = q_risk.get(q, "Safe")
            # Effective risk: max(market risk, type mismatch)
            _RANK = {"Safe": 0, "Caution": 1, "Risk": 2}
            effective = "Safe" if _RANK[mkt_risk] == 0 and _RANK[type_pref] == 0 else \
                        "Risk" if _RANK[mkt_risk] == 2 or _RANK[type_pref] == 2 else "Caution"
            bg = _CELL_COLOR[effective]
            fg = _TEXT_COLOR[effective]
            cells += (
                f'<td style="background:{bg};color:{fg};font-weight:700;font-size:11px;'
                f'text-align:center;padding:8px 4px;border:1px solid #E2E8F0;">'
                f'{effective}</td>'
            )
        rows_html += (
            f'<tr><td style="color:#0F172A;font-size:11.5px;padding:8px 12px;'
            f'border:1px solid #E2E8F0;font-weight:700;">{ttype}</td>'
            f'{cells}</tr>'
        )

    header_cells_parts = []
    for q in quarters:
        q_r = q_risk.get(q, "Safe")
        q_c = _TEXT_COLOR[q_r]
        header_cells_parts.append(
            f'<th style="color:#475569;font-size:10.5px;text-align:center;padding:8px;'
            f'border:1px solid #E2E8F0;background:#F8FAFC;">{q}<br>'
            f'<span style="color:{q_c};font-size:9px;font-weight:700;">({q_r})</span></th>'
        )
    header_cells = "".join(header_cells_parts)

    st.markdown(f"""
    <div style="margin-top:14px;margin-bottom:6px;color:#0891B2;font-size:10px;
                font-weight:800;text-transform:uppercase;letter-spacing:.08em;">
      SHOULDER SEASON ALIGNMENT MATRIX — Traveler Types × Quarters
    </div>
    <div style="overflow-x:auto;">
    <table style="width:100%;border-collapse:collapse;background:#FFFFFF;
                  border:1px solid #E2E8F0;border-radius:8px;overflow:hidden;">
      <thead>
        <tr>
          <th style="color:#475569;font-size:10.5px;text-align:left;padding:8px 12px;
                     border:1px solid #E2E8F0;background:#F8FAFC;">Traveler Type</th>
          {header_cells}
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
    </div>
    <div style="margin-top:8px;font-size:10px;color:#64748B;">
      🟢 Safe = low displacement risk + type alignment · 🟡 Caution = monitor demand ·
      🔴 Risk = high occ displacement or seasonal mismatch. Market occ threshold: 80%+.
    </div>
    """, unsafe_allow_html=True)


# ── STR Group Segment Visualizations ──────────────────────────────────────────

def _chart_group_vs_transient_trends(df_group: pd.DataFrame) -> go.Figure:
    """Line chart: Group vs Transient Occ/ADR/RevPAR trends over time — one metric per sub-chart."""
    if df_group.empty:
        fig = _dark_fig()
        fig.add_annotation(text="No group segment data available", showarrow=False)
        return fig

    df_pivot = df_group.pivot_table(
        index="as_of_date",
        columns=["segment", "metric_name"],
        values="metric_value",
        aggfunc="first"
    ).reset_index()
    df_pivot["as_of_date"] = pd.to_datetime(df_pivot["as_of_date"])
    df_pivot = df_pivot.sort_values("as_of_date")

    # Show only ADR to keep chart readable — most actionable single metric
    fig = _dark_fig(height=360)

    _SEG_CFG = [
        ("Grp.",   "Group",     "#10B981", "solid"),
        ("Trans.", "Transient", "#F5B940", "dash"),
        ("Cont.",  "Contract",  "#A78BFA", "dot"),
        ("Total",  "Total",     "#64748B", "dashdot"),
    ]
    for metric, m_label, m_color in [("ADR", "ADR ($)", "#0891B2"),
                                      ("Occupancy (%)", "Occupancy (%)", "#10B981"),
                                      ("RevPAR", "RevPAR ($)", "#F5B940")]:
        for seg, seg_label, color, dash in _SEG_CFG:
            col = (seg, metric)
            if col not in df_pivot.columns:
                continue
            fig.add_trace(go.Scatter(
                x=df_pivot["as_of_date"],
                y=df_pivot[col],
                mode="lines",
                name=f"{seg_label} {m_label}",
                line=dict(color=color, width=2, dash=dash),
                visible=(metric == "ADR"),  # default: show ADR only
                hovertemplate=f"<b>%{{x|%b %d}}</b><br>{seg_label} {m_label}: %{{y:.2f}}<extra></extra>",
            ))

    # Dropdown to switch metric
    n_segs = sum(1 for s, _, _, _ in _SEG_CFG if (s, "ADR") in df_pivot.columns)
    n_per = n_segs  # traces per metric group

    def _vis(metric_idx):
        n = len(fig.data)
        v = [False] * n
        start = metric_idx * n_per
        for i in range(start, min(start + n_per, n)):
            v[i] = True
        return v

    fig.update_layout(
        title_text="Group vs Transient Segment Trends",
        title_font=dict(size=13, color=_TITLE_CLR),
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="top", y=-0.18,
            xanchor="center", x=0.5,
            font=dict(size=10, color=_FONT_CLR),
            bgcolor="rgba(255,255,255,0.85)", bordercolor="#E2E8F0", borderwidth=1,
        ),
        margin=dict(l=14, r=20, t=56, b=90, autoexpand=True),
        updatemenus=[dict(
            type="buttons", direction="right",
            x=0.0, xanchor="left", y=1.12, yanchor="top",
            showactive=True,
            buttons=[
                dict(label="ADR", method="update",
                     args=[{"visible": _vis(0)}, {"yaxis.title.text": "ADR ($)"}]),
                dict(label="Occupancy %", method="update",
                     args=[{"visible": _vis(1)}, {"yaxis.title.text": "Occupancy (%)"}]),
                dict(label="RevPAR", method="update",
                     args=[{"visible": _vis(2)}, {"yaxis.title.text": "RevPAR ($)"}]),
            ],
            bgcolor="#F1F5F9", bordercolor="#CBD5E1",
            font=dict(size=11, color="#0F172A"),
            pad=dict(r=4, t=4),
        )],
    )
    fig.update_xaxes(tickangle=-30, tickfont=dict(size=10))
    fig.update_yaxes(title_text="ADR ($)")
    return fig


def _chart_segment_mix_donut(df_group: pd.DataFrame) -> go.Figure:
    """Donut chart: Group demand share vs Transient vs Contract (latest date)."""
    if df_group.empty:
        fig = _dark_fig(height=360)
        fig.add_annotation(text="No group segment data available", showarrow=False)
        return fig

    latest = df_group["as_of_date"].max()
    latest_df = df_group[df_group["as_of_date"] == latest]
    seg_sum = (
        latest_df[latest_df["metric_name"] == "Occupancy (%)"]
        .groupby("segment")["metric_value"].sum()
    )
    if seg_sum.empty:
        fig = _dark_fig(height=360)
        fig.add_annotation(text="No occupancy data for latest date", showarrow=False)
        return fig

    fig = _dark_fig(height=360)
    fig.add_trace(go.Pie(
        labels=seg_sum.index,
        values=seg_sum.values,
        hole=0.42,
        marker=dict(colors=[_COLORWAY[i % len(_COLORWAY)] for i in range(len(seg_sum))],
                    line=dict(color="#FFFFFF", width=2)),
        textinfo="label+percent",
        textposition="outside",
        textfont=dict(size=11, color=_FONT_CLR),
        insidetextorientation="horizontal",
        hovertemplate="<b>%{label}</b><br>Occupancy sum: %{value:.1f}%<br>Share: %{percent}<extra></extra>",
    ))
    fig.update_layout(
        title_text="Segment Demand Mix — Latest Week",
        title_font=dict(size=13, color=_TITLE_CLR),
        showlegend=False,
        margin=dict(l=30, r=30, t=56, b=30, autoexpand=True),
    )
    return fig


def _chart_group_adr_premium(df_group: pd.DataFrame) -> go.Figure:
    """Area chart: Group ADR premium vs Transient over time."""
    if df_group.empty:
        fig = _dark_fig(height=340)
        fig.add_annotation(text="No group segment data available", showarrow=False)
        return fig

    df_pivot = df_group.pivot_table(
        index="as_of_date",
        columns=["segment", "metric_name"],
        values="metric_value",
        aggfunc="first"
    ).reset_index()
    df_pivot["as_of_date"] = pd.to_datetime(df_pivot["as_of_date"])
    df_pivot = df_pivot.sort_values("as_of_date")

    has_grp   = ("Grp.",   "ADR") in df_pivot.columns
    has_trans = ("Trans.", "ADR") in df_pivot.columns

    fig = _dark_fig(height=340)

    if has_grp and has_trans:
        df_pivot["_premium"] = (
            (df_pivot[("Grp.", "ADR")] - df_pivot[("Trans.", "ADR")])
            / df_pivot[("Trans.", "ADR")].replace(0, float("nan")) * 100
        ).fillna(0)
        fig.add_trace(go.Scatter(
            x=df_pivot["as_of_date"], y=df_pivot["_premium"],
            mode="lines", name="Group ADR Premium %",
            line=dict(color="#A78BFA", width=2.5),
            fill="tozeroy", fillcolor="rgba(167,139,250,0.12)",
            hovertemplate="<b>%{x|%b %d}</b><br>ADR Premium: %{y:+.1f}%<extra></extra>",
        ))
        fig.add_hline(y=0, line_dash="dot", line_color="#94A3B8", line_width=1)
        y_title = "Premium vs Transient (%)"
    elif has_grp:
        fig.add_trace(go.Scatter(
            x=df_pivot["as_of_date"], y=df_pivot[("Grp.", "ADR")],
            mode="lines", name="Group ADR",
            line=dict(color="#10B981", width=2.5),
            hovertemplate="<b>%{x|%b %d}</b><br>Group ADR: $%{y:.2f}<extra></extra>",
        ))
        y_title = "Group ADR ($)"
    else:
        fig.add_annotation(text="ADR data not available", showarrow=False)
        y_title = ""

    fig.update_layout(
        title_text="Group ADR Premium vs Transient",
        title_font=dict(size=13, color=_TITLE_CLR),
        showlegend=False,
        margin=dict(l=14, r=20, t=56, b=40, autoexpand=True),
    )
    fig.update_xaxes(tickangle=-30, tickfont=dict(size=10))
    fig.update_yaxes(title_text=y_title)
    return fig


def _chart_group_event_correlation(df_group: pd.DataFrame) -> go.Figure:
    """Horizontal bar: Group occupancy by property — latest week, sorted descending."""
    if df_group.empty:
        fig = _dark_fig(height=360)
        fig.add_annotation(text="No group segment data available", showarrow=False)
        return fig

    latest = df_group["as_of_date"].max()
    # occ is stored as decimal (0.688 = 68.8%) — multiply by 100.
    # Metric name may be 'occ_pct' (new loader) or 'Occupancy (%)' (legacy).
    occ_mask = df_group["metric_name"].isin(["occ_pct", "Occupancy (%)"])
    raw = df_group[
        (df_group["as_of_date"] == latest) &
        (df_group["segment"] == "Grp.") &
        occ_mask
    ].copy()
    # Values already stored as decimal (≤1.0) → multiply by 100.
    # If legacy data was mistakenly stored as room-night counts (>100), reject it.
    raw["occ_pct"] = raw["metric_value"].apply(
        lambda v: v * 100 if pd.notna(v) and 0 <= v <= 1.5 else (v if pd.notna(v) and 0 < v <= 100 else None)
    )
    prop_group = (
        raw.dropna(subset=["occ_pct"])
        .groupby("property")["occ_pct"].mean()
        .sort_values(ascending=True)
        .tail(10)
    )

    if prop_group.empty:
        fig = _dark_fig(height=360)
        fig.add_annotation(
            text="Group occupancy data pending re-import.<br>Re-run the pipeline with the latest STR multi-segment export.",
            showarrow=False, font=dict(size=12, color="#94A3B8"),
        )
        return fig

    # Shorten long names
    short_names = [n[:28] + "…" if len(n) > 28 else n for n in prop_group.index]

    fig = _dark_fig(height=max(320, 36 * len(prop_group) + 80))
    fig.add_trace(go.Bar(
        y=short_names,
        x=prop_group.values,
        orientation="h",
        marker=dict(
            color=prop_group.values,
            colorscale=[[0, "#BAE6FD"], [1, "#0891B2"]],
            line=dict(color="#0369A1", width=1),
        ),
        text=[f"{v:.1f}%" for v in prop_group.values],
        textposition="outside",
        textfont=dict(size=10, color=_FONT_CLR),
        cliponaxis=False,
        hovertemplate="<b>%{y}</b><br>Group Occ: %{x:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        title_text=f"Group Occupancy by Property — {pd.Timestamp(latest).strftime('%b %d, %Y')}",
        title_font=dict(size=13, color=_TITLE_CLR),
        showlegend=False,
        margin=dict(l=14, r=60, t=56, b=36, autoexpand=True),
    )
    fig.update_xaxes(title_text="Group Occupancy (%)", range=[0, min(prop_group.max() * 1.25, 100)])
    fig.update_yaxes(automargin=True, tickfont=dict(size=10))
    return fig


def _chart_property_group_performance(df_group: pd.DataFrame) -> go.Figure:
    """Horizontal bar: Group demand share % by property (top 10)."""
    if df_group.empty:
        fig = _dark_fig(height=360)
        fig.add_annotation(text="No group segment data available", showarrow=False)
        return fig

    latest = df_group["as_of_date"].max()
    latest_df = df_group[df_group["as_of_date"] == latest]
    occ_mask = latest_df["metric_name"].isin(["occ_pct", "Occupancy (%)"])

    prop_data = []
    for prop in latest_df["property"].unique():
        sub = latest_df[latest_df["property"] == prop]
        # Use only rows with valid occupancy metric names; filter out room-night counts (>1.5 as decimal, >100 as pct)
        grp_sub   = sub[(sub["segment"] == "Grp.")  & occ_mask & sub["metric_value"].between(0, 1.5)]
        total_sub = sub[(sub["segment"] == "Total") & occ_mask & sub["metric_value"].between(0, 1.5)]
        grp_occ   = grp_sub["metric_value"].sum()
        total_occ = total_sub["metric_value"].sum()
        if total_occ > 0:
            prop_data.append({"property": prop, "group_pct": grp_occ / total_occ * 100})

    if not prop_data:
        fig = _dark_fig(height=360)
        fig.add_annotation(text="No property data available", showarrow=False)
        return fig

    prop_df = pd.DataFrame(prop_data).sort_values("group_pct", ascending=True).tail(10)
    short_names = [n[:28] + "…" if len(n) > 28 else n for n in prop_df["property"]]

    fig = _dark_fig(height=max(320, 36 * len(prop_df) + 80))
    fig.add_trace(go.Bar(
        y=short_names,
        x=prop_df["group_pct"].values,
        orientation="h",
        marker=dict(
            color=prop_df["group_pct"].values,
            colorscale=[[0, "#D1FAE5"], [1, "#059669"]],
            line=dict(color="#047857", width=1),
        ),
        text=[f"{v:.1f}%" for v in prop_df["group_pct"].values],
        textposition="outside",
        textfont=dict(size=10, color=_FONT_CLR),
        cliponaxis=False,
        hovertemplate="<b>%{y}</b><br>Group Share: %{x:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        title_text="Group Demand Share by Property (Top 10)",
        title_font=dict(size=13, color=_TITLE_CLR),
        showlegend=False,
        margin=dict(l=14, r=60, t=56, b=36, autoexpand=True),
    )
    fig.update_xaxes(title_text="Group Demand Share (%)",
                     range=[0, prop_df["group_pct"].max() * 1.25])
    fig.update_yaxes(automargin=True, tickfont=dict(size=10))
    return fig


def _chart_segment_occupancy_heatmap(df_group: pd.DataFrame) -> go.Figure:
    """Heatmap: Property × Segment Occupancy — latest date.

    occ is stored as decimal (0.688 = 68.8%); multiplied by 100 before display.
    Values > 1.5 (room-night counts from malformed imports) are treated as invalid.
    """
    if df_group.empty:
        fig = _dark_fig(height=420)
        fig.add_annotation(text="No group segment data available", showarrow=False)
        return fig

    latest = df_group["as_of_date"].max()
    occ_mask = df_group["metric_name"].isin(["occ_pct", "Occupancy (%)"])
    latest_df = df_group[
        (df_group["as_of_date"] == latest) &
        occ_mask
    ].copy()

    # occ stored as decimal (0.0–1.0); multiply by 100 → percentage.
    # Reject legacy room-night counts stored in this field (value > 1.5).
    latest_df = latest_df[latest_df["metric_value"].between(0, 1.5)].copy()
    latest_df["occ_pct"] = latest_df["metric_value"] * 100

    heatmap_pivot = latest_df.pivot_table(
        index="property", columns="segment",
        values="occ_pct", aggfunc="first"
    )
    if heatmap_pivot.empty:
        fig = _dark_fig(height=420)
        fig.add_annotation(
            text="Segment occupancy data pending re-import.<br>Re-run the pipeline with the latest STR multi-segment export.",
            showarrow=False, font=dict(size=12, color="#94A3B8"),
        )
        return fig

    # Sort by group occupancy; shorten long names
    sort_col = "Grp." if "Grp." in heatmap_pivot.columns else heatmap_pivot.columns[0]
    heatmap_pivot = heatmap_pivot.sort_values(sort_col, ascending=True)
    short_y = [n[:26] + "…" if len(n) > 26 else n for n in heatmap_pivot.index]

    n_rows = len(heatmap_pivot)
    row_h  = max(22, min(36, 420 // max(n_rows, 1)))
    fig = _dark_fig(height=max(340, row_h * n_rows + 120))

    fig.add_trace(go.Heatmap(
        z=heatmap_pivot.values,
        x=heatmap_pivot.columns.tolist(),
        y=short_y,
        colorscale="RdYlGn",
        zmin=0, zmax=100,
        colorbar=dict(
            title=dict(text="Occ %", font=dict(size=11, color=_FONT_CLR)),
            thickness=12, len=0.75, x=1.01,
            tickfont=dict(size=10, color=_FONT_CLR),
        ),
        text=[[f"{v:.0f}%" if pd.notna(v) else "" for v in row] for row in heatmap_pivot.values],
        texttemplate="%{text}",
        textfont=dict(size=9.5, color="#0F172A"),
        hovertemplate="<b>%{y}</b><br>%{x}: %{z:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        title_text=f"Segment Occupancy Heatmap — {pd.Timestamp(latest).strftime('%b %d, %Y')}",
        title_font=dict(size=13, color=_TITLE_CLR),
        showlegend=False,
        margin=dict(l=14, r=80, t=56, b=40, autoexpand=True),
    )
    fig.update_xaxes(side="bottom", tickfont=dict(size=11), automargin=True)
    fig.update_yaxes(automargin=True, tickfont=dict(size=10))
    return fig


# ── Sub-tab renderers ─────────────────────────────────────────────────────────

def _render_group_strategy(g: dict, df_monthly: pd.DataFrame,
                           df_chain: pd.DataFrame, insights: pd.DataFrame,
                           social: dict,
                           df_comp: Optional[pd.DataFrame] = None,
                           df_pipeline: Optional[pd.DataFrame] = None,
                           web_groups: Optional[pd.DataFrame] = None,
                           med_groups: Optional[pd.DataFrame] = None) -> None:
    # Load STR group segment data
    df_group = _load_str_group_metrics()

    # ── Executive Brief ───────────────────────────────────────────────────────
    tbid_low   = g.get("estimated_group_tbid_rev_low",  3_603_940)
    tbid_high  = g.get("estimated_group_tbid_rev_high", 4_613_043)
    uplift     = g.get("tbid_uplift_per_5pp_shift",      720_788)
    compression = int(g.get("compression_days_annual", 0) or 0)
    market_adr  = float(g.get("market_blended_adr", 288.50) or 288.50)
    group_adr   = float(g.get("estimated_group_adr", 236.57) or 236.57)
    adr_gap     = market_adr - group_adr

    if compression >= 30:
        risk_label, risk_color, risk_action = "HIGH", "#EF4444", "Target Q1/Q4 shoulder exclusively"
    elif compression >= 10:
        risk_label, risk_color, risk_action = "MODERATE", "#F59E0B", "Focus shoulder + midweek group blocks"
    else:
        risk_label, risk_color, risk_action = "LOW", "#22C55E", "Group blocks viable year-round"

    st.markdown(f"""
    <div style="background:linear-gradient(135deg,rgba(8,145,178,0.08),rgba(16,185,129,0.05));
                border:1px solid #BAE6FD;border-radius:12px;padding:20px 24px;
                margin-bottom:18px;box-shadow:0 1px 3px rgba(15,23,42,0.06);">
      <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                  letter-spacing:.1em;margin-bottom:10px;">EXECUTIVE BRIEF — GROUP BUSINESS CASE</div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:14px;">
        <div style="text-align:center;">
          <div style="color:#0F172A;font-family:'Outfit',sans-serif;font-size:26px;font-weight:900;letter-spacing:-0.02em;">${tbid_low/1e6:.1f}M–${tbid_high/1e6:.1f}M</div>
          <div style="color:#475569;font-size:10.5px;margin-top:2px;font-weight:600;">Est. Annual Group TBID</div>
          <div style="color:#94A3B8;font-size:9.5px;">25–32% demand share benchmark</div>
        </div>
        <div style="text-align:center;">
          <div style="color:{risk_color};font-family:'Outfit',sans-serif;font-size:26px;font-weight:900;letter-spacing:-0.02em;">{risk_label}</div>
          <div style="color:#475569;font-size:10.5px;margin-top:2px;font-weight:600;">Displacement Risk</div>
          <div style="color:#94A3B8;font-size:9.5px;">{compression} compression days/yr</div>
        </div>
        <div style="text-align:center;">
          <div style="color:#D97706;font-family:'Outfit',sans-serif;font-size:26px;font-weight:900;letter-spacing:-0.02em;">+${uplift/1000:.0f}K/yr</div>
          <div style="color:#475569;font-size:10.5px;margin-top:2px;font-weight:600;">TBID Uplift per +5pp</div>
          <div style="color:#94A3B8;font-size:9.5px;">each 5pp shift in group mix</div>
        </div>
      </div>
      <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:6px;padding:10px 14px;">
        <div style="color:#B45309;font-size:10px;font-weight:800;text-transform:uppercase;
                    letter-spacing:.05em;margin-bottom:4px;">TOP RECOMMENDATION</div>
        <div style="color:#334155;font-size:12px;line-height:1.6;">
          {risk_action}. Group ADR is estimated at <strong style="color:#0F172A;">${group_adr:.0f}</strong> vs.
          <strong style="color:#0F172A;">${market_adr:.0f}</strong> blended market ADR — a
          <strong style="color:#0F172A;">${adr_gap:.0f}/room/night gap</strong>. Shoulder-season group bookings
          maximize TBID without displacing peak transient revenue.
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── KPI row ───────────────────────────────────────────────────────────────
    total_rooms  = int(g.get("total_market_rooms", 5120) or 5120)
    group_rooms  = int(g.get("group_primary_rooms", 3098) or 3098)
    group_pct    = float(g.get("group_primary_pct", 0.605) or 0.605)

    c1, c2, c3, c4 = st.columns(4)
    for col, lbl, val, note, color, action in [
        (c1, "Group-Capable Rooms", f"{group_rooms:,}", f"{group_pct*100:.0f}% of {total_rooms:,} market rooms",
         "#0891B2", "Upper Upscale + Upscale carry highest group amenity density"),
        (c2, "Est. Annual Group TBID", f"${tbid_low/1e6:.1f}M–${tbid_high/1e6:.1f}M",
         "industry benchmark range", "#10B981",
         "Each 1pp growth in group demand mix = +$144K TBID/yr"),
        (c3, "TBID per +5pp Shift", f"+${uplift/1000:.0f}K/yr",
         "incremental on group mix growth", "#F5B940",
         "Set 5pp group mix growth as a 12-month TBID goal"),
        (c4, "Displacement Risk", f"{risk_label}", f"{compression} compression days/yr",
         risk_color, risk_action),
    ]:
        with col:
            st.markdown(_metric_box(lbl, val, note, color, action), unsafe_allow_html=True)

    st.markdown("<div style='margin:18px 0 8px;'></div>", unsafe_allow_html=True)

    # ── Charts ────────────────────────────────────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        st.plotly_chart(_chart_tbid_bar(g), use_container_width=True, key="gt_tbid_bar")
    with col_b:
        st.plotly_chart(_chart_occ_heatmap(df_monthly), use_container_width=True, key="gt_occ_heat")

    # ── Chain scale context ───────────────────────────────────────────────────
    if not df_chain.empty:
        _GROUP_SCALES = {"Upper Upscale", "Upscale"}
        chain_s = df_chain.drop_duplicates("chain_scale").sort_values("supply_rooms", ascending=False)
        bar_clrs   = ["#0891B2" if str(r.get("chain_scale", "")) in _GROUP_SCALES else "#475569"
                      for _, r in chain_s.iterrows()]
        bord_clrs  = ["#38BDF8" if str(r.get("chain_scale", "")) in _GROUP_SCALES else "#64748B"
                      for _, r in chain_s.iterrows()]
        rvp_vals = chain_s["revpar_usd"].tolist()
        fig_ch = _dark_fig(height=300)
        fig_ch.add_trace(go.Bar(
            x=chain_s["chain_scale"].tolist(),
            y=chain_s["supply_rooms"].tolist(),
            marker=dict(color=bar_clrs, line=dict(color=bord_clrs, width=2), opacity=0.90),
            text=[f"<b>{int(v):,}</b>" for v in chain_s["supply_rooms"]],
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(size=13, color="#FFFFFF", family="'Outfit',sans-serif"),
            hovertemplate=(
                "<b>%{x}</b><br>Rooms: <b>%{y:,}</b><br>"
                "RevPAR: <b>$%{customdata:.0f}</b><extra></extra>"
            ),
            customdata=rvp_vals,
        ))
        # RevPAR as annotation above each bar
        for scale, rooms, rvp in zip(chain_s["chain_scale"], chain_s["supply_rooms"], rvp_vals):
            fig_ch.add_annotation(
                x=scale, y=rooms,
                text=f"RevPAR ${rvp:.0f}",
                showarrow=False, yshift=14,
                font=dict(size=9.5, color="#94A3B8"),
            )
        fig_ch.update_layout(
            title=dict(text="Dana Point Supply by Chain Scale  ·  Blue = Group-Primary",
                       font=dict(size=12.5, color="#0F172A")),
            showlegend=False,
            yaxis=dict(title="Supply Rooms", gridcolor="rgba(148,163,184,0.12)"),
            margin=dict(l=14, r=14, t=52, b=14),
        )
        st.plotly_chart(fig_ch, use_container_width=True, key="gt_chain_bar")

    # ── Social reach context ──────────────────────────────────────────────────
    ig_f = social.get("ig_followers", 0)
    fb_f = social.get("fb_followers", 0)
    ig_r = social.get("ig_reach", 0)
    ig_e = social.get("ig_eng", 0.0)
    if ig_f or fb_f:
        st.markdown("""
        <div style="margin-top:6px;margin-bottom:4px;color:#0891B2;font-size:10px;
                    font-weight:800;text-transform:uppercase;letter-spacing:.06em;">
        SOCIAL AUDIENCE — GROUP OUTREACH CHANNEL
        </div>
        """, unsafe_allow_html=True)
        cols = st.columns(4)
        for col, lbl, val, note, color in [
            (cols[0], "Instagram Followers", f"{ig_f:,}" if ig_f else "–", "VDP @visitdanapoint", "#E1306C"),
            (cols[1], "Facebook Followers",  f"{fb_f:,}" if fb_f else "–", "VDP Facebook Page",   "#1877F2"),
            (cols[2], "IG Monthly Reach",    f"{ig_r:,}" if ig_r else "–", "unique accounts reached", "#A78BFA"),
            (cols[3], "IG Engagement Rate",  f"{ig_e:.1f}%" if ig_e else "–", "30-day avg", "#10B981"),
        ]:
            with col:
                st.markdown(_metric_box(lbl, val, note, color), unsafe_allow_html=True)

    st.markdown("<div style='margin-top:18px;'></div>", unsafe_allow_html=True)

    # ── A. Competitive Set Positioning ───────────────────────────────────────
    if df_comp is not None and not df_comp.empty:
        st.markdown("""
        <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                    letter-spacing:.06em;margin-bottom:10px;">
        A. COMPETITIVE SET POSITIONING — How Dana Point Properties Index Against the Set
        </div>
        """, unsafe_allow_html=True)

        col_scatter, col_index = st.columns([2, 1])
        with col_scatter:
            st.plotly_chart(_chart_competitive_scatter(df_comp), use_container_width=True,
                            key="gt_comp_scatter")
        with col_index:
            # Show MPI / ARI / RGI summary cards
            avg_mpi = df_comp["mpi"].mean() if "mpi" in df_comp.columns else None
            avg_ari = df_comp["ari"].mean() if "ari" in df_comp.columns else None
            avg_rgi = df_comp["rgi"].mean() if "rgi" in df_comp.columns else None
            for lbl, val, note, color in [
                ("Avg MPI", f"{avg_mpi:.1f}" if avg_mpi else "–",
                 "Market Penetration Index (100 = fair share)", "#0891B2"),
                ("Avg ARI", f"{avg_ari:.1f}" if avg_ari else "–",
                 "ADR Index (100 = fair share)", "#10B981"),
                ("Avg RGI", f"{avg_rgi:.1f}" if avg_rgi else "–",
                 "RevPAR Growth Index (100 = fair share)", "#F5B940"),
            ]:
                st.markdown(_metric_box(lbl, val, note, color), unsafe_allow_html=True)
                st.markdown("<div style='margin-bottom:10px;'></div>", unsafe_allow_html=True)

    # ── B. TBID Waterfall by Chain Scale ─────────────────────────────────────
    st.markdown("""
    <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                letter-spacing:.06em;margin:14px 0 10px;">
    B. TBID WATERFALL BY CHAIN SCALE — rooms × ADR × 28% group share × 1.25% TBID rate
    </div>
    """, unsafe_allow_html=True)
    if not df_chain.empty:
        col_wf, col_meta = st.columns([3, 1])
        with col_wf:
            st.plotly_chart(_chart_tbid_chain_waterfall(df_chain, g),
                            use_container_width=True, key="gt_tbid_waterfall")
        with col_meta:
            tbid_target = 4_100_000
            tbid_actual = g.get("estimated_group_tbid_rev_low", 3_603_940)
            gap = tbid_target - tbid_actual
            st.markdown(_metric_box(
                "Gap to $4.1M Target",
                f"${gap/1000:.0f}K",
                "midpoint TBID group target",
                "#EF4444" if gap > 0 else "#22C55E",
            ), unsafe_allow_html=True)
    else:
        st.info("Chain scale breakdown data not loaded. Run the pipeline.")

    # ── C. Supply Pipeline Timeline ───────────────────────────────────────────
    if df_pipeline is not None and not df_pipeline.empty:
        st.markdown("""
        <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                    letter-spacing:.06em;margin:14px 0 10px;">
        C. SUPPLY PIPELINE TIMELINE — Upcoming Hotel Openings
        </div>
        """, unsafe_allow_html=True)
        st.plotly_chart(_chart_supply_pipeline(df_pipeline), use_container_width=True,
                        key="gt_supply_pipeline")
        # Detail table
        show_cols = ["property_name", "rooms", "chain_scale", "status",
                     "projected_open_date", "brand"]
        show_cols = [c for c in show_cols if c in df_pipeline.columns]
        if show_cols:
            st.dataframe(df_pipeline[show_cols].rename(columns={
                "property_name": "Property",
                "rooms": "Rooms",
                "chain_scale": "Scale",
                "status": "Status",
                "projected_open_date": "Est. Open",
                "brand": "Brand",
            }), use_container_width=True, hide_index=True)

    # ── D. Attribution Channel Contribution ───────────────────────────────────
    if (web_groups is not None and not web_groups.empty) or \
       (med_groups is not None and not med_groups.empty):
        st.markdown("""
        <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                    letter-spacing:.06em;margin:14px 0 10px;">
        D. ATTRIBUTION CHANNEL CONTRIBUTION — Website vs Media Group Trips & Impact
        </div>
        """, unsafe_allow_html=True)
        col_attr, col_gap = st.columns([3, 1])
        with col_attr:
            st.plotly_chart(
                _chart_attribution_channel(
                    web_groups if web_groups is not None else pd.DataFrame(),
                    med_groups if med_groups is not None else pd.DataFrame(),
                ),
                use_container_width=True,
                key="gt_attr_channel",
            )
        with col_gap:
            total_impact = 0.0
            if web_groups is not None and not web_groups.empty:
                total_impact += float(web_groups["est_impact_usd"].sum())
            if med_groups is not None and not med_groups.empty:
                total_impact += float(med_groups["est_impact_usd"].sum())
            tbid_from_impact = total_impact * TBID_RATE
            tbid_gap = 4_100_000 - tbid_from_impact
            st.markdown(_metric_box(
                "Attribution Impact",
                f"${total_impact/1e6:.2f}M",
                "website + media combined",
                "#10B981",
            ), unsafe_allow_html=True)
            st.markdown("<div style='margin:8px 0;'></div>", unsafe_allow_html=True)
            st.markdown(_metric_box(
                "TBID from Attribution",
                f"${tbid_from_impact/1000:.0f}K",
                f"gap to $4.1M: ${tbid_gap/1000:.0f}K",
                "#F5B940" if tbid_gap > 0 else "#22C55E",
            ), unsafe_allow_html=True)

    # ── STR Group Segment Data Visualizations ────────────────────────────────
    if not df_group.empty:
        st.markdown("""
        <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                    letter-spacing:.06em;margin:14px 0 10px;">
        E. STR GROUP-SEGMENT ANALYTICS — Live Demand Trends
        </div>
        """, unsafe_allow_html=True)

        # 1. Group vs Transient Trends
        st.plotly_chart(_chart_group_vs_transient_trends(df_group), use_container_width=True,
                       key="gt_group_trans_trends")

        # 2. Segment Mix (Donut) and ADR Premium side-by-side
        col_donut, col_premium = st.columns(2)
        with col_donut:
            st.plotly_chart(_chart_segment_mix_donut(df_group), use_container_width=True,
                           key="gt_segment_donut")
        with col_premium:
            st.plotly_chart(_chart_group_adr_premium(df_group), use_container_width=True,
                           key="gt_adr_premium")

        # 3. Group Event Correlation and Property Performance side-by-side
        col_event, col_prop = st.columns(2)
        with col_event:
            st.plotly_chart(_chart_group_event_correlation(df_group), use_container_width=True,
                           key="gt_group_event")
        with col_prop:
            st.plotly_chart(_chart_property_group_performance(df_group), use_container_width=True,
                           key="gt_prop_perf")

        # 4. Occupancy Heatmap
        st.plotly_chart(_chart_segment_occupancy_heatmap(df_group), use_container_width=True,
                       key="gt_heatmap")

        st.markdown("""
        <div style="color:#64748B;font-size:11px;margin-top:12px;line-height:1.6;">
        📊 <strong>Data Source:</strong> STR multi-segment weekly/monthly exports (Trans., Grp., Cont. segments) |
        <strong>Updated:</strong> """ + df_group["as_of_date"].max().strftime("%b %d, %Y") + """ |
        <strong>Coverage:</strong> """ + str(len(df_group["property"].unique())) + """ properties
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background:rgba(8,145,178,0.04);border:1px dashed rgba(8,145,178,0.30);
                    border-radius:8px;padding:14px 18px;margin:14px 0;">
          <div style="color:#38BDF8;font-weight:700;font-size:11.5px;margin-bottom:6px;">
            🔜 STR GROUP-SEGMENT DATA — READY TO LOAD
          </div>
          <div style="color:#94A3B8;font-size:11px;line-height:1.7;">
            STR group-segment exports have been configured for live ingestion. Visualizations will
            appear here automatically once data is downloaded via <code>python scripts/fetch_str_dropbox.py</code>
            and loaded via <code>python scripts/load_str_multiseg.py</code>.<br>
            Run the pipeline to populate live <strong>group demand rooms</strong>, <strong>group ADR actuals</strong>,
            and <strong>segment composition</strong> analyses.
          </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top:14px;'></div>", unsafe_allow_html=True)

    # ── Actionable insights (by audience) ────────────────────────────────────
    if not insights.empty:
        _AUD_ORDER = ["dmo", "city", "cross", "visitor", "resident"]
        _AUD_LABELS = {
            "dmo":      ("🏢 DMO / TBID Board", "#0891B2"),
            "city":     ("🏛 City of Dana Point", "#4A6FA5"),
            "visitor":  ("✈️ Visitors", "#F59E0B"),
            "resident": ("🏡 Residents", "#6A4F8A"),
            "cross":    ("🔀 Hidden Signal", "#EF4444"),
        }
        _ACTION_BY_AUD = {
            "dmo":      "Integrate into VDP marketing and TBID strategy",
            "city":     "Present to City Council for infrastructure/policy decision",
            "visitor":  "Surface on visitdanapoint.com for trip-planning guidance",
            "resident": "Share via Dana Point community newsletter/social",
            "cross":    "Escalate to senior leadership — non-obvious signal",
        }
        for _aud in _AUD_ORDER:
            _aud_rows = insights[insights["audience"] == _aud] if "audience" in insights.columns else pd.DataFrame()
            if _aud_rows.empty:
                continue
            _albl, _aclr = _AUD_LABELS.get(_aud, ("Insight", "#64748B"))
            st.markdown(f"""
            <div style="color:{_aclr};font-size:10px;font-weight:700;text-transform:uppercase;
                        letter-spacing:.06em;margin:14px 0 8px;">{_albl} — ACTIONABLE INTELLIGENCE</div>
            """, unsafe_allow_html=True)
            for _, row in _aud_rows.iterrows():
                pri    = int(row.get("priority") or 5)
                icon   = "🎯" if pri <= 2 else ("⚠️" if pri <= 4 else "ℹ️")
                _bdy   = str(row.get("body", ""))
                # Show first 2 sentences (up to 500 chars)
                _bddot = _bdy.find(". ", 80)
                _bdy_s = _bdy[:_bddot+1 if 0 < _bddot < 500 else 500].rstrip() + ("…" if len(_bdy) > 500 and _bddot < 0 else "")
                st.markdown(_action_card(
                    icon=icon,
                    headline=str(row.get("headline", ""))[:140],
                    body=_bdy_s,
                    action=_ACTION_BY_AUD.get(_aud, "Review and integrate into strategy"),
                    accent=_aclr,
                ), unsafe_allow_html=True)

    # ── STR Data Status ───────────────────────────────────────────────────────
    if not df_group.empty:
        st.markdown(f"""
        <div style="background:rgba(16,185,129,0.06);border:1px solid rgba(16,185,129,0.30);
                    border-radius:8px;padding:12px 16px;margin-top:12px;">
          <div style="color:#10B981;font-weight:700;font-size:11px;margin-bottom:4px;">
            ✅ STR GROUP-SEGMENT DATA — LIVE
          </div>
          <div style="color:#94A3B8;font-size:10.5px;">
            {len(df_group)} segment records loaded · {df_group["as_of_date"].max().strftime("%b %d, %Y")} latest update ·
            Covering {len(df_group["property"].unique())} properties and 3 segments (Transient, Group, Contract)
          </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background:rgba(245,158,11,0.06);border:1px dashed rgba(245,158,11,0.35);
                    border-radius:8px;padding:14px 18px;margin-top:12px;">
          <div style="color:#FCD34D;font-weight:700;font-size:11.5px;margin-bottom:6px;">
            ⏳ STR GROUP-SEGMENT DATA — WAITING FOR DATA
          </div>
          <div style="color:#94A3B8;font-size:11px;line-height:1.7;">
            STR group-segment exports are configured for auto-ingestion.
            Visualizations will populate automatically once data is fetched and loaded.<br>
            To load data now: Run <code>python scripts/run_pipeline.py</code>
          </div>
        </div>
        """, unsafe_allow_html=True)


def _render_traveler_types(df_types: pd.DataFrame) -> None:
    if df_types.empty:
        st.info("Traveler type data not loaded. Run `python scripts/run_pipeline.py`.")
        return

    st.markdown("""
    <div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:10px;padding:16px 20px;
                margin-bottom:16px;">
      <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                  letter-spacing:.08em;margin-bottom:6px;">WHY THIS MATTERS</div>
      <div style="color:#334155;font-size:12.5px;line-height:1.7;">
        Not all travelers are equal. Business travelers generate <strong style="color:#0F172A;">highest revenue</strong>
        but represent only 20% of volume — they fund 60% of hotel revenue nationally.
        SMERF groups (shoulder-season buyers) are the <em>lowest displacement risk</em> because
        they book 8–48 weeks out into periods when transient demand is soft.
        Knowing which type Dana Point over- or under-indexes tells us where to invest marketing dollars.
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Radar chart
    st.plotly_chart(_chart_traveler_radar(df_types), use_container_width=True, key="gt_radar")

    st.markdown("<div style='margin:12px 0 6px;'></div>", unsafe_allow_html=True)

    # Revenue contribution breakdown
    rev_order = {"highest": 0, "high": 1, "medium": 2, "low": 3}
    df_sorted = df_types.copy()
    df_sorted["_order"] = df_sorted["revenue_contribution"].map(rev_order).fillna(9)
    df_sorted = df_sorted.sort_values("_order")

    st.markdown("""
    <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                letter-spacing:.06em;margin-bottom:10px;">TRAVELER TYPE PROFILES — SORTED BY REVENUE CONTRIBUTION</div>
    """, unsafe_allow_html=True)

    for _, row in df_sorted.iterrows():
        t_type  = str(row.get("traveler_type", "")).title()
        rev     = str(row.get("revenue_contribution", "medium")).title()
        los_h   = row.get("typical_los_nights_high", 3)
        bkw_l   = row.get("booking_window_weeks_low", 1)
        bkw_h   = row.get("booking_window_weeks_high", 4)
        season  = str(row.get("seasonal_pattern", "year_round")).replace("_", " ").title()
        motiv   = str(row.get("primary_motivation", "")).replace("_", " ").title()
        group_s = str(row.get("group_size_typical", "")).replace("_", " ")

        rev_colors = {"Highest": "#10B981", "High": "#0891B2", "Medium": "#F5B940", "Low": "#EF4444"}
        rc = rev_colors.get(rev, "#64748B")

        # Booking window relative to SMERF (8–48 wks) for group outreach tip
        try:
            bkw_h_int = int(bkw_h)
        except Exception:
            bkw_h_int = 4
        if bkw_h_int >= 8:
            group_tip = f"✅ Book {bkw_l}–{bkw_h} wks out — ideal for group sales outreach"
        else:
            group_tip = f"⚡ Short booking window ({bkw_l}–{bkw_h} wks) — last-minute fill strategy"

        st.markdown(f"""
        <div style="background:linear-gradient(180deg,{rc}08 0%,#FFFFFF 60%);
                    border:1px solid #E2E8F0;border-left:3px solid {rc};
                    border-radius:8px;padding:12px 16px;margin-bottom:8px;
                    box-shadow:0 1px 2px rgba(15,23,42,0.05);">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px;">
            <div>
              <span style="color:#0F172A;font-family:'Outfit',sans-serif;font-weight:800;font-size:14px;">{t_type}</span>
              <span style="color:{rc};font-size:10px;font-weight:800;text-transform:uppercase;
                           letter-spacing:.06em;margin-left:10px;">{rev} Revenue</span>
            </div>
            <div style="color:#64748B;font-size:10.5px;font-weight:600;">{season}</div>
          </div>
          <div style="color:#475569;font-size:11.5px;margin-top:4px;line-height:1.5;">{motiv}</div>
          <div style="display:flex;gap:24px;margin-top:8px;flex-wrap:wrap;">
            <div><span style="color:#64748B;font-size:10px;">LOS up to </span>
                 <span style="color:#0F172A;font-weight:700;font-size:11px;">{los_h} nights</span></div>
            <div><span style="color:#64748B;font-size:10px;">Books </span>
                 <span style="color:#0F172A;font-weight:700;font-size:11px;">{bkw_l}–{bkw_h} wks out</span></div>
            {f'<div><span style="color:#64748B;font-size:10px;">Group size: </span><span style="color:#0F172A;font-weight:700;font-size:11px;">{group_s}</span></div>' if group_s else ''}
          </div>
          <div style="margin-top:8px;font-size:10.5px;color:#0891B2;font-weight:600;">{group_tip}</div>
        </div>
        """, unsafe_allow_html=True)


def _render_national_context(df_segs: pd.DataFrame, df_biz: pd.DataFrame,
                              g: dict,
                              df_costar_monthly: Optional[pd.DataFrame] = None,
                              web_groups: Optional[pd.DataFrame] = None,
                              df_types: Optional[pd.DataFrame] = None,
                              comp: Optional[pd.DataFrame] = None) -> None:
    # National summary callout
    total_group = 319
    meetings_b  = 126
    spectator_b = 102
    sports_b    = 52
    leisure_b   = 39
    meetings_rec = 82   # % recovery vs 2019

    st.markdown(f"""
    <div style="background:linear-gradient(135deg,rgba(8,145,178,0.07),rgba(167,139,250,0.05));
                border:1px solid #BAE6FD;border-radius:12px;padding:18px 22px;
                margin-bottom:16px;box-shadow:0 1px 3px rgba(15,23,42,0.06);">
      <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                  letter-spacing:.1em;margin-bottom:10px;">U.S. GROUP TRAVEL — NATIONAL PICTURE</div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px;">
        <div style="text-align:center;">
          <div style="color:#0F172A;font-family:'Outfit',sans-serif;font-size:22px;font-weight:900;letter-spacing:-0.02em;">${total_group}B</div>
          <div style="color:#475569;font-size:10px;font-weight:600;">Total Group Travel</div>
        </div>
        <div style="text-align:center;">
          <div style="color:#059669;font-family:'Outfit',sans-serif;font-size:22px;font-weight:900;letter-spacing:-0.02em;">{meetings_rec}%</div>
          <div style="color:#475569;font-size:10px;font-weight:600;">Meetings Recovery (2019)</div>
        </div>
        <div style="text-align:center;">
          <div style="color:#D97706;font-family:'Outfit',sans-serif;font-size:22px;font-weight:900;letter-spacing:-0.02em;">3M+</div>
          <div style="color:#475569;font-size:10px;font-weight:600;">U.S. Jobs Supported</div>
        </div>
        <div style="text-align:center;">
          <div style="color:#7C3AED;font-family:'Outfit',sans-serif;font-size:22px;font-weight:900;letter-spacing:-0.02em;">$188B</div>
          <div style="color:#475569;font-size:10px;font-weight:600;">Meetings 2026 Projected</div>
        </div>
      </div>
      <div style="color:#334155;font-size:12px;line-height:1.7;">
        Meetings &amp; events (<strong style="color:#0F172A;">$126B</strong>) are the largest group segment and at 82% recovery — still growing.
        Live spectator sports (<strong style="color:#0F172A;">$102B</strong>) and participatory sports (<strong style="color:#0F172A;">$52B</strong>) are segments where
        Dana Point can directly compete via surf events (Doheny), sailing regattas,
        and ocean-sports tourism. Leisure group travel (<strong style="color:#0F172A;">$39B</strong>) is fully recovered.
      </div>
    </div>
    """, unsafe_allow_html=True)

    col_a, col_b = st.columns(2)
    with col_a:
        if not df_segs.empty:
            st.plotly_chart(_chart_segment_donut(df_segs), use_container_width=True, key="gt_donut")
    with col_b:
        st.plotly_chart(_chart_revenue_funnel(g), use_container_width=True, key="gt_funnel")

    # Business travel context
    if not df_biz.empty:
        biz_total = df_biz[df_biz["category"] == "total_business"]
        if not biz_total.empty:
            biz_s   = biz_total.iloc[0]
            biz_sp  = float(biz_s.get("spend_billion_usd") or 312)
            biz_rec = float(biz_s.get("pct_recovery_vs_2019") or 85)
            biz_lod = float(biz_s.get("pct_total_lodging_rev") or 60)
            st.markdown(f"""
            <div style="background:rgba(8,145,178,0.05);border:1px solid #BAE6FD;
                        border-radius:10px;padding:16px 20px;margin-top:14px;
                        box-shadow:0 1px 3px rgba(15,23,42,0.06);">
              <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                          letter-spacing:.08em;margin-bottom:10px;">BUSINESS TRAVEL — HOTEL IMPACT</div>
              <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;">
                <div style="text-align:center;">
                  <div style="color:#0F172A;font-family:'Outfit',sans-serif;font-size:22px;font-weight:900;letter-spacing:-0.02em;">${biz_sp:.0f}B</div>
                  <div style="color:#475569;font-size:10px;font-weight:600;">National Business Travel Spend</div>
                  <div style="color:#94A3B8;font-size:9.5px;">{biz_rec:.0f}% of 2019 recovery</div>
                </div>
                <div style="text-align:center;">
                  <div style="color:#0F172A;font-family:'Outfit',sans-serif;font-size:22px;font-weight:900;letter-spacing:-0.02em;">{biz_lod:.0f}%</div>
                  <div style="color:#475569;font-size:10px;font-weight:600;">of Hotel Revenue</div>
                  <div style="color:#94A3B8;font-size:9.5px;">from just 20% of travelers</div>
                </div>
                <div style="text-align:center;">
                  <div style="color:#D97706;font-family:'Outfit',sans-serif;font-size:22px;font-weight:900;letter-spacing:-0.02em;">5×</div>
                  <div style="color:#475569;font-size:10px;font-weight:600;">Revenue per Traveler</div>
                  <div style="color:#94A3B8;font-size:9.5px;">vs. average leisure visitor</div>
                </div>
              </div>
              <div style="margin-top:10px;background:#FFFFFF;border:1px solid #E2E8F0;border-radius:6px;
                          padding:8px 12px;color:#334155;font-size:11.5px;line-height:1.6;">
                <strong style="color:#0F172A;">Dana Point implication:</strong> Corporate meeting planners evaluating
                South OC coastal venues represent the highest-value group segment. Waldorf Astoria
                and Ritz-Carlton carry national brand recognition that opens doors to corporate
                contracting. TBID investment in group sales missions targeting these planners
                has the highest potential ROI.
              </div>
            </div>
            """, unsafe_allow_html=True)

    # Dana Point opportunities
    st.markdown("""
    <div style="margin-top:16px;">
      <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                  letter-spacing:.08em;margin-bottom:10px;">DANA POINT SEGMENT OPPORTUNITIES</div>
    </div>
    """, unsafe_allow_html=True)

    opportunities = [
        ("🏄", "Participatory Sports ($52B)", "#0891B2",
         "Doheny surf events, sailing regattas, ocean sports tours target this fast-growing segment. "
         "Dana Point's natural assets position it as a West Coast hub.",
         "Develop 'Sports & Adventure' group package with participating hotels + TBID co-investment"),
        ("🎪", "Meetings & Events ($126B)", "#10B981",
         "82% recovery and growing. Waldorf Astoria and Ritz-Carlton can compete for corporate "
         "board retreats, incentive travel, and high-end association events.",
         "Launch group sales mission targeting corporate meeting planners in LA, San Diego, Phoenix"),
        ("👥", "SMERF Groups ($39B leisure group)", "#F5B940",
         "Social, Military, Educational, Religious, Fraternal — primary shoulder-season buyer. "
         "Books 8–48 weeks out into Q1/Q4 when displacement risk is lowest.",
         "Create SMERF rate program for Jan–Mar and Oct–Dec with dedicated group blocks"),
    ]
    for icon, title, accent, body, action in opportunities:
        st.markdown(_action_card(icon, title, body, action, accent), unsafe_allow_html=True)

    # ── E. CoStar Monthly Occ overlay ─────────────────────────────────────────
    if (df_costar_monthly is not None and not df_costar_monthly.empty) or \
       (web_groups is not None and not web_groups.empty):
        st.markdown("""
        <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                    letter-spacing:.08em;margin:18px 0 10px;">
        E. COSTAR MONTHLY OCC vs DATAFY GROUP ATTRIBUTION
        </div>
        """, unsafe_allow_html=True)
        st.plotly_chart(
            _chart_costar_occ_overlay(
                df_costar_monthly if df_costar_monthly is not None else pd.DataFrame(),
                web_groups if web_groups is not None else pd.DataFrame(),
            ),
            use_container_width=True,
            key="gt_costar_occ_overlay",
        )

    # ── F. Shoulder Season Alignment Matrix ───────────────────────────────────
    if df_types is not None and not df_types.empty and comp is not None and not comp.empty:
        st.markdown("""
        <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                    letter-spacing:.08em;margin:18px 0 10px;">
        F. SHOULDER SEASON ALIGNMENT MATRIX — Traveler Types × Quarters
        </div>
        """, unsafe_allow_html=True)
        _render_shoulder_alignment(df_types, comp)


def _render_ai_analyst(ai_keys: dict, selected_model: str) -> None:
    """AI Analyst sub-tab with group travel pre-loaded prompts."""
    GROUP_PROMPTS = [
        ("🎯 TBID Opportunity",
         "Based on our group intelligence data, what is the estimated TBID revenue opportunity "
         "from growing group demand share by 5 percentage points, and which traveler segments "
         "should we prioritize to achieve this with the least displacement risk?"),
        ("📅 Shoulder Season Strategy",
         "Analyze our occupancy pattern and compression days. Which months have the lowest "
         "displacement risk for group business? What types of groups (SMERF, corporate, sports) "
         "should we target by quarter to maximize TBID without cannibalizing peak transient revenue?"),
        ("🏆 National Benchmark Gap",
         "Compare Dana Point's estimated group demand share to national benchmarks. "
         "Are we above or below the STR/CBRE industry standard of 25–32% for upper-upscale "
         "coastal resort markets? What would it take to reach the top of that range?"),
        ("💼 Corporate Sales Pitch",
         "Write a compelling one-paragraph pitch for a corporate meeting planner explaining "
         "why Dana Point should be their next incentive travel destination, using our actual "
         "ADR, occupancy, and amenity data."),
        ("📊 Board Report Summary",
         "Generate a board-ready executive summary of our group business intelligence, "
         "including TBID opportunity, displacement risk assessment, and 3 recommended actions "
         "for the next 12 months."),
    ]

    st.markdown("""
    <div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:10px;padding:16px 20px;
                margin-bottom:16px;">
      <div style="color:#0891B2;font-size:10px;font-weight:800;text-transform:uppercase;
                  letter-spacing:.08em;margin-bottom:6px;">AI GROUP TRAVEL ANALYST</div>
      <div style="color:#334155;font-size:12px;line-height:1.6;">
        Ask the AI analyst a question about group travel strategy, or use one of the
        pre-loaded prompts below. The AI has full access to our group intelligence data,
        U.S. Travel benchmarks, and CoStar market data.
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**Pre-loaded Group Travel Prompts**")
    prompt_cols = st.columns(len(GROUP_PROMPTS))
    selected_prompt = ""
    for i, (label, prompt_text) in enumerate(GROUP_PROMPTS):
        with prompt_cols[i]:
            if st.button(label, key=f"gt_prompt_{i}", use_container_width=True):
                st.session_state["gt_ai_prompt"]     = prompt_text
                st.session_state["gt_ai_needs_call"] = True

    custom = st.text_area(
        "Or type your own group travel question:",
        value=st.session_state.get("gt_ai_prompt", ""),
        height=100,
        key="gt_ai_custom",
        placeholder="E.g., What shoulder months should we target for SMERF group blocks?",
    )
    if custom:
        st.session_state["gt_ai_prompt"] = custom

    if st.button("🔍 Ask AI Analyst", type="primary", key="gt_ai_submit"):
        st.session_state["gt_ai_needs_call"] = True

    if st.session_state.get("gt_ai_needs_call") and st.session_state.get("gt_ai_prompt"):
        # Delegate to the main app's AI panel by setting shared session keys
        st.session_state["ai_current_prompt"] = st.session_state["gt_ai_prompt"]
        st.session_state["ai_prompt_label"]   = "Group & Travel Analysis"
        st.session_state["ai_needs_call"]     = True
        st.session_state["gt_ai_needs_call"]  = False
        st.info("Prompt queued for AI Analyst. Switch to **Today's Overview → AI Assistant** tab to see the response, or scroll up if already there.")


# ── Main entry point ──────────────────────────────────────────────────────────

def render_group_tab(ai_keys: dict | None = None, selected_model: str = "claude") -> None:
    """Render the full Group & Traveler Intelligence tab."""

    # Load all data
    g                 = _load_group_intel()
    df_monthly        = _load_monthly_occ()
    df_segs           = _load_ust_segments()
    df_biz            = _load_ust_biz()
    df_types          = _load_traveler_types()
    insights          = _load_group_insights()
    social            = _load_social_summary()
    df_chain          = _load_costar_chain()
    df_comp           = _load_competitive_set()
    df_pipeline       = _load_supply_pipeline()
    web_groups, med_groups = _load_attribution_groups()
    df_costar_monthly = _load_costar_monthly()

    # Load compression for shoulder matrix
    try:
        _comp_conn = sqlite3.connect(_DB_PATH, timeout=10)
        df_compression = pd.read_sql_query(
            "SELECT quarter, days_above_80_occ, days_above_90_occ FROM kpi_compression_quarterly ORDER BY quarter",
            _comp_conn,
        )
        _comp_conn.close()
    except Exception as _exc:
        import logging; logging.getLogger("vdp_dashboard").debug("compression load failed: %s", _exc)
        df_compression = pd.DataFrame()

    # Hero banner
    tbid_low  = g.get("estimated_group_tbid_rev_low",  3_603_940)
    tbid_high = g.get("estimated_group_tbid_rev_high", 4_613_043)
    st.markdown(f"""
    <div class="hero-banner" style="margin-bottom:0;">
      <div class="hero-title">👥 Group &amp; Traveler Intelligence</div>
      <div class="hero-subtitle">
        Est. Group TBID Contribution: <strong>${tbid_low/1e6:.1f}M–${tbid_high/1e6:.1f}M/yr</strong>
        · U.S. Group Travel: <strong>$319B</strong> national market
        · Shoulder-Season Group Strategy · SMERF &amp; Corporate Targeting
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top:12px;'></div>", unsafe_allow_html=True)

    # Data quality notice
    if not g:
        st.warning("Group intelligence data not loaded. Run `python scripts/run_pipeline.py`.")
        return

    st.info(
        "📌 **Data context:** Group demand estimates use CoStar chain-scale supply + "
        "industry benchmarks (STR/CBRE 2024 upper-upscale coastal resort averages). "
        "Actual STR group-segment data will replace benchmarks when provided. "
        "U.S. Travel benchmarks sourced from U.S. Travel Association 2024 reports."
    )

    sub_strategy, sub_types, sub_national, sub_ai = st.tabs([
        "🎯 Group Strategy",
        "🧭 Traveler Types",
        "🌐 National Context",
        "🤖 AI Analyst",
    ])

    with sub_strategy:
        _render_group_strategy(
            g, df_monthly, df_chain, insights, social,
            df_comp=df_comp,
            df_pipeline=df_pipeline,
            web_groups=web_groups,
            med_groups=med_groups,
        )

    with sub_types:
        _render_traveler_types(df_types)

    with sub_national:
        _render_national_context(
            df_segs, df_biz, g,
            df_costar_monthly=df_costar_monthly,
            web_groups=web_groups,
            df_types=df_types,
            comp=df_compression,
        )

    with sub_ai:
        _render_ai_analyst(ai_keys or {}, selected_model)
