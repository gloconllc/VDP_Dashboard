"""
Live mini-visualizations for the "View a Section" cards, plus the upgraded
feeder-market map.

Every card on the report page used to show only a rasterized thumbnail of the
matching PDF page. A thumbnail is a picture of a chart, not a chart: it cannot
be hovered, it carries no current figure, and at card size it is unreadable.
Each builder here reads analytics.sqlite directly and returns a small, real
Plotly figure plus a one-line caption, so every section card leads with a live
number from the same source its PDF page is built from.

Design rules for these figures, since they render at roughly 330 x 190 px:
  - one series, one message, no legend
  - value labels drawn on the marks themselves rather than on an axis
  - no gridlines, no axis titles, transparent background
  - every query anchored to MAX(date) in its own table, never to the wall
    clock, since STR, CoStar, and Datafy each lag the calendar by a different
    amount (see CLAUDE.md Lessons Learned)
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

TEAL = "#1D6E86"
TEAL_DK = "#123C4A"
TEAL_LT = "#7FD6C4"
AMBER = "#B45309"
GREEN = "#1D9E6F"
SLATE = "#475569"
GRID = "#E2E8F0"
INK = "#0B2530"
FONT = "-apple-system, Segoe UI, sans-serif"

CATEGORY_COLORS = [TEAL, TEAL_DK, AMBER, GREEN, TEAL_LT, SLATE, "#94A3B8", "#CBD9DE"]


def _mini_layout(fig: go.Figure, height: int = 190) -> go.Figure:
    """Shared layout for card-sized figures. Deliberately strips the axis
    furniture a full-page chart needs, since at this size labels on the marks
    read faster than an axis the viewer has to trace back to."""
    fig.update_layout(
        height=height,
        margin=dict(l=4, r=8, t=6, b=4),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=INK, family=FONT, size=10.5),
        showlegend=False,
        hoverlabel=dict(bgcolor="#FFFFFF", font_size=11, font_family=FONT),
    )
    fig.update_xaxes(showgrid=False, zeroline=False, title=None)
    fig.update_yaxes(showgrid=False, zeroline=False, title=None)
    return fig


def _q(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# 1. Hotel Performance - Occupancy: occupancy by day of week
# ---------------------------------------------------------------------------

@st.cache_data(ttl=1800, show_spinner=False)
def _occupancy_dow(_conn, trailing_days: int = 180) -> pd.DataFrame:
    df = _q(
        _conn,
        "SELECT as_of_date, occ_pct FROM kpi_daily_summary "
        "WHERE occ_pct IS NOT NULL AND as_of_date >= "
        "  date((SELECT MAX(as_of_date) FROM kpi_daily_summary), ?) "
        "ORDER BY as_of_date",
        (f"-{trailing_days} day",),
    )
    if df.empty:
        return df
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df.dropna(subset=["as_of_date"])
    if df.empty:
        return df
    df["dow"] = df["as_of_date"].dt.dayofweek
    out = df.groupby("dow", as_index=False)["occ_pct"].mean()
    labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    out["label"] = out["dow"].map(lambda d: labels[int(d)])
    out["n_days"] = len(df)
    out["period_end"] = df["as_of_date"].max().date().isoformat()
    return out.sort_values("dow")


def _fig_occupancy_dow(conn):
    df = _occupancy_dow(conn)
    if df.empty:
        return None
    peak = df.loc[df["occ_pct"].idxmax()]
    trough = df.loc[df["occ_pct"].idxmin()]
    colors = [AMBER if v == peak["occ_pct"] else TEAL for v in df["occ_pct"]]
    fig = go.Figure(go.Bar(
        x=df["label"], y=df["occ_pct"],
        marker=dict(color=colors),
        text=[f"{v:.0f}%" for v in df["occ_pct"]],
        textposition="outside", textfont=dict(size=9.5),
        hovertemplate="%{x}: %{y:.1f}% occupancy<extra></extra>",
        cliponaxis=False,
    ))
    fig.update_yaxes(range=[0, max(df["occ_pct"]) * 1.22], showticklabels=False)
    caption = (
        f"{peak['label']} runs hottest at {peak['occ_pct']:.1f}% occupancy, "
        f"{trough['label']} softest at {trough['occ_pct']:.1f}%. "
        f"STR, trailing {int(df['n_days'].iloc[0])} reported days through {df['period_end'].iloc[0]}."
    )
    return _mini_layout(fig), caption


# ---------------------------------------------------------------------------
# 2. Hotel Performance - ADR & Compression: compression days by quarter
# ---------------------------------------------------------------------------

@st.cache_data(ttl=1800, show_spinner=False)
def _compression(_conn, quarters: int = 8) -> pd.DataFrame:
    df = _q(
        _conn,
        "SELECT quarter, days_above_80_occ, days_above_90_occ "
        "FROM kpi_compression_quarterly ORDER BY quarter DESC LIMIT ?",
        (quarters,),
    )
    return df.sort_values("quarter") if not df.empty else df


def _fig_compression(conn, quarters: int = 8, height: int = 190):
    df = _compression(conn, quarters=quarters)
    if df.empty:
        return None
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["quarter"], y=df["days_above_80_occ"], name="80%+",
        marker=dict(color=TEAL), width=0.68,
        hovertemplate="%{x}: %{y} days above 80%<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=df["quarter"], y=df["days_above_90_occ"], name="90%+",
        marker=dict(color=AMBER), width=0.30,
        hovertemplate="%{x}: %{y} days above 90%<extra></extra>",
    ))
    fig.update_layout(barmode="overlay", bargap=0.3, showlegend=height > 220,
                       legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0))
    if height > 220:
        # _mini_layout's t=6 top margin has no room for the horizontal
        # legend this size turns on; give it real headroom instead of
        # letting the legend clip against the card border. t=40 (up from
        # 34) adds a bit more buffer for narrow/portrait mobile widths.
        fig.update_layout(margin=dict(l=4, r=8, t=40, b=4))
    fig.update_xaxes(tickfont=dict(size=9 if height <= 220 else 11), tickangle=-35)
    fig.update_yaxes(showticklabels=True, tickfont=dict(size=9 if height <= 220 else 11),
                     showgrid=True, gridcolor=GRID)
    latest = df.iloc[-1]
    caption = (
        f"{latest['quarter']}: {int(latest['days_above_80_occ'])} days above 80% occupancy, "
        f"{int(latest['days_above_90_occ'])} of them above 90%. The narrow amber bar is the "
        "90%-plus subset of the teal bar. Source: STR."
    )
    return _mini_layout(fig, height=height), caption


# ---------------------------------------------------------------------------
# 3. Visitor Origins: top feeder markets
# ---------------------------------------------------------------------------

@st.cache_data(ttl=1800, show_spinner=False)
def _feeder_markets(_conn, limit: int = 6) -> pd.DataFrame:
    df = _q(
        _conn,
        "SELECT dma, spend_share_pct * 100 AS share_pct FROM datafy_overview_spending_by_market "
        "WHERE report_period_start = (SELECT MAX(report_period_start) FROM datafy_overview_spending_by_market) "
        "ORDER BY spend_share_pct DESC LIMIT ?",
        (limit,),
    )
    if not df.empty:
        df["metric"] = "share of visitor spend"
        return df
    df = _q(
        _conn,
        "SELECT dma, trips_share_pct AS share_pct FROM datafy_overview_top_markets "
        "WHERE report_period_start = (SELECT MAX(report_period_start) FROM datafy_overview_top_markets) "
        "ORDER BY trips_share_pct DESC LIMIT ?",
        (limit,),
    )
    if not df.empty:
        df["metric"] = "share of trips"
    return df


def _fig_feeder_markets(conn, limit: int = 6, height: int = 200):
    df = _feeder_markets(conn, limit=limit)
    if df.empty:
        return None
    df = df.sort_values("share_pct")
    # One color per bar (cycled from the app's validated categorical set)
    # instead of a single-hue bar with only the leader picked out. A ranked
    # bar chart is not a persistent category encoding the way a pie's
    # categories are, so cycling the palette by rank position is safe here.
    n = len(df)
    bar_colors = [CATEGORY_COLORS[i % len(CATEGORY_COLORS)] for i in range(n)][::-1]
    fig = go.Figure(go.Bar(
        x=df["share_pct"], y=df["dma"], orientation="h",
        marker=dict(color=bar_colors),
        text=[f"{v:.1f}%" for v in df["share_pct"]],
        textposition="outside", textfont=dict(size=9.5),
        hovertemplate="%{y}: %{x:.1f}%<extra></extra>",
        cliponaxis=False,
    ))
    fig.update_xaxes(showticklabels=False, range=[0, df["share_pct"].max() * 1.28])
    fig.update_yaxes(tickfont=dict(size=9), automargin=True,
                     ticks="outside", ticklen=5, tickcolor="rgba(0,0,0,0)")
    top = df.iloc[-1]
    caption = (
        f"{top['dma']} leads at {top['share_pct']:.1f}% {df['metric'].iloc[0]}. "
        "Source: Datafy, latest published period."
    )
    return _mini_layout(fig, height=height), caption


# ---------------------------------------------------------------------------
# 4. Market Segments: CoStar chain-scale RevPAR by tier
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600, show_spinner=False)
def _costar_tiers(_conn) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT report_scope, occupancy_pct, adr_usd, revpar_usd, year_label "
        "FROM costar_annual_performance "
        "WHERE market LIKE '%Dana Point%' AND year_label = 'YTD' "
        "  AND report_scope <> 'Overall' "
        "  AND report_date = (SELECT MAX(report_date) FROM costar_annual_performance "
        "                     WHERE market LIKE '%Dana Point%') "
        "ORDER BY revpar_usd DESC",
    )


_TIER_SHORT = {
    "Luxury & Upper Upscale": "Luxury /<br>Upper Upscale",
    "Upscale & Upper Midscale": "Upscale /<br>Upper Midscale",
    "Midscale & Economy": "Midscale /<br>Economy",
}


def _fig_costar_tiers(conn, height: int = 190):
    df = _costar_tiers(conn)
    if df.empty:
        return None
    df["label"] = df["report_scope"].map(lambda s: _TIER_SHORT.get(s, s))
    fig = go.Figure(go.Bar(
        x=df["label"], y=df["revpar_usd"],
        marker=dict(color=[TEAL_DK, TEAL, TEAL_LT][: len(df)]),
        text=[f"${v:,.0f}" for v in df["revpar_usd"]],
        textposition="outside", textfont=dict(size=9.5 if height <= 220 else 11.5),
        customdata=df[["occupancy_pct", "adr_usd"]].values,
        hovertemplate="%{x}<br>RevPAR $%{y:,.0f}<br>Occ %{customdata[0]:.1f}%"
                      "<br>ADR $%{customdata[1]:,.0f}<extra></extra>",
        cliponaxis=False,
    ))
    fig.update_xaxes(tickfont=dict(size=8.5 if height <= 220 else 11))
    fig.update_yaxes(showticklabels=False, range=[0, df["revpar_usd"].max() * 1.25])
    top = df.iloc[0]
    caption = (
        f"{top['report_scope']} leads the Newport Beach/Dana Point submarket at "
        f"${top['revpar_usd']:,.0f} RevPAR on {top['occupancy_pct']:.1f}% occupancy. "
        "Source: CoStar, year to date."
    )
    return _mini_layout(fig, height=height), caption


# ---------------------------------------------------------------------------
# 5. Chain-Scale Segment Detail: room inventory by tier
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600, show_spinner=False)
def _room_split(_conn) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT total_properties, total_rooms, luxury_upper_upscale_rooms, "
        "       upscale_upper_midscale_rooms, midscale_economy_rooms, report_date "
        "FROM costar_segment_room_split ORDER BY report_date DESC LIMIT 1",
    )


def _fig_room_split(conn, height: int = 190):
    df = _room_split(conn)
    if df.empty:
        return None
    row = df.iloc[0]
    labels = ["Luxury / Upper Upscale", "Upscale / Upper Midscale", "Midscale / Economy"]
    values = [
        row["luxury_upper_upscale_rooms"],
        row["upscale_upper_midscale_rooms"],
        row["midscale_economy_rooms"],
    ]
    if not any(pd.notna(v) and v for v in values):
        return None
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.58, sort=False,
        marker=dict(colors=[TEAL_DK, TEAL, TEAL_LT], line=dict(color="#FFFFFF", width=1.5)),
        textinfo="percent", textfont=dict(size=9.5, color="#FFFFFF"),
        hovertemplate="%{label}<br>%{value:,.0f} rooms (%{percent})<extra></extra>",
    ))
    total = row["total_rooms"]
    fig.add_annotation(
        text=f"<b>{total:,.0f}</b><br><span style='font-size:8.5px'>rooms</span>",
        showarrow=False, font=dict(size=13, color=INK), x=0.5, y=0.5,
    )
    caption = (
        f"{int(row['total_properties'])} properties, {total:,.0f} rooms across the submarket. "
        f"Luxury and upper upscale hold {values[0] / total * 100:.0f}% of inventory. "
        f"Source: CoStar, {row['report_date']}."
    )
    return _mini_layout(fig, height=height), caption


# ---------------------------------------------------------------------------
# 6. Visitor Profile & Spend: spend by category
# ---------------------------------------------------------------------------

@st.cache_data(ttl=1800, show_spinner=False)
def _spend_categories(_conn, top_n: int = 5) -> pd.DataFrame:
    df = _q(
        _conn,
        "SELECT category, spend_share_pct FROM datafy_overview_spending_by_category "
        "WHERE report_period_start = (SELECT MAX(report_period_start) "
        "                             FROM datafy_overview_spending_by_category) "
        "ORDER BY spend_share_pct DESC",
    )
    if df.empty:
        return df
    df["share_pct"] = df["spend_share_pct"] * 100
    head = df.head(top_n).copy()
    rest = df.iloc[top_n:]["share_pct"].sum()
    if rest > 0.5:
        head = pd.concat(
            [head, pd.DataFrame([{"category": "All other", "share_pct": rest}])],
            ignore_index=True,
        )
    return head[["category", "share_pct"]]


def _fig_spend_categories(conn):
    df = _spend_categories(conn)
    if df.empty:
        return None
    fig = go.Figure(go.Bar(
        x=df["share_pct"], y=df["category"], orientation="h",
        marker=dict(color=CATEGORY_COLORS[: len(df)]),
        text=[f"{v:.1f}%" for v in df["share_pct"]],
        textposition="outside", textfont=dict(size=9.5),
        hovertemplate="%{y}: %{x:.1f}% of visitor spend<extra></extra>",
        cliponaxis=False,
    ))
    fig.update_yaxes(autorange="reversed", tickfont=dict(size=8.8), automargin=True,
                     ticks="outside", ticklen=5, tickcolor="rgba(0,0,0,0)")
    fig.update_xaxes(showticklabels=False, range=[0, df["share_pct"].max() * 1.3])
    top = df.iloc[0]
    caption = (
        f"{top['category']} takes {top['share_pct']:.1f}% of every visitor dollar. "
        "Source: Datafy, latest published period."
    )
    return _mini_layout(fig, height=200), caption


# ---------------------------------------------------------------------------
# Market Performance (main-page section): CoStar submarket KPIs and a
# multi-year RevPAR trend, alongside the tier RevPAR and room-split figures
# already built above (reused here with a larger height, not duplicated).
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600, show_spinner=False)
def costar_overall_kpis(_conn) -> dict | None:
    """Latest year-to-date Overall-scope row for the Newport Beach/Dana Point
    submarket, the same row the Market Segments card's tier chart is built
    around. Returns None rather than a partial dict so callers can show a
    single "not available" message instead of guessing at missing keys."""
    df = _q(
        _conn,
        "SELECT occupancy_pct, occ_yoy_pct, adr_usd, adr_yoy_pct, revpar_usd, "
        "       revpar_yoy_pct, report_date "
        "FROM costar_annual_performance "
        "WHERE market LIKE '%Dana Point%' AND report_scope = 'Overall' AND year_label = 'YTD' "
        "ORDER BY report_date DESC LIMIT 1",
    )
    return None if df.empty else df.iloc[0].to_dict()


@st.cache_data(ttl=3600, show_spinner=False)
def costar_overall_trend_df(_conn) -> pd.DataFrame:
    """Overall-scope RevPAR by year for the submarket, real closed-book years
    only. CoStar's own multi-year tables also carry a numeric row for the
    current, still-in-progress year (e.g. "2026") that is a pace projection,
    not an actual, and a separate forecast PDF adds further-out numeric years
    on top of that (see CLAUDE.md Lessons Learned, 2026-08-14 CoStar forecast
    entry). This keeps only real completed years plus the current YTD actual,
    all drawn from the same report_date so a stale forecast pull never mixes
    with the fresh historical one."""
    hist = _q(
        _conn,
        "SELECT year_label, revpar_usd, report_date FROM costar_annual_performance "
        "WHERE market LIKE '%Dana Point%' AND report_scope = 'Overall' "
        "  AND year_label GLOB '[0-9][0-9][0-9][0-9]' "
        "  AND report_date = (SELECT MAX(report_date) FROM costar_annual_performance "
        "                     WHERE market LIKE '%Dana Point%' AND report_scope = 'Overall' "
        "                       AND year_label = 'YTD')",
    )
    if hist.empty:
        return hist
    current_year = pd.Timestamp.now().year
    hist = hist[hist["year_label"].astype(int) < current_year].copy()
    ytd = _q(
        _conn,
        "SELECT year_label, revpar_usd, report_date FROM costar_annual_performance "
        "WHERE market LIKE '%Dana Point%' AND report_scope = 'Overall' AND year_label = 'YTD' "
        "ORDER BY report_date DESC LIMIT 1",
    )
    if not ytd.empty:
        ytd = ytd.copy()
        ytd["year_label"] = f"YTD {current_year}"
        hist = pd.concat([hist, ytd[["year_label", "revpar_usd"]]], ignore_index=True)
    return hist.sort_values("year_label").reset_index(drop=True)


def fig_costar_overall_trend(conn, height: int = 260):
    df = costar_overall_trend_df(conn)
    if df.empty or len(df) < 2:
        return None
    fig = go.Figure(go.Scatter(
        x=df["year_label"], y=df["revpar_usd"], mode="lines+markers+text",
        line=dict(color=TEAL, width=3), marker=dict(size=8, color=TEAL_DK),
        text=[f"${v:,.0f}" for v in df["revpar_usd"]], textposition="top center",
        textfont=dict(size=10.5),
        hovertemplate="%{x}: $%{y:,.0f} RevPAR<extra></extra>",
    ))
    fig.update_yaxes(showticklabels=False, range=[0, df["revpar_usd"].max() * 1.22])
    fig.update_xaxes(tickfont=dict(size=10.5))
    caption = (
        "Overall-scope RevPAR by year, Newport Beach/Dana Point submarket. Only closed "
        "calendar years plus the current year-to-date actual are shown. Source: CoStar."
    )
    return _mini_layout(fig, height=height), caption


# ---------------------------------------------------------------------------
# Forward Outlook & Group Business (main-page section): built entirely from
# real, live sources -- STR group-segment mix (fact_str_group_metrics),
# STR compression history (kpi_compression_quarterly), the seeded VDP events
# calendar, and pipeline-generated insights_daily rows. Deliberately does NOT
# surface group_intelligence's dollar projections: that table is derived from
# costar_chain_scale_breakdown / costar_competitive_set, which
# load_costar_reports.py's own docstring documents as hardcoded baseline
# data, not a real parsed CoStar export (see CLAUDE.md Lessons Learned). The
# Data Hierarchy rule is never present unvetted data as current fact, so
# those estimates stay out of a client-facing section until group_intelligence
# is rebuilt on a real source.
# ---------------------------------------------------------------------------

@st.cache_data(ttl=1800, show_spinner=False)
def upcoming_events_df(_conn, limit: int = 5) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT event_name, event_date, is_major FROM vdp_events "
        "WHERE event_date >= date('now') ORDER BY event_date LIMIT ?",
        (limit,),
    )


@st.cache_data(ttl=1800, show_spinner=False)
def top_forward_insight(_conn) -> dict | None:
    """The single highest-priority forward-looking insight from today's
    pipeline run, preferring the dmo/cross audiences and the categories most
    relevant to a "what's ahead" section. Falls back to the single
    highest-priority dmo/cross insight of any category if none of the
    preferred ones ran today, so the callout is never empty just because the
    day's mix of insights shifted."""
    preferred = (
        "event_roi", "group_displacement_risk", "compression_outlook",
        "group_revenue_opportunity", "group_channel_roi", "demand_trend",
    )
    df = _q(
        _conn,
        "SELECT audience, category, headline, body, priority FROM insights_daily "
        "WHERE as_of_date = (SELECT MAX(as_of_date) FROM insights_daily) "
        "  AND audience IN ('dmo', 'cross') "
        "ORDER BY priority DESC",
    )
    if df.empty:
        return None
    ranked = df[df["category"].isin(preferred)]
    row = (ranked.iloc[0] if not ranked.empty else df.iloc[0])
    return row.to_dict()


# ---------------------------------------------------------------------------
# 7. Forward Outlook & Group Business: demand mix by segment
# ---------------------------------------------------------------------------

@st.cache_data(ttl=1800, show_spinner=False)
def _group_mix(_conn) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT segment, metric_value AS occ_pct, as_of_date FROM fact_str_group_metrics "
        "WHERE market = 'Dana Point' AND metric_name = 'occ_pct' "
        "  AND data_period = 'current' AND segment <> 'Total' "
        "  AND metric_value IS NOT NULL "
        "  AND as_of_date = (SELECT MAX(as_of_date) FROM fact_str_group_metrics "
        "                    WHERE market = 'Dana Point' AND data_period = 'current') "
        "ORDER BY metric_value DESC",
    )


_SEGMENT_NAMES = {"Trans.": "Transient", "Grp.": "Group", "Cont.": "Contract"}


def _fig_group_mix(conn, height: int = 160):
    df = _group_mix(conn)
    if df.empty or df["occ_pct"].sum() <= 0:
        return None
    df = df[df["occ_pct"] > 0].copy()
    df["label"] = df["segment"].map(lambda s: _SEGMENT_NAMES.get(s, s))
    total = df["occ_pct"].sum()
    fig = go.Figure(go.Bar(
        x=df["occ_pct"], y=["Occupancy mix"] * len(df), orientation="h",
        marker=dict(color=[TEAL_DK, AMBER, TEAL_LT][: len(df)],
                    line=dict(color="#FFFFFF", width=1)),
        text=[f"{r.label}<br>{r.occ_pct:.1f} pts" for r in df.itertuples()],
        textposition="inside", insidetextanchor="middle",
        textfont=dict(size=9.5, color="#FFFFFF"),
        hovertemplate="%{text}<extra></extra>",
    ))
    fig.update_layout(barmode="stack")
    fig.update_xaxes(showticklabels=False)
    fig.update_yaxes(showticklabels=False)
    grp = df[df["segment"] == "Grp."]
    grp_share = (grp["occ_pct"].iloc[0] / total * 100) if not grp.empty else None
    caption = (
        f"Dana Point ran {total:.1f}% total occupancy in the latest STR week ending "
        f"{df['as_of_date'].iloc[0]}"
        + (f", with group business contributing {grp_share:.0f}% of it." if grp_share else ".")
    )
    return _mini_layout(fig, height=height), caption


# ---------------------------------------------------------------------------
# Registry: section title -> builder. Titles must match SECTIONS in app.py.
# Executive Summary and Notes & Commentary are intentionally absent: the
# first already leads the page with its own live KPI tiles and trend chart,
# and the second is text by definition.
# ---------------------------------------------------------------------------

SECTION_FIGURE_BUILDERS = {
    "Hotel Performance — Occupancy": _fig_occupancy_dow,
    "Hotel Performance — ADR & Compression": _fig_compression,
    "Visitor Origins": _fig_feeder_markets,
    "Market Segments": _fig_costar_tiers,
    "Chain-Scale Segment Detail": _fig_room_split,
    "Visitor Profile & Spend": _fig_spend_categories,
    "Forward Outlook & Group Business": _fig_group_mix,
}


def build_section_visual(title: str, conn):
    """Returns (figure, caption) for a section card, or None when that
    section has no live chart of its own or its source table is empty. A
    None result is not an error: the caller falls back to the PDF-page
    thumbnail, so a missing table degrades to the previous behavior rather
    than an empty card."""
    builder = SECTION_FIGURE_BUILDERS.get(title)
    if builder is None:
        return None
    try:
        return builder(conn)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Upgraded feeder-market map
# ---------------------------------------------------------------------------

def _map_trace_classes():
    """plotly 5.24 introduced the MapLibre-backed `map` traces (go.Scattermap)
    and began deprecating the Mapbox ones. Both render a real tiled basemap
    with no access token on the styles used here, so prefer the newer class
    when the installed plotly has it and fall back rather than pinning the
    app to one plotly generation."""
    if hasattr(go, "Scattermap"):
        return go.Scattermap, "map"
    return go.Scattermapbox, "mapbox"


def build_feeder_market_map(
    markets_df: pd.DataFrame,
    dma_coords: dict,
    height: int = 420,
) -> go.Figure | None:
    """A tiled-basemap feeder-market map: each origin market is a bubble
    graduated by its share of visitor spend, joined to Dana Point by a
    connector line whose weight carries the same value, so the picture reads
    as flow into the destination rather than as unrelated dots on a page.

    Replaces the previous flat `Scattergeo` USA outline, which showed state
    borders and nothing else: no place names, no coastline detail, and no
    sense of the distance a fly market actually represents.

    Returns None when no current market has a known coordinate, so callers
    keep their bar-chart fallback.
    """
    if markets_df is None or markets_df.empty:
        return None

    mapped = markets_df.assign(
        lat=markets_df["dma"].map(lambda d: dma_coords.get(d, (None, None))[0]),
        lon=markets_df["dma"].map(lambda d: dma_coords.get(d, (None, None))[1]),
    ).dropna(subset=["lat", "lon"])
    if mapped.empty:
        return None

    mapped = mapped.sort_values("share_pct", ascending=False).reset_index(drop=True)
    max_share = float(mapped["share_pct"].max()) or 1.0
    dp_lat, dp_lon = 33.467, -117.698

    ScatterCls, kind = _map_trace_classes()
    fig = go.Figure()

    # Connector lines first so bubbles sit on top of them. Drawn as one trace
    # per market rather than a single trace with None separators, because the
    # per-market line width is what encodes share.
    for row in mapped.itertuples():
        weight = float(row.share_pct) / max_share
        fig.add_trace(ScatterCls(
            lon=[row.lon, dp_lon], lat=[row.lat, dp_lat],
            mode="lines",
            line=dict(width=0.8 + weight * 3.2, color="rgba(29,110,134,0.35)"),
            hoverinfo="skip", showlegend=False,
        ))

    fig.add_trace(ScatterCls(
        lon=mapped["lon"], lat=mapped["lat"],
        mode="markers",
        marker=dict(
            size=(mapped["share_pct"] / max_share) * 20 + 9,
            color=mapped["share_pct"],
            colorscale=[[0.0, TEAL_LT], [0.5, TEAL], [1.0, TEAL_DK]],
            opacity=0.72,
        ),
        customdata=mapped[["dma", "share_pct"]].values,
        hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]:.1f}% share<extra></extra>",
        showlegend=False,
    ))

    # Dana Point is drawn last, in two stacked markers: a white disc under an
    # amber one. Los Angeles is by far the largest bubble and sits almost on
    # top of Dana Point geographically, so without the white ring the
    # destination marker disappears into it. The label is amber for the same
    # reason: dark ink on a dark teal bubble is unreadable.
    fig.add_trace(ScatterCls(
        lon=[dp_lon], lat=[dp_lat], mode="markers",
        marker=dict(size=22, color="#FFFFFF"),
        hoverinfo="skip", showlegend=False,
    ))
    fig.add_trace(ScatterCls(
        lon=[dp_lon], lat=[dp_lat], mode="markers+text",
        marker=dict(size=14, color=AMBER),
        text=["Dana Point"], textposition="bottom right",
        textfont=dict(size=12.5, color=AMBER),
        hovertemplate="<b>Dana Point</b><extra></extra>",
        showlegend=False,
    ))

    map_settings = dict(
        style="carto-positron",
        center=dict(lat=39.0, lon=-98.0),
        zoom=2.6,
    )
    layout = {
        kind: map_settings,
        "height": height,
        "margin": dict(l=0, r=0, t=0, b=0),
        "paper_bgcolor": "rgba(0,0,0,0)",
        "font": dict(color=INK, family=FONT),
        "showlegend": False,
        "hoverlabel": dict(bgcolor="#FFFFFF", font_size=12, font_family=FONT),
    }
    fig.update_layout(**layout)
    return fig


# ---------------------------------------------------------------------------
# Section picker for the flipbook and its matching export -- builds a new
# PDF containing only the pages the viewer chose, in report order, always
# with the cover page kept.
# ---------------------------------------------------------------------------

def build_subset_pdf(pdf_bytes: bytes, page_indices: list[int]) -> bytes:
    """Returns a new single PDF containing only page_indices, in the given
    order, using PyMuPDF (already a dependency for the flipbook rasterizer).
    doc.select() reorders/subsets in place, so pass indices already sorted
    the way they should appear in the exported file."""
    import fitz  # PyMuPDF

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        doc.select(page_indices)
        return doc.tobytes()
    finally:
        doc.close()

