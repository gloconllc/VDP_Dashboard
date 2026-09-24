"""
Dana Point PULSE — Multi-Page Dashboard

12 focused pages, one topic per page.
Each page: Headline insight + 4 hero metrics + 2 charts + 3 recommendations + navigation.
Designed to be scannable in 2 minutes. No clutter.

Every number on every page below is computed from the same SQLite tables and
loaders that back the Classic View (see app.py's `load_*` functions). Where a
metric has no backing table yet (e.g. a total-dollar destination-spend figure,
or a day-by-day length-of-stay distribution), the page says so explicitly
instead of inventing a number.

Data Hierarchy note (see CLAUDE.md): STR/CoStar/Datafy are current-truth
sources and are used for anything presented as "now." Zartico is historical
reference only (Jun 2025 snapshot) and is always labeled as such below, never
presented as current performance.

Architecture:
  Tier 1: Hotel Operations (4 pages)
  Tier 2: Visitor Economy (4 pages)
  Tier 3: Strategic Planning (4 pages)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import timedelta

from chart_theme import style_fig, CATEGORICAL
from utils import format_metric_card, format_insight_card

# Same thresholds used on the Classic View (app.py OCC_HIGH_THRESHOLD / OCC_MED_THRESHOLD),
# expressed on the 0-100 scale that kpi_daily_summary.occ_pct uses.
OCC_HIGH_THRESHOLD = 90.0
OCC_MED_THRESHOLD = 80.0

_TRANSITION = dict(duration=400, easing="cubic-in-out")


# ═══════════════════════════════════════════════════════════════════════════
# ICON LIBRARY — minimal Feather-style line icons (viewBox 0 0 24 24). These
# replace every emoji/dingbat used as a pseudo-icon anywhere in this file or
# in app.py's page navigation. Do not add emoji back; add a new SVG entry here
# instead, following the same stroke/line-cap conventions.
# ═══════════════════════════════════════════════════════════════════════════

ICONS = {
    "overview": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/></svg>',
    "hotel_operations": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 21V9a1 1 0 0 1 .4-.8l7-5.25a1 1 0 0 1 1.2 0l7 5.25a1 1 0 0 1 .4.8V21"/><path d="M2 21h20"/><path d="M9 21v-4a3 3 0 0 1 6 0v4"/><path d="M9 9h1M14 9h1M9 13h1M14 13h1"/></svg>',
    "visitor_economy": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M3 12h18"/><path d="M12 3c2.5 2.5 4 5.7 4 9s-1.5 6.5-4 9c-2.5-2.5-4-5.7-4-9s1.5-6.5 4-9z"/></svg>',
    "strategic_planning": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1"/></svg>',
    "occupancy": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 18v-7a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v7"/><path d="M3 18h18"/><path d="M7 9V6a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v3"/></svg>',
    "rate_strategy": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41 12 22l-9-9V4a1 1 0 0 1 1-1h9z"/><circle cx="7.5" cy="7.5" r="1.5"/></svg>',
    "revenue": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M7 15l4-4 3 3 5-6"/></svg>',
    "compression": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="17" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/><circle cx="12" cy="15" r="1.6"/></svg>',
    "visitor_markets": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s7-6.3 7-12a7 7 0 1 0-14 0c0 5.7 7 12 7 12z"/><circle cx="12" cy="10" r="2.5"/></svg>',
    "spend": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="5" width="20" height="14" rx="2"/><path d="M2 10h20"/></svg>',
    "stay": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M20 14.5A8.5 8.5 0 1 1 9.5 4a7 7 0 0 0 10.5 10.5z"/></svg>',
    "group": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="8" r="3.2"/><path d="M3 20c0-3.3 2.7-6 6-6s6 2.7 6 6"/><circle cx="17" cy="9" r="2.6"/><path d="M15.5 14.2c2.4.4 4.5 2.4 4.5 5.8"/></svg>',
    "events": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="17" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/><path d="M12 13l1 2 2 .3-1.5 1.4.3 2.1-1.8-1-1.8 1 .3-2.1L9 15.3l2-.3z"/></svg>',
    "intelligence": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 17l6-6 4 4 8-8"/><path d="M15 6h6v6"/></svg>',
    "stakeholder": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="8" r="3.2"/><path d="M3 20c0-3.3 2.7-6 6-6s6 2.7 6 6"/><path d="M16.5 11.5l1.6 1.6 3-3.2"/></svg>',
    "brain_status": '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h4l2 7 4-14 2 7h6"/></svg>',
}

# Which of the 12 topic pages belongs to which of the 4 top-level nav items,
# in display order. "overview" is a standalone 5th item with no sub-pages.
PAGE_CATEGORIES = {
    "hotel_operations": ["occupancy", "rate_strategy", "revenue", "compression"],
    "visitor_economy": ["visitor_markets", "spend", "stay", "group"],
    "strategic_planning": ["events", "intelligence", "stakeholder", "brain_status"],
}
PAGE_TO_CATEGORY = {p: cat for cat, plist in PAGE_CATEGORIES.items() for p in plist}
CATEGORY_LABELS = {
    "hotel_operations": "Hotel Operations",
    "visitor_economy": "Visitor Economy",
    "strategic_planning": "Strategic Planning",
}
PAGE_LABELS = {
    "occupancy": "Occupancy Outlook",
    "rate_strategy": "Rate Strategy",
    "revenue": "Revenue Generation",
    "compression": "Compression Calendar",
    "visitor_markets": "Visitor Markets",
    "spend": "Spend Pathways",
    "stay": "Stay Patterns",
    "group": "Group Demand",
    "events": "Events Impact",
    "intelligence": "Market Intelligence",
    "stakeholder": "Stakeholder Brief",
    "brain_status": "Brain Status",
}
ALL_PAGE_KEYS = ["overview"] + list(PAGE_TO_CATEGORY.keys())

# Set by render_page() just before it dispatches to a page function, so _hero()
# can build a breadcrumb without every one of the 13 page functions needing to
# pass its own category/label through. Defaults to "overview" so a direct call
# to a page function in a test/smoke-test context never raises.
_CURRENT_PAGE = "overview"


def _icon(key: str, size: int = 22, color: str = "") -> str:
    """Wrap an ICONS entry in a sized inline-flex span so it renders cleanly
    at KPI/insight-card/nav scale (18-24px) regardless of where it's dropped."""
    svg = ICONS.get(key, "")
    style = f"width:{size}px;height:{size}px;display:inline-flex;flex-shrink:0;"
    if color:
        style += f"color:{color};"
    return f'<span style="{style}">{svg}</span>'


# ═══════════════════════════════════════════════════════════════════════════
# TOP-LEVEL NAVIGATION — persistent 4-item bar (Overview / Hotel Operations /
# Visitor Economy / Strategic Planning) rendered above every page, plus a
# second-level pill row of the active category's 4 sub-pages. Both use plain
# <a href="?page=..."> links (query-param routing, read once at the top of
# app.py's `if not use_classic_view:` block) rather than st.button, because a
# button can't sit outside the sidebar column carrying inline SVG easily.
# ═══════════════════════════════════════════════════════════════════════════

def render_masthead(last_str_update: str = "", period_note: str = "") -> None:
    """Persistent dark top bar rendered once, above render_top_nav(), on every
    page in both the primary nav and Classic View. Carries the wordmark, two
    real (never decorative) status pills — LIVE and the STR as-of date, both
    computed by the caller from the actual data — and a Board Report export
    action that triggers the browser's print dialog via the existing
    @media print rules already used by the print/export flow elsewhere in
    app.py. `period_note`, if given, surfaces the *real* current value of
    render_period_selector() on pages where that control applies, rather than
    introducing a second, competing period control that doesn't drive anything."""
    as_of_html = (
        f'<span class="pulse-pill">STR THROUGH {last_str_update}</span>' if last_str_update else ""
    )
    period_html = f'<span class="pulse-pill">{period_note}</span>' if period_note else ""
    st.markdown(
        f'<div class="pulse-topbar">'
        f'<a href="?page=overview" target="_self" class="pulse-topbar-brand">'
        f'<span class="pulse-topbar-mark">{_icon("overview", 16, "#FFFFFF")}</span>'
        f'<span class="pulse-topbar-word">Dana Point <span>PULSE</span></span>'
        f'</a>'
        f'<div class="pulse-topbar-mid">'
        f'<span class="pulse-pill live"><span class="dot"></span>LIVE</span>'
        f'{as_of_html}{period_html}'
        f'</div>'
        f'<a class="pulse-topbar-cta" href="javascript:window.print()" title="Export this view to PDF">'
        f'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" '
        f'stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 6 2 18 2 18 9"/>'
        f'<path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"/>'
        f'<rect x="6" y="14" width="12" height="8"/></svg> Board Report</a>'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_top_nav(active_page: str) -> None:
    active_cat = "overview" if active_page == "overview" else PAGE_TO_CATEGORY.get(active_page, "hotel_operations")
    nav_items = [
        ("overview", "overview", "Overview", "overview"),
        ("hotel_operations", "occupancy", "Hotel Operations", "hotel_operations"),
        ("visitor_economy", "visitor_markets", "Visitor Economy", "visitor_economy"),
        ("strategic_planning", "events", "Strategic Planning", "strategic_planning"),
    ]
    links = []
    for cat_key, target_page, label, icon_key in nav_items:
        is_active = active_cat == cat_key
        bg = "var(--dp-teal-dim)" if is_active else "transparent"
        fg = "#0E7490" if is_active else "var(--dp-text-2)"
        weight = "700" if is_active else "600"
        border = "1px solid var(--dp-teal)" if is_active else "1px solid transparent"
        links.append(
            f'<a href="?page={target_page}" target="_self" style="display:inline-flex;align-items:center;'
            f'gap:8px;padding:9px 18px;border-radius:var(--dp-radius);text-decoration:none;'
            f'background:{bg};color:{fg} !important;font-weight:{weight};border:{border};'
            f'font-family:\'DM Sans\',\'Inter\',sans-serif;font-size:14px;">'
            f'{_icon(icon_key, 18)}{label}</a>'
        )
    st.markdown(
        '<div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;padding:10px 4px 14px 4px;'
        'border-bottom:1px solid var(--dp-border);margin-bottom:14px;">' + "".join(links) + '</div>',
        unsafe_allow_html=True,
    )


def render_sub_nav(active_page: str) -> None:
    if active_page == "overview" or active_page not in PAGE_TO_CATEGORY:
        return
    cat = PAGE_TO_CATEGORY[active_page]
    links = []
    for p in PAGE_CATEGORIES[cat]:
        is_active = p == active_page
        bg = "var(--dp-card-hover)" if is_active else "var(--dp-card)"
        fg = "#0E7490" if is_active else "var(--dp-text-2)"
        weight = "700" if is_active else "600"
        border = "1px solid var(--dp-teal)" if is_active else "1px solid var(--dp-border)"
        links.append(
            f'<a href="?page={p}" target="_self" style="display:inline-flex;align-items:center;'
            f'gap:6px;padding:6px 14px;border-radius:999px;text-decoration:none;'
            f'background:{bg};color:{fg} !important;font-weight:{weight};border:{border};font-size:12.5px;">'
            f'{_icon(p, 14)}{PAGE_LABELS[p]}</a>'
        )
    st.markdown(
        '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:18px;">' + "".join(links) + '</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# SHARED RENDER HELPERS (reuse app.py's existing CSS classes; no new CSS)
# ═══════════════════════════════════════════════════════════════════════════

def _breadcrumb_html() -> str:
    """Home > Category > Page, built from _CURRENT_PAGE (set by render_page()
    right before it dispatches). Overview has no category, so it renders as
    a single current-page crumb rather than a dangling separator."""
    if _CURRENT_PAGE == "overview" or _CURRENT_PAGE not in PAGE_TO_CATEGORY:
        return '<div class="pulse-breadcrumb"><span class="current">Overview</span></div>'
    cat = PAGE_TO_CATEGORY[_CURRENT_PAGE]
    cat_label = CATEGORY_LABELS.get(cat, cat)
    page_label = PAGE_LABELS.get(_CURRENT_PAGE, _CURRENT_PAGE)
    return (
        '<div class="pulse-breadcrumb">Home<span class="sep">/</span>'
        f'{cat_label}<span class="sep">/</span>'
        f'<span class="current">{page_label}</span></div>'
    )


def _hero(title_html: str, subtitle: str) -> None:
    """Reuses the .hero-banner / .hero-title / .hero-subtitle classes defined
    in app.py's style block (the same simple pattern used for sub-tab headers,
    e.g. the Visitor Intelligence banner), plus a breadcrumb row above the
    title built from whichever page render_page() last dispatched to."""
    st.markdown(
        f'<div class="hero-banner">'
        f'{_breadcrumb_html()}'
        f'<div class="hero-title">{title_html}</div>'
        f'<div class="hero-subtitle">{subtitle}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _framing(sentence: str) -> None:
    """The one to two sentences of real prose under each page's hero, framing
    what the page answers. Rendered at a comfortably readable size instead of
    default Streamlit body text, via the shared .insight-body typography rule."""
    st.markdown(f'<p class="insight-body" style="padding-left:0;margin:6px 0 16px 0;">{sentence}</p>',
                unsafe_allow_html=True)


def _kpis(cards: list) -> None:
    """cards: list of (label, value, delta_text_or_empty, icon_key_or_html, as_of)
    or (label, value, delta_text_or_empty, icon_key_or_html, as_of, href) if the
    card should drill down to a `#anchor-id` further down the same page.
    Renders via utils.format_metric_card. Every card must carry a real as_of
    date/period reference for the specific metric it shows (STR as-of date,
    Datafy report period, trailing window, etc.) — never a blank or generic one."""
    cols = st.columns(len(cards))
    for col, card in zip(cols, cards):
        label, value, delta, icon, as_of, href = (list(card) + [""] * 6)[:6]
        icon_html = _icon(icon) if icon in ICONS else icon
        with col:
            st.markdown(
                format_metric_card(label, str(value), icon=icon_html, context=delta or "", as_of=as_of or "",
                                    href=href or ""),
                unsafe_allow_html=True,
            )


def _anchor(anchor_id: str) -> None:
    """Drop an invisible drill-down target so a kpi-card href="#id" from higher
    up the same page scrolls here. Streamlit strips raw <a name> in some
    renders, so this uses a real id'd div, which every modern browser still
    honors for in-page hash navigation."""
    st.markdown(f'<div id="{anchor_id}" style="position:relative; top:-70px;"></div>', unsafe_allow_html=True)


def _headline(icon: str, title: str, main_value: str, subtitle: str, body: str,
              accent: str = "#0891B2", as_of: str = "") -> None:
    """Renders via utils.format_insight_card, the shared headline-insight card."""
    icon_html = _icon(icon, 26) if icon in ICONS else icon
    st.markdown(
        format_insight_card(icon_html, title, main_value, subtitle=subtitle, body=body,
                             accent_color=accent, as_of=as_of),
        unsafe_allow_html=True,
    )


def _render_chart(fig: go.Figure) -> None:
    fig.update_layout(transition=_TRANSITION)
    st.plotly_chart(style_fig(fig), use_container_width=True)


def _fmt_money(v, decimals: int = 0) -> str:
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "N/A"
    if pd.isna(v):
        return "N/A"
    if abs(v) >= 1e6:
        return f"${v / 1e6:.1f}M"
    if abs(v) >= 1e3:
        return f"${v / 1e3:.0f}K"
    return f"${v:,.{decimals}f}"


def _fmt_pct(v, decimals: int = 1) -> str:
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "N/A"
    if pd.isna(v):
        return "N/A"
    return f"{v:.{decimals}f}%"


def _delta_pct(v, decimals: int = 1, suffix: str = " YoY") -> str:
    """Percent-change delta with arrow, or '' if not available."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    if pd.isna(v):
        return ""
    arrow = "▲" if v >= 0 else "▼"
    return f"{arrow} {v:+.{decimals}f}%{suffix}"


def _delta_pp(v, decimals: int = 1, suffix: str = " YoY") -> str:
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    if pd.isna(v):
        return ""
    arrow = "▲" if v >= 0 else "▼"
    return f"{arrow} {v:+.{decimals}f}pp{suffix}"


def _nav_row(buttons: list) -> None:
    """buttons: list of (label, page_key, unique_key)."""
    st.divider()
    cols = st.columns(len(buttons))
    for col, (label, page_key, key) in zip(cols, buttons):
        with col:
            if st.button(f"→ {label}", use_container_width=True, key=key):
                st.session_state.page = page_key
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# MAIN-PAGE PERIOD FILTER — per owner feedback, a date/period control belongs
# on the main content area, not only in the sidebar. Rendered at the top of
# every hotel-operations-facing page (occupancy, rate, revenue) and the
# Overview page, right below the top nav / sub nav and before the page's own
# hero. Selection persists in st.session_state across page navigation and
# actually re-slices the STR-derived trend charts on those pages, it is not
# decorative.
# ═══════════════════════════════════════════════════════════════════════════

PERIOD_OPTIONS = ["Last 30 Days", "Last 90 Days", "Trailing 12 Months", "Year to Date"]
# Only pages whose trend charts the selector actually re-slices. Compression
# Calendar is quarterly-only and Overview has no windowed STR trend chart, so
# neither is included, per the "no decorative control that does nothing" rule.
_PERIOD_PAGES = {"occupancy", "rate_strategy", "revenue"}


def render_period_selector() -> str:
    """Compact selector, persisted in st.session_state['pulse_period']. Only
    called for pages whose numbers it actually changes (see _PERIOD_PAGES)."""
    if "pulse_period" not in st.session_state:
        st.session_state["pulse_period"] = "Last 90 Days"
    c1, c2 = st.columns([1, 3])
    with c1:
        selected = st.selectbox(
            "Reporting period",
            PERIOD_OPTIONS,
            index=PERIOD_OPTIONS.index(st.session_state["pulse_period"]),
            key="pulse_period_select",
        )
    st.session_state["pulse_period"] = selected
    return selected


def _lodging_mix(df_compset: pd.DataFrame) -> dict:
    """Real, current Dana Point group-vs-transient lodging mix, computed from
    fact_str_group_metrics (via load_str_compset, the same table the Market
    Intelligence page's 6-market comp set already uses). This is the STR
    segment breakdown, not the group_intelligence benchmark estimate, so it
    is Dana Point's own actual mix, not an industry benchmark. Returns an
    empty dict if the segment rows are not present for the latest date."""
    out = {}
    if df_compset.empty:
        return out
    dp = df_compset[(df_compset["market"] == "Dana Point") & (df_compset["data_period"] == "current")]
    if dp.empty:
        return out
    latest_date = dp["as_of_date"].max()
    dp = dp[dp["as_of_date"] == latest_date]
    total_demand = dp[(dp["segment"] == "Total") & (dp["metric_name"] == "demand")]["metric_value"]
    trans_demand = dp[(dp["segment"] == "Trans.") & (dp["metric_name"] == "demand")]["metric_value"]
    grp_demand = dp[(dp["segment"] == "Grp.") & (dp["metric_name"] == "demand")]["metric_value"]
    cont_demand = dp[dp["segment"].isin(["Cont.", "Con."]) & (dp["metric_name"] == "demand")]["metric_value"]
    trans_adr = dp[(dp["segment"] == "Trans.") & (dp["metric_name"] == "adr")]["metric_value"]
    grp_adr = dp[(dp["segment"] == "Grp.") & (dp["metric_name"] == "adr")]["metric_value"]
    total = float(total_demand.iloc[0]) if not total_demand.empty else np.nan
    if pd.isna(total) or total <= 0:
        return out
    out["as_of"] = str(latest_date)
    out["group_pct"] = float(grp_demand.iloc[0]) / total * 100 if not grp_demand.empty else np.nan
    out["transient_pct"] = float(trans_demand.iloc[0]) / total * 100 if not trans_demand.empty else np.nan
    out["contract_pct"] = float(cont_demand.iloc[0]) / total * 100 if not cont_demand.empty else 0.0
    out["group_adr"] = float(grp_adr.iloc[0]) if not grp_adr.empty else np.nan
    out["transient_adr"] = float(trans_adr.iloc[0]) if not trans_adr.empty else np.nan
    return out


def _period_window(df: pd.DataFrame, date_col: str, period_label: str) -> pd.DataFrame:
    """Slice df to the selected reporting period, anchored to the latest date
    actually present in df (not the calendar date, per CLAUDE.md's freshness
    lesson: anchoring to today's calendar date on a lagged feed returns an
    empty window instead of the intended trend)."""
    if df.empty or date_col not in df.columns:
        return df
    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col])
    latest = d[date_col].max()
    if pd.isna(latest):
        return df
    if period_label == "Last 30 Days":
        cutoff = latest - timedelta(days=30)
    elif period_label == "Trailing 12 Months":
        cutoff = latest - timedelta(days=365)
    elif period_label == "Year to Date":
        cutoff = pd.Timestamp(year=latest.year, month=1, day=1)
    else:  # "Last 90 Days" default
        cutoff = latest - timedelta(days=90)
    return d[d[date_col] > cutoff].sort_values(date_col)


# ═══════════════════════════════════════════════════════════════════════════
# TIER 1: HOTEL OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def _render_lodging_mix_block(df_compset: pd.DataFrame, df_travel_types: pd.DataFrame) -> None:
    """Group vs. transient vs. leisure lodging mix, rendered upfront per owner
    feedback ("they always want to know about group and transient and
    leisure lodging statistics", not just blended occupancy). Two layers,
    clearly separated: (1) Dana Point's own current STR segment mix, real
    demand-share data from fact_str_group_metrics, and (2) US Travel
    Association national traveler-type context (leisure/business/family
    typical length of stay), explicitly labeled as national benchmark
    context, not Dana Point-specific, per CLAUDE.md's Layer 2 context rule."""
    mix = _lodging_mix(df_compset)
    if not mix:
        st.info("No current-week STR group/transient segment data loaded yet (fact_str_group_metrics).")
        return
    st.markdown('<p class="insight-body" style="padding-left:0;font-weight:700;">Lodging Mix: Group, Transient &amp; Leisure</p>',
                unsafe_allow_html=True)
    as_of_wk = pd.to_datetime(mix["as_of"]).strftime("%b %d, %Y")
    cols = st.columns(4)
    with cols[0]:
        st.markdown(format_metric_card("Transient Demand Share", f"{mix['transient_pct']:.0f}%",
                                        icon=_icon("visitor_markets"), context="of room-nights",
                                        as_of=f"STR weekly, week of {as_of_wk}"), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(format_metric_card("Group Demand Share", f"{mix['group_pct']:.0f}%",
                                        icon=_icon("group"), context="of room-nights",
                                        as_of=f"STR weekly, week of {as_of_wk}"), unsafe_allow_html=True)
    with cols[2]:
        adr_gap = mix["transient_adr"] - mix["group_adr"] if pd.notna(mix.get("transient_adr")) and pd.notna(mix.get("group_adr")) else np.nan
        st.markdown(format_metric_card("Transient vs. Group ADR", f"${adr_gap:+,.0f}" if pd.notna(adr_gap) else "N/A",
                                        icon=_icon("rate_strategy"), context="transient premium over group",
                                        as_of=f"STR weekly, week of {as_of_wk}"), unsafe_allow_html=True)
    with cols[3]:
        leisure_note, leisure_los = "N/A", "N/A"
        if not df_travel_types.empty and "traveler_type" in df_travel_types.columns:
            leisure_row = df_travel_types[df_travel_types["traveler_type"] == "leisure"]
            if leisure_row.empty:
                leisure_row = df_travel_types[df_travel_types["traveler_type"] == "family"]
            if not leisure_row.empty:
                lo = leisure_row.iloc[0].get("typical_los_nights_low")
                hi = leisure_row.iloc[0].get("typical_los_nights_high")
                if pd.notna(lo) and pd.notna(hi):
                    leisure_los = f"{lo:.0f}-{hi:.0f} nights"
        st.markdown(format_metric_card("Leisure Typical Stay", leisure_los,
                                        icon=_icon("stay"), context="US Travel Assn., national benchmark",
                                        as_of="us_travel_traveler_types, 2024"), unsafe_allow_html=True)
    st.caption(
        "Transient and group shares are Dana Point's own current STR segment mix. Leisure typical "
        "stay is US Travel Association national context, not a Dana Point-specific figure, shown "
        "here because leadership consistently asks for it alongside occupancy."
    )
    st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)


def page_occupancy_outlook(df_kpi: pd.DataFrame, df_comp: pd.DataFrame, df_str: pd.DataFrame,
                            period_label: str = "Last 90 Days", df_compset: pd.DataFrame = None,
                            df_travel_types: pd.DataFrame = None) -> None:
    """Page 1: Occupancy Outlook. Source: kpi_daily_summary (load_kpi_daily),
    kpi_compression_quarterly (load_compression), fact_str_metrics daily
    (load_str_daily, for the daily trend + day-of-week breakdown). The daily
    trend chart's window is driven by the main-page period selector
    (render_period_selector), not hardcoded, per owner feedback that the date
    filter needs to live on the main page, not only the sidebar."""
    _hero("Occupancy <span>Outlook</span>",
          "Hotel Operations &nbsp;·&nbsp; STR Daily &nbsp;·&nbsp; Compression Tracking")
    _framing(
        "This page answers two questions leadership asks most often: how strong is demand right "
        "now, and when do the peak periods land. The numbers below come straight from STR daily "
        "exports, the same source of truth used everywhere else in this dashboard."
    )

    if df_kpi.empty:
        st.warning("No STR daily KPI data loaded yet. Run the pipeline to populate kpi_daily_summary.")
        return

    df_compset = df_compset if df_compset is not None else pd.DataFrame()
    df_travel_types = df_travel_types if df_travel_types is not None else pd.DataFrame()

    df_kpi = df_kpi.sort_values("as_of_date")
    latest = df_kpi.iloc[-1]
    occ_now = float(latest.get("occ_pct", 0) or 0)
    occ_yoy = latest.get("occ_yoy", np.nan)
    as_of = pd.to_datetime(latest["as_of_date"]).strftime("%b %d, %Y")

    q3_days, q3_90, q3_label = None, None, ""
    if not df_comp.empty:
        q3_rows = df_comp[df_comp["quarter"].astype(str).str.endswith("Q3")].sort_values("quarter", ascending=False)
        if not q3_rows.empty:
            q3_days = int(q3_rows.iloc[0]["days_above_80_occ"])
            q3_90 = int(q3_rows.iloc[0]["days_above_90_occ"])
            q3_label = str(q3_rows.iloc[0]["quarter"])

    last30 = df_kpi.tail(30)["occ_pct"].mean()
    prev30 = df_kpi.iloc[-60:-30]["occ_pct"].mean() if len(df_kpi) >= 60 else np.nan
    if pd.notna(last30) and pd.notna(prev30):
        trend_delta = last30 - prev30
        trend_word = "Strengthening" if trend_delta > 1 else ("Softening" if trend_delta < -1 else "Stable")
        trend_sub = f"{trend_delta:+.1f}pp vs. prior 30d"
    else:
        trend_word, trend_sub = "N/A", "insufficient history"

    if occ_now >= OCC_HIGH_THRESHOLD:
        demand_label = "Peak"
    elif occ_now >= OCC_MED_THRESHOLD:
        demand_label = "Strong"
    else:
        demand_label = "Moderate"

    yoy_str = _delta_pct(occ_yoy) or "flat vs. last year"
    comp_sentence = (
        f"The most recent {q3_label} logged {q3_days} days above 80% occupancy "
        f"({q3_90} of those above 90%). "
        if q3_days is not None else ""
    )
    _headline(
        "occupancy", "Occupancy Outlook", f"{occ_now:.1f}%",
        f"as of {as_of} &nbsp;·&nbsp; {yoy_str}",
        f"Current occupancy is {occ_now:.1f}% ({yoy_str}). {comp_sentence}"
        f"30-day momentum is {trend_word.lower()} ({trend_sub}).",
    )

    _kpis([
        ("Occupancy", f"{occ_now:.1f}%", yoy_str, "occupancy", f"STR daily, as of {as_of}"),
        (f"Compression ({q3_label or 'Q3'})", f"{q3_days if q3_days is not None else 'N/A'} days", "80%+ occupancy", "compression", f"{q3_label or 'Most recent Q3'}, STR daily", "?page=compression"),
        ("Demand Level", demand_label, "current read", "strategic_planning", f"STR daily, as of {as_of}"),
        ("30-Day Momentum", trend_word, trend_sub, "revenue", "Trailing 30 vs. prior 30 days", "?page=revenue"),
    ])

    st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
    _render_lodging_mix_block(df_compset, df_travel_types)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader(f"Daily Occupancy, {period_label}")
        recent = _period_window(df_str, "as_of_date", period_label) if not df_str.empty else pd.DataFrame()
        if not recent.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=recent["as_of_date"], y=recent["occupancy"], mode="lines", name="Occ %",
                line=dict(color=CATEGORICAL[0], width=2), fill="tozeroy",
            ))
            fig.add_hline(y=80, line_dash="dash", line_color="#D97706", annotation_text="80%")
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0),
                               xaxis_title="Date", yaxis_title="Occ %")
            _render_chart(fig)
        else:
            st.info("No STR daily rows available for a 90-day trend yet.")

    with c2:
        st.subheader("Occupancy by Day of Week")
        if not df_str.empty:
            dow = df_str.copy()
            dow["dow"] = pd.to_datetime(dow["as_of_date"]).dt.day_name()
            order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            dow_avg = dow.groupby("dow")["occupancy"].mean().reindex(order)
            fig = go.Figure([go.Bar(
                x=[d[:3] for d in order], y=dow_avg.values,
                marker_color=[CATEGORICAL[0] if pd.notna(v) and v < 80 else CATEGORICAL[1] for v in dow_avg.values],
            )])
            fig.add_hline(y=80, line_dash="dash", line_color="#DC2626")
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), showlegend=False,
                               xaxis_title="Day", yaxis_title="Avg Occ %")
            _render_chart(fig)
        else:
            st.info("No STR daily rows available for a day-of-week breakdown yet.")

    weekend_avg = weekday_avg = None
    if not df_str.empty:
        dow2 = df_str.copy()
        dow2["dow"] = pd.to_datetime(dow2["as_of_date"]).dt.dayofweek
        weekend_avg = dow2[dow2["dow"].isin([4, 5, 6])]["occupancy"].mean()
        weekday_avg = dow2[~dow2["dow"].isin([4, 5, 6])]["occupancy"].mean()
    weekend_note = (
        f"weekend avg {weekend_avg:.0f}% vs. weekday {weekday_avg:.0f}%"
        if pd.notna(weekend_avg) and pd.notna(weekday_avg) else "insufficient day-of-week history"
    )

    st.markdown(f"""
    **Next Best Actions**
    1. **Lock rates ahead of high-occupancy dates** ({weekend_note}). Enforce minimum-stay rules and dynamic tiers 6–8 weeks out.
    2. **Watch the {q3_label or "next"} compression window** ({q3_days if q3_days is not None else "N/A"} days above 80% occ). This is when rate discipline pays off most.
    3. **Use the 30-day momentum read** ({trend_word.lower()}, {trend_sub}) to decide whether to hold or ease rate floors into the next 30 days.
    """)

    _nav_row([
        ("Rate Strategy", "rate_strategy", "occ_to_rate"),
        ("Compression Calendar", "compression", "occ_to_comp"),
        ("Revenue Generation", "revenue", "occ_to_rev"),
    ])


def page_rate_strategy(df_kpi: pd.DataFrame, df_str: pd.DataFrame,
                        df_costar: pd.DataFrame, df_compset: pd.DataFrame,
                        period_label: str = "Last 90 Days") -> None:
    """Page 2: Rate Strategy. Source: kpi_daily_summary (ADR/RevPAR + YoY),
    fact_str_metrics daily (weekday/weekend ADR premium), costar_competitive_set
    (load_costar_compset, RGI per property vs. the South OC comp set),
    fact_str_group_metrics (load_str_compset, the 6-market Dana Point comp set:
    Dana Point, Newport Beach, La Jolla, Santa Barbara, Monterey-Carmel,
    Huntington Beach — NOT Anaheim, per CLAUDE.md)."""
    _hero("Rate <span>Strategy</span>",
          "Hotel Operations &nbsp;·&nbsp; ADR &amp; RGI &nbsp;·&nbsp; CoStar Competitive Set")
    _framing(
        "Rate decisions are easier to defend when they are anchored to a comp set, not a gut feel. "
        "This page checks ADR against the CoStar competitive set and against Dana Point's own "
        "weekday-to-weekend pattern before answering whether current pricing has room to move."
    )

    if df_kpi.empty:
        st.warning("No STR daily KPI data loaded yet.")
        return

    df_kpi = df_kpi.sort_values("as_of_date")
    latest = df_kpi.iloc[-1]
    adr_now = float(latest.get("adr", 0) or 0)
    adr_yoy = latest.get("adr_yoy", np.nan)
    rvp_now = float(latest.get("revpar", 0) or 0)
    rvp_yoy = latest.get("revpar_yoy", np.nan)
    as_of = pd.to_datetime(latest["as_of_date"]).strftime("%b %d, %Y")

    rgi_avg = np.nan
    if not df_costar.empty and "rgi" in df_costar.columns:
        props = df_costar[~df_costar["property_name"].astype(str).str.contains("Blended", na=False)]
        if not props.empty:
            rgi_avg = props["rgi"].mean()

    weekend_adr = weekday_adr = np.nan
    if not df_str.empty:
        s = df_str.copy()
        s["dow"] = pd.to_datetime(s["as_of_date"]).dt.dayofweek
        weekend_adr = s[s["dow"].isin([4, 5, 6])]["adr"].mean()
        weekday_adr = s[~s["dow"].isin([4, 5, 6])]["adr"].mean()
    premium = (weekend_adr - weekday_adr) if pd.notna(weekend_adr) and pd.notna(weekday_adr) else np.nan

    adr_yoy_str = _delta_pct(adr_yoy) or "flat vs. last year"
    rgi_sentence = (
        f" VDP portfolio properties average {rgi_avg:.0f} RGI against the South Orange County comp set (100 = at par)."
        if pd.notna(rgi_avg) else ""
    )
    premium_sentence = f" Weekend ADR runs ${premium:+.0f}/night over weekday." if pd.notna(premium) else ""
    _headline(
        "rate_strategy", "Rate Strategy", f"${adr_now:,.0f}",
        f"as of {as_of} &nbsp;·&nbsp; {adr_yoy_str}",
        f"ADR is ${adr_now:,.0f} ({adr_yoy_str}).{rgi_sentence}{premium_sentence}",
    )

    _kpis([
        ("ADR", f"${adr_now:,.0f}", adr_yoy_str, "rate_strategy", f"STR daily, as of {as_of}"),
        ("RGI (vs. Comp Set)", f"{rgi_avg:.0f}" if pd.notna(rgi_avg) else "N/A", "CoStar, 100 = par", "intelligence", "CoStar competitive set, latest report"),
        ("Weekend Premium", f"${premium:+.0f}/night" if pd.notna(premium) else "N/A", "vs. weekday ADR", "compression", "STR daily, full history on file"),
        ("RevPAR", f"${rvp_now:,.0f}", _delta_pct(rvp_yoy) or "flat vs. last year", "revenue", f"STR daily, as of {as_of}"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader(f"ADR Trend, {period_label}")
        recent = _period_window(df_kpi, "as_of_date", period_label)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=recent["as_of_date"], y=recent["adr"], name="Dana Point ADR",
                                  line=dict(color=CATEGORICAL[0], width=2)))
        if not df_compset.empty:
            comp_rows = df_compset[
                (df_compset["metric_name"] == "adr") &
                (df_compset["data_period"] == "current") &
                (df_compset["market"] != "Dana Point")
            ]
            if not comp_rows.empty:
                comp_adr = comp_rows.groupby("market")["metric_value"].mean().mean()
                if pd.notna(comp_adr):
                    fig.add_hline(y=comp_adr, line_dash="dash", line_color=CATEGORICAL[1],
                                  annotation_text=f"5-market comp avg ${comp_adr:,.0f}")
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), xaxis_title="Date", yaxis_title="ADR ($)")
        _render_chart(fig)

    with c2:
        st.subheader("RGI by Property, CoStar Competitive Set")
        if not df_costar.empty and "rgi" in df_costar.columns:
            plot_df = df_costar.sort_values("rgi", ascending=False)
            fig = go.Figure([go.Bar(
                x=plot_df["property_name"], y=plot_df["rgi"],
                marker_color=[CATEGORICAL[1] if "Blended" in str(n) else CATEGORICAL[0]
                              for n in plot_df["property_name"]],
            )])
            fig.add_hline(y=100, line_dash="dash", line_color="#64748B", annotation_text="Index 100")
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), showlegend=False, yaxis_title="RGI")
            _render_chart(fig)
        else:
            st.info("No CoStar competitive set data loaded yet (costar_competitive_set).")

    premium_action = f"currently ${premium:+.0f}/night" if pd.notna(premium) else "not yet measurable from STR daily data"
    rgi_action = f"portfolio RGI avg {rgi_avg:.0f}" if pd.notna(rgi_avg) else "RGI not yet loaded"
    st.markdown(f"""
    **Next Best Actions**
    1. **Hold or extend the weekend rate premium** ({premium_action}) on Fri–Sun dates.
    2. **Benchmark against the comp set monthly** ({rgi_action}) to catch rate-floor slippage early.
    3. **Move rates with the ADR trend, not against it**. Current YoY change is {adr_yoy_str}.
    """)

    _nav_row([
        ("Occupancy Outlook", "occupancy", "rate_to_occ"),
        ("Revenue Generation", "revenue", "rate_to_rev"),
        ("Market Intelligence", "intelligence", "rate_to_intel"),
    ])


def page_revenue_generation(df_kpi: pd.DataFrame, df_str: pd.DataFrame,
                             period_label: str = "Last 90 Days") -> None:
    """Page 3: Revenue Generation. Source: kpi_daily_summary (RevPAR + YoY),
    fact_str_metrics daily revenue column (trailing-12-month room revenue for
    the TBID 1.25% / TOT 10% projections, per CLAUDE.md's TBID formula), and
    the Sept 26–Oct 4, 2025 Ohana Fest window in kpi_daily_summary compared to
    the all-period average (a real, computed lift, not the CLAUDE.md verified
    benchmark constants — those live on the Events Impact page)."""
    _hero("Revenue <span>Generation</span>",
          "Hotel Operations &nbsp;·&nbsp; RevPAR &nbsp;·&nbsp; TBID / TOT Projection")
    _framing(
        "RevPAR tells us how much room revenue Dana Point hotels are actually capturing, and the "
        "same trailing 12-month figure, run through the TBID's blended 1.25 percent assessment rate "
        "and the city's 10 percent TOT rate, tells the City and the TBID board what that revenue is "
        "worth in assessment dollars."
    )

    if df_kpi.empty:
        st.warning("No STR daily KPI data loaded yet.")
        return

    df_kpi = df_kpi.sort_values("as_of_date")
    latest = df_kpi.iloc[-1]
    rvp_now = float(latest.get("revpar", 0) or 0)
    rvp_yoy = latest.get("revpar_yoy", np.nan)
    as_of = pd.to_datetime(latest["as_of_date"]).strftime("%b %d, %Y")

    trailing_rev = np.nan
    if not df_str.empty:
        s = df_str.copy()
        s["as_of_date"] = pd.to_datetime(s["as_of_date"])
        cutoff = s["as_of_date"].max() - timedelta(days=365)
        trailing_rev = s[s["as_of_date"] > cutoff]["revenue"].sum()
    tbid_est = trailing_rev * 0.0125 if pd.notna(trailing_rev) else np.nan
    tot_est = trailing_rev * 0.10 if pd.notna(trailing_rev) else np.nan

    ev_mask = (df_kpi["as_of_date"] >= "2025-09-26") & (df_kpi["as_of_date"] <= "2025-10-04")
    ev_rows = df_kpi[ev_mask]
    ev_adr = ev_rows["adr"].mean() if not ev_rows.empty else np.nan
    ev_rvp = ev_rows["revpar"].mean() if not ev_rows.empty else np.nan
    base_adr = df_kpi["adr"].mean()
    base_rvp = df_kpi["revpar"].mean()
    adr_lift = (ev_adr - base_adr) if pd.notna(ev_adr) else np.nan
    rvp_lift_pct = ((ev_rvp - base_rvp) / base_rvp * 100) if pd.notna(ev_rvp) and base_rvp else np.nan

    rvp_yoy_str = _delta_pct(rvp_yoy) or "flat vs. last year"
    tbid_sentence = (
        f" Trailing 12-month room revenue of {_fmt_money(trailing_rev)} implies roughly "
        f"{_fmt_money(tbid_est)} in TBID (1.25%) and {_fmt_money(tot_est)} in TOT (10%)."
        if pd.notna(trailing_rev) else ""
    )
    event_sentence = (
        f" During the Sept 26–Oct 4, 2025 Ohana Fest window, ADR ran ${ev_adr:,.0f} vs. a "
        f"${base_adr:,.0f} all-period average, a ${adr_lift:+,.0f} lift."
        if pd.notna(ev_adr) else ""
    )
    _headline(
        "revenue", "Revenue Generation", f"${rvp_now:,.0f}",
        f"RevPAR as of {as_of} &nbsp;·&nbsp; {rvp_yoy_str}",
        f"RevPAR is ${rvp_now:,.0f} ({rvp_yoy_str}).{tbid_sentence}{event_sentence}",
    )

    _kpis([
        ("RevPAR", f"${rvp_now:,.0f}", rvp_yoy_str, "revenue", f"STR daily, as of {as_of}"),
        ("Event-Window RevPAR", f"${ev_rvp:,.0f}" if pd.notna(ev_rvp) else "N/A",
         f"{rvp_lift_pct:+.0f}% vs. all-period avg" if pd.notna(rvp_lift_pct) else "Sept 26–Oct 4, 2025", "events", "Ohana Fest 2025 window, Sept 26–Oct 4"),
        ("TBID Potential (TTM)", _fmt_money(tbid_est), "1.25% blended rate", "revenue", "Trailing 12 months of STR daily revenue"),
        ("TOT Potential (TTM)", _fmt_money(tot_est), "10% rate", "revenue", "Trailing 12 months of STR daily revenue"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader(f"RevPAR Trend, {period_label}")
        recent = _period_window(df_kpi, "as_of_date", period_label)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=recent["as_of_date"], y=recent["revpar"], name="RevPAR",
                                  line=dict(color=CATEGORICAL[3], width=2)))
        if pd.notna(base_rvp):
            fig.add_hline(y=base_rvp, line_dash="dash", line_color=CATEGORICAL[5],
                          annotation_text=f"All-period avg ${base_rvp:,.0f}")
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), xaxis_title="Date", yaxis_title="RevPAR ($)")
        _render_chart(fig)

    with c2:
        st.subheader("Trailing 12-Month Revenue Breakdown")
        if pd.notna(trailing_rev):
            categories = ["Room Revenue (TTM)", "TBID Est. (1.25%)", "TOT Est. (10%)"]
            values = [trailing_rev, tbid_est, tot_est]
            fig = go.Figure([go.Bar(x=categories, y=values,
                                     marker_color=[CATEGORICAL[0], CATEGORICAL[3], CATEGORICAL[1]])])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="$ (TTM)", yaxis_type="log")
            _render_chart(fig)
        else:
            st.info("Not enough STR daily history yet for a trailing 12-month revenue figure.")

    lift_action = f"+${adr_lift:,.0f} vs. baseline in the Sept 26–Oct 4 window" if pd.notna(adr_lift) else "not yet measurable, event window outside loaded STR history"
    tbid_action = _fmt_money(tbid_est) if pd.notna(tbid_est) else "not yet computable"
    st.markdown(f"""
    **Next Best Actions**
    1. **Defend the event-window ADR lift** ({lift_action}). Hold rate floors through comparable compression windows.
    2. **Brief the TBID board on the {tbid_action} annual TBID opportunity** derived from trailing 12-month room revenue.
    3. **Reconcile RevPAR momentum** ({rvp_yoy_str}) against the comp set on the Rate Strategy page before adjusting rate floors.
    """)

    _nav_row([
        ("Occupancy Outlook", "occupancy", "rev_to_occ"),
        ("Rate Strategy", "rate_strategy", "rev_to_rate"),
        ("Events Impact", "events", "rev_to_events"),
    ])


def page_compression_calendar(df_comp: pd.DataFrame, df_str: pd.DataFrame) -> None:
    """Page 4: Compression Calendar. Source: kpi_compression_quarterly
    (load_compression) for the quarter-over-quarter bar chart, fact_str_metrics
    daily (load_str_daily) to find the actual highest-occupancy stretch within
    the most recent Q3 on record, rather than a fixed illustrative date range."""
    _hero("Compression <span>Calendar</span>",
          "Hotel Operations &nbsp;·&nbsp; Peak Planning &nbsp;·&nbsp; STR Quarterly Compression")
    _framing(
        "Compression days, those above 80 percent occupancy, are where rate discipline either pays "
        "off or gets given away. This page maps them by quarter and then zooms into the sharpest "
        "stretch on record so staffing and rate decisions can be planned around it, not reacted to."
    )

    if df_comp.empty:
        st.warning("No compression data loaded yet (kpi_compression_quarterly).")
        return

    df_comp_sorted = df_comp.sort_values("quarter")
    q3_rows = df_comp[df_comp["quarter"].astype(str).str.endswith("Q3")].sort_values("quarter", ascending=False)
    q3_days = int(q3_rows.iloc[0]["days_above_80_occ"]) if not q3_rows.empty else None
    q3_90 = int(q3_rows.iloc[0]["days_above_90_occ"]) if not q3_rows.empty else None
    q3_label = str(q3_rows.iloc[0]["quarter"]) if not q3_rows.empty else ""

    peak_start = peak_end = None
    peak_days = 0
    hot = pd.DataFrame()
    q3_window = pd.DataFrame()
    if not df_str.empty and q3_label:
        try:
            year = int(q3_label[:4])
        except ValueError:
            year = None
        if year:
            sd = df_str.copy()
            sd["as_of_date"] = pd.to_datetime(sd["as_of_date"])
            q3_window = sd[(sd["as_of_date"].dt.year == year) & (sd["as_of_date"].dt.month.isin([7, 8, 9]))]
            hot = q3_window[q3_window["occupancy"] >= 90].sort_values("as_of_date")
            if not hot.empty:
                peak_start = hot["as_of_date"].min().strftime("%b %d")
                peak_end = hot["as_of_date"].max().strftime("%b %d, %Y")
                peak_days = len(hot)

    peak_window_label = f"{peak_start}–{peak_end}" if peak_start else "N/A"
    q3_days_str = str(q3_days) if q3_days is not None else "N/A"
    ninety_sentence = f" and {q3_90} days above 90%." if q3_90 is not None else "."
    peak_sentence = (
        f" Within that window, {peak_window_label} saw {peak_days} days at 90%+ occupancy, the sharpest compression stretch."
        if peak_start else ""
    )
    _headline(
        "compression", "Compression Calendar", f"{q3_days_str} days",
        f"{q3_label or 'Q3'} above 80% occupancy",
        f"The most recent {q3_label or 'Q3'} on record logged {q3_days_str} days above 80% occupancy{ninety_sentence}{peak_sentence}",
    )

    _kpis([
        (f"{q3_label or 'Q3'} Compression", f"{q3_days_str} days", "80%+ occupancy", "compression", f"{q3_label or 'Most recent Q3'}, STR daily"),
        ("Peak Window", peak_window_label, f"{peak_days} days at 90%+" if peak_days else "no 90%+ run found", "compression", f"{q3_label or 'Most recent Q3'}, STR daily"),
        ("90%+ Days", f"{q3_90 if q3_90 is not None else 'N/A'}", q3_label, "compression", f"{q3_label or 'Most recent Q3'}, STR daily"),
        ("Quarters Tracked", f"{len(df_comp)}", "kpi_compression_quarterly", "compression", "Full quarterly history on file"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Compression Days by Quarter")
        fig = go.Figure([go.Bar(
            x=df_comp_sorted["quarter"], y=df_comp_sorted["days_above_80_occ"],
            marker_color=[CATEGORICAL[5] if d < 20 else CATEGORICAL[1] for d in df_comp_sorted["days_above_80_occ"]],
        )])
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Days >80% Occ")
        _render_chart(fig)

    with c2:
        chart_title = f"Daily Occupancy, Peak Window ({peak_window_label})" if peak_start else "Daily Occupancy, Most Recent Q3"
        st.subheader(chart_title)
        plot_window = hot if peak_start else q3_window
        if not plot_window.empty:
            pw = plot_window.sort_values("as_of_date")
            fig = go.Figure([go.Scatter(
                x=pw["as_of_date"].dt.strftime("%b %d"), y=pw["occupancy"], mode="markers+lines",
                marker=dict(size=10, color=pw["occupancy"], colorscale="RdYlGn", showscale=True,
                            colorbar=dict(title="Occ %")),
                line=dict(color=CATEGORICAL[0], width=2),
            )])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Occupancy %")
            _render_chart(fig)
        else:
            st.info("No STR daily rows in the most recent Q3 window yet.")

    st.markdown(f"""
    **Next Best Actions**
    1. **Lock rate strategy 6–8 weeks before {q3_label or 'peak season'}**. Communicate minimum-stay rules ahead of the {peak_window_label} window.
    2. **Coordinate a compression reserve with hotels** during the {q3_days_str}-day compression stretch to protect rate integrity.
    3. **Brief ops on staffing timing**. Plan the ramp-up before {peak_start or 'the next Q3 peak'} based on this quarter's history.
    """)

    _nav_row([
        ("Occupancy Outlook", "occupancy", "comp_to_occ"),
        ("Revenue Generation", "revenue", "comp_to_rev"),
        ("Events Impact", "events", "comp_to_events"),
    ])


# ═══════════════════════════════════════════════════════════════════════════
# TIER 2: VISITOR ECONOMY
# ═══════════════════════════════════════════════════════════════════════════

def page_visitor_markets(df_dfy: pd.DataFrame, df_dma: pd.DataFrame) -> None:
    """Page 5: Visitor Markets. Source: datafy_overview_kpis (load_datafy_overview)
    for total trips / OOS share / repeat rate, datafy_overview_dma
    (load_datafy_dma) for feeder-market share."""
    _hero("Visitor <span>Markets</span>",
          "Visitor Economy &nbsp;·&nbsp; Datafy Feeder Market DMA")
    _framing(
        "Every marketing dollar performs better when it is aimed at a feeder market that is already "
        "growing rather than one we hope will grow. This page ranks Dana Point's visitor-day share "
        "by DMA and tracks the in-state versus out-of-state split over the most recent Datafy period."
    )

    if df_dfy.empty:
        st.warning("No Datafy visitor overview data loaded yet (datafy_overview_kpis).")
        return

    latest = df_dfy.sort_values("report_period_start", ascending=False).iloc[0]
    period = f"{latest['report_period_start']} to {latest['report_period_end']}"
    total_trips = latest.get("total_trips")
    oos_pct = latest.get("out_of_state_vd_pct")
    oos_delta = latest.get("out_of_state_vd_vs_compare_pct")
    repeat_pct = latest.get("repeat_visitors_pct")

    top_dma_row = None
    if not df_dma.empty:
        dma_period = df_dma[df_dma["report_period_start"] == latest["report_period_start"]]
        dma_pool = dma_period if not dma_period.empty else df_dma
        top_dma_row = dma_pool.sort_values("visitor_days_share_pct", ascending=False).iloc[0]

    trips_str = f"{int(total_trips):,}" if pd.notna(total_trips) else "N/A"
    oos_str = _fmt_pct(oos_pct)
    oos_delta_str = f" ({oos_delta:+.1f}pp vs. compare period)" if pd.notna(oos_delta) else ""
    top_dma_str = top_dma_row["dma"] if top_dma_row is not None else "N/A"
    top_dma_share = f"{top_dma_row['visitor_days_share_pct']:.1f}%" if top_dma_row is not None else "N/A"
    repeat_str = _fmt_pct(repeat_pct)

    _headline(
        "visitor_markets", "Visitor Markets", trips_str,
        f"total trips, {period}",
        f"{top_dma_str} leads visitor-day share at {top_dma_share}. "
        f"Out-of-state visitor days are {oos_str}{oos_delta_str}. Repeat visitors are {repeat_str} of all trips.",
    )

    _kpis([
        ("Total Trips", trips_str, period, "visitor_markets", f"Datafy, {period}"),
        ("Out-of-State Visitor Days", oos_str, oos_delta_str.strip(" ()") or "vs. compare period", "visitor_markets", f"Datafy, {period}"),
        ("Top DMA", top_dma_str, f"{top_dma_share} visitor-day share", "visitor_markets", f"Datafy, {period}"),
        ("Repeat Visitors", repeat_str, "one-time vs. repeat", "visitor_economy", f"Datafy, {period}"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top Feeder Markets by Visitor-Day Share")
        if not df_dma.empty:
            dma_period = df_dma[df_dma["report_period_start"] == latest["report_period_start"]]
            dma_pool = dma_period if not dma_period.empty else df_dma
            dma_plot = dma_pool.sort_values("visitor_days_share_pct", ascending=True).tail(10)
            fig = go.Figure([go.Bar(y=dma_plot["dma"], x=dma_plot["visitor_days_share_pct"],
                                     orientation="h", marker_color=CATEGORICAL[0])])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), xaxis_title="% of Visitor Days")
            _render_chart(fig)
        else:
            st.info("No Datafy DMA data loaded yet (datafy_overview_dma).")

    with c2:
        st.subheader("In-State vs. Out-of-State Visitor Days")
        in_state = latest.get("in_state_visitor_days_pct")
        out_state = latest.get("out_of_state_vd_pct")
        in_delta = latest.get("in_state_vd_vs_compare_pct") or 0
        out_delta = latest.get("out_of_state_vd_vs_compare_pct") or 0
        if pd.notna(in_state) and pd.notna(out_state):
            prior_in = in_state - in_delta
            prior_out = out_state - out_delta
            periods = ["Compare Period", "Current Period"]
            fig = go.Figure()
            fig.add_trace(go.Bar(x=periods, y=[prior_in, in_state], name="In-State", marker_color=CATEGORICAL[0]))
            fig.add_trace(go.Bar(x=periods, y=[prior_out, out_state], name="Out-of-State", marker_color=CATEGORICAL[1]))
            fig.update_layout(barmode="stack", height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="% of Visitor Days")
            _render_chart(fig)
        else:
            st.info("In-state/out-of-state split not available for this report period.")

    st.markdown(f"""
    **Next Best Actions**
    1. **Prioritize {top_dma_str} in 2026 media planning**. It leads visitor-day share at {top_dma_share}.
    2. **Track the out-of-state shift** ({oos_delta_str.strip(' ()') or 'not available this period'}). A growing OOS base usually means higher-spend, longer-stay visitors.
    3. **Cross-check the {repeat_str} repeat-visitor rate** against loyalty and email programs to see where retention is working.
    """)

    _nav_row([
        ("Spend Pathways", "spend", "mkt_to_spend"),
        ("Stay Patterns", "stay", "mkt_to_stay"),
        ("Market Intelligence", "intelligence", "mkt_to_intel"),
    ])


def page_spend_pathways(df_dfy: pd.DataFrame, df_spend: pd.DataFrame) -> None:
    """Page 6: Spend Pathways. Source: datafy_overview_category_spending
    (load_datafy_spending) for category share, datafy_overview_kpis for the
    local-vs-visitor spend split. Note: the DB has spend *shares*, not a total
    dollar figure, so no total-destination-spend $ number is shown; the
    "growth by category" chart in the old version had no backing data at all
    (there is no per-category YoY column) and is replaced with the real
    spend-correlation column that datafy_overview_category_spending does carry."""
    _hero("Spend <span>Pathways</span>",
          "Visitor Economy &nbsp;·&nbsp; Datafy Category Spending")
    _framing(
        "Datafy tracks visitor spend as category shares rather than total dollars, so this page "
        "answers a narrower but still useful question: which category captures the largest slice of "
        "visitor spending, and how closely does each category's spend track overall visitor spend."
    )

    if df_spend.empty or df_dfy.empty:
        st.warning("No Datafy spending data loaded yet (datafy_overview_category_spending).")
        return

    latest_period = df_spend["report_period_start"].max()
    spend_latest = df_spend[df_spend["report_period_start"] == latest_period].sort_values(
        "spend_share_pct", ascending=False)
    top_cat = spend_latest.iloc[0] if not spend_latest.empty else None

    dfy_latest = df_dfy.sort_values("report_period_start", ascending=False).iloc[0]
    visitor_spend_pct = dfy_latest.get("visitor_spending_pct")
    local_spend_pct = dfy_latest.get("local_spending_pct")

    top_cat_name = top_cat["category"] if top_cat is not None else "N/A"
    top_cat_share = f"{top_cat['spend_share_pct']:.1f}%" if top_cat is not None else "N/A"

    _headline(
        "spend", "Spend Pathways", top_cat_share,
        f"{top_cat_name} share of visitor spend",
        f"{top_cat_name} captures the largest share of visitor spend at {top_cat_share}. "
        f"Visitors account for {_fmt_pct(visitor_spend_pct)} of all destination spend, versus "
        f"{_fmt_pct(local_spend_pct)} from locals. Datafy tracks category *shares*, not total dollars, "
        f"so no total-spend figure is shown here.",
    )

    _kpis([
        ("Top Category", top_cat_name, top_cat_share, "spend", f"Datafy, report period starting {latest_period}"),
        ("Visitor Share of Spend", _fmt_pct(visitor_spend_pct), "vs. local spend", "spend", f"Datafy, report period starting {latest_period}"),
        ("Local Share of Spend", _fmt_pct(local_spend_pct), "resident spend", "spend", f"Datafy, report period starting {latest_period}"),
        ("Categories Tracked", f"{len(spend_latest)}", "datafy_overview_category_spending", "spend", f"Datafy, report period starting {latest_period}"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader(f"Share of Total Spend by Category ({str(latest_period)[:4]})")
        fig = go.Figure([go.Pie(
            labels=spend_latest["category"], values=spend_latest["spend_share_pct"],
            marker=dict(colors=CATEGORICAL[: len(spend_latest)]),
        )])
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        _render_chart(fig)

    with c2:
        st.subheader("Spend Correlation with Total Visitor Spending")
        st.caption("How closely each category's spend tracks overall visitor spend, not YoY growth (no per-category growth table exists yet).")
        corr = spend_latest.sort_values("spending_correlation_pct", ascending=False)
        fig = go.Figure([go.Bar(
            x=corr["category"], y=corr["spending_correlation_pct"],
            marker_color=[CATEGORICAL[3] if v > 50 else CATEGORICAL[0] for v in corr["spending_correlation_pct"]],
        )])
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Correlation %")
        _render_chart(fig)

    st.markdown(f"""
    **Next Best Actions**
    1. **Build a partnership around {top_cat_name}** ({top_cat_share} of visitor spend). The largest, most defensible category to co-market.
    2. **Track the visitor-vs-local spend split** ({_fmt_pct(visitor_spend_pct)} visitor / {_fmt_pct(local_spend_pct)} local) each report cycle to see whether events are pulling in outside dollars.
    3. **Request a per-category YoY $ breakdown from Datafy**. Today's data supports share and correlation, not category-level growth trend.
    """)

    _nav_row([
        ("Visitor Markets", "visitor_markets", "spend_to_mkt"),
        ("Stay Patterns", "stay", "spend_to_stay"),
        ("Group Demand", "group", "spend_to_group"),
    ])


def page_stay_patterns(df_dfy: pd.DataFrame, df_los: pd.DataFrame = None) -> None:
    """Page 7: Stay Patterns. Source: datafy_overview_kpis (load_datafy_overview)
    avg_length_of_stay_days / day_trips_pct / overnight_trips_pct and their
    vs.-compare deltas, PLUS datafy_overview_los_distribution (load_datafy_los_distribution),
    the real 1-night-through-6+-night length-of-stay distribution. A prior
    pass claimed this distribution table did not exist in the schema; that
    was wrong, it is in analytics.sqlite and is wired in here."""
    _hero("Stay <span>Patterns</span>",
          "Visitor Economy &nbsp;·&nbsp; Datafy Length of Stay")
    _framing(
        "Length of stay is one of the simplest levers on room revenue, and it is also one of the "
        "easiest to lose track of. This page tracks the average nights per trip, the real "
        "night-by-night length-of-stay distribution, and the day-trip versus overnight split "
        "against the prior comparison period."
    )

    if df_dfy.empty:
        st.warning("No Datafy visitor overview data loaded yet (datafy_overview_kpis).")
        return

    df_los = df_los if df_los is not None else pd.DataFrame()

    latest = df_dfy.sort_values("report_period_start", ascending=False).iloc[0]
    avg_los = latest.get("avg_length_of_stay_days")
    los_delta = latest.get("avg_los_vs_compare_days")
    overnight_pct = latest.get("overnight_trips_pct")
    overnight_delta = latest.get("overnight_vs_compare_pct")
    day_trip_pct = latest.get("day_trips_pct")
    day_trip_delta = latest.get("day_trips_vs_compare_pct")
    period = f"{latest['report_period_start']} to {latest['report_period_end']}"

    los_str = f"{avg_los:.1f} nights" if pd.notna(avg_los) else "N/A"
    los_delta_str = f"{los_delta:+.1f}d vs. compare" if pd.notna(los_delta) else ""
    overnight_str = _fmt_pct(overnight_pct)
    overnight_delta_str = f"{overnight_delta:+.1f}pp vs. compare" if pd.notna(overnight_delta) else ""

    _headline(
        "stay", "Stay Patterns", los_str,
        f"average length of stay, {period}",
        f"Average stay is {los_str} ({los_delta_str or 'no compare-period delta available'}). "
        f"{overnight_str} of trips are overnight ({overnight_delta_str or 'flat vs. compare period'}), "
        f"with the remainder as day trips.",
    )

    _kpis([
        ("Avg. Length of Stay", los_str, los_delta_str, "stay", f"Datafy, {period}"),
        ("Overnight Trips", overnight_str, overnight_delta_str, "stay", f"Datafy, {period}"),
        ("Day Trips", _fmt_pct(day_trip_pct), f"{day_trip_delta:+.1f}pp vs. compare" if pd.notna(day_trip_delta) else "", "stay", f"Datafy, {period}"),
        ("Report Period", str(latest["report_period_start"])[:7], period, "compression", "Datafy annual report cycle"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Day Trip vs. Overnight Split")
        if pd.notna(day_trip_pct) and pd.notna(overnight_pct):
            fig = go.Figure([go.Pie(
                labels=["Day Trips", "Overnight"], values=[day_trip_pct, overnight_pct],
                marker=dict(colors=[CATEGORICAL[1], CATEGORICAL[0]]),
            )])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
            _render_chart(fig)
        else:
            st.info("Day trip / overnight split not available for this report period.")

    with c2:
        st.subheader("Length-of-Stay Distribution")
        if not df_los.empty:
            los_period = df_los["report_period_start"].max()
            los_latest = df_los[df_los["report_period_start"] == los_period].copy()
            # Sort buckets numerically ("1 Day", "2 Days", ... "6+ Days") rather
            # than alphabetically, so the chart reads left-to-right by length.
            los_latest["_sort_key"] = pd.to_numeric(
                los_latest["length_of_stay"].astype(str).str.extract(r"(\d+)")[0], errors="coerce"
            )
            los_latest = los_latest.sort_values("_sort_key")
            fig = go.Figure([go.Bar(
                x=los_latest["length_of_stay"], y=los_latest["pct_of_trips"],
                marker_color=CATEGORICAL[0],
            )])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="% of Trips")
            _render_chart(fig)
        else:
            st.info("No length-of-stay distribution loaded yet (datafy_overview_los_distribution).")

    one_night_pct = np.nan
    if not df_los.empty:
        los_period = df_los["report_period_start"].max()
        one_row = df_los[(df_los["report_period_start"] == los_period) &
                          (df_los["length_of_stay"].astype(str).str.contains("1", na=False))]
        if not one_row.empty:
            one_night_pct = float(one_row.iloc[0]["pct_of_trips"])
    one_night_action = (
        f"{one_night_pct:.0f}% of trips are single-night stays, the single largest bucket by far"
        if pd.notna(one_night_pct) else "the single-night share is not available this period"
    )

    st.markdown(f"""
    **Next Best Actions**
    1. **Target day-trip-to-overnight conversion**. {_fmt_pct(day_trip_pct)} of trips are same-day; a hotel-plus-dining bundle aimed at even a few points of that pool moves real room-nights.
    2. **Target the single-night bucket specifically**. {one_night_action}; a second-night incentive aimed at this bucket is the most direct lever on average length of stay.
    3. **Track the {los_str} average stay** each report cycle; a rising trend means packages are working, a falling one means demand is thinning toward single-night stays.
    """)

    _nav_row([
        ("Visitor Markets", "visitor_markets", "stay_to_mkt"),
        ("Spend Pathways", "spend", "stay_to_spend"),
        ("Group Demand", "group", "stay_to_group"),
    ])


def page_group_demand(df_group: pd.DataFrame, df_comp: pd.DataFrame) -> None:
    """Page 8: Group Demand. Source: group_intelligence (load_group_intelligence,
    the most recent benchmark row) joined with kpi_compression_quarterly
    (load_compression) to flag which quarters carry the most displacement risk
    for group bookings — a real cross-dataset read, not a fabricated quarter list."""
    _hero("Group <span>Demand</span>",
          "Visitor Economy &nbsp;·&nbsp; Group Intelligence Benchmark")
    _framing(
        "Group business fills rooms at a discount to market rate, which makes timing the whole "
        "strategy. This page benchmarks Dana Point's group mix against the South Orange County "
        "upper-upscale-coastal set and flags the quarter where group demand is most likely to "
        "displace higher-paying transient business."
    )

    if df_group.empty:
        st.warning("No group intelligence benchmark loaded yet (group_intelligence).")
        return

    g = df_group.iloc[0]
    group_pct = g.get("group_primary_pct")
    bench_low = g.get("benchmark_group_demand_share_low")
    bench_high = g.get("benchmark_group_demand_share_high")
    discount = g.get("benchmark_group_adr_discount_pct")
    market_adr = g.get("market_blended_adr")
    group_adr = g.get("estimated_group_adr")
    tbid_low = g.get("estimated_group_tbid_rev_low")
    tbid_high = g.get("estimated_group_tbid_rev_high")
    uplift_5pp = g.get("tbid_uplift_per_5pp_shift")
    comp_days_annual = g.get("compression_days_annual")
    benchmark_date = g.get("benchmark_date")

    group_pct_str = f"{group_pct * 100:.1f}%" if pd.notna(group_pct) else "N/A"
    bench_range_str = (
        f"{bench_low * 100:.0f}–{bench_high * 100:.0f}%" if pd.notna(bench_low) and pd.notna(bench_high) else "N/A"
    )
    discount_str = f"-{discount * 100:.0f}%" if pd.notna(discount) else "N/A"

    # Which quarters carry the most displacement risk (high compression = avoid group)
    risk_quarters = []
    if not df_comp.empty:
        by_q = df_comp.copy()
        by_q["q"] = by_q["quarter"].astype(str).str[-2:]
        avg_by_q = by_q.groupby("q")["days_above_80_occ"].mean().sort_values(ascending=False)
        risk_quarters = list(avg_by_q.index)

    high_risk_q = risk_quarters[0] if risk_quarters else "Q3"

    _headline(
        "group", "Group Demand", group_pct_str,
        f"of primary demand is group business, benchmarked {benchmark_date}",
        f"Group business runs {group_pct_str} of primary demand, inside the {bench_range_str} South OC "
        f"upper-upscale-coastal benchmark. Group rates average {discount_str} below market blended ADR "
        f"(${group_adr:,.0f} vs. ${market_adr:,.0f}). Historically, {high_risk_q} carries the most "
        f"compression days, the highest-risk quarter to displace transient demand with group.",
    )

    _kpis([
        ("Group Mix", group_pct_str, f"benchmark {bench_range_str}", "group", f"group_intelligence, benchmarked {benchmark_date}"),
        ("Group ADR Discount", discount_str, "vs. market blended", "rate_strategy", f"group_intelligence, benchmarked {benchmark_date}"),
        ("Highest-Risk Quarter", high_risk_q, "most compression days on record", "compression", "kpi_compression_quarterly, full history on file"),
        ("TBID Uplift / 5pp Shift", _fmt_money(uplift_5pp), "shoulder-season target", "revenue", f"group_intelligence, benchmarked {benchmark_date}"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Compression Days by Quarter, Averaged (Displacement Risk)")
        if not df_comp.empty:
            avg_by_q_full = df_comp.copy()
            avg_by_q_full["q"] = avg_by_q_full["quarter"].astype(str).str[-2:]
            avg_by_q_full = avg_by_q_full.groupby("q")["days_above_80_occ"].mean().reindex(["Q1", "Q2", "Q3", "Q4"])
            fig = go.Figure([go.Bar(
                x=avg_by_q_full.index, y=avg_by_q_full.values,
                marker_color=[CATEGORICAL[1] if v == avg_by_q_full.max() else CATEGORICAL[3] for v in avg_by_q_full.values],
            )])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Avg. Days >80% Occ")
            _render_chart(fig)
        else:
            st.info("No compression data loaded yet (kpi_compression_quarterly).")

    with c2:
        st.subheader("ADR: Group Rate vs. Market Blended")
        if pd.notna(group_adr) and pd.notna(market_adr):
            fig = go.Figure([go.Bar(
                x=["Group Rate", "Market Blended", "Gap"],
                y=[group_adr, market_adr, market_adr - group_adr],
                marker_color=[CATEGORICAL[1], CATEGORICAL[0], CATEGORICAL[4]],
            )])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="ADR ($)")
            _render_chart(fig)
        else:
            st.info("Group vs. market ADR not available in this benchmark row.")

    tbid_range_str = f"{_fmt_money(tbid_low)}–{_fmt_money(tbid_high)}"
    st.markdown(f"""
    **Next Best Actions**
    1. **Avoid group bookings on {high_risk_q} peak dates**. That quarter carries the market's highest compression, and group ADR (${group_adr:,.0f}) runs {discount_str} below market blended (${market_adr:,.0f}).
    2. **Target SMERF and corporate groups for the lower-compression quarters** to fill otherwise vacant shoulder-season rooms.
    3. **Track group displacement ROI**. Each 5pp shift toward group in shoulder seasons is worth roughly {_fmt_money(uplift_5pp)} in annual TBID, against a {tbid_range_str} full-year group TBID range.
    """)

    _nav_row([
        ("Stay Patterns", "stay", "group_to_stay"),
        ("Compression Calendar", "compression", "group_to_comp"),
        ("Revenue Generation", "revenue", "group_to_rev"),
    ])


# ═══════════════════════════════════════════════════════════════════════════
# TIER 3: STRATEGIC PLANNING
# ═══════════════════════════════════════════════════════════════════════════

def page_events_impact(df_kpi: pd.DataFrame, df_comp: pd.DataFrame, df_events: pd.DataFrame,
                        df_zfe: pd.DataFrame, df_zartico_impact: pd.DataFrame) -> None:
    """Page 9: Events Impact. Source: vdp_events (load_vdp_events) for the
    real event calendar and days-away math, kpi_compression_quarterly for
    Q3 compression, zartico_future_events_summary (load_zartico_future_events)
    for YoY event/attendee growth, explicitly labeled as Zartico historical
    reference (Jun 2025 snapshot) per CLAUDE.md's data hierarchy, never
    presented as current data. The Ohana Fest 2025 dollar figures ($14.6M
    event spend, $18.4M destination impact, +$139 ADR lift, 68% OOS, 3.2x
    multiplier) are CLAUDE.md's verified Datafy benchmark, the same constants
    the Classic View's Event Impact tab uses; they are not invented here."""
    _hero("Events <span>Impact</span>",
          "Strategic Planning &nbsp;·&nbsp; VDP Events Calendar &nbsp;·&nbsp; Ohana Fest Benchmark")
    _framing(
        "Ohana Fest 2025 is the clearest event-ROI benchmark Dana Point has on record, and this page "
        "uses it as the yardstick for every future signature event. It also carries Zartico's "
        "historical reference figures for the broader events calendar, labeled as such and never "
        "presented as current performance."
    )

    q3_days, q3_label = None, ""
    if not df_comp.empty:
        q3_rows = df_comp[df_comp["quarter"].astype(str).str.endswith("Q3")].sort_values("quarter", ascending=False)
        if not q3_rows.empty:
            q3_days = int(q3_rows.iloc[0]["days_above_80_occ"])
            q3_label = str(q3_rows.iloc[0]["quarter"])

    ev_growth = att_growth = np.nan
    if not df_zfe.empty:
        ev_growth = df_zfe.iloc[0].get("yoy_pct_change_events")
        att_growth = df_zfe.iloc[0].get("yoy_pct_change_attendees")

    _headline(
        "events", "Events Impact", "+$139",
        "Ohana Fest 2025 ADR lift ($542 vs. $403 baseline, verified Datafy benchmark)",
        f"Ohana Fest 2025 drove $14.6M in direct event expenditure and $18.4M in total destination "
        f"impact, a 3.2x spend multiplier, with 68% out-of-state attendance. Per Zartico's historical "
        f"reference snapshot (Jun 2025), Dana Point events grew {ev_growth:+.0f}% YoY in count and "
        f"{att_growth:+.0f}% YoY in attendees." if pd.notna(ev_growth) else
        "Ohana Fest 2025 drove $14.6M in direct event expenditure and $18.4M in total destination "
        "impact, a 3.2x spend multiplier, with 68% out-of-state attendance.",
    )

    _kpis([
        ("Event Expenditure (Ohana '25)", "$14.6M", "verified Datafy benchmark", "events", "Datafy, Ohana Fest 2025 (Sept 26–Oct 4, 2025)"),
        ("Destination Impact (Ohana '25)", "$18.4M", "3.2x spend multiplier", "revenue", "Datafy, Ohana Fest 2025 (Sept 26–Oct 4, 2025)"),
        ("Q3 Compression", f"{q3_days if q3_days is not None else 'N/A'} days", q3_label or "80%+ occupancy", "compression", f"{q3_label or 'Most recent Q3'}, STR daily"),
        ("YoY Event Growth", f"{ev_growth:+.0f}%" if pd.notna(ev_growth) else "N/A",
         "Zartico historical reference, Jun 2025", "intelligence", "Zartico, historical reference snapshot, Jun 2025"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Historical Reference: Event-Window Spend Composition (Zartico)")
        st.caption(
            "Zartico's event-window snapshot, historical reference only per CLAUDE.md's data "
            "hierarchy. This tracks the OC Marathon window (May 4-10, 2025), not Ohana Fest, and "
            "is shown for spend-composition context, not as current performance."
        )
        if not df_zartico_impact.empty:
            zr = df_zartico_impact.iloc[0]
            cats = ["Accommodation", "Food & Bev", "Gas", "Retail", "Arts"]
            vals = [
                zr.get("pct_accommodation_spend"), zr.get("pct_food_bev_spend"),
                zr.get("pct_gas_spend"), zr.get("pct_retail_spend"), zr.get("pct_arts_spend"),
            ]
            pairs = [(c, v) for c, v in zip(cats, vals) if pd.notna(v)]
            if pairs:
                fig = go.Figure([go.Pie(
                    labels=[p[0] for p in pairs], values=[p[1] for p in pairs],
                    marker=dict(colors=CATEGORICAL[: len(pairs)]),
                )])
                fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
                _render_chart(fig)
            else:
                st.info("No spend-composition percentages available in zartico_event_impact.")
        else:
            st.info("No Zartico event-impact data loaded yet (zartico_event_impact).")

    with c2:
        st.subheader("Event ADR & RevPAR vs. Baseline")
        if not df_kpi.empty:
            k = df_kpi.copy()
            ev_mask = (k["as_of_date"] >= "2025-09-26") & (k["as_of_date"] <= "2025-10-04")
            ev_rows = k[ev_mask]
            if not ev_rows.empty:
                ev_adr = ev_rows["adr"].mean()
                ev_rvp = ev_rows["revpar"].mean()
                base_adr = k["adr"].mean()
                base_rvp = k["revpar"].mean()
                fig = go.Figure()
                fig.add_trace(go.Bar(x=["ADR", "RevPAR"], y=[ev_adr, ev_rvp], name="Event Window", marker_color=CATEGORICAL[3]))
                fig.add_trace(go.Bar(x=["ADR", "RevPAR"], y=[base_adr, base_rvp], name="All-Period Baseline", marker_color=CATEGORICAL[6]))
                fig.update_layout(barmode="group", height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="$ per Room")
                _render_chart(fig)
            else:
                st.info("Sept 26–Oct 4, 2025 window not present in loaded STR daily data.")
        else:
            st.info("No STR daily KPI data loaded yet.")

    st.subheader("Upcoming Events")
    if not df_events.empty:
        today = pd.Timestamp.now().normalize()
        ev = df_events.copy()
        ev["event_date"] = pd.to_datetime(ev["event_date"], errors="coerce")
        upcoming = ev[ev["event_date"] >= today].sort_values("event_date").head(6)
        if not upcoming.empty:
            upcoming = upcoming.assign(
                days_away=(upcoming["event_date"] - today).dt.days,
                event_date_str=upcoming["event_date"].dt.strftime("%b %d, %Y"),
            )
            st.dataframe(
                upcoming[["event_name", "event_date_str", "category", "days_away", "is_major"]]
                .rename(columns={"event_name": "Event", "event_date_str": "Date", "category": "Category",
                                  "days_away": "Days Away", "is_major": "Major"}),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("No upcoming events found in vdp_events.")
    else:
        st.info("No events calendar loaded yet (vdp_events).")

    st.markdown(f"""
    **Next Best Actions**
    1. **Build 3–5 signature annual events around the Ohana model**. Target 60%+ OOS draw, timed against the {q3_label or "Q3"} compression window to compound demand rather than just fill shoulder nights.
    2. **Annualize Ohana economics in budget planning**. $18.4M destination spend against $14.6M direct expenditure is the strongest event-ROI benchmark on record.
    3. **Brief the City on TBID impact per event dollar**. At the 3.2x multiplier and 1.25% blended TBID rate, each $1M in event expenditure yields roughly $40K in TBID revenue.
    """)

    _nav_row([
        ("Compression Calendar", "compression", "events_to_comp"),
        ("Revenue Generation", "revenue", "events_to_rev"),
        ("Market Intelligence", "intelligence", "events_to_intel"),
    ])


def page_market_intelligence(df_costar: pd.DataFrame, df_compset: pd.DataFrame,
                              df_costar_snap: pd.DataFrame) -> None:
    """Page 10: Market Intelligence. Source: costar_competitive_set
    (load_costar_compset) for RGI-by-property, fact_str_group_metrics
    (load_str_compset) for the real 6-market Dana Point comp set (Dana Point,
    Newport Beach, La Jolla, Santa Barbara, Monterey-Carmel, Huntington Beach,
    per CLAUDE.md, NOT Anaheim), and costar_market_snapshot
    (load_costar_snapshot, filtered to annual report_period rows) for the
    Newport Beach/Dana Point submarket trend. The old "external festival
    calendar" chart had no backing table (no competitor-city attendance data
    exists in this DB) and is replaced with this real regional trend.

    NOTE (2026-09-19): no real CoStar export carries per-property MPI/ARI/RGI
    data (full audit: vdp_costar_data_audit.md). Whichever loader eventually
    populates the df_costar argument for this page must NOT query
    costar_competitive_set, that table is a documented hardcoded placeholder
    (see load_costar_reports.py). Pass an empty DataFrame instead, matching
    components_group.py's _load_competitive_set, the rgi_avg block below
    already degrades to "N/A" on an empty df, no fabricated RGI is shown.
    Per Heather, only Newport Beach/Dana Point CoStar data is ever shown on
    this page, never Orange County or United States."""
    _hero("Market <span>Intelligence</span>",
          "Strategic Planning &nbsp;·&nbsp; CoStar Comp Set &nbsp;·&nbsp; STR 6-Market Comparison")
    _framing(
        "Dana Point's real comp set is Newport Beach, La Jolla, Santa Barbara, Monterey-Carmel and "
        "Huntington Beach, not a broader Orange County average. This page checks RGI against the "
        "CoStar comp set and occupancy against that six-market STR group so the competitive read "
        "stays anchored to the right peers."
    )

    rgi_avg = np.nan
    if not df_costar.empty and "rgi" in df_costar.columns:
        props = df_costar[~df_costar["property_name"].astype(str).str.contains("Blended", na=False)]
        if not props.empty:
            rgi_avg = props["rgi"].mean()

    dp_occ = dp_adr = dp_rvp = np.nan
    leader_market, leader_occ = "N/A", np.nan
    if not df_compset.empty:
        latest_date = df_compset["as_of_date"].max()
        snap = df_compset[
            (df_compset["as_of_date"] == latest_date) &
            (df_compset["segment"] == "Total") &
            (df_compset["data_period"] == "current")
        ]
        occ_by_market = snap[snap["metric_name"] == "occ_pct"].set_index("market")["metric_value"]
        adr_by_market = snap[snap["metric_name"] == "adr"].set_index("market")["metric_value"]
        rvp_by_market = snap[snap["metric_name"] == "revpar"].set_index("market")["metric_value"]
        dp_occ = occ_by_market.get("Dana Point", np.nan)
        dp_adr = adr_by_market.get("Dana Point", np.nan)
        dp_rvp = rvp_by_market.get("Dana Point", np.nan)
        if not occ_by_market.empty:
            leader_market = occ_by_market.idxmax()
            leader_occ = occ_by_market.max()

    rgi_sentence = f"VDP portfolio properties average {rgi_avg:.0f} RGI against the South OC comp set. " if pd.notna(rgi_avg) else ""
    _headline(
        "intelligence", "Market Intelligence", f"{rgi_avg:.0f}" if pd.notna(rgi_avg) else "N/A",
        "RGI vs. South Orange County comp set (CoStar)",
        f"{rgi_sentence}Among the 6-market Dana Point STR comp set, {leader_market} leads on occupancy "
        f"at {leader_occ:.1f}%, while Dana Point runs {dp_occ:.1f}% occupancy at a ${dp_adr:,.0f} ADR."
        if pd.notna(leader_occ) and pd.notna(dp_occ) else rgi_sentence,
    )

    _kpis([
        ("RGI (vs. Comp)", f"{rgi_avg:.0f}" if pd.notna(rgi_avg) else "N/A", "CoStar, 100 = par", "intelligence", "CoStar competitive set, latest report"),
        ("Dana Point Occupancy", _fmt_pct(dp_occ), "vs. 6-market STR comp set", "occupancy", "STR 6-market comp set, latest snapshot"),
        ("Dana Point ADR", f"${dp_adr:,.0f}" if pd.notna(dp_adr) else "N/A", "vs. 6-market STR comp set", "rate_strategy", "STR 6-market comp set, latest snapshot"),
        ("Comp-Set Occupancy Leader", leader_market, f"{leader_occ:.1f}%" if pd.notna(leader_occ) else "", "intelligence", "STR 6-market comp set, latest snapshot"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Occupancy, Dana Point Comp Set (STR)")
        if not df_compset.empty:
            latest_date = df_compset["as_of_date"].max()
            snap = df_compset[
                (df_compset["as_of_date"] == latest_date) &
                (df_compset["segment"] == "Total") &
                (df_compset["data_period"] == "current") &
                (df_compset["metric_name"] == "occ_pct")
            ].sort_values("metric_value", ascending=False)
            fig = go.Figure([go.Bar(
                x=snap["market"], y=snap["metric_value"],
                marker_color=[CATEGORICAL[1] if m == "Dana Point" else CATEGORICAL[0] for m in snap["market"]],
            )])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Occupancy %")
            _render_chart(fig)
        else:
            st.info("No STR comp-set data loaded yet (fact_str_group_metrics).")

    with c2:
        st.subheader("Newport Beach/Dana Point Submarket, Annual ADR & RevPAR (CoStar)")
        if not df_costar_snap.empty:
            snap = df_costar_snap[df_costar_snap["market"].astype(str).str.contains("Dana", na=False)].copy()
            snap = snap[snap["report_period"].astype(str).str.match(r"^\d{4}-12-31$", na=False)]
            snap = snap.sort_values("report_period")
            if not snap.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=snap["report_period"].str[:4], y=snap["adr_usd"], name="ADR",
                                          line=dict(color=CATEGORICAL[0], width=2)))
                fig.add_trace(go.Scatter(x=snap["report_period"].str[:4], y=snap["revpar_usd"], name="RevPAR",
                                          line=dict(color=CATEGORICAL[3], width=2)))
                fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="$", xaxis_title="Year")
                _render_chart(fig)
            else:
                st.info("No annual CoStar market snapshot rows found for the Dana Point submarket.")
        else:
            st.info("No CoStar market snapshot data loaded yet (costar_market_snapshot).")

    st.markdown(f"""
    **Next Best Actions**
    1. **Track {leader_market}'s occupancy lead** ({leader_occ:.1f}% vs. Dana Point's {dp_occ:.1f}%). It is the pace-setter in the 6-market comp set.
    2. **Monitor CoStar RGI monthly** ({rgi_avg:.0f} average across VDP portfolio properties) to catch rate-floor slippage before it compounds.
    3. **Use the Newport Beach/Dana Point submarket trend** as the forward planning baseline for capital and rate decisions, it is the only multi-year, forward-looking series in this comp set.
    """)

    _nav_row([
        ("Rate Strategy", "rate_strategy", "intel_to_rate"),
        ("Visitor Markets", "visitor_markets", "intel_to_mkt"),
        ("Compression Calendar", "compression", "intel_to_comp"),
    ])


def page_stakeholder_brief(df_kpi: pd.DataFrame, df_comp: pd.DataFrame, df_dfy: pd.DataFrame,
                            df_spend: pd.DataFrame, df_group: pd.DataFrame, df_zfe: pd.DataFrame) -> None:
    """Page 11: Stakeholder Brief. Source: kpi_daily_summary + kpi_compression_quarterly
    for hotel-side roles, datafy_overview_kpis + datafy_overview_category_spending
    for visitor-economy roles, group_intelligence for TBID/TOT, zartico_future_events_summary
    (historical reference) for event growth. Any figure with no backing table
    (e.g. a total-dollar destination-spend number, or event attendance counts)
    is omitted rather than invented."""
    _hero("Stakeholder <span>Brief</span>",
          "Strategic Planning &nbsp;·&nbsp; Role-Specific Summary")
    _framing(
        "A city official, a hotel manager, a BID business owner and a festival operator each need a "
        "different slice of the same data. Select a role below to see the four figures and three "
        "next steps that matter most to that seat."
    )

    role = st.radio("Who are you?", ["City Official", "Hotel Manager", "BID/Business", "Festival Ops"], horizontal=True)

    if role == "City Official":
        g = df_group.iloc[0] if not df_group.empty else None
        tbid_low = g.get("estimated_group_tbid_rev_low") if g is not None else np.nan
        tbid_high = g.get("estimated_group_tbid_rev_high") if g is not None else np.nan
        dfy = df_dfy.sort_values("report_period_start", ascending=False).iloc[0] if not df_dfy.empty else None
        total_trips = dfy.get("total_trips") if dfy is not None else np.nan
        oos_pct = dfy.get("out_of_state_vd_pct") if dfy is not None else np.nan
        tot_est = np.nan
        if not df_kpi.empty:
            k = df_kpi.copy()
            k["as_of_date"] = pd.to_datetime(k["as_of_date"])
            cutoff = k["as_of_date"].max() - timedelta(days=365)
            # revenue not in kpi_daily_summary; approximate TOT via ADR*occ isn't reliable,
            # so TOT here is left to the Revenue Generation page which has the real figure.
        _headline(
            "stakeholder", "City Official Brief", _fmt_money((tbid_low + tbid_high) / 2 if pd.notna(tbid_low) else np.nan),
            "estimated annual group TBID potential (group_intelligence)",
            f"Group segment TBID potential runs {_fmt_money(tbid_low)}–{_fmt_money(tbid_high)} annually. "
            f"Total visitor trips are {int(total_trips):,} with {_fmt_pct(oos_pct)} out-of-state visitor days, "
            f"the base for state tourism co-op grant qualification." if pd.notna(total_trips) else
            f"Group segment TBID potential runs {_fmt_money(tbid_low)}–{_fmt_money(tbid_high)} annually.",
        )
        _kpis([
            ("Group TBID Potential", f"{_fmt_money(tbid_low)}–{_fmt_money(tbid_high)}", "1.25% blended rate", "revenue", "group_intelligence, most recent benchmark"),
            ("Total Visitor Trips", f"{int(total_trips):,}" if pd.notna(total_trips) else "N/A", "Datafy annual", "visitor_markets", "Datafy annual report"),
            ("Out-of-State Visitor Days", _fmt_pct(oos_pct), "state co-op grant eligibility", "visitor_markets", "Datafy annual report"),
            ("Full TOT/TBID Detail", "See Revenue Page", "trailing-12mo room revenue basis", "revenue", "STR daily, trailing 12 months"),
        ])
        st.markdown("""
        **Your Next Steps**
        1. **Review the full TBID/TOT projection on the Revenue Generation page**. It is built from trailing 12-month room revenue, the most defensible basis for a Council presentation.
        2. **Use the out-of-state visitor share** to support state tourism co-op grant applications.
        3. **Track group_intelligence's TBID uplift-per-5pp-shift figure** (Group Demand page) when discussing shoulder-season event incentives with hotel partners.
        """)

    elif role == "Hotel Manager":
        if df_kpi.empty:
            st.info("No STR daily KPI data loaded yet.")
        else:
            k = df_kpi.sort_values("as_of_date")
            latest = k.iloc[-1]
            recent90 = k.tail(90)
            peak_adr = recent90["adr"].max()
            peak_occ = recent90["occ_pct"].max()
            q3_days = None
            if not df_comp.empty:
                q3_rows = df_comp[df_comp["quarter"].astype(str).str.endswith("Q3")].sort_values("quarter", ascending=False)
                if not q3_rows.empty:
                    q3_days = int(q3_rows.iloc[0]["days_above_80_occ"])
            _headline(
                "hotel_operations", "Hotel Manager Brief", f"${float(latest.get('adr', 0) or 0):,.0f}",
                f"current ADR, as of {pd.to_datetime(latest['as_of_date']).strftime('%b %d, %Y')}",
                f"Current occupancy is {float(latest.get('occ_pct', 0) or 0):.1f}%. Over the last 90 days, "
                f"peak ADR reached ${peak_adr:,.0f} and peak occupancy hit {peak_occ:.1f}%. The most recent "
                f"Q3 logged {q3_days} compression days above 80% occupancy." if q3_days is not None else
                f"Current occupancy is {float(latest.get('occ_pct', 0) or 0):.1f}%.",
            )
            _asof_hm = pd.to_datetime(latest['as_of_date']).strftime('%b %d, %Y')
            _kpis([
                ("Current ADR", f"${float(latest.get('adr', 0) or 0):,.0f}", _delta_pct(latest.get("adr_yoy")), "rate_strategy", f"STR daily, as of {_asof_hm}"),
                ("Current Occupancy", f"{float(latest.get('occ_pct', 0) or 0):.1f}%", _delta_pct(latest.get("occ_yoy")), "occupancy", f"STR daily, as of {_asof_hm}"),
                ("90-Day Peak ADR", f"${peak_adr:,.0f}", "last 90 days", "rate_strategy", "STR daily, trailing 90 days"),
                ("Q3 Compression", f"{q3_days if q3_days is not None else 'N/A'} days", "above 80% occ", "compression", "kpi_compression_quarterly, most recent Q3"),
            ])
            st.markdown("""
            **Your Next Steps**
            1. **Deploy minimum-stay rules on high-occupancy dates**. Check the Compression Calendar page for the exact peak window.
            2. **Build early-arrival packages ahead of the next compression stretch** to extend shoulder-night demand.
            3. **Coordinate with the comp set on rate floors**. See the Rate Strategy page for the current RGI and weekend premium.
            """)

    elif role == "BID/Business":
        top_cat = None
        if not df_spend.empty:
            latest_period = df_spend["report_period_start"].max()
            spend_latest = df_spend[df_spend["report_period_start"] == latest_period].sort_values(
                "spend_share_pct", ascending=False)
            top_cat = spend_latest.iloc[0] if not spend_latest.empty else None
        dfy = df_dfy.sort_values("report_period_start", ascending=False).iloc[0] if not df_dfy.empty else None
        repeat_pct = dfy.get("repeat_visitors_pct") if dfy is not None else np.nan
        top_cat_name = top_cat["category"] if top_cat is not None else "N/A"
        top_cat_share = f"{top_cat['spend_share_pct']:.1f}%" if top_cat is not None else "N/A"
        _headline(
            "spend", "BID/Business Brief", top_cat_share,
            f"{top_cat_name} share of visitor spend",
            f"{top_cat_name} is the largest visitor-spend category at {top_cat_share}. Repeat visitors "
            f"account for {_fmt_pct(repeat_pct)} of trips, a base worth building loyalty programs around.",
        )
        _kpis([
            ("Top Spend Category", top_cat_name, top_cat_share, "spend", f"Datafy, report period starting {latest_period}" if top_cat is not None else "Datafy"),
            ("Repeat Visitor Rate", _fmt_pct(repeat_pct), "loyalty base", "visitor_economy", "Datafy annual report"),
            ("Category Detail", "See Spend Pathways", "full category breakdown", "spend", "Datafy category spending"),
            ("Feeder Markets", "See Visitor Markets", "top DMA detail", "visitor_markets", "Datafy DMA breakdown"),
        ])
        st.markdown(f"""
        **Your Next Steps**
        1. **Form a partnership around {top_cat_name}** ({top_cat_share} of visitor spend). The highest-leverage category for co-marketing.
        2. **Build a repeat-visitor loyalty offer**. {_fmt_pct(repeat_pct)} of trips are repeat visits already.
        3. **Review the Spend Pathways page** for the full category share and spend-correlation breakdown before committing marketing budget.
        """)

    else:  # Festival Ops
        ev_growth = att_growth = np.nan
        if not df_zfe.empty:
            ev_growth = df_zfe.iloc[0].get("yoy_pct_change_events")
            att_growth = df_zfe.iloc[0].get("yoy_pct_change_attendees")
        _headline(
            "events", "Festival Ops Brief", "68%",
            "out-of-state attendance, Ohana Fest 2025 (verified Datafy benchmark)",
            f"Ohana Fest 2025 drew 68% out-of-state attendees at a 3.2x spend multiplier. Per Zartico's "
            f"historical reference snapshot, Dana Point's broader events calendar grew {ev_growth:+.0f}% YoY "
            f"in event count and {att_growth:+.0f}% YoY in attendees. Attendance counts by individual event "
            f"are not tracked in the current database." if pd.notna(ev_growth) else
            "Ohana Fest 2025 drew 68% out-of-state attendees at a 3.2x spend multiplier.",
        )
        _kpis([
            ("OOS Attendance (Ohana '25)", "68%", "verified Datafy benchmark", "events", "Datafy, Ohana Fest 2025 (Sept 26–Oct 4, 2025)"),
            ("Spend Multiplier (Ohana '25)", "3.2x", "direct expenditure", "revenue", "Datafy, Ohana Fest 2025 (Sept 26–Oct 4, 2025)"),
            ("YoY Event Growth", f"{ev_growth:+.0f}%" if pd.notna(ev_growth) else "N/A", "Zartico historical reference", "intelligence", "Zartico, historical reference snapshot, Jun 2025"),
            ("YoY Attendee Growth", f"{att_growth:+.0f}%" if pd.notna(att_growth) else "N/A", "Zartico historical reference", "group", "Zartico, historical reference snapshot, Jun 2025"),
        ])
        st.markdown("""
        **Your Next Steps**
        1. **Review the full Ohana Fest ROI breakdown on the Events Impact page** before pitching new signature events.
        2. **Negotiate a multi-year date hold with the City** to protect the compression window Ohana Fest anchors.
        3. **Request per-event attendance and ticket-price tracking**. The current database has verified spend and ADR-lift figures for Ohana Fest but no attendance counts for the rest of the calendar.
        """)


def page_brain_status(table_counts: dict, df_log: pd.DataFrame, df_kpi: pd.DataFrame, df_dfy: pd.DataFrame) -> None:
    """Page 12: Brain Status. Source: get_table_counts() (row counts across all
    tracked tables), load_load_log() (per-source pipeline run timestamps),
    kpi_daily_summary (STR freshness), datafy_overview_kpis (Datafy freshness)."""
    _hero("Brain <span>Status</span>",
          "Strategic Planning &nbsp;·&nbsp; Pipeline Health &nbsp;·&nbsp; Data Freshness")
    _framing(
        "Every figure on this dashboard is only as trustworthy as the pipeline that fed it. This page "
        "is the honest accounting of that: how old the STR data is, when Datafy last refreshed, when "
        "the pipeline last ran and what share of the tracked tables actually hold rows."
    )

    today = pd.Timestamp.now().normalize()

    str_age = None
    if not df_kpi.empty:
        str_max = pd.to_datetime(df_kpi["as_of_date"]).max()
        str_age = (today - str_max).days

    datafy_period = None
    if not df_dfy.empty:
        datafy_period = df_dfy.sort_values("report_period_start", ascending=False).iloc[0]["report_period_end"]

    last_run = None
    if not df_log.empty:
        last_run = pd.to_datetime(df_log["run_at"], errors="coerce").max()

    total_tables = len(table_counts) if table_counts else 0
    populated_tables = sum(1 for v in (table_counts or {}).values() if isinstance(v, int) and v > 0)
    quality_pct = (populated_tables / total_tables * 100) if total_tables else 0

    str_age_str = f"{str_age} days" if str_age is not None else "N/A"
    last_run_str = last_run.strftime("%b %d, %Y") if last_run is not None and pd.notna(last_run) else "N/A"

    _headline(
        "brain_status", "Brain Status", f"{quality_pct:.0f}%",
        f"of {total_tables} tracked tables have data",
        f"STR data is {str_age_str} old. Datafy's latest report period ends {datafy_period}. "
        f"The pipeline last ran {last_run_str}. {populated_tables} of {total_tables} tracked tables "
        f"currently hold rows.",
    )

    _kpis([
        ("STR Data Age", str_age_str, "days since latest STR row", "compression", "As of today"),
        ("Datafy Report Period", str(datafy_period) if datafy_period else "N/A", "latest report_period_end", "intelligence", "Datafy, latest report cycle"),
        ("Last Pipeline Run", last_run_str, "load_log, most recent run_at", "brain_status", "load_log, most recent run"),
        ("Data Coverage", f"{quality_pct:.0f}%", f"{populated_tables}/{total_tables} tables populated", "brain_status", "As of today"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Latest Run by Pipeline Source")
        if not df_log.empty:
            latest_by_source = df_log.copy()
            latest_by_source["run_at"] = pd.to_datetime(latest_by_source["run_at"], errors="coerce")
            latest_by_source = latest_by_source.groupby("source")["run_at"].max().sort_values(ascending=False)
            for source, ts in latest_by_source.items():
                ts_str = ts.strftime("%b %d, %Y %H:%M") if pd.notna(ts) else "N/A"
                st.markdown(
                    f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
                    f'background:#059669;margin-right:8px;"></span>'
                    f'<strong>{source}</strong> &nbsp;{ts_str}',
                    unsafe_allow_html=True,
                )
        else:
            st.info("No load_log rows found yet.")

    with c2:
        st.subheader("Data Freshness by Source, Days Since Last Run")
        if not df_log.empty:
            latest_by_source = df_log.copy()
            latest_by_source["run_at"] = pd.to_datetime(latest_by_source["run_at"], errors="coerce")
            latest_by_source = latest_by_source.groupby("source")["run_at"].max()
            days_ago = (today - latest_by_source).dt.days.sort_values()
            colors_fresh = [CATEGORICAL[3] if d < 5 else (CATEGORICAL[1] if d < 30 else CATEGORICAL[4]) for d in days_ago]
            fig = go.Figure([go.Bar(x=days_ago.index, y=days_ago.values, marker_color=colors_fresh)])
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Days Since Last Run")
            _render_chart(fig)
        else:
            st.info("No load_log rows found yet.")

    st.markdown("""
    **Maintenance Items**
    - STR daily export: check every Mon/Wed/Fri.
    - Datafy annual refresh: schedule alongside Datafy's next annual report cycle.
    - Insights regenerate: `compute_insights.py` runs on every pipeline execution (per CLAUDE.md, this step is not optional).
    - Data quality audits: monthly health check against `get_table_counts()`.

    **To Add a New Data Source (per CLAUDE.md's Standard Process):**
    1. Raw files → `data/<source_name>/`
    2. Loader → `scripts/load_<source>.py`
    3. Relationships → `scripts/build_table_relationships.py`
    4. Pipeline → `scripts/run_pipeline.py`
    5. Dashboard → `dashboard/app.py` (loader) + `dashboard/pages.py` (visualization)
    """)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 13: OVERVIEW — cross-category landing page. Per owner feedback, this
# is now a genuine cross-dataset analysis, not a light summary: the top
# insight across ALL FIVE insights_daily audiences (not just cross/dmo), a
# multi-category KPI section spanning hotel operations, visitor economy and
# strategic planning with real numbers and date stamps, and a direct path
# into the Classic View's Board Report for the full brain / board-level view.
# ═══════════════════════════════════════════════════════════════════════════

_OVERVIEW_AUDIENCE_LABELS = {
    "dmo": "DMO Strategy", "city": "City / Finance", "visitor": "Visitor Intel",
    "resident": "Community", "cross": "Hidden Signals",
}


def page_overview(df_kpi: pd.DataFrame, df_dfy: pd.DataFrame, df_comp: pd.DataFrame,
                   df_group: pd.DataFrame, df_insights: pd.DataFrame,
                   df_compset: pd.DataFrame = None, df_spend: pd.DataFrame = None,
                   df_events: pd.DataFrame = None, df_zfe: pd.DataFrame = None) -> None:
    df_compset = df_compset if df_compset is not None else pd.DataFrame()
    df_spend = df_spend if df_spend is not None else pd.DataFrame()
    df_events = df_events if df_events is not None else pd.DataFrame()
    df_zfe = df_zfe if df_zfe is not None else pd.DataFrame()

    _hero("Dana Point <span>PULSE</span>",
          "Cross-Category Overview &nbsp;·&nbsp; Hotel Operations &nbsp;·&nbsp; Visitor Economy &nbsp;·&nbsp; Strategic Planning")
    _framing(
        "This is Dana Point's full cross-dataset read: today's top signal from every stakeholder "
        "audience the brain tracks, the real numbers behind hotel performance, the visitor economy "
        "and strategic planning in one place, and a direct line into the Board Report for the "
        "board-level version of the same data."
    )

    # ── (a) Top signal across ALL FIVE insights_daily audiences ─────────────
    # Per CLAUDE.md's "All-audience summaries" lesson: showing only dmo/cross
    # is incomplete. City officials, visitors, residents and the DMO itself
    # are five separate stakeholders with five separate top priorities.
    if not df_insights.empty:
        _top_cross = df_insights[df_insights["audience"].isin(["cross", "dmo"])].sort_values("priority")
        _headline_src = _top_cross if not _top_cross.empty else df_insights.sort_values("priority")
        _top = _headline_src.iloc[0]
        _top_headline = str(_top.get("headline", "") or "").strip()
        _top_body = str(_top.get("body", "") or "").strip()
        if len(_top_body) > 320:
            _top_body = _top_body[:317].rstrip() + "…"
        _top_as_of = str(_top.get("as_of_date", "") or "")
        _headline(
            "overview", "Today's Top Signal", "",
            "highest-priority insight, cross and DMO audiences" + (f", generated {_top_as_of}" if _top_as_of else ""),
            f"{_top_headline} {_top_body}".strip(),
            as_of=f"insights_daily, generated {_top_as_of}" if _top_as_of else "insights_daily",
        )

        st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)
        st.markdown('<p class="insight-body" style="padding-left:0;font-weight:700;">'
                     'Top Signal by Stakeholder Audience</p>', unsafe_allow_html=True)
        audience_order = ["dmo", "city", "visitor", "resident", "cross"]
        acols = st.columns(5)
        for i, aud in enumerate(audience_order):
            subset = df_insights[df_insights["audience"] == aud].sort_values("priority")
            with acols[i]:
                if subset.empty:
                    st.markdown(
                        f'<div class="kpi-card"><div class="kpi-label">{_OVERVIEW_AUDIENCE_LABELS[aud]}</div>'
                        f'<div class="kpi-date" style="text-align:center;">No insight generated today</div></div>',
                        unsafe_allow_html=True,
                    )
                    continue
                row = subset.iloc[0]
                body = str(row.get("body", "") or "").strip()
                if len(body) > 130:
                    body = body[:127].rstrip() + "…"
                as_of_i = str(row.get("as_of_date", "") or "")
                st.markdown(
                    f'<div class="kpi-card" style="text-align:left;">'
                    f'<div class="kpi-label">{_OVERVIEW_AUDIENCE_LABELS[aud]}</div>'
                    f'<div style="font-size:13px;font-weight:700;color:var(--dp-text-1);margin:6px 0 4px 0;">'
                    f'{row["headline"]}</div>'
                    f'<div style="font-size:11.5px;color:var(--dp-text-3);line-height:1.4;">{body}</div>'
                    f'<div class="kpi-date">insights_daily, {as_of_i}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
    else:
        st.info("No insights_daily rows loaded yet. Run the pipeline (compute_insights.py) to generate today's signals.")

    # ── (b) Multi-category KPI section, real numbers, hotel ops + visitor
    #        economy + strategic planning, per owner request for a genuine
    #        cross-dataset analysis rather than one metric per category. ────
    occ_now, occ_as_of, occ_delta = None, "", ""
    adr_now, rvp_now = None, None
    if not df_kpi.empty:
        _k = df_kpi.sort_values("as_of_date")
        _latest = _k.iloc[-1]
        occ_now = float(_latest.get("occ_pct", 0) or 0)
        adr_now = float(_latest.get("adr", 0) or 0)
        rvp_now = float(_latest.get("revpar", 0) or 0)
        occ_as_of = pd.to_datetime(_latest["as_of_date"]).strftime("%b %d, %Y")
        occ_delta = _delta_pct(_latest.get("occ_yoy")) or "flat vs. last year"

    trips_now, trips_period, oos_pct = None, "", None
    top_cat_name, top_cat_share = "N/A", "N/A"
    if not df_dfy.empty:
        _d = df_dfy.sort_values("report_period_start", ascending=False).iloc[0]
        trips_now = _d.get("total_trips")
        oos_pct = _d.get("out_of_state_vd_pct")
        trips_period = f"{_d['report_period_start']} to {_d['report_period_end']}"
    if not df_spend.empty:
        _sp_period = df_spend["report_period_start"].max()
        _sp_latest = df_spend[df_spend["report_period_start"] == _sp_period].sort_values(
            "spend_share_pct", ascending=False)
        if not _sp_latest.empty:
            top_cat_name = _sp_latest.iloc[0]["category"]
            top_cat_share = f"{_sp_latest.iloc[0]['spend_share_pct']:.1f}%"

    q3_days, q3_label = None, ""
    if not df_comp.empty:
        _q3 = df_comp[df_comp["quarter"].astype(str).str.endswith("Q3")].sort_values("quarter", ascending=False)
        if not _q3.empty:
            q3_days = int(_q3.iloc[0]["days_above_80_occ"])
            q3_label = str(_q3.iloc[0]["quarter"])

    mix = _lodging_mix(df_compset)

    next_event_name, next_event_days = "N/A", None
    if not df_events.empty:
        today = pd.Timestamp.now().normalize()
        ev = df_events.copy()
        ev["event_date"] = pd.to_datetime(ev["event_date"], errors="coerce")
        upcoming = ev[ev["event_date"] >= today].sort_values("event_date")
        if not upcoming.empty:
            next_event_name = upcoming.iloc[0]["event_name"]
            next_event_days = (upcoming.iloc[0]["event_date"] - today).days

    st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
    st.markdown('<p class="insight-body" style="padding-left:0;font-weight:700;">Hotel Operations</p>',
                unsafe_allow_html=True)
    _kpis([
        ("Occupancy", f"{occ_now:.1f}%" if occ_now is not None else "N/A", occ_delta, "occupancy",
         f"STR daily, as of {occ_as_of}" if occ_as_of else "STR daily, no data loaded"),
        ("ADR", f"${adr_now:,.0f}" if adr_now is not None else "N/A", "current", "rate_strategy",
         f"STR daily, as of {occ_as_of}" if occ_as_of else "STR daily, no data loaded"),
        ("RevPAR", f"${rvp_now:,.0f}" if rvp_now is not None else "N/A", "current", "revenue",
         f"STR daily, as of {occ_as_of}" if occ_as_of else "STR daily, no data loaded"),
        (f"{q3_label or 'Q3'} Compression", f"{q3_days} days" if q3_days is not None else "N/A", "80%+ occupancy", "compression",
         f"{q3_label}, STR daily" if q3_label else "STR daily, no data loaded"),
    ])
    if mix:
        st.caption(
            f"Lodging mix, week of {pd.to_datetime(mix['as_of']).strftime('%b %d, %Y')}: "
            f"{mix['transient_pct']:.0f}% transient, {mix['group_pct']:.0f}% group "
            f"(STR weekly, fact_str_group_metrics). Full breakdown on the Occupancy Outlook page."
        )

    st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)
    st.markdown('<p class="insight-body" style="padding-left:0;font-weight:700;">Visitor Economy</p>',
                unsafe_allow_html=True)
    _kpis([
        ("Total Visitor Trips", f"{int(trips_now):,}" if pd.notna(trips_now) else "N/A", trips_period, "visitor_markets",
         f"Datafy, {trips_period}" if trips_period else "Datafy, no data loaded"),
        ("Out-of-State Visitor Days", _fmt_pct(oos_pct), "vs. compare period", "visitor_markets",
         f"Datafy, {trips_period}" if trips_period else "Datafy, no data loaded"),
        ("Top Spend Category", top_cat_name, top_cat_share, "spend",
         f"Datafy, {trips_period}" if trips_period else "Datafy, no data loaded"),
    ])

    st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)
    st.markdown('<p class="insight-body" style="padding-left:0;font-weight:700;">Strategic Planning</p>',
                unsafe_allow_html=True)
    _kpis([
        ("Next Event", next_event_name, f"{next_event_days} days away" if next_event_days is not None else "vdp_events",
         "events", "Visit Dana Point events calendar"),
        ("Ohana Fest '25 ADR Lift", "+$139", "verified Datafy benchmark", "revenue",
         "Datafy, Sept 26–Oct 4, 2025"),
        ("YoY Event Growth", f"{df_zfe.iloc[0].get('yoy_pct_change_events'):+.0f}%" if not df_zfe.empty and pd.notna(df_zfe.iloc[0].get('yoy_pct_change_events')) else "N/A",
         "Zartico historical reference", "intelligence", "Zartico, historical reference snapshot, Jun 2025"),
    ])

    # ── (c) Path to the Board Report / brain in Classic View ────────────────
    st.markdown('<div style="height:14px"></div>', unsafe_allow_html=True)
    st.markdown(
        format_insight_card(
            _icon("brain_status", 24, "#0891B2"), "Run the Full Brain",
            "", body=(
                "This page is the 30-second cross-section. The Board Report carries the full "
                "12-month scorecard, the AI Assistant panel and every insight the brain has "
                "generated across all five audiences, all inside Classic View."
            ), accent_color="#0891B2",
        ),
        unsafe_allow_html=True,
    )
    if st.button("Open Board Report (Classic View)", use_container_width=True, key="ov_to_board_report"):
        st.session_state["classic_toggle"] = True
        st.rerun()
    st.caption(
        "Opens Classic View's Today's Overview tab, Deep Dives, Board Report sub-tab, the same "
        "auto-generated talking points used for board and council presentations."
    )

    st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
    st.markdown('<p class="insight-body" style="padding-left:0;font-weight:700;">Go deeper</p>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    _sections = [
        (c1, "hotel_operations", "Hotel Operations", "Occupancy, rate strategy, revenue generation and the compression calendar.",
         "#0891B2", "occupancy", "ov_to_hotel"),
        (c2, "visitor_economy", "Visitor Economy", "Where visitors come from, what they spend on, how long they stay and how group business fits in.",
         "#7C3AED", "visitor_markets", "ov_to_visitor"),
        (c3, "strategic_planning", "Strategic Planning", "Event ROI, the competitive landscape, role-specific briefs and pipeline health.",
         "#D97706", "events", "ov_to_strategic"),
    ]
    for col, icon_key, title, body, accent, target_page, btn_key in _sections:
        with col:
            st.markdown(
                format_insight_card(_icon(icon_key, 24, accent), title, "", body=body, accent_color=accent),
                unsafe_allow_html=True,
            )
            if st.button(f"Open {title}", use_container_width=True, key=btn_key):
                st.session_state.page = target_page
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE ROUTER
# ═══════════════════════════════════════════════════════════════════════════

def render_page(page_name: str, data: dict) -> None:
    """Route to the correct page based on page_name.

    `data` carries every loader render_page's callers currently need, keyed by
    short name (see the call site in app.py, just above `render_page(...)`):
        kpi, dfy, comp, str, dma, spend, group, events, zfe, zartico_impact,
        costar, compset, costar_snap, table_counts, log, insights,
        travel_types, los
    """
    global _CURRENT_PAGE
    _CURRENT_PAGE = page_name if page_name in ALL_PAGE_KEYS else "overview"

    df_kpi = data.get("kpi", pd.DataFrame())
    df_dfy = data.get("dfy", pd.DataFrame())
    df_comp = data.get("comp", pd.DataFrame())
    df_str = data.get("str", pd.DataFrame())
    df_dma = data.get("dma", pd.DataFrame())
    df_spend = data.get("spend", pd.DataFrame())
    df_group = data.get("group", pd.DataFrame())
    df_events = data.get("events", pd.DataFrame())
    df_zfe = data.get("zfe", pd.DataFrame())
    df_zartico_impact = data.get("zartico_impact", pd.DataFrame())
    df_costar = data.get("costar", pd.DataFrame())
    df_compset = data.get("compset", pd.DataFrame())
    df_costar_snap = data.get("costar_snap", pd.DataFrame())
    table_counts = data.get("table_counts", {})
    df_log = data.get("log", pd.DataFrame())
    df_insights = data.get("insights", pd.DataFrame())
    df_travel_types = data.get("travel_types", pd.DataFrame())
    df_los = data.get("los", pd.DataFrame())

    # Main-page period filter: rendered above the hero on the three pages
    # whose trend charts it actually re-slices (see _PERIOD_PAGES). Owner
    # feedback: this control needs to live on the main content area, not
    # only in the sidebar.
    period_label = "Last 90 Days"
    if page_name in _PERIOD_PAGES:
        period_label = render_period_selector()

    if page_name == "overview":
        page_overview(df_kpi, df_dfy, df_comp, df_group, df_insights, df_compset,
                      df_spend, df_events, df_zfe)
    elif page_name == "occupancy":
        page_occupancy_outlook(df_kpi, df_comp, df_str, period_label, df_compset, df_travel_types)
    elif page_name == "rate_strategy":
        page_rate_strategy(df_kpi, df_str, df_costar, df_compset, period_label)
    elif page_name == "revenue":
        page_revenue_generation(df_kpi, df_str, period_label)
    elif page_name == "compression":
        page_compression_calendar(df_comp, df_str)
    elif page_name == "visitor_markets":
        page_visitor_markets(df_dfy, df_dma)
    elif page_name == "spend":
        page_spend_pathways(df_dfy, df_spend)
    elif page_name == "stay":
        page_stay_patterns(df_dfy, df_los)
    elif page_name == "group":
        page_group_demand(df_group, df_comp)
    elif page_name == "events":
        page_events_impact(df_kpi, df_comp, df_events, df_zfe, df_zartico_impact)
    elif page_name == "intelligence":
        page_market_intelligence(df_costar, df_compset, df_costar_snap)
    elif page_name == "stakeholder":
        page_stakeholder_brief(df_kpi, df_comp, df_dfy, df_spend, df_group, df_zfe)
    elif page_name == "brain_status":
        page_brain_status(table_counts, df_log, df_kpi, df_dfy)
    else:
        st.title("Dana Point PULSE")
        st.markdown("Select a page from the menu.")
