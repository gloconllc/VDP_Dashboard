"""
generate_weekly_report.py
--------------------------
Builds the Dana Point PULSE weekly report PDF from live data in analytics.sqlite.

Reads:  dashboard/report_template.html (static layout, [[TOKEN]] placeholders)
Writes: logs/weekly_report_latest.pdf   (also logs/weekly_report_<date>.pdf)

Charts are rendered server-side with matplotlib (no browser/JS dependency) and
embedded as base64 PNGs. Final HTML -> PDF conversion uses WeasyPrint (pure
Python, no headless-browser dependency -- reliable on Streamlit Cloud).

Run:
    python3 scripts/generate_weekly_report.py
"""

from __future__ import annotations

import base64
import html
import io
import os
import sqlite3
from datetime import datetime, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
TEMPLATE_PATH = os.path.join(PROJECT_ROOT, "dashboard", "report_template.html")
LOGO_PATH = os.path.join(PROJECT_ROOT, "dashboard", "assets", "vdp_logo.svg")
LOGO_NAV_PATH = os.path.join(PROJECT_ROOT, "dashboard", "assets", "vdp_logo_nav.svg")
PHOTOS_DIR = os.path.join(PROJECT_ROOT, "dashboard", "assets", "photos")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")

# Same NOTES_DB_PATH convention as dashboard/app.py's notes/annotation layer
# (render_notes_block) -- deliberately a separate sqlite file from
# analytics.sqlite so team commentary survives a data-only pipeline commit.
# Reading it here means notes added live also show up in the exported PDF.
NOTES_DB_PATH = os.environ.get(
    "NOTES_DB_PATH", os.path.join(PROJECT_ROOT, "data", "dashboard_notes.sqlite")
)


def _svg_data_uri(path: str) -> str:
    try:
        with open(path, "rb") as fh:
            b64 = base64.b64encode(fh.read()).decode("ascii")
        return f"data:image/svg+xml;base64,{b64}"
    except FileNotFoundError:
        return ""


def _logo_data_uri() -> str:
    return _svg_data_uri(LOGO_PATH)


def _format_datafy_period(period_start: str, period_end: str) -> str:
    """Label a Datafy report_period_start/end pair as 'Annual' when it spans a
    full calendar year, otherwise as a Year-to-Date window. Avoids hardcoding
    'Annual' for exports that are, in fact, partial-year snapshots."""
    if not period_start or not period_end:
        return "N/A"
    try:
        start = datetime.strptime(period_start[:10], "%Y-%m-%d")
        end = datetime.strptime(period_end[:10], "%Y-%m-%d")
    except ValueError:
        return period_start[:4]
    if start.month == 1 and start.day == 1 and end.month == 12 and end.day == 31 and start.year == end.year:
        return f"{start.year} Annual"
    if start.year == end.year:
        return f"{start.year} Year-to-Date ({start.strftime('%b')}-{end.strftime('%b')})"
    return f"{start.strftime('%b %Y')} to {end.strftime('%b %Y')}"


def _logo_nav_data_uri() -> str:
    return _svg_data_uri(LOGO_NAV_PATH)


def _photo_data_uri(filename: str) -> str:
    try:
        with open(os.path.join(PHOTOS_DIR, filename), "rb") as fh:
            b64 = base64.b64encode(fh.read()).decode("ascii")
        return f"data:image/jpeg;base64,{b64}"
    except FileNotFoundError:
        return ""

# Dana Point coastal palette, sampled from Visit Dana Point's own destination
# photography (drone/harbor shots for the ocean navy-teal family, golden-hour
# shots for the terracotta family). Site CSS was not reachable to pull exact
# brand hex values, so this palette is grounded in their real imagery instead.
TEAL = "#1D6E86"
TEAL_DK = "#123C4A"
TEAL_LT = "#8FC4D6"
MAROON = "#A8461F"
MAROON_LT = "#E08A54"
SLATE = "#9C9186"

# Benchmark figures (industry STR/CBRE benchmarks; no live STR group-segment
# feed exists yet -- see CLAUDE.md "Group Business Estimate" methodology).
COMPSET_BENCHMARK = {
    "Newport": 246.48,
    "La Jolla": 216.06,
    "Monterey": 206.98,
    "Sta Barb": 201.84,
    "Hunt Bch": 198.06,
}
GROUP_BENCHMARK = {
    "adr_occ": "$324 / 72.9%",
    "demand_range": "25% – 32%",
    "revenue_range": "$116.3M – $148.9M",
    "tbid_range": "$1.82M – $2.33M",
    "tot_range": "$11.63M – $14.89M",
    "compression_days_annual": "135",
    "demand_share": "60.5%",
}


def _connect() -> sqlite3.Connection:
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)


def _fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    # 220 dpi keeps bar-value labels and tick text sharp even when the PNG is
    # scaled down inside a two-up grid; 150 dpi looked soft/blurry at that
    # render size (2026-08-11 legibility pass).
    fig.savefig(buf, format="png", dpi=220, bbox_inches="tight", transparent=True)
    plt.close(fig)
    buf.seek(0)
    return "data:image/png;base64," + base64.b64encode(buf.read()).decode("ascii")


def _bar_chart(labels, series, colors, ylabel, figsize=(4.6, 2.4), legend=False, label_fmt=None):
    """label_fmt: optional callable(value) -> str, printed above each bar."""
    fig, ax = plt.subplots(figsize=figsize)
    n = len(series)
    width = 0.8 / n
    x = range(len(labels))
    for i, (name, vals, color) in enumerate(zip([s[0] for s in series], [s[1] for s in series], colors)):
        offs = [xi + (i - (n - 1) / 2) * width for xi in x]
        bars = ax.bar(offs, vals, width=width, color=color, label=name)
        if label_fmt:
            ax.bar_label(bars, labels=[label_fmt(v) for v in vals],
                          fontsize=9, fontweight="bold", color="#1E293B", padding=2)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=10.5)
    ax.set_ylabel(ylabel, fontsize=10.5)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=9.5)
    if legend:
        ax.legend(fontsize=9, frameon=False, loc="upper left")
    top = ax.get_ylim()[1]
    if top > 0:
        ax.set_ylim(0, top * 1.18)  # headroom so bar-value labels don't clip
    fig.tight_layout()
    return _fig_to_b64(fig)


def _dual_axis_bar(labels, occ_vals, revpar_vals):
    fig, ax1 = plt.subplots(figsize=(9, 3.6))
    x = range(len(labels))
    width = 0.35
    bars1 = ax1.bar([xi - width / 2 for xi in x], occ_vals, width=width, color=TEAL, label="Occupancy %")
    ax1.bar_label(bars1, labels=[f"{v:.1f}%" for v in occ_vals], fontsize=9, fontweight="bold",
                   color="#0E4B5C", padding=2)
    ax1.set_ylabel("Occupancy %", fontsize=10.5)
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(labels, fontsize=11)
    ax1.set_ylim(0, max(occ_vals, default=0) * 1.25 if occ_vals else 1)
    ax2 = ax1.twinx()
    bars2 = ax2.bar([xi + width / 2 for xi in x], revpar_vals, width=width, color=TEAL_LT, label="RevPAR $")
    ax2.bar_label(bars2, labels=[f"${v:,.0f}" for v in revpar_vals], fontsize=9, fontweight="bold",
                   color="#0E4B5C", padding=2)
    ax2.set_ylabel("RevPAR $", fontsize=10.5)
    ax2.set_ylim(0, max(revpar_vals, default=0) * 1.25 if revpar_vals else 1)
    ax1.tick_params(labelsize=9.5)
    ax2.tick_params(labelsize=9.5)
    ax1.spines[["top"]].set_visible(False)
    ax2.spines[["top"]].set_visible(False)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=9.5, frameon=False, loc="upper right")
    fig.tight_layout()
    return _fig_to_b64(fig)


def _pie_chart(labels, values, colors, figsize=(3.8, 3.8)):
    # Visitor Origins pie (up to 10 DMA slices): the % labels were crowding
    # into each other on adjacent thin slices. Larger canvas, a slightly
    # smaller font, labels pushed further out toward the rim (more
    # circumferential room out there than near the center), and a higher
    # suppression threshold for the smallest slices all reduce the collision.
    fig, ax = plt.subplots(figsize=figsize)
    wedges, _ = ax.pie(
        values, colors=colors, startangle=90, wedgeprops={"linewidth": 1.5, "edgecolor": "white"},
    )
    total = sum(values) or 1
    for wedge, value in zip(wedges, values):
        pct = value / total * 100
        if pct < 6:
            continue
        angle = (wedge.theta1 + wedge.theta2) / 2
        import math
        x = 0.82 * math.cos(math.radians(angle))
        y = 0.82 * math.sin(math.radians(angle))
        ax.text(x, y, f"{pct:.0f}%", ha="center", va="center",
                fontsize=8.5, fontweight="bold", color="white")
    ax.axis("equal")
    fig.tight_layout()
    return _fig_to_b64(fig)


def _donut_chart(labels, values, colors, figsize=(3, 3)):
    fig, ax = plt.subplots(figsize=figsize)
    wedges, _, _ = ax.pie(
        values, colors=colors, startangle=90,
        wedgeprops={"width": 0.42, "linewidth": 1, "edgecolor": "white"},
        autopct="%1.0f%%", pctdistance=0.80,
        textprops={"fontsize": 9.5, "fontweight": "bold", "color": "#1E293B"},
    )
    ax.legend(wedges, labels, loc="lower center", bbox_to_anchor=(0.5, -0.25),
              fontsize=9, frameon=False, ncol=1)
    ax.axis("equal")
    fig.tight_layout()
    return _fig_to_b64(fig)


def _hbar_chart(labels, values, color, xlabel, figsize=(6.4, 3.6), label_fmt=None):
    fig, ax = plt.subplots(figsize=figsize)
    y = range(len(labels))
    bars = ax.barh(list(y), values, color=color)
    fmt = label_fmt or (lambda v: f"{v:.1f}%")
    ax.bar_label(bars, labels=[fmt(v) for v in values], fontsize=9, fontweight="bold",
                 color="#1E293B", padding=3)
    ax.set_yticks(list(y))
    ax.set_yticklabels(labels, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel(xlabel, fontsize=10.5)
    ax.tick_params(labelsize=9.5)
    right = ax.get_xlim()[1]
    if right > 0:
        ax.set_xlim(0, right * 1.15)  # headroom so bar-value labels don't clip
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    return _fig_to_b64(fig)


def _fmt_pct(v, decimals=1, sign=False):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "N/A"
    s = f"{v:+.{decimals}f}%" if sign else f"{v:.{decimals}f}%"
    return s


_NOTE_SECTION_LABELS = {"general": "General"}


def _load_notes_for_report(limit: int = 8) -> str:
    """Pulls the most recent team commentary added in the live dashboard's
    Notes layer (dashboard/app.py's render_notes_block) so it shows up in
    the exported PDF too, not only in the live view. Every field is
    escaped -- this text was typed by a user, not generated by this
    script, and it lands straight into unsafe_allow_html-equivalent HTML."""
    if not os.path.exists(NOTES_DB_PATH):
        return ""
    try:
        ncon = sqlite3.connect(f"file:{NOTES_DB_PATH}?mode=ro", uri=True, timeout=10)
        notes = pd.read_sql_query(
            "SELECT section, note_text, author, created_at FROM dashboard_notes "
            "ORDER BY created_at DESC LIMIT ?", ncon, params=(limit,),
        )
        ncon.close()
    except Exception:
        return ""
    if notes.empty:
        return ""
    cards = ""
    for _, r in notes.iterrows():
        section = str(r["section"] or "general")
        tag = _NOTE_SECTION_LABELS.get(section, section.title())
        author = html.escape(str(r["author"] or "Team"))
        created = html.escape(str(r["created_at"] or ""))
        body = html.escape(str(r["note_text"] or ""))
        cards += (
            f'<div class="insight-card"><div class="insight-tag">{html.escape(tag)} '
            f'&middot; {author} &middot; {created}</div>'
            f'<div class="insight-body">{body}</div></div>\n'
        )
    return cards


def _fmt_usd(v, decimals=0):
    try:
        return f"${float(v):,.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def build_report(date_range: tuple[str, str] | None = None) -> str:
    """Returns path to the generated PDF.

    date_range: optional (start, end) ISO date strings (YYYY-MM-DD) that
    override the hotel-performance (STR) window shown on the cover and on
    the Executive Summary / Hotel Performance pages. When omitted, or when
    the requested range contains no STR data, this falls back to the last
    complete 7-day window ending on the freshest STR date. Datafy and
    CoStar sections always report on their own freshest available period
    regardless of this override, since those sources update on a different
    cadence than STR.
    """
    con = _connect()

    kpi = pd.read_sql_query(
        "SELECT * FROM kpi_daily_summary ORDER BY as_of_date DESC LIMIT 400", con
    )
    kpi["as_of_date"] = pd.to_datetime(kpi["as_of_date"])
    latest_date = kpi["as_of_date"].max()

    # --- hotel-performance window: user override, else last complete 7 days ---
    range_note = ""
    week_df = pd.DataFrame()
    if date_range and len(date_range) == 2 and date_range[0] and date_range[1]:
        req_start = pd.Timestamp(date_range[0])
        req_end = pd.Timestamp(date_range[1])
        week_df = kpi[(kpi["as_of_date"] >= req_start) & (kpi["as_of_date"] <= req_end)].copy()
        if not week_df.empty:
            week_start, week_end = req_start, req_end
        else:
            range_note = " (no hotel data in requested range; showing latest available week)"

    if week_df.empty:
        week_end = latest_date
        week_start = week_end - timedelta(days=6)
        week_df = kpi[(kpi["as_of_date"] >= week_start) & (kpi["as_of_date"] <= week_end)].copy()

    week_df = week_df.sort_values("as_of_date")

    occ_avg = week_df["occ_pct"].mean()
    adr_avg = week_df["adr"].mean()
    revpar_avg = week_df["revpar"].mean()
    occ_yoy_avg = week_df["occ_yoy"].mean() if "occ_yoy" in week_df else None
    adr_yoy_avg = week_df["adr_yoy"].mean() if "adr_yoy" in week_df else None
    revpar_yoy_avg = week_df["revpar_yoy"].mean() if "revpar_yoy" in week_df else None

    if week_start.month == week_end.month:
        week_label = f"{week_start.strftime('%B %-d')} – {week_end.strftime('%-d, %Y')}"
    else:
        week_label = f"{week_start.strftime('%B %-d')} – {week_end.strftime('%B %-d, %Y')}"
    week_label += range_note

    # day-of-week occ/adr for the week (Sun-Sat order)
    week_df["dow"] = week_df["as_of_date"].dt.dayofweek  # Mon=0..Sun=6
    dow_order = [6, 0, 1, 2, 3, 4, 5]  # Sun,Mon,Tue,Wed,Thu,Fri,Sat
    dow_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    occ_by_dow, adr_by_dow = [], []
    for d in dow_order:
        row = week_df[week_df["dow"] == d]
        occ_by_dow.append(float(row["occ_pct"].mean()) if not row.empty else 0.0)
        adr_by_dow.append(float(row["adr"].mean()) if not row.empty else 0.0)

    # Figsize widened/heightened for the full-page (one-category-per-page)
    # layout -- these four charts used to share a half-width page with
    # another category and are now the only visual on their own page.
    chart_occ_dp = _bar_chart(dow_labels, [("Occupancy %", occ_by_dow)], [MAROON], "Occ %",
                               figsize=(6.2, 4.6), label_fmt=lambda v: f"{v:.0f}%")
    chart_adr_dp = _bar_chart(dow_labels, [("ADR $", adr_by_dow)], [MAROON], "ADR $",
                               figsize=(6.2, 4.6), label_fmt=lambda v: f"${v:.0f}")

    # comp-set chart: Dana Point (live) + 5 benchmark markets
    compset_labels = ["Dana Pt"] + list(COMPSET_BENCHMARK.keys())
    compset_vals = [round(revpar_avg, 2)] + list(COMPSET_BENCHMARK.values())
    compset_colors = [MAROON] + [SLATE] * len(COMPSET_BENCHMARK)
    fig, ax = plt.subplots(figsize=(6.2, 4.6))
    compset_bars = ax.bar(compset_labels, compset_vals, color=compset_colors)
    ax.bar_label(compset_bars, labels=[f"${v:,.0f}" for v in compset_vals], fontsize=9,
                 fontweight="bold", color="#1E293B", padding=2)
    ax.set_ylabel("RevPAR $", fontsize=10.5)
    ax.tick_params(labelsize=9.5)
    ax.set_xticklabels(compset_labels, fontsize=10, rotation=30, ha="right")
    ax.set_ylim(0, max(compset_vals) * 1.18)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    chart_compset = _fig_to_b64(fig)

    # compression by quarter
    comp_q = pd.read_sql_query(
        "SELECT quarter, days_above_80_occ, days_above_90_occ FROM kpi_compression_quarterly "
        "ORDER BY quarter DESC LIMIT 5", con
    ).iloc[::-1]
    q_labels = [q.replace("20", "") for q in comp_q["quarter"]]
    chart_compression = _bar_chart(
        q_labels,
        [("80%+ occ", comp_q["days_above_80_occ"].tolist()), ("90%+ occ", comp_q["days_above_90_occ"].tolist())],
        [MAROON, MAROON_LT], "Days", legend=True, figsize=(6.2, 4.6),
        label_fmt=lambda v: f"{int(v)}",
    )
    compression_label = comp_q["quarter"].iloc[-1] if not comp_q.empty else "Q-"
    compression_days = int(comp_q["days_above_80_occ"].iloc[-1]) if not comp_q.empty else 0

    # CoStar chain-scale segments -- real submarket-level data parsed from the
    # newest CoStar "Hospitality-Submarket" PDF (see load_costar_reports.py's
    # _extract_annual_performance / _extract_segment_room_split). This
    # replaces costar_chain_scale_breakdown.csv / costar_competitive_set.csv,
    # two hand-built files ("South Orange County CA", 6 tiers, named
    # individual properties) that traced to no real CoStar export on file.
    # See CLAUDE.md Lessons Learned, 2026-08-11.
    COSTAR_MARKET = "Newport Beach/Dana Point"
    SEGMENT_SCOPES = ["Luxury & Upper Upscale", "Upscale & Upper Midscale", "Midscale & Economy"]

    year_labels = pd.read_sql_query(
        "SELECT DISTINCT year_label FROM costar_annual_performance "
        "WHERE market=? AND report_scope='Overall'", con, params=(COSTAR_MARKET,)
    )["year_label"].tolist()
    current_year = datetime.now().year
    # CoStar's numeric row for the current year is a pace/forecast figure, not
    # a complete "Full Year" actual -- only years strictly before this one are
    # real, closed-book years. The current year's real, actual-to-date figure
    # is the "YTD" row instead.
    real_years = sorted(int(y) for y in year_labels if y.isdigit() and int(y) < current_year)
    has_ytd = "YTD" in year_labels

    # Default to the YTD row when it exists -- it's CoStar's real, actual-
    # to-date figure for the current year and is fresher than "Full Year
    # {last complete year}", even though it covers a partial year. Falls
    # back to the freshest complete year only if no YTD row was parsed.
    if has_ytd:
        seg_year_label, seg_period_text = "YTD", f"Year-to-Date {current_year}"
    elif real_years:
        seg_year_label, seg_period_text = str(real_years[-1]), f"Full Year {real_years[-1]}"
    else:
        seg_year_label, seg_period_text = None, "N/A"

    if date_range and len(date_range) == 2 and date_range[1]:
        req_year = pd.Timestamp(date_range[1]).year
        if req_year == current_year and has_ytd:
            seg_year_label, seg_period_text = "YTD", f"Year-to-Date {current_year}"
        elif req_year in real_years:
            seg_year_label, seg_period_text = str(req_year), f"Full Year {req_year}"
        # else: requested year has no real CoStar data -- keep the default above

    seg = pd.read_sql_query(
        "SELECT report_scope AS chain_scale, occupancy_pct, adr_usd, revpar_usd "
        "FROM costar_annual_performance WHERE market=? AND report_scope IN (?,?,?) "
        "AND year_label=? ORDER BY revpar_usd DESC",
        con, params=[COSTAR_MARKET] + SEGMENT_SCOPES + [seg_year_label],
    ) if seg_year_label else pd.DataFrame(columns=["chain_scale", "occupancy_pct", "adr_usd", "revpar_usd"])

    room_split = pd.read_sql_query(
        "SELECT * FROM costar_segment_room_split WHERE market=? ORDER BY report_date DESC LIMIT 1",
        con, params=(COSTAR_MARKET,),
    )
    rooms_by_segment = {}
    if not room_split.empty:
        rs = room_split.iloc[0]
        rooms_by_segment = {
            "Luxury & Upper Upscale": rs["luxury_upper_upscale_rooms"],
            "Upscale & Upper Midscale": rs["upscale_upper_midscale_rooms"],
            "Midscale & Economy": rs["midscale_economy_rooms"],
        }
    seg["rooms"] = seg["chain_scale"].map(rooms_by_segment) if not seg.empty else None
    seg["room_revenue_usd"] = (
        seg["occupancy_pct"] / 100 * seg["rooms"].fillna(0) * 365 * seg["adr_usd"] if not seg.empty else None
    )

    chart_segments = _dual_axis_bar(
        seg["chain_scale"].tolist(), seg["occupancy_pct"].tolist(), seg["revpar_usd"].tolist()
    ) if not seg.empty else _dual_axis_bar([], [], [])
    segment_list = " · ".join(seg["chain_scale"].tolist()) if not seg.empty else "No CoStar segment data for this period"

    pill = {
        "Luxury & Upper Upscale": "pill-luxury",
        "Upscale & Upper Midscale": "pill-upper",
        "Midscale & Economy": "pill-mid",
    }
    property_rows = ""
    for _, r in seg.iterrows():
        cls = pill.get(r["chain_scale"], "pill-upper")
        rooms_txt = f'{int(r["rooms"]):,}' if pd.notna(r["rooms"]) else "N/A"
        property_rows += (
            f'<tr><td><span class="{cls}">{r["chain_scale"]}</span></td><td class="num">{rooms_txt}</td>'
            f'<td class="num">{r["occupancy_pct"]:.1f}%</td><td class="num">${r["adr_usd"]:,.0f}</td>'
            f'<td class="num strong">${r["revpar_usd"]:,.0f}</td></tr>\n'
        )
    if not property_rows:
        property_rows = '<tr><td colspan="5" style="text-align:center; color:#94A3B8;">No CoStar segment data for this period</td></tr>'

    tax_rows = ""
    total_rev = seg["room_revenue_usd"].sum() if not seg.empty else 0
    for _, r in seg.iterrows():
        rev = r["room_revenue_usd"] or 0
        share = (rev / total_rev * 100) if total_rev else 0
        tbid = rev * 0.0125
        tot = rev * 0.10
        cls = pill.get(r["chain_scale"], "pill-upper")
        tax_rows += (
            f'<tr><td><span class="{cls}">{r["chain_scale"]}</span></td>'
            f'<td class="num">${rev/1e6:,.1f}M</td><td class="num">{share:.1f}%</td>'
            f'<td class="num">${tbid/1e6:,.2f}M</td><td class="num strong">${tot/1e6:,.2f}M</td></tr>\n'
        )
    if not tax_rows:
        tax_rows = '<tr><td colspan="5" style="text-align:center; color:#94A3B8;">No CoStar segment data for this period</td></tr>'

    # Datafy visitor profile -- prefer the freshest export for each metric.
    # The August 2026 "DynamicHomePage" export set covers 2026-01-01 to
    # 2026-07-31 (a year-to-date snapshot, not a full annual pull like the
    # legacy 2025 kpis file), so each field is sourced independently and the
    # period label is derived rather than assumed.
    total_kpi = pd.read_sql_query(
        "SELECT total_trips, avg_los_days, report_period_start, report_period_end "
        "FROM datafy_overview_total_kpis "
        "WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_total_kpis) LIMIT 1", con
    )
    legacy_kpi = pd.read_sql_query(
        "SELECT total_trips, avg_length_of_stay_days, day_trips_pct, overnight_trips_pct, "
        "out_of_state_vd_pct, report_period_start, report_period_end FROM datafy_overview_kpis "
        "ORDER BY report_period_end DESC LIMIT 1", con
    )
    los_dist = pd.read_sql_query(
        "SELECT length_of_stay, pct_of_trips, report_period_start, report_period_end "
        "FROM datafy_overview_los_distribution "
        "WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_los_distribution)", con
    )
    inout_state = pd.read_sql_query(
        "SELECT in_state_pct, out_of_state_pct, report_period_start, report_period_end "
        "FROM datafy_overview_instate_outstate "
        "WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_instate_outstate) LIMIT 1", con
    )

    freshest_period = (None, None)
    if not total_kpi.empty:
        d = total_kpi.iloc[0]
        total_trips = f"{int(d['total_trips']):,}"
        avg_los = f"{d['avg_los_days']:.1f} nights"
        freshest_period = (d["report_period_start"], d["report_period_end"])
    elif not legacy_kpi.empty:
        d = legacy_kpi.iloc[0]
        total_trips = f"{int(d['total_trips']):,}"
        avg_los = f"{d['avg_length_of_stay_days']:.1f} nights"
        freshest_period = (d["report_period_start"], d["report_period_end"])
    else:
        total_trips, avg_los = "N/A", "N/A"

    if not los_dist.empty:
        day_row = los_dist[los_dist["length_of_stay"].str.contains("Day", case=False, na=False)
                            & ~los_dist["length_of_stay"].str.contains("Overnight", case=False, na=False)]
        overnight_row = los_dist[los_dist["length_of_stay"].str.contains("Overnight", case=False, na=False)]
        day_pct = float(day_row["pct_of_trips"].iloc[0]) if not day_row.empty else None
        overnight_pct = float(overnight_row["pct_of_trips"].iloc[0]) if not overnight_row.empty else None
        if day_pct is not None and overnight_pct is not None:
            chart_trip = _donut_chart(["Overnight Trips", "Day Trips"], [overnight_pct, day_pct], [TEAL_DK, TEAL_LT])
        else:
            chart_trip = _donut_chart(["Overnight Trips", "Day Trips"],
                                       [legacy_kpi.iloc[0]["overnight_trips_pct"], legacy_kpi.iloc[0]["day_trips_pct"]],
                                       [TEAL_DK, TEAL_LT]) if not legacy_kpi.empty else _donut_chart(
                                       ["Overnight", "Day"], [60, 40], [TEAL_DK, TEAL_LT])
    elif not legacy_kpi.empty:
        d = legacy_kpi.iloc[0]
        chart_trip = _donut_chart(["Overnight Trips", "Day Trips"], [d["overnight_trips_pct"], d["day_trips_pct"]], [TEAL_DK, TEAL_LT])
    else:
        chart_trip = _donut_chart(["Overnight", "Day"], [60, 40], [TEAL_DK, TEAL_LT])

    if not inout_state.empty:
        oos_pct = f"{float(inout_state.iloc[0]['out_of_state_pct']) * 100:.1f}%"
        if freshest_period == (None, None):
            freshest_period = (inout_state.iloc[0]["report_period_start"], inout_state.iloc[0]["report_period_end"])
    elif not legacy_kpi.empty:
        oos_pct = f"{legacy_kpi.iloc[0]['out_of_state_vd_pct']:.1f}%"
    else:
        oos_pct = "N/A"

    datafy_period = _format_datafy_period(*freshest_period) if freshest_period != (None, None) else "N/A"

    # Category spend share -- prefer the fresh 2026 YTD export (stored as a
    # 0-1 fraction) over the legacy 2025 annual export (stored as 0-100).
    cat_fresh = pd.read_sql_query(
        "SELECT category, spend_share_pct FROM datafy_overview_spending_by_category "
        "WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_spending_by_category) "
        "ORDER BY spend_share_pct DESC LIMIT 6", con
    )
    if not cat_fresh.empty:
        cat_labels = cat_fresh["category"].tolist()[::-1]
        cat_values = (cat_fresh["spend_share_pct"] * 100).tolist()[::-1]
    else:
        cat_legacy = pd.read_sql_query(
            "SELECT category, spend_share_pct FROM datafy_overview_category_spending "
            "ORDER BY spend_share_pct DESC LIMIT 6", con
        )
        cat_labels = cat_legacy["category"].tolist()[::-1]
        cat_values = cat_legacy["spend_share_pct"].tolist()[::-1]
    chart_category = _hbar_chart(cat_labels, cat_values, TEAL, "Spend Share %")

    # visitor origins -- prefer the fresh 2026 YTD trip-share export over the
    # legacy 2025 annual visitor-days-share export.
    top_mkt_fresh = pd.read_sql_query(
        "SELECT dma, trips_share_pct AS share_pct FROM datafy_overview_top_markets "
        "WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_top_markets) "
        "AND trips_share_pct IS NOT NULL ORDER BY trips_share_pct DESC LIMIT 10", con
    )
    if not top_mkt_fresh.empty:
        dma = top_mkt_fresh
    else:
        dma = pd.read_sql_query(
            "SELECT dma, visitor_days_share_pct AS share_pct FROM datafy_overview_dma "
            "WHERE visitor_days_share_pct IS NOT NULL ORDER BY visitor_days_share_pct DESC LIMIT 10", con
        )
    origin_colors = [TEAL_DK, TEAL, "#3F97AC", "#6FB8CB", TEAL_LT, MAROON, "#C97A47", MAROON_LT, "#2A4A54", "#6B5C4E"]
    chart_origins = _pie_chart(dma["dma"].tolist(), dma["share_pct"].tolist(), origin_colors[: len(dma)])
    origins_legend = ""
    for i, (_, r) in enumerate(dma.iterrows()):
        origins_legend += (
            f'<div class="origins-legend-row"><div class="dot" style="background:{origin_colors[i % len(origin_colors)]}">'
            f'</div><div style="flex:1;">{r["dma"]}</div><div style="font-weight:700;">{r["share_pct"]:.1f}%</div></div>\n'
        )

    # Real data callouts for the "Why Visitors Choose Dana Point" photo cards
    repeat_row = pd.read_sql_query(
        "SELECT repeat_pct FROM datafy_overview_repeat_spenders "
        "WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_repeat_spenders) LIMIT 1", con
    )
    stat_repeat = f"{float(repeat_row.iloc[0]['repeat_pct']) * 100:.0f}%" if not repeat_row.empty else "N/A"

    harbor_row = pd.read_sql_query(
        "SELECT visitor_days_share_pct FROM datafy_overview_top_pois "
        "WHERE cluster='Harbor' AND report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_top_pois) LIMIT 1", con
    )
    stat_harbor = f"{float(harbor_row.iloc[0]['visitor_days_share_pct']):.0f}%" if not harbor_row.empty else "N/A"

    stat_los = avg_los.replace(" nights", " nights") if avg_los != "N/A" else "N/A"

    # executive headline: top dmo insight(s) for the front page
    exec_insights = pd.read_sql_query(
        "SELECT headline, body FROM insights_daily WHERE audience='dmo' "
        "AND as_of_date=(SELECT MAX(as_of_date) FROM insights_daily WHERE audience='dmo') "
        "ORDER BY priority LIMIT 3", con
    )
    exec_headline_cards = ""
    for _, r in exec_insights.iterrows():
        tag = r["headline"].split(":")[0].split("→")[0][:44]
        body = r["body"]
        body = body if len(body) < 200 else body[:197] + "..."
        exec_headline_cards += (
            f'<div class="insight-card"><div class="insight-tag">{tag}</div>'
            f'<div class="insight-body">{body}</div></div>\n'
        )
    if not exec_headline_cards:
        exec_headline_cards = '<div class="insight-body">No headline insight available for this period.</div>'

    # cross-dataset insights
    insights = pd.read_sql_query(
        "SELECT headline, body FROM insights_daily WHERE audience='cross' "
        "AND as_of_date=(SELECT MAX(as_of_date) FROM insights_daily WHERE audience='cross') "
        "ORDER BY priority LIMIT 3", con
    )
    insight_cards = ""
    for _, r in insights.iterrows():
        tag = r["headline"].split(":")[0].split("→")[0][:40]
        body = r["body"]
        body = body if len(body) < 220 else body[:217] + "..."
        insight_cards += (
            f'<div class="insight-card"><div class="insight-tag">{tag}</div>'
            f'<div class="insight-body">{body}</div></div>\n'
        )
    if not insight_cards:
        insight_cards = '<div class="insight-body">No cross-dataset insights available for this period.</div>'

    # Team commentary from the live dashboard's Notes layer -- see Page 9.
    notes_cards = _load_notes_for_report()
    if not notes_cards:
        notes_cards = (
            '<div class="insight-body">No commentary has been added in the live dashboard yet. '
            "Notes added there under “General Notes” will appear here on the next report.</div>"
        )

    con.close()

    tokens = {
        "WEEK_LABEL": week_label,
        "GEN_TIMESTAMP": datetime.now().strftime("%b %-d, %Y %-I:%M %p"),
        "OCC_PCT": _fmt_pct(occ_avg),
        "ADR": _fmt_usd(adr_avg),
        "REVPAR": _fmt_usd(revpar_avg),
        "COMPRESSION_DAYS": compression_days,
        "COMPRESSION_LABEL": compression_label,
        "OCC_YOY": _fmt_pct(occ_yoy_avg, sign=True) if occ_yoy_avg is not None else "N/A",
        "ADR_YOY": _fmt_pct(adr_yoy_avg, sign=True) if adr_yoy_avg is not None else "N/A",
        "REVPAR_YOY": _fmt_pct(revpar_yoy_avg, sign=True) if revpar_yoy_avg is not None else "N/A",
        "GROUP_DEMAND_SHARE": GROUP_BENCHMARK["demand_share"],
        "CHART_OCC_DP": chart_occ_dp,
        "CHART_COMPSET": chart_compset,
        "CHART_ADR_DP": chart_adr_dp,
        "CHART_COMPRESSION": chart_compression,
        "CHART_ORIGINS": chart_origins,
        "ORIGINS_LEGEND": origins_legend,
        "DATAFY_PERIOD": datafy_period,
        "COSTAR_SEGMENT_PERIOD": f"{seg_period_text}, {COSTAR_MARKET}",
        "COSTAR_SEGMENT_LIST": segment_list,
        "CHART_SEGMENTS": chart_segments,
        "PROPERTY_ROWS": property_rows,
        "TAX_ROWS": tax_rows,
        "CHART_TRIP": chart_trip,
        "TOTAL_TRIPS": total_trips,
        "AVG_LOS": avg_los,
        "OOS_PCT": oos_pct,
        "CHART_CATEGORY": chart_category,
        "STAT_REPEAT": stat_repeat,
        "STAT_HARBOR": stat_harbor,
        "STAT_LOS": stat_los,
        "LATEST_STR_DATE": latest_date.strftime("%B %-d, %Y"),
        "GROUP_ADR_OCC": GROUP_BENCHMARK["adr_occ"],
        "GROUP_DEMAND_RANGE": GROUP_BENCHMARK["demand_range"],
        "GROUP_REVENUE_RANGE": GROUP_BENCHMARK["revenue_range"],
        "GROUP_TBID_RANGE": GROUP_BENCHMARK["tbid_range"],
        "GROUP_TOT_RANGE": GROUP_BENCHMARK["tot_range"],
        "COMPRESSION_DAYS_ANNUAL": GROUP_BENCHMARK["compression_days_annual"],
        "INSIGHT_CARDS": insight_cards,
        "EXEC_HEADLINE_CARDS": exec_headline_cards,
        "NOTES_CARDS": notes_cards,
        "LOGO_DATA_URI": _logo_data_uri(),
        "LOGO_NAV_DATA_URI": _logo_nav_data_uri(),
        "COVER_PHOTO": _photo_data_uri("drone_aerial.jpg"),
        "COVER_PHOTO_2": _photo_data_uri("hero_coast.jpg"),
        "COVER_PHOTO_3": _photo_data_uri("whale_watching.jpg"),
        "PHOTO_WHALE": _photo_data_uri("whale_watching.jpg"),
        "PHOTO_HARBOR": _photo_data_uri("harbor_coastline.jpg"),
        "PHOTO_WOODY": _photo_data_uri("classic_woody.jpg"),
    }

    html = open(TEMPLATE_PATH, encoding="utf-8").read()
    for k, v in tokens.items():
        html = html.replace(f"[[{k}]]", str(v))

    os.makedirs(LOGS_DIR, exist_ok=True)
    html_path = os.path.join(LOGS_DIR, "weekly_report_latest.html")
    with open(html_path, "w", encoding="utf-8") as fh:
        fh.write(html)

    from weasyprint import HTML
    pdf_path = os.path.join(LOGS_DIR, "weekly_report_latest.pdf")
    HTML(html_path).write_pdf(pdf_path)

    dated_path = os.path.join(LOGS_DIR, f"weekly_report_{latest_date.strftime('%Y-%m-%d')}.pdf")
    with open(pdf_path, "rb") as src, open(dated_path, "wb") as dst:
        dst.write(src.read())

    print(f"Report generated: {pdf_path}")
    print(f"Dated copy: {dated_path}")
    return pdf_path


if __name__ == "__main__":
    build_report()
