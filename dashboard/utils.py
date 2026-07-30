"""
VDP Dashboard Utilities, Consolidated helpers for KPI formatting, SQL queries, and logging.
GloCon Solutions LLC, Dana Point PULSE
"""

import logging
import pandas as pd
from typing import Optional, Dict, Any

_logger = logging.getLogger("vdp_dashboard")


def format_hero_kpi_card(label: str, value: str, delta: str = "", delta_class: str = "neutral", color: str = "#00D4C8") -> str:
    """
    Unified KPI card formatter for hero metrics (4-column layout on main Overview tab).

    Args:
        label: KPI label (e.g., "RevPAR (30d)")
        value: Main metric value (e.g., "$145")
        delta: Optional delta text (e.g., "+2.5%")
        delta_class: CSS class ("up", "down", "neutral")
        color: Accent border color

    Returns:
        HTML string for the card
    """
    delta_html = ""
    if delta:
        delta_html = f'<div class="hero-metric-delta {delta_class}">{delta}</div>'

    return (
        f'<div class="hero-metric-card">'
        f'<div class="hero-metric-label">{label}</div>'
        f'<div class="hero-metric-value">{value}</div>'
        f'{delta_html}'
        f'</div>'
    )


def format_exec_kpi_banner(label: str, value: str, sub: str = "", color: str = "#00D4C8") -> str:
    """
    Unified KPI box formatter for board executive summary banner (9-box grid).
    Used in sub-tabs and detailed views.

    Args:
        label: KPI label
        value: Main value
        sub: Optional subtext
        color: Accent color

    Returns:
        HTML string for KPI box
    """
    return (
        f'<div style="flex:1;min-width:140px;max-width:220px;padding:14px 18px;'
        f'background:rgba(255,255,255,0.05);'
        f'border-radius:12px;border:1px solid rgba(255,255,255,0.10);'
        f'border-top:3px solid {color};'
        f'box-shadow:0 2px 8px rgba(0,0,0,0.20);">'
        f'<div style="font-size:10px;font-weight:700;letter-spacing:.08em;'
        f'text-transform:uppercase;color:#8AAEC6;margin-bottom:5px;">{label}</div>'
        f'<div style="font-size:22px;font-weight:900;letter-spacing:-.03em;font-family:\'Outfit\',sans-serif;color:#FFFFFF;">{value}</div>'
        + (f'<div style="font-size:11px;font-weight:600;margin-top:4px;color:{color};">{sub}</div>' if sub else '')
        + '</div>'
    )


def format_section_header(icon: str, title: str, subtitle: str = "") -> str:
    """
    Premium section header formatter with consistent styling across all tabs.

    Args:
        icon: Emoji icon (e.g., "📊")
        title: Section title
        subtitle: Optional subtitle/description

    Returns:
        HTML string for section header
    """
    subtitle_html = f'<p style="font-size:13px;color:#8EC4DC;margin:6px 0 0 0;font-weight:500;">{subtitle}</p>' if subtitle else ''

    return f"""<div style="margin-bottom:28px;margin-top:0;">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
    <span style="font-size:24px;">{icon}</span>
    <h2 style="
    font-family:'Outfit',sans-serif;
    font-size:24px;
    font-weight:800;
    letter-spacing:-0.01em;
    color:#F4FAFF;
    margin:0;
    ">{title}</h2>
    </div>
    {subtitle_html}
    </div>"""


def format_metric_card(label: str, value: str, icon: str = "", context: str = "") -> str:
    """
    Consistent metric card for use throughout dashboard tabs.

    Args:
        label: Metric label
        value: Metric value
        icon: Optional icon/emoji
        context: Optional context text

    Returns:
        HTML string for metric card
    """
    icon_html = f'<div style="font-size:22px;margin-bottom:8px;opacity:0.8;">{icon}</div>' if icon else ''
    context_html = f'<div style="font-size:11px;color:#8EC4DC;margin-top:8px;font-weight:500;">{context}</div>' if context else ''

    return f"""<div style="
    background:linear-gradient(135deg,rgba(0,212,200,0.08) 0%,rgba(56,189,248,0.04) 100%);
    border:1px solid rgba(0,212,200,0.25);
    border-top:3px solid #00D4C8;
    border-radius:12px;
    padding:20px;
    box-shadow:0 2px 12px rgba(0,0,0,0.12);
    ">
    {icon_html}
    <div style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:#8EC4DC;margin-bottom:6px;">{label}</div>
    <div style="font-family:'Outfit',sans-serif;font-size:28px;font-weight:900;letter-spacing:-0.03em;color:#F4FAFF;line-height:1;">{value}</div>
    {context_html}
    </div>"""


def safe_sql_query(conn, query: str, params: tuple = ()) -> pd.DataFrame:
    """
    Execute SQL query with error logging (no silent failures).

    Args:
        conn: SQLite connection
        query: SQL query string
        params: Optional query parameters

    Returns:
        DataFrame with results, or empty DataFrame on error
    """
    try:
        result = pd.read_sql_query(query, conn, params=params)
        return result
    except Exception as e:
        _logger.debug(f"SQL query failed: {query[:100]}... Error: {str(e)}")
        return pd.DataFrame()


def combine_social_followers(conn) -> Dict[str, int]:
    """
    Fetch latest social followers from all platforms in one query.
    Replaces 3 separate queries with 1 round-trip.

    Args:
        conn: SQLite connection

    Returns:
        Dict with 'ig', 'fb', 'tk' follower counts
    """
    result = {
        "ig": 0,
        "fb": 0,
        "tk": 0,
    }

    try:
        # IG followers
        ig_row = pd.read_sql_query(
            "SELECT followers FROM later_ig_profile_growth WHERE followers IS NOT NULL ORDER BY data_date DESC LIMIT 1",
            conn
        )
        if not ig_row.empty:
            result["ig"] = int(ig_row.iloc[0, 0] or 0)

        # FB followers
        fb_row = pd.read_sql_query(
            "SELECT page_followers FROM later_fb_profile_growth WHERE page_followers IS NOT NULL ORDER BY data_date DESC LIMIT 1",
            conn
        )
        if not fb_row.empty:
            result["fb"] = int(fb_row.iloc[0, 0] or 0)

        # TK followers
        tk_row = pd.read_sql_query(
            "SELECT followers FROM later_tk_profile_growth WHERE followers IS NOT NULL ORDER BY data_date DESC LIMIT 1",
            conn
        )
        if not tk_row.empty:
            result["tk"] = int(tk_row.iloc[0, 0] or 0)

    except Exception as e:
        _logger.debug(f"Failed to fetch social followers: {str(e)}")

    return result


def safe_execute_with_logging(func, *args, **kwargs) -> Any:
    """
    Execute any function with error logging (no silent failures).

    Args:
        func: Callable to execute
        *args, **kwargs: Arguments to pass to func

    Returns:
        Result of func, or None on error (with logging)
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        _logger.debug(f"Function {func.__name__} failed: {str(e)}")
        return None


def format_metric_delta(value: float, decimals: int = 1, as_percentage: bool = True) -> tuple:
    """
    Format a metric delta and return (formatted_string, css_class).

    Args:
        value: Delta value
        decimals: Number of decimal places
        as_percentage: If True, format as %, else as number

    Returns:
        Tuple of (formatted_string, css_class)
    """
    arrow = "▲" if value >= 0 else "▼"
    formatted = f"{arrow} {abs(value):.{decimals}f}{'%' if as_percentage else ''}"
    css_class = "up" if value >= 0 else "down"
    return formatted, css_class


def format_insight_card(icon: str, title: str, main_value: str, subtitle: str = "", body: str = "", accent_color: str = "#0284C7") -> str:
    """
    Format a styled insight card with metric highlight, supporting text, and visual hierarchy.

    Args:
        icon: Emoji icon (e.g., "💰")
        title: Card title (e.g., "Transient Occupancy Tax")
        main_value: Large metric value (e.g., "$3.6M")
        subtitle: Optional subtext below title (e.g., "90-day trailing")
        body: Supporting context paragraph
        accent_color: Left border color

    Returns:
        HTML string for the insight card
    """
    subtitle_html = f'<div style="font-size:12px;color:#64748B;margin-top:4px;font-weight:500;">{subtitle}</div>' if subtitle else ''
    body_html = f'<p style="font-size:13px;color:#334155;line-height:1.6;margin:16px 0 0 0;">{body}</p>' if body else ''

    return f"""<div style="
    background: linear-gradient(135deg, #FFFFFF 0%, #F8FAFC 100%);
    border: 1px solid #E2E8F0;
    border-left: 4px solid {accent_color};
    border-radius: 12px;
    padding: 20px 24px;
    margin: 16px 0;
    box-shadow: 0 1px 3px rgba(15,23,42,0.12), 0 1px 2px rgba(15,23,42,0.24);
    ">
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 12px;">
    <span style="font-size: 24px;">{icon}</span>
    <div>
    <h3 style="font-family: 'Outfit', sans-serif; font-size: 16px; font-weight: 800; color: #0F172A; margin: 0; letter-spacing: -0.01em;">{title}</h3>
    {subtitle_html}
    </div>
    </div>
    <div style="font-family: 'Outfit', sans-serif; font-size: 2.2rem; font-weight: 900; letter-spacing: -0.03em; color: {accent_color}; margin: 8px 0;">{main_value}</div>
    {body_html}
    </div>"""


def format_action_card(signal: str, title: str, body: str, kpi: str = "", accent_color: str = "#0891B2") -> str:
    """
    Compact recommendation card for click-to-drill explorer panels: a data
    signal chip, a recommended play, supporting body copy, and a KPI-to-watch
    footer. Pair with a category/market picker (e.g. st.pills) so each
    selection swaps in a fresh card built from live data.

    Args:
        signal: short data-driven trigger, e.g. "#2 category at 24.0% of spend"
        title: the recommended play, e.g. "Formalize a Dining Partnership Program"
        body: supporting context paragraph
        kpi: table.column to monitor for this recommendation
        accent_color: left border + signal label color

    Returns:
        HTML string for the action card
    """
    kpi_html = (
        f'<div style="font-size:11px;color:#64748B;font-style:italic;margin-top:9px;">'
        f'📍 KPI to watch: {kpi}</div>' if kpi else ""
    )
    return f"""<div style="
    background:#F8FAFC;border:1px solid #E2E8F0;border-left:3px solid {accent_color};
    border-radius:10px;padding:14px 16px;margin:0 0 10px 0;">
    <div style="font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;
    color:{accent_color};margin-bottom:6px;">📊 {signal}</div>
    <div style="font-family:'Outfit',sans-serif;font-size:14px;font-weight:800;color:#0F172A;
    -webkit-text-fill-color:#0F172A;margin-bottom:5px;line-height:1.3;">{title}</div>
    <div style="font-size:12.5px;color:#334155;line-height:1.55;">{body}</div>
    {kpi_html}
    </div>"""


# ──────────────────────────────────────────────────────────────────────────────
# INFOGRAPHIC STAT BAND, OCFEC-inspired, but sleeker
# A color-coded band that pulls the MAIN IDEAS out of a plain paragraph into
# scannable "key points" + optional stat chips, with a gradient icon tile and an
# optional status pill. Use in place of dense explanatory text boxes.
# ──────────────────────────────────────────────────────────────────────────────

# Named accent palette, each entry: (solid, tint_bg, deep_text)
STAT_BAND_PALETTE = {
    "teal":   ("#0891B2", "#ECFEFF", "#075E6B"),
    "blue":   ("#2563EB", "#EFF6FF", "#1E40AF"),
    "amber":  ("#D97706", "#FFFBEB", "#92400E"),
    "purple": ("#7C3AED", "#F5F3FF", "#5B21B6"),
    "green":  ("#059669", "#ECFDF5", "#065F46"),
    "red":    ("#DC2626", "#FEF2F2", "#991B1B"),
    "slate":  ("#475569", "#F8FAFC", "#1E293B"),
}


def _stat_band_colors(accent: str) -> tuple:
    """Resolve an accent name or hex into (solid, tint_bg, deep_text)."""
    if accent in STAT_BAND_PALETTE:
        return STAT_BAND_PALETTE[accent]
    # Custom hex, derive a very light tint and a dark text shade
    hex_clr = accent.lstrip("#")
    if len(hex_clr) == 6:
        r, g, b = (int(hex_clr[i:i + 2], 16) for i in (0, 2, 4))
        tint = f"rgba({r},{g},{b},0.07)"
        deep = f"rgb({int(r * 0.55)},{int(g * 0.55)},{int(b * 0.55)})"
        return (accent, tint, deep)
    return ("#0891B2", "#ECFEFF", "#075E6B")


def format_stat_band(
    icon: str,
    title: str,
    points,
    *,
    eyebrow: str = "",
    accent: str = "teal",
    stats=None,
    status=None,
) -> str:
    """
    Sleek infographic stat band, OCFEC-style category band, modernized.

    Args:
        icon:    emoji/glyph shown in a gradient tile on the left
        title:   bold band title
        points:  list of "main idea" strings (scannable key points). Inline HTML
                 like <strong> is honored. Pass [] to omit.
        eyebrow: small uppercase category label above the title (optional)
        accent:  palette name (teal|blue|amber|purple|green|red|slate) or #hex
        stats:   optional list of dicts -> stat chips. Each:
                   {"label": str, "value": str, "delta": str?, "good": bool?}
        status:  optional (text, palette_or_hex) -> status pill, top-right

    Returns:
        HTML string. Render with st.markdown(..., unsafe_allow_html=True).
    """
    solid, tint, deep = _stat_band_colors(accent)

    eyebrow_html = (
        f'<div style="font-size:10.5px;font-weight:800;letter-spacing:.12em;'
        f'text-transform:uppercase;color:{solid};margin-bottom:3px;">{eyebrow}</div>'
        if eyebrow else ""
    )

    status_html = ""
    if status:
        s_txt, s_clr = status
        s_solid, _, _ = _stat_band_colors(s_clr)
        status_html = (
            f'<span style="background:{s_solid};color:#FFFFFF;font-size:10px;'
            f'font-weight:800;letter-spacing:.08em;text-transform:uppercase;'
            f'padding:4px 11px;border-radius:20px;white-space:nowrap;">{s_txt}</span>'
        )

    # Key points, each with an accent marker; main ideas, not a wall of text
    points_html = ""
    if points:
        items = "".join(
            f'<li style="position:relative;padding-left:20px;margin:0 0 7px 0;'
            f'font-size:13px;line-height:1.6;color:#334155;-webkit-text-fill-color:#334155;">'
            f'<span style="position:absolute;left:0;top:7px;width:7px;height:7px;'
            f'border-radius:2px;background:{solid};transform:rotate(45deg);"></span>'
            f'{p}</li>'
            for p in points
        )
        points_html = (
            f'<ul style="list-style:none;padding:0;margin:12px 0 0 0;">{items}</ul>'
        )

    # Stat chips, the hard numbers, OCFEC-style values, as mini cards
    stats_html = ""
    if stats:
        chips = []
        for s in stats:
            # Accept either a dict or a (label, value[, delta[, good]]) tuple/list
            if isinstance(s, (tuple, list)):
                label = s[0] if len(s) > 0 else ""
                value = s[1] if len(s) > 1 else ""
                delta = s[2] if len(s) > 2 else ""
                good = s[3] if len(s) > 3 else True
            else:
                label = s.get("label", "")
                value = s.get("value", "")
                delta = s.get("delta", "")
                good = s.get("good", True)
            d_clr = "#15803D" if good else "#DC2626"
            arrow = ""
            if delta:
                arrow = (
                    f'<div style="font-size:11px;font-weight:700;color:{d_clr};'
                    f'margin-top:2px;">{delta}</div>'
                )
            chips.append(
                f'<div style="flex:1;min-width:118px;background:#FFFFFF;'
                f'border:1px solid #E2E8F0;border-top:3px solid {solid};'
                f'border-radius:9px;padding:11px 13px;">'
                f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
                f'letter-spacing:.07em;color:#64748B;margin-bottom:3px;">{label}</div>'
                f'<div style="font-family:\'Outfit\',sans-serif;font-size:22px;'
                f'font-weight:900;letter-spacing:-0.02em;color:#0F172A;'
                f'-webkit-text-fill-color:#0F172A;line-height:1.05;">{value}</div>'
                f'{arrow}</div>'
            )
        stats_html = (
            f'<div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:14px;">'
            f'{"".join(chips)}</div>'
        )

    return f"""<div style="
    background:linear-gradient(180deg,{tint} 0%,#FFFFFF 62%);
    border:1px solid #E2E8F0; border-left:5px solid {solid};
    border-radius:13px; padding:16px 20px 18px 18px; margin:16px 0;
    box-shadow:0 1px 3px rgba(15,23,42,0.06),0 6px 18px rgba(15,23,42,0.05);">
    <div style="display:flex;align-items:flex-start;gap:14px;">
      <div style="flex-shrink:0;width:46px;height:46px;border-radius:12px;
        background:linear-gradient(135deg,{solid} 0%,{deep} 100%);
        display:flex;align-items:center;justify-content:center;font-size:23px;
        box-shadow:0 4px 12px {solid}40;">{icon}</div>
      <div style="flex:1;min-width:0;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;">
          <div>
            {eyebrow_html}
            <div style="font-family:'Outfit',sans-serif;font-size:16px;font-weight:800;
              color:#0F172A;-webkit-text-fill-color:#0F172A;letter-spacing:-0.01em;
              line-height:1.25;">{title}</div>
          </div>
          {status_html}
        </div>
      </div>
    </div>
    {points_html}
    {stats_html}
    </div>"""



# ──────────────────────────────────────────────────────────────────────────────
# OCFEC-style benchmark band, value-vs-benchmark bars grouped by category
# ──────────────────────────────────────────────────────────────────────────────
def format_benchmark_band(category: str, icon: str, color: str, rows, *,
                          benchmark_label: str = "BENCHMARK") -> str:
    """
    OCFEC-style horizontal benchmark bars for one category band.

    Args:
        category: band label shown vertically on the left (e.g. "Activity Volume")
        icon:     emoji/glyph for the band
        color:    accent hex for the band + bars
        rows:     list of (label, ratio, display) where ratio is value/benchmark
                  (1.0 = at benchmark) and display is the printed value string.
        benchmark_label: right-edge label for the 1.0 reference line.

    Returns:
        HTML string. Render with st.markdown(..., unsafe_allow_html=True).
    """
    if not rows:
        return ""
    _max_ratio = max((abs(r[1]) for r in rows), default=1.0) or 1.0
    _scale = 92.0 / max(_max_ratio, 1.0)
    bar_rows = ""
    for label, ratio, display in rows:
        pct = max(2.0, min(100.0, abs(ratio) * _scale))
        bar_rows += (
            f'<div style="display:flex;align-items:center;gap:10px;margin:6px 0;">'
            f'<div style="flex:0 0 190px;text-align:right;font-size:12.5px;'
            f'font-weight:600;color:#475569;">{label}:</div>'
            f'<div style="flex:1;position:relative;height:22px;">'
            f'<div style="position:absolute;left:0;top:0;height:100%;width:{pct}%;'
            f'background:{color};border-radius:2px;display:flex;align-items:center;'
            f'justify-content:flex-end;padding-right:8px;">'
            f'<span style="font-size:11.5px;font-weight:800;color:#FFFFFF;'
            f'-webkit-text-fill-color:#FFFFFF;">{display}</span></div>'
            f'<div style="position:absolute;left:94%;top:-3px;height:28px;'
            f'width:2px;background:#0F172A;"></div>'
            f'</div></div>'
        )
    return (
        f'<div style="display:flex;border:2px solid {color};border-radius:10px;'
        f'overflow:hidden;margin:10px 0;background:#FFFFFF;">'
        f'<div style="flex:0 0 54px;background:{color}14;display:flex;'
        f'flex-direction:column;align-items:center;justify-content:center;gap:8px;'
        f'padding:12px 4px;">'
        f'<div style="font-size:22px;">{icon}</div>'
        f'<div style="writing-mode:vertical-rl;transform:rotate(180deg);'
        f'font-size:12px;font-weight:800;letter-spacing:.04em;color:{color};'
        f'text-transform:uppercase;">{category}</div></div>'
        f'<div style="flex:1;padding:12px 16px 12px 8px;">{bar_rows}'
        f'<div style="text-align:right;font-size:11px;font-weight:800;'
        f'letter-spacing:.06em;color:#0F172A;margin-top:4px;">{benchmark_label}</div>'
        f'</div></div>'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Auto chart analysis, derive an infographic stat-band from a labeled series
# ──────────────────────────────────────────────────────────────────────────────
def _fmt_val(v, unit: str = "") -> str:
    """Compact value formatter honoring a unit hint ($, %, or count)."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return str(v)
    if unit == "%":
        return f"{v:.1f}%"
    if unit == "$":
        if abs(v) >= 1e6:
            return f"${v/1e6:.1f}M"
        if abs(v) >= 1e3:
            return f"${v/1e3:.0f}K"
        return f"${v:,.0f}"
    if abs(v) >= 1e6:
        return f"{v/1e6:.2f}M"
    if abs(v) >= 1e3:
        return f"{v:,.0f}"
    return f"{v:,.0f}" if v == int(v) else f"{v:,.1f}"


def auto_distribution_analysis(labels, values, *, title: str, noun: str = "share",
                               unit: str = "%", eyebrow: str = "What this shows",
                               accent: str = "teal", icon: str = "📊",
                               extra_points=None) -> str:
    """
    Build an infographic analysis band automatically from a labeled distribution
    (feeder markets, spend categories, channels, etc.). Computes the leader, the
    top-3 concentration, and the tail, then renders via format_stat_band.

    Returns an HTML string (empty if no usable data).
    """
    pairs = []
    for l, v in zip(labels, values):
        try:
            if v is None or (isinstance(v, float) and v != v):  # NaN
                continue
            pairs.append((str(l), float(v)))
        except (TypeError, ValueError):
            continue
    if not pairs:
        return ""
    pairs.sort(key=lambda x: x[1], reverse=True)
    total = sum(v for _, v in pairs) or 1.0
    top_l, top_v = pairs[0]
    top3 = pairs[:3]
    top3_sum = sum(v for _, v in top3)
    top3_names = ", ".join(l for l, _ in top3)
    top_share = top_v / total * 100
    top3_share = top3_sum / total * 100
    n = len(pairs)

    points = [
        f"<strong>{top_l}</strong> leads at {_fmt_val(top_v, unit)} "
        f"({top_share:.0f}% of total) {noun}.",
        f"The top 3 ({top3_names}) account for <strong>{top3_share:.0f}%</strong> "
        f"of all {n} tracked entries, "
        + ("a concentrated mix." if top3_share >= 60 else "a relatively even spread."),
    ]
    if n > 3:
        tail_l, tail_v = pairs[-1]
        points.append(
            f"The long tail bottoms out at <strong>{tail_l}</strong> "
            f"({_fmt_val(tail_v, unit)}), a growth or pruning candidate."
        )
    if extra_points:
        points.extend(extra_points)

    stats = [
        ("Leader", _fmt_val(top_v, unit)),
        ("Top-3 share", f"{top3_share:.0f}%"),
        ("Entries", f"{n}"),
    ]
    return format_stat_band(
        icon, title, points,
        eyebrow=eyebrow, accent=accent, stats=stats,
        status=(f"{top_l} #1", accent),
    )
