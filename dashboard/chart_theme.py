"""
chart_theme.py, single source of truth for Dana Point PULSE chart styling.

Every chart in the dashboard renders on the app's WHITE page surface, so the
theme is built for a light background: dark text, recessive grid, and a
categorical palette whose colors all clear 3:1 contrast against white.

Before this module existed the dashboard carried three separate themes:
app.py's style_fig, components_group.py's _dark_fig, and a second copy of
style_fig in components_coastal.py. They disagreed on fonts, sizes, and
color, and the Group tab's palette failed contrast badly on white (its gold
sat at 1.72:1 and its seafoam at 1.48:1, both far under the 3:1 floor, which
is what made those charts look washed out). Import from here instead of
adding a fourth theme.

── Palette provenance ───────────────────────────────────────────────────────
CATEGORICAL is validated, not eyeballed. Against the light surface it passes
the lightness band, the chroma floor, adjacent-pair CVD separation (worst
adjacent pair #EA580C/#059669 at deltaE 10.1 protan), the normal-vision floor
(worst adjacent 27.2, well over the 15 hard floor), and 3:1 contrast on all
eight slots.

Two rules keep it valid, so do not reorder or extend casually:
  1. Order is fixed. The checks are run on ADJACENT pairs, so swapping slots
     can silently break CVD separation. The previous order paired coral red
     next to sunset orange at normal-vision deltaE 8.7, under the floor of 15,
     which meant full-color readers could not reliably tell those two series
     apart. Red was pulled out of the categorical set entirely and is now
     reserved for status only.
  2. There is no ninth color. A ninth series folds into "Other", becomes a
     small multiple, or gets a second encoding. Never generate a new hue.

STATUS colors are reserved for state (good / warning / serious / critical) and
are never reused as a series color. Always pair them with a label or icon so
meaning never rides on color alone.
"""

from __future__ import annotations

import plotly.graph_objects as go

# ── Validated categorical palette (fixed order, see provenance above) ────────
CATEGORICAL: list[str] = [
    "#0891B2",  # 1 Pacific teal (primary)
    "#D97706",  # 2 coastal amber
    "#7C3AED",  # 3 violet
    "#059669",  # 4 wave green
    "#EA580C",  # 5 sunset orange
    "#0369A1",  # 6 ocean blue
    "#65A30D",  # 7 olive
    "#DB2777",  # 8 rose
]

# Back-compat alias, some modules refer to the palette as the colorway.
COLORWAY = CATEGORICAL

# ── Status palette, reserved (never a series color) ──────────────────────────
STATUS = {
    "good":     "#059669",
    "warning":  "#D97706",
    "serious":  "#EA580C",
    "critical": "#DC2626",
}

# ── Ink and surface tokens ───────────────────────────────────────────────────
# Text always wears an ink token, never a series color.
INK_PRIMARY   = "#0F172A"   # chart titles
INK_BODY      = "#334155"   # body text
INK_MUTED     = "#64748B"   # axis labels, legend text
GRID          = "rgba(15,23,42,0.07)"
AXIS_LINE     = "rgba(15,23,42,0.14)"
SURFACE       = "#FFFFFF"

FONT_BODY  = "Syne, DM Sans, Inter, system-ui, sans-serif"
FONT_TITLE = "Syne, Outfit, DM Sans, system-ui, sans-serif"
FONT_MONO  = "'JetBrains Mono', Syne, DM Sans, Inter, system-ui, sans-serif"

# Low-alpha area fills derived from the categorical palette, same fixed order.
FILL_PALETTE: list[str] = [
    "rgba(8,145,178,0.14)",   # teal
    "rgba(217,119,6,0.12)",   # amber
    "rgba(124,58,237,0.11)",  # violet
    "rgba(5,150,105,0.12)",   # green
    "rgba(234,88,12,0.12)",   # orange
    "rgba(3,105,161,0.12)",   # blue
]


DEFAULT_HEIGHT = 360


def style_fig(fig: go.Figure, height: int | None = None) -> go.Figure:
    """Apply the PULSE chart theme to a Plotly figure, in place, and return it.

    Safe to call on any figure type. Traces that already set an explicit color
    keep it, so a chart with deliberate semantic colors (a red risk bar, a
    diverging scale) is not overwritten.

    `height` omitted means "keep whatever height this figure already set," and
    only figures with no height of their own fall back to DEFAULT_HEIGHT. That
    matters because this is applied at render time to charts that sized
    themselves deliberately (gauges, tall heatmaps); a hard 360 default would
    silently resize them.
    """
    if height is None:
        try:
            height = int(fig.layout.height) if fig.layout.height else DEFAULT_HEIGHT
        except (TypeError, ValueError):
            height = DEFAULT_HEIGHT
    fig.update_layout(
        plot_bgcolor  = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)",
        font    = dict(family=FONT_BODY, size=12.5, color=INK_BODY),
        height  = height,
        autosize = True,
        margin  = dict(l=14, r=20, t=64, b=36, autoexpand=True),
        transition = {"duration": 500, "easing": "cubic-in-out"},
        legend = dict(
            orientation = "h",
            yanchor = "bottom", y = 1.02,
            xanchor = "left",   x = 0,
            font    = dict(size=11, family=FONT_BODY, color=INK_MUTED),
            bgcolor = "rgba(255,255,255,0.80)",
            bordercolor = "rgba(100,116,139,0.20)",
            borderwidth = 1,
            itemsizing  = "constant",
            tracegroupgap = 4,
        ),
        uniformtext = dict(minsize=9, mode="hide"),
        hoverlabel = dict(
            bgcolor     = "rgba(20,50,80,0.96)",
            bordercolor = "rgba(8,145,178,0.60)",
            font        = dict(size=13, family=FONT_BODY, color="#EFF6FF"),
            namelength  = -1,
            align       = "left",
        ),
        colorway = CATEGORICAL,
        modebar = dict(
            bgcolor     = "rgba(0,0,0,0)",
            color       = "#5A7A95",
            activecolor = CATEGORICAL[0],
        ),
        dragmode = "zoom",
    )
    # Recessive but VISIBLE grid and axes. Values here must stay dark-on-light,
    # an earlier version used rgba(255,255,255,...) on the white page, so charts
    # rendered with no axis line and no gridlines at all.
    fig.update_xaxes(
        showgrid    = False,
        zeroline    = False,
        tickfont    = dict(size=12, family=FONT_BODY, color=INK_MUTED),
        linecolor   = AXIS_LINE,
        linewidth   = 1,
        showline    = True,
        ticks       = "outside",
        ticklen     = 3,
        tickcolor   = "rgba(15,23,42,0.12)",
        automargin  = True,
        tickangle   = -30,
    )
    fig.update_yaxes(
        gridcolor   = GRID,
        gridwidth   = 1,
        griddash    = "dot",
        zeroline    = False,
        tickfont    = dict(size=12, family=FONT_MONO, color=INK_MUTED),
        showline    = False,
        ticks       = "",
        automargin  = True,
    )

    _fill_idx = 0
    for trace in fig.data:
        ttype = type(trace).__name__
        if ttype in ("Scatter", "Scattergl"):
            mode = str(getattr(trace, "mode", "") or "")
            if "lines" in mode and getattr(trace, "fill", None) in (None, "none"):
                trace.fill = "tozeroy"
                _c = (getattr(trace.line, "color", None) or CATEGORICAL[0])
                if "rgb(" in str(_c):
                    trace.fillcolor = _c.replace("rgb(", "rgba(").replace(")", ",0.13)")
                else:
                    trace.fillcolor = FILL_PALETTE[_fill_idx % len(FILL_PALETTE)]
                    if not getattr(trace.line, "color", None):
                        try:
                            trace.line.color = CATEGORICAL[_fill_idx % len(CATEGORICAL)]
                        except Exception:
                            pass
                    _fill_idx += 1
            if hasattr(trace, "line") and trace.line is not None:
                if not getattr(trace.line, "shape", None):
                    try:
                        trace.line.shape = "spline"
                        trace.line.smoothing = 0.8
                    except Exception:
                        pass
                try:
                    if (getattr(trace.line, "width", None) is None or
                            getattr(trace.line, "width", 2) < 2):
                        trace.line.width = 2.5
                except Exception:
                    pass
        elif ttype == "Bar":
            try:
                if not getattr(trace.marker, "cornerradius", None):
                    trace.marker.cornerradius = 5
            except Exception:
                pass
            try:
                if getattr(trace.marker, "opacity", None) is None:
                    trace.marker.opacity = 0.88
            except Exception:
                pass

    # ── Title band separated from the legend band ────────────────────────────
    # The title and the horizontal legend (y=1.02) otherwise share the same 64px
    # top margin, which makes chart headers collide with legend rows. With a
    # title present, pin it to the very top and widen the top margin so title
    # and legend each get their own band.
    _has_title  = bool(fig.layout.title and fig.layout.title.text)
    _has_legend = sum(1 for t in fig.data if getattr(t, "showlegend", True) is not False) >= 2
    if _has_title:
        fig.update_layout(
            title_font = dict(family=FONT_TITLE, size=14, color=INK_BODY),
            title_x    = 0,
            title_y    = 0.99,
            title_yanchor = "top",
            title_pad  = dict(l=4, t=2),
            margin     = dict(t=96 if _has_legend else 72),
        )
    return fig


def base_fig(height: int = 320) -> go.Figure:
    """An empty figure that already carries the theme, for build-then-add code."""
    return style_fig(go.Figure(), height=height)


# ── Visit Dana Point brand layer (added 2026-09-25) ──────────────────────────
# The board view (pulse_charts.py) draws every chart with brand_fig(). It lives
# in this module on purpose: CLAUDE.md asks for one chart theme, so this is a
# second function in the same theme, not a fourth theme. It differs from
# style_fig() above in three deliberate ways, all from the Visit Dana Point
# design system and the dataviz method:
#   * Brand tokens (brand_tokens.py) for ink, grid, and the hover label, and
#     the system sans stack the rest of the page uses.
#   * No spline smoothing and no automatic area fills: a line is the data,
#     not a curve fitted through it.
#   * Horizontal tick labels, a single recessive y grid, no in-chart title
#     (the question above each chart is its title).
try:
    import brand_tokens as _bt
except Exception:  # pragma: no cover
    _bt = None


def brand_fig(fig: go.Figure, height: int = 300, legend: bool = True, hover: str = "closest",
              y_suffix: str | None = None, y_prefix: str | None = None, x_grid: bool = False) -> go.Figure:
    ink2 = getattr(_bt, "INK_2", INK_BODY)
    ink3 = getattr(_bt, "INK_3", INK_MUTED)
    border = getattr(_bt, "BORDER", "#E2E8F0")
    font = getattr(_bt, "FONT_SANS", FONT_BODY)
    fig.update_layout(
        height=height,
        autosize=True,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=font, size=12.5, color=ink2),
        margin=dict(l=6, r=10, t=34 if legend else 10, b=6, autoexpand=True),
        showlegend=legend,
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0,
                    font=dict(size=11.5, color=ink3), bgcolor="rgba(0,0,0,0)",
                    itemsizing="constant", traceorder="normal"),
        hovermode=hover,
        hoverlabel=dict(bgcolor="rgba(20,50,80,0.96)", bordercolor="rgba(20,50,80,0.96)",
                        font=dict(size=12.5, family=font, color="#FFFFFF"), align="left"),
        bargap=0.32,
        bargroupgap=0.12,
        uniformtext=dict(minsize=10, mode="hide"),
    )
    fig.update_xaxes(showgrid=x_grid, gridcolor=border, zeroline=False, showline=True,
                     linecolor=border, ticks="", tickfont=dict(size=11.5, color=ink3),
                     automargin=True, tickangle=0, title=None)
    fig.update_yaxes(showgrid=True, gridcolor=border, gridwidth=1, zeroline=False, showline=False,
                     ticks="", tickfont=dict(size=11.5, color=ink3), automargin=True, title=None,
                     ticksuffix=y_suffix or "", tickprefix=y_prefix or "")
    for tr in fig.data:
        if type(tr).__name__ == "Bar":
            try:
                if not getattr(tr.marker, "cornerradius", None):
                    tr.marker.cornerradius = 4
            except Exception:
                pass
    return fig
