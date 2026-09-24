"""
brand_tokens.py, single source of truth for Dana Point PULSE color and font
NAMES, as documented in the Visit Dana Point brand design system
(project/tokens.json, synced 2026-09-23 from this same repo at 77cb2e8).

Every value below is copied verbatim from that design system, nothing here
changes what the app looks like. The point is that app.py and
section_visuals.py used to repeat these same hex strings inline, by hand,
dozens of times each (37 repeats of ink alone in app.py). One typo in any
one of those copies is invisible until it renders wrong. Import the name
from here instead of retyping the hex.

Chart series colors are a separate concern and stay in chart_theme.py (the
CATEGORICAL / STATUS palettes there are validated for contrast and color-
vision-deficiency separation as a set; do not reuse loose brand colors like
TEAL or AMBER as a multi-series chart palette, that is the exact bug
chart_theme.py's own docstring describes fixing).
"""

from __future__ import annotations

# ── Ink / text ────────────────────────────────────────────────────────────
INK = "#0B2530"          # headings, KPI values on white (15.9:1)
INK_1 = "#0F172A"        # chart titles, primary report text
INK_BODY = "#1E293B"     # body text inside summary/AI-answer cards
INK_2 = "#334155"        # body text, help items, notes
INK_3 = "#64748B"        # axis labels, legend text, captions (4.8:1)
INK_4 = "#94A3B8"        # disabled/placeholder marks only, never text (2.6:1)
GRAY_LABEL = "#6B7280"   # uppercase metric labels
SLATE = "#475569"        # subheader text (7.6:1)
TEAL_TEXT = "#0E7490"    # accessible teal for text, e.g. active tab labels (5.4:1)

# ── Teal (primary brand) ─────────────────────────────────────────────────
TEAL_DK = "#123C4A"          # logo badge, button border/hover, banner overlay (11.9:1)
TEAL_DEEP = "#0E4B5C"        # pressed button fill
TEAL = "#1D6E86"             # primary brand teal: buttons, eyebrows, links (5.8:1)
TEAL_LT = "#8FC4D6"          # report chart fill/tints, not for text (1.9:1)
TEAL_LT_CHART = "#7FD6C4"    # live-viewer light seafoam series, not for text

# ── Terracotta (counterweight) ───────────────────────────────────────────
AMBER = "#B45309"            # warm accent series (5.0:1)
MAROON = "#A8461F"           # STR / "hotel truth" series (5.9:1)
MAROON_LT = "#FBE4D5"        # light terracotta panel tint
MAROON_LT_CHART = "#E08A54"  # lighter terracotta chart series, not for text (2.7:1)
SAND = "#9C9186"             # warm neutral comparison series (3.1:1)
GREEN = "#1D9E6F"            # live status dot, positive series (3.4:1, a mark not text)

# ── Surfaces ──────────────────────────────────────────────────────────────
MIST = "#F0F7F9"          # tinted surface: summary cards, AI answer box, status pill
MIST_BORDER = "#CBE3EA"   # border on mist surfaces
RULE = "#CBD9DE"          # input borders, dot separators, light chart series
BORDER = "#E2E8F0"        # grid/card borders, table rules, notes box border
SURFACE_1 = "#F8FAFC"     # jump-nav chip background
SURFACE_2 = "#F1F5F9"     # dividers between note rows, subtle fills
PAGE_BG = "#E7EBEF"       # report page background behind white sheets
WHITE = "#FFFFFF"
SKY_ON_DARK = "#A5DDE9"   # eyebrow text on teal-dk banners (8.0:1)

# ── Status (state only, never a chart series) ────────────────────────────
STATUS_GOOD = "#059669"
STATUS_WARNING = "#D97706"
STATUS_SERIOUS = "#EA580C"
STATUS_CRITICAL = "#DC2626"

# ── Overlays ──────────────────────────────────────────────────────────────
HOVER_BG = "#143250F5"    # chart hover label background, rgba(20,50,80,0.96)
SCRIM = "#092028B8"       # photo hero overlay bottom stop, rgba(9,32,40,0.72)

# ── Type ──────────────────────────────────────────────────────────────────
FONT_SERIF = "Georgia, \"DejaVu Serif\", serif"                          # display/cover/page titles
FONT_SANS = "-apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif"  # UI
FONT_REPORT = "\"Helvetica Neue\", Arial, sans-serif"                     # executive report values
FONT_CHART = "Syne, \"DM Sans\", Inter, system-ui, sans-serif"            # chart labels/legends
