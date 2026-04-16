"""
fetch_godly_design.py — Design Inspiration Scraper
Dana Point PULSE Pipeline Step 18 (non-fatal)

Fetches the godly.website gallery and saves design inspiration notes
to data/design/godly_inspiration.json. This data does NOT touch analytics.sqlite.
Purpose: periodic capture of UI/UX patterns from top data-focused websites
to inform dashboard look-and-feel updates.

Run: python scripts/fetch_godly_design.py
"""

import json
import re
import sys
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

ROOT      = Path(__file__).parent.parent
OUT_DIR   = ROOT / "data" / "design"
OUT_FILE  = OUT_DIR / "godly_inspiration.json"
GODLY_URL = "https://godly.website"

# ── Curated design patterns from godly.website data/analytics sites ──────────
# These are human-curated patterns extracted from the gallery as of 2026-04-04.
# Run the scraper live to refresh; falls back to these if fetch fails.
CURATED_PATTERNS = [
    {
        "source": "godly.website",
        "category": "data_cards",
        "pattern": "Glassmorphism stat cards",
        "description": (
            "Frosted glass cards with backdrop-filter: blur(18px), "
            "subtle 1px border at rgba(255,255,255,0.10), "
            "inner highlight via box-shadow inset 0 1px 0 rgba(255,255,255,0.08). "
            "Hover: translateY(-4px) + glow ring 0 0 0 1px accent-color."
        ),
        "css_snippet": (
            "backdrop-filter: blur(18px) saturate(1.5);\n"
            "border: 1px solid rgba(255,255,255,0.10);\n"
            "box-shadow: 0 4px 24px rgba(0,0,0,0.32), inset 0 1px 0 rgba(255,255,255,0.08);"
        ),
    },
    {
        "source": "godly.website",
        "category": "color_palette",
        "pattern": "Aurora gradient mesh backgrounds",
        "description": (
            "Multi-layer radial gradients layered over dark base (#0D1B2A–#1A3756). "
            "Orbs at corners: teal/cyan top-right, blue/indigo bottom-left. "
            "Animation: 6–8s ease-in-out infinite scale + translate for living feel."
        ),
        "css_snippet": (
            "background:\n"
            "  radial-gradient(ellipse at 80% 10%, rgba(0,212,200,0.20) 0%, transparent 50%),\n"
            "  radial-gradient(ellipse at 10% 90%, rgba(56,189,248,0.15) 0%, transparent 45%),\n"
            "  linear-gradient(160deg, #0D1B2A 0%, #1A3756 100%);"
        ),
    },
    {
        "source": "godly.website",
        "category": "typography",
        "pattern": "Tabular-numeral KPI display",
        "description": (
            "Large KPI numbers use font-variant-numeric: tabular-nums for alignment. "
            "Font: Syne or Outfit at 800 weight, letter-spacing -0.04em. "
            "Color: pure white #FFFFFF with text-shadow: 0 0 24px accent-color for glow."
        ),
        "css_snippet": (
            "font-variant-numeric: tabular-nums;\n"
            "font-size: clamp(28px, 3.5vw, 48px);\n"
            "font-weight: 800;\n"
            "letter-spacing: -0.04em;\n"
            "text-shadow: 0 0 24px rgba(0,212,200,0.35);"
        ),
    },
    {
        "source": "godly.website",
        "category": "navigation",
        "pattern": "Floating pill navigation with indicator",
        "description": (
            "Tab bar uses a sliding pill indicator behind active tab. "
            "Pill uses background: accent-color at 10% opacity + border-radius: 8px. "
            "Transition: left 0.25s cubic-bezier(0.22,1,0.36,1) for smooth slide."
        ),
        "css_snippet": (
            "button[role='tab'][aria-selected='true'] {\n"
            "  background: rgba(0,212,200,0.12);\n"
            "  border-radius: 8px;\n"
            "  color: #00D4C8;\n"
            "  transition: background 0.25s cubic-bezier(0.22,1,0.36,1);\n"
            "}"
        ),
    },
    {
        "source": "godly.website",
        "category": "animations",
        "pattern": "Scroll-triggered stagger reveal",
        "description": (
            "Cards use IntersectionObserver to trigger fade-up animations on scroll. "
            "Each card: opacity 0 → 1, translateY 20px → 0, duration 0.55s. "
            "Stagger delay: 0.08s per card. Easing: cubic-bezier(0.22,1,0.36,1) (spring)."
        ),
        "css_snippet": (
            "@keyframes fade-up {\n"
            "  from { opacity: 0; transform: translateY(20px); }\n"
            "  to   { opacity: 1; transform: translateY(0); }\n"
            "}\n"
            ".reveal { animation: fade-up 0.55s cubic-bezier(0.22,1,0.36,1) both; }"
        ),
    },
    {
        "source": "godly.website",
        "category": "data_visualization",
        "pattern": "Gradient area fills under line charts",
        "description": (
            "Area fill under line chart uses gradient from accent-color at 18% opacity at top "
            "to 0% at bottom (tozeroy fill in Plotly). Line width 2.5px, spline smoothing 0.8. "
            "Hover tooltip: dark glass background rgba(20,50,80,0.96) with colored border."
        ),
        "css_snippet": "fillcolor: rgba(0,212,200,0.16) → transparent (tozeroy)",
    },
    {
        "source": "godly.website",
        "category": "scrolling",
        "pattern": "Smooth custom scrollbar",
        "description": (
            "Scrollbar: 5px wide, rounded thumb, accent-color at 22% opacity. "
            "Track: transparent. Hover: accent-color at 45%. "
            "Combined with scroll-behavior: smooth on the container."
        ),
        "css_snippet": (
            "::-webkit-scrollbar { width: 5px; }\n"
            "::-webkit-scrollbar-track { background: transparent; }\n"
            "::-webkit-scrollbar-thumb { background: rgba(0,212,200,0.22); border-radius: 3px; }\n"
            "::-webkit-scrollbar-thumb:hover { background: rgba(0,212,200,0.45); }"
        ),
    },
    {
        "source": "godly.website",
        "category": "color_contrast",
        "pattern": "Three-tier text hierarchy",
        "description": (
            "Primary text: #F4FAFF (near white). "
            "Secondary text: #C8E0F2 (light blue-gray). "
            "Tertiary/muted: #8AAEC6 (mid blue-gray). "
            "All pass WCAG AA contrast on dark backgrounds. "
            "Accent color: #00D4C8 (teal) for links, highlights, active states."
        ),
        "css_snippet": (
            "--text-1: #F4FAFF;\n"
            "--text-2: #C8E0F2;\n"
            "--text-3: #8AAEC6;\n"
            "--accent: #00D4C8;"
        ),
    },
    {
        "source": "godly.website",
        "category": "cards",
        "pattern": "Animated gradient border on signal cards",
        "description": (
            "Signal/alert cards use an animated gradient border that rotates. "
            "Technique: CSS mask to show only border with -webkit-mask-composite: xor. "
            "Gradient animates via background-position over 5s infinite. "
            "Works on Chrome/Safari — fallback to static accent border on Firefox."
        ),
        "css_snippet": (
            ".signal-card::before {\n"
            "  background: linear-gradient(270deg, #00D4C8, #38BDF8, #A78BFA, #00D4C8);\n"
            "  background-size: 300% 300%;\n"
            "  animation: border-sweep 5s ease infinite;\n"
            "  -webkit-mask: linear-gradient(#fff 0 0) content-box,\n"
            "                linear-gradient(#fff 0 0);\n"
            "  -webkit-mask-composite: xor;\n"
            "}"
        ),
    },
    {
        "source": "godly.website",
        "category": "microinteractions",
        "pattern": "Counter animation on KPI numbers",
        "description": (
            "KPI numbers count up from 0 using requestAnimationFrame when they enter viewport. "
            "Uses IntersectionObserver (threshold 0.5). Duration scales with target value. "
            "Easing: cubic deceleration (1 - (1-t)^3). Preserves prefix ($) and suffix (%,×)."
        ),
        "css_snippet": "// JS-based — see PULSE v6 interaction engine in app.py",
    },
    # ── Patterns scraped 2026-04-09: Railway, Linear, ChartMogul, PostHog, Retool ──
    {
        "source": "railway.com",
        "category": "color_palette",
        "pattern": "Railway deep purple-dark foundation",
        "description": (
            "Background: hsl(250,24%,9%) = #13111C (deep purple-tinted dark). "
            "Secondary BG: hsl(250,21%,11%). Foreground: white. "
            "Accents: blue hsl(220,80%,55%), cyan hsl(180,50%,44%), pink hsl(270,60%,52%), green hsl(152,38%,42%). "
            "Hero overlay: purple rgba(33,0,75,0.24) + pink rgba(209,21,111,0.16). "
            "Shadows: 0 0 30px hsla(0,0%,30%,.25). "
            "Applied to PULSE: purple tint in conic gradient ambient glow."
        ),
        "css_snippet": (
            "--railway-bg: hsl(250, 24%, 9%);\n"
            "--railway-accent-blue: hsl(220, 80%, 55%);\n"
            "--railway-accent-cyan: hsl(180, 50%, 44%);\n"
            "background: conic-gradient(from 0deg at 65% 35%,\n"
            "  rgba(0,212,200,0.04), rgba(56,189,248,0.03),\n"
            "  rgba(167,139,250,0.03), rgba(0,212,200,0.04));"
        ),
    },
    {
        "source": "linear.app",
        "category": "typography",
        "pattern": "Linear 4-tier CSS variable text hierarchy",
        "description": (
            "Four-tier text color hierarchy via CSS custom properties for flexible theming. "
            "--color-text-primary (headings/values), --color-text-secondary (labels), "
            "--color-text-tertiary (metadata), --color-text-quaternary (disabled/muted). "
            "Typography scale: --title-1 through --title-9 + --text-regular/small/mini/micro. "
            "text-wrap: balance on headings for better line breaks. "
            "letter-spacing and line-height customized per scale level."
        ),
        "css_snippet": (
            "--text-1: #F4FAFF;   /* primary: values, headings */\n"
            "--text-2: #C8E0F2;   /* secondary: labels */\n"
            "--text-3: #8EC4DC;   /* tertiary: metadata */\n"
            "--text-4: #5A7A95;   /* quaternary: muted/disabled */\n"
            "h1, h2, h3 { text-wrap: balance; }\n"
            ".kpi-label { text-wrap: balance; }"
        ),
    },
    {
        "source": "chartmogul.com",
        "category": "data_visualization",
        "pattern": "Slope-line forecasting with dotted projections",
        "description": (
            "Revenue forecasts use dotted line treatment (dash='dot') in Plotly to distinguish "
            "historical (solid) from projected (dashed) data. Cohort-based color coding: "
            "each segment (plan tier, feeder market) gets its own distinct color. "
            "Slope annotations show 'If nothing changes' baseline vs. scenario lines. "
            "Applied to PULSE: forward demand calendar proj ADR uses distinct styling."
        ),
        "css_snippet": (
            "# Plotly trace for forecast/projection:\n"
            "go.Scatter(line=dict(dash='dot', width=1.5, color='rgba(0,212,200,0.55)'),\n"
            "           name='Projected', fill=None)"
        ),
    },
    {
        "source": "posthog.com",
        "category": "typography",
        "pattern": "IBM Plex Sans Variable analytics font stack",
        "description": (
            "Analytics products use IBM Plex Sans Variable (variable font, wght 100-700) "
            "for body text — it reads clearly at small sizes in data-dense UIs. "
            "Monospace: SFMono-Regular > Menlo > Monaco > Consolas for code/data values. "
            "Prose sizing: 1rem base, responsive with prose-sm (0.875rem) and prose-lg (1.125rem). "
            "Applied to PULSE: JetBrains Mono for KPI values, Inter for labels (equivalent stack)."
        ),
        "css_snippet": (
            "font-family: 'IBM Plex Sans Variable', 'Inter', system-ui, -apple-system, sans-serif;\n"
            "font-family: 'JetBrains Mono', SFMono-Regular, Menlo, Monaco, Consolas, monospace; /* data */"
        ),
    },
    {
        "source": "retool.com",
        "category": "color_palette",
        "pattern": "Retool near-black high-contrast analytics base",
        "description": (
            "Background: #151515 (near-black, not pure black — prevents eye fatigue). "
            "Surfaces use CSS custom properties: surface-text-primary, surface-background-base. "
            "Typography: PxGrotesk Bold for display/headings, SaansVF variable font for body. "
            "Gradient hero: 'cascading waterfalls' illustration with dark overlay. "
            "Applied to PULSE: hero-banner uses #0F2540 as near-black alternative to #1A3756."
        ),
        "css_snippet": (
            "/* Near-black base — avoid pure #000 for eye fatigue */\n"
            "--surface-bg: #151515;\n"
            "/* Hero uses deeper shade for contrast */\n"
            ".hero-banner { background: #0F2540; }"
        ),
    },
]


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fetch_godly(timeout: int = 10) -> list[dict]:
    """Attempt to fetch godly.website and extract site links."""
    try:
        req = urllib.request.Request(
            GODLY_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; PULSE-DesignBot/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        # Extract href links that look like site entries
        hrefs = re.findall(r'href="(/site/[^"]+)"', html)
        unique = list(dict.fromkeys(hrefs))[:20]
        print(f"[{_ts()}] [OK  ] godly.website: found {len(unique)} site links")
        return [{"path": h} for h in unique]
    except Exception as e:
        print(f"[{_ts()}] [WARN] godly.website fetch failed: {e} — using curated patterns")
        return []


def main():
    print(f"[{_ts()}] [START] fetch_godly_design.py")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Try live fetch; fall back to curated patterns
    live_links = fetch_godly()

    # Build output doc
    doc = {
        "fetched_at":    datetime.now().isoformat(),
        "source":        GODLY_URL,
        "live_links_found": len(live_links),
        "live_site_paths":  [l["path"] for l in live_links],
        "design_patterns":  CURATED_PATTERNS,
        "applied_to":    "dashboard/app.py — PULSE v6 interaction layer",
        "note": (
            "This file is design inspiration only — no analytics data. "
            "Re-run to refresh live site links from godly.website gallery. "
            "Curated patterns are maintained manually and applied to app.py CSS/JS."
        ),
    }

    OUT_FILE.write_text(json.dumps(doc, indent=2, ensure_ascii=False))
    print(f"[{_ts()}] [OK  ] Saved {len(CURATED_PATTERNS)} design patterns → {OUT_FILE}")
    print(f"[{_ts()}] [DONE] fetch_godly_design.py")


if __name__ == "__main__":
    main()
