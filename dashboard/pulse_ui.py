"""
pulse_ui.py, HTML components and the stylesheet for the Dana Point PULSE board view.

All colors and type come from brand_tokens.py (the Visit Dana Point design
system), exposed once as CSS custom properties on :root, so every class below
names a token instead of repeating a hex value. The page stays light: white
surfaces, borders doing the work, shadows only on the logo badge and the
primary action, Georgia italic as the signature display face.

Every component here is plain HTML handed to st.markdown(unsafe_allow_html=True).
Text that comes from data is escaped before it is placed in markup.
"""

from __future__ import annotations

import html

import pandas as pd

import brand_tokens as bt


def esc(s) -> str:
    return html.escape("" if s is None else str(s))


# ---------------------------------------------------------------------------
# Stylesheet
# ---------------------------------------------------------------------------

def css() -> str:
    v = {
        "ink": bt.INK, "ink1": bt.INK_1, "inkb": bt.INK_BODY, "ink2": bt.INK_2, "ink3": bt.INK_3,
        "ink4": bt.INK_4, "label": bt.GRAY_LABEL, "slate": bt.SLATE, "tealtext": bt.TEAL_TEXT,
        "tealdk": bt.TEAL_DK, "tealdeep": bt.TEAL_DEEP, "teal": bt.TEAL, "teallt": bt.TEAL_LT,
        "amber": bt.AMBER, "maroon": bt.MAROON, "maroonlt": bt.MAROON_LT, "sand": bt.SAND, "green": bt.GREEN,
        "mist": bt.MIST, "mistb": bt.MIST_BORDER, "rule": bt.RULE, "border": bt.BORDER,
        "s1": bt.SURFACE_1, "s2": bt.SURFACE_2, "sky": bt.SKY_ON_DARK,
        "good": bt.STATUS_GOOD, "warn": bt.STATUS_WARNING, "bad": bt.STATUS_CRITICAL,
        "serif": bt.FONT_SERIF, "sans": bt.FONT_SANS,
    }
    return f"""
<style>
:root {{
  --ink:{v['ink']}; --ink-1:{v['ink1']}; --ink-body:{v['inkb']}; --ink-2:{v['ink2']}; --ink-3:{v['ink3']};
  --ink-4:{v['ink4']}; --gray-label:{v['label']}; --slate:{v['slate']}; --teal-text:{v['tealtext']};
  --teal-dk:{v['tealdk']}; --teal-deep:{v['tealdeep']}; --teal:{v['teal']}; --teal-lt:{v['teallt']};
  --amber:{v['amber']}; --maroon:{v['maroon']}; --maroon-lt:{v['maroonlt']}; --sand:{v['sand']}; --green:{v['green']};
  --mist:{v['mist']}; --mist-border:{v['mistb']}; --rule:{v['rule']}; --border:{v['border']};
  --surface-1:{v['s1']}; --surface-2:{v['s2']}; --sky-on-dark:{v['sky']};
  --good:{v['good']}; --warn:{v['warn']}; --bad:{v['bad']};
  --serif:{v['serif']}; --sans:{v['sans']};
  --radius-card:10px; --radius-banner:12px; --radius-hero:14px; --radius-pill:20px;
}}
html, body, .stApp {{ background:#FFFFFF; }}
.stApp, .stMarkdown, .stCaption, p, li {{ font-family:var(--sans); }}
.block-container {{ max-width:min(100%, 2200px) !important; padding-left:clamp(16px, 3vw, 56px) !important; padding-right:clamp(16px, 3vw, 56px) !important;
  padding-top:1.4rem !important; }}
.tabnum, .pk-value, .gl-title, .ev-num {{ font-variant-numeric: tabular-nums; }}

/* Freshness strip */
.pf-strip {{ display:flex; flex-wrap:wrap; gap:8px; margin:2px 0 18px 0; }}
.pf-chip {{ display:inline-flex; align-items:center; gap:7px; background:var(--surface-1);
  border:1px solid var(--border); border-radius:var(--radius-pill); padding:5px 12px 5px 10px;
  font-size:12px; color:var(--ink-2); line-height:1.35; }}
.pf-chip b {{ color:var(--ink); font-weight:700; }}
.pf-dot {{ width:7px; height:7px; border-radius:50%; background:var(--good); flex-shrink:0; }}
.pf-dot.warn {{ background:var(--warn); }}

/* At a glance */
.gl-wrap {{ border:1px solid var(--border); border-radius:var(--radius-banner); padding:22px 24px 20px 24px;
  margin:4px 0 22px 0; background:linear-gradient(180deg, var(--mist) 0%, #FFFFFF 70%); }}
.gl-eyebrow {{ font-size:11px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--teal); }}
.gl-h {{ font-family:var(--serif); font-style:italic; font-weight:700; font-size:26px; color:var(--ink);
  line-height:1.15; margin:4px 0 4px 0; }}
.gl-q {{ font-size:14px; color:var(--slate); margin-bottom:16px; }}
.gl-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(230px, 1fr)); gap:12px; }}
.gl-card {{ display:block; text-decoration:none !important; background:#FFFFFF; border:1px solid var(--border);
  border-top:3px solid var(--teal); border-radius:var(--radius-card); padding:14px 16px 14px 16px;
  transition: border-color .15s ease, background .15s ease; }}
.gl-card:hover {{ border-color:var(--teal); background:var(--surface-1); }}
.gl-card.maroon {{ border-top-color:var(--maroon); }}
.gl-card.teal-dk {{ border-top-color:var(--teal-dk); }}
.gl-card.amber {{ border-top-color:var(--amber); }}
.gl-kicker {{ font-size:10.5px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:var(--gray-label);
  margin-bottom:6px; }}
.gl-title {{ font-size:17px; font-weight:800; color:var(--ink); line-height:1.25; margin-bottom:6px; }}
.gl-body {{ font-size:13px; color:var(--ink-2); line-height:1.5; }}

/* Section header */
.sx-head {{ margin:30px 0 14px 0; padding-top:18px; border-top:1px solid var(--border); }}
.sx-eyebrow {{ font-size:11px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--teal);
  display:flex; align-items:center; gap:8px; }}
.sx-num {{ display:inline-flex; align-items:center; justify-content:center; width:20px; height:20px;
  border-radius:50%; background:var(--teal-dk); color:#FFFFFF; font-size:10.5px; letter-spacing:0; }}
.sx-title {{ font-family:var(--serif); font-style:italic; font-weight:700; font-size:28px; color:var(--ink);
  line-height:1.15; margin:6px 0 8px 0; }}
.sx-q {{ display:flex; gap:10px; align-items:flex-start; font-size:15.5px; font-weight:600; color:var(--ink-2);
  line-height:1.45; }}
.sx-q .qmark {{ flex-shrink:0; font-family:var(--serif); font-style:italic; font-weight:700; color:var(--teal);
  font-size:18px; line-height:1.2; }}

/* KPI tiles */
.pk-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:12px; margin:14px 0 14px 0; }}
.pk {{ background:#FFFFFF; border:1px solid var(--border); border-radius:var(--radius-card); padding:14px 16px 12px 16px;
  position:relative; overflow:hidden; }}
.pk-label {{ font-size:11.5px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; color:var(--gray-label); }}
.pk-value {{ font-size:32px; font-weight:800; color:var(--ink); line-height:1.1; margin:6px 0 6px 0; }}
.pk-delta {{ display:inline-flex; align-items:center; gap:5px; font-size:12.5px; font-weight:700; border-radius:6px;
  padding:2px 8px; }}
.pk-delta.good {{ color:#047857; background:#ECFDF5; }}
.pk-delta.bad {{ color:#B91C1C; background:#FEF2F2; }}
.pk-delta.flat {{ color:var(--ink-3); background:var(--surface-2); }}
.pk-foot {{ font-size:11.5px; color:var(--ink-3); margin-top:6px; line-height:1.4; }}
.pk-spark {{ position:absolute; right:10px; top:12px; opacity:.9; }}

/* Written answers */
.ans {{ background:var(--mist); border:1px solid var(--mist-border); border-left:4px solid var(--teal);
  border-radius:var(--radius-card); padding:14px 18px; margin:6px 0 16px 0; }}
.ans-label {{ font-size:11px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:var(--teal-deep);
  margin-bottom:6px; }}
.ans-body {{ font-size:14.5px; line-height:1.6; color:var(--ink-body); }}

/* Visual blocks */
.vz-q {{ font-size:15px; font-weight:700; color:var(--ink); line-height:1.35; margin:2px 0 2px 0; }}
.vz-src {{ font-size:12px; color:var(--ink-3); margin-bottom:4px; }}
.vz-a {{ font-size:13.5px; line-height:1.55; color:var(--ink-2); margin:2px 0 6px 0; }}
.vz-a b {{ color:var(--ink); }}
details.howto {{ margin:2px 0 4px 0; }}
details.howto summary {{ cursor:pointer; font-size:12px; font-weight:600; color:var(--teal-text); list-style:none; }}
details.howto summary::-webkit-details-marker {{ display:none; }}
details.howto summary:before {{ content:"+ "; font-weight:700; }}
details.howto[open] summary:before {{ content:"– "; }}
details.howto div {{ font-size:12.5px; line-height:1.55; color:var(--ink-3); padding:6px 0 2px 0; }}
div[data-testid="stVerticalBlockBorderWrapper"] {{ border-radius:var(--radius-card) !important; border-color:var(--border) !important; }}

/* RevPAR split bar */
.sp-wrap {{ margin:6px 0 4px 0; }}
.sp-bar {{ display:flex; height:12px; border-radius:6px; overflow:hidden; background:var(--surface-2); }}
.sp-seg {{ height:100%; }}
.sp-leg {{ display:flex; flex-wrap:wrap; gap:14px; font-size:12px; color:var(--ink-2); margin-top:6px; }}
.sp-leg i {{ display:inline-block; width:10px; height:10px; border-radius:3px; margin-right:5px; vertical-align:-1px; }}

/* Events */
.ev-list {{ display:grid; gap:8px; }}
.ev {{ display:grid; grid-template-columns:64px 1fr auto; gap:12px; align-items:center; border:1px solid var(--border);
  border-radius:var(--radius-card); padding:10px 14px; background:#FFFFFF; }}
.ev-date {{ text-align:center; border-right:1px solid var(--border); padding-right:10px; }}
.ev-mon {{ font-size:10.5px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:var(--maroon); }}
.ev-num {{ font-size:22px; font-weight:800; color:var(--ink); line-height:1.05; }}
.ev-name {{ font-size:14px; font-weight:700; color:var(--ink); }}
.ev-meta {{ font-size:12.5px; color:var(--ink-3); margin-top:2px; }}
.ev-pill {{ font-size:11.5px; font-weight:700; color:var(--teal-deep); background:var(--mist); border:1px solid var(--mist-border);
  border-radius:var(--radius-pill); padding:3px 10px; white-space:nowrap; }}
.ev-pill.now {{ color:#FFFFFF; background:var(--maroon); border-color:var(--maroon); }}

/* About this data */
.ab-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(260px, 1fr)); gap:12px; }}
.ab-card {{ border:1px solid var(--border); border-radius:var(--radius-card); padding:12px 14px; background:#FFFFFF; }}
.ab-card h5 {{ margin:0 0 6px 0; font-size:13px; font-weight:700; color:var(--ink); }}
.ab-card p, .ab-card li {{ font-size:12.5px; line-height:1.55; color:var(--ink-2); margin:0; }}
.ab-card ul {{ margin:0; padding-left:16px; }}
.nv-wrap {{ border:1px solid var(--border); border-radius:var(--radius-card); padding:10px; background:var(--surface-1);
  overflow-x:auto; }}
.nv-wrap svg {{ display:block; width:100%; min-width:640px; height:auto; }}

/* Sidebar */
section[data-testid="stSidebar"] {{ background:var(--surface-1); border-right:1px solid var(--border); }}
.sb-brand {{ display:flex; align-items:center; gap:10px; margin:2px 0 14px 0; }}
.sb-badge {{ background:var(--teal-dk); border-radius:10px; width:44px; height:38px; display:flex; align-items:center;
  justify-content:center; box-shadow:0 2px 6px rgba(18,60,74,0.35); }}
.sb-badge img {{ width:28px; }}
.sb-t {{ font-family:var(--serif); font-style:italic; font-weight:700; font-size:18px; color:var(--ink); line-height:1.1; }}
.sb-s {{ font-size:11px; color:var(--ink-3); }}
.sb-h {{ font-size:10.5px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--gray-label);
  margin:16px 0 6px 0; }}
.sb-nav a {{ display:flex; align-items:center; gap:8px; padding:7px 10px; border-radius:8px; font-size:13.5px;
  font-weight:600; color:var(--teal-dk) !important; text-decoration:none !important; }}
.sb-nav a:hover {{ background:var(--mist); color:var(--teal) !important; }}
.sb-nav .n {{ display:inline-flex; width:18px; height:18px; border-radius:50%; background:#FFFFFF; border:1px solid var(--rule);
  align-items:center; justify-content:center; font-size:10px; color:var(--ink-3); }}
.sb-fresh {{ font-size:12px; color:var(--ink-2); line-height:1.5; }}
.sb-fresh div {{ padding:5px 0; border-bottom:1px solid var(--border); }}
.sb-fresh b {{ color:var(--ink); }}

.pz-md {{ padding-bottom:1rem; }}

/* Data window: a segmented control over the same st.radio (same key, same
   values), so the selection logic is untouched. */
.st-key-pulse_period [data-testid="stWidgetLabel"] p {{ font-size:11px; letter-spacing:.14em; text-transform:uppercase;
  font-weight:700; color:var(--ink-3); }}
.st-key-pulse_period [data-testid="stRadioGroup"] {{ display:inline-flex; flex-wrap:wrap; gap:4px; padding:4px;
  background:#F1F5F7; border:1px solid var(--border); border-radius:14px; }}
.st-key-pulse_period [data-testid="stRadioOption"] {{ margin:0 !important; padding:7px 14px; border-radius:10px;
  cursor:pointer; transition:background .15s ease, box-shadow .15s ease; }}
.st-key-pulse_period [data-testid="stRadioOption"] > div > div:first-child {{ display:none !important; }}
.st-key-pulse_period [data-testid="stRadioOption"] p {{ font-size:13px; font-weight:650; color:var(--ink-2); margin:0; }}
.st-key-pulse_period [data-testid="stRadioOption"]:hover {{ background:#FFFFFF; }}
.st-key-pulse_period [data-testid="stRadioOption"]:has(input:checked) {{ background:var(--teal-dk);
  box-shadow:0 6px 14px -8px rgba(18,60,74,.7); }}
.st-key-pulse_period [data-testid="stRadioOption"]:has(input:checked) p {{ color:#FFFFFF; }}

/* Phones: the header buttons sit in one row, and the data window and jump
   links become single swipeable rows instead of stacking four rows deep. */
@media (max-width: 640px) {{
  .st-key-pz_head [data-testid="stHorizontalBlock"] {{ flex-wrap:wrap !important; gap:8px !important; }}
  .st-key-pz_head [data-testid="stColumn"]:first-child {{ flex:1 1 100% !important; width:100% !important; }}
  .st-key-pz_head [data-testid="stColumn"]:not(:first-child) {{ flex:1 1 calc(50% - 8px) !important; min-width:0 !important; width:auto !important; }}
  .st-key-pz_head [data-testid="stColumn"]:not(:first-child) button {{ padding-left:4px !important; padding-right:4px !important; }}
  .st-key-pz_head [data-testid="stColumn"]:not(:first-child) button p {{ font-size:12.5px !important; white-space:nowrap; }}
  .st-key-pulse_period [data-testid="stRadioGroup"] {{ flex-wrap:nowrap; overflow-x:auto; max-width:100%;
    scrollbar-width:none; -webkit-overflow-scrolling:touch; }}
  .st-key-pulse_period [data-testid="stRadioGroup"]::-webkit-scrollbar {{ display:none; }}
  .st-key-pulse_period [data-testid="stRadioOption"] {{ flex:none; padding:7px 12px; }}
  .st-key-pulse_period [data-testid="stRadioOption"] p {{ white-space:nowrap; }}
  .pulse-jumpnav {{ flex-wrap:nowrap !important; overflow-x:auto; scrollbar-width:none; -webkit-overflow-scrolling:touch;
    padding-bottom:4px; }}
  .pulse-jumpnav::-webkit-scrollbar {{ display:none; }}
  .pulse-jumpnav a {{ flex:none; white-space:nowrap; }}
}}

@media (max-width: 640px) {{
  .pk-grid {{ grid-template-columns:repeat(2, minmax(0, 1fr)); gap:10px; }}
  .pk {{ min-width:0; padding:12px 12px 10px 12px; }}
  .pk-delta {{ white-space:normal; }}
  .gl-wrap {{ padding:16px 14px; }}
  .gl-h {{ font-size:21px; }}
  .sx-title {{ font-size:22px; }}
  .sx-q {{ font-size:14.5px; }}
  .pk-value {{ font-size:22px; overflow-wrap:anywhere; }}
  .pk-spark {{ display:none; }}
  .ev {{ grid-template-columns:52px 1fr; }}
  .ev-pill {{ grid-column:2; justify-self:start; }}
  .pf-chip {{ font-size:11.5px; }}
}}
</style>
"""


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------

def sparkline(values, color: str = bt.TEAL, w: int = 84, h: int = 26) -> str:
    vals = [float(v) for v in values if v is not None and not pd.isna(v)]
    if len(vals) < 3:
        return ""
    lo, hi = min(vals), max(vals)
    rng = (hi - lo) or 1.0
    step = w / (len(vals) - 1)
    pts = " ".join(f"{i * step:.1f},{h - 3 - (v - lo) / rng * (h - 6):.1f}" for i, v in enumerate(vals))
    lx, ly = (len(vals) - 1) * step, h - 3 - (vals[-1] - lo) / rng * (h - 6)
    return (f'<svg class="pk-spark" width="{w}" height="{h}" viewBox="0 0 {w} {h}" aria-hidden="true">'
            f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="1.8" stroke-linejoin="round" '
            f'stroke-linecap="round"/><circle cx="{lx:.1f}" cy="{ly:.1f}" r="2.6" fill="{color}"/></svg>')


def delta_chip(text: str | None, value, good_when_up: bool = True, tol: float = 0.05) -> str:
    if not text or text == "n/a" or value is None or pd.isna(value):
        return '<span class="pk-delta flat">No prior-year match</span>'
    if abs(value) <= tol:
        tone, arrow = "flat", "&#8594;"
    else:
        up = value > 0
        tone = "good" if (up == good_when_up) else "bad"
        arrow = "&#8593;" if up else "&#8595;"
    return f'<span class="pk-delta {tone}">{arrow} {esc(text)}</span>'


def kpi_grid(tiles: list[dict]) -> str:
    cells = []
    for t in tiles:
        cells.append(
            f'<div class="pk">{t.get("spark", "")}'
            f'<div class="pk-label">{esc(t["label"])}</div>'
            f'<div class="pk-value">{esc(t["value"])}</div>'
            f'{t.get("delta_html", "")}'
            f'<div class="pk-foot">{esc(t.get("foot", ""))}</div></div>'
        )
    return f'<div class="pk-grid">{"".join(cells)}</div>'


def freshness_strip(items: list[tuple[str, str, bool]]) -> str:
    chips = "".join(
        f'<span class="pf-chip"><span class="pf-dot{"" if ok else " warn"}"></span>{label_html}</span>'
        for label_html, _, ok in items
    )
    return f'<div class="pf-strip">{chips}</div>'


def glance(takeaways: list[dict], window_label: str, span: str) -> str:
    tone = {"hotel": "", "market": "maroon", "visitors": "teal-dk", "calendar": "amber"}
    kick = {"hotel": "Hotels", "market": "Market position", "visitors": "Visitors", "calendar": "Coming up"}
    cards = "".join(
        f'<a class="gl-card {tone.get(t["icon"], "")}" href="#{t["anchor"]}">'
        f'<div class="gl-kicker">{esc(kick.get(t["icon"], ""))}</div>'
        f'<div class="gl-title">{esc(t["title"])}</div>'
        f'<div class="gl-body">{esc(t["body"])}</div></a>'
        for t in takeaways
    )
    return (f'<div class="gl-wrap"><div class="gl-eyebrow">At a glance &middot; {esc(window_label)}</div>'
            f'<div class="gl-h">What the board should know</div>'
            f'<div class="gl-q">Four answers from STR, CoStar, and Datafy for {esc(span)}. '
            f'Select any card to jump to the detail.</div>'
            f'<div class="gl-grid">{cards}</div></div>')


def section_header(anchor: str, number: int, eyebrow: str, title: str, question: str) -> str:
    return (f'<div id="{esc(anchor)}" class="pulse-anchor"></div>'
            f'<div class="sx-head"><div class="sx-eyebrow"><span class="sx-num">{number}</span>{esc(eyebrow)}</div>'
            f'<div class="sx-title">{esc(title)}</div>'
            f'<div class="sx-q"><span class="qmark">Q</span><span>{esc(question)}</span></div></div>')


def answer(text: str, label: str = "What the data says") -> str:
    if not text:
        return ""
    return (f'<div class="ans"><div class="ans-label">{esc(label)}</div>'
            f'<div class="ans-body">{esc(text)}</div></div>')


def viz_head(question: str, source: str = "") -> str:
    return (f'<div class="vz-q">{esc(question)}</div>'
            + (f'<div class="vz-src">{esc(source)}</div>' if source else ""))


def viz_foot(text: str, how: str = "") -> str:
    out = f'<div class="vz-a">{esc(text)}</div>' if text else ""
    if how:
        out += f'<details class="howto"><summary>How to read this</summary><div>{esc(how)}</div></details>'
    return out


def split_bar(occ_pts, adr_pts, total_pct) -> str:
    """The RevPAR change split into its occupancy and rate parts."""
    if occ_pts is None or adr_pts is None or total_pct is None:
        return ""
    a, b = max(float(occ_pts), 0.0), max(float(adr_pts), 0.0)
    tot = a + b
    if tot <= 0:
        return ""
    return (f'<div class="sp-wrap"><div class="sp-bar">'
            f'<div class="sp-seg" style="width:{a / tot * 100:.1f}%; background:var(--teal-lt);"></div>'
            f'<div class="sp-seg" style="width:{b / tot * 100:.1f}%; background:var(--teal);"></div></div>'
            f'<div class="sp-leg"><span><i style="background:var(--teal-lt)"></i>Rooms sold {occ_pts:+.1f} pts</span>'
            f'<span><i style="background:var(--teal)"></i>Rate {adr_pts:+.1f} pts</span>'
            f'<span>RevPAR {total_pct:+.1f}%</span></div></div>')


def mix_bar(parts: list[tuple[str, float, str]]) -> str:
    tot = sum(max(p[1], 0) for p in parts) or 1
    segs = "".join(f'<div class="sp-seg" style="width:{max(v, 0) / tot * 100:.1f}%; background:{c};"></div>'
                   for _, v, c in parts)
    leg = "".join(f'<span><i style="background:{c}"></i>{esc(n)} {max(v, 0) / tot * 100:.0f}%</span>'
                  for n, v, c in parts if v > 0)
    return f'<div class="sp-wrap"><div class="sp-bar" style="height:14px">{segs}</div><div class="sp-leg">{leg}</div></div>'


def event_list(events: pd.DataFrame, days_phrase, pct, usd) -> str:
    if events is None or events.empty:
        return '<div class="vz-a">No events are on the calendar for the next 120 days.</div>'
    rows = []
    for e in events.head(6).itertuples():
        d = pd.Timestamp(e.date)
        meta = (f"Same dates last year: {pct(e.ly_occ, 0)} occupancy at {usd(e.ly_adr)}"
                if pd.notna(getattr(e, "ly_occ", None)) else "No hotel history for these dates last year")
        pill_cls = "ev-pill now" if int(e.days_out) <= 1 else "ev-pill"
        rows.append(
            f'<div class="ev"><div class="ev-date"><div class="ev-mon">{d.strftime("%b")}</div>'
            f'<div class="ev-num">{d.day}</div></div>'
            f'<div><div class="ev-name">{esc(e.event_name)}</div><div class="ev-meta">{esc(meta)}</div></div>'
            f'<span class="{pill_cls}">{esc(days_phrase(int(e.days_out)).capitalize())}</span></div>'
        )
    return f'<div class="ev-list">{"".join(rows)}</div>'


def node_view() -> str:
    """How the three sources connect to each other and to the page. Drawn as
    inline SVG so it stays sharp at any size and needs no library."""
    t, m, s, dk, ink, ink3, border = bt.TEAL, bt.MAROON, bt.SAND, bt.TEAL_DK, bt.INK, bt.INK_3, bt.RULE

    def node(x, y, w, h, fill, title, sub, text_fill="#FFFFFF", sub_fill=None):
        return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}"/>'
                f'<text x="{x + w / 2}" y="{y + h / 2 - 3}" text-anchor="middle" font-size="13" font-weight="700" fill="{text_fill}">{esc(title)}</text>'
                f'<text x="{x + w / 2}" y="{y + h / 2 + 13}" text-anchor="middle" font-size="10.5" fill="{sub_fill or text_fill}" opacity="0.92">{esc(sub)}</text>')

    def edge(x1, y1, x2, y2, label=""):
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        lab = (f'<rect x="{mx - len(label) * 3.1 - 6}" y="{my - 9}" width="{len(label) * 6.2 + 12}" height="16" rx="8" fill="#FFFFFF" stroke="{border}"/>'
               f'<text x="{mx}" y="{my + 3}" text-anchor="middle" font-size="10" fill="{ink3}">{esc(label)}</text>') if label else ""
        return f'<path d="M{x1},{y1} C{x1},{my} {x2},{my} {x2},{y2}" fill="none" stroke="{border}" stroke-width="2"/>' + lab

    svg = [f'<svg viewBox="0 0 960 330" xmlns="http://www.w3.org/2000/svg" font-family="-apple-system, Segoe UI, sans-serif" role="img" '
           f'aria-label="How STR, CoStar, and Datafy connect to each section of the dashboard">']
    # sources
    svg.append(edge(160, 88, 330, 222, "same hotels"))
    svg.append(edge(480, 88, 330, 222, "comp set + market"))
    svg.append(edge(480, 88, 630, 222, "benchmarks"))
    svg.append(edge(800, 88, 630, 222, "visitors + media"))
    svg.append(edge(160, 88, 170, 222, "rooms, rate"))
    svg.append(edge(800, 88, 800, 222, "spend by month"))
    svg.append(node(60, 30, 200, 58, m, "STR", "Daily and weekly hotel reports"))
    svg.append(node(380, 30, 200, 58, dk, "CoStar", "Market reports, segments, daily grid"))
    svg.append(node(700, 30, 200, 58, t, "Datafy", "Visitor spend, origins, campaigns"))
    # sections
    svg.append(node(80, 222, 180, 58, "#FFFFFF", "1 Performance", "Occupancy, ADR, RevPAR", ink, ink3))
    svg.append(node(240, 222, 180, 58, "#FFFFFF", "4 Forward outlook", "Events, compression, group", ink, ink3))
    svg.append(node(540, 222, 180, 58, "#FFFFFF", "3 Market position", "Peers, submarket, O.C., U.S.", ink, ink3))
    svg.append(node(710, 222, 180, 58, "#FFFFFF", "2 Visitors and spend", "Markets, categories, media", ink, ink3))
    svg.append(f'<rect x="80" y="222" width="180" height="58" rx="10" fill="none" stroke="{border}"/>'
               f'<rect x="240" y="222" width="180" height="58" rx="10" fill="none" stroke="{border}"/>'
               f'<rect x="540" y="222" width="180" height="58" rx="10" fill="none" stroke="{border}"/>'
               f'<rect x="710" y="222" width="180" height="58" rx="10" fill="none" stroke="{border}"/>')
    svg.append(f'<text x="480" y="318" text-anchor="middle" font-size="11" fill="{ink3}">Every written answer reads all three sources for the same data window; '
               f'the cross-source links above are where the page compares them.</text>')
    svg.append("</svg>")
    return f'<div class="nv-wrap">{"".join(svg)}</div>'


def sidebar(logo_uri: str, links: list[tuple[str, str]], fresh_rows: list[tuple[str, str]]) -> str:
    nav = "".join(f'<a href="#{esc(a)}"><span class="n">{i}</span>{esc(t)}</a>' for i, (a, t) in enumerate(links, start=1))
    fr = "".join(f"<div><b>{esc(k)}</b><br>{esc(v)}</div>" for k, v in fresh_rows)
    return (f'<div class="sb-brand"><div class="sb-badge"><img src="{logo_uri}" alt="Visit Dana Point"></div>'
            f'<div><div class="sb-t">Dana Point PULSE</div><div class="sb-s">Destination intelligence</div></div></div>'
            f'<div class="sb-h">On this page</div><div class="sb-nav">{nav}</div>'
            f'<div class="sb-h">Data on file</div><div class="sb-fresh">{fr}</div>')
