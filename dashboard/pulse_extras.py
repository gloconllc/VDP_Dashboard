"""Make something new: companion pieces built from the Insights report.

Added 2026-10-15. Purely additive. Each builder takes the report dict that
pulse_insights.build_report returns for the window the reader picked, so every
piece follows that selection. Nothing here computes a number: figures, labels
and sources are read from the report as it is. Pieces:

- build_docx: a briefing document (Word), news-report order.
- build_dashboard_html: a one-page dashboard (self-contained HTML).
- build_highlights_html: an animated highlights page (self-contained HTML,
  about 20 seconds, replays, still under reduced motion).

The slide deck is pulse_insights.build_pptx, unchanged.
"""
from __future__ import annotations

import html
import io
import json

TEAL, TEAL_DK, INK, INK2, INK3, MAROON, SAND, MIST, MIST_B = (
    "#1D6E86", "#123C4A", "#0B2530", "#1F3A45", "#55707A", "#A8461F", "#9C9186", "#F0F7F9", "#CBE3EA")

OPTIONS = {
    "docx": "Briefing document (Word)",
    "pptx": "Slide deck (PowerPoint)",
    "dashboard": "One-page dashboard (HTML)",
    "highlights": "Animated highlights (HTML)",
}


def _e(s) -> str:
    return html.escape("" if s is None else str(s))


def _sections(report: dict) -> dict:
    return {s["id"]: s for s in report.get("sections", [])}


def _glance(sec: dict | None, kind: str) -> dict | None:
    return next((g for g in (sec or {}).get("glance", []) if g.get("type") == kind), None)


def _usd_m(v: float) -> str:
    a = abs(v)
    return "$" + (f"{v / 1e9:.2f}B" if a >= 1e9 else f"{v / 1e6:.1f}M" if a >= 1e6 else f"{round(v / 1e3)}K" if a >= 1e3 else f"{round(v)}")


# ---------------------------------------------------------------------------
# Briefing document (Word)
# ---------------------------------------------------------------------------

def build_docx(report: dict) -> bytes | None:
    try:
        from docx import Document
        from docx.shared import Pt, RGBColor
    except Exception:  # noqa: BLE001
        return None
    s = report.get("summary") or {}
    secs = report.get("sections", [])
    doc = Document()
    st = doc.styles["Normal"]
    st.font.name = "Georgia"
    st.font.size = Pt(11)

    def para(text, size=11, bold=False, italic=False, color=INK):
        p = doc.add_paragraph()
        r = p.add_run(text)
        r.font.size, r.bold, r.italic = Pt(size), bold, italic
        r.font.color.rgb = RGBColor.from_string(color.lstrip("#"))
        return p

    para(f"{report.get('title', 'Dana Point PULSE Insights')} · {report.get('period', '')}", 9, True, color=TEAL)
    para(s.get("headline", ""), 22, True, True)
    if s.get("sub"):
        para(s["sub"], 13, color=INK2)
    para(f"Prepared {report.get('prepared', '')} · {report.get('period_detail', '')}", 9, color=INK3)

    doc.add_heading("PULSE Summary", level=1)
    for ptxt in s.get("paras", []):
        para(ptxt)
    stats = s.get("stats") or []
    if stats:
        t = doc.add_table(rows=1, cols=3)
        t.style = "Light Grid Accent 1"
        for i, h in enumerate(("Figure", "What it measures", "Source")):
            t.rows[0].cells[i].text = h
        for x in stats:
            row = t.add_row().cells
            row[0].text, row[1].text, row[2].text = x.get("v", ""), x.get("l", ""), x.get("src", "")

    doc.add_heading("Key takeaways", level=1)
    for tk in report.get("takeaways", []):
        para(f"{tk.get('tab')}: {tk.get('v')} {tk.get('l')}. {tk.get('head')}. Source: {tk.get('src')}.", 10.5)

    for sec in secs:
        doc.add_heading(sec.get("tab", ""), level=1)
        para(sec.get("headline", ""), 15, True, True)
        if sec.get("lede"):
            para(sec["lede"])
        if sec.get("lede_src"):
            para(sec["lede_src"], 9, color=INK3)
        if sec.get("why"):
            para("Why it matters: " + sec["why"], 10.5, color=INK2)
        nums = sec.get("numbers") or []
        if nums:
            t = doc.add_table(rows=1, cols=4)
            t.style = "Light Grid Accent 1"
            for i, h in enumerate(("Figure", "Measure", "Change or context", "Source")):
                t.rows[0].cells[i].text = h
            for n in nums:
                row = t.add_row().cells
                row[0].text, row[1].text, row[2].text, row[3].text = n.get("v", ""), n.get("l", ""), n.get("p", ""), n.get("src", "")

    doc.add_heading("Sources and contact", level=1)
    para(f"Every figure names its source above. {report.get('period_detail', '')}.", 10)
    para("Media contact: John Picou, GloCon Solutions LLC, john.picou@gloconsolutions.com", 10)
    para("Prepared by GloCon Solutions LLC for Visit Dana Point.", 10, color=INK3)
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Shared page shell
# ---------------------------------------------------------------------------

_BASE_CSS = f"""
*{{box-sizing:border-box}}
body{{margin:0;background:#FBFCFC;color:{INK};font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;line-height:1.5}}
.wrap{{max-width:1180px;margin:0 auto;padding:28px 20px 40px}}
.eye{{font-size:12.5px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:{TEAL}}}
.hed{{font-family:Georgia,serif;font-style:italic;font-weight:700;color:{INK};line-height:1.12;margin:6px 0 10px;font-size:clamp(26px,3.4vw,40px)}}
.dek{{font-size:17px;color:{INK2};margin:0 0 6px;max-width:72ch}}
.cap{{font-size:12.5px;color:{INK3}}}
.grid{{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(min(100%,200px),1fr))}}
.grid2{{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(min(100%,360px),1fr))}}
.card{{background:#fff;border:1px solid {MIST_B};border-top:4px solid {TEAL};border-radius:14px;padding:16px 18px;min-width:0}}
.v{{font-size:clamp(26px,3vw,34px);font-weight:800;line-height:1.1;font-variant-numeric:tabular-nums}}
.l{{font-size:14px;font-weight:600;color:{INK2}}}
h2{{font-family:Georgia,serif;font-style:italic;font-size:21px;margin:0 0 4px}}
table{{width:100%;border-collapse:collapse;font-size:14px;font-variant-numeric:tabular-nums}}
th{{text-align:left;font-size:12px;color:{INK3};border-bottom:1px solid {MIST_B};padding:6px}}
td{{padding:8px 6px;border-bottom:1px solid {MIST_B}}}
.n{{text-align:right}}
svg text{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}}
.foot{{margin-top:22px;border-top:1px solid {MIST_B};padding-top:10px}}
"""


def _page(title: str, body: str, extra_css: str = "", script: str = "") -> str:
    return ("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
            f"<title>{_e(title)}</title><style>{_BASE_CSS}{extra_css}</style></head><body>{body}"
            + (f"<script>{script}</script>" if script else "") + "</body></html>")


def _waterfall_svg(wf: dict) -> str:
    rows = wf.get("data") or []
    if not rows:
        return ""
    W, H, top, base = 640, 300, 40, 250
    vals = [float(r["value"]) for r in rows]
    mx = max(vals) * 1.12 or 1
    band = (W - 20) / len(rows)
    bw = band * 0.56
    run, out = 0.0, []
    for i, r in enumerate(rows):
        v = float(r["value"])
        tot = bool(r.get("isTotal")) or i == 0
        lo, hi = (0.0, v) if tot else (run, run + v)
        run = hi
        y = lambda x: base - x / mx * (base - top)  # noqa: E731
        x = 10 + i * band + (band - bw) / 2
        col = TEAL_DK if r.get("isTotal") else SAND if i == 0 else TEAL
        lab = ("+" if not tot and v >= 0 else "") + _usd_m(v)
        out.append(f'<rect x="{x:.1f}" y="{y(hi):.1f}" width="{bw:.1f}" height="{max(2, y(lo) - y(hi)):.1f}" rx="4" fill="{col}"><title>{_e(r["category"])}: {_e(lab)}</title></rect>'
                   f'<text x="{x + bw / 2:.1f}" y="{y(hi) - 7:.1f}" text-anchor="middle" font-size="14" font-weight="800" fill="{INK}">{_e(lab)}</text>'
                   f'<text x="{x + bw / 2:.1f}" y="{base + 22}" text-anchor="middle" font-size="12.5" fill="{INK2}">{_e(r["category"])}</text>')
    return (f'<svg viewBox="0 0 {W} {H}" width="100%" role="img" aria-label="{_e(wf.get("title", "Room revenue bridge"))}">'
            f'<line x1="0" x2="{W}" y1="{base}" y2="{base}" stroke="#9FB4BC"/>' + "".join(out) + "</svg>")


def _towers_svg(g: dict) -> str:
    rows = g.get("rows") or []
    if not rows:
        return ""
    W, H, top, base = 640, 280, 34, 240
    band = (W - 20) / len(rows)
    bw = band * 0.6
    out = []
    for i, r in enumerate(rows):
        v = float(r["v"])
        y = base - v / 100 * (base - top)
        x = 10 + i * band + (band - bw) / 2
        col = MAROON if r.get("hi") else SAND
        out.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{base - y:.1f}" rx="4" fill="{col}"><title>{_e(r["l"])}: {v:.1f}% occupancy</title></rect>'
                   f'<text x="{x + bw / 2:.1f}" y="{y - 7:.1f}" text-anchor="middle" font-size="13" font-weight="800" fill="{INK}">{v:.1f}%</text>'
                   f'<text x="{x + bw / 2:.1f}" y="{base + 22}" text-anchor="middle" font-size="12.5" fill="{INK2}">{_e(r["l"])}</text>')
    return (f'<svg viewBox="0 0 {W} {H}" width="100%" role="img" aria-label="{_e(g.get("title", "Occupancy by night"))}">'
            f'<line x1="0" x2="{W}" y1="{base}" y2="{base}" stroke="#9FB4BC"/>' + "".join(out) + "</svg>")


# ---------------------------------------------------------------------------
# One-page dashboard
# ---------------------------------------------------------------------------

def build_dashboard_html(report: dict) -> str:
    s = report.get("summary") or {}
    S = _sections(report)
    today, hotel, out = S.get("today") or {}, S.get("hotel") or {}, S.get("outlook") or {}
    cards = "".join(
        f'<div class="card"><div class="v">{_e(x.get("v"))}</div><div class="l">{_e(x.get("l"))}</div>'
        f'<div class="cap">Source: {_e(x.get("src"))}</div></div>' for x in (s.get("stats") or []))
    wf = today.get("waterfall") or {}
    tw = _glance(hotel, "towers") or {}
    panels = ""
    if wf.get("data"):
        panels += (f'<div class="card"><h2>{_e(wf.get("title"))}</h2><p class="cap">{_e(wf.get("sub"))}</p>{_waterfall_svg(wf)}'
                   f'<p class="cap">Calculation: {_e(wf.get("calc"))}<br>Source: {_e(wf.get("src"))}</p></div>')
    if tw.get("rows"):
        panels += (f'<div class="card"><h2>{_e(tw.get("title"))}</h2><p class="cap">{_e(tw.get("sub"))}</p>{_towers_svg(tw)}'
                   f'<p class="cap">Source: {_e(tw.get("src"))}</p></div>')
    tk = "".join(f'<tr><td>{_e(t.get("tab"))}</td><td class="n"><b>{_e(t.get("v"))}</b></td><td>{_e(t.get("l"))}</td><td class="cap">{_e(t.get("src"))}</td></tr>'
                 for t in report.get("takeaways", []))
    ev = out.get("table") or {}
    ev_html = ""
    if ev.get("rows"):
        heads = "".join(f'<th{" class=n" if i in (ev.get("num") or []) else ""}>{_e(c)}</th>' for i, c in enumerate(ev.get("cols", [])))
        body = "".join("<tr>" + "".join(f'<td{" class=n" if i in (ev.get("num") or []) else ""}>{_e(c)}</td>' for i, c in enumerate(r)) + "</tr>" for r in ev["rows"])
        ev_html = (f'<div class="card" style="margin-top:14px"><h2>{_e(ev.get("title"))}</h2><p class="cap">{_e(ev.get("sub"))}</p>'
                   f'<div style="overflow-x:auto"><table><thead><tr>{heads}</tr></thead><tbody>{body}</tbody></table></div>'
                   f'<p class="cap">Source: {_e(ev.get("src"))}</p></div>')
    body = (f'<div class="wrap"><div class="eye">Dana Point PULSE · Dashboard · {_e(report.get("period"))}</div>'
            f'<h1 class="hed">{_e(s.get("headline"))}</h1><p class="dek">{_e(s.get("sub"))}</p>'
            f'<p class="cap">{_e(report.get("period_detail"))} · Prepared {_e(report.get("prepared"))}</p>'
            f'<div class="grid" style="margin-top:16px">{cards}</div>'
            f'<div class="grid2" style="margin-top:14px">{panels}</div>{ev_html}'
            f'<div class="card" style="margin-top:14px"><h2>Key takeaways</h2><div style="overflow-x:auto"><table><thead><tr><th>Section</th><th class="n">Number</th><th>What it measures</th><th>Source</th></tr></thead><tbody>{tk}</tbody></table></div></div>'
            f'<p class="cap foot">Prepared by GloCon Solutions LLC for Visit Dana Point. Media contact: John Picou, GloCon Solutions LLC, john.picou@gloconsolutions.com</p></div>')
    return _page(f"{report.get('title', 'Dana Point PULSE')} dashboard", body)


# ---------------------------------------------------------------------------
# Animated highlights
# ---------------------------------------------------------------------------

_HL_CSS = f"""
body{{background:#F7F5F0}}
.stage{{position:relative;max-width:1180px;margin:0 auto;aspect-ratio:16/9;background:#F7F5F0;overflow:hidden;border-bottom:1px solid {MIST_B}}}
.sc{{position:absolute;inset:0;padding:6% 7%;display:flex;flex-direction:column;justify-content:center;gap:2.2vmin;opacity:0;visibility:hidden}}
.sc.on{{opacity:1;visibility:visible}}
.mast{{font-family:Georgia,serif;font-weight:700;font-size:clamp(34px,7vw,92px);line-height:1;color:{INK}}}
.rule{{height:4px;background:{INK};transform-origin:0 50%;transform:scaleX(0)}}
.sc.on .rule{{animation:draw .9s ease-out .3s forwards}}
.big{{font-weight:800;font-size:clamp(54px,13vw,170px);line-height:1;font-variant-numeric:tabular-nums;color:{INK}}}
.up{{font-weight:800;color:{TEAL};font-size:clamp(22px,3.4vw,42px)}}
.hl{{font-family:Georgia,serif;font-weight:700;font-size:clamp(24px,4.2vw,54px);line-height:1.12;color:{INK}}}
.sub{{font-size:clamp(14px,1.9vw,24px);color:{INK2}}}
.src{{font-size:clamp(11px,1.3vw,15px);color:{INK3}}}
.sc.on .rise{{animation:rise .6s ease-out both}}
.sc.on .d1{{animation-delay:.2s}} .sc.on .d2{{animation-delay:.6s}} .sc.on .d3{{animation-delay:1.1s}} .sc.on .d4{{animation-delay:1.6s}}
.bars rect{{transform-box:fill-box;transform-origin:50% 100%;transform:scaleY(0)}}
.sc.on .bars rect{{animation:grow .7s ease-out forwards}}
@keyframes draw{{to{{transform:scaleX(1)}}}}
@keyframes rise{{from{{opacity:0;transform:translateY(18px)}}to{{opacity:1;transform:none}}}}
@keyframes grow{{to{{transform:scaleY(1)}}}}
@media (orientation:portrait),(max-width:640px){{.stage{{aspect-ratio:4/5}}.sc{{padding:8% 6%}}}}
.ctl{{max-width:1180px;margin:10px auto 0;display:flex;gap:10px;align-items:center;padding:0 12px}}
.ctl button{{font:700 14px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;padding:9px 16px;border-radius:999px;border:1px solid {TEAL_DK};background:{TEAL};color:#fff;cursor:pointer}}
.ctl button.sec{{background:#fff;color:{TEAL_DK}}}
.ctl button:focus-visible{{outline:2px solid {INK};outline-offset:2px}}
@media (prefers-reduced-motion:reduce){{.sc.on .rule{{animation:none;transform:none}}.sc.on .rise,.sc.on .bars rect{{animation:none;transform:none;opacity:1}}}}
"""


def build_highlights_html(report: dict) -> str:
    s = report.get("summary") or {}
    S = _sections(report)
    stats = s.get("stats") or []
    first = stats[1] if len(stats) > 1 else (stats[0] if stats else {"v": "", "l": "", "src": ""})
    wf = (S.get("today") or {}).get("waterfall") or {}
    wf_svg = _waterfall_svg(wf).replace("<svg ", '<svg class="bars" ', 1) if wf.get("data") else ""
    sc = [
        (f'<div class="eye rise d1">Visit Dana Point · {_e(report.get("period"))}</div><div class="mast rise d1">Dana Point PULSE</div>'
         f'<div class="rule"></div><div class="sub rise d3">{_e(report.get("period_detail"))}</div>'),
        (f'<div class="eye rise d1">News report</div><div class="hl rise d1">{_e(s.get("headline"))}</div>'
         f'<div class="big rise d2">{_e(first.get("v"))}</div><div class="sub rise d3">{_e(first.get("l"))}</div>'
         f'<div class="src rise d4">Source: {_e(first.get("src"))}</div>'),
    ]
    if wf_svg:
        sc.append(f'<div class="eye rise d1">{_e(wf.get("title"))}</div><div class="sub rise d1">{_e(wf.get("sub"))}</div>'
                  f'<div style="max-width:900px;width:100%">{wf_svg}</div><div class="src rise d4">Source: {_e(wf.get("src"))}</div>')
    sc.append(f'<div class="eye rise d1">Dana Point PULSE · Insights</div><div class="hl rise d1">{_e(s.get("sub"))}</div>'
              f'<div class="rule"></div><div class="src rise d3">Prepared by GloCon Solutions LLC for Visit Dana Point · Prepared {_e(report.get("prepared"))}</div>')
    durs = [4, 5, 6, 5][:len(sc)] if len(sc) == 4 else [4, 5, 5]
    scenes = "".join(f'<section class="sc" data-dur="{d}" aria-label="Scene {i + 1}">{c}</section>' for i, (c, d) in enumerate(zip(sc, durs)))
    body = (f'<div class="stage" role="region" aria-roledescription="animation" aria-label="Dana Point PULSE highlights">{scenes}</div>'
            '<div class="ctl"><button id="play">Replay</button><button id="pause" class="sec">Pause</button>'
            f'<span class="cap">About {sum(durs)} seconds. {_e(report.get("period_detail"))}</span></div>')
    js = """
(function(){var sc=[].slice.call(document.querySelectorAll('.sc')),i=-1,tm=null,paused=false;
var reduce=window.matchMedia&&window.matchMedia('(prefers-reduced-motion: reduce)').matches;
function show(n){sc.forEach(function(s,k){s.classList.toggle('on',k===n);});i=n;}
function next(){if(paused)return;var n=i+1;if(n>=sc.length){return;}show(n);tm=setTimeout(next,(+sc[n].getAttribute('data-dur')||5)*1000);}
function play(){clearTimeout(tm);paused=false;document.getElementById('pause').textContent='Pause';i=-1;next();}
document.getElementById('play').onclick=play;
document.getElementById('pause').onclick=function(){if(paused){paused=false;this.textContent='Pause';next();}else{paused=true;clearTimeout(tm);this.textContent='Resume';}};
if(reduce){show(1);}else{play();}})();
"""
    return _page(f"{report.get('title', 'Dana Point PULSE')} highlights", body, _HL_CSS, js)
