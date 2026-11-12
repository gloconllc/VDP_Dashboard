# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
build_weekly_motion.py — weekly motion recap GIF and newsletter HTML, from the database only

Every number, date, headline and highlight in the GIF and the newsletter is computed
here from data/analytics.sqlite each run. The templates
(recaps/templates/weekly_motion.html, recaps/templates/weekly_newsletter.html) hold
layout only; nothing in them is a figure.

Sources
  Week (latest 7 days) and last 28 days  costar_market_daily  (same method as
                                         build_weekly_recap.week_kpis, so the GIF,
                                         email and PDF agree)
  Latest full month and weekday/weekend  costar_market_monthly, costar_weekpart_monthly

Rules
  * Last-year values are backed out of CoStar's own year-over-year percent columns, so
    each night is compared with the same weekday last year.
  * Occupancy change is shown in points, computed from occupancy levels. CoStar's
    *_yoy_pp columns are percent change and are never printed as points.
  * Quality gate: if any week figure is missing or zero, no GIF is written and the
    script exits 3. Optional panels (28 days, month) are left out when their data is
    missing; nothing is filled in.
  * Headlines describe direction only (rose, fell, held) and come from the numbers.

Outputs
  dashboard/assets/motion/Dana_Point_PULSE_Weekly_<week end>.gif   shown on the dashboard
  dashboard/assets/motion/Dana_Point_PULSE_Weekly_<week end>.json  every figure used
  <out-dir>/ (optional) the GIF, the JSON and newsletter.html (GIF referenced by cid)

Usage
  python3 scripts/build_weekly_motion.py [--db PATH] [--out-dir DIR] [--no-dashboard]
Needs: playwright (Chromium) and Pillow.
"""
from __future__ import annotations

import argparse
import base64
import html
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
TEMPLATES = ROOT / "recaps" / "templates"
MOTION_DIR = ROOT / "dashboard" / "assets" / "motion"
PHOTO = ROOT / "dashboard" / "assets" / "photos" / "hero_coast.jpg"
LOGO = ROOT / "dashboard" / "assets" / "vdp_logo_nav.svg"
APP_URL = "https://vdppulse.gloconsolutions.com/"
FPS = 10
DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


class GateError(Exception):
    pass


# ------------------------------------------------------------------ figures
def _window(con, end: str, days: int) -> dict | None:
    rows = con.execute(
        """SELECT as_of_date, supply, demand, revenue_usd, supply_yoy_pct, demand_yoy_pct, revenue_yoy_pct,
                  occupancy_pct, adr_usd
           FROM costar_market_daily WHERE as_of_date > date(?, ?) AND as_of_date <= ?
           ORDER BY as_of_date""", (end, f"-{days} day", end)).fetchall()
    if len(rows) != days:
        return None
    s = d = r = s_ly = d_ly = r_ly = 0.0
    nights = []
    for dt, sup, dem, rev, sy, dy, ry, occ_src, adr_src in rows:
        if not sup or not dem or not rev or None in (sy, dy, ry):
            return None
        ls, ld, lr = sup / (1 + sy / 100), dem / (1 + dy / 100), rev / (1 + ry / 100)
        s, d, r, s_ly, d_ly, r_ly = s + sup, d + dem, r + rev, s_ly + ls, d_ly + ld, r_ly + lr
        nights.append({"date": dt, "dow": DOW[datetime.strptime(dt, "%Y-%m-%d").weekday()],
                       # nightly occupancy and ADR exactly as CoStar prints them
                       "occ": occ_src if occ_src else dem / sup * 100, "occ_ly": ld / ls * 100,
                       "adr": adr_src if adr_src else rev / dem, "adr_chg": ((rev / dem) / (lr / ld) - 1) * 100})
    occ, occ_ly = d / s * 100, d_ly / s_ly * 100
    return {"days": days, "start": rows[0][0], "end": rows[-1][0],
            "supply": s, "demand": d, "revenue": r,
            "occ": occ, "occ_pts": occ - occ_ly,
            "adr": r / d, "adr_chg": ((r / d) / (r_ly / d_ly) - 1) * 100,
            "revpar": r / s, "revpar_chg": ((r / s) / (r_ly / s_ly) - 1) * 100,
            "revenue_chg": (r / r_ly - 1) * 100, "nights": nights}


def _month(con) -> dict | None:
    row = con.execute(
        """SELECT report_period, supply, supply_yoy_pct, demand, demand_yoy_pct, revenue_usd,
                  adr_usd, adr_yoy_pct, revpar_usd, revpar_yoy_pct
           FROM costar_market_monthly
           WHERE supply > 0 AND demand > 0 AND adr_usd > 0 AND revpar_usd > 0
           ORDER BY report_period DESC LIMIT 1""").fetchone()
    if not row or None in row:
        return None
    per, sup, sy, dem, dy, rev, adr, adr_c, rvp, rvp_c = row
    occ = dem / sup * 100
    occ_ly = (dem / (1 + dy / 100)) / (sup / (1 + sy / 100)) * 100
    out = {"period": per, "label": datetime.strptime(per, "%Y-%m").strftime("%B %Y"),
           "occ": occ, "occ_pts": occ - occ_ly, "adr": adr, "adr_chg": adr_c,
           "revpar": rvp, "revpar_chg": rvp_c}
    parts = dict(con.execute(
        """SELECT day_part, occupancy_pct FROM costar_weekpart_monthly
           WHERE report_period = ? AND day_part IN ('weekend','weekday') AND occupancy_pct > 0""", (per,)).fetchall())
    if "weekend" in parts and "weekday" in parts:
        out["weekend_occ"], out["weekday_occ"] = parts["weekend"], parts["weekday"]
    return out


def figures(db: Path) -> dict:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        end = con.execute("SELECT MAX(as_of_date) FROM costar_market_daily").fetchone()[0]
        if not end:
            raise GateError("no CoStar daily rows")
        week = _window(con, end, 7)
        if not week:
            raise GateError(f"the 7 days ending {end} are not complete in costar_market_daily")
        for k in ("occ", "adr", "revpar", "revenue", "demand", "supply"):
            if not week.get(k):
                raise GateError(f"week figure {k} is missing or zero")
        last28 = _window(con, end, 28)
        month = _month(con)
    finally:
        con.close()
    return {"built": date.today().isoformat(), "data_through": end,
            "week": week, "last28": last28, "month": month,
            "sources": {"week": "costar_market_daily", "last28": "costar_market_daily",
                        "month": "costar_market_monthly, costar_weekpart_monthly"}}


# --------------------------------------------------------------- formatting
def d(iso: str) -> date:
    return datetime.strptime(iso, "%Y-%m-%d").date()


def ap_date(x: date, year=True) -> str:
    mon = {1: "Jan.", 2: "Feb.", 8: "Aug.", 9: "Sept.", 10: "Oct.", 11: "Nov.", 12: "Dec."}.get(
        x.month, x.strftime("%B"))
    return f"{mon} {x.day}" + (f", {x.year}" if year else "")


def ap_range(a: date, b: date) -> str:
    if a.year != b.year:
        return f"{ap_date(a)} to {ap_date(b)}"
    if a.month == b.month:
        return f"{ap_date(a, False)} to {b.day}, {b.year}"
    return f"{ap_date(a, False)} to {ap_date(b)}"


def money(v, cents=True):
    return f"${v:,.2f}" if cents else f"${v:,.0f}"


def millions(v):
    return f"${v / 1e6:.2f}M" if v < 1e7 else f"${v / 1e6:.1f}M"


def var(v, pts=False, digits=1):
    """Glyph, value and favorability. Higher is favorable for every hotel KPI here."""
    r = round(v, digits)
    if r == 0:
        return "▶", f"{abs(r):.{digits}f}{' pts' if pts else '%'}", "No change", "ink"
    glyph = "▲" if r > 0 else "▼"
    return glyph, f"{abs(r):.{digits}f}{' pts' if pts else '%'}", ("Favorable" if r > 0 else "Unfavorable"), (
        "fav" if r > 0 else "unf")


def direction(v, digits=1):
    r = round(v, digits)
    return "held" if r == 0 else ("rose" if r > 0 else "fell")


def headline(w) -> str:
    rate, occ = direction(w["adr_chg"]), direction(w["occ_pts"])
    if rate == "rose" and occ == "fell":
        return "Rates rose; fewer rooms filled"
    if rate == "fell" and occ == "rose":
        return "More rooms filled; rates fell"
    if rate == "rose" and occ == "rose":
        return "Rates and occupancy both rose"
    if rate == "fell" and occ == "fell":
        return "Rates and occupancy both fell"
    return f"Rates {rate}; occupancy {occ}"


def adr_up_nights(nights) -> str:
    up = [n["dow"] for n in nights if round(n["adr_chg"], 1) > 0]
    if not up:
        return "ADR was below last year every night."
    if len(up) == len(nights):
        return "ADR was higher than last year every night."
    names = {"Mon": "Monday", "Tue": "Tuesday", "Wed": "Wednesday", "Thu": "Thursday",
             "Fri": "Friday", "Sat": "Saturday", "Sun": "Sunday"}
    idx = [i for i, n in enumerate(nights) if round(n["adr_chg"], 1) > 0]
    if idx == list(range(idx[0], idx[-1] + 1)) and len(idx) >= 3:
        return f"ADR was higher than last year {names[up[0]]} through {names[up[-1]]}."
    full = [names[u] for u in up]
    joined = full[0] if len(full) == 1 else (", ".join(full[:-1]) + f" and {full[-1]}")
    return f"ADR was higher than last year on {joined}."


# ------------------------------------------------------------------- render
NOTES = {"Occupancy": "Share of rooms sold", "ADR": "Price paid per room sold",
         "RevPAR": "Room revenue per available room", "Room revenue": "Room revenue for the seven days"}


def _report(db: Path):
    """The app's own year-to-date report (pulse_insights), for the Datafy and STR scenes."""
    import build_pulse_motion as bpm  # noqa: E402  (shares the app-engine loader)
    report, _, _, _ = bpm.report_and_week(db)
    return report, bpm.clean


def motion_data(f, report, clean) -> dict:
    w, m = f["week"], f["month"]
    a, b = d(w["start"]), d(w["end"])
    ly_a = a - timedelta(days=364)
    names = {"Mon": "Monday", "Tue": "Tuesday", "Wed": "Wednesday", "Thu": "Thursday",
             "Fri": "Friday", "Sat": "Saturday", "Sun": "Sunday"}
    nights = w["nights"]
    top = max(range(len(nights)), key=lambda i: nights[i]["occ"])
    hi_r = max(range(len(nights)), key=lambda i: nights[i]["adr"])
    lo_r = min(range(len(nights)), key=lambda i: nights[i]["adr"])

    def k(label, val, v):
        return {"l": label, "v": val, "glyph": v[0], "d": v[1], "word": v[2],
                "fav": True if v[3] == "fav" else (False if v[3] == "unf" else None), "note": NOTES[label]}
    out = {
        "intro": {"eyebrow": "Dana Point PULSE weekly", "title": "This week in Dana Point hotels",
                  "sub": f"{ap_range(a, b)}, compared with the same seven",
                  "sub2": f"days of {ly_a.year}", "src": "Source: CoStar daily, Dana Point market"},
        "week": {"eyebrow": f"This week · {ap_range(a, b)}", "headline": headline(w),
                 "kpis": [k("Occupancy", f"{w['occ']:.1f}%", var(w["occ_pts"], True)),
                          k("ADR", money(w["adr"]), var(w["adr_chg"])),
                          k("RevPAR", money(w["revpar"]), var(w["revpar_chg"])),
                          k("Room revenue", money(w["revenue"], False), var(w["revenue_chg"]))],
                 "src": f"Each figure compared with {ap_range(ly_a, ly_a + timedelta(days=6))}. Source: CoStar daily, Dana Point market"},
        "nights": [{"dow": n["dow"], "occ": round(n["occ"], 1), "adr": round(n["adr"], 2)} for n in nights],
        "top": top,
        "night_headline": f"{names[nights[top]['dow']]} was the strongest night at {nights[top]['occ']:.1f}%",
        "night_src": f"Source: CoStar daily, Dana Point market, {ap_range(a, b)}",
        "rate": {"eyebrow": f"Nightly rate, {ap_range(a, b)}",
                 "headline": (f"{names[nights[hi_r]['dow']]}'s rate, {money(nights[hi_r]['adr'], False)}, topped "
                              f"{names[nights[lo_r]['dow']]}'s by {money(nights[hi_r]['adr'] - nights[lo_r]['adr'], False)}"),
                 "src": "Source: CoStar daily, Dana Point market"},
        "outro": {"eyebrow": "Visit Dana Point · See the full picture",
                  "title": "Explore the live Dana Point PULSE dashboard", "url": "vdppulse.gloconsolutions.com",
                  "foot": f"© {b.year} GloCon Solutions LLC · Prepared by John Picou for Visit Dana Point · Hotel data through {ap_date(b)}"},
    }
    S = {s_["id"]: s_ for s_ in (report or {}).get("sections", [])}
    # Datafy: visitors and spending, exactly as the app's Insights report states them
    sp, who, today = S.get("spend"), S.get("who"), S.get("today")
    if sp and who:
        n_sp = sp.get("numbers", [{}])[0]
        n_on = next((x for x in who.get("numbers", []) if "Overnight" in x.get("l", "")), None)
        chain = next((g for g in (today or {}).get("glance", []) if g.get("type") == "chain"), None)
        trips = chain["steps"][0] if chain else None
        bars = next((c for c in who.get("charts", []) if c.get("type") == "hbar"), None)
        stats = []
        if n_sp.get("v"):
            g = "▲" if "▲" in n_sp.get("p", "") else ("▼" if "▼" in n_sp.get("p", "") else "")
            chg_m = re.search(r"[+-][0-9.]+%", n_sp.get("p", ""))
            stats.append({"l": f"Visitor spending, {clean(n_sp['k'])}", "v": n_sp["v"],
                          "p": f"{g} {chg_m.group(0) if chg_m else ''}".strip(),
                          "fav": True if g == "▲" else (False if g == "▼" else None)})
        if trips:
            stats.append({"l": "Visitor trips, " + clean(trips["l"]).split("(")[-1].rstrip(")"), "v": trips["v"], "p": "", "fav": None})
        if n_on:
            stats.append({"l": "Overnight share of trips", "v": n_on["v"], "p": clean(n_on.get("p", "")), "fav": None})
        if stats and bars and bars.get("rows"):
            out["visitors"] = {"eyebrow": "Visitors · Datafy", "headline": clean(sp["headline"]), "stats": stats,
                               "markets": [{"l": r_["l"], "v": round(r_["v"], 1)} for r_ in bars["rows"][:5]],
                               "markets_title": "Share of visitor spending by market",
                               "src": "Source: Datafy, " + clean(bars.get("sub", "")).split(", ", 1)[-1]}
    # STR: weekly coastal peer report
    wk = S.get("week")
    hb = next((c for c in (wk or {}).get("charts", []) if c.get("type") == "hbar"), None)
    if hb and hb.get("rows"):
        rows = sorted(hb["rows"], key=lambda r_: -r_["v"])
        rank = next(i for i, r_ in enumerate(rows) if r_.get("hi")) + 1
        out["peers"] = {"eyebrow": f"Coastal peers · {clean(hb['sub'])}",
                        "headline": f"Dana Point ranked #{rank} of {len(rows)} coastal markets on revenue per available room",
                        "rows": [{"l": r_["l"], "v": r_["v"], "hi": bool(r_.get("hi"))} for r_ in rows],
                        "src": "Source: " + clean(hb["src"])}
    # The wider view: latest four weeks as the app states them, plus the latest CoStar month
    cards, ahead = [], []
    ol = S.get("outlook")
    vs = next((g for g in (ol or {}).get("glance", []) if g.get("type") == "versus"), None)
    o_occ = next((x for x in (ol or {}).get("numbers", []) if x.get("l") == "Occupancy"), None)
    if vs and vs["a"]["v"] and vs["b"]["v"]:
        chg = (vs["a"]["v"] / vs["b"]["v"] - 1) * 100
        v = var(chg)
        start28 = b - timedelta(days=27)
        lines = []
        if o_occ:
            om = re.search(r"([+-])([0-9.]+) point", o_occ.get("p", ""))
            lines.append(f"Occupancy {o_occ['v']}" + (f" ({'▲' if om.group(1) == '+' else '▼'} {om.group(2)} pts)" if om else ""))
        lines.append(f"Same weeks last year: {money(vs['b']['v'])} RevPAR")
        cards.append({"label": f"Latest four weeks · {ap_range(start28, b)}", "v": money(vs["a"]["v"]), "unit": "RevPAR",
                      "glyph": v[0], "d": v[1], "word": v[2], "fav": v[3] == "fav" if v[3] != "ink" else None, "lines": lines})
        ahead.append(round(chg, 1) > 0)
    if m:
        v = var(m["revpar_chg"]); o = var(m["occ_pts"], True); r = var(m["adr_chg"])
        lines = [f"ADR {money(m['adr'])} ({r[0]} {r[1]}), occupancy {m['occ']:.1f}% ({o[0]} {o[1]})"]
        if "weekend_occ" in m:
            lines.append(f"Weekends {m['weekend_occ']:.1f}% full, weekdays {m['weekday_occ']:.1f}%")
        cards.append({"label": f"{m['label']} · latest full month", "v": money(m["revpar"]), "unit": "RevPAR",
                      "glyph": v[0], "d": v[1], "word": v[2], "fav": v[3] == "fav" if v[3] != "ink" else None, "lines": lines})
        ahead.append(round(m["revpar_chg"], 1) > 0)
    if cards:
        hl = ("Four weeks and the latest month ran ahead of last year" if all(ahead) else
              "Four weeks and the latest month ran behind last year" if not any(ahead) else
              "The latest four weeks and the latest full month")
        out["wide"] = {"eyebrow": "The wider view", "headline": hl, "cards": cards,
                       "src": "Source: STR and CoStar daily (four weeks, as on the dashboard); CoStar monthly, Dana Point market"}
    return out


def _fonts() -> tuple[str, str]:
    fd = ROOT / "dashboard" / "assets" / "fonts"
    enc = lambda f: "data:font/woff2;base64," + base64.b64encode((fd / f).read_bytes()).decode()
    return enc("PlusJakartaSans-var.woff2"), enc("PlusJakartaSans-italic-var.woff2")


def motion_page(data: dict, capture: bool) -> str:
    reg, ita = _fonts()
    t = (TEMPLATES / "weekly_motion.html").read_text().replace("url(FONT_REG)", f"url({reg})").replace("url(FONT_ITA)", f"url({ita})")
    inject = (f"<script>window.__CAPTURE__={'true' if capture else 'false'};"
              f"window.PULSE_DATA={json.dumps(data, default=str)};</script>")
    return t.replace("<body>", "<body>\n" + inject, 1)


def render(data: dict, work: Path, name: str) -> dict:
    """GIF 640x360 at 12.5 fps (the style John approved), MP4 1280x720, poster JPG, playable HTML."""
    import subprocess
    from PIL import Image
    from playwright.sync_api import sync_playwright
    src = work / "m.html"
    src.write_text(motion_page(data, True))
    fr = work / "frames"
    fr.mkdir()
    fps = 12.5
    with sync_playwright() as p:
        br = p.chromium.launch()
        pg = br.new_page(viewport={"width": 640, "height": 360}, device_scale_factor=2)
        pg.goto(src.as_uri())
        pg.wait_for_timeout(700)
        dur = pg.evaluate("window.DUR")
        n = int(dur * fps)
        # Outlook and some phones show only a GIF's first frame: lead with the finished title card.
        pg.evaluate("window.renderAt(4.0)")
        pg.screenshot(path=str(work / "first.png"))
        for i in range(n):
            pg.evaluate(f"window.renderAt({i / fps})")
            pg.screenshot(path=str(fr / f"f{i:05d}.png"))
        br.close()
    mp4 = work / f"{name}.mp4"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-framerate", "12.5", "-i", str(fr / "f%05d.png"),
                    "-vf", "fps=25", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "20", "-movflags", "+faststart", str(mp4)],
                   check=True)
    frames = [Image.open(fr / f"f{i:05d}.png").convert("RGB").resize((640, 360), Image.LANCZOS) for i in range(n)]
    picks = [frames[int(len(frames) * k)] for k in (0.03, 0.15, 0.3, 0.42, 0.55, 0.68, 0.8, 0.92)]
    sample = Image.new("RGB", (640, 360 * len(picks)))
    for i, im in enumerate(picks):
        sample.paste(im, (0, 360 * i))
    sw = Image.new("RGB", (640, 40))
    for i, hexc in enumerate(["#b45309", "#a8461f", "#0e7490", "#17595c", "#b9e0e3", "#1f3b3d", "#f6f5ef", "#ffffff"]):
        sw.paste(Image.new("RGB", (80, 40), hexc), (i * 80, 0))
    sample2 = Image.new("RGB", (640, sample.height + 40 * 6))
    sample2.paste(sample, (0, 0))
    for j in range(6):
        sample2.paste(sw, (0, sample.height + 40 * j))
    pal = sample2.quantize(colors=200, method=Image.Quantize.MEDIANCUT)
    first = Image.open(work / "first.png").convert("RGB").resize((640, 360), Image.LANCZOS)
    q = [im.quantize(palette=pal, dither=Image.Dither.NONE) for im in [first] + frames]
    gif = work / f"{name}.gif"
    q[0].save(gif, save_all=True, append_images=q[1:], duration=[80] * len(q), loop=0, optimize=True, disposal=1)
    poster = work / f"{name}.jpg"
    Image.open(fr / f"f{int(n * 0.12):05d}.png").convert("RGB").save(poster, "JPEG", quality=88)
    play = work / f"{name}.html"
    play.write_text(motion_page(data, False))
    return {"gif": gif, "mp4": mp4, "jpg": poster, "html": play, "seconds": round(dur, 1)}


def newsletter_html(f, data: dict, gif_src: str, intro: str, extra_html: str = "", new_line: str = "") -> tuple[str, str, str]:
    """Return (subject, html, plain text) for Heather's weekly newsletter. Every number comes from
    `data` (motion_data: CoStar week, Datafy and STR as the app states them, four weeks as on the
    dashboard, latest CoStar month), so the email, the GIF and the dashboard agree."""
    w = f["week"]
    a, b = d(w["start"]), d(w["end"])
    subject = f"Dana Point PULSE Weekly | Week Ending {b.strftime('%B')} {b.day}, {b.year}"
    cells = []
    for i, k in enumerate(data["week"]["kpis"]):
        pad = "10px 8px 10px 0" if i == 0 else ("10px 0 10px 8px" if i == 3 else "10px 8px")
        color = "#0e7490" if k["fav"] is True else ("#a8461f" if k["fav"] is False else "#334155")
        cells.append(f'<td width="25%" valign="top" style="padding:{pad};border-top:3px solid #17595c;font-family:Arial,sans-serif;">'
                     f'<div style="font-size:11px;font-weight:bold;letter-spacing:1px;text-transform:uppercase;color:#6b7280;">{html.escape(k["l"])}</div>'
                     f'<div style="font-size:22px;font-weight:bold;color:#0b2530;padding-top:4px;">{html.escape(k["v"])}</div>'
                     f'<div style="font-size:12px;font-weight:bold;color:{color};">{html.escape(k["glyph"])} {html.escape(k["d"])}</div></td>')

    def block(eyebrow, head, body_html, src):
        return (f'<tr><td style="padding:18px 24px 0;font-family:Arial,sans-serif;">'
                f'<div style="font-size:11px;font-weight:bold;letter-spacing:1.5px;text-transform:uppercase;color:#0e7490;">{html.escape(eyebrow)}</div>'
                f'<div style="font-family:Georgia,serif;font-style:italic;font-weight:bold;font-size:20px;line-height:26px;color:#0b2530;padding:4px 0 6px;">{html.escape(head)}</div>'
                f'<div style="font-size:14px;line-height:22px;color:#334155;">{body_html}</div>'
                f'<div style="font-size:11px;color:#64748b;padding-top:4px;">{html.escape(src)}</div></td></tr>')
    blocks, text = [], []
    n = data["nights"]
    top = n[data["top"]]
    rate = data["rate"]
    blocks.append(block(f"Night by night · {ap_range(a, b)}", data["night_headline"],
                        f"{html.escape(adr_up_nights(w['nights']))} {html.escape(rate['headline'])}.", data["night_src"]))
    text.append(f"Night by night: {data['night_headline']} ({money(top['adr'], False)} rate). {adr_up_nights(w['nights'])} {rate['headline']}.")
    v = data.get("visitors")
    if v:
        stats = "; ".join(f"{s_['l']}: <b>{html.escape(s_['v'])}</b>" + (f" ({html.escape(s_['p'])})" if s_["p"] else "") for s_ in v["stats"])
        mk = ", ".join(f"{m_['l']} {m_['v']:.1f}%" for m_ in v["markets"][:3])
        blocks.append(block(v["eyebrow"], v["headline"], f"{stats}. Top markets by share of visitor spending: {html.escape(mk)}.", v["src"]))
        text.append(f"Visitors (Datafy): {v['headline']}. " + "; ".join(f"{s_['l']}: {s_['v']}" + (f" ({s_['p']})" if s_['p'] else "") for s_ in v["stats"]) + f". Top markets: {mk}.")
    pr = data.get("peers")
    if pr:
        rows = ", ".join(f"{r_['l']} ${r_['v']:.0f}" for r_ in pr["rows"])
        blocks.append(block(pr["eyebrow"], pr["headline"], f"Revenue per available room: {html.escape(rows)}.", pr["src"]))
        text.append(f"Coastal peers (STR, {pr['eyebrow'].split('· ')[-1]}): {pr['headline']}. {rows}.")
    wd = data.get("wide")
    if wd:
        parts_h, parts_t = [], []
        for c in wd["cards"]:
            parts_h.append(f"<b>{html.escape(c['label'])}:</b> RevPAR <b>{html.escape(c['v'])}</b> ({html.escape(c['glyph'])} {html.escape(c['d'])}). "
                           + html.escape(". ".join(c["lines"])) + ".")
            parts_t.append(f"{c['label']}: RevPAR {c['v']} ({c['glyph']} {c['d']}). " + ". ".join(c["lines"]) + ".")
        blocks.append(block("The wider view", wd["headline"], "<br>".join(parts_h), wd["src"]))
        text.append("The wider view: " + " ".join(parts_t))
    week_line = f"Week of {ap_range(a, b)}, compared with the same seven weekdays last year. Source: CoStar daily, Dana Point market."
    tok = {"subject": subject, "week_end_short": ap_date(b), "intro": intro,
           "gif_alt": html.escape(f"Dana Point PULSE weekly motion, week of {ap_range(a, b)}: occupancy {w['occ']:.1f}%, "
                                  f"ADR {money(w['adr'])}, RevPAR {money(w['revpar'])}, room revenue {money(w['revenue'], False)}."),
           "headline": headline(w), "week_line": week_line, "kpi_cells": "".join(cells),
           "detail_html": "".join(blocks), "new_line": new_line, "extra_html": extra_html,
           "sources": "CoStar daily and monthly (Dana Point market), STR weekly coastal peer report, Datafy visitor economy dashboard"}
    t = (TEMPLATES / "weekly_newsletter.html").read_text().replace("__GIF__", gif_src)
    for k_, v_ in tok.items():
        t = t.replace("{{" + k_ + "}}", v_)
    if "{{" in t:
        raise GateError("newsletter template has an unfilled token")
    def sg(x, pts=False):
        r_ = round(x, 1)
        return f"{'+' if r_ > 0 else ''}{r_:.1f}{' pts' if pts else '%'}"
    week_txt = (f"This week, {ap_range(a, b)}, compared with the same weekdays last year (CoStar daily):\n"
                f"Occupancy: {w['occ']:.1f}% ({sg(w['occ_pts'], True)})\nADR: {money(w['adr'])} ({sg(w['adr_chg'])})\n"
                f"RevPAR: {money(w['revpar'])} ({sg(w['revpar_chg'])})\nRoom revenue: {money(w['revenue'], False)} ({sg(w['revenue_chg'])})")
    return subject, t, week_txt + "\n\n" + "\n\n".join(text)


STATIC = ROOT / "dashboard" / "static" / "motion"


def build(db: Path, out_dir: Path | None = None, dashboard: bool = True) -> dict:
    f = figures(db)
    try:
        report, clean = _report(db)
    except Exception as exc:  # noqa: BLE001  the hotel scenes still build; Datafy/STR scenes need the app engine
        print(f"[motion] app report unavailable, Datafy and STR scenes left out: {exc}")
        report, clean = None, (lambda x: str(x))
    data = motion_data(f, report, clean)
    end = f["week"]["end"]
    name = f"Dana_Point_PULSE_Weekly_{end}"
    record = {"built": date.today().isoformat(), "data_through": end, "scenes": [k for k in data], "figures": data,
              "week_figures": f}
    with tempfile.TemporaryDirectory() as tmp:
        res = render(data, Path(tmp), name)
        (Path(tmp) / f"{name}.json").write_text(json.dumps(record, indent=2, default=str))
        res["json"] = Path(tmp) / f"{name}.json"
        targets = []
        if dashboard:
            STATIC.mkdir(parents=True, exist_ok=True)
            targets.append(STATIC)
        if out_dir:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            targets.append(Path(out_dir))
        for t in targets:
            for k in ("gif", "mp4", "jpg", "json", "html"):
                if t == STATIC and k == "html":
                    continue
                shutil.copy(res[k], t / res[k].name)
                if t == STATIC:
                    shutil.copy(res[k], t / f"weekly_motion_latest{res[k].suffix}")
    base = targets[-1] if targets else None
    return {"figures": f, "data": data, "name": name, "seconds": res["seconds"],
            "gif": str(base / f"{name}.gif") if base else "", "dir": str(base) if base else ""}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "analytics.sqlite"))
    ap.add_argument("--out-dir")
    ap.add_argument("--no-dashboard", action="store_true")
    a = ap.parse_args()
    try:
        res = build(Path(a.db), a.out_dir, not a.no_dashboard)
    except GateError as exc:
        print(f"[motion] quality gate: {exc}. No GIF written.")
        return 3
    w = res["figures"]["week"]
    print(f"[motion] {res['name']}.gif  week {w['start']} to {w['end']}: occ {w['occ']:.1f}% "
          f"({w['occ_pts']:+.1f} pts), ADR ${w['adr']:.2f} ({w['adr_chg']:+.1f}%), "
          f"RevPAR ${w['revpar']:.2f} ({w['revpar_chg']:+.1f}%), revenue ${w['revenue']:,.0f} ({w['revenue_chg']:+.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
