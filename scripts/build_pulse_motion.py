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
build_pulse_motion.py — the Dana Point PULSE motion recap, built from the whole app's data

One motion clip that walks through the app section by section: the latest hotel week,
2026 to date, revenue per room by month, coastal peers, where visitors come from,
visitor spending, night-of-week occupancy, marketing reach and the outlook.

Real data only
  * Every figure, headline, label and source line comes from the app's own engine:
    pulse_story.build_model + pulse_insights.build_report (year to date, exactly what the
    Insights report shows) and, for the latest hotel week, build_weekly_motion.figures
    (CoStar daily, the same week as the weekly PDF).
  * recaps/templates/pulse_motion.html is layout and animation only.
  * A scene whose data is missing is left out; nothing is filled in. If the latest week
    fails the quality gate the script exits 3 and writes nothing.

Outputs (newest copy is always *_latest.*)
  dashboard/static/motion/pulse_motion_<week end>.mp4 / .gif / .jpg (poster) / .json (figures)
  dashboard/static/motion/pulse_motion_latest.mp4 / .gif / .jpg / .json
  The app serves this folder at /app/static/motion/ (enableStaticServing).
  pulse_motion_<week end>.html  self-contained, plays in a browser (for the artifact)

Usage
  python3 scripts/build_pulse_motion.py [--db PATH] [--out-dir DIR] [--no-dashboard]
Needs: playwright (Chromium), Pillow and ffmpeg.
"""
from __future__ import annotations

import argparse
import ast
import base64
import io
import json
import logging
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard"))
sys.path.insert(0, str(ROOT / "scripts"))
TEMPLATE = ROOT / "recaps" / "templates" / "pulse_motion.html"
STATIC = ROOT / "dashboard" / "static" / "motion"
PHOTO = ROOT / "dashboard" / "assets" / "photos" / "hero_coast.jpg"
LOGO = ROOT / "dashboard" / "assets" / "vdp_logo_nav.svg"
STATES = ROOT / "dashboard" / "assets" / "us_states_albers.json"
VID_FPS, GIF_FPS = 24, 6
GIF_W, GIF_H = 600, 338


class GateError(Exception):
    pass


def _streamlit_stub() -> None:
    """The app modules import streamlit for caching only; give them a no-op when it is absent."""
    try:
        import streamlit  # noqa: F401
        return
    except ImportError:
        pass
    st = types.ModuleType("streamlit")

    class _Cache:
        def __call__(self, *a, **k):
            if a and callable(a[0]) and not k:
                a[0].clear = lambda *x, **y: None
                return a[0]

            def deco(f):
                f.clear = lambda *x, **y: None
                return f
            return deco
    st.cache_data = st.cache_resource = _Cache()
    st.__getattr__ = lambda name: (lambda *a, **k: None)
    sys.modules["streamlit"] = st


def clean(s) -> str:
    """User-facing text: no en or em dashes (ranges read 'to')."""
    s = str(s)
    s = re.sub(r"(\d)\s*[–—]\s*(\d)", r"\1 to \2", s)
    return s.replace("—", ", ").replace("–", " to ")


def _num(v) -> str:
    v = float(v)
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{v:,.0f}"


def _signed(p: str) -> str | None:
    m = re.search(r"([+-]\d[\d.,]*%)", p or "")
    return m.group(1) if m else None


def report_and_week(db: Path):
    _streamlit_stub()
    logging.disable(logging.WARNING)
    import pulse_data as pdx
    import pulse_insights
    import pulse_story
    import build_weekly_motion as bwm
    src = (ROOT / "dashboard" / "app.py").read_text()
    i = src.index("DMA_COORDS: dict[str, tuple[float, float]] = {")
    dma = ast.literal_eval(src[src.index("{", i):src.index("\n}\n", i) + 2])
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        daily = pdx.hotel_daily(conn)
        if daily is None or daily.empty:
            raise GateError("no hotel daily data")
        end = daily["date"].max()
        model = pulse_story.build_model(conn, 12, f"{end.year}-01-01", end.strftime("%Y-%m-%d"),
                                        "Year to date", dma_coords=dma)
        report = pulse_insights.build_report(conn, model, dma, date.today())
        events = conn.execute("SELECT event_name, event_date FROM vdp_events WHERE event_date >= ? "
                              "ORDER BY is_major DESC, event_date LIMIT 20", (date.today().isoformat(),)).fetchall()
    finally:
        conn.close()
    week = bwm.figures(db)
    return report, week, events, bwm


def scene_data(report, wk, events, bwm) -> dict:
    S = {s["id"]: s for s in report.get("sections", [])}
    w = wk["week"]
    a, b = bwm.d(w["start"]), bwm.d(w["end"])
    d = {}
    d["intro"] = {"eyebrow": "Dana Point PULSE · Weekly recap",
                  "title": f"Week ending {bwm.ap_date(b)}",
                  "sub": "Hotels, visitors, spending and marketing for Visit Dana Point",
                  "foot": f"Prepared by GloCon Solutions LLC for Visit Dana Point · Hotel data through {bwm.ap_date(b)}"}

    def kpi(l, v, var):
        g = var
        return {"l": l, "v": v, "d": f"{g[0]} {g[1]}", "fav": True if g[3] == "fav" else (False if g[3] == "unf" else None)}
    top = max(range(len(w["nights"])), key=lambda i: w["nights"][i]["occ"])
    d["week"] = {"eyebrow": f"Latest week · {bwm.ap_range(a, b)}", "headline": bwm.headline(w),
                 "kpis": [kpi("Occupancy", f"{w['occ']:.1f}%", bwm.var(w["occ_pts"], True)),
                          kpi("ADR", f"${w['adr']:,.2f}", bwm.var(w["adr_chg"])),
                          kpi("RevPAR", f"${w['revpar']:,.2f}", bwm.var(w["revpar_chg"])),
                          kpi("Room revenue", bwm.millions(w["revenue"]), bwm.var(w["revenue_chg"]))],
                 "nights": [{"dow": n["dow"], "occ": round(n["occ"], 1), "ly": round(n["occ_ly"], 1), "adr": n["adr"]} for n in w["nights"]],
                 "top": top, "nights_title": f"NIGHT BY NIGHT · BARS {b.year} OCCUPANCY, DASHES SAME WEEKDAY {b.year - 1}, LABELS {b.year} ADR",
                 "src": "Source: CoStar daily, Dana Point market, compared with the same weekdays last year"}

    t = S.get("today")
    if t:
        chain = next((g for g in t.get("glance", []) if g.get("type") == "chain"), None)
        if chain:
            d["ytd"] = {"eyebrow": "2026 to date", "headline": clean(t["headline"]),
                        "steps": [{"v": s_["v"], "l": clean(s_["l"])} for s_ in chain["steps"]],
                        "src": "Source: " + clean(chain.get("src", ""))}
        line = next((c for c in t.get("charts", []) if c.get("type") == "line"), None)
        rp = t.get("numbers", [{}])[0]
        if line and len(line["series"]) >= 2 and _signed(rp.get("p")):
            cur, prior = line["series"][0], line["series"][1]
            d["revpar"] = {"eyebrow": "Revenue per available room by month", "headline": clean(line["title"]),
                           "x": line["x"], "cur": [None if v is None else round(v, 2) for v in cur["values"]],
                           "prior": [None if v is None else round(v, 2) for v in prior["values"]],
                           "cur_name": cur["name"], "prior_name": prior["name"],
                           "key_v": _signed(rp["p"]), "key_l": f"{rp['l']} ({rp['v']}) vs. last year" + (f", {m_.group(1)}" if (m_ := re.search(r"(\w{3}\. \d+ to \w{3}\. \d+, \d{4})", clean(rp['src']))) else ""),
                           "src": "Source: " + clean(rp["src"]) + ". Latest month is partial."}
    wks = S.get("week")
    if wks:
        hb = next((c for c in wks.get("charts", []) if c.get("type") == "hbar"), None)
        if hb and hb.get("rows"):
            rows = sorted(hb["rows"], key=lambda r: -r["v"])
            rank = next(i for i, r in enumerate(rows) if r.get("hi")) + 1
            d["peers"] = {"eyebrow": f"Coastal peers · {clean(hb['sub'])}",
                          "headline": f"Dana Point ranks #{rank} of {len(rows)} coastal markets on revenue per room",
                          "rows": [{"l": r["l"], "v": r["v"], "hi": bool(r.get("hi"))} for r in rows],
                          "rank": f"#{rank} of {len(rows)}", "src": "Source: " + clean(hb["src"])}
    who = S.get("who")
    if who and who.get("map"):
        mp = who["map"]
        paths = json.loads(STATES.read_text())["states"]
        out_state = next((n for n in who.get("numbers", []) if "outside California" in n.get("l", "")), None)
        d["map"] = {"eyebrow": "Who visits", "headline": clean(who["headline"]),
                    "paths": paths, "states": mp["states"], "home": mp["home"],
                    "pins": [{"name": p["name"], "v": round(p["v"], 2), "x": p["x"], "y": p["y"]} for p in mp["pins"]],
                    "list_title": "Share of visitor spending",
                    "note": f"{out_state['v']} of trips start outside California" if out_state else "",
                    "src": "Source: " + clean(mp["src"]).split(". Boundaries")[0]}
    sp = S.get("spend")
    if sp:
        rc = next((g for g in sp.get("glance", []) if g.get("type") == "receipt"), None)
        n0 = sp.get("numbers", [{}])[0]
        if rc and n0.get("v"):
            d["spend"] = {"eyebrow": "Visitor spending", "headline": clean(sp["headline"]),
                          "items": [{"l": i["l"], "v": round(i["v"], 2)} for i in rc["items"]],
                          "total": n0["v"], "total_l": clean(n0["k"]),
                          "chg": clean(n0.get("p", "")), "src": "Source: " + clean(rc["src"])}
    ho, ni = S.get("hotel"), S.get("night")
    if ho and ni:
        tw = next((g for g in ho.get("glance", []) if g.get("type") == "towers"), None)
        nn = next((n for n in ho.get("numbers", []) if "Sunday" in n.get("l", "")), None)
        sc = ni.get("numbers", [{}])[0]
        if tw and nn and sc.get("v"):
            order = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
            rows = sorted(tw["rows"], key=lambda r: order.index(r["l"]) if r["l"] in order else 9)
            d["nights"] = {"eyebrow": "Night of the week, 2026 to date", "headline": clean(ho["dek"]).rstrip("."),
                           "rows": [{"l": r["l"], "v": round(r["v"], 1), "hi": bool(r.get("hi"))} for r in rows],
                           "gap_v": ho["key"]["v"], "gap_l": "Weekend vs. Sunday and Monday",
                           "gap_note": clean(f"{nn['l']} {nn['v']}; {nn['p']}"),
                           "sc_v": sc["v"], "sc_l": clean(sc["l"]), "sc_note": clean(f"{sc['p']} (scenario, PULSE calculation)"),
                           "src": "Source: " + clean(tw["src"])}
    mk = S.get("mkt")
    if mk:
        fn = next((g for g in mk.get("glance", []) if g.get("type") == "funnel"), None)
        roas = next((n for n in mk.get("numbers", []) if "per $1" in n.get("l", "")), None)
        if fn:
            d["mkt"] = {"eyebrow": "Marketing reach", "headline": clean(mk["headline"]),
                        "stages": [{"l": s_["l"], "v": _num(s_["v"])} for s_ in fn["stages"]],
                        "side_v": roas["v"] if roas else "", "side_l": clean(roas["l"]) if roas else "",
                        "side_note": clean(f"{roas['k']}: {roas['p']}") if roas else "",
                        "src": "Source: " + clean(fn["src"]) + (f"; {clean(roas['src'])}" if roas else "")}
    ol = S.get("outlook")
    if ol:
        o = {"eyebrow": "Market outlook", "v": ol["key"]["v"], "l": clean(ol["key"]["l"]) + f", through {bwm.ap_date(b)}",
             "foot": "© 2026 GloCon Solutions LLC · Prepared by John Picou for Visit Dana Point"}
        if events:
            name, ev = events[0]
            evd = datetime.strptime(ev, "%Y-%m-%d").date()
            o["event"] = {"name": name, "date": bwm.ap_date(evd), "days": (evd - date.today()).days,
                          "days_txt": str((evd - date.today()).days)}
        d["outlook"] = o
    return d


def _data_uris(d: dict) -> None:
    from PIL import Image
    img = Image.open(PHOTO).convert("RGB")
    img.thumbnail((1600, 1600))
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=82)
    d["photo"] = "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()
    d["logo"] = "data:image/svg+xml;base64," + base64.b64encode(LOGO.read_bytes()).decode()


def page(d: dict, capture: bool) -> str:
    t = TEMPLATE.read_text()
    inject = (f"<script>window.__CAPTURE__={'true' if capture else 'false'};"
              f"window.PULSE_DATA={json.dumps(d, default=str)};</script>")
    return t.replace("<body>", "<body>\n" + inject, 1)


def render(d: dict, work: Path, name: str) -> dict:
    from PIL import Image
    from playwright.sync_api import sync_playwright
    html = work / "capture.html"
    html.write_text(page(d, True))
    fr = work / "frames"
    fr.mkdir()
    with sync_playwright() as p:
        br = p.chromium.launch()
        pg = br.new_page(viewport={"width": 1280, "height": 720}, device_scale_factor=1)
        pg.goto(html.as_uri())
        pg.wait_for_timeout(500)
        dur = pg.evaluate("window.DUR")
        n = int(dur * VID_FPS)
        for i in range(n):
            pg.evaluate(f"window.renderAt({i / VID_FPS})")
            pg.screenshot(path=str(fr / f"f{i:05d}.png"))
        br.close()
    mp4 = work / f"{name}.mp4"
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-framerate", str(VID_FPS), "-i", str(fr / "f%05d.png"),
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "22", "-preset", "medium",
                    "-movflags", "+faststart", str(mp4)], check=True)
    # poster: the outlook card near the end reads as a summary; first data scene is clearer
    poster = work / f"{name}.jpg"
    Image.open(fr / f"f{int(n * 0.03):05d}.png").convert("RGB").save(poster, "JPEG", quality=86)
    # GIF: 640x360, GIF_FPS, one shared palette
    step = VID_FPS // GIF_FPS
    frames = [Image.open(fr / f"f{i:05d}.png").convert("RGB").resize((GIF_W, GIF_H), Image.LANCZOS) for i in range(0, n, step)]
    picks = [frames[int(len(frames) * k)] for k in (0.05, 0.15, 0.28, 0.4, 0.5, 0.6, 0.72, 0.85, 0.95)]
    sample = Image.new("RGB", (GIF_W, GIF_H * len(picks)))
    for i, im in enumerate(picks):
        sample.paste(im, (0, GIF_H * i))
    pal = sample.quantize(colors=160, method=Image.Quantize.MEDIANCUT)
    q = [im.quantize(palette=pal, dither=Image.Dither.NONE) for im in frames]
    durs = [int(1000 / GIF_FPS)] * len(q)
    durs[-1] = 3000
    gif = work / f"{name}.gif"
    q[0].save(gif, save_all=True, append_images=q[1:], duration=durs, loop=0, optimize=True, disposal=1)
    play = work / f"{name}.html"
    play.write_text(page(d, False))
    return {"mp4": mp4, "gif": gif, "poster": poster, "html": play, "seconds": round(dur, 1)}


def build(db: Path, out_dir: Path | None = None, dashboard: bool = True) -> dict:
    report, wk, events, bwm = report_and_week(db)
    d = scene_data(report, wk, events, bwm)
    end = wk["week"]["end"]
    name = f"pulse_motion_{end}"
    figures = {k: v for k, v in d.items() if k not in ("photo", "logo")}
    if "map" in figures:
        figures["map"] = {k: v for k, v in figures["map"].items() if k != "paths"}
    _data_uris(d)
    with tempfile.TemporaryDirectory() as tmp:
        res = render(d, Path(tmp), name)
        (Path(tmp) / f"{name}.json").write_text(json.dumps(
            {"built": date.today().isoformat(), "data_through": end, "report_period": report.get("period_detail"),
             "scenes": list(figures.keys()), "figures": figures}, indent=2, default=str))
        res["json"] = Path(tmp) / f"{name}.json"
        targets = []
        if dashboard:
            STATIC.mkdir(parents=True, exist_ok=True)
            targets.append(STATIC)
        if out_dir:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            targets.append(Path(out_dir))
        out = {"name": name, "seconds": res["seconds"], "scenes": list(figures.keys()), "figures": figures}
        for tdir in targets:
            for k in ("mp4", "gif", "poster", "json", "html"):
                if tdir == STATIC and k == "html":
                    continue
                shutil.copy(res[k], tdir / res[k].name)
                if tdir == STATIC:
                    shutil.copy(res[k], tdir / f"pulse_motion_latest{res[k].suffix}")
        base = targets[-1] if targets else None
        if base:
            for k in ("mp4", "gif", "poster", "json", "html"):
                out[k] = str(base / res[k].name)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "analytics.sqlite"))
    ap.add_argument("--out-dir")
    ap.add_argument("--no-dashboard", action="store_true")
    a = ap.parse_args()
    try:
        out = build(Path(a.db), Path(a.out_dir) if a.out_dir else None, not a.no_dashboard)
    except Exception as exc:  # noqa: BLE001
        if exc.__class__.__name__ == "GateError":
            print(f"[motion] quality gate: {exc}. Nothing written.")
            return 3
        raise
    print(f"[motion] {out['name']}: {out['seconds']}s, scenes {', '.join(out['scenes'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
