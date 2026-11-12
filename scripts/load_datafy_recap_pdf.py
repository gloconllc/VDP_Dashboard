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
load_datafy_recap_pdf.py — Datafy Report Builder monthly recap PDF → datafy_recap_* tables

Reads every Report Builder export in data/datafy/recap/ (and any
"*Deep Dive*Report*.pdf" left in data/datafy/ top level) and extracts every
data point printed as text: KPI cards, share tables, rankings, demographics,
airports, category spending, spending correlation, and the report filters.

Tables
  datafy_recap_metrics  one row per extracted value (long format)
  datafy_recap_visuals  one row per visual: its window and how many values came out
                        (status 'chart_only' = values are drawn, not printed, so the
                        PDF holds no numbers for it; those come from the card CSVs)

Rules
  * Each value carries the date window printed on its own visual header, never a
    guessed one. Report-level filter dates are kept separately.
  * Values the PDF prints garbled (e.g. "24Days", "230%" with the decimal lost)
    are stored with quality='unreadable' and value NULL, so the quality gate never
    shows them.
  * Upsert keyed on (source_file, page, visual, label, metric): safe to re-run.
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
RECAP_DIR = os.path.join(PROJECT_ROOT, "data", "datafy", "recap")
TOP_DIR = os.path.join(PROJECT_ROOT, "data", "datafy")

SIDEBAR_X = 170          # report-filter sidebar sits left of this
FOOTER_Y = 425           # model / disclaimer footer below this
LINE_TOL = 5.5
DATE_RX = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
NUM_RX = re.compile(r"^(-?\$?-?[\d,]*\.?\d+)(%|K|M|B)?$")
DAYS_TOKEN = {"Days", "Day"}
VS_RX = re.compile(r"^(-?[\d.,]+%?)(Days)?vs$")
SOURCES = ("Geolocation", "Advanced", "Airport")

# Metric names for the standard Datafy visuals, in printed column order.
VISUAL_METRICS = {
    "dma visitation": ["share_of_visitor_days_pct", "change_in_share_pts"],
    "dma spending": ["share_of_spend_pct", "avg_spend_usd"],
    "length of stay by top dmas": ["avg_los_days", "share_pct"],
    "cluster visitation": ["share_of_visitor_days_pct", "change_in_share_pts"],
    "category spending": ["share_of_spend_pct"],
    "top journey origin airports": ["share_of_passengers_pct"],
    "dma trends by visitor days": ["share_of_visitor_days_pct"],
}


TICK_RX = re.compile(r"^\$?[\d.]+[KMB]$")
MONTHS = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"}


def token_kind(t, nxt):
    pv = parse_value(t)
    if not pv:
        return "label", None
    v, u = pv
    if nxt in DAYS_TOKEN:
        return "value", (v, "days")
    if u == "number" and float(v).is_integer() and 0 < v <= 25 and nxt and not parse_value(nxt) and nxt not in DAYS_TOKEN:
        return "rank", v
    return "value", (v, u)


def page_has_visitor_days_header(words, a):
    return any(w["text"] == "Visitor" and a["x0"] - 10 <= w["x0"] and a["top"] < w["top"] < a["top"] + 30 for w in words)


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(status: str, msg: str) -> None:
    print(f"[{ts()}] [{status:<4}] {msg}", flush=True)


def to_iso(d: str) -> str | None:
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(d, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def parse_value(tok: str):
    """Return (value, unit) or None."""
    t = tok.strip()
    m = NUM_RX.match(t.replace("$-", "-$"))
    if not m:
        return None
    raw, suf = m.group(1), m.group(2)
    neg = raw.startswith("-")
    num = raw.replace("-", "").replace("$", "").replace(",", "")
    try:
        v = float(num)
    except ValueError:
        return None
    v = -v if neg else v
    if suf == "%":
        return v, "pct"
    if suf in ("K", "M", "B"):
        return None  # axis tick, not a data point
    if "$" in raw:
        return v, "usd"
    return v, "number"


def lines_of(words):
    words = sorted(words, key=lambda w: (round(w["top"]), w["x0"]))
    out = []
    for w in words:
        if out and abs(out[-1]["top"] - w["top"]) <= LINE_TOL:
            out[-1]["words"].append(w)
        else:
            out.append({"top": w["top"], "words": [w]})
    for ln in out:
        ln["words"].sort(key=lambda w: w["x0"])
        ln["text"] = " ".join(w["text"] for w in ln["words"])
    return out


def find_anchors(words):
    """Visual anchors = 'Geolocation|Advanced Spending|Airport [d - d]' phrases
    and date ranges inside the visual area. Each anchor starts a column."""
    anchors = []
    body = [w for w in words if w["x0"] >= SIDEBAR_X and w["top"] < FOOTER_Y]
    for ln in lines_of(body):
        ws = ln["words"]
        if any("%" in w["text"] or "(" in w["text"] or "$" in w["text"] for w in ws):
            continue
        i = 0
        while i < len(ws):
            t = ws[i]["text"]
            if t in SOURCES:
                src = "Advanced Spending" if t == "Advanced" else t
                j = i + (2 if t == "Advanced" else 1)
                start = end = None
                if j + 2 < len(ws) + 0 and j < len(ws) and DATE_RX.match(ws[j]["text"]) and j + 2 < len(ws) and DATE_RX.match(ws[j + 2]["text"]):
                    start, end = to_iso(ws[j]["text"]), to_iso(ws[j + 2]["text"])
                    j += 3
                anchors.append({"x0": ws[i]["x0"], "top": ln["top"], "source": src, "start": start, "end": end})
                i = j
                continue
            if DATE_RX.match(t) and i + 2 < len(ws) and ws[i + 1]["text"] == "-" and DATE_RX.match(ws[i + 2]["text"]):
                anchors.append({"x0": ws[i]["x0"] - 20, "top": ln["top"], "source": None,
                                "start": to_iso(t), "end": to_iso(ws[i + 2]["text"])})
                i += 3
                continue
            i += 1
    return anchors


def page_title(lines):
    top = [l for l in lines if l["top"] < 70 and min(w["x0"] for w in l["words"]) < SIDEBAR_X + 200]
    return " ".join(l["text"] for l in top).strip()


def sidebar_filters(words):
    side = [w for w in words if w["x0"] < SIDEBAR_X and 65 < w["top"] < FOOTER_Y]
    return " | ".join(l["text"] for l in lines_of(side))


def visual_titles(lines, anchors):
    """Title of each anchor column = the text line(s) just above the question line."""
    titles = {}
    for a in anchors:
        cands = [l for l in lines if a["top"] - 40 < l["top"] < a["top"] - 2]
        best = None
        for l in sorted(cands, key=lambda l: l["top"]):
            seg = [w for w in l["words"] if a["x0"] - 25 <= w["x0"] < a["x0"] + 200]
            txt = " ".join(w["text"] for w in seg).strip()
            if txt and not txt.endswith("?") and not DATE_RX.match(seg[0]["text"]) \
                    and seg[0]["text"] not in ("What", "Where", "Which", "How", "When", "Of", "visitors?") \
                    and seg[0]["text"] not in SOURCES and not txt.lower().startswith(("dma share", "cluster share", "category share")):
                best = txt
                break
        titles[id(a)] = best
    return titles


def extract_page(pg, pno, fname):
    words = pg.extract_words(keep_blank_chars=False)
    all_lines = lines_of(words)
    title = page_title(all_lines)
    anchors = find_anchors(words)
    rows, visuals = [], []
    if not anchors:
        # KPI cards without a source anchor (rare) or a divider/glossary page
        return title, rows, visuals
    titles = visual_titles(all_lines, anchors)
    anchors.sort(key=lambda a: a["x0"])
    # Group anchors that sit on different rows (stacked visuals) by top band
    bands = {}
    for a in anchors:
        key = min(bands.keys(), key=lambda k: abs(k - a["top"]), default=None)
        if key is None or abs(key - a["top"]) > 25:
            bands[a["top"]] = [a]
        else:
            bands[key].append(a)
    band_tops = sorted(bands)
    body = [w for w in words if w["x0"] >= SIDEBAR_X and w["top"] < FOOTER_Y]

    def anchor_for(w):
        # nearest band above the word, then the column within that band
        above = [b for b in band_tops if b <= w["top"] + 2]
        if not above:
            return None
        band = bands[above[-1]]
        cols = [a for a in band if a["x0"] <= w["x0"] + 15]
        return max(cols, key=lambda a: a["x0"]) if cols else None

    # Every line in the visual area is cut into segments of [label words][value words];
    # a segment belongs to the column whose anchor sits left of its first value, so
    # labels that run across a column edge stay with their own number.
    recs_by_anchor = {id(a): [] for a in anchors}
    pending = {}
    first = band_tops[0]
    for ln in lines_of(body):
        toks = [w for w in ln["words"] if not TICK_RX.match(w["text"])]
        if not toks:
            continue
        # header for KPI cards (ALL CAPS) is kept until its value arrives
        if ln["text"].isupper() and not any(parse_value(w["text"]) for w in toks):
            for w in toks:
                a = anchor_for(w)
                if a is not None:
                    pending[id(a)] = ln["text"].title() if len([x for x in anchors if abs(x["top"]-a["top"])<=25]) == 1 else None
            # split headers per card by gaps
            cards, cur = [], [toks[0]]
            for w in toks[1:]:
                (cards.append(cur), cur := [w]) if w["x0"] - cur[-1]["x1"] > 40 else cur.append(w)
            cards.append(cur)
            for c in cards:
                a = anchor_for(c[0])
                if a is not None:
                    pending[("card", round(c[0]["x0"]))] = (" ".join(w["text"] for w in c).title(), a)
            continue
        segs, cur_l, cur_v, rank = [], [], [], None
        i = 0
        depth = 0
        while i < len(toks):
            t = toks[i]["text"]
            if depth or t.startswith("("):
                depth = max(0, depth + t.count("(") - t.count(")"))
                if cur_v:
                    segs.append(("rec", cur_l, cur_v, rank)); cur_l, cur_v, rank = [], [], None
                cur_l.append(toks[i]); i += 1; continue
            nxt = toks[i + 1]["text"] if i + 1 < len(toks) else None
            if t == "Daysvs" or VS_RX.match(t):
                m = VS_RX.match(t)
                if t == "Daysvs" and cur_v:
                    v = cur_v.pop(); segs.append(("vs", toks[i], (v[1][0], "days")))
                elif m and parse_value(m.group(1)):
                    pv = parse_value(m.group(1)); segs.append(("vs", toks[i], (pv[0], "days" if m.group(2) else pv[1])))
                i += 1
                if nxt == "Compare":
                    i += 2
                continue
            kind, val = token_kind(t, nxt)
            if kind == "value" and cur_v and toks[i]["x0"] - cur_v[-1][0]["x1"] > 70:
                segs.append(("rec", cur_l, cur_v, rank)); cur_l, cur_v, rank = [], [], None
            if kind == "value":
                cur_v.append((toks[i], val)); i += 2 if val[1] == "days" and nxt in DAYS_TOKEN else 1
                continue
            if kind == "rank" and not cur_l and not cur_v:
                rank = int(val); i += 1; continue
            if cur_v:
                segs.append(("rec", cur_l, cur_v, rank)); cur_l, cur_v, rank = [], [], None
            cur_l.append(toks[i]); i += 1
        if cur_v:
            segs.append(("rec", cur_l, cur_v, rank))
        elif cur_l:
            segs.append(("label", cur_l))
        for sg in segs:
            if sg[0] == "vs":
                a = anchor_for(sg[1])
                if a is not None and recs_by_anchor[id(a)]:
                    recs_by_anchor[id(a)][-1]["vs"] = sg[2]
                continue
            if sg[0] == "label":
                lab = " ".join(w["text"] for w in sg[1])
                a = anchor_for(sg[1][0])
                if a is None or lab.endswith("?"):
                    continue
                rl = recs_by_anchor[id(a)]
                if rl and 0 < ln["top"] - rl[-1]["top"] <= 13 and abs(sg[1][0]["x0"] - rl[-1]["x0"]) < 30:
                    rl[-1]["label"] = (rl[-1]["label"] + " " + lab).strip()
                continue
            _, lw, vw, rk = sg
            a = anchor_for(vw[0][0])
            if a is None or vw[0][0]["top"] <= a["top"] + 1:
                continue
            lab = " ".join(w["text"] for w in lw)
            if not lw or lab in ("Trips",):
                key = min((k for k in pending if isinstance(k, tuple)), key=lambda k: abs(k[1] - vw[0][0]["x0"]), default=None)
                if key is None or abs(key[1] - vw[0][0]["x0"]) > 60:
                    continue
                lab = pending.pop(key)[0]
            if set(lab.replace("'26", "").split()) <= MONTHS:
                continue
            vals = [(v[0], v[1], "ok") for _, v in vw]
            vals = [(None, u, "unreadable") if (u == "pct" and abs(x) > 100) else (x, u, q) for x, u, q in vals]
            lab = re.sub(r"\s+", " ", lab).replace("Sacramnto", "Sacramento").replace("Francisco-Oak- San Jose", "Francisco-Oak-San Jose")
            if re.fullmatch(r"[A-Z][a-z]+[A-Z][A-Za-z]+", lab) or re.search(r"\b(\w) \1 ", lab):
                vals = [(None, u, "unreadable") for _, u, _ in vals]
            x0 = (lw[0] if lw else vw[0][0])["x0"]
            recs_by_anchor[id(a)].append({"label": lab, "values": vals, "rank": rk, "top": ln["top"], "x0": x0})

    for a in anchors:
        vt = titles.get(id(a))
        if a["source"] is None:
            band = [b for b in anchors if abs(b["top"] - a["top"]) <= 25 and b["source"]]
            base = next((titles.get(id(b)) for b in band if titles.get(id(b))), None) or "DMA Trends by Visitor Days"
            vt = f"{base} ({a['start']} to {a['end']})"
            names = ["share_of_visitor_days_pct"]
        else:
            vt = vt or title
            names = VISUAL_METRICS.get((vt or "").lower().strip())
        recs = recs_by_anchor[id(a)]
        if (vt or "").lower().startswith("length of stay") and page_has_visitor_days_header(words, a):
            names = ["avg_los_days", "share_of_visitor_days_pct"]
        n_ok = 0
        for r in recs:
            per_unit = {}
            for idx, (val, unit, quality) in enumerate(r["values"]):
                if names and idx < len(names):
                    metric = names[idx]
                elif idx == 1 and unit == "pct" and r["values"][0][1] == "pct" and abs(val or 0) < 5:
                    metric = "change_vs_compare"
                else:
                    per_unit[unit] = per_unit.get(unit, 0) + 1
                    metric = {"pct": "share_pct", "usd": "value_usd", "days": "days",
                              "number": "value"}[unit] + ("" if per_unit[unit] == 1 else f"_{per_unit[unit]}")
                rows.append(dict(source_file=fname, page=pno, section=title, visual=vt,
                                 data_source=a["source"], label=r["label"], rank=r.get("rank"),
                                 metric=metric, value=val if quality == "ok" else None,
                                 unit=unit, quality=quality, period_start=a["start"], period_end=a["end"]))
                n_ok += quality == "ok"
            if r.get("vs") is not None:
                rows.append(dict(source_file=fname, page=pno, section=title, visual=vt, data_source=a["source"],
                                 label=r["label"], rank=r.get("rank"), metric="change_vs_compare",
                                 value=r["vs"][0], unit=r["vs"][1], quality="ok",
                                 period_start=a["start"], period_end=a["end"]))
        visuals.append(dict(source_file=fname, page=pno, section=title, visual=vt, data_source=a["source"],
                            period_start=a["start"], period_end=a["end"], values_extracted=n_ok,
                            status="ok" if n_ok else "chart_only"))
    return title, rows, visuals


def classify_tokens(ws):
    """Split a line into label tokens and value tokens."""
    label, vals, rank = [], [], None
    i = 0
    depth = 0
    while i < len(ws):
        t = ws[i]["text"]
        if depth or t.startswith("("):
            depth += t.count("(") - t.count(")")
            label.append(t); i += 1; continue
        nxt = ws[i + 1]["text"] if i + 1 < len(ws) else None
        pv = parse_value(t)
        if i == 0 and pv and pv[1] == "number" and float(pv[0]).is_integer() and 0 < pv[0] <= 25 and nxt and not parse_value(nxt):
            rank = int(pv[0]); i += 1; continue
        if pv and nxt in DAYS_TOKEN:
            vals.append((pv[0], "days", "ok")); i += 2; continue
        if re.match(r"^\d+Days$", t):
            vals.append((None, "days", "unreadable")); i += 1; continue
        if pv:
            v, u = pv
            q = "unreadable" if (u == "pct" and abs(v) > 100) else "ok"
            vals.append((v, u, q)); i += 1; continue
        label.append(t); i += 1
    return label, vals, rank


def extract_column(ws):
    recs = []
    pending = None
    for ln in lines_of(ws):
        toks = ln["words"]
        if len(toks) >= 2 and toks[1]["text"] == "Daysvs" and parse_value(toks[0]["text"]):
            if recs:
                recs[-1]["vs"] = (parse_value(toks[0]["text"])[0], "days")
            continue
        m = VS_RX.match(toks[0]["text"]) if toks else None
        if m:
            if recs:
                v = parse_value(m.group(1))
                if v:
                    recs[-1]["vs"] = (v[0], "days" if m.group(2) else v[1])
            continue
        label, vals, rank = classify_tokens(toks)
        text = " ".join(label)
        if not vals and text.isupper():
            pending = text.title()
            continue
        if vals and (not label or label == ["Trips"]) and pending:
            recs.append({"label": pending, "values": vals, "rank": rank, "top": ln["top"], "x0": toks[0]["x0"]})
            pending = None
            continue
        if not vals:
            # label-only line: continuation of a wrapped label ("San Jose", "Modesto")
            if recs and label and ln["top"] - recs[-1]["top"] <= 12 and not text.endswith("?") \
                    and abs(toks[0]["x0"] - recs[-1]["x0"]) < 8:
                recs[-1]["label"] = (recs[-1]["label"] + " " + text).strip()
            continue
        if not label:
            continue  # axis ticks / legends carry no label
        if label[-1] in ("Trips",) and len(label) == 1:
            continue
        lab = re.sub(r"\s+", " ", text).replace("Sacramnto", "Sacramento")
        lab = lab.replace("Francisco-Oak- San Jose", "Francisco-Oak-San Jose")
        if re.fullmatch(r"[A-Za-z]+[A-Z][a-z]+[A-Za-z]*", lab) and lab not in ("Resorts",):
            vals = [(None, u, "unreadable") for _, u, _ in vals]
        recs.append({"label": lab, "values": vals, "rank": rank, "top": ln["top"], "x0": toks[0]["x0"]})
    return recs


def extract_kpi_cards(ws, pno, fname, title, anchors):
    rows = []
    src = anchors[0] if anchors else {"source": None, "start": None, "end": None}
    lns = lines_of(ws)
    headers = [l for l in lns if l["text"].isupper()]
    for h in headers:
        for hw_i, hw in enumerate(h["words"]):
            pass
        # split header words into cards by gaps > 40px
        cards, cur = [], [h["words"][0]]
        for w in h["words"][1:]:
            if w["x0"] - cur[-1]["x1"] > 40:
                cards.append(cur); cur = [w]
            else:
                cur.append(w)
        cards.append(cur)
        for c in cards:
            name = " ".join(w["text"] for w in c)
            x0 = c[0]["x0"] - 15
            below = [w for w in ws if w["top"] > h["top"] + 2 and w["top"] < h["top"] + 30 and x0 <= w["x0"] < x0 + 180]
            bl = lines_of(below)
            for ln in bl:
                toks = [w["text"] for w in ln["words"]]
                if VS_RX.match(toks[0]):
                    v = parse_value(VS_RX.match(toks[0]).group(1))
                    if v:
                        rows.append(dict(source_file=fname, page=pno, section=title, visual="KPI cards", data_source=src["source"],
                                         label=name.title(), rank=None, metric="change_vs_compare", value=v[0],
                                         unit="days" if "Days" in toks[0] else v[1], quality="ok",
                                         period_start=src["start"], period_end=src["end"]))
                    continue
                pv = parse_value(toks[0])
                if pv:
                    unit = "days" if len(toks) > 1 and toks[1] in DAYS_TOKEN else pv[1]
                    rows.append(dict(source_file=fname, page=pno, section=title, visual="KPI cards", data_source=src["source"],
                                     label=name.title(), rank=None, metric="value", value=pv[0], unit=unit,
                                     quality="ok", period_start=src["start"], period_end=src["end"]))
    return rows


def extract_correlation(pg, pno, fname, title):
    """Spending Correlation: category names on one band, percentages below."""
    text = pg.extract_text() or ""
    m = re.search(r"Category:\s*([A-Za-z ,&]+?),\s*which", text)
    base = m.group(1).strip() if m else None
    pcts = re.findall(r"(\d+\.\d+)%", text)
    words = [w for w in pg.extract_words() if w["x0"] >= SIDEBAR_X and w["top"] < FOOTER_Y]
    pct_words = [w for w in words if re.fullmatch(r"\d+\.\d+%", w["text"])]
    if not pct_words:
        return []
    ytop = min(w["top"] for w in pct_words)
    name_words = [w for w in words if ytop - 30 < w["top"] < ytop - 1]
    rows = []
    for pw in pct_words:
        col = [w for w in name_words if pw["x0"] - 8 <= w["x0"] < pw["x0"] + 55]
        name = " ".join(w["text"] for w in sorted(col, key=lambda w: (round(w["top"]), w["x0"])))
        rows.append(dict(source_file=fname, page=pno, section=title, visual="Average Spending Correlation",
                         data_source="Advanced Spending", label=name or None, rank=None,
                         metric=f"co_spend_with_{(base or 'base').lower().replace(' ', '_')}_pct",
                         value=float(pw["text"].rstrip("%")), unit="pct", quality="ok",
                         period_start=None, period_end=None))
    return rows


SCHEMA = """
CREATE TABLE IF NOT EXISTS datafy_recap_metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_file TEXT, page INTEGER, section TEXT, visual TEXT, data_source TEXT,
  label TEXT, rank INTEGER, metric TEXT, value REAL, unit TEXT, quality TEXT,
  period_start TEXT, period_end TEXT, report_filters TEXT, loaded_at TEXT,
  UNIQUE(source_file, page, visual, label, metric)
);
CREATE TABLE IF NOT EXISTS datafy_recap_visuals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_file TEXT, page INTEGER, section TEXT, visual TEXT, data_source TEXT,
  period_start TEXT, period_end TEXT, values_extracted INTEGER, status TEXT,
  report_filters TEXT, loaded_at TEXT,
  UNIQUE(source_file, page, visual, period_start)
);
"""


def fill_periods(path, rows, vis):
    """Visuals without a printed window take the report-level dates for their
    source from the Filters Summary page (Advanced Spending, Airport)."""
    import pdfplumber
    txt = ""
    with pdfplumber.open(path) as pdf:
        for pg in pdf.pages:
            t = pg.extract_text() or ""
            if t.strip().startswith("Filters Summary"):
                txt = t
                break
    found = {}
    for src, key in (("Advanced Spending", r"Advanced Spending Dates:\s*(\S+)\s*-\s*(\S+)"),
                     ("Airport", r"Airport Dates:\s*(\S+)\s*-\s*(\S+)")):
        m = re.search(key, txt)
        if m:
            found[src] = (to_iso(m.group(1)), to_iso(m.group(2)))
    for r in list(rows) + list(vis):
        if not r.get("period_start") and r.get("data_source") in found:
            r["period_start"], r["period_end"] = found[r["data_source"]]
            r["period_basis"] = "report filter"


def recap_files():
    out = []
    if os.path.isdir(RECAP_DIR):
        out += [os.path.join(RECAP_DIR, f) for f in sorted(os.listdir(RECAP_DIR)) if f.lower().endswith(".pdf")]
    out += [os.path.join(TOP_DIR, f) for f in sorted(os.listdir(TOP_DIR))
            if f.lower().endswith(".pdf") and re.search(r"deep dive|recap|report builder", f, re.I)]
    return out


def main() -> int:
    try:
        import pdfplumber
    except ImportError:
        log("WARN", "pdfplumber not installed; skipping Datafy recap PDF")
        return 0
    files = recap_files()
    if not files:
        log("SKIP", "no Datafy recap PDF in data/datafy/recap/")
        return 0
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    now = ts()
    total = 0
    for path in files:
        fname = os.path.basename(path)
        rows_all, vis_all = [], []
        with pdfplumber.open(path) as pdf:
            for pno, pg in enumerate(pdf.pages, start=1):
                text = pg.extract_text() or ""
                if re.search(r"^(Glossary|Filters Summary)", text.strip()) or len(text) < 120:
                    continue
                filters = sidebar_filters(pg.extract_words())
                if "Spending Correlation" in text.split("\n")[0]:
                    rows = extract_correlation(pg, pno, fname, "Spending Correlation")
                    vis = [dict(source_file=fname, page=pno, section="Spending Correlation",
                                visual="Average Spending Correlation", data_source="Advanced Spending",
                                period_start=None, period_end=None, values_extracted=len(rows),
                                status="ok" if rows else "chart_only")]
                else:
                    _, rows, vis = extract_page(pg, pno, fname)
                for r in rows:
                    r["report_filters"] = filters
                for v in vis:
                    v["report_filters"] = filters
                rows_all += rows
                vis_all += vis
        fill_periods(path, rows_all, vis_all)
        # Labels Datafy prints letter-spaced ("S a n F ra n cisco") are matched
        # back to the same name printed elsewhere in the report.
        known = {re.sub(r"[\s.]", "", r["label"]).lower(): r["label"] for r in rows_all
                 if r["label"] and not re.search(r"\b\w \w \w\b", r["label"])}
        for r in rows_all:
            if r["label"] and re.search(r"\b\w \w \w\b", r["label"]):
                r["label"] = known.get(re.sub(r"[\s.]", "", r["label"]).lower(), r["label"])
        conn.execute("DELETE FROM datafy_recap_metrics WHERE source_file=?", (fname,))
        conn.execute("DELETE FROM datafy_recap_visuals WHERE source_file=?", (fname,))
        conn.executemany("""INSERT OR REPLACE INTO datafy_recap_metrics
            (source_file,page,section,visual,data_source,label,rank,metric,value,unit,quality,period_start,period_end,report_filters,loaded_at)
            VALUES (:source_file,:page,:section,:visual,:data_source,:label,:rank,:metric,:value,:unit,:quality,:period_start,:period_end,:report_filters,'%s')""" % now,
            rows_all)
        conn.executemany("""INSERT OR REPLACE INTO datafy_recap_visuals
            (source_file,page,section,visual,data_source,period_start,period_end,values_extracted,status,report_filters,loaded_at)
            VALUES (:source_file,:page,:section,:visual,:data_source,:period_start,:period_end,:values_extracted,:status,:report_filters,'%s')""" % now,
            vis_all)
        try:
            conn.execute("INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at) VALUES (?,?,?,?,?)",
                         ("datafy_recap", "report", fname, len(rows_all), now))
        except sqlite3.Error:
            pass
        conn.commit()
        ok = sum(1 for r in rows_all if r["quality"] == "ok")
        bad = len(rows_all) - ok
        chart = sum(1 for v in vis_all if v["status"] == "chart_only")
        log("OK", f"{fname}: {ok} values, {bad} unreadable, {len(vis_all)} visuals ({chart} chart-only)")
        total += len(rows_all)
    conn.close()
    log("DONE", f"datafy_recap_metrics: {total} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
