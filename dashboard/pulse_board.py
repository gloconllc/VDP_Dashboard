"""
pulse_board.py, the Dana Point PULSE board view drawn as one custom surface.

Nothing here computes a new number. Every figure, label, and sentence comes
from the Model that pulse_story.build_model() already builds for the selected
data window (pulse_data.py does the math, pulse_story.py writes the answers).
This module only packages that Model into plain JSON and hands it to a
Streamlit components.v2 component (pulse_board.js + pulse_board.css), which
draws the page by hand: a photo masthead with the headline numbers, the four
answers for the board, and the four sections with hand-built SVG charts that
all carry hover detail.

The page is mounted in chunks (brief, then one per section) so app.py can keep
each section's anchor and its "Ask about this section" AI card exactly where
they were. If the component API is unavailable, app.py falls back to the
Streamlit-native renderers in pulse_sections.py.
"""

from __future__ import annotations

import base64
import math
import os

import pandas as pd
import streamlit as st

import pulse_data as pdx
import pulse_story as ps

_HERE = os.path.dirname(os.path.abspath(__file__))
_PHOTO = os.path.join(_HERE, "assets", "photos", "hero_coast_board.jpg")
_LOGO = os.path.join(_HERE, "assets", "vdp_logo_nav.svg")

DP_LAT, DP_LON = 33.467, -117.698

SEC_META = {
    "snapshot": (1, "pulse-snapshot", "Hotel performance · STR", "Performance Snapshot"),
    "visitors": (2, "pulse-origins", "Visitor economy · Datafy", "Visitors and Spend"),
    "market": (3, "pulse-market", "Competitive position · CoStar and STR", "Market Position"),
    "forward": (4, "pulse-forward", "What is ahead · STR and events calendar", "Forward Outlook"),
}
KICKER = {"hotel": "Hotel performance", "market": "Market position", "visitors": "Visitors", "calendar": "Coming up"}


# ---------------------------------------------------------------------------
# Component registration (once per process)
# ---------------------------------------------------------------------------

def _read(name: str) -> str:
    with open(os.path.join(_HERE, name), "r", encoding="utf-8") as fh:
        return fh.read()


def _data_uri(path: str, mime: str) -> str:
    try:
        with open(path, "rb") as fh:
            return f"data:{mime};base64," + base64.b64encode(fh.read()).decode("ascii")
    except OSError:
        return ""


@st.cache_resource(show_spinner=False)
def _component():
    css = _read("pulse_board.css")
    # The masthead photo and logo ship with the component's stylesheet rather
    # than with every rerun's data, so they are sent once, not on each filter change.
    photo = _data_uri(_PHOTO, "image/jpeg")
    if photo:
        css += "\n.pb-brief { --photo: url('" + photo + "'); }\n"
    logo = _data_uri(_LOGO, "image/svg+xml")
    if logo:
        css += "\n.pb-logo { background-image: url('" + logo + "'); }\n"
    return st.components.v2.component("pulse_board", css=css, js=_read("pulse_board.js"))


def available() -> bool:
    try:
        return hasattr(st, "components") and hasattr(st.components, "v2") and _component() is not None
    except Exception:  # noqa: BLE001
        return False


def mount(payload: dict, key: str) -> None:
    _component()(key=key, data=payload)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _f(v, nd: int | None = 2):
    try:
        if v is None or pd.isna(v):
            return None
        v = float(v)
        if math.isinf(v):
            return None
        return round(v, nd) if nd is not None else v
    except (TypeError, ValueError):
        return None


def _list(df: pd.DataFrame | None, col: str, nd: int | None = 2) -> list:
    if df is None or df.empty or col not in df:
        return []
    return [_f(x, nd) for x in df[col]]


def _tone(v, tol: float = 0.05):
    v = _f(v, None)
    if v is None:
        return None
    return "up" if v > tol else "down" if v < -tol else "flat"


def _delta(text: str | None, v, tol: float = 0.05) -> dict:
    if text is None or _f(v, None) is None:
        return {"text": "No prior-year match", "tone": "none"}
    return {"text": text, "tone": _tone(v, tol)}


def _label_only(text: str) -> dict:
    return {"text": text, "tone": "none"}


def _kpi(label, value, num, fmt, delta, foot, spark=None) -> dict:
    return {"label": label, "value": value, "num": _f(num, 4), "fmt": fmt, "delta": delta, "foot": foot,
            "spark": [x for x in (spark or []) if x is not None] or None}


def _src_hotel(m: ps.Model) -> str:
    f = m.fresh
    s = "Source: STR daily, VDP Select comp set"
    if f.get("costar_extends") and f.get("str_through") is not None and m.hotel and m.hotel.get("costar_days"):
        s += f", extended past {pdx.fmt_date(f['str_through'])} with CoStar's daily export for the same hotels"
    return s + ". Rooms-weighted."


def _bearing(lat: float, lon: float) -> float:
    p1, p2 = math.radians(DP_LAT), math.radians(lat)
    dl = math.radians(lon - DP_LON)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _section(name: str, m: ps.Model) -> dict:
    n, anchor, eyebrow, title = SEC_META[name]
    t = m.text.get(name, {})
    return {"kind": "section", "name": name, "n": n, "anchor": anchor, "eyebrow": eyebrow, "title": title,
            "question": t.get("question", ""), "answer": t.get("answer", ""), "kpis": [], "cards": []}


# ---------------------------------------------------------------------------
# Brief: masthead, headline numbers, four answers, freshness
# ---------------------------------------------------------------------------

def _fresh(m: ps.Model) -> list[dict]:
    f = m.fresh
    today = pd.Timestamp.now().normalize()
    out = []
    if f.get("hotel_through") is not None:
        txt = f"through {pdx.fmt_date(f['hotel_through'])}"
        if f.get("costar_extends") and f.get("str_through") is not None:
            txt += f" (STR to {pdx.fmt_date(f['str_through'], False)}, CoStar after)"
        out.append({"label": "Hotels", "text": txt, "ok": (today - pd.Timestamp(f["hotel_through"])).days <= 35})
    if f.get("str_week"):
        out.append({"label": "STR weekly", "text": f"week ending {pdx.fmt_date(f['str_week'])}",
                    "ok": (today - pd.Timestamp(f["str_week"])).days <= 21})
    if f.get("costar_report"):
        pull = f.get("costar_pull") or f["costar_report"]
        out.append({"label": "CoStar", "text": f"pulled {pdx.fmt_date(pull)}",
                    "ok": (today - pd.Timestamp(pull)).days <= 10})
    if f.get("datafy_end"):
        out.append({"label": "Datafy", "text": f"{pdx.fmt_month(f['datafy_start'])} to {pdx.fmt_month(f['datafy_end'])}",
                    "ok": (today - pd.Timestamp(f["datafy_end"])).days <= 62})
    return out


def brief(m: ps.Model) -> dict:
    h, tr, d = m.hotel or {}, m.trend, m.dfy or {}
    glance = m.text.get("glance", []) or []
    kpis = []
    if h:
        kpis.append(_kpi("RevPAR", ps.usd(h["revpar"]), h["revpar"], "usd0",
                         _delta(ps.spct(h.get("revpar_pct")) + " vs last year" if h.get("revpar_pct") is not None else None, h.get("revpar_pct")),
                         "Room revenue per available room", _list(tr, "revpar")))
        kpis.append(_kpi("Occupancy", ps.pct(h["occ"]), h["occ"], "pct1",
                         _delta(ps.pts(h.get("occ_pts")) + " vs last year" if h.get("occ_pts") is not None else None, h.get("occ_pts")),
                         "Share of rooms sold", _list(tr, "occ")))
        kpis.append(_kpi("Average daily rate", ps.usd(h["adr"]), h["adr"], "usd0",
                         _delta(ps.spct(h.get("adr_pct")) + " vs last year" if h.get("adr_pct") is not None else None, h.get("adr_pct")),
                         "Room revenue per room sold", _list(tr, "adr")))
    if d.get("spend") is not None:
        span = pdx.fmt_month(d["first"]) + ("" if d["n_months"] == 1 else f" to {pdx.fmt_month(d['last'])}")
        kpis.append(_kpi("Visitor spending", ps.usd_big(d["spend"]), d["spend"], "usdbig",
                         _delta((ps.spct(d.get("spend_pct")) + (" vs same month last year" if d.get("fallback") else " vs last year"))
                                if d.get("spend_pct") is not None else None, d.get("spend_pct")),
                         f"Datafy, {span}" + (" (latest month on file)" if d.get("fallback") else ""),
                         _list(m.dfy_trend, "spending_usd", 0)))
    span = (f"{pdx.fmt_date(h['start'])} to {pdx.fmt_date(h['end'])}" if h.get("start") is not None
            else m.window.span_text())
    head = glance[0] if glance else {}
    cards = [{"icon": g.get("icon", "hotel"), "kicker": KICKER.get(g.get("icon"), ""), "title": g.get("title", ""),
              "body": g.get("body", ""), "anchor": g.get("anchor", "")} for g in glance]
    return {
        "kind": "brief",
        "eyebrow": f"Board brief · {m.window.label}",
        "span": span,
        "headline": head.get("title") or "Dana Point PULSE",
        "dek": head.get("body") or "",
        "kpis": kpis,
        "cards": cards,
        "cardsTitle": "Four answers for the board",
        "cardsSub": "Computed from STR, CoStar, and Datafy for the window above. Select a card to jump to its detail.",
        "fresh": _fresh(m),
    }


# ---------------------------------------------------------------------------
# 1. Performance snapshot
# ---------------------------------------------------------------------------

def snapshot(m: ps.Model) -> dict:
    sec = _section("snapshot", m)
    t, h, tr = m.text["snapshot"], m.hotel, m.trend
    if not h:
        sec["empty"] = "STR hotel data is not available for this window yet."
        return sec
    prior = h.get("prior") or {}
    d80_delta = (h["d80"] - prior["d80"]) if prior.get("d80") is not None and h.get("method") == "daily" else None
    sec["kpis"] = [
        _kpi("Occupancy", ps.pct(h["occ"]), h["occ"], "pct1",
             _delta(ps.pts(h.get("occ_pts")) + " vs last year" if h.get("occ_pts") is not None else None, h.get("occ_pts")),
             "Rooms sold / rooms available", _list(tr, "occ")),
        _kpi("ADR", ps.usd(h["adr"]), h["adr"], "usd0",
             _delta(ps.spct(h.get("adr_pct")) + " vs last year" if h.get("adr_pct") is not None else None, h.get("adr_pct")),
             "Room revenue / rooms sold", _list(tr, "adr")),
        _kpi("RevPAR", ps.usd(h["revpar"]), h["revpar"], "usd0",
             _delta(ps.spct(h.get("revpar_pct")) + " vs last year" if h.get("revpar_pct") is not None else None, h.get("revpar_pct")),
             "Room revenue / rooms available", _list(tr, "revpar")),
        _kpi("Nights at 80%+", f"{h.get('d80', 0)}", h.get("d80", 0), "num0",
             _delta(f"{d80_delta:+d} nights vs last year" if d80_delta is not None else None, d80_delta, 0.5),
             f"{h.get('d90', 0)} of them at 90% or higher, {h['n']} nights in window"),
    ]
    labels = [str(x) for x in tr["label"]] if tr is not None and not tr.empty else []
    partial = [bool(x) for x in tr["partial"]] if tr is not None and "partial" in tr else None
    split = None
    if h.get("rev_from_occ_pts") is not None and h.get("rev_from_adr_pts") is not None:
        split = {"total": f"RevPAR {ps.spct(h.get('revpar_pct'))}",
                 "parts": [{"label": "Rooms sold", "value": abs(h["rev_from_occ_pts"]), "color": "tealLt",
                            "text": f"{h['rev_from_occ_pts']:+.1f} pts"},
                           {"label": "Rate", "value": abs(h["rev_from_adr_pts"]), "color": "teal",
                            "text": f"{h['rev_from_adr_pts']:+.1f} pts"}]}
    sec["cards"] = [
        {"type": "line", "q": t["occ_q"], "src": "Occupancy, this period against the same days last year. " + _src_hotel(m),
         "a": t["occ_a"], "fmt": "pct1", "labels": labels, "partial": partial, "compare": True, "aria": "Occupancy trend",
         "series": [{"name": "This period", "values": _list(tr, "occ"), "color": "cur", "area": True, "dots": len(labels) <= 16, "endLabel": True},
                    {"name": "Same period last year", "values": _list(tr, "occ_ly"), "color": "prior", "dash": True, "width": 2}],
         "how": ("The solid teal line is this period; the dashed sand line is the same dates a year earlier, shifted "
                 "364 days so weekdays line up. Where teal sits above sand, hotels sold a larger share of their rooms "
                 "than last year. Hover or tap for exact values.")},
        {"type": "adr", "q": t["adr_q"], "src": "Average daily rate against last year, and what moved RevPAR.",
         "a": t["adr_a"], "fmt": "usd0", "labels": labels, "partial": partial, "compare": True, "aria": "ADR trend",
         "series": [{"name": "This period", "values": _list(tr, "adr"), "color": "cur", "area": True, "dots": len(labels) <= 16, "endLabel": True},
                    {"name": "Same period last year", "values": _list(tr, "adr_ly"), "color": "prior", "dash": True, "width": 2}],
         "split": split,
         "how": ("RevPAR is occupancy times ADR, so any change in RevPAR comes from selling more rooms, charging more "
                 "per room, or both. The bar under the chart splits the change into those two parts using a log "
                 "decomposition, so the parts add back to the total.")},
        {"type": "bars", "wide": True, "side": True, "q": t["dow_q"],
         "src": "Occupancy by night of week for the selected window. Teal marks Friday and Saturday.",
         "a": t["dow_a"], "fmt": "pct1", "labels": [str(x) for x in m.dow["label"]] if m.dow is not None and not m.dow.empty else [],
         "tipLabels": [ps.DAY_NAMES.get(str(x), str(x)) + " nights" for x in m.dow["label"]] if m.dow is not None and not m.dow.empty else None,
         "axis": False, "valueLabels": True, "max": 100, "aria": "Occupancy by night of week",
         "series": [{"name": "Occupancy", "values": _list(m.dow, "occ"), "color": "sand", "hiColor": "teal",
                     "highlight": [int(x) in (4, 5) for x in m.dow["dow"]] if m.dow is not None and not m.dow.empty else None}],
         "extra": [{"name": "ADR", "values": _list(m.dow, "adr"), "fmt": "usd0"},
                   {"name": "RevPAR", "values": _list(m.dow, "revpar"), "fmt": "usd0"}],
         "how": "Each bar is the share of rooms sold on that night across the whole window. Hover a bar for its ADR and RevPAR."},
    ]
    return sec


# ---------------------------------------------------------------------------
# 2. Visitors and spend
# ---------------------------------------------------------------------------

def visitors(m: ps.Model, dma_coords: dict | None = None) -> dict:
    sec = _section("visitors", m)
    t = m.text["visitors"]
    d, prof, mk = m.dfy, m.profile or {}, m.markets
    kp = []
    if d and d.get("spend") is not None:
        span = pdx.fmt_month(d["first"]) + ("" if d["n_months"] == 1 else f" to {pdx.fmt_month(d['last'])}")
        kp.append(_kpi("Visitor spending", ps.usd_big(d["spend"]), d["spend"], "usdbig",
                       _delta(ps.spct(d.get("spend_pct")) + " vs last year" if d.get("spend_pct") is not None else None, d.get("spend_pct")),
                       span + (" (latest month on file)" if d.get("fallback") else ""), _list(m.dfy_trend, "spending_usd", 0)))
        if d.get("visitor_days"):
            kp.append(_kpi("Visitor days", ps.num_big(d["visitor_days"]), None, None,
                           _delta(ps.spct(d.get("vd_pct")) + " vs last year" if d.get("vd_pct") is not None else None, d.get("vd_pct")),
                           "Days spent in Dana Point by visitors"))
    period = None
    if mk is not None and not mk.empty:
        top = mk.iloc[0]
        if pd.notna(top.get("report_period_start")) and pd.notna(top.get("report_period_end")):
            period = f"{pdx.fmt_month(pd.Timestamp(top['report_period_start']))} to {pdx.fmt_month(pd.Timestamp(top['report_period_end']))}"
        kp.append(_kpi("Top origin market", ps.clean_market(top["dma"]), None, None,
                       _label_only(f"{ps.pct(top['share_pct'])} of visitor spending"),
                       f"Datafy, {period}" if period else "Datafy, latest published period"))
    if prof.get("out_of_state_pct") is not None:
        kp.append(_kpi("From out of state", f"{prof['out_of_state_pct']:.0f}%", prof["out_of_state_pct"], "pct0",
                       _label_only("Share of visitor spending"),
                       f"Average trip {prof['avg_los']:.1f} days" if prof.get("avg_los") else "Datafy"))
    sec["kpis"] = kp
    cards = []
    dt = m.dfy_trend
    if dt is not None and not dt.empty and "spending_usd" in dt:
        cards.append({"type": "bars", "wide": True, "q": t["spend_q"],
                      "src": "Visitor spending by month, this period against the same months last year. Source: Datafy.",
                      "a": t["spend_a"], "fmt": "usdbig", "labels": [str(x) for x in dt["label"]], "compare": True,
                      "aria": "Visitor spending by month",
                      "series": [{"name": "Same month last year", "values": _list(dt, "spend_ly", 0), "color": "sand", "opacity": .55},
                                 {"name": "This period", "values": _list(dt, "spending_usd", 0), "color": "cur"}],
                      "how": ("Teal bars are this period, sand bars the same months a year earlier. Datafy publishes "
                              "monthly, so the most recent weeks of the window may not have a Datafy month yet; the months "
                              "shown are the ones Datafy has reported.")})
    src_mk = f"Share of visitor spending by origin market. Datafy, {period}." if period else "Share of visitor spending by origin market. Datafy."
    if mk is not None and not mk.empty:
        pts = []
        for r in mk.itertuples():
            ll = (dma_coords or {}).get(r.dma)
            if not ll:
                continue
            miles = m.miles.get(r.dma) if m.miles else None
            pts.append({"name": r.dma, "label": ps.clean_market(r.dma), "share": _f(r.share_pct),
                        "miles": _f(miles, 0), "bearing": _f(_bearing(ll[0], ll[1]), 1)})
        cards.append({"type": "polar", "q": t["mkt_q"], "src": src_mk, "a": t["mkt_a"], "points": pts,
                      "how": ("Each circle is an origin market, placed by its direction and distance from Dana Point and "
                              "sized by its share of visitor spending; rings mark 100, 500, 1,000, and 2,500 miles on a "
                              "compressed scale. Datafy updates this view when it publishes a new period, so it does not "
                              "follow the data window.")})
        top8 = mk.head(8)
        cards.append({"type": "hbars", "q": "Top origin markets", "src": "Share of all visitor spending, latest Datafy period.",
                      "fmt": "pct1", "rows": [{"label": ps.clean_market(r.dma), "value": _f(r.share_pct)} for r in top8.itertuples()]})
    cats = m.categories
    if cats is not None and not cats.empty:
        cards.append({"type": "hbars", "q": t["cat_q"], "src": "Share of visitor spending by category, latest Datafy period.",
                      "a": t["cat_a"], "fmt": "pct1",
                      "rows": [{"label": str(r.category), "value": _f(r.share_pct)} for r in cats.head(8).itertuples()],
                      "how": ("Bars show each category's true share of all visitor spending, so they do not add to 100% "
                              "when smaller categories are left off.")})
    ads = m.ads or {}
    if ads:
        mix = m.ad_mix
        cards.append({"type": "ads", "q": t["ads_q"],
                      "src": f"Datafy advertising export as of {pdx.fmt_date(ads['snapshot_date'])}." if ads.get("snapshot_date") else "Datafy advertising export.",
                      "a": t["ads_a"],
                      "tiles": [{"label": "Impressions", "value": ps.num_big(ads.get("total_impressions")),
                                 "foot": f"{ads.get('total_clicks', 0):,.0f} clicks"},
                                {"label": "Media spend", "value": ps.usd(ads.get("total_spend_usd")),
                                 "foot": f"Cost per visitor day {ps.usd(ads.get('cost_per_visitor_day_usd'), 2)}"},
                                {"label": "Est. return", "value": f"{ps.usd(ads.get('est_roas'), 2)} per $1",
                                 "foot": f"{ps.usd_big(ads.get('est_campaign_impact_usd'))} estimated visitor impact"}],
                      "aName": "Share of visitor spending", "bName": "Share of campaign trips",
                      "rows": ([{"label": ps.clean_market(r.dma), "a": _f(r.share_pct), "b": _f(r.trip_share_pct)}
                                for r in mix.itertuples()] if mix is not None and not mix.empty else []),
                      "how": ("Teal is each market's share of all visitor spending (Datafy visitor economy); terracotta is "
                              "its share of trips Datafy attributes to the paid campaign. Where the two diverge, media "
                              "weight and visitor value are out of step.")})
    sec["cards"] = cards
    return sec


# ---------------------------------------------------------------------------
# 3. Market position
# ---------------------------------------------------------------------------

_CMP_COLS = [{"key": "occ", "label": "Occupancy", "fmt": "pct1"},
             {"key": "adr", "label": "ADR", "fmt": "usd0"},
             {"key": "revpar", "label": "RevPAR", "fmt": "usd0"}]


def market(m: ps.Model, tiers_df: pd.DataFrame | None, rooms_df: pd.DataFrame | None) -> dict:
    sec = _section("market", m)
    t = m.text["market"]
    p, b, sg = m.peers, m.bench, m.segments
    kp = []
    if p and p.get("dp"):
        kp.append(_kpi("Peer rank, RevPAR", f"{p['rank_revpar']} of {p['n_markets']}", None, None,
                       _label_only(f"Index {p['rgi']:.0f} vs peer average"), "Six coastal markets, STR weekly"))
        kp.append(_kpi("Peer rank, occupancy", f"{p['rank_occ']} of {p['n_markets']}", None, None,
                       _label_only(f"Index {p['mpi']:.0f} vs peer average"),
                       f"Dana Point {ps.pct(p['dp']['occ'])}, peers {ps.pct(p['peer_avg']['occ'])}"))
    if b and b.get("comp"):
        kp.append(_kpi("YTD RevPAR vs submarket", f"{b['rgi_sub'] - 100:+.0f}%", None, None,
                       _delta(f"Dana Point {ps.usd(b['comp']['revpar'])}", b["rgi_sub"] - 100, 0.5),
                       f"Through {pdx.fmt_month(b['through'])}, CoStar"))
    if sg:
        tot = sg["totals"].set_index("segment")["share"]
        if "Group" in tot:
            kp.append(_kpi("Group share of rooms", f"{tot['Group']:.0f}%", tot["Group"], "pct0",
                           _label_only(f"Transient {tot.get('Transient', 0):.0f}%"),
                           "CoStar segmentation, selected window" + (" (latest available)" if sg.get("fallback") else "")))
    sec["kpis"] = kp
    cards = []
    if p and not p["table"].empty:
        wk = p["weeks"]
        src = (f"STR weekly report, week ending {pdx.fmt_date(wk[-1])}." if (p["fallback"] or len(wk) == 1)
               else f"STR weekly reports, {len(wk)} weeks from {pdx.fmt_date(wk[0])} to {pdx.fmt_date(wk[-1])}.")
        cards.append({"type": "compare", "q": t["peer_q"], "src": src, "a": t["peer_a"], "cols": _CMP_COLS,
                      "rows": [{"name": r.market, "occ": _f(r.occ), "adr": _f(r.adr), "revpar": _f(r.revpar),
                                "hi": r.market == "Dana Point"} for r in p["table"].itertuples()],
                      "how": ("Terracotta is Dana Point; sand bars are the five peer markets in STR's weekly report. Each "
                              "column has its own scale. The index compares Dana Point with the simple average of the "
                              "five peers, where 100 means at par.")})
    if b and not b["table"].empty:
        cards.append({"type": "compare", "q": t["bench_q"],
                      "src": (f"Year to date through {pdx.fmt_month(b['through'])}. CoStar market reports dated "
                              f"{pdx.fmt_date(b['report_date'])}; Dana Point hotels computed for the same months."),
                      "a": t["bench_a"], "cols": _CMP_COLS,
                      "rows": [{"name": r.market, "occ": _f(r.occ), "adr": _f(r.adr), "revpar": _f(r.revpar),
                                "hi": bool(r.is_dp)} for r in b["table"].itertuples()],
                      "how": ("Dana Point hotels are the VDP Select comp set. The submarket is CoStar's Newport "
                              "Beach/Dana Point hotel market, about 110 properties. Occupancy is rooms sold over rooms "
                              "available for January through the latest closed month.")})
    my = m.multiyear
    if my is not None and not my.empty:
        years = sorted(my["year_label"].astype(str).unique(), key=lambda y: (not y.isdigit(), y))
        styles = [("Newport Beach/Dana Point", "teal", False, 2.6), ("Orange County", "maroon", False, 2.2),
                  ("United States", "sand", True, 2)]
        series = []
        for name, col, dash, wdt in styles:
            dd = my[my["market"] == name].assign(year_label=lambda x: x["year_label"].astype(str)).set_index("year_label")
            if dd.empty:
                continue
            series.append({"name": name, "color": col, "dash": dash, "width": wdt, "dots": True,
                           "values": [_f(dd["revpar_usd"].get(y)) if y in dd.index else None for y in years],
                           "endLabel": name == "Newport Beach/Dana Point"})
        cards.append({"type": "line", "q": t["trend_q"],
                      "src": "RevPAR by calendar year, closed years plus the current year to date. CoStar.",
                      "a": t["trend_a"], "fmt": "usd0", "labels": years, "series": series, "legend": True,
                      "aria": "RevPAR by year",
                      "how": ("Each line is one geography's RevPAR by year. The last point is year to date, which runs "
                              "higher in seasonal markets because it includes the summer peak but not the quieter fall and "
                              "winter months.")})
    if tiers_df is not None and not tiers_df.empty:
        short = {"Luxury & Upper Upscale": "Luxury and upper upscale", "Upscale & Upper Midscale": "Upscale and upper midscale",
                 "Midscale & Economy": "Midscale and economy"}
        top = tiers_df.iloc[0]
        card = {"type": "tiers", "q": t["tier_q"],
                "src": "Year-to-date RevPAR by chain-scale tier, Newport Beach/Dana Point submarket. CoStar.",
                "fmt": "usd0", "axis": False, "valueLabels": True, "aria": "RevPAR by tier",
                "labels": [short.get(x, x) for x in tiers_df["report_scope"]],
                "ticktext": [[w.split(" and ")[0] + " and", w.split(" and ")[1]] if " and " in w else [w]
                             for w in [short.get(x, x) for x in tiers_df["report_scope"]]],
                "series": [{"name": "RevPAR", "values": _list(tiers_df, "revpar_usd"), "color": "cur"}],
                "extra": [{"name": "Occupancy", "values": _list(tiers_df, "occupancy_pct"), "fmt": "pct1"},
                          {"name": "ADR", "values": _list(tiers_df, "adr_usd"), "fmt": "usd0"}],
                "tierText": (f"{top['report_scope']} hotels lead the submarket at {ps.usd(top['revpar_usd'])} RevPAR on "
                             f"{ps.pct(top['occupancy_pct'])} occupancy and {ps.usd(top['adr_usd'])} ADR."),
                "how": "Hover a bar for that tier's occupancy and ADR. The ring shows how the submarket's rooms split across the same tiers."}
        if rooms_df is not None and not rooms_df.empty:
            r0 = rooms_df.iloc[0]
            cols = ("luxury_upper_upscale_rooms", "upscale_upper_midscale_rooms", "midscale_economy_rooms")
            tier_total = sum(float(r0[c]) for c in cols if pd.notna(r0[c]))
            lux = r0["luxury_upper_upscale_rooms"] / tier_total * 100 if tier_total else None
            card["donut"] = {"center": f"{int(r0['total_rooms']):,}", "centerLabel": "rooms",
                             "parts": [{"label": "Luxury and upper upscale", "value": _f(r0[cols[0]]), "color": "teal"},
                                       {"label": "Upscale and upper midscale", "value": _f(r0[cols[1]]), "color": "maroon"},
                                       {"label": "Midscale and economy", "value": _f(r0[cols[2]]), "color": "sand"}]}
            card["a"] = (f"CoStar counts {int(r0['total_properties']):,} properties and about {int(r0['total_rooms']):,} rooms "
                         f"in the submarket. Luxury and upper upscale hotels hold {int(r0['luxury_upper_upscale_rooms']):,} "
                         f"rooms, {ps.pct(lux, 0)} of the rooms CoStar assigns to a tier. CoStar, {pdx.fmt_date(r0['report_date'])}.")
        cards.append(card)
    if sg:
        src = "CoStar Transient, Group, and Contract segmentation for the VDP Select comp set"
        src += f", {pdx.fmt_date(sg['start'])} to {pdx.fmt_date(sg['end'])}"
        if sg.get("fallback"):
            src += " (latest available, the segment export does not cover this window)"
        mdf = sg["monthly"]
        months = sorted(mdf["month"].unique())
        series = []
        for name, col in (("Transient", "teal"), ("Group", "maroon"), ("Contract", "sand")):
            dd = mdf[mdf["segment"] == name].set_index("month")
            if dd.empty or dd["demand"].sum() == 0:
                continue
            series.append({"name": name, "color": col,
                           "values": [_f(dd["share"].get(mm), 1) if mm in dd.index else 0 for mm in months]})
        tot = sg["totals"].set_index("segment")["share"]
        cards.append({"type": "stack", "wide": True, "q": t["mix_q"], "src": src + ".", "a": t["mix_a"],
                      "labels": [pd.Timestamp(mm).strftime("%b %Y") for mm in months], "series": series,
                      "totals": [{"label": n, "value": _f(tot.get(n, 0), 1), "color": c}
                                 for n, c in (("Transient", "teal"), ("Group", "maroon"), ("Contract", "sand")) if _f(tot.get(n, 0))],
                      "how": ("Each month's bar adds to 100% of rooms sold, split by how they were booked. Transient covers "
                              "individual leisure and business travelers; group covers blocks of ten or more rooms; contract "
                              "covers long-term negotiated business such as crews.")})
    sec["cards"] = cards
    return sec


# ---------------------------------------------------------------------------
# 4. Forward outlook
# ---------------------------------------------------------------------------

def forward(m: ps.Model) -> dict:
    sec = _section("forward", m)
    t = m.text["forward"]
    kp = []
    ev = m.events
    if ev is not None and not ev.empty:
        e0 = ev.iloc[0]
        kp.append(_kpi("Next major event", str(e0["event_name"]), None, None,
                       _label_only(ps.days_phrase(int(e0["days_out"])).capitalize()),
                       f"Same dates last year {ps.pct(e0['ly_occ'], 0)} occupancy" if pd.notna(e0.get("ly_occ")) else ""))
    cq = m.compression
    if cq is not None and not cq.empty:
        cur = cq.iloc[-1]
        kp.append(_kpi(f"{cur['quarter']} nights at 80%+", f"{int(cur['d80'])}", int(cur["d80"]), "num0",
                       _label_only(f"{int(cur['d90'])} at 90%+"), "Quarter in progress" if not cur["complete"] else "Full quarter"))
    mo = m.momentum
    if mo and mo.get("revpar_pct") is not None:
        kp.append(_kpi("Last 4 weeks RevPAR", ps.usd(mo["revpar"]), mo["revpar"], "usd0",
                       _delta(ps.spct(mo["revpar_pct"]) + " vs last year", mo["revpar_pct"]),
                       f"{pdx.fmt_date(mo['start'], False)} to {pdx.fmt_date(mo['end'])}"))
    g = m.group
    if g and g.get("group_share") is not None:
        kp.append(_kpi("Group share, latest week", f"{g['group_share']:.0f}%", g["group_share"], "pct0",
                       _label_only(f"{ps.pct(g['total_occ'])} occupancy"), f"STR week ending {pdx.fmt_date(g['week'])}"))
    sec["kpis"] = kp
    cards = []
    items = []
    if ev is not None and not ev.empty:
        for r in ev.itertuples():
            dt = pd.Timestamp(r.date)
            ly = []
            if pd.notna(getattr(r, "ly_occ", None)):
                ly.append(f"{ps.pct(r.ly_occ, 0)} occupancy")
            if pd.notna(getattr(r, "ly_adr", None)):
                ly.append(f"at {ps.usd(r.ly_adr)}")
            items.append({"name": str(r.event_name), "mon": dt.strftime("%b"), "day": str(dt.day),
                          "when": ps.days_phrase(int(r.days_out)).capitalize(), "soon": int(r.days_out) <= 7,
                          "ly": ("Same dates last year: " + " ".join(ly)) if ly else "No prior-year hotel data for these dates",
                          "ly_occ": _f(getattr(r, "ly_occ", None), 1)})
    cards.append({"type": "events", "q": t["events_q"],
                  "src": "Visit Dana Point events calendar, next 120 days, with STR occupancy on the matching dates a year earlier.",
                  "a": t["events_a"], "items": items,
                  "how": ("Last year's figures cover the night before and the night of each event, shifted 364 days so the "
                          "weekday matches. High prior-year occupancy marks dates with pricing power; softer dates are where "
                          "event marketing can add the most. The thin bar shows last year's occupancy on a 0 to 100% scale.")})
    if cq is not None and not cq.empty:
        cards.append({"type": "compression", "q": t["comp_q"],
                      "src": "Nights at 80% and 90% occupancy or higher, by quarter. An asterisk marks a quarter still in progress.",
                      "a": t["comp_a"], "labels": [str(q) for q in cq["quarter"]],
                      "d80": [int(x) for x in cq["d80"]], "d90": [int(x) for x in cq["d90"]],
                      "complete": [bool(x) for x in cq["complete"]],
                      "how": ("Compression nights are nights when hotels are nearly full, the nights with the strongest "
                              "pricing power. The narrow terracotta bar is the 90%-plus subset of the teal bar.")})
    if g and g.get("parts") is not None:
        seg_col = {"Transient": "teal", "Group": "maroon", "Contract": "sand"}
        parts = [{"label": r.segment, "value": _f(r.occ_pct, 1), "color": seg_col.get(r.segment, "sand")}
                 for r in g["parts"].itertuples()]
        cards.append({"type": "split", "wide": True, "q": t["group_q"], "src": f"STR weekly report, week ending {pdx.fmt_date(g['week'])}.",
                      "a": t["group_a"], "parts": parts})
    sec["cards"] = cards
    # The pipeline's insight carries relative timing ("in 2 days", "Q3 is
    # underway"), so it is shown only while it is current; after that the
    # section's own answer above, which is recomputed on every load, stands alone.
    ins = m.insight
    if ins and ins.get("as_of_date"):
        age = (pd.Timestamp.now().normalize() - pd.Timestamp(ins["as_of_date"])).days
        if 0 <= age <= 2:
            sec["note"] = {"label": f"From the insights engine, {pdx.fmt_date(ins['as_of_date'])}",
                           "text": ins.get("body") or ins.get("headline") or ""}
    return sec
