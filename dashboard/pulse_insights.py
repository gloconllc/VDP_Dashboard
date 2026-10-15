"""
pulse_insights.py, the Dana Point PULSE Insights report.

A newspaper-style, section-by-section report built for Visit Dana Point from
the same window-aware model the board uses (pulse_story.build_model), plus a
few read-only queries for sources the board does not show (airport traffic,
state employment, campaign and website attribution, social audience).

Structure follows the BI report pattern used across GloCon's DMO work: an
Overview (PULSE Summary announcement, key takeaways, questions and short
answers), then one section per question, each written as a news report
(lede, why it matters, at a glance, by the numbers, how it is calculated,
who/what/when/where/why/how) followed by analysis (charts, main table, what
it means for Dana Point, compare and contrast, more data and notes, up next),
then an Appendix (glossary, methods, formulas) and a Sources register.

Rules this module follows:
  * Facts only. Every figure comes from analytics.sqlite and carries its
    source and period. Calculated figures show their arithmetic. Scenario
    figures are labeled "Scenario" and state their assumption.
  * Layer 1 (STR, CoStar, Datafy) leads. Visit California, BLS and FAA-style
    airport counts are context and labeled as such. Zartico is not used.
  * Tables CLAUDE.md flags as placeholder data (costar_chain_scale_breakdown,
    costar_competitive_set, costar_supply_pipeline, group_intelligence) are
    never read here.
  * No writes to the database.

Public API:
  build_report(conn, model, dma_coords, prepared) -> dict
  render_html(report, assets_dir) -> str
  build_xlsx(report, section_ids) -> bytes
  build_csv(report, section_ids) -> str
  build_pptx(report, section_ids) -> bytes | None
"""

from __future__ import annotations

import base64
import csv
import html
import io
import json
import math
import os
from datetime import date

import pandas as pd

import pulse_data as pdx
import pulse_story as ps

_HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Formatting (AP style)
# ---------------------------------------------------------------------------

_AP_MONTHS = ["Jan.", "Feb.", "March", "April", "May", "June", "July", "Aug.", "Sept.", "Oct.", "Nov.", "Dec."]


def _ok(v) -> bool:
    try:
        return v is not None and not pd.isna(v)
    except Exception:
        return v is not None


def ap_date(d) -> str:
    d = pd.Timestamp(d)
    return f"{_AP_MONTHS[d.month - 1]} {d.day}, {d.year}"


_FULL_MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
                "October", "November", "December"]


def ap_month(d) -> str:
    """AP: spell out the month when it stands alone or with only a year."""
    d = pd.Timestamp(d)
    return f"{_FULL_MONTHS[d.month - 1]} {d.year}"


def ap_md(d) -> str:
    d = pd.Timestamp(d)
    return f"{_AP_MONTHS[d.month - 1]} {d.day}"


def span(a, b) -> str:
    a, b = pd.Timestamp(a), pd.Timestamp(b)
    if a.year == b.year:
        return f"{ap_md(a)} to {ap_date(b)}"
    return f"{ap_date(a)} to {ap_date(b)}"


def mspan(a, b) -> str:
    a, b = pd.Timestamp(a), pd.Timestamp(b)
    if a.year == b.year and a.month == b.month:
        return ap_month(a)
    if a.year == b.year:
        return f"{_FULL_MONTHS[a.month - 1]} to {ap_month(b)}"
    return f"{ap_month(a)} to {ap_month(b)}"


def pct(v, d=1):
    return "n/a" if not _ok(v) else f"{v:.{d}f}%"


def spct(v, d=1):
    if not _ok(v):
        return "n/a"
    return f"{'+' if v >= 0 else '−'}{abs(v):.{d}f}%"


def pts(v, d=1):
    if not _ok(v):
        return "n/a"
    return f"{'+' if v >= 0 else '−'}{abs(v):.{d}f} points"


def usd(v, d=0):
    if not _ok(v):
        return "n/a"
    return f"${v:,.{d}f}"


def usd_big(v):
    if not _ok(v):
        return "n/a"
    a = abs(v)
    if a >= 1e9:
        return f"${v / 1e9:.2f}B"
    if a >= 1e6:
        return f"${v / 1e6:.1f}M"
    if a >= 1e3:
        return f"${v / 1e3:.0f}K"
    return f"${v:,.0f}"


def num_big(v):
    if not _ok(v):
        return "n/a"
    a = abs(v)
    if a >= 1e6:
        return f"{v / 1e6:.2f}M"
    if a >= 1e4:
        return f"{v / 1e3:.0f}K"
    return f"{v:,.0f}"


def num(v):
    return "n/a" if not _ok(v) else f"{v:,.0f}"


def glyph(v, tol=0.05):
    if not _ok(v) or abs(v) < tol:
        return "▶"
    return "▲" if v > 0 else "▼"


def _q(conn, sql, params=()):
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Albers USA (lower 48), matching us-atlas states-albers-10m
# (d3.geoAlbersUsa: parallels 29.5/45.5, rotate 96, center -0.6/38.7, scale 1300)
# ---------------------------------------------------------------------------

def _albers(lon, lat):
    rad = math.radians
    p0, p1 = rad(29.5), rad(45.5)
    n = (math.sin(p0) + math.sin(p1)) / 2
    c = 1 + math.sin(p0) * (2 * n - math.sin(p0))
    r0 = math.sqrt(c) / n

    def raw(lam, phi):
        r = math.sqrt(max(c - 2 * n * math.sin(phi), 0)) / n
        return r * math.sin(lam * n), r0 - r * math.cos(lam * n)

    x, y = raw(rad(lon + 96), rad(lat))
    cx, cy = raw(rad(-0.6), rad(38.7))
    k = 1300
    return 487.5 + k * (x - cx), 305 - k * (y - cy)


# ---------------------------------------------------------------------------
# Small builders
# ---------------------------------------------------------------------------

def _det(fig, fig_l, text, src):
    return {"fig": fig, "fig_l": fig_l, "text": text, "src": src}


def _stat(k, v, l, p, src):
    return {"k": k, "v": v, "l": l, "p": p, "src": src}


# How data reaches PULSE (CLAUDE.md, Standard Process and Pipeline). Drawn with X6.
PIPELINE = {
    "columns": ["Sources", "Loaders", "Database", "Calculations", "What people see"],
    "nodes": [
        {"id": "str", "col": 0, "label": "STR daily, monthly and weekly exports"},
        {"id": "costar", "col": 0, "label": "CoStar daily grid and market reports"},
        {"id": "datafy", "col": 0, "label": "Datafy visitor and attribution exports"},
        {"id": "public", "col": 0, "label": "Later, Visit California, BLS"},
        {"id": "site", "col": 0, "label": "visitdanapoint.com events feed"},
        {"id": "load", "col": 1, "label": "Loader scripts (load_*, fetch_*)"},
        {"id": "db", "col": 2, "label": "analytics.sqlite"},
        {"id": "kpis", "col": 3, "label": "KPIs and compression"},
        {"id": "ins", "col": 3, "label": "Daily insights and table relationships"},
        {"id": "board", "col": 4, "label": "PULSE board and Intelligence Brief"},
        {"id": "report", "col": 4, "label": "Insights report and exports"},
        {"id": "pdf", "col": 4, "label": "Weekly PDF report"},
    ],
    "edges": [["str", "load"], ["costar", "load"], ["datafy", "load"], ["public", "load"], ["site", "load"],
              ["load", "db"], ["db", "kpis"], ["db", "ins"], ["kpis", "board"], ["kpis", "report"], ["kpis", "pdf"],
              ["ins", "board"], ["ins", "report"]],
}


def _photo_credits() -> list:
    try:
        import vdp_site
        return [list(x) for x in vdp_site.PHOTO_CREDITS]
    except Exception:  # noqa: BLE001
        return []


def _hi_lo_spend(dma) -> str:
    """Highest vs. top-market spend per visitor, from Datafy's market table."""
    try:
        d = dma.dropna(subset=["avg_spend_usd"])
        top, hi = d.iloc[0], d.sort_values("avg_spend_usd", ascending=False).iloc[0]
        return (f"{ps.clean_market(top.dma)} visitors average ${top.avg_spend_usd:,.0f} each, while "
                f"{ps.clean_market(hi.dma)} visitors average ${hi.avg_spend_usd:,.0f}")
    except Exception:  # noqa: BLE001
        return "spend per visitor differs by market"


def _adv(kind, point, action, src, calc=None):
    """One labeled consideration (GloCon perspective). Kept apart from the
    findings so facts and suggestions never mix."""
    return {"type": kind, "point": point, "action": action or "None needed", "src": src, "calc": calc}


def _cc(kind, point, note, src, calc=None):
    """One compare-and-contrast row: a sourced finding plus, where needed, a
    note on how to read it. Findings only; no recommendations."""
    return {"type": kind, "point": point, "solution": note or "As reported by the source.", "src": src, "calc": calc}


# Sources, as short labels plus a register entry each
SRC = {
    "str": "STR daily data, Visit Dana Point select hotels",
    "costar_daily": "CoStar daily export, same Visit Dana Point hotels",
    "costar_rpt": "CoStar Newport Beach/Dana Point submarket, Orange County and U.S. reports",
    "costar_seg": "CoStar daily segmentation export",
    "str_weekly": "STR weekly coastal peer report",
    "datafy": "Datafy visitor economy dashboard",
    "datafy_ads": "Datafy paid-media attribution",
    "datafy_web": "Datafy website attribution report",
    "datafy_camp": "Datafy annual campaign attribution report",
    "later": "Later social analytics (Instagram, Facebook, TikTok)",
    "vca_air": "Visit California airport traffic (John Wayne Airport)",
    "vca_fc": "Visit California travel forecast",
    "bls": "U.S. Bureau of Labor Statistics, California employment",
    "events": "Visit Dana Point events calendar",
    "insights": "PULSE insights engine (STR and Datafy)",
    "calc": "PULSE calculation from the sources named",
}


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------

def build_report(conn, m, dma_coords: dict | None = None, prepared: date | None = None) -> dict:
    """m is a pulse_story.Model built on a year-to-date window."""
    prepared = prepared or date.today()
    prep = ap_date(prepared)
    f = m.fresh
    h = m.hotel or {}
    hp = h.get("prior") or {}
    hstart, hend = h.get("start", m.window.start), h.get("end", m.window.end)
    hspan = span(hstart, hend)
    # Wording follows the selected window (added 2026-10-15): "year to date"
    # only when the window runs from Jan. 1; otherwise "this period".
    _ytd = pd.Timestamp(hstart).month == 1 and pd.Timestamp(hstart).day == 1 and pd.Timestamp(hstart).year == pd.Timestamp(hend).year
    P_TD = "year to date" if _ytd else "this period"
    P_THE = "the year" if _ytd else "the period"
    P_THIS = "this year" if _ytd else "this period"
    year = pd.Timestamp(hend).year
    P_YTD = f"{year} to date" if _ytd else "this period"
    P_YTD_CAP = f"{year} to date" if _ytd else "This period"
    str_through = f.get("str_through")
    costar_ext = bool(f.get("costar_extends")) and str_through is not None and pd.Timestamp(str_through) < pd.Timestamp(hend)
    hotel_src = SRC["str"] + (f" (through {ap_date(str_through)}), extended with " + SRC["costar_daily"] +
                              f" to {ap_date(hend)}" if costar_ext else f" ({hspan})")
    hotel_src_short = f"STR and CoStar daily data, {hspan}" if costar_ext else f"STR daily data, {hspan}"
    dateline = f"DANA POINT, Calif. ({prep}):"

    # Hotel comp-set size (CoStar participation roster)
    part = _q(conn, """SELECT COUNT(DISTINCT building_name) AS n, SUM(rooms) AS rooms
                       FROM costar_participation
                       WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM costar_participation)
                         AND period_month=(SELECT MAX(period_month) FROM costar_participation
                                           WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM costar_participation))""")
    n_hotels = int(part.iloc[0]["n"]) if not part.empty and _ok(part.iloc[0]["n"]) else None
    n_rooms = float(part.iloc[0]["rooms"]) if not part.empty and _ok(part.iloc[0]["rooms"]) else None
    avg_rooms_day = (h["supply"] / h["n"]) if h.get("supply") and h.get("n") else None

    # ---------------- Datafy profile ----------------
    prof = m.profile or {}
    dfy = m.dfy or {}
    d_start, d_end = f.get("datafy_start"), f.get("datafy_end")
    dfy_period = span(d_start, d_end) if d_start and d_end else "latest period"
    dfy_src = f"{SRC['datafy']}, {dfy_period}"
    trips = prof.get("trips")
    los = prof.get("avg_los")
    oos = prof.get("out_of_state_pct")
    losd = _q(conn, """SELECT length_of_stay, pct_of_trips FROM datafy_overview_los_distribution
                       WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_los_distribution)""")
    overnight_pct = None
    day_pct = None
    if not losd.empty:
        o = losd[losd["length_of_stay"].str.contains("Overnight", case=False)]
        d_ = losd[losd["length_of_stay"].str.contains("Day Trips", case=False)]
        overnight_pct = float(o.iloc[0]["pct_of_trips"]) if not o.empty else None
        day_pct = float(d_.iloc[0]["pct_of_trips"]) if not d_.empty else None
    dfy_spend = dfy.get("spend")
    dfy_months = dfy.get("months")
    dfy_mspan = mspan(dfy["first"], dfy["last"]) if dfy.get("first") is not None else dfy_period
    cats = m.categories if m.categories is not None else pd.DataFrame()
    acc_share = None
    if not cats.empty:
        a = cats[cats["category"].str.contains("Accommodation", case=False)]
        acc_share = float(a.iloc[0]["share_pct"]) if not a.empty else None
    dine_share = None
    if not cats.empty:
        a = cats[cats["category"].str.contains("Dining", case=False)]
        dine_share = float(a.iloc[0]["share_pct"]) if not a.empty else None
    markets = m.markets if m.markets is not None else pd.DataFrame()
    top_mkt = ps.clean_market(markets.iloc[0]["dma"]) if not markets.empty else None
    top_mkt_share = float(markets.iloc[0]["share_pct"]) if not markets.empty else None
    top3_share = float(markets.head(3)["share_pct"].sum()) if not markets.empty else None

    states = _q(conn, """SELECT state, share_of_trips_pct FROM datafy_overview_state_origin
                         WHERE (report_period_start, report_period_end) = (
                           SELECT report_period_start, report_period_end FROM datafy_overview_state_origin
                           ORDER BY report_period_end DESC, report_period_start DESC LIMIT 1)
                         ORDER BY share_of_trips_pct DESC""")
    ca_share = None
    if not states.empty:
        r = states[states["state"] == "California"]
        ca_share = float(r.iloc[0]["share_of_trips_pct"]) if not r.empty else None
    countries = _q(conn, """SELECT country, share_pct FROM datafy_overview_top_countries
                            WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_top_countries)
                              AND report_period_start=(SELECT MAX(report_period_start) FROM datafy_overview_top_countries
                                   WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_top_countries))
                            ORDER BY share_pct DESC""")
    demo = _q(conn, """SELECT dimension, segment, share_pct FROM datafy_overview_demographics
                       WHERE report_period_start='2025-01-01'
                         AND report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_demographics
                                                WHERE report_period_start='2025-01-01')
                         AND dimension IN ('Age','Income','Household')""")
    demo_period = None
    dp_ = _q(conn, """SELECT MAX(report_period_end) AS e FROM datafy_overview_demographics WHERE report_period_start='2025-01-01'""")
    if not dp_.empty and dp_.iloc[0]["e"]:
        demo_period = span("2025-01-01", dp_.iloc[0]["e"])
    repeat = _q(conn, "SELECT repeat_pct FROM datafy_overview_repeat_spenders ORDER BY report_period_end DESC LIMIT 1")
    repeat_pct = float(repeat.iloc[0]["repeat_pct"]) * 100 if not repeat.empty and _ok(repeat.iloc[0]["repeat_pct"]) else None
    dma_full = _q(conn, """SELECT dma, spending_share_pct, avg_spend_usd, avg_length_of_stay_days
                           FROM datafy_overview_dma WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_dma)
                             AND spending_share_pct IS NOT NULL ORDER BY spending_share_pct DESC""")
    dma_period = None
    if not dma_full.empty:
        dp2 = _q(conn, "SELECT MIN(report_period_start) s, MAX(report_period_end) e FROM datafy_overview_dma WHERE report_period_end=(SELECT MAX(report_period_end) FROM datafy_overview_dma)")
        dma_period = span(dp2.iloc[0]["s"], dp2.iloc[0]["e"])
    airports = _q(conn, """SELECT airport_code, airport_name, passengers_share_pct FROM datafy_overview_airports
                           WHERE (report_period_start, report_period_end)=(SELECT report_period_start, report_period_end
                                 FROM datafy_overview_airports ORDER BY report_period_end DESC, report_period_start DESC LIMIT 1)
                           ORDER BY passengers_share_pct DESC""")

    # ---------------- Other sources ----------------
    sna = _q(conn, """SELECT year, month, total_pax, domestic_pax, intl_pax, total_yoy_pct
                      FROM visit_ca_airport_traffic WHERE airport LIKE '%SNA%' ORDER BY year, month""")
    bls = _q(conn, """SELECT series_name, year, month, value_thousands, yoy_chg_pct FROM bls_employment_monthly
                      WHERE series_name IN ('CA Leisure & Hospitality Employment','CA Accommodation Employment')
                      ORDER BY year, month""")
    vcfc = _q(conn, "SELECT year, total_visits_m, total_yoy_pct, is_forecast FROM visit_ca_travel_forecast ORDER BY year")
    web = _q(conn, "SELECT * FROM datafy_attribution_website_kpis ORDER BY report_period_end DESC LIMIT 1")
    camp = _q(conn, """SELECT * FROM datafy_attribution_media_kpis WHERE campaign_name IS NOT NULL
                       ORDER BY report_period_end DESC LIMIT 1""")
    social = {}
    for k, sql in (("Instagram", "SELECT data_date d, followers v FROM later_ig_profile_growth WHERE followers IS NOT NULL ORDER BY data_date"),
                   ("Facebook", "SELECT data_date d, page_followers v FROM later_fb_profile_growth WHERE page_followers IS NOT NULL ORDER BY data_date"),
                   ("TikTok", "SELECT data_date d, followers v FROM later_tk_profile_growth WHERE followers IS NOT NULL ORDER BY data_date")):
        s = _q(conn, sql)
        if not s.empty:
            social[k] = {"first": s.iloc[0]["d"], "v0": float(s.iloc[0]["v"]), "last": s.iloc[-1]["d"], "v": float(s.iloc[-1]["v"])}
    ads = m.ads or {}

    # ---------------- Hotel derived ----------------
    wk = m.wk or {}
    dow = m.dow if m.dow is not None else pd.DataFrame()
    sun = dow[dow["label"] == "Sun"].iloc[0] if not dow.empty and (dow["label"] == "Sun").any() else None
    mon = dow[dow["label"] == "Mon"].iloc[0] if not dow.empty and (dow["label"] == "Mon").any() else None
    sm_occ = sm_adr = None
    if sun is not None and mon is not None:
        sm_occ = (sun["demand"] + mon["demand"]) / (sun["supply"] + mon["supply"]) * 100
        sm_adr = (sun["revenue"] + mon["revenue"]) / (sun["demand"] + mon["demand"])
    fri = dow[dow["label"].isin(["Fri", "Sat"])] if not dow.empty else pd.DataFrame()
    fs_occ = (fri["demand"].sum() / fri["supply"].sum() * 100) if not fri.empty else None
    # Claims that depend on the selected window are checked against the data
    # (added 2026-10-15); each reads exactly as before when the data agrees.
    _rv, _ad = h.get("revpar_pct"), h.get("adr_pct")
    _occ_rel = ((1 + _rv / 100) / (1 + _ad / 100) - 1) * 100 if _ok(_rv) and _ok(_ad) and _ad > -100 else None
    RATE_LEADS = _ok(_ad) and (_occ_rel is None or _ad >= _occ_rel)
    LEADER = "rate" if RATE_LEADS else "occupancy"
    try:
        _low2 = list(dow.sort_values("occ")["label"].head(2)) if not dow.empty else ["Sun", "Mon"]
    except Exception:  # noqa: BLE001
        _low2 = ["Sun", "Mon"]
    SM_LOW = set(_low2) == {"Sun", "Mon"}
    _DAYN = {"Mon": "Monday", "Tue": "Tuesday", "Wed": "Wednesday", "Thu": "Thursday", "Fri": "Friday", "Sat": "Saturday", "Sun": "Sunday"}
    LOW_NIGHTS = "Sunday and Monday" if SM_LOW else " and ".join(_DAYN.get(x, x) for x in sorted(_low2, key=lambda d: list(_DAYN).index(d) if d in _DAYN else 9))
    point_rn = point_val = None
    if avg_rooms_day and sm_adr:
        point_rn = avg_rooms_day * 0.01 * 104
        point_val = point_rn * sm_adr
    rev = h.get("revenue")
    tot = rev * 0.10 if rev else None
    tbid = rev * 0.0125 if rev else None
    tot_p = hp.get("revenue") * 0.10 if hp.get("revenue") else None

    # Monthly heat: weeknights (Sun-Thu) vs Fri-Sat, by month, this year
    heat_rows = []
    d = m.daily
    if d is not None and not d.empty:
        y = d[(d["date"] >= pd.Timestamp(year=year, month=1, day=1)) & (d["date"] <= pd.Timestamp(hend))].copy()
        y["wkend"] = y["date"].dt.dayofweek.isin([4, 5])
        y["ym"] = y["date"].dt.to_period("M")
        for per, g in y.groupby("ym"):
            wn, we = g[~g["wkend"]], g[g["wkend"]]
            allo = g["demand"].sum() / g["supply"].sum() * 100 if g["supply"].sum() else None
            wno = wn["demand"].sum() / wn["supply"].sum() * 100 if wn["supply"].sum() else None
            weo = we["demand"].sum() / we["supply"].sum() * 100 if we["supply"].sum() else None
            n90 = int((g["demand"] / g["supply"] * 100 >= 90).sum())
            heat_rows.append({"m": ap_month(per.to_timestamp()).split(" ")[0], "wn": wno, "we": weo, "all": allo,
                              "n90": n90, "partial": len(g) < per.days_in_month})

    # Peers and benchmarks
    peers = m.peers or {}
    bench = m.bench or {}
    btab = bench.get("table") if bench else None
    bthrough = ap_month(bench["through"]) if bench.get("through") is not None else None
    sub_row = None
    if btab is not None and not btab.empty:
        s_ = btab[btab["market"] == "Newport Beach/Dana Point"]
        sub_row = s_.iloc[0].to_dict() if not s_.empty else None
    comp_ytd = bench.get("comp") or {}

    # Segments (transient vs group)
    seg = m.segments or {}
    segt = seg.get("totals")
    grp_share = trn_share = None
    if segt is not None and not segt.empty:
        tot_d = segt["demand"].sum()
        for r in segt.itertuples():
            share = r.demand / tot_d * 100 if tot_d else None
            if str(r.segment).lower().startswith("group"):
                grp_share = share
            if str(r.segment).lower().startswith("trans"):
                trn_share = share

    # Momentum and events
    mo = m.momentum or {}
    ev = m.events if m.events is not None else pd.DataFrame()
    # Lead with the next major event (site "Signature Events", festivals, races);
    # the full dated calendar stays in the events table.
    if not ev.empty and "is_major" in ev.columns and (ev["is_major"] == 1).any():
        ev_lead = ev[ev["is_major"] == 1].reset_index(drop=True)
    else:
        ev_lead = ev
    comp = m.compression if m.compression is not None else pd.DataFrame()

    # ==================================================================
    # SECTIONS
    # ==================================================================
    sections: list[dict] = []

    # ---- 1. Dana Point today ----------------------------------------
    s_today = {
        "id": "today", "tab": "Dana Point today",
        "question": "Where does Dana Point's visitor economy stand today?",
        "headline": f"Dana Point hotels earn {usd_big(rev)} in room revenue as {LEADER} leads {P_THE}",
        "dek": f"Revenue per available room is {spct(h.get('revpar_pct'))} vs. last year, and visitors spent {usd_big(dfy_spend)} from {dfy_mspan}.",
        "byline_src": f"{hotel_src_short}; Datafy, {dfy_mspan}",
        "lede": (f"{dateline} Visit Dana Point's select hotels sold {num(h.get('demand'))} room nights from {hspan}, "
                 f"earning {usd_big(rev)} in room revenue ({spct(h.get('revenue_pct'))} vs. last year). Occupancy was "
                 f"{pct(h.get('occ'))} at a {usd(h.get('adr'))} average nightly rate, and revenue per available room "
                 f"reached {usd(h.get('revpar'))} ({spct(h.get('revpar_pct'))}). Visitors spent {usd_big(dfy_spend)} "
                 f"in Dana Point from {dfy_mspan} ({spct(dfy.get('spend_pct'))}), according to Datafy."),
        "lede_src": f"Source: {hotel_src}; {SRC['datafy']}, {dfy_mspan}",
        "key": {"v": usd_big(rev), "l": f"hotel room revenue, {P_YTD}"},
        "why": (f"Room revenue is the base for the city's 10% transient occupancy tax and the Tourism Business "
                f"Improvement District assessment, so hotel performance is the clearest read on what visitors "
                f"return to Dana Point. On the comp-set hotels alone, {P_THE}'s room revenue equals about "
                f"{usd_big(tot)} in transient occupancy tax at the 10% rate."),
        "why_calc": f"{usd_big(rev)} room revenue × 10% = {usd_big(tot)} (comp-set hotels only; not city-reported collections)",
        "glance": [
            {"type": "chain", "title": "From visitors to city revenue",
             "sub": f"{P_YTD_CAP} · each step is a figure in this section",
             "steps": [
                 {"v": num_big(trips), "l": f"visitor trips ({dfy_period})", "tip": f"{num(trips)} trips, Datafy"},
                 {"v": usd_big(dfy_spend), "l": f"visitor spending ({dfy_mspan})", "tip": f"{usd(dfy_spend)} card-observed spending, Datafy"},
                 {"v": num_big(h.get("demand")), "l": "hotel room nights sold", "tip": f"{num(h.get('demand'))} room nights, comp set"},
                 {"v": usd_big(rev), "l": "hotel room revenue", "tip": f"{usd(rev)}, comp set"},
                 {"v": usd_big(tot), "l": "transient occupancy tax at 10%", "tip": f"{usd(rev)} × 10% = {usd(tot)}"},
             ],
             "src": f"Datafy ({dfy_period}); {hotel_src_short}; PULSE calculation"},
        ],
        "numbers": [
            _stat(f"Hotels, {P_YTD}", usd(h.get("revpar")), "Revenue per available room",
                  f"{glyph(h.get('revpar_pct'))} {spct(h.get('revpar_pct'))} vs. last year", hotel_src_short),
            _stat(f"Hotels, {P_YTD}", pct(h.get("occ")), "Hotel occupancy",
                  f"{glyph(h.get('occ_pts'))} {pts(h.get('occ_pts'))} vs. last year", hotel_src_short),
            _stat(f"Hotels, {P_YTD}", usd(h.get("adr")), "Average nightly rate",
                  f"{glyph(h.get('adr_pct'))} {spct(h.get('adr_pct'))} vs. last year", hotel_src_short),
            _stat(f"Visitors, {dfy_mspan}", usd_big(dfy_spend), "Visitor spending",
                  f"{glyph(dfy.get('spend_pct'))} {spct(dfy.get('spend_pct'))} vs. same months last year", f"Datafy, {dfy_mspan}"),
        ],
        "calc": [
            ["Revenue per available room", "Room revenue ÷ rooms available", f"{usd(rev)} ÷ {num(h.get('supply'))} = {usd(h.get('revpar'))}", hotel_src_short],
            ["Occupancy", "Rooms sold ÷ rooms available", f"{num(h.get('demand'))} ÷ {num(h.get('supply'))} = {pct(h.get('occ'))}", hotel_src_short],
            ["Average nightly rate", "Room revenue ÷ rooms sold", f"{usd(rev)} ÷ {num(h.get('demand'))} = {usd(h.get('adr'))}", hotel_src_short],
            ["Transient occupancy tax (estimate)", "Room revenue × 10%", f"{usd(rev)} × 10% = {usd(tot)}", "PULSE calculation"],
        ],
        "details": {
            "who": _det(f"{n_hotels or 'n/a'} hotels", f"{num(n_rooms)} rooms in the CoStar participation roster",
                        "Visit Dana Point's select hotel set, the same hotels in STR's daily export and CoStar's daily grid.",
                        f"CoStar participation roster, pulled {ap_date(f['costar_pull']) if f.get('costar_pull') else 'n/a'}"),
            "what": _det(usd(h.get("revpar")), "revenue per available room",
                         f"Occupancy {pct(h.get('occ'))} × average rate {usd(h.get('adr'))}. Rate rose {spct(h.get('adr_pct'))}; occupancy moved {pts(h.get('occ_pts'))}.",
                         hotel_src_short),
            "when": _det(hspan, "hotel data period",
                         f"Compared with the same dates a year earlier, shifted 364 days so weekdays line up. Visitor spending covers {dfy_mspan}.",
                         hotel_src_short),
            "where": _det("Dana Point", "Visit Dana Point select hotels",
                          "Hotels inside Dana Point; CoStar's Newport Beach/Dana Point submarket is the wider benchmark.",
                          SRC["costar_rpt"]),
            "why": _det(usd_big(tot), f"{usd_big(rev)} × 10% transient occupancy tax",
                        "Room revenue funds city services through the transient occupancy tax and Visit Dana Point through the TBID.",
                        "PULSE calculation (comp-set hotels only)"),
            "how": _det(f"{h.get('n', 'n/a')} nights", "daily hotel records in this window",
                        "Rooms-weighted daily STR data, extended with CoStar daily data for the same hotels after STR's latest file.",
                        hotel_src_short),
        },
        "charts": [
            {"type": "line", "title": "Revenue per available room ran above last year most months",
             "sub": f"Monthly revenue per available room, {year} vs. {year - 1}",
             "x": [r.label.split(" ")[0] for r in m.trend.itertuples()] if m.trend is not None and not m.trend.empty else [],
             "series": [
                 {"name": str(year), "kind": "cur", "fmt": "usd",
                  "values": [float(v) if _ok(v) else None for v in (m.trend["revpar"] if m.trend is not None and not m.trend.empty else [])]},
                 {"name": str(year - 1), "kind": "prior", "fmt": "usd",
                  "values": [float(v) if _ok(v) else None for v in (m.trend["revpar_ly"] if m.trend is not None and not m.trend.empty else [])]},
             ], "src": hotel_src_short},
        ],
        "table": {
            "title": f"Hotel performance, {P_THIS} vs. last year",
            "sub": hspan,
            "cols": ["Measure", P_YTD_CAP, "Same period last year", "Change"],
            "num": [1, 2, 3],
            "rows": [
                ["Revenue per available room", usd(h.get("revpar")), usd(hp.get("revpar")), f"{glyph(h.get('revpar_pct'))} {spct(h.get('revpar_pct'))}"],
                ["Occupancy", pct(h.get("occ")), pct(hp.get("occ")), f"{glyph(h.get('occ_pts'))} {pts(h.get('occ_pts'))}"],
                ["Average nightly rate", usd(h.get("adr")), usd(hp.get("adr")), f"{glyph(h.get('adr_pct'))} {spct(h.get('adr_pct'))}"],
                ["Room nights sold", num(h.get("demand")), num(hp.get("demand")), f"{glyph(h.get('demand_pct'))} {spct(h.get('demand_pct'))}"],
                ["Room nights available", num(h.get("supply")), num(hp.get("supply")), f"{glyph(h.get('supply_pct'))} {spct(h.get('supply_pct'))}"],
                ["Room revenue", usd(rev), usd(hp.get("revenue")), f"{glyph(h.get('revenue_pct'))} {spct(h.get('revenue_pct'))}"],
                ["Nights at 80% or higher", num(h.get("d80")), num(hp.get("d80")), f"{(h.get('d80') or 0) - (hp.get('d80') or 0):+d} nights"],
                ["Nights at 90% or higher", num(h.get("d90")), num(hp.get("d90")), f"{(h.get('d90') or 0) - (hp.get('d90') or 0):+d} nights"],
            ],
            "src": hotel_src,
        },
        "means": {
            "enh": [
                {"tag": "Revenue", "text": f"Room revenue is {spct(h.get('revenue_pct'))} vs. last year, with {pts(h.get('rev_from_adr_pts'))[:-7] if h.get('rev_from_adr_pts') is not None else 'n/a'} of the {spct(h.get('revpar_pct'))} revenue per room gain coming from rate.", "src": hotel_src_short},
                {"tag": "City", "text": f"Comp-set room revenue equals about {usd_big(tot)} in transient occupancy tax at 10% and {usd_big(tbid)} at the blended 1.25% TBID rate.", "src": "PULSE calculation"},
                {"tag": "Visitors", "text": f"Visitor spending of {usd_big(dfy_spend)} moved {spct(dfy.get('spend_pct'))} on the same months last year.", "src": f"Datafy, {dfy_mspan}"},
            ],
            "needs": [
                {"tag": "Occupancy", "text": f"Rooms sold changed {spct(h.get('demand_pct'))}; occupancy of {pct(h.get('occ'))} leaves about {pct(100 - h['occ']) if h.get('occ') else 'n/a'} of room nights unsold.", "src": hotel_src_short},
                {"tag": "Measurement", "text": "City-reported transient occupancy tax collections, to replace the 10% estimate with the actual figure.", "src": "PULSE data note"},
            ],
        },
        "cc": [
            _cc("Gain", f"Revenue per available room is {spct(h.get('revpar_pct'))} vs. last year.", None, hotel_src_short),
            _cc("Gap", f"Occupancy is {pct(h.get('occ'))}; one point of occupancy equals about {num(h['supply'] * 0.01) if h.get('supply') else 'n/a'} room nights in this window.",
                "Arithmetic on the window's available room nights.", hotel_src_short,
                calc=f"{num(h.get('supply'))} room nights available × 1% = {num(h['supply'] * 0.01) if h.get('supply') else 'n/a'}"),
            _cc("Data limit", f"STR's daily file ends {ap_date(str_through) if str_through is not None else 'n/a'}; later dates come from CoStar's daily grid for the same hotels.",
                "Both sources cover the same hotels; the source of each date is labeled.", SRC["costar_daily"]) if costar_ext else
            _cc("Data limit", "Hotel data covers the select hotels, not every lodging property in the city.", "CoStar's submarket report covers the wider market.", SRC["costar_rpt"]),
            _cc("Data limit", "The 10% tax figure is an estimate on comp-set hotels, not city collections.", "City-reported collections are not loaded in PULSE.", "PULSE calculation"),
            _cc("Explanation", "Datafy measures card spending and location-modeled visits; STR measures hotel rooms. Compare trends, not totals.", None, "PULSE data note"),
        ],
        "advice": [
            _adv("Growth", f"Revenue per available room is {spct(h.get('revpar_pct'))} vs. last year.", None, hotel_src_short),
            _adv("Opportunity", f"Occupancy is {pct(h.get('occ'))}; each additional point is about {num(h['supply'] * 0.01) if h.get('supply') else 'n/a'} room nights at the current rate.",
                "Target the nights with the most open rooms (see Hotel market and One more night).", hotel_src_short,
                calc=f"{num(h.get('supply'))} room nights available × 1% = {num(h['supply'] * 0.01) if h.get('supply') else 'n/a'}"),
            _adv("Quick win", "Report room revenue and the tax estimate each month alongside occupancy.", None, "PULSE"),
            _adv("Early hurdle", f"STR's daily file ends {ap_date(str_through) if str_through is not None else 'n/a'}; later dates come from CoStar's daily grid for the same hotels.",
                "Load each new STR file as it arrives; CoStar fills the gap until then.", SRC["costar_daily"]) if costar_ext else
            _adv("Early hurdle", "Hotel data covers the select hotels, not every lodging property in the city.", "Use CoStar's submarket report for the wider market.", SRC["costar_rpt"]),
            _adv("Pitfall", "Reading the 10% tax figure as city collections.", "Label it an estimate on comp-set hotels; use city-reported figures when available.", "PULSE calculation"),
            _adv("Explanation", "Datafy measures card spending and location-modeled visits; STR measures hotel rooms. Compare trends, not totals.", None, "PULSE data note"),
        ],
        "more": [],
        "notes": [
            "Hotel figures are rooms-weighted: occupancy = rooms sold ÷ rooms available; rate = revenue ÷ rooms sold.",
            "Year-over-year compares the same window shifted back 364 days.",
            "The transient occupancy tax and TBID figures are estimates on comp-set room revenue, not city-reported collections.",
        ],
    }
    if m.trend is not None and not m.trend.empty:
        s_today["more"].append({"kind": "table", "title": "Monthly hotel performance", "sub": f"{year} with last year's value",
                                "cols": ["Month", "Occupancy", "Last year", "Average rate", "Last year", "Revenue per room", "Last year"],
                                "num": [1, 2, 3, 4, 5, 6],
                                "rows": [[r.label, pct(r.occ), pct(r.occ_ly), usd(r.adr), usd(r.adr_ly), usd(r.revpar), usd(r.revpar_ly)]
                                         for r in m.trend.itertuples()],
                                "src": hotel_src_short})
    sections.append(s_today)

    # ---- 2. Who visits ------------------------------------------------
    age = demo[demo["dimension"] == "Age"].sort_values("share_pct", ascending=False) if not demo.empty else pd.DataFrame()
    inc = demo[demo["dimension"] == "Income"] if not demo.empty else pd.DataFrame()
    hi_inc = float(inc[inc["segment"].isin(["$100K-$150K", "$150K+"])]["share_pct"].sum()) if not inc.empty else None
    pins = []
    if dma_coords and not markets.empty:
        for r in markets.itertuples():
            ll = dma_coords.get(r.dma)
            if ll:
                x, y_ = _albers(ll[1], ll[0])
                pins.append({"name": ps.clean_market(r.dma), "v": float(r.share_pct), "x": round(x, 1), "y": round(y_, 1)})
    dpx, dpy = _albers(-117.698, 33.467)
    state_vals = {r.state: float(r.share_of_trips_pct) for r in states.itertuples()} if not states.empty else {}
    s_who = {
        "id": "who", "tab": "Who visits",
        "question": "Who visits Dana Point, and where do they come from?",
        "headline": (f"{top_mkt or 'Los Angeles'} leads visitor spending as half of trips start out of state" if ca_share is not None and 40 <= 100 - ca_share <= 60
                     else f"{top_mkt or 'Los Angeles'} leads visitor spending as {pct(100 - ca_share) if ca_share is not None else 'n/a'} of trips start out of state"),
        "dek": f"The top three markets deliver {pct(top3_share)} of visitor spending; {pct(overnight_pct)} of trips are overnight.",
        "byline_src": dfy_src,
        "lede": (f"{dateline} {top_mkt} accounts for {pct(top_mkt_share)} of visitor spending in Dana Point, and the top three "
                 f"feeder markets together deliver {pct(top3_share)}, according to Datafy ({dfy_period}). About "
                 f"{pct(100 - ca_share) if ca_share is not None else 'n/a'} of trips start outside California, and "
                 f"{pct(overnight_pct)} of trips include an overnight stay."),
        "lede_src": f"Source: {dfy_src}",
        "key": {"v": pct(top_mkt_share), "l": f"of visitor spending from {top_mkt}"},
        "why": ("Where visitors come from decides where marketing dollars travel furthest and which travelers are likely to "
                "stay overnight. Fly-in markets spend more per visitor than drive markets, so the mix matters as much as the volume. "
                f"In Datafy's market table, {_hi_lo_spend(dma_full)}."),
        "glance": [
            {"type": "waffle", "title": "Of every 100 trips to Dana Point",
             "sub": f"Datafy, {dfy_period}",
             "parts": [{"v": overnight_pct or 0, "l": "overnight trips"}, {"v": day_pct or 0, "l": "day trips"}],
             "src": dfy_src},
            {"type": "share", "title": "California vs. out-of-state trips", "sub": f"Share of trips by origin state, {dfy_period}",
             "parts": [{"v": ca_share or 0, "l": "From California"}, {"v": (100 - ca_share) if ca_share is not None else 0, "l": "From other states and countries"}],
             "src": dfy_src},
        ],
        "numbers": [
            _stat("Top market", pct(top_mkt_share), f"Visitor spending from {top_mkt}", f"Top three: {pct(top3_share)}", dfy_src),
            _stat("Trip type", pct(overnight_pct), "Overnight share of trips", f"Day trips {pct(day_pct)}", dfy_src),
            _stat("Origin", pct(100 - ca_share) if ca_share is not None else "n/a", "Trips from outside California", f"California {pct(ca_share)}", dfy_src),
            _stat("Loyalty", pct(repeat_pct), "Repeat visitors (card spending)", "Share of spenders seen more than once", SRC["datafy"]),
        ],
        "calc": [
            ["Top three markets", "Sum of the three largest spending shares",
             " + ".join(pct(v) for v in markets.head(3)["share_pct"]) + f" = {pct(top3_share)}" if not markets.empty else "n/a", dfy_src],
            ["Out-of-state trips", "100% − California share of trips", f"100% − {pct(ca_share)} = {pct(100 - ca_share) if ca_share is not None else 'n/a'}", dfy_src],
        ],
        "details": {
            "who": _det(age.iloc[0]["segment"].replace(" years old", "") if not age.empty else "n/a", f"largest age group, {pct(age.iloc[0]['share_pct']) if not age.empty else 'n/a'} of visitors",
                        f"Households earning $100,000 or more make up {pct(hi_inc)} of visitors.", f"Datafy, {demo_period or 'n/a'}"),
            "what": _det(pct(overnight_pct), "of trips are overnight", f"Day trips make up {pct(day_pct)}; the average trip lasts {los:.1f} days." if los else "", dfy_src),
            "when": _det(dfy_period, "Datafy reporting period", "Feeder markets and origins refresh with each Datafy export.", dfy_src),
            "where": _det(top_mkt or "n/a", f"{pct(top_mkt_share)} of visitor spending", f"Next: {', '.join(ps.clean_market(x) for x in markets['dma'].iloc[1:3])}." if len(markets) > 2 else "", dfy_src),
            "why": _det(pct(100 - ca_share) if ca_share is not None else "n/a", "of trips from outside California", "Out-of-state travelers are the likeliest to stay overnight and book hotels.", dfy_src),
            "how": _det("Card + location", "Datafy measurement", "Card transactions for spending; mobile location data for trips and origins.", SRC["datafy"]),
        },
        "map": {"title": "Where Dana Point's visitors come from",
                "sub": f"States shaded by share of trips; circles sized by share of visitor spending, {dfy_period}",
                "states": state_vals, "pins": pins, "home": {"x": round(dpx, 1), "y": round(dpy, 1), "name": "Dana Point"},
                "src": f"{dfy_src}. Boundaries: U.S. Census Bureau cartographic boundaries (us-atlas 3.0.1, Albers USA)."},
        "charts": [
            {"type": "hbar", "title": f"{top_mkt} sends the most visitor spending", "sub": f"Share of visitor spending by market, {dfy_period}",
             "rows": [{"l": ps.clean_market(r.dma), "v": float(r.share_pct), "fmt": "pct", "hi": i == 0} for i, r in enumerate(markets.itertuples())],
             "src": dfy_src},
            {"type": "hbar", "title": "Visitors skew 45 and older", "sub": f"Share of visitors by age, {demo_period or 'n/a'}",
             "rows": [{"l": r.segment.replace(" years old", ""), "v": float(r.share_pct), "fmt": "pct", "hi": i == 0} for i, r in enumerate(age.itertuples())],
             "src": f"Datafy, {demo_period or 'n/a'}"},
        ],
        "table": {"title": "Feeder markets: spending share, spend per visitor and stay", "sub": dma_period or "",
                  "cols": ["Market", "Share of visitor spending", "Average spend per visitor", "Average stay (days)"], "num": [1, 2, 3],
                  "rows": [[ps.clean_market(r.dma), pct(r.spending_share_pct), usd(r.avg_spend_usd), f"{r.avg_length_of_stay_days:.1f}" if _ok(r.avg_length_of_stay_days) else "n/a"]
                           for r in dma_full.itertuples()],
                  "src": f"Datafy, {dma_period or 'n/a'}"},
        "means": {
            "enh": [
                {"tag": "Marketing", "text": f"{top_mkt} delivers {pct(top_mkt_share)} of spending; the drive market is already the base.", "src": dfy_src},
                {"tag": "Hotels", "text": f"Fly-in markets such as Dallas-Fort Worth and Las Vegas show higher spend per visitor in Datafy's market table.", "src": f"Datafy, {dma_period or 'n/a'}"},
            ],
            "needs": [
                {"tag": "Overnight", "text": f"Only {pct(overnight_pct)} of trips are overnight; converting day trips is the larger pool.", "src": dfy_src},
                {"tag": "Measurement", "text": "Datafy, STR and CoStar count different things; compare within each source.", "src": "PULSE data note"},
            ]},
        "cc": [
            _cc("Gain", f"Out-of-state travelers make up about {pct(oos)} of visitor spending.", None, dfy_src),
            _cc("Gap", f"{pct(day_pct)} of trips are day trips; {pct(overnight_pct)} are overnight.", None, dfy_src),
            _cc("Gain", f"{top_mkt} leads all markets at {pct(top_mkt_share)} of spending.", None, dfy_src),
            _cc("Data limit", "Market shares shift with each Datafy export period.", "Comparisons use like periods only.", SRC["datafy"]),
            _cc("Data limit", "Share of trips and share of spending are different measures.", "Each chart names its measure.", SRC["datafy"]),
            _cc("Explanation", "Datafy's demographic profile covers visitors observed with card and location data, not every visitor.", None, SRC["datafy"]),
        ],
        "advice": [
            _adv("Growth", f"Out-of-state travelers make up about {pct(oos)} of visitor spending.", None, dfy_src),
            _adv("Opportunity", f"{pct(day_pct)} of trips are day trips.", "Pair day-trip markets with overnight offers.", dfy_src),
            _adv("Quick win", f"Keep {top_mkt} in always-on media; it already leads spending.", None, dfy_src),
            _adv("Early hurdle", "Market shares shift with each Datafy export period.", "Compare like periods only.", SRC["datafy"]),
            _adv("Pitfall", "Treating share of trips and share of spending as the same measure.", "Label each chart's measure.", SRC["datafy"]),
            _adv("Explanation", "Datafy's demographic profile covers visitors observed with card and location data, not every visitor.", None, SRC["datafy"]),
        ],
        "more": [],
        "notes": ["Market names follow Nielsen DMA areas.", "Demographics use Datafy's 2025-to-date profile."],
    }
    if not states.empty:
        s_who["more"].append({"kind": "chart", "chart": {"type": "hbar", "title": "Top origin states", "sub": f"Share of trips, {dfy_period}",
                              "rows": [{"l": r.state, "v": float(r.share_of_trips_pct), "fmt": "pct", "hi": r.state == "California"} for r in states.head(12).itertuples()],
                              "src": dfy_src}})
    if not countries.empty:
        s_who["more"].append({"kind": "chart", "chart": {"type": "hbar", "title": "International visitors by country", "sub": f"Share of international visitors, {dfy_period}",
                              "rows": [{"l": r.country, "v": float(r.share_pct), "fmt": "pct", "hi": False} for r in countries.itertuples()],
                              "src": dfy_src}})
    if not inc.empty:
        s_who["more"].append({"kind": "chart", "chart": {"type": "hbar", "title": "Visitors by household income", "sub": f"Share of visitors, {demo_period}",
                              "rows": [{"l": r.segment, "v": float(r.share_pct), "fmt": "pct", "hi": False} for r in inc.itertuples()],
                              "src": f"Datafy, {demo_period}"}})
    sections.append(s_who)

    # ---- 3. Visitor spending -------------------------------------------
    dt = m.dfy_trend if m.dfy_trend is not None else pd.DataFrame()
    receipt = []
    if not cats.empty:
        for r in cats.head(6).itertuples():
            receipt.append({"l": r.category, "v": float(r.share_pct)})
        rest = float(cats.iloc[6:]["share_pct"].sum()) if len(cats) > 6 else 0
        if rest:
            receipt.append({"l": "All other categories", "v": rest})
    s_spend = {
        "id": "spend", "tab": "Visitor spending",
        "question": "How much do visitors spend, and where does each dollar go?",
        "headline": f"Visitors spent {usd_big(dfy_spend)} with lodging and dining taking {pct((acc_share or 0) + (dine_share or 0))}",
        "dek": f"Spending moved {spct(dfy.get('spend_pct'))} on the same months last year; July was the peak month.",
        "byline_src": f"Datafy, {dfy_mspan}",
        "lede": (f"{dateline} Visitors spent {usd_big(dfy_spend)} in Dana Point from {dfy_mspan}, {spct(dfy.get('spend_pct'))} "
                 f"from the same months last year, according to Datafy. Accommodations took {pct(acc_share)} of every visitor dollar "
                 f"and dining and nightlife {pct(dine_share)}. The peak month was {ap_month(dfy['peak_month']) if dfy.get('peak_month') is not None else 'n/a'} "
                 f"at {usd_big(dfy.get('peak_spend'))}."),
        "lede_src": f"Source: {SRC['datafy']}, {dfy_mspan}",
        "key": {"v": usd_big(dfy_spend), "l": f"visitor spending, {dfy_mspan}"},
        "why": ("Spending shows where visitor dollars land in the local economy. Lodging and dining together account for most "
                "of it, so hotel and restaurant health are the two clearest reads on the visitor economy."),
        "glance": [{"type": "receipt", "title": "Where each visitor dollar goes", "sub": f"Share of visitor spending by category, {dfy_period}",
                    "items": receipt, "src": dfy_src}],
        "numbers": [
            _stat(f"{dfy_mspan}", usd_big(dfy_spend), "Visitor spending", f"{glyph(dfy.get('spend_pct'))} {spct(dfy.get('spend_pct'))} vs. last year", f"Datafy, {dfy_mspan}"),
            _stat(f"{dfy_mspan}", num_big(dfy.get("visitor_days")), "Visitor days", f"{glyph(dfy.get('vd_pct'))} {spct(dfy.get('vd_pct'))} vs. last year", f"Datafy, {dfy_mspan}"),
            _stat(dfy_period, pct(acc_share), "Accommodations share", "Largest category", dfy_src),
            _stat(dfy_period, pct(dine_share), "Dining and nightlife share", "Second largest", dfy_src),
        ],
        "calc": [
            ["Change in visitor spending", "(This period ÷ same months last year) − 1",
             f"({usd(dfy_spend)} ÷ {usd(dfy.get('prior_spend'))}) − 1 = {spct(dfy.get('spend_pct'))}", f"Datafy, {dfy_mspan}"],
            ["Lodging and dining share", "Accommodations + dining and nightlife", f"{pct(acc_share)} + {pct(dine_share)} = {pct((acc_share or 0) + (dine_share or 0))}", dfy_src],
        ],
        "details": {
            "who": _det(num_big(dfy.get("visitor_days")), "visitor days", "Visitors observed in Dana Point by Datafy's location panel.", f"Datafy, {dfy_mspan}"),
            "what": _det(usd_big(dfy_spend), "card-observed visitor spending", f"{spct(dfy.get('spend_pct'))} vs. the same months last year.", f"Datafy, {dfy_mspan}"),
            "when": _det(ap_month(dfy["peak_month"]) if dfy.get("peak_month") is not None else "n/a", f"peak month, {usd_big(dfy.get('peak_spend'))}", "Summer months lead spending each year.", f"Datafy, {dfy_mspan}"),
            "where": _det(pct(acc_share), "to accommodations", f"Dining and nightlife {pct(dine_share)}; grocery and department stores follow.", dfy_src),
            "why": _det(pct((acc_share or 0) + (dine_share or 0)), "lodging plus dining", "Most visitor dollars go to hotels and restaurants.", dfy_src),
            "how": _det("Card data", "Datafy spending method", "Spending is observed from card transactions by visitors, not modeled from surveys.", SRC["datafy"]),
        },
        "charts": [
            {"type": "vbar", "title": "Monthly visitor spending vs. last year", "sub": f"Datafy visitor spending by month, {dfy_mspan}",
             "x": [r.label.split(" ")[0] for r in dt.itertuples()],
             "series": [{"name": "This year", "kind": "cur", "fmt": "usdbig", "values": [float(v) for v in dt["spending_usd"]]},
                        {"name": "Last year", "kind": "prior", "fmt": "usdbig", "values": [float(v) if _ok(v) else None for v in dt["spend_ly"]]}] if not dt.empty else [],
             "src": f"Datafy, {dfy_mspan}"},
            {"type": "hbar", "title": f"Accommodations take {pct(acc_share)} of spending", "sub": f"Share of visitor spending by category, {dfy_period}",
             "rows": [{"l": r.category, "v": float(r.share_pct), "fmt": "pct", "hi": i == 0} for i, r in enumerate(cats.itertuples())],
             "src": dfy_src},
        ],
        "table": {"title": "Visitor spending by category", "sub": dfy_period,
                  "cols": ["Category", "Share of spending", "Average spend per visitor"], "num": [1, 2],
                  "rows": [[r.category, pct(r.share_pct), usd(r.avg_spend_usd)] for r in cats.itertuples()],
                  "src": dfy_src},
        "means": {
            "enh": [{"tag": "Lodging", "text": f"Accommodations lead at {pct(acc_share)} of spending.", "src": dfy_src},
                    {"tag": "Dining", "text": f"Dining and nightlife hold {pct(dine_share)}.", "src": dfy_src}],
            "needs": [{"tag": "Shoulder months", "text": "The lowest spending months fall in winter; see the monthly chart.", "src": f"Datafy, {dfy_mspan}"},
                      {"tag": "Measurement", "text": "Spending and visitor days use different methods; compare growth rates, not dollars per day.", "src": "PULSE data note"}]},
        "cc": [
            _cc("Gain", f"Visitor spending is {spct(dfy.get('spend_pct'))} on last year.", None, f"Datafy, {dfy_mspan}"),
            _cc("Gap", "Winter months record the lowest visitor spending in the monthly series.", "See the monthly chart for each month's value.", f"Datafy, {dfy_mspan}"),
            _cc("Gain", f"Accommodations ({pct(acc_share)}) and dining ({pct(dine_share)}) lead the category mix.", None, dfy_src),
            _cc("Data limit", "The latest month can be partial until Datafy's next refresh.", "Comparisons use complete months.", SRC["datafy"]),
            _cc("Data limit", "Spending and visitor days come from different panels.", "Compare growth rates, not dollars per visitor day.", "PULSE data note"),
            _cc("Explanation", f"Hotel room revenue and visitor spending move together month to month (correlation {m.link['r']:.2f})." if m.link else "Hotel room revenue and visitor spending tend to move together.", None, "PULSE calculation, STR and Datafy"),
        ],
        "advice": [
            _adv("Growth", f"Visitor spending is {spct(dfy.get('spend_pct'))} on last year.", None, f"Datafy, {dfy_mspan}"),
            _adv("Opportunity", "Winter months spend the least.", "Package winter events with lodging.", f"Datafy, {dfy_mspan}"),
            _adv("Quick win", "Share the category mix with restaurant and retail partners.", None, dfy_src),
            _adv("Early hurdle", "The latest month is often partial until Datafy's next refresh.", "Use complete months for comparisons.", SRC["datafy"]),
            _adv("Pitfall", "Dividing spending by visitor days to get a daily spend figure.", "The two measures use different panels; compare growth rates only.", "PULSE data note"),
            _adv("Explanation", f"Hotel room revenue and visitor spending move together month to month (correlation {m.link['r']:.2f})." if m.link else "Hotel room revenue and visitor spending tend to move together.", None, "PULSE calculation, STR and Datafy"),
        ],
        "more": [],
        "notes": ["Datafy spending is card-observed and excludes cash.", "Category shares come from Datafy's latest export period."],
    }
    if not dt.empty:
        s_spend["more"].append({"kind": "table", "title": "Monthly visitor spending and visitor days", "sub": dfy_mspan,
                                "cols": ["Month", "Visitor spending", "Last year", "Visitor days", "Last year"], "num": [1, 2, 3, 4],
                                "rows": [[r.label, usd(r.spending_usd), usd(r.spend_ly), num(r.visitor_days), num(r.vd_ly)] for r in dt.itertuples()],
                                "src": f"Datafy, {dfy_mspan}"})
    sections.append(s_spend)

    # ---- 4. City revenue and jobs --------------------------------------
    lh = bls[bls["series_name"] == "CA Leisure & Hospitality Employment"] if not bls.empty else pd.DataFrame()
    ac = bls[bls["series_name"] == "CA Accommodation Employment"] if not bls.empty else pd.DataFrame()
    lh_last = lh.iloc[-1] if not lh.empty else None
    ac_last = ac.iloc[-1] if not ac.empty else None
    lh_m = ap_month(pd.Timestamp(int(lh_last.year), int(lh_last.month), 1)) if lh_last is not None else "n/a"
    s_city = {
        "id": "city", "tab": "City revenue and jobs",
        "question": "What does hotel and visitor activity return to the city and the workforce?",
        "headline": f"Comp-set room revenue equals about {usd_big(tot)} in occupancy tax {P_THIS}",
        "dek": f"The TBID assessment at the blended 1.25% rate equals about {usd_big(tbid)}; California leisure and hospitality jobs are {spct(lh_last.yoy_chg_pct) if lh_last is not None else 'n/a'} vs. last year.",
        "byline_src": "PULSE calculation on STR and CoStar room revenue; BLS",
        "lede": (f"{dateline} Room revenue at Visit Dana Point's select hotels reached {usd_big(rev)} from {hspan}. At the city's 10% "
                 f"transient occupancy tax rate, that equals about {usd_big(tot)}, compared with {usd_big(tot_p)} on the same dates last year. "
                 f"At the blended 1.25% TBID rate it equals about {usd_big(tbid)}. California employed {num(lh_last.value_thousands * 1000) if lh_last is not None else 'n/a'} "
                 f"people in leisure and hospitality in {lh_m}, {spct(lh_last.yoy_chg_pct) if lh_last is not None else 'n/a'} vs. a year earlier (BLS)."),
        "lede_src": f"Source: {hotel_src}; PULSE calculation; {SRC['bls']}",
        "key": {"v": usd_big(tot), "l": "transient occupancy tax at 10%, comp-set hotels (estimate)"},
        "why": ("Hotel stays fund city services through the transient occupancy tax and fund destination marketing through the TBID. "
                "These estimates cover only the select hotels in this report, so city-reported collections will be higher."),
        "glance": [{"type": "versus", "title": f"Estimated occupancy tax, {P_THIS} vs. last year", "sub": f"{hspan} vs. same dates last year",
                    "a": {"l": P_YTD_CAP, "v": tot or 0, "fmt": "usdbig"}, "b": {"l": "Last year", "v": tot_p or 0, "fmt": "usdbig"},
                    "src": "PULSE calculation (room revenue × 10%)"}],
        "numbers": [
            _stat("Estimate, comp-set hotels", usd_big(tot), "Transient occupancy tax at 10%", f"{glyph(h.get('revenue_pct'))} {spct(h.get('revenue_pct'))} vs. last year", "PULSE calculation"),
            _stat("Estimate, comp-set hotels", usd_big(tbid), "TBID at blended 1.25%", "Rate tiers 1.0%, 1.5% and 2.0%", "PULSE calculation"),
            _stat(f"California, {lh_m}", f"{lh_last.value_thousands / 1000:.2f}M" if lh_last is not None else "n/a", "Leisure and hospitality jobs",
                  f"{glyph(lh_last.yoy_chg_pct if lh_last is not None else None)} {spct(lh_last.yoy_chg_pct) if lh_last is not None else 'n/a'} vs. last year", SRC["bls"]),
            _stat(f"California, {lh_m}", f"{ac_last.value_thousands:,.0f}K" if ac_last is not None else "n/a", "Accommodation jobs",
                  f"{glyph(ac_last.yoy_chg_pct if ac_last is not None else None)} {spct(ac_last.yoy_chg_pct) if ac_last is not None else 'n/a'} vs. last year", SRC["bls"]),
        ],
        "calc": [
            ["Transient occupancy tax (estimate)", "Room revenue × 10%", f"{usd(rev)} × 10% = {usd(tot)}", "PULSE calculation"],
            ["TBID assessment (estimate)", "Room revenue × 1.25% blended", f"{usd(rev)} × 1.25% = {usd(tbid)}", "PULSE calculation"],
            ["Last year's tax (estimate)", "Prior room revenue × 10%", f"{usd(hp.get('revenue'))} × 10% = {usd(tot_p)}", "PULSE calculation"],
        ],
        "details": {
            "who": _det(f"{n_hotels or 'n/a'} hotels", "comp-set hotels in the estimate", "Visit Dana Point's select hotels only.", "CoStar participation roster"),
            "what": _det(usd_big(tot), "occupancy tax estimate at 10%", f"Plus {usd_big(tbid)} at the blended TBID rate.", "PULSE calculation"),
            "when": _det(hspan, "room revenue period", "Same window as the hotel section.", hotel_src_short),
            "where": _det("City of Dana Point", "tax jurisdiction", "TBID tiers: 1.0% under $200, 1.5% from $200 to $399.99, 2.0% at $400 and up.", "TBID assessment structure"),
            "why": _det(usd_big(rev), "room revenue base", "Both tax and assessment are charged on room revenue.", hotel_src_short),
            "how": _det("× 10% and × 1.25%", "rates applied", "Applied to rooms-weighted comp-set revenue; not city-reported collections.", "PULSE calculation"),
        },
        "charts": [
            {"type": "line", "title": "California leisure and hospitality employment", "sub": "Jobs, thousands, by month",
             "x": [f"{_AP_MONTHS[int(r.month) - 1][:3]} {str(int(r.year))[2:]}" for r in lh.tail(24).itertuples()],
             "series": [{"name": "Leisure and hospitality", "kind": "cur", "fmt": "num", "values": [float(r.value_thousands) for r in lh.tail(24).itertuples()]}],
             "src": SRC["bls"]} if not lh.empty else None,
        ],
        "table": {"title": "Estimated city revenue from comp-set room revenue", "sub": hspan,
                  "cols": ["Measure", P_YTD_CAP, "Same dates last year", "Change"], "num": [1, 2, 3],
                  "rows": [["Room revenue", usd(rev), usd(hp.get("revenue")), f"{glyph(h.get('revenue_pct'))} {spct(h.get('revenue_pct'))}"],
                           ["Transient occupancy tax at 10%", usd(tot), usd(tot_p), f"{glyph(h.get('revenue_pct'))} {spct(h.get('revenue_pct'))}"],
                           ["TBID at 1.25%", usd(tbid), usd(hp.get("revenue") * 0.0125 if hp.get("revenue") else None), f"{glyph(h.get('revenue_pct'))} {spct(h.get('revenue_pct'))}"]],
                  "src": "PULSE calculation on " + hotel_src_short},
        "means": {
            "enh": [{"tag": "City", "text": f"Estimated occupancy tax on comp-set hotels is {spct(h.get('revenue_pct'))} vs. last year.", "src": "PULSE calculation"},
                    {"tag": "Workforce", "text": f"California leisure and hospitality jobs are {spct(lh_last.yoy_chg_pct) if lh_last is not None else 'n/a'} vs. last year.", "src": SRC["bls"]}],
            "needs": [{"tag": "Measurement", "text": "City-reported monthly TOT and TBID collections to confirm the estimates.", "src": "PULSE data note"},
                      {"tag": "Workforce", "text": "Dana Point or Orange County job counts; BLS figures here are statewide.", "src": SRC["bls"]}]},
        "cc": [
            _cc("Gain", f"Room revenue, and with it the tax estimate, is {spct(h.get('revenue_pct'))} vs. last year.", None, hotel_src_short),
            _cc("Explanation", "The tax estimate is a fixed share of room revenue, so it moves with rate as well as rooms sold.", None, hotel_src_short),
            _cc("Data limit", "No city-reported collections are loaded in PULSE.", "Figures here are estimates.", "PULSE data note"),
            _cc("Data limit", "The estimate covers comp-set hotels only, not the city's total collections.", "Labeled comp-set hotels throughout.", "PULSE calculation"),
            _cc("Explanation", "The TBID blended rate approximates three rate tiers; actual assessments depend on each night's rate.", None, "TBID assessment structure"),
        ],
        "advice": [
            _adv("Growth", f"Room revenue, and with it the tax estimate, is {spct(h.get('revenue_pct'))} vs. last year.", None, hotel_src_short),
            _adv("Opportunity", "Rate gains lift tax revenue without adding rooms.", None, hotel_src_short),
            _adv("Quick win", "Publish the estimate monthly beside occupancy.", None, "PULSE"),
            _adv("Early hurdle", "No city-reported collections are loaded yet.", "Add the city's monthly TOT report as a source.", "PULSE data note"),
            _adv("Pitfall", "Quoting the estimate as the city's total collections.", "Always label it comp-set hotels only.", "PULSE calculation"),
            _adv("Explanation", "The TBID blended rate approximates three rate tiers; actual assessments depend on each night's rate.", None, "TBID assessment structure"),
        ],
        "more": [], "notes": ["Employment figures are statewide (BLS), not Dana Point.", "TOT 10% and TBID 1.25% follow the PULSE formula sheet."],
    }
    s_city["charts"] = [c for c in s_city["charts"] if c]
    sections.append(s_city)

    # ---- 5. Hotel market ----------------------------------------------
    towers = [{"l": r.label, "v": float(r.occ), "hi": r.label in ("Sun", "Mon")} for r in dow.itertuples()] if not dow.empty else []
    ptab = peers.get("table") if peers else None
    pk = peers.get("dp") or {}
    s_hotel = {
        "id": "hotel", "tab": "Hotel market",
        "question": "How are Dana Point hotels performing against peers and the wider market, and when is there room?",
        "headline": (f"Dana Point leads coastal peers on rate while {LOW_NIGHTS} nights have the most room" if peers.get("rank_adr") == 1
                     else f"Dana Point ranks {ps._ordinal(peers['rank_adr']) if peers.get('rank_adr') else 'n/a'} of {peers.get('n_markets', 'n/a')} coastal peers on rate while {LOW_NIGHTS} nights have the most room"),
        "dek": f"Friday and Saturday ran {pct(fs_occ)} full vs. {pct(sm_occ)} on Sunday and Monday nights.",
        "byline_src": f"{hotel_src_short}; STR weekly peers; CoStar",
        "lede": (f"{dateline} Dana Point ranks {'first' if peers.get('rank_revpar') == 1 else (ps._ordinal(peers['rank_revpar']) if peers.get('rank_revpar') else 'n/a')} of {peers.get('n_markets', 'n/a')} coastal peer markets on revenue per available room "
                 f"and {ps._ordinal(peers['rank_occ']) if peers.get('rank_occ') else 'n/a'} on occupancy across STR's weekly reports. "
                 f"Year to date through {bthrough or 'n/a'}, Dana Point's select hotels earned {usd(comp_ytd.get('revpar'))} per available room, "
                 f"against {usd(sub_row['revpar']) if sub_row else 'n/a'} for the Newport Beach/Dana Point submarket (CoStar). Weeknights have the "
                 f"most room: Sunday and Monday nights ran {pct(sm_occ)} full vs. {pct(fs_occ)} on Friday and Saturday."),
        "lede_src": f"Source: {SRC['str_weekly']}; {SRC['costar_rpt']}; {hotel_src}",
        "key": {"v": f"{(fs_occ - sm_occ):.1f} points" if fs_occ and sm_occ else "n/a", "l": "Friday–Saturday vs. Sunday–Monday occupancy gap"},
        "why": ("Dana Point's premium is rate, not volume. Rooms are fullest on weekends and emptiest Sunday and Monday, so the nights with "
                "the most open rooms are where added demand would show up fastest in revenue."),
        "glance": [
            {"type": "towers", "title": "How full Dana Point hotels are, night by night",
             "sub": f"Average occupancy by night of the week, {hspan} · Sunday and Monday highlighted",
             "rows": towers, "src": hotel_src_short},
            {"type": "versus", "title": "Revenue per room vs. the submarket", "sub": f"Year to date through {bthrough or 'n/a'}",
             "a": {"l": "Dana Point hotels", "v": comp_ytd.get("revpar") or 0, "fmt": "usd"},
             "b": {"l": "Newport Beach/Dana Point submarket", "v": (sub_row or {}).get("revpar") or 0, "fmt": "usd"},
             "src": f"{SRC['costar_rpt']}; {hotel_src_short}"},
        ],
        "numbers": [
            _stat("Coastal peers", f"#{peers.get('rank_revpar', 'n/a')} of {peers.get('n_markets', 'n/a')}", "Revenue per available room rank",
                  f"Revenue index {peers['rgi']:.0f} (100 = peer average)" if peers.get("rgi") else "", SRC["str_weekly"]),
            _stat("Coastal peers", usd(pk.get("adr")), "Average nightly rate", f"Peer average {usd((peers.get('peer_avg') or {}).get('adr'))}", SRC["str_weekly"]),
            _stat(f"Year to date, {bthrough or ''}", usd(comp_ytd.get("revpar")), "Revenue per room, Dana Point hotels",
                  f"Submarket {usd((sub_row or {}).get('revpar'))}", SRC["costar_rpt"]),
            _stat("Night of week", pct(sm_occ), "Sunday–Monday occupancy", f"Friday–Saturday {pct(fs_occ)}", hotel_src_short),
        ],
        "calc": [
            ["Sunday–Monday occupancy", "(Sun + Mon rooms sold) ÷ (Sun + Mon rooms available)",
             f"({num(sun['demand'])} + {num(mon['demand'])}) ÷ ({num(sun['supply'])} + {num(mon['supply'])}) = {pct(sm_occ)}" if sun is not None else "n/a", hotel_src_short],
            ["Friday–Saturday occupancy", "(Fri + Sat rooms sold) ÷ (Fri + Sat rooms available)",
             f"{num(fri['demand'].sum())} ÷ {num(fri['supply'].sum())} = {pct(fs_occ)}" if not fri.empty else "n/a", hotel_src_short],
            ["Revenue index vs. peers", "Dana Point revenue per room ÷ peer average × 100",
             f"{usd(pk.get('revpar'))} ÷ {usd((peers.get('peer_avg') or {}).get('revpar'))} × 100 = {peers['rgi']:.0f}" if peers.get("rgi") else "n/a", SRC["str_weekly"]],
        ],
        "details": {
            "who": _det(f"{peers.get('n_markets', 'n/a')} markets", "STR coastal peer set", "Dana Point, Newport Beach, La Jolla, Santa Barbara, Huntington Beach and Monterey-Carmel.", SRC["str_weekly"]),
            "what": _det(usd(pk.get("revpar")), "Dana Point revenue per room across peer weeks", f"Rank {peers.get('rank_revpar', 'n/a')} on revenue, {peers.get('rank_occ', 'n/a')} on occupancy.", SRC["str_weekly"]),
            "when": _det(f"{len(peers.get('weeks', []))} weeks", "STR weekly reports in the window", f"Hotel daily data runs {hspan}.", SRC["str_weekly"]),
            "where": _det("Newport Beach/Dana Point", "CoStar submarket", "CoStar's submarket, Orange County and U.S. reports set the wider benchmark.", SRC["costar_rpt"]),
            "why": _det(f"{(fs_occ - sm_occ):.1f} points" if fs_occ and sm_occ else "n/a", f"Fri–Sat {pct(fs_occ)} − Sun–Mon {pct(sm_occ)}", "The open rooms are concentrated early in the week.", hotel_src_short),
            "how": _det("Rooms-weighted", "STR and CoStar method", "Occupancy, rate and revenue per room from rooms sold, rooms available and revenue.", hotel_src_short),
        },
        "heat": {"title": "Weeknights have the most room", "sub": f"Occupancy by month, {year} · weeknights = Sunday–Thursday",
                 "cols": ["Weeknights", "Friday–Saturday", "All nights"], "rows": heat_rows, "src": hotel_src_short},
        "charts": [
            {"type": "vbar", "title": "Compression nights by quarter", "sub": "Nights at 80% or higher and 90% or higher occupancy",
             "x": [r.quarter.replace("-", " ") for r in comp.itertuples()],
             "series": [{"name": "80% or higher", "kind": "cur", "fmt": "num", "values": [int(r.d80) for r in comp.itertuples()]},
                        {"name": "90% or higher", "kind": "alt", "fmt": "num", "values": [int(r.d90) for r in comp.itertuples()]}] if not comp.empty else [],
             "src": hotel_src_short + " (latest quarter partial)"},
        ],
        "table": {"title": "Hotel performance: Dana Point vs. submarket, county and nation", "sub": f"Year to date through {bthrough or 'n/a'}",
                  "cols": ["Market", "Occupancy", "Average nightly rate", "Revenue per available room", "Change in revenue per room"], "num": [1, 2, 3, 4],
                  "hl": 0,
                  "rows": [[r.market, pct(r.occ), usd(r.adr), usd(r.revpar), f"{glyph(r.revpar_pct)} {spct(r.revpar_pct)}"] for r in btab.itertuples()] if btab is not None and not btab.empty else [],
                  "src": f"{SRC['costar_rpt']} (report {bench.get('report_date', 'n/a')}); Dana Point row from {hotel_src_short}"},
        "means": {
            "enh": [{"tag": "Rate", "text": (f"Dana Point's average rate of {usd(pk.get('adr'))} leads the coastal peer set." if peers.get("rank_adr") == 1
                                     else f"Dana Point's average rate of {usd(pk.get('adr'))} ranks {ps._ordinal(peers['rank_adr']) if peers.get('rank_adr') else 'n/a'} in the coastal peer set."), "src": SRC["str_weekly"]},
                    {"tag": "Events", "text": (f"Sunday and Monday ran {pct(sm_occ)} full, leaving the most open rooms for events and groups." if SM_LOW
                                       else f"Sunday and Monday ran {pct(sm_occ)} full; {LOW_NIGHTS} nights had the most open rooms in this window."), "src": hotel_src_short}],
            "needs": [{"tag": "Occupancy", "text": f"Occupancy ranks {ps._ordinal(peers['rank_occ']) if peers.get('rank_occ') else 'n/a'} of {peers.get('n_markets', 'n/a')} peers.", "src": SRC["str_weekly"]},
                      {"tag": "Groups", "text": f"Group business took {pct(grp_share)} of occupied rooms in this window (CoStar segmentation).", "src": SRC["costar_seg"]}]},
        "cc": [
            _cc("Gain", f"Revenue per room is {spct(comp_ytd.get('revpar_pct'))} year to date, vs. {spct((sub_row or {}).get('revpar_pct'))} for the submarket.", None, SRC["costar_rpt"]),
            _cc("Gap", f"Sunday and Monday occupancy trails Friday and Saturday by {(fs_occ - sm_occ):.1f} points." if fs_occ and sm_occ else "Early-week nights have the most open rooms.",
                "Night-of-week averages over the window.", hotel_src_short),
            _cc("Data limit", "Peer weekly reports are missing for some weeks.", "Peer figures use every week on file.", SRC["str_weekly"]),
            _cc("Data limit", "Dana Point's select hotels and the whole submarket differ in level.", "Compare trends and indexes, not raw levels.", SRC["costar_rpt"]),
            _cc("Explanation", "STR and CoStar cover different hotel sets at different grains; compare trends, not levels.", None, "PULSE data note"),
        ],
        "advice": [
            _adv("Growth", f"Revenue per room is {spct(comp_ytd.get('revpar_pct'))} year to date, vs. {spct((sub_row or {}).get('revpar_pct'))} for the submarket.", None, SRC["costar_rpt"]),
            _adv("Opportunity", f"Sunday–Monday occupancy trails Friday–Saturday by {(fs_occ - sm_occ):.1f} points." if fs_occ and sm_occ else "Early-week nights have the most room.",
                "Schedule events and group arrivals Sunday through Tuesday.", hotel_src_short),
            _adv("Quick win", "Add early-week stay offers to existing campaigns.", None, hotel_src_short),
            _adv("Early hurdle", "Peer weekly reports are missing for some weeks.", "Report the number of weeks used beside every peer figure.", SRC["str_weekly"]),
            _adv("Pitfall", "Comparing Dana Point's select hotels with the whole submarket on level.", "Compare trends and indexes, not raw levels.", SRC["costar_rpt"]),
            _adv("Explanation", "STR and CoStar cover different hotel sets at different grains; compare trends, not levels.", None, "PULSE data note"),
        ],
        "more": [],
        "notes": ["Peer ranks use every STR weekly report whose week ends in the window.", "The latest compression quarter is partial."],
    }
    if ptab is not None and not ptab.empty:
        s_hotel["more"].append({"kind": "table", "title": "Coastal peer markets", "sub": f"{len(peers.get('weeks', []))} STR weekly reports",
                                "cols": ["Market", "Occupancy", "Average nightly rate", "Revenue per available room", "Change in revenue per room"], "num": [1, 2, 3, 4],
                                "hl": int(ptab.index[ptab["market"] == "Dana Point"][0]) if (ptab["market"] == "Dana Point").any() else None,
                                "rows": [[r.market, pct(r.occ), usd(r.adr), usd(r.revpar), f"{glyph(getattr(r, 'revpar_pct', None))} {spct(getattr(r, 'revpar_pct', None))}"] for r in ptab.itertuples()],
                                "src": SRC["str_weekly"]})
    if segt is not None and not segt.empty:
        s_hotel["more"].append({"kind": "chart", "chart": {"type": "hbar", "title": "Business mix by segment", "sub": "Share of occupied rooms, CoStar segmentation",
                                "rows": [{"l": str(r.segment), "v": float(r.demand / segt["demand"].sum() * 100), "fmt": "pct", "hi": False} for r in segt.itertuples()],
                                "src": SRC["costar_seg"]}})
    if m.multiyear is not None and not m.multiyear.empty:
        my = m.multiyear[m.multiyear["market"] == "Newport Beach/Dana Point"].copy()
        my["k"] = my["year_label"].str.extract(r"(\d{4})").astype(float)
        my = my.sort_values("k")
        s_hotel["more"].append({"kind": "chart", "chart": {"type": "line", "title": "Submarket revenue per room, 2016 to date", "sub": "Newport Beach/Dana Point, CoStar",
                                "x": list(my["year_label"]), "series": [{"name": "Revenue per room", "kind": "cur", "fmt": "usd", "values": [float(v) for v in my["revpar_usd"]]}],
                                "src": SRC["costar_rpt"]}})
    sections.append(s_hotel)

    # ---- 6. One more night ---------------------------------------------
    ov_trips = trips * overnight_pct / 100 if trips and overnight_pct else None
    one_pct_rn = ov_trips * 0.01 if ov_trips else None
    one_pct_val = one_pct_rn * h["adr"] if one_pct_rn and h.get("adr") else None
    s_night = {
        "id": "night", "tab": "One more night",
        "question": "What is one more night worth to Dana Point?",
        "headline": f"Each point of Sunday–Monday occupancy is worth about {usd_big(point_val)} a year",
        "dek": f"If 1% of overnight trips added one night, hotels would sell about {num(one_pct_rn)} more room nights.",
        "byline_src": f"{hotel_src_short}; Datafy, {dfy_period}; PULSE calculation",
        "lede": (f"{dateline} One more point of Sunday and Monday occupancy at Visit Dana Point's select hotels is worth about "
                 f"{usd_big(point_val)} in room revenue a year, at this year's {usd(sm_adr)} Sunday–Monday rate. If 1% of the "
                 f"{num(ov_trips)} overnight trips Datafy measured added one night, hotels would sell about {num(one_pct_rn)} more room nights, "
                 f"or {usd_big(one_pct_val)} at the {usd(h.get('adr'))} average rate. Both are scenarios built from measured inputs."),
        "lede_src": f"Source: PULSE calculation from {hotel_src_short} and Datafy ({dfy_period})",
        "key": {"v": usd_big(point_val), "l": "one Sunday–Monday occupancy point, a year (scenario)"},
        "why": (f"Longer stays and fuller early-week nights add revenue without adding rooms. Visitors stay {los:.1f} days on average, "
                f"and Sunday is the softest night of the week." if los else "Longer stays add revenue without adding rooms."),
        "glance": [{"type": "funnel", "title": "From trips to added room nights (scenario)", "sub": f"Datafy trips, {dfy_period}",
                    "stages": [{"l": "Visitor trips", "v": trips or 0, "fmt": "num"},
                               {"l": f"Overnight trips ({pct(overnight_pct)})", "v": ov_trips or 0, "fmt": "num"},
                               {"l": "1% add one night", "v": one_pct_rn or 0, "fmt": "num"}],
                    "src": f"Datafy, {dfy_period}; PULSE calculation (assumes one room per trip)"}],
        "numbers": [
            _stat("Scenario", usd_big(point_val), "Value of one Sunday–Monday point", f"{num(point_rn)} room nights a year", "PULSE calculation"),
            _stat("Scenario", usd_big(one_pct_val), "1% of overnight trips stay one more night", f"{num(one_pct_rn)} room nights", "PULSE calculation"),
            _stat(dfy_period, f"{los:.1f} days" if los else "n/a", "Average trip length", f"Overnight share {pct(overnight_pct)}", dfy_src),
            _stat(hspan, pct(sm_occ), "Sunday–Monday occupancy", f"Rate {usd(sm_adr)}", hotel_src_short),
        ],
        "calc": [
            ["Average rooms per night", "Rooms available ÷ nights", f"{num(h.get('supply'))} ÷ {h.get('n', 'n/a')} = {num(avg_rooms_day)}", hotel_src_short],
            ["One Sunday–Monday point", "Rooms × 1% × 104 Sun and Mon nights", f"{num(avg_rooms_day)} × 1% × 104 = {num(point_rn)} room nights", "PULSE calculation"],
            ["Value of that point", "Room nights × Sunday–Monday rate", f"{num(point_rn)} × {usd(sm_adr)} = {usd(point_val)}", "PULSE calculation"],
            ["Overnight trips", "Trips × overnight share", f"{num(trips)} × {pct(overnight_pct)} = {num(ov_trips)}", dfy_src],
            ["1% stay one more night", "Overnight trips × 1% × average rate", f"{num(ov_trips)} × 1% × {usd(h.get('adr'))} = {usd(one_pct_val)}", "PULSE calculation"],
        ],
        "details": {
            "who": _det(num_big(ov_trips), "overnight trips", "Visitors who already stay at least one night.", dfy_src),
            "what": _det(usd_big(point_val), "one Sunday–Monday point a year", "Room revenue only; dining and retail spending would add to it.", "PULSE calculation"),
            "when": _det("Sun–Mon", "softest nights", f"Sunday {pct(wk.get('worst_occ'))}; best night {wk.get('best', 'Sat')} {pct(wk.get('best_occ'))}.", hotel_src_short),
            "where": _det(f"{num(avg_rooms_day)} rooms", "average rooms a night in the comp set", "Visit Dana Point select hotels.", hotel_src_short),
            "why": _det(f"{los:.1f} days" if los else "n/a", "average trip length", "A longer stay fills early-week nights without new supply.", dfy_src),
            "how": _det("Scenario", "holds rate constant", "Uses measured rooms, rates and trips; assumes one room per overnight trip.", "PULSE calculation"),
        },
        "charts": [
            {"type": "hbar", "title": "Most trips last one day", "sub": f"Share of trips by length of stay",
             "rows": [{"l": r.length_of_stay, "v": float(r.pct_of_trips), "fmt": "pct", "hi": False}
                      for r in losd[~losd["length_of_stay"].str.contains("Trips")].itertuples()] if not losd.empty else [],
             "src": SRC["datafy"]},
        ],
        "table": {"title": "Occupancy and rate by night of week", "sub": hspan,
                  "cols": ["Night", "Occupancy", "Average nightly rate", "Revenue per available room"], "num": [1, 2, 3],
                  "rows": [[r.label, pct(r.occ), usd(r.adr), usd(r.revpar)] for r in dow.itertuples()], "src": hotel_src_short},
        "means": {
            "enh": [{"tag": "Events", "text": "Sunday and Monday programming fills the softest nights.", "src": hotel_src_short},
                    {"tag": "Hotels", "text": f"Weekend rates run {spct(wk.get('adr_premium_pct'))} above weeknights, so weeknight demand has rate room.", "src": hotel_src_short}],
            "needs": [{"tag": "Packages", "text": "Stay-longer offers tied to the top feeder markets.", "src": dfy_src},
                      {"tag": "Measurement", "text": "Hotel length-of-stay data from partner hotels to replace the trip-based scenario.", "src": "PULSE data note"}]},
        "cc": [
            _cc("Gain", f"Weekend rates run {spct(wk.get('adr_premium_pct'))} above weeknights.", None, hotel_src_short),
            _cc("Gap", f"Each Sunday and Monday point of occupancy is worth about {usd_big(point_val)} a year.", "Scenario at the current rate, not a forecast.", "PULSE calculation"),
            _cc("Gap", f"Most trips are day trips ({pct(day_pct)}).", None, dfy_src),
            _cc("Data limit", "The one-more-night figure is a scenario, not a forecast.", "Assumptions are stated in the notes.", "PULSE calculation"),
            _cc("Explanation", "Datafy trip length counts visitor days per trip; hotel length of stay is not in this data.", None, SRC["datafy"]),
        ],
        "advice": [
            _adv("Growth", f"Weekend rate premium of {spct(wk.get('adr_premium_pct'))} shows pricing power.", None, hotel_src_short),
            _adv("Opportunity", f"Each Sunday–Monday point is worth about {usd_big(point_val)} a year.", None, "PULSE calculation"),
            _adv("Quick win", "Promote Sunday-night stays to drive-market visitors.", None, dfy_src),
            _adv("Early hurdle", "Most trips are day trips.", "Pair day-trip markets with an overnight reason to stay.", dfy_src),
            _adv("Pitfall", "Treating the scenario as a forecast.", "Label it a scenario and state the assumption.", "PULSE calculation"),
            _adv("Explanation", "Datafy trip length counts visitor days per trip; hotel length of stay is not in this data.", None, SRC["datafy"]),
        ],
        "more": [], "notes": ["Scenarios hold rate constant and assume one room per overnight trip.", "104 = 52 Sundays + 52 Mondays a year."],
    }
    sections.append(s_night)

    # ---- 7. Getting here ------------------------------------------------
    sna_last = sna.iloc[-1] if not sna.empty else None
    sna_m = ap_month(pd.Timestamp(int(sna_last.year), int(sna_last.month), 1)) if sna_last is not None else "n/a"
    sna_ytd = sna[sna["year"] == sna["year"].max()] if not sna.empty else pd.DataFrame()
    sna_ytd_sum = float(sna_ytd["total_pax"].sum()) if not sna_ytd.empty else None
    board = [{"code": r.airport_code, "name": r.airport_name, "v": float(r.passengers_share_pct)} for r in airports.head(8).itertuples()]
    s_here = {
        "id": "here", "tab": "Getting here",
        "question": "How do visitors reach Dana Point?",
        "headline": (f"{'Nearly half of trips' if ca_share is not None and 40 <= ca_share < 50 else (pct(ca_share) + ' of trips') if ca_share is not None else 'Trips'} start in California as John Wayne Airport traffic "
                     f"{'grows' if sna_last is not None and _ok(sna_last.total_yoy_pct) and sna_last.total_yoy_pct > 0 else 'shifts'}"),
        "dek": f"John Wayne Airport handled {num(sna_last.total_pax) if sna_last is not None else 'n/a'} passengers in {sna_m}, {spct(sna_last.total_yoy_pct) if sna_last is not None else 'n/a'} vs. last year.",
        "byline_src": f"Datafy, {dfy_period}; Visit California",
        "lede": (f"{dateline} Nearly half of trips to Dana Point ({pct(ca_share)}) start in California, according to Datafy "
                 f"({dfy_period}). For those who fly, John Wayne Airport handled {num(sna_last.total_pax) if sna_last is not None else 'n/a'} passengers in {sna_m}, "
                 f"{spct(sna_last.total_yoy_pct) if sna_last is not None else 'n/a'} vs. a year earlier (Visit California)."),
        "lede_src": f"Source: {dfy_src}; {SRC['vca_air']}",
        "key": {"v": pct(ca_share), "l": "of trips start in California"},
        "why": "Drive access and nearby airports decide how far Dana Point's marketing can reach and how easily visitors can stay longer.",
        "glance": [{"type": "board", "title": "Where flying visitors start", "sub": f"Top origin airports by share of passengers, {dfy_period}",
                    "rows": board, "src": dfy_src}],
        "numbers": [
            _stat(dfy_period, pct(ca_share), "Trips from California", "Drive market", dfy_src),
            _stat(f"John Wayne Airport, {sna_m}", num_big(sna_last.total_pax) if sna_last is not None else "n/a", "Monthly passengers",
                  f"{glyph(sna_last.total_yoy_pct if sna_last is not None else None)} {spct(sna_last.total_yoy_pct) if sna_last is not None else 'n/a'} vs. last year", SRC["vca_air"]),
            _stat(f"John Wayne Airport, {int(sna['year'].max()) if not sna.empty else ''} to date", num_big(sna_ytd_sum), "Passengers year to date", f"{len(sna_ytd)} months", SRC["vca_air"]),
            _stat(dfy_period, board[0]["code"] if board else "n/a", "Top origin airport", f"{pct(board[0]['v'])} of flying visitors" if board else "", dfy_src),
        ],
        "calc": [["Out-of-state trips", "100% − California share", f"100% − {pct(ca_share)} = {pct(100 - ca_share) if ca_share is not None else 'n/a'}", dfy_src],
                 ["John Wayne passengers, year to date", "Sum of monthly passengers", f"{len(sna_ytd)} months = {num(sna_ytd_sum)}", SRC["vca_air"]]],
        "details": {
            "who": _det(pct(ca_share), "of trips from California", "Drive visitors from Southern California lead.", dfy_src),
            "what": _det(num_big(sna_last.total_pax) if sna_last is not None else "n/a", f"John Wayne passengers, {sna_m}", "Domestic travel carries the airport.", SRC["vca_air"]),
            "when": _det(sna_m, "latest airport month", f"Datafy origins cover {dfy_period}.", SRC["vca_air"]),
            "where": _det(board[0]["code"] if board else "n/a", "top origin airport for flying visitors", ", ".join(b["code"] for b in board[1:5]) + " follow.", dfy_src),
            "why": _det(f"{los:.1f} days" if los else "n/a", "average trip", "Easy access supports short, repeat trips.", dfy_src),
            "how": _det("Location + airport counts", "methods", "Datafy origin airports from mobile data; passenger counts from the airport via Visit California.", SRC["vca_air"]),
        },
        "charts": [
            {"type": "line", "title": "John Wayne Airport monthly passengers", "sub": "Total passengers by month",
             "x": [f"{_AP_MONTHS[int(r.month) - 1][:3]} {str(int(r.year))[2:]}" for r in sna.itertuples()],
             "series": [{"name": "Passengers", "kind": "cur", "fmt": "num", "values": [float(r.total_pax) for r in sna.itertuples()]}],
             "src": SRC["vca_air"]} if not sna.empty else None,
        ],
        "table": {"title": "Top origin airports for flying visitors", "sub": dfy_period,
                  "cols": ["Code", "Airport", "Share of passengers"], "num": [2],
                  "rows": [[r.airport_code, r.airport_name, pct(r.passengers_share_pct)] for r in airports.head(15).itertuples()], "src": dfy_src},
        "means": {"enh": [{"tag": "Access", "text": f"John Wayne passengers are {spct(sna_last.total_yoy_pct) if sna_last is not None else 'n/a'} vs. last year.", "src": SRC["vca_air"]},
                          {"tag": "Marketing", "text": "The leading origin airports point to fly-in markets for paid media.", "src": dfy_src}],
                  "needs": [{"tag": "Transit", "text": "Local transit and parking data are not in PULSE yet.", "src": "PULSE data note"},
                            {"tag": "Measurement", "text": "More recent airport months as Visit California publishes them.", "src": SRC["vca_air"]}]},
        "cc": [
            _cc("Gain", f"John Wayne passengers were {spct(sna_last.total_yoy_pct) if sna_last is not None else 'n/a'} vs. last year in {sna_m}.", None, SRC["vca_air"]),
            _cc("Explanation", "Datafy's market table reports spend per visitor and trip length for each market, including fly-in markets.", "See the Who visits market table.", f"Datafy, {dma_period or 'n/a'}"),
            _cc("Data limit", f"Airport data lags: the latest month is {sna_m}.", "Updates with each Visit California release.", SRC["vca_air"]),
            _cc("Data limit", "Origin-airport shares are not Dana Point airport arrivals.", "They show where flying visitors started their trip.", SRC["datafy"]),
            _cc("Explanation", "International passengers at John Wayne are a small share of the total.", None, SRC["vca_air"]),
        ],
        "advice": [
            _adv("Growth", f"John Wayne domestic passengers continue to grow ({spct(sna_last.total_yoy_pct) if sna_last is not None else 'n/a'} total in {sna_m}).", None, SRC["vca_air"]),
            _adv("Opportunity", "Fly-in visitors stay longer and spend more per trip.", "Weight fly-market media toward multi-night offers.", f"Datafy, {dma_period or 'n/a'}"),
            _adv("Quick win", "Add John Wayne arrival tips to visitor content.", None, SRC["vca_air"]),
            _adv("Early hurdle", f"Airport data lag: the latest month is {sna_m}.", "Refresh with each Visit California release.", SRC["vca_air"]),
            _adv("Pitfall", "Reading origin-airport shares as Dana Point airport arrivals.", "They show where flying visitors started their trip.", SRC["datafy"]),
            _adv("Explanation", "International passengers at John Wayne are a small share of the total.", None, SRC["vca_air"]),
        ],
        "more": [], "notes": ["Airport counts are context (Layer 2), not Dana Point visitors."],
    }
    s_here["charts"] = [c for c in s_here["charts"] if c]
    sections.append(s_here)

    # ---- 8. Marketing reach --------------------------------------------
    w = web.iloc[0] if not web.empty else None
    cp = camp.iloc[0] if not camp.empty else None
    funnel = []
    if cp is not None:
        funnel = [{"l": "Impressions", "v": float(cp.total_impressions), "fmt": "num"},
                  {"l": "Unique reach", "v": float(cp.unique_reach), "fmt": "num"},
                  {"l": "Attributed trips", "v": float(cp.attributable_trips), "fmt": "num"}]
    snap = ap_date(ads["snapshot_date"]) if ads.get("snapshot_date") else "n/a"
    s_mkt = {
        "id": "mkt", "tab": "Marketing reach",
        "question": "Does Visit Dana Point's marketing bring visitors?",
        "headline": f"The annual campaign reached {num_big(cp.unique_reach) if cp is not None else 'n/a'} people and drove {num(cp.attributable_trips) if cp is not None else 'n/a'} attributed trips",
        "dek": f"The current paid-media flight returned about {usd(ads.get('est_roas'), 2) if ads.get('est_roas') else 'n/a'} in visitor impact per $1 spent.",
        "byline_src": "Datafy attribution reports; Later",
        "lede": (f"{dateline} Visit Dana Point's 2025-26 annual campaign delivered {num_big(cp.total_impressions) if cp is not None else 'n/a'} impressions to "
                 f"{num_big(cp.unique_reach) if cp is not None else 'n/a'} people and {num(cp.attributable_trips) if cp is not None else 'n/a'} attributed trips, "
                 f"with an estimated {usd_big(cp.est_campaign_impact_usd) if cp is not None else 'n/a'} in visitor impact (Datafy, "
                 f"{span(cp.report_period_start, cp.report_period_end) if cp is not None else 'n/a'}). The current paid-media flight logged "
                 f"{num_big(ads.get('total_impressions'))} impressions on {usd(ads.get('total_spend_usd'))} through {snap}."),
        "lede_src": f"Source: {SRC['datafy_camp']}; {SRC['datafy_ads']}",
        "key": {"v": num(cp.attributable_trips) if cp is not None else "n/a", "l": "trips attributed to the annual campaign"},
        "why": "Attribution links media exposure to observed visits, so the team can see which markets and tactics bring travelers.",
        "glance": [{"type": "funnel", "title": "From impressions to visits", "sub": f"2025-26 annual campaign, {span(cp.report_period_start, cp.report_period_end) if cp is not None else ''}",
                    "stages": funnel, "src": SRC["datafy_camp"]}] if funnel else [],
        "numbers": [
            _stat("Annual campaign", num_big(cp.unique_reach) if cp is not None else "n/a", "Unique reach", f"{num_big(cp.total_impressions) if cp is not None else 'n/a'} impressions", SRC["datafy_camp"]),
            _stat("Annual campaign", usd_big(cp.est_campaign_impact_usd) if cp is not None else "n/a", "Estimated visitor impact", f"{num(cp.attributable_trips) if cp is not None else 'n/a'} trips", SRC["datafy_camp"]),
            _stat(f"Paid media, {snap}", f"{usd(ads.get('est_roas'), 2)}" if ads.get("est_roas") else "n/a", "Visitor impact per $1", f"{usd_big(ads.get('est_campaign_impact_usd'))} on {usd(ads.get('total_spend_usd'))}", SRC["datafy_ads"]),
            _stat("Website", num(w.attributable_trips) if w is not None else "n/a", "Trips attributed to the website", f"{num(w.total_website_sessions) if w is not None else 'n/a'} sessions", SRC["datafy_web"]),
        ],
        "calc": [
            ["Paid-media return", "Reported by Datafy: estimated visitor impact per $1 of media",
             f"{usd(ads.get('est_roas'), 2) if ads.get('est_roas') else 'n/a'} per $1 (impact {usd(ads.get('est_campaign_impact_usd'))})", SRC["datafy_ads"]],
            ["Click rate", "Clicks ÷ impressions", f"{num(ads.get('total_clicks'))} ÷ {num(ads.get('total_impressions'))} = {pct(ads.get('ctr_pct'), 2)}", SRC["datafy_ads"]],
        ],
        "details": {
            "who": _det(num_big(cp.unique_reach) if cp is not None else "n/a", "people reached", "Audiences in Datafy's targeted markets.", SRC["datafy_camp"]),
            "what": _det(num(cp.attributable_trips) if cp is not None else "n/a", "attributed trips", "Visits observed after ad exposure.", SRC["datafy_camp"]),
            "when": _det(span(cp.report_period_start, cp.report_period_end) if cp is not None else "n/a", "campaign period", f"Paid-media flight through {snap}.", SRC["datafy_camp"]),
            "where": _det(ps.clean_market(ads["markets"].iloc[0]["dma"]) if ads.get("markets") is not None and not ads["markets"].empty else "n/a",
                          "top market for campaign trips", "Campaign trips lean on the drive market.", SRC["datafy_ads"]),
            "why": _det(usd_big(cp.est_campaign_impact_usd) if cp is not None else "n/a", "estimated visitor impact", "Impact uses Datafy's cohort spend per visitor.", SRC["datafy_camp"]),
            "how": _det("Attribution", "Datafy method", "Exposed devices matched to later visits within the visitation window.", SRC["datafy"]),
        },
        "charts": [],
        "table": {"title": "Paid media vs. visitor spending, by market", "sub": f"Share of campaign trips vs. share of all visitor spending",
                  "cols": ["Market", "Share of campaign trips", "Share of visitor spending"], "num": [1, 2],
                  "rows": [[ps.clean_market(r.dma), pct(r.trip_share_pct), pct(r.share_pct)] for r in m.ad_mix.itertuples()] if m.ad_mix is not None and not m.ad_mix.empty else [],
                  "src": f"{SRC['datafy_ads']}; {dfy_src}"},
        "means": {"enh": [{"tag": "Media", "text": f"Paid media returns about {usd(ads.get('est_roas'), 2) if ads.get('est_roas') else 'n/a'} per $1 in visitor impact (Datafy estimate).", "src": SRC["datafy_ads"]},
                          {"tag": "Social", "text": f"Facebook followers reached {num(social['Facebook']['v'])} by {ap_date(social['Facebook']['last'])}." if "Facebook" in social else "Social audience data is loaded from Later.", "src": SRC["later"]}],
                  "needs": [{"tag": "Media", "text": "Campaign trips over-index on Los Angeles relative to its spending share.", "src": SRC["datafy_ads"]},
                            {"tag": "Measurement", "text": "Media cost for the annual campaign, to compute its return.", "src": SRC["datafy_camp"]}]},
        "cc": [
            _cc("Gain", f"Instagram followers grew from {num(social['Instagram']['v0'])} to {num(social['Instagram']['v'])}." if "Instagram" in social else "Social audience data is loaded from Later.", None, SRC["later"]),
            _cc("Gap", "Campaign-trip share and visitor-spending share differ by market.", "See the paid media vs. spending table.", SRC["datafy_ads"]),
            _cc("Data limit", "The annual campaign report has no media cost recorded.", "Its return is not computed.", SRC["datafy_camp"]),
            _cc("Data limit", "Campaign, paid-media and website trips overlap.", "Each is reported on its own, never added.", SRC["datafy"]),
            _cc("Explanation", "Attribution is an estimate based on matched devices, not a census of visitors.", None, SRC["datafy"]),
        ],
        "advice": [
            _adv("Growth", f"Instagram followers grew from {num(social['Instagram']['v0'])} to {num(social['Instagram']['v'])}." if "Instagram" in social else "Social audiences are growing.", None, SRC["later"]),
            _adv("Opportunity", "Fly-in markets spend more per visitor but take a smaller share of campaign trips.", "Test fly-market flights with multi-night offers.", SRC["datafy_ads"]),
            _adv("Quick win", "Report campaign trips by market monthly.", None, SRC["datafy_ads"]),
            _adv("Early hurdle", "The annual campaign report has no media cost recorded.", "Add spend to compute a return.", SRC["datafy_camp"]),
            _adv("Pitfall", "Adding campaign, paid-media and website trips together.", "They overlap; report each on its own.", SRC["datafy"]),
            _adv("Explanation", "Attribution is an estimate based on matched devices, not a census of visitors.", None, SRC["datafy"]),
        ],
        "more": [], "notes": ["Campaign, paid-media and website attribution use separate Datafy reports and windows."],
    }
    if social:
        s_mkt["charts"].append({"type": "hbar", "title": "Social audience by platform", "sub": "Followers at the latest Later export",
                                "rows": [{"l": k, "v": v["v"], "fmt": "num", "hi": False} for k, v in social.items()], "src": SRC["later"]})
    tac = ads.get("tactics")
    if tac is not None and not tac.empty:
        s_mkt["charts"].append({"type": "hbar", "title": "Attribution rate by tactic", "sub": f"Paid media through {snap}",
                                "rows": [{"l": r.tactic, "v": float(r.attribution_rate_pct), "fmt": "pct2", "hi": i == 0} for i, r in enumerate(tac.itertuples())],
                                "src": SRC["datafy_ads"]})
    if w is not None:
        s_mkt["more"].append({"kind": "table", "title": "Website attribution", "sub": span(w.report_period_start, w.report_period_end),
                              "cols": ["Measure", "Value"], "num": [1],
                              "rows": [["Website sessions", num(w.total_website_sessions)], ["Pageviews", num(w.website_pageviews)],
                                       ["Engagement rate", pct(w.avg_engagement_rate_pct)], ["Attributed trips", num(w.attributable_trips)],
                                       ["Estimated visitor impact", usd(w.est_impact_usd)]],
                              "src": SRC["datafy_web"]})
    sections.append(s_mkt)

    # ---- 9. Market outlook ---------------------------------------------
    ev_rows = [[r.event_name, ap_date(r.date), f"{int(r.days_out)} days", pct(r.ly_occ), usd(r.ly_adr)] for r in ev.itertuples()] if not ev.empty else []
    fc = vcfc[vcfc["year"] >= year - 1] if not vcfc.empty else pd.DataFrame()
    fc_row = vcfc[vcfc["year"] == year] if not vcfc.empty else pd.DataFrame()
    s_out = {
        "id": "outlook", "tab": "Market outlook",
        "question": "What is coming that will affect Dana Point?",
        "headline": f"Latest four weeks run {spct(mo.get('revpar_pct'))} on revenue per room{' heading into the holidays' if pd.Timestamp(hend).month in (9, 10, 11) else ''}",
        "dek": f"{ev_lead.iloc[0]['event_name']} is in {int(ev_lead.iloc[0]['days_out'])} days; Visit California forecasts {spct(fc_row.iloc[0]['total_yoy_pct']) if not fc_row.empty else 'n/a'} statewide visits this year." if not ev_lead.empty else "Momentum, events and forecasts.",
        "byline_src": f"{hotel_src_short}; events calendar; Visit California",
        "lede": (f"{dateline} The latest four weeks of hotel data, through {ap_date(mo['end']) if mo.get('end') is not None else 'n/a'}, ran "
                 f"{pct(mo.get('occ'))} occupancy at {usd(mo.get('adr'))}, {spct(mo.get('revpar_pct'))} on revenue per room vs. the same weeks last year. "
                 + (f"{ev_lead.iloc[0]['event_name']} is {int(ev_lead.iloc[0]['days_out'])} days out; the same dates last year ran {pct(ev_lead.iloc[0]['ly_occ'])} full. " if not ev_lead.empty else "")
                 + (f"Visit California forecasts {fc_row.iloc[0]['total_visits_m']:.1f} million statewide visits in {year} ({spct(fc_row.iloc[0]['total_yoy_pct'])})." if not fc_row.empty else "")),
        "lede_src": f"Source: {hotel_src_short}; {SRC['events']}; {SRC['vca_fc']}",
        "key": {"v": spct(mo.get("revpar_pct")), "l": "revenue per room, latest four weeks vs. last year"},
        "why": "Recent momentum, the events calendar and statewide forecasts show where demand is heading before monthly reports catch up.",
        "glance": [{"type": "versus", "title": "Latest four weeks vs. same weeks last year", "sub": "Revenue per available room",
                    "a": {"l": "Latest four weeks", "v": mo.get("revpar") or 0, "fmt": "usd"},
                    "b": {"l": "Same weeks last year", "v": (mo.get("prior") or {}).get("revpar") or 0, "fmt": "usd"},
                    "src": hotel_src_short}],
        "numbers": [
            _stat("Latest four weeks", usd(mo.get("revpar")), "Revenue per available room", f"{glyph(mo.get('revpar_pct'))} {spct(mo.get('revpar_pct'))} vs. last year", hotel_src_short),
            _stat("Latest four weeks", pct(mo.get("occ")), "Occupancy", f"{glyph(mo.get('occ_pts'))} {pts(mo.get('occ_pts'))}", hotel_src_short),
            _stat("Next event", ev_lead.iloc[0]["event_name"] if not ev_lead.empty else "n/a", f"{ap_date(ev_lead.iloc[0]['date'])}" if not ev_lead.empty else "", f"Last year {pct(ev_lead.iloc[0]['ly_occ'])} occupancy" if not ev_lead.empty else "", SRC["events"]),
            _stat(f"California, {year}", f"{fc_row.iloc[0]['total_visits_m']:.1f}M" if not fc_row.empty else "n/a", "Statewide visits (forecast)", spct(fc_row.iloc[0]['total_yoy_pct']) if not fc_row.empty else "", SRC["vca_fc"]),
        ],
        "calc": [["Change in revenue per room", "(Latest four weeks ÷ same weeks last year) − 1",
                  f"({usd(mo.get('revpar'), 2)} ÷ {usd((mo.get('prior') or {}).get('revpar'), 2)}) − 1 = {spct(mo.get('revpar_pct'))}", hotel_src_short]],
        "details": {
            "who": _det(f"{len(ev)} events", "major events in the next 120 days", ", ".join(ev["event_name"].tolist()) if not ev.empty else "None on the calendar.", SRC["events"]),
            "what": _det(spct(mo.get("revpar_pct")), "revenue per room, latest four weeks", f"Rate {spct(mo.get('adr_pct'))}; occupancy {pts(mo.get('occ_pts'))}.", hotel_src_short),
            "when": _det(span(mo["start"], mo["end"]) if mo.get("start") is not None else "n/a", "momentum window", "Compared with the same weeks a year earlier.", hotel_src_short),
            "where": _det("California", "statewide forecast", "Visit California's travel forecast sets the statewide backdrop.", SRC["vca_fc"]),
            "why": _det(f"{int(comp.iloc[-2]['d80'])} nights" if len(comp) > 1 else "n/a", f"at 80% or higher in {comp.iloc[-2]['quarter'] if len(comp) > 1 else ''}", "Compression shows when rate has the most room.", hotel_src_short),
            "how": _det("Four weeks", "rolling window", "The newest 28 days of hotel data against the same days last year.", hotel_src_short),
        },
        "charts": [
            {"type": "line", "title": "California visits: actual and forecast", "sub": "Millions of visits; forecast years marked",
             "x": [str(int(r.year)) + ("F" if r.is_forecast else "") for r in vcfc[vcfc["year"] >= 2018].itertuples()],
             "series": [{"name": "Visits", "kind": "cur", "fmt": "m", "values": [float(r.total_visits_m) for r in vcfc[vcfc["year"] >= 2018].itertuples()]}],
             "src": SRC["vca_fc"]} if not vcfc.empty else None,
        ],
        "table": {"title": "Upcoming events and the same dates last year", "sub": "Next 120 days",
                  "cols": ["Event", "Date", "Days out", "Occupancy last year", "Rate last year"], "num": [2, 3, 4],
                  "rows": ev_rows, "src": f"{SRC['events']}; {hotel_src_short}"},
        "means": {"enh": [{"tag": "Events", "text": "Holiday events fall on dates that ran well under full last year.", "src": SRC["events"]},
                          {"tag": "Rate", "text": f"Recent rate is {spct(mo.get('adr_pct'))} vs. last year.", "src": hotel_src_short}],
                  "needs": [{"tag": "Forecast", "text": "A Dana Point hotel forecast; the forecast here is statewide.", "src": SRC["vca_fc"]},
                            {"tag": "Events", "text": "Attendance and ticket-buyer origins for major events.", "src": "PULSE data note"}]},
        "cc": [
            _cc("Gain", f"Latest four weeks: {spct(mo.get('revpar_pct'))} on revenue per room.", None, hotel_src_short),
            _cc("Gap", "Upcoming event dates ran under full on the same dates last year.", "See the events table for each date.", SRC["events"]),
            _cc("Gap", "The fourth quarter has the fewest compression nights.", None, hotel_src_short),
            _cc("Data limit", "A four-week window is short.", "The monthly table shows the longer trend.", hotel_src_short),
            _cc("Explanation", "Visit California figures are statewide and include forecast years.", None, SRC["vca_fc"]),
        ],
        "advice": [
            _adv("Growth", f"Latest four weeks: {spct(mo.get('revpar_pct'))} on revenue per room.", None, hotel_src_short),
            _adv("Opportunity", "Holiday event dates had open rooms last year.", "Package events with lodging.", SRC["events"]),
            _adv("Quick win", "Promote upcoming events in the top feeder markets.", None, dfy_src),
            _adv("Early hurdle", "The fourth quarter has the fewest compression nights.", "Lean on events to lift the shoulder season.", hotel_src_short),
            _adv("Pitfall", "Reading a four-week window as a trend.", "Confirm with the monthly table before acting.", hotel_src_short),
            _adv("Explanation", "Visit California figures are statewide and include forecast years.", None, SRC["vca_fc"]),
        ],
        "more": [], "notes": ["Events come from the Visit Dana Point events calendar.", "Forecast years are labeled F."],
    }
    if m.insight:
        s_out["notes"].append(f"PULSE insights engine, {m.insight['as_of_date']}: {m.insight['headline']}")
    s_out["charts"] = [c for c in s_out["charts"] if c]
    sections.append(s_out)

    # ---- link "up next" ------------------------------------------------
    bridges = {
        "today": "Room revenue starts with visitors. The next section shows who they are and where they come from.",
        "who": "Knowing who visits, the next question is how much they spend and where it goes.",
        "spend": "Visitor spending returns to the city through taxes and jobs, measured next.",
        "city": "Most of that revenue starts in hotel rooms. The next section looks at the hotel market night by night.",
        "hotel": "Open rooms on Sunday and Monday point to the most direct lever: longer stays.",
        "night": "Longer stays depend on access. The next section shows how visitors reach Dana Point.",
        "here": "Access brings visitors; marketing decides whether they choose Dana Point.",
        "mkt": "With performance, visitors and marketing measured, the outlook shows what comes next.",
        "outlook": "The appendix defines every term, method and formula in this report.",
    }
    # ---- added visuals (2026-10-14): D3 calendar, GPT-Vis waterfall ----
    _sec = {x["id"]: x for x in sections}
    try:
        _dd = pdx.hotel_daily(conn)
        _dd = _dd[(_dd["date"] >= pd.Timestamp(hstart)) & (_dd["date"] <= pd.Timestamp(hend))]
        if not _dd.empty and "hotel" in _sec:
            _sec["hotel"]["calendar"] = {
                "title": "Every night of the year, by occupancy" if _ytd else "Every night in the period, by occupancy",
                "sub": f"Daily hotel occupancy, {hspan}. Darker is fuller; hover a day for its figures.",
                "days": [[r.date.strftime("%Y-%m-%d"), round(float(r.occ_pct), 1), round(float(r.adr), 0) if pd.notna(r.adr) else None]
                         for r in _dd.itertuples() if pd.notna(r.occ_pct)],
                "src": hotel_src_short,
            }
    except Exception as _exc:  # noqa: BLE001
        print(f"[insights] calendar skipped: {_exc}")
    try:
        _d, _dp = h.get("demand"), hp.get("demand")
        _a, _ap = h.get("adr"), hp.get("adr")
        if all(v is not None for v in (_d, _dp, _a, _ap)) and "today" in _sec:
            _vol = (_d - _dp) * _ap
            _rate = (_a - _ap) * _d
            _sec["today"]["waterfall"] = {
                "title": "What moved room revenue",
                "sub": f"{hspan} vs. the same dates last year",
                "data": [{"category": "Last year", "value": round(float(hp.get("revenue") or _dp * _ap))},
                         {"category": "More rooms sold", "value": round(float(_vol))},
                         {"category": "Higher nightly rate", "value": round(float(_rate))},
                         {"category": "This year", "value": round(float(h.get("revenue") or _d * _a)), "isTotal": True}],
                "calc": (f"Rooms sold effect: ({num(_d)} - {num(_dp)}) x {usd(_ap)} = {usd_big(_vol)}; "
                         f"rate effect: ({usd(_a)} - {usd(_ap)}) x {num(_d)} = {usd_big(_rate)}"),
                "src": "PULSE calculation on " + hotel_src_short,
            }
    except Exception as _exc:  # noqa: BLE001
        print(f"[insights] waterfall skipped: {_exc}")

    for i, s in enumerate(sections):
        s["n"] = i + 1
        s["bridge"] = bridges.get(s["id"], "")
        # The original compare-and-contrast table, kept in full in its own labeled block.
        s["advice"] = [a for a in s.get("advice", []) if a]

    # ==================================================================
    # Overview: summary, takeaways, Q&A
    # ==================================================================
    takeaways = [{"id": s["id"], "tab": s["tab"], "v": s["key"]["v"], "l": s["key"]["l"], "head": s["headline"], "src": s["byline_src"]} for s in sections]
    qa = [{"id": s["id"], "q": s["question"], "a": s["lede"].replace(dateline + " ", ""), "src": s["lede_src"].replace("Source: ", ""), "v": s["key"]["v"], "l": s["key"]["l"]}
          for s in sections]
    growth = []
    for lbl, v in (("Revenue per room, latest four weeks", mo.get("revpar_pct")), (f"Revenue per room, {P_TD}", h.get("revpar_pct")),
                   (f"Room revenue, {P_TD}", h.get("revenue_pct")), (f"Average nightly rate, {P_TD}", h.get("adr_pct")),
                   ("Visitor spending", dfy.get("spend_pct")), ("Visitor days", dfy.get("vd_pct"))):
        if _ok(v):
            growth.append({"l": lbl, "v": float(v)})
    growth.sort(key=lambda r: -r["v"])
    summary = {
        "highlight": "Hotel market",
        "headline": (f"Dana Point hotels lift revenue per room {spct(h.get('revpar_pct'))} as {LEADER} leads {P_THE}" if not _ok(h.get('revpar_pct')) or h.get('revpar_pct') >= 0
                     else f"Dana Point hotels see revenue per room move {spct(h.get('revpar_pct'))} for {P_THE}"),
        "sub": (f"Room revenue reached {usd_big(rev)} ({spct(h.get('revenue_pct'))}), visitors spent {usd_big(dfy_spend)} and " +
                (f"Dana Point leads its coastal peers on rate." if peers.get("rank_adr") == 1
                 else f"Dana Point ranks {ps._ordinal(peers['rank_adr']) if peers.get('rank_adr') else 'n/a'} of {peers.get('n_markets', 'n/a')} coastal peers on rate.")),
        "paras": [
            (f"{dateline} Visit Dana Point's select hotels earned {usd(h.get('revpar'))} per available room from {hspan}, "
             f"{spct(h.get('revpar_pct'))} from a year earlier, as the average nightly rate rose {spct(h.get('adr_pct'))} to {usd(h.get('adr'))} "
             f"and occupancy moved {pts(h.get('occ_pts'))} to {pct(h.get('occ'))}, according to STR and CoStar data."),
            (f"Room revenue totaled {usd_big(rev)}, equal to about {usd_big(tot)} in transient occupancy tax at the city's 10% rate on these hotels alone. "
             f"Visitors spent {usd_big(dfy_spend)} in Dana Point from {dfy_mspan} ({spct(dfy.get('spend_pct'))}), with {top_mkt} the largest source "
             f"at {pct(top_mkt_share)} of spending (Datafy)."),
            ((f"Weeknights have the most room: hotels ran {pct(sm_occ)} full on Sunday and Monday nights vs. {pct(fs_occ)} on Friday and Saturday. " if SM_LOW
              else f"{LOW_NIGHTS} nights had the most room in this window; hotels ran {pct(sm_occ)} full on Sunday and Monday nights vs. {pct(fs_occ)} on Friday and Saturday. ") +
             f"Each point of Sunday–Monday occupancy is worth about {usd_big(point_val)} in room revenue a year "
             f"({num(avg_rooms_day)} rooms × 1% × 104 nights × {usd(sm_adr)})."),
            ((f"Dana Point ranks first of {peers.get('n_markets', 'n/a')} coastal peer markets on revenue per available room and average rate in STR's weekly reports. "
              if peers.get("rank_revpar") == 1 and peers.get("rank_adr") == 1 else
              f"Dana Point ranks {ps._ordinal(peers['rank_revpar']) if peers.get('rank_revpar') else 'n/a'} of {peers.get('n_markets', 'n/a')} coastal peer markets on revenue per available room and {ps._ordinal(peers['rank_adr']) if peers.get('rank_adr') else 'n/a'} on average rate in STR's weekly reports. ") +
             f"The latest four weeks ran {spct(mo.get('revpar_pct'))} on revenue per room vs. the same weeks last year."),
            "The full report covers Dana Point today, who visits, visitor spending, city revenue and jobs, the hotel market, one more night, "
            "getting here, marketing reach and the market outlook, with the source and date for every figure.",
        ],
        "stats": [
            {"v": usd_big(rev), "l": f"Hotel room revenue, {P_YTD} ({spct(h.get('revenue_pct'))})", "src": hotel_src_short},
            {"v": spct(h.get("revpar_pct")), "l": "Revenue per available room vs. last year", "src": hotel_src_short},
            {"v": usd_big(dfy_spend), "l": f"Visitor spending, {dfy_mspan}", "src": f"Datafy, {dfy_mspan}"},
            {"v": f"#{peers.get('rank_revpar', 'n/a')}", "l": f"Revenue per room among {peers.get('n_markets', 'n/a')} coastal peers", "src": SRC["str_weekly"]},
            {"v": pct(sm_occ), "l": "Sunday–Monday hotel occupancy", "src": hotel_src_short,
             "calc": f"(Sun + Mon rooms sold) ÷ (Sun + Mon rooms available) = {pct(sm_occ)}"},
        ],
        "growth": growth,
        "about": (f"About the data: {hotel_src}; {SRC['costar_rpt']} (report {bench.get('report_date', 'n/a')}); {SRC['str_weekly']}; "
                  f"{SRC['datafy']} ({dfy_period}; monthly trend {dfy_mspan}); {SRC['datafy_camp']}; {SRC['datafy_ads']} (through {snap}); "
                  f"{SRC['vca_air']}; {SRC['vca_fc']}; {SRC['bls']}. Prepared by GloCon Solutions for Visit Dana Point, {prep}."),
    }

    # ==================================================================
    # Appendix
    # ==================================================================
    glossary = [
        ("Occupancy", "Rooms sold ÷ rooms available, for the hotels and dates shown."),
        ("Average nightly rate (ADR)", "Room revenue ÷ rooms sold."),
        ("Revenue per available room (RevPAR)", "Room revenue ÷ rooms available; equals occupancy × average rate."),
        ("Room night", "One room sold for one night."),
        ("Comp set", "Visit Dana Point's select hotels, tracked by STR and CoStar."),
        ("Submarket", "CoStar's Newport Beach/Dana Point hotel market area."),
        ("Coastal peers", "Dana Point, Newport Beach, La Jolla, Santa Barbara, Huntington Beach and Monterey-Carmel in STR's weekly report."),
        ("Revenue index", "Dana Point revenue per room ÷ peer average × 100; 100 is par."),
        ("Compression night", "A night at 80% occupancy or higher; 90% or higher is high compression."),
        ("Transient", "Individual leisure and business guests, not part of a group block."),
        ("Group", "Rooms sold in blocks of 10 or more for meetings, weddings or events."),
        ("Visitor trip", "A visit observed by Datafy's location panel, from arrival to departure."),
        ("Visitor day", "One visitor present in Dana Point on one day."),
        ("Day trip", "A trip with no overnight stay."),
        ("Overnight trip", "A trip that includes at least one night."),
        ("Feeder market", "A Nielsen DMA area that sends visitors to Dana Point."),
        ("DMA", "Designated Market Area, a Nielsen media market."),
        ("Visitor spending", "Card transactions by non-residents in Dana Point, observed by Datafy."),
        ("Attribution", "Matching people exposed to media with later observed visits."),
        ("Impressions", "Times an ad was shown."),
        ("Unique reach", "Different people who saw an ad at least once."),
        ("ROAS", "Estimated visitor impact ÷ media spend."),
        ("TOT", "Transient occupancy tax, 10% of room revenue in Dana Point."),
        ("TBID", "Tourism Business Improvement District assessment: 1.0%, 1.5% or 2.0% by nightly rate; about 1.25% blended."),
        ("Year to date", "January 1 through the latest date in the source."),
        ("Scenario", "A what-if calculation from measured inputs with a stated assumption; not a forecast."),
    ]
    methods = [
        ("Hotel performance", "Rooms-weighted daily STR data for the select hotels, extended with CoStar's daily grid for the same hotels after STR's latest file."),
        ("Year over year", "Each window compared with the same dates 364 days earlier, so weekdays line up."),
        ("Peer comparison", "Every STR weekly report whose week ends inside the window, aggregated by market."),
        ("Benchmarks", "CoStar's year-to-date submarket, Orange County and U.S. reports, set against the select hotels for the same months."),
        ("Visitor spending", "Datafy card-transaction spending by non-residents, by month and category."),
        ("Feeder markets", "Datafy spending share by Nielsen DMA from the latest export period."),
        ("Attribution", "Datafy's matched-device method for campaign, paid-media and website reports, each with its own window."),
        ("Scenarios", "Measured rooms, rates and trips with one stated assumption; rate held constant."),
        ("Tax estimates", "Comp-set room revenue × 10% (TOT) and × 1.25% (blended TBID); not city-reported collections."),
    ]
    formulas = [
        ["Occupancy", "Rooms sold ÷ rooms available", f"{num(h.get('demand'))} ÷ {num(h.get('supply'))} = {pct(h.get('occ'))}"],
        ["Average nightly rate", "Room revenue ÷ rooms sold", f"{usd(rev)} ÷ {num(h.get('demand'))} = {usd(h.get('adr'))}"],
        ["Revenue per available room", "Occupancy × average rate", f"{pct(h.get('occ'))} × {usd(h.get('adr'))} = {usd(h.get('revpar'))}"],
        ["Transient occupancy tax", "Room revenue × 10%", f"{usd(rev)} × 10% = {usd(tot)}"],
        ["TBID (blended)", "Room revenue × 1.25%", f"{usd(rev)} × 1.25% = {usd(tbid)}"],
        ["One Sunday–Monday point", "Rooms × 1% × 104 nights × Sun–Mon rate", f"{num(avg_rooms_day)} × 1% × 104 × {usd(sm_adr)} = {usd(point_val)}"],
        ["Revenue index", "Dana Point revenue per room ÷ peer average × 100",
         f"{usd(pk.get('revpar'))} ÷ {usd((peers.get('peer_avg') or {}).get('revpar'))} × 100 = {peers['rgi']:.0f}" if peers.get("rgi") else "n/a"],
    ]
    appendix = {
        "id": "appx", "tab": "Appendix", "n": len(sections) + 1,
        "headline": "Terms, methods and formulas behind every number",
        "dek": "A plain-language guide to the glossary, methods and formulas used in this report.",
        "key": {"v": str(len(formulas)), "l": "formulas worked through with this report's inputs"},
        "counts": {"terms": len(glossary), "methods": len(methods), "formulas": len(formulas)},
        "glossary": glossary, "methods": methods, "formulas": formulas, "photos": _photo_credits(), "pipeline": PIPELINE,
        "question": "What do the terms, methods and formulas in this report mean?",
    }

    # ==================================================================
    # Sources register
    # ==================================================================
    ll = _q(conn, "SELECT source, MAX(run_at) AS last_load FROM load_log GROUP BY source")
    last = {r.source: r.last_load for r in ll.itertuples()} if not ll.empty else {}

    def _ll(*keys):
        vals = [str(last[k])[:10] for k in keys if k in last and last[k]]
        return ap_date(max(vals)) if vals else "n/a"

    register = [
        ["STR daily data", "Visit Dana Point select hotels", hspan if not costar_ext else f"Through {ap_date(str_through)}", _ll("STR", "str"), "Hotel occupancy, rate, revenue, night of week, compression", "Rooms-weighted; checked against CoStar daily for shared dates"],
        ["CoStar daily export", "Same hotels", f"Through {ap_date(hend)}" if costar_ext else "n/a", ap_date(f["costar_pull"]) if f.get("costar_pull") else "n/a", "Extends hotel data past STR's latest file; segmentation", f"Occupancy agrees within 0.15 points on {f['agreement']['occ_ok']} of {f['agreement']['n']} shared dates" if f.get("agreement") else "Compared with STR"],
        ["CoStar market reports", "Newport Beach/Dana Point, Orange County, U.S.", f"Year to date through {bthrough or 'n/a'}", ap_date(bench["report_date"]) if bench.get("report_date") else "n/a", "Benchmarks, multi-year trend", "Report date pinned per market"],
        ["CoStar participation roster", "Comp-set hotels and rooms", "Latest month", ap_date(f["costar_pull"]) if f.get("costar_pull") else "n/a", "Hotel count and rooms", "Latest snapshot only"],
        ["STR weekly peer report", "Six coastal markets", f"{len(peers.get('weeks', []))} weeks", ap_date(f["str_week"]) if f.get("str_week") else "n/a", "Peer ranks and revenue index", "Weeks counted beside each figure"],
        ["Datafy visitor economy", "Dana Point visitors", dfy_period, _ll("datafy", "Datafy"), "Trips, spending, markets, categories, origins, demographics", "Latest export period per measure"],
        ["Datafy monthly spending", "Dana Point visitors", dfy_mspan, _ll("datafy", "Datafy"), "Monthly spending and visitor days", "Complete months only"],
        ["Datafy campaign attribution", "2025-26 annual campaign", span(cp.report_period_start, cp.report_period_end) if cp is not None else "n/a", _ll("datafy", "Datafy"), "Reach, trips, impact", "No media cost recorded"],
        ["Datafy paid-media attribution", "Current flight", f"Through {snap}", snap, "Impressions, clicks, spend, return, markets, tactics", "Latest snapshot"],
        ["Datafy website attribution", "visitdanapoint.com", span(w.report_period_start, w.report_period_end) if w is not None else "n/a", _ll("datafy", "Datafy"), "Sessions, attributed trips, impact", "Single report"],
        ["Later social analytics", "Instagram, Facebook, TikTok", f"Through {ap_date(max(v['last'] for v in social.values()))}" if social else "n/a", _ll("later_ig_profile_growth", "later_fb_profile_growth"), "Followers", "Latest non-blank value"],
        ["Visit California airport traffic", "John Wayne Airport", f"Through {sna_m}", _ll("Visit_CA"), "Passengers", "Context only"],
        ["Visit California travel forecast", "California", "2009 to 2028", _ll("Visit_CA"), "Statewide visits and forecast", "Forecast years labeled"],
        ["U.S. Bureau of Labor Statistics", "California", f"Through {lh_m}", _ll("bls", "BLS"), "Leisure and hospitality, accommodation jobs", "Statewide, not Dana Point"],
        ["Visit Dana Point events calendar", "Dana Point", "Next 120 days", _ll("VDP_Events"), "Upcoming events",
         "Read from the visitdanapoint.com events feed; dated events only, weekly series left out"],
        ["visitdanapoint.com", "Dana Point destination facts", "Pages read Oct. 14, 2026", "Oct. 14, 2026",
         "About Dana Point notes, events, photos", "Quoted as published; each line links to its page"],
    ]
    bl = _q(conn, "SELECT MAX(updated_at) AS u FROM bls_employment_monthly")
    if not bl.empty and bl.iloc[0]["u"]:
        for r in register:
            if r[0].startswith("U.S. Bureau"):
                r[3] = ap_date(str(bl.iloc[0]["u"])[:10])
    # About Dana Point: the destination in Visit Dana Point's own published words.
    destination = [
        {"text": "Dana Point is located in the heart of Orange County, between San Diego and Los Angeles, close to John Wayne Airport.",
         "src": "visitdanapoint.com, Why Dana Point", "url": "https://visitdanapoint.com/plan-your-visit/why-dana-point/"},
        {"text": "In 2013 Dana Point earned its trademark as the Dolphin and Whale Watching Capital of the World\u00ae.",
         "src": "visitdanapoint.com, Whale Watching", "url": "https://visitdanapoint.com/things-to-do/whale-watching/"},
        {"text": "Public dolphin and whale watching trips are offered every day, all year long; gray whales pass from December to May.",
         "src": "visitdanapoint.com, Whale Watching and Why Dana Point", "url": "https://visitdanapoint.com/things-to-do/whale-watching/"},
        {"text": "The site names early June to mid-October as the best time to visit.",
         "src": "visitdanapoint.com, Why Dana Point", "url": "https://visitdanapoint.com/plan-your-visit/why-dana-point/"},
        {"text": "Dana Point is more than a destination. It's a feeling.",
         "src": "visitdanapoint.com, home page", "url": "https://visitdanapoint.com/"},
    ]
    # ---- G6: which source feeds which section (matched on each section's own source lines) ----
    _src_keys = [("STR", ["STR "]), ("CoStar", ["CoStar"]), ("Datafy", ["Datafy"]), ("Later", ["Later"]),
                 ("Visit California", ["Visit California"]), ("BLS", ["Bureau of Labor", "BLS"]),
                 ("Events calendar", ["events calendar"]), ("visitdanapoint.com", ["visitdanapoint.com"])]
    g_nodes, g_edges = [], []
    for name, _k in _src_keys:
        g_nodes.append({"id": "s:" + name, "label": name, "kind": "source"})
    for sec in sections:
        g_nodes.append({"id": "r:" + sec["id"], "label": sec["tab"], "kind": "section"})
        blob = json.dumps(sec, default=str)
        for name, keys in _src_keys:
            if any(k in blob for k in keys):
                g_edges.append({"source": "s:" + name, "target": "r:" + sec["id"]})
    g_nodes.append({"id": "r:overview", "label": "About Dana Point", "kind": "section"})
    g_edges.append({"source": "s:visitdanapoint.com", "target": "r:overview"})
    _used = {e["source"] for e in g_edges}
    graph = {"nodes": [n for n in g_nodes if n["kind"] == "section" or n["id"] in _used], "edges": g_edges}

    sources = {"prepared": prep, "register": register, "graph": graph,
               "cols": ["Source", "Coverage", "Period", "Pulled or loaded", "Used for", "How PULSE checked it"],
               "recs": [
                   "City-reported monthly TOT and TBID collections are not loaded; tax figures are estimates.",
                   "Dates after STR's daily file ends come from CoStar's daily grid for the same hotels.",
                   "The annual campaign report records no media cost, so its return is not computed.",
               ],
               "recs_advice": [
                   "Load city-reported monthly TOT and TBID collections to replace the estimates.",
                   "Load each STR daily file as it arrives so CoStar's extension covers fewer days.",
                   "Add media cost to the annual campaign report to compute its return.",
               ]}

    return {
        "title": "Dana Point PULSE Insights",
        "subtitle": "Prepared for Visit Dana Point · Hotels, visitors, spending, marketing and outlook",
        "period": f"Data through {ap_date(hend)}",
        "window_label": "Year to date" if _ytd else (m.window.label if getattr(m.window, "label", "") and m.window.label != "Custom range" else hspan),
        "period_detail": f"Hotels {hspan}; Datafy {dfy_period}",
        "prepared": prep,
        "summary": summary, "takeaways": takeaways, "qa": qa,
        "sections": sections, "appendix": appendix, "sources": sources, "destination": destination,
        "contact": "Powered by GloCon Solutions for Visit Dana Point",
    }


# ---------------------------------------------------------------------------
# Tables for export (every table and chart in a section, as rows)
# ---------------------------------------------------------------------------

def section_tables(report: dict, sid: str) -> list[dict]:
    """Every table in a section, numbers kept as display strings plus a parsed value."""
    out = []
    if sid == "overview":
        s = report["summary"]
        out.append({"title": "By the numbers", "cols": ["Measure", "Value", "Source"],
                    "rows": [[x["l"], x["v"], x["src"]] for x in s["stats"]], "src": "PULSE Summary"})
        out.append({"title": "Growth vs. a year earlier", "cols": ["Measure", "Change"],
                    "rows": [[g["l"], spct(g["v"])] for g in s["growth"]], "src": "PULSE Summary"})
        out.append({"title": "Key takeaways", "cols": ["Section", "Headline metric", "Meaning", "Source"],
                    "rows": [[t["tab"], t["v"], t["l"], t["src"]] for t in report["takeaways"]], "src": "PULSE"})
        return out
    if sid == "appx":
        a = report["appendix"]
        out.append({"title": "Glossary", "cols": ["Term", "Definition"], "rows": [list(x) for x in a["glossary"]], "src": "PULSE"})
        out.append({"title": "Methods", "cols": ["Method", "How"], "rows": [list(x) for x in a["methods"]], "src": "PULSE"})
        out.append({"title": "Formulas", "cols": ["Measure", "Formula", "Worked example"], "rows": a["formulas"], "src": "PULSE"})
        if a.get("photos"):
            out.append({"title": "Photo credits", "cols": ["Photo", "Where it appears", "Source", "Link"],
                        "rows": [list(x) for x in a["photos"]], "src": "PULSE"})
        return out
    if sid == "sources":
        s = report["sources"]
        out.append({"title": "Source register", "cols": s["cols"], "rows": s["register"], "src": "PULSE"})
        return out
    sec = next((x for x in report["sections"] if x["id"] == sid), None)
    if not sec:
        return out
    out.append({"title": "By the numbers", "cols": ["Label", "Value", "Measure", "Change or context", "Source"],
                "rows": [[x["k"], x["v"], x["l"], x["p"], x["src"]] for x in sec["numbers"]], "src": sec["byline_src"]})
    out.append({"title": "How the key insight is calculated", "cols": ["Step", "Calculation", "Result", "Source"], "rows": sec["calc"], "src": "PULSE calculation"})
    t = sec.get("table")
    if t and t.get("rows"):
        out.append({"title": t["title"], "cols": t["cols"], "rows": t["rows"], "src": t["src"]})
    for c in sec.get("charts", []):
        out.extend(_chart_table(c))
    for mo in sec.get("more", []):
        if mo["kind"] == "table":
            out.append({"title": mo["title"], "cols": mo["cols"], "rows": mo["rows"], "src": mo["src"]})
        else:
            out.extend(_chart_table(mo["chart"]))
    if sec.get("heat"):
        hm = sec["heat"]
        out.append({"title": hm["title"], "cols": ["Month"] + hm["cols"] + ["Nights at 90%+"],
                    "rows": [[r["m"], pct(r["wn"]), pct(r["we"]), pct(r["all"]), str(r["n90"])] for r in hm["rows"]], "src": hm["src"]})
    if sec.get("advice"):
        out.append({"title": "Compare and contrast, with solutions (GloCon Solutions perspective)", "cols": ["Type", "Point", "Solution", "Source"],
                    "rows": [[a["type"], a["point"], a["action"], a["src"]] for a in sec["advice"]], "src": "GloCon Solutions LLC; findings sourced as listed"})
    out.append({"title": "Compare and contrast", "cols": ["Type", "Finding", "How to read it", "Source"],
                "rows": [[c["type"], c["point"], c["solution"], c["src"]] for c in sec["cc"]], "src": "PULSE"})
    return out


def _fmtv(v, f):
    if v is None:
        return "n/a"
    return {"pct": pct(v), "pct2": pct(v, 2), "usd": usd(v), "usdbig": usd_big(v), "num": num(v), "m": f"{v:.1f}M"}.get(f, str(v))


def _chart_table(c: dict) -> list[dict]:
    if not c:
        return []
    if c["type"] == "hbar":
        return [{"title": c["title"], "cols": ["Item", "Value"], "rows": [[r["l"], _fmtv(r["v"], r.get("fmt"))] for r in c["rows"]], "src": c["src"]}]
    if c["type"] in ("line", "vbar"):
        if not c.get("series"):
            return []
        cols = ["Period"] + [s["name"] for s in c["series"]]
        rows = [[x] + [_fmtv(s["values"][i] if i < len(s["values"]) else None, s.get("fmt")) for s in c["series"]] for i, x in enumerate(c["x"])]
        return [{"title": c["title"], "cols": cols, "rows": rows, "src": c["src"]}]
    return []


SECTION_ORDER_EXTRA = [("overview", "PULSE Summary and overview")]


def all_section_ids(report) -> list[tuple[str, str]]:
    return [("overview", "PULSE Summary and overview")] + [(s["id"], s["tab"]) for s in report["sections"]] + [("appx", "Appendix"), ("sources", "Sources")]


def _parse_num(s):
    """'$1,234' -> 1234.0, '12.5%' -> 0.125 (percent), '+3.1 points' -> 3.1. Returns (value, kind) or (None, None)."""
    if not isinstance(s, str):
        return None, None
    t = s.strip().replace("▲", "").replace("▼", "").replace("▶", "").replace("−", "-").strip()
    try:
        if t.endswith("%") and t[:-1].replace(",", "").replace("+", "").replace("-", "").replace(".", "").isdigit():
            return float(t[:-1].replace(",", "")) / 100, "pct"
        if t.startswith("$") and t[1:].replace(",", "").replace(".", "").isdigit():
            return float(t[1:].replace(",", "")), "usd"
        if t.replace(",", "").replace(".", "").replace("-", "").isdigit() and t not in ("-",):
            return float(t.replace(",", "")), "num"
    except Exception:
        pass
    return None, None


def build_csv(report: dict, section_ids: list[str]) -> str:
    buf = io.StringIO()
    wr = csv.writer(buf)
    wr.writerow(["Section", "Table", "Row", "Column", "Value", "Source"])
    names = dict(all_section_ids(report))
    for sid in section_ids:
        for t in section_tables(report, sid):
            for i, r in enumerate(t["rows"]):
                for j, v in enumerate(r):
                    wr.writerow([names.get(sid, sid), t["title"], i + 1, t["cols"][j] if j < len(t["cols"]) else "", v, t["src"]])
    return buf.getvalue()


def build_xlsx(report: dict, section_ids: list[str]) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill, Border, Side

    wb = Workbook()
    ab = wb.active
    ab.title = "About"
    teal = "1D6E86"
    ab["A1"] = report["title"]
    ab["A1"].font = Font(bold=True, italic=True, size=18, color="0B2530", name="Georgia")
    rows = [("Prepared", report["prepared"]), ("Period", report["period"]), ("Detail", report["period_detail"]),
            ("Prepared by", "GloCon Solutions LLC for Visit Dana Point"), ("Contact", "John Picou, GloCon Solutions LLC, john.picou@gloconsolutions.com"), ("Note", "Licensed data (STR, CoStar, Datafy) appears as summary tables only.")]
    for i, (k, v) in enumerate(rows, start=3):
        ab.cell(i, 1, k).font = Font(bold=True, color=teal)
        ab.cell(i, 2, v)
    ab.column_dimensions["A"].width = 16
    ab.column_dimensions["B"].width = 90
    names = dict(all_section_ids(report))
    thin = Side(style="thin", color="E2E8F0")
    for sid in section_ids:
        ws = wb.create_sheet((names.get(sid, sid)[:31]).replace("/", "-"))
        ws["A1"] = names.get(sid, sid)
        ws["A1"].font = Font(bold=True, italic=True, size=15, color="0B2530", name="Georgia")
        r0 = 3
        widths = {}
        for t in section_tables(report, sid):
            ws.cell(r0, 1, t["title"]).font = Font(bold=True, size=12, color=teal)
            r0 += 1
            for j, c in enumerate(t["cols"], start=1):
                cell = ws.cell(r0, j, c)
                cell.font = Font(bold=True, color="0B2530")
                cell.border = Border(bottom=Side(style="medium", color=teal))
                widths[j] = max(widths.get(j, 10), min(len(str(c)) + 2, 60))
            r0 += 1
            for row in t["rows"]:
                for j, v in enumerate(row, start=1):
                    val, kind = _parse_num(v)
                    cell = ws.cell(r0, j, val if val is not None else v)
                    if kind == "pct":
                        cell.number_format = "0.0%"
                    elif kind == "usd":
                        cell.number_format = '"$"#,##0'
                    elif kind == "num":
                        cell.number_format = "#,##0"
                    cell.border = Border(bottom=thin)
                    cell.alignment = Alignment(wrap_text=True, vertical="top")
                    widths[j] = max(widths.get(j, 10), min(len(str(v)) + 2, 60))
                r0 += 1
            ws.cell(r0, 1, f"Source: {t['src']}").font = Font(italic=True, size=9, color="64748B")
            r0 += 2
        for j, wdt in widths.items():
            ws.column_dimensions[ws.cell(1, j).column_letter].width = wdt
    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()


def build_pptx(report: dict, section_ids: list[str]) -> bytes | None:
    """Editable deck: title slide, then for each section a news slide, a by-the-numbers slide,
    a details slide and one native chart per chart spec."""
    try:
        from pptx import Presentation
        from pptx.chart.data import CategoryChartData
        from pptx.dml.color import RGBColor
        from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
        from pptx.util import Inches, Pt
    except Exception:
        return None
    TEAL, TEAL_DK, INK, INK3, MAROON, WHITE = (RGBColor(0x1D, 0x6E, 0x86), RGBColor(0x12, 0x3C, 0x4A), RGBColor(0x0B, 0x25, 0x30),
                                               RGBColor(0x64, 0x74, 0x8B), RGBColor(0xA8, 0x46, 0x1F), RGBColor(0xFF, 0xFF, 0xFF))
    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
    blank = prs.slide_layouts[6]

    def text(slide, x, y, w, h, s, size=18, bold=False, italic=False, color=INK, font="Georgia"):
        tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
        tf = tb.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        r = p.add_run()
        r.text = s
        r.font.size, r.font.bold, r.font.italic, r.font.color.rgb = Pt(size), bold, italic, color
        r.font.name = font
        return tb

    def band(slide, color=TEAL_DK, h=0.18):
        sh = slide.shapes.add_shape(1, 0, 0, prs.slide_width, Inches(h))
        sh.fill.solid()
        sh.fill.fore_color.rgb = color
        sh.line.fill.background()

    def footer(slide, src):
        text(slide, 0.6, 6.9, 12.1, 0.4, f"Source: {src}", 10, color=INK3, font="Helvetica Neue")

    s = prs.slides.add_slide(blank)
    bg = s.shapes.add_shape(1, 0, 0, prs.slide_width, prs.slide_height)
    bg.fill.solid()
    bg.fill.fore_color.rgb = TEAL_DK
    bg.line.fill.background()
    text(s, 0.8, 2.3, 11.5, 1.4, report["title"], 54, True, True, WHITE)
    text(s, 0.8, 3.7, 11.5, 0.6, report["subtitle"], 20, color=RGBColor(0xA5, 0xDD, 0xE9), font="Helvetica Neue")
    text(s, 0.8, 4.4, 11.5, 0.6, f"{report['period']} · Prepared {report['prepared']}", 16, color=WHITE, font="Helvetica Neue")

    names = dict(all_section_ids(report))
    for sid in section_ids:
        if sid == "overview":
            sm = report["summary"]
            sl = prs.slides.add_slide(blank)
            band(sl)
            text(sl, 0.6, 0.4, 12, 0.4, "PULSE SUMMARY", 12, True, color=TEAL, font="Helvetica Neue")
            text(sl, 0.6, 0.8, 12, 1.2, sm["headline"], 32, True, True)
            text(sl, 0.6, 2.0, 7.4, 4.6, "\n\n".join(sm["paras"][:3]), 13, color=INK, font="Helvetica Neue")
            y = 2.0
            for st_ in sm["stats"][:4]:
                text(sl, 8.4, y, 4.4, 0.6, st_["v"], 28, True, True, TEAL_DK)
                text(sl, 8.4, y + 0.6, 4.4, 0.5, st_["l"], 11, color=INK3, font="Helvetica Neue")
                y += 1.15
            footer(sl, "STR, CoStar, Datafy; PULSE")
            sl = prs.slides.add_slide(blank)
            band(sl)
            text(sl, 0.6, 0.4, 12, 0.8, "Key takeaways", 32, True, True)
            for i, t in enumerate(report["takeaways"]):
                cx, cy = 0.6 + (i % 3) * 4.1, 1.4 + (i // 3) * 1.8
                text(sl, cx, cy, 3.9, 0.3, t["tab"].upper(), 10, True, color=TEAL, font="Helvetica Neue")
                text(sl, cx, cy + 0.3, 3.9, 0.6, t["v"], 26, True, True, TEAL_DK)
                text(sl, cx, cy + 0.9, 3.9, 0.8, t["l"], 11, color=INK, font="Helvetica Neue")
            footer(sl, "PULSE")
            continue
        if sid in ("appx", "sources"):
            for t in section_tables(report, sid):
                sl = prs.slides.add_slide(blank)
                band(sl)
                text(sl, 0.6, 0.4, 12, 0.7, f"{names[sid]}: {t['title']}", 26, True, True)
                rows = t["rows"][:14]
                tbl = sl.shapes.add_table(len(rows) + 1, len(t["cols"]), Inches(0.6), Inches(1.3), Inches(12.1), Inches(0.3 * (len(rows) + 1))).table
                for j, c in enumerate(t["cols"]):
                    tbl.cell(0, j).text = c
                for i, r in enumerate(rows, start=1):
                    for j, v in enumerate(r):
                        tbl.cell(i, j).text = str(v)
                for row in tbl.rows:
                    for cell in row.cells:
                        for p in cell.text_frame.paragraphs:
                            for rr in p.runs:
                                rr.font.size = Pt(9)
                footer(sl, t["src"])
            continue
        sec = next((x for x in report["sections"] if x["id"] == sid), None)
        if not sec:
            continue
        sl = prs.slides.add_slide(blank)
        band(sl)
        text(sl, 0.6, 0.35, 12, 0.4, f"NEWS REPORT · {sec['tab'].upper()}", 12, True, color=TEAL, font="Helvetica Neue")
        text(sl, 0.6, 0.75, 12, 1.3, sec["headline"], 30, True, True)
        text(sl, 0.6, 2.1, 8.2, 3.5, sec["lede"], 14, font="Helvetica Neue")
        text(sl, 9.2, 2.1, 3.6, 1.0, sec["key"]["v"], 40, True, True, TEAL_DK)
        text(sl, 9.2, 3.1, 3.6, 1.0, sec["key"]["l"], 12, color=INK3, font="Helvetica Neue")
        text(sl, 0.6, 5.3, 12, 1.4, "Why it matters: " + sec["why"], 12, color=INK, font="Helvetica Neue")
        footer(sl, sec["lede_src"].replace("Source: ", ""))
        sl = prs.slides.add_slide(blank)
        band(sl)
        text(sl, 0.6, 0.4, 12, 0.7, f"{sec['tab']}: by the numbers", 28, True, True)
        for i, st_ in enumerate(sec["numbers"]):
            cx = 0.6 + i * 3.1
            text(sl, cx, 1.5, 2.9, 0.3, st_["k"].upper(), 10, True, color=TEAL, font="Helvetica Neue")
            text(sl, cx, 1.85, 2.9, 0.8, st_["v"], 30, True, True, TEAL_DK)
            text(sl, cx, 2.7, 2.9, 0.4, st_["l"], 13, True, font="Helvetica Neue")
            text(sl, cx, 3.1, 2.9, 0.8, st_["p"], 11, color=INK3, font="Helvetica Neue")
        y = 4.3
        for k in ("who", "what", "when", "where", "why", "how"):
            d = sec["details"][k]
            text(sl, 0.6 + (["who", "what", "when", "where", "why", "how"].index(k) % 3) * 4.1, y + (0 if k in ("who", "what", "when") else 1.2), 3.9, 1.1,
                 f"{k.upper()}  {d['fig']}\n{d['fig_l']}", 12, font="Helvetica Neue")
        footer(sl, sec["byline_src"])
        for c in sec.get("charts", []) + [mo["chart"] for mo in sec.get("more", []) if mo["kind"] == "chart"]:
            if c["type"] not in ("hbar", "line", "vbar"):
                continue
            cd = CategoryChartData()
            if c["type"] == "hbar":
                rows = c["rows"][:12][::-1]
                if not rows:
                    continue
                cd.categories = [r["l"] for r in rows]
                cd.add_series("Value", [r["v"] for r in rows])
                ct = XL_CHART_TYPE.BAR_CLUSTERED
            else:
                if not c.get("series") or not c.get("x"):
                    continue
                cd.categories = c["x"]
                for se in c["series"]:
                    cd.add_series(se["name"], [v if v is not None else 0 for v in se["values"]])
                ct = XL_CHART_TYPE.LINE_MARKERS if c["type"] == "line" else XL_CHART_TYPE.COLUMN_CLUSTERED
            sl = prs.slides.add_slide(blank)
            band(sl)
            text(sl, 0.6, 0.4, 12, 0.7, c["title"], 26, True, True)
            text(sl, 0.6, 1.05, 12, 0.4, c.get("sub", ""), 13, color=INK3, font="Helvetica Neue")
            ch = sl.shapes.add_chart(ct, Inches(0.6), Inches(1.6), Inches(12.1), Inches(5.1), cd).chart
            ch.has_legend = c["type"] != "hbar" and len(c.get("series", [])) > 1
            if ch.has_legend:
                ch.legend.position = XL_LEGEND_POSITION.TOP
                ch.legend.include_in_layout = False
            for i, plot_se in enumerate(ch.plots[0].series):
                col = TEAL if i == 0 else MAROON
                if c["type"] == "line":
                    plot_se.format.line.color.rgb = col
                else:
                    plot_se.format.fill.solid()
                    plot_se.format.fill.fore_color.rgb = col
            ch.plots[0].has_data_labels = c["type"] == "hbar"
            footer(sl, c["src"])
    bio = io.BytesIO()
    prs.save(bio)
    return bio.getvalue()


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

def _read(p, mode="r"):
    with open(p, mode) as fh:
        return fh.read()


def _b64(p):
    return base64.b64encode(_read(p, "rb")).decode()


# Visual add-ons a reader can switch on or off in the app (added 2026-10-15).
# Keys match pulse_viz.js renderers; anything switched off keeps its original
# chart, table or text.
VIZ_OPTIONS = {
    "t8": "Highlighted figures in each lede",
    "g2": "Interactive charts",
    "waterfall": "What moved room revenue (waterfall)",
    "calendar": "Every night by occupancy (calendar)",
    "graph": "Which source feeds which section (map)",
    "pipeline": "How data reaches PULSE (diagram)",
    "info": "About Dana Point (cards)",
}


def render_html(report: dict, public_url: str = "https://vdppulse.gloconsolutions.com/", embed_exports: bool = True,
                viz_on: list | None = None) -> str:
    """One self-contained page (CSS, JS, logo, photo, map geometry inline)."""
    a = os.path.join(_HERE, "assets")
    css = _read(os.path.join(_HERE, "pulse_insights.css"))
    js = _read(os.path.join(_HERE, "pulse_insights.js"))
    viz = _read(os.path.join(_HERE, "pulse_viz.js")) if os.path.exists(os.path.join(_HERE, "pulse_viz.js")) else ""
    d3js = _read(os.path.join(_HERE, "assets", "d3.v7.9.0.min.js")) if os.path.exists(os.path.join(_HERE, "assets", "d3.v7.9.0.min.js")) else ""
    geo = _read(os.path.join(a, "us_states_albers.json"))
    jszip = _read(os.path.join(a, "jszip.min.js")) if os.path.exists(os.path.join(a, "jszip.min.js")) else ""
    logo = f"data:image/svg+xml;base64,{_b64(os.path.join(a, 'vdp_logo_nav.svg'))}"
    photo = f"data:image/jpeg;base64,{_b64(os.path.join(a, 'photos', 'hero_coast_board.jpg'))}" if os.path.exists(os.path.join(a, "photos", "hero_coast_board.jpg")) else ""
    try:  # visitdanapoint.com photo for the report banner; the bundled photo stays underneath
        import vdp_site
        site_photo, photo_credit = vdp_site.photo_url("header_aerial", 2000, 640), html.escape(vdp_site.PHOTO_CREDIT)
    except Exception:  # noqa: BLE001
        site_photo, photo_credit = "", ""
    data = dict(report)
    data["sectionList"] = [{"id": i, "tab": t} for i, t in all_section_ids(report)]
    data["publicUrl"] = public_url
    if embed_exports:
        ids = [i for i, _ in all_section_ids(report)]
        data["csvBySection"] = {i: build_csv(report, [i]) for i in ids}
        data["tablesBySection"] = {i: section_tables(report, i) for i in ids}
        deck = build_pptx(report, ids)
        data["deck"] = base64.b64encode(deck).decode() if deck else None
        # slide index ranges per section in the full deck, for client-side subsetting
        data["deckMap"] = _deck_map(report, ids) if deck else None
    payload = json.dumps(data, default=str).replace("</", "<\\/")
    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        f"<title>{html.escape(report['title'])}</title><style>{css}</style></head>"
        f"<body><div id=\"app\" data-logo=\"{logo}\" data-photo=\"{photo}\" data-photo-site=\"{site_photo}\" data-photo-credit=\"{photo_credit}\"></div>"
        f"<script>window.PULSE_DATA={payload};window.PULSE_GEO={geo};window.PULSE_VIZ_ON={json.dumps(viz_on) if viz_on is not None else 'null'};</script>"
        f"<script>{jszip}</script><script>{d3js}</script><script>{js}</script><script>{viz}</script></body></html>"
    )


def _deck_map(report, ids):
    """Count slides build_pptx makes per section, in order, so JS can keep only chosen ones."""
    counts = {}
    for sid in ids:
        if sid == "overview":
            counts[sid] = 2
        elif sid in ("appx", "sources"):
            counts[sid] = len(section_tables(report, sid))
        else:
            sec = next((x for x in report["sections"] if x["id"] == sid), None)
            n = 2
            for c in sec.get("charts", []) + [mo["chart"] for mo in sec.get("more", []) if mo["kind"] == "chart"]:
                if c["type"] == "hbar" and c.get("rows"):
                    n += 1
                elif c["type"] in ("line", "vbar") and c.get("series") and c.get("x"):
                    n += 1
            counts[sid] = n
    out, i = {}, 2  # slide 1 is the title slide
    for sid in ids:
        out[sid] = [i, i + counts[sid] - 1]
        i += counts[sid]
    return out
