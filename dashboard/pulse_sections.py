"""
pulse_sections.py, the four live sections of the Dana Point PULSE board view
plus the at-a-glance band, the freshness strip, the sidebar, and the About
panel. app.py keeps everything else (header, filters, PDF report, Intelligence
Brief, section viewer, flipbook, notes, repository, admin tools) and calls
these renderers in place of the older section code.

Each section follows one pattern so the page reads the same way throughout:
  section question -> KPI tiles -> written answer -> visuals, each with its own
  question, source line, written answer, and "How to read this" -> an
  optional AI deep dive (click to generate).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

import brand_tokens as bt
import pulse_charts as pc
import pulse_data as pdx
import pulse_story as ps
import pulse_ui as ui

CFG = pc.CONFIG

SECTION_LINKS = [
    ("pulse-glance", "At a glance"),
    ("pulse-snapshot", "Performance snapshot"),
    ("pulse-origins", "Visitors and spend"),
    ("pulse-market", "Market position"),
    ("pulse-forward", "Forward outlook"),
    ("pulse-brain", "Intelligence brief"),
    ("pulse-fullreport", "Full report"),
    ("pulse-about", "About this data"),
    ("pulse-notes", "Notes and downloads"),
]


def _md(s: str) -> None:
    """Raw-HTML markdown block. Streamlit gives every markdown container a
    -1rem bottom margin (meant to cancel a trailing <p> margin) and clips the
    element at that height, so HTML built from divs lost its last 16px (the
    "How to read this" line and wrapped source notes on phones). The pz-md
    wrapper pads that 1rem back so nothing is cut and spacing is unchanged."""
    if s:
        st.markdown(f'<div class="pz-md">{s}</div>', unsafe_allow_html=True)


def _chart(fig, key: str) -> None:
    if fig is not None:
        st.plotly_chart(fig, use_container_width=True, config=CFG, key=key)


def _src_hotel(m: ps.Model) -> str:
    f = m.fresh
    s = "Source: STR daily, VDP Select comp set"
    if f.get("costar_extends") and f.get("str_through") is not None and m.hotel and m.hotel.get("costar_days"):
        s += f", extended past {pdx.fmt_date(f['str_through'])} with CoStar's daily export for the same hotels"
    return s + ". Rooms-weighted."


# ---------------------------------------------------------------------------
# Freshness, sidebar, glance
# ---------------------------------------------------------------------------

def freshness_items(m: ps.Model) -> list[tuple[str, str, bool]]:
    f = m.fresh
    today = pd.Timestamp.now().normalize()
    items = []
    if f.get("hotel_through") is not None:
        lag = (today - pd.Timestamp(f["hotel_through"])).days
        txt = f"<b>Hotels</b> through {pdx.fmt_date(f['hotel_through'])}"
        if f.get("costar_extends") and f.get("str_through") is not None:
            txt += f" (STR file to {pdx.fmt_date(f['str_through'], False)}, CoStar after)"
        items.append((txt, "", lag <= 35))
    if f.get("str_week"):
        items.append((f"<b>STR weekly</b> week ending {pdx.fmt_date(f['str_week'])}", "",
                      (today - pd.Timestamp(f["str_week"])).days <= 21))
    if f.get("costar_report"):
        items.append((f"<b>CoStar</b> pulled {pdx.fmt_date(f['costar_pull'] or f['costar_report'])}", "",
                      (today - pd.Timestamp(f["costar_pull"] or f["costar_report"])).days <= 10))
    if f.get("datafy_end"):
        items.append((f"<b>Datafy</b> {pdx.fmt_month(f['datafy_start'])} to {pdx.fmt_month(f['datafy_end'])}"
                      + (f", campaigns {pdx.fmt_date(f['ads_snapshot'], False)}" if f.get("ads_snapshot") else ""), "",
                      (today - pd.Timestamp(f["datafy_end"])).days <= 62))
    return items


def render_sidebar(m: ps.Model | None, logo_uri: str) -> None:
    rows = []
    if m is not None:
        f = m.fresh
        if f.get("hotel_through") is not None:
            rows.append(("Hotel performance", f"Through {pdx.fmt_date(f['hotel_through'])}"))
        if f.get("str_week"):
            rows.append(("STR peer markets", f"Week ending {pdx.fmt_date(f['str_week'])}"))
        if f.get("costar_report"):
            rows.append(("CoStar market reports", f"Report dated {pdx.fmt_date(f['costar_report'])}"))
        if f.get("datafy_end"):
            rows.append(("Datafy visitors", f"{pdx.fmt_month(f['datafy_start'])} to {pdx.fmt_month(f['datafy_end'])}"))
        if f.get("ads_snapshot"):
            rows.append(("Datafy campaigns", f"As of {pdx.fmt_date(f['ads_snapshot'])}"))
        if f.get("insights"):
            rows.append(("Insights engine", f"Run {pdx.fmt_date(f['insights'])}"))
    with st.sidebar:
        _md(ui.sidebar(logo_uri, SECTION_LINKS, rows))
        if m is not None:
            st.caption(f"Data window: {m.window.label}, {m.window.span_text()}.")


def render_freshness(m: ps.Model) -> None:
    _md(ui.freshness_strip(freshness_items(m)))


def render_glance(m: ps.Model) -> None:
    span = m.window.span_text() if not m.hotel else f"{pdx.fmt_date(m.hotel['start'])} to {pdx.fmt_date(m.hotel['end'])}"
    _md('<div id="pulse-glance" class="pulse-anchor"></div>')
    _md(ui.glance(m.text.get("glance", []), m.window.label, span))


# ---------------------------------------------------------------------------
# 1. Performance snapshot
# ---------------------------------------------------------------------------

def render_snapshot(m: ps.Model, ask) -> None:
    t = m.text["snapshot"]
    h, tr = m.hotel, m.trend
    _md(ui.section_header("pulse-snapshot", 1, "Hotel performance · STR", "Performance Snapshot", t["question"]))
    if not h:
        st.info("STR hotel data is not available for this window yet.")
        ask("snapshot", "Performance Snapshot", t["question"])
        return
    spark = (lambda col: ui.sparkline(tr[col].tolist()) if tr is not None and not tr.empty and col in tr else "")
    prior = h.get("prior") or {}
    d80_delta = (h["d80"] - prior["d80"]) if prior.get("d80") is not None and h.get("method") == "daily" else None
    tiles = [
        {"label": "Occupancy", "value": ps.pct(h["occ"]), "spark": spark("occ"),
         "delta_html": ui.delta_chip(ps.pts(h.get("occ_pts")) + " vs last year" if h.get("occ_pts") is not None else None, h.get("occ_pts")),
         "foot": "Rooms sold / rooms available"},
        {"label": "ADR", "value": ps.usd(h["adr"]), "spark": spark("adr"),
         "delta_html": ui.delta_chip(ps.spct(h.get("adr_pct")) + " vs last year" if h.get("adr_pct") is not None else None, h.get("adr_pct")),
         "foot": "Room revenue / rooms sold"},
        {"label": "RevPAR", "value": ps.usd(h["revpar"]), "spark": spark("revpar"),
         "delta_html": ui.delta_chip(ps.spct(h.get("revpar_pct")) + " vs last year" if h.get("revpar_pct") is not None else None, h.get("revpar_pct")),
         "foot": "Room revenue / rooms available"},
        {"label": "Nights at 80%+", "value": f"{h.get('d80', 0)}",
         "delta_html": ui.delta_chip(f"{d80_delta:+d} nights vs last year" if d80_delta is not None else None, d80_delta, tol=0.5),
         "foot": f"{h.get('d90', 0)} of them at 90% or higher, {h['n']} nights in window"},
    ]
    _md(ui.kpi_grid(tiles))
    _md(ui.answer(t["answer"]))

    c1, c2 = st.columns(2, gap="medium")
    with c1:
        with st.container(border=True):
            _md(ui.viz_head(t["occ_q"], "Occupancy, this period against the same days last year. " + _src_hotel(m)))
            _chart(pc.occupancy_trend(tr), "pz_occ_trend")
            _md(ui.viz_foot(t["occ_a"], "The solid teal line is this period; the dashed sand line is the same dates a year "
                                        "earlier, shifted 364 days so weekdays line up. Where teal sits above sand, hotels "
                                        "sold a larger share of their rooms than last year. Hover for exact values."))
    with c2:
        with st.container(border=True):
            _md(ui.viz_head(t["adr_q"], "Average daily rate against last year, and what moved RevPAR."))
            _chart(pc.adr_trend(tr), "pz_adr_trend")
            _md(ui.split_bar(h.get("rev_from_occ_pts"), h.get("rev_from_adr_pts"), h.get("revpar_pct")))
            _md(ui.viz_foot(t["adr_a"], "RevPAR is occupancy times ADR, so any change in RevPAR comes from selling more rooms, "
                                        "charging more per room, or both. The bar under the chart splits the change into "
                                        "those two parts using a log decomposition, so the parts add back to the total."))
    with st.container(border=True):
        _md(ui.viz_head(t["dow_q"], "Occupancy by night of week for the selected window. Teal marks Friday and Saturday."))
        cc1, cc2 = st.columns([1.35, 1], gap="medium")
        with cc1:
            _chart(pc.day_of_week(m.dow), "pz_dow")
        with cc2:
            _md(ui.viz_foot(t["dow_a"], "Each bar is the share of rooms sold on that night across the whole window. "
                                        "Hover a bar for its ADR and RevPAR."))
    ask("snapshot", "Performance Snapshot", t["question"])


# ---------------------------------------------------------------------------
# 2. Visitors and spend
# ---------------------------------------------------------------------------

def render_visitors(m: ps.Model, ask, map_builder) -> None:
    t = m.text["visitors"]
    d, prof = m.dfy, m.profile or {}
    _md(ui.section_header("pulse-origins", 2, "Visitor economy · Datafy", "Visitors and Spend", t["question"]))
    tiles = []
    if d and d.get("spend") is not None:
        span = pdx.fmt_month(d["first"]) + ("" if d["n_months"] == 1 else f" to {pdx.fmt_month(d['last'])}")
        spark_vals = m.dfy_trend["spending_usd"].tolist() if m.dfy_trend is not None and not m.dfy_trend.empty else []
        tiles.append({"label": "Visitor spending", "value": ps.usd_big(d["spend"]), "spark": ui.sparkline(spark_vals),
                      "delta_html": ui.delta_chip(ps.spct(d.get("spend_pct")) + " vs last year" if d.get("spend_pct") is not None else None, d.get("spend_pct")),
                      "foot": span + (" (latest month on file)" if d.get("fallback") else "")})
        if d.get("visitor_days"):
            tiles.append({"label": "Visitor days", "value": ps.num_big(d["visitor_days"]),
                          "delta_html": ui.delta_chip(ps.spct(d.get("vd_pct")) + " vs last year" if d.get("vd_pct") is not None else None, d.get("vd_pct")),
                          "foot": "Days spent in Dana Point by visitors"})
    if m.markets is not None and not m.markets.empty:
        top_mkt = m.markets.iloc[0]
        tiles.append({"label": "Top origin market", "value": ps.clean_market(top_mkt["dma"]),
                      "delta_html": f'<span class="pk-delta flat">{ps.pct(top_mkt["share_pct"])} of visitor spending</span>',
                      "foot": ("Datafy, " + pdx.fmt_month(pd.Timestamp(top_mkt["report_period_start"])) + " to "
                               + pdx.fmt_month(pd.Timestamp(top_mkt["report_period_end"])))
                              if pd.notna(top_mkt.get("report_period_start")) and pd.notna(top_mkt.get("report_period_end"))
                              else "Datafy, latest published period"})
    if prof.get("out_of_state_pct") is not None:
        tiles.append({"label": "From out of state", "value": f"{prof['out_of_state_pct']:.0f}%",
                      "delta_html": '<span class="pk-delta flat">Share of visitor spending</span>',
                      "foot": (f"Average trip {prof['avg_los']:.1f} days" if prof.get("avg_los") else "Datafy")})
    if tiles:
        _md(ui.kpi_grid(tiles))
    _md(ui.answer(t["answer"]))

    with st.container(border=True):
        _md(ui.viz_head(t["spend_q"], "Visitor spending by month, this period against the same months last year. Source: Datafy."))
        _chart(pc.spend_trend(m.dfy_trend), "pz_spend_trend")
        _md(ui.viz_foot(t["spend_a"], "Teal bars are this period, sand bars the same months a year earlier. Datafy "
                                      "publishes monthly, so the most recent weeks of the window may not have a Datafy "
                                      "month yet; the months shown are the ones Datafy has reported."))

    mk = m.markets
    c1, c2 = st.columns([1.25, 1], gap="medium")
    with c1:
        with st.container(border=True):
            period = (f"Datafy, {pdx.fmt_month(mk.iloc[0]['report_period_start'])} to {pdx.fmt_month(mk.iloc[0]['report_period_end'])}"
                      if mk is not None and not mk.empty else "Datafy")
            _md(ui.viz_head(t["mkt_q"], f"Share of visitor spending by origin market. {period}, latest published period."))
            fig = map_builder(mk) if (map_builder is not None and mk is not None and not mk.empty) else None
            _chart(fig, "pz_map")
            _md(ui.viz_foot(t["mkt_a"], "Each bubble is an origin market sized by its share of visitor spending, and the "
                                        "line into Dana Point carries the same weight. Datafy updates this view when it "
                                        "publishes a new period, so it does not follow the data window."))
    with c2:
        with st.container(border=True):
            _md(ui.viz_head("Top origin markets", "Share of all visitor spending, latest Datafy period."))
            if mk is not None and not mk.empty:
                top = mk.head(8)
                _chart(pc.ranked_bars([ps.clean_market(x) for x in top["dma"]], top["share_pct"].tolist()), "pz_markets")
    c3, c4 = st.columns(2, gap="medium")
    with c3:
        with st.container(border=True):
            cats = m.categories
            _md(ui.viz_head(t["cat_q"], "Share of visitor spending by category, latest Datafy period."))
            if cats is not None and not cats.empty:
                top = cats.head(8)
                _chart(pc.ranked_bars(top["category"].tolist(), top["share_pct"].tolist()), "pz_categories")
            _md(ui.viz_foot(t["cat_a"], "Bars show each category's true share of all visitor spending, so they do not "
                                        "add to 100% when smaller categories are left off."))
    with c4:
        with st.container(border=True):
            ads = m.ads or {}
            _md(ui.viz_head(t["ads_q"], f"Datafy advertising export as of {pdx.fmt_date(ads['snapshot_date'])}." if ads.get("snapshot_date") else "Datafy advertising export."))
            if ads:
                _md(ui.kpi_grid([
                    {"label": "Impressions", "value": ps.num_big(ads.get("total_impressions")), "foot": f"{ads.get('total_clicks', 0):,.0f} clicks"},
                    {"label": "Media spend", "value": ps.usd(ads.get("total_spend_usd")), "foot": f"Cost per visitor day {ps.usd(ads.get('cost_per_visitor_day_usd'), 2)}"},
                    {"label": "Est. return", "value": f"{ps.usd(ads.get('est_roas'), 2)} per $1",
                     "foot": f"{ps.usd_big(ads.get('est_campaign_impact_usd'))} estimated visitor impact"},
                ]))
            _chart(pc.ad_vs_spend(m.ad_mix, ps.clean_market), "pz_ad_mix")
            _md(ui.viz_foot(t["ads_a"], "Teal is each market's share of all visitor spending (Datafy visitor economy); "
                                        "terracotta is its share of trips Datafy attributes to the paid campaign. Where the "
                                        "two diverge, media weight and visitor value are out of step."))
    ask("origins", "Visitors and Spend", t["question"])


# ---------------------------------------------------------------------------
# 3. Market position
# ---------------------------------------------------------------------------

def render_market(m: ps.Model, ask, sv, conn) -> None:
    t = m.text["market"]
    p, b = m.peers, m.bench
    _md(ui.section_header("pulse-market", 3, "Competitive position · CoStar and STR", "Market Position", t["question"]))
    tiles = []
    if p and p.get("dp"):
        tiles.append({"label": "Peer rank, RevPAR", "value": f"{p['rank_revpar']} of {p['n_markets']}",
                      "delta_html": f'<span class="pk-delta flat">Index {p["rgi"]:.0f} vs peer average</span>',
                      "foot": "Six coastal markets, STR weekly"})
        tiles.append({"label": "Peer rank, occupancy", "value": f"{p['rank_occ']} of {p['n_markets']}",
                      "delta_html": f'<span class="pk-delta flat">Index {p["mpi"]:.0f} vs peer average</span>',
                      "foot": f"Dana Point {ps.pct(p['dp']['occ'])}, peers {ps.pct(p['peer_avg']['occ'])}"})
    if b and b.get("comp"):
        tiles.append({"label": "YTD RevPAR vs submarket", "value": f"{b['rgi_sub'] - 100:+.0f}%",
                      "delta_html": ui.delta_chip(f"Dana Point {ps.usd(b['comp']['revpar'])}", b["rgi_sub"] - 100, tol=0.5),
                      "foot": f"Through {pdx.fmt_month(b['through'])}, CoStar"})
    sg = m.segments
    if sg:
        tot = sg["totals"].set_index("segment")["share"]
        if "Group" in tot:
            tiles.append({"label": "Group share of rooms", "value": f"{tot['Group']:.0f}%",
                          "delta_html": f'<span class="pk-delta flat">Transient {tot.get("Transient", 0):.0f}%</span>',
                          "foot": "CoStar segmentation, selected window" + (" (latest available)" if sg.get("fallback") else "")})
    if tiles:
        _md(ui.kpi_grid(tiles))
    _md(ui.answer(t["answer"]))

    c1, c2 = st.columns(2, gap="medium")
    with c1:
        with st.container(border=True):
            if p and not p["table"].empty:
                wk = p["weeks"]
                src = (f"STR weekly report, week ending {pdx.fmt_date(wk[-1])}." if (p["fallback"] or len(wk) == 1)
                       else f"STR weekly reports, {len(wk)} weeks from {pdx.fmt_date(wk[0])} to {pdx.fmt_date(wk[-1])}.")
                _md(ui.viz_head(t["peer_q"], src))
                tbl = p["table"].assign(is_dp=lambda d: d["market"] == "Dana Point")
                _chart(pc.place_compare(tbl, height=290), "pz_peers")
                _md(ui.viz_foot(t["peer_a"], "Terracotta is Dana Point; sand bars are the five peer markets in STR's weekly "
                                             "report. Each metric has its own scale. The index compares Dana Point with the "
                                             "simple average of the five peers, where 100 means at par."))
            else:
                _md(ui.viz_head(t["peer_q"]))
                st.info("STR weekly peer data is not available yet.")
    with c2:
        with st.container(border=True):
            if b and not b["table"].empty:
                _md(ui.viz_head(t["bench_q"], f"Year to date through {pdx.fmt_month(b['through'])}. CoStar market reports "
                                              f"dated {pdx.fmt_date(b['report_date'])}; Dana Point hotels computed for the same months."))
                _chart(pc.place_compare(b["table"], height=250), "pz_bench")
                _md(ui.viz_foot(t["bench_a"], "Dana Point hotels are the VDP Select comp set. The submarket is CoStar's "
                                              "Newport Beach/Dana Point hotel market, about 110 properties. Occupancy is "
                                              "rooms sold over rooms available for January through the latest closed month."))
            else:
                _md(ui.viz_head(t["bench_q"]))
                st.info("CoStar benchmark data is not available yet.")

    c3, c4 = st.columns(2, gap="medium")
    with c3:
        with st.container(border=True):
            _md(ui.viz_head(t["trend_q"], "RevPAR by calendar year, closed years plus the current year to date. CoStar."))
            _chart(pc.multiyear(m.multiyear), "pz_multiyear")
            _md(ui.viz_foot(t["trend_a"], "Each line is one geography's RevPAR by year. The last point is year to date, "
                                          "which runs higher in seasonal markets because it includes the summer peak but "
                                          "not the quieter fall and winter months."))
    with c4:
        with st.container(border=True):
            tiers_df = sv._costar_tiers(conn) if sv is not None else pd.DataFrame()
            _md(ui.viz_head(t["tier_q"], "Year-to-date RevPAR by chain-scale tier, Newport Beach/Dana Point submarket. CoStar."))
            _chart(pc.tiers(tiers_df), "pz_tiers")
            if tiers_df is not None and not tiers_df.empty:
                top = tiers_df.iloc[0]
                _md(ui.viz_foot(f"{top['report_scope']} hotels lead the submarket at {ps.usd(top['revpar_usd'])} RevPAR on "
                                f"{ps.pct(top['occupancy_pct'])} occupancy and {ps.usd(top['adr_usd'])} ADR.",
                                "Hover a bar for that tier's occupancy and ADR."))
            rs = sv._room_split(conn) if sv is not None else pd.DataFrame()
            if rs is not None and not rs.empty:
                _chart(pc.room_split(rs, height=230), "pz_rooms")
                r0 = rs.iloc[0]
                tier_total = sum(float(r0[c]) for c in ("luxury_upper_upscale_rooms", "upscale_upper_midscale_rooms",
                                                         "midscale_economy_rooms") if pd.notna(r0[c]))
                lux = r0["luxury_upper_upscale_rooms"] / tier_total * 100 if tier_total else None
                _md(ui.viz_foot(f"CoStar counts {int(r0['total_properties']):,} properties and about {int(r0['total_rooms']):,} rooms "
                                f"in the submarket. Luxury and upper upscale hotels hold {int(r0['luxury_upper_upscale_rooms']):,} "
                                f"rooms, {ps.pct(lux, 0)} of the rooms CoStar assigns to a tier. CoStar, {pdx.fmt_date(r0['report_date'])}."))

    with st.container(border=True):
        src = "CoStar Transient, Group, and Contract segmentation for the VDP Select comp set"
        if sg:
            src += f", {pdx.fmt_date(sg['start'])} to {pdx.fmt_date(sg['end'])}"
            if sg.get("fallback"):
                src += " (latest available, the segment export does not cover this window)"
        _md(ui.viz_head(t["mix_q"], src + "."))
        cc1, cc2 = st.columns([1.35, 1], gap="medium")
        with cc1:
            _chart(pc.segment_mix(sg), "pz_segments")
        with cc2:
            if sg:
                tot = sg["totals"].set_index("segment")["share"]
                _md(ui.mix_bar([(n, float(tot.get(n, 0)), c) for n, c in pc.SEG.items()]))
            _md(ui.viz_foot(t["mix_a"], "Each month's bar adds to 100% of rooms sold, split by how they were booked. "
                                        "Transient covers individual leisure and business travelers; group covers blocks "
                                        "of ten or more rooms; contract covers long-term negotiated business such as crews."))
    ask("market", "Market Position", t["question"])


# ---------------------------------------------------------------------------
# 4. Forward outlook
# ---------------------------------------------------------------------------

def render_forward(m: ps.Model, ask) -> None:
    t = m.text["forward"]
    _md(ui.section_header("pulse-forward", 4, "What is ahead · STR and events calendar", "Forward Outlook", t["question"]))
    tiles = []
    ev = m.events
    if ev is not None and not ev.empty:
        e0 = ev.iloc[0]
        tiles.append({"label": "Next major event", "value": str(e0["event_name"]),
                      "delta_html": f'<span class="pk-delta flat">{ui.esc(ps.days_phrase(int(e0["days_out"])).capitalize())}</span>',
                      "foot": (f"Same dates last year {ps.pct(e0['ly_occ'], 0)} occupancy" if pd.notna(e0.get("ly_occ")) else "")})
    cq = m.compression
    if cq is not None and not cq.empty:
        cur = cq.iloc[-1]
        tiles.append({"label": f"{cur['quarter']} nights at 80%+", "value": f"{int(cur['d80'])}",
                      "delta_html": f'<span class="pk-delta flat">{int(cur["d90"])} at 90%+</span>',
                      "foot": "Quarter in progress" if not cur["complete"] else "Full quarter"})
    mo = m.momentum
    if mo and mo.get("revpar_pct") is not None:
        tiles.append({"label": "Last 4 weeks RevPAR", "value": ps.usd(mo["revpar"]),
                      "delta_html": ui.delta_chip(ps.spct(mo["revpar_pct"]) + " vs last year", mo["revpar_pct"]),
                      "foot": f"{pdx.fmt_date(mo['start'], False)} to {pdx.fmt_date(mo['end'])}"})
    g = m.group
    if g and g.get("group_share") is not None:
        tiles.append({"label": "Group share, latest week", "value": f"{g['group_share']:.0f}%",
                      "delta_html": f'<span class="pk-delta flat">{ps.pct(g["total_occ"])} occupancy</span>',
                      "foot": f"STR week ending {pdx.fmt_date(g['week'])}"})
    if tiles:
        _md(ui.kpi_grid(tiles))
    _md(ui.answer(t["answer"]))

    c1, c2 = st.columns([1.1, 1], gap="medium")
    with c1:
        with st.container(border=True):
            _md(ui.viz_head(t["events_q"], "Visit Dana Point events calendar, next 120 days, with STR occupancy on the matching dates a year earlier."))
            _md(ui.event_list(ev, ps.days_phrase, ps.pct, ps.usd))
            _md(ui.viz_foot(t["events_a"], "Last year's figures cover the night before and the night of each event, shifted "
                                           "364 days so the weekday matches. High prior-year occupancy marks dates with pricing "
                                           "power; softer dates are where event marketing can add the most."))
    with c2:
        with st.container(border=True):
            _md(ui.viz_head(t["comp_q"], "Nights at 80% and 90% occupancy or higher, by quarter. An asterisk marks a quarter still in progress."))
            _chart(pc.compression(m.compression), "pz_compression")
            _md(ui.viz_foot(t["comp_a"], "Compression nights are nights when hotels are nearly full, the nights with the "
                                         "strongest pricing power. The narrow terracotta bar is the 90%-plus subset of the "
                                         "teal bar."))
        if g and g.get("parts") is not None:
            with st.container(border=True):
                _md(ui.viz_head(t["group_q"], f"STR weekly report, week ending {pdx.fmt_date(g['week'])}."))
                parts = [(r.segment, float(r.occ_pct), pc.SEG.get(r.segment, bt.SAND)) for r in g["parts"].itertuples()]
                _md(ui.mix_bar(parts))
                _md(ui.viz_foot(t["group_a"]))
    if m.insight:
        ins = m.insight
        _md(ui.answer(ins.get("body") or ins.get("headline"),
                      label=f"From the insights engine, {pdx.fmt_date(ins['as_of_date'])}"))
    ask("forward", "Forward Outlook", t["question"])


# ---------------------------------------------------------------------------
# About this data
# ---------------------------------------------------------------------------

def render_about(m: ps.Model) -> None:
    _md(ui.section_header("pulse-about", 5, "Methodology", "About This Data",
                          "Where does each number come from, and how do the sources connect?"))
    with st.expander("Sources, definitions, and how the data connects", expanded=False):
        _md(ui.node_view())
        f = m.fresh
        ag = f.get("agreement")
        _agree = (f"; on the {ag['n']} dates both feeds cover, occupancy matches to within 0.15 points on {ag['occ_ok']}"
                  if ag and ag.get("n") else "")
        _md(f"""
<div class="ab-grid" style="margin-top:12px">
 <div class="ab-card"><h5>Hotel performance</h5><p>STR's daily report for the VDP Select comp set of Dana Point hotels.
 Where STR's file ends ({pdx.fmt_date(f['str_through']) if f.get('str_through') is not None else 'n/a'}), CoStar's daily export for the
 same hotels continues it{_agree}. Occupancy is rooms sold divided by rooms
 available, ADR is room revenue divided by rooms sold, and RevPAR is room revenue divided by rooms available,
 so occupancy times ADR equals RevPAR.</p></div>
 <div class="ab-card"><h5>Year-over-year comparisons</h5><p>Every change compares the selected window with the same window 364 days
 earlier, which keeps Saturdays compared with Saturdays. Occupancy change is in percentage points; ADR, RevPAR,
 spending, and visitor days are in percent. When daily history does not reach a full year back, whole calendar
 months are compared instead, and totals are not compared if the comp set's room count changed.</p></div>
 <div class="ab-card"><h5>Market and peers</h5><p>CoStar's market reports supply year-to-date figures for the Newport
 Beach/Dana Point submarket, Orange County, and the United States, through the latest closed month. STR's weekly
 report supplies Dana Point against five coastal peers. Indices compare Dana Point with the peer average, where 100
 is par.</p></div>
 <div class="ab-card"><h5>Visitors</h5><p>Datafy estimates visitor spending and visitor days from card and location data,
 published monthly. Feeder markets, categories, and campaign results reflect Datafy's latest published period and do
 not move with the data window. Campaign returns are Datafy estimates, not audited results.</p></div>
 <div class="ab-card"><h5>How the written answers work</h5><p>The text under each section and chart is calculated from the
 same figures the chart shows, for the window you select, and reads across all three sources, so it always agrees
 with the page. The optional "Ask about this section" buttons send those figures to an AI model for a deeper read.</p></div>
 <div class="ab-card"><h5>Known limits</h5><ul>
 <li>Datafy is modeled data; month-to-month swings can reflect panel changes as well as real demand.</li>
 <li>The STR weekly peer report arrives irregularly, so the peer view averages the weeks on file inside the window.</li>
 <li>Relationships between sources (for example, hotel revenue and visitor spending) show that numbers move together, not
 that one causes the other.</li></ul></div>
</div>
""")
