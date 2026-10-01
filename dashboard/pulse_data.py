"""
pulse_data.py, the window-aware data layer behind the Dana Point PULSE board view.

Every number on the redesigned page comes from one of the loaders below, and
every loader answers for the SAME selected data window, so the KPI tiles, the
charts, the written summaries, and the AI context all agree with each other.

Sources, and the rule for each:
  * Hotels (STR). The "VDP Select" comp set. STR's daily export is primary;
    CoStar's daily HospitalityDataGrid export for the same comp set extends it
    forward past STR's newest date (identical on 733 of 738 shared dates).
    This mirrors scripts/compute_kpis.py exactly, so the PDF, the insights
    engine, and this page all read the same hotel history.
  * Market (CoStar). Submarket, Orange County, and U.S. year-to-date figures
    from CoStar's market report PDFs, plus the chain-scale tiers, room
    inventory, business mix, and participation roster.
  * Peers (STR weekly). Dana Point against the five coastal markets in the
    STR weekly report (Huntington Beach, La Jolla, Monterey-Carmel, Newport
    Beach, Santa Barbara).
  * Visitors (Datafy). Monthly visitor spend and visitor days, feeder markets,
    spend by category, and the separate paid-media advertising export.

Calculation standards (documented for the board in the About panel):
  * Occupancy = rooms sold / rooms available. ADR = room revenue / rooms sold.
    RevPAR = room revenue / rooms available. Rooms-weighted, the STR and
    CoStar convention, so Occupancy x ADR = RevPAR on screen.
  * Year over year compares the same window shifted back 364 days, which keeps
    the day-of-week mix aligned (a Saturday is compared with a Saturday).
  * Occupancy change is reported in percentage points; ADR, RevPAR, spend, and
    visitor days in percent.

No function here writes to the database.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd
import streamlit as st

_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
_MONTH_NUM = {m: i + 1 for i, m in enumerate(_MONTHS)}
_DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

PEER_MARKETS = ["Dana Point", "Newport Beach", "La Jolla", "Santa Barbara",
                "Huntington Beach", "Monterey-Carmel"]


def _q(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


def _safe_div(a, b):
    try:
        if b is None or a is None or pd.isna(a) or pd.isna(b) or float(b) == 0:
            return None
        return float(a) / float(b)
    except Exception:
        return None


def _pct_change(cur, prior):
    if cur is None or prior is None or pd.isna(cur) or pd.isna(prior) or prior == 0:
        return None
    return (float(cur) / float(prior) - 1.0) * 100.0


# ---------------------------------------------------------------------------
# The data window
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Window:
    start: pd.Timestamp
    end: pd.Timestamp
    label: str
    custom: bool = False

    @property
    def days(self) -> int:
        return int((self.end - self.start).days) + 1

    @property
    def prior(self) -> "Window":
        return Window(self.start - pd.Timedelta(days=364), self.end - pd.Timedelta(days=364),
                      f"same period last year", self.custom)

    def span_text(self) -> str:
        return f"{fmt_date(self.start)} to {fmt_date(self.end)}"


def fmt_date(d, with_year: bool = True) -> str:
    d = pd.Timestamp(d)
    return d.strftime("%b %-d, %Y") if with_year else d.strftime("%b %-d")


def fmt_month(d) -> str:
    return pd.Timestamp(d).strftime("%b %Y")


def resolve_window(anchor_end, months: float, start_iso: str | None, end_iso: str | None,
                   label: str) -> Window:
    """Presets end on the newest hotel date on file and run back
    round(30 x months) days, inclusive, so "Last 12 months" is 360 days and
    "This week" is 7 days. A custom range is used exactly as entered."""
    if start_iso and end_iso:
        s, e = pd.Timestamp(start_iso), pd.Timestamp(end_iso)
        if s > e:
            s, e = e, s
        return Window(s.normalize(), e.normalize(), label, True)
    end = pd.Timestamp(anchor_end).normalize()
    days = max(int(round(30 * months)), 1)
    return Window(end - pd.Timedelta(days=days - 1), end, label, False)


# ---------------------------------------------------------------------------
# Hotels: STR comp set, CoStar-extended
# ---------------------------------------------------------------------------

@st.cache_data(ttl=1800, show_spinner=False)
def hotel_daily(_conn) -> pd.DataFrame:
    str_df = _q(_conn, """
        SELECT as_of_date,
               MAX(CASE WHEN metric_name='supply'  THEN metric_value END) AS supply,
               MAX(CASE WHEN metric_name='demand'  THEN metric_value END) AS demand,
               MAX(CASE WHEN metric_name='revenue' THEN metric_value END) AS revenue,
               MAX(CASE WHEN metric_name='occ'     THEN metric_value END) * 100 AS occ_pct,
               MAX(CASE WHEN metric_name='adr'     THEN metric_value END) AS adr,
               MAX(CASE WHEN metric_name='revpar'  THEN metric_value END) AS revpar
        FROM fact_str_metrics
        WHERE grain='daily' AND source='STR'
        GROUP BY as_of_date
    """)
    if not str_df.empty:
        str_df["source"] = "STR"
    cs = _q(_conn, """
        SELECT as_of_date, supply, demand, revenue_usd AS revenue,
               occupancy_pct AS occ_pct, adr_usd AS adr, revpar_usd AS revpar
        FROM costar_market_daily WHERE occupancy_pct IS NOT NULL
    """)
    if not cs.empty:
        cs["source"] = "CoStar"
        if not str_df.empty:
            cs = cs[cs["as_of_date"] > str_df["as_of_date"].max()]
    df = pd.concat([d for d in (str_df, cs) if not d.empty], ignore_index=True) if (
        not str_df.empty or not cs.empty) else pd.DataFrame()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="first")
    for c in ("supply", "demand", "revenue", "occ_pct", "adr", "revpar"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # Fill any missing ratio from the counts, never the other way round.
    df["occ_pct"] = df["occ_pct"].fillna(df["demand"] / df["supply"] * 100)
    df["dow"] = df["date"].dt.dayofweek
    return df.reset_index(drop=True)


@st.cache_data(ttl=1800, show_spinner=False)
def hotel_monthly(_conn) -> pd.DataFrame:
    """Month-level supply/demand/revenue: STR's monthly export first, CoStar's
    monthly grid for later months, and daily data rolled up for any month the
    daily frame covers in full (so a month is never missing just because the
    monthly export lags)."""
    m = _q(_conn, """
        SELECT as_of_date,
               MAX(CASE WHEN metric_name='supply'  THEN metric_value END) AS supply,
               MAX(CASE WHEN metric_name='demand'  THEN metric_value END) AS demand,
               MAX(CASE WHEN metric_name='revenue' THEN metric_value END) AS revenue
        FROM fact_str_metrics WHERE grain='monthly' AND source='STR'
        GROUP BY as_of_date
    """)
    rows = []
    if not m.empty:
        m["period"] = pd.to_datetime(m["as_of_date"]).dt.to_period("M").dt.to_timestamp()
        rows.append(m[["period", "supply", "demand", "revenue"]].assign(source="STR"))
    cm = _q(_conn, """
        SELECT report_period, supply, demand, revenue_usd AS revenue
        FROM costar_market_monthly WHERE supply IS NOT NULL AND demand IS NOT NULL
    """)
    if not cm.empty:
        cm["period"] = pd.to_datetime(cm["report_period"] + "-01", errors="coerce")
        cm = cm.dropna(subset=["period"])
        have = set(rows[0]["period"]) if rows else set()
        cm = cm[~cm["period"].isin(have)]
        rows.append(cm[["period", "supply", "demand", "revenue"]].assign(source="CoStar"))
    d = hotel_daily(_conn)
    if not d.empty:
        dm = d.assign(period=d["date"].dt.to_period("M").dt.to_timestamp())
        g = dm.groupby("period").agg(supply=("supply", "sum"), demand=("demand", "sum"),
                                      revenue=("revenue", "sum"), n=("date", "count"))
        g["days_in_month"] = [pd.Timestamp(p).days_in_month for p in g.index]
        full = g[g["n"] >= g["days_in_month"]].reset_index()
        if not full.empty:
            existing = set(pd.concat(rows)["period"]) if rows else set()
            full = full[~full["period"].isin(existing)]
            rows.append(full[["period", "supply", "demand", "revenue"]].assign(source="Daily"))
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    for c in ("supply", "demand", "revenue"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["supply", "demand", "revenue"])
    out = out[(out["supply"] > 0)].sort_values("period").drop_duplicates("period")
    out["occ_pct"] = out["demand"] / out["supply"] * 100
    out["adr"] = out["revenue"] / out["demand"]
    out["revpar"] = out["revenue"] / out["supply"]
    return out.reset_index(drop=True)


def agg_hotel(df: pd.DataFrame) -> dict | None:
    if df is None or df.empty:
        return None
    s, dmd, rev = df["supply"].sum(), df["demand"].sum(), df["revenue"].sum()
    if not s or not dmd:
        return None
    out = {
        "occ": dmd / s * 100,
        "adr": rev / dmd,
        "revpar": rev / s,
        "supply": s, "demand": dmd, "revenue": rev,
        "n": int(len(df)),
    }
    if "occ_pct" in df:
        out["d80"] = int((df["occ_pct"] >= 80).sum())
        out["d90"] = int((df["occ_pct"] >= 90).sum())
    return out


def _slice(df: pd.DataFrame, w: Window) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    return df[(df["date"] >= w.start) & (df["date"] <= w.end)]


def hotel_window(daily: pd.DataFrame, monthly: pd.DataFrame, w: Window) -> dict | None:
    """Current window versus the same window a year earlier, with the
    method recorded so the page can say how the comparison was made."""
    cur_df = _slice(daily, w)
    cur = agg_hotel(cur_df)
    if cur is None:
        return None
    pw = w.prior
    prior_df = _slice(daily, pw)
    method, prior, cmp_cur = "daily", None, cur
    if len(prior_df) >= 0.9 * len(cur_df):
        prior = agg_hotel(prior_df)
    elif monthly is not None and not monthly.empty:
        # Like-for-like fallback when the daily history does not reach back a
        # full year before the window: whole calendar months on both sides.
        # The headline stays on the exact window; only the change uses months.
        months = list(pd.period_range(w.start, w.end, freq="M").to_timestamp())
        cur_m = monthly[monthly["period"].isin(months)]
        prior_m = monthly[monthly["period"].isin([m - pd.DateOffset(years=1) for m in months])]
        if len(months) and len(prior_m) >= 0.9 * len(months) and len(cur_m) >= 0.9 * len(months):
            method = "monthly"
            cmp_cur = agg_hotel(cur_m)
            prior = agg_hotel(prior_m)
    res = dict(cur)
    res["start"], res["end"] = cur_df["date"].min(), cur_df["date"].max()
    res["sources"] = sorted(set(cur_df["source"])) if "source" in cur_df else []
    res["costar_days"] = int((cur_df["source"] == "CoStar").sum()) if "source" in cur_df else 0
    res["method"] = method if prior else None
    res["prior"] = prior
    if prior and cmp_cur:
        c = cmp_cur
        res["occ_pts"] = c["occ"] - prior["occ"]
        res["adr_pct"] = _pct_change(c["adr"], prior["adr"])
        res["revpar_pct"] = _pct_change(c["revpar"], prior["revpar"])
        res["revenue_pct"] = _pct_change(c["revenue"], prior["revenue"])
        res["demand_pct"] = _pct_change(c["demand"], prior["demand"])
        # A comp set whose room count moved more than 2% is not like-for-like
        # on totals (revenue, rooms sold); the ratios above still are.
        res["supply_pct"] = _pct_change(c["supply"], prior["supply"])
        res["totals_comparable"] = res["supply_pct"] is not None and abs(res["supply_pct"]) <= 2.0
        # Log decomposition of the RevPAR change into its occupancy and rate
        # parts, so the two shares always add back to the RevPAR change.
        try:
            lr = math.log(c["revpar"] / prior["revpar"])
            lo = math.log(c["occ"] / prior["occ"])
            if abs(lr) > 1e-9:
                share_occ = lo / lr
                res["rev_from_occ_pts"] = res["revpar_pct"] * share_occ
                res["rev_from_adr_pts"] = res["revpar_pct"] * (1 - share_occ)
        except (ValueError, ZeroDivisionError, TypeError):
            pass
    return res


def hotel_trend(daily: pd.DataFrame, monthly: pd.DataFrame, w: Window) -> pd.DataFrame:
    """Current series plus the prior-year series on the same x positions.
    Daily points for windows of 45 days or less, months otherwise."""
    cur = _slice(daily, w)
    if cur.empty:
        return pd.DataFrame()
    if w.days <= 45:
        prior = _slice(daily, w.prior)[["date", "occ_pct", "adr", "revpar"]].copy()
        prior["date"] = prior["date"] + pd.Timedelta(days=364)
        out = cur[["date", "occ_pct", "adr", "revpar"]].merge(
            prior.rename(columns={"occ_pct": "occ_ly", "adr": "adr_ly", "revpar": "revpar_ly"}),
            on="date", how="left")
        out = out.rename(columns={"occ_pct": "occ"})
        out["label"] = out["date"].dt.strftime("%b %-d")
        out["grain"] = "day"
        return out
    c = cur.assign(period=cur["date"].dt.to_period("M").dt.to_timestamp())
    g = c.groupby("period").agg(supply=("supply", "sum"), demand=("demand", "sum"),
                                revenue=("revenue", "sum"), n=("date", "count")).reset_index()
    g["occ"] = g["demand"] / g["supply"] * 100
    g["adr"] = g["revenue"] / g["demand"]
    g["revpar"] = g["revenue"] / g["supply"]
    # Prior-year months: the same calendar days a year earlier where daily
    # coverage exists, otherwise the whole month from the monthly table.
    ly_rows = []
    for row in g.itertuples():
        p_start = max(row.period, w.start) - pd.DateOffset(years=1)
        p_end = min(row.period + pd.offsets.MonthEnd(0), w.end) - pd.DateOffset(years=1)
        sl = daily[(daily["date"] >= p_start) & (daily["date"] <= p_end)] if not daily.empty else pd.DataFrame()
        a = agg_hotel(sl) if len(sl) >= 0.9 * row.n else None
        if a is None and monthly is not None and not monthly.empty:
            mm = monthly[monthly["period"] == row.period - pd.DateOffset(years=1)]
            a = agg_hotel(mm) if not mm.empty else None
        ly_rows.append(((a or {}).get("occ"), (a or {}).get("adr"), (a or {}).get("revpar")))
    g["occ_ly"] = [r[0] for r in ly_rows]
    g["adr_ly"] = [r[1] for r in ly_rows]
    g["revpar_ly"] = [r[2] for r in ly_rows]
    g["date"] = g["period"]
    g["label"] = g["period"].dt.strftime("%b %Y")
    g["partial"] = g["n"] < [pd.Timestamp(p).days_in_month for p in g["period"]]
    g["grain"] = "month"
    return g


def day_of_week(daily: pd.DataFrame, w: Window) -> pd.DataFrame:
    cur = _slice(daily, w)
    if cur.empty:
        return pd.DataFrame()
    g = cur.groupby("dow").agg(supply=("supply", "sum"), demand=("demand", "sum"),
                               revenue=("revenue", "sum"), n=("date", "count")).reset_index()
    g["occ"] = g["demand"] / g["supply"] * 100
    g["adr"] = g["revenue"] / g["demand"]
    g["revpar"] = g["revenue"] / g["supply"]
    g["label"] = g["dow"].map(lambda i: _DOW[int(i)])
    return g.sort_values("dow").reset_index(drop=True)


def weekend_gap(dow: pd.DataFrame) -> dict | None:
    if dow is None or dow.empty:
        return None
    wk = dow[dow["dow"].isin([4, 5])]      # Fri, Sat nights
    md = dow[~dow["dow"].isin([4, 5])]     # Sun through Thu
    a, b = agg_hotel(wk), agg_hotel(md)
    if not a or not b:
        return None
    return {"weekend_occ": a["occ"], "midweek_occ": b["occ"], "weekend_adr": a["adr"],
            "midweek_adr": b["adr"], "occ_gap": a["occ"] - b["occ"],
            "adr_premium_pct": _pct_change(a["adr"], b["adr"]),
            "best": dow.loc[dow["occ"].idxmax(), "label"], "worst": dow.loc[dow["occ"].idxmin(), "label"],
            "best_occ": float(dow["occ"].max()), "worst_occ": float(dow["occ"].min())}


def compression_by_quarter(daily: pd.DataFrame, quarters: int = 8) -> pd.DataFrame:
    if daily is None or daily.empty:
        return pd.DataFrame()
    d = daily.assign(q=daily["date"].dt.to_period("Q"))
    g = d.groupby("q").agg(d80=("occ_pct", lambda s: int((s >= 80).sum())),
                           d90=("occ_pct", lambda s: int((s >= 90).sum())),
                           n=("date", "count")).reset_index()
    g["quarter"] = g["q"].astype(str).str.replace("Q", "-Q", regex=False)
    g["complete"] = g["n"] >= 89
    return g.tail(quarters).reset_index(drop=True)


def momentum(daily: pd.DataFrame, days: int = 28) -> dict | None:
    """The most recent four weeks against the same four weeks last year, the
    freshest read on direction regardless of the window selected."""
    if daily is None or daily.empty:
        return None
    end = daily["date"].max()
    w = Window(end - pd.Timedelta(days=days - 1), end, f"last {days} days")
    return hotel_window(daily, None, w)


# ---------------------------------------------------------------------------
# Peers: STR weekly six-market coastal comp set
# ---------------------------------------------------------------------------

@st.cache_data(ttl=1800, show_spinner=False)
def peer_weekly(_conn) -> pd.DataFrame:
    df = _q(_conn, """
        SELECT as_of_date, market, metric_name, data_period, metric_value
        FROM fact_str_group_metrics
        WHERE grain='weekly' AND segment='Total'
          AND metric_name IN ('supply','demand','revenue')
          AND data_period IN ('current','prior_year')
    """)
    if df.empty:
        return df
    wide = df.pivot_table(index=["as_of_date", "market"], columns=["metric_name", "data_period"],
                          values="metric_value", aggfunc="first")
    wide.columns = [f"{m}_{p}" for m, p in wide.columns]
    wide = wide.reset_index()
    wide["date"] = pd.to_datetime(wide["as_of_date"])
    return wide


def peer_window(peers: pd.DataFrame, w: Window) -> dict | None:
    """Aggregates every STR weekly report whose week ends inside the window.
    Falls back to the newest report on file, flagged, when none do."""
    if peers is None or peers.empty:
        return None
    sel = peers[(peers["date"] >= w.start) & (peers["date"] <= w.end)]
    fallback = False
    if sel.empty:
        sel = peers[peers["date"] == peers["date"].max()]
        fallback = True
    rows = []
    for mkt, g in sel.groupby("market"):
        s, d, r = g.get("supply_current"), g.get("demand_current"), g.get("revenue_current")
        if s is None or d is None or r is None:
            continue
        s, d, r = s.sum(), d.sum(), r.sum()
        sp, dp, rp = (g.get("supply_prior_year", pd.Series(dtype=float)).sum(),
                      g.get("demand_prior_year", pd.Series(dtype=float)).sum(),
                      g.get("revenue_prior_year", pd.Series(dtype=float)).sum())
        if not s or not d:
            continue
        row = {"market": mkt, "occ": d / s * 100, "adr": r / d, "revpar": r / s}
        if sp and dp and rp:
            row["revpar_pct"] = _pct_change(r / s, rp / sp)
            row["occ_pts"] = d / s * 100 - dp / sp * 100
            row["adr_pct"] = _pct_change(r / d, rp / dp)
        rows.append(row)
    if not rows:
        return None
    t = pd.DataFrame(rows)
    dp_row = t[t["market"] == "Dana Point"]
    others = t[t["market"] != "Dana Point"]
    out = {"table": t.sort_values("revpar", ascending=False).reset_index(drop=True),
           "weeks": sorted(sel["date"].dt.date.unique()), "fallback": fallback}
    if not dp_row.empty and not others.empty:
        dpv = dp_row.iloc[0]
        out["dp"] = dpv.to_dict()
        out["peer_avg"] = {k: float(others[k].mean()) for k in ("occ", "adr", "revpar")}
        out["rank_revpar"] = int((t["revpar"] > dpv["revpar"]).sum()) + 1
        out["rank_adr"] = int((t["adr"] > dpv["adr"]).sum()) + 1
        out["rank_occ"] = int((t["occ"] > dpv["occ"]).sum()) + 1
        out["n_markets"] = int(len(t))
        # STR-style indices against the peer average (100 = at par).
        out["mpi"] = dpv["occ"] / out["peer_avg"]["occ"] * 100
        out["ari"] = dpv["adr"] / out["peer_avg"]["adr"] * 100
        out["rgi"] = dpv["revpar"] / out["peer_avg"]["revpar"] * 100
    return out


# ---------------------------------------------------------------------------
# Market: CoStar submarket, Orange County, United States
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600, show_spinner=False)
def costar_benchmarks(_conn) -> pd.DataFrame:
    """Overall-scope YTD for each geography from the newest CoStar report."""
    return _q(_conn, """
        SELECT market, occupancy_pct, occ_yoy_pct, adr_usd, adr_yoy_pct,
               revpar_usd, revpar_yoy_pct, report_date
        FROM costar_annual_performance a
        WHERE report_scope='Overall' AND year_label='YTD'
          AND report_date = (SELECT MAX(report_date) FROM costar_annual_performance b
                             WHERE b.market = a.market AND b.report_scope='Overall'
                               AND b.year_label='YTD')
    """)


@st.cache_data(ttl=3600, show_spinner=False)
def costar_ytd_through(_conn) -> pd.Timestamp | None:
    """CoStar's market reports run year-to-date through the last closed
    month, which is the newest month in its monthly grid for the same pull."""
    df = _q(_conn, "SELECT MAX(report_period) AS p FROM costar_market_monthly WHERE supply IS NOT NULL")
    if df.empty or not df.iloc[0]["p"]:
        return None
    return pd.Timestamp(df.iloc[0]["p"] + "-01") + pd.offsets.MonthEnd(0)


def benchmark_ytd(conn, daily: pd.DataFrame) -> dict | None:
    bm = costar_benchmarks(conn)
    through = costar_ytd_through(conn)
    if bm.empty or through is None:
        return None
    ytd = Window(pd.Timestamp(year=through.year, month=1, day=1), through, "YTD")
    comp = hotel_window(daily, None, ytd)
    rows = []
    if comp:
        rows.append({"market": "Dana Point hotels", "occ": comp["occ"], "adr": comp["adr"],
                     "revpar": comp["revpar"], "revpar_pct": comp.get("revpar_pct"),
                     "occ_chg": comp.get("occ_pts"), "adr_pct": comp.get("adr_pct"), "is_dp": True})
    order = {"Newport Beach/Dana Point": 1, "Orange County CA": 2, "United States": 3}
    for r in bm.itertuples():
        rows.append({"market": {"Orange County CA": "Orange County"}.get(r.market, r.market),
                     "occ": r.occupancy_pct, "adr": r.adr_usd, "revpar": r.revpar_usd,
                     "revpar_pct": r.revpar_yoy_pct, "occ_chg": r.occ_yoy_pct,
                     "adr_pct": r.adr_yoy_pct, "is_dp": False, "_o": order.get(r.market, 9)})
    t = pd.DataFrame(rows)
    t["_o"] = t.get("_o", pd.Series(dtype=float)).fillna(0)
    t = t.sort_values("_o").drop(columns="_o").reset_index(drop=True)
    sub = t[t["market"] == "Newport Beach/Dana Point"]
    out = {"table": t, "through": through, "report_date": bm["report_date"].max(), "comp": comp}
    if comp and not sub.empty:
        s = sub.iloc[0]
        out["rgi_sub"] = comp["revpar"] / s["revpar"] * 100
        out["ari_sub"] = comp["adr"] / s["adr"] * 100
        out["mpi_sub"] = comp["occ"] / s["occ"] * 100
    return out


@st.cache_data(ttl=3600, show_spinner=False)
def costar_multiyear(_conn) -> pd.DataFrame:
    df = _q(_conn, """
        SELECT market, year_label, revpar_usd, occupancy_pct, adr_usd
        FROM costar_annual_performance a
        WHERE report_scope='Overall'
          AND report_date = (SELECT MAX(report_date) FROM costar_annual_performance b
                             WHERE b.market = a.market AND b.report_scope='Overall'
                               AND b.year_label='YTD')
    """)
    if df.empty:
        return df
    cy = pd.Timestamp.now().year
    hist = df[df["year_label"].str.fullmatch(r"\d{4}")].copy()
    hist = hist[hist["year_label"].astype(int) < cy]
    ytd = df[df["year_label"] == "YTD"].copy()
    ytd["year_label"] = f"YTD {cy}"
    out = pd.concat([hist, ytd], ignore_index=True)
    out["market"] = out["market"].replace({"Orange County CA": "Orange County"})
    return out


def costar_segments(conn, w: Window) -> dict | None:
    df = _q(conn, """
        SELECT as_of_date, segment, demand, revenue_usd FROM costar_market_daily_segment
        WHERE as_of_date BETWEEN ? AND ?
    """, (w.start.strftime("%Y-%m-%d"), w.end.strftime("%Y-%m-%d")))
    fallback = False
    if df.empty:
        latest = _q(conn, "SELECT MAX(as_of_date) AS d FROM costar_market_daily_segment")
        if latest.empty or not latest.iloc[0]["d"]:
            return None
        end = pd.Timestamp(latest.iloc[0]["d"])
        df = _q(conn, """
            SELECT as_of_date, segment, demand, revenue_usd FROM costar_market_daily_segment
            WHERE as_of_date BETWEEN ? AND ?
        """, ((end - pd.Timedelta(days=w.days - 1)).strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
        fallback = True
    if df.empty:
        return None
    df["demand"] = pd.to_numeric(df["demand"], errors="coerce").fillna(0)
    df["revenue_usd"] = pd.to_numeric(df.get("revenue_usd"), errors="coerce")
    df["month"] = pd.to_datetime(df["as_of_date"]).dt.to_period("M").dt.to_timestamp()
    tot = df.groupby("segment").agg(demand=("demand", "sum"), revenue=("revenue_usd", "sum")).reset_index()
    total = tot["demand"].sum()
    if not total:
        return None
    tot["share"] = tot["demand"] / total * 100
    tot["adr"] = tot.apply(lambda r: _safe_div(r["revenue"], r["demand"]), axis=1)
    monthly = df.groupby(["month", "segment"]).agg(demand=("demand", "sum")).reset_index()
    mtot = monthly.groupby("month")["demand"].transform("sum")
    monthly["share"] = monthly["demand"] / mtot * 100
    return {"totals": tot.sort_values("share", ascending=False).reset_index(drop=True),
            "monthly": monthly, "fallback": fallback,
            "start": pd.to_datetime(df["as_of_date"]).min(), "end": pd.to_datetime(df["as_of_date"]).max()}


# ---------------------------------------------------------------------------
# Visitors: Datafy
# ---------------------------------------------------------------------------

def _parse_month(month_val, year_val) -> pd.Timestamp | None:
    m = str(month_val or "").strip()
    parts = m.split()
    try:
        if len(parts) == 2 and parts[0][:3] in _MONTH_NUM:
            return pd.Timestamp(year=int(parts[1]), month=_MONTH_NUM[parts[0][:3]], day=1)
        if parts and parts[0][:3] in _MONTH_NUM and year_val not in (None, "") and not pd.isna(year_val):
            return pd.Timestamp(year=int(float(year_val)), month=_MONTH_NUM[parts[0][:3]], day=1)
    except (ValueError, TypeError):
        return None
    return None


@st.cache_data(ttl=1800, show_spinner=False)
def datafy_monthly(_conn) -> pd.DataFrame:
    """Monthly visitor spend and visitor days. Datafy's two monthly exports
    store the month two different ways ("Mar 2021" in one, "Mar" plus a year
    column in the other); both are parsed here. Zero rows are Datafy's
    placeholder for months not yet reported and are dropped, and any month
    past Datafy's newest reported month is treated as partial and excluded."""
    sp = _q(_conn, "SELECT id, year, month, spending_usd FROM datafy_overview_spending_by_month")
    vd = _q(_conn, "SELECT id, year, month, visitor_days FROM datafy_overview_visitation_by_month")
    frames = []
    for df, col in ((sp, "spending_usd"), (vd, "visitor_days")):
        if df.empty:
            continue
        df = df.copy()
        df["period"] = [_parse_month(m, y) for m, y in zip(df["month"], df["year"])]
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["period"])
        df = df[df[col] > 0].sort_values("id").drop_duplicates("period", keep="last")
        frames.append(df[["period", col]].set_index("period"))
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, axis=1).reset_index().sort_values("period")
    end = _q(_conn, "SELECT MAX(report_period_end) AS e FROM datafy_overview_spending_by_market")
    if not end.empty and end.iloc[0]["e"]:
        cutoff = pd.Timestamp(end.iloc[0]["e"])
        out = out[out["period"] <= cutoff]
    if "spending_usd" in out:
        last_spend = out.dropna(subset=["spending_usd"])["period"].max()
        if pd.notna(last_spend):
            out = out[out["period"] <= last_spend]
    out["spend_per_vd"] = out.apply(
        lambda r: _safe_div(r.get("spending_usd"), r.get("visitor_days")), axis=1)
    return out.reset_index(drop=True)


def datafy_window(dm: pd.DataFrame, w: Window) -> dict | None:
    if dm is None or dm.empty:
        return None
    months = dm[(dm["period"] <= w.end) & ((dm["period"] + pd.offsets.MonthEnd(0)) >= w.start)]
    fallback = False
    if months.empty:
        months = dm.tail(1)
        fallback = True
    prior_periods = [p - pd.DateOffset(years=1) for p in months["period"]]
    prior = dm[dm["period"].isin(prior_periods)]
    spend = months["spending_usd"].sum(min_count=1) if "spending_usd" in months else None
    vdays = months["visitor_days"].sum(min_count=1) if "visitor_days" in months else None
    out = {"months": months, "first": months["period"].min(), "last": months["period"].max(),
           "n_months": int(len(months)), "fallback": fallback, "spend": spend, "visitor_days": vdays,
           "spend_per_vd": _safe_div(spend, vdays)}
    if len(prior) == len(months):
        ps = prior["spending_usd"].sum(min_count=1) if "spending_usd" in prior else None
        pv = prior["visitor_days"].sum(min_count=1) if "visitor_days" in prior else None
        out["spend_pct"] = _pct_change(spend, ps)
        out["vd_pct"] = _pct_change(vdays, pv)
        out["spvd_pct"] = _pct_change(_safe_div(spend, vdays), _safe_div(ps, pv))
        out["prior_spend"], out["prior_vd"] = ps, pv
    # Peak month inside the window.
    if spend and "spending_usd" in months:
        pk = months.loc[months["spending_usd"].idxmax()]
        out["peak_month"], out["peak_spend"] = pk["period"], pk["spending_usd"]
    return out


def datafy_trend(dm: pd.DataFrame, w: Window) -> pd.DataFrame:
    if dm is None or dm.empty:
        return pd.DataFrame()
    months = dm[(dm["period"] <= w.end) & ((dm["period"] + pd.offsets.MonthEnd(0)) >= w.start)]
    if months.empty:
        months = dm.tail(12)
    ly = dm.set_index("period")
    out = months.copy()
    out["spend_ly"] = [ly["spending_usd"].get(p - pd.DateOffset(years=1)) if "spending_usd" in ly else None
                       for p in out["period"]]
    out["vd_ly"] = [ly["visitor_days"].get(p - pd.DateOffset(years=1)) if "visitor_days" in ly else None
                    for p in out["period"]]
    out["label"] = out["period"].dt.strftime("%b %Y")
    return out.reset_index(drop=True)


def hotel_vs_visitor_spend(monthly_hotel: pd.DataFrame, dm: pd.DataFrame) -> dict | None:
    """How closely hotel room revenue and destination-wide visitor spend move
    together, month by month, across every month both sources cover."""
    if monthly_hotel is None or monthly_hotel.empty or dm is None or dm.empty:
        return None
    j = monthly_hotel[["period", "revenue", "occ_pct"]].merge(
        dm[["period", "spending_usd"]].dropna(), on="period", how="inner")
    if len(j) < 12:
        return None
    r = j["revenue"].corr(j["spending_usd"])
    r_occ = j["occ_pct"].corr(j["spending_usd"])
    return {"r": float(r), "r_occ": float(r_occ), "n": int(len(j)),
            "first": j["period"].min(), "last": j["period"].max()}


@st.cache_data(ttl=1800, show_spinner=False)
def datafy_markets(_conn, limit: int = 10) -> pd.DataFrame:
    df = _q(_conn, """
        SELECT dma, spend_share_pct * 100 AS share_pct, report_period_start, report_period_end
        FROM datafy_overview_spending_by_market
        WHERE report_period_start = (SELECT MAX(report_period_start) FROM datafy_overview_spending_by_market)
        ORDER BY spend_share_pct DESC LIMIT ?
    """, (limit,))
    if not df.empty:
        df["metric"] = "Share of visitor spend"
    return df


@st.cache_data(ttl=1800, show_spinner=False)
def datafy_categories(_conn) -> pd.DataFrame:
    df = _q(_conn, """
        SELECT category, spend_share_pct * 100 AS share_pct, avg_spend_usd,
               report_period_start, report_period_end
        FROM datafy_overview_spending_by_category
        WHERE report_period_start = (SELECT MAX(report_period_start) FROM datafy_overview_spending_by_category)
        ORDER BY spend_share_pct DESC
    """)
    return df


@st.cache_data(ttl=1800, show_spinner=False)
def datafy_profile(_conn) -> dict:
    """Latest-period visitor profile, plus a flag when a 'latest period'
    value is byte-for-byte the same as the prior full year, which in this
    data means the same export was loaded under two period labels."""
    out: dict = {}
    k = _q(_conn, "SELECT report_period_start, report_period_end, total_trips, avg_los_days "
                  "FROM datafy_overview_total_kpis ORDER BY report_period_start DESC")
    if not k.empty:
        out["trips"] = k.iloc[0]["total_trips"]
        out["avg_los"] = k.iloc[0]["avg_los_days"]
        out["period_start"], out["period_end"] = k.iloc[0]["report_period_start"], k.iloc[0]["report_period_end"]
    io = _q(_conn, "SELECT report_period_start, in_state_pct, out_of_state_pct "
                   "FROM datafy_overview_instate_outstate ORDER BY report_period_start DESC")
    if not io.empty:
        out["out_of_state_pct"] = float(io.iloc[0]["out_of_state_pct"]) * 100
        out["oos_repeats_prior"] = bool(len(io) > 1 and
                                        abs(io.iloc[0]["out_of_state_pct"] - io.iloc[1]["out_of_state_pct"]) < 1e-9)
    return out


@st.cache_data(ttl=1800, show_spinner=False)
def advertising(_conn) -> dict | None:
    k = _q(_conn, "SELECT * FROM datafy_advertising_kpis ORDER BY snapshot_date DESC LIMIT 1")
    o = _q(_conn, "SELECT * FROM datafy_advertising_overview ORDER BY snapshot_date DESC LIMIT 1")
    if k.empty and o.empty:
        return None
    out: dict = {}
    if not k.empty:
        out.update(k.iloc[0].to_dict())
    if not o.empty:
        out.update({c: o.iloc[0][c] for c in o.columns if c not in ("id", "loaded_at")})
    snap = out.get("snapshot_date")
    mk = _q(_conn, "SELECT dma, trip_share_pct, trips, est_impact_usd, spend_per_visitor_usd "
                   "FROM datafy_advertising_top_markets WHERE snapshot_date = ? "
                   "ORDER BY trip_share_pct DESC", (snap,))
    tc = _q(_conn, "SELECT tactic, attribution_rate_pct FROM datafy_advertising_tactic_performance "
                   "WHERE snapshot_date = ? ORDER BY attribution_rate_pct DESC", (snap,))
    out["markets"], out["tactics"] = mk, tc
    imp, clk = out.get("total_impressions"), out.get("total_clicks")
    out["ctr_pct"] = (_safe_div(clk, imp) or 0) * 100 if imp else None
    return out


def ad_vs_spend_markets(ads: dict | None, markets: pd.DataFrame, n: int = 6) -> pd.DataFrame:
    """Paid-media trip share next to share of visitor spend, market by market."""
    if not ads or ads.get("markets") is None or ads["markets"].empty or markets is None or markets.empty:
        return pd.DataFrame()
    a = ads["markets"][["dma", "trip_share_pct"]]
    j = markets[["dma", "share_pct"]].merge(a, on="dma", how="outer")
    j["rank_key"] = j[["share_pct", "trip_share_pct"]].max(axis=1)
    return j.sort_values("rank_key", ascending=False).head(n).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Forward look
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900, show_spinner=False)
def upcoming_events(_conn, days_ahead: int = 120) -> pd.DataFrame:
    df = _q(_conn, """
        SELECT event_name, event_date, is_major FROM vdp_events
        WHERE event_date >= date('now', 'localtime') AND event_date <= date('now', 'localtime', ?)
        ORDER BY event_date
    """, (f"+{days_ahead} day",))
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["event_date"])
    df["days_out"] = (df["date"] - pd.Timestamp.now().normalize()).dt.days
    return df


def event_last_year(daily: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Occupancy and ADR on the matching dates a year earlier (the event day
    and the night before it, shifted 364 days so the weekday lines up)."""
    if events is None or events.empty or daily is None or daily.empty:
        return events if events is not None else pd.DataFrame()
    rows = []
    for ev in events.itertuples():
        # 364 days back keeps the weekday; take the night before and the night of.
        a = ev.date - pd.Timedelta(days=365)
        sl = daily[(daily["date"] >= a) & (daily["date"] <= a + pd.Timedelta(days=1))]
        s = agg_hotel(sl)
        rows.append({"ly_occ": s["occ"] if s else None, "ly_adr": s["adr"] if s else None})
    return pd.concat([events.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


@st.cache_data(ttl=1800, show_spinner=False)
def group_mix_latest(_conn) -> dict | None:
    df = _q(_conn, """
        SELECT segment, metric_value AS occ_pct, as_of_date FROM fact_str_group_metrics
        WHERE market='Dana Point' AND metric_name='occ_pct' AND data_period='current'
          AND grain='weekly'
          AND as_of_date = (SELECT MAX(as_of_date) FROM fact_str_group_metrics
                            WHERE market='Dana Point' AND grain='weekly' AND data_period='current')
    """)
    if df.empty:
        return None
    tot = df[df["segment"] == "Total"]["occ_pct"]
    parts = df[df["segment"] != "Total"].copy()
    parts["segment"] = parts["segment"].replace({"Trans.": "Transient", "Grp.": "Group", "Cont.": "Contract"})
    total = float(tot.iloc[0]) if not tot.empty else float(parts["occ_pct"].sum())
    grp = parts[parts["segment"] == "Group"]["occ_pct"]
    return {"week": df["as_of_date"].iloc[0], "total_occ": total, "parts": parts,
            "group_share": (float(grp.iloc[0]) / total * 100) if (not grp.empty and total) else None}


@st.cache_data(ttl=1800, show_spinner=False)
def forward_insight(_conn) -> dict | None:
    """The pipeline's newest forward-looking insight, restricted to the
    categories built only on STR, Datafy, and the events calendar. Group
    dollar estimates are left out on purpose: group_intelligence is derived
    from placeholder CoStar tables (see CLAUDE.md)."""
    df = _q(_conn, """
        SELECT as_of_date, audience, category, headline, body, priority FROM insights_daily
        WHERE as_of_date = (SELECT MAX(as_of_date) FROM insights_daily)
          AND audience IN ('dmo', 'cross')
          AND category IN ('event_roi', 'compression_outlook', 'demand_trend')
          AND NOT (category = 'event_roi' AND COALESCE(horizon_days, 0) > 120)
        ORDER BY CASE category WHEN 'event_roi' THEN 0 WHEN 'compression_outlook' THEN 1 ELSE 2 END,
                 priority DESC
    """)
    return None if df.empty else df.iloc[0].to_dict()


# ---------------------------------------------------------------------------
# Freshness, measured the right way (pull recency, not calendar lag)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900, show_spinner=False)
def freshness(_conn) -> dict:
    f: dict = {}
    d = hotel_daily(_conn)
    if not d.empty:
        f["hotel_through"] = d["date"].max()
        s = d[d["source"] == "STR"]
        f["str_through"] = s["date"].max() if not s.empty else None
        f["costar_extends"] = bool((d["source"] == "CoStar").any())
    pr = _q(_conn, "SELECT MAX(report_date) AS r FROM costar_annual_performance")
    f["costar_report"] = pr.iloc[0]["r"] if not pr.empty else None
    pl = _q(_conn, "SELECT MAX(snapshot_date) AS s FROM costar_participation")
    f["costar_pull"] = pl.iloc[0]["s"] if not pl.empty else None
    # The participation roster is pulled less often than the daily/monthly/segment exports,
    # so its snapshot date alone made the badge read "pulled Sep 7" after a Sep 30 pull.
    # Those exports carry their pull date in the file name (daily_09_30_26.xlsx, mm_dd_yy);
    # load_log records the file each CoStar load used, so take the newest of the two.
    try:
        import re
        ll = _q(_conn, "SELECT DISTINCT file_name AS n FROM load_log WHERE source = 'CoStar' "
                       "AND grain IN ('daily', 'monthly', 'daily_segment', 'monthly_segment')")
        dates = []
        for name in (ll["n"].tolist() if not ll.empty else []):
            m = re.search(r"(?<!\d)(\d{2})_(\d{2})_(\d{2})(?!\d)", str(name))
            if m:
                dates.append(pd.Timestamp(2000 + int(m.group(3)), int(m.group(1)), int(m.group(2))))
        if dates:
            newest = max(dates).strftime("%Y-%m-%d")
            if not f["costar_pull"] or newest > str(f["costar_pull"]):
                f["costar_pull"] = newest
    except Exception:
        pass  # a malformed log row must never break the header badges
    dp = _q(_conn, "SELECT MIN(report_period_start) AS s, MAX(report_period_end) AS e "
                   "FROM datafy_overview_spending_by_market WHERE report_period_start = "
                   "(SELECT MAX(report_period_start) FROM datafy_overview_spending_by_market)")
    if not dp.empty:
        f["datafy_start"], f["datafy_end"] = dp.iloc[0]["s"], dp.iloc[0]["e"]
    ad = _q(_conn, "SELECT MAX(snapshot_date) AS s FROM datafy_advertising_kpis")
    f["ads_snapshot"] = ad.iloc[0]["s"] if not ad.empty else None
    wk = _q(_conn, "SELECT MAX(as_of_date) AS w FROM fact_str_group_metrics WHERE grain='weekly'")
    f["str_week"] = wk.iloc[0]["w"] if not wk.empty else None
    ins = _q(_conn, "SELECT MAX(as_of_date) AS d FROM insights_daily")
    f["insights"] = ins.iloc[0]["d"] if not ins.empty else None
    ag = _q(_conn, """
        SELECT COUNT(*) AS n,
               SUM(CASE WHEN ABS(k.occ_pct - m.occupancy_pct) <= 0.15 THEN 1 ELSE 0 END) AS occ_ok
        FROM kpi_daily_summary k JOIN costar_market_daily m USING (as_of_date)
        WHERE COALESCE(k.data_source, 'STR') = 'STR'
    """)
    if ag.empty or not ag.iloc[0]["n"]:
        ag = _q(_conn, """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN ABS(k.occ_pct - m.occupancy_pct) <= 0.15 THEN 1 ELSE 0 END) AS occ_ok
            FROM kpi_daily_summary k JOIN costar_market_daily m USING (as_of_date)
        """)
    if not ag.empty and ag.iloc[0]["n"]:
        f["agreement"] = {"n": int(ag.iloc[0]["n"]), "occ_ok": int(ag.iloc[0]["occ_ok"] or 0)}
    return f
