"""
compute_insights.py
-------------------
Generates forward-looking daily insights for four audiences:

  dmo      — Destination Marketing Organization / TBID board
  city     — City of Dana Point / City Council
  visitor  — Trip planners and incoming visitors
  resident — Local residents of Dana Point

Reads from ALL tables in analytics.sqlite and cross-references them to
produce time-anchored, data-driven signals.  One insight row per
audience/category is stored in insights_daily (UPSERT).

Table relationships are seeded once into table_relationships.

Run:
    python3 scripts/compute_insights.py
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")

TODAY = date.today().isoformat()   # YYYY-MM-DD


# ---------------------------------------------------------------------------
# DDL — new tables
# ---------------------------------------------------------------------------

DDL_INSIGHTS_DAILY = """
CREATE TABLE IF NOT EXISTS insights_daily (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of_date   TEXT NOT NULL,           -- YYYY-MM-DD of generation
    audience     TEXT NOT NULL,           -- 'dmo' | 'city' | 'visitor' | 'resident' | 'cross'
    category     TEXT NOT NULL,           -- e.g. 'demand_trend', 'tot_revenue'
    headline     TEXT NOT NULL,           -- 1-line summary (≤ 120 chars)
    body         TEXT NOT NULL,           -- 2–4 sentence detail
    metric_basis TEXT,                    -- JSON key→value of driving metrics
    priority     INTEGER DEFAULT 5,       -- 1=highest, 10=lowest
    horizon_days INTEGER DEFAULT 30,      -- forward lookahead in days
    data_sources TEXT,                    -- comma-sep list of tables used
    created_at   TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of_date, audience, category)
);
"""

DDL_TABLE_RELATIONSHIPS = """
CREATE TABLE IF NOT EXISTS table_relationships (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    table_a           TEXT NOT NULL,
    table_b           TEXT NOT NULL,
    relationship_type TEXT NOT NULL,   -- 'date_join' | 'derived_from' | 'enriches' | 'cross_ref'
    join_key          TEXT,            -- e.g. 'as_of_date', 'market', 'quarter'
    description       TEXT,
    created_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(table_a, table_b, relationship_type)
);
"""

# All documented cross-table relationships in the brain
RELATIONSHIPS: list[tuple[str, str, str, str, str]] = [
    # (table_a, table_b, type, join_key, description)
    ("fact_str_metrics",           "kpi_daily_summary",              "derived_from", "as_of_date",
     "kpi_daily_summary is pivoted and aggregated from fact_str_metrics grain=daily"),
    ("kpi_daily_summary",          "kpi_compression_quarterly",      "derived_from", "as_of_date→quarter",
     "kpi_compression_quarterly groups kpi_daily_summary by calendar quarter"),
    ("fact_str_metrics",           "load_log",                       "audited_by",   "run_at",
     "load_log records every ETL batch that writes into fact_str_metrics"),
    ("kpi_daily_summary",          "load_log",                       "audited_by",   "run_at",
     "load_log records KPI compute runs"),
    ("fact_str_metrics",           "datafy_overview_kpis",           "cross_ref",    "report_period",
     "STR hotel demand aligns with Datafy overall trip volumes for same period"),
    ("fact_str_metrics",           "datafy_attribution_media_kpis",  "cross_ref",    "report_period",
     "Media campaign attribution overlaps with STR demand period for ADR lift analysis"),
    ("datafy_overview_dma",        "datafy_attribution_website_dma", "cross_ref",    "dma",
     "DMA visitor share in overview vs website-attributed trips by DMA"),
    ("datafy_overview_kpis",       "datafy_overview_dma",            "enriches",     "report_period",
     "DMA breakdown enriches overall Datafy KPIs with feeder-market detail"),
    ("datafy_overview_kpis",       "datafy_overview_demographics",   "enriches",     "report_period",
     "Demographic breakdown enriches overall visitor profile"),
    ("datafy_overview_kpis",       "datafy_overview_category_spending", "enriches",  "report_period",
     "Spending categories enrich overall economic impact of visitors"),
    ("datafy_overview_kpis",       "datafy_overview_cluster_visitation", "enriches", "report_period",
     "Cluster visitation enriches overall trip counts by area type"),
    ("datafy_overview_kpis",       "datafy_overview_airports",       "enriches",     "report_period",
     "Airport origins enrich visitor origin intelligence"),
    ("datafy_attribution_website_kpis", "datafy_attribution_website_top_markets", "enriches", "report_period",
     "Top markets enrich website attribution KPIs"),
    ("datafy_attribution_website_kpis", "datafy_attribution_website_channels",    "enriches", "report_period",
     "Channel breakdown enriches website attribution KPIs"),
    ("datafy_attribution_website_kpis", "datafy_attribution_website_clusters",    "enriches", "report_period",
     "Cluster breakdown enriches website attribution by area type"),
    ("datafy_attribution_website_kpis", "datafy_attribution_website_demographics","enriches", "report_period",
     "Demographics enrich website-attributed visitor profile"),
    ("datafy_attribution_media_kpis",   "datafy_attribution_media_top_markets",   "enriches", "report_period",
     "Top markets enrich media attribution KPIs with feeder-market ROI detail"),
    ("datafy_social_traffic_sources",   "datafy_social_audience_overview",        "enriches", "loaded_at",
     "Traffic source breakdown enriches overall social/website audience metrics"),
    ("datafy_social_top_pages",         "datafy_social_audience_overview",        "enriches", "loaded_at",
     "Top pages enrich audience engagement data"),
    ("kpi_daily_summary",               "insights_daily",                         "derived_from", "as_of_date",
     "insights_daily is generated from kpi_daily_summary plus all Datafy tables"),
    ("datafy_overview_kpis",            "insights_daily",                         "derived_from", "report_period",
     "Datafy overview KPIs feed the insights engine for visitor/resident/city insights"),
    # Cross-dataset joins (STR ↔ Datafy) that produce hidden insights
    ("kpi_daily_summary",               "datafy_overview_dma",                    "cross_ref",    "time_period",
     "STR ADR joined with DMA avg_spend reveals which feeder markets under/overpay relative to rate"),
    ("kpi_daily_summary",               "datafy_overview_kpis",                   "cross_ref",    "time_period",
     "STR weekend/weekday occ gap joined with Datafy avg LOS reveals LOS extension revenue opportunity"),
    ("kpi_compression_quarterly",       "datafy_overview_kpis",                   "cross_ref",    "time_period",
     "Compression days joined with day_trip_pct reveals hidden infrastructure multiplier on peak days"),
    ("kpi_compression_quarterly",       "datafy_attribution_website_channels",    "cross_ref",    "report_period",
     "Compression by quarter joined with attribution channel reveals whether campaigns drive peak or shoulder"),
    ("kpi_daily_summary",               "datafy_overview_kpis",                   "cross_ref",    "time_period",
     "ADR YOY joined with OOS spend share reveals rate capture gap vs. visitor willingness to pay"),
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def ensure_tables(cur: sqlite3.Cursor) -> None:
    cur.execute(DDL_INSIGHTS_DAILY)
    cur.execute(DDL_TABLE_RELATIONSHIPS)


def seed_relationships(cur: sqlite3.Cursor) -> int:
    """Upsert all documented table relationships.  Returns insert count."""
    count = 0
    for table_a, table_b, rel_type, join_key, desc in RELATIONSHIPS:
        cur.execute(
            """
            INSERT INTO table_relationships (table_a, table_b, relationship_type, join_key, description)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(table_a, table_b, relationship_type) DO UPDATE SET
                join_key    = excluded.join_key,
                description = excluded.description
            """,
            (table_a, table_b, rel_type, join_key, desc),
        )
        count += 1
    return count


def upsert_insight(
    cur: sqlite3.Cursor,
    audience: str,
    category: str,
    headline: str,
    body: str,
    metric_basis: dict[str, Any],
    priority: int = 5,
    horizon_days: int = 30,
    data_sources: str = "",
) -> None:
    cur.execute(
        """
        INSERT INTO insights_daily
            (as_of_date, audience, category, headline, body,
             metric_basis, priority, horizon_days, data_sources)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(as_of_date, audience, category) DO UPDATE SET
            headline     = excluded.headline,
            body         = excluded.body,
            metric_basis = excluded.metric_basis,
            priority     = excluded.priority,
            horizon_days = excluded.horizon_days,
            data_sources = excluded.data_sources,
            created_at   = datetime('now')
        """,
        (
            TODAY, audience, category,
            headline[:120], body,
            json.dumps(metric_basis),
            priority, horizon_days, data_sources,
        ),
    )


# ---------------------------------------------------------------------------
# Data loaders (read-only snapshots for insight generation)
# ---------------------------------------------------------------------------

def load_kpi_recent(conn: sqlite3.Connection, days: int = 90) -> pd.DataFrame:
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    df = pd.read_sql_query(
        "SELECT as_of_date, occ_pct, adr, revpar, occ_yoy, adr_yoy, revpar_yoy, "
        "       is_occ_80, is_occ_90 "
        "FROM kpi_daily_summary "
        "WHERE as_of_date >= ? ORDER BY as_of_date",
        conn, params=(cutoff,),
    )
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def load_kpi_all(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT as_of_date, occ_pct, adr, revpar, is_occ_80, is_occ_90 "
        "FROM kpi_daily_summary ORDER BY as_of_date",
        conn,
    )
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def load_compression(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT * FROM kpi_compression_quarterly ORDER BY quarter DESC", conn
    )


def load_str_revenue(conn: sqlite3.Connection, days: int = 90) -> pd.DataFrame:
    """Trailing room revenue from fact_str_metrics for TBID/TOT calcs."""
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    df = pd.read_sql_query(
        "SELECT as_of_date, metric_name, metric_value "
        "FROM fact_str_metrics "
        "WHERE source='STR' AND grain='daily' AND metric_name='revenue' "
        "  AND as_of_date >= ? ORDER BY as_of_date",
        conn, params=(cutoff,),
    )
    return df


def load_datafy_overview(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return the most recent Datafy overview KPI row as a dict."""
    try:
        df = pd.read_sql_query(
            "SELECT * FROM datafy_overview_kpis ORDER BY report_period_end DESC LIMIT 1",
            conn,
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def load_top_dmas(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            "SELECT dma, visitor_days_share_pct, spending_share_pct, avg_spend_usd "
            "FROM datafy_overview_dma "
            "ORDER BY visitor_days_share_pct DESC LIMIT 5",
            conn,
        )
    except Exception:
        return pd.DataFrame()


def load_spending_categories(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            "SELECT category, spend_share_pct "
            "FROM datafy_overview_category_spending "
            "ORDER BY spend_share_pct DESC LIMIT 5",
            conn,
        )
    except Exception:
        return pd.DataFrame()


def load_media_kpis(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        df = pd.read_sql_query(
            "SELECT * FROM datafy_attribution_media_kpis "
            "ORDER BY report_period_end DESC LIMIT 1",
            conn,
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def load_website_kpis(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        df = pd.read_sql_query(
            "SELECT * FROM datafy_attribution_website_kpis "
            "ORDER BY report_period_end DESC LIMIT 1",
            conn,
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def load_social_overview(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        df = pd.read_sql_query(
            "SELECT * FROM datafy_social_audience_overview "
            "ORDER BY loaded_at DESC LIMIT 1",
            conn,
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def load_all_dmas(conn: sqlite3.Connection) -> pd.DataFrame:
    """All DMA rows including spend efficiency calculation."""
    try:
        df = pd.read_sql_query(
            "SELECT dma, visitor_days_share_pct, spending_share_pct, avg_spend_usd "
            "FROM datafy_overview_dma "
            "ORDER BY visitor_days_share_pct DESC",
            conn,
        )
        # Spend efficiency index: spending_share / visitor_days_share
        # >1.0 means this DMA spends above their visitor-volume proportion (high value)
        df["spend_efficiency"] = (
            df["spending_share_pct"] / df["visitor_days_share_pct"]
        ).where(df["visitor_days_share_pct"] > 0)
        return df
    except Exception:
        return pd.DataFrame()


def load_attribution_channels(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            "SELECT acquisition_channel, attribution_rate_pct, "
            "       attributable_trips_dest "
            "FROM datafy_attribution_website_channels "
            "ORDER BY attributable_trips_dest DESC",
            conn,
        )
    except Exception:
        return pd.DataFrame()


def load_kpi_with_dow(conn: sqlite3.Connection) -> pd.DataFrame:
    """Full KPI table with day-of-week column attached."""
    df = pd.read_sql_query(
        "SELECT as_of_date, occ_pct, adr, revpar FROM kpi_daily_summary "
        "ORDER BY as_of_date",
        conn,
    )
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df["dow"] = df["as_of_date"].dt.dayofweek   # 0=Mon … 6=Sun
    return df


# ---------------------------------------------------------------------------
# Signal helpers
# ---------------------------------------------------------------------------

def _trend_direction(series: pd.Series, window: int = 14) -> str:
    """Return 'up', 'down', or 'flat' based on linear slope over last `window` rows."""
    s = series.dropna()
    if len(s) < 4:
        return "flat"
    recent = s.tail(window)
    slope = pd.Series(range(len(recent))).cov(recent) / max(pd.Series(range(len(recent))).var(), 1e-9)
    if slope > 0.05:
        return "up"
    if slope < -0.05:
        return "down"
    return "flat"


def _pct(val: float | None, decimals: int = 1) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.{decimals}f}%"


def _dollar(val: float | None) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    return f"${val:,.2f}"


def _seasonal_position() -> tuple[str, str, str]:
    """Return (quarter_label, season_name, next_peak_note) for today."""
    today = date.today()
    m = today.month
    if m in (1, 2, 3):
        return "Q1", "shoulder", "Spring break (late March) is the next demand inflection."
    if m in (4, 5, 6):
        return "Q2", "spring secondary peak", "Summer peak (July–Aug) is approaching."
    if m in (7, 8, 9):
        return "Q3", "peak summer", "Ohana Fest (Sept) is the season's final compression event."
    return "Q4", "fall shoulder", "Q1 soft season begins January; defend rate floors now."


def _days_to_event(month: int, day: int) -> int:
    """Days until next occurrence of (month, day)."""
    today = date.today()
    target = date(today.year, month, day)
    if target <= today:
        target = date(today.year + 1, month, day)
    return (target - today).days


def _5wh(
    who: str,
    what: str,
    when: str,
    where: str,
    why: str,
    how: str,
) -> str:
    """Return a structured 5W+H intelligence block appended to insight bodies.

    All insights must answer: WHO is affected, WHAT the data shows, WHEN action is needed,
    WHERE the opportunity or risk is located, WHY it matters strategically, HOW to act.
    This makes every insight immediately actionable for the board, staff, and hotel GMs.
    """
    return (
        f" | WHO: {who} | WHAT: {what} | WHEN: {when}"
        f" | WHERE: {where} | WHY: {why} | HOW: {how}"
    )


# ---------------------------------------------------------------------------
# Insight generators — DMO
# ---------------------------------------------------------------------------

def gen_dmo_demand_trend(kpi: pd.DataFrame, comp: pd.DataFrame) -> dict:
    if kpi.empty:
        return {}
    latest = kpi.iloc[-1]
    trend = _trend_direction(kpi["revpar"])
    trend_word = {"up": "strengthening", "down": "softening", "flat": "stable"}.get(trend, "stable")
    q_lbl, season, next_peak = _seasonal_position()

    # 30-day averages
    last30 = kpi.tail(30)
    avg_occ  = last30["occ_pct"].mean()
    avg_adr  = last30["adr"].mean()
    avg_rvp  = last30["revpar"].mean()
    avg_ryoy = last30["revpar_yoy"].mean()

    # Compression count for current quarter
    cq = f"{date.today().year}-{q_lbl}"
    comp_row = comp[comp["quarter"] == cq] if not comp.empty else pd.DataFrame()
    comp_80 = int(comp_row["days_above_80_occ"].iloc[0]) if not comp_row.empty else 0

    headline = (
        f"RevPAR {trend_word} at {_dollar(avg_rvp)} (30-day avg); "
        f"YOY {_pct(avg_ryoy)} — {season} position"
    )
    action = "Maintain rate discipline and lock 2-night minimums on compression dates." if (avg_ryoy or 0) >= 0 else "Launch targeted demand programs for mid-week shoulder periods."
    body = (
        f"The trailing 30-day average RevPAR is {_dollar(avg_rvp)}, "
        f"with ADR at {_dollar(avg_adr)} and occupancy at {avg_occ:.1f}%. "
        f"Year-over-year RevPAR growth is {_pct(avg_ryoy)}, signaling "
        f"{'a healthy pricing environment — rate discipline should be maintained' if (avg_ryoy or 0) >= 0 else 'rate pressure — evaluate demand generation programs'}. "
        f"{next_peak} "
        f"Current-quarter compression: {comp_80} days above 80% occupancy."
        + _5wh(
            who="VDP TBID board, hotel revenue managers",
            what=f"RevPAR {_dollar(avg_rvp)} ({_pct(avg_ryoy)} YOY), {comp_80} compression days QTD",
            when=f"Next 30 days — {season} season, {q_lbl}",
            where="Dana Point select portfolio (12 properties)",
            why="RevPAR trajectory sets TBID revenue and board narrative for the quarter",
            how=action,
        )
    )
    return dict(
        headline=headline, body=body, priority=1, horizon_days=30,
        data_sources="kpi_daily_summary,kpi_compression_quarterly",
        metric_basis={"avg_revpar_30d": round(avg_rvp or 0, 2),
                      "avg_adr_30d": round(avg_adr or 0, 2),
                      "avg_occ_30d": round(avg_occ or 0, 1),
                      "avg_revpar_yoy_30d": round(avg_ryoy or 0, 2),
                      "trend": trend,
                      "comp_80_qtd": comp_80},
    )


def gen_dmo_tbid_projection(kpi: pd.DataFrame, str_rev: pd.DataFrame) -> dict:
    if kpi.empty:
        return {}
    # TBID ≈ room revenue × 0.0125  |  TOT ≈ room revenue × 0.10
    # Trailing 90-day room revenue from fact_str_metrics
    total_rev_90d = str_rev["metric_value"].sum() if not str_rev.empty else None

    # Seasonal projection multiplier
    q_lbl, _, _ = _seasonal_position()
    q_mult = {"Q1": 0.75, "Q2": 1.00, "Q3": 1.25, "Q4": 1.00}.get(q_lbl, 1.00)

    if total_rev_90d and total_rev_90d > 0:
        tbid_90d  = total_rev_90d * 0.0125
        tot_90d   = total_rev_90d * 0.10
        # Trailing 90d ≈ one quarter; apply seasonal mult to project next quarter
        next_q_tbid = (total_rev_90d * q_mult) * 0.0125
        headline = (
            f"Trailing 90-day TBID est.: {_dollar(tbid_90d)} | "
            f"Projected next-quarter: {_dollar(next_q_tbid)}"
        )
        body = (
            f"Based on {_dollar(total_rev_90d)} in trailing 90-day room revenue, "
            f"estimated TBID assessments total {_dollar(tbid_90d)} (blended 1.25%). "
            f"The City of Dana Point's TOT share for the same period is approximately "
            f"{_dollar(tot_90d)} (10% of room revenue). "
            f"Applying a {q_mult:.2f}× seasonal factor for {q_lbl}, "
            f"next-quarter TBID revenue is projected at {_dollar(next_q_tbid)}. "
            f"Rate discipline — not volume growth — is the highest-ROI lever for both metrics."
        )
        basis = {"total_rev_90d": round(total_rev_90d, 2),
                 "tbid_90d": round(tbid_90d, 2), "tot_90d": round(tot_90d, 2),
                 "next_q_tbid_projected": round(next_q_tbid, 2),
                 "seasonal_mult": q_mult}
    else:
        # Fall back to RevPAR-based estimate if revenue not available
        last30 = kpi.tail(30)
        avg_rvp = last30["revpar"].mean()
        avg_sup = 0  # supply not available in kpi table
        headline = f"RevPAR trending at {_dollar(avg_rvp)} — load room revenue data for TBID projections"
        body = (
            f"Daily RevPAR averages {_dollar(avg_rvp)} over the trailing 30 days. "
            f"Run the full STR pipeline with revenue metric data to enable TBID and TOT projections. "
            f"At current RevPAR, every 1% ADR gain across the 12-property portfolio materially "
            f"lifts both TBID assessments and TOT receipts. "
            f"Prioritize rate discipline heading into the next demand cycle."
        )
        basis = {"avg_revpar_30d": round(avg_rvp or 0, 2)}

    return dict(headline=headline, body=body, priority=2, horizon_days=90,
                data_sources="fact_str_metrics,kpi_daily_summary",
                metric_basis=basis)


def gen_dmo_feeder_market(dmas: pd.DataFrame, web_kpis: dict, media_kpis: dict) -> dict:
    if dmas.empty:
        return {}
    top_dma   = dmas.iloc[0]["dma"] if not dmas.empty else "Los Angeles"
    top_share = dmas.iloc[0]["visitor_days_share_pct"] if not dmas.empty else 0
    second    = dmas.iloc[1]["dma"] if len(dmas) > 1 else "San Diego"
    sec_share = dmas.iloc[1]["visitor_days_share_pct"] if len(dmas) > 1 else 0

    web_trips = web_kpis.get("attributable_trips") or 0
    med_trips = media_kpis.get("attributable_trips") or 0

    headline = (
        f"{top_dma} drives {top_share:.1f}% of visitor days; "
        f"website + media generated {int(web_trips + med_trips):,} attributable trips"
    )
    body = (
        f"Datafy data shows {top_dma} ({top_share:.1f}%) and {second} ({sec_share:.1f}%) "
        f"as the top feeder markets by visitor-days. "
        f"VDP website attribution produced {int(web_trips):,} tracked trips; "
        f"media campaigns added {int(med_trips):,} attributable trips. "
        f"Forward focus: increase shoulder-season targeting in {second} and SF Bay Area "
        f"to diversify drive-market dependency and reduce Q1 softness. "
        f"Out-of-state visitor days represent 61% of total — protect that mix with fly-market content."
    )
    dma_list = [(r["dma"], r["visitor_days_share_pct"]) for _, r in dmas.iterrows()]
    return dict(
        headline=headline, body=body, priority=2, horizon_days=60,
        data_sources="datafy_overview_dma,datafy_attribution_website_kpis,datafy_attribution_media_kpis",
        metric_basis={"top_dmas": dma_list,
                      "website_trips": int(web_trips),
                      "media_trips": int(med_trips)},
    )


def gen_dmo_compression_outlook(comp: pd.DataFrame, kpi: pd.DataFrame) -> dict:
    if comp.empty:
        return {}
    q_lbl, season, _ = _seasonal_position()

    # Historical Q3 compression (benchmark)
    q3_rows = comp[comp["quarter"].str.contains("-Q3")]
    avg_q3_80 = q3_rows["days_above_80_occ"].mean() if not q3_rows.empty else 34

    # Current quarter
    cq = f"{date.today().year}-{q_lbl}"
    cq_row = comp[comp["quarter"] == cq]
    cq_80  = int(cq_row["days_above_80_occ"].iloc[0]) if not cq_row.empty else 0
    cq_90  = int(cq_row["days_above_90_occ"].iloc[0]) if not cq_row.empty else 0

    days_to_q3 = _days_to_event(7, 1)   # July 1 = Q3 start proxy
    headline = (
        f"{cq} compression: {cq_80} days >80% occ — "
        f"Q3 peak ({int(avg_q3_80)}-day avg) starts in ~{days_to_q3} days"
    )
    body = (
        f"Current quarter ({cq}) has logged {cq_80} days above 80% occupancy "
        f"and {cq_90} days above 90%. "
        f"Historical Q3 average is {avg_q3_80:.0f} days above 80% — "
        f"the highest compression window of the year. "
        f"Q3 peak demand is approximately {days_to_q3} days away; "
        f"revenue management teams should be implementing BAR increases and "
        f"closing discount channels for high-demand dates now. "
        f"Each additional compression day above 90% represents a rate-capture opportunity "
        f"worth an estimated 10–20% ADR premium over the daily baseline."
    )
    return dict(
        headline=headline, body=body, priority=1, horizon_days=90,
        data_sources="kpi_compression_quarterly,kpi_daily_summary",
        metric_basis={"cq": cq, "cq_80": cq_80, "cq_90": cq_90,
                      "avg_q3_80_historical": round(avg_q3_80, 1),
                      "days_to_q3": days_to_q3},
    )


def gen_dmo_event_roi(media_kpis: dict, web_kpis: dict) -> dict:
    days_to_ohana = _days_to_event(9, 26)   # Ohana Fest ≈ last weekend Sept
    roas = media_kpis.get("roas_description", "5.6× return on ad spend (Datafy)")
    camp_impact = media_kpis.get("total_impact_usd") or 0
    web_impact  = web_kpis.get("est_impact_usd") or 0
    total_impact = (camp_impact or 0) + (web_impact or 0)

    headline = (
        f"Ohana Fest in ~{days_to_ohana} days — "
        f"combined marketing impact: {_dollar(total_impact)} est."
    )
    body = (
        f"Ohana Fest (annual September music event) is approximately {days_to_ohana} days away "
        f"and represents the single highest ADR-lift event in the VDP calendar (+$139 ADR vs baseline). "
        f"Verified Datafy benchmarks: $18.4M total destination spend, 68% out-of-state attendees, "
        f"3.2× economic multiplier. "
        f"Combined website + media attribution estimated {_dollar(total_impact)} in visitor impact. "
        f"Begin event-specific campaign activation 90 days out; "
        f"prioritize out-of-state LA and SF feeder markets for highest incremental spend."
    )
    return dict(
        headline=headline, body=body, priority=3, horizon_days=days_to_ohana,
        data_sources="datafy_attribution_media_kpis,datafy_attribution_website_kpis",
        metric_basis={"days_to_ohana": days_to_ohana,
                      "media_impact_usd": round(camp_impact or 0, 2),
                      "web_impact_usd": round(web_impact or 0, 2),
                      "total_impact_usd": round(total_impact, 2)},
    )


# ---------------------------------------------------------------------------
# Insight generators — City
# ---------------------------------------------------------------------------

def gen_city_tot_revenue(str_rev: pd.DataFrame, kpi: pd.DataFrame) -> dict:
    q_lbl, _, _ = _seasonal_position()
    q_mult = {"Q1": 0.75, "Q2": 1.00, "Q3": 1.25, "Q4": 1.00}.get(q_lbl, 1.00)

    total_rev_90d = str_rev["metric_value"].sum() if not str_rev.empty else None

    if total_rev_90d and total_rev_90d > 0:
        tot_90d   = total_rev_90d * 0.10
        # Trailing 90d ≈ one quarter; apply seasonal mult to project next quarter
        next_q_tot = (total_rev_90d * q_mult) * 0.10
        headline = (
            f"Trailing 90-day TOT est.: {_dollar(tot_90d)} | "
            f"Next-quarter projection: {_dollar(next_q_tot)}"
        )
        body = (
            f"Transient Occupancy Tax (10% of room revenue) for the trailing 90 days "
            f"is estimated at {_dollar(tot_90d)}, based on {_dollar(total_rev_90d)} "
            f"in STR-verified room revenue. "
            f"Applying a {q_mult:.2f}× seasonal adjustment for {q_lbl}, "
            f"next-quarter TOT revenue is projected at {_dollar(next_q_tot)}. "
            f"TOT flows directly to the City of Dana Point general fund — "
            f"a {'+' if q_mult > 1 else ''}{(q_mult-1)*100:.0f}% seasonal swing is "
            f"expected versus the trailing period. "
            f"Budget teams should anchor next-year projections to Q3 actuals, not annual averages."
        )
        basis = {"total_rev_90d": round(total_rev_90d, 2),
                 "tot_90d": round(tot_90d, 2),
                 "next_q_tot": round(next_q_tot, 2), "seasonal_mult": q_mult}
    else:
        last30 = kpi.tail(30)
        avg_rvp = last30["revpar"].mean()
        headline = f"RevPAR at {_dollar(avg_rvp)} — load revenue data to project TOT receipts"
        body = (
            f"The current RevPAR of {_dollar(avg_rvp)} reflects active demand. "
            f"Dana Point's 10% Transient Occupancy Tax accrues on gross room revenue. "
            f"Run the full STR pipeline with revenue data to generate accurate TOT projections. "
            f"At historical supply levels, each $10 ADR gain translates to material incremental TOT revenue."
        )
        basis = {"avg_revpar_30d": round(avg_rvp or 0, 2)}

    return dict(headline=headline, body=body, priority=1, horizon_days=90,
                data_sources="fact_str_metrics,kpi_daily_summary",
                metric_basis=basis)


def gen_city_infrastructure(comp: pd.DataFrame) -> dict:
    q_lbl, season, _ = _seasonal_position()
    days_to_q3 = _days_to_event(7, 1)
    days_to_memorial = _days_to_event(5, 26)   # Memorial Day weekend proxy
    days_to_labor    = _days_to_event(9, 1)    # Labor Day proxy

    # Most recent quarter compression
    cq = f"{date.today().year}-{q_lbl}"
    cq_row = comp[comp["quarter"] == cq] if not comp.empty else pd.DataFrame()
    cq_80 = int(cq_row["days_above_80_occ"].iloc[0]) if not cq_row.empty else 0

    # Upcoming high-pressure event (closest)
    upcoming_days = min(days_to_memorial, days_to_q3, days_to_labor)
    upcoming_name = "Memorial Day weekend" if upcoming_days == days_to_memorial else (
        "Summer peak (Q3)" if upcoming_days == days_to_q3 else "Labor Day weekend"
    )

    headline = (
        f"Next high-traffic period: {upcoming_name} in ~{upcoming_days} days — "
        f"prepare parking, transit, and coastal access resources"
    )
    body = (
        f"Current-quarter compression: {cq_80} days above 80% hotel occupancy — "
        f"each such day signals elevated visitor volume across beaches, harbor, and "
        f"downtown Dana Point. "
        f"{upcoming_name} (~{upcoming_days} days away) historically triggers "
        f"90%+ occupancy and peak coastal traffic. "
        f"City departments should coordinate beach parking overflow, Harbor Drive traffic, "
        f"and Doheny State Beach access management. "
        f"Q3 (July–Sept) generates approximately 37+ compression days on average — "
        f"the highest sustained infrastructure pressure of the year."
    )
    return dict(
        headline=headline, body=body, priority=2, horizon_days=upcoming_days,
        data_sources="kpi_compression_quarterly",
        metric_basis={"cq_80": cq_80, "days_to_next_peak": upcoming_days,
                      "next_peak_event": upcoming_name},
    )


def gen_city_visitor_profile(overview: dict) -> dict:
    if not overview:
        return {}
    total_trips     = overview.get("total_trips") or 0
    overnight_pct   = overview.get("overnight_trips_pct") or 0
    out_of_state    = overview.get("out_of_state_vd_pct") or 0
    repeat_vis      = overview.get("repeat_visitors_pct") or 0
    avg_los         = overview.get("avg_length_of_stay_days") or 0

    headline = (
        f"{int(total_trips):,} annual trips to Dana Point — "
        f"{overnight_pct:.1f}% overnight, {out_of_state:.1f}% out-of-state"
    )
    body = (
        f"Datafy geolocation data for the most recent annual period shows "
        f"{int(total_trips):,} total trips to Dana Point, with "
        f"{overnight_pct:.1f}% classified as overnight stays "
        f"(avg {avg_los:.1f} nights). "
        f"{out_of_state:.1f}% of visitor-days originate from out-of-state — "
        f"these visitors generate the highest per-trip economic impact. "
        f"{repeat_vis:.1f}% are repeat visitors, indicating strong destination loyalty. "
        f"City services, signage, and visitor programs should be calibrated for a primarily "
        f"overnight, out-of-state audience to maximize economic return per visitor."
    )
    return dict(
        headline=headline, body=body, priority=3, horizon_days=365,
        data_sources="datafy_overview_kpis",
        metric_basis={"total_trips": int(total_trips),
                      "overnight_pct": overnight_pct,
                      "out_of_state_pct": out_of_state,
                      "repeat_visitors_pct": repeat_vis,
                      "avg_los_days": avg_los},
    )


def gen_city_economic_impact(overview: dict, spending: pd.DataFrame) -> dict:
    if not overview:
        return {}
    # Total spend proxy: visitor_spending_pct × total est (use Ohana Fest multiplier as frame)
    out_of_state_spend_pct = overview.get("out_of_state_spending_pct") or 60

    # Top spending categories
    top_cats = []
    if not spending.empty:
        for _, r in spending.head(3).iterrows():
            top_cats.append(f"{r['category']} ({r['spend_share_pct']:.1f}%)")

    cats_str = ", ".join(top_cats) if top_cats else "accommodation, dining, retail"
    headline = (
        f"Tourism drives {out_of_state_spend_pct:.0f}% out-of-state spending; "
        f"top categories: {cats_str}"
    )
    body = (
        f"Out-of-state visitors account for {out_of_state_spend_pct:.0f}% of total "
        f"tourism spending in Dana Point — the highest-value economic segment. "
        f"Primary spend categories are {cats_str}. "
        f"The Ohana Fest benchmark ($18.4M destination spend, 3.2× multiplier) demonstrates "
        f"events' ability to generate genuine incremental economic activity, "
        f"not just redistribution of existing visitor spend. "
        f"City economic development strategy should prioritize event attraction, "
        f"fly-market feeder campaigns, and extended-stay programming to maximize per-trip spending."
    )
    return dict(
        headline=headline, body=body, priority=3, horizon_days=365,
        data_sources="datafy_overview_kpis,datafy_overview_category_spending",
        metric_basis={"out_of_state_spending_pct": out_of_state_spend_pct,
                      "top_spending_categories": top_cats},
    )


# ---------------------------------------------------------------------------
# Insight generators — Visitor
# ---------------------------------------------------------------------------

def gen_visitor_best_value(kpi: pd.DataFrame) -> dict:
    """Identify the best upcoming low-rate windows for trip planners."""
    if kpi.empty:
        return {}
    q_lbl, season, _ = _seasonal_position()
    avg_adr = kpi["adr"].mean()
    low_occ_days = kpi[kpi["occ_pct"] < 70]
    pct_low = len(low_occ_days) / max(len(kpi), 1) * 100

    # Best months for value
    if q_lbl == "Q1":
        value_window = "January through mid-March (current window) — lowest ADR and best availability"
        action = "Book now for the best rates before spring break demand lifts pricing."
    elif q_lbl == "Q2":
        value_window = "weekday stays in April–May before summer rates take effect"
        action = "Lock in weekday rates before Memorial Day; weekends already compress."
    elif q_lbl == "Q3":
        value_window = "October (post-Labor Day shoulder) — expect 20–30% ADR drop vs summer"
        action = "Consider October or November travel for significant savings over summer pricing."
    else:
        value_window = "November through December (excluding holiday weekends)"
        action = "Early January is the lowest-rate period in the Dana Point calendar — plan ahead."

    headline = (
        f"Best value window: {value_window[:60]} — avg ADR {_dollar(avg_adr)}"
    )
    body = (
        f"Over the trailing data period, {pct_low:.0f}% of days showed occupancy below 70%, "
        f"signaling available inventory and competitive rates. "
        f"Average ADR across all dates is {_dollar(avg_adr)}; "
        f"visitors who choose weekday stays or shoulder periods typically see "
        f"15–30% savings versus Friday–Saturday peak nights. "
        f"{action}"
    )
    return dict(
        headline=headline, body=body, priority=2, horizon_days=30,
        data_sources="kpi_daily_summary",
        metric_basis={"avg_adr_all": round(avg_adr or 0, 2),
                      "pct_days_below_70occ": round(pct_low, 1),
                      "value_window": value_window},
    )


def gen_visitor_rate_outlook(kpi: pd.DataFrame) -> dict:
    if kpi.empty:
        return {}
    last30 = kpi.tail(30)
    avg_adr_30  = last30["adr"].mean()
    avg_yoy_30  = last30["adr_yoy"].mean()
    trend       = _trend_direction(kpi["adr"])
    trend_word  = {"up": "rising", "down": "declining", "flat": "holding steady"}.get(trend, "stable")

    q_lbl, _, next_peak = _seasonal_position()
    headline = (
        f"Hotel rates {trend_word}: avg ADR {_dollar(avg_adr_30)}, "
        f"YOY {_pct(avg_yoy_30)} — book early for peak season"
    )
    body = (
        f"Average daily hotel rates are currently {trend_word} at {_dollar(avg_adr_30)} "
        f"(30-day average), with year-over-year change of {_pct(avg_yoy_30)}. "
        f"Dana Point is a demand-driven leisure market: rates rise sharply "
        f"as summer compression events approach. "
        f"{next_peak} "
        f"Booking 4–6 weeks in advance typically secures rates 10–20% below "
        f"last-minute peak pricing. "
        f"Flexibility on day-of-week (Tue–Thu) can yield an additional 15–25% savings."
    )
    return dict(
        headline=headline, body=body, priority=1, horizon_days=60,
        data_sources="kpi_daily_summary",
        metric_basis={"avg_adr_30d": round(avg_adr_30 or 0, 2),
                      "avg_adr_yoy_30d": round(avg_yoy_30 or 0, 2),
                      "trend": trend},
    )


def gen_visitor_upcoming_events(media_kpis: dict) -> dict:
    days_to_ohana    = _days_to_event(9, 26)
    days_to_memorial = _days_to_event(5, 26)
    days_to_fourth   = _days_to_event(7, 4)
    days_to_labor    = _days_to_event(9, 1)

    upcoming = sorted([
        ("Ohana Fest (Doheny State Beach — annual Sept music festival)", days_to_ohana, 90),
        ("Memorial Day weekend (high occupancy / rate premium)", days_to_memorial, 60),
        ("Fourth of July (peak compression, harbor fireworks)", days_to_fourth, 45),
        ("Labor Day weekend (final summer compression event)", days_to_labor, 45),
    ], key=lambda x: x[1])
    next_event, next_days, rate_premium = upcoming[0]

    headline = (
        f"Next major event: {next_event[:55]} in ~{next_days} days"
    )
    body = (
        f"The next major visitor demand event is {next_event}, approximately {next_days} days away. "
        f"Hotel rates during this period typically carry a {rate_premium}%+ premium over baseline ADR. "
        f"Ohana Fest (late September) is the marquee annual event: "
        f"$18.4M total destination spend, +$139 ADR lift, and 68% out-of-state attendees. "
        f"If you plan to attend, book accommodations immediately — "
        f"event-weekend inventory is typically exhausted 4–8 weeks in advance. "
        f"Consider properties 5–10 minutes inland for rate relief while still accessing all venues."
    )
    return dict(
        headline=headline, body=body, priority=1, horizon_days=next_days,
        data_sources="datafy_attribution_media_kpis",
        metric_basis={"days_to_next_event": next_days, "next_event": next_event,
                      "days_to_ohana": days_to_ohana,
                      "days_to_memorial": days_to_memorial},
    )


def gen_visitor_booking_timing(kpi: pd.DataFrame) -> dict:
    if kpi.empty:
        return {}
    q_lbl, season, _ = _seasonal_position()
    avg_rvp = kpi.tail(30)["revpar"].mean()

    if q_lbl == "Q1":
        advice = (
            "You are in the best booking window of the year. "
            "January–March is the softest demand period; rates are at their seasonal floor. "
            "Book immediately to lock in the lowest ADR of 2026."
        )
        urgency = "low — book now for best rates"
    elif q_lbl == "Q2":
        advice = (
            "Spring demand is building. "
            "Weekday trips in April–May still offer value, "
            "but weekend rates are already pricing toward summer levels. "
            "Book at least 3–4 weeks in advance."
        )
        urgency = "moderate — act within 2 weeks for best availability"
    elif q_lbl == "Q3":
        advice = (
            "Peak season. "
            "Most compression dates (80%+ occupancy) are already sold out or at premium BAR. "
            "Consider shoulder weekdays or book flex-cancel rates for any remaining openings."
        )
        urgency = "high — book immediately or consider alternative dates"
    else:
        advice = (
            "Fall shoulder offers good value, especially for October and early November. "
            "December holiday weekends book fast — secure those dates now. "
            "Early January is the absolute lowest-rate period in the calendar."
        )
        urgency = "low-moderate — holiday dates are filling; January is wide open"

    headline = f"Booking urgency: {urgency} | RevPAR signal: {_dollar(avg_rvp)}"
    body = f"{advice} Current market RevPAR is {_dollar(avg_rvp)}, reflecting {season} demand patterns. Dana Point's weekend-to-weekday rate spread is 15–30 percentage points — day-of-week flexibility is the single biggest lever for value travelers."
    return dict(
        headline=headline, body=body, priority=1, horizon_days=30,
        data_sources="kpi_daily_summary",
        metric_basis={"avg_revpar_30d": round(avg_rvp or 0, 2),
                      "season": season, "urgency": urgency},
    )


# ---------------------------------------------------------------------------
# Insight generators — Resident
# ---------------------------------------------------------------------------

def gen_resident_peak_alert(comp: pd.DataFrame) -> dict:
    q_lbl, season, _ = _seasonal_position()
    days_to_memorial = _days_to_event(5, 26)
    days_to_q3       = _days_to_event(7, 1)
    days_to_ohana    = _days_to_event(9, 26)
    days_to_labor    = _days_to_event(9, 1)

    upcoming = sorted([
        ("Memorial Day weekend", days_to_memorial),
        ("Summer peak season (Q3)", days_to_q3),
        ("Ohana Fest weekend", days_to_ohana),
        ("Labor Day weekend", days_to_labor),
    ], key=lambda x: x[1])
    next_name, next_days = upcoming[0]

    cq = f"{date.today().year}-{q_lbl}"
    cq_row = comp[comp["quarter"] == cq] if not comp.empty else pd.DataFrame()
    cq_80 = int(cq_row["days_above_80_occ"].iloc[0]) if not cq_row.empty else 0

    headline = (
        f"Heads-up: {next_name} in ~{next_days} days — "
        f"expect heavy beach, harbor & downtown traffic"
    )
    body = (
        f"The next major visitor-volume surge is {next_name}, approximately {next_days} days away. "
        f"Current quarter has already seen {cq_80} high-occupancy days (80%+). "
        f"During compression events, parking at Doheny State Beach, Salt Creek, "
        f"and the harbor area fills by mid-morning. "
        f"Residents are encouraged to use beach and harbor amenities on weekday mornings "
        f"during visitor-heavy weekends. "
        f"Q3 (July–Sept) historically delivers 34+ high-occupancy days — "
        f"peak beach and downtown crowding lasts the full summer season."
    )
    return dict(
        headline=headline, body=body, priority=1, horizon_days=next_days,
        data_sources="kpi_compression_quarterly",
        metric_basis={"days_to_next_peak": next_days, "next_peak": next_name,
                      "current_q_compression_days": cq_80},
    )


def gen_resident_economic_benefit(str_rev: pd.DataFrame, overview: dict) -> dict:
    total_rev_90d = str_rev["metric_value"].sum() if not str_rev.empty else None
    total_trips   = overview.get("total_trips") or 3_551_929   # Datafy 2025 baseline

    if total_rev_90d and total_rev_90d > 0:
        tot_90d  = total_rev_90d * 0.10
        tbid_90d = total_rev_90d * 0.0125
        headline = (
            f"Your community earned ~{_dollar(tot_90d)} in TOT + {_dollar(tbid_90d)} TBID "
            f"from tourism in the last 90 days"
        )
        body = (
            f"Visitor hotel spending over the past 90 days generated an estimated "
            f"{_dollar(tot_90d)} in Transient Occupancy Tax for the City of Dana Point's "
            f"general fund — funding parks, roads, public safety, and coastal programs. "
            f"An additional {_dollar(tbid_90d)} in TBID assessments funds destination marketing "
            f"that attracts the visitors who generate this revenue. "
            f"Tourism also directly supports local restaurants, retail, and service businesses. "
            f"{int(total_trips):,} annual visits to Dana Point underpin a visitor economy "
            f"that reduces the tax burden on residents."
        )
        basis = {"tot_90d": round(tot_90d, 2), "tbid_90d": round(tbid_90d, 2),
                 "total_annual_trips": int(total_trips)}
    else:
        headline = (
            f"{int(total_trips):,} annual visitor trips fund city services "
            f"through TOT and TBID assessments"
        )
        body = (
            f"Datafy data shows {int(total_trips):,} annual trips to Dana Point. "
            f"Each overnight visitor generates Transient Occupancy Tax (10% of room rate) "
            f"and TBID assessments that collectively fund city services and destination marketing. "
            f"Tourism is Dana Point's primary economic engine — "
            f"a healthy visitor economy means lower pressure on resident tax rates "
            f"and a well-funded parks and coastal program."
        )
        basis = {"total_annual_trips": int(total_trips)}

    return dict(
        headline=headline, body=body, priority=2, horizon_days=365,
        data_sources="fact_str_metrics,datafy_overview_kpis",
        metric_basis=basis,
    )


def gen_resident_quiet_windows(kpi: pd.DataFrame) -> dict:
    if kpi.empty:
        return {}
    q_lbl, season, _ = _seasonal_position()

    # Find recent low-occ days (< 65%) for guidance
    low_days = kpi[kpi["occ_pct"] < 65] if not kpi.empty else pd.DataFrame()
    pct_quiet = len(low_days) / max(len(kpi), 1) * 100

    if q_lbl == "Q1":
        window_advice = (
            "Right now is the quietest stretch of the year. "
            "Weekday mornings at Doheny, Salt Creek, and the harbor are uncrowded. "
            "January–mid-March offers resident-friendly access to all coastal amenities."
        )
    elif q_lbl == "Q2":
        window_advice = (
            "Weekday mornings (Tue–Thu) are still resident-friendly through May. "
            "Avoid beach areas on Memorial Day weekend and any warm-weather Friday afternoons."
        )
    elif q_lbl == "Q3":
        window_advice = (
            "Peak season — beaches and harbor are busiest July–September. "
            "Weekday mornings before 9 a.m. are the best resident access window. "
            "Ohana Fest weekend (late September) brings the highest single-weekend volume."
        )
    else:
        window_advice = (
            "Fall shoulder is transitioning back to resident-friendly conditions. "
            "October and November offer uncrowded beach access. "
            "Avoid Thanksgiving weekend and Christmas–New Year's as visitor volume spikes."
        )

    headline = (
        f"Resident access: {pct_quiet:.0f}% of recent days had <65% hotel occupancy — "
        f"{season} conditions"
    )
    body = (
        f"{window_advice} "
        f"Over the trailing period, {pct_quiet:.0f}% of days registered hotel occupancy "
        f"below 65% — a reliable proxy for lower beach and downtown crowding. "
        f"High-occupancy days (80%+) correlate with maximum visitor density at "
        f"parking lots, Doheny State Beach, Dana Point Harbor, and Lantern District dining."
    )
    return dict(
        headline=headline, body=body, priority=2, horizon_days=30,
        data_sources="kpi_daily_summary",
        metric_basis={"pct_days_below_65occ": round(pct_quiet, 1), "season": season},
    )


def gen_resident_annual_impact(overview: dict, comp: pd.DataFrame) -> dict:
    total_trips   = overview.get("total_trips") or 3_551_929
    overnight_pct = overview.get("overnight_trips_pct") or 59
    out_of_state  = overview.get("out_of_state_vd_pct") or 61

    # Sum all compression across known quarters
    total_80 = comp["days_above_80_occ"].sum() if not comp.empty else 0

    headline = (
        f"{int(total_trips):,} annual visits — {total_80} total compression days across "
        f"{len(comp)} quarters of STR data"
    )
    body = (
        f"Dana Point attracted {int(total_trips):,} total trips in the most recent annual "
        f"Datafy report, of which {overnight_pct:.0f}% were overnight stays. "
        f"{out_of_state:.0f}% of visitor-days came from out-of-state, "
        f"bringing net-new economic activity into the community. "
        f"Across all tracked quarters, Dana Point hotels recorded {total_80} days of "
        f"80%+ occupancy — each representing a day when visitor economic contribution "
        f"is at maximum. Tourism is the city's primary economic driver, "
        f"directly benefiting residents through TOT-funded city services and "
        f"employment in hospitality, dining, and retail."
    )
    return dict(
        headline=headline, body=body, priority=3, horizon_days=365,
        data_sources="datafy_overview_kpis,kpi_compression_quarterly",
        metric_basis={"total_trips": int(total_trips),
                      "overnight_pct": overnight_pct,
                      "out_of_state_pct": out_of_state,
                      "total_compression_days_80": int(total_80)},
    )


# ---------------------------------------------------------------------------
# Cross-dataset insight generators
# Insights that require BOTH STR + Datafy data — invisible to either alone
# ---------------------------------------------------------------------------

def gen_cross_feeder_value_gap(all_dmas: pd.DataFrame, kpi: pd.DataFrame) -> dict:
    """
    STR ADR + Datafy DMA spend efficiency.
    Identifies that high-volume drive markets underperform on spend per visit
    vs. fly markets — mis-allocation risk if campaign budgets track volume.
    """
    if all_dmas.empty or kpi.empty:
        return {}

    valid = all_dmas.dropna(subset=["spend_efficiency", "visitor_days_share_pct"])
    if valid.empty:
        return {}

    # Drive markets (LA, SD, Phoenix): high volume, low efficiency
    drive = valid[valid["dma"].isin(["Los Angeles", "San Diego", "Phoenix -Prescott"])]
    fly   = valid[valid["dma"].isin(
        ["San Francisco-Oak-San Jose", "Las Vegas", "Dallas-Ft. Worth",
         "New York", "Salt Lake City"]
    )]

    avg_drive_eff = drive["spend_efficiency"].mean() if not drive.empty else 0.84
    avg_fly_eff   = fly["spend_efficiency"].mean()   if not fly.empty  else 1.28

    # Top fly market by spend efficiency
    if not fly.empty:
        top_fly = fly.loc[fly["spend_efficiency"].idxmax()]
        top_fly_name = top_fly["dma"]
        top_fly_eff  = top_fly["spend_efficiency"]
        top_fly_avg  = top_fly.get("avg_spend_usd", 0)
    else:
        top_fly_name, top_fly_eff, top_fly_avg = "Las Vegas", 1.23, 378.0

    # LA stats
    la_row = valid[valid["dma"] == "Los Angeles"]
    la_share = float(la_row["visitor_days_share_pct"].iloc[0]) if not la_row.empty else 18.73
    la_eff   = float(la_row["spend_efficiency"].iloc[0])       if not la_row.empty else 0.84
    la_avg   = float(la_row["avg_spend_usd"].iloc[0])          if not la_row.empty and pd.notna(la_row["avg_spend_usd"].iloc[0]) else 205.0

    avg_adr = kpi["adr"].tail(30).mean()

    headline = (
        f"HIDDEN SIGNAL: LA drives {la_share:.0f}% of visits but spends ${la_avg:.0f}/day "
        f"({la_eff:.2f}× efficiency) — {top_fly_name} spends ${top_fly_avg:.0f}/day "
        f"({top_fly_eff:.2f}×)"
    )
    body = (
        f"Cross-referencing STR and Datafy reveals a critical campaign misallocation risk. "
        f"Los Angeles drives {la_share:.0f}% of visitor-days (the most of any DMA) but "
        f"spends only ${la_avg:.0f}/visitor-day ({la_eff:.2f}× spend-efficiency index). "
        f"By contrast, fly markets like {top_fly_name} average ${top_fly_avg:.0f}/visitor-day "
        f"({top_fly_eff:.2f}× efficiency) — {(top_fly_eff/la_eff - 1)*100:.0f}% more per trip. "
        f"Current ADR of {_dollar(avg_adr)} is closer to fly-market daily spend, "
        f"suggesting these visitors are paying premium rates. "
        f"Rebalancing campaign spend toward out-of-state fly markets (NYC, LV, Dallas) "
        f"generates more room revenue per attributed trip than volume-focused LA campaigns."
    )
    return dict(
        headline=headline, body=body, priority=1, horizon_days=90,
        data_sources="datafy_overview_dma,kpi_daily_summary",
        metric_basis={"la_visitor_share_pct": la_share, "la_avg_spend_usd": la_avg,
                      "la_spend_efficiency": round(la_eff, 3),
                      "top_fly_market": top_fly_name,
                      "top_fly_avg_spend_usd": top_fly_avg,
                      "top_fly_efficiency": round(top_fly_eff, 3),
                      "avg_adr_30d": round(avg_adr or 0, 2)},
    )


def gen_cross_daytrip_conversion(overview: dict, kpi: pd.DataFrame) -> dict:
    """
    Datafy day-trip % + STR room revenue.
    Quantifies the untapped overnight conversion opportunity hiding in day-trip volume.
    """
    if not overview or kpi.empty:
        return {}

    total_trips    = overview.get("total_trips") or 3_551_929
    day_trip_pct   = overview.get("day_trips_pct") or 40.57
    day_trips      = total_trips * (day_trip_pct / 100)

    avg_adr = kpi["adr"].tail(30).mean() or 351.0

    # If even 3% of day trips converted to 1-night stays
    conversion_3pct = day_trips * 0.03
    revenue_3pct    = conversion_3pct * avg_adr * 1          # 1 night avg

    # If 5% converted
    conversion_5pct = day_trips * 0.05
    revenue_5pct    = conversion_5pct * avg_adr

    headline = (
        f"HIDDEN OPPORTUNITY: {int(day_trips):,} annual day trips never touch a hotel — "
        f"3% conversion = {_dollar(revenue_3pct)} in incremental room revenue"
    )
    body = (
        f"Datafy records {int(day_trips):,} day trips ({day_trip_pct:.1f}% of {int(total_trips):,} total trips) "
        f"that generate zero hotel revenue. "
        f"At current ADR of {_dollar(avg_adr)}, converting just 3% ({int(conversion_3pct):,} trips) "
        f"to one-night stays would generate {_dollar(revenue_3pct)} in incremental room revenue. "
        f"A 5% conversion would yield {_dollar(revenue_5pct)}. "
        f"The highest-ROI conversion levers are: "
        f"(1) 'Stay the Night' packages for sunset dinner + beach activities, "
        f"(2) late check-out promotions targeting same-day bookers, "
        f"(3) whale watching / harbor experience packages with hotel bundling. "
        f"These require no new visitor acquisition — the audience is already on property."
    )
    return dict(
        headline=headline, body=body, priority=1, horizon_days=365,
        data_sources="datafy_overview_kpis,kpi_daily_summary",
        metric_basis={"annual_day_trips": int(day_trips),
                      "day_trip_pct": day_trip_pct,
                      "avg_adr": round(avg_adr, 2),
                      "revenue_at_3pct_conversion": round(revenue_3pct, 2),
                      "revenue_at_5pct_conversion": round(revenue_5pct, 2)},
    )


def gen_cross_weekday_los_gap(kpi_dow: pd.DataFrame, overview: dict) -> dict:
    """
    STR weekday vs. weekend occupancy gap + Datafy avg LOS.
    A short LOS concentrates revenue on Fri-Sat and artificially widens the midweek gap.
    The fix is LOS extension programs, not midweek discounting.
    """
    if kpi_dow.empty or not overview:
        return {}

    weekend = kpi_dow[kpi_dow["dow"].isin([4, 5])]   # Fri, Sat
    midweek = kpi_dow[kpi_dow["dow"].isin([1, 2])]   # Tue, Wed

    wknd_occ = weekend["occ_pct"].mean() if not weekend.empty else 72.5
    wkdy_occ = midweek["occ_pct"].mean() if not midweek.empty else 64.2
    occ_gap  = wknd_occ - wkdy_occ

    wknd_adr = weekend["adr"].mean() if not weekend.empty else 0
    wkdy_adr = midweek["adr"].mean() if not midweek.empty else 0
    adr_gap  = wknd_adr - wkdy_adr

    avg_los  = overview.get("avg_length_of_stay_days") or 2.0

    # If visitors extended by 0.5 nights avg (LOS 2.0 → 2.5):
    # Those extra nights land on midweek — uplift to midweek occupancy
    overnight_pct = overview.get("overnight_trips_pct") or 59.43
    overnight_trips = (overview.get("total_trips") or 3_551_929) * (overnight_pct / 100)
    # Extra 0.5 nights × overnight trips × wkdy_adr / 365 ≈ annual revenue lift
    los_extension_rev = overnight_trips * 0.5 * (wkdy_adr or 300) / 365

    headline = (
        f"HIDDEN SIGNAL: {occ_gap:.1f}pp weekend-weekday occ gap + {avg_los:.1f}-day avg LOS "
        f"= LOS extension worth {_dollar(los_extension_rev)} annually"
    )
    body = (
        f"STR data shows weekend occupancy at {wknd_occ:.1f}% vs. weekday {wkdy_occ:.1f}% "
        f"(only {occ_gap:.1f}pp gap). Datafy's {avg_los:.1f}-day average length of stay explains this: "
        f"most visitors arrive Friday and leave Sunday, leaving Monday–Thursday "
        f"${adr_gap:.0f} below weekend ADR. "
        f"The conventional fix — midweek discounting — sacrifices rate. "
        f"The higher-ROI lever is LOS extension: packages that reward 3+ night stays "
        f"(Fri–Mon), converting Sunday checkout to Monday checkout. "
        f"Every 0.5-day LOS increase across overnight visitors translates to "
        f"approximately {_dollar(los_extension_rev)} in annual room revenue "
        f"without discounting a single rate."
    )
    return dict(
        headline=headline, body=body, priority=1, horizon_days=90,
        data_sources="kpi_daily_summary,datafy_overview_kpis",
        metric_basis={"weekend_occ_pct": round(wknd_occ, 1),
                      "weekday_occ_pct": round(wkdy_occ, 1),
                      "occ_gap_pp": round(occ_gap, 1),
                      "weekend_adr": round(wknd_adr, 2),
                      "weekday_adr": round(wkdy_adr, 2),
                      "adr_gap_usd": round(adr_gap, 2),
                      "avg_los_days": avg_los,
                      "los_extension_revenue_est": round(los_extension_rev, 2)},
    )


def gen_cross_campaign_seasonality(comp: pd.DataFrame, channels: pd.DataFrame,
                                   web_kpis: dict, media_kpis: dict) -> dict:
    """
    STR compression by quarter + Datafy channel attribution.
    Tests whether campaigns are building shoulder demand (high ROI)
    or amplifying peak demand (low marginal ROI).
    """
    if comp.empty:
        return {}

    # Peak (Q3) vs. shoulder (Q1/Q4) compression ratio
    q3_rows  = comp[comp["quarter"].str.endswith("-Q3")]
    q1_rows  = comp[comp["quarter"].str.endswith("-Q1")]
    avg_q3   = q3_rows["days_above_80_occ"].mean() if not q3_rows.empty else 35
    avg_q1   = q1_rows["days_above_80_occ"].mean() if not q1_rows.empty else 4

    # Attribution: search has lowest conversion rate but highest trip volume
    top_channel = None
    top_trips   = 0
    low_rate_channel = None
    low_rate = 100.0
    if not channels.empty:
        top_ch_row = channels.loc[channels["attributable_trips_dest"].idxmax()]
        top_channel = top_ch_row["acquisition_channel"]
        top_trips   = int(top_ch_row["attributable_trips_dest"])
        lr_row = channels.loc[channels["attribution_rate_pct"].idxmin()]
        low_rate_channel = lr_row["acquisition_channel"]
        low_rate = float(lr_row["attribution_rate_pct"])

    web_period_end = web_kpis.get("report_period_end", "")
    campaign_month = ""
    if web_period_end:
        try:
            from datetime import datetime as dt
            campaign_month = dt.strptime(web_period_end, "%Y-%m-%d").strftime("%B %Y")
        except Exception:
            campaign_month = str(web_period_end)

    headline = (
        f"HIDDEN RISK: Q3 averages {avg_q3:.0f} compression days vs. Q1's {avg_q1:.0f} — "
        f"'{top_channel}' campaigns drive {top_trips:,} trips at only {low_rate:.2f}% conversion"
    )
    body = (
        f"Cross-referencing STR compression with Datafy attribution reveals campaign timing risk. "
        f"Q3 already averages {avg_q3:.0f} days above 80% occupancy — hotels are near capacity. "
        f"Q1 averages only {avg_q1:.0f} compression days — a wide demand gap. "
        f"Datafy shows '{top_channel}' drives the most attributed trips ({top_trips:,}) "
        f"but '{low_rate_channel}' channel has the lowest conversion rate ({low_rate:.2f}%). "
        f"If high-volume campaigns are concentrated in peak months (Q3 campaign period: {campaign_month}), "
        f"they generate marginal incremental stays — hotels are already full. "
        f"Shifting 20–30% of Q3 campaign spend to Q1–Q2 shoulder campaigns could generate "
        f"2–3× more incremental room nights per marketing dollar spent."
    )
    return dict(
        headline=headline, body=body, priority=2, horizon_days=90,
        data_sources="kpi_compression_quarterly,datafy_attribution_website_channels,datafy_attribution_website_kpis",
        metric_basis={"avg_q3_compression_80": round(avg_q3, 1),
                      "avg_q1_compression_80": round(avg_q1, 1),
                      "top_volume_channel": top_channel,
                      "top_channel_trips": top_trips,
                      "lowest_conversion_channel": low_rate_channel,
                      "lowest_conversion_rate_pct": low_rate},
    )


def gen_cross_oos_adr_premium(overview: dict, kpi: pd.DataFrame, all_dmas: pd.DataFrame) -> dict:
    """
    STR ADR YOY + Datafy out-of-state spend share.
    Out-of-state visitors represent premium willingness to pay but ADR growth
    may not be fully capturing it — especially vs. fly-market avg spend.
    """
    if not overview or kpi.empty:
        return {}

    oos_spend_pct = overview.get("out_of_state_spending_pct") or 60.41
    oos_vd_pct    = overview.get("out_of_state_vd_pct")       or 61.01
    # Spend-to-visitor ratio: if equal, index = 1.0; >1.0 = OOS spends above their share
    oos_spend_index = oos_spend_pct / oos_vd_pct if oos_vd_pct > 0 else 1.0

    adr_30        = kpi["adr"].tail(30).mean()
    adr_yoy_30    = kpi["adr_yoy"].tail(30).mean()

    # Highest avg_spend DMA (proxy for max willingness to pay in market)
    max_spend_dma = ""
    max_spend_usd = 0.0
    if not all_dmas.empty:
        valid = all_dmas.dropna(subset=["avg_spend_usd"])
        if not valid.empty:
            top = valid.loc[valid["avg_spend_usd"].idxmax()]
            max_spend_dma = top["dma"]
            max_spend_usd = float(top["avg_spend_usd"])

    # If top fly market avg spend is $X/day and ADR is $Y, capture ratio
    capture_ratio = (adr_30 / max_spend_usd * 100) if max_spend_usd > 0 else 0

    headline = (
        f"HIDDEN GAP: OOS visitors spend {oos_spend_index:.2f}× their visitor share "
        f"but ADR YOY only {_pct(adr_yoy_30)} — "
        f"{max_spend_dma} avg ${max_spend_usd:.0f}/day vs ADR {_dollar(adr_30)}"
    )
    body = (
        f"Out-of-state visitors account for {oos_vd_pct:.1f}% of visitor-days "
        f"but {oos_spend_pct:.1f}% of total destination spending "
        f"({oos_spend_index:.2f}× spend-to-visit ratio). "
        f"Top fly markets like {max_spend_dma} average ${max_spend_usd:.0f}/visitor-day — "
        f"{'above' if max_spend_usd > (adr_30 or 0) else 'near'} the current ADR of {_dollar(adr_30)}. "
        f"Yet ADR year-over-year growth is only {_pct(adr_yoy_30)}. "
        f"This gap signals that premium out-of-state demand is not being fully captured through rate. "
        f"Tactics to close the gap: tiered rate packages for fly markets, "
        f"premium experience bundles (whale watching + harbor + premium room), "
        f"and direct booking incentives that bypass OTA commission dilution."
    )
    return dict(
        headline=headline, body=body, priority=2, horizon_days=60,
        data_sources="datafy_overview_kpis,datafy_overview_dma,kpi_daily_summary",
        metric_basis={"oos_visitor_days_pct": oos_vd_pct,
                      "oos_spending_pct": oos_spend_pct,
                      "oos_spend_index": round(oos_spend_index, 3),
                      "adr_30d": round(adr_30 or 0, 2),
                      "adr_yoy_30d": round(adr_yoy_30 or 0, 2),
                      "top_spend_dma": max_spend_dma,
                      "top_dma_avg_spend_usd": max_spend_usd},
    )


def gen_cross_compression_daytrip(comp: pd.DataFrame, overview: dict) -> dict:
    """
    STR compression days + Datafy day-trip %.
    On 80%+ occupancy days, 40% of all visitors are non-hotel day trippers —
    they consume parking, beaches, and services without generating hotel revenue.
    This is the hidden infrastructure cost invisible in STR data alone.
    """
    if comp.empty or not overview:
        return {}

    day_trip_pct  = overview.get("day_trips_pct") or 40.57
    total_trips   = overview.get("total_trips") or 3_551_929

    # Total compression days across all quarters
    total_80 = int(comp["days_above_80_occ"].sum())
    total_90 = int(comp["days_above_90_occ"].sum())

    # On a compression day (80%+ occ), hotel demand is near max
    # But day trippers add ~68% more visitors on top (40.57% day / 59.43% overnight)
    day_multiplier = day_trip_pct / (100 - day_trip_pct)  # day trippers per hotel guest
    # Annotated: if overnight visitors = 100, day trippers = ~68

    # Q3 compression (worst case)
    q3_rows = comp[comp["quarter"].str.endswith("-Q3")]
    worst_q3_80 = int(q3_rows["days_above_80_occ"].max()) if not q3_rows.empty else 37

    headline = (
        f"HIDDEN COST: On {total_80} compression days, day trippers add "
        f"{day_multiplier:.1f}× hotel-guest volume — "
        f"invisible in STR data, visible in parking & beach data"
    )
    body = (
        f"STR data shows {total_80} days across all quarters where hotel occupancy exceeded 80%. "
        f"Datafy reveals that {day_trip_pct:.1f}% of all Dana Point visits are day trips — "
        f"meaning for every hotel guest, there are approximately {day_multiplier:.1f} additional "
        f"day visitors consuming parking, beaches, and City services on those peak days. "
        f"During Q3 peak ({worst_q3_80} days at 80%+ occupancy), total visitor density is "
        f"{(1 + day_multiplier):.1f}× what STR data alone suggests. "
        f"This has three implications: "
        f"(1) City infrastructure must be planned for {(1 + day_multiplier):.1f}× hotel capacity, "
        f"(2) the day-trip audience is a conversion target for overnight revenue, "
        f"(3) STR metrics alone undercount the true economic activity on compression days."
    )
    return dict(
        headline=headline, body=body, priority=2, horizon_days=90,
        data_sources="kpi_compression_quarterly,datafy_overview_kpis",
        metric_basis={"total_compression_days_80": total_80,
                      "total_compression_days_90": total_90,
                      "day_trip_pct": day_trip_pct,
                      "day_tripper_multiplier": round(day_multiplier, 2),
                      "worst_q3_compression_days": worst_q3_80},
    )


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== compute_insights.py ===\n")
    conn = get_connection()

    try:
        cur = conn.cursor()
        ensure_tables(cur)
        conn.commit()
        print("  Tables ensured: insights_daily, table_relationships")

        # ── Seed relationships ───────────────────────────────────────────────
        rel_count = seed_relationships(cur)
        conn.commit()
        print(f"  Seeded {rel_count} table relationships")

        # ── Load data snapshots ──────────────────────────────────────────────
        print(f"\n  Loading data for {TODAY} ...")
        kpi_recent  = load_kpi_recent(conn, days=90)
        kpi_all     = load_kpi_all(conn)
        kpi_dow     = load_kpi_with_dow(conn)
        comp        = load_compression(conn)
        str_rev     = load_str_revenue(conn, days=90)
        overview    = load_datafy_overview(conn)
        top_dmas    = load_top_dmas(conn)
        all_dmas    = load_all_dmas(conn)
        spending    = load_spending_categories(conn)
        media_kpis  = load_media_kpis(conn)
        web_kpis    = load_website_kpis(conn)
        channels    = load_attribution_channels(conn)

        print(f"  KPI rows: {len(kpi_recent)} (90d) | {len(kpi_all)} (all)")
        print(f"  Compression quarters: {len(comp)}")
        print(f"  STR revenue rows (90d): {len(str_rev)}")
        print(f"  Datafy overview KPIs: {'loaded' if overview else 'empty'}")
        print(f"  All DMA rows: {len(all_dmas)} | Attribution channels: {len(channels)}")

        # ── Generate insights ────────────────────────────────────────────────
        generators = {
            # DMO
            ("dmo", "demand_trend"):      lambda: gen_dmo_demand_trend(kpi_recent, comp),
            ("dmo", "tbid_projection"):   lambda: gen_dmo_tbid_projection(kpi_recent, str_rev),
            ("dmo", "feeder_market"):     lambda: gen_dmo_feeder_market(top_dmas, web_kpis, media_kpis),
            ("dmo", "compression_outlook"): lambda: gen_dmo_compression_outlook(comp, kpi_recent),
            ("dmo", "event_roi"):         lambda: gen_dmo_event_roi(media_kpis, web_kpis),

            # City
            ("city", "tot_revenue"):      lambda: gen_city_tot_revenue(str_rev, kpi_recent),
            ("city", "infrastructure"):   lambda: gen_city_infrastructure(comp),
            ("city", "visitor_profile"):  lambda: gen_city_visitor_profile(overview),
            ("city", "economic_impact"):  lambda: gen_city_economic_impact(overview, spending),

            # Visitor
            ("visitor", "best_value"):    lambda: gen_visitor_best_value(kpi_all),
            ("visitor", "rate_outlook"):  lambda: gen_visitor_rate_outlook(kpi_recent),
            ("visitor", "upcoming_events"): lambda: gen_visitor_upcoming_events(media_kpis),
            ("visitor", "booking_timing"): lambda: gen_visitor_booking_timing(kpi_recent),

            # Resident
            ("resident", "peak_alert"):   lambda: gen_resident_peak_alert(comp),
            ("resident", "economic_benefit"): lambda: gen_resident_economic_benefit(str_rev, overview),
            ("resident", "quiet_windows"): lambda: gen_resident_quiet_windows(kpi_all),
            ("resident", "annual_impact"): lambda: gen_resident_annual_impact(overview, comp),

            # Cross-dataset: insights only visible by joining STR + Datafy
            ("cross", "feeder_value_gap"):    lambda: gen_cross_feeder_value_gap(all_dmas, kpi_recent),
            ("cross", "daytrip_conversion"):  lambda: gen_cross_daytrip_conversion(overview, kpi_recent),
            ("cross", "weekday_los_gap"):     lambda: gen_cross_weekday_los_gap(kpi_dow, overview),
            ("cross", "campaign_seasonality"): lambda: gen_cross_campaign_seasonality(comp, channels, web_kpis, media_kpis),
            ("cross", "oos_adr_premium"):     lambda: gen_cross_oos_adr_premium(overview, kpi_recent, all_dmas),
            ("cross", "compression_daytrip"): lambda: gen_cross_compression_daytrip(comp, overview),
        }

        inserted = 0
        for (audience, category), fn in generators.items():
            try:
                result = fn()
                if not result:
                    print(f"  [skip] {audience}/{category} — no data")
                    continue
                upsert_insight(
                    cur,
                    audience=audience,
                    category=category,
                    headline=result["headline"],
                    body=result["body"],
                    metric_basis=result.get("metric_basis", {}),
                    priority=result.get("priority", 5),
                    horizon_days=result.get("horizon_days", 30),
                    data_sources=result.get("data_sources", ""),
                )
                inserted += 1
                print(f"  [OK] {audience}/{category}")
            except Exception as exc:
                print(f"  [WARN] {audience}/{category} failed: {exc}")

        conn.commit()
        print(f"\n  Total insights upserted: {inserted} for {TODAY}")

        # ── Log to load_log ──────────────────────────────────────────────────
        cur.execute(
            "INSERT INTO load_log (source, grain, file_name, rows_inserted) "
            "VALUES (?, ?, ?, ?)",
            ("INSIGHTS", "daily", "compute_insights.py", inserted),
        )
        conn.commit()
        print(f"  Logged {inserted} rows to load_log")
        print("\nDone.\n")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
