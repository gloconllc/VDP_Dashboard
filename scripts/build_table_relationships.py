"""
build_table_relationships.py
─────────────────────────────
Rebuilds the complete table_relationships map in analytics.sqlite.

Run automatically as the LAST step in run_pipeline.py after every data load,
and manually whenever a new table or data source is added.

STANDARD PROCESS — always follow when adding new data:
  1. Put raw files in data/<source_name>/  (CSV, Excel, PDF)
  2. Write or update scripts/load_<source>.py to parse → DB table(s)
  3. Add new table entries to the RELATIONSHIPS list below with proper
     join keys and relationship_type ('derived_from', 'enriches',
     'cross_ref', 'context', 'cross_platform')
  4. Add the loader to STEPS in run_pipeline.py
  5. Re-run: python scripts/run_pipeline.py
  6. This script runs last and refreshes all relationships automatically.

Relationship types:
  derived_from   — table_b is computed/derived from table_a
  enriches       — table_a adds detail to table_b's parent records
  cross_ref      — tables share a dimension (time, geography, market)
  context        — table_a provides macro context for interpreting table_b
  cross_platform — same concept measured across different platforms/sources
"""

import os
import sqlite3
from datetime import datetime

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH     = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")


# ─────────────────────────────────────────────────────────────────────────────
# MASTER RELATIONSHIP REGISTRY
# Add new entries here whenever a new table or data source is added.
# Format: (table_a, table_b, relationship_type, join_key, description)
# ─────────────────────────────────────────────────────────────────────────────

RELATIONSHIPS: list[tuple[str, str, str, str, str]] = [

    # ── STR Core → KPI Chain ───────────────────────────────────────────────
    ("fact_str_metrics",         "kpi_daily_summary",            "derived_from",  "as_of_date",
     "kpi_daily_summary is computed from fact_str_metrics (pivot long→wide, occ conversion, YOY delta)"),

    ("fact_str_response_markets", "fact_str_metrics",            "same_source",   "grain,as_of_date",
     "fact_str_response_markets is the property-roster companion to fact_str_metrics — same STR files, different granularity (roster vs. aggregate performance)"),

    ("fact_str_response_markets", "kpi_daily_summary",           "context",       "as_of_date",
     "fact_str_response_markets property roster provides comp-set context for kpi_daily_summary performance figures"),

    ("fact_str_group_metrics",    "fact_str_metrics",            "same_source",   "grain,as_of_date",
     "fact_str_group_metrics (multi-seg: OCC/ADR/RevPAR/Supply/Demand/Revenue by segment and comp market) derived from same STR exports as fact_str_metrics"),

    ("fact_str_group_metrics",    "kpi_daily_summary",           "context",       "as_of_date",
     "fact_str_group_metrics competitive market performance provides comp-set context alongside kpi_daily_summary Dana Point aggregates"),

    ("fact_str_group_metrics",    "fact_str_response_markets",   "related",       "grain,as_of_date,market",
     "fact_str_group_metrics (performance by market/segment) joins to fact_str_response_markets (property roster by market) on grain+as_of_date+market"),

    ("str_holiday_calendar",      "fact_str_group_metrics",      "context",       "as_of_date",
     "str_holiday_calendar holiday shift (TY vs LY) explains YOY variance in fact_str_group_metrics weekly performance"),

    ("str_holiday_calendar",      "kpi_daily_summary",           "context",       "as_of_date",
     "str_holiday_calendar holiday alignment context for kpi_daily_summary YOY delta interpretation"),

    ("fact_str_metrics",         "kpi_compression_quarterly",    "derived_from",  "as_of_date→quarter",
     "Compression quarters derived by counting fact_str_metrics days ≥80%/90% occ per quarter"),

    ("fact_str_metrics",         "load_log",                     "audited_by",    "run_at",
     "Every STR ingest is logged to load_log for ETL audit trail"),

    ("kpi_daily_summary",        "kpi_compression_quarterly",    "derived_from",  "as_of_date→quarter",
     "Compression quarterly counts aggregated from kpi_daily_summary.occ_pct thresholds"),

    ("kpi_daily_summary",        "insights_daily",               "derived_from",  "as_of_date",
     "insights_daily forward-looking rows are generated from kpi_daily_summary trends"),

    ("kpi_daily_summary",        "load_log",                     "audited_by",    "run_at",
     "compute_kpis.py logs its run to load_log after each rebuild"),

    # ── STR × Datafy Cross-Dataset ─────────────────────────────────────────
    ("fact_str_metrics",         "datafy_overview_kpis",         "cross_ref",     "report_period",
     "STR hotel metrics (Layer 1) align with Datafy visitor economy KPIs on the same reporting period"),

    ("fact_str_metrics",         "datafy_attribution_media_kpis","cross_ref",     "report_period",
     "STR RevPAR/ADR trends validate media campaign attribution periods from Datafy"),

    ("kpi_daily_summary",        "datafy_overview_kpis",         "cross_ref",     "time_period",
     "KPI daily summary overlaps with Datafy annual visitor KPIs for the same destination"),

    ("kpi_daily_summary",        "datafy_overview_dma",          "cross_ref",     "time_period",
     "KPI ADR by day cross-referenced with Datafy DMA feeder market spend efficiency"),

    ("kpi_compression_quarterly","datafy_overview_kpis",         "cross_ref",     "time_period",
     "Compression quarters compared against Datafy overnight vs. day-trip ratios"),

    ("kpi_compression_quarterly","datafy_attribution_website_channels", "cross_ref", "report_period",
     "Compression windows aligned with website attribution channels to identify campaign seasonality"),

    # ── Datafy Overview Hierarchy ──────────────────────────────────────────
    ("datafy_overview_kpis",     "datafy_overview_dma",          "enriches",      "report_period",
     "DMA feeder market breakdown enriches the annual visitor KPIs"),

    ("datafy_overview_kpis",     "datafy_overview_demographics", "enriches",      "report_period",
     "Visitor demographics (age, HHI, travel party) enrich the visitor overview KPIs"),

    ("datafy_overview_kpis",     "datafy_overview_category_spending", "enriches", "report_period",
     "Spending by category (accommodation, dining, retail) enriches visitor spend KPIs"),

    ("datafy_overview_kpis",     "datafy_overview_cluster_visitation", "enriches","report_period",
     "Visitation by area cluster (beach, downtown, etc.) enriches geographic demand picture"),

    ("datafy_overview_kpis",     "datafy_overview_airports",     "enriches",      "report_period",
     "Origin airports by passenger share enriches the visitor origin profile"),

    ("datafy_overview_kpis",     "insights_daily",               "derived_from",  "report_period",
     "compute_insights.py reads Datafy overview KPIs to generate DMO/city audience insights"),

    # ── Datafy Attribution Hierarchy ───────────────────────────────────────
    ("datafy_attribution_website_kpis", "datafy_attribution_website_channels", "enriches", "report_period",
     "Channel-level breakdown (SEO, paid, email) enriches website attribution KPI summary"),

    ("datafy_attribution_website_kpis", "datafy_attribution_website_dma",      "enriches", "report_period",
     "DMA-level website attribution enriches overall website attribution KPIs"),

    ("datafy_attribution_website_kpis", "datafy_attribution_website_top_markets","enriches","report_period",
     "Top feeder markets from website attribution enrich the attribution KPI summary"),

    ("datafy_attribution_website_kpis", "datafy_attribution_website_clusters",  "enriches","report_period",
     "Cluster-level website attribution (beach, downtown) enriches overall website KPIs"),

    ("datafy_attribution_website_kpis", "datafy_attribution_website_demographics","enriches","report_period",
     "Visitor demographics from website attribution enriches the attribution KPI summary"),

    ("datafy_attribution_media_kpis",   "datafy_attribution_media_top_markets", "enriches","report_period",
     "Top feeder markets from media attribution enrich media campaign KPIs"),

    # ── Datafy DMA Cross-References ────────────────────────────────────────
    ("datafy_overview_dma",      "datafy_attribution_website_dma","cross_ref",    "dma",
     "Same DMA markets appear in both organic visitor data and website attribution — compare efficiency"),

    ("datafy_overview_dma",      "datafy_attribution_media_top_markets","cross_ref","market_name",
     "Top media markets cross-referenced with DMA organic visitor share for campaign ROI analysis"),

    # ── Datafy Social ──────────────────────────────────────────────────────
    ("datafy_social_traffic_sources", "datafy_social_audience_overview", "enriches","loaded_at",
     "Traffic source breakdown enriches the GA4 website audience overview"),

    ("datafy_social_top_pages",  "datafy_social_audience_overview",     "enriches","loaded_at",
     "Top pages by view count enriches the website audience overview"),

    # ── CoStar Market Intelligence ─────────────────────────────────────────
    ("costar_market_snapshot",   "fact_str_metrics",             "cross_ref",     "year",
     "CoStar South OC market snapshot benchmarks VDP portfolio (fact_str_metrics) against full market"),

    ("costar_market_snapshot",   "kpi_daily_summary",            "cross_ref",     "year",
     "CoStar annual market occ/ADR/RevPAR benchmarks KPI daily summary for premium/discount analysis"),

    ("costar_monthly_performance","costar_market_snapshot",      "enriches",      "report_period",
     "Monthly CoStar performance trend enriches the annual market snapshot"),

    ("costar_monthly_performance","kpi_daily_summary",           "cross_ref",     "month_year",
     "CoStar monthly market data cross-referenced with STR-derived KPI daily summary"),

    ("costar_annual_performance", "costar_market_snapshot",      "enriches",      "year_label",
     "Annual historical actuals and forecasts (2016–2030) enrich the market snapshot"),

    ("costar_annual_performance", "kpi_daily_summary",           "cross_ref",     "year",
     "CoStar annual performance cross-referenced with KPI daily summary to validate trend alignment"),

    ("costar_chain_scale_breakdown","costar_market_snapshot",    "enriches",      "year",
     "Chain scale breakdown (Luxury, Upper Upscale, Upscale, Independent) enriches market snapshot"),

    ("costar_chain_scale_breakdown","fact_str_metrics",          "cross_ref",     "year",
     "Chain scale ADR/occ benchmarks cross-referenced with VDP portfolio STR metrics"),

    ("costar_supply_pipeline",   "costar_market_snapshot",       "enriches",      "market",
     "Hotel supply pipeline (rooms under construction, planned) enriches market snapshot supply data"),

    ("costar_supply_pipeline",   "fact_str_metrics",             "cross_ref",     "projected_open_date",
     "Pipeline opening dates cross-referenced with STR demand trends to model supply impact"),

    ("costar_competitive_set",   "costar_market_snapshot",       "enriches",      "report_date",
     "Individual property MPI/ARI/RGI rankings enrich the South OC market snapshot"),

    ("costar_competitive_set",   "fact_str_metrics",             "cross_ref",     "report_date",
     "Comp set property performance cross-referenced with VDP portfolio STR metrics for benchmarking"),

    ("costar_profitability",     "costar_market_snapshot",       "enriches",      "report_date",
     "Hotel profitability data (GOP, NOI, labor cost) enriches the CoStar market snapshot"),

    ("costar_profitability",     "kpi_daily_summary",            "cross_ref",     "year",
     "Profitability metrics (GOP margin, labor cost per room) contextualize RevPAR performance"),

    # ── Visit California Context ───────────────────────────────────────────
    ("visit_ca_travel_indicators","kpi_daily_summary",           "context",       "report_period",
     "Monthly CA travel indicators (air pax, hotel KPIs, spending, gas) contextualize VDP STR performance"),

    ("visit_ca_travel_indicators","fact_str_metrics",            "context",       "report_period",
     "CA statewide hotel OCC/ADR/RevPAR travel indicators benchmark against VDP local STR metrics"),

    ("visit_ca_travel_indicators","eia_gas_prices",              "cross_ref",     "report_period",
     "CA gas price travel indicator cross-referenced with EIA weekly gas price series for drive-market analysis"),

    ("visit_ca_travel_indicators","visit_ca_airport_traffic",    "enriches",      "report_period",
     "Monthly air passenger indicators enrich airport traffic counts with statewide demand context"),

    ("visit_ca_travel_indicators","visit_ca_intl_arrivals",      "cross_ref",     "report_period",
     "CA international air arrivals indicator cross-referenced with NTTO monthly international arrivals table"),

    ("visit_ca_intl_market_profiles","datafy_overview_dma",      "cross_ref",     "country",
     "International market visitor profiles cross-referenced with Datafy DMA feeder market spending data"),

    ("visit_ca_intl_market_profiles","visit_ca_intl_arrivals",   "enriches",      "country",
     "Market-level visitor profiles (spend/LOS/activities) enrich raw international arrivals counts"),

    ("visit_ca_intl_market_profiles","fact_str_metrics",         "context",       "report_year",
     "International market spend-per-visitor context informs ADR premium opportunity in STR metrics"),

    ("visit_ca_intl_market_profiles","insights_daily",           "derived_from",  "report_year",
     "International market profiles feed compute_insights.py OOS visitor premium and feeder value gap insights"),

    ("visit_ca_lodging_monthly",   "kpi_daily_summary",          "cross_ref",     "report_period",
     "CA statewide monthly lodging actuals (STR) cross-referenced with VDP local KPI daily summary"),

    ("visit_ca_lodging_monthly",   "fact_str_metrics",           "cross_ref",     "report_period",
     "CA regional lodging OCC/ADR/RevPAR monthly benchmarks against VDP portfolio STR metrics"),

    ("visit_ca_lodging_monthly",   "visit_ca_lodging_forecast",  "enriches",      "region",
     "Monthly lodging actuals enrich the annual lodging forecast with realized performance by region"),

    ("visit_ca_lodging_monthly",   "costar_market_snapshot",     "cross_ref",     "report_period",
     "CA lodging monthly actuals cross-referenced with CoStar South OC market snapshot for benchmarking"),

    ("visit_ca_travel_forecast", "kpi_daily_summary",            "context",       "year",
     "CA statewide travel demand forecast provides macro demand context for VDP KPI interpretation"),

    ("visit_ca_travel_forecast", "fact_str_metrics",             "context",       "year",
     "CA travel forecast contextualize STR demand trends at state vs. destination level"),

    ("visit_ca_lodging_forecast","fact_str_metrics",             "context",       "year",
     "CA lodging forecast (occ/ADR/RevPAR) sets state benchmark for VDP portfolio metrics"),

    ("visit_ca_lodging_forecast","kpi_daily_summary",            "context",       "year",
     "CA statewide lodging KPIs provide benchmark context for VDP market KPI daily summary"),

    ("visit_ca_airport_traffic", "datafy_overview_airports",     "cross_ref",     "airport",
     "CA airport passenger traffic cross-referenced with Datafy visitor origin airports"),

    ("visit_ca_airport_traffic", "fact_str_metrics",             "context",       "month",
     "JWA/LAX/SNA passenger counts contextualize hotel demand — fly-market feeder signal"),

    ("visit_ca_intl_arrivals",   "datafy_overview_dma",          "context",       "year",
     "International arrivals to CA provide context for OOS visitor share in Datafy DMA data"),

    ("visit_ca_intl_arrivals",   "costar_market_snapshot",       "context",       "year",
     "International travel trends to CA provide macro context for CoStar market performance"),

    # ── Zartico Historical Reference ───────────────────────────────────────
    ("zartico_kpis",             "datafy_overview_kpis",         "cross_ref",     "report_period",
     "Zartico historical visitor KPIs (Jun 2025 snapshot) vs. Datafy current KPIs — growth story"),

    ("zartico_kpis",             "fact_str_metrics",             "cross_ref",     "report_period",
     "Zartico device/spend KPIs cross-referenced with STR hotel metrics for the same period"),

    ("zartico_markets",          "zartico_kpis",                 "enriches",      "report_date",
     "Top visitor origin markets enrich the Zartico visitor economy KPIs"),

    ("zartico_markets",          "datafy_overview_dma",          "cross_ref",     "market_name",
     "Zartico historical market rankings cross-referenced with current Datafy DMA data — trend shifts"),

    ("zartico_spending_monthly", "zartico_kpis",                 "enriches",      "month_str",
     "Monthly visitor spend (vs. benchmark) enriches the Zartico annual KPIs"),

    ("zartico_spending_monthly", "weather_monthly",              "context",       "month",
     "Monthly visitor spend cross-referenced with weather to identify climate/spend correlation"),

    ("zartico_spending_monthly", "kpi_daily_summary",            "cross_ref",     "month",
     "Historical visitor spend trend cross-referenced with KPI daily summary for same months"),

    ("zartico_lodging_kpis",     "zartico_kpis",                 "enriches",      "report_date",
     "Hotel/STVR lodging summary (occ, ADR, LOS, day-of-week pattern) enriches visitor KPIs"),

    ("zartico_lodging_kpis",     "fact_str_metrics",             "cross_ref",     "report_period",
     "Zartico lodging KPIs (historical) cross-referenced with current STR metrics for trend validation"),

    ("zartico_overnight_trend",  "zartico_kpis",                 "enriches",      "month_str",
     "Monthly overnight visitor % trend enriches the Zartico visitor economy KPIs"),

    ("zartico_overnight_trend",  "kpi_daily_summary",            "cross_ref",     "month_str",
     "Historical overnight visitor % trend cross-referenced with current KPI summary"),

    ("zartico_movement_monthly", "zartico_kpis",                 "enriches",      "month_str",
     "Visitor-to-resident ratio by month enriches the Zartico visitor economy KPIs"),

    ("zartico_movement_monthly", "weather_monthly",              "context",       "month",
     "Visitor/resident ratio by month cross-referenced with weather to understand seasonality"),

    ("zartico_event_impact",     "zartico_kpis",                 "enriches",      "report_date",
     "Event period vs. baseline spend change enriches overall Zartico KPIs"),

    ("zartico_event_impact",     "vdp_events",                   "cross_ref",     "event_name",
     "Zartico event impact data matched to VDP events calendar for ROI attribution"),

    ("zartico_future_events_summary","vdp_events",               "cross_ref",     "report_date",
     "Zartico YOY event/attendee growth cross-referenced with VDP events calendar"),

    # ── VDP Events ─────────────────────────────────────────────────────────
    ("vdp_events",               "kpi_daily_summary",            "cross_ref",     "event_date",
     "VDP events matched to KPI daily summary dates to identify event-driven occupancy spikes"),

    ("vdp_events",               "fact_str_metrics",             "cross_ref",     "event_date",
     "VDP event dates matched to STR daily metrics for event lift vs. baseline analysis"),

    ("vdp_events",               "zartico_event_impact",         "cross_ref",     "event_name",
     "VDP events calendar linked to Zartico event impact measurements for ROI validation"),

    ("vdp_events",               "insights_daily",               "cross_ref",     "event_date",
     "VDP upcoming events feed into insights_daily forward-looking recommendations"),

    # ── Later Social Media Hierarchy ───────────────────────────────────────
    ("later_ig_profile_growth",  "kpi_daily_summary",            "context",       "data_date",
     "Instagram follower/engagement growth provides social demand signal alongside hotel KPIs"),

    ("later_ig_profile_growth",  "later_fb_profile_growth",      "cross_platform","data_date",
     "Instagram and Facebook follower growth compared on same date for cross-platform social audit"),

    ("later_ig_profile_growth",  "later_tk_profile_growth",      "cross_platform","data_date",
     "Instagram and TikTok growth compared for cross-platform audience building analysis"),

    ("later_ig_posts",           "later_ig_profile_growth",      "enriches",      "posted_at",
     "Individual post performance enriches Instagram profile growth trend"),

    ("later_ig_stories",         "later_ig_posts",               "enriches",      "posted_at",
     "Story performance enriches overall Instagram content performance picture"),

    ("later_ig_reels",           "later_ig_profile_growth",      "enriches",      "posted_at",
     "Reel performance (video) enriches Instagram profile growth data"),

    ("later_ig_reels",           "later_ig_posts",               "cross_platform","posted_at",
     "Reels vs. static posts compared on same date to identify highest-performing content format"),

    ("later_ig_audience_demographics","later_ig_profile_growth", "enriches",      "data_date",
     "Audience demographics (gender, age) enrich Instagram follower profile data"),

    ("later_ig_audience_demographics","datafy_overview_demographics","cross_ref", "—",
     "Instagram audience demographics cross-referenced with Datafy verified visitor demographics"),

    ("later_ig_audience_engagement","later_ig_profile_growth",   "enriches",      "data_date",
     "Daily engagement rate data enriches Instagram profile growth trends"),

    ("later_ig_audience_engagement","later_ig_audience_demographics","enriches",  "data_date",
     "Engagement patterns by date enriches audience demographic understanding"),

    ("later_ig_location",        "later_ig_profile_growth",      "enriches",      "data_date",
     "Geographic location of Instagram audience enriches profile growth analysis"),

    ("later_fb_posts",           "later_fb_profile_growth",      "enriches",      "posted_at",
     "Facebook post performance enriches Facebook page profile growth trends"),

    ("later_fb_posts",           "later_ig_posts",               "cross_platform","posted_at",
     "Facebook and Instagram posts compared for cross-platform content strategy"),

    ("later_fb_profile_interactions","later_fb_profile_growth",  "enriches",      "data_date",
     "Facebook page interactions enriches Facebook profile growth data"),

    ("later_tk_interactions",    "later_tk_profile_growth",      "enriches",      "data_date",
     "TikTok engagement interactions enriches TikTok profile growth trends"),

    ("later_tk_audience_demographics","later_tk_profile_growth", "enriches",      "data_date",
     "TikTok audience demographics enriches TikTok profile growth analysis"),

    ("later_tk_audience_engagement","later_tk_profile_growth",   "enriches",      "data_date",
     "TikTok daily engagement rate data enriches TikTok profile growth trends"),

    # ── External Economic Signals ──────────────────────────────────────────
    ("bls_employment_monthly",   "kpi_daily_summary",            "context",       "year_month",
     "OC hospitality employment (BLS) provides labor market context for hotel performance trends"),

    ("bls_employment_monthly",   "fact_str_metrics",             "context",       "year_month",
     "Hospitality employment levels contextualize STR demand trends (employment ↔ hotel demand)"),

    ("bls_employment_monthly",   "weather_monthly",              "cross_ref",     "year_month",
     "Employment and weather cross-referenced to identify seasonal labor/demand co-movement"),

    ("bls_employment_monthly",   "costar_market_snapshot",       "context",       "year",
     "OC hospitality employment trends provide labor market context for CoStar market data"),

    ("google_trends_weekly",     "kpi_daily_summary",            "context",       "week_date",
     "Google search interest for Dana Point keywords provides leading demand signal for KPI trends"),

    ("google_trends_weekly",     "fact_str_metrics",             "context",       "week_date",
     "Search demand trend (Google) leads STR hotel demand by 2–4 weeks — forward-looking signal"),

    ("google_trends_weekly",     "vdp_events",                   "context",       "event_date",
     "Google search spikes around event dates cross-referenced with VDP events calendar"),

    ("weather_monthly",          "kpi_daily_summary",            "context",       "year_month",
     "Coastal weather (avg temp, beach day score) provides seasonal context for hotel KPIs"),

    ("weather_monthly",          "fact_str_metrics",             "context",       "year_month",
     "Weather conditions correlated with STR hotel demand — beach day score vs. occupancy"),

    ("weather_monthly",          "bls_employment_monthly",       "cross_ref",     "year_month",
     "Weather and employment cross-referenced to separate seasonal labor from demand effects"),

    ("eia_gas_prices",           "kpi_daily_summary",            "context",       "week_date",
     "CA retail gas prices (EIA) signal drive-market demand changes — inverse correlation with occ"),

    ("eia_gas_prices",           "fact_str_metrics",             "context",       "week_date",
     "Weekly gas prices cross-referenced with STR daily demand as drive-market demand signal"),

    ("eia_gas_prices",           "bls_employment_monthly",       "context",       "year_month",
     "Gas prices and employment contextualized together for drive-market consumer spending picture"),

    ("tsa_checkpoint_daily",     "kpi_daily_summary",            "context",       "data_date",
     "TSA national air travel throughput signals fly-market demand into Dana Point"),

    ("tsa_checkpoint_daily",     "fact_str_metrics",             "context",       "data_date",
     "TSA checkpoint data cross-referenced with STR demand to isolate fly vs. drive market impact"),

    ("tsa_checkpoint_daily",     "visit_ca_airport_traffic",     "context",       "month",
     "National TSA throughput provides macro context for CA airport traffic (JWA/LAX/SNA)"),

    ("noaa_marine_monthly",      "weather_monthly",              "enriches",      "year_month",
     "NOAA buoy ocean conditions (wave height, water temp) enrich coastal weather picture"),

    ("noaa_marine_monthly",      "kpi_daily_summary",            "context",       "year_month",
     "Ocean conditions (swell, water temp, beach activity score) contextualize coastal demand"),

    ("noaa_marine_monthly",      "fact_str_metrics",             "context",       "year_month",
     "Ocean/beach conditions cross-referenced with STR demand for beach-day demand correlation"),

    ("census_demographics",      "datafy_overview_dma",          "cross_ref",     "county_name",
     "Census ACS demographics (OC/LA/SD) provide population context for Datafy feeder DMA data"),

    ("census_demographics",      "datafy_overview_demographics", "cross_ref",     "—",
     "Census demographics cross-referenced with Datafy verified visitor demographics for penetration analysis"),

    ("census_demographics",      "datafy_attribution_website_dma","cross_ref",    "county_name",
     "Census demographics provide denominator for website attribution DMA market penetration rates"),

    ("census_demographics",      "costar_market_snapshot",       "context",       "county_name",
     "Census feeder market demographics provide consumer demand context for CoStar market data"),

    # ── FRED Economic Indicators ───────────────────────────────────────────
    ("fred_economic_indicators", "kpi_daily_summary",            "context",       "data_date",
     "FRED macro indicators (CPI lodging, disposable income, consumer sentiment) contextualize ADR trends"),

    ("fred_economic_indicators", "fact_str_metrics",             "context",       "data_date",
     "FRED economic series (unemployment, savings rate) provide macro demand context for STR metrics"),

    ("fred_economic_indicators", "costar_market_snapshot",       "context",       "year",
     "FRED hotel CPI and income data provide inflation/demand macro context for CoStar market data"),

    ("fred_economic_indicators", "bls_employment_monthly",       "cross_ref",     "data_date",
     "FRED macro employment data cross-referenced with BLS sector-specific hospitality employment"),

    ("fred_economic_indicators", "eia_gas_prices",               "context",       "data_date",
     "FRED consumer indicators (savings rate, sentiment) provide demand context alongside EIA gas prices"),

    # ── Insights Daily (cross-audience) ───────────────────────────────────
    ("insights_daily",           "load_log",                     "audited_by",    "as_of_date",
     "compute_insights.py logs its run to load_log; insights_daily rows tied to pipeline run date"),

    ("insights_daily",           "vdp_events",                   "cross_ref",     "horizon_days",
     "Forward-looking insights reference upcoming VDP events within the horizon_days window"),

    # ── New Datafy Overview Tables (April 2026 format) ────────────────────
    ("datafy_overview_total_kpis",       "datafy_overview_kpis",              "enriches",    "report_period",
     "New-format total trips/visitor days/avg LOS enriches the legacy overview KPIs for same period"),

    ("datafy_overview_total_kpis",       "insights_daily",                    "derived_from","report_period",
     "Total trip count and avg LOS feed into compute_insights.py forward-looking demand projections"),

    ("datafy_overview_top_markets",      "datafy_overview_dma",               "cross_ref",   "dma",
     "New-format top feeder markets (trips share) cross-referenced with legacy DMA spend detail"),

    ("datafy_overview_top_markets",      "kpi_daily_summary",                 "cross_ref",   "report_period",
     "Top feeder DMA markets cross-referenced with KPI daily summary for drive/fly market segmentation"),

    ("datafy_overview_top_pois",         "datafy_overview_cluster_visitation","cross_ref",   "cluster",
     "Top POI clusters (City Council Districts, Lantern District) cross-referenced with legacy cluster visitation"),

    ("datafy_overview_top_pois",         "datafy_attribution_polygons",       "cross_ref",   "cluster",
     "Overview POI clusters cross-referenced with attribution polygon footprints for overlap analysis"),

    ("datafy_overview_spending_by_market","datafy_overview_dma",              "cross_ref",   "dma",
     "Per-DMA spend share (new format) cross-referenced with legacy DMA visitor days share for efficiency calc"),

    ("datafy_overview_spending_by_market","kpi_daily_summary",                "cross_ref",   "report_period",
     "Feeder market spend shares cross-referenced with STR ADR trends for market-value gap analysis"),

    ("datafy_overview_spending_by_category","datafy_overview_category_spending","cross_ref", "category",
     "New-format category spending (with avg spend) cross-references legacy category spend share table"),

    ("datafy_overview_spending_by_category","kpi_daily_summary",              "cross_ref",   "report_period",
     "Accommodation spend share and avg rate cross-referenced with ADR trends for rate capture analysis"),

    # ── New Datafy Attribution Tables ──────────────────────────────────────
    ("datafy_attribution_polygons",      "datafy_attribution_website_clusters","cross_ref",  "cluster",
     "Attribution polygon footprints cross-referenced with website attribution cluster breakdown"),

    ("datafy_attribution_polygons",      "datafy_overview_top_pois",          "cross_ref",   "cluster",
     "Destination polygon trip shares cross-referenced with overall POI visitation rankings"),

    ("datafy_attribution_website_groups","datafy_attribution_website_kpis",   "enriches",    "report_period",
     "Attribution group breakdown (Destination/Resorts/Hotels) enriches the website attribution KPI summary"),

    ("datafy_attribution_website_groups","datafy_attribution_media_groups",   "cross_platform","report_period",
     "Website vs. media attribution group performance compared for channel effectiveness analysis"),

    ("datafy_attribution_website_visitor_markets","datafy_attribution_website_dma","enriches","report_period",
     "Visitor-level market attribution (spend, impact share) enriches the website attribution DMA table"),

    ("datafy_attribution_website_visitor_markets","datafy_overview_dma",      "cross_ref",   "market",
     "Website-attributed visitor markets cross-referenced with organic DMA visitor share for lift analysis"),

    ("datafy_attribution_website_market_performance","datafy_attribution_website_dma","enriches","report_period",
     "Detailed market performance (spend/LOS/impact by DMA) enriches the website attribution DMA summary"),

    ("datafy_attribution_website_market_performance","kpi_daily_summary",     "cross_ref",   "report_period",
     "Website attribution market performance cross-referenced with STR KPIs for revenue attribution analysis"),

    ("datafy_attribution_peak_visitation","kpi_daily_summary",                "cross_ref",   "day_of_week",
     "Peak visitation days/months (% of max) cross-referenced with KPI daily summary for demand timing"),

    ("datafy_attribution_peak_visitation","vdp_events",                       "cross_ref",   "month",
     "Peak visitation months cross-referenced with VDP events calendar to identify event-driven demand peaks"),

    ("datafy_attribution_media_groups",  "datafy_attribution_media_kpis",     "enriches",    "report_period",
     "Media attribution group breakdown (Destination/Resorts/Hotels) enriches media campaign KPI summary"),

    ("datafy_attribution_media_groups",  "datafy_attribution_website_groups", "cross_platform","report_period",
     "Media vs. website attribution groups compared to identify channel-specific conversion patterns"),

    ("datafy_attribution_website_media_breakdown","datafy_attribution_website_channels","enriches","report_period",
     "Channel-level estimated impact (search/direct/redirect) enriches website attribution channel breakdown"),

    ("datafy_attribution_website_media_breakdown","datafy_attribution_media_kpis","cross_ref","report_period",
     "Website media channel impact cross-referenced with media campaign ROAS for budget allocation analysis"),

    # ── New Datafy Social / GA4 Tables ─────────────────────────────────────
    ("datafy_social_ga_overview",        "datafy_social_audience_overview",   "cross_platform","report_period",
     "New GA4 audience overview (Total Users, sessions, engagement) cross-references legacy audience overview"),

    ("datafy_social_ga_overview",        "later_ig_profile_growth",           "cross_platform","data_date",
     "GA4 website session data cross-referenced with Instagram follower growth for social-to-web funnel"),

    ("datafy_social_ga_overview",        "kpi_daily_summary",                 "context",     "report_period",
     "Website audience metrics (sessions, engagement rate) provide digital demand context for hotel KPIs"),

    ("datafy_social_ga_channels",        "datafy_social_traffic_sources",     "cross_ref",   "report_period",
     "New GA4 channel user share (organic/direct/display) cross-references legacy traffic source breakdown"),

    ("datafy_social_ga_channels",        "datafy_attribution_website_channels","cross_ref",  "channel",
     "GA4 acquisition channels cross-referenced with Datafy website attribution channels for funnel analysis"),

    ("datafy_social_ga_channels",        "datafy_attribution_website_media_breakdown","cross_ref","channel",
     "GA4 channel share cross-referenced with estimated destination impact per channel for ROI analysis"),

    ("datafy_social_device_breakdown",   "datafy_social_ga_overview",         "enriches",    "report_period",
     "Device breakdown (desktop/mobile/tablet) enriches the GA4 website audience overview"),

    ("datafy_social_device_breakdown",   "datafy_social_audience_overview",   "enriches",    "report_period",
     "Device usage split enriches the overall website audience overview for UX optimization insight"),

    ("datafy_social_new_vs_returning",   "datafy_social_ga_overview",         "enriches",    "report_period",
     "New vs. returning visitor ratio enriches the GA4 website audience overview"),

    ("datafy_social_new_vs_returning",   "datafy_attribution_website_visitor_markets","cross_ref","report_period",
     "New visitor share cross-referenced with website-attributed visitor markets for acquisition efficiency"),

    ("datafy_social_top_searches",       "datafy_social_ga_overview",         "enriches",    "report_period",
     "Top site search terms enrich the GA4 website audience overview with intent signals"),

    ("datafy_social_top_searches",       "vdp_events",                        "context",     "report_period",
     "Top search terms cross-referenced with VDP events calendar to identify event-driven search intent"),

    ("datafy_social_top_searches",       "google_trends_weekly",              "cross_platform","report_period",
     "Onsite search terms cross-referenced with Google external search trends for full demand intent picture"),

    ("datafy_social_geo_breakdown",      "datafy_overview_dma",               "cross_ref",   "market",
     "GA4 city-level visitor geography cross-referenced with Datafy DMA feeder market rankings"),

    ("datafy_social_geo_breakdown",      "datafy_overview_top_markets",       "cross_ref",   "market",
     "GA4 geographic user origins cross-referenced with top feeder market trip share data"),

    ("datafy_social_geo_breakdown",      "census_demographics",               "context",     "county_name",
     "GA4 geographic traffic origins contextualized with Census feeder market population data"),

    # ── Cross-platform social ──────────────────────────────────────────────
    ("later_ig_profile_growth",  "datafy_social_audience_overview","cross_ref",   "data_date",
     "Instagram follower/engagement cross-referenced with GA4 website audience — social-to-web funnel"),

    ("later_fb_profile_growth",  "datafy_social_audience_overview","cross_ref",   "data_date",
     "Facebook page reach cross-referenced with GA4 website sessions — social traffic attribution"),

    ("later_tk_profile_growth",  "datafy_social_audience_overview","cross_ref",   "data_date",
     "TikTok growth cross-referenced with GA4 traffic to measure TikTok→website conversion"),

    ("later_ig_profile_growth",  "datafy_social_traffic_sources","cross_ref",     "data_date",
     "Instagram follower growth cross-referenced with social traffic sources from GA4"),

    ("later_fb_profile_growth",  "datafy_social_traffic_sources","cross_ref",     "data_date",
     "Facebook reach cross-referenced with social traffic sources from GA4 analytics"),

    # ── Strategy Goals ─────────────────────────────────────────────────────────
    ("strategy_goals",           "kpi_daily_summary",            "derived_from",  "as_of_date",
     "RevPAR and ADR goals track progress against kpi_daily_summary trailing averages"),

    ("strategy_goals",           "kpi_compression_quarterly",    "cross_ref",     "quarter",
     "Compression-day goals track progress against kpi_compression_quarterly counts"),

    ("strategy_goals",           "datafy_overview_kpis",         "cross_ref",     "report_period",
     "Visitor trip and OOS share goals track progress against Datafy annual KPIs"),

    ("strategy_goals",           "datafy_attribution_media_kpis","cross_ref",     "report_period",
     "Media ROAS goals track progress against Datafy media attribution ROAS values"),

    ("strategy_goals",           "fact_str_metrics",             "derived_from",  "as_of_date",
     "TBID revenue goals compute current progress from fact_str_metrics room revenue × 0.0125"),

    ("strategy_goals",           "insights_daily",               "enriches",      "as_of_date",
     "Strategy goal progress informs and contextualizes daily AI insights for all audiences"),

    # ── Event Analytics (May 2026 Event Impact Initiative) ──────────────────
    ("vdp_events",               "events_metrics",               "enriches",      "event_id",
     "Event base data enriched with estimated attendance, web interest, and promotion channels"),

    ("vdp_events",               "events_economic_impact",       "derived_from",  "event_id",
     "Economic impact analysis computed from event dates correlated with STR occupancy/ADR periods"),

    ("vdp_events",               "events_promotion_analysis",    "derived_from",  "event_id",
     "Promotion effectiveness analyzed from Later.com social posts and Google Trends search volume"),

    ("vdp_events",               "events_visitor_mix",           "enriches",      "event_id",
     "Visitor demographics during event periods estimated from Datafy overview KPIs baseline"),

    ("vdp_events",               "events_insights",              "derived_from",  "event_id",
     "Pre-computed event ROI/promotion insights cached for dashboard display"),

    ("events_metrics",           "events_economic_impact",       "enriches",      "event_id",
     "Event attendance estimates inform economic impact revenue-per-attendee calculations"),

    ("events_metrics",           "later_ig_posts",               "cross_ref",     "event_date",
     "Event social promotion metrics aggregated from Later.com Instagram posts during event window"),

    ("events_metrics",           "later_fb_posts",               "cross_ref",     "event_date",
     "Event social promotion metrics aggregated from Later.com Facebook posts during event window"),

    ("events_metrics",           "google_trends_weekly",         "cross_ref",     "event_date",
     "Event web interest tracked via Google Trends search volume spikes for event keywords"),

    ("events_economic_impact",   "kpi_daily_summary",            "cross_ref",     "event_date",
     "Event-period occupancy/ADR/RevPAR compared against baseline kpi_daily_summary periods"),

    ("events_economic_impact",   "fact_str_metrics",             "cross_ref",     "event_date",
     "Event economic impact correlated with STR daily/monthly metrics for the same property/market"),

    ("events_economic_impact",   "insights_daily",               "enriches",      "event_date",
     "Economic impact metrics enrich AI-generated event ROI insights for all audiences"),

    ("events_promotion_analysis","later_ig_posts",              "cross_ref",     "event_date",
     "Promotion effectiveness sourced from Instagram post count/engagement during event window"),

    ("events_promotion_analysis","later_fb_posts",              "cross_ref",     "event_date",
     "Promotion effectiveness sourced from Facebook post count/engagement during event window"),

    ("events_promotion_analysis","later_tk_profile_growth",     "cross_ref",     "event_date",
     "TikTok audience growth tracking during event promotion period"),

    ("events_promotion_analysis","google_trends_weekly",         "cross_ref",     "event_date",
     "Hashtag/keyword volume and sentiment proxied via Google Trends search interest peaks"),

    ("events_visitor_mix",       "datafy_overview_kpis",         "derived_from",  "report_period",
     "Event visitor demographics (age, HHI, LOS) estimated from Datafy overview KPIs baseline"),

    ("events_visitor_mix",       "datafy_overview_dma",          "cross_ref",     "report_period",
     "Event top origin markets estimated from Datafy DMA feeder market breakdown"),

    ("events_visitor_mix",       "datafy_overview_demographics", "enriches",      "report_period",
     "Event visitor age/income segments estimated from Datafy demographic distribution"),

    ("events_insights",          "vdp_events",                   "enriches",      "event_id",
     "Pre-computed event insights (ROI, promotion effectiveness, visitor value) ready for dashboard"),

    ("events_insights",          "events_economic_impact",       "enriches",      "event_id",
     "ROI and revenue per attendee insights derived from events_economic_impact calculations"),

    ("events_insights",          "events_promotion_analysis",    "enriches",      "event_id",
     "Promotion effectiveness and recommended spend insights derived from events_promotion_analysis"),

    # ── Ticketmaster Events (regional event demand pipeline) ───────────────
    ("ticketmaster_events",      "vdp_events",                   "cross_ref",     "event_date",
     "Regional Ticketmaster events (50-mi radius) cross-referenced with local VDP-curated events on shared dates"),

    ("ticketmaster_events",      "kpi_daily_summary",            "context",       "as_of_date",
     "Concert/sports demand on event_date contextualizes hotel occupancy spikes in kpi_daily_summary"),

    ("ticketmaster_events",      "kpi_compression_quarterly",    "context",       "event_date→quarter",
     "Concentration of regional events per quarter helps explain compression-day clustering"),

    ("ticketmaster_events",      "datafy_overview_dma",          "cross_ref",     "venue_market",
     "Event venues align with Datafy DMA feeder markets — campaign timing follow-through"),

    ("ticketmaster_events",      "google_trends_weekly",         "cross_ref",     "event_date→week",
     "Search interest peaks should precede regional event dates by 2–6 weeks"),

    ("ticketmaster_events",      "insights_daily",               "derived_from",  "event_date",
     "compute_insights.py uses ticketmaster_events to forecast event-driven compression and ROI"),

    ("ticketmaster_events",      "events_economic_impact",       "cross_ref",     "event_date",
     "Major regional events cross-referenced with local event economic impact for spillover analysis"),

    # ── Wikipedia Pageviews (awareness / intent signal) ────────────────────
    ("wikipedia_pageviews_daily","google_trends_weekly",         "cross_platform","time_period",
     "Wikipedia pageviews and Google Trends together triangulate organic visitor interest"),

    ("wikipedia_pageviews_daily","kpi_daily_summary",            "context",       "as_of_date",
     "Pageviews on Dana Point articles 4–6 weeks ahead correlate with downstream occupancy"),

    ("wikipedia_pageviews_daily","datafy_attribution_website_kpis","cross_ref",   "time_period",
     "Wikipedia awareness signal pairs with Datafy website attribution to model intent → visit funnel"),

    ("wikipedia_pageviews_daily","datafy_attribution_website_dma","cross_ref",    "time_period",
     "Awareness lift broken down by DMA via cross-reference with Datafy attribution by feeder market"),

    ("wikipedia_pageviews_daily","insights_daily",               "derived_from",  "as_of_date",
     "compute_insights.py reads pageview spikes to flag emerging interest in Dana Point articles"),

    ("wikipedia_pageviews_daily","vdp_events",                   "cross_ref",     "event_date",
     "Pageviews lift around major events validates earned-media reach"),

    # ── NOAA Tides (Dana Point Harbor visitor capacity) ────────────────────
    ("noaa_tides_daily",         "kpi_daily_summary",            "context",       "as_of_date",
     "Tide range and negative-low days affect beach width and harbor visitor capacity"),

    ("noaa_tides_daily",         "vdp_events",                   "context",       "event_date",
     "Harbor / tidepool / Festival of Whales events sensitive to tide cycle — tide context per event date"),

    ("noaa_tides_daily",         "ticketmaster_events",          "context",       "event_date",
     "Boat-based and waterfront ticketed events cross-referenced with daily tide forecast"),

    ("noaa_tides_daily",         "noaa_marine_monthly",          "enriches",      "month",
     "Daily Dana Point tide and water-temp detail enriches the monthly NOAA marine summary"),

    ("noaa_tides_daily",         "weather_monthly",              "cross_ref",     "month",
     "Tide / water-temp data complements coastal weather for visitor-experience modeling"),

    # ── AirNow AQI (visitor outdoor experience / event cancellation risk) ─
    ("airnow_aqi_daily",         "kpi_daily_summary",            "context",       "as_of_date",
     "AQI > 100 days correlate with softer leisure demand even when weather is clear"),

    ("airnow_aqi_daily",         "vdp_events",                   "context",       "event_date",
     "AQI > 150 raises outdoor-event cancellation risk; tracked per event date"),

    ("airnow_aqi_daily",         "ticketmaster_events",          "context",       "event_date",
     "Outdoor concerts (Pacific Amphitheatre, Doheny Blues) screened against AQI by event date"),

    ("airnow_aqi_daily",         "weather_monthly",              "cross_ref",     "month",
     "AQI joins coastal weather to give a complete visitor-experience risk panel"),

    ("airnow_aqi_daily",         "insights_daily",               "derived_from",  "as_of_date",
     "compute_insights.py flags AQI risk windows for residents/visitors audience cards"),

    # ── 2026-05-22: New coastal + demand intelligence tables ────────────────────
    ("surf_conditions_daily",    "kpi_daily_summary",            "cross_ref",     "obs_date→as_of_date",
     "Wave height and water temp correlate with weekend occupancy spikes (+4-7% on good surf days)"),
    ("surf_conditions_daily",    "weather_monthly",              "enriches",      "month",
     "Daily surf conditions enrich monthly weather with actionable beach-quality signal"),
    ("surf_conditions_daily",    "vdp_events",                   "context",       "event_date",
     "Surf quality on event dates (Ohana Fest, whale watching) directly affects attendance"),
    ("surf_conditions_daily",    "insights_daily",               "derived_from",  "as_of_date",
     "Beach conditions inform visitor audience cards (best_value, booking_timing)"),
    ("surf_conditions_daily",    "noaa_tides_daily",             "cross_ref",     "obs_date",
     "Surf + tides combined give complete coastal visitor experience signal"),

    ("ca_state_parks_visitation","kpi_daily_summary",            "cross_ref",     "report_month",
     "State park day-use visits are proxy for beach tourism invisible in hotel STR data"),
    ("ca_state_parks_visitation","datafy_overview_kpis",         "cross_ref",     "report_year",
     "Doheny camping nights add to overnight visitor count not captured in hotel STR"),
    ("ca_state_parks_visitation","insights_daily",               "derived_from",  "report_year",
     "Park visitation informs resident/visitor insights on peak periods and community impact"),

    ("demand_signal_weekly",     "kpi_daily_summary",            "derived_from",  "week_date→as_of_date",
     "Demand signal index synthesizes 6 sources to predict STR occupancy 2-4 weeks forward"),
    ("demand_signal_weekly",     "google_trends_weekly",         "derived_from",  "week_date",
     "Google Trends primary (35% weight) is the strongest leading indicator in demand index"),
    ("demand_signal_weekly",     "eia_gas_prices",               "derived_from",  "week_date",
     "Gas price component (15% weight) proxies drive-market affordability in demand index"),
    ("demand_signal_weekly",     "wikipedia_pageviews_daily",    "derived_from",  "week_date",
     "Wikipedia awareness (15% weight) measures destination mindshare in demand index"),
    ("demand_signal_weekly",     "weather_monthly",              "derived_from",  "month",
     "Seasonal beach quality (20% weight) anchors demand index baseline by month"),
    ("demand_signal_weekly",     "insights_daily",               "derived_from",  "as_of_date",
     "Demand signal generates forward-looking DMO/visitor insights with quantified confidence"),

    ("data_correlation_matrix",  "google_trends_weekly",         "measures",      "lag_weeks",
     "Quantifies how many weeks Google Trends leads STR occupancy (typically 2-3 week lag)"),
    ("data_correlation_matrix",  "eia_gas_prices",               "measures",      "lag_weeks",
     "Measures gas price impact on occupancy with seasonal adjustment for summer demand"),
    ("data_correlation_matrix",  "kpi_daily_summary",            "measures",      "as_of_date",
     "All correlations in matrix target kpi_occ_pct or kpi_adr as the dependent variable"),
    ("data_correlation_matrix",  "insights_daily",               "derived_from",  "updated_at",
     "Correlation insights inform AI analyst answers with statistically-backed lead times"),

    # ── Beach Water Quality (Heal the Bay / CA monitoring) ────────────────────
    ("beach_water_quality_weekly","kpi_daily_summary",           "context",       "sample_date→as_of_date",
     "Beach advisory days correlate with RevPAR softness 1-3 days after rain; A/B grades support demand"),

    ("beach_water_quality_weekly","weather_monthly",             "cross_ref",     "sample_date→month",
     "Post-rain bacterial exceedances (C/D/F grades) directly follow wet-weather events in weather data"),

    ("beach_water_quality_weekly","surf_conditions_daily",       "cross_ref",     "sample_date→obs_date",
     "Beach grade and surf quality combined give complete coastal visitor-experience signal per day"),

    ("beach_water_quality_weekly","noaa_tides_daily",            "context",       "sample_date→obs_date",
     "Incoming tides after rain flush bacteria from creek mouths; tide data contextualizes grade swings"),

    ("beach_water_quality_weekly","vdp_events",                  "context",       "event_date",
     "Beach-centric events (Doheny Blues, triathlon) screened against advisory status on event date"),

    ("beach_water_quality_weekly","airnow_aqi_daily",            "cross_ref",     "sample_date",
     "Beach advisories and poor AQI days together quantify total outdoor visitor-experience risk"),

    ("beach_water_quality_weekly","datafy_overview_kpis",        "context",       "report_period",
     "Seasonal advisory frequency contextualizes Datafy overnight visitor mix and accommodation share"),

    ("beach_water_quality_weekly","insights_daily",              "derived_from",  "sample_date→as_of_date",
     "compute_insights.py flags advisory windows for visitor/resident audience cards"),

    # ── Whale Watching Activity ────────────────────────────────────────────────
    ("whale_watching_activity",   "kpi_daily_summary",           "cross_ref",     "obs_month",
     "Whale watching index >= 70 correlates with +8-12% shoulder-season ADR premium on boat-tour weekends"),

    ("whale_watching_activity",   "datafy_overview_dma",         "cross_ref",     "obs_month",
     "Fly markets (SLC, Dallas, NYC) over-index during whale season vs. drive markets — premium segment signal"),

    ("whale_watching_activity",   "datafy_overview_kpis",        "context",       "obs_month",
     "Whale watching visitor trips add to destination total overnight count; shoulder season LOS driver"),

    ("whale_watching_activity",   "kpi_compression_quarterly",   "context",       "obs_month→quarter",
     "Gray whale season (Q1) lifts compression-day count; blue whale season anchors Q3 compression"),

    ("whale_watching_activity",   "vdp_events",                  "cross_ref",     "obs_month→event_date",
     "Festival of Whales (March) and Tall Ships Festival (September) directly overlap whale seasons"),

    ("whale_watching_activity",   "noaa_marine_monthly",         "enriches",      "obs_month",
     "Ocean conditions (wave height, water temp) directly affect charter sighting rates and trip quality"),

    ("whale_watching_activity",   "surf_conditions_daily",       "context",       "obs_month",
     "Calm seas (wave < 3ft) maximize charter comfort; combined with whale index for visit-quality score"),

    ("whale_watching_activity",   "ticketmaster_events",         "context",       "obs_month",
     "Marine and outdoor events near whale season amplify shoulder-demand beyond whale watching alone"),

    ("whale_watching_activity",   "insights_daily",              "derived_from",  "obs_month",
     "compute_insights.py uses whale index to forecast shoulder-season demand and OOS visitor premium"),

    ("whale_watching_activity",   "google_trends_weekly",        "cross_ref",     "obs_month",
     "Google search interest for 'whale watching Dana Point' leads charter bookings by 2-4 weeks"),

    # ── visit_ca_intl_market_profiles (GMP PDFs) ─────────────────────────────
    ("visit_ca_intl_market_profiles", "datafy_overview_dma",         "cross_ref",     "country",
     "GMP origin country maps to Datafy DMA feeder markets: Australia→LA, Canada→PNW/NY, Mexico→LA/SD"),
    ("visit_ca_intl_market_profiles", "datafy_overview_airports",    "cross_ref",     "country",
     "GMP visitor origin country aligns with origin airports in Datafy airport mix data"),
    ("visit_ca_intl_market_profiles", "visit_ca_intl_arrivals",      "enriches",      "country→report_period",
     "GMP 2025 country profiles enrich monthly intl arrivals with per-country spend/LOS/activity data"),
    ("visit_ca_intl_market_profiles", "costar_market_snapshot",      "context",       "report_year",
     "Intl visitor mix affects ADR premium: high-spend markets (AUS, UK, Middle East) drive luxury rate capture"),
    ("visit_ca_intl_market_profiles", "kpi_daily_summary",           "context",       "report_year",
     "High-spend international markets (avg $4K-$8K/visitor) support ADR above $300 when mix shifts"),
    ("visit_ca_intl_market_profiles", "datafy_attribution_website_dma", "cross_ref",  "country",
     "International GMP countries cross-reference website attribution DMA to measure digital reach by origin"),
    ("visit_ca_intl_market_profiles", "zartico_markets",             "cross_ref",     "country",
     "GMP international profiles complement Zartico top visitor markets for full origin-market picture"),
    ("visit_ca_intl_market_profiles", "insights_daily",              "derived_from",  "country",
     "International market profiles inform AI insights on international visitor opportunity gaps"),
    ("visit_ca_intl_market_profiles", "fred_economic_indicators",    "context",       "report_year",
     "Currency strength and macro indicators affect international visitor volumes by origin country"),

    # ── visit_ca_lodging_monthly (CA Lodging Performance XLS) ────────────────
    ("visit_ca_lodging_monthly", "kpi_daily_summary",               "context",       "report_period→as_of_date",
     "CA statewide + South Coast lodging benchmarks provide competitive context for Dana Point KPIs"),
    ("visit_ca_lodging_monthly", "costar_market_snapshot",          "cross_ref",     "report_period",
     "CA lodging monthly and CoStar snapshot cover same time window — join on period for market vs submarket"),
    ("visit_ca_lodging_monthly", "costar_monthly_performance",      "cross_ref",     "report_period",
     "CA lodging OCC/ADR vs CoStar OC submarket monthly: measures Dana Point premium over state average"),
    ("visit_ca_lodging_monthly", "visit_ca_lodging_forecast",       "enriches",      "report_period",
     "Actual monthly lodging performance vs Visit CA forecast — measures forecast accuracy"),
    ("visit_ca_lodging_monthly", "fact_str_metrics",                "context",       "report_period",
     "CA statewide lodging monthly provides state-level context for STR daily/monthly metrics"),
    ("visit_ca_lodging_monthly", "zartico_lodging_kpis",            "cross_ref",     "report_period",
     "CA lodging monthly actuals cross-reference Zartico lodging KPIs for consistency validation"),
    ("visit_ca_lodging_monthly", "bls_employment_monthly",          "context",       "report_period",
     "Lodging performance correlates with hospitality employment — both reflect tourism health"),

    # ── visit_ca_travel_indicators (Monthly Travel Indicators Summary PDF) ───
    ("visit_ca_travel_indicators", "fred_economic_indicators",      "cross_ref",     "report_period",
     "CA travel indicators (gas, consumer confidence) align with FRED macro data for same period"),
    ("visit_ca_travel_indicators", "visit_ca_airport_traffic",      "enriches",      "report_period",
     "Travel indicators monthly air passengers cross-reference Visit CA airport traffic data"),
    ("visit_ca_travel_indicators", "kpi_daily_summary",             "context",       "report_period",
     "CA statewide hotel OCC/ADR in travel indicators provides state benchmark for Dana Point KPIs"),
    ("visit_ca_travel_indicators", "eia_gas_prices",                "cross_ref",     "report_period",
     "CA travel indicator gas prices align with EIA CA regular gas price series for same month"),
    ("visit_ca_travel_indicators", "demand_signal_weekly",          "derived_from",  "report_period",
     "Travel indicators (gas price, air traffic, hotel OCC) feed into PULSE Demand Signal Index"),
    ("visit_ca_travel_indicators", "google_trends_weekly",          "context",       "report_period",
     "Monthly travel indicators context for Google search demand trends in same period"),

    # ── airbnb_market_data / airbnb_market_summary (InsideAirbnb) ────────────
    ("airbnb_market_summary", "costar_market_snapshot",            "cross_ref",     "report_date→report_period",
     "STVR median price vs hotel ADR — measures hotel rate premium/discount vs short-term rental market"),
    ("airbnb_market_summary", "costar_monthly_performance",        "cross_ref",     "report_date",
     "Airbnb availability (avg 210 days/yr) + hotel OCC together define total accommodation supply picture"),
    ("airbnb_market_summary", "kpi_daily_summary",                 "cross_ref",     "report_date→as_of_date",
     "STVR pricing vs hotel ADR gap: when Airbnb median >$285 and hotel ADR <$300, rate capture opportunity exists"),
    ("airbnb_market_summary", "zartico_lodging_kpis",              "enriches",      "report_date",
     "Airbnb market summary enriches Zartico lodging KPIs with STVR competitive pricing context"),
    ("airbnb_market_summary", "ca_state_parks_visitation",         "context",       "report_date",
     "High state park day-use visits correlate with STVR demand — beach proximity drives Airbnb pricing"),
    ("airbnb_market_summary", "demand_signal_weekly",              "context",       "report_date",
     "STVR availability rate is a leading indicator of forward demand — high occupancy forecasts demand spike"),
    ("airbnb_market_summary", "datafy_overview_kpis",              "cross_ref",     "report_date",
     "STVR market size (450+ Dana Point listings) relative to hotel supply — total overnight capacity"),
    ("airbnb_market_summary", "insights_daily",                    "derived_from",  "report_date",
     "STVR pricing intelligence informs AI insights on hotel rate positioning and competitive gaps"),
    ("airbnb_market_data",    "airbnb_market_summary",             "derived_from",  "report_date+city",
     "Listing-level Airbnb data aggregated into summary stats by city and report period"),

    # ── us_travel_* (2026-05-29) — U.S. Travel Association national benchmarks ──
    ("us_travel_group_segments",  "group_intelligence",            "cross_ref",     "report_year",
     "US Travel national group segment spend ($319B total) provides benchmark context for Dana Point group opportunity"),
    ("us_travel_group_segments",  "insights_daily",                "derived_from",  "report_year",
     "National group segment data ($126B meetings, $102B spectator, $52B sports, $39B leisure) informs group insights"),
    ("us_travel_group_segments",  "events_economic_impact",        "cross_ref",     "report_year",
     "National event + spectator spend context for Dana Point event ROI benchmarking"),
    ("us_travel_business_travel", "kpi_daily_summary",             "context",       "report_year",
     "Business travelers = 20% of volume, 60% of lodging revenue — national ratio to benchmark against Dana Point mix"),
    ("us_travel_business_travel", "costar_market_snapshot",        "context",       "report_year",
     "National business travel recovery rate (87% transient, 82% meetings) provides market context for CoStar South OC data"),
    ("us_travel_business_travel", "group_intelligence",            "enriches",      "report_year",
     "National meetings & events spend ($126B) anchors Dana Point group TBID opportunity projections"),
    ("us_travel_business_travel", "datafy_attribution_media_kpis", "context",       "report_year",
     "National business travel volume/revenue split informs Datafy media campaign ROI benchmarking"),
    ("us_travel_traveler_types",  "datafy_overview_demographics",  "cross_ref",     "report_year",
     "10 national traveler-type profiles map to Datafy visitor demographics (age, income, travel party)"),
    ("us_travel_traveler_types",  "events_visitor_mix",            "cross_ref",     "report_year",
     "Traveler type profiles (group_smerf booking_window=8-48wks, LOS=2-5nts) benchmark against event visitor mix data"),
    ("us_travel_traveler_types",  "kpi_daily_summary",             "context",       "report_year",
     "Traveler type seasonal patterns (family=peak, SMERF=shoulder, business=year-round) explain OCC/ADR weekly rhythms"),
    ("us_travel_traveler_types",  "insights_daily",                "derived_from",  "report_year",
     "10 traveler type benchmarks (booking window, LOS, priorities) inform visitor and DMO insights"),
    ("us_travel_national_kpis",   "kpi_daily_summary",             "context",       "report_year",
     "National travel KPIs provide macro benchmarking context for Dana Point hotel performance"),
    ("us_travel_national_kpis",   "fred_economic_indicators",      "cross_ref",     "report_year",
     "US Travel national KPIs cross-reference FRED macro data — both measure travel demand macro environment"),
    ("us_travel_national_kpis",   "visit_ca_travel_forecast",      "enriches",      "report_year",
     "National US Travel recovery rates provide context for California-specific VCA travel forecasts"),
    ("us_travel_national_kpis",   "insights_daily",                "derived_from",  "report_year",
     "National group travel economic impact ($319B, 3M jobs) anchors Dana Point TBID revenue opportunity sizing"),

    # ── group_intelligence (2026-05-29) ──────────────────────────────────────
    ("costar_chain_scale_breakdown", "group_intelligence",           "derived_from",  "year",
     "Chain-scale room counts (Upper Upscale + Upscale) used to estimate group-primary supply capacity"),
    ("costar_market_snapshot",       "group_intelligence",           "derived_from",  "report_period",
     "Annual room revenue and blended ADR/occ from market snapshot used in group TBID/TOT projections"),
    ("kpi_compression_quarterly",    "group_intelligence",           "derived_from",  "quarter",
     "Annual compression days used to assess group displacement risk on peak dates"),
    ("group_intelligence",           "insights_daily",               "derived_from",  "benchmark_date",
     "Group benchmarks generate 4 insights: group_revenue_opportunity, group_displacement_risk, "
     "group_adr_premium, group_demand_trend — for dmo and city audiences"),
    ("group_intelligence",           "kpi_daily_summary",            "context",       "benchmark_date",
     "STR blended ADR from kpi_daily_summary compared against group estimated ADR to measure discount gap"),
    ("group_intelligence",           "costar_monthly_performance",   "context",       "benchmark_date",
     "CoStar monthly ADR/occ trends provide market context for group demand share estimates"),
    ("events_economic_impact",       "group_intelligence",           "cross_ref",     "event_date",
     "Event room revenue and attendee mix (group_travel_pct) provides event-level group demand context"),
    ("events_visitor_mix",           "group_intelligence",           "cross_ref",     "event_date",
     "Event visitor mix group_travel_pct and avg_party_size will enrich group intelligence when populated"),
    ("datafy_attribution_media_groups", "group_intelligence",        "context",       "report_period_start",
     "Media attribution by property type (Destination/Resorts/Hotels) provides channel context for group bookings"),

    # ── BTS T-100 Route Passengers ─────────────────────────────────────────
    ("bts_route_passengers",         "datafy_overview_dma",          "cross_ref",     "origin_city/dma",
     "BTS air seat capacity by origin city cross-references Datafy DMA feeder market share — fly markets (SLC/Dallas/NYC) can be validated against actual passenger volumes"),

    ("bts_route_passengers",         "datafy_overview_airports",     "cross_ref",     "dest_airport_code",
     "BTS destination airport (SNA/SAN/LAX) cross-references Datafy origin airport share for same feeder markets"),

    ("bts_route_passengers",         "kpi_daily_summary",            "context",       "quarter/as_of_date",
     "BTS quarterly route capacity provides air connectivity context for Dana Point hotel demand — more seats = more potential fly-market visitors"),

    # ── InsideAirbnb STVR Market Summary ───────────────────────────────────
    ("stvr_market_summary",          "fact_str_metrics",             "cross_ref",     "month",
     "STVR monthly ADR and occupancy compared against STR hotel metrics — competitive supply pressure and rate gap analysis"),

    ("stvr_market_summary",          "kpi_daily_summary",            "cross_ref",     "month/as_of_date",
     "STVR occupancy and RevPAR benchmarked against hotel kpi_daily_summary for total accommodations demand picture"),

    ("stvr_market_summary",          "zartico_lodging_kpis",         "cross_platform","report_period",
     "InsideAirbnb STVR summary cross-referenced with Zartico lodging KPIs (STVR OCC/ADR) for historical validation"),

    # ── SoCal Gas Prices ───────────────────────────────────────────────────
    ("socal_gas_prices",             "kpi_daily_summary",            "cross_ref",     "period/as_of_date",
     "LA Basin weekly gas prices cross-referenced with hotel KPIs — inverse correlation: gas >$5.00 correlates with 2-4% dip in drive-market weekend occ 2-3 weeks out"),

    ("socal_gas_prices",             "eia_gas_prices",               "cross_platform","period",
     "SoCal LA Basin prices (fetch_socal_gas) are a finer regional cut of the CA statewide series in eia_gas_prices — use together for state vs. metro comparison"),

    ("socal_gas_prices",             "datafy_overview_dma",          "context",       "report_period",
     "LA Basin gas prices provide drive-market cost context for Datafy DMA feeder market (LA Metro = largest feeder by volume)"),

    ("socal_gas_prices",             "demand_signal_weekly",         "context",       "period/week_date",
     "LA Basin gas prices augment the drive-market affordability component of the PULSE Demand Signal Index"),

    # ── NWS weather.gov: forecast / hourly / observations ──────────────────
    # Written by fetch_nws_weather.py. weather_monthly is the long-run seasonal
    # baseline; these three are the live short-horizon layer on top of it.
    ("weather_forecast",             "weather_monthly",              "enriches",      "start_time→year+month",
     "Seven-day NWS forecast is the live short-horizon layer over the weather_monthly seasonal baseline"),

    ("weather_forecast",             "kpi_daily_summary",            "context",       "start_time→as_of_date",
     "Forecast conditions in the booking window shape walk-in and short-lead demand: clear 70F+ weekends support rate holds, rain forecasts precede same-week softness"),

    ("weather_forecast",             "vdp_events",                   "context",       "start_time→event_date",
     "Forecast for each event date flags weather risk on outdoor events (Doheny Blues, Festival of Whales, Tall Ships)"),

    ("weather_forecast",             "surf_conditions_daily",        "cross_ref",     "start_time→obs_date",
     "Wind speed and direction in the forecast drive surf quality: offshore wind plus clean swell is the premium beach-day signal"),

    ("weather_forecast",             "beach_water_quality_weekly",   "context",       "start_time→sample_date",
     "Rain in the forecast predicts post-storm bacterial exceedances 24-72 hours out, ahead of the next beach sample"),

    ("weather_forecast",             "demand_signal_weekly",         "context",       "start_time→week_date",
     "Forecast beach-day quality is the near-term weather input to the PULSE Demand Signal Index"),

    ("weather_forecast",             "insights_daily",               "derived_from",  "start_time→as_of_date",
     "compute_insights.py uses the forecast horizon for visitor best-value and resident peak-alert cards"),

    ("weather_hourly",               "weather_forecast",             "enriches",      "start_time",
     "48-hour hourly detail (temp, wind, precip chance) breaks the daily forecast period down to arrival-time granularity"),

    ("weather_hourly",               "noaa_tides_daily",             "cross_ref",     "start_time→obs_date",
     "Hourly conditions joined to tide timing identify the best beach windows within a day, not just the best days"),

    ("weather_hourly",               "vdp_events",                   "context",       "start_time→event_date",
     "Hourly precipitation chance across event hours is a sharper go/no-go signal than the daily forecast summary"),

    ("weather_observations",         "weather_forecast",             "measures",      "obs_time→start_time",
     "Actual station observations scored against the prior forecast measure local NWS forecast accuracy before it is trusted for pricing"),

    ("weather_observations",         "weather_monthly",              "derived_from",  "obs_time→year+month",
     "Thirty days of station observations roll up into the weather_monthly averages and beach_day_score"),

    ("weather_observations",         "kpi_daily_summary",            "cross_ref",     "obs_time→as_of_date",
     "Observed conditions matched to same-day occupancy and ADR quantify realized weather sensitivity by day of week"),

    ("weather_observations",         "beach_water_quality_weekly",   "context",       "obs_time→sample_date",
     "Observed rainfall is the upstream cause of creek-mouth bacterial exceedances in the beach grade data"),

    ("weather_observations",         "noaa_marine_monthly",          "cross_ref",     "obs_time→year+month",
     "Land station observations and NOAA buoy readings together describe the full coastal condition picture"),

    # ── CoStar Market Daily/Monthly (raw HospitalityDataGrid export) ───────
    ("costar_market_daily",          "fact_str_metrics",             "cross_ref",     "as_of_date",
     "CoStar's raw daily submarket export cross-referenced against STR daily VDP metrics for third-party rate validation"),

    ("costar_market_daily",          "costar_market_monthly",        "same_source",   "as_of_date→report_period",
     "costar_market_daily rolls up into costar_market_monthly — same CoStar submarket export, daily vs. monthly grain"),

    ("costar_market_monthly",        "costar_monthly_performance",   "cross_platform","report_period→as_of_date",
     "costar_market_monthly (raw CoStar tool export, current) and costar_monthly_performance (parsed from CoStar PDF reports) measure the same submarket — cross-check for consistency"),

    ("costar_market_monthly",        "kpi_daily_summary",            "cross_ref",     "report_period→as_of_date",
     "CoStar submarket OCC/ADR/RevPAR benchmarks VDP's own STR-reported performance"),

    # ── Visit California Resident Sentiment ─────────────────────────────────
    ("visit_ca_resident_sentiment",  "kpi_daily_summary",            "context",       "area/report_period",
     "Orange County resident sentiment on tourism benefits/costs contextualizes community support for VDP visitor growth"),

    ("visit_ca_resident_sentiment",  "datafy_overview_kpis",         "context",       "area",
     "Resident sentiment scores (jobs, cost of living, quality of life impact) inform destination-management narrative alongside visitor economy KPIs"),

    # ── U.S. Travel Inbound Market Profiles (national overseas visitor activity) ──
    ("us_travel_inbound_market_profile", "datafy_overview_category_spending", "context", "year",
     "National overseas-visitor activity participation trends (Hotel-Motel, Shopping, Fine-Dining, Vacation, etc.) contextualize Datafy's local visitor spending-by-category mix"),

    ("us_travel_inbound_market_profile", "fact_str_metrics",         "context",       "year→as_of_date",
     "National inbound Hotel-Motel visitor volume trend provides macro demand context for VDP's local STR performance"),

    # ── Datafy: new July-2026 Advanced/Enhanced Spending Overview family ───
    ("datafy_overview_instate_outstate",     "datafy_overview_dma",              "enriches", "report_period",
     "In-state vs. out-of-state spend split enriches the DMA feeder-market breakdown with an origin-state summary"),

    ("datafy_overview_local_visitor_spend",  "datafy_overview_kpis",             "enriches", "report_period",
     "Local vs. visitor spend split enriches the annual overview KPIs with a resident/visitor spending divide"),

    ("datafy_overview_spending_peak_insights", "datafy_overview_category_spending", "context", "report_period",
     "Highest/lowest spend day-of-week context explains seasonality in the category spending breakdown"),

    ("datafy_overview_spending_by_month",    "fact_str_metrics",                 "cross_ref", "year+month→as_of_date",
     "Datafy monthly visitor spending cross-referenced against STR monthly room revenue for total economic-impact validation"),

    ("datafy_advanced_spending_top_markets", "datafy_overview_dma",              "cross_ref", "dma",
     "Advanced Spending Overview top (hotel) markets is a finer-grained, more current cut of the same DMA spend-share concept as datafy_overview_dma"),

    ("datafy_overview_spend_density_by_zip", "datafy_overview_dma",              "enriches", "report_period",
     "ZIP-level spend density enriches the DMA-level feeder market view with sub-metro geographic resolution"),

    ("datafy_overview_los_distribution",     "datafy_overview_kpis",             "enriches", "report_period",
     "Length-of-stay distribution enriches the overview avg_los_days KPI with the full stay-length curve"),

    ("datafy_overview_state_origin",         "datafy_overview_dma",              "cross_ref", "report_period",
     "State-of-origin share is a coarser geographic cut of the same visitor-origin concept as the DMA breakdown"),

    ("datafy_overview_visitation_by_month",  "kpi_daily_summary",                "cross_ref", "year+month→as_of_date",
     "Datafy monthly visitor-days cross-referenced against STR monthly demand (room-nights) for total visitation vs. hotel-only demand"),

    ("datafy_overview_local_visitor_trips",  "datafy_overview_kpis",             "enriches", "year→report_period",
     "Local vs. visitor trip and visitor-day split by year enriches the annual overview KPIs with a resident/visitor trip divide"),

    ("datafy_overview_weekday_visitation",   "kpi_daily_summary",                "cross_ref", "day_of_week",
     "Datafy weekday vs. weekend visitor-day totals cross-referenced against STR weekday/weekend occupancy gap"),

    ("datafy_social_ga_performance_visual",  "datafy_social_audience_overview",  "enriches", "report_period",
     "GA4 performance visual summary (sessions, engagement rate, conversions) enriches the audience overview snapshot"),

    ("datafy_campaign_pixel_fires_daily",    "datafy_attribution_media_kpis",    "context",  "fire_date→report_period",
     "Daily campaign pixel fire counts provide the day-level activity trend underlying the media attribution KPI totals"),

    ("datafy_campaign_pixel_fires_daily",    "kpi_daily_summary",                "cross_ref", "fire_date→as_of_date",
     "Daily campaign pixel activity cross-referenced against same-day STR demand to gauge campaign-to-booking lag"),

    # ── New CoStar Market Daily/Monthly (July 2026) ────────────────────────────
    ("costar_market_daily",      "costar_market_monthly",        "enriches",      "report_date→month",
     "CoStar daily submarket data aggregates into monthly grain for month-over-month performance"),

    ("costar_market_daily",      "kpi_daily_summary",            "cross_ref",     "report_date→as_of_date",
     "CoStar South OC market daily occ/ADR/RevPAR benchmarks VDP portfolio daily KPIs"),

    ("costar_market_daily",      "fact_str_metrics",             "cross_ref",     "report_date→as_of_date",
     "CoStar daily submarket performance cross-referenced with STR daily metrics for direct comp"),

    ("costar_market_monthly",    "kpi_daily_summary",            "cross_ref",     "report_period→month",
     "CoStar monthly TTM and capital market metrics benchmark VDP portfolio monthly compression"),

    ("costar_market_monthly",    "costar_monthly_performance",   "enriches",      "report_date",
     "New CoStar daily/monthly loaders supplement legacy costar_monthly_performance for trend continuity"),

    ("costar_market_daily",      "datafy_overview_kpis",         "context",       "year",
     "CoStar daily market context informs Datafy annual visitor economy segmentation"),

    # ── Visit California Resident Sentiment (July 2026) ───────────────────────
    ("visit_ca_resident_sentiment","kpi_daily_summary",          "context",       "area,report_period",
     "OC resident sentiment on tourism (support, concerns, community impact) contextualizes hotel performance"),

    ("visit_ca_resident_sentiment","datafy_overview_kpis",       "context",       "report_period",
     "Resident attitudes on visitor economy benefits/costs inform destination management narrative"),

    ("visit_ca_resident_sentiment","insights_daily",             "derived_from",  "report_period",
     "Resident sentiment feeds compute_insights.py for resident audience cards on community relations"),

    ("visit_ca_resident_sentiment","vdp_events",                 "cross_ref",     "area,report_period",
     "Major events (Ohana Fest) correlated with resident sentiment shifts in Community section"),

    # ── US Travel Inbound Market Profiles (July 2026) ───────────────────────────
    ("us_travel_inbound_market_profile","datafy_overview_airports", "enriches",    "report_period",
     "U.S. Travel overseas visitor profiles by activity (Air, Hotel, Shopping) enrich airport/origin analysis"),

    ("us_travel_inbound_market_profile","datafy_overview_kpis",   "context",       "year",
     "Overseas visitor trends contextualize Datafy domestic visitor growth and market composition"),

    ("us_travel_inbound_market_profile","visit_ca_intl_market_profiles","cross_ref","category",
     "U.S. Travel national activity profiles cross-referenced with Visit CA international market spend profiles"),

    ("us_travel_inbound_market_profile","kpi_daily_summary",      "context",       "year",
     "International visitor arrivals/category participation contextualize ADR and foreign-exchange trends"),

    # ── Demand Signals (Cross-Source Correlation Engine) ──────────────────────
    ("demand_signal_weekly",     "kpi_daily_summary",            "derived_from",  "week_date→as_of_date",
     "Demand signal weekly index computed from cross-source correlations (STR, Datafy, Google, BLS, EIA)"),

    ("demand_signal_weekly",     "fact_str_metrics",             "derived_from",  "week_date→as_of_date",
     "Demand signal aggregates STR OCC/ADR trends with external signals (weather, employment, gas prices)"),

    ("demand_signal_weekly",     "google_trends_weekly",         "enriches",       "week_date",
     "Search interest correlation matrix feeds the demand signal weekly index calculation"),

    ("demand_signal_weekly",     "weather_monthly",              "enriches",       "month",
     "Weather-demand correlation computed and stored in demand_signal_weekly for impact modeling"),

    ("demand_signal_weekly",     "eia_gas_prices",               "enriches",       "week_date",
     "Gas price-demand correlation (negative: higher gas → lower drive-market occ) in demand signal"),

    ("demand_signal_weekly",     "bls_employment_monthly",       "enriches",       "year_month",
     "Employment-demand correlation (positive: OC employment ↔ hotel occupancy) in signal index"),

    ("demand_signal_weekly",     "tsa_checkpoint_daily",         "enriches",       "data_date",
     "National air travel (TSA) correlation with local demand provides fly-market signal component"),

    ("demand_signal_weekly",     "insights_daily",               "derived_from",  "week_date",
     "Demand signal index feeds compute_insights.py forward-looking visitor/dmo demand outlook cards"),

    # ── BTS Transit (regional trip mobility) ───────────────────────────────────
    ("bts_route_passengers",     "kpi_daily_summary",            "context",       "month",
     "Regional transit passenger volume provides macro mobility context for regional hotel demand"),

    ("bts_route_passengers",     "datafy_overview_kpis",         "context",       "month",
     "BTS ridership trends contextualize visitor trip volume — transit accessibility → visitation"),

    ("bts_route_passengers",     "demand_signal_weekly",         "enriches",       "month",
     "Transit mobility trends incorporated into cross-source demand signal correlation"),

    # ── STVR Market Summary (Vacation Rental Competitive Set) ──────────────────
    ("stvr_market_summary",      "fact_str_metrics",             "cross_ref",     "month",
     "Vacation rental market occupancy/ADR cross-referenced with hotel STR metrics for total-lodging picture"),

    ("stvr_market_summary",      "kpi_daily_summary",            "cross_ref",     "month",
     "STVR occupancy and rates alongside hotel KPIs show competitive pressure on pricing"),

    ("stvr_market_summary",      "datafy_overview_kpis",         "context",       "report_period",
     "STVR availability contextualize visitor accommodation choices (hotel vs. short-term rental)"),

    # ── Weather Forecast (real-time operational signal) ────────────────────────
    ("weather_forecast",         "kpi_daily_summary",            "context",       "forecast_date→as_of_date",
     "14-day weather forecast drives same-week occupancy pacing adjustments and pricing signals"),

    ("weather_forecast",         "vdp_events",                   "context",       "forecast_date→event_date",
     "Beach events (Ohana Fest, whale watching) weather-sensitive; forecast affects attendance"),

    ("weather_forecast",         "insights_daily",               "derived_from",  "forecast_date",
     "Weekend weather forecast informs visitor audience cards (best_value, booking_timing)"),

    ("weather_forecast",         "ca_state_parks_visitation",    "cross_ref",     "forecast_date",
     "Forecast quality affects park day-use visitation proxy for overall beach tourism"),

    # ── Ticketmaster Events (Regional Demand Drivers) ───────────────────────
    ("ticketmaster_events",      "demand_signal_weekly",         "enriches",      "event_date",
     "Regional concert/sports events factored into weekly demand signal correlation computation"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Schema + UPSERT
# ─────────────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS table_relationships (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    table_a           TEXT    NOT NULL,
    table_b           TEXT    NOT NULL,
    relationship_type TEXT    NOT NULL,
    join_key          TEXT,
    description       TEXT,
    created_at        TEXT    DEFAULT (datetime('now')),
    UNIQUE(table_a, table_b, relationship_type)
);
"""

# Add created_at column to legacy schema if missing
MIGRATE_SQL = """
ALTER TABLE table_relationships ADD COLUMN created_at TEXT DEFAULT (datetime('now'));
"""

UPSERT_SQL = """
INSERT INTO table_relationships
    (table_a, table_b, relationship_type, join_key, description, created_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(table_a, table_b, relationship_type) DO UPDATE SET
    join_key    = excluded.join_key,
    description = excluded.description,
    created_at  = excluded.created_at;
"""


def get_existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {r[0] for r in rows}


def build_relationships() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(DDL)
    # Migrate: add created_at column if the table existed before this script
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(table_relationships)").fetchall()}
    if "created_at" not in existing_cols:
        try:
            conn.execute(MIGRATE_SQL)
        except Exception:
            pass
    conn.commit()

    existing = get_existing_tables(conn)
    now      = datetime.now().isoformat(timespec="seconds")

    inserted = 0
    skipped  = 0

    for table_a, table_b, rel_type, join_key, desc in RELATIONSHIPS:
        # Only insert if BOTH tables exist in the DB
        if table_a not in existing:
            print(f"  SKIP (table_a not found): {table_a}")
            skipped += 1
            continue
        if table_b not in existing:
            print(f"  SKIP (table_b not found): {table_b}")
            skipped += 1
            continue
        conn.execute(UPSERT_SQL, (table_a, table_b, rel_type, join_key, desc, now))
        inserted += 1

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM table_relationships").fetchone()[0]
    print(f"build_table_relationships: {inserted} upserted, {skipped} skipped (table not yet loaded)")
    print(f"  Total relationships in DB: {total}")

    conn.close()


if __name__ == "__main__":
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Rebuilding table_relationships …")
    build_relationships()
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Done.")
