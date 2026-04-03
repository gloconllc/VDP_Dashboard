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
