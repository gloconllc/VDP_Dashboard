# Datafy Source Files

This folder holds **normalized CSV intake files** that feed `data/analytics.sqlite`.
Raw source files (STR exports, Datafy PDFs, GA4 CSV exports) live in `downloads/`
at the project root — that folder is gitignored so large/proprietary files don't
end up in the repo.

**Two-folder model:**
```
downloads/         ← drop raw STR exports, Datafy PDFs, GA4 exports here (gitignored)
data/datafy/       ← manually transcribed / cleaned CSVs (committed to git)
```

---

## Folder Structure

```
data/datafy/
├── overview/                   ← Annual Pull Deep Dive Visitation Report
│   ├── kpis_<period>.csv
│   ├── dma_<period>.csv
│   ├── demographics_<period>.csv
│   ├── clusters_<period>.csv
│   ├── spending_<period>.csv
│   └── airports_<period>.csv
├── attribution_website/        ← Attribution Website Report (quarterly)
│   ├── kpis_<period>.csv
│   ├── top_markets_<period>.csv
│   ├── dma_<period>.csv
│   ├── channels_<period>.csv
│   ├── clusters_<period>.csv
│   └── demographics_<period>.csv
├── attribution_media/          ← Attribution Media / Campaign Report
│   ├── kpis_<period>.csv
│   └── top_markets_<period>.csv
└── social/                     ← Google Analytics 4 exports
    ├── traffic_<period>.csv
    ├── audience_<period>.csv
    └── pages_<period>.csv
```

---

## Naming Convention

`<table_prefix>_<period_label>.csv`

| Period type   | Label format   | Example                          |
|---------------|----------------|----------------------------------|
| Annual        | `YYYY-annual`  | `kpis_2025-annual.csv`           |
| Fiscal year   | `YYYY-YY-annual` | `kpis_2025-26-annual.csv`      |
| Quarter       | `YYYY-QN`      | `kpis_2025-Q3.csv`               |
| Custom range  | `YYYY-MM`      | `kpis_2026-01.csv`               |

The period label is for human reference only.  The actual date range is stored
in the `report_period_start` / `report_period_end` columns inside the CSV, and
those values are what the database uses.

---

## Required Columns (all files)

Every CSV **must** include:

| Column               | Format     | Example      |
|----------------------|------------|--------------|
| `report_period_start`| YYYY-MM-DD | `2025-01-01` |
| `report_period_end`  | YYYY-MM-DD | `2025-12-31` |

Additional columns vary by file — see the headers of existing files as
templates.

---

## Column Reference by File Type

### overview/kpis
`report_period_start, report_period_end, compare_period_start, compare_period_end,
report_title, data_source, total_trips, avg_length_of_stay_days, avg_los_vs_compare_days,
day_trips_pct, day_trips_vs_compare_pct, overnight_trips_pct, overnight_vs_compare_pct,
one_time_visitors_pct, repeat_visitors_pct, in_state_visitor_days_pct,
in_state_vd_vs_compare_pct, out_of_state_vd_pct, out_of_state_vd_vs_compare_pct,
in_state_spending_pct, out_of_state_spending_pct, locals_pct, locals_vs_compare_pct,
visitors_pct, visitors_vs_compare_pct, local_spending_pct, visitor_spending_pct,
total_trips_vs_compare_pct`

### overview/dma
`report_period_start, report_period_end, dma, visitor_days_share_pct,
visitor_days_vs_compare_pct, spending_share_pct, avg_spend_usd,
avg_length_of_stay_days, trips_share_pct`

### overview/demographics
`report_period_start, report_period_end, dimension, segment, share_pct`
- `dimension` values: `age` | `income` | `household_size`

### overview/clusters
`report_period_start, report_period_end, cluster, visitor_days_share_pct, vs_compare_pct`

### overview/spending
`report_period_start, report_period_end, category, spend_share_pct, spending_correlation_pct`

### overview/airports
`report_period_start, report_period_end, airport_name, airport_code, passengers_share_pct`

### attribution_website/kpis
`report_period_start, report_period_end, visitation_window_start, visitation_window_end,
report_title, market_radius_miles, website_url, cohort_spend_per_visitor, manual_adr,
attributable_trips, unique_reach, est_impact_usd, total_website_sessions,
website_pageviews, avg_time_on_site_sec, avg_engagement_rate_pct`

### attribution_website/top_markets
`report_period_start, report_period_end, cluster_type, total_trips,
visitor_days_observed, est_room_nights, est_avg_length_of_stay_days, est_impact_usd,
top_dma, dma_share_of_impact_pct, dma_est_impact_usd`
- `cluster_type` values: `destination` | `resorts` | `hotels`

### attribution_website/dma
`report_period_start, report_period_end, dma, total_trips,
avg_los_destination_days, vs_overall_destination_days`

### attribution_website/channels
`report_period_start, report_period_end, acquisition_channel, attribution_rate_pct,
sessions, avg_time_on_site_mmss, engagement_rate_pct, attributable_trips_dest,
attributable_trips_hotels, attributable_trips_resorts`

### attribution_website/clusters
`report_period_start, report_period_end, area, pct_of_total_destination_trips, area_type`
- `area_type` values: `cluster` | `poi`

### attribution_website/demographics
`report_period_start, report_period_end, cluster_type, dimension, segment, share_pct`

### attribution_media/kpis
`report_period_start, report_period_end, visitation_window_start, visitation_window_end,
report_title, campaign_name, market_radius_miles, program_type,
cohort_spend_per_visitor, manual_adr, total_impressions, unique_reach,
attributable_trips, est_campaign_impact_usd, roas_description,
total_impact_usd, total_investment_usd`

### attribution_media/top_markets
`report_period_start, report_period_end, cluster_type, total_trips,
visitor_days_observed, est_room_nights, est_avg_length_of_stay_days, est_impact_usd,
top_dma, dma_share_of_impact_pct, dma_est_impact_usd`

### social/traffic
`report_period_start, report_period_end, source, sessions, screen_page_views,
avg_session_duration_mmss, engagement_rate_pct`

### social/audience
`report_period_start, report_period_end, audience_name, sessions, screen_page_views,
avg_session_duration_mmss, engagement_rate_pct, conversions`

### social/pages
`report_period_start, report_period_end, page_title, page_views, page_path`

---

## Monthly Workflow

1. **Download** the new Datafy PDF or GA4 CSV export → save to `downloads/`
   (gitignored, so the file stays local).
2. **Transcribe** the values into the matching CSV template in `data/datafy/`
   (for Datafy PDFs) or copy the GA4 export directly (for social/ files).
3. **Name** the file using the period label convention (e.g., `kpis_2026-Q1.csv`).
4. **Save** it to the correct subfolder under `data/datafy/`.
5. **Run** the pipeline:
   ```bash
   python scripts/run_pipeline.py
   ```
5. **Commit** both the new CSV and the updated `data/analytics.sqlite`:
   ```bash
   git add data/datafy/ data/analytics.sqlite
   git commit -m "Add Datafy Q1 2026 reports"
   git push
   ```

The loader automatically detects every CSV in this folder.  Existing periods
are replaced (not duplicated) if you re-run.  Historical periods are preserved.

---

## Upsert Safety

The loader uses a **period-level replace** strategy:

- Before inserting a CSV's rows, it deletes any existing rows in the target
  table that share the same `(report_period_start, report_period_end)`.
- Re-running the pipeline for the same period is safe — it replaces, not
  duplicates.
- Adding a new period CSV accumulates it alongside existing history.

---

## Files Present

| File | Period | DB Table | Rows |
|------|--------|----------|------|
| overview/kpis_2025-annual.csv | 2025 full year | datafy_overview_kpis | 1 |
| overview/dma_2025-annual.csv | 2025 full year | datafy_overview_dma | 14 |
| overview/demographics_2025-annual.csv | 2025 full year | datafy_overview_demographics | 12 |
| overview/clusters_2025-annual.csv | 2025 full year | datafy_overview_cluster_visitation | 9 |
| overview/spending_2025-annual.csv | 2025 full year | datafy_overview_category_spending | 10 |
| overview/airports_2025-annual.csv | 2025 airport season | datafy_overview_airports | 10 |
| attribution_website/kpis_2025-Q3.csv | Jul 10–Aug 27, 2025 | datafy_attribution_website_kpis | 1 |
| attribution_website/top_markets_2025-Q3.csv | Jul 10–Aug 27, 2025 | datafy_attribution_website_top_markets | 12 |
| attribution_website/dma_2025-Q3.csv | Jul 10–Aug 27, 2025 | datafy_attribution_website_dma | 9 |
| attribution_website/channels_2025-Q3.csv | Jul 10–Aug 27, 2025 | datafy_attribution_website_channels | 3 |
| attribution_website/clusters_2025-Q3.csv | Jul 10–Aug 27, 2025 | datafy_attribution_website_clusters | 20 |
| attribution_website/demographics_2025-Q3.csv | Jul 10–Aug 27, 2025 | datafy_attribution_website_demographics | 25 |
| attribution_media/kpis_2025-26-annual.csv | Aug 2025–Mar 2026 | datafy_attribution_media_kpis | 1 |
| attribution_media/top_markets_2025-26-annual.csv | Aug 2025–Mar 2026 | datafy_attribution_media_top_markets | 14 |
| social/traffic_2025-annual.csv | 2025 full year | datafy_social_traffic_sources | 26 |
| social/audience_2025-annual.csv | 2025 full year | datafy_social_audience_overview | 1 |
| social/pages_2025-annual.csv | 2025 full year | datafy_social_top_pages | 99 |
