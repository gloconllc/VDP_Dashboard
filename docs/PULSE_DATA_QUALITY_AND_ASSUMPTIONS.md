# Dana Point PULSE: Data Quality Audit and Analysis Assumptions Log

Prepared September 25, 2026, alongside the board view redesign of `dashboard/app.py`.
Scope: the three current sources behind the board view (STR, CoStar, Datafy), the
KPI and insight layers built from them, and the pipeline that refreshes them.

The board view is written for Visit Dana Point's board of directors. Every number
on it is computed from `data/analytics.sqlite` at page load by `dashboard/pulse_data.py`,
turned into plain-language answers by `dashboard/pulse_story.py`, and recomputed for
whichever data window is selected. No figure on the page is typed by hand.

## 1. Sources on file

| Source | Table(s) | Coverage on file | Last pull | Refresh path |
|---|---|---|---|---|
| STR daily, VDP Select comp set | `fact_str_metrics` (daily) | Feb 28, 2024 to Aug 1, 2026 | Aug 1, 2026 file | Power Automate, Dropbox, `fetch_str_dropbox.py` |
| STR monthly | `fact_str_metrics` (monthly) | Jan 1987 to Jun 2026 | Jun 2026 file | Same as above |
| STR weekly six-market peer set | `fact_str_group_metrics` | 24 weekly reports, Dec 1, 2025 to Aug 23, 2026 | Week ending Aug 23 | Same as above |
| CoStar daily, same comp set | `costar_market_daily` | Jul 25, 2024 to Aug 29, 2026 | Sep 7, 2026 | Manual portal export |
| CoStar monthly | `costar_market_monthly` | Jan 1987 to Jul 2026 | Sep 7, 2026 | Manual portal export |
| CoStar segmentation | `costar_market_*_segment` | Transient, Group, Contract through Aug 29, 2026 | Sep 7, 2026 | Manual portal export |
| CoStar submarket report | `costar_annual_performance`, `costar_segment_room_split` | Report dated Sep 8, 2026, YTD through Jul 2026 | Sep 8, 2026 | Manual PDF download |
| Datafy visitor economy | `datafy_overview_*` | Spend Mar 2021 to Jul 2026; visitor days Jan 2023 to Jul 2026 | Sep 10 load | Manual export to `data/datafy/` |
| Datafy advertising | `datafy_advertising_*` | Snapshot Sep 7, 2026 | Sep 7, 2026 | Manual export |
| Insights engine | `insights_daily` | 48 insights for Sep 25, 2026 | Sep 25, 2026 | `compute_insights.py` |

## 2. Data quality findings

Severity follows the data-quality-audit scale: critical, high, medium, low.

| # | Finding | Dimension | Severity | Resolution |
|---|---|---|---|---|
| F1 | The STR daily comp-set feed stops at Aug 1, 2026, four weeks behind CoStar. | Timeliness | High | CoStar's daily export for the identical VDP Select comp set now extends `kpi_daily_summary` from Aug 2 to Aug 29 (28 rows, tagged `data_source = 'CoStar'`). Across 738 overlapping days the two sources differ by 0.007 occupancy points on average, so the extension is the same hotels measured the same way. STR remains the source wherever it has a row. |
| F2 | STR weekly reports are missing for May 3 to 17, May 31 to Jun 28, Jul 19 to Aug 16, and every week after Aug 23. | Completeness | High | Open. Root cause is in intake (section 4). The board view weights peer ranks across whatever weekly reports fall inside the window and says how many it used. |
| F3 | `data/str/weekly/2026-09-02_VisitDanaPoint_20260823.xls` is 27 bytes, and its entire content is the text of its own file name. A real 524 KB copy sits beside it as `... (1).xls`. | Validity | Medium | The loaders skip it. The content pattern points to the STR Power Automate flow's "Create file" step writing the attachment name instead of the attachment content (section 4). |
| F4 | `load_datafy_advertising.py` stamped each run with the run date, so every pipeline run created a duplicate advertising snapshot. | Uniqueness | Medium | Fixed. The snapshot date now comes from the export's file name, and an identical re-load reuses the existing snapshot date. |
| F5 | `run_pipeline.py` in the working tree had dropped the Datafy advertising step. | Completeness | High | Fixed. Step 4a restored. |
| F6 | Quarterly compression counted nights above 80% with `>`, so a night at exactly 80.0% was missed. | Accuracy | Low | Fixed. `>=`, matching STR's "80% or higher" definition. |
| F7 | The previous page averaged daily ratios for occupancy and ADR and reported occupancy change as a percent. | Accuracy | Medium | Fixed in the board view. KPIs are rooms-weighted and occupancy change is reported in points. |
| F8 | Datafy's monthly spending is card-observed spend, and its visitor days are modeled from location data. Dividing one by the other produced "$13.83 per visitor day," which is not a real spending level. | Validity | Medium | Fixed. The board compares the two growth rates only, and the tile now shows the top origin market. |
| F9 | Two Datafy exports disagree on 2025 scale: 3,551,929 trips in the Annual Pull Deep Dive against 12,846,073 in the newer total-KPI export. | Consistency | Medium | Open. The board uses the monthly series, which matches the Annual Pull scale, and does not show the conflicting total. Confirm the definition with Datafy. |
| F10 | CoStar's tier room counts sum to 11,700 while its narrative total is 12,000. | Consistency | Low | Captions compute tier shares from the tier sum and describe the total as "about 12,000." |
| F11 | The insights engine wrote "in ~1 days," said the Q3 peak "starts in ~279 days" while Q3 was underway, and advised starting a campaign 90 days out on the day of Ohana Fest. | Accuracy | Medium | Fixed. Timing-aware wording, Ohana Fest anchored to the events calendar date, and a grammar pass at the single write point. |
| F12 | Several `insights_daily` categories (the group revenue, TBID, and competitive-set items) are derived from `costar_chain_scale_breakdown` and `costar_competitive_set`, which are placeholder tables. | Validity | High | The board view excludes them and draws only on STR, Datafy, and the events calendar. Open: retire or rebuild those categories from `costar_participation` and the real segmentation tables. |
| F13 | Datafy stores zero rows for months it has not yet reported. | Validity | Low | Handled. Zeros are dropped and the series is cut at Datafy's own reported end month. |
| F14 | Datafy's two monthly exports store the month differently ("Mar 2021" against "Mar" plus a year column). | Consistency | Low | Handled in one parser. |

## 3. Analysis assumptions log

Confidence is high, medium, or low. Items marked critical are low or medium
confidence with a high impact if wrong, and each carries a validation step.

| ID | Assumption | Category | Rationale | Confidence | Validation |
|---|---|---|---|---|---|
| A1 | The VDP Select Portfolio comp set represents Dana Point hotel performance, and CoStar's daily export covers the same hotels. | Data | Named comp set in both exports; 738-day agreement check. | High | Re-run the agreement check whenever either export changes. |
| A2 | Year over year compares the same window shifted 364 days, so weekdays line up. | Business logic | Hotel demand is weekday-driven; a 365-day shift misaligns Fridays and Saturdays. | High | None needed. |
| A3 | Occupancy is rooms sold over rooms available, ADR is room revenue over rooms sold, and RevPAR is room revenue over rooms available, all summed across the window. | Business logic | STR's own definitions. | High | Spot-check against STR's monthly totals. |
| A4 | When daily prior-year coverage is under 90% of the window, the comparison falls back to monthly totals. | Statistical | Daily history starts Feb 2024, so a 24-month window has no daily prior year. | Medium | The page labels the fallback. |
| A5 | Revenue totals are not compared year over year when room supply changed by more than 2%. | Business logic | A property entering or leaving the comp set changes totals without any change in performance. | Medium | Confirm participation changes with `costar_participation`. |
| A6 | The RevPAR change is split into occupancy and rate parts using log shares. | Statistical | The two parts add up exactly on a log scale. | Medium | Reported rounded, as an approximation. |
| A7 | A compression night is occupancy at or above 80% (and at or above 90%); a quarter is complete at 89 or more nights. | Business logic | STR convention; quarters run 90 to 92 days. | High | None needed. |
| A8 | Peer ranks are rooms-weighted across the STR weekly reports inside the window, falling back to the latest report when none fall inside. | Data | Weekly reports are the only six-market source. | Medium, critical while F2 is open | Close the weekly gap (section 4). |
| A9 | The CoStar year-to-date benchmark runs through the last month in `costar_market_monthly`, and Dana Point hotels are computed for the same months. | Business logic | Like-for-like months. | High | None needed. |
| A10 | Datafy figures use the months that overlap the data window; when none overlap, the latest month on file is shown and labeled. | Data | Datafy publishes monthly with a lag. | Medium | The page labels the fallback. |
| A11 | The 0.64 correlation between monthly hotel revenue and visitor spending (65 months) is descriptive, not causal. | Statistical | Both follow the same seasons. | High | The page says so. |
| A12 | Event dates come from the seeded `vdp_events` calendar, and last year's performance is the STR occupancy 364 days earlier. | Data | The live events site is script-rendered. | Medium | Confirm dates each season. |
| A13 | Group share comes from CoStar Transient, Group, and Contract segmentation for the same comp set; the STR weekly group share is shown separately and labeled. | Data | Two independent reads of the same business mix. | Medium | Compare the two each month. |
| A14 | Advertising return is Datafy's estimated visitor impact divided by media spend. | Data | Datafy's attribution model, not an audited figure. | Low to medium, critical before any budget decision | Ask Datafy for the attribution method and window. |
| A15 | Out-of-state share is Datafy's out-of-state share of visitor spending. | Data | Spending share is the board-relevant measure. | Medium | None needed. |

## 4. Pipeline status: STR, CoStar, and Datafy

Every loader was run against the files on disk on September 25, 2026, and each
exited cleanly. The gaps are in intake, meaning getting new files onto disk,
not in loading them.

### STR

Working: `load_str_daily_sqlite.py`, `load_str_monthly_sqlite.py`, and
`load_str_multiseg.py` load every real file present, including the week ending
Aug 23, 2026, which had been sitting unloaded.

Not working:

1. No weekly schedule is active. `str_weekly_sync.yml` is manual-only, and the
   Sunday schedule in `full_sync.yml` is commented out, so new STR files are only
   picked up by the monthly pipeline on the 1st or a manual run.
2. The Power Automate flow appears to write the attachment name rather than the
   attachment content (finding F3). In the flow's "Create file" action, set File
   Content to the "Attachments Content" value from the "For each" attachment loop,
   and confirm "Include Attachments" is set to Yes on the email trigger.
3. Weekly reports for the weeks listed in F2 are not in the Dropbox-synced folder.
   Once the flow is fixed, request those weeks from STR or re-save them from the
   original emails.

Recommended: re-enable the Thursday cron in `str_weekly_sync.yml` (`0 15 * * 4`)
after the flow fix, so each weekly file reaches the live site the day it arrives.

### CoStar

Working: all CoStar loaders, including daily through Aug 29, monthly through
Jul 2026, segmentation, participation, and the Sep 8 submarket report.

Not working: the unattended portal fetch. `scripts/fetch_costar_portal.py` logs
in and opens the VDP Select saved search, then stops (its step screenshots in
`logs/costar_fetch_screens/` show the final state; that folder is now kept out of
git because one capture showed the portal login). Segmentation,
participation, and the market-report download are still stubs, and
`costar_fetch.yml` and `costar_sync.yml` are in `Claude outputs/` rather than
`.github/workflows/`, so neither can be run from GitHub.

Recommended: one watched run with the portal open to confirm the current export
selectors, then move both workflow files into `.github/workflows/`. Until then,
the manual weekly export into `data/costar/` followed by `run_pipeline.py` is the
proven path.

### Datafy

Working: all Datafy loaders, including monthly spending through Jul 2026,
visitor days, markets, categories, and the Sep 7 advertising snapshot.

Not automated: Datafy offers no API or scheduled export for this account, so a
monthly manual export into `data/datafy/`, named with its period, followed by
`run_pipeline.py`, remains the path. A Power Automate folder watch on that
folder can trigger the reload once the file lands.

## 5. Validation performed

All edited modules compile under Python 3.11. The app was run headless against
the refreshed database and driven with Playwright at 1,400 px and 390 px widths
across all seven data windows, in admin mode, and through a section dialog, with
zero exceptions. The PDF report generated at 10 pages, and the flipbook, section
picker, continuous scroll, notes, and report repository all rendered.
