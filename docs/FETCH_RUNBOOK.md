# VDP Data Fetch Runbook

Written 2026-10-07. This is the script Claude follows (Claude in Chrome on
John's Mac) for on-demand data pulls. Claude never types passwords: John
must already be logged in to SharePoint and Datafy in Chrome. If a login
page appears, stop and tell John.

## 0. Before every fetch: remind John about CoStar

CoStar's Terms of Use prohibit automated agents. On 2026-10-07 the portal
blocked the Claude extension ("AI Agent Activity Detected"). Claude must
never open costar.com or costargroup.com.

So John pulls CoStar by hand and uploads it to the Visit Dana Point
SharePoint. At the start of every STR or Datafy fetch, Claude asks John
whether he has uploaded a fresh CoStar pull, and waits for his answer
before moving on.

CoStar location (since 2026-10-07):
`/sites/BusinessIntelligenceResearch/Shared Documents/Business Intelligence/Reports/Costar`
- `Daily/`: daily_MM_DD_YY.xlsx, daily_seg_MM_DD_YY.xlsx
- `Monthly/`: monthly_MM_DD_YY.xlsx, monthly_seg_MM_DD_YY.xlsx
- `Hospitality Reports/`: CoStar market PDFs (Newport Beach-Dana Point
  submarket, Orange County market, and Orange County capital market)
- Participation (`Particpation_MM_DD_YY.xlsx`) sits in the folder root.

Steps: list the folder recursively with the SharePoint REST call in
section 1. Sort by TimeLastModified and take the newest batch. Skip any
file whose canonical name already exists in `data/costar/`. Download each
file with download.aspx. Check the header: xlsx starts with `PK` (504b0304)
and PDFs start with `%PDF`. Copy flat into `data/costar/` under the loader
names. Rename typos on the way in: John's 2026-10-07 upload was named
`monhtly_10_07_26.xlsx` and was filed as `monthly_10_07_26.xlsx`. Never
overwrite an existing file.

Note: SharePoint reports a larger `Length` for xlsx than the downloaded
size (Office property promotion). That is normal, so compare headers,
not sizes.

Fallback if SharePoint is unavailable: John saves the files into the
repo's `downloads/` folder and runs `./scripts/ingest_costar.sh`.

## 1. STR: Visit Dana Point SharePoint

Source folder (Heather's flow drops the weekly STR export here):
`https://visitdanapointcom.sharepoint.com/sites/BusinessIntelligenceResearch`
`/Shared Documents/Business Intelligence/Reports/STR`

File naming: `YYYY-MM-DD_VisitDanaPoint_YYYYMMDD.xls` (weekly, the second
date is the week ending) and `YYYY-MM-DD_VisitDanaPoint_YYYYMM.xls`
(monthly). Real files are about 500 KB and start with the OLE header
`D0 CF 11 E0`. Anything around 24 bytes is a failed download.

Steps:
1. Open any page on visitdanapointcom.sharepoint.com in a Claude tab.
2. List the folder with the SharePoint REST API from the page (same-origin,
   uses John's session, no clicking):
   `GET /sites/BusinessIntelligenceResearch/_api/web/GetFolderByServerRelativeUrl('<folder>')?$expand=Files`
   with header `Accept: application/json;odata=nometadata`.
3. Compare against `data/str/weekly/` and download only names not already there:
   `/sites/BusinessIntelligenceResearch/_layouts/15/download.aspx?SourceUrl=<encoded path>`
   (create an anchor with `download` and click it). Files land in `~/Downloads`.
4. Check size and OLE header, then copy into `data/str/weekly/` (never overwrite).
5. Run the loaders: `load_str_daily_sqlite.py`, `load_str_monthly_sqlite.py`,
   `compute_kpis.py`, `compute_insights.py` (or the full `run_pipeline.py`).

Verified live 2026-10-07: pulled `2026-10-07_VisitDanaPoint_20260927.xls`
(527,872 bytes, valid OLE) into `data/str/weekly/`.

The same SharePoint `Reports` folder also has `Costar/` and `Datafy/`
subfolders, but as of 2026-10-07 they only hold August 2026 material
already loaded. The Datafy portal is the live Datafy source.

## 2. Datafy: portal.datafy.com

Logged-in landing page shows "Welcome back, Visit Dana Point!" and the
active window (2026-10-07: Year to Date, 1/1/2026 - 9/12/2026).
No bot block observed.

Tabs: Overview, Destination Performance, Origin Markets, Business
Performance, Marketing & Advertising.

Per-card CSV export (verified on Origin Markets > "DMA Visitation"):
1. Click the card's three-dot menu. A settings panel opens with a download
   icon and a small arrow next to it in the card header.
2. Click the small arrow (not the icon itself, which exports a JPG straight away).
3. The export dialog opens. Click the arrow on the "Export JPG" button and
   choose "Export as CSV".
4. The file is generated in the browser and lands in `~/Downloads` with
   Datafy's own name, for example `TopMarkets_Export (6).csv`.

Some cards (for example Datafy Snapshot) have CSV disabled; those only
export as an image or PDF.

Period stamping: Datafy CSVs carry no dates. Before filing into
`data/datafy/overview/` (or the matching subfolder), rename each file to
add the window shown on the page as `_DD-MM-YYYY_to_DD-MM-YYYY`, for example
`TopMarkets_Export_01-01-2026_to_12-09-2026.csv`. `_extract_period()` in
`load_datafy_reports.py` reads this. Never change `FOLDER_DEFAULT_PERIODS`.

### Card map (verified 2026-10-07, Home dashboard, window YTD 1/1/2026-9/12/2026)

Save as `data/datafy/Datafy_MM_DD_YY/<Datafy name without (N)>_DD-MM-YYYY_to_DD-MM-YYYY.csv`.

| Home tab | Card | Datafy file | Loader table |
|---|---|---|---|
| Origin Markets | DMA Visitation | TopMarkets_Export | datafy_overview_top_markets |
| Origin Markets | DMA Spending | DynamicHomePage-Top Markets_Export | datafy_overview_spending_by_market |
| Origin Markets | Top DMAs by Trips | ComparisonTopMarketsVisual_Export | unmapped |
| Origin Markets | Share of Trips by State | StateMap_Export | datafy_overview_state_origin |
| Origin Markets | Locals vs Visitors in Market | CSV disabled | - |
| Origin Markets | Local vs Visitor Spending | DynamicHomePage-Local Vs Visitor_Export | datafy_overview_local_visitor_spend |
| Origin Markets | Top Journey Origin Airports | TopJourneyOriginAirportsForDestinationAirports_Export | datafy_overview_airports |
| Origin Markets | Top Countries | TopCountries_Export | datafy_overview_top_countries |
| Origin Markets | Top Age / Income / Household | DynamicHomePage-TopBoxes_Export | datafy_overview_demographics |
| Destination Performance | Visitation Key Indicators | OverallNumbers_Export | datafy_overview_total_kpis |
| Destination Performance | In/Out of State Visitation | CSV disabled | - |
| Destination Performance | In/Out of State Spending | DynamicHomePage-In Vs Out Of State_Export | datafy_overview_instate_outstate |
| Destination Performance | Trips by Length of Stay | LengthOfStayDonut_Export | datafy_overview_los_distribution |
| Destination Performance | Repeat vs One Time | RepeatVsOneTimeTripsComparison_Export | unmapped |
| Business Performance | Cluster Visitation | TopPOIs_Export | datafy_overview_top_pois |
| Business Performance | Category Spending | DynamicHomePage-Top Markets_Export **(1)** | datafy_overview_spending_by_category |
| Business Performance | Accommodations Correlation | Spending Correlation by Category_Export | exports empty |

Naming trap: Category Spending and DMA Spending both download as
`DynamicHomePage-Top Markets_Export`. The loader tells them apart only by the
`(1)` suffix, so keep `(1)` on the Category Spending file and drop it from
the DMA Spending file.

Click positions shift with the dialog height. Take a screenshot of each
dialog and click the small arrow right of "Export JPG". "Export as CSV" sits
about 29 px left and 19 px below that arrow. Close with Cancel, not Escape.

### Apps-grid modules (verified 2026-10-08)

Each module has its own date window. Read it from the page's filter bar and stamp it into the file name.

| Module / page | Window on 10/8 | Cards exported (Datafy name) | Where filed |
|---|---|---|---|
| Analytics > Overview | 1/1/25-9/12/26 | VisitationByYear, Visitation, WeekdayVsWeekend (also OverallNumbers, StateMap, LengthOfStayDonut, TopPOIs, TopMarkets, TopDemographics) | Datafy_MM_DD_YY/ |
| Analytics > Spending > Advanced Spending > Overview | 1/1/21-10/2/26 | Peak Insights, Top Markets, Top Hotel Markets (set the category card to Accommodations), Spending By Year, Enhanced Spending Density Map (also In Vs Out Of State, Local Vs Visitor) | Datafy_MM_DD_YY/ |
| Advanced Spending > Spending Insights | same | Top Spending category / Top Spending market trend card (toggle tabs) | top level, `_MM_DD_YY` suffix (load_datafy_extended reads top level only) |
| Social Analytics | property **Dana Point - GA4**, last 30 days | Acquisition-Channels, TopSearches, DeviceBreakdown (other visuals errored on Datafy's side 10/8) | Datafy_MM_DD_YY/ |
| Attribution Insights > Overview | FY26-27 Annual Campaign, 7/8/26-10/8/26 | Media Overview Totals, Media Attribution Groups, Media Overview Top Markets, Overview Media Breakdown | Datafy_MM_DD_YY/ |
| Datafy Advertising > Overview | FY26-27 campaign, 7/8/26-10/6/26 | Overview Dashboard (Campaign Impact), TopMarketsVisual, AttributionGroupsSummaryVisual, TopPerformersVisual | top level, `_MM_DD_YY` suffix (load_datafy_advertising reads top level only) |

**Overlap rule:** do not file a second window for a table the home-dashboard
YTD set already feeds. The overlapping files are TopMarkets, StateMap,
LengthOfStayDonut, TopPOIs, OverallNumbers, TopDemographics/TopBoxes,
In Vs Out Of State, Local Vs Visitor, and the category and market spending
tables. Two windows ending on the same date make "freshest period" ambiguous
for the app. On 10/8 these were exported but left in ~/Downloads.

**Mechanics that work:** click the card's three-dot menu, then the small arrow
20 px left of it. Use `find` "Export JPG button in export dialog", then
`scroll_to` it and take a screenshot. Click the arrow right of the button,
then `find` "Export as CSV" and click it by ref. Close with Cancel.

**Still not pulled:** home Marketing & Advertising tab cards; Advertising
Traditional KPIs and Line Item Performance pages; Spending Correlation and
Peak Spending sub-tabs (no loaders); the GA visuals that error out.

## 3. Load

After fetching, run `./scripts/refresh_all.sh` (or `--no-push`). It runs the
STR, CoStar, and Datafy loaders, then KPIs, insights, relationships, DB
optimize, and audit. It runs no network fetches and opens no portal logins.
