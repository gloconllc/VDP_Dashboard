# Project: VDP Analytics (Visit Dana Point)

DMO tourism analytics platform — ETL pipeline, SQLite brain, Streamlit dashboard, Claude AI Analyst panel.
Owner: John Picou | Org: gloconllc | Repo: VDPDashboard

---

## Data Hierarchy (NEVER violate)

- **Layer 1 — Truth:** STR daily/monthly exports, Datafy event data, TBID assessment docs. These are vetted. Always cite these first.
- **Layer 1 (Current):** Datafy, CoStar, STR are the CURRENT data sources. Always present these as current performance.
- **Layer 1.5 — Historical Reference:** Zartico (Jun 2025 snapshot) is historical reference only. Use for trend comparison and to tell the growth story. NEVER present Zartico as current data.
- **Layer 2 — Context:** FRED hotel pricing index, CA State TOT data, JWA passenger counts, Visit California forecasts.
- **Layer 2.5 — Social Performance:** Later.com social media exports (Instagram, Facebook, TikTok). Current social data. Use for digital/social narrative alongside STR and Datafy. Files in `data/later/IG/`, `data/later/FB/`, `data/later/TikTok/`. Parsed by `load_later_reports.py` into 12 `later_*` tables.
- **Layer 3 — Color:** Media, social sentiment, competitive anecdotes. Never override Layer 1 with Layer 3.

---

## Repository Structure

```
VDP_Dashboard/                      (project root)
├── CLAUDE.md                       ← YOU ARE HERE
├── dashboard/
│   └── app.py                      ← Streamlit entry point (tabs: Overview, Trends, Forward Outlook, Event Impact, Data Log)
├── data/
│   ├── analytics.sqlite            ← Single source-of-truth database (25+ tables)
│   └── datafy/                     ← Normalized CSV intake files, committed to git
├── downloads/                      ← Raw source files: STR exports, Datafy PDFs, GA4 exports (gitignored)
├── scripts/
│   ├── run_pipeline.py             ← Orchestrator: ETL → KPIs → Insights → log
│   ├── load_str_daily_sqlite.py    ← Daily STR → fact_str_metrics
│   ├── load_str_monthly_sqlite.py  ← Monthly STR → fact_str_metrics
│   ├── load_datafy_reports.py      ← Datafy visitor economy → 17 tables
│   ├── compute_kpis.py             ← Refreshes kpi_daily_summary + kpi_compression_quarterly
│   ├── compute_insights.py         ← Generates insights_daily for 4 audiences (runs DAILY)
│   ├── fetch_costar_data.py        ← CoStar market data
│   ├── fetch_external_all.py       ← Layer-2 external data orchestrator
│   ├── fetch_fred_data.py          ← External context pull
│   ├── fetch_ca_tot.py
│   ├── fetch_jwa_stats.py
│   ├── load_later_reports.py       ← Later.com social media (IG/FB/TikTok) → 12 tables (STEP 10, non-fatal)
│   └── init_sqlite_db.py           ← DB initialization
├── logs/
│   └── pipeline.log
├── .claude/
│   └── commands/
│       ├── enhance.md              ← /enhance slash command
│       ├── refresh.md              ← /refresh slash command
│       └── home-button.md          ← /home-button slash command
├── requirements.txt
├── .gitignore
└── venv/                           (excluded from git)
```

---

## SQLite Schema (data/analytics.sqlite)

### Layer 1 — STR & KPI Tables (Truth)

| Table | Purpose |
|---|---|
| `fact_str_metrics` | Long-format STR metrics (source, grain, property_name, market, submarket, as_of_date, metric_name, metric_value, unit) |
| `kpi_daily_summary` | Wide-format daily KPIs (as_of_date, occ_pct, adr, revpar, occ_yoy, adr_yoy, revpar_yoy, is_occ_80, is_occ_90) |
| `kpi_compression_quarterly` | Compression days per quarter (quarter YYYY-Qn, days_above_80_occ, days_above_90_occ) |
| `load_log` | ETL audit trail (source, grain, file_name, rows_inserted, run_at) |

### Layer 1 — Datafy Visitor Economy Tables (Truth)

| Table | Purpose |
|---|---|
| `datafy_overview_kpis` | Annual visitor overview KPIs (total_trips, overnight_pct, out_of_state_vd_pct, avg_los, etc.) |
| `datafy_overview_dma` | Feeder market DMA breakdown (dma, visitor_days_share_pct, spending_share_pct, avg_spend_usd) |
| `datafy_overview_demographics` | Visitor demographics by segment |
| `datafy_overview_category_spending` | Spending by category (accommodation, dining, retail, etc.) |
| `datafy_overview_cluster_visitation` | Visitation by area cluster type |
| `datafy_overview_airports` | Origin airports by passenger share |
| `datafy_attribution_website_kpis` | Website-attributed trips and estimated destination impact |
| `datafy_attribution_website_top_markets` | Website attribution top feeder markets |
| `datafy_attribution_website_dma` | Website attribution DMA breakdown |
| `datafy_attribution_website_channels` | Website attribution by acquisition channel |
| `datafy_attribution_website_clusters` | Website attribution by area cluster |
| `datafy_attribution_website_demographics` | Website attribution visitor demographics |
| `datafy_attribution_media_kpis` | Media campaign: attributable_trips, total_impact_usd, ROAS |
| `datafy_attribution_media_top_markets` | Media attribution top feeder markets |
| `datafy_social_traffic_sources` | GA4 web traffic sources: sessions, engagement |
| `datafy_social_audience_overview` | Website audience KPIs |
| `datafy_social_top_pages` | Top website pages by view count |

### Intelligence Tables (Generated Daily)

| Table | Purpose |
|---|---|
| `insights_daily` | Forward-looking insights for 4 audiences (as_of_date, audience, category, headline, body, metric_basis JSON, priority, horizon_days) |
| `table_relationships` | Cross-table join/derivation map (table_a, table_b, relationship_type, join_key, description) |

### Dedup Rule

`fact_str_metrics` composite key: `(source, grain, property_name, market, as_of_date, metric_name)` — daily and monthly never collide because `grain` differs.

`insights_daily` unique key: `(as_of_date, audience, category)` — one insight per audience/category per pipeline run.

### Metric Names in fact_str_metrics

`supply`, `demand`, `revenue`, `occ`, `adr`, `revpar`

### Units

- `occ` stored as decimal (0.688 = 68.8%). `kpi_daily_summary.occ_pct` stores percentage (68.8).
- `adr`, `revpar`, `revenue` in USD.
- `supply`, `demand` in room-nights.

### Table Relationships Summary

Key relationships documented in `table_relationships`:
- `fact_str_metrics` → `kpi_daily_summary` (derived_from, as_of_date)
- `kpi_daily_summary` → `kpi_compression_quarterly` (derived_from, quarter)
- `fact_str_metrics` → `datafy_overview_kpis` (cross_ref, report_period — same time window)
- `datafy_overview_dma` ↔ `datafy_attribution_website_dma` (cross_ref, dma)
- `kpi_daily_summary` → `insights_daily` (derived_from, as_of_date)
- `datafy_overview_kpis` → `insights_daily` (derived_from, report_period)
- All `datafy_*` sub-tables → their parent KPI table (enriches, report_period)

---

## TBID Assessment Structure

| Nightly Rate | Assessment Rate |
|---|---|
| ≤ $199.99 | 1.0% |
| $200.00 – $399.99 | 1.5% |
| ≥ $400.00 | 2.0% |
| Blended estimate | ~1.25% |

Formula: `TBID Revenue ≈ Room Revenue × 0.0125`
Formula: `TOT Revenue = Room Revenue × 0.10`

---

## Ohana Fest / Datafy Reference Metrics

- Event expenditure: $14.6M
- Destination spend: $18.4M
- ADR lift during event: $139
- Avg accommodation spend/trip: $1,219
- Out-of-state visitors: 68%
- Spend multiplier: 3.2×

---

## Dashboard Architecture (dashboard/app.py)

- **Framework:** Streamlit (wide layout)
- **DB connection:** `sqlite3` with `?mode=ro` (read-only)
- **Caching:** `@st.cache_data(ttl=300)` on all data loaders
- **Tabs (9):** Overview Brain, STR & Pipeline, Forward Outlook, Visitor Economy, Feeder Markets, Event Impact, Supply & Pipeline, Market Intelligence, Data & Downloads
- **AI Analyst panel:** Server-side Claude API call via `ANTHROPIC_API_KEY` env var. Key never exposed in UI.
- **Home button:** Dashboard title "VDP Analytics" in the header is a clickable link that resets to Overview tab.
- **AI system prompt:** Includes full DB schema for all 25+ tables — AI is aware of every table.

### Data Loaders (always use these names)

- `load_str_daily()` — pivots fact_str_metrics long→wide, converts occ decimal→%
- `load_kpi_daily()` — reads kpi_daily_summary
- `load_compression()` — reads kpi_compression_quarterly
- `load_load_log()` — reads load_log for Data Log tab
- `load_insights(audience=None)` — reads insights_daily (optional audience filter)
- `get_table_counts()` — returns row counts for all 23 tracked tables

---

## Pipeline (scripts/run_pipeline.py)

Execution order:

| Step | Script | Fatal? |
|---|---|---|
| 1 | `load_str_daily_sqlite.py` | Yes — abort if missing |
| 2 | `load_str_monthly_sqlite.py` | Yes — abort if missing |
| 3 | `load_datafy_reports.py` | No — log warning, continue |
| 4 | `compute_kpis.py` | Yes — abort if fails |
| 5 | `compute_insights.py` | Yes — runs every pipeline push |
| 16 | `fetch_eia_gas.py` | No — skip-safe; seeds demo data if no EIA_API_KEY |
| 17 | `fetch_tsa_data.py` | No — skip-safe; seeds benchmark data if live fetch fails |

Each step: logged with timestamp + OK/SKIP/WARN/FAIL to `logs/pipeline.log`.
`compute_insights.py` always runs last — it reads all tables and generates today's forward-looking insights.

---

## Insights Engine (scripts/compute_insights.py)

Generates `insights_daily` rows for 4 audiences on every pipeline run:

| Audience | Categories |
|---|---|
| `dmo` | demand_trend, tbid_projection, feeder_market, compression_outlook, event_roi |
| `city` | tot_revenue, infrastructure, visitor_profile, economic_impact |
| `visitor` | best_value, rate_outlook, upcoming_events, booking_timing |
| `resident` | peak_alert, economic_benefit, quiet_windows, annual_impact |
| `cross` | feeder_value_gap, daytrip_conversion, weekday_los_gap, campaign_seasonality, oos_adr_premium, compression_daytrip |

**Cross-Dataset Insights** require BOTH STR and Datafy data to compute — they are invisible in either dataset alone:
- `feeder_value_gap` — STR ADR × Datafy DMA spend efficiency → LA over-indexed on volume, fly markets (SLC, Dallas, NYC) generate 1.3–1.4× more revenue per trip
- `daytrip_conversion` — STR room revenue × Datafy day_trip_pct → 1.44M day trips; 3% conversion = ~$15M incremental room revenue
- `weekday_los_gap` — STR weekday/weekend occ gap × Datafy avg_LOS → 2.0-day stays concentrate revenue on Fri-Sat; LOS extension worth ~$1M/yr
- `campaign_seasonality` — STR compression by quarter × Datafy attribution channels → campaigns may be amplifying peak (Q3=36 days) vs. building shoulder (Q1=4 days)
- `oos_adr_premium` — STR ADR YOY × Datafy out-of-state spend share → OOS visitors nearly 1:1 spend-to-visit but ADR only +6.7% YOY; rate capture gap exists
- `compression_daytrip` — STR compression days × Datafy day_trip_pct → on 80%+ occ days, day trippers add 0.7× more visitors invisible to hotel data

All insights are forward-looking (horizon_days configurable per insight).
One row per audience/category per day (UPSERT on `as_of_date + audience + category`).

---

## Standard Process — Adding New Data (ALWAYS FOLLOW)

Every time new data, a new source, or new logic is added:

```
1. Raw files  →  data/<source_name>/        (CSV, Excel, PDF — committed to git)
2. Loader     →  scripts/load_<source>.py   (parse → analytics.sqlite table)
3. Relations  →  scripts/build_table_relationships.py   (add entries for new tables)
4. Pipeline   →  scripts/run_pipeline.py    (add new step to STEPS list)
5. Run        →  python scripts/run_pipeline.py
   Step 20 (build_relationships) ALWAYS runs last — auto-refreshes all relationships
6. Dashboard  →  dashboard/app.py           (add loader + visualization for new table)
7. Commit     →  git add data/analytics.sqlite data/<source>/ scripts/ dashboard/
               →  git commit -m "Add <source>: N rows → N tables + N relationships"
               →  git push origin main
```

Raw data directories (canonical locations):
```
data/str/             ← STR Excel exports (str_daily.xlsx, str_monthly.xlsx)
data/datafy/          ← Datafy CSV exports (4 subdirs)
data/costar/          ← CoStar PDFs + CSVs
data/Zartico/         ← Zartico PDF reports
data/Visit_California/← Visit California Excel files
data/later/           ← Later.com CSV exports (IG/, FB/, TikTok/)
downloads/            ← Staging area only — move files to data/<source>/ before running pipeline
```

## Commands

```bash
# Local development
source venv/bin/activate
streamlit run dashboard/app.py

# Full refresh (all tables → KPIs → insights → relationships)
python scripts/run_pipeline.py

# Full refresh + latest code from GitHub
git pull origin main && python scripts/run_pipeline.py

# Rebuild ONLY table relationships (after schema change, no new data)
python scripts/build_table_relationships.py

# Deploy — ALWAYS commit directly to main, never create feature branches
git add <specific files> && git commit -m "description" && git push origin main
# Railway auto-redeploys from main branch (builds via Dockerfile) — this is the
# real production target. vdppulse.gloconsolutions.com's DNS CNAME points to
# Railway (*.up.railway.app), NOT Streamlit Community Cloud. Streamlit Cloud
# builds via packages.txt/requirements.txt; Railway builds via the Dockerfile
# and ignores packages.txt entirely — any new system library dependency must
# be added to the Dockerfile's apt-get install line, not just packages.txt.
```

---

## Code Style

- Python 3.11+, type hints where practical
- Use `pandas` for data shaping, `sqlite3` for DB access
- Logging via `print()` with timestamps for scripts; `st.spinner()` / `st.success()` for dashboard
- Treat `-` in Excel as NULL (use `pd.to_numeric(..., errors='coerce')`)
- No writes from dashboard — all writes via ETL scripts only
- Use `pd.notna()` for null checks before float conversion
- AP style for all user-facing text

---

## Important Rules

- NEVER commit `.env`, `venv/`, or API keys to git
- `data/analytics.sqlite` IS committed intentionally — it contains STR market data (no PII). Commit after every pipeline run that inserts new rows.
- NEVER override Layer 1 data with Layer 2/3 sources
- ALWAYS run `python scripts/run_pipeline.py` after schema changes — step 20 auto-rebuilds all table relationships
- ALWAYS reference this CLAUDE.md before making changes
- ALWAYS add new table relationships to `build_table_relationships.py` when adding new data sources
- Raw data MUST live in `data/<source_name>/` — never parse from `downloads/` permanently
- Dashboard is customer-facing — no API key fields, no debug output
- Admin-only features (API key field, Pipeline Controls) are gated by `st.query_params.get("admin","").lower() == "true"` — append `?admin=true` to URL to access. Never expose to customers.
- The Anthropic API key is set server-side via `ANTHROPIC_API_KEY` env var only
- After every code change, verify the app still runs: `streamlit run dashboard/app.py`
- `compute_insights.py` must run on every pipeline execution — it is the brain's daily self-update

---

## John Picou Writing Style

When drafting any communication (emails, summaries, reports) on behalf of John Picou:

- **Never use em dashes** ("—"). Use a comma, period, or restructure the sentence instead.
- Tone: warm, direct, and professional. Not stiff or corporate.
- Sign-offs: "Sincerely, John Picou / GloCon Solutions LLC"
- AP style for all user-facing text (no Oxford comma, numerals for 10+, etc.)

---

## Self-Improvement Protocol

After every session or error correction:
1. Reflect on what went wrong and why.
2. Abstract and generalize the learning.
3. Append the lesson to the `## Lessons Learned` section below.
4. Keep each lesson to 1–2 lines.

## Lessons Learned

- STR monthly exports use `-` for missing values; always coerce with `pd.to_numeric(..., errors='coerce')` before insertion.
- Shell prompts (zsh) will error if you paste Python code directly — always edit inside files with nano or Claude Code.
- `float(row.metricvalue)` fails on NaN; use `float(row.metricvalue) if pd.notna(row.metricvalue) else None`.
- Streamlit Cloud requires `requirements.txt` at repo root and `Main file path` must match GitHub breadcrumb exactly.
- GitHub auth from Mac: use Personal Access Token (classic) with `repo` scope, or SSH key.
- `insights_daily` uses UPSERT (ON CONFLICT) keyed on `(as_of_date, audience, category)` — safe to run multiple times per day.
- `table_relationships` documents every cross-table join/derivation — update it whenever a new table is added to the schema.
- The AI system prompt must include full DB schema for all tables so Claude can correctly answer cross-table queries.
- Cross-dataset (`cross` audience) insights require BOTH STR and Datafy to be loaded — they silently return empty if either is missing.
- Always prefix cross insights with `HIDDEN SIGNAL/OPPORTUNITY/RISK/GAP` to flag them as non-obvious findings.
- Zartico is historical reference only (Jun 2025 snapshot). NEVER present Zartico as current data. Datafy/CoStar/STR are current sources. Zartico tells the growth story.
- The VDP events calendar is JavaScript-rendered — live scraping requires Playwright. `fetch_vdp_events.py` seeds 10 known major Dana Point events as fallback data.
- All new Zartico tables (`zartico_*`) use `UNIQUE(month_str)` or `UNIQUE(report_date)` for safe UPSERT re-runs.
- `vdp_events` table uses `UNIQUE(event_name, event_date)` — safe to re-run seeding.
- `beautifulsoup4` is required in `requirements.txt` for the events scraper.
- Platform is branded **PULSE** (Performance, Understanding, Leadership, Spending, Economy). Page title, sidebar, and AI system prompt all use "Dana Point PULSE" — this is live, not just a suggestion.
- `visit_ca_airport_traffic` and `visit_ca_intl_arrivals` use column `month` (not `month_num`) — wrong name causes silent exception → empty DataFrame → ⚫ sidebar indicator.
- Data loaders use `try/except: return pd.DataFrame()` — a ⚫ indicator means the loader threw silently. Diagnose by running SQL directly: `python3 -c "import sqlite3,pandas as pd; print(pd.read_sql_query('SELECT * FROM <table> LIMIT 1', sqlite3.connect('data/analytics.sqlite')))"`.
- ALWAYS commit directly to `main` — never create feature branches. User explicitly requires this.
- Raw data MUST go in `data/<source>/` (not `downloads/`). Loaders read from `data/str/`, `data/datafy/`, etc. — downloads/ is a staging area only.
- `build_table_relationships.py` is the LAST step (step 20) in `run_pipeline.py` — it auto-rebuilds ALL 120+ relationships from the RELATIONSHIPS registry. Always add new entries there when adding tables.
- `table_relationships.created_at` is the correct column name (not `updated_at`) — check schema with `PRAGMA table_info(table_relationships)` before writing UPSERT SQL.
- Multi-model AI: `stream_ai_response(prompt, model_key, _ai_keys)` routes to Anthropic/OpenAI/Google/Perplexity. `_ai_keys` is computed in the sidebar; `selected_model` is stored in session_state. Both have module-level defaults before sidebar renders to prevent NameError.
- NEVER use the Write tool on `.env` — it overwrites the file and destroys live API keys. Always Read first; if the file exists, use Edit to add/change only specific lines.
- STR loaders use bulk `executemany()` + a single upfront `SELECT` of all existing keys — never row-by-row `SELECT COUNT` + `INSERT` (2N round-trips). Keep this pattern for all new loaders.
- Negative metric values in STR data are stored as `NULL`, not floored to `0.0`. Flooring silently masks data quality issues and corrupts downstream TBID/TOT projections.
- Dashboard `_logger = logging.getLogger("vdp_dashboard")` is the standard logger. Use `_logger.debug()` in except blocks so silent failures are diagnosable without crashing the UI.
- Cache TTLs are tiered: real-time KPIs = 300s, social/campaign = 1800s, historical (Zartico/VCA) = 3600s. Don't use 300s for everything.
- `OCC_HIGH_THRESHOLD`, `OCC_MED_THRESHOLD`, `OCC_SHOULDER_TARGET` are named constants at the top of app.py — use these instead of hardcoded 0.90/0.80/0.65 magic numbers.
- `requirements.txt` uses upper-bound pins (e.g., `pandas>=2.0.0,<3.0.0`) to prevent breaking changes on fresh installs. Update upper bounds only with deliberate testing.
- `data/str/*.xlsx` is gitignored — raw STR Excel exports are NOT committed; only `data/analytics.sqlite` is the committed truth.
- **Executive summary design rule:** Main tabs should be 2-minute scans, not data dumps. Move detailed analytics to sub-tabs. Headline insight + 4 hero metrics + call-to-action exploration cards (not 12 metric boxes + 8 button banks).
- **KPI consolidation:** Create utility functions for repeated formatting patterns (e.g., `format_hero_kpi_card()`, `format_exec_kpi_banner()`). Reduces duplicate code, makes style updates centralized, eases maintenance.
- **SQL query batching:** Replace N sequential single-row queries with 1 batched query. Example: 3 separate `SELECT ... FROM later_*_profile_growth` queries → `combine_social_followers()` function. Reduces round-trips and improves performance.
- **Sub-tab naming:** Use consistent, self-explanatory names (Scorecard, Board Report, Goals, AI Assistant) not generic ones (Performance, Stories, Analysis). Users shouldn't need to click to understand content.
- **Exploration cards:** Use small cards with icons, titles, and brief descriptions to guide users to related tabs. Improves navigation without cluttering the main tab.
- **App size management:** Watch line counts. If a single file approaches 18K+ lines, begin extracting components. The Overview tab refactor reduced it from 2,180 → 428 lines—same functionality, clearer intent.
- **Top-spacing nuclear fix:** Streamlit's React sets `paddingTop` inline on `.block-container` AFTER stylesheets load, overriding `!important` CSS rules. Defeat it with: (a) `<style>` injected into `<head>` at runtime (post-emotion, wins cascade), (b) `requestAnimationFrame` loop for 5 s forcing inline `setProperty('padding-top','0px','important')`, (c) `MutationObserver` on `document.documentElement` as persistent watchdog. CSS alone is not sufficient.
- **Dark cards on white page:** `_kfm_card` and similar dark-gradient metric cards look jarring on the light theme. Always check card bg color matches the page theme — use `#FFFFFF` + `border-top` accent for light pages, dark gradient only inside dark-bg sections.
- **Overview headline:** Never default to "On Track" — always pull the highest-priority `cross` or `dmo` insight. Leadership opens the Overview tab first; the headline is the first impression.
- **All-audience summaries:** Every summary section (Overview, Board Report, Forward Outlook tabs) must surface all 5 audiences (dmo, city, visitor, resident, cross). Showing only `dmo` is incomplete — city officials, visitors, and residents are separate stakeholders with different needs.
- **Insight body truncation:** 160 chars is too short for actionable context. Use first 2 sentences (up to 280 chars for overview, 320 for audience tabs). Always find the first `. ` after 60 chars to split at a natural sentence boundary.
- **Audience-labeled insight cards:** When showing group/category insights from multiple audiences, always label each card with the audience name and provide audience-specific action guidance (not a generic "review" CTA).
- **Splash "stuck on loading" bug:** The build-time splash (`patch_streamlit_splash.py`) self-removes when the app mounts. Its `ready()` check must NOT measure `#root` height — the app shell (`.stApp`) is `position:absolute; height:100vh`, so it's pulled out of normal flow and `#root` collapses to height 0 forever. Detect content height inside `[data-testid="stMainBlockContainer"]/.block-container` instead. A broken detector makes the splash hang on its 12 s failsafe = "stuck on loading screen." Verified fix clears the splash in ~0.9 s.
- **STR comp set market is Dana Point, NOT Anaheim.** The 6 comp markets in `fact_str_group_metrics` are: Dana Point, Newport Beach, La Jolla, Santa Barbara, Monterey-Carmel, Huntington Beach. Never default to "Anaheim Area" — that is wrong.
- **`DB_PATH` is a `Path`, not a str.** `sqlite3.connect(DB_PATH + "?mode=ro", ...)` raises `TypeError` (Path + str), which the loader's `except` swallows → silent empty DataFrame → ⚫ indicator. Always read through the cached `get_connection()`; never build a `?mode=ro` URI by concatenating to `DB_PATH`. This bug silently broke load_stvr_summary / load_bts_routes / load_socal_gas / load_weather_forecast / comp-set radar / content funnel.
- **One cached connection, tuned once.** All dashboard reads go through `get_connection()` (`@st.cache_resource`). It applies read PRAGMAs (WAL, synchronous=NORMAL, temp_store=MEMORY, 16MB cache, 128MB mmap, busy_timeout) via `_apply_read_pragmas()`. Never open ad-hoc `sqlite3.connect()` in a loader and never `.close()` the shared connection.
- **Hot-path indexes are embedded in two places** so they exist "all the time": `_init_db()` (CREATE INDEX IF NOT EXISTS — covers fresh Streamlit Cloud DBs) and `scripts/optimize_db.py` (pipeline step: ANALYZE + WAL checkpoint after every refresh). Key index `idx_str_src_grain_date` on `fact_str_metrics(source, grain, as_of_date)` backs the most frequent filter AND satisfies the ORDER BY.
- **Pipeline parallelizes independent fetches.** `run_pipeline.py` runs contiguous runs of `PARALLEL_SAFE` network-bound steps in a thread pool (`PIPELINE_MAX_WORKERS`, default 5). Core STR/KPI/insight steps and anything reading other freshly-written tables stay strictly sequential. `log()` is lock-guarded for thread safety.
- **NEVER `conn.close()` a `get_connection()` handle — it took production down.** `get_connection()` returns a shared `@st.cache_resource` connection reused across every Streamlit rerun. `load_str_compset` / `load_str_holiday_calendar` / `load_str_property_roster` each called `conn.close()`, so the next rerun's `load_str_daily()` crashed with `sqlite3.ProgrammingError: Cannot operate on a closed database` (whole app stuck on loading). Fix: removed the closes; `get_connection()` is now self-healing — `_open_connection()` is the cached builder, `get_connection()` runs `SELECT 1` and rebuilds via `_open_connection.clear()` if the handle was closed. A `with get_connection() as conn:` is fine (sqlite `__exit__` commits, does not close), but a bare `.close()` is poison.
- **`.streamlit/config.toml` is part of the theme, not just server config.** It declared `base="dark"` + `textColor="#F4FAFF"` long after the app moved to a white page. Custom CSS forced the surfaces light but never overrode Streamlit's generated text color, so anything the CSS didn't explicitly recolor rendered near-white on white: inactive tab labels measured **1.05:1** contrast (invisible). If the page is light, the config must say `base="light"`.
- **Measure contrast, don't eyeball it.** Drive the running app with Playwright and compute ratios from `getComputedStyle` against the composited background. That is how the 1.05:1 tab bug was found, and it is how you confirm a fix (now 10.35:1).
- **Careful: `backgroundColor` is empty for gradient backgrounds.** A naive "walk up to the first non-transparent ancestor bg" contrast audit reports the dark app shell behind a light gradient card and floods you with false positives. Verify suspicious hits against a screenshot before "fixing" them.
- **Brand teal `#0891B2` is only 3.68:1 on white** — fine for a chart mark (3:1 floor), too low for text. Use `#0E7490` (5.36:1) for teal text like active tab labels.
- **One chart theme: `dashboard/chart_theme.py`.** `app.py`, `components_group.py`, and `components_coastal.py` each used to carry their own. Import `style_fig` from there; never add a fourth. `style_fig(fig)` with no height now PRESERVES a height the figure already set, so applying it at render time will not resize gauges or tall heatmaps.
- **A categorical palette is validated, not chosen.** Check lightness band, chroma floor, adjacent-pair CVD separation, normal-vision floor, and 3:1 contrast. Order is fixed because checks run on ADJACENT pairs: reordering can silently break CVD. Red is reserved for status and is not a series color. The old Group palette had 6 of 8 colors under 3:1 on white (gold 1.72:1, seafoam 1.48:1) — that was the "washed out" look.
- **Charts can bypass the theme silently.** Grep for `st.plotly_chart(` call sites where the figure never passed through `style_fig` — 42 were found this way. A chart that renders is not a chart that is themed.
- **Freshness checks must anchor to the data, not the calendar.** `audit_app.py` averaged KPIs over the last 90 *calendar* days; whenever STR was behind, the window was empty, `AVG()` returned NULL, and it reported 3 false "out of range" errors. Anchor to `MAX(as_of_date)` and report "no rows" as its own condition.
- **A loader with no chart is invisible work.** `load_weather_forecast()` existed for weeks with zero call sites. After adding a table, check that something actually renders it.
- **The design system was never the problem; check what's actually rendered before rewriting CSS.** A 2026-07-30 audit for "make it look more executive" found `chart_theme.py` and the main CSS token block (`app.py` ~line 731) already form a validated, consistent light theme. The real damage was two things that ARE rendered on every page load: `inject_shader_wallpaper()` (a mouse-reactive animated canvas behind the whole app) and a `.kpi-card` entrance/hover-pop animation applied to every KPI everywhere. Both read as consumer-demo flourishes, not board-level software, and both are now removed/disabled. Always grep for `class="X"` usage in the HTML before touching a CSS rule for `.X` — `app.py` has accumulated dead, never-applied classes (`.glass-card`, `.noise-overlay`, `.reveal-up/-fade/-scale`, `.reveal-card/-clip/-left`, `.signal-card`, `.grad-border-card`, `.skeleton`, `.counter-num`, blob loaders, mono-cards) from prior sessions that designed effects and never wired them in. They cost nothing visually but they cost real time to audit; a dead-CSS pass is worth scheduling.
- **17 separate inline `<style>` blocks live inside `app.py`** (plus two more, fully unused, orphaned copies at `dashboard/assets/styles.css` / `styles-light.css` that nothing imports). The one at line 731 is the real, load-bearing design-tokens block. Before editing "the CSS," grep `st.markdown("""` + `<style>` to find every block touching the same selector, edit the one that's actually live, and consider deleting the two orphaned asset files in a future pass so nobody edits them by mistake.
- **Two navigation systems exist in this repo simultaneously.** `app.py`'s `st.tabs()`-based "Classic View" and `dashboard/pages.py`'s session-state-driven 12-page sidebar nav (`from pages import render_page` at app.py's routing block, `use_classic_view` checkbox defaulting to `False`). A whole redesign pass can land entirely on the wrong one if you don't check which view is actually default-active. `pages.py` is now the primary app; `render_top_nav()`/`render_sub_nav()` render the persistent Overview/Hotel Operations/Visitor Economy/Strategic Planning bar via `?page=` query-param links (not `st.button`, so they can carry inline SVG icons outside a form).
- **Never interpolate data-driven text (headline, body, label, value, as_of) into an `unsafe_allow_html=True` card without escaping it.** `compute_insights.py` had been writing literal comparison symbols straight into insight text — `"days >80% occ"`, `"had <65% hotel occupancy"` — which a browser's HTML parser can choke on inside a raw-HTML markdown block, corrupting the rest of that card's markup into visible text on the page. Fixed at the source (words instead of symbols: "above 80%", "under 100") and defensively in `utils.py`'s `format_metric_card`/`format_insight_card` via a shared `_esc()` (`html.escape`) helper applied to every data field — never to the hand-authored icon SVG strings, which must still render as markup.
- **`compute_insights.py` had its own copy of the calendar-vs-data freshness bug**, in `load_kpi_recent()` and `load_str_revenue()` — both built their trailing-window cutoff from `date.today()` instead of `MAX(as_of_date)` in the table. Since STR data lags the calendar (ends 2026-03-28 while "today" moves forward), the query silently returned zero rows, which downstream insight generators read as RevPAR/ADR `== 0` and rendered as "$0.00" or "N/A" — and silently skipped 4 whole insight categories (`rate_capture_efficiency`, `tbid_tot_forecast`, `daytrip_conversion_scenario`, `booking_window_alert`) for lack of data. Same root cause `audit_app.py`'s fix already documents; the fix pattern (anchor to `SELECT MAX(as_of_date)`, not the wall clock) needs to be checked in every new data loader, not just the one place it was first found.
- **Railway (via `Dockerfile` + `railway.toml`), not Streamlit Community Cloud, is the real production target.** `vdppulse.gloconsolutions.com`'s DNS CNAME points at `*.up.railway.app` — confirmed via `dns.google/resolve`, since Vercel's own DNS panel for the domain showed nothing for that subdomain and raw `dig` isn't available in a sandboxed shell. A parallel, apparently-unused Streamlit Cloud deployment also existed at a `*.streamlit.app` URL, deploying from the same repo, and cost real debugging time: three rounds of `packages.txt` fixes (a Streamlit-Cloud-only convention) had zero effect on the live site because Railway builds the `Dockerfile` and ignores `packages.txt` completely. `packages.txt` was removed from the repo since it no longer serves any deployment. When a runtime dependency error shows up on the live site, confirm which platform is actually serving traffic (`dns.google/resolve?name=<domain>&type=CNAME`) before editing build config, don't assume it's whichever platform you're most familiar with.
- **Don't trust cached knowledge of a base image's Debian codename — read the actual build log.** Assumed `python:3.11-slim`'s untagged floating tag was still Debian bookworm based on web search results; Railway's own build log showed `deb.debian.org/debian trixie InRelease`, proving the untagged tag now floats to trixie same as Streamlit Cloud's container. Both platforms needed the identical trixie package renames (`libgdk-pixbuf-2.0-0`, `libglib2.0-0t64`), so the Dockerfile and any future `packages.txt`-equivalent should use the same names. The Railway MCP's `get-logs` tool (types: `["build"]`, keyed off the deployment ID visible in the failure page's URL) surfaces this directly, check it before guessing at a second round of package names.
- **When a brand palette cannot be pulled from the live site's CSS, sample it from approved real photography instead of guessing hex values.** The sandbox cannot reach `visitdanapoint.com` or `assets.simpleviewinc.com` by direct HTTP (only the dedicated web-fetch tool works, and it strips styling), so exact brand hex values were not confirmed. Pillow's median-cut quantization on the user's own uploaded Dana Point photos produced a defensible, on-brand palette (deep ocean navy-teal plus golden-hour terracotta) without fabricating unverified hex codes. Constants live in `generate_weekly_report.py` (`TEAL`, `TEAL_DK`, `TEAL_LT`, `MAROON`, `MAROON_LT`, `SLATE`) and mirror in `report_template.html`'s `:root` block and `dashboard/app.py`'s inline CSS. All three must be updated together.
- **Pillow (12.x in this environment) reads AVIF natively, no plugin needed.** User-uploaded `.avif` photography converts cleanly with `Image.open(...).convert("RGB")` and re-saves as JPEG for embedding as base64 data URIs. Real photos live in `dashboard/assets/photos/`; `vdp_logo_nav.svg` and `vdp_logo_footer.svg` were confirmed byte-identical in structure to the live assets at `visitdanapoint.com/includes/public/assets/shared/logos/`, verified by fetching the homepage and comparing the referenced asset paths directly.
- **Matplotlib bar charts scaled with `object-fit:contain` need a figsize aspect ratio close to the CSS box's aspect ratio, not just a bigger figsize.** A near-square `figsize=(4.8, 4.6)` inside a tall, narrow HTML box still left a large gap underneath because `contain` scales to the limiting dimension (width) first. Fixed by making the figures taller than wide (`figsize=(4.4, 6.8)`) to match the box's actual aspect ratio. When a chart looks small inside its container despite a large CSS height, check the figure's aspect ratio against the container's aspect ratio, not just its absolute size.
- **A skip-safe step can still fail 100% of the time silently for weeks.** `fetch_str_dropbox.py` (pipeline step 0, `fail_fast=False`) had been exiting immediately at `get_token()` since at least 2026-08-10 because `DROPBOX_API_TOKEN` was never set, before ever attempting the public no-token path its own docstring promised ("no token needed"). `run_pipeline.py` correctly logged this as `WARN` and moved on each monthly run, which made the pipeline LOOK healthy in the log while this one source silently never ran. Fixed by making `get_token()` return an empty string instead of `sys.exit(1)`, so it actually reaches the HTML-scrape fallback. Lesson: a "skip-safe, non-fatal" design still needs its actual success/failure checked periodically (`grep <step_name> logs/pipeline.log`), not just trusted because the pipeline as a whole exits 0.
- **A "stale report" complaint means check the raw files before touching any code.** The report showed "Week of March 22 to 28, 2026" while `data/str/str_daily.xlsx` (the canonical file `load_str_daily_sqlite.py` always reads) already contained rows through 2026-08-01, a four-month gap that had simply never been loaded. `load_log` showed STR loads running as recently as 2026-08-01, which made it look like the pipeline was current; the STR daily/monthly loaders and `compute_kpis.py`/`compute_insights.py` simply had not been re-run since the newer file landed in the repo. Fix was three commands (`load_str_daily_sqlite.py`, `load_str_monthly_sqlite.py`, `compute_kpis.py`, `compute_insights.py`), no code changes. Before debugging "why is the data old," diff the raw source files' actual date ranges (`pd.read_excel(...).tail()`) against `SELECT MAX(as_of_date)` in the table; a stale DB with fresh source files on disk means "re-run the loader," not "fix a bug."
- **The Streamlit Cloud/Railway "local vs. cloud" confusion re-surfaces easily.** The live report has been on Railway at `vdppulse.gloconsolutions.com` since the trixie Dockerfile fix; a `streamlit run dashboard/app.py` on the owner's own Mac will print a `localhost` URL that only he can open, which reads exactly like "this only works locally" even though the real production URL is already public and cloud-hosted. When this comes up, point back to the live Railway URL rather than assuming new hosting infrastructure is needed.
- **"I already added the new data" can mean it landed in chat uploads, not the mounted repo folder.** A user insisting fresh Datafy files exist, while `find data/datafy -newer <marker>` shows nothing, is not necessarily wrong: check the session's uploads mount too. This turned out to be exactly what happened with an Aug 2026 "DynamicHomePage" export batch (13 CSVs) the owner had sent as chat attachments rather than saving into `data/datafy/`.
- **Datafy's export naming convention changes over time** (legacy `kpis_2025-annual.csv` → April 2026 `OverallNumbers_Export*.csv` → August 2026 `DynamicHomePage-*_Export.csv`), and each generation needs its own entries in `load_datafy_reports.py`'s `NEW_FILE_HANDLERS` list. When two filename patterns are substrings of each other (e.g. `dynamichomepage-top markets_export` vs. `...export (1)`), list the more specific one first since `find_new_handler` returns on first match.
- **Files without a `DD-MM-YYYY_to_DD-MM-YYYY` period in the filename fall back to `FOLDER_DEFAULT_PERIODS`, a shared constant.** Never bump that constant to tag one new batch, since it silently relabels every other undated file in the same subfolder on the next load. Instead, rename the new batch's files to embed the correct period in the filename; `_extract_period()` will pick it up per-file without touching the shared default.
- **A Datafy field named `*_pct` is not reliably a percent number.** Some exports store it as a 0-100 percent (`day_trips_pct: 40.57`), others as a 0-1 fraction (`in_state_pct: 0.338`), and it can vary by export generation for the *same logical value* (the newer "Local Vs Visitor" export ships `"69.01%"` as a string where the legacy version shipped a bare `0.69`). Check the actual on-disk convention for the specific column and export before wiring a new source into an existing chart; converting silently in the wrong direction (or not at all) produces a chart that is off by 100x without erroring.
- **Never assume a Datafy snapshot is a full calendar year just because a legacy sibling was.** `datafy_overview_kpis` used a real Jan 1 to Dec 31 annual pull, but the freshest `datafy_overview_total_kpis` row can be a partial-year snapshot (2026-01-01 to 2026-07-31). A shared `_format_datafy_period()` that checks the actual start/end dates and only prints "Annual" for a true Jan 1 to Dec 31 span prevents relabeling seven months of data as a full year.
- **A whitespace fix and a "looks unnatural" complaint pull in opposite directions on chart `figsize`.** Stretching a bar chart's `figsize` to match a tall, narrow CSS container (to kill leftover whitespace) makes the bars themselves look distorted once the aspect ratio gets extreme. The fix is to move the container's own aspect ratio toward the chart's natural shape (shorter, wider) rather than distorting the chart to fit an unnaturally tall box.
- **A flex child holding an `<img>` needs `min-width:0` (or must not be `display:flex` itself) or it silently overflows its column.** Adding `display:flex; flex-direction:column` directly to a `.market-col` so an image could be vertically centered via `margin:auto` caused the two side-by-side chart panels to overlap, because a flex item's default `min-width:auto` lets its intrinsic content (the image) push past its allotted `flex:1` share. Vertically center by putting `align-items:center` on the *row* instead, and leave the column a plain block.
- **A file existing on disk with a plausible name is not the same as that file being real, sourced data.** `costar_chain_scale_breakdown.csv` and `costar_competitive_set.csv` (a 6-tier "South Orange County CA" chain-scale breakdown and 8 named individual hotels with MPI/ARI/RGI) were cited in the weekly report as "SOURCE: COSTAR" but traced to no real CoStar PDF in `data/costar/`; `load_costar_reports.py`'s own docstring admitted they're "auto-created on first run from hardcoded baseline data." They stayed frozen at FY2024 because nothing ever re-derived them from a real export. Before extending any "stale data" complaint with fresher numbers, confirm the file feeding it is actually parsed from a live source, not hand-typed baseline/seed data from an earlier session, especially for anything labeled as a real Layer-1 source in front of the client.
- **CoStar's own submarket PDFs already had the real fix.** `Newport Beach-Dana Point-Hospitality-Submarket-2026-08-11.pdf` (already sitting in `data/costar/`, nobody had parsed its segment tables) contains real FY2025 (complete) and YTD 2026 occupancy/ADR/RevPAR for three chain-scale tiers (Luxury & Upper Upscale, Upscale & Upper Midscale, Midscale & Economy) for the real "Newport Beach/Dana Point" submarket, plus a narrative room-count split by tier (6,000 / 4,300 / 1,300 rooms). `load_costar_reports.py`'s existing `_extract_annual_performance` already parsed the OVERALL PERFORMANCE table this way; it just needed extending to the three segment PERFORMANCE tables and a new room-split extractor, feeding `costar_annual_performance` (`report_scope` column, already had a `UNIQUE(year_label, market, report_scope)` constraint built for exactly this) and a new `costar_segment_room_split` table. `generate_weekly_report.py`'s Market Segments / Chain-Scale Segment Detail pages now read from these instead of the fabricated CSVs.
- **Customer-facing comparison materials must be verified against the actual deployed code, not against a parallel scratch repo.** Heather-facing PDFs/emails described the live `vdppulse.gloconsolutions.com` app as having an AI assistant, notes, and a report archive; those features existed only in `VDP_udpates` (a planning/staging copy), not in `VDP_Dashboard`'s real production `app.py`. Always grep the actual deployed repo (confirm via the hosting platform's domain-to-service mapping first, e.g. Railway's `list-domains`/`get-service-config`, not by assumption) for the specific claim ("AI assistant", "notes", "archive", page count) before it goes out to a client.
- **Ported the Intelligence Brief AI assistant, editable notes layer, and report archive from `VDP_udpates`'s scratch `app.py` into this repo's real `app.py`** (2026-08-17), closing the gap above. All three are additive (492 lines, zero deletions) and reuse the existing Summarize/Regenerate/admin-digest code untouched. `NOTES_DB_PATH` and `REPORT_ARCHIVE_DIR` default to local paths under `data/`, gitignored, and are NOT persistent across a Railway redeploy without a mounted Volume: set both env vars to a Volume-backed path before Heather starts relying on notes or the archive surviving a data push.
- **The "12-page interactive board" figure quoted to Heather does not match either app.** The PDF report itself has 6 pages in production (`report_template.html`) and 8 in the `VDP_udpates` scratch copy; neither is 12. Correct this figure in any future Heather-facing material rather than repeating it.
- **A "still missing key things" complaint after a port means the port was under-scoped, not that the push failed.** A first pass at closing the VDP_udpates-vs-production gap only ported three named features (AI assistant, notes, archive) and left production's own PDF-viewer skeleton untouched, reasoning its photography-based cover was already good. That missed the point: the live native Plotly visualizations (KPI trend, Datafy spend trend, a geographic feeder-market bubble map, category pie), the period selector driving them, and the page-by-page flipbook viewer were still absent, and the fix was a second, much larger port, not a caching or deployment issue. When told to match "the local" build, port everything demonstrated there, not just the pieces that seem most relevant.
- **`venv/` got committed onto a stray `main` branch in VDP_udpates (~29,600 files), which is why `git checkout master --force` kept timing out.** Git had to delete tens of thousands of files over a slow mounted filesystem before it could even start restoring `master`. When a checkout of a scratch/staging repo times out repeatedly, check `git ls-tree -r <branch> --name-only | wc -l` on both branches before retrying the same slow command again; a bloated wrong branch is a different problem than a slow disk. `git show <branch>:<path>` reads a file straight out of git's object database without touching the working tree at all, and is the fast way to recover a specific file's content when the full checkout is what's timing out.
- **Each `mcp__workspace__bash` call is its own fresh environment: a background process (`nohup ... &`) started in one call is gone by the next call.** Confirmed twice, once with a Streamlit test server, once with a `streamlit.testing.v1.AppTest` script. Anything that needs to run longer than one call's timeout has to complete inside that same call (raise `timeout_ms`, cap ~180s observed in practice regardless of a higher requested value) rather than being backgrounded and polled later.
- **`streamlit.testing.v1.AppTest` is the fast, no-browser way to catch real runtime exceptions in a Streamlit app**, since this sandbox cannot launch headless Chromium (missing `libXdamage.so.1`, no root to install it). `AppTest.from_file(path)` needs an absolute path resolved against the caller script's own location, not cwd. `.run(timeout=...)`, then `.click().run(...)` / `.set_value(...).run(...)` on `at.button` / `at.radio` / `at.text_area`, checking `len(at.exception)`, exercises real interaction paths (button clicks, note saves, period-selector changes) that a plain `curl` against the page never reaches, since Streamlit only executes the script inside an actual session.
- **CoStar's own multi-year forecast tables label the current, in-progress year as a numeric row too** (e.g. "2026" alongside "YTD" and "2025"), but that numeric current-year row is a pace/forecast figure, not a closed-book actual. Treating `year <= current_year` as "real" mislabeled an in-progress year as "Full Year 2026." The fix: only years strictly before the current year are real, complete "Full Year" data; the current year's real actual-to-date figure is the "YTD" row.
- **A two-column PDF layout breaks `pdfplumber`'s plain `extract_text()` for cross-column phrase matching**, even with `layout=True` — it reads left-to-right across the full page width and interleaves fragments from both columns onto the same line (e.g. "comprises 110 hotel **generates consistent business travel, while executive** properties, which contain..."). A regex anchored on a multi-word phrase spanning that boundary silently returns no match. Fix: crop the page into left/right halves (`page.crop((0,0,mid,height))` / `page.crop((mid,0,width,height))`) and extract each independently before regex-matching; full-width data tables on the same PDF are unaffected and don't need this treatment.
- **When a dependent script/dashboard surface reads a table you now know is fabricated, but fixing your immediate task doesn't require touching it, leave it running and document the risk loudly instead of a blast-radius refactor.** `dashboard/pages.py`, `dashboard/components_group.py`, `scripts/compute_insights.py`, and `scripts/build_table_relationships.py` all still read `costar_chain_scale_breakdown`/`costar_competitive_set`. Rather than delete those tables (breaking a currently-dormant second dashboard) to fix the weekly PDF report, the PDF report was migrated to the real tables and a prominent warning was added to `load_costar_reports.py`'s module docstring instead.

---

## New Tables (2026-03-17)

### Zartico Historical Reference Tables (8 tables)
| Table | Rows | Purpose |
|---|---|---|
| `zartico_kpis` | 4 | Visitor economy KPIs (devices %, spend %, demographics, accommodation %) |
| `zartico_markets` | 11 | Top visitor origin markets (rank, %, avg spend) |
| `zartico_spending_monthly` | 11 | Monthly avg visitor spend vs benchmark (Jul 2024–May 2025) |
| `zartico_lodging_kpis` | 1 | Hotel/STVR summary (YTD occ, ADR, LOS, ADR by day of week) |
| `zartico_overnight_trend` | 13 | Monthly overnight visitor % trend (May 2024–May 2025) |
| `zartico_event_impact` | 1 | Event period vs baseline spend changes |
| `zartico_movement_monthly` | 10 | Visitor-to-resident ratio by month |
| `zartico_future_events_summary` | 1 | YoY event + attendee growth |

### VDP Events Table
| Table | Rows | Purpose |
|---|---|---|
| `vdp_events` | 10 | Known major Dana Point events (scraped or seeded; `is_major` flag) |

---

## Upload Your Data — Pattern for Client Self-Serve Uploads

The dashboard's "Upload Your Data" panel (`app.py`) saves a client-uploaded STR/Datafy/CoStar/Later file into the correct canonical `data/<source>/` folder, then, only if `GITHUB_TOKEN` is set, commits it to `VDP_Dashboard` main via GitHub's Contents API and dispatches `str_weekly_sync.yml` via the Actions API. Without `GITHUB_TOKEN`, the file still saves for the running Railway instance but is lost on the next redeploy, since the container filesystem is ephemeral and canonical data only persists by being in the git repo. The UI is written to say this plainly rather than promise a sync that isn't configured. To make this fully live: add a fine-grained GitHub personal access token scoped to `gloconllc/VDP_Dashboard` with `contents:write` and `actions:write`, and set it as `GITHUB_TOKEN` (plus optionally `GITHUB_REPO`/`GITHUB_WORKFLOW_FILE` if either should differ from the defaults) on the Railway service's environment variables.

---

## Update Log

| Date | Change | Author |
|---|---|---|
| 2026-03-09 | Initial CLAUDE.md created | Claude + John Picou |
| 2026-03-09 | CLAUDE.md installed at project root; slash commands created; home button added to dashboard title | Claude + John Picou |
| 2026-03-16 | Full brain upgrade: insights_daily + table_relationships schema; compute_insights.py (4 audiences, 17 insight types); pipeline updated to run all 25+ tables; Forward Outlook tab added to dashboard; AI system prompt extended with full schema | Claude + John Picou |
| 2026-03-17 | Zartico integration (8 tables, historical reference); VDP Events table (10 seeded events); CoStar filter fix; Data & Downloads dynamic row counts; Zartico section in Visitor Economy tab; 6-point Board Report; pipeline steps 7+8 added | Claude + John Picou |
| 2026-03-17 | Rebrand to Dana Point PULSE; 9-tab layout (+ Feeder Markets, Event Impact, Supply & Pipeline); Visit California ⚫ bug fix; admin mode (?admin=true); PULSE Score widget; footer with GloCon branding + glossary; direct-to-main commit workflow | Claude + John Picou |
| 2026-03-25 | Later.com social media integration (IG/FB/TikTok → 12 tables); Pipeline step 10; Pipeline Status dot; Data & Downloads card; Datafy GA4 summary in Board Report; Performance Command Center card+chart pairs; PULSE Score whitespace fix + scale readability; STR chart animations; Key Forward Metrics date references | Claude + John Picou |
| 2026-04-22 | Full /enhance audit: 53 bare except blocks → logged; SQLite timeout=10; bulk executemany() in STR loaders (2N→1 round-trip); tiered cache TTLs (300/1800/3600s); negative value preservation; NaT date validation; retry logic in FRED/EIA fetch scripts; model selector shows strengths; empty-state card for insights; RevPAR axis label; requirements.txt version pinned; .gitignore xlsx; OCC threshold constants; 8 new lessons learned | Claude + John Picou |
| 2026-03-30 | EIA gas prices + TSA checkpoint data sources (pipeline steps 16+17); intel panels added to tab_sp and tab_dl; gas price correlation section in Market Intelligence; EIA/TSA source health cards in Data Vault; updated DB inventory; EIA/TSA sidebar status dots | Claude + John Picou |
| 2026-03-31 | Multi-model AI engine (Claude + GPT-4o + Gemini + Perplexity Sonar — 8 models); universal stream_ai_response() router; sidebar model selector; Live Market Intelligence panel; all charts downloadable (scale=3, 1600×800); 7 new CSV download buttons; style_fig v4 | Claude + John Picou |
| 2026-03-31 | Data organization standard: all raw data in data/<source>/ canonical dirs; STR files moved to data/str/; build_table_relationships.py (step 20, always-last); 120 relationships (from 37); FRED_API_KEY placeholder; Standard Process section in CLAUDE.md | Claude + John Picou |
| 2026-04-24 | Major Overview tab redesign: reduced from 2,180 to 428 lines; new exec summary format (headline insight + 4 hero metrics + 5 exploration cards); moved AI Analyst panel to dedicated 🤖 AI Assistant sub-tab; consolidated KPI formatting functions into utils.py module; renamed sub-tabs (Scorecard, Board Report, Goals, AI Assistant) for clarity; added error logging to all except blocks; optimized social followers query; created dashboard/assets/styles.css for future stylesheet separation | Claude |
| 2026-04-24 | Full UX/visual audit & enhancement: ticker readability improvement (65%→95% opacity, font 12px→16px, labels 8.5px→9.5px); new format_insight_card() utility for styled metric cards (replaces plain text paragraphs with visual hierarchy); light theme color consistency pass (updated CSS token usage); improved section spacing and visual separation between topic areas | Claude |
| 2026-05-26 | Thursday demo prep: pipeline refresh (28 steps, 29 fresh insights, 288 relationships); top white-space nuclear fix (RAF loop + head-injected style + MutationObserver); hero banner margin-top 0; splash text to pure white; tab bar to clean white-pill SaaS style; Overview exec brief upgraded (live KPI snapshot + status badge + top insight block); Forward Outlook _kfm_card to light theme; font antialiasing global; hero "Brain refreshed" badge | Claude |
| 2026-05-29 | Group & Travel Intelligence tab (PR #35): new 10th tab with 4 sub-tabs (Group Strategy, Traveler Types, National Context, AI Analyst); 5 new charts (TBID bar, occ heatmap, segment donut, revenue funnel, traveler type radar); board-ready executive brief; 2 new cross insights (group_event_synergy, traveler_mix_revenue_gap); 37 total insights; group KPIs in ticker; x-axis readability fix (12px, -30°); insight body text 9.5pt→11.5pt | Claude |
| 2026-05-29 | Summary & audience enhance: 5-audience Stakeholder Intelligence Brief on Overview hero (dmo/city/visitor/resident/cross); Board Report expanded to show all 5 audiences; Forward Outlook audience tabs each get executive summary card; Group Strategy insights labeled by audience with tailored action guidance; insight body expanded from 160→280 chars in overview | Claude |
| 2026-06-16 | Load + refresh speed pass: all dashboard reads routed through one cached, PRAGMA-tuned `get_connection()` (WAL, NORMAL sync, MEMORY temp, 16MB cache, 128MB mmap, busy_timeout); fixed 6 silently-broken loaders (`DB_PATH + "?mode=ro"` TypeError → empty data); hot-path indexes embedded in `_init_db()` + new always-on `scripts/optimize_db.py` pipeline step (indexes + ANALYZE + WAL checkpoint); `run_pipeline.py` now runs independent network fetches in parallel (thread pool, `PIPELINE_MAX_WORKERS` default 5) with lock-guarded logging | Claude |
| 2026-07-27 | Full refresh + readability pass: pipeline rerun (insights for all 5 audiences, relationships 329 → 344 incl. 15 new weather entries, zero orphan tables); **fixed white-on-white text app-wide** by correcting `.streamlit/config.toml` from dark to light theme (inactive tab labels 1.05:1 → 10.35:1); active tab teal → `#0E7490` for 5.36:1; new `dashboard/chart_theme.py` replaces 3 divergent themes and ships a validated palette (old Group palette had 6/8 colors under 3:1); 42 unthemed charts routed through `style_fig`; 2 new visualizations (Visitor Conditions Outlook, Brain Map); `audit_app.py` KPI window anchored to latest data | Claude |
| 2026-07-30 | Executive polish pass, phase 1: removed `inject_shader_wallpaper()` (mouse-reactive animated canvas behind the whole app) and the `.kpi-card` entrance/hover-pop animation, both of which were live on every page load and read as consumer-demo flourish rather than board-level software; identified but left in place a large body of dead, never-applied CSS (`.glass-card`, `.noise-overlay`, `.reveal-*`, `.signal-card`, `.grad-border-card`, `.skeleton`, blob loaders, mono-cards) and two fully orphaned stylesheets (`dashboard/assets/styles.css`, `styles-light.css`) for a future cleanup pass; confirmed `chart_theme.py` + the live design-tokens block are already sound, so no palette/typography rewrite was needed. Full tab-by-tab IA pass (reducing density across the other 9 tabs) is scoped as phase 2, pending direction on priority. | Claude |
| 2026-07-30 | Executive polish pass, phase 2 (real visual redesign, not just cleanup): found and read `dashboard/vdp-dashboard-v2.html`, a full reference mockup the owner had built in a past session and never wired into the live app — light SaaS aesthetic (tabular-nums KPIs, `sh-sm/md/lg` soft shadow scale, `r-sm/r/r-lg/r-xl` radius scale, quiet hover states) versus `data/design/godly_inspiration.json`'s flashier scraped patterns (glassmorphism, aurora mesh, animated gradient borders), which turned out to be largely what phase 1 had already stripped out. Rewrote the `:root` token block to the v2.html shadow/radius/timing scale (kept `--dp-teal:#0891B2` rather than v2's `#21808D` so KPI accents keep matching `chart_theme.py`'s contrast-validated chart palette instead of introducing a second, unaudited teal). Rebuilt `.kpi-card`/`.event-stat` to a quieter hover language (top accent line fades in, `translateY(-2px)`, tabular-nums, pill-style delta badges) instead of the always-on colored border + `translateY(-4px)` glow. **Rebuilt `.hero-banner` from a dark navy "aurora mesh" background with a permanently shimmering gradient title and a 5s-infinite glow-pulse orb into a light, static masthead** — this was the single most visible unrefreshed surface in the app (top of every tab) and is very likely what read as "still looks the same" after phase 1's under-the-hood cleanup. **Found and fixed a second, later `<style>` block that silently re-applied the old dark aurora background to `.hero-banner` with `!important`, overriding the rewrite above** — exactly the multi-block cascade trap this file's Lessons Learned already warns about; always grep every occurrence of a selector across the whole file, not just the first one. Converted the KPI ticker strip below the hero (`.pulse-ticker-*`) from a dark Bloomberg-style band to match the new light masthead — it was hardcoded to "seamlessly extend the hero banner, same dark bg" and became a jarring dark seam once the hero went light. **Found and fixed three separate real contrast bugs while doing this**, all dark-navy-card-with-dark-text (the `.tab-info-tooltip` popover, the Census demographic cards in Market Intelligence, and the `_SOURCES_HTML` source-attribution strip in Data & Downloads) plus two places where JS was force-setting the app shell background to dark navy (`#0B1E38`/`#0E1B2A`) as a leftover "nuclear top-spacing fix" from the dark-theme era, now `#FFFFFF`. Scope for this round was the masthead/ticker/KPI system only — the other 9 tabs' interiors (charts, tables, tab-specific cards) have not yet had this same v2.html-alignment pass. | Claude |
| 2026-08-11 | Ingested a fresh Aug 2026 Datafy "DynamicHomePage" export batch (13 CSVs: total KPIs, LOS distribution, state origin, top POIs, top markets, top-boxes demographics, repeat spenders, in/out-of-state split, local/visitor split, monthly category spend trend, top countries, top origin airports), period-tagged 2026-01-01 to 2026-07-31; extended `load_datafy_reports.py` with new schemas/parsers/handlers for the naming convention and fixed a pre-existing `%`-parsing bug in `parse_top_pois`; `generate_weekly_report.py` now sources Visitor Origins, category spend, and visitor-profile KPIs from whichever Datafy period is freshest per metric (falling back to the legacy annual tables), and labels the window "Annual" only when it truly spans a full calendar year, otherwise "Year-to-Date"; added real sourced stat badges to the "Why Visitors Choose Dana Point" photo cards; fixed a Hotel Performance chart aspect-ratio regression (bars looked stretched after an earlier whitespace fix); removed remaining em dashes from user-facing report/app copy; app hero band got a stronger scrim, drop-shadow on the logo, centered content, a colored Regenerate button, and a date-range control in the header (UI only, not yet wired to reflow the report). | Claude |
| 2026-08-14 | Live-tested SendGrid digest email end-to-end from the deployed Railway app: switched `send_weekly_digest.py`/`send_alerts.py` from SMTP (hung on Railway's filtered outbound ports) to SendGrid's HTTP API; completed SendGrid domain authentication for gloconsolutions.com to fix inbox-vs-junk placement; added an admin-gated "Send Digest Email Now" button (`?admin=true`) so a real send can be triggered from inside the deployed app itself. Expanded/centered/styled the main Download PDF button. Diagnosed two "new STR files" as 24-byte corrupted downloads sitting in `data/str/` (not `data/str/weekly/`) rather than loading bad data. Found and fixed a real bug in `fetch_str_dropbox.py`: `get_token()` was hard-exiting with no `DROPBOX_API_TOKEN` set, meaning the Dropbox sync step (pipeline step 0) had silently failed on every run since at least 2026-08-10 despite being "skip-safe" — it never reached its own documented no-token public-link fallback. Fixed to warn and continue instead of exiting. Added `.github/workflows/str_weekly_sync.yml`: a dedicated GitHub Actions job, scheduled Thursdays 8am Pacific (`0 15 * * 4`, UTC) plus manual `workflow_dispatch`, that syncs the Dropbox STR folder, reloads STR/KPIs/insights, and pushes straight to main (GitHub Actions' own token handles the push, no manual step needed once the workflow file itself is merged) so Railway's auto-redeploy picks up fresh numbers automatically every week. | Claude |
| 2026-08-17 | Ported three features into the live app.py from the VDP_udpates scratch build after discovering Heather-facing comparison materials described them as already live: an Intelligence Brief AI assistant answering questions against full STR/CoStar/Datafy history, an editable notes layer, and a browsable report archive. All additive, 492 lines, no deletions to the existing Summarize/Regenerate/admin-digest code. Verified with Streamlit's AppTest harness (default load, Generate Latest Intelligence click, note save, Regenerate/archive click) at zero exceptions. Added data/dashboard_notes.sqlite and data/report_archive/ to .gitignore. Flagged that NOTES_DB_PATH and REPORT_ARCHIVE_DIR need a Railway persistent Volume before either survives a redeploy, and that the "12-page" figure quoted to Heather does not match either app's actual PDF page count (6 in production, 8 in the scratch copy). Also fixed compute_insights.py's load_datafy_overview() to overlay the fresher datafy_overview_total_kpis total_trips figure (1,888,637) instead of the stale 2025-only datafy_overview_kpis figure (3,551,929). | Claude |
| 2026-08-17 | Second, larger pass at the same VDP_udpates-vs-production gap after John reported the live site still lacked visualizations, a proper one-page-per-section view, and mobile/desktop responsiveness compared to what he ran locally. Recovered the real VDP_udpates/dashboard/app.py content via `git show master:dashboard/app.py` after discovering `venv/` had been accidentally committed onto a stray `main` branch (~29,600 files), which is what made `git checkout master --force` keep timing out. Ported into production app.py: a period selector (This week through Last 24 months, plus custom range) driving every chart and the Intelligence Brief; native Plotly KPI occupancy/ADR trend chart; Datafy monthly spend trend chart; a geographic feeder-market bubble map (DMA_COORDS lookup) plus bar chart; a visitor-spend-by-category pie chart; a jump-to-section nav strip; a mobile/narrow-desktop responsive CSS block (@media max-width:640px); and an Issuu-style page-by-page flipbook viewer (PyMuPDF page rasterization) as an alternative to the single scrolling iframe, added pymupdf to requirements.txt. Verified with AppTest across default load, each period-selector option, and Custom range, zero exceptions, 4 Plotly charts confirmed rendering. Did NOT restructure report_template.html itself: production's PDF still combines 2 categories on some of its 6 pages, where VDP_udpates' has 8 pages, one category each. The flipbook gives real one-page-at-a-time viewing regardless, but the underlying page count/grouping is still a separate, bigger decision if full parity there is wanted too. | Claude |
| 2026-08-17 | Third pass, same day: added a thumbnail preview image (reusing the flipbook's cached PyMuPDF rasterization, no added render cost) to every "View a Section" card, since the cards previously showed only an icon and text and read as "no charts/visualizations" even though the Performance Snapshot and Visitor Origins sections above them do render live Plotly charts. Restructured `report_template.html` to one category per page (6 pages to 8), splitting the two combined pages (Exec Summary + Occupancy, ADR + Visitor Origins) into four full-width pages and widening the four STR bar charts (figsize 4.8x4.2 to 6.2x4.6) to use the extra space; every `[[N]] / 6` footer and `app.py`'s `SECTIONS` list were updated to match. Fixed the Visitor Origins pie chart's percentage labels crowding on adjacent thin DMA slices by placing each label manually at its own wedge's angular midpoint instead of relying on matplotlib's `autopct`/`pctdistance`. Added a 9th report page, "Notes & Commentary," that reads `dashboard_notes.sqlite` (same `NOTES_DB_PATH` the live Notes layer writes to) so commentary added in the app also appears in the exported PDF (report is now 10 total pages, cover + 9). `render_report_archive()` now renders nothing when zero reports are archived instead of an empty "Past Issues (0)" expander. Added an "Upload Your Data" panel: a source-type dropdown plus file uploader that saves into the correct canonical `data/<source>/` folder and, when `GITHUB_TOKEN`/`GITHUB_REPO` are configured, commits the file to `VDP_Dashboard` main via GitHub's Contents API and dispatches the existing Weekly STR Sync workflow, so an upload actually flows through to a dashboard refresh end to end rather than just landing in the ephemeral container. **Still needed for that last piece to work live**: a fine-grained GitHub PAT (`contents:write` + `actions:write` scoped to `gloconllc/VDP_Dashboard`) set as `GITHUB_TOKEN` on the Railway service; without it the panel still accepts and saves the file for the running session but says so plainly instead of promising a sync that isn't wired up yet. All four changes verified with AppTest (zero exceptions) and, for the PDF changes, a real `build_report()` run confirming page counts. | Claude |
