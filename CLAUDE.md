# Project: VDP Analytics (Visit Dana Point)

DMO tourism analytics platform — ETL pipeline, SQLite brain, Streamlit dashboard, Claude AI Analyst panel.
Owner: John Picou | Org: gloconllc | Repo: VDPDashboard

---

## Data Hierarchy (NEVER violate)

- **Layer 1 — Truth:** STR daily/monthly exports, Datafy event data, TBID assessment docs. These are vetted. Always cite these first.
- **Layer 2 — Context:** FRED hotel pricing index, CA State TOT data, JWA passenger counts, Visit California forecasts.
- **Layer 3 — Color:** Media, social sentiment, competitive anecdotes. Never override Layer 1 with Layer 3.

---

## Repository Structure

```
VDP_Dashboard/                      (project root)
├── CLAUDE.md                       ← YOU ARE HERE
├── dashboard/
│   └── app.py                      ← Streamlit entry point (tabs: Overview, Trends, Forward Outlook, Event Impact, Data Log)
├── data/
│   └── analytics.sqlite            ← Single source-of-truth database (25+ tables)
├── downloads/                      ← Raw STR/Datafy Excel exports
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
- **Tabs:** Overview, Trends, Forward Outlook, Event Impact, Data Log
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

## Commands

```bash
# Local development
source venv/bin/activate
streamlit run dashboard/app.py

# Full refresh (all tables → KPIs → insights)
python scripts/run_pipeline.py

# Full refresh + latest code from GitHub
git pull origin main && python scripts/run_pipeline.py

# Deploy
git add . && git commit -m "description" && git push
# Streamlit Cloud auto-redeploys from main branch
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
- ALWAYS run `python scripts/run_pipeline.py` after schema changes
- ALWAYS reference this CLAUDE.md before making changes
- Dashboard is customer-facing — no API key fields, no debug output
- The Anthropic API key is set server-side via `ANTHROPIC_API_KEY` env var only
- After every code change, verify the app still runs: `streamlit run dashboard/app.py`
- `compute_insights.py` must run on every pipeline execution — it is the brain's daily self-update

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

---

## Update Log

| Date | Change | Author |
|---|---|---|
| 2026-03-09 | Initial CLAUDE.md created | Claude + John Picou |
| 2026-03-09 | CLAUDE.md installed at project root; slash commands created; home button added to dashboard title | Claude + John Picou |
| 2026-03-16 | Full brain upgrade: insights_daily + table_relationships schema; compute_insights.py (4 audiences, 17 insight types); pipeline updated to run all 25+ tables; Forward Outlook tab added to dashboard; AI system prompt extended with full schema | Claude + John Picou |
