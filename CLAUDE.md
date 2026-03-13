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
~/Documents/dmo-analytics/          (local project root)
├── CLAUDE.md                       ← YOU ARE HERE
├── dashboard/
│   └── app.py                      ← Streamlit entry point
├── data/
│   └── analytics.sqlite            ← Single source-of-truth database
├── downloads/                      ← Raw STR/Datafy Excel exports
├── scripts/
│   ├── load_str_daily_sqlite.py    ← Daily STR → factstrmetrics
│   ├── load_str_monthly_sqlite.py  ← Monthly STR → factstrmetrics
│   ├── compute_kpis.py             ← Refreshes kpidailysummary
│   ├── run_pipeline.py             ← Orchestrator: ETL → KPIs → log
│   ├── fetch_fred_data.py          ← External context pull
│   ├── fetch_ca_tot.py
│   └── fetch_jwa_stats.py
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

### Core Tables

| Table | Purpose |
|---|---|
| `factstrmetrics` | Long-format STR metrics (source, grain, propertyname, market, submarket, asofdate, metricname, metricvalue, unit) |
| `kpidailysummary` | Wide-format daily KPIs (asofdate, occpct, adr, revpar, occyoy, adryoy, revparyoy, isocc80, isocc90) |
| `loadlog` | ETL audit trail (source, grain, filename, rowsinserted, createdat) |

### Dedup Rule

Composite key: `(source, grain, propertyname, market, asofdate, metricname)` — daily and monthly never collide because `grain` differs.

### Metric Names in factstrmetrics

`supply`, `demand`, `revenue`, `occ`, `adr`, `revpar`

### Units

- `occ` stored as decimal (0.688 = 68.8%). `kpidailysummary.occpct` stores percentage (68.8).
- `adr`, `revpar`, `revenue` in USD.
- `supply`, `demand` in room-nights.

---

## TBID Assessment Structure

| Nightly Rate | Assessment Rate |
|---|---|
| ≤ $199.99 | 1.0% |
| $200.00 – $399.99 | 1.5% |
| ≥ $400.00 | 2.0% |
| Blended estimate | ~1.25% |

Formula: `TBID Revenue ≈ Room Revenue × 0.0125`

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
- **Tabs:** Overview, Trends, Event Impact, Data Log
- **AI Analyst panel:** Server-side Claude API call via `ANTHROPIC_API_KEY` env var. Key never exposed in UI.
- **Home button:** Dashboard title "VDP Analytics" in the header is a clickable link that resets to Overview tab.

### Data Loaders (always use these names)

- `load_str_daily()` — pivots factstrmetrics long→wide, converts occ decimal→%
- `load_kpi_daily()` — reads kpidailysummary
- `load_compression()` — reads isocc80/isocc90 counts
- `load_loadlog()` — reads loadlog for Data Log tab

---

## Pipeline (scripts/run_pipeline.py)

Execution order:
1. `load_str_daily_sqlite.py`
2. `load_str_monthly_sqlite.py`
3. `compute_kpis.py`
4. (Future) `load_datafy_events.py`
5. (Future) `fetch_fred_data.py`, `fetch_ca_tot.py`, `fetch_jwa_stats.py`

Each step: logged with timestamp + OK/FAIL to `logs/pipeline.log`.
Fail-fast: if any step fails, pipeline aborts immediately.

---

## Commands

```bash
# Local development
cd ~/Documents/dmo-analytics
source venv/bin/activate
streamlit run dashboard/app.py

# Full refresh (data + KPIs)
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

---

## Update Log

| Date | Change | Author |
|---|---|---|
| 2026-03-09 | Initial CLAUDE.md created | Claude + John Picou |
| 2026-03-09 | CLAUDE.md installed at project root; slash commands created; home button added to dashboard title | Claude + John Picou |
