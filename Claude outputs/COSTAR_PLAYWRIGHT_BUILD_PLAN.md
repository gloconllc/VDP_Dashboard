# CoStar Playwright Automation, Build Plan (Final)

**Project:** VDP Analytics (Dana Point PULSE)
**Repo:** gloconllc/VDP_Dashboard, main branch only
**Real repo path:** `~/Library/CloudStorage/OneDrive-VisitAnaheim/Documents/GitHub/VDP_Dashboard`
**Prep done:** September 20, 2026
**Build session:** September 21, 2026

---

## 1. The headline

**Everything already works manually. The only thing missing is automation.**

On September 7 someone downloaded all five CoStar artifacts by hand, and on September 10 the pipeline loaded every one of them cleanly. The `load_log` shows it:

| File | Rows loaded |
|---|---|
| `daily_09_07_26.xlsx` | 731 |
| `monthly_09_07_26.xlsx` | 475 |
| `daily_seg_09_07_26.xlsx` | **2,193** |
| `monthly_seg_09_07_26.xlsx` | **1,425** |
| `particp_09_07_26.xlsx` | 325 |
| Market report PDFs | 8 tables refreshed |

So the loaders, the schema, the segmentation parsing, and the PDF extraction are all proven. This is not a build, it is a finishing job.

**The backfill question is also already answered.** I checked the date spans:

| Table | Span |
|---|---|
| `costar_market_monthly` | **1987-01** to 2026-07 |
| `costar_market_monthly_segment` | **1987-01** to 2026-07 |
| `costar_market_daily` | 2024-07-25 to 2026-08-29 |
| `costar_market_daily_segment` | 2024-08-29 to 2026-08-29 |
| `costar_participation` | 2024-07 to 2026-07 |

Thirty-nine years of monthly history, two years of daily, all three segments (Transient, Group, Contract) present. No backfill step is needed tomorrow. That removes about 30 minutes and one whole risk row from the earlier draft.

**What is missing is that nobody is driving it.** `fetch_costar_portal.py` covers daily and monthly only, its four other export functions are explicit `NotImplementedError` stubs, nothing moves files from `downloads/` into `data/costar/`, and nothing is scheduled.

**How stale is it, measured correctly.** The last CoStar pull was **September 7 and 8** and it loaded on September 10. That is **13 days ago** against an intended weekly cadence, so one run has been missed. Nothing more dramatic than that.

This is worth stating carefully because the obvious way to measure it is wrong. The newest *covered* dates are 2026-08-29 daily and 2026-07 monthly, which looks alarming against today's calendar, but CoStar publishes with a real lag: the September 7 pull contained daily data only through August 29 (10 days behind) and monthly only through July, because July was the last closed month. That lag is normal and is not staleness. The question that matters is how long since somebody pulled, not how old the newest row is.

---

## 2. What I built tonight

Four new files, written into the repo and validated. They are the parts with no unknowns, so tomorrow can be spent entirely on the parts that need your screen.

**`scripts/file_costar_downloads.py`** (190 lines). Moves validated exports from `downloads/` into flat `data/costar/` under exactly the names the loaders already parse: `daily_MM_DD_YY.xlsx`, `monthly_MM_DD_YY.xlsx`, `daily_seg_MM_DD_YY.xlsx`, `monthly_seg_MM_DD_YY.xlsx`, `particp_MM_DD_YY.xlsx`, and CoStar's own PDF names. Refuses to overwrite. Re-validates everything, with a separate PyMuPDF check for PDFs since the Excel check does not apply. Skips any file with no `MM_DD_YY` tag rather than falling back to a shared default period, which is the trap that already bit the Datafy loader.

**`scripts/check_costar_freshness.py`**. The second half of skip-safe, and the one file I had to write twice.

The first version compared each table's newest covered period against today's calendar and reported the tables as 23 and 82 days stale. That was wrong. It was measuring CoStar's normal publishing lag and calling it staleness, which is precisely the bug this repo has already found and fixed three times, in `audit_app.py` and in `compute_insights.py`'s `load_kpi_recent` and `load_str_revenue`. I wrote a comment in the file warning about that bug and then committed it anyway.

The corrected version measures two separate things. **Pull recency** is the real check: how many days since anyone actually pulled, read from `costar_annual_performance.report_date`, `costar_segment_room_split.report_date`, `costar_participation.snapshot_date`, and the newest `MM_DD_YY` stamp on a file in `data/costar/`. Threshold 10 days, so one missed weekly run is caught before a second goes by. **Coverage lag** is a sanity check only, measuring how far each table sits behind *the pull date*, never behind today, so an abnormally truncated export still gets flagged without normal lag tripping it.

Run against your live database it now reports the honest answer: last pull 2026-09-08, 13 days ago, coverage normal on all five tables.

**`scripts/run_costar_sync.sh`**. Fetch, file, pipeline, freshness, commit, push, in one command. `set -euo pipefail` so a failed download cannot fall through and push an unchanged database. Reads credentials from Keychain. Sets `COSTAR_HEADLESS=0`, which is the one setting that makes a local run different from a CI run. Skips the commit entirely when nothing changed.

**`deploy/com.glocon.costar-sync.plist`**. launchd, Thursday 8:00 a.m. local, matching your existing `str_weekly_sync.yml` cadence. Validated as well-formed. Install instructions are in the file header.

All four are new and untracked. Nothing existing was modified.

---

## 3. Tomorrow, three steps

### Step 1. Recon, 30 minutes, you at the keyboard

Run the existing script with `COSTAR_HEADLESS=0` so we can watch it. Two things come out of this.

First, we find out whether the daily and monthly selectors still work. The script's own docstring admits they were copied from `str_playwright_automation.py` and **have never been run against the live site**. Better to learn that with you there than at 8:00 on a Thursday.

Second, you walk me to the three screens nobody has mapped: the segmentation view, wherever the participation export comes from, and the Market Reports library. I capture selectors and screenshots as we go.

### Step 2. Fill in the four stubs, 90 minutes

`export_daily_segmentation`, `export_monthly_segmentation`, `export_participation`, and `export_market_report_pdf` already exist as named functions raising `NotImplementedError`. They get real bodies, using the same session, the same validation, and the same screenshot discipline the working exports use. Then `main()` calls them.

### Step 3. Wire, schedule, verify, 60 minutes

Add `fetch_costar_portal.py` and `file_costar_downloads.py` to `run_pipeline.py` as steps 0a and 0b, both skip-safe. Install the launchd agent. Then a full end-to-end run.

Verification is not just "it ran." Read three segmentation figures off the CoStar screen and match them to rows in `costar_market_daily_segment` by hand. A loader that runs clean and writes the wrong column is this project's most expensive failure mode, and the percent-versus-fraction problem has already caused it once.

About two and a half hours, most of it with you free to do other things after the first half hour.

---

## 4. What I need from you

**One real blocker.** The repo's `venv` does not work. `venv/bin/python` exits 0 and produces no output at all, for a script file, for `-c`, even for `-V`. Its `pyvenv.cfg` says Python 3.10.12 and the symlink chain in `venv/bin/` looks circular. **Playwright is not importable**, even though `requirements.txt` correctly pins `playwright>=1.40.0,<2.0.0`. So before tomorrow:

```bash
cd ~/Library/CloudStorage/OneDrive-VisitAnaheim/Documents/GitHub/VDP_Dashboard
rm -rf venv
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/playwright install chromium
./venv/bin/python -V          # should print a version. If it prints nothing, tell me.
```

One caveat on that diagnosis: I am reading your folder through a Linux sandbox, not macOS directly, so some of what I see may be an artifact of that. Rebuilding the venv is harmless either way and takes two minutes.

**At the keyboard for the first 30 minutes.** The segmentation view, the participation export's origin, and the Market Reports library. Three screens, that is all.

**Deferred at your request.** The `.env` situation. Whenever you want to pick it back up, it is a five-minute job.

---

## 5. Housekeeping, when you want it

Seven directories in the repo root named `#`, `already`, `created`, `if`, `not`, `skip`, and `you` are complete Python 3.9 virtualenvs, and their `site-packages` are **tracked in git**. A multi-line command got pasted into zsh and ran `python -m venv` on each word of an error message. This is the same bloat that made VDP_udpates slow to check out, and it may well be related to why the real `venv` is broken.

Also in the root: `diagnose_docstring.py`, `fix_docstring.py`, and three variant copies of the monthly loader in `scripts/` (`.save`, `.pyo`, `_broken.py`). One cleanup commit clears all of it.

---

## 6. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| The carried-over daily and monthly selectors are stale | **Moderate** | High | This is step 1, with you watching |
| Akamai blocks the session even headed | Low | High | Residential IP and real browser. Fallback is a persistent profile so login runs rarely |
| Segmentation not exportable at daily grain | **Low** | Low | A `daily_seg` file already exists and loaded 2,193 rows, so it clearly is |
| Segmentation uses a different percent convention | Low | High | Already loaded once correctly, so the parse is proven. Step 3's manual check confirms |
| Mac asleep Thursday 8 a.m. | High | Low | launchd runs at next wake, freshness check tolerates 10 days |
| Broken venv blocks everything | **Certain until fixed** | High | Rebuild tonight, two minutes |

---

## 7. Why bother

Your CoStar data is not broken and it is not badly out of date. It is 13 days since the last pull, against a weekly intention. One missed run. The reason it slipped is simply that refreshing means someone remembering to click through five exports, and that is the kind of gap that widens quietly rather than failing loudly.

The segmentation tables in particular are what let Forward Outlook and Group Business show sourced group revenue instead of the benchmark estimates it currently has to exclude. They are already populated and already wired into `pages.py` and `components_group.py`. Keeping them current is the difference between that section being trustworthy and it slowly aging out.

---

**End of plan.**
