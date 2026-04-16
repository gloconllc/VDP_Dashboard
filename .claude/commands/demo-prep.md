# Dana Point PULSE — Demo Prep (Thursday, April 9)

Run the appropriate day's tasks to get the app ready for the Visit Dana Point demo.

Usage:
- `/demo-prep day1` — Data freshness, API keys, smoke test
- `/demo-prep day2` — Polish audit, narrative, AI questions
- `/demo-prep day3` — Final refresh, dress rehearsal, cheat sheet

---

## DAY 1 — Data Freshness + API Keys + Smoke Test

Run these if argument is `day1` or empty.

1. Read `CLAUDE.md` to confirm project structure.
2. Run `python3 scripts/run_pipeline.py` — all 21 steps must complete. Note any FAIL or WARN.
3. Audit all tables: run `python3 -c "import sqlite3; conn=sqlite3.connect('data/analytics.sqlite'); cur=conn.cursor(); cur.execute(\"SELECT name FROM sqlite_master WHERE type='table'\"); [print(cur.execute(f'SELECT COUNT(*) FROM \"{t[0]}\"').fetchone()[0], t[0]) for t in cur.fetchall()]"` — flag any 0-row tables.
4. Check `downloads/` directory for any new STR, Datafy, CoStar, or Later.com files — if found, move to `data/<source>/` and re-run pipeline.
5. Verify insights exist for today's date across all 5 audiences (dmo, city, visitor, resident, cross): `python3 -c "import sqlite3,pandas as pd; df=pd.read_sql_query('SELECT audience,COUNT(*) n FROM insights_daily WHERE as_of_date=date(\"now\") GROUP BY audience',sqlite3.connect('data/analytics.sqlite')); print(df)"`.
6. Read the `.env` file (use relative path with `os.listdir`) — confirm these keys are present and non-empty: ANTHROPIC_API_KEY, FRED_API_KEY, EIA_API_KEY, GOOGLE_AI_API_KEY. Report any missing.
7. Verify ANTHROPIC_API_KEY is at least 100 characters — truncated keys will pass the UI check but fail at runtime.
8. Check app.py syntax: `python3 -c "import ast; ast.parse(open('dashboard/app.py').read()); print('OK')"`.
9. Verify Board Report numbers from DB: avg occ, ADR, RevPAR for last 30 days; 12-month revenue; TBID and TOT estimates; Datafy total trips, overnight %, OOS %.
10. Commit any changed files (`data/analytics.sqlite`, logs) to `main` and push: `git add data/analytics.sqlite && git commit -m "Day 1 pipeline refresh" && git push origin main`.
11. Report: pipeline status, table count, any 0-row tables, key metrics, and any issues found.

---

## DAY 2 — Polish Audit + Demo Narrative

Run these if argument is `day2`.

### Polish Checks (code audit — no browser needed)

1. Confirm occupancy is displayed as `%` (not decimal) — search app.py for `occ_pct` display patterns.
2. Confirm Event Impact tab has Ohana Fest hardcoded metrics: $14.6M expenditure, $18.4M destination spend, +$139 ADR lift, 3.2× multiplier, 68% OOS — grep for these values.
3. Scan Board Report section for any `TBD`, `N/A`, or `0.0` that would show to the client when data is available — flag any that are not proper fallbacks.
4. Confirm admin panel is gated: `grep -n "_is_admin\|?admin" dashboard/app.py` — API key input fields must only appear when `?admin=true`.
5. Confirm sidebar status dots all use `> 0` logic and all tables have data — all should be 🟢.
6. Confirm `_LOGIN_ENABLED = False` in app.py so VDP goes straight in with no login screen.

### Demo Narrative Output

Print the following ready-to-use demo materials:

**3-Minute Walk-Through Script** (tab order: Overview → Event Impact → Our Visitors → Where They're From → What's Next → AI Analyst):
- Tab 1 Overview (:00–:45): Cite 30-day occ, ADR, RevPAR from DB. Cite 12-month revenue, TBID, TOT.
- Tab 6 Event Impact (:45–1:15): Ohana Fest $14.6M / $18.4M / $139 ADR lift / 3.2× multiplier.
- Tab 4 Our Visitors (1:15–1:45): 3.55M trips, 61% OOS, 59% overnight, 2.0-day LOS.
- Tab 5 Where They're From (1:45–2:15): LA = volume, fly markets (SLC/Dallas/NYC) = 1.3–1.4× more revenue per trip.
- Tab 3 What's Next (2:15–2:40): Q3 2025 34 compression days; forward signals from Visit CA + FRED + Trends.
- AI Analyst (2:40–3:00): Paste killer question live and let it stream.

**4 Killer AI Analyst Questions** (cross-dataset, STR + Datafy):
1. "Which feeder markets are sending the most visitors but capturing the least in ADR? Where is the rate gap and how much revenue is being left on the table?"
2. "We have 1.44 million day trippers. If we convert just 3% to overnight stays, what does that mean for room revenue, TBID, and TOT?"
3. "Is our marketing spend amplifying peak season or building shoulder demand? What does compression data say about where to shift media dollars?"
4. "Based on Ohana Fest 2025 — $18.4M destination spend, 3.2× multiplier, +$139 ADR lift — what should our 2026 event marketing strategy look like?"

**Verbal Bridges Between Tabs**:
- Overview → Event Impact: "The headline numbers are strong — let me show you what's driving the peaks."
- Event Impact → Our Visitors: "Events tell us when demand spikes. Visitor data tells us who's actually showing up."
- Our Visitors → Where They're From: "Not just who, but where they're coming from and what they're worth."
- Where They're From → What's Next: "That's the current picture. Here's what the data says is coming."
- What's Next → AI Analyst: "Instead of me interpreting all of this — ask it directly."

---

## DAY 3 — Stability + Dress Rehearsal + Cheat Sheet

Run these if argument is `day3`.

1. Run final pipeline: `python3 scripts/run_pipeline.py` — confirm all 21 steps OK.
2. Commit and push: `git add data/analytics.sqlite && git commit -m "Day 3 final pipeline refresh — demo ready" && git push origin main`.
3. Verify git log shows `main` is clean and up to date with origin.
4. Read the `.env` file — confirm ANTHROPIC_API_KEY is present, non-empty, and at least 100 characters long.
5. Report the exact live URL and admin URL for the demo (from CLAUDE.md or Streamlit Cloud config if available).
6. Print the final 1-page cheat sheet with:
   - Live URL and admin URL (?admin=true)
   - Key metrics to cite (pull fresh from DB: occ, ADR, RevPAR, revenue, TBID, TOT, trips, OOS%)
   - The 4 killer AI questions (formatted to copy-paste)
   - The 5 verbal bridge lines
   - Reminder: ANTHROPIC_API_KEY must be set on Streamlit Cloud, not just local .env
   - Reminder: STR data last date (check DB) — if > 30 days old, note it for VDP audience
7. Do a final syntax check: `python3 -c "import ast; ast.parse(open('dashboard/app.py').read()); print('app.py OK')"`.
8. Confirm no uncommitted changes remain: `git status --short`.

---

$ARGUMENTS
