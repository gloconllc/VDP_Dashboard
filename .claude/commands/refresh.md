# Full Refresh: Code + Data + Dashboard

Pull latest code, run the full data pipeline, and restart the dashboard.

## Steps

1. Read `CLAUDE.md` to confirm current project structure and pipeline order.
2. Run: `git pull origin main`
3. Run: `python scripts/run_pipeline.py`
4. Verify: `streamlit run dashboard/app.py` starts without errors.
5. If any step fails, diagnose and fix the issue before proceeding.
6. Report: which steps succeeded, row counts from pipeline, and any warnings.

$ARGUMENTS
