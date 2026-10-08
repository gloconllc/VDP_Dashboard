#!/bin/bash
#
# refresh_all.sh — load whatever STR, CoStar, and Datafy files are already
# staged in data/ and rebuild KPIs, insights, and relationships. No network
# fetches, no portal logins. Written 2026-10-07.
#
# Fetching is done first by Claude (Claude in Chrome) per docs/FETCH_RUNBOOK.md:
#   STR    -> data/str/weekly/ (monthly reports also copied to data/str/monthly/)
#   CoStar -> data/costar/ (from the VDP SharePoint Costar folder; never costar.com)
#   Datafy -> data/datafy/Datafy_MM_DD_YY/<name>_DD-MM-YYYY_to_DD-MM-YYYY.csv
#
# Usage:  ./scripts/refresh_all.sh            (load, then commit + push)
#         ./scripts/refresh_all.sh --no-push  (load only; commit in GitHub Desktop)

set -euo pipefail
PUSH=1; [ "${1:-}" = "--no-push" ] && PUSH=0
REPO="${VDP_REPO:-$HOME/Library/CloudStorage/OneDrive-VisitAnaheim/Documents/GitHub/VDP_Dashboard}"
cd "$REPO"
PY="${PY:-$REPO/venv/bin/python}"; [ -x "$PY" ] || PY=python3
mkdir -p logs
exec > >(tee -a "logs/refresh_all_$(date +%Y-%m-%d_%H%M).log") 2>&1

cp data/analytics.sqlite "$HOME/Downloads/analytics.sqlite.backup_$(date +%Y-%m-%d_%H%M)"  # safety copy outside the repo

STEPS=(
  load_str_daily_sqlite load_str_monthly_sqlite load_str_multiseg
  load_str_response_sheets load_str_translation_table
  load_costar_market_daily load_costar_segmentation load_costar_reports
  load_datafy_reports load_datafy_advertising load_datafy_extended
  compute_kpis compute_insights build_table_relationships optimize_db audit_app
)
for s in "${STEPS[@]}"; do
  echo "--- $s"
  "$PY" "scripts/$s.py"
done

if [ "$PUSH" -eq 1 ]; then
  git add data/analytics.sqlite data/costar/ data/datafy/ data/str/ 2>/dev/null || true
  git commit -m "Data refresh: STR, CoStar, Datafy $(date +%Y-%m-%d)" && git push origin main
else
  echo "Loaded. Commit data/analytics.sqlite and data/ in GitHub Desktop."
fi
