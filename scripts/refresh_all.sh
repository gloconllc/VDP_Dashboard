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
# Added 2026-10-15: after loading, new Datafy and STR files are mirrored to the
# synced Visit Dana Point SharePoint folders (copy only, never overwrite), and
# the push step requires typing "yes" at a prompt (explicit approval).
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

# Mirror this week's files to the synced SharePoint folders (desktop + online).
SP_ROOT="${VDP_SP_ROOT:-$HOME/Library/CloudStorage}"
SP_DATAFY="${VDP_SP_DATAFY:-$(ls -d "$SP_ROOT"/*/"Business Intelligence - Reports"/Datafy 2>/dev/null | head -1)}"
SP_STR="${VDP_SP_STR:-$(ls -d "$SP_ROOT"/*/"Business Intelligence - Reports"/STR 2>/dev/null | head -1)}"
if [ -n "$SP_DATAFY" ]; then
  for B in $(ls -d data/datafy/Datafy_??_??_?? 2>/dev/null | tail -1); do
    STAMP="${B##*/}"; DEST="$SP_DATAFY/For the Dashboard/$STAMP"; mkdir -p "$DEST"
    cp -n "$B"/*.csv "$DEST"/ 2>/dev/null || true
    SUF="${STAMP#Datafy}"   # _MM_DD_YY
    cp -n data/datafy/*"$SUF".csv "$DEST"/ 2>/dev/null || true
    echo "Synced Datafy batch to $DEST"
  done
else
  echo "NOTE: synced SharePoint Datafy folder not found; set VDP_SP_DATAFY."
fi
if [ -n "$SP_STR" ]; then
  cp -n data/str/weekly/*.xls "$SP_STR"/ 2>/dev/null || true
  echo "Synced STR weekly files to $SP_STR"
fi

if [ "$PUSH" -eq 1 ]; then
  read -r -p "Commit and push to main (Railway will redeploy)? Type yes to approve: " OK
  [ "$OK" = "yes" ] || { echo "Push skipped. Re-run or commit in GitHub Desktop."; exit 0; }
  git add data/analytics.sqlite data/costar/ data/datafy/ data/str/ 2>/dev/null || true
  git commit -m "Data refresh: STR, CoStar, Datafy $(date +%Y-%m-%d)" && git push origin main
else
  echo "Loaded. Commit data/analytics.sqlite and data/ in GitHub Desktop."
fi
