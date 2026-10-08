#!/bin/bash
#
# ingest_costar.sh — on-demand CoStar refresh from files John downloads by hand.
#
# Written 2026-10-07. CoStar's Terms of Use prohibit automated agents (the
# portal blocked the Claude extension on 2026-10-07), so nothing in this
# project logs in to CoStar or downloads from it. John pulls the exports
# himself; this script does everything after that.
#
# HOW TO USE
#   1. Download from CoStar as usual and save into the repo's downloads/ folder
#      using these names (MM_DD_YY = the day you pulled them):
#        daily_MM_DD_YY.xlsx         monthly_MM_DD_YY.xlsx
#        daily_seg_MM_DD_YY.xlsx     monthly_seg_MM_DD_YY.xlsx
#        particp_MM_DD_YY.xlsx       market report PDFs (keep CoStar's name)
#   2. In Terminal:
#        cd ~/Library/CloudStorage/OneDrive-VisitAnaheim/Documents/GitHub/VDP_Dashboard
#        ./scripts/ingest_costar.sh            # file, load, commit, push
#        ./scripts/ingest_costar.sh --no-push  # same, but leave the commit
#                                              # to GitHub Desktop
#
# Chain: file -> pipeline -> freshness -> commit -> push.
# A file that fails validation stops the run before anything is loaded or
# pushed, so a bad download never reaches the live Railway app.

set -euo pipefail

PUSH=1
[ "${1:-}" = "--no-push" ] && PUSH=0

REPO="${VDP_REPO:-$HOME/Library/CloudStorage/OneDrive-VisitAnaheim/Documents/GitHub/VDP_Dashboard}"
cd "$REPO"

LOG_DIR="$REPO/logs"
mkdir -p "$LOG_DIR" "$REPO/downloads"
STAMP="$(date +%Y-%m-%d_%H%M)"
exec > >(tee -a "$LOG_DIR/costar_ingest_${STAMP}.log") 2>&1

echo "=============================================================="
echo "  CoStar on-demand ingest — $(date)"
echo "=============================================================="

if [ ! -x "$REPO/venv/bin/python" ]; then
  echo "FAIL: venv missing. Rebuild:"
  echo "  rm -rf venv && /opt/homebrew/bin/python3.11 -m venv venv && ./venv/bin/pip install -r requirements.txt"
  exit 1
fi
PY="$REPO/venv/bin/python"

if [ -z "$(find downloads -maxdepth 1 -type f \( -iname '*.xlsx' -o -iname '*.xls' -o -iname '*.pdf' \) 2>/dev/null)" ]; then
  echo "Nothing in downloads/. Save the CoStar exports there first, then rerun."
  exit 0
fi

echo
echo "--- 1/4  File downloads into data/costar/ ---"
"$PY" scripts/file_costar_downloads.py

echo
echo "--- 2/4  Run the pipeline ---"
"$PY" scripts/run_pipeline.py

echo
echo "--- 3/4  Freshness check (warning only) ---"
"$PY" scripts/check_costar_freshness.py || echo "WARN: freshness check flagged something; review above. Continuing."

echo
echo "--- 4/4  Commit and push ---"
if git diff --quiet --exit-code -- data/analytics.sqlite data/costar/ 2>/dev/null \
   && [ -z "$(git ls-files --others --exclude-standard -- data/costar/)" ]; then
  echo "OK: nothing changed, nothing to commit."
elif [ "$PUSH" -eq 1 ]; then
  git add data/analytics.sqlite data/costar/
  git commit -m "CoStar data refresh (manual pull): $(date +%Y-%m-%d)"
  git push origin main
  echo "OK: pushed to main. Railway will redeploy."
else
  echo "OK: data loaded. Review and commit data/analytics.sqlite + data/costar/ in GitHub Desktop."
fi

echo
echo "=============================================================="
echo "  Done — $(date)"
echo "=============================================================="
