#!/bin/bash
#
# run_costar_sync.sh — the whole CoStar weekly refresh, one command.
#
# Written 2026-09-20 as prep for the CoStar automation build.
# Called by deploy/com.glocon.costar-sync.plist (launchd, Thursdays 8am
# Pacific) and safe to run by hand at any time.
#
# Chain: fetch -> file -> pipeline -> freshness -> commit -> push.
# Railway auto-redeploys from main, so the push is the deploy.
#
# set -euo pipefail matters here. Without it a failed fetch would fall
# through to the commit step and push an unchanged database, which reads in
# the log as a successful weekly run. That is the exact class of silent
# failure this project has already had with fetch_str_dropbox.py.

set -euo pipefail

REPO="${VDP_REPO:-$HOME/Library/CloudStorage/OneDrive-VisitAnaheim/Documents/GitHub/VDP_Dashboard}"
cd "$REPO"

LOG_DIR="$REPO/logs"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y-%m-%d_%H%M)"
exec > >(tee -a "$LOG_DIR/costar_sync_${STAMP}.log") 2>&1

echo "=============================================================="
echo "  CoStar weekly sync — $(date)"
echo "=============================================================="

# --- credentials -----------------------------------------------------------
# Preferred: macOS Keychain, so nothing sensitive sits in a file that an
# unattended job reads. Falls back to whatever is already exported.
if command -v security >/dev/null 2>&1; then
  STR_USERNAME="$(security find-generic-password -a costar -s VDP_COSTAR_USER -w 2>/dev/null || true)"
  STR_PASSWORD="$(security find-generic-password -a costar -s VDP_COSTAR_PASS -w 2>/dev/null || true)"
  export STR_USERNAME STR_PASSWORD
fi

if [ -z "${STR_USERNAME:-}" ] || [ -z "${STR_PASSWORD:-}" ]; then
  echo "FAIL: no CoStar credentials available."
  echo "      Add them once with:"
  echo "        security add-generic-password -a costar -s VDP_COSTAR_USER -w '<email>'"
  echo "        security add-generic-password -a costar -s VDP_COSTAR_PASS -w '<password>'"
  exit 1
fi

# --- environment -----------------------------------------------------------
if [ ! -x "$REPO/venv/bin/python" ]; then
  echo "FAIL: $REPO/venv/bin/python is missing or not executable."
  echo "      Rebuild:  python3 -m venv venv && ./venv/bin/pip install -r requirements.txt"
  exit 1
fi
PY="$REPO/venv/bin/python"

# Headed. CoStar sits behind Akamai bot detection and headless from this
# machine has never been proven to get through. This is the single setting
# that makes a local scheduled run different from a CI run.
export COSTAR_HEADLESS=0

echo
echo "--- 1/5  Fetch from CoStar portal ---"
"$PY" scripts/fetch_costar_portal.py

echo
echo "--- 2/5  File downloads into data/costar/ ---"
"$PY" scripts/file_costar_downloads.py

echo
echo "--- 3/5  Run the pipeline ---"
"$PY" scripts/run_pipeline.py

echo
echo "--- 4/5  Freshness check ---"
"$PY" scripts/check_costar_freshness.py

echo
echo "--- 5/5  Commit and push ---"
if git diff --quiet --exit-code -- data/analytics.sqlite data/costar/ 2>/dev/null \
   && [ -z "$(git ls-files --others --exclude-standard -- data/costar/)" ]; then
  echo "OK: nothing changed, nothing to commit."
else
  git add data/analytics.sqlite data/costar/
  git commit -m "CoStar weekly sync: $(date +%Y-%m-%d)"
  git push origin main
  echo "OK: pushed to main. Railway will redeploy."
fi

echo
echo "=============================================================="
echo "  Done — $(date)"
echo "=============================================================="
