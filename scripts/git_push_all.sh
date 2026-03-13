#!/usr/bin/env bash
set -e

# Go to project root
cd /Users/johnpicou/Documents/dmo-analytics

# Activate venv if it exists
if [ -d "venv" ]; then
  echo "==> Activating venv"
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

echo "==> Showing git status"
git status

echo "==> Staging all changes"
git add .

# Build a timestamped commit message if none provided
if [ -z "$1" ]; then
  MSG="Update $(date '+%Y-%m-%d %H:%M')"
else
  MSG="$1"
fi

echo "==> Committing with message: $MSG"
git commit -m "$MSG" || echo "No changes to commit."

echo "==> Pushing to origin main"
git push origin main

echo "==> Done. Check Streamlit Cloud for redeploy."

