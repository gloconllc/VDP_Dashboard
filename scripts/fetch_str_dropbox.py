"""
fetch_str_dropbox.py

Downloads STR weekly and monthly Excel exports from Dropbox public shared folder links.
No API token required — uses public shared folder links with dl=1 direct download.

Saves files to:
  data/str/weekly/   ← STR weekly (daily-grain) exports
  data/str/monthly/  ← STR monthly-grain exports

Usage:
  python scripts/fetch_str_dropbox.py
"""

import os
import re
import sys
import time
import json
import requests
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent

WEEKLY_DIR  = PROJECT_ROOT / "data" / "str" / "weekly"
MONTHLY_DIR = PROJECT_ROOT / "data" / "str" / "monthly"

# Public shared folder links — no token needed
WEEKLY_FOLDER_URL  = (
    "https://www.dropbox.com/scl/fo/ua6phm862g2dhlivuhhzh/ANKnc0sSWvtvF2TTcT4_j1w/2026"
    "?rlkey=mmhmwkgp0qcyoop3szrz8xtdx&dl=0"
)
MONTHLY_FOLDER_URL = (
    "https://www.dropbox.com/scl/fo/ua6phm862g2dhlivuhhzh/APCDmFO0BYYCJhG7y8cPLWk/2026/2026%20Monthly"
    "?rlkey=mmhmwkgp0qcyoop3szrz8xtdx&dl=0"
)

# Dropbox API for listing shared folders (no auth needed for public links)
DROPBOX_LIST_SHARED_URL = "https://api.dropboxapi.com/2/sharing/list_folder"
DROPBOX_LIST_CONTINUE_URL = "https://api.dropboxapi.com/2/sharing/list_folder/continue"
DROPBOX_SHARED_DOWNLOAD_URL = "https://content.dropboxapi.com/2/sharing/get_shared_link_file"

HEADERS = {"Content-Type": "application/json"}


def get_token() -> str:
    """Read Dropbox token — accepts DROPBOX_API_TOKEN (GitHub secret) or DROPBOX_ACCESS_TOKEN.

    A token is OPTIONAL. The shared folder links above are public (dl=0 links with
    an rlkey), so this script can list and download files with no token at all via
    the HTML-scrape fallback (list_shared_folder_via_html / download_file_direct).
    A real token just makes listing more reliable (uses the proper Dropbox API
    instead of scraping the folder page). Never hard-exit here — that previously
    caused every scheduled run to fail outright before it ever tried the public,
    no-token path. Confirmed via logs/pipeline.log: every run since 2026-08-10
    failed at this line with no attempt made.
    """
    token = (
        os.environ.get("DROPBOX_API_TOKEN", "").strip()
        or os.environ.get("DROPBOX_ACCESS_TOKEN", "").strip()
    )
    if not token:
        print("  [INFO] No DROPBOX_API_TOKEN set — using public shared-link access (no auth).")
    return token


def list_shared_folder_files(folder_url: str, token: str = "") -> list[dict]:
    """List .xlsx/.xls files in a Dropbox shared folder."""
    auth_headers = {**HEADERS, "Authorization": f"Bearer {token}"} if token else HEADERS
    entries = []
    cursor = None

    while True:
        if cursor:
            resp = requests.post(
                "https://api.dropboxapi.com/2/files/list_folder/continue",
                headers=auth_headers,
                json={"cursor": cursor},
                timeout=30,
            )
        else:
            resp = requests.post(
                "https://api.dropboxapi.com/2/files/list_folder",
                headers=auth_headers,
                json={"shared_link": {"url": folder_url}, "path": "", "recursive": False},
                timeout=30,
            )

        if resp.status_code != 200:
            print(f"  [WARN] API list failed ({resp.status_code}), falling back to HTML scrape")
            return list_shared_folder_via_html(folder_url)

        data = resp.json()
        for e in data.get("entries", []):
            if e.get(".tag") == "file" and e["name"].lower().endswith((".xlsx", ".xls")):
                entries.append(e)

        if not data.get("has_more"):
            break
        cursor = data.get("cursor")

    return entries


def list_shared_folder_via_html(folder_url: str) -> list[dict]:
    """Fallback: scrape the Dropbox folder page for .xls/.xlsx filenames."""
    try:
        resp = requests.get(folder_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except Exception as e:
        print(f"  [ERROR] Could not fetch folder page: {e}")
        return []

    # Dropbox embeds file metadata as JSON in the page
    names = re.findall(r'"filename"\s*:\s*"([^"]+\.(?:xlsx|xls))"', resp.text, re.IGNORECASE)
    if not names:
        # Try alternate pattern
        names = re.findall(r'href="[^"]*/(VisitDanaPoint_[^"]+\.(?:xlsx|xls))"', resp.text, re.IGNORECASE)

    entries = [{"name": n, "path_lower": f"/{n.lower()}"} for n in set(names)]
    print(f"  [HTML scrape] Found {len(entries)} file(s)")
    return entries


def download_file_direct(folder_url: str, filename: str, dest_path: Path) -> bool:
    """Download a file from a public Dropbox shared folder using direct URL."""
    # Build direct download URL: replace dl=0 with dl=1 and append filename
    base = folder_url.split("?")[0].rstrip("/")
    rlkey = re.search(r"rlkey=([^&]+)", folder_url)
    rlkey_param = f"&rlkey={rlkey.group(1)}" if rlkey else ""

    # Method 1: subfolder URL + filename path
    direct_url = f"{base}/{filename}?dl=1{rlkey_param}"

    for attempt in range(4):
        try:
            resp = requests.get(direct_url, timeout=120, stream=True,
                                headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 1000:
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            print(f"  [WARN] attempt {attempt+1}: HTTP {resp.status_code} ({len(resp.content)} bytes)")
        except requests.RequestException as e:
            print(f"  [WARN] attempt {attempt+1} network error: {e}")
        if attempt < 3:
            time.sleep(2 ** attempt)

    return False


def sync_folder(folder_url: str, local_dir: Path, label: str, token: str = "") -> list[Path]:
    """List and download all new/updated .xlsx/.xls files from a Dropbox shared folder."""
    local_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[{label}] Listing files...")
    entries = list_shared_folder_files(folder_url, token)

    if not entries:
        print(f"  No files found — check folder URL or network access.")
        return []

    print(f"  Found {len(entries)} file(s):")
    for e in entries:
        print(f"    {e['name']}")

    downloaded = []
    for entry in entries:
        name = entry["name"]
        dest = local_dir / name

        if dest.exists():
            remote_modified = entry.get("server_modified", "")
            local_mtime = datetime.fromtimestamp(dest.stat().st_mtime).strftime("%Y-%m-%dT%H:%M:%SZ")
            if remote_modified and remote_modified <= local_mtime:
                print(f"  SKIP (up to date): {name}")
                downloaded.append(dest)
                continue

        print(f"  Downloading: {name} ...", end=" ", flush=True)
        ok = download_file_direct(folder_url, name, dest)
        if ok:
            size = dest.stat().st_size
            print(f"OK ({size:,} bytes)")
            downloaded.append(dest)
        else:
            print("FAILED")
            if dest.exists() and dest.stat().st_size < 1000:
                dest.unlink()  # remove corrupt partial file

    return downloaded


def main():
    token = get_token()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] fetch_str_dropbox starting")

    weekly_files  = sync_folder(WEEKLY_FOLDER_URL,  WEEKLY_DIR,  "WEEKLY/DAILY",  token)
    monthly_files = sync_folder(MONTHLY_FOLDER_URL, MONTHLY_DIR, "MONTHLY", token)

    print(f"\nSummary:")
    print(f"  Weekly files:  {len(weekly_files)} in {WEEKLY_DIR}")
    print(f"  Monthly files: {len(monthly_files)} in {MONTHLY_DIR}")

    if not weekly_files and not monthly_files:
        print("\n[WARN] No files downloaded. Possible causes:")
        print("  - Dropbox folder URL changed or expired")
        print("  - Network blocked in this environment")
        print("  - Files already up to date")
        sys.exit(1)

    print(f"\n[DONE] fetch_str_dropbox complete")


if __name__ == "__main__":
    main()
