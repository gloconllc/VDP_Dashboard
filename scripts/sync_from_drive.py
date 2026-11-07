# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
sync_from_drive.py — Download updated data files from Google Drive to local data/ directories.

Uses a service account for authentication. Folder IDs are read from environment variables.
Preserves subfolder structure. Skip-safe: missing env vars are warned and skipped.

Usage:
    export GOOGLE_SERVICE_ACCOUNT_JSON=/path/to/sa_key.json   # or set to raw JSON content
    export DRIVE_FOLDER_STR=<folder_id>
    export DRIVE_FOLDER_DATAFY=<folder_id>
    ...
    python scripts/sync_from_drive.py

Exit codes:
    0 — success (even if some folders were skipped due to missing env vars)
    1 — fatal error (auth failure, unexpected exception)
"""

from __future__ import annotations

import io
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Optional imports — fail loudly if google libs not installed
# ---------------------------------------------------------------------------
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaIoBaseDownload
except ImportError as exc:
    print(
        f"[sync_from_drive] ERROR: Google API libraries not installed. "
        f"Run: pip install google-auth google-auth-httplib2 google-api-python-client\n{exc}"
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# Maps (env_var_name, local_data_subdir)
FOLDER_MAP: list[tuple[str, str]] = [
    ("DRIVE_FOLDER_STR", "data/str"),
    ("DRIVE_FOLDER_DATAFY", "data/datafy"),
    ("DRIVE_FOLDER_COSTAR", "data/costar"),
    ("DRIVE_FOLDER_ZARTICO", "data/Zartico"),
    ("DRIVE_FOLDER_VISIT_CA", "data/Visit_California"),
    ("DRIVE_FOLDER_LATER", "data/later"),
    ("DRIVE_FOLDER_US_TRAVEL", "data/us_travel"),
]

# Project root = two levels up from this script (scripts/ → project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _build_service():
    """Build and return an authenticated Drive v3 service client."""
    sa_json_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "/tmp/sa_key.json")

    # If the env var contains raw JSON (common in CI), write it to a temp file first
    if sa_json_path and sa_json_path.strip().startswith("{"):
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        tmp.write(sa_json_path.strip())
        tmp.flush()
        sa_json_path = tmp.name

    if not os.path.exists(sa_json_path):
        raise FileNotFoundError(
            f"Service account key not found at '{sa_json_path}'. "
            "Set GOOGLE_SERVICE_ACCOUNT_JSON env var to the file path or raw JSON content."
        )

    creds = service_account.Credentials.from_service_account_file(
        sa_json_path, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


# ---------------------------------------------------------------------------
# Drive helpers
# ---------------------------------------------------------------------------

def _parse_drive_datetime(dt_str: str | None) -> datetime | None:
    """Parse RFC 3339 datetime string returned by Drive API."""
    if not dt_str:
        return None
    # Drive returns strings like '2025-04-01T12:34:56.789Z'
    dt_str = dt_str.rstrip("Z").split(".")[0]  # strip microseconds + Z
    return datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)


def _list_folder_contents(service, folder_id: str) -> list[dict]:
    """Return all files and subfolders in a Drive folder (non-recursive, paginated)."""
    items: list[dict] = []
    page_token: str | None = None
    query = f"'{folder_id}' in parents and trashed = false"

    while True:
        params: dict = {
            "q": query,
            "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, size)",
            "pageSize": 200,
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            resp = service.files().list(**params).execute()
        except HttpError as exc:
            print(f"  [sync] Drive API error listing folder {folder_id}: {exc}")
            raise

        items.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return items


def _download_file(service, file_id: str, dest_path: Path) -> None:
    """Download a Drive file to dest_path, creating parent dirs as needed."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id)

    with io.FileIO(str(dest_path), mode="wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=8 * 1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _is_google_doc(mime_type: str) -> bool:
    """Return True for Google Workspace native types (Docs, Sheets, Slides, etc.)."""
    return mime_type.startswith("application/vnd.google-apps.")


# ---------------------------------------------------------------------------
# Core sync logic
# ---------------------------------------------------------------------------

def _sync_folder(
    service,
    folder_id: str,
    local_dir: Path,
    *,
    stats: dict,
    depth: int = 0,
) -> None:
    """
    Recursively sync a Drive folder to local_dir.

    Updates stats dict in-place:
        stats['checked']    — total files visited
        stats['downloaded'] — files actually downloaded
        stats['skipped']    — files skipped (already up to date)
        stats['errors']     — files that failed to download
    """
    indent = "  " * (depth + 1)

    try:
        items = _list_folder_contents(service, folder_id)
    except HttpError as exc:
        print(f"{indent}[sync] ERROR listing folder {folder_id}: {exc}")
        stats["errors"] += 1
        return

    for item in items:
        name: str = item["name"]
        mime: str = item["mimeType"]
        item_id: str = item["id"]

        # Recurse into subfolders to preserve directory structure
        if mime == "application/vnd.google-apps.folder":
            subfolder_local = local_dir / name
            subfolder_local.mkdir(parents=True, exist_ok=True)
            _sync_folder(
                service,
                item_id,
                subfolder_local,
                stats=stats,
                depth=depth + 1,
            )
            continue

        # Skip native Google Workspace files (Docs, Sheets, etc.)
        if _is_google_doc(mime):
            print(f"{indent}[sync] SKIP (Google Workspace format, export not supported): {name}")
            continue

        stats["checked"] += 1
        dest_path = local_dir / name
        drive_mtime = _parse_drive_datetime(item.get("modifiedTime"))

        # Compare modification times — skip if local copy is current
        if dest_path.exists() and drive_mtime:
            local_mtime = datetime.fromtimestamp(
                dest_path.stat().st_mtime, tz=timezone.utc
            )
            if local_mtime >= drive_mtime:
                print(
                    f"{indent}[sync] up-to-date: {dest_path.relative_to(PROJECT_ROOT)}"
                )
                stats["skipped"] += 1
                continue

        # Download the file
        print(f"{indent}[sync] downloading: {dest_path.relative_to(PROJECT_ROOT)}")
        try:
            _download_file(service, item_id, dest_path)
            # Stamp local mtime with Drive mtime so future runs detect correctly
            if drive_mtime:
                ts = drive_mtime.timestamp()
                os.utime(str(dest_path), (ts, ts))
            stats["downloaded"] += 1
        except Exception as exc:  # noqa: BLE001
            print(f"{indent}[sync] ERROR downloading '{name}': {exc}")
            stats["errors"] += 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"[sync_from_drive] Starting Drive sync — {datetime.now().isoformat()}")

    # Build authenticated service once; bail out on auth failure
    try:
        service = _build_service()
        print("[sync_from_drive] Authenticated with Google Drive API.")
    except Exception as exc:  # noqa: BLE001
        print(f"[sync_from_drive] FATAL: Could not authenticate with Google Drive: {exc}")
        return 1

    overall_stats: dict = {"checked": 0, "downloaded": 0, "skipped": 0, "errors": 0}
    any_configured = False

    for env_var, rel_dir in FOLDER_MAP:
        folder_id = os.environ.get(env_var, "").strip()
        if not folder_id:
            print(f"[sync_from_drive] WARNING: {env_var} not set — skipping {rel_dir}/")
            continue

        any_configured = True
        local_dir = PROJECT_ROOT / rel_dir
        local_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n[sync_from_drive] Syncing {env_var} → {rel_dir}/")
        folder_stats: dict = {"checked": 0, "downloaded": 0, "skipped": 0, "errors": 0}

        try:
            _sync_folder(service, folder_id, local_dir, stats=folder_stats)
        except Exception as exc:  # noqa: BLE001
            print(f"  [sync] Unexpected error for {rel_dir}/: {exc}")
            folder_stats["errors"] += 1

        print(
            f"  → checked={folder_stats['checked']} "
            f"downloaded={folder_stats['downloaded']} "
            f"skipped={folder_stats['skipped']} "
            f"errors={folder_stats['errors']}"
        )

        for key in overall_stats:
            overall_stats[key] += folder_stats[key]

    if not any_configured:
        print(
            "\n[sync_from_drive] WARNING: No Drive folder env vars configured. "
            "Set DRIVE_FOLDER_STR, DRIVE_FOLDER_DATAFY, DRIVE_FOLDER_COSTAR, "
            "DRIVE_FOLDER_ZARTICO, DRIVE_FOLDER_VISIT_CA, and/or DRIVE_FOLDER_LATER "
            "to enable sync."
        )

    print(
        f"\n[sync_from_drive] Done — "
        f"total checked={overall_stats['checked']} "
        f"downloaded={overall_stats['downloaded']} "
        f"skipped={overall_stats['skipped']} "
        f"errors={overall_stats['errors']}"
    )

    if overall_stats["errors"] > 0:
        print("[sync_from_drive] Completed with errors — see output above for details.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
