"""
file_dates.py
-------------
Shared "date added" helpers for the VDP pipeline.

Why this exists
---------------
Datafy, CoStar and STR exports are downloaded with names like "TopMarkets_Export (6).csv"
or "daily_09_30_26.xlsx". A file name says very little about which export is the newest, and
name order is wrong as often as it is right ("marketAnalysis-..." sorts after "TopMarkets-...",
"(10)" sorts before "(2)", "Seg" sorts before "seg"). The pipeline therefore ranks files by the
DATE THEY WERE ADDED and uses the name only as the last tiebreaker.

Date added is the moment the file's current content landed:
  * Tracked and unchanged since its last commit  -> the date of the newest commit that added,
    modified, or renamed it. This is reliable on GitHub Actions and Railway, where a checkout
    stamps every file with the same time, and it correctly dates a file that was replaced in
    place under the same name (str_daily.xlsx, for example).
  * Untracked, or edited since the last commit    -> the file system modified time. This is
    reliable on a laptop, where the file has just been downloaded and not yet committed.
  * No git history available (a shallow clone)    -> the file system modified time.

All values are naive UTC datetimes so they compare across machines.

Functions
---------
  date_added(path, root)          -> (datetime, source)  source is "git", "mtime" or "unknown"
  copy_number(filename)           -> the N in "name (N).csv", else 0
  family_key(filename)            -> normalized report-type key (drops "(N)", dates, extension)
  sort_key(path, root)            -> (date added, copy number, lower-case name)
  sort_oldest_first(paths, root)  -> paths ordered oldest to newest by date added
  group_batches(paths, root, gap_hours=6) -> list of path lists, one per upload session
  newest(paths, root)             -> the single most recently added path (or None)
  local_date(path, root)          -> "YYYY-MM-DD" of the date added in Pacific time
"""

from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timedelta, timezone

_LANDED: dict[str, dict[str, datetime]] = {}
_DIRTY: dict[str, set[str]] = {}


def _naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _git(root: str, *args: str, timeout: int = 120) -> str:
    res = subprocess.run(
        ["git", "-C", root, "-c", "core.quotepath=off", *args],
        capture_output=True, text=True, timeout=timeout,
    )
    return res.stdout if res.returncode == 0 else ""


def landed_map(root: str) -> dict[str, datetime]:
    """Newest add/modify/rename commit date per tracked path under data/ (one git call, cached)."""
    root = os.path.abspath(root)
    if root in _LANDED:
        return _LANDED[root]
    out: dict[str, datetime] = {}
    try:
        if _git(root, "rev-parse", "--is-shallow-repository", timeout=30).strip() != "true":
            current = None
            # git log runs newest to oldest, so the first time a path appears is its newest change.
            for line in _git(root, "log", "--diff-filter=AMR", "-M", "--name-status",
                             "--format=@@%aI", "--", "data").splitlines():
                if line.startswith("@@"):
                    try:
                        current = _naive_utc(datetime.fromisoformat(line[2:]))
                    except ValueError:
                        current = None
                elif line.strip() and current is not None:
                    parts = line.split("\t")
                    path = parts[-1].strip()      # for renames the last column is the new path
                    out.setdefault(path, current)
    except Exception:
        out = {}
    _LANDED[root] = out
    return out


def dirty_set(root: str) -> set[str]:
    """Paths under data/ that are untracked or changed since the last commit (cached)."""
    root = os.path.abspath(root)
    if root in _DIRTY:
        return _DIRTY[root]
    out: set[str] = set()
    try:
        raw = _git(root, "status", "--porcelain", "-z", "--untracked-files=all", "--", "data", timeout=60)
        entries = raw.split("\0")
        i = 0
        while i < len(entries):
            e = entries[i]
            i += 1
            if len(e) < 4:
                continue
            code, path = e[:2], e[3:]
            if code[0] in "RC":          # the next entry is the original path
                i += 1
            out.add(path)
    except Exception:
        out = set()
    _DIRTY[root] = out
    return out


def date_added(path: str, root: str) -> tuple[datetime, str]:
    """(date added as naive UTC, source)."""
    root = os.path.abspath(root)
    rel = os.path.relpath(os.path.abspath(path), root).replace(os.sep, "/")
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc).replace(tzinfo=None)
    except OSError:
        mtime = None
    if rel not in dirty_set(root):
        g = landed_map(root).get(rel)
        if g:
            return g, "git"
    if mtime:
        return mtime, "mtime"
    return datetime(1970, 1, 1), "unknown"


_COPY = re.compile(r"\s*\((\d+)\)\s*$")


def copy_number(filename: str) -> int:
    stem = os.path.splitext(os.path.basename(filename))[0]
    m = _COPY.search(stem)
    return int(m.group(1)) if m else 0


_DATE_TOKENS = [
    re.compile(r"\d{4}-\d{2}-\d{2}"),
    re.compile(r"\d{2}-\d{2}-\d{4}"),
    re.compile(r"(?<!\d)\d{2}_\d{2}_\d{2}(?!\d)"),
    re.compile(r"(?<!\d)\d{6,8}(?!\d)"),
]


def family_key(filename: str) -> str:
    """Report type: the file name without extension, copy number and date tokens."""
    stem = os.path.splitext(os.path.basename(filename))[0]
    stem = _COPY.sub("", stem)
    for rx in _DATE_TOKENS:
        stem = rx.sub(" ", stem)
    stem = re.sub(r"\bto\b", " ", stem, flags=re.I)
    stem = re.sub(r"[_\-\s]+", " ", stem).strip().lower()
    return stem


def sort_key(path: str, root: str):
    d, _ = date_added(path, root)
    return (d, copy_number(path), os.path.basename(path).lower())


def sort_oldest_first(paths, root: str) -> list[str]:
    return sorted(paths, key=lambda p: sort_key(p, root))


def group_batches(paths, root: str, gap_hours: float = 6.0) -> list[list[str]]:
    """Cluster files into upload sessions: a gap of more than gap_hours starts a new batch."""
    ordered = sort_oldest_first(paths, root)
    batches: list[list[str]] = []
    last = None
    gap = timedelta(hours=gap_hours)
    for p in ordered:
        d, _ = date_added(p, root)
        if last is None or d - last > gap:
            batches.append([])
        batches[-1].append(p)
        last = d
    return batches


def newest(paths, root: str):
    """The most recently added path by date added (copy number, then name, break ties)."""
    paths = list(paths)
    return max(paths, key=lambda p: sort_key(p, root)) if paths else None


def local_date(path: str, root: str, tz: str = "America/Los_Angeles") -> str:
    """Date added as a calendar date in the organization's time zone (Pacific by default)."""
    added, _src = date_added(path, root)
    try:
        from zoneinfo import ZoneInfo
        return added.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(tz)).strftime("%Y-%m-%d")
    except Exception:
        return added.strftime("%Y-%m-%d")
