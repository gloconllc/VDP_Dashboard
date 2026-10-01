"""
weekly_data_check.py
--------------------
Thursday companion to the GitHub Action STR sync (str_weekly_sync.yml).

check (default)
    1. Confirms the Thursday GitHub Action pushed to main (commit message contains
       "STR sync"), and reports the latest commit.
    2. Lists raw source files in data/str, data/costar, and data/datafy that exist
       locally but are not on main yet (new CoStar or Datafy drops, or an STR file
       saved by hand). Compares git blob hashes against GitHub's public tree, so no
       git command is run locally (avoids the lock-file quirk in this repo).

load (staged, so each stage fits a short shell time limit)
    --load prepare      download main's live DB + copy scripts and raw data to a scratch dir
    --load list         print the loader steps
    --load step NAME    run one loader step against the scratch DB
    --load finalize     back up the local data/analytics.sqlite to _to_delete_local/
                        and put the merged DB (main + local drops) in its place

The merged DB is built FROM main's DB, so it already contains whatever the
Thursday Action pushed. When GitHub Desktop reports a conflict on
data/analytics.sqlite after a pull, keep the local version.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = "gloconllc/VDP_Dashboard"
API = f"https://api.github.com/repos/{REPO}"
RAW_DB = f"https://raw.githubusercontent.com/{REPO}/main/data/analytics.sqlite"
ROOT = Path(__file__).resolve().parent.parent
INPUT_DIRS = ["data/str", "data/costar", "data/datafy"]
SKIP_NAMES = {".DS_Store", "Thumbs.db"}
WORK = Path.home() / "vdp_load_work"

# Loader subset: everything that reads local raw files, then the derived steps.
# Network fetchers are left to the GitHub Action and the monthly pipeline.
STEPS = [
    "load_str_daily_sqlite", "load_str_monthly_sqlite", "load_str_multiseg",
    "load_str_response_sheets", "load_str_translation_table", "compute_kpis",
    "load_datafy_reports", "load_datafy_advertising", "load_datafy_extended",
    "load_costar_reports", "load_costar_market_daily", "load_costar_segmentation",
    "compute_insights", "optimize_db", "build_table_relationships",
]
REQUIRED = {"load_str_daily_sqlite", "load_str_monthly_sqlite", "compute_kpis"}


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "vdp-weekly-check"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def ignored(rel: str) -> bool:
    # mirrors .gitignore: top-level data/str/*.xls(x) are never committed
    p = Path(rel)
    return p.parent.as_posix() == "data/str" and p.suffix.lower() in (".xls", ".xlsx")


def check() -> dict:
    commits = get_json(f"{API}/commits?per_page=10")
    latest = commits[0]
    now = datetime.now(timezone.utc)
    str_sync = None
    for c in commits:
        if "str sync" in c["commit"]["message"].lower():
            str_sync = c
            break
    tree = get_json(f"{API}/git/trees/main?recursive=1")
    remote = {t["path"]: t["sha"] for t in tree.get("tree", []) if t["type"] == "blob"}
    new_local, changed_local = [], []
    for d in INPUT_DIRS:
        base = ROOT / d
        if not base.exists():
            continue
        for f in sorted(base.rglob("*")):
            if not f.is_file() or f.name in SKIP_NAMES or f.name.startswith("~$"):
                continue
            rel = f.relative_to(ROOT).as_posix()
            if ignored(rel):
                continue
            if rel not in remote:
                new_local.append(rel)
            elif blob_sha(f) != remote[rel]:
                changed_local.append(rel)
    sync_age = None
    if str_sync:
        sd = datetime.fromisoformat(str_sync["commit"]["author"]["date"].replace("Z", "+00:00"))
        sync_age = round((now - sd).total_seconds() / 3600, 1)
    return {
        "latest_commit": {"sha": latest["sha"][:7], "date": latest["commit"]["author"]["date"],
                          "message": latest["commit"]["message"].splitlines()[0]},
        "str_sync_commit": ({"sha": str_sync["sha"][:7], "date": str_sync["commit"]["author"]["date"],
                             "hours_ago": sync_age} if str_sync else None),
        "str_sync_ran_this_week": bool(sync_age is not None and sync_age <= 48),
        "tree_truncated": tree.get("truncated", False),
        "new_local_files": new_local,
        "changed_local_files": changed_local,
    }


def prepare():
    if WORK.exists():
        shutil.rmtree(WORK)
    (WORK / "data").mkdir(parents=True)
    (WORK / "logs").mkdir()
    req = urllib.request.Request(RAW_DB, headers={"User-Agent": "vdp-weekly-check"})
    with urllib.request.urlopen(req, timeout=150) as r, open(WORK / "data" / "analytics.sqlite", "wb") as fh:
        shutil.copyfileobj(r, fh)
    shutil.copytree(ROOT / "scripts", WORK / "scripts", ignore=shutil.ignore_patterns("__pycache__", "*.bak"))
    for d in INPUT_DIRS:
        if (ROOT / d).exists():
            shutil.copytree(ROOT / d, WORK / d)
    if (ROOT / ".env").exists():
        shutil.copy(ROOT / ".env", WORK / ".env")
    print(json.dumps({"prepared": str(WORK), "steps": STEPS}))


def run_step(name: str):
    script = WORK / "scripts" / f"{name}.py"
    if not script.exists():
        print(json.dumps({"step": name, "status": "missing"}))
        return 0
    p = subprocess.run([sys.executable, str(script)], cwd=WORK, capture_output=True, text=True, timeout=170)
    status = "ok" if p.returncode == 0 else "failed"
    print((p.stdout + p.stderr)[-1500:])
    print(json.dumps({"step": name, "status": status, "required": name in REQUIRED}))
    return 1 if (status == "failed" and name in REQUIRED) else 0


def finalize():
    merged = WORK / "data" / "analytics.sqlite"
    if not merged.exists():
        print(json.dumps({"finalize": "no merged DB, run prepare first"}))
        return 1
    target = ROOT / "data" / "analytics.sqlite"
    backup_dir = ROOT / "_to_delete_local"
    backup_dir.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if target.exists():
        shutil.move(str(target), str(backup_dir / f"analytics.sqlite.before_weekly_{stamp}"))
    shutil.copy(merged, target)
    print(json.dumps({"finalize": "ok", "db": str(target), "backup": f"_to_delete_local/analytics.sqlite.before_weekly_{stamp}"}))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--load", nargs="*", help="prepare | list | step NAME | finalize")
    a = ap.parse_args()
    if not a.load:
        print("RESULT " + json.dumps(check()))
        return 0
    cmd = a.load[0]
    if cmd == "prepare":
        prepare()
        return 0
    if cmd == "list":
        print(json.dumps(STEPS))
        return 0
    if cmd == "step" and len(a.load) > 1:
        return run_step(a.load[1])
    if cmd == "finalize":
        return finalize()
    ap.error("unknown --load command")
    return 2


if __name__ == "__main__":
    sys.exit(main())
