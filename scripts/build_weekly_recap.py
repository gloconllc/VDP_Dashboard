"""
build_weekly_recap.py
---------------------
Builds the Friday Dana Point PULSE recap for Heather (GloCon Solutions, LLC).

Produces, in recaps/<YYYY-MM-DD>/ (gitignored, never committed to the public repo):
    Dana_Point_PULSE_Weekly_<date>.pdf   the app's weekly report PDF
    email.txt                            plain-text email, filled from the template
    Dana_Point_PULSE_Weekly_<date>.eml   ready-to-send draft with the PDF attached
    summary.md                           internal notes for John: what changed, warnings

Design choices
    * Always reads the LIVE database from GitHub main (exactly what the app serves),
      never the local working copy, so the recap matches what Heather sees.
    * Builds the PDF in a scratch workspace with scripts/generate_weekly_report.py,
      so nothing in the repo is modified.
    * Tracks every source's freshness marker in recaps/state.json and reports which
      sources moved since the last recap. Monthly sources that have not moved are
      reported as "unchanged, monthly cadence", and the email mentions them only
      when they do move.
    * Freshness follows the project rule: measure pull recency, never coverage
      date against the calendar (see check_costar_freshness.py).

Usage
    python3 scripts/build_weekly_recap.py              # build + update state.json
    python3 scripts/build_weekly_recap.py --dry-run    # build, leave state.json alone
    python3 scripts/build_weekly_recap.py --db PATH    # use a specific DB file
Needs: pandas, matplotlib, weasyprint (pip install weasyprint).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

REPO = "gloconllc/VDP_Dashboard"
RAW_DB = f"https://raw.githubusercontent.com/{REPO}/main/data/analytics.sqlite"
COMMITS_API = f"https://api.github.com/repos/{REPO}/commits?per_page=1"
APP_URL = "https://vdppulse.gloconsolutions.com/"

ROOT = Path(__file__).resolve().parent.parent
RECAPS = ROOT / "recaps"
TEMPLATE = RECAPS / "templates" / "heather_weekly_recap.txt"
CONFIG = RECAPS / "recap_config.json"
STATE = RECAPS / "state.json"

# marker -> (plain-language label, cadence)
SOURCES = {
    "costar_daily_end": ("Weekly hotel performance (CoStar daily)", "weekly"),
    "costar_monthly_period": ("Monthly hotel performance (CoStar monthly)", "monthly"),
    "costar_participation": ("CoStar participating hotel list", "monthly"),
    "str_daily_end": ("STR weekly file (Dropbox)", "weekly"),
    "str_monthly_end": ("STR monthly file (Dropbox)", "monthly"),
    "datafy_files": ("Datafy visitor and spending data", "monthly"),
}


# --------------------------------------------------------------------------- io
def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def download(url: str, dest: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "vdp-weekly-recap"})
    with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as fh:
        shutil.copyfileobj(r, fh)


def latest_commit() -> dict:
    try:
        req = urllib.request.Request(COMMITS_API, headers={"User-Agent": "vdp-weekly-recap"})
        with urllib.request.urlopen(req, timeout=30) as r:
            c = json.load(r)[0]
        return {"sha": c["sha"][:7], "date": c["commit"]["author"]["date"],
                "message": c["commit"]["message"].splitlines()[0]}
    except Exception as exc:  # noqa: BLE001
        return {"sha": "unknown", "date": "", "message": f"lookup failed: {exc}"}


# --------------------------------------------------------------------- markers
def _one(con, sql):
    try:
        row = con.execute(sql).fetchone()
        return row[0] if row else None
    except sqlite3.Error:
        return None


def markers(con) -> dict:
    m = {
        "costar_daily_end": _one(con, "SELECT MAX(as_of_date) FROM costar_market_daily"),
        "costar_monthly_period": _one(con, "SELECT MAX(report_period) FROM costar_market_monthly"),
        "costar_participation": _one(con, "SELECT MAX(snapshot_date) FROM costar_participation"),
        "str_daily_end": _one(con, "SELECT MAX(as_of_date) FROM fact_str_metrics WHERE grain='daily'"),
        "str_monthly_end": _one(con, "SELECT MAX(as_of_date) FROM fact_str_metrics WHERE grain='monthly'"),
    }
    # Datafy: a new export is a new file name or a file with a new date added. load_log keeps only
    # glob patterns for Datafy (data/datafy/*/*.csv), so it cannot see a new upload; the per-file
    # ingest log written by load_datafy_reports.py can. Falls back to load_log if that table is absent.
    try:
        rows = sorted(f"{fn}|{added}" for fn, added in con.execute(
            "SELECT file_name, date_added FROM datafy_file_ingest WHERE status = 'ok'"))
        if rows:
            m["datafy_files"] = hashlib.sha1("|".join(rows).encode()).hexdigest()[:10]
            m["datafy_file_count"] = len(rows)
        else:
            raise sqlite3.Error("no ingest rows")
    except sqlite3.Error:
        try:
            names = sorted({r[0] for r in con.execute(
                "SELECT DISTINCT file_name FROM load_log WHERE lower(source)='datafy'")})
            m["datafy_files"] = hashlib.sha1("|".join(names).encode()).hexdigest()[:10] if names else None
            m["datafy_file_count"] = len(names)
        except sqlite3.Error:
            m["datafy_files"] = None
    # CoStar pull date lives in the export filenames (daily_09_30_26.xlsx), per project rule.
    pulls = []
    try:
        for (fn,) in con.execute("SELECT file_name FROM load_log WHERE source='CoStar'"):
            mm = re.search(r"(\d{2})_(\d{2})_(\d{2})", fn or "")
            if mm:
                try:
                    pulls.append(date(2000 + int(mm.group(3)), int(mm.group(1)), int(mm.group(2))))
                except ValueError:
                    pass
    except sqlite3.Error:
        pass
    m["costar_last_pull"] = max(pulls).isoformat() if pulls else None
    return m


# ------------------------------------------------------------------------ kpis
def week_kpis(con) -> dict:
    end = _one(con, "SELECT MAX(as_of_date) FROM costar_market_daily")
    if not end:
        return {}
    rows = con.execute(
        """SELECT supply, demand, revenue_usd, supply_yoy_pct, demand_yoy_pct, revenue_yoy_pct
           FROM costar_market_daily WHERE as_of_date > date(?, '-7 day') AND as_of_date <= ?""",
        (end, end)).fetchall()
    s = d = r = s_ly = d_ly = r_ly = 0.0
    for sup, dem, rev, sy, dy, ry in rows:
        s, d, r = s + (sup or 0), d + (dem or 0), r + (rev or 0)
        s_ly += (sup or 0) / (1 + (sy or 0) / 100)
        d_ly += (dem or 0) / (1 + (dy or 0) / 100)
        r_ly += (rev or 0) / (1 + (ry or 0) / 100)
    if not s or not d or not s_ly or not d_ly:
        return {}
    occ, adr, revpar = d / s * 100, r / d, r / s
    occ_ly, adr_ly, revpar_ly = d_ly / s_ly * 100, r_ly / d_ly, r_ly / s_ly
    end_d = datetime.strptime(end, "%Y-%m-%d").date()
    return {
        "days": len(rows), "week_start": (end_d - timedelta(days=6)), "week_end": end_d,
        "occ": occ, "occ_chg": occ - occ_ly, "adr": adr, "adr_chg": (adr / adr_ly - 1) * 100,
        "revpar": revpar, "revpar_chg": (revpar / revpar_ly - 1) * 100,
        "revenue": r, "revenue_chg": (r / r_ly - 1) * 100,
        "demand": d, "demand_chg": (d / d_ly - 1) * 100,
    }


def month_kpis(con) -> dict:
    row = con.execute(
        """SELECT report_period, occupancy_pct, occupancy_yoy_pp, adr_usd, adr_yoy_pct,
                  revpar_usd, revpar_yoy_pct, revenue_usd, revenue_yoy_pct
           FROM costar_market_monthly ORDER BY report_period DESC LIMIT 1""").fetchone()
    if not row:
        return {}
    keys = ["period", "occ", "occ_chg", "adr", "adr_chg", "revpar", "revpar_chg", "revenue", "revenue_chg"]
    out = dict(zip(keys, row))
    try:
        out["label"] = datetime.strptime(out["period"], "%Y-%m").strftime("%B %Y")
    except (TypeError, ValueError):
        out["label"] = str(out["period"])
    return out


# ---------------------------------------------------------------- formatting
def pct(v, pts=False):
    if v is None:
        return "n/a"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f} pts" if pts else f"{sign}{v:.1f}%"


def money(v, cents=True):
    if v is None:
        return "n/a"
    return f"${v:,.2f}" if cents else f"${v:,.0f}"


def long_date(d: date) -> str:
    return f"{d.strftime('%B')} {d.day}, {d.year}"


def join_list(items):
    items = list(items)
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


# ------------------------------------------------------------------------ main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", help="use this DB instead of downloading main's")
    ap.add_argument("--dry-run", action="store_true", help="do not update recaps/state.json")
    ap.add_argument("--work", default=str(Path.home() / "vdp_recap_work"))
    args = ap.parse_args()

    today = date.today()
    out_dir = RECAPS / today.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    work = Path(args.work)
    if work.exists():
        shutil.rmtree(work)
    (work / "data").mkdir(parents=True)
    (work / "logs").mkdir()
    (work / "dashboard").mkdir()

    # 1. Live database from main
    db = work / "data" / "analytics.sqlite"
    if args.db:
        shutil.copy(args.db, db)
        db_source = f"local file {args.db}"
    else:
        log("Downloading live analytics.sqlite from GitHub main")
        download(RAW_DB, db)
        db_source = "GitHub main (live app database)"
    commit = latest_commit()

    # 2. PDF via the app's own generator, in a scratch copy
    shutil.copytree(ROOT / "scripts", work / "scripts", ignore=shutil.ignore_patterns("__pycache__", "*.bak"))
    shutil.copy(ROOT / "dashboard" / "report_template.html", work / "dashboard" / "report_template.html")
    shutil.copytree(ROOT / "dashboard" / "assets", work / "dashboard" / "assets")
    log("Generating PDF")
    proc = subprocess.run([sys.executable, str(work / "scripts" / "generate_weekly_report.py")],
                          cwd=work, capture_output=True, text=True, timeout=900)
    pdf_src = work / "logs" / "weekly_report_latest.pdf"
    if proc.returncode != 0 or not pdf_src.exists():
        print(proc.stdout[-2000:], proc.stderr[-2000:])
        log("ERROR: PDF generation failed")
        return 2

    # 3. Metrics and change detection
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    cur = markers(con)
    wk, mo = week_kpis(con), month_kpis(con)
    con.close()
    if not wk:
        log("ERROR: no CoStar daily rows; cannot build weekly KPIs")
        return 3

    prev = json.loads(STATE.read_text()) if STATE.exists() else {}
    prev_m = prev.get("markers", {})
    first_run = not prev_m
    moved, unchanged = [], []
    for key, (label, cadence) in SOURCES.items():
        if cur.get(key) is None:
            continue
        if first_run or cur.get(key) != prev_m.get(key):
            moved.append((key, label, cadence))
        else:
            unchanged.append((key, label, cadence))

    warnings = []
    pull = cur.get("costar_last_pull")
    if pull:
        age = (today - date.fromisoformat(pull)).days
        if age > 9:
            warnings.append(f"Last CoStar pull was {pull} ({age} days ago). Weekly data may not have refreshed.")
    if not first_run and cur.get("costar_daily_end") == prev_m.get("costar_daily_end"):
        warnings.append("CoStar daily coverage did not advance since the last recap. Review before sending.")
    if cur.get("str_daily_end") and cur.get("costar_daily_end") and cur["str_daily_end"] < cur["costar_daily_end"]:
        warnings.append(f"STR Dropbox daily data ends {cur['str_daily_end']}, behind CoStar daily "
                        f"({cur['costar_daily_end']}). Weekly KPIs use CoStar.")
    if commit.get("date"):
        try:
            cd = datetime.fromisoformat(commit["date"].replace("Z", "+00:00")).date()
            if (today - cd).days > 8:
                warnings.append(f"No commit to main in {(today - cd).days} days. The Thursday sync may not have run.")
        except ValueError:
            pass

    # 4. Email
    cfg = json.loads(CONFIG.read_text()) if CONFIG.exists() else {}
    month_moved = any(k == "costar_monthly_period" for k, _, _ in moved)
    if mo:
        month_line = (f"{mo['label']} (latest full month{', new this week' if month_moved and not first_run else ''}): "
                      f"occupancy {mo['occ']:.1f}% ({pct(mo['occ_chg'], True)}), ADR {money(mo['adr'])} "
                      f"({pct(mo['adr_chg'])}), RevPAR {money(mo['revpar'])} ({pct(mo['revpar_chg'])}).")
    else:
        month_line = ""
    moved_labels = [l for _, l, _ in moved]
    monthly_unchanged = [l for _, l, c in unchanged if c == "monthly"]
    # First run only sets the baseline, so the email makes no "updated" claims yet.
    updated_line = (f"Updated this week: {join_list(moved_labels)}."
                    if moved_labels and not first_run else "")
    unchanged_line = (f"No change this week to {join_list(monthly_unchanged)}; these refresh monthly."
                      if monthly_unchanged else "")

    fields = defaultdict(str, {
        "first_name": cfg.get("to_first_name", "Heather"),
        "week_start": (long_date(wk["week_start"]) if wk["week_start"].year != wk["week_end"].year
                       else f"{wk['week_start'].strftime('%B')} {wk['week_start'].day}"),
        "week_end": long_date(wk["week_end"]),
        "occ": f"{wk['occ']:.1f}%", "occ_chg": pct(wk["occ_chg"], True),
        "adr": money(wk["adr"]), "adr_chg": pct(wk["adr_chg"]),
        "revpar": money(wk["revpar"]), "revpar_chg": pct(wk["revpar_chg"]),
        "revenue": money(wk["revenue"], cents=False), "revenue_chg": pct(wk["revenue_chg"]),
        "month_line": month_line, "updated_line": updated_line, "unchanged_line": unchanged_line,
        "app_url": cfg.get("app_url", APP_URL), "signature": cfg.get("signature", "John\nGloCon Solutions, LLC"),
    })
    tmpl = TEMPLATE.read_text()
    subject_line, body_t = tmpl.split("\n", 1)
    subject = subject_line.replace("Subject:", "").strip().format_map(fields)
    body = body_t.lstrip("\n").format_map(fields)
    body = "\n".join(l.strip() if not l.strip() else l.rstrip() for l in body.splitlines())
    body = re.sub(r"\n{3,}", "\n\n", body).strip() + "\n"   # collapse empty optional lines

    stamp = wk["week_end"].isoformat()
    pdf_name = f"Dana_Point_PULSE_Weekly_{stamp}.pdf"
    pdf_out = out_dir / pdf_name
    shutil.copy(pdf_src, pdf_out)
    (out_dir / "email.txt").write_text(f"Subject: {subject}\n\n{body}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.get("from", "")
    msg["To"] = cfg.get("to", "")
    if cfg.get("cc"):
        msg["Cc"] = cfg["cc"]
    msg["X-Unsent"] = "1"   # Outlook and Apple Mail open this as an editable draft
    msg.set_content(body)
    msg.add_attachment(pdf_out.read_bytes(), maintype="application", subtype="pdf", filename=pdf_name)
    eml = out_dir / f"Dana_Point_PULSE_Weekly_{stamp}.eml"
    eml.write_bytes(bytes(msg))

    # 5. Internal summary for John
    lines = [f"# Dana Point PULSE weekly recap, built {today.isoformat()}", "",
             f"- Database: {db_source}",
             f"- Latest commit on main: {commit['sha']} {commit['date']} \"{commit['message']}\"",
             f"- Week: {long_date(wk['week_start'])} to {long_date(wk['week_end'])} ({wk['days']} days of CoStar daily)",
             f"- Occupancy {fields['occ']} ({fields['occ_chg']}), ADR {fields['adr']} ({fields['adr_chg']}), "
             f"RevPAR {fields['revpar']} ({fields['revpar_chg']}), revenue {fields['revenue']} ({fields['revenue_chg']})",
             f"- {month_line}" if month_line else "", "", "## Sources", ""]
    for key, (label, cadence) in SOURCES.items():
        status = "moved" if any(k == key for k, _, _ in moved) else "unchanged"
        lines.append(f"- {label} [{cadence}]: {cur.get(key)} (previous {prev_m.get(key, 'none')}), {status}")
    lines += [f"- CoStar last pull: {cur.get('costar_last_pull')}", "", "## Warnings", ""]
    lines += [f"- {w}" for w in warnings] or ["- None"]
    lines += ["", "## Files", "", f"- {pdf_out.name}", f"- {eml.name}", "- email.txt"]
    (out_dir / "summary.md").write_text("\n".join(l for l in lines if l is not None) + "\n")

    if not args.dry_run:
        STATE.write_text(json.dumps({"built": today.isoformat(), "week_end": stamp,
                                     "markers": cur}, indent=2, default=str))
    result = {"status": "ok", "out_dir": str(out_dir), "pdf": pdf_out.name, "eml": eml.name,
              "subject": subject, "moved": moved_labels, "warnings": warnings,
              "missing_config": [k for k in ("from", "to") if not cfg.get(k)]}
    print("RESULT " + json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
