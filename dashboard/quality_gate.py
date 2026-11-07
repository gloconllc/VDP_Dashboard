# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created by John Picou, GloCon Solutions LLC, for Visit Dana Point.
#  Use, copying, sharing, or redistribution of this code or the PULSE report
#  requires written permission. Contact GloCon Solutions LLC (John Picou),
#  https://gloconsolutions.com. See NOTICE.md in the repository root.
# =============================================================================
"""
quality_gate.py, the PULSE publish quality gate.

The rule (John, 2026-11-07): a value that is missing, unavailable, or zero
where zero is impossible is EXCLUDED, never shown and never counted.

  * Never show "0%" and "0%", "$0", "n/a", "N/A", "not available", or a blank
    stat. If a figure is not there, the card, sentence, row, or chart point
    that would carry it is left out entirely, so nobody assumes it exists.
  * Never feed a placeholder zero into a formula. A zero-filled month or share
    is stored as NULL, so AVG/SUM/YoY math skips it instead of being dragged
    toward zero (a zero is a value; a missing number is not).

Three layers use this module:
  1. normalize_missing(conn): pipeline step (scripts/qa_normalize_missing.py)
     turns zero placeholders into NULL in measures where zero is impossible.
  2. prune_report(report): pulse_insights.build_report() runs every report
     through it, so the app, the strips, and every export get clean content.
  3. scan(obj): scripts/qa_publish_check.py rebuilds the report from the
     database and fails the pipeline (blocks publish) if anything slipped by.
"""
from __future__ import annotations

import math
import re

# ---------------------------------------------------------------------------
# 1. Measures where zero means "missing", never a real value
# ---------------------------------------------------------------------------
# (table, column, optional extra WHERE). Only measures that cannot truly be
# zero for a destination: a month with $0 visitor spending, a 0% day-trip
# share, or a hotel night at $0 rate is a placeholder in the export, not data.
# Counts that CAN be zero (compression days, flags, per-geo new users,
# segment rows) are deliberately not listed.
ZERO_IS_MISSING: list[tuple[str, str, str]] = [
    ("fact_str_metrics", "metric_value", "metric_name IN ('supply','demand','revenue','occ','adr','revpar')"),
    ("kpi_daily_summary", "occ_pct", ""),
    ("kpi_daily_summary", "adr", ""),
    ("kpi_daily_summary", "revpar", ""),
    ("datafy_overview_spending_by_month", "spending_usd", ""),
    ("datafy_overview_visitation_by_month", "visitor_days", ""),
    ("datafy_overview_los_distribution", "pct_of_trips", ""),
    ("datafy_overview_local_visitor_spend", "local_pct", ""),
    ("datafy_overview_local_visitor_spend", "visitor_pct", ""),
    ("datafy_overview_state_origin", "share_of_trips_pct", ""),
    ("datafy_overview_dma", "visitor_days_share_pct", ""),
    ("datafy_overview_dma", "spending_share_pct", ""),
    ("datafy_overview_category_spending", "spend_share_pct", ""),
    ("datafy_overview_kpis", "total_trips", ""),
    ("datafy_overview_kpis", "avg_length_of_stay_days", ""),
    ("datafy_overview_kpis", "day_trips_pct", ""),
    ("datafy_overview_kpis", "overnight_trips_pct", ""),
    ("datafy_overview_total_kpis", "total_trips", ""),
    ("datafy_overview_total_kpis", "avg_los_days", ""),
    ("datafy_attribution_media_kpis", "total_investment_usd", ""),
    ("datafy_trip_share_by_dma", "share_of_trips_pct", ""),
    ("costar_market_daily", "occupancy_pct", ""),
    ("costar_market_daily", "adr", ""),
    ("costar_market_daily", "revpar", ""),
    ("costar_market_daily", "supply", ""),
    ("costar_market_daily", "demand", ""),
    ("costar_market_daily", "revenue", ""),
]


def _columns(conn, table: str) -> set[str]:
    try:
        return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}
    except Exception:  # noqa: BLE001
        return set()


def normalize_missing(conn, dry_run: bool = False) -> list[dict]:
    """Set zero placeholders to NULL in ZERO_IS_MISSING measures. Returns one
    row per (table, column) with the number of values excluded. Safe to run
    on every pipeline pass: loaders re-insert zeros, this removes them again."""
    out = []
    for table, col, where in ZERO_IS_MISSING:
        if col not in _columns(conn, table):
            continue
        cond = f'"{col}" = 0' + (f" AND ({where})" if where else "")
        n = conn.execute(f'SELECT COUNT(*) FROM "{table}" WHERE {cond}').fetchone()[0]
        if n and not dry_run:
            conn.execute(f'UPDATE "{table}" SET "{col}" = NULL WHERE {cond}')
        if n:
            out.append({"table": table, "column": col, "zeros_excluded": int(n)})
    if not dry_run:
        conn.commit()
    return out


# ---------------------------------------------------------------------------
# 2. Value tests
# ---------------------------------------------------------------------------
_MISSING_WORD = r"(?:\bn/a\b|\bN/A\b|\bnan\b|\bNaN\b|not available|unavailable|no data available)"
MISSING_RE = re.compile(_MISSING_WORD, re.IGNORECASE)
# An unsigned zero shown as a value: 0%, 0.0%, $0, $0.00. A signed change such
# as +0.0% is a real comparison of two real numbers and is kept.
# "$0-$50K" (an income bucket) and "$0.5M" are labels and amounts, not zeros.
ZERO_RE = re.compile(r"(?<![\w.,+\-−$])(?:\$0(?:\.0+)?(?![\d,]*\.?\d|\s*[-–—]|[KMBkmb])|0(?:\.0+)?%(?!\s*[-–—]\s*\d))(?!\d)")
_BLANK_VALUES = {"", "n/a", "na", "nan", "none", "null", "-", "—", "–", "--"}
_ZERO_VALUE_RE = re.compile(r"^\s*\$?0(?:\.0+)?\s*(?:%|days?|nights?|pts|points|trips|visitors)?\s*$", re.IGNORECASE)


def is_missing(v) -> bool:
    """True for None, NaN, zero, blank, or any "not available" placeholder."""
    if v is None:
        return True
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return (isinstance(v, float) and math.isnan(v)) or v == 0
    if isinstance(v, str):
        s = v.strip()
        return s.lower() in _BLANK_VALUES or bool(_ZERO_VALUE_RE.match(s)) or bool(MISSING_RE.fullmatch(s))
    return False


def ok(v) -> bool:
    return not is_missing(v)


def day_overnight_split(losd) -> tuple[float | None, float | None]:
    """Day-trip and overnight shares from datafy_overview_los_distribution.

    Datafy has shipped two label sets: "Day Trips"/"Overnight Trips" and the
    newer length-of-stay buckets "1 Day", "2 Days" ... "6+ Days". In the
    bucket version a 1-day trip is a day trip and 2+ days are overnight. If
    neither set is complete, both come back None and the figure is left out
    (never 0% and 0%)."""
    if losd is None or getattr(losd, "empty", True):
        return None, None
    labels = losd["length_of_stay"].astype(str)
    vals = losd["pct_of_trips"]
    day_m = labels.str.contains("day trip", case=False)
    ovn_m = labels.str.contains("overnight", case=False)
    day = float(vals[day_m].sum()) if day_m.any() else None
    ovn = float(vals[ovn_m].sum()) if ovn_m.any() else None
    if day is None and ovn is None:
        one = labels.str.fullmatch(r"\s*1\s*day\s*", case=False)
        multi = labels.str.contains(r"^\s*(?:[2-9]|\d{2,})\+?\s*days?", case=False, regex=True)
        if one.any() and multi.any():
            day = float(vals[one].sum())
            ovn = float(vals[multi].sum())
    if not ok(day) or not ok(ovn) or not (95 <= (day or 0) + (ovn or 0) <= 105):
        return None, None
    return day, ovn


# ---------------------------------------------------------------------------
# 3. Text cleanup: drop the sentence or clause that carries a missing value
# ---------------------------------------------------------------------------
_ABBR = re.compile(r"(?:\b(?:Jan|Feb|Mar|Apr|Aug|Sept|Sep|Oct|Nov|Dec|vs|St|No|Inc|approx|est|Mr|Ms|Dr)\.|\b[A-Z]\.|\bU\.S\.|\be\.g\.|\bi\.e\.)$")


def _segments(text: str) -> list[str]:
    """Split into sentences and semicolon clauses, keeping each separator on
    its segment, without splitting after "Oct." or "U.S."."""
    out, start = [], 0
    for m in re.finditer(r"[.;!?]\s+", text):
        piece = text[start:m.end()]
        if m.group(0)[0] == "." and _ABBR.search(text[start:m.start() + 1]):
            continue
        out.append(piece)
        start = m.end()
    if start < len(text):
        out.append(text[start:])
    return out


def has_issue(text: str) -> bool:
    return bool(MISSING_RE.search(text) or ZERO_RE.search(text))


def strip_missing(text: str) -> str:
    if not isinstance(text, str) or not has_issue(text):
        return text
    keep = [s for s in _segments(text) if not has_issue(s)]
    out = "".join(keep).strip()
    if out.endswith(";"):
        out = out[:-1].rstrip() + "."
    # A clause that followed a removed one now starts a sentence.
    out = re.sub(r"([.!?]\s+)([a-z])", lambda m: m.group(1) + m.group(2).upper(), out)
    if out[:1].islower() and text[:1].isupper():
        out = out[0].upper() + out[1:]
    return out


def _clean_src(text: str) -> str:
    return re.sub(r"\s*[,;:(]?\s*" + _MISSING_WORD + r"\)?", "", text, flags=re.IGNORECASE).strip(" ,;:")


# ---------------------------------------------------------------------------
# 4. Report pruning
# ---------------------------------------------------------------------------
_DROP = object()
_SKIP_KEYS = {"id", "url", "type", "fmt", "anchor", "cls", "color", "icon", "go", "code"}
_SRC_KEYS = {"src", "lede_src", "byline_src", "sub", "period", "period_detail", "cap"}
# Lists whose items each carry one value "v"; an item with no real value is removed.
_VALUE_LISTS = {"numbers", "stats", "takeaways", "parts", "items", "stages", "steps", "growth", "rows", "data", "points"}
_COMPOSITION = {"waffle", "share"}


def _prune(obj, key: str | None, log: list):
    if isinstance(obj, dict):
        t = obj.get("type")
        # Compositions (shares that add to 100) are all or nothing.
        if t in _COMPOSITION and isinstance(obj.get("parts"), list):
            if not obj["parts"] or any(is_missing(p.get("v")) for p in obj["parts"] if isinstance(p, dict)):
                log.append(f"dropped {t} infographic '{obj.get('title', '')}' (a share was missing)")
                return _DROP
        if t == "versus" and (is_missing((obj.get("a") or {}).get("v")) or is_missing((obj.get("b") or {}).get("v"))):
            log.append(f"dropped versus infographic '{obj.get('title', '')}'")
            return _DROP
        # A stat card or value item with no real value.
        if key == "qa" and "v" in obj and is_missing(obj.get("v")):
            obj = {**obj, "v": "", "l": ""}  # keep the question and answer, hide the empty key number
        elif "v" in obj and ("l" in obj or "k" in obj) and key not in ("key", "qa") and is_missing(obj.get("v")):
            log.append(f"dropped value '{obj.get('k') or obj.get('l') or ''}' ({obj.get('v')!r})")
            return _DROP
        # A who/what/when card with no figure: blanked (JS and exports skip it).
        if {"fig", "fig_l", "text"} <= set(obj) and is_missing(obj.get("fig")):
            log.append(f"blanked detail card '{obj.get('fig_l', '')}'")
            return {"fig": "", "fig_l": "", "text": "", "src": ""}
        # A compare/advice row whose finding needs a missing value.
        if "point" in obj and isinstance(obj.get("point"), str) and has_issue(obj["point"]):
            log.append("dropped compare row with a missing value")
            return _DROP
        out = {}
        for k, v in obj.items():
            if k in _SKIP_KEYS:
                out[k] = v
                continue
            if k == "key" and isinstance(v, dict):
                out[k] = v if ok(v.get("v")) else {"v": "", "l": ""}
                continue
            if k == "calc" and isinstance(v, list):
                rows = [r for r in v if not (isinstance(r, (list, tuple)) and any(isinstance(c, str) and has_issue(c) for c in r))]
                if len(rows) != len(v):
                    log.append(f"dropped {len(v) - len(rows)} calculation row(s) with a missing input")
                out[k] = rows
                continue
            res = _prune(v, k, log)
            if res is _DROP:
                continue
            out[k] = res
        # Tables: blank missing cells, drop rows with no real values, drop empty tables.
        if isinstance(out.get("rows"), list) and out["rows"] and isinstance(out["rows"][0], (list, tuple)):
            rows = []
            for r in out["rows"]:
                cells = ["" if (isinstance(c, str) and is_missing(c)) or (isinstance(c, float) and math.isnan(c)) or c is None else c for c in r]
                if any(str(c).strip() for c in cells[1:]) or len(cells) == 1:
                    rows.append(cells)
            if len(rows) != len(out["rows"]):
                log.append(f"dropped {len(out['rows']) - len(rows)} empty table row(s) in '{out.get('title', '')}'")
            out["rows"] = rows
            if not rows and "cols" in out:
                log.append(f"dropped empty table '{out.get('title', '')}'")
                return _DROP
        # A glance block whose value list emptied out.
        if t and any(k in out and isinstance(out[k], list) and not out[k] for k in ("parts", "items", "stages", "steps")):
            log.append(f"dropped empty infographic '{out.get('title', '')}'")
            return _DROP
        if "headline" in out and isinstance(out["headline"], str) and not out["headline"].strip():
            out["headline"] = out.get("question") or out.get("tab") or ""
        return out
    if isinstance(obj, list):
        res = []
        for it in obj:
            r = _prune(it, key, log)
            if r is _DROP:
                continue
            if key in _VALUE_LISTS and isinstance(r, dict) and "v" in r and is_missing(r.get("v")):
                log.append(f"dropped {key} item '{r.get('l', '')}'")
                continue
            if isinstance(r, str) and key not in _SKIP_KEYS and not r.strip() and isinstance(it, str) and it.strip():
                continue  # a note or paragraph that was only a missing value
            res.append(r)
        return res
    if isinstance(obj, str):
        if key in _SRC_KEYS:
            return _clean_src(obj) if MISSING_RE.search(obj) else obj
        new = strip_missing(obj)
        if new != obj:
            log.append(f"removed text with a missing value ({key}): {obj[:70]!r}")
        return new
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return obj


def prune_report(report: dict) -> dict:
    """Return a copy of a pulse_insights report with every missing, zero, or
    "not available" value removed. The log is kept under report['_qa']."""
    if not isinstance(report, dict):
        return report
    log: list[str] = []
    out = _prune(report, None, log)
    if isinstance(out, dict):
        for s in out.get("sections", []) or []:
            if isinstance(s, dict) and isinstance(s.get("details"), dict):
                for k, d in list(s["details"].items()):
                    if not isinstance(d, dict):
                        s["details"][k] = {"fig": "", "fig_l": "", "text": "", "src": ""}
        out["_qa"] = log
    return out


def scan(obj, path: str = "") -> list[str]:
    """Residual issues after pruning: any missing/zero placeholder still in a
    string, or a value item still holding a missing value. Empty means pass."""
    found: list[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in _SKIP_KEYS or k == "_qa":
                continue
            p = f"{path}.{k}" if path else k
            if k == "v" and ("l" in obj or "k" in obj) and v != "" and is_missing(v):
                found.append(f"{p}: missing value {v!r}")
            found.extend(scan(v, p))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            found.extend(scan(v, f"{path}[{i}]"))
    elif isinstance(obj, str):
        m = MISSING_RE.search(obj) or ZERO_RE.search(obj)
        if m:
            found.append(f"{path}: '{m.group(0)}' in {obj[:90]!r}")
    return found


# ---------------------------------------------------------------------------
# 5. HTML pruning (weekly PDF report and any other templated page)
# ---------------------------------------------------------------------------
def prune_html(html_text: str) -> tuple[str, list[str]]:
    """Remove every stat, chart, cell value, or sentence in a templated page
    that would show a missing, zero, or "N/A" value. A stat (value plus its
    label) is removed as a pair; a table cell is blanked; a chart image with
    no data is removed with its frame; sentences lose the missing clause."""
    log: list[str] = []
    try:
        from bs4 import BeautifulSoup, NavigableString
    except Exception:  # noqa: BLE001
        return html_text, ["bs4 not installed; HTML pruning skipped"]
    soup = BeautifulSoup(html_text, "html.parser")
    body = soup.body or soup
    for img in list(body.find_all("img")):
        src = (img.get("src") or "").strip()
        if not src or src in ("None", "nan") or src.startswith("[["):
            box = img.parent if img.parent is not None and img.parent.name == "div" and len(img.parent.find_all(True)) == 1 else img
            box.decompose()
            log.append("removed a chart with no data")
    for el in list(body.find_all(True)):
        if getattr(el, "decomposed", False) or el.parent is None:
            continue
        if el.find(True) is not None:
            continue
        txt = el.get_text(" ", strip=True)
        if not txt or not is_missing(txt):
            continue
        if el.name in ("td", "th"):
            el.string = ""
            log.append("blanked a table cell with a missing value")
            continue
        parent = el.parent
        target = parent if parent is not None and parent.name in ("div", "li", "span") and parent is not body and len(parent.find_all(True)) <= 4 else el
        target.decompose()
        log.append(f"removed a stat with a missing value ({txt})")
    for node in list(body.find_all(string=True)):
        if isinstance(node, NavigableString) and node.parent is not None and node.parent.name not in ("script", "style"):
            s = str(node)
            if has_issue(s):
                new = strip_missing(s)
                node.replace_with(new)
                log.append(f"removed text with a missing value: {s.strip()[:60]!r}")
    return str(soup), log
