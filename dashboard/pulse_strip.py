"""At a glance strips and the PULSE Summary announcement under the board.

Added 2026-10-14. Purely additive: each strip mounts below an existing board
section and never replaces it. Kept deliberately small (clean, basic,
impactful): one announcement with three numbers, and per board section three
data cards plus two infographics, all from the year-to-date Insights report
(pulse_insights.build_report), each with its period and source.
"""
from __future__ import annotations

import os

import streamlit as st

_HERE = os.path.dirname(os.path.abspath(__file__))

# Board section -> (section supplying the data cards, [(report section, infographic type)])
PLAN = {
    # Only what the board does not already show (no repeats of its charts).
    # (cards from section, card indexes, [(report section, infographic type)])
    "snapshot": ("city", [0, 1, 2], [("today", "chain")]),
    "visitors": ("mkt", [0, 1, 3], [("who", "waffle"), ("mkt", "funnel")]),
    "forward": ("night", [0, 1, 2], [("night", "funnel")]),
}
TITLES = {
    "snapshot": "City revenue and jobs",
    "visitors": "Trips and marketing reach",
    "forward": "One more night",
}

_STRIP_CSS = """
.ps{font-family:var(--sans);color:var(--ink-body);font-size:16px;line-height:1.5;position:relative}
.ps-strip{margin:6px 0 22px;padding:20px 22px;border:1px solid var(--border);border-radius:16px;background:#fff}
.ps-head{display:flex;flex-wrap:wrap;gap:8px 18px;align-items:flex-end;justify-content:space-between;margin-bottom:14px}
.ps-eyebrow{font-size:11.5px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:var(--teal)}
.ps-title{font-family:var(--serif);font-style:italic;font-weight:700;font-size:clamp(20px,2vw,26px);color:var(--ink);margin:2px 0 0}
.ps-more{font-weight:700;font-size:14px;color:var(--teal-text);text-decoration:none;white-space:nowrap}
.ps-more:hover{text-decoration:underline}
.ps-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr));gap:12px;margin-bottom:14px}
.ps-card{border:1px solid var(--mist-border);border-top:4px solid var(--teal);border-radius:12px;padding:14px 16px;background:var(--mist);transition:transform .15s,border-color .15s}
.ps-card:hover,.ps-card:focus-visible{transform:translateY(-2px);border-color:var(--teal)}
.ps-k{font-size:11.5px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--gray-label)}
.ps-v{font-size:clamp(28px,3vw,36px);font-weight:800;color:var(--ink);line-height:1.1;margin:4px 0 2px;font-variant-numeric:tabular-nums}
.ps-l{font-size:14.5px;font-weight:600;color:var(--ink-2)}
.ps-p{font-size:13.5px;color:var(--ink-3);margin-top:4px}
.ps-p.up{color:var(--teal-text);font-weight:700}.ps-p.dn{color:var(--maroon);font-weight:700}
.ps-src{font-size:12px;color:var(--ink-3);margin-top:8px}
.ps-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,420px),1fr));gap:14px}
.ps .ig{margin:0}
.ps .waffle{max-width:220px;width:min(220px,100%)}
.ps .dim>[data-tip]:not(.sel){opacity:.5}
.ps .sel{outline:2px solid var(--ink);outline-offset:1px}
.ps-tip{position:absolute;z-index:20;max-width:280px;padding:7px 10px;border-radius:8px;background:#0B2530;color:#fff;font-size:13px;transform:translateY(-100%);pointer-events:none}
.ps-ann{display:flex;flex-wrap:wrap;align-items:center;gap:8px 14px;margin:8px 0 18px;padding:14px 20px;border-radius:14px;color:#fff;text-decoration:none;background:linear-gradient(135deg,#0E3340 0%,var(--teal-dk) 55%,var(--teal-deep) 100%);transition:background .15s}
.ps-ann:hover{background:var(--teal-dk)}
.ps-flag{background:#fff;color:var(--teal-dk);font-weight:800;font-size:12px;letter-spacing:.06em;padding:3px 10px;border-radius:20px}
.ps-ann-h{font-family:var(--serif);font-style:italic;font-weight:700;font-size:clamp(17px,1.7vw,21px);flex:1 1 360px}
.ps-date{font-size:12.5px;color:#CFE8EF}
.ps-go{font-weight:800;font-size:14px;white-space:nowrap}
@media (max-width:640px){.ps-strip{padding:16px 14px}.ps-ann{padding:18px 16px}.ps-more{white-space:normal}}
@media (prefers-reduced-motion:reduce){.ps-card{transition:none}}
"""


def _read(name: str) -> str:
    with open(os.path.join(_HERE, name), encoding="utf-8") as fh:
        return fh.read()


@st.cache_resource(show_spinner=False)
def _component():
    base = _read("pulse_insights.css")
    # The report stylesheet targets a full page; inside the component's shadow
    # root its tokens belong on :host and its page rules on the strip root.
    base = base.replace(":root{", ":host{").replace("html,body{", ".ps{")
    return st.components.v2.component("pulse_strip", css=base + _STRIP_CSS, js=_read("pulse_strip.js"))


def available() -> bool:
    try:
        return hasattr(st, "components") and hasattr(st.components, "v2") and _component() is not None
    except Exception:  # noqa: BLE001
        return False


def _sections(report: dict) -> dict:
    return {s["id"]: s for s in report.get("sections", [])}


def glance(report: dict, board_section: str) -> dict | None:
    plan = PLAN.get(board_section)
    if not plan or not report:
        return None
    secs = _sections(report)
    cards_from, idx, igs = plan
    glance_blocks = []
    for sid, kind in igs:
        sec = secs.get(sid) or {}
        blk = next((g for g in sec.get("glance", []) if g.get("type") == kind), None)
        if blk:
            glance_blocks.append(blk)
    nums = (secs.get(cards_from) or {}).get("numbers", [])
    cards = [nums[i] for i in idx if i < len(nums)]
    if not glance_blocks and not cards:
        return None
    return {"kind": "glance", "title": TITLES.get(board_section, ""), "period": report.get("window_label", "Year to date"),
            "cards": cards, "glance": glance_blocks[:2]}


def announce(report: dict) -> dict | None:
    s = (report or {}).get("summary") or {}
    if not s.get("headline"):
        return None
    return {"kind": "announce", "highlight": s.get("highlight", ""), "headline": s["headline"],
            "period": report.get("period", "")}


def mount(payload: dict | None, key: str) -> None:
    if payload:
        _component()(key=key, data=payload)
