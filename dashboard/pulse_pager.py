"""One section per page for the Dana Point PULSE board (added 2026-10-15).

mount() adds a zero-height component whose script hides every section except the
current one and shows a Back / Next bar. It changes no content: the long scroll
comes back with "Show all sections", with PULSE_PAGER=0, or whenever the page
layout is not recognised. All sections, charts, downloads and the existing
sidebar links stay where they are.
"""
from __future__ import annotations

import json
import os

import streamlit.components.v1 as components

_HERE = os.path.dirname(os.path.abspath(__file__))
# The Intelligence Brief and the Insights report no longer have pages of their own (2026-10-16):
# the brief sits on At a glance, and old links to either land on the page that now holds it.
ALIAS = {"pulse-brain": "pulse-glance", "pulse-insights": "pulse-notes"}

# Every Insights report section sits on the page it belongs with. Pages whose id is not a real
# anchor are "virtual": they only show their embedded report section(s).
EMBEDS = {
    "pulse-glance": [["overview", "Summary and overview"]],
    "pulse-snapshot": [["today", "Dana Point today"]],
    "pulse-origins": [["spend", "Visitor spending"]],
    "pulse-who": [["who", "Who visits"]],
    "pulse-here": [["here", "Getting here"]],
    "pulse-mkt": [["mkt", "Marketing reach"]],
    "pulse-city": [["city", "City revenue and jobs"]],
    "pulse-market": [["hotel", "Hotel market"]],
    "pulse-night": [["night", "One more night"]],
    "pulse-forward": [["outlook", "Market outlook"]],
    "pulse-appx": [["appx", "Appendix"]],
    "pulse-sources": [["sources", "Sources"]],
}

# (label, host page it hangs off) for pages that exist only to show a report section.
VIRTUAL = {
    "pulse-who": ("Who visits", "pulse-origins"),
    "pulse-here": ("Getting here", "pulse-origins"),
    "pulse-mkt": ("Marketing reach", "pulse-origins"),
    "pulse-city": ("City revenue and jobs", "pulse-snapshot"),
    "pulse-night": ("One more night", "pulse-market"),
    "pulse-appx": ("Appendix", "pulse-about"),
    "pulse-sources": ("Sources", "pulse-about"),
}
ORDER = ["pulse-glance", "pulse-snapshot", "pulse-origins", "pulse-who", "pulse-here", "pulse-mkt",
         "pulse-city", "pulse-market", "pulse-night", "pulse-forward", "pulse-fullreport",
         "pulse-about", "pulse-appx", "pulse-sources", "pulse-notes"]


def mount(links: list[tuple[str, str]]) -> None:
    if os.environ.get("PULSE_PAGER", "1") == "0" or not links:
        return
    try:
        with open(os.path.join(_HERE, "pulse_pager.js"), encoding="utf-8") as fh:
            js = fh.read()
        real = {a: t for a, t in links if a}
        pages = []
        for pid in ORDER:
            if pid in VIRTUAL:
                pages.append([pid, VIRTUAL[pid][0], VIRTUAL[pid][1]])
            elif pid in real:
                pages.append([pid, real[pid]])
        cfg = json.dumps({"pages": pages, "alias": ALIAS, "embeds": EMBEDS, "combine": []})
        components.html(f"<script>window.PZ_CFG={cfg};</script><script>{js}</script>", height=0)
    except Exception as exc:  # noqa: BLE001
        print(f"[pulse] pager skipped: {exc}")
