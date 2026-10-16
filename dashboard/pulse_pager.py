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
# The Intelligence Brief now sits on the At a glance page, so old links to it land there.
ALIAS = {"pulse-brain": "pulse-glance"}


# Each Insights report section sits on the page it belongs with (2026-10-16).
EMBEDS = {
    "pulse-glance": [["overview", "Summary and overview"]],
    "pulse-snapshot": [["today", "Dana Point today"], ["city", "City revenue and jobs"]],
    "pulse-origins": [["who", "Who visits"], ["spend", "Visitor spending"], ["here", "Getting here"], ["mkt", "Marketing reach"]],
    "pulse-market": [["hotel", "Hotel market"]],
    "pulse-forward": [["night", "One more night"], ["outlook", "Market outlook"]],
    "pulse-about": [["appx", "Appendix"], ["sources", "Sources"]],
}


def mount(links: list[tuple[str, str]]) -> None:
    if os.environ.get("PULSE_PAGER", "1") == "0" or not links:
        return
    try:
        with open(os.path.join(_HERE, "pulse_pager.js"), encoding="utf-8") as fh:
            js = fh.read()
        cfg = json.dumps({"pages": [[a, t] for a, t in links], "alias": ALIAS, "embeds": EMBEDS})
        components.html(f"<script>window.PZ_CFG={cfg};</script><script>{js}</script>", height=0)
    except Exception as exc:  # noqa: BLE001
        print(f"[pulse] pager skipped: {exc}")
