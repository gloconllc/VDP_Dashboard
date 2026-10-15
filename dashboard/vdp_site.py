"""visitdanapoint.com as the master reference for Visit Dana Point content.

Photos are served by Visit Dana Point's own image service (Simpleview) at the
size each surface needs. Every use layers the bundled local photo underneath,
so if the service is unreachable the page looks exactly as it did before.
"""

SITE_URL = "https://visitdanapoint.com/"
EVENTS_URL = "https://visitdanapoint.com/events/"
PHOTO_CREDIT = "Photo: Visit Dana Point (visitdanapoint.com)"

_CDN = "https://assets.simpleviewinc.com/simpleview/image/upload/"
_IMG = {
    # Aerial of the Dana Point headlands and harbor (site footer image).
    "header_aerial": "v1/clients/danapointca/footer_bg_993c3e06-98b5-4286-b6cf-2212200443d8.jpg",
    # Breaching whale (site share image).
    "whale": "v1/clients/danapointca/a9d9781aa3a1d8c1324d5706e9d88b608f2fcd08_d0b323b4-304a-4d05-897d-85be02ad7bfc.jpg",
    # Resort on the bluff above the coast.
    "resort_bluff": "v1/clients/danapointca/Ritz_Carlton_Seth_Willingham_3_b4cf6c4a-c78c-41a7-8488-eff26247e3c7.jpg",
}


def photo_url(key: str, w: int = 1800, h: int = 700, q: int = 70) -> str:
    """Sized photo URL from the Visit Dana Point image service."""
    return f"{_CDN}c_fill,f_auto,g_auto,h_{h},q_{q},w_{w}/{_IMG[key]}"


def layered(key: str, fallback_uri: str, w: int = 1800, h: int = 700) -> str:
    """CSS background-image value: site photo on top, bundled photo underneath."""
    parts = [f"url('{photo_url(key, w, h)}')"]
    if fallback_uri:
        parts.append(f"url('{fallback_uri}')")
    return ", ".join(parts)


# Every photo PULSE shows, for the Photo credits table in the report appendix.
# Site photos carry their visitdanapoint.com page; bundled photos say where they
# came from as far as the project records it, without guessing a photographer.
PHOTO_CREDITS = [
    ["Aerial of the Dana Point headlands and harbor", "Board masthead; page header; Insights report banner",
     "visitdanapoint.com (site footer image), via Visit Dana Point's image service",
     "https://visitdanapoint.com/"],
    ["Breaching whale", "Loading screen",
     "visitdanapoint.com (site share image), via Visit Dana Point's image service",
     "https://visitdanapoint.com/"],
    ["Dana Point coastline (hero_coast.jpg)", "Page header fallback",
     "Dana Point photography supplied for PULSE; photographer not recorded", "Bundled with PULSE"],
    ["Dana Point coastline, board crop (hero_coast_board.jpg)", "Board masthead fallback; Insights report header",
     "Dana Point photography supplied for PULSE; photographer not recorded", "Bundled with PULSE"],
    ["Drone aerial (drone_aerial.jpg)", "Intelligence Brief banner; weekly PDF cover",
     "Dana Point photography supplied for PULSE; photographer not recorded", "Bundled with PULSE"],
    ["Whale watching (whale_watching.jpg)", "Weekly PDF photo cards",
     "Dana Point photography supplied for PULSE; photographer not recorded", "Bundled with PULSE"],
    ["Harbor coastline (harbor_coastline.jpg)", "Weekly PDF photo cards",
     "Dana Point photography supplied for PULSE; photographer not recorded", "Bundled with PULSE"],
    ["Classic woody (classic_woody.jpg)", "Weekly PDF photo cards",
     "Dana Point photography supplied for PULSE; photographer not recorded", "Bundled with PULSE"],
]
