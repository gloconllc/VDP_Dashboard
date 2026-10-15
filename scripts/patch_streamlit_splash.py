#!/usr/bin/env python3
"""
patch_streamlit_splash.py — Brand Streamlit's static index.html at build time.

Streamlit serves a static index.html before any app code runs. By default the
browser shows the "Streamlit" title, the Streamlit favicon, and a generic
loading state for a beat before the Dana Point PULSE app mounts. That flash of
Streamlit branding is what users see "first."

This script patches that static template once, during the Docker build, so the
very first paint is already Dana Point PULSE:
  1. <title>           -> "Dana Point PULSE"
  2. favicon <link>    -> embedded data-URI of dashboard/static/favicon.png
                          (no network request, shows instantly, never pops in)
  3. a branded splash  -> full-screen ocean-gradient overlay with the PULSE
                          wordmark + spinner, injected as the first child of
                          <body>. A tiny inline script fades it out as soon as
                          the React root has mounted real content.

Idempotent: re-running is a no-op (guard marker check). Safe to call on every
build. Locates Streamlit's static dir via the installed package, so it tracks
whatever version requirements.txt pins.

Usage:  python scripts/patch_streamlit_splash.py
"""

from __future__ import annotations

import base64
import os
import sys

MARKER = "dp-pulse-splash"  # guard so we never double-patch
APP_TITLE = "Dana Point PULSE"


def _find_index_html() -> str | None:
    try:
        import streamlit  # noqa: WPS433 (runtime import by design)
    except ImportError:
        print("[patch] streamlit not importable — skipping (non-fatal)")
        return None
    path = os.path.join(os.path.dirname(streamlit.__file__), "static", "index.html")
    return path if os.path.isfile(path) else None


def _favicon_data_uri() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    fav = os.path.join(here, "..", "dashboard", "static", "favicon.png")
    fav = os.path.normpath(fav)
    if not os.path.isfile(fav):
        print(f"[patch] favicon not found at {fav} — using empty icon")
        return ""
    with open(fav, "rb") as fh:
        b64 = base64.b64encode(fh.read()).decode("ascii")
    return f"data:image/png;base64,{b64}"


def _splash_html(data_uri: str) -> str:
    """Full-screen cinematic branded overlay + self-removing script. Paints before React."""
    return f"""
<style id="{MARKER}-style">
  #{MARKER} {{ transition:opacity 0.6s ease, visibility 0.6s ease; opacity:1; }}
  #{MARKER}.dp-hide {{ opacity:0; visibility:hidden; pointer-events:none; }}
  @keyframes splash-progress {{ 0%{{width:0%}} 70%{{width:85%}} 100%{{width:100%}} }}
  @keyframes ring-pulse {{ 0%,100%{{transform:translate(-50%,-50%) scale(1);opacity:.6}} 50%{{transform:translate(-50%,-50%) scale(1.18);opacity:.15}} }}
  @keyframes ring-pulse2 {{ 0%,100%{{transform:translate(-50%,-50%) scale(1);opacity:.4}} 50%{{transform:translate(-50%,-50%) scale(1.3);opacity:.08}} }}
  @keyframes glitch-logo {{ 0%,95%,100%{{transform:none;filter:none}} 96%{{transform:translate(-2px,1px);filter:hue-rotate(60deg)}} 97%{{transform:translate(2px,-1px);filter:hue-rotate(-60deg)}} 98%{{transform:none;filter:none}} }}
  @keyframes dp-rise {{ from {{ opacity:0; transform:translateY(8px); }} to {{ opacity:1; transform:translateY(0); }} }}
  .dp-splash-ring {{ position:absolute;top:50%;left:50%;border-radius:50%;border:1px solid rgba(0,195,190,0.4); }}
  .dp-splash-r1 {{ width:80px;height:80px;animation:ring-pulse 2s ease-in-out infinite; }}
  .dp-splash-r2 {{ width:110px;height:110px;animation:ring-pulse 2s ease-in-out infinite .4s; }}
  .dp-splash-r3 {{ width:140px;height:140px;animation:ring-pulse2 2.5s ease-in-out infinite .2s; }}
  .dp-splash-logo {{ font-size:40px;text-align:center;animation:glitch-logo 4s infinite; }}
  @media (prefers-reduced-motion: reduce) {{
    .dp-splash-ring, .dp-splash-logo, .dp-splash-bar {{ animation:none !important; }}
  }}
</style>
<div id="{MARKER}" style="
  position:fixed;inset:0;z-index:2147483647;
  background:linear-gradient(180deg,rgba(5,10,20,.70) 0%,rgba(5,10,20,.86) 100%),
    url('https://assets.simpleviewinc.com/simpleview/image/upload/c_fill,f_auto,g_auto,h_1000,q_65,w_1600/v1/clients/danapointca/a9d9781aa3a1d8c1324d5706e9d88b608f2fcd08_d0b323b4-304a-4d05-897d-85be02ad7bfc.jpg') center/cover no-repeat,
    #050A14;
  display:flex;flex-direction:column;align-items:center;justify-content:center;
  font-family:'Inter','Segoe UI',system-ui,sans-serif;
">
  <div style="position:relative;width:140px;height:140px;margin-bottom:32px;">
    <div class="dp-splash-ring dp-splash-r1"></div>
    <div class="dp-splash-ring dp-splash-r2"></div>
    <div class="dp-splash-ring dp-splash-r3"></div>
    <div class="dp-splash-logo" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);">🌊</div>
  </div>
  <div style="font-size:28px;font-weight:800;color:#fff;letter-spacing:.12em;text-transform:uppercase;margin-bottom:8px;animation:dp-rise .7s ease both;">Dana Point PULSE</div>
  <div style="font-size:11px;color:#4A90A4;letter-spacing:.18em;text-transform:uppercase;margin-bottom:32px;animation:dp-rise .7s .1s ease both;">Performance · Understanding · Leadership · Spending · Economy</div>
  <div style="width:200px;height:2px;background:rgba(255,255,255,0.08);border-radius:2px;overflow:hidden;">
    <div class="dp-splash-bar" style="height:100%;width:0%;background:linear-gradient(90deg,#00C3BE,#2563EB);border-radius:2px;animation:splash-progress 2.2s ease forwards;"></div>
  </div>
</div>
<script id="{MARKER}-js">
(function(){{
  var SEL = '#{MARKER}';
  function ready(){{
    var root = document.getElementById('root');
    if(!root || root.children.length === 0) return false;
    // NOTE: the app shell (.stApp) is position:absolute + height:100vh, so
    // #root's own bounding height stays 0 even after the app mounts. Measuring
    // #root here would never satisfy the height check and the splash would hang
    // on its failsafe. Detect the real rendered content inside the main block
    // container instead.
    var main = document.querySelector('[data-testid="stMain"]') ||
               document.querySelector('[data-testid="stAppViewContainer"]') ||
               document.querySelector('.stApp');
    if(!main) return false;
    var content = main.querySelector(
      '[data-testid="stMainBlockContainer"], .block-container, [data-testid="stVerticalBlock"]');
    return !!(content && content.getBoundingClientRect().height > 80);
  }}
  function hide(){{
    var el = document.querySelector(SEL);
    if(!el) return;
    el.classList.add('dp-hide');
    setTimeout(function(){{ if(el && el.parentNode) el.parentNode.removeChild(el); }}, 650);
  }}
  var start = Date.now();
  var iv = setInterval(function(){{
    // Hold a 400ms minimum so the brand reads, then leave as soon as app is up.
    if((ready() && Date.now()-start > 400) || Date.now()-start > 12000){{
      clearInterval(iv); hide();
    }}
  }}, 80);
  window.addEventListener('load', function(){{ setTimeout(function(){{ if(ready()) hide(); }}, 600); }});
}})();
</script>
"""


def patch() -> int:
    index_path = _find_index_html()
    if not index_path:
        print("[patch] no index.html located — skipping (non-fatal)")
        return 0

    with open(index_path, "r", encoding="utf-8") as fh:
        html = fh.read()

    if MARKER in html:
        print("[patch] already patched — no-op")
        return 0

    data_uri = _favicon_data_uri()

    # 1) Title
    import re
    html = re.sub(r"<title>.*?</title>", f"<title>{APP_TITLE}</title>", html, count=1, flags=re.S)
    if f"<title>{APP_TITLE}</title>" not in html:
        # No <title> present — add one inside <head>
        html = html.replace("</head>", f"<title>{APP_TITLE}</title></head>", 1)

    # 2) Favicon — replace any existing icon links, then inject ours
    if data_uri:
        html = re.sub(
            r'<link[^>]*rel=["\'][^"\']*icon[^"\']*["\'][^>]*>',
            "",
            html,
            flags=re.I,
        )
        icon_link = f'<link rel="icon" type="image/png" href="{data_uri}"/>'
        html = html.replace("</head>", f"{icon_link}</head>", 1)

    # 3) Splash overlay — first child of <body> so it paints before the bundle
    splash = _splash_html(data_uri)
    if "<body>" in html:
        html = html.replace("<body>", "<body>" + splash, 1)
    else:
        # Some builds add attributes to <body>; match the opening tag generically
        html = re.sub(r"(<body[^>]*>)", r"\1" + splash.replace("\\", "\\\\"), html, count=1)

    with open(index_path, "w", encoding="utf-8") as fh:
        fh.write(html)

    print(f"[patch] branded splash + favicon + title injected into {index_path}")
    return 0


if __name__ == "__main__":
    sys.exit(patch())
