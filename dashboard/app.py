"""
Dana Point PULSE Report Viewer
-----------------------------------------
Minimal app. Its only job: generate the PULSE report PDF from live STR /
CoStar / Datafy data (each on its own native reporting window) and display
it in full.
"""

import base64
import io
import os
import sys
from datetime import datetime

import streamlit as st

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
PDF_PATH = os.path.join(LOGS_DIR, "weekly_report_latest.pdf")
LOGO_PATH = os.path.join(BASE_DIR, "assets", "vdp_logo.svg")
LOGO_NAV_PATH = os.path.join(BASE_DIR, "assets", "vdp_logo_nav.svg")
HERO_PHOTO_PATH = os.path.join(BASE_DIR, "assets", "photos", "hero_coast.jpg")

st.set_page_config(
    page_title="Dana Point PULSE",
    page_icon="📄",
    layout="wide",
)


def _file_data_uri(path: str, mime: str) -> str:
    try:
        with open(path, "rb") as fh:
            b64 = base64.b64encode(fh.read()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except FileNotFoundError:
        return ""


def _logo_data_uri() -> str:
    return _file_data_uri(LOGO_PATH, "image/svg+xml")


def _logo_nav_data_uri() -> str:
    return _file_data_uri(LOGO_NAV_PATH, "image/svg+xml")


def _hero_photo_data_uri() -> str:
    return _file_data_uri(HERO_PHOTO_PATH, "image/jpeg")

st.markdown(
    """
    <style>
      #MainMenu {visibility: hidden;}
      footer {visibility: hidden;}
      .block-container { padding-top: 3rem; max-width: 100%; padding-left: 3rem; padding-right: 3rem; }
      .pulse-header { display:flex; align-items:center; justify-content:space-between;
                      padding-top: 6px; padding-bottom: 14px;
                      border-bottom: 3px solid transparent;
                      border-image: linear-gradient(90deg, #123C4A 0%, #1D6E86 45%, #B4530980 100%) 1;
                      margin-bottom: 18px; overflow: visible; line-height: 1.3; }
      .pulse-header-left { display:flex; align-items:center; gap:16px; }
      .pulse-logo-badge { background:#123C4A; border-radius:10px; width:58px; height:50px;
                           display:flex; align-items:center; justify-content:center; flex-shrink:0;
                           box-shadow: 0 2px 6px rgba(18,60,74,0.35); }

      /* Destination hero band: real Dana Point photography */
      .pulse-hero { position:relative; height:180px; border-radius:14px; overflow:hidden;
        background-size:cover; background-position:center; margin-bottom:22px; }
      .pulse-hero-overlay { position:absolute; inset:0;
        background:linear-gradient(180deg, rgba(9,32,40,0.58) 0%, rgba(9,32,40,0.72) 100%); }
      .pulse-hero-content { position:absolute; inset:0; display:flex; flex-direction:column;
        align-items:center; justify-content:center; text-align:center; }
      .pulse-hero-logo { width:170px; height:auto; margin-bottom:10px;
        filter: drop-shadow(0 2px 10px rgba(0,0,0,0.65)) drop-shadow(0 0 2px rgba(0,0,0,0.5)); }
      .pulse-hero-tag { color:#FFFFFF; font-size:13px; font-weight:600; letter-spacing:.06em; text-transform:uppercase;
        text-shadow: 0 1px 6px rgba(0,0,0,0.55); }
      .pulse-logo-badge img { width:36px; height:auto; }
      .pulse-eyebrow { font-size: 11px; font-weight:700; letter-spacing:.12em; text-transform:uppercase;
        color:#1D6E86; margin-bottom:2px; }
      .pulse-title { font-family:Georgia,'DejaVu Serif',serif; font-style:italic; font-size: 28px;
        font-weight: 700; color: #0B2530; line-height:1.15; }
      .pulse-sub { font-size: 13.5px; color: #475569; margin-top:5px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
      .pulse-sub .pulse-sub-brand { color:#1D6E86; font-weight:600; }
      .pulse-sub .pulse-sub-dot { color:#CBD9DE; }
      .pulse-sub .pulse-status-pill { display:inline-flex; align-items:center; gap:6px;
        background:#F0F7F9; border:1px solid #CBE3EA; border-radius:20px; padding:3px 10px 3px 8px;
        color:#0E4B5C; font-weight:600; font-size:12.5px; }
      .pulse-sub .pulse-status-dot { width:7px; height:7px; border-radius:50%; background:#1D9E6F; flex-shrink:0; }

      /* Regenerate button + filter row */
      div[data-testid="stButton"] > button {
        background:#1D6E86; color:#FFFFFF; border:1px solid #123C4A; border-radius:8px;
        font-weight:600; transition: background 0.15s ease;
      }
      div[data-testid="stButton"] > button:hover { background:#123C4A; color:#FFFFFF; border-color:#123C4A; }
      div[data-testid="stButton"] > button:active { background:#0E4B5C; color:#FFFFFF; }
      .pulse-filter-row { display:flex; align-items:flex-end; gap:14px; margin-bottom:6px; }
      div[data-testid="stDateInput"] input { border-color:#CBD9DE; }
      div[data-testid="column"] { display:flex; align-items:center; }

      /* Whale-watching loading splash */
      .whale-splash {
        position:relative; height:340px; border-radius:14px; overflow:hidden;
        background: linear-gradient(180deg, #D2E0E9 0%, #75A9CC 35%, #255A6D 70%, #16333D 100%);
        display:flex; align-items:center; justify-content:center; flex-direction:column;
        margin: 10px 0 24px 0;
      }
      .whale-splash .sun { position:absolute; top:26px; right:60px; width:46px; height:46px;
        border-radius:50%; background:#FFE9A8; box-shadow:0 0 40px 10px rgba(255,233,168,0.5); }
      .whale-splash .wave-row { position:absolute; left:0; right:0; height:26px; opacity:.55; }
      .whale-splash .wave-row svg { width:200%; height:100%; animation: wave-drift 9s linear infinite; }
      .whale-splash .wave1 { bottom:64px; }
      .whale-splash .wave2 { bottom:34px; opacity:.4; }
      .whale-splash .wave2 svg { animation-duration: 13s; animation-direction: reverse; }
      @keyframes wave-drift { from { transform: translateX(0); } to { transform: translateX(-50%); } }

      .whale-splash .scene { position:relative; width:220px; height:170px; }
      .whale-splash .tail {
        position:absolute; bottom:18px; left:50%; width:150px; height:150px;
        transform-origin: bottom center; transform: translateX(-50%) rotate(0deg);
        animation: whale-dive 4.5s ease-in-out infinite;
      }
      @keyframes whale-dive {
        0%   { transform: translateX(-50%) translateY(20px) rotate(0deg); opacity:0; }
        12%  { opacity:1; }
        30%  { transform: translateX(-50%) translateY(-58px) rotate(-6deg); opacity:1; }
        45%  { transform: translateX(-50%) translateY(-64px) rotate(4deg); opacity:1; }
        60%  { transform: translateX(-50%) translateY(-40px) rotate(-2deg); opacity:1; }
        78%  { transform: translateX(-50%) translateY(30px) rotate(0deg); opacity:0.4; }
        100% { transform: translateX(-50%) translateY(30px) rotate(0deg); opacity:0; }
      }
      .whale-splash .splash-ring {
        position:absolute; bottom:18px; left:50%; width:120px; height:18px; margin-left:-60px;
        border-radius:50%; background:radial-gradient(ellipse at center, rgba(255,255,255,0.75) 0%, rgba(255,255,255,0) 72%);
        animation: splash-pulse 4.5s ease-in-out infinite;
      }
      @keyframes splash-pulse {
        0%, 68% { opacity:0; transform: translateX(-50%) scale(0.6); }
        74% { opacity:1; transform: translateX(-50%) scale(1); }
        90% { opacity:0; transform: translateX(-50%) scale(1.5); }
        100% { opacity:0; }
      }
      .whale-splash .boat { position:absolute; bottom:78px; left:26px; width:54px;
        animation: boat-bob 3.2s ease-in-out infinite; }
      @keyframes boat-bob { 0%,100% { transform: translateY(0px) rotate(-2deg); } 50% { transform: translateY(-6px) rotate(2deg); } }
      .whale-splash .caption { color:#F0FAFF; font-size:13px; font-weight:600; letter-spacing:.02em;
        margin-top:14px; text-shadow:0 1px 4px rgba(0,0,0,0.25); }
      .whale-splash .caption-sub { color:#CFEFF9; font-size:11px; margin-top:2px; }
    </style>
    """,
    unsafe_allow_html=True,
)

_WAVE_SVG = (
    '<svg viewBox="0 0 200 20" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">'
    '<path d="M0 10 Q 12.5 0, 25 10 T 50 10 T 75 10 T 100 10 T 125 10 T 150 10 T 175 10 T 200 10 V20 H0 Z" fill="#F0FAFF"/>'
    '</svg>'
)

WHALE_SPLASH_HTML = f"""
<div class="whale-splash">
  <div class="sun"></div>
  <div class="wave-row wave1">{_WAVE_SVG}{_WAVE_SVG}</div>
  <div class="wave-row wave2">{_WAVE_SVG}{_WAVE_SVG}</div>
  <div class="scene">
    <svg class="boat" viewBox="0 0 60 30" xmlns="http://www.w3.org/2000/svg">
      <path d="M4 22 L56 22 L48 29 L12 29 Z" fill="#0F172A"/>
      <rect x="27" y="4" width="2.4" height="18" fill="#0F172A"/>
      <path d="M29.4 6 L44 20 L29.4 20 Z" fill="#F8FAFB"/>
    </svg>
    <div class="splash-ring"></div>
    <svg class="tail" viewBox="0 0 150 150" xmlns="http://www.w3.org/2000/svg">
      <path d="M75 150 C 60 100, 40 90, 10 70 C 45 78, 60 65, 75 40 C 90 65, 105 78, 140 70 C 110 90, 90 100, 75 150 Z"
            fill="#255A6D" stroke="#16333D" stroke-width="2"/>
    </svg>
  </div>
  <div class="caption">Heading out for this week&rsquo;s numbers&hellip;</div>
  <div class="caption-sub">Building the Dana Point PULSE report from live STR, CoStar &amp; Datafy data</div>
</div>
"""


@st.cache_data(ttl=3600, show_spinner=False)
def _generate(_cache_key: str, range_start: str | None, range_end: str | None):
    from generate_weekly_report import build_report
    override = (range_start, range_end) if range_start and range_end else None
    return build_report(date_range=override)


# Section map: each entry is one poppable section a viewer can jump to.
# "page" is the 0-based index into the physical PDF (0 = cover, which is
# intentionally not offered as its own pop-out section).
SECTIONS = [
    {"icon": "📊", "title": "Executive Summary & Hotel Performance",
     "desc": "Headline KPIs, the occupancy trend, and the story behind this week's numbers.", "page": 1},
    {"icon": "🏨", "title": "Hotel Performance (ADR) & Visitor Origins",
     "desc": "Average daily rate by day of week and where visitors are traveling from.", "page": 2},
    {"icon": "🏢", "title": "Market Segments",
     "desc": "CoStar chain-scale occupancy and RevPAR by tier for the Newport Beach/Dana Point submarket.", "page": 3},
    {"icon": "🏬", "title": "Chain-Scale Segment Detail",
     "desc": "Segment-level performance and the TBID/TOT tax estimate.", "page": 4},
    {"icon": "🧳", "title": "Visitor Profile & Spend",
     "desc": "Datafy visitor demographics, category spend, and length of stay.", "page": 5},
    {"icon": "📈", "title": "Forward Outlook & Group Business",
     "desc": "What's ahead for compression, group bookings, and travel trends.", "page": 6},
]


@st.cache_data(ttl=3600, show_spinner=False)
def _split_pdf_pages(pdf_bytes: bytes) -> list[bytes]:
    """Split the full report into single-page PDFs so a section can be
    viewed or printed on its own without the rest of the report."""
    from pypdf import PdfReader, PdfWriter

    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages: list[bytes] = []
    for page in reader.pages:
        writer = PdfWriter()
        writer.add_page(page)
        buf = io.BytesIO()
        writer.write(buf)
        pages.append(buf.getvalue())
    return pages


def _pdf_generated_at() -> str:
    if os.path.exists(PDF_PATH):
        return datetime.fromtimestamp(os.path.getmtime(PDF_PATH)).strftime("%b %d, %Y at %I:%M %p")
    return "not yet generated"


def _header_html(status_text: str, ok: bool = True) -> str:
    dot_color = "#1D9E6F" if ok else "#C2410C"
    return f"""
        <div class="pulse-header">
          <div class="pulse-header-left">
            <div class="pulse-logo-badge"><img src="{_logo_data_uri()}"></div>
            <div>
              <div class="pulse-eyebrow">Destination Intelligence Report</div>
              <div class="pulse-title">Dana Point PULSE</div>
              <div class="pulse-sub">
                <span class="pulse-sub-brand">Prepared by GloCon Solutions LLC for Visit Dana Point</span>
                <span class="pulse-sub-dot">&bull;</span>
                <span class="pulse-status-pill">
                  <span class="pulse-status-dot" style="background:{dot_color};"></span>{status_text}
                </span>
              </div>
            </div>
          </div>
        </div>
        """


col1, col2, col3 = st.columns([4, 2, 1])
with col1:
    header_slot = st.empty()
    # Shown immediately, before this run's report generation has actually
    # happened, so it can't yet reflect a real timestamp. Replaced below
    # with the true "Last generated" stamp once generation completes.
    header_slot.markdown(_header_html("Generating latest report&hellip;"), unsafe_allow_html=True)
with col2:
    date_range = st.date_input(
        "Report window",
        value=(),
        label_visibility="collapsed",
        help="Pick a start and end date to rebuild the hotel-performance window (cover, Executive Summary, Hotel Performance pages). Datafy and CoStar sections always show their own freshest available period.",
    )
with col3:
    regenerate = st.button("Regenerate", use_container_width=True)

range_start_iso = date_range[0].isoformat() if len(date_range) == 2 else None
range_end_iso = date_range[1].isoformat() if len(date_range) == 2 else None

st.markdown(
    f"""
    <div class="pulse-hero" style="background-image:url('{_hero_photo_data_uri()}');">
      <div class="pulse-hero-overlay"></div>
      <div class="pulse-hero-content">
        <img class="pulse-hero-logo" src="{_logo_nav_data_uri()}">
        <div class="pulse-hero-tag">Dana Point, California</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if regenerate:
    _generate.clear()

splash = st.empty()
splash.markdown(WHALE_SPLASH_HTML, unsafe_allow_html=True)

try:
    cache_key = datetime.now().strftime("%Y-%m-%d-%H") if not regenerate else datetime.now().isoformat()
    pdf_path = _generate(cache_key, range_start_iso, range_end_iso)
except Exception as exc:  # noqa: BLE001
    splash.empty()
    header_slot.markdown(_header_html("Last generated: generation failed, see logs", ok=False), unsafe_allow_html=True)
    st.error(f"Report generation failed: {exc}")
    st.stop()

splash.empty()

if not os.path.exists(pdf_path):
    header_slot.markdown(_header_html("Last generated: report file missing", ok=False), unsafe_allow_html=True)
    st.error("Report file was not created. Check logs for details.")
    st.stop()

header_slot.markdown(_header_html(f"Last generated: {_pdf_generated_at()}"), unsafe_allow_html=True)

with open(pdf_path, "rb") as f:
    pdf_bytes = f.read()

st.download_button(
    "⬇ Download PDF",
    data=pdf_bytes,
    file_name=f"Visit Dana Point PULSE Report {datetime.now().strftime('%Y-%m-%d')}.pdf",
    mime="application/pdf",
)

section_pages = _split_pdf_pages(pdf_bytes)
available_sections = [s for s in SECTIONS if s["page"] < len(section_pages)]

if "open_section" not in st.session_state:
    st.session_state["open_section"] = None

if available_sections:
    st.markdown("#### View a Section")
    st.caption("Click any section below to open, view, or print just that part of the report.")
    grid_cols = st.columns(3)
    for i, section in enumerate(available_sections):
        with grid_cols[i % 3]:
            with st.container(border=True):
                st.markdown(f"**{section['icon']} {section['title']}**")
                st.caption(section["desc"])
                if st.button("View section", key=f"open_section_{i}", use_container_width=True):
                    st.session_state["open_section"] = i
                    st.rerun()

open_idx = st.session_state.get("open_section")
if open_idx is not None and 0 <= open_idx < len(available_sections):
    open_section = available_sections[open_idx]

    @st.dialog(open_section["title"], width="large")
    def _section_dialog():
        st.caption(open_section["desc"])
        page_bytes = section_pages[open_section["page"]]
        page_b64 = base64.b64encode(page_bytes).decode("utf-8")
        st.markdown(
            f"""
            <iframe src="data:application/pdf;base64,{page_b64}"
                    width="100%" height="620px" style="border:1px solid #E2E8F0; border-radius:8px;">
            </iframe>
            """,
            unsafe_allow_html=True,
        )
        dl_col, close_col = st.columns(2)
        with dl_col:
            st.download_button(
                "⬇ Download this section",
                data=page_bytes,
                file_name=f"Visit Dana Point PULSE - {open_section['title']} - {datetime.now().strftime('%Y-%m-%d')}.pdf",
                mime="application/pdf",
                use_container_width=True,
                key="download_section_pdf",
            )
        with close_col:
            if st.button("Close, return to full report", use_container_width=True, key="close_section_dialog"):
                st.session_state["open_section"] = None
                st.rerun()

    _section_dialog()

st.markdown("#### Full Report")
b64 = base64.b64encode(pdf_bytes).decode("utf-8")
st.markdown(
    f"""
    <iframe src="data:application/pdf;base64,{b64}"
            width="100%" height="900px" style="border:1px solid #E2E8F0; border-radius:8px;">
    </iframe>
    """,
    unsafe_allow_html=True,
)
