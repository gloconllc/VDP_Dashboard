"""
fetch_costar_portal.py
-----------------------
Headless CoStar portal automation, built 2026-09-21 for the "click a button
and it fetches new data" workflow.

Adapted from scripts/str_playwright_automation.py, the prior interactive
version (headed browser, meant to be watched, or run through Claude in
Chrome against an already-logged-in session). This version is meant to run
unattended in CI (GitHub Actions), so it logs in itself using stored
credentials rather than relying on someone's live browser session.

STATUS AS OF THIS BUILD:
  Daily and Monthly  — ported from the prior script's mapped navigation
                        (Properties tab -> Save drawer -> "VDP Select" saved
                        search -> Analytics -> Data -> period combobox ->
                        Export -> Data Export). NOT yet run against the live
                        site from this environment (this session's cloud
                        container has no network path to costar.com at all,
                        confirmed by a direct connection test, and no one's
                        live Chrome was available to verify it either) so
                        treat the selectors as carried over, not re-verified.
  Daily/Monthly segmentation, Participation, PDF market report
                     — NOT implemented. The prior script never covered these
                        and guessing at selectors for screens nobody has
                        actually looked at is exactly how the earlier
                        attempt produced 24-byte corrupt files
                        (_to_delete/costar_VisitDanaPoint_*.xls.24byte-corrupt
                        in the repo). Left as a clearly-marked follow-up.

Every step takes a screenshot into SCREENSHOT_DIR regardless of outcome, so
a run can be debugged from its GitHub Actions artifact alone, without
anyone needing to be at a keyboard watching it live.

Credentials:
  Reads STR_USERNAME/STR_PASSWORD first (that's what's already sitting in
  the project's local .env), falls back to COSTAR_USER/COSTAR_PASS if those
  aren't set, so this runs correctly regardless of which name ends up in
  GitHub's secrets. Never commit real credentials to git.

Output:
  Saves into OUTPUT_DIR (default: downloads/) as daily_<MM_DD_YY>.xlsx and
  monthly_<MM_DD_YY>.xlsx, matching the case-insensitive, date-aware pattern
  scripts/load_costar_market_daily.py already looks for. A separate step
  (not this script) is responsible for moving these into data/costar/ and
  triggering the reload pipeline.

Run:
    pip install playwright && playwright install chromium
    python fetch_costar_portal.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright, Locator, Page

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOGIN_URL = "https://product.costar.com"

USERNAME = os.getenv("STR_USERNAME") or os.getenv("COSTAR_USER") or ""
PASSWORD = os.getenv("STR_PASSWORD") or os.getenv("COSTAR_PASS") or ""

OUTPUT_DIR = Path(os.getenv("COSTAR_OUTPUT_DIR", "downloads"))
SCREENSHOT_DIR = Path(os.getenv("COSTAR_SCREENSHOT_DIR", "logs/costar_fetch_screens"))
HEADLESS = os.getenv("COSTAR_HEADLESS", "true").strip().lower() not in ("0", "false", "no")

TODAY_TAG = datetime.now().strftime("%m_%d_%y")

# The live app pulls its data straight from data/costar/ on every push to
# main, so a bad download here is not a cosmetic problem, it is one commit
# away from being live. The prior attempt at this (see
# _to_delete/costar_VisitDanaPoint_*.xls.24byte-corrupt in the repo) failed
# silently and produced 24-byte files that were not real exports. This
# floor (real exports run well into six figures of bytes) plus an actual
# "can pandas open this as an Excel file" check exist so that failure mode
# raises loudly here, inside this script, rather than reaching
# data/costar/ and the live app.
MIN_VALID_EXPORT_BYTES = 20_000


def _validate_export(path: Path) -> None:
    """Raise if the downloaded file is not a real, readable Excel export.

    Two checks: a size floor (catches the exact 24-byte-corrupt failure
    mode seen before) and an actual attempt to open it with pandas (catches
    a same-size-but-wrong file, e.g. an HTML error/login page saved with an
    .xlsx extension).
    """
    if not path.exists():
        raise RuntimeError(f"Export validation failed: {path} does not exist.")
    size = path.stat().st_size
    if size < MIN_VALID_EXPORT_BYTES:
        raise RuntimeError(
            f"Export validation failed: {path} is only {size} bytes "
            f"(expected at least {MIN_VALID_EXPORT_BYTES}). This is the same "
            f"failure mode as the corrupt downloads already in _to_delete/, "
            f"treating this as a failed fetch rather than saving it."
        )
    try:
        import pandas as pd  # local import: only needed for this check

        df = pd.read_excel(path, sheet_name=0, nrows=5)
    except Exception as exc:
        raise RuntimeError(
            f"Export validation failed: {path} is {size} bytes but pandas "
            f"could not read it as an Excel file ({exc}). Not a real export."
        ) from exc
    if df.empty:
        raise RuntimeError(
            f"Export validation failed: {path} opened but has no rows."
        )
    print(f"    [validated] {path} ({size:,} bytes, {len(df.columns)} columns readable)")


def _fail_if_no_credentials() -> None:
    if not USERNAME or not PASSWORD:
        print(
            "[FATAL] No CoStar credentials found. Set STR_USERNAME/STR_PASSWORD "
            "(preferred, matches the project's .env) or COSTAR_USER/COSTAR_PASS "
            "as environment variables / GitHub Actions secrets.",
            file=sys.stderr,
        )
        sys.exit(2)


# ---------------------------------------------------------------------------
# Selectors — carried over from scripts/str_playwright_automation.py,
# "sourced from live DOM inspection" per that script's own comments as of
# its last update. NOT re-verified against the live site by this build.
# If a run fails, the screenshot immediately before the failing step is the
# fastest way to see what actually changed.
# ---------------------------------------------------------------------------

class Sel:
    USERNAME_INPUT = "input[type='email'], input[name='username'], input[name='email']"
    PASSWORD_INPUT = "input[type='password']"
    SUBMIT_BTN = "button[type='submit']"
    POST_LOGIN_MARK = "span[role='tab'][content='Properties']"

    PROPERTIES_TAB = "span[role='tab'][content='Properties']"
    SAVE_BTN = "span.placeholder-normal"  # filter has_text="Save"
    # 2026-09-21: confirmed live via DevTools inspection (John). It's a <div>, not
    # an <a>, so the earlier "a:has-text(...)" guess could never have matched.
    # data-action is a real semantic attribute, not a build hash, so prefer it
    # over the "save-survey-dropdown__save-survey-name--JNO6x" class next to it.
    VDP_SELECT_LINK = "div[data-action='open saved survey'][title='VDP Select']"
    # 2026-09-21: John's live DOM pull shows this button's class has moved from
    # csg-tui-button to Tailwind-style utility classes (csg-tw-button ...), and
    # the text "Okay, got it" is the button's own content, not wrapped in a
    # child <span> as the old filter assumed -- likely why a real popup was
    # reported as "not found" in the prior run. Match on the button's own text
    # only, nothing about its class or internal structure.
    OK_GOT_IT_BTN = "button"  # filter has_text="Okay, got it"
    # 2026-09-21: confirmed live. This is a stable id, not a build-hashed class,
    # and it lives in a separate MAP/LIST/ANALYTICS view-switcher widget, not
    # the top-nav role='tab' group the old selector assumed.
    ANALYTICS_TAB = "#search-bar-analytics-view-button"
    # 2026-09-21: confirmed live. Once inside Analytics, the Summary / KPI /
    # Performance / Construction / Sales / Players / Data / Participation
    # sub-tabs each carry a stable automation-id, not a role='tab' + text match.
    DATA_TAB = "[automation-id='analytics_dashboard_tab_tabKey.data']"
    PARTICIPATION_TAB = "[automation-id='analytics_dashboard_tab_tabKey.participation']"
    PERIOD_COMBOBOX = "input#autocomplete[role='combobox']"  # confirmed live, unchanged
    EXPORT_BTN = "span.placeholder-normal"  # filter has_text="Export" -- confirmed live, unchanged
    # 2026-09-21: the hashed class suffix here changed between when this was
    # first written (--drqqC) and John's latest live pull (--Wn145), exactly
    # the build-hash instability this file's other comments warn about.
    # Match on role + text only now.
    DATA_EXPORT_ITEM = "div[role='menuitem']"  # filter has_text="Data Export"
    # 2026-09-21: confirmed live via DevTools (John), for the Market Reports
    # flow. Markets and Market Insights use the same content-attribute pattern
    # already proven for PROPERTIES_TAB.
    MARKETS_TAB = "span[role='tab'][content='Markets']"
    MARKET_INSIGHTS_TAB = "span[role='tab'][content='Market Insights']"
    SECTOR_SELECT_PLACEHOLDER = "div.cstp-Select-placeholder"  # filter has_text="Sector"
    REPORT_SEARCH_INPUT = "input[placeholder='Search Title or Keyword']"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _click_span_with_text(page: Page, selector: str, text: str, timeout: int = 15_000) -> None:
    loc: Locator = page.locator(selector).filter(has_text=text).first
    await loc.wait_for(state="visible", timeout=timeout)
    await loc.click()


async def _screenshot(page: Page, name: str) -> None:
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = SCREENSHOT_DIR / f"{name}.png"
    try:
        await page.screenshot(path=str(path), full_page=True)
        print(f"    [screenshot] {path}")
    except Exception as exc:
        print(f"    [screenshot FAILED] {name}: {exc}")


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------

async def _dismiss_access_upsell(page: Page) -> None:
    # 2026-09-21: CoStar intermittently overlays a "CoStar property records are
    # available to all CoStar Suite subscribers / Get Access" modal on top of
    # the login page itself (confirmed live, John caught the DOM: a
    # button[title="Close"] in the modal's corner). Not reproduced on every
    # run, so this is a short, non-fatal check, not a hard requirement.
    try:
        close_btn: Locator = page.locator("button[title='Close']").first
        await close_btn.wait_for(state="visible", timeout=4_000)
        await close_btn.click()
        print("    Dismissed subscriber upsell popup.")
    except Exception:
        pass


async def login(page: Page) -> None:
    print("[1] Navigating to CoStar login...")
    await page.goto(LOGIN_URL, wait_until="networkidle", timeout=45_000)
    await _dismiss_access_upsell(page)
    await _screenshot(page, "01_login_page")

    await page.fill(Sel.USERNAME_INPUT, USERNAME)
    await page.fill(Sel.PASSWORD_INPUT, PASSWORD)
    await _screenshot(page, "02_login_filled")
    await page.click(Sel.SUBMIT_BTN)

    try:
        # 2026-09-21: POST_LOGIN_MARK (the Properties tab) is not actually
        # proof of being logged in — confirmed live, it exists on CoStar's
        # public marketing site too (product.costar.com's News section and
        # the public costar.com one share the same header component). A run
        # can "pass" this wait while having silently landed back on the
        # public site with a still-visible Login button, which is what
        # "asks to log in again" looks like from the outside. Check the
        # actual domain first; only that proves the login POST took.
        await page.wait_for_url(lambda url: "product.costar.com" in url, timeout=30_000)
        await page.wait_for_selector(Sel.POST_LOGIN_MARK, timeout=15_000)
    except Exception:
        await _screenshot(page, "02b_login_stuck")
        # Common cause: an MFA / new-device / "verify it's you" challenge,
        # OR landed back on the public marketing site instead of the
        # authenticated app (see note above).
        text_preview = (await page.inner_text("body"))[:800]
        print(f"[FATAL] Did not reach product.costar.com after login (stuck at {page.url}). "
              f"Page text follows:\n{text_preview}")
        raise
    print("    Logged in, dashboard loaded.")
    await _screenshot(page, "03_dashboard")


async def navigate_to_vdp_select(page: Page) -> None:
    print("[2] Clicking Properties tab...")
    # 2026-09-21: this click has been seen to silently not navigate at all, once
    # landing back on the News homepage with no error raised (page.click() found
    # SOME matching element and clicked it without complaint). Verify we actually
    # reached the Properties page, using the Save button as proof, and retry the
    # tab click once before trusting the real 15s timeout to surface a genuine
    # failure with its own screenshot.
    save_btn: Locator = page.locator(Sel.SAVE_BTN).filter(has_text="Save").first
    on_properties = False
    for attempt in (1, 2):
        await page.wait_for_selector(Sel.PROPERTIES_TAB, timeout=15_000)
        await page.click(Sel.PROPERTIES_TAB)
        await _screenshot(page, f"04_properties_tab_attempt{attempt}")
        try:
            await save_btn.wait_for(state="visible", timeout=8_000)
            on_properties = True
            break
        except Exception:
            print(f"    Not on Properties after attempt {attempt}, retrying..."
                  if attempt == 1 else "    Still not on Properties after two attempts.")
    if not on_properties:
        await save_btn.wait_for(state="visible", timeout=15_000)

    print("[3] Opening Save drawer...")
    # Same defensive pattern: the Save click has landed without the dropdown
    # panel rendering in time, on a separate occasion from the issue above.
    vdp_link: Locator = page.locator(Sel.VDP_SELECT_LINK).first
    opened = False
    for attempt in (1, 2):
        await save_btn.click()
        await _screenshot(page, f"05_save_drawer_attempt{attempt}")
        try:
            await vdp_link.wait_for(state="visible", timeout=6_000)
            opened = True
            break
        except Exception:
            print(f"    Save drawer not open after attempt {attempt}, retrying..."
                  if attempt == 1 else "    Still not open after two attempts.")
    if not opened:
        await vdp_link.wait_for(state="visible", timeout=15_000)

    print("[4] Selecting 'VDP Select' saved search...")
    await vdp_link.click()
    await _screenshot(page, "06_vdp_select_loaded")


async def dismiss_popup(page: Page) -> None:
    print("    Checking for composite popup...")
    try:
        ok_btn: Locator = page.locator(Sel.OK_GOT_IT_BTN, has_text="Okay, got it").first
        await ok_btn.wait_for(state="visible", timeout=5_000)
        await ok_btn.click()
        print("    Popup dismissed.")
    except Exception:
        print("    No popup found, continuing.")


async def navigate_to_analytics_data(page: Page) -> None:
    print("[6] Clicking Analytics tab...")
    await page.wait_for_selector(Sel.ANALYTICS_TAB, timeout=15_000)
    await page.click(Sel.ANALYTICS_TAB)
    await _screenshot(page, "07_analytics_view")

    # 2026-09-21: confirmed live (John's DOM pull) that the composite popup
    # can reappear here, after entering Analytics, not only after VDP Select.
    await dismiss_popup(page)

    print("[7] Clicking Data sub-tab...")
    await page.wait_for_selector(Sel.DATA_TAB, timeout=15_000)
    await page.click(Sel.DATA_TAB)
    await _screenshot(page, "08_analytics_data_tab")


async def export_for_period(page: Page, period: str, output_path: Path) -> None:
    print(f"    Setting period to '{period}'...")
    combobox: Locator = page.locator(Sel.PERIOD_COMBOBOX)
    await combobox.wait_for(state="visible", timeout=10_000)
    await combobox.triple_click()
    await combobox.fill(period)
    await page.locator(f"[role='option']:has-text('{period}')").first.click()
    await _screenshot(page, f"08_{period.lower()}_period_set")

    print(f"    Clicking Export for {period}...")
    await _click_span_with_text(page, Sel.EXPORT_BTN, "Export")

    print("    Clicking 'Data Export' menu item...")
    data_export: Locator = page.locator(Sel.DATA_EXPORT_ITEM).filter(has_text="Data Export").first
    await data_export.wait_for(state="visible", timeout=10_000)

    async with page.expect_download(timeout=60_000) as dl_info:
        await data_export.click()

    download = await dl_info.value
    output_path.parent.mkdir(parents=True, exist_ok=True)
    await download.save_as(str(output_path))
    print(f"    Saved -> {output_path}")
    _validate_export(output_path)


# ---------------------------------------------------------------------------
# Not yet implemented — placeholders so this file stays the single source
# of truth for "what covers what," rather than scattering TODOs elsewhere.
# ---------------------------------------------------------------------------

async def export_daily_segmentation(page: Page) -> None:
    raise NotImplementedError(
        "Daily segmentation export navigation has not been observed live yet. "
        "Open question as of 2026-09-21: this may simply be another value in the "
        "same PERIOD_COMBOBOX already used by export_for_period() (only 'Daily' "
        "and 'Monthly' confirmed there so far), rather than a separate flow. "
        "Needs the dropdown's actual option list confirmed live before assuming that."
    )


async def export_monthly_segmentation(page: Page) -> None:
    raise NotImplementedError(
        "Monthly segmentation export navigation has not been observed live yet. "
        "Same open question as export_daily_segmentation(): may just be another "
        "PERIOD_COMBOBOX value, not confirmed live yet."
    )


async def open_participation_tab(page: Page) -> None:
    """Navigate into the Participation sub-tab of Analytics.

    2026-09-21: confirmed live via DevTools (John) -- Participation is a
    sibling of Data in the same analytics-container tab list, with its own
    stable automation-id. What controls (period combobox? a different
    Export flow?) live inside it once open were not observed, so this stops
    at the screenshot rather than guessing.
    """
    print("    Clicking Participation sub-tab...")
    await page.wait_for_selector(Sel.PARTICIPATION_TAB, timeout=15_000)
    await page.click(Sel.PARTICIPATION_TAB)
    await _screenshot(page, "09_participation_tab")


async def export_participation(page: Page) -> None:
    raise NotImplementedError(
        "The Participation tab itself is now mapped (see open_participation_tab(), "
        "confirmed live 2026-09-21), but what's inside it (period selector? its own "
        "Export button?) has not been observed yet. Run open_participation_tab() and "
        "check screenshot 09_participation_tab before writing the export step."
    )


async def _select_hospitality_sector(page: Page) -> None:
    """Click the Sector filter and choose Hospitality.

    2026-09-21: confirmed live (John) that the placeholder text goes from
    "Sector" to "Hospitality" (class gains "selected") once chosen. The
    dropdown option list itself was not captured, so this reuses the
    role='option' pattern already proven live for the period combobox
    elsewhere in this file.
    """
    sector: Locator = page.locator(Sel.SECTOR_SELECT_PLACEHOLDER).filter(has_text="Sector").first
    await sector.wait_for(state="visible", timeout=10_000)
    await sector.click()
    await page.locator("[role='option']:has-text('Hospitality')").first.click()


async def open_market_report(page: Page, search_term: str, report_title: str) -> None:
    """Navigate to one Market Insights report and click it.

    2026-09-21: confirmed live via DevTools (John) through the click on the
    report title -- Markets tab, Market Insights tab, Sector filter, and the
    "Search Title or Keyword" box all use stable, non-hashed selectors. What
    happens after the title click (a new tab with a download button, a
    direct browser download, an inline PDF viewer) was not observed, so this
    stops at a screenshot rather than guessing at download-handling code,
    which is exactly how the earlier attempt produced 24-byte corrupt files.
    """
    print(f"    Navigating to Markets > Market Insights for '{report_title}'...")
    await page.wait_for_selector(Sel.MARKETS_TAB, timeout=15_000)
    await page.click(Sel.MARKETS_TAB)
    await page.wait_for_selector(Sel.MARKET_INSIGHTS_TAB, timeout=15_000)
    await page.click(Sel.MARKET_INSIGHTS_TAB)
    await _screenshot(page, "10_market_insights")

    await _select_hospitality_sector(page)
    await _screenshot(page, "11_sector_hospitality")

    search_box: Locator = page.locator(Sel.REPORT_SEARCH_INPUT).first
    await search_box.wait_for(state="visible", timeout=10_000)
    await search_box.fill(search_term)
    await _screenshot(page, f"12_report_search_{search_term.replace(' ', '_')}")

    result: Locator = page.locator(f"text={report_title}").first
    await result.wait_for(state="visible", timeout=15_000)
    await result.click()
    await _screenshot(page, "13_report_clicked")
    print("    Report clicked. Download-handling not yet written -- see "
          "screenshot 13_report_clicked to map what happens next.")


async def export_market_report_pdf(page: Page, search_term: str, report_title: str) -> None:
    raise NotImplementedError(
        "Navigation to the report is now mapped (see open_market_report(), "
        "confirmed live 2026-09-21: Markets -> Market Insights -> Sector "
        "Hospitality -> search -> click). What happens after the click is not "
        "yet observed. Run open_market_report() for each of the three reports "
        "and check screenshot 13_report_clicked before writing the actual "
        "save step."
    )


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main() -> None:
    _fail_if_no_credentials()

    print("=" * 60)
    print("  CoStar Portal Fetch (headless) — Daily + Monthly")
    print("=" * 60)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            await login(page)
            await navigate_to_vdp_select(page)
            print("[5] Checking for composite popup...")
            await dismiss_popup(page)
            await navigate_to_analytics_data(page)

            print("\n-- Daily Export --")
            await export_for_period(page, "Daily", OUTPUT_DIR / f"daily_{TODAY_TAG}.xlsx")

            print("\n-- Monthly Export --")
            await export_for_period(page, "Monthly", OUTPUT_DIR / f"monthly_{TODAY_TAG}.xlsx")

            print("\n" + "=" * 60)
            print("  Daily and Monthly exports complete.")
            print("  Segmentation, participation, and the PDF market report")
            print("  were NOT attempted (navigation not yet mapped).")
            print("=" * 60)

        except Exception as exc:
            print(f"\n[ERROR] {exc}")
            await _screenshot(page, "99_error_final_state")
            raise
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
