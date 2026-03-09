"""
STR / CoStar browser automation script.

Two ways to run this:
  1. Via Claude in Chrome MCP (recommended, already active):
     Just tell Claude: "automate my STR workflow"
     Claude will use the Claude-in-Chrome MCP tools directly in your browser.

  2. Standalone Playwright (headless/CI):
     pip install playwright && playwright install chromium
     python scripts/str_playwright_automation.py

Downloads are saved to:  downloads/

Credentials:
  Set env vars COSTAR_USER and COSTAR_PASS, or fill the constants below.
  Never commit real credentials to git.
"""

import asyncio
import os
from pathlib import Path
from playwright.async_api import async_playwright, Locator, Page


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOGIN_URL  = "https://product.costar.com"
USERNAME   = os.getenv("COSTAR_USER", "your_username")   # prefer env var
PASSWORD   = os.getenv("COSTAR_PASS", "your_password")   # prefer env var
DOWNLOADS  = Path("downloads")


# ---------------------------------------------------------------------------
# Selectors — sourced from live DOM inspection, not generic heuristics.
# Update only when CoStar changes its markup.
# ---------------------------------------------------------------------------

class Sel:
    # ── Login ────────────────────────────────────────────────────────────────
    # Standard CoStar SSO form (product.costar.com/login or SSO redirect)
    USERNAME_INPUT  = "input[type='email'], input[name='username'], input[name='email']"
    PASSWORD_INPUT  = "input[type='password']"
    SUBMIT_BTN      = "button[type='submit']"
    POST_LOGIN_MARK = "span[role='tab'][content='Properties']"  # confirms dashboard loaded

    # ── Properties tab (top nav) ─────────────────────────────────────────────
    # <span class="csg-tui-text css-uui-1ckdt8n" content="Properties" role="tab">
    PROPERTIES_TAB  = "span[role='tab'][content='Properties']"

    # ── Save button (opens saved-search drawer) ───────────────────────────────
    # <span class="placeholder-normal">Save</span>
    SAVE_BTN        = "span.placeholder-normal"        # filter has_text="Save" at call site

    # ── VDP Select saved search ───────────────────────────────────────────────
    # <a class="placards-alley__survey-name--G63Pv">Saved Search: VDP Select</a>
    VDP_SELECT_LINK = "a.placards-alley__survey-name--G63Pv"  # filter has_text="VDP Select"

    # ── Composite-data popup dismiss ──────────────────────────────────────────
    # <button class="csg-tui-button css-yhbmqs"><span>Okay, got it</span></button>
    OK_GOT_IT_BTN   = "button.csg-tui-button"          # filter has inner span "Okay, got it"

    # ── Analytics sub-tab ────────────────────────────────────────────────────
    # No unique class provided; use role + text (robust enough, only one Analytics tab)
    ANALYTICS_TAB   = "[role='tab']:has-text('Analytics')"

    # ── Data sub-tab ─────────────────────────────────────────────────────────
    DATA_TAB        = "[role='tab']:has-text('Data')"

    # ── Period combobox (Daily / Monthly) ────────────────────────────────────
    # <input id="autocomplete" role="combobox" value="Monthly">
    PERIOD_COMBOBOX = "input#autocomplete[role='combobox']"

    # ── Export trigger button ─────────────────────────────────────────────────
    # <span class="placeholder-normal">Export</span>
    EXPORT_BTN      = "span.placeholder-normal"        # filter has_text="Export" at call site

    # ── Data Export menu item (dropdown that appears after clicking Export) ───
    # <div role="menuitem" class="more-menu__item--drqqC">Data Export</div>
    DATA_EXPORT_ITEM = "div.more-menu__item--drqqC[role='menuitem']"  # filter has_text="Data Export"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

async def _click_span_with_text(page: Page, selector: str, text: str, timeout: int = 10_000) -> None:
    """Click the first element matching selector whose visible text equals `text`."""
    loc: Locator = page.locator(selector).filter(has_text=text).first
    await loc.wait_for(state="visible", timeout=timeout)
    await loc.click()


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------

async def login(page: Page) -> None:
    """
    Navigate to CoStar and log in.
    Skip (or call selectively) if already authenticated via Claude in Chrome MCP.
    """
    print("[1/6] Navigating to CoStar login...")
    await page.goto(LOGIN_URL, wait_until="networkidle")

    # Fill credentials
    await page.fill(Sel.USERNAME_INPUT, USERNAME)
    await page.fill(Sel.PASSWORD_INPUT, PASSWORD)
    await page.click(Sel.SUBMIT_BTN)

    # Wait until the Properties tab appears — confirms we're on the dashboard
    await page.wait_for_selector(Sel.POST_LOGIN_MARK, timeout=30_000)
    print("    ✓ Logged in — dashboard loaded.")


async def navigate_to_vdp_select(page: Page) -> None:
    """
    Click the Properties tab, open the Save drawer, then load the VDP Select saved search.
    Properties tab → Save → 'Saved Search: VDP Select'
    """
    print("[2/6] Clicking Properties tab...")
    await page.wait_for_selector(Sel.PROPERTIES_TAB, timeout=15_000)
    await page.click(Sel.PROPERTIES_TAB)
    print("    ✓ Properties tab active.")

    print("[3/6] Opening Save drawer...")
    # span.placeholder-normal filtered to text "Save"
    await _click_span_with_text(page, Sel.SAVE_BTN, "Save")
    print("    ✓ Save drawer opened.")

    print("[4/6] Selecting 'VDP Select' saved search...")
    # a.placards-alley__survey-name--G63Pv filtered to text containing "VDP Select"
    vdp_link: Locator = page.locator(Sel.VDP_SELECT_LINK).filter(has_text="VDP Select").first
    await vdp_link.wait_for(state="visible", timeout=15_000)
    await vdp_link.click()
    print("    ✓ VDP Select loaded.")


async def dismiss_popup(page: Page) -> None:
    """
    Dismiss the composite-data info popup if it appears.
    button.csg-tui-button containing span text "Okay, got it"
    Safe to call even if the popup is absent (timeout → silently skips).
    """
    print("[5/6] Checking for composite popup...")
    try:
        # button.csg-tui-button that has an inner span reading "Okay, got it"
        ok_btn: Locator = page.locator(Sel.OK_GOT_IT_BTN).filter(
            has=page.locator("span", has_text="Okay, got it")
        ).first
        await ok_btn.wait_for(state="visible", timeout=5_000)
        await ok_btn.click()
        print("    ✓ Popup dismissed.")
    except Exception:
        print("    – No popup found, continuing.")


async def navigate_to_analytics_data(page: Page) -> None:
    """
    Navigate to Analytics → Data sub-tabs within the VDP Select view.
    """
    print("[6/6a] Clicking Analytics tab...")
    await page.wait_for_selector(Sel.ANALYTICS_TAB, timeout=15_000)
    await page.click(Sel.ANALYTICS_TAB)
    print("    ✓ Analytics tab active.")

    print("[6/6b] Clicking Data sub-tab...")
    await page.wait_for_selector(Sel.DATA_TAB, timeout=15_000)
    await page.click(Sel.DATA_TAB)
    print("    ✓ Data tab active.")


async def export_for_period(
    page: Page,
    period: str,           # "Daily" or "Monthly"
    output_path: Path,
) -> None:
    """
    Set the period combobox to `period`, click Export → Data Export,
    and capture the downloaded file to `output_path`.

    Selectors used:
      Period combobox : input#autocomplete[role="combobox"]
      Export button   : span.placeholder-normal (text "Export")
      Data Export item: div.more-menu__item--drqqC[role="menuitem"] (text "Data Export")
    """
    print(f"    Setting period to '{period}'...")
    combobox: Locator = page.locator(Sel.PERIOD_COMBOBOX)
    await combobox.wait_for(state="visible", timeout=10_000)
    await combobox.triple_click()          # select all existing text
    await combobox.fill(period)
    # Pick the matching option from the dropdown list that appears
    await page.locator(f"[role='option']:has-text('{period}')").first.click()
    print(f"    ✓ Period set to '{period}'.")

    print(f"    Clicking Export for {period}...")
    # span.placeholder-normal filtered to text "Export"
    await _click_span_with_text(page, Sel.EXPORT_BTN, "Export")

    print(f"    Clicking 'Data Export' menu item...")
    # div.more-menu__item--drqqC[role="menuitem"] filtered to text "Data Export"
    data_export: Locator = page.locator(Sel.DATA_EXPORT_ITEM).filter(has_text="Data Export").first
    await data_export.wait_for(state="visible", timeout=10_000)

    async with page.expect_download(timeout=60_000) as dl_info:
        await data_export.click()

    download = await dl_info.value
    output_path.parent.mkdir(parents=True, exist_ok=True)
    await download.save_as(str(output_path))
    print(f"    ✓ {period} CSV saved → {output_path}")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main() -> None:
    print("=" * 54)
    print("  CoStar STR Export Automation")
    print("=" * 54)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,          # visible window so you can watch
            downloads_path=str(DOWNLOADS),
        )
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()

        try:
            # ── Authentication ────────────────────────────────────────────
            # Comment out login() if running via Claude in Chrome MCP
            # (you're already authenticated in that session).
            await login(page)

            # ── Navigate to VDP Select ────────────────────────────────────
            await navigate_to_vdp_select(page)
            await dismiss_popup(page)

            # ── Analytics → Data ──────────────────────────────────────────
            await navigate_to_analytics_data(page)

            # ── Daily export ──────────────────────────────────────────────
            print("\n── Daily Export ─────────────────────────────────────")
            await export_for_period(
                page,
                period="Daily",
                output_path=DOWNLOADS / "str_daily.csv",
            )

            # ── Monthly export ────────────────────────────────────────────
            print("\n── Monthly Export ───────────────────────────────────")
            await export_for_period(
                page,
                period="Monthly",
                output_path=DOWNLOADS / "str_monthly.csv",
            )

            print("\n" + "=" * 54)
            print("  ✓ Both exports complete.")
            print("=" * 54)

        except Exception as exc:
            print(f"\n[ERROR] {exc}")
            screenshot = Path("logs") / "error_screenshot.png"
            screenshot.parent.mkdir(exist_ok=True)
            await page.screenshot(path=str(screenshot))
            print(f"  Screenshot saved → {screenshot}")
            raise

        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
