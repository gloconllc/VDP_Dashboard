"""
Datafy platform browser automation script.

Downloads PDF reports from three sections:
  • Report Builder
  • Attribution Insights
  • Social Analytics

Two ways to run:
  1. Standalone (headless=False, interactive window):
       pip install playwright python-dotenv
       playwright install chromium
       python scripts/datafy_playwright_automation.py

  2. Via Claude in Chrome MCP (already authenticated):
     Tell Claude: "run the Datafy automation"
     Claude will drive the already-open browser session.

Credentials:
  Loaded from the project .env file (DATAFY_USERNAME / DATAFY_PASSWORD).
  Never commit credentials to git.

Downloads saved to:  downloads/datafy/
"""

import asyncio
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Locator, Page


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Load .env from project root (two levels up from this script)
_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(_ENV_PATH)

LOGIN_URL = "https://platform.datafy.com"
USERNAME  = os.getenv("DATAFY_USERNAME", "")
PASSWORD  = os.getenv("DATAFY_PASSWORD", "")
DOWNLOADS = Path("downloads") / "datafy"

_STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
# Selectors
# NOTE: These are best-guess selectors based on common SaaS platform patterns.
# Open DevTools on platform.datafy.com and update any that don't match.
# ---------------------------------------------------------------------------

class Sel:
    # ── Login ────────────────────────────────────────────────────────────────
    USERNAME_INPUT  = "input[type='email'], input[name='email'], input[name='username']"
    PASSWORD_INPUT  = "input[type='password']"
    SUBMIT_BTN      = "button[type='submit']"
    # Element that confirms a successful login (update to a real post-login landmark)
    POST_LOGIN_MARK = "nav, [data-testid='dashboard'], .sidebar, .main-nav"

    # ── Main navigation items ────────────────────────────────────────────────
    # Update these text values to match the exact nav labels in Datafy
    NAV_REPORT_BUILDER    = "a:has-text('Report Builder'), [role='menuitem']:has-text('Report Builder')"
    NAV_ATTRIBUTION       = "a:has-text('Attribution Insights'), [role='menuitem']:has-text('Attribution Insights')"
    NAV_SOCIAL            = "a:has-text('Social Analytics'), [role='menuitem']:has-text('Social Analytics')"

    # ── Report/export triggers ───────────────────────────────────────────────
    # Button or link that opens the export/download dialog
    EXPORT_BTN            = "button:has-text('Export'), button:has-text('Download'), a:has-text('Export')"
    # PDF option inside the export dropdown or dialog
    PDF_OPTION            = "[role='option']:has-text('PDF'), button:has-text('PDF'), a:has-text('PDF')"
    # Final confirm/download button inside the export dialog
    CONFIRM_DOWNLOAD_BTN  = "button:has-text('Download'), button:has-text('Generate'), button[type='submit']"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _click(page: Page, selector: str, label: str, timeout: int = 15_000) -> None:
    """Wait for selector to be visible, then click it."""
    loc: Locator = page.locator(selector).first
    await loc.wait_for(state="visible", timeout=timeout)
    await loc.click()
    print(f"    ✓ Clicked: {label}")


async def _download_pdf(page: Page, filename: str) -> Path:
    """
    Trigger the export flow (Export → PDF → Download/Generate) and
    capture the downloaded file.  Returns the saved path.
    """
    output_path = DOWNLOADS / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Open export menu
    await _click(page, Sel.EXPORT_BTN, "Export button")

    # Choose PDF
    try:
        await _click(page, Sel.PDF_OPTION, "PDF option", timeout=8_000)
    except Exception:
        print("    – PDF option not found in menu; attempting direct download trigger.")

    # Capture download
    async with page.expect_download(timeout=90_000) as dl_info:
        await _click(page, Sel.CONFIRM_DOWNLOAD_BTN, "Confirm download")

    download = await dl_info.value
    await download.save_as(str(output_path))
    print(f"    ✓ Saved → {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------

async def login(page: Page) -> None:
    """Navigate to Datafy and log in with stored credentials."""
    if not USERNAME or not PASSWORD:
        raise EnvironmentError(
            "DATAFY_USERNAME and DATAFY_PASSWORD must be set in .env "
            "or as environment variables."
        )

    print("[1/4] Navigating to Datafy login...")
    await page.goto(LOGIN_URL, wait_until="networkidle")

    await page.fill(Sel.USERNAME_INPUT, USERNAME)
    await page.fill(Sel.PASSWORD_INPUT, PASSWORD)
    await page.click(Sel.SUBMIT_BTN)

    await page.wait_for_selector(Sel.POST_LOGIN_MARK, timeout=30_000)
    print("    ✓ Logged in — dashboard loaded.")


async def download_report_builder(page: Page) -> None:
    """Navigate to Report Builder and download its PDF export."""
    print("\n[2/4] Report Builder")
    await _click(page, Sel.NAV_REPORT_BUILDER, "Report Builder nav item")
    await page.wait_for_load_state("networkidle")

    await _download_pdf(page, f"datafy_report_builder_{_STAMP}.pdf")


async def download_attribution_insights(page: Page) -> None:
    """Navigate to Attribution Insights and download its PDF export."""
    print("\n[3/4] Attribution Insights")
    await _click(page, Sel.NAV_ATTRIBUTION, "Attribution Insights nav item")
    await page.wait_for_load_state("networkidle")

    await _download_pdf(page, f"datafy_attribution_insights_{_STAMP}.pdf")


async def download_social_analytics(page: Page) -> None:
    """Navigate to Social Analytics and download its PDF export."""
    print("\n[4/4] Social Analytics")
    await _click(page, Sel.NAV_SOCIAL, "Social Analytics nav item")
    await page.wait_for_load_state("networkidle")

    await _download_pdf(page, f"datafy_social_analytics_{_STAMP}.pdf")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main() -> None:
    print("=" * 54)
    print("  Datafy Export Automation")
    print(f"  Run: {_STAMP}")
    print("=" * 54)

    DOWNLOADS.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,               # visible window so you can watch / intervene
            downloads_path=str(DOWNLOADS),
        )
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()

        try:
            await login(page)
            await download_report_builder(page)
            await download_attribution_insights(page)
            await download_social_analytics(page)

            print("\n" + "=" * 54)
            print("  ✓ All Datafy exports complete.")
            print(f"  Files saved to: {DOWNLOADS.resolve()}")
            print("=" * 54)

        except Exception as exc:
            print(f"\n[ERROR] {exc}")
            screenshot = Path("logs") / f"datafy_error_{_STAMP}.png"
            screenshot.parent.mkdir(exist_ok=True)
            await page.screenshot(path=str(screenshot))
            print(f"  Screenshot saved → {screenshot}")
            raise

        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
