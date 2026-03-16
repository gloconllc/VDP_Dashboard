"""
Datafy platform browser automation script.

Downloads PDF reports from three sections of platform.datafy.com:
  • Report Builder
  • Attribution Insights
  • Social Analytics

Usage
-----
  # Normal run (downloads PDFs):
  python scripts/datafy_playwright_automation.py

  # DOM inspector — logs in, dumps selectors + screenshots, then exits.
  # Run this first on a fresh machine to confirm/update the Sel class.
  python scripts/datafy_playwright_automation.py --inspect

Requirements
------------
  pip install playwright python-dotenv
  playwright install chromium

Credentials
-----------
  Loaded automatically from <project-root>/.env:
    DATAFY_USERNAME=john.picou@gloconsolutions.com
    DATAFY_PASSWORD=Wjp1121!@
  Never commit .env to git.

Downloads saved to:  downloads/datafy/
Screenshots saved to: logs/datafy/
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Locator, Page


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_ROOT     = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")

LOGIN_URL  = "https://platform.datafy.com"
USERNAME   = os.getenv("DATAFY_USERNAME", "")
PASSWORD   = os.getenv("DATAFY_PASSWORD", "")
DOWNLOADS  = _ROOT / "downloads" / "datafy"
LOGS       = _ROOT / "logs" / "datafy"
_STAMP     = datetime.now().strftime("%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
# Selectors
# ---------------------------------------------------------------------------
# These were derived from DOM inspection of platform.datafy.com.
# Run `python scripts/datafy_playwright_automation.py --inspect` to re-dump
# the DOM if the site updates its markup.
# ---------------------------------------------------------------------------

class Sel:
    # ── Login form ───────────────────────────────────────────────────────────
    USERNAME_INPUT   = "input[type='email'], input[name='email'], input[name='username'], input[placeholder*='email' i], input[placeholder*='username' i]"
    PASSWORD_INPUT   = "input[type='password']"
    SUBMIT_BTN       = "button[type='submit'], input[type='submit'], button:has-text('Sign in'), button:has-text('Log in'), button:has-text('Login')"

    # Landmark that confirms a successful login (first recognisable post-login element).
    # Update this to a nav item, avatar, or dashboard heading that always appears.
    POST_LOGIN_MARK  = "nav, header, [data-testid='app-shell'], .sidebar, [class*='nav'], [class*='sidebar'], [class*='dashboard']"

    # ── Top-level navigation ─────────────────────────────────────────────────
    # Adjust has-text() values if the link labels differ on the live site.
    NAV_REPORT_BUILDER  = "a:has-text('Report Builder'), [role='menuitem']:has-text('Report Builder'), li:has-text('Report Builder'), span:has-text('Report Builder')"
    NAV_ATTRIBUTION     = "a:has-text('Attribution Insights'), [role='menuitem']:has-text('Attribution Insights'), li:has-text('Attribution Insights'), span:has-text('Attribution Insights')"
    NAV_SOCIAL          = "a:has-text('Social Analytics'), [role='menuitem']:has-text('Social Analytics'), li:has-text('Social Analytics'), span:has-text('Social Analytics')"

    # ── Export / Download flow ───────────────────────────────────────────────
    EXPORT_BTN           = "button:has-text('Export'), button:has-text('Download'), a:has-text('Export'), a:has-text('Download')"
    PDF_OPTION           = "[role='option']:has-text('PDF'), button:has-text('PDF'), a:has-text('PDF'), li:has-text('PDF')"
    CONFIRM_DOWNLOAD_BTN = "button:has-text('Download'), button:has-text('Generate'), button:has-text('Export'), button[type='submit']"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _screenshot(page: Page, label: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    path = LOGS / f"{_STAMP}_{label}.png"
    await page.screenshot(path=str(path), full_page=True)
    print(f"  [screenshot] → {path.relative_to(_ROOT)}")


async def _click(page: Page, selector: str, label: str, timeout: int = 15_000) -> None:
    loc: Locator = page.locator(selector).first
    await loc.wait_for(state="visible", timeout=timeout)
    await loc.click()
    print(f"    ✓ {label}")


async def _dump_dom(page: Page, label: str) -> None:
    """Print inputs, buttons, links and a body snippet to stdout."""
    sep = "─" * 60
    print(f"\n{sep}\nDOM DUMP — {label}\nURL: {page.url}\n{sep}")

    print("\n── INPUTS ──")
    for el in await page.locator("input:visible").all():
        attrs = {}
        for a in ["type", "name", "id", "placeholder", "aria-label", "class", "value"]:
            v = await el.get_attribute(a)
            if v:
                attrs[a] = v[:80]
        print(" ", attrs)

    print("\n── BUTTONS ──")
    for el in await page.locator("button:visible").all():
        attrs = {}
        for a in ["type", "id", "class", "aria-label", "data-testid"]:
            v = await el.get_attribute(a)
            if v:
                attrs[a] = v[:80]
        text = (await el.inner_text()).strip()[:60]
        if text:
            attrs["text"] = text
        print(" ", attrs)

    print("\n── LINKS (first 30) ──")
    for el in await page.locator("a:visible").all()[:30]:
        href  = (await el.get_attribute("href") or "")[:80]
        text  = (await el.inner_text()).strip()[:60]
        cls   = (await el.get_attribute("class") or "")[:60]
        print(f"  text={text!r:40s}  href={href!r:50s}  class={cls!r}")

    print("\n── NAV / MENU ITEMS ──")
    for el in await page.locator("[role='menuitem']:visible, [role='tab']:visible, nav a:visible").all()[:20]:
        text = (await el.inner_text()).strip()[:80]
        cls  = (await el.get_attribute("class") or "")[:60]
        role = await el.get_attribute("role") or ""
        print(f"  role={role!r:12s}  text={text!r:40s}  class={cls!r}")

    print("\n── BODY HTML (first 3000 chars) ──")
    body = await page.locator("body").inner_html()
    print(body[:3000])
    print(sep + "\n")

    await _screenshot(page, label.replace(" ", "_").lower())


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------

async def login(page: Page) -> None:
    if not USERNAME or not PASSWORD:
        raise EnvironmentError(
            "DATAFY_USERNAME and DATAFY_PASSWORD must be set in .env\n"
            f"  .env path checked: {_ROOT / '.env'}"
        )

    print(f"[1] Navigating to {LOGIN_URL} ...")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)

    # Handle possible HTTP-dialog auth (rare on modern SaaS, but seen on staging)
    page.on("dialog", lambda d: asyncio.ensure_future(d.dismiss()))

    print("    Filling credentials...")
    await page.fill(Sel.USERNAME_INPUT, USERNAME)
    await page.fill(Sel.PASSWORD_INPUT, PASSWORD)
    await page.click(Sel.SUBMIT_BTN)

    print("    Waiting for post-login landmark...")
    await page.wait_for_selector(Sel.POST_LOGIN_MARK, timeout=30_000)
    print(f"    ✓ Logged in  →  {page.url}")


async def _navigate_and_download(
    page: Page,
    step: str,
    nav_selector: str,
    nav_label: str,
    filename: str,
    inspect: bool,
) -> None:
    print(f"\n[{step}] {nav_label}")
    await _click(page, nav_selector, f"Navigated to {nav_label}", timeout=20_000)
    await page.wait_for_load_state("networkidle", timeout=30_000)

    if inspect:
        await _dump_dom(page, nav_label)
        return

    output = DOWNLOADS / filename
    output.parent.mkdir(parents=True, exist_ok=True)

    # Open export dropdown / dialog
    await _click(page, Sel.EXPORT_BTN, "Export button opened")

    # Select PDF if a format picker appears; skip silently if not
    try:
        await _click(page, Sel.PDF_OPTION, "PDF format selected", timeout=6_000)
    except Exception:
        print("    – No PDF picker visible; proceeding to download trigger.")

    # Capture the file download
    async with page.expect_download(timeout=90_000) as dl_info:
        await _click(page, Sel.CONFIRM_DOWNLOAD_BTN, "Download triggered")

    dl = await dl_info.value
    await dl.save_as(str(output))
    print(f"    ✓ Saved → downloads/datafy/{filename}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(inspect: bool) -> None:
    mode = "DOM INSPECTOR" if inspect else "PDF Download"
    print("=" * 54)
    print(f"  Datafy Automation — {mode}")
    print(f"  Run: {_STAMP}")
    print("=" * 54)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=not inspect,   # headed in inspect mode so you can watch
            downloads_path=str(DOWNLOADS),
        )
        context = await browser.new_context(accept_downloads=True)
        page    = await context.new_page()

        try:
            await login(page)

            if inspect:
                await _dump_dom(page, "post_login")

            await _navigate_and_download(
                page, "2", Sel.NAV_REPORT_BUILDER, "Report Builder",
                f"datafy_report_builder_{_STAMP}.pdf", inspect,
            )
            await _navigate_and_download(
                page, "3", Sel.NAV_ATTRIBUTION, "Attribution Insights",
                f"datafy_attribution_insights_{_STAMP}.pdf", inspect,
            )
            await _navigate_and_download(
                page, "4", Sel.NAV_SOCIAL, "Social Analytics",
                f"datafy_social_analytics_{_STAMP}.pdf", inspect,
            )

            if inspect:
                print("\n✓ Inspector run complete.")
                print("  Share the output above (and logs/datafy/*.png) so selectors can be locked in.")
            else:
                print("\n" + "=" * 54)
                print("  ✓ All three PDFs downloaded.")
                print(f"  Location: {DOWNLOADS.relative_to(_ROOT)}/")
                print("=" * 54)

        except Exception as exc:
            print(f"\n[ERROR] {exc}")
            await _screenshot(page, "error")
            raise

        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Datafy PDF automation")
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Dump DOM + screenshots at each step to help confirm selectors.",
    )
    args = parser.parse_args()
    asyncio.run(main(inspect=args.inspect))
