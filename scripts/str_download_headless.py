# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
Headless CoStar/STR export downloader.
Downloads Daily and Monthly STR data exports → data/str/str_daily.xlsx + str_monthly.xlsx
"""

from __future__ import annotations

import asyncio
import os
import shutil
import urllib.parse
from pathlib import Path
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout

USERNAME  = os.getenv("COSTAR_USER", "")
PASSWORD  = os.getenv("COSTAR_PASS", "")
LOGIN_URL = "https://product.costar.com"
DL_DIR    = Path("downloads")
STR_DIR   = Path("data/str")

# Pull proxy config from environment (set by Claude Code runtime)
def _get_proxy_config() -> dict | None:
    raw = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or ""
    if not raw:
        return None
    p = urllib.parse.urlparse(raw)
    cfg = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        cfg["username"] = urllib.parse.unquote(p.username)
    if p.password:
        cfg["password"] = urllib.parse.unquote(p.password)
    return cfg


async def _dismiss_popup(page: Page) -> None:
    """Dismiss CoStar modal popups — handles both button and span[role=button]."""
    for sel in [
        "span[content='Okay, got it'][role='button']",
        "span[role='button']:has-text('Okay, got it')",
        "button:has-text('Okay, got it')",
        "button:has-text('Got it')",
        "button:has-text('Dismiss')",
    ]:
        try:
            btn = page.locator(sel).first
            await btn.wait_for(state="visible", timeout=2_000)
            await btn.click(force=True)
            log(f"  Dismissed popup via: {sel}")
            await page.wait_for_timeout(500)
            return
        except PWTimeout:
            pass


def log(msg: str) -> None:
    import datetime
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


async def screenshot(page: Page, name: str) -> None:
    p = Path("logs") / f"{name}.png"
    p.parent.mkdir(exist_ok=True)
    await page.screenshot(path=str(p), full_page=True)
    log(f"  Screenshot → {p}")


async def login(page: Page) -> bool:
    log("[1/5] Navigating to CoStar login...")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(2000)
    await screenshot(page, "01_landing")

    # CoStar login can be multi-step: email → continue → password
    # Try filling email
    try:
        email_sel = "input[type='email'], input[name='username'], input[name='email'], input[placeholder*='email' i], input[placeholder*='user' i]"
        await page.wait_for_selector(email_sel, timeout=10_000)
        await page.fill(email_sel, USERNAME)
        log(f"  Filled email: {USERNAME}")

        # Click continue/next if present before password field appears
        try:
            next_btn = page.locator("button[type='submit'], button:has-text('Continue'), button:has-text('Next')").first
            await next_btn.wait_for(state="visible", timeout=5_000)
            await next_btn.click()
            await page.wait_for_timeout(2000)
            log("  Clicked Continue/Next")
        except PWTimeout:
            pass

        # Now fill password
        pw_sel = "input[type='password']"
        await page.wait_for_selector(pw_sel, timeout=10_000)
        await page.fill(pw_sel, PASSWORD)
        log("  Filled password")

        await screenshot(page, "02_credentials_filled")

        # Submit
        submit = page.locator("button[type='submit']").last
        await submit.click()
        log("  Clicked login submit")

        # Wait for post-login indicator (broad set of possible selectors)
        post_login = "[role='tab'], nav, .dashboard, [class*='dashboard'], [class*='nav']"
        await page.wait_for_selector(post_login, timeout=30_000)
        await page.wait_for_timeout(2000)
        await screenshot(page, "03_post_login")
        log("  ✓ Logged in")
        return True

    except Exception as exc:
        await screenshot(page, "02_login_error")
        log(f"  ✗ Login failed: {exc}")
        return False


async def find_str_section(page: Page) -> bool:
    """Navigate to the STR/Analytics data section."""
    log("[2/5] Looking for STR / Analytics section...")
    await screenshot(page, "04_after_login")

    # Step 1: Click Properties tab (top nav)
    try:
        props = page.locator("span[role='tab'][content='Properties'], [role='tab']:has-text('Properties')").first
        await props.wait_for(state="visible", timeout=15_000)
        await props.click()
        await page.wait_for_timeout(2000)
        log("  ✓ Properties tab clicked")
    except Exception as exc:
        log(f"  – Properties tab not found: {exc}")

    await screenshot(page, "05_properties_tab")

    # Step 2: Click Save button to open saved-search drawer
    try:
        save_btn = page.locator("span.placeholder-normal").filter(has_text="Save").first
        await save_btn.wait_for(state="visible", timeout=10_000)
        await save_btn.click()
        await page.wait_for_timeout(1500)
        log("  ✓ Save drawer opened")
    except Exception as exc:
        log(f"  – Save button not found: {exc}")

    # Step 3: Click VDP Select saved search
    try:
        vdp = page.locator("div[title='VDP Select']").first
        await vdp.wait_for(state="visible", timeout=10_000)
        await vdp.click()
        await page.wait_for_timeout(3000)
        log("  ✓ VDP Select loaded")
        await screenshot(page, "06_vdp_select")
    except PWTimeout:
        log("  – VDP Select not found by direct click, continuing...")

    # Step 4: Dismiss popup (before Analytics click)
    await _dismiss_popup(page)

    # Step 5: Click Analytics tab — icon-only (SVG bar chart), no text label
    try:
        analytics = page.locator(
            "[role='tab']:has(.analytics_component_svg__setStroke), "
            "button:has(.analytics_component_svg__setStroke), "
            "i:has(.analytics_component_svg__setStroke)"
        ).first
        await analytics.wait_for(state="visible", timeout=15_000)
        await analytics.click()
        await page.wait_for_timeout(3000)
        log("  ✓ Analytics tab clicked")
        await screenshot(page, "05b_analytics_tab")
    except Exception as exc:
        log(f"  ✗ Analytics tab error: {exc}")
        await screenshot(page, "05b_analytics_tab_err")

    # Step 5b: Dismiss popup that appears after Analytics click
    await _dismiss_popup(page)

    # Step 6: Click Data sub-tab — use span[title='Data'] (exact DOM match)
    try:
        data_tab = page.locator("span[title='Data'], [role='tab']:has-text('Data')").first
        await data_tab.wait_for(state="visible", timeout=15_000)
        await data_tab.click()
        await page.wait_for_timeout(3000)
        log("  ✓ Data tab clicked")
    except Exception as exc:
        log(f"  ✗ Data tab error: {exc}")
        await screenshot(page, "06b_data_tab_err")

    # Step 6b: Dismiss any popup after Data tab click too
    await _dismiss_popup(page)

    await screenshot(page, "07_data_tab")
    return True


async def export_period(page: Page, period: str, dest: Path) -> bool:
    """Set period combobox, click Export → Data Export, capture download."""
    log(f"[{period}] Checking period combobox...")
    try:
        combo = page.locator("input#autocomplete[role='combobox'], input[role='combobox']").first
        await combo.wait_for(state="visible", timeout=10_000)

        current = await combo.get_attribute("value") or ""
        log(f"  Combobox current value: '{current}'")

        if current.strip().lower() != period.lower():
            # Need to change period — click to open dropdown
            await combo.click()
            await page.wait_for_timeout(800)
            # Try option by role first, then by text match in list items
            option = page.locator(f"[role='option']:has-text('{period}'), li:has-text('{period}'), div[class*='option']:has-text('{period}')").first
            try:
                await option.wait_for(state="visible", timeout=5_000)
                await option.click()
                await page.wait_for_timeout(1000)
                log(f"  ✓ Period changed to '{period}'")
            except PWTimeout:
                # Dropdown didn't show options — press Escape and continue anyway
                await page.keyboard.press("Escape")
                log(f"  – Could not find option '{period}' in dropdown, continuing with current value")
        else:
            log(f"  ✓ Period already set to '{period}' — skipping selection")

    except Exception as exc:
        log(f"  ✗ Combobox error: {exc}")
        await screenshot(page, f"err_{period.lower()}_combobox")
        return False

    log(f"[{period}] Clicking Export button (force)...")
    try:
        export_btn = page.locator("button[automation-id='toolbar-button-export']").first
        await export_btn.wait_for(timeout=10_000)  # just wait for it to exist, not visible
        await export_btn.click(force=True)          # force bypasses hidden check
        await page.wait_for_timeout(1000)
        log(f"  ✓ Export button clicked")
    except Exception as exc:
        log(f"  ✗ Export button error: {exc}")
        await screenshot(page, f"err_{period.lower()}_export_btn")
        return False

    log(f"[{period}] Clicking 'Data Export' menu item...")
    try:
        data_export = page.locator("div.more-menu__item--drqqC[role='menuitem']").filter(has_text="Data Export").first
        await data_export.wait_for(state="visible", timeout=10_000)

        async with page.expect_download(timeout=90_000) as dl_info:
            await data_export.click()

        download = await dl_info.value
        suggested = download.suggested_filename or f"str_{period.lower()}.xlsx"
        dest.parent.mkdir(parents=True, exist_ok=True)
        await download.save_as(str(dest))
        log(f"  ✓ {period} export saved → {dest}  (original: {suggested})")
        return True

    except Exception as exc:
        log(f"  ✗ Download failed for {period}: {exc}")
        await screenshot(page, f"err_{period.lower()}_download")
        return False


async def main() -> None:
    if not USERNAME or not PASSWORD:
        print("ERROR: COSTAR_USER and COSTAR_PASS must be set.")
        return

    DL_DIR.mkdir(exist_ok=True)
    STR_DIR.mkdir(parents=True, exist_ok=True)

    log("=" * 52)
    log("  CoStar/STR Headless Download")
    log("=" * 52)

    async with async_playwright() as pw:
        proxy_cfg = _get_proxy_config()
        if proxy_cfg:
            log(f"  Using proxy: {proxy_cfg['server']}")

        # headless=True is blocked by Akamai bot detection on CoStar.
        # Run headed (visible window) locally; on CI/server use xvfb-run.
        headless = os.getenv("PLAYWRIGHT_HEADLESS", "0") == "1"
        browser = await pw.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            downloads_path=str(DL_DIR),
            proxy=proxy_cfg,
        )
        context = await browser.new_context(
            accept_downloads=True,
            ignore_https_errors=True,
            viewport={"width": 1440, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            proxy=proxy_cfg,
        )
        page = await context.new_page()

        try:
            ok = await login(page)
            if not ok:
                log("Aborting — login failed. Check logs/ screenshots.")
                return

            await find_str_section(page)

            daily_dl   = DL_DIR / "str_daily_new.xlsx"
            monthly_dl = DL_DIR / "str_monthly_new.xlsx"

            log("\n── Daily Export ──────────────────────────────────")
            daily_ok = await export_period(page, "Daily",   daily_dl)

            log("\n── Monthly Export ────────────────────────────────")
            monthly_ok = await export_period(page, "Monthly", monthly_dl)

            # Move to canonical data/str/ location
            if daily_ok and daily_dl.exists():
                shutil.copy2(daily_dl, STR_DIR / "str_daily.xlsx")
                log(f"  ✓ Copied daily   → {STR_DIR}/str_daily.xlsx")

            if monthly_ok and monthly_dl.exists():
                shutil.copy2(monthly_dl, STR_DIR / "str_monthly.xlsx")
                log(f"  ✓ Copied monthly → {STR_DIR}/str_monthly.xlsx")

            log("\n" + "=" * 52)
            if daily_ok and monthly_ok:
                log("  ✓ Both exports complete. Run pipeline next.")
            else:
                log("  ⚠ One or more exports failed. Check logs/ screenshots.")
            log("=" * 52)

        except Exception as exc:
            log(f"[ERROR] {exc}")
            await screenshot(page, "fatal_error")
            raise
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
