"""
Browser automation for Target checkout.

Three modes:
  1. headless_login_and_get_cookies() — log in via headless Chromium and
     capture all cookies. Called on demand or when tokens expire.

  2. headless_place_order() — fully automated, invisible Chromium checkout.
     Used by the monitor when API-based CVV submission fails (ID2 token issue).

  3. open_checkout_and_click() — visible browser fallback for manual completion.
"""

import asyncio
import logging
import os
from typing import Optional

log = logging.getLogger("browser")

SCREENSHOT_PATH = "/tmp/target_checkout_debug.png"

# All known selectors for CVV input across Target's checkout UI
CVV_SELECTORS = [
    'input[data-test="cvv"]',
    'input[data-test="cvvInput"]',
    'input[data-test="security-code"]',
    'input[aria-label*="CVV"]',
    'input[aria-label*="Security code"]',
    'input[aria-label*="security code"]',
    'input[aria-label*="CVC"]',
    'input[name="cvv"]',
    'input[name="securityCode"]',
    'input[placeholder*="CVV"]',
    'input[placeholder*="CVC"]',
    'input[autocomplete="cc-csc"]',
    'input[maxlength="4"]',
    'input[maxlength="3"]',
]

PLACE_ORDER_SELECTORS = [
    'button[data-test="placeOrderButton"]',
    'button[data-test="place-order"]',
    'button[data-test="checkout-submit"]',
    'button:has-text("Place your order")',
    'button:has-text("Place order")',
    'button:has-text("Submit order")',
]


def _parse_cookie_string(cookie_str: str) -> list[dict]:
    """Parse a browser cookie string into a list of Playwright cookie dicts."""
    cookies = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        name, _, value = part.partition("=")
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        cookies.append({
            "name": name,
            "value": value,
            "domain": ".target.com",
            "path": "/",
            "secure": True,
            "httpOnly": False,
            "sameSite": "Lax",
        })
    return cookies


async def _try_fill_cvv(page, ccv: str) -> tuple[bool, str]:
    """
    Try to fill the CVV field in the main page and any iframes.
    Returns (filled: bool, detail: str).
    """
    from playwright.async_api import TimeoutError as PWTimeout

    # ── Try main page selectors ───────────────────────────────────────────
    for sel in CVV_SELECTORS:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=200):
                await loc.click()
                await loc.fill(ccv)
                log.info("Browser: filled CVV via selector: %s", sel)
                return True, f"filled via {sel}"
        except PWTimeout:
            continue
        except Exception:
            continue

    # ── Try inside iframes (Target may sandbox payment fields) ───────────
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        frame_url = frame.url or ""
        # Focus on payment-related frames
        if not any(kw in frame_url for kw in ("pay", "card", "checkout", "wallet", "target")):
            if frame_url and frame_url != "about:blank":
                continue
        for sel in CVV_SELECTORS:
            try:
                loc = frame.locator(sel).first
                if await loc.is_visible(timeout=200):
                    await loc.click()
                    await loc.fill(ccv)
                    log.info("Browser: filled CVV in iframe (%s) via: %s", frame_url, sel)
                    return True, f"filled in iframe via {sel}"
            except Exception:
                continue

    return False, "CVV field not found in page or iframes"


async def _do_login_if_needed(page, email: str, password: str, steps: list) -> bool:
    """
    Detect if a Target login modal/page is showing and complete the login flow.
    Target's login is multi-step: email → method selection → password → submit.
    Returns True if login was attempted (regardless of success), False if no modal found.
    """
    from playwright.async_api import TimeoutError as PWTimeout

    # Check for email input (login modal or page)
    email_el = page.locator('input[type="email"], input[name="username"], input[id="username"]').first
    try:
        if not await email_el.is_visible(timeout=3_000):
            return False
    except Exception:
        return False

    steps.append("login prompt detected — filling email")
    try:
        await email_el.fill(email)
    except Exception:
        return False

    # Click Continue / Submit to advance past email step
    for cont_sel in ['button:has-text("Continue")', 'button[type="submit"]']:
        try:
            btn = page.locator(cont_sel).first
            if await btn.is_visible(timeout=2_000):
                await btn.click()
                break
        except Exception:
            continue
    await asyncio.sleep(2)

    # Target shows auth method selection — click "Enter your password"
    # NOTE: these options are NOT <button> elements, use Playwright text= selector
    for method_sel in [
        'text=Enter your password',
        '[data-test="password-login-button"]',
        'button:has-text("Enter your password")',
    ]:
        try:
            btn = page.locator(method_sel).first
            if await btn.is_visible(timeout=3_000):
                await btn.click()
                steps.append(f"selected password method: {method_sel}")
                await asyncio.sleep(1)
                break
        except Exception:
            continue

    # Fill password
    pw_el = page.locator('input[type="password"]').first
    try:
        if not await pw_el.is_visible(timeout=5_000):
            steps.append("password field not visible after method selection")
            return True
        await pw_el.fill(password)
    except Exception as e:
        steps.append(f"password fill error: {e}")
        return True

    # Submit
    for submit_sel in ['button[type="submit"]', 'button:has-text("Sign in")']:
        try:
            btn = page.locator(submit_sel).first
            if await btn.is_visible(timeout=2_000):
                await btn.click()
                break
        except Exception:
            continue

    # Wait for the page to settle after login
    await asyncio.sleep(4)
    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
    except PWTimeout:
        pass
    steps.append(f"login submitted — {page.url}")
    return True


async def headless_login_and_get_cookies(email: str, password: str) -> dict:
    """
    Launch headless Chromium, log into Target with email + password,
    and return all cookies as a raw cookie string.

    Returns:
        {"success": bool, "cookies": str, "access_token": str,
         "refresh_token": str, "error": str | None}
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        return {"success": False, "cookies": "", "access_token": "",
                "refresh_token": "", "error": "playwright not installed"}

    pw = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        log.info("Headless login: navigating to target.com/account/login")
        await page.goto("https://www.target.com/account/login",
                        wait_until="domcontentloaded", timeout=30_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except PWTimeout:
            pass

        # Complete the multi-step login flow (email → method select → password → submit)
        steps: list[str] = []
        await _do_login_if_needed(page, email, password, steps)
        log.info("Headless login steps: %s", " | ".join(steps))

        # Wait for redirect away from the login page
        try:
            await page.wait_for_url(
                lambda url: "account/login" not in url,
                timeout=20_000,
            )
        except PWTimeout:
            # Check for error message
            error_text = ""
            for err_sel in ['[data-test*="error"]', '[role="alert"]']:
                try:
                    el = page.locator(err_sel).first
                    if await el.is_visible(timeout=500):
                        error_text = (await el.inner_text(timeout=500)).strip()
                        break
                except Exception:
                    pass
            return {"success": False, "cookies": "", "access_token": "",
                    "refresh_token": "",
                    "error": f"Login timed out — still on login page. {error_text}"}

        # Extract all cookies
        all_cookies = await context.cookies()
        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in all_cookies)
        access_token = next((c["value"] for c in all_cookies if c["name"] == "accessToken"), "")
        refresh_token = next((c["value"] for c in all_cookies if c["name"] == "refreshToken"), "")

        if not access_token:
            return {"success": False, "cookies": cookie_str, "access_token": "",
                    "refresh_token": "", "error": "Logged in but no accessToken found in cookies"}

        log.info("Headless login: success — captured %d cookies", len(all_cookies))
        return {"success": True, "cookies": cookie_str, "access_token": access_token,
                "refresh_token": refresh_token, "error": None}

    except Exception as exc:
        log.error("Headless login error: %s", exc)
        return {"success": False, "cookies": "", "access_token": "",
                "refresh_token": "", "error": str(exc)}
    finally:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass


async def _click_confirm_or_place_order(page, steps: list) -> None:
    """
    After CVV is filled, click the modal's Confirm button if visible,
    otherwise fall back to the Place Order button.
    """
    from playwright.async_api import TimeoutError as PWTimeout

    # Target's CVV modal has a "Confirm" button — try that first
    confirm_selectors = [
        'button:has-text("Confirm")',
        'button[data-test="confirm-cvv"]',
        'button[data-test="cvv-confirm"]',
        'button[data-test="submit-cvv"]',
    ]
    for sel in confirm_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=2_000):
                await loc.click()
                steps.append(f"clicked Confirm CVV modal: {sel}")
                return
        except PWTimeout:
            continue

    # Fall back to Place Order button
    for sel in PLACE_ORDER_SELECTORS:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=2_000):
                await loc.click()
                steps.append(f"clicked Place Order (fallback): {sel}")
                return
        except PWTimeout:
            continue

    steps.append("WARNING: could not find Confirm or Place Order button after CVV fill")


async def headless_place_order(
    checkout_cookies: str,
    access_token: str,
    visitor_id: str,
    ccv: str = "",
    email: str = "",
    password: str = "",
    tcin: str = "",
    quantity: int = 1,
    timeout_ms: int = 90_000,
    status_callback=None,
) -> dict:
    """
    Place a Target order headlessly via Playwright.

    Injects all stored cookies into a headless Chromium context,
    navigates to /checkout, fills the CVV field if present, clicks
    Place Order, and waits for the confirmation page.

    Returns:
        {"success": bool, "order_id": str | None, "error": str | None,
         "debug": str}
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        return {
            "success": False,
            "order_id": None,
            "error": "playwright not installed — run: pip3 install playwright && python3 -m playwright install chromium",
            "debug": "",
        }

    steps: list[str] = []
    pw = None
    context = None

    async def _status(msg: str) -> None:
        if status_callback:
            try:
                await status_callback(msg)
            except Exception:
                pass

    try:
        pw = await async_playwright().start()

        # ── Use a persistent browser profile so login survives between runs ──
        # The profile is stored per account email. On the first run it logs in
        # and saves the session to disk; on subsequent runs it reuses it
        # automatically — no login needed until the session actually expires.
        if email:
            safe = email.replace("@", "_at_").replace(".", "_")[:40]
            profile_dir = os.path.join(
                os.path.expanduser("~"), ".targetb0t", "profiles", safe
            )
            os.makedirs(profile_dir, exist_ok=True)
            context = await pw.chromium.launch_persistent_context(
                profile_dir,
                headless=False,
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )
            steps.append(f"persistent profile: {profile_dir}")
        else:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )

        page = await context.new_page()

        # ── Step 1: ensure logged in ──────────────────────────────────────
        # Navigate to the login page. If already logged in, Target redirects
        # to /account and _do_login_if_needed() is a no-op (no email input visible).
        await _status("logging in…")
        log.info("Headless checkout: checking login state")
        await page.goto("https://www.target.com/account/login",
                        wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(2)
        steps.append(f"login check — {page.url}")

        if email and password:
            did_login = await _do_login_if_needed(page, email, password, steps)
            if did_login:
                # Wait for redirect away from login page to confirm auth worked
                try:
                    await page.wait_for_url(
                        lambda url: "/account/login" not in url,
                        timeout=20_000,
                    )
                except PWTimeout:
                    pass
                steps.append(f"post-login — {page.url}")

        # ── Step 2: add to cart via browser (same session) ────────────────
        if tcin:
            await _status("finding product…")
            product_url = f"https://www.target.com/p/-/A-{tcin}"
            log.info("Headless checkout: navigating to product page for TCIN %s", tcin)
            await page.goto(product_url, wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(3)
            steps.append(f"product page — {page.url}")

            # First: select the Shipping fulfillment tab so we don't ATC as pickup
            shipping_tab_selectors = [
                '[data-test="fulfillmentSectionButton-SHIPIT"]',
                '[data-test="shipItButton"]',
                '[data-test="fulfillment-option-SHIPIT"]',
                'button[aria-label*="shipping" i]',
                'text=Shipping',
            ]
            for tab_sel in shipping_tab_selectors:
                try:
                    tab = page.locator(tab_sel).first
                    if await tab.is_visible(timeout=3_000):
                        await tab.click()
                        steps.append(f"selected shipping tab: {tab_sel}")
                        await asyncio.sleep(1)
                        break
                except PWTimeout:
                    continue

            await _status("adding to cart…")
            atc_clicked = False
            for sel in [
                'button[data-test="shipItButton"]',
                'button[data-test="fulfillment-add-to-cart-button"]',
                'button:has-text("Add to cart")',
                'button:has-text("Add to Cart")',
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=6_000):
                        await btn.click()
                        atc_clicked = True
                        steps.append(f"browser ATC clicked: {sel}")
                        log.info("Headless checkout: clicked Add to Cart (%s)", sel)
                        await asyncio.sleep(3)
                        break
                except PWTimeout:
                    continue

            if not atc_clicked:
                await page.screenshot(path=SCREENSHOT_PATH)
                steps.append(f"ATC button NOT found — screenshot saved | URL: {page.url}")
                log.warning("Headless checkout: Add to Cart button not found on %s", page.url)
            else:
                steps.append(f"ATC done — {page.url}")

        # ── Step 3: go to cart page, then proceed to checkout ─────────────
        # Going via /cart gives Target a natural navigation signal and lets
        # us click the "Check out" button, landing on the first checkout step.
        await _status("checking out…")
        log.info("Headless checkout: navigating to cart")
        await page.goto("https://www.target.com/cart",
                        wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(5)
        steps.append(f"cart — {page.url}")
        log.info("Headless checkout: on cart page (%s) — looking for checkout button", page.url)

        # Handle re-auth modal that Target sometimes shows on cart load
        if password:
            for method_sel in ['text=Enter your password', '[data-test="password-login-button"]']:
                try:
                    btn = page.locator(method_sel).first
                    if await btn.is_visible(timeout=1_000):
                        await btn.click()
                        await asyncio.sleep(1)
                        pw_el = page.locator('input[type="password"]').first
                        if await pw_el.is_visible(timeout=3_000):
                            await pw_el.fill(password)
                            await page.keyboard.press("Enter")
                            await asyncio.sleep(4)
                            steps.append(f"cart re-auth done — {page.url}")
                            log.info("Headless checkout: cart re-auth done — %s", page.url)
                        break
                except PWTimeout:
                    continue

        # Adjust quantity on cart page if > 1 (Target's stepper is here, not product page)
        if quantity > 1:
            _cart_qty_set = False
            _cart_inc_sels = [
                '[data-test="cart-item-qty-increment"]',
                '[data-test="increaseQty"]',
                'button[aria-label*="increase quantity" i]',
                'button[aria-label*="add one more" i]',
                'button[aria-label*="increase" i]',
                'button[aria-label*="increment" i]',
            ]
            for inc_sel in _cart_inc_sels:
                try:
                    inc_btn = page.locator(inc_sel).first
                    if await inc_btn.is_visible(timeout=2_000):
                        for _ in range(quantity - 1):
                            await inc_btn.click()
                            await asyncio.sleep(0.5)
                        steps.append(f"cart qty set to {quantity} via {inc_sel}")
                        log.info("Headless checkout: cart quantity set to %d", quantity)
                        _cart_qty_set = True
                        await asyncio.sleep(1)
                        break
                except PWTimeout:
                    continue
                except Exception:
                    continue

            # Fallback: fill the qty input directly
            if not _cart_qty_set:
                _cart_qty_inputs = [
                    'input[data-test="cart-item-qty-input"]',
                    'input[data-test="quantity-stepper-input"]',
                    'input[aria-label*="quantity" i]',
                ]
                for qty_sel in _cart_qty_inputs:
                    try:
                        qty_el = page.locator(qty_sel).first
                        if await qty_el.is_visible(timeout=2_000):
                            await qty_el.triple_click()
                            await qty_el.fill(str(quantity))
                            await page.keyboard.press("Tab")
                            await asyncio.sleep(1)
                            steps.append(f"cart qty set to {quantity} via input {qty_sel}")
                            log.info("Headless checkout: cart qty input set to %d", quantity)
                            _cart_qty_set = True
                            break
                    except PWTimeout:
                        continue
                    except Exception:
                        continue

            if not _cart_qty_set:
                steps.append(f"WARNING: could not set cart quantity to {quantity}")
                log.warning("Headless checkout: cart quantity selector not found — proceeding with qty 1")

        checkout_clicked = False
        for sel in [
            'a[data-test="checkout-button"]',
            'button[data-test="checkout-button"]',
            '[data-test="checkout-button"]',
            'a:has-text("Check out")',
            'button:has-text("Check out")',
            'a:has-text("Checkout")',
            'button:has-text("Checkout")',
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=3_000):
                    await btn.click()
                    checkout_clicked = True
                    await asyncio.sleep(4)
                    steps.append(f"checkout started: {sel} — {page.url}")
                    log.info("Headless checkout: clicked checkout — now at %s", page.url)
                    break
            except PWTimeout:
                continue

        if not checkout_clicked:
            try:
                await page.screenshot(path=SCREENSHOT_PATH)
            except Exception:
                pass
            steps.append(f"checkout button NOT found on cart — URL={page.url} — screenshot saved")
            log.warning("Headless checkout: checkout button not found on cart page (%s)", page.url)

        steps.append(f"checkout page — {page.url}")
        log.info("Headless checkout: entering checkout loop at %s", page.url)

        # ── Step 4: step through checkout steps until Place Order appears ──
        # Target uses a multi-step checkout (address → payment → review).
        # We loop: fill CVV if visible, click Place Order if visible,
        # otherwise click Continue/Next to advance.
        # All is_visible() checks use 500 ms — the loop itself retries every 2 s.
        CONTINUE_SELECTORS = [
            'button[data-test="save-address-button"]',
            'button[data-test="save-payment-button"]',
            'button[data-test="checkout-continue"]',
            'button:has-text("Save and continue")',
            'button:has-text("Save & continue")',
            'button:has-text("Continue")',
        ]
        T = 500  # ms timeout per selector — fast fail, loop retries

        filled = False
        clicked = False
        done_reauth = False
        for attempt in range(10):
            await asyncio.sleep(2)
            log.info("Headless checkout: loop attempt %d — %s", attempt, page.url)
            # Always save a screenshot so we can diagnose the current page state
            try:
                await page.screenshot(path=SCREENSHOT_PATH)
            except Exception:
                pass
            # Scroll to bottom to expose any sticky buttons
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(0.3)
            except Exception:
                pass

            # Handle Target's checkout re-auth modal — shown even when already
            # logged in. It skips the email step and jumps straight to method
            # selection ("Enter your password" / passkey / code).
            if password and not done_reauth and attempt == 0:
                await _status("verifying identity…")
            if password and not done_reauth:
                reauthed = False
                for method_sel in [
                    'text=Enter your password',
                    '[data-test="password-login-button"]',
                ]:
                    try:
                        btn = page.locator(method_sel).first
                        if await btn.is_visible(timeout=T):
                            await btn.click()
                            await asyncio.sleep(1)
                            pw_el = page.locator('input[type="password"]').first
                            if await pw_el.is_visible(timeout=3_000):
                                await pw_el.click()
                                await pw_el.fill(password)
                                await page.keyboard.press("Enter")
                                await asyncio.sleep(4)
                                steps.append(f"checkout re-auth done (step {attempt}) — {page.url}")
                                log.info("Headless checkout: re-auth done — %s", page.url)
                                reauthed = True
                                done_reauth = True
                            break
                    except PWTimeout:
                        continue

                if reauthed:
                    # After re-auth we may still be on /cart — click checkout again.
                    # Give the page extra time to settle after the auth modal closes.
                    await asyncio.sleep(3)
                    await _status("checking out…")
                    _checkout_sels = [
                        'a[data-test="checkout-button"]',
                        'button[data-test="checkout-button"]',
                        '[data-test="checkout-button"]',
                        'a:has-text("Check out")',
                        'button:has-text("Check out")',
                        'a:has-text("Checkout")',
                        'button:has-text("Checkout")',
                    ]
                    _post_reauth_clicked = False
                    for _retry in range(3):
                        for sel in _checkout_sels:
                            try:
                                btn = page.locator(sel).first
                                if await btn.is_visible(timeout=4_000):
                                    await btn.click()
                                    _post_reauth_clicked = True
                                    await asyncio.sleep(4)
                                    steps.append(f"post-reauth checkout click (try {_retry}): {sel} — {page.url}")
                                    log.info("Headless checkout: post-reauth checkout clicked — %s", page.url)
                                    break
                            except PWTimeout:
                                continue
                        if _post_reauth_clicked:
                            break
                        # Button not found yet — take a screenshot and wait before retry
                        try:
                            await page.screenshot(path=SCREENSHOT_PATH)
                        except Exception:
                            pass
                        steps.append(f"post-reauth checkout btn not found (try {_retry}) — URL={page.url} — screenshot saved")
                        log.warning("Headless checkout: post-reauth checkout button not found (try %d) — %s", _retry, page.url)
                        await asyncio.sleep(3)

                    if not _post_reauth_clicked:
                        log.warning("Headless checkout: gave up finding checkout button after re-auth")
                    continue  # restart loop to check Place Order / Continue fresh

            # Fill CVV if the field is visible at this step
            if ccv and not filled:
                _f, _d = await _try_fill_cvv(page, ccv)
                if _f:
                    filled = True
                    steps.append(f"CVV filled (step {attempt}): {_d}")

            # Check if Place Order is visible AND enabled (not greyed out)
            for sel in PLACE_ORDER_SELECTORS:
                try:
                    loc = page.locator(sel).first
                    if await loc.is_visible(timeout=T) and await loc.is_enabled(timeout=T):
                        await _status("processing order…")
                        await loc.click()
                        clicked = True
                        steps.append(f"clicked Place Order (step {attempt}): {sel}")
                        log.info("Headless checkout: clicked Place Order (%s)", sel)
                        break
                except PWTimeout:
                    continue
            if clicked:
                break

            # Select any unselected radio buttons (address/payment options) first
            for radio_sel in [
                'input[type="radio"]:not(:checked)',
                'input[type="radio"]',
            ]:
                try:
                    radio = page.locator(radio_sel).first
                    if await radio.is_visible(timeout=T):
                        await radio.click()
                        await asyncio.sleep(0.5)
                        steps.append(f"step {attempt}: selected radio via {radio_sel}")
                        break
                except PWTimeout:
                    continue
                except Exception:
                    continue

            # Advance to the next checkout step
            advanced = False
            for cont_sel in CONTINUE_SELECTORS:
                try:
                    btn = page.locator(cont_sel).first
                    if await btn.is_visible(timeout=T):
                        await btn.click()
                        await asyncio.sleep(3)
                        steps.append(f"step {attempt}: advanced via {cont_sel} — {page.url}")
                        log.info("Headless checkout: advanced via %s — %s", cont_sel, page.url)
                        advanced = True
                        break
                except PWTimeout:
                    continue

            if not advanced:
                steps.append(f"step {attempt}: no Place Order or Continue — URL={page.url}")
                log.info("Headless checkout: step %d — no button found at %s", attempt, page.url)
                # Don't give up immediately — checkout page may still be rendering.
                # Break only after 3 consecutive stuck attempts.
                if attempt >= 2:
                    break

        if not clicked:
            await page.screenshot(path=SCREENSHOT_PATH)
            steps.append("Place Order button NOT found — screenshot saved to " + SCREENSHOT_PATH)
            return {
                "success": False,
                "order_id": None,
                "error": "Could not find Place Order button",
                "debug": " | ".join(steps),
            }

        # ── Post-click: CVV modal may appear after clicking Place Order ───
        # Target shows a "Confirm CVV" modal with a separate Confirm button.
        # Wait a moment then fill CVV and click Confirm (not Place Order).
        await asyncio.sleep(2)
        if ccv and not filled:
            filled2, fill_detail2 = await _try_fill_cvv(page, ccv)
            steps.append(f"CVV post-click fill: {fill_detail2}")
            if filled2:
                await _click_confirm_or_place_order(page, steps)
        elif ccv and filled:
            # Check if a CVV confirm modal appeared post-click anyway
            refill, refill_detail = await _try_fill_cvv(page, ccv)
            if refill:
                steps.append(f"CVV re-filled post-click: {refill_detail}")
                await _click_confirm_or_place_order(page, steps)

        # ── Poll for confirmation, busy modal, or error (max 120s) ──────────
        # After Place Order is clicked Target may show a "checkout is busy"
        # modal. Click Ok, focus Place Order, hold Enter 5s, release, then
        # wait for the spinner to clear and check again. Repeat until we get
        # a confirmation page, a hard error, or we time out.
        import time as _time
        _deadline = _time.monotonic() + 300  # 5 min — covers many busy retries + slow spinners
        _confirmed = False
        _error_text = ""
        _busy_retries = 0
        _decline_keywords = ("issue with", "verify your card", "card info",
                             "contact your card", "declined", "payment method",
                             "card number", "billing", "mastercard", "visa",
                             "problem with your")
        _busy_selectors = [
            'text=checkout is busy',
            'text=Checkout is busy',
            'text=busy right now',
            'text=Busy right now',
            'text=is currently busy',
            'text=currently busy',
            'text=try again',
            'text=Try again',
        ]

        while _time.monotonic() < _deadline:
            if "/checkout/confirmation" in page.url:
                _confirmed = True
                break

            # Check for "checkout is busy" modal
            _busy_found = False
            for busy_sel in _busy_selectors:
                try:
                    busy_el = page.locator(busy_sel).first
                    if await busy_el.is_visible(timeout=300):
                        _busy_found = True
                        _busy_retries += 1
                        steps.append(f"busy modal #{_busy_retries} detected")
                        log.info("Headless checkout: busy modal #%d — dismissing and holding Enter", _busy_retries)
                        await _status(f"checkout busy — retry #{_busy_retries}…")
                        # Click Ok to dismiss
                        for ok_sel in ['button:has-text("Ok")', 'button:has-text("OK")', 'button:has-text("ok")', 'button:has-text("Okay")', 'button:has-text("Got it")', 'button:has-text("Try again")', 'button:has-text("Retry")']:
                            try:
                                ok_btn = page.locator(ok_sel).first
                                if await ok_btn.is_visible(timeout=500):
                                    await ok_btn.click()
                                    await asyncio.sleep(0.5)
                                    break
                            except Exception:
                                continue
                        # Focus Place Order and hold Enter for 5s
                        for po_sel in PLACE_ORDER_SELECTORS:
                            try:
                                po_btn = page.locator(po_sel).first
                                if await po_btn.is_visible(timeout=1_000) and await po_btn.is_enabled(timeout=1_000):
                                    await po_btn.focus()
                                    await asyncio.sleep(0.2)
                                    await page.keyboard.down("Enter")
                                    await asyncio.sleep(5)
                                    await page.keyboard.up("Enter")
                                    steps.append(f"busy retry #{_busy_retries}: held Enter on Place Order")
                                    break
                            except Exception:
                                continue
                        # Wait for spinner to resolve before checking again
                        await asyncio.sleep(3)
                        break
                except Exception:
                    continue

            if _busy_found:
                continue  # immediately re-check (confirmation or another busy modal)

            # Check for decline/payment error
            for err_sel in [
                '[data-test*="error"]', '[role="alert"]',
                '[aria-live="assertive"]', '[class*="errorMessage"]',
                '[class*="error-message"]',
            ]:
                try:
                    el = page.locator(err_sel).first
                    if await el.is_visible(timeout=200):
                        t = (await el.inner_text(timeout=200)).strip()
                        if t and any(kw in t.lower() for kw in _decline_keywords):
                            _error_text = t[:200]
                            break
                except Exception:
                    continue

            if _error_text:
                break

            await asyncio.sleep(2)

        if _confirmed:
            url = page.url
            order_id = None
            if "order_id=" in url:
                order_id = url.split("order_id=")[-1].split("&")[0]
            if not order_id:
                try:
                    el = page.locator('[data-test="orderNumber"], [data-test="order-number"]').first
                    order_id = await el.inner_text(timeout=3_000)
                except Exception:
                    pass
            steps.append(f"confirmed — order_id={order_id}")
            log.info("Headless checkout: order confirmed! order_id=%s", order_id)
            return {
                "success": True,
                "order_id": order_id,
                "error": None,
                "debug": " | ".join(steps),
            }

        # Failed — grab screenshot and classify the error
        try:
            await page.screenshot(path=SCREENSHOT_PATH)
            steps.append("screenshot → " + SCREENSHOT_PATH)
        except Exception:
            pass

        current_url = page.url

        # If we didn't catch it in the poll loop, do one final sweep for any error
        if not _error_text:
            for err_sel in [
                '[data-test*="error"]', '[role="alert"]',
                '[aria-live="assertive"]', '[class*="error"]',
            ]:
                try:
                    el = page.locator(err_sel).first
                    if await el.is_visible(timeout=500):
                        t = (await el.inner_text(timeout=500)).strip()
                        if t:
                            _error_text = t[:200]
                            break
                except Exception:
                    continue

        steps.append(f"URL={current_url}")
        is_decline = any(kw in _error_text.lower() for kw in _decline_keywords)

        if is_decline:
            clean_decline = _error_text.split("If you keep")[0].strip().rstrip(".")
            human_error = f"card declined: {clean_decline}"
        elif "/checkout/payment" in current_url and _error_text:
            human_error = f"payment error: {_error_text}"
        elif _error_text:
            human_error = _error_text
        else:
            human_error = "checkout timed out — no response from Target"

        return {
            "success": False,
            "order_id": None,
            "error": human_error,
            "debug": " | ".join(steps),
        }

    except Exception as exc:
        log.error("Headless checkout error: %s", exc)
        return {"success": False, "order_id": None, "error": str(exc), "debug": " | ".join(steps)}
    finally:
        # Close context first (flushes persistent profile to disk), then stop playwright
        try:
            await context.close()
        except Exception:
            pass
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass


async def open_checkout_and_click(
    account_token: Optional[str] = None,
    visitor_id: Optional[str] = None,
) -> None:
    """Launch a VISIBLE Chromium browser for manual checkout completion."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("playwright not installed — opening checkout in default browser.")
        import webbrowser
        webbrowser.open("https://www.target.com/checkout")
        return

    pw = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()

        cookies: list[dict] = []
        if account_token:
            cookies.append({
                "name": "accessToken", "value": account_token,
                "domain": ".target.com", "path": "/",
                "secure": True, "httpOnly": True,
            })
        if visitor_id:
            cookies.append({
                "name": "visitorId", "value": visitor_id,
                "domain": ".target.com", "path": "/",
                "secure": False, "httpOnly": False,
            })
        if cookies:
            await context.add_cookies(cookies)

        page = await context.new_page()
        await page.goto("https://www.target.com/checkout", wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass

        for sel in PLACE_ORDER_SELECTORS:
            try:
                loc = page.locator(sel).first
                if await loc.is_visible(timeout=3_000):
                    await loc.click()
                    log.info("Clicked checkout button: %s", sel)
                    break
            except Exception:
                continue

        while browser.is_connected():
            await asyncio.sleep(2)

    except Exception as exc:
        log.error("Browser automation failed: %s", exc)
        import webbrowser
        webbrowser.open("https://www.target.com/checkout")
    finally:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass
