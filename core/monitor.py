"""
Background monitoring: MonitorRegistry and the per-task polling coroutine.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from core import db
from core.browser import headless_place_order, headless_login_and_get_cookies
from api.target import (
    get_or_refresh_visitor_id,
    check_availability,
    add_to_cart,
    place_order,
    make_client,
    refresh_access_token,
    login_with_credentials,
    _update_cookie_token,
)

log = logging.getLogger("monitor")


# ── Live status helper ────────────────────────────────────────────────────────

async def _set_live_status(task_id: int, status: str) -> None:
    """Update the live_status column so the dashboard can display real-time state."""
    await db.execute(
        "UPDATE tasks SET live_status = ? WHERE id = ?",
        (status, task_id),
    )


# ── Proxy helpers ──────────────────────────────────────────────────────────────

async def get_next_proxy(list_id: int) -> Optional[dict]:
    """
    Pick the least-recently-used enabled proxy from a list.
    Updates last_used_at so subsequent calls rotate to the next proxy.
    Returns the proxy row dict, or None if no enabled proxies exist.
    """
    row = await db.fetch_one(
        """
        SELECT * FROM proxies
        WHERE list_id = ? AND enabled = 1
        ORDER BY COALESCE(last_used_at, '1970-01-01') ASC
        LIMIT 1
        """,
        (list_id,),
    )
    if row is None:
        return None
    now = _now()
    await db.execute(
        "UPDATE proxies SET last_used_at = ? WHERE id = ?",
        (now, row["id"]),
    )
    return row


async def mark_proxy_failed(proxy_id: int) -> None:
    """Increment fail_count; disable the proxy after 3 consecutive failures."""
    await db.execute(
        """
        UPDATE proxies
        SET fail_count = fail_count + 1,
            enabled    = CASE WHEN fail_count + 1 >= 3 THEN 0 ELSE enabled END
        WHERE id = ?
        """,
        (proxy_id,),
    )


async def reset_proxy_fails(proxy_id: int) -> None:
    """Reset fail_count after a successful request through this proxy."""
    await db.execute(
        "UPDATE proxies SET fail_count = 0 WHERE id = ?",
        (proxy_id,),
    )


# ── Timestamp helper ───────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ── Re-auth helper ────────────────────────────────────────────────────────────

async def _try_reauth(
    task_id: int,
    account_id: int,
    proxy_url: Optional[str],
    checkout_cookies: str,
) -> Optional[dict]:
    """
    Attempt to re-authenticate with stored email + password.

    Returns a dict with fresh credentials on success:
        {"access_token": str, "checkout_cookies": str}
    Returns None on failure (also logs the error).
    """
    from datetime import timedelta

    acct = await db.fetch_one(
        "SELECT * FROM accounts WHERE id = ?", (account_id,),
    )
    stored_pw = (acct or {}).get("password", "")
    stored_email = (acct or {}).get("email", "")

    if not stored_pw and not stored_email:
        missing = "email and password"
    elif not stored_email:
        missing = "email (required for re-auth)"
    elif not stored_pw:
        missing = "password"
    else:
        missing = None

    if missing:
        log.warning("Task %d: re-auth needed but missing: %s", task_id, missing)
        await _log_event(task_id, "error", {
            "message": f"Token expired — re-auth needs {missing}. Go to Accounts tab → Store Password and save both email + password.",
        })
        return None

    # ── Attempt 1: fast API login ─────────────────────────────────────────
    log.info("Task %d: attempting API re-auth with stored credentials", task_id)
    await _set_live_status(task_id, "re-authenticating…")

    async with make_client(proxy_url) as auth_c:
        auth_result = await login_with_credentials(auth_c, stored_email, stored_pw)

    if auth_result.get("access_token"):
        new_token = auth_result["access_token"]
        new_refresh = auth_result.get("refresh_token") or (acct or {}).get("refresh_token")
        new_exp = None
        if auth_result.get("expires_in"):
            new_exp = (
                datetime.now(timezone.utc)
                + timedelta(seconds=auth_result["expires_in"])
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        updated_cookies = _update_cookie_token(checkout_cookies, new_token) if checkout_cookies else checkout_cookies
        await db.execute(
            "UPDATE accounts SET access_token=?, refresh_token=?, expires_at=?, checkout_cookies=? WHERE id=?",
            (new_token, new_refresh, new_exp, updated_cookies, account_id),
        )
        log.info("Task %d: API re-auth success", task_id)
        await _set_live_status(task_id, "re-auth OK")
        return {"access_token": new_token, "checkout_cookies": updated_cookies or checkout_cookies}

    # ── Attempt 2: headless Playwright browser login ──────────────────────
    # API login failed (likely blocked by PerimeterX) — use a real browser.
    log.info("Task %d: API re-auth failed (%s) — trying headless browser login",
             task_id, auth_result.get("error"))
    await _set_live_status(task_id, "re-authenticating via browser…")
    await _log_event(task_id, "info", {"message": "API login blocked — retrying via headless browser"})

    browser_auth = await headless_login_and_get_cookies(stored_email, stored_pw)

    if not browser_auth["success"]:
        log.warning("Task %d: browser re-auth failed: %s", task_id, browser_auth["error"])
        await _log_event(task_id, "error", {
            "message": f"Browser re-auth failed: {browser_auth['error']}",
        })
        return None

    new_token = browser_auth["access_token"]
    new_refresh = browser_auth["refresh_token"] or (acct or {}).get("refresh_token")
    fresh_cookies = browser_auth["cookies"]

    await db.execute(
        "UPDATE accounts SET access_token=?, refresh_token=?, checkout_cookies=? WHERE id=?",
        (new_token, new_refresh, fresh_cookies, account_id),
    )
    log.info("Task %d: browser re-auth success — fresh cookies saved", task_id)
    await _set_live_status(task_id, "re-auth OK (browser)")
    await _log_event(task_id, "info", {"message": "Browser re-auth success — fresh cookies saved automatically"})

    return {
        "access_token": new_token,
        "checkout_cookies": fresh_cookies,
    }


# ── Per-task monitor coroutine ─────────────────────────────────────────────────

async def monitor_task(task_id: int) -> None:
    """
    Infinite polling loop for a single task.
    Exits when:
    - Task is successfully added to cart (status → 'checkout', browser opens)
    - Task is deleted or status changes to 'paused' (row disappears / status != 'active')
    - The asyncio.Task is cancelled (clean shutdown)
    """
    log.info("Monitor started for task %d", task_id)

    while True:
        try:
            task = await db.fetch_one(
                "SELECT * FROM tasks WHERE id = ? AND status = 'active'",
                (task_id,),
            )
            if task is None:
                log.info("Task %d no longer active — stopping monitor.", task_id)
                break

            await _set_live_status(task_id, "checking")

            proxy_url: Optional[str] = None
            proxy_id: Optional[int] = None

            if task.get("proxy_list_id"):
                proxy_row = await get_next_proxy(task["proxy_list_id"])
                if proxy_row:
                    proxy_url = proxy_row["url"]
                    proxy_id = proxy_row["id"]
                else:
                    log.warning("Task %d: proxy list %d has no enabled proxies — using local.", task_id, task["proxy_list_id"])

            async with make_client(proxy_url) as client:
                # Refresh / acquire visitor ID
                try:
                    visitor_id = await get_or_refresh_visitor_id(
                        client, task.get("visitor_id")
                    )
                    await db.execute(
                        "UPDATE tasks SET visitor_id = ? WHERE id = ?",
                        (visitor_id, task_id),
                    )
                except Exception as exc:
                    log.warning("Task %d: could not get visitorId: %s", task_id, exc)
                    if proxy_id:
                        await mark_proxy_failed(proxy_id)
                    await _log_event(task_id, "error", {"message": f"visitorId: {exc}"})
                    await _set_live_status(task_id, "error — visitorId failed")
                    await asyncio.sleep(60)
                    continue

                # Check availability
                try:
                    result = await check_availability(
                        client, task["tcin"], task.get("store_id"), visitor_id
                    )
                    if proxy_id:
                        await reset_proxy_fails(proxy_id)
                except (httpx.ProxyError, httpx.ConnectError, httpx.TimeoutException) as exc:
                    log.warning("Task %d: proxy/network error: %s", task_id, exc)
                    if proxy_id:
                        await mark_proxy_failed(proxy_id)
                    await _log_event(task_id, "error", {"message": str(exc)})
                    await _set_live_status(task_id, "error — network/proxy")
                    await asyncio.sleep(30)
                    continue
                except Exception as exc:
                    log.error("Task %d: availability check failed: %s", task_id, exc)
                    await _log_event(task_id, "error", {"message": str(exc)})
                    await _set_live_status(task_id, "error — check failed")
                    await asyncio.sleep(60)
                    continue

            # Update last_checked_at
            now = _now()
            await db.execute(
                "UPDATE tasks SET last_checked_at = ? WHERE id = ?",
                (now, task_id),
            )

            if result.get("raw_status") == "RATE_LIMITED":
                http_status = result.get("_http_status", "?")
                log.warning("Task %d: rate limited (HTTP %s) — backing off 60s", task_id, http_status)
                await _log_event(task_id, "rate_limited", {
                    "message": f"Blocked by Target (HTTP {http_status}) — waiting 60s",
                    "http_status": http_status,
                })
                await _set_live_status(task_id, "rate limited — 60s cooldown")
                await asyncio.sleep(60)
                continue

            event_type = "in_stock" if result["available"] else "out_of_stock"
            await _log_event(task_id, event_type, result)

            if result["available"]:
                await _set_live_status(task_id, "in stock!")
            else:
                await _set_live_status(task_id, "out of stock")

            log.info(
                "Task %d (TCIN %s): %s — %s",
                task_id,
                task["tcin"],
                event_type,
                result.get("raw_status"),
            )

            if result["available"]:
                # Fetch account token, auto-refreshing if it's expired
                account_token: Optional[str] = None
                checkout_cookies: str = ""
                account_ccv: str = ""
                account_email: str = ""
                account_password: str = ""
                if task.get("account_id"):
                    acct = await db.fetch_one(
                        "SELECT * FROM accounts WHERE id = ?",
                        (task["account_id"],),
                    )
                    if acct:
                        account_token = acct["access_token"]
                        checkout_cookies = acct.get("checkout_cookies") or ""
                        account_ccv = acct.get("ccv") or ""
                        account_email = acct.get("email") or ""
                        account_password = acct.get("password") or ""
                        # Refresh if within 5 minutes of expiry or already expired
                        expires_at = acct.get("expires_at")
                        if expires_at and acct.get("refresh_token"):
                            from datetime import datetime, timezone, timedelta
                            try:
                                exp = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                                if exp - datetime.now(timezone.utc) < timedelta(minutes=5):
                                    log.info("Task %d: refreshing account token", task_id)
                                    async with make_client(proxy_url) as rc:
                                        refreshed = await refresh_access_token(rc, acct["refresh_token"])
                                    if refreshed["access_token"]:
                                        new_exp = None
                                        if refreshed.get("expires_in"):
                                            new_exp = (
                                                datetime.now(timezone.utc)
                                                + timedelta(seconds=refreshed["expires_in"])
                                            ).strftime("%Y-%m-%dT%H:%M:%SZ")
                                        await db.execute(
                                            "UPDATE accounts SET access_token=?, refresh_token=?, expires_at=? WHERE id=?",
                                            (refreshed["access_token"], refreshed["refresh_token"] or acct["refresh_token"], new_exp, acct["id"]),
                                        )
                                        account_token = refreshed["access_token"]
                                    else:
                                        log.warning("Task %d: token refresh failed: %s", task_id, refreshed["error"])
                            except Exception as e:
                                log.warning("Task %d: token expiry check failed: %s", task_id, e)

                # ── Full browser flow when account has credentials ──────────
                # Target's checkout API rejects non-browser sessions (INVALID_GUEST_STATUS).
                # When email+password are available, do login → ATC → checkout
                # entirely in one headless browser session.
                if account_email and account_password:
                    await db.execute(
                        "UPDATE tasks SET last_in_stock_at = ? WHERE id = ?",
                        (now, task_id),
                    )

                    async def _browser_status(msg: str, _tid: int = task_id) -> None:
                        await _set_live_status(_tid, msg)

                    order_result = await headless_place_order(
                        checkout_cookies=checkout_cookies,
                        access_token=account_token or "",
                        visitor_id=visitor_id,
                        ccv=account_ccv,
                        email=account_email,
                        password=account_password,
                        tcin=task["tcin"],
                        quantity=task.get("quantity") or 1,
                        status_callback=_browser_status,
                    )
                    # Log debug steps as info event so they appear in the dashboard
                    if order_result.get("debug"):
                        await _log_event(task_id, "info", {
                            "message": f"Browser: {order_result['debug'][:400]}"
                        })
                    await _log_event(task_id, "order_placed", order_result)

                    if order_result["success"]:
                        await _set_live_status(task_id, f"✓ order placed #{order_result.get('order_id','')}")
                        log.info("Task %d: order placed! order_id=%s", task_id, order_result.get("order_id"))
                        await db.execute(
                            "UPDATE tasks SET status = 'checkout' WHERE id = ?",
                            (task_id,),
                        )
                    else:
                        err = order_result.get("error") or "unknown error"
                        clean = err.split(" | steps:")[0].split(" | timed")[0][:80]
                        await _set_live_status(task_id, f"order failed — {clean}")
                        log.warning("Task %d: browser checkout failed: %s", task_id, err)
                        await db.execute(
                            "UPDATE tasks SET status = 'error' WHERE id = ?",
                            (task_id,),
                        )
                    break

                # ── API flow (no credentials stored) ──────────────────────
                # Fall back to API add-to-cart + API checkout for guest/token-only accounts.
                detected_fulfillment = result.get("fulfillment_type")
                max_retries = 3
                cart_result = None
                reauthed_during_cart = False

                for attempt in range(1, max_retries + 1):
                    await _set_live_status(
                        task_id,
                        "adding to cart…" if attempt == 1 else f"retrying cart ({attempt}/{max_retries})…",
                    )
                    async with make_client(proxy_url) as client:
                        cart_result = await add_to_cart(
                            client, task["tcin"], task.get("store_id"), visitor_id,
                            account_token=account_token,
                            fulfillment_type=detected_fulfillment,
                            quantity=task.get("quantity") or 1,
                        )

                    if cart_result["success"]:
                        break

                    if cart_result.get("needs_reauth") and task.get("account_id") and not reauthed_during_cart:
                        reauthed_during_cart = True
                        fresh = await _try_reauth(task_id, task["account_id"], proxy_url, checkout_cookies)
                        if fresh:
                            account_token = fresh["access_token"]
                            checkout_cookies = fresh["checkout_cookies"]
                            await _set_live_status(task_id, "re-auth OK — retrying cart…")
                            async with make_client(proxy_url) as client:
                                cart_result = await add_to_cart(
                                    client, task["tcin"], task.get("store_id"), visitor_id,
                                    account_token=account_token,
                                    fulfillment_type=detected_fulfillment,
                                    quantity=task.get("quantity") or 1,
                                )
                            if cart_result["success"]:
                                break

                    log.warning(
                        "Task %d: cart attempt %d/%d failed: %s",
                        task_id, attempt, max_retries, cart_result.get("error"),
                    )
                    if attempt < max_retries:
                        await _set_live_status(task_id, f"cart failed — retrying in {attempt * 2}s")
                        await _log_event(task_id, "added_to_cart", {
                            **cart_result,
                            "_retry": f"attempt {attempt}/{max_retries} — retrying in {attempt * 2}s",
                        })
                        await asyncio.sleep(attempt * 2)

                await _log_event(task_id, "added_to_cart", cart_result)

                if cart_result["success"]:
                    await _set_live_status(task_id, "added to cart — placing order…")
                    await db.execute(
                        "UPDATE tasks SET last_in_stock_at = ? WHERE id = ?",
                        (now, task_id),
                    )
                    log.info("Task %d: added to cart! cart_id=%s", task_id, cart_result.get("cart_id"))

                    if account_token and cart_result.get("cart_id"):
                        async with make_client(proxy_url) as oc:
                            order_result = await place_order(
                                oc,
                                cart_result["cart_id"],
                                account_token,
                                visitor_id,
                                checkout_cookies=checkout_cookies,
                                ccv=account_ccv,
                            )

                        await _log_event(task_id, "order_placed", order_result)

                        if order_result["success"]:
                            await _set_live_status(task_id, f"✓ order placed #{order_result.get('order_id','')}")
                            log.info("Task %d: order placed! order_id=%s", task_id, order_result.get("order_id"))
                            await db.execute(
                                "UPDATE tasks SET status = 'checkout' WHERE id = ?",
                                (task_id,),
                            )
                        else:
                            # Show a clean status — pull the human-readable part before any debug trace
                            err = order_result.get("error") or "unknown error"
                            clean = err.split(" | steps:")[0].split(" | timed")[0][:80]
                            await _set_live_status(task_id, f"order failed — {clean}")
                            log.warning("Task %d: place_order failed: %s", task_id, err)
                            await db.execute(
                                "UPDATE tasks SET status = 'error' WHERE id = ?",
                                (task_id,),
                            )
                    else:
                        await _set_live_status(task_id, "in cart — no account for checkout")
                        log.warning("Task %d: no account token or cart_id — cannot place order", task_id)
                        await db.execute(
                            "UPDATE tasks SET status = 'checkout' WHERE id = ?",
                            (task_id,),
                        )
                    break
                else:
                    await _set_live_status(task_id, "cart failed — all retries exhausted")
                    log.warning("Task %d: all %d cart attempts failed: %s", task_id, max_retries, cart_result.get("error"))

            await _set_live_status(task_id, f"waiting {task['interval_seconds']}s…")
            await asyncio.sleep(task["interval_seconds"])

        except asyncio.CancelledError:
            log.info("Monitor for task %d cancelled.", task_id)
            await _set_live_status(task_id, "")
            break
        except Exception as exc:
            log.exception("Task %d: unexpected error: %s", task_id, exc)
            try:
                await _log_event(task_id, "error", {"message": str(exc)})
                await _set_live_status(task_id, "error — unexpected")
            except Exception:
                pass
            interval = task["interval_seconds"] if task else 10
            await asyncio.sleep(interval)


async def _log_event(task_id: int, event_type: str, detail: dict) -> None:
    await db.execute(
        "INSERT INTO events (task_id, event_type, detail) VALUES (?, ?, ?)",
        (task_id, event_type, json.dumps(detail)),
    )


# ── Monitor Registry ───────────────────────────────────────────────────────────

class MonitorRegistry:
    """Tracks running asyncio.Tasks keyed by DB task_id."""

    def __init__(self) -> None:
        self._tasks: dict[int, asyncio.Task] = {}

    def start(self, task_id: int) -> None:
        if task_id in self._tasks and not self._tasks[task_id].done():
            return  # already running
        t = asyncio.create_task(monitor_task(task_id), name=f"monitor-{task_id}")
        t.add_done_callback(lambda _: self._tasks.pop(task_id, None))
        self._tasks[task_id] = t

    def stop(self, task_id: int) -> None:
        if t := self._tasks.pop(task_id, None):
            t.cancel()

    def is_running(self, task_id: int) -> bool:
        return task_id in self._tasks and not self._tasks[task_id].done()

    async def start_all_active(self) -> None:
        rows = await db.fetch_all("SELECT id FROM tasks WHERE status = 'active'")
        for row in rows:
            self.start(row["id"])
        if rows:
            log.info("Resumed %d active monitor(s).", len(rows))

    def stop_all(self) -> None:
        for task_id in list(self._tasks):
            self.stop(task_id)
