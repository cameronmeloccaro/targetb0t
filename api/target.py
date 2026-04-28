"""
Target.com API client — visitorId, availability check, add-to-cart.
All network calls go through the caller-supplied httpx.AsyncClient so that
proxies and headers are configured once in monitor.py.
"""

import json
import uuid
from typing import Optional

import httpx

API_KEY = "ff457966e64d5e877fdbad070f276d18ecec4a01"

VISITOR_ID_URL = "https://visitorid.target.com/v1/visitor"

# Default store ID used when the user hasn't specified one (Target HQ store, Minneapolis)
DEFAULT_STORE_ID = "1031"

REDSKY_BASE = (
    "https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1"
    "?key={key}"
    "&tcin={tcin}"
    "&channel=WEB"
    "&pricing_store_id={store_id}"
    "&scheduled_delivery_store_id={store_id}"
    "&latitude=44.97&longitude=-93.27"
    "&zip=55413&state=MN&country=USA"
    "&visitor_id={visitor_id}"
)

GUEST_TOKEN_URL = (
    "https://api.target.com/guests/v2/tokens"
    f"?api_key={API_KEY}&channel_id=10&type=guest"
)

# Candidate auth endpoint paths — tried in order until one returns non-404
_AUTH_CANDIDATES = [
    f"https://api.target.com/accounts/v3/members/auth/token?api_key={API_KEY}",
    "https://api.target.com/accounts/v3/members/auth/token",
    f"https://api.target.com/accounts/v2/auth/token?api_key={API_KEY}",
    "https://api.target.com/auth/v1/token",
    f"https://api.target.com/auth/v1/token?api_key={API_KEY}",
]

CART_URL = "https://carts.target.com/web_checkouts/v1/cart_items"

# Checkout uses a different API key than the product/cart endpoints
CHECKOUT_API_KEY = "e59ce3b531b2c39afb2e2b8a71ff10113aac2a14"
WEBCHECKOUTS_BASE = "https://carts.target.com/web_checkouts/v1"
CHECKOUT_URL = f"{WEBCHECKOUTS_BASE}/checkout"
CHECKOUT_FIELD_GROUPS = "ADDRESSES,CART,CART_ITEMS,FINANCE_PROVIDERS,PAYMENT_INSTRUCTIONS,PICKUP_INSTRUCTIONS,PROMOTION_CODES,SUMMARY"

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.target.com",
    "Referer": "https://www.target.com/",
}


def make_client(proxy_url: Optional[str] = None, timeout: float = 15.0) -> httpx.AsyncClient:
    """Create a pre-configured httpx.AsyncClient, optionally routed through a proxy."""
    return httpx.AsyncClient(headers=BASE_HEADERS, proxy=proxy_url, timeout=timeout)


async def get_or_refresh_visitor_id(
    client: httpx.AsyncClient,
    stored_visitor_id: Optional[str],
) -> str:
    """
    Return the stored visitorId if present, otherwise try Target's visitor
    identity endpoint and fall back to a locally generated UUID.
    """
    if stored_visitor_id:
        return stored_visitor_id

    try:
        resp = await client.get(VISITOR_ID_URL)
        resp.raise_for_status()
        data = resp.json()
        visitor_id = data.get("visitorId") or data.get("visitor_id")
        if visitor_id:
            return visitor_id
    except Exception:
        pass

    # Target's visitorId is a standard UUID — generate one locally as fallback
    return str(uuid.uuid4())


async def get_guest_token(client: httpx.AsyncClient) -> tuple[Optional[str], str]:
    """Fetch a short-lived guest auth token for the cart API.
    Returns (token_or_None, debug_message).
    """
    try:
        resp = await client.get(GUEST_TOKEN_URL)
        data = resp.json()
        if not resp.is_success:
            return None, f"token HTTP {resp.status_code}: {resp.text[:200]}"
        token = data.get("access_token")
        if token:
            return token, "ok"
        return None, f"no access_token in: {list(data.keys())}"
    except Exception as exc:
        return None, str(exc)


async def login_with_credentials(
    client: httpx.AsyncClient,
    email: str,
    password: str,
) -> dict:
    """
    Authenticate with Target using email + password.
    Tries each known auth endpoint in turn until one responds with non-404.
    Returns {"access_token", "refresh_token", "expires_in", "error"}.
    Credentials are never stored — only the resulting tokens are saved.
    """
    payload = {
        "client_id": "ecom-web-1.0.0",
        "client_secret": "",
        "grant_type": "password",
        "username": email,
        "password": password,
        "credentials": "include",
    }
    attempts: list[str] = []
    for url in _AUTH_CANDIDATES:
        try:
            resp = await client.post(url, json=payload)
            if resp.status_code == 404:
                attempts.append(f"{url} → 404")
                continue
            # Got a real response (success or auth error) — use this endpoint
            try:
                data = resp.json()
            except Exception:
                data = {}
            if resp.is_success:
                return {
                    "access_token": data.get("access_token"),
                    "refresh_token": data.get("refresh_token"),
                    "expires_in": data.get("expires_in", 3600),
                    "error": None,
                }
            msg = data.get("message") or data.get("error_description") or resp.text[:300]
            return {
                "access_token": None, "refresh_token": None, "expires_in": None,
                "error": f"HTTP {resp.status_code}: {msg}",
            }
        except Exception as exc:
            attempts.append(f"{url} → {exc}")
            continue

    return {
        "access_token": None, "refresh_token": None, "expires_in": None,
        "error": "All auth endpoints returned 404 — Target may have changed their API. Tried: " + "; ".join(attempts),
    }


async def refresh_access_token(
    client: httpx.AsyncClient,
    refresh_token: str,
) -> dict:
    """Exchange a refresh_token for a new access_token using the first working auth endpoint."""
    payload = {
        "client_id": "ecom-web-1.0.0",
        "client_secret": "",
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    for url in _AUTH_CANDIDATES:
        try:
            resp = await client.post(url, json=payload)
            if resp.status_code == 404:
                continue
            try:
                data = resp.json()
            except Exception:
                data = {}
            if resp.is_success:
                return {
                    "access_token": data.get("access_token"),
                    "refresh_token": data.get("refresh_token", refresh_token),
                    "expires_in": data.get("expires_in", 3600),
                    "error": None,
                }
            return {
                "access_token": None, "refresh_token": None, "expires_in": None,
                "error": f"HTTP {resp.status_code}: {data.get('message') or resp.text[:200]}",
            }
        except Exception:
            continue
    return {"access_token": None, "refresh_token": None, "expires_in": None, "error": "all auth endpoints 404"}


# Well-known always-in-stock canary TCIN (Coca-Cola 12pk Cans)
_CANARY_TCIN = "12953747"


async def _is_target_blocked(client: httpx.AsyncClient, visitor_id: Optional[str] = None) -> bool:
    """
    Probe the redsky API to detect whether we're being blocked/rate-limited.

    Uses two checks:
    1. Quick check: hit the API and look for hard-block status codes
       (403/429/503) or non-JSON responses.
    2. Canary check: fetch a known always-in-stock product.  If the API
       returns 200 but with product=null, we're being soft-blocked (Target
       returns valid JSON but strips product data).
    """
    vid = visitor_id or str(uuid.uuid4())
    try:
        # -- Quick check with dummy TCIN --
        probe_url = REDSKY_BASE.format(
            key=API_KEY, tcin="00000001", store_id=DEFAULT_STORE_ID, visitor_id=vid
        )
        r = await client.get(probe_url)
        if r.status_code in (403, 429, 503):
            return True
        if r.status_code == 404:
            try:
                r.json()
            except Exception:
                return True  # non-JSON 404 = captcha/block page

        # -- Canary check with a real product --
        canary_url = REDSKY_BASE.format(
            key=API_KEY, tcin=_CANARY_TCIN, store_id=DEFAULT_STORE_ID, visitor_id=vid
        )
        r2 = await client.get(canary_url, headers={"Cookie": f"visitorId={vid}"})
        if r2.status_code in (403, 429, 503):
            return True
        try:
            data = r2.json()
            product = data.get("data", {}).get("product")
            if product is None:
                # The canary product should always exist — null means soft-block
                return True
        except Exception:
            return True  # non-JSON = blocked

        return False
    except Exception:
        return True


async def check_availability(
    client: httpx.AsyncClient,
    tcin: str,
    store_id: Optional[str],
    visitor_id: str,
) -> dict:
    """
    Poll redsky for product availability.

    Returns:
        {
          "available": bool,
          "fulfillment_type": "ship" | "pickup" | None,
          "price": float | None,
          "raw_status": str,
        }
    """
    sid = store_id or DEFAULT_STORE_ID
    url = REDSKY_BASE.format(key=API_KEY, tcin=tcin, store_id=sid, visitor_id=visitor_id)

    headers = {"Cookie": f"visitorId={visitor_id}"}

    resp = await client.get(url, headers=headers)
    if resp.status_code in (403, 429, 503):
        return {
            "available": False,
            "fulfillment_type": None,
            "price": None,
            "raw_status": "RATE_LIMITED",
            "_http_status": resp.status_code,
        }
    if resp.status_code == 404:
        # Target sometimes returns 404 as a soft block — probe the homepage to tell apart
        blocked = await _is_target_blocked(client, visitor_id)
        if blocked:
            return {
                "available": False,
                "fulfillment_type": None,
                "price": None,
                "raw_status": "RATE_LIMITED",
                "_http_status": 404,
            }
        return {
            "available": False,
            "fulfillment_type": None,
            "price": None,
            "raw_status": "PRODUCT_NOT_FOUND",
        }
    resp.raise_for_status()
    try:
        data = resp.json()
    except Exception:
        # Non-JSON response on a 200 is almost always a block/captcha page
        return {
            "available": False,
            "fulfillment_type": None,
            "price": None,
            "raw_status": "RATE_LIMITED",
            "_http_status": resp.status_code,
        }

    result: dict = {
        "available": False,
        "fulfillment_type": None,
        "price": None,
        "raw_status": "UNKNOWN",
    }

    try:
        product = data["data"]["product"]
        if product is None:
            # Target sometimes returns {product: null} when rate-limiting
            # instead of a proper 403/429. Probe to tell apart.
            blocked = await _is_target_blocked(client, visitor_id)
            if blocked:
                return {
                    "available": False,
                    "fulfillment_type": None,
                    "price": None,
                    "raw_status": "RATE_LIMITED",
                    "_http_status": resp.status_code,
                }
            result["raw_status"] = "PRODUCT_NOT_FOUND"
            return result

        # Fulfillment lives under product.item.fulfillment in the current API
        item = product.get("item") or {}
        fulfillment = item.get("fulfillment") or product.get("fulfillment") or {}
        eligibility_rules = item.get("eligibility_rules") or {}

        # Price
        try:
            result["price"] = product["price"]["current_retail"]
        except (KeyError, TypeError):
            pass

        # Ship-to-home check via shipping_options (classic path)
        try:
            ship_status: str = fulfillment["shipping_options"]["availability_status"]
            result["raw_status"] = ship_status
            if ship_status in ("IN_STOCK", "LIMITED_STOCK"):
                result["available"] = True
                result["fulfillment_type"] = "ship"
                return result
        except (KeyError, TypeError):
            pass

        # Ship-to-home via eligibility_rules (newer API shape)
        try:
            ship_rule = eligibility_rules.get("ship_to_guest") or {}
            if ship_rule.get("is_active") is True:
                result["available"] = True
                result["fulfillment_type"] = "ship"
                result["raw_status"] = "IN_STOCK"
                return result
        except (KeyError, TypeError):
            pass

        # Scheduled delivery (same-day / Shipt) — common for grocery & beverages
        try:
            sched_rule = eligibility_rules.get("scheduled_delivery") or {}
            if sched_rule.get("is_active") is True:
                result["available"] = True
                result["fulfillment_type"] = "scheduled_delivery"
                result["raw_status"] = "IN_STOCK_DELIVERY"
                return result
        except (KeyError, TypeError):
            pass

        # In-store pickup via eligibility_rules.hold
        try:
            hold_rule = eligibility_rules.get("hold") or {}
            if hold_rule.get("is_active") is True:
                result["available"] = True
                result["fulfillment_type"] = "pickup"
                result["raw_status"] = "IN_STOCK_PICKUP"
                return result
        except (KeyError, TypeError):
            pass

        # In-store pickup via store_options quantity (when store_id provided)
        if store_id:
            try:
                store_opts = fulfillment.get("store_options") or []
                if store_opts:
                    qty = store_opts[0].get("location_available_to_promise_quantity", 0) or 0
                    if float(qty) > 0:
                        result["available"] = True
                        result["fulfillment_type"] = "pickup"
                        result["raw_status"] = "IN_STOCK_PICKUP"
                        return result
            except (KeyError, TypeError, ValueError):
                pass

        # Add-on item (can be added to cart with qualifying order)
        try:
            addon_rule = eligibility_rules.get("add_on") or {}
            if addon_rule.get("is_active") is True:
                result["available"] = True
                result["fulfillment_type"] = "add_on"
                result["raw_status"] = "IN_STOCK_ADDON"
                return result
        except (KeyError, TypeError):
            pass

        # Fallback: is_out_of_stock_in_all_store_locations
        if result["raw_status"] == "UNKNOWN":
            all_oos = fulfillment.get("is_out_of_stock_in_all_store_locations")
            if all_oos is False:
                result["available"] = True
                result["fulfillment_type"] = "ship"
                result["raw_status"] = "IN_STOCK_FALLBACK"
            elif all_oos is True:
                result["raw_status"] = "OUT_OF_STOCK"
            else:
                # Check if ALL eligibility rules are inactive → truly out of stock
                any_active = any(
                    (eligibility_rules.get(k) or {}).get("is_active") is True
                    for k in eligibility_rules
                )
                result["raw_status"] = "OUT_OF_STOCK" if not any_active else "IN_STOCK_OTHER"
                if any_active:
                    result["available"] = True
                    result["fulfillment_type"] = "other"
            result["_debug_keys"] = list(fulfillment.keys())
            result["_eligibility_keys"] = list(eligibility_rules.keys())
            # Show what each eligibility rule says
            result["_eligibility_detail"] = {
                k: (eligibility_rules.get(k) or {}).get("is_active")
                for k in eligibility_rules
            }

    except (KeyError, TypeError) as exc:
        # Missing expected keys often means a rate-limit/block page
        blocked = await _is_target_blocked(client, visitor_id)
        if blocked:
            return {
                "available": False,
                "fulfillment_type": None,
                "price": None,
                "raw_status": "RATE_LIMITED",
                "_http_status": resp.status_code,
            }
        result["raw_status"] = "PARSE_ERROR"
        result["_debug"] = str(exc)
        result["_response_keys"] = list(data.keys()) if isinstance(data, dict) else repr(data)[:200]

    return result


async def add_to_cart(
    client: httpx.AsyncClient,
    tcin: str,
    store_id: Optional[str],
    visitor_id: str,
    account_token: Optional[str] = None,
    fulfillment_type: Optional[str] = None,
    quantity: int = 1,
) -> dict:
    """
    POST a cart item to Target's cart API.

    If account_token is provided (a logged-in user's Bearer token) it is used
    directly.  Otherwise we fall back to a guest token.

    fulfillment_type comes from the availability check (e.g. 'ship',
    'scheduled_delivery', 'pickup', 'add_on').

    Returns:
        {"success": bool, "cart_id": str | None, "cart_item_id": str | None, "error": str | None}
    """
    sid = store_id or DEFAULT_STORE_ID

    fulfillment: dict
    if fulfillment_type == "scheduled_delivery":
        fulfillment = {"type": "SCHEDULED_DELIVERY", "store_id": sid}
    elif fulfillment_type == "pickup" or (store_id and fulfillment_type != "ship"):
        fulfillment = {"type": "PICKUP", "store_id": sid}
    else:
        fulfillment = {"type": "SHIP"}

    payload = {
        "cart_type": "REGULAR",
        "channel_id": 10,
        "shopping_context": "digital",
        "cart_item": {
            "type": "REGULAR",
            "tcin": tcin,
            "quantity": quantity,
            "relationship_type": "STAND_ALONE",
            "fulfillment": fulfillment,
        },
    }

    # Use account token if available, otherwise try guest token
    if account_token:
        auth_token = account_token
        token_source = "account"
    else:
        auth_token, token_debug = await get_guest_token(client)
        if not auth_token:
            return {
                "success": False,
                "cart_id": None,
                "cart_item_id": None,
                "error": f"no account set and guest token failed: {token_debug}",
            }
        token_source = "guest"

    headers = {
        "Content-Type": "application/json",
        "Cookie": f"accessToken={auth_token}; visitorId={visitor_id}",
    }

    cart_url = f"{CART_URL}?key={API_KEY}&channel=WEB"

    try:
        resp = await client.post(cart_url, json=payload, headers=headers)
        if resp.status_code in (200, 201):
            body = resp.json()
            return {
                "success": True,
                "cart_id": body.get("cart_id"),
                "cart_item_id": body.get("cart_item_id"),
                "error": None,
                "needs_reauth": False,
            }
        else:
            reauth = _needs_reauth(resp.status_code, resp.text) if token_source == "account" else False
            return {
                "success": False,
                "cart_id": None,
                "cart_item_id": None,
                "error": f"HTTP {resp.status_code}: {resp.text[:300]} [token_source={token_source}]",
                "needs_reauth": reauth,
            }
    except Exception as exc:
        return {
            "success": False,
            "cart_id": None,
            "cart_item_id": None,
            "error": str(exc),
            "needs_reauth": False,
        }


def _needs_reauth(status_code: int, body_text: str) -> bool:
    """Detect whether a Target API response indicates a step-up auth /
    password re-entry challenge.  Common patterns:
      - HTTP 401 (session expired)
      - HTTP 403 with messages about guest status, login, or step-up
      - Response body containing password/step-up/re-authenticate keywords
    """
    if status_code == 401:
        return True
    lower = body_text.lower()
    reauth_signals = [
        "step_up",
        "step-up",
        "password",
        "re-authenticate",
        "reauthenticate",
        "login_required",
        "login required",
        "session_expired",
        "session expired",
        "not registered",
        "invalid_guest_status",
    ]
    return any(sig in lower for sig in reauth_signals)


def _update_cookie_token(cookie_str: str, new_token: str) -> str:
    """Replace the accessToken value inside a cookie header string."""
    import re
    # Replace existing accessToken=<value>
    updated = re.sub(
        r'accessToken=[^;]+',
        f'accessToken={new_token}',
        cookie_str,
    )
    # If accessToken wasn't in the cookie string, append it
    if 'accessToken=' not in updated:
        updated = f"accessToken={new_token}; {updated}"
    return updated


def _extract_cvv_payment_id(checkout_data: dict) -> Optional[str]:
    """
    Inspect a checkout API response (success or error) for a payment
    instruction that requires CVV verification.
    Returns the payment_instruction_id if CVV is needed, or None.

    Handles two shapes:
    1. Error response with ``code: "MISSING_CREDIT_CARD_CVV"`` and
       ``alerts[].metadata.payment_instruction_ids``
    2. Successful response with ``payment_instructions`` list containing
       boolean flags like ``requires_cvv``.
    """
    # ── Shape 1: error response (HTTP 400 with MISSING_CREDIT_CARD_CVV) ──
    code = str(checkout_data.get("code") or "").upper()
    if "CVV" in code or "CARD_VERIFICATION" in code:
        # Try to get the payment_instruction_id from alerts metadata
        for alert in (checkout_data.get("alerts") or []):
            meta = alert.get("metadata") or {}
            pi_ids = meta.get("payment_instruction_ids") or meta.get("payment_instruction_id") or ""
            if pi_ids:
                # Could be a single ID or comma-separated
                return str(pi_ids).split(",")[0].strip()
        # Fallback: check top-level metadata
        meta = checkout_data.get("metadata") or {}
        pi_ids = meta.get("payment_instruction_ids") or meta.get("payment_instruction_id") or ""
        if pi_ids:
            return str(pi_ids).split(",")[0].strip()

    # ── Shape 2: successful response with payment_instructions list ──
    instructions = checkout_data.get("payment_instructions") or []
    if isinstance(instructions, dict):
        instructions = [instructions]

    for pi in instructions:
        if not isinstance(pi, dict):
            continue
        pi_id = pi.get("payment_instruction_id") or pi.get("id")
        if not pi_id:
            continue

        # Direct boolean flags
        if pi.get("requires_cvv") or pi.get("cvv_required") or pi.get("card_verification_required"):
            return pi_id

        # Status string containing CVV keywords
        status = str(pi.get("status") or pi.get("payment_status") or "").upper()
        if "CVV" in status or "VERIFICATION" in status or "CARD_VERIFY" in status:
            return pi_id

        # Check nested wallet/card objects
        wallet = pi.get("wallet") or {}
        card = wallet.get("card") or pi.get("card") or {}
        if card.get("requires_cvv") or card.get("cvv_required"):
            return pi_id

    return None


def _extract_access_token(cookie_str: str) -> Optional[str]:
    """Pull the accessToken value out of a Cookie header string."""
    import re
    m = re.search(r'accessToken=([^;]+)', cookie_str)
    return m.group(1).strip() if m else None


def _find_jwt_cookies(cookie_str: str) -> list[tuple[str, str]]:
    """
    Scan a Cookie header string for values that look like JWTs.
    A JWT is three base64url segments separated by dots.
    Returns [(cookie_name, jwt_value), ...] sorted longest-first.
    """
    import re
    results = []
    # Split cookies by "; " and parse key=value
    for part in cookie_str.split("; "):
        eq = part.find("=")
        if eq < 1:
            continue
        name = part[:eq].strip()
        value = part[eq + 1:].strip()
        # JWT pattern: three dot-separated base64url segments, min length ~50
        if len(value) > 50 and value.count(".") >= 2:
            segments = value.split(".")
            if len(segments) >= 3 and all(len(s) > 5 for s in segments[:3]):
                results.append((name, value))
    # Sort by value length descending (real JWTs are long)
    results.sort(key=lambda x: -len(x[1]))
    return results


def _decode_jwt_issuer(jwt_str: str) -> Optional[str]:
    """Decode the payload of a JWT and return the 'iss' claim, if any."""
    import base64
    try:
        parts = jwt_str.split(".")
        if len(parts) < 2:
            return None
        # Pad the base64url payload
        payload_b64 = parts[1]
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes)
        return payload.get("iss")
    except Exception:
        return None


async def _submit_cvv(
    client: httpx.AsyncClient,
    checkout_id: Optional[str],
    payment_id: str,
    cvv: str,
    headers: dict,
) -> dict:
    """
    Submit the CVV for a payment instruction during checkout.
    The PI endpoint rejects MI6-issued Bearer tokens.  We scan ALL cookies
    for JWT tokens and try each as Bearer auth to find the right issuer.
    Returns {"success": bool, "error": str | None, "debug_log": list[str]}.
    """
    debug_log: list[str] = []

    cookie_str = headers.get("Cookie", "")

    # Log all cookie names so we can see if the ID2 token was captured
    all_cookie_names = [c.split("=")[0].strip() for c in cookie_str.split(";") if "=" in c]
    debug_log.append(f"Cookies present: {', '.join(all_cookie_names)}")

    # Find all JWT-like tokens in cookies
    jwt_cookies = _find_jwt_cookies(cookie_str)
    jwt_summary = ", ".join(
        f"{name}({_decode_jwt_issuer(val) or '?'},{len(val)}ch)"
        for name, val in jwt_cookies
    )
    debug_log.append(f"JWTs found: {jwt_summary or 'NONE'}")

    # Build auth header variants from each JWT cookie
    header_variants: list[tuple[str, dict]] = []
    for cookie_name, jwt_val in jwt_cookies:
        issuer = _decode_jwt_issuer(jwt_val) or "?"
        label = f"{cookie_name}({issuer})"
        hdr = {**headers, "Authorization": f"Bearer {jwt_val}"}
        header_variants.append((label, hdr))

    # Also try cookie-only as baseline
    header_variants.append(("cookie-only", headers))

    # ── Payload ──
    cvv_payload = {
        "cart_type": "REGULAR",
        "channel_id": "90",
        "wallet": {
            "card": {
                "card_verification_value": cvv,
            },
        },
    }

    # ── URL: just use the main v1/pi endpoint (it exists, just auth is wrong) ──
    pi_url = (
        f"{WEBCHECKOUTS_BASE}"
        f"/payment_instructions/{payment_id}"
        f"?key={CHECKOUT_API_KEY}"
    )

    # Try each JWT cookie as Bearer on the v1/pi endpoint
    for hdr_label, hdr in header_variants:
        try:
            r = await client.put(pi_url, json=cvv_payload, headers=hdr)
            entry = f"PUT v1/pi [{hdr_label}] → {r.status_code}: {r.text[:100]}"
            debug_log.append(entry)
            if r.is_success:
                return {"success": True, "error": None, "debug_log": debug_log}
        except Exception as exc:
            debug_log.append(f"PUT v1/pi [{hdr_label}] → ERR: {exc}")

    # If we have a checkout_id, also try checkout-scoped PI
    if checkout_id:
        co_pi_url = (
            f"{CHECKOUT_URL}/{checkout_id}"
            f"/payment_instructions/{payment_id}"
            f"?key={CHECKOUT_API_KEY}"
        )
        for hdr_label, hdr in header_variants:
            try:
                r = await client.put(co_pi_url, json=cvv_payload, headers=hdr)
                entry = f"PUT co/pi [{hdr_label}] → {r.status_code}: {r.text[:100]}"
                debug_log.append(entry)
                if r.is_success:
                    return {"success": True, "error": None, "debug_log": debug_log}
            except Exception as exc:
                debug_log.append(f"PUT co/pi [{hdr_label}] → ERR: {exc}")

    all_entries = " | ".join(debug_log)
    return {
        "success": False,
        "error": f"CVV failed {len(debug_log)} attempts: {all_entries}",
        "debug_log": debug_log,
    }


async def place_order(
    client: httpx.AsyncClient,
    cart_id: str,
    account_token: str,
    visitor_id: str,
    checkout_cookies: str = "",
    ccv: str = "",
) -> dict:
    """
    Initiate checkout and place the order via Target's checkout API.

    Requires a logged-in account with a saved shipping address and payment
    method on file.  The ``checkout_cookies`` string should be the full
    Cookie header copied from a real browser checkout request — it contains
    session tokens (login-session, _px3, etc.) that the checkout API requires
    beyond just the accessToken.

    If ``ccv`` is provided, it will be submitted when Target requests card
    verification (CVV/CCV) during checkout.

    Returns:
        {"success": bool, "order_id": str | None, "error": str | None,
         "needs_reauth": bool, "needs_cvv": bool}
    """
    # Build cookie header — use the full browser cookie string if available,
    # otherwise fall back to just the essential cookies.
    if checkout_cookies:
        cookie_str = checkout_cookies
    else:
        cookie_str = f"accessToken={account_token}; visitorId={visitor_id}"

    headers = {
        "Content-Type": "application/json",
        "Cookie": cookie_str,
        "Origin": "https://www.target.com",
        "Referer": "https://www.target.com/checkout",
    }

    checkout_url = (
        f"{CHECKOUT_URL}"
        f"?cart_type=REGULAR"
        f"&field_groups={CHECKOUT_FIELD_GROUPS}"
        f"&key={CHECKOUT_API_KEY}"
    )

    try:
        cvv_debug: list[str] = []

        # ── Step 1: initiate checkout from the cart ──────────────────────
        init_payload = {
            "cart_type": "REGULAR",
            "channel_id": "90",
        }
        resp = await client.post(checkout_url, json=init_payload, headers=headers)

        # ── Capture any new cookies set by the checkout init response ─────
        # Target may set an ID2-scoped session token here (httpOnly Set-Cookie).
        # Merge those into cookie_str so _submit_cvv uses them as Bearer auth.
        new_cookies = dict(resp.cookies)
        new_cookies.update(dict(client.cookies))
        if new_cookies:
            extra = "; ".join(f"{k}={v}" for k, v in new_cookies.items()
                              if k not in cookie_str)
            if extra:
                cookie_str = f"{cookie_str}; {extra}"
                headers = {**headers, "Cookie": cookie_str}

        # ── Check if init failed due to missing CVV ──────────────────────
        if not resp.is_success:
            try:
                err_data = resp.json()
            except Exception:
                err_data = {}

            err_pi_id = _extract_cvv_payment_id(err_data)
            if err_pi_id:
                # Target rejected checkout because CVV is missing
                if not ccv:
                    return {
                        "success": False,
                        "order_id": None,
                        "error": "Checkout requires CVV but none stored. Go to Accounts tab and save your CCV.",
                        "needs_reauth": False,
                        "needs_cvv": True,
                    }

                # ── Try all JWT cookies from checkout_cookies as Bearer auth ──
                cvv_result = await _submit_cvv(
                    client, None, err_pi_id, ccv, headers,
                )
                cvv_debug.extend(cvv_result.get("debug_log", []))

                if cvv_result["success"]:
                    # CVV accepted — retry full init
                    resp = await client.post(checkout_url, json=init_payload, headers=headers)
                    if not resp.is_success:
                        return {
                            "success": False,
                            "order_id": None,
                            "error": (
                                f"CVV submitted OK but checkout init still failed: "
                                f"HTTP {resp.status_code}: {resp.text[:300]}"
                            ),
                            "needs_reauth": False,
                            "needs_cvv": True,
                        }
                else:
                    debug_str = " | ".join(cvv_debug)
                    return {
                        "success": False,
                        "order_id": None,
                        "error": (
                            f"CVV failed (pi={err_pi_id}). "
                            f"DEBUG: {debug_str}"
                        ),
                        "needs_reauth": False,
                        "needs_cvv": True,
                    }
            else:
                reauth = _needs_reauth(resp.status_code, resp.text)
                return {
                    "success": False,
                    "order_id": None,
                    "error": f"checkout init HTTP {resp.status_code}: {resp.text[:400]}",
                    "needs_reauth": reauth,
                    "needs_cvv": False,
                }

        checkout_data = resp.json()
        checkout_id = (
            checkout_data.get("checkout_id")
            or checkout_data.get("id")
            or checkout_data.get("order_id")
        )
        if not checkout_id:
            return {
                "success": False,
                "order_id": None,
                "error": f"no checkout_id in response — keys: {list(checkout_data.keys())}",
                "needs_reauth": False,
                "needs_cvv": False,
                "_response": checkout_data,
            }

        # ── Step 1.5: submit CVV if the successful response still asks ───
        cvv_payment_id = _extract_cvv_payment_id(checkout_data)
        if cvv_payment_id:
            if ccv:
                cvv_result = await _submit_cvv(client, checkout_id, cvv_payment_id, ccv, headers)
                if not cvv_result["success"]:
                    return {
                        "success": False,
                        "order_id": None,
                        "error": f"CVV submission failed: {cvv_result['error']}",
                        "needs_reauth": False,
                        "needs_cvv": True,
                    }
            else:
                return {
                    "success": False,
                    "order_id": None,
                    "error": "Checkout requires CVV but none stored. Go to Accounts tab and save your CCV.",
                    "needs_reauth": False,
                    "needs_cvv": True,
                }

        # ── Step 2: place the order ─────────────────────────────────────
        place_url = (
            f"{CHECKOUT_URL}/{checkout_id}"
            f"?field_groups={CHECKOUT_FIELD_GROUPS}"
            f"&key={CHECKOUT_API_KEY}"
        )
        place_payload = {
            "cart_type": "REGULAR",
            "channel_id": "90",
        }
        place_resp = await client.put(place_url, json=place_payload, headers=headers)

        if place_resp.is_success:
            order_data = place_resp.json()
            return {
                "success": True,
                "order_id": order_data.get("order_id") or order_data.get("id") or checkout_id,
                "error": None,
                "needs_reauth": False,
                "needs_cvv": False,
            }
        else:
            # Check if step 2 itself is asking for CVV
            place_text = place_resp.text.lower()
            is_cvv = "cvv" in place_text or "card_verification" in place_text or "verification" in place_text
            if is_cvv and ccv:
                # Try submitting CVV now and retrying step 2
                # Look for payment_id in the error response
                try:
                    err_data = place_resp.json()
                    pi_id = _extract_cvv_payment_id(err_data) or cvv_payment_id
                    if pi_id:
                        await _submit_cvv(client, checkout_id, pi_id, ccv, headers)
                        retry_resp = await client.put(place_url, json=place_payload, headers=headers)
                        if retry_resp.is_success:
                            order_data = retry_resp.json()
                            return {
                                "success": True,
                                "order_id": order_data.get("order_id") or order_data.get("id") or checkout_id,
                                "error": None,
                                "needs_reauth": False,
                                "needs_cvv": False,
                            }
                except Exception:
                    pass

            reauth = _needs_reauth(place_resp.status_code, place_resp.text)
            return {
                "success": False,
                "order_id": None,
                "error": f"place_order HTTP {place_resp.status_code}: {place_resp.text[:400]}",
                "needs_reauth": reauth,
                "needs_cvv": is_cvv,
            }

    except Exception as exc:
        return {
            "success": False,
            "order_id": None,
            "error": str(exc),
            "needs_reauth": False,
            "needs_cvv": False,
        }
