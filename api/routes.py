"""
FastAPI router — task CRUD + proxy list CRUD.
"""

import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import HTMLResponse

from core import db
from core.models import (
    TaskCreate,
    TaskUpdate,
    TaskResponse,
    EventResponse,
    ProxyListCreate,
    ProxyListResponse,
    ProxyCreate,
    ProxyResponse,
    ProxyUpdate,
    AccountLogin,
    AccountResponse,
    AccountUpdate,
)
from api.target import make_client, get_or_refresh_visitor_id, check_availability, login_with_credentials

router = APIRouter(prefix="/api")


# ── Helper ─────────────────────────────────────────────────────────────────────

def _registry(request: Request):
    return request.app.state.registry


# ═══════════════════════════════════════════════════════════════════════════════
# TASK endpoints
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/tasks", response_model=TaskResponse, status_code=201)
async def create_task(body: TaskCreate, request: Request):
    row_id = await db.execute(
        """
        INSERT INTO tasks (nickname, tcin, store_id, interval_seconds, quantity, proxy_list_id, account_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (body.nickname, body.url_or_tcin, body.store_id, body.interval_seconds, body.quantity, body.proxy_list_id, body.account_id),
    )
    _registry(request).start(row_id)
    row = await db.fetch_one("SELECT * FROM tasks WHERE id = ?", (row_id,))
    return _task_row(row)


@router.get("/tasks", response_model=list[TaskResponse])
async def list_tasks():
    rows = await db.fetch_all("SELECT * FROM tasks ORDER BY created_at DESC")
    return [_task_row(r) for r in rows]


@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: int):
    row = await db.fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if not row:
        raise HTTPException(404, "Task not found")
    return _task_row(row)


@router.patch("/tasks/{task_id}", response_model=TaskResponse)
async def update_task(task_id: int, body: TaskUpdate, request: Request):
    row = await db.fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if not row:
        raise HTTPException(404, "Task not found")

    fields = body.model_dump(exclude_none=True)
    if not fields:
        return _task_row(row)

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [task_id]
    await db.execute(f"UPDATE tasks SET {set_clause} WHERE id = ?", tuple(values))

    # Handle monitor start/stop based on status change
    if "status" in fields:
        reg = _registry(request)
        if fields["status"] == "active":
            reg.start(task_id)
        elif fields["status"] in ("paused", "in_cart", "checkout"):
            reg.stop(task_id)

    row = await db.fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    return _task_row(row)


@router.delete("/tasks/{task_id}", status_code=204)
async def delete_task(task_id: int, request: Request):
    row = await db.fetch_one("SELECT id FROM tasks WHERE id = ?", (task_id,))
    if not row:
        raise HTTPException(404, "Task not found")
    _registry(request).stop(task_id)
    await db.execute("DELETE FROM tasks WHERE id = ?", (task_id,))


@router.get("/tasks/{task_id}/events", response_model=list[EventResponse])
async def get_events(task_id: int):
    rows = await db.fetch_all(
        "SELECT * FROM events WHERE task_id = ? ORDER BY occurred_at DESC LIMIT 50",
        (task_id,),
    )
    return [_event_row(r) for r in rows]


@router.post("/tasks/{task_id}/check-now")
async def check_now(task_id: int):
    """Trigger an immediate one-shot availability check and return the result."""
    task = await db.fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if not task:
        raise HTTPException(404, "Task not found")

    proxy_url: Optional[str] = None
    if task.get("proxy_list_id"):
        from core.monitor import get_next_proxy
        proxy_row = await get_next_proxy(task["proxy_list_id"])
        if proxy_row:
            proxy_url = proxy_row["url"]

    async with make_client(proxy_url) as client:
        visitor_id = await get_or_refresh_visitor_id(client, task.get("visitor_id"))
        await db.execute(
            "UPDATE tasks SET visitor_id = ? WHERE id = ?",
            (visitor_id, task_id),
        )
        result = await check_availability(client, task["tcin"], task.get("store_id"), visitor_id)

    from core.monitor import _now, _log_event
    await db.execute(
        "UPDATE tasks SET last_checked_at = ? WHERE id = ?",
        (_now(), task_id),
    )
    event_type = "in_stock" if result["available"] else "out_of_stock"
    await _log_event(task_id, event_type, result)

    return {"task_id": task_id, **result}


# ═══════════════════════════════════════════════════════════════════════════════
# PROXY LIST endpoints
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/proxy-lists", response_model=ProxyListResponse, status_code=201)
async def create_proxy_list(body: ProxyListCreate):
    try:
        row_id = await db.execute(
            "INSERT INTO proxy_lists (name) VALUES (?)", (body.name,)
        )
    except Exception:
        raise HTTPException(409, f"A proxy list named '{body.name}' already exists.")
    row = await db.fetch_one("SELECT * FROM proxy_lists WHERE id = ?", (row_id,))
    return ProxyListResponse(**row, proxy_count=0)


@router.get("/proxy-lists", response_model=list[ProxyListResponse])
async def list_proxy_lists():
    rows = await db.fetch_all(
        """
        SELECT pl.*, COUNT(p.id) AS proxy_count
        FROM proxy_lists pl
        LEFT JOIN proxies p ON p.list_id = pl.id
        GROUP BY pl.id
        ORDER BY pl.created_at DESC
        """
    )
    return [ProxyListResponse(**r) for r in rows]


@router.delete("/proxy-lists/{list_id}", status_code=204)
async def delete_proxy_list(list_id: int):
    row = await db.fetch_one("SELECT id FROM proxy_lists WHERE id = ?", (list_id,))
    if not row:
        raise HTTPException(404, "Proxy list not found")
    await db.execute("DELETE FROM proxy_lists WHERE id = ?", (list_id,))


@router.post("/proxy-lists/{list_id}/proxies", response_model=list[ProxyResponse], status_code=201)
async def add_proxies(list_id: int, body: ProxyCreate):
    row = await db.fetch_one("SELECT id FROM proxy_lists WHERE id = ?", (list_id,))
    if not row:
        raise HTTPException(404, "Proxy list not found")

    added_ids: list[int] = []
    for url in body.urls:
        url = url.strip()
        if not url:
            continue
        pid = await db.execute(
            "INSERT INTO proxies (list_id, url) VALUES (?, ?)", (list_id, url)
        )
        added_ids.append(pid)

    if not added_ids:
        raise HTTPException(400, "No valid proxy URLs provided.")

    placeholders = ",".join("?" * len(added_ids))
    rows = await db.fetch_all(
        f"SELECT * FROM proxies WHERE id IN ({placeholders})", tuple(added_ids)
    )
    return [_proxy_row(r) for r in rows]


@router.get("/proxy-lists/{list_id}/proxies", response_model=list[ProxyResponse])
async def list_proxies(list_id: int):
    rows = await db.fetch_all(
        "SELECT * FROM proxies WHERE list_id = ? ORDER BY created_at ASC",
        (list_id,),
    )
    return [_proxy_row(r) for r in rows]


@router.patch("/proxies/{proxy_id}", response_model=ProxyResponse)
async def update_proxy(proxy_id: int, body: ProxyUpdate):
    row = await db.fetch_one("SELECT * FROM proxies WHERE id = ?", (proxy_id,))
    if not row:
        raise HTTPException(404, "Proxy not found")

    if body.enabled is not None:
        enabled_val = 1 if body.enabled else 0
        # If re-enabling, also reset fail_count
        if body.enabled:
            await db.execute(
                "UPDATE proxies SET enabled = 1, fail_count = 0 WHERE id = ?",
                (proxy_id,),
            )
        else:
            await db.execute(
                "UPDATE proxies SET enabled = 0 WHERE id = ?",
                (proxy_id,),
            )

    row = await db.fetch_one("SELECT * FROM proxies WHERE id = ?", (proxy_id,))
    return _proxy_row(row)


@router.delete("/proxies/{proxy_id}", status_code=204)
async def delete_proxy(proxy_id: int):
    row = await db.fetch_one("SELECT id FROM proxies WHERE id = ?", (proxy_id,))
    if not row:
        raise HTTPException(404, "Proxy not found")
    await db.execute("DELETE FROM proxies WHERE id = ?", (proxy_id,))


# ═══════════════════════════════════════════════════════════════════════════════
# ACCOUNT endpoints
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/accounts", response_model=AccountResponse, status_code=201)
async def create_account(body: AccountLogin):
    """
    Save a Target account (email + password). The bot logs in automatically
    at the start of each task — no immediate login required here.
    """
    try:
        await db.execute(
            """INSERT INTO accounts (nickname, email, password, ccv)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(nickname) DO UPDATE SET
                 email=excluded.email,
                 password=excluded.password,
                 ccv=CASE WHEN excluded.ccv != '' THEN excluded.ccv ELSE ccv END
            """,
            (body.nickname, body.email, body.password, body.ccv or ""),
        )
    except Exception as exc:
        raise HTTPException(400, str(exc))
    row = await db.fetch_one("SELECT * FROM accounts WHERE nickname = ?", (body.nickname,))
    return _account_row(row)



@router.get("/accounts", response_model=list[AccountResponse])
async def list_accounts():
    rows = await db.fetch_all("SELECT * FROM accounts ORDER BY created_at DESC")
    return [_account_row(r) for r in rows]


@router.patch("/accounts/{account_id}", response_model=AccountResponse)
async def update_account(account_id: int, body: AccountUpdate):
    row = await db.fetch_one("SELECT * FROM accounts WHERE id = ?", (account_id,))
    if not row:
        raise HTTPException(404, "Account not found")
    fields = body.model_dump(exclude_none=True)
    if fields:
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        await db.execute(
            f"UPDATE accounts SET {set_clause} WHERE id = ?",
            tuple(fields.values()) + (account_id,),
        )
    row = await db.fetch_one("SELECT * FROM accounts WHERE id = ?", (account_id,))
    return _account_row(row)


@router.delete("/accounts/{account_id}", status_code=204)
async def delete_account(account_id: int):
    row = await db.fetch_one("SELECT id FROM accounts WHERE id = ?", (account_id,))
    if not row:
        raise HTTPException(404, "Account not found")
    await db.execute("DELETE FROM accounts WHERE id = ?", (account_id,))


# ── Row serialisers ────────────────────────────────────────────────────────────

def _account_row(row: dict) -> AccountResponse:
    return AccountResponse(
        id=row["id"],
        nickname=row["nickname"],
        email=row.get("email"),
        has_ccv=bool(row.get("ccv")),
        created_at=row["created_at"],
    )


def _task_row(row: dict) -> TaskResponse:
    return TaskResponse(
        id=row["id"],
        nickname=row["nickname"],
        tcin=row["tcin"],
        store_id=row.get("store_id"),
        interval_seconds=row["interval_seconds"],
        quantity=row.get("quantity") or 1,
        status=row["status"],
        live_status=row.get("live_status", ""),
        proxy_list_id=row.get("proxy_list_id"),
        account_id=row.get("account_id"),
        last_checked_at=row.get("last_checked_at"),
        last_in_stock_at=row.get("last_in_stock_at"),
        created_at=row["created_at"],
    )


def _event_row(row: dict) -> EventResponse:
    return EventResponse(
        id=row["id"],
        task_id=row["task_id"],
        event_type=row["event_type"],
        detail=row.get("detail"),
        occurred_at=row["occurred_at"],
    )


def _proxy_row(row: dict) -> ProxyResponse:
    return ProxyResponse(
        id=row["id"],
        list_id=row["list_id"],
        url=row["url"],
        enabled=bool(row["enabled"]),
        fail_count=row["fail_count"],
        last_used_at=row.get("last_used_at"),
        created_at=row["created_at"],
    )
