import re
from typing import Optional
from pydantic import BaseModel, field_validator


# ── Proxy List models ──────────────────────────────────────────────────────────

class ProxyListCreate(BaseModel):
    name: str


class ProxyListResponse(BaseModel):
    id: int
    name: str
    created_at: str
    proxy_count: int = 0


class ProxyCreate(BaseModel):
    urls: list[str]  # bulk add one or more proxy URLs

    @field_validator("urls", mode="before")
    @classmethod
    def coerce_single(cls, v):
        if isinstance(v, str):
            return [v]
        return v


class ProxyResponse(BaseModel):
    id: int
    list_id: int
    url: str
    enabled: bool
    fail_count: int
    last_used_at: Optional[str]
    created_at: str


class ProxyUpdate(BaseModel):
    enabled: Optional[bool] = None


# ── Account models ─────────────────────────────────────────────────────────────

class AccountLogin(BaseModel):
    nickname: str
    email: str
    password: str
    ccv: Optional[str] = None


class AccountResponse(BaseModel):
    id: int
    nickname: str
    email: Optional[str]
    has_ccv: bool = False
    created_at: str


class AccountUpdate(BaseModel):
    email: Optional[str] = None
    password: Optional[str] = None
    ccv: Optional[str] = None


# ── Task models ────────────────────────────────────────────────────────────────

class TaskCreate(BaseModel):
    nickname: str
    url_or_tcin: str
    interval_seconds: int = 10
    quantity: int = 1

    @field_validator("interval_seconds")
    @classmethod
    def check_interval(cls, v: int) -> int:
        if not (1 <= v <= 30):
            raise ValueError("interval_seconds must be between 1 and 30")
        return v

    @field_validator("quantity")
    @classmethod
    def check_quantity(cls, v: int) -> int:
        if not (1 <= v <= 10):
            raise ValueError("quantity must be between 1 and 10")
        return v

    store_id: Optional[str] = None
    proxy_list_id: Optional[int] = None
    account_id: Optional[int] = None

    @field_validator("url_or_tcin")
    @classmethod
    def parse_tcin(cls, v: str) -> str:
        v = v.strip()
        if re.fullmatch(r"\d+", v):
            return v
        match = re.search(r"/-/A-(\d+)", v) or re.search(r"[?&]preselect=(\d+)", v)
        if match:
            return match.group(1)
        raise ValueError(
            f"Cannot extract a TCIN from: {v!r}. "
            "Paste a Target product URL or the numeric TCIN directly."
        )


class TaskResponse(BaseModel):
    id: int
    nickname: str
    tcin: str
    store_id: Optional[str]
    interval_seconds: int
    quantity: int = 1
    status: str
    live_status: str = ""
    proxy_list_id: Optional[int]
    account_id: Optional[int]
    last_checked_at: Optional[str]
    last_in_stock_at: Optional[str]
    created_at: str


class TaskUpdate(BaseModel):
    nickname: Optional[str] = None
    interval_seconds: Optional[int] = None
    quantity: Optional[int] = None
    store_id: Optional[str] = None
    status: Optional[str] = None
    proxy_list_id: Optional[int] = None
    account_id: Optional[int] = None


# ── Event models ───────────────────────────────────────────────────────────────

class EventResponse(BaseModel):
    id: int
    task_id: int
    event_type: str
    detail: Optional[str]
    occurred_at: str
