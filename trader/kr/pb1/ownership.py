from __future__ import annotations

from trader.kr.infinite.config import InfiniteConfig


def reserved_kr_infinite_symbol() -> str:
    """Return the configured Infinite sleeve symbol; one routing SSOT."""
    return str(InfiniteConfig.from_env().symbol or "").lstrip("A").zfill(6)


def enforce_kr_order_ownership(symbol: str, strategy_owner: str | None) -> tuple[bool, str | None]:
    """Final routing fence for the configured KR Infinite sleeve."""
    code = str(symbol or "").lstrip("A").zfill(6)
    owner = str(strategy_owner or "").upper()
    reserved = reserved_kr_infinite_symbol()
    if reserved and code == reserved and owner != "KR_INFINITE":
        return False, "KR_INF_OWNERSHIP_RESERVED"
    return True, None
