from __future__ import annotations


def enforce_kr_order_ownership(symbol: str, strategy_owner: str | None) -> tuple[bool, str | None]:
    """Final routing fence for the KR Infinite reserved symbol."""
    code = str(symbol or "").lstrip("A").zfill(6)
    owner = str(strategy_owner or "").upper()
    if code == "122630" and owner != "KR_INFINITE":
        return False, "KR_INF_OWNERSHIP_RESERVED"
    return True, None
