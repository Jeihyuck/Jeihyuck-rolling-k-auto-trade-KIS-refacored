"""Hard blocks for inverse/derivative hedge products in KR long-only trading."""
from __future__ import annotations
import os
from typing import Any

BLOCK_REASON = "KR_FORBIDDEN_HEDGE_OR_INVERSE_PRODUCT"

def _csv(key: str, default: str = "") -> list[str]:
    raw = os.getenv(key, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

def forbidden_symbols() -> set[str]:
    return {s.zfill(6) if s.isdigit() else s for s in _csv("KR_FORBIDDEN_HEDGE_SYMBOLS")}

def forbidden_keywords() -> list[str]:
    return _csv("KR_FORBIDDEN_HEDGE_NAME_KEYWORDS", "인버스,선물인버스,2X인버스,곱버스,레버리지인버스,VIX,변동성,ELW")

def is_forbidden_kr_product(symbol: str | None = None, name: str | None = None, row: dict[str, Any] | None = None) -> bool:
    row = row or {}
    sym = str(symbol or row.get("symbol") or row.get("code") or "").strip()
    if sym.isdigit(): sym = sym.zfill(6)
    nm = str(name or row.get("name") or row.get("code_name") or "")
    return bool((sym and sym in forbidden_symbols()) or any(k and k.lower() in nm.lower().replace(" ", "") for k in forbidden_keywords()))

def block_buy_if_forbidden(intent: dict[str, Any]) -> dict[str, Any]:
    side = str(intent.get("side") or intent.get("action") or "").upper()
    symbol = str(intent.get("symbol") or intent.get("code") or "").zfill(6)
    if side == "BUY" and symbol == "122630" and os.getenv("KR_INFINITE_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}:
        owner = (str(intent.get("strategy_id") or ""), str(intent.get("book") or ""))
        if owner == ("KR_KODEX_LEVERAGE_INFINITE_V1", "KR_INFINITE_BOOK"):
            return dict(intent)
        out = dict(intent); out.update({"status": "BLOCKED", "reason": "KR_INFINITE_OWNERSHIP_RESERVED", "blocked_reason": "KR_INFINITE_OWNERSHIP_RESERVED"})
        return out
    if side == "BUY" and is_forbidden_kr_product(row=intent):
        out = dict(intent); out.update({"status": "BLOCKED", "reason": BLOCK_REASON, "blocked_reason": BLOCK_REASON})
        return out
    return dict(intent)
