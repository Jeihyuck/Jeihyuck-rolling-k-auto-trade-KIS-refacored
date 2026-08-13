from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib, os
from typing import Any, Callable
from trader.kis_wrapper import extract_order_no, is_order_accepted, mask_order_response
from .config import InfiniteConfig
from .models import Decision, SleeveState


def order_gates(env: dict[str, str] | None = None) -> tuple[bool, str]:
    e = env or os.environ
    truth = lambda k: str(e.get(k, "0")).lower() in {"1", "true", "yes", "on"}
    for key in ("LIVE_TRADING_ENABLED", "KR_LIVE_TRADING_ENABLED", "KR_ORDER_ARMED"):
        if not truth(key): return False, f"{key}_OFF"
    if str(e.get("STRATEGY_MODE", "")).upper() != "LIVE": return False, "STRATEGY_MODE_NOT_LIVE"
    for key in ("DRY_RUN", "DISABLE_LIVE_TRADING", "FORCE_BLOCK_LIVE"):
        if truth(key): return False, key
    return True, "OK"


def client_order_key(cycle_id: str, trade_date: date, side: str) -> str:
    raw = f"KR_INF_V2|122630|{cycle_id}|{trade_date}|{side.upper()}"
    return "kr-inf-" + hashlib.sha256(raw.encode()).hexdigest()[:32]


@dataclass
class CanonicalOrderRouter:
    orders_repo: Any
    broker_submit: Callable[..., dict]
    config: InfiniteConfig

    def route(self, decision: Decision, state: SleeveState, trade_date: date, *, env: str, run_id: str,
              price: float, regime: str) -> dict:
        if decision.action not in {"BUY", "SELL"} or decision.quantity <= 0: return {"sent": False, "reason": "NO_ORDER"}
        allowed, reason = order_gates()
        if not allowed: return {"sent": False, "reason": reason}
        effective_env = self.config.effective_env(env)
        key = client_order_key(state.cycle_id, trade_date, decision.action)
        metadata = {"strategy_id":"kr_kodex_infinite_v2", "book":"KR_INFINITE", "cycle_id":state.cycle_id,
            "policy_version":self.config.policy_version, "client_order_key":key, "trade_date":str(trade_date),
            "side":decision.action, "symbol":"122630", "quantity":decision.quantity,
            "unit_intent":str(decision.unit_intent), "market_state_at_decision":regime,
            "authoritative_average_price":str(state.average_price), "decision_price":price,
            "order_mode":effective_env, "run_id":run_id}
        order_id, created = self.orders_repo.create_intent_idempotent(env=effective_env, run_id=run_id,
            strategy="kr_kodex_infinite_v2", sid=1, mode=1, code="122630", market="KOSPI",
            side=decision.action, ord_type="MARKET", qty=decision.quantity, limit_price=None,
            stage="KR_INFINITE", client_order_key=key, request_json=metadata)
        if not created: return {"sent": False, "reason": "DUPLICATE_INTENT", "order_id": order_id}
        try:
            response = self.broker_submit(env=effective_env, symbol="122630", side=decision.action, qty=decision.quantity)
        except Exception as exc:
            # KIS transports use requests/urllib timeout classes and project-specific
            # temporary network errors.  All ambiguous transport failures are kept
            # non-terminal and are never resent under the same deterministic key.
            ambiguous = isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower() or \
                type(exc).__name__ in {"NetTemporaryError", "ConnectionError", "ReadTimeout"}
            if not ambiguous:
                self.orders_repo.mark_error(effective_env, key, {"error_type": type(exc).__name__})
                raise
            return {"sent": True, "status": "RECONCILE_PENDING", "order_id": order_id}
        odno = extract_order_no(response)
        masked = mask_order_response(response)
        self.orders_repo.mark_submitted(effective_env, key, odno, masked)
        if is_order_accepted(response, kis_env=effective_env) and odno:
            self.orders_repo.mark_acked(effective_env, odno, masked)
            return {"sent": True, "status": "ACKED", "order_id": order_id, "metadata": metadata}
        if is_order_accepted(response, kis_env=effective_env) and not odno:
            return {"sent": True, "status": "RECONCILE_PENDING", "order_id": order_id}
        self.orders_repo.mark_error(effective_env, key, masked)
        return {"sent": True, "status": "REJECTED", "order_id": order_id}


def ingest_fills(*, fills_repo: Any, orders_repo: Any, order: dict, broker_fills: list[dict], env: str,
                 run_id: str | None = None) -> int:
    """Persist only fills attributed to a canonical sleeve order."""
    meta = order.get("request_json") or {}
    if not (meta.get("strategy_id") == "kr_kodex_infinite_v2" and meta.get("book") == "KR_INFINITE"
            and meta.get("cycle_id") and meta.get("client_order_key") == order.get("client_order_key")
            and str(meta.get("symbol")) == "122630"):
        return 0
    count = 0
    odno = str(order.get("kis_odno") or "")
    for fill in broker_fills:
        if str(fill.get("odno") or fill.get("ODNO") or "") != odno or str(fill.get("symbol") or fill.get("pdno") or "").zfill(6) != "122630":
            continue
        fills_repo.upsert_fill(env=env, run_id=run_id, order_id=str(order["order_id"]), kis_odno=odno,
            trade_id=str(fill.get("trade_id") or fill.get("fill_id") or fill.get("odno_dvsn_no") or "") or None,
            code="122630", market="KOSPI", side=str(fill["side"]).upper(), qty=int(fill["qty"]),
            price=float(fill["price"]), fee=float(fill.get("fee") or 0), tax=float(fill.get("tax") or 0),
            filled_at=fill["filled_at"], raw_json=fill, fill_meta_json=meta)
        count += 1
    if count:
        orders_repo.mark_filled(env, kis_odno=odno)
    return count
