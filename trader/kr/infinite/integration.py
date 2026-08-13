from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from trader.kr.regime import KRExecutionPolicy, KRMarketExecutionPolicy, KRMarketState, KRRegimeSnapshot

from .config import InfiniteConfig
from .models import Action, BrokerPosition, InfiniteState, Quote
from .repository import InfiniteRepository
from .risk_adapter import assess
from .strategy import evaluate

logger = logging.getLogger(__name__)


def owns_symbol(symbol: str, config: InfiniteConfig | None = None) -> bool:
    config = config or InfiniteConfig.from_env()
    return config.enabled and str(symbol or "").strip().zfill(6) == config.symbol


def exclude_owned(rows: list[dict], config: InfiniteConfig | None = None) -> list[dict]:
    """The only legacy boundary: reserve 122630 BUY ownership when enabled."""
    config = config or InfiniteConfig.from_env()
    if not config.enabled:
        return rows
    return [row for row in rows if str(row.get("symbol") or row.get("code") or "").zfill(6) != config.symbol]


def global_order_gate(env: dict[str, str] | None = None) -> tuple[bool, str]:
    e = env or os.environ
    required_true = ("LIVE_TRADING_ENABLED",)
    required_false = ("DRY_RUN", "DISABLE_LIVE_TRADING", "FORCE_BLOCK_LIVE")
    if str(e.get("STRATEGY_MODE", "")).upper() != "LIVE": return False, "STRATEGY_MODE_NOT_LIVE"
    if str(e.get("KIS_ENV") or e.get("STRATEGY_ENV") or "").lower() not in {"prod", "production", "live", "real"}: return False, "KIS_NOT_LIVE"
    for key in required_true:
        if str(e.get(key, "0")).lower() not in {"1", "true", "yes", "on"}: return False, f"{key}_OFF"
    for key in required_false:
        if str(e.get(key, "0")).lower() in {"1", "true", "yes", "on"}: return False, key
    return True, ""


def _snapshot(path: Path, trade_date: date) -> KRRegimeSnapshot | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        states = {k: KRMarketState(**v) for k, v in raw["market_states"].items()}
        policies = {k: KRMarketExecutionPolicy(**v) for k, v in raw["market_policies"].items()}
        return KRRegimeSnapshot(as_of=raw["as_of"], global_state=raw["global_state"], market_states=states,
            data_quality=raw["data_quality"], execution_policy=KRExecutionPolicy(**raw["execution_policy"]),
            market_policies=policies, source=raw.get("source", "PREP"))
    except (OSError, ValueError, TypeError, KeyError):
        return None


def run_sleeve(*, trading_date: date, positions: list[dict] | None = None, price: float = 0,
               quote_at: datetime | None = None, snapshot: Any = None,
               repository: InfiniteRepository | None = None,
               route: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
               orderable_cash: float = 0, pending: bool = False,
               daily_filled_buy_notional: float = 0) -> dict[str, Any]:
    config = InfiniteConfig.from_env(); config.validate()
    gate, gate_reason = global_order_gate()
    mode = "LIVE" if gate and config.real_order else "SHADOW" if config.real_order else "BLOCKED"
    logger.info("[KR_INF][CONFIG] enabled=%s real_order=%s allow_buy=%s allow_sell=%s symbol=%s capital_krw=%s units=%s unit_krw=%s global_live_gate=%s effective_order_mode=%s block_reason=%s",
                int(config.enabled), int(config.real_order), int(config.allow_buy), int(config.allow_sell), config.symbol,
                config.capital_krw, config.units, config.unit_krw, int(gate), mode, gate_reason)
    if not config.enabled: return {"status": "OFF", "orders": []}
    repository = repository or InfiniteRepository()
    locked = False
    try:
        repository.ensure_schema()
        locked = repository.try_lock()
        if not locked: return {"status": "BLOCK", "reason": "CONCURRENT_RUN", "orders": []}
        raw = next((p for p in positions or [] if str(p.get("symbol") or p.get("code") or "").zfill(6) == config.symbol), {})
        qty = int(float(raw.get("quantity") or raw.get("qty") or raw.get("hldg_qty") or 0))
        avg = float(raw.get("average_price") or raw.get("avg_price") or raw.get("pchs_avg_pric") or 0)
        state = repository.load_state(config.symbol)
        ownership = qty > 0 and (state is None or not state.cycle_id)
        if ownership: logger.error("[KR_INF][OWNERSHIP_CONFLICT] symbol=%s quantity=%s", config.symbol, qty)
        snap = snapshot or _snapshot(Path("artifacts/kr_regime_snapshot.json"), trading_date)
        risk = assess(snap, trading_date)
        local = getattr(snap, "market_states", {}).get("KOSPI") if snap else None
        logger.info("[KR_INF][REGIME] trade_date=%s symbol=%s global_state=%s kospi_state=%s kospi_score=%s kospi_data_quality=%s infinite_action=%s reason=%s",
                    trading_date, config.symbol, getattr(snap, "global_state", None), getattr(local, "state", None),
                    getattr(local, "score", None), getattr(local, "data_quality", None), risk.decision, risk.decision)
        decision = evaluate(config=config, state=state, broker=BrokerPosition(qty, avg, orderable_cash),
            quote=Quote(float(price), quote_at or datetime.now(timezone.utc)), trade_date=trading_date,
            regime=risk, pending=pending, daily_filled_buy_notional=daily_filled_buy_notional,
            ownership_conflict=ownership, reconciled=not (state and state.filled_quantity != qty))
        logger.info("[KR_INF][DECISION] trade_date=%s symbol=%s decision=%s block_reason=%s quantity=%s client_order_key=%s effective_order_mode=%s",
                    trading_date, config.symbol, decision.action.value, decision.reason, decision.quantity, decision.client_order_key, mode)
        if decision.action == Action.BLOCK: logger.info("[KR_INF][BUY_BLOCKED] reason=%s", decision.reason)
        if decision.action not in {Action.BUY, Action.SELL} or not gate or not config.real_order or route is None:
            return {"status": "BLOCKED" if not gate else "SHADOW", "reason": gate_reason or decision.reason, "decision": decision, "orders": []}
        if decision.action == Action.BUY and not config.allow_buy or decision.action == Action.SELL and not config.allow_sell:
            return {"status": "BLOCKED", "reason": "SIDE_DISABLED", "orders": []}
        active = state or InfiniteState(cycle_id=str(uuid.uuid4()), cycle_status="RESERVED")
        intent = {"side": decision.action.value, "symbol": config.symbol, "quantity": decision.quantity,
            "strategy_id": active.strategy_id, "book": active.book, "cycle_id": active.cycle_id,
            "policy_version": config.policy_version, "client_order_key": decision.client_order_key,
            "trade_date": str(trading_date), "unit_intent": min(1.0, decision.estimated_cost / config.unit_krw),
            "market_state_at_decision": getattr(local, "state", None), "authoritative_average_price": avg,
            "decision_price": price}
        repository.save_state(replace(active, pending_order_key=decision.client_order_key))
        logger.info("[KR_INF][ORDER_INTENT] %s", json.dumps(intent, ensure_ascii=False))
        return {"status": "SUBMITTED", "orders": [route(intent)], "decision": decision}
    except Exception as exc:
        logger.exception("[KR_INF][ERROR] trade_date=%s symbol=%s error=%s", trading_date, config.symbol, exc)
        return {"status": "ERROR_ISOLATED", "reason": str(exc), "orders": []}
    finally:
        if locked:
            try: repository.unlock()
            except Exception: logger.exception("[KR_INF][ERROR] advisory_unlock_failed")


def run_session_hook(trading_date: date) -> dict[str, Any]:
    """Minimal session hook. Missing authoritative inputs deliberately produce zero orders."""
    return run_sleeve(trading_date=trading_date)
