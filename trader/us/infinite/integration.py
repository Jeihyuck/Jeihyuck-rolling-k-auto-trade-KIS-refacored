from __future__ import annotations

import logging
import math
import uuid
from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Any, Callable

from .config import InfiniteConfig
from .models import Action, InfiniteState, PositionSnapshot, Status
from .repository import InfiniteRepository
from .policy_state import reserve_new_cycle, update_adaptive_policy_state
from .strategy import _trading_days_since, evaluate
from .risk_adapter import effective_regime

logger = logging.getLogger(__name__)


def owns_symbol(symbol: str, config: InfiniteConfig | None = None) -> bool:
    config = config or InfiniteConfig.from_env()
    return bool(config.enabled and str(symbol or "").upper().strip() == config.symbol)


def legacy_ownership_reserved(*, positions: list[dict], config: InfiniteConfig | None = None,
                              repository: InfiniteRepository | None = None) -> bool:
    """Keep open/pending Infinite TQQQ out of legacy even after the kill switch."""
    config = config or InfiniteConfig.from_env()
    if config.enabled:
        return True
    broker_has_tqqq = any(
        str(p.get("symbol") or p.get("code") or "").upper() == config.symbol
        and _position(p, 0).qty > 0 for p in positions or []
    )
    try:
        repository = repository or InfiniteRepository()
        if repository.has_pending_infinite_order(config.symbol):
            return True
        if not broker_has_tqqq:
            return False
        repository.ensure_schema()
        state = repository.load_state(symbol=config.symbol)
        return bool(state and state.cycle_id and state.total_filled_notional > 0)
    except Exception as exc:
        # With an open broker position and uncertain ownership, never transfer it.
        if broker_has_tqqq:
            logger.warning("[TQQQ_INF][OWNERSHIP][RESERVED] reason=ownership_uncertain error=%s", exc)
        return True


def exclude_owned(rows: list[dict], config: InfiniteConfig | None = None,
                  *, reserved: bool | None = None) -> list[dict]:
    """Exclude the dedicated symbol from standard strategy inputs.

    Ownership is invariant and intentionally independent of feature/real-order
    switches. Disabling the sleeve must never hand TQQQ back to US_STANDARD.
    """
    config = config or InfiniteConfig.from_env()
    from trader.us.strategy_ownership import exclude_non_standard
    return exclude_non_standard(rows)


def _position(raw: dict | None, price: float) -> PositionSnapshot:
    raw = raw or {}
    def number(*keys: str) -> float:
        for key in keys:
            try:
                value = float(str(raw.get(key, "")).replace(",", ""))
                if value > 0: return value
            except (TypeError, ValueError): pass
        return 0.0
    orderable = None
    for key in ("orderable_qty", "sellable_qty", "ord_psbl_qty", "ovrs_ord_psbl_qty"):
        if key in raw and raw.get(key) not in (None, ""):
            try:
                orderable = max(0, int(float(str(raw.get(key)).replace(",", ""))))
            except (TypeError, ValueError):
                orderable = None
            break
    return PositionSnapshot(
        qty=int(number("qty", "quantity", "holding_qty")),
        orderable_qty=orderable,
        average_price=number("avg_price_usd", "avg_price", "avg_cost", "pchs_avg_pric"),
        price=price or number("current_price", "last_price", "price", "ovrs_now_pric"),
        exchange=str(raw.get("exchange") or "NASDAQ"),
    )


def recover_state_from_broker(*, config: InfiniteConfig, broker: PositionSnapshot,
                              trading_date: date, cycle_id: str | None = None) -> InfiniteState:
    """Recover ownership/state from authoritative KIS balance facts only."""
    notional = broker.qty * broker.average_price
    return InfiniteState(
        symbol=config.symbol, cycle_id=cycle_id or str(uuid.uuid4()),
        cycle_start_date=trading_date, anchor_price=broker.average_price or None,
        core_filled_notional=min(notional, config.core_capital_usd),
        reserve_filled_notional=max(0.0, notional - config.core_capital_usd),
        status=Status.ACTIVE,
        metadata={
            "strategy_owner": "TQQQ_INFINITE", "strategy_name": "TQQQ_INFINITE",
            "strategy_version": config.policy_version, "sleeve_id": "TQQQ_INFINITE",
            "ownership_source": "SYMBOL_INVARIANT_RECOVERY",
            "recovery_accounting_uncertain": True,
            "broker_qty": broker.qty, "broker_orderable_qty": broker.orderable_qty,
            "broker_average_price": broker.average_price,
        },
    )


def run_sleeve(*, positions: list[dict], price: float, trading_date: date, overlay: dict,
               repository: InfiniteRepository | None = None,
               route: Callable[[dict], dict] | None = None) -> dict[str, Any]:
    """Exception-isolated boundary. OFF returns before any DB access."""
    config = InfiniteConfig.from_env()
    if not config.enabled:
        return {"status": "OFF", "orders": []}
    try:
        valid_quote = isinstance(price, (int, float)) and math.isfinite(float(price)) and float(price) > 0
        quote_stale = bool(overlay.get("tqqq_quote_stale") or overlay.get("tqqq_quote_suspect")
                           or overlay.get("tqqq_quote_degraded"))
        valid_quote = valid_quote and not quote_stale
        logger.info("[TQQQ_INF][QUOTE] price=%s source=%s stale=%s valid=%s", price,
                    overlay.get("tqqq_quote_source", "provider"), int(quote_stale), int(valid_quote))
        if not valid_quote:
            return {"status": "BLOCK", "reason": "tqqq_quote_invalid", "orders": []}
        repository = repository or InfiniteRepository()
        repository.ensure_schema()
        raw = next((p for p in positions if str(p.get("symbol") or p.get("code") or "").upper() == config.symbol), None)
        broker = _position(raw, price)
        logger.info("[TQQQ_INF][OWNERSHIP] symbol=TQQQ owner=TQQQ_INFINITE holding_qty=%s orderable_qty=%s",
                    broker.qty, broker.orderable_qty)
        logger.info("[TQQQ_INF][MARKET_DATA] context_quality=%s quote_stale=%s qqq_close=%s qqq_ma50=%s qqq_ma200=%s",
                    overlay.get("tqqq_context_quality"), int(quote_stale), overlay.get("qqq_completed_close"),
                    overlay.get("qqq_ma50"), overlay.get("qqq_ma200"))
        state = repository.load_state(symbol=config.symbol)
        if state is None:
            if broker.qty > 0:
                cycle_id = repository.find_recovery_cycle_id(config.symbol) if hasattr(repository, "find_recovery_cycle_id") else None
                state = recover_state_from_broker(config=config, broker=broker,
                                                  trading_date=trading_date, cycle_id=cycle_id)
                repository.save_state(state)
                logger.warning("[TQQQ_INF][OWNERSHIP_RECOVERY] symbol=TQQQ qty=%s orderable_qty=%s average_price=%s cycle_id=%s ownership_source=SYMBOL_INVARIANT_RECOVERY",
                               broker.qty, broker.orderable_qty, broker.average_price, state.cycle_id)
            else:
                state = InfiniteState(symbol=config.symbol)
        elif broker.qty > 0 and (not state.cycle_id or state.total_filled_notional <= 0):
            recovered = recover_state_from_broker(
                config=config, broker=broker, trading_date=trading_date,
                cycle_id=state.cycle_id or (
                    repository.find_recovery_cycle_id(config.symbol)
                    if hasattr(repository, "find_recovery_cycle_id") else None
                ),
            )
            state = replace(
                state, cycle_id=recovered.cycle_id,
                cycle_start_date=state.cycle_start_date or recovered.cycle_start_date,
                anchor_price=state.anchor_price or recovered.anchor_price,
                core_filled_notional=recovered.core_filled_notional,
                reserve_filled_notional=recovered.reserve_filled_notional,
                status=Status.ACTIVE,
                metadata={**state.metadata, **recovered.metadata},
            )
            repository.save_state(state)
        if state is not None and broker.qty > 0:
            needs_attribution_backfill = (
                state.metadata.get("strategy_owner") != "TQQQ_INFINITE"
                or state.metadata.get("sleeve_id") != "TQQQ_INFINITE"
            )
            ownership = {
                "strategy_owner": "TQQQ_INFINITE", "strategy_name": "TQQQ_INFINITE",
                "strategy_version": config.policy_version, "sleeve_id": "TQQQ_INFINITE",
                "broker_qty": broker.qty, "broker_orderable_qty": broker.orderable_qty,
                "broker_average_price": broker.average_price,
            }
            if needs_attribution_backfill:
                ownership["ownership_source"] = "SYMBOL_INVARIANT_RECOVERY"
            merged_metadata = {**state.metadata, **ownership}
            if merged_metadata != state.metadata:
                state = replace(state, metadata=merged_metadata)
                repository.save_state(state)
            if needs_attribution_backfill and hasattr(repository, "backfill_tqqq_attribution"):
                repository.backfill_tqqq_attribution(config.policy_version)
        if state is not None:
            state = repository.reconcile_metadata(state, trading_date=trading_date, broker_qty=broker.qty,
                                                  broker_average_price=broker.average_price,
                                                  core_cap=config.core_capital_usd,
                                                  rebound_cooldown=config.rebound_cooldown)
            if state.metadata.get("last_buy_fill_price"):
                state = replace(state, metadata={**state.metadata, "recovery_accounting_uncertain": False})
            state = update_adaptive_policy_state(state=state, trading_date=trading_date,
                                                 overlay=overlay, config=config)
            # A cycle becomes ACTIVE only from broker/fill evidence, never from
            # an order request or ACK (PostgreSQL and KIS are not one transaction).
            if broker.qty > 0 and state.core_filled_notional + state.reserve_filled_notional > 0 and not state.cycle_id:
                state = replace(state, cycle_id=str(uuid.uuid4()), cycle_start_date=state.last_buy_date or trading_date,
                                status=Status.ACTIVE)
            repository.save_state(state)
        pending_buy, pending_sell = repository.pending_sides(trading_date, config.symbol)
        pending_buy_notional = (repository.pending_buy_notional(trading_date, config.symbol)
                                if hasattr(repository, "pending_buy_notional") else 0.0)
        daily = 0.0
        if state is not None:
            _cycle, daily, _sells, _last, _anchor = repository.fill_accounting(state, trading_date)
        decision_state = state
        was_full_exit_pending = bool(state and state.status == Status.EXIT_PENDING)
        regime, multiplier, regime_reserve_permission, entry_allowed, regime_reason = effective_regime(overlay)
        effective_reserve_available = bool(
            state is not None and state.reserve_unlocked and regime_reserve_permission
        )
        logger.info("[TQQQ_INF][REGIME_DECISION] raw_market_state=%s raw_standard_regime=%s tqqq_effective_regime=%s buy_multiplier=%s regime_reserve_permission=%s reserve_unlocked=%s effective_reserve_available=%s entry_allowed=%s reason=%s",
                    overlay.get("market_state"), overlay.get("market_regime"), regime, multiplier,
                    int(regime_reserve_permission), int(bool(state and state.reserve_unlocked)),
                    int(effective_reserve_available), int(entry_allowed), regime_reason)
        decision = evaluate(config=config, state=decision_state, position=broker, trading_date=trading_date,
                            pending_buy=pending_buy, pending_sell=pending_sell,
                            daily_filled_buy_notional=daily, overlay=overlay,
                            entry_allowed=entry_allowed, buy_multiplier=multiplier,
                            regime_reserve_permission=regime_reserve_permission,
                            effective_regime_name=regime)
        logger.info("[TQQQ_INF][BUY_POLICY] effective_regime=%s multiplier=%s entry_evaluation_allowed=%s final_notional=%s reason=%s",
                    regime, multiplier, int(entry_allowed), decision.notional, decision.reason)
        calculated_return = ((broker.price / broker.average_price) - 1
                             if broker.average_price > 0 and broker.price > 0 else None)
        logger.info("[TQQQ_INF][FULL_EXIT] holding_qty=%s orderable_qty=%s pending_sell=%s broker_avg=%s executable_price=%s calculated_return=%s target_return=%s requested_full_exit_qty=%s cycle_id=%s action=%s block_reason=%s",
                    broker.qty, broker.orderable_qty, int(pending_sell), broker.average_price, broker.price,
                    calculated_return, config.take_profit_pct, decision.qty, getattr(state, "cycle_id", None),
                    decision.action.value, decision.reason if decision.action != Action.SELL else "")
        logger.info("[TQQQ_INF][RECONCILE] broker_qty=%s state_deployed=%s ownership_source=%s",
                    broker.qty, getattr(state, "total_filled_notional", None),
                    (getattr(state, "metadata", {}) or {}).get("ownership_source"))
        if decision.reason == "unknown_market_risk":
            logger.warning("[TQQQ_INF][MARKET_STATE_CONTRACT_MISMATCH] market_state=%s", overlay.get("market_state"))
        md = getattr(state, "metadata", {}) or {}
        days_since_buy = _trading_days_since(getattr(state, "last_buy_date", None), trading_date)
        logger.info("[TQQQ_INF][POLICY] policy_version=%s market_state=%s market_regime=%s long_trend=%s qqq_drawdown_252=%s qqq_rv20=%s qqq_efficiency20=%s chop_high_vol=%s capital_preservation=%s broker_qty=%s broker_avg=%s tqqq_price=%s last_buy_fill_price=%s days_since_buy=%s cycle_age=%s core_filled=%s reserve_filled=%s remaining_units=%s reserve_unlocked=%s action=%s reason=%s",
                    config.policy_version, overlay.get("market_state"), overlay.get("market_regime"), md.get("long_trend"),
                    overlay.get("qqq_drawdown_252"), overlay.get("qqq_realized_vol_20d"), overlay.get("qqq_trend_efficiency_20d"),
                    md.get("chop_high_vol"), md.get("capital_preservation"), broker.qty, broker.average_price, broker.price,
                    md.get("last_buy_fill_price"), days_since_buy, getattr(state, "cycle_age_trading_days", None), getattr(state, "core_filled_notional", None),
                    getattr(state, "reserve_filled_notional", None), md.get("remaining_units"), getattr(state, "reserve_unlocked", None),
                    decision.action.value, decision.reason)
        logger.info("[TQQQ_INF][STATE] cycle_id=%s status=%s price=%s broker_qty=%s broker_avg=%s anchor=%s drawdown=%s cycle_age=%s core_filled=%s reserve_filled=%s market_state=%s market_reason=%s crash_streak=%s reserve_unlocked=%s pending_buy=%s pending_sell=%s",
                    getattr(state, "cycle_id", None), getattr(getattr(state, "status", None), "value", None), broker.price, broker.qty, broker.average_price,
                    getattr(state, "anchor_price", None), (broker.price / state.anchor_price - 1 if state and state.anchor_price else None),
                    getattr(state, "cycle_age_trading_days", None), getattr(state, "core_filled_notional", None), getattr(state, "reserve_filled_notional", None),
                    overlay.get("market_state"), overlay.get("reason") or overlay.get("market_reason"), getattr(state, "market_crash_streak", None),
                    getattr(state, "reserve_unlocked", None), pending_buy, pending_sell)
        logger.info("[TQQQ_INF][DECISION] action=%s qty=%s notional=%.2f reason=%s shadow=%s",
                    decision.action.value, decision.qty, decision.notional, decision.reason, int(not config.real_order))
        needs_new_cycle = bool(
            decision.action == Action.BUY and state
            and (not state.cycle_id or state.status == Status.COMPLETE)
        )
        if state and decision.next_status and state.status != decision.next_status and not needs_new_cycle:
            state = replace(state, status=decision.next_status)
            repository.save_state(state)
        if needs_new_cycle and state:
            # Reserving a cycle identity is metadata only. Capital and ACTIVE
            # state still require later broker/fill evidence.
            state = reserve_new_cycle(state, trading_date)
            repository.save_state(state)
        if not config.real_order or decision.action not in {Action.BUY, Action.SELL}:
            return {"status": "SHADOW" if not config.real_order else decision.action.value,
                    "decision": decision, "orders": []}
        if route is None:
            return {"status": "BLOCK", "reason": "router_unavailable", "decision": decision, "orders": []}
        if not math.isfinite(broker.price) or broker.price <= 0:
            return {"status": "BLOCK", "reason": "tqqq_price_unavailable", "decision": decision, "orders": []}
        theme_cluster = "ETF_INDEX"
        classification_source = "TQQQ_INFINITE_POLICY"
        position_state = "HELD" if broker.qty > 0 else "NOT_HELD"
        position_action = ("FULL_EXIT_SELL" if decision.action == Action.SELL else
                           "ADD_TO_EXISTING_BUY" if broker.qty > 0 else "NEW_POSITION_BUY")
        policy_action = ("REBOUND_PROBE" if decision.action == Action.BUY
                         and str(overlay.get("market_state")) == "DEFENSE_CRASH_REBOUND" else None)
        lifecycle_id = str((state.metadata or {}).get("position_lifecycle_id") or state.cycle_id)
        avg_asof = str(raw.get("broker_avg_price_asof") or raw.get("balance_asof") or datetime.now(timezone.utc).isoformat()) if raw else ""
        sell_contract = {
            "broker_avg_price": broker.average_price,
            "broker_avg_price_source": str((raw or {}).get("broker_avg_price_source") or "kis_pchs_avg_pric"),
            "broker_avg_price_currency": "USD", "broker_avg_price_asof": avg_asof,
            "balance_source": "kis_balance_authoritative", "authoritative_positions": True,
            "position_lifecycle_id": lifecycle_id, "tp_threshold_fraction": str(config.take_profit_pct),
            "holding_qty": broker.qty, "orderable_qty": broker.orderable_qty,
            "sellable_qty": broker.orderable_qty, "available_qty": broker.orderable_qty,
            "partial_exit_allowed": False,
        } if decision.action == Action.SELL else {}
        client_order_key = f"TQQQ_INF_V3:{state.cycle_id}:{trading_date.isoformat()}:{decision.action.value}"
        if decision.action == Action.SELL and was_full_exit_pending:
            sequence = (repository.next_full_exit_sequence(trading_date, state.cycle_id)
                        if hasattr(repository, "next_full_exit_sequence") else 1)
            client_order_key += f":RETRY:{sequence}"
        intent = {
            "symbol": config.symbol, "exchange": broker.exchange or "NASDAQ", "side": decision.action.value,
            "qty": decision.qty, "limit_price": broker.price, "notional_usd": decision.notional,
            "trade_date": trading_date.isoformat(),
            "client_order_key": client_order_key,
            "strategy": "TQQQ_INFINITE_V3", "strategy_owner": "TQQQ_INFINITE",
            "strategy_name": "TQQQ_INFINITE", "strategy_version": config.policy_version,
            "sleeve_id": "TQQQ_INFINITE", "theme_cluster": theme_cluster,
            "classification_source": classification_source, "position_state": position_state,
            "position_action": position_action,
            **sell_contract,
            "reason": "TAKE_PROFIT_TQQQ_INFINITE" if decision.action == Action.SELL else decision.reason,
            "meta": {"strategy": "TQQQ_INFINITE_V3", "reason": decision.reason, **sell_contract,
                     "strategy_owner": "TQQQ_INFINITE", "strategy_name": "TQQQ_INFINITE",
                     "strategy_version": config.policy_version, "sleeve_id": "TQQQ_INFINITE",
                     "theme_cluster": theme_cluster, "classification_source": classification_source,
                     "position_state": position_state, "position_action": position_action,
                     "policy_action": policy_action,
                     "book": "TQQQ_INFINITE", "horizon": "INFINITE_CYCLE",
                     "cycle_id": state.cycle_id,
                     "tqqq_daily_committed_before_usd": daily + pending_buy_notional,
                     "tqqq_cycle_committed_before_usd": state.total_filled_notional + pending_buy_notional,
                     "tqqq_max_daily_buy_usd": config.max_daily_buy_usd,
                     "tqqq_max_total_capital_usd": config.max_total_capital_usd},
        }
        result = route(intent)
        logger.info("[TQQQ_INF][ORDER_STATUS] side=%s requested_qty=%s status=%s cycle_id=%s",
                    decision.action.value, decision.qty, result.get("status", "UNKNOWN"), state.cycle_id)
        return {"status": result.get("status", "UNKNOWN"), "decision": decision, "orders": [result]}
    except Exception as exc:
        logger.exception("[TQQQ_INF][BLOCK] reason=isolated_exception error=%s", exc)
        return {"status": "BLOCK", "reason": "isolated_exception", "error": str(exc), "orders": []}
