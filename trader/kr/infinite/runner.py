"""Standalone KR Infinite runtime orchestration."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, replace
from datetime import date
from typing import Callable

from trader.kis_wrapper import KisAPI

from .accounting import apply_confirmed_fill
from .config import InfiniteConfig
from .diagnostics import log_decision
from .executor import KISExecutor
from .models import Action, BrokerPosition, Decision, State, Status
from .policy_state import cycle_id
from .reconciliation import reconcile
from .repository import InfiniteRepository
from .regime_adapter import load_canonical_snapshot
from .risk_adapter import allows_new_cycle
from .strategy import evaluate, trading_days_since

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    decision: Decision
    state: State | None
    submitted: bool = False


def load_regime() -> tuple[str | None, str]:
    configured_path = os.getenv("KR_INFINITE_REGIME_PATH")
    view = load_canonical_snapshot(configured_path or None)
    return view.state, view.data_quality


def _new_cycle(state: State | None, executor: KISExecutor, config: InfiniteConfig, trade_date: date) -> State:
    allocated = executor.account_equity() * config.account_exposure_pct
    return State(cycle_id=cycle_id(trade_date), cycle_start_date=trade_date,
                 allocated_capital_krw=allocated, unit_krw=allocated / config.total_units,
                 status=Status.READY, last_exit_date=state.last_exit_date if state else None)


def _reconcile_pending(repo: InfiniteRepository, executor: KISExecutor, state: State | None,
                       trade_date: date) -> tuple[State | None, list]:
    updates = []
    for intent in repo.pending_intents():
        broker = executor.order_state(intent, trade_date)
        updates.append((intent, broker))
        if state is not None:
            state, _, _ = apply_confirmed_fill(state, intent, broker, trade_date)
    return state, updates


def run_once(*, config: InfiniteConfig, kis, repository: InfiniteRepository,
             regime_provider: Callable[[], tuple[str | None, str]] = load_regime,
             trade_date: date | None = None, kis_env: str | None = None,
             allow_entry: bool = True) -> RunResult:
    """Execute one exit-first tick. Every dependency is injectable for integration tests."""
    day = trade_date or date.today()
    env = (kis_env or os.getenv("KIS_ENV") or "practice").lower()
    try:
        config.validate()
    except ValueError as exc:
        return RunResult(Decision(Action.BLOCK, str(exc), next_status=Status.FROZEN), None)
    if not config.enabled:
        return RunResult(Decision(Action.WAIT, "KR_INF_FEATURE_DISABLED"), None)

    executor = KISExecutor(kis, env)
    try:
        repository.ensure_schema()
        state = repository.load_state()
        # An intent journal is reconciled before any new decision. INTENT_CREATED
        # without a broker id is deliberately retained and never blindly retried.
        state, updates = _reconcile_pending(repository, executor, state, day)
        position = executor.position(config.symbol)
        unresolved_sell = any(i.side == "SELL_ALL" and broker.status not in {"FILLED", "CANCELLED", "REJECTED"}
                              for i, broker in updates)
        state, reconcile_reason = reconcile(state, position, day, pending_sell=unresolved_sell,
                                             balance_grace_attempts=config.balance_reconcile_grace_attempts)
        if state is not None and updates:
            repository.persist_reconciliation(state, updates)
        if reconcile_reason.startswith("KR_INF_"):
            if state is not None:
                repository.save_state(state)
            decision = Decision(Action.BLOCK, reconcile_reason, next_status=Status.FROZEN)
            log_decision(decision=decision.action.value, reason=decision.reason, symbol=config.symbol)
            return RunResult(decision, state)
        if reconcile_reason == "FILL_CONFIRMED_BALANCE_PENDING":
            repository.save_state(state)
            return RunResult(Decision(Action.WAIT, reconcile_reason), state)

        market_state, quality = regime_provider()
        if state is not None:
            age = trading_days_since(state.cycle_start_date, day)
            crash_seen = state.crash_seen or market_state == "KR_DEFENSE_CRASH"
            normalized_after_crash = crash_seen and market_state in {"KR_NORMAL", "KR_RISK_ON", "KR_STRONG_RISK_ON"}
            state = replace(state, crash_seen=crash_seen,
                            reserve_unlocked=state.reserve_unlocked or normalized_after_crash,
                            cycle_age_trading_days=age,
                            capital_preservation=age >= config.long_cycle_days)
            repository.save_state(state)
        existing_keys = repository.intent_keys()
        pending = repository.pending_intents()
        pending_buy = any(item.side in {"BUY", "RECOVERY"} for item in pending)
        pending_sell = any(item.side == "SELL_ALL" for item in pending)

        if allow_entry and position.qty == 0 and not pending_buy and allows_new_cycle(market_state):
            same_day = state is not None and state.last_exit_date == day and not config.same_day_restart
            if not same_day and (state is None or state.status in {Status.READY, Status.COMPLETE}):
                state = _new_cycle(state, executor, config, day)
                repository.save_state(state)

        cash = executor.orderable_cash(config.symbol, position.current_price)
        decision = evaluate(config=config, state=state, position=position, trade_date=day,
                            market_state=market_state, orderable_cash=cash, pending_buy=pending_buy,
                            pending_sell=pending_sell, existing_intent_keys=existing_keys,
                            regime_data_quality=quality,
                            trading_days_since_last_buy=trading_days_since(state.last_buy_date if state else None, day),
                            allow_entry=allow_entry)
        log_decision(decision=decision.action.value, reason=decision.reason, cycle_id=state.cycle_id if state else None,
                     symbol=config.symbol, broker_qty=position.qty, market_state=market_state,
                     idempotency_key=decision.idempotency_key)
        if decision.action not in {Action.BUY, Action.RECOVERY, Action.SELL_ALL}:
            return RunResult(decision, state)
        if not config.orders_allowed(env):
            return RunResult(Decision(Action.BLOCK, "KR_INF_CANONICAL_ORDER_GATE_CLOSED"), state)
        if state is None or not state.cycle_id:
            return RunResult(Decision(Action.BLOCK, "KR_INF_CYCLE_ID_MISSING", next_status=Status.FROZEN), state)
        if not repository.create_intent(state, decision, day, market_state):
            return RunResult(Decision(Action.WAIT, "DUPLICATE_INTENT"), state)

        if decision.action == Action.SELL_ALL:
            state = replace(state, status=Status.EXIT_PENDING)
            repository.save_state(state)
        try:
            order_id, _ = executor.submit(decision, config.symbol)
        except Exception as exc:
            repository.mark_rejected(decision.idempotency_key, str(exc))
            return RunResult(Decision(Action.BLOCK, str(exc)), state)
        repository.mark_submitted(decision.idempotency_key, order_id)

        # One immediate reconciliation is safe; absence of a fill consumes no unit.
        state, post_updates = _reconcile_pending(repository, executor, state, day)
        post_position = executor.position(config.symbol)
        state, _ = reconcile(state, post_position, day,
                             pending_sell=decision.action == Action.SELL_ALL and post_position.qty > 0,
                             balance_grace_attempts=config.balance_reconcile_grace_attempts)
        if state is not None:
            repository.persist_reconciliation(state, post_updates)
        return RunResult(decision, state, submitted=True)
    except Exception as exc:
        logger.exception("[KR_INFINITE][BLOCK] reason=%s", exc)
        return RunResult(Decision(Action.BLOCK, str(exc), next_status=Status.FROZEN), locals().get("state"))


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    config = InfiniteConfig.from_env()
    kis = KisAPI(kis_env=os.getenv("KIS_ENV", "practice"))
    result = run_once(config=config, kis=kis,
                      repository=InfiniteRepository(), kis_env=os.getenv("KIS_ENV", "practice"))
    return 0 if result.decision.action != Action.BLOCK else 2


def run_canonical_session(*, session: str, env: str, allow_entry: bool = True) -> RunResult:
    """Auxiliary-sleeve hook called by the existing KR session owner."""
    effective_allow_entry = bool(allow_entry and session in {"am", "afternoon"})
    logger.info("[KR_INFINITE][SESSION_HOOK] session=%s env=%s allow_entry=%s",
                session, env, int(effective_allow_entry))
    return run_once(config=InfiniteConfig.from_env(), kis=KisAPI(kis_env=env),
                    repository=InfiniteRepository(), kis_env=env, allow_entry=effective_allow_entry)


if __name__ == "__main__":
    raise SystemExit(main())
