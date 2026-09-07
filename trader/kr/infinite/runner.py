"""Standalone KR Infinite runtime orchestration."""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Callable

from trader.kis_wrapper import KisAPI

from .accounting import apply_confirmed_fill
from .config import InfiniteConfig
from .diagnostics import log_decision
from .executor import KISExecutor
from .models import Action, BrokerPosition, Decision, State, Status
from .policy_state import cycle_id
from .reconciliation import reconcile
from .reconciliation import reconcile_order_fill_prices
from .repository import InfiniteRepository
from .regime_adapter import load_canonical_snapshot
from .risk_adapter import allows_new_cycle
from .strategy import KR_ADAPTIVE_TP_MAP, evaluate, trading_days_since

logger = logging.getLogger(__name__)
_LAST_GOOD_BALANCE: tuple[dict, datetime] | None = None


def _is_kr_infinite_opening_buy_blocked(at: datetime) -> tuple[bool, str]:
    """Return the KR Infinite entry-only gate; sell actions never call this gate."""
    if os.getenv("KR_OPENING_BUY_BLOCK_ENABLED", "1").strip().lower() not in {"1", "true", "yes", "on"}:
        return False, ""
    hour, minute = (int(part) for part in os.getenv("KR_MARKET_OPEN_HHMM", "09:00").split(":"))
    market_open = at.replace(hour=hour, minute=minute, second=0, microsecond=0)
    buy_start = market_open + timedelta(minutes=max(0, int(os.getenv("KR_OPENING_BUY_BLOCK_MINUTES", "30"))))
    return market_open <= at < buy_start, buy_start.strftime("%H:%M:%S")


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


def evaluate_exit_only(*, config: InfiniteConfig, state: State | None,
                       position: BrokerPosition, trade_date: date,
                       market_state: str | None = None,
                       orderable_cash: float = 0) -> Decision:
    """Evaluate KR_INFINITE exits only; BUY/RECOVERY/NEW_CYCLE are impossible."""
    decision = evaluate(config=config, state=state, position=position, trade_date=trade_date,
                        market_state=market_state or "KR_NORMAL", orderable_cash=orderable_cash,
                        allow_entry=False, best_ask=position.current_price)
    if decision.action not in {Action.SELL_PARTIAL, Action.SELL_ALL}:
        return Decision(Action.WAIT, "KR_INF_EXIT_ONLY_NO_SELL")
    return decision


def run_once(*, config: InfiniteConfig, kis, repository: InfiniteRepository,
             regime_provider: Callable[[], tuple[str | None, str]] = load_regime,
             trade_date: date | None = None, kis_env: str | None = None,
             allow_entry: bool = True, balance_snapshot: dict | None = None,
             now_kst_value: datetime | None = None) -> RunResult:
    """Execute one exit-first tick. Every dependency is injectable for integration tests."""
    day = trade_date or date.today()
    env = (kis_env or os.getenv("KIS_ENV") or "practice").lower()
    try:
        config.validate()
    except ValueError as exc:
        return RunResult(Decision(Action.BLOCK, str(exc), next_status=Status.FROZEN), None)
    if not config.enabled:
        return RunResult(Decision(Action.WAIT, "KR_INF_FEATURE_DISABLED"), None)

    executor = KISExecutor(kis, env, balance_snapshot=balance_snapshot)
    try:
        repository.ensure_schema()
        state = repository.load_state()
        # An intent journal is reconciled before any new decision. INTENT_CREATED
        # without a broker id is deliberately retained and never blindly retried.
        state, updates = _reconcile_pending(repository, executor, state, day)
        position = executor.position(config.symbol)
        if not math.isfinite(position.current_price) or position.current_price <= 0:
            if state is not None and updates:
                repository.persist_reconciliation(state, updates)
            decision = Decision(Action.BLOCK, "KR_INF_QUOTE_INVALID",
                                next_status=state.status if state else Status.READY)
            log_decision(decision=decision.action.value, reason=decision.reason, symbol=config.symbol)
            return RunResult(decision, state)
        if state is None and position.qty > 0:
            state = State(status=Status.ACTIVE, cycle_id=f"BROKER_ADOPTION-{day.isoformat()}",
                          cycle_start_date=day,
                          metadata={"ownership_source": "KR_INF_EXIT_ONLY_BROKER_ADOPTION",
                                    "broker_qty": position.qty,
                                    "broker_average_price": position.average_price,
                                    "broker_orderable_qty": position.orderable_qty})
            repository.save_state(state)
        unresolved_sell = any(i.side in {"SELL_PARTIAL", "SELL_ALL"} and broker.status not in {"FILLED", "CANCELLED", "REJECTED"}
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

        raw_market_state, quality = regime_provider()
        market_state = raw_market_state
        decision_allow_entry = bool(allow_entry)

        # Persist the last verified regime for exit continuity only. Missing
        # regime data can never authorize BUY/RECOVERY, but it must not make a
        # profitable existing Infinite position unmanageable because PB1 died.
        if state is not None and raw_market_state in KR_ADAPTIVE_TP_MAP:
            state = replace(
                state,
                metadata={
                    **(state.metadata or {}),
                    "last_market_state": raw_market_state,
                    "last_market_state_trade_date": day.isoformat(),
                    "last_market_state_quality": quality,
                },
            )
        elif position.qty > 0:
            last_market_state = str(
                ((state.metadata if state else {}) or {}).get("last_market_state") or ""
            ).upper()
            if last_market_state in KR_ADAPTIVE_TP_MAP:
                logger.warning(
                    "[KR_INF][REGIME][EXIT_ONLY_FALLBACK] raw=%s last=%s quality=%s action=disable_entry",
                    raw_market_state, last_market_state, quality,
                )
                market_state = last_market_state
                decision_allow_entry = False

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
        pending_sell = any(item.side in {"SELL_PARTIAL", "SELL_ALL"} for item in pending)

        if decision_allow_entry and position.qty == 0 and not pending_buy and allows_new_cycle(market_state):
            same_day = state is not None and state.last_exit_date == day and not config.same_day_restart
            if not same_day and (state is None or state.status in {Status.READY, Status.COMPLETE}):
                state = _new_cycle(state, executor, config, day)
                repository.save_state(state)

        # Exit-only and already-held ticks never need buying-power data.  This
        # avoids a second KIS transaction endpoint call (EGW00215) on the hot
        # sell/reconcile path; cash is fetched only for a possible new cycle.
        cash = (executor.orderable_cash(config.symbol, position.current_price)
                if decision_allow_entry and position.qty == 0 else 0.0)
        now_kst = now_kst_value or datetime.now(ZoneInfo("Asia/Seoul"))
        if now_kst.tzinfo is None:
            now_kst = now_kst.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        minutes_since_open = (now_kst.hour * 60 + now_kst.minute + now_kst.second / 60) - (9 * 60)
        decision = evaluate(config=config, state=state, position=position, trade_date=day,
                            market_state=market_state, orderable_cash=cash, pending_buy=pending_buy,
                            pending_sell=pending_sell, existing_intent_keys=existing_keys,
                            regime_data_quality=quality,
                            trading_days_since_last_buy=trading_days_since(state.last_buy_date if state else None, day),
                            allow_entry=decision_allow_entry,
                            best_ask=position.current_price,
                            minutes_since_open=minutes_since_open)
        if decision.metadata and state is not None:
            durable = {key: value for key, value in decision.metadata.items()
                       if key not in {"profit_stage", "desired_profit_stage", "tp1_sold_qty", "remaining_qty"}}
            state = replace(state, metadata={**state.metadata, **durable})
            repository.save_state(state)
        log_decision(decision=decision.action.value, reason=decision.reason, cycle_id=state.cycle_id if state else None,
                     symbol=config.symbol, broker_qty_before=position.qty, sell_qty=decision.qty if decision.action in {Action.SELL_PARTIAL, Action.SELL_ALL} else 0,
                     expected_qty_after=max(0, position.qty - decision.qty) if decision.action in {Action.SELL_PARTIAL, Action.SELL_ALL} else position.qty, market_state=market_state,
                     idempotency_key=decision.idempotency_key)
        if decision.action not in {Action.BUY, Action.RECOVERY, Action.SELL_PARTIAL, Action.SELL_ALL}:
            return RunResult(decision, state)
        if decision.action in {Action.BUY, Action.RECOVERY}:
            opening_blocked, buy_start = _is_kr_infinite_opening_buy_blocked(now_kst)
            if opening_blocked:
                logger.info(
                    "[OPENING_BUY_BLOCK][KR_INF] now=%s buy_start=%s symbol=%s decision=%s "
                    "action=SKIP_BUY reason=OPENING_30MIN_BUY_BLOCK exit_allowed=1",
                    now_kst.strftime("%H:%M:%S"), buy_start, config.symbol, decision.action.value,
                )
                return RunResult(
                    Decision(Action.WAIT, "OPENING_30MIN_BUY_BLOCK",
                             next_status=state.status if state else Status.READY),
                    state,
                )
        if not config.orders_allowed(env):
            return RunResult(Decision(Action.BLOCK, "KR_INF_CANONICAL_ORDER_GATE_CLOSED"), state)
        if state is None or not state.cycle_id:
            return RunResult(Decision(Action.BLOCK, "KR_INF_CYCLE_ID_MISSING", next_status=Status.FROZEN), state)
        if not repository.create_intent(state, decision, day, market_state):
            return RunResult(Decision(Action.WAIT, "DUPLICATE_INTENT"), state)

        if decision.action in {Action.SELL_PARTIAL, Action.SELL_ALL}:
            desired = str(decision.metadata.get("desired_profit_stage") or "")
            pending_stage = f"{desired}_SUBMITTED" if desired and not desired.endswith("_SUBMITTED") else desired
            state = replace(state, status=Status.EXIT_PENDING,
                            metadata={**state.metadata, "pending_profit_stage": pending_stage})
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
        for intent, broker_state in post_updates:
            reconciliation = reconcile_order_fill_prices(
                order_price=position.current_price,
                order_qty=int(getattr(intent, "requested_qty", 0) or 0),
                broker_order_no=getattr(intent, "broker_order_id", None),
                fill_price=getattr(broker_state, "filled_avg_price", None),
                fill_qty=getattr(broker_state, "filled_qty", None),
                broker_avg_after=post_position.average_price,
                close_avg_price=post_position.average_price,
            )
            logger.info("[KR_INF][ORDER_RECONCILIATION] %s", reconciliation)
            if state is not None:
                state = replace(state, metadata={**state.metadata, "last_order_reconciliation": reconciliation})
        state, _ = reconcile(state, post_position, day,
                             pending_sell=decision.action in {Action.SELL_PARTIAL, Action.SELL_ALL} and post_position.qty > 0,
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
    """Run one standalone Infinite tick with its own KIS/DB dependencies."""
    effective_allow_entry = bool(allow_entry and session in {"am", "afternoon"})
    logger.info("[KR_INFINITE][SESSION_HOOK] session=%s env=%s allow_entry=%s",
                session, env, int(effective_allow_entry))
    return run_once(config=InfiniteConfig.from_env(), kis=KisAPI(kis_env=env),
                    repository=InfiniteRepository(), kis_env=env, allow_entry=effective_allow_entry)


def run_kr_infinite_sleeve_tick(*, kis, balance_snapshot: dict, env: str,
                                trade_date: date, allow_entry: bool) -> RunResult:
    """Compatibility adapter for callers that already hold a balance snapshot.

    Production session orchestration is owned by the independent Infinite
    session runner and is never called from PB1.
    """
    return run_once(config=InfiniteConfig.from_env(), kis=kis, repository=InfiniteRepository(),
                    kis_env=env, trade_date=trade_date, allow_entry=allow_entry,
                    balance_snapshot=balance_snapshot)


if __name__ == "__main__":
    raise SystemExit(main())
