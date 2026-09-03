from datetime import date

import trader.us.db.repos as repos
from trader.us.data_provider import normalize_us_order_status_row
from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.lifecycle import rollover_partial_fill
from trader.us.infinite.models import InfiniteState, PositionSnapshot, Status
from trader.us.infinite.strategy import evaluate
from trader.us.runner.trade_tick_runner import (
    apply_crash_rebound_entry_recovery,
)


def _order():
    return {
        "client_order_key": "TQQQ_INF:cycle:2026-09-03:BUY",
        "order_no": "123",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": 2,
        "qty_filled": 0,
        "status": "OPEN",
        "meta": {},
    }


def test_cancel_broker_open_keeps_pending_order():
    repos._MEM_ORDERS.clear()
    assert repos.save_order_ack(_order(), trade_date="2026-09-03")
    observed = normalize_us_order_status_row({
        "odno": "000123", "pdno": "TQQQ", "sll_buy_dvsn_cd": "02",
        "ft_ord_qty": "2", "ft_ccld_qty": "0", "nccs_qty": "2",
    })
    assert observed["status"] == "OPEN"
    assert repos.apply_broker_order_observation(
        trade_date="2026-09-03", client_order_key=_order()["client_order_key"],
        raw_order_no=observed["raw_order_no"],
        canonical_order_no=observed["canonical_order_no"], symbol="TQQQ",
        side="BUY", requested_qty=2, filled_qty=0, remaining_qty=2,
        broker_status=observed["status"], evidence_type="test",
    )["status"] == "OK"
    assert repos._MEM_ORDERS[0]["status"] == "OPEN"


def test_cancel_broker_cancelled_apply_failure_is_not_terminal():
    repos._MEM_ORDERS.clear()
    result = repos.apply_broker_order_observation(
        trade_date="2026-09-03", client_order_key="missing",
        raw_order_no="123", canonical_order_no="123", symbol="TQQQ",
        side="BUY", requested_qty=2, filled_qty=0, remaining_qty=0,
        broker_status="CANCELLED", evidence_type="test",
    )
    assert result["status"] == "ORDER_IDENTITY_NOT_UNIQUE"


def test_cancel_broker_cancelled_apply_success_terminalizes_order():
    repos._MEM_ORDERS.clear()
    order = _order()
    assert repos.save_order_ack(order, trade_date="2026-09-03")
    result = repos.apply_broker_order_observation(
        trade_date="2026-09-03", client_order_key=order["client_order_key"],
        raw_order_no="123", canonical_order_no="123", symbol="TQQQ",
        side="BUY", requested_qty=2, filled_qty=0, remaining_qty=0,
        broker_status="CANCELLED", evidence_type="test",
    )
    assert result["status"] == "OK"
    assert repos._MEM_ORDERS[0]["status"] == "CANCELLED"


def test_overcap_partial_rollover_preserves_sell_lane():
    state = InfiniteState(
        cycle_id="cycle", status=Status.ACTIVE,
        core_filled_notional=7500, reserve_filled_notional=3300,
    )
    result = rollover_partial_fill(
        state, order_status="FILLED", filled_qty=10, requested_qty=10,
        filled_notional=1000, position=PositionSnapshot(qty=10, average_price=1080, price=1200),
        trading_date=date(2026, 9, 3), order_key="sell-1", hard_cap=10000,
        broker_position_authoritative=True,
    )
    assert result.metadata["rollover_seed_capital_usd"] == 10800
    assert result.metadata["hard_cap_exceeded"] is True
    assert result.metadata["buy_round_effective_unit_usd"] == 0
    assert result.status == Status.ACTIVE
    decision = evaluate(
        state=result,
        position=PositionSnapshot(qty=10, orderable_qty=10, average_price=1080, price=1200),
        trading_date=date(2026, 9, 3), config=InfiniteConfig(),
        pending_buy=False, pending_sell=False, entry_allowed=True,
    )
    assert decision.action.value == "SELL"


def test_non_authoritative_residual_does_not_rollover():
    state = InfiniteState(cycle_id="cycle", status=Status.ACTIVE,
                          metadata={"buy_round": 2})
    result = rollover_partial_fill(
        state, order_status="FILLED", filled_qty=10, requested_qty=10,
        filled_notional=1000, position=PositionSnapshot(qty=20, average_price=100, price=120),
        trading_date=date(2026, 9, 3), order_key="stale", hard_cap=10000,
    )
    assert result == state


def test_authoritative_terminal_fill_rollover_is_idempotent():
    state = InfiniteState(cycle_id="cycle", status=Status.ACTIVE)
    kwargs = dict(
        order_status="FILLED", filled_qty=10, requested_qty=10,
        filled_notional=1000, position=PositionSnapshot(qty=10, average_price=80, price=100),
        trading_date=date(2026, 9, 3), order_key="once", broker_position_authoritative=True,
    )
    first = rollover_partial_fill(state, **kwargs)
    second = rollover_partial_fill(first, **kwargs)
    assert first == second
    assert first.metadata["rollover_seed_capital_usd"] == 800


def test_rollover_uses_configured_core_cap():
    result = rollover_partial_fill(
        InfiniteState(cycle_id="cycle", status=Status.ACTIVE),
        order_status="FILLED", filled_qty=10, requested_qty=10,
        filled_notional=1000, position=PositionSnapshot(qty=10, average_price=900, price=1000),
        trading_date=date(2026, 9, 3), order_key="core-cap", hard_cap=10000, core_cap=8000,
        broker_position_authoritative=True,
    )
    assert result.core_filled_notional == 8000
    assert result.reserve_filled_notional == 1000


def test_rollover_below_configured_core_has_no_reserve():
    result = rollover_partial_fill(
        InfiniteState(cycle_id="cycle", status=Status.ACTIVE),
        order_status="FILLED", filled_qty=10, requested_qty=10,
        filled_notional=1000, position=PositionSnapshot(qty=10, average_price=600, price=1000),
        trading_date=date(2026, 9, 3), order_key="core-cap-low", hard_cap=10000, core_cap=8000,
        broker_position_authoritative=True,
    )
    assert result.core_filled_notional == 6000
    assert result.reserve_filled_notional == 0


def test_overcap_state_blocks_buy_after_sell_evaluation():
    state = InfiniteState(
        cycle_id="cycle", status=Status.ACTIVE,
        core_filled_notional=7500, reserve_filled_notional=3300,
        metadata={"hard_cap_exceeded": True, "broker_deployed_notional_usd": 10800,
                  "ownership_source": "KIS_BALANCE_AUTHORITATIVE"},
    )
    decision = evaluate(
        state=state,
        position=PositionSnapshot(qty=10, orderable_qty=10, average_price=1080, price=1080),
        trading_date=date(2026, 9, 3), config=InfiniteConfig(),
        pending_buy=False, pending_sell=False, entry_allowed=True,
    )
    assert decision.action.value == "BLOCK"
    assert "HARD_CAP" in decision.reason.upper()


def test_below_cap_rollover_clears_overcap_flag():
    state = InfiniteState(
        cycle_id="cycle", status=Status.ACTIVE,
        core_filled_notional=7500, reserve_filled_notional=3300,
        metadata={"hard_cap_exceeded": True, "broker_deployed_notional_usd": 10800,
                  "ownership_source": "KIS_BALANCE_AUTHORITATIVE"},
    )
    result = rollover_partial_fill(
        state, order_status="FILLED", filled_qty=10, requested_qty=10,
        filled_notional=1000, position=PositionSnapshot(qty=10, average_price=950, price=1000),
        trading_date=date(2026, 9, 3), order_key="sell-2", hard_cap=10000,
        broker_position_authoritative=True,
    )
    assert result.metadata["rollover_seed_capital_usd"] == 9500
    assert result.metadata["hard_cap_exceeded"] is False
    assert result.metadata["hard_cap_exceeded_reason"] is None


def test_unresolved_ack_crash_rebound_keeps_buy_fenced():
    assert apply_crash_rebound_entry_recovery(
        False, False, market_state="DEFENSE_CRASH_REBOUND",
        prep_reason="risk_off_entry_block", execution_buy_fenced=True,
    ) == (False, False)


def test_unresolved_ack_crash_rebound_blocks_standard_buy():
    entry, symbols = apply_crash_rebound_entry_recovery(
        False, False, market_state="DEFENSE_CRASH_REBOUND",
        prep_reason="risk_off_entry_block", execution_buy_fenced=True,
    )
    assert entry is False
    assert symbols is False


def test_unresolved_ack_crash_rebound_blocks_tqqq_buy():
    assert apply_crash_rebound_entry_recovery(
        False, False, market_state="DEFENSE_CRASH_REBOUND",
        prep_reason="force_entry_block", execution_buy_fenced=True,
    ) == (False, False)


def test_clean_crash_rebound_restores_policy_buy_permission():
    assert apply_crash_rebound_entry_recovery(
        False, False, market_state="DEFENSE_CRASH_REBOUND",
        prep_reason="risk_off_entry_block", execution_buy_fenced=False,
    ) == (True, True)
