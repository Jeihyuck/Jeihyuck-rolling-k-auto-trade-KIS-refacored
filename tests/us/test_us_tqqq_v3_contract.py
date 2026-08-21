from dataclasses import replace
from datetime import date

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot
from trader.us.infinite.risk_adapter import CANONICAL_MARKET_STATES
from trader.us.infinite.strategy import (INTRADAY_OVERLAY_STATES, US_TQQQ_ADAPTIVE_TP_MAP,
                                         evaluate, normalize_tqqq_adaptive_state)

DAY = date(2026, 8, 21)


def test_us_map_covers_all_official_input_labels():
    effective_labels = {"RISK_ON", "GROWTH_LEADERSHIP", "NEUTRAL", "DEFENSIVE", "RISK_OFF",
                        "CAPITAL_PRESERVATION", "CHOP_HIGH_VOL", "UNKNOWN"}
    assert CANONICAL_MARKET_STATES | effective_labels <= set(US_TQQQ_ADAPTIVE_TP_MAP)


def test_intraday_pb1_labels_are_never_tqqq_tp_states():
    for label in INTRADAY_OVERLAY_STATES:
        assert normalize_tqqq_adaptive_state({"market_state": label}, "") == "UNKNOWN"


def test_normal_eleven_percent_remains_partial_tp1():
    decision = evaluate(config=InfiniteConfig(), state=InfiniteState(cycle_id="cycle"),
                        position=PositionSnapshot(qty=10, average_price=100, price=111, orderable_qty=10),
                        trading_date=DAY, overlay={"market_state": "NORMAL"})
    assert (decision.action, decision.reason, decision.qty) == (Action.SELL, "TAKE_PROFIT_TP1", 5)


def test_submitted_stage_blocks_without_pending_order_lookup():
    state = replace(InfiniteState(cycle_id="cycle"), metadata={"pending_profit_stage": "TP1_REBOUND_SUBMITTED"})
    decision = evaluate(config=InfiniteConfig(), state=state,
                        position=PositionSnapshot(qty=10, average_price=100, price=111, orderable_qty=10),
                        trading_date=DAY, overlay={"market_state": "NORMAL"}, pending_sell=False)
    assert (decision.action, decision.reason) == (Action.WAIT, "tqqq_profit_sell_pending")
