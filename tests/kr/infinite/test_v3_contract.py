from dataclasses import replace
from datetime import date
from pathlib import Path

from trader.kr.infinite.accounting import apply_confirmed_fill
from trader.kr.infinite.models import Action, BrokerOrderState, BrokerPosition, OrderIntent
from trader.kr.infinite.strategy import KR_ADAPTIVE_TP_MAP, evaluate
from trader.kr.regime import STATE_ORDER
from .conftest import active

DAY = date(2026, 8, 21)


def test_kr_regime_map_exactly_matches_official_states():
    assert set(STATE_ORDER) == set(KR_ADAPTIVE_TP_MAP)


def test_normal_eleven_percent_is_adaptive_half_not_full(default_config):
    decision = evaluate(config=default_config, state=active(),
                        position=BrokerPosition(10, 10, 100, 111), trade_date=DAY,
                        market_state="KR_NORMAL")
    assert (decision.action, decision.reason, decision.qty) == (Action.SELL_PARTIAL, "TAKE_PROFIT_TP1", 5)
    assert decision.metadata["fallback_10pct_used"] == 0


def test_submitted_stage_blocks_even_when_pending_lookup_misses(default_config):
    state = replace(active(), metadata={"pending_profit_stage": "TP1_SUBMITTED"})
    decision = evaluate(config=default_config, state=state,
                        position=BrokerPosition(10, 10, 100, 111), trade_date=DAY,
                        market_state="KR_NORMAL", pending_sell=False)
    assert (decision.action, decision.reason) == (Action.WAIT, "KR_INF_PROFIT_SELL_PENDING")


def test_sell_fill_alone_promotes_confirmed_profit_stage():
    state = replace(active(), metadata={"pending_profit_stage": "TP1_DEFENSE_SUBMITTED"})
    intent = OrderIntent(1, state.cycle_id, DAY, "SELL_ALL", "key", 8)
    filled, qty, _ = apply_confirmed_fill(state, intent, BrokerOrderState("FILLED", 8, 840, 105), DAY)
    assert qty == 8
    assert filled.metadata["profit_stage"] == "TP1_DEFENSE_FILLED"
    assert filled.metadata["pending_profit_stage"] is None


def test_pb1_does_not_own_sleeve_and_standard_engine_reserves_symbol():
    runner = Path("trader/pb1_runner.py").read_text()
    engine = Path("trader/pb1_engine.py").read_text()
    independent = Path("trader/kr/infinite/session_runner.py").read_text()
    assert "run_kr_infinite_sleeve_tick(" not in runner
    assert "run_canonical_session" in independent
    assert 'if code == "122630":' not in engine
    assert "enforce_kr_order_ownership" in engine
    assert "KR_INF_OWNERSHIP_RESERVED" in engine


def test_partial_sell_fill_keeps_submitted_stage_pending():
    state = replace(active(), metadata={"pending_profit_stage": "TP1_SUBMITTED"})
    intent = OrderIntent(2, state.cycle_id, DAY, "SELL_ALL", "partial", 10)
    partial, qty, _ = apply_confirmed_fill(state, intent, BrokerOrderState("PARTIALLY_FILLED", 4, 420, 105), DAY)
    assert qty == 4
    assert partial.metadata["pending_profit_stage"] == "TP1_SUBMITTED"
    assert partial.metadata["partial_profit_stage"] == "TP1_PARTIAL"
    assert not str(partial.metadata.get("profit_stage") or "").endswith("_FILLED")


def test_pb1_route_final_fence_rejects_reserved_symbol():
    from trader.pb1_engine import enforce_kr_order_ownership
    assert enforce_kr_order_ownership("122630", "KR_STANDARD") == (False, "KR_INF_OWNERSHIP_RESERVED")
    assert enforce_kr_order_ownership("122630", "KR_INFINITE") == (True, None)
