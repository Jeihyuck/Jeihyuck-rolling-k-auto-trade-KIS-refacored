from datetime import date

from trader.us.execution.order_router import route_order
import pytest

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.integration import exclude_owned
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot
from trader.us.infinite.risk_adapter import effective_regime
from trader.us.infinite.strategy import evaluate
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.pb1.us_exit_router import route_exit_by_book_horizon


def test_standard_profit_and_swing_exit_never_create_tqqq_intent():
    position = {"symbol": "TQQQ", "qty": 10, "orderable_qty": 10,
                "avg_price": 100, "current_price": 110, "exchange": "NASDAQ"}
    assert build_profit_capture_intents([position], {"profit_capture_enabled": True},
                                        profit_capture_state={}) == []
    assert route_exit_by_book_horizon(position, 80) is None
    assert exclude_owned([position]) == []


def test_router_rejects_non_owner_before_any_broker_or_db_work():
    result = route_order({"symbol": "TQQQ", "side": "BUY"})
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "tqqq_ownership_rejected"
    assert result["broker_submit"] is False


def test_every_documented_tqqq_regime_has_one_effective_policy():
    states = ["STRONG_RISK_ON", "RISK_ON", "NEUTRAL", "DEFENSIVE", "RISK_OFF",
              "CRASH", "DEFENSE_CRASH", "CHOP_HIGH_VOL", "CAPITAL_PRESERVATION"]
    for state in states:
        effective, multiplier, _reserve, allowed, reason = effective_regime(
            {"market_state": state, "market_regime": "DEFENSIVE" if state == "STRONG_RISK_ON" else state}
        )
        assert effective
        assert multiplier >= 0
        assert isinstance(allowed, bool)
        assert reason


def test_conflicting_defensive_regime_blocks_buy_but_never_safe_sell():
    effective, multiplier, reserve, allowed, _ = effective_regime(
        {"market_state": "STRONG_RISK_ON", "market_regime": "DEFENSIVE"}
    )
    assert (effective, multiplier, reserve, allowed) == ("DEFENSIVE", 0.0, False, False)
    config = InfiniteConfig(unit_usd=1000, max_daily_buy_usd=1000)
    blocked = evaluate(config=config, state=InfiniteState(), position=PositionSnapshot(price=100),
                       trading_date=date(2026, 8, 14), overlay={"market_state": "STRONG_RISK_ON"},
                       entry_allowed=allowed, buy_multiplier=multiplier)
    assert (blocked.action, blocked.reason) == (Action.BLOCK, "tqqq_effective_regime_entry_block")
    sold = evaluate(config=config, state=InfiniteState(cycle_id="c", core_filled_notional=100),
                    position=PositionSnapshot(qty=1, average_price=100, price=110),
                    trading_date=date(2026, 8, 14), overlay={"market_state": "STRONG_RISK_ON"},
                    entry_allowed=False, buy_multiplier=0)
    assert sold.action == Action.SELL


def test_strong_risk_on_multiplier_changes_real_order_size():
    regime, multiplier, _reserve, allowed, _ = effective_regime(
        {"market_state": "STRONG_RISK_ON", "market_regime": "STRONG_RISK_ON"}
    )
    assert (regime, multiplier, allowed) == ("STRONG_RISK_ON", 1.25, True)
    decision = evaluate(
        config=InfiniteConfig(unit_usd=400, max_daily_buy_usd=1000), state=InfiniteState(),
        position=PositionSnapshot(price=100), trading_date=date(2026, 8, 14),
        overlay={"market_state": "STRONG_RISK_ON"}, entry_allowed=allowed,
        buy_multiplier=multiplier,
    )
    assert (decision.action, decision.qty, decision.notional) == (Action.BUY, 5, 500)


@pytest.mark.parametrize("regime", ["CRASH", "DEFENSE_CRASH", "CAPITAL_PRESERVATION"])
def test_fail_closed_effective_regimes_block_new_buy(regime):
    _effective, multiplier, _reserve, allowed, _reason = effective_regime(
        {"market_state": regime, "market_regime": regime}
    )
    decision = evaluate(
        config=InfiniteConfig(), state=InfiniteState(), position=PositionSnapshot(price=100),
        trading_date=date(2026, 8, 14), overlay={"market_state": "STRONG_RISK_ON"},
        entry_allowed=allowed, buy_multiplier=multiplier,
    )
    assert decision.action == Action.BLOCK


def test_non_tqqq_symbol_configuration_fails_fast(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_SYMBOL", "QLD")
    with pytest.raises(ValueError, match="must be TQQQ"):
        InfiniteConfig.from_env()


def test_owner_attribution_is_mirrored_to_meta_and_survives_intent_store(monkeypatch):
    from trader.us.db import repos

    monkeypatch.delenv("PBCORE_DB_URL", raising=False)
    repos.reset_memory_stores()
    intent = {
        "symbol": "TQQQ", "side": "BUY", "qty": 1, "notional_usd": 100,
        "limit_price": 100, "trade_date": "2026-08-14", "client_order_key": "TQQQ_INF:test:BUY",
        "strategy": "TQQQ_INFINITE_V3", "strategy_owner": "TQQQ_INFINITE",
        "strategy_name": "TQQQ_INFINITE", "strategy_version": "ADAPTIVE_RUNWAY_V2",
        "sleeve_id": "TQQQ_INFINITE", "meta": {},
    }
    routed = route_order(intent, signal_only=True)
    for field in ("strategy_owner", "strategy_name", "strategy_version", "sleeve_id"):
        assert routed["intent"]["meta"][field] == routed["intent"][field]
    assert repos.save_order_intent(routed["intent"], trade_date="2026-08-14")
    loaded = repos.load_open_order_intents("2026-08-14")[0]
    for field in ("strategy_owner", "strategy_name", "strategy_version", "sleeve_id"):
        assert loaded[field] == routed["intent"][field]
        assert loaded["meta"][field] == routed["intent"][field]
    assert repos.save_order_ack({
        **routed["intent"], "order_no": "TQQQ-ACK-1", "qty_requested": 1,
        "qty_filled": 0, "status": "ACK",
    }, trade_date="2026-08-14")
    stored_order = repos._MEM_ORDERS[0]
    for field in ("strategy_owner", "strategy_name", "strategy_version", "sleeve_id"):
        assert stored_order["meta"][field] == routed["intent"][field]
