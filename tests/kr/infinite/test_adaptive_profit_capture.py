from datetime import date


def _state(**metadata):
    from trader.kr.infinite.models import State
    return State(cycle_id="cycle", allocated_capital_krw=4_000_000, unit_krw=100_000,
                 units_used=5, core_units_used=5, metadata=metadata)


def test_kr_normal_tp1_partial_then_tp2():
    from trader.kr.infinite.config import InfiniteConfig
    from trader.kr.infinite.models import Action, BrokerPosition
    from trader.kr.infinite.strategy import evaluate

    first = evaluate(config=InfiniteConfig(), state=_state(), position=BrokerPosition(10, 10, 100, 106),
                     trade_date=date(2026, 8, 20), market_state="KR_NORMAL")
    assert first.action == Action.SELL_PARTIAL
    assert first.reason == "TAKE_PROFIT_TP1"
    assert first.qty == 5
    second = evaluate(config=InfiniteConfig(), state=_state(profit_stage="TP1"),
                      position=BrokerPosition(5, 5, 100, 108),
                      trade_date=date(2026, 8, 20), market_state="KR_NORMAL")
    assert second.reason == "TAKE_PROFIT_TP2"
    assert second.qty == 5


def test_kr_exit_only_never_buys():
    from trader.kr.infinite.config import InfiniteConfig
    from trader.kr.infinite.models import Action, BrokerPosition
    from trader.kr.infinite.runner import evaluate_exit_only

    decision = evaluate_exit_only(config=InfiniteConfig(), state=_state(),
                                  position=BrokerPosition(0, 0, 0, 100),
                                  trade_date=date(2026, 8, 20), market_state="KR_NORMAL")
    assert decision.action == Action.WAIT
    assert decision.reason == "KR_INF_EXIT_ONLY_NO_SELL"


def test_kr_zero_orderable_qty_never_sells():
    from trader.kr.infinite.config import InfiniteConfig
    from trader.kr.infinite.models import Action, BrokerPosition
    from trader.kr.infinite.strategy import evaluate

    decision = evaluate(config=InfiniteConfig(), state=_state(),
                        position=BrokerPosition(10, 0, 100, 106),
                        trade_date=date(2026, 8, 20), market_state="KR_NORMAL")
    assert decision.action == Action.WAIT
    assert decision.reason == "KR_INF_PROFIT_NO_ORDERABLE_QTY"


def test_profitable_holding_uses_existing_10pct_fallback_when_regime_missing():
    from trader.kr.infinite.config import InfiniteConfig
    from trader.kr.infinite.models import Action, BrokerPosition
    from trader.kr.infinite.strategy import evaluate

    decision = evaluate(
        config=InfiniteConfig(),
        state=_state(),
        position=BrokerPosition(10, 10, 100, 115),
        trade_date=date(2026, 9, 7),
        market_state=None,
    )
    assert decision.action == Action.SELL_ALL
    assert decision.reason == "TAKE_PROFIT_REGIME_UNAVAILABLE_FALLBACK"
    assert decision.qty == 10
    assert decision.metadata["fallback_10pct_used"] == 1


def test_missing_regime_never_authorizes_a_buy():
    from trader.kr.infinite.config import InfiniteConfig
    from trader.kr.infinite.models import Action, BrokerPosition
    from trader.kr.infinite.strategy import evaluate

    decision = evaluate(
        config=InfiniteConfig(),
        state=_state(),
        position=BrokerPosition(10, 10, 100, 95),
        trade_date=date(2026, 9, 7),
        market_state=None,
        allow_entry=True,
    )
    assert decision.action in {Action.BLOCK, Action.WAIT}
    assert decision.action != Action.BUY
    assert decision.action != Action.RECOVERY
