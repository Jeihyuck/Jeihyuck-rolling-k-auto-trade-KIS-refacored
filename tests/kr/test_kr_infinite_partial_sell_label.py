from datetime import date

from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.models import Action, BrokerPosition, State, Status
from trader.kr.infinite.strategy import evaluate


def test_tp1_partial_quantity_is_labeled_sell_partial():
    decision = evaluate(
        config=InfiniteConfig(enabled=True),
        state=State(cycle_id="cycle", cycle_start_date=date(2026, 1, 1), allocated_capital_krw=4_000_000,
                    unit_krw=100_000, units_used=5, core_units_used=5, status=Status.ACTIVE),
        position=BrokerPosition(qty=6, orderable_qty=6, average_price=100, current_price=110),
        trade_date=date(2026, 8, 24),
        market_state="KR_NORMAL",
    )
    assert decision.reason == "TAKE_PROFIT_TP1"
    assert decision.action == Action.SELL_PARTIAL
    assert 0 < decision.qty < 6
    assert 6 - decision.qty == decision.metadata["remaining_qty"]
