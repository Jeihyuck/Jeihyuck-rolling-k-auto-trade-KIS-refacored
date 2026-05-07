from trader.us.runner.prep_runner import _resolve_final_prep_status
from trader.us.strategy.base import BaseUSStrategy, make_order_intent


class _DummyProvider:
    def get_orderable_cash(self):
        return 10000.0

    def get_daily_prices(self, symbol, exchange):
        return [{"close": 100.0}]

    def get_current_price(self, symbol, exchange):
        return {"price": 101.0}


class _DummyStrategy(BaseUSStrategy):
    name = "dummy_score_contract"

    def score(self, symbol, daily_prices, current_price):
        return 0.77

    def generate_intent(self, symbol, exchange, score, daily_prices, current_price, available_cash_usd):
        return make_order_intent(
            run_id=self.run_id,
            trade_date=self.trade_date,
            symbol=symbol,
            exchange=exchange,
            side="BUY",
            qty=1,
            limit_price=100.0,
            strategy=self.name,
            reason_json={},
        )


def test_base_strategy_run_injects_canonical_score_fields():
    strategy = _DummyStrategy(run_id="run-1", trade_date="2026-05-07")

    intent = strategy.run_on_universe(["QQQ"], _DummyProvider())[0]

    assert intent["score"] == 0.77
    assert intent["score_final"] == 0.77
    assert intent["final_score"] == 0.77
    assert intent["reason_json"]["score"] == 0.77
    assert intent["reason_json"]["score_final"] == 0.77
    assert intent["reason_json"]["final_score"] == 0.77
    assert intent["scores"]["final"] == 0.77
    assert intent["scores"]["score"] == 0.77
    assert intent["scores"]["score_final"] == 0.77


def test_final_status_gate_rejects_saved_watchlist_when_score_contract_fails():
    final_status, has_fatal_error = _resolve_final_prep_status(
        provisional_status="OK",
        saved_count=19,
        score_nonzero_count=0,
        score_contract_ok=False,
        has_fatal_error=False,
    )

    assert final_status == "ERROR"
    assert has_fatal_error is True