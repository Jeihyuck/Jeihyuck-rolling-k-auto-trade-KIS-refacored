from trader.us.market_state_overlay import (
    build_profit_capture_intents,
    evaluate_us_market_state,
    filter_entry_intents_for_market_state,
)


class RuntimeProvider:
    def __init__(self, last, completed_latest=None):
        self.last = last
        self.completed_latest = completed_latest or {}

    def get_completed_daily_prices(self, symbol, exchange, **kwargs):
        # Previous completed close seed can simulate the PR63 log, but runtime
        # quotes must be authoritative intraday.
        prev = 100.0
        latest = self.completed_latest.get(symbol, 100.0)
        return [{"date": "20260713", "close": prev}, {"date": "20260714", "close": latest}]

    def get_current_price(self, symbol, exchange):
        return {"last": self.last.get(symbol, 100.0)}


def test_runtime_recovery_overrides_previous_close_semi_shock():
    overlay = evaluate_us_market_state(
        trade_date="2026-07-15",
        provider=RuntimeProvider(
            {"SPY": 100.5, "QQQ": 100.5, "SMH": 98.0},
            {"SPY": 99.2344, "QQQ": 98.1020, "SMH": 95.8414},
        ),
        rotation_context={"benchmark_data_quality": "ok", "rotation_regime": "BROAD_UP"},
        prep_result={"rotation_regime": "BROAD_UP"},
        positions=[],
        account_snapshot={},
    )

    assert overlay["previous_close_market_state"] == "PREVIOUS_CLOSE_SEMI_SHOCK"
    assert overlay["runtime_market_state"] != "DEFENSE_CRASH"
    assert overlay["allow_new_buy"] is True


def test_smh_single_runtime_crash_is_sector_risk_off_not_global_block():
    overlay = evaluate_us_market_state(
        trade_date="2026-07-15",
        provider=RuntimeProvider({"SPY": 99.7, "QQQ": 99.3, "SMH": 95.8}),
        rotation_context={"benchmark_data_quality": "ok"},
        prep_result={},
        positions=[],
        account_snapshot={},
    )

    assert overlay["market_state"] == "SECTOR_RISK_OFF_AI_SEMI"
    assert overlay["allow_new_buy"] is True
    kept, blocked = filter_entry_intents_for_market_state(
        [
            {"symbol": "NVDA", "side": "BUY", "theme_cluster": "AI_SEMI"},
            {"symbol": "JPM", "side": "BUY", "theme_cluster": "FINANCIAL"},
        ],
        overlay,
    )
    assert [i["symbol"] for i in kept] == ["JPM"]
    assert blocked[0]["reason"] == "SECTOR_RISK_OFF_AI_SEMI_ENTRY_BLOCK"


def test_profit_capture_intent_has_exchange_and_does_not_mark_pending(monkeypatch):
    calls = []

    def fake_mark(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr("trader.us.db.repos.mark_us_profit_capture_stage", fake_mark)
    intents = build_profit_capture_intents(
        [{"symbol": "AAPL", "exchange": "NASDAQ", "qty": 10, "last_price": 110, "entry_price": 100}],
        {"profit_capture_enabled": True},
        trade_date="2026-07-15",
    )

    assert intents[0]["exchange"] == "NASDAQ"
    assert intents[0]["side"] == "SELL"
    assert calls == []
