import inspect

from trader.us.execution.order_router import route_order
from trader.us.market_state_overlay import filter_entry_intents_for_market_state


def test_order_router_blocks_forbidden_buy_before_db():
    for symbol in ("SH", "PSQ", "SQQQ", "SOXS", "UVXY", "VXX", "VIXY"):
        out = route_order({"symbol": symbol, "side": "BUY", "qty": 1, "limit_price": 10, "notional_usd": 10, "trade_date": "2026-07-09"})
        assert out["status"] == "BLOCKED"
        assert out["reason"] == "FORBIDDEN_HEDGE_OR_INVERSE_ETF"


def test_order_router_does_not_forbidden_block_defensive_etfs():
    for symbol in ("XLV", "XLP", "XLU"):
        out = route_order({"symbol": symbol, "side": "BUY", "qty": 1, "limit_price": 10, "notional_usd": 10, "trade_date": "2026-07-09"}, signal_only=True)
        assert out["reason"] != "FORBIDDEN_HEDGE_OR_INVERSE_ETF"


def test_order_router_blocks_market_state_and_prep_buy_meta_but_not_sell():
    for reason in ("DEFENSE_CRASH_ENTRY_BLOCK", "PREP_CONTRACT_TRADE_BLOCK"):
        out = route_order({"symbol": "AAPL", "side": "BUY", "qty": 1, "limit_price": 10, "notional_usd": 10, "trade_date": "2026-07-09", "meta": {"blocked_reason": reason}})
        assert out["status"] == "BLOCKED"
        assert out["reason"] == reason
        sell = route_order({"symbol": "AAPL", "side": "SELL", "qty": 1, "limit_price": 10, "notional_usd": 10, "trade_date": "2026-07-09", "meta": {"blocked_reason": reason}}, signal_only=True)
        assert sell["status"] == "SIGNAL_ONLY"


def test_entry_filter_preserves_exit_when_buy_blocked():
    intents = [
        {"symbol": "AAPL", "side": "BUY", "meta": {"blocked_reason": "PREP_CONTRACT_TRADE_BLOCK"}},
        {"symbol": "AAPL", "side": "SELL", "qty": 1},
    ]
    kept, _blocked = filter_entry_intents_for_market_state(intents, {"market_state": "NORMAL"})
    assert any(i.get("side") == "SELL" for i in kept)


def test_trade_tick_cluster_guard_reads_result_rotation_regime():
    from trader.us.runner import trade_tick_runner

    src = inspect.getsource(trade_tick_runner.run_trade_tick)
    assert '_prep_cluster_result = _prep_for_cluster.get("result")' in src
    assert '_prep_cluster_result.get("rotation_regime")' in src
    assert '_rotation_context.get("rotation_context_suspect")' in src


def test_prep_runner_persists_market_state_fields_in_result_and_status_payload():
    from trader.us.runner import prep_runner

    src = inspect.getsource(prep_runner.run_prep)
    for token in ("market_state_fields", "**market_state_fields", '"trailing_stop_pct"', '"forbidden_hedge_symbols"'):
        assert token in src
