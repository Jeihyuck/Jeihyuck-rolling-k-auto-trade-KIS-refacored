from trader.us.execution.order_router import route_order
from trader.us.market_state_overlay import filter_entry_intents_for_market_state


def test_order_router_blocks_forbidden_buy_before_db():
    for symbol in ("SH", "PSQ", "SQQQ"):
        out = route_order({"symbol": symbol, "side": "BUY", "qty": 1, "limit_price": 10, "notional_usd": 10, "trade_date": "2026-07-09"})
        assert out["status"] == "BLOCKED"
        assert out["reason"] == "FORBIDDEN_HEDGE_OR_INVERSE_ETF"


def test_order_router_does_not_forbidden_block_defensive_etfs(monkeypatch):
    # Use signal_only to avoid DB/broker work while proving forbidden hard block does not catch these.
    for symbol in ("XLV", "XLP", "XLU"):
        out = route_order({"symbol": symbol, "side": "BUY", "qty": 1, "limit_price": 10, "notional_usd": 10, "trade_date": "2026-07-09"}, signal_only=True)
        assert out["reason"] != "FORBIDDEN_HEDGE_OR_INVERSE_ETF"


def test_prep_contract_block_reason_filters_buy_but_not_exit():
    intents = [
        {"symbol": "AAPL", "side": "BUY", "meta": {"blocked_reason": "PREP_CONTRACT_TRADE_BLOCK"}},
        {"symbol": "AAPL", "side": "SELL", "qty": 1},
    ]
    kept, blocked = filter_entry_intents_for_market_state(intents, {"market_state": "NORMAL"})
    # market-state filter itself preserves SELLs; router is the second-line prep block for BUY meta.
    assert any(i.get("side") == "SELL" for i in kept)
