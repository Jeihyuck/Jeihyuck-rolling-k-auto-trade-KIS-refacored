from trader.pb1_engine import PB1Engine


def test_pb1_regime_path_has_no_literal_index_fallback():
    assert not hasattr(PB1Engine, "_kr_index_context_for_overlay")
    assert not hasattr(PB1Engine, "_kr_return_with_symbol_fallbacks")


def test_pb1_get_return_public_wrapper_calls_private():
    engine = PB1Engine.__new__(PB1Engine)
    engine._kr_return_from_daily = lambda symbol, lookback: 0.123 if (symbol, lookback)==("A",3) else None
    assert PB1Engine.get_return(engine, "A", lookback=3) == 0.123
    assert PB1Engine.get_index_return(engine, "A", lookback=3) == 0.123


def test_pb1_account_snapshot_does_not_use_unrealized_as_intraday():
    engine = PB1Engine.__new__(PB1Engine)
    engine._holdings_summary = {}
    engine._to_float = lambda value: None if value is None else float(value)
    pos = [{"code":"005930","qty":10,"market_value_krw":980_000,"total_cost":1_000_000,"sector_cluster":"SEMICONDUCTOR"}]
    snap = PB1Engine._kr_account_snapshot_for_overlay(engine, pos, available_cash_krw=500_000)
    assert snap["account_intraday_pnl_pct"] is None
    assert snap["portfolio_unrealized_pnl_pct"] == -0.02
