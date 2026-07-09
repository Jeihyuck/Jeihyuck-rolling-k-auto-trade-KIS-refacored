from trader.pb1_engine import PB1Engine


def test_pb1_index_context_uses_symbol_fallback(monkeypatch):
    engine = PB1Engine.__new__(PB1Engine)
    calls=[]
    def fake(symbol, lookback):
        calls.append((symbol, lookback))
        if symbol in {"KOSPI", "KOSDAQ", "KOSPI200"}:
            return None
        return {("KOSPIETF",1):0.01,("KOSPIETF",3):0.02,("KOSDAQETF",1):0.005,("KOSDAQETF",3):0.01,("K200ETF",1):0.006,("K200ETF",3):0.012,("229200",1):0.004,("229200",3):0.008}.get((symbol, lookback))
    engine._kr_return_from_daily = fake
    monkeypatch.setenv("KR_INDEX_KOSPI_FALLBACK_PROXY", "KOSPIETF")
    monkeypatch.setenv("KR_INDEX_KOSDAQ_FALLBACK_PROXY", "KOSDAQETF")
    monkeypatch.setenv("KR_INDEX_KOSPI200_FALLBACK_PROXY", "K200ETF")
    ctx = PB1Engine._kr_index_context_for_overlay(engine)
    assert ctx["kospi_1d_return"] == 0.01
    assert ctx["kosdaq_3d_return"] == 0.01
    assert ctx["kospi200_1d_return"] == 0.006
    assert ctx["kosdaq150_3d_return"] == 0.008
    assert ("KOSPI", 1) in calls and ("KOSPIETF", 1) in calls


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
