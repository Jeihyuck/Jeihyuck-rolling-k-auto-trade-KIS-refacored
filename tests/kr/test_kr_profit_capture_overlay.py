from trader.kr.market_state_overlay import evaluate_kr_market_state, filter_kr_entry_intent, generate_kr_profit_capture_intents

class P:
    sector_strength = {"FINANCIAL": {"vs_kospi_3d": 0.02, "source_quality":"high", "source":"basket"}}

def ctx(**kw):
    base = dict(kospi_1d_return=0.0,kosdaq_1d_return=0.0,kospi200_1d_return=0.0,kosdaq150_1d_return=0.0,kospi_3d_return=0.01,kosdaq_3d_return=0.01,kospi200_3d_return=0.01,kosdaq150_3d_return=0.01)
    base.update(kw); return base

def test_kospi_crash_overrides_good_229200():
    o = evaluate_kr_market_state(trade_date="2026-07-09", provider=P(), index_context=ctx(kospi_1d_return=-0.021, kosdaq150_1d_return=0.01), sector_context={"rotation_regime":"KR_MIXED","sector_leaders":["FINANCIAL"],"sector_laggards":[],"sector_strength":{},"sector_proxy_quality":{"FINANCIAL":"high"}})
    assert o["market_state"] == "KR_DEFENSE_CRASH"
    assert o["exposure_multiplier"] == 0
    assert o["force_entry_block"] is True

def test_kosdaq_weak_but_kospi_financial_strong_not_full_block():
    o = evaluate_kr_market_state(trade_date="2026-07-09", provider=P(), index_context=ctx(kospi_1d_return=0.01,kospi200_1d_return=0.01,kosdaq150_1d_return=-0.026,kosdaq_1d_return=-0.011), sector_context={"rotation_regime":"KR_KOSPI_VALUE_LEAD","sector_leaders":["FINANCIAL"],"sector_laggards":["BIO_HEALTHCARE"],"sector_strength":{},"sector_proxy_quality":{"FINANCIAL":"high"}})
    assert o["market_state"] == "KR_DEFENSE_CAUTION"
    assert o["allow_new_buy"] is True
    assert o["allow_high_beta_buy"] is False
    assert o["allow_financial_buy"] is True

def test_missing_market_data_does_not_crash_or_strong_risk_on():
    o = evaluate_kr_market_state(trade_date="2026-07-09", provider=P(), index_context={"kosdaq150_1d_return":0.02})
    assert o["market_state"] != "KR_DEFENSE_CRASH"
    assert o["market_state"] != "KR_STRONG_RISK_ON"
    assert o["data_quality"] == "degraded"

def test_account_loss_kill_switch_crash():
    o = evaluate_kr_market_state(trade_date="2026-07-09", provider=P(), index_context=ctx(), account_snapshot={"account_intraday_pnl_pct":-0.019})
    assert o["market_state"] == "KR_DEFENSE_CRASH"
    assert o["account_loss_kill_switch_triggered"] is True

def test_filter_crash_blocks_buy_but_not_sell():
    overlay={"market_state":"KR_DEFENSE_CRASH","force_entry_block":True}
    assert filter_kr_entry_intent({"side":"BUY","code":"005930","name":"삼성전자"}, overlay)["status"] == "BLOCKED"
    assert "status" not in filter_kr_entry_intent({"side":"SELL","code":"005930","name":"삼성전자"}, overlay)

def test_profit_capture_tp1_partial_runner():
    intents = generate_kr_profit_capture_intents([{"code":"005930","qty":100,"unrealized_pnl_pct":0.031,"meta":{}}], {"market_state":"KR_STRONG_RISK_ON"})
    assert intents[0]["reason"] == "KR_TAKE_PROFIT_TP1"
    assert intents[0]["qty"] < 100
