from trader.kr.market_state_overlay import generate_kr_defense_trim_intents

def test_trim_partial_no_duplicate():
    out=generate_kr_defense_trim_intents([{"code":"A","qty":10,"sector_cluster":"BIO_HEALTHCARE","unrealized_pnl_pct":-0.01},{"code":"B","qty":10,"sector_cluster":"AUTO"}], {"market_state":"KR_DEFENSE_RISK_OFF"}, existing_sell_symbols={"B"})
    assert out and out[0]["reason"] == "KR_DEFENSE_RISK_OFF_TRIM" and out[0]["qty"] < 10
