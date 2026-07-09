from trader.kr.market_state_overlay import build_index_context, evaluate_kr_market_state


def test_pr50_preserves_pr49_base_metadata_and_proxy_guard():
    ctx = build_index_context(lambda symbol, lookback: 0.02 if symbol == "229200" else None)
    overlay = evaluate_kr_market_state(ctx)
    assert overlay["overlay_base"] == "PR49"
    assert overlay["overlay_version"] == "PR50"
    assert overlay["market_state"] == "KR_NORMAL"
