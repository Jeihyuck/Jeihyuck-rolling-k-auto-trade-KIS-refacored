from trader.kr.market_state_overlay import build_index_context, evaluate_kr_market_state, filter_kr_entry_intent, write_overlay_artifact


def test_only_229200_proxy_cannot_create_risk_on(tmp_path):
    returns = {("229200", 1): 0.03}
    ctx = build_index_context(lambda symbol, lookback: returns.get((symbol, lookback)), lookback=1)
    overlay = evaluate_kr_market_state(ctx)
    assert overlay["market_state"] not in {"KR_RISK_ON", "KR_STRONG_RISK_ON"}
    assert overlay["market_state"] == "KR_NORMAL"
    assert overlay["data_quality"] == "degraded"
    assert "only_kosdaq150_proxy_available" in overlay["data_quality_warnings"]
    out = tmp_path / "kr_market_state_overlay.json"
    write_overlay_artifact(out, overlay)
    text = out.read_text()
    assert '"overlay_version": "PR50"' in text
    assert '"overlay_base": "PR49"' in text
    assert '"index_resolution"' in text


def test_only_229200_with_stress_is_defensive_not_risk_on():
    ctx = build_index_context(lambda symbol, lookback: 0.03 if symbol == "229200" else None, lookback=1)
    overlay = evaluate_kr_market_state(ctx, final30_stress=True)
    assert overlay["market_state"] == "KR_DEFENSE_CAUTION"


def test_account_intraday_pnl_separate_from_unrealized():
    ctx = {"kospi_1d_return": 0.0, "kosdaq_1d_return": 0.0, "kospi200_1d_return": 0.0, "kosdaq150_1d_return": 0.0, "index_resolution": {}}
    unrealized_only = evaluate_kr_market_state(ctx, account_snapshot={"portfolio_unrealized_pnl_pct": -0.02})
    assert unrealized_only["market_state"] != "KR_DEFENSE_CRASH"
    assert not unrealized_only["account_loss_kill_switch_triggered"]
    intraday_loss = evaluate_kr_market_state(ctx, account_snapshot={"account_intraday_pnl_pct": -0.018})
    assert intraday_loss["market_state"] == "KR_DEFENSE_CRASH"
    assert intraday_loss["account_loss_kill_switch_triggered"]


def test_sell_and_exit_intents_are_never_market_state_blocked():
    overlay = {"market_state": "KR_DEFENSE_CRASH"}
    for reason in ["KR_CLOSE_LIQUIDATION_KIS_HOLDING", "defense_trim", "profit_capture", "hard_stop", "normal_exit"]:
        result = filter_kr_entry_intent({"side": "SELL", "symbol": "252670", "reason": reason}, overlay)
        assert result.get("status") != "BLOCKED"
    buy = filter_kr_entry_intent({"side": "BUY", "symbol": "252670"}, {"market_state": "KR_NORMAL"})
    assert buy["status"] == "BLOCKED"
