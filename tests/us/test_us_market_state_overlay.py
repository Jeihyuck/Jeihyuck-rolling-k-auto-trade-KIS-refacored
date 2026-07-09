from trader.us.market_state_overlay import (
    FORBIDDEN_HEDGE_SYMBOLS,
    build_defense_trim_intents,
    build_profit_capture_intents,
    evaluate_us_market_state,
    filter_entry_intents_for_market_state,
)


def rows(*closes):
    return [{"close": c} for c in closes]


def provider(spy=(100, 101, 102, 103), qqq=(100, 101, 102, 103), smh=(100, 101, 102, 103)):
    base = {"SPY": rows(*spy), "QQQ": rows(*qqq), "SMH": rows(*smh)}
    for s in ["DIA", "RSP", "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE"]:
        base[s] = rows(100, 100, 100, 100)
    return base


def eval_state(**kw):
    return evaluate_us_market_state(
        trade_date="2026-07-09",
        provider=kw.pop("provider", provider()),
        rotation_context=kw.pop("rotation_context", {"rotation_regime": "NORMAL", "benchmark_data_quality": "ok"}),
        prep_result=kw.pop("prep_result", {}),
        positions=kw.pop("positions", []),
        account_snapshot=kw.pop("account_snapshot", {}),
    )


def test_forbidden_symbols_include_inverse_and_not_defensive_etfs():
    assert {"SH", "PSQ", "SQQQ"} <= FORBIDDEN_HEDGE_SYMBOLS
    assert "XLV" not in FORBIDDEN_HEDGE_SYMBOLS
    assert "XLP" not in FORBIDDEN_HEDGE_SYMBOLS
    assert "XLU" not in FORBIDDEN_HEDGE_SYMBOLS


def test_defense_crash_thresholds_and_entry_block():
    for p in [provider(spy=(98, 100, 100, 100)), provider(qqq=(97.2, 100, 100, 100)), provider(smh=(96, 100, 100, 100))]:
        o = eval_state(provider=p)
        assert o["market_state"] == "DEFENSE_CRASH"
        assert o["force_entry_block"] is True
        assert o["exposure_multiplier"] == 0.0
    kept, blocked = filter_entry_intents_for_market_state([{"symbol": "AAPL", "side": "BUY"}], o)
    assert kept == []
    assert blocked[0]["reason"] == "DEFENSE_CRASH_ENTRY_BLOCK"


def test_risk_off_and_caution_budget_multipliers():
    assert eval_state(provider=provider(spy=(98.8, 100, 100, 100)))["market_state"] == "DEFENSE_RISK_OFF"
    caution = eval_state(provider=provider(spy=(99.3, 100, 100, 100)))
    assert caution["market_state"] == "DEFENSE_CAUTION"
    assert caution["exposure_multiplier"] == 0.5


def test_account_loss_kill_switch_overrides_risk_on():
    strong_provider = provider(spy=(103, 102, 101, 100), qqq=(106, 104, 102, 100), smh=(108, 105, 102, 100))
    strong = eval_state(provider=strong_provider, rotation_context={"rotation_regime": "AI_ON", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "AI_ON"}, account_snapshot={"account_intraday_pnl_pct": 0})
    assert strong["market_state"] == "STRONG_RISK_ON"
    assert strong["exposure_multiplier"] == 1.25
    killed = eval_state(provider=strong_provider, rotation_context={"rotation_regime": "AI_ON", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "AI_ON"}, account_snapshot={"account_intraday_pnl_pct": -0.018})
    assert killed["market_state"] == "DEFENSE_CRASH"
    assert killed["account_loss_kill_switch_triggered"] is True


def test_risk_on_multiplier_and_trailing_widths():
    risk = eval_state(provider=provider(spy=(101, 100, 100, 100), qqq=(102, 100, 100, 100), smh=(101, 100, 100, 100)), rotation_context={"rotation_regime": "BROAD_UP", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "BROAD_UP"})
    normal = eval_state(provider=provider(spy=(100, 100, 100, 100), qqq=(100, 100, 100, 100), smh=(100, 100, 100, 100)))
    off = eval_state(provider=provider(spy=(98.8, 100, 100, 100)))
    crash = eval_state(provider=provider(spy=(98, 100, 100, 100)))
    assert risk["market_state"] == "RISK_ON"
    assert risk["exposure_multiplier"] == 1.10
    assert risk["trailing_stop_pct"] > normal["trailing_stop_pct"] > off["trailing_stop_pct"] > crash["trailing_stop_pct"]


def test_risk_off_blocks_ai_but_allows_xlv():
    overlay = eval_state(provider=provider(spy=(98.8, 100, 100, 100)), prep_result={"rotation_regime": "RISK_OFF"})
    intents = [{"symbol": "NVDA", "side": "BUY", "theme_cluster": "AI_SEMI"}, {"symbol": "XLV", "side": "BUY", "theme_cluster": "HEALTHCARE"}]
    kept, blocked = filter_entry_intents_for_market_state(intents, overlay)
    assert [i["symbol"] for i in kept] == ["XLV"]
    assert blocked[0]["reason"] == "DEFENSE_RISK_OFF_AI_TECH_BLOCK"


def test_profit_capture_stages_no_full_exit_and_no_duplicate():
    overlay = {"market_state": "STRONG_RISK_ON", "profit_capture_enabled": True}
    pos = {"symbol": "AAPL", "qty": 100, "current_price": 110, "unrealized_pnl_pct": 0.03, "meta": {}}
    intents = build_profit_capture_intents([pos], overlay)
    assert intents[0]["reason"] == "TAKE_PROFIT_TP1"
    assert intents[0]["qty"] == 25
    assert intents[0]["qty"] < pos["qty"]
    pos["meta"]["tp1_done"] = True
    assert build_profit_capture_intents([pos], overlay) == []


def test_defense_trim_partial_and_no_duplicate_existing_sell():
    overlay = {"market_state": "DEFENSE_RISK_OFF"}
    positions = [
        {"symbol": "NVDA", "qty": 10, "current_price": 100, "theme_cluster": "AI_SEMI", "unrealized_pnl_pct": -0.02},
        {"symbol": "MSFT", "qty": 10, "current_price": 100, "theme_cluster": "MEGA_TECH", "unrealized_pnl_pct": -0.01},
    ]
    intents = build_defense_trim_intents(positions, overlay, existing_sell_symbols={"MSFT"})
    assert len(intents) == 1
    assert intents[0]["symbol"] == "NVDA"
    assert 0 < intents[0]["qty"] < 10
