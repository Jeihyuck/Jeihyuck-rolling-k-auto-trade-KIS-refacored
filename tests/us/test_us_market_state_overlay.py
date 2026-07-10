import pytest

from trader.us.db import repos
from trader.us.market_state_overlay import (
    FORBIDDEN_HEDGE_SYMBOLS,
    build_defense_trim_intents,
    build_profit_capture_intents,
    evaluate_us_market_state,
    filter_entry_intents_for_market_state,
)


def rows_oldest_first(*closes, start=1):
    return [{"date": f"202607{start+i:02d}", "close": c} for i, c in enumerate(closes)]


def provider(spy=(100, 100, 100, 100), qqq=(100, 100, 100, 100), smh=(100, 100, 100, 100), dated=True):
    def make(vals):
        return rows_oldest_first(*vals) if dated else [{"close": c} for c in vals]
    base = {"SPY": make(spy), "QQQ": make(qqq), "SMH": make(smh)}
    for s in ["DIA", "RSP", "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE"]:
        base[s] = make((100, 100, 100, 100))
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
    assert {"SH", "PSQ", "SQQQ", "SOXS", "UVXY", "VXX", "VIXY"} <= FORBIDDEN_HEDGE_SYMBOLS
    assert "XLV" not in FORBIDDEN_HEDGE_SYMBOLS
    assert "XLP" not in FORBIDDEN_HEDGE_SYMBOLS
    assert "XLU" not in FORBIDDEN_HEDGE_SYMBOLS


def test_oldest_first_defense_crash_thresholds_and_entry_block():
    for p in [provider(spy=(100, 100, 100, 98)), provider(qqq=(100, 100, 100, 97.2)), provider(smh=(100, 100, 100, 96))]:
        o = eval_state(provider=p)
        assert o["market_state"] == "DEFENSE_CRASH"
        assert o["force_entry_block"] is True
        assert o["exposure_multiplier"] == 0.0
    kept, blocked = filter_entry_intents_for_market_state([{"symbol": "AAPL", "side": "BUY"}], o)
    assert kept == []
    assert blocked[0]["reason"] == "DEFENSE_CRASH_ENTRY_BLOCK"


def test_mixed_date_rows_are_sorted_before_return_calculation():
    p = provider()
    p["SPY"] = [
        {"date": "20260704", "close": 98},
        {"date": "20260701", "close": 100},
        {"date": "20260703", "close": 100},
        {"date": "20260702", "close": 100},
    ]
    o = eval_state(provider=p)
    assert o["spy_1d_return"] == pytest.approx(-0.02)
    assert o["market_state"] == "DEFENSE_CRASH"


def test_risk_off_and_caution_budget_multipliers():
    assert eval_state(provider=provider(spy=(100, 100, 100, 98.8)))["market_state"] == "DEFENSE_RISK_OFF"
    caution = eval_state(provider=provider(spy=(100, 100, 100, 99.3)))
    assert caution["market_state"] == "DEFENSE_CAUTION"
    assert caution["exposure_multiplier"] == 0.5


def test_oldest_first_risk_on_and_strong_risk_on_multipliers():
    strong_provider = provider(spy=(100, 101, 102, 103), qqq=(100, 102, 104, 106), smh=(100, 102, 105, 108))
    strong = eval_state(provider=strong_provider, rotation_context={"rotation_regime": "AI_ON", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "AI_ON"}, account_snapshot={"account_intraday_pnl_pct": 0})
    assert strong["market_state"] == "STRONG_RISK_ON"
    assert strong["exposure_multiplier"] == 1.25
    risk = eval_state(provider=provider(spy=(100, 100, 100, 102), qqq=(100, 100, 100, 103), smh=(100, 100, 100, 102)), rotation_context={"rotation_regime": "BROAD_UP", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "BROAD_UP"})
    assert risk["market_state"] == "RISK_ON"
    assert risk["exposure_multiplier"] == 1.10


def test_account_loss_kill_switch_overrides_risk_on():
    strong_provider = provider(spy=(100, 101, 102, 103), qqq=(100, 102, 104, 106), smh=(100, 102, 105, 108))
    killed = eval_state(provider=strong_provider, rotation_context={"rotation_regime": "AI_ON", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "AI_ON"}, account_snapshot={"account_intraday_pnl_pct": -0.018})
    assert killed["market_state"] == "DEFENSE_CRASH"
    assert killed["account_loss_kill_switch_triggered"] is True


def test_trailing_widths_ordered_by_state():
    risk = eval_state(provider=provider(spy=(100, 100, 100, 102), qqq=(100, 100, 100, 103), smh=(100, 100, 100, 102)), rotation_context={"rotation_regime": "BROAD_UP", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "BROAD_UP"})
    normal = eval_state(provider=provider())
    off = eval_state(provider=provider(spy=(100, 100, 100, 98.8)))
    crash = eval_state(provider=provider(spy=(100, 100, 100, 98)))
    assert risk["trailing_stop_pct"] > normal["trailing_stop_pct"] > off["trailing_stop_pct"] > crash["trailing_stop_pct"]


def test_risk_off_blocks_ai_but_allows_xlv():
    overlay = eval_state(provider=provider(spy=(100, 100, 100, 98.8)), prep_result={"rotation_regime": "RISK_OFF"})
    intents = [{"symbol": "NVDA", "side": "BUY", "theme_cluster": "AI_SEMI"}, {"symbol": "XLV", "side": "BUY", "theme_cluster": "HEALTHCARE"}]
    kept, blocked = filter_entry_intents_for_market_state(intents, overlay)
    assert [i["symbol"] for i in kept] == ["XLV"]
    assert blocked[0]["reason"] == "DEFENSE_RISK_OFF_AI_TECH_BLOCK"


def test_profit_capture_price_aliases_and_pnl_rate_resolver(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    td = "2026-07-09"
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    overlay = {"market_state": "STRONG_RISK_ON", "profit_capture_enabled": True}
    cases = [
        ({"symbol": "AAPL", "qty": 100, "current_price_usd": 110, "unrealized_pnl_pct": 0.03}, "TAKE_PROFIT_TP1"),
        ({"symbol": "MSFT", "qty": 100, "current_price_usd": 110, "pnl_rate": 3.0}, "TAKE_PROFIT_TP1"),
        ({"symbol": "GOOG", "qty": 100, "current_price_usd": 110, "pnl_rate": 0.03}, "TAKE_PROFIT_TP1"),
        ({"symbol": "META", "qty": 100, "current_price_usd": 110, "evlu_pfls_rt": 5.0, "meta": {"tp1_done": True}}, "TAKE_PROFIT_TP2"),
        ({"symbol": "AMZN", "qty": 100, "current_price_usd": 103, "entry_price": 100}, "TAKE_PROFIT_TP1"),
    ]
    for idx, (pos, reason) in enumerate(cases):
        intents = build_profit_capture_intents([pos], overlay, trade_date=f"{td}-{idx}")
        assert intents and intents[0]["reason"] == reason
    assert build_profit_capture_intents([{"symbol": "LOSS", "qty": 100, "current_price_usd": 95, "entry_price": 100}], overlay, trade_date=td) == []


def test_profit_capture_persistent_duplicate_prevention(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    overlay = {"market_state": "STRONG_RISK_ON", "profit_capture_enabled": True}
    pos = {"symbol": "AAPL", "qty": 100, "current_price_usd": 110, "pnl_rate": 3.0}
    first = build_profit_capture_intents([pos], overlay, trade_date="2026-07-09")
    second = build_profit_capture_intents([pos], overlay, trade_date="2026-07-09")
    assert len(first) == 1
    assert second == []
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos.mark_us_profit_capture_stage("2026-07-09", "AAPL", "tp1", status="ACK")
    tp2 = build_profit_capture_intents([{**pos, "pnl_rate": 5.0}], overlay, trade_date="2026-07-09")
    assert tp2 and tp2[0]["reason"] == "TAKE_PROFIT_TP2"
    repos.mark_us_profit_capture_stage("2026-07-09", "AAPL", "tp2", status="ACK")
    repos.mark_us_profit_capture_stage("2026-07-09", "AAPL", "tp3", status="ACK")
    assert build_profit_capture_intents([{**pos, "pnl_rate": 9.0}], overlay, trade_date="2026-07-09") == []



def test_profit_capture_stage_order_gap_up_starts_with_tp1(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    overlay = {"market_state": "STRONG_RISK_ON", "profit_capture_enabled": True}
    pos = {"symbol": "GAP", "qty": 100, "current_price_usd": 109, "pnl_rate": 9.0}
    first = build_profit_capture_intents([pos], overlay, trade_date="2026-07-10")
    assert len(first) == 1
    assert first[0]["reason"] == "TAKE_PROFIT_TP1"
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos.mark_us_profit_capture_stage("2026-07-10", "GAP", "tp1", status="ACK")
    second = build_profit_capture_intents([pos], overlay, trade_date="2026-07-10")
    assert len(second) == 1
    assert second[0]["reason"] == "TAKE_PROFIT_TP2"
    repos.mark_us_profit_capture_stage("2026-07-10", "GAP", "tp2", status="ACK")
    third = build_profit_capture_intents([pos], overlay, trade_date="2026-07-10")
    assert len(third) == 1
    assert third[0]["reason"] == "TAKE_PROFIT_TP3"
    repos.mark_us_profit_capture_stage("2026-07-10", "GAP", "tp3", status="ACK")
    assert build_profit_capture_intents([pos], overlay, trade_date="2026-07-10") == []


def test_profit_capture_rejected_stage_can_retry(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    overlay = {"market_state": "STRONG_RISK_ON", "profit_capture_enabled": True}
    pos = {"symbol": "RETRY", "qty": 100, "current_price_usd": 103, "pnl_rate": 3.0}
    repos.mark_us_profit_capture_stage("2026-07-11", "RETRY", "tp1", status="PENDING")
    assert build_profit_capture_intents([pos], overlay, trade_date="2026-07-11") == []
    repos.mark_us_profit_capture_stage("2026-07-11", "RETRY", "tp1", status="REJECTED")
    retry = build_profit_capture_intents([pos], overlay, trade_date="2026-07-11")
    assert len(retry) == 1
    assert retry[0]["reason"] == "TAKE_PROFIT_TP1"
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos.mark_us_profit_capture_stage("2026-07-11", "RETRY", "tp1", status="ACK")
    assert build_profit_capture_intents([pos], overlay, trade_date="2026-07-11") == []
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos.mark_us_profit_capture_stage("2026-07-11", "RETRY", "tp1", status="DONE")
    assert build_profit_capture_intents([pos], overlay, trade_date="2026-07-11") == []

def test_defense_trim_current_px_alias_partial_and_no_duplicate_existing_sell():
    overlay = {"market_state": "DEFENSE_RISK_OFF"}
    positions = [
        {"symbol": "NVDA", "qty": 10, "current_px": 100, "theme_cluster": "AI_SEMI", "pnl_rate": -2.0},
        {"symbol": "MSFT", "qty": 10, "current_px": 100, "theme_cluster": "MEGA_TECH", "pnl_rate": -1.0},
    ]
    intents = build_defense_trim_intents(positions, overlay, existing_sell_symbols={"MSFT"})
    assert len(intents) == 1
    assert intents[0]["symbol"] == "NVDA"
    assert intents[0]["notional_usd"] > 0
    assert 0 < intents[0]["qty"] < 10


def test_gross_cap_blocks_new_buy_even_in_risk_on_and_weak_add_blocked():
    overlay = eval_state(provider=provider(spy=(100, 100, 100, 102), qqq=(100, 100, 100, 103), smh=(100, 100, 100, 102)), rotation_context={"rotation_regime": "BROAD_UP", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "BROAD_UP"}, account_snapshot={"gross_exposure_pct": 0.96})
    assert overlay["market_state"] == "RISK_ON"
    assert overlay["allow_new_buy"] is False
    strong = eval_state(provider=provider(spy=(100, 101, 102, 103), qqq=(100, 102, 104, 106), smh=(100, 102, 105, 108)), rotation_context={"rotation_regime": "AI_ON", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "AI_ON"})
    kept, blocked = filter_entry_intents_for_market_state([{"symbol": "NVDA", "side": "BUY", "rank_final30": 1, "score_final": 0.9, "trend_score": 1.0}], strong, positions=[{"symbol": "NVDA", "qty": 10, "current_price_usd": 95, "entry_price": 100}])
    assert kept == []
    assert blocked[0]["reason"] == "MARKET_STATE_ENTRY_BLOCK"


def provider20(vals: dict[str, tuple[float, float, float, float]]):
    # Four anchors are expanded into 21 oldest-first rows preserving 20d/3d/1d returns.
    base = {}
    for sym in ["SPY", "QQQ", "SMH", "RSP", "IWM", "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE", "DIA"]:
        p20, p3, p1, last = vals.get(sym, (100, 100, 100, 100))
        closes = [p20] + [p20] * 16 + [p3, 100, p1, last]
        base[sym] = rows_oldest_first(*closes)
    return base


def test_market_regime_risk_on():
    p = provider20({"SPY": (100, 101, 102, 104), "QQQ": (100, 104, 104, 108), "SMH": (100, 105, 105, 110), "RSP": (100, 102, 103, 106), "IWM": (100, 102, 103, 106), "XLK": (100, 104, 104, 108), "XLI": (100, 103, 103, 107), "XLF": (100, 103, 103, 107), "XLE": (100, 103, 103, 107)})
    o = eval_state(provider=p, rotation_context={"rotation_regime": "AI_ON", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "AI_ON"})
    assert o["market_regime"] == "RISK_ON"
    assert o["capital_scale"] >= 0.9
    assert o["allow_ai_tech_buy"] is True


def test_market_regime_growth_leadership():
    p = provider20({"SPY": (100, 101, 102, 104), "QQQ": (100, 104, 104, 108), "SMH": (100, 105, 105, 110), "RSP": (100, 99, 100, 102), "IWM": (100, 99, 100, 102), "XLK": (100, 104, 104, 108)})
    o = eval_state(provider=p, rotation_context={"rotation_regime": "AI_ON", "benchmark_data_quality": "ok"}, prep_result={"rotation_regime": "AI_ON"})
    assert o["market_regime"] == "GROWTH_LEADERSHIP"
    assert o["capital_scale"] == pytest.approx(0.70)
    assert o["max_ai_tech_ratio"] <= 0.45


def test_market_regime_defensive():
    p = provider20({"SPY": (100, 99, 99, 98.5), "QQQ": (100, 98, 98, 97.5), "SMH": (100, 98, 98, 97.5), "XLV": (100, 102, 102, 103), "XLP": (100, 102, 102, 103), "XLU": (100, 102, 102, 103)})
    o = eval_state(provider=p)
    assert o["market_regime"] == "DEFENSIVE"
    assert o["allow_ai_tech_buy"] is False
    assert o["max_ai_tech_ratio"] <= 0.20


def test_market_regime_risk_off_contract_block_fields():
    o = eval_state(provider=provider20({"SPY": (100, 98, 99, 97), "QQQ": (100, 96, 98, 95), "SMH": (100, 95, 97, 94)}), rotation_context={"rotation_regime": "NORMAL", "rotation_context_suspect": True, "rotation_suspect_policy": "block"})
    assert o["market_regime"] == "RISK_OFF"
    assert o["force_entry_block"] is True
    assert o["allow_new_buy"] is False
