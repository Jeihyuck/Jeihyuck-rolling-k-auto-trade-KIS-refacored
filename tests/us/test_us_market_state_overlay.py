import pytest

from trader.us.db import repos
from trader.us.market_state_overlay import (
    FORBIDDEN_HEDGE_SYMBOLS,
    build_defense_trim_intents,
    build_profit_capture_intents,
    evaluate_us_market_state,
    filter_entry_intents_for_market_state,
    filter_watchlist_rows_for_market_state,
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
    for p in [provider(spy=(100, 100, 100, 98)), provider(qqq=(100, 100, 100, 97.2))]:
        o = eval_state(provider=p)
        assert o["market_state"] == "DEFENSE_CRASH_CONFIRMED"
    smh_only = eval_state(provider=provider(smh=(100, 100, 100, 96)))
    assert smh_only["market_state"] != "DEFENSE_CRASH"
    assert smh_only.get("sector_state") == "SECTOR_CRASH_AI_SEMI"
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
    assert o["market_state"] == "DEFENSE_CRASH_CONFIRMED"


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
    assert killed["market_state"] == "DEFENSE_CRASH_CONFIRMED"
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


def test_risk_off_prefilter_backfills_after_ai_leaders_and_preserves_mpc_metadata():
    rows = [
        {"symbol": "DDOG", "theme_cluster": "AI_SOFTWARE", "trend_score": 1.0, "score_final": .9, "rank_final30": 1},
        {"symbol": "SNOW", "theme_cluster": "AI_SOFTWARE", "trend_score": 1.0, "score_final": .8, "rank_final30": 2},
        {"symbol": "MPC", "theme_cluster": "ENERGY_MATERIALS", "trend_score": 1.0, "score_final": .6063, "rank_final30": 3},
        {"symbol": "KO", "theme_cluster": "CONSUMER_STAPLES", "trend_score": .5, "score_final": .55, "rank_final30": 4},
        {"symbol": "AMGN", "theme_cluster": "HEALTHCARE", "trend_score": .4, "score_final": .5, "rank_final30": 5},
    ]
    overlay = {"market_state": "DEFENSE_RISK_OFF", "market_regime": "DEFENSIVE", "allow_new_buy": True}

    eligible, preblocked = filter_watchlist_rows_for_market_state(rows, overlay)

    assert [row["symbol"] for row in eligible[:3]] == ["MPC", "KO", "AMGN"]
    assert [row["symbol"] for row in preblocked] == ["DDOG", "SNOW"]
    mpc = eligible[0]
    assert mpc["theme_cluster"] == "ENERGY_MATERIALS"
    assert mpc["trend_score"] == 1.0
    assert mpc["score_final"] >= .6
    assert mpc["rank_final30"] == 3

    kept, blocked = filter_entry_intents_for_market_state(
        [{**mpc, "side": "BUY", "meta": dict(mpc)}], overlay
    )
    assert blocked == []
    assert kept[0]["symbol"] == "MPC"


@pytest.mark.parametrize("state", ["DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED"])
def test_crash_states_prefilter_all_new_buys(state):
    eligible, blocked = filter_watchlist_rows_for_market_state(
        [{"symbol": "XLV", "theme_cluster": "HEALTHCARE", "score_final": .8, "rank_final30": 1}],
        {"market_state": state, "allow_new_buy": True},
    )
    assert eligible == []
    assert blocked[0]["reason"] == "DEFENSE_CRASH_ENTRY_BLOCK"


@pytest.mark.parametrize(
    "state,overlay_extra,cluster,expected",
    [
        ("NORMAL", {}, "HEALTHCARE", True),
        ("DEFENSE_CAUTION", {"allow_ai_tech_buy": False}, "AI_SOFTWARE", False),
        ("DEFENSE_RISK_OFF", {}, "CONSUMER_STAPLES", True),
        ("DEFENSE_CRASH_PENDING", {}, "HEALTHCARE", False),
        ("DEFENSE_CRASH_CONFIRMED", {}, "HEALTHCARE", False),
        ("DEFENSE_CRASH_REBOUND", {"allow_ai_tech_buy": False}, "AI_SOFTWARE", False),
        ("RISK_ON", {}, "AI_SOFTWARE", True),
        ("STRONG_RISK_ON", {}, "AI_SOFTWARE", True),
    ],
)
def test_market_state_candidate_policy_matrix(state, overlay_extra, cluster, expected):
    overlay = {"market_state": state, "market_regime": "RISK_ON", "allow_new_buy": True, "allow_ai_tech_buy": True, **overlay_extra}
    kept, _blocked = filter_watchlist_rows_for_market_state(
        [{"symbol": "TEST", "theme_cluster": cluster, "score_final": .8, "trend_score": 1, "rank_final30": 1}],
        overlay,
    )
    assert bool(kept) is expected


@pytest.mark.parametrize(
    "before,after,after_allowed",
    [
        ("NORMAL", "DEFENSE_CAUTION", False),
        ("DEFENSE_CAUTION", "DEFENSE_RISK_OFF", False),
        ("DEFENSE_RISK_OFF", "DEFENSE_CRASH_PENDING", False),
        ("DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED", False),
        ("DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_REBOUND", True),
        ("DEFENSE_CRASH_CONFIRMED", "DEFENSE_CRASH_REBOUND", True),
        ("DEFENSE_CRASH_REBOUND", "DEFENSE_RISK_OFF", False),
        ("DEFENSE_RISK_OFF", "NORMAL", True),
        ("NORMAL", "STRONG_RISK_ON", True),
    ],
)
def test_market_state_transition_uses_current_overlay_without_stale_block(before, after, after_allowed):
    row = {"symbol": "AI", "theme_cluster": "AI_SOFTWARE", "score_final": .8, "trend_score": 1, "rank_final30": 1}
    common = {"market_regime": "RISK_ON", "allow_new_buy": True, "allow_ai_tech_buy": True}
    def overlay(state):
        return {
            **common,
            "market_state": state,
            "allow_ai_tech_buy": state not in {"DEFENSE_CAUTION", "DEFENSE_RISK_OFF"},
        }
    filter_watchlist_rows_for_market_state([row], overlay(before))
    kept, _ = filter_watchlist_rows_for_market_state([row], overlay(after))
    assert bool(kept) is after_allowed


def test_profit_capture_price_aliases_and_pnl_rate_resolver(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    td = "2026-07-09"
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    overlay = {"market_state": "STRONG_RISK_ON", "profit_capture_enabled": True}
    cases = [
        ({"symbol": "AAPL", "qty": 100, "current_price_usd": 103, "avg_price_usd": 100}, "TAKE_PROFIT_TP1"),
        ({"symbol": "MSFT", "qty": 100, "current_price_usd": 103, "avg_price_usd": 100}, "TAKE_PROFIT_TP1"),
        ({"symbol": "GOOG", "qty": 100, "current_price_usd": 103, "avg_price_usd": 100}, "TAKE_PROFIT_TP1"),
        ({"symbol": "META", "qty": 100, "current_price_usd": 105, "avg_price_usd": 100, "meta": {"tp1_done": True}}, "TAKE_PROFIT_TP2"),
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
    pos = {"symbol": "AAPL", "qty": 100, "current_price_usd": 103, "avg_price_usd": 100}
    first = build_profit_capture_intents([pos], overlay, trade_date="2026-07-09")
    second = build_profit_capture_intents([pos], overlay, trade_date="2026-07-09")
    assert len(first) == 1
    assert second == []
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos.mark_us_profit_capture_stage("2026-07-09", "AAPL", "tp1", status="ACK")
    assert build_profit_capture_intents([{**pos, "current_price_usd": 105}], overlay, trade_date="2026-07-09") == []
    repos.mark_us_profit_capture_stage("2026-07-09", "AAPL", "tp1", status="FILLED")
    tp2 = build_profit_capture_intents([{**pos, "current_price_usd": 105}], overlay, trade_date="2026-07-09")
    assert tp2 and tp2[0]["reason"] == "TAKE_PROFIT_TP2"
    repos.mark_us_profit_capture_stage("2026-07-09", "AAPL", "tp2", status="FILLED")
    repos.mark_us_profit_capture_stage("2026-07-09", "AAPL", "tp3", status="FILLED")
    assert build_profit_capture_intents([{**pos, "current_price_usd": 109}], overlay, trade_date="2026-07-09") == []



def test_profit_capture_stage_order_gap_up_starts_with_tp1(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    overlay = {"market_state": "STRONG_RISK_ON", "profit_capture_enabled": True}
    pos = {"symbol": "GAP", "qty": 100, "current_price_usd": 109, "avg_price_usd": 100}
    first = build_profit_capture_intents([pos], overlay, trade_date="2026-07-10")
    assert len(first) == 1
    assert first[0]["reason"] == "TAKE_PROFIT_TP1"
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos.mark_us_profit_capture_stage("2026-07-10", "GAP", "tp1", status="FILLED")
    second = build_profit_capture_intents([pos], overlay, trade_date="2026-07-10")
    assert len(second) == 1
    assert second[0]["reason"] == "TAKE_PROFIT_TP2"
    repos.mark_us_profit_capture_stage("2026-07-10", "GAP", "tp2", status="FILLED")
    third = build_profit_capture_intents([pos], overlay, trade_date="2026-07-10")
    assert len(third) == 1
    assert third[0]["reason"] == "TAKE_PROFIT_TP3"
    repos.mark_us_profit_capture_stage("2026-07-10", "GAP", "tp3", status="FILLED")
    assert build_profit_capture_intents([pos], overlay, trade_date="2026-07-10") == []


def test_profit_capture_rejected_stage_can_retry(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    overlay = {"market_state": "STRONG_RISK_ON", "profit_capture_enabled": True}
    pos = {"symbol": "RETRY", "qty": 100, "current_price_usd": 103, "avg_price_usd": 100}
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
    intents = build_defense_trim_intents(positions, overlay, existing_sell_symbols={"MSFT"}, trade_date="2026-07-16")
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


def test_daily_crash_unlocks_on_valid_intraday_rebound(monkeypatch):
    p = provider(qqq=(100, 100, 100, 98), smh=(100, 100, 100, 95))
    p["intraday_quotes"] = {
        "SPY": {"last": 100.4, "previous_close": 100, "open": 100.1, "vwap": 100.2},
        "QQQ": {"last": 99.0, "previous_close": 98, "open": 98.2, "vwap": 98.5},
        "SMH": {"last": 96.6, "previous_close": 95, "open": 95.2, "vwap": 96.0},
    }
    o = eval_state(provider=p)
    assert o["market_state"] == "DEFENSE_CRASH_REBOUND"
    assert o["exposure_multiplier"] == pytest.approx(.25)
    assert o["allow_new_buy"] is True
    assert o["force_entry_block"] is False
    assert o["allow_add_to_existing"] is False
    assert o["allow_ai_tech_buy"] is True
    assert o["effective_max_new_positions"] == 3
    assert o["trailing_stop_mode"] == "crash_rebound_tight"
    assert o["take_profit_mode"] == "fast_profit_capture"


def test_daily_crash_rebound_missing_or_hard_down_fails_closed():
    missing = eval_state(provider=provider(qqq=(100, 100, 100, 98), smh=(100, 100, 100, 95)))
    assert missing["market_state"] == "DEFENSE_CRASH_CONFIRMED"
    assert missing["allow_new_buy"] is False
    assert missing["force_entry_block"] is True

    p = provider(qqq=(100, 100, 100, 98), smh=(100, 100, 100, 95))
    p["intraday_quotes"] = {
        "SPY": {"last": 99.4, "previous_close": 100, "open": 100},
        "QQQ": {"last": 99, "previous_close": 98, "open": 98},
        "SMH": {"last": 97, "previous_close": 95, "open": 95},
    }
    blocked = eval_state(provider=p)
    assert blocked["market_state"] == "DEFENSE_CRASH_CONFIRMED"
    assert blocked["intraday_rebound"]["hard_down"] is True
