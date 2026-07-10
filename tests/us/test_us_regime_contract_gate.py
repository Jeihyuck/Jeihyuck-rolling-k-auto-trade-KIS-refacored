import os

from trader.us.runner.trade_tick_runner import validate_us_regime_contract_for_entry
from trader.us.prep_contract import build_us_prep_contract
from trader.us.watchlist_builder import enforce_regime_sector_caps


def test_real_trade_missing_contract_version_blocks_entry(monkeypatch):
    monkeypatch.delenv("US_ALLOW_LEGACY_PREP_FOR_TEST", raising=False)
    gate = validate_us_regime_contract_for_entry({"status": "OK"}, real_order_mode=True, kis_order_allowed=True)
    assert gate["ok"] is False
    assert gate["reason"] == "prep_contract_version_mismatch"


def test_schedule_missing_contract_version_blocks_entry(monkeypatch):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
    gate = validate_us_regime_contract_for_entry({"status": "OK"}, real_order_mode=False, kis_order_allowed=False)
    assert gate["ok"] is False
    assert gate["reason"] == "prep_contract_version_mismatch"


def test_legacy_contract_allowed_only_in_test_env(monkeypatch):
    monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)
    monkeypatch.setenv("US_ALLOW_LEGACY_PREP_FOR_TEST", "1")
    assert validate_us_regime_contract_for_entry({"status": "OK"}, real_order_mode=False, kis_order_allowed=False)["ok"] is True
    blocked = validate_us_regime_contract_for_entry({"status": "OK"}, real_order_mode=True, kis_order_allowed=True)
    assert blocked["ok"] is False
    assert blocked["reason"] == "prep_contract_version_mismatch"


def test_sector_cap_violation_blocks_trade():
    rows = [{"symbol": f"AI{i}", "theme_cluster": "AI_SEMI", "score_final": 1.0 - i * 0.001} for i in range(21)] + [{"symbol": f"H{i}", "theme_cluster": "HEALTHCARE", "score_final": 0.7} for i in range(9)]
    final, meta = enforce_regime_sector_caps(rows, rows, {"market_regime": "NEUTRAL", "max_ai_tech_ratio": 0.35, "max_single_cluster_ratio": 1.0}, 30)
    assert "AI_TECH_COMBINED" in meta["cap_violations"]
    contract = build_us_prep_contract(
        trade_date="2026-07-09",
        env="practice",
        status="OK",
        dynamic_universe_result={"filtered_count": 50},
        candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 30, "final30_count": 30, "final30_scored_count": 30, "cluster_contract_ok": False, "final30_cluster_cap_clean": False, "cap_violations": meta["cap_violations"], "blocked_by_cluster_cap": meta["blocked_by_cluster_cap"], "market_state_overlay": {"market_regime": "NEUTRAL", "allow_new_buy": True}},
        validation={"ok": True, "score_nonzero_count": 30, "warnings": [], "errors": []},
        paths={},
    )
    assert contract["cluster_contract_ok"] is False
    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] in {"sector_cap_violation_block", "cluster_cap_contract_failed", "final30_incomplete"}


def test_risk_off_is_not_prep_error():
    contract = build_us_prep_contract(
        trade_date="2026-07-09",
        env="practice",
        status="OK",
        dynamic_universe_result={"filtered_count": 50},
        candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 30, "final30_count": 30, "final30_scored_count": 30, "cluster_contract_ok": True, "final30_cluster_cap_clean": True, "cap_violations": [], "market_state_overlay": {"market_state": "NORMAL", "market_regime": "RISK_OFF", "capital_scale": 0.0, "max_ai_tech_ratio": 0.1, "max_single_cluster_ratio": 0.1, "allow_new_buy": False, "allow_ai_tech_buy": False, "allow_defensive_buy": False, "force_entry_block": True}},
        validation={"ok": True, "score_nonzero_count": 30, "warnings": [], "errors": []},
        paths={},
    )
    assert contract["status"] in {"RISK_OFF_ENTRY_BLOCKED", "DEFENSE_CRASH_ENTRY_BLOCKED"}
    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] == "risk_off_entry_block"
    assert contract["contract_version"] == "us_sector_rotation_v3"
    assert contract["market_regime_version"] == "us_leading_regime_v1"


def test_exchange_alias_nasd_does_not_break_rotation_context():
    from trader.us.symbols import get_quote_exchange_code
    from trader.us.watchlist_builder import _build_rotation_context

    assert get_quote_exchange_code("NASD") == "NAS"
    assert get_quote_exchange_code("NAS") == "NAS"
    assert get_quote_exchange_code("NASDAQ") == "NAS"

    calls = []

    class Provider:
        def get_daily_prices(self, symbol, exchange="NYSE", as_of_date=None):
            calls.append((symbol, exchange))
            if symbol in {"QQQ", "SMH", "NVDA"}:
                assert exchange == "NASDAQ"
            return [{"close": 100 + i} for i in range(6)]

    ctx = _build_rotation_context(Provider(), [], "2026-07-10")
    assert ctx["benchmark_symbol_quality"]["QQQ"] == "ok"
    assert ctx["benchmark_symbol_quality"]["SMH"] == "ok"
    assert "QQQ" not in ctx["missing_symbols"]
    assert "SMH" not in ctx["missing_symbols"]
    assert ("NVDA", "NASDAQ") in calls


def test_enforce_regime_sector_caps_uses_finaln_denominator():
    from trader.us.watchlist_builder import enforce_regime_sector_caps

    rows = [{"symbol": f"H{i}", "theme_cluster": "HEALTHCARE", "score_final": 1.0 - i * 0.01} for i in range(3)]
    final, meta = enforce_regime_sector_caps(
        rows,
        rows,
        {"market_regime": "NEUTRAL", "max_ai_tech_ratio": 1.0, "max_single_cluster_ratio": 0.10},
        30,
    )
    assert len(final) == 3
    assert meta["cap_violations"] == []


def test_risk_off_preserves_final30_artifact():
    from trader.us.prep_contract import build_us_prep_contract
    from trader.us.watchlist_builder import enforce_regime_sector_caps

    rows = [{"symbol": f"H{i}", "theme_cluster": "HEALTHCARE", "score_final": 1.0 - i * 0.01} for i in range(12)]
    final, meta = enforce_regime_sector_caps(
        rows,
        rows,
        {"market_regime": "RISK_OFF", "max_ai_tech_ratio": 0.10, "max_single_cluster_ratio": 0.10},
        30,
    )
    assert len(final) == 12
    assert meta["risk_off_entry_block"] is True
    assert meta["cap_violations"] == []

    contract = build_us_prep_contract(
        trade_date="2026-07-10",
        env="practice",
        status="OK",
        dynamic_universe_result={"filtered_count": 50},
        candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={
            "top50_count": 50,
            "final30_count": len(final),
            "final30_scored_count": len(final),
            "cluster_contract_ok": True,
            "final30_cluster_cap_clean": True,
            "cap_violations": [],
            "market_state_overlay": {
                "market_regime": "RISK_OFF",
                "capital_scale": 0.0,
                "allow_new_buy": False,
                "force_entry_block": True,
            },
        },
        validation={"ok": True, "score_nonzero_count": len(final), "warnings": [], "errors": []},
        paths={},
    )
    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] == "risk_off_entry_block"
    assert contract["final30_count"] == 12
    assert contract["score_nonzero_count"] == 12


def test_degraded_benchmark_with_mild_weakness_not_crash():
    from trader.us.market_state_overlay import evaluate_us_market_state

    def series(last_return):
        return [{"date": "20260709", "close": 100.0}, {"date": "20260710", "close": 100.0 * (1 + last_return)}]

    provider = {
        "SPY": series(0.0),
        "QQQ": series(-0.003),
        "SMH": series(-0.011),
    }
    result = evaluate_us_market_state(
        trade_date="2026-07-10",
        provider=provider,
        rotation_context={"benchmark_data_quality": "degraded", "missing_symbols": ["XLK"], "rotation_regime": "NEUTRAL"},
        prep_result={},
        positions=[],
        account_snapshot={},
    )
    assert result["market_state"] != "DEFENSE_CRASH"
    assert "DEGRADED_BENCHMARK_WITH_WEAK_INDEX" not in result["market_state_reasons"]


def test_market_state_uses_etf_exchange_map_for_breadth_and_sector_etfs():
    from trader.us.market_state_overlay import _market_returns

    calls = []

    class Provider:
        def get_daily_prices(self, symbol, exchange="NYSE", as_of_date=None):
            calls.append((symbol, exchange))
            return [{"date": f"202607{day:02d}", "close": 100.0 + day} for day in range(1, 25)]

    warnings = []
    result = _market_returns(Provider(), "2026-07-10", warnings)
    call_map = dict(calls)

    for symbol in ("SPY", "DIA", "IWM", "RSP", "XLK", "XLI", "XLF", "XLV", "XLP", "XLU", "XLE"):
        assert call_map[symbol] == "AMEX"
    for symbol in ("QQQ", "SMH"):
        assert call_map[symbol] == "NASDAQ"

    assert result["rsp_20d_return"] is not None
    assert result["iwm_20d_return"] is not None
    assert result["xlk_20d_return"] is not None
    assert not [w for w in warnings if w.startswith("market_return_missing:")]


def test_risk_on_final30_28_allows_trade_without_haircut(monkeypatch):
    monkeypatch.setenv("US_ABSOLUTE_MIN_FINAL30_FOR_TRADE", "15")
    monkeypatch.setenv("US_DEGRADED_MIN_FINAL30_FOR_TRADE", "20")
    monkeypatch.setenv("US_NORMAL_MIN_FINAL30_FOR_TRADE", "25")

    contract = build_us_prep_contract(
        trade_date="2026-07-10", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50},
        candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": 28, "final30_scored_count": 28, "cluster_contract_ok": True, "final30_cluster_cap_clean": True, "cap_violations": [], "market_state_overlay": {"market_state": "STRONG_RISK_ON", "market_regime": "RISK_ON", "capital_scale": 1.0, "max_new_positions": 30, "allow_new_buy": True, "allow_ai_tech_buy": True, "force_entry_block": False}},
        validation={"ok": True, "score_nonzero_count": 28, "agent_a_nonzero_count": 28, "agent_b_nonzero_count": 28, "warnings": [], "errors": []},
        paths={},
    )

    assert contract["status"] == "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE"
    assert contract["trade_can_proceed"] == 1
    assert contract["trade_block_reason"] == "ok"
    assert contract["contract_ok"] is True
    assert contract["final30_complete"] is False
    assert contract["final30_trade_ready"] is True
    assert contract["underfilled_final30"] is True
    assert contract["underfilled_tier"] == "normal_underfilled"
    assert contract["underfilled_capital_haircut"] == 1.0
    assert contract["effective_capital_scale"] == 1.0
    assert contract["effective_max_new_positions"] == 28


def test_final30_24_allows_trade_with_degraded_haircut(monkeypatch):
    monkeypatch.setenv("US_ABSOLUTE_MIN_FINAL30_FOR_TRADE", "15")
    monkeypatch.setenv("US_DEGRADED_MIN_FINAL30_FOR_TRADE", "20")
    monkeypatch.setenv("US_NORMAL_MIN_FINAL30_FOR_TRADE", "25")
    monkeypatch.setenv("US_UNDERFILLED_DEGRADED_CAPITAL_HAIRCUT", "0.75")

    contract = build_us_prep_contract(
        trade_date="2026-07-10", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50}, candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": 24, "final30_scored_count": 24, "cluster_contract_ok": True, "final30_cluster_cap_clean": True, "cap_violations": [], "market_state_overlay": {"market_state": "NORMAL", "market_regime": "NEUTRAL", "capital_scale": 0.5, "max_new_positions": 10, "allow_new_buy": True, "force_entry_block": False}},
        validation={"ok": True, "score_nonzero_count": 24, "agent_a_nonzero_count": 24, "agent_b_nonzero_count": 24, "warnings": [], "errors": []}, paths={},
    )

    assert contract["trade_can_proceed"] == 1
    assert contract["trade_block_reason"] == "ok"
    assert contract["underfilled_tier"] == "degraded_underfilled"
    assert contract["underfilled_capital_haircut"] == 0.75
    assert contract["effective_capital_scale"] == 0.375
    assert contract["effective_max_new_positions"] == 10


def test_final30_18_risk_on_allows_limited_trade(monkeypatch):
    monkeypatch.setenv("US_ABSOLUTE_MIN_FINAL30_FOR_TRADE", "15")
    monkeypatch.setenv("US_DEGRADED_MIN_FINAL30_FOR_TRADE", "20")
    monkeypatch.setenv("US_NORMAL_MIN_FINAL30_FOR_TRADE", "25")
    monkeypatch.setenv("US_UNDERFILLED_SEVERE_CAPITAL_HAIRCUT", "0.50")

    contract = build_us_prep_contract(
        trade_date="2026-07-10", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50}, candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": 18, "final30_scored_count": 18, "cluster_contract_ok": True, "final30_cluster_cap_clean": True, "cap_violations": [], "market_state_overlay": {"market_state": "STRONG_RISK_ON", "market_regime": "RISK_ON", "capital_scale": 1.0, "max_new_positions": 30, "allow_new_buy": True, "force_entry_block": False}},
        validation={"ok": True, "score_nonzero_count": 18, "agent_a_nonzero_count": 18, "agent_b_nonzero_count": 18, "warnings": [], "errors": []}, paths={},
    )

    assert contract["trade_can_proceed"] == 1
    assert contract["trade_block_reason"] == "ok"
    assert contract["underfilled_tier"] == "severe_underfilled"
    assert contract["underfilled_capital_haircut"] == 0.5
    assert contract["effective_capital_scale"] == 0.5
    assert contract["effective_max_new_positions"] <= 5


def test_final30_18_neutral_blocks_trade(monkeypatch):
    monkeypatch.setenv("US_ABSOLUTE_MIN_FINAL30_FOR_TRADE", "15")
    monkeypatch.setenv("US_DEGRADED_MIN_FINAL30_FOR_TRADE", "20")
    monkeypatch.setenv("US_NORMAL_MIN_FINAL30_FOR_TRADE", "25")

    contract = build_us_prep_contract(
        trade_date="2026-07-10", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50}, candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": 18, "final30_scored_count": 18, "cluster_contract_ok": True, "cap_violations": [], "market_state_overlay": {"market_state": "NORMAL", "market_regime": "NEUTRAL", "capital_scale": 0.5, "allow_new_buy": True, "force_entry_block": False}},
        validation={"ok": True, "score_nonzero_count": 18, "agent_a_nonzero_count": 18, "agent_b_nonzero_count": 18, "warnings": [], "errors": []}, paths={},
    )

    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] == "final30_underfilled_regime_block"
    assert contract["underfilled_tier"] == "severe_underfilled_blocked"
    assert contract["effective_capital_scale"] == 0.0
    assert contract["effective_max_new_positions"] == 0


def test_final30_14_blocks_even_in_risk_on(monkeypatch):
    monkeypatch.setenv("US_ABSOLUTE_MIN_FINAL30_FOR_TRADE", "15")

    contract = build_us_prep_contract(
        trade_date="2026-07-10", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50}, candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": 14, "final30_scored_count": 14, "cluster_contract_ok": True, "cap_violations": [], "market_state_overlay": {"market_state": "STRONG_RISK_ON", "market_regime": "RISK_ON", "capital_scale": 1.0, "allow_new_buy": True, "force_entry_block": False}},
        validation={"ok": True, "score_nonzero_count": 14, "agent_a_nonzero_count": 14, "agent_b_nonzero_count": 14, "warnings": [], "errors": []}, paths={},
    )

    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] == "final30_below_absolute_min"
    assert contract["underfilled_tier"] == "blocked_underfilled"


def test_risk_off_blocks_even_when_final30_28_ready(monkeypatch):
    contract = build_us_prep_contract(
        trade_date="2026-07-10", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50}, candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": 28, "final30_scored_count": 28, "cluster_contract_ok": True, "cap_violations": [], "market_state_overlay": {"market_state": "DEFENSE_RISK_OFF", "market_regime": "RISK_OFF", "capital_scale": 0.0, "allow_new_buy": False, "force_entry_block": True}},
        validation={"ok": True, "score_nonzero_count": 28, "agent_a_nonzero_count": 28, "agent_b_nonzero_count": 28, "warnings": [], "errors": []}, paths={},
    )

    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] == "risk_off_entry_block"
    assert contract["effective_capital_scale"] == 0.0
    assert contract["effective_max_new_positions"] == 0


def test_underfilled_score_mismatch_blocks_trade(monkeypatch):
    contract = build_us_prep_contract(
        trade_date="2026-07-10", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50}, candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": 24, "final30_scored_count": 24, "cluster_contract_ok": True, "cap_violations": [], "market_state_overlay": {"market_regime": "RISK_ON", "capital_scale": 1.0, "allow_new_buy": True, "force_entry_block": False}},
        validation={"ok": True, "score_nonzero_count": 23, "agent_a_nonzero_count": 24, "agent_b_nonzero_count": 24, "warnings": [], "errors": []}, paths={},
    )

    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] == "score_contract_failed"


def test_underfilled_cap_violation_blocks_trade(monkeypatch):
    contract = build_us_prep_contract(
        trade_date="2026-07-10", env="practice", status="OK",
        dynamic_universe_result={"filtered_count": 50}, candidate_pool_result={"selected_count": 30, "status": "OK"},
        watchlist_result={"top50_count": 50, "final30_count": 24, "final30_scored_count": 24, "cluster_contract_ok": False, "cap_violations": ["SINGLE_CLUSTER"], "market_state_overlay": {"market_regime": "RISK_ON", "capital_scale": 1.0, "allow_new_buy": True, "force_entry_block": False}},
        validation={"ok": True, "score_nonzero_count": 24, "agent_a_nonzero_count": 24, "agent_b_nonzero_count": 24, "warnings": [], "errors": []}, paths={},
    )

    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] == "sector_cap_violation_block"
