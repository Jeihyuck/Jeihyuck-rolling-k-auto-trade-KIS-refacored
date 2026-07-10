from trader.us.watchlist_builder import validate_final30_cluster_contract, refill_under_cluster_caps
from trader.us.portfolio_cluster_guard import evaluate_portfolio_cluster_guard, filter_entry_intents_for_cluster_guard
from trader.us.utils.session_guard import check_us_session_file_guard, write_us_session_done_file


def row(sym, cluster, score=1.0):
    return {"symbol": sym, "theme_cluster": cluster, "score_final": score}


def test_risk_off_ai_tech_25_of_30_blocks_contract():
    rows = [row(f"A{i}", "AI_SEMI") for i in range(25)] + [row(f"H{i}", "HEALTHCARE") for i in range(5)]
    result = validate_final30_cluster_contract(rows, "RISK_OFF")
    assert result["cluster_contract_ok"] is False
    assert "AI_TECH_COMBINED" in result["cap_violations"]


def test_risk_off_refill_does_not_break_ai_tech_cap():
    selected = [row(f"AI{i}", "AI_SEMI") for i in range(5)]
    pool = [row(f"AIx{i}", "AI_SOFTWARE", 2.0) for i in range(20)] + [row(f"N{i}", "HEALTHCARE", 1.0) for i in range(20)]
    final, meta = refill_under_cluster_caps(selected, pool, 30, "RISK_OFF", set())
    result = validate_final30_cluster_contract(final, "RISK_OFF")
    assert result["cluster_contract_ok"] is True
    assert result["final30_ai_tech_ratio"] <= 0.20
    assert meta["fallback_fill_cap_safe"] is True


def test_ai_off_rotation_ai_tech_over_30_blocks_contract():
    rows = [row(f"A{i}", "AI_SEMI") for i in range(10)] + [row(f"N{i}", "HEALTHCARE") for i in range(20)]
    result = validate_final30_cluster_contract(rows, "AI_OFF_ROTATION")
    assert result["cluster_contract_ok"] is False
    assert "AI_TECH_COMBINED" in result["cap_violations"] or "AI_SEMI" in result["cap_violations"]


def test_portfolio_guard_blocks_buys_and_creates_trim():
    positions = [{"symbol": "NVDA", "qty": 10, "market_value_usd": 8000, "last_price": 800}, {"symbol": "JNJ", "qty": 10, "market_value_usd": 2000, "last_price": 200}]
    guard = evaluate_portfolio_cluster_guard(positions, "RISK_OFF", 10000, [], None, None)
    assert guard["portfolio_cluster_cap_violations"]
    assert guard["cluster_guard_trim_intents"]
    dup_guard = evaluate_portfolio_cluster_guard(positions, "RISK_OFF", 10000, [{"symbol": "NVDA", "side": "SELL"}], None, None)
    assert not dup_guard["cluster_guard_trim_intents"]
    kept, blocked = filter_entry_intents_for_cluster_guard([{"symbol": "AMD", "side": "BUY"}, {"symbol": "JNJ", "side": "BUY"}], guard)
    assert blocked == ["AMD"]
    assert [i["symbol"] for i in kept] == ["JNJ"]


def test_manual_done_does_not_block_schedule(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_us_session_done_file("2026-07-09", "am", "r1", "s", "f", "OK", ticks=2, extra={"event_name": "workflow_dispatch", "max_ticks": 2, "dry_run": False})
    assert check_us_session_file_guard("2026-07-09", "am")["already_ran"] is False
    write_us_session_done_file("2026-07-09", "am", "r2", "s", "f", "OK", ticks=10, extra={"event_name": "schedule", "run_type": "schedule", "dry_run": False})
    assert check_us_session_file_guard("2026-07-09", "am")["already_ran"] is True


def test_legacy_workflow_dispatch_done_is_ignored(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    done = tmp_path / "runtime/session_guard/us/2026-07-09/am.done"
    done.parent.mkdir(parents=True)
    done.write_text('{"event_name":"workflow_dispatch","status":"OK","ticks":2}', encoding="utf-8")
    assert check_us_session_file_guard("2026-07-09", "am")["already_ran"] is False


def test_incomplete_final30_never_trade_can_proceed_even_when_cap_clean():
    from trader.us.prep_contract import build_us_prep_contract

    contract = build_us_prep_contract(
        trade_date="2026-07-09",
        env="practice",
        status="OK",
        dynamic_universe_result={"filtered_count": 100},
        candidate_pool_result={"selected_count": 80, "status": "OK"},
        watchlist_result={
            "top50_count": 50,
            "final30_count": 24,
            "final30_scored_count": 24,
            "cluster_contract_ok": True,
            "final30_cluster_cap_clean": True,
            "cap_violations": [],
            "rotation_regime": "RISK_OFF",
            "final30_cluster_counts": {"HEALTHCARE": 24},
            "final30_ai_tech_ratio": 0.0,
        },
        validation={"ok": True, "score_nonzero_count": 24, "warnings": [], "errors": []},
        paths={},
    )
    assert contract["final30_complete"] is False
    assert contract["cluster_contract_ok"] is True
    assert contract["cap_violations"] == []
    assert contract["status"] == "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE"
    assert contract["trade_can_proceed"] == 1
    assert contract["trade_block_reason"] == "ok"
    assert contract["underfilled_tier"] == "degraded_underfilled"


def test_sector_cap_enforced_neutral_removes_ai_overweight():
    from trader.us.watchlist_builder import enforce_regime_sector_caps
    selected = [row(f"AI{i}", "AI_SEMI", 1.0 - i * 0.001) for i in range(21)] + [row(f"H{i}", "HEALTHCARE", 0.7) for i in range(9)]
    pool = selected + [row(f"D{i}", "UTILITIES", 0.69 - i * 0.001) for i in range(30)]
    final, meta = enforce_regime_sector_caps(selected, pool, {"market_regime": "NEUTRAL", "max_ai_tech_ratio": 0.35, "max_single_cluster_ratio": 1.0}, 30)
    assert sum(1 for r in final if r["theme_cluster"].startswith("AI")) / len(final) <= 0.35
    assert meta["blocked_by_cluster_cap"]
    assert meta["cap_violations"] == []


def test_cap_violation_without_replacement_blocks_trade():
    from trader.us.watchlist_builder import enforce_regime_sector_caps
    from trader.us.prep_contract import build_us_prep_contract
    selected = [row(f"AI{i}", "AI_SEMI", 1.0 - i * 0.001) for i in range(21)] + [row(f"H{i}", "HEALTHCARE", 0.7) for i in range(9)]
    final, meta = enforce_regime_sector_caps(selected, selected, {"market_regime": "NEUTRAL", "max_ai_tech_ratio": 0.35, "max_single_cluster_ratio": 1.0}, 30)
    contract = build_us_prep_contract(trade_date="2026-07-09", env="practice", status="OK", dynamic_universe_result={"filtered_count": 50}, candidate_pool_result={"selected_count": 30, "status": "OK"}, watchlist_result={"top50_count": 30, "final30_count": len(final), "final30_scored_count": len(final), "cluster_contract_ok": False, "final30_cluster_cap_clean": False, "cap_violations": meta["cap_violations"] or ["AI_TECH_COMBINED"], "blocked_by_cluster_cap": meta["blocked_by_cluster_cap"], "market_state_overlay": {"market_regime": "NEUTRAL", "allow_new_buy": True}}, validation={"ok": True, "score_nonzero_count": len(final), "warnings": [], "errors": []}, paths={})
    assert contract["trade_can_proceed"] == 0
    assert contract["trade_block_reason"] in {"sector_cap_violation_block", "cluster_cap_contract_failed"}
