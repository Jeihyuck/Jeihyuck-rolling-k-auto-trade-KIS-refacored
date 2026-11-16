"""2026-11-16 PB1 KR P0: verified opening-proof recovery and broker-truth scope.

All candles are synthetic fixtures. No KIS credentials, broker orders, live
account mutations or claims of historical market execution.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from trader.kr import opening_bar_contract as opening

KST = ZoneInfo("Asia/Seoul")
START = datetime(2026, 11, 16, 9, 0, tzinfo=KST)
LATE = datetime(2026, 11, 16, 9, 36, tzinfo=KST)


def candles(start: int, stop: int) -> list[dict]:
    rows = []
    for minute in range(start, stop):
        ts = START + timedelta(minutes=minute)
        close = 10000.0 + minute * 10.0
        rows.append({
            "date": "20261116", "time": ts.strftime("%H%M%S"),
            "open": close - 2, "high": close + 2,
            "low": close - 3, "close": close, "volume": 1000,
        })
    return rows


def save(now, rows, *, account="p:account:1", code="093320", vwap=10000):
    return opening.save_opening_proof(
        env="practice", account_scope=account, code=code,
        now=now, raw_bars=rows,
        vwap=vwap,
        vwap_source="KIS_NATIVE_WEIGHTED_AVG" if vwap else "UNKNOWN",
    )


def load(now, *, account="p:account:1", code="093320", age=180):
    return opening.load_opening_proof(
        env="practice", account_scope=account, code=code, now=now,
        max_age_seconds=age,
    )


def test_partial_opening_18_plus_18_recovers_afternoon_gate_without_fabrication(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path",
                        lambda *parts: tmp_path.joinpath(*parts))
    early = START + timedelta(minutes=18)
    first = save(early, candles(0, 18), vwap=None)
    assert first["bars"] == 18  # 09:17 has completed by 09:18
    # The original proof is too old for any live order after five minutes.
    assert load(LATE) is None
    second = save(LATE, candles(17, 36), vwap=10100)
    assert second["added_bars"] == 18
    assert second["bars"] == 36
    merged = load(LATE)
    assert merged is not None
    assert len(merged["bars"]) == 36
    assert len({x["timestamp"] for x in merged["bars"]}) == 36
    cov = opening.inspect_completed_bar_coverage(rows=merged["bars"], now=LATE)
    assert cov["state"] == "COVERED", cov
    assert cov["opening_count"] == 30
    assert load(LATE, account="another-account") is None
    assert load(LATE, code="121600") is None


def test_overlapping_native_pages_do_not_duplicate_or_replace_good_candles(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path",
                        lambda *parts: tmp_path.joinpath(*parts))
    save(LATE, candles(0, 30), vwap=10100)
    result = save(LATE, candles(15, 36), vwap=None)
    assert result["bars"] == 36
    assert result["added_bars"] == 6
    proof = load(LATE)
    assert proof["source_conflicts"] == []
    assert proof["vwap"] is None  # Never recycle an old VWAP as freshly verified.
    assert proof["vwap_source"] == "UNKNOWN"
    assert len(proof["bars"]) == 36


def test_conflicting_completed_native_candle_blocks_buy_without_price_guess(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path",
                        lambda *parts: tmp_path.joinpath(*parts))
    save(LATE, candles(0, 36))
    conflict = candles(0, 36)
    conflict[10]["high"] += 20.0
    updated = save(LATE, conflict)
    assert len(updated["source_conflicts"]) == 1
    result = opening.evaluate_proof_for_candidate(
        env="practice", account_scope="p:account:1", code="093320",
        now=LATE, style="ENTRY_MOMENTUM", setup_ok=True,
        trigger_ok=False, pivot=None, order_price=10350.0,
    )
    assert result["state"] == "UNKNOWN"
    assert result["reason"] == "CONFLICTING_NATIVE_COMPLETED_BAR"


def test_failed_atomic_replace_preserves_prior_checksum_proof(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path",
                        lambda *parts: tmp_path.joinpath(*parts))
    prior = save(LATE, candles(0, 22))
    original_sha = prior["sha256"]
    monkeypatch.setattr(opening.os, "replace",
                        lambda *args: (_ for _ in ()).throw(OSError("disk failed")))
    with pytest.raises(OSError):
        save(LATE, candles(22, 36))
    recovered = load(LATE)
    assert recovered and recovered["sha256"] == original_sha
    assert len(recovered["bars"]) == 22
    assert list(tmp_path.rglob("*.tmp")) == []


def test_two_concurrent_collectors_merge_without_lost_updates(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path",
                        lambda *parts: tmp_path.joinpath(*parts))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(save, LATE, candles(a, b))
                   for a, b in ((0, 20), (20, 36))]
        [f.result() for f in futures]
    merged = load(LATE)
    assert merged is not None
    assert len(merged["bars"]) == 36
    assert opening.inspect_completed_bar_coverage(
        rows=merged["bars"], now=LATE,
    )["state"] == "COVERED"


def test_post_tick_reconcile_forces_current_only_even_when_legacy_default_full(monkeypatch):
    import trader.reconcile_kis as rk
    import trader.kr.broker_truth_hardening as hardening

    calls = []

    def spy_reconcile_kis(**kwargs):
        calls.append(kwargs)
        return {"ok": True, "orders": 0, "fills": 0,
                "_final_holdings_rows": []}

    monkeypatch.setattr(rk, "reconcile_kis", spy_reconcile_kis)
    monkeypatch.setattr(hardening, "kr_tick_remaining_sec", lambda: 120.0)
    monkeypatch.setattr(hardening, "_broker_crossed_this_tick",
                        lambda engine_obj: False)
    monkeypatch.setattr(hardening, "_recover_proven_policy_positions",
                        lambda **kwargs: {"recovered": [], "review_required": []})
    monkeypatch.setattr(hardening, "_health_after_reconcile",
                        lambda **kwargs: {"qty_mismatch_count": 0,
                                          "stale_open_order_count": 0})
    engine_obj = SimpleNamespace(
        dry_run=False, intended_live=True, env="practice",
        engine=object(), kis=object(), run_id="synthetic-only",
        _balance_snapshot={"output1": []}, _run_summary_payload={},
        STRATEGY_NAME="pb1_pullback_close",
    )
    hardening._post_pb1_tick_reconcile(engine_obj)
    assert len(calls) == 1
    assert calls[0]["historical_recovery"] is False
    assert engine_obj._run_summary_payload["broker_truth_reconcile_degraded"] == 0


def test_original_momentum_signal_and_final_swing_exit_contract_are_separate():
    from trader.trade_plan import build_entry_exit_plan
    from trader.kr.final_entry_contract_proof import (
        build_final_entry_contract_proof,
        verify_final_entry_contract_proof,
    )

    plan = build_entry_exit_plan(
        code="093320", market="KR",
        entry_style_selected="ENTRY_MOMENTUM_SWING",
        entry_reason="ENTRY_MOMENTUM", entry_price=10000.0,
    ).to_dict()
    original = {"selected_style": "ENTRY_MOMENTUM",
                "contract_trade_horizon": "DAY_TRADE"}
    proof = build_final_entry_contract_proof(
        code="093320", owner="PB1", plan=plan,
        original_identity=original,
    )
    assert proof["original_selected_style"] == "ENTRY_MOMENTUM"
    assert proof["original_trade_horizon"] == "DAY_TRADE"
    assert proof["final_style"] == "ENTRY_MOMENTUM_SWING"
    assert proof["trade_horizon"] == "SWING"
    assert proof["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert verify_final_entry_contract_proof(plan=plan, proof=proof)
    changed = {**plan, "trade_horizon": "DAY_TRADE"}
    assert not verify_final_entry_contract_proof(plan=changed, proof=proof)
    with pytest.raises(ValueError, match="OWNER"):
        build_final_entry_contract_proof(
            code="093320", owner="KR_INFINITE_V1", plan=plan,
        )


def test_pre_submit_rejects_modified_final_exit_contract_before_timing_or_broker():
    from trader.pb1_engine import PB1Engine
    from trader.trade_plan import build_entry_exit_plan
    from trader.kr.final_entry_contract_proof import build_final_entry_contract_proof

    plan = build_entry_exit_plan(
        code="093320", market="KR",
        entry_style_selected="ENTRY_MOMENTUM_SWING",
        entry_reason="ENTRY_MOMENTUM", entry_price=10000,
    ).to_dict()
    proof = build_final_entry_contract_proof(
        code="093320", owner="PB1", plan=plan,
    )
    wrong = {**plan, "exit_policy_family": "INTRADAY_PROFIT_PROTECT"}
    candidate = SimpleNamespace(
        code="093320",
        features={"entry_exit_plan": wrong, "final_entry_contract_proof": proof},
    )
    eng = PB1Engine.__new__(PB1Engine)
    result = eng._resolve_entry_pre_submit(
        cf=candidate, stage="PB1-AM", order_price=10000, order_type="LIMIT",
    )
    assert result.ok is False
    assert result.blocking_stage == "pre_submit_contract"
    assert result.reason_codes == ["KR_FINAL_ENTRY_CONTRACT_PROOF_MISMATCH"]


def test_ws_appkey_contention_backs_off_all_symbols_without_fabricating_quote():
    import asyncio
    import json
    import time
    from trader.marketdata.kis_ws_price import KisWebSocketPriceService

    service = KisWebSocketPriceService()
    before = time.monotonic()
    asyncio.run(service._handle_message(None, json.dumps({
        "header": {"tr_id": "H0STCNT0", "tr_key": "093320"},
        "body": {"rt_cd": "1", "msg1": "ALREADY IN USE appkey"},
    })))
    assert service._appkey_busy_events == 1
    assert service._appkey_busy_until >= before + 30
    other = {("KR", "121600"): ("H0STCNT0", "121600")}
    assert service._next_pending_subscription(
        other, set(), max_subscriptions=40,
        now_mono=time.monotonic(),
    ) is None
    assert service.get_fresh_quote(
        "KR", "121600", max_age_sec=5.0,
    ) is None
    # After the circuit cooldown, unrelated subscriptions can be selected.
    assert service._next_pending_subscription(
        other, set(), max_subscriptions=40,
        now_mono=service._appkey_busy_until + 0.1,
    ) is not None


def test_actual_20261116_anonymous_appkey_collision_does_not_reconnect_loop():
    """Actual archive reports header tr_id='(null)', tr_key='', appkey in use."""
    import asyncio
    import json
    import time
    from trader.marketdata.kis_ws_price import KisWebSocketPriceService

    service = KisWebSocketPriceService()
    service._desired = {
        ("KR", "093320"): ("H0STCNT0", "093320"),
        ("KR", "121600"): ("H0STCNT0", "121600"),
    }
    # No socket is needed: unlike the old handler, this must not raise a
    # reconnect-worthy RuntimeError for an anonymous broker-wide conflict.
    asyncio.run(service._handle_message(None, json.dumps({
        "header": {"tr_id": "(null)", "tr_key": ""},
        "body": {"rt_cd": "1", "msg1": "ALREADY IN USE appkey"},
    })))
    assert service._appkey_busy_events == 1
    assert service._appkey_busy_until >= time.monotonic() + 29
    assert service._retry_subscriptions == {
        ("H0STCNT0", "093320"), ("H0STCNT0", "121600")
    }
    assert service._broker_subscriptions_acked == set()
    assert service._next_pending_subscription(
        service._desired, set(), max_subscriptions=40,
        now_mono=time.monotonic(),
    ) is None


def test_nano_order_price_evidence_records_signed_0p5pct_without_gate_relaxation(monkeypatch):
    import trader.pb1_engine as pb1
    now = datetime(2026, 11, 16, 9, 36, tzinfo=KST)
    monkeypatch.setattr(pb1, "now_kst", lambda: now)
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    monkeypatch.setattr(pb1, "evaluate_proof_for_candidate", lambda **kwargs: {
        "state": "WAIT", "reason": "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR",
        "reference_price": 10000.0, "latest_bar_ts": "2026-11-16T09:34:00+09:00",
        "observed_volume_ratio": 1.4, "opening_count": 30,
        "max_entry_price": None,
    })
    eng = pb1.PB1Engine.__new__(pb1.PB1Engine)
    eng.env = "practice"
    eng._now_kst = now
    candidate = SimpleNamespace(
        code="121600", setup_ok=True,
        features={"entry_style_selected": "ENTRY_MOMENTUM",
                  "setup_ok": True, "trigger_ok": False,
                  "price_source": "prpr_slippage"},
    )
    decision = eng._resolve_entry_pre_submit(
        cf=candidate, stage="PB1-AM", order_price=10050, order_type="LIMIT",
    )
    assert not decision.ok
    proof = candidate.features["entry_timing_evidence"]
    assert proof["reference_price"] == 10000.0
    assert proof["signed_price_deviation_pct"] == 0.5
    assert proof["order_price_source"] == "prpr_slippage"
    assert proof["is_order_authorization"] is False
    assert decision.reason_codes == ["KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH"]


def test_ws_identical_appkey_has_one_cross_process_owner(tmp_path, monkeypatch):
    """One appkey has one socket owner, whether selected via KR or US key.

    A WSL process crash automatically releases the kernel flock; no stale
    PID file may grant ownership. No broker connection is made in this test.
    """
    from trader.marketdata.kis_ws_price import KisWebSocketPriceService
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KIS_WS_OWNER_LOCK_DIR", str(tmp_path))
    monkeypatch.setenv("KIS_APP_KEY", "synthetic-shared-app-key")
    monkeypatch.setenv("KIS_APP_SECRET", "synthetic-secret")
    monkeypatch.setenv("KIS_US_APP_KEY", "synthetic-shared-app-key")
    monkeypatch.setenv("KIS_US_APP_SECRET", "synthetic-secret")
    first = KisWebSocketPriceService()
    second = KisWebSocketPriceService()
    a = first._acquire_owner_lock()
    assert a is not None
    try:
        assert second._acquire_owner_lock() is None
        assert list(tmp_path.glob("*.lock"))
        assert "synthetic-shared-app-key" not in (
            list(tmp_path.glob("*.lock"))[0].read_text()
        )
    finally:
        first._release_owner_lock(a)
    b = second._acquire_owner_lock()
    assert b is not None
    second._release_owner_lock(b)


def test_ws_different_appkeys_do_not_block_each_other(tmp_path, monkeypatch):
    from trader.marketdata.kis_ws_price import KisWebSocketPriceService
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KIS_WS_OWNER_LOCK_DIR", str(tmp_path))
    monkeypatch.setenv("KIS_APP_SECRET", "offline-only")
    first = KisWebSocketPriceService()
    second = KisWebSocketPriceService()
    monkeypatch.setenv("KIS_APP_KEY", "offline-key-one")
    a = first._acquire_owner_lock()
    monkeypatch.setenv("KIS_APP_KEY", "offline-key-two")
    b = second._acquire_owner_lock()
    try:
        assert a is not None and b is not None
    finally:
        first._release_owner_lock(a)
        second._release_owner_lock(b)


def test_appkey_rejection_invalidates_even_recent_ws_quote(monkeypatch):
    from trader.marketdata.kis_ws_price import KisWebSocketPriceService
    monkeypatch.setenv("KIS_WS_PRICE_TEST_ENABLE", "1")
    monkeypatch.setenv("KIS_WS_PRICE_ENABLED", "1")
    monkeypatch.setenv("FORCE_HTTP", "1")
    svc = KisWebSocketPriceService()
    svc.put_quote(market="KR", symbol="093320", last=175300.0)
    assert svc.get_fresh_quote("KR", "093320", max_age_sec=10.0)
    svc._schedule_appkey_contention()
    assert svc.get_fresh_quote("KR", "093320", max_age_sec=10.0) is None
    # Even if a late socket callback provides data during the broker
    # ownership cooldown, it is not a verified usable market quote.
    svc.put_quote(market="KR", symbol="093320", last=175400.0)
    assert svc.get_fresh_quote("KR", "093320", max_age_sec=10.0) is None


def test_kinx_native_bar_diagnostics_isolates_source_date_and_ohlcv_failures():
    from trader.kr.opening_bar_contract import diagnose_native_bar_rows
    now = datetime(2026, 11, 16, 13, 30, tzinfo=KST)
    good = {"date": "2026-11-16", "time": "090100",
            "open": 100.0, "high": 110.0, "low": 90.0,
            "close": 101.0, "volume": 12.0}
    rows = [
        good, dict(good), {**good, "time": "090200", "date": ""},
        {**good, "time": "090300", "date": "2026-11-13"},
        {**good, "time": "090400", "volume": 0},
        {**good, "time": "090500", "low": 120.0},
        {**good, "time": "bad"},
        {**good, "time": "130000"},  # accepted completed current day
        {**good, "time": "133000"},  # current unfinished minute
    ]
    info = diagnose_native_bar_rows(rows, now)
    assert info["total_rows"] == 9
    assert info["valid_unique_minutes"] == 2
    assert info["duplicate_valid_rows"] == 1
    assert info["excluded_by_reason"] == {
        "SOURCE_DATE_MISSING": 1,
        "SOURCE_DATE_MISMATCH": 1,
        "OHLCV_MISSING_OR_NONPOSITIVE": 1,
        "OHLC_INCONSISTENT": 1,
        "SOURCE_CLOCK_INVALID": 1,
        "CURRENT_OR_FUTURE_MINUTE": 1,
    }
    assert info["no_trade_minutes_proven"] is False


def test_nano_0p5pct_artificial_limit_is_bounded_to_original_0p4pct_gate():
    from trader.kr.entry_timing_price import resolve_bounded_prpr_limit

    now = datetime(2026, 11, 16, 9, 40, tzinfo=KST)
    proof = {
        "state": "READY_SHADOW", "reference_price": 72100.0,
        "max_entry_price": 72800.0,
    }
    quote = {
        "source": "KIS_REST_INQUIRE_PRICE", "requested_symbol": "121600",
        "last": 72100.0, "received_at": now.timestamp() - 1,
        "stck_bsop_date": "20261116",
    }
    result = resolve_bounded_prpr_limit(
        code="121600", original_price=72500.0, timing=proof,
        quote=quote, observed_at=now,
    )
    assert result["state"] == "BOUNDED_LIMIT"
    assert result["reason"] == "PASSIVE_LIMIT_NO_ASK"
    assert result["new_limit"] == 72300  # Native KRX tick, rounded DOWN
    assert result["new_limit"] <= proof["max_entry_price"]
    assert abs(result["new_limit"] / 72100 - 1) <= 0.004
    assert result["not_order_authorization"] is True

    # A native offer beyond the accepted maximum may NOT be chased.
    high_ask = resolve_bounded_prpr_limit(
        code="121600", original_price=72500, timing=proof,
        quote={**quote, "ask": 72400}, observed_at=now,
    )
    assert high_ask["reason"] == "ASK_ABOVE_MAX_ENTRY_PRICE"
    assert high_ask["new_limit"] is None

    within = resolve_bounded_prpr_limit(
        code="121600", original_price=72500, timing=proof,
        quote={**quote, "ask": 72300}, observed_at=now,
    )
    assert within["state"] == "BOUNDED_LIMIT"
    assert within["new_limit"] == 72300
    assert within["reason"] == "VERIFIED_ASK"

    stale = resolve_bounded_prpr_limit(
        code="121600", original_price=72500, timing=proof,
        quote={**quote, "received_at": now.timestamp() - 20},
        observed_at=now,
    )
    assert stale["reason"] == "QUOTE_STALE"
    assert stale["new_limit"] is None

    under = resolve_bounded_prpr_limit(
        code="121600", original_price=72500, timing=proof,
        quote={**quote, "last": 70000}, observed_at=now,
    )
    assert under["reason"] == "MINUTE_CLOSE_PRICE_DEVIATION"
    assert under["new_limit"] is None


def test_bounded_price_never_turns_unready_momentum_into_buy(monkeypatch):
    from trader.kr import entry_timing_price as ep
    now = datetime(2026, 11, 16, 10, 30, tzinfo=KST)
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    monkeypatch.setattr("trader.account_state.get_account_key", lambda env: "test-account")
    monkeypatch.setattr("trader.kr.opening_bar_contract.load_opening_proof",
                        lambda **kw: {"bars": [{"close": 72100.0}]})
    monkeypatch.setattr("trader.kr.opening_bar_contract.evaluate_proof_for_candidate",
                        lambda **kw: {"state": "WAIT",
                                      "reason": "NO_CONFIRMED_MOMENTUM_RECLAIM"})
    cf = SimpleNamespace(code="121600", setup_ok=True,
                         features={"order_price_source": "prpr_slippage",
                                   "entry_style_selected": "ENTRY_MOMENTUM",
                                   "setup_ok": True})
    eng = SimpleNamespace(
        env="practice", STRATEGY_NAME="pb1_pullback_close",
        _get_price_snapshot_cached=lambda *a, **kw: pytest.fail(
            "No quote lookup after unready timing signal"),
    )
    result = ep.maybe_bound_pb1_prpr_fallback(
        engine=eng, cf=cf, stage="PB1-AM", order_type="LIMIT",
        original_price=72500, now=now,
    )
    assert result["state"] == "BLOCK"
    assert result["reason"] == "TIMING_NOT_READY"
    # Independent owners, earlier policy and CLOSE are never rewritten.
    assert ep.maybe_bound_pb1_prpr_fallback(
        engine=eng, cf=cf, stage="PB1-CLOSE", order_type="LIMIT",
        original_price=72500, now=now,
    )["state"] == "NOT_APPLICABLE"
