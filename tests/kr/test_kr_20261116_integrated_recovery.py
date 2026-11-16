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
