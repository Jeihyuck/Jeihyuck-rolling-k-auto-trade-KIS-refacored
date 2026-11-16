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
    assert first["bars"] == 17  # 09:17 has not completed at 09:18
    # The original proof is too old for any live order after five minutes.
    assert load(LATE) is None
    second = save(LATE, candles(17, 36), vwap=10100)
    assert second["added_bars"] == 19
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
