"""Verified KR PB1 native minute policy replay, no KIS orders or DB mutation."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from trader.kr.opening_bar_contract import (
    evaluate_price_aware_entry, inspect_completed_bar_coverage,
)

KST = ZoneInfo("Asia/Seoul")
DAY = "20261118"
BASE = datetime(2026, 11, 18, 9, 0, tzinfo=KST)
NOW = datetime(2026, 11, 18, 9, 38, 20, tzinfo=KST)


def native_bar(minute, *, high, low, close, volume=1000):
    instant = BASE + timedelta(minutes=minute)
    return {
        "date": DAY, "time": instant.strftime("%H%M%S"),
        "open": close, "high": high, "low": low, "close": close,
        "volume": volume,
    }


def sparse_opening_and_native_rebound():
    opening = [native_bar(i, high=101, low=99, close=100)
               for i in range(16)]
    previous = [
        (110, 108, 109), (109, 106, 108), (107, 104, 106),
        (106, 102, 104), (104, 100, 102),
    ]
    recent = [
        (102, 100, 101.5), (103, 101, 102.5), (104, 102, 103.5),
    ]
    post = [
        native_bar(30 + i, high=h, low=l, close=c,
                   volume=1200 if i >= 5 else 1000)
        for i, (h, l, c) in enumerate(previous + recent)
    ]
    return opening + post


def signal(rows, now=NOW, **kwargs):
    return evaluate_price_aware_entry(
        style=kwargs.pop("style", "ENTRY_PULLBACK"),
        rows=rows, now=now, vwap=kwargs.pop("vwap", None),
        vwap_verified=kwargs.pop("vwap_verified", False),
        setup_ok=kwargs.pop("setup_ok", True),
        trigger_ok=kwargs.pop("trigger_ok", False),
        pivot=kwargs.pop("pivot", None),
        order_price=kwargs.pop("order_price", 103.5),
    )


def test_observer_can_report_partial_opening_while_pb1_verified_intraday_signal_is_ready():
    rows = sparse_opening_and_native_rebound()
    health = inspect_completed_bar_coverage(rows=rows, now=NOW)
    assert health["state"] == "UNKNOWN"
    assert health["opening_count"] == 16
    assert health["native_post_open_window_ready"] is True
    verdict = signal(rows)
    assert verdict["state"] == "READY_SHADOW"
    assert verdict["opening_range_complete"] is False
    assert verdict["reference_source"] == "KIS_NATIVE_POST_OPENING_5_PLUS_3"
    assert "opening_high" not in verdict
    assert verdict["max_entry_price"] >= 103.5


def test_sparse_native_window_can_use_zero_opening_bars_without_synthetic_open():
    rows = sparse_opening_and_native_rebound()[16:]
    verdict = signal(rows)
    assert verdict["state"] == "READY_SHADOW"
    assert verdict["opening_count"] == 0


def test_no_entry_before_0930_even_with_apparent_rebound():
    rows = sparse_opening_and_native_rebound()
    decision = signal(rows, now=datetime(2026, 11, 18, 9, 25, tzinfo=KST))
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "OPENING_OBSERVATION"


def test_missing_a_recent_native_minute_cannot_be_interpolated_or_approved():
    rows = [row for row in sparse_opening_and_native_rebound()
            if row["time"] != "093400"]
    decision = signal(rows)
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "POST_OPENING_NATIVE_WINDOW_GAP"


def test_stale_postopening_bars_fail_even_if_previous_signal_was_ready():
    decision = signal(
        sparse_opening_and_native_rebound(),
        now=datetime(2026, 11, 18, 9, 42, tzinfo=KST),
    )
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "COMPLETED_BAR_STALE"


def test_incomplete_native_window_cannot_approve_early_subsequent_entry():
    rows = sparse_opening_and_native_rebound()[:-3]
    decision = signal(rows, now=datetime(2026, 11, 18, 9, 35, tzinfo=KST))
    assert decision["state"] != "READY_SHADOW"
    assert decision["reason"] == "POST_OPENING_NATIVE_WINDOW_PENDING"


def test_real_price_move_beyond_limit_protects_rebound_entry():
    decision = signal(sparse_opening_and_native_rebound(), order_price=105)
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR"


def test_no_vwap_never_authorizes_sparse_momentum():
    decision = signal(sparse_opening_and_native_rebound(),
                      style="ENTRY_MOMENTUM")
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "NO_VERIFIED_INTRADAY_VWAP"


def test_breakout_without_owner_pivot_never_becomes_native_buy():
    decision = signal(sparse_opening_and_native_rebound(),
                      style="ENTRY_BREAKOUT", trigger_ok=False, pivot=102)
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "ORIGINAL_PIVOT_TRIGGER_UNCONFIRMED"


def test_policy_will_not_use_fake_previous_date_native_minutes():
    rows = sparse_opening_and_native_rebound()
    for row in rows:
        row["date"] = "20261117"
    decision = signal(rows)
    assert decision["state"] != "READY_SHADOW"


def test_16_native_bars_reach_actual_pb1_pre_submit_gate(monkeypatch):
    """An independently proven entry reaches PB1's real final timing fence.

    No fake broker acceptance is claimed: this test is strictly the native
    timing -> existing order-pre-submit boundary, with no KIS IO.
    """
    from types import SimpleNamespace
    import trader.pb1_engine as pb1

    rows = sparse_opening_and_native_rebound()
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    monkeypatch.setattr(pb1, "now_kst", lambda: NOW)
    def native_16(**kwargs):
        assert kwargs["style"] == "ENTRY_PULLBACK"
        return evaluate_price_aware_entry(
            style=kwargs["style"], rows=rows, now=NOW,
            vwap=None, vwap_verified=False, pivot=None,
            trigger_ok=False, setup_ok=kwargs["setup_ok"],
            order_price=kwargs["order_price"],
        )
    monkeypatch.setattr(pb1, "evaluate_proof_for_candidate", native_16)
    engine = pb1.PB1Engine.__new__(pb1.PB1Engine)
    engine.env = "practice"
    engine._now_kst = NOW
    candidate = SimpleNamespace(
        code="093320", setup_ok=True,
        features={"entry_style_selected": "ENTRY_PULLBACK",
                  "setup_ok": True, "trigger_ok": False},
    )
    decision = engine._resolve_entry_pre_submit(
        cf=candidate, stage="PB1-AM",
        order_price=103.5, order_type="LIMIT",
    )
    assert decision.ok is True, decision
    assert candidate.features["entry_timing_evidence"]["opening_count"] == 16
    assert candidate.features["entry_timing_evidence"]["state"] == "READY_SHADOW"
    assert candidate.features["entry_timing_evidence"]["is_order_authorization"] is False


def test_sparse_native_signal_still_fails_pre_submit_if_limit_chases(monkeypatch):
    from types import SimpleNamespace
    import trader.pb1_engine as pb1
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    monkeypatch.setattr(pb1, "now_kst", lambda: NOW)
    monkeypatch.setattr(
        pb1, "evaluate_proof_for_candidate",
        lambda **kwargs: signal(
            sparse_opening_and_native_rebound(),
            order_price=kwargs["order_price"],
        ),
    )
    engine = pb1.PB1Engine.__new__(pb1.PB1Engine)
    engine.env = "practice"
    engine._now_kst = NOW
    candidate = SimpleNamespace(
        code="093320", setup_ok=True,
        features={"entry_style_selected": "ENTRY_PULLBACK",
                  "setup_ok": True, "trigger_ok": False},
    )
    decision = engine._resolve_entry_pre_submit(
        cf=candidate, stage="PB1-AM",
        order_price=105.0, order_type="LIMIT",
    )
    assert decision.ok is False
    assert decision.reason_codes == ["KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH"]


def test_missing_opening_high_cannot_be_replaced_by_five_minute_high_for_breakout():
    rows = sparse_opening_and_native_rebound()
    decision = signal(
        rows, style="ENTRY_BREAKOUT", trigger_ok=True, pivot=102.0,
    )
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "OPENING_RANGE_HIGH_UNVERIFIED_FOR_BREAKOUT"
    assert decision["opening_range_complete"] is False
    assert decision["verified_pivot"] == 102.0


def test_partial_momentum_breakout_requires_true_opening_high():
    rows = sparse_opening_and_native_rebound()
    # A real VWAP and native 8-bar window do not make the 09:00-09:29
    # opening high known; use a same-day post-open series that surges above
    # its preceding five bars, with a valid rising three-minute signal.
    now = NOW
    day = DAY
    raw = []
    for minute, (lo, close, hi, vol) in enumerate([
        (96.0, 97.0, 97.5, 1000), (96.2, 97.0, 97.5, 1000),
        (96.0, 97.0, 97.5, 1000), (96.3, 97.0, 97.5, 1000),
        (96.0, 97.0, 97.5, 1000), (96.5, 97.3, 97.6, 1600),
        (96.8, 98.0, 98.1, 1600), (97.2, 98.8, 99.0, 1600),
    ]):
        instant = BASE + timedelta(minutes=30 + minute)
        raw.append({
            "date": day, "time": instant.strftime("%H%M%S"),
            "open": close, "high": hi, "low": lo,
            "close": close, "volume": vol,
        })
    decision = signal(
        rows[:16] + raw, style="ENTRY_MOMENTUM",
        vwap=96.0, vwap_verified=True, trigger_ok=True,
        order_price=98.8,
    )
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "OPENING_RANGE_HIGH_UNVERIFIED_FOR_MOMENTUM_BREAKOUT"
    assert decision["opening_range_complete"] is False
