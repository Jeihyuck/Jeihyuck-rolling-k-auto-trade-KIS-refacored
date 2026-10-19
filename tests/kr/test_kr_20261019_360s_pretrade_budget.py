"""KR Oct-19 PB1 liveness regressions: longer tick, bounded quote retries.

No broker HTTP calls, no live trading, and no relaxation of the KIS freshness
or duplicate-order gates.
"""
from __future__ import annotations

import time
from pathlib import Path

from trader import kis_wrapper
from trader.kr import runtime_integrity_20260930_freshness_review as freshness
from trader.pb1_runner import _resolve_kr_shared_tick_budget


def test_regular_quote_retry_behavior_is_unchanged():
    assert kis_wrapper._bounded_quote_attempt_count(2, 5) == 5
    assert kis_wrapper._bounded_quote_attempt_count(6, 5) == 6


def test_pb1_pretrade_caps_only_outer_quote_retry_attempts():
    assert kis_wrapper._bounded_quote_attempt_count(2, 5, cap=2) == 2
    assert kis_wrapper._bounded_quote_attempt_count(2, 5, cap=1) == 1
    assert kis_wrapper._bounded_quote_attempt_count(2, 5, cap=0) == 1


def test_360_second_watchdog_and_session_grace_share_one_deadline():
    assert _resolve_kr_shared_tick_budget(
        remaining_to_session_end=1000, base_tick_timeout=360, grace_sec=15
    ) == 360
    assert _resolve_kr_shared_tick_budget(
        remaining_to_session_end=250, base_tick_timeout=360, grace_sec=15
    ) == 235
    assert _resolve_kr_shared_tick_budget(
        remaining_to_session_end=12, base_tick_timeout=360, grace_sec=15
    ) == 0


def test_strict_pb1_rest_fallback_uses_bounded_retry_without_weakening_freshness(monkeypatch):
    calls = []

    class ControlledKis:
        def get_price_quote(self, code, *, diag_mode=False, attempts=2, retry_cap=None):
            calls.append((code, diag_mode, attempts, retry_cap))
            return {"prpr": 10000.0}

    # Constrain the adapter identity to test only the KIS-path argument contract.
    monkeypatch.setattr(freshness.kis_wrapper_module, "KisAPI", ControlledKis)
    monkeypatch.setattr(freshness, "_evict_outer_cache", lambda *a, **kw: None)
    monkeypatch.setattr(freshness, "_evict_inner_cache_if_not_fresh", lambda *a, **kw: None)
    monkeypatch.setattr(freshness, "_evict_ws_cache_if_not_fresh", lambda *a, **kw: False)
    monkeypatch.setattr(freshness, "_inner_cache_source_ts", lambda code: time.time())
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_RETRY_CAP", "2")

    quote, reason = freshness._strict_rest_quote(
        ControlledKis(), "096530", max_age_sec=5.0
    )

    assert reason == "ok"
    assert quote is not None and quote["_source_kind"] == "KIS_REST"
    assert calls == [("096530", False, 2, 2)]


def test_legacy_quote_adapter_is_not_forced_to_accept_new_keyword(monkeypatch):
    calls = []

    class LegacyAdapter:
        def get_price_quote(self, code, *, diag_mode=False, attempts=2):
            calls.append((code, attempts))
            return {"prpr": 10000.0}

    class ActualKisMarker:
        pass

    monkeypatch.setattr(freshness.kis_wrapper_module, "KisAPI", ActualKisMarker)
    monkeypatch.setattr(freshness, "_evict_outer_cache", lambda *a, **kw: None)
    monkeypatch.setattr(freshness, "_evict_inner_cache_if_not_fresh", lambda *a, **kw: None)
    monkeypatch.setattr(freshness, "_evict_ws_cache_if_not_fresh", lambda *a, **kw: False)
    monkeypatch.setattr(freshness, "_inner_cache_source_ts", lambda code: time.time())

    quote, reason = freshness._strict_rest_quote(
        LegacyAdapter(), "192820", max_age_sec=5.0
    )

    assert reason == "ok"
    assert quote is not None
    assert calls == [("192820", 2)]


def test_wsl_am_and_pm_defaults_use_360_and_380_budgets():
    root = Path(__file__).resolve().parents[2]
    for script in ("run-kr-am.sh", "run-kr-afternoon.sh"):
        source = (root / "scripts" / "wsl" / script).read_text(encoding="utf-8")
        assert 'PB1_TICK_HARD_TIMEOUT_SEC="${PB1_TICK_HARD_TIMEOUT_SEC:-360}"' in source
        assert 'PB1_MIN_TICK_BUDGET_SEC="${PB1_MIN_TICK_BUDGET_SEC:-380}"' in source
