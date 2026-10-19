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


def test_preintent_budget_defer_protects_durable_created_row():
    from trader.pb1_engine import _kr_pb1_can_create_buy_intent

    assert _kr_pb1_can_create_buy_intent(remaining_sec=float("inf"))
    assert _kr_pb1_can_create_buy_intent(remaining_sec=360)
    assert _kr_pb1_can_create_buy_intent(remaining_sec=45)
    assert not _kr_pb1_can_create_buy_intent(remaining_sec=44.99)
    assert not _kr_pb1_can_create_buy_intent(remaining_sec=20)
    assert not _kr_pb1_can_create_buy_intent(remaining_sec=float("nan"))
    assert not _kr_pb1_can_create_buy_intent(remaining_sec=-float("inf"))
    assert not _kr_pb1_can_create_buy_intent(remaining_sec="unknown")


def test_canonical_pretrade_http_does_not_probe_u_market_or_a_variant(monkeypatch):
    """Exercise the real KIS quote retry loop with stubbed I/O, never a broker."""
    import threading

    class NoWs:
        def subscribe_kr(self, code):
            pass

        def get_fresh_quote(self, market, code, *, max_age_sec):
            return None

    class AllowedGate:
        def wait_until_allowed(self, endpoint, *, max_wait_sec):
            return True

    class NoWaitLimiter:
        def wait(self, endpoint):
            return None

    class NotFoundResponse:
        status_code = 200
        text = "test only"

        def json(self):
            return {"rt_cd": "1", "msg1": "simulated quote miss"}

    monkeypatch.setenv("KIS_WS_INITIAL_WAIT_SEC", "0")
    monkeypatch.setenv("KIS_RETRY_MAX", "5")
    monkeypatch.setattr(kis_wrapper, "get_kis_ws_price_service", lambda: NoWs())
    monkeypatch.setattr(kis_wrapper, "get_kis_gate", lambda: AllowedGate())
    monkeypatch.setattr(kis_wrapper, "_pick_tr", lambda env, key: ["TEST_TR"])
    monkeypatch.setattr(kis_wrapper, "_kr_sleep_with_budget", lambda *a, **k: True)
    kis_wrapper.price_cache.cache.pop(("inquire-price", "096530"), None)

    api = object.__new__(kis_wrapper.KisAPI)
    api.env = "practice"
    api._concurrency_sem = threading.Semaphore(1)
    api._limiter = NoWaitLimiter()
    api._headers = lambda tr: {}
    api._log_kis_resp = lambda *a, **k: None
    calls = []

    def _stub_request(method, url, **kwargs):
        calls.append(dict(kwargs.get("params") or {}))
        return NotFoundResponse()

    api._safe_request = _stub_request

    import pytest
    with pytest.raises(RuntimeError, match="get_price_quote failed"):
        api.get_price_quote("096530", retry_cap=2)

    assert len(calls) == 2
    assert all(row["fid_cond_mrkt_div_code"] == "J" for row in calls)
    assert all(row["fid_input_iscd"] == "096530" for row in calls)


def test_source_time_freshness_is_still_rejected_when_quote_is_old():
    stale = {"prpr": 10000.0, "_source_ts": time.time() - 12}
    checked, reason = freshness._validate_source_freshness(
        stale, max_age_sec=5.0, source_kind="KIS_REST"
    )
    assert checked is None
    assert reason.startswith("quote_stale:")


def test_proven_prebroker_abort_closes_created_order_without_submitting():
    from trader.pb1_engine import _close_pb1_prebroker_buy_intent

    calls = []

    class FakeOrdersRepo:
        def mark_error(self, env, key, payload):
            calls.append((env, key, payload))

    assert _close_pb1_prebroker_buy_intent(
        FakeOrdersRepo(), env="practice", client_order_key="pb1:test:1",
        code="096530", reason="INTENT_LEDGER_ABORT"
    )
    assert len(calls) == 1
    env, key, payload = calls[0]
    assert env == "practice" and key == "pb1:test:1"
    assert payload["broker_submit"] is False
    assert payload["msg_cd"] == "INTENT_LEDGER_ABORT"


def test_failed_prebroker_db_cleanup_is_not_falsely_reported_as_reconciled():
    from trader.pb1_engine import _close_pb1_prebroker_buy_intent

    class FailedOrdersRepo:
        def mark_error(self, *args, **kwargs):
            raise TimeoutError("database unavailable")

    assert not _close_pb1_prebroker_buy_intent(
        FailedOrdersRepo(), env="practice", client_order_key="pb1:test:1",
        code="096530", reason="INTENT_LEDGER_ABORT"
    )
    assert not _close_pb1_prebroker_buy_intent(
        FailedOrdersRepo(), env="practice", client_order_key="",
        code="096530", reason="INTENT_LEDGER_ABORT"
    )


def test_real_kr_loop_allows_240s_candidate_before_360s_deadline_without_overlap(monkeypatch, tmp_path):
    """Offline session-runner replay: at 180s the 240s candidate would expire."""
    from datetime import datetime, timedelta
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    from trader import pb1_runner as runner

    market_start = datetime(2026, 10, 19, 13, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    clock = {"elapsed": 0.0}
    started_at = []
    timeout_values = []

    def _sleep(seconds):
        assert seconds >= 0
        clock["elapsed"] += seconds

    monkeypatch.setattr(
        runner, "time_mod",
        SimpleNamespace(monotonic=lambda: clock["elapsed"], sleep=_sleep),
    )
    monkeypatch.setattr(
        runner, "_get_now_kst", lambda: market_start + timedelta(seconds=clock["elapsed"])
    )
    monkeypatch.setattr(runner, "_resolve_session_kind", lambda: "afternoon")
    monkeypatch.setattr(
        runner, "_resolve_session_end_dt", lambda _: market_start + timedelta(seconds=400)
    )
    monkeypatch.setattr(runner, "_resolve_loop_limits", lambda: (10, 10, 400, False))
    monkeypatch.setattr(runner, "make_engine", lambda: MagicMock())
    monkeypatch.setattr(runner, "RunsRepo", lambda _: MagicMock())
    monkeypatch.setattr(runner, "try_acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "_acquire_session_guard_or_takeover", lambda **k: {})
    monkeypatch.setattr(runner, "runtime_root", lambda: tmp_path)
    monkeypatch.setattr(runner, "decide_window", lambda **k: object())
    monkeypatch.setattr(runner, "_run_nontrading_smoke_if_needed", lambda **k: False)
    monkeypatch.setattr(runner.signal, "signal", lambda *a, **k: None)
    for name in (
        "_touch_session_guard", "_ensure_bootstrap_migrations", "_write_change_flag",
        "_finish_session_guard", "release_advisory_xact_lock", "dispose_engine_safely",
        "_record_session_execution_marker", "_write_session_result_file",
    ):
        monkeypatch.setattr(runner, name, lambda *a, **k: None)

    def _watchdog(*, timeout_sec, call):
        timeout_values.append(timeout_sec)
        return call()

    def _offline_candidate(**kwargs):
        started_at.append(clock["elapsed"])
        assert kwargs["max_seconds"] == 360
        clock["elapsed"] += 240
        return [], True, {
            "api_submitted": 1, "buy_orders": 1, "buy_orders_ack": 1,
            "order_candidates": 1, "accepted": 1,
        }, "entry", "OK"

    monkeypatch.setattr(runner, "_run_once_with_hard_timeout", _watchdog)
    monkeypatch.setattr(runner, "run_once", _offline_candidate)
    monkeypatch.setenv("PB1_CANDIDATE_ONLY", "1")
    monkeypatch.setenv("PB1_LOOP_INTERVAL_SEC", "60")
    monkeypatch.setenv("PB1_TICK_HARD_TIMEOUT_SEC", "360")
    monkeypatch.setenv("PB1_MIN_TICK_BUDGET_SEC", "380")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")

    runner._run_loop(
        args=SimpleNamespace(window="day", env="practice", strategy_mode="LIVE")
    )

    assert started_at == [0.0]
    assert timeout_values == [360]
    assert clock["elapsed"] >= 240
