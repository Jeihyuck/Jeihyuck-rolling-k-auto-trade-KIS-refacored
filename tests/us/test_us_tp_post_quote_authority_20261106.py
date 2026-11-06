"""TP native POST boundary revalidation after a delayed quote GET.

All tests are side-effect-free. The would-be HTTP POST is a local list append.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from trader.us.execution.kis_us_client import KisUSPreSubmitError
from trader.us.execution.order_router import _assert_tp_post_quote_submit_authority


def _tp():
    return {
        "symbol": "HCA", "side": "SELL", "limit_price": 103,
        "client_order_key": "US_PC_HCA_TAKE_PROFIT_TP1",
        "meta": {
            "profit_capture_stage": "tp1",
            "last_profit_capture_at": datetime.now(timezone.utc).isoformat(),
            "broker_avg_price": 100,
            "tp_threshold_fraction": "0.03",
        },
    }


def _quote(age=0.0):
    return {
        "_quote_quality": "FRESH", "_quote_source": "KIS_LIVE",
        "_quote_asof_epoch": time.time() - age,
        "output": {"last": "104"},
    }


def _call(*, quote=None, health=None, authority=None, context=None):
    return _assert_tp_post_quote_submit_authority(
        _tp(), _quote() if quote is None else quote,
        account_env="practice", context=context,
        account_authority=authority, attempt_id="hca-exact-attempt",
        client_order_key="US_PC_HCA_TAKE_PROFIT_TP1",
        strategy_owner="US_STANDARD", symbol="HCA", side="SELL", qty=1,
        max_decision_age_sec=30.0, worker_health_reader=health,
    )


def _ready(**kwargs):
    return {"reconcile_status": "OK", "replay_status": "UNRESOLVED"}


def test_slow_31_second_quote_get_expired_stage4_cannot_reach_post(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    would_post = []
    def expired_stage4(*, env):
        assert env == "practice"
        raise RuntimeError("US_STAGE4_WRITER_STALE")
    with pytest.raises(KisUSPreSubmitError, match="US_STAGE4_WRITER_FENCED_BEFORE_HTTP"):
        _call(health=expired_stage4)
        would_post.append("HTTP_POST")
    assert would_post == []


def test_quote_older_than_five_seconds_cannot_reach_post(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    would_post = []
    with pytest.raises(KisUSPreSubmitError, match="US_TP_LIVE_QUOTE_EXPIRED"):
        _call(quote=_quote(age=31.0), health=_ready)
        would_post.append("HTTP_POST")
    assert would_post == []


def test_reservation_db_delay_cannot_keep_previous_stage4_health(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "ENFORCE")
    calls = []
    class DelayedReservation:
        def assert_reserved_submit_authority(self, **kwargs):
            calls.append("DB_RESERVATION")
            assert kwargs["attempt_id"] == "hca-exact-attempt"
    def stage4(*, env):
        calls.append("STAGE4")
        if calls.count("STAGE4") > 1:
            raise RuntimeError("US_STAGE4_WRITER_STALE")
        return _ready()
    with pytest.raises(KisUSPreSubmitError, match="US_STAGE4_WRITER_FENCED_BEFORE_HTTP"):
        _call(health=stage4, authority=DelayedReservation())
    assert calls == ["STAGE4", "DB_RESERVATION", "STAGE4"]


def test_expired_tick_authority_after_quote_is_pre_http_fence(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    class CancelledTick:
        def broker_submit_allowed(self):
            return False
    with pytest.raises(KisUSPreSubmitError, match="US_TICK_AUTHORITY_EXPIRED_BEFORE_HTTP"):
        _call(context=CancelledTick(), health=_ready)


def test_fresh_authority_preserves_normal_tp_path(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    class ValidTick:
        def broker_submit_allowed(self):
            return True
    assert _call(context=ValidTick(), health=_ready) is None


def test_unknown_replay_still_blocks_post(monkeypatch):
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    with pytest.raises(KisUSPreSubmitError, match="US_STAGE4_WRITER_FENCED_BEFORE_HTTP"):
        _call(health=lambda **kw: {"reconcile_status": "FAILED", "replay_status": "ERROR"})
