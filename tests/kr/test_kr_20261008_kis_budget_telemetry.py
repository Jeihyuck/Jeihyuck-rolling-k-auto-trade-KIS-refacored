"""October 8 PB1 AM/PM KIS rate limiter and balance-budget exhaustion tests."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

import trader.kis_wrapper as kis


def test_rate_limiter_uses_elapsed_monotonic_time_not_wall_clock(monkeypatch):
    clock = {"monotonic": 100.0, "wall": 1000.0}
    monkeypatch.setattr(
        kis, "time", SimpleNamespace(
            monotonic=lambda: clock["monotonic"],
            time=lambda: clock["wall"],
            sleep=lambda s: None,
        ),
    )
    sleeps = []
    monkeypatch.setattr(kis, "_kr_sleep_with_budget", lambda s: sleeps.append(s) or True)
    monkeypatch.setattr(kis.random, "uniform", lambda _a, _b: 0.0)
    limiter = kis._RateLimiter(min_interval_sec=1.0)
    limiter.wait("KR_ORDER")
    assert limiter.last_at["KR_ORDER"] == 100.0

    # NTP correction goes backwards, but elapsed time must not.
    clock["wall"] = -100_000.0
    clock["monotonic"] = 100.25
    limiter.wait("KR_ORDER")
    assert sleeps == [pytest.approx(0.75)]
    assert limiter.last_at["KR_ORDER"] == 100.25


def test_rate_limiter_fails_closed_without_advancing_submission_slot(monkeypatch):
    clock = {"t": 100.0}
    monkeypatch.setattr(
        kis, "time",
        SimpleNamespace(monotonic=lambda: clock["t"], time=lambda: 0.0),
    )
    monkeypatch.setattr(kis, "_kr_sleep_with_budget", lambda _s: False)
    monkeypatch.setattr(kis.random, "uniform", lambda _a, _b: 0.0)
    limiter = kis._RateLimiter(min_interval_sec=1.0)
    limiter.wait("KR_ORDER")
    clock["t"] = 100.1
    with pytest.raises(kis.KisTemporaryError, match="KR_TICK_DEADLINE_EXHAUSTED_DURING_RATE_LIMIT"):
        limiter.wait("KR_ORDER")
    assert limiter.last_at["KR_ORDER"] == 100.0


def test_balance_pre_fetch_budget_exhaustion_never_calls_broker(monkeypatch, caplog):
    api = kis.KisAPI.__new__(kis.KisAPI)
    api._balance_cache = None
    api._kr_stage_deadline = None
    monkeypatch.setattr(kis, "kr_tick_remaining_sec", lambda *_args: 1.0)
    monkeypatch.setattr(kis.KisAPI, "inquire_balance_all", lambda self: pytest.fail("unexpected broker call"))
    monkeypatch.delenv("KIS_FORCE_500_BALANCE", raising=False)
    with pytest.raises(kis.KisBalanceUnavailable, match="KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH"):
        api.get_balance_cached(force=True)
    assert "stage=balance_pre_fetch" in caplog.text
    assert "action=FAIL_CLOSED" in caplog.text
