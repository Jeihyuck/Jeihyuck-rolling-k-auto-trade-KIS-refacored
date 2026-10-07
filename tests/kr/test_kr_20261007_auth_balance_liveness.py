from __future__ import annotations

import inspect

import pytest

from trader import kis_wrapper
from trader.kis_wrapper import KisAuthError, KisTokenRateLimitError
from trader.kr.runner import trade_session_runner as runner


def test_token_403_egw00133_is_transient_rate_limit_not_generic_auth():
    limited, detail = kis_wrapper._token_rate_limit_response(
        403,
        {
            "error_code": "EGW00133",
            "error_description": "접근토큰 발급 잠시 후 다시 시도하세요(1분당 1회)",
        },
    )
    assert limited is True
    assert "EGW00133" in detail
    source = inspect.getsource(kis_wrapper.KisAPI._safe_request)
    assert "_token_rate_limit_response" in source
    assert "raise KisTokenRateLimitError" in source


def test_generic_token_403_remains_hard_auth_failure():
    limited, detail = kis_wrapper._token_rate_limit_response(
        403,
        {"error_code": "INVALID_APPKEY", "error_description": "invalid credential"},
    )
    assert limited is False
    assert "INVALID_APPKEY" in detail


def test_am_balance_precheck_maps_token_rate_limit_into_fail_closed_recovery(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setenv("KR_TRADE_DATE", "2026-10-08")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr(runner, "_LAST_GOOD_BALANCE", None)

    class RateLimitedKis:
        def __init__(self):
            raise KisTokenRateLimitError(
                "KIS_TOKEN_RATE_LIMIT EGW00133", retry_after=65
            )

    monkeypatch.setattr(runner, "KisAPI", RateLimitedKis)
    result = runner._assert_balance_available("am")

    assert result is not None
    assert result["status"] == "WARN"
    assert result["reason"] == "KIS_TOKEN_RATE_LIMIT"
    assert result["entry_allowed"] == 0
    assert result["order_allowed"] == 0
    assert result["exit_allowed"] == 0


def test_am_balance_recovery_requires_fresh_success_before_resume(monkeypatch):
    attempts = iter([
        {"status": "WARN", "reason": "KIS_TOKEN_RATE_LIMIT", "exit_allowed": 0},
        None,
    ])
    sleeps: list[int] = []
    monkeypatch.setenv("KR_BALANCE_RECOVERY_INTERVAL_SEC", "1")

    result = runner.recover_temporary_balance(
        "am",
        {"status": "WARN", "reason": "KIS_TOKEN_RATE_LIMIT", "exit_allowed": 0},
        probe=lambda: next(attempts),
        sleep_fn=sleeps.append,
        max_attempts=2,
    )

    assert result is None
    assert sleeps == [1, 1]
    assert runner.os.environ["ENTRY_ALLOWED"] == "1"
    assert runner.os.environ["ORDER_ALLOWED"] == "1"
    assert runner.os.environ["EXIT_ALLOWED"] == "1"


def test_generic_auth_error_is_not_downgraded_to_balance_timeout(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setenv("KR_TRADE_DATE", "2026-10-08")

    class BadCredentialKis:
        def __init__(self):
            raise KisAuthError("HTTP 403 invalid credential")

    monkeypatch.setattr(runner, "KisAPI", BadCredentialKis)
    with pytest.raises(KisAuthError, match="invalid credential"):
        runner._assert_balance_available("am")
