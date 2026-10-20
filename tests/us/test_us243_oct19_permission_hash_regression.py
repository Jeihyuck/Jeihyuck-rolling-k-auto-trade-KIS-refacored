"""10/19 KIS-order PRE_IO incident exact production SHA256 fingerprint.

2026-10-19 Supabase: 86 US_STANDARD META/TMO PRE_IO_ABORT events, 0 broker
order numbers. All share raw_response_hash below. It is generated from
json.dumps({"error": string}, sort_keys=True, ensure_ascii=False, default=str).
The original string is reconstructible from current KIS config guard,
not from invented real fills. Never auto-enable DISABLE_REAL_TRADING.
"""
from __future__ import annotations

import hashlib
import json

from trader.us.execution.order_permissions import resolve_us_order_permissions


OBSERVED_PRE_IO_ERROR_SHA256 = (
    "fcee13aa7697b31459ba1a95f886b8d38896cea4f7676a9226fc15353d238290"
)


def _fingerprint(msg: str) -> str:
    raw = json.dumps({"error": msg},sort_keys=True,ensure_ascii=False,default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def test_original_oct19_abort_sha256_identifies_disabled_real_trading_guard():
    # This is an exact evidence match, not a likely-error inference.
    msg = (
        "SELL order setup failed before broker HTTP: "
        "[US_ORDER][BLOCKED] reason=disable_real_trading_enabled "
        "required=DISABLE_REAL_TRADING=0"
    )
    assert _fingerprint(msg) == OBSERVED_PRE_IO_ERROR_SHA256


def test_session_and_native_broker_permissions_agree_that_disabled_means_disabled(
    monkeypatch,
):
    import trader.us.config as cfg

    active_env = {
        "DRY_RUN":"0", "DISABLE_LIVE_TRADING":"0",
        "DISABLE_REAL_TRADING":"1", "LIVE_TRADING_ENABLED":"1",
        "US_LIVE_TRADING_ENABLED":"1", "US_ORDER_ARMED":"1",
        "US_PAPER_TRADING_ENABLED":"1",
    }
    for session in ("am", "afternoon"):
        p=resolve_us_order_permissions(session,"practice","TRADE",active_env)
        assert not p.allowed
        assert "disable_real_trading_enabled" in p.reasons

    # Assert the corresponding unmodified last-line KIS guard is still
    # fail-closed. Never disable this guard to achieve a green test.
    for name,value in {
        "TRADING_REGION":"US","KIS_ENV":"practice",
        "US_PAPER_TRADING_ENABLED":True,"US_LIVE_TRADING_ENABLED":True,
        "DISABLE_REAL_TRADING":True,"DRY_RUN":False,
    }.items():
        monkeypatch.setattr(cfg,name,value)
    import pytest
    with pytest.raises(RuntimeError,match="disable_real_trading_enabled"):
        cfg.assert_us_paper_order_allowed()


def test_allowed_practice_profile_requires_explicit_operator_disarm_clear():
    env={
        "DRY_RUN":"0", "DISABLE_LIVE_TRADING":"0",
        "DISABLE_REAL_TRADING":"0", "LIVE_TRADING_ENABLED":"1",
        "US_LIVE_TRADING_ENABLED":"1", "US_ORDER_ARMED":"1",
    }
    for session in ("am", "afternoon"):
        p=resolve_us_order_permissions(session,"practice","TRADE",env)
        assert p.allowed
    env["DISABLE_REAL_TRADING"]="1"
    assert not resolve_us_order_permissions("am","practice","TRADE",env).allowed
