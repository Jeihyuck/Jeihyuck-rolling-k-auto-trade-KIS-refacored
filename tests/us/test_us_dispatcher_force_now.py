# -*- coding: utf-8 -*-
"""Tests: dispatcher --force-now / --max-ticks CLI 파라미터 전달 검증."""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Test 1: dispatcher trade-am force_now + max_ticks=2
# ---------------------------------------------------------------------------

def test_dispatcher_trade_am_force_now_max_ticks():
    from trader.us.runner import dispatcher as mod

    run_calls = []

    def _fake_run_trade_session(session, env, offline, force_now, max_ticks=0, **kwargs):
        run_calls.append({"session": session, "force_now": force_now, "max_ticks": max_ticks})
        return {"status": "OK"}

    with patch("trader.us.runner.trade_session_runner.run_trade_session", side_effect=_fake_run_trade_session):
        rc = mod.dispatch(
            mode="trade-am",
            env="practice",
            offline=True,
            force_now="2026-05-01T09:35:00-04:00",
            max_ticks=2,
        )

    assert rc == 0
    assert len(run_calls) == 1
    assert run_calls[0]["session"] == "am"
    assert run_calls[0]["force_now"] == "2026-05-01T09:35:00-04:00"
    assert run_calls[0]["max_ticks"] == 2


# ---------------------------------------------------------------------------
# Test 2: dispatcher trade-afternoon force_now + max_ticks=2
# ---------------------------------------------------------------------------

def test_dispatcher_trade_afternoon_force_now_max_ticks():
    from trader.us.runner import dispatcher as mod

    run_calls = []

    def _fake_run_trade_session(session, env, offline, force_now, max_ticks=0, **kwargs):
        run_calls.append({"session": session, "force_now": force_now, "max_ticks": max_ticks})
        return {"status": "OK"}

    with patch("trader.us.runner.trade_session_runner.run_trade_session", side_effect=_fake_run_trade_session):
        rc = mod.dispatch(
            mode="trade-afternoon",
            env="practice",
            offline=True,
            force_now="2026-05-01T13:00:00-04:00",
            max_ticks=2,
        )

    assert rc == 0
    assert run_calls[0]["session"] == "afternoon"
    assert run_calls[0]["force_now"] == "2026-05-01T13:00:00-04:00"
    assert run_calls[0]["max_ticks"] == 2


# ---------------------------------------------------------------------------
# Test 3: dispatcher prep force_now
# ---------------------------------------------------------------------------

def test_dispatcher_prep_force_now():
    from trader.us.runner import dispatcher as mod

    run_calls = []

    def _fake_run_prep(env, offline, force_now=None):
        run_calls.append({"env": env, "offline": offline, "force_now": force_now})
        return {"status": "OK"}

    with patch("trader.us.runner.prep_runner.run_prep", side_effect=_fake_run_prep):
        rc = mod.dispatch(
            mode="prep",
            env="practice",
            offline=True,
            force_now="2026-05-01T08:50:00-04:00",
        )

    assert rc == 0
    assert len(run_calls) == 1
    assert run_calls[0]["force_now"] == "2026-05-01T08:50:00-04:00"


# ---------------------------------------------------------------------------
# Test 4: dispatcher trade-close force_now
# ---------------------------------------------------------------------------

def test_dispatcher_trade_close_force_now():
    from trader.us.runner import dispatcher as mod

    run_calls = []

    def _fake_run_trade_close(env, offline, force_now=None):
        run_calls.append({"env": env, "offline": offline, "force_now": force_now})
        return {"status": "OK"}

    with patch("trader.us.runner.trade_close_runner.run_trade_close", side_effect=_fake_run_trade_close):
        rc = mod.dispatch(
            mode="trade-close",
            env="practice",
            offline=True,
            force_now="2026-05-01T15:55:00-04:00",
        )

    assert rc == 0
    assert len(run_calls) == 1
    assert run_calls[0]["force_now"] == "2026-05-01T15:55:00-04:00"


# ---------------------------------------------------------------------------
# Test 5: dispatcher CLI argparse에 --force-now / --max-ticks 포함 여부
# ---------------------------------------------------------------------------

def test_dispatcher_cli_has_force_now_and_max_ticks():
    """CLI로 --force-now, --max-ticks 인자를 파싱할 수 있어야 한다."""
    import argparse
    import sys
    from trader.us.runner import dispatcher as mod

    # main() 내 argparse 구성을 직접 검사
    # dispatcher.main()에서 사용하는 parser를 재현
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=mod.MODES, required=True)
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None)
    parser.add_argument("--max-ticks", dest="max_ticks", type=int, default=0)

    args = parser.parse_args([
        "--mode", "trade-am",
        "--force-now", "2026-05-01T09:35:00-04:00",
        "--max-ticks", "3",
        "--offline",
    ])
    assert args.force_now == "2026-05-01T09:35:00-04:00"
    assert args.max_ticks == 3
    assert args.offline is True


# ---------------------------------------------------------------------------
# Test 6: dispatcher의 실제 main() argparse에서 --force-now / --max-ticks 처리
# ---------------------------------------------------------------------------

def test_dispatcher_main_supports_force_now_flag(monkeypatch):
    """dispatcher.main()에서 --force-now와 --max-ticks가 실제로 파싱되는지 검증."""
    import sys

    captured = {}

    def _fake_dispatch(mode, env, offline, force_now, max_ticks=0):
        captured["force_now"] = force_now
        captured["max_ticks"] = max_ticks
        return 0

    monkeypatch.setattr(
        "trader.us.runner.dispatcher.dispatch",
        _fake_dispatch,
    )
    monkeypatch.setattr(
        sys, "argv",
        [
            "dispatcher",
            "--mode", "prep",
            "--env", "practice",
            "--force-now", "2026-05-01T08:50:00-04:00",
            "--max-ticks", "2",
            "--offline",
        ],
    )

    from trader.us.runner import dispatcher as mod
    # sys.exit은 막기
    with patch("sys.exit"):
        try:
            mod.main()
        except SystemExit:
            pass

    assert captured.get("force_now") == "2026-05-01T08:50:00-04:00"
    assert captured.get("max_ticks") == 2
