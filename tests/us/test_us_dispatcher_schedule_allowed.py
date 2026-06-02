# -*- coding: utf-8 -*-
"""schedule 이벤트에서 dispatcher가 즉시 return 1 하지 않는지 확인."""
from __future__ import annotations

import importlib
import os

import pytest


def test_dispatcher_allows_schedule_event(monkeypatch):
    """GITHUB_EVENT_NAME=schedule 시 dispatcher가 차단하지 않아야 한다."""
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")

    # prep_runner.run_prep를 mock
    import trader.us.runner.prep_runner as pr_mod
    monkeypatch.setattr(pr_mod, "run_prep", lambda **kw: {"status": "OK"})

    from trader.us.runner.dispatcher import dispatch
    rc = dispatch("prep", env="practice", offline=True)
    assert rc == 0, f"schedule 이벤트에서 dispatch가 {rc}를 반환했다 (0이어야 함)"


def test_dispatcher_schedule_allowed_log(monkeypatch, caplog):
    """schedule 이벤트 시 SCHEDULE_ALLOWED 로그가 찍혀야 한다."""
    import logging
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")

    import trader.us.runner.prep_runner as pr_mod
    monkeypatch.setattr(pr_mod, "run_prep", lambda **kw: {"status": "OK"})

    with caplog.at_level(logging.INFO, logger="trader.us.runner.dispatcher"):
        from trader.us.runner.dispatcher import dispatch
        dispatch("prep", env="practice", offline=True)

    assert any("SCHEDULE_ALLOWED" in r.message for r in caplog.records), (
        "SCHEDULE_ALLOWED 로그가 없다"
    )
    assert not any("auto_schedule_not_allowed" in r.message for r in caplog.records), (
        "auto_schedule_not_allowed 로그가 있으면 안 된다"
    )


def test_dispatcher_no_auto_schedule_not_allowed_string():
    """dispatcher.py 소스에 auto_schedule_not_allowed 문자열이 없어야 한다."""
    import trader.us.runner.dispatcher as mod
    import inspect
    src = inspect.getsource(mod)
    assert "auto_schedule_not_allowed" not in src
    assert "dispatcher_is_manual_only_use_dedicated_workflow" not in src


def test_dispatcher_manual_allowed_log(monkeypatch, caplog):
    """workflow_dispatch 이벤트 시 MANUAL_ALLOWED 로그가 찍혀야 한다."""
    import logging
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")

    import trader.us.runner.prep_runner as pr_mod
    monkeypatch.setattr(pr_mod, "run_prep", lambda **kw: {"status": "OK"})

    with caplog.at_level(logging.INFO, logger="trader.us.runner.dispatcher"):
        from trader.us.runner.dispatcher import dispatch
        dispatch("prep", env="practice", offline=True)

    assert any("MANUAL_ALLOWED" in r.message for r in caplog.records)


def test_dispatcher_unsupported_event_blocked(monkeypatch):
    """지원하지 않는 이벤트는 차단되어야 한다."""
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")

    from trader.us.runner.dispatcher import dispatch
    rc = dispatch("prep", env="practice", offline=True)
    assert rc == 1, "지원하지 않는 이벤트는 rc=1이어야 한다"
