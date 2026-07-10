# -*- coding: utf-8 -*-
"""Tests: dispatcher prep mode uses prep_runner contract-authoritative exit policy."""
from __future__ import annotations


def test_dispatcher_prep_allows_underfilled_contract_trade_ready(monkeypatch):
    """trade_can_proceed=1 must make dispatcher prep succeed even for underfilled warning status."""
    monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)

    import trader.us.runner.prep_runner as pr_mod

    monkeypatch.setattr(
        pr_mod,
        "run_prep",
        lambda **kw: {
            "status": "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE",
            "trade_can_proceed": 1,
            "trade_block_reason": "ok",
        },
    )

    from trader.us.runner.dispatcher import dispatch

    assert dispatch("prep", env="practice", offline=True) == 0


def test_dispatcher_prep_blocks_failed_underfilled_contract(monkeypatch):
    """Failed underfilled prep with trade_can_proceed=0 must remain a dispatcher failure."""
    monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)

    import trader.us.runner.prep_runner as pr_mod

    monkeypatch.setattr(
        pr_mod,
        "run_prep",
        lambda **kw: {
            "status": "FAILED_FINAL30_UNDERFILLED",
            "trade_can_proceed": 0,
            "trade_block_reason": "final30_below_absolute_min",
        },
    )

    from trader.us.runner.dispatcher import dispatch

    assert dispatch("prep", env="practice", offline=True) == 1


def test_dispatcher_prep_preserves_ok_backward_compatibility(monkeypatch):
    """Existing OK status remains successful for backward compatibility."""
    monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)

    import trader.us.runner.prep_runner as pr_mod

    monkeypatch.setattr(
        pr_mod,
        "run_prep",
        lambda **kw: {
            "status": "OK",
            "trade_can_proceed": 0,
        },
    )

    from trader.us.runner.dispatcher import dispatch

    assert dispatch("prep", env="practice", offline=True) == 0
