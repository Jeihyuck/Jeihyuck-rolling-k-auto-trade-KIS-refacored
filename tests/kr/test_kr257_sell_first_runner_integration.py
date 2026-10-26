"""Execute the *real* PB1 runner post-exit callback through its guarded branch.

Only the callback's post-SELL reconciliation segment is isolated via AST:
there is no duplicate implementation of its conditional flow. This directly
regresses the unreachable `sell_only_lane=True / pre_exit_exact_proof=False`
case flagged by the PR257 P1 review. Broker and DB are offline fakes.
"""
from __future__ import annotations

import ast
import copy
import inspect
import logging
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import textwrap
import time

import pytest

from trader import pb1_runner
import trader.kr.protective_sell_handoff as handoff


KST = ZoneInfo("Asia/Seoul")


@lru_cache(maxsize=1)
def _runner_post_exit_callback_code():
    original = ast.parse(textwrap.dedent(inspect.getsource(pb1_runner.run_once)))
    callback = next(
        node for node in ast.walk(original)
        if isinstance(node, ast.FunctionDef)
        and node.name == "_kr_after_protective_exit"
    )
    cb = copy.deepcopy(callback)
    # No KIS/DB entry/scan side effects: only execute the true runner
    # callback through the guarded post-exit FULL/skip branch.
    stop_at = next(
        i for i, stmt in enumerate(cb.body)
        if isinstance(stmt, ast.If)
        and isinstance(stmt.test, ast.Name)
        and stmt.test.id == "deferred_full_reconcile"
    )
    cb.body = cb.body[:stop_at+1]
    outer_init = ast.parse("""
kr_inf_run_result = None
kr_inf_invoked = False
reconcile_result = None
balance_snapshot_raw = _initial_balance
""").body
    outer_done = ast.parse("""
_kr_after_protective_exit()
return reconcile_result
""").body
    outer = ast.FunctionDef(
        name="_test_real_runner_exit_fragment",
        args=ast.arguments(posonlyargs=[], args=[], vararg=None,
                           kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]),
        body=outer_init + [cb] + outer_done,
        decorator_list=[],
    )
    module = ast.fix_missing_locations(
        ast.Module(body=[outer], type_ignores=[])
    )
    return compile(module, "pb1_runner.py::actual_post_exit_callback", "exec")


class FakeKis:
    def __init__(self, balance):
        self.balance = balance
        self.refreshes = 0

    def get_balance_cached(self, *, force):
        assert force is True
        self.refreshes += 1
        return copy.deepcopy(self.balance)


def _account_balance(quantity=12, leverage_quantity=7):
    return {
        "output1": [
            {"pdno": "053800", "hldg_qty": str(quantity)},
            {"pdno": "122630", "hldg_qty": str(leverage_quantity)},
        ],
        "output2": [{"tot_evlu_amt": "2000000"}],
    }


def _call_real_runner_fragment(
    monkeypatch, *, post_exact=True, pre_exact=False, sell_only=True,
    post_balance=None, pre_balance=None, attempt=0, blocked=0,
    reset=False, exit_ok=True,
):
    before = pre_balance or _account_balance()
    after = post_balance or _account_balance()
    kis = FakeKis(after)
    full_calls = []
    summaries = dict(
        holdings=1, checked=1, submit_attempt_count=attempt,
        accepted_sell_count=0, fill_confirmed_sell_count=0,
        submitted=0, blocked_count=blocked,
    )
    runner = SimpleNamespace(
        _kr_exit_pass_ok=exit_ok,
        _exit_summary_payload=summaries,
        _authoritative_balance=None,
        _kr_post_exit_reconcile_ok=True,
    )
    evidence_calls = []

    def prove(**kw):
        evidence_calls.append(kw)
        return bool(post_exact), ("EXACT" if post_exact else "INCOMPLETE")

    monkeypatch.setattr(handoff, "prove_safe_protective_sell_handoff", prove)
    monkeypatch.setattr("trader.account_state.account_reset_mode", lambda: reset)

    def full_reconcile(**kw):
        full_calls.append(kw)
        return {"ok": True, "status": "FULL_DONE"}

    class BalanceRepresentation:
        @staticmethod
        def from_kis(snapshot, *, source):
            assert source == "api"
            return snapshot

    context = dict(
        _initial_balance=before,
        kis=kis, engine=object(), engine_runner=runner,
        deferred_full_reconcile=True,
        pre_exit_exact_proof=pre_exact, sell_only_lane=sell_only,
        now_kst=lambda: datetime(2026, 10, 26, 9, 46, tzinfo=KST),
        env_effective="practice", trade_date=datetime(2026, 10, 26).date(),
        time_mod=time, logger=logging.getLogger(__name__),
        reconcile_kis=full_reconcile,
        BrokerBalanceSnapshot=BalanceRepresentation,
        db_write_reasons=[], run_record_id="test-run",
        now=datetime(2026, 10, 26, 9, 46, tzinfo=KST),
        runtime_root_dir=Path("/tmp"),
    )
    exec(_runner_post_exit_callback_code(), context)
    result = context["_test_real_runner_exit_fragment"]()
    return result, full_calls, evidence_calls, kis, runner, context["db_write_reasons"]


def test_actual_runner_narrow_preflight_no_sell_skips_full_after_exact_broker_proof(monkeypatch):
    result, calls, proofs, kis, runner, writes = _call_real_runner_fragment(monkeypatch)
    assert not calls, "Normal narrow-preflight no-SELL path must not run redundant FULL"
    assert len(proofs) == 1
    assert kis.refreshes == 1
    assert runner._kr_post_exit_reconcile_ok is True
    assert result["status"] == "PRE_SELL_SAFE_POST_SELL_EXACT_NO_SELL"
    assert result["historical_repair_fastpath"] is True
    assert "two_sided_exact_proof_skip_historical_reconcile" in writes


@pytest.mark.parametrize("reason,change", [
    ("BROKER_PROOF_MISSING", {"post_exact": False}),
    ("HOLDINGS_CHANGED", {"post_balance": _account_balance(quantity=11)}),
    ("FOREIGN_OWNER_CHANGED", {"post_balance": _account_balance(leverage_quantity=6)}),
    ("SELL_ATTEMPTED", {"attempt": 1}),
    ("SELL_BLOCKED", {"blocked": 1}),
    ("ACCOUNT_RESET", {"reset": True}),
    ("EXIT_PASS_INCOMPLETE", {"exit_ok": False}),
])
def test_actual_runner_fails_closed_to_full_reconcile(monkeypatch, reason, change):
    result, calls, proofs, kis, runner, _ = _call_real_runner_fragment(monkeypatch, **change)
    assert len(calls) == 1, reason
    if reason in {"SELL_ATTEMPTED", "SELL_BLOCKED", "EXIT_PASS_INCOMPLETE"}:
        assert len(proofs) == 0, "No wasted post-exit exact KIS read before FULL"
        if reason != "SELL_ATTEMPTED":
            assert kis.refreshes == 0
    assert result["status"] == "FULL_DONE"
    assert runner._kr_post_exit_reconcile_ok is True


def test_actual_runner_never_skips_with_no_pre_exit_authorization(monkeypatch):
    # An unauthorized/no proof path cannot make a BUY-ready shortcut.
    result, calls, proofs, kis, _, _ = _call_real_runner_fragment(
        monkeypatch, pre_exact=False, sell_only=False,
    )
    assert len(calls) == 1
    assert len(proofs) == 0
    assert kis.refreshes == 0
    assert result["status"] == "FULL_DONE"
