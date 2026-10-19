"""Protective SELL precedes KR_INFINITE; unrelated PB1 entry follows it.

This checks the actual shared runner/engine wiring, not KR_INFINITE trade
signals, size, strategy capital or risk policy.
"""
import inspect

from trader import pb1_runner
from trader.pb1_engine import PB1Engine


def test_kr_infinite_stays_after_protection_but_before_unrelated_entry():
    runner = inspect.getsource(pb1_runner.run_once)
    engine = inspect.getsource(PB1Engine.run)
    assert engine.index('"_kr_after_protective_exit"') > engine.index(
        '"post_capital.exit_pass"'
    )
    assert engine.index("_after_exit()") < engine.index(
        '"[PB1][POST_CAPITAL][OPEN_ORDERS_LOOKUP]"'
    )
    assert runner.index("run_kr_infinite_sleeve_tick(") < runner.index(
        "result = engine_runner.run()"
    )
    assert runner.count("run_kr_infinite_sleeve_tick(") == 1
    assert "if kr_inf_invoked:" in runner
    assert "kr_inf_invoked = True" in runner


def test_kr_infinite_uses_existing_owner_shared_snapshot_and_entry_guard():
    runner = inspect.getsource(pb1_runner.run_once)
    snippet = runner[runner.index("def _kr_after_protective_exit"):runner.index(
        "engine_runner._kr_after_protective_exit"
    )]
    assert "balance_snapshot=balance_snapshot_raw or {}" in snippet
    assert "order_allowed and calc_allowed" in snippet
    assert 'getattr(engine_runner, "_kr_exit_pass_ok", False)' in snippet
    assert "self._kr_exit_pass_ok = bool(exit_pass_ok)" in inspect.getsource(PB1Engine.run)
    assert "kr_inf_run_result = run_kr_infinite_sleeve_tick" in snippet
    assert "shared_balance=1" in snippet
    assert "[KR_INF][NOT_RUN]" in runner
