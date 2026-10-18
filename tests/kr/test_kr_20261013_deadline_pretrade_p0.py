"""KR Oct-13 2026: watchdog, stage budget and SELL liveness regression.
Offline tests; no KIS calls, no broker submissions.
"""
import inspect
import math
import signal
import sys

import pytest

from trader.pb1_engine import PB1Engine


def test_stage_remaining_never_exceeds_shared_monotonic_tick(monkeypatch):
    import trader.kis_wrapper as wrapper

    monkeypatch.setattr(wrapper, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 1.25)
    engine = object.__new__(PB1Engine)
    engine._tick_remaining_sec = None
    with engine._stage_timer_with_timeout("test.shared-budget", max_sec=50.0):
        pass
    assert 0 < engine._tick_remaining_sec <= 1.25


def test_stage_budget_still_works_outside_trading_tick(monkeypatch):
    import trader.kis_wrapper as wrapper

    monkeypatch.setattr(wrapper, "kr_tick_remaining_sec", lambda *_args, **_kwargs: math.inf)
    engine = object.__new__(PB1Engine)
    with engine._stage_timer_with_timeout("test.no-global-deadline", max_sec=10.0):
        pass
    assert 0 < engine._tick_remaining_sec <= 10


@pytest.mark.skipif(sys.platform != "linux", reason="WSL/POSIX tick watchdog uses SIGALRM")
def test_nested_exit_db_update_does_not_cancel_outer_watchdog():
    # The previous noncritical write used signal.alarm(2) and signal.alarm(0),
    # both of which replace/disarm the outer tick's ITIMER_REAL. Never again.
    source = inspect.getsource(PB1Engine._plan_exit_event)
    assert "signal.alarm(" not in source
    assert "_signal.alarm(" not in source

    from trader.pb1_runner import _run_once_with_hard_timeout
    def check():
        running, _ = signal.getitimer(signal.ITIMER_REAL)
        assert running > 0
        return True
    assert _run_once_with_hard_timeout(timeout_sec=5, call=check) is True
    assert signal.getitimer(signal.ITIMER_REAL)[0] == 0


def test_no_default_full_holdings_prewarm_before_first_protective_sell():
    code = inspect.getsource(PB1Engine.run)
    assert 'os.getenv("KR_EXIT_BLOCKING_PREWARM", "0") == "1"' in code
    assert code.index('os.getenv("KR_EXIT_BLOCKING_PREWARM", "0")') < code.index("self._run_exit_always(")
    assert 'self._fetch_exit_ohlcv(pos["code"])' in inspect.getsource(PB1Engine._run_exit_always)


def test_durable_intent_and_claim_still_precede_kis_sell():
    code = inspect.getsource(PB1Engine._plan_exit_event)
    assert code.index("create_intent_idempotent(") < code.index("_claim_pb1_submit_action(")
    assert code.index("_claim_pb1_submit_action(") < code.index("sell_stock_market(")
    assert "mark_unresolved_ack(" in code
