"""Proof that failed/over-budget ticks cannot disappear from latency p95."""
import inspect

from trader import pb1_runner


def test_full_tick_latency_trace_is_in_finally_not_success_only():
    source = inspect.getsource(pb1_runner)
    start = source.index('os.environ["KR_TICK_ORIGIN_MONOTONIC"] =')
    end = source.index('last_tick_metrics = dict(metrics or {})', start)
    segment = source[start:end]
    assert '_tick_result_status_for_latency = "TIMEOUT_OR_FAILURE"' in segment
    assert 'finally:' in segment
    assert segment.index('finally:') < segment.index('[KR_LATENCY][TICK_END]')
    assert segment.index('[KR_LATENCY][TICK_END]') < segment.index("clear_kr_tick_deadline()")
    assert 'str(os.getenv("GITHUB_SHA") or "UNVERSIONED")' in segment
    assert 'str(os.getenv("TRADER_RUN_ID") or "NO_RUN_ID")' in segment
    assert '[KR_LATENCY][TICK_TARGET_MISS]' in segment


def test_execution_failure_never_inherits_previous_tick_success_status():
    source = inspect.getsource(pb1_runner)
    start = source.index('os.environ["KR_TICK_ORIGIN_MONOTONIC"] =')
    end = source.index('last_tick_metrics = dict(metrics or {})', start)
    segment = source[start:end]
    assert segment.index('_tick_result_status_for_latency = "TIMEOUT_OR_FAILURE"') < segment.index("_run_once_with_hard_timeout(")
    assert segment.index("_tick_result_status_for_latency = str(result_status or") < segment.index("finally:")
