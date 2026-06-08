from __future__ import annotations

from trader.pb1_runner import _update_runtime_fatal_guard


def test_runtime_fatal_guard_stops_on_repeated_identical_exception():
    sig = None
    count = 0

    sig, count, should_stop = _update_runtime_fatal_guard(
        previous_signature=sig,
        previous_count=count,
        exc=RuntimeError("same failure"),
        repeat_threshold=2,
    )
    assert should_stop is False
    assert count == 1

    _sig2, count2, should_stop2 = _update_runtime_fatal_guard(
        previous_signature=sig,
        previous_count=count,
        exc=RuntimeError("same failure"),
        repeat_threshold=2,
    )
    assert count2 == 2
    assert should_stop2 is True


def test_runtime_fatal_guard_resets_for_different_exception_message():
    sig, count, _should_stop = _update_runtime_fatal_guard(
        previous_signature=None,
        previous_count=0,
        exc=RuntimeError("first failure"),
        repeat_threshold=2,
    )

    _sig2, count2, should_stop2 = _update_runtime_fatal_guard(
        previous_signature=sig,
        previous_count=count,
        exc=RuntimeError("second failure"),
        repeat_threshold=2,
    )
    assert count2 == 1
    assert should_stop2 is False