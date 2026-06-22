import time

from trader.prep_runner import _resolve_prep_final_status, _run_prep_aux_with_timeout


def test_prep_aux_timeout_continues_degraded_when_core_ok():
    def slow():
        time.sleep(0.3)
        return "done"

    ok, result, reason = _run_prep_aux_with_timeout(
        stage="unit_slow_aux",
        timeout_sec=0.05,
        core_ok=True,
        fn=slow,
        default=None,
    )

    assert ok is False
    assert result is None
    assert reason == "timeout"


def test_prep_aux_success_returns_result():
    ok, result, reason = _run_prep_aux_with_timeout(
        stage="unit_fast_aux",
        timeout_sec=1,
        core_ok=True,
        fn=lambda: "ok",
        default=None,
    )

    assert ok is True
    assert result == "ok"
    assert reason is None


def test_prep_final_status_core_aux_degraded_is_zero_exit():
    status, exit_code = _resolve_prep_final_status(
        prep_core={"core_ok": 1, "reasons": []},
        aux_failures=[{"stage": "watchlist_load_verify", "reason": "timeout"}],
    )
    assert status == "OK_CORE_AUX_DEGRADED"
    assert exit_code == 0
