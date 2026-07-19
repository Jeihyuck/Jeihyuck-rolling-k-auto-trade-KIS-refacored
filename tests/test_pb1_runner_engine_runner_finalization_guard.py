from __future__ import annotations

import builtins

import pytest


def _session_finalization_marker_metrics_lookup():
    # Equivalent to pb1_runner finalization after Python name resolution falls
    # back to builtins.engine_runner.
    engine_runner = builtins.engine_runner
    return getattr(engine_runner, "_run_summary_payload", {}) or getattr(
        engine_runner, "_debug_summary", {}
    )


def test_engine_runner_guard_tracks_real_engine_summary():
    import trader

    class DummyEngine:
        _run_summary_payload = {"order_candidates": 2, "api_submitted": 1}
        _debug_summary = {"fallback": True}

        def run(self):
            return {"ok": True}

    assert trader._wrap_pb1_engine_class(DummyEngine) is True
    engine = DummyEngine()
    assert engine.run() == {"ok": True}

    assert builtins.engine_runner is engine
    assert _session_finalization_marker_metrics_lookup() == {
        "order_candidates": 2,
        "api_submitted": 1,
    }


def test_engine_runner_guard_survives_engine_exception_and_keeps_summary():
    import trader

    class FailingEngine:
        _run_summary_payload = {"order_candidates": 0, "api_submitted": 0, "skipped": 3}
        _debug_summary = {"skip_reasons": ["EXHAUSTED_CANDIDATES"]}

        def run(self):
            raise RuntimeError("simulated tick failure")

    assert trader._wrap_pb1_engine_class(FailingEngine) is True
    engine = FailingEngine()
    with pytest.raises(RuntimeError, match="simulated tick failure"):
        engine.run()

    assert builtins.engine_runner is engine
    assert _session_finalization_marker_metrics_lookup() == {
        "order_candidates": 0,
        "api_submitted": 0,
        "skipped": 3,
    }


def test_engine_runner_guard_is_idempotent_for_engine_class():
    import trader

    class DummyEngine:
        def run(self):
            return "ok"

    assert trader._wrap_pb1_engine_class(DummyEngine) is True
    first_run = DummyEngine.run
    assert trader._wrap_pb1_engine_class(DummyEngine) is False
    assert DummyEngine.run is first_run
