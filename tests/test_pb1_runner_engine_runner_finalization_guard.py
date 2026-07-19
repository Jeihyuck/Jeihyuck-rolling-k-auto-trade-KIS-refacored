from __future__ import annotations

import builtins


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

    # Mirrors the legacy pb1_runner session-finalization lookup.  It must read
    # the real latest engine summary, not an empty builtins placeholder.
    marker_metrics = getattr(engine_runner, "_run_summary_payload", {}) or getattr(  # noqa: F821
        engine_runner, "_debug_summary", {}  # noqa: F821
    )

    assert builtins.engine_runner is engine
    assert marker_metrics == {"order_candidates": 2, "api_submitted": 1}


def test_engine_runner_guard_is_idempotent_for_engine_class():
    import trader

    class DummyEngine:
        def run(self):
            return "ok"

    assert trader._wrap_pb1_engine_class(DummyEngine) is True
    first_run = DummyEngine.run
    assert trader._wrap_pb1_engine_class(DummyEngine) is False
    assert DummyEngine.run is first_run
