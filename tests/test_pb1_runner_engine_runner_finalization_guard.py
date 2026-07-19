from __future__ import annotations

import builtins


def test_engine_runner_builtin_guard_prevents_legacy_summary_nameerror():
    import trader  # noqa: F401 - package import installs the guard

    assert hasattr(builtins, "engine_runner")

    # Mirrors the legacy pb1_runner session-finalization lookup that used to
    # raise NameError when no local/global engine_runner existed.
    marker_metrics = getattr(engine_runner, "_run_summary_payload", {}) or getattr(  # noqa: F821
        engine_runner, "_debug_summary", {}  # noqa: F821
    )

    assert marker_metrics == {}
