from __future__ import annotations

from trader import prep_runner


def test_make_flow_provider_exposes_run_level_provider_state() -> None:
    provider = prep_runner._make_flow_provider(engine=None)
    state = getattr(provider, "provider_state", {})

    assert "kis" in state
    assert "pykrx" in state
    for name in ("kis", "pykrx"):
        assert "enabled" in state[name]
        assert "disabled_reason" in state[name]
        assert "fail_count" in state[name]
        assert "success_count" in state[name]
        assert "reason_counts" in state[name]
