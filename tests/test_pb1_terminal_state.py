from __future__ import annotations

from trader.pb1_engine import PB1Engine
from trader.kr.pb1.terminal_state import resolve_terminal_state


class _FakeEngine:
    def __init__(self, warning_counts: dict[str, int] | None = None):
        self._warning_counts = warning_counts or {}

    def _warning_counts_dict(self) -> dict[str, int]:
        return dict(self._warning_counts)


def test_resolve_terminal_state_maps_baseline_statuses() -> None:
    assert resolve_terminal_state(status="FATAL_RUNTIME") == "SESSION_END_FATAL"
    assert resolve_terminal_state(status="FATAL_POSTPROCESS") == "SESSION_END_FATAL"
    assert resolve_terminal_state(status="SKIP_DRY_RUN") == "SESSION_END_SKIPPED"
    assert resolve_terminal_state(status="OK_DEGRADED") == "SESSION_END_OK_DEGRADED"
    assert resolve_terminal_state(status="DEGRADED_POSTPROCESS") == "SESSION_END_OK_DEGRADED"
    assert resolve_terminal_state(status="WARN_FAIL_OPEN") == "SESSION_END_OK_WITH_WARNINGS"
    assert resolve_terminal_state(status="OK_WITH_WARNINGS") == "SESSION_END_OK_WITH_WARNINGS"
    assert resolve_terminal_state(status="OK", notes="degraded by upstream") == "SESSION_END_OK_WITH_WARNINGS"
    assert resolve_terminal_state(status="OK") == "SESSION_END_OK"


def test_resolve_terminal_state_uses_warning_counts() -> None:
    assert resolve_terminal_state(status="OK", warning_counts={"degraded_stage_count": 1}) == "SESSION_END_OK_DEGRADED"
    assert resolve_terminal_state(status="OK", warning_counts={"timeout_count": 1}) == "SESSION_END_OK_WITH_WARNINGS"


def test_pb1engine_resolve_terminal_state_matches_helper() -> None:
    engine = _FakeEngine({"timeout_count": 1})
    helper_result = resolve_terminal_state(status="OK", warning_counts=engine._warning_counts)
    wrapper_result = PB1Engine._resolve_terminal_state(engine, status="OK")

    assert helper_result == wrapper_result
