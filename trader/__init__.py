"""Trade API package."""

from __future__ import annotations

import builtins
import functools
import logging
from typing import Any

_logger = logging.getLogger(__name__)


class _MissingEngineRunnerSummary:
    """Last-resort empty summary for PB1 session finalization.

    Used only before the first PB1 tick has run. Once a real PB1Engine run starts,
    the wrapper below replaces builtins.engine_runner with the actual engine
    instance so finalization reads real run metrics.
    """

    _run_summary_payload: dict[str, Any] = {}
    _debug_summary: dict[str, Any] = {}


def _ensure_engine_runner_fallback() -> None:
    if not hasattr(builtins, "engine_runner"):
        builtins.engine_runner = _MissingEngineRunnerSummary()


def _wrap_pb1_engine_class(engine_cls: type[Any]) -> bool:
    """Make session finalization see the latest real PB1Engine.

    pb1_runner finalization reads an engine_runner name after loop execution.
    Without this guard the name can be unresolved and finalization can fail before
    pb1_result.json is written. The guard publishes the actual current PB1Engine
    instance through builtins during every run, preserving _run_summary_payload and
    _debug_summary for the result marker.
    """

    if getattr(engine_cls, "_engine_runner_finalization_guard_installed", False):
        return False

    def _wrap_method(method_name: str) -> None:
        original = getattr(engine_cls, method_name, None)
        if not callable(original):
            return

        @functools.wraps(original)
        def guarded(self: Any, *args: Any, **kwargs: Any) -> Any:
            builtins.engine_runner = self
            try:
                return original(self, *args, **kwargs)
            finally:
                builtins.engine_runner = self

        setattr(engine_cls, method_name, guarded)

    _wrap_method("run")
    _wrap_method("run_close_cancel")
    setattr(engine_cls, "_engine_runner_finalization_guard_installed", True)
    return True


def _install_pb1_engine_runner_finalization_guard() -> None:
    _ensure_engine_runner_fallback()
    try:
        from trader.pb1_engine import PB1Engine
    except Exception as exc:  # pragma: no cover - keep package import non-fatal
        _logger.warning("[PB1][ENGINE_RUNNER_GUARD][INSTALL_DEFERRED] err=%s", exc)
        return
    installed = _wrap_pb1_engine_class(PB1Engine)
    if installed:
        _logger.info("[PB1][ENGINE_RUNNER_GUARD][INSTALLED] target=PB1Engine")


_install_pb1_engine_runner_finalization_guard()
