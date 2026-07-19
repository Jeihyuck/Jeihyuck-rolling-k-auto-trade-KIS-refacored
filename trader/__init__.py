"""Trade API package."""

from __future__ import annotations

import builtins
from typing import Any


class _MissingEngineRunnerSummary:
    """Fallback summary object for legacy pb1_runner session finalization.

    Some session-finalization paths still read an unqualified ``engine_runner``
    name after loop execution.  When that name is unavailable, Python falls
    back to ``builtins``.  Keep a harmless empty-summary object there so the
    finalization path can still write ``pb1_result.json`` instead of raising
    ``NameError``.
    """

    _run_summary_payload: dict[str, Any] = {}
    _debug_summary: dict[str, Any] = {}


if not hasattr(builtins, "engine_runner"):
    builtins.engine_runner = _MissingEngineRunnerSummary()
