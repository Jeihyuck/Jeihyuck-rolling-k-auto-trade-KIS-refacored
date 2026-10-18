"""KR Oct-19: run the exact runner/engine path, EXIT before entry-only scans.

Offline structural regression; never invokes KIS or emits actual orders.
"""
from __future__ import annotations

import ast
import inspect
import textwrap

from trader.pb1_engine import PB1Engine


def _sources():
    from trader import install_legacy_pb1_runtime_guards
    from trader import pb1_runner
    install_legacy_pb1_runtime_guards()
    # inspect unwraps the installed functools.wraps chain.
    return inspect.getsource(pb1_runner.run_once), inspect.getsource(PB1Engine.run)


def test_exit_pass_precedes_diagnostic_entry_scan_in_installed_engine():
    runner, engine = _sources()
    assert "def _kr_entry_scan_after_exit():" in runner
    assert "nonlocal entry_signals_result, entry_scan_compat_failed" in runner
    assert "engine_runner._kr_deferred_entry_scan = _kr_entry_scan_after_exit" in runner
    assert "scan_all_strategies(" in runner
    assert engine.index("self._run_exit_always(") < engine.index("_deferred_entry_scan()")
    assert engine.index("reason=\"PORTFOLIO_FULL\"") < engine.index("_deferred_entry_scan()")
    assert engine.index("_deferred_entry_scan()") < engine.index("entry_pass_ok = True")


def test_independent_infinite_price_prewarm_never_runs_before_pb1_exit():
    runner, engine = _sources()
    assert "engine_runner._kr_after_protective_exit = _kr_after_protective_exit" in runner
    assert engine.index("self._run_exit_always(") < engine.index("_after_exit()")
    assert "_kr_after_protective_exit" in engine
    start = runner.index("        if close_cancel_only:")
    end = runner.index("                result = engine_runner.run()", start)
    assert "prewarm_kr_infinite_price" not in runner[start:end]


def test_deferred_scan_uses_same_clock_and_preserves_scanner_context_results():
    runner, _ = _sources()
    block = runner.split("def _kr_entry_scan_after_exit():", 1)[1].split(
        "# Runner-side diagnostics", 1
    )[0]
    assert "time_mod.monotonic()" in block
    assert "time.monotonic()" not in block
    assert 'setattr(engine_runner, "_scanner_evaluations"' in block
    assert '"scanner_passed_codes": _all_scan_codes or _existing_ctx.get(' in block
    assert "entry_scan_compat_failed = True" in block
    # Scan call remains inside the deferred callback, not the top-level
    # pre-engine instructions (which would starve protective SELL).
    tree = ast.parse(textwrap.dedent(inspect.getsource(
        __import__("trader.pb1_runner", fromlist=["run_once"]).run_once
    )))
    functions = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    deferred = [n for n in functions if n.name == "_kr_entry_scan_after_exit"]
    assert len(deferred) == 1
    scans = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and
             isinstance(n.func, ast.Name) and n.func.id == "scan_all_strategies"]
    assert len(scans) == 1 and scans[0] in list(ast.walk(deferred[0]))
