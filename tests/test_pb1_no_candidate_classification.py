from __future__ import annotations


def test_no_candidate_classification_is_ok_no_trade() -> None:
    from trader.pb1_engine import _classify_no_candidate_result

    status, reason = _classify_no_candidate_result()

    assert status == "OK_NO_TRADE"
    assert reason == "NO_CANDIDATES_AFTER_RELAX"


def test_run_entry_mode_has_safe_default_and_loop_uses_distinct_local() -> None:
    import inspect
    import trader.pb1_engine as pb1_engine

    src = inspect.getsource(pb1_engine.PB1Engine.run)
    assert 'entry_mode = str(getattr(self, "entry_mode", os.getenv("PB1_ENTRY_COND_MODE", "OR")) or "OR").upper()' in src
    assert 'entry_ok, entry_reasons, entry_mode_used = self._entry_gate(' in src
    assert 'entry_ok, entry_reasons, entry_mode = self._entry_gate(' not in src
