from pathlib import Path


def test_wsl_close_uses_exit_engine_phase_without_forcing_liquidation():
    text = Path("scripts/wsl/run-kr-close.sh").read_text(encoding="utf-8")
    assert "FORCE_PB1_PHASE=exit" in text
    assert 'PB1_CLOSE_LIQUIDATION_ENABLED="${PB1_CLOSE_LIQUIDATION_ENABLED:-0}"' in text


def test_kr_close_runner_uses_exit_phase_and_keeps_entry_disabled():
    text = Path("trader/kr/runner/trade_session_runner.py").read_text(encoding="utf-8")
    start = text.index("def _run_pb1_session")
    section = text[start:start + 7000]
    assert 'os.environ["FORCE_PB1_PHASE"] = "exit"' in section
    assert 'os.environ["PB1_ENTRY_ENABLED"] = "0"' in section
    assert 'os.environ["PB1_EXIT_ENABLED"] = "1"' in section
    assert 'os.environ["PB1_CLOSE_LIQUIDATION_ENABLED"] = os.getenv("PB1_CLOSE_LIQUIDATION_ENABLED", "0")' in section


def test_reconciliation_status_is_namespaced_and_cannot_overwrite_session_status():
    source = Path("trader/kr/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert '"reconciliation_status": count_reconcile.get("status")' in source
    assert '"reconciliation_reason": count_reconcile.get("reason")' in source
    assert '**count_reconcile}' not in source


def test_close_phase_failure_precedes_balance_warning():
    source = Path("trader/kr/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert 'close_phase_not_executed = session == "close" and pb1_last == "SKIP_PHASE_WINDOW"' in source
    close_guard = source.index('close_phase_not_executed = session == "close" and pb1_last == "SKIP_PHASE_WINDOW"')
    balance_guard = source.index('if session == "close" and balance_state is not None and balance_state.get("status") == "WARN":', close_guard)
    section = source[balance_guard:balance_guard + 750]
    assert 'if close_phase_not_executed:' in section
    assert '[KR_CLOSE][FAIL_PRESERVED]' in section
    assert section.index('if close_phase_not_executed:') < section.index('status = "WARN"')
