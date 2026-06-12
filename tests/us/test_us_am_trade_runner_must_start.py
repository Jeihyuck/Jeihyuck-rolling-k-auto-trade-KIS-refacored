from __future__ import annotations

from pathlib import Path

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "us-trade-am.yml"


def test_am_expectation_and_runner_contract_present() -> None:
    src = WORKFLOW.read_text(encoding="utf-8")
    assert "[US_TRADE_AM][INTENT_EXPECTATION] trade_intent_expected=${expected_to_trade}" in src
    assert "[US_TRADE_AM][EXPECTATION] expected_to_trade=${expected_to_trade}" in src
    assert "dry_run_flag" in src and "offline_flag" in src and "force_now_set" in src
    run_step = src[src.index("- name: Run US AM trade session"):]
    run_step = run_step[:run_step.index("working-directory:")]
    assert "github.event_name == 'schedule'" not in run_step
    assert "steps.session_lock.outputs.claimed == '1'" in run_step
    assert "steps.expectation.outputs.expected_to_trade == '1'" in run_step
    assert "[US_TRADE_AM][TRADE_RUNNER][START] session=am" in src


def test_am_final_status_fails_when_expected_but_runner_not_started() -> None:
    src = WORKFLOW.read_text(encoding="utf-8")
    assert "FAILED_TRADE_NOT_STARTED" in src
    assert "[ \"${expected_to_trade}\" = \"1\" ] && [ \"${trade_runner_started}\" != \"1\" ]" in src
