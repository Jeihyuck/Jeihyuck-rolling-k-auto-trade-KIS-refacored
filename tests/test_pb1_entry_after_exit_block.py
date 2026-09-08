from __future__ import annotations

from trader.kr.pb1.entry_after_exit_block import should_block_entry_after_exit
from trader.pb1_engine import PB1Engine


def test_should_block_entry_after_exit_uses_exit_summary_metrics() -> None:
    blocked, metrics = should_block_entry_after_exit(
        enabled=True,
        exit_summary_payload={"submit_attempt_count": 2, "accepted_sell_count": 1},
    )

    assert blocked is True
    assert metrics["exit_submit_attempt_count"] == 2
    assert metrics["accepted_sell_count"] == 1


def test_should_block_entry_after_exit_can_be_disabled() -> None:
    blocked, metrics = should_block_entry_after_exit(
        enabled=False,
        exit_summary_payload={"submit_attempt_count": 2, "accepted_sell_count": 1},
    )

    assert blocked is False
    assert metrics == {"exit_submit_attempt_count": 0, "accepted_sell_count": 0}


def test_pb1engine_should_block_entry_after_exit_matches_helper(monkeypatch) -> None:
    monkeypatch.setenv("PB1_BLOCK_ENTRY_AFTER_EXIT", "1")
    engine = PB1Engine.__new__(PB1Engine)
    engine._exit_summary_payload = {"submit_attempt_count": 2, "accepted_sell_count": 1}

    helper_result = should_block_entry_after_exit(
        enabled=True,
        exit_summary_payload=engine._exit_summary_payload,
    )
    wrapper_result = PB1Engine._should_block_entry_after_exit(engine)

    assert helper_result == wrapper_result
