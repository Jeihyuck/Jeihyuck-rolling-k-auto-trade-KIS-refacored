from trader.pb1_engine import PB1Engine


def test_block_entry_after_exit_submit(monkeypatch):
    monkeypatch.setenv("PB1_BLOCK_ENTRY_AFTER_EXIT", "1")
    engine = PB1Engine.__new__(PB1Engine)
    engine._exit_summary_payload = {"submit_attempt_count": 2, "accepted_sell_count": 1}

    blocked, metrics = PB1Engine._should_block_entry_after_exit(engine)

    assert blocked is True
    assert metrics["exit_submit_attempt_count"] == 2
    assert metrics["accepted_sell_count"] == 1


def test_block_entry_after_exit_can_be_disabled(monkeypatch):
    monkeypatch.setenv("PB1_BLOCK_ENTRY_AFTER_EXIT", "0")
    engine = PB1Engine.__new__(PB1Engine)
    engine._exit_summary_payload = {"submit_attempt_count": 2, "accepted_sell_count": 1}

    blocked, metrics = PB1Engine._should_block_entry_after_exit(engine)

    assert blocked is False
    assert metrics == {"exit_submit_attempt_count": 0, "accepted_sell_count": 0}