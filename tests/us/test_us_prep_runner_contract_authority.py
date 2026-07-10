import inspect

from trader.us.runner import prep_runner


def test_prep_runner_uses_contract_as_authoritative_source_for_status_outputs():
    src = inspect.getsource(prep_runner.run_prep)

    assert "provisional_status" in src
    assert 'final_status = contract.get("status", provisional_status)' in src
    assert 'trade_can_proceed = int(contract.get("trade_can_proceed", 0) or 0)' in src
    assert "result_dict = dict(contract)" in src
    assert "prep_status_payload = {" in src and "**contract" in src
    assert "result = dict(contract)" in src

    # The legacy final30==30 gate must not drive trade_can_proceed in prep_runner.
    assert "final30_complete = wl_final30 == 30" not in src
    assert "and final30_complete" not in src
    assert 'final_status in ("OK", "OK_WITH_WARNINGS")' not in src


def test_prep_exit_code_allows_underfilled_contract_trade_ready_status():
    assert prep_runner._prep_exit_code({
        "status": "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE",
        "trade_can_proceed": 1,
        "trade_block_reason": "ok",
    }) == 0


def test_prep_exit_code_blocks_failed_underfilled_contract():
    assert prep_runner._prep_exit_code({
        "status": "FAILED_FINAL30_UNDERFILLED",
        "trade_can_proceed": 0,
        "trade_block_reason": "final30_below_absolute_min",
    }) == 1
