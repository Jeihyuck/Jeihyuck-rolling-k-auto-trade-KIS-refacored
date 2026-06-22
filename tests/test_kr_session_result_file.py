from trader.kr.runner.trade_session_runner import extract_sell_orders_ack, load_pb1_session_result
from trader.pb1_runner import _write_session_result_file


def test_session_result_file_sell_ack_roundtrip(tmp_path, monkeypatch):
    path = tmp_path / "pb1_result.json"
    monkeypatch.setenv("PB1_SESSION_RESULT_PATH", str(path))

    _write_session_result_file({
        "status": "FAIL_PRECHECK",
        "exit_reason": "missing_db_exact_scored_final30",
        "sell_orders_ack": 1,
        "sell_orders": 1,
        "buy_orders": 0,
    })

    loaded = load_pb1_session_result(path)
    assert loaded["status"] == "FAIL_PRECHECK"
    assert extract_sell_orders_ack(loaded) == 1
