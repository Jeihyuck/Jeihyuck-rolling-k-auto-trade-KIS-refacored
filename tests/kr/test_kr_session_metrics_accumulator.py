from __future__ import annotations

import json

from trader.pb1_runner import _write_session_result_file


def test_session_result_uses_accumulated_metrics_and_status(tmp_path, monkeypatch) -> None:
    out = tmp_path / "session.json"
    monkeypatch.setenv("PB1_SESSION_RESULT_PATH", str(out))

    _write_session_result_file(
        {
            "status": "OK_NO_TRADE",
            "exit_reason": "session_end",
            "ticks_total": 5,
            "buy_orders": 3,
            "buy_orders_ack": 3,
            "sell_orders": 0,
            "sell_orders_ack": 0,
            "order_candidates": 4,
            "api_submitted": 3,
        }
    )

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["status"] == "OK_WITH_ORDERS"
    assert payload["ticks_total"] == 5
    assert payload["api_submitted"] == 3
    assert payload["buy_orders_ack"] == 3
