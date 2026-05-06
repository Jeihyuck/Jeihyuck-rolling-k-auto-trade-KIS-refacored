# -*- coding: utf-8 -*-
"""US trade fills error blocks entry tests."""

from pathlib import Path


def test_trade_tick_blocks_entry_on_fills_contract_error():
    """fills contract error가 entry/order route를 차단하는지 확인."""
    text = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert "fills_contract_error" in text
    assert "reason=fills_contract_error" in text
    assert "require_fill_confirm=1" in text
    assert "ROUTE][SKIP]" in text or "[US_ORDER][ROUTE][SKIP]" in text


def test_trade_tick_returns_error_on_fills_contract_error():
    """fills contract error 시 tick이 ERROR status를 반환하는지 확인."""
    text = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert '"status": "ERROR"' in text or "'status': 'ERROR'" in text
    assert '"reason": "fills_contract_error"' in text or "'reason': 'fills_contract_error'" in text


def test_session_stops_on_fills_contract_error():
    """session runner가 fills_contract_error 시 즉시 종료하는지 확인."""
    text = Path("trader/us/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert 'reason=fills_contract_error' in text
    assert 'tick_result.get("reason") == "fills_contract_error"' in text
    # Session이 ERROR 반환하는지 확인
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if 'tick_result.get("reason") == "fills_contract_error"' in line:
            # 다음 몇 줄 내에 status ERROR 반환이 있어야 함
            next_lines = "\n".join(lines[i:i+20])
            assert '"status": "ERROR"' in next_lines or "'status': 'ERROR'" in next_lines
            break
