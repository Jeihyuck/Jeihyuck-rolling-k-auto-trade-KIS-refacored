# -*- coding: utf-8 -*-
"""US reconcile positions tests."""
import pytest
from unittest.mock import MagicMock
from trader.us.execution.reconcile import reconcile_positions


def test_us_reconcile_positions_from_kis_balance():
    """provider.get_balance()가 normalized positions 5개를 반환할 때 position_count=5."""
    provider = MagicMock()
    provider.get_balance.return_value = {
        "positions": [
            {"symbol": "AAPL", "qty": 4},
            {"symbol": "AVGO", "qty": 2},
            {"symbol": "CIEN", "qty": 2},
            {"symbol": "AMZN", "qty": 4},
            {"symbol": "AAOI", "qty": 6},
        ],
        "total_pvs": "10000",
        "raw_output1_count": 5,
        "normalized_position_count": 5,
        "position_symbols": ["AAPL", "AVGO", "CIEN", "AMZN", "AAOI"],
        "balance_parse_status": "OK",
        "balance_parse_error": None,
    }
    
    result = reconcile_positions(provider=provider)
    
    assert result["status"] == "OK"
    assert result["position_count"] == 5
    assert len(result["positions"]) == 5
    assert set(result["position_symbols"]) == {"AAPL", "AVGO", "CIEN", "AMZN", "AAOI"}
    assert result["block_new_entry"] is False


def test_us_reconcile_blocks_entry_on_balance_parse_error():
    """balance_parse_status != OK이면 block_new_entry=True."""
    provider = MagicMock()
    provider.get_balance.return_value = {
        "positions": [],
        "raw_output1_count": 0,
        "normalized_position_count": 0,
        "position_symbols": [],
        "balance_parse_status": "ERROR",
        "balance_parse_error": "rt_cd_not_zero",
    }
    
    result = reconcile_positions(provider=provider)
    
    assert result["status"] == "CONTRACT_ERROR"
    assert result["block_new_entry"] is True
    assert result["reason"] == "balance_position_parse_error"


def test_us_reconcile_contract_error_raw_nonzero_normalized_zero():
    """raw_output1_count > 0인데 normalized_position_count == 0이면 CONTRACT_ERROR."""
    provider = MagicMock()
    provider.get_balance.return_value = {
        "positions": [],
        "raw_output1_count": 3,
        "normalized_position_count": 0,
        "position_symbols": [],
        "balance_parse_status": "OK",
        "balance_parse_error": None,
    }
    
    result = reconcile_positions(provider=provider)
    
    assert result["status"] == "CONTRACT_ERROR"
    assert result["block_new_entry"] is True
    assert result["balance_parse_error"] == "raw_output1_nonzero_positions_zero"


def test_us_reconcile_empty_positions_ok():
    """실제 보유가 0개일 때는 정상 (raw_output1_count=0, normalized=0)."""
    provider = MagicMock()
    provider.get_balance.return_value = {
        "positions": [],
        "raw_output1_count": 0,
        "normalized_position_count": 0,
        "position_symbols": [],
        "balance_parse_status": "OK",
        "balance_parse_error": None,
    }
    
    result = reconcile_positions(provider=provider)
    
    assert result["status"] == "OK"
    assert result["position_count"] == 0
    assert result["block_new_entry"] is False
