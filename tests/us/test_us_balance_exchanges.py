# -*- coding: utf-8 -*-
"""US balance multi-exchange tests."""
import pytest
from unittest.mock import MagicMock, patch


def test_us_balance_multi_exchange_merge():
    """NASD 4개, NYSE 1개 fixture를 병합하여 최종 positions=5."""
    from trader.us.execution.kis_us_client import KisUSClient
    
    # Mock _get_us_balance_single_exchange 반환값
    def mock_single_exchange(exchange_code: str, max_pages: int = 10):
        if exchange_code == "NASD":
            return {
                "rt_cd": "0",
                "output1": [
                    {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "4", "ovrs_excg_cd": "NASD"},
                    {"ovrs_pdno": "AVGO", "ovrs_cblc_qty": "2", "ovrs_excg_cd": "NASD"},
                    {"ovrs_pdno": "AMZN", "ovrs_cblc_qty": "4", "ovrs_excg_cd": "NASD"},
                    {"ovrs_pdno": "AAOI", "ovrs_cblc_qty": "6", "ovrs_excg_cd": "NASD"},
                ],
                "output2": {"tot_evlu_pfls_amt": "10000"},
            }
        elif exchange_code == "NYSE":
            return {
                "rt_cd": "0",
                "output1": [
                    {"ovrs_pdno": "CIEN", "ovrs_cblc_qty": "2", "ovrs_excg_cd": "NYSE"},
                ],
                "output2": {},
            }
        else:
            return {"rt_cd": "0", "output1": [], "output2": {}}
    
    client = KisUSClient(env="practice", offline=True)
    client._get_us_balance_single_exchange = mock_single_exchange
    client._offline = False  # get_us_balance는 offline check 하므로 False로
    
    # Mock _assert_not_offline
    client._assert_not_offline = lambda x: None
    
    result = client.get_us_balance()
    
    assert result["rt_cd"] == "0"
    assert len(result["output1"]) == 5
    assert result["queried_exchanges"] == ["NASD", "NYSE", "AMEX"]
    assert result["exchange_result_counts"]["NASD"] == 4
    assert result["exchange_result_counts"]["NYSE"] == 1
    assert result["exchange_result_counts"]["AMEX"] == 0
    
    symbols = [row["ovrs_pdno"] for row in result["output1"]]
    assert set(symbols) == {"AAPL", "AVGO", "CIEN", "AMZN", "AAOI"}


def test_us_balance_symbol_duplicate_merge():
    """동일 symbol이 여러 거래소에서 나오면 qty 합산."""
    from trader.us.execution.kis_us_client import KisUSClient
    
    rows = [
        {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "10", "ovrs_stck_evlu_amt": "1500.00"},
        {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "5", "ovrs_stck_evlu_amt": "750.00"},
    ]
    
    client = KisUSClient(env="practice", offline=True)
    merged = client._merge_duplicate_symbols(rows)
    
    assert len(merged) == 1
    assert merged[0]["ovrs_pdno"] == "AAPL"
    # qty 합산: 10 + 5 = 15
    assert int(merged[0]["ovrs_cblc_qty"]) == 15
    # market_value 합산: 1500 + 750 = 2250
    assert float(merged[0]["ovrs_stck_evlu_amt"]) == 2250.0
