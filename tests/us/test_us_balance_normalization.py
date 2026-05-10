# -*- coding: utf-8 -*-
"""US balance normalization tests."""
import pytest
from trader.us.data_provider import normalize_us_balance


def test_us_balance_output1_dict_to_list():
    """output1이 dict 하나로 오는 경우 positions 1개로 변환."""
    raw = {
        "rt_cd": "0",
        "output1": {
            "ovrs_pdno": "AAPL",
            "ovrs_item_name": "Apple Inc.",
            "ovrs_cblc_qty": "10",
            "pchs_avg_pric": "150.00",
            "now_pric2": "160.00",
        },
        "output2": {},
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert result["normalized_position_count"] == 1
    assert len(result["positions"]) == 1
    assert result["positions"][0]["symbol"] == "AAPL"
    assert result["positions"][0]["qty"] == 10


def test_us_balance_output1_five_positions_normalized():
    """KIS raw output1에 5개 종목이 있을 때 정확히 5개로 정규화."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "4", "pchs_avg_pric": "150"},
            {"ovrs_pdno": "AVGO", "ovrs_cblc_qty": "2", "pchs_avg_pric": "800"},
            {"ovrs_pdno": "CIEN", "ovrs_cblc_qty": "2", "pchs_avg_pric": "50"},
            {"ovrs_pdno": "AMZN", "ovrs_cblc_qty": "4", "pchs_avg_pric": "170"},
            {"ovrs_pdno": "AAOI", "ovrs_cblc_qty": "6", "pchs_avg_pric": "40"},
        ],
        "output2": {},
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert result["raw_output1_count"] == 5
    assert result["normalized_position_count"] == 5
    assert len(result["positions"]) == 5
    assert set(result["position_symbols"]) == {"AAPL", "AVGO", "CIEN", "AMZN", "AAOI"}


def test_us_balance_output2_list_to_dict():
    """output2가 list[dict]로 오는 경우 summary dict로 변환."""
    raw = {
        "rt_cd": "0",
        "output1": [],
        "output2": [
            {"tot_evlu_pfls_amt": "10000.00", "frcr_pchs_amt1": "5000.00"},
        ],
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert isinstance(result["output2"], dict)
    assert result["output2"]["tot_evlu_pfls_amt"] == "10000.00"


def test_us_balance_raw_output1_nonzero_positions_zero_contract_error():
    """raw_output1_count > 0인데 normalized_position_count == 0이면 CONTRACT_ERROR."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {"symbol": "UNKNOWN", "qty": "0"},  # qty=0이라 필터링됨
            {"name": "NoSymbol", "qty": "5"},   # symbol 없어서 필터링됨
        ],
        "output2": {},
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "CONTRACT_ERROR"
    assert result["balance_parse_error"] == "raw_output1_nonzero_positions_zero"
    assert result["raw_output1_count"] == 2
    assert result["normalized_position_count"] == 0


def test_us_balance_rt_cd_not_zero_error():
    """rt_cd가 "0"이 아니면 ERROR."""
    raw = {
        "rt_cd": "1",
        "msg1": "API error",
        "output1": [],
        "output2": {},
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "ERROR"
    assert "rt_cd_not_zero" in result["balance_parse_error"]


def test_us_balance_qty_zero_filtered():
    """qty <= 0인 row는 positions에서 제외."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "10"},
            {"ovrs_pdno": "GOOGL", "ovrs_cblc_qty": "0"},
            {"ovrs_pdno": "MSFT", "ovrs_cblc_qty": "-5"},
        ],
        "output2": {},
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert result["normalized_position_count"] == 1
    assert result["positions"][0]["symbol"] == "AAPL"


def test_us_balance_safe_numeric_conversion():
    """콤마, None, 빈 문자열 등 안전한 숫자 변환."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {
                "ovrs_pdno": "AAPL",
                "ovrs_cblc_qty": "10",
                "pchs_avg_pric": "1,173.28",  # 콤마 포함
                "now_pric2": "1200.50",
                "frcr_evlu_amt2": "12,005.00",  # 콤마 포함
            },
        ],
        "output2": {},
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    pos = result["positions"][0]
    assert pos["avg_price_usd"] == 1173.28
    assert pos["current_price_usd"] == 1200.50
    assert pos["market_value_usd"] == 12005.00
