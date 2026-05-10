# -*- coding: utf-8 -*-
"""US total_pvs summary parsing tests.

total_pvs는 총 평가금액이어야 하며, 손익(pnl) 필드가 아니어야 한다.
output2에서 평가금액 필드를 우선 사용하고, 없으면 positions의 market_value_usd 합계를 사용한다.
"""
from trader.us.data_provider import normalize_us_balance


def test_total_pvs_from_evaluation_amount():
    """output2에 평가금액(frcr_evlu_amt2)이 있으면 이를 사용해야 한다."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {
                "ovrs_pdno": "AAPL",
                "ovrs_item_name": "Apple",
                "ovrs_excg_cd": "NASD",
                "ovrs_cblc_qty": "10",
                "pchs_avg_pric": "100.00",
                "now_pric2": "150.00",
                "ovrs_stck_evlu_amt": "1500.00",  # market_value
                "frcr_evlu_pfls_amt": "500.00",  # pnl (not total_pvs)
            }
        ],
        "output2": {
            "frcr_evlu_amt2": "1500.00",  # 평가금액 (total_pvs)
            "tot_evlu_pfls_amt": "500.00",  # 손익 (pnl)
        }
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert float(result["total_pvs"]) == 1500.0, "total_pvs should be evaluation amount"
    assert float(result["pnl_usd"]) == 500.0, "pnl_usd can be separate field"


def test_total_pvs_from_position_sum_when_output2_missing():
    """output2에 평가금액이 없으면 positions의 market_value_usd 합계를 사용해야 한다."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {
                "ovrs_pdno": "AAPL",
                "ovrs_item_name": "Apple",
                "ovrs_excg_cd": "NASD",
                "ovrs_cblc_qty": "10",
                "pchs_avg_pric": "100.00",
                "now_pric2": "150.00",
                "ovrs_stck_evlu_amt": "1500.00",
                "frcr_evlu_pfls_amt": "500.00",
            },
            {
                "ovrs_pdno": "MSFT",
                "ovrs_item_name": "Microsoft",
                "ovrs_excg_cd": "NASD",
                "ovrs_cblc_qty": "5",
                "pchs_avg_pric": "200.00",
                "now_pric2": "250.00",
                "ovrs_stck_evlu_amt": "1250.00",
                "frcr_evlu_pfls_amt": "250.00",
            }
        ],
        "output2": {}  # No evaluation amount
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert len(result["positions"]) == 2
    # total_pvs should be sum of market_value_usd
    expected_total = 1500.0 + 1250.0
    assert float(result["total_pvs"]) == expected_total


def test_total_pvs_not_negative_pnl():
    """total_pvs는 음수 손익이 아니라 양수 평가금액이어야 한다."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {
                "ovrs_pdno": "AAPL",
                "ovrs_item_name": "Apple",
                "ovrs_excg_cd": "NASD",
                "ovrs_cblc_qty": "10",
                "pchs_avg_pric": "100.00",
                "now_pric2": "80.00",
                "ovrs_stck_evlu_amt": "800.00",  # market_value
                "frcr_evlu_pfls_amt": "-200.00",  # negative pnl
            }
        ],
        "output2": {
            "frcr_evlu_amt2": "800.00",  # 평가금액 (positive)
            "tot_evlu_pfls_amt": "-200.00",  # 손익 (negative)
        }
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert float(result["total_pvs"]) == 800.0, "total_pvs should be positive evaluation amount"
    assert float(result["pnl_usd"]) == -200.0, "pnl_usd can be negative"


def test_total_pvs_zero_when_no_positions():
    """포지션이 없으면 total_pvs는 0이어야 한다."""
    raw = {
        "rt_cd": "0",
        "output1": [],
        "output2": {}
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert result["total_pvs"] == "0"
    assert len(result["positions"]) == 0


def test_pnl_usd_separate_from_total_pvs():
    """pnl_usd와 total_pvs는 별도 필드로 분리되어야 한다."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {
                "ovrs_pdno": "AAPL",
                "ovrs_item_name": "Apple",
                "ovrs_excg_cd": "NASD",
                "ovrs_cblc_qty": "10",
                "pchs_avg_pric": "100.00",
                "now_pric2": "150.00",
                "ovrs_stck_evlu_amt": "1500.00",
                "frcr_evlu_pfls_amt": "500.00",
            }
        ],
        "output2": {
            "frcr_evlu_amt2": "1500.00",
            "tot_evlu_pfls_amt": "500.00",
        }
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert "total_pvs" in result
    assert "pnl_usd" in result
    assert result["total_pvs"] != result["pnl_usd"]
    assert float(result["total_pvs"]) == 1500.0
    assert float(result["pnl_usd"]) == 500.0


def test_total_pvs_fallback_to_positions_when_output2_value_zero():
    """output2 평가금액이 0이면 positions 합계로 계산해야 한다."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {
                "ovrs_pdno": "AAPL",
                "ovrs_item_name": "Apple",
                "ovrs_excg_cd": "NASD",
                "ovrs_cblc_qty": "10",
                "pchs_avg_pric": "100.00",
                "now_pric2": "150.00",
                "ovrs_stck_evlu_amt": "1500.00",
                "frcr_evlu_pfls_amt": "500.00",
            }
        ],
        "output2": {
            "frcr_evlu_amt2": "0",  # Zero value (unreliable)
            "tot_evlu_pfls_amt": "500.00",
        }
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    # Should fallback to positions sum
    assert float(result["total_pvs"]) == 1500.0


def test_position_exchange_normalized():
    """position의 exchange는 정규화되어야 한다 (NASD → NASDAQ)."""
    raw = {
        "rt_cd": "0",
        "output1": [
            {
                "ovrs_pdno": "AAPL",
                "ovrs_item_name": "Apple",
                "ovrs_excg_cd": "NASD",  # Raw KIS code
                "ovrs_cblc_qty": "10",
                "pchs_avg_pric": "100.00",
                "now_pric2": "150.00",
                "ovrs_stck_evlu_amt": "1500.00",
            }
        ],
        "output2": {}
    }
    
    result = normalize_us_balance(raw)
    
    assert result["balance_parse_status"] == "OK"
    assert len(result["positions"]) == 1
    pos = result["positions"][0]
    assert pos["exchange"] == "NASDAQ", "exchange should be normalized"
    assert pos["raw_exchange"] == "NASD", "raw_exchange should be preserved"
