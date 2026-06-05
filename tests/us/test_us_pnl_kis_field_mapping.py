from __future__ import annotations


def test_normalize_kis_position_supports_extended_price_and_value_fields() -> None:
    from scripts.generate_us_portfolio_pnl_report import _normalize_kis_position

    normalized = _normalize_kis_position({
        "ovrs_pdno": "CIEN",
        "ovrs_cblc_qty": 4,
        "pchs_avg_pric": 10.0,
        "frcr_pchs_amt1": 40.0,
        "ovrs_stck_evlu_amt": 48.0,
        "now_pric2": 12.0,
    })

    assert normalized["symbol"] == "CIEN"
    assert normalized["qty"] == 4
    assert normalized["last_price"] == 12.0
    assert normalized["cost_usd"] == 40.0
    assert normalized["market_value_usd"] == 48.0
