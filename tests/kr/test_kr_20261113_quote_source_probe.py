"""No-order tests for practice WS vs independent REST quote evidence."""
from scripts.kr_quote_source_probe import public_quote_evidence


def test_ws_price_only_cannot_prove_native_clock_or_vwap():
    data = public_quote_evidence(
        {"source": "KIS_WEBSOCKET", "stck_prpr": 75000.0,
         "received_at": 1794440000, "age_sec": 0.01},
        requested_code="219130", origin="WS_FIRST_OR_CACHE",
    )
    assert data["native_exchange_clock"] is None
    assert data["native_trade_date"] is None
    assert not data["native_vwap_present"]
    assert data["quote_clock_field_source"] == "MISSING"


def test_independent_rest_keeps_source_specific_exchange_clock_without_inference():
    data = public_quote_evidence({
        "raw": {"stck_cntg_hour": "120013", "stck_bsop_date": "20261113",
                "acml_vol": "150000", "wghn_avrg_stck_prc": "75050"},
    }, requested_code="219130", origin="INDEPENDENT_NATIVE_REST_GET")
    assert data["native_exchange_clock"] == "120013"
    assert data["native_trade_date"] == "20261113"
    assert data["native_weighted_average_field_source"] == "raw"
    assert data["cumulative_volume_present"]


def test_invalid_explicit_primary_does_not_steal_cached_raw_provenance():
    data = public_quote_evidence({
        "stck_cntg_hour": None,
        "raw": {"stck_cntg_hour": "120013"},
    }, requested_code="219130", origin="WS_FIRST_OR_CACHE")
    assert data["native_exchange_clock"] is None
    assert data["quote_clock_field_source"] == "quote"
