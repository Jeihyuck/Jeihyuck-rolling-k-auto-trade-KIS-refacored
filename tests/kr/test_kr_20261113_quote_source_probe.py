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



def test_actual_time_itemconclusion_source_is_independently_observed(monkeypatch, capsys):
    import json
    import sys
    import trader.kis_wrapper as kis_wrapper
    import scripts.kr_quote_source_probe as probe

    class FakeKis:
        env = "practice"

        def __init__(self, kis_env):
            assert kis_env == "practice"

        def get_price_snapshot(self, code, *, market):
            assert (code, market) == ("219130", "J")
            return {"source": "KIS_WEBSOCKET", "symbol": code, "stck_prpr": 10000}

        def get_kr_native_quote_readonly(self, code):
            return {"source": "KIS_REST_INQUIRE_PRICE", "requested_symbol": code}

        def get_kr_entry_native_trade_quote(self, code):
            return {
                "source": "KIS_REST_TIME_ITEMCONCLUSION",
                "source_identity": "AUTHENTICATED_REST_REQUEST_BOUND",
                "requested_symbol": code,
                "quote_source_contract": "SAME_OUTPUT2_EXECUTION_ROW",
                "stck_cntg_hour": "101001", "acml_vol": 500,
                "stck_prpr": 10100,
            }

    monkeypatch.setattr(kis_wrapper, "KisAPI", FakeKis)
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr(sys, "argv", ["probe", "--code", "219130", "--require-native-row"])
    probe.main()
    report = json.loads(capsys.readouterr().out)
    assert report["broker_order_calls"] == 0
    assert report["same_row_native_trade_status"] == "ATTESTED_REQUEST_BOUND"
    assert len(report["probes"]) == 3
    assert report["probes"][2]["native_exchange_clock"] == "101001"
    assert report["probes"][2]["cumulative_volume_present"] is True
    assert report["probes"][2]["origin"] == "INDEPENDENT_TIME_ITEMCONCLUSION_OUTPUT2"


def test_missing_native_trade_row_remains_not_attested_and_exits_nonzero(monkeypatch, capsys):
    import json
    import sys
    import pytest
    import trader.kis_wrapper as kis_wrapper
    import scripts.kr_quote_source_probe as probe

    class NoNativeKis:
        env = "practice"

        def __init__(self, kis_env):
            pass

        def get_price_snapshot(self, code, *, market):
            return {}

        def get_kr_native_quote_readonly(self, code):
            return {}

        def get_kr_entry_native_trade_quote(self, code):
            return {}

    monkeypatch.setattr(kis_wrapper, "KisAPI", NoNativeKis)
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr(sys, "argv", ["probe", "--code", "219130", "--require-native-row"])
    with pytest.raises(SystemExit) as exc:
        probe.main()
    assert exc.value.code == 3
    data = json.loads(capsys.readouterr().out)
    assert data["same_row_native_trade_status"] == "NOT_ATTESTED"
    assert data["broker_order_calls"] == 0
