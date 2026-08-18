from types import SimpleNamespace

from trader.kr.infinite.regime_adapter import from_canonical_snapshot
from trader.kr.infinite.regime_adapter import load_canonical_snapshot
import json


def test_extracts_same_kospi_state_from_canonical_object():
    market = SimpleNamespace(state="KR_RISK_ON", data_quality="OK")
    view = from_canonical_snapshot(SimpleNamespace(market_states={"KOSPI": market}, data_quality="OK"))
    assert (view.state, view.data_quality, view.available) == ("KR_RISK_ON", "OK", True)


def test_missing_canonical_snapshot_is_unavailable_not_normal():
    view = from_canonical_snapshot(None)
    assert view.state is None and view.data_quality == "BLOCKED" and not view.available


def test_adapter_contains_no_market_calculation_dependencies():
    source = __import__("pathlib").Path("trader/kr/infinite/regime_adapter.py").read_text()
    for forbidden in ("calculate_market_state", "get_daily_candles", "ma20", "breadth"):
        assert forbidden not in source.lower()


def test_stale_canonical_snapshot_is_buy_unavailable(tmp_path, monkeypatch):
    path=tmp_path/"snapshot.json";path.write_text(json.dumps({"as_of":"2026-08-13T10:00:00+09:00","data_quality":"OK","market_states":{"KOSPI":{"state":"KR_NORMAL","data_quality":"OK"}}}))
    monkeypatch.setenv("KR_TRADE_DATE","2026-08-14")
    assert not load_canonical_snapshot(path).available


def test_prep_contract_market_state_is_available_to_infinite(tmp_path, monkeypatch):
    path = tmp_path / "prep_contract.json"
    path.write_text(json.dumps({
        "trade_date": "2026-08-18", "market_state": "KR_NORMAL",
        "regime_quality": "OK", "regime_source": "prep_ok_default",
    }))
    monkeypatch.setenv("KR_TRADE_DATE", "2026-08-18")

    view = load_canonical_snapshot(path)

    assert (view.state, view.data_quality, view.available) == ("KR_NORMAL", "OK", True)
