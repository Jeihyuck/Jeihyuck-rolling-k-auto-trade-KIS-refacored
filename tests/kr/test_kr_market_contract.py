from trader.watchlist_builder import WatchlistBuilder
from trader.final30_quality import normalize_final30_contract_row
from trader.contracts.final30_contract import KR_REGIME_REQUIRED_FINAL30_FIELDS


def test_stage_a_base_item_preserves_market_and_benchmark():
    builder=WatchlistBuilder.__new__(WatchlistBuilder)
    kospi=builder._base_item("005930",market="KS")
    kosdaq=builder._base_item("035720",market="KQ")
    assert (kospi["market"],kospi["market_code"],kospi["rs_benchmark"]) == ("KOSPI","KOSPI","069500")
    assert (kosdaq["market"],kosdaq["market_code"],kosdaq["rs_benchmark"]) == ("KOSDAQ","KOSDAQ","229200")


def test_final30_normalizer_preserves_regime_inputs_without_zero_defaults():
    row=normalize_final30_contract_row({"code":"5930","market_code":"P","close":100,"ma20":90,"ma50":80,"return_1d":.02,"return_5d":-.01,"rs_benchmark":"069500"})
    assert row["market"] == "KOSPI"
    assert row["return_1d"] == .02 and row["return_5d"] == -.01
    assert KR_REGIME_REQUIRED_FINAL30_FIELDS <= set(row)
    missing=normalize_final30_contract_row({"code":"1","market":"KOSPI"})
    assert missing["return_1d"] is None
