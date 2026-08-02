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


def test_normalize_item_preserves_kr_regime_fields_top_level_and_meta():
    builder=WatchlistBuilder.__new__(WatchlistBuilder)
    builder.minervini_config={}; builder.tech_weight=.7; builder.flow_weight=.3
    item=builder._base_item("005930",market="KOSPI")
    item.update({"close":100,"last_close":100,"ma20":90,"ma50":80,"return_1d":.02,"return_5d":.03,
                 "above_ma20":True,"above_ma50":True,"volume_avg20":100000,"final_score":80,"tech_score":80})
    normalized=builder._normalize_item(item,score_key="final_score",rank_key="rank")
    for key in ("market","market_code","rs_benchmark","return_1d","return_5d","volume_avg20"):
        assert normalized[key] == normalized["meta"][key]


def test_final30_file_roundtrip_preserves_market_and_returns(monkeypatch,tmp_path):
    import pandas as pd
    import trader.prep_runner as prep
    from trader.path_contract import read_final30_file_rows
    monkeypatch.setattr(prep,"repo_root",lambda:tmp_path)
    row={"code":"005930","name":"Samsung","rank_final30":1,"score_final":80,"market":"KOSPI","market_code":"KOSPI",
         "rs_benchmark":"069500","close":100,"ma20":90,"ma50":80,"return_1d":.02,"return_5d":.03,
         "above_ma20":True,"above_ma50":True,"volume_avg20":100000}
    prep._write_canonical_final30_scored_files(env="practice",as_of="2026-07-31",df=pd.DataFrame([row]))
    paths=prep.build_final30_scored_paths(tmp_path,"practice","2026-07-31")
    loaded=[]
    for path in paths.values():
        rows,ok=read_final30_file_rows(path)
        if ok and rows: loaded=rows; break
    assert loaded[0]["market"] == "KOSPI"
    assert loaded[0]["return_1d"] == .02 and loaded[0]["volume_avg20"] == 100000
