from datetime import datetime, timedelta

from trader.kr.opening_bar_contract import KST
from trader.kr.opening_intelligence import analyze_opening, ObservationPromotion


def test_opening_geometry_is_observation_and_conflicts_invalidate():
    now = datetime(2026,11,27,9,5,tzinfo=KST)
    start = now.replace(minute=0)
    rows = []
    for i,(hi,lo,cl) in enumerate([(10000,9900,9950),(9800,9700,9750),
                                    (9770,9700,9750),(9780,9710,9760),
                                    (9790,9720,9770)]):
        rows.append({"date":"20261127","time":(start+timedelta(minutes=i)).strftime("%H%M%S"),
                     "open":cl,"high":hi,"low":lo,"close":cl,"volume":100})
    proof = {"source":"KIS_FHKST03010200","trade_date":"2026-11-27",
             "as_of":now.isoformat(),"bars":rows,"sha256":"fixture"}
    result = analyze_opening(proof=proof,now=now,prep_style="MOMENTUM")
    assert result["state"] == "PROVISIONAL_RANGE"
    assert result["opening_high"] == 10000 and result["opening_low"] == 9700
    assert result["opening_count"] == 5 and not result["opening_range_complete"]
    assert result["provisional_entry_ceiling"] > result["structural_support"]
    assert result["is_order_authorization"] is False
    assert analyze_opening(proof=proof,now=now,prep_style="BREAKOUT")["provisional_entry_ceiling"] is None
    assert analyze_opening(proof={**proof,"source_conflicts":["x"]},now=now)["state"] == "INVALIDATED"
    assert analyze_opening(proof=proof,now=now+timedelta(minutes=4))["state"] == "EXPIRED"
    # A future or incomplete native bar must not enlarge the opening target.
    proof["bars"].append({**rows[-1],"time":"090500","high":50000})
    assert analyze_opening(proof=proof,now=now,prep_style="MOMENTUM")["opening_high"] == 10000


def test_dynamic_promotion_only_observation_and_bounded():
    now = datetime(2026,11,27,9,10,tzinfo=KST)
    p = ObservationPromotion()
    seed = {"state":"PREP_COLLECTION_ACTIVE","hot":["1"],"warm":["2"],"cold":["3"],
            "code_sha":"a","final30_digest":"x","is_order_authorization":False}
    assert p.apply(seed,now)["hot"] == ["1"]
    p.observe("2",{"state":"PROVISIONAL_RANGE","price_near_range":True,
                   "is_order_authorization":False,"observed_at":now.isoformat()})
    assert p.apply(seed,now)["hot"] == ["2"]
    p.observe("1",{"state":"PROVISIONAL_RANGE","price_near_range":True,
                   "is_order_authorization":False,"observed_at":now.isoformat()})
    assert p.apply(seed,now+timedelta(seconds=1))["hot"] == ["2"]
    actual = {**seed,"state":"PB1_ACTIVE"}
    assert p.apply(actual,now)["hot"] == ["1"]


def test_promotion_clears_facts_on_prep_revision_even_with_same_symbols():
    now = datetime(2026,11,27,9,10,tzinfo=KST)
    p = ObservationPromotion()
    seed = {"state":"PREP_COLLECTION_ACTIVE","hot":["1"],"warm":["2"],"cold":["3"],
            "code_sha":"a","final30_digest":"x","is_order_authorization":False}
    p.apply(seed,now)
    facts = {"state":"PROVISIONAL_RANGE","price_near_range":True,
             "is_order_authorization":False,"observed_at":now.isoformat()}
    p.observe("2",facts)
    for revision in ({**seed,"final30_digest":"y"}, {**seed,"code_sha":"b"}):
        assert p.apply(revision,now)["hot"] == ["1"]
        assert not p.facts
        p.observe("2",facts)
    assert p.apply(revision,now)["hot"] == ["2"]


def test_cold_discovery_is_spare_only_bounded_and_owner_specific(monkeypatch):
    from trader.kr.opening_intelligence import ColdDiscovery
    from trader.kr.entry_priority_queue import CandidateSchedule
    now = datetime(2026,11,27,9,10,tzinfo=KST)
    seed = {"state":"PREP_COLLECTION_ACTIVE","hot":["000001"],"warm":["000002"],
            "cold":["000003","000004","000005","122630"],"code_sha":"a",
            "final30_digest":"x","is_order_authorization":False,
            "observation_styles":{"000003":"PULLBACK","000004":"BREAKOUT",
                                  "000005":"ENTRY_MOMENTUM","122630":"PULLBACK"}}
    s = CandidateSchedule()
    d = ColdDiscovery()
    assert d.select(seed,s,now) is None
    s.select({**seed,"cold":[]},now)
    assert d.select(seed,s,now+timedelta(seconds=26)) is None
    assert d.select(seed,s,now) == "000003"
    assert d.select(seed,s,now+timedelta(seconds=10)) is None
    later = now+timedelta(seconds=180)
    s.select({**seed,"cold":[]},later)
    assert d.select({**seed,"state":"PB1_ACTIVE"},s,later) is None
    assert d.select(seed,s,later) == "000005"


def test_cold_promotion_keeps_partition_and_execution_manifest(monkeypatch):
    monkeypatch.setenv("KR_COLD_DISCOVERY_ENABLED","1")
    now = datetime(2026,11,27,9,10,tzinfo=KST)
    p = ObservationPromotion()
    seed = {"state":"PREP_COLLECTION_ACTIVE","hot":["1"],"warm":["2"],
            "cold":["3","4"],"code_sha":"a","final30_digest":"x",
            "is_order_authorization":False}
    p.apply(seed,now)
    facts = {"state":"PROVISIONAL_RANGE","price_near_range":True,
             "is_order_authorization":False,"observed_at":now.isoformat()}
    p.observe("3",facts)
    result = p.apply(seed,now)
    assert result["warm"] == ["3"]
    assert set(result["cold"]) == {"2","4"}
    assert result["hot"] == ["1"]
    assert len(set(result["hot"]+result["warm"]+result["cold"])) == 4
    assert result["is_order_authorization"] is False
    p.observe("4",facts)
    assert p.apply(seed,now+timedelta(seconds=1))["warm"] == ["3"]
    assert p.apply({**seed,"state":"PB1_ACTIVE"},now)["warm"] == ["2"]
    p.apply(seed,now)
    monkeypatch.setenv("KR_COLD_DISCOVERY_ENABLED","0")
    p.observe("3",facts)
    assert p.apply(seed,now)["warm"] == ["2"]


def test_scoped_opening_incremental_matches_original_and_rechecks_freshness(tmp_path, caplog, monkeypatch):
    import logging
    from trader.kr.opening_intelligence import _opening_features
    monkeypatch.setenv("KR_OPENING_FEATURE_SHARED_ENABLED", "1")
    now = datetime(2026, 11, 27, 9, 5, tzinfo=KST)
    start = now.replace(minute=0)
    rows = [{"date":"20261127", "time":(start+timedelta(minutes=i)).strftime("%H%M%S"),
             "open":cl, "high":hi, "low":lo, "close":cl, "volume":100}
            for i,(hi,lo,cl) in enumerate([(10000,9900,9950),(9800,9700,9750),
                                          (9770,9700,9750),(9780,9710,9760),
                                          (9790,9720,9770),(10100,9700,9800)])]
    proof = {"source":"KIS_FHKST03010200","trade_date":"2026-11-27",
             "env":"practice","code":"005930","sha256":"fixture","as_of":now.isoformat(),
             "bars":rows[:5]}
    args = {"account_scope":"practice:fixture","code":"005930","cache_base":tmp_path/"private"}
    with caplog.at_level(logging.DEBUG, logger="trader.marketdata.feature_reuse"):
        first = analyze_opening(proof=proof, now=now, prep_style="MOMENTUM", **args)
        assert first == analyze_opening(proof=proof, now=now, prep_style="MOMENTUM")
        _opening_features.cache_clear()  # no process-local hit can explain shared reuse
        assert first == analyze_opening(proof=proof, now=now, prep_style="MOMENTUM", **args)
        assert "[NATIVE_FEATURES][SHARED_HIT]" in caplog.text
        caplog.clear()
        later = now+timedelta(minutes=1)
        extended = {**proof,"bars":rows,"as_of":later.isoformat()}
        second = analyze_opening(proof=extended, now=later, prep_style="MOMENTUM", **args)
        assert second == analyze_opening(proof=extended, now=later, prep_style="MOMENTUM")
        assert second["opening_high"] == 10100
        assert "[NATIVE_FEATURES][APPEND_REUSE]" in caplog.text
        caplog.clear()
        corrected = {**extended,"bars":[{**rows[0],"high":10200},*rows[1:]]}
        third = analyze_opening(proof=corrected, now=later, prep_style="MOMENTUM", **args)
        assert third == analyze_opening(proof=corrected, now=later, prep_style="MOMENTUM")
        assert third["opening_high"] == 10200
        assert "[NATIVE_FEATURES][APPEND_REUSE]" not in caplog.text
        assert analyze_opening(proof=corrected,now=later+timedelta(minutes=4),**args)["state"] == "EXPIRED"
        assert analyze_opening(proof={**corrected,"source_conflicts":["x"]},now=later,**args)["state"] == "INVALIDATED"
        monkeypatch.setenv("KR_OPENING_FEATURE_SHARED_ENABLED","0")
        assert third == analyze_opening(proof=corrected,now=later,prep_style="MOMENTUM",**args)
