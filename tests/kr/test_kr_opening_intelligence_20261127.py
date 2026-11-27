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
