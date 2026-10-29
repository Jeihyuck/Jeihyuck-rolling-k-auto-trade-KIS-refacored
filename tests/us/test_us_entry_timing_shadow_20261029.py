import pytest

from trader.us.entry_timing_shadow import evaluate_entry_timing_shadow as evaluate

@pytest.mark.parametrize("family,facts", [
    ("ENTRY_PULLBACK", {"support_reaction_confirmed": True}),
    ("ENTRY_MOMENTUM", {"intraday_continuation_confirmed": True, "volume_confirmation": True}),
    ("ENTRY_BREAKOUT", {"pivot_breakout_confirmed": True}),
    ("ENTRY_VCP", {"vcp_live_breakout_verified": True}),
])
def test_all_families_observation_only(family, facts):
    record = evaluate(owner="US_STANDARD", style=family, prep_eligible=True,
                      live_facts={"quote_fresh": True, **facts})
    assert record["timing_state"] == "BUY_READY"
    assert record["may_authorize_order"] is False

def test_unverified_intraday_data_waits_in_shadow():
    record = evaluate(owner="US_STANDARD", style="ENTRY_MOMENTUM",
                      prep_eligible=True, live_facts={"quote_fresh": True})
    assert record["timing_state"] == "WAIT"
    assert record["reason"] == "intraday_evidence_unknown"

def test_stale_and_owner_isolation():
    assert evaluate(owner="US_STANDARD", style="ENTRY_PULLBACK", prep_eligible=True,
                    live_facts={})["reason"] == "fresh_quote_not_proven"
    assert evaluate(owner="TQQQ_INFINITE", style="ENTRY_PULLBACK", prep_eligible=True,
                    live_facts={"quote_fresh": True})["timing_state"] == "NOT_EVALUATED"

def test_pullback_does_not_require_vwap():
    record = evaluate(owner="US_STANDARD", style="ENTRY_PULLBACK", prep_eligible=True,
                      live_facts={"quote_fresh": True, "support_reaction_confirmed": True,
                                  "above_vwap": False})
    assert record["timing_state"] == "BUY_READY"

def test_derived_freshness_requires_actual_timestamped_kis_quote():
    from trader.us.entry_timing_shadow import derive_quote_freshness
    assert not derive_quote_freshness({"source": "KIS_CACHE", "last": 100,
                                       "age_sec": 0}, now_epoch=1000)["quote_fresh"]
    assert not derive_quote_freshness({"source": "PREP", "last": 100,
                                       "asof_epoch": 999}, now_epoch=1000)["quote_fresh"]
    fresh = derive_quote_freshness({"source": "KIS_LIVE", "last": 100,
                                   "asof_epoch": 998}, now_epoch=1000)
    assert fresh["quote_fresh"] is True
    assert fresh["age_sec"] == 2.0

def test_future_and_expired_quotes_are_not_proof():
    from trader.us.entry_timing_shadow import derive_quote_freshness
    for timestamp in [900, 1005]:
        evidence = derive_quote_freshness({"source": "KIS_LIVE", "last": 100,
                                           "asof_epoch": timestamp}, now_epoch=1000)
        assert not evidence["quote_fresh"]

def test_real_ws_collector_freshness_requires_connection_generation():
    from trader.us.entry_timing_shadow import derive_quote_freshness
    quote = {"source": "KIS_WEBSOCKET_COLLECTOR", "last": 100.0,
             "asof_epoch": 998.0, "quality": "fresh"}
    result = derive_quote_freshness(quote, now_epoch=1000.0)
    assert result["quote_fresh"] is False
    assert result["quote_reason"] == "collector_generation_missing"
    quote["collector_generation"] = "gen-1"
    assert derive_quote_freshness(quote, now_epoch=1000.0)["quote_fresh"] is True
    assert derive_quote_freshness({
        "source": "KIS_WEBSOCKET", "last": 100, "asof_epoch": 999,
        "quality": "fresh",
    }, now_epoch=1000)["quote_fresh"] is True
