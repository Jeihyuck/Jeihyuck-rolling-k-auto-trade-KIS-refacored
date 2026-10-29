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
