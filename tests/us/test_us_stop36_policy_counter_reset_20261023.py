"""New -3% soft-stop must not inherit confirmation ticks from old -5% policy."""
from datetime import datetime, timezone


def test_approved_policy_revision_resets_stale_soft_count_once(monkeypatch):
    import trader.us.db.repos as repos
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:None)
    dt="2026-10-23"
    sym="STOP36_REVISION_COUNTER_TEST"
    now=datetime(2026,10,23,17,tzinfo=timezone.utc)
    old={"soft_stop_breach_count":49,
         "first_soft_stop_seen_at":"2026-10-23T16:00:00+00:00",
         "last_soft_stop_seen_at":"2026-10-23T16:02:00+00:00",
         "state":{"soft_stop_execution":{"soft_stop_partial_done":False},
                  "lifecycle":{"lifecycle_id":"old-life","is_open":True}}}
    assert repos.save_us_position_risk_state(sym,dt,old)
    first=repos.update_us_soft_stop_risk_state(
        symbol=sym,trade_date=dt,pnl_pct=-.031,current_price=96.9,
        now=now,soft_stop_pct=.03,policy_revision="PB1_STOP_3_6_V1"
    )
    assert first["soft_stop_breach_count"]==1
    assert first["first_soft_stop_seen_at"]==now.isoformat()
    assert first["state"]["soft_stop_policy_revision"]=="PB1_STOP_3_6_V1"
    assert first["state"]["lifecycle"]["lifecycle_id"]=="old-life"
    second=repos.update_us_soft_stop_risk_state(
        symbol=sym,trade_date=dt,pnl_pct=-.032,current_price=96.8,
        now=now,soft_stop_pct=.03,policy_revision="PB1_STOP_3_6_V1"
    )
    assert second["soft_stop_breach_count"]==2


def test_legacy_policy_without_revision_preserves_existing_count(monkeypatch):
    import trader.us.db.repos as repos
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:None)
    sym="STOP36_OLD_COUNTER_TEST"
    dt="2026-10-23"
    now=datetime(2026,10,23,17,tzinfo=timezone.utc)
    assert repos.save_us_position_risk_state(sym,dt,{"soft_stop_breach_count":3})
    state=repos.update_us_soft_stop_risk_state(
        symbol=sym,trade_date=dt,pnl_pct=-.051,current_price=94.9,
        now=now,soft_stop_pct=.05)
    assert state["soft_stop_breach_count"]==4
