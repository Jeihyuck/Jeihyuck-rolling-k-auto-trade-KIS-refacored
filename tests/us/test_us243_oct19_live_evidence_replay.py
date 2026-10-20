"""Production-observed 2026-10-19 identities; no network, broker or live DB writes.

Supabase evidence: META/TMO, 86 BROKER_SUBMIT_STARTED and precisely 86
BROKER_SUBMIT_ABORTED_PRE_IO, no matching broker order numbers, 48 claims for
two lifecycles due to dynamic human SELL reason strings. This test exercises
the ACTUAL journal replay and semantic identity functions without changing
strategy trigger thresholds, quantity rules or TQQQ ownership.
"""
from __future__ import annotations

from trader.us.execution.order_journal import (
    replay_order_journal, proven_unsent_attempts,
    broker_submission_possible_for_client_key,
)
from trader.us.execution.order_router import _semantic_action_identity


def _event(key, attempt, symbol, kind, *, order_no=""):
    return {
        "event_id": f"evt-{key}-{attempt}-{kind}",
        "event_type": kind,
        "trade_date": "2026-10-19",
        "session": "afternoon",
        "session_run_id": "oct19-replay",
        "tick_id": f"tick-{attempt}",
        "client_order_key": key,
        "submit_attempt_id": attempt,
        "symbol": symbol, "side": "SELL",
        "exchange": "NASDAQ" if symbol == "META" else "NYSE",
        "qty": 2 if symbol == "META" else 3,
        "broker_order_no": order_no,
    }


def test_oct19_all_86_proven_pre_io_aborts_are_not_86_real_broker_orders(
    monkeypatch,
):
    import trader.us.execution.order_journal as journal
    # Source-day count and two stable client keys were verified against
    # production Supabase. This is a structured fact replay, not fabricated
    # historical KIS responses.
    keys = [("META", "fdfb5dcbd7a3ae68a191735e", 43),
            ("TMO", "89ad4c27715970e823db8808", 43)]
    events = []
    for symbol, key, n in keys:
        for i in range(n):
            attempt = f"historical-{symbol}-{i:03d}"
            events.append(_event(key, attempt, symbol, "BROKER_SUBMIT_STARTED"))
            events.append(_event(key, attempt, symbol, "BROKER_SUBMIT_ABORTED_PRE_IO"))
            events.append(_event(key, attempt, symbol, "JOURNAL_REPLAY_UNRESOLVED"))
    assert len(events) == 258
    assert len(proven_unsent_attempts(events)) == 86
    assert all(
        not broker_submission_possible_for_client_key(events, key)
        for _, key, _ in keys
    )
    monkeypatch.setattr(journal, "load_order_events", lambda *_a, **_kw: events)
    def forbid(*_a, **_kw):
        raise AssertionError("PRE_IO replay must not query a nonexistent broker order")
    class Provider:
        get_balance = forbid
        get_today_orders = forbid
        get_fills_by_order_no = forbid
    replayed = replay_order_journal("2026-10-19", provider=Provider())
    assert replayed["unresolved_count"] == 0, replayed
    assert replayed["broker_unknown_count"] == 0, replayed
    assert replayed["status"] == "OK", replayed
    assert replayed["events_scanned"] == 0, replayed


def test_pre_io_abort_with_actual_ack_is_never_classified_as_proven_unsent():
    key="same-client-key"
    clean=[
        _event(key,"attempt-1","META","BROKER_SUBMIT_STARTED"),
        _event(key,"attempt-1","META","BROKER_SUBMIT_ABORTED_PRE_IO"),
    ]
    actual_ack=[
        _event(key,"attempt-2","META","BROKER_SUBMIT_STARTED"),
        _event(key,"attempt-2","META","BROKER_SUBMIT_ABORTED_PRE_IO"),
        _event(key,"attempt-2","META","BROKER_ACK_RECEIVED",order_no="TRUE-KIS-123"),
    ]
    assert (key,"attempt-1") in proven_unsent_attempts(clean+actual_ack)
    assert (key,"attempt-2") not in proven_unsent_attempts(clean+actual_ack)
    assert broker_submission_possible_for_client_key(clean+actual_ack,key)


def test_oct19_meta_tmo_dynamic_reasons_are_only_two_stable_us_standard_actions(
    monkeypatch,
):
    from tests.us.test_us_route_order_claim_boundary import _route_fixture
    _route_fixture(monkeypatch, production_identity=True)
    examples=(
        ("META","1ebcff3e982d4e099002056c","TRAILING_STOP",
         [f"MAX_PROFIT=0.044 TRAIL_PCT={.080+i*.001:.3f} > 0.05" for i in range(10)]),
        ("TMO","a695b937c82ab920732025fe","GIVEBACK",
         [f"GIVEBACK_RATIO={.359+i*.005:.3f} >= 0.33" for i in range(38)]),
    )
    by_symbol={}
    for symbol,lifecycle,family,reasons in examples:
        ids=[]
        for reason in reasons:
            identity=_semantic_action_identity({
                "trade_date":"2026-10-19","side":"SELL","symbol":symbol,
                "strategy_owner":"US_STANDARD",
                "position_lifecycle_id":lifecycle,
                "exit_family":family, "reason":reason,
                "client_order_key":"oct19-"+symbol,
                "meta":{"strategy_owner":"US_STANDARD", "exit_family":family,
                        "reason":reason},
            },account_env="practice")
            assert identity.action==family
            ids.append(identity.action_key)
        assert len(set(ids))==1
        by_symbol[symbol]=ids[0]
    assert len(set(by_symbol.values()))==2


def test_explicit_profit_stage_and_tqqq_owner_remain_independent(monkeypatch):
    from tests.us.test_us_route_order_claim_boundary import _route_fixture
    _route_fixture(monkeypatch, production_identity=True)
    def action(owner, family, *, stage=None, reason="floating=0.50"):
        meta={"exit_family":family,"reason":reason}
        if stage:
            meta["profit_capture_stage"]=stage
        return _semantic_action_identity({
            "trade_date":"2026-10-19","side":"SELL","symbol":"META",
            "strategy_owner":owner, "position_lifecycle_id":"same-life",
            "exit_family":family,"reason":reason,"meta":meta,
        },account_env="practice")
    tp1=action("US_STANDARD","TAKE_PROFIT",stage="TP1")
    tp2=action("US_STANDARD","TAKE_PROFIT",stage="TP2")
    tqqq=action("TQQQ_INFINITE","TRAILING_STOP")
    standard=action("US_STANDARD","TRAILING_STOP")
    assert len({tp1.action_key,tp2.action_key,tqqq.action_key,standard.action_key})==4
    assert tqqq.action=="floating=0.50"
