"""Broker replay memo identity, clock bridge and partial-cancel regression."""
from __future__ import annotations

import pytest

from trader.us.execution.order_journal import _replay_order_row_matches


@pytest.fixture
def proof():
    journal = {
        "symbol": "MCK", "side": "SELL", "exchange": "NASDAQ",
        "market": "US", "session": "am", "session_run_id": "session-1",
        "session_generation": 1, "account_id": "account-1",
        "trading_epoch_id": "epoch-1", "strategy_owner": "US_STANDARD",
    }
    order = {
        "order_no": "0000043251", "symbol": "MCK", "side": "SELL",
        "requested_qty": 2, "filled_qty": 1, "remaining_qty": 0,
        "status": "CANCELLED", "normalization_result": "normalized",
        "exchange": "NASD", "market": "US", "session": "am",
        "session_run_id": "session-1", "session_generation": 1,
        "account_id": "account-1", "trading_epoch_id": "epoch-1",
        "strategy_owner": "US_STANDARD",
        "trade_date": "20261014", "us_trade_date": "2026-10-13",
    }
    return journal, order


def _matches(journal, order):
    return _replay_order_row_matches(
        order, order_no="0000043251", symbol="MCK", side="SELL",
        requested_qty=2, source_trade_date="2026-10-13", journal_event=journal,
    )


def test_valid_partial_fill_then_cancel_and_kst_date_bridge(proof):
    journal, order = proof
    assert _matches(journal, order)


@pytest.mark.parametrize("field,value", [
    ("requested_qty", 3), ("filled_qty", 3), ("remaining_qty", 2),
    ("account_id", "other-account"), ("market", "KR"),
    ("session", "afternoon"), ("session_run_id", "different-session"),
    ("session_generation", 2), ("trading_epoch_id", "other-epoch"),
    ("strategy_owner", "TQQQ_INFINITE"), ("exchange", "NYSE"),
    ("us_trade_date", "2026-10-12"), ("trade_date", "20261015"),
])
def test_conflicting_or_overflowing_broker_identity_forces_fallback(proof, field, value):
    journal, order = proof
    order[field] = value
    assert not _matches(journal, order)


@pytest.mark.parametrize("field", ["requested_qty", "filled_qty"])
def test_incomplete_quantity_proof_is_never_reused(proof, field):
    journal, order = proof
    del order[field]
    assert not _matches(journal, order)


def test_submit_cancel_and_late_fill_require_fresh_replay_snapshot(monkeypatch):
    import trader.us.db.repos as repos
    import trader.us.execution.fills as fills
    import trader.us.execution.order_journal as journal

    trade_date = "2026-10-13"
    events = [
        {"event_type": "BROKER_SUBMIT_STARTED", "event_id": "a",
         "trade_date": trade_date, "client_order_key": "key-MCK",
         "submit_attempt_id": "attempt-1", "symbol": "MCK",
         "side": "SELL", "exchange": "NASDAQ", "session": "am", "qty": 2},
        {"event_type": "BROKER_ACK_RECEIVED", "event_id": "b",
         "trade_date": trade_date, "client_order_key": "key-MCK",
         "submit_attempt_id": "attempt-1", "symbol": "MCK",
         "side": "SELL", "exchange": "NASDAQ", "session": "am",
         "qty": 2, "broker_order_no": "0000043251"},
    ]
    monkeypatch.setattr(journal, "load_order_events", lambda date, session_run_id=None: events)
    monkeypatch.setattr(journal, "append_order_event", lambda *a, **kw: {})
    monkeypatch.setattr(repos, "save_order_ack", lambda *a, **kw: True)
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(fills, "get_fills_today",
                        lambda provider, trade_date: {"status": "OK", "fills": []})
    observations = []
    def save_broker(**kwargs):
        observations.append(kwargs)
        return {"status": "OK"}
    monkeypatch.setattr(repos, "apply_broker_order_observation", save_broker)

    class Provider:
        stage = 0
        reads = 0
        def get_balance(self, force_refresh=False):
            return {"positions": []}
        def get_order_lookup_provenance(self, trade_date):
            return {"verified": True, "source": "kis_authenticated_order_book",
                    "query_trade_date": trade_date, "market": "US",
                    "account_id": "verified-practice-account", "session": "am"}

        def get_today_orders(self, *, trade_date):
            self.reads += 1
            return [{
                "order_no": "0000043251", "symbol": "MCK", "side": "SELL",
                "exchange": "NASD", "trade_date": "20261013",
                "requested_qty": 2, "filled_qty": (0 if self.stage == 0 else 1),
                "remaining_qty": (2 if self.stage == 0 else 0),
                "status": ("OPEN" if self.stage == 0 else "CANCELLED"),
                "normalization_result": "normalized", "avg_price": 100.0,
            }]
        def get_fills_by_order_no(self, **kwargs):
            pytest.fail("validated KIS order book must not need a duplicate request")
    provider = Provider()
    first = journal.replay_order_journal(trade_date, provider=provider)
    assert first["broker_unfilled_ack_count"] == 1
    assert provider.reads == 1
    provider.stage = 1   # Broker cancel/partial fill arrived after previous observation.
    second = journal.replay_order_journal(trade_date, provider=provider)
    assert provider.reads == 2
    assert second["broker_partial_fill_count"] == 1
    assert observations[-1]["requested_qty"] == 2
    assert observations[-1]["filled_qty"] == 1
    assert observations[-1]["remaining_qty"] == 0
    assert observations[-1]["broker_status"] == "CANCELLED"


def test_absent_kis_row_account_session_and_date_require_proven_scope(proof):
    journal, row = proof
    # A row containing order number/symbol/quantity but no context is NOT a
    # verified account/session/date match for reusing a KIS snapshot.
    for name in ("account_id", "session", "trade_date", "us_trade_date"):
        row.pop(name, None)
    assert not _matches(journal, row)
    proven = {"verified": True, "source": "kis_authenticated_order_book",
              "query_trade_date": "2026-10-13", "market": "US",
              "account_id": "account-1", "session": "am",
              "session_run_id": "session-1", "session_generation": 1}
    assert _replay_order_row_matches(
        row, order_no="0000043251", symbol="MCK", side="SELL",
        requested_qty=2, source_trade_date="2026-10-13",
        journal_event=journal, lookup_context=proven)
    for field, value in (("account_id", "wrong-account"),
                         ("query_trade_date", "2026-10-12")):
        wrong = dict(proven, **{field: value})
        assert not _replay_order_row_matches(
            row, order_no="0000043251", symbol="MCK", side="SELL",
            requested_qty=2, source_trade_date="2026-10-13",
            journal_event=journal, lookup_context=wrong)


def test_broker_detail_fallback_rejects_cross_account_and_source_date(proof):
    from trader.us.execution.order_journal import _replay_detail_identity_conflicts
    journal, row = proof
    for key, value in (("account_id", "different-account"),
                       ("market", "KR"), ("session", "afternoon"),
                       ("trade_date", "20261016"),
                       ("requested_qty", 3)):
        conflicting = dict(row, **{key: value})
        assert _replay_detail_identity_conflicts(
            conflicting, order_no="0000043251", symbol="MCK", side="SELL",
            requested_qty=2, source_trade_date="2026-10-13",
            journal_event=journal, lookup_context={},
        ), key


def test_historical_order_is_not_falsely_rejected_by_current_session(proof):
    """A new tick in afternoon may reconcile yesterday's original AM order."""
    immutable, order = proof
    for name in ("session", "session_run_id", "session_generation",
                 "trading_epoch_id", "trade_date", "account_id"):
        order.pop(name, None)
    context = {"verified": True, "source": "kis_authenticated_order_book",
               "query_trade_date": "2026-10-13", "market": "US",
               "account_id": "account-1",
               "session": "afternoon", "session_run_id": "current-run",
               "session_generation": 8}
    assert _replay_order_row_matches(
        order, order_no="0000043251", symbol="MCK", side="SELL",
        requested_qty=2, source_trade_date="2026-10-13",
        journal_event=immutable, lookup_context=context,
    )
    order["session_run_id"] = "wrong-original-run"
    assert not _replay_order_row_matches(
        order, order_no="0000043251", symbol="MCK", side="SELL",
        requested_qty=2, source_trade_date="2026-10-13",
        journal_event=immutable, lookup_context=context,
    )


def test_broker_raw_cano_and_hashed_claim_identity_agree_in_both_paths():
    """Same account => memo + detail agree; other account/env/product => deny."""
    import hashlib
    from trader.account_state import get_account_key
    from trader.us.execution.order_journal import _replay_detail_identity_conflicts
    cano, product = "12345678", "01"
    def scope(env="practice", c=cano, p=product):
        digest = hashlib.sha256(get_account_key(env=env, cano=c, product_code=p).encode()).hexdigest()
        return {"verified": True, "source": "kis_authenticated_order_book",
                "query_trade_date": "2026-10-13", "market": "US",
                "broker_account_no": c, "account_id": digest,
                "account_product_code": p, "env": env}
    valid = scope()
    journal = {"symbol": "MCK", "side": "SELL", "exchange": "NASDAQ",
               "account_id": valid["account_id"], "market": "US",
               "session": "am", "submit_attempt_id": "attempt-1",
               "trade_date": "2026-10-13", "qty": 2}
    row = {"order_no": "0000043251", "symbol": "MCK", "side": "SELL",
           "exchange": "NASD", "account_id": cano, "requested_qty": 2,
           "filled_qty": 1, "remaining_qty": 0, "status": "CANCELLED",
           "trade_date": "20261014"}
    def memo(ctx):
        return _replay_order_row_matches(
            row, order_no="0000043251", symbol="MCK", side="SELL",
            requested_qty=2, source_trade_date="2026-10-13",
            journal_event=journal, lookup_context=ctx,
        )
    def conflict(ctx):
        return _replay_detail_identity_conflicts(
            row, order_no="0000043251", symbol="MCK", side="SELL",
            requested_qty=2, source_trade_date="2026-10-13",
            journal_event=journal, lookup_context=ctx,
        )
    assert memo(valid) and not conflict(valid)
    for invalid in (scope(c="87654321"), scope(env="prod"), scope(p="02")):
        assert not memo(invalid), invalid
        assert conflict(invalid), invalid


def test_old_attempt_late_actual_fill_survives_new_independent_intent(monkeypatch):
    import trader.us.execution.order_journal as journal
    import trader.us.execution.fills as fills
    import trader.us.db.repos as repos
    td = "2026-10-13"
    events = []
    for attempt, num in (("older", "0000001001"), ("newer", "0000001002")):
        base = {"trade_date": td, "client_order_key": "key-" + attempt,
                "submit_attempt_id": attempt, "symbol": "AMD", "side": "SELL",
                "session": "am", "exchange": "NASDAQ", "qty": 1,
                "limit_price": 25.0}
        events.append(dict(base, event_type="BROKER_SUBMIT_STARTED", event_id=attempt + "-start"))
        events.append(dict(base, event_type="BROKER_ACK_RECEIVED",
                           event_id=attempt + "-ack", broker_order_no=num))
    monkeypatch.setattr(journal, "load_order_events", lambda day, session_run_id=None: events)
    monkeypatch.setattr(journal, "append_order_event", lambda *args, **kw: {})
    monkeypatch.setattr(repos, "save_order_ack", lambda *args, **kw: True)
    monkeypatch.setattr(repos, "load_active_execution_claim_attempts", lambda: [])
    monkeypatch.setattr(fills, "get_fills_today",
                        lambda provider, trade_date: {"status": "OK", "fills": []})
    recorded = []
    monkeypatch.setattr(repos, "apply_broker_order_observation",
                        lambda **kwargs: (recorded.append(kwargs) or {"status": "OK"}))
    class Provider:
        stage = 0
        reads = 0
        def get_balance(self, force_refresh=False):
            return {"positions": []}
        def get_order_lookup_provenance(self, date):
            return {"verified": True, "source": "kis_authenticated_order_book",
                    "query_trade_date": date, "market": "US", "account_id": "acct"}
        def get_today_orders(self, trade_date):
            self.reads += 1
            return [{"order_no": number, "symbol": "AMD", "side": "SELL",
                     "exchange": "NASD", "requested_qty": 1,
                     "filled_qty": (1 if attempt == "older" and self.stage else 0),
                     "remaining_qty": (0 if attempt == "older" and self.stage else 1),
                     "status": ("FILLED" if attempt == "older" and self.stage else "OPEN"),
                     "trade_date": "20261014", "avg_price": 25.0}
                    for attempt, number in (("older", "0000001001"),
                                             ("newer", "0000001002"))]
        def get_fills_by_order_no(self, **kwargs):
            pytest.fail("two exact broker order rows must be reused without extra REST")
    provider = Provider()
    first = journal.replay_order_journal(td, provider=provider)
    assert first["broker_unfilled_ack_count"] == 2, first
    provider.stage = 1
    recorded.clear()
    second = journal.replay_order_journal(td, provider=provider)
    assert provider.reads == 2
    assert second["broker_full_fill_count"] == 1, second
    assert second["broker_unfilled_ack_count"] == 1, second
    assert {(x["raw_order_no"], x["filled_qty"]) for x in recorded} == {
        ("0000001001", 1), ("0000001002", 0),
    }


def test_same_client_key_two_real_broker_orders_preserves_proof_and_fences(monkeypatch):
    """Never corrupt one-key us_orders by writing two different KIS order numbers."""
    import trader.us.execution.order_journal as journal
    import trader.us.execution.fills as fills
    import trader.us.db.repos as repos
    td = "2026-10-13"
    events = []
    for attempt, order_no in (("old", "0000010201"), ("new", "0000010202")):
        base = {"trade_date": td, "client_order_key": "same-key",
                "submit_attempt_id": attempt, "symbol": "AMD", "side": "SELL",
                "qty": 1, "session": "am", "exchange": "NASDAQ"}
        events += [dict(base, event_id=attempt+"-start",
                        event_type="BROKER_SUBMIT_STARTED"),
                   dict(base, event_id=attempt+"-ack",
                        event_type="BROKER_ACK_RECEIVED", broker_order_no=order_no)]
    monkeypatch.setattr(journal, "load_order_events", lambda *_a, **_kw: events)
    recorded_events = []
    monkeypatch.setattr(journal, "append_order_event",
                        lambda kind, intent, **kwargs:
                        (recorded_events.append((kind, kwargs)) or {}))
    writes = []
    monkeypatch.setattr(repos, "save_order_ack",
                        lambda *a, **k: (writes.append(("ACK", a, k)) or True))
    monkeypatch.setattr(repos, "apply_broker_order_observation",
                        lambda **kw: (writes.append(("OBS", kw)) or {"status": "OK"}))
    monkeypatch.setattr(fills, "get_fills_today",
                        lambda provider, trade_date: {"status": "OK", "fills": []})
    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}
        def get_order_lookup_provenance(self, trade_date):
            return {"verified": True, "source": "kis_authenticated_order_book",
                    "query_trade_date": trade_date, "market": "US", "account_id": "acct"}
        def get_today_orders(self, trade_date):
            return [
                {"order_no": "0000010201", "status": "FILLED", "symbol": "AMD",
                 "side": "SELL", "requested_qty": 1, "filled_qty": 1,
                 "remaining_qty": 0, "trade_date": "20261014"},
                {"order_no": "0000010202", "status": "OPEN", "symbol": "AMD",
                 "side": "SELL", "requested_qty": 1, "filled_qty": 0,
                 "remaining_qty": 1, "trade_date": "20261014"},
            ]
    result = journal.replay_order_journal(td, provider=Provider())
    assert not writes, "same client key cannot represent distinct broker orders in us_orders"
    assert result["broker_unknown_count"] == 2, result
    assert result["status"] == "UNRESOLVED", result
    evidence = [details for kind, details in recorded_events
                if kind == "JOURNAL_REPLAY_MULTI_ATTEMPT_BROKER_EVIDENCE"]
    assert evidence and any(
        "0000010201" in str(row["broker_order_no"]) and
        row["raw_response"]["cumulative_filled_qty"] == 1 for row in evidence
    )
    assert any(kind == "JOURNAL_REPLAY_UNRESOLVED" for kind, _ in recorded_events)


@pytest.mark.parametrize("active_only", [False, True])
def test_multi_order_collision_actual_ledger_and_replay_survives_restarts(
        tmp_path, monkeypatch, active_only):
    """Use REAL journal writer, JSONL and persisted DB JSONB payload contract."""
    import json
    import trader.us.execution.order_journal as journal
    import trader.us.execution.fills as fills
    import trader.us.db.repos as repos

    original_date, current_date = "2026-10-13", "2026-10-14"
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path / "journal"))
    monkeypatch.setenv("US_OFFLINE", "1")
    store = {}
    def save_event(row):
        key = row["idempotency_key"]
        store.setdefault(key, dict(row))
        return True
    def read_events(day, session_run_id=None):
        result = []
        for row in store.values():
            if row["trade_date"] != day:
                continue
            item = {**row, **row["payload"]}
            if not session_run_id or item.get("session_run_id") == session_run_id:
                result.append(item)
        return result
    monkeypatch.setattr(repos, "append_us_order_event", save_event)
    monkeypatch.setattr(repos, "load_us_order_events", read_events)
    monkeypatch.setattr(
        repos, "load_active_execution_claim_attempts",
        lambda: [{"trade_date": original_date,
                  "client_order_key": "one-claim",
                  "active_attempt_id": "new"}],
    )
    def never_mutate(*args, **kwargs):
        pytest.fail("conflicting KIS orders must not mutate us_orders or clear a claim")
    monkeypatch.setattr(repos, "save_order_ack", never_mutate)
    monkeypatch.setattr(repos, "apply_broker_order_observation", never_mutate)
    monkeypatch.setattr(repos, "mark_order_filled_by_reconcile", never_mutate)
    monkeypatch.setattr(
        fills, "get_fills_today",
        lambda provider, trade_date: {"status": "OK", "fills": []},
    )
    for attempt, order_no in (("old", "0000010201"), ("new", "0000010202")):
        intent = {"trade_date": original_date, "client_order_key": "one-claim",
                  "submit_attempt_id": attempt, "symbol": "AMD", "side": "SELL",
                  "qty": 1, "session": "am", "exchange": "NASDAQ"}
        journal.append_order_event("BROKER_SUBMIT_STARTED", intent)
        journal.append_order_event(
            "BROKER_ACK_RECEIVED", intent, broker_order_no=order_no,
            broker_status="ACK",
        )
    class Provider:
        reads = 0
        source_reads = 0
        def get_balance(self, force_refresh=False):
            return {"positions": []}
        def get_order_lookup_provenance(self, trade_date):
            return {"verified": True, "source": "kis_authenticated_order_book",
                    "query_trade_date": trade_date, "market": "US",
                    "account_id": "verified-account-sha",
                    "account_product_code": "01", "env": "practice"}
        def get_today_orders(self, trade_date):
            self.reads += 1
            if trade_date != original_date:
                return []
            self.source_reads += 1
            # A later observation timestamp with UNCHANGED cumulative qty
            # is the same broker truth, not a second fill.
            old_seen_at = ("2026-10-14T01:31:00Z" if self.source_reads == 1
                           else "2026-10-14T01:35:00Z")
            return [
                {"order_no": "0000010201", "status": "FILLED",
                 "symbol": "AMD", "side": "SELL", "requested_qty": 1,
                 "filled_qty": 1, "remaining_qty": 0,
                 "avg_price": 23.75, "observed_at": old_seen_at,
                 "trade_date": "20261014"},
                {"order_no": "0000010202", "status": "OPEN",
                 "symbol": "AMD", "side": "SELL", "requested_qty": 1,
                 "filled_qty": 0, "remaining_qty": 1,
                 "avg_price": 24.0, "observed_at": "2026-10-14T01:32:00Z",
                 "trade_date": "20261014"},
            ]
    provider = Provider()
    for _ in range(2):
        result = journal.replay_order_journal(
            current_date if active_only else original_date,
            provider=provider, include_active_claims=active_only,
            active_claims_only=active_only,
        )
        assert result["status"] == "UNRESOLVED", result
        assert result["db_ack_restored_count"] == 0
        assert result["broker_full_fill_count"] == 0
        assert result["unresolved_symbol_sides"]
    proof_type = "JOURNAL_REPLAY_MULTI_ATTEMPT_BROKER_EVIDENCE"
    db_proofs = [v["payload"] for v in store.values() if v["event_type"] == proof_type]
    assert len(db_proofs) == 2, db_proofs
    assert {e["submit_attempt_id"] for e in db_proofs} == {"old", "new"}
    assert {e["broker_order_no"] for e in db_proofs} == {
        "0000010201", "0000010202",
    }
    filled = next(e for e in db_proofs if e["submit_attempt_id"] == "old")
    opened = next(e for e in db_proofs if e["submit_attempt_id"] == "new")
    assert filled["broker_evidence"]["cumulative_filled_qty"] == 1
    assert filled["broker_evidence"]["remaining_qty"] == 0
    assert filled["broker_evidence"]["avg_price"] == 23.75
    assert filled["broker_evidence"]["observed_at"] == "2026-10-14T01:31:00Z"
    assert filled["broker_evidence"]["lookup_scope"]["account_id"] == "verified-account-sha"
    assert opened["broker_evidence"]["cumulative_filled_qty"] == 0
    assert opened["broker_evidence"]["remaining_qty"] == 1
    assert opened["broker_evidence"]["avg_price"] == 24.0
    assert all(e["raw_response_hash"] and e["idempotency_key"] for e in db_proofs)
    assert all("broker_account_no" not in json.dumps(e) for e in db_proofs)

    # DB ledger and JSONL both carry the proof. Duplicate deliveries cannot
    # be counted as new fills, even with a new per-event UUID/timestamp.
    raw_jsonl = (tmp_path / "journal" / f"{original_date}.jsonl").read_text()
    jsonl_proofs = [
        json.loads(line) for line in raw_jsonl.splitlines()
        if json.loads(line)["event_type"] == proof_type
    ]
    assert len(jsonl_proofs) >= 2
    assert {x["idempotency_key"] for x in jsonl_proofs} == {
        x["idempotency_key"] for x in db_proofs
    }
    monkeypatch.setattr(repos, "load_us_order_events", lambda *a, **kw: None)
    deduped = journal.load_order_events(original_date)
    assert sum(e["event_type"] == proof_type for e in deduped) == 2
