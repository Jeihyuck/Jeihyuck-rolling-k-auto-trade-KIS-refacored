"""KR 2026-10-20: bounded dedupe reads and exact-order SELL evidence regressions.

No broker HTTP calls, production credentials, or live order submissions.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kr.unpriced_sell_audit import audit_kr_pending_sell_orders


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    return engine, schema


def _order(schema, *, key, code, side="BUY", status="ERROR", created_at=None, odno=None):
    return dict(
        order_id=str(uuid4()), env="practice", run_id=None,
        strategy="pb1_pullback_close", sid=1, mode=1,
        code=code, market="KRX", side=side, stage="PB1-AM",
        ord_type="LIMIT", qty=2, limit_price=10000.0,
        client_order_key=key, status=status, kis_odno=odno,
        response_json={"confirmed_fill_qty": 2, "confirmed_fill_price": None},
        created_at=created_at or datetime(2026, 10, 20, 9, 5),
    )


def test_prior_intents_batch_preserves_existing_and_retryable_states():
    engine, schema = _db()
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key="k-error", code="251970", status="ERROR"),
            _order(schema, key="k-acked", code="192820", status="SUBMITTED", odno="0000001234"),
        ])
    repo = OrdersRepo(engine)
    rows = repo.list_orders_by_client_order_keys(
        "practice", ["k-acked", "k-error", "k-none", "k-acked"]
    )
    keyed = {r["client_order_key"]: r for r in rows}
    assert set(keyed) == {"k-error", "k-acked"}
    assert keyed["k-error"]["status"] == "ERROR"
    assert keyed["k-acked"]["kis_odno"] == "0000001234"
    assert repo.list_orders_by_client_order_keys("real", ["k-error"]) == []
    assert repo.list_orders_by_client_order_keys("practice", []) == []
    engine.dispose()


def test_prior_intents_query_failure_never_fails_open(monkeypatch):
    engine, _ = _db()
    repo = OrdersRepo(engine)
    seen = []

    def broken(*args, **kwargs):
        seen.append(kwargs["fail_open"])
        raise RuntimeError("simulated DB query timeout")

    monkeypatch.setattr(repo, "_read_mappings_with_guard", broken)
    with pytest.raises(RuntimeError, match="timeout"):
        repo.list_orders_by_client_order_keys("practice", ["k-unconfirmed"])
    assert seen == [False]
    engine.dispose()


def test_pending_sell_queue_keeps_ack_and_qty_without_broker_price_unresolved():
    engine, schema = _db()
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key="sell-a", code="036540", side="SELL",
                   status="FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED", odno="0000004318"),
            _order(schema, key="sell-b", code="006400", side="SELL",
                   status="SUBMITTED", odno="0000035346"),
            _order(schema, key="buy-c", code="011200", side="BUY",
                   status="SUBMITTED", odno="0000010123"),
        ])
    report = audit_kr_pending_sell_orders(engine, env="practice", trade_date=date(2026, 10, 20))
    assert report["unsettled_sell_count"] == 2
    assert report["missing_order_number_count"] == 0
    assert report["missing_broker_price_count"] == 2
    assert {r["code"] for r in report["orders"]} == {"036540", "006400"}
    assert all(r["required_action"] == "MATCH_KIS_ORDER_NUMBER_AND_EXECUTION_PRICE" for r in report["orders"])
    engine.dispose()


def test_close_submit_then_reconcile_only_with_broker_evidence_contract():
    source = Path("trader/pb1_runner.py").read_text(encoding="utf-8")
    assert source.index('"[KR_CLOSE][POLICY][DONE]') < source.index('"[KR_CLOSE][POSTSUBMIT_RECONCILE]')
    assert 'accepted_policy_sells > 0' in source
    assert 'not parse_bool_any(dry_run, default=True)' in source
    assert '"1" if str(env_effective).lower() == "practice" else "0"' in source
    assert '"[KR_CLOSE][SELL_EVIDENCE_QUEUE]' in source


def test_buyable_dedupe_bulk_guard_preserves_fallback():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert 'with self._stage_timer("entry.prior_intents_bulk_lookup")' in source
    assert 'bulk_prior_intents_enabled and str(cf.client_order_key) in prior_intent_keys' in source
    assert 'self.orders_repo.get_order_by_client_order_key(' in source


def test_broker_order_number_scoped_identity_excludes_other_dates_and_symbols():
    engine, schema = _db()
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key="old", code="036540", side="SELL",
                   status="SUBMITTED", odno="0000004318",
                   created_at=datetime(2026, 10, 19, 9, 5)),
            _order(schema, key="same-id-different-code", code="042700", side="SELL",
                   status="SUBMITTED", odno="0000004318",
                   created_at=datetime(2026, 10, 20, 9, 5)),
            _order(schema, key="correct", code="036540", side="SELL",
                   status="SUBMITTED", odno="0000004318",
                   created_at=datetime(2026, 10, 20, 9, 6)),
        ])
    repo = OrdersRepo(engine)
    row = repo.get_order_by_kis_odno_scoped(
        "practice", "0000004318", trade_date=date(2026, 10, 20),
        code="036540", side="SELL",
    )
    assert row["client_order_key"] == "correct"
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000004318", trade_date=date(2026, 10, 21),
        code="036540", side="SELL",
    ) is None
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000004318", trade_date=date(2026, 10, 20),
        code="036540", side="BUY",
    ) is None
    engine.dispose()


def test_broker_order_number_ambiguity_is_not_attributed():
    engine, schema = _db()
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key="first", code="036540", side="SELL",
                   status="SUBMITTED", odno="0000004318"),
            _order(schema, key="second", code="036540", side="SELL",
                   status="SUBMITTED", odno="0000004318"),
        ])
    repo = OrdersRepo(engine)
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000004318", trade_date=date(2026, 10, 20),
        code="036540", side="SELL",
    ) is None
    engine.dispose()
