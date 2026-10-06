"""Read-only shadow projection: different owners/markets never share settlement."""
from __future__ import annotations

from datetime import datetime, timezone
import sqlalchemy as sa

from trader.settlement.schema import metadata
from trader.settlement.shadow import shadow_kr_promotion, shadow_us_reconcile


def test_shadow_disabled_by_default(monkeypatch):
    monkeypatch.delenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", raising=False)
    assert shadow_kr_promotion(engine=None, order={}, request_json={},
                               cumulative_qty=2, broker_fill_price=None,
                               pre_holding_qty=None, post_holding_qty=2,
                               exclusive_order_proof=False) is None


def test_kr_shadow_is_read_only_and_proves_owner(monkeypatch):
    monkeypatch.setenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", "1")
    monkeypatch.setattr("trader.settlement.shadow._account_scope", lambda env: "scoped-account")
    engine = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    row = {
        "code": "039030", "env": "practice", "strategy": "pb1_pullback_close",
        "position_cycle_id": "cycle-kr", "trading_epoch_id": "epoch-a",
        "client_order_key": "key-a", "market": "J", "side": "BUY",
        "qty": 2, "submitted_at": datetime(2026, 10, 6, 1, 0, tzinfo=timezone.utc),
        "kis_odno": "0000010241",
    }
    result = shadow_kr_promotion(
        engine=engine, order=row, request_json={"strategy_owner": "KR_STANDARD"},
        cumulative_qty=2, broker_fill_price=None,
        pre_holding_qty=0, post_holding_qty=2, exclusive_order_proof=True,
    )
    assert result["status"] == "SHADOW_ONLY"
    assert result["owner"] == "PB1"
    assert result["proposed_qty_delta"] == 2
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT count(*) FROM settlement_applications")).scalar() == 0


def test_us_owner_incomplete_is_review_required_not_guessed(monkeypatch):
    monkeypatch.setenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", "1")
    monkeypatch.setattr("trader.settlement.shadow._account_scope", lambda env: "scoped-account")
    result = shadow_us_reconcile(
        order={"symbol": "NVDA", "client_order_key": "key-b",
               "qty_requested": 3, "side": "SELL", "exchange": "NASD"},
        trade_date="2026-10-05", cumulative_qty=3, broker_fill_price="1.2",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
    )
    assert result["status"] == "REVIEW_REQUIRED"
    assert result["reason"] == "STRATEGY_OWNER_MISSING"


def test_us_balancedelta_requires_exclusive_proof(monkeypatch):
    monkeypatch.setenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", "1")
    monkeypatch.setattr("trader.settlement.shadow._account_scope", lambda env: "scoped-account")
    row = {
        "symbol": "JNJ", "trade_date": "2026-10-05",
        "qty_requested": 3, "side": "BUY",
        "client_order_key": "key-us", "exchange": "NYSE",
        "trading_epoch_id": "epoch-a",
        "meta": {"strategy_owner": "US_STANDARD", "position_lifecycle_id": "cycle-us"},
    }
    result = shadow_us_reconcile(
        order=row, trade_date="2026-10-05", cumulative_qty=3,
        broker_fill_price=None, evidence_type="BALANCE_DELTA_SYNTHETIC",
        pre_holding_qty=0, post_holding_qty=3, exclusive_order_proof=False,
    )
    assert result["status"] == "REVIEW_REQUIRED"
    assert "ATTRIBUTION_UNPROVEN" in result["reason"]


def test_kr_execution_price_alone_never_upgrades_balance_delta_to_actual(monkeypatch):
    monkeypatch.setenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", "1")
    monkeypatch.setattr("trader.settlement.shadow._account_scope", lambda env: "scoped-account")
    row = {
        "code": "293490", "env": "practice",
        "position_cycle_id": "cycle-kr", "trading_epoch_id": "epoch-a",
        "client_order_key": "key-sell", "market": "J", "side": "SELL",
        "qty": 37, "submitted_at": datetime(2026, 10, 6, 1, 0, tzinfo=timezone.utc),
        "kis_odno": "0000012522",
    }
    # This is a holdings promotion; even if order JSON contains a broker price,
    # the holdings delta alone does not prove the per-order execution quantity.
    candidate = shadow_kr_promotion(
        engine=None, order=row, request_json={"strategy_owner": "KR_STANDARD"},
        cumulative_qty=37, broker_fill_price="10050",
        pre_holding_qty=115, post_holding_qty=78, exclusive_order_proof=True,
    )
    assert candidate["status"] == "SHADOW_ONLY"
    assert candidate["price_status"] == "PRICE_PENDING"
    blocked = shadow_kr_promotion(
        engine=None, order=row, request_json={"strategy_owner": "KR_STANDARD"},
        cumulative_qty=37, broker_fill_price="10050",
        pre_holding_qty=115, post_holding_qty=78, exclusive_order_proof=False,
    )
    assert blocked["status"] == "REVIEW_REQUIRED"
    assert "ATTRIBUTION_UNPROVEN" in blocked["reason"]


def test_kr_real_holdings_promotion_path_invokes_shadow_without_second_writer(monkeypatch):
    """Execute the existing production holdings promotion, not just our probe."""
    from datetime import datetime
    from unittest.mock import MagicMock
    from trader.reconcile_kis import _promote_open_buy_orders_from_holdings

    observed = []
    monkeypatch.setattr(
        "trader.settlement.shadow.shadow_kr_promotion",
        lambda **kwargs: observed.append(kwargs) or {"status": "REVIEW_REQUIRED"},
    )
    orders = MagicMock()
    fills = MagicMock()
    orders.get_open_orders.return_value = [{
        "order_id": "ord-kr-test", "env": "practice",
        "strategy": "pb1_pullback_close", "code": "039030", "side": "BUY",
        "status": "ACCEPTED", "qty": 2, "limit_price": 529000,
        "client_order_key": "test-kr-promotion", "kis_odno": "0000010241",
        "submitted_at": datetime(2026, 10, 6, 9, 1, 0),
        "acked_at": datetime(2026, 10, 6, 9, 1, 1),
        "request_json": {"pre_order_holding_qty": 0, "strategy_owner": "KR_STANDARD"},
        "response_json": {},
    }]
    orders.list_recent_holdings_promoted_orders_for_repair.return_value = []
    result = _promote_open_buy_orders_from_holdings(
        env="practice", strategy="pb1_pullback_close",
        ctx_run_id="dry-run", tick_ts=datetime(2026, 10, 6, 9, 3, 0),
        holdings_rows=[{"pdno": "039030", "hldg_qty": "2", "pchs_avg_pric": "529000"}],
        orders_repo=orders, fills_repo=fills,
    )
    assert result["orders"] == 1
    assert len(observed) == 1
    assert observed[0]["cumulative_qty"] == 2
    # One outstanding local order is NOT sufficient broker-wide attribution.
    assert observed[0]["exclusive_order_proof"] is False
    orders.upsert_reconciled_order.assert_called_once()
    fills.upsert_fill.assert_called_once()


def test_us_real_ack_reconcile_path_invokes_shadow_with_actual_order_evidence(monkeypatch):
    """Execute existing US reconcile API under a fake broker, never real KIS."""
    from trader.us.execution import reconcile

    monkeypatch.setenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", "1")
    sentinel_engine = object()
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: sentinel_engine)
    observed = []
    monkeypatch.setattr(
        "trader.settlement.shadow.shadow_us_reconcile",
        lambda **kwargs: observed.append(kwargs) or {"status": "SHADOW_ONLY"},
    )
    orders = [{
        "symbol": "AMAT", "side": "SELL", "order_no": "31806",
        "client_order_key": "us-ack-test", "qty_requested": 1,
        "pre_order_position_qty": 1, "status": "ACK",
        "trading_epoch_id": "epoch-a",
        "meta": {"strategy_owner": "US_STANDARD", "position_lifecycle_id": "cycle-us"},
    }]
    monkeypatch.setattr(
        "trader.us.db.repos.load_pending_ack_orders",
        lambda trade_date, env="practice": orders,
    )
    monkeypatch.setattr(
        "trader.us.db.repos.apply_broker_order_observation",
        lambda **kwargs: {"status": "OK", "authoritative": True, "requires_reconcile": False},
    )
    class FakeProvider:
        def get_balance(self, **kwargs):
            return {"positions": []}
        def get_fills_by_order_no(self, **kwargs):
            return {
                "status": "OK", "order_no": "31806", "symbol": "AMAT",
                "side": "SELL", "filled_qty": 1, "avg_price": 100,
            }
    result = reconcile.reconcile_ack_orders_with_balance(
        provider=FakeProvider(), trade_date="2026-10-06",
    )
    assert result["status"] == "OK"
    assert len(observed) == 1
    assert observed[0]["cumulative_qty"] == 1
    assert observed[0]["evidence_type"] == "KIS_ORDER_CUMULATIVE_ACTUAL"
    assert observed[0]["engine"] is sentinel_engine


def test_us_pb1_owner_is_cross_market_invalid(monkeypatch):
    monkeypatch.setenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", "1")
    monkeypatch.setattr("trader.settlement.shadow._account_scope", lambda env: "scoped-account")
    result = shadow_us_reconcile(
        order={
            "symbol": "AMAT", "env": "practice", "side": "SELL",
            "qty_requested": 1, "client_order_key": "bad-owner",
            "exchange": "NASD", "trading_epoch_id": "epoch-a",
            "meta": {"strategy_owner": "PB1", "position_lifecycle_id": "cycle-us"},
        },
        trade_date="2026-10-06", cumulative_qty=1, broker_fill_price="100",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
    )
    assert result["status"] == "REVIEW_REQUIRED"
    assert result["reason"] == "STRATEGY_OWNER_MISSING"


def test_shadow_rejects_existing_application_requested_qty_scope_conflict(monkeypatch):
    from datetime import date
    from decimal import Decimal
    from trader.settlement.core import SettlementObservation, settle_atomic

    monkeypatch.setenv("NULLIM_SETTLEMENT_SHADOW_ENABLED", "1")
    monkeypatch.setattr("trader.settlement.shadow._account_scope", lambda env: "scoped-account")
    engine = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(engine)

    obs = SettlementObservation(
        env="practice", market="US", account_scope="scoped-account",
        trading_epoch_id="epoch-a", strategy_owner="US_STANDARD",
        position_cycle_id="cycle-us", client_order_key="same-key",
        broker_trade_date=date(2026, 10, 6), exchange="NASD",
        broker_order_no="31806", side="SELL", requested_qty=1,
        cumulative_qty=1, evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest="one", currency="USD", execution_price=Decimal("100"),
    )
    settle_atomic(engine, obs, lambda conn, observation, decision: None)
    result = shadow_us_reconcile(
        engine=engine,
        order={
            "symbol": "AMAT", "env": "practice", "side": "SELL",
            "qty_requested": 2, "client_order_key": "same-key",
            "exchange": "NASD", "trading_epoch_id": "epoch-a",
            "order_no": "31806",
            "meta": {"strategy_owner": "US_STANDARD", "position_lifecycle_id": "cycle-us"},
        },
        trade_date="2026-10-06", cumulative_qty=1, broker_fill_price="100",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
    )
    assert result["status"] == "REVIEW_REQUIRED"
    assert result["reason"] == "SETTLEMENT_SCOPE_CONFLICT"
