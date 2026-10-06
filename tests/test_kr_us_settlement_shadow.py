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
