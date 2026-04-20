from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.pb1_engine import PB1Engine


def _entry_meta() -> dict:
    return {
        "trace_id": "trace-1",
        "entry_reason": "ENTRY_BREAKOUT",
        "entry_style_selected": "BREAKOUT",
        "entry_decision_family": "SETUP_OVERRIDE",
        "entry_rule_version": "pb1_entry_reason_v1",
        "score_final_at_entry": 97.2,
        "stop_price_at_entry": 346214.29,
        "pivot_price_at_entry": 474000.0,
        "exit_policy_family": "BREAKOUT_EXIT",
    }


def test_orders_fills_positions_persist_entry_metadata() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    positions_repo = PositionsRepo(engine)
    entry_meta = _entry_meta()

    order_id, created = orders_repo.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="009150",
        market="KOSPI",
        side="BUY",
        ord_type="LIMIT",
        qty=1,
        limit_price=474000.0,
        stage="PB1-CLOSE",
        client_order_key="key-1",
        request_json={"foo": "bar"},
        entry_meta_json=entry_meta,
    )
    assert created is True

    orders_repo.mark_submitted("practice", "key-1", "odno-1", {"rt_cd": "0"}, entry_meta_json=entry_meta)
    orders_repo.mark_acked("practice", "odno-1", {"rt_cd": "0"}, entry_meta_json=entry_meta)

    fills_repo.upsert_fill(
        env="practice",
        run_id=None,
        order_id=order_id,
        kis_odno="odno-1",
        trade_id="trade-1",
        code="009150",
        market="KOSPI",
        side="BUY",
        qty=1,
        price=474000.0,
        fee=0.0,
        tax=0.0,
        filled_at=datetime.utcnow(),
        raw_json={"rt_cd": "0"},
        fill_meta_json=entry_meta,
    )
    positions_repo.apply_fill(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="009150",
        market="KOSPI",
        side="BUY",
        qty=1,
        price=474000.0,
        fee=0.0,
        tax=0.0,
        filled_at=datetime.utcnow(),
        entry_meta_json=entry_meta,
    )
    positions_repo.update_position_fields(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="009150",
        fields={
            "entry_reason": entry_meta["entry_reason"],
            "entry_style_selected": entry_meta["entry_style_selected"],
            "entry_decision_family": entry_meta["entry_decision_family"],
            "entry_rule_version": entry_meta["entry_rule_version"],
            "entry_meta_json": entry_meta,
            "stop_price_at_entry": entry_meta["stop_price_at_entry"],
            "pivot_price_at_entry": entry_meta["pivot_price_at_entry"],
            "exit_policy_family": entry_meta["exit_policy_family"],
        },
    )

    with engine.connect() as conn:
        order_row = conn.execute(sa.select(schema.orders)).mappings().first()
        fill_row = conn.execute(sa.select(schema.fills)).mappings().first()
        position_row = conn.execute(sa.select(schema.positions)).mappings().first()

    assert order_row["entry_reason"] == "ENTRY_BREAKOUT"
    assert fill_row["entry_decision_family"] == "SETUP_OVERRIDE"
    assert position_row["entry_reason"] == "ENTRY_BREAKOUT"
    assert position_row["stop_price_at_entry"] == entry_meta["stop_price_at_entry"]
    assert position_row["pivot_price_at_entry"] == entry_meta["pivot_price_at_entry"]
    assert position_row["entry_meta_json"]["trace_id"] == "trace-1"


def test_exit_family_dispatch_uses_entry_reason() -> None:
    assert PB1Engine._resolve_exit_family("ENTRY_BREAKOUT", None) == ("ENTRY_BREAKOUT", "BREAKOUT_EXIT")
    assert PB1Engine._resolve_exit_family("ENTRY_PULLBACK", None) == ("ENTRY_PULLBACK", "PULLBACK_EXIT")
    assert PB1Engine._resolve_exit_family("ENTRY_MOMENTUM", None) == ("ENTRY_MOMENTUM", "MOMENTUM_EXIT")
    assert PB1Engine._resolve_exit_family(None, None) == ("ENTRY_GENERIC", "GENERIC_EXIT")


def test_entry_identity_uses_single_source_when_reason_missing() -> None:
    engine = PB1Engine.__new__(PB1Engine)

    identity = engine._resolve_entry_identity_from_mapping(
        {
            "entry_style_selected": "momentum",
            "entry_decision_family": "ENTRY_MOMENTUM_CONTINUATION",
        }
    )

    assert identity["entry_reason"] == "ENTRY_MOMENTUM"
    assert identity["entry_style_selected"] == "ENTRY_MOMENTUM"
    assert identity["entry_decision_family"] == "ENTRY_MOMENTUM_CONTINUATION"
    assert identity["exit_policy_family"] == "MOMENTUM_EXIT"