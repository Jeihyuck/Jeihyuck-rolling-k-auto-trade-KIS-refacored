from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kis_wrapper import KisTemporaryError, is_kr_order_submit_outcome_ambiguous
from trader.kr import runtime_integrity_20260929_log_review as fix
from trader.time_utils import now_kst


def test_pre_hashkey_buy_budget_fails_retry_safe_before_public_buy(monkeypatch):
    calls = []

    def original(self, code, qty, price):
        calls.append((code, qty, price))
        return {"rt_cd": "0"}

    monkeypatch.setattr(fix, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 8.0)
    guarded = fix._build_buy_pipeline_budget_guard(original)
    with pytest.raises(KisTemporaryError, match="BEFORE_KIS_REQUEST") as caught:
        guarded(SimpleNamespace(_kr_stage_deadline=None), "039030", 2, 467500)
    assert calls == []
    assert is_kr_order_submit_outcome_ambiguous(caught.value) is False


def test_pre_hashkey_buy_budget_allows_buy_with_tail_budget(monkeypatch):
    calls = []

    def original(self, code, qty, price):
        calls.append((code, qty, price))
        return {"rt_cd": "0"}

    monkeypatch.setattr(fix, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 25.0)
    guarded = fix._build_buy_pipeline_budget_guard(original)
    assert guarded(SimpleNamespace(_kr_stage_deadline=None), "047050", 20, 56900)["rt_cd"] == "0"
    assert calls == [("047050", 20, 56900)]


def test_get_open_orders_date_object_is_normalized_to_supported_string_path():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    repo = OrdersRepo(engine)
    guarded = fix._build_get_open_orders_trade_date_guard(OrdersRepo.get_open_orders)

    # Regression for 2026-09-29 logs:
    # _has_unresolved_broker_activity passes now_kst().date().  The legacy
    # date-object path raises before issuing the query; the ISO-string path is
    # already supported by OrdersRepo and must simply return an empty list here.
    rows = guarded(repo, "practice", trade_date=now_kst().date())
    assert rows == []


def test_trade_date_datetime_is_reduced_to_date_string():
    captured = {}

    def original(self, env, *args, **kwargs):
        captured.update(kwargs)
        return []

    guarded = fix._build_get_open_orders_trade_date_guard(original)
    value = datetime(2026, 9, 29, 13, 2, 0)
    assert guarded(SimpleNamespace(), "practice", trade_date=value) == []
    assert captured["trade_date"] == "2026-09-29"


def test_typed_meta_restore_clears_policy_missing_without_raw_case_binds():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    position_id = str(uuid4())
    cycle_id = str(uuid4())
    epoch_id = str(uuid4())
    meta = {
        "book": "SWING_BOOK",
        "trade_horizon": "SWING_CARRY",
        "entry_thesis": "PULLBACK_CONTINUATION",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "eod_action": "CARRY_IF_NO_EXIT_SIGNAL",
        "force_eod_close": False,
        "policy_source": "style_mapping",
        "policy_version": "pb1_entry_exit_plan_v1",
        "entry_reason": "ENTRY_PULLBACK",
        "entry_style_selected": "ENTRY_PULLBACK",
    }
    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=position_id,
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                opened_at=now_kst(),
                position_origin="SYSTEM",
                env="practice",
                strategy="pb1_pullback_close",
                sid=1,
                mode=1,
                code="000660",
                market="KOSPI",
                qty=1,
                avg_buy_price=1844000.0,
                status="OPEN",
                entry_thesis="POLICY_MISSING",
                exit_policy_family="POLICY_MISSING",
                policy_source="missing",
                entry_meta_json=meta,
                entry_exit_plan_json={},
                position_meta={},
            )
        )

    restored = fix._typed_restore_entry_meta_for_promoted_positions(
        env="practice",
        strategy="pb1_pullback_close",
        engine=engine,
        orders_repo=None,
        positions_repo=None,
        ledger_repo=None,
    )
    assert restored == 1
    with engine.connect() as conn:
        row = conn.execute(
            sa.select(schema.positions).where(schema.positions.c.position_id == position_id)
        ).mappings().one()
    assert row["entry_thesis"] == "PULLBACK_CONTINUATION"
    assert row["trade_horizon"] == "SWING"
    assert row["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert row["policy_source"] == "style_mapping"
