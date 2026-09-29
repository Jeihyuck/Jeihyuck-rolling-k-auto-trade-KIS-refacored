from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

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
