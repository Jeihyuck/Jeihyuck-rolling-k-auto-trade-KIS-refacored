from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.kr import runtime_integrity_20260929 as fix
from trader.kis_wrapper import KisTemporaryError
from trader.time_utils import now_kst

STRATEGY = "pb1_pullback_close"


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _portfolio_epoch(conn, schema, epoch_id: str):
    conn.execute(
        sa.insert(schema.portfolio_epochs).values(
            portfolio_epoch_id=epoch_id,
            env="practice",
            account_id="practice:test",
            sid=1,
            mode=1,
            strategy=STRATEGY,
            status="ACTIVE",
        )
    )


def _plan():
    return {
        "entry_thesis": "PULLBACK_CONTINUATION",
        "entry_reason": "ENTRY_PULLBACK",
        "entry_style_selected": "ENTRY_PULLBACK",
        "trade_horizon": "SWING",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "eod_action": "CARRY_IF_NO_EXIT_SIGNAL",
        "force_eod_close": False,
        "policy_source": "style_mapping",
        "policy_version": "pb1_entry_exit_plan_v1",
        "risk_plan": {"initial_stop": 43573.21428571428, "risk_R": 5776.785714285717},
        "time_plan": {"max_trading_days": 10},
    }


def _meta():
    return {
        "entry_reason": "ENTRY_PULLBACK",
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_decision_family": "ENTRY_PULLBACK_OVERRIDE",
        "entry_thesis": "PULLBACK_CONTINUATION",
        "trade_horizon": "SWING",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "initial_stop_price": 43573.21428571428,
        "tp1_done": False,
        "tp2_done": False,
    }


def test_buy_submit_budget_gate_fails_before_http(monkeypatch):
    calls = []

    def original(self, method, url, *args, **kwargs):
        calls.append((method, url))
        return {"rt_cd": "0"}

    monkeypatch.setattr(fix, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 8.0)
    wrapped = fix._build_order_submit_budget_guard(original)
    with pytest.raises(KisTemporaryError, match="BEFORE_KIS_REQUEST"):
        wrapped(
            SimpleNamespace(_kr_stage_deadline=None),
            "POST",
            "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash",
            headers={"tr_id": "VTTC0012U"},
        )
    assert calls == []


def test_buy_submit_budget_gate_never_blocks_sell(monkeypatch):
    calls = []

    def original(self, method, url, *args, **kwargs):
        calls.append((method, url))
        return {"rt_cd": "0"}

    monkeypatch.setattr(fix, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 1.0)
    wrapped = fix._build_order_submit_budget_guard(original)
    result = wrapped(
        SimpleNamespace(_kr_stage_deadline=None),
        "POST",
        "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash",
        headers={"tr_id": "VTTC0011U"},
    )
    assert result["rt_cd"] == "0"
    assert len(calls) == 1


def test_buy_submit_budget_gate_does_not_block_data_endpoint(monkeypatch):
    calls = []

    def original(self, method, url, *args, **kwargs):
        calls.append((method, url))
        return {"rt_cd": "0"}

    monkeypatch.setattr(fix, "kr_tick_remaining_sec", lambda *_args, **_kwargs: 1.0)
    wrapped = fix._build_order_submit_budget_guard(original)
    result = wrapped(
        SimpleNamespace(_kr_stage_deadline=None),
        "GET",
        "https://x/uapi/domestic-stock/v1/quotations/inquire-price",
    )
    assert result["rt_cd"] == "0"
    assert len(calls) == 1


def test_same_day_unresolved_requires_two_distinct_negative_proofs(monkeypatch):
    engine = _db()
    schema = schema_for_engine(engine)
    order_id, cycle_id, epoch_id = str(uuid4()), str(uuid4()), str(uuid4())
    now = now_kst()
    with engine.begin() as conn:
        _portfolio_epoch(conn, schema, epoch_id)
        conn.execute(
            sa.insert(schema.orders).values(
                order_id=order_id,
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="039030",
                market="KOSPI",
                side="BUY",
                ord_type="LIMIT",
                qty=2,
                stage="ENTRY",
                client_order_key="sep29-unresolved",
                status="UNRESOLVED_ACK",
                request_json={"pre_order_holding_qty": 0},
                response_json={"rt_cd": "UNRESOLVED_ACK"},
                created_at=now - timedelta(minutes=2),
            )
        )
    monkeypatch.setattr(fix, "_active_epoch", lambda *_args, **_kwargs: None)
    kis = SimpleNamespace(
        _kr_20260929_daily_ccld_snapshot={
            "captured_mono": fix.time.monotonic(),
            "start_date": now.strftime("%Y%m%d"),
            "end_date": now.strftime("%Y%m%d"),
            "codes": [],
        }
    )

    first = fix._terminalize_unresolved_orders(
        engine=engine, kis=kis, env="practice", strategy=STRATEGY, holdings_rows=[], now=now
    )
    assert first["terminalized"] == []
    with engine.connect() as conn:
        row = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
    assert row["status"] == "UNRESOLVED_ACK"
    assert fix._json_dict(row["response_json"])["negative_broker_truth"]["count"] == 1

    same = fix._terminalize_unresolved_orders(
        engine=engine,
        kis=kis,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[],
        now=now + timedelta(seconds=5),
    )
    assert same["terminalized"] == []

    kis._kr_20260929_daily_ccld_snapshot["captured_mono"] = fix.time.monotonic() - 0.001
    second = fix._terminalize_unresolved_orders(
        engine=engine,
        kis=kis,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[],
        now=now + timedelta(seconds=10),
    )
    assert second["terminalized"] == ["039030"]
    with engine.connect() as conn:
        row = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
    assert row["status"] == "ERROR"
    assert fix._json_dict(row["response_json"])["manual_resolution"] == "BROKER_TRUTH_NO_ORDER_OBSERVED"


def test_prior_day_no_odno_day_order_without_fill_is_terminalized(monkeypatch):
    engine = _db()
    schema = schema_for_engine(engine)
    order_id, cycle_id, epoch_id = str(uuid4()), str(uuid4()), str(uuid4())
    now = now_kst()
    with engine.begin() as conn:
        _portfolio_epoch(conn, schema, epoch_id)
        conn.execute(
            sa.insert(schema.orders).values(
                order_id=order_id,
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="047050",
                market="KOSPI",
                side="BUY",
                ord_type="LIMIT",
                qty=20,
                stage="ENTRY",
                client_order_key="sep28-stale-unresolved",
                status="UNRESOLVED_ACK",
                request_json={"pre_order_holding_qty": 0},
                response_json={"rt_cd": "UNRESOLVED_ACK"},
                created_at=now - timedelta(days=1, minutes=10),
            )
        )
    monkeypatch.setattr(fix, "_active_epoch", lambda *_args, **_kwargs: None)
    result = fix._terminalize_unresolved_orders(
        engine=engine,
        kis=SimpleNamespace(),
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[],
        now=now,
    )
    assert result["terminalized"] == ["047050"]
    with engine.connect() as conn:
        row = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
    assert row["status"] == "ERROR"
    assert fix._json_dict(row["response_json"])["manual_resolution"] == "BROKER_TRUTH_EXPIRED_DAY_ORDER_NO_HOLDING"


def test_cross_day_acked_buy_restores_exact_contract_and_cycle(monkeypatch):
    engine = _db()
    schema = schema_for_engine(engine)
    order_id = str(uuid4())
    source_cycle, imported_cycle, epoch_id, position_id = (
        str(uuid4()), str(uuid4()), str(uuid4()), str(uuid4())
    )
    now = now_kst()
    with engine.begin() as conn:
        _portfolio_epoch(conn, schema, epoch_id)
        conn.execute(
            sa.insert(schema.orders).values(
                order_id=order_id,
                position_cycle_id=source_cycle,
                portfolio_epoch_id=epoch_id,
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="028050",
                market="KOSPI",
                side="BUY",
                ord_type="LIMIT",
                qty=21,
                stage="ENTRY",
                client_order_key="sep28-samsung-ea",
                status="ACKED",
                kis_odno="0000010034",
                broker_order_id="0000010034",
                entry_meta_json=_meta(),
                entry_reason="ENTRY_PULLBACK",
                entry_style_selected="ENTRY_PULLBACK",
                entry_decision_family="ENTRY_PULLBACK_OVERRIDE",
                request_json={
                    "pre_order_holding_qty": 0,
                    "entry_meta": _meta(),
                    "entry_exit_plan": _plan(),
                },
                response_json={"rt_cd": "0"},
                submitted_at=now - timedelta(days=1, minutes=1),
                acked_at=now - timedelta(days=1),
                created_at=now - timedelta(days=1, minutes=2),
            )
        )
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=position_id,
                position_cycle_id=imported_cycle,
                portfolio_epoch_id=epoch_id,
                opened_at=now,
                position_origin="IMPORTED",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="028050",
                market="KOSPI",
                qty=21,
                avg_buy_price=49350.0,
                status="OPEN",
                entry_thesis="POLICY_MISSING",
                exit_policy_family="POLICY_MISSING",
                policy_source="missing",
                entry_meta_json={},
                entry_exit_plan_json={},
                position_meta={},
            )
        )
    monkeypatch.setattr(fix, "_active_epoch", lambda *_args, **_kwargs: None)

    result = fix._recover_cross_day_contract_from_holdings(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[{"pdno": "028050", "hldg_qty": "21", "pchs_avg_pric": "49350"}],
        now=now,
    )
    assert result["recovered"] == ["028050"]
    with engine.connect() as conn:
        pos = conn.execute(sa.select(schema.positions).where(schema.positions.c.position_id == position_id)).mappings().one()
        order = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
        fill = conn.execute(sa.select(schema.fills).where(schema.fills.c.order_id == order_id)).mappings().one()
    assert pos["position_origin"] == "RECOVERY"
    assert str(pos["position_cycle_id"]) == source_cycle
    assert pos["entry_reason"] == "ENTRY_PULLBACK"
    assert pos["entry_decision_family"] == "ENTRY_PULLBACK_OVERRIDE"
    assert pos["trade_horizon"] == "SWING"
    assert pos["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert float(pos["initial_stop_price"]) == pytest.approx(43573.21428571428)
    assert order["status"] == "FILLED"
    assert int(fill["qty"]) == 21
    assert float(fill["price"]) == pytest.approx(49350.0)


def test_cross_day_policy_recovery_refuses_symbol_only_ambiguity(monkeypatch):
    engine = _db()
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    now = now_kst()
    with engine.begin() as conn:
        _portfolio_epoch(conn, schema, epoch_id)
        for suffix in ("A", "B"):
            conn.execute(
                sa.insert(schema.orders).values(
                    order_id=str(uuid4()),
                    position_cycle_id=str(uuid4()),
                    portfolio_epoch_id=epoch_id,
                    env="practice",
                    strategy=STRATEGY,
                    sid=1,
                    mode=1,
                    code="028050",
                    market="KOSPI",
                    side="BUY",
                    ord_type="LIMIT",
                    qty=21,
                    stage="ENTRY",
                    client_order_key=f"ambiguous-{suffix}",
                    status="ACKED",
                    kis_odno=f"ODNO-{suffix}",
                    broker_order_id=f"ODNO-{suffix}",
                    request_json={
                        "pre_order_holding_qty": 0,
                        "entry_meta": _meta(),
                        "entry_exit_plan": _plan(),
                    },
                    response_json={"rt_cd": "0"},
                    created_at=now - timedelta(days=1),
                )
            )
        conn.execute(
            sa.insert(schema.positions).values(
                position_id=str(uuid4()),
                position_cycle_id=str(uuid4()),
                portfolio_epoch_id=epoch_id,
                opened_at=now,
                position_origin="IMPORTED",
                env="practice",
                strategy=STRATEGY,
                sid=1,
                mode=1,
                code="028050",
                market="KOSPI",
                qty=21,
                avg_buy_price=49350.0,
                status="OPEN",
                entry_thesis="POLICY_MISSING",
                exit_policy_family="POLICY_MISSING",
                policy_source="missing",
                entry_meta_json={},
                entry_exit_plan_json={},
                position_meta={},
            )
        )
    monkeypatch.setattr(fix, "_active_epoch", lambda *_args, **_kwargs: None)
    result = fix._recover_cross_day_contract_from_holdings(
        engine=engine,
        env="practice",
        strategy=STRATEGY,
        holdings_rows=[{"pdno": "028050", "hldg_qty": "21", "pchs_avg_pric": "49350"}],
        now=now,
    )
    assert result["recovered"] == []
    assert result["review_required"] == ["028050"]
