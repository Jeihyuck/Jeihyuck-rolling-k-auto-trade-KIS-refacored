from __future__ import annotations

from dataclasses import replace
from datetime import date
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kr.runtime_integrity_20260915 import install_kr_20260915_runtime_integrity
from trader.reconcile_kis import _promote_open_buy_orders_from_holdings
from trader.time_utils import now_kst


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _epoch(engine, epoch_id: str) -> None:
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.portfolio_epochs).values(
            portfolio_epoch_id=epoch_id,
            env="practice",
            account_id="practice:test",
            sid=1,
            mode=1,
            strategy="pb1_pullback_close",
            status="ACTIVE",
        ))


def test_policy_missing_adoption_accepts_real_fractional_average_roundtrip() -> None:
    install_kr_20260915_runtime_integrity()
    import trader.kr.market_state_overlay as overlay

    cases = [
        ("067290", 2358.671, 2358.6709999999998, 3600.0),
        ("005830", 173903.846, 173903.84599999996, 190000.0),
    ]
    for code, avg, roundtripped, mark in cases:
        original = {
            "code": code,
            "qty": 20,
            "orderable_qty": 20,
            "avg_buy_price": avg,
            "position_cycle_id": f"cycle-{code}",
            "portfolio_epoch_id": "epoch-20260915",
            "entry_thesis": "POLICY_MISSING",
            "exit_policy_family": "POLICY_MISSING",
            "position_meta": {},
        }
        adoption = overlay.build_kr_policy_missing_adoption(original, current_price=mark)
        assert adoption is not None
        persisted = {**original, **adoption["position_fields"], "avg_buy_price": roundtripped}
        assert overlay.is_verified_kr_policy_missing_adoption(persisted)

        tampered = dict(persisted)
        tampered["avg_buy_price"] = avg + 0.01
        assert not overlay.is_verified_kr_policy_missing_adoption(tampered)


def test_pb1_sell_baseline_is_verified_before_submit_and_mirrored_on_ack() -> None:
    install_kr_20260915_runtime_integrity()
    engine = _db()
    repo = OrdersRepo(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    _epoch(engine, epoch_id)
    key = "pr131-sell-baseline-ok"
    request = {
        "strategy_owner": "KR_STANDARD",
        "exit_stage": "FULL_EXIT",
        "pre_order_holding_qty": 1,
        "pre_order_orderable_qty": 1,
        "pre_order_avg_price": 1086000.0,
        "requested_qty": 1,
        "submitted_qty": 1,
        "balance_snapshot_id": "snap-402340",
        "order_intent_ts": now_kst().isoformat(),
        "trade_session": "am",
    }
    _, created = repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="402340", market="KOSPI", side="SELL", ord_type="MARKET", qty=1,
        limit_price=None, stage="FULL_EXIT", client_order_key=key, request_json=request,
        account_id="practice:test", portfolio_epoch_id=epoch_id, position_cycle_id=cycle_id,
    )
    assert created
    repo.mark_submitted("practice", key, "0000000039", {"rt_cd": "0"}, submitted_qty=1)
    repo.mark_acked("practice", "0000000039", {"rt_cd": "0", "msg1": "accepted"})
    row = repo.get_order_by_client_order_key("practice", key)
    assert row["request_json"]["pre_order_holding_qty"] == 1
    assert row["response_json"]["pre_order_holding_qty"] == 1
    assert row["response_json"]["_order_execution"]["pre_order_holding_qty"] == 1
    assert row["response_json"]["_order_execution"]["baseline_contract"] == "KR_SELL_BASELINE_V1"


def test_pb1_sell_with_missing_baseline_is_blocked_before_broker_submit_boundary() -> None:
    install_kr_20260915_runtime_integrity()
    engine = _db()
    repo = OrdersRepo(engine)
    epoch_id = str(uuid4())
    _epoch(engine, epoch_id)
    with pytest.raises(RuntimeError, match="KR_SELL_BASELINE_INVALID"):
        repo.create_intent_idempotent(
            env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
            code="402340", market="KOSPI", side="SELL", ord_type="MARKET", qty=1,
            limit_price=None, stage="FULL_EXIT", client_order_key="pr131-missing-baseline",
            request_json={
                "strategy_owner": "KR_STANDARD",
                "exit_stage": "FULL_EXIT",
                "requested_qty": 1,
                "submitted_qty": 1,
            },
            account_id="practice:test", portfolio_epoch_id=epoch_id,
        )


def test_legacy_full_exit_lost_baseline_recovers_from_exact_lifecycle_and_terminalizes() -> None:
    """Replay the 402340 shape: ACK SELL1, request baseline lost, KIS holding is zero."""
    install_kr_20260915_runtime_integrity()
    engine = _db()
    schema = schema_for_engine(engine)
    repo = OrdersRepo(engine)
    positions = PositionsRepo(engine)
    fills = FillsRepo(engine)
    epoch_id = str(uuid4())
    cycle_id = str(uuid4())
    order_id = str(uuid4())
    _epoch(engine, epoch_id)
    ts = now_kst()
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.positions).values(
            position_id=str(uuid4()), position_cycle_id=cycle_id, portfolio_epoch_id=epoch_id,
            opened_at=ts, position_origin="SYSTEM", env="practice", strategy="pb1_pullback_close",
            sid=1, mode=1, code="402340", market="KOSPI", qty=1,
            avg_buy_price=1086000.0, total_cost=1086000.0, realized_pnl=0.0,
            status="OPEN", entry_meta_json={}, entry_exit_plan_json={}, position_meta={},
        ))
        conn.execute(sa.insert(schema.orders).values(
            order_id=order_id, position_cycle_id=cycle_id, portfolio_epoch_id=epoch_id,
            env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
            code="402340", market="KOSPI", side="SELL", ord_type="MARKET", qty=1,
            stage="FULL_EXIT", client_order_key="legacy-402340-hard-stop", status="ACKED",
            kis_odno="0000000039", broker_order_id="0000000039",
            request_json={"requested_qty": 1, "submitted_qty": 1, "trade_session": "am"},
            response_json={"rt_cd": "0"}, created_at=ts, submitted_at=ts, acked_at=ts,
        ))

    open_rows = repo.get_open_orders("practice", include_stale=True)
    target = next(row for row in open_rows if row["code"] == "402340")
    assert target["request_json"]["pre_order_holding_qty"] == 1
    assert target["request_json"]["recovered_baseline_source"] == "EXACT_OPEN_LIFECYCLE_FULL_EXIT"

    result = _promote_open_buy_orders_from_holdings(
        env="practice", strategy="pb1_pullback_close", ctx_run_id=None, tick_ts=ts,
        holdings_rows=[], orders_repo=repo, fills_repo=fills, positions_repo=positions,
    )
    assert result["orders"] == 1
    with engine.connect() as conn:
        row = dict(conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one())
    assert row["status"] == "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED"
    assert row["response_json"]["confirmed_fill_qty"] == 1
    assert row["response_json"]["holding_qty"] == 0


def test_close_metrics_do_not_recount_am_ack(monkeypatch) -> None:
    install_kr_20260915_runtime_integrity()
    import trader.execution_state as execution_state

    monkeypatch.setenv("PB1_SESSION_KIND", "close")
    orders = [
        {"order_id": "am-1", "side": "SELL", "status": "ACKED", "request_json": {"trade_session": "am"}},
        {"order_id": "close-1", "side": "SELL", "status": "ACKED", "request_json": {"trade_session": "close"}},
    ]
    scoped = execution_state.durable_order_metrics(orders, [])
    assert scoped["order_intents_created"] == 1
    assert scoped["broker_acked"] == 1
    assert scoped["by_side"]["SELL"]["broker_acked"] == 1

    only_am = execution_state.durable_order_metrics(orders[:1], [])
    assert only_am["order_intents_created"] == 0
    assert only_am["broker_acked"] == 0


def test_kis_marketcap_provider_uses_required_091_fields_without_fake_count() -> None:
    from trader.universe.providers.kis_marketcap_top import KISMarketcapTopProvider

    class FakeKis:
        env = "practice"

    provider = KISMarketcapTopProvider(kis=FakeKis(), env="practice")
    params = provider._build_params("KOSPI", 150)
    assert params["FID_COND_SCR_DIV_CODE"] == "20174"
    assert params["FID_COND_MRKT_DIV_CODE"] == "J"
    for key in (
        "FID_INPUT_PRICE_1", "FID_INPUT_PRICE_2", "FID_DIV_CLS_CODE",
        "FID_INPUT_ISCD", "FID_TRGT_CLS_CODE", "FID_TRGT_EXLS_CLS_CODE", "FID_VOL_CNT",
    ):
        assert key in params
    assert "FID_INPUT_CNT_1" not in params
    assert "FID_PRC_CLS_CODE" not in params


def test_kr_infinite_historical_zero_fill_pending_expires_then_tp2_rearms(monkeypatch) -> None:
    """Replay stale TP2 starvation: old SELL is proven dead, current +10% can SELL again."""
    from trader.kr.infinite.config import InfiniteConfig
    from trader.kr.infinite.models import Action, OrderIntent, State, Status
    from trader.kr.infinite.repository import InfiniteRepository
    from trader.kr.infinite.runner import run_once

    day = date(2026, 9, 15)
    old_day = date(2026, 9, 14)
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")

    class KIS:
        def __init__(self):
            self.qty = 3
            self.sell_calls: list[int] = []

        def get_balance(self, force=True):
            return {
                "output1": [{"pdno": "122630", "hldg_qty": str(self.qty), "ord_psbl_qty": str(self.qty), "pchs_avg_pric": "98000"}],
                "output2": {"tot_evlu_amt": "4000000"},
            }

        def get_current_price(self, symbol):
            return 108000

        def get_orderable_cash(self, symbol, price):
            return 4_000_000, {}

        def inquire_daily_ccld(self, **kwargs):
            raise RuntimeError("simulated KIS daily-ccld HTTP 500")

        def sell_stock(self, symbol, qty):
            self.sell_calls.append(qty)
            return {"rt_cd": "0", "output": {"ODNO": "NEW-TP2"}}

    class Repo:
        def __init__(self):
            self.state = State(
                cycle_id="KRINF-20260819-1d1a9a3d", cycle_start_date=date(2026, 8, 19),
                allocated_capital_krw=4_000_000, unit_krw=100_000,
                core_filled_notional=300_000, units_used=3, core_units_used=3,
                last_buy_date=date(2026, 8, 19), last_buy_price=98000, status=Status.EXIT_PENDING,
                metadata={"profit_stage": "TP1_FILLED", "pending_profit_stage": "TP2_SUBMITTED"},
            )
            self.intents = [OrderIntent(
                1, self.state.cycle_id, old_day, "SELL_ALL", "old-tp2", 3,
                broker_order_id="OLD-TP2", status="SUBMITTED",
                metadata={"pre_order_holding_qty": 3, "pre_order_avg_price": 98000.0},
            )]

        def ensure_schema(self):
            return None

        def load_state(self):
            return self.state

        def intent_keys(self):
            return frozenset(item.idempotency_key for item in self.intents)

        def pending_intents(self):
            return [item for item in self.intents if item.status in {
                "INTENT_CREATED", "SUBMITTED", "ACK", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING"
            }]

        def save_state(self, state):
            self.state = state

        def create_intent(self, state, decision, trade_date, market_state):
            if decision.idempotency_key in self.intent_keys():
                return False
            metadata = InfiniteRepository._intent_metadata(state, decision)
            self.intents.append(OrderIntent(
                len(self.intents) + 1, state.cycle_id, trade_date, decision.action.value,
                decision.idempotency_key, decision.qty, status="INTENT_CREATED", metadata=metadata,
            ))
            return True

        def mark_submitted(self, key, order_id):
            self.intents = [
                replace(item, broker_order_id=order_id, status="SUBMITTED")
                if item.idempotency_key == key else item for item in self.intents
            ]

        def mark_rejected(self, key, reason):
            self.intents = [
                replace(item, status="REJECTED") if item.idempotency_key == key else item
                for item in self.intents
            ]

        def persist_reconciliation(self, state, updates):
            self.state = state
            observed = {intent.id: broker for intent, broker in updates}
            self.intents = [
                replace(item, status=observed[item.id].status,
                        filled_qty=observed[item.id].filled_qty,
                        filled_notional_krw=observed[item.id].filled_notional_krw)
                if item.id in observed else item for item in self.intents
            ]

    kis = KIS()
    repo = Repo()
    result = run_once(
        config=InfiniteConfig(enabled=True), kis=kis, repository=repo,
        regime_provider=lambda: ("KR_NORMAL", "OK"), trade_date=day, kis_env="practice",
    )

    assert repo.intents[0].status == "EXPIRED"
    assert result.decision.action == Action.SELL_ALL
    assert result.decision.reason == "TAKE_PROFIT_TP2"
    assert result.submitted
    assert kis.sell_calls == [3]
