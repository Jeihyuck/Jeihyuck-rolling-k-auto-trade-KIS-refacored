"""Regression for the 2026-10-08 LG전자 unknown-ACK false-fill incident."""
from trader.reconcile_kis import _kr_sell_holdings_promotion_unproven


def test_unknown_sell_submit_with_internal_client_key_cannot_promote_holdings_delta():
    order = {
        "side": "SELL",
        "code": "066570",
        "kis_odno": "practice:pb1_pullback_close:2026-10-08:066570:EXIT:SELL:exit:close:1:retry2",
        "broker_order_id": None,
        "client_order_key": "practice:pb1_pullback_close:2026-10-08:066570:EXIT:SELL:exit:close:1:retry2",
    }
    response = {
        "rt_cd": "UNRESOLVED_ACK",
        "msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN",
        "msg1": "KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH",
        "pre_order_holding_qty": 5,
        "holding_qty": 0,
        "holding_delta": 5,
    }
    # The apparent holdings delta MUST NOT become a confirmed order fill.
    assert _kr_sell_holdings_promotion_unproven(order, response) is True

    # A genuine KIS number can support a later exact broker-execution match,
    # but NEVER upgrades an unknown submit based only on aggregate holdings.
    order["broker_order_id"] = "0000006722"
    assert _kr_sell_holdings_promotion_unproven(order, response) is True


def test_confirmed_broker_ack_does_not_change_existing_sell_promotion_policy():
    order = {"side": "SELL", "code": "010120", "kis_odno": "0000006722"}
    response = {"rt_cd": "0", "msg_cd": "40590000", "output": {"ODNO": "0000006722"}}
    assert _kr_sell_holdings_promotion_unproven(order, response) is False


def test_guard_does_not_interfere_with_pb1_buy_or_other_owner_decisions():
    order = {"side": "BUY", "code": "006400", "kis_odno": "internal-key"}
    response = {"rt_cd": "UNRESOLVED_ACK", "msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN"}
    assert _kr_sell_holdings_promotion_unproven(order, response) is False


def test_unknown_sell_stays_unproven_with_no_identifier_or_fake_identifier():
    response = {"msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN"}
    assert _kr_sell_holdings_promotion_unproven(
        {"side": "SELL", "kis_odno": None, "broker_order_id": None}, response,
    )
    assert _kr_sell_holdings_promotion_unproven(
        {"side": "SELL", "kis_odno": "practice:semantic:retry3"}, response,
    )


def test_reconciliation_rejects_both_open_and_previously_promoted_lg_orders_without_broker_proof():
    from trader.reconcile_kis import _promote_open_buy_orders_from_holdings

    semantic = "practice:pb1_pullback_close:2026-10-08:066570:EXIT:SELL:exit:close:1:retry2"
    order = {
        "order_id": "lg-unknown-ack-order",
        "client_order_key": semantic,
        "kis_odno": semantic,
        "broker_order_id": semantic,
        "code": "066570",
        "side": "SELL",
        "status": "ACKED",
        "qty": 5,
        "strategy": "pb1_pullback_close",
        "request_json": {"pre_order_holding_qty": 5},
        "response_json": {
            "rt_cd": "UNRESOLVED_ACK",
            "msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN",
            "msg1": "KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH",
        },
    }

    class Repo:
        def __init__(self, open_orders=None, terminal_orders=None):
            self.open_orders = open_orders or []
            self.terminal_orders = terminal_orders or []
            self.upserts = []

        def get_open_orders(self, *_args):
            return self.open_orders

        def list_recent_holdings_promoted_orders_for_repair(self, *_args, **_kwargs):
            return self.terminal_orders

        def upsert_reconciled_order(self, **kwargs):
            self.upserts.append(kwargs)
            raise AssertionError("unproven SELL must not be promoted")

    for prior_status, opened in (
        ("ACKED", True),
        ("FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED", False),
    ):
        candidate = {
            **order,
            "status": prior_status,
            "response_json": {
                **order["response_json"],
                "promotion_source": "kis_holdings",
                "confirmed_fill_qty": 5,
                "pre_order_holding_qty": 5,
                "holding_qty": 0,
            } if not opened else order["response_json"],
        }
        repo = Repo(open_orders=[candidate] if opened else [], terminal_orders=[] if opened else [candidate])
        result = _promote_open_buy_orders_from_holdings(
            env="practice", strategy="pb1_pullback_close",
            ctx_run_id=None, tick_ts=__import__("datetime").datetime.now(),
            holdings_rows=[{"pdno": "066570", "hldg_qty": "0"}],
            orders_repo=repo, fills_repo=object(), positions_repo=None,
        )
        assert result["orders"] == 0 and result["fills"] == 0
        assert repo.upserts == []


def test_unresolved_broker_identity_never_substitutes_semantic_client_key():
    from trader.db.repos import _kr_unresolved_broker_identity

    response = {
        "rt_cd": "UNRESOLVED_ACK",
        "msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN",
    }
    assert _kr_unresolved_broker_identity(
        "practice:pb1_pullback_close:2026-10-08:066570:EXIT:SELL:retry2", response,
    ) is None
    assert _kr_unresolved_broker_identity(None, response) is None
    assert _kr_unresolved_broker_identity("0000006722", response) == "0000006722"
    # Do not rewrite normal/known KIS acknowledgements or other order owners.
    assert _kr_unresolved_broker_identity("KIS-LEGACY-ORDER", {"rt_cd": "0"}) == "KIS-LEGACY-ORDER"


def test_real_orders_repo_does_not_store_lg_semantic_key_as_kis_broker_identity():
    from datetime import datetime, timezone
    from trader.account_state import get_account_key
    from trader.db.repos import OrdersRepo
    from trader.db.schema import schema_for_engine
    from tests.kr.test_kr_20261006_execution_convergence import _db
    import sqlalchemy as sa

    engine = _db()
    orders = OrdersRepo(engine)
    semantic = "practice:pb1_pullback_close:2026-10-08:066570:EXIT:SELL:retry2"
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="066570", market="J",
        side="SELL", ord_type="LIMIT", qty=5, limit_price=204000,
        stage="FULL_EXIT", client_order_key=semantic,
        request_json={"pre_order_holding_qty": 5},
        status="ACKED", account_id=get_account_key(env="practice"),
    )
    assert created
    now = datetime.now(timezone.utc)
    orders.upsert_reconciled_order(
        env="practice", run_id=None, strategy="pb1_pullback_close",
        sid=1, mode=1, code="066570", market="J", side="SELL",
        ord_type="LIMIT", qty=5, limit_price=204000,
        stage="FULL_EXIT", client_order_key=semantic,
        kis_odno=semantic, status="ACKED",
        request_json={"pre_order_holding_qty": 5},
        response_json={"rt_cd": "UNRESOLVED_ACK", "msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN"},
        submitted_at=now, acked_at=now,
    )
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        row = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
    assert row["client_order_key"] == semantic
    assert row["status"] == "UNRESOLVED_ACK"
    assert row["broker_order_id"] is None
    assert row["kis_odno"] is None

    # Older callers can omit the exact rt_cd/msg_cd in the error payload.
    orders.mark_unresolved_ack(
        env="practice", client_order_key=semantic,
        error_payload={}, kis_odno=semantic, submitted_qty=5,
    )
    with engine.connect() as conn:
        row = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
    assert row["status"] == "UNRESOLVED_ACK"
    assert row["broker_order_id"] is None and row["kis_odno"] is None
