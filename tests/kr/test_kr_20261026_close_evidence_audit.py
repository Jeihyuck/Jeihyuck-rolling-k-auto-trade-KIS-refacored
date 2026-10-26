from scripts.kr_close_settlement_audit import audit_sell_settlement

def test_missing_native_fill_is_reported():
    report = audit_sell_settlement(
        orders=[dict(order_id="o", code="293490", side="SELL", qty=170)],
        fills=[dict(order_id="o", side="SELL", qty=17, price=11292,
                    evidence_kind="KIS_ORDER_CUMULATIVE_EXECUTION")],
        positions=[],
    )
    assert report["gap_count"] == 1
    assert report["gaps"][0]["missing_priced_sell_qty"] == 153


def test_filled_order_status_does_not_substitute_for_zero_native_execution():
    report = audit_sell_settlement(
        orders=[dict(order_id="tfe", code="425420", side="SELL", status="FILLED", qty=30)],
        fills=[],
        positions=[dict(position_cycle_id="cycle", status="CLOSED",
                        closed_reason="BROKER_ZERO_ECONOMIC_SETTLEMENT_PENDING")],
    )
    assert report["status"] == "ECONOMIC_SETTLEMENT_PENDING"
    assert report["gaps"][0]["missing_priced_sell_qty"] == 30


def test_cumulative_repeated_snapshots_are_not_double_counted():
    report = audit_sell_settlement(
        orders=[dict(order_id="k", side="SELL", qty=170, code="293490")],
        fills=[
            dict(order_id="k", side="SELL", qty=17, price=11292, evidence_kind="KIS_ORDER_CUMULATIVE_EXECUTION"),
            dict(order_id="k", side="SELL", qty=170, price=11288, evidence_kind="KIS_ORDER_CUMULATIVE_EXECUTION"),
        ],
        positions=[],
    )
    assert report["gap_count"] == 0


def _confirmed_nonexecution_order(
    *, order_id="original", code="293490", order_no="0000034710",
    qty=170, executed=17, cancelled=153, rejected=0, status="CANCELLED",
):
    import json

    broker_row = {
        "odno": order_no,
        "pdno": code,
        "sll_buy_dvsn_cd": "01",
        "ord_qty": str(qty),
        "tot_ccld_qty": str(executed),
        "cncl_cfrm_qty": str(cancelled),
        "rjct_qty": str(rejected),
        "rmn_qty": "0",
    }
    return dict(
        order_id=order_id, code=code, side="SELL", qty=qty,
        status=status, kis_odno=order_no,
        response_json=json.dumps({"kis_row": broker_row}),
    )


def test_broker_confirmed_cancel_replacement_sells_do_not_create_false_gap():
    original = _confirmed_nonexecution_order(
        order_id="cancelled", qty=170, executed=0, cancelled=170,
    )
    replacement = dict(
        order_id="replacement", code="293490", side="SELL", qty=170,
        status="FILLED", kis_odno="0000034720",
    )
    report = audit_sell_settlement(
        orders=[original, replacement],
        fills=[dict(
            order_id="replacement", code="293490", side="SELL", qty=170,
            price=11288, evidence_kind="KIS_ORDER_CUMULATIVE_EXECUTION",
        )],
        positions=[],
    )
    assert report["sell_orders_checked"] == 2
    assert report["gap_count"] == 0
    assert report["status"] == "PRICED_SELL_EVIDENCE_MATCHED"


def test_partially_filled_cancelled_order_requires_only_native_priced_executions():
    order = _confirmed_nonexecution_order()
    report = audit_sell_settlement(
        orders=[order],
        fills=[dict(
            order_id="original", code="293490", side="SELL", qty=17,
            price=11292, evidence_kind="KIS_ORDER_CUMULATIVE_EXECUTION",
        )],
        positions=[],
    )
    assert report["gap_count"] == 0
    missing = audit_sell_settlement(orders=[order], fills=[], positions=[])
    assert missing["gap_count"] == 1
    assert missing["gaps"][0]["required_priced_sell_qty"] == 17
    assert missing["gaps"][0]["broker_confirmed_nonexecution_qty"] == 153
    assert missing["gaps"][0]["missing_priced_sell_qty"] == 17


def test_broker_confirmed_full_rejection_requires_no_priced_execution():
    rejected = _confirmed_nonexecution_order(
        qty=30, code="425420", order_no="0000034718",
        executed=0, cancelled=0, rejected=30, status="REJECTED",
    )
    report = audit_sell_settlement(orders=[rejected], fills=[], positions=[])
    assert report["gap_count"] == 0


def test_cancelled_or_rejected_label_alone_cannot_suppress_missing_fill():
    for status in ("REJECTED", "CANCELLED", "PARTIAL_FILLED"):
        order = dict(
            order_id=status, code="293490", side="SELL", qty=170,
            status=status, kis_odno="0000034710",
            response_json='{"rt_cd":"0","output":{"ODNO":"0000034710"}}',
        )
        report = audit_sell_settlement(orders=[order], fills=[], positions=[])
        assert report["gap_count"] == 1
        assert report["gaps"][0]["missing_priced_sell_qty"] == 170


def test_broker_nonexecution_proof_must_match_original_order_and_zero_remaining():
    import json

    valid = _confirmed_nonexecution_order()
    for changed in (
        {"odno": "0000039999"},
        {"pdno": "012750"},
        {"sll_buy_dvsn_cd": "02"},
        {"ord_qty": "171"},
        {"rmn_qty": "153"},
        {"tot_ccld_qty": "18"},
    ):
        order = dict(valid)
        broker_row = json.loads(order["response_json"])["kis_row"]
        broker_row.update(changed)
        order["response_json"] = json.dumps({"kis_row": broker_row})
        report = audit_sell_settlement(orders=[order], fills=[], positions=[])
        assert report["gap_count"] == 1, changed
        assert report["gaps"][0]["missing_priced_sell_qty"] == 170, changed


def test_same_epoch_prior_day_unpriced_sell_carries_into_next_day_audit():
    """Oct26 missing native SELL economics cannot disappear on Oct27."""
    from datetime import date, datetime
    import sqlalchemy as sa

    from scripts.kr_close_settlement_audit import load_snapshot

    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.exec_driver_sql("ATTACH DATABASE ':memory:' AS public")
    md = sa.MetaData()
    orders = sa.Table(
        "orders", md,
        sa.Column("order_id", sa.String, primary_key=True),
        sa.Column("env", sa.String), sa.Column("trading_epoch_id", sa.String),
        sa.Column("code", sa.String), sa.Column("side", sa.String),
        sa.Column("status", sa.String), sa.Column("qty", sa.Integer),
        sa.Column("kis_odno", sa.String), sa.Column("position_cycle_id", sa.String),
        sa.Column("response_json", sa.Text),
        sa.Column("created_at", sa.DateTime),
        schema="public",
    )
    sa.Table(
        "fills", md, sa.Column("order_id", sa.String),
        sa.Column("env", sa.String), sa.Column("trading_epoch_id", sa.String),
        sa.Column("side", sa.String), sa.Column("qty", sa.Integer),
        sa.Column("price", sa.Float), sa.Column("fill_meta_json", sa.JSON),
        sa.Column("filled_at", sa.DateTime), schema="public",
    )
    sa.Table(
        "positions", md, sa.Column("code", sa.String),
        sa.Column("env", sa.String), sa.Column("trading_epoch_id", sa.String),
        sa.Column("position_cycle_id", sa.String), sa.Column("status", sa.String),
        sa.Column("qty", sa.Integer), sa.Column("closed_reason", sa.String),
        schema="public",
    )
    md.create_all(engine)
    with engine.begin() as conn:
        conn.execute(sa.insert(orders), [
            {
                "order_id": "missing-sell-oct26", "env": "practice",
                "trading_epoch_id": "current", "code": "293490",
                "side": "SELL", "status": "PARTIAL_FILLED", "qty": 170,
                "kis_odno": "0000034710", "position_cycle_id": "cycle",
                "response_json": "{}", "created_at": datetime(2026, 10, 26, 6, 16),
            },
            {
                "order_id": "old-account-not-ours", "env": "practice",
                "trading_epoch_id": "old", "code": "293490",
                "side": "SELL", "status": "FILLED", "qty": 36,
                "kis_odno": "0000020000", "position_cycle_id": "old",
                "response_json": "{}", "created_at": datetime(2026, 10, 26, 6, 16),
            },
            {
                "order_id": "future-order-excluded", "env": "practice",
                "trading_epoch_id": "current", "code": "425420",
                "side": "SELL", "status": "SUBMITTED", "qty": 30,
                "kis_odno": "0000039999", "position_cycle_id": "next-day",
                "response_json": "{}", "created_at": datetime(2026, 10, 28, 0, 0),
            },
        ])
    snapshot = load_snapshot(
        engine, env="practice", epoch="current", day=date(2026, 10, 27),
    )
    assert [row["order_id"] for row in snapshot["orders"]] == ["missing-sell-oct26"]
    report = audit_sell_settlement(**snapshot)
    assert report["gap_count"] == 1
    assert report["gaps"][0]["missing_priced_sell_qty"] == 170


def test_manual_priced_fill_does_not_replace_native_kis_economic_proof():
    order = dict(order_id="manual-price", code="425420", side="SELL", qty=30)
    manual = dict(
        order_id="manual-price", side="SELL", qty=30, price=62273,
        evidence_kind="MANUAL_MTS_PRICE",
    )
    report = audit_sell_settlement(
        orders=[order], fills=[manual], positions=[],
    )
    assert report["status"] == "ECONOMIC_SETTLEMENT_PENDING"
    assert report["gaps"][0]["missing_priced_sell_qty"] == 30


def test_native_individual_kis_fills_are_summed_without_manual_fills():
    order = dict(order_id="native-price", code="425420", side="SELL", qty=30)
    fills = [
        dict(order_id="native-price", side="SELL", qty=12, price=62270,
             evidence_kind="KIS_INDIVIDUAL_EXECUTION"),
        dict(order_id="native-price", side="SELL", qty=18, price=62275,
             evidence_kind="KIS_INDIVIDUAL_EXECUTION"),
        dict(order_id="native-price", side="SELL", qty=30, price=62273,
             evidence_kind="MANUAL_MTS_PRICE"),
    ]
    report = audit_sell_settlement(orders=[order], fills=fills, positions=[])
    assert report["gap_count"] == 0
    missing = audit_sell_settlement(orders=[order], fills=fills[1:], positions=[])
    assert missing["gap_count"] == 1
    assert missing["gaps"][0]["missing_priced_sell_qty"] == 12
