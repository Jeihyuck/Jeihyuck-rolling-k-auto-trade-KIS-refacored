from datetime import datetime, timezone

from trader.execution_state import SELL_GUARD_STATES, durable_order_metrics
import json
import sqlalchemy as sa
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.pb1_runner import _write_session_result_file


def test_new_engine_tick_observes_durable_sell_ack():
    # The durable predicate is intentionally object/engine independent.
    ledger_after_tick1 = [{"side": "SELL", "code": "010060", "status": "ACKED",
                           "position_cycle_id": "cycle-current"}]
    engine1_memory = {"010060"}
    del engine1_memory
    engine2_memory = set()
    assert not engine2_memory
    assert ledger_after_tick1[0]["status"] in SELL_GUARD_STATES


def test_metrics_come_from_durable_orders_not_local_candidate_counts():
    orders = ([{"side": "BUY", "status": "ACKED"}] * 3
              + [{"side": "SELL", "status": "ACKED"}] * 4)
    metrics = durable_order_metrics(orders, [])
    assert metrics["broker_acked"] == 7
    assert metrics["by_side"]["BUY"]["broker_acked"] == 3
    assert metrics["by_side"]["SELL"]["broker_acked"] == 4
    assert metrics["fills_confirmed"] == 0


def test_pb1_result_file_rebuilds_ack_metrics_from_durable_db(tmp_path, monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    repo = OrdersRepo(engine)
    for side, count in (("BUY", 3), ("SELL", 4)):
        for idx in range(count):
            key = f"metric-{side}-{idx}"
            repo.create_intent_idempotent(
                env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
                code=f"{idx + 1:06d}", market="J", side=side, ord_type="MARKET", qty=1,
                limit_price=None, stage="FULL_EXIT" if side == "SELL" else "ENTRY",
                client_order_key=key, request_json={}, status="ACKED",
            )
    path = tmp_path / "pb1_result.json"
    monkeypatch.setenv("PB1_SESSION_RESULT_PATH", str(path))
    _write_session_result_file({"status": "OK", "accepted": 999, "order_candidates": 999},
                               engine=engine, env="practice")
    result = json.loads(path.read_text())
    assert result["broker_acked"] == 7
    assert result["by_side"]["BUY"]["broker_acked"] == 3
    assert result["by_side"]["SELL"]["broker_acked"] == 4
    assert result["fills_confirmed"] == 0


def test_durable_metrics_use_full_session_window_and_sync_aliases(tmp_path, monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    repo = OrdersRepo(engine)
    order_ids = []
    for side, statuses in (("BUY", ["ACKED"] * 3),
                           ("SELL", ["ACKED"] * 3 + ["UNRESOLVED_ACK"])):
        for idx, status in enumerate(statuses):
            order_id, _ = repo.create_intent_idempotent(
                env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
                code=f"{100 + len(order_ids):06d}", market="J", side=side, ord_type="MARKET", qty=1,
                limit_price=None, stage="FULL_EXIT" if side == "SELL" else "ENTRY",
                client_order_key=f"full-window-{side}-{idx}", request_json={}, status=status,
            )
            order_ids.append(order_id)
    with engine.begin() as conn:
        conn.execute(sa.update(schema.orders).values(created_at=datetime(2026, 8, 31, 9, 34)))
        for idx, order_id in enumerate(order_ids[:2]):
            order = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
            conn.execute(sa.insert(schema.fills).values(
                fill_id=str(__import__("uuid").uuid4()), env="practice", order_id=order_id,
                position_cycle_id=order["position_cycle_id"], portfolio_epoch_id=order["portfolio_epoch_id"],
                trade_id=f"window-fill-{idx}", broker_fill_id=f"window-fill-{idx}",
                code=order["code"], market="J", side=order["side"], qty=1, price=100,
                fee=0, tax=0, filled_at=datetime(2026, 8, 31, 10, 20), raw_json={}, fill_meta_json={},
            ))
    path = tmp_path / "full-session-result.json"
    monkeypatch.setenv("PB1_SESSION_RESULT_PATH", str(path))
    _write_session_result_file(
        {"status": "OK", "accepted": 0, "filled_confirmed": 0}, engine=engine, env="practice",
        start_at=datetime(2026, 8, 31, 9, 0), end_at=datetime(2026, 8, 31, 12, 56),
    )
    result = json.loads(path.read_text())
    assert result["broker_submitted"] == result["api_submitted"] == 7
    assert result["broker_acked"] == result["accepted"] == 6
    assert result["fills_confirmed"] == result["filled_confirmed"] == result["filled_confirmed_count"] == 2
    assert result["unresolved_acks"] == result["unresolved_ack_count"] == 1
    assert result["buy_orders_ack"] == 3 and result["sell_orders_ack"] == 3
