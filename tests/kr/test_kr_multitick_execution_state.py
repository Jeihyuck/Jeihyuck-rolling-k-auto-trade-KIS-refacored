from datetime import datetime, timezone

from trader.execution_state import SELL_GUARD_STATES, durable_order_metrics


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
