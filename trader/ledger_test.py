from __future__ import annotations

from trader.exit_allocation import allocate_sell_qty, apply_sell_allocation
from trader.ctx_schema import normalize_daily_ctx
from trader.execution import record_entry_state
from trader.ledger import apply_sell_fill_fifo, record_buy_fill, remaining_qty_for_strategy
from trader.position_state_store import migrate_position_state
from trader import state_store as runtime_state_store
from trader.code_utils import normalize_code
from trader.order_map_store import load_order_map_index


def _sample_state() -> dict:
    return {"version": 1, "lots": [], "updated_at": None}


def test_fifo_and_fallback() -> None:
    state = _sample_state()
    record_buy_fill(
        state,
        lot_id="L1",
        pdno="005930",
        strategy_id=1,
        engine="test",
        entry_ts="2025-01-01T09:00:00+09:00",
        entry_price=70000.0,
        qty=5,
        meta={},
    )
    record_buy_fill(
        state,
        lot_id="L2",
        pdno="005930",
        strategy_id=2,
        engine="test",
        entry_ts="2025-01-01T09:05:00+09:00",
        entry_price=70500.0,
        qty=5,
        meta={},
    )

    apply_sell_fill_fifo(
        state,
        pdno="005930",
        qty_filled=3,
        sell_ts="2025-01-01T10:00:00+09:00",
        strategy_id=1,
    )
    lots = state["lots"]
    assert lots[0]["remaining_qty"] == 2
    assert lots[1]["remaining_qty"] == 5

    apply_sell_fill_fifo(
        state,
        pdno="005930",
        qty_filled=4,
        sell_ts="2025-01-01T10:10:00+09:00",
        strategy_id=1,
    )
    assert lots[0]["remaining_qty"] == 0
    assert lots[1]["remaining_qty"] == 5


def test_record_entry_state_accumulates() -> None:
    state = {"positions": {}}
    state = record_entry_state(
        state=state,
        code="000001",
        qty=10,
        avg_price=100.0,
        strategy_id=1,
        engine="test",
        entry_reason="init",
        order_type="limit",
        best_k=None,
        tgt_px=None,
        gap_pct_at_entry=None,
        flags={"bear_s1_done": True},
    )
    state = record_entry_state(
        state=state,
        code="000001",
        qty=5,
        avg_price=110.0,
        strategy_id=1,
        engine="test",
        entry_reason="add",
        order_type="limit",
        best_k=None,
        tgt_px=None,
        gap_pct_at_entry=None,
        flags={"bear_s2_done": True},
    )
    entry = state["positions"]["000001"]["strategies"]["1"]
    assert entry["qty"] == 15
    assert round(entry["avg_price"], 2) == round((10 * 100 + 5 * 110) / 15, 2)
    assert entry["flags"]["bear_s1_done"] is True
    assert entry["flags"]["bear_s2_done"] is True
    assert entry["flags"]["sold_p1"] is False
    assert entry["entry"].get("time")
    assert entry["entry"].get("last_entry_time")


def test_strategy_scoped_sell() -> None:
    lot_state = {
        "lots": [
            {"pdno": "000001", "strategy_id": 1, "remaining_qty": 5, "entry_price": 100.0},
            {"pdno": "000001", "strategy_id": 2, "remaining_qty": 4, "entry_price": 105.0},
        ]
    }
    allocations = allocate_sell_qty(
        lot_state,
        "000001",
        3,
        scope="strategy",
        trigger_strategy_id=1,
    )
    sold_total = apply_sell_allocation(
        lot_state,
        "000001",
        allocations,
        sell_ts="2025-01-01T10:00:00+09:00",
    )
    assert sold_total == 3
    assert remaining_qty_for_strategy(lot_state, "000001", 1) == 2
    assert remaining_qty_for_strategy(lot_state, "000001", 2) == 4


def test_migrate_position_state_v1() -> None:
    legacy_state = {
        "schema_version": 1,
        "positions": {
            "000001": {
                "entries": {
                    "1": {
                        "qty": 3,
                        "avg_price": 100.0,
                        "entry": {"time": "t1"},
                        "meta": {},
                    }
                },
                "flags": {"bear_s1_done": True, "bear_s2_done": False},
            }
        },
        "memory": {"last_price": {}, "last_seen": {}},
    }
    migrated = migrate_position_state(legacy_state)
    assert migrated["schema_version"] == 2
    strategies = migrated["positions"]["000001"]["strategies"]
    assert "1" in strategies
    assert strategies["1"]["qty"] == 3
    assert strategies["1"]["avg_price"] == 100.0
    assert strategies["1"]["flags"]["bear_s1_done"] is True


def test_global_liquidation_orphan_priority() -> None:
    lot_state = {
        "lots": [
            {"pdno": "000001", "strategy_id": "MANUAL", "remaining_qty": 2, "entry_price": 90.0},
            {"pdno": "000001", "strategy_id": 1, "remaining_qty": 4, "entry_price": 100.0},
            {"pdno": "000001", "strategy_id": 2, "remaining_qty": 3, "entry_price": 105.0},
        ]
    }
    allocations = allocate_sell_qty(
        lot_state,
        "000001",
        9,
        scope="global",
        trigger_strategy_id=None,
    )
    assert allocations[0]["strategy_id"] == "MANUAL"
    sold_total = apply_sell_allocation(
        lot_state,
        "000001",
        allocations,
        sell_ts="2025-01-01T10:00:00+09:00",
    )
    assert sold_total == 9
    assert remaining_qty_for_strategy(lot_state, "000001", "MANUAL") == 0
    assert remaining_qty_for_strategy(lot_state, "000001", 1) == 0
    assert remaining_qty_for_strategy(lot_state, "000001", 2) == 0


def test_normalize_ctx_missing_setup_flag() -> None:
    ctx = normalize_daily_ctx({"strong_trend": True})
    assert ctx.get("setup_flag") is False
    assert ctx.get("setup_ok") is False


def test_normalize_code() -> None:
    assert normalize_code("A476830") == "476830"
    assert normalize_code("123") == "000123"
    assert normalize_code("00123456") == "123456"


def test_idempotent_order_block() -> None:
    state = runtime_state_store.load_state()
    ts = "2025-01-01T10:00:00+09:00"
    runtime_state_store.mark_order(
        state,
        "000001",
        "BUY",
        1,
        1,
        100.0,
        ts,
        status="submitted",
    )
    assert runtime_state_store.should_block_order(
        state, "000001", "BUY", "2025-01-01T10:01:00+09:00"
    )


def test_rejected_order_does_not_update_window() -> None:
    state: dict = {"orders": {}, "order_windows": {}}
    ts = "2025-01-02T10:00:00+09:00"
    oid = runtime_state_store.mark_order(
        state,
        "000002",
        "BUY",
        "S1",
        1,
        100.0,
        ts,
        status="rejected",
        update_window=False,
        rejection_reason="closed",
    )
    assert not state.get("order_windows")
    omap = load_order_map_index()
    assert omap.get(oid, {}).get("status") == "rejected"


def main() -> None:
    test_fifo_and_fallback()
    test_record_entry_state_accumulates()
    test_strategy_scoped_sell()
    test_migrate_position_state_v1()
    test_global_liquidation_orphan_priority()
    test_normalize_ctx_missing_setup_flag()
    test_idempotent_order_block()
    print("OK")


if __name__ == "__main__":
    main()
