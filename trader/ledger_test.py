from __future__ import annotations

from trader.ledger import apply_sell_fill_fifo, record_buy_fill


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
    assert lots[1]["remaining_qty"] == 3


def main() -> None:
    test_fifo_and_fallback()
    print("OK")


if __name__ == "__main__":
    main()
