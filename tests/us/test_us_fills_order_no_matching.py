from trader.us.db import repos

PAIRS = [("AMAT", "0000031806", "31806"), ("AMD", "0000031809", "31809"),
         ("CRDO", "0000031813", "31813"), ("MRVL", "0000031817", "31817"),
         ("CRWD", "0000031819", "31819"), ("GOOGL", "0000031822", "31822"),
         ("QQQ", "0000031824", "31824"), ("QQQM", "0000031828", "31828")]
TD = "2026-07-20"


def setup_function():
    repos.reset_memory_stores()


def _ack(symbol, padded, i):
    assert repos.save_order_ack({"client_order_key": f"K{i}", "symbol": symbol, "side": "SELL", "qty_requested": 1, "order_no": padded, "status": "ACK"}, TD)


def _fill(symbol, unpadded):
    return repos.mark_order_filled_by_reconcile(order_no=unpadded, client_order_key="", symbol=symbol, side="SELL",
        filled_qty=1, requested_qty=1, cumulative_filled_qty=1, avg_price_usd=100, trade_date=TD,
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL")["status"]


def test_padded_ack_matches_unpadded_actual_fill_and_replay_is_idempotent():
    for i, (symbol, padded, unpadded) in enumerate(PAIRS): _ack(symbol, padded, i)
    assert [_fill(symbol, unpadded) for symbol, _, unpadded in PAIRS] == ["OK"] * 8
    assert repos.load_pending_ack_orders(TD) == []
    assert [_fill(symbol, unpadded) for symbol, _, unpadded in PAIRS] == ["OK"] * 8
    assert len(repos.load_today_fills(TD)) == 8
