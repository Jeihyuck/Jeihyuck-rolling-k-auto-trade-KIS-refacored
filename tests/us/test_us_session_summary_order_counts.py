from trader.us.runner.trade_session_runner import resolve_session_order_counts


def test_journal_session_and_daily_counts_override_tick_snapshots():
    # The four tick contributions are 5+3+4+4 (16 total); the observed final
    # tick snapshot is arranged last to reproduce the erroneous summary value 5.
    ticks = [{"orders_sent": n, "orders_ack": n} for n in (3, 4, 4, 5)]
    result = resolve_session_order_counts(
        tick_results=ticks, prior_cumulative={"orders_sent": 0, "orders_ack": 0},
        journal_session={"orders_sent_total": 16, "orders_ack_total": 16, "orders_reject_total": 0},
        journal_daily={"orders_sent_total": 17, "orders_ack_total": 17},
    )
    assert ticks[-1]["orders_ack"] == 5
    assert result["session_orders_ack"] == 16
    assert result["daily_cumulative_orders_ack"] == 17
    assert result["order_count_source"] == "order_journal"
