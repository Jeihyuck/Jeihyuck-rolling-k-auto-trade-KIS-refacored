from trader.kr.runner.trade_session_runner import _balance_market_value, close_balance_guard, reconcile_kr_order_counts
from trader.rate_limit import KisCallGate


def test_close_balance_inconsistent_rows_zero_market_value_positive():
    result = close_balance_guard(
        holdings_count=0,
        market_value=2589451,
        raw_balance={"output1": [], "output2": {"tot_evlu_amt": "2589451"}},
    )

    assert result.status == "WARN_BALANCE_INCONSISTENT"
    assert result.retryable == 1
    assert result.completed == 0


def test_close_balance_cash_only_is_not_inconsistent():
    raw_balance = {"output1": [], "output2": {"tot_evlu_amt": "100000000", "dnca_tot_amt": "100000000"}}
    result = close_balance_guard(
        holdings_count=0,
        market_value=_balance_market_value(raw_balance),
        raw_balance=raw_balance,
    )

    assert result.status == "OK"
    assert result.retryable == 0
    assert result.completed == 1


def test_priority_requests_still_consume_endpoint_bucket(monkeypatch):
    monkeypatch.setenv("KIS_GLOBAL_RPS_PRACTICE", "100")
    monkeypatch.setenv("KIS_ENDPOINT_RPS_BALANCE", "1")
    gate = KisCallGate()
    first = gate.acquire(environment="practice", account_key="test", endpoint_category="balance", priority=True)
    second = gate.acquire(environment="practice", account_key="test", endpoint_category="balance", priority=True)
    assert first == 0
    assert second > 0


def test_kr_order_counts_reconcile_ack_db_failure_and_balance_confirmation():
    result = reconcile_kr_order_counts(
        engine_order_count=6,
        broker_ack_count=6,
        db_ack_count=5,
        ack_db_failed_count=1,
        balance_confirmed_count=6,
        filled_confirmed_count=6,
    )
    assert result["unresolved_ack_count"] == 0
    assert result["manual_reconcile_required"] == 1
    assert result["status"] == "BALANCE_CONFIRMED_AFTER_ACK_DB_FAILED"
    assert result["reconciliation_source"] == "engine_order_ledger_close_balance"
