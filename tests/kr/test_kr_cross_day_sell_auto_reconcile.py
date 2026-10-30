"""PR: cross-date PB1 SELL reconciliation is bounded, original-day and fail-closed."""
from datetime import date, datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from trader.kr import cross_day_sell_auto_reconcile as recovery

KST = ZoneInfo("Asia/Seoul")


def _order(**overrides):
    row = {
        "order_id": "order-one",
        "env": "practice",
        "strategy": "pb1_pullback_close",
        "code": "028050",
        "side": "SELL",
        "qty": 33,
        "status": "SUBMITTED",
        "kis_odno": "0000034802",
        "broker_order_id": "0000034802",
        "trading_epoch_id": "current-epoch",
        "position_cycle_id": "exact-cycle",
        "portfolio_epoch_id": "exact-portfolio",
        "submitted_at": datetime(2026, 10, 27, 6, 16, tzinfo=timezone.utc),
        "created_at": datetime(2026, 10, 27, 6, 16, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def test_oct27_samsung_ea_is_still_candidate_oct30():
    selected, reason = recovery.select_pending_historical_sell(
        [_order()], today=date(2026, 10, 30), epoch_id="current-epoch"
    )
    assert reason == "READY"
    assert selected["code"] == "028050"
    assert selected["qty"] == 33
    assert selected["kis_odno"] == "0000034802"


def test_no_arbitrary_day_cutoff_for_still_unsettled_sell():
    selected, reason = recovery.select_pending_historical_sell(
        [_order(submitted_at=datetime(2026, 3, 1, tzinfo=timezone.utc))],
        today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert reason == "READY" and selected is not None


def test_other_epoch_never_reconciled_under_current_account():
    selected, reason = recovery.select_pending_historical_sell(
        [_order(trading_epoch_id="previous-account-epoch")],
        today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert selected is None and reason == "NO_PRIOR_DAY_PENDING_SELL"


def test_duplicate_broker_identity_and_missing_cycle_fail_closed():
    selected, reason = recovery.select_pending_historical_sell(
        [_order(), _order(order_id="another-order")],
        today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert selected is None and reason == "DUPLICATE_ORIGINAL_SELL_IDENTITY"
    selected, reason = recovery.select_pending_historical_sell(
        [_order(position_cycle_id=None)],
        today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert selected is None and reason == "MISSING_ORIGINAL_SELL_IDENTITY"


def test_never_reconcile_current_day_or_filled_or_missing_broker_id():
    rows = [
        _order(status="FILLED"),
        _order(submitted_at=datetime(2026, 10, 30, 0, 1, tzinfo=timezone.utc)),
    ]
    selected, reason = recovery.select_pending_historical_sell(
        rows, today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert selected is None and reason == "NO_PRIOR_DAY_PENDING_SELL"
    selected, reason = recovery.select_pending_historical_sell(
        [_order(kis_odno=None, broker_order_id=None)],
        today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert selected is None and reason == "MISSING_ORIGINAL_SELL_IDENTITY"


def test_close_only_and_practice_only(monkeypatch):
    stamp = datetime(2026, 10, 30, 15, 20, tzinfo=KST)
    monkeypatch.setenv("PB1_SESSION", "am")
    assert not recovery.should_run_close_recovery(env="practice", tick_ts=stamp)
    monkeypatch.setenv("PB1_SESSION", "close")
    assert recovery.should_run_close_recovery(env="practice", tick_ts=stamp)
    assert not recovery.should_run_close_recovery(env="real", tick_ts=stamp)
    assert not recovery.should_run_close_recovery(
        env="practice", tick_ts=datetime(2026, 10, 30, 14, 0, tzinfo=KST)
    )
    assert not recovery.should_run_close_recovery(env="practice", tick_ts=stamp, enabled="0")


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _Engine:
    def connect(self):
        return _Connection()


def test_calls_original_day_authoritative_writer_once(monkeypatch):
    monkeypatch.setenv("PB1_SESSION", "close")
    monkeypatch.setattr(recovery, "get_account_key", lambda **kw: "practice-account")
    monkeypatch.setattr(recovery, "active_trading_epoch_id", lambda *a, **kw: "current-epoch")
    monkeypatch.setattr(
        recovery, "OrdersRepo",
        lambda engine: SimpleNamespace(
            get_open_orders=lambda *a, **kw: [_order()],
        ),
    )
    import trader.reconcile_kis as rk
    calls = []

    def verified_reconcile(**kwargs):
        calls.append(kwargs)
        return {"orders": 1, "fills": 1}

    monkeypatch.setattr(rk, "reconcile_today", verified_reconcile)
    import trader.kr.unpriced_sell_audit as audit
    monkeypatch.setattr(audit, "audit_kr_pending_sell_orders", lambda *a, **kw: {"orders": []})
    result = recovery.run_one_historical_sell_recovery(
        engine=_Engine(), kis=object(), env="practice",
        strategy="pb1_pullback_close",
        tick_ts=datetime(2026, 10, 30, 15, 20, tzinfo=KST),
        ctx=object(),
    )
    assert result["state"] == "BATCH_COMPLETE"
    assert len(calls) == 1
    assert calls[0]["trade_date"] == date(2026, 10, 27)
    assert calls[0]["require_authoritative"] is True
    assert calls[0]["strict_sell_targets"] == {"028050": ("0000034802", 33)}


def test_unverified_broker_response_keeps_retry_state(monkeypatch):
    monkeypatch.setenv("PB1_SESSION", "close")
    monkeypatch.setattr(recovery, "get_account_key", lambda **kw: "practice-account")
    monkeypatch.setattr(recovery, "active_trading_epoch_id", lambda *a, **kw: "current-epoch")
    monkeypatch.setattr(recovery, "OrdersRepo", lambda engine: SimpleNamespace(
        get_open_orders=lambda *a, **kw: [_order()],
    ))
    import trader.reconcile_kis as rk

    def failed_original_day(**kwargs):
        raise RuntimeError("KR_KIS_EXECUTION_EVIDENCE_NOT_AUTHORITATIVE")

    monkeypatch.setattr(rk, "reconcile_today", failed_original_day)
    result = recovery.run_one_historical_sell_recovery(
        engine=_Engine(), kis=object(), env="practice",
        strategy="pb1_pullback_close",
        tick_ts=datetime(2026, 10, 30, 15, 20, tzinfo=KST),
        ctx=object(),
    )
    assert result["state"] == "BATCH_WITH_PENDING"
    assert result["orders"][0]["error_type"] == "RuntimeError"


def test_order_read_failure_is_not_false_empty(monkeypatch):
    monkeypatch.setenv("PB1_SESSION", "close")
    monkeypatch.setattr(recovery, "get_account_key", lambda **kw: "practice-account")
    monkeypatch.setattr(recovery, "active_trading_epoch_id", lambda *a, **kw: "current-epoch")
    def no_db_read(*args, **kwargs):
        assert kwargs["fail_open"] is False
        raise TimeoutError("database unavailable")
    monkeypatch.setattr(recovery, "OrdersRepo", lambda engine: SimpleNamespace(
        get_open_orders=no_db_read,
    ))
    result = recovery.run_one_historical_sell_recovery(
        engine=_Engine(), kis=object(), env="practice",
        strategy="pb1_pullback_close",
        tick_ts=datetime(2026, 10, 30, 15, 20, tzinfo=KST),
        ctx=object(),
    )
    assert result["state"] == "RETRY_NEXT_CLOSE"


def test_all_symbols_are_recovered_even_when_one_proof_fails(monkeypatch):
    monkeypatch.setenv("PB1_SESSION", "close")
    monkeypatch.setattr(recovery, "get_account_key", lambda **kw: "account")
    monkeypatch.setattr(recovery, "active_trading_epoch_id", lambda *a, **kw: "current-epoch")
    orders = [
        _order(code="028050", order_id="one", kis_odno="0000034802"),
        _order(code="253590", order_id="two", kis_odno="0000013020"),
        _order(code="293490", order_id="three", kis_odno="0000034803"),
    ]
    monkeypatch.setattr(recovery, "OrdersRepo", lambda eng: SimpleNamespace(
        get_open_orders=lambda *a, **kw: list(orders),
    ))
    import trader.reconcile_kis as rk
    calls = []

    def fake_reconcile(**kwargs):
        code = next(iter(kwargs["strict_sell_targets"]))
        calls.append(code)
        if code == "253590":
            raise ValueError("unproven KIS execution amount")
        return {"orders": 1, "fills": 1}

    monkeypatch.setattr(rk, "reconcile_today", fake_reconcile)
    import trader.kr.unpriced_sell_audit as audit
    monkeypatch.setattr(audit, "audit_kr_pending_sell_orders", lambda *a, **kw: {"orders": []})
    result = recovery.run_all_historical_sell_recovery(
        engine=_Engine(), kis=object(), env="practice",
        strategy="pb1_pullback_close",
        tick_ts=datetime(2026, 10, 30, 15, 20, tzinfo=KST), ctx=object(),
    )
    assert result["eligible"] == 3
    assert result["attempted"] == 3
    assert result["pending"] == 1
    assert set(calls) == {"028050", "253590", "293490"}
    assert result["state"] == "BATCH_WITH_PENDING"


def test_invalid_one_symbol_does_not_starve_valid_other():
    valid, blocked = recovery.classify_pending_historical_sells(
        [_order(code="028050", order_id="bad", kis_odno="", broker_order_id=""),
         _order(code="293490", order_id="good", kis_odno="0000034803")],
        today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert len(valid) == 1 and valid[0]["order_id"] == "good"
    assert len(blocked) == 1 and blocked[0]["order_id"] == "bad"


def test_two_distinct_orders_same_symbol_both_processed():
    items, blocked = recovery.classify_pending_historical_sells(
        [_order(order_id="one", kis_odno="0000034802"),
         _order(order_id="two", kis_odno="0000034803")],
        today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert len(items) == 2 and not blocked
