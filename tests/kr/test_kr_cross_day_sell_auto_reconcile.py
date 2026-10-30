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
    assert selected is None and reason == "ORIGINAL_SELL_IDENTITY_OR_QTY_INVALID"


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
    assert selected is None and reason == "ORIGINAL_SELL_IDENTITY_OR_QTY_INVALID"


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
    monkeypatch.setattr(recovery, "_load_pending_orders", lambda **kw: [_order()])
    monkeypatch.setattr(recovery, "_verify_original_sell_economics", lambda **kw: (True, "ECONOMIC_SETTLED"))
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
    monkeypatch.setattr(recovery, "_load_pending_orders", lambda **kw: [_order()])
    monkeypatch.setattr(recovery, "_verify_original_sell_economics", lambda **kw: (True, "ECONOMIC_SETTLED"))
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
    def no_db_read(**kwargs):
        assert kwargs["env"] == "practice"
        assert kwargs["strategy"] == "pb1_pullback_close"
        raise TimeoutError("database unavailable")
    monkeypatch.setattr(recovery, "_load_pending_orders", no_db_read)
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
    monkeypatch.setattr(recovery, "_load_pending_orders", lambda **kw: list(orders))
    monkeypatch.setattr(recovery, "_verify_original_sell_economics", lambda **kw: (True, "ECONOMIC_SETTLED"))
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



def test_terminal_filled_with_missing_economics_still_retries():
    selected, blocked = recovery.classify_pending_historical_sells(
        [_order(status="FILLED", _economic_repair_required=True)],
        today=date(2026, 10, 30), epoch_id="current-epoch",
    )
    assert len(selected) == 1 and not blocked


def test_actual_trading_reconcile_has_no_historical_calls():
    from pathlib import Path
    source = Path("trader/reconcile_kis.py").read_text(encoding="utf-8")
    # Only the post-close separate process may call historical original-day
    # KIS proof; never under synchronous protective SELL/BUIY reconciliation.
    assert "run_one_historical_sell_recovery(" not in source
    assert "run_all_historical_sell_recovery(" not in source


def test_close_wrapper_waits_for_trading_child_before_historical_repair():
    from pathlib import Path
    source = Path("scripts/wsl/run-kr-close.sh").read_text(encoding="utf-8")
    assert source.index("python -m trader.kr.runner.trade_session_runner") < source.index(
        "python -m scripts.kr_cross_day_sell_reconcile_close")
    assert "DISABLE_REAL_TRADING=1 NO_TRADE=1" in source
    assert 'if [[ "$rc" -eq 0' in source


def test_sqlite_all_statuses_and_native_economic_gap():
    """Exercise the *actual* SQLAlchemy schema and historical SELL queue."""
    import uuid
    import sqlalchemy as sa
    from trader.db.schema import schema_for_engine

    def new_id():
        return str(uuid.uuid4())

    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    epoch = new_id()
    portfolio = new_id()
    cycle_a = new_id()
    cycle_b = new_id()
    order_a = new_id()
    order_b = new_id()
    now = datetime(2026, 10, 27, 6, 16, tzinfo=timezone.utc)
    def order_row(oid, cycle, code, no, status):
        return dict(order_id=oid, env="practice", strategy="pb1_pullback_close",
                    sid=1, mode=1, code=code, side="SELL", ord_type="MARKET",
                    market="KRX", qty=33, client_order_key="kr292:" + oid,
                    status=status, kis_odno=no, broker_order_id=no,
                    trading_epoch_id=epoch, portfolio_epoch_id=portfolio,
                    position_cycle_id=cycle, submitted_at=now, created_at=now,
                    request_json={"reason": "KR_CLOSE_DAY_FORCE_EOD"},
                    response_json={"confirmed_fill_qty": 33, "confirmed_fill_price": 24000,
                                   "confirmed_fill_notional": 792000})
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.orders), [
            order_row(order_a, cycle_a, "028050", "0000034802", "FILLED"),
            order_row(order_b, cycle_b, "253590", "0000034803", "RECONCILED"),
        ])
        conn.execute(sa.insert(schema.positions), [
            dict(position_id=new_id(), env="practice", strategy="pb1_pullback_close",
                 sid=1, mode=1, code="028050", market="KRX", qty=0,
                 status="CLOSED", position_cycle_id=cycle_a, portfolio_epoch_id=portfolio,
                 trading_epoch_id=epoch, total_cost=0, realized_pnl=-50000,
                 avg_buy_price=None, closed_reason="FULL_SELL"),
            dict(position_id=new_id(), env="practice", strategy="pb1_pullback_close",
                 sid=1, mode=1, code="253590", market="KRX", qty=33,
                 status="OPEN", position_cycle_id=cycle_b, portfolio_epoch_id=portfolio,
                 trading_epoch_id=epoch, total_cost=825000, realized_pnl=0,
                 avg_buy_price=25000),
        ])
        conn.execute(sa.insert(schema.fills), dict(
            fill_id=new_id(), env="practice", order_id=order_a, code="028050",
            market="KRX", side="SELL", qty=33, price=24000,
            trading_epoch_id=epoch, portfolio_epoch_id=portfolio,
            position_cycle_id=cycle_a, trade_id="kr292-exact-native", fee=0,
            tax=0, filled_at=now, raw_json={},
            fill_meta_json={"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION"},
        ))
    try:
        rows = recovery._load_pending_orders(
            engine=engine, env="practice", strategy="pb1_pullback_close", epoch_id=epoch,
        )
        assert [r["code"] for r in rows] == ["253590"], rows
        assert rows[0]["_economic_repair_required"] is True
        selected, blocked = recovery.classify_pending_historical_sells(
            rows, today=date(2026, 10, 30), epoch_id=epoch,
        )
        assert len(selected) == 1 and not blocked
        assert selected[0]["code"] == "253590"
        with engine.connect() as conn:
            assert conn.execute(sa.select(schema.positions.c.qty).where(
                schema.positions.c.code == "253590")).scalar() == 33
    finally:
        engine.dispose()


def test_sqlite_sell_quantity_and_epoch_mismatch_never_pass_as_settled():
    import uuid
    import sqlalchemy as sa
    from trader.db.schema import schema_for_engine
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    uid = lambda: str(uuid.uuid4())
    order_id, cycle, portfolio, epoch = uid(), uid(), uid(), uid()
    original = _order(
        order_id=order_id, position_cycle_id=cycle,
        portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
    )
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.orders), dict(
            order_id=order_id, env="practice", strategy="pb1_pullback_close",
            sid=1, mode=1, code="028050", market="KRX", side="SELL",
            ord_type="MARKET", qty=33, client_order_key="kr292:" + order_id,
            status="FILLED", kis_odno="0000034802",
            portfolio_epoch_id=portfolio, position_cycle_id=cycle,
            trading_epoch_id=epoch, request_json={"reason": "KR_CLOSE_DAY_FORCE_EOD"},
            response_json={"confirmed_fill_qty": 33, "confirmed_fill_notional": 792000,
                           "confirmed_fill_price": 24000},
        ))
        conn.execute(sa.insert(schema.positions), dict(
            position_id=uid(), env="practice", strategy="pb1_pullback_close",
            sid=1, mode=1, code="028050", market="KRX", qty=0, status="CLOSED",
            total_cost=0, realized_pnl=0, position_cycle_id=cycle,
            portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
            closed_reason="FULL_SELL",
        ))
    try:
        good, reason = recovery._verify_original_sell_economics(engine=engine, source=original)
        assert not good and reason == "NATIVE_PRICED_SELL_FILL_GAP"
        old_epoch = dict(original, trading_epoch_id=uid())
        good, reason = recovery._verify_original_sell_economics(engine=engine, source=old_epoch)
        assert not good and reason == "ORIGINAL_ORDER_NOT_FILLED"
    finally:
        engine.dispose()
