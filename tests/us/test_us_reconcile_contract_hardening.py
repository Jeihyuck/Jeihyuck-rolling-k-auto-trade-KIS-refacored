from __future__ import annotations


def _pending_sell_order():
    return {
        "symbol": "AMD",
        "side": "SELL",
        "order_no": "O1",
        "client_order_key": "CK1",
        "qty_requested": 7,
        "pre_order_position_qty": 7,
        "limit_price": 100,
    }


def test_ack_reconcile_non_dict_mark_result_is_failure(monkeypatch):
    from trader.us.execution import reconcile

    monkeypatch.setattr(
        "trader.us.db.repos.load_pending_ack_orders",
        lambda trade_date, env="practice": [_pending_sell_order()],
    )
    monkeypatch.setattr(
        "trader.us.db.repos.mark_order_filled_by_reconcile",
        lambda **kwargs: None,
    )

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):
            return {
                "status": "OK",
                "filled_qty": 7,
                "avg_price": 100,
                "symbol": "AMD",
                "side": "SELL",
                "order_no": "O1",
            }

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date="2026-07-17"
    )

    assert result["status"] == "ERROR"
    assert result["confirmed_count"] == 0
    assert result["failed_count"] == 1
    assert result["unresolved_count"] == 1


def test_ack_reconcile_rejects_regressed_provider_snapshot(monkeypatch):
    from trader.us.execution import reconcile

    monkeypatch.setattr(
        "trader.us.db.repos.load_pending_ack_orders",
        lambda trade_date, env="practice": [_pending_sell_order()],
    )
    calls = []
    monkeypatch.setattr(
        "trader.us.db.repos.mark_order_filled_by_reconcile",
        lambda **kwargs: calls.append(kwargs) or {"status": "OK"},
    )

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):
            return {
                "status": "EVIDENCE_QUANTITY_REGRESSION",
                "filled_qty": 3,
                "avg_price": 100,
                "symbol": "AMD",
                "side": "SELL",
                "order_no": "O1",
            }

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date="2026-07-17"
    )

    assert result["status"] == "ERROR"
    assert result["confirmed_count"] == 0
    assert result["failed_count"] == 1
    assert calls == []


def test_authoritative_zero_fill_rejection_releases_claim_for_fresh_retry(monkeypatch, tmp_path):
    from datetime import date
    import sqlalchemy as sa
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db import repos
    from trader.us.db.execution_claim_schema import (
        metadata,
        us_execution_attempts,
        us_execution_claims,
    )
    from trader.us.execution.order_journal import append_order_event
    from trader.us.execution.reconcile import reconcile_ack_orders_with_balance

    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path / "journal"))
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'reject-claim.sqlite'}")
    metadata.create_all(engine)
    claim_repo = DurableExecutionClaimRepo(
        engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    identity = SemanticActionIdentity(
        env="practice",
        account_id="reject-recovery-account",
        market="US",
        trading_epoch_id="reject-recovery-epoch",
        strategy_owner="US_STANDARD",
        lifecycle_id="reject-recovery-lifecycle",
        action="SELL_TRIM",
        trade_date=date.fromisoformat("2026-10-05"),
    )
    attempt_id = "reject-attempt-1"
    client_order_key = "reject-order-key"
    assert claim_repo.acquire(
        identity, attempt_id=attempt_id, requested_qty=2,
        client_order_key=client_order_key,
    ).acquired
    claim_repo.record_observation(
        identity, attempt_id=attempt_id, state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    intent = {
        "trade_date": "2026-10-05",
        "client_order_key": client_order_key,
        "submit_attempt_id": attempt_id,
        "symbol": "REJECT_TEST",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty": 2,
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "reject-recovery-lifecycle",
        "meta": {
            "execution_action_key": identity.action_key,
            "submit_attempt_id": attempt_id,
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": "reject-recovery-lifecycle",
        },
    }
    assert repos.save_order_intent(intent, trade_date="2026-10-05")
    assert repos.save_order_ack({
        **intent,
        "qty_requested": 2,
        "qty_filled": 0,
        "order_no": "reject-broker-order",
        "status": "ACK",
        "meta": intent["meta"],
    }, trade_date="2026-10-05")
    append_order_event("BROKER_SUBMIT_STARTED", intent)
    append_order_event(
        "BROKER_ACK_RECEIVED", intent, broker_order_no="reject-broker-order",
    )

    class Provider:
        def get_balance(self, **_kwargs):
            return {"positions": []}

        def get_fills_by_order_no(self, **_kwargs):
            return {
                "status": "REJECTED",
                "requested_qty": 2,
                "filled_qty": 0,
                "symbol": "REJECT_TEST",
                "side": "SELL",
                "order_no": "reject-broker-order",
            }

    result = reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date="2026-10-05",
    )

    assert result["status"] == "OK"
    assert result["rejected_count"] == 1
    assert result["unresolved_count"] == 0
    assert repos._MEM_ORDERS[0]["status"] == "REJECTED"
    assert claim_repo.get(identity).action_state == "RETRYABLE"
    assert claim_repo.get(identity).active_attempt_id is None
    retry = claim_repo.acquire(
        identity, attempt_id="reject-attempt-2", requested_qty=2,
        fresh_validation=True, client_order_key="reject-order-key-retry",
    )
    assert retry.acquired
    engine.dispose()


def test_rejection_without_explicit_zero_fill_evidence_stays_fenced(monkeypatch, tmp_path):
    from trader.us.execution.reconcile import is_terminal_zero_fill_rejection

    assert not is_terminal_zero_fill_rejection({
        "status": "REJECTED", "requested_qty": 2,
    })
    assert not is_terminal_zero_fill_rejection({
        "status": "REJECTED", "requested_qty": 2, "filled_qty": 1,
    })
    assert is_terminal_zero_fill_rejection({
        "status": "거부", "requested_qty": 2, "filled_qty": 0,
    })


def test_close_does_not_retry_reconcile_without_trade_date(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    calls = []

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK"}

    def broken_reconcile(provider, *, trade_date):
        calls.append(trade_date)
        raise TypeError("internal reconcile bug")

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr(
        "trader.us.execution.fills.get_fills_today",
        lambda **kwargs: {"status": "OK", "fills": []},
    )
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", broken_reconcile)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "trader.us.execution.reconcile.classify_ack_orders_with_final_balance",
        lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0},
    )
    monkeypatch.setattr(
        "trader.us.runner.daily_report_runner.run_daily_report",
        lambda **kwargs: {"status": "OK", "report": {"report_consistency": "OK"}},
    )

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(
        env="practice", offline=False, force_now="2026-07-17T16:05:00-04:00"
    )

    assert calls == ["2026-07-17"]
    assert result["status"] == "ERROR"
    assert result["reconcile_status"] == "CONTRACT_ERROR"


def test_close_position_snapshot_failure_is_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK"}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr(
        "trader.us.execution.fills.get_fills_today",
        lambda **kwargs: {"status": "OK", "fills": []},
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda provider, trade_date: {
            "status": "OK",
            "balance_fetch_status": "OK",
            "balance_parse_status": "OK",
            "authoritative_positions": True,
            "preserve_previous_positions": False,
            "positions": [],
        },
    )
    monkeypatch.setattr(
        "trader.us.db.repos.save_position_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "trader.us.execution.reconcile.classify_ack_orders_with_final_balance",
        lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0},
    )
    monkeypatch.setattr(
        "trader.us.runner.daily_report_runner.run_daily_report",
        lambda **kwargs: {"status": "OK", "report": {"report_consistency": "OK"}},
    )

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(
        env="practice", offline=False, force_now="2026-07-17T16:05:00-04:00"
    )

    assert result["status"] == "ERROR"
    assert result["report_consistency"] == "FAILED"
    assert result["position_snapshot_error"] == "db down"


def test_close_reconcile_position_persist_error_is_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK"}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr(
        "trader.us.execution.fills.get_fills_today",
        lambda **kwargs: {"status": "OK", "fills": []},
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda provider, trade_date: {
            "status": "POSITION_PERSIST_ERROR",
            "balance_fetch_status": "OK",
            "authoritative_positions": True,
            "preserve_previous_positions": True,
            "positions": [{"symbol": "AMD", "qty": 3}],
            "error": "db down",
        },
    )
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "trader.us.execution.reconcile.classify_ack_orders_with_final_balance",
        lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0},
    )
    monkeypatch.setattr(
        "trader.us.runner.daily_report_runner.run_daily_report",
        lambda **kwargs: {"status": "OK", "report": {"report_consistency": "OK"}},
    )

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(
        env="practice", offline=False, force_now="2026-07-17T16:05:00-04:00"
    )

    assert result["status"] == "ERROR"
    assert result["reconcile_status"] == "POSITION_PERSIST_ERROR"
    assert result["report_consistency"] == "FAILED"

def test_ack_reconcile_quarantines_fill_when_broker_requested_qty_differs(monkeypatch):
    from trader.us.execution import reconcile
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    assert repos.save_order_ack(
        {
            "client_order_key": "CK-MISMATCH",
            "symbol": "AMD",
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty_requested": 2,
            "order_no": "O-MISMATCH",
            "status": "ACK",
            "meta": {"pre_order_position_qty": 2},
        },
        "2026-09-24",
    )

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):
            return {
                "status": "CANCELLED",
                "requested_qty": 3,
                "filled_qty": 2,
                "cumulative_filled_qty": 2,
                "remaining_qty": 0,
                "avg_price": 100.0,
                "symbol": "AMD",
                "side": "SELL",
                "order_no": "O-MISMATCH",
            }

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=Provider(),
        trade_date="2026-09-24",
        env="practice",
    )

    assert result["confirmed_count"] == 0
    assert result["failed_count"] == 1
    assert result["unresolved_count"] == 1
    assert repos._MEM_ORDERS[0]["status"] == "ACK"
    assert repos._MEM_ORDERS[0]["qty_filled"] == 0
    assert repos._MEM_FILLS == []


def test_apply_broker_observation_quarantines_filled_request_identity_mismatch(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    assert repos.save_order_ack(
        {
            "client_order_key": "CK-DIRECT-MISMATCH",
            "symbol": "AMD",
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty_requested": 2,
            "order_no": "O-DIRECT-MISMATCH",
            "status": "ACK",
        },
        "2026-09-24",
    )

    applied = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key="CK-DIRECT-MISMATCH",
        raw_order_no="O-DIRECT-MISMATCH",
        canonical_order_no="O-DIRECT-MISMATCH",
        symbol="AMD",
        side="SELL",
        requested_qty=2,
        filled_qty=2,
        remaining_qty=0,
        broker_status="FILLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row={
            "status": "CANCELLED",
            "requested_qty": 3,
            "filled_qty": 2,
            "remaining_qty": 0,
            "avg_price": 100.0,
        },
    )

    assert applied["status"] == "BROKER_OBSERVATION_QUARANTINED"
    assert applied["reason"] == "fill_requested_qty_mismatch"
    assert applied["local_requested_qty"] == 2
    assert applied["broker_requested_qty"] == 3
    assert repos._MEM_ORDERS[0]["status"] == "ACK"
    assert repos._MEM_FILLS == []

def test_full_fill_correction_supersedes_prior_zero_fill_cancel(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    assert repos.save_order_ack(
        {
            "client_order_key": "CK-CANCEL-CORRECT",
            "symbol": "AMD",
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty_requested": 2,
            "order_no": "O-CANCEL-CORRECT",
            "status": "ACK",
        },
        "2026-09-24",
    )

    cancelled = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key="CK-CANCEL-CORRECT",
        raw_order_no="O-CANCEL-CORRECT",
        canonical_order_no="O-CANCEL-CORRECT",
        symbol="AMD",
        side="BUY",
        requested_qty=2,
        filled_qty=0,
        remaining_qty=0,
        broker_status="CANCELLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row={
            "status": "CANCELLED",
            "requested_qty": 2,
            "filled_qty": 0,
            "remaining_qty": 0,
        },
    )
    assert cancelled["status"] == "OK"
    assert cancelled["order_status"] == "CANCELLED"
    assert repos._MEM_ORDERS[0]["status"] == "CANCELLED"
    assert repos._MEM_ORDERS[0]["qty_filled"] == 0
    assert repos._MEM_FILLS == []

    corrected = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key="CK-CANCEL-CORRECT",
        raw_order_no="O-CANCEL-CORRECT",
        canonical_order_no="O-CANCEL-CORRECT",
        symbol="AMD",
        side="BUY",
        requested_qty=2,
        filled_qty=2,
        remaining_qty=0,
        broker_status="CANCELLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row={
            "status": "CANCELLED",
            "requested_qty": 2,
            "filled_qty": 2,
            "cumulative_filled_qty": 2,
            "remaining_qty": 0,
            "avg_price": 100.0,
        },
    )

    assert corrected["status"] == "OK"
    assert corrected["order_status"] == "FILLED"
    assert repos._MEM_ORDERS[0]["status"] == "FILLED"
    assert repos._MEM_ORDERS[0]["qty_filled"] == 2
    assert len(repos._MEM_FILLS) == 1
    assert repos._MEM_FILLS[0]["qty"] == 2
    assert repos._MEM_FILLS[0]["price_usd"] == 100.0


def test_unvalidated_terminal_transition_still_cannot_supersede_cancel(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    assert repos.save_order_ack(
        {
            "client_order_key": "CK-CANCEL-STAYS",
            "symbol": "AMD",
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty_requested": 2,
            "order_no": "O-CANCEL-STAYS",
            "status": "CANCELLED",
            "qty_filled": 0,
        },
        "2026-09-24",
    )

    result = repos.apply_broker_order_observation(
        trade_date="2026-09-24",
        client_order_key="CK-CANCEL-STAYS",
        raw_order_no="O-CANCEL-STAYS",
        canonical_order_no="O-CANCEL-STAYS",
        symbol="AMD",
        side="BUY",
        requested_qty=2,
        filled_qty=2,
        remaining_qty=0,
        broker_status="FILLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row={
            "status": "FILLED",
            "requested_qty": 2,
            "filled_qty": 2,
            "remaining_qty": 0,
            "avg_price": 100.0,
        },
    )

    assert result["status"] == "OK"
    assert result["order_status"] == "CANCELLED"
    assert result["observation_ignored"] == "ORDER_OBSERVATION_IGNORED_STALE"
    assert repos._MEM_ORDERS[0]["status"] == "CANCELLED"
    assert repos._MEM_FILLS == []
