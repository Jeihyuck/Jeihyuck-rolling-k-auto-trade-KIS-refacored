"""Residual incident regressions: safety, audit populations and completed-bar IO."""
from copy import deepcopy
from datetime import datetime, timedelta
import time

import pytest

from trader.us.runner.status_contract import expected_pending_settlement
from trader.us.decision_evidence_5w1h import attach_standard_sell_evidence, summarize_order_evidence
from trader.us.entry_exit_contract import build_us_entry_exit_contract


def settled_inputs():
    return dict(before={"status": "OK", "pending_count": 0, "unresolved_count": 0},
                after={"status": "ACK_PENDING_RECONCILE", "pending_count": 1, "unresolved_count": 1},
                orders=[{"status": "ACK", "ack": {"order_no": "000123"},
                         "intent": {"client_order_key": "native-sell"}}])


def test_native_ack_is_expected_settlement_until_independent_fill_reconcile():
    args = settled_inputs()
    assert expected_pending_settlement(**args)
    assert args["after"]["unresolved_count"] == 1  # fence and counts retained


@pytest.mark.parametrize("change", ["unknown_submit", "db_ack_failed", "missing_number", "prior_pending",
    "prior_unresolved", "prior_open", "unknown_ack_status", "manual_failure", "extra_pending", "prior_reconcile_gate"])
def test_expected_settlement_never_hides_actual_integrity_failure(change):
    args = settled_inputs()
    if change == "unknown_submit": args["orders"].append({"status": "BROKER_SUBMIT_RESULT_UNKNOWN"})
    if change == "db_ack_failed": args["orders"][0]["status"] = "ACK_DB_FAILED"
    if change == "missing_number": args["orders"][0]["ack"] = {}
    if change == "prior_pending": args["before"]["pending_count"] = 1
    if change == "prior_unresolved": args["before"]["unresolved_count"] = 1
    if change == "prior_open": args["before"]["open_order_pending_count"] = 1
    if change == "unknown_ack_status": args["after"]["status"] = "ERROR"
    if change == "manual_failure": args["manual_reconcile_required"] = True
    if change == "extra_pending": args["after"]["unresolved_count"] = 2
    if change == "prior_reconcile_gate": args["reconcile_entry_block"] = True
    assert not expected_pending_settlement(**args)


@pytest.mark.parametrize("symbol", ["KO", "KR", "MCK", "PFE", "AAPL", "MSFT", "BRK.B", "GENERIC"])
@pytest.mark.parametrize("reason", ["hard_stop", "soft_stop", "TAKE_PROFIT_TP2", "defense_trim"])
def test_legacy_original_contract_records_current_sell_without_fabricating_buy(symbol, reason):
    contract = build_us_entry_exit_contract({"symbol": symbol, "strategy_owner": "US_STANDARD",
                                           "book": "SWING_BOOK", "horizon": "SWING_CARRY"})
    original = deepcopy(contract)
    position = {"symbol": symbol, "strategy_owner": "US_STANDARD", "position_lifecycle_id": "original-cycle",
                "meta": {"entry_exit_contract": contract}}
    intent = {"symbol": symbol, "side": "SELL", "qty": 3, "limit_price": 100,
              "reason": reason, "client_order_key": "key-" + symbol, "meta": {"decision_price": 100}}
    status = attach_standard_sell_evidence(intent, position, decision_at="2026-11-18T15:33:00Z", code_sha="revision")
    assert status == "CURRENT_DECISION_VERIFIED_LEGACY_ENTRY"
    evidence = summarize_order_evidence(intent["meta"], side="SELL")
    assert evidence["status"] == status
    assert evidence["decision_reason"] == reason
    assert evidence["entry_link"]["sha256"] == original["sha256"]
    assert evidence["entry_link"]["historical_buy_decision_status"] == "NOT_AVAILABLE_IN_ORIGINAL_CONTRACT"
    assert "entry_decision_evidence_5w1h" not in contract["entry_provenance"]
    assert contract == original
    intent["meta"]["exit_decision_observation_5w1h"]["how"]["sizing"]["qty"] = 99
    assert summarize_order_evidence(intent["meta"], side="SELL")["status"] == "INVALID"


def test_infinite_has_no_standard_observation():
    intent = {"symbol": "TQQQ", "side": "SELL", "qty": 3, "reason": "GRID"}
    before = deepcopy(intent)
    assert attach_standard_sell_evidence(intent, {"symbol": "TQQQ"}, decision_at="now", code_sha="sha") == "NOT_APPLICABLE"
    assert intent == before


def test_ledger_numbers_normalize_and_open_orders_are_not_missing_fills(monkeypatch):
    from trader.us.execution import order_journal as journal
    from trader.us.runner.close_audit import audit_us_close_report
    events = []
    for attempt, number, filled in [("mck", "40538", True), ("pfe", "41936", True), ("open", "90001", False)]:
        for kind in ["BROKER_SUBMIT_STARTED", "BROKER_ACK_RECEIVED", "DB_ACK_PERSISTED"] + (["ORDER_FILLED"] if filled else []):
            events.append({"submit_attempt_id": attempt, "client_order_key": attempt, "side": "SELL",
                           "event_type": kind, "broker_order_no": number if kind != "ORDER_FILLED" else number.zfill(10)})
    monkeypatch.setattr(journal, "load_order_events", lambda *a, **kw: events)
    totals = journal.aggregate_order_events("2026-11-18")
    assert totals["unique_broker_order_count"] == 3  # 2 filled + 1 genuinely open
    assert totals["orders_fill_confirmed"] == 2
    assert totals["orders_sent_total"] == 3
    report = {**totals, "trade_date": "2026-11-18", "broker_recovery_health": {"available": True},
              "kis_actual_fill_order_count": 2, "daily_fills_confirmed_total": 2}
    assert audit_us_close_report(report)["findings"] == []
    report["kis_actual_fill_order_count"] = 1
    assert "BROKER_ORDER_NUMBER_COVERAGE_REVIEW" in {x["code"] for x in audit_us_close_report(report)["findings"]}


def test_boundary_is_not_a_tick_recovery(monkeypatch):
    from trader.us.execution.tp_retry_boundary import repair_tp_retries_at_boundary
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: pytest.fail("nonboundary DB access"))
    for phase in ("TICK", "AFTERNOON", "", "worker"):
        assert repair_tp_retries_at_boundary(trade_date="2026-11-18", phase=phase)["status"] == "SKIPPED_NOT_BOUNDARY"


@pytest.mark.parametrize("symbol", ["KO", "KR", "PG", "GENERIC"])
def test_legacy_status_without_durable_event_is_not_unsent_proof(symbol):
    from trader.us.execution.tp_retry_boundary import _release_certificate_row
    class Empty:
        def execute(self, *a, **k): return self
        def mappings(self): return self
        def all(self): return []
    row = {"trade_date": "2026-11-11", "symbol": symbol, "stage": "tp2", "position_lifecycle_id": "cycle",
           "client_order_key": "old-key", "stage_status": "NOT_TRIGGERED", "cumulative_filled_qty": 0}
    assert not _release_certificate_row(Empty(), row, {"trading_epoch_id": "epoch", "account_id": "account"})


def test_completed_daily_io_before_after_same_production_path(monkeypatch, capsys):
    """Controlled latency measures production calls, keeping the same bar outputs."""
    from trader.us.data_provider import USDataProvider
    from trader.us.execution.tick_context import TickExecutionContext
    from trader.us.db import price_daily_repo as repo
    rows = [{"date": (datetime(2026, 11, 17) - timedelta(days=259-i)).date(),
             "open": 99, "high": 101, "low": 98, "close": 100, "volume": 10000} for i in range(260)]
    calls = []
    def load(**kw):
        calls.append(kw["symbol"])
        # 20 ms fixed simulated remote DB roundtrip, not a live latency
        # claim. The former 2 ms was dominated by runner scheduling noise.
        time.sleep(.020)
        return deepcopy(rows)
    monkeypatch.setattr(repo, "load_recent_us_daily_bars", load)
    monkeypatch.setattr(repo, "get_latest_us_daily_date", lambda **kw: pytest.fail("duplicate latest-date query"))
    provider = USDataProvider(offline=True)
    symbols = ["SPY", "QQQ", "SMH", "RSP", "IWM", "XLK"]
    # Compare equivalent end-to-end work, not DB-only latency against
    # DB + calendar + audit + normalization + defensive copies. The old
    # path had three reads per evaluation; replay its two redundant reads
    # with identical controlled I/O and run the actual uncached production
    # evaluator for the remaining read. Both paths return normalized bars.
    started = time.perf_counter()
    before = []
    for repeat in range(2):
        for sym in symbols:
            load(symbol=sym); load(symbol=sym)
            result = provider.get_completed_daily_prices_result(
                sym, "NYSE", trade_date="2026-11-18", required_bars=260,
                allow_http_sync=False,
            )
            assert result["quality"] == "OK"
            before.append(result)
    before_sec = time.perf_counter() - started
    before_calls = len(calls); calls.clear()
    provider.bind_tick_context(TickExecutionContext("2026-11-18", "am", "run", 1, "tick1"))
    started = time.perf_counter(); after = []
    for repeat in range(2):
        for sym in symbols:
            result = provider.get_completed_daily_prices_result(sym, "NYSE", trade_date="2026-11-18", required_bars=260, allow_http_sync=False)
            assert result["quality"] == "OK"
            after.append(result)
    after_sec = time.perf_counter() - started
    assert len(calls) == 6 and before_calls == 36
    assert len(before) == len(after) == 12
    assert all(len(x["rows"]) == 260 for x in after)
    assert before == after  # same bars AND quality/provenance metadata
    # Exact DB-call reduction is the deterministic performance contract.
    # Keep the end-to-end controlled-latency assertion as well.
    assert before_calls - len(calls) == 30
    assert after_sec < before_sec
    print({"before_db_reads": before_calls, "after_db_reads": len(calls),
           "before_ms": round(before_sec*1000, 2), "after_ms": round(after_sec*1000, 2)})
    # Mutable callers cannot corrupt a shared cached snapshot.
    after[0]["rows"][0]["close"] = -999.0
    assert provider.get_completed_daily_prices_result("SPY", "NYSE", trade_date="2026-11-18", required_bars=260, allow_http_sync=False)["rows"][0]["close"] == 100.0
    after[0]["rows"].clear()
    assert len(provider.get_completed_daily_prices_result("SPY", "NYSE", trade_date="2026-11-18", required_bars=260, allow_http_sync=False)["rows"]) == 260
    provider.bind_tick_context(TickExecutionContext("2026-11-18", "am", "run", 1, "tick2"))
    provider.get_completed_daily_prices_result("SPY", "NYSE", trade_date="2026-11-18", required_bars=260, allow_http_sync=False)
    assert len(calls) == 7  # no cross-tick reuse

# Run atomic repair and rollback against the disposable CI PostgreSQL service.
from tests.us.test_us_postgres_integration_real import pg_engine
import os
import json


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires disposable PostgreSQL")
@pytest.mark.parametrize("symbol,contradiction", [("KO", None), ("KR", None), ("GENERIC", None),
    ("KO", "BROKER_SUBMIT_STARTED"), ("KR", "BROKER_ACK_RECEIVED"),
    ("GENERIC", "ORDER_PARTIALLY_FILLED"), ("PFE", "ORDER_CANCELLED"), ("MCK", "NO_CERTIFICATE")])
def test_postgres_boundary_release_requires_exact_certificate_and_no_broker_evidence(
    pg_engine, monkeypatch, symbol, contradiction,
):
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.execution.tp_retry_boundary import repair_tp_retries_at_boundary, CERTIFICATE
    epoch, account, cycle = "residual-epoch", "residual-account", "original-cycle"
    key = f"US_PC_2026-11-11_{symbol}_{cycle}_TAKE_PROFIT_TP2"
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: epoch)
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *a, **kw: epoch)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": account, "trading_epoch_id": epoch})
    repos.reset_memory_stores()
    _seed_boundary_case(pg_engine, symbol=symbol, contradiction=contradiction,
                        epoch=epoch, account=account, cycle=cycle, key=key)
    # Use the actual production trade_session_runner boundary name.
    first = repair_tp_retries_at_boundary(trade_date="2026-11-18", phase="AM_START")
    assert first["status"] != "UNAVAILABLE", first
    assert first["released"] == (0 if contradiction else 1)
    with pg_engine.connect() as conn:
        state = conn.execute(text("SELECT state FROM us_profit_capture_lifecycle WHERE client_order_key=:key"), {"key": key}).scalar_one()
        assert not state.get("tp2_done")
        assert state["meta"]["tp2_order_keys"] == [key]
        assert conn.execute(text("SELECT count(*) FROM us_orders")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) FROM us_fills")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) FROM us_execution_attempts")).scalar_one() == 0
        intent_status = conn.execute(text("SELECT status FROM us_order_intents WHERE client_order_key=:key"), {"key": key}).scalar_one()
        assert intent_status == ("PENDING" if contradiction else "BLOCKED")
        if not contradiction:
            assert state["meta"]["last_evidence_type"] == CERTIFICATE
    second = repair_tp_retries_at_boundary(trade_date="2026-11-18", phase="CLOSE")
    assert second["released"] == 0  # restart/idempotence; never recreate a trade


def _seed_boundary_case(pg_engine, *, symbol, contradiction, epoch, account, cycle, key):
    from sqlalchemy import text
    from trader.us.execution.tp_retry_boundary import CERTIFICATE
    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO us_order_intents
            (trade_date,client_order_key,symbol,exchange,side,qty,status,trading_epoch_id,meta)
            VALUES ('2026-11-11',:key,:symbol,'NYSE','SELL',3,'PENDING',:epoch,'{}'::jsonb)"""),
            {"key": key, "symbol": symbol, "epoch": epoch})
        conn.execute(text("""INSERT INTO us_profit_capture_lifecycle
          (trade_date,symbol,position_lifecycle_id,stage,stage_status,client_order_key,
           requested_qty,cumulative_filled_qty,trading_epoch_id,state)
          VALUES ('2026-11-11',:symbol,:cycle,'tp2','NOT_TRIGGERED',:key,3,0,:epoch,CAST(:state AS jsonb))"""),
          {"symbol": symbol, "cycle": cycle, "key": key, "epoch": epoch,
           "state": json.dumps({"position_lifecycle_id": cycle, "tp2_order_key": key,
               "meta": {"tp2_order_keys": [key], "last_stage": "tp2",
                        "last_status": "US_RECOVERY_SCOPE_UNVERIFIED"}})})
        if contradiction != "NO_CERTIFICATE":
            for n, typ in enumerate([CERTIFICATE] + ([contradiction] if contradiction else [])):
                payload = {"symbol": symbol, "side": "SELL", "profit_capture_stage": "tp2",
                           "strategy_owner": "US_STANDARD", "broker_account_hash": account,
                           "meta": {"router_abort_checkpoint": "original_scope_rejected"}}
                conn.execute(text("""INSERT INTO us_order_events
                    (event_id,requested_qty,trade_date,event_type,event_timestamp,client_order_key,submit_attempt_id,
                     symbol,side,position_lifecycle_id,profit_capture_stage,trading_epoch_id,
                     payload,idempotency_key)
                    VALUES (CAST(:event_id AS uuid),3,'2026-11-11',:type,NOW(),:key,'router-attempt',:symbol,'SELL',
                      :cycle,'tp2',:epoch,CAST(:payload AS jsonb),:idem)"""),
                    {"event_id": str(__import__('uuid').uuid4()), "type": typ, "key": key, "symbol": symbol, "cycle": cycle,
                     "epoch": epoch, "payload": json.dumps(payload), "idem": f"{symbol}-{n}"})


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires disposable PostgreSQL")
def test_postgres_repair_rolls_back_intent_and_continues_unrelated_symbol(pg_engine, monkeypatch):
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.execution.tp_retry_boundary import repair_tp_retries_at_boundary
    epoch, account, cycle = "residual-epoch", "residual-account", "original-cycle"
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: epoch)
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *a, **kw: epoch)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": account, "trading_epoch_id": epoch})
    for symbol in ("KO", "KR"):
        key = f"US_PC_2026-11-11_{symbol}_{cycle}_TAKE_PROFIT_TP2"
        _seed_boundary_case(pg_engine, symbol=symbol, contradiction=None, epoch=epoch,
                            account=account, cycle=cycle, key=key)
    original = repos.mark_us_profit_capture_stage
    def fail_after_intent_update(trade_date, symbol, *args, **kwargs):
        if symbol == "KO":
            raise RuntimeError("injected_stage_write_failure")
        return original(trade_date, symbol, *args, **kwargs)
    monkeypatch.setattr(repos, "mark_us_profit_capture_stage", fail_after_intent_update)
    result = repair_tp_retries_at_boundary(trade_date="2026-11-18", phase="AM")
    assert result["released"] == 1 and result["unverified"][0]["symbol"] == "KO"
    with pg_engine.connect() as conn:
        rows = dict(conn.execute(text("SELECT symbol,status FROM us_order_intents ORDER BY symbol")).all())
        assert rows == {"KO": "PENDING", "KR": "BLOCKED"}


@pytest.mark.parametrize("mismatch", ["account_id", "trading_epoch_id", "trade_date", "unverified"])
def test_different_order_fill_scopes_are_not_compared_as_a_count_divergence(mismatch):
    from trader.us.runner.close_audit import audit_us_close_report
    scope = {"verified": True, "account_id": "account", "trading_epoch_id": "epoch", "trade_date": "2026-11-18"}
    other = dict(scope)
    if mismatch == "unverified": other["verified"] = False
    else: other[mismatch] = "another-scope"
    report = {"trade_date": "2026-11-18", "broker_recovery_health": {"available": True},
              "kis_actual_fill_order_count": 2, "daily_fills_confirmed_total": 0, "unique_broker_order_count": 3,
              "journal_order_population": {"filled_identity_scope": scope}, "broker_fill_identity_scope": other}
    codes = {x["code"] for x in audit_us_close_report(report)["findings"]}
    assert codes == {"ORDER_FILL_SCOPE_UNVERIFIED"}


def test_scope_proof_rejects_mixed_accounts_and_missing_epoch():
    from trader.us.execution.report_scope import population_scope
    row = {"trade_date": "2026-11-18", "account_id": "a", "trading_epoch_id": "e"}
    assert population_scope([row, row], "2026-11-18")["verified"]
    assert not population_scope([row, {**row, "account_id": "other"}], "2026-11-18")["verified"]
    assert not population_scope([{**row, "trading_epoch_id": None}], "2026-11-18")["verified"]


def test_partial_pre_io_boundary_uses_original_atomic_claim_checks(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution.tp_retry_boundary import _release_certificate_row
    class Conn:
        def execute(self, *a, **kw): return self
        def mappings(self): return self
        def all(self): return [{"event_type": "BROKER_SUBMIT_ABORTED_PRE_IO",
                                "trade_date": "2026-11-11", "trading_epoch_id": "epoch"}]
    conn = Conn()
    captured = {}
    def checked_release(**kw):
        captured.update(kw)
        return False  # independent claim evidence missing: must stay fenced
    monkeypatch.setattr(repos, "release_us_profit_capture_pre_http_abort", checked_release)
    row = {"trade_date": "2026-11-11", "symbol": "GENERIC", "stage": "tp2", "position_lifecycle_id": "original",
           "client_order_key": "retry-key", "stage_status": "PENDING", "cumulative_filled_qty": 3,
           "canonical_broker_order_no": "prior-fill-order"}
    assert not _release_certificate_row(conn, row, {"account_id": "account", "trading_epoch_id": "epoch"})
    assert captured["_conn"] is conn
    assert captured["close_intent"] is True


def test_cancelled_unfilled_day_is_not_a_missing_fill_population():
    from trader.us.runner.close_audit import audit_us_close_report
    empty = {"verified": False, "trade_date": "2026-11-18", "reason": "EMPTY_OR_MIXED_SOURCE_SCOPE"}
    report = {"trade_date": "2026-11-18", "broker_recovery_health": {"available": True},
              "kis_actual_fill_order_count": 0, "daily_fills_confirmed_total": 0, "unique_broker_order_count": 1,
              "journal_order_population": {"scope": "trade_day", "trade_date": "2026-11-18",
                  "filled_identity_scope": empty, "filled_broker_order_numbers": []},
              "broker_fill_identity_scope": empty}
    assert audit_us_close_report(report)["findings"] == []


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires disposable PostgreSQL")
@pytest.mark.parametrize("mismatch", ["account_id", "lifecycle"])
def test_postgres_certificate_cannot_change_foreign_intent_scope(pg_engine, monkeypatch, mismatch):
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.execution.tp_retry_boundary import repair_tp_retries_at_boundary
    epoch, account, cycle = "residual-epoch", "residual-account", "original-cycle"
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: epoch)
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *a, **kw: epoch)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": account, "trading_epoch_id": epoch})
    key = f"US_PC_2026-11-11_KO_{cycle}_TAKE_PROFIT_TP2"
    _seed_boundary_case(pg_engine, symbol="KO", contradiction=None, epoch=epoch, account=account, cycle=cycle, key=key)
    with pg_engine.begin() as conn:
        if mismatch == "account_id":
            conn.execute(text("UPDATE us_order_intents SET account_id='foreign-account' WHERE client_order_key=:key"), {"key": key})
        else:
            conn.execute(text("UPDATE us_order_intents SET meta=CAST(:meta AS jsonb) WHERE client_order_key=:key"),
                         {"key": key, "meta": json.dumps({"position_lifecycle_id": "foreign-cycle"})})
    result = repair_tp_retries_at_boundary(trade_date="2026-11-18", phase="AM")
    assert result["released"] == 0
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT status FROM us_order_intents WHERE client_order_key=:key"), {"key": key}).scalar_one() == "PENDING"



def test_native_partial_fill_is_confirmed_without_claiming_terminal_completion(monkeypatch):
    from trader.us.execution import order_journal as journal
    from trader.us.runner.close_audit import audit_us_close_report
    events = [{"submit_attempt_id": "partial", "client_order_key": "partial-key", "side": "SELL",
               "event_type": kind, "broker_order_no": "000123", "cumulative_filled_qty": 2,
               "trade_date": "2026-11-18", "account_id": "account", "trading_epoch_id": "epoch"}
              for kind in ("BROKER_SUBMIT_STARTED", "BROKER_ACK_RECEIVED", "DB_ACK_PERSISTED", "ORDER_PARTIALLY_FILLED")]
    monkeypatch.setattr(journal, "load_order_events", lambda *a, **kw: events)
    totals = journal.aggregate_order_events("2026-11-18")
    assert totals["orders_fill_confirmed"] == 0  # remaining native order stays open
    assert totals["orders_with_confirmed_fills"] == 1
    report = {**totals, "trade_date": "2026-11-18", "broker_recovery_health": {"available": True},
              "kis_actual_fill_order_count": 1, "daily_fills_confirmed_total": totals["orders_with_confirmed_fills"],
              "broker_fill_identity_scope": totals["journal_order_population"]["filled_identity_scope"]}
    assert audit_us_close_report(report)["findings"] == []


from tests.us.test_us_legacy_nine_active_sell_20261111 import scenario, _authorize, OLD_CYCLE


@pytest.mark.parametrize("tamper", [False, True])
def test_forward_abort_certificate_binds_original_stage_using_pr368_proof(scenario, monkeypatch, tamper):
    from trader.us.db import repos
    from trader.us.execution import order_journal
    from trader.us.execution.tp_retry_boundary import record_router_abort
    db, client, intent = scenario
    ok, reason, proof = _authorize(scenario)
    assert ok, reason
    intent["client_order_key"] = f"US_PC_2026-11-11_ABBV_{OLD_CYCLE}_TAKE_PROFIT_TP2"
    runtime = intent["position_lifecycle_id"]
    assert runtime != OLD_CYCLE
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": intent["meta"]["broker_account_hash"],
        "trading_epoch_id": intent["meta"]["trading_epoch_id"]})
    recorded = []
    monkeypatch.setattr(order_journal, "append_order_event", lambda kind, event, **kw: recorded.append((kind, event, kw)))
    if tamper:
        intent["meta"]["source_original_runtime_lifecycle_id"] = "another-original-cycle"
    assert record_router_abort(intent, checkpoint="original_scope_rejected") is (not tamper)
    if tamper:
        assert not recorded
    else:
        assert recorded[0][1]["position_lifecycle_id"] == OLD_CYCLE
        assert recorded[0][1]["meta"]["router_abort_runtime_lifecycle_id"] == runtime
        assert recorded[0][2]["require_db"] is True
        assert intent["position_lifecycle_id"] == runtime  # no source or runtime retagging


def test_completed_daily_copy_isolates_nested_provenance():
    from trader.us.data_provider import _copy_completed_daily_result
    original = {"quality": "OK", "rows": [{"close": 100.0, "extra": {"source": ["native"]}}]}
    copied = _copy_completed_daily_result(original)
    copied["rows"][0]["extra"]["source"].append("corrupt")
    assert original["rows"][0]["extra"]["source"] == ["native"]
    flat = {"quality": "OK", "rows": [{"close": 100.0, "xymd": "20261123"}]}
    copied = _copy_completed_daily_result(flat)
    copied["rows"][0]["close"] = -1
    copied["rows"].clear()
    assert flat["rows"] == [{"close": 100.0, "xymd": "20261123"}]


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires disposable PostgreSQL")
def test_tp_boundary_budget_retains_every_unexamined_candidate(pg_engine, monkeypatch):
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.execution.tp_retry_boundary import repair_tp_retries_at_boundary
    epoch,account,cycle="budget-epoch","budget-account","original-cycle"
    monkeypatch.setattr(repos,"_active_us_epoch",lambda *a,**kw:epoch)
    monkeypatch.setattr(repos,"_us_state_epoch_id",lambda *a,**kw:epoch)
    monkeypatch.setattr(repos,"_us_execution_claim_scope",lambda:{"env":"practice","account_id":account,"trading_epoch_id":epoch})
    for symbol in ("KO","PG"):
        _seed_boundary_case(pg_engine,symbol=symbol,contradiction=None,epoch=epoch,account=account,
                           cycle=cycle,key=f"US_PC_2026-11-11_{symbol}_{cycle}_TAKE_PROFIT_TP2")
    # The real SQL read still executes; only the budget clock is injected.
    import time
    calls=[0]
    def clock():
        calls[0]+=1
        return 0.0 if calls[0]==1 else 31.0
    monkeypatch.setattr(time,"monotonic",clock)
    result=repair_tp_retries_at_boundary(trade_date="2026-11-18",phase="AM_START",budget_sec=.1)
    assert result["released"]==0 and result["checked"]==0
    assert result["candidate_count"]==2
    assert {x["symbol"] for x in result["unverified"]}=={"KO","PG"}
    assert all(x["reason"]=="DEFERRED_BOUNDARY_BUDGET" for x in result["unverified"])
    with pg_engine.connect() as conn:
        assert set(conn.execute(text("SELECT status FROM us_order_intents")).scalars())=={"PENDING"}
