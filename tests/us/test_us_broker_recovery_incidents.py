from __future__ import annotations

from datetime import datetime
from datetime import date
from zoneinfo import ZoneInfo

import sqlalchemy as sa
import pytest

from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.us.db.execution_claim_schema import (
    metadata as claim_metadata,
    us_execution_attempts,
    us_execution_claims,
)
from trader.us.db import repos
from trader.us.execution.fills import get_fills_today
from trader.us.execution.order_journal import append_order_event, load_order_events, replay_order_journal
from trader.us.execution.reconcile import is_terminal_zero_fill_cancel, reconcile_ack_orders_with_balance
from trader.us.execution.order_router import route_order


TRADE_DATE = "2026-10-01"


def _reset(monkeypatch, tmp_path):
    monkeypatch.setenv("US_ORDER_JOURNAL_DIR", str(tmp_path))
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()


def _local_submit(
    key: str, *, owner: str = "US_STANDARD", lifecycle: str = "life-1",
    action_key: str | None = None,
) -> dict:
    submitted_at_utc = "2026-10-01T14:00:00+00:00"
    meta = {
        "strategy_owner": owner,
        "position_lifecycle_id": lifecycle,
        "semantic_action": "DEFENSE_RISK_OFF_TRIM",
        "trend_stage": "trend_trim",
        "avg_cost": 250.0,
        "submitted_at_utc": submitted_at_utc,
        "execution_action_key": action_key or f"action-{key}",
        "submit_attempt_id": f"attempt-{key}",
    }
    intent = {
        "trade_date": TRADE_DATE,
        "client_order_key": key,
        "submit_attempt_id": meta["submit_attempt_id"],
        "symbol": "JNJ",
        "exchange": "NYSE",
        "side": "SELL",
        "qty": 2,
        "limit_price": 262.90,
        "submitted_at_utc": submitted_at_utc,
        "strategy_owner": owner,
        "position_lifecycle_id": lifecycle,
        "meta": meta,
    }
    assert repos.save_order_intent(intent, trade_date=TRADE_DATE)
    assert repos.save_order_ack({
        **intent,
        "qty_requested": 2,
        "qty_filled": 0,
        "order_no": "",
        "status": "ACK",
    }, trade_date=TRADE_DATE)
    append_order_event("BROKER_SUBMIT_STARTED", intent)
    return intent


def _broker_fill(
    *,
    order_no: str = "36025",
    order_timestamp_utc: str = "2026-10-01T14:00:00+00:00",
    exchange: str = "NYSE",
    limit_price: float | None = 262.90,
    strategy_owner: str | None = None,
) -> dict:
    meta = {
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "cumulative_filled_qty": 2,
        "requested_qty": 2,
        "order_timestamp": "2026-10-01T23:00:00+09:00",
        "order_timestamp_utc": order_timestamp_utc,
        "order_timestamp_source": "KIS_INQUIRE_CCNL_ORD_DT_ORD_TMD",
        "source_timezone": "Asia/Seoul",
        "source_timestamp_date": "20261001",
        "source_timestamp_time": "230000",
    }
    if strategy_owner:
        meta["strategy_owner"] = strategy_owner
    return {
        "symbol": "JNJ",
        "exchange": exchange,
        "side": "SELL",
        "qty": 2,
        "price": 262.905,
        "limit_price": limit_price,
        "order_no": order_no,
        "requested_qty": 2,
        "cumulative_filled_qty": 2,
        "remaining_qty": 0,
        "order_timestamp": "2026-10-01T23:00:00+09:00",
        "order_timestamp_utc": order_timestamp_utc,
        "order_timestamp_source": "KIS_INQUIRE_CCNL_ORD_DT_ORD_TMD",
        "order_timestamp_source_timezone": "Asia/Seoul",
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "meta": meta,
    }


def test_jnj_broker_fill_rebounds_to_unique_unresolved_local_intent(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'jnj-claims.sqlite'}")
    claim_metadata.create_all(engine)
    claim_repo = DurableExecutionClaimRepo(engine, us_execution_claims, us_execution_attempts)
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    claim_identity = SemanticActionIdentity(
        env="practice",
        account_id="recovery-test-account",
        market="US",
        trading_epoch_id="recovery-test-epoch",
        strategy_owner="US_STANDARD",
        lifecycle_id="life-1",
        action="DEFENSE_RISK_OFF_TRIM",
        trade_date=date.fromisoformat(TRADE_DATE),
    )
    attempt_id = "attempt-pb1-jinj-1"
    assert claim_repo.acquire(
        claim_identity, attempt_id=attempt_id, requested_qty=2,
        client_order_key="pb1-jinj-1",
    ).acquired
    claim_repo.record_observation(
        claim_identity, attempt_id=attempt_id, state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    original = _local_submit("pb1-jinj-1", action_key=claim_identity.action_key)
    repos.save_us_position_risk_state(
        "JNJ", TRADE_DATE,
        {"state": {
            "lifecycle": {"lifecycle_id": "life-1"},
            "trend": {
                "lifecycle_id": "life-1",
                "trend_trim_pending": True,
                "trend_trim_done": False,
            },
        }},
    )

    result = repos.save_fills_with_result([_broker_fill()], trade_date=TRADE_DATE)

    orders = repos._MEM_ORDERS
    assert result["status"] == "OK"
    assert len(orders) == 1
    assert orders[0]["client_order_key"] == original["client_order_key"]
    assert orders[0]["order_no"] == "36025"
    assert orders[0]["status"] == "FILLED"
    fill_meta = repos._MEM_FILLS[0]["meta"]
    assert fill_meta["strategy_owner"] == "US_STANDARD"
    assert fill_meta["position_lifecycle_id"] == "life-1"
    assert fill_meta["semantic_action"] == "DEFENSE_RISK_OFF_TRIM"
    assert fill_meta["cost_basis_price_usd"] == 250.0
    assert fill_meta["realized_pnl_usd"] == 25.81
    from trader.us.runner.trade_tick_runner import _mark_trend_stages_from_records
    _mark_trend_stages_from_records(repos._MEM_FILLS, trade_date=TRADE_DATE, status="FILLED")
    trend_state = repos.load_us_position_risk_state("JNJ", TRADE_DATE)["state"]["trend"]
    assert trend_state["trend_trim_pending"] is False
    assert trend_state["trend_trim_done"] is True
    assert claim_repo.get(claim_identity).action_state == "SATISFIED"
    engine.dispose()


def test_pb1_entry_fill_rebounds_only_to_matching_trade_date_generation(
    monkeypatch, tmp_path,
):
    _reset(monkeypatch, tmp_path)
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'pb1-generations.sqlite'}")
    claim_metadata.create_all(engine)
    claim_repo = DurableExecutionClaimRepo(
        engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    symbol = "ENTRY_TEST_SYMBOL"
    lifecycle = f"ENTRY:US_STANDARD:{symbol}"
    generations = []

    def persist_generation(trade_date: str, *, terminal: bool = False):
        identity = SemanticActionIdentity(
            env="practice",
            account_id="recovery-test-account",
            market="US",
            trading_epoch_id="recovery-test-epoch",
            strategy_owner="US_STANDARD",
            lifecycle_id=lifecycle,
            action="US_STANDARD_NEW_POSITION_BUY",
            trade_date=date.fromisoformat(trade_date),
            action_instance=trade_date,
        )
        attempt_id = f"attempt-entry-{trade_date}"
        order_key = f"pb1-entry-{trade_date}"
        assert claim_repo.acquire(
            identity, attempt_id=attempt_id, requested_qty=2,
            client_order_key=order_key,
        ).acquired
        claim_repo.record_observation(
            identity,
            attempt_id=attempt_id,
            state="FILLED" if terminal else "UNRESOLVED",
            cumulative_filled_qty=2 if terminal else None,
            authoritative=terminal,
        )
        submitted_at = f"{trade_date}T14:00:00+00:00"
        meta = {
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": lifecycle,
            "semantic_action": "US_STANDARD_NEW_POSITION_BUY",
            "action_instance": trade_date,
            "execution_action_key": identity.action_key,
            "submit_attempt_id": attempt_id,
            "submitted_at_utc": submitted_at,
            "limit_price_usd": 100.0,
            "frozen_buy_contract": {"entry_price_usd": 100.0},
        }
        intent = {
            "trade_date": trade_date,
            "client_order_key": order_key,
            "submit_attempt_id": attempt_id,
            "symbol": symbol,
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty": 2,
            "limit_price": 100.0,
            "submitted_at_utc": submitted_at,
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": lifecycle,
            "meta": meta,
        }
        assert repos.save_order_intent(intent, trade_date=trade_date)
        assert repos.save_order_ack({
            **intent,
            "qty_requested": 2,
            "qty_filled": 2 if terminal else 0,
            "order_no": "",
            "status": "FILLED" if terminal else "ACK",
        }, trade_date=trade_date)
        append_order_event("BROKER_SUBMIT_STARTED", intent)
        generations.append((identity, order_key))

    persist_generation("2026-10-02", terminal=True)
    persist_generation("2026-10-05")
    prior_identity, _ = generations[0]
    current_identity, current_key = generations[1]
    fill = {
        "symbol": symbol,
        "exchange": "NASDAQ",
        "side": "BUY",
        "qty": 2,
        "price": 100.0,
        "limit_price": 100.0,
        "order_no": "generation-order-2",
        "requested_qty": 2,
        "cumulative_filled_qty": 2,
        "remaining_qty": 0,
        "status": "FILLED",
        "order_timestamp": "2026-10-05T23:00:00+09:00",
        "order_timestamp_utc": "2026-10-05T14:00:00+00:00",
        "order_timestamp_source": "KIS_INQUIRE_CCNL_ORD_DT_ORD_TMD",
        "order_timestamp_source_timezone": "Asia/Seoul",
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "meta": {
            "requested_qty": 2,
            "cumulative_filled_qty": 2,
            "order_timestamp": "2026-10-05T23:00:00+09:00",
            "order_timestamp_utc": "2026-10-05T14:00:00+00:00",
            "order_timestamp_source": "KIS_INQUIRE_CCNL_ORD_DT_ORD_TMD",
            "source_timezone": "Asia/Seoul",
            "source_timestamp_date": "20261005",
            "source_timestamp_time": "230000",
        },
    }

    result = repos.save_fills_with_result([fill], trade_date="2026-10-05")

    assert result["status"] == "OK"
    assert len(repos._MEM_FILLS) == 1
    assert repos._MEM_FILLS[0]["client_order_key"] == current_key
    fill_meta = repos._MEM_FILLS[0]["meta"]
    assert fill_meta["strategy_owner"] == "US_STANDARD"
    assert fill_meta["position_lifecycle_id"] == lifecycle
    assert fill_meta["action_instance"] == "2026-10-05"
    assert fill_meta["execution_action_key"] == current_identity.action_key
    assert fill_meta["frozen_buy_contract"] == {"entry_price_usd": 100.0}
    assert claim_repo.get(current_identity).action_state == "SATISFIED"
    assert claim_repo.get(prior_identity).action_state == "SATISFIED"
    assert not any(
        str(row.get("client_order_key") or "").startswith("KIS_IMPORTED_")
        for row in repos._MEM_ORDERS
    )
    engine.dispose()


def test_multiple_submit_candidates_are_persisted_unattributed_not_guessed(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    _local_submit("pb1-candidate", owner="US_STANDARD", lifecycle="pb1-cycle")
    _local_submit("tqqq-candidate", owner="TQQQ_INFINITE", lifecycle="infinite-cycle")

    result = repos.save_fills_with_result([_broker_fill()], trade_date=TRADE_DATE)

    assert result["status"] == "UNATTRIBUTED_BROKER_FILL"
    assert not any(str(row["client_order_key"]).startswith("KIS_IMPORTED_") for row in repos._MEM_ORDERS)
    assert len(repos._MEM_FILLS) == 1
    assert repos._MEM_FILLS[0]["meta"]["attribution_status"] == "UNATTRIBUTED"
    assert repos._MEM_FILLS[0]["meta"].get("strategy_owner") is None


def test_active_claim_replay_skips_completed_attempts_and_queries_only_unresolved(
    monkeypatch, tmp_path,
):
    _reset(monkeypatch, tmp_path / "journal")
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'active-replay.sqlite'}")
    claim_metadata.create_all(engine)
    claim_repo = DurableExecutionClaimRepo(
        engine, us_execution_claims, us_execution_attempts,
    )
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)

    active_identity = None
    for key, attempt_id, action, terminal in (
        ("completed-attempt-key", "completed-attempt", "COMPLETED_ACTION", True),
        ("unresolved-attempt-key", "unresolved-attempt", "UNRESOLVED_ACTION", False),
    ):
        identity = SemanticActionIdentity(
            env="practice",
            account_id="active-replay-account",
            market="US",
            trading_epoch_id="active-replay-epoch",
            strategy_owner="US_STANDARD",
            lifecycle_id=f"lifecycle-{action}",
            action=action,
            trade_date=date.fromisoformat(TRADE_DATE),
        )
        assert claim_repo.acquire(
            identity, attempt_id=attempt_id, requested_qty=1,
            client_order_key=key,
        ).acquired
        claim_repo.record_observation(
            identity,
            attempt_id=attempt_id,
            state="FILLED" if terminal else "UNRESOLVED",
            cumulative_filled_qty=1 if terminal else None,
            authoritative=terminal,
        )
        intent = {
            "trade_date": TRADE_DATE,
            "client_order_key": key,
            "submit_attempt_id": attempt_id,
            "symbol": "REPLAY_TEST",
            "exchange": "NASDAQ",
            "side": "SELL",
            "qty": 1,
            "limit_price": 10,
            "submitted_at_utc": "2026-10-01T14:00:00+00:00",
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": identity.lifecycle_id,
            "meta": {
                "execution_action_key": identity.action_key,
                "submit_attempt_id": attempt_id,
                "strategy_owner": "US_STANDARD",
                "position_lifecycle_id": identity.lifecycle_id,
            },
        }
        assert repos.save_order_intent(intent, trade_date=TRADE_DATE)
        assert repos.save_order_ack({
            **intent,
            "qty_requested": 1,
            "qty_filled": 0,
            "order_no": f"broker-{attempt_id}",
            "status": "ACK",
            "meta": intent["meta"],
        }, trade_date=TRADE_DATE)
        append_order_event("BROKER_SUBMIT_STARTED", intent)
        append_order_event(
            "BROKER_ACK_RECEIVED", intent,
            broker_order_no=f"broker-{attempt_id}",
        )
        if not terminal:
            active_identity = identity

    class Provider:
        _offline = False
        _tick_context = None

        def __init__(self):
            self.balance_calls = 0
            self.order_calls = 0
            self.fill_calls = []

        def get_balance(self, **_kwargs):
            self.balance_calls += 1
            return {"positions": []}

        def get_today_orders(self, trade_date):
            self.order_calls += 1
            return [{
                "trade_date": TRADE_DATE,
                "order_no": "broker-unresolved-attempt",
                "symbol": "REPLAY_TEST",
                "exchange": "NASDAQ",
                "side": "SELL",
                "requested_qty": 1,
                "filled_qty": 0,
                "remaining_qty": 1,
                "status": "OPEN",
            }]

        def get_fills_by_order_no(self, *, order_no, **_kwargs):
            self.fill_calls.append(order_no)
            return None

        def _get_client(self):
            return self

        def get_us_fills_today(self, **_kwargs):
            return []

    provider = Provider()
    first = replay_order_journal(
        TRADE_DATE,
        provider=provider,
        include_active_claims=True,
        active_claims_only=True,
    )
    second = replay_order_journal(
        TRADE_DATE,
        provider=provider,
        include_active_claims=True,
        active_claims_only=True,
    )

    assert first["broker_unfilled_ack_count"] == second["broker_unfilled_ack_count"] == 1
    assert provider.fill_calls == ["broker-unresolved-attempt"] * 2
    assert provider.order_calls == provider.balance_calls == 2
    assert claim_repo.get(active_identity).active_attempt_id == "unresolved-attempt"
    claim_repo.record_observation(
        active_identity, attempt_id="unresolved-attempt", state="FILLED",
        cumulative_filled_qty=1, authoritative=True,
    )
    completed_replay = replay_order_journal(
        TRADE_DATE, provider=provider,
        include_active_claims=True, active_claims_only=True,
    )
    assert completed_replay["events_scanned"] == 0
    assert completed_replay["balance_fetched"] is False
    assert provider.fill_calls == ["broker-unresolved-attempt"] * 2
    engine.dispose()


@pytest.mark.parametrize(
    "fill",
    [
        _broker_fill(order_timestamp_utc="2026-10-01T17:00:00+00:00"),
        _broker_fill(exchange="NASDAQ"),
        _broker_fill(limit_price=263.25),
        _broker_fill(strategy_owner="TQQQ_INFINITE"),
    ],
    ids=("timestamp-outside-window", "exchange-mismatch", "limit-price-mismatch", "owner-mismatch"),
)
def test_weak_or_conflicting_single_candidate_is_not_rebound_or_imported(
    monkeypatch, tmp_path, fill,
):
    _reset(monkeypatch, tmp_path)
    _local_submit("weak-candidate")

    result = repos.save_fills_with_result([fill], trade_date=TRADE_DATE)

    assert result["status"] == "UNATTRIBUTED_BROKER_FILL"
    assert repos._MEM_ORDERS[0]["client_order_key"] == "weak-candidate"
    assert not any(str(row["client_order_key"]).startswith("KIS_IMPORTED_") for row in repos._MEM_ORDERS)
    assert repos._MEM_FILLS[0]["meta"]["attribution_status"] == "UNATTRIBUTED"
    assert repos._MEM_FILLS[0]["meta"].get("strategy_owner") is None


def test_exact_broker_order_journal_identity_precedes_fallback_evidence(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'exact-claims.sqlite'}")
    claim_metadata.create_all(engine)
    claim_repo = DurableExecutionClaimRepo(engine, us_execution_claims, us_execution_attempts)
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    identity = SemanticActionIdentity(
        env="practice", account_id="exact-recovery-account", market="US",
        trading_epoch_id="exact-recovery-epoch", strategy_owner="US_STANDARD",
        lifecycle_id="life-1", action="DEFENSE_RISK_OFF_TRIM",
        trade_date=date.fromisoformat(TRADE_DATE),
    )
    assert claim_repo.acquire(
        identity, attempt_id="attempt-exact-broker-order", requested_qty=2,
        client_order_key="exact-broker-order",
    ).acquired
    claim_repo.record_observation(
        identity, attempt_id="attempt-exact-broker-order", state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    intent = _local_submit(
        "exact-broker-order", action_key=identity.action_key,
    )
    append_order_event(
        "BROKER_ACK_RECEIVED", intent, broker_order_no="36025", broker_status="ACK",
    )
    fill = _broker_fill(
        order_timestamp_utc="2026-10-01T17:00:00+00:00",
        limit_price=263.25,
    )

    result = repos.save_fills_with_result([fill], trade_date=TRADE_DATE)

    assert result["status"] == "OK"
    assert len(repos._MEM_ORDERS) == 1
    assert repos._MEM_ORDERS[0]["client_order_key"] == "exact-broker-order"
    assert repos._MEM_FILLS[0]["meta"]["broker_recovery_evidence"] == "EXACT_BROKER_ORDER_JOURNAL"
    assert claim_repo.get(identity).action_state == "SATISFIED"
    engine.dispose()


def test_exact_broker_identity_does_not_override_conflicting_owner(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    intent = _local_submit("exact-owner-conflict")
    append_order_event(
        "BROKER_ACK_RECEIVED", intent, broker_order_no="36025", broker_status="ACK",
    )

    result = repos.save_fills_with_result(
        [_broker_fill(strategy_owner="TQQQ_INFINITE")], trade_date=TRADE_DATE,
    )

    assert result["status"] == "UNATTRIBUTED_BROKER_FILL"
    assert repos._MEM_ORDERS[0]["client_order_key"] == "exact-owner-conflict"
    assert not any(
        str(row["client_order_key"]).startswith("KIS_IMPORTED_")
        for row in repos._MEM_ORDERS
    )
    assert repos._MEM_FILLS[0]["meta"]["attribution_status"] == "UNATTRIBUTED"


def test_no_internal_candidate_keeps_auditable_unlinked_fill_in_memory(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)

    result = repos.save_fills_with_result([_broker_fill()], trade_date=TRADE_DATE)

    assert result["status"] == "OK"
    assert repos._MEM_ORDERS == []
    assert repos._MEM_FILLS[0]["client_order_key"].startswith("KIS_")
    assert repos._MEM_FILLS[0]["meta"]["order_origin"] == "broker_actual_without_local_order"


def test_cancel_without_explicit_zero_fill_quantity_is_not_terminal_zero_fill():
    from trader.us.data_provider import normalize_us_order_status_row

    assert not is_terminal_zero_fill_cancel({
        "status": "CANCELLED",
        "requested_qty": 2,
        "remaining_qty": 0,
    })
    assert is_terminal_zero_fill_cancel({
        "status": "CANCELLED",
        "requested_qty": 2,
        "filled_qty": 0,
        "remaining_qty": 0,
    })
    normalized = normalize_us_order_status_row({
        "odno": "terminal-cancel",
        "pdno": "XYZ",
        "sll_buy_dvsn_cd": "01",
        "ord_qty": "2",
        "ft_ccld_qty": "0",
        "nccs_qty": "0",
        "ord_sttus": "CANCELLED",
    })
    assert normalized["status"] == "CANCELLED"
    assert normalized["requested_qty_present"] is True
    assert normalized["filled_qty_present"] is True
    assert normalized["broker_open_qty"] == 0
    assert normalized["broker_open_qty_present"] is True
    assert is_terminal_zero_fill_cancel(normalized)


def test_cancel_ack_without_fill_quantity_stays_unresolved_in_reconciliation(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    assert repos.save_order_ack({
        "client_order_key": "cancel-unknown-fill",
        "symbol": "JNJ",
        "exchange": "NYSE",
        "side": "SELL",
        "qty_requested": 2,
        "qty_filled": 0,
        "order_no": "cancel-36025",
        "status": "ACK",
        "meta": {},
    }, trade_date=TRADE_DATE)

    class Provider:
        def get_balance(self, **_kwargs):
            return {"positions": [{"symbol": "JNJ", "qty": 8, "orderable_qty": 8}]}

        def get_fills_by_order_no(self, **_kwargs):
            return {"status": "CANCELLED", "remaining_qty": 0}

    result = reconcile_ack_orders_with_balance(provider=Provider(), trade_date=TRADE_DATE)

    assert result["unresolved_count"] == 1
    assert repos._MEM_ORDERS[0]["status"] == "CANCELLED"
    assert repos._MEM_ORDERS[0]["qty_filled"] == 0


def test_journal_cancel_without_quantity_is_unresolved_not_rejected(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    intent = {
        "trade_date": TRADE_DATE,
        "client_order_key": "cancel-unknown-journal",
        "symbol": "XYZ",
        "exchange": "NYSE",
        "side": "SELL",
        "qty": 2,
    }
    assert repos.save_order_ack({
        **intent,
        "qty_requested": 2,
        "qty_filled": 0,
        "order_no": "cancel-unknown-order",
        "status": "ACK",
        "meta": {},
    }, trade_date=TRADE_DATE)
    append_order_event("BROKER_SUBMIT_STARTED", intent)
    append_order_event(
        "BROKER_ACK_RECEIVED", intent, broker_order_no="cancel-unknown-order",
    )

    class Provider:
        def get_balance(self, **_kwargs):
            return {"positions": []}

        def get_today_orders(self, **_kwargs):
            return [{
                "order_no": "cancel-unknown-order",
                "symbol": "XYZ",
                "side": "SELL",
                "requested_qty": 2,
                "filled_qty": None,
                "filled_qty_present": False,
                "remaining_qty": 2,
                "remaining_qty_present": False,
                "status": "CANCELLED",
            }]

    result = replay_order_journal(
        TRADE_DATE, provider=Provider(),
    )
    assert result["unresolved_count"] == 1
    assert result["broker_rejected_count"] == 0


def test_kis_inquire_ccnl_timestamp_preserves_kst_source_and_date_boundary():
    class Client:
        def get_us_fills_today(self, trade_date=None):
            return [{
                "pdno": "JNJ",
                "ovrs_excg_cd": "NYSE",
                "sll_buy_dvsn_cd": "01",
                "odno": "36025",
                "ord_dt": "20261002",
                "ord_tmd": "001500",
                "ft_ord_qty": "2",
                "ft_ccld_qty": "2",
                "nccs_qty": "0",
                "ft_ord_unpr3": "262.90",
                "ft_ccld_unpr3": "262.905",
            }]

    class Provider:
        _offline = False
        _tick_context = None

        def _get_client(self):
            return Client()

    fills = get_fills_today(provider=Provider(), trade_date=TRADE_DATE)["fills"]

    assert fills[0]["order_timestamp"] == "2026-10-02T00:15:00+09:00"
    assert fills[0]["order_timestamp_utc"] == "2026-10-01T15:15:00+00:00"
    assert datetime.fromisoformat(fills[0]["order_timestamp"]).astimezone(
        ZoneInfo("America/New_York")
    ).isoformat() == "2026-10-01T11:15:00-04:00"
    assert fills[0]["meta"]["source_timezone"] == "Asia/Seoul"
    assert fills[0]["meta"]["source_timestamp_date"] == "20261002"
    assert fills[0]["meta"]["source_timestamp_time"] == "001500"
    assert fills[0]["order_price"] == 262.90


def test_broker_order_timestamp_conversion_handles_est_and_kst_midnight():
    from trader.us.data_provider import normalize_us_order_status_row

    normalized = normalize_us_order_status_row({
        "odno": "est-boundary",
        "pdno": "JNJ",
        "sll_buy_dvsn_cd": "01",
        "ft_ord_qty": "1",
        "ft_ccld_qty": "0",
        "nccs_qty": "1",
        "ord_dt": "20261102",
        "ord_tmd": "001500",
    })

    assert normalized["submitted_at_utc"] == "2026-11-01T15:15:00+00:00"
    assert datetime.fromisoformat(normalized["submitted_at_utc"]).astimezone(
        ZoneInfo("America/New_York")
    ).isoformat() == "2026-11-01T10:15:00-05:00"
    assert normalized["order_timestamp_source_timezone"] == "Asia/Seoul"
    assert normalized["source_timestamp_date"] == "20261102"
    assert normalized["source_timestamp_time"] == "001500"
    assert normalized["us_trade_date"] == "2026-11-01"


def test_ambiguous_submit_match_uses_et_date_for_kst_midnight_order():
    from trader.us.data_provider import normalize_us_order_status_row
    from trader.us.execution.order_journal import match_ambiguous_submit_to_broker_order

    broker_row = normalize_us_order_status_row({
        "odno": "boundary-order",
        "pdno": "XYZ",
        "sll_buy_dvsn_cd": "01",
        "ft_ord_qty": "2",
        "ft_ccld_qty": "0",
        "nccs_qty": "2",
        "ord_dt": "20261002",
        "ord_tmd": "001500",
        "ovrs_excg_cd": "NYSE",
    })
    assert broker_row["trade_date"] == "20261002"
    assert broker_row["us_trade_date"] == "2026-10-01"

    matched = match_ambiguous_submit_to_broker_order(
        trade_date="2026-10-01",
        symbol="XYZ",
        side="SELL",
        requested_qty=2,
        limit_price=0,
        submitted_at_utc="2026-10-01T15:15:00+00:00",
        exchange="NYSE",
        broker_rows=[broker_row],
    )
    assert matched["status"] == "MATCHED"


def test_broker_observation_timestamp_is_not_used_as_order_event_time():
    from trader.us.data_provider import normalize_us_order_status_row

    normalized = normalize_us_order_status_row({
        "odno": "observation-only",
        "pdno": "XYZ",
        "sll_buy_dvsn_cd": "01",
        "ft_ord_qty": "1",
        "ft_ccld_qty": "0",
        "nccs_qty": "1",
        "observed_at": "2026-10-01T15:15:00+00:00",
    })
    assert normalized["submitted_at_utc"] is None
    assert normalized["us_trade_date"] is None


def test_nvda_ambiguous_ack_recovers_original_claim_across_restart_and_date(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path / "journal")
    from trader.us.execution import order_router

    engine = sa.create_engine(f"sqlite:///{tmp_path / 'claims.sqlite'}")
    claim_metadata.create_all(engine)
    claim_repo = DurableExecutionClaimRepo(engine, us_execution_claims)
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: claim_repo)
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_args, **_kwargs: "incident-epoch")
    monkeypatch.setattr(order_router, "same_day_semantic_sell_exists", lambda _intent: False)
    monkeypatch.setattr(
        "trader.us.execution.order_identity.normalize_and_validate_order_identity",
        lambda intent, _context: intent,
    )
    monkeypatch.setattr(order_router, "canonical_order_risk_check", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(order_router, "resolve_dry_run_for_us_order", lambda: False)
    monkeypatch.setattr(order_router, "assert_order_allowed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repos, "claim_execution_action", claim_repo.acquire)
    monkeypatch.setattr(repos, "record_execution_action_observation", claim_repo.record_observation)
    monkeypatch.setattr(repos, "release_execution_action_before_submit", claim_repo.release_before_submit)
    monkeypatch.setattr(repos, "load_today_order_keys", lambda **_kwargs: set())

    class Broker:
        calls = 0
        response_lost = True

        def place_us_sell_order(self, *_args):
            self.calls += 1
            if self.response_lost:
                raise TimeoutError("ReadTimeout after broker accepted the order")
            return {"ok": True, "order_no": f"3602{self.calls}"}

    broker = Broker()
    action_intent = {
        "trade_date": TRADE_DATE,
        "client_order_key": "nvda-defense-trim-attempt",
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "side": "SELL",
        "qty": 3,
        "limit_price": 90.0,
        "notional_usd": 270.0,
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "nvda-lifecycle",
        "reason": "DEFENSE_RISK_OFF_TRIM",
        "semantic_action": "DEFENSE_RISK_OFF_TRIM",
        "submitted_at_utc": "2026-10-01T14:00:00+00:00",
        "holding_qty": 10,
        "orderable_qty": 10,
        "sellable_qty": 10,
        "available_qty": 10,
        "meta": {
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": "nvda-lifecycle",
            "semantic_action": "DEFENSE_RISK_OFF_TRIM",
            "reason": "DEFENSE_RISK_OFF_TRIM",
            "avg_cost": 80.0,
        },
    }

    first = route_order(action_intent, kis_client=broker)
    assert first["status"] == "BROKER_SUBMIT_RESULT_UNKNOWN"
    assert broker.calls == 1
    unresolved_next_date = route_order(
        {**action_intent, "trade_date": "2026-10-05", "client_order_key": "nvda-before-truth"},
        kis_client=broker,
    )
    assert unresolved_next_date["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert broker.calls == 1
    submit = next(
        event for event in load_order_events(TRADE_DATE)
        if event["event_type"] == "BROKER_SUBMIT_STARTED"
    )

    class BrokerTruth:
        _offline = False
        _tick_context = None

        def get_balance(self, **_kwargs):
            return {"positions": [{"symbol": "NVDA", "qty": 7}]}

        def get_today_orders(self, trade_date):
            assert trade_date == TRADE_DATE
            return [{
                "trade_date": TRADE_DATE,
                "order_no": "36025",
                "symbol": "NVDA",
                "exchange": "NASDAQ",
                "side": "SELL",
                "requested_qty": 3,
                "filled_qty": 3,
                "remaining_qty": 0,
                "limit_price": 90.0,
                "submitted_at_utc": submit["submitted_at_utc"],
                "status": "FILLED",
                "avg_price": 90.0,
            }]

        def get_fills_by_order_no(self, **_kwargs):
            return {
                "order_no": "36025",
                "symbol": "NVDA",
                "side": "SELL",
                "requested_qty": 3,
                "filled_qty": 3,
                "remaining_qty": 0,
                "status": "FILLED",
                "avg_price": 90.0,
            }

        def _get_client(self):
            return self

        def get_us_fills_today(self, **_kwargs):
            return []

    recovery = replay_order_journal(
        "2026-10-02", provider=BrokerTruth(), include_active_claims=True,
    )
    assert recovery["broker_full_fill_count"] == 1
    assert repos._MEM_ORDERS[0]["client_order_key"] == action_intent["client_order_key"]
    assert repos._MEM_ORDERS[0]["order_no"] == "36025"
    assert repos._MEM_ORDERS[0]["qty_filled"] == 3
    assert len(repos._MEM_FILLS) == 1

    restarted_repo = DurableExecutionClaimRepo(engine, us_execution_claims)
    monkeypatch.setattr(repos, "_execution_claim_repo", lambda: restarted_repo)
    monkeypatch.setattr(repos, "claim_execution_action", restarted_repo.acquire)
    broker.response_lost = False
    next_intent = {
        **action_intent,
        "trade_date": "2026-10-05",
        "client_order_key": "nvda-defense-trim-next-tick",
    }
    repeated = route_order(next_intent, kis_client=broker)

    assert repeated["status"] == "ACK"
    assert broker.calls == 2
    assert restarted_repo.health()["unresolved_execution_actions"] == 1
    action_key = submit["meta"]["execution_action_key"]
    assert restarted_repo.get(action_key).action_state == "SATISFIED"
    same_day_duplicate = route_order(
        {**next_intent, "client_order_key": "nvda-same-day-duplicate"},
        kis_client=broker,
    )
    assert same_day_duplicate["status"] == "ORDER_FENCED_UNRESOLVED_ACTION"
    assert broker.calls == 2
    engine.dispose()


def test_cancel_then_late_fill_and_repeated_snapshot_are_monotonic(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)
    assert repos.save_order_ack({
        "client_order_key": "late-fill-order",
        "symbol": "JNJ",
        "exchange": "NYSE",
        "side": "SELL",
        "qty_requested": 10,
        "qty_filled": 0,
        "order_no": "late-36025",
        "status": "ACK",
        "meta": {"strategy_owner": "US_STANDARD", "position_lifecycle_id": "late-cycle", "avg_cost": 250},
    }, trade_date=TRADE_DATE)

    def observe(cumulative, status):
        return repos.apply_broker_order_observation(
            trade_date=TRADE_DATE,
            client_order_key="late-fill-order",
            raw_order_no="late-36025",
            canonical_order_no="late-36025",
            symbol="JNJ",
            side="SELL",
            requested_qty=10,
            filled_qty=cumulative,
            remaining_qty=10 - cumulative,
            broker_status=status,
            evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
            raw_row={
                "requested_qty": 10,
                "filled_qty": cumulative,
                "remaining_qty": 0 if status == "CANCELLED" else 10 - cumulative,
                "avg_price": 262.905,
            },
        )

    assert observe(2, "PARTIALLY_FILLED")["status"] == "OK"
    assert repos._MEM_ORDERS[0]["status"] == "PARTIALLY_FILLED"
    unknown_cancel = repos.apply_broker_order_observation(
        trade_date=TRADE_DATE,
        client_order_key="late-fill-order",
        raw_order_no="late-36025",
        canonical_order_no="late-36025",
        symbol="JNJ",
        side="SELL",
        requested_qty=10,
        filled_qty=None,
        remaining_qty=8,
        broker_status="CANCELLED",
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        raw_row={
            "requested_qty": 10, "avg_price": 262.905,
            "filled_qty_present": False, "remaining_qty_present": False,
        },
    )
    assert unknown_cancel["status"] == "OK"
    assert repos._MEM_ORDERS[0]["qty_filled"] == 2
    assert repos._MEM_ORDERS[0]["meta"]["filled_quantity_status"] == "UNKNOWN"
    assert observe(2, "CANCELLED")["status"] == "OK"
    assert repos._MEM_ORDERS[0]["status"] == "CANCELLED"
    assert observe(5, "CANCELLED")["status"] == "OK"
    assert observe(5, "CANCELLED")["status"] == "OK"
    assert repos._MEM_ORDERS[0]["status"] == "CANCELLED"
    assert repos._MEM_ORDERS[0]["qty_filled"] == 5
    assert len(repos._MEM_FILLS) == 1
    assert repos._MEM_FILLS[0]["qty"] == 5
    assert repos.verify_order_fill_accounting(trade_date=TRADE_DATE, order_no="late-36025")["status"] == "OK"


def test_claim_health_query_failure_is_reported_as_unavailable(monkeypatch, tmp_path):
    _reset(monkeypatch, tmp_path)

    def unavailable():
        raise RuntimeError("claim database unavailable")

    monkeypatch.setattr(repos, "load_execution_claim_health", unavailable)
    health = repos.load_broker_recovery_health(TRADE_DATE)

    assert health["available"] is False
    assert health["execution_claim_health_available"] is False
    assert health["recovery_health_error_count"] == 1
