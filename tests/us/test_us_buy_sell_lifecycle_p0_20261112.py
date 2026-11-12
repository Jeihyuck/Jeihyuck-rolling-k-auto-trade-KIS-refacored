"""Cross-session original BUY binding and TQQQ actual-fill activation regressions.

These synthetic fixtures are offline proofs, not live KIS or operating-DB
certification.  Real PostgreSQL / practice fills remain separate release gates.
"""
from datetime import date
import hashlib

from trader.us.entry_exit_contract import build_us_entry_exit_contract
from trader.us.original_buy_lineage import verified_carried_us_snapshot_identity
from trader.us.infinite.models import InfiniteState, Status
from trader.us.infinite.repository import InfiniteRepository


ACCOUNT = "practice:test-fixture-only"
HASH = hashlib.sha256(ACCOUNT.encode()).hexdigest()
EPOCH = "fixture-current-epoch"
CYCLE = "fixture-original-cycle"
PORTFOLIO = "fixture-portfolio-epoch"


def _source():
    signed = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY", "book": "SWING_BOOK",
        "horizon": "SWING_CARRY", "entry_reason": "fixture",
    })
    return {
        "as_of": date(2026, 11, 10), "qty": 6,
        # Reproduce the 11/10->11/11 defect: identity in signed source and
        # risk state, but account_id added by 0062 is NULL.
        "account_id": None, "portfolio_epoch_id": PORTFOLIO,
        "position_cycle_id": CYCLE,
        "meta": {
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": CYCLE, "position_cycle_id": CYCLE,
            "portfolio_epoch_id": PORTFOLIO, "broker_account_hash": HASH,
            "entry_exit_contract": signed,
            "entry_exit_contract_sha256": signed["sha256"],
            "lineage_resolution": "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
        },
    }


class _Query:
    def __init__(self, *, row=None, rows=None, scalar=None):
        self.row, self.rows, self.value = row, rows, scalar

    def mappings(self):
        return self

    def first(self):
        return self.row

    def all(self):
        return self.rows

    def scalar(self):
        return self.value


class _Conn:
    def __init__(self, prior, *, account=ACCOUNT, risky=True, pending=0):
        self.prior, self.account, self.risky, self.pending = prior, account, risky, pending

    def execute(self, statement, params):
        sql = str(statement)
        if "SELECT as_of,qty,account_id,portfolio_epoch_id" in sql:
            return _Query(row=self.prior)
        if "SELECT account_id FROM trading_epochs" in sql:
            return _Query(rows=[{"account_id": self.account}])
        if "SELECT state->'lifecycle' AS lifecycle" in sql:
            contract = self.prior["meta"]["entry_exit_contract"]
            return _Query(row={"lifecycle": {
                "is_open": self.risky,
                "lifecycle_id": CYCLE, "position_cycle_id": CYCLE,
                "portfolio_epoch_id": PORTFOLIO, "account_id": self.account,
                "trading_epoch_id": EPOCH, "strategy_owner": "US_STANDARD",
                "opened_trade_date": "2026-11-10",
                "entry_policy": {"entry_exit_contract_sha256": contract["sha256"]},
            }})
        if "SELECT COUNT(*) FROM us_orders" in sql:
            return _Query(scalar=self.pending)
        raise AssertionError("Unexpected DB read: " + sql[:150])


def _carry(conn, position=None):
    return verified_carried_us_snapshot_identity(
        conn=conn, trade_date="2026-11-11",
        position=position or {"symbol": "AMGN", "qty": 6},
        trading_epoch_id=EPOCH,
    )


def test_amgn_signed_original_but_null_0062_account_column_carries():
    proof = _carry(_Conn(_source()))
    assert proof is not None
    assert proof["position_cycle_id"] == CYCLE
    assert proof["broker_account_hash"] == HASH
    assert proof["lineage_resolution"] == "VERIFIED_CURRENT_EPOCH_CONTINUITY"
    assert proof["entry_exit_contract"]["sha256"] == _source()["meta"]["entry_exit_contract_sha256"]


def test_carry_rejects_scope_conflict_unproved_lifecycle_and_pending_orders():
    source = _source()
    assert _carry(_Conn(source, account="practice:wrong")) is None
    assert _carry(_Conn(source, risky=False)) is None
    assert _carry(_Conn(source, pending=1)) is None
    assert _carry(_Conn(source), {"symbol": "AMGN", "qty": 7}) is None
    assert _carry(_Conn(source), {
        "symbol": "AMGN", "qty": 6, "meta": {"position_cycle_id": "foreign"}
    }) is None
    conflicting = _source()
    conflicting["account_id"] = "conflicting-column-hash"
    assert _carry(_Conn(conflicting)) is None
    damaged = _source()
    damaged["meta"]["entry_exit_contract_sha256"] = "tampered"
    assert _carry(_Conn(damaged)) is None


class _GapConn(_Conn):
    """Latest snapshot has no identity; one older proven source remains."""
    def __init__(self, latest, source, **kwargs):
        super().__init__(source, **kwargs)
        self.latest = latest
        self.source = source

    def execute(self, statement, params):
        sql = str(statement)
        if "SELECT as_of,qty,account_id" in sql:
            if "as_of<:prior_date" in sql:
                return _Query(rows=[self.source])
            return _Query(row=self.latest)
        return super().execute(statement, params)


def test_amgn_carries_across_one_unverified_snapshot_without_history_rewrite():
    source = _source()
    intermediate = {
        "as_of": date(2026, 11, 11), "qty": 6,
        "account_id": None, "portfolio_epoch_id": None,
        "position_cycle_id": None,
        "meta": {
            "entry_exit_contract": source["meta"]["entry_exit_contract"],
            "entry_exit_contract_sha256": source["meta"]["entry_exit_contract_sha256"],
            "lineage_identity_status": "ORIGINAL_BUY_SCOPE_UNVERIFIED",
        },
    }
    result = verified_carried_us_snapshot_identity(
        conn=_GapConn(intermediate, source),
        trade_date="2026-11-12",
        position={"symbol": "AMGN", "qty": 6},
        trading_epoch_id=EPOCH,
    )
    assert result is not None
    assert result["position_cycle_id"] == CYCLE
    assert result["lineage_resolution"] == "VERIFIED_CURRENT_EPOCH_CONTINUITY"


def test_amgn_missing_identity_gap_rejects_conflict_and_intervening_order():
    source = _source()
    intermediate = {
        "as_of": date(2026, 11, 11), "qty": 6,
        "account_id": None, "portfolio_epoch_id": None,
        "position_cycle_id": None, "meta": {},
    }

    def carry(middle, *, pending=0):
        return verified_carried_us_snapshot_identity(
            conn=_GapConn(middle, source, pending=pending),
            trade_date="2026-11-12",
            position={"symbol": "AMGN", "qty": 6},
            trading_epoch_id=EPOCH,
        )

    assert carry(intermediate) is not None
    assert carry(intermediate, pending=1) is None
    assert carry({**intermediate, "qty": 0}) is None
    assert carry({**intermediate, "qty": 7}) is None
    assert carry({**intermediate, "position_cycle_id": "foreign-cycle"}) is None
    assert carry({**intermediate, "account_id": "foreign-hash"}) is None
    assert carry({**intermediate, "meta": {"strategy_owner": "TQQQ_INFINITE"}}) is None
    assert carry({**intermediate, "meta": {"entry_exit_contract_sha256": "wrong"}}) is None


class _FillProof:
    def __init__(self, buy_shares, net_shares):
        self.buy_shares = buy_shares
        self.net_shares = net_shares

    def cycle_fill_stats(self, _state, _trading_date):
        return {
            "total_buy_notional": 118.71, "last_buy_date": date(2026, 11, 11),
            "first_fill_price": 59.355, "last_buy_fill_price": 59.355,
            "last_profit_stage": None, "last_rebound_probe_fill_date": None,
            "confirmed_buy_shares": self.buy_shares,
            "confirmed_net_shares": self.net_shares,
        }


def _tqqq_reconcile(buys, net, broker_qty):
    state = InfiniteState(
        cycle_id="original-tqqq-cycle", cycle_start_date=date(2026, 11, 11),
        status=Status.READY, version=709,
    )
    return InfiniteRepository.reconcile_metadata(
        _FillProof(buys, net), state,
        trading_date=date(2026, 11, 11), broker_qty=broker_qty,
        broker_average_price=59.355, core_cap=5000,
    )


def test_tqqq_ready_reservation_becomes_active_after_same_cycle_actual_fill():
    result = _tqqq_reconcile(2, 2, 2)
    assert result.status == Status.ACTIVE
    assert result.metadata["ready_activation_proof"] == "CURRENT_CYCLE_KIS_ACTUAL_FILL_AND_BROKER_BALANCE"
    assert result.cycle_id == "original-tqqq-cycle"


def test_tqqq_ack_without_fill_or_mismatched_balance_cannot_activate():
    assert _tqqq_reconcile(0, 0, 2).status == Status.READY
    assert _tqqq_reconcile(2, 2, 0).status == Status.READY
    assert _tqqq_reconcile(2, 1, 2).status == Status.READY
    assert _tqqq_reconcile(2, 2, 3).status == Status.READY


def test_real_postgres_tqqq_cas_rejects_stale_ready_and_preserves_active(monkeypatch):
    """Runs against CI PostgreSQL; no KIS or operating database access."""
    import os
    from pathlib import Path
    from dataclasses import replace

    import pytest

    uri = os.environ.get("PBCORE_TEST_POSTGRES_URL")
    if not uri:
        pytest.skip("no isolated PostgreSQL service")
    from sqlalchemy import create_engine, text

    schema = "us_lifecycle_p0_ci"
    root = create_engine(uri, future=True)
    with root.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(
        uri, future=True,
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            # Exact production 0048 DDL with the 0052 scoped-PK shape
            # applied in this isolated schema (not a fake SQLite table).
            conn.exec_driver_sql(
                Path("migrations/0048_add_us_tqqq_infinite_state.sql").read_text()
            )
            conn.exec_driver_sql(
                "ALTER TABLE us_tqqq_infinite_state "
                "ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT"
            )
            conn.exec_driver_sql(
                "ALTER TABLE us_tqqq_infinite_state DROP CONSTRAINT "
                "us_tqqq_infinite_state_pkey"
            )
            conn.exec_driver_sql(
                "ALTER TABLE us_tqqq_infinite_state "
                "ADD CONSTRAINT us_tqqq_infinite_state_pkey "
                "PRIMARY KEY (trading_epoch_id, strategy_id, symbol)"
            )
        monkeypatch.setattr(
            InfiniteRepository, "_epoch_id",
            lambda self, bind=None, required=None: "fixture-practice-epoch",
        )
        repo = InfiniteRepository(engine)
        ready = InfiniteState(
            strategy_id="TQQQ_INFINITE_V3", symbol="TQQQ",
            cycle_id="fixture-cycle-1", cycle_start_date=date(2026, 11, 11),
            status=Status.READY, version=1,
        )
        repo.save_state(ready)
        assert ready.version == 1
        stale = replace(ready)
        active = replace(ready, status=Status.ACTIVE, core_filled_notional=118.71)
        repo.save_state(active)
        assert active.version == 2
        from trader.us.infinite.repository import StateCorruptionError

        with pytest.raises(StateCorruptionError, match="STALE_VERSION_OR_CYCLE"):
            repo.save_state(stale)
        stored = repo.load_state()
        assert stored.status == Status.ACTIVE
        assert stored.version == 2
        assert stored.cycle_id == "fixture-cycle-1"
        assert float(stored.core_filled_notional) == pytest.approx(118.71)
        next_update = replace(stored, metadata={"broker_qty": 2})
        repo.save_state(next_update)
        assert next_update.version == 3
        assert repo.load_state().metadata["broker_qty"] == 2
    finally:
        engine.dispose()
        with root.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        root.dispose()


import pytest


@pytest.mark.parametrize("style", [
    "ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_BREAKOUT", "ENTRY_MINERVINI",
])
def test_original_selected_strategy_and_version_survive_sell_contract(style, monkeypatch):
    from trader.us.entry_exit_contract import (
        build_us_entry_exit_contract, verify_us_entry_exit_contract,
    )
    monkeypatch.setenv("US_SWING_EXIT_MODE", "LEGACY_V2")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "0")
    contract = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "sleeve_id": "US_STANDARD", "strategy": "us_pb1",
        "strategy_version": "fixture-selection-v42",
        "position_action": "NEW_POSITION_BUY",
        "entry_style_selected": style,
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        "entry_reason": "proven-original-selection",
    })
    assert verify_us_entry_exit_contract(contract)
    assert contract["entry_strategy"] == "us_pb1"
    assert contract["entry_strategy_detail"] == {
        "selected_style": style,
        "entry_component": "",
        "execution_strategy": "us_pb1",
        "strategy_version": "fixture-selection-v42",
    }
    # A later change to the runtime configuration may not mutate a holding's
    # original V2/V3 policy or selected entry family.
    monkeypatch.setenv("US_SWING_EXIT_MODE", "V3_ACTIVE_PRACTICE")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "1")
    assert build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_action": "ADD_TO_EXISTING_BUY",
        "entry_exit_contract": contract,
    }) == contract
    assert build_us_entry_exit_contract({
        "symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE",
        "entry_exit_contract": contract,
    }) == {}


def test_unregistered_owner_rejected_before_any_database_or_kis_io():
    from trader.us.execution.order_router import route_order

    result = route_order({
        "symbol": "AMGN", "side": "BUY",
        "strategy_owner": "FUTURE_OWNER_WITHOUT_SELL_ADAPTER",
        "sleeve_id": "FUTURE_OWNER_WITHOUT_SELL_ADAPTER",
    })
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "US_OWNER_ADAPTER_NOT_REGISTERED"
    assert result["broker_submit"] is False


def test_immutable_signed_buy_contract_is_durable_in_original_intent(monkeypatch):
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_MEM_INTENTS", [])
    monkeypatch.setenv("KIS_ENV", "practice")
    base = {
        "symbol": "AMGN", "side": "BUY", "strategy_owner": "US_STANDARD",
        "sleeve_id": "US_STANDARD", "strategy": "us_pb1",
        "strategy_name": "us_pb1",
        "strategy_version": "fixture-v42",
        "position_action": "NEW_POSITION_BUY",
        "entry_style_selected": "ENTRY_MOMENTUM",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        "client_order_key": "a" * 24, "qty": 2, "exchange": "NASDAQ",
        "limit_price": 60, "notional_usd": 120,
    }
    contract = build_us_entry_exit_contract(base)
    assert repos.save_order_intent({**base, "meta": {
        "entry_exit_contract": contract,
        "entry_exit_contract_sha256": contract["sha256"],
        "entry_exit_contract_version": contract["version"],
        "entry_style_selected": "ENTRY_MOMENTUM",
    }}, trade_date="2026-11-12") is True
    assert len(repos._MEM_INTENTS) == 1
    recorded = repos._MEM_INTENTS[0]["meta"]
    assert recorded["entry_exit_contract"] == contract
    assert recorded["entry_exit_contract_sha256"] == contract["sha256"]

    # Reusing the order key to change the entry strategy is not an ADD and
    # must not silently overwrite the BUY's original management agreement.
    different = build_us_entry_exit_contract({
        **base, "entry_style_selected": "ENTRY_BREAKOUT",
        "entry_exit_contract": None, "meta": {},
    })
    assert different["sha256"] != contract["sha256"]
    assert repos.save_order_intent({**base, "meta": {
        "entry_exit_contract": different,
        "entry_exit_contract_sha256": different["sha256"],
        "entry_exit_contract_version": different["version"],
        "entry_style_selected": "ENTRY_BREAKOUT",
    }}, trade_date="2026-11-12") is False


def test_tqqq_state_version_cas_refuses_stale_writer_without_rollback(monkeypatch):
    from trader.us.infinite.repository import InfiniteRepository, StateCorruptionError

    class Tx:
        def __init__(self, future_version):
            self.future_version = future_version
            self.statements = []
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def execute(self, query, params):
            sql = str(query)
            self.statements.append((sql, dict(params)))
            assert "WHERE us_tqqq_infinite_state.version = EXCLUDED.version" in sql
            assert "RETURNING version" in sql
            assert "status='ACTIVE' AND EXCLUDED.status='READY'" in sql
            class Result:
                def __init__(self, version):
                    self.version = version
                def scalar_one_or_none(self):
                    return self.version
            return Result(self.future_version)
    class Engine:
        def __init__(self, tx):
            self.tx = tx
        def begin(self):
            return self.tx

    tx = Tx(710)
    repository = InfiniteRepository(Engine(tx))
    monkeypatch.setattr(repository, "_epoch_id", lambda _bind: EPOCH)
    state = InfiniteState(
        cycle_id="specific-tqqq-cycle", status=Status.ACTIVE,
        cycle_start_date=date(2026, 11, 11), version=709,
    )
    repository.save_state(state)
    assert state.version == 710
    assert tx.statements[0][1]["version"] == 709
    assert tx.statements[0][1]["cycle_id"] == "specific-tqqq-cycle"
    tx.future_version = None
    with pytest.raises(StateCorruptionError, match="STALE_VERSION_OR_CYCLE"):
        repository.save_state(InfiniteState(
            cycle_id="specific-tqqq-cycle", status=Status.READY,
            cycle_start_date=date(2026, 11, 11), version=709,
        ))


def test_resized_buy_finalization_preserves_original_signed_sell_contract(monkeypatch):
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_MEM_INTENTS", [])
    monkeypatch.setenv("KIS_ENV", "practice")
    initial = {
        "client_order_key": "f" * 24, "symbol": "AMGN",
        "side": "BUY", "strategy_owner": "US_STANDARD",
        "strategy_name": "us_pb1", "strategy": "us_pb1",
        "strategy_version": "fixture-original-version",
        "sleeve_id": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "entry_style_selected": "ENTRY_BREAKOUT",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        "qty": 7, "limit_price": 50, "notional_usd": 350,
    }
    signed = build_us_entry_exit_contract(initial)
    initial["meta"] = {
        "strategy_owner": "US_STANDARD",
        "entry_exit_contract": signed,
        "entry_exit_contract_sha256": signed["sha256"],
        "entry_exit_contract_version": signed["version"],
    }
    assert repos.save_order_intent(initial, trade_date="2026-11-12")
    final = {**initial, "qty": 4, "notional_usd": 200}
    assert repos.finalize_us_buy_intent_pre_submit(final, trade_date="2026-11-12")
    stored = repos._MEM_INTENTS[0]
    assert stored["qty"] == 4
    assert stored["notional_usd"] == 200
    assert stored["meta"]["entry_exit_contract"] == signed
    assert stored["meta"]["execution_qty_finalization"] == {
        "original_qty": 7, "final_qty": 4,
        "reason": "PRE_HTTP_RISK_OR_CASH_RESIZE",
    }
    assert repos.finalize_us_buy_intent_pre_submit(final, trade_date="2026-11-12")
    assert not repos.finalize_us_buy_intent_pre_submit(
        {**initial, "qty": 8, "notional_usd": 400}, trade_date="2026-11-12"
    )
    assert not repos.finalize_us_buy_intent_pre_submit(
        {**initial, "qty": 3, "notional_usd": 150}, trade_date="2026-11-12"
    )
    stored["status"] = "SENT"
    assert not repos.finalize_us_buy_intent_pre_submit(final, trade_date="2026-11-12")


def test_buy_finalization_blocks_missing_original_or_foreign_strategy(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_MEM_INTENTS", [])
    base = {
        "client_order_key": "c" * 24, "symbol": "AMGN",
        "side": "BUY", "qty": 2, "notional_usd": 100,
        "strategy_owner": "US_STANDARD", "meta": {},
    }
    assert not repos.finalize_us_buy_intent_pre_submit(
        base, trade_date="2026-11-12"
    )
    assert not repos.finalize_us_buy_intent_pre_submit(
        {**base, "qty": 2.5}, trade_date="2026-11-12"
    )
    assert not repos.finalize_us_buy_intent_pre_submit(
        {**base, "strategy_owner": "OTHER"}, trade_date="2026-11-12"
    )



def test_same_contract_add_preserves_original_and_mixed_strategy_is_fenced():
    from trader.us.execution.order_router import us_add_contract_requires_lot_policy
    from trader.us.entry_exit_contract import build_us_entry_exit_contract

    original = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "entry_style_selected": "ENTRY_PULLBACK",
    })
    assert us_add_contract_requires_lot_policy({
        "symbol": "AMGN", "side": "BUY",
        "meta": {"entry_exit_contract": original,
                 "entry_style_selected": "ENTRY_PULLBACK"},
    }, original) is False
    assert us_add_contract_requires_lot_policy({
        "symbol": "AMGN", "side": "BUY",
        "meta": {"entry_style_selected": "ENTRY_MOMENTUM"},
    }, original) is True
    different = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "entry_style_selected": "ENTRY_BREAKOUT",
    })
    assert us_add_contract_requires_lot_policy({
        "symbol": "AMGN", "side": "BUY",
        "meta": {"entry_exit_contract": different},
    }, original) is True
