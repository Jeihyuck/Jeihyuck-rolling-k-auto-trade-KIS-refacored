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
@pytest.mark.parametrize("mode,approved,version", [
    ("LEGACY_V2", "0", "us_entry_exit_contract_v2"),
    ("V3_ACTIVE_PRACTICE", "1", "us_entry_exit_contract_v3"),
])
def test_original_selected_strategy_and_version_survive_sell_contract(
    style, mode, approved, version, monkeypatch,
):
    from trader.us.entry_exit_contract import (
        build_us_entry_exit_contract, verify_us_entry_exit_contract,
    )
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_SWING_EXIT_MODE", mode)
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", approved)
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
    assert contract["version"] == version
    assert contract["entry_strategy"] == "us_pb1"
    assert contract["entry_strategy_detail"] == {
        "selected_style": style,
        "entry_component": "",
        "execution_strategy": "us_pb1",
        "strategy_version": "fixture-selection-v42",
    }
    # A later change to the runtime configuration may not mutate a holding's
    # original V2/V3 policy or selected entry family.
    # Reverse today's runtime mode, proving an overnight holding is not
    # silently upgraded from V2 or downgraded from V3.
    monkeypatch.setenv(
        "US_SWING_EXIT_MODE",
        "V3_ACTIVE_PRACTICE" if version.endswith("_v2") else "LEGACY_V2",
    )
    monkeypatch.setenv(
        "US_SWING_V3_DEPLOY_APPROVED", "1" if version.endswith("_v2") else "0",
    )
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


class _CycleDeltaConn(_Conn):
    """DB read harness: intervening actual native orders, no broker stubs."""
    def __init__(self, prior, rows, **kwargs):
        super().__init__(prior, **kwargs)
        self.rows = rows

    def execute(self, statement, params):
        sql = str(statement)
        if "FROM us_orders o" in sql and "LEFT JOIN us_fills f" in sql:
            return _Query(rows=self.rows)
        if "SELECT COUNT(*) FROM us_orders" in sql and "status NOT IN" not in sql:
            return _Query(scalar=len(self.rows))
        return super().execute(statement, params)


def _cycle_delta_row(side, shares, *, key=None, status="FILLED", source=None):
    source = source or _source()
    contract = source["meta"]["entry_exit_contract"]
    key = key or f"TEST_NATIVE_{side}_{shares}"
    order_no = f"NATIVE_{key}"
    meta = {
        "strategy_owner": "US_STANDARD", "position_lifecycle_id": CYCLE,
        "broker_account_hash": HASH, "entry_exit_contract_sha256": contract["sha256"],
        "entry_exit_contract": contract,
    }
    if side == "BUY":
        meta["parent_entry_contract_sha256"] = contract["sha256"]
    return {
        "trade_date": date(2026, 11, 11), "side": side,
        "status": status, "qty_requested": shares,
        "qty_filled": shares, "client_order_key": key, "order_no": order_no,
        "trading_epoch_id": EPOCH,
        "order_account_id": HASH, "fill_account_id": HASH,
        "order_portfolio_epoch_id": PORTFOLIO, "fill_portfolio_epoch_id": PORTFOLIO,
        "order_position_cycle_id": CYCLE, "fill_position_cycle_id": CYCLE,
        "order_meta": dict(meta),
        "fill_meta": {**meta, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                      "is_synthetic": False},
        "fill_qty": shares, "fill_order_no": order_no, "fill_side": side,
        "fill_client_order_key": key,
    }


def test_same_cycle_add_and_partial_sell_preserve_original_contract():
    """6 original -> BUY 3 -> SELL 2 -> 7 shares, frozen V2/V3 unchanged."""
    source = _source()
    rows = [_cycle_delta_row("BUY", 3), _cycle_delta_row("SELL", 2)]
    proven = _carry(_CycleDeltaConn(source, rows), {"symbol": "AMGN", "qty": 7})
    assert proven is not None
    assert proven["position_cycle_id"] == CYCLE
    assert proven["entry_exit_contract"] == source["meta"]["entry_exit_contract"]
    assert proven["lineage_verified_net_add_shares"] == 1
    assert proven["lineage_verified_partial_sell_shares"] == 0


def test_same_cycle_partial_tp_then_next_day_contract_stays_frozen():
    source = _source()
    proven = _carry(_CycleDeltaConn(source, [_cycle_delta_row("SELL", 2)]),
                    {"symbol": "AMGN", "qty": 4})
    assert proven is not None
    assert proven["lineage_verified_partial_sell_shares"] == 2
    assert proven["entry_exit_contract_sha256"] == source["meta"]["entry_exit_contract_sha256"]


def test_cycle_delta_rejects_unproved_add_foreign_contract_and_broker_gaps():
    source = _source()
    added = _cycle_delta_row("BUY", 2)
    def carry(row, *, qty=8):
        return _carry(_CycleDeltaConn(source, [row]), {"symbol": "AMGN", "qty": qty})
    assert carry(added) is not None
    no_parent = {**added, "order_meta": {
        **added["order_meta"], "parent_entry_contract_sha256": None,
    }}
    assert carry(no_parent) is None
    for field, bad in (
        ("status", "ACK"), ("fill_qty", None),
        ("fill_order_no", "ANOTHER_NATIVE_ORDER"),
        ("fill_side", "SELL"), ("fill_account_id", "foreign"),
        ("fill_position_cycle_id", "another-lot"),
    ):
        assert carry({**added, field: bad}) is None, field
    fake_meta = dict(added["fill_meta"], fill_evidence_type="BALANCE_DELTA_SYNTHETIC")
    assert carry({**added, "fill_meta": fake_meta}) is None
    tampered = dict(added["order_meta"], entry_exit_contract_sha256="foreign-contract")
    assert carry({**added, "order_meta": tampered}) is None
    # A broker status stuck at PARTIALLY_FILLED cannot be adopted as terminal.
    assert carry({**added, "status": "PARTIALLY_FILLED"}) is None
    assert carry(added, qty=9) is None
    assert carry(added, qty=6) is None


def test_cycle_delta_rejects_duplicate_native_fill_or_unresolved_peer_order():
    source = _source()
    add = _cycle_delta_row("BUY", 2)
    # A 1:many native join or second open order cannot be hidden by net math.
    assert _carry(_CycleDeltaConn(source, [add, add]),
                  {"symbol": "AMGN", "qty": 8}) is None
    open_sell = _cycle_delta_row("SELL", 1, status="ACK")
    assert _carry(_CycleDeltaConn(source, [add, open_sell]),
                  {"symbol": "AMGN", "qty": 7}) is None


def test_cycle_delta_net_zero_native_round_trip_requires_every_fill():
    source = _source()
    rows = [_cycle_delta_row("BUY", 2), _cycle_delta_row("SELL", 2)]
    proven = _carry(_CycleDeltaConn(source, rows))
    assert proven is not None
    assert proven["entry_exit_contract_sha256"] == source["meta"]["entry_exit_contract_sha256"]
    assert _carry(_CycleDeltaConn(source, [rows[0], {
        **rows[1], "fill_qty": None,
    }])) is None


def test_add_with_missing_style_cannot_sneak_different_engine_version_or_parent():
    from trader.us.execution.order_router import us_add_contract_requires_lot_policy
    parent = build_us_entry_exit_contract({
        "symbol": "AMGN", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_strategy": "us_pb1",
        "strategy_version": "2026.11",
    })
    def blocked(meta):
        return us_add_contract_requires_lot_policy(
            {"symbol": "AMGN", "side": "BUY", "meta": meta}, parent,
        )
    assert blocked({"entry_strategy": "DIFFERENT_ENGINE"})
    assert blocked({"strategy_version": "2026.12"})
    assert blocked({"parent_entry_contract_sha256": "foreign-cycle-contract"})
    assert not blocked({"entry_strategy": "us_pb1", "strategy_version": "2026.11",
                        "parent_entry_contract_sha256": parent["sha256"]})


def test_ten_symbol_real_exit_evaluation_isolates_one_corrupt_and_one_pending(
    monkeypatch, caplog,
):
    """8 independent hard stops remain evaluated when 1 data row raises and
    another symbol has unresolved SELL. No strategy/exit math is mocked.
    Broker POST is deliberately outside this test (not a live KIS test).
    """
    from datetime import datetime, timezone
    from trader.us.pb1 import us_exit_engine as engine
    from trader.us.protective_stop_obligation import record_confirmed_stop

    symbols = ["ABBV", "AMGN", "DHR", "HCA", "KO",
               "KR", "MCK", "PFE", "PG", "TMO"]
    now = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(engine, "should_skip_exit_due_to_pending_sell",
                        lambda sym, _day: (True, "UNRESOLVED_KIS_SELL", {})
                        if sym == "KO" else (False, None, None))
    # Stop logging is a database side effect, not exit policy selection.
    monkeypatch.setattr(
        "trader.us.protective_stop_obligation.record_confirmed_stop",
        lambda *_args, **_kwargs: None,
    )
    snapshots = []
    source_shas = {}
    for sym in symbols:
        contract = build_us_entry_exit_contract({
            "symbol": sym, "strategy_owner": "US_STANDARD",
            "position_action": "NEW_POSITION_BUY",
            "entry_style_selected": "ENTRY_PULLBACK",
            "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        })
        source_shas[sym] = contract["sha256"]
        cycle = f"cycle-{sym}"
        snapshots.append({
            "symbol": sym, "exchange": "NASDAQ",
            "qty": 4 if sym != "ABBV" else "corrupted",
            "holding_qty": 4, "orderable_qty": 4,
            "entry_price": 100.0, "entry_price_source": "kis_avg_price_usd",
            "resolved_current_price": 80.0,
            "position_lifecycle_id": cycle,
            "position_cycle_id": cycle,
            "strategy_owner": "US_STANDARD",
            "entry_time": "2026-11-10T16:00:00+00:00",
            "entry_exit_contract": contract,
            "meta": {
                "strategy_owner": "US_STANDARD",
                "position_lifecycle_id": cycle, "position_cycle_id": cycle,
                "entry_exit_contract": contract,
                "entry_exit_contract_sha256": contract["sha256"],
            },
        })
    signals = engine.generate_exit_intents(
        [], prepared_snapshots=snapshots, now=now,
        include_trend_time=False,
    )
    actual = {row["symbol"]: row for row in signals if row.get("side") == "SELL"}
    assert len(actual) == 8
    assert "ABBV" not in actual and "KO" not in actual
    assert all(actual[sym]["qty"] == 4 for sym in actual)
    assert all(
        actual[sym]["source_entry_contract_sha256"] == source_shas[sym]
        and actual[sym]["position_lifecycle_id"] == f"cycle-{sym}"
        for sym in actual
    )
    assert any(
        "[US_EXIT][SYMBOL_ISOLATED][EVALUATION_ERROR]" in record.message
        and "ABBV" in record.message for record in caplog.records
    )


def test_ten_symbol_exit_resolver_exception_is_symbol_local(monkeypatch, caplog):
    """One broken original BUY lookup must not abort nine other exit inputs.

    This exercises the real public resolver, not a mocked batch return.
    No KIS HTTP or operational database writes are performed.
    """
    from trader.us.pb1 import us_exit_position_resolver as resolver

    symbols = ["ABBV", "AMGN", "DHR", "HCA", "KO",
               "KR", "MCK", "PFE", "PG", "TMO"]
    positions = [
        {"symbol": symbol, "qty": 4, "entry_price": 100.0,
         "position_cycle_id": f"cycle-{symbol}",
         "strategy_owner": "US_STANDARD"}
        for symbol in symbols
    ]
    def original_resolver(position, **_kwargs):
        if position["symbol"] == "AMGN":
            raise ValueError("original_buy_scope_conflict")
        return {**position, "pnl_input_ok": True,
                "entry_price_source": "verified_original_buy"}

    monkeypatch.setattr(resolver, "_enrich_single_position", original_resolver)
    monkeypatch.setattr(
        "trader.us.db.repos.load_us_position_risk_state_candidates_batch",
        lambda *_args, **_kwargs: {},
    )
    enriched, meta = resolver.enrich_us_positions_for_exit(
        positions, trade_date="2026-11-12", env="practice",
    )
    assert len(enriched) == 10
    assert meta["ok"] == 9 and meta["missing"] == 1
    assert meta["missing_symbols"] == ["AMGN"]
    bad = next(row for row in enriched if row["symbol"] == "AMGN")
    assert bad["qty"] == 4
    assert bad["position_cycle_id"] == "cycle-AMGN"
    assert bad["exit_resolution_failed"] is True
    assert bad["exit_resolution_failure_reason"] == "ValueError"
    assert bad["pnl_input_ok"] is False
    assert all(
        not row.get("exit_resolution_failed") and row["pnl_input_ok"]
        for row in enriched if row["symbol"] != "AMGN"
    )
    assert any(
        "[US_EXIT_RESOLVER][SYMBOL_ISOLATED][ERROR]" in rec.message
        and "AMGN" in rec.message for rec in caplog.records
    )


def test_failed_exit_resolver_is_fenced_before_price_and_exit_evaluation(monkeypatch):
    """A failed resolver must never turn a stale entry price into a SELL."""
    from datetime import datetime, timezone
    from trader.us.pb1 import us_exit_engine as engine

    class QuoteProvider:
        def get_current_price(self, symbol, exchange):
            if symbol == "AMGN":
                raise AssertionError("fenced symbol reached price provider")
            return 80.0

    broken = {
        "symbol": "AMGN", "exchange": "NASDAQ", "qty": 4,
        "entry_price": 100.0, "exit_resolution_failed": True,
        "exit_resolution_failure_reason": "ValueError",
        "strategy_owner": "US_STANDARD",
    }
    snapshots = engine.prepare_exit_position_snapshots(
        [broken], provider=QuoteProvider(),
        now=datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc),
    )
    assert snapshots == []


def test_corrupt_qty_in_one_of_ten_is_diagnostic_only_and_peer_exits_continue(monkeypatch):
    """One invalid broker qty is retained as an incident, never normalized to SELL."""
    from trader.us.pb1 import us_exit_position_resolver as resolver
    symbols = ["ABBV", "AMGN", "DHR", "HCA", "KO",
               "KR", "MCK", "PFE", "PG", "TMO"]
    positions = [
        {"symbol": s, "qty": ("corrupt" if s == "AMGN" else 4),
         "holding_qty": 4, "position_cycle_id": f"cycle-{s}"}
        for s in symbols
    ]
    original_single = resolver._enrich_single_position
    def resolved_with_one_actual_invalid_branch(position, **kwargs):
        if position["symbol"] == "AMGN":
            # Do not mock the branch under test: the actual resolver must
            # retain corrupt qty as a broker-data incident rather than zero.
            return original_single(position, **kwargs)
        return {**position, "pnl_input_ok": True,
                "entry_price_source": "verified"}

    monkeypatch.setattr(
        resolver, "_enrich_single_position", resolved_with_one_actual_invalid_branch,
    )
    monkeypatch.setattr(
        "trader.us.db.repos.load_us_position_risk_state_candidates_batch",
        lambda *_a, **_k: {},
    )
    enriched, meta = resolver.enrich_us_positions_for_exit(
        positions, trade_date="2026-11-12",
    )
    assert len(enriched) == 10
    assert meta["ok"] == 9 and meta["missing_symbols"] == ["AMGN"]
    broken = next(p for p in enriched if p["symbol"] == "AMGN")
    assert broken["qty"] == "corrupt"  # no fabricated executable quantity
    assert broken["exit_resolution_failed"] is True
    assert broken["exit_resolution_failure_reason"] == "INVALID_POSITION_QTY"
    assert all(
        p["pnl_input_ok"] for p in enriched if p["symbol"] != "AMGN"
    )


def test_prepared_exit_engine_never_uses_bad_resolver_contract(monkeypatch):
    """Direct prepared-snapshot callers cannot bypass a failed resolver fence."""
    from datetime import datetime, timezone
    from trader.us.pb1.us_exit_engine import generate_exit_intents
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    bad = {
        "symbol": "AMGN", "qty": 4, "exchange": "NASDAQ",
        "entry_price": 100, "orderable_qty": 4,
        "resolved_current_price": 80,
        "exit_resolution_failed": True,
        "exit_resolution_failure_reason": "ORIGINAL_BUY_SCOPE_CONFLICT",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "bad-cycle",
        "entry_exit_contract": build_us_entry_exit_contract({
            "symbol": "AMGN", "strategy_owner": "US_STANDARD",
            "position_action": "NEW_POSITION_BUY",
            "entry_style_selected": "ENTRY_PULLBACK",
        }),
    }
    # Even an otherwise triggerable hard stop must remain unsubmitted when
    # its original scope has been explicitly quarantined by the resolver.
    actual = generate_exit_intents(
        [], prepared_snapshots=[bad],
        now=datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc),
        include_trend_time=False,
    )
    assert not [row for row in actual if row.get("side") == "SELL"]
