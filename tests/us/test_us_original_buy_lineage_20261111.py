"""AMGN 2026-11-10 original KIS BUY identity: fake DB, zero broker HTTP."""
import hashlib
from datetime import datetime, timezone

import pytest

from trader.us.entry_exit_contract import build_us_entry_exit_contract
from trader.us.original_buy_lineage import select_verified_us_original_buy_scope


ACCOUNT = "practice:fixture:01"
ACCOUNT_HASH = hashlib.sha256(ACCOUNT.encode()).hexdigest()
IDENTITY = {"env": "practice", "account_id": ACCOUNT,
            "trading_epoch_id": "epoch-current", "strategy_owner": "US_STANDARD"}
CYCLE = "position-cycle-amgn-20261110"
PORTFOLIO = "portfolio-current"


def original_row():
    contract = build_us_entry_exit_contract(
        {"symbol": "AMGN", "strategy_owner": "US_STANDARD",
         "position_action": "NEW_POSITION_BUY", "book": "SWING_BOOK",
         "horizon": "SWING_CARRY", "entry_reason": "fixture"},
    )
    meta = {
        "broker_account_hash": ACCOUNT_HASH, "position_lifecycle_id": CYCLE,
        "position_cycle_id": CYCLE, "portfolio_epoch_id": PORTFOLIO,
        "entry_exit_contract_sha256": contract["sha256"],
        "strategy_owner": "US_STANDARD", "entry_exit_contract": contract,
    }
    return {
        "symbol": "AMGN", "trading_epoch_id": "epoch-current",
        "account_id": ACCOUNT_HASH, "position_cycle_id": CYCLE,
        "portfolio_epoch_id": PORTFOLIO, "order_no": "0000039784",
        "client_order_key": "fixture-amgn-buy",
        "order_qty": 6, "intent_qty": 6, "fill_qty": 6,
        "order_filled": 6, "order_status": "FILLED",
        "filled_at": datetime(2026, 11, 10, 14, 31, tzinfo=timezone.utc),
        "order_meta": {**meta, "pre_order_holding_qty": 0},
        "intent_meta": dict(meta),
        "fill_meta": {
            **meta, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "cumulative_filled_qty": 6,
        },
    }


def test_verified_original_kis_buy_binds_account_and_cycle_without_synthesis():
    original = original_row()
    proof = select_verified_us_original_buy_scope(
        [original], symbol="AMGN", qty=6, identity=IDENTITY,
    )
    assert proof is not None
    assert proof["position_lifecycle_id"] == CYCLE
    assert proof["position_cycle_id"] == CYCLE
    assert proof["portfolio_epoch_id"] == PORTFOLIO
    assert proof["account_id"] == ACCOUNT  # raw lifecycle key, not broker hash
    assert proof["meta"]["broker_account_hash"] == ACCOUNT_HASH
    assert proof["filled_at"] == original["filled_at"]
    assert proof["meta"]["lineage_resolution"] == "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY"


@pytest.mark.parametrize("field,value", [
    ("account_id", "wrong-hash"),
    ("trading_epoch_id", "old-epoch"),
    ("fill_qty", 5),
    ("order_qty", 7),
    ("intent_qty", 7),
    ("position_cycle_id", "other-cycle"),
    ("portfolio_epoch_id", ""),
    ("filled_at", None),
])
def test_conflicting_order_or_partial_fill_never_adopted(field, value):
    row = original_row()
    row[field] = value
    assert select_verified_us_original_buy_scope([row], symbol="AMGN", qty=6,
                                                  identity=IDENTITY) is None


def test_tampered_signed_contract_old_account_or_ambiguous_original_fails():
    row = original_row()
    tampered = original_row()
    tampered["order_meta"]["entry_exit_contract"] = {
        **tampered["order_meta"]["entry_exit_contract"],
        "version": "forged",
    }
    assert select_verified_us_original_buy_scope([tampered], symbol="AMGN", qty=6,
                                                  identity=IDENTITY) is None
    assert select_verified_us_original_buy_scope([row, original_row()], symbol="AMGN",
                                                  qty=6, identity=IDENTITY) is None
    assert select_verified_us_original_buy_scope([row], symbol="AMGN", qty=6,
                        identity={**IDENTITY, "account_id": "practice:other:01"}) is None
    assert select_verified_us_original_buy_scope([row], symbol="AMGN", qty=6,
                        identity={**IDENTITY, "env": "real"}) is None



@pytest.mark.parametrize("field,invalid", [
    ("fill_qty", 6.5),
    ("fill_qty", True),
    ("fill_qty", "6.0"),
    ("intent_qty", 6.9),
    ("intent_qty", True),
    ("order_qty", 6.9),
    ("order_qty", "6.0"),
    ("order_filled", 6.1),
    ("order_filled", None),
    ("order_status", None),
    ("order_status", ""),
])
def test_original_buy_source_quantities_and_status_are_never_coerced(field, invalid):
    row = original_row()
    row[field] = invalid
    assert select_verified_us_original_buy_scope(
        [row], symbol="AMGN", qty=6, identity=IDENTITY,
    ) is None


@pytest.mark.parametrize("section,key,invalid", [
    ("fill_meta", "cumulative_filled_qty", 6.5),
    ("fill_meta", "cumulative_filled_qty", True),
    ("fill_meta", "cumulative_filled_qty", "6.0"),
    ("order_meta", "pre_order_holding_qty", 0.5),
    ("order_meta", "pre_order_holding_qty", False),
    ("order_meta", "pre_order_holding_qty", "-0"),
])
def test_original_buy_broker_metadata_numbers_are_never_coerced(section, key, invalid):
    row = original_row()
    row[section][key] = invalid
    assert select_verified_us_original_buy_scope(
        [row], symbol="AMGN", qty=6, identity=IDENTITY,
    ) is None


def test_original_buy_exact_integer_strings_still_work():
    row = original_row()
    row["fill_meta"]["cumulative_filled_qty"] = "6"
    row["order_meta"]["pre_order_holding_qty"] = "0"
    proof = select_verified_us_original_buy_scope(
        [row], symbol="AMGN", qty=6, identity=IDENTITY,
    )
    assert proof is not None and proof["qty"] == 6


def test_normalized_original_kis_fill_is_usable_by_existing_confirmed_fill_logic():
    from trader.us.position_lifecycle_state import _confirmed_buy_fill
    fill = select_verified_us_original_buy_scope([original_row()], symbol="AMGN",
                                                   qty=6, identity=IDENTITY)
    scoped = {**IDENTITY, "position_lifecycle_id": CYCLE,
              "portfolio_epoch_id": PORTFOLIO, "position_cycle_id": CYCLE}
    matched = _confirmed_buy_fill([fill], "AMGN", scoped)
    assert matched is not None
    assert matched[0]["meta"]["entry_exit_contract"]["strategy_owner"] == "US_STANDARD"


def test_authoritative_only_original_scope_identity_bridge(monkeypatch):
    import trader.us.original_buy_lineage as original
    from trader.us.position_lifecycle_state import _bind_authoritative_position_identity

    proven = select_verified_us_original_buy_scope([original_row()], symbol="AMGN",
                                                    qty=6, identity=IDENTITY)
    monkeypatch.setattr(original, "load_verified_us_original_buy_scope",
                        lambda **_kwargs: proven)
    raw = {"symbol": "AMGN", "qty": 6}
    got = _bind_authoritative_position_identity(
        raw, identity=dict(IDENTITY), trade_date="2026-11-10",
        fills=[], authoritative=True, preloaded_risk_candidates=[],
    )
    assert got["portfolio_epoch_id"] == PORTFOLIO
    assert got["position_lifecycle_id"] == CYCLE
    assert got["position_cycle_id"] == CYCLE
    assert raw["_verified_original_buy_fill"] == proven
    raw2 = {"symbol": "AMGN", "qty": 6}
    got2 = _bind_authoritative_position_identity(
        raw2, identity=dict(IDENTITY), trade_date="2026-11-10",
        fills=[], authoritative=False, preloaded_risk_candidates=[],
    )
    assert "portfolio_epoch_id" not in got2 or not got2["portfolio_epoch_id"]
    assert "_verified_original_buy_fill" not in raw2


def test_raw_kis_snapshot_does_not_null_out_verified_provenance(monkeypatch):
    from trader.us.db import repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_MEM_POSITIONS", [])
    row = {
        "symbol": "AMGN", "qty": 6, "exchange": "NASDAQ",
        "avg_cost": 461.80, "current_px": 465.0,
        "meta": {
            "position_lifecycle_id": CYCLE,
            "high_watermark": 470.0,
            "opened_trade_date": "2026-11-10",
            "strategy_owner": "US_STANDARD",
        },
    }
    assert repos.save_position_snapshot([row], trade_date="2026-11-10") == 1
    stored = repos._MEM_POSITIONS[0]["meta"]
    assert stored["position_lifecycle_id"] == CYCLE
    assert stored["opened_trade_date"] == "2026-11-10"
    assert stored["high_watermark"] == 470.0
    assert stored["strategy_owner"] == "US_STANDARD"


def test_first_authoritative_snapshot_binds_original_scope_before_insert(monkeypatch):
    from trader.us.original_buy_lineage import verified_new_buy_snapshot_identity
    import trader.us.original_buy_lineage as bridge

    class Query:
        def mappings(self):
            return self
        def all(self):
            return [{"account_id": ACCOUNT}]

    class Conn:
        def execute(self, query, params):
            assert "trading_epochs" in str(query)
            assert params["epoch"] == IDENTITY["trading_epoch_id"]
            return Query()

    called = []
    def proved(**kwargs):
        called.append(kwargs)
        return select_verified_us_original_buy_scope(
            [original_row()], symbol="AMGN", qty=6, identity=IDENTITY,
        )
    monkeypatch.setattr(bridge, "load_verified_us_original_buy_scope", proved)
    result = verified_new_buy_snapshot_identity(
        conn=Conn(), trade_date="2026-11-10",
        position={"symbol": "AMGN", "qty": 6, "meta": {}},
        trading_epoch_id=IDENTITY["trading_epoch_id"],
    )
    assert result["position_lifecycle_id"] == CYCLE
    assert result["portfolio_epoch_id"] == PORTFOLIO
    assert result["broker_account_hash"] == ACCOUNT_HASH
    assert result["account_id"] == ACCOUNT
    assert len(called) == 1
    assert called[0]["conn"].__class__.__name__ == "Conn"


def test_snapshot_proof_missing_or_conflicting_never_rebinds(monkeypatch):
    from trader.us.original_buy_lineage import verified_new_buy_snapshot_identity
    import trader.us.original_buy_lineage as bridge

    class Query:
        def mappings(self):
            return self
        def all(self):
            return [{"account_id": ACCOUNT}]

    class Conn:
        def execute(self, _query, _params):
            return Query()

    proof = select_verified_us_original_buy_scope(
        [original_row()], symbol="AMGN", qty=6, identity=IDENTITY,
    )
    monkeypatch.setattr(bridge, "load_verified_us_original_buy_scope",
                        lambda **kwargs: proof)
    kwargs = {"conn": Conn(), "trade_date": "2026-11-10",
              "trading_epoch_id": IDENTITY["trading_epoch_id"]}
    assert verified_new_buy_snapshot_identity(
        position={"symbol": "AMGN", "qty": 6,
                  "meta": {"position_cycle_id": "foreign-cycle"}}, **kwargs
    ) is None
    assert verified_new_buy_snapshot_identity(
        position={"symbol": "TQQQ", "qty": 6, "meta": {}}, **kwargs
    ) is None
    assert verified_new_buy_snapshot_identity(
        position={"symbol": "AMGN", "qty": 6, "strategy_owner": "OTHER"}, **kwargs
    ) is None
    monkeypatch.setattr(bridge, "load_verified_us_original_buy_scope",
                        lambda **kwargs: None)
    assert verified_new_buy_snapshot_identity(
        position={"symbol": "AMGN", "qty": 6}, **kwargs
    ) is None


def test_partial_original_buy_binds_exact_3_of_6_not_any_arbitrary_qty():
    row = original_row()
    row["fill_qty"] = 3
    row["order_filled"] = 3
    row["order_status"] = "PARTIALLY_FILLED"
    row["fill_meta"]["cumulative_filled_qty"] = 3
    partial = select_verified_us_original_buy_scope(
        [row], symbol="AMGN", qty=3, identity=IDENTITY,
    )
    assert partial and partial["qty"] == 3
    assert partial["position_cycle_id"] == CYCLE
    assert select_verified_us_original_buy_scope(
        [row], symbol="AMGN", qty=6, identity=IDENTITY,
    ) is None
    row["order_status"] = "FILLED"  # Broker ACK/status inconsistent with KIS
    assert select_verified_us_original_buy_scope(
        [row], symbol="AMGN", qty=3, identity=IDENTITY,
    ) is None


def test_next_day_kis_balance_carries_only_verified_live_cycle():
    from trader.us.original_buy_lineage import verified_carried_us_snapshot_identity

    contract = original_row()["order_meta"]["entry_exit_contract"]
    prior = {
        "as_of": "2026-11-10", "qty": 6,
        "account_id": ACCOUNT_HASH, "portfolio_epoch_id": PORTFOLIO,
        "position_cycle_id": CYCLE,
        "meta": {
            "strategy_owner": "US_STANDARD",
            "broker_account_hash": ACCOUNT_HASH,
            "position_lifecycle_id": CYCLE, "position_cycle_id": CYCLE,
            "portfolio_epoch_id": PORTFOLIO,
            "entry_exit_contract": contract,
            "entry_exit_contract_sha256": contract["sha256"],
            "lineage_resolution": "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
        },
    }
    lifecycle = {
        "is_open": True, "lifecycle_id": CYCLE, "position_cycle_id": CYCLE,
        "portfolio_epoch_id": PORTFOLIO,
        "account_id": ACCOUNT, "strategy_owner": "US_STANDARD",
        "trading_epoch_id": "epoch-current",
        "opened_trade_date": "2026-11-10",
        "opened_at": "2026-11-10T16:26:14+00:00",
        "opened_at_source": "confirmed_buy_fill",
        "high_watermark": 470.0,
        "high_watermark_at": "2026-11-10T20:00:00+00:00",
        "high_watermark_source": "us_position_risk_state",
        "holding_trade_days": 1,
        "entry_policy": {"entry_exit_contract_sha256": contract["sha256"]},
    }

    class Result:
        def __init__(self, rows=None, value=None):
            self.rows = list(rows or [])
            self.value = value
        def mappings(self):
            return self
        def first(self):
            return self.rows[0] if self.rows else None
        def all(self):
            return self.rows
        def scalar(self):
            return self.value

    class Conn:
        def __init__(self):
            self.prior = dict(prior)
            self.lifecycle = dict(lifecycle)
            self.raw_account = ACCOUNT
            self.intervening = 0
        def execute(self, sql, params):
            sql = str(sql)
            if "FROM us_positions" in sql:
                assert params["epoch"] == "epoch-current"
                return Result([self.prior])
            if "FROM trading_epochs" in sql:
                return Result([{"account_id": self.raw_account}])
            if "FROM us_position_risk_state" in sql:
                return Result([{"lifecycle": self.lifecycle}])
            if "FROM us_orders" in sql:
                return Result(value=self.intervening)
            raise AssertionError(sql)

    def resolve(conn, qty=6):
        return verified_carried_us_snapshot_identity(
            conn=conn, trade_date="2026-11-11",
            position={"symbol": "AMGN", "qty": qty},
            trading_epoch_id="epoch-current",
        )

    connection = Conn()
    matched = resolve(connection)
    assert matched
    assert matched["position_lifecycle_id"] == CYCLE
    assert matched["portfolio_epoch_id"] == PORTFOLIO
    assert matched["account_id"] == ACCOUNT
    assert matched["lineage_resolution"] == "VERIFIED_CURRENT_EPOCH_CONTINUITY"
    assert matched["high_watermark"] == 470.0

    connection.intervening = 1
    assert resolve(connection) is None  # any new/pending BUY or SELL
    connection.intervening = 0
    assert resolve(connection, qty=5) is None  # late fill / split / partial SELL

    connection.lifecycle = {**lifecycle, "is_open": False}
    assert resolve(connection) is None  # closed previous cycle
    connection.lifecycle = dict(lifecycle)
    connection.raw_account = "practice:another-account:01"
    assert resolve(connection) is None  # no cross-account scope adoption
    connection.raw_account = ACCOUNT
    connection.prior = {
        **prior, "meta": {**prior["meta"], "lineage_resolution": "UNVERIFIED"},
    }
    assert resolve(connection) is None  # no provenance-less carry


@pytest.mark.parametrize("bad_qty", [True, False, 6.0, 6.5, "6.0", "-6", "", None, 0])
def test_original_buy_proof_rejects_non_integer_authoritative_qty(bad_qty):
    # Never coerce a partially parsed or bogus broker quantity into shares.
    assert select_verified_us_original_buy_scope(
        [original_row()], symbol="AMGN", qty=bad_qty, identity=IDENTITY,
    ) is None


def test_original_buy_proof_accepts_exact_broker_integer_string():
    assert select_verified_us_original_buy_scope(
        [original_row()], symbol="AMGN", qty="6", identity=IDENTITY,
    ) is not None


def test_truncated_original_buy_source_window_is_never_identity_proof(monkeypatch):
    from trader.us import original_buy_lineage as bridge
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_active_us_epoch", lambda conn: IDENTITY["trading_epoch_id"])

    class DBRows:
        def __init__(self, rows):
            self.rows = rows
        def mappings(self):
            return self
        def all(self):
            return self.rows

    class DBConn:
        def __init__(self, rows):
            self.rows = rows
        def execute(self, sql, params):
            assert "LIMIT 51" in str(sql)
            assert params["symbol"] == "AMGN"
            return DBRows(self.rows)

    assert bridge.load_verified_us_original_buy_scope(
        symbol="AMGN", trade_date="2026-11-10", qty=6,
        identity=IDENTITY, conn=DBConn([original_row()] * 51),
    ) is None
    assert bridge.load_verified_us_original_buy_scope(
        symbol="AMGN", trade_date="2026-11-10", qty=6,
        identity=IDENTITY, conn=DBConn([original_row()]),
    ) is not None
