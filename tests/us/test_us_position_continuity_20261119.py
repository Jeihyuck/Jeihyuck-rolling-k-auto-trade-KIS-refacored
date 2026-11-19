"""Symbol-independent regression for the Nov18 original/forward disconnect."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import json
import os

import pytest

from tests.us.test_us_legacy_nine_active_sell_20261111 import (
    scenario, _authorize, RAW_ACCOUNT, EPOCH, OLD_CYCLE, PORTFOLIO,
)
from trader.us.entry_exit_contract import build_us_entry_exit_contract, verify_us_entry_exit_contract
from trader.us.execution import us_legacy_nine_active_sell as bridge
from trader.us.position_contract_provenance import carry_entry_policy, restore_null_elided_policy


SYMBOLS = ("AAPL", "MSFT", "NVDA", "AMD", "JNJ", "XOM", "BRK.B", "UNLISTED_TEST")


def _generalize(scenario, symbol):
    db, client, intent = scenario
    db.position["symbol"] = symbol
    for meta in (db.buy["meta"], db.fills[0]["meta"]):
        meta["position_lifecycle_id"] = f"ENTRY:US_STANDARD:{symbol}"
    intent["symbol"] = symbol
    client.get_us_balance = lambda **kw: {
        "rt_cd": "0", "balance_complete": True, "balance_authoritative": True,
        "balance_conflicts": [], "failed_exchanges": {},
        "output1": [{"ovrs_pdno": symbol, "ovrs_cblc_qty": str(client.holding),
                     "ord_psbl_qty": str(client.orderable)}],
    }
    return db, client, intent


def _apply_forward_fill(scenario, *, qty=1):
    db, client, intent = scenario
    ok, why, proof = _authorize(scenario)
    assert ok, why
    meta = deepcopy(intent["meta"])
    meta.update({"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "is_synthetic": False})
    identity = {"meta": meta, "trading_epoch_id": EPOCH,
                "portfolio_epoch_id": proof["portfolio"], "position_cycle_id": proof["prospective_cycle"],
                "client_order_key": "forward-order", "account_id": proof["account_hash"]}
    db.fills.append({**deepcopy(identity), "side": "SELL", "qty": qty,
                     "trade_date": "2026-11-11", "order_no": "5001"})
    db.sell_orders.append({**deepcopy(identity), "qty_requested": qty, "qty_filled": qty,
                           "status": "FILLED", "trade_date": "2026-11-11", "order_no": "5001"})
    db.position["qty"] -= qty
    client.holding -= qty
    client.orderable -= qty
    return proof


def _engine_for(monkeypatch, db):
    from trader.us.db import repos

    class Engine:
        @contextmanager
        def connect(self):
            yield db

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: EPOCH)


@pytest.mark.parametrize("symbol", SYMBOLS)
@pytest.mark.parametrize("buy_qty,prior_sell,sell_qty", [(5, 1, 1), (116, 29, 43)])
def test_partial_sell_then_restart_keeps_original_and_next_stop(scenario, monkeypatch, symbol, buy_qty, prior_sell, sell_qty):
    from trader.us.pb1 import us_exit_engine
    db, client, intent = _generalize(scenario, symbol)
    db.buy.update(qty_requested=buy_qty, qty_filled=buy_qty)
    db.fills[0]["qty"] = buy_qty
    db.fills[1]["qty"] = prior_sell
    db.sell_orders[0].update(qty_requested=prior_sell, qty_filled=prior_sell)
    db.position["qty"] = client.holding = client.orderable = buy_qty - prior_sell
    intent["qty"] = sell_qty
    immutable_buy = deepcopy(db.buy)
    proof = _apply_forward_fill(scenario, qty=sell_qty)
    remaining = buy_qty - prior_sell - sell_qty
    # Reconstruct every DB input as after a process restart, not a cached proof.
    for name in ("position", "history", "buy", "fills", "sell_orders", "stages"):
        setattr(db, name, json.loads(json.dumps(getattr(db, name))))
    _engine_for(monkeypatch, db)
    position = {"symbol": symbol, "qty": remaining, "entry_price": 100.0,
                "exchange": "NYSE", "strategy_owner": "US_STANDARD", "meta": {}}
    enriched = bridge.enrich_legacy_nine_v2_decision_position(position, trade_date="2026-11-11")
    assert enriched["position_lifecycle_id"] == OLD_CYCLE
    scoped = bridge.build_verified_legacy_stop_obligation_position(enriched, trade_date="2026-11-11")
    assert scoped["position_cycle_id"] == proof["prospective_cycle"]
    assert scoped["portfolio_epoch_id"] == proof["portfolio"]
    monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE", "ENFORCE")
    monkeypatch.setenv("KIS_HTTP_BLOCK", "0")
    monkeypatch.setenv("OFFLINE_MODE", "0")
    debts = []
    monkeypatch.setattr("trader.us.protective_stop_obligation.record_confirmed_stop",
                        lambda pos, **kw: debts.append((pos, kw)) or True)
    monkeypatch.setattr(us_exit_engine, "_make_exit_intent", lambda **kw: kw)
    decision = us_exit_engine.evaluate_exit(
        enriched, 90.0, now=datetime(2026, 11, 11, 15, tzinfo=timezone.utc), include_trend_time=False,
    )
    assert decision["qty"] == remaining
    assert debts[0][0]["position_cycle_id"] == proof["prospective_cycle"]
    assert db.buy == immutable_buy
    assert position["meta"] == {}
    # Filled TP2 is final on the ORIGINAL stage cycle; replay must not sell it
    # twice. A subsequent protective STOP can use the reduced holding.
    db.stages.append({"trade_date": "2026-11-11", "stage": "tp2", "stage_status": "DONE",
                      "position_lifecycle_id": OLD_CYCLE})
    assert not _authorize(scenario)[0]
    intent["qty"] = 1
    intent["reason"] = "HARD_STOP"
    intent["meta"].pop("profit_capture_stage")
    # A second authorization uses the persisted reduced holding, same lineage.
    ok, why, again = _authorize(scenario)
    assert ok, why
    assert again["prospective_cycle"] == proof["prospective_cycle"]
    assert bridge.recheck_legacy_nine_sell_before_http(intent, kis_client=client, proof=again, qty=1)[0]


@pytest.mark.parametrize("field,bad", [
    ("source_original_runtime_lifecycle_id", "foreign-cycle"),
    ("source_original_buy_order_no", "99999"),
    ("source_original_entry_exit_contract_sha256", "bad-contract"),
    ("broker_account_hash", "foreign-account"),
    ("portfolio_epoch_id", "foreign-portfolio"),
    ("position_cycle_id", "foreign-cycle"),
    ("symbol", "OTHER"),
    ("strategy_owner", "TQQQ_INFINITE"),
])
def test_forward_fill_cannot_borrow_other_source(scenario, field, bad):
    db, _, _ = _generalize(scenario, "MSFT")
    _apply_forward_fill(scenario)
    db.fills[-1]["meta"][field] = bad
    ok, _, _ = _authorize(scenario)
    assert not ok


def test_forward_order_and_fill_must_have_same_execution_key(scenario):
    db, _, _ = _generalize(scenario, "AMD")
    _apply_forward_fill(scenario)
    db.fills[-1]["client_order_key"] = "other-execution"
    assert not _authorize(scenario)[0]


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_authoritative_snapshot_then_reload_preserves_legacy_source(scenario, symbol):
    db, _, intent = _generalize(scenario, symbol)
    _apply_forward_fill(scenario)
    db.position["qty"] = 4  # snapshot preceded the latest proven SELL
    current = {"symbol": symbol, "qty": 3}
    context = bridge.verified_legacy_snapshot_context(db, position=current, trade_date="2026-11-11", active_epoch=EPOCH)
    assert context["position_lifecycle_id"] == OLD_CYCLE
    assert "portfolio_epoch_id" not in context and "position_cycle_id" not in context
    # Mirror the authoritative writer's persisted source reference and net qty.
    db.position["qty"] = 3
    db.position["meta"].update(context)
    evidence, why = bridge._evidence_from_db(db, symbol=symbol, trade_date="2026-11-11",
        active_epoch=EPOCH, raw_account=RAW_ACCOUNT, intent=intent)
    assert evidence, why
    assert evidence["holding"] == 3
    assert db.position["position_cycle_id"] is None
    assert bridge.verified_legacy_snapshot_context(db, position={**current, "qty": 2},
        trade_date="2026-11-11", active_epoch=EPOCH) is None


@pytest.mark.parametrize("bad", ["owner", "cycle", "policy"])
def test_snapshot_bridge_cannot_overwrite_conflicting_current_provenance(scenario, bad):
    db, _, _ = _generalize(scenario, "MSFT")
    pos = {"symbol": "MSFT", "qty": 4}
    if bad == "owner": pos["strategy_owner"] = "OTHER"
    if bad == "cycle": pos["position_lifecycle_id"] = "foreign-cycle"
    if bad == "policy":
        pos["entry_exit_contract"] = deepcopy(db.position["meta"]["entry_exit_contract"])
        pos["entry_exit_contract"]["management"]["swing"]["hard_stop"] = .9
    assert bridge.verified_legacy_snapshot_context(db, position=pos, trade_date="2026-11-11", active_epoch=EPOCH) is None


def test_general_bridge_does_not_include_infinite(scenario):
    _generalize(scenario, "TQQQ")
    assert not _authorize(scenario)[0]


@pytest.mark.parametrize("field,value", [("account_id", "foreign-account"), ("trading_epoch_id", "retired-epoch")])
def test_legacy_context_never_overwrites_conflicting_input_scope(scenario, monkeypatch, field, value):
    db, _, _ = _generalize(scenario, "MSFT")
    _engine_for(monkeypatch, db)
    pos = {"symbol": "MSFT", "qty": 4, "strategy_owner": "US_STANDARD", field: value}
    assert bridge.enrich_legacy_nine_v2_decision_position(pos, trade_date="2026-11-11") is pos
    assert pos[field] == value


@pytest.mark.parametrize("symbol", SYMBOLS)
@pytest.mark.parametrize("authoritative", [True, False])
def test_legacy_provenance_is_linked_before_generic_lifecycle_fence(scenario, monkeypatch, caplog, symbol, authoritative):
    from trader.us import position_lifecycle_state as lifecycle
    from trader.us.db import repos
    db, _, _ = _generalize(scenario, symbol)
    _engine_for(monkeypatch, db)
    monkeypatch.setattr(lifecycle, "load_latest_open_us_position_lifecycles", lambda *a: {})
    monkeypatch.setattr(repos, "load_us_position_risk_state_candidates_batch", lambda *a: {})
    pos = {"symbol": symbol, "qty": 4, "strategy_owner": "US_STANDARD", "entry_price": 100.0, "meta": {}}
    lifecycle.reconcile_us_position_lifecycles(
        positions=[pos], trade_date="2026-11-11", now=datetime.now(timezone.utc),
        authoritative=authoritative, env="practice",
    )
    assert pos["position_lifecycle_id"] == OLD_CYCLE
    assert pos["lifecycle_identity_status"] == "VERIFIED_LEGACY_ORIGINAL"
    assert not pos.get("portfolio_epoch_id")
    assert "ORIGINAL_SCOPE_FENCED" not in caplog.text


def _policy():
    contract = build_us_entry_exit_contract({
        "symbol": "GENERIC", "strategy_owner": "US_STANDARD", "book": "SWING_BOOK",
        "horizon": "SWING_CARRY", "exit_policy": "US_SWING_DEFAULT",
        "entry_identity_proof": {"quote_source": None, "selected_score": 1.0},
        "entry_decision_evidence_5w1h": {"entry_link": {"decision_id": None}, "value": [None, 1]},
    })
    return {"entry_exit_contract": contract, "entry_exit_contract_sha256": contract["sha256"],
            "entry_exit_contract_version": contract["version"], "book": "SWING_BOOK"}


def _elide(value):
    if isinstance(value, dict):
        return {k: _elide(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_elide(v) for v in value]
    return value


def test_null_elided_derivative_restores_exact_original_without_resigning():
    original = _policy()
    broken = _elide(original)
    assert not verify_us_entry_exit_contract(broken["entry_exit_contract"])
    repaired = carry_entry_policy(broken, original)
    assert repaired == original
    assert verify_us_entry_exit_contract(repaired["entry_exit_contract"])
    assert repaired["entry_exit_contract"]["sha256"] == broken["entry_exit_contract"]["sha256"]
    assert not verify_us_entry_exit_contract(broken["entry_exit_contract"])


@pytest.mark.parametrize("symbol", SYMBOLS)
def test_exit_resolver_keeps_valid_original_when_derived_policy_lost_nulls(monkeypatch, symbol):
    from trader.us.pb1 import us_exit_position_resolver as resolver
    original = _policy()
    lifecycle = {"lifecycle_id": "cycle", "strategy_owner": "US_STANDARD", "env": "practice",
                 "account_id": RAW_ACCOUNT, "trading_epoch_id": EPOCH, "is_open": True,
                 "opened_at": "2026-11-10T15:00:00Z", "entry_policy": _elide(original)}
    row = {"trade_date": "2026-11-18", "trading_epoch_id": EPOCH,
           "account_id": RAW_ACCOUNT, "state": {"lifecycle": lifecycle}}
    monkeypatch.setattr(resolver, "_identity_is_current", lambda *a: True)
    pos = {"symbol": symbol, "qty": 6, "entry_price": 100, "env": "practice",
           "account_id": RAW_ACCOUNT, "trading_epoch_id": EPOCH,
           "position_lifecycle_id": "cycle", "strategy_owner": "US_STANDARD", "meta": original}
    enriched = resolver._enrich_single_position(pos, trade_date="2026-11-19", env="practice",
                                               preloaded_risk_candidates=[row])
    assert enriched["entry_exit_contract"] == original["entry_exit_contract"]
    assert enriched["position_lifecycle_view"]["contract_integrity"] == "VALID"
    assert verify_us_entry_exit_contract(enriched["meta"]["entry_exit_contract"])
    assert not verify_us_entry_exit_contract(lifecycle["entry_policy"]["entry_exit_contract"])


@pytest.mark.parametrize("tamper", ["policy", "signature", "owner", "version", "provenance"])
def test_actual_contract_change_is_not_repaired(tamper):
    original = _policy()
    broken = _elide(original)
    c = broken["entry_exit_contract"]
    if tamper == "policy": c["management"]["swing"]["hard_stop"] = .99
    if tamper == "signature": c["sha256"] = "other-signature"
    if tamper == "owner": c["strategy_owner"] = "OTHER"
    if tamper == "version": broken["entry_exit_contract_version"] = "other-version"
    if tamper == "provenance": c["entry_provenance"]["entry_identity_proof"]["selected_score"] = .1
    assert restore_null_elided_policy(broken, original) is None


def test_atomic_memory_carry_repairs_only_null_loss(monkeypatch):
    from trader.us.db import repos
    repos.reset_memory_stores()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *a: EPOCH)
    life = {"lifecycle_id": "canonical-cycle", "strategy_owner": "US_STANDARD", "account_id": RAW_ACCOUNT,
            "env": "practice", "trading_epoch_id": EPOCH, "is_open": True, "high_watermark": 120,
            "opened_at": "2026-11-10T15:00:00+00:00", "entry_policy": _elide(_policy())}
    repos.save_us_position_lifecycle_atomic("GENERIC", "2026-11-19", life)
    restored = repos.save_us_position_lifecycle_atomic("GENERIC", "2026-11-19", {
        **life, "high_watermark": 100, "entry_policy": _policy(),
    })
    assert restored["entry_policy"] == _policy()
    assert restored["high_watermark"] == 120
    wrong = deepcopy(_policy())
    wrong["entry_exit_contract"]["management"]["swing"]["hard_stop"] = .99
    again = repos.save_us_position_lifecycle_atomic("GENERIC", "2026-11-19", {**life, "entry_policy": wrong})
    assert again["entry_policy"] == restored["entry_policy"]


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires real PostgreSQL")
def test_postgres_signed_json_null_roundtrip_and_atomic_repair(pg_engine, monkeypatch):
    from sqlalchemy import text
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *a: EPOCH)
    original = _policy()
    life = {"lifecycle_id": "canonical-cycle", "strategy_owner": "US_STANDARD", "account_id": RAW_ACCOUNT,
            "env": "practice", "trading_epoch_id": EPOCH, "is_open": True, "high_watermark": 120,
            "opened_at": "2026-11-10T15:00:00+00:00", "entry_policy": original}
    first = repos.save_us_position_lifecycle_atomic("GENERIC", "2026-11-19", life)
    assert verify_us_entry_exit_contract(first["entry_policy"]["entry_exit_contract"])
    with pg_engine.begin() as conn:
        conn.execute(text("UPDATE us_position_risk_state SET state=jsonb_strip_nulls(state) WHERE symbol='GENERIC'"))
    again = repos.save_us_position_lifecycle_atomic("GENERIC", "2026-11-19", {**life, "high_watermark": 90})
    assert again["entry_policy"] == original
    assert again["high_watermark"] == 120
    final = repos.save_us_position_lifecycle_atomic("GENERIC", "2026-11-19", life)
    assert final["entry_policy"] == original


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires real PostgreSQL")
@pytest.mark.parametrize("symbol", ["AAPL", "UNLISTED_TEST"])
def test_postgres_forward_partial_fill_is_accepted_after_reload(pg_engine, scenario, monkeypatch, symbol):
    from sqlalchemy import text
    from trader.us.db import repos
    from trader.us.execution import recovery_identity
    db, _, intent = _generalize(scenario, symbol)
    proof = _apply_forward_fill(scenario)
    monkeypatch.setattr(recovery_identity, "current_us_portfolio_epoch", lambda *a, **kw: PORTFOLIO)
    with pg_engine.begin() as conn:
        conn.exec_driver_sql("ALTER TABLE us_orders ADD COLUMN IF NOT EXISTS env TEXT DEFAULT 'practice'")
        repos._ensure_us_position_risk_state_table(conn)
        conn.execute(text("DELETE FROM us_position_risk_state WHERE trading_epoch_id=:epoch AND symbol=:symbol"),
                     {"epoch": EPOCH, "symbol": symbol})
        conn.execute(text("""INSERT INTO us_position_risk_state(trading_epoch_id,trade_date,symbol,state)
            VALUES (:epoch,'2026-11-04',:symbol,CAST(:state AS jsonb))"""),
                     {"epoch": EPOCH, "symbol": symbol, "state": json.dumps({"lifecycle": db.history["lifecycle"]})})
        conn.execute(text("""INSERT INTO us_positions(as_of,symbol,exchange,qty,avg_cost,meta,trading_epoch_id)
            VALUES ('2026-11-11',:symbol,'NYSE',3,100,CAST(:meta AS jsonb),:epoch)"""),
                     {"symbol": symbol, "meta": json.dumps(db.position["meta"]), "epoch": EPOCH})
        for row in [dict(db.buy, side="BUY"), *[dict(r, side="SELL") for r in db.sell_orders]]:
            conn.execute(text("""INSERT INTO us_orders(trade_date,symbol,exchange,side,qty_requested,qty_filled,
                avg_price_usd,order_no,status,env,dry_run,meta,trading_epoch_id,account_id,
                portfolio_epoch_id,position_cycle_id,client_order_key)
                VALUES (:day,:symbol,'NYSE',:side,:requested,:filled,100,:no,:status,'practice',false,
                CAST(:meta AS jsonb),:epoch,:account,:portfolio,:cycle,:key)"""),
                {"day": row["trade_date"], "symbol": symbol, "side": row["side"],
                 "requested": row["qty_requested"], "filled": row["qty_filled"], "no": row["order_no"],
                 "status": row["status"], "meta": json.dumps(row["meta"]), "epoch": EPOCH,
                 "account": row.get("account_id"), "portfolio": row.get("portfolio_epoch_id"),
                 "cycle": row.get("position_cycle_id"), "key": row.get("client_order_key", row["order_no"])})
        for row in db.fills:
            conn.execute(text("""INSERT INTO us_fills(trade_date,symbol,exchange,side,qty,price_usd,
                order_no,meta,trading_epoch_id,account_id,portfolio_epoch_id,position_cycle_id,client_order_key)
                VALUES (:day,:symbol,'NYSE',:side,:qty,100,:no,CAST(:meta AS jsonb),:epoch,
                        :account,:portfolio,:cycle,:key)"""),
                {"day": row["trade_date"], "symbol": symbol, "side": row["side"], "qty": row["qty"],
                 "no": row["order_no"], "meta": json.dumps(row["meta"]), "epoch": EPOCH,
                 "account": row.get("account_id"), "portfolio": row.get("portfolio_epoch_id"),
                 "cycle": row.get("position_cycle_id"), "key": row.get("client_order_key", row["order_no"])})
    # Fresh connections exercise SQL selection and JSONB reconstruction.
    for _ in range(2):
        with pg_engine.connect() as conn:
            evidence, why = bridge._evidence_from_db(conn, symbol=symbol, trade_date="2026-11-11",
                active_epoch=EPOCH, raw_account=RAW_ACCOUNT, intent=intent)
            assert evidence, why
            assert evidence["holding"] == 3
            assert evidence["original_cycle"] == OLD_CYCLE
            assert len(evidence["prior_sell_proofs"]) == 2
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: EPOCH)
    assert repos.save_position_snapshot(
        [{"symbol": symbol, "exchange": "NYSE", "qty": 3, "avg_cost": 100}],
        trade_date="2026-11-11", authoritative_positions=True,
        balance_fetch_status="OK", balance_parse_status="OK",
    ) == 1
    with pg_engine.connect() as conn:
        stored = conn.execute(text("SELECT account_id,portfolio_epoch_id,position_cycle_id,meta FROM us_positions WHERE symbol=:symbol AND as_of='2026-11-11'"),
                              {"symbol": symbol}).mappings().one()
        assert stored["portfolio_epoch_id"] is None and stored["position_cycle_id"] is None
        assert stored["meta"]["position_lifecycle_id"] == OLD_CYCLE
        assert stored["meta"]["lineage_identity_status"] == "VERIFIED_LEGACY_ORIGINAL"
        evidence, why = bridge._evidence_from_db(conn, symbol=symbol, trade_date="2026-11-11",
            active_epoch=EPOCH, raw_account=RAW_ACCOUNT, intent=intent)
        assert evidence, why
    with pg_engine.begin() as conn:
        conn.execute(text("UPDATE us_fills SET client_order_key='different-order' WHERE order_no='5001'"))
    with pg_engine.connect() as conn:
        evidence, _ = bridge._evidence_from_db(conn, symbol=symbol, trade_date="2026-11-11",
            active_epoch=EPOCH, raw_account=RAW_ACCOUNT, intent=intent)
        assert evidence is None


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires real PostgreSQL")
def test_postgres_soft_stop_actual_fill_progress_is_idempotent_and_not_terminal(pg_engine, monkeypatch):
    import hashlib
    from sqlalchemy import text
    from trader import account_state
    from trader.us.db import repos
    from trader.us.protective_stop_obligation import refresh_soft_stop_fill_progress
    monkeypatch.setattr(account_state, "get_account_key", lambda **kw: RAW_ACCOUNT)
    monkeypatch.setattr(account_state, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: EPOCH)
    hashed = hashlib.sha256(RAW_ACCOUNT.encode()).hexdigest()
    with pg_engine.begin() as conn:
        conn.exec_driver_sql("ALTER TABLE us_orders ADD COLUMN IF NOT EXISTS env TEXT DEFAULT 'practice'")
        conn.exec_driver_sql(open("migrations/0058_us_protective_stop_obligations.sql").read())
        for column in ("portfolio_epoch_id", "position_cycle_id"):
            conn.exec_driver_sql(f"ALTER TABLE us_protective_stop_obligations ADD COLUMN IF NOT EXISTS {column} TEXT")
        conn.execute(text("DELETE FROM us_protective_stop_obligations WHERE trading_epoch_id=:epoch"), {"epoch": EPOCH})
        conn.execute(text("""INSERT INTO us_protective_stop_obligations(trading_epoch_id,env,account_id,
            strategy_owner,symbol,lifecycle_id,stop_family,severity,target_qty,signal_price,signal_pnl_pct,
            soft_stop_pct,hard_stop_pct,first_signal_at,last_signal_at,portfolio_epoch_id,position_cycle_id)
            VALUES (:epoch,'practice',:raw,'US_STANDARD','GENERIC','cycle','soft_stop_loss',1,43,90,-.05,
                    .03,.06,'2026-11-18T14:00:00Z','2026-11-18T14:00:00Z',:portfolio,'cycle')"""),
                     {"epoch": EPOCH, "raw": RAW_ACCOUNT, "portfolio": PORTFOLIO})
        meta = json.dumps({"stop_type": "soft_stop_loss", "execution_action_key": "stop-action",
                           "position_lifecycle_id": "cycle", "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                           "is_synthetic": False})
        params = {"epoch": EPOCH, "account": hashed, "portfolio": PORTFOLIO, "meta": meta}
        conn.execute(text("""INSERT INTO us_orders(trading_epoch_id,trade_date,client_order_key,symbol,
            exchange,side,qty_requested,qty_filled,avg_price_usd,order_no,status,env,dry_run,account_id,
            portfolio_epoch_id,position_cycle_id,meta) VALUES (:epoch,'2026-11-18','stop-order','GENERIC',
            'NYSE','SELL',43,43,90,'9001','FILLED','practice',false,:account,:portfolio,'cycle',CAST(:meta AS jsonb))"""), params)
        conn.execute(text("""INSERT INTO us_fills(trading_epoch_id,trade_date,client_order_key,symbol,
            exchange,side,qty,price_usd,order_no,filled_at,account_id,portfolio_epoch_id,position_cycle_id,meta)
            VALUES (:epoch,'2026-11-18','stop-order','GENERIC','NYSE','SELL',43,90,'9001',
                    '2026-11-18T15:00:00Z',:account,:portfolio,'cycle',CAST(:meta AS jsonb))"""), params)
        conn.execute(text("""INSERT INTO us_execution_claims(action_key,env,account_id,market,trading_epoch_id,
            strategy_owner,lifecycle_id,action,target_qty,cumulative_filled_qty,remaining_target_qty,action_state,
            portfolio_epoch_id,position_cycle_id) VALUES ('stop-action','practice',:account,'US',:epoch,
            'US_STANDARD','cycle','SOFT_STOP',43,43,0,'SATISFIED',:portfolio,'cycle')"""), params)
    assert refresh_soft_stop_fill_progress(trade_date="2026-11-18")["updated"] == 1
    assert refresh_soft_stop_fill_progress(trade_date="2026-11-18")["updated"] == 0
    with pg_engine.connect() as conn:
        row = conn.execute(text("SELECT confirmed_filled_qty,obligation_state FROM us_protective_stop_obligations WHERE symbol='GENERIC'")).one()
        assert tuple(row) == (43, "PARTIAL")
    with pg_engine.begin() as conn:
        conn.execute(text("UPDATE us_protective_stop_obligations SET severity=2,target_qty=44,obligation_state='ESCALATED' WHERE symbol='GENERIC'"))
    assert refresh_soft_stop_fill_progress(trade_date="2026-11-18")["updated"] == 0


from tests.us.test_us_postgres_integration_real import pg_engine
