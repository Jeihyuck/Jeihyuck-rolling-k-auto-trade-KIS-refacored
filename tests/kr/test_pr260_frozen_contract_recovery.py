"""Offline reproduction of the 2026-10-27 KR frozen BUY contract loss.

No KIS orders or Supabase mutations. Reconciliation and position updates run
against in-memory SQLite using the same schema and contract hash functions.
"""
from datetime import date, datetime, timezone, timedelta
import hashlib
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.repos import (
    PositionsRepo, _kr_buy_entry_contract_hash, KR_BUY_ENTRY_CONTRACT_VERSION,
)
from trader.db.schema import schema_for_engine
from trader.kr.frozen_contract_recovery_260 import restore_verified_imported_buy_contracts


DAY = date(2026, 10, 27)
ACCOUNT_ID = "practice:dummy:01"
ACCOUNT_HASH = hashlib.sha256(ACCOUNT_ID.encode()).hexdigest()


def fixture_case(*, code="028050", qty=33, claim_state="SATISFIED",
                 broker_qty=None, bad_ack=False, duplicate=False,
                 existing_source=False, zero_broker_fill=False,
                 wrong_broker_order=False, wrong_trade_date=False,
                 wrong_broker_price=False, extra_side=None,
                 claim_account=None, nonzero_preorder=False,
                 broker_avg=61906, other_sleeve_order=False):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    epoch, portfolio, source_cycle, imported_cycle = (str(uuid4()) for _ in range(4))
    order_id = str(uuid4())
    plan = {
        "entry_reason": "ENTRY_MOMENTUM",
        "entry_style_selected": "ENTRY_MOMENTUM",
        "entry_thesis": "MOMENTUM_RECLAIM",
        "exit_policy_family": "INTRADAY_PROFIT_PROTECT",
        "trade_horizon": "DAY_TRADE",
        "eod_action": "FORCE_EXIT",
        "force_eod_close": True,
        "policy_version": "pb1_entry_exit_plan_v1",
        "policy_source": "style_mapping",
        "time_plan": {"max_trading_days": 1},
        "risk_plan": {"initial_stop": 55000, "risk_R": 5000},
    }
    request = {
        "entry_contract_version": KR_BUY_ENTRY_CONTRACT_VERSION,
        "entry_exit_plan": plan,
        "entry_meta": {"entry_reason": "ENTRY_MOMENTUM"},
        "pre_order_holding_qty": 4 if nonzero_preorder else 0,
        "requested_qty": qty,
        "submitted_qty": qty,
        "balance_snapshot_id": str(uuid4()),
        "client_order_key": "pb1-20261027-"+code,
        "position_cycle_id": source_cycle,
        "portfolio_epoch_id": portfolio,
        "trading_epoch_id": epoch,
    }
    request["entry_contract_sha256"] = _kr_buy_entry_contract_hash(request)
    ack = "invalid_hash" if bad_ack else request["entry_contract_sha256"]
    now = datetime(2026, 10, 27, 0, 32, tzinfo=timezone.utc)
    base = dict(env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
                code=code, portfolio_epoch_id=portfolio, trading_epoch_id=epoch)
    with engine.begin() as c:
        c.execute(schema.trading_epochs.insert().values(
            trading_epoch_id=epoch, env="practice", account_id=ACCOUNT_ID,
            status="ACTIVE", reason="unit",
        ))
        c.execute(schema.portfolio_epochs.insert().values(
            portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
            env="practice", account_id=ACCOUNT_ID,
            sid=1, mode=1, strategy="pb1_pullback_close",
            status="ACTIVE", reason="unit",
        ))
        c.execute(schema.orders.insert().values(
            order_id=order_id, position_cycle_id=source_cycle, **base,
            side="BUY", ord_type="LIMIT", qty=qty,
            client_order_key=request["client_order_key"],
            status="RECONCILED", kis_odno="0000011241",
            request_json=request,
            response_json={
                "_kr_buy_entry_contract": {"entry_contract_sha256": ack},
                "pdno": code,
                "odno": "0000099999" if wrong_broker_order else "0000011241",
                "ord_dt": "20261026" if wrong_trade_date else "20261027",
                "sll_buy_dvsn_cd": "02",
                "ord_qty": str(qty),
                "tot_ccld_qty": "0" if zero_broker_fill else str(qty),
                "rmn_qty": str(qty) if zero_broker_fill else "0",
                "tot_ccld_amt": "0" if zero_broker_fill else str(qty * 61906),
                "avg_prvs": "0" if zero_broker_fill else (
                    "100" if wrong_broker_price else "61906"
                ),
                "cncl_yn": "N",
            },
            created_at=now,
        ))
        c.execute(schema.positions.insert().values(
            position_id=str(uuid4()), position_cycle_id=imported_cycle, **base,
            qty=qty, avg_buy_price=61906, total_cost=qty*61906,
            status="OPEN", position_origin="IMPORTED",
            exit_policy_family="POLICY_MISSING", policy_source="missing",
            entry_exit_plan_json={}, entry_meta_json={},
        ))
        if other_sleeve_order:
            c.execute(schema.orders.insert().values(
                order_id=str(uuid4()), position_cycle_id=str(uuid4()),
                env="practice", strategy="KR_INFINITE",
                sid=1, mode=1, code=code,
                portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
                side="SELL", ord_type="LIMIT", qty=1,
                client_order_key="other-sleeve-"+code,
                status="CANCELLED", request_json={}, response_json={},
                created_at=now,
            ))
        if extra_side:
            c.execute(schema.orders.insert().values(
                order_id=str(uuid4()), position_cycle_id=str(uuid4()), **base,
                side=extra_side, ord_type="LIMIT", qty=2,
                client_order_key="extra-"+extra_side+"-"+code,
                status="RECONCILED", kis_odno="0000098765",
                request_json={}, response_json={}, created_at=now,
            ))
        if duplicate:
            c.execute(schema.positions.insert().values(
                position_id=str(uuid4()), position_cycle_id=str(uuid4()), **base,
                qty=3, avg_buy_price=61906, total_cost=3*61906,
                status="OPEN", position_origin="SYSTEM",
                exit_policy_family="INTRADAY_PROFIT_PROTECT",
            ))
        if existing_source:
            c.execute(schema.positions.insert().values(
                position_id=str(uuid4()), position_cycle_id=source_cycle, **base,
                qty=0, status="CLOSED", position_origin="SYSTEM",
            ))
        c.execute(schema.execution_claims.insert().values(
            action_key="claim-"+code, env="practice",
            account_id=claim_account if claim_account is not None else ACCOUNT_HASH,
            market="KR", trading_epoch_id=epoch, strategy_owner="PB1",
            lifecycle_id="PB1_ENTRY:pb1_pullback_close:KOSPI:1:"+code,
            action="BUY_ENTRY:PB1-AM", target_qty=qty,
            cumulative_filled_qty=qty if claim_state=="SATISFIED" else 0,
            remaining_target_qty=0 if claim_state=="SATISFIED" else qty,
            action_state=claim_state, trade_date=DAY,
        ))
    holdings = [{"pdno": code, "hldg_qty": str(qty if broker_qty is None else broker_qty),
                 "pchs_avg_pric": str(broker_avg)}]
    return engine, schema, holdings, source_cycle, request["entry_contract_sha256"]


def _run(engine, holdings, *, fresh=True, on_date=DAY, account_id=ACCOUNT_ID):
    return restore_verified_imported_buy_contracts(
        engine=engine, env="practice", strategy="pb1_pullback_close",
        trade_date=on_date, holdings_rows=holdings, fresh_balance_proven=fresh,
        account_id=account_id,
    )


def test_verified_momentum_33shares_restored_once_with_exact_sha():
    engine, schema, holdings, cycle, digest = fixture_case()
    assert _run(engine, holdings) == 1
    assert _run(engine, holdings) == 0
    with engine.connect() as c:
        rows = c.execute(sa.select(schema.positions)).mappings().all()
    assert len(rows) == 1
    pos = rows[0]
    assert pos["qty"] == 33
    assert pos["position_origin"] == "SYSTEM"
    assert pos["position_cycle_id"] == cycle
    assert pos["exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"
    assert pos["trade_horizon"] == "DAY_TRADE"
    assert pos["force_eod_close"] is True
    assert pos["entry_meta_json"]["entry_contract_sha256"] == digest
    assert pos["position_meta"]["frozen_contract_recovery"]["old_imported_cycle"]


@pytest.mark.parametrize("case", [
    {"claim_state": "UNCERTAIN"},
    {"broker_qty": 31},
    {"bad_ack": True},
    {"duplicate": True},
    {"existing_source": True},
    {"zero_broker_fill": True},  # Kakao Games: RECONCILED but 0 shares filled
    {"wrong_broker_order": True},
    {"wrong_trade_date": True},
    {"wrong_broker_price": True},
    {"extra_side": "SELL"},  # no adoption after ANY sell attempt
    {"extra_side": "BUY"},   # no adoption after another buy
    {"nonzero_preorder": True},
    {"claim_account": "different-account-hash"},
    {"broker_avg": 100},
    {"other_sleeve_order": True},
])
def test_unproven_or_ambiguous_buy_never_inherits_exit_contract(case):
    engine, schema, holdings, _cycle, _digest = fixture_case(**case)
    assert _run(engine, holdings) == 0
    with engine.connect() as c:
        rows = c.execute(sa.select(schema.positions).where(
            schema.positions.c.position_origin == "IMPORTED"
        )).mappings().all()
    assert len(rows) == 1
    assert rows[0]["exit_policy_family"] == "POLICY_MISSING"


def test_no_repair_without_fresh_broker_holdings():
    engine, schema, holdings, _, _ = fixture_case()
    assert _run(engine, holdings, fresh=False) == 0


def test_infinite_owner_not_reinterpreted_as_pb1():
    engine, schema, holdings, _, _ = fixture_case(code="122630", qty=11)
    assert _run(engine, holdings) == 0


def test_position_diagnostics_cannot_overwrite_frozen_exit_family():
    engine, schema, holdings, _, _ = fixture_case()
    assert _run(engine, holdings) == 1
    repo = PositionsRepo(engine)
    repo.update_position_fields(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="028050",
        fields={"exit_policy_family": "POLICY_MISSING", "max_price": 63000},
    )
    with engine.connect() as c:
        row = c.execute(sa.select(schema.positions)).mappings().one()
    assert row["exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"
    assert row["max_price"] == 63000


def test_previous_day_original_buy_restored_and_idempotent():
    engine, schema, holdings, cycle, _ = fixture_case(code="012750", qty=21)
    assert _run(engine, holdings, on_date=DAY + timedelta(days=1)) == 1
    assert _run(engine, holdings, on_date=DAY + timedelta(days=1)) == 0
    with engine.connect() as c:
        row = c.execute(sa.select(schema.positions)).mappings().one()
    assert row["position_cycle_id"] == cycle
    audit = row["position_meta"]["frozen_contract_recovery"]
    assert audit["source_trade_date"] == "2026-10-27"
    assert audit["reconciled_trade_date"] == "2026-10-28"
    assert row["exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"


def test_previous_day_broker_scope_and_lookback_fail_closed():
    engine, schema, holdings, _, _ = fixture_case()
    assert _run(engine, holdings, on_date=DAY + timedelta(days=8)) == 0
    assert _run(engine, holdings, account_id="practice:wrong-account") == 0
    from trader.kr.frozen_contract_recovery_260 import verify_recovered_contract_invariants
    assert verify_recovered_contract_invariants(
        engine=engine, env="practice", strategy="pb1_pullback_close"
    ) == 0


def test_reconciliation_detects_post_recovery_policy_regression():
    from trader.kr.frozen_contract_recovery_260 import verify_recovered_contract_invariants
    engine, schema, holdings, _, _ = fixture_case()
    assert _run(engine, holdings, on_date=DAY + timedelta(days=1)) == 1
    assert verify_recovered_contract_invariants(
        engine=engine, env="practice", strategy="pb1_pullback_close"
    ) == 0
    with engine.begin() as c:
        c.execute(schema.positions.update().values(
            exit_policy_family="POLICY_MISSING"
        ))
    assert verify_recovered_contract_invariants(
        engine=engine, env="practice", strategy="pb1_pullback_close"
    ) == 1


def test_prior_day_recovery_never_changes_fill_or_economics():
    engine, schema, holdings, _, _ = fixture_case()
    with engine.connect() as c:
        pre = c.execute(sa.select(
            schema.positions.c.qty, schema.positions.c.total_cost,
            schema.positions.c.realized_pnl,
        )).one()
    assert _run(engine, holdings, on_date=DAY + timedelta(days=1)) == 1
    with engine.connect() as c:
        post = c.execute(sa.select(
            schema.positions.c.qty, schema.positions.c.total_cost,
            schema.positions.c.realized_pnl,
        )).one()
        assert c.execute(sa.select(sa.func.count()).select_from(schema.fills)).scalar() == 0
    assert tuple(post) == tuple(pre)


def test_legacy_metadata_lane_cannot_clear_verified_eod_close():
    from trader.reconcile_kis import _restore_entry_meta_for_promoted_positions
    from trader.db.repos import OrdersRepo, LedgerEventsRepo
    engine, schema, holdings, _, _ = fixture_case()
    assert _run(engine, holdings, on_date=DAY + timedelta(days=1)) == 1
    _restore_entry_meta_for_promoted_positions(
        env="practice", strategy="pb1_pullback_close", engine=engine,
        orders_repo=OrdersRepo(engine), positions_repo=PositionsRepo(engine),
        ledger_repo=LedgerEventsRepo(engine),
    )
    with engine.connect() as c:
        p = c.execute(sa.select(schema.positions)).mappings().one()
    assert p["exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"
    assert p["trade_horizon"] == "DAY_TRADE"
    assert p["force_eod_close"] is True


def test_post_reconcile_detects_eod_flag_drift():
    from trader.kr.frozen_contract_recovery_260 import verify_recovered_contract_invariants
    engine, schema, holdings, _, _ = fixture_case()
    assert _run(engine, holdings, on_date=DAY + timedelta(days=1)) == 1
    with engine.begin() as c:
        c.execute(schema.positions.update().values(force_eod_close=False))
    assert verify_recovered_contract_invariants(
        engine=engine, env="practice", strategy="pb1_pullback_close"
    ) == 1



def test_zero_quantity_holding_with_zero_price_does_not_veto_valid_recovery():
    engine, schema, holdings, cycle, _ = fixture_case()
    holdings.insert(0, {"pdno": "999999", "hldg_qty": "0", "pchs_avg_pric": "0"})
    assert _run(engine, holdings, on_date=DAY + timedelta(days=1)) == 1
    with engine.connect() as c:
        pos = c.execute(sa.select(schema.positions)).mappings().one()
    assert pos["position_cycle_id"] == cycle
    assert pos["exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"


def test_prior_day_restore_preserves_real_holding_age_not_same_day():
    engine, schema, holdings, _, _ = fixture_case()
    assert _run(engine, holdings, on_date=DAY + timedelta(days=1)) == 1
    with engine.connect() as c:
        pos = c.execute(sa.select(schema.positions)).mappings().one()
    meta = pos["position_meta"]
    assert meta["same_day_entry_proven"] is False
    assert meta["same_day_entry_provenance"] == "ORDER_FILL_PRIOR_DAY"
    assert meta["frozen_contract_recovery"]["source_trade_date"] == "2026-10-27"
    assert pos["entry_ts"] is None  # exact broker fill timestamp not fabricated
    from trader.position_age import calc_position_age
    age = calc_position_age(meta["frozen_contract_recovery"]["source_trade_date"], DAY + timedelta(days=1))
    assert age.days_held == 1


def test_current_day_restore_keeps_current_day_flag_only_for_current_day():
    engine, schema, holdings, _, _ = fixture_case()
    assert _run(engine, holdings) == 1
    with engine.connect() as c:
        pos = c.execute(sa.select(schema.positions)).mappings().one()
    assert pos["position_meta"]["same_day_entry_proven"] is True
    assert pos["position_meta"]["same_day_entry_provenance"] == "ORDER_FILL_CURRENT_DAY"
