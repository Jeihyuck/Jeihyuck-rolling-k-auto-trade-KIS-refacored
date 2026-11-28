"""PORT-01/03/06/07/08 offline protections for future native integration.

NOT an authorization to deploy ACTIVE. Testing values are synthetic.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import hmac

import pytest

from trader.us.portfolio_profit_guard import (
    canonical, plan_verified_guard_sell, policy_digest, triggered_stage,
    verify_portfolio_policy, verified_sleeve_pnl,
)

STAMP = datetime(2026, 11, 23, 15, 0, tzinfo=timezone.utc)
ACCOUNT = "f" * 64
EPOCH = "practice-test-trading-epoch"
PORT = "practice-test-portfolio-epoch"
KEY = "offline-only-HMAC-" + "X" * 50


def _policy():
    doc = {
        "purpose": "US_STANDARD_PORTFOLIO_GIVEBACK_SELL_V1",
        "mode": "ACTIVE",
        "env": "practice",
        "sleeve": "US_STANDARD",
        "account_hash": ACCOUNT,
        "trading_epoch_id": EPOCH,
        "portfolio_epoch_id": PORT,
        "policy_id": "synthetic-policy",
        "policy_version": "v1",
        "effective_at": "2026-11-23T14:00:00+00:00",
        "expires_at": "2026-11-24T14:00:00+00:00",
        "accounting_method": "NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1",
        "mark_source": "KIS_BID",
        "activation_profit_usd": "100",
        "quote_ttl_sec": "5",
        "max_snapshot_skew_sec": "3",
        "rearm_mode": "OPERATOR_SIGNED_NEW_EPISODE",
        "selection_method": "NATIVE_UNDERPERFORMER_FIRST",
        "reentry_mode": "SIGNED_RELEASE_AFTER_COOLDOWN",
        "reentry_min_cooldown_seconds": "3600",
        "reentry_min_recovery_fraction": "0.1",
        "min_order_notional_usd": "1",
        "max_order_notional_usd": "1000000",
        "stages": [
            {"giveback_fraction": "0.25", "cumulative_reduction_fraction": "0.20"},
            {"giveback_fraction": "0.50", "cumulative_reduction_fraction": "0.35"},
        ],
    }
    doc["hmac_sha256"] = hmac.new(KEY.encode(), canonical(doc), hashlib.sha256).hexdigest()
    return doc


def _authorize(doc):
    return verify_portfolio_policy(
        doc, signing_key=KEY, account_hash=ACCOUNT,
        trading_epoch_id=EPOCH, portfolio_epoch_id=PORT, now_utc=STAMP,
    )


def test_signed_practice_only_sleeve_policy_allows_explicit_test_tiers():
    signed = _policy()
    body, digest = _authorize(signed)
    assert digest == policy_digest(signed)
    assert body["sleeve"] == "US_STANDARD"
    assert triggered_stage(pnl=Decimal("75"), hwm=Decimal("100"), policy=body) == 1
    assert triggered_stage(pnl=Decimal("45"), hwm=Decimal("100"), policy=body) == 2
    assert triggered_stage(pnl=Decimal("98"), hwm=Decimal("100"), policy=body) == 0
    assert triggered_stage(pnl=Decimal("0"), hwm=Decimal("0"), policy=body) == 0


@pytest.mark.parametrize("mutation", [
    lambda d: d.update({"account_hash": "e" * 64}),
    lambda d: d.update({"sleeve": "TQQQ_INFINITE"}),
    lambda d: d.update({"env": "live"}),
    lambda d: d.update({"mode": "SHADOW"}),
    lambda d: d.update({"activation_profit_usd": "1"}),
    lambda d: d.update({"stages": [
        {"giveback_fraction": "0.25", "cumulative_reduction_fraction": "0.20"},
        {"giveback_fraction": "0.20", "cumulative_reduction_fraction": "0.35"},
    ]}),
])
def test_unsigned_tamper_or_cross_owner_is_not_approval(mutation):
    doc = _policy()
    mutation(doc)
    with pytest.raises(ValueError):
        _authorize(doc)


def test_policy_wrong_epoch_expired_short_key_invalid():
    doc = _policy()
    with pytest.raises(ValueError):
        verify_portfolio_policy(
            doc, signing_key=KEY, account_hash=ACCOUNT,
            trading_epoch_id="other-epoch", portfolio_epoch_id=PORT, now_utc=STAMP,
        )
    with pytest.raises(ValueError):
        verify_portfolio_policy(
            doc, signing_key="short", account_hash=ACCOUNT,
            trading_epoch_id=EPOCH, portfolio_epoch_id=PORT, now_utc=STAMP,
        )
    with pytest.raises(ValueError):
        verify_portfolio_policy(
            doc, signing_key=KEY, account_hash=ACCOUNT,
            trading_epoch_id=EPOCH, portfolio_epoch_id=PORT,
            now_utc=STAMP + timedelta(days=3),
        )


def _fill(sym, side, qty, price, no):
    return {
        "symbol": sym, "side": side, "qty": qty,
        "price_usd": price, "trade_date": "2026-11-19",
        "order_no": no, "client_order_key": "key-" + no,
        "strategy_owner": "US_STANDARD",
        "account_hash": ACCOUNT, "trading_epoch_id": EPOCH,
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        "is_synthetic": False,
        "native_fee_proof": True,
        "commission_usd": "0", "tax_usd": "0",
    }


def _position(sym="KO", qty=5, mark="105", at=None):
    return {
        "symbol": sym, "strategy_owner": "US_STANDARD",
        "account_hash": ACCOUNT, "trading_epoch_id": EPOCH,
        "holding_qty": qty, "orderable_qty": qty, "mark_bid_usd": mark,
        "mark_source": "KIS_BID", "snapshot_native_kis": True,
        "quote_asof_utc": (at or STAMP).isoformat(),
    }


def _pnl(fills, positions, **kwargs):
    return verified_sleeve_pnl(
        fills, positions,
        snapshot_at=STAMP, account_hash=ACCOUNT, trading_epoch_id=EPOCH,
        quote_ttl_sec=Decimal("5"), max_skew_sec=Decimal("3"),
        broker_full_balance_verified=True,
        broker_cash_ledger_complete=True,
        cash_events=[], **kwargs,
    )


def test_us_standard_native_realized_plus_marked_pnl_with_closed_lot():
    rows = [
        _fill("KO", "BUY", 10, "100", "101"),
        _fill("KO", "SELL", 5, "110", "102"),
        _fill("PG", "BUY", 2, "50", "103"),
        _fill("PG", "SELL", 2, "60", "104"),
    ]
    pnl, amounts = _pnl(rows, [_position()])
    assert pnl == Decimal("95")
    assert amounts["buy_total"] == Decimal("1100")
    assert amounts["sell_total"] == Decimal("670")
    assert amounts["marked_open_equity"] == Decimal("525")
    # Input rows in a different arrival order may NOT change native net PnL.
    assert _pnl(list(reversed(rows)), [_position()])[0] == pnl


def test_broker_balance_ledger_and_price_fail_closed():
    rows = [_fill("KO", "BUY", 10, "100", "101"), _fill("KO", "SELL", 5, "110", "102")]
    with pytest.raises(ValueError, match="FULL_NATIVE_BALANCE_UNVERIFIED"):
        verified_sleeve_pnl(
            rows, [_position()], snapshot_at=STAMP, account_hash=ACCOUNT,
            trading_epoch_id=EPOCH, quote_ttl_sec=Decimal("5"),
            max_skew_sec=Decimal("3"),
        )
    with pytest.raises(ValueError, match="MARK_STALE"):
        _pnl(rows, [_position(at=STAMP - timedelta(seconds=8))])
    with pytest.raises(ValueError, match="KIS_LEDGER_NET_MISMATCH"):
        _pnl(rows, [_position(qty=6)])
    with pytest.raises(ValueError, match="DUPLICATE_NATIVE_FILL"):
        _pnl(rows + rows[:1], [_position()])
    bad = _fill("KO", "SELL", 5, "110", "102")
    bad["is_synthetic"] = True
    with pytest.raises(ValueError, match="SOURCE_INVALID"):
        _pnl(rows[:1] + [bad], [_position()])
    bad = _fill("TQQQ", "BUY", 10, "100", "105")
    bad["strategy_owner"] = "TQQQ_INFINITE"
    with pytest.raises(ValueError, match="SOURCE_INVALID"):
        _pnl(rows + [bad], [_position()])


def test_plan_fixed_episode_percent_not_repeated_on_remaining_balance():
    p = _policy()
    policy, _ = _authorize(p)
    positions = [
        {**_position("KO", qty=10, mark="10"),
         "position_cycle_id": "cycle-ko", "verified_entry_usd": "12"},
        {**_position("PG", qty=20, mark="10"),
         "position_cycle_id": "cycle-pg", "verified_entry_usd": "9"},
    ]
    # First stage cumulative reduction target=20% of frozen $500 exposure.
    plan = plan_verified_guard_sell(
        positions, policy=policy, stage_no=1,
        reference_exposure_usd=Decimal("500"), already_reduced_usd=Decimal("0"),
        excluded_symbols=set(),
    )
    assert sum(Decimal(x["planned_reduced_usd"]) for x in plan) == Decimal("100")
    assert plan[0]["symbol"] == "KO"
    # A $50 prior native fill reduces needed additional target to $50.
    plan = plan_verified_guard_sell(
        positions, policy=policy, stage_no=1,
        reference_exposure_usd=Decimal("500"), already_reduced_usd=Decimal("50"),
        excluded_symbols=set(),
    )
    assert sum(Decimal(x["planned_reduced_usd"]) for x in plan) == Decimal("50")
    assert plan_verified_guard_sell(
        positions, policy=policy, stage_no=1,
        reference_exposure_usd=Decimal("500"), already_reduced_usd=Decimal("0"),
        excluded_symbols={"KO", "PG"},
    ) == []


def test_unresolved_native_other_strategy_sell_symbol_is_not_eligible_for_guard():
    doc, _ = _authorize(_policy())
    blocked = {
        **_position("DHR", qty=1, mark="90"),
        "position_cycle_id": "forward-dhr-cycle",
        "verified_entry_usd": "100",
        "unresolved_sell": True,
    }
    other = {
        **_position("KO", qty=7, mark="100"),
        "position_cycle_id": "verified-ko-cycle",
        "verified_entry_usd": "95",
    }
    planned = plan_verified_guard_sell(
        [blocked, other], policy=doc, stage_no=1,
        reference_exposure_usd=Decimal("1000"),
        already_reduced_usd=Decimal("0"),
        excluded_symbols=set(),
    )
    assert planned
    assert all(row["symbol"] != "DHR" for row in planned)


def test_inactive_guard_close_is_explicit_off_and_cannot_touch_kis_or_db(monkeypatch):
    from trader.us.portfolio_profit_guard_store import portfolio_guard_close_health
    monkeypatch.setenv("US_PORTFOLIO_GUARD_ACTIVE", "0")
    assert portfolio_guard_close_health(env="practice", offline=False) == {
        "status": "OFF", "orders_checked": 0,
    }
    monkeypatch.setenv("US_PORTFOLIO_GUARD_ACTIVE", "1")
    assert portfolio_guard_close_health(env="practice", offline=True) == {
        "status": "OFF", "orders_checked": 0,
    }


def test_verified_net_cash_changes_profit_without_external_capital_miscount():
    buy = _fill("KO", "BUY", 10, "100", "net-buy")
    sell = _fill("KO", "SELL", 5, "110", "net-sell")
    buy["commission_usd"] = "2.25"
    sell["commission_usd"] = "1.75"
    sell["tax_usd"] = "0.50"
    one_native_dividend = [{
        "event_id": "kis-cash-123",
        "strategy_owner": "US_STANDARD",
        "account_hash": ACCOUNT, "trading_epoch_id": EPOCH,
        "evidence_type": "KIS_NATIVE_CASH_CONFIRMED",
        "event_type": "DIVIDEND_CASH",
        "occurred_at_utc": (STAMP - timedelta(minutes=1)).isoformat(),
        "amount_usd": "3.25",
    }]
    net, proof = verified_sleeve_pnl(
        [buy, sell], [_position()],
        snapshot_at=STAMP, account_hash=ACCOUNT, trading_epoch_id=EPOCH,
        quote_ttl_sec=Decimal("5"), max_skew_sec=Decimal("3"),
        broker_full_balance_verified=True,
        broker_cash_ledger_complete=True,
        cash_events=one_native_dividend,
    )
    # Gross $75 = sale $550 + holding $525 - purchase $1000.
    # Net = $75 - $4.00 fees - $0.50 tax + $3.25 dividend.
    assert net == Decimal("73.75")
    assert proof["commissions_usd"] == Decimal("4.00")
    assert proof["broker_cash_adjustments_usd"] == Decimal("3.25")
    # A second row with the same broker cash event id may not double-count.
    with pytest.raises(ValueError, match="CASH_EVENT_SCOPE_OR_SOURCE"):
        verified_sleeve_pnl(
            [buy, sell], [_position()], snapshot_at=STAMP,
            account_hash=ACCOUNT, trading_epoch_id=EPOCH,
            quote_ttl_sec=Decimal("5"), max_skew_sec=Decimal("3"),
            broker_full_balance_verified=True,
            broker_cash_ledger_complete=True,
            cash_events=one_native_dividend + one_native_dividend,
        )


def test_missing_proven_costs_or_cash_ledger_cannot_authorize_guard_sell():
    rows = [_fill("KO","BUY",10,"100","fee-buy"),
            _fill("KO","SELL",5,"110","fee-sell")]
    with pytest.raises(ValueError, match="NET_CASH_EVENTS_UNVERIFIED"):
        verified_sleeve_pnl(
            rows, [_position()], snapshot_at=STAMP, account_hash=ACCOUNT,
            trading_epoch_id=EPOCH, quote_ttl_sec=Decimal("5"),
            max_skew_sec=Decimal("3"),broker_full_balance_verified=True,
        )
    broken = dict(rows[0])
    broken.pop("native_fee_proof")
    with pytest.raises(ValueError, match="BROKER_FEES_UNVERIFIED"):
        _pnl([broken,rows[1]], [_position()])


@pytest.mark.parametrize("qty,price,client", [(6, "111", "different-client"), (5, "112", "key-102"), (5, "110", "different-client")])
def test_duplicate_cumulative_broker_order_is_not_an_incremental_fill(qty, price, client):
    buy = _fill("KO", "BUY", 20, "100", "101")
    sell = _fill("KO", "SELL", 5, "110", "102")
    duplicate = {**sell, "qty": qty, "price_usd": price,
                 "client_order_key": client, "order_no": "000000102"}
    with pytest.raises(ValueError, match="DUPLICATE_NATIVE_FILL"):
        _pnl([buy, sell, duplicate], [_position(qty=20-5-qty)])



def test_signed_trading_basis_is_explicit_and_not_a_net_policy_fallback():
    from trader.us.portfolio_profit_guard import TRADING_ACCOUNTING_METHOD
    doc = _policy()
    doc["accounting_method"] = TRADING_ACCOUNTING_METHOD
    with pytest.raises(ValueError, match="SIGNATURE_INVALID"):
        _authorize(doc)
    doc.pop("hmac_sha256")
    doc["hmac_sha256"] = hmac.new(KEY.encode(), canonical(doc), hashlib.sha256).hexdigest()
    body, _ = _authorize(doc)
    assert body["accounting_method"] == TRADING_ACCOUNTING_METHOD
    assert body["stages"] == _policy()["stages"]


def test_trading_basis_marks_open_and_closed_lots_without_inventing_costs():
    from trader.us.portfolio_profit_guard import TRADING_ACCOUNTING_METHOD
    rows = [
        _fill("KO", "BUY", 10, "100", "101"),
        _fill("KO", "SELL", 5, "110", "102"),
        _fill("PG", "BUY", 2, "50", "103"),
        _fill("PG", "SELL", 2, "60", "104"),
    ]
    for row in rows:
        row.pop("native_fee_proof")
        row.pop("commission_usd")
        row.pop("tax_usd")
    kwargs = dict(snapshot_at=STAMP, account_hash=ACCOUNT, trading_epoch_id=EPOCH,
                  quote_ttl_sec=Decimal("5"), max_skew_sec=Decimal("3"),
                  broker_full_balance_verified=True,
                  accounting_method=TRADING_ACCOUNTING_METHOD)
    pnl, proof = verified_sleeve_pnl(rows, [_position()], **kwargs)
    assert pnl == Decimal("95")
    assert proof["known_commissions_usd"] is None
    assert proof["confirmed_net_pnl_usd"] is None
    assert proof["broker_cash_ledger_complete"] is False
    rows[0].update(native_fee_proof=True, commission_usd="2.50", tax_usd="0.25")
    later, proof = verified_sleeve_pnl(rows, [_position()], **kwargs)
    assert later == pnl  # late fee evidence must not create a false drawdown
    assert proof["known_commissions_usd"] == Decimal("2.50")
    assert proof["known_taxes_usd"] == Decimal("0.25")
    assert proof["verified_fee_fill_count"] == 1
    assert proof["all_fill_fees_verified"] is False
    assert proof["confirmed_net_pnl_usd"] is None
    with pytest.raises(ValueError, match="KIS_LEDGER_NET_MISMATCH"):
        verified_sleeve_pnl(rows, [_position(qty=6)], **kwargs)
    with pytest.raises(ValueError, match="MARK_STALE"):
        verified_sleeve_pnl(rows, [_position(at=STAMP-timedelta(seconds=6))], **kwargs)
    with pytest.raises(ValueError, match="DUPLICATE_NATIVE_FILL"):
        verified_sleeve_pnl(rows+rows[:1], [_position()], **kwargs)
    with pytest.raises(ValueError, match="TRADING_MODE_CASH_INPUT_FORBIDDEN"):
        verified_sleeve_pnl(rows, [_position()], cash_events=[], **kwargs)



# Original archived native economics must never be assigned a retroactive
# portfolio identity. These are credential-free fixtures; no original rows
# or strategy settings from production are embedded.
def _legacy_guard_fixture(symbol="KO", *, close=False):
    from datetime import date
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    from trader.us.portfolio_guard_original_ledger import legacy_management_cycle
    raw = "synthetic-legacy-guard-account"
    account = hashlib.sha256(raw.encode()).hexdigest()
    original = "original-runtime-cycle-" + symbol
    day = "2026-10-27"
    contract = build_us_entry_exit_contract({
        "symbol": symbol, "strategy_owner": "US_STANDARD",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        "entry_reason": "TEST_ORIGINAL_BUY",
    })
    digest = contract["sha256"]
    management = legacy_management_cycle(
        raw_account=raw, epoch=EPOCH, portfolio=PORT,
        original_cycle=original, symbol=symbol, contract_sha=digest,
    )
    fills, orders = [], []

    def add(side, qty, price, order_no, when, meta, scoped=False):
        common = {"symbol": symbol, "side": side, "trade_date": when,
                  "order_no": order_no, "client_order_key": "key-" + order_no,
                  "account_id": account if scoped else None,
                  "portfolio_epoch_id": PORT if scoped else None,
                  "position_cycle_id": management if scoped else None,
                  "trading_epoch_id": EPOCH}
        meta = {"strategy_owner": "US_STANDARD", "is_synthetic": False,
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "trading_epoch_id": EPOCH, **meta}
        import copy
        fills.append({**common, "qty": qty, "price_usd": price, "meta": copy.deepcopy(meta)})
        orders.append({**common, "qty_requested": qty, "qty_filled": qty,
                       "avg_price_usd": price, "env": "practice", "dry_run": False,
                       "status": "FILLED", "meta": copy.deepcopy(meta)})

    add("BUY", 10, "100", "000101", day, {
        "position_lifecycle_id": "ENTRY:US_STANDARD:" + symbol,
        "entry_exit_contract": contract, "entry_exit_contract_sha256": digest,
    })
    add("SELL", 2, "110", "102", "2026-11-02", {
        "position_lifecycle_id": original, "source_entry_contract_sha256": digest,
    })
    add("SELL", 8 if close else 3, "120", "103", "2026-11-20", {
        "position_lifecycle_id": management, "position_cycle_id": management,
        "portfolio_epoch_id": PORT, "broker_account_hash": account,
        "forward_management_not_original_buy": True, "automated_sell_authorized": True,
        "us_legacy_forward_management_version": "US_LEGACY_NINE_V2_SELL_V1",
        "source_original_runtime_lifecycle_id": original,
        "source_original_buy_date": day, "source_original_buy_order_no": "101",
        "source_original_entry_exit_contract_sha256": digest,
    }, scoped=True)
    histories = [{"symbol": symbol, "trading_epoch_id": EPOCH, "lifecycle": {
        "lifecycle_id": original, "opened_trade_date": day, "account_id": raw,
        "trading_epoch_id": EPOCH, "strategy_owner": "US_STANDARD",
        "env": "practice", "is_open": True,
    }}]
    args = dict(raw_account=raw, epoch=EPOCH, portfolio=PORT,
                portfolio_start_day=date(2026, 11, 4))
    return fills, orders, histories, args, management


def test_legacy_original_and_forward_economics_proven_without_retroactive_scope():
    from trader.us.portfolio_guard_original_ledger import prove_legacy_epoch_fills, fill_key
    rows, orders, histories, args, cycle = _legacy_guard_fixture()
    keyed, groups = prove_legacy_epoch_fills(rows, orders, histories, **args)
    assert len(keyed) == 3
    assert groups["KO"]["net_qty"] == 5
    assert all(group["management_cycle"] == cycle for group in keyed.values())
    assert keyed[fill_key(rows[0])]["original_cycle"] == histories[0]["lifecycle"]["lifecycle_id"]
    assert rows[0]["account_id"] is None
    assert rows[0]["portfolio_epoch_id"] is None
    assert rows[0]["position_cycle_id"] is None
    assert rows[2]["position_cycle_id"] == cycle


def test_closed_original_and_new_canonical_buy_same_symbol_remain_separate():
    from trader.us.portfolio_guard_original_ledger import prove_legacy_epoch_fills, fill_key
    rows, orders, histories, args, cycle = _legacy_guard_fixture("ABBV", close=True)
    rows.append({**rows[0], "trade_date": "2026-11-23", "order_no": "104",
                 "client_order_key": "new-buy", "account_id": hashlib.sha256(args["raw_account"].encode()).hexdigest(),
                 "portfolio_epoch_id": PORT, "position_cycle_id": "new-native-cycle"})
    keyed, groups = prove_legacy_epoch_fills(rows, orders, histories, **args)
    assert groups["ABBV"]["net_qty"] == 0
    assert groups["ABBV"]["management_cycle"] == cycle
    assert fill_key(rows[-1]) not in keyed


@pytest.mark.parametrize("mutation,reason", [
    ("missing_order", "ORIGINAL_ORDER_NOT_UNIQUE"),
    ("duplicate_order", "ORIGINAL_ORDER_NOT_UNIQUE"),
    ("wrong_price", "ORIGINAL_ORDER_ECONOMICS_MISMATCH"),
    ("foreign_history", "ORIGINAL_RUNTIME_ACCOUNT_OR_CYCLE_CONFLICT"),
    ("wrong_forward", "FORWARD_SELL_SOURCE_CONFLICT"),
    ("partial_scope", "PARTIAL_SCOPE_UNVERIFIED"),
    ("synthetic", "NATIVE_EVIDENCE_INVALID"),
    ("tampered_buy", "ORIGINAL_BUY_CONTRACT_INVALID"),
    ("wrong_legacy_sell", "ARCHIVED_SELL_SOURCE_CONFLICT"),
    ("wrong_legacy_date", "BUY_NOT_PREPORTFOLIO"),
])
def test_legacy_proof_rejects_conflicting_or_unverified_sources(mutation, reason):
    from trader.us.portfolio_guard_original_ledger import prove_legacy_epoch_fills
    rows, orders, histories, args, _ = _legacy_guard_fixture()
    if mutation == "missing_order":
        orders.pop(0)
    elif mutation == "duplicate_order":
        orders.append(dict(orders[0]))
    elif mutation == "wrong_price":
        orders[1]["avg_price_usd"] = "111"
    elif mutation == "foreign_history":
        histories[0]["lifecycle"]["account_id"] = "other-account"
    elif mutation == "wrong_forward":
        rows[2]["meta"]["source_original_buy_order_no"] = "999"
    elif mutation == "partial_scope":
        rows[0]["portfolio_epoch_id"] = PORT
    elif mutation == "synthetic":
        rows[1]["meta"]["is_synthetic"] = True
    elif mutation == "tampered_buy":
        rows[0]["meta"]["entry_exit_contract"]["sha256"] = "bad"
    elif mutation == "wrong_legacy_sell":
        rows[1]["meta"]["position_lifecycle_id"] = "other-original-cycle"
        orders[1]["meta"]["position_lifecycle_id"] = "other-original-cycle"
    elif mutation == "wrong_legacy_date":
        from datetime import date
        args["portfolio_start_day"] = date(2026, 10, 27)
    with pytest.raises(ValueError, match=reason):
        prove_legacy_epoch_fills(rows, orders, histories, **args)


def test_legacy_history_scope_requires_explicit_signed_trading_basis():
    from trader.us.portfolio_profit_guard import LEGACY_ACCOUNTING_SCOPE, TRADING_ACCOUNTING_METHOD
    signed = _policy()
    signed["legacy_accounting_scope"] = LEGACY_ACCOUNTING_SCOPE
    with pytest.raises(ValueError, match="SIGNATURE_INVALID"):
        _authorize(signed)
    signed.pop("hmac_sha256")
    signed["hmac_sha256"] = hmac.new(KEY.encode(), canonical(signed), hashlib.sha256).hexdigest()
    with pytest.raises(ValueError, match="LEGACY_ACCOUNTING_SCOPE_UNVERIFIED"):
        _authorize(signed)
    signed.pop("hmac_sha256")
    signed["accounting_method"] = TRADING_ACCOUNTING_METHOD
    signed["hmac_sha256"] = hmac.new(KEY.encode(), canonical(signed), hashlib.sha256).hexdigest()
    assert _authorize(signed)[0]["legacy_accounting_scope"] == LEGACY_ACCOUNTING_SCOPE


def test_runtime_portfolio_guard_legacy_ledger_to_canonical_intent(monkeypatch):
    from contextlib import contextmanager
    from trader import account_state
    from trader.us.db import repos
    from trader.us.execution import recovery_identity, us_legacy_nine_active_sell as legacy
    from trader.us import portfolio_profit_guard_store as store
    from trader.us.portfolio_profit_guard import LEGACY_ACCOUNTING_SCOPE, TRADING_ACCOUNTING_METHOD
    from trader.us.portfolio_guard_original_ledger import prove_legacy_epoch_fills

    rows, orders, histories, args, cycle = _legacy_guard_fixture()
    _, groups = prove_legacy_epoch_fills(rows, orders, histories, **args)
    group = groups["KO"]
    account = hashlib.sha256(args["raw_account"].encode()).hexdigest()

    class Result:
        def __init__(self, rows): self.rows = rows
        def mappings(self): return self
        def all(self): return self.rows

    class Conn:
        def execute(self, query, params):
            sql = str(query)
            if "SELECT DISTINCT symbol FROM public.us_orders" in sql:
                return Result([])
            if "FROM public.us_execution_claims" in sql:
                return Result([])
            if "FROM public.us_fills" in sql:
                return Result(rows)
            if "SELECT started_at FROM public.portfolio_epochs" in sql:
                return Result([{"started_at": datetime(2026,11,4,tzinfo=timezone.utc)}])
            if "FROM public.us_orders" in sql:
                return Result(orders)
            if "FROM public.us_position_risk_state" in sql:
                return Result(histories)
            raise AssertionError(sql)

    class Engine:
        @contextmanager
        def begin(self): yield Conn()

    for key, val in {"US_PORTFOLIO_GUARD_ACTIVE":"1", "KIS_ENV":"practice",
                     "US_WS_COLLECTOR_ENABLED":"1", "US_SHARED_KIS_LIMITER_ENABLED":"1"}.items():
        monkeypatch.setenv(key, val)
    monkeypatch.setattr(account_state, "get_account_key", lambda **kw: args["raw_account"])
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: EPOCH)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
    monkeypatch.setattr(recovery_identity, "current_us_portfolio_epoch", lambda *a, **kw: PORT)
    # Confirm the shared production forward scope computes the same identity.
    assert legacy._verified_prospective_legacy_scope(
        Conn(), evidence={"original_cycle": group["original_cycle"], "v2_sha": group["contract_sha"]},
        symbol="KO", raw_account=args["raw_account"], epoch=EPOCH,
    ) == (PORT, cycle, account)
    monkeypatch.setattr(legacy, "_evidence_from_db", lambda *a, **kw: ({
        "original_cycle": group["original_cycle"], "v2_sha": group["contract_sha"],
        "source_buy_day": group["buy_day"], "source_buy_order": group["buy_order_no"],
        "holding": 5, "old_unresolved": [], "zero_fill_sell_proofs": [],
    }, "DB_LEGACY_EVIDENCE_VERIFIED"))
    policy = _policy()
    policy.pop("hmac_sha256")
    policy.update(account_hash=account, accounting_method=TRADING_ACCOUNTING_METHOD,
                  legacy_accounting_scope=LEGACY_ACCOUNTING_SCOPE)
    monkeypatch.setattr(store, "_get_policy", lambda *a, **kw: (policy, "f"*64))
    monkeypatch.setattr("trader.marketdata.us_ws_collector.get_quote", lambda *a, **kw: {
        "bid": "105", "received_at": datetime.now(timezone.utc).timestamp(),
    })
    captured = []
    def reserve(conn, **kwargs):
        captured.append(kwargs)
        return {"state":"PLANNED", "intents":[{"symbol":"KO", "position_cycle_id":cycle,
                 "action_key":"US_PPG_synthetic", "episode_id":"ep", "stage_no":1, "qty":1}]}
    monkeypatch.setattr(store, "persist_guard_observation_and_reserve", reserve)
    original = {"symbol":"KO", "qty":5, "orderable_qty":5, "strategy_owner":"US_STANDARD",
                "meta":{"entry_exit_contract":group["contract"],
                        "entry_exit_contract_sha256":group["contract_sha"],
                        "position_lifecycle_id":group["original_cycle"]}}
    kwargs = dict(current_positions=[original], trade_date="2026-11-24", env="practice",
                  authoritative_native_balance=True, excluded_sell_symbols=set())
    output = store.build_authorized_portfolio_guard_intents(**kwargs)
    # 2*110 + 3*120 + 5*105 - 10*100 = $105, before costs.
    assert captured[0]["verified_pnl_usd"] == Decimal("105")
    assert captured[0]["verified_accounting"]["legacy_fill_count"] == 3
    assert captured[0]["native_verified_sold_qty_by_cycle"] == {cycle:5}
    assert output[0]["meta"]["forward_management_not_original_buy"] is True
    assert output[0]["meta"]["source_original_runtime_lifecycle_id"] == group["original_cycle"]
    assert output[0]["meta"].get("automated_sell_authorized") is not True
    assert output[0]["position_cycle_id"] == cycle
    assert original.get("position_cycle_id") is None
    assert original["meta"]["position_lifecycle_id"] == group["original_cycle"]
    # Unapproved scope and balance mismatch never reach reservation.
    policy.pop("legacy_accounting_scope")
    assert store.build_authorized_portfolio_guard_intents(**kwargs) == []
    policy["legacy_accounting_scope"] = LEGACY_ACCOUNTING_SCOPE
    original["qty"] = 4
    assert store.build_authorized_portfolio_guard_intents(**kwargs) == []
    assert len(captured) == 1


@pytest.mark.parametrize("bid,expect", [
    ("100", "99.80"),
    ("0.01", None),  # previously stranded PLANNED after DB commit
    ("NaN", None),
    ("Infinity", None),
    ("0", None),
    ("-1", None),
])
def test_native_guard_price_is_preflighted_before_durable_reservation(bid, expect):
    from trader.us.portfolio_profit_guard_store import (
        preflight_guard_native_execution_prices, PortfolioGuardUnavailable,
    )
    native = [{"symbol": "KO", "mark_bid_usd": bid}]
    if expect is None:
        with pytest.raises((ValueError, PortfolioGuardUnavailable)):
            preflight_guard_native_execution_prices(native, lambda sym: "NYSE")
    else:
        result = preflight_guard_native_execution_prices(native, lambda sym: "NYSE")
        assert result["KO"] == ("NYSE", Decimal("100"), Decimal(expect))


def test_native_guard_preflight_fails_closed_on_exchange_or_duplicate_symbol():
    from trader.us.portfolio_profit_guard_store import (
        preflight_guard_native_execution_prices, PortfolioGuardUnavailable,
    )
    with pytest.raises(PortfolioGuardUnavailable, match="EXCHANGE_UNVERIFIED"):
        preflight_guard_native_execution_prices(
            [{"symbol": "KO", "mark_bid_usd": "100"}], lambda sym: None,
        )
    with pytest.raises(PortfolioGuardUnavailable, match="EXECUTABLE_PRICE_INVALID"):
        preflight_guard_native_execution_prices(
            [{"symbol": "KO", "mark_bid_usd": "100"},
             {"symbol": "ko", "mark_bid_usd": "100"}], lambda sym: "NYSE",
        )
