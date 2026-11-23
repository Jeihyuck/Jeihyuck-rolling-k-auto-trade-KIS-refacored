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
        broker_full_balance_verified=True, **kwargs,
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
