"""PORT-01/02/03/05/08/09/12 policy and execution proof tests.

Synthetic prices and portfolio figures are fixture values, not operating
thresholds. No KIS POST or real Supabase writes may be used.
"""
from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from trader.us.portfolio_profit_guard import (
    canonical, plan_verified_guard_sell, verify_portfolio_policy,
)
from trader.us.portfolio_profit_guard_store import (
    PortfolioGuardUnavailable, proven_episode_sells_at_frozen_prices,
    assert_portfolio_guard_reentry_budget,
)

KEY = "fixture-portfolio-" + "Z" * 60
NOW = datetime.now(timezone.utc)
ACCOUNT = "b" * 64


def _signed(*, stages=None, method="NATIVE_UNDERPERFORMER_FIRST", extra=None):
    p = {
        "purpose": "US_STANDARD_PORTFOLIO_GIVEBACK_SELL_V1",
        "mode": "ACTIVE", "env": "practice", "sleeve": "US_STANDARD",
        "policy_id": "portfolio-synthetic",
        "policy_version": "1", "account_hash": ACCOUNT,
        "trading_epoch_id": "synthetic-epoch",
        "portfolio_epoch_id": "synthetic-portfolio",
        "effective_at": (NOW - timedelta(hours=1)).isoformat(),
        "expires_at": (NOW + timedelta(days=1)).isoformat(),
        "accounting_method": "NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1",
        "mark_source": "KIS_BID", "quote_ttl_sec": "5",
        "max_snapshot_skew_sec": "3", "activation_profit_usd": "100",
        "selection_method": method, "rearm_mode": "OPERATOR_SIGNED_NEW_EPISODE",
        "reentry_mode": "SIGNED_RELEASE_AFTER_COOLDOWN",
        "reentry_min_cooldown_seconds": "3600",
        "reentry_min_recovery_fraction": "0.1",
        "min_order_notional_usd": "10", "max_order_notional_usd": "20000",
        "stages": stages or [
            {"giveback_fraction": "0.25", "cumulative_reduction_fraction": "0.20"},
            {"giveback_fraction": "0.50", "cumulative_reduction_fraction": "0.40"},
        ],
    }
    if extra:
        p.update(extra)
    p["hmac_sha256"] = hmac.new(
        KEY.encode(), canonical(p), hashlib.sha256,
    ).hexdigest()
    return p


def _verify(policy):
    return verify_portfolio_policy(
        policy, signing_key=KEY, account_hash=ACCOUNT,
        trading_epoch_id="synthetic-epoch",
        portfolio_epoch_id="synthetic-portfolio", now_utc=NOW,
    )[0]


def _position(sym, cycle, orderable, bid, entry):
    return {
        "symbol": sym, "strategy_owner": "US_STANDARD",
        "position_cycle_id": cycle, "orderable_qty": orderable,
        "mark_bid_usd": str(bid), "verified_entry_usd": str(entry),
    }


def test_signed_stages_are_partial_and_never_automatic_full_liquidation():
    doc = _signed()
    assert _verify(doc)["stages"][-1]["cumulative_reduction_fraction"] == "0.40"
    # 100% must be explicitly approved; signed *alone* is not sufficient.
    unsigned_liquidation_permission = _signed(
        stages=[{"giveback_fraction": "0.3", "cumulative_reduction_fraction": "1"}],
    )
    with pytest.raises(ValueError, match="FULL_LIQUIDATION"):
        _verify(unsigned_liquidation_permission)


def test_unapproved_strategy_or_reentry_rule_fences_without_native_post():
    with pytest.raises(ValueError, match="SELECTION_POLICY"):
        _verify(_signed(method="SELL_ALL_WINNERS"))
    with pytest.raises(ValueError, match="SIGNATURE_INVALID"):
        doc = _signed()
        doc["reentry_mode"] = "AUTOMATIC_REBUY"
        _verify(doc)


def test_original_episode_bid_not_current_quote_defines_new_sell_qty():
    policy = _verify(_signed())
    pos = [_position("KO", "ko-cycle", 10, "70", "100")]
    result = plan_verified_guard_sell(
        pos, policy=policy, stage_no=1,
        reference_exposure_usd=Decimal("1000"),
        already_reduced_usd=Decimal("0"), excluded_symbols=set(),
        episode_reference_prices={"ko-cycle": "100"},
    )
    # The stage fixed target is $200 at the original $100 price, hence 2
    # shares, not 2.85 based on the depressed executable bid $70.
    assert len(result) == 1
    assert result[0]["qty"] == 2
    assert result[0]["planned_reduced_usd"] == "200"


def test_prior_regular_hard_stop_or_tp_fill_counts_toward_portfolio_target():
    original = {
        "reference_exposure_usd": "1500",
        "original_lots": {
            "ko-cycle": {
                "initial_native_sell_qty": 2, "initial_holding_qty": 8,
                "reference_price_usd": "100",
            },
            "pg-cycle": {
                "initial_native_sell_qty": 0, "initial_holding_qty": 5,
                "reference_price_usd": "100",
            },
        },
    }
    assert proven_episode_sells_at_frozen_prices(
        original, {"ko-cycle": 4, "pg-cycle": 1},
    ) == Decimal("300")
    policy = _verify(_signed())
    pending = plan_verified_guard_sell(
        [_position("KO", "ko-cycle", 6, "60", "90"),
         _position("PG", "pg-cycle", 4, "90", "95")],
        policy=policy, stage_no=1,
        reference_exposure_usd=Decimal("1500"),
        already_reduced_usd=Decimal("300"),
        excluded_symbols=set(),
        episode_reference_prices={"ko-cycle": "100", "pg-cycle": "100"},
    )
    assert pending == []  # $300 stop/TP fills already met the signed 20% target


def test_one_pending_stop_symbol_never_selected_as_separate_guard_sell():
    policy = _verify(_signed())
    ko = _position("KO", "ko-cycle", 8, "100", "110")
    pg = _position("PG", "pg-cycle", 6, "100", "95")
    ko["unresolved_sell"] = True
    plan = plan_verified_guard_sell(
        [ko, pg], policy=policy, stage_no=1,
        reference_exposure_usd=Decimal("2000"),
        already_reduced_usd=Decimal("0"),
        excluded_symbols={"KO"},
        episode_reference_prices={"ko-cycle": "100", "pg-cycle": "100"},
    )
    assert plan and all(p["symbol"] == "PG" for p in plan)


def test_frozen_share_reduction_never_counts_a_market_drop_as_fill():
    anchor = {
        "reference_exposure_usd": "1000",
        "original_lots": {
            "ko-cycle": {
                "initial_native_sell_qty": 0, "initial_holding_qty": 10,
                "reference_price_usd": "100",
            },
        },
    }
    assert proven_episode_sells_at_frozen_prices(anchor, {}) == Decimal("0")
    # Price changes have no input into the native execution tally.
    assert proven_episode_sells_at_frozen_prices(anchor, {}) == Decimal("0")
    with pytest.raises(PortfolioGuardUnavailable, match="LEDGER_REGRESSED"):
        proven_episode_sells_at_frozen_prices(anchor, {"ko-cycle": -1})
    with pytest.raises(PortfolioGuardUnavailable, match="MORE_THAN_INITIAL"):
        proven_episode_sells_at_frozen_prices(anchor, {"ko-cycle": 11})


def test_tqqq_infinite_owner_never_enters_standard_guard_plan():
    policy = _verify(_signed())
    infinite = _position("TQQQ", "inf", 50, "100", "120")
    infinite["strategy_owner"] = "TQQQ_INFINITE"
    assert plan_verified_guard_sell(
        [infinite], policy=policy, stage_no=1,
        reference_exposure_usd=Decimal("5000"),
        already_reduced_usd=Decimal("0"),
        excluded_symbols=set(), episode_reference_prices={"inf": "100"},
    ) == []


def test_stage_two_uses_frozen_total_and_credited_real_fills():
    policy = _verify(_signed())
    pos = [_position("KO", "ko", 20, "80", "100")]
    plan = plan_verified_guard_sell(
        pos, policy=policy, stage_no=2,
        reference_exposure_usd=Decimal("2000"),
        already_reduced_usd=Decimal("400"),  # stage 1 already confirmed
        excluded_symbols=set(), episode_reference_prices={"ko": "100"},
    )
    assert sum(Decimal(p["planned_reduced_usd"]) for p in plan) == Decimal("400")


def test_guard_reentry_hold_fails_closed_if_shared_reservation_disabled(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_PORTFOLIO_GUARD_ACTIVE", "1")
    monkeypatch.setenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "0")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    with pytest.raises(PortfolioGuardUnavailable, match="SHARED_RESERVATION"):
        assert_portfolio_guard_reentry_budget(
            {"side": "BUY", "strategy_owner": "US_STANDARD"},
            order_notional_usd=Decimal("100"), now=NOW,
        )
    # TQQQ_INFINITE belongs to a completely different owner contract.
    assert_portfolio_guard_reentry_budget(
        {"side": "BUY", "strategy_owner": "TQQQ_INFINITE"},
        order_notional_usd=Decimal("100"), now=NOW,
    )
