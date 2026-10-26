"""PR258: frozen PB1 entry horizon is authoritative over legacy position book tags."""
from datetime import date

from trader.pb1_engine import (
    _resolve_position_book,
    _resolve_position_horizon,
    _resolve_day_protect_exit,
)


def _momentum_position(**fields):
    base = {
        "trade_horizon": "DAY_TRADE",
        "exit_policy_family": "INTRADAY_PROFIT_PROTECT",
        "entry_exit_plan_json": {
            "trade_horizon": "DAY_TRADE",
            "entry_style_selected": "ENTRY_MOMENTUM",
            "exit_policy_family": "INTRADAY_PROFIT_PROTECT",
        },
        "entry_meta_json": {"trade_horizon": "DAY_TRADE", "book": "SWING_BOOK"},
        "position_meta": {
            "position_book": "SWING_BOOK",
            "trade_horizon": "DAY_TRADE",
        },
        "avg_buy_price": 113900,
        "qty": 17,
        "orderable_qty": 17,
    }
    base.update(fields)
    return base


def test_momentum_oct26_frozen_day_trade_overrides_swing_storage_book(monkeypatch):
    pos = _momentum_position()
    assert _resolve_position_horizon(pos, now_kst_date=date(2026, 10, 26)) == "DAY_PROTECT"
    assert _resolve_position_book(pos) == "DAY_BOOK"
    monkeypatch.setenv("PB1_DAY_PROTECT_ENABLED", "1")
    monkeypatch.setenv("PB1_DAY_STOP_LOSS_PCT", "2.0")
    decision = _resolve_day_protect_exit(
        pos, mark=111200, now_hhmm=1030,
        ret_pct=-2.37, max_pnl_pct=0.0, stop_hit=False,
    )
    assert decision["exit_ok"] is True
    assert decision["reason"] == "EXIT_DAY_STOP_LOSS"
    assert decision["qty"] == 17


def test_frozen_swing_and_core_keep_carry_no_forced_day_exit():
    for horizon, expected_horizon, expected_book in (
        ("SWING", "SWING_CARRY", "SWING_BOOK"),
        ("CORE", "CORE_CARRY", "CORE_BOOK"),
    ):
        pos = _momentum_position(
            trade_horizon=horizon,
            entry_exit_plan_json={"trade_horizon": horizon},
        )
        assert _resolve_position_horizon(pos) == expected_horizon
        assert _resolve_position_book(pos) == expected_book


def test_missing_contract_never_inferred_as_day_trade():
    pos = _momentum_position(
        trade_horizon="DAY_TRADE",
        exit_policy_family="POLICY_MISSING",
    )
    assert _resolve_position_horizon(pos, now_kst_date=date(2026, 10, 26)) == "POLICY_MISSING"
    assert _resolve_position_book(pos) == "POLICY_MISSING"


def test_legacy_book_without_frozen_horizon_keeps_old_behavior():
    pos = {"entry_meta_json": {"book": "SWING_BOOK"}, "position_meta": {}}
    assert _resolve_position_book(pos) == "SWING_BOOK"
    assert _resolve_position_horizon(pos) == "SWING_CARRY"


def test_proven_entry_contract_precedes_conflicting_unfrozen_position_tag():
    pos = _momentum_position(
        trade_horizon="SWING",
        entry_exit_plan_json={"trade_horizon": "DAY_TRADE"},
    )
    assert _resolve_position_book(pos) == "DAY_BOOK"
    assert _resolve_position_horizon(pos) == "DAY_PROTECT"
