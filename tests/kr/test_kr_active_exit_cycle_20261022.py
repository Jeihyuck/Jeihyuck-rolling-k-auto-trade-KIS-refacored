"""Regression for 2026-10-22 053800: CLOSED momentum must not own new SWING sell.

KIS balance is account-level; only the matching active position cycle
supplies entry contract and exit policy. No KIS orders are sent by these tests.
"""
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd

from trader.pb1_engine import PB1Engine


OLD_CYCLE = "3f8e25dc-56f5-4e1a-a6f7-961b4a412d0d"
NEW_CYCLE = "bb24815a-f713-49ab-8a05-bde33c9d8a53"


def _position(cycle, *, status, style, family, qty):
    return {
        "code": "053800", "strategy": "pb1_pullback_close", "sid": 1, "mode": 1,
        "status": status, "qty": qty, "avg_buy_price": 90200,
        "position_cycle_id": cycle, "portfolio_epoch_id": "same-active-epoch",
        "trading_epoch_id": "same-trading-epoch", "position_origin": "SYSTEM",
        "entry_style_selected": style, "exit_policy_family": family,
        "entry_meta_json": {"entry_style_selected": style},
        "position_meta": {},
    }


def _ctx(positions):
    class NoImport:
        def get_or_create_imported_cycle_for_kis_holding(self, **kwargs):
            raise AssertionError("Must not import a new cycle when one is already OPEN")
    ctx = SimpleNamespace(
        _now_kst=datetime(2026, 10, 22, 9, 42, tzinfo=ZoneInfo("Asia/Seoul")),
        _balance_price_map={}, _code_name_map={},
        positions_repo=NoImport(), fills_repo=object(),
    )
    holding = {"pdno": "053800", "hldg_qty": "12", "ord_psbl_qty": "12",
               "pchs_avg_pric": "90200", "prpr": "90100", "prdt_name": "안랩",
               "prdt_type_cd": "KOSDAQ"}
    return PB1Engine._build_holding_contexts_from_balance_rows(ctx, [holding], positions)


def test_oct22_anlab_closed_day_plan_never_overrides_new_open_swing():
    old = _position(OLD_CYCLE, status="CLOSED", style="ENTRY_MOMENTUM",
                    family="INTRADAY_PROFIT_PROTECT", qty=0)
    new = _position(NEW_CYCLE, status="OPEN", style="ENTRY_PULLBACK",
                    family="SWING_STAGED_EXIT", qty=12)
    for rows in ([old, new], [new, old]):
        holdings = _ctx(rows)
        assert len(holdings) == 1
        pos = holdings[0].to_position_dict()
        assert pos["position_cycle_id"] == NEW_CYCLE
        assert pos["entry_style_selected"] == "ENTRY_PULLBACK"
        assert pos["exit_policy_family"] == "SWING_STAGED_EXIT"
        assert pos["qty"] == 12


def test_same_code_multiple_open_cycles_fail_closed_before_order_submit():
    first = _position(OLD_CYCLE, status="OPEN", style="ENTRY_MOMENTUM",
                      family="INTRADAY_PROFIT_PROTECT", qty=15)
    second = _position(NEW_CYCLE, status="OPEN", style="ENTRY_PULLBACK",
                       family="SWING_STAGED_EXIT", qty=12)
    for rows in ([first, second], [second, first]):
        holdings = _ctx(rows)
        assert len(holdings) == 1
        pos = holdings[0].to_position_dict()
        assert pos["position_meta"]["lifecycle_blocked"] is True
        decision = PB1Engine._plan_exit_event(
            SimpleNamespace(), pos, {}, pd.DataFrame(), "manage",
        )
        assert decision["submitted"] == 0
        assert decision["order_result"] == "ORDER_SKIPPED_CYCLE_BINDING"


def test_explicit_closed_cycle_cannot_drive_exit_even_with_kis_qty():
    closed = _position(OLD_CYCLE, status="CLOSED", style="ENTRY_MOMENTUM",
                       family="INTRADAY_PROFIT_PROTECT", qty=0)
    closed["qty"] = 12  # former bug: live account qty overlaid on old policy
    decision = PB1Engine._plan_exit_event(
        SimpleNamespace(), closed, {}, pd.DataFrame(), "manage",
    )
    assert decision["order_result"] == "ORDER_SKIPPED_CYCLE_BINDING"
    assert decision["submitted"] == 0
