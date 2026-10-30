"""Mico 2026-10-29: restore frozen EOD policy with exact KIS proof only.

No original KIS order API is called; this is offline replay from saved
order request_json._reconcile_observation.kis_row + current balance.
"""
from __future__ import annotations

from datetime import datetime, date
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from trader.db.repos import KR_BUY_ENTRY_CONTRACT_VERSION, _kr_buy_entry_contract_hash
from trader.pb1_runner import (
    _verified_kr_close_broker_buy_contract,
    run_kr_close_policy_from_tagged_positions,
)
from trader.trade_plan import build_entry_exit_plan


def _fixtures(*, style="ENTRY_MOMENTUM", cost=2057950, qty=79, **overrides):
    cycle, epoch, trading = (str(uuid4()), str(uuid4()), str(uuid4()))
    entry_plan = build_entry_exit_plan(
        code="059090", market="KOSDAQ",
        entry_style_selected=style, entry_reason=style,
        entry_price=26050,
        features={"initial_stop": 25000},
    ).to_dict()
    order = {
        "strategy": "pb1_pullback_close",
        "env": "practice",
        "side": "BUY",
        "status": "RECONCILED",
        "code": "059090",
        "market": "KOSDAQ",
        "sid": 1, "mode": 1,
        "qty": qty, "kis_odno": "0000011865",
        "position_cycle_id": cycle,
        "portfolio_epoch_id": epoch,
        "trading_epoch_id": trading,
        "client_order_key": "practice:pb1_pullback_am:2026-10-29:059090:ENTRY:BUY:PB1-AM:morning:1",
    }
    request = {
        "enforce_entry_contract": True,
        "entry_contract_version": KR_BUY_ENTRY_CONTRACT_VERSION,
        "entry_exit_plan": entry_plan,
        "pre_order_holding_qty": 0,
        "requested_qty": qty,
        "submitted_qty": qty,
        "balance_snapshot_id": "20261029-open",
        "client_order_key": order["client_order_key"],
        "position_cycle_id": cycle,
        "portfolio_epoch_id": epoch,
        "trading_epoch_id": trading,
    }
    request["entry_contract_sha256"] = _kr_buy_entry_contract_hash(request)
    request["_reconcile_observation"] = {
        "kis_row": {
            "pdno": "059090", "odno": "0000011865",
            "ord_dt": "20261029", "sll_buy_dvsn_cd": "02",
            "cncl_yn": "N", "tot_ccld_qty": str(qty),
            "tot_ccld_amt": str(cost),
        },
    }
    order["request_json"] = request
    holding = {
        "pdno": "059090", "hldg_qty": str(qty), "ord_psbl_qty": str(qty),
        "pchs_amt": str(cost), "pchs_avg_pric": str(cost / qty),
        "prpr": "25450",
    }
    order.update(overrides)
    return holding, order


class _Positions:
    def list_positions_by_codes(self, **kwargs):
        return []


class _Fills:
    def list_latest_buy_fills_by_cycles(self, *args, **kwargs):
        return {}


class _Orders:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def list_kr_close_recovery_buy_orders(self, **kwargs):
        self.calls.append(kwargs)
        return self.rows


def _run(monkeypatch, holding, rows):
    import trader.pb1_runner as runner
    monkeypatch.setattr(runner, "now_kst", lambda: datetime(2026, 10, 29, 15, 15, tzinfo=ZoneInfo("Asia/Seoul")))
    order_repo = _Orders(rows)
    result = run_kr_close_policy_from_tagged_positions(
        kis_holdings=[holding], positions_repo=_Positions(),
        fills_repo=_Fills(), orders_repo=order_repo,
        kis_client=None, env="practice",
        strategy="pb1_pullback_close", dry_run=True,
    )
    return result, order_repo


def test_day_mico_79_proven_order_recovers_frozen_forced_exit(monkeypatch):
    holding, order = _fixtures()
    pure = _verified_kr_close_broker_buy_contract(holding, order, date(2026, 10, 29))
    assert pure is not None
    assert pure["position_cycle_id"] == order["position_cycle_id"]
    assert pure["fill_meta_json"]["trade_horizon"] == "DAY_TRADE"
    result, orders_repo = _run(monkeypatch, holding, [order])
    assert len(result["policy_orders"]) == 1
    sell = result["policy_orders"][0]
    assert sell["side"] == "SELL"
    assert sell["qty"] == 79
    assert sell["reason"] == "KR_CLOSE_DAY_FORCE_EOD"
    assert sell["position_cycle_id"] == order["position_cycle_id"]
    assert sell["portfolio_epoch_id"] == order["portfolio_epoch_id"]
    assert sell["metadata_source"] == "fill_meta"
    assert result["policy_results"][0]["result"] == "DRY_RUN"
    assert orders_repo.calls[0]["trade_date"] == date(2026, 10, 29)


@pytest.mark.parametrize("modify", [
    "wrong_cost", "wrong_holding_qty", "wrong_date",
    "wrong_broker_order", "wrong_side", "no_broker_proof",
    "unverified_contract", "preexisting_position", "no_orderable_qty",
    "wrong_cycle", "status_unresolved",
])
def test_any_unverified_broker_or_frozen_contract_keeps_hold(monkeypatch, modify):
    holding, order = _fixtures()
    req = order["request_json"]
    row = req["_reconcile_observation"]["kis_row"]
    if modify == "wrong_cost":
        holding["pchs_amt"] = "2100000"
    elif modify == "wrong_holding_qty":
        holding["hldg_qty"] = "78"
    elif modify == "wrong_date":
        row["ord_dt"] = "20261028"
    elif modify == "wrong_broker_order":
        row["odno"] = "0000000001"
    elif modify == "wrong_side":
        row["sll_buy_dvsn_cd"] = "01"
    elif modify == "no_broker_proof":
        del req["_reconcile_observation"]
    elif modify == "unverified_contract":
        req["entry_exit_plan"]["eod_action"] = "CARRY_IF_NO_EXIT_SIGNAL"
    elif modify == "preexisting_position":
        req["pre_order_holding_qty"] = 7
    elif modify == "no_orderable_qty":
        holding["ord_psbl_qty"] = "0"
    elif modify == "wrong_cycle":
        order["position_cycle_id"] = str(uuid4())
    elif modify == "status_unresolved":
        order["status"] = "SUBMITTED"
    result, _ = _run(monkeypatch, holding, [order])
    assert result["policy_orders"] == []
    assert result["policy_results"] == []


def test_ambiguous_same_day_buy_orders_cannot_force_close(monkeypatch):
    holding, order = _fixtures()
    other = {**order, "client_order_key": "other", "order_id": str(uuid4())}
    result, _ = _run(monkeypatch, holding, [order, other])
    assert result["policy_orders"] == []


def test_stored_swing_contract_is_not_forced_to_day(monkeypatch):
    holding, order = _fixtures(style="ENTRY_PULLBACK")
    result, _ = _run(monkeypatch, holding, [order])
    assert result["policy_orders"] == []
