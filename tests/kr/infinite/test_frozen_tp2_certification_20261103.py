"""KIS original-day proof gate for a frozen Infinite final TP2 and stale PB1 mirror.

All inputs are synthetic and no broker or database calls are made.
"""
from copy import deepcopy
from datetime import date, datetime, timezone
import pytest

from trader.kr.infinite.frozen_exit_certification import validate_frozen_tp2_proof

EPOCH = "09772c87-27ab-412d-b1c7-0a4a55196462"
CYCLE = "KRINF-20261026-eb6285bb"
PORTFOLIO = "f9fb141f-68a4-4c92-aa5b-20f9016b2b1a"


def _fixture():
    state = {
        "strategy_id": "KR_INFINITE_V1", "symbol": "122630",
        "trading_epoch_id": EPOCH, "status": "FROZEN",
        "cycle_id": CYCLE, "cycle_start_date": date(2026,10,26),
        "last_buy_date": date(2026,10,26), "cycle_complete_date": None,
        "last_exit_date": None, "units_used": 1, "core_units_used": 1,
        "version": 644,
        "metadata": {"profit_stage": "TP2_FILLED", "pending_profit_stage": None,
                     "last_profit_fill_date": "2026-10-27", "last_profit_fill_qty": 6},
    }
    history = [
        ("BUY", "NEW_CYCLE_BUY", 11, "20261026", "0000011438", "02", 740795),
        ("SELL_PARTIAL", "TAKE_PROFIT_TP1", 5, "20261027", "0000008799", "01", 358230),
        ("SELL_ALL", "TAKE_PROFIT_TP2", 6, "20261027", "0000017474", "01", 432540),
    ]
    intents, claims, mirror_orders = [], [], []
    pages = {
        "20261026": {"rt_cd": "0", "_sep29_complete": True, "output1": []},
        "20261027": {"rt_cd": "0", "_sep29_complete": True, "output1": []},
    }
    for i, (side, reason, qty, day, odno, side_cd, notional) in enumerate(history):
        key = f"infinite-action-{i}"
        intents.append({
            "strategy_id": "KR_INFINITE_V1", "symbol": "122630",
            "cycle_id": CYCLE, "trading_epoch_id": EPOCH,
            "trade_date": datetime.strptime(day, "%Y%m%d").date(),
            "side": side, "reason": reason, "requested_qty": qty, "filled_qty": qty,
            "filled_notional_krw": notional, "status": "FILLED",
            "broker_order_id": odno,
            "metadata": {"strategy_owner": "KR_INFINITE",
                         "trading_epoch_id": EPOCH,
                         "execution_claim_action_key": key},
        })
        claims.append({
            "action_key": key, "strategy_owner": "KR_INFINITE",
            "trading_epoch_id": EPOCH, "lifecycle_id": f"122630:{CYCLE}",
            "action_state": "SATISFIED", "target_qty": qty,
            "cumulative_filled_qty": qty, "remaining_target_qty": 0,
        })
        pages[day]["output1"].append({
            "ord_dt": day, "pdno": "122630", "odno": odno,
            "sll_buy_dvsn_cd": side_cd, "tot_ccld_qty": str(qty),
            "tot_ccld_amt": str(notional), "rmn_qty": "0", "cncl_yn": "N",
            "ord_tmd": ("103004" if i == 2 else "093151"),
        })
        mirror_orders.append({
            "order_id": f"legacy-{i}",
            "strategy": "pb1_pullback_close",
            "env": "practice", "code": "122630",
            "trading_epoch_id": EPOCH, "portfolio_epoch_id": PORTFOLIO,
            "position_cycle_id": None, "side": "BUY" if i == 0 else "SELL",
            "qty": qty, "status": "RECONCILED" if i == 0 else "FILLED",
            "kis_odno": odno,
            "submitted_at": datetime.strptime(
                day+" 00:31:54", "%Y%m%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc),
            "request_json": {},
        })
    positions = [{
        "position_id": "legacy-ghost-11", "code": "122630",
        "strategy": "pb1_pullback_close", "env": "practice",
        "trading_epoch_id": EPOCH, "status": "OPEN", "qty": 11,
        "position_cycle_id": "legacy-cycle", "portfolio_epoch_id": PORTFOLIO,
        "total_cost": 740800, "avg_buy_price": 67345.5, "realized_pnl": 0,
    }]
    # Original BUY and two SELLs were submitted on different KST dates.
    for x, spec in zip(mirror_orders, history):
        day = datetime.strptime(spec[3], "%Y%m%d").date()
        x["submitted_at"] = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    args = dict(
        state=state, intents=intents, claims=claims,
        mirror_positions=positions, mirror_orders=mirror_orders,
        broker_balance={"rt_cd": "0", "output1": []},
        balance_source="api", original_pages=pages, epoch_id=EPOCH,
    )
    return args


def test_nov_3_exact_original_infinite_proof_certifies_11_filled_and_mirror():
    proof = validate_frozen_tp2_proof(**_fixture())
    assert proof["state"] == "READY_TO_CERTIFY"
    assert proof["original_buy_qty"] == proof["original_sell_qty"] == 11
    assert proof["mirror_cost_gap_krw"] == "5"
    assert proof["gross_realized_krw"] == "49975"
    assert proof["updated_rows"] == 0
    assert proof["exit_date"] == "20261027"


@pytest.mark.parametrize("mutate,reason", [
    (lambda x: x["broker_balance"]["output1"].append({"pdno":"122630","hldg_qty":"1"}), "BROKER_POSITION_NOT_FLAT"),
    (lambda x: x["broker_balance"].update({"_diag_stub":True}), "FRESH_KIS_BALANCE_REQUIRED"),
    (lambda x: x["original_pages"]["20261027"].update({"_sep29_complete":False}), "ORIGINAL_KIS_FULL_HISTORY_REQUIRED"),
    (lambda x: x["intents"][2].update({"status":"RECONCILE_PENDING"}), "INFINITE_ORDER_OR_CLAIM_NOT_FINAL"),
    (lambda x: x["claims"][2].update({"action_state":"UNCERTAIN"}), "INFINITE_ORDER_OR_CLAIM_NOT_FINAL"),
    (lambda x: x["mirror_orders"][2]["request_json"].update({"strategy_owner":"PB1"}), "LEGACY_ORDER_NOT_PASSIVE_MIRROR"),
    (lambda x: x["mirror_positions"][0].update({"total_cost":700000}), "LEGACY_MIRROR_COST_NOT_RECONCILABLE"),
    (lambda x: x["state"].update({"status":"ACTIVE"}), "FROZEN_TP2_CYCLE_NOT_PROVEN"),
    (lambda x: x["intents"][2].update({"filled_qty":5}), "INFINITE_ORDER_OR_CLAIM_NOT_FINAL"),
])
def test_fail_closed_without_proof(mutate,reason):
    args = _fixture()
    mutate(args)
    result = validate_frozen_tp2_proof(**args)
    assert result == {"state": "BLOCKED", "reason": reason, "updated_rows": 0}


def test_unattributed_extra_122630_broker_trade_blocks_repair():
    args = _fixture()
    args["original_pages"]["20261027"]["output1"].append({
        "ord_dt": "20261027", "pdno": "122630",
        "odno": "0000099999", "sll_buy_dvsn_cd": "01",
        "tot_ccld_qty": "1", "tot_ccld_amt": "72090", "rmn_qty": "0",
    })
    assert validate_frozen_tp2_proof(**args)["reason"] == "UNATTRIBUTED_ORIGINAL_KIS_SYMBOL_ORDER"
