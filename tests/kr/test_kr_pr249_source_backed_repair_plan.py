"""PR249 dry-run delta planner; synthetic proofs never certify live KIS."""
from copy import deepcopy
from datetime import date
import pytest
from scripts.kr.pr249_original_kis_readonly import TARGETS
from scripts.kr.pr249_source_backed_repair_plan import build_repair_plan

QUANTITY = {
    ("251970","BUY"):17, ("251970","SELL"):10,
    ("086450","BUY"):48, ("086450","SELL"):48,
    ("053800","BUY"):12, ("053800","SELL"):12,
    ("144960","BUY"):81, ("144960","SELL"):81,
    ("253590","BUY"):79, ("253590","SELL"):52,
}
AMOUNT = {
    ("251970","BUY"):1162800, ("251970","SELL"):685000,
    ("086450","BUY"):1176000, ("086450","SELL"):1180800,
    ("053800","BUY"):1082400, ("053800","SELL"):1084800,
    ("144960","BUY"):1137680, ("144960","SELL"):1091430,
    ("253590","BUY"):1274270, ("253590","SELL"):836180,
}

def _fixtures():
    reports, durable = [], []
    for day, targets in TARGETS.items():
        entries = []
        for (code,side,odno), requested in targets.items():
            qty, amount = QUANTITY[(code,side)], AMOUNT[(code,side)]
            entries.append({
                "trade_date":day.isoformat(),"code":code,"side":side,
                "kis_odno_suffix":odno[-4:],"requested_qty":requested,
                "broker_verified_qty":qty,"broker_verified_krw":str(amount),
                "status":"FULL_VERIFIED" if qty == requested else "PARTIAL_VERIFIED",
            })
            stored_qty = {
                ("251970","BUY"):32, ("251970","SELL"):0,
                ("086450","BUY"):70, ("253590","SELL"):0,
            }.get((code,side),qty)
            stored_amount = {
                ("251970","BUY"):2188800, ("251970","SELL"):0,
                ("086450","BUY"):1715000, ("253590","SELL"):0,
                ("144960","BUY"):1137645,
            }.get((code,side),amount)
            durable.append({
                "trade_date":day.isoformat(),"code":code,"side":side,
                "kis_odno":odno,"ordered_qty":requested,
                "env":"practice","strategy":"pb1_pullback_close","order_id":code+side,
                "position_cycle_id":"old-053800-cycle" if code=="053800" and side=="SELL" else code+"-cycle",
                "portfolio_epoch_id":"same-practice-epoch",
                "stored_fill_qty":stored_qty,"stored_fill_notional":str(stored_amount),
            })
        reports.append({"complete_kis_pages":True,"results":entries})
    return {"source":"KIS_PRACTICE_READONLY_NETWORK",
            "status":"READONLY_EVIDENCE_ONLY_NO_DB_MUTATIONS",
            "reports":reports},durable


def test_plan_reports_all_known_oct21_22_anomalies_without_mutation():
    original, durable = _fixtures()
    plan = build_repair_plan(original,durable)
    assert plan["targets"] == 10 and plan["operations_executed"] == 0
    rows={(v["code"],v["side"]):v for v in plan["rows"]}
    assert rows["251970","BUY"]["dry_run_qty_delta"] == -15
    assert rows["086450","BUY"]["dry_run_qty_delta"] == -22
    assert rows["251970","SELL"]["dry_run_qty_delta"] == 10
    assert rows["253590","SELL"]["dry_run_qty_delta"] == 52
    assert rows["144960","BUY"]["dry_run_notional_delta_krw"] == "35"
    assert "SELL_CYCLE_MISMATCH_MANUAL_ATTRIBUTION_REQUIRED" in rows["053800","SELL"]["issues"]


def test_offline_or_unauthenticated_source_is_never_accepted():
    original,durable = _fixtures()
    original["source"] = "OFFLINE_INPUT_UNAUTHENTICATED"
    with pytest.raises(ValueError,match="PR249_AUTHENTICATED_KIS_GET_REQUIRED"):
        build_repair_plan(original,durable)


def test_missing_original_or_missing_db_row_blocks_entire_plan():
    original,durable = _fixtures()
    original["reports"][0]["results"].pop()
    with pytest.raises(ValueError,match="PR249_MISSING_ORIGINAL_KIS_TARGETS"):
        build_repair_plan(original,durable)
    original,durable = _fixtures()
    durable.pop()
    with pytest.raises(ValueError,match="PR249_DB_TARGETS_OR_PORTFOLIO_EPOCH_INCOMPLETE"):
        build_repair_plan(original,durable)


def test_mixed_epoch_and_duplicate_order_block():
    original,durable = _fixtures()
    durable[0]["portfolio_epoch_id"] = "wrong-epoch"
    with pytest.raises(ValueError,match="PORTFOLIO_EPOCH_INCOMPLETE"):
        build_repair_plan(original,durable)
    original,durable = _fixtures()
    durable.append(deepcopy(durable[0]))
    with pytest.raises(ValueError,match="DUPLICATE_OR_SCOPE_MISMATCH"):
        build_repair_plan(original,durable)


def test_original_broker_overfill_cannot_be_laundered_into_a_repair_plan():
    original,durable = _fixtures()
    original["reports"][0]["results"][0]["broker_verified_qty"] = 999
    with pytest.raises(ValueError,match="ORIGINAL_ECONOMICS_INCONSISTENT"):
        build_repair_plan(original,durable)
