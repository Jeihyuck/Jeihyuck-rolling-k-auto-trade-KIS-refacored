"""No KIS, DB mutation or scheduler action: synthetic broker snapshot audit tests."""
from trader.kr.broker_db_position_audit import compare_pb1_db_to_kis, summarize_pb1_execution_claim_backlog


def _broker(*holdings):
    return {"rt_cd": "0", "output1": list(holdings)}


def test_nov03_mico_db_113_vs_verified_kis_91_is_red():
    result = compare_pb1_db_to_kis(
        positions=[{"code": "059090", "status": "OPEN", "qty": 113}],
        broker_snapshot=_broker({"pdno": "059090", "hldg_qty": "91"}),
        source="api",
    )
    assert not result["verified_match"]
    assert result["status"] == "MISMATCH"
    assert result["mismatches"] == [
        {"code": "059090", "reason": "DB_KIS_QTY_MISMATCH",
         "db_qty": 113, "kis_qty": 91}
    ]


def test_neosem_db_110_and_broker_zero_remain_mismatch_without_auto_close():
    result = compare_pb1_db_to_kis(
        positions=[{"code": "253590", "status": "OPEN", "qty": 110}],
        broker_snapshot=_broker({"pdno": "059090", "hldg_qty": "91"}),
        source="api",
    )
    assert result["mismatches"][0]["kis_qty"] == 0
    assert result["mismatches"][0]["reason"] == "DB_KIS_QTY_MISMATCH"


def test_unverified_or_partial_broker_snapshot_never_produces_match():
    for snapshot, source in (
        (None, "api"),
        (_broker(), "wrapper_cache"),
        ({"rt_cd": "1", "output1": []}, "api"),
        ({"rt_cd": "0", "output1": [], "_diag_stub": True}, "api"),
        ({"rt_cd": "0"}, "api"),
        (_broker({"pdno": "059090", "hldg_qty": "91"},
                 {"pdno": "059090", "hldg_qty": "91"}), "api"),
    ):
        result = compare_pb1_db_to_kis(
            positions=[{"code": "059090", "status": "OPEN", "qty": 91}],
            broker_snapshot=snapshot, source=source,
        )
        assert result["verified_match"] is False


def test_broker_only_is_unattributed_not_spuriously_pb1_owned():
    result = compare_pb1_db_to_kis(
        positions=[{"code": "059090", "status": "OPEN", "qty": 91}],
        broker_snapshot=_broker({"pdno": "059090", "hldg_qty": "91"},
                                {"pdno": "088980", "hldg_qty": "15"}),
        source="api",
    )
    assert result["verified_match"] is True
    assert result["broker_only_symbols_unattributed"] == ["088980"]


def test_duplicate_open_pb1_lifecycles_is_red():
    result = compare_pb1_db_to_kis(
        positions=[{"code": "059090", "status": "OPEN", "qty": 30},
                   {"code": "059090", "status": "OPEN", "qty": 61}],
        broker_snapshot=_broker({"pdno": "059090", "hldg_qty": "91"}),
        source="api",
    )
    assert result["mismatches"][0]["reason"] == "MULTIPLE_OPEN_LIFECYCLES"


def test_broker_economics_must_be_proven_for_91_share_position():
    matching = compare_pb1_db_to_kis(
        positions=[{"code": "059090", "status": "OPEN", "qty": 91,
                    "total_cost": 1982400}],
        broker_snapshot=_broker({"pdno": "059090", "hldg_qty": "91",
                                 "pchs_amt": "1982400"}),
        source="api", require_economics=True,
    )
    assert matching["verified_match"] is True
    wrong_cost = compare_pb1_db_to_kis(
        positions=[{"code": "059090", "status": "OPEN", "qty": 91,
                    "total_cost": 1900000}],
        broker_snapshot=_broker({"pdno": "059090", "hldg_qty": "91",
                                 "pchs_amt": "1982400"}),
        source="api", require_economics=True,
    )
    assert wrong_cost["verified_match"] is False
    assert wrong_cost["mismatches"][0]["reason"] == "DB_KIS_COST_MISMATCH"
    missing_proof = compare_pb1_db_to_kis(
        positions=[{"code": "059090", "status": "OPEN", "qty": 91,
                    "total_cost": 1982400}],
        broker_snapshot=_broker({"pdno": "059090", "hldg_qty": "91"}),
        source="api", require_economics=True,
    )
    assert missing_proof["verified_match"] is False
    assert missing_proof["mismatches"][0]["reason"] == "COST_PROOF_UNAVAILABLE"


def test_old_pb1_claim_backlog_is_not_silently_marked_settled():
    """Nov 3: 5 historical claims still need exact original broker proof."""
    cases = [
        ("293490", "UNCERTAIN", 174, 0, 174),
        ("012750", "PARTIALLY_SATISFIED", 11, 2, 9),
        ("086450", "PARTIALLY_SATISFIED", 64, 48, 16),
        ("192820", "OPEN", 4, 0, 4),
        ("086450-pm", "OPEN", 66, 0, 66),
    ]
    rows = [{
        "action_key": code, "trade_date": "2026-10-27",
        "action": "BUY_ENTRY:PB1-AM", "action_state": state,
        "target_qty": target, "cumulative_filled_qty": filled,
        "remaining_target_qty": remaining,
    } for code, state, target, filled, remaining in cases]
    rows += [{
        "action_key": "neosem-recovered", "trade_date": "2026-10-30",
        "action": "SELL:HARD_STOP:FULL_EXIT", "action_state": "SATISFIED",
        "target_qty": 110, "cumulative_filled_qty": 110,
        "remaining_target_qty": 0,
    }]
    result = summarize_pb1_execution_claim_backlog(rows)
    assert result["unresolved_count"] == 5
    assert len(result["execution_claims"]) == 5
    assert {x["reason"] for x in result["execution_claims"]} == {
        "BROKER_SUBMIT_OUTCOME_UNRESOLVED",
        "BROKER_PARTIAL_EXECUTION_PENDING",
        "CLAIM_NEEDS_ORIGINAL_ORDER_PROOF",
    }


def test_inconsistent_claim_quantity_is_explicitly_flagged():
    rows = [{
        "action_key": "bad", "trade_date": "2026-11-03",
        "action": "SELL:TP1", "action_state": "PARTIALLY_SATISFIED",
        "target_qty": 100, "cumulative_filled_qty": 70,
        "remaining_target_qty": 40,
    }]
    result = summarize_pb1_execution_claim_backlog(rows)
    assert result["unresolved_count"] == 1
    assert result["execution_claims"][0]["reason"] == "CLAIM_QUANTITY_INCONSISTENT"
