"""No KIS, DB mutation or scheduler action: synthetic broker snapshot audit tests."""
from trader.kr.broker_db_position_audit import compare_pb1_db_to_kis


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
