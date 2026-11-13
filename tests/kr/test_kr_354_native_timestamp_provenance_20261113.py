"""PR354 broker economic evidence: invalid rows never prove fill timestamps."""
from trader.kr.order_execution_evidence import summarize_native_order


ORDER = {
    "order_id": "signed-buy-id",
    "env": "practice",
    "strategy": "pb1_pullback_close",
    "market": "KOSDAQ",
    "code": "035760",
    "side": "BUY",
    "trading_epoch_id": "epoch",
    "portfolio_epoch_id": "portfolio",
    "position_cycle_id": "cycle",
    "kis_odno": "0000011759",
    "qty": 4,
}


def _fill(*, kind="KIS_ORDER_CUMULATIVE_EXECUTION",
          timestamp_source="ORDER_TIME_PROXY", broker_fill_id="KR_KIS_CUM:11759"):
    return {
        "order_id": ORDER["order_id"],
        "env": ORDER["env"],
        "market": ORDER["market"],
        "code": ORDER["code"],
        "side": ORDER["side"],
        "trading_epoch_id": ORDER["trading_epoch_id"],
        "portfolio_epoch_id": ORDER["portfolio_epoch_id"],
        "position_cycle_id": ORDER["position_cycle_id"],
        "kis_odno": ORDER["kis_odno"],
        "qty": 4,
        "price": 2000,
        "filled_at": "2026-11-13T09:31:11+09:00",
        "broker_fill_id": broker_fill_id,
        "fill_meta_json": {
            "evidence_kind": kind,
            "filled_at_source": timestamp_source,
        },
        "raw_json": {"kis_response": {
            "tot_ccld_qty": "4", "tot_ccld_amt": "8000",
        }},
    }


def test_unproven_ack_timestamp_cannot_upgrade_valid_order_proxy_fill():
    actual = _fill()
    rejected = _fill(
        kind="KIS_BROKER_ACK", timestamp_source="KIS_EXECUTION_TIMESTAMP",
        broker_fill_id="unrelated-ack",
    )
    summary = summarize_native_order(ORDER, [actual, rejected])
    assert summary["proven_filled_qty"] == 4
    assert summary["time_quality"] == "ORDER_TIME_PROXY_OR_UNKNOWN"
    assert summary["evidence_status"] == "ECONOMIC_PROOF_TIMESTAMP_PROXY"
    assert "FILL_NATIVE_EVIDENCE_KIND_UNPROVEN" in summary["issues"]


def test_valid_kis_execution_timestamp_is_counted_for_valid_cumulative():
    actual = _fill(timestamp_source="KIS_EXECUTION_TIMESTAMP")
    summary = summarize_native_order(ORDER, [actual])
    assert summary["proven_filled_qty"] == 4
    assert summary["time_quality"] == "NATIVE_EXECUTION_TIMESTAMP"
    assert summary["evidence_status"] == "ECONOMIC_AND_TIMESTAMP_PROVEN"


def test_invalid_cumulative_amount_does_not_certify_native_timestamp():
    valid = _fill()
    invalid = _fill(
        timestamp_source="KIS_EXECUTION_TIMESTAMP",
        broker_fill_id="KR_KIS_CUM:invalid",
    )
    invalid["raw_json"] = {"kis_response": {
        "tot_ccld_qty": "4", "tot_ccld_amt": "9000",
    }}
    summary = summarize_native_order(ORDER, [valid, invalid])
    assert summary["time_quality"] == "ORDER_TIME_PROXY_OR_UNKNOWN"
    assert "CUMULATIVE_NOTIONAL_CONFLICT_WITH_ORIGINAL_KIS" in summary["issues"]
