"""KR 11/11 CLOSE reporting regression: no order or broker side effects."""
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import json

from trader.kr.close_reporting_20261111 import (
    close_economic_warning, reuse_close_balance_for_reporting,
)

KST = ZoneInfo("Asia/Seoul")


def _make_snapshot(tmp_path: Path, *, checked_at=None, raw=None):
    checked_at = checked_at or datetime(2026, 11, 11, 15, 19, 18, tzinfo=KST)
    raw = raw if raw is not None else {
        "output1": [{"pdno": "028050", "hldg_qty": "33"}],
        "output2": {"tot_evlu_amt": "98855713"},
    }
    p = tmp_path / "balance_precheck.json"
    p.write_text(json.dumps({
        "state": "OK", "source": "KIS", "raw_snapshot_available": True,
        "checked_at_kst": checked_at.isoformat(), "raw_snapshot": raw,
    }), encoding="utf-8")
    return p


def test_11nov_pending_economics_never_reported_as_complete():
    assert close_economic_warning({
        "status": "OK_CLOSE_POLICY",
        "economic_settlement_state": "ECONOMIC_SETTLEMENT_PENDING",
        "economic_settlement_pending": 1,
        "native_priced_sell_gap_count": 2,
        "postsubmit_reconcile_ok": 0,
    }) == "ECONOMIC_SETTLEMENT_PENDING"


def test_zero_order_healthy_economic_state_not_falsely_degraded():
    assert close_economic_warning({
        "economic_settlement_state": "COMPLETE",
        "economic_settlement_pending": 0,
        "native_priced_sell_gap_count": 0,
    }) is None
    assert close_economic_warning({"native_priced_sell_gap_count": "invalid"}) == "ECONOMIC_SETTLEMENT_UNVERIFIED"


def test_recent_same_session_kis_balance_reusable_for_reporting_only(tmp_path):
    p = _make_snapshot(tmp_path)
    now = datetime(2026, 11, 11, 15, 20, 55, tzinfo=KST)
    raw = reuse_close_balance_for_reporting(p, new_orders_submitted=0, now=now)
    assert raw["output1"][0]["hldg_qty"] == "33"
    assert reuse_close_balance_for_reporting(p, new_orders_submitted=1, now=now) is None
    assert reuse_close_balance_for_reporting(p, new_orders_submitted=-1, now=now) is None
    assert reuse_close_balance_for_reporting(p, new_orders_submitted=False, now=now) is None


def test_old_or_future_snapshot_forces_current_broker_refresh(tmp_path):
    p = _make_snapshot(tmp_path)
    now = datetime(2026, 11, 11, 15, 22, 0, tzinfo=KST)
    assert reuse_close_balance_for_reporting(p, new_orders_submitted=0, now=now) is None
    future = _make_snapshot(tmp_path, checked_at=now + timedelta(seconds=5))
    assert reuse_close_balance_for_reporting(future, new_orders_submitted=0, now=now) is None


def test_unproven_or_corrupt_broker_evidence_is_rejected(tmp_path):
    p = _make_snapshot(tmp_path)
    now = datetime(2026, 11, 11, 15, 20, 0, tzinfo=KST)
    for field,value in (("source","CACHE"),("state","FAIL"),("raw_snapshot_available",False)):
        payload=json.loads(p.read_text())
        payload[field]=value
        p.write_text(json.dumps(payload))
        assert reuse_close_balance_for_reporting(p, new_orders_submitted=0, now=now) is None
        p = _make_snapshot(tmp_path)
    p = _make_snapshot(tmp_path, raw={"output1":[{"pdno":"028050","hldg_qty":"bad"}],"output2":{}})
    assert reuse_close_balance_for_reporting(p, new_orders_submitted=0, now=now) is None


def test_runner_applies_economic_warning_separately_from_execution_status():
    source = Path("trader/kr/runner/trade_session_runner.py").read_text()
    assert "close_economic_warning(pb1_result)" in source
    assert '"economic_settlement_state": str(pb1_result.get(' in source
    assert "trade_order_retry=0" in source
    assert "zero_order_proven = bool(" in source
