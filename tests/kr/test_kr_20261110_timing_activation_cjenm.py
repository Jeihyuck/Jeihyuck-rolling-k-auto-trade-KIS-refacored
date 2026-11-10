"""PR340: KR practice default-on timing activation and CJ ENM conditional replay.

The CJ minute candles below are SYNTHETIC counterfactuals. The original KIS
completed 1m bars for 2026-11-10 are not available in this offline test.
Never claim a historical trade would have been prevented without native bars.
"""
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from trader.kr import opening_bar_contract
from trader.pb1_engine import PB1Engine

KST = ZoneInfo("Asia/Seoul")
CJ_BUY_TIME = datetime(2026, 11, 10, 9, 34, 51, tzinfo=KST)


def test_kr_wsl_am_and_pm_enable_both_timing_shadows_and_readonly_collector():
    helper = Path("scripts/wsl/kr-entry-timing-activation.sh").read_text()
    assert 'KR_PB1_ENTRY_TIMING_MODE:-ENFORCE' in helper
    assert 'KR_PB1_ENTRY_TIMING_APPROVED:-1' in helper
    assert 'KR_PB1_ENTRY_TIMING_SHADOW_ENABLED:-1' in helper
    assert 'KR_PB1_OPENING_OBSERVER_ENABLED:-1' in helper
    assert 'STRATEGY_ENV:-' in helper and 'KIS_ENV:-' in helper
    assert "KR_PB1_OPENING_OBSERVER_ENABLED=0" in helper
    assert 'exec flock -n' in helper
    assert 'NULLIM_SESSION_LOCK_FD' in helper
    assert 'python -m scripts.kr_opening_observer --loop' in helper
    for name in ("run-kr-am.sh", "run-kr-afternoon.sh"):
        body = Path("scripts/wsl", name).read_text()
        assert 'source scripts/wsl/kr-entry-timing-activation.sh' in body
        assert 'kr_pb1_entry_timing_defaults' in body
        assert 'kr_pb1_opening_observer_start "$LOG_DIR" "$TODAY_KST"' in body
    assert "kr_pb1_entry_timing_defaults" not in Path("scripts/wsl/run-kr-close.sh").read_text()


def test_cj_original_order_without_verified_completed_minute_proof_is_blocked_under_enforce(monkeypatch):
    """Known: 44@45350 limit, 09:34:51, ENTRY_PULLBACK_OVERRIDE, no native 1m audit."""
    import trader.pb1_engine as pb1
    monkeypatch.setattr(pb1, "now_kst", lambda: CJ_BUY_TIME)
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    observed = []
    def _missing(**kwargs):
        observed.append(kwargs)
        return {"state": "UNKNOWN", "reason": "NO_FRESH_VERIFIED_KIS_MINUTE_BAR_PROOF"}
    monkeypatch.setattr(pb1, "evaluate_proof_for_candidate", _missing)
    engine = PB1Engine.__new__(PB1Engine)
    engine.env = "practice"
    engine._now_kst = CJ_BUY_TIME
    candidate = SimpleNamespace(
        code="035760", setup_ok=True,
        features={"entry_style_selected": "ENTRY_PULLBACK", "setup_ok": True,
                  "trigger_ok": False, "entry_decision_family": "ENTRY_PULLBACK_OVERRIDE"},
    )
    decision = engine._resolve_entry_pre_submit(
        cf=candidate, stage="PB1-AM", order_price=45350.0, order_type="LIMIT",
    )
    assert observed and observed[0]["style"] == "ENTRY_PULLBACK"
    assert decision.ok is False
    assert decision.blocking_stage == "pre_submit_timing"
    assert decision.context["timing_reason"] == "NO_FRESH_VERIFIED_KIS_MINUTE_BAR_PROOF"
    assert decision.reason_codes == ["KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH"]


def _synthetic_cj_decline():
    """Construct a falling 1m sequence; not KIS historical 2026-11-10 data."""
    start = datetime(2026, 11, 10, 9, 0, tzinfo=KST)
    rows = []
    for minute in range(36):
        at = start + timedelta(minutes=minute)
        px = 46200.0 - minute * 31.4
        rows.append({
            "date": "20261110", "time": at.strftime("%H%M%S"),
            "open": px + 5, "high": px + 15,
            "low": px - 15, "close": px, "volume": 1000,
        })
    return rows


def test_cj_declining_minute_bar_hypothesis_waits_not_buy():
    now = datetime(2026, 11, 10, 9, 35, tzinfo=KST)
    proof = opening_bar_contract.evaluate_price_aware_entry(
        style="ENTRY_PULLBACK", rows=_synthetic_cj_decline(), now=now,
        vwap=None, vwap_verified=False, setup_ok=True, trigger_ok=False,
        order_price=45100.0,
    )
    assert proof["state"] == "WAIT"
    assert proof["reason"] in {
        "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR",
        "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED",
    }


def test_kis_observer_uses_opening_and_current_anchors_without_making_up_dates(monkeypatch):
    import scripts.kr_opening_observer as obs
    called = []
    bars = _synthetic_cj_decline()
    monkeypatch.setattr(obs, "get_account_key", lambda **kwargs: "test-only")
    monkeypatch.setattr(obs, "now_kst", lambda: datetime(2026, 11, 10, 9, 35, tzinfo=KST))
    def _save(**kwargs):
        captured = kwargs["raw_bars"]
        assert len(captured) == 60
        assert all(row.get("date") == "20261110" for row in captured)
        return {"bars": 35, "path": "/tmp/test-observer", "sha256": "offline-only"}
    monkeypatch.setattr(obs, "save_opening_proof", _save)
    class ReadOnlyKis:
        def get_intraday_candles_today(self, code, start_hhmm="090000"):
            assert code == "035760"
            called.append(start_hhmm)
            return bars[:30] if start_hhmm == "092900" else bars[6:]
        def get_price_snapshot(self, code, market="J"):
            return {}
        def buy_stock(self, *args, **kwargs):
            raise AssertionError("observer must never buy")
        def sell_stock(self, *args, **kwargs):
            raise AssertionError("observer must never sell")
    result = obs.run_once(
        kis=ReadOnlyKis(), codes=["035760"],
        observed_at=datetime(2026, 11, 10, 9, 35, tzinfo=KST), sleep_between=0,
    )
    assert called == ["092900", "093500"]
    assert result["completed"] == 1


def test_real_prep_canonical_array_payload_and_contract_are_accepted(tmp_path):
    """PREP mirrors may contain arrays, not an embedded trade_date JSON dict."""
    import json
    from scripts.kr_opening_observer import final30_codes
    f = tmp_path / "final30_scored.json"
    rows = [{"code": f"{i:06d}", "rank_final30": i} for i in range(1, 31)]
    contract = {
        "market": "KR", "env": "practice", "trade_date": "2026-11-11",
        "as_of": "2026-11-10", "final30_rows": 30,
        "contract_ok": True, "trade_can_proceed": True,
    }
    (tmp_path / "prep_contract.json").write_text(
        json.dumps(contract), encoding="utf-8",
    )
    f.write_text(json.dumps(rows), encoding="utf-8")
    codes = final30_codes(trade_date="2026-11-11", filename=str(f))
    assert codes == [f"{i:06d}" for i in range(1, 31)]
    f.write_text(json.dumps({"as_of": "2026-11-10", "items": rows}), encoding="utf-8")
    assert final30_codes(trade_date="2026-11-11", filename=str(f)) == codes


def test_observer_rejects_stale_final30_contract_and_payload_mismatch(tmp_path):
    import json
    from scripts.kr_opening_observer import final30_codes
    f = tmp_path / "final30_scored.json"
    rows = [{"code": f"{i:06d}"} for i in range(1, 31)]
    contract = {
        "market": "KR", "env": "practice", "trade_date": "2026-11-10",
        "as_of": "2026-11-09", "final30_rows": 30,
        "contract_ok": True, "trade_can_proceed": True,
    }
    (tmp_path / "prep_contract.json").write_text(json.dumps(contract))
    f.write_text(json.dumps(rows))
    with pytest.raises(RuntimeError, match="SCOPE_MISMATCH"):
        final30_codes(trade_date="2026-11-11", filename=str(f))
    contract["trade_date"] = "2026-11-11"
    (tmp_path / "prep_contract.json").write_text(json.dumps(contract))
    f.write_text(json.dumps({"trade_date": "2026-11-10", "items": rows}))
    with pytest.raises(RuntimeError, match="SCOPE_MISMATCH"):
        final30_codes(trade_date="2026-11-11", filename=str(f))
