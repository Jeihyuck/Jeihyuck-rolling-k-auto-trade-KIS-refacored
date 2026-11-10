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
        order_price=45150.0,
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


def test_enforce_without_explicit_approval_never_bypasses_entry_gate(monkeypatch):
    """A stale .env approval=0 must not silently turn ENFORCE into SHADOW."""
    import trader.pb1_engine as pb1
    monkeypatch.setattr(pb1, "now_kst", lambda: CJ_BUY_TIME)
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "0")
    def _must_not_be_queried(**kwargs):
        raise AssertionError("no proof should be queried when timing policy unapproved")
    monkeypatch.setattr(pb1, "evaluate_proof_for_candidate", _must_not_be_queried)
    engine = PB1Engine.__new__(PB1Engine)
    engine.env = "practice"
    engine._now_kst = CJ_BUY_TIME
    candidate = SimpleNamespace(
        code="035760", setup_ok=True,
        features={"entry_style_selected": "ENTRY_PULLBACK", "setup_ok": True,
                  "trigger_ok": False},
    )
    decision = engine._resolve_entry_pre_submit(
        cf=candidate, stage="PB1-AM", order_price=45350.0, order_type="LIMIT",
    )
    assert not decision.ok
    assert decision.reason_codes == ["KR_ENTRY_TIMING_POLICY_NOT_APPROVED"]
    assert decision.blocking_stage == "pre_submit_timing"


def _synthetic_proven_rebound():
    """Synthetic completed same-date minute OHLCV, NOT 2026-11-10 KIS history."""
    start = datetime(2026, 11, 10, 9, 0, tzinfo=KST)
    rows = []
    for minute in range(30):
        at = start + timedelta(minutes=minute)
        rows.append({
            "date": "20261110", "time": at.strftime("%H%M%S"),
            "open": 96, "high": 100, "low": 94,
            "close": 96, "volume": 500,
        })
    post = [
        # Minute, open, high, low, close, volume
        (30, 94.8, 95.6, 94.5, 95.0, 600),
        (31, 94.9, 95.4, 94.0, 95.0, 650),
        (32, 95.0, 95.5, 94.8, 95.3, 650),
        (33, 95.4, 96.2, 95.2, 96.1, 650),
    ]
    for minute, o, h, lo, c, vol in post:
        at = start + timedelta(minutes=minute)
        rows.append({
            "date": "20261110", "time": at.strftime("%H%M%S"),
            "open": o, "high": h, "low": lo, "close": c, "volume": vol,
        })
    return rows


def test_verified_positive_pullback_rebound_roundtrips_from_collector_to_new_buy_gate(tmp_path, monkeypatch):
    """Verify actual producer/consumer file contract, not just WAIT-only paths."""
    from trader.kr import opening_bar_contract as op
    def _test_runtime_path(*parts):
        return tmp_path.joinpath(*parts)
    monkeypatch.setattr(op, "runtime_path", _test_runtime_path)
    now = datetime(2026, 11, 10, 9, 34, 51, tzinfo=KST)
    original_rows = _synthetic_proven_rebound()
    saved = op.save_opening_proof(
        env="practice", account_scope="test-account:no-orders",
        code="035760", now=now, raw_bars=original_rows,
        vwap=None, vwap_source="UNKNOWN",
    )
    assert saved["bars"] == 34
    proof = op.evaluate_proof_for_candidate(
        env="practice", account_scope="test-account:no-orders", code="035760",
        now=now, style="ENTRY_PULLBACK", setup_ok=True, trigger_ok=False,
        pivot=None, order_price=96.1,
    )
    assert proof["state"] == "READY_SHADOW", proof
    assert proof["reason"] == "PULLBACK_REBOUND_CONFIRMED"
    assert proof["max_entry_price"] >= 96.1
    assert op.evaluate_proof_for_candidate(
        env="practice", account_scope="test-account:no-orders", code="035760",
        now=now, style="ENTRY_PULLBACK", setup_ok=True, trigger_ok=False,
        pivot=None, order_price=97.0,
    )["state"] == "WAIT"
    # Source provenance mismatch and missing actual 1m date cannot make
    # synthetic / other-date rows into verified same-day trading evidence.
    bad_rows = [{**row, "date": "20261109"} for row in original_rows]
    saved_bad = op.save_opening_proof(
        env="practice", account_scope="other-account", code="035760",
        now=now, raw_bars=bad_rows, vwap=None, vwap_source="UNKNOWN",
    )
    assert saved_bad["bars"] == 0
    assert op.evaluate_proof_for_candidate(
        env="practice", account_scope="other-account", code="035760",
        now=now, style="ENTRY_PULLBACK", setup_ok=True, trigger_ok=False,
        pivot=None, order_price=96.1,
    )["state"] != "READY_SHADOW"


def test_practice_wsl_legacy_shadow_is_upgraded_but_emergency_off_survives():
    """Read-only shell profile evaluation; neither code path runs a broker."""
    import os
    import subprocess
    root = Path("scripts/wsl/kr-entry-timing-activation.sh")
    script = (
        'source "$1"; kr_pb1_entry_timing_defaults; '
        'printf "RESULT:%s|%s|%s\\n" '
        '"$KR_PB1_ENTRY_TIMING_MODE" "$KR_PB1_ENTRY_TIMING_APPROVED" '
        '"$KR_PB1_OPENING_OBSERVER_ENABLED"'
    )
    base = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "STRATEGY_ENV": "practice", "KIS_ENV": "practice"}
    def _run(overrides):
        env = {**base, **overrides}
        return subprocess.run(
            ["bash", "-c", script, "bash", str(root)],
            env=env, text=True, capture_output=True, check=True,
        )
    assert "RESULT:ENFORCE|1|1" in _run({}).stdout
    old_shadow = _run({"KR_PB1_ENTRY_TIMING_MODE": "SHADOW"})
    assert "legacy_SHADOW_to_ENFORCE=1" in old_shadow.stdout
    assert "RESULT:ENFORCE|1|1" in old_shadow.stdout
    safety_off = _run({"KR_PB1_ENTRY_TIMING_MODE": "OFF"})
    assert "RESULT:OFF|1|0" in safety_off.stdout
    invalid = _run({"KR_PB1_ENTRY_TIMING_MODE": "typo-unrecognized"})
    assert "RESULT:ENFORCE|0|1" in invalid.stdout
    assert "CONFIG_ERROR" in invalid.stderr
    explicit_unapproved = _run({"KR_PB1_ENTRY_TIMING_APPROVED": "0"})
    assert "RESULT:ENFORCE|0|1" in explicit_unapproved.stdout
    real = _run({"STRATEGY_ENV": "real", "KIS_ENV": "real"})
    assert "RESULT:SHADOW|0|0" in real.stdout


def test_native_kis_api_envelope_to_observer_to_ready_proof_contract(tmp_path, monkeypatch):
    """Offline contract replay using native KIS field NAMES, no broker credentials.

    This is NOT a live KIS smoke test. It validates normalized OHLCV delivery
    across the actual KisAPI parser, observer file writer and PB1 proof reader.
    """
    from trader.kr import opening_bar_contract as op
    import scripts.kr_opening_observer as observer
    import trader.kis_wrapper as kw

    monkeypatch.setattr(op, "runtime_path", lambda *parts: tmp_path.joinpath(*parts))
    observed = datetime(2026, 11, 10, 9, 34, 51, tzinfo=KST)
    monkeypatch.setattr(observer, "now_kst", lambda: observed)
    monkeypatch.setattr(observer, "get_account_key", lambda **kw: "offline-kis-api-contract")
    native = _synthetic_proven_rebound()
    called = []
    class NativeResponse:
        status_code = 200
        def __init__(self, rows):
            self.rows = rows
        def raise_for_status(self):
            return None
        def json(self):
            return {"rt_cd": "0", "output2": self.rows}
    def _request(*args, **kwargs):
        hour = kwargs["params"]["fid_input_hour_1"]
        called.append(hour)
        current = int(hour[:4])
        native_window = [bar for bar in native
                         if int(str(bar["time"])[:4]) <= current][-30:]
        return NativeResponse([{
            "stck_bsop_date": bar["date"],
            "stck_cntg_hour": bar["time"],
            "stck_prpr": str(bar["close"]),
            "stck_oprc": str(bar["open"]),
            "stck_hgpr": str(bar["high"]),
            "stck_lwpr": str(bar["low"]),
            "cntg_vol": str(bar["volume"]),
        } for bar in native_window])
    monkeypatch.setattr(kw, "_pick_tr", lambda *args: ["FHKST03010200"])
    client = kw.KisAPI.__new__(kw.KisAPI)
    client.env = "practice"
    client._limiter = SimpleNamespace(wait=lambda *args, **kwargs: None)
    client._headers = lambda *args, **kwargs: {}
    client._safe_request = _request
    client.get_price_snapshot = lambda *args, **kwargs: {
        "raw": {"stck_bsop_date": "20261110",
                "stck_cntg_hour": "093450",
                "wghn_avrg_stck_prc": "95.5"},
    }
    summary = observer.run_once(
        kis=client, codes=["035760"], observed_at=observed,
        sleep_between=0,
    )
    assert called == ["092900", "093451"]
    assert summary["completed"] == 1, summary
    assert summary["unknown"] == 0, summary
    decision = op.evaluate_proof_for_candidate(
        env="practice", account_scope="offline-kis-api-contract", code="035760",
        now=observed, style="ENTRY_PULLBACK", setup_ok=True,
        trigger_ok=False, pivot=None, order_price=96.1,
    )
    assert decision["state"] == "READY_SHADOW", decision
    assert decision["vwap_proven"] is True, decision


def test_observer_poll_period_is_start_to_start_and_below_proof_ttl():
    """Guard against stale proofs caused by scan duration + full interval sleep."""
    source = Path("scripts/kr_opening_observer.py").read_text(encoding="utf-8")
    helper = Path("scripts/wsl/kr-entry-timing-activation.sh").read_text(encoding="utf-8")
    assert "cycle_started = time.monotonic()" in source
    assert "elapsed = time.monotonic() - cycle_started" in source
    assert "float(args.interval_sec)) - elapsed" in source
    assert 'default=120.0' in source
    assert "KR_PB1_OPENING_OBSERVER_INTERVAL_SEC:-120" in helper
    # With 75s elapsed scanning 30 tickers, next cycle starts at T+120,
    # rather than at T+195 where all 180s proofs are guaranteed stale.
    scan_duration_sec, interval_sec, proof_ttl_sec = 75.0, 120.0, 180.0
    subsequent_start = scan_duration_sec + max(1.0, interval_sec - scan_duration_sec)
    assert subsequent_start == 120.0
    assert subsequent_start < proof_ttl_sec
