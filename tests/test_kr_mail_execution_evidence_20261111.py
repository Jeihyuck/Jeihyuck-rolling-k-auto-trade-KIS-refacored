"""11/11 Korea archive: degraded delivery must expose failed execution and settlement."""
import importlib.util
import json
from pathlib import Path


def _helper():
    path = Path(__file__).resolve().parents[1] / "scripts" / "wsl" / "kr_mail_execution_evidence.py"
    spec = importlib.util.spec_from_file_location("kr_mail_execution_evidence", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write(stage: Path, relative: str, data):
    file = stage / relative
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(json.dumps(data) if isinstance(data, dict) else data, encoding="utf-8")


def _manifest(prep=True):
    sessions = {}
    for purpose in ("prep", "am", "afternoon", "close"):
        if not prep and purpose == "prep":
            continue
        run_id = purpose + "-1"
        failed = purpose in {"am", "afternoon"}
        sessions[purpose] = {
            "effective_run_id": run_id,
            "attempts": [{
                "run_id": run_id, "status": "FAILED" if failed else "OK",
                "reason": "EXIT_CODE_1" if failed else "SESSION_END",
                "log_file": f"runtime/logs/kr/2026-11-11/{purpose}/{run_id}.log",
            }],
        }
    return {"sessions": sessions}


def test_11nov_archive_exposes_exact_failed_sessions_and_close_unsettled(tmp_path):
    _write(tmp_path, "runtime/logs/kr/2026-11-11/session-manifest.json", _manifest(prep=False))
    for purpose in ("am", "afternoon"):
        _write(
            tmp_path,
            f"runtime/logs/kr/2026-11-11/{purpose}/{purpose}-1.log",
            f"[KR_SESSION][PRECHECK_FAIL] session={purpose} reason=TRADE_DATE_MISMATCH detail=old_contract\\n",
        )
    _write(tmp_path, "runtime/kr/session/2026-11-11/close/pb1_result.json", {
        "status": "OK_CLOSE_POLICY",
        "economic_settlement_state": "ECONOMIC_SETTLEMENT_PENDING",
        "native_priced_sell_gap_count": 2,
        "broker_submitted": 0,
    })
    result = _helper().collect_kr_execution_status(tmp_path, "2026-11-11")
    assert result["daily_execution_status"] == "FAILED"
    assert result["execution_sessions"]["prep"]["status"] == "MISSING"
    assert result["execution_sessions"]["am"]["reason"] == "TRADE_DATE_MISMATCH"
    assert result["execution_sessions"]["afternoon"]["reason"] == "TRADE_DATE_MISMATCH"
    assert result["execution_sessions"]["close"]["status"] == "OK"
    assert result["close_economic_settlement_state"] == "ECONOMIC_SETTLEMENT_PENDING"
    assert "CLOSE_ORDERS_SUBMITTED=0" in result["execution_summary"]


def test_no_global_zero_orders_inferred_from_close_only(tmp_path):
    _write(tmp_path, "runtime/logs/kr/2026-11-11/session-manifest.json", _manifest(prep=False))
    _write(tmp_path, "runtime/kr/session/2026-11-11/close/pb1_result.json", {"broker_submitted":0})
    result = _helper().collect_kr_execution_status(tmp_path, "2026-11-11")
    assert "TODAY_ORDERS_SUBMITTED=0" not in result["execution_summary"]
    assert result["daily_execution_status"] == "FAILED"


def test_all_sessions_healthy_and_settlement_complete(tmp_path):
    m = _manifest(prep=True)
    for key in m["sessions"]:
        m["sessions"][key]["attempts"][0].update(status="OK",reason="SESSION_END")
    _write(tmp_path, "runtime/logs/kr/2026-11-11/session-manifest.json", m)
    _write(tmp_path, "runtime/kr/session/2026-11-11/close/pb1_result.json", {
        "economic_settlement_state": "COMPLETE", "native_priced_sell_gap_count": 0,
    })
    result = _helper().collect_kr_execution_status(tmp_path, "2026-11-11")
    assert result["daily_execution_status"] == "COMPLETED"


def test_missing_manifest_never_claims_success(tmp_path):
    assert _helper().collect_kr_execution_status(tmp_path, "2026-11-11")["daily_execution_status"] == "DEGRADED"


def test_mail_shell_uses_truthful_kr_execution_and_latency_markers():
    text = (Path("scripts/wsl/send-market-log-mail.sh")).read_text()
    assert "collect_kr_execution_status" in text
    assert "MAIL_SCRIPT_STARTED_AT_KST" in text
    assert "[LOG_MAIL][SCHEDULE_LAG]" in text
    assert "[LOG_MAIL][SMTP_ACCEPTED]" in text
    assert 'if [[ "$DAILY_EXECUTION_STATUS" == FAILED ]]' in text


def test_nov16_close_reports_samsung_today_settled_separately_from_kodex_history(tmp_path):
    """Today an already-priced Samsung TP3 is not the Oct27 KODEX gap."""
    m = _manifest(prep=True)
    for row in m["sessions"].values():
        row["attempts"][0].update(status="OK", reason="SESSION_END")
    _write(tmp_path, "runtime/logs/kr/2026-11-16/session-manifest.json", m)
    _write(tmp_path, "runtime/kr/session/2026-11-16/close/pb1_result.json", {
        "economic_settlement_state": "ECONOMIC_SETTLEMENT_PENDING",
        "economic_settlement_pending": 1,
        "today_economic_settlement_state": "TODAY_ECONOMIC_SETTLED",
        "historical_economic_settlement_state": "HISTORICAL_ECONOMIC_PENDING",
        "native_priced_sell_gap_count": 2,
        "today_priced_sell_gap_count": 0,
        "historical_priced_sell_gap_count": 2,
        "broker_submitted": 1,
    })
    result = _helper().collect_kr_execution_status(tmp_path, "2026-11-16")
    assert result["daily_execution_status"] == "DEGRADED"
    assert result["close_today_economic_settlement_state"] == "TODAY_ECONOMIC_SETTLED"
    assert result["close_historical_economic_settlement_state"] == "HISTORICAL_ECONOMIC_PENDING"
    assert result["close_today_priced_sell_gap_count"] == 0
    assert result["close_historical_priced_sell_gap_count"] == 2
    assert "TODAY_SETTLEMENT=TODAY_ECONOMIC_SETTLED" in result["execution_summary"]
    assert "HISTORICAL_SETTLEMENT=HISTORICAL_ECONOMIC_PENDING" in result["execution_summary"]


def test_watchdog_alerts_reported_for_exact_current_session_only(tmp_path):
    trade = "2026-11-11"
    m = _manifest()
    for row in m["sessions"].values():
        row["attempts"][0].update(status="OK",reason="SESSION_END")
    _write(tmp_path, f"runtime/logs/kr/{trade}/session-manifest.json", m)
    _write(tmp_path, f"runtime/kr/session/{trade}/close/pb1_result.json",
           {"economic_settlement_state":"COMPLETE","native_priced_sell_gap_count":0})
    warning = {"date":trade,"session":"am","run_id":"am-1",
               "alerts":["TICK_COMPLETION_STALE"]}
    other = dict(warning, run_id="older", alerts=["SESSION_PARENT_EXITED_WITHOUT_TERMINAL"])
    _write(tmp_path, f"runtime/logs/kr/{trade}/am/am-1.watchdog.log",
           "\n".join("[KR][SESSION_WATCHDOG] "+json.dumps(x) for x in [warning,other]))
    report = _helper().collect_kr_execution_status(tmp_path, trade)
    assert report["daily_execution_status"] == "COMPLETED"
    assert report["watchdog_alerts"] == {"am":["TICK_COMPLETION_STALE"]}
    assert "AM_WATCHDOG=TICK_COMPLETION_STALE" in report["execution_summary"]


def test_effective_success_does_not_hide_later_failed_attempt(tmp_path):
    trade = "2026-11-11"
    m = _manifest()
    for row in m["sessions"].values():
        row["attempts"][0].update(status="OK", reason="SESSION_END")
    m["sessions"]["am"]["attempts"].append(
        {"run_id": "am-2", "status": "FAILED", "reason": "EXIT_CODE_1"})
    _write(tmp_path, f"runtime/logs/kr/{trade}/session-manifest.json", m)
    _write(tmp_path, f"runtime/kr/session/{trade}/close/pb1_result.json",
           {"economic_settlement_state":"COMPLETE","native_priced_sell_gap_count":0})
    report = _helper().collect_kr_execution_status(tmp_path, trade)
    assert report["execution_sessions"]["am"]["status"] == "OK"
    assert report["latest_recorded_attempts"]["am"]["status"] == "FAILED"
    assert report["daily_execution_status"] == "FAILED"
    assert "AM_LATEST_ATTEMPT=FAILED:EXIT_CODE_1" in report["execution_summary"]
    m["sessions"]["am"]["attempts"][-1].update(status="RUNNING")
    _write(tmp_path, f"runtime/logs/kr/{trade}/session-manifest.json", m)
    assert _helper().collect_kr_execution_status(tmp_path, trade)["daily_execution_status"] == "DEGRADED"
    m["sessions"]["am"]["attempts"][-1].update(status="SKIP_DUPLICATE")
    _write(tmp_path, f"runtime/logs/kr/{trade}/session-manifest.json", m)
    assert _helper().collect_kr_execution_status(tmp_path, trade)["daily_execution_status"] == "COMPLETED"


def test_pre_submit_latency_is_measured_but_not_hot_cadence_proof():
    def event(code, stamp, ms):
        return (f"[KR][ENTRY][DECISION_LATENCY] code={code} style=PULLBACK "
                f"decision_at={stamp} latest_bar_ts=None eval_elapsed_ms={ms} state=WAIT reason=WAIT")
    a = event("293490", "2026-11-25T09:10:00+09:00", 10)
    b = event("293490", "2026-11-25T09:11:10+09:00", 30)
    c = event("059090", "2026-11-25T09:10:05+09:00", 20)
    bad = event("059090", "2026-11-24T09:10:05+09:00", 1)
    nan = event("059090", "2026-11-25T09:10:06+09:00", "nan")
    report = _helper()._entry_timing_observations([a,b,c,a,bad,nan], "2026-11-25")
    assert report["evaluation_elapsed_ms"] == {"samples":3, "p50":20, "p95":30, "max":30}
    assert report["same_symbol_observed_gap_sec"]["max"] == 70
    assert report["invalid_or_duplicate_samples"] == 3
    assert report["hot_60s_acceptance"] == "NOT_PROVEN"
    assert _helper()._entry_timing_observations([], "2026-11-25")["evaluation_elapsed_ms"]["p95"] is None
