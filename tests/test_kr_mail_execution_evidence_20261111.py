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
