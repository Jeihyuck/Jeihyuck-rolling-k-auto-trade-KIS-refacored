"""Session reporting: PREP must leave RUNNING and CLOSE cannot mask repair debt."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def test_prep_finalizer_explicit_and_idempotent_contract():
    prep = (ROOT/"scripts/wsl/run-kr-prep.sh").read_text()
    init = (ROOT/"scripts/wsl/init-session-log.sh").read_text()
    assert 'nullim_finish_session_log "$rc"' in prep
    assert 'NULLIM_SESSION_FINISHED=1' in init
    assert '[[ "${NULLIM_SESSION_FINISHED:-0}" == "1" ]]' in init


def test_normal_prep_manifest_finish_is_idempotent_without_broker(tmp_path):
    path = tmp_path/"session-manifest.json"
    path.write_text(json.dumps({
        "market":"KR", "sessions":{"prep":{
            "attempts":[{"run_id":"prep-test", "status":"RUNNING",
                         "reason":"STARTED", "exit_code":None, "ended_at":None}],
            "effective_run_id":None,"effective_status":None,
        }}
    }),encoding="utf-8")
    script = (
        'source scripts/wsl/init-session-log.sh\n'
        'export NULLIM_SESSION_MANIFEST="$M" NULLIM_RUN_ID="prep-test"\n'
        'nullim_finish_session_log 0\n'
        'nullim_finish_session_log 0\n'
    )
    env={**os.environ,"M":str(path)}
    result=subprocess.run(
        ["bash","-c",script],cwd=ROOT,env=env,text=True,
        stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=20,
    )
    assert result.returncode==0, result.stderr
    assert result.stdout.count("[NULLIM_RUN][END]")==1
    done=json.loads(path.read_text(encoding="utf-8"))["sessions"]["prep"]
    assert done["effective_status"]=="OK"
    assert done["attempts"][0]["exit_code"]==0
    assert done["attempts"][0]["ended_at"]


def test_close_reconcile_debt_manifest_is_warning_not_false_ok():
    close=(ROOT/"scripts/wsl/run-kr-close.sh").read_text()
    assert 'if [[ "$cross_date_rc" -ne 0 && "$rc" -eq 0 ]]' in close
    assert 'NULLIM_SESSION_FINAL_STATUS=OK_WITH_WARNINGS' in close
    assert 'NULLIM_SESSION_FINAL_REASON=HISTORICAL_SELL_RECONCILIATION_PENDING' in close
    assert 'action=PRESERVE_TRADE_RC_NO_SELL_RETRY' in close
