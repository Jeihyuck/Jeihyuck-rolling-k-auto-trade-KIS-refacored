import json
import os
import subprocess
from pathlib import Path

ROOT=Path(__file__).parents[1]
def test_policy_is_windows_single_owner():
    p=json.loads((ROOT/'config/scheduler_policy.json').read_text())
    assert p['automatic_scheduler_owner']=='windows_task_scheduler'
    assert p['wsl_user_cron_allowed'] is False
    assert p['wsl_systemd_timer_allowed'] is False
    assert p['github_actions_live_schedule_allowed'] is False

def test_cron_cleanup_removes_only_nullim_entries_and_is_idempotent(tmp_path):
    state=tmp_path/'cron'; state.write_text('MAILTO=ops@example\n# NULLIM_CRON_START\n0 9 * * * /x/run_pb1_kr.sh am\n# NULLIM_CRON_END\n5 * * * * /x/scripts/wsl/run-us-am.sh\n')
    fake=tmp_path/'bin'; fake.mkdir(); cr=fake/'crontab'
    cr.write_text('#!/usr/bin/env bash\nif [[ "$1" == "-l" ]]; then cat "$CRON_STATE"; else cat > "$CRON_STATE"; fi\n'); cr.chmod(0o755)
    env={**os.environ,'PATH':f'{fake}:{os.environ["PATH"]}','CRON_STATE':str(state),'NULLIM_CRON_BACKUP_DIR':str(tmp_path/'backup')}
    cmd=['bash',str(ROOT/'scripts/wsl/install-nullim-cron.sh')]
    subprocess.run(cmd, cwd=ROOT, env=env, check=True, text=True, capture_output=True)
    first=state.read_text(); assert first == 'MAILTO=ops@example\n'
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)
    assert state.read_text()==first

def test_manual_dispatcher_uses_canonical_wrappers():
    text=(ROOT/'run_pb1_kr.sh').read_text()
    assert 'python -m trader.pb1_runner' not in text
    for name in ('run-kr-prep.sh','run-kr-am.sh','run-kr-afternoon.sh','run-kr-close.sh'): assert name in text

def test_us_prep_and_recovery_share_prep_lock():
    assert 'LOCK_FILE="runtime/locks/us-${SESSION_NAME}.lock"' in (ROOT/'scripts/wsl/run-us-prep.sh').read_text()
    assert 'exec bash scripts/wsl/run-us-prep.sh' in (ROOT/'scripts/wsl/run-us-prep-recovery.sh').read_text()

def test_order_capable_workflows_have_no_schedule():
    needles=('run-kr-','run-us-','run_pb1_kr.sh','trader.pb1_runner','trade_session_runner','LIVE_TRADING_ENABLED','KR_ORDER_ARMED','US_ORDER_ARMED')
    for path in (ROOT/'.github/workflows').glob('*.*ml'):
        text=path.read_text()
        if any(n in text for n in needles): assert 'schedule:' not in text, path

def test_docs_and_windows_installer_declare_single_owner():
    assert 'sole automatic owner' in (ROOT/'docs/WSL_KR_US_SCHEDULE_RUNBOOK.md').read_text()
    installer=(ROOT/'scripts/windows/update-nullim-scheduler.ps1').read_text()
    assert 'install-nullim-cron.sh' in installer and 'verify-scheduler.ps1' in installer

def test_kr_duplicate_evidence_never_overwrites_canonical_result():
    text=(ROOT/'scripts/wsl/kr-session-lock.sh').read_text()
    assert '/duplicates/duplicate-' in text
    assert 'Path(sys.argv[2]).write_text' in text
    assert 'duplicates/duplicate-' in text


def test_cleanup_refuses_unbalanced_markers(tmp_path):
    state=tmp_path/'cron'; state.write_text('safe cron\n# NULLIM_CRON_START\nunsafe\n')
    fake=tmp_path/'bin'; fake.mkdir(); cr=fake/'crontab'
    cr.write_text('#!/usr/bin/env bash\nif [[ "$1" == "-l" ]]; then cat "$CRON_STATE"; else cat > "$CRON_STATE"; fi\n'); cr.chmod(0o755)
    env={**os.environ,'PATH':f'{fake}:{os.environ["PATH"]}','CRON_STATE':str(state),'NULLIM_CRON_BACKUP_DIR':str(tmp_path/'backup')}
    result=subprocess.run(['bash',str(ROOT/'scripts/wsl/install-nullim-cron.sh')],cwd=ROOT,env=env,text=True,capture_output=True)
    assert result.returncode == 1 and state.read_text().startswith('safe cron')

def test_us_health_uses_kst_overnight_window_not_day_substring():
    text=(ROOT/'scripts/wsl/check-nullim-day-health.sh').read_text()
    assert "timedelta(hours=12, minutes=30)" in text
    assert "start <= stamp <= end" in text
    assert "day in line" not in text


def test_scheduler_ci_installs_pytest_and_separates_steps():
    text=(ROOT/'.github/workflows/scheduler-policy.yml').read_text()
    assert 'Install test dependencies' in text and 'pip install --upgrade pip pytest' in text
    assert 'Validate shell syntax' in text and 'Run scheduler policy regression tests' in text

def test_windows_legacy_detection_requires_repo_and_runner_markers():
    for path in (ROOT/'scripts/windows/update-nullim-scheduler.ps1', ROOT/'scripts/windows/verify-scheduler.ps1'):
        text=path.read_text()
        assert "$repoMarker" in text and "$runnerMarker" in text and "$isNullimTask" in text
        assert "Jeihyuck-rolling-k-auto-trade-KIS-refacored" in text
        assert "-and $action -match $runnerMarker" in text or "-and $a -match $runnerMarker" in text
