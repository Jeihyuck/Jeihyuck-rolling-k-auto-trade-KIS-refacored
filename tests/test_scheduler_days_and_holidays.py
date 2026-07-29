import json
import os
import re
import shutil
import subprocess
from pathlib import Path

ROOT=Path(__file__).parents[1]
INSTALLER=ROOT/'scripts/windows/update-nullim-scheduler.ps1'
VERIFIER=ROOT/'scripts/windows/verify-scheduler.ps1'


def test_windows_weekly_day_groups_are_exact():
    text=INSTALLER.read_text()
    assert 'New-ScheduledTaskTrigger -Daily' not in text
    assert 'New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $d.Days' in text
    assert '$krDays=@("Monday","Tuesday","Wednesday","Thursday","Friday")' in text
    assert '$usEveningDays=@("Monday","Tuesday","Wednesday","Thursday","Friday")' in text
    assert '$usMorningDays=@("Tuesday","Wednesday","Thursday","Friday","Saturday")' in text
    assert text.count('Days=$krDays') == 6
    assert text.count('Days=$usEveningDays') == 6
    assert text.count('Days=$usMorningDays') == 4
    assert 'Sunday' not in '\n'.join(line for line in text.splitlines() if 'Days=@' in line)


def test_us_morning_uses_masks_and_requires_saturday():
    text=VERIFIER.read_text()
    assert 'function ConvertTo-NullimDayMask' in text
    assert text.count(',124)') == 4
    assert text.count(',62)') == 12
    assert 'DaysOfWeek=(New-NullimCheck -Ok ($actualDaysMask -eq [int]$e[3])' in text
    contract=(ROOT/'scripts/windows/test-scheduler-trigger-contract.ps1').read_text()
    assert 'New-ScheduledTaskTrigger -Weekly' in contract
    assert "if(($usMask -band 64) -eq 0)" in contract
    assert "if(($usMask -band 2) -ne 0)" in contract


def test_windows_installer_preserves_health_script_arguments():
    text=INSTALLER.read_text()
    assert '[string[]]$ScriptArgs' in text
    assert '[string[]]$Args' not in text
    assert '$scriptArgsQ=@($ScriptArgs' in text
    assert '-ScriptArgs @($d.Args)' in text
    assert 'Args=@("kr")' in text
    assert 'Args=@("us")' in text


def test_windows_verifier_uses_structured_checks_and_accepts_never_run_code():
    text=VERIFIER.read_text()
    assert 'function New-NullimCheck' in text
    assert 'TaskPath=(New-NullimCheck -Ok ($task.TaskPath -eq' in text
    assert '$c.Ok' in text
    assert '$c.Expected' in text
    assert '$c.Actual' in text
    assert '267011 # 0x00041303 SCHED_S_TASK_HAS_NOT_RUN' in text
    assert "runState='LAST_RUN_NOT_YET_EXECUTED'" in text
    assert '@($task.TaskPath -eq' not in text
    assert '@($a -match' not in text


def test_canonical_calendar_exit_contract():
    checker=ROOT/'scripts/wsl/check-nullim-trading-day.py'
    env={**os.environ,'NULLIM_TRADING_DAY_OVERRIDE':'closed','NULLIM_PYTHON_BIN':os.sys.executable}
    closed=subprocess.run(['python',str(checker),'--market','us','--date','2026-12-25'],cwd=ROOT,env=env,text=True,capture_output=True)
    assert closed.returncode == 10
    data=json.loads(closed.stdout); assert data['status']=='SKIPPED_NON_TRADING_DAY' and data['is_trading_day'] is False
    opened=subprocess.run(['python',str(checker),'--market','kr','--date','2026-08-17'],cwd=ROOT,env={**env,'NULLIM_TRADING_DAY_OVERRIDE':'open'},text=True,capture_output=True)
    assert opened.returncode == 0 and json.loads(opened.stdout)['is_trading_day'] is True


def _closed_repo(tmp_path, market):
    root=tmp_path/market; (root/'scripts/wsl').mkdir(parents=True)
    for name in ('send-market-log-mail.sh',f'send-{market}-log-mail.sh','check-nullim-trading-day.py','check-nullim-day-health.sh','resolve-nullim-python.sh'):
        shutil.copy2(ROOT/'scripts/wsl'/name,root/'scripts/wsl'/name)
    return root


def test_closed_day_mail_skips_before_missing_evidence_and_never_builds_all(tmp_path):
    for market in ('us',):
        root=_closed_repo(tmp_path,market); date='2026-12-25'; archive=root/f'nullim-{market}-logs.tar.gz'
        env={**os.environ,'NULLIM_TRADING_DAY_OVERRIDE':'closed','NULLIM_KST_RUN_DATE':date,'US_TRADE_DATE':date,'NULLIM_LOG_MAIL_OUT':str(archive),'NULLIM_PYTHON_BIN':os.sys.executable}
        result=subprocess.run(['bash',str(root/f'scripts/wsl/send-{market}-log-mail.sh')],cwd=root,env=env,text=True,capture_output=True)
        assert result.returncode == 0 and 'SKIPPED_NON_TRADING_DAY' in result.stdout
        marker=json.loads((root/f'runtime/health/{market}-mail-{date}.json').read_text())
        assert marker['ok'] is True and marker['mail_sent'] is False and marker['status']=='SKIPPED_NON_TRADING_DAY'
        assert not archive.exists() and not list(root.glob('nullim-*-all-logs-*.tar.gz'))


def _prepare_closed_health(root):
    verify=root/'scripts/wsl/verify-no-nullim-auto-scheduler.sh'
    verify.write_text("""#!/usr/bin/env bash
if [[ "${FORBIDDEN_TEST:-0}" == 1 ]]; then echo '[SCHEDULER_POLICY][WSL][SUMMARY] forbidden_sources=1'; exit 1; fi
echo '[SCHEDULER_POLICY][WSL][OK] forbidden_sources=0'
"""); verify.chmod(0o755)
    subprocess.run(['git','init','-q'],cwd=root,check=True)
    subprocess.run(['git','config','user.email','fixture@example.test'],cwd=root,check=True); subprocess.run(['git','config','user.name','Fixture'],cwd=root,check=True)
    (root/'tracked').write_text('x'); subprocess.run(['git','add','tracked'],cwd=root,check=True); subprocess.run(['git','commit','-qm','fixture'],cwd=root,check=True)
    sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    health=root/'runtime/health'; health.mkdir(parents=True)
    (health/'windows-scheduler-install.json').write_text(json.dumps({'status':'OK','scheduler_owner':'WINDOWS_TASK_SCHEDULER','installed_commit_sha':sha}))
    return sha


def test_closed_day_health_still_enforces_policy_and_sha(tmp_path):
    root=_closed_repo(tmp_path,'us'); date='2026-08-17'; sha=_prepare_closed_health(root)
    env={**os.environ,'NULLIM_TRADING_DAY_OVERRIDE':'closed','NULLIM_PYTHON_BIN':os.sys.executable}
    script=root/'scripts/wsl/check-nullim-day-health.sh'
    result=subprocess.run(['bash',str(script),'us',date],cwd=root,env=env,text=True,capture_output=True)
    assert result.returncode == 0
    data=json.loads((root/f'runtime/health/us-{date}.json').read_text())
    assert data['status']=='SKIPPED_NON_TRADING_DAY' and data['ok'] is True and data['scheduler_sha_matches'] is True
    marker=root/'runtime/health/windows-scheduler-install.json'
    marker.write_text(json.dumps({'status':'OK','scheduler_owner':'WINDOWS_TASK_SCHEDULER','installed_commit_sha':'drift'}))
    drift=subprocess.run(['bash',str(script),'us',date],cwd=root,env=env,text=True,capture_output=True)
    drift_data=json.loads((root/f'runtime/health/us-{date}.json').read_text())
    assert drift.returncode == 1 and drift_data['failure_reason']=='FAILED_SCHEDULER_DRIFT'
    marker.write_text(json.dumps({'status':'OK','scheduler_owner':'WINDOWS_TASK_SCHEDULER','installed_commit_sha':sha}))
    forbidden=subprocess.run(['bash',str(script),'us',date],cwd=root,env={**env,'FORBIDDEN_TEST':'1'},text=True,capture_output=True)
    forbidden_data=json.loads((root/f'runtime/health/us-{date}.json').read_text())
    assert forbidden.returncode == 1 and forbidden_data['failure_reason']=='SCHEDULER_POLICY_VIOLATION'


def test_wrappers_gate_before_runner_and_snapshot_lock_contract():
    wrappers=('run-kr-prep.sh','run-kr-am.sh','run-kr-afternoon.sh','run-kr-close.sh','run-us-prep.sh','run-us-prep-recovery.sh','check-us-prep-before-am.sh','run-us-am.sh','run-us-afternoon.sh','run-us-close.sh')
    for name in wrappers:
        text=(ROOT/'scripts/wsl'/name).read_text()
        assert text.index('nullim_require_trading_day') < text.index('deploy_preflight')
    init=(ROOT/'scripts/wsl/init-session-log.sh').read_text(); mail=(ROOT/'scripts/wsl/send-market-log-mail.sh').read_text()
    assert 'flock -s "$NULLIM_SNAPSHOT_FD"' in init
    assert 'flock -x "$SNAPSHOT_FD"' in mail


def test_closed_session_wrapper_records_attempt_without_reaching_preflight(tmp_path):
    for market,name,purpose in (("us","run-us-am.sh","am"),):
        root=tmp_path/f"wrapper-{market}"; scripts=root/"scripts/wsl"; scripts.mkdir(parents=True)
        for source in (name,"init-session-log.sh","check-nullim-trading-day.py","resolve-nullim-python.sh"):
            shutil.copy2(ROOT/"scripts/wsl"/source,scripts/source)
        env={**os.environ,"NULLIM_TRADING_DAY_OVERRIDE":"closed","NULLIM_KST_RUN_DATE":"2026-12-25","US_TRADE_DATE":"2026-12-25","NULLIM_PYTHON_BIN":os.sys.executable}
        result=subprocess.run(["bash",str(scripts/name)],cwd=root,env=env,text=True,capture_output=True)
        assert result.returncode == 0
        manifest=json.loads((root/f"runtime/logs/{market}/2026-12-25/session-manifest.json").read_text())
        attempt=manifest["sessions"][purpose]["attempts"][0]
        assert attempt["status"] == "SKIPPED_NON_TRADING_DAY" and attempt["reason"] == "MARKET_CLOSED"
        log=(root/attempt["log_file"]).read_text()
        assert "[NULLIM_RUN][SKIP] reason=SKIPPED_NON_TRADING_DAY" in log
        assert "deploy-preflight" not in log


def test_kr_wrappers_treat_calendar_as_advisory():
    for name in ('run-kr-prep.sh','run-kr-am.sh','run-kr-afternoon.sh','run-kr-close.sh'):
        text=(ROOT/'scripts/wsl'/name).read_text()
        assert '[KR_SESSION][CALENDAR_WARN]' in text
        assert '[[ "$trading_day_rc" == 10 ]] && exit 0' not in text
        assert '[[ "$trading_day_rc" == 0 ]] || exit "$trading_day_rc"' not in text
