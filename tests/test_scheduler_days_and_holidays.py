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


def test_us_morning_keeps_saturday_and_excludes_monday_sunday():
    text=VERIFIER.read_text()
    morning='Friday,Saturday,Thursday,Tuesday,Wednesday'
    assert text.count(morning) == 4
    assert 'DaysOfWeek=@(' in text and 'WeeksInterval=@(' in text
    for task in ('PB1 US Afternoon WSL','PB1 US Close WSL','PB1 US Mail WSL','PB1 US Health WSL'):
        match=re.search(rf'"{re.escape(task)}"=@\([^\n]+',text)
        assert match and morning in match.group(0)


def test_canonical_calendar_exit_contract():
    checker=ROOT/'scripts/wsl/check-nullim-trading-day.py'
    env={**os.environ,'NULLIM_TRADING_DAY_OVERRIDE':'closed'}
    closed=subprocess.run(['python',str(checker),'--market','us','--date','2026-12-25'],cwd=ROOT,env=env,text=True,capture_output=True)
    assert closed.returncode == 10
    data=json.loads(closed.stdout); assert data['status']=='SKIPPED_NON_TRADING_DAY' and data['is_trading_day'] is False
    opened=subprocess.run(['python',str(checker),'--market','kr','--date','2026-08-17'],cwd=ROOT,env={**env,'NULLIM_TRADING_DAY_OVERRIDE':'open'},text=True,capture_output=True)
    assert opened.returncode == 0 and json.loads(opened.stdout)['is_trading_day'] is True


def _closed_repo(tmp_path, market):
    root=tmp_path/market; (root/'scripts/wsl').mkdir(parents=True)
    for name in ('send-market-log-mail.sh',f'send-{market}-log-mail.sh','check-nullim-trading-day.py','check-nullim-day-health.sh'):
        shutil.copy2(ROOT/'scripts/wsl'/name,root/'scripts/wsl'/name)
    return root


def test_closed_day_mail_skips_before_missing_evidence_and_never_builds_all(tmp_path):
    for market in ('kr','us'):
        root=_closed_repo(tmp_path,market); date='2026-12-25'; archive=root/f'nullim-{market}-logs.tar.gz'
        env={**os.environ,'NULLIM_TRADING_DAY_OVERRIDE':'closed','NULLIM_KST_RUN_DATE':date,'US_TRADE_DATE':date,'NULLIM_LOG_MAIL_OUT':str(archive)}
        result=subprocess.run(['bash',str(root/f'scripts/wsl/send-{market}-log-mail.sh')],cwd=root,env=env,text=True,capture_output=True)
        assert result.returncode == 0 and 'SKIPPED_NON_TRADING_DAY' in result.stdout
        marker=json.loads((root/f'runtime/health/{market}-mail-{date}.json').read_text())
        assert marker['ok'] is True and marker['mail_sent'] is False and marker['status']=='SKIPPED_NON_TRADING_DAY'
        assert not archive.exists() and not list(root.glob('nullim-*-all-logs-*.tar.gz'))


def test_closed_day_health_is_ok_without_mail_ticks_or_scheduler_marker(tmp_path):
    root=_closed_repo(tmp_path,'kr'); date='2026-08-17'
    result=subprocess.run(['bash',str(root/'scripts/wsl/check-nullim-day-health.sh'),'kr',date],cwd=root,env={**os.environ,'NULLIM_TRADING_DAY_OVERRIDE':'closed'},text=True,capture_output=True)
    assert result.returncode == 0
    data=json.loads((root/f'runtime/health/kr-{date}.json').read_text())
    assert data['status']=='SKIPPED_NON_TRADING_DAY' and data['ok'] is True and data['mail_required'] is False and data['mail_ok'] is True


def test_wrappers_gate_before_runner_and_snapshot_lock_contract():
    wrappers=('run-kr-prep.sh','run-kr-am.sh','run-kr-afternoon.sh','run-kr-close.sh','run-us-prep.sh','run-us-prep-recovery.sh','check-us-prep-before-am.sh','run-us-am.sh','run-us-afternoon.sh','run-us-close.sh')
    for name in wrappers:
        text=(ROOT/'scripts/wsl'/name).read_text()
        assert text.index('nullim_require_trading_day') < text.index('deploy_preflight')
    init=(ROOT/'scripts/wsl/init-session-log.sh').read_text(); mail=(ROOT/'scripts/wsl/send-market-log-mail.sh').read_text()
    assert 'flock -s "$NULLIM_SNAPSHOT_FD"' in init
    assert 'flock -x "$SNAPSHOT_FD"' in mail


def test_closed_session_wrapper_records_attempt_without_reaching_preflight(tmp_path):
    for market,name,purpose in (("kr","run-kr-am.sh","am"),("us","run-us-am.sh","am")):
        root=tmp_path/f"wrapper-{market}"; scripts=root/"scripts/wsl"; scripts.mkdir(parents=True)
        for source in (name,"init-session-log.sh","check-nullim-trading-day.py"):
            shutil.copy2(ROOT/"scripts/wsl"/source,scripts/source)
        env={**os.environ,"NULLIM_TRADING_DAY_OVERRIDE":"closed","NULLIM_KST_RUN_DATE":"2026-12-25","US_TRADE_DATE":"2026-12-25"}
        result=subprocess.run(["bash",str(scripts/name)],cwd=root,env=env,text=True,capture_output=True)
        assert result.returncode == 0
        manifest=json.loads((root/f"runtime/logs/{market}/2026-12-25/session-manifest.json").read_text())
        attempt=manifest["sessions"][purpose]["attempts"][0]
        assert attempt["status"] == "SKIPPED_NON_TRADING_DAY" and attempt["reason"] == "MARKET_CLOSED"
        log=(root/attempt["log_file"]).read_text()
        assert "[NULLIM_RUN][SKIP] reason=SKIPPED_NON_TRADING_DAY" in log
        assert "deploy-preflight" not in log
