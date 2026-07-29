import importlib
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import trader.time_utils as kr_time
import trader.us.market_calendar as us_calendar

ROOT=Path(__file__).parents[1]
CHECKER=ROOT/'scripts/wsl/check-nullim-trading-day.py'
RESOLVER=ROOT/'scripts/wsl/resolve-nullim-python.sh'


def checker(market, day, env=None):
    return subprocess.run([sys.executable,str(CHECKER),'--market',market,'--date',day],cwd=ROOT,env={**os.environ,**(env or {})},text=True,capture_output=True)


def test_configured_krx_holiday_is_closed_without_pykrx():
    result=checker('kr','2026-05-25')
    assert result.returncode == 10
    data=json.loads(result.stdout); assert data['status']=='SKIPPED_NON_TRADING_DAY' and data['calendar_source']=='KRX_HOLIDAY_CONFIG'


def test_kr_weekday_does_not_call_pykrx():
    with patch.object(kr_time,'_safe_get_nearest_business_day_in_a_week',side_effect=AssertionError('must not call pykrx')):
        assert kr_time.resolve_krx_trading_day_strict(date(2026,5,26)) == ('OPEN','KRX_HOLIDAY_CONFIG_FALLBACK')


def test_strict_krx_missing_config_fails_open():
    with patch.object(kr_time,'_get_krx_holidays_config_path',side_effect=FileNotFoundError):
        assert kr_time.resolve_krx_trading_day_strict(date(2026,8,17)) == ('OPEN','KRX_CALENDAR_UNAVAILABLE_FAIL_OPEN')


def test_strict_krx_unsupported_year_fails_open():
    assert kr_time.resolve_krx_trading_day_strict(date(2028,1,17)) == ('OPEN','UNSUPPORTED_CALENDAR_YEAR_FAIL_OPEN')


def test_kr_checker_never_fails_when_external_calendar_is_disabled():
    result=checker('kr','2026-07-29',env={'NULLIM_DISABLE_PYKRX_FOR_TEST':'1'})
    assert result.returncode == 0
    assert json.loads(result.stdout)['status'] == 'TRADING_DAY'


def test_us_calendar_open_closed_early_and_unsupported():
    assert us_calendar.us_calendar_load_error() is None
    assert {2026,2027} <= us_calendar.us_calendar_supported_years()
    normal=checker('us','2026-07-06'); closed=checker('us','2026-07-03'); early=checker('us','2026-11-27'); unsupported=checker('us','2028-01-17')
    assert normal.returncode == 0 and json.loads(normal.stdout)['early_close'] is False
    assert closed.returncode == 10
    early_data=json.loads(early.stdout); assert early.returncode == 0 and early_data['early_close'] is True and early_data['regular_close_et']=='13:00'
    assert unsupported.returncode == 1 and json.loads(unsupported.stdout)['reason']=='UNSUPPORTED_CALENDAR_YEAR'


def test_us_yaml_loader_failure_is_exposed(monkeypatch):
    real_import=__import__
    def broken_import(name,*args,**kwargs):
        if name=='yaml': raise ImportError('fixture missing yaml')
        return real_import(name,*args,**kwargs)
    monkeypatch.setattr('builtins.__import__',broken_import)
    module=importlib.reload(us_calendar)
    assert module.us_calendar_load_error().startswith('US_CALENDAR_LOAD_FAILED')
    monkeypatch.setattr('builtins.__import__',real_import)
    importlib.reload(module)


def test_python_resolver_prefers_venv_then_explicit_and_never_system(tmp_path):
    root=tmp_path/'repo'; (root/'.venv/bin').mkdir(parents=True); log=root/'selected'
    venv=root/'.venv/bin/python'; venv.write_text(f'#!/usr/bin/env bash\necho venv > "{log}"\n'); venv.chmod(0o755)
    explicit=tmp_path/'explicit'; explicit.write_text(f'#!/usr/bin/env bash\necho explicit > "{log}"\n'); explicit.chmod(0o755)
    command=f'source "{RESOLVER}"; selected=$(nullim_resolve_python "{root}"); "$selected"'
    result=subprocess.run(['bash','-c',command],env={**os.environ,'NULLIM_PYTHON_BIN':str(explicit)})
    assert result.returncode == 0 and log.read_text().strip()=='venv'
    venv.unlink(); subprocess.run(['bash','-c',command],env={**os.environ,'NULLIM_PYTHON_BIN':str(explicit)},check=True)
    assert log.read_text().strip()=='explicit'
    explicit.unlink(); missing=subprocess.run(['bash','-c',f'source "{RESOLVER}"; nullim_resolve_python "{root}"'],text=True,capture_output=True)
    assert missing.returncode != 0 and 'VENV_PYTHON_MISSING' in missing.stderr


def test_wrapper_without_project_python_fails_before_runner(tmp_path):
    import shutil
    root=tmp_path/'wrapper'; scripts=root/'scripts/wsl'; scripts.mkdir(parents=True)
    for name in ('run-us-am.sh','init-session-log.sh','check-nullim-trading-day.py','resolve-nullim-python.sh'):
        shutil.copy2(ROOT/'scripts/wsl'/name,scripts/name)
    env={key:value for key,value in os.environ.items() if key!='NULLIM_PYTHON_BIN'}
    env.update(NULLIM_KST_RUN_DATE='2026-07-27',US_TRADE_DATE='2026-07-27')
    result=subprocess.run(['bash',str(scripts/'run-us-am.sh')],cwd=root,env=env,text=True,capture_output=True)
    assert result.returncode != 0
    logs=list((root/'runtime/logs/us/2026-07-27/am').glob('*.log'))
    assert logs and 'VENV_PYTHON_MISSING' in logs[0].read_text()
    assert not (root/'runtime/logs/deploy-preflight.log').exists()


def test_operational_calendar_calls_use_project_python_resolver():
    init=(ROOT/'scripts/wsl/init-session-log.sh').read_text()
    mail=(ROOT/'scripts/wsl/send-market-log-mail.sh').read_text()
    health=(ROOT/'scripts/wsl/check-nullim-day-health.sh').read_text()
    resolver=RESOLVER.read_text()
    assert '$root/.venv/bin/python' in resolver and 'NULLIM_PYTHON_BIN' in resolver
    assert 'python3 "' not in '\n'.join(line for line in init.splitlines() if 'check-nullim-trading-day.py' in line)
    assert '"$CALENDAR_PYTHON"' in mail and '"$CALENDAR_PYTHON"' in health
