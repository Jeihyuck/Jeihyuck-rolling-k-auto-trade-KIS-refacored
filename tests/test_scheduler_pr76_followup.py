from pathlib import Path

ROOT = Path(__file__).parents[1]
ORDER_TASKS = ('PB1 KR AM WSL', 'PB1 KR Afternoon WSL', 'PB1 KR Close WSL', 'PB1 US AM WSL', 'PB1 US Afternoon WSL', 'PB1 US Close WSL')

def test_windows_restart_zero_omits_restart_xml_fields_and_unsets_stale_env():
    text = (ROOT / 'scripts/windows/update-nullim-scheduler.ps1').read_text()
    assert 'if ($restart -gt 0)' in text
    assert '-RestartCount $restart -RestartInterval' in text
    assert 'else {' in text and '-MultipleInstances IgnoreNew' in text
    assert 'unset NULLIM_APP_DIR' in text
    assert 'Out-String' not in text
    for task in ORDER_TASKS:
        assert task in text

def test_windows_verifier_uses_raw_actions_and_null_restart_is_valid():
    text = (ROOT / 'scripts/windows/verify-scheduler.ps1').read_text()
    assert '"$($_.Execute) $($_.Arguments)"' in text
    assert 'Out-String' not in text
    assert '$null -eq $task.Settings.RestartCount' in text
    assert 'field=$field' in text

def test_wsl_verifier_skips_missing_files():
    text = (ROOT / 'scripts/wsl/verify-no-nullim-auto-scheduler.sh').read_text()
    assert '[[ -f "$1" ]] || return 0' in text

def test_mail_and_health_define_scripts_and_arguments_separately():
    text = (ROOT / 'scripts/windows/update-nullim-scheduler.ps1').read_text()
    assert 'function New-NullimTaskActionArguments' in text
    assert 'Script="$base/send-kr-log-mail.sh";Args=@()' in text
    assert 'Script="$base/send-us-log-mail.sh";Args=@()' in text
    assert 'Script="$base/send-market-log-mail.sh"' not in text
    assert 'Script="$base/check-nullim-day-health.sh";Args=@("us")' in text
    assert 'exec bash $entryQ $scriptQ' in text

def _bash_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"

def test_mail_health_actions_keep_script_and_market_as_distinct_bash_words(tmp_path):
    fixture = tmp_path / 'fixture.sh'
    fixture.write_text('#!/usr/bin/env bash\nprintf "%s|%s" "$0" "$1"\n')
    fixture.chmod(0o755)
    for market in ('kr', 'us'):
        command = f'exec bash {_bash_single_quote(str(fixture))} {_bash_single_quote(market)}'
        import subprocess
        result = subprocess.run(['bash', '-lc', command], text=True, capture_output=True, check=True)
        assert result.stdout == f'{fixture}|{market}'
        bad = f'exec bash {_bash_single_quote(str(fixture) + " " + market)}'
        assert str(fixture) + ' ' + market in bad
        assert bad != command

def test_windows_scheduler_scripts_have_a_real_parser_check():
    text = (ROOT / 'scripts/windows/test-parse.ps1').read_text()
    assert 'Parser]::ParseFile' in text
    workflow = (ROOT / '.github/workflows/scheduler-policy.yml').read_text()
    assert 'powershell-parse:' in workflow and 'test-parse.ps1' in workflow
