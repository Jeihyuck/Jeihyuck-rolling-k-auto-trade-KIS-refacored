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
    assert 'Script="$base/send-market-log-mail.sh";Args=@("kr")' in text
    assert 'Script="$base/check-nullim-day-health.sh";Args=@("us")' in text
    assert 'exec bash $scriptQ' in text
