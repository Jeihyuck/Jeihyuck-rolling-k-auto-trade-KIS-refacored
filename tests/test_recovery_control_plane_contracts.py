from pathlib import Path


def test_kr_am_loop_env_contract():
    s = Path('scripts/wsl/run-kr-am.sh').read_text()
    for needle in ['PB1_LOOP_ENABLED="${PB1_LOOP_ENABLED:-1}"','PB1_RUN_LOOP="${PB1_RUN_LOOP:-1}"','PB1_RUN_LOOP_MINUTES="${PB1_RUN_LOOP_MINUTES:-235}"','PB1_AM_SESSION_END="${PB1_AM_SESSION_END:-12:55}"','KR_AM_SESSION_TIMEOUT_SEC="${KR_AM_SESSION_TIMEOUT_SEC:-15000}"','PB1 loop enabled']:
        assert needle in s


def test_kr_pm_loop_env_contract():
    s = Path('scripts/wsl/run-kr-afternoon.sh').read_text()
    for needle in ['PB1_LOOP_ENABLED="${PB1_LOOP_ENABLED:-1}"','PB1_RUN_LOOP="${PB1_RUN_LOOP:-1}"','PM_TARGET_START_TIME="${PM_TARGET_START_TIME:-13:00}"','PM_SESSION_END="${PM_SESSION_END:-15:10}"','KR_AFTERNOON_TIMEOUT_SEC="${KR_AFTERNOON_TIMEOUT_SEC:-9000}"','PB1 loop enabled']:
        assert needle in s


def test_cron_installer_blocks_wsl_and_scheduler_verifies_settings():
    cron = Path('scripts/wsl/install-nullim-cron.sh').read_text()
    assert '[CRON_INSTALL][BLOCK] reason=WSL_USES_WINDOWS_TASK_SCHEDULER' in cron
    verify = Path('scripts/windows/verify-scheduler.ps1').read_text()
    for needle in ['StartWhenAvailable','MultipleInstances','NextRunTime','LastTaskResult','send-market-log-mail.sh us','check-nullim-day-health.sh kr']:
        assert needle in verify


def test_kis_token_cache_private_and_rate_limit_error():
    s = Path('trader/kis_wrapper.py').read_text()
    assert 'runtime", "private", f"kis_token_{suffix}.json"' in s
    assert 'class KisTokenRateLimitError' in s
    assert 'EGW00133' in s and 'retry_after=65' in s
    us = Path('trader/us/execution/kis_us_client.py').read_text()
    assert 'Path("runtime/private")' in us
    assert 'kis_token_practice.json' in us
    assert 'TOKENP_UNKNOWN_OR_RATE_LIMIT' in us
