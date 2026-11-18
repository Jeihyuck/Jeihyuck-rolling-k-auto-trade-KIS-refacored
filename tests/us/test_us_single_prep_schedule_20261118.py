"""US Windows scheduler contract: one primary PREP, no PREWARM tasks.

The two former EDT/EST PREWARM jobs both invoked the full run-us-prep.sh,
and pinned the code hours before the official PREP.  Removing these jobs
must not weaken same-session SHA pins or the conditional recovery gate.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _src(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_one_primary_us_prep_in_windows_scheduler():
    installer = _src("scripts/windows/update-nullim-scheduler.ps1")
    assert installer.count('@{Name="PB1 US Prep WSL"') == 1
    assert installer.count('Script="$base/run-us-prep.sh"') == 1
    assert '"PB1 US Prep Prewarm EDT WSL"' not in installer
    assert '"PB1 US Prep Prewarm EST WSL"' not in installer
    assert 'canonical_windows_tasks=14' in installer
    # Reinstall must remove obsolete Windows tasks rather than merely
    # omit them from the new task set.
    assert "Unregister-ScheduledTask" in installer
    assert 'run-us-prep-recovery.sh' in installer
    assert 'check-us-prep-before-am.sh' in installer


def test_preflight_expects_one_us_prep_and_keeps_est_profile():
    verifier = _src("scripts/windows/verify-scheduler.ps1")
    assert verifier.count('"PB1 US Prep WSL"=') == 1
    assert '"PB1 US Prep Prewarm EDT WSL"' not in verifier
    assert '"PB1 US Prep Prewarm EST WSL"' not in verifier
    assert 'canonical_windows_tasks=14' in verifier
    assert 'Get-UsKstTime "21:30"' in verifier
    assert 'Get-UsKstTime "22:30"' in verifier


def test_mail_evidence_requires_prep_but_not_retired_prewarms():
    mail = _src("scripts/wsl/send-market-log-mail.sh")
    assert "purposes=(prep prep-recovery am-preflight am afternoon close)" in mail
    assert "prep-prewarm-edt" not in mail
    assert "prep-prewarm-est" not in mail


def test_us_code_pin_preserved_until_after_session():
    sync = _src("scripts/wsl/sync-market-code.sh")
    preflight = _src("scripts/wsl/deploy-preflight.sh")
    assert 'reason=SESSION_ALREADY_PINNED' in sync
    assert 'git fetch origin dual-agent' in sync
    assert 'git reset --hard origin/dual-agent' in sync
    assert "PINNED_SHA_MISMATCH" in preflight
