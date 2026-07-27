import os
import subprocess
from pathlib import Path

ROOT=Path(__file__).parents[1]
MAIL=ROOT/'scripts/wsl/send-market-log-mail.sh'

def run(*args, env=None):
    return subprocess.run(['bash',str(MAIL),*args],cwd=ROOT,env={**os.environ,**(env or {})},text=True,capture_output=True)

def test_market_is_fail_closed():
    assert run().returncode == 64
    assert run('KR').returncode == 64
    assert run('all').returncode == 64

def test_dedicated_wrappers_pin_market():
    assert (ROOT/'scripts/wsl/send-kr-log-mail.sh').read_text().rstrip().endswith('send-market-log-mail.sh" kr')
    assert (ROOT/'scripts/wsl/send-us-log-mail.sh').read_text().rstrip().endswith('send-market-log-mail.sh" us')

def test_collector_has_fail_closed_security_contracts():
    text=MAIL.read_text()
    for needle in ('REQUIRED_LOG_MISSING','TAR_FAILED','TAR_VERIFY_FAILED','SESSION_STILL_RUNNING','ARCHIVE_TOO_LARGE','NULLIM_LOG_ARCHIVE_MANIFEST.json','redaction_applied','NULLIM_FORCE_RESEND'):
        assert needle in text
    assert 'tar -czf "$OUT"' not in text or 'if ! tar' in text
    assert 'runtime/wsl-kr-' not in text and 'runtime/wsl-us-' not in text

def test_mail_sender_requires_and_validates_attachment():
    text=(ROOT/'scripts/notify/send_mail_attachment.py').read_text()
    assert 'at least one attachment is required' in text
    assert 'tarfile.open' in text
    assert 'attachment exceeds' in text
