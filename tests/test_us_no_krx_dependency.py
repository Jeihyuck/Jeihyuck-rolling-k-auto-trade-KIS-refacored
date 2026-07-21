"""US entry points must remain isolated from Korean-market imports and credentials."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


FORBIDDEN_TEXT = ("KRX 로그인 실패", "KRX_ID", "KRX_PW")
FORBIDDEN_MODULES = (
    "trader.universe.krx_safe",
    "trader.universe.providers.krx_provider",
    "trader.kr.calendar",
    "trader.kr.market_scope",
)
ROOT = Path(__file__).resolve().parents[1]


def _us_env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("KRX_ID", None)
    env.pop("KRX_PW", None)
    env.update({"MARKET_SCOPE": "us", "TRADING_MARKET": "us", "DISABLE_KR_IMPORTS_IN_US": "1"})
    return env


def test_us_session_lock_does_not_import_krx_modules_or_require_credentials():
    code = """
import sys
from trader.us.session_lock import acquire_session_lock
result = acquire_session_lock('us', 'am', trade_date='2026-07-21', min_interval_sec=0, lock_dir='/tmp/us-no-krx-lock')
print(result.ok)
print(','.join(sorted(name for name in sys.modules if name.startswith('trader.kr') or name.startswith('trader.universe.krx'))))
"""
    completed = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=_us_env(), text=True, capture_output=True, check=True)
    output = completed.stdout + completed.stderr
    for text in FORBIDDEN_TEXT:
        assert text not in output
    for module in FORBIDDEN_MODULES:
        assert module not in output


def test_us_am_lock_only_preflight_has_no_krx_credential_side_effects(tmp_path):
    env = _us_env()
    env.update({
        "NULLIM_APP_DIR": str(ROOT), "ALLOW_NON_CANONICAL_PATH": "1", "ALLOW_STALE_CODE": "1",
        "ALLOW_DIRTY_CODE": "1", "US_LOCK_ONLY": "1", "US_TRADE_DATE": "2099-07-21",
        "PYTHON_BIN": sys.executable,
    })
    completed = subprocess.run(
        ["bash", "scripts/wsl/run-us-am.sh"], cwd=ROOT, env=env, text=True, capture_output=True, check=True,
    )
    output = completed.stdout + completed.stderr
    for text in FORBIDDEN_TEXT:
        assert text not in output
