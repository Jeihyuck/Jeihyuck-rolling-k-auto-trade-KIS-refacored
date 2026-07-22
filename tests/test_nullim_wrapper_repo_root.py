from pathlib import Path

ROOT = Path(__file__).parents[1]
WRAPPERS = ('run-kr-prep.sh', 'run-kr-am.sh', 'run-kr-afternoon.sh', 'run-kr-close.sh', 'run-us-prep.sh', 'run-us-prep-recovery.sh', 'check-us-prep-before-am.sh', 'run-us-am.sh', 'run-us-afternoon.sh', 'run-us-close.sh')

def test_session_wrappers_resolve_their_own_repo_and_have_preflight_only_mode():
    for name in WRAPPERS:
        text = (ROOT / 'scripts/wsl' / name).read_text()
        assert 'nullim-repo-root.sh' in text
        assert 'nullim_resolve_repo_root "${BASH_SOURCE[0]}"' in text
        assert 'NULLIM_PREFLIGHT_ONLY' in text
        assert 'APP_DIR="${NULLIM_APP_DIR:-' not in text

def test_root_helper_ignores_stale_environment_and_preflight_requires_resolved_root():
    helper = (ROOT / 'scripts/wsl/nullim-repo-root.sh').read_text()
    preflight = (ROOT / 'scripts/wsl/deploy-preflight.sh').read_text()
    assert 'STALE_ENV_IGNORED' in helper
    assert 'NULLIM_RESOLVED_REPO_ROOT' in helper
    assert 'NULLIM_APP_DIR="$repo_root"' in helper
    assert 'expected="${NULLIM_RESOLVED_REPO_ROOT:-}"' in preflight
    assert 'RESOLVED_REPO_ROOT_MISSING' in preflight
    assert 'runtime/logs' in preflight
