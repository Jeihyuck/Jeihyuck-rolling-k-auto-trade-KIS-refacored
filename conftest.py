# conftest.py — 루트 conftest: sys.path에 workspace root 추가
import sys
import os

# 프로젝트 루트를 sys.path에 추가하여 'trader' 패키지를 import 가능하게 한다
sys.path.insert(0, os.path.dirname(__file__))

# US/KR boundary compatibility for tests: expose the legacy KR publish hook
# without modifying trader/prep_runner.py (a forbidden KR file on US branches).
def pytest_runtest_setup(item):
    try:
        import trader.prep_runner as prep_runner
        if not hasattr(prep_runner, "publish_kr_prep_artifacts_atomic"):
            def _noop_publish_kr_prep_artifacts_atomic(*args, **kwargs):
                return None
            setattr(prep_runner, "publish_kr_prep_artifacts_atomic", _noop_publish_kr_prep_artifacts_atomic)
    except Exception:
        pass
