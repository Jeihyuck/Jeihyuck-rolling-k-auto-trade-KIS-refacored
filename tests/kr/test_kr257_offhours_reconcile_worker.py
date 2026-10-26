"""PR257 worker decommission: no cross-session competing economic writer."""
from datetime import datetime
from zoneinfo import ZoneInfo
import inspect

import pytest

import scripts.kr257_offhours_reconcile_worker as worker

KST = ZoneInfo("Asia/Seoul")


def test_worker_daypart_diagnostic_stays_conservative():
    for h, m in [(5, 30), (6, 30), (8, 55), (9, 5), (15, 30), (16, 29)]:
        assert not worker.offhours_allowed(datetime(2026, 10, 26, h, m, tzinfo=KST))
    assert worker.offhours_allowed(datetime(2026, 10, 26, 17, 0, tzinfo=KST))
    assert worker.offhours_allowed(datetime(2026, 10, 25, 13, 0, tzinfo=KST))


@pytest.mark.parametrize("at", [
    datetime(2026, 10, 26, 5, 29, 59, tzinfo=KST),
    datetime(2026, 10, 26, 5, 30, 0, tzinfo=KST),
    datetime(2026, 10, 26, 16, 31, tzinfo=KST),
    datetime(2026, 10, 25, 13, 0, tzinfo=KST),
])
def test_even_operator_approved_offhours_writer_is_always_blocked(at):
    assert worker.worker_permitted(
        env="practice", strategy="pb1_pullback_close",
        endpoint="https://openapivts.koreainvestment.com:29443",
        explicit=True, approved=True, now=at,
    ) == (False, worker.DISABLED_REASON)


def test_retired_worker_entrypoint_no_db_or_kis_import_even_if_execute(capsys, monkeypatch):
    # A dirty production-looking process environment cannot bypass the
    # hard disable; importing the script has no KIS/DB effect.
    for key, value in {
        "KIS_ENV": "practice", "STRATEGY_ENV": "practice",
        "KR257_OFFHOURS_RECONCILE_APPROVED": "1",
        "KIS_REST_URL": "https://openapivts.koreainvestment.com:29443",
    }.items():
        monkeypatch.setenv(key, value)
    assert worker.main(["--execute"]) == 2
    output = capsys.readouterr().out
    assert "BLOCKED" in output and worker.DISABLED_REASON in output
    src = inspect.getsource(worker)
    assert "reconcile_kis(" not in src
    assert "get_engine(" not in src
    assert "KisAPI(" not in src
    assert "pg_try_advisory_lock" not in src
    assert "buy_stock" not in src and "sell_stock" not in src
