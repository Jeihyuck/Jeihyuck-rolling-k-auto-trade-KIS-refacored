from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock, patch


KST = ZoneInfo("Asia/Seoul")


def test_preopen_date_policy_uses_prev_trading_day() -> None:
    from trader import prep_runner

    run_ts = datetime(2026, 3, 9, 8, 30, tzinfo=KST)
    with patch("trader.prep_runner.now_kst", return_value=run_ts):
        as_of = prep_runner._pick_as_of_date_always_prev()

    assert as_of == date(2026, 3, 6)


def test_candidate_pool_future_snapshot_rejected() -> None:
    from trader.candidate_pool_builder import load_candidate_pool

    fake_repo = MagicMock()
    fake_repo.get_latest_watchlist_date.return_value = date(2026, 3, 7)

    with patch("trader.candidate_pool_builder.WatchlistRepo", return_value=fake_repo):
        codes, used_as_of, reason = load_candidate_pool(
            engine=MagicMock(),
            env="practice",
            today=date(2026, 3, 6),
        )

    assert codes is None
    assert used_as_of is None
    assert reason == "future_snapshot"
