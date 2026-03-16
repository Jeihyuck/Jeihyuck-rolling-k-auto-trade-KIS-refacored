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


def test_trade_readiness_policy_uses_prev_trading_day_on_monday_morning() -> None:
    from trader.time_utils import resolve_trade_readiness_as_of

    def fake_resolver(target: date, exchange: str) -> date:
        mapping = {
            date(2026, 3, 15): date(2026, 3, 13),
        }
        return mapping.get(target, target)

    resolved = resolve_trade_readiness_as_of(
        run_date=date(2026, 3, 16),
        market_window="morning",
        exchange="KRX",
        trading_day_resolver=fake_resolver,
    )

    assert resolved["requested_as_of"] == date(2026, 3, 15)
    assert resolved["resolved_as_of"] == date(2026, 3, 13)
    assert resolved["reason"] == "PREV_TRADING_DAY"


def test_trade_readiness_policy_uses_prior_session_on_tuesday_morning() -> None:
    from trader.time_utils import resolve_trade_readiness_as_of

    resolved = resolve_trade_readiness_as_of(
        run_date=date(2026, 3, 17),
        market_window="morning",
        exchange="KRX",
        trading_day_resolver=lambda target, _exchange: target,
    )

    assert resolved["requested_as_of"] == date(2026, 3, 16)
    assert resolved["resolved_as_of"] == date(2026, 3, 16)
    assert resolved["reason"] == "PREV_TRADING_DAY"


def test_trade_readiness_policy_uses_latest_trading_day_after_holiday() -> None:
    from trader.time_utils import resolve_trade_readiness_as_of

    def fake_resolver(target: date, exchange: str) -> date:
        mapping = {
            date(2026, 3, 2): date(2026, 2, 27),
        }
        return mapping.get(target, target)

    resolved = resolve_trade_readiness_as_of(
        run_date=date(2026, 3, 3),
        market_window="morning",
        exchange="KRX",
        trading_day_resolver=fake_resolver,
    )

    assert resolved["requested_as_of"] == date(2026, 3, 2)
    assert resolved["resolved_as_of"] == date(2026, 2, 27)
    assert resolved["reason"] == "PREV_TRADING_DAY"


def test_trade_readiness_policy_corrects_naive_non_trading_candidate() -> None:
    from trader.time_utils import resolve_trade_readiness_as_of

    def fake_resolver(target: date, exchange: str) -> date:
        mapping = {
            date(2026, 3, 15): date(2026, 3, 13),
        }
        return mapping.get(target, target)

    resolved = resolve_trade_readiness_as_of(
        run_date=date(2026, 3, 16),
        market_window="after",
        exchange="KRX",
        candidate_as_of=date(2026, 3, 15),
        trading_day_resolver=fake_resolver,
    )

    assert resolved["requested_as_of"] == date(2026, 3, 15)
    assert resolved["resolved_as_of"] == date(2026, 3, 13)
    assert resolved["reason"] == "PREV_TRADING_DAY"


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
