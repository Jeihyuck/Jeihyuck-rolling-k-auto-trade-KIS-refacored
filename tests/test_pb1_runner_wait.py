import sys
import types
import logging
import importlib.util
from datetime import datetime
from zoneinfo import ZoneInfo
from types import SimpleNamespace


def _install_sa_stub() -> None:
    if importlib.util.find_spec("sqlalchemy") is not None:
        return
    if "sqlalchemy" in sys.modules:
        return
    sa = types.ModuleType("sqlalchemy")

    class _DummyResult:
        def scalar(self):
            return None

        def scalars(self):
            return self

        def mappings(self):
            return self

        def all(self):
            return []

        def first(self):
            return None

        def __iter__(self):
            return iter([])

    class _DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args, **kwargs):
            return _DummyResult()

    class _DummyEngine:
        def __init__(self, url):
            self.url = url

        def begin(self):
            return _DummyConn()

    class _DummyStatement:
        def values(self, *args, **kwargs):
            return self

        def where(self, *args, **kwargs):
            return self

        def returning(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def limit(self, *args, **kwargs):
            return self

    sa.Engine = _DummyEngine
    sa.create_engine = lambda url, **kwargs: _DummyEngine(url)
    sa.text = lambda sql: sql
    sa.MetaData = lambda *args, **kwargs: object()
    sa.Column = lambda *args, **kwargs: object()
    sa.Table = lambda *args, **kwargs: object()
    sa.Connection = object
    scalar_type = lambda *args, **kwargs: object()
    sa.String = sa.Integer = sa.Float = sa.JSON = sa.Boolean = sa.DateTime = sa.Text = scalar_type
    sa.ForeignKey = lambda *args, **kwargs: None
    sa.func = types.SimpleNamespace(now=lambda: None)
    sa.insert = lambda *args, **kwargs: _DummyStatement()
    sa.update = lambda *args, **kwargs: _DummyStatement()
    sa.select = lambda *args, **kwargs: _DummyStatement()
    sa.and_ = lambda *args, **kwargs: None
    sa.UniqueConstraint = lambda *args, **kwargs: None

    sa_engine = types.ModuleType("sqlalchemy.engine")
    sa_engine.url = types.SimpleNamespace(make_url=lambda url: types.SimpleNamespace(database=url))
    sa_engine.make_url = sa_engine.url.make_url
    sa_engine.Engine = _DummyEngine
    sys.modules["sqlalchemy.engine"] = sa_engine
    sys.modules["sqlalchemy.engine.url"] = sa_engine.url
    sys.modules["sqlalchemy"] = sa


_install_sa_stub()

import trader.pb1_runner as pb1_runner
from trader.pb1_runner import _next_window_start, _parse_hhmm_to_time


def test_next_window_start_returns_next_in_day() -> None:
    now = datetime(2024, 1, 2, 8, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    starts = [_parse_hhmm_to_time("08:50"), _parse_hhmm_to_time("14:00"), _parse_hhmm_to_time("15:20")]

    target = _next_window_start(now, starts)

    assert target is not None
    assert target.hour == 8 and target.minute == 50
    assert target.tzinfo == now.tzinfo


def test_next_window_start_none_when_past_all_windows() -> None:
    now = datetime(2024, 1, 2, 16, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    starts = [_parse_hhmm_to_time("08:50"), _parse_hhmm_to_time("14:00"), _parse_hhmm_to_time("15:20")]

    target = _next_window_start(now, starts)

    assert target is None


def test_wait_branch_does_not_crash(monkeypatch) -> None:
    # Arrange times to enter wait then proceed
    times = [
        datetime(2024, 1, 2, 8, 40, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2024, 1, 2, 8, 45, tzinfo=ZoneInfo("Asia/Seoul")),
        datetime(2024, 1, 2, 8, 50, tzinfo=ZoneInfo("Asia/Seoul")),
    ]

    def fake_now():
        return times.pop(0) if times else datetime(2024, 1, 2, 8, 50, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.setattr(pb1_runner, "now_kst", fake_now)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "push")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.delenv("EXPECT_LIVE_TRADING", raising=False)
    monkeypatch.setattr(pb1_runner, "try_acquire_lock", lambda *_, **__: False)
    monkeypatch.setattr(
        pb1_runner,
        "parse_args",
        lambda: SimpleNamespace(window="auto", phase="auto", target_branch="bot-state"),
    )
    monkeypatch.setattr(
        pb1_runner,
        "KisAPI",
        lambda *_, **__: SimpleNamespace(
            env="practice",
            get_balance_cached=lambda **__: {},
            get_price_quote=lambda *_, **__: {},
        ),
    )
    dummy_engine = SimpleNamespace(url="postgresql+psycopg://localhost/postgres")
    monkeypatch.setattr(pb1_runner, "assert_db_ready", lambda: None)
    monkeypatch.setattr(pb1_runner, "make_engine", lambda *_, **__: dummy_engine)
    monkeypatch.setattr(pb1_runner, "run_migrations", lambda *_, **__: None)

    slept = []

    def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(pb1_runner.time_mod, "sleep", fake_sleep)
    monkeypatch.setattr(pb1_runner, "is_trading_day", lambda _: True)

    # Act / Assert: should exit cleanly without raising
    pb1_runner.main()
    assert slept, "wait path should invoke sleep"


def test_expect_live_guard_skipped_in_diag(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 16, 10, tzinfo=ZoneInfo("Asia/Seoul"))
    monkeypatch.setattr(pb1_runner, "now_kst", lambda: now)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("EXPECT_LIVE_TRADING", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("API_BASE_URL", "https://openapivts.koreainvestment.com:29443")
    monkeypatch.setattr(pb1_runner, "try_acquire_lock", lambda *_, **__: False)
    monkeypatch.setattr(
        pb1_runner,
        "parse_args",
        lambda: SimpleNamespace(window="auto", phase="auto", target_branch="bot-state"),
    )
    monkeypatch.setattr(
        pb1_runner,
        "KisAPI",
        lambda *_, **__: SimpleNamespace(
            env="practice",
            get_balance_cached=lambda **__: {},
            get_price_quote=lambda *_, **__: {},
        ),
    )
    dummy_engine = SimpleNamespace(url="postgresql+psycopg://localhost/postgres")
    monkeypatch.setattr(pb1_runner, "assert_db_ready", lambda: None)
    monkeypatch.setattr(pb1_runner, "make_engine", lambda *_, **__: dummy_engine)
    monkeypatch.setattr(pb1_runner, "run_migrations", lambda *_, **__: None)

    pb1_runner.main()  # should not raise even with EXPECT_LIVE_TRADING=1 in diag path


def test_schedule_event_does_not_wait(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 8, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    monkeypatch.setattr(pb1_runner, "now_kst", lambda: now)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("PB1_ALLOW_WAIT", "0")
    monkeypatch.setattr(pb1_runner, "try_acquire_lock", lambda *_, **__: False)
    monkeypatch.setattr(
        pb1_runner,
        "parse_args",
        lambda: SimpleNamespace(window="auto", phase="auto", target_branch="bot-state"),
    )
    monkeypatch.setattr(
        pb1_runner,
        "KisAPI",
        lambda *_, **__: SimpleNamespace(
            env="practice",
            get_balance_cached=lambda **__: {},
            get_price_quote=lambda *_, **__: {},
        ),
    )
    dummy_engine = SimpleNamespace(url="postgresql+psycopg://localhost/postgres")
    monkeypatch.setattr(pb1_runner, "assert_db_ready", lambda: None)
    monkeypatch.setattr(pb1_runner, "make_engine", lambda *_, **__: dummy_engine)
    monkeypatch.setattr(pb1_runner, "run_migrations", lambda *_, **__: None)

    def sleep_fail(_):
        raise AssertionError("schedule run should not sleep")

    monkeypatch.setattr(pb1_runner.time_mod, "sleep", sleep_fail)

    pb1_runner.main()


def test_trade_prep_not_done_emits_run_summary(monkeypatch, caplog) -> None:
    now = datetime(2024, 1, 2, 9, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    dummy_engine = SimpleNamespace(url="postgresql+psycopg://localhost/postgres")

    class DummyLedgerEventsRepo:
        def __init__(self, _engine):
            pass

        def prep_done_status(self, env, as_of):
            assert env == "practice"
            assert as_of.isoformat() == "2024-01-01"
            return False, 0

    monkeypatch.setattr(pb1_runner, "now_kst", lambda: now)
    monkeypatch.setattr(pb1_runner, "parse_args", lambda: SimpleNamespace(window="auto", phase="auto", target_branch="bot-state", env="practice"))
    monkeypatch.setattr(pb1_runner, "make_engine", lambda *_, **__: dummy_engine)
    monkeypatch.setattr(pb1_runner, "LedgerEventsRepo", DummyLedgerEventsRepo)
    monkeypatch.setattr(pb1_runner, "resolve_derived_as_of", lambda _now: datetime(2024, 1, 1, tzinfo=ZoneInfo("Asia/Seoul")).date())
    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("PB1_SESSION_KIND", "am")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.delenv("ALLOW_TRADE_WITH_WATCHLIST_ONLY", raising=False)

    with caplog.at_level(logging.INFO, logger="trader.pb1_runner"):
        exit_code = pb1_runner.main()

    assert exit_code == 0
    assert "[RUN_SUMMARY][RESULT] status=SKIP_PRECHECK reason=PREP_NOT_DONE session=am event=schedule" in caplog.text


def test_close_manual_replay_late_start_runs_single_pass(monkeypatch, caplog) -> None:
    now = datetime(2024, 1, 2, 15, 54, tzinfo=ZoneInfo("Asia/Seoul"))

    class DummyLockConn:
        def close(self):
            return None

    class DummyEngine:
        url = "postgresql+psycopg://localhost/postgres"

        def connect(self):
            return DummyLockConn()

    calls = {"run_once": 0}

    def fake_run_once(**kwargs):
        calls["run_once"] += 1
        assert kwargs["loop_mode"] is False
        return [], True, {}, "exit", "OK_MANUAL_REPLAY"

    monkeypatch.setattr(pb1_runner, "now_kst", lambda: now)
    monkeypatch.setattr(pb1_runner, "parse_args", lambda: SimpleNamespace(window="auto", phase="exit", target_branch="bot-state", env="practice"))
    monkeypatch.setattr(pb1_runner, "assert_db_ready", lambda: None)
    monkeypatch.setattr(pb1_runner, "make_engine", lambda *_, **__: DummyEngine())
    monkeypatch.setattr(pb1_runner, "try_acquire_lock", lambda *_, **__: True)
    monkeypatch.setattr(pb1_runner, "release_advisory_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "_ensure_bootstrap_migrations", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "_write_change_flag", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "run_once", fake_run_once)
    monkeypatch.setattr(pb1_runner, "_run_loop", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("manual replay should not enter loop")))
    monkeypatch.setenv("MODE", "noop")
    monkeypatch.setenv("PB1_SESSION_KIND", "close")
    monkeypatch.setenv("PB1_LOOP_ENABLED", "1")
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "close")
    monkeypatch.setenv("FORCE_PB1_PHASE", "exit")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("FORCE_STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("CLOSE_MANUAL_MODE", "diag_replay")

    with caplog.at_level(logging.INFO, logger="trader.pb1_runner"):
        exit_code = pb1_runner.main()

    assert exit_code == 0
    assert calls["run_once"] == 1
    assert "[TRADE_CLOSE][MANUAL_REPLAY] mode=diag_replay" in caplog.text
    assert "[TRADE_CLOSE][MANUAL_REPLAY][DONE] mode=diag_replay status=OK_MANUAL_REPLAY phase=exit" in caplog.text


def test_decide_action_smoke_after_close():
    now = datetime(2024, 1, 2, 16, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    open_dt, close_dt = pb1_runner._market_session(now)

    action, target_start = pb1_runner._decide_action(
        now=now,
        trading_day=True,
        open_dt=open_dt,
        close_dt=close_dt,
        allow_wait=True,
        max_wait_s=3600,
        smoke_enabled=False,
    )

    assert action == "smoke"
    assert target_start is None


def test_decide_action_force_close_exit_run(monkeypatch):
    monkeypatch.setenv("FORCE_RUN", "1")
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "close")
    monkeypatch.setenv("FORCE_PB1_PHASE", "exit")
    now = datetime(2024, 1, 2, 16, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    open_dt, close_dt = pb1_runner._market_session(now)

    action, target_start = pb1_runner._decide_action(
        now=now,
        trading_day=True,
        open_dt=open_dt,
        close_dt=close_dt,
        allow_wait=True,
        max_wait_s=3600,
        smoke_enabled=False,
    )

    assert action == "run"
    assert target_start is None


def test_pm_handoff_to_close_after_close_start(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "day")
    monkeypatch.setenv("FORCE_PB1_PHASE", "entry")

    now = datetime(2024, 1, 2, 15, 16, tzinfo=ZoneInfo("Asia/Seoul"))

    assert pb1_runner._pm_should_handoff_to_close(now) is True


def test_forced_close_live_execution_enabled(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "close")
    monkeypatch.setenv("PB1_PHASE_DEFAULT", "exit")
    monkeypatch.setenv("FORCE_PB1_PHASE", "exit")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("FORCE_STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("FORCE_BLOCK_LIVE", "0")

    assert pb1_runner._forced_close_live_execution_enabled() is True


def test_session_recovery_guard_allows_normal_am_run(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 9, 12, tzinfo=ZoneInfo("Asia/Seoul"))

    class DummyOrdersRepo:
        def __init__(self, _engine):
            pass

        def list_today_buy_orders(self, *_args, **_kwargs):
            return []

        def consume_fail_open_marker(self, _op_name):
            return False

    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyOrdersRepo)
    monkeypatch.setattr(pb1_runner, "load_job_checkpoint", lambda *_args, **_kwargs: None)

    result = pb1_runner._evaluate_session_recovery_guard(
        engine=SimpleNamespace(),
        env="practice",
        session_kind="am",
        now=now,
    )

    assert result["skip"] is False
    assert result["recovery_used"] is False


def test_session_recovery_guard_allows_late_am_recovery(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 10, 7, tzinfo=ZoneInfo("Asia/Seoul"))

    class DummyOrdersRepo:
        def __init__(self, _engine):
            pass

        def list_today_buy_orders(self, *_args, **_kwargs):
            return []

        def consume_fail_open_marker(self, _op_name):
            return False

    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyOrdersRepo)
    monkeypatch.setattr(pb1_runner, "load_job_checkpoint", lambda *_args, **_kwargs: None)

    result = pb1_runner._evaluate_session_recovery_guard(
        engine=SimpleNamespace(),
        env="practice",
        session_kind="am",
        now=now,
    )

    assert result["skip"] is False
    assert result["recovery_used"] is True
    assert result["exit_reason"] == "recovery_continue"


def test_session_recovery_guard_skips_duplicate_am_run(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 10, 7, tzinfo=ZoneInfo("Asia/Seoul"))

    class DummyOrdersRepo:
        def __init__(self, _engine):
            pass

        def list_today_buy_orders(self, *_args, **_kwargs):
            return [{"code": "005930", "status": "SUBMITTED"}]

        def consume_fail_open_marker(self, _op_name):
            return False

    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyOrdersRepo)
    monkeypatch.setattr(pb1_runner, "load_job_checkpoint", lambda *_args, **_kwargs: None)

    result = pb1_runner._evaluate_session_recovery_guard(
        engine=SimpleNamespace(),
        env="practice",
        session_kind="am",
        now=now,
    )

    assert result["skip"] is True
    assert result["exit_reason"] == "skip_duplicate_am_run"


def test_session_recovery_guard_skips_after_am_cutoff(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 10, 42, tzinfo=ZoneInfo("Asia/Seoul"))

    class DummyOrdersRepo:
        def __init__(self, _engine):
            pass

        def list_today_buy_orders(self, *_args, **_kwargs):
            return []

        def consume_fail_open_marker(self, _op_name):
            return False

    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyOrdersRepo)
    monkeypatch.setattr(pb1_runner, "load_job_checkpoint", lambda *_args, **_kwargs: None)

    result = pb1_runner._evaluate_session_recovery_guard(
        engine=SimpleNamespace(),
        env="practice",
        session_kind="am",
        now=now,
    )

    assert result["skip"] is True
    assert result["exit_reason"] == "late_schedule_start_after_recovery_window_am"


def test_session_recovery_guard_allows_late_pm_recovery(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 13, 32, tzinfo=ZoneInfo("Asia/Seoul"))

    class DummyOrdersRepo:
        def __init__(self, _engine):
            pass

        def list_today_buy_orders(self, *_args, **_kwargs):
            return []

        def consume_fail_open_marker(self, _op_name):
            return False

    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyOrdersRepo)
    monkeypatch.setattr(pb1_runner, "load_job_checkpoint", lambda *_args, **_kwargs: None)

    result = pb1_runner._evaluate_session_recovery_guard(
        engine=SimpleNamespace(),
        env="practice",
        session_kind="pm",
        now=now,
    )

    assert result["skip"] is False
    assert result["recovery_used"] is True
    assert result["exit_reason"] == "recovery_continue"
