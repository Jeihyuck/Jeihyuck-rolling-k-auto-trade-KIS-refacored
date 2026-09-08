from contextlib import contextmanager

from trader.kr.infinite.models import Action, Decision
from trader.kr.infinite.runner import RunResult


@contextmanager
def _unlocked():
    yield True


def test_close_independent_tick_is_exit_only(monkeypatch):
    from trader.kr.infinite import session_runner

    calls = []
    monkeypatch.setattr(session_runner, "_tick_lock", _unlocked)
    monkeypatch.setattr(session_runner, "_write_health", lambda **_: None)

    def fake_run(**kwargs):
        calls.append(kwargs)
        return RunResult(Decision(Action.WAIT, "TEST_CLOSE"), None)

    result = session_runner.run_independent_tick(
        session="close",
        env="practice",
        allow_entry=False,
        run_fn=fake_run,
    )

    assert result is not None
    assert calls == [{"session": "close", "env": "practice", "allow_entry": False}]


def test_infinite_tick_failure_is_fail_soft_and_does_not_raise(monkeypatch):
    from trader.kr.infinite import session_runner

    monkeypatch.setattr(session_runner, "_tick_lock", _unlocked)
    monkeypatch.setattr(session_runner, "_write_health", lambda **_: None)

    def fail_run(**_):
        raise RuntimeError("pb1-independent-test")

    result = session_runner.run_independent_tick(
        session="afternoon",
        env="practice",
        allow_entry=True,
        run_fn=fail_run,
    )
    assert result is None


def test_session_runner_has_no_pb1_runtime_import():
    from pathlib import Path

    text = Path("trader/kr/infinite/session_runner.py").read_text()
    assert "trader.pb1_runner" not in text
    assert "PB1_LAST_RESULT_STATUS" not in text
    assert "PB1_LAST_EXIT_REASON" not in text
    assert "PB1_SESSION_RESULT_PATH" not in text
