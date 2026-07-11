"""US trade session prep guard CLI status handling contracts."""

import pytest


def test_failed_prep_guard_status_exits_one(monkeypatch):
    from trader.us.runner import trade_session_runner as mod

    monkeypatch.setattr(mod, "run_trade_session", lambda **kwargs: {"status": "FAILED_PREP_GUARD"})
    monkeypatch.setattr("sys.argv", ["trade_session_runner.py", "--session", "am"])
    with pytest.raises(SystemExit) as exc:
        mod.main()
    assert exc.value.code == 1


@pytest.mark.parametrize("status", ["OK", "OK_WITH_WARNINGS", "SKIP"])
def test_non_failed_status_does_not_exit_one(monkeypatch, status):
    from trader.us.runner import trade_session_runner as mod

    monkeypatch.setattr(mod, "run_trade_session", lambda **kwargs: {"status": status})
    monkeypatch.setattr("sys.argv", ["trade_session_runner.py", "--session", "am"])
    assert mod.main() is None
