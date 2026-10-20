"""No-network exact-run PREP recovery regression: newer STARTED cannot mask valid completed run."""
from contextlib import contextmanager
from pathlib import Path

import pytest

from trader.us.db import repos

DATE = "2026-10-16"
RUN_ID = "effective-defensive-run"


class FakeRows:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = {}

    def execute(self, statement, params):
        self.sql = str(statement)
        self.params = dict(params)
        return FakeRows(self.rows)


class FakeEngine:
    def __init__(self, rows):
        self.conn = FakeConnection(rows)

    @contextmanager
    def begin(self):
        yield self.conn


def row(status="DEFENSE_CRASH_ENTRY_BLOCKED", run_id=RUN_ID, **extra):
    return dict(run_id=run_id, status=status, trade_date=DATE, result={},
                started_at="2026-10-16T10:30:00+00:00",
                finished_at="2026-10-16T11:00:00+00:00", **extra)


def use_rows(monkeypatch, rows):
    engine = FakeEngine(rows)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(repos, "_set_statement_timeout", lambda *_args, **_kwargs: None)
    return engine


def test_defensive_completion_exact_lookup_survives_newer_started(monkeypatch):
    engine = use_rows(monkeypatch, [row()])
    result = repos.load_us_prep_completion_for_run(DATE, RUN_ID)
    assert result["status"] == "DEFENSE_CRASH_ENTRY_BLOCKED"
    assert ":run_id" in engine.conn.sql
    assert "finished_at IS NOT NULL" in engine.conn.sql
    assert engine.conn.params == {"trade_date": DATE, "run_id": RUN_ID}


@pytest.mark.parametrize("status", [
    "STARTED", "ERROR", "DEGRADED", "FAILED", "UNKNOWN_ENTRY_BLOCKED"
])
def test_failed_or_incomplete_run_never_reused(monkeypatch, status):
    use_rows(monkeypatch, [row(status)])
    assert repos.load_us_prep_completion_for_run(DATE, RUN_ID) == {}


def test_missing_or_duplicate_run_fails_closed(monkeypatch):
    use_rows(monkeypatch, [])
    assert repos.load_us_prep_completion_for_run(DATE, RUN_ID) == {}
    use_rows(monkeypatch, [row(), row()])
    assert repos.load_us_prep_completion_for_run(DATE, RUN_ID) == {}


def test_cross_run_or_cross_date_fails_closed(monkeypatch):
    use_rows(monkeypatch, [row(run_id="different")])
    assert repos.load_us_prep_completion_for_run(DATE, RUN_ID) == {}
    use_rows(monkeypatch, [dict(row(), trade_date="2026-10-15")])
    assert repos.load_us_prep_completion_for_run(DATE, RUN_ID) == {}


def test_preflight_and_cleanup_resolve_specific_completed_artifact():
    for file in (
        "scripts/wsl/check-us-prep-before-am.sh",
        "scripts/wsl/run-us-prep.sh",
    ):
        script = Path(file).read_text(encoding="utf-8")
        assert "check_effective_prep_sources(trade_date)" in script
        helper = Path("trader/us/prep_effective.py").read_text(encoding="utf-8")
        assert "load_us_prep_completion_for_run(trade_date, run_id)" in helper
        assert "load_latest_us_prep_status" not in script
