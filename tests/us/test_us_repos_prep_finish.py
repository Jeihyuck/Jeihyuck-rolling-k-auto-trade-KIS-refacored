from __future__ import annotations

import json


class _FakeConn:
    def __init__(self, sink: dict):
        self._sink = sink

    def execute(self, _stmt, params):
        self._sink.update(params)
        return None


class _FakeBegin:
    def __init__(self, sink: dict):
        self._sink = sink

    def __enter__(self):
        return _FakeConn(self._sink)

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def __init__(self, sink: dict):
        self._sink = sink

    def begin(self):
        return _FakeBegin(self._sink)


def test_finish_us_prep_run_handles_string_result(monkeypatch):
    from trader.us.db import repos

    sink: dict = {}
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: _FakeEngine(sink))

    ok = repos.finish_us_prep_run(run_id="run-1", status="error", result="plain failure string")
    assert ok is True
    assert sink["status"] == "ERROR"

    payload = json.loads(sink["result"])
    assert payload["status"] == "ERROR"
    assert isinstance(payload["warnings"], list)
    assert isinstance(payload["errors"], list)
    assert payload["errors"] == ["plain failure string"]