from __future__ import annotations

import json
from pathlib import Path

from trader.us.db import repos


def test_load_us_prep_status_runtime_fallback_when_db_missing(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(repos, "load_latest_us_prep_status", lambda trade_date, timeout_sec=20: {})

    status_dir = Path("runtime/us/prep_status/2026-06-12")
    status_dir.mkdir(parents=True)
    (status_dir / "prep_status.json").write_text(
        json.dumps(
            {
                "trade_date": "2026-06-12",
                "status": "OK",
                "trade_can_proceed": True,
                "contract_ok": True,
                "locked_count": 30,
                "run_id": "prep-123",
            }
        ),
        encoding="utf-8",
    )

    result = repos.load_us_prep_status("2026-06-12")

    assert result is not None
    assert result["status"] == "OK"
    assert result["run_id"] == "prep-123"
    assert result["result"]["trade_can_proceed"] is True
    assert result["result"]["score_contract_ok"] is True
    assert result["source"] == "runtime_prep_status_json"


def test_load_us_prep_status_returns_none_when_db_fails_and_no_runtime(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    def fail_db(trade_date, timeout_sec=20):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(repos, "load_latest_us_prep_status", fail_db)

    assert repos.load_us_prep_status("2026-06-12") is None
