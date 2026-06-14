# -*- coding: utf-8 -*-
"""US daily report prep status loader compatibility tests."""
from __future__ import annotations


def test_load_us_prep_status_aliases_latest_loader(monkeypatch):
    """daily_report_runner import target should delegate to the latest prep loader."""
    import trader.us.db.repos as repos

    captured = {}

    def fake_load_latest_us_prep_status(trade_date: str, timeout_sec: int = 20) -> dict:
        captured["trade_date"] = trade_date
        captured["timeout_sec"] = timeout_sec
        return {"status": "OK", "trade_date": trade_date}

    monkeypatch.setattr(repos, "load_latest_us_prep_status", fake_load_latest_us_prep_status)

    assert repos.load_us_prep_status("2026-06-12", timeout_sec=7) == {
        "status": "OK",
        "trade_date": "2026-06-12",
    }
    assert captured == {"trade_date": "2026-06-12", "timeout_sec": 7}
