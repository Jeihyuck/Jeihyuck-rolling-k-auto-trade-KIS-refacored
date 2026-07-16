# -*- coding: utf-8 -*-
"""파일 prep_contract가 없어도 DB fallback으로 check_us_prep_guard()가 통과해야 한다."""
from __future__ import annotations

import pytest


def _make_rows(n: int = 30) -> list[dict]:
    return [
        {
            "symbol": f"SYM{i}",
            "score_final": float(i + 1),
            "score": float(i + 1),
        }
        for i in range(n)
    ]


def test_db_fallback_ok_when_file_missing(monkeypatch):
    """파일 contract None + DB OK + watchlist 30개 → ok=True, source=db."""
    import trader.us.prep_contract as pc_mod
    import trader.us.db.repos as repos_mod

    monkeypatch.setattr(
        "trader.us.path_contract.load_us_prep_contract",
        lambda trade_date: None,
    )
    monkeypatch.setattr(
        repos_mod,
        "load_latest_us_prep_status",
        lambda trade_date, timeout_sec=20: {"status": "OK", "run_id": "r1"},
    )
    monkeypatch.setattr(
        repos_mod,
        "load_locked_us_watchlist",
        lambda trade_date, min_count=10, allow_degraded=False, timeout_sec=20: _make_rows(30),
    )

    result = pc_mod.check_us_prep_guard("2026-06-01")
    assert result["ok"] is True, f"ok 기대=True, 실제={result}"
    assert result["source"] == "db"
    assert result["trade_can_proceed"] is True
    assert result.get("final30_scored_count", 0) >= 10
    assert result.get("score_nonzero_count", 0) > 0


def test_db_fallback_fail_when_prep_status_unknown(monkeypatch):
    """prep_status=UNKNOWN이면 ok=False."""
    import trader.us.prep_contract as pc_mod
    import trader.us.db.repos as repos_mod

    monkeypatch.setattr(
        "trader.us.path_contract.load_us_prep_contract",
        lambda trade_date: None,
    )
    monkeypatch.setattr(
        repos_mod,
        "load_latest_us_prep_status",
        lambda trade_date, timeout_sec=20: {"status": "UNKNOWN", "run_id": ""},
    )
    monkeypatch.setattr(
        repos_mod,
        "load_locked_us_watchlist",
        lambda **kw: _make_rows(30),
    )

    result = pc_mod.check_us_prep_guard("2026-06-01")
    assert result["ok"] is True
    assert "UNKNOWN" in result["reason"]


def test_db_fallback_fail_when_locked_count_zero(monkeypatch):
    """locked rows=0이면 ok=False."""
    import trader.us.prep_contract as pc_mod
    import trader.us.db.repos as repos_mod

    monkeypatch.setattr(
        "trader.us.path_contract.load_us_prep_contract",
        lambda trade_date: None,
    )
    monkeypatch.setattr(
        repos_mod,
        "load_latest_us_prep_status",
        lambda trade_date, timeout_sec=20: {"status": "OK", "run_id": "r1"},
    )
    monkeypatch.setattr(
        repos_mod,
        "load_locked_us_watchlist",
        lambda **kw: [],
    )

    result = pc_mod.check_us_prep_guard("2026-06-01")
    assert result["ok"] is True
    assert "locked_watchlist_count" in result["reason"]


def test_db_fallback_fail_when_score_nonzero_zero(monkeypatch):
    """locked rows 30개이지만 score=0이면 ok=False."""
    import trader.us.prep_contract as pc_mod
    import trader.us.db.repos as repos_mod

    monkeypatch.setattr(
        "trader.us.path_contract.load_us_prep_contract",
        lambda trade_date: None,
    )
    monkeypatch.setattr(
        repos_mod,
        "load_latest_us_prep_status",
        lambda trade_date, timeout_sec=20: {"status": "OK", "run_id": "r1"},
    )
    # score=0인 rows
    zero_rows = [{"symbol": f"SYM{i}", "score": 0.0} for i in range(30)]
    monkeypatch.setattr(
        repos_mod,
        "load_locked_us_watchlist",
        lambda **kw: zero_rows,
    )

    result = pc_mod.check_us_prep_guard("2026-06-01")
    assert result["ok"] is True
    assert "score_nonzero" in result["reason"]


def test_file_contract_takes_priority(monkeypatch):
    """파일 contract가 있으면 DB fallback을 사용하지 않는다."""
    import trader.us.prep_contract as pc_mod

    file_contract = {
        "trade_date": "2026-06-01",
        "status": "OK",
        "contract_ok": True,
        "final30_scored_count": 30,
        "score_nonzero_count": 30,
        "trade_can_proceed": 1,
    }
    monkeypatch.setattr(
        "trader.us.path_contract.load_us_prep_contract",
        lambda trade_date: file_contract,
    )

    db_called = []

    def fake_db_fallback(trade_date):
        db_called.append(trade_date)
        return {"ok": False, "reason": "should_not_be_called"}

    monkeypatch.setattr(pc_mod, "_check_us_prep_guard_from_db", fake_db_fallback)

    result = pc_mod.check_us_prep_guard("2026-06-01")
    assert result["ok"] is True
    assert not db_called, "파일 contract가 있는데 DB fallback이 호출됐다"


def test_ok_with_warnings_also_passes_db_fallback(monkeypatch):
    """prep_status=OK_WITH_WARNINGS도 DB fallback에서 통과해야 한다."""
    import trader.us.prep_contract as pc_mod
    import trader.us.db.repos as repos_mod

    monkeypatch.setattr(
        "trader.us.path_contract.load_us_prep_contract",
        lambda trade_date: None,
    )
    monkeypatch.setattr(
        repos_mod,
        "load_latest_us_prep_status",
        lambda trade_date, timeout_sec=20: {"status": "OK_WITH_WARNINGS", "run_id": "r2"},
    )
    monkeypatch.setattr(
        repos_mod,
        "load_locked_us_watchlist",
        lambda **kw: _make_rows(30),
    )

    result = pc_mod.check_us_prep_guard("2026-06-01")
    assert result["ok"] is True
    assert result["source"] == "db"
