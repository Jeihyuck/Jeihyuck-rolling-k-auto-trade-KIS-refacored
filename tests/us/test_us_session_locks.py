# -*- coding: utf-8 -*-
"""tests/us/test_us_session_locks.py

DB session_locks 기능 검증 (DB 없이 unit mock).
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, call

import pytest

from trader.us.db.session_locks import claim_us_session_lock, make_client_order_id


class TestMakeClientOrderId:
    def test_format(self):
        # 실제 구현: strategy/symbol/side/reason은 대문자, env/session은 소문자
        cid = make_client_order_id(
            trade_date="2026-05-01",
            env="practice",
            session="am",
            strategy="vcp",
            symbol="NVDA",
            side="BUY",
            reason="entry",
        )
        assert cid == "US:2026-05-01:practice:am:VCP:NVDA:BUY:ENTRY"

    def test_uniqueness_by_symbol(self):
        base = dict(
            trade_date="2026-05-01",
            env="practice",
            session="am",
            strategy="vcp",
            side="BUY",
            reason="entry",
        )
        a = make_client_order_id(**base, symbol="NVDA")
        b = make_client_order_id(**base, symbol="AAPL")
        assert a != b

    def test_uniqueness_by_session(self):
        base = dict(
            trade_date="2026-05-01",
            env="practice",
            strategy="vcp",
            symbol="NVDA",
            side="BUY",
            reason="entry",
        )
        a = make_client_order_id(**base, session="am")
        b = make_client_order_id(**base, session="afternoon")
        assert a != b

    def test_deterministic(self):
        kwargs = dict(
            trade_date="2026-05-01",
            env="practice",
            session="am",
            strategy="vcp",
            symbol="NVDA",
            side="BUY",
            reason="entry",
        )
        assert make_client_order_id(**kwargs) == make_client_order_id(**kwargs)

    def test_no_spaces(self):
        cid = make_client_order_id(
            trade_date="2026-05-01",
            env="practice",
            session="am",
            strategy="vcp",
            symbol="NVDA",
            side="BUY",
            reason="entry",
        )
        assert " " not in cid


class TestClaimLock:
    """claim_us_session_lock의 동작 contract 검증 (DB 없이 SQLAlchemy engine mock)."""

    def _mock_engine(self, fetchone_result):
        """SQLAlchemy engine.begin() context manager를 시뮬레이션하는 mock."""
        result = MagicMock()
        result.fetchone.return_value = fetchone_result
        conn = MagicMock()
        conn.execute.return_value = result
        conn.__enter__ = lambda s: conn
        conn.__exit__ = MagicMock(return_value=False)
        engine = MagicMock()
        engine.begin.return_value = conn
        return engine

    def test_claim_ok_returns_true(self, monkeypatch):
        """INSERT 성공 시 (True, None) 반환."""
        # fetchone이 row를 반환 → INSERT 성공
        row = MagicMock()
        engine = self._mock_engine(fetchone_result=row)

        import trader.us.db.session_locks as mod
        monkeypatch.setattr(mod, "_get_engine", lambda: engine)

        ok, existing = claim_us_session_lock(
            env="practice",
            trade_date="2026-05-01",
            session="am",
            github_run_id="run-001",
            github_workflow="us-trade-am",
            github_run_attempt="1",
        )
        assert ok is True

    def test_duplicate_returns_false(self, monkeypatch):
        """ON CONFLICT → fetchone=None → False 반환."""
        engine = self._mock_engine(fetchone_result=None)

        import trader.us.db.session_locks as mod
        monkeypatch.setattr(mod, "_get_engine", lambda: engine)

        existing_row = {"status": "DONE", "github_run_id": "run-001", "started_at": None}
        monkeypatch.setattr(mod, "_fetch_existing_lock", lambda *a, **kw: existing_row)

        ok, existing = claim_us_session_lock(
            env="practice",
            trade_date="2026-05-01",
            session="am",
            github_run_id="run-002",
            github_workflow="us-trade-am",
            github_run_attempt="1",
        )
        assert ok is False
