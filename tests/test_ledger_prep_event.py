from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

from trader.db.repos import LedgerEventsRepo
from trader.db.schema import schema_for_engine


def _load_script_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_ledger_events_repo_has_upsert_prep_event() -> None:
    assert hasattr(LedgerEventsRepo, "upsert_prep_event")


def test_upsert_prep_event_appends_prep_done(monkeypatch) -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    repo = LedgerEventsRepo(engine)
    captured: dict[str, object] = {}

    def fake_append_event(**kwargs):
        captured.update(kwargs)
        return "evt-1"

    monkeypatch.setattr(repo, "append_event", fake_append_event)

    event_id = repo.upsert_prep_event(
        env="practice",
        strategy="pb1",
        as_of="2026-04-20",
        trade_date="2026-04-21",
        event_type="PREP_DONE",
        status="READY_FROM_CANONICAL",
        reason="canonical_prep_already_ready",
        final30_count=30,
        quality_ok=True,
        trade_can_proceed=True,
    )

    assert event_id == "evt-1"
    assert captured["event_type"] == "PREP_DONE"
    assert captured["payload_json"]["as_of"] == "2026-04-20"
    assert captured["payload_json"]["trade_date"] == "2026-04-21"
    assert captured["payload_json"]["status"] == "READY_FROM_CANONICAL"
    assert captured["payload_json"]["trade_can_proceed"] == 1


def test_upsert_prep_event_skip_existing(monkeypatch) -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    repo = LedgerEventsRepo(engine)

    existing_id = repo.append_event(
        env="practice",
        run_id=None,
        strategy="pb1",
        run_window="prep",
        event_type="PREP_DONE",
        ts=pd.Timestamp("2026-04-20T08:50:00+09:00").to_pydatetime(),
        ok=True,
        reasons=["canonical_prep_already_ready"],
        stage="prep_duplicate_guard",
        payload_json={
            "env": "practice",
            "strategy": "pb1",
            "as_of": "2026-04-20",
            "trade_date": "2026-04-21",
            "event_type": "PREP_DONE",
            "status": "READY_FROM_CANONICAL",
            "reason": "canonical_prep_already_ready",
            "final30_count": 30,
            "quality_ok": 1,
            "trade_can_proceed": 1,
        },
    )

    def fail_append_event(**_kwargs):
        raise AssertionError("append_event should not be called for duplicate prep event")

    monkeypatch.setattr(repo, "append_event", fail_append_event)

    event_id = repo.upsert_prep_event(
        env="practice",
        strategy="pb1",
        as_of="2026-04-20",
        trade_date="2026-04-21",
        event_type="PREP_DONE",
        status="READY_FROM_CANONICAL",
        reason="canonical_prep_already_ready",
        final30_count=30,
        quality_ok=True,
        trade_can_proceed=True,
    )

    assert event_id == existing_id


def test_check_prep_duplicate_guard_canonical_ok_exits_zero(monkeypatch) -> None:
    module = _load_script_module(
        "check_prep_duplicate_guard",
        Path(__file__).resolve().parents[1] / "scripts" / "check_prep_duplicate_guard.py",
    )
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    monkeypatch.setattr(module, "get_engine", lambda: engine)
    monkeypatch.setattr(
        module,
        "resolve_trade_context",
        lambda **_kwargs: {"as_of": "2026-04-20", "trade_date": "2026-04-21"},
    )
    monkeypatch.setattr(
        module,
        "_load_final30_snapshot",
        lambda **_kwargs: (
            pd.DataFrame([{"code": f"{idx:06d}"} for idx in range(30)]),
            {"status": "OK", "quality_ok": 1, "trade_can_proceed": 1},
        ),
    )
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("DB_STORE_REQUIRED", "0")

    assert module.main() == 0