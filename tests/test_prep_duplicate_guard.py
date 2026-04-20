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


def test_duplicate_prep_upserts_prep_done_event(monkeypatch, capsys) -> None:
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
        lambda **_kwargs: {"as_of": "2026-04-17", "trade_date": "2026-04-20"},
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
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.delenv("ALLOW_DUPLICATE_PREP", raising=False)

    exit_code = module.main()
    stdout = capsys.readouterr().out

    repo = LedgerEventsRepo(engine)
    event = repo.get_prep_event(
        env="practice",
        strategy="pb1",
        as_of="2026-04-17",
        trade_date="2026-04-20",
    )

    assert exit_code == 0
    assert event is not None
    assert event["payload_json"]["status"] == "READY_FROM_CANONICAL"
    assert event["payload_json"]["reason"] == "canonical_prep_already_ready"
    assert event["payload_json"]["trade_can_proceed"] == 1
    assert "[PREP][DUPLICATE_GUARD][LEDGER_UPSERT] event=PREP_DONE status=READY_FROM_CANONICAL" in stdout
    assert "[RUN_SUMMARY][RESULT] status=SKIP_DUPLICATE_PREP reason=canonical_prep_already_ready event=schedule" in stdout