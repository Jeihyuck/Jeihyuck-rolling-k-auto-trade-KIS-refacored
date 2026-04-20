from __future__ import annotations

from datetime import date

import sqlalchemy as sa

from trader import pb1_runner
from trader.constants import REQUIRED_FINAL30_SCORED_COLS
from trader.db.repos import LedgerEventsRepo
from trader.db.schema import schema_for_engine


class _FallbackWatchlistRepo:
    def __init__(self, summary: dict):
        self._summary = summary

    def verify_watchlist_scored_contract(self, **_kwargs):
        return dict(self._summary)


def test_trade_prep_done_check_fallback_to_final30_when_ledger_missing(caplog) -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    repo = LedgerEventsRepo(engine)
    summary = {
        "rows": 30,
        "ok": True,
        "columns": list(REQUIRED_FINAL30_SCORED_COLS),
        "env": "practice",
        "strategy": "pb1_watchlist_final_scored",
        "as_of": "2026-04-17",
    }

    with caplog.at_level("INFO", logger="trader.pb1_runner"):
        prep_done, source, _, _ = pb1_runner._resolve_trade_prep_done_status(
            ledger_repo=repo,
            watchlist_repo=_FallbackWatchlistRepo(summary),
            env="practice",
            strategy="pb1",
            as_of=date(2026, 4, 17),
            trade_date=date(2026, 4, 20),
        )

    event = repo.get_prep_event(
        env="practice",
        strategy="pb1",
        as_of="2026-04-17",
        trade_date="2026-04-20",
    )

    assert prep_done is True
    assert source == "final30_canonical_fallback"
    assert event is not None
    assert event["payload_json"]["status"] == "READY_FROM_FINAL30_FALLBACK"
    assert event["payload_json"]["reason"] == "ledger_missing_but_final30_canonical_ok"
    assert "[TRADE_TICK][PREP_DONE_CHECK][FALLBACK_CANONICAL_OK] ledger_prep_done=0 fallback_prep_done=1 final30_count=30" in caplog.text
    assert "[TRADE_TICK][PREP_DONE_CHECK] source=final30_canonical_fallback prep_done=1" in caplog.text


def test_trade_prep_done_check_does_not_fallback_when_final30_invalid(caplog) -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    repo = LedgerEventsRepo(engine)
    summary = {
        "rows": 29,
        "ok": False,
        "columns": [col for col in REQUIRED_FINAL30_SCORED_COLS if col != "ma20"],
        "env": "practice",
        "strategy": "pb1_watchlist_final_scored",
        "as_of": "2026-04-17",
    }

    with caplog.at_level("WARNING", logger="trader.pb1_runner"):
        prep_done, source, _, details = pb1_runner._resolve_trade_prep_done_status(
            ledger_repo=repo,
            watchlist_repo=_FallbackWatchlistRepo(summary),
            env="practice",
            strategy="pb1",
            as_of=date(2026, 4, 17),
            trade_date=date(2026, 4, 20),
        )

    event = repo.get_prep_event(
        env="practice",
        strategy="pb1",
        as_of="2026-04-17",
        trade_date="2026-04-20",
    )

    assert prep_done is False
    assert source == "none"
    assert details["final30_count"] == 29
    assert event is None
    assert "[TRADE_TICK][PREP_DONE_CHECK][FAIL] reason=PREP_NOT_DONE ledger_prep_done=0 final30_count=29" in caplog.text