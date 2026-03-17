from __future__ import annotations

import argparse
from datetime import date

from scripts import check_prep_readiness


class _DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyEngine:
    def connect(self):
        return _DummyConn()


def test_readiness_stays_ready_when_db_is_usable_and_only_warn_conditions_exist(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    resolved_as_of = date(2026, 3, 16)

    monkeypatch.setattr(
        check_prep_readiness,
        "_parse_args",
        lambda: argparse.Namespace(
            env="practice",
            as_of="2026-03-16",
            run_date="2026-03-17",
            window="outside_intraday",
            exchange="KRX",
        ),
    )
    monkeypatch.setattr(check_prep_readiness, "make_engine", lambda: _DummyEngine())
    monkeypatch.setattr(check_prep_readiness, "schema_for_engine", lambda _engine: object())
    monkeypatch.setattr(check_prep_readiness, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        check_prep_readiness,
        "resolve_trade_readiness_as_of",
        lambda **_kwargs: {
            "resolved_as_of": resolved_as_of,
            "calendar_prev": resolved_as_of,
            "reason": "outside_intraday_use_prev_close",
        },
    )
    monkeypatch.setattr(
        check_prep_readiness,
        "_count_watchlist_rows",
        lambda _conn, _schema, *, strategy, **_kwargs: 30 if strategy in {"pb1_watchlist_final", "pb1_watchlist_final_scored"} else 0,
    )

    class _FakeLedgerRepo:
        def __init__(self, _engine):
            pass

        def prep_done_status(self, *, env, as_of):
            assert env == "practice"
            assert as_of == resolved_as_of
            return True, 2

    class _FakeDerivedRepo:
        def __init__(self, _engine):
            pass

        def count_as_of(self, *, env, as_of):
            assert env == "practice"
            assert as_of == resolved_as_of
            return 196

    class _FakeWatchlistRepo:
        def __init__(self, _engine):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            return {
                "ok": True,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "rank_warn": True,
                "rank_source": "synthetic_for_diag",
                "null_critical": 0,
                "missing_fields": [],
                "columns": list(check_prep_readiness.CRITICAL_SCORED_COLS),
            }

    monkeypatch.setattr(check_prep_readiness, "LedgerEventsRepo", _FakeLedgerRepo)
    monkeypatch.setattr(check_prep_readiness, "DerivedMinerviniRepo", _FakeDerivedRepo)
    monkeypatch.setattr(check_prep_readiness, "WatchlistRepo", _FakeWatchlistRepo)

    exit_code = check_prep_readiness.main()
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert "[PREP][READINESS][DB]" in stdout
    assert "db_contract_ok=1" in stdout
    assert "rank_contract_warn=1" in stdout
    assert "[PREP][READINESS][FILES] runtime=0 ledger=0 signals=0 file_contract_ok=0 warn_only=1" in stdout
    assert "[PREP][READINESS] status=ready grade=warn reason=DB_READY_FILE_MISSING_OR_RANK_WARN" in stdout