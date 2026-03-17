from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
import sqlalchemy as sa

from trader import pb1_runner
from trader.db.repos import CRITICAL_SCORED_COLS, WatchlistRepo, save_watchlist, verify_final30_scored_contract
from trader.db.schema import schema_for_engine


def _scored_member(idx: int) -> dict:
    code = f"{5930 + idx:06d}"
    return {
        "as_of": "2026-03-11",
        "code": code,
        "name": f"N{code}",
        "rank": idx,
        "rank_pool120": idx,
        "rank_top50": idx,
        "rank_final30": idx,
        "score": 30.0 + idx,
        "score_final": 30.0 + idx,
        "score_flow": 10.0,
        "score_liq": 10.0,
        "score_tech": 10.0,
        "tech_score": 31.0 + idx,
        "flow_score": 10.0,
        "final_score": 30.0 + idx,
        "breakout_score": 40.0,
        "pullback_score": 20.0,
        "momentum_score": 50.0,
        "entry_style_selected": "MOMENTUM",
        "entry_component": "momentum",
        "rs_pctile": 0.9,
        "rs_percentile": 0.9,
        "rs_score": 0.9,
        "vcp_score": 2.0,
        "trend_score": 75.0,
        "atr_pct": 0.02,
        "pullback_pct": 0.03,
        "foreign_20_ratio": 0.1,
        "inst_20_ratio": 0.2,
        "liq_avg": 1000000,
        "last_close": 50000,
        "close": 50000,
        "volume": 100000,
        "volume_avg20": 90000,
        "ma20": 49000,
        "ma50": 47000,
        "ma150": 43000,
        "rows": 220,
        "meta": {"source": "test"},
        "scores": {"final": 30.0 + idx},
        "reasons": ["ok"],
        "reject_reasons": [],
        "filters_passed": ["minervini"],
        "filters_failed": [],
    }


def test_scored_watchlist_roundtrip_preserves_critical_columns() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = WatchlistRepo(engine)
    as_of = date(2026, 3, 11)
    members = [_scored_member(i) for i in range(1, 31)]

    repo.save_watchlist(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        members=members,
    )

    loaded, used_as_of = repo.load_watchlist_scored(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        allow_latest_fallback=False,
    )

    assert used_as_of == as_of
    assert len(loaded) == 30
    df = pd.DataFrame(loaded)
    assert set(CRITICAL_SCORED_COLS).issubset(set(df.columns))

    summary = verify_final30_scored_contract(
        engine,
        env="practice",
        as_of=as_of,
        allow_latest_fallback=False,
        log_result=False,
    )
    assert summary["ok"] is True
    assert summary["rows"] == 30
    assert summary["uniq_codes"] == 30
    assert summary["uniq_ranks"] == 30
    assert summary["null_critical"] == 0


def test_universe_scored_roundtrip_preserves_critical_columns() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = WatchlistRepo(engine)
    as_of = date(2026, 3, 11)
    members = [_scored_member(i) for i in range(1, 6)]

    repo.save_watchlist(
        env="practice",
        strategy="pb1_universe_scored",
        as_of=as_of,
        members=members,
    )

    loaded, used_as_of = repo.load_watchlist_scored(
        env="practice",
        strategy="pb1_universe_scored",
        as_of=as_of,
        allow_latest_fallback=False,
    )

    assert used_as_of == as_of
    assert len(loaded) == 5
    df = pd.DataFrame(loaded)
    assert set(CRITICAL_SCORED_COLS).issubset(set(df.columns))


def test_scored_strategy_rejects_plain_serializer_shape() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    with pytest.raises(ValueError):
        save_watchlist(
            engine,
            env="practice",
            strategy="pb1_watchlist_final_scored",
            as_of=date(2026, 3, 11),
            members=[{"code": "005930", "rank": 1, "score": 1.0, "meta": {}}],
        )


def test_trade_loader_uses_db_scored_without_reject(tmp_path, monkeypatch, caplog) -> None:
    monkeypatch.chdir(tmp_path)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist_scored(self, *, strategy, **_kwargs):
            if strategy == "pb1_watchlist_final_scored":
                return [_scored_member(1)], date(2026, 3, 11)
            return [], None

        def load_watchlist(self, **_kwargs):
            return [], None

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    with caplog.at_level("INFO"):
        result = pb1_runner.load_trade_final30_scored(
            engine=object(),
            env="practice",
            as_of="2026-03-11",
        )

    assert result["source_name"] == "db_pb1_watchlist_final_scored"
    assert result["is_scored"] is True
    assert "[TRADE][FINAL30][LOAD_REJECT]" not in caplog.text
