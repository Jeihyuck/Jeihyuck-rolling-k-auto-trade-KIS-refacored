from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
import sqlalchemy as sa

from trader import pb1_runner, prep_runner
from trader.db.repos import CRITICAL_SCORED_COLS, WatchlistRepo, save_watchlist, verify_final30_scored_contract
from trader.db.schema import schema_for_engine


def _scored_member(idx: int) -> dict:
    code = f"{5930 + idx:06d}"
    breakout_score = 30.0 + (idx % 7)
    pullback_score = 18.0 + (idx % 5)
    momentum_score = 42.0 + (idx % 9)
    entry_style = "MOMENTUM"
    if idx % 3 == 0:
        entry_style = "BREAKOUT"
    elif idx % 3 == 1:
        entry_style = "PULLBACK"
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
        "breakout_score": breakout_score,
        "pullback_score": pullback_score,
        "momentum_score": momentum_score,
        "entry_style_selected": entry_style,
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
    assert int(df["ma20"].notna().sum()) == 30
    assert int(df["ma50"].notna().sum()) == 30
    assert int(df["ma150"].notna().sum()) == 30
    assert int(df["close"].notna().sum()) == 30
    assert int(df["atr_pct"].notna().sum()) == 30
    assert int(df["rs_percentile"].notna().sum()) == 30
    assert int(df["score_final"].notna().sum()) == 30
    assert ((pd.to_numeric(df["score"], errors="coerce") - pd.to_numeric(df["score_final"], errors="coerce")).abs().fillna(0.0) < 1e-9).all()
    assert ((pd.to_numeric(df["final_score"], errors="coerce") - pd.to_numeric(df["score_final"], errors="coerce")).abs().fillna(0.0) < 1e-9).all()


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


def test_scored_watchlist_load_restores_ma20_from_meta_alias() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    as_of = date(2026, 3, 11)
    payload = _scored_member(1)
    payload["ma20"] = None
    payload["meta"] = {**payload.get("meta", {}), "ma_20": "49000.5", "score_final": payload["score_final"]}

    with engine.begin() as conn:
        conn.execute(
            sa.insert(schema.pb1_watchlist),
            [{
                "env": "practice",
                "strategy": "pb1_watchlist_final_scored",
                "as_of": as_of,
                "code": payload["code"],
                "rank": payload["rank"],
                "score": payload["score_final"],
                "meta": payload["meta"],
            }],
        )

    repo = WatchlistRepo(engine)
    loaded, used_as_of = repo.load_watchlist_scored(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        allow_latest_fallback=False,
    )

    assert used_as_of == as_of
    assert len(loaded) == 1
    assert loaded[0]["ma20"] == 49000.5


def test_scored_contract_rank_warning_does_not_fail_verification() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = WatchlistRepo(engine)
    as_of = date(2026, 3, 11)
    members = []
    for idx in range(1, 31):
        member = _scored_member(idx)
        member["rank"] = 0
        member["rank_final30"] = 0
        members.append(member)

    repo.save_watchlist(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        members=members,
    )

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
    assert summary["null_critical"] == 0
    assert summary["rank_warn"] is True
    assert summary["rank_source"] == "synthetic_for_diag"
    assert summary["uniq_ranks"] == 30


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


def test_scored_watchlist_roundtrip_normalizes_score_to_score_final() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = WatchlistRepo(engine)
    as_of = date(2026, 3, 11)
    members = [_scored_member(i) for i in range(1, 31)]
    members[0]["score"] = 6515861196985.0
    members[0]["final_score"] = members[0]["score_final"]
    members[1]["score"] = 95087558410.0
    members[1]["final_score"] = members[1]["score_final"]

    repo.save_watchlist(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        members=members,
    )

    loaded, _ = repo.load_watchlist_scored(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        allow_latest_fallback=False,
    )

    df = pd.DataFrame(loaded)
    assert len(df) == 30
    assert ((pd.to_numeric(df["score"], errors="coerce") - pd.to_numeric(df["score_final"], errors="coerce")).abs().fillna(0.0) < 1e-9).all()
    assert ((pd.to_numeric(df["final_score"], errors="coerce") - pd.to_numeric(df["score_final"], errors="coerce")).abs().fillna(0.0) < 1e-9).all()
    assert float(df.loc[df["code"] == members[0]["code"], "score"].iloc[0]) == float(members[0]["score_final"])
    assert float(df.loc[df["code"] == members[1]["code"], "score"].iloc[0]) == float(members[1]["score_final"])


def test_prep_build_scored_members_prefers_score_final_over_corrupted_score() -> None:
    df = pd.DataFrame([_scored_member(1)])
    df.loc[0, "score"] = 6515861196985.0

    members = prep_runner._build_scored_members(df)

    assert len(members) == 1
    assert members[0]["score"] == members[0]["score_final"] == members[0]["final_score"]
    assert members[0]["meta"]["score"] == members[0]["score_final"]
    assert members[0]["meta"]["score_final"] == members[0]["score_final"]


def test_scored_watchlist_save_rejects_null_ma20_before_insert() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = WatchlistRepo(engine)
    as_of = date(2026, 3, 11)
    members = [_scored_member(i) for i in range(1, 31)]
    members[0]["ma20"] = None

    with pytest.raises(ValueError, match="FINAL30_SCORED_INVALID_BEFORE_DB_INSERT"):
        repo.save_watchlist(
            env="practice",
            strategy="pb1_watchlist_final_scored",
            as_of=as_of,
            members=members,
        )


def test_scored_watchlist_save_normalizes_meta_score_fields() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = WatchlistRepo(engine)
    as_of = date(2026, 3, 11)
    members = [_scored_member(i) for i in range(1, 31)]
    members[0]["score"] = 6515861196985.0
    members[0]["meta"]["score"] = 6515861196985.0

    repo.save_watchlist(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        members=members,
    )

    loaded, _ = repo.load_watchlist_scored(
        env="practice",
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        allow_latest_fallback=False,
    )

    assert loaded[0]["score"] == loaded[0]["score_final"] == loaded[0]["final_score"]
    assert loaded[0]["meta"]["score"] == loaded[0]["score_final"]
    assert loaded[0]["score_liq"] == members[0]["score_liq"]


def test_trade_loader_uses_db_scored_without_reject(tmp_path, monkeypatch, caplog) -> None:
    monkeypatch.chdir(tmp_path)

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            rows = [_scored_member(idx) for idx in range(1, 31)]
            return {
                "ok": True,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "rank_warn": False,
                "rank_source": "rank_final30",
                "null_critical": 0,
                "missing_fields": [],
                "columns": list(CRITICAL_SCORED_COLS) + ["code", "rank_final30", "score"],
                "rows_data": rows,
            }

        def load_watchlist_scored(self, *, strategy, **_kwargs):
            if strategy == "pb1_watchlist_final_scored":
                return [_scored_member(idx) for idx in range(1, 31)], date(2026, 3, 11)
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
