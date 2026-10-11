from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
import sqlalchemy as sa

from trader import pb1_runner, prep_runner
from trader.db.repos import (
    CRITICAL_SCORED_COLS,
    ScoredWatchlistInvalidError,
    ScoredWatchlistNotFoundError,
    WatchlistRepo,
    save_watchlist,
    verify_final30_scored_contract,
)
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
        "market": "KOSPI" if idx % 2 else "KOSDAQ",
        "market_code": "KOSPI" if idx % 2 else "KOSDAQ",
        "rs_benchmark": "069500" if idx % 2 else "229200",
        "return_1d": 0.01, "return_5d": 0.03,
        "above_ma20": True, "above_ma50": True,
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
    assert set(df["market"]) == {"KOSPI", "KOSDAQ"}
    assert df["return_1d"].notna().all() and df["return_5d"].notna().all()
    assert (df["volume_avg20"] > 0).all()
    assert set(df["rs_benchmark"]) == {"069500", "229200"}

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


def test_load_watchlist_scored_strict_missing_rows_raises() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = WatchlistRepo(engine)

    with pytest.raises(ScoredWatchlistNotFoundError, match="scored_final30_missing"):
        repo.load_watchlist_scored(
            env="practice",
            strategy="pb1_watchlist_final_scored",
            as_of=date(2026, 3, 11),
            allow_latest_fallback=False,
            require_exact_rows=30,
            require_scored=True,
            fail_if_missing=True,
        )


def test_load_watchlist_scored_strict_strategy_mismatch_raises() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = WatchlistRepo(engine)

    with pytest.raises(ScoredWatchlistInvalidError, match="strategy_mismatch"):
        repo.load_watchlist_scored(
            env="practice",
            strategy="best_k_meta",
            as_of=date(2026, 3, 11),
            allow_latest_fallback=False,
            require_exact_rows=30,
            require_scored=True,
            fail_if_missing=True,
        )


def test_load_watchlist_scored_strict_missing_cols_raises() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    as_of = date(2026, 3, 11)
    broken_rows = [
        {
            "env": "practice",
            "strategy": "pb1_watchlist_final_scored",
            "as_of": as_of,
            "code": f"{7000 + idx:06d}",
            "rank": idx,
            "score": float(idx),
            "meta": {"code": f"{7000 + idx:06d}", "ma20": 100.0 + idx},
        }
        for idx in range(1, 31)
    ]
    with engine.begin() as conn:
        conn.execute(schema.pb1_watchlist.insert(), broken_rows)

    repo = WatchlistRepo(engine)

    with pytest.raises(ScoredWatchlistInvalidError, match="required_scored_cols_missing"):
        repo.load_watchlist_scored(
            env="practice",
            strategy="pb1_watchlist_final_scored",
            as_of=as_of,
            allow_latest_fallback=False,
            require_exact_rows=30,
            require_scored=True,
            fail_if_missing=True,
        )


def test_real_supabase_breakout_proof_persists_through_scored_db_to_pb1_buy(monkeypatch):
    """SQLite replica: true Oct8 OHLCV evidence must survive locked Final30."""
    import json
    from pathlib import Path
    from trader.kr_four_family_candidate_admission import completed_daily_candidate_proofs, completed_breakout_evidence
    from trader.pb1_engine import PB1Engine

    fixture = json.loads(
        (Path(__file__).parent / "kr/fixtures" / "supabase_kr_breakout_63bars_20261008.json").read_text()
    )
    bars = fixture["samples"]["083450"]
    df = pd.DataFrame(bars, columns=["date", "close", "high", "low", "volume"])
    for key in ("close", "high", "low", "volume"):
        df[key] = df[key].astype(float)
    proof = completed_breakout_evidence(df, expected_as_of="2026-10-08")
    assert completed_daily_candidate_proofs(df, expected_as_of="2026-10-08")["BREAKOUT"]

    member = _scored_member(1)
    member.update({
        "code": "083450", "as_of": "2026-10-08", "rank": 1,
        "rank_final30": 1, "entry_style_selected": "BREAKOUT",
        "breakout_score": 100.0, "score_final": 86., "score": 86.,
        "final_score": 86., "rs_percentile": .969,
        "close": proof["close"], "last_close": proof["close"],
        "volume": proof["volume"], "volume_avg20": proof["average_volume20"],
        "candidate_family_screens": {"PULLBACK": False, "MOMENTUM": True, "BREAKOUT": True, "VCP": False},
        "candidate_family_proof_as_of": "2026-10-08",
        "candidate_breakout_evidence": proof,
        "breakout_completed_proof_valid": True,
        "breakout_score_source": "completed_daily_pb1_55d",
        "breakout_pivot_price": proof["pivot55"], "pivot": proof["pivot55"],
    })
    member["meta"] = {
        **member["meta"],
        **{key: member[key] for key in (
            "candidate_family_screens", "candidate_family_proof_as_of", "candidate_breakout_evidence",
            "breakout_completed_proof_valid", "breakout_score_source",
            "breakout_pivot_price", "breakout_score",
        )},
    }

    engine_db = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine_db).metadata.create_all(engine_db)
    repo = WatchlistRepo(engine_db)
    # The real strict Final30 contract requires EXACTLY 30, not one row.
    # Fill the other 29 with synthetic locked, internally valid rows while
    # preserving one genuine 63-bar Supabase Breakout proof unchanged.
    members = [_scored_member(i) for i in range(2, 31)]
    for other in members:
        other["as_of"] = "2026-10-08"
    members.insert(0, member)
    repo.save_watchlist(
        env="practice", strategy="pb1_watchlist_final_scored",
        as_of=date(2026, 10, 8), members=members,
    )
    stored, loaded_asof = repo.load_watchlist_scored(
        env="practice", strategy="pb1_watchlist_final_scored",
        as_of=date(2026, 10, 8), allow_latest_fallback=False,
    )
    assert loaded_asof == date(2026, 10, 8)
    assert len(stored) == 30
    saved = next(row for row in stored if row["code"] == "083450")
    assert saved["breakout_score"] == 100.0
    assert saved["candidate_family_screens"]["BREAKOUT"] is True
    assert saved["candidate_family_proof_as_of"] == "2026-10-08"
    assert saved["breakout_completed_proof_valid"] is True
    assert saved["breakout_score_source"] == "completed_daily_pb1_55d"

    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "1")
    engine = PB1Engine.__new__(PB1Engine)
    engine.require_volume = False
    engine.env = "practice"
    engine._precomputed_final30_map = {"083450": saved}
    engine._precomputed_derived_map = {}
    engine._precomputed_universe_map = {}
    mapped, checks, missing, valid_row, data_ok = engine._map_precomputed_candidate_row("083450")
    assert mapped["breakout_completed_proof_valid"] is True, missing
    assert mapped["candidate_family_screens"]["BREAKOUT"] is True
    allowed, reasons, contract = engine._evaluate_final30_entry_setup("083450", mapped, market="KOSPI")
    assert allowed is True, reasons
    assert contract["entry_reason"] == "ENTRY_BREAKOUT"
