"""P0 PREP DB/file parity: strategy, rank and score cannot silently diverge."""

from copy import deepcopy

import pytest

from trader.prep_runner import assert_kr_final30_db_file_identity


def rows():
    return [
        {
            "code": f"{i:06d}",
            "rank": i,
            "rank_final30": i,
            "score_final": 80 - i / 10,
            "entry_style_selected": "ENTRY_MOMENTUM" if i % 3 == 0 else "ENTRY_PULLBACK",
            "market": "KOSPI",
        }
        for i in range(1, 31)
    ]


def test_identical_semantic_rows_allow_different_order():
    source = rows()
    assert_kr_final30_db_file_identity(source, list(reversed(deepcopy(source))), source="runtime")


@pytest.mark.parametrize(
    "change,expected",
    [
        (lambda x: x[0].update(entry_style_selected="ENTRY_BREAKOUT"), "CONTRACT_MISMATCH"),
        (lambda x: x[0].update(rank_final30=30), "INVALID_IDENTITY"),
        (lambda x: x[0].update(code="999999"), "CODES_MISMATCH"),
        (lambda x: x[0].update(score_final=0.2), "CONTRACT_MISMATCH"),
        (lambda x: x[0].pop("entry_style_selected"), "INVALID_IDENTITY"),
    ],
)
def test_rejects_file_or_db_contract_divergence(change, expected):
    db = rows()
    mirror = deepcopy(db)
    change(mirror)
    with pytest.raises(RuntimeError, match=expected):
        assert_kr_final30_db_file_identity(db, mirror, source="runtime")


def test_rejects_missing_db_row_even_when_file_is_complete():
    with pytest.raises(RuntimeError, match="COUNT"):
        assert_kr_final30_db_file_identity(rows()[:-1], rows(), source="ledger")



def test_rejects_20261105_production_shape_rank_zero_and_033790_style_null():
    """Read-only observed 2026-11-05 incident: 30 codes but zero meta ranks.

    The 2026-11-05 authoritative DB rows had rank outside 1..30 and all
    meta.rank_final30=0; 033790 (rank 44) had no selected entry style.
    Never invent ENTRY_MOMENTUM from its entry_component=momentum.
    """
    db = rows()
    for index, row in enumerate(db):
        row["rank"] = index + 6
        row["rank_final30"] = 0.0
        row["meta"] = {
            "rank_final30": 0.0,
            "entry_style_selected": row["entry_style_selected"],
        }
    db[21]["code"] = "033790"
    db[21]["rank"] = 44
    db[21]["entry_style_selected"] = None
    db[21]["meta"]["entry_style_selected"] = None
    db[21]["entry_component"] = "momentum"
    with pytest.raises(RuntimeError, match="INVALID_IDENTITY"):
        assert_kr_final30_db_file_identity(db, deepcopy(db), source="production_20261105")


def test_no_style_fallback_from_entry_component():
    db = rows()
    mirror = deepcopy(db)
    db[0]["entry_style_selected"] = None
    db[0]["entry_component"] = "momentum"
    mirror[0]["entry_style_selected"] = None
    mirror[0]["entry_component"] = "momentum"
    with pytest.raises(RuntimeError, match="INVALID_IDENTITY"):
        assert_kr_final30_db_file_identity(db, mirror, source="runtime")



def test_auxiliary_writer_does_not_overwrite_final30_with_candidate_rank():
    from trader.watchlist_builder import canonicalize_final30_aux_db_payload

    initial = rows()
    for i, row in enumerate(initial, 1):
        row["rank"] = i + 5
        row["rank_final30"] = 0.0
        row["entry_style_selected"] = "MOMENTUM" if i % 3 == 0 else "PULLBACK"
        row["meta"] = {"rank_final30": 0.0, "entry_style_selected": row["entry_style_selected"]}
    source = deepcopy(initial)
    canonical = canonicalize_final30_aux_db_payload(source, source="test_candidate_rank")
    assert [x["rank"] for x in canonical] == list(range(1, 31))
    assert [x["rank_final30"] for x in canonical] == list(range(1, 31))
    assert [x["meta"]["rank_final30"] for x in canonical] == list(range(1, 31))
    assert all(x["entry_style_selected"] == y["entry_style_selected"]
               for x, y in zip(canonical, initial))
    assert [x["rank"] for x in source] == [i + 5 for i in range(1, 31)]
    assert_kr_final30_db_file_identity(canonical, deepcopy(canonical), source="aux_db")


def test_auxiliary_writer_refuses_missing_style_even_if_component_is_momentum():
    from trader.watchlist_builder import canonicalize_final30_aux_db_payload

    data = rows()
    for x in data:
        x["entry_style_selected"] = "PULLBACK"
    data[21]["code"] = "033790"
    data[21]["entry_style_selected"] = None
    data[21]["entry_component"] = "momentum"
    with pytest.raises(RuntimeError, match="STYLE_NOT_SELECTED.*033790"):
        canonicalize_final30_aux_db_payload(data, source="20261105")


def test_aux_and_cache_db_writers_both_apply_new_final30_contract():
    import inspect
    from trader.watchlist_builder import build_and_save_watchlist

    source = inspect.getsource(build_and_save_watchlist)
    assert source.count("canonicalize_final30_aux_db_payload(") >= 2
    assert "CACHE_DIRECT_DB_SAVE" in source
    assert "BUILDER_AUX_DB_SAVE" in source


def test_core_parity_rejects_unrecognized_style_even_when_both_sources_agree():
    db = rows()
    db[0]["entry_style_selected"] = "UNKNOWN_STRATEGY"
    with pytest.raises(RuntimeError, match="INVALID_IDENTITY"):
        assert_kr_final30_db_file_identity(db, deepcopy(db), source="file")


def test_aux_writer_normalizes_proven_entry_style_alias_without_inference():
    from trader.watchlist_builder import canonicalize_final30_aux_db_payload
    data = rows()
    out = canonicalize_final30_aux_db_payload(data, source="source_entry_contract")
    assert out[0]["entry_style_selected"] == "PULLBACK"
    assert out[2]["entry_style_selected"] == "MOMENTUM"
    assert all(out[i]["rank_final30"] == i+1 for i in range(30))


def test_sqlite_actual_repo_core_then_aux_db_roundtrip_and_file_parity(tmp_path, monkeypatch):
    """Two consecutive real repository upserts must preserve exact Final30.

    Replays the 11/05 candidate-rank overwrite shape with a disposable DB:
    30 source rows -> canonical CORE save -> AUX overwrite -> exact DB
    reload -> file mirror identity. No KIS/network/production database.
    """
    from datetime import date

    import sqlalchemy as sa

    from trader.db.repos import WatchlistRepo
    from trader.db.schema import schema_for_engine
    from trader.watchlist_builder import canonicalize_final30_aux_db_payload

    monkeypatch.setenv("PB1_WATCHLIST_SAVE_ALIAS_LEGACY", "0")
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'final30.db'}")
    try:
        schema_for_engine(engine).metadata.create_all(engine)
        repo = WatchlistRepo(engine)
        as_of = date(2026, 11, 5)
        scored = rows()
        canonical_core = canonicalize_final30_aux_db_payload(
            scored, source="CORE_DB_UPSERT",
        )
        repo.save_watchlist(
            env="practice", strategy="pb1_watchlist_final_scored",
            as_of=as_of, members=canonical_core,
        )
        candidate_ranked = deepcopy(scored)
        for i, row in enumerate(candidate_ranked, 1):
            row["rank"] = i + 5
            row["rank_final30"] = 0.0
            row["meta"] = {"rank_final30": 0.0,
                           "entry_style_selected": row["entry_style_selected"]}
        canonical_aux = canonicalize_final30_aux_db_payload(
            candidate_ranked, source="AUX_AFTER_CORE_DB_UPSERT",
        )
        repo.save_watchlist(
            env="practice", strategy="pb1_watchlist_final_scored",
            as_of=as_of, members=canonical_aux,
        )
        saved, used_as_of = repo.load_watchlist_scored(
            env="practice", strategy="pb1_watchlist_final_scored",
            as_of=as_of, allow_latest_fallback=False,
            require_exact_rows=30, require_scored=False,
        )
        assert used_as_of == as_of
        assert len(saved) == 30
        assert_kr_final30_db_file_identity(saved, canonical_aux, source="final30.json")
        assert_kr_final30_db_file_identity(
            saved, list(reversed(deepcopy(canonical_core))), source="bundle",
        )
        with engine.connect() as conn:
            persisted = conn.execute(
                sa.select(schema_for_engine(engine).pb1_watchlist).where(
                    schema_for_engine(engine).pb1_watchlist.c.env == "practice",
                    schema_for_engine(engine).pb1_watchlist.c.strategy == "pb1_watchlist_final_scored",
                    schema_for_engine(engine).pb1_watchlist.c.as_of == as_of,
                )
            ).mappings().all()
        assert sorted(int(x["rank"]) for x in persisted) == list(range(1, 31))
        assert sorted(int(x["meta"]["rank_final30"]) for x in persisted) == list(range(1, 31))
    finally:
        engine.dispose()
