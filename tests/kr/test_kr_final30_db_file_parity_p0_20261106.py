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
