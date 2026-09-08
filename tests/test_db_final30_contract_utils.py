from trader.db.final30_contract_utils import (
    field_null_counts,
    normalize_final30_score_fields,
    roundtrip_mismatch_counts,
    roundtrip_value_matches,
)
from trader.db.repos import (
    _field_null_counts,
    _normalize_final30_score_fields,
    _roundtrip_mismatch_counts,
    _roundtrip_value_matches,
)


def test_db_final30_contract_utils_match_wrappers():
    rows = [{"code": "000001", "score": "1", "meta": {}}]
    loaded = [{"code": "000001", "score_final": "1", "meta": {}}]
    assert _field_null_counts(rows, ["score"]) == field_null_counts(rows, ["score"])
    assert _normalize_final30_score_fields(dict(rows[0])) == normalize_final30_score_fields(dict(rows[0]))
    assert _roundtrip_value_matches("1", 1.0) == roundtrip_value_matches("1", 1.0)
    assert _roundtrip_mismatch_counts(rows, loaded, fields=["score"]) == roundtrip_mismatch_counts(rows, loaded, fields=["score"])
