from trader.db.repos import _merge_json_dict, _restore_numeric_from_sources, _safe_float_or_none
from trader.db.value_utils import merge_json_dict, restore_numeric_from_sources, safe_float_or_none


def test_db_value_helpers_match_wrappers() -> None:
    base = {"a": 1, "nested": {"x": 1}}
    incoming = {"b": 2, "nested": {"y": 2}}
    assert merge_json_dict(base, incoming) == _merge_json_dict(base, incoming)
    assert merge_json_dict(None, incoming) == _merge_json_dict(None, incoming)
    assert merge_json_dict(base, None) == _merge_json_dict(base, None)

    assert safe_float_or_none(None) is _safe_float_or_none(None)
    assert safe_float_or_none(1) == _safe_float_or_none(1)
    assert safe_float_or_none("1.5") == _safe_float_or_none("1.5")
    assert safe_float_or_none("") is _safe_float_or_none("")

    sources = ({"score": "3.25"}, {"score": 7.0})
    assert restore_numeric_from_sources(*sources, aliases=("score",)) == _restore_numeric_from_sources(*sources, aliases=("score",))
