from trader.watchlist_builder import _infer_entry_style_from_scores, _normalize_entry_style_value
from trader.watchlist_entry_style import infer_entry_style_from_scores, normalize_entry_style_value


def test_normalize_entry_style_value_matches_wrapper():
    assert normalize_entry_style_value("entry_pullback") == _normalize_entry_style_value("entry_pullback")
    assert normalize_entry_style_value("momo") == "MOMENTUM"


def test_infer_entry_style_from_scores_matches_wrapper():
    row = {"breakout_score": 1, "pullback_score": 2, "momentum_score": 3}
    assert infer_entry_style_from_scores(row) == _infer_entry_style_from_scores(row)
    assert infer_entry_style_from_scores({}) == "MOMENTUM"
