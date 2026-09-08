from trader.watchlist_builder import _is_ma20_candidate_column, _ma20_candidate_priority, _normalize_column_token
from trader.watchlist_column_utils import (
    is_ma20_candidate_column,
    ma20_candidate_priority,
    normalize_column_token,
)


def test_column_token_helpers_match_wrappers():
    tokens = ["MA20", "ma_20", "avg20", "close ma20"]
    for token in tokens:
        assert normalize_column_token(token) == _normalize_column_token(token)
        assert is_ma20_candidate_column(token) == _is_ma20_candidate_column(token)
        assert ma20_candidate_priority(token) == _ma20_candidate_priority(token)
