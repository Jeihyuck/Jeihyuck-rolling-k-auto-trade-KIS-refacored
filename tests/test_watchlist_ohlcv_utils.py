from __future__ import annotations

import pandas as pd

from trader.watchlist_builder import _normalize_ohlcv_columns
from trader.watchlist_ohlcv_utils import normalize_ohlcv_columns


def test_normalize_ohlcv_columns_matches_wrapper() -> None:
    df = pd.DataFrame(
        {
            " Date ": ["2026-03-24"],
            "Adj Close": [123.4],
            "High Price": [125.0],
            "Low Price": [122.0],
            "Acml Vol": [1000],
        }
    )

    expected = _normalize_ohlcv_columns(df)
    actual = normalize_ohlcv_columns(df)

    pd.testing.assert_frame_equal(actual, expected)


def test_normalize_ohlcv_columns_preserves_empty_inputs() -> None:
    empty = pd.DataFrame()

    expected = _normalize_ohlcv_columns(empty)
    actual = normalize_ohlcv_columns(empty)

    pd.testing.assert_frame_equal(actual, expected)
