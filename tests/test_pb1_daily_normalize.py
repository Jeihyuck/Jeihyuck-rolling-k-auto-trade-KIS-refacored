import numpy as np
import pandas as pd

from trader.utils.ohlcv import normalize_ohlcv


def test_normalize_adds_volume_nan_when_missing():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "open": [1, 2, 3],
            "high": [2, 3, 4],
            "low": [0.5, 1.5, 2.5],
            "close": [1.5, 2.5, 3.5],
        }
    )

    norm, meta = normalize_ohlcv(df)

    assert "volume" in norm.columns
    assert np.isnan(norm["volume"]).all()
    assert meta["volume_missing"] is True


def test_normalize_maps_alternative_volume_column():
    df = pd.DataFrame(
        {
            "날짜": pd.date_range("2024-01-01", periods=2, freq="D"),
            "시가": [10, 11],
            "고가": [12, 13],
            "저가": [9, 10],
            "종가": ["11", "12"],
            "거래량": ["1,000", "2,345,678"],
        }
    )

    norm, meta = normalize_ohlcv(df)

    assert "volume" in norm.columns
    assert meta["volume_missing"] is False
    assert norm["volume"].tolist() == [1000, 2345678]
    assert norm["close"].tolist() == [11.0, 12.0]


def test_normalize_handles_comma_separated_numeric_columns():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=2, freq="D"),
            "open": ["1,000", "2,000"],
            "high": ["1,500", "2,500"],
            "low": ["900", "1,900"],
            "close": ["1,250", "2,200"],
            "volume": ["1,000", "2,000"],
        }
    )

    norm, meta = normalize_ohlcv(df)

    assert meta["volume_missing"] is False
    assert norm["volume"].tolist() == [1000, 2000]
    assert norm["close"].tolist() == [1250.0, 2200.0]
