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
            "종가": [11, 12],
            "거래량": [1000, 2000],
        }
    )

    norm, meta = normalize_ohlcv(df)

    assert "volume" in norm.columns
    assert meta["volume_missing"] is False
    assert norm["volume"].tolist() == [1000, 2000]
