import pandas as pd

from trader.data.ohlcv_provider import OHLCVResult
from trader.universe.build import _sanitize_members


class FakeOHLCVProvider:
    def __init__(self, rows_by_symbol: dict[str, int]) -> None:
        self.rows_by_symbol = rows_by_symbol

    def get_ohlcv(self, symbol: str, days: int) -> OHLCVResult:
        rows = self.rows_by_symbol.get(symbol, 0)
        df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=rows, freq="D")})
        return OHLCVResult(df, {"rows": rows, "insufficient_candles": rows < days})


def test_universe_sanitize_filters_invalid_and_insufficient():
    members = [
        {"code": "005930", "market": "KOSPI"},
        {"code": "0009K0", "market": "KOSDAQ"},
        {"code": "1234", "market": "KOSDAQ"},
    ]
    provider = FakeOHLCVProvider({"005930": 30, "001234": 10})

    sanitized, stats = _sanitize_members(members, ohlcv_provider=provider, min_candles=20)

    assert [m["code"] for m in sanitized] == ["005930"]
    assert stats["invalid_format"] == 1
    assert stats["insufficient_history"] == 1
    assert stats["final"] == 1
