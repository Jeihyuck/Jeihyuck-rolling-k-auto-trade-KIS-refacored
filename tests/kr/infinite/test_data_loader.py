from trader.kr.infinite.data_loader import validate


def test_loader_deduplicates_and_rejects_invalid_ohlc():
    rows = [
        {"date": "2026-08-10", "open": 100, "high": 110, "low": 90, "close": 105, "volume": 10},
        {"date": "2026-08-10", "open": 101, "high": 111, "low": 91, "close": 106, "volume": 11},
        {"date": "2026-08-11", "open": -1, "high": 1, "low": 1, "close": 1, "volume": 1},
    ]
    clean, audit = validate(rows)
    assert len(clean) == 1 and clean[0]["close"] == 106
    assert audit["duplicate_count"] == 1 and audit["invalid_ohlc_count"] == 1
