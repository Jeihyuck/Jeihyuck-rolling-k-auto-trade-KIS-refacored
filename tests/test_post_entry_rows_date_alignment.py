import pandas as pd

from trader.pb1_engine import _compute_highest_since_entry
from trader.position_age import calc_position_age


def test_post_entry_rows_date_alignment_uses_trade_date_not_raw_timestamp():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-04-22", "2026-04-23", "2026-04-24", "2026-05-04"]),
            "high": [101.0, 103.0, 104.0, 110.0],
            "close": [100.0, 102.0, 103.0, 109.0],
        }
    )

    age = calc_position_age("2026-04-22T15:15:00+00:00", "2026-05-04", df)
    highest, post_entry_rows = _compute_highest_since_entry(df, "2026-04-22T15:15:00+00:00", 100.0)

    assert age.post_entry_rows > 0
    assert age.holding_bars > 0
    assert post_entry_rows > 0
    assert highest == 110.0