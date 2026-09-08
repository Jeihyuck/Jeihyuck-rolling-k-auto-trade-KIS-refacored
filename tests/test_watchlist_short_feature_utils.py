from __future__ import annotations

import pandas as pd

from trader.watchlist_builder import _short_feature_null_count, _short_feature_sample_rows, _should_backfill_short_horizon_features
from trader.watchlist_short_feature_utils import (
    short_feature_null_count,
    short_feature_sample_rows,
    should_backfill_short_horizon_features,
)


def test_short_feature_helpers_match_wrappers() -> None:
    df = pd.DataFrame(
        {
            "code": ["000001", "000002"],
            "ma20": [1.0, None],
            "volume_avg20": [None, 5.0],
            "ma50": [3.0, 4.0],
            "close": [10.0, 11.0],
        }
    )

    pd.testing.assert_frame_equal(
        pd.DataFrame(short_feature_sample_rows(df, limit=1)),
        pd.DataFrame(_short_feature_sample_rows(df, limit=1)),
        check_dtype=False,
    )
    assert short_feature_null_count(df, "ma20") == _short_feature_null_count(df, "ma20")
    assert short_feature_null_count(df, "volume_avg20") == _short_feature_null_count(df, "volume_avg20")
    assert should_backfill_short_horizon_features(df, require_all_null=True) == _should_backfill_short_horizon_features(df, require_all_null=True)
    assert should_backfill_short_horizon_features(df, require_all_null=False) == _should_backfill_short_horizon_features(df, require_all_null=False)
