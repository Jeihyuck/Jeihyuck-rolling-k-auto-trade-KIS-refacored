import pandas as pd
import pytest

from trader.pb1_runner import _guard_empty_final30_for_engine_boot


def test_empty_final30_blocks_non_close_engine_boot():
    with pytest.raises(RuntimeError, match="TRADE_FINAL30_EMPTY"):
        _guard_empty_final30_for_engine_boot(pd.DataFrame(), phase_name="entry", session_kind="am")
