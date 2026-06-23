import logging

import pandas as pd
import pytest

from trader import pb1_runner


def test_close_exit_empty_final30_bypasses_lock_validation(caplog):
    caplog.set_level(logging.INFO)
    pb1_runner._validate_trade_locked_final30_or_raise(
        final30_df=pd.DataFrame(),
        source_name="none",
        allow_empty_for_close_exit=True,
    )
    assert "[TRADE][FINAL30][LOCK][BYPASS_FOR_CLOSE_EXIT] source=none" in caplog.text


def test_entry_empty_final30_fails_lock_validation():
    with pytest.raises(RuntimeError, match="ENTRY_ABORT_PRECHECK:db_exact_scored_zero_rows"):
        pb1_runner._validate_trade_locked_final30_or_raise(
            final30_df=pd.DataFrame(),
            source_name="none",
            allow_empty_for_close_exit=False,
        )
