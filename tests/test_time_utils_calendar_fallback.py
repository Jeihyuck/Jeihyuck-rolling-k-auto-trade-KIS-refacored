from __future__ import annotations

from unittest.mock import patch

import trader.time_utils as time_utils


def test_calendar_fallback_logs_once(caplog):
    caplog.set_level("INFO")
    with patch.object(time_utils, "_get_krx_holidays_config_path", side_effect=FileNotFoundError()):
        time_utils._KRX_HOLIDAYS_CACHE = None
        time_utils._KRX_CALENDAR_FALLBACK_LOGGED = False
        time_utils._load_krx_holidays()
        time_utils._KRX_HOLIDAYS_CACHE = None
        time_utils._load_krx_holidays()

    fallback_logs = [
        record.message
        for record in caplog.records
        if "[TIME][KRX][CALENDAR_FALLBACK]" in record.message
    ]
    assert len(fallback_logs) == 1
