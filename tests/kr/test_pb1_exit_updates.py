from __future__ import annotations

from trader.kr.pb1.exit_updates import build_exit_position_update_fields


def test_build_exit_position_update_fields_updates_high_water_mark_and_stop_price_when_missing():
    assert build_exit_position_update_fields(
        current_max_price=100.0,
        mark=123.4,
        stop_price=95.0,
        stop_price_missing=True,
    ) == {"max_price": 123.4, "stop_price": 95.0}


def test_build_exit_position_update_fields_keeps_existing_stop_price():
    assert build_exit_position_update_fields(
        current_max_price=120.0,
        mark=110.0,
        stop_price=95.0,
        stop_price_missing=False,
    ) == {"max_price": 120.0}
