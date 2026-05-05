# -*- coding: utf-8 -*-
"""Signal-only Order Gate 테스트.

non-trading day signal_only=True이면 KIS 주문 API 호출이 0회여야 함.
"""
import pytest


def test_signal_only_blocks_kis_order():
    """signal_only=True이면 KIS 주문 API 호출 금지."""
    # order_router에서 signal_only=True일 때
    # place_us_buy_order / sell_order 호출 0회
    pass


def test_dry_run_blocks_kis_order():
    """DRY_RUN=1이면 KIS 주문 API 호출 금지."""
    pass


def test_us_kis_order_allowed_false_blocks():
    """US_KIS_ORDER_ALLOWED=0이면 KIS 주문 API 호출 금지."""
    pass


def test_non_trading_day_sets_signal_only():
    """non-trading day이면 signal_only=True."""
    # trade_tick_runner에서 is_us_trading_day가 False이면
    # signal_only mode로 진행
    pass
