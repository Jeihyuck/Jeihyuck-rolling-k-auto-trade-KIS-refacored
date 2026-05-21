# -*- coding: utf-8 -*-
"""Phase 3-4: BUY intent에 book/horizon/exit_policy/entry_meta 필드 검증."""
from __future__ import annotations

import os
import pytest
from unittest.mock import patch


def _make_watchlist_item(symbol: str = "CRDO") -> dict:
    return {
        "symbol": symbol,
        "exchange": "NASDAQ",
        "entry_price": 68.0,
        "limit_price": 68.5,
        "qty": 5,
        "score": 0.85,  # score 필드 필수
        "final_score": 0.85,
        "entry_meta": {
            "entry_signal_type": "breakout",
            "vcp_tight_count": 3,
        },
        "entry_signal_type": "breakout",
    }


def _call_generate(items, env_extra: dict | None = None):
    """generate_entry_intents 공통 호출 헬퍼."""
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    from unittest.mock import MagicMock

    provider = MagicMock()
    # watchlist_entries를 직접 넘기면 provider.get_quote 호출 없이 intent 생성
    env = env_extra or {}
    with patch.dict(os.environ, env):
        intents = generate_entry_intents(
            tickers=None,
            provider=provider,
            sold_today=set(),
            available_cash_usd=50000.0,
            position_count=0,
            capital_usd_cap=100000.0,
            watchlist_entries=items,
        )
    return intents


def test_buy_intent_has_book_field():
    """BUY intent에 book 필드가 있어야 함."""
    intents = _call_generate([_make_watchlist_item()], {"US_DEFAULT_ENTRY_BOOK": "SWING_BOOK"})

    assert intents, "intent가 생성되어야 함"
    assert intents[0]["book"] == "SWING_BOOK"


def test_buy_intent_has_horizon_field():
    intents = _call_generate([_make_watchlist_item()], {"US_DEFAULT_ENTRY_HORIZON": "SWING_CARRY"})

    assert intents[0]["horizon"] == "SWING_CARRY"


def test_buy_intent_has_exit_policy():
    intents = _call_generate([_make_watchlist_item()], {"US_DEFAULT_EXIT_POLICY": "US_SWING_DEFAULT"})

    assert intents[0]["exit_policy"] == "US_SWING_DEFAULT"


def test_buy_intent_has_entry_strategy():
    intents = _call_generate([_make_watchlist_item()])

    assert intents[0]["entry_strategy"] == "us_pb1"


def test_buy_intent_partial_exit_allowed_default_false():
    """US_SELL_PARTIAL_ALLOWED=0(기본)이면 partial_exit_allowed=False."""
    intents = _call_generate([_make_watchlist_item()], {"US_SELL_PARTIAL_ALLOWED": "0"})

    assert intents[0]["partial_exit_allowed"] is False


def test_buy_intent_partial_exit_allowed_env_true():
    """US_SELL_PARTIAL_ALLOWED=1이면 partial_exit_allowed=True."""
    intents = _call_generate([_make_watchlist_item()], {"US_SELL_PARTIAL_ALLOWED": "1"})

    assert intents[0]["partial_exit_allowed"] is True


def test_buy_intent_meta_has_schema_version():
    """meta 필드에 schema_version이 있어야 함."""
    intents = _call_generate([_make_watchlist_item()])

    meta = intents[0].get("meta", {})
    assert meta.get("schema_version") == 1


# ── _resolve_entry_signal_type ────────────────────────────────────────────

def test_resolve_signal_type_from_entry_meta():
    from trader.us.pb1.us_entry_engine import _resolve_entry_signal_type

    meta = {"entry_signal_type": "breakout"}
    assert _resolve_entry_signal_type(meta) == "breakout"


def test_resolve_signal_type_from_entry_style_selected():
    from trader.us.pb1.us_entry_engine import _resolve_entry_signal_type

    meta = {"entry_style_selected": "pullback"}
    assert _resolve_entry_signal_type(meta) == "pullback"


def test_resolve_signal_type_from_entry_style():
    from trader.us.pb1.us_entry_engine import _resolve_entry_signal_type

    meta = {"entry_style": "vcp"}
    assert _resolve_entry_signal_type(meta) == "vcp"


def test_resolve_signal_type_none_returns_unknown():
    from trader.us.pb1.us_entry_engine import _resolve_entry_signal_type

    assert _resolve_entry_signal_type(None) == "unknown"


def test_resolve_signal_type_empty_returns_unknown():
    from trader.us.pb1.us_entry_engine import _resolve_entry_signal_type

    assert _resolve_entry_signal_type({}) == "unknown"


def test_buy_intent_entry_signal_type_reflected():
    """watchlist item에서 entry_signal_type이 BUY intent에 반영되어야 함."""
    intents = _call_generate([_make_watchlist_item()])

    assert intents[0]["entry_signal_type"] == "breakout"
