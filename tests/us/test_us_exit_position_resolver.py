# -*- coding: utf-8 -*-
"""Tests for us_exit_position_resolver.py

미국장 전체 보유 종목의 exit input 표준화 resolver 테스트.
특정 종목(CRDO, LITE) 하드코딩이 없음을 확인한다.
"""
import pytest
from trader.us.pb1.us_exit_position_resolver import enrich_us_positions_for_exit


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: avg_price_usd 기반 entry_price
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_uses_kis_avg_price_usd():
    positions, meta = enrich_us_positions_for_exit(
        [
            {
                "symbol": "CRDO",
                "exchange": "NASDAQ",
                "qty": 10,
                "avg_price_usd": 100.0,
                "current_price_usd": 89.0,
            }
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    assert len(positions) == 1
    assert positions[0]["entry_price"] == 100.0
    assert positions[0]["entry_price_source"] == "kis_avg_price_usd"
    assert meta["ok"] == 1
    assert meta["missing"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: buy_amount_usd / qty fallback
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_uses_buy_amount_usd_div_qty():
    positions, meta = enrich_us_positions_for_exit(
        [
            {
                "symbol": "LITE",
                "exchange": "NASDAQ",
                "qty": 5,
                "buy_amount_usd": 500.0,
                "current_price_usd": 88.0,
            }
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    assert len(positions) == 1
    assert positions[0]["entry_price"] == 100.0
    assert positions[0]["entry_price_source"] == "kis_buy_amount_usd"
    assert meta["ok"] == 1
    assert meta["missing"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: pnl_rate fallback
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_uses_pnl_rate_fallback():
    positions, meta = enrich_us_positions_for_exit(
        [
            {
                "symbol": "NVDA",
                "exchange": "NASDAQ",
                "qty": 2,
                "current_price_usd": 90.0,
                "pnl_rate": -10.0,
            }
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    assert len(positions) == 1
    assert round(positions[0]["entry_price"], 2) == 100.00
    assert positions[0]["entry_price_source"] == "kis_pnl_rate_fallback"
    assert meta["ok"] == 1
    assert meta["missing"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: multiple positions 전체 처리
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_handles_all_us_positions_not_specific_symbols():
    positions, meta = enrich_us_positions_for_exit(
        [
            {"symbol": "AAPL", "qty": 1, "avg_price_usd": 100.0, "current_price_usd": 99.0},
            {"symbol": "MSFT", "qty": 2, "buy_amount_usd": 400.0, "current_price_usd": 180.0},
            {"symbol": "NVDA", "qty": 1, "current_price_usd": 90.0, "pnl_rate": -10.0},
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    assert len(positions) == 3
    assert meta["ok"] == 3
    assert meta["missing"] == 0
    assert {p["symbol"] for p in positions} == {"AAPL", "MSFT", "NVDA"}


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: PNL_MISSING 처리 — entry_price 없으면 pnl_input_ok=False
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_marks_pnl_missing_when_no_entry_info():
    positions, meta = enrich_us_positions_for_exit(
        [
            {
                "symbol": "XYZ",
                "exchange": "NASDAQ",
                "qty": 3,
                # entry_price, avg_price_usd, buy_amount_usd, pnl_rate 모두 없음
                "current_price_usd": 50.0,
            }
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    assert len(positions) == 1
    assert positions[0]["pnl_input_ok"] is False
    assert positions[0]["entry_price_source"] == "missing"
    assert meta["missing"] == 1
    assert meta["ok"] == 0
    assert "XYZ" in meta["missing_symbols"]


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: qty=0 제외
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_excludes_zero_qty_positions():
    positions, meta = enrich_us_positions_for_exit(
        [
            {"symbol": "AAPL", "qty": 0, "avg_price_usd": 100.0},
            {"symbol": "MSFT", "qty": 5, "avg_price_usd": 200.0},
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    assert len(positions) == 1
    assert positions[0]["symbol"] == "MSFT"
    assert meta["total"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: 빈 positions 입력
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_handles_empty_positions():
    positions, meta = enrich_us_positions_for_exit(
        [],
        trade_date="2026-05-18",
        env="practice",
    )

    assert positions == []
    assert meta["total"] == 0
    assert meta["ok"] == 0
    assert meta["missing"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: symbol 대문자 정규화
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_normalizes_symbol_to_uppercase():
    positions, meta = enrich_us_positions_for_exit(
        [{"symbol": "aapl", "qty": 1, "avg_price_usd": 100.0}],
        trade_date="2026-05-18",
        env="practice",
    )

    assert positions[0]["symbol"] == "AAPL"


# ─────────────────────────────────────────────────────────────────────────────
# Test 9: entry_price 이미 있으면 그대로 사용
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_preserves_existing_entry_price_with_source():
    positions, meta = enrich_us_positions_for_exit(
        [
            {
                "symbol": "TSLA",
                "qty": 2,
                "entry_price": 150.0,
                "entry_price_source": "us_positions_avg_cost",
            }
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    assert positions[0]["entry_price"] == 150.0
    assert positions[0]["pnl_input_ok"] is True
    assert meta["ok"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Test 10: max_price fallback — position에 max_price 없으면 entry_price로 설정
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_sets_max_price_fallback():
    positions, meta = enrich_us_positions_for_exit(
        [
            {
                "symbol": "GOOG",
                "qty": 1,
                "avg_price_usd": 120.0,
                "current_price_usd": 115.0,
            }
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    pos = positions[0]
    assert "max_price" in pos
    # max_price = max(entry_price, current_price) = max(120, 115) = 120
    assert pos["max_price"] == 120.0


# ─────────────────────────────────────────────────────────────────────────────
# Test 11: sources 집계 정확성
# ─────────────────────────────────────────────────────────────────────────────

def test_resolver_sources_count_accurately():
    positions, meta = enrich_us_positions_for_exit(
        [
            {"symbol": "A", "qty": 1, "avg_price_usd": 10.0},
            {"symbol": "B", "qty": 1, "avg_price_usd": 20.0},
            {"symbol": "C", "qty": 1, "buy_amount_usd": 300.0},
        ],
        trade_date="2026-05-18",
        env="practice",
    )

    assert meta["sources"].get("kis_avg_price_usd", 0) == 2
    assert meta["sources"].get("kis_buy_amount_usd", 0) == 1
