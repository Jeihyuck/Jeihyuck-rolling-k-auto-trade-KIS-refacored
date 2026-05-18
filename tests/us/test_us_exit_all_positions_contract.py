# -*- coding: utf-8 -*-
"""Tests for US exit all positions contract.

한국장 import 금지, 특정 종목 하드코딩 금지 등 계약 조건 검증.
"""
import inspect


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: 한국장 모듈 import 금지
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_resolver_does_not_import_kr_modules():
    """resolver는 trader.db.repos, FillsRepo, PositionsRepo 등 한국장 모듈을 import하지 않는다."""
    import trader.us.pb1.us_exit_position_resolver as resolver

    src = inspect.getsource(resolver)

    assert "trader.db.repos" not in src, "KR DB repo import 금지"
    assert "FillsRepo" not in src, "KR FillsRepo import 금지"
    assert "PositionsRepo" not in src, "KR PositionsRepo import 금지"
    assert "trader.pb1_engine" not in src, "KR pb1_engine import 금지"
    assert "trader.pb1_runner" not in src, "KR pb1_runner import 금지"


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: 특정 종목 하드코딩 금지
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_resolver_has_no_specific_symbol_hardcoding():
    """resolver에 CRDO, LITE 등 특정 종목이 하드코딩되어 있지 않다."""
    import trader.us.pb1.us_exit_position_resolver as resolver

    src = inspect.getsource(resolver)

    assert "CRDO" not in src, "CRDO 하드코딩 금지"
    assert "LITE" not in src, "LITE 하드코딩 금지"


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: exit engine 한국장 import 금지
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_engine_does_not_import_kr_modules():
    import trader.us.pb1.us_exit_engine as engine_mod

    src = inspect.getsource(engine_mod)

    assert "trader.db.repos" not in src, "KR DB repo import 금지"
    assert "FillsRepo" not in src
    assert "PositionsRepo" not in src
    assert "trader.pb1_engine" not in src
    assert "trader.pb1_runner" not in src


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: repos.py 한국장 테이블 접근 금지 확인
# ─────────────────────────────────────────────────────────────────────────────

def test_us_db_repos_does_not_access_kr_tables():
    """us/db/repos.py에서 한국장 테이블(orders, positions, fills, runs, pb1_watchlist) 직접 접근 금지."""
    import trader.us.db.repos as repos_mod

    src = inspect.getsource(repos_mod)

    # 한국장 테이블을 FROM / INTO 로 직접 조회하지 않는지 확인
    # (테이블 이름 언급이 있더라도 FROM/INTO가 아닌 주석/string은 허용)
    import re

    kr_tables = ["orders", "positions", "fills", "runs", "pb1_watchlist", "universe_members"]
    for table in kr_tables:
        # FROM table 또는 INTO table 패턴 (us_ prefix 없는 테이블만)
        pattern = rf"\bFROM\s+{table}\b|\bINTO\s+{table}\b|\bJOIN\s+{table}\b"
        matches = re.findall(pattern, src, re.IGNORECASE)
        assert not matches, f"KR 테이블 '{table}' 직접 접근 금지: {matches}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: data_provider 특정 종목 하드코딩 금지
# ─────────────────────────────────────────────────────────────────────────────

def test_us_data_provider_has_no_specific_symbol_hardcoding():
    import trader.us.data_provider as dp_mod

    src = inspect.getsource(dp_mod)

    # normalize_us_balance 부분에 CRDO/LITE 하드코딩 없음
    assert "CRDO" not in src
    assert "LITE" not in src


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: resolver enrich_us_positions_for_exit 공개 API 시그니처 검증
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_resolver_public_api_signature():
    from trader.us.pb1.us_exit_position_resolver import enrich_us_positions_for_exit
    import inspect as _inspect

    sig = _inspect.signature(enrich_us_positions_for_exit)
    params = list(sig.parameters.keys())

    assert "positions" in params
    assert "trade_date" in params
    assert "env" in params


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: repos.py 신규 함수 존재 확인
# ─────────────────────────────────────────────────────────────────────────────

def test_us_db_repos_has_position_and_fill_lookup_functions():
    from trader.us.db import repos

    assert hasattr(repos, "load_us_positions_by_symbols"), "load_us_positions_by_symbols 누락"
    assert hasattr(repos, "load_latest_us_buy_fills_by_symbols"), "load_latest_us_buy_fills_by_symbols 누락"

    # callable 확인
    assert callable(repos.load_us_positions_by_symbols)
    assert callable(repos.load_latest_us_buy_fills_by_symbols)


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: US_EXIT_FAIL_CLOSED_ON_PNL_MISSING 기본값 1 확인
# ─────────────────────────────────────────────────────────────────────────────

def test_us_exit_fail_closed_default_is_1(monkeypatch):
    """환경변수 미설정 시 fail-closed가 기본값 1이어야 한다."""
    import os
    monkeypatch.delenv("US_EXIT_FAIL_CLOSED_ON_PNL_MISSING", raising=False)

    from trader.us.pb1.us_exit_engine import evaluate_exit

    pos = {"symbol": "MISSING_ENTRY", "qty": 5}
    intent = evaluate_exit(pos, current_price=50.0)

    # 기본값 fail-closed → SELL intent 생성
    assert intent is not None
    assert intent["exit_type"] == "pnl_missing_fail_closed"


# ─────────────────────────────────────────────────────────────────────────────
# Test 9: us_explain.py pnl_missing_entry_price 처리
# ─────────────────────────────────────────────────────────────────────────────

def test_us_explain_hold_pnl_missing_uses_correct_why_not_sell():
    """entry_price 없이 HOLD → why_not_sell=pnl_missing_entry_price"""
    from trader.us.pb1.us_explain import build_us_exit_explanation

    pos = {"symbol": "AAPL", "qty": 5}  # entry_price 없음
    explanation = build_us_exit_explanation(
        symbol="AAPL",
        position=pos,
        exit_intent=None,
        current_price=90.0,
    )

    assert explanation["why_not_sell"] == "pnl_missing_entry_price"
    assert explanation["pnl_pct"] is None, "pnl_pct=0.0000으로 위장 금지"


# ─────────────────────────────────────────────────────────────────────────────
# Test 10: data_provider normalize_us_balance entry_price alias 포함
# ─────────────────────────────────────────────────────────────────────────────

def test_us_data_provider_normalize_includes_entry_price_alias():
    """normalize_us_balance 결과 position에 entry_price alias가 있다."""
    from trader.us.data_provider import normalize_us_balance

    raw = {
        "output1": [
            {
                "ovrs_pdno": "AAPL",
                "prdt_name": "Apple",
                "ovrs_excg_cd": "NASD",
                "ovrs_cblc_qty": "10",
                "pchs_avg_pric": "150.0",
                "now_pric2": "155.0",
                "frcr_pchs_amt1": "1500.0",
                "evlu_pfls_rt": "3.33",
            }
        ],
        "output2": {},
    }

    result = normalize_us_balance(raw)
    positions = result.get("positions", [])

    assert len(positions) == 1
    pos = positions[0]
    assert "entry_price" in pos
    assert pos["entry_price"] == 150.0
    assert pos["entry_price_source"] == "kis_avg_price_usd"
    assert pos["avg_cost"] == 150.0
