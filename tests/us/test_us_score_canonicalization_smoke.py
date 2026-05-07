#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""US Score Canonicalization Smoke Test.

한국장 패턴을 미국장에 적용한 score alias recovery 테스트.
"""
import sys

def test_score_canonicalization():
    """Score canonicalization smoke test."""
    from trader.us.score_columns import (
        canonicalize_us_watchlist_row,
        extract_us_score,
        collect_us_score_nonzero_stats,
        validate_us_watchlist_score_contract,
    )
    
    print("[TEST] US Score Canonicalization Smoke Test")
    print("=" * 80)
    
    # Test 1: score alias recovery from score_final
    print("\nTest 1: score_final alias recovery")
    row1 = {"symbol": "AAPL", "score": 0, "score_final": 0.4215}
    canonical1 = canonicalize_us_watchlist_row(row1)
    assert canonical1["score"] == 0.4215, f"Expected 0.4215, got {canonical1['score']}"
    assert canonical1["score_source"] == "row.score_final", f"Expected row.score_final, got {canonical1['score_source']}"
    print(f"✓ AAPL: score={canonical1['score']:.6f} source={canonical1['score_source']}")
    
    # Test 2: meta.score_final recovery
    print("\nTest 2: meta.score_final recovery")
    row2 = {"symbol": "AMZN", "score": 0, "meta": {"score_final": 0.3821}}
    canonical2 = canonicalize_us_watchlist_row(row2)
    assert canonical2["score"] == 0.3821, f"Expected 0.3821, got {canonical2['score']}"
    print(f"✓ AMZN: score={canonical2['score']:.6f} source={canonical2['score_source']}")
    
    # Test 3: scores.final recovery
    print("\nTest 3: scores.final recovery")
    row3 = {"symbol": "MSFT", "score": 0, "scores": {"final": 0.5123}}
    canonical3 = canonicalize_us_watchlist_row(row3)
    assert canonical3["score"] == 0.5123, f"Expected 0.5123, got {canonical3['score']}"
    print(f"✓ MSFT: score={canonical3['score']:.6f} source={canonical3['score_source']}")
    
    # Test 4: JSON string meta recovery
    print("\nTest 4: JSON string meta recovery")
    row4 = {"symbol": "NVDA", "score": "0", "meta": '{"final_score": "0.491"}'}
    canonical4 = canonicalize_us_watchlist_row(row4)
    assert canonical4["score"] == 0.491, f"Expected 0.491, got {canonical4['score']}"
    print(f"✓ NVDA: score={canonical4['score']:.6f} source={canonical4['score_source']}")
    
    # Test 5: score stats collection
    print("\nTest 5: score stats collection")
    rows = [canonical1, canonical2, canonical3, canonical4]
    stats = collect_us_score_nonzero_stats(rows)
    assert stats["total"] == 4
    assert stats["unique_symbols"] == 4
    assert stats["score_nonzero"] == 4
    assert stats["score_zero"] == 0
    assert stats["score_missing"] == 0
    assert stats["score_nonzero_ratio"] == 1.0
    print(f"✓ Stats: total={stats['total']} unique={stats['unique_symbols']} "
          f"nonzero={stats['score_nonzero']} ratio={stats['score_nonzero_ratio']:.4f}")
    
    # Test 6: score contract validation
    print("\nTest 6: score contract validation")
    contract = validate_us_watchlist_score_contract(rows, stage="test", min_nonzero_ratio=0.80)
    assert contract["ok"] is True, f"Contract should pass, got {contract}"
    print(f"✓ Contract: ok={contract['ok']} errors={len(contract['errors'])} warnings={len(contract['warnings'])}")
    
    # Test 7: score zero mass detection
    print("\nTest 7: score zero mass detection")
    bad_rows = [
        {"symbol": "BAD1", "score": 0, "meta": {}},
        {"symbol": "BAD2", "score": 0, "meta": {}},
        {"symbol": "BAD3", "score": 0, "meta": {}},
    ]
    bad_canonical = [canonicalize_us_watchlist_row(r) for r in bad_rows]
    bad_contract = validate_us_watchlist_score_contract(bad_canonical, stage="test", min_nonzero_ratio=0.80)
    assert bad_contract["ok"] is False, f"Contract should fail for zero mass"
    print(f"✓ Zero mass detected: ok={bad_contract['ok']} errors={bad_contract['errors']}")
    
    # Test 8: extract_us_score with return_source
    print("\nTest 8: extract_us_score with return_source")
    score, source = extract_us_score(row1, "final", return_source=True)
    assert score == 0.4215
    assert source == "row.score_final"
    print(f"✓ extract_us_score: score={score:.6f} source={source}")
    
    print("\n" + "=" * 80)
    print("[TEST] All smoke tests passed ✓")
    return True


def test_watchlist_quality():
    """Watchlist quality contract smoke test."""
    from trader.us.watchlist_quality import validate_us_locked_watchlist_quality
    from trader.us.score_columns import canonicalize_us_watchlist_row
    
    print("\n[TEST] US Watchlist Quality Smoke Test")
    print("=" * 80)
    
    # Good watchlist
    print("\nTest: Good watchlist quality")
    good_rows = [
        {"symbol": f"GOOD{i}", "score": 0.5 + i * 0.01, "exchange": "NASDAQ"}
        for i in range(20)
    ]
    good_canonical = [canonicalize_us_watchlist_row(r) for r in good_rows]
    good_quality = validate_us_locked_watchlist_quality(good_canonical, stage="test")
    assert good_quality["ok"] is True
    assert good_quality["trade_can_proceed"] is True
    print(f"✓ Good quality: ok={good_quality['ok']} trade_can_proceed={good_quality['trade_can_proceed']}")
    
    # Bad watchlist: insufficient unique count
    print("\nTest: Insufficient unique count")
    bad_rows = [
        {"symbol": f"BAD{i}", "score": 0.5, "exchange": "NASDAQ"}
        for i in range(5)  # < US_MIN_LOCKED_WATCHLIST_COUNT (10)
    ]
    bad_canonical = [canonicalize_us_watchlist_row(r) for r in bad_rows]
    bad_quality = validate_us_locked_watchlist_quality(bad_canonical, stage="test")
    assert bad_quality["ok"] is False
    assert bad_quality["trade_can_proceed"] is False
    print(f"✓ Insufficient count detected: ok={bad_quality['ok']} errors={bad_quality['errors']}")
    
    print("\n" + "=" * 80)
    print("[TEST] All quality tests passed ✓")
    return True


if __name__ == "__main__":
    try:
        test_score_canonicalization()
        test_watchlist_quality()
        print("\n✓✓✓ All smoke tests passed ✓✓✓\n")
        sys.exit(0)
    except Exception as exc:
        print(f"\n✗✗✗ Smoke test failed: {exc} ✗✗✗\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
