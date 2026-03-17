"""Tests for policy-aware verify_prep_log parser."""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest


def test_verify_success_full_policy_path():
    log_content = """
[PREP][START] as_of=2026-03-08
[PREP][ASOF_CONSISTENCY] universe=2026-03-08 ohlcv=2026-03-08 derived=2026-03-08 candidate_pool=2026-03-08 watchlist=2026-03-08 flow=2026-03-08 final30=2026-03-08 consistent=1
[PREP][DERIVED][MINERVINI] upserted=196
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-08 rows=196 rs_nonzero=196 vcp_nonzero=196 trend_nonzero=196
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[PREP][WATCHLIST_FINAL][SAVE] n=30
event_type=PREP_DONE
[PREP][DONE] as_of=2026-03-08 symbols=196 pool=120 watchlist=30 dt=30.1
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.prep_done is True
        assert results.prep_done_log is True
        assert results.derived_count == 196
        assert results.asof_consistent is True
        assert results.exporter_preserved_scores is True
        assert not results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_contract_recovery_success():
    log_content = """
[PREP][START] as_of=2026-03-08
[PREP][ASOF_CONSISTENCY] universe=2026-03-08 ohlcv=2026-03-08 derived=2026-03-08 candidate_pool=2026-03-08 watchlist=2026-03-08 flow=2026-03-08 final30=2026-03-08 consistent=1
event_type=PREP_DONE
[PREP][DONE] as_of=2026-03-08 symbols=196 pool=120 watchlist=30 dt=31.0
[PREP][DERIVED][MINERVINI] upserted=120
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-08 rows=120 rs_nonzero=120 vcp_nonzero=120 trend_nonzero=120
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[CONTRACT_VALIDATION][FAIL] failures=['contract_universe_too_small:120<150']
[PREP][WATCHLIST][CONTRACT_FAIL] failures=['contract_universe_too_small:120<150']
[PREP][WATCHLIST][RECOVERY][DB_SUCCESS]
[PREP][WATCHLIST_FINAL][SAVE] n=30
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.prep_done is True
        assert "contract_universe_too_small" in results.contract_failures
        assert "DB_SUCCESS" in results.contract_recoveries
        assert not results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_contract_unrecovered_failure():
    log_content = """
[PREP][START] as_of=2026-03-08
[PREP][ASOF_CONSISTENCY] universe=2026-03-08 ohlcv=2026-03-08 derived=2026-03-08 candidate_pool=2026-03-08 watchlist=2026-03-08 flow=2026-03-08 final30=2026-03-08 consistent=1
event_type=PREP_DONE
[PREP][DONE] as_of=2026-03-08 symbols=196 pool=120 watchlist=30 dt=31.0
[PREP][DERIVED][MINERVINI] upserted=120
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-08 rows=120 rs_nonzero=120 vcp_nonzero=120 trend_nonzero=120
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
contract_universe_too_small:120<150
[PREP][WATCHLIST_FINAL][SAVE] n=30
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file
        
        results = parse_log_file(log_path)
        assert results.prep_done is True
        assert "contract_universe_too_small" in results.contract_failures
        assert len(results.contract_recoveries) == 0
        assert results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_asof_consistency_failure():
    log_content = """
[PREP][START] as_of=2026-03-08
[PREP][ASOF_CONSISTENCY] universe=2026-03-08 ohlcv=2026-03-08 derived=2026-03-08 candidate_pool=2026-03-07 watchlist=2026-03-08 flow=2026-03-08 final30=2026-03-08 consistent=0
event_type=PREP_DONE
[PREP][DONE] as_of=2026-03-08 symbols=196 pool=120 watchlist=30 dt=31.0
[PREP][DERIVED][MINERVINI] upserted=196
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-08 rows=196 rs_nonzero=196 vcp_nonzero=196 trend_nonzero=196
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[PREP][WATCHLIST_FINAL][SAVE] n=30
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file

        results = parse_log_file(log_path)
        assert results.asof_consistent is False
        assert results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_exporter_score_mismatch_failure():
    log1 = """
[PREP][ASOF_CONSISTENCY] universe=2026-03-08 ohlcv=2026-03-08 derived=2026-03-08 candidate_pool=2026-03-08 watchlist=2026-03-08 flow=2026-03-08 final30=2026-03-08 consistent=1
[PREP][DERIVED][MINERVINI] upserted=196
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-08 rows=196 rs_nonzero=196 vcp_nonzero=196 trend_nonzero=196
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=0 pullback_nonzero=0 momentum_nonzero=0
event_type=PREP_DONE
[PREP][DONE] as_of=2026-03-08 symbols=196 pool=120 watchlist=30 dt=31.0
[PREP][WATCHLIST_FINAL][SAVE] n=30
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log1)
        log_path = Path(f.name)
    
    try:
        from scripts.verify_prep_log import parse_log_file

        results = parse_log_file(log_path)
        assert results.exporter_preserved_scores is False
        assert results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_candidate_pool_future_reject_is_non_fatal():
    log_content = """
[PREP][ASOF_CONSISTENCY] universe=2026-03-08 ohlcv=2026-03-08 derived=2026-03-08 candidate_pool=2026-03-08 watchlist=2026-03-08 flow=2026-03-08 final30=2026-03-08 consistent=1
[PREP][DERIVED][MINERVINI] upserted=196
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-08 rows=196 rs_nonzero=196 vcp_nonzero=196 trend_nonzero=196
[CANDIDATE_POOL][DATE_GUARD] requested_as_of=2026-03-08 actual_as_of=2026-03-09 action=reject_future_snapshot
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
event_type=PREP_DONE
[PREP][DONE] as_of=2026-03-08 symbols=196 pool=120 watchlist=30 dt=31.0
[PREP][WATCHLIST_FINAL][SAVE] n=30
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)

    try:
        from scripts.verify_prep_log import parse_log_file

        results = parse_log_file(log_path)
        assert results.candidate_pool_future_rejected is True
        assert not results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_traceback_is_critical_failure():
    log_content = """
[PREP][ASOF_CONSISTENCY] universe=2026-03-08 ohlcv=2026-03-08 derived=2026-03-08 candidate_pool=2026-03-08 watchlist=2026-03-08 flow=2026-03-08 final30=2026-03-08 consistent=1
[PREP][DERIVED][MINERVINI] upserted=196
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-08 rows=196 rs_nonzero=196 vcp_nonzero=196 trend_nonzero=196
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
event_type=PREP_DONE
[PREP][DONE] as_of=2026-03-08 symbols=196 pool=120 watchlist=30 dt=31.0
[PREP][WATCHLIST_FINAL][SAVE] n=30
Traceback (most recent call last)
RuntimeError: boom
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)

    try:
        from scripts.verify_prep_log import parse_log_file

        results = parse_log_file(log_path)
        assert results.traceback_detected is True
        assert results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_pykrx_traceback_is_non_fatal_when_done_marker_exists():
    log_content = """
[TIME][TRADING_DAY][PYKRX_FAIL] date=2026-03-08 fallback=weekday_heuristic err_type=JSONDecodeError
Traceback (most recent call last)
TypeError: not all arguments converted during string formatting
[PREP][DERIVED][MINERVINI] upserted=196
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-08 rows=196 rs_nonzero=196 vcp_nonzero=196 trend_nonzero=196
[PREP][ASOF_CONSISTENCY] universe=2026-03-08 ohlcv=2026-03-08 derived=2026-03-08 candidate_pool=2026-03-08 watchlist=2026-03-08 flow=2026-03-08 final30=2026-03-08 consistent=1
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=30 pullback_nonzero=30 momentum_nonzero=30
[PREP][WATCHLIST_FINAL][SAVE] n=30
event_type=PREP_DONE
[PREP][DONE] as_of=2026-03-08 source=db_exact universe=196 pool120=120 top50=50 final30=30 flow_coverage=100.0 final_source=db_roundtrip contract_mode=full_universe_based dt=31.0
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)

    try:
        from scripts.verify_prep_log import parse_log_file

        results = parse_log_file(log_path)
        assert results.traceback_detected is True
        assert results.traceback_non_fatal is True
        assert not results.has_critical_failure()
    finally:
        log_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
