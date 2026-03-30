"""Tests for policy-aware verify_prep_log parser."""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from scripts.verify_prep_log import parse_log_file


def test_parse_log_file_accepts_file_contract_evidence(tmp_path) -> None:
    log_path = tmp_path / "prep.log"
    log_path.write_text(
        "\n".join(
            [
                "[LEDGER_EVENT] event_type=PREP_DONE as_of=2026-03-16 symbols=30 flow_coverage=100.0%",
                "[PREP][DONE] as_of=2026-03-16 source=fresh_build universe=200 pool120=120 top50=50 final30=30 flow_coverage=100.0 final_source=watchlist_result.final30_scored contract_mode=strict dt=1.23",
                "[PREP][DERIVED_VERIFY][OK]",
                "[PREP][ASOF_CONSISTENCY] universe=2026-03-16 ohlcv=2026-03-16 derived=2026-03-16 candidate_pool=2026-03-16 watchlist=2026-03-16 flow=2026-03-16 final30=2026-03-16 consistent=1",
                "[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=10 pullback_nonzero=8 momentum_nonzero=12",
                "[EXPORT][SCORES] name=final30 tech_nonzero=30 score_final_nonzero=30 breakout_nonzero=10 pullback_nonzero=8 momentum_nonzero=12",
                "[PREP][WATCHLIST_FINAL][SAVE] strategy=pb1_watchlist_final as_of=2026-03-16 n=30",
                "[PREP][FINAL30_FILE][WRITE] label=runtime path=/repo/runtime/watchlist/2026-03-16/final30_scored.json exists=True bytes=1024 rows=30",
                "[PREP][FINAL30_FILE][WRITE] label=ledger path=/repo/bot_state/trader_ledger/final30/practice/2026-03-16/final30_scored.json exists=True bytes=1024 rows=30",
                "[PREP][FINAL30_FILE][WRITE] label=signals path=/repo/signals/final30.json exists=True bytes=1024 rows=30",
                "[PREP][FINAL30_FILE][CONTRACT] ok=1 strict=0 failures=[]",
                "[DERIVED][LOAD] rows=196",
            ]
        ),
        encoding="utf-8",
    )

    results = parse_log_file(log_path)

    assert results.final30_file_contract_ok is True
    assert results.final30_contract_ok is True
    assert results.final30_file_labels == ["ledger", "runtime", "signals"]
    assert results.has_critical_failure() is False


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
        assert results.final30_log_success is True
        assert not results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_final30_false_negative_regression_uses_recognized_success_logs():
    log_content = """
[PREP][ASOF_CONSISTENCY] universe=2026-03-21 ohlcv=2026-03-21 derived=2026-03-21 candidate_pool=2026-03-21 watchlist=2026-03-21 flow=2026-03-21 final30=2026-03-21 consistent=1
[PREP][DERIVED][MINERVINI] upserted=212
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-21 rows=212 rs_nonzero=212 vcp_nonzero=212 trend_nonzero=212
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=11 pullback_nonzero=9 momentum_nonzero=10
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=11 pullback_nonzero=9 momentum_nonzero=10
[WATCHLIST][SAVE] strategy=pb1_watchlist_final_scored members=30
[PREP][WATCHLIST_FINAL][SAVE] strategy=pb1_watchlist_final as_of=2026-03-21 n=30
[PREP][FINAL30_SNAPSHOT][SAVE] as_of=2026-03-21 count=30 path=/repo/runtime/prep/2026-03-21/final30_locked.json warn_only=1
[FINAL30][REPAIR][DONE] success=3 failed=0
[FINAL30_FILE][CONTRACT] ok=1
[PREP][DONE] as_of=2026-03-21 source=fresh_build universe=212 pool120=120 top50=50 final30=30 flow_coverage=100.0 final_source=db_roundtrip contract_mode=strict dt=4.20
event_type=PREP_DONE as_of=2026-03-21
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)

    try:
        results = parse_log_file(log_path)
        assert results.final30_log_success is True
        assert results.final30_contract_ok is True
        assert "watchlist_final_scored_save" in results.final30_success_logs
        assert "final30_snapshot_save" in results.final30_success_logs
        assert not results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_final30_db_success_without_files_is_non_fatal(tmp_path, monkeypatch):
    log_content = """
[PREP][ASOF_CONSISTENCY] universe=2026-03-21 ohlcv=2026-03-21 derived=2026-03-21 candidate_pool=2026-03-21 watchlist=2026-03-21 flow=2026-03-21 final30=2026-03-21 consistent=1
[PREP][DERIVED][MINERVINI] upserted=180
[PREP][DERIVED_VERIFY][OK] as_of=2026-03-21 rows=180 rs_nonzero=180 vcp_nonzero=180 trend_nonzero=180
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=8 pullback_nonzero=12 momentum_nonzero=10
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=8 pullback_nonzero=12 momentum_nonzero=10
[DB][FINAL30_SCORED][VERIFY] rows=30 uniq_codes=30 uniq_ranks=30 rank_source=rank_final30 rank_warn=0 null_critical=0 env=practice as_of=2026-03-21 ok=1
[PREP][DB_COMMIT][VERIFY] env=practice as_of=2026-03-21 final=30 final_scored=30 required=30 ok=1
[PREP][DONE] as_of=2026-03-21 source=fresh_build universe=180 pool120=120 top50=50 final30=30 flow_coverage=100.0 final_source=db_roundtrip contract_mode=strict dt=4.20
event_type=PREP_DONE as_of=2026-03-21
"""
    monkeypatch.setenv("TRADER_REPO_ROOT", str(tmp_path))
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)

    try:
        results = parse_log_file(log_path)
        assert results.final30_log_success is True
        assert results.final30_contract_ok is True
        assert results.final30_file_contract_ok is False
        assert not results.has_critical_failure()
    finally:
        log_path.unlink()


def test_verify_final30_contract_missing_fails_with_detail(tmp_path, monkeypatch):
    log_content = """
[PREP][ASOF_CONSISTENCY] universe=2026-03-21 ohlcv=2026-03-21 derived=2026-03-21 candidate_pool=2026-03-21 watchlist=2026-03-21 flow=2026-03-21 final30=2026-03-21 consistent=1
[PREP][DERIVED][MINERVINI] upserted=0
[PREP][DERIVED_VERIFY][FAIL] as_of=2026-03-21 rows=0 rs_nonzero=0 vcp_nonzero=0 trend_nonzero=0
[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=8 pullback_nonzero=12 momentum_nonzero=10
[EXPORT][SCORES] name=final30 rows=30 tech_nonzero=30 score_final_nonzero=30 final_score_nonzero=30 breakout_nonzero=8 pullback_nonzero=12 momentum_nonzero=10
"""
    monkeypatch.setenv("TRADER_REPO_ROOT", str(tmp_path))
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
        f.write(log_content)
        log_path = Path(f.name)

    try:
        results = parse_log_file(log_path)
        assert results.final30_contract_ok is False
        assert "FAIL final30 contract missing" in results.final30_failure_detail
        assert results.has_critical_failure()
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


def test_verify_fails_when_canonical_quality_is_not_ok(tmp_path, monkeypatch):
    as_of = "2026-03-26"
    manifest_dir = tmp_path / "runtime" / "prep" / as_of
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "prep_manifest.json").write_text(
        """{
  "as_of": "2026-03-26",
  "build_status": "FAIL",
  "final30_quality_ok": false,
  "trade_can_proceed": 0,
  "flow_failed_ratio": 1.0,
  "flow_fail_reason_counts": {"kis:init_failed": 30},
  "canonical_quality": {
    "status": "FAIL",
    "quality_ok": 0,
    "trade_can_proceed": 0,
    "hard_fail_reasons": ["flow_failed_ratio_hard_fail"],
    "soft_fail_reasons": ["entry_style_monoculture"]
  }
}""",
        encoding="utf-8",
    )
    log_path = tmp_path / "prep.log"
    log_path.write_text(
        "\n".join(
            [
                f"[PREP][DONE] as_of={as_of} status=FAIL quality_ok=0 trade_can_proceed=0",
                "[PREP][DERIVED_VERIFY][OK]",
                f"[PREP][ASOF_CONSISTENCY] universe={as_of} ohlcv={as_of} derived={as_of} candidate_pool={as_of} watchlist={as_of} flow={as_of} final30={as_of} consistent=1",
                "[PREP][EXPORT][FINAL30][INMEM] rows=30 tech_nonzero=30 final_nonzero=30 score_final_nonzero=30 breakout_nonzero=10 pullback_nonzero=8 momentum_nonzero=12",
                "[EXPORT][SCORES] name=final30 tech_nonzero=30 score_final_nonzero=30 breakout_nonzero=10 pullback_nonzero=8 momentum_nonzero=12",
                "[PREP][WATCHLIST_FINAL][SAVE] strategy=pb1_watchlist_final as_of=2026-03-26 n=30",
                "event_type=PREP_DONE as_of=2026-03-26",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADER_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("VERIFY_PREP_REQUIRE_CANONICAL", "1")

    results = parse_log_file(log_path)

    assert "quality_not_ok" in results.failures
    assert "trade_cannot_proceed" in results.failures
    assert results.has_critical_failure() is True
