from __future__ import annotations

from datetime import date
from pathlib import Path


def test_run_kr_prep_window_allows_0530():
    text = Path("scripts/wsl/run-kr-prep.sh").read_text(encoding="utf-8")
    assert '"$NOW_HM" < "05:00"' in text
    assert 'reason=PREP_WINDOW_OK' in text


def test_run_kr_prep_timeout_default_10800():
    text = Path("scripts/wsl/run-kr-prep.sh").read_text(encoding="utf-8")
    assert 'KR_PREP_TIMEOUT_SEC="${KR_PREP_TIMEOUT_SEC:-10800}"' in text
    assert '[KR_PREP][EFFECTIVE_ENV]' in text
    assert 'last_stage=${LAST_STAGE}' in text


def test_kr_prep_writes_canonical_artifact_immediately_after_final30():
    text = Path("trader/prep_runner.py").read_text(encoding="utf-8")
    assert 'stage="watchlist_build_done"' in text
    assert '[PREP][CANONICAL_ARTIFACT][START]' in text
    assert 'publish_kr_prep_artifacts_atomic(' in text
    assert '[PREP][CANONICAL_ARTIFACT][VERIFY_OK]' in text
    assert '[PREP][DONE_MARKER][SAVED]' in text
    assert text.index('stage="watchlist_build_done"') < text.index('[PREP][CANONICAL_ARTIFACT][START]')


def test_prep_aux_failure_does_not_delete_or_block_core_artifact():
    text = Path("trader/prep_runner.py").read_text(encoding="utf-8")
    assert '[PREP][AUX][FAIL_SOFT] stage=save_bundle_aux' in text
    assert 'action=continue_core_artifact_ready' in text


def test_kis_marketcap_provider_failure_falls_back_without_blocking_prep():
    cap = Path("trader/universe/capabilities.py").read_text(encoding="utf-8")
    provider = Path("trader/universe/providers/kis_marketcap_top.py").read_text(encoding="utf-8")
    assert '"practice": ["seed_static", "kis_marketcap_top", "emergency_seed"]' in cap
    assert '"fid_input_cnt_1": str(n)' in provider


def test_kr_am_rescues_from_db_when_contract_missing_but_final30_valid(monkeypatch, caplog):
    from trader.kr.runner import trade_session_runner as runner
    from trader.kr.artifacts import KrArtifactValidationResult

    trade_date = date(2026, 6, 26)
    expected = date(2026, 6, 25)
    monkeypatch.setenv("KR_TRADE_DATE", trade_date.isoformat())
    monkeypatch.setenv("KR_EXPECTED_AS_OF", expected.isoformat())
    monkeypatch.setattr(runner, "validate_kr_prep_artifact", lambda **kw: KrArtifactValidationResult(False, True, "CANONICAL_PREP_ARTIFACT_INVALID", detail="CONTRACT_MISSING", trade_date=trade_date, expected_as_of=expected))
    monkeypatch.setattr(runner, "rescue_kr_final30_from_db", lambda **kw: KrArtifactValidationResult(True, False, "DB_FINAL30_VALID_BUT_CONTRACT_MISSING", source="db_final30_scored", rows=30, db_exact_rows=30, trade_date=trade_date, expected_as_of=expected))
    ctx = runner._session_context("am", "practice")

    with caplog.at_level("INFO"):
        result = runner._guard_trade_session("am", ctx)

    assert result is None
    assert "[KR_SESSION][PRECHECK_RESCUED] session=am" in caplog.text


def test_kr_am_blocks_when_contract_missing_and_db_final30_invalid(monkeypatch, caplog):
    from trader.kr.runner import trade_session_runner as runner
    from trader.kr.artifacts import KrArtifactValidationResult

    trade_date = date(2026, 6, 26)
    expected = date(2026, 6, 25)
    monkeypatch.setenv("KR_TRADE_DATE", trade_date.isoformat())
    monkeypatch.setenv("KR_EXPECTED_AS_OF", expected.isoformat())
    monkeypatch.setattr(runner, "validate_kr_prep_artifact", lambda **kw: KrArtifactValidationResult(False, True, "CANONICAL_PREP_ARTIFACT_INVALID", detail="CONTRACT_MISSING", trade_date=trade_date, expected_as_of=expected))
    monkeypatch.setattr(runner, "rescue_kr_final30_from_db", lambda **kw: KrArtifactValidationResult(False, True, "DB_FINAL30_INVALID", rows=29, db_exact_rows=29, detail="DB_FINAL30_INVALID", trade_date=trade_date, expected_as_of=expected))
    ctx = runner._session_context("am", "practice")

    with caplog.at_level("INFO"):
        result = runner._guard_trade_session("am", ctx)

    assert result == {"status": "FAIL", "reason": "CANONICAL_PREP_ARTIFACT_INVALID"}
    assert "db_rescue=failed" in caplog.text
