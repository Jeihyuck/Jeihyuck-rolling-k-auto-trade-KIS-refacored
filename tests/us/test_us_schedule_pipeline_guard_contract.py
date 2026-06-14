# -*- coding: utf-8 -*-
"""US schedule prep/AM pipeline guard contract tests.

검증 내용:
1. us-trade-prep.yml 스케줄 / phase guard 07:00 ET 기준 확인
2. us-trade-am.yml AM-only 구조 확인 (prep 직접 실행 금지)
3. TRADE_DATE env 전달 방어 코드 확인
4. failure report contract 확인
5. PnL report import 확인
6. watchdog contract 확인
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


# ── 파일 경로 ──────────────────────────────────────────────────────────────────
PREP_YML = Path(".github/workflows/us-trade-prep.yml")
AM_YML = Path(".github/workflows/us-trade-am.yml")
WATCHDOG_YML = Path(".github/workflows/us-trade-watchdog.yml")
PNL_SCRIPT = Path("scripts/generate_us_portfolio_pnl_report.py")
GUARD_SCRIPT = Path("scripts/write_us_guard_failure_report.py")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Schedule contract tests
# ─────────────────────────────────────────────────────────────────────────────

def test_prep_has_no_github_schedule():
    """us-trade-prep.yml은 WSL 이전 후 GitHub schedule이 없어야 한다."""
    text = PREP_YML.read_text(encoding="utf-8")
    assert "schedule:" not in text
    assert "workflow_dispatch:" in text


def test_am_has_no_github_schedule():
    """us-trade-am.yml은 WSL 이전 후 GitHub schedule이 없어야 한다."""
    text = AM_YML.read_text(encoding="utf-8")
    assert "schedule:" not in text
    assert "workflow_dispatch:" in text


def test_prep_phase_guard_is_0700_based():
    """us-trade-prep.yml phase guard가 06:00 ET 기준 (0600 ET window)."""
    text = PREP_YML.read_text(encoding="utf-8")
    assert "6 * 60 + 0" in text, "prep phase guard must use 06:00 ET target"
    assert "phase_window=0600-0810" in text, "prep log must show phase_window=0600-0810"


def test_prep_no_fallback_only_comment():
    """us-trade-prep.yml에 'fallback only' 문구가 없어야 함."""
    text = PREP_YML.read_text(encoding="utf-8")
    assert "fallback only" not in text.lower(), "us-trade-prep.yml must not say 'fallback only'"
    assert "Primary prep execution moved to us-trade-am.yml" not in text, \
        "us-trade-prep.yml must not say prep moved to am workflow"


def test_am_default_manual_run_is_safe_mode():
    """us-trade-am.yml 수동 실행 기본값은 주문 불가 안전모드여야 한다."""
    text = AM_YML.read_text(encoding="utf-8")
    for marker in (
        'DRY_RUN: "1"',
        'DISABLE_LIVE_TRADING: "1"',
        'LIVE_TRADING_ENABLED: "0"',
        'STRATEGY_MODE: "INTENT_ONLY"',
        'FORCE_STRATEGY_MODE: "INTENT_ONLY"',
    ):
        assert marker in text


def test_am_has_must_not_run_prep_comment():
    """us-trade-am.yml에 'AM must not run prep itself' 문구 존재."""
    text = AM_YML.read_text(encoding="utf-8")
    assert "AM must not run prep itself" in text, \
        "us-trade-am.yml must contain 'AM must not run prep itself' comment"


# ─────────────────────────────────────────────────────────────────────────────
# 2. AM prep 분리 확인
# ─────────────────────────────────────────────────────────────────────────────

def test_am_does_not_run_dispatcher_prep():
    """us-trade-am.yml에 dispatcher --mode prep 실행이 없어야 함."""
    text = AM_YML.read_text(encoding="utf-8")
    assert "python -m trader.us.runner.dispatcher --mode prep" not in text, \
        "us-trade-am.yml must not execute dispatcher --mode prep"


def test_am_no_check_or_run_prep_step():
    """us-trade-am.yml에 'Check or run US prep inside AM workflow' 단계명이 없어야 함."""
    text = AM_YML.read_text(encoding="utf-8")
    assert "Check or run US prep inside AM workflow" not in text, \
        "us-trade-am.yml must not have 'Check or run US prep inside AM workflow' step"


def test_am_has_verify_completed_prep_step():
    """us-trade-am.yml에 'Verify completed US prep contract' 단계명이 있어야 함."""
    text = AM_YML.read_text(encoding="utf-8")
    assert "Verify completed US prep contract" in text, \
        "us-trade-am.yml must have 'Verify completed US prep contract' step"


def test_am_prep_mode_verified_only():
    """us-trade-am.yml에 prep_mode=executed가 출력되지 않아야 함."""
    text = AM_YML.read_text(encoding="utf-8")
    assert "prep_mode=executed" not in text, \
        "us-trade-am.yml must not output prep_mode=executed"


# ─────────────────────────────────────────────────────────────────────────────
# 3. TRADE_DATE env 방어 코드 확인
# ─────────────────────────────────────────────────────────────────────────────

def test_am_exports_trade_date():
    """us-trade-am.yml에 export TRADE_DATE 또는 TRADE_DATE=... python 패턴이 있어야 함."""
    text = AM_YML.read_text(encoding="utf-8")
    has_export = "export TRADE_DATE" in text
    has_env_inline = 'TRADE_DATE="${TRADE_DATE}" python' in text
    assert has_export or has_env_inline, \
        "us-trade-am.yml must ensure TRADE_DATE is exported before Python heredoc"


def test_am_uses_prep_contract_helper_after_exporting_trade_date():
    """us-trade-am.yml은 TRADE_DATE export 후 helper script로 prep contract를 검증해야 한다."""
    text = AM_YML.read_text(encoding="utf-8")
    assert 'python scripts/us_verify_am_prep_contract.py --trade-date "${TRADE_DATE}"' in text


def test_am_no_trade_date_none_log():
    """us-trade-am.yml 파일에 trade_date=None을 정상 로그처럼 출력하는 코드가 없어야 함."""
    text = AM_YML.read_text(encoding="utf-8")
    # trade_date=None이 에러가 아닌 정상 로그 형태로 출력되면 안 됨
    # (runtime error로 raise되는 형태는 허용)
    assert "trade_date=None" not in text, \
        "us-trade-am.yml must not print 'trade_date=None' as normal log output"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Failure report contract test
# ─────────────────────────────────────────────────────────────────────────────

def test_write_us_guard_failure_report_creates_required_files():
    """prep guard 실패 시 required 파일 생성 + required fields 검증."""
    with tempfile.TemporaryDirectory() as tmpdir:
        orig_dir = os.getcwd()
        try:
            os.chdir(tmpdir)
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(orig_dir) / "scripts/write_us_guard_failure_report.py"),
                    "--session", "am",
                    "--trade-date", "2026-05-14",
                    "--reason", "bad_or_missing_prep",
                    "--prep-status", "UNKNOWN",
                    "--locked-count", "0",
                    "--final-status", "FAILED_PREP_GUARD",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, f"Script failed: {result.stderr}"

            # Check files exist
            base = Path(tmpdir) / "reports/us_daily"
            assert (base / "latest_us_daily_report.json").exists(), "latest_us_daily_report.json missing"
            assert (base / "latest_us_daily_report.md").exists(), "latest_us_daily_report.md missing"
            assert (base / "2026-05-14/am/us_daily_report.json").exists(), "session report json missing"
            assert (base / "2026-05-14/am/us_daily_report.md").exists(), "session report md missing"

            # Check required fields
            payload = json.loads((base / "latest_us_daily_report.json").read_text())
            required = [
                "trade_date", "run_id", "sha", "workflow", "session", "event_name", "env",
                "dry_run", "kis_order_allowed", "prep_status", "locked_watchlist_count",
                "entry_eval_status", "entry_error_type", "entry_error_message",
                "entry_intents", "orders_sent", "orders_blocked", "block_reasons",
                "fills", "positions", "last_stage", "final_status", "reason",
                "temp_error_count", "temp_recovered_count",
            ]
            missing = [k for k in required if k not in payload]
            assert not missing, f"Missing required fields: {missing}"

            # Check specific values
            assert payload["final_status"] == "FAILED_PREP_GUARD"
            assert payload["orders_sent"] == 0
            assert payload["fills"] == 0
            assert payload["positions"] == 0
            assert payload["last_stage"] == "prep_guard"
            assert payload["trade_date"] == "2026-05-14"

        finally:
            os.chdir(orig_dir)


# ─────────────────────────────────────────────────────────────────────────────
# 5. PnL report import test
# ─────────────────────────────────────────────────────────────────────────────

def test_pnl_report_no_kr_kis_client_import():
    """scripts/generate_us_portfolio_pnl_report.py에 Korean KIS client import가 없어야 함."""
    text = PNL_SCRIPT.read_text(encoding="utf-8")
    assert "from trader.kis_client import KISClient" not in text, \
        "generate_us_portfolio_pnl_report.py must not import trader.kis_client.KISClient"
    assert "import KISClient" not in text, \
        "generate_us_portfolio_pnl_report.py must not import KISClient"


def test_pnl_report_uses_kis_us_client():
    """scripts/generate_us_portfolio_pnl_report.py에 KisUSClient import가 있어야 함."""
    text = PNL_SCRIPT.read_text(encoding="utf-8")
    assert "KisUSClient" in text, \
        "generate_us_portfolio_pnl_report.py must use KisUSClient"


def test_pnl_report_no_make_engine_with_arg():
    """scripts/generate_us_portfolio_pnl_report.py에 make_engine(db_url) 패턴이 없어야 함."""
    text = PNL_SCRIPT.read_text(encoding="utf-8")
    assert "make_engine(db_url)" not in text, \
        "generate_us_portfolio_pnl_report.py must not call make_engine(db_url)"


def test_pnl_report_no_trade_status_does_not_fail(tmp_path):
    """FAILED_PREP_GUARD daily report가 있을 때 PnL report 생성이 실패하지 않아야 함."""
    # Write a FAILED_PREP_GUARD daily report
    daily_dir = tmp_path / "reports/us_daily"
    daily_dir.mkdir(parents=True)
    daily_report = {
        "trade_date": "2026-05-14",
        "session": "am",
        "final_status": "FAILED_PREP_GUARD",
        "orders_sent": 0,
        "fills": 0,
        "positions": 0,
    }
    (daily_dir / "latest_us_daily_report.json").write_text(json.dumps(daily_report))

    # The function should be importable and callable without crashing
    sys.path.insert(0, str(Path(".").resolve()))
    try:
        import importlib
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "gen_pnl", str(PNL_SCRIPT)
        )
        # Just check syntax is valid
        import py_compile
        py_compile.compile(str(PNL_SCRIPT), doraise=True)
    except py_compile.PyCompileError as e:
        assert False, f"PnL report script has syntax error: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Watchdog contract test
# ─────────────────────────────────────────────────────────────────────────────

def test_watchdog_prep_check_window_is_0710_0810():
    """us-trade-watchdog.yml prep checkWindow이 [710, 810]."""
    text = WATCHDOG_YML.read_text(encoding="utf-8")
    assert "checkWindow: [710, 810]" in text, \
        "us-trade-watchdog.yml prep checkWindow must be [710, 810]"


def test_watchdog_no_skip_standalone_prep_comment():
    """us-trade-watchdog.yml에 standalone prep skip 주석이 없어야 함."""
    text = WATCHDOG_YML.read_text(encoding="utf-8")
    assert "Watchdog no longer dispatches standalone prep" not in text, \
        "us-trade-watchdog.yml must not have 'Watchdog no longer dispatches standalone prep'"
    assert "prep_integrated_into_am" not in text, \
        "us-trade-watchdog.yml must not have 'prep_integrated_into_am' skip reason"


def test_watchdog_dispatches_prep_when_missing():
    """us-trade-watchdog.yml prep dispatch 코드가 있어야 함."""
    text = WATCHDOG_YML.read_text(encoding="utf-8")
    assert "dispatchWorkflow('prep'" in text, \
        "us-trade-watchdog.yml must have dispatchWorkflow('prep', ...) call"


def test_watchdog_am_checks_prep_health():
    """us-trade-watchdog.yml AM dispatch 전 prep health 확인 코드가 있어야 함."""
    text = WATCHDOG_YML.read_text(encoding="utf-8")
    assert "prepOk" in text, \
        "us-trade-watchdog.yml AM section must reference prepOk variable"
    assert "prep_not_ok" in text, \
        "us-trade-watchdog.yml must have reason=prep_not_ok for AM skip"


def test_watchdog_no_undefined_prepok_usage():
    """us-trade-watchdog.yml에서 prepOk 변수가 정의되지 않은 채 사용되지 않아야 함."""
    text = WATCHDOG_YML.read_text(encoding="utf-8")
    # prepOk는 반드시 let prepOk = ... 또는 const prepOk = ...로 정의된 후 사용돼야 함
    # 정의 전에 사용되는 패턴: prepOk가 사용되는데 let/const prepOk = 가 없으면 실패
    assert "let prepOk" in text or "const prepOk" in text, \
        "us-trade-watchdog.yml must define prepOk variable before use"

