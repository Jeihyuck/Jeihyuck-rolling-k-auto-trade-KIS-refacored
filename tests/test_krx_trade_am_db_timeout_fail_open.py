"""
KRX trade-am DB 타임아웃 fail-open 통합 테스트.

검증 항목:
1. safe_read_mappings: KRX+KRX_DB_READ_FAIL_OPEN=1 시 TickTimeoutError → ([], True) 반환
2. safe_read_mappings: 비-KRX 컨텍스트에서 TickTimeoutError → 재발생
3. _maybe_reconcile_practice_account_state: positions_repo.list_positions 타임아웃 시 skipped 반환
4. PositionsRepo 계열 메서드가 _safe_repo_read 경로를 사용하는지 확인
5. trade-am.yml에 필수 KRX env 설정 존재 / US 워크플로우에 KRX env 부재
"""

from __future__ import annotations

import os
import sys
import yaml
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ────────────────────────────────────────────────────
# 헬퍼: TickTimeoutError 생성
# ────────────────────────────────────────────────────

def _make_tick_timeout(msg: str = "tick_hard_timeout timeout_sec=90 last_stage=account_reconcile.positions_lookup.start"):
    from trader.pb1_runner import TickTimeoutError
    return TickTimeoutError(msg)


# ────────────────────────────────────────────────────
# 테스트 1: KRX + fail-open 시 TickTimeoutError → ([], True)
# ────────────────────────────────────────────────────

class _BoomEngine:
    """connect()를 호출하면 TickTimeoutError를 발생시키는 더미 엔진."""

    def __init__(self, exc):
        self._exc = exc

    def connect(self):
        raise self._exc

    def dispose(self):
        pass


def test_safe_read_mappings_krx_tick_timeout_fail_open():
    """KRX 컨텍스트 + KRX_DB_READ_FAIL_OPEN=1 → TickTimeoutError 발생 시 ([], True) 반환."""
    import sqlalchemy as sa
    from trader.db.engine import safe_read_mappings

    exc = _make_tick_timeout()
    engine = _BoomEngine(exc)

    env_patch = {
        "PB1_MARKET_SCOPE": "KRX",
        "KRX_DB_READ_FAIL_OPEN": "1",
        "GITHUB_WORKFLOW": "",
        "MARKET": "",
        "EXCHANGE": "",
        "TRADE_MARKET": "",
    }
    with patch.dict(os.environ, env_patch, clear=False):
        rows, fail_open_flag = safe_read_mappings(
            engine,
            sa.text("SELECT 1"),
            op_name="positions.list_positions",
            fail_open=False,  # 환경 변수에 의해 override 됨
        )

    assert rows == []
    assert fail_open_flag is True


def test_safe_read_mappings_krx_tick_timeout_fail_open_via_arg():
    """fail_open=True 인수 전달 시에도 TickTimeoutError → ([], True)."""
    import sqlalchemy as sa
    from trader.db.engine import safe_read_mappings

    exc = _make_tick_timeout()
    engine = _BoomEngine(exc)

    env_patch = {
        "PB1_MARKET_SCOPE": "KRX",
        "KRX_DB_READ_FAIL_OPEN": "0",
        "GITHUB_WORKFLOW": "",
        "MARKET": "",
        "EXCHANGE": "",
        "TRADE_MARKET": "",
    }
    with patch.dict(os.environ, env_patch, clear=False):
        rows, fail_open_flag = safe_read_mappings(
            engine,
            sa.text("SELECT 1"),
            op_name="positions.list_positions",
            fail_open=True,  # 직접 지정
        )

    assert rows == []
    assert fail_open_flag is True


# ────────────────────────────────────────────────────
# 테스트 2: 비-KRX 컨텍스트에서 TickTimeoutError → 재발생
# ────────────────────────────────────────────────────

def test_safe_read_mappings_non_krx_tick_timeout_reraises():
    """비-KRX 컨텍스트(US)에서 TickTimeoutError는 잡지 않고 re-raise 해야 한다."""
    import sqlalchemy as sa
    from trader.pb1_runner import TickTimeoutError
    from trader.db.engine import safe_read_mappings

    exc = _make_tick_timeout()
    engine = _BoomEngine(exc)

    env_patch = {
        "PB1_MARKET_SCOPE": "US",
        "KRX_DB_READ_FAIL_OPEN": "0",
        "GITHUB_WORKFLOW": "",
        "MARKET": "US",
        "EXCHANGE": "NASDAQ",
        "TRADE_MARKET": "",
    }
    with patch.dict(os.environ, env_patch, clear=False):
        with pytest.raises(TickTimeoutError):
            safe_read_mappings(
                engine,
                sa.text("SELECT 1"),
                op_name="positions.list_positions",
                fail_open=False,
            )


def test_safe_read_mappings_no_scope_tick_timeout_reraises():
    """PB1_MARKET_SCOPE 미설정 + GITHUB_WORKFLOW 미설정 시에도 KRX 아님 → re-raise."""
    import sqlalchemy as sa
    from trader.pb1_runner import TickTimeoutError
    from trader.db.engine import safe_read_mappings

    exc = _make_tick_timeout()
    engine = _BoomEngine(exc)

    env_to_clear = {
        "PB1_MARKET_SCOPE": "",
        "KRX_DB_READ_FAIL_OPEN": "0",
        "GITHUB_WORKFLOW": "",
        "MARKET": "",
        "EXCHANGE": "",
        "TRADE_MARKET": "",
    }
    with patch.dict(os.environ, env_to_clear, clear=False):
        with pytest.raises(TickTimeoutError):
            safe_read_mappings(
                engine,
                sa.text("SELECT 1"),
                op_name="positions.list_positions",
                fail_open=False,
            )


# ────────────────────────────────────────────────────
# 테스트 3: _maybe_reconcile_practice_account_state 타임아웃 시 skipped 반환
# ────────────────────────────────────────────────────

def test_maybe_reconcile_positions_timeout_returns_skipped():
    """positions_repo.list_positions가 TickTimeoutError를 발생시키면
    _maybe_reconcile_practice_account_state는 skipped=True 를 반환해야 한다."""
    from trader.pb1_runner import TickTimeoutError

    # _maybe_reconcile_practice_account_state를 직접 임포트하려면
    # 모듈 전체를 가져와야 하므로 패턴을 확인한다.
    # 함수가 존재하는지와 TickTimeoutError를 잡는지를 코드 수준에서 검증.
    import inspect
    import trader.pb1_runner as _runner

    assert hasattr(_runner, "_maybe_reconcile_practice_account_state"), (
        "_maybe_reconcile_practice_account_state 함수가 pb1_runner에 존재해야 한다"
    )

    src = inspect.getsource(_runner._maybe_reconcile_practice_account_state)
    assert "TickTimeoutError" in src or "except" in src, (
        "_maybe_reconcile_practice_account_state가 예외를 처리해야 한다"
    )
    assert "skipped" in src, (
        "타임아웃 시 skipped 키를 포함한 dict를 반환해야 한다"
    )


def test_maybe_reconcile_positions_timeout_no_fatal_propagation(monkeypatch):
    """positions_repo.list_positions TickTimeoutError 시 함수가 예외를 전파하지 않고
    skipped=True dict를 반환해야 한다."""
    import trader.pb1_runner as _runner
    from trader.pb1_runner import TickTimeoutError

    class _FakePositionsRepo:
        def list_positions(self, *args, **kwargs):
            raise TickTimeoutError(
                "tick_hard_timeout timeout_sec=90 last_stage=account_reconcile.positions_lookup.start"
            )

    class _FakeKisWrapper:
        account_balance = {"output2": []}
        def get_account_balance(self):
            return self.account_balance

    result = _runner._maybe_reconcile_practice_account_state(
        env="practice",
        engine=None,
        kis=None,
        balance_state="OK",
        balance_snapshot={"output1": []},  # _extract_kis_holdings_count가 기대하는 구조
        positions_repo=_FakePositionsRepo(),
    )

    assert isinstance(result, dict), "반환값은 dict여야 한다"
    assert result.get("skipped") is True, f"skipped=True여야 하는데 실제: {result}"
    assert result.get("reason") == "positions_lookup_failed", (
        f"reason='positions_lookup_failed'여야 하는데 실제: {result.get('reason')}"
    )


# ────────────────────────────────────────────────────
# 테스트 4: PositionsRepo 계열 메서드가 safe_read_mappings 경로 사용
# ────────────────────────────────────────────────────

def test_positions_repo_does_not_use_engine_begin_for_reads():
    """PositionsRepo._get_position_row 등이 engine.begin() 대신
    _safe_repo_read / safe_read_mappings 경로를 사용하는지 소스 검증."""
    import inspect
    from trader.db.repos import PositionsRepo

    src = inspect.getsource(PositionsRepo._get_position_row)
    # engine.begin()은 사용해서는 안 됨
    assert "engine.begin()" not in src, (
        "_get_position_row가 engine.begin()을 직접 사용해서는 안 된다"
    )
    # _safe_repo_read 또는 safe_read_mappings 경로여야 함
    assert "_safe_repo_read" in src or "safe_read_mappings" in src, (
        "_get_position_row는 _safe_repo_read 또는 safe_read_mappings를 사용해야 한다"
    )


def test_positions_repo_list_positions_delegates_to_safe_read():
    """PositionsRepo.list_positions 가 _safe_repo_read 경로를 사용하는지 확인."""
    import inspect
    from trader.db.repos import PositionsRepo

    src = inspect.getsource(PositionsRepo.list_positions)
    assert "_safe_repo_read" in src or "safe_read_mappings" in src, (
        "list_positions는 _safe_repo_read 또는 safe_read_mappings를 사용해야 한다"
    )


# ────────────────────────────────────────────────────
# 테스트 5: trade-am.yml / US 워크플로우 env 설정 검증
# ────────────────────────────────────────────────────

WORKFLOW_ROOT = Path(__file__).resolve().parents[1] / ".github" / "workflows"

KRX_REQUIRED_VARS = {
    "PB1_MARKET_SCOPE": "KRX",
    "KRX_DB_READ_FAIL_OPEN": "1",
    "PB1_FAIL_OPEN_ON_POSITION_LOOKUP_TIMEOUT": "1",
}


def _load_workflow_env(name: str) -> dict:
    """워크플로우 YAML에서 최상위 env 블록을 로드한다."""
    path = WORKFLOW_ROOT / name
    with path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    return doc.get("env") or {}


def test_trade_am_yml_has_krx_env_vars():
    """trade-am.yml 최상위 env에 KRX 필수 변수들이 설정되어야 한다."""
    env = _load_workflow_env("trade-am.yml")
    for key, expected in KRX_REQUIRED_VARS.items():
        assert str(env.get(key)) == expected, (
            f"trade-am.yml env.{key}={env.get(key)!r}, 기대값: {expected!r}"
        )


def test_trade_afternoon_yml_has_krx_env_vars():
    """trade-afternoon.yml 최상위 env에 KRX 필수 변수들이 설정되어야 한다."""
    env = _load_workflow_env("trade-afternoon.yml")
    for key, expected in KRX_REQUIRED_VARS.items():
        assert str(env.get(key)) == expected, (
            f"trade-afternoon.yml env.{key}={env.get(key)!r}, 기대값: {expected!r}"
        )


def test_trade_am_yml_no_duplicate_env_keys():
    """trade-am.yml 전역 env 블록에 중복 키가 없어야 한다."""
    path = WORKFLOW_ROOT / "trade-am.yml"
    with path.open(encoding="utf-8") as f:
        content = f.read()
    # 전역 env 블록만 추출 (파일 최상위 env: 아래 indent==2 키)
    in_global_env = False
    seen: dict[str, int] = {}
    for lineno, line in enumerate(content.splitlines(), start=1):
        raw_stripped = line.strip()
        if not raw_stripped or raw_stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        # 전역 env 섹션 시작
        if indent == 0 and raw_stripped == "env:":
            in_global_env = True
            continue
        # indent 0의 다른 섹션이 시작되면 전역 env 끝
        if indent == 0 and raw_stripped != "env:":
            in_global_env = False
            continue
        if in_global_env and indent == 2 and ":" in raw_stripped:
            key = raw_stripped.split(":")[0].strip()
            if key in seen:
                pytest.fail(
                    f"trade-am.yml 전역 env 중복 키 '{key}' at line {lineno} "
                    f"(first seen at line {seen[key]})"
                )
            seen[key] = lineno


US_FORBIDDEN_KRX_VARS = ["PB1_MARKET_SCOPE", "KRX_DB_READ_FAIL_OPEN", "KRX_RECONCILE_FAIL_OPEN"]


@pytest.mark.parametrize("wf_name", ["us-trade.yml", "us-prep.yml", "us-trade-am.yml"])
def test_us_workflow_no_krx_env_vars(wf_name):
    """US 워크플로우에 KRX 전용 env 변수가 없어야 한다."""
    path = WORKFLOW_ROOT / wf_name
    if not path.exists():
        pytest.skip(f"{wf_name} 파일이 없음 — 생성 시 재검증 필요")
    with path.open(encoding="utf-8") as f:
        content = f.read()
    for var in US_FORBIDDEN_KRX_VARS:
        assert var not in content, (
            f"{wf_name}에 KRX 전용 변수 '{var}'가 포함되어 있으면 안 된다"
        )
