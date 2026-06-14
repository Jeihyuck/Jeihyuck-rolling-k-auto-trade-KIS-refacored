"""
한국장 주문 가능 workflow GitHub schedule 제거 정책 검증.

자동매매 실행 주체는 Windows 작업 스케줄러 → Ubuntu WSL 스크립트로 이전되었으므로
GitHub Actions 주문 가능 workflow는 workflow_dispatch만 유지한다.
"""
from __future__ import annotations

from pathlib import Path

WORKFLOWS_DIR = Path(__file__).parent.parent / ".github" / "workflows"

KR_ORDER_WORKFLOWS = [
    "trade-prep.yml",
    "trade-am.yml",
    "trade-afternoon.yml",
    "trade-pm.yml",
    "trade-close.yml",
]

SAFE_ENV = [
    'DRY_RUN: "1"',
    'DISABLE_LIVE_TRADING: "1"',
    'LIVE_TRADING_ENABLED: "0"',
    'STRATEGY_MODE: "INTENT_ONLY"',
    'FORCE_STRATEGY_MODE: "INTENT_ONLY"',
]


def _load_workflow(name: str) -> str:
    path = WORKFLOWS_DIR / name
    assert path.exists(), f"workflow file not found: {path}"
    return path.read_text(encoding="utf-8")


class TestKrWorkflowScheduleMigration:
    def test_kr_order_workflows_have_no_schedule(self):
        for name in KR_ORDER_WORKFLOWS:
            text = _load_workflow(name)
            assert "schedule:" not in text, f"{name} must not have schedule trigger"
            assert "workflow_dispatch:" in text, f"{name} must keep workflow_dispatch"

    def test_kr_order_workflows_default_safe_mode(self):
        for name in KR_ORDER_WORKFLOWS:
            text = _load_workflow(name)
            for marker in SAFE_ENV:
                assert marker in text, f"{name} missing {marker}"

    def test_wsl_kr_runner_exists(self):
        text = Path("scripts/wsl/run-kr-trader.sh").read_text(encoding="utf-8")
        assert 'export MARKET="KR"' in text
        assert "runtime/wsl-kr-trader.log" in text
