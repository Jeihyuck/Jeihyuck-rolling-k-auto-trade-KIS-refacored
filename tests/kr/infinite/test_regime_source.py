from datetime import date, timedelta
from pathlib import Path

from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.regime_source import produce_current_regime
from trader.kr.infinite.runner import load_regime, run_once
from trader.kr.infinite.models import Action
from .test_runner_integration import FakeKIS, FakeRepository


class CandleKIS(FakeKIS):
    def get_daily_candles(self, symbol, count=260):
        start = date(2025, 9, 1)
        return [{"date": (start + timedelta(days=index)).strftime("%Y%m%d"),
                 "close": 100 + index} for index in range(260)]


def test_clean_standalone_runtime_produces_current_regime_and_buys(tmp_path, monkeypatch):
    path = tmp_path / "kr_regime_snapshot.json"
    kis = CandleKIS(fill_qty=100)
    # The same producer invoked by the workflow creates the previously absent artifact.
    produce_current_regime(kis, path=path, trade_date=date(2026, 5, 18))
    monkeypatch.setenv("KR_INFINITE_REGIME_PATH", str(path))
    state, quality = load_regime()
    assert state in {"KR_NORMAL", "KR_RISK_ON", "KR_STRONG_RISK_ON"} and quality == "OK"
    result = run_once(config=InfiniteConfig(enabled=True), kis=kis, repository=FakeRepository(),
                      regime_provider=load_regime, trade_date=date(2026, 5, 18), kis_env="practice")
    assert result.decision.action == Action.BUY


def test_workflow_builds_regime_before_runner():
    workflow = Path(".github/workflows/kr-infinite.yml").read_text()
    producer = "python -m trader.kr.infinite.regime_source"
    runner = "python -m trader.kr.infinite.runner"
    assert producer in workflow and runner in workflow and workflow.index(producer) < workflow.index(runner)
