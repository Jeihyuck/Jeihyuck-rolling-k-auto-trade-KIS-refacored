from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from trader.kr.infinite.models import Action, Decision
from trader.kr.infinite.runner import run_once
from tests.kr.infinite.test_runner_integration import FakeKIS, FakeRepository, REGIME, DAY, active, config


KST = ZoneInfo("Asia/Seoul")


@pytest.fixture
def armed_practice_env(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")


@pytest.mark.parametrize("action", [Action.BUY, Action.RECOVERY])
def test_opening_gate_blocks_actual_infinite_buy_submission(monkeypatch, armed_practice_env, action, caplog):
    caplog.set_level("INFO")
    decision = Decision(action, "TEST_ENTRY", qty=1, notional=100, idempotency_key=f"{action.value}-key")
    monkeypatch.setattr("trader.kr.infinite.runner.evaluate", lambda **_kwargs: decision)
    kis, repo = FakeKIS(qty=6, average=100), FakeRepository(active())

    result = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME,
                      trade_date=DAY, kis_env="practice",
                      now_kst_value=datetime(2026, 8, 14, 9, 10, tzinfo=KST))

    assert result.decision.reason == "OPENING_30MIN_BUY_BLOCK"
    assert kis.orders == []
    assert not repo.intents
    assert "[OPENING_BUY_BLOCK][KR_INF]" in caplog.text


@pytest.mark.parametrize("action,qty", [(Action.SELL_PARTIAL, 3), (Action.SELL_ALL, 6)])
def test_opening_gate_keeps_actual_infinite_sell_submission_open(monkeypatch, armed_practice_env, action, qty):
    decision = Decision(action, "TEST_EXIT", qty=qty, notional=qty * 100,
                        idempotency_key=f"{action.value}-key")
    monkeypatch.setattr("trader.kr.infinite.runner.evaluate", lambda **_kwargs: decision)
    kis, repo = FakeKIS(qty=6, average=100), FakeRepository(active())

    result = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME,
                      trade_date=DAY, kis_env="practice",
                      now_kst_value=datetime(2026, 8, 14, 9, 10, tzinfo=KST))

    assert result.submitted
    assert kis.orders == [("SELL", qty)]


def test_infinite_buy_submits_at_0930(monkeypatch, armed_practice_env):
    decision = Decision(Action.BUY, "TEST_ENTRY", qty=1, notional=100, idempotency_key="BUY-key")
    monkeypatch.setattr("trader.kr.infinite.runner.evaluate", lambda **_kwargs: decision)
    kis, repo = FakeKIS(qty=6, average=100), FakeRepository(active())

    result = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME,
                      trade_date=DAY, kis_env="practice",
                      now_kst_value=datetime(2026, 8, 14, 9, 30, tzinfo=KST))

    assert result.submitted
    assert kis.orders == [("BUY", 1)]
