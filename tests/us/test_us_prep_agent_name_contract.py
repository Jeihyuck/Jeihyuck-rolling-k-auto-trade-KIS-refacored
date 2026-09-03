# -*- coding: utf-8 -*-
"""prep 저장 agent_name과 조회 agent_name의 정합성을 확인한다."""
from __future__ import annotations

import inspect


def test_prep_runner_saves_canonical_agent_name():
    """prep_runner.py가 agent_name='us_prep'으로 저장해야 한다."""
    import trader.us.runner.prep_runner as mod
    src = inspect.getsource(mod)
    assert 'agent_name="us_prep"' in src or "agent_name='us_prep'" in src, (
        "prep_runner.py에서 agent_name='us_prep'이 없다"
    )
    assert "us_prep_dual_agent" not in src, (
        "prep_runner.py에 us_prep_dual_agent가 남아 있으면 안 된다"
    )


def test_repos_load_prep_status_includes_dual_agent_name():
    """load_latest_us_prep_status()가 us_prep_dual_agent도 조회 가능해야 한다."""
    import trader.us.db.repos as mod
    src = inspect.getsource(mod.load_latest_us_prep_status)
    assert "us_prep_dual_agent" in src, (
        "load_latest_us_prep_status SQL에 us_prep_dual_agent가 없다"
    )
    assert "us_prep" in src


def test_repos_load_prep_status_requires_finished_contract():
    """A newer STARTED row must not replace the last completed contract."""
    import trader.us.db.repos as mod
    src = inspect.getsource(mod.load_latest_us_prep_status)
    assert "FINISHED_AT IS NOT NULL" in src.upper()
    assert "ORDER BY FINISHED_AT DESC" in src.upper()


def test_save_us_prep_run_default_agent_name():
    """save_us_prep_run의 기본 agent_name이 'us_prep'이어야 한다."""
    import trader.us.db.repos as mod
    src = inspect.getsource(mod.save_us_prep_run)
    assert "us_prep" in src


def test_prep_runner_saves_agent_name_via_monkeypatch(monkeypatch):
    """prep_runner가 실제로 agent_name='us_prep'을 넘기는지 monkeypatch로 확인."""
    captured = {}

    def fake_save_us_prep_run(trade_date, agent_name, mode, env):
        captured["agent_name"] = agent_name
        return "fake-run-id"

    import trader.us.runner.prep_runner as pr_mod
    monkeypatch.setattr(pr_mod, "save_us_prep_run", fake_save_us_prep_run)

    # 최소한의 mock으로 prep 흐름 진입 후 run_id 저장 구간까지 실행
    # force_now를 사용해 비거래일 가드를 우회
    try:
        pr_mod.run_prep(env="practice", offline=True, force_now="2026-06-02T09:00:00-04:00")
    except Exception:
        pass  # DB 없어도 agent_name 캡처는 가능

    assert captured.get("agent_name") == "us_prep", (
        f"agent_name={captured.get('agent_name')!r}, 'us_prep'이어야 함"
    )
