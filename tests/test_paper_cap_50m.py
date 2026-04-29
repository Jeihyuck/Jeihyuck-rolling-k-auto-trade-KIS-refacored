"""
tests/test_paper_cap_50m.py

Paper/practice 계좌 최대 자본 cap 5천만원 검증:
- 기본값이 50,000,000인지 확인
- _resolve_entry_capital이 50M 이상 잔고를 50M으로 clamp하는지 확인
- 환경변수로 override 가능한지 확인
"""
from __future__ import annotations

import os
import importlib

import pytest


# ────────────────────────────────────────────────────────────────
# config.py 기본값 검증
# ────────────────────────────────────────────────────────────────

def test_paper_max_capital_default_is_50m(monkeypatch):
    """PAPER_MAX_CAPITAL_KRW 환경변수가 없으면 기본값은 50,000,000."""
    monkeypatch.delenv("PAPER_MAX_CAPITAL_KRW", raising=False)
    # 모듈 재로딩 없이 현재 config 값 확인
    import trader.config as cfg
    importlib.reload(cfg)
    assert cfg.PAPER_MAX_CAPITAL_KRW == 50_000_000, (
        f"Expected 50000000 but got {cfg.PAPER_MAX_CAPITAL_KRW}"
    )


def test_paper_max_capital_env_override(monkeypatch):
    """환경변수 PAPER_MAX_CAPITAL_KRW가 설정되면 그 값을 사용."""
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "30000000")
    import trader.config as cfg
    importlib.reload(cfg)
    assert cfg.PAPER_MAX_CAPITAL_KRW == 30_000_000


def test_paper_max_capital_env_50m(monkeypatch):
    """환경변수로 50M 명시 설정 확인."""
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "50000000")
    import trader.config as cfg
    importlib.reload(cfg)
    assert cfg.PAPER_MAX_CAPITAL_KRW == 50_000_000


# ────────────────────────────────────────────────────────────────
# _resolve_entry_capital clamp 검증
# ────────────────────────────────────────────────────────────────

class _FakeEngine:
    """PB1Engine의 _resolve_entry_capital만 테스트하기 위한 최소 stub."""
    def __init__(self, env: str = "practice", intended_live: bool = False):
        self.env = env
        self.intended_live = intended_live


def _make_engine(env: str = "practice"):
    """PB1Engine 인스턴스 없이 _resolve_entry_capital 테스트."""
    from trader.pb1_engine import PB1Engine
    # PB1Engine은 __init__이 복잡하므로 언바운드 메서드로 직접 호출
    fake = _FakeEngine(env=env)
    fake._resolve_entry_capital = PB1Engine._resolve_entry_capital.__get__(fake, type(fake))
    return fake


def test_clamp_to_50m_when_balance_exceeds_cap(monkeypatch):
    """
    잔고=100M, cap=50M → entry_capital=50M으로 clamp.
    로그에 [PB1][CAPITAL][CLAMP] ... cap=50000000 찍히는지는 caplog으로 확인.
    """
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "50000000")
    monkeypatch.setenv("CAP_CAP", "0")  # CAP_CAP 비활성화

    # config 재로딩
    import trader.config as cfg
    importlib.reload(cfg)
    # pb1_engine의 PAPER_MAX_CAPITAL_KRW도 갱신
    import trader.pb1_engine as eng
    importlib.reload(eng)

    fake_engine = _FakeEngine(env="practice", intended_live=False)
    entry_cap, usable, meta = eng.PB1Engine._resolve_entry_capital(
        fake_engine,
        base_cash_krw=100_000_000,
        override_capital=None,
        reserve_pct=0.10,
    )

    # usable = 50M * 0.90 = 45M
    assert entry_cap == 45_000_000, f"expected 45000000 got {entry_cap}"
    assert meta["clamp"]["cap"] == 50_000_000
    assert meta["clamp"]["after"] == 50_000_000


def test_no_clamp_when_balance_below_cap(monkeypatch):
    """
    잔고=30M < cap=50M → clamp 미발생, entry_capital = 30M * 0.90 = 27M
    """
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "50000000")
    monkeypatch.setenv("CAP_CAP", "0")

    import trader.config as cfg
    importlib.reload(cfg)
    import trader.pb1_engine as eng
    importlib.reload(eng)

    fake_engine = _FakeEngine(env="practice", intended_live=False)
    entry_cap, usable, meta = eng.PB1Engine._resolve_entry_capital(
        fake_engine,
        base_cash_krw=30_000_000,
        override_capital=None,
        reserve_pct=0.10,
    )

    assert entry_cap == 27_000_000, f"expected 27000000 got {entry_cap}"
    assert meta["clamp"] == {}  # clamp 미발생


def test_clamp_with_real_balance_69m(monkeypatch):
    """
    실제 로그 케이스: before=68592464 cap=50000000 after=50000000 reason=paper_limit
    entry_capital = 50M * 0.90 = 45M
    """
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "50000000")
    monkeypatch.setenv("CAP_CAP", "0")

    import trader.config as cfg
    importlib.reload(cfg)
    import trader.pb1_engine as eng
    importlib.reload(eng)

    fake_engine = _FakeEngine(env="practice", intended_live=False)
    entry_cap, usable, meta = eng.PB1Engine._resolve_entry_capital(
        fake_engine,
        base_cash_krw=68_592_464,
        override_capital=None,
        reserve_pct=0.10,
    )

    assert meta["clamp"]["before"] == 68_592_464
    assert meta["clamp"]["cap"] == 50_000_000
    assert meta["clamp"]["after"] == 50_000_000
    # usable = 50M * 0.90 = 45M
    assert usable == 45_000_000
    # entry_capital = 45M (no CAP_CAP)
    assert entry_cap == 45_000_000


def test_entry_budget_pct_per_tick(monkeypatch):
    """
    잔고=69M, cap=50M, reserve=10%, budget_pct=0.60
    tick_budget = 45M * 0.60 = 27M
    """
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "50000000")
    monkeypatch.setenv("PB1_ENTRY_BUDGET_PCT_PER_TICK", "0.60")
    monkeypatch.setenv("CAP_CAP", "0")

    import trader.config as cfg
    importlib.reload(cfg)
    import trader.pb1_engine as eng
    importlib.reload(eng)

    fake_engine = _FakeEngine(env="practice", intended_live=False)
    entry_cap, usable, meta = eng.PB1Engine._resolve_entry_capital(
        fake_engine,
        base_cash_krw=69_000_000,
        override_capital=None,
        reserve_pct=0.10,
    )

    budget_pct = float(os.getenv("PB1_ENTRY_BUDGET_PCT_PER_TICK", "0.60"))
    tick_budget = int(entry_cap * budget_pct)
    # 45M * 0.60 = 27M
    assert abs(tick_budget - 27_000_000) < 1000, f"expected ~27000000 got {tick_budget}"


# ────────────────────────────────────────────────────────────────
# real 환경에서는 cap 미적용
# ────────────────────────────────────────────────────────────────

def test_real_env_no_cap(monkeypatch):
    """실전(real) 환경에서는 PAPER_MAX_CAPITAL_KRW cap이 적용되지 않음."""
    monkeypatch.setenv("PAPER_MAX_CAPITAL_KRW", "50000000")
    monkeypatch.setenv("CAP_CAP", "0")

    import trader.config as cfg
    importlib.reload(cfg)
    import trader.pb1_engine as eng
    importlib.reload(eng)

    fake_engine = _FakeEngine(env="real", intended_live=True)
    entry_cap, usable, meta = eng.PB1Engine._resolve_entry_capital(
        fake_engine,
        base_cash_krw=100_000_000,
        override_capital=None,
        reserve_pct=0.10,
    )

    # real 환경: clamp 없음 → usable = 100M * 0.90 = 90M
    assert entry_cap == 90_000_000
    assert meta["clamp"] == {}
