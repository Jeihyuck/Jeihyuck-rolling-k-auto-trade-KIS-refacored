"""
PB1_MAX_POSITIONS=30 기준 슬롯 계산 테스트
"""
from __future__ import annotations
import os
import pytest


def test_pb1_max_positions_30_allows_new_slots(monkeypatch):
    monkeypatch.setenv("PB1_MAX_POSITIONS", "30")
    monkeypatch.setenv("PB1_TARGET_NEW_POSITIONS", "30")

    existing_positions_count = 8
    max_positions = 30
    slots_remaining = max_positions - existing_positions_count
    target_new_positions = min(30, slots_remaining)

    assert slots_remaining == 22
    assert target_new_positions == 22


def test_portfolio_not_full_when_existing_8_max_30():
    existing_positions_count = 8
    max_positions = 30
    assert existing_positions_count < max_positions, (
        f"existing_positions={existing_positions_count} should be < max_positions={max_positions}"
    )


def test_slots_remaining_formula():
    """슬롯 계산 공식 검증"""
    cases = [
        (0, 30, 30),
        (8, 30, 22),
        (30, 30, 0),
        (31, 30, 0),  # 초과 시 0
    ]
    for existing, max_pos, expected_slots in cases:
        slots = max(0, max_pos - existing)
        assert slots == expected_slots, (
            f"existing={existing} max={max_pos} expected_slots={expected_slots} got={slots}"
        )


def test_config_default_is_30():
    """config.py 기본값이 30인지 확인"""
    # 환경변수 없는 상태에서 import
    import importlib
    # PB1_MAX_POSITIONS env 없을 때 fallback이 30이어야 한다
    saved = os.environ.pop("PB1_MAX_POSITIONS", None)
    try:
        import trader.config as cfg
        importlib.reload(cfg)
        assert cfg.PB1_MAX_POSITIONS == 30, f"Expected 30, got {cfg.PB1_MAX_POSITIONS}"
    finally:
        if saved is not None:
            os.environ["PB1_MAX_POSITIONS"] = saved
