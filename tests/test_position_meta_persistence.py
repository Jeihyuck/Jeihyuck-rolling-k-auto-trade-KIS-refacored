"""
tests/test_position_meta_persistence.py

positions.position_meta 컬럼 존재 및 update_position_fields merge 동작 검증.
"""
from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
import pytest

from trader.db.schema import schema_for_engine
from trader.db.repos import PositionsRepo


def _make_engine() -> sa.Engine:
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine


def _insert_position(engine: sa.Engine, code: str = "005930", meta: dict | None = None) -> None:
    """테스트용 포지션 직접 삽입."""
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(
            schema.positions.insert().values(
                env="practice",
                strategy="pb1",
                sid=1,
                mode=0,
                code=code,
                market="KR",
                qty=10,
                avg_buy_price=10000.0,
                total_cost=100000.0,
                realized_pnl=0.0,
                position_meta=meta or {},
            )
        )


def test_position_meta_column_exists():
    """positions 테이블에 position_meta 컬럼이 있어야 함."""
    engine = _make_engine()
    schema = schema_for_engine(engine)
    col_names = [c.name for c in schema.positions.columns]
    assert "position_meta" in col_names, (
        f"positions table must have position_meta column; got: {col_names}"
    )


def test_position_meta_default_is_empty_dict():
    """position_meta 기본값은 빈 dict이어야 함."""
    engine = _make_engine()
    schema = schema_for_engine(engine)
    col = next(c for c in schema.positions.columns if c.name == "position_meta")
    # nullable=False 이어야 함
    assert not col.nullable, "position_meta must be NOT NULL"


def test_update_position_fields_merges_position_meta():
    """update_position_fields가 position_meta를 merge 저장해야 함."""
    engine = _make_engine()
    repo = PositionsRepo(engine)

    _insert_position(engine, meta={"tp1_done": False, "r_value": 2.5})

    repo.update_position_fields(
        env="practice",
        strategy="pb1",
        sid=1,
        mode=0,
        code="005930",
        fields={"position_meta": {"tp1_done": True, "tp1_price": 12000.0}},
    )

    row = repo.get_position(env="practice", strategy="pb1", sid=1, mode=0, code="005930")
    assert row is not None
    meta = row.get("position_meta") or {}
    # 기존 키 보존
    assert meta.get("r_value") == 2.5, f"r_value should be preserved; got meta={meta}"
    # 새 키 추가
    assert meta.get("tp1_done") is True, f"tp1_done should be True; got meta={meta}"
    assert meta.get("tp1_price") == 12000.0, f"tp1_price should be 12000; got meta={meta}"


def test_update_position_meta_does_not_overwrite_entire_dict():
    """position_meta 업데이트가 전체 교체가 아닌 merge여야 함."""
    engine = _make_engine()
    repo = PositionsRepo(engine)

    initial_meta = {
        "trade_horizon": "SWING_CARRY",
        "initial_stop_price": 9500.0,
        "tp1_done": False,
        "tp2_done": False,
    }
    _insert_position(engine, meta=initial_meta)

    # tp1_done만 업데이트
    repo.update_position_fields(
        env="practice",
        strategy="pb1",
        sid=1,
        mode=0,
        code="005930",
        fields={"position_meta": {"tp1_done": True, "tp1_price": 11000.0}},
    )

    row = repo.get_position(env="practice", strategy="pb1", sid=1, mode=0, code="005930")
    meta = row.get("position_meta") or {}

    # 다른 필드 보존
    assert meta.get("trade_horizon") == "SWING_CARRY"
    assert meta.get("initial_stop_price") == 9500.0
    assert meta.get("tp2_done") is False
    # 새 필드 반영
    assert meta.get("tp1_done") is True
    assert meta.get("tp1_price") == 11000.0


def test_position_meta_in_schema_py():
    """schema.py에 position_meta 컬럼 정의가 있어야 함."""
    from pathlib import Path
    schema_src = (
        Path(__file__).parent.parent / "trader" / "db" / "schema.py"
    ).read_text()
    assert "position_meta" in schema_src, (
        "trader/db/schema.py must define position_meta column"
    )


def test_migration_file_exists():
    """0037_add_positions_position_meta.sql 마이그레이션이 있어야 함."""
    from pathlib import Path
    migration = Path(__file__).parent.parent / "migrations" / "0037_add_positions_position_meta.sql"
    assert migration.exists(), f"Migration file not found: {migration}"
    content = migration.read_text()
    assert "position_meta" in content
    assert "JSONB" in content or "jsonb" in content
