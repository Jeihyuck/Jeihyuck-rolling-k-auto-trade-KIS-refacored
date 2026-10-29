"""Stage4 0060 schema must be verified before WORKER starts, not after failure."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts.us_stage123_paper_preflight import (
    stage4_starting_constraint_supported,
)


def test_stage4_0060_constraint_only_accepts_starting_capability():
    assert stage4_starting_constraint_supported(
        "CHECK ((worker_state = ANY (ARRAY['STARTING'::text, "
        "'READY'::text, 'FAILED'::text, 'STOPPED'::text])))"
    )
    assert not stage4_starting_constraint_supported(
        "CHECK ((worker_state = ANY (ARRAY['READY'::text, "
        "'FAILED'::text, 'STOPPED'::text])))"
    )
    assert not stage4_starting_constraint_supported(None)
    assert not stage4_starting_constraint_supported("")


def test_stage4_practice_preflight_checks_0060_before_worker_launch():
    source = (
        Path(__file__).resolve().parents[2] / "scripts" /
        "us_stage123_paper_preflight.py"
    ).read_text(encoding="utf-8")
    assert "paper_stage4_migration_0060_starting_state_missing" in source
    assert "us_stage4_reconcile_health_worker_state_check" in source
    assert "stage4_starting_constraint_supported(state_check)" in source


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"),
                    reason="requires real PostgreSQL integration")
def test_stage4_0060_check_agrees_with_actual_migrated_postgres():
    from sqlalchemy import create_engine, text
    engine = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True)
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                (Path(__file__).resolve().parents[2] / "migrations" /
                 "0059_us_stage4_reconcile_health.sql").read_text()
            )
            conn.exec_driver_sql(
                (Path(__file__).resolve().parents[2] / "migrations" /
                 "0060_us_stage4_starting_health.sql").read_text()
            )
            actual = conn.execute(text("""
                SELECT pg_get_constraintdef(oid) FROM pg_constraint
                WHERE conrelid='public.us_stage4_reconcile_health'::regclass
                  AND conname='us_stage4_reconcile_health_worker_state_check'
            """)).scalar_one()
            assert stage4_starting_constraint_supported(actual)
    finally:
        engine.dispose()
