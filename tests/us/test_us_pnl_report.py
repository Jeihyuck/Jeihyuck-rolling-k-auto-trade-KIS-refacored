# -*- coding: utf-8 -*-
"""tests/us/test_us_pnl_report.py

US PnL Report 생성 테스트.

목적:
- PnL report가 항상 생성되는지 확인
- 데이터 부족 시에도 FAILED_PNL_REPORT 상태로 파일 생성
- JSON/MD/CSV 모두 생성
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest


def test_us_pnl_report_generates_files_even_without_data(monkeypatch, tmp_path):
    """데이터가 없어도 PnL report 파일이 생성된다."""
    # DB, KIS balance 모두 없는 상황 시뮬레이션
    monkeypatch.setenv("PBCORE_DB_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    
    output_dir = tmp_path / "us_pnl"
    
    # daily report도 없는 상황
    from scripts.generate_us_portfolio_pnl_report import generate_us_pnl_report
    
    result = generate_us_pnl_report(
        session="am",
        env="practice",
        trade_date="2026-05-12",
        output_dir=str(output_dir),
        latest_daily_report=None,
    )
    
    # status는 FAILED_PNL_REPORT여야 함
    assert result["status"] == "FAILED_PNL_REPORT"
    assert "missing_position_source" in result["warnings"]
    
    # 파일은 생성되어야 함
    assert (output_dir / "latest_us_pnl_report.json").exists()
    assert (output_dir / "latest_us_pnl_report.md").exists()
    assert (output_dir / "latest_us_pnl_report.csv").exists()
    
    # dated directory도 생성
    dated_dir = output_dir / "2026-05-12" / "am"
    assert (dated_dir / "us_pnl_report.json").exists()
    assert (dated_dir / "us_pnl_report.md").exists()
    assert (dated_dir / "us_pnl_report.csv").exists()


def test_us_pnl_report_uses_daily_report_orders_sent_total(monkeypatch, tmp_path):
    """PnL report가 daily_report의 orders_sent_total을 반영한다."""
    monkeypatch.setenv("PBCORE_DB_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    
    # mock daily report
    daily_report_dir = tmp_path / "daily"
    daily_report_dir.mkdir()
    daily_report_path = daily_report_dir / "latest_us_daily_report.json"
    daily_report_path.write_text(json.dumps({
        "trade_date": "2026-05-12",
        "session": "am",
        "orders_sent_total": 5,
        "positions": 10,  # count only
    }))
    
    output_dir = tmp_path / "us_pnl"
    
    from scripts.generate_us_portfolio_pnl_report import generate_us_pnl_report
    
    result = generate_us_pnl_report(
        session="am",
        env="practice",
        trade_date="2026-05-12",
        output_dir=str(output_dir),
        latest_daily_report=str(daily_report_path),
    )
    
    assert result["orders_sent_total"] == 5
    assert result["status"] == "FAILED_PNL_REPORT"  # 여전히 position 데이터 없음


def test_us_pnl_report_json_has_required_fields(monkeypatch, tmp_path):
    """PnL report JSON이 필수 필드를 포함한다."""
    monkeypatch.setenv("PBCORE_DB_URL", "")
    
    output_dir = tmp_path / "us_pnl"
    
    from scripts.generate_us_portfolio_pnl_report import generate_us_pnl_report
    
    result = generate_us_pnl_report(
        session="am",
        env="practice",
        trade_date="2026-05-12",
        output_dir=str(output_dir),
        latest_daily_report=None,
    )
    
    # 필수 필드 확인
    required_fields = [
        "trade_date", "session", "run_id", "env", "dry_run", "source", "status",
        "positions_count", "fills_count", "orders_sent_total",
        "total_market_value_usd", "total_cost_usd", "unrealized_pnl_usd",
        "unrealized_pnl_pct", "realized_pnl_usd", "total_pnl_usd",
        "positions", "warnings", "generated_at",
    ]
    
    for field in required_fields:
        assert field in result, f"Missing required field: {field}"
    
    # JSON 파일도 동일한 필드 포함
    json_path = output_dir / "latest_us_pnl_report.json"
    json_data = json.loads(json_path.read_text())
    
    for field in required_fields:
        assert field in json_data, f"JSON missing required field: {field}"


def test_us_pnl_report_includes_trade_expectation_metadata(monkeypatch, tmp_path):
    monkeypatch.setenv("PBCORE_DB_URL", "")
    monkeypatch.setenv("US_EXPECTED_TO_TRADE", "1")
    monkeypatch.setenv("US_TRADE_STATUS", "FAILED")
    monkeypatch.setenv("US_TRADE_RUNNER_STARTED", "0")
    monkeypatch.setenv("US_TRADE_RUNNER_BLOCK_REASON", "db_migration_failed")
    monkeypatch.setenv("US_ORDER_ALLOWED", "1")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setenv("US_SCHEDULE_EXPECTED_ET", "1130")
    monkeypatch.setenv("US_ACTUAL_START_ET", "123000")
    monkeypatch.setenv("US_DELAY_SECONDS", "0")
    monkeypatch.setenv("US_RUN_WINDOW", "manual_trade")

    output_dir = tmp_path / "us_pnl"

    from scripts.generate_us_portfolio_pnl_report import generate_us_pnl_report

    result = generate_us_pnl_report(
        session="afternoon",
        env="practice",
        trade_date="2026-06-06",
        output_dir=str(output_dir),
        latest_daily_report=None,
    )

    assert result["expected_to_trade"] == 1
    assert result["trade_runner_started"] == 0
    assert result["trade_runner_block_reason"] == "db_migration_failed"

    markdown = (output_dir / "latest_us_pnl_report.md").read_text()
    assert "**Expected To Trade**: 1" in markdown
    assert "**Trade Runner Started**: 0" in markdown
    assert "**Trade Runner Block Reason**: db_migration_failed" in markdown
    assert "**Order Allowed**: 1" in markdown
    assert "**KIS Order Allowed**: 1" in markdown
