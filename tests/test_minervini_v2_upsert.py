from __future__ import annotations

import sqlalchemy as sa

from trader.db.repos_minervini_v2 import upsert_minervini_v2


def _create_table(engine: sa.Engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS signals_minervini_daily_v2 (
      env TEXT NOT NULL,
      as_of TEXT NOT NULL,
      symbol TEXT NOT NULL,
      benchmark TEXT NOT NULL DEFAULT '229200',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      run_id TEXT NULL,
      regime_pass INTEGER NOT NULL DEFAULT 0,
      rs63 REAL NULL,
      rs126 REAL NULL,
      rs_score REAL NULL,
      rs_pctile REAL NULL,
      rs_pass INTEGER NOT NULL DEFAULT 0,
      close REAL NULL,
      ma50 REAL NULL,
      ma150 REAL NULL,
      ma200 REAL NULL,
      ma200_up INTEGER NOT NULL DEFAULT 0,
      high_52w REAL NULL,
      near_52w_high INTEGER NOT NULL DEFAULT 0,
      trend_template_pass INTEGER NOT NULL DEFAULT 0,
      pivot_price REAL NULL,
      contraction_count INTEGER NOT NULL DEFAULT 0,
      c1_pct REAL NULL,
      c2_pct REAL NULL,
      c3_pct REAL NULL,
      tight_pct REAL NULL,
      atr14 REAL NULL,
      atr_pct REAL NULL,
      vol_shrink_ratio REAL NULL,
      vcp_pass INTEGER NOT NULL DEFAULT 0,
      vcp_reasons TEXT NOT NULL DEFAULT '[]',
      pass INTEGER NOT NULL DEFAULT 0,
      reject_reasons TEXT NOT NULL DEFAULT '[]',
      PRIMARY KEY (env, as_of, symbol)
    )
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)


def test_upsert_minervini_v2_count() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    _create_table(engine)

    rows = [
        {
            "symbol": "005930",
            "benchmark": "229200",
            "regime_pass": True,
            "rs63": 1.1,
            "rs126": 1.2,
            "rs_score": 1.15,
            "rs_pctile": 90.0,
            "rs_pass": True,
            "close": 70000.0,
            "ma50": 68000.0,
            "ma150": 65000.0,
            "ma200": 64000.0,
            "ma200_up": True,
            "high_52w": 72000.0,
            "near_52w_high": True,
            "trend_template_pass": True,
            "pivot_price": 71500.0,
            "contraction_count": 2,
            "c1_pct": 0.32,
            "c2_pct": 0.24,
            "c3_pct": 0.16,
            "tight_pct": 0.03,
            "atr14": 1200.0,
            "atr_pct": 0.017,
            "vol_shrink_ratio": 0.65,
            "vcp_pass": True,
            "vcp_reasons": [],
            "pass": True,
            "reject_reasons": [],
        },
        {
            "symbol": "000660",
            "benchmark": "229200",
            "regime_pass": True,
            "rs63": 1.0,
            "rs126": 1.0,
            "rs_score": 1.0,
            "rs_pctile": 50.0,
            "rs_pass": False,
            "close": 120000.0,
            "ma50": 110000.0,
            "ma150": 100000.0,
            "ma200": 98000.0,
            "ma200_up": True,
            "high_52w": 130000.0,
            "near_52w_high": True,
            "trend_template_pass": True,
            "pivot_price": 128000.0,
            "contraction_count": 1,
            "c1_pct": 0.30,
            "c2_pct": 0.26,
            "c3_pct": 0.19,
            "tight_pct": 0.05,
            "atr14": 2500.0,
            "atr_pct": 0.021,
            "vol_shrink_ratio": 0.95,
            "vcp_pass": False,
            "vcp_reasons": ["vcp_volume_not_shrinking"],
            "pass": False,
            "reject_reasons": ["rs_below_cut", "vcp_volume_not_shrinking"],
        },
    ]

    count = upsert_minervini_v2(engine, "practice", "2026-02-20", "run-1", rows)
    assert count == 2

    with engine.connect() as conn:
        total = conn.execute(sa.text("SELECT COUNT(*) FROM signals_minervini_daily_v2")).scalar()
    assert int(total or 0) == 2
