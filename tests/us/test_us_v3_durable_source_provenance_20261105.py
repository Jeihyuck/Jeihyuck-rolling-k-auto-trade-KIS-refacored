"""v3 source provenance across a real Tick snapshot and persisted claim schema."""
from datetime import datetime, timezone

import sqlalchemy as sa

from trader.us.pb1.swing_runner_exit_v3 import (
    V3_RUNNER_POLICY, evaluate_v3_from_position,
)
from trader.us.db.execution_claim_schema import metadata, us_execution_claims
from trader.us.execution.v3_stage_truth import read_v3_lifecycle_stage_states


NOW = datetime(2026, 11, 5, 15, 0, tzinfo=timezone.utc)
DAY = "2026-11-05"


def _position(*, trend_date=DAY, latest_high=112.0, wrong_life=False):
    return {
        "symbol": "SPY", "qty": 10, "orderable_qty": 10,
        "entry_price": 100, "position_lifecycle_id": "life-verified",
        "position_cycle_id": "life-verified", "portfolio_epoch_id": "portfolio-verified",
        "high_watermark": latest_high,
        "high_watermark_at": "2026-11-05T14:59:00+00:00",
        "high_watermark_lifecycle_id": "life-other" if wrong_life else "life-verified",
        "high_watermark_source": "us_position_risk_state",
        "risk_state": {"state": {
            "lifecycle": {
                "lifecycle_id": "life-verified",
                "high_watermark": 110.0,
                "high_watermark_at": "2026-11-05T14:00:00+00:00",
                "high_watermark_source": "us_position_risk_state",
            },
            "trend": {
                "lifecycle_id": "life-verified",
                "last_daily_update_trade_date": trend_date,
                "trend_state": "EXIT",
                "trend_data_quality": "ok",
                "daily_history_quality": "OK",
            },
        }},
    }


def _patch(monkeypatch):
    monkeypatch.setattr(
        "trader.us.pb1.v3_atr_readonly_cache.load_verified_kis_daily_cache",
        lambda **kw: [],
    )
    monkeypatch.setattr(
        "trader.us.execution.v3_stage_truth.read_v3_lifecycle_stage_states",
        lambda **kw: {
            "PROFIT_DEFENSE_PARTIAL": "NOT_TRIGGERED",
            "RUNNER_FINAL": "NOT_TRIGGERED",
        },
    )


def _eval(pos, price=109):
    return evaluate_v3_from_position(
        position=pos, current_price=price, now=NOW,
        contract={"sha256": "a"*64,
                  "management": {"runner_exit": V3_RUNNER_POLICY}},
    )


def test_later_durable_hwm_and_persisted_trend_produce_one_verified_r1(monkeypatch):
    _patch(monkeypatch)
    decision = _eval(_position())
    assert decision["action"] == "SELL"
    assert decision["reason"] == "R1_TREND_EXIT"
    assert decision["qty"] == 10
    assert abs(decision["peak_gain"] - 0.12) < 1e-12
    assert decision["high_watermark_observed_at"] == "2026-11-05T14:59:00+00:00"
    assert decision["trend_verified"] is True


def test_cross_lifecycle_forged_new_hwm_fails_back_to_older_durable_proof(monkeypatch):
    _patch(monkeypatch)
    decision = _eval(_position(wrong_life=True))
    assert decision["action"] == "SELL"
    assert abs(decision["peak_gain"] - 0.10) < 1e-12
    assert decision["high_watermark_observed_at"] == "2026-11-05T14:00:00+00:00"


def test_stale_trend_not_upgraded_to_exit(monkeypatch):
    _patch(monkeypatch)
    decision = _eval(_position(trend_date="2026-11-04"))
    assert decision["action"] == "HOLD"
    assert decision["trend_verified"] is False


def test_v3_stage_reader_matches_sql_schema_and_survives_new_epoch(monkeypatch):
    from trader.us.db import repos
    db = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(db)
    with db.begin() as conn:
        conn.execute(us_execution_claims.insert().values(
            action_key="a"*64,
            env="practice", account_id="account-hash", market="US",
            trading_epoch_id="original-trade-epoch",
            portfolio_epoch_id="portfolio-verified",
            position_cycle_id="life-verified",
            strategy_owner="US_STANDARD", lifecycle_id="life-verified",
            action="PROFIT_DEFENSE_PARTIAL", action_instance="b"*64,
            action_state="SATISFIED", target_qty=5,
            cumulative_filled_qty=5, remaining_target_qty=0,
            filled_qty_before_attempt=0,
        ))
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: db)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": "account-hash",
        "market": "US", "trading_epoch_id": "new-trade-epoch",
    })
    stages = read_v3_lifecycle_stage_states(
        lifecycle_id="life-verified", position_cycle_id="life-verified",
        portfolio_epoch_id="portfolio-verified", contract_sha="b"*64,
    )
    assert stages == {
        "PROFIT_DEFENSE_PARTIAL": "DONE",
        "RUNNER_FINAL": "NOT_TRIGGERED",
    }
    # An authentic new lifecycle must not inherit an old spent stage.
    assert read_v3_lifecycle_stage_states(
        lifecycle_id="new-life", position_cycle_id="new-life",
        portfolio_epoch_id="new-portfolio", contract_sha="b"*64,
    )["PROFIT_DEFENSE_PARTIAL"] == "NOT_TRIGGERED"
