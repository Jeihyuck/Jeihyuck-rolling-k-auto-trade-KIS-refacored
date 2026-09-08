from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import sqlalchemy as sa

from trader.execution_state import exit_stage_for_reason
from trader.exit_policy.router import resolve_exit_policy_for_position
from trader.kr.position_provenance import Confidence, audit_position
from trader.pb1_engine import PB1Engine, _authoritative_holding_qty as engine_holding_qty
from trader.pb1_runtime_guards import _authoritative_holding_qty as guard_holding_qty
from trader.kr.pb1.entry_submit import _authoritative_holding_qty as submit_holding_qty
from trader.reconcile_db import close_stale_positions
from trader.reconcile_kis import _commit_confirmed_sell_stage


def test_explicit_kis_zero_beats_stale_db_qty_everywhere() -> None:
    snapshot = {"kis_holding_qty": 0, "holding_qty": 43}
    assert engine_holding_qty(snapshot) == 0
    assert guard_holding_qty(snapshot) == 0
    assert submit_holding_qty(snapshot) == 0


def test_order_skip_unknown_reason_and_ledger_failure_are_fail_soft() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine._display_code = lambda code: code

    def fail_ledger(*args, **kwargs):
        raise RuntimeError("telemetry unavailable")

    engine._append_ledger_event = fail_ledger
    engine._log_order_skip(
        SimpleNamespace(code="036930"),
        ["NEW_UNKNOWN_SKIP_REASON"],
        "PB1-AM",
    )


def _confirmed_plan() -> dict:
    return {
        "entry_thesis": "pullback confirmation",
        "trade_horizon": "SWING_CARRY",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "policy_version": "pb1_entry_exit_plan_v1",
        "risk_plan": {"initial_stop": 95.0},
    }


def _buy_fill(*, plan: dict | None = None) -> dict:
    request_json = {
        "entry_meta": {
            "entry_reason": "ENTRY_PULLBACK",
            "entry_style_selected": "ENTRY_PULLBACK",
            "stop_price_at_entry": 95.0,
        }
    }
    if plan is not None:
        request_json["entry_exit_plan"] = plan
    return {
        "fill_id": "buy-1",
        "order_id": "order-buy-1",
        "code": "123456",
        "side": "BUY",
        "qty": 10,
        "price": 100.0,
        "filled_at": "2026-08-20T01:00:00+00:00",
        "request_json": request_json,
    }


def test_provenance_last_flat_replay_confirmed_only_with_exact_qty_avg_and_plan() -> None:
    fills = [
        _buy_fill(plan=_confirmed_plan()),
        {
            "fill_id": "sell-1",
            "code": "123456",
            "side": "SELL",
            "qty": 4,
            "price": 120.0,
            "filled_at": "2026-09-01T01:00:00+00:00",
        },
    ]
    audit = audit_position(
        code="123456",
        broker_qty=6,
        broker_avg=100.0,
        fills=fills,
    )
    assert audit.confidence is Confidence.CONFIRMED
    assert audit.reconstructed_qty == 6
    assert audit.reconstructed_avg == 100.0
    assert audit.updates is not None
    assert audit.updates["entry_reason"] == "ENTRY_PULLBACK"
    assert audit.updates["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert audit.updates["entry_exit_plan_json"]["policy_version"] == "pb1_entry_exit_plan_v1"


def test_provenance_does_not_invent_entry_reason_when_original_plan_missing() -> None:
    audit = audit_position(
        code="123456",
        broker_qty=10,
        broker_avg=100.0,
        fills=[_buy_fill(plan=None)],
    )
    assert audit.confidence is Confidence.PARTIAL
    assert audit.reason == "ORIGINAL_ENTRY_EXIT_PLAN_MISSING"
    assert audit.updates is None


def test_provenance_qty_mismatch_is_ambiguous_and_not_repaired() -> None:
    audit = audit_position(
        code="123456",
        broker_qty=9,
        broker_avg=100.0,
        fills=[_buy_fill(plan=_confirmed_plan())],
    )
    assert audit.confidence is Confidence.AMBIGUOUS
    assert audit.reason == "QTY_MISMATCH"
    assert audit.updates is None


def test_policy_missing_uses_separate_current_hold_policy_without_mutating_origin() -> None:
    pos = {
        "code": "123456",
        "qty": 10,
        "avg_buy_price": 100.0,
        "exit_policy_family": "POLICY_MISSING",
    }
    policy = resolve_exit_policy_for_position(
        pos=pos,
        features={
            "momentum_score": 82.0,
            "rs_percentile": 88.0,
            "vcp_score": 71.0,
            "volume_ratio": 1.4,
        },
        holding_ctx={
            "mark": 120.0,
            "current_return_pct": 20.0,
            "days_held": 0,
        },
        market_ctx={"ma20": 110.0, "ma50": 105.0, "regime": "BULL"},
    )
    assert "entry_reason" not in pos
    assert policy["policy_missing"] is True
    assert policy["policy_source"] == "LEGACY_CURRENT_REEVALUATION"
    assert policy["original_entry_reason_unknown"] is True
    assert policy["exit_family"] == "SWING_STAGED_EXIT"
    assert policy["current_hold_class"] == "CURRENT_SWING_STRONG"
    assert policy["time_stop_enabled"] is False


def test_missing_ma50_is_never_treated_as_strong_trend() -> None:
    policy = resolve_exit_policy_for_position(
        pos={
            "code": "123456",
            "qty": 10,
            "avg_buy_price": 100.0,
            "entry_style_selected": "ENTRY_PULLBACK",
            "exit_policy_family": "SWING_STAGED_EXIT",
        },
        features={},
        holding_ctx={"mark": 120.0, "current_return_pct": 20.0, "days_held": 4},
        market_ctx={"ma20": 110.0, "ma50": None, "regime": "BULL"},
    )
    assert policy["trend_ok"] is True
    assert policy["trend_strong"] is False
    assert policy["trend_data_complete"] is False


def test_router_percent_tp_reasons_classify_as_partial_stages() -> None:
    assert exit_stage_for_reason(
        "SWING_PCT_TP1",
        requested_sell_qty=4,
        broker_qty_before=15,
    ) == "TP1"
    assert exit_stage_for_reason(
        "EXIT_SWING_TP2",
        requested_sell_qty=3,
        broker_qty_before=11,
    ) == "TP2"
    assert exit_stage_for_reason(
        "SWING_PCT_TP1",
        requested_sell_qty=15,
        broker_qty_before=15,
    ) == "FULL_EXIT"


def test_exit_stage_metadata_commits_only_from_confirmed_sell_evidence() -> None:
    repo = MagicMock()
    repo.get_position.return_value = {
        "sid": 1,
        "mode": 1,
        "position_meta": {"max_pnl_pct_since_entry": 12.0},
    }
    committed = _commit_confirmed_sell_stage(
        env="practice",
        strategy="pb1_pullback_close",
        code="123456",
        sid=1,
        mode=1,
        confirmed_qty=4,
        request_json={"pending_position_meta_update": {"tp1_done": True}},
        positions_repo=repo,
        source="test",
    )
    assert committed is True
    fields = repo.update_position_fields.call_args.kwargs["fields"]
    assert fields["position_meta"]["tp1_done"] is True
    assert fields["position_meta"]["last_stage_confirmed_qty"] == 4

    repo.reset_mock()
    committed = _commit_confirmed_sell_stage(
        env="practice",
        strategy="pb1_pullback_close",
        code="123456",
        sid=1,
        mode=1,
        confirmed_qty=4,
        request_json={},
        positions_repo=repo,
        source="test",
    )
    assert committed is False
    repo.update_position_fields.assert_not_called()


def _make_reconcile_engine() -> sa.Engine:
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                CREATE TABLE positions (
                    position_id TEXT PRIMARY KEY,
                    env TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    code TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    avg_buy_price REAL,
                    total_cost REAL NOT NULL DEFAULT 0,
                    status TEXT,
                    closed_reason TEXT,
                    closed_ts TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                INSERT INTO positions (
                    position_id, env, strategy, code, qty, avg_buy_price,
                    total_cost, status, updated_at
                ) VALUES (
                    'pos-1', 'practice', 'pb1_pullback_close', '123456',
                    15, 100.0, 1500.0, 'OPEN', CURRENT_TIMESTAMP
                )
                """
            )
        )
    return engine


def test_kis_present_holding_row_reconciles_live_qty_without_stage_inference(tmp_path) -> None:
    engine = _make_reconcile_engine()
    closed = close_stale_positions(
        engine=engine,
        env="practice",
        strategy="pb1_pullback_close",
        reason="test",
        ts=datetime.utcnow(),
        kis_balance={
            "output1": [
                {
                    "pdno": "123456",
                    "hldg_qty": "11",
                    "ord_psbl_qty": "7",
                    "pchs_avg_pric": "101.5",
                }
            ]
        },
        sell_fill_codes=set(),
        runtime_dir=tmp_path,
    )
    assert closed == 0
    with engine.connect() as conn:
        row = conn.execute(
            sa.text(
                "SELECT qty, avg_buy_price, total_cost, status FROM positions WHERE code='123456'"
            )
        ).first()
    assert int(row[0]) == 11
    assert float(row[1]) == 101.5
    assert float(row[2]) == 1116.5
    assert row[3] == "OPEN"


def test_close_wrapper_forces_exit_only_safety_engine() -> None:
    source = Path("scripts/wsl/run-kr-close.sh").read_text(encoding="utf-8")
    assert 'PB1_CLOSE_EXIT_SAFETY_ENGINE="${PB1_CLOSE_EXIT_SAFETY_ENGINE:-1}"' in source
    assert "export PB1_EXIT_ONLY_MODE=1" in source
