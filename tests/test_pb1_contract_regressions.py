from __future__ import annotations

from datetime import date

import pandas as pd

from trader.entry_engine.scanner import scan_entry_candidates
from trader.minervini_filter import (
    normalize_rs_percentile,
    normalize_trend_score,
    normalize_vcp_score,
    select_buyable_with_relax,
)
from trader.path_contract import build_final30_paths, build_watchlist_paths
from trader.pb1_engine import (
    PB1Engine,
    _compute_affordable_buy_qty,
    _compute_highest_since_entry,
    _normalize_sizing_failure_reason,
    _resolve_exit_policy,
    _should_allow_single_share_position_cap_override,
)
from trader.window_router import WindowDecision


class DummyOrdersRepo:
    def __init__(self):
        self.engine = object()


def _make_engine(**kwargs):
    return PB1Engine(
        universe_repo=object(),
        orders_repo=DummyOrdersRepo(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=object(),
        kis=None,
        window=WindowDecision(name="morning", phase="entry"),
        window_label="morning",
        phase="entry",
        dry_run=True,
        env="practice",
        run_id="run-1",
        intended_live=False,
        **kwargs,
    )


def test_pb1_engine_initializes_as_of_state():
    engine = _make_engine(as_of="2026-03-17", trade_date="2026-03-18", run_ctx={"derived_as_of": "2026-03-17"})

    assert engine._as_of == "2026-03-17"
    assert engine._trade_date == "2026-03-18"
    assert engine.get_as_of() == "2026-03-17"


def test_pb1_engine_get_as_of_backfills_from_run_ctx():
    engine = _make_engine(run_ctx={"derived_as_of": "2026-03-17"}, derived_as_of=None)
    engine._as_of = None

    assert engine.get_as_of() == "2026-03-17"
    assert engine._as_of == "2026-03-17"


def test_final30_path_contract_is_repo_root_anchored(tmp_path):
    final30_paths = build_final30_paths(tmp_path, "practice", "2026-03-17")
    watchlist_paths = build_watchlist_paths(tmp_path, "2026-03-17")

    assert final30_paths["runtime"] == tmp_path / "runtime" / "watchlist" / "2026-03-17" / "final30_scored.json"
    assert final30_paths["ledger"] == tmp_path / "bot_state" / "trader_ledger" / "final30" / "practice" / "2026-03-17" / "final30_scored.json"
    assert final30_paths["signals"] == tmp_path / "signals" / "final30.json"
    assert watchlist_paths["snapshot"] == tmp_path / "runtime" / "snapshots" / "final30.json"


def test_minervini_normalize_helpers_scale_fraction_values():
    assert normalize_rs_percentile(0.9746) == 97.46
    assert normalize_vcp_score(0.61) == 61.0
    assert normalize_trend_score(0.82) == 82.0


def test_minervini_relax_uses_normalized_scores(monkeypatch):
    monkeypatch.setenv("MINERVINI_RS_MIN_PCTILE", "70")
    monkeypatch.setenv("MINERVINI_VCP_MIN_SCORE", "60")

    buyable, report = select_buyable_with_relax(
        signals={
            "regime_pass": True,
            "items": [
                {"code": "000001", "data_ok": True, "trend_pass": True, "atr_pass": True, "rs_pctile": 0.9746, "vcp_score": 0.71, "trend_score": 0.8},
                {"code": "000002", "data_ok": True, "trend_pass": True, "atr_pass": True, "rs_pctile": 0.65, "vcp_score": 0.55, "trend_score": 0.7},
            ],
        },
        min_buyable=1,
        relax_passes=2,
        rs_step=5,
        vcp_step=5,
        keep_trend=True,
    )

    assert buyable == ["000001"]
    assert report["final_gating_mode"] == "hard_pass"
    assert report["normalized_items"][0]["normalized_rs"] == 97.46


def test_scanner_precomputed_hit_is_not_auto_breakout_pass():
    watchlist = [
        {
            "code": "005930",
            "name": "Samsung",
            "score_final": 90.0,
            "tech_score": 80.0,
            "breakout_score": 10.0,
            "pullback_score": 72.0,
            "momentum_score": 15.0,
            "rs_percentile": 85.0,
            "vcp_score": 75.0,
            "entry_style_selected": "PULLBACK",
        }
    ]
    precomputed_df = pd.DataFrame(
        [
            {
                "code": "005930",
                "close": 100.0,
                "ma20": 98.0,
                "ma50": 96.0,
                "pullback_pct": 0.08,
                "high_52w": 108.0,
                "high_50": 110.0,
                "volume_avg20": 1000.0,
                "volume": 700.0,
                "rs_percentile": 85.0,
                "vcp_score": 75.0,
                "breakout_score": 10.0,
                "pullback_score": 72.0,
                "momentum_score": 15.0,
                "breakout_pass": False,
                "pullback_pass": True,
                "momentum_pass": False,
            }
        ]
    )

    result = scan_entry_candidates(
        watchlist=watchlist,
        ohlcv_provider=lambda *_args, **_kwargs: pd.DataFrame(),
        precomputed_final30_df=precomputed_df,
        trade_precomputed_only=True,
    )

    assert len(result["breakout"]) == 0
    assert len(result["pullback"]) == 1
    assert result["all"][0].strategy == "pullback"


def test_slippage_normalization_matches_fraction_and_percent():
    fraction, percent = PB1Engine._normalize_slippage("10")
    assert round(fraction, 6) == 0.001
    assert round(percent, 2) == 0.10


def test_sizing_reason_normalization_matches_binding_constraint():
    assert _normalize_sizing_failure_reason("ORDER_PX_ABOVE_POSITION_CAP") == "ORDER_PX_ABOVE_POSITION_CAP"
    assert _normalize_sizing_failure_reason("ORDER_PX_ABOVE_USABLE_CASH") == "ORDER_PX_ABOVE_USABLE_CASH"
    assert _normalize_sizing_failure_reason("MIN_ORDER_KRW_NOT_MET") == "MIN_ORDER_KRW_NOT_MET"


def test_highest_since_entry_uses_only_post_entry_bars():
    df = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-04-14 09:01:00+09:00",
                    "2026-04-14 09:05:00+09:00",
                    "2026-04-14 09:07:00+09:00",
                ]
            ),
            "high": [260000.0, 214800.0, 214700.0],
        }
    )

    highest, rows = _compute_highest_since_entry(df, "2026-04-14 09:05:00+09:00", 214500.0)

    assert rows == 2
    assert highest == 214800.0


def test_same_day_soft_exit_is_blocked_even_when_raw_signals_hit():
    decision = _resolve_exit_policy(
        days_held=0,
        holding_bars=1,
        stop_hit=False,
        trail_stop_price=229580.35,
        mark=213500.0,
        ma20=214000.0,
        ma50=214000.0,
        time_stop_hit=False,
        risk_off_signal=True,
    )

    assert decision["same_day_entry"] is True
    assert decision["trail_eligible"] is False
    assert decision["soft_exit_eligible"] is False
    assert decision["trail_hit"] is False
    assert decision["ma50_break"] is False
    assert decision["risk_off_hit"] is False
    assert decision["exit_ok"] is False
    assert decision["final_reason"] == "NO_EXIT_SIGNAL"


def test_same_day_hard_stop_remains_sellable():
    decision = _resolve_exit_policy(
        days_held=0,
        holding_bars=1,
        stop_hit=True,
        trail_stop_price=229580.35,
        mark=213500.0,
        ma20=214000.0,
        ma50=214000.0,
        time_stop_hit=False,
        risk_off_signal=True,
    )

    assert decision["exit_ok"] is True
    assert decision["final_reason"] == "EXIT_HARD_STOP"


def test_single_share_override_allows_buy_with_enough_cash():
    qty, details = _compute_affordable_buy_qty(
        target_budget=200000.0,
        buy_ref_price=214500.0,
        cash_available=500000.0,
        min_remaining_cash_krw=10000.0,
        allow_single_share_override=True,
        budget_flex_pct=1.0,
    )

    assert details["qty_by_budget"] == 0
    assert qty == 1
    assert details["buy_mode"] == "single_share_override"


def test_single_share_override_rejects_when_cash_is_insufficient():
    qty, details = _compute_affordable_buy_qty(
        target_budget=200000.0,
        buy_ref_price=214500.0,
        cash_available=180000.0,
        min_remaining_cash_krw=10000.0,
        allow_single_share_override=True,
        budget_flex_pct=1.0,
    )

    assert qty == 0
    assert details["skip_reason"] == "insufficient_cash_for_one_share"


def test_budget_flex_can_enable_one_share_without_override():
    qty, details = _compute_affordable_buy_qty(
        target_budget=200000.0,
        buy_ref_price=214500.0,
        cash_available=500000.0,
        min_remaining_cash_krw=10000.0,
        allow_single_share_override=False,
        budget_flex_pct=1.10,
    )

    assert details["effective_budget"] == 220000.0
    assert qty == 1
    assert details["buy_mode"] == "budget"


def test_single_share_position_cap_override_allowed_for_top_rank() -> None:
    allowed = _should_allow_single_share_position_cap_override(
        rank=1,
        final_qty=1,
        afford_details={
            "buy_mode": "single_share_override",
            "one_share_cost": 1117560.0,
        },
        force_min1_topn=3,
        force_min1_override_position_cap=True,
        cash_available=1500000.0,
        min_remaining_cash_krw=10000.0,
        order_possible_cash=1500000.0,
    )

    assert allowed is True


def test_single_share_position_cap_override_blocked_for_low_rank() -> None:
    allowed = _should_allow_single_share_position_cap_override(
        rank=4,
        final_qty=1,
        afford_details={
            "buy_mode": "single_share_override",
            "one_share_cost": 1117560.0,
        },
        force_min1_topn=3,
        force_min1_override_position_cap=True,
        cash_available=1500000.0,
        min_remaining_cash_krw=10000.0,
        order_possible_cash=1500000.0,
    )

    assert allowed is False
