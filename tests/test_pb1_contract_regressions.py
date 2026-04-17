from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy.dialects import postgresql

import trader.db.repos as repos_module
from trader.db.repos import RunsRepo
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
from trader.pb1_runner import _acquire_session_guard_or_takeover, _fail_open_on_runs_ledger_error
from trader.window_router import WindowDecision


def _load_script_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class DummyOrdersRepo:
    def __init__(self):
        self.engine = object()


class _FakeRepoEngine:
    url = "postgresql+psycopg://"
    dialect = postgresql.dialect()

    def begin(self):
        raise RuntimeError("not used")


class _GuardFakeRunsRepo:
    def __init__(self, existing_session=None, *, stale=False):
        self.existing_session = existing_session
        self.stale = stale
        self.finish_calls = []
        self.create_calls = []

    def find_active_session_today(self, **_kwargs):
        return self.existing_session

    def is_session_stale(self, _row, *, stale_sec):
        self.last_stale_sec = stale_sec
        return self.stale

    def finish_stale_session_if_needed(self, run_row, *, stale_sec, reason, takeover_from_run_id=None, status="SESSION_STALE_TAKEOVER"):
        self.finish_calls.append(
            {
                "run_id": run_row.get("run_id"),
                "stale_sec": stale_sec,
                "reason": reason,
                "takeover_from_run_id": takeover_from_run_id,
                "status": status,
            }
        )
        return True

    def create_session_guard(self, **kwargs):
        self.create_calls.append(kwargs)
        return "new-guard-run"


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


def test_runs_repo_started_at_expr_casts_reflected_text_column():
    repo = RunsRepo(_FakeRepoEngine())
    repo._runs_column_type_cache["started_at"] = ("text", True)

    expr, detected, cast_mode = repo._safe_runs_timestamp_expr("started_at")
    compiled = str(expr.compile(dialect=postgresql.dialect()))

    assert detected == "text"
    assert cast_mode == 1
    assert "CASE" in compiled
    assert "CAST" in compiled


def test_runs_repo_is_session_stale_uses_recent_heartbeat(monkeypatch):
    repo = RunsRepo(_FakeRepoEngine())
    now = pd.Timestamp("2026-04-17T09:10:00+09:00").to_pydatetime()
    monkeypatch.setattr(repos_module, "now_kst", lambda: now)

    recent_row = {
        "status": "SESSION_GUARD_STARTED",
        "started_at": "2026-04-17T09:00:00+09:00",
        "heartbeat_at": "2026-04-17T09:09:40+09:00",
        "updated_at": "2026-04-17T09:09:40+09:00",
        "finished_at": None,
    }
    stale_row = {
        "status": "SESSION_GUARD_STARTED",
        "started_at": "2026-04-17T08:30:00+09:00",
        "heartbeat_at": "2026-04-17T08:55:00+09:00",
        "updated_at": "2026-04-17T08:55:00+09:00",
        "finished_at": None,
    }

    assert repo.is_session_stale(recent_row, stale_sec=60) is False
    assert repo.is_session_stale(stale_row, stale_sec=600) is True


def test_acquire_session_guard_blocks_only_active_session(monkeypatch):
    repo = _GuardFakeRunsRepo(
        existing_session={
            "run_id": "old-run",
            "status": "SESSION_GUARD_STARTED",
            "started_at": "2026-04-17T09:07:00+09:00",
            "heartbeat_at": "2026-04-17T09:08:30+09:00",
            "workflow_run_id": "wf-old",
        },
        stale=False,
    )
    monkeypatch.setenv("PB1_ALLOW_MANUAL_SESSION_TAKEOVER", "1")

    result = _acquire_session_guard_or_takeover(
        runs_repo=repo,
        env="practice",
        session_kind="am",
        run_id="new-run",
        workflow_run_id="wf-new",
        workflow_attempt=1,
        event_name="schedule",
        workflow="Trade AM",
        git_sha="deadbeef",
        strategy_env="practice",
        kis_env="practice",
        ctx_env="practice",
    )

    assert result["blocked"] is True
    assert repo.finish_calls == []
    assert repo.create_calls == []


def test_acquire_session_guard_allows_manual_takeover_for_stale_session(monkeypatch):
    repo = _GuardFakeRunsRepo(
        existing_session={
            "run_id": "old-run",
            "status": "SESSION_GUARD_STARTED",
            "started_at": "2026-04-17T09:07:00+09:00",
            "heartbeat_at": "2026-04-17T09:07:30+09:00",
            "workflow_run_id": "wf-old",
        },
        stale=True,
    )
    monkeypatch.setenv("PB1_ALLOW_MANUAL_SESSION_TAKEOVER", "1")
    monkeypatch.setenv("PB1_MANUAL_SESSION_GUARD_STALE_SEC", "60")

    result = _acquire_session_guard_or_takeover(
        runs_repo=repo,
        env="practice",
        session_kind="am",
        run_id="new-run",
        workflow_run_id="wf-new",
        workflow_attempt=2,
        event_name="workflow_dispatch",
        workflow="Trade AM",
        git_sha="deadbeef",
        strategy_env="practice",
        kis_env="practice",
        ctx_env="practice",
    )

    assert result["blocked"] is False
    assert result["session_guard_run_id"] == "new-guard-run"
    assert repo.finish_calls[0]["reason"] == "manual_session_takeover"
    assert repo.finish_calls[0]["status"] == "SESSION_MANUAL_TAKEOVER"
    assert repo.finish_calls[0]["takeover_from_run_id"] is None
    assert repo.create_calls[0]["takeover_from_run_id"] == "old-run"


def test_migration_0036_keeps_takeover_fk_text_compatible():
    sql = (Path(__file__).resolve().parents[1] / "migrations" / "0036_runs_session_guard_metadata.sql").read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS takeover_from_run_id TEXT" in sql
    assert "ADD COLUMN IF NOT EXISTS takeover_from_run_id UUID" not in sql
    assert "ALTER COLUMN takeover_from_run_id TYPE TEXT" in sql
    assert "skip FK because type mismatch remains" in sql


def test_fail_open_on_runs_ledger_error_defaults_to_practice(monkeypatch):
    monkeypatch.delenv("PB1_FAIL_OPEN_ON_RUNS_LEDGER_ERROR", raising=False)

    assert _fail_open_on_runs_ledger_error("practice") is True
    assert _fail_open_on_runs_ledger_error("live") is False


def test_fail_open_on_runs_ledger_error_can_be_disabled(monkeypatch):
    monkeypatch.setenv("PB1_FAIL_OPEN_ON_RUNS_LEDGER_ERROR", "0")

    assert _fail_open_on_runs_ledger_error("practice") is False


def test_fail_open_on_runs_ledger_error_can_be_enabled_for_live(monkeypatch):
    monkeypatch.setenv("PB1_FAIL_OPEN_ON_RUNS_LEDGER_ERROR", "1")

    assert _fail_open_on_runs_ledger_error("live") is True


def test_prep_log_verifier_allows_duplicate_skip(tmp_path):
    verifier = _load_script_module(
        "verify_prep_log",
        Path(__file__).resolve().parents[1] / "scripts" / "verify_prep_log.py",
    )
    log_path = tmp_path / "prep.log"
    log_path.write_text(
        "\n".join(
            [
                "[PREP][TRIGGER] event=schedule actor=tester run_id=123 attempt=1",
                "[PREP][START_META] now_kst=2026-04-17 08:00:00 KST ref=nullim sha=deadbeef",
                "[PREP][DUPLICATE_GUARD][SKIP] as_of=2026-04-17 reason=canonical_prep_already_ready",
                "[RUN_SUMMARY][RESULT] status=SKIP_DUPLICATE_PREP reason=canonical_prep_already_ready event=schedule",
            ]
        ),
        encoding="utf-8",
    )

    results = verifier.parse_log_file(log_path)

    assert results.run_summary_status == "SKIP_DUPLICATE_PREP"
    assert results.run_summary_reason == "canonical_prep_already_ready"
    assert results.has_critical_failure() is False


def test_config_defaults_include_target_new_positions():
    from trader.config import PB1_TARGET_NEW_POSITIONS

    assert PB1_TARGET_NEW_POSITIONS == 4


def test_base_migration_uses_timestamptz_for_runs_columns():
    migration = Path(__file__).resolve().parents[1] / "migrations" / "0001_pbcore.sql"
    sql = migration.read_text(encoding="utf-8")

    assert "started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP" in sql
    assert "finished_at TIMESTAMPTZ" in sql


def test_session_guard_schema_uses_text_takeover_column():
    from trader.db.schema import RUNS

    assert isinstance(RUNS.c.run_id.type, type(RUNS.c.takeover_from_run_id.type))
    assert str(RUNS.c.takeover_from_run_id.type).lower() == "text"


def test_0036_migration_converts_takeover_column_to_text_and_skips_non_text_fk():
    migration = Path(__file__).resolve().parents[1] / "migrations" / "0036_runs_session_guard_metadata.sql"
    sql = migration.read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS takeover_from_run_id TEXT" in sql
    assert "ALTER COLUMN takeover_from_run_id TYPE TEXT" in sql
    assert "USING takeover_from_run_id::text" in sql
    assert "skip FK because run_id is not text" in sql
    assert "[DB][MIGRATE][TYPE_MISMATCH] version=0036" in sql


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
