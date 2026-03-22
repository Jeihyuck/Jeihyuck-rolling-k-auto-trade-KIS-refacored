from __future__ import annotations

import sqlalchemy as sa

from trader.db.repos import summarize_final30_scored_contract
from trader.pb1_engine import CandidateFeature, PB1Engine
from trader.watchlist_builder import _sanitize_scored_item
from trader.window_router import WindowDecision


class DummyOrdersRepo:
    def __init__(self) -> None:
        self.engine = sa.create_engine("sqlite:///:memory:")


class DummyFillsRepo:
    def list_latest_buy_fills_by_codes(self, _env, _codes):
        return {}


class DummyPositionsRepo:
    pass


class DummyLedgerRepo:
    pass


def _make_engine(**kwargs):
    return PB1Engine(
        universe_repo=object(),
        orders_repo=DummyOrdersRepo(),
        fills_repo=DummyFillsRepo(),
        positions_repo=DummyPositionsRepo(),
        ledger_repo=DummyLedgerRepo(),
        kis=None,
        window=WindowDecision(name="day", phase="trade"),
        window_label="day",
        phase="trade",
        dry_run=True,
        env="practice",
        run_id="run-1",
        intended_live=False,
        **kwargs,
    )


def test_order_precheck_gate_skips_on_nontrading_sell():
    engine = _make_engine(order_allowed=False, trading_day=False, force_block_live=True)

    reasons = engine._order_precheck_gate_reasons(side="SELL", stage="PB1-EXIT")

    assert "nontrading_day" in reasons
    assert "order_blocked" in reasons
    assert "live_gate_blocked" in reasons
    assert engine._pretrade_check(
        code="058470",
        market="KOSPI",
        mode=1,
        side="SELL",
        qty=1,
        price=10000.0,
        client_order_key="exit-key",
        stage="PB1-EXIT",
    ) is False


def test_stop_builder_uses_atr_fallback_without_stop_calc_fail():
    engine = _make_engine()
    candidate = CandidateFeature(
        code="032830",
        market="KOSPI",
        features={
            "close": 231500.0,
            "atr14": 15617.9,
            "ma20": None,
            "ma50": None,
            "entry_style_selected": "BREAKOUT",
        },
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
    )

    stop_price, source, degraded, reason = engine._build_stop_price(candidate, 231500.0)

    assert source == "atr_fallback"
    assert degraded == 0
    assert reason is None
    assert stop_price is not None
    assert stop_price < 231500.0


def test_watchlist_sanitize_replaces_invalid_zero_with_none_or_repair():
    row = {
        "code": "005930",
        "close": 70000.0,
        "ma20": 0.0,
        "ma50": 65000.0,
        "ma150": 60000.0,
        "atr_pct": 0.0,
        "breakout_score": 0.0,
        "pullback_score": 25.0,
        "momentum_score": 12.0,
        "rs_percentile": None,
        "meta": {},
    }
    ref = {
        "code": "005930",
        "close": 70000.0,
        "ma20": 68000.0,
        "atr_pct": 0.034,
    }

    sanitized = _sanitize_scored_item(row, ref, source="pre_save_normalize")

    assert sanitized["ma20"] == 68000.0
    assert sanitized["atr_pct"] == 0.034
    assert sanitized["rs_percentile"] is None


def test_final30_contract_allows_nullable_fields_but_rejects_zero_invalid():
    rows = [
        {
            "code": f"{idx:06d}",
            "as_of": "2026-03-22",
            "rank_final30": idx,
            "score_final": 90.0,
            "tech_score": 80.0,
            "breakout_score": 10.0,
            "pullback_score": 20.0,
            "momentum_score": 30.0,
            "entry_style_selected": "BREAKOUT",
            "ma20": 0.0 if idx == 1 else None,
            "ma50": None,
            "ma150": None,
            "rs_percentile": None,
            "vcp_score": 70.0,
            "atr_pct": 0.0 if idx == 1 else None,
            "close": 70000.0,
            "reasons": [],
            "filters_passed": [],
            "filters_failed": [],
        }
        for idx in range(1, 31)
    ]

    summary = summarize_final30_scored_contract(rows, env="practice", as_of="2026-03-22")

    assert summary["null_critical"] == 0
    assert summary["null_warnings"] > 0
    assert summary["critical_numeric_zero_invalid_count"] == 1
    assert summary["atr_pct_zero_invalid_when_close_positive"] == 1
    assert summary["ok"] is False
