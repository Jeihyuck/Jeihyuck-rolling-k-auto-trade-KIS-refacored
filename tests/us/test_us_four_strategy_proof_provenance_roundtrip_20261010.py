"""Evidence provenance survives completed PREP → watchlist DB → BUY contract.

Keep old US_STANDARD contracts intact; do not use missing new proof fields as
license to change established Pullback orders or risk/exit policy.
"""
import copy
import pytest

from trader.us.db.repos import _merge_us_daily_metrics_meta
from trader.us.score_columns import canonicalize_us_watchlist_row
from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract, verify_us_entry_exit_contract,
)


@pytest.mark.parametrize(
    ("style", "evidence"),
    [
        ("pb1_pullback", {}),
        ("momentum", {
            "independent_entry_contract_v1": True,
            "momentum_pass": True,
            "standalone_momentum_score": 0.89,
            "entry_signal_proof_source": "completed_daily_ohlcv",
        }),
        ("breakout", {
            "independent_entry_contract_v1": True,
            "breakout_pass": True,
            "breakout_pivot_price": 100.0,
            "entry_signal_proof_source": "completed_daily_ohlcv",
        }),
        ("vcp", {
            "vcp_pass": True, "trend_template_pass": True,
            "pivot_price": 99.5, "vcp_evidence_source": "completed_daily_ohlcv",
            "vcp_evidence_status": "ok",
            "vcp_evidence": {"contractions": [0.08, 0.04, 0.02], "volume_dryup": True},
        }),
    ],
)
def test_prep_db_canonical_frozen_contract_keeps_four_style_proofs(style, evidence):
    row = {
        "symbol": "AAPL", "exchange": "NASDAQ",
        "entry_style_selected": style, "entry_style_raw": style,
        "entry_reason": "ENTRY_" + ("PULLBACK" if style=="pb1_pullback" else style.upper()),
        "reasons": [], "score_final": 0.87, "score": 0.87,
        "pullback_score": 0.7, "momentum_score": 0.8,
        "breakout_score": 0.8, "vcp_score": 0.9,
        **evidence,
    }
    persisted = {
        "symbol": "AAPL", "exchange": "NASDAQ", "score": .87,
        "meta": _merge_us_daily_metrics_meta(row),
    }
    # Force an actual serialization boundary resembling JSONB from the DB.
    import json
    persisted = json.loads(json.dumps(persisted))
    canonical = canonicalize_us_watchlist_row(persisted)
    for field, expected in evidence.items():
        assert canonical[field] == expected, field
    assert canonical["entry_style_selected"] == style

    frozen = build_us_entry_exit_contract(canonical)
    assert verify_us_entry_exit_contract(frozen)
    assert frozen["entry_provenance"]["entry_style_selected"] == style
    assert frozen["strategy_owner"] == "US_STANDARD"
    for field, expected in evidence.items():
        assert frozen["entry_provenance"][field] == expected, field

    # Changing proof after BUY must invalidate the frozen digest.
    if evidence:
        corrupted = copy.deepcopy(frozen)
        key = next(iter(evidence))
        corrupted["entry_provenance"][key] = "CORRUPT"
        assert verify_us_entry_exit_contract(corrupted) is False
    else:
        assert "vcp_pass" not in frozen["entry_provenance"]


def test_false_proofs_remain_false_through_frozen_contract():
    row = {"symbol": "AAPL", "entry_style_selected": "momentum",
           "momentum_pass": False, "breakout_pass": False, "vcp_pass": False}
    db_meta = _merge_us_daily_metrics_meta(row)
    live = canonicalize_us_watchlist_row({"symbol": "AAPL", "score": .75, "meta": db_meta})
    assert live["momentum_pass"] is False
    assert live["breakout_pass"] is False
    assert live["vcp_pass"] is False
    frozen = build_us_entry_exit_contract(live)
    assert frozen["entry_provenance"]["momentum_pass"] is False
    assert frozen["entry_provenance"]["breakout_pass"] is False
    assert frozen["entry_provenance"]["vcp_pass"] is False


def test_tqqq_owner_remains_disjoint_from_four_standard_strategy_contracts():
    infinite = {
        "symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE",
        "entry_style_selected": "momentum", "momentum_pass": True,
    }
    assert build_us_entry_exit_contract(infinite) == {}


@pytest.mark.parametrize(
    ("raw_style", "evidence"),
    [
        ("pb1_pullback", {}),
        ("momentum", {
            "independent_entry_contract_v1": True,
            "momentum_pass": True,
            "standalone_momentum_score": .89,
            "entry_signal_proof_source": "completed_daily_ohlcv",
        }),
        ("breakout", {
            "independent_entry_contract_v1": True,
            "breakout_pass": True,
            "breakout_pivot_price": 95.0,
            "entry_signal_proof_source": "completed_daily_ohlcv",
        }),
        ("vcp", {
            "vcp_pass": True, "trend_template_pass": True,
            "pivot_price": 96.0, "vcp_evidence_status": "ok",
            "vcp_evidence_source": "completed_daily_ohlcv",
        }),
    ],
)
def test_route_order_receives_actual_prep_evidence(monkeypatch, raw_style, evidence):
    """Regression for losing proof between DB canonical row and live BUY intent."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from trader.us.db import repos
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    from trader.us.execution.order_router import route_order

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    for k, v in {
        "US_MIN_ENTRY_SCORE": ".01",
        "US_MAX_NEW_ENTRIES_PER_TICK": "1",
        "US_MAX_ORDER_USD": "5000",
        "US_MAX_POSITION_WEIGHT": "1",
        "US_MIN_CASH_BUFFER_USD": "0",
        "US_MAX_DAILY_NOTIONAL_USD": "50000",
        "KIS_ENV": "practice",
    }.items():
        monkeypatch.setenv(k, v)

    prep = {
        "symbol": "AAPL", "exchange": "NASDAQ",
        "score": .90, "score_final": .90,
        "entry_style_selected": raw_style,
        "entry_style_raw": raw_style,
        "momentum_score": .8, "breakout_score": .7,
        "pullback_score": .6, "vcp_score": .9,
        "reasons": ["ENTRY_" + ("PULLBACK" if raw_style == "pb1_pullback" else raw_style.upper())],
        "filters_passed": ["score", "liquidity"],
        "score_breakdown": {"momentum_score": .8},
        "rank_final30": 1,
        **evidence,
    }
    db_row = {
        "symbol": "AAPL", "exchange": "NASDAQ",
        "strategy": raw_style, "score": .90,
        "meta": _merge_us_daily_metrics_meta(prep),
        "prep_status": "OK", "run_id": "proof-chain-regression",
    }
    canonical = canonicalize_us_watchlist_row(db_row)

    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": 100.0}

    diagnostics = {}
    intents = generate_entry_intents(
        tickers=None,
        provider=Provider(),
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=10000.0,
        now=datetime(2026, 10, 9, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1,
        watchlist_entries=[canonical],
        current_position_symbols=set(),
        diagnostics=diagnostics,
    )
    assert len(intents) == 1, diagnostics
    for field, value in evidence.items():
        assert intents[0]["meta"][field] == value, field

    routed = route_order(
        intents[0],
        signal_only=True,
        current_position_symbols=set(),
        allowed_symbols={"AAPL"},
    )
    assert routed["status"] == "SIGNAL_ONLY"
    frozen = routed["intent"]["meta"]["entry_exit_contract"]
    assert verify_us_entry_exit_contract(frozen)
    for field, value in evidence.items():
        assert frozen["entry_provenance"][field] == value, field
    assert routed["intent"]["meta"]["entry_exit_contract_sha256"] == frozen["sha256"]
