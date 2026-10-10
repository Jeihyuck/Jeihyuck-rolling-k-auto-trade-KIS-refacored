"""Each entry strategy is independently qualified; US_STANDARD orders once per symbol."""
import pytest
from trader.us.watchlist_builder import _select_entry_style
from trader.us.db.repos import _merge_us_daily_metrics_meta
from trader.us.score_columns import canonicalize_us_watchlist_row
from trader.us.entry_exit_contract import build_us_entry_exit_contract


def _proof(**overrides):
    return {
        "independent_entry_contract_v1": True,
        "entry_signal_proof_source": "completed_daily_ohlcv",
        "momentum_pass": True,
        "breakout_pass": True,
        "vcp_pass": True,
        "trend_template_pass": True,
        "pivot_price": 100.0,
        "close": 101.0,
        **overrides,
    }


def _choose(row, **scores):
    return _select_entry_style(
        row,
        pb1_score=scores.get("pb1_score", 0.60),
        momentum_score=scores.get("momentum_score", 0.82),
        pullback_score=scores.get("pullback_score", 0.50),
        breakout_score=scores.get("breakout_score", 0.85),
        vcp_score=scores.get("vcp_score", 0.90),
    )


def test_all_four_proven_families_can_be_independently_eligible(monkeypatch):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    row = _proof()
    assert _choose(row) == "vcp"
    assert row["independent_eligible_entry_styles"] == [
        "breakout", "momentum", "momentum_pullback", "pb1_pullback", "vcp",
    ]
    assert row["independent_proof_status"] == {
        "momentum": True, "breakout": True, "vcp": True,
    }
    # Eligibility of another family does not force a duplicate BUY.
    assert row["independent_arbitration_mode"] == "proof_first_one_order_per_symbol"


@pytest.mark.parametrize(
    ("style", "s", "expected"),
    [
        ("momentum", {"momentum_score": .98, "breakout_score": .40, "vcp_score": .40}, "momentum"),
        ("breakout", {"momentum_score": .40, "breakout_score": .98, "vcp_score": .40}, "breakout"),
        ("vcp", {"momentum_score": .40, "breakout_score": .40, "vcp_score": .98}, "vcp"),
        ("pb1", {"pb1_score": .99, "momentum_score": .4, "breakout_score": .4, "vcp_score": .4}, "pb1_pullback"),
    ],
)
def test_each_independently_proven_family_can_win_one_symbol(monkeypatch, style, s, expected):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    row = _proof()
    assert _choose(row, **s) == expected


@pytest.mark.parametrize(
    ("override", "eligible"),
    [
        ({"independent_entry_contract_v1": None}, False),
        ({"entry_signal_proof_source": None}, False),
        ({"entry_signal_proof_source": "stale_or_unknown"}, False),
        ({"momentum_pass": False, "breakout_pass": False}, False),
    ],
)
def test_independent_proofs_do_not_pass_on_missing_source(monkeypatch, override, eligible):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    row = _proof(**override)
    _choose(row, momentum_score=.99, breakout_score=.98, vcp_score=.30)
    has_standalone = any(
        v in row["independent_eligible_entry_styles"] for v in ("momentum", "breakout")
    )
    assert has_standalone is eligible


def test_four_family_eligibility_roundtrip_to_immutable_exit_contract(monkeypatch):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    row = {**_proof(), "symbol": "AAPL", "exchange": "NASDAQ",
           "entry_style_selected": "vcp", "score": .9, "score_final": .9}
    assert _choose(row) == "vcp"
    meta = _merge_us_daily_metrics_meta(row)
    canonical = canonicalize_us_watchlist_row({
        "symbol": "AAPL", "score": .9, "meta": meta
    })
    assert canonical["independent_eligible_entry_styles"] == row["independent_eligible_entry_styles"]
    assert canonical["independent_proof_status"] == row["independent_proof_status"]
    frozen = build_us_entry_exit_contract(canonical)
    assert frozen["entry_provenance"]["independent_eligible_entry_styles"] == row["independent_eligible_entry_styles"]
    assert frozen["strategy_owner"] == "US_STANDARD"


def test_legacy_selection_unchanged_when_opt_in_off(monkeypatch):
    monkeypatch.delenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", raising=False)
    row = _proof(independent_entry_contract_v1=None)
    assert _choose(row, momentum_score=.98, breakout_score=.50, vcp_score=.1) == "momentum"
    assert "independent_eligible_entry_styles" not in row


def test_duplicate_locked_symbol_with_conflicting_styles_never_reaches_broker(monkeypatch):
    """Last-row-wins would silently replace chosen Momentum with Breakout."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    class Provider:
        def get_current_price(self, symbol, exchange):
            raise AssertionError("Ambiguous lock must block before price lookup")

    rows = [
        {"symbol": "AAPL", "exchange": "NASDAQ",
         "entry_style_selected": "momentum", "score": .93,
         "entry_style_raw": "momentum"},
        {"symbol": "AAPL", "exchange": "NASDAQ",
         "entry_style_selected": "breakout", "score": .90,
         "entry_style_raw": "breakout"},
    ]
    diag = {}
    intents = generate_entry_intents(
        tickers=None, watchlist_entries=rows, provider=Provider(),
        sold_today=set(), available_cash_usd=10000,
        position_count=0, capital_usd_cap=10000,
        current_position_symbols=set(),
        now=datetime(2026, 10, 9, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        diagnostics=diag,
    )
    assert intents == []
    assert [x["reason"] for x in diag.get("blocked", [])] == [
        "duplicate_locked_watchlist_symbol",
        "duplicate_locked_watchlist_symbol",
    ]


@pytest.mark.parametrize(
    ("style", "expected_signal", "proof", "expect_volume_probe"),
    [
        ("pb1_pullback", "pullback", {}, False),
        ("momentum", "momentum", {
            "independent_entry_contract_v1": True,
            "entry_signal_proof_source": "completed_daily_ohlcv",
            "momentum_pass": True, "standalone_momentum_score": .90,
        }, False),
        ("breakout", "breakout", {
            "independent_entry_contract_v1": True,
            "entry_signal_proof_source": "completed_daily_ohlcv",
            "breakout_pass": True, "breakout_pivot_price": 95,
        }, False),
        ("vcp", "vcp", {
            "vcp_pass": True, "trend_template_pass": True,
            "pivot_price": 100,
            "vcp_daily_avg_volume20": 100000,
            "vcp_evidence_source": "completed_daily_ohlcv",
        }, True),
    ],
)
def test_all_four_independent_families_create_one_owner_safe_buy_intent(
    monkeypatch, style, expected_signal, proof, expect_volume_probe
):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from trader.us.db import repos
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    from trader.us.execution.order_router import route_order
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    for key, val in {
        "KIS_ENV": "practice",
        "US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED": "1",
        "US_MINERVINI_VCP_PROOF_ENABLED": "1",
        "US_VCP_LIVE_REST_MAX_PER_TICK": "1",
        "US_MIN_ENTRY_SCORE": ".01",
        "US_MAX_NEW_ENTRIES_PER_TICK": "1",
        "US_MAX_ORDER_USD": "5000",
        "US_MAX_POSITION_WEIGHT": "1",
        "US_MIN_CASH_BUFFER_USD": "0",
        "US_MAX_DAILY_NOTIONAL_USD": "50000",
    }.items():
        monkeypatch.setenv(key, val)
    row = {
        "symbol": "AAPL", "exchange": "NASDAQ", "score": .90,
        "score_final": .90, "entry_style_selected": style,
        "entry_style_raw": style,
        "momentum_score": .80, "breakout_score": .80,
        "pullback_score": .65, "vcp_score": .9,
        "rank_final30": 1,
        "reasons": ["ENTRY_" + ("PULLBACK" if style == "pb1_pullback" else style.upper())],
        "filters_passed": ["score", "liquidity"],
        "score_breakdown": {"trend": .9},
        **proof,
    }
    db_row = {
        "symbol": "AAPL", "exchange": "NASDAQ", "score": .90,
        "strategy": style, "meta": _merge_us_daily_metrics_meta(row),
    }
    locked = canonicalize_us_watchlist_row(db_row)
    class Provider:
        calls = 0
        def get_current_price(self, symbol, exchange):
            return {"last": "101"}
        def get_current_price_with_volume(self, symbol, exchange):
            self.calls += 1
            return {
                "last": "101", "tvol": "180000",
                "quality": "fresh", "stale": False,
                "source": "KIS_LIVE", "age_sec": 0.0,
            }
    p = Provider()
    diag = {}
    intents = generate_entry_intents(
        tickers=None, watchlist_entries=[locked], provider=p,
        sold_today=set(), available_cash_usd=10000,
        position_count=0, capital_usd_cap=10000,
        current_position_symbols=set(),
        now=datetime(2026, 10, 9, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1, diagnostics=diag,
    )
    assert len(intents) == 1, diag
    assert p.calls == int(expect_volume_probe)
    intent = intents[0]
    assert intent["strategy_owner"] == "US_STANDARD"
    assert intent["entry_signal_type"] == expected_signal
    routed = route_order(
        intent, signal_only=True,
        current_position_symbols=set(), allowed_symbols={"AAPL"},
    )
    assert routed["status"] == "SIGNAL_ONLY"
    assert verify_us_entry_exit_contract(
        routed["intent"]["meta"]["entry_exit_contract"]
    )


def test_standalone_momentum_proof_does_not_disable_legacy_pullback_hybrid(monkeypatch):
    """Independent new family gating must not alter old hybrid Pullback policy."""
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    row = _proof(
        momentum_pass=False,
        breakout_pass=False,
        vcp_pass=False,
        trend_template_pass=False,
    )
    selected = _choose(
        row, pb1_score=.45, momentum_score=.96, pullback_score=.90,
        breakout_score=.99, vcp_score=.99,
    )
    assert selected == "momentum_pullback"
    assert "momentum_pullback" in row["independent_eligible_entry_styles"]
    assert "momentum" not in row["independent_eligible_entry_styles"]
    assert "breakout" not in row["independent_eligible_entry_styles"]
    assert "vcp" not in row["independent_eligible_entry_styles"]
    assert row["independent_proof_status"]["momentum"] is False


@pytest.mark.parametrize(
    ("source", "volume20", "expected", "vcp_allowed"),
    [
        (None, 100000, "momentum", False),
        ("completed_daily_ohlcv", None, "momentum", False),
        ("completed_daily_ohlcv", 0, "momentum", False),
        ("completed_daily_ohlcv", 100000, "vcp", True),
    ],
)
def test_vcp_without_live_eligible_prep_proof_cannot_crowd_out_momentum(
    monkeypatch, source, volume20, expected, vcp_allowed,
):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    row = _proof(
        vcp_evidence_source=source, vcp_daily_avg_volume20=volume20,
    )
    selected = _choose(row, momentum_score=.83, breakout_score=.60, vcp_score=.99)
    assert selected == expected
    assert ("vcp" in row["independent_eligible_entry_styles"]) is vcp_allowed
    assert row["independent_proof_status"]["vcp"] is vcp_allowed
