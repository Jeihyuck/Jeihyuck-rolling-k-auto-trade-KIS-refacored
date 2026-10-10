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
