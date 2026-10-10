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
