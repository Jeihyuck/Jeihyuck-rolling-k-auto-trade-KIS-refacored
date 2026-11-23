"""Point-in-time sector research, no BUY order, KIS network or DB writes."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import pytest

from trader.us.strategy.pullback_sector_research import (
    SectorResearchEvidenceError,
    point_in_time_sector_strength,
    pullback_research_priority,
)

END = date(2026, 11, 19)
DECISION = "2026-11-20T15:00:00+00:00"


def _bars(base: float, daily_growth: float, length: int = 64):
    sessions = []
    day = END
    while len(sessions) < length:
        if day.weekday() < 5:
            sessions.append(day)
        day -= timedelta(days=1)
    sessions.reverse()
    return [
        {
            "trade_date": day.isoformat(), "close": base * (1 + daily_growth) ** i,
            "complete": True, "adjustment_basis": "SPLIT_ADJUSTED",
            "source": "KIS_DAILY_VERIFIED",
            "available_at_utc": datetime.combine(day, datetime.min.time(),
                tzinfo=timezone.utc).replace(hour=23).isoformat(),
        }
        for i, day in enumerate(sessions)
    ]


def _kwargs():
    return {
        "stock_bars": _bars(100, .004),
        "sector_etf_bars": _bars(55, .003),
        "spy_bars": _bars(300, .002),
        "decision_at_utc": DECISION,
        "expected_completed_date": END.isoformat(),
        "sector_etf": "XLV",
        "sector_mapping_asof": "2026-11-19T15:00:00+00:00",
    }


def test_aligned_completed_stock_sector_and_spy_evidence_is_computable():
    result = point_in_time_sector_strength(**_kwargs())
    assert result["observations"] == 64
    assert result["sector_etf"] == "XLV"
    assert result["source"] == "VERIFIED_COMPLETED_ALIGNED_DAILY"
    for days in (20, 63):
        assert result[f"stock_vs_sector_{days}d"] > 0
        assert result[f"sector_vs_spy_{days}d"] > 0


@pytest.mark.parametrize("mutator,reason", [
    (lambda k: k["stock_bars"].pop(), "LAST_COMPLETED_BAR_STALE"),
    (lambda k: k["stock_bars"].append(k["stock_bars"][-1].copy()), "DUPLICATE_OR_CONFLICTING_BAR"),
    (lambda k: k["stock_bars"][-1].update({"available_at_utc": "2026-11-21T12:00:00+00:00"}), "LOOKAHEAD_AVAILABLE_AFTER_DECISION"),
    (lambda k: k["stock_bars"][-1].update({"complete": False}), "COMPLETED_DAILY_BAR_UNVERIFIED"),
    (lambda k: k["stock_bars"][-1].update({"source": "UNVERIFIED"}), "DAILY_SOURCE_UNVERIFIED"),
    (lambda k: k["stock_bars"][-1].update({"adjustment_basis": "OTHER"}), "ADJUSTMENT_BASIS_UNVERIFIED"),
    (lambda k: k["stock_bars"][-1].update({"close": 0}), "BAR_CLOSE_UNVERIFIED"),
])
def test_invalid_or_lookahead_data_is_rejected(mutator, reason):
    args = _kwargs()
    mutator(args)
    with pytest.raises(SectorResearchEvidenceError, match=reason):
        point_in_time_sector_strength(**args)


def test_future_sector_mapping_and_missing_sector_etf_fail_closed():
    args = _kwargs()
    args["sector_mapping_asof"] = "2026-11-25T00:00:00+00:00"
    with pytest.raises(SectorResearchEvidenceError, match="SECTOR_MAPPING_LOOKAHEAD"):
        point_in_time_sector_strength(**args)
    args = _kwargs()
    args["sector_etf"] = ""
    with pytest.raises(SectorResearchEvidenceError, match="SECTOR_ETF_MAPPING_MISSING"):
        point_in_time_sector_strength(**args)


def _candidate(symbol, score, momentum):
    return {
        "symbol": symbol, "score_final": score, "pullback_pass": True,
        "momentum_pass": momentum, "independent_entry_contract_v1": True,
        "entry_signal_proof_source": "completed_daily_ohlcv",
    }


def test_research_momentum_priority_does_not_delete_unconfirmed_pb1():
    rows = [_candidate("A", .95, False), _candidate("B", .70, True), _candidate("C", .55, False)]
    baseline = pullback_research_priority(rows, variant="baseline")
    enhanced = pullback_research_priority(rows, variant="momentum_first")
    assert [r["symbol"] for r in baseline] == ["A", "B", "C"]
    assert [r["symbol"] for r in enhanced] == ["B", "A", "C"]
    assert len(enhanced) == len(rows)
    assert rows[0]["score_final"] == .95
    assert not any("side" in row or "qty" in row for row in enhanced)


def test_research_never_promotes_unverified_momentum_or_unqualified_pb1():
    row = _candidate("X", .7, True)
    row.pop("entry_signal_proof_source")
    with pytest.raises(SectorResearchEvidenceError, match="MOMENTUM_PROOF_REQUIRED"):
        pullback_research_priority([row], variant="momentum_first")
    row = _candidate("Y", .7, True)
    row["pullback_pass"] = False
    with pytest.raises(SectorResearchEvidenceError, match="PULLBACK_PROOF_REQUIRED"):
        pullback_research_priority([row], variant="baseline")
