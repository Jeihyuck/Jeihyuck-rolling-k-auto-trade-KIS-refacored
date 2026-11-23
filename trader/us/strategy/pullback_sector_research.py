"""Point-in-time sector evidence for US pullback *research*, no trading I/O.

Does not grant BUY eligibility, alter PB1 policy, score_final or submit
orders.  An experiment may compare ranked cohorts after all completed-bar
and availability checks pass.  Never substitute missing ETF strength with
a default 0.5 pseudo-signal.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import math
from zoneinfo import ZoneInfo


class SectorResearchEvidenceError(ValueError):
    pass


def _utc(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("timezone missing")
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError) as exc:
        raise SectorResearchEvidenceError("ASOF_TIMESTAMP_UNVERIFIED") from exc


def _completed_series(rows: list[dict], *, decision_at: datetime,
                      expected_completed_date: date) -> dict[date, float]:
    prices: dict[date, float] = {}
    basis = None
    for row in rows:
        if not isinstance(row, dict) or row.get("complete") is not True:
            raise SectorResearchEvidenceError("COMPLETED_DAILY_BAR_UNVERIFIED")
        if row.get("source") not in {"KIS_DAILY_VERIFIED", "INDEPENDENT_DAILY_VERIFIED"}:
            raise SectorResearchEvidenceError("DAILY_SOURCE_UNVERIFIED")
        this_basis = row.get("adjustment_basis")
        if not this_basis or (basis is not None and basis != this_basis):
            raise SectorResearchEvidenceError("ADJUSTMENT_BASIS_UNVERIFIED")
        basis = this_basis
        try:
            day = date.fromisoformat(str(row.get("trade_date") or row.get("date")))
        except (TypeError, ValueError) as exc:
            raise SectorResearchEvidenceError("BAR_DATE_UNVERIFIED") from exc
        if day > expected_completed_date:
            # A same-day unfinished candle or future bar is never historical evidence.
            raise SectorResearchEvidenceError("FUTURE_COMPLETED_BAR")
        if _utc(row.get("available_at_utc")) > decision_at:
            raise SectorResearchEvidenceError("LOOKAHEAD_AVAILABLE_AFTER_DECISION")
        try:
            price = float(row.get("close"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise SectorResearchEvidenceError("BAR_CLOSE_UNVERIFIED") from exc
        if not math.isfinite(price) or price <= 0:
            raise SectorResearchEvidenceError("BAR_CLOSE_UNVERIFIED")
        if day in prices:
            raise SectorResearchEvidenceError("DUPLICATE_OR_CONFLICTING_BAR")
        prices[day] = price
    if not prices or max(prices) != expected_completed_date:
        raise SectorResearchEvidenceError("LAST_COMPLETED_BAR_STALE")
    return prices


def point_in_time_sector_strength(*, stock_bars: list[dict],
                                  sector_etf_bars: list[dict],
                                  spy_bars: list[dict],
                                  decision_at_utc: str,
                                  expected_completed_date: str,
                                  sector_etf: str,
                                  sector_mapping_asof: str) -> dict:
    """Return raw excess total-price returns for 20/63 trading days.

    All three price paths must have the same 64 completed trade dates. The
    sector ETF mapping must be known by decision time. No sector is inferred
    from a modern classification of a historical ticker.
    """
    if not sector_etf or not str(sector_etf).strip():
        raise SectorResearchEvidenceError("SECTOR_ETF_MAPPING_MISSING")
    decision = _utc(decision_at_utc)
    if _utc(sector_mapping_asof) > decision:
        raise SectorResearchEvidenceError("SECTOR_MAPPING_LOOKAHEAD")
    try:
        last_day = date.fromisoformat(str(expected_completed_date))
    except (TypeError, ValueError) as exc:
        raise SectorResearchEvidenceError("EXPECTED_DATE_UNVERIFIED") from exc
    if last_day >= decision.astimezone(ZoneInfo("America/New_York")).date():
        # PREP research uses only strictly prior ET completed sessions.
        raise SectorResearchEvidenceError("EXPECTED_BAR_NOT_YET_COMPLETED")
    stock = _completed_series(stock_bars, decision_at=decision, expected_completed_date=last_day)
    sector = _completed_series(sector_etf_bars, decision_at=decision, expected_completed_date=last_day)
    spy = _completed_series(spy_bars, decision_at=decision, expected_completed_date=last_day)
    common = sorted(set(stock) & set(sector) & set(spy))
    if len(common) < 64 or common[-1] != last_day:
        raise SectorResearchEvidenceError("UNALIGNED_64_COMPLETED_BARS")
    dates = common[-64:]
    # Reject gaps where a provider omitted a session present for a peer.
    # In a normal US equities calendar the three must share each session.
    if any(set(series) & set(dates) != set(dates) for series in (stock, sector, spy)):
        raise SectorResearchEvidenceError("UNALIGNED_COMPLETED_DAILY_BARS")

    def ret(series: dict[date, float], window: int) -> float:
        return series[dates[-1]] / series[dates[-1 - window]] - 1.0

    evidence = {"sector_etf": sector_etf, "as_of": str(last_day),
                "decision_at_utc": decision.isoformat(),
                "source": "VERIFIED_COMPLETED_ALIGNED_DAILY", "observations": 64}
    for days in (20, 63):
        stock_return = ret(stock, days)
        sector_return = ret(sector, days)
        spy_return = ret(spy, days)
        evidence.update({
            f"stock_return_{days}d": stock_return,
            f"sector_return_{days}d": sector_return,
            f"spy_return_{days}d": spy_return,
            f"sector_vs_spy_{days}d": sector_return - spy_return,
            f"stock_vs_sector_{days}d": stock_return - sector_return,
        })
    return evidence


def pullback_research_priority(rows: list[dict], *, variant: str) -> list[dict]:
    """Pure A/B cohort sort: no BUY permission and no owner change.

    `baseline` retains original score_final. `momentum_first` ranks
    *already PB1-qualified* candidates with independently verified
    momentum first, while preserving all momentum-unconfirmed candidates.
    This is a research arm, NOT an operational BUY gate.
    """
    if variant not in {"baseline", "momentum_first"}:
        raise ValueError("UNREGISTERED_RESEARCH_VARIANT")
    for row in rows:
        if row.get("pullback_pass") is not True:
            raise SectorResearchEvidenceError("PULLBACK_PROOF_REQUIRED")
        if variant == "momentum_first" and (
            row.get("independent_entry_contract_v1") is not True
            or row.get("entry_signal_proof_source") != "completed_daily_ohlcv"
            or type(row.get("momentum_pass")) is not bool
        ):
            raise SectorResearchEvidenceError("MOMENTUM_PROOF_REQUIRED")
    if variant == "baseline":
        return sorted(rows, key=lambda r: (-float(r.get("score_final") or 0), str(r.get("symbol") or "")))
    return sorted(
        rows,
        key=lambda r: (
            0 if r["momentum_pass"] is True else 1,
            -float(r.get("score_final") or 0),
            str(r.get("symbol") or ""),
        ),
    )
