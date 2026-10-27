"""Fail-closed KR PB1 BUY volume proof; no broker or DB access.

Only completed *prior-session* OHLCV bars qualify as baseline.
Missing evidence remains UNKNOWN, not a zero-value signal.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from math import isfinite
from typing import Any
from zoneinfo import ZoneInfo

PROOF_VERSION = "kr_pb1_volume_proof_v1"
KST = ZoneInfo("Asia/Seoul")


def _day(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _positive(value: Any) -> float | None:
    try:
        result = float(value)
        return result if isfinite(result) and result > 0 else None
    except (TypeError, ValueError):
        return None


def build_kr_volume_proof(
    *,
    bars: Any,
    snapshot: dict[str, Any] | None,
    trade_date: Any,
    observed_at: datetime,
    code: str,
    market: str,
    entry_style: str,
    trigger_ok: bool = False,
    trigger_info: dict[str, Any] | None = None,
    previous_cumulative: float | None = None,
    price_above_vwap: bool | None = None,
    daily_source: str | None = None,
    expected_intraday_fraction: float | None = None,
) -> dict[str, Any]:
    """Validate previous completed bars and a same-session KIS cumulative tick.

    observed_at is quote receipt time, not the exchange timestamp.
    """
    proof: dict[str, Any] = {
        "source": "KIS_TICK_CACHE|DAILY_" + str(daily_source or "UNKNOWN"),
        "daily_source": str(daily_source or "UNKNOWN"),
        "proof_date": None,
        "quote_as_of": None,
        "quote_received_at": observed_at.isoformat(),
        "daily_avg_volume20": None,
        "daily_avg_volume50": None,
        "previous_day_volume": None,
        "intraday_cumulative_volume": None,
        "relative_volume_at_time": None,
        "volume_expansion_ratio": None,
        "volume_contraction_ratio": None,
        "average_trading_value20": None,
        "price_above_vwap": price_above_vwap,
        "price_direction": None,
        "accumulation_distribution_signal": None,
        "confirmation_state": "UNKNOWN",
        "failure_reason": "uninitialized",
        "proof_version": PROOF_VERSION,
        "code": str(code),
        "market": str(market),
        "entry_style": str(entry_style),
        "breakout_volume_confirmed": False,
    }

    def reject(reason: str, *, failed: bool = False) -> dict[str, Any]:
        proof["confirmation_state"] = "FAILED" if failed else "UNKNOWN"
        proof["failure_reason"] = reason
        return proof

    session = _day(trade_date)
    if session is None or not (str(code).isdigit() and len(str(code)) == 6):
        return reject("invalid_trade_date_or_code", failed=True)
    if not str(market).strip():
        return reject("missing_market")
    if observed_at.tzinfo is None:
        return reject("naive_observed_at")
    received = observed_at.astimezone(KST)
    if received.date() != session:
        return reject("quote_trade_date_mismatch")

    if bars is None or not hasattr(bars, "columns") or not {"date", "volume"} <= set(bars.columns):
        return reject("completed_bars_missing")
    history: list[tuple[date, float]] = []
    for row in bars[["date", "volume"]].to_dict("records"):
        candle_day = _day(row.get("date"))
        if candle_day is None:
            return reject("invalid_daily_bar_date")
        if candle_day >= session:
            continue  # NEVER use the incomplete current daily bar.
        volume = _positive(row.get("volume"))
        if volume is None:
            return reject("invalid_completed_bar_volume")
        history.append((candle_day, volume))
    history.sort(key=lambda item: item[0])
    if len(history) < 20:
        return reject("insufficient_completed_bars")
    if len({day for day, _ in history}) != len(history):
        return reject("duplicate_completed_bar_date", failed=True)
    if session - history[-1][0] > timedelta(days=14):
        return reject("completed_bar_stale")
    proof["proof_date"] = history[-1][0].isoformat()
    proof["previous_day_volume"] = history[-1][1]
    proof["daily_avg_volume20"] = sum(vol for _, vol in history[-20:]) / 20
    if len(history) >= 50:
        proof["daily_avg_volume50"] = sum(vol for _, vol in history[-50:]) / 50

    quote = snapshot if isinstance(snapshot, dict) else {}
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    quoted_code = str(quote.get("symbol") or quote.get("code") or "").strip().lstrip("A")
    raw_quote_day = (quote.get("trade_date") or quote.get("stck_bsop_date")
                     or raw.get("trade_date") or raw.get("stck_bsop_date"))
    if raw_quote_day is not None:
        actual_quote_day = _day(raw_quote_day)
        if actual_quote_day is None or actual_quote_day != session:
            return reject("kis_quote_trade_date_mismatch", failed=True)
    if quoted_code and quoted_code.zfill(6) != str(code).zfill(6):
        return reject("quote_symbol_mismatch", failed=True)
    # stck_vol is not a substitute: it can represent a single trade.
    cumulative = _positive(quote.get("acml_vol") or raw.get("acml_vol"))
    if cumulative is None:
        return reject("kis_cumulative_volume_missing")
    proof["intraday_cumulative_volume"] = cumulative
    if previous_cumulative is not None:
        previous = _positive(previous_cumulative)
        if previous is None:
            return reject("previous_cumulative_invalid", failed=True)
        if cumulative < previous:
            return reject("cumulative_volume_decreased", failed=True)

    trade_clock = quote.get("stck_cntg_hour") or raw.get("stck_cntg_hour")
    clock = str(trade_clock or "").zfill(6)
    if len(clock) != 6 or not clock.isdigit():
        return reject("kis_exchange_quote_time_missing")
    try:
        exchange_ts = datetime.combine(
            session, datetime.strptime(clock, "%H%M%S").time(), tzinfo=KST
        )
    except ValueError:
        return reject("kis_exchange_quote_time_invalid", failed=True)
    proof["quote_as_of"] = exchange_ts.isoformat()
    age_seconds = (received - exchange_ts).total_seconds()
    if age_seconds < -10 or age_seconds > 600:
        return reject("kis_exchange_quote_stale_or_future")

    # Raw cumulative/day-average ratio is NOT time-adjusted RVOL.
    # Never use it alone to approve a 1.2x or 1.5x intraday threshold.
    proof["volume_expansion_ratio"] = cumulative / proof["daily_avg_volume20"]
    # Optional externally validated profile of completed historical intraday bars.
    # Never derive this fraction from clock time or the current-day full-volume ratio.
    if expected_intraday_fraction is not None:
        try:
            fraction = float(expected_intraday_fraction)
        except (TypeError, ValueError):
            return reject("historical_intraday_profile_invalid", failed=True)
        if not isfinite(fraction) or not 0.0 < fraction <= 1.0:
            return reject("historical_intraday_profile_invalid", failed=True)
        proof["relative_volume_at_time"] = cumulative / (proof["daily_avg_volume20"] * fraction)
    style = str(entry_style or "").upper()
    if style in {"BREAKOUT", "ENTRY_BREAKOUT"}:
        info = trigger_info if isinstance(trigger_info, dict) else {}
        if not trigger_ok:
            return reject("breakout_price_trigger_missing", failed=True)
        if info.get("vol_ok") is not True:
            return reject("breakout_volume_trigger_missing")
        proof["breakout_volume_confirmed"] = True
    proof["confirmation_state"] = "VERIFIED"
    proof["failure_reason"] = None
    return proof
