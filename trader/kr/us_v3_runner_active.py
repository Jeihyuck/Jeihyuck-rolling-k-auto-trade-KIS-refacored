"""Native KR PB1 V3 runner proposal using the original US V3 policy.

This module cannot submit. The enclosing PB1 engine must obtain the same
native SELL claim, durable stage reservation and original KIS fill proof as TP.

Never label DB/cache/third-party OHLCV as KIS-verified history. Without a
KIS-origin, completed daily chart the runner fails closed. A same-day daily
high is NOT evidence of a high after the original BUY; only later daily highs
and current native executable bid are eligible. Missing KR trend provenance
disables only trend-specific US rules (R1/R2); US R3/R4 still run on native
price/ATR evidence without a synthetic HEALTHY/TRIM/EXIT state.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Mapping

from trader.kr.us_standard_profit_parity import (
    STAGES, evaluate_v3_runner_shadow, verified_contract,
)
from trader.us.pb1.swing_runner_exit_v3 import (
    chandelier_from_verified_completed_bars,
)

_ALLOWED = {"kis"}
_RUNNER = {"PROFIT_DEFENSE_PARTIAL", "RUNNER_FINAL"}


def _day(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        text = str(value).strip().replace("Z", "+00:00")
        return date.fromisoformat(text[:10])
    except (TypeError, ValueError):
        return None


def _valid_daily_rows(rows: Any, *, source: str | None,
                      today: date) -> list[dict] | None:
    if str(source or "").lower() not in _ALLOWED:
        return None
    if rows is None:
        return None
    if hasattr(rows, "to_dict"):
        rows = rows.to_dict("records")
    if not isinstance(rows, list):
        return None
    clean = []
    for row in rows:
        if not isinstance(row, Mapping):
            return None
        d = _day(row.get("date"))
        if d is None:
            return None
        # Never include a forming market-session daily bar.
        if d >= today:
            continue
        try:
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])
        except (TypeError, ValueError, KeyError):
            return None
        import math
        if not all(math.isfinite(x) and x > 0 for x in (high, low, close)):
            return None
        if not low <= close <= high:
            return None
        clean.append({
            "trade_date": d.isoformat(), "high": high,
            "low": low, "close": close,
            "complete": True, "source": "KIS_DAILY_VERIFIED",
            "adjustment_basis": "KIS_DAILY_NATIVE",
        })
    clean.sort(key=lambda x: x["trade_date"])
    if len(clean) < 24 or len({x["trade_date"] for x in clean}) != len(clean):
        return None
    last_day = date.fromisoformat(clean[-1]["trade_date"])
    if (today - last_day) > timedelta(days=7):
        return None
    return clean


def evaluate_kr_v3_runner(
    *, contract: Mapping[str, Any], stage_states: Mapping[str, Mapping[str, Any]],
    position: Mapping[str, Any], verified_entry_price: float,
    executable_price: float, holding_qty: int, orderable_qty: int,
    completed_daily: Any, completed_daily_source: str | None,
    today: date, unresolved_sell: bool,
) -> dict:
    """Return an ACTIVE_RUNNER candidate or explicit HOLD/BLOCK with provenance.

    V3 decisions are *proposals*. No broker permission is conferred here.
    """
    def hold(reason: str) -> dict:
        return {"action": "HOLD", "reason": reason, "order_authorized": False}

    if not verified_contract(contract) or contract.get("runner_version") != "US_V3":
        return {"action": "BLOCK", "reason": "V3_FROZEN_CONTRACT_INVALID",
                "order_authorized": False}
    if contract.get("activation_mode") != "ACTIVE_PRACTICE":
        return hold("V3_NOT_APPROVED_FOR_NATIVE_ORDER")
    if unresolved_sell:
        return {"action": "BLOCK", "reason": "V3_UNRESOLVED_SELL",
                "order_authorized": False}
    for stage, *_ in STAGES:
        saved = dict(stage_states.get(stage) or {})
        if saved.get("status") != "FILLED" or saved.get("broker_fill_proven") is not True:
            return {"action": "BLOCK", "reason": "V3_PREDECESSOR_KIS_FILL_UNPROVEN",
                    "stage": stage, "order_authorized": False}
    rows = _valid_daily_rows(completed_daily, source=completed_daily_source, today=today)
    if not rows:
        return hold("V3_KIS_COMPLETED_DAILY_PROOF_MISSING")
    entry_date = _day(position.get("entry_date") or position.get("last_fill_at"))
    if entry_date is None or entry_date > today:
        return hold("V3_ORIGINAL_ENTRY_DATE_UNVERIFIED")
    # KIS daily entry-day HIGH could predate BUY and hence is ineligible.
    later_highs = [
        bar["high"] for bar in rows
        if date.fromisoformat(bar["trade_date"]) > entry_date
    ]
    if not later_highs:
        return hold("V3_POST_ENTRY_KIS_HIGH_MISSING")
    try:
        bid = float(executable_price)
        entry = float(verified_entry_price)
        if bid <= 0 or entry <= 0:
            return hold("V3_EXECUTABLE_PRICE_INVALID")
    except (TypeError, ValueError, OverflowError):
        return hold("V3_EXECUTABLE_PRICE_INVALID")
    verified_high = max(max(later_highs), bid)
    atr = chandelier_from_verified_completed_bars(
        rows, decision_date=today.isoformat(), atr_period=14,
        lookback=22, multiplier=3.0, ratchet=True,
    )
    if atr.get("valid") is not True:
        return hold("V3_COMPLETED_CHANDELIER_UNVERIFIED:" + str(atr.get("reason")))
    partial = str((stage_states.get("PROFIT_DEFENSE_PARTIAL") or {}).get("status") or "NOT_TRIGGERED").upper()
    final = str((stage_states.get("RUNNER_FINAL") or {}).get("status") or "NOT_TRIGGERED").upper()
    # The original US evaluator performs all R1-R4 priority and quantity
    # calculations. Unknown KR trend is never manufactured as verified.
    result = evaluate_v3_runner_shadow(
        contract=contract, stage_states=stage_states,
        verified_entry_price=entry, verified_high=verified_high,
        executable_price=bid, holding_qty=holding_qty,
        orderable_qty=orderable_qty, high_verified=True,
        trend_state="UNKNOWN", trend_verified=False,
        atr_evidence=atr, partial_stage_status=partial,
        final_stage_status=final, unresolved_sell=unresolved_sell,
    )
    if result.get("source_policy_action") != "SELL":
        return {**result, "action": "HOLD",
                "order_authorized": False}
    stage = str(result.get("stage") or "")
    quantity = result.get("qty")
    if stage not in _RUNNER or isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
        return {"action": "BLOCK", "reason": "V3_ORDER_STAGE_OR_QTY_INVALID",
                "order_authorized": False}
    return {**result, "action": "ACTIVE_RUNNER", "stage": stage,
            "qty": quantity, "native_source": "KIS_COMPLETED_DAILY_VERIFIED",
            "verified_high": verified_high, "atr_evidence": atr,
            "order_authorized": False}



# A direct original KIS HTTP response only. The generic get_daily_candles()
# and OHLCV provider read DB/Parquet caches and cannot attest provenance.
# Do not turn those values into falsely labelled "KIS_DAILY_VERIFIED".
_NATIVE_CACHE: dict[tuple[str, str, str], tuple[float, list[dict]]] = {}


def load_kr_kis_native_completed_daily(
    *, kis: Any, code: str, today: date,
) -> tuple[list[dict] | None, str | None]:
    """Read-only bounded native KIS daily chart; cache verified response <=10min.

    Do not use this on real accounts or in HTTP-disabled diagnostic mode.
    The broker wrapper's stage deadline still governs native HTTP calls.
    """
    import os
    import time
    from trader.kis_wrapper import API_BASE_URL, _pick_tr, kis_http_enabled

    symbol = str(code).zfill(6)
    if (
        str(getattr(kis, "env", "")).lower() != "practice"
        or str(os.getenv("KIS_ENV", "")).lower() != "practice"
        or not kis_http_enabled() or not symbol.isdecimal()
    ):
        return None, None
    account = str(getattr(kis, "CANO", "") or "")
    key = (account, symbol, today.isoformat())
    now = time.monotonic()
    saved = _NATIVE_CACHE.get(key)
    if saved is not None and now - saved[0] <= 600:
        return saved[1], "kis"
    chart_ids = _pick_tr("practice", "DAILY_CHART")
    if not chart_ids:
        return None, None
    # KIS returns recent daily bars as output2, sometimes in descending
    # order. 120 calendar days is sufficient for US V3 ATR14+22 chandelier.
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": symbol,
        "fid_input_date_1": (today - timedelta(days=120)).strftime("%Y%m%d"),
        "fid_input_date_2": today.strftime("%Y%m%d"),
        "fid_org_adj_prc": "0",
        "fid_period_div_code": "D",
    }
    url = f"{API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    for tr in list(chart_ids)[:1]:
        try:
            response = kis._safe_request(
                "GET", url, headers=kis._headers(tr), params=params,
                timeout=(2.5, 6.0),
            )
            if response.status_code != 200:
                continue
            payload = response.json()
            if not isinstance(payload, dict) or str(payload.get("rt_cd")) != "0":
                continue
            raw = payload.get("output2")
            if not isinstance(raw, list):
                continue
            bars = []
            for row in raw:
                if not isinstance(row, dict):
                    return None, None
                # KIS sometimes pads chart responses with empty-date rows;
                # these are not completed candles and must not poison the
                # valid dated portion of the *same* original native response.
                if not str(row.get("stck_bsop_date") or "").strip():
                    continue
                try:
                    d = str(row["stck_bsop_date"])
                    parsed = date.fromisoformat(d)
                    if parsed >= today:
                        continue
                    bars.append({
                        "date": parsed.isoformat(),
                        "high": float(row["stck_hgpr"]),
                        "low": float(row["stck_lwpr"]),
                        "close": float(row["stck_clpr"]),
                    })
                except (KeyError, ValueError, TypeError):
                    return None, None
            valid = _valid_daily_rows(bars, source="kis", today=today)
            if valid is not None:
                _NATIVE_CACHE[key] = (now, bars)
                return bars, "kis"
        except Exception:
            # A transient KIS error is NOT authorization to use DB-derived
            # prices/unknown ATR in a profit-defense native order.
            continue
    return None, None
