"""Practice KIS-only, read-only completed daily-bar cache for v3 ATR.

Designed for US PREP: the market Tick NEVER requests historical bars over HTTP.
A file is authoritative only for its claimed *source*, not for future returns
or the account's order/position state. Known OHLC anomaly symbols are fenced
pending independent source reconciliation.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


CACHE_CONTRACT = "US_V3_COMPLETED_KIS_DAILY_MODP0_V1"
DEFAULT_QUARANTINE = frozenset({"CL", "MDLZ", "GILD"})
_ROOT = Path(__file__).resolve().parents[3]


def _digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")).hexdigest()


def _day(raw: Any) -> date | None:
    try:
        token = str(raw or "").replace("-", "").strip()
        return datetime.strptime(token, "%Y%m%d").date() if len(token) == 8 else None
    except ValueError:
        return None


def _cache_path(symbol: str, decision_date: str, *, root: Path) -> Path:
    sym = str(symbol or "").upper().strip()
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,11}", sym):
        raise ValueError("US_V3_ATR_SYMBOL_UNVERIFIED")
    d = date.fromisoformat(str(decision_date))
    return root / "runtime" / "us-v3-atr" / d.isoformat() / (sym + ".json")


def normalize_kis_completed_bars(
    raw_rows: list[dict], *, symbol: str, decision_date: str,
) -> tuple[list[dict], str]:
    """Normalize only raw KIS MODP=0 bars before the US decision session.

    Caller must prove it used the authenticated practice KIS daily endpoint.
    These pure checks never turn a raw DB row into 'KIS_DAILY_VERIFIED'.
    """
    from trader.us.data_provider import normalize_daily_row
    from trader.us.market_calendar import previous_completed_us_session
    target = date.fromisoformat(decision_date)
    latest_expected = previous_completed_us_session(target)
    if symbol.upper() in DEFAULT_QUARANTINE:
        raise ValueError("US_V3_ATR_PRICE_SOURCE_DISCREPANCY_QUARANTINE")
    if not isinstance(raw_rows, list) or len(raw_rows) < 44:
        raise ValueError("US_V3_ATR_INSUFFICIENT_KIS_ROWS")
    parsed: dict[str, dict] = {}
    for raw in raw_rows:
        if not isinstance(raw, dict):
            raise ValueError("US_V3_ATR_RAW_ROW_INVALID")
        normalized = normalize_daily_row(raw)
        seen = _day(normalized.get("date"))
        if seen is None:
            raise ValueError("US_V3_ATR_BAR_DATE_UNVERIFIED")
        if seen >= target:
            # This KIS response contains a live/current partial bar.
            # Ignore it; never put it in ATR evidence.
            continue
        try:
            high = float(normalized["high"])
            low = float(normalized["low"])
            close = float(normalized["close"])
        except (ValueError, TypeError, KeyError):
            raise ValueError("US_V3_ATR_KIS_OHLC_MISSING") from None
        if not (
            math.isfinite(high) and math.isfinite(low)
            and math.isfinite(close) and low > 0 and low <= close <= high
        ):
            raise ValueError("US_V3_ATR_KIS_OHLC_INVALID")
        key = seen.isoformat()
        record = {
            "trade_date": key, "high": high, "low": low,
            "close": close, "complete": True,
            "source": "KIS_DAILY_VERIFIED",
            "adjustment_basis": "RAW_KIS_MODP0",
        }
        if key in parsed and parsed[key] != record:
            raise ValueError("US_V3_ATR_DUPLICATE_BAR_CONFLICT")
        parsed[key] = record
    ordered = [parsed[k] for k in sorted(parsed)]
    if len(ordered) < 44 or ordered[-1]["trade_date"] != latest_expected.isoformat():
        raise ValueError("US_V3_ATR_LAST_COMPLETED_DATE_MISMATCH")
    # A raw stock split/corporate action across the ATR seed window must be
    # separately verified; a discontinuity must not silently become huge TR.
    for previous, following in zip(ordered, ordered[1:]):
        ratio = following["close"] / previous["close"]
        if not 0.60 <= ratio <= 1.40:
            raise ValueError("US_V3_ATR_CORPORATE_ACTION_OR_GAP_UNVERIFIED")
    return ordered[-80:], latest_expected.isoformat()


def write_verified_kis_daily_cache(
    *, raw_rows: list[dict], symbol: str, decision_date: str,
    exchange: str, root: Path | None = None, now: datetime | None = None,
) -> dict:
    """Only call after an authenticated, GET-only practice KIS daily query."""
    if os.getenv("KIS_ENV", "").lower() != "practice":
        raise RuntimeError("US_V3_ATR_PRACTICE_ONLY")
    bars, latest = normalize_kis_completed_bars(
        raw_rows, symbol=symbol, decision_date=decision_date,
    )
    timestamp = now or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise ValueError("US_V3_ATR_NAIVE_CAPTURE_TIME")
    facts = {
        "contract": CACHE_CONTRACT,
        "symbol": symbol.strip().upper(),
        "exchange": exchange.upper(), "decision_date": decision_date,
        "last_completed_date": latest,
        "captured_utc": timestamp.astimezone(timezone.utc).isoformat(),
        "provider": "AUTHENTICATED_KIS_PRACTICE_GET_US_DAILY_PRICE_HISTORY",
        "modp": "0", "bars": bars,
    }
    signed = {**facts, "sha256": _digest(facts)}
    path = _cache_path(symbol, decision_date, root=root or _ROOT)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        os.chmod(tmp, 0o600)
        json.dump(signed, handle, ensure_ascii=False, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    return {
        "status": "KIS_DAILY_COMPLETED_CACHED",
        "symbol": facts["symbol"], "decision_date": decision_date,
        "last_completed_date": latest, "rows": len(bars),
        "sha256_prefix": signed["sha256"][:12],
    }


def load_verified_kis_daily_cache(
    *, symbol: str, decision_date: str,
    root: Path | None = None, now: datetime | None = None,
    max_age_seconds: int = 24 * 3600,
) -> list[dict]:
    """Disk-only, cheap and fail-closed; safe inside the SELL strategy Tick."""
    try:
        path = _cache_path(symbol, decision_date, root=root or _ROOT)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return []
        signature = payload.get("sha256")
        facts = {k: v for k, v in payload.items() if k != "sha256"}
        timestamp = datetime.fromisoformat(
            str(facts.get("captured_utc") or "").replace("Z", "+00:00")
        )
        observed = now or datetime.now(timezone.utc)
        if timestamp.tzinfo is None or observed.tzinfo is None:
            return []
        age = (observed - timestamp).total_seconds()
        if not 0 <= age <= max_age_seconds:
            return []
        if (not isinstance(signature, str) or signature != _digest(facts)
                or facts.get("contract") != CACHE_CONTRACT
                or facts.get("symbol") != symbol.upper()
                or facts.get("decision_date") != decision_date
                or facts.get("modp") != "0"
                or facts.get("provider") != "AUTHENTICATED_KIS_PRACTICE_GET_US_DAILY_PRICE_HISTORY"):
            return []
        # Check every bar and the last completed session again on read:
        # accepting the JSON digest alone is not an authenticity proof.
        from trader.us.market_calendar import previous_completed_us_session
        if facts.get("last_completed_date") != previous_completed_us_session(
            decision_date
        ).isoformat():
            return []
        bars = facts.get("bars")
        if not isinstance(bars, list) or len(bars) < 44:
            return []
        previous = None
        for row in bars:
            if not isinstance(row, dict):
                return []
            d = date.fromisoformat(str(row.get("trade_date")))
            if (previous is not None and d <= previous) or d >= date.fromisoformat(decision_date):
                return []
            previous = d
            if (row.get("source") != "KIS_DAILY_VERIFIED"
                    or row.get("complete") is not True
                    or row.get("adjustment_basis") != "RAW_KIS_MODP0"):
                return []
        if bars[-1]["trade_date"] != facts["last_completed_date"]:
            return []
        return bars
    except (ValueError, TypeError, KeyError, OSError, UnicodeError):
        return []
