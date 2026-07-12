# -*- coding: utf-8 -*-
"""US-only accessors for shared price_daily OHLCV storage.

Never import KR OHLCV repository helpers here; every query is explicitly
scoped by market='US' and code is a raw uppercase US symbol (no zfill).
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from sqlalchemy import text
from trader.db.engine import get_engine
from trader.us.dates import canonical_us_bar_date, canonical_us_bar_date_str

logger = logging.getLogger(__name__)
_MEM_US_DAILY: dict[tuple[str, date], dict] = {}


def normalize_us_price_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _engine_or_none():
    try:
        return get_engine()
    except Exception:
        return None


def _bar_date(row: dict) -> date | None:
    return canonical_us_bar_date(row.get("date") or row.get("xymd") or row.get("stck_bsop_date"))


def _num(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", ""))
    except Exception:
        return default


def _row_from_bar(symbol: str, bar: dict, source: str) -> dict | None:
    d = _bar_date(bar)
    close = _num(bar.get("close", bar.get("clos", bar.get("stck_clpr"))))
    if not d or close is None:
        return None
    open_v = _num(bar.get("open", bar.get("stck_oprc")), close)
    high_v = _num(bar.get("high", bar.get("stck_hgpr")), close)
    low_v = _num(bar.get("low", bar.get("stck_lwpr")), close)
    vol = _num(bar.get("volume", bar.get("tvol", bar.get("acml_vol"))), 0.0) or 0.0
    value = _num(bar.get("value", bar.get("amount")), None)
    return {
        "market": "US", "code": normalize_us_price_symbol(symbol), "date": d,
        "open": open_v, "high": high_v, "low": low_v, "close": close,
        "volume": int(vol), "value": value, "source": source,
    }


def _to_provider_row(row: dict, symbol: str | None = None) -> dict:
    d = canonical_us_bar_date(row.get("date"))
    code = normalize_us_price_symbol(symbol or row.get("code") or row.get("symbol"))
    return {
        "date": d.isoformat() if d else str(row.get("date") or ""),
        "xymd": d.strftime("%Y%m%d") if d else str(row.get("date") or ""),
        "open": row.get("open"), "high": row.get("high"), "low": row.get("low"),
        "close": row.get("close"), "clos": str(row.get("close") or "0"),
        "volume": row.get("volume"), "tvol": str(row.get("volume") or "0"),
        "value": row.get("value"), "source": row.get("source"), "symbol": code,
    }


def load_recent_us_daily_bars(*, symbol: str, before_date: str, limit: int = 260) -> list[dict]:
    sym = normalize_us_price_symbol(symbol)
    before = canonical_us_bar_date(before_date)
    if not sym or before is None:
        return []
    engine = _engine_or_none()
    if engine is None:
        rows = [r for (s, d), r in _MEM_US_DAILY.items() if s == sym and d < before]
        rows.sort(key=lambda r: r["date"], reverse=True)
        return [_to_provider_row(r, sym) for r in reversed(rows[:limit])]
    with engine.begin() as conn:
        db_rows = conn.execute(text("""
            SELECT date, open, high, low, close, volume, value, source
            FROM price_daily
            WHERE market = 'US' AND code = :symbol AND date < :before_date
            ORDER BY date DESC
            LIMIT :limit
        """), {"symbol": sym, "before_date": before, "limit": int(limit)}).mappings().all()
    rows = [dict(r) | {"code": sym} for r in db_rows]
    return [_to_provider_row(r, sym) for r in reversed(rows)]


def get_latest_us_daily_date(*, symbol: str, before_date: str | None = None) -> date | None:
    sym = normalize_us_price_symbol(symbol)
    before = canonical_us_bar_date(before_date) if before_date else None
    engine = _engine_or_none()
    if engine is None:
        dates = [d for (s, d), r in _MEM_US_DAILY.items() if s == sym and (before is None or d < before)]
        return max(dates) if dates else None
    where_before = "AND date < :before_date" if before else ""
    params = {"symbol": sym}
    if before:
        params["before_date"] = before
    with engine.begin() as conn:
        row = conn.execute(text(f"""
            SELECT MAX(date) AS latest_date
            FROM price_daily
            WHERE market = 'US' AND code = :symbol {where_before}
        """), params).mappings().first()
    return canonical_us_bar_date(row.get("latest_date")) if row else None


def upsert_us_daily_bars(*, symbol: str, bars: list[dict], source: str = "KIS_US_DAILY") -> int:
    sym = normalize_us_price_symbol(symbol)
    clean = [r for b in (bars or []) if (r := _row_from_bar(sym, b, source))]
    if not clean:
        return 0
    engine = _engine_or_none()
    if engine is None:
        for r in clean:
            _MEM_US_DAILY[(sym, r["date"])] = r
        logger.info("[US_OHLCV][UPSERT] symbol=%s fetched=%d upserted=%d source=%s", sym, len(bars or []), len(clean), source)
        return len(clean)
    with engine.begin() as conn:
        for r in clean:
            conn.execute(text("""
                INSERT INTO price_daily (market, code, date, open, high, low, close, volume, value, source)
                VALUES ('US', :symbol, :date, :open, :high, :low, :close, :volume, :value, :source)
                ON CONFLICT (market, code, date)
                DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                              close = EXCLUDED.close, volume = EXCLUDED.volume,
                              value = EXCLUDED.value, source = EXCLUDED.source
            """), {"symbol": sym, **r})
    logger.info("[US_OHLCV][UPSERT] symbol=%s fetched=%d upserted=%d source=%s", sym, len(bars or []), len(clean), source)
    return len(clean)


def audit_us_daily_history(*, symbol: str, before_date: str, required_bars: int = 260) -> dict:
    rows = load_recent_us_daily_bars(symbol=symbol, before_date=before_date, limit=required_bars)
    dates = [canonical_us_bar_date(r.get("date")) for r in rows]
    dates = [d for d in dates if d]
    invalid = sum(1 for r in rows if _num(r.get("close")) is None or (_num(r.get("close")) or 0) <= 0)
    duplicate_count = len(dates) - len(set(dates))
    out = {
        "symbol": normalize_us_price_symbol(symbol), "row_count": len(rows),
        "first_date": min(dates).isoformat() if dates else None,
        "last_date": max(dates).isoformat() if dates else None,
        "required_bars": required_bars, "enough_history": len(rows) >= required_bars,
        "invalid_close_count": invalid, "duplicate_count": duplicate_count,
    }
    quality = "OK" if len(rows) >= required_bars and invalid == 0 and duplicate_count == 0 else "INSUFFICIENT_DAILY_HISTORY"
    logger.info("[US_OHLCV][AUDIT] symbol=%s bars=%d first=%s last=%s quality=%s", out["symbol"], out["row_count"], out["first_date"], out["last_date"], quality)
    return out


def reset_us_daily_memory() -> None:
    _MEM_US_DAILY.clear()
