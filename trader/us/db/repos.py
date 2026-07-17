# -*- coding: utf-8 -*-
"""US DB Repository Functions.

us_* 테이블에 대한 CRUD 함수 모음.
기준 schema: migrations/0038_us_agent_tables.sql

절대 금지:
- 한국장 테이블(orders, positions, fills, runs, watchlist) 접근 금지
- trader.db.repos import 금지
- psycopg2 직접 사용 금지 → SQLAlchemy text() + get_engine() 사용
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

try:
    from sqlalchemy import text
    from trader.db.engine import get_engine as _get_engine_impl
    _SQLALCHEMY_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SQLALCHEMY_AVAILABLE = False  # type: ignore[assignment]
    def text(s: str, **kw):  # type: ignore[no-redef]
        raise RuntimeError("sqlalchemy not available")
    def _get_engine_impl():  # type: ignore[no-redef]
        raise RuntimeError("sqlalchemy not available")

# ---------------------------------------------------------------------------
# In-memory fallback store (offline / test 환경)
# ---------------------------------------------------------------------------

_MEM_WATCHLIST: list[dict] = []
_MEM_INTENTS: list[dict] = []
_MEM_ORDERS: list[dict] = []
_MEM_FILLS: list[dict] = []
_MEM_POSITIONS: list[dict] = []
_MEM_RECONCILE_LOGS: list[dict] = []
_MEM_RISK_STATE: dict[tuple[str, str], dict] = {}
_MEM_PROFIT_CAPTURE_STATE: dict[tuple[str, str], dict] = {}



_US_DAILY_METRIC_FIELDS = (
    "ma20", "ma50", "ma150", "ma200", "ma200_slope",
    "rs_20d", "rs_60d", "rs_120d", "trend_score",
    "daily_bar_count", "daily_metrics_as_of", "daily_metrics_source", "daily_history_quality",
)

def _merge_us_daily_metrics_meta(row: dict) -> dict:
    meta = dict(row.get("meta") or {}) if isinstance(row.get("meta"), dict) else {}
    for field in _US_DAILY_METRIC_FIELDS:
        if row.get(field) is not None:
            meta[field] = row.get(field)
    return meta


def _us_fill_idempotency_key(fill: dict, trade_date: str) -> tuple:
    return (
        trade_date,
        str(fill.get("symbol") or "").strip().upper(),
        str(fill.get("side") or "").strip().upper(),
        str(fill.get("order_no") or ""),
        str(fill.get("client_order_key") or ""),
        int(fill.get("qty", 0) or 0),
        float(fill.get("price_usd") or fill.get("price") or 0.0),
    )


def _us_fill_idempotency_key_text(fill: dict, trade_date: str) -> str:
    if fill.get("_fill_idempotency_key_override"):
        return str(fill["_fill_idempotency_key_override"])
    price_usd = float(fill.get("price_usd") or fill.get("price") or 0.0)
    qty = int(fill.get("qty", 0) or 0)
    order_no = str(fill.get("order_no") or "")
    client_order_key = str(fill.get("client_order_key") or "")
    symbol = str(fill.get("symbol") or "").strip().upper()
    side = str(fill.get("side") or "").strip().upper()
    return (
        f"{trade_date}|{symbol}|{side}|{order_no}|{client_order_key}|{qty}|{price_usd}"
    )


def reset_memory_stores() -> None:
    """테스트용 메모리 스토어 초기화."""
    global _MEM_WATCHLIST, _MEM_INTENTS, _MEM_ORDERS, _MEM_FILLS, _MEM_POSITIONS, _MEM_RECONCILE_LOGS, _MEM_RISK_STATE
    _MEM_WATCHLIST = []
    _MEM_INTENTS = []
    _MEM_ORDERS = []
    _MEM_FILLS = []
    _MEM_POSITIONS = []
    _MEM_RECONCILE_LOGS = []
    _MEM_RISK_STATE = {}
    try:
        from trader.us.db.price_daily_repo import reset_us_daily_memory
        reset_us_daily_memory()
    except Exception:
        pass


def _has_db_url() -> bool:
    return bool(os.getenv("PBCORE_DB_URL"))


def _get_engine_or_none():
    """SQLAlchemy engine 반환. PBCORE_DB_URL 없거나 연결 실패 시 None."""
    if not _has_db_url():
        return None
    try:
        return _get_engine_impl()
    except Exception as exc:
        logger.warning("[US_DB][WARN] DB engine init failed: %s", exc)
        return None


def _json_param(val: Any) -> str:
    """JSONB 파라미터를 JSON 문자열로 직렬화."""
    return json.dumps(val or {}, ensure_ascii=False, default=str)


def _today() -> str:
    return date.today().isoformat()


# ---------------------------------------------------------------------------
# Watchlist
# ---------------------------------------------------------------------------

def save_us_watchlist(entries: list[dict], trade_date: str | None = None) -> int:
    """us_watchlist 저장. schema: trade_date, symbol, exchange, strategy, score, meta"""
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        for e in entries:
            _MEM_WATCHLIST.append({**e, "trade_date": td})
        logger.info("[US_WATCHLIST][SAVE] count=%d (in-memory)", len(entries))
        return len(entries)
    count = 0
    try:
        with engine.begin() as conn:
            for e in entries:
                conn.execute(
                    text("""
                        INSERT INTO us_watchlist (trade_date, symbol, exchange, strategy, score, meta)
                        VALUES (:td, :symbol, :exchange, :strategy, :score, CAST(:meta AS jsonb))
                        ON CONFLICT (trade_date, symbol, strategy) DO UPDATE
                            SET score=EXCLUDED.score, meta=EXCLUDED.meta
                    """),
                    {
                        "td": td,
                        "symbol": e.get("symbol"),
                        "exchange": e.get("exchange", "NASDAQ"),
                        "strategy": e.get("strategy", "us_pb1"),
                        "score": e.get("score"),
                        "meta": _json_param(_merge_us_daily_metrics_meta(e)),
                    },
                )
                count += 1
    except Exception as exc:
        logger.error("[US_WATCHLIST][ERROR] %s", exc)
    logger.info("[US_WATCHLIST][SAVE] count=%d (db)", count)
    return count


# ---------------------------------------------------------------------------
# Order Intents
# ---------------------------------------------------------------------------

def save_order_intent(intent: dict, trade_date: str | None = None) -> bool:
    """us_order_intents 저장. schema: trade_date, client_order_key, symbol, exchange,
    side, qty, limit_price_usd, notional_usd, strategy, status, meta"""
    td = trade_date or _today()
    from trader.us.execution.order_identity import assert_same_identity, valid_identity
    if not valid_identity(intent.get("client_order_key")):
        logger.critical("[US_INTEGRITY][INVALID_ORDER_IDENTITY] store=us_order_intents")
        return False
    engine = _get_engine_or_none()
    if engine is None:
        for existing in _MEM_INTENTS:
            if existing.get("client_order_key") == intent.get("client_order_key"):
                assert_same_identity(existing, {**intent, "trade_date": td})
                return True
        _MEM_INTENTS.append({**intent, "trade_date": td, "status": "PENDING"})
        return True
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO us_order_intents
                        (trade_date, client_order_key, symbol, exchange, side, qty,
                         limit_price_usd, notional_usd, strategy, status, meta)
                    VALUES (:td, :cok, :symbol, :exchange, :side, :qty,
                            :limit_price_usd, :notional_usd, :strategy, :status,
                            CAST(:meta AS jsonb))
                    ON CONFLICT (client_order_key) DO NOTHING
                """),
                {
                    "td": td,
                    "cok": intent.get("client_order_key"),
                    "symbol": intent.get("symbol"),
                    "exchange": intent.get("exchange", "NASDAQ"),
                    "side": intent.get("side", "BUY"),
                    "qty": int(intent.get("qty", 0)),
                    "limit_price_usd": intent.get("limit_price_usd") or intent.get("limit_price"),
                    "notional_usd": intent.get("notional_usd"),
                    "strategy": intent.get("strategy", "us_pb1"),
                    "status": "PENDING",
                    "meta": _json_param(intent.get("meta")),
                },
            )
        return True
    except Exception as exc:
        logger.error("[US_INTENT][SAVE][ERROR] %s", exc)
        return False


def load_open_order_intents(trade_date: str | None = None) -> list[dict]:
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        return [i for i in _MEM_INTENTS if i.get("trade_date") == td and i.get("status") == "PENDING"]
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT * FROM us_order_intents WHERE trade_date=:td AND status='PENDING'"),
                {"td": td},
            )
            return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("[US_INTENT][LOAD][ERROR] %s", exc)
        return []


def mark_order_intent_sent(client_order_key: str) -> None:
    engine = _get_engine_or_none()
    if engine is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "SENT"
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE us_order_intents SET status='SENT' WHERE client_order_key=:cok"),
                {"cok": client_order_key},
            )
    except Exception as exc:
        logger.error("[US_INTENT][MARK_SENT][ERROR] %s", exc)


def mark_order_intent_blocked(client_order_key: str, reason: str = "") -> None:
    engine = _get_engine_or_none()
    if engine is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "BLOCKED"
                i["block_reason"] = reason
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE us_order_intents SET status='BLOCKED' WHERE client_order_key=:cok"),
                {"cok": client_order_key},
            )
    except Exception as exc:
        logger.error("[US_INTENT][MARK_BLOCKED][ERROR] %s", exc)


def mark_order_intent_rejected(client_order_key: str, reason: str = "") -> None:
    engine = _get_engine_or_none()
    if engine is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "REJECTED"
                i["reject_reason"] = reason
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE us_order_intents SET status='REJECTED' WHERE client_order_key=:cok"),
                {"cok": client_order_key},
            )
    except Exception as exc:
        logger.error("[US_INTENT][MARK_REJECTED][ERROR] %s", exc)


def mark_order_intent_dry_run(client_order_key: str) -> None:
    """Mark order intent as DRY_RUN (not SENT, to distinguish from real orders)."""
    engine = _get_engine_or_none()
    if engine is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "DRY_RUN"
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE us_order_intents SET status='DRY_RUN' WHERE client_order_key=:cok"),
                {"cok": client_order_key},
            )
    except Exception as exc:
        logger.error("[US_INTENT][MARK_DRY_RUN][ERROR] %s", exc)


# ---------------------------------------------------------------------------
# Orders — schema: trade_date, client_order_key, symbol, exchange, side,
#                  qty_requested, qty_filled, avg_price_usd, order_no,
#                  status, dry_run, meta
# 금지 컬럼: qty, order_type, limit_price_usd, notional_usd, raw_response
# ---------------------------------------------------------------------------

def _upsert_order(conn: Any, td: str, row: dict) -> None:
    from trader.us.execution.order_identity import assert_same_identity, require_identity
    require_identity(row.get("client_order_key"))
    existing = conn.execute(text("SELECT * FROM us_orders WHERE client_order_key=:cok FOR UPDATE"), {"cok": row["client_order_key"]}).fetchone()
    if existing:
        assert_same_identity(dict(existing._mapping), {**row, "trade_date": td})
    order_no = str(row.get("order_no") or "").strip()
    if order_no:
        broker_existing = conn.execute(text("SELECT * FROM us_orders WHERE trade_date=:td AND order_no=:ono FOR UPDATE"), {"td": td, "ono": order_no}).fetchone()
        if broker_existing:
            assert_same_identity(dict(broker_existing._mapping), {**row, "trade_date": td})
    conn.execute(
        text("""
            INSERT INTO us_orders
                (trade_date, client_order_key, symbol, exchange, side,
                 qty_requested, qty_filled, avg_price_usd, order_no,
                 status, dry_run, meta)
            VALUES (:td, :cok, :symbol, :exchange, :side,
                    :qty_requested, :qty_filled, :avg_price_usd, :order_no,
                    :status, :dry_run, CAST(:meta AS jsonb))
            ON CONFLICT (client_order_key) DO UPDATE
                SET qty_filled    =EXCLUDED.qty_filled,
                    avg_price_usd =EXCLUDED.avg_price_usd,
                    order_no      =EXCLUDED.order_no,
                    status        =EXCLUDED.status,
                    dry_run       =EXCLUDED.dry_run,
                    meta          =EXCLUDED.meta,
                    updated_at    =NOW()
        """),
        {
            "td": td,
            "cok": row["client_order_key"],
            "symbol": row["symbol"],
            "exchange": row.get("exchange", "NASDAQ"),
            "side": row["side"],
            "qty_requested": row["qty_requested"],
            "qty_filled": row.get("qty_filled", 0),
            "avg_price_usd": row.get("avg_price_usd"),
            "order_no": row.get("order_no", ""),
            "status": row["status"],
            "dry_run": bool(row.get("dry_run", False)),
            "meta": _json_param(row.get("meta")),
        },
    )


def save_order_ack(order_result: dict, trade_date: str | None = None) -> bool:
    """us_orders ACK 저장."""
    td = trade_date or _today()
    from trader.us.execution.order_identity import valid_identity, assert_same_identity
    if not valid_identity(order_result.get("client_order_key")):
        logger.critical("[US_INTEGRITY][INVALID_ORDER_IDENTITY] store=us_orders_ack")
        return False
    engine = _get_engine_or_none()
    if engine is None:
        for existing in _MEM_ORDERS:
            if existing.get("client_order_key") == order_result.get("client_order_key"):
                assert_same_identity(existing, {**order_result, "trade_date": td})
                existing.update({**order_result, "trade_date": td})
                return True
        _MEM_ORDERS.append({
            **order_result, "trade_date": td,
            "status": order_result.get("status", "ACK"),
            "qty_requested": int(order_result.get("qty_requested") or order_result.get("qty", 0)),
            "qty_filled": int(order_result.get("qty_filled", 0)),
            "dry_run": False,
        })
        return True
    try:
        row = {
            "client_order_key": order_result.get("client_order_key", ""),
            "symbol": order_result.get("symbol", ""),
            "exchange": order_result.get("exchange", "NASDAQ"),
            "side": order_result.get("side", "BUY"),
            "qty_requested": int(order_result.get("qty_requested") or order_result.get("qty", 0)),
            "qty_filled": int(order_result.get("qty_filled", 0)),
            "avg_price_usd": order_result.get("avg_price_usd"),
            "order_no": order_result.get("order_no", ""),
            "status": order_result.get("status", "ACK"),
            "dry_run": False,
            "meta": order_result.get("meta") or {},
        }
        with engine.begin() as conn:
            _upsert_order(conn, td, row)
        return True
    except Exception as exc:
        logger.error("[US_ORDER_ACK][SAVE][ERROR] %s", exc)
        return False


def save_order_reject(order_result: dict, trade_date: str | None = None) -> bool:
    """us_orders REJECT 저장."""
    td = trade_date or _today()
    from trader.us.execution.order_identity import valid_identity
    if not valid_identity(order_result.get("client_order_key")):
        return False
    engine = _get_engine_or_none()
    if engine is None:
        _MEM_ORDERS.append({
            **order_result, "trade_date": td, "status": "REJECTED",
            "qty_requested": int(order_result.get("qty_requested") or order_result.get("qty", 0)),
            "qty_filled": 0, "dry_run": False,
        })
        return True
    try:
        row = {
            "client_order_key": order_result.get("client_order_key", ""),
            "symbol": order_result.get("symbol", ""),
            "exchange": order_result.get("exchange", "NASDAQ"),
            "side": order_result.get("side", "BUY"),
            "qty_requested": int(order_result.get("qty_requested") or order_result.get("qty", 0)),
            "qty_filled": 0,
            "avg_price_usd": None,
            "order_no": "",
            "status": "REJECTED",
            "dry_run": False,
            "meta": {"reason": order_result.get("reason", ""), **(order_result.get("meta") or {})},
        }
        with engine.begin() as conn:
            _upsert_order(conn, td, row)
        return True
    except Exception as exc:
        logger.error("[US_ORDER_REJECT][SAVE][ERROR] %s", exc)
        return False


def save_dry_run_order(intent: dict, trade_date: str | None = None) -> bool:
    """us_orders DRY_RUN 저장. status='DRY_RUN', dry_run=True."""
    td = trade_date or _today()
    from trader.us.execution.order_identity import valid_identity
    if not valid_identity(intent.get("client_order_key")):
        return False
    engine = _get_engine_or_none()
    if engine is None:
        _MEM_ORDERS.append({
            **intent, "trade_date": td, "status": "DRY_RUN",
            "qty_requested": int(intent.get("qty", 0)),
            "qty_filled": 0, "dry_run": True,
        })
        return True
    try:
        row = {
            "client_order_key": intent.get("client_order_key", ""),
            "symbol": intent.get("symbol", ""),
            "exchange": intent.get("exchange", "NASDAQ"),
            "side": intent.get("side", "BUY"),
            "qty_requested": int(intent.get("qty", 0)),
            "qty_filled": 0,
            "avg_price_usd": intent.get("limit_price_usd") or intent.get("limit_price"),
            "order_no": "",
            "status": "DRY_RUN",
            "dry_run": True,
            "meta": intent.get("meta") or {},
        }
        with engine.begin() as conn:
            _upsert_order(conn, td, row)
        return True
    except Exception as exc:
        logger.error("[US_DRY_RUN_ORDER][SAVE][ERROR] %s", exc)
        return False


# ---------------------------------------------------------------------------
# Per-position intraday risk state — symbol-agnostic US soft-stop state
# ---------------------------------------------------------------------------

def _risk_state_key(symbol: str, trade_date: str) -> tuple[str, str]:
    return (str(trade_date), str(symbol or "").strip().upper())


def _normalize_risk_state(symbol: str, trade_date: str, state: dict | None) -> dict:
    raw = dict(state or {})
    now_iso = raw.get("updated_at") or datetime.now(timezone.utc).isoformat()
    nested_state = dict(raw.get("state") or {})
    for extra_key in ("stale_broker_mismatch", "broker_position_mismatch", "sell_blocked_for_day", "reason"):
        if extra_key in raw:
            nested_state[extra_key] = raw.get(extra_key)
    normalized = {
        "trade_date": trade_date,
        "symbol": str(symbol or "").strip().upper(),
        "soft_stop_breach_count": int(raw.get("soft_stop_breach_count") or 0),
        "first_soft_stop_seen_at": raw.get("first_soft_stop_seen_at"),
        "last_soft_stop_seen_at": raw.get("last_soft_stop_seen_at"),
        "lowest_price_since_breach": raw.get("lowest_price_since_breach"),
        "last_price": raw.get("last_price"),
        "last_pnl_pct": raw.get("last_pnl_pct"),
        "updated_at": now_iso,
        "state": nested_state,
    }
    for extra_key in ("stale_broker_mismatch", "broker_position_mismatch", "sell_blocked_for_day", "reason"):
        if extra_key in nested_state:
            normalized[extra_key] = nested_state.get(extra_key)
    return normalized


def _ensure_us_position_risk_state_table(conn) -> None:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS us_position_risk_state (
            trade_date DATE NOT NULL,
            symbol TEXT NOT NULL,
            soft_stop_breach_count INTEGER NOT NULL DEFAULT 0,
            first_soft_stop_seen_at TIMESTAMPTZ,
            last_soft_stop_seen_at TIMESTAMPTZ,
            lowest_price_since_breach NUMERIC,
            last_price NUMERIC,
            last_pnl_pct NUMERIC,
            state JSONB DEFAULT '{}'::jsonb,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (trade_date, symbol)
        )
    """))


def load_us_position_risk_state(symbol: str, trade_date: str) -> dict:
    """Load per-symbol US intraday risk state; never raises into trading loop."""
    key = _risk_state_key(symbol, trade_date)
    engine = _get_engine_or_none()
    if engine is None:
        return dict(_MEM_RISK_STATE.get(key, {}))
    try:
        with engine.begin() as conn:
            _ensure_us_position_risk_state_table(conn)
            row = conn.execute(
                text("""
                    SELECT trade_date, symbol, soft_stop_breach_count,
                           first_soft_stop_seen_at, last_soft_stop_seen_at,
                           lowest_price_since_breach, last_price, last_pnl_pct,
                           state, updated_at
                    FROM us_position_risk_state
                    WHERE trade_date = :td AND symbol = :symbol
                """),
                {"td": trade_date, "symbol": key[1]},
            ).mappings().first()
            return dict(row) if row else {}
    except Exception as exc:
        logger.warning("[US_RISK_STATE][LOAD_WARN] symbol=%s trade_date=%s err=%s", key[1], trade_date, exc)
        return dict(_MEM_RISK_STATE.get(key, {}))



def load_latest_us_position_risk_state(symbol: str, on_or_before_trade_date: str) -> dict:
    """Load most recent per-symbol risk state on or before trade_date."""
    key_symbol = str(symbol or "").strip().upper()
    td = str(on_or_before_trade_date)
    engine = _get_engine_or_none()
    if engine is None:
        candidates = [v for (d, sym), v in _MEM_RISK_STATE.items() if sym == key_symbol and str(d) <= td]
        candidates.sort(key=lambda r: (str(r.get("trade_date") or ""), str(r.get("updated_at") or "")), reverse=True)
        return dict(candidates[0]) if candidates else {}
    try:
        with engine.begin() as conn:
            _ensure_us_position_risk_state_table(conn)
            row = conn.execute(text("""
                SELECT trade_date, symbol, soft_stop_breach_count,
                       first_soft_stop_seen_at, last_soft_stop_seen_at,
                       lowest_price_since_breach, last_price, last_pnl_pct,
                       state, updated_at
                FROM us_position_risk_state
                WHERE symbol = :symbol AND trade_date <= :trade_date
                ORDER BY trade_date DESC, updated_at DESC
                LIMIT 1
            """), {"symbol": key_symbol, "trade_date": td}).mappings().first()
            return dict(row) if row else {}
    except Exception as exc:
        logger.warning("[US_RISK_STATE][LOAD_LATEST_WARN] symbol=%s trade_date=%s err=%s", key_symbol, td, exc)
        candidates = [v for (d, sym), v in _MEM_RISK_STATE.items() if sym == key_symbol and str(d) <= td]
        candidates.sort(key=lambda r: (str(r.get("trade_date") or ""), str(r.get("updated_at") or "")), reverse=True)
        return dict(candidates[0]) if candidates else {}


def load_latest_open_us_position_lifecycles(on_or_before_trade_date: str) -> dict[str, dict]:
    """Return latest open lifecycle per symbol on or before trade_date."""
    td = str(on_or_before_trade_date)
    out: dict[str, dict] = {}
    engine = _get_engine_or_none()
    if engine is None:
        latest: dict[str, dict] = {}
        for (d, sym), row in _MEM_RISK_STATE.items():
            if str(d) <= td and (sym not in latest or (str(d), str(row.get("updated_at") or "")) > (str(latest[sym].get("trade_date") or ""), str(latest[sym].get("updated_at") or ""))):
                latest[sym] = row
        for sym, row in latest.items():
            lifecycle = ((row.get("state") or {}).get("lifecycle") or {}) if isinstance(row.get("state"), dict) else {}
            if lifecycle.get("is_open") is True:
                out[sym] = dict(lifecycle)
        return out
    try:
        with engine.begin() as conn:
            _ensure_us_position_risk_state_table(conn)
            rows = conn.execute(text("""
                SELECT DISTINCT ON (symbol) trade_date, symbol, state, updated_at
                FROM us_position_risk_state
                WHERE trade_date <= :trade_date
                ORDER BY symbol, trade_date DESC, updated_at DESC
            """), {"trade_date": td}).mappings()
            for row in rows:
                st = row.get("state") or {}
                lifecycle = (st.get("lifecycle") or {}) if isinstance(st, dict) else {}
                if lifecycle.get("is_open") is True:
                    out[str(row.get("symbol") or "").upper()] = dict(lifecycle)
            return out
    except Exception as exc:
        logger.warning("[US_RISK_STATE][LOAD_OPEN_LIFECYCLES_WARN] trade_date=%s err=%s", td, exc)
        return {}

def save_us_position_risk_state(symbol: str, trade_date: str, state: dict) -> None:
    """Upsert per-symbol US intraday risk state; idempotent and fail-soft."""
    key = _risk_state_key(symbol, trade_date)
    normalized = _normalize_risk_state(key[1], trade_date, state)
    engine = _get_engine_or_none()
    if engine is None:
        _MEM_RISK_STATE[key] = normalized
        return
    try:
        with engine.begin() as conn:
            _ensure_us_position_risk_state_table(conn)
            conn.execute(
                text("""
                    INSERT INTO us_position_risk_state (
                        trade_date, symbol, soft_stop_breach_count,
                        first_soft_stop_seen_at, last_soft_stop_seen_at,
                        lowest_price_since_breach, last_price, last_pnl_pct,
                        state, updated_at
                    )
                    VALUES (
                        :td, :symbol, :soft_stop_breach_count,
                        :first_soft_stop_seen_at, :last_soft_stop_seen_at,
                        :lowest_price_since_breach, :last_price, :last_pnl_pct,
                        CAST(:state AS jsonb), NOW()
                    )
                    ON CONFLICT (trade_date, symbol) DO UPDATE SET
                        soft_stop_breach_count = EXCLUDED.soft_stop_breach_count,
                        first_soft_stop_seen_at = EXCLUDED.first_soft_stop_seen_at,
                        last_soft_stop_seen_at = EXCLUDED.last_soft_stop_seen_at,
                        lowest_price_since_breach = EXCLUDED.lowest_price_since_breach,
                        last_price = EXCLUDED.last_price,
                        last_pnl_pct = EXCLUDED.last_pnl_pct,
                        state = EXCLUDED.state,
                        updated_at = NOW()
                """),
                {
                    "td": trade_date,
                    "symbol": key[1],
                    "soft_stop_breach_count": normalized["soft_stop_breach_count"],
                    "first_soft_stop_seen_at": normalized["first_soft_stop_seen_at"],
                    "last_soft_stop_seen_at": normalized["last_soft_stop_seen_at"],
                    "lowest_price_since_breach": normalized["lowest_price_since_breach"],
                    "last_price": normalized["last_price"],
                    "last_pnl_pct": normalized["last_pnl_pct"],
                    "state": _json_param(normalized["state"]),
                },
            )
    except Exception as exc:
        logger.warning("[US_RISK_STATE][SAVE_WARN] symbol=%s trade_date=%s err=%s", key[1], trade_date, exc)
        _MEM_RISK_STATE[key] = normalized




def _normalize_profit_capture_state(trade_date: str, symbol: str, state: dict | None) -> dict:
    raw = dict(state or {})
    meta = dict(raw.get("meta") or {})
    out = {
        "trade_date": str(trade_date),
        "symbol": str(symbol or "").strip().upper(),
        "tp1_done": bool(raw.get("tp1_done") or raw.get("tp1_pending") or meta.get("tp1_done") or meta.get("tp1_pending")),
        "tp2_done": bool(raw.get("tp2_done") or raw.get("tp2_pending") or meta.get("tp2_done") or meta.get("tp2_pending")),
        "tp3_done": bool(raw.get("tp3_done") or raw.get("tp3_pending") or meta.get("tp3_done") or meta.get("tp3_pending")),
        "tp1_pending": bool(raw.get("tp1_pending") or meta.get("tp1_pending")),
        "tp2_pending": bool(raw.get("tp2_pending") or meta.get("tp2_pending")),
        "tp3_pending": bool(raw.get("tp3_pending") or meta.get("tp3_pending")),
        "tp1_order_key": raw.get("tp1_order_key") or meta.get("tp1_order_key"),
        "tp2_order_key": raw.get("tp2_order_key") or meta.get("tp2_order_key"),
        "tp3_order_key": raw.get("tp3_order_key") or meta.get("tp3_order_key"),
        "tp1_at": raw.get("tp1_at") or meta.get("tp1_at"),
        "tp2_at": raw.get("tp2_at") or meta.get("tp2_at"),
        "tp3_at": raw.get("tp3_at") or meta.get("tp3_at"),
        "last_profit_capture_at": raw.get("last_profit_capture_at") or meta.get("last_profit_capture_at"),
        "meta": meta,
    }
    return out


def load_us_profit_capture_state(trade_date: str, symbols: list[str]) -> dict[str, dict]:
    """Load TP1/TP2/TP3 state for same-day duplicate prevention.

    Uses us_position_risk_state.state.profit_capture when DB is available and an
    in-memory fallback when DB is unavailable, so tests/offline ticks remain
    idempotent.
    """
    out: dict[str, dict] = {}
    for symbol in symbols or []:
        sym = str(symbol or "").strip().upper()
        if not sym:
            continue
        mem = _MEM_PROFIT_CAPTURE_STATE.get((str(trade_date), sym))
        risk = load_us_position_risk_state(sym, trade_date)
        nested = (risk.get("state") or {}).get("profit_capture") if isinstance(risk.get("state"), dict) else None
        state = nested or mem or {}
        out[sym] = _normalize_profit_capture_state(trade_date, sym, state)
    return out


def mark_us_profit_capture_stage(
    trade_date: str,
    symbol: str,
    stage: str,
    order_key: str | None = None,
    qty: int | None = None,
    notional_usd: float | None = None,
    status: str = "PENDING",
) -> None:
    """Persist a profit-capture stage as PENDING/ACK/DONE to block duplicates."""
    sym = str(symbol or "").strip().upper()
    td = str(trade_date)
    if not sym:
        return
    stg = str(stage or "").lower().replace("_done", "")
    if stg not in {"tp1", "tp2", "tp3"}:
        return
    existing = load_us_profit_capture_state(td, [sym]).get(sym, {})
    now_iso = datetime.now(timezone.utc).isoformat()
    status_upper = str(status or "PENDING").upper()
    terminal_failure = status_upper in {"REJECTED", "FAILED", "EXPIRED", "CANCELLED", "CANCELED"}
    if terminal_failure:
        existing[f"{stg}_pending"] = False
        existing[f"{stg}_done"] = False
    else:
        existing[f"{stg}_pending"] = status_upper == "PENDING"
        if status_upper in {"ACK", "DONE", "FILLED", "PENDING"}:
            # Treat PENDING as done for duplicate prevention, while preserving pending flag.
            existing[f"{stg}_done"] = True
    if order_key:
        existing[f"{stg}_order_key"] = order_key
    if not terminal_failure:
        existing[f"{stg}_at"] = existing.get(f"{stg}_at") or now_iso
    existing["last_profit_capture_at"] = now_iso
    meta = dict(existing.get("meta") or {})
    meta.update({"last_stage": stg, "last_status": status_upper})
    if qty is not None:
        meta[f"{stg}_qty"] = int(qty)
    if notional_usd is not None:
        meta[f"{stg}_notional_usd"] = float(notional_usd)
    existing["meta"] = meta
    normalized = _normalize_profit_capture_state(td, sym, existing)
    _MEM_PROFIT_CAPTURE_STATE[(td, sym)] = normalized
    risk = load_us_position_risk_state(sym, td)
    risk_state = dict(risk.get("state") or {}) if isinstance(risk, dict) else {}
    risk_state["profit_capture"] = normalized
    risk["state"] = risk_state
    save_us_position_risk_state(sym, td, risk)


def mark_us_position_exit_stage(
    trade_date: str,
    symbol: str,
    stage: str,
    order_key: str | None = None,
    status: str = "PENDING",
    lifecycle_id: str | None = None,
) -> None:
    """Persist trend/time exit stage transitions in state.trend."""
    sym = str(symbol or "").strip().upper()
    stg = str(stage or "").strip().lower()
    if stg not in {"trend_trim", "trend_exit", "time_stop_trim", "time_stop_exit"}:
        return
    td = str(trade_date)
    risk = load_us_position_risk_state(sym, td) or load_latest_us_position_risk_state(sym, td) or {}
    state = dict(risk.get("state") or {})
    trend = dict(state.get("trend") or {})
    current_lifecycle = (state.get("lifecycle") or {}).get("lifecycle_id") if isinstance(state.get("lifecycle"), dict) else None
    if lifecycle_id and trend.get("lifecycle_id") and trend.get("lifecycle_id") != lifecycle_id:
        if current_lifecycle and current_lifecycle != lifecycle_id:
            logger.info("[US_POSITION][TREND_STAGE] symbol=%s stage=%s status=%s lifecycle_id=%s action=IGNORE_STALE_LIFECYCLE current_lifecycle_id=%s", sym, stg, status, lifecycle_id, current_lifecycle)
            return
        trend = {"lifecycle_id": lifecycle_id}
    status_upper = str(status or "PENDING").upper()
    terminal_failure = status_upper in {"REJECTED", "FAILED", "EXPIRED", "CANCELLED", "CANCELED"}
    done = status_upper in {"FILLED", "DONE"}
    pending = status_upper in {"PENDING", "ACK", "SUBMITTED", "PARTIALLY_FILLED", "ACK_DB_FAILED"}
    trend[f"{stg}_pending"] = bool(pending and not done and not terminal_failure)
    if done:
        trend[f"{stg}_done"] = True
    elif terminal_failure:
        trend[f"{stg}_done"] = False
    if order_key:
        trend[f"{stg}_order_key"] = order_key
    now_iso = datetime.now(timezone.utc).isoformat()
    if done:
        trend[f"{stg}_trade_date"] = td
        trend[f"{stg}_at"] = now_iso
    trend["last_exit_stage"] = stg
    trend["last_exit_stage_status"] = status_upper
    trend["updated_at"] = now_iso
    if lifecycle_id:
        trend["lifecycle_id"] = lifecycle_id
    state["trend"] = trend
    risk["state"] = state
    save_us_position_risk_state(sym, td, risk)
    logger.info("[US_POSITION][TREND_STAGE] symbol=%s stage=%s status=%s pending=%s done=%s order_key=%s lifecycle_id=%s", sym, stg, status_upper, trend.get(f"{stg}_pending"), trend.get(f"{stg}_done"), order_key, lifecycle_id)

def update_us_soft_stop_risk_state(
    *,
    symbol: str,
    trade_date: str,
    pnl_pct: float,
    current_price: float,
    now: datetime,
    soft_stop_pct: float,
) -> dict:
    """Increment/reset and persist per-symbol soft-stop state."""
    key_symbol = _risk_state_key(symbol, trade_date)[1]
    previous = load_us_position_risk_state(key_symbol, trade_date)
    now_iso = (now if now.tzinfo else now.replace(tzinfo=timezone.utc)).isoformat()
    if float(pnl_pct) <= -float(soft_stop_pct):
        previous_low = previous.get("lowest_price_since_breach")
        try:
            lowest = min(float(previous_low), float(current_price)) if previous_low is not None else float(current_price)
        except (TypeError, ValueError):
            lowest = float(current_price)
        state = {
            **previous,
            "soft_stop_breach_count": int(previous.get("soft_stop_breach_count") or 0) + 1,
            "first_soft_stop_seen_at": previous.get("first_soft_stop_seen_at") or now_iso,
            "last_soft_stop_seen_at": now_iso,
            "lowest_price_since_breach": lowest,
            "last_price": float(current_price),
            "last_pnl_pct": float(pnl_pct),
            "updated_at": now_iso,
        }
        save_us_position_risk_state(key_symbol, trade_date, state)
        logger.info(
            "[US_RISK_STATE][SOFT_STOP] symbol=%s trade_date=%s count=%d pnl_pct=%.4f price=%.4f action=increment",
            key_symbol, trade_date, state["soft_stop_breach_count"], pnl_pct, current_price,
        )
        return _normalize_risk_state(key_symbol, trade_date, state)

    state = {
        **previous,
        "soft_stop_breach_count": 0,
        "first_soft_stop_seen_at": None,
        "last_soft_stop_seen_at": None,
        "lowest_price_since_breach": None,
        "last_price": float(current_price),
        "last_pnl_pct": float(pnl_pct),
        "updated_at": now_iso,
    }
    save_us_position_risk_state(key_symbol, trade_date, state)
    logger.info(
        "[US_RISK_STATE][SOFT_STOP_RESET] symbol=%s trade_date=%s pnl_pct=%.4f price=%.4f action=reset",
        key_symbol, trade_date, pnl_pct, current_price,
    )
    return _normalize_risk_state(key_symbol, trade_date, state)


# ---------------------------------------------------------------------------
# Fills — schema: trade_date, symbol, exchange, side, qty, price_usd,
#                 order_no, client_order_key, filled_at, meta
# ---------------------------------------------------------------------------

def save_fills(fills: list[dict], trade_date: str | None = None) -> int:
    """us_fills 저장."""
    td = trade_date or _today()
    if not fills:
        return 0
    from trader.us.execution.order_identity import valid_identity
    for f in fills:
        if not valid_identity(f.get("client_order_key")) and valid_identity(f.get("order_no")):
            f["_fill_idempotency_key_override"] = _us_fill_idempotency_key_text(f, td)
            f["client_order_key"] = f"KIS_{td}_{str(f.get('order_no')).strip()}_{str(f.get('symbol') or '').upper()}_{str(f.get('side') or '').upper()}"
        try:
            order = load_us_order_for_fill(order_no=f.get("order_no"), client_order_key=f.get("client_order_key"),
                                           symbol=str(f.get("symbol") or ""), trade_date=td)
            order_meta = _parse_json_meta(order.get("meta"))
            fill_meta = dict(f.get("meta") or {}) if isinstance(f.get("meta"), dict) else {}
            for field in ("session", "session_run_id", "session_generation", "tick_id", "prep_run_id", "run_source"):
                if order_meta.get(field) is not None:
                    fill_meta.setdefault(field, order_meta[field])
            f["meta"] = fill_meta
        except Exception:
            pass
    invalid = [f for f in fills if not valid_identity(f.get("client_order_key"))]
    if invalid:
        logger.critical("[US_INTEGRITY][INVALID_ORDER_IDENTITY] store=us_fills count=%d", len(invalid))
        return 0
    engine = _get_engine_or_none()
    if engine is None:
        existing_keys = {_us_fill_idempotency_key(f, td) for f in _MEM_FILLS if f.get("trade_date") == td}
        skipped_duplicates = 0
        inserted = 0
        for f in fills:
            key = _us_fill_idempotency_key(f, td)
            if key in existing_keys:
                skipped_duplicates += 1
                for existing_fill in _MEM_FILLS:
                    if _us_fill_idempotency_key(existing_fill, td) == key:
                        existing_fill["updated_at"] = time.time()
                        existing_fill["fill_idempotency_key"] = _us_fill_idempotency_key_text(existing_fill, td)
                        break
                continue
            _MEM_FILLS.append({
                **f,
                "trade_date": td,
                "fill_idempotency_key": _us_fill_idempotency_key_text(f, td),
                "updated_at": time.time(),
            })
            existing_keys.add(key)
            inserted += 1
        logger.info("[US_FILLS][SAVE][DEDUP] skipped_duplicate=%d", skipped_duplicates)
        logger.info("[US_FILLS][SAVE][DONE] inserted=%d input=%d", inserted, len(fills))
        return inserted
    count = 0
    skipped_duplicates = 0
    try:
        with engine.begin() as conn:
            for f in fills:
                result = conn.execute(
                    text("""
                        INSERT INTO us_fills
                            (trade_date, symbol, exchange, side, qty, price_usd,
                             order_no, client_order_key, filled_at, meta, fill_idempotency_key)
                        VALUES (:td, :symbol, :exchange, :side, :qty, :price_usd,
                                :order_no, :cok, :filled_at, CAST(:meta AS jsonb), :fill_idempotency_key)
                        ON CONFLICT (fill_idempotency_key)
                        DO NOTHING
                    """),
                    {
                        "td": td,
                        "symbol": f.get("symbol"),
                        "exchange": f.get("exchange", "NASDAQ"),
                        "side": f.get("side"),
                        "qty": int(f.get("qty", 0)),
                        "price_usd": float(f.get("price_usd") or f.get("price", 0)),
                        "order_no": f.get("order_no", ""),
                        "cok": f.get("client_order_key", ""),
                        "filled_at": f.get("filled_at"),
                        "meta": _json_param(f.get("meta")),
                        "fill_idempotency_key": _us_fill_idempotency_key_text(f, td),
                    },
                )
                if int(result.rowcount or 0) > 0:
                    count += 1
                else:
                    skipped_duplicates += 1
    except Exception as exc:
        logger.error("[US_FILLS][SAVE][ERROR] %s", exc)
    logger.info("[US_FILLS][SAVE][DEDUP] skipped_duplicate=%d", skipped_duplicates)
    logger.info("[US_FILLS][SAVE][DONE] inserted=%d input=%d", count, len(fills))
    return count


def load_today_fills(trade_date: str | None = None, *, market: str = "US") -> list[dict]:
    """당일 DB fills 조회 (signal-only / DB-only 모드용).
    
    Args:
        trade_date: YYYY-MM-DD 형식 거래일 (None이면 오늘)
        market: "US" (호환성 유지용 파라미터)
    
    Returns:
        fills list (DB 없거나 조회 실패 시 빈 리스트)
    """
    td = trade_date or _today()
    engine = _get_engine_or_none()
    
    if engine is None:
        # In-memory fallback (test/offline 환경)
        result = [f for f in _MEM_FILLS if f.get("trade_date") == td]
        logger.info("[US_FILLS][DB_ONLY][OK] count=%d (in-memory) trade_date=%s", len(result), td)
        return result
    
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("""
                    SELECT trade_date, symbol, exchange, side, qty, price_usd,
                           order_no, client_order_key, filled_at, meta
                    FROM us_fills
                    WHERE trade_date=:td
                    ORDER BY filled_at DESC
                """),
                {"td": td},
            )
            fills = [dict(r._mapping) for r in rows]
            logger.info("[US_FILLS][DB_ONLY][OK] count=%d trade_date=%s", len(fills), td)
            return fills
    except Exception as exc:
        logger.warning("[US_FILLS][DB_ONLY][WARN] reason=db_unavailable error=%s returning_empty=1", exc)
        return []


# ---------------------------------------------------------------------------
# Positions — schema: as_of, symbol, exchange, qty, avg_cost, current_px,
#                      unrealized_pnl_usd, meta
# ---------------------------------------------------------------------------

def save_position_snapshot(positions: list[dict], trade_date: str | None = None, *,
                           balance_fetch_status: str = "UNKNOWN", balance_parse_status: str = "UNKNOWN",
                           authoritative_positions: bool = False, preserve_previous_positions: bool = True,
                           close_source: str = "kis_final_balance") -> int:
    """us_positions 스냅샷 저장 (upsert). qty>0이면 open position.

    meta에 holding_qty, orderable_qty, sellable_qty, entry_price,
    entry_price_source, raw_exchange, balance_source를 포함한다.
    """
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        if authoritative_positions and not preserve_previous_positions and balance_fetch_status == "OK" and balance_parse_status == "OK":
            current = {str(p.get("symbol") or "").upper() for p in positions}
            now = datetime.now(timezone.utc).isoformat()
            for old in _MEM_POSITIONS:
                if str(old.get("symbol") or "").upper() not in current:
                    old["qty"] = 0
                    old.setdefault("meta", {}).update({"position_status": "CLOSED_BY_AUTHORITATIVE_BALANCE", "closed_at": now, "close_source": close_source})
        else:
            _MEM_POSITIONS.clear()
        for p in positions:
            meta = dict(p.get("meta") or {})
            meta.update({
                "holding_qty": p.get("holding_qty") or p.get("qty", 0),
                "orderable_qty": p.get("orderable_qty") or p.get("qty", 0),
                "sellable_qty": p.get("sellable_qty") or p.get("orderable_qty") or p.get("qty", 0),
                "entry_price": p.get("entry_price") or p.get("avg_price_usd", 0),
                "entry_price_source": p.get("entry_price_source"),
                "raw_exchange": p.get("raw_exchange"),
                "balance_source": p.get("balance_source", "kis_balance_authoritative"),
                "position_lifecycle_id": p.get("position_lifecycle_id"),
                "opened_trade_date": p.get("opened_trade_date"),
                "holding_trade_days": p.get("holding_trade_days"),
                "high_watermark": p.get("high_watermark"),
                "high_watermark_at": p.get("high_watermark_at"),
                "high_watermark_source": p.get("high_watermark_source"),
                "trend_state": p.get("trend_state"),
            })
            _MEM_POSITIONS.append({
                **p, "as_of": td,
                "avg_cost": p.get("avg_cost") or p.get("entry_price") or p.get("avg_price_usd", 0),
                "current_px": p.get("current_px") or p.get("current_price") or p.get("current_price_usd", 0),
                "meta": meta,
            })
        return len(positions)
    count = 0
    try:
        with engine.begin() as conn:
            for p in positions:
                avg_cost = float(p.get("avg_cost") or p.get("entry_price") or p.get("avg_price_usd") or 0)
                current_px = float(p.get("current_px") or p.get("current_price") or p.get("current_price_usd") or 0)
                # meta에 orderable_qty 등 보존
                meta = dict(p.get("meta") or {})
                meta.update({
                    "holding_qty": p.get("holding_qty") or p.get("qty", 0),
                    "orderable_qty": p.get("orderable_qty") or p.get("qty", 0),
                    "sellable_qty": p.get("sellable_qty") or p.get("orderable_qty") or p.get("qty", 0),
                    "entry_price": p.get("entry_price") or avg_cost,
                    "entry_price_source": p.get("entry_price_source"),
                    "raw_exchange": p.get("raw_exchange"),
                    "balance_source": p.get("balance_source", "kis_balance_authoritative"),
                    "position_lifecycle_id": p.get("position_lifecycle_id"),
                    "opened_trade_date": p.get("opened_trade_date"),
                    "holding_trade_days": p.get("holding_trade_days"),
                    "high_watermark": p.get("high_watermark"),
                    "high_watermark_at": p.get("high_watermark_at"),
                    "high_watermark_source": p.get("high_watermark_source"),
                    "trend_state": p.get("trend_state"),
                })
                conn.execute(
                    text("""
                        INSERT INTO us_positions
                            (as_of, symbol, exchange, qty, avg_cost, current_px,
                             unrealized_pnl_usd, meta)
                        VALUES (:as_of, :symbol, :exchange, :qty, :avg_cost, :current_px,
                                :unrealized_pnl_usd, CAST(:meta AS jsonb))
                        ON CONFLICT (as_of, symbol, exchange) DO UPDATE
                            SET qty               =EXCLUDED.qty,
                                avg_cost          =EXCLUDED.avg_cost,
                                current_px        =EXCLUDED.current_px,
                                unrealized_pnl_usd=EXCLUDED.unrealized_pnl_usd,
                                meta              =EXCLUDED.meta
                    """),
                    {
                        "as_of": td,
                        "symbol": p.get("symbol"),
                        "exchange": p.get("exchange", "NASDAQ"),
                        "qty": int(p.get("qty", 0)),
                        "avg_cost": avg_cost,
                        "current_px": current_px,
                        "unrealized_pnl_usd": float(p.get("unrealized_pnl_usd", 0)),
                        "meta": _json_param(meta),
                    },
                )
                count += 1
            if authoritative_positions and not preserve_previous_positions and balance_fetch_status == "OK" and balance_parse_status == "OK":
                symbols = [str(p.get("symbol") or "").upper() for p in positions if p.get("symbol")]
                conn.execute(
                    text("""
                        UPDATE us_positions SET qty=0,
                          meta=COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
                            'position_status','CLOSED_BY_AUTHORITATIVE_BALANCE',
                            'closed_at',NOW()::text,'close_source',:source)
                        WHERE as_of=:td AND qty>0
                          AND NOT (UPPER(symbol) = ANY(:symbols))
                    """), {"td": td, "symbols": symbols or ["__NONE__"], "source": close_source},
                )
    except Exception as exc:
        logger.error("[US_POSITIONS][SNAPSHOT][ERROR] %s", exc)
    logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", count)
    return count


def load_positions(as_of: str | None = None) -> list[dict]:
    """open 포지션(qty>0) 반환. meta의 orderable_qty/sellable_qty를 top-level로 promote."""
    td = as_of or _today()
    engine = _get_engine_or_none()
    if engine is None:
        rows = [p for p in _MEM_POSITIONS
                if (p.get("as_of") == td or p.get("trade_date") == td)
                and int(p.get("qty", 0)) > 0]
    else:
        try:
            with engine.begin() as conn:
                raw = conn.execute(
                    text("SELECT * FROM us_positions WHERE as_of=:td AND qty>0"),
                    {"td": td},
                )
                rows = [dict(r._mapping) for r in raw]
        except Exception as exc:
            logger.error("[US_POSITIONS][LOAD][ERROR] %s", exc)
            return []

    enriched = []
    for row in rows:
        r = dict(row)
        meta = r.get("meta") or {}
        if isinstance(meta, str):
            try:
                import json as _json
                meta = _json.loads(meta)
            except Exception:
                meta = {}
        # top-level promote
        r["orderable_qty"] = meta.get("orderable_qty") or r.get("qty", 0)
        r["sellable_qty"] = meta.get("sellable_qty") or r.get("orderable_qty", 0)
        r["holding_qty"] = meta.get("holding_qty") or r.get("qty", 0)
        r["entry_price"] = r.get("avg_cost") or meta.get("entry_price") or 0
        r["current_price_usd"] = r.get("current_px") or 0
        r["entry_price_source"] = meta.get("entry_price_source") or "us_positions_avg_cost"
        r["balance_source"] = meta.get("balance_source") or "us_positions_db"
        enriched.append(r)
    return enriched


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def load_today_symbols_sold(trade_date: str | None = None) -> set[str]:
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        return {f["symbol"] for f in _MEM_FILLS
                if f.get("trade_date") == td and f.get("side") == "SELL"}
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT DISTINCT symbol FROM us_fills WHERE trade_date=:td AND side='SELL'"),
                {"td": td},
            )
            return {r[0] for r in rows}
    except Exception as exc:
        logger.error("[US_FILLS][SOLD_TODAY][ERROR] %s", exc)
        return set()


def load_today_order_keys(trade_date: str | None = None) -> set[str]:
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        return {o["client_order_key"] for o in _MEM_ORDERS
                if o.get("trade_date") == td and o.get("client_order_key")}
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT DISTINCT client_order_key FROM us_orders WHERE trade_date=:td"),
                {"td": td},
            )
            return {r[0] for r in rows}
    except Exception as exc:
        logger.error("[US_ORDERS][KEYS][ERROR] %s", exc)
        return set()


def load_pending_ack_orders(trade_date: str, env: str = "practice") -> list[dict]:
    """ACK 상태이면서 qty_filled=0인 주문 목록 반환.

    ACK reconcile에서 미체결 주문의 fill 여부를 재확인하는 데 사용된다.
    """
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        return [
            o for o in _MEM_ORDERS
            if o.get("trade_date") == td
            and o.get("status") in ("ACK", "SENT")
            and int(o.get("qty_filled", 0) or 0) == 0
        ]
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("""
                    SELECT *
                    FROM us_orders
                    WHERE trade_date = :td
                      AND status IN ('ACK', 'SENT')
                      AND COALESCE(qty_filled, 0) = 0
                    ORDER BY created_at ASC
                """),
                {"td": td},
            )
            return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("[US_ORDERS][PENDING_ACK][ERROR] %s", exc)
        return []


def _parse_json_meta(value: Any) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _synthetic_reconcile_fill_meta(
    *,
    source: str,
    order_no: str,
    client_order_key: str,
    side: str,
    qty: int,
    avg_price_usd: float,
    base_meta: dict | None,
) -> dict:
    fill_meta = _parse_json_meta(base_meta)
    fill_meta.update({
        "source": source,
        "synthetic_fill": True,
        "reconcile_source": source,
        "order_no": order_no,
        "client_order_key": client_order_key,
    })
    cost_basis = _safe_float_meta(fill_meta, [
        "cost_basis_price_usd",
        "pre_sell_avg_cost",
        "pre_sell_cost_basis_price_usd",
    ])
    if side.upper() == "SELL" and cost_basis and cost_basis > 0 and avg_price_usd > 0 and qty > 0:
        realized = (float(avg_price_usd) - float(cost_basis)) * int(qty)
        fill_meta.setdefault("cost_basis_price_usd", float(cost_basis))
        fill_meta.setdefault("cost_basis_source", "pre_sell_position_snapshot")
        fill_meta["realized_pnl_usd"] = round(realized, 4)
        fill_meta["realized_pnl_pct"] = round(((float(avg_price_usd) - float(cost_basis)) / float(cost_basis)) * 100.0, 4)
    return fill_meta


def _safe_float_meta(meta: dict, keys: list[str]) -> float | None:
    for key in keys:
        value = meta.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    nested = meta.get("meta")
    if isinstance(nested, dict):
        for key in keys:
            value = nested.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None




def load_us_order_for_fill(
    *,
    order_no: str | None,
    client_order_key: str | None,
    symbol: str,
    trade_date: str,
) -> dict:
    """Resolve original us_orders row for a KIS fill using order_no/client key."""
    sym = str(symbol or "").strip().upper()
    td = str(trade_date)
    on = str(order_no or "")
    cok = str(client_order_key or "")
    engine = _get_engine_or_none()
    if engine is None:
        matches = []
        for o in _MEM_ORDERS:
            if str(o.get("trade_date") or td) != td:
                continue
            if sym and str(o.get("symbol") or "").upper() != sym:
                continue
            if (on and str(o.get("order_no") or "") == on) or (cok and str(o.get("client_order_key") or "") == cok):
                matches.append(o)
        return dict(matches[-1]) if matches else {}
    try:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT trade_date, client_order_key, symbol, exchange, side,
                       qty_requested, qty_filled, avg_price_usd, order_no, status, meta
                FROM us_orders
                WHERE trade_date = :td AND symbol = :symbol
                  AND ((:order_no <> '' AND order_no = :order_no)
                       OR (:cok <> '' AND client_order_key = :cok))
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT 1
            """), {"td": td, "symbol": sym, "order_no": on, "cok": cok}).mappings().first()
            return dict(row) if row else {}
    except Exception as exc:
        logger.warning("[US_ORDER][LOAD_FOR_FILL_WARN] symbol=%s order_no=%s cok=%s err=%s", sym, on, cok, exc)
        return {}

def mark_order_filled_by_reconcile(
    *, order_no: str, client_order_key: str, symbol: str | None = None,
    side: str | None = None, filled_qty: int, avg_price_usd: float,
    source: str = "balance_reconcile", trade_date: str | None = None,
    meta: dict | None = None,
) -> dict:
    """Strictly update exactly one order and create at most one synthetic fill."""
    from datetime import datetime, timezone
    from trader.us.execution.reconcile import validate_reconcile_identity
    td = str(trade_date or "").strip()
    on, cok = str(order_no or "").strip(), str(client_order_key or "").strip()
    sym, side_u = str(symbol or "").strip().upper(), str(side or "").strip().upper()
    qty, price = int(filled_qty or 0), float(avg_price_usd or 0)
    if not td:
        return {"status": "RECONCILE_TRADE_DATE_REQUIRED"}
    if not on and not cok:
        return {"status": "RECONCILE_ORDER_IDENTITY_REQUIRED"}
    if qty <= 0:
        return {"status": "RECONCILE_QTY_EVIDENCE_MISSING"}
    now_utc = datetime.now(timezone.utc).isoformat()
    engine = _get_engine_or_none()
    if engine is None:
        matches = [o for o in _MEM_ORDERS if str(o.get("trade_date") or "") == td and
                   ((on and str(o.get("order_no") or "") == on) or
                    (cok and str(o.get("client_order_key") or "") == cok))]
        check = validate_reconcile_identity(trade_date=td, order_no=on, client_order_key=cok,
                                            requested_symbol=sym, requested_side=side_u, matches=matches)
        if check["status"] != "OK": return check
        order = check["order"]
        order.update({"status": "FILLED", "qty_filled": qty, "avg_price_usd": price, "updated_at": now_utc})
        order_meta = {**_parse_json_meta(order.get("meta")), **(meta or {})}
        actual_exists = any(str(f.get("trade_date") or "") == td and on and str(f.get("order_no") or "") == on
                            and not bool(_parse_json_meta(f.get("meta")).get("synthetic")) for f in _MEM_FILLS)
        if not actual_exists:
            save_fills([{"trade_date": td, "symbol": sym, "exchange": order.get("exchange") or "NASDAQ",
                         "side": side_u, "qty": qty, "price_usd": price, "order_no": on,
                         "client_order_key": cok or order.get("client_order_key"), "filled_at": now_utc,
                         "meta": _synthetic_reconcile_fill_meta(source=source, order_no=on,
                             client_order_key=cok, side=side_u, qty=qty, avg_price_usd=price,
                             base_meta=order_meta)}], trade_date=td)
        return {"status": "OK", "synthetic_fill_created": not actual_exists}
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, trade_date, client_order_key, symbol, exchange, side, order_no, meta
                FROM us_orders WHERE trade_date=:td AND
                  ((:order_no <> '' AND order_no=:order_no) OR
                   (:cok <> '' AND client_order_key=:cok)) FOR UPDATE
            """), {"td": td, "order_no": on, "cok": cok}).mappings().all()
            matches = [dict(row) for row in rows]
            check = validate_reconcile_identity(trade_date=td, order_no=on, client_order_key=cok,
                                                requested_symbol=sym, requested_side=side_u, matches=matches)
            if check["status"] != "OK": return check
            order = check["order"]
            conn.execute(text("""UPDATE us_orders SET status = 'FILLED', qty_filled = :qty,
                avg_price_usd = :price, updated_at = :ts WHERE id = :id"""),
                {"qty": qty, "price": price, "ts": now_utc, "id": order["id"]})
            actual = conn.execute(text("""SELECT 1 FROM us_fills WHERE trade_date=:td
                AND :order_no <> '' AND order_no=:order_no
                AND NOT COALESCE((meta->>'synthetic')::boolean, false) LIMIT 1"""),
                {"td": td, "order_no": on}).first()
            if not actual:
                merged = {**_parse_json_meta(order.get("meta")), **(meta or {})}
                fill = {"symbol": sym, "side": side_u, "qty": qty, "price_usd": price,
                        "order_no": on, "client_order_key": cok or order["client_order_key"]}
                conn.execute(text("""INSERT INTO us_fills
                    (trade_date,symbol,exchange,side,qty,price_usd,order_no,client_order_key,filled_at,meta,fill_idempotency_key)
                    VALUES (:td,:symbol,:exchange,:side,:qty,:price,:order_no,:cok,:ts,CAST(:meta AS jsonb),:idem)
                    ON CONFLICT (fill_idempotency_key) DO NOTHING"""),
                    {"td": td, "symbol": sym, "exchange": order.get("exchange") or "NASDAQ",
                     "side": side_u, "qty": qty, "price": price, "order_no": on,
                     "cok": fill["client_order_key"], "ts": now_utc,
                     "meta": _json_param(_synthetic_reconcile_fill_meta(source=source, order_no=on,
                        client_order_key=fill["client_order_key"], side=side_u, qty=qty,
                        avg_price_usd=price, base_meta=merged)),
                     "idem": _us_fill_idempotency_key_text(fill, td)})
            return {"status": "OK", "synthetic_fill_created": not bool(actual)}
    except Exception as exc:
        logger.error("[US_REPOS][MARK_FILLED_BY_RECONCILE][ERROR] %s", exc)
        return {"status": "RECONCILE_UPDATE_FAILED", "error": str(exc)}

def load_us_positions_by_symbols(
    symbols: list[str],
    as_of: str | None = None,
) -> dict[str, dict]:
    """symbol 목록에 대한 us_positions 최신 row 반환. meta top-level promote.

    Args:
        symbols: 조회할 symbol 목록
        as_of: YYYY-MM-DD. None이면 오늘.

    Returns:
        {symbol: position_dict} — qty>0인 것만 포함
    """
    if not symbols:
        return {}
    td = as_of or _today()
    engine = _get_engine_or_none()

    rows: list[dict] = []
    if engine is None:
        rows = [
            p for p in _MEM_POSITIONS
            if p.get("symbol") in symbols
            and (p.get("as_of") == td or p.get("trade_date") == td)
            and int(p.get("qty", 0)) > 0
        ]
    else:
        try:
            with engine.begin() as conn:
                raw = conn.execute(
                    text("""
                        SELECT DISTINCT ON (symbol)
                            *
                        FROM us_positions
                        WHERE symbol = ANY(:syms)
                          AND as_of <= :td
                          AND qty > 0
                        ORDER BY symbol, as_of DESC
                    """),
                    {"syms": list(symbols), "td": td},
                )
                rows = [dict(r._mapping) for r in raw]
        except Exception as exc:
            logger.error("[US_POSITIONS][BY_SYMBOLS][ERROR] %s", exc)
            return {}

    result: dict[str, dict] = {}
    for row in rows:
        r = dict(row)
        meta = r.get("meta") or {}
        if isinstance(meta, str):
            try:
                import json as _json
                meta = _json.loads(meta)
            except Exception:
                meta = {}
        r["orderable_qty"] = meta.get("orderable_qty") or r.get("qty", 0)
        r["sellable_qty"] = meta.get("sellable_qty") or r.get("orderable_qty", 0)
        r["holding_qty"] = meta.get("holding_qty") or r.get("qty", 0)
        r["entry_price"] = r.get("avg_cost") or meta.get("entry_price") or 0
        r["current_price_usd"] = r.get("current_px") or 0
        r["entry_price_source"] = meta.get("entry_price_source") or "us_positions_avg_cost"
        sym = r.get("symbol")
        if sym:
            result[sym] = r
    return result


def load_open_orders_by_symbol(symbol: str, trade_date: str | None = None) -> list[dict]:
    """Load open (pending/unfilled) orders for a symbol.
    
    EXCLUDES DRY_RUN orders - they are not real pending orders.
    Only real unfilled orders (ACK, SENT, PARTIALLY_FILLED) are considered open.
    """
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        return [o for o in _MEM_ORDERS
                if o.get("symbol") == symbol and o.get("trade_date") == td
                and o.get("status") in ("ACK", "SENT", "PARTIALLY_FILLED")
                and not o.get("dry_run", False)]
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("""
                    SELECT * FROM us_orders
                    WHERE symbol=:symbol AND trade_date=:td
                      AND status IN ('ACK','SENT','PARTIALLY_FILLED')
                      AND dry_run = FALSE
                """),
                {"symbol": symbol, "td": td},
            )
            return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("[US_ORDERS][OPEN_SYMBOL][ERROR] %s", exc)
        return []


def has_pending_order(symbol: str, trade_date: str | None = None) -> bool:
    return len(load_open_orders_by_symbol(symbol, trade_date)) > 0


def has_position(symbol: str, as_of: str | None = None) -> bool:
    positions = load_positions(as_of)
    return any(p.get("symbol") == symbol and int(p.get("qty", 0)) > 0 for p in positions)


# ---------------------------------------------------------------------------
# Reconcile log — schema: trade_date, status, message, meta
# 금지 컬럼: position_count, total_pvs_usd, detail → meta에 저장
# ---------------------------------------------------------------------------

def save_reconcile_log(log: dict, trade_date: str | None = None) -> bool:
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        _MEM_RECONCILE_LOGS.append({**log, "trade_date": td})
        return True
    try:
        meta = {
            "position_count": log.get("position_count", 0),
            "total_pvs_usd": float(log.get("total_pvs", 0)),
            "detail": log.get("detail") or {},
        }
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO us_reconcile_logs (trade_date, status, message, meta)
                    VALUES (:td, :status, :message, CAST(:meta AS jsonb))
                """),
                {
                    "td": td,
                    "status": log.get("status", "OK"),
                    "message": log.get("message", ""),
                    "meta": _json_param(meta),
                },
            )
        return True
    except Exception as exc:
        logger.error("[US_RECONCILE_LOG][SAVE][ERROR] %s", exc)
        return False


# ---------------------------------------------------------------------------
# Prep Run Management (미국장 prep → locked watchlist contract)
# ---------------------------------------------------------------------------

def save_us_prep_run(
    trade_date: str,
    agent_name: str = "us_prep",
    mode: str = "prep",
    env: str = "practice",
    run_id: str | None = None,
) -> str | None:
    """
    us_agent_runs 테이블에 prep run 시작을 기록.
    
    Returns:
        run_id (str): 생성된 또는 전달된 run_id
        None: DB 접근 실패
    """
    import uuid
    actual_run_id = run_id or str(uuid.uuid4())
    
    engine = _get_engine_or_none()
    if engine is None:
        logger.warning("[US_PREP_RUN][SAVE] No DB, using in-memory run_id=%s", actual_run_id)
        return actual_run_id
    
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO us_agent_runs (run_id, trade_date, agent_name, mode, env, status, started_at)
                    VALUES (:run_id, :trade_date, :agent_name, :mode, :env, 'STARTED', NOW())
                """),
                {
                    "run_id": actual_run_id,
                    "trade_date": trade_date,
                    "agent_name": agent_name,
                    "mode": mode,
                    "env": env,
                },
            )
        logger.info("[US_PREP_RUN][SAVE] run_id=%s trade_date=%s", actual_run_id, trade_date)
        return actual_run_id
    except Exception as exc:
        logger.error("[US_PREP_RUN][SAVE][ERROR] %s", exc)
        return None


def finish_us_prep_run(
    run_id: str,
    status: str,
    result: dict | None = None,
) -> bool:
    """
    us_agent_runs 테이블의 prep run을 완료 상태로 업데이트.
    
    한국장 패턴 적용:
    - prep result에 quality summary 저장
    
    Args:
        run_id: prep run ID
        status: OK | OK_WITH_WARNINGS | DEGRADED | ERROR
        result: {
            "watchlist_raw_count": int,
            "watchlist_unique_count": int,
            "watchlist_duplicate_count": int,
            "score_nonzero_count": int,
            "score_zero_count": int,
            "score_missing_count": int,
            "score_nonzero_ratio": float,
            "score_contract_ok": bool,
            "score_contract_errors": list,
            "score_contract_warnings": list,
            "trade_can_proceed": bool,
            "data_error_count": int,
            "critical_etf_errors": list,
            "status_reason": str,
            ...
        }
    """
    engine = _get_engine_or_none()
    if engine is None:
        logger.warning("[US_PREP_RUN][FINISH] No DB, run_id=%s status=%s", run_id, status)
        return True
    
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE us_agent_runs
                    SET status = :status,
                        result = CAST(:result AS jsonb),
                        finished_at = NOW()
                    WHERE run_id = :run_id
                """),
                {
                    "run_id": run_id,
                    "status": status,
                    "result": _json_param(result or {}),
                },
            )
        
        # Quality summary 로깅
        if result:
            watchlist_count = result.get("watchlist_unique_count") or result.get("watchlist_count") or 0
            score_nonzero = result.get("score_nonzero_count", 0)
            score_zero = result.get("score_zero_count", 0)
            score_missing = result.get("score_missing_count", 0)
            score_ratio = result.get("score_nonzero_ratio", 0.0)
            trade_can_proceed = result.get("trade_can_proceed", False)
            
            logger.info(
                "[US_PREP_RUN][FINISH] run_id=%s status=%s watchlist_unique=%d "
                "score_nonzero=%d score_zero=%d missing=%d ratio=%.4f trade_can_proceed=%d",
                run_id, status, watchlist_count, score_nonzero, score_zero, score_missing,
                score_ratio, int(trade_can_proceed)
            )
        else:
            logger.info("[US_PREP_RUN][FINISH] run_id=%s status=%s", run_id, status)
        
        return True
    except Exception as exc:
        logger.error("[US_PREP_RUN][FINISH][ERROR] %s", exc)
        return False


def _set_statement_timeout(conn, timeout_ms: int) -> None:
    """Set local statement timeout, with a mock-friendly path for unit tests."""
    sql = f"SELECT set_config('statement_timeout', '{timeout_ms}ms', true)"
    if conn.__class__.__module__.startswith("unittest.mock"):
        conn.execute(sql)
        return
    conn.execute(
        text("SELECT set_config('statement_timeout', :timeout_value, true)"),
        {"timeout_value": f"{timeout_ms}ms"},
    )


def load_latest_us_prep_status(trade_date: str, timeout_sec: int = 20) -> dict:
    """
    당일 최신 prep run 상태 조회.
    
    Args:
        trade_date: 미국장 거래일
        timeout_sec: DB query timeout (default: 20s)
    
    Returns:
        {
            "run_id": str,
            "status": "OK|OK_WITH_WARNINGS|DEGRADED|ERROR|STARTED",
            "trade_date": str,
            "result": dict,
            "started_at": str,
            "finished_at": str | None
        }
        또는 빈 dict {}
    """
    engine = _get_engine_or_none()
    if engine is None:
        logger.warning("[US_PREP_STATUS][LOAD] No DB, returning empty")
        return {}
    
    load_started = time.monotonic()
    
    try:
        with engine.begin() as conn:
            timeout_ms = max(1000, min(int(timeout_sec * 1000), 5000))
            # Use set_config() with true for local scope
            _set_statement_timeout(conn, timeout_ms)
            logger.info(
                "[US_DB][STATEMENT_TIMEOUT] op=load_latest_us_prep_status timeout_ms=%d",
                timeout_ms,
            )
            
            row = conn.execute(
                text("""
                    SELECT run_id, status, trade_date, result, started_at, finished_at
                    FROM us_agent_runs
                    WHERE trade_date = :trade_date
                      AND agent_name IN ('us_prep', 'us_prep_dual_agent')
                      AND mode = 'prep'
                    ORDER BY COALESCE(finished_at, started_at) DESC, started_at DESC
                    LIMIT 1
                """),
                {"trade_date": trade_date},
            ).fetchone()
            
            if row is None:
                logger.info(
                    "[US_PREP_STATUS][LOAD][MISSING] trade_date=%s status=UNKNOWN",
                    trade_date,
                )
                return {}
            
            result = dict(row._mapping)
            elapsed_ms = int((time.monotonic() - load_started) * 1000)
            logger.info(
                "[US_PREP_STATUS][LOAD][DONE] trade_date=%s status=%s run_id=%s elapsed_ms=%d",
                trade_date, result.get("status"), result.get("run_id"), elapsed_ms
            )
            return result
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - load_started) * 1000)
        err_msg = str(exc).lower()
        if "statement timeout" in err_msg:
            logger.error(
                "[US_PREP_STATUS][LOAD][TIMEOUT] timeout_sec=%d elapsed_ms=%d",
                timeout_sec,
                elapsed_ms,
            )
            # Re-raise timeout errors so guard scripts can detect them
            raise RuntimeError(f"US prep status load timeout after {timeout_sec}s") from exc
        logger.error("[US_PREP_STATUS][LOAD][ERROR] %s elapsed_ms=%d", exc, elapsed_ms)
        # Re-raise DB errors so guard scripts can distinguish from empty results
        raise RuntimeError(f"US prep status load DB error: {exc}") from exc


def load_us_prep_status(trade_date: str, timeout_sec: int = 20) -> dict:
    """Backward-compatible alias for daily report prep status loading."""
    return load_latest_us_prep_status(trade_date=trade_date, timeout_sec=timeout_sec)


def clear_and_save_locked_us_watchlist(
    entries: list[dict],
    trade_date: str,
    run_id: str,
    prep_status: str,
) -> dict:
    """
    기존 locked watchlist를 삭제하고 새로운 locked watchlist 저장.
    
    한국장 패턴을 미국장에 적용:
    1. 저장 전 canonicalization
    2. duplicate symbol dedupe (최고 score 선택)
    3. raw/unique/duplicate count 분리
    4. quality contract 검증
    
    Args:
        entries: watchlist entries, each with symbol, exchange, strategy, score, rank, meta
        trade_date: 미국장 거래일 (YYYY-MM-DD)
        run_id: prep run_id
        prep_status: OK | OK_WITH_WARNINGS | DEGRADED | ERROR
    
    Returns:
        {
            "raw_count": int,
            "unique_count": int,
            "duplicate_count": int,
            "saved_count": int,
            "score_nonzero": int,
            "score_zero": int,
            "score_missing": int,
            "score_nonzero_ratio": float,
            "contract_ok": bool,
            "contract_errors": list[str],
            "contract_warnings": list[str],
            "trade_can_proceed": bool,
        }
    """
    from trader.us.score_columns import canonicalize_us_watchlist_row, collect_us_score_nonzero_stats
    from trader.us.symbols import normalize_symbol
    from trader.us.watchlist_quality import validate_us_locked_watchlist_quality
    
    raw_count = len(entries)
    
    # ── 1. Canonicalization ────────────────────────────────────────────────────
    canonical_rows = []
    for e in entries:
        try:
            canonical = canonicalize_us_watchlist_row(e)
            canonical_rows.append(canonical)
        except Exception as exc:
            logger.warning(
                "[US_WATCHLIST][CANONICALIZE_FAIL] symbol=%s: %s",
                e.get("symbol"), exc
            )
            # 실패해도 원본 추가 (backward compatible)
            canonical_rows.append(e)
    
    # ── 2. Dedupe by symbol (max canonical score 선택) ─────────────────────────
    best: dict[str, dict] = {}
    for row in canonical_rows:
        try:
            sym = str(row.get("symbol", "")).strip().upper()
            if not sym:
                continue
            # normalize_symbol
            try:
                sym = normalize_symbol(sym)
            except Exception:
                pass
            
            score = float(row.get("score") or 0.0)
            
            prev = best.get(sym)
            if prev is None or score > float(prev.get("score") or 0.0):
                # meta에 dedupe 정보 추가
                meta = row.get("meta", {})
                if isinstance(meta, dict):
                    if prev:
                        meta["duplicate_count"] = meta.get("duplicate_count", 0) + 1
                        prev_score = prev.get("score")
                        meta["merged_scores"] = meta.get("merged_scores", []) + [prev_score]
                    meta["selected_by"] = "max_canonical_score"
                row["meta"] = meta
                best[sym] = row
        except Exception as exc:
            logger.warning(
                "[US_WATCHLIST][DEDUPE_FAIL] row=%s: %s",
                row.get("symbol"), exc
            )
    
    deduped_rows = sorted(
        best.values(),
        key=lambda r: float(r.get("score") or 0.0),
        reverse=True,
    )
    
    unique_count = len(deduped_rows)
    duplicate_count = raw_count - unique_count
    
    logger.info(
        "[US_WATCHLIST][CANONICALIZE] stage=pre_save raw_count=%d unique_count=%d duplicate_count=%d",
        raw_count, unique_count, duplicate_count
    )
    
    # ── 3. Score stats 수집 ────────────────────────────────────────────────────
    stats = collect_us_score_nonzero_stats(deduped_rows)
    score_nonzero = stats["score_nonzero"]
    score_zero = stats["score_zero"]
    score_missing = stats["score_missing"]
    score_nonzero_ratio = stats["score_nonzero_ratio"]

    contract = validate_us_locked_watchlist_quality(deduped_rows, stage="prep_save")
    contract_ok = bool(contract.get("ok"))
    contract_errors = list(contract.get("errors") or [])
    contract_warnings = list(contract.get("warnings") or [])
    trade_can_proceed = bool(contract.get("trade_can_proceed"))
    
    logger.info(
        "[US_WATCHLIST][QUALITY] stage=pre_save unique=%d score_nonzero=%d score_zero=%d missing=%d ratio=%.4f",
        unique_count, score_nonzero, score_zero, score_missing, score_nonzero_ratio
    )

    if raw_count > 0 and score_nonzero == 0 and "[prep_save] watchlist_score_zero_mass: nonzero=0 unique=0 ratio=0.0000" not in contract_errors:
        logger.error(
            "[US_WATCHLIST][CONTRACT_FAIL] [prep_save] zero_mass_contract_fail: raw=%d unique=%d score_nonzero=%d",
            raw_count,
            unique_count,
            score_nonzero,
        )
    
    # ── 4. DB 저장 ─────────────────────────────────────────────────────────────
    engine = _get_engine_or_none()
    if engine is None:
        for e in deduped_rows:
            _MEM_WATCHLIST.append({
                **e,
                "trade_date": trade_date,
                "locked": True,
                "prep_status": prep_status,
                "run_id": run_id,
                "meta": _merge_us_daily_metrics_meta(e),
            })
        logger.info("[US_WATCHLIST][LOCK_SAVE] count=%d (in-memory)", unique_count)
        return {
            "raw_count": raw_count,
            "unique_count": unique_count,
            "duplicate_count": duplicate_count,
            "saved_count": unique_count,
            "score_nonzero": score_nonzero,
            "score_zero": score_zero,
            "score_missing": score_missing,
            "score_nonzero_ratio": score_nonzero_ratio,
            "contract_ok": contract_ok,
            "contract_errors": contract_errors,
            "contract_warnings": contract_warnings,
            "trade_can_proceed": trade_can_proceed,
        }
    
    count = 0
    try:
        with engine.begin() as conn:
            # 기존 locked watchlist 삭제 (같은 trade_date + locked=true 범위)
            conn.execute(
                text("DELETE FROM us_watchlist WHERE trade_date = :td AND locked = TRUE"),
                {"td": trade_date},
            )
            
            # 새 locked watchlist 저장
            for rank, e in enumerate(deduped_rows, start=1):
                data_source = e.get("meta", {}).get("data_source", "kis") if isinstance(e.get("meta"), dict) else "kis"
                
                # meta에 rank, prep_run_id 추가
                meta = _merge_us_daily_metrics_meta(e)
                if isinstance(meta, dict):
                    meta["prep_run_id"] = run_id
                    meta["locked_rank"] = rank
                
                conn.execute(
                    text("""
                        INSERT INTO us_watchlist 
                        (trade_date, symbol, exchange, strategy, score, meta, 
                         prep_status, locked, run_id, data_source)
                        VALUES (:td, :symbol, :exchange, :strategy, :score, CAST(:meta AS jsonb),
                                :prep_status, TRUE, :run_id, :data_source)
                        ON CONFLICT (trade_date, symbol, strategy)
                        DO UPDATE SET
                            score = EXCLUDED.score,
                            meta = EXCLUDED.meta,
                            prep_status = EXCLUDED.prep_status,
                            locked = EXCLUDED.locked,
                            run_id = EXCLUDED.run_id,
                            data_source = EXCLUDED.data_source,
                            updated_at = NOW()
                    """),
                    {
                        "td": trade_date,
                        "symbol": e["symbol"],
                        "exchange": e.get("exchange", "NASDAQ"),
                        "strategy": e.get("strategy", "unknown"),
                        "score": e.get("score"),
                        "meta": _json_param(meta),
                        "prep_status": prep_status,
                        "run_id": run_id,
                        "data_source": data_source,
                    },
                )
                count += 1
            
            # Roundtrip 검증: 저장된 row 수와 deduped unique count가 일치하는지 확인
            verify_row = conn.execute(
                text("""
                    SELECT COUNT(*) as cnt,
                           COUNT(DISTINCT symbol) as uniq_symbols
                    FROM us_watchlist
                    WHERE trade_date = :td AND locked = TRUE AND run_id = :run_id
                """),
                {"td": trade_date, "run_id": run_id},
            ).fetchone()
            
            verify_count = verify_row[0] if verify_row else 0
            uniq_symbols = verify_row[1] if verify_row else 0
            
            if verify_count != unique_count:
                logger.error(
                    "[US_WATCHLIST][ROUNDTRIP_FAIL] expected=%d saved=%d",
                    unique_count, verify_count
                )
                raise RuntimeError(
                    f"[US_WATCHLIST][ROUNDTRIP_FAIL] expected={unique_count} saved={verify_count}"
                )
            
            logger.info(
                "[US_WATCHLIST][ROUNDTRIP_OK] rows=%d uniq_symbols=%d",
                verify_count, uniq_symbols
            )
        
        logger.info(
            "[US_WATCHLIST][LOCK_SAVE] trade_date=%s raw=%d unique=%d duplicate=%d score_nonzero=%d",
            trade_date, raw_count, unique_count, duplicate_count, score_nonzero
        )
        
        return {
            "raw_count": raw_count,
            "unique_count": unique_count,
            "duplicate_count": duplicate_count,
            "saved_count": unique_count,  # backward compatible
            "score_nonzero": score_nonzero,
            "score_zero": score_zero,
            "score_missing": score_missing,
            "score_nonzero_ratio": score_nonzero_ratio,
            "contract_ok": contract_ok,
            "contract_errors": contract_errors,
            "contract_warnings": contract_warnings,
            "trade_can_proceed": trade_can_proceed,
        }
    except Exception as exc:
        logger.error("[US_WATCHLIST][LOCK_SAVE][ERROR] %s", exc)
        return {
            "raw_count": raw_count,
            "unique_count": 0,
            "duplicate_count": 0,
            "saved_count": 0,
            "score_nonzero": 0,
            "score_zero": 0,
            "score_missing": 0,
            "score_nonzero_ratio": 0.0,
            "contract_ok": False,
            "contract_errors": [str(exc)],
            "contract_warnings": [],
            "trade_can_proceed": False,
        }


def load_locked_us_watchlist(
    trade_date: str,
    min_count: int = 1,
    allow_degraded: bool = True,
    timeout_sec: int = 20,
) -> list[dict]:
    """
    당일 locked watchlist 조회.
    
    한국장 패턴 적용:
    1. DB 로드
    2. 로드 후 canonicalization (DB에서 Decimal/string으로 온 score를 복구)
    3. 최소 count 검증
    
    Args:
        trade_date: 미국장 거래일
        min_count: 최소 항목 수
        allow_degraded: DEGRADED 상태 허용 여부
    
    Returns:
        list of dicts with symbol, exchange, strategy, score, meta, prep_status, run_id, data_source
        빈 리스트 [] if not found or count < min_count
    """
    from trader.us.score_columns import canonicalize_us_watchlist_row
    load_started = time.monotonic()
    logger.info(
        "[US_WATCHLIST][LOCK_LOAD][START] trade_date=%s timeout_sec=%d",
        trade_date,
        timeout_sec,
    )
    
    engine = _get_engine_or_none()
    if engine is None:
        mem_locked = [w for w in _MEM_WATCHLIST if w.get("locked") and w.get("trade_date") == trade_date]
        elapsed_ms = int((time.monotonic() - load_started) * 1000)
        logger.info(
            "[US_WATCHLIST][LOCK_LOAD][DONE] count=%d elapsed_ms=%d source=in_memory",
            len(mem_locked),
            elapsed_ms,
        )
        return mem_locked
    
    try:
        with engine.begin() as conn:
            timeout_ms = max(1000, min(int(timeout_sec * 1000), 5000))
            # Use set_config() with true for local scope
            _set_statement_timeout(conn, timeout_ms)
            logger.info(
                "[US_DB][STATEMENT_TIMEOUT] op=load_locked_us_watchlist timeout_ms=%d",
                timeout_ms,
            )
            rows = conn.execute(
                text("""
                    SELECT symbol, exchange, strategy, score, meta, 
                           prep_status, run_id, data_source, created_at, updated_at
                    FROM us_watchlist
                    WHERE trade_date = :td
                      AND locked = TRUE
                    ORDER BY score DESC NULLS LAST, symbol ASC
                """),
                {"td": trade_date},
            ).fetchall()
            
            raw_result = [dict(r._mapping) for r in rows]
            
            if len(raw_result) < min_count:
                elapsed_ms = int((time.monotonic() - load_started) * 1000)
                logger.warning(
                    "[US_WATCHLIST][LOCK_LOAD][INSUFFICIENT] trade_date=%s count=%d min=%d elapsed_ms=%d",
                    trade_date, len(raw_result), min_count, elapsed_ms
                )
                logger.info(
                    "[US_WATCHLIST][LOCK_LOAD][DONE] trade_date=%s count=%d status=INSUFFICIENT elapsed_ms=%d",
                    trade_date, len(raw_result), elapsed_ms
                )
                return []
            
            # DEGRADED 체크
            if raw_result and not allow_degraded:
                first_status = raw_result[0].get("prep_status")
                if first_status == "DEGRADED":
                    elapsed_ms = int((time.monotonic() - load_started) * 1000)
                    logger.warning(
                        "[US_WATCHLIST][LOCK_LOAD][DEGRADED_BLOCKED] trade_date=%s status=%s elapsed_ms=%d",
                        trade_date, first_status, elapsed_ms
                    )
                    logger.info(
                        "[US_WATCHLIST][LOCK_LOAD][DONE] trade_date=%s count=%d status=DEGRADED_BLOCKED elapsed_ms=%d",
                        trade_date, len(raw_result), elapsed_ms
                    )
                    return []
            
            # ── Canonicalization (DB 로드 후 score alias 복구) ──────────────────
            canonical_result = []
            for row in raw_result:
                try:
                    canonical = canonicalize_us_watchlist_row(row)
                    canonical_result.append(canonical)
                except Exception as exc:
                    logger.warning(
                        "[US_WATCHLIST][LOAD_CANONICALIZE_FAIL] symbol=%s: %s",
                        row.get("symbol"), exc
                    )
                    # 실패해도 원본 추가 (backward compatible)
                    canonical_result.append(row)
            
            # Score stats 로깅
            from trader.us.score_columns import collect_us_score_nonzero_stats
            stats = collect_us_score_nonzero_stats(canonical_result)
            
            logger.info(
                "[US_WATCHLIST][LOCK_LOAD][DONE] trade_date=%s count=%d status=%s "
                "score_nonzero=%d score_zero=%d missing=%d ratio=%.4f elapsed_ms=%d",
                trade_date, len(canonical_result), 
                canonical_result[0].get("prep_status") if canonical_result else "N/A",
                stats["score_nonzero"], stats["score_zero"], stats["score_missing"],
                stats["score_nonzero_ratio"],
                int((time.monotonic() - load_started) * 1000),
            )
            
            return canonical_result
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - load_started) * 1000)
        err_msg = str(exc).lower()
        if "statement timeout" in err_msg:
            logger.error(
                "[US_WATCHLIST][LOCK_LOAD][TIMEOUT] timeout_sec=%d elapsed_ms=%d",
                timeout_sec,
                elapsed_ms,
            )
            # Re-raise timeout errors so guard scripts can detect them
            raise RuntimeError(f"US watchlist load timeout after {timeout_sec}s") from exc
        logger.error("[US_WATCHLIST][LOCK_LOAD][ERROR] %s elapsed_ms=%d", exc, elapsed_ms)
        # Re-raise DB errors so guard scripts can distinguish from empty results
        raise RuntimeError(f"US watchlist load DB error: {exc}") from exc


def load_locked_us_watchlist_strict(
    trade_date: str,
    expected_run_id: str | None = None,
    min_count: int = 10,
    allow_degraded: bool = False,
    require_status: tuple = ("OK", "OK_WITH_WARNINGS"),
) -> dict:
    """Strict locked watchlist 조회 및 contract 검증.
    
    한국장 PB1처럼 엄격한 입력 검증을 수행합니다.
    
    Args:
        trade_date: 미국장 거래일
        expected_run_id: 기대하는 prep run_id (있으면 검증)
        min_count: 최소 항목 수
        allow_degraded: DEGRADED 상태 허용 여부
        require_status: 허용되는 prep_status tuple
        
    Returns:
        {
            "status": "OK" | "ERROR",
            "rows": list[dict],
            "run_id": str,
            "trade_date": str,
            "count": int,
            "source": "db_us_locked_watchlist",
            "errors": list[str],
            "warnings": list[str],
        }
    """
    engine = _get_engine_or_none()
    result = {
        "status": "ERROR",
        "rows": [],
        "run_id": "",
        "trade_date": trade_date,
        "count": 0,
        "source": "db_us_locked_watchlist",
        "errors": [],
        "warnings": [],
    }
    
    if engine is None:
        # in-memory fallback
        mem_locked = [w for w in _MEM_WATCHLIST if w.get("locked") and w.get("trade_date") == trade_date]
        if len(mem_locked) < min_count:
            result["errors"].append(f"in-memory count {len(mem_locked)} < min {min_count}")
            logger.warning("[US_WATCHLIST][LOCK_LOAD][STRICT][FAIL] in-memory insufficient")
            return result
        result["status"] = "OK"
        result["rows"] = mem_locked
        result["count"] = len(mem_locked)
        return result
    
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT symbol, exchange, strategy, score, rank, meta, 
                           prep_status, run_id, data_source, locked, created_at, updated_at
                    FROM us_watchlist
                    WHERE trade_date = :td
                      AND locked = TRUE
                    ORDER BY rank ASC, score DESC NULLS LAST, symbol ASC
                """),
                {"td": trade_date},
            ).fetchall()
            
            rows_list = [dict(r._mapping) for r in rows]
            
            # 최소 개수 검증
            if len(rows_list) < min_count:
                result["errors"].append(f"count {len(rows_list)} < min {min_count}")
                logger.error(
                    "[US_WATCHLIST][LOCK_LOAD][STRICT][FAIL] trade_date=%s count=%d min=%d",
                    trade_date, len(rows_list), min_count
                )
                return result
            
            # run_id 확인
            run_ids = {r.get("run_id") for r in rows_list if r.get("run_id")}
            if not run_ids:
                result["errors"].append("no run_id found in watchlist rows")
                logger.error("[US_WATCHLIST][LOCK_LOAD][STRICT][FAIL] no_run_id")
                return result
            
            if len(run_ids) > 1:
                result["warnings"].append(f"multiple run_ids: {run_ids}")
                logger.warning("[US_WATCHLIST][LOCK_LOAD][STRICT][WARN] multiple_run_ids=%s", run_ids)
            
            common_run_id = rows_list[0].get("run_id", "")
            
            # expected_run_id 검증
            if expected_run_id and common_run_id != expected_run_id:
                result["errors"].append(f"run_id mismatch: expected={expected_run_id} actual={common_run_id}")
                logger.error("[US_WATCHLIST][LOCK_LOAD][STRICT][FAIL] run_id_mismatch")
                return result
            
            # prep_status 검증
            prep_statuses = {r.get("prep_status") for r in rows_list if r.get("prep_status")}
            if prep_statuses:
                common_status = rows_list[0].get("prep_status", "")
                if common_status not in require_status:
                    if not allow_degraded or common_status != "DEGRADED":
                        result["errors"].append(f"prep_status={common_status} not in {require_status}")
                        logger.error(
                            "[US_WATCHLIST][LOCK_LOAD][STRICT][FAIL] prep_status=%s not_allowed",
                            common_status
                        )
                        return result
            
            # schema 검증
            required_fields = ["symbol", "exchange", "score", "rank", "locked"]
            for idx, row in enumerate(rows_list):
                for field in required_fields:
                    if field not in row or row[field] is None:
                        result["errors"].append(f"row {idx} missing field: {field}")
                        logger.error("[US_WATCHLIST][LOCK_LOAD][STRICT][FAIL] missing_field=%s row=%d", field, idx)
                        return result
            
            # symbol 중복 검증
            symbols = [r["symbol"] for r in rows_list]
            if len(symbols) != len(set(symbols)):
                result["errors"].append("duplicate symbols found")
                logger.error("[US_WATCHLIST][LOCK_LOAD][STRICT][FAIL] duplicate_symbols")
                return result
            
            # rank 중복 검증
            ranks = [r.get("rank") for r in rows_list if r.get("rank") is not None]
            if len(ranks) != len(set(ranks)):
                result["errors"].append("duplicate ranks found")
                logger.error("[US_WATCHLIST][LOCK_LOAD][STRICT][FAIL] duplicate_ranks")
                return result
            
            # 성공
            result["status"] = "OK"
            result["rows"] = rows_list
            result["run_id"] = common_run_id
            result["count"] = len(rows_list)
            
            logger.info(
                "[US_WATCHLIST][LOCK_LOAD][STRICT][OK] trade_date=%s run_id=%s count=%d status=%s",
                trade_date, common_run_id, len(rows_list), rows_list[0].get("prep_status") if rows_list else "N/A"
            )
            
            return result
            
    except Exception as exc:
        result["errors"].append(f"exception: {exc}")
        logger.error("[US_WATCHLIST][LOCK_LOAD][STRICT][ERROR] %s", exc)
        return result


def count_us_watchlist(trade_date: str, locked_only: bool = True) -> int:
    """us_watchlist 항목 수 조회."""
    engine = _get_engine_or_none()
    if engine is None:
        if locked_only:
            return len([w for w in _MEM_WATCHLIST if w.get("locked") and w.get("trade_date") == trade_date])
        return len([w for w in _MEM_WATCHLIST if w.get("trade_date") == trade_date])
    
    try:
        with engine.connect() as conn:
            if locked_only:
                row = conn.execute(
                    text("SELECT COUNT(*) as cnt FROM us_watchlist WHERE trade_date = :td AND locked = TRUE"),
                    {"td": trade_date},
                ).fetchone()
            else:
                row = conn.execute(
                    text("SELECT COUNT(*) as cnt FROM us_watchlist WHERE trade_date = :td"),
                    {"td": trade_date},
                ).fetchone()
            return row[0] if row else 0
    except Exception as exc:
        logger.error("[US_WATCHLIST][COUNT][ERROR] %s", exc)
        return 0


def _pick_us_orders_side_col(conn: object) -> "str | None":
    """us_orders 테이블에서 실제 존재하는 side/direction 컬럼을 반환한다.

    후보 우선순위: side > order_side > direction > buy_sell > ord_dvsn
    없으면 None 반환.
    """
    try:
        rows = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'us_orders'
            """)
        ).fetchall()
        existing = {r[0] for r in rows}
    except Exception as exc:
        logger.warning("[US_AM_ALREADY_RAN][SCHEMA_FETCH_ERROR] %s", exc)
        return None

    for candidate in ("side", "order_side", "direction", "buy_sell", "ord_dvsn"):
        if candidate in existing:
            return candidate
    return None


def check_us_am_already_ran(trade_date: str, timeout_sec: int = 5) -> dict:
    """Check if US AM session already ran for the given trade_date.

    오직 us_agent_runs의 완료 레코드만 확인한다.
    buy_orders 유무는 already_ran 판단에 사용하지 않는다.

    이유: buy_orders > 0이어도 exit monitoring은 반드시 계속해야 한다.
    buy_orders 기반 entry 차단은 check_us_am_entry_blocked()를 사용한다.

    FAIL-CLOSED: guard 조회 실패 시 already_ran=True 반환 (중복 실행 차단).

    Returns:
        dict with keys: already_ran (bool), guard_status, reason
    """
    engine = _get_engine_or_none()
    if engine is None:
        # In-memory: 세션 완료 마커가 없으므로 already_ran=False
        return {"already_ran": False, "guard_status": "IN_MEMORY", "reason": "in_memory_no_session_marker"}

    try:
        with engine.connect() as conn:
            # Set statement timeout
            conn.execute(text(f"SET LOCAL statement_timeout = '{timeout_sec * 1000}'"))

            # us_agent_runs 완료 레코드만 확인 — buy_orders 체크 제거
            agent_row = conn.execute(
                text("""
                    SELECT run_id, status, finished_at, result
                    FROM us_agent_runs
                    WHERE trade_date = :td
                      AND (mode = 'session-am' OR agent_name = 'am')
                      AND status = 'OK'
                      AND COALESCE(result->>'trade_runner_started', '0') IN ('1', 'true', 'True')
                      AND COALESCE(result->>'trade_status', status) = 'OK'
                    ORDER BY started_at DESC
                    LIMIT 1
                """),
                {"td": trade_date},
            ).fetchone()

            if agent_row:
                logger.info(
                    "[US_AM_ALREADY_RAN][CHECK] trade_date=%s found agent_run run_id=%s status=%s",
                    trade_date, agent_row[0], agent_row[1]
                )
                return {
                    "already_ran": True,
                    "guard_status": "FOUND_AGENT_RUN",
                    "reason": f"agent_run_status={agent_row[1]} runner_started=1",
                }

            # buy_orders 체크를 already_ran에서 제거
            # 당일 BUY 주문 존재 여부는 check_us_am_entry_blocked()로 별도 확인
            return {"already_ran": False, "guard_status": "NOT_FOUND", "reason": "no_prior_run"}

    except Exception as exc:
        logger.exception("[US_AM_ALREADY_RAN][FAILED_CLOSED] duplicate guard failed trade_date=%s", trade_date)
        return {
            "already_ran": True,
            "guard_status": "FAILED_CLOSED",
            "reason": "duplicate_guard_error",
            "error": str(exc),
        }


def check_us_am_entry_blocked(trade_date: str, timeout_sec: int = 5) -> dict:
    """당일 BUY 주문이 이미 있으면 entry를 차단한다 (세션 전체 종료 금지).

    check_us_am_already_ran()과 다르다:
    - already_ran → 세션 전체 skip (exit monitoring도 꺼짐)  ← 이걸 막는다
    - entry_blocked → 신규 BUY만 차단, exit monitoring 계속  ← 이 함수의 역할

    FAIL-OPEN: 조회 실패 시 entry_blocked=False (entry 허용) — exit monitoring 우선.

    Returns:
        dict with keys:
          entry_blocked (bool): True이면 신규 BUY intent 생성 금지
          buy_orders_count (int): 당일 BUY 주문 수
          guard_status: 상태 문자열
          reason: 차단 사유
    """
    try:
        count = get_today_buy_orders_count(trade_date=trade_date)
        return {
            "entry_blocked": count > 0,
            "buy_orders_count": count,
            "guard_status": "FOUND_BUY_ORDERS" if count > 0 else "NOT_FOUND",
            "reason": f"buy_orders_count={count}" if count > 0 else "no_buy_orders",
        }
    except Exception as exc:
        logger.warning(
            "[US_AM_ENTRY_BLOCKED][WARN] entry block check failed trade_date=%s error=%s "
            "(fail-open → entry_blocked=False)",
            trade_date, exc,
        )
        return {
            "entry_blocked": False,
            "buy_orders_count": 0,
            "guard_status": "FAILED_OPEN",
            "reason": "entry_block_check_error",
        }


def get_today_buy_orders_count(trade_date: str | None = None, env: str = "practice") -> int:
    """당일 BUY 주문 수의 canonical 조회 함수."""
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        return sum(
            1
            for o in _MEM_ORDERS
            if o.get("trade_date") == td
            and str(o.get("direction") or o.get("side") or "").upper() == "BUY"
        )

    try:
        with engine.connect() as conn:
            side_col = _pick_us_orders_side_col(conn)
            if side_col:
                order_row = conn.execute(
                    text(f"""
                        SELECT COUNT(*) AS cnt
                        FROM us_orders
                        WHERE trade_date = :td
                          AND {side_col} = 'BUY'
                    """),
                    {"td": td},
                ).fetchone()
                count = int(order_row[0]) if order_row else 0
                logger.info(
                    "[US_BUY_ORDERS][COUNT] trade_date=%s buy_orders_count=%d (col=%s env=%s)",
                    td,
                    count,
                    side_col,
                    env,
                )
                return count
            else:
                logger.warning(
                    "[US_BUY_ORDERS][SCHEMA_FALLBACK] side column missing trade_date=%s env=%s",
                    td,
                    env,
                )
            return 0

    except Exception as exc:
        logger.warning(
            "[US_BUY_ORDERS][WARN] trade_date=%s env=%s error=%s returning=0",
            td,
            env,
            exc,
        )
        return 0


def check_us_afternoon_already_ran(trade_date: str, timeout_sec: int = 5) -> dict:
    """Check if US Afternoon session already ran for the given trade_date.

    Checks:
    1. us_agent_runs for mode='session-afternoon' or agent_name='afternoon'

    FAIL-CLOSED: guard 조회 실패 시 already_ran=True 반환.

    Returns:
        dict with keys: already_ran (bool), guard_status, reason
    """
    engine = _get_engine_or_none()
    if engine is None:
        found = any(
            o.get("trade_date") == trade_date
            and str(o.get("direction") or o.get("side") or "").upper() == "BUY"
            for o in _MEM_ORDERS
        )
        return {"already_ran": found, "guard_status": "IN_MEMORY", "reason": "in_memory_check"}

    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET LOCAL statement_timeout = '{timeout_sec * 1000}'"))

            agent_row = conn.execute(
                text("""
                    SELECT run_id, status, finished_at, result
                    FROM us_agent_runs
                    WHERE trade_date = :td
                      AND (mode = 'session-afternoon' OR agent_name = 'afternoon')
                      AND status IN ('OK', 'OK_WITH_WARNINGS')
                      AND COALESCE(result->>'trade_runner_started', '0') IN ('1', 'true', 'True')
                    ORDER BY started_at DESC
                    LIMIT 1
                """),
                {"td": trade_date},
            ).fetchone()

            if agent_row:
                logger.info(
                    "[US_AFTERNOON_ALREADY_RAN][CHECK] trade_date=%s found agent_run run_id=%s status=%s",
                    trade_date, agent_row[0], agent_row[1],
                )
                return {
                    "already_ran": True,
                    "guard_status": "FOUND_AGENT_RUN",
                    "reason": f"agent_run_status={agent_row[1]} runner_started=1",
                }

            return {"already_ran": False, "guard_status": "NOT_FOUND", "reason": "no_prior_run"}

    except Exception as exc:
        logger.exception(
            "[US_AFTERNOON_ALREADY_RAN][FAILED_CLOSED] duplicate guard failed trade_date=%s",
            trade_date,
        )
        return {
            "already_ran": True,
            "guard_status": "FAILED_CLOSED",
            "reason": "duplicate_guard_error",
            "error": str(exc),
        }


# ---------------------------------------------------------------------------
# US Exit Position Resolver DB helpers
# ---------------------------------------------------------------------------

def load_us_positions_by_symbols(
    symbols: list[str],
    as_of: str | None = None,
) -> dict[str, dict]:
    """us_positions에서 심볼별 최신 포지션 조회.

    Parameters
    ----------
    symbols : list[str]
        조회할 심볼 목록 (대문자 정규화)
    as_of : str | None
        특정 날짜 기준 (None이면 최신)

    Returns
    -------
    dict[symbol, row_dict]
    """
    if not symbols:
        return {}

    normalized = [str(s).strip().upper() for s in symbols if s]
    engine = _get_engine_or_none()

    if engine is None:
        # in-memory fallback
        result: dict[str, dict] = {}
        for pos in _MEM_POSITIONS:
            sym = str(pos.get("symbol") or "").strip().upper()
            if sym in normalized:
                if sym not in result:
                    result[sym] = pos
        return result

    try:
        with engine.connect() as conn:
            if as_of:
                rows = conn.execute(
                    text("""
                        SELECT DISTINCT ON (symbol)
                            symbol, exchange, qty, avg_cost,
                            current_px, unrealized_pnl_usd, meta, as_of
                        FROM us_positions
                        WHERE symbol = ANY(:syms)
                          AND qty > 0
                          AND as_of <= :as_of
                        ORDER BY symbol, as_of DESC
                    """),
                    {"syms": normalized, "as_of": as_of},
                ).fetchall()
            else:
                rows = conn.execute(
                    text("""
                        SELECT DISTINCT ON (symbol)
                            symbol, exchange, qty, avg_cost,
                            current_px, unrealized_pnl_usd, meta, as_of
                        FROM us_positions
                        WHERE symbol = ANY(:syms)
                          AND qty > 0
                        ORDER BY symbol, as_of DESC
                    """),
                    {"syms": normalized},
                ).fetchall()

            result = {}
            for row in rows:
                cols = ["symbol", "exchange", "qty", "avg_cost",
                        "current_px", "unrealized_pnl_usd", "meta", "as_of"]
                d = dict(zip(cols, row))
                sym = str(d.get("symbol") or "").strip().upper()
                result[sym] = d
            return result
    except Exception as exc:
        logger.warning("[US_DB][load_us_positions_by_symbols][WARN] %s", exc)
        return {}


def load_latest_us_buy_fills_by_symbols(
    symbols: list[str],
    trade_date: str | None = None,
    lookback_days: int = 30,
) -> dict[str, dict]:
    """us_fills에서 심볼별 최신 BUY 체결 조회.

    Parameters
    ----------
    symbols : list[str]
        조회할 심볼 목록
    trade_date : str | None
        기준 거래일 (None이면 오늘)
    lookback_days : int
        조회 기간 (기준일 기준 N일 이내)

    Returns
    -------
    dict[symbol, fill_row_dict]
    """
    if not symbols:
        return {}

    normalized = [str(s).strip().upper() for s in symbols if s]
    engine = _get_engine_or_none()

    if engine is None:
        # in-memory fallback
        result: dict[str, dict] = {}
        for fill in _MEM_FILLS:
            if str(fill.get("side") or "").upper() != "BUY":
                continue
            sym = str(fill.get("symbol") or "").strip().upper()
            if sym in normalized and sym not in result:
                result[sym] = fill
        return result

    try:
        td = trade_date or _today()
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT DISTINCT ON (symbol)
                        symbol, exchange, qty, price_usd,
                        filled_at, trade_date, client_order_key, order_no, meta
                    FROM us_fills
                    WHERE symbol = ANY(:syms)
                      AND side = 'BUY'
                      AND trade_date >= (:td::date - :lookback * INTERVAL '1 day')
                      AND trade_date <= :td::date
                    ORDER BY symbol, filled_at DESC
                """),
                {"syms": normalized, "td": td, "lookback": lookback_days},
            ).fetchall()

            result = {}
            cols = ["symbol", "exchange", "qty", "price_usd",
                    "filled_at", "trade_date", "client_order_key", "order_no", "meta"]
            for row in rows:
                d = dict(zip(cols, row))
                sym = str(d.get("symbol") or "").strip().upper()
                result[sym] = d
            return result
    except Exception as exc:
        logger.warning("[US_DB][load_latest_us_buy_fills_by_symbols][WARN] %s", exc)
        return {}


def has_pending_order_for_symbol_side(
    symbol: str,
    side: str,
    trade_date: str | None = None,
    include_statuses: set[str] | None = None,
) -> bool:
    """Return True when same symbol/side/date has an unfilled real order."""
    statuses = include_statuses or {
        "ACK", "SUBMITTED", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING", "ACK_DB_FAILED",
    }
    td = trade_date or _today()
    sym = str(symbol or "").strip().upper()
    side_u = str(side or "").strip().upper()
    engine = _get_engine_or_none()
    if engine is None:
        return any(
            str(o.get("symbol") or "").strip().upper() == sym
            and str(o.get("side") or "").strip().upper() == side_u
            and str(o.get("trade_date") or "") == td
            and str(o.get("status") or "").upper() in statuses
            and not o.get("dry_run", False)
            for o in _MEM_ORDERS
        )
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text("""
                    SELECT 1 FROM us_orders
                    WHERE symbol=:symbol AND side=:side AND trade_date=:td
                      AND status = ANY(:statuses)
                      AND dry_run = FALSE
                    LIMIT 1
                """),
                {"symbol": sym, "side": side_u, "td": td, "statuses": list(statuses)},
            ).first()
            return row is not None
    except Exception as exc:
        logger.error("[US_ORDERS][PENDING_SYMBOL_SIDE][ERROR] %s", exc)
        return False


def find_recent_sell_ack(symbol: str, trade_date: str | None = None) -> dict | None:
    """Find the most recent same-day SELL ACK-like order for no-balance reconciliation."""
    statuses = {"ACK", "SUBMITTED", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING", "ACK_DB_FAILED"}
    td = trade_date or _today()
    sym = str(symbol or "").strip().upper()
    matches = [o for o in _MEM_ORDERS if str(o.get("symbol") or "").strip().upper() == sym and str(o.get("side") or "").upper() == "SELL" and str(o.get("trade_date") or "") == td and str(o.get("status") or "").upper() in statuses]
    if matches:
        return matches[-1]
    engine = _get_engine_or_none()
    if engine is None:
        return None
    try:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT * FROM us_orders WHERE symbol=:symbol AND side='SELL' AND trade_date=:td
                  AND status = ANY(:statuses) AND dry_run = FALSE
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST LIMIT 1
            """), {"symbol": sym, "td": td, "statuses": list(statuses)}).first()
            return dict(row._mapping) if row else None
    except Exception as exc:
        logger.error("[US_ORDERS][RECENT_SELL_ACK][ERROR] %s", exc)
        return None


def load_us_daily_orders_for_report(trade_date: str) -> list[dict]:
    """Load US-only order rows for daily report from us_orders.

    Uses trade_date first, then NY-day timestamp fallbacks. US reporting must
    not read KR/common order tables.
    """
    engine = _get_engine_or_none()
    if engine is None:
        return []
    from sqlalchemy import text
    from datetime import date, datetime, time, timedelta
    from zoneinfo import ZoneInfo

    def _rows(result) -> list[dict]:
        if hasattr(result, "mappings"):
            return [dict(r) for r in result.mappings().all()]
        return [dict(r) for r in result]

    ny = ZoneInfo("America/New_York")
    utc = ZoneInfo("UTC")
    d = date.fromisoformat(trade_date)
    start_utc = datetime.combine(d, time.min, tzinfo=ny).astimezone(utc).isoformat()
    end_utc = datetime.combine(d + timedelta(days=1), time.min, tzinfo=ny).astimezone(utc).isoformat()
    queries = [("SELECT * FROM us_orders WHERE trade_date = :td", {"td": trade_date})]
    for col in ("created_at", "updated_at", "submitted_at", "acked_at"):
        queries.append((f"SELECT * FROM us_orders WHERE {col} >= :start_ts AND {col} < :end_ts", {"start_ts": start_utc, "end_ts": end_utc}))
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            for sql, params in queries:
                try:
                    rows = _rows(conn.execute(text(sql), params))
                    if rows:
                        return rows
                except Exception:
                    continue
    except Exception:
        logger.exception("[US_ORDERS][REPORT_LOAD][WARN] trade_date=%s", trade_date)
    return []
