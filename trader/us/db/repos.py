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
from datetime import date
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


def reset_memory_stores() -> None:
    """테스트용 메모리 스토어 초기화."""
    global _MEM_WATCHLIST, _MEM_INTENTS, _MEM_ORDERS, _MEM_FILLS, _MEM_POSITIONS, _MEM_RECONCILE_LOGS
    _MEM_WATCHLIST = []
    _MEM_INTENTS = []
    _MEM_ORDERS = []
    _MEM_FILLS = []
    _MEM_POSITIONS = []
    _MEM_RECONCILE_LOGS = []


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
                        "meta": _json_param(e.get("meta")),
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
    engine = _get_engine_or_none()
    if engine is None:
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


# ---------------------------------------------------------------------------
# Orders — schema: trade_date, client_order_key, symbol, exchange, side,
#                  qty_requested, qty_filled, avg_price_usd, order_no,
#                  status, dry_run, meta
# 금지 컬럼: qty, order_type, limit_price_usd, notional_usd, raw_response
# ---------------------------------------------------------------------------

def _upsert_order(conn: Any, td: str, row: dict) -> None:
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
    engine = _get_engine_or_none()
    if engine is None:
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
# Fills — schema: trade_date, symbol, exchange, side, qty, price_usd,
#                 order_no, client_order_key, filled_at, meta
# ---------------------------------------------------------------------------

def save_fills(fills: list[dict], trade_date: str | None = None) -> int:
    """us_fills 저장."""
    td = trade_date or _today()
    if not fills:
        return 0
    engine = _get_engine_or_none()
    if engine is None:
        for f in fills:
            _MEM_FILLS.append({**f, "trade_date": td})
        return len(fills)
    count = 0
    try:
        with engine.begin() as conn:
            for f in fills:
                conn.execute(
                    text("""
                        INSERT INTO us_fills
                            (trade_date, symbol, exchange, side, qty, price_usd,
                             order_no, client_order_key, filled_at, meta)
                        VALUES (:td, :symbol, :exchange, :side, :qty, :price_usd,
                                :order_no, :cok, :filled_at, CAST(:meta AS jsonb))
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
                    },
                )
                count += 1
    except Exception as exc:
        logger.error("[US_FILLS][SAVE][ERROR] %s", exc)
    return count


# ---------------------------------------------------------------------------
# Positions — schema: as_of, symbol, exchange, qty, avg_cost, current_px,
#                      unrealized_pnl_usd, meta
# ---------------------------------------------------------------------------

def save_position_snapshot(positions: list[dict], trade_date: str | None = None) -> int:
    """us_positions 스냅샷 저장 (upsert). qty>0이면 open position."""
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        _MEM_POSITIONS.clear()
        for p in positions:
            _MEM_POSITIONS.append({
                **p, "as_of": td,
                "avg_cost": p.get("avg_cost") or p.get("entry_price") or p.get("avg_price_usd", 0),
                "current_px": p.get("current_px") or p.get("current_price") or p.get("current_price_usd", 0),
            })
        return len(positions)
    count = 0
    try:
        with engine.begin() as conn:
            for p in positions:
                avg_cost = float(p.get("avg_cost") or p.get("entry_price") or p.get("avg_price_usd") or 0)
                current_px = float(p.get("current_px") or p.get("current_price") or p.get("current_price_usd") or 0)
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
                        "meta": _json_param(p.get("meta")),
                    },
                )
                count += 1
    except Exception as exc:
        logger.error("[US_POSITIONS][SNAPSHOT][ERROR] %s", exc)
    logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", count)
    return count


def load_positions(as_of: str | None = None) -> list[dict]:
    """open 포지션(qty>0) 반환."""
    td = as_of or _today()
    engine = _get_engine_or_none()
    if engine is None:
        return [p for p in _MEM_POSITIONS
                if (p.get("as_of") == td or p.get("trade_date") == td)
                and int(p.get("qty", 0)) > 0]
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT * FROM us_positions WHERE as_of=:td AND qty>0"),
                {"td": td},
            )
            return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("[US_POSITIONS][LOAD][ERROR] %s", exc)
        return []


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


def load_open_orders_by_symbol(symbol: str, trade_date: str | None = None) -> list[dict]:
    td = trade_date or _today()
    engine = _get_engine_or_none()
    if engine is None:
        return [o for o in _MEM_ORDERS
                if o.get("symbol") == symbol and o.get("trade_date") == td
                and o.get("status") in ("ACK", "SENT", "DRY_RUN")]
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("""
                    SELECT * FROM us_orders
                    WHERE symbol=:symbol AND trade_date=:td
                      AND status IN ('ACK','SENT','DRY_RUN')
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
