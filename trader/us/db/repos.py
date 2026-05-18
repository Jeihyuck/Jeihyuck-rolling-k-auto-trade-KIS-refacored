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
            timeout_ms = max(1000, min(int(timeout_sec * 1000), 120000))
            # Use set_config() with true for local scope
            conn.execute(
                text("SELECT set_config('statement_timeout', :timeout_value, true)"),
                {"timeout_value": f"{timeout_ms}ms"},
            )
            logger.info(
                "[US_DB][STATEMENT_TIMEOUT] op=load_latest_us_prep_status timeout_ms=%d",
                timeout_ms,
            )
            
            row = conn.execute(
                text("""
                    SELECT run_id, status, trade_date, result, started_at, finished_at
                    FROM us_agent_runs
                    WHERE trade_date = :trade_date
                      AND agent_name = 'us_prep'
                      AND mode = 'prep'
                    ORDER BY started_at DESC
                    LIMIT 1
                """),
                {"trade_date": trade_date},
            ).fetchone()
            
            if row is None:
                return {}
            
            result = dict(row._mapping)
            elapsed_ms = int((time.monotonic() - load_started) * 1000)
            logger.info(
                "[US_PREP_STATUS][LOAD] trade_date=%s status=%s run_id=%s elapsed_ms=%d",
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
                meta = e.get("meta", {})
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
            timeout_ms = max(1000, min(int(timeout_sec * 1000), 120000))
            # Use set_config() with true for local scope
            conn.execute(
                text("SELECT set_config('statement_timeout', :timeout_value, true)"),
                {"timeout_value": f"{timeout_ms}ms"},
            )
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


def check_us_am_already_ran(trade_date: str, timeout_sec: int = 5) -> bool:
    """Check if US AM session already ran for the given trade_date.
    
    Checks:
    1. us_agent_runs for mode='session-am' or agent_name='am'
    2. us_orders for any BUY orders on trade_date
    
    Returns:
        True if AM already ran, False otherwise
    """
    engine = _get_engine_or_none()
    if engine is None:
        # In-memory: check if any BUY orders exist
        return any(
            o.get("trade_date") == trade_date and o.get("direction") == "BUY"
            for o in _MEM_ORDERS
        )
    
    try:
        with engine.connect() as conn:
            # Set statement timeout
            conn.execute(text(f"SET LOCAL statement_timeout = '{timeout_sec * 1000}'"))
            
            # Check us_agent_runs for AM session marker
            agent_row = conn.execute(
                text("""
                    SELECT run_id, status, finished_at
                    FROM us_agent_runs
                    WHERE trade_date = :td
                      AND (mode = 'session-am' OR agent_name = 'am')
                      AND status IN ('OK', 'OK_WITH_WARNINGS')
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
                return True
            
            # Fallback: check us_orders for BUY orders
            order_row = conn.execute(
                text("""
                    SELECT COUNT(*) as cnt
                    FROM us_orders
                    WHERE trade_date = :td
                      AND direction = 'BUY'
                    LIMIT 1
                """),
                {"td": trade_date},
            ).fetchone()
            
            if order_row and order_row[0] > 0:
                logger.info(
                    "[US_AM_ALREADY_RAN][CHECK] trade_date=%s found buy_orders count=%d",
                    trade_date, order_row[0]
                )
                return True
            
            return False
    except Exception as exc:
        logger.error("[US_AM_ALREADY_RAN][CHECK][ERROR] %s", exc)
        return False


def check_us_afternoon_already_ran(trade_date: str, timeout_sec: int = 5) -> bool:
    """Check if US Afternoon session already ran for the given trade_date.

    Checks:
    1. us_agent_runs for mode='session-afternoon' or agent_name='afternoon'
    2. us_orders for any BUY orders placed after 12:30 ET on trade_date

    Returns:
        True if afternoon already ran, False otherwise
    """
    engine = _get_engine_or_none()
    if engine is None:
        return any(
            o.get("trade_date") == trade_date and o.get("direction") == "BUY"
            for o in _MEM_ORDERS
        )

    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET LOCAL statement_timeout = '{timeout_sec * 1000}'"))

            agent_row = conn.execute(
                text("""
                    SELECT run_id, status, finished_at
                    FROM us_agent_runs
                    WHERE trade_date = :td
                      AND (mode = 'session-afternoon' OR agent_name = 'afternoon')
                      AND status IN ('OK', 'OK_WITH_WARNINGS')
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
                return True

            return False
    except Exception as exc:
        logger.error("[US_AFTERNOON_ALREADY_RAN][CHECK][ERROR] %s", exc)
        return False

