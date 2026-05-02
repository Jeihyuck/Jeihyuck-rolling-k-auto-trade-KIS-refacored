# -*- coding: utf-8 -*-
"""US DB Repository Functions.

us_* 테이블에 대한 CRUD 함수 모음.
기준 schema: migrations/0038_us_agent_tables.sql

절대 금지:
- 한국장 테이블(orders, positions, fills, runs, watchlist) 접근 금지
- trader.db.repos import 금지
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

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


def _get_conn():
    url = os.getenv("PBCORE_DB_URL")
    if not url:
        return None
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(url)
    except Exception as exc:
        logger.warning("[US_DB][WARN] DB connection failed: %s", exc)
        return None


def _json(conn: Any, val: Any):
    try:
        from psycopg2.extras import Json  # type: ignore
        return Json(val)
    except ImportError:
        return json.dumps(val)


def _today() -> str:
    return date.today().isoformat()


# ---------------------------------------------------------------------------
# Watchlist
# ---------------------------------------------------------------------------

def save_us_watchlist(entries: list[dict], trade_date: str | None = None) -> int:
    """us_watchlist 저장. schema: trade_date, symbol, exchange, strategy, score, meta"""
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        for e in entries:
            _MEM_WATCHLIST.append({**e, "trade_date": td})
        logger.info("[US_WATCHLIST][SAVE] count=%d (in-memory)", len(entries))
        return len(entries)
    count = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for e in entries:
                    cur.execute(
                        """
                        INSERT INTO us_watchlist (trade_date, symbol, exchange, strategy, score, meta)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (trade_date, symbol, strategy) DO UPDATE
                            SET score=EXCLUDED.score, meta=EXCLUDED.meta
                        """,
                        (td, e.get("symbol"), e.get("exchange","NASDAQ"),
                         e.get("strategy","us_pb1"), e.get("score"),
                         _json(conn, e.get("meta") or {})),
                    )
                    count += 1
    except Exception as exc:
        logger.error("[US_WATCHLIST][ERROR] %s", exc)
    finally:
        conn.close()
    logger.info("[US_WATCHLIST][SAVE] count=%d (db)", count)
    return count


# ---------------------------------------------------------------------------
# Order Intents
# ---------------------------------------------------------------------------

def save_order_intent(intent: dict, trade_date: str | None = None) -> bool:
    """us_order_intents 저장. schema: trade_date, client_order_key, symbol, exchange,
    side, qty, limit_price_usd, notional_usd, strategy, status, meta"""
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        _MEM_INTENTS.append({**intent, "trade_date": td, "status": "PENDING"})
        return True
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO us_order_intents
                        (trade_date, client_order_key, symbol, exchange, side, qty,
                         limit_price_usd, notional_usd, strategy, status, meta)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (client_order_key) DO NOTHING
                    """,
                    (td, intent.get("client_order_key"), intent.get("symbol"),
                     intent.get("exchange","NASDAQ"), intent.get("side","BUY"),
                     int(intent.get("qty",0)),
                     intent.get("limit_price_usd") or intent.get("limit_price"),
                     intent.get("notional_usd"),
                     intent.get("strategy","us_pb1"), "PENDING",
                     _json(conn, intent.get("meta") or {})),
                )
        return True
    except Exception as exc:
        logger.error("[US_INTENT][SAVE][ERROR] %s", exc)
        return False
    finally:
        conn.close()


def load_open_order_intents(trade_date: str | None = None) -> list[dict]:
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        return [i for i in _MEM_INTENTS if i.get("trade_date") == td and i.get("status") == "PENDING"]
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM us_order_intents WHERE trade_date=%s AND status='PENDING'", (td,))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("[US_INTENT][LOAD][ERROR] %s", exc)
        return []
    finally:
        conn.close()


def mark_order_intent_sent(client_order_key: str) -> None:
    conn = _get_conn()
    if conn is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "SENT"
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE us_order_intents SET status='SENT' WHERE client_order_key=%s",
                    (client_order_key,))
    except Exception as exc:
        logger.error("[US_INTENT][MARK_SENT][ERROR] %s", exc)
    finally:
        conn.close()


def mark_order_intent_blocked(client_order_key: str, reason: str = "") -> None:
    conn = _get_conn()
    if conn is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "BLOCKED"
                i["block_reason"] = reason
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE us_order_intents SET status='BLOCKED' WHERE client_order_key=%s",
                    (client_order_key,))
    except Exception as exc:
        logger.error("[US_INTENT][MARK_BLOCKED][ERROR] %s", exc)
    finally:
        conn.close()


def mark_order_intent_rejected(client_order_key: str, reason: str = "") -> None:
    conn = _get_conn()
    if conn is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "REJECTED"
                i["reject_reason"] = reason
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE us_order_intents SET status='REJECTED' WHERE client_order_key=%s",
                    (client_order_key,))
    except Exception as exc:
        logger.error("[US_INTENT][MARK_REJECTED][ERROR] %s", exc)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orders — schema: trade_date, client_order_key, symbol, exchange, side,
#                  qty_requested, qty_filled, avg_price_usd, order_no,
#                  status, dry_run, meta
# 금지 컬럼: qty, order_type, limit_price_usd, notional_usd, raw_response
# ---------------------------------------------------------------------------

def _upsert_order(conn: Any, td: str, row: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO us_orders
                (trade_date, client_order_key, symbol, exchange, side,
                 qty_requested, qty_filled, avg_price_usd, order_no,
                 status, dry_run, meta)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (client_order_key) DO UPDATE
                SET qty_filled    =EXCLUDED.qty_filled,
                    avg_price_usd =EXCLUDED.avg_price_usd,
                    order_no      =EXCLUDED.order_no,
                    status        =EXCLUDED.status,
                    dry_run       =EXCLUDED.dry_run,
                    meta          =EXCLUDED.meta,
                    updated_at    =NOW()
            """,
            (td,
             row["client_order_key"],
             row["symbol"],
             row.get("exchange","NASDAQ"),
             row["side"],
             row["qty_requested"],
             row.get("qty_filled", 0),
             row.get("avg_price_usd"),
             row.get("order_no",""),
             row["status"],
             bool(row.get("dry_run", False)),
             _json(conn, row.get("meta") or {})),
        )


def save_order_ack(order_result: dict, trade_date: str | None = None) -> bool:
    """us_orders ACK 저장."""
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        _MEM_ORDERS.append({
            **order_result, "trade_date": td,
            "status": order_result.get("status","ACK"),
            "qty_requested": int(order_result.get("qty_requested") or order_result.get("qty",0)),
            "qty_filled": int(order_result.get("qty_filled",0)),
            "dry_run": False,
        })
        return True
    try:
        row = {
            "client_order_key": order_result.get("client_order_key",""),
            "symbol": order_result.get("symbol",""),
            "exchange": order_result.get("exchange","NASDAQ"),
            "side": order_result.get("side","BUY"),
            "qty_requested": int(order_result.get("qty_requested") or order_result.get("qty",0)),
            "qty_filled": int(order_result.get("qty_filled",0)),
            "avg_price_usd": order_result.get("avg_price_usd"),
            "order_no": order_result.get("order_no",""),
            "status": order_result.get("status","ACK"),
            "dry_run": False,
            "meta": order_result.get("meta") or {},
        }
        with conn:
            _upsert_order(conn, td, row)
        return True
    except Exception as exc:
        logger.error("[US_ORDER_ACK][SAVE][ERROR] %s", exc)
        return False
    finally:
        conn.close()


def save_order_reject(order_result: dict, trade_date: str | None = None) -> bool:
    """us_orders REJECT 저장."""
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        _MEM_ORDERS.append({
            **order_result, "trade_date": td, "status": "REJECTED",
            "qty_requested": int(order_result.get("qty_requested") or order_result.get("qty",0)),
            "qty_filled": 0, "dry_run": False,
        })
        return True
    try:
        row = {
            "client_order_key": order_result.get("client_order_key",""),
            "symbol": order_result.get("symbol",""),
            "exchange": order_result.get("exchange","NASDAQ"),
            "side": order_result.get("side","BUY"),
            "qty_requested": int(order_result.get("qty_requested") or order_result.get("qty",0)),
            "qty_filled": 0,
            "avg_price_usd": None,
            "order_no": "",
            "status": "REJECTED",
            "dry_run": False,
            "meta": {"reason": order_result.get("reason",""), **(order_result.get("meta") or {})},
        }
        with conn:
            _upsert_order(conn, td, row)
        return True
    except Exception as exc:
        logger.error("[US_ORDER_REJECT][SAVE][ERROR] %s", exc)
        return False
    finally:
        conn.close()


def save_dry_run_order(intent: dict, trade_date: str | None = None) -> bool:
    """us_orders DRY_RUN 저장. status='DRY_RUN', dry_run=True."""
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        _MEM_ORDERS.append({
            **intent, "trade_date": td, "status": "DRY_RUN",
            "qty_requested": int(intent.get("qty",0)),
            "qty_filled": 0, "dry_run": True,
        })
        return True
    try:
        row = {
            "client_order_key": intent.get("client_order_key",""),
            "symbol": intent.get("symbol",""),
            "exchange": intent.get("exchange","NASDAQ"),
            "side": intent.get("side","BUY"),
            "qty_requested": int(intent.get("qty",0)),
            "qty_filled": 0,
            "avg_price_usd": intent.get("limit_price_usd") or intent.get("limit_price"),
            "order_no": "",
            "status": "DRY_RUN",
            "dry_run": True,
            "meta": intent.get("meta") or {},
        }
        with conn:
            _upsert_order(conn, td, row)
        return True
    except Exception as exc:
        logger.error("[US_DRY_RUN_ORDER][SAVE][ERROR] %s", exc)
        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fills — schema: trade_date, symbol, exchange, side, qty, price_usd,
#                 order_no, client_order_key, filled_at, meta
# 금지 컬럼: fill_price_usd, filled_at_str
# ---------------------------------------------------------------------------

def save_fills(fills: list[dict], trade_date: str | None = None) -> int:
    """us_fills 저장."""
    td = trade_date or _today()
    count = 0
    conn = _get_conn()
    if conn is None:
        for f in fills:
            _MEM_FILLS.append({**f, "trade_date": td})
        return len(fills)
    try:
        with conn:
            with conn.cursor() as cur:
                for f in fills:
                    cur.execute(
                        """
                        INSERT INTO us_fills
                            (trade_date, symbol, exchange, side, qty, price_usd,
                             order_no, client_order_key, filled_at, meta)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (td,
                         f.get("symbol"),
                         f.get("exchange","NASDAQ"),
                         f.get("side"),
                         int(f.get("qty",0)),
                         float(f.get("price_usd") or f.get("price",0)),
                         f.get("order_no",""),
                         f.get("client_order_key",""),
                         f.get("filled_at"),
                         _json(conn, f.get("meta") or {})),
                    )
                    count += 1
    except Exception as exc:
        logger.error("[US_FILLS][SAVE][ERROR] %s", exc)
    finally:
        conn.close()
    return count


# ---------------------------------------------------------------------------
# Positions — schema: as_of, symbol, exchange, qty, avg_cost, current_px,
#                      unrealized_pnl_usd, meta
# 금지 컬럼: trade_date, avg_price_usd, current_price_usd, status
# ---------------------------------------------------------------------------

def save_position_snapshot(positions: list[dict], trade_date: str | None = None) -> int:
    """us_positions 스냅샷 저장 (upsert). qty>0이면 open position."""
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        _MEM_POSITIONS.clear()
        for p in positions:
            _MEM_POSITIONS.append({
                **p, "as_of": td,
                "avg_cost": p.get("avg_cost") or p.get("entry_price") or p.get("avg_price_usd",0),
                "current_px": p.get("current_px") or p.get("current_price") or p.get("current_price_usd",0),
            })
        return len(positions)
    count = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for p in positions:
                    avg_cost = float(p.get("avg_cost") or p.get("entry_price") or p.get("avg_price_usd") or 0)
                    current_px = float(p.get("current_px") or p.get("current_price") or p.get("current_price_usd") or 0)
                    cur.execute(
                        """
                        INSERT INTO us_positions
                            (as_of, symbol, exchange, qty, avg_cost, current_px,
                             unrealized_pnl_usd, meta)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (as_of, symbol, exchange) DO UPDATE
                            SET qty               =EXCLUDED.qty,
                                avg_cost          =EXCLUDED.avg_cost,
                                current_px        =EXCLUDED.current_px,
                                unrealized_pnl_usd=EXCLUDED.unrealized_pnl_usd,
                                meta              =EXCLUDED.meta
                        """,
                        (td, p.get("symbol"), p.get("exchange","NASDAQ"),
                         int(p.get("qty",0)), avg_cost, current_px,
                         float(p.get("unrealized_pnl_usd",0)),
                         _json(conn, p.get("meta") or {})),
                    )
                    count += 1
    except Exception as exc:
        logger.error("[US_POSITIONS][SNAPSHOT][ERROR] %s", exc)
    finally:
        conn.close()
    logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", count)
    return count


def load_positions(as_of: str | None = None) -> list[dict]:
    """open 포지션(qty>0) 반환."""
    td = as_of or _today()
    conn = _get_conn()
    if conn is None:
        return [p for p in _MEM_POSITIONS
                if (p.get("as_of") == td or p.get("trade_date") == td)
                and int(p.get("qty",0)) > 0]
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM us_positions WHERE as_of=%s AND qty>0", (td,))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("[US_POSITIONS][LOAD][ERROR] %s", exc)
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def load_today_symbols_sold(trade_date: str | None = None) -> set[str]:
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        return {f["symbol"] for f in _MEM_FILLS
                if f.get("trade_date") == td and f.get("side") == "SELL"}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT symbol FROM us_fills WHERE trade_date=%s AND side='SELL'", (td,))
                return {row[0] for row in cur.fetchall()}
    except Exception as exc:
        logger.error("[US_FILLS][SOLD_TODAY][ERROR] %s", exc)
        return set()
    finally:
        conn.close()


def load_today_order_keys(trade_date: str | None = None) -> set[str]:
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        return {o["client_order_key"] for o in _MEM_ORDERS
                if o.get("trade_date") == td and o.get("client_order_key")}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT client_order_key FROM us_orders WHERE trade_date=%s", (td,))
                return {row[0] for row in cur.fetchall()}
    except Exception as exc:
        logger.error("[US_ORDERS][KEYS][ERROR] %s", exc)
        return set()
    finally:
        conn.close()


def load_open_orders_by_symbol(symbol: str, trade_date: str | None = None) -> list[dict]:
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        return [o for o in _MEM_ORDERS
                if o.get("symbol") == symbol and o.get("trade_date") == td
                and o.get("status") in ("ACK", "SENT", "DRY_RUN")]
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT * FROM us_orders
                    WHERE symbol=%s AND trade_date=%s
                      AND status IN ('ACK','SENT','DRY_RUN')""",
                    (symbol, td))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("[US_ORDERS][OPEN_SYMBOL][ERROR] %s", exc)
        return []
    finally:
        conn.close()


def has_pending_order(symbol: str, trade_date: str | None = None) -> bool:
    return len(load_open_orders_by_symbol(symbol, trade_date)) > 0


def has_position(symbol: str, as_of: str | None = None) -> bool:
    positions = load_positions(as_of)
    return any(p.get("symbol") == symbol and int(p.get("qty",0)) > 0 for p in positions)


# ---------------------------------------------------------------------------
# Reconcile log — schema: trade_date, status, message, meta
# 금지 컬럼: position_count, total_pvs_usd, detail → meta에 저장
# ---------------------------------------------------------------------------

def save_reconcile_log(log: dict, trade_date: str | None = None) -> bool:
    td = trade_date or _today()
    conn = _get_conn()
    if conn is None:
        _MEM_RECONCILE_LOGS.append({**log, "trade_date": td})
        return True
    try:
        meta = {
            "position_count": log.get("position_count", 0),
            "total_pvs_usd": float(log.get("total_pvs", 0)),
            "detail": log.get("detail") or {},
        }
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO us_reconcile_logs (trade_date, status, message, meta)
                    VALUES (%s,%s,%s,%s)""",
                    (td, log.get("status","OK"), log.get("message",""),
                     _json(conn, meta)),
                )
        return True
    except Exception as exc:
        logger.error("[US_RECONCILE_LOG][SAVE][ERROR] %s", exc)
        return False
    finally:
        conn.close()
