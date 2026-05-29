# -*- coding: utf-8 -*-
"""US Portfolio PNL Report Runner.

미국장 전용 PNL report 생성.

Features:
- KIS 해외주식 practice 잔고/평가금액 기반
- Traditional return formula: (current_price - avg_cost) / avg_cost * 100
- USD 기준 (KRW는 보조)
- Markdown/JSON/CSV export
- Holdings PNL table, today trades, blocked orders
- Watchlist score contract, order contract
- Data quality warnings

CRITICAL: 한국장 trader/db/repos.py 사용 금지. trader/us/db/repos.py만 사용.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from trader.us.utils.pnl_utils import (
    first_present,
    normalize_us_position,
    pick_column_from_list,
    safe_float,
    safe_int,
)

logger = logging.getLogger(__name__)

NY_TZ = ZoneInfo("America/New_York")


def get_ny_trade_date(force_now: str | None = None) -> str:
    """Get NY-based trade date."""
    if force_now:
        try:
            dt = datetime.fromisoformat(force_now).astimezone(NY_TZ)
            return dt.strftime("%Y-%m-%d")
        except Exception as exc:
            logger.warning("[US_PNL][WARN] force_now parse failed: %s", exc)
    return datetime.now(tz=NY_TZ).strftime("%Y-%m-%d")


def get_fx_krw_per_usd() -> float:
    """Get KRW/USD exchange rate from env or default."""
    try:
        return float(os.getenv("US_BUDGET_FX_KRW_PER_USD", "1450"))
    except Exception:
        return 1450.0


def _build_holding_from_kis_item(item: dict) -> dict | None:
    """KIS balance output1 항목 하나를 표준화된 holding dict로 변환한다.

    normalize_us_position을 사용해 current → last_price 매핑 포함.
    """
    # KIS 필드를 normalize_us_position이 인식하는 이름으로 보강한 복사본 생성
    enriched = dict(item)
    # KIS 잔고에서 pdno/ovrs_pdno → symbol 매핑 추가 (normalize_us_position은 symbol 우선)
    if "ovrs_pdno" in enriched and "symbol" not in enriched:
        enriched["symbol"] = enriched["ovrs_pdno"]
    elif "pdno" in enriched and "symbol" not in enriched:
        enriched["symbol"] = enriched["pdno"]
    # qty 매핑: KIS US 잔고에서 사용하는 수량 필드 후보 순서대로 시도
    if "qty" not in enriched:
        for qty_key in ("ovrs_cblc_qty", "hldg_qty", "ccld_qty_smtl", "cblc_qty"):
            if qty_key in enriched and enriched[qty_key] not in (None, "", "0"):
                enriched["qty"] = enriched[qty_key]
                break
    # KIS US 잔고 output1에는 직접 현재가 필드가 없고 평가금액만 있다.
    # last_price = ovrs_stck_evlu_amt / qty 로 역산한다.
    if "last_price" not in enriched or safe_float(enriched.get("last_price")) <= 0:
        mv_raw = (
            enriched.get("ovrs_stck_evlu_amt")
            or enriched.get("frcr_evlu_amt2")
            or enriched.get("evlu_amt")
        )
        qty_val = safe_int(enriched.get("qty", 0))
        mv_val = safe_float(mv_raw)
        if qty_val > 0 and mv_val > 0:
            enriched["last_price"] = mv_val / qty_val
            logger.debug(
                "[US_PNL][PRICE_DERIVED] symbol=%s mv=%.4f qty=%d last_price=%.4f",
                enriched.get("symbol", ""),
                mv_val,
                qty_val,
                enriched["last_price"],
            )

    pos = normalize_us_position(enriched)

    if not pos["symbol"] or pos["qty"] <= 0:
        return None

    exchange = str(item.get("natn_cd", item.get("exchange", "NASDAQ"))).strip().upper()
    price_missing = pos["last_price"] <= 0

    return {
        "symbol": pos["symbol"],
        "exchange": exchange,
        "qty": pos["qty"],
        "avg_cost_usd": round(pos["avg_price"], 4),
        "current_price_usd": round(pos["last_price"], 4),
        "price_missing": price_missing,
        "cost_basis_usd": round(pos["cost_basis_usd"], 2),
        "market_value_usd": round(pos["market_value_usd"], 2),
        "unrealized_pnl_usd": round(pos["unrealized_pnl_usd"], 2),
        "pnl_pct": round(pos["unrealized_pnl_pct"], 2),
        "source": "kis_balance",
    }


def fetch_kis_balance(client, env: str) -> dict:
    """Fetch KIS US balance.

    Returns:
        {
            "status": "OK" | "ERROR" | "ERROR_PNL_PRICE_MAPPING",
            "cash_usd": float,
            "holdings": list[dict],
            "error": str | None,
            "pnl_status": "OK" | "ERROR_PNL_PRICE_MAPPING",
        }
    """
    try:
        balance_resp = client.get_us_balance()

        output1 = balance_resp.get("output1") or []
        output2 = balance_resp.get("output2") or {}

        # Cash
        cash_usd = safe_float(output2.get("frcr_dncl_amt_2"))  # USD 현금

        # KIS balance 현재가 매핑 로그 (상세)
        for item in output1:
            try:
                _sym = str(
                    item.get("symbol") or item.get("ovrs_pdno") or item.get("pdno") or ""
                ).strip().upper()
                for _field in ("last_price", "current", "current_price", "market_price",
                               "ovrs_now_pric", "ovrs_now_pric1", "now_price"):
                    _val = item.get(_field)
                    if _val not in (None, "", "0", 0):
                        try:
                            _fval = float(str(_val).replace(",", ""))
                            if _fval > 0:
                                logger.info(
                                    "[US_PNL][PRICE_MAP] symbol=%s source=kis_balance field=%s last_price=%.4f",
                                    _sym, _field, _fval,
                                )
                                break
                        except Exception:
                            pass
            except Exception:
                pass

        output1_count = len(output1)

        # Holdings — normalize_us_position으로 current → last_price 매핑 포함
        holdings = []
        for item in output1:
            try:
                holding = _build_holding_from_kis_item(item)
                if holding is not None:
                    holdings.append(holding)
            except Exception as exc:
                logger.warning("[US_PNL][WARN] parse KIS position failed: %s", exc)

        # PnL 품질 검증
        total_qty = sum(h["qty"] for h in holdings)
        total_market_value = sum(h["market_value_usd"] for h in holdings)
        price_missing_count = sum(1 for h in holdings if h.get("price_missing", False))

        logger.info(
            "[US_PNL][KIS_BALANCE][OK] positions_raw=%d positions_valid=%d cash_usd=%.2f",
            output1_count, len(holdings), cash_usd,
        )
        if price_missing_count > 0:
            logger.warning(
                "[US_PNL][PRICE_MISSING] missing_count=%d",
                price_missing_count,
            )
        else:
            logger.info("[US_PNL][PRICE_MISSING] missing_count=0")

        if total_qty > 0 and total_market_value <= 0:
            pnl_status = "ERROR_PNL_PRICE_MAPPING"
            logger.error(
                "[US_PNL][ERROR_PNL_PRICE_MAPPING] holdings=%d total_qty=%d total_market_value=%.2f",
                len(holdings),
                total_qty,
                total_market_value,
            )
        else:
            pnl_status = "OK"

        logger.info(
            "[US_PNL][KIS_BALANCE_SUMMARY] holdings=%d pnl_status=%s price_missing=%d",
            len(holdings),
            pnl_status,
            price_missing_count,
        )

    except Exception as exc:
        logger.error("[US_PNL][KIS_BALANCE][ERROR] %s", exc)
        return {
            "status": "ERROR",
            "cash_usd": 0.0,
            "holdings": [],
            "error": str(exc),
            "pnl_status": "ERROR",
        }


def _fetch_latest_us_positions_safe(engine: object) -> list[dict]:
    """us_positions에서 최신 포지션을 조회한다 (trade_date 컬럼 없이 fallback).

    trade_date 컬럼 유무를 자동 감지하여 쿼리를 전환한다.
    """
    from sqlalchemy import text as sa_text
    from trader.us.utils.pnl_utils import get_table_columns_sync

    try:
        columns = get_table_columns_sync(engine, "us_positions")
    except Exception:
        columns = set()

    try:
        with engine.connect() as conn:
            if "trade_date" in columns:
                # trade_date 컬럼이 있으면 최신 trade_date 기준
                rows = conn.execute(
                    sa_text("""
                        SELECT DISTINCT ON (symbol) *
                        FROM us_positions
                        WHERE qty > 0
                        ORDER BY symbol, trade_date DESC
                    """)
                ).fetchall()
            else:
                # trade_date 컬럼 없음: 최신 as_of 기준으로 fallback
                logger.warning(
                    "[US_PNL][SCHEMA_FALLBACK] us_positions.trade_date missing; "
                    "using latest as_of or all rows"
                )
                if "as_of" in columns:
                    rows = conn.execute(
                        sa_text("""
                            SELECT DISTINCT ON (symbol) *
                            FROM us_positions
                            WHERE qty > 0
                            ORDER BY symbol, as_of DESC
                        """)
                    ).fetchall()
                else:
                    rows = conn.execute(
                        sa_text("SELECT * FROM us_positions WHERE qty > 0")
                    ).fetchall()
        keys = list(rows[0]._fields) if rows and hasattr(rows[0], "_fields") else []
        return [dict(zip(keys, r)) for r in rows] if keys else [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.warning("[US_PNL][DB_POSITIONS_FALLBACK_ERROR] %s", exc)
        return []


def enrich_holdings_with_db(holdings: list[dict], trade_date: str) -> list[dict]:
    """Enrich KIS holdings with DB metadata (entry_date, score, stop_price, etc.)."""
    from trader.us.db.repos import load_positions

    try:
        db_positions = load_positions(as_of=trade_date)
    except Exception as exc:
        logger.warning("[US_PNL][WARN] DB positions load failed: %s", exc)
        db_positions = []
    
    # Build symbol map
    db_map = {p.get("symbol"): p for p in db_positions if p.get("symbol")}
    
    enriched = []
    for h in holdings:
        symbol = h["symbol"]
        db_pos = db_map.get(symbol, {})
        
        enriched_h = dict(h)
        enriched_h["entry_date"] = db_pos.get("entry_date")
        enriched_h["entry_style"] = db_pos.get("entry_style")
        enriched_h["score"] = safe_float(db_pos.get("score"))
        enriched_h["stop_price_usd"] = safe_float(db_pos.get("stop_price"))
        
        # Days held
        if enriched_h["entry_date"]:
            try:
                entry_dt = datetime.fromisoformat(str(enriched_h["entry_date"]))
                now_dt = datetime.now(tz=NY_TZ)
                days_held = (now_dt - entry_dt).days
                enriched_h["days_held"] = days_held
            except Exception:
                enriched_h["days_held"] = None
        else:
            enriched_h["days_held"] = None
        
        enriched.append(enriched_h)
    
    return enriched


def get_today_trades(trade_date: str) -> list[dict]:
    """Get today's order/fill summary."""
    # This uses load_us_orders (added in daily_report_runner as temp,  should move to repos.py)
    try:
        # Import load_us_orders from daily_report_runner temporarily
        from trader.us.runner.daily_report_runner import load_us_orders
        orders = load_us_orders(trade_date)
    except Exception as exc:
        logger.warning("[US_PNL][WARN] load orders failed: %s", exc)
        orders = []
    
    trades = []
    for order in orders:
        try:
            trades.append({
                "time": str(order.get("created_at", "")),
                "side": order.get("side", ""),
                "symbol": order.get("symbol", ""),
                "exchange": order.get("exchange", ""),
                "qty": int(order.get("qty_requested", 0)),
                "price_usd": safe_float(order.get("avg_price_usd")),
                "notional_usd": safe_float(order.get("qty_requested", 0)) * safe_float(order.get("avg_price_usd")),
                "order_status": order.get("status", ""),
                "fill_status": "FILLED" if int(order.get("qty_filled", 0)) >= int(order.get("qty_requested", 1)) else "PARTIAL",
                "order_no": order.get("order_no", ""),
                "client_order_key": order.get("client_order_key", ""),
                "reason": order.get("meta", {}).get("reason", "") if isinstance(order.get("meta"), dict) else "",
            })
        except Exception as exc:
            logger.warning("[US_PNL][WARN] parse order failed: %s", exc)
    
    return trades


def get_blocked_orders(trade_date: str) -> list[dict]:
    """Get today's blocked/skipped order intents."""
    from trader.us.db.repos import _get_engine_or_none
    from sqlalchemy import text
    
    engine = _get_engine_or_none()
    if engine is None:
        return []
    
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("""
                    SELECT * FROM us_order_intents
                    WHERE trade_date = :td
                      AND status IN ('BLOCKED', 'REJECTED')
                    ORDER BY created_at
                """),
                {"td": trade_date},
            )
            blocked = []
            for r in rows:
                blocked.append({
                    "time": str(r.created_at) if r.created_at else "",
                    "symbol": r.symbol,
                    "side": r.side,
                    "reason": r.status,
                    "source": "intent",
                    "client_order_key": r.client_order_key,
                })
            return blocked
    except Exception as exc:
        logger.warning("[US_PNL][WARN] load blocked orders failed: %s", exc)
        return []


def get_watchlist_score_contract(trade_date: str) -> dict:
    """Get watchlist score contract summary."""
    from trader.us.db.repos import load_locked_us_watchlist
    from trader.us.score_columns import collect_us_score_nonzero_stats
    
    try:
        watchlist = load_locked_us_watchlist(trade_date)
    except Exception as exc:
        logger.warning("[US_PNL][WARN] load watchlist failed: %s", exc)
        return {}
    
    if not watchlist:
        return{}
    
    unique_symbols = set(row.get("symbol") for row in watchlist if row.get("symbol"))
    duplicate_count = len(watchlist) - len(unique_symbols)
    
    stats = collect_us_score_nonzero_stats(watchlist)
    
    return {
        "watchlist_raw_count": len(watchlist),
        "watchlist_unique_count": len(unique_symbols),
        "watchlist_duplicate_count": duplicate_count,
        "score_nonzero_count": stats["score_nonzero"],
        "score_zero_count": stats["score_zero"],
        "score_missing_count": stats["score_missing"],
        "score_nonzero_ratio": stats["score_nonzero_ratio"],
    }


def get_order_contract(today_trades: list[dict]) -> dict:
    """Get order contract summary from today's trades."""
    orders_ack = sum(1 for t in today_trades if t["order_status"] in ("ACK", "SENT"))
    orders_rejected = sum(1 for t in today_trades if t["order_status"] == "REJECTED")
    orders_dry_run = sum(1 for t in today_trades if t["order_status"] == "DRY_RUN")
    orders_blocked = sum(1 for t in today_trades if t["order_status"] == "BLOCKED")
    orders_disabled = sum(1 for t in today_trades if t["order_status"] == "ORDER_DISABLED")
    
    ack_notional = sum(t["notional_usd"] for t in today_trades if t["order_status"] in ("ACK", "SENT"))
    dry_run_notional = sum(t["notional_usd"] for t in today_trades if t["order_status"] == "DRY_RUN")
    
    return {
        "orders_ack": orders_ack,
        "orders_rejected": orders_rejected,
        "orders_dry_run": orders_dry_run,
        "orders_blocked": orders_blocked,
        "orders_disabled": orders_disabled,
        "ack_notional_usd": round(ack_notional, 2),
        "dry_run_notional_usd": round(dry_run_notional, 2),
    }


def generate_pnl_report(
    env: str = "practice",
    session: str | None = None,
    trade_date: str | None = None,
    offline: bool = False,
    fail_on_missing_report: bool = False,
) -> dict:
    """Generate US Portfolio PNL Report.
    
    Args:
        env: Environment
        session: Trading session
        trade_date: Trade date (auto if None)
        offline: Skip KIS queries
        fail_on_missing_report: If True, return error status on failure
        
    Returns:
        {"status": "OK" | "ERROR", "report": {...}}
    """
    force_now = os.getenv("FORCE_NOW", "").strip()
    
    if trade_date is None:
        trade_date = get_ny_trade_date(force_now)
    
    logger.info(
        "[US_PNL][START] env=%s session=%s trade_date=%s offline=%d",
        env, session or "N/A", trade_date, int(offline)
    )
    
    fx_krw_per_usd = get_fx_krw_per_usd()
    
    report = {
        "runtime": {
            "workflow": os.getenv("GITHUB_WORKFLOW", ""),
            "branch": os.getenv("GITHUB_REF_NAME", ""),
            "commit_sha": os.getenv("GITHUB_SHA", "")[:8] if os.getenv("GITHUB_SHA") else "",
            "run_id": os.getenv("GITHUB_RUN_ID", ""),
            "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", ""),
            "actor": os.getenv("GITHUB_ACTOR", ""),
            "session": session,
            "env": env,
            "kis_env": os.getenv("KIS_ENV", ""),
            "dry_run": os.getenv("DRY_RUN", ""),
            "trade_date_ny": trade_date,
            "generated_at_ny": datetime.now(tz=NY_TZ).isoformat(),
            "generated_at_utc": datetime.utcnow().isoformat() + "Z",
            "account_masked": f"{os.getenv('CANO', '')[:4]}****",
            "fx_krw_per_usd": fx_krw_per_usd,
        },
        "portfolio_summary": {},
        "holdings": [],
        "today_trades": [],
        "blocked_orders": [],
        "watchlist_score_contract": {},
        "order_contract": {},
        "data_quality": {
            "warnings": [],
            "errors": [],
        },
    }
    
    # Fetch KIS balance
    if offline:
        logger.info("[US_PNL][OFFLINE] skipping KIS balance")
        balance_result = {"status": "ERROR", "cash_usd": 0.0, "holdings": [], "error": "offline"}
    else:
        from trader.us.execution.kis_us_client import KisUSClient
        client = KisUSClient(env=env)
        balance_result = fetch_kis_balance(client, env)

    pnl_source = "unknown"
    if balance_result["status"] == "OK":
        holdings = balance_result["holdings"]
        cash_usd = balance_result["cash_usd"]
        pnl_source = "kis_balance"
        balance_pnl_status = balance_result.get("pnl_status", "OK")
        if balance_pnl_status != "OK":
            report["data_quality"]["warnings"].append(
                f"pnl_price_mapping_error: {balance_pnl_status}"
            )
    else:
        # Fallback to DB positions
        report["data_quality"]["warnings"].append(f"kis_balance_failed: {balance_result['error']}")
        logger.warning("[US_PNL][WARN] KIS balance failed, using DB fallback")

        # DB schema-aware fallback
        from trader.us.db.repos import _get_engine_or_none
        db_engine = _get_engine_or_none()
        if db_engine is not None:
            db_rows = _fetch_latest_us_positions_safe(db_engine)
            holdings = [normalize_us_position(r) for r in db_rows if r]
            pnl_source = "db_snapshot"
        else:
            holdings = []
            pnl_source = "none"
        cash_usd = 0.0
        balance_pnl_status = "UNKNOWN"

    # Enrich holdings with DB metadata
    enriched_holdings = enrich_holdings_with_db(holdings, trade_date)
    report["holdings"] = enriched_holdings

    # Calculate portfolio summary
    total_positions = len(enriched_holdings)
    market_value_usd = sum(h.get("market_value_usd", 0.0) for h in enriched_holdings)
    cost_basis_usd = sum(h.get("cost_basis_usd", 0.0) for h in enriched_holdings)
    unrealized_pnl_usd = sum(h.get("unrealized_pnl_usd", 0.0) for h in enriched_holdings)
    unrealized_pnl_pct = (unrealized_pnl_usd / cost_basis_usd * 100.0) if cost_basis_usd > 0 else 0.0

    # PnL 검증: 포지션이 있는데 market_value가 0이면 ERROR
    total_qty = sum(h.get("qty", 0) for h in enriched_holdings)
    if total_qty > 0 and market_value_usd <= 0:
        pnl_status = "ERROR_PNL_PRICE_MAPPING"
        report["data_quality"]["errors"].append("total_market_value_zero_with_positions")
        logger.error(
            "[US_PNL][ERROR_PNL_PRICE_MAPPING] total_qty=%d total_market_value_usd=%.2f",
            total_qty,
            market_value_usd,
        )
    else:
        pnl_status = "OK"

    # TODO: Calculate realized PNL from fills
    realized_pnl_today_usd = 0.0

    total_pnl_usd = realized_pnl_today_usd + unrealized_pnl_usd
    total_equity_estimate_usd = cash_usd + market_value_usd

    # Winners/losers
    winners = sum(1 for h in enriched_holdings if h.get("pnl_pct", 0) > 0)
    losers = sum(1 for h in enriched_holdings if h.get("pnl_pct", 0) < 0)

    # Best/worst
    sorted_by_pnl = sorted(enriched_holdings, key=lambda h: h.get("pnl_pct", 0), reverse=True)
    best_position = sorted_by_pnl[0]["symbol"] if sorted_by_pnl else "N/A"
    worst_position = sorted_by_pnl[-1]["symbol"] if sorted_by_pnl else "N/A"

    report["portfolio_summary"] = {
        "source": pnl_source,
        "pnl_status": pnl_status,
        "total_positions": total_positions,
        "cash_usd": round(cash_usd, 2),
        "market_value_usd": round(market_value_usd, 2),
        "cost_basis_usd": round(cost_basis_usd, 2),
        "unrealized_pnl_usd": round(unrealized_pnl_usd, 2),
        "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
        "realized_pnl_today_usd": round(realized_pnl_today_usd, 2),
        "total_pnl_usd": round(total_pnl_usd, 2),
        "total_equity_estimate_usd": round(total_equity_estimate_usd, 2),
        "market_value_krw": round(market_value_usd * fx_krw_per_usd, 0),
        "total_pnl_krw": round(total_pnl_usd * fx_krw_per_usd, 0),
        "winners": winners,
        "losers": losers,
        "best_position": best_position,
        "worst_position": worst_position,
    }

    # Today trades
    report["today_trades"] = get_today_trades(trade_date)

    # Blocked orders
    report["blocked_orders"] = get_blocked_orders(trade_date)

    # Watchlist score contract
    report["watchlist_score_contract"] = get_watchlist_score_contract(trade_date)

    # Order contract
    report["order_contract"] = get_order_contract(report["today_trades"])

    # Save reports
    try:
        os.makedirs("repo/reports/us_portfolio_pnl", exist_ok=True)
    except Exception:
        os.makedirs("reports/us_portfolio_pnl", exist_ok=True)

    report_base = "repo/reports/us_portfolio_pnl" if os.path.exists("repo") else "reports/us_portfolio_pnl"

    # Latest reports
    latest_md = f"{report_base}/latest_us_portfolio_pnl.md"
    latest_json = f"{report_base}/latest_us_portfolio_pnl.json"
    latest_csv = f"{report_base}/latest_us_portfolio_pnl.csv"

    # Dated reports
    dated_dir = f"{report_base}/{trade_date}"
    if session:
        dated_dir = f"{dated_dir}/{session}"
    os.makedirs(dated_dir, exist_ok=True)
    dated_md = f"{dated_dir}/us_portfolio_pnl.md"
    dated_json = f"{dated_dir}/us_portfolio_pnl.json"
    dated_csv = f"{dated_dir}/us_portfolio_pnl.csv"

    # Generate Markdown
    md_content = generate_markdown_report(report)

    # Write reports
    try:
        # Markdown
        with open(latest_md, "w") as f:
            f.write(md_content)
        with open(dated_md, "w") as f:
            f.write(md_content)
        
        # JSON
        with open(latest_json, "w") as f:
            json.dump(report, f, indent=2, default=str)
        with open(dated_json, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        # CSV (holdings only)
        # price_missing 등 추가 필드가 있어도 안전하게 처리.
        # 방법 A: 모든 row 키 합집합 + extrasaction="ignore" (기본)
        # 방법 B: 필수 fieldnames에 price_missing 명시 포함
        _REQUIRED_FIELDNAMES = [
            "trade_date", "session", "env", "symbol", "qty",
            "avg_price", "avg_cost_usd", "last_price", "current_price_usd",
            "market_value", "market_value_usd", "cost_basis", "cost_basis_usd",
            "unrealized_pnl", "unrealized_pnl_usd", "unrealized_pnl_pct", "pnl_pct",
            "price_missing", "source", "exchange",
        ]
        if enriched_holdings:
            all_keys: set[str] = set(_REQUIRED_FIELDNAMES)
            for _h in enriched_holdings:
                all_keys.update(_h.keys())
            fieldnames = sorted(all_keys)
            # trade_date, session, env 필드가 holding dict에 없으면 추가
            for _h in enriched_holdings:
                _h.setdefault("trade_date", trade_date)
                _h.setdefault("session", session or "")
                _h.setdefault("env", env)
                _h.setdefault("price_missing", _h.get("current_price_usd", 0) <= 0)
            with open(latest_csv, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(enriched_holdings)
            with open(dated_csv, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(enriched_holdings)
        
        logger.info(
            "[US_PNL][SAVED] latest=%s dated=%s",
            latest_md, dated_md
        )
    except Exception as exc:
        logger.error("[US_PNL][SAVE_FAILED] %s", exc)
        report["data_quality"]["errors"].append(f"report_save_failed: {exc}")
    
    pnl_ok = not report["data_quality"]["errors"]
    pnl_final_status = "OK" if pnl_ok else "ERROR"
    logger.info(
        "[US_PNL][OK] date=%s positions=%d total_pnl_usd=%.2f",
        trade_date, total_positions, total_pnl_usd
    )
    logger.info(
        "[US_PNL][DONE] status=%s positions=%d price_missing=%d",
        pnl_final_status,
        total_positions,
        sum(1 for h in enriched_holdings if h.get("price_missing", False)),
    )
    
    status = "OK" if not report["data_quality"]["errors"] else "ERROR"
    if fail_on_missing_report and status == "ERROR":
        return {"status": "ERROR", "report": report}
    
    return {"status": pnl_final_status, "report": report}


def generate_markdown_report(report: dict) -> str:
    """Generate markdown content from report dict."""
    lines = [
        "# US Portfolio PNL Report",
        "",
        "## Runtime Metadata",
        "",
        "| Field | Value |",
        "|------|------|",
    ]
    
    for key, val in report["runtime"].items():
        lines.append(f"| {key} | {val} |")
    
    lines.extend([
        "",
        "## Portfolio Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
    ])
    
    for key, val in report["portfolio_summary"].items():
        lines.append(f"| {key} | {val} |")
    
    lines.extend([
        "",
        "## Holdings PNL Table",
        "",
        "| Rank | Symbol | Exchange | Qty | Avg Cost USD | Current Price USD | Cost Basis USD | Market Value USD | Unrealized PNL USD | PNL % | Entry Date | Days Held | Score |",
        "|------|--------|----------|-----|--------------|-------------------|----------------|------------------|--------------------| ------|-----------|-----------|-------|",
    ])
    
    holdings = report["holdings"]
    for rank, h in enumerate(holdings, start=1):
        lines.append(
            f"| {rank} | {h['symbol']} | {h.get('exchange', 'N/A')} | {h['qty']} | "
            f"{h['avg_cost_usd']:.4f} | {h['current_price_usd']:.4f} | {h['cost_basis_usd']:.2f} | "
            f"{h['market_value_usd']:.2f} | {h['unrealized_pnl_usd']:.2f} | {h['pnl_pct']:.2f}% | "
            f"{h.get('entry_date', 'N/A')} | {h.get('days_held', 'N/A')} | {h.get('score', 'N/A')} |"
        )
    
    lines.extend([
        "",
        "## Today Trade Summary",
        "",
        "| Time | Side | Symbol | Qty | Price USD | Notional USD | Order Status | Fill Status | Order No |",
        "|------|------|--------|-----|-----------|--------------|--------------|------------|----------|",
    ])
    
    for t in report["today_trades"]:
        lines.append(
            f"| {t.get('time', 'N/A')[:19]} | {t.get('side', '')} | {t.get('symbol', '')} | "
            f"{t.get('qty', 0)} | {t.get('price_usd', 0):.2f} | {t.get('notional_usd', 0):.2f} | "
            f"{t.get('order_status', '')} | {t.get('fill_status', '')} | {t.get('order_no', '')} |"
        )
    
    if report["blocked_orders"]:
        lines.extend([
            "",
            "## Blocked / Skipped Orders",
            "",
            "| Time | Symbol | Side | Reason |",
            "|------|--------|------|--------|",
        ])
        
        for b in report["blocked_orders"]:
            lines.append(
                f"| {b.get('time', 'N/A')[:19]} | {b.get('symbol', '')} | {b.get('side', '')} | {b.get('reason', '')} |"
            )
    
    wsc = report["watchlist_score_contract"]
    if wsc:
        lines.extend([
            "",
            "## Watchlist / Score Contract",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| watchlist_raw_count | {wsc.get('watchlist_raw_count', 0)} |",
            f"| watchlist_unique_count | {wsc.get('watchlist_unique_count', 0)} |",
            f"| watchlist_duplicate_count | {wsc.get('watchlist_duplicate_count', 0)} |",
            f"| score_nonzero | {wsc.get('score_nonzero_count', 0)} |",
            f"| score_zero | {wsc.get('score_zero_count', 0)} |",
            f"| score_missing | {wsc.get('score_missing_count', 0)} |",
            f"| score_nonzero_ratio | {wsc.get('score_nonzero_ratio', 0.0):.4f} |",
            "",
        ])
    
    oc = report["order_contract"]
    if oc:
        lines.extend([
            "",
            "## Order Contract",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| orders_ack | {oc.get('orders_ack', 0)} |",
            f"| orders_rejected | {oc.get('orders_rejected', 0)} |",
            f"| orders_dry_run | {oc.get('orders_dry_run', 0)} |",
            f"| orders_blocked | {oc.get('orders_blocked', 0)} |",
            f"| orders_disabled | {oc.get('orders_disabled', 0)} |",
            f"| ack_notional_usd | {oc.get('ack_notional_usd', 0.0):.2f} |",
            f"| dry_run_notional_usd | {oc.get('dry_run_notional_usd', 0.0):.2f} |",
            "",
        ])
    
    dq = report["data_quality"]
    if dq["warnings"]:
        lines.extend([
            "",
            "## Data Quality Warnings",
            "",
        ])
        for w in dq["warnings"]:
            lines.append(f"- {w}")
        lines.append("")
    
    if dq["errors"]:
        lines.extend([
            "",
            "## Errors",
            "",
        ])
        for e in dq["errors"]:
            lines.append(f"- {e}")
        lines.append("")
    
    return "\n".join(lines)


def main() -> None:
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()

    parser = argparse.ArgumentParser(description="US Portfolio PNL Report Runner")
    parser.add_argument("--env", default="practice", help="Environment")
    parser.add_argument("--session", default=None, help="Session (am|afternoon|close)")
    parser.add_argument("--trade-date", default=None, help="Trade date YYYY-MM-DD")
    parser.add_argument("--offline", action="store_true", help="Offline mode")
    parser.add_argument("--fail-on-missing-report", action="store_true", help="Exit 1 on error")
    args = parser.parse_args()
    
    result = generate_pnl_report(
        env=args.env,
        session=args.session,
        trade_date=args.trade_date,
        offline=args.offline,
        fail_on_missing_report=args.fail_on_missing_report,
    )
    
    if result["status"] == "ERROR" and args.fail_on_missing_report:
        sys.exit(1)


if __name__ == "__main__":
    main()
