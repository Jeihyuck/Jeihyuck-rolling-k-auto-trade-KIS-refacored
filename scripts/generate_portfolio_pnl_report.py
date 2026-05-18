#!/usr/bin/env python3
"""scripts/generate_portfolio_pnl_report.py

PB1 Portfolio PNL Report 생성기.
- KIS balance output1 → DB positions → DB fills → DB price_daily 순서로 데이터 조합.
- GitHub Actions Summary / artifact 용 Markdown / JSON / CSV 출력.
- 계좌번호, token, app key, secret은 절대 원문 출력하지 않는다.
- KIS price API fallback은 PB1_PNL_PRICE_FALLBACK_LIMIT 이하로만 허용.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import sys
import traceback
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any

# ── stdlib 로깅 설정 ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("generate_portfolio_pnl_report")

# ── 경로 설정 ────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

KST = timezone(timedelta(hours=9))
REPORT_DIR = REPO_ROOT / "reports" / "portfolio_pnl"


def _now_kst() -> datetime:
    return datetime.now(KST)


def _mask_account(account_no: str | None, env: str | None) -> str:
    """계좌번호 마스킹. 예: practice:****3616:****"""
    if not account_no:
        return "****"
    masked = account_no[-4:] if len(account_no) >= 4 else "****"
    suffix = "****"
    env_label = str(env or "practice")
    return f"{env_label}:****{masked}:{suffix}"


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _fmt_krw(v: float) -> str:
    val = int(round(v))
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:,}"


def _fmt_pct(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def _days_held(entry_date_str: str | None, trade_date: date) -> int:
    if not entry_date_str:
        return 0
    try:
        ed = datetime.fromisoformat(str(entry_date_str)).date()
        return max(0, (trade_date - ed).days)
    except Exception:
        return 0


def _load_engine():
    """DB engine 로드. 실패 시 None 반환."""
    try:
        from trader.db.engine import make_engine
        db_url = os.getenv("PBCORE_DB_URL") or os.getenv("DATABASE_URL")
        if not db_url:
            logger.warning("[PNL_REPORT][DB_SKIP] PBCORE_DB_URL not set")
            return None
        return make_engine()
    except Exception as exc:
        logger.warning("[PNL_REPORT][DB_ENGINE_FAIL] err=%s", exc)
        traceback.print_exc()
        return None


def _load_kis_api():
    """KIS API 인스턴스 로드. 실패 시 None 반환."""
    try:
        from trader.kis_wrapper import KisAPI
        env = os.getenv("KIS_ENV") or os.getenv("STRATEGY_ENV") or "practice"
        kis = KisAPI(env=env)
        return kis
    except Exception as exc:
        logger.warning("[PNL_REPORT][KIS_INIT_FAIL] err=%s", exc)
        return None


def _get_positions_from_db(engine, env: str, trade_date: date) -> list[dict]:
    if engine is None:
        return []
    try:
        from trader.db.repos import PositionsRepo
        repo = PositionsRepo(engine)
        rows = repo.list_positions(env=env, strategy="pb1_watchlist_final")
        return [dict(r) for r in (rows or [])]
    except Exception as exc:
        logger.warning("[PNL_REPORT][DB_POSITIONS_FAIL] err=%s", exc)
        return []


def _get_balance_from_kis(kis) -> dict:
    """KIS 잔고 조회. output1(종목별) + output2(계좌 요약) 반환."""
    if kis is None:
        return {}
    try:
        balance = kis.get_balance()
        return balance or {}
    except Exception as exc:
        logger.warning("[PNL_REPORT][KIS_BALANCE_FAIL] err=%s", exc)
        return {}


def _get_today_orders_from_db(engine, env: str, trade_date: date) -> list[dict]:
    if engine is None:
        return []
    try:
        from trader.db.repos import OrdersRepo
        from trader.utils.time_utils import now_kst
        repo = OrdersRepo(engine)
        rows = repo.list_today_orders(env=env)
        logger.info("[PNL_REPORT][DB_ORDERS][OK] rows=%s", len(rows or []))
        return [dict(r) for r in (rows or [])]
    except Exception as exc:
        logger.warning("[PNL_REPORT][DB_ORDERS_FAIL] err=%s", exc)
        return []


def _get_today_fills_from_db(engine, env: str) -> list[dict]:
    if engine is None:
        return []
    try:
        from trader.db.repos import FillsRepo
        repo = FillsRepo(engine)
        rows = repo.list_today_fills(env=env)
        return [dict(r) for r in (rows or [])]
    except Exception as exc:
        logger.warning("[PNL_REPORT][DB_FILLS_FAIL] err=%s", exc)
        return []


def _get_blocked_orders_from_db(engine, env: str) -> list[dict]:
    """오늘 SKIP된 주문 조회."""
    if engine is None:
        return []
    try:
        from trader.db.repos import LedgerEventsRepo
        repo = LedgerEventsRepo(engine)
        today_start = _now_kst().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today_start + timedelta(days=1)
        rows = repo.list_events_in_window(
            env=env,
            start_at=today_start,
            end_at=tomorrow,
            event_types=["ORDER_SKIP"],
        )
        logger.info("[PNL_REPORT][DB_BLOCKED][OK] rows=%s", len(rows or []))
        return [dict(r) for r in (rows or [])]
    except Exception as exc:
        logger.warning("[PNL_REPORT][DB_BLOCKED_FAIL] err=%s", exc)
        return []


def _name_for_code(code: str, balance_rows: list[dict], db_positions: list[dict]) -> str:
    """종목명 조회 (balance → DB positions 순)."""
    for row in balance_rows:
        if str(row.get("pdno") or "").zfill(6) == code:
            return str(row.get("prdt_name") or "")
    for pos in db_positions:
        if str(pos.get("code") or "").zfill(6) == code:
            return str(pos.get("name") or "")
    return code


def _build_holdings_pnl(
    balance_rows: list[dict],
    db_positions: list[dict],
    today_fills: list[dict],
    trade_date: date,
) -> tuple[list[dict], list[str]]:
    """Holdings별 PNL 계산. (holdings_list, data_quality_warnings)"""
    warnings: list[str] = []
    holdings: list[dict] = []
    db_pos_by_code = {str(p.get("code") or "").zfill(6): p for p in db_positions}
    balance_by_code = {str(r.get("pdno") or "").zfill(6): r for r in balance_rows}

    all_codes = sorted(set(list(db_pos_by_code.keys()) + list(balance_by_code.keys())))

    for code in all_codes:
        bal = balance_by_code.get(code, {})
        pos = db_pos_by_code.get(code, {})

        # 수량
        qty = _safe_int(bal.get("hldg_qty") or pos.get("qty"))
        if qty <= 0:
            continue

        # 매수 평균가
        avg_buy = _safe_float(bal.get("pchs_avg_pric") or pos.get("avg_buy_price"))

        # 현재가 (balance prpr 우선)
        current_price = _safe_float(bal.get("prpr") or bal.get("stck_prpr"))
        price_source = "kis_balance_prpr" if current_price > 0 else "unknown"
        if current_price <= 0:
            current_price = _safe_float(pos.get("last_price"))
            price_source = "db_last_price" if current_price > 0 else "unknown"
        if current_price <= 0:
            warnings.append(f"PRICE_SOURCE_DEGRADED:{code}")
            price_source = "degraded"

        # 종목명
        name = str(bal.get("prdt_name") or pos.get("name") or code)

        # PNL 계산 (전통 방식)
        cost_basis = avg_buy * qty
        market_value = current_price * qty
        unrealized_pnl = (current_price - avg_buy) * qty if avg_buy > 0 else 0.0
        pnl_pct = ((current_price - avg_buy) / avg_buy * 100) if avg_buy > 0 else 0.0

        # 오늘 실현손익 (fill에서 계산)
        today_sell_fills = [
            f for f in today_fills
            if str(f.get("code") or "").zfill(6) == code
            and str(f.get("side") or "").upper() == "SELL"
        ]
        realized_pnl_today = sum(
            (_safe_float(f.get("price")) - avg_buy) * _safe_int(f.get("qty"))
            for f in today_sell_fills
        )

        # ========== entry_date / days_held (우선순위: entry_date → entry_ts → last_fill_at → created_at) ==========
        entry_date = None
        for key in ("entry_date", "entry_ts", "first_buy_fill_at", "last_fill_at", "created_at"):
            value = pos.get(key)
            if value:
                entry_date = str(value)
                break
        
        # entry_date가 없으면 warning
        if not entry_date:
            entry_date = ""
            warnings.append(f"DAYS_HELD_ENTRY_DATE_MISSING:{code}")
        
        days_held = _days_held(entry_date, trade_date)
        rank = _safe_int(bal.get("rank") or pos.get("rank_final30") or 0)

        holdings.append({
            "rank": rank,
            "code": code,
            "name": name,
            "qty": qty,
            "entry_date": entry_date[:10] if entry_date else "",
            "days_held": days_held,
            "avg_buy": avg_buy,
            "current_price": current_price,
            "cost_basis": cost_basis,
            "market_value": market_value,
            "unrealized_pnl": unrealized_pnl,
            "pnl_pct": pnl_pct,
            "realized_pnl_today": realized_pnl_today,
            "total_pnl": unrealized_pnl + realized_pnl_today,
            "stop_price": _safe_float(pos.get("stop_price")),
            "pivot_price": _safe_float(pos.get("pivot") or pos.get("pivot_price_at_entry")),
            "entry_style": str(pos.get("entry_style_selected") or pos.get("entry_reason") or ""),
            "exit_policy": str(pos.get("exit_policy_family") or ""),
            "last_fill": str(pos.get("last_fill_at") or "")[:8],
            "price_source": price_source,
        })

    # rank 없으면 유저 코드순
    holdings.sort(key=lambda x: (x["rank"] if x["rank"] > 0 else 999, x["code"]))
    for i, h in enumerate(holdings, start=1):
        if h["rank"] == 0:
            h["rank"] = i

    return holdings, warnings


def _build_today_trades(today_orders: list[dict], today_fills: list[dict], balance_rows: list[dict], db_positions: list[dict]) -> list[dict]:
    trades = []
    for fill in today_fills:
        code = str(fill.get("code") or "").zfill(6)
        name = _name_for_code(code, balance_rows, db_positions)
        trades.append({
            "time": str(fill.get("filled_at") or "")[:19],
            "side": str(fill.get("side") or "").upper(),
            "code": code,
            "name": name,
            "qty": _safe_int(fill.get("qty")),
            "price": _safe_float(fill.get("price")),
            "amount": _safe_float(fill.get("price")) * _safe_int(fill.get("qty")),
            "reason": str((fill.get("fill_meta_json") or {}).get("entry_reason") or ""),
            "order_id": str(fill.get("kis_odno") or fill.get("order_id") or ""),
            "fill_status": "FILL_CONFIRMED",
        })
    for order in today_orders:
        code = str(order.get("code") or "").zfill(6)
        # fill이 이미 있으면 skip
        if any(f.get("code") == code and f.get("side", "").upper() == str(order.get("side") or "").upper() for f in today_fills):
            continue
        name = _name_for_code(code, balance_rows, db_positions)
        meta = order.get("entry_meta_json") or {}
        trades.append({
            "time": str(order.get("submitted_at") or order.get("acked_at") or order.get("created_at") or "")[:19],
            "side": str(order.get("side") or "").upper(),
            "code": code,
            "name": name,
            "qty": _safe_int(order.get("qty")),
            "price": _safe_float(order.get("price")),
            "amount": _safe_float(order.get("price")) * _safe_int(order.get("qty")),
            "reason": str(meta.get("entry_reason") or ""),
            "order_id": str(order.get("kis_odno") or order.get("id") or ""),
            "fill_status": "ACCEPTED_OR_CONFIRMED",
        })
    trades.sort(key=lambda x: x["time"])
    return trades


def _build_blocked_orders(blocked_events: list[dict], balance_rows: list[dict], db_positions: list[dict]) -> list[dict]:
    blocked = []
    for ev in blocked_events:
        payload = ev.get("payload_json") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        code = str(ev.get("code") or "").zfill(6)
        name = _name_for_code(code, balance_rows, db_positions)
        reasons = payload.get("reasons") or [ev.get("reason") or "SKIP"]
        blocked.append({
            "time": str(ev.get("created_at") or "")[:19],
            "code": code,
            "name": name,
            "side": str(ev.get("side") or "BUY").upper(),
            "reason": ", ".join(str(r) for r in reasons),
        })
    return blocked


def _build_portfolio_summary(
    holdings: list[dict],
    today_fills: list[dict],
    db_positions: list[dict],
    cash: "float | None",
) -> dict:
    """
    Portfolio summary 계산. realized_pnl_today는 today_fills 기준 (전량매도 포함).
    """
    total_cost = sum(h["cost_basis"] for h in holdings)
    market_value = sum(h["market_value"] for h in holdings)
    unrealized_pnl = sum(h["unrealized_pnl"] for h in holdings)
    unrealized_pnl_pct = (unrealized_pnl / total_cost * 100) if total_cost > 0 else 0.0
    
    # ========== realized_pnl_today 계산: today SELL fills 전체 기준 ==========
    # 전량매도된 종목도 포함하도록 today_fills에서 직접 계산
    db_pos_by_code = {str(p.get("code") or "").zfill(6): p for p in db_positions}
    realized_today = 0.0
    realized_warnings: list[str] = []
    
    for fill in today_fills:
        if str(fill.get("side") or "").upper() != "SELL":
            continue
        
        code = str(fill.get("code") or "").zfill(6)
        sell_qty = _safe_int(fill.get("qty"))
        sell_price = _safe_float(fill.get("price"))
        
        # avg_buy 조회: DB position 우선
        avg_buy = 0.0
        pos = db_pos_by_code.get(code)
        if pos:
            avg_buy = _safe_float(pos.get("avg_buy_price"))
        
        # avg_buy 찾기 실패 시 경고
        if avg_buy <= 0:
            realized_warnings.append(f"REALIZED_PNL_AVG_BUY_MISSING:{code}")
            continue
        
        pnl = (sell_price - avg_buy) * sell_qty
        realized_today += pnl
    
    logger.info(
        "[PNL_REPORT][REALIZED][SUMMARY] realized_today=%.2f warnings=%s",
        realized_today, realized_warnings,
    )
    
    total_pnl = unrealized_pnl + realized_today
    winners = sum(1 for h in holdings if h["pnl_pct"] >= 0)
    losers = sum(1 for h in holdings if h["pnl_pct"] < 0)
    best = max(holdings, key=lambda x: x["pnl_pct"], default=None)
    worst = min(holdings, key=lambda x: x["pnl_pct"], default=None)
    return {
        "total_positions": len(holdings),
        "total_cost": total_cost,
        "market_value": market_value,
        "unrealized_pnl": unrealized_pnl,
        "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
        "realized_pnl_today": realized_today,
        "total_pnl": total_pnl,
        "cash": cash,
        "cash_unavailable": cash is None,
        "total_equity_estimate": (market_value + cash) if cash is not None else None,
        "winners": winners,
        "losers": losers,
        "best_position": f"{best['name']} {_fmt_pct(best['pnl_pct'])}" if best else "",
        "worst_position": f"{worst['name']} {_fmt_pct(worst['pnl_pct'])}" if worst else "",
    }


def _build_markdown(
    runtime: dict,
    summary: dict,
    holdings: list[dict],
    today_trades: list[dict],
    blocked_orders: list[dict],
    data_quality: dict,
) -> str:
    lines: list[str] = []
    lines.append("# PB1 Portfolio PNL Report\n")

    # Runtime Metadata
    lines.append("## Runtime Metadata\n")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    for k, v in runtime.items():
        lines.append(f"| {k} | {v} |")
    lines.append("")

    # Portfolio Summary
    lines.append("## Portfolio Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| Total Positions | {summary['total_positions']} |")
    lines.append(f"| Total Cost | {_fmt_krw(summary['total_cost'])} KRW |")
    lines.append(f"| Market Value | {_fmt_krw(summary['market_value'])} KRW |")
    lines.append(f"| Unrealized PNL | {_fmt_krw(summary['unrealized_pnl'])} KRW |")
    lines.append(f"| Unrealized PNL % | {_fmt_pct(summary['unrealized_pnl_pct'])} |")
    lines.append(f"| Realized PNL Today | {_fmt_krw(summary['realized_pnl_today'])} KRW |")
    lines.append(f"| Total PNL | {_fmt_krw(summary['total_pnl'])} KRW |")
    _cash_val = summary["cash"]
    _cash_src = summary.get("cash_source", "unknown")
    _cash_disp = "N/A (unavailable)" if _cash_val is None else f"{_fmt_krw(_cash_val)} KRW"
    lines.append(f"| Cash | {_cash_disp} | source={_cash_src} |")
    _eq = summary["total_equity_estimate"]
    _eq_disp = "N/A" if _eq is None else f"{_fmt_krw(_eq)} KRW"
    lines.append(f"| Total Equity Estimate | {_eq_disp} |")
    lines.append(f"| Winners / Losers | {summary['winners']} / {summary['losers']} |")
    lines.append(f"| Best Position | {summary.get('best_position', '')} |")
    lines.append(f"| Worst Position | {summary.get('worst_position', '')} |")
    lines.append("")

    # Holdings PNL Table
    lines.append("## Holdings PNL Table\n")
    lines.append("| Rank | Code | Name | Qty | Entry Date | Days Held | Avg Buy | Final Price | Cost | Market Value | Unrealized PNL | PNL % | Stop | Pivot | Entry Style | Exit Policy | Last Fill |")
    lines.append("|---:|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|")
    for h in holdings:
        lines.append(
            f"| {h['rank']} | {h['code']} | {h['name']} | {h['qty']} "
            f"| {h['entry_date']} | {h['days_held']} "
            f"| {int(h['avg_buy']):,} | {int(h['current_price']):,} "
            f"| {int(h['cost_basis']):,} | {int(h['market_value']):,} "
            f"| {_fmt_krw(h['unrealized_pnl'])} | {_fmt_pct(h['pnl_pct'])} "
            f"| {int(h['stop_price']):,} | {int(h['pivot_price']):,} "
            f"| {h['entry_style']} | {h['exit_policy']} | {h['last_fill']} |"
        )
    lines.append("")

    # Today Trade Summary
    if today_trades:
        lines.append("## Today Trade Summary\n")
        lines.append("| Time | Side | Code | Name | Qty | Price | Amount | Reason | Order ID | Fill Status |")
        lines.append("|---|---|---|---|---:|---:|---:|---|---|---|")
        for t in today_trades:
            lines.append(
                f"| {t['time'][:8]} | {t['side']} | {t['code']} | {t['name']} "
                f"| {t['qty']} | {int(t['price']):,} | {int(t['amount']):,} "
                f"| {t['reason']} | {t['order_id']} | {t['fill_status']} |"
            )
        lines.append("")

    # Blocked / Skipped Orders
    if blocked_orders:
        lines.append("## Blocked / Skipped Orders\n")
        lines.append("| Time | Code | Name | Side | Reason |")
        lines.append("|---|---|---|---|---|")
        for b in blocked_orders:
            lines.append(f"| {b['time'][:8]} | {b['code']} | {b['name']} | {b['side']} | {b['reason']} |")
        lines.append("")

    # Data Quality Warnings
    if data_quality.get("warnings"):
        lines.append("## Data Quality Warnings\n")
        lines.append("| Warning | Detail |")
        lines.append("|---|---|")
        for w in data_quality["warnings"]:
            parts = str(w).split(":", 1)
            key = parts[0]
            detail = parts[1] if len(parts) > 1 else ""
            lines.append(f"| {key} | {detail} |")
        lines.append("")

    return "\n".join(lines)


def _failure_report(reason: str) -> str:
    return (
        "# PB1 Portfolio PNL Report\n\n"
        "## Report Status\n\n"
        "| Field | Value |\n"
        "|---|---|\n"
        f"| Status | FAILED_TO_GENERATE |\n"
        f"| Reason | {reason} |\n"
        "| Fallback | None |\n"
        "| Action Required | Check PBCORE_DB_URL / DB availability |\n"
    )


def main() -> int:
    enabled = os.getenv("PB1_PNL_REPORT_ENABLED", "1") not in {"0", "false", "False"}
    if not enabled:
        logger.info("[PNL_REPORT][SKIP] PB1_PNL_REPORT_ENABLED=0")
        return 0

    session = os.getenv("PB1_PNL_REPORT_SESSION", "am")
    env = os.getenv("KIS_ENV") or os.getenv("STRATEGY_ENV") or "practice"
    trade_date_str = os.getenv("TRADE_DATE") or _now_kst().strftime("%Y-%m-%d")
    try:
        trade_date = date.fromisoformat(trade_date_str)
    except Exception:
        trade_date = _now_kst().date()

    generated_at = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("[PNL_REPORT][START] session=%s env=%s trade_date=%s", session, env, trade_date)

    try:
        engine = _load_engine()
        kis = _load_kis_api()

        # KIS 잔고 조회
        balance = _get_balance_from_kis(kis)
        balance_output1 = list(balance.get("output1") or [])
        balance_output2 = balance.get("output2") or {}
        if isinstance(balance_output2, list):
            balance_output2 = balance_output2[0] if balance_output2 else {}
        _raw_cash_val = (
            balance_output2.get("nxdy_auto_rdpt_amt")
            or balance_output2.get("dnca_tot_amt")
            or balance_output2.get("tot_evlu_amt")
        )
        if _raw_cash_val is not None and str(_raw_cash_val).strip() not in ("", "0", "0.0"):
            cash: "float | None" = _safe_float(_raw_cash_val)
            cash_source = "kis_balance"
        else:
            cash = None
            cash_source = "unavailable"
            logger.warning("[PNL_REPORT][CASH_UNAVAILABLE] balance_output2=%s", balance_output2)

        # DB 데이터 조회
        db_positions = _get_positions_from_db(engine, env, trade_date)
        today_orders = _get_today_orders_from_db(engine, env, trade_date)
        today_fills = _get_today_fills_from_db(engine, env)
        blocked_events = _get_blocked_orders_from_db(engine, env)

        # Holdings PNL
        holdings, quality_warnings = _build_holdings_pnl(
            balance_output1, db_positions, today_fills, trade_date
        )

        # Summary
        summary = _build_portfolio_summary(holdings, today_fills, db_positions, cash)
        summary["cash_source"] = cash_source

        # Today trades
        today_trades = _build_today_trades(today_orders, today_fills, balance_output1, db_positions)

        # Blocked orders
        blocked_orders = _build_blocked_orders(blocked_events, balance_output1, db_positions)

        # Data quality
        degraded_count = sum(1 for h in holdings if h.get("price_source") == "degraded")
        data_quality = {
            "degraded": degraded_count > 0,
            "warnings": quality_warnings[:20],
        }
        if degraded_count > 0:
            data_quality["warnings"].insert(
                0,
                f"PRICE_SOURCE_DEGRADED:{degraded_count} positions used DB latest close instead of KIS balance prpr",
            )

        # Runtime metadata
        account_no = os.getenv("KIS_ACCOUNT") or os.getenv("KIS_CANO")
        runtime = {
            "Workflow": f"Trade {session.upper()}",
            "Branch": os.getenv("GITHUB_REF_NAME") or "nullim",
            "Commit SHA": (os.getenv("GITHUB_SHA") or "")[:7] or "local",
            "Run ID": os.getenv("GITHUB_RUN_ID") or "local",
            "Run Attempt": os.getenv("GITHUB_RUN_ATTEMPT") or "1",
            "Actor": os.getenv("GITHUB_ACTOR") or "local",
            "Strategy Env": env,
            "KIS Env": env,
            "Session": session,
            "Trade Date": str(trade_date),
            "As Of": str(trade_date - timedelta(days=1)),
            "Account": _mask_account(account_no, env),
            "Report Generated KST": generated_at,
        }

        # Markdown 생성
        md_content = _build_markdown(runtime, summary, holdings, today_trades, blocked_orders, data_quality)

        # JSON 구조
        json_data = {
            "runtime": {k.lower().replace(" ", "_"): v for k, v in runtime.items()},
            "portfolio_summary": summary,
            "holdings": holdings,
            "today_trades": today_trades,
            "blocked_orders": blocked_orders,
            "pending_orders": [],
            "risk_summary": [],
            "data_quality": data_quality,
        }

        # 출력 디렉토리 생성
        dated_dir = REPORT_DIR / str(trade_date) / session
        dated_dir.mkdir(parents=True, exist_ok=True)
        REPORT_DIR.mkdir(parents=True, exist_ok=True)

        # 파일 저장
        def _write(path: Path, content: str) -> None:
            path.write_text(content, encoding="utf-8")
            logger.info("[PNL_REPORT][WRITE] path=%s bytes=%s", path, len(content))

        _write(REPORT_DIR / "latest_portfolio_pnl.md", md_content)
        _write(REPORT_DIR / "latest_portfolio_pnl.json", json.dumps(json_data, ensure_ascii=False, indent=2, default=str))
        _write(dated_dir / "portfolio_pnl.md", md_content)
        _write(dated_dir / "portfolio_pnl.json", json.dumps(json_data, ensure_ascii=False, indent=2, default=str))

        # CSV
        if holdings:
            csv_path = REPORT_DIR / "latest_portfolio_pnl.csv"
            dated_csv = dated_dir / "portfolio_pnl.csv"
            fieldnames = list(holdings[0].keys())
            for p in [csv_path, dated_csv]:
                with open(p, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(holdings)
            logger.info("[PNL_REPORT][CSV] rows=%s", len(holdings))

        logger.info(
            "[PNL_REPORT][DONE] positions=%s total_cost=%s market_value=%s unrealized_pnl=%s",
            summary["total_positions"],
            int(summary["total_cost"]),
            int(summary["market_value"]),
            int(summary["unrealized_pnl"]),
        )
        return 0

    except Exception as exc:
        logger.error("[PNL_REPORT][ERROR] %s", traceback.format_exc())
        reason = type(exc).__name__ + ":" + str(exc)[:80]
        md_fail = _failure_report(reason)
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "latest_portfolio_pnl.md").write_text(md_fail, encoding="utf-8")
        return 0  # workflow 전체 실패 금지


if __name__ == "__main__":
    sys.exit(main())
