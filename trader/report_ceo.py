# -*- coding: utf-8 -*-
# report_ceo.py — 거래 요약/리포트 생성(로컬 로그 기반)

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import logging

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# 데이터 구조
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class TradeRow:
    dt: datetime
    code: str
    side: str                 # BUY / SELL
    qty: int
    price: Optional[float]    # 'price' or 'fill_price'
    order_price: Optional[float]
    pnl_pct: Optional[float]
    profit: Optional[float]   # 원화 손익 (가능 시)
    name: Optional[str] = None
    reason: Optional[str] = None
    raw: Dict[str, Any] = None

@dataclass
class CodeAgg:
    code: str
    name: Optional[str]
    buy_qty: int = 0
    sell_qty: int = 0
    gross_profit: float = 0.0
    wins: int = 0
    losses: int = 0
    last_reason: Optional[str] = None

@dataclass
class SummaryAgg:
    n_trades: int = 0
    n_buy: int = 0
    n_sell: int = 0
    wins: int = 0
    losses: int = 0
    gross_profit: float = 0.0
    avg_pnl_pct: Optional[float] = None


# ──────────────────────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────────────────────

def _parse_trade_line(line: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(line)
    except Exception:
        return None

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        f = float(x)
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None

def _to_int(x: Any) -> int:
    try:
        i = int(float(x))
        return i
    except Exception:
        return 0

def _pick_price(obj: Dict[str, Any]) -> Optional[float]:
    """
    로그 포맷이 case-by-case라서, 우선순위로 체결가 후보를 고른다.
    """
    for k in ("fill_price", "price"):
        v = _to_float(obj.get(k))
        if v and v > 0:
            return v
    # 일부 result.output.prdt_price 경로
    try:
        v = _to_float(((obj.get("result") or {}).get("output") or {}).get("prdt_price"))
        if v and v > 0:
            return v
    except Exception:
        pass
    return None

def _collect_trade_rows(start_dt_kst: datetime, end_dt_kst: datetime) -> List[TradeRow]:
    """
    기간 내 trades_YYYY-MM-DD.json 파일들을 읽어 TradeRow 배열로 만든다.
    """
    rows: List[TradeRow] = []

    d = start_dt_kst.date()
    while d <= end_dt_kst.date():
        fpath = LOG_DIR / f"trades_{d.strftime('%Y-%m-%d')}.json"
        if fpath.exists():
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    for line in f:
                        rec = _parse_trade_line(line)
                        if not rec or not isinstance(rec, dict):
                            continue
                        # 시간
                        ts = rec.get("datetime") or rec.get("time") or rec.get("ts")
                        try:
                            dt = datetime.fromisoformat(ts) if isinstance(ts, str) else None
                        except Exception:
                            dt = None
                        if dt is None:
                            # 파일 날짜라도 사용
                            dt = datetime(d.year, d.month, d.day, tzinfo=KST)
                        elif dt.tzinfo is None:
                            dt = dt.replace(tzinfo=KST)

                        if not (start_dt_kst <= dt <= end_dt_kst + timedelta(seconds=86399)):
                            continue

                        side = str(rec.get("side") or "").upper()
                        if side not in ("BUY", "SELL"):
                            continue

                        code = str(rec.get("code") or "").strip()
                        if not code:
                            continue

                        qty = _to_int(rec.get("qty"))
                        name = rec.get("name")
                        # 가격 후보
                        price = _pick_price(rec)
                        order_price = _to_float(rec.get("order_price"))
                        pnl_pct = _to_float(rec.get("pnl_pct"))
                        profit = _to_float(rec.get("profit"))
                        reason = rec.get("reason")

                        rows.append(
                            TradeRow(
                                dt=dt, code=code, side=side, qty=qty, price=price,
                                order_price=order_price, pnl_pct=pnl_pct, profit=profit,
                                name=name, reason=reason, raw=rec
                            )
                        )
            except Exception as e:
                logger.warning(f"[REPORT] 로그 파일 읽기 실패: {fpath.name} err={e}")
        d += timedelta(days=1)

    rows.sort(key=lambda r: (r.dt, r.code, r.side))
    return rows

def _aggregate(rows: List[TradeRow]) -> Tuple[SummaryAgg, Dict[str, CodeAgg]]:
    summ = SummaryAgg()
    by_code: Dict[str, CodeAgg] = {}
    pnl_pct_accum = 0.0
    pnl_pct_count = 0

    for r in rows:
        summ.n_trades += 1
        if r.side == "BUY":
            summ.n_buy += 1
        elif r.side == "SELL":
            summ.n_sell += 1

        ca = by_code.get(r.code)
        if ca is None:
            ca = CodeAgg(code=r.code, name=r.name)
            by_code[r.code] = ca

        if r.side == "BUY":
            ca.buy_qty += max(0, r.qty)
        else:
            ca.sell_qty += max(0, r.qty)

        # 수익률 집계(가능 시)
        if r.pnl_pct is not None:
            pnl_pct_accum += r.pnl_pct
            pnl_pct_count += 1

        # 손익 집계: SELL 로그에 profit이 기록되는 경우가 많음.
        realized = None
        if r.side == "SELL":
            if r.profit is not None:
                realized = r.profit
            else:
                # 백업: price와 raw.buy_price가 있다면 계산해보지만
                # 대부분의 케이스에서 매수평균이 로그에 없으므로 보수적으로 0 처리.
                realized = None

            if realized is not None:
                summ.gross_profit += realized
                ca.gross_profit += realized
                if realized >= 0:
                    summ.wins += 1
                    ca.wins += 1
                else:
                    summ.losses += 1
                    ca.losses += 1

        ca.last_reason = r.reason or ca.last_reason

    if pnl_pct_count > 0:
        summ.avg_pnl_pct = round(pnl_pct_accum / pnl_pct_count, 2)
    else:
        summ.avg_pnl_pct = None

    return summ, by_code

def _period_range(base_dt_kst: datetime, period: str) -> Tuple[datetime, datetime]:
    p = (period or "daily").lower()
    if p == "weekly":
        # 월요일~일요일 범위로 보고서(혹은 최근 7일): 여기선 최근 7일로 단순화
        end_dt = base_dt_kst.replace(hour=23, minute=59, second=59, microsecond=0)
        start_dt = (end_dt - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start_dt, end_dt
    if p == "monthly":
        end_dt = base_dt_kst.replace(hour=23, minute=59, second=59, microsecond=0)
        first_day = date(end_dt.year, end_dt.month, 1)
        start_dt = datetime(first_day.year, first_day.month, first_day.day, tzinfo=KST)
        return start_dt, end_dt
    # daily
    day = base_dt_kst.date()
    start_dt = datetime(day.year, day.month, day.day, tzinfo=KST)
    end_dt = start_dt.replace(hour=23, minute=59, second=59)
    return start_dt, end_dt

def _fmt_krw(v: Optional[float]) -> str:
    if v is None:
        return "-"
    try:
        return f"{int(round(v)):,.0f}원"
    except Exception:
        return str(v)

def _mk_md_table(rows: List[List[str]]) -> str:
    if not rows:
        return ""
    head = rows[0]
    body = rows[1:]
    md = "| " + " | ".join(head) + " |\n"
    md += "| " + " | ".join(["---"] * len(head)) + " |\n"
    for r in body:
        md += "| " + " | ".join(r) + " |\n"
    return md

# ──────────────────────────────────────────────────────────────────────────────
# 메인: CEO 리포트
# ──────────────────────────────────────────────────────────────────────────────

def ceo_report(base_dt_kst: Optional[datetime] = None, period: str = "daily") -> Dict[str, Any]:
    """
    거래 로그(trades_YYYY-MM-DD.json) 기반 요약 리포트를 생성하고 저장한다.
    Args:
        base_dt_kst: 기준 시각(KST). None이면 현재 KST 사용.
        period: 'daily' | 'weekly' | 'monthly'
    Returns:
        {
          "title": "...",
          "period": "...",
          "start": "YYYY-MM-DD",
          "end":   "YYYY-MM-DD",
          "path":  "<markdown path>",
          "summary": {...},
          "top_winners": [...],
          "top_losers":  [...]
        }
    """
    base_dt_kst = base_dt_kst or datetime.now(KST)
    start_dt, end_dt = _period_range(base_dt_kst, period)

    rows = _collect_trade_rows(start_dt, end_dt)
    summ, by_code = _aggregate(rows)

    # 상/하위 종목(손익 기준)
    code_aggs = list(by_code.values())
    code_aggs.sort(key=lambda x: x.gross_profit, reverse=True)
    top_w = code_aggs[:5]
    top_l = list(reversed(code_aggs))[:5]  # 손익 오름차순의 상위 5개

    # 마크다운 생성
    title = f"CEO Report — {end_dt.strftime('%Y-%m-%d')} ({period})"
    md_lines: List[str] = []
    md_lines.append(f"# {title}")
    md_lines.append("")
    md_lines.append(f"- 기간: **{start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')}** (KST)")
    md_lines.append(f"- 총 거래수: **{summ.n_trades}**건 (BUY {summ.n_buy} / SELL {summ.n_sell})")
    md_lines.append(f"- 승/패: **{summ.wins} / {summ.losses}**")
    md_lines.append(f"- 총 손익: **{_fmt_krw(summ.gross_profit)}**")
    md_lines.append(f"- 평균 수익률: **{str(summ.avg_pnl_pct) + '%' if summ.avg_pnl_pct is not None else '-'}**")
    md_lines.append("")

    # Top Winners
    md_lines.append("## Top Winners (by Profit)")
    if top_w:
        rows_w = [["코드", "종목명", "손익", "승/패", "비고"]]
        for c in top_w:
            rows_w.append([
                c.code,
                c.name or "-",
                _fmt_krw(c.gross_profit),
                f"{c.wins}/{c.losses}",
                (c.last_reason or "").replace("|", "/")
            ])
        md_lines.append(_mk_md_table(rows_w))
    else:
        md_lines.append("> 데이터 없음")
    md_lines.append("")

    # Top Losers
    md_lines.append("## Top Losers (by Profit)")
    if top_l:
        rows_l = [["코드", "종목명", "손익", "승/패", "비고"]]
        for c in top_l:
            rows_l.append([
                c.code,
                c.name or "-",
                _fmt_krw(c.gross_profit),
                f"{c.wins}/{c.losses}",
                (c.last_reason or "").replace("|", "/")
            ])
        md_lines.append(_mk_md_table(rows_l))
    else:
        md_lines.append("> 데이터 없음")
    md_lines.append("")

    # 거래 상세(선택)
    md_lines.append("## 거래 상세 (최근 순)")
    if rows:
        rows_d = [["시간", "코드", "종목명", "Side", "수량", "체결가", "손익(원)", "사유"]]
        for r in reversed(rows[-200:]):  # 너무 길어지는 걸 방지: 최근 200건만
            rows_d.append([
                r.dt.strftime("%Y-%m-%d %H:%M:%S"),
                r.code,
                r.name or "-",
                r.side,
                str(r.qty),
                f"{r.price:.2f}" if r.price is not None else "-",
                _fmt_krw(r.profit) if (r.side == "SELL" and r.profit is not None) else "-",
                (r.reason or "").replace("|", "/")
            ])
        md_lines.append(_mk_md_table(rows_d))
    else:
        md_lines.append("> 해당 기간 거래 없음")
    md_lines.append("")

    # 저장
    md_name = f"CEO_Report_{end_dt.strftime('%Y%m%d')}_{period}.md"
    md_path = LOG_DIR / md_name
    try:
        md_path.write_text("\n".join(md_lines), encoding="utf-8")
        logger.info(f"[CEO REPORT] 마크다운 저장: {md_path}")
    except Exception as e:
        logger.error(f"[CEO REPORT] 저장 실패: {e}")

    # 반환 payload
    payload = {
        "title": title,
        "period": period,
        "start": start_dt.strftime("%Y-%m-%d"),
        "end": end_dt.strftime("%Y-%m-%d"),
        "path": str(md_path),
        "summary": asdict(summ),
        "top_winners": [asdict(x) for x in top_w],
        "top_losers": [asdict(x) for x in top_l],
    }
    return payload


# ──────────────────────────────────────────────────────────────────────────────
# CLI 테스트용
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    out = ceo_report(datetime.now(KST), period=os.getenv("CEO_REPORT_PERIOD", "daily"))
    print(json.dumps(out, ensure_ascii=False, indent=2))
