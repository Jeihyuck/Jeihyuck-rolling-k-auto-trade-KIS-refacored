# FILE: trader/execution.py
from __future__ import annotations
import logging
import time
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

@dataclass
class ExecParams:
    timeout_sec: int = 2           # 2초 미체결 시 시장가 전환
    spread_reprice_pct: int = 1    # 스프레드 1% 초과 시 재가격
    iceberg_parts: int = 3         # 아이스버그 3회(33%씩)


def _split_qty(qty: int, parts: int) -> list[int]:
    base = qty // parts
    rem = qty % parts
    out = [base] * parts
    for i in range(rem):
        out[i] += 1
    return [x for x in out if x > 0]


def smart_buy(kis, code: str, qty: int, params: ExecParams, reason: str = "") -> list[str]:
    """지정가±틱 → timeout → 시장가 전환, 스프레드 크면 아이스버그 분할."""
    oids = []
    try:
        # 간소화: 바로 시장가(실전은 지정가+틱 시도 후 timeout)
        parts = _split_qty(qty, params.iceberg_parts)
        for i, q in enumerate(parts, 1):
            oid = kis.buy_market(code, q, reason=reason or f"ICEBERG_{i}/{len(parts)}")
            oids.append(oid)
            log.info(f"[BUY] {code} x{q} oid={oid}")
            time.sleep(0.2)
    except Exception:
        log.exception(f"[EXEC][BUY][ERR] {code}")
    return oids


def smart_sell(kis, code: str, qty: int, params: ExecParams, reason: str = "") -> list[str]:
    oids = []
    try:
        parts = _split_qty(qty, params.iceberg_parts)
        for i, q in enumerate(parts, 1):
            oid = kis.sell_market(code, q, reason=reason or f"ICEBERG_{i}/{len(parts)}")
            oids.append(oid)
            log.info(f"[SELL] {code} x{q} oid={oid}")
            time.sleep(0.2)
    except Exception:
        log.exception(f"[EXEC][SELL][ERR] {code}")
    return oids
