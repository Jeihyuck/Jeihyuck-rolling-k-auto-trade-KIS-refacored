# -*- coding: utf-8 -*-
from __future__ import annotations


def krx_tick(price: float) -> int:
    p = float(price or 0)
    if p >= 500_000:
        return 1_000
    if p >= 100_000:
        return 500
    if p >= 50_000:
        return 100
    if p >= 10_000:
        return 50
    if p >= 5_000:
        return 10
    if p >= 1_000:
        return 5
    return 1


def normalize_kr_order_price(price: float, *, side: str = "BUY") -> tuple[int, int]:
    tick = krx_tick(price)
    q = float(price or 0) / tick
    if str(side or "").upper() == "SELL":
        normalized = int(q) * tick
    else:
        normalized = (int(q) if q == int(q) else int(q) + 1) * tick
    return int(normalized), int(tick)


def round_to_tick(price: float, mode: str = "nearest") -> int:
    if price is None or price <= 0:
        return 0

    mode_norm = str(mode or "nearest").lower()

    if mode_norm == "down":
        normalized, _tick = normalize_kr_order_price(price, side="SELL")
        return int(normalized)

    if mode_norm == "up":
        normalized, _tick = normalize_kr_order_price(price, side="BUY")
        return int(normalized)

    tick = krx_tick(price)
    q = float(price) / tick
    return int(int(q + 0.5) * tick)


def etf_tick(price: float) -> int:
    """KRX ETF quotation unit: 1 KRW below 2,000; otherwise 5 KRW.

    This is the ETF product rule, independent of the ordinary KOSPI/KOSDAQ
    equity price bands. A *known* ETF symbol must use this path explicitly.
    Source: KRX securities products / ETF trading regulations.
    """
    return 1 if float(price or 0) < 2_000 else 5


def normalize_kr_etf_order_price(price: float, *, side: str = "BUY") -> tuple[int, int]:
    """Round an ETF limit to the exact KRX-accepted tick without inventing ASK."""
    p = float(price or 0)
    if not 0 < p < float("inf"):
        return 0, etf_tick(p)
    tick = etf_tick(p)
    q = p / tick
    if str(side or "").upper() == "SELL":
        normalized = int(q) * tick
    else:
        normalized = (int(q) if q == int(q) else int(q) + 1) * tick
    return int(normalized), int(tick)
