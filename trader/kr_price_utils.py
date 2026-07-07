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
