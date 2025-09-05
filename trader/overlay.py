# FILE: trader/overlay.py
from __future__ import annotations
from dataclasses import dataclass

@dataclass
class OverlayDecision:
    carry_over: bool
    carry_frac: float  # 0.4 ~ 0.6 권장
    reason: str


def decide_carry_over(hit_tp1: bool, close: float, day_high: float, atr: float,
                       close_ge_ma20: bool, close_ge_vwap: bool,
                       volume_rank_pct: int, had_cutoff: bool, carry_days: int,
                       carry_max_days: int = 3) -> OverlayDecision:
    score = 0
    if hit_tp1:
        score += 1
    if close >= day_high - atr * 0.2:
        score += 1
    if close_ge_ma20 and close_ge_vwap:
        score += 1
    if volume_rank_pct <= 30:
        score += 1
    if not had_cutoff:
        score += 1
    if carry_days >= carry_max_days:
        return OverlayDecision(False, 0.0, f"MAX_DAYS:{carry_days}")
    carry = score >= 3
    frac = 0.5 if hit_tp1 else 0.4
    return OverlayDecision(carry, frac, f"SCORE={score}")
