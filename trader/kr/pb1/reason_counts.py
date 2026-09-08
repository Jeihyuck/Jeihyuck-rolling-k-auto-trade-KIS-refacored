from __future__ import annotations

from collections import Counter
from typing import Iterable

ENTRY_BLOCK_REASON_MAP = {
    "SIZING_CAP_BELOW_ONE_SHARE": "SIZING_CAP_BELOW_ONE_SHARE",
    "SIZING_MIN_ORDER_NOTIONAL_FAIL": "SIZING_MIN_ORDER_NOTIONAL_FAIL",
    "SIZING_QTY_ZERO": "SIZING_QTY_ZERO",
    "cap_below_min_order": "MIN_ORDER_KRW",
    "min_order_krw": "MIN_ORDER_KRW",
    "cap_below_one_share": "MIN_ORDER_KRW",
    "planned_qty_zero_or_min_order": "MIN_ORDER_KRW",
    "unaffordable_min1share": "MIN_ORDER_KRW",
    "order_price_missing": "PRICE_MISSING",
    "available_cash_zero": "NO_CASH",
    "insufficient_cash": "NO_CASH",
    "entry_capital_zero": "NO_CASH",
    "tick_budget_zero": "NO_CASH",
    "entry_cap_exceeded": "NO_CASH",
    "BUYABLE_EXISTING_HOLDING": "BUYABLE_EXISTING_HOLDING",
    "BUYABLE_OPEN_ORDER": "BUYABLE_OPEN_ORDER",
    "BUYABLE_TODAY_BUY_EXISTS": "BUYABLE_TODAY_BUY_EXISTS",
    "BUYABLE_TODAY_SELL_REBUY_BLOCKED": "BUYABLE_TODAY_SELL_REBUY_BLOCKED",
    "BUYABLE_COOLDOWN": "BUYABLE_COOLDOWN",
    "BUYABLE_DUPLICATE": "BUYABLE_DUPLICATE",
    "BUYABLE_WINDOW_BLOCK": "BUYABLE_WINDOW_BLOCK",
    "open_order": "RATE_LIMIT",
    "today_buy_exists": "RATE_LIMIT",
    "duplicate_order": "DUPLICATE",
    "rate_limit": "RATE_LIMIT",
    "entry_cutoff": "CUTOFF",
    "entry_disabled": "ENTRY_DISABLED",
}



def _normalize_entry_block_reasons(reasons: Iterable[str] | None) -> Counter[str]:
    counter: Counter[str] = Counter()
    for reason in reasons or []:
        mapped = ENTRY_BLOCK_REASON_MAP.get(reason, reason.upper())
        counter[mapped] += 1
    return counter


def _normalize_entry_block_counts(reason_counts: Counter[str]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for reason, count in reason_counts.items():
        mapped = ENTRY_BLOCK_REASON_MAP.get(reason, reason.upper())
        counter[mapped] += count
    return counter


def _format_reason_counts(counter: Counter[str]) -> str:
    if not counter:
        return "none"
    parts = [f"{key}:{count}" for key, count in counter.most_common()]
    return ",".join(parts)


def _summarize_blocked_reasons(counter: Counter[str] | dict[str, int] | None) -> Counter[str]:
    normalized_input = Counter(counter or {})
    return _normalize_entry_block_counts(normalized_input)
