from collections import Counter

from trader.kr.pb1.reason_counts import (
    _format_reason_counts,
    _normalize_entry_block_counts,
    _normalize_entry_block_reasons,
    _summarize_blocked_reasons,
)
from trader.pb1_engine import (
    _format_reason_counts as facade_format_reason_counts,
    _normalize_entry_block_counts as facade_normalize_entry_block_counts,
    _normalize_entry_block_reasons as facade_normalize_entry_block_reasons,
    _summarize_blocked_reasons as facade_summarize_blocked_reasons,
)


def test_reason_counts_module_matches_facade():
    reasons = ["BUYABLE_TODAY_BUY_EXISTS", "entry_disabled", "rate_limit", "rate_limit"]
    counter = Counter({"BUYABLE_TODAY_BUY_EXISTS": 2, "MIN_ORDER_KRW": 1, "entry_disabled": 3})

    assert _normalize_entry_block_reasons(reasons) == facade_normalize_entry_block_reasons(reasons)
    assert _normalize_entry_block_counts(counter) == facade_normalize_entry_block_counts(counter)
    assert _summarize_blocked_reasons(counter) == facade_summarize_blocked_reasons(counter)
    assert _format_reason_counts(_normalize_entry_block_reasons(reasons)) == facade_format_reason_counts(
        facade_normalize_entry_block_reasons(reasons)
    )


def test_reason_counts_formatting_is_stable():
    counter = Counter({"ORDER_SKIP_BUYABLE_TODAY_BUY_EXISTS": 2, "MIN_ORDER_KRW": 1})
    assert _format_reason_counts(counter) == "ORDER_SKIP_BUYABLE_TODAY_BUY_EXISTS:2,MIN_ORDER_KRW:1"
    assert _format_reason_counts(Counter()) == "none"
