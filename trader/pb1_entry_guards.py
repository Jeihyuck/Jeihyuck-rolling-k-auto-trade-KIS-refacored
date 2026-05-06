"""
pb1_entry_guards.py
===================
Pure function for Korean market code-level entry guards.

Policy (as of 2026-05-06):
- NO global entry blocking after any sell
- Only sold codes blocked from same-day re-entry
- Only codes with open BUY orders blocked from duplicate BUY
- All other candidates proceed to normal PB1 evaluation

Guards applied:
1. SAME_DAY_SELL_REENTRY_BLOCK - code sold today
2. OPEN_BUY_ORDER_SAME_CODE - code has pending BUY order

Returns:
- global_entry_blocked: Always False (no global blocking)
- allowed: List of codes cleared for entry eval
- skipped: List of codes blocked at code level
- results: Dict mapping code -> {status, reason}
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


def normalize_code(value: Any) -> str:
    """
    Normalize stock code to 6-digit zero-padded string.
    
    Args:
        value: Raw code value (str, int, or other)
        
    Returns:
        6-digit zero-padded code string, or empty string if invalid
    """
    code = str(value or "").strip()
    if not code:
        return ""
    # Remove any non-digit prefix if present
    code = "".join(c for c in code if c.isdigit())
    if not code:
        return ""
    return code.zfill(6)


def apply_kr_candidate_order_guards(
    candidates: list[dict],
    *,
    sold_codes_today: set[str],
    open_buy_codes: set[str],
) -> dict:
    """
    Apply Korean market code-level order guards to entry candidates.
    
    Args:
        candidates: List of candidate dicts (must have 'code' key)
        sold_codes_today: Set of codes sold today (any format, will be normalized)
        open_buy_codes: Set of codes with open BUY orders (any format, will be normalized)
        
    Returns:
        dict with:
            global_entry_blocked: bool (always False)
            allowed: list[str] - codes cleared for eval
            skipped: list[str] - codes blocked at code level
            results: dict[str, dict] - code -> {status, reason}
    """
    # Normalize sold and open buy code sets
    sold = {normalize_code(c) for c in sold_codes_today if normalize_code(c)}
    open_buy = {normalize_code(c) for c in open_buy_codes if normalize_code(c)}
    
    logger.info(
        "[ENTRY][GUARD][CODE_LEVEL][LOAD] sold_codes=%s open_buy_codes=%s",
        len(sold),
        len(open_buy),
    )
    
    results = {}
    allowed = []
    skipped = []
    
    for raw in candidates:
        # Extract code from candidate (handle dict or object)
        if isinstance(raw, dict):
            code = normalize_code(raw.get("code"))
        else:
            code = normalize_code(getattr(raw, "code", ""))
            
        if not code:
            logger.warning("[ENTRY][GUARD][CODE_LEVEL][SKIP] reason=MISSING_CODE")
            continue
            
        # Guard 1: Same-day sell reentry block
        if code in sold:
            results[code] = {
                "status": "SKIP",
                "reason": "SAME_DAY_SELL_REENTRY_BLOCK",
            }
            skipped.append(code)
            logger.debug(
                "[ENTRY][CANDIDATE][SKIP] code=%s reason=SAME_DAY_SELL_REENTRY_BLOCK",
                code,
            )
            continue
            
        # Guard 2: Open BUY order same code
        if code in open_buy:
            results[code] = {
                "status": "SKIP",
                "reason": "OPEN_BUY_ORDER_SAME_CODE",
            }
            skipped.append(code)
            logger.debug(
                "[ENTRY][CANDIDATE][SKIP] code=%s reason=OPEN_BUY_ORDER_SAME_CODE",
                code,
            )
            continue
            
        # Cleared for normal entry evaluation
        results[code] = {
            "status": "EVAL",
            "reason": "CLEAR",
        }
        allowed.append(code)
        
    logger.info(
        "[ENTRY][GUARD][CODE_LEVEL][RESULT] allowed=%s skipped=%s total=%s",
        len(allowed),
        len(skipped),
        len(candidates),
    )
    
    return {
        "global_entry_blocked": False,
        "allowed": allowed,
        "skipped": skipped,
        "results": results,
    }
