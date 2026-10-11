"""KR candidate admission: preserve independently qualified setups before the pool cap.

This module does *candidate* screening, not a live order authorization.
A final KR style must still pass its existing owner, score, and BUY gates.
"""
from __future__ import annotations

import math


def completed_daily_candidate_proofs(df) -> dict[str, bool]:
    """Conservative OHLCV screens for three KR entry families.

    Verified Minervini/VCP remains solely controlled by the existing
    derived-Minervini proof bridge, not the scanner's approximate VCP score.
    """
    empty = {"PULLBACK": False, "MOMENTUM": False, "BREAKOUT": False, "VCP": False}
    if df is None or len(df) < 63:
        return empty
    needed = {"close", "high", "low", "volume"}
    if not needed.issubset(df.columns):
        return empty
    bars = df.tail(63)
    try:
        closes = [float(v) for v in bars["close"]]
        highs = [float(v) for v in bars["high"]]
        lows = [float(v) for v in bars["low"]]
        vols = [float(v) for v in bars["volume"]]
    except (TypeError, ValueError):
        return empty
    if any(not math.isfinite(v) or v <= 0 for values in (closes, highs, lows, vols) for v in values):
        return empty
    if any(not (h >= c >= l) for h, c, l in zip(highs, closes, lows)):
        return empty
    close, volume = closes[-1], vols[-1]
    ma20 = sum(closes[-20:]) / 20
    ma50 = sum(closes[-50:]) / 50
    high55 = max(highs[-56:-1])
    avg_vol20 = sum(vols[-21:-1]) / 20
    pullback_depth = (high55 - close) / high55
    return {
        "PULLBACK": bool(close >= ma50 and 0.03 <= pullback_depth <= 0.18 and volume < avg_vol20),
        "MOMENTUM": bool(close >= ma50 and close / closes[-21] - 1 >= .05 and close / closes[-61] - 1 >= .05),
        "BREAKOUT": bool(close > high55 and volume >= 1.5 * avg_vol20),
        "VCP": False,
    }


def merge_verified_candidate_screens(
    legacy_selected: list[dict],
    prefilter_survivors: list[dict],
    *,
    target_size: int,
) -> tuple[list[dict], dict]:
    """Union verified screens with legacy choices; fail instead of silently truncating signals."""
    if target_size <= 0:
        raise ValueError("four_family_pool_target_nonpositive")
    by_code = {}
    for row in prefilter_survivors:
        code = str(row.get("code") or "").zfill(6)
        if code != "000000":
            by_code[code] = row
    proven = [
        row for row in by_code.values()
        if any((row.get("candidate_family_screens") or {}).values())
    ]
    proven.sort(key=lambda r: (-float(r.get("score") or 0), str(r.get("code") or "")))
    if len(proven) > target_size:
        raise RuntimeError(f"FOUR_FAMILY_POOL_CAP_INSUFFICIENT proof_count={len(proven)} target={target_size}")
    selected, seen = list(proven), {str(r.get("code") or "").zfill(6) for r in proven}
    for row in legacy_selected:
        code = str(row.get("code") or "").zfill(6)
        if code not in seen and code in by_code and len(selected) < target_size:
            selected.append(row)
            seen.add(code)
    # Fill from equally screened survivors only if legacy selection had a shortage.
    original_target = min(target_size, max(len(legacy_selected), len(proven)))
    if len(selected) < original_target:
        for row in sorted(by_code.values(), key=lambda r: (-float(r.get("score") or 0), str(r.get("code") or ""))):
            code = str(row.get("code") or "").zfill(6)
            if code not in seen:
                selected.append(row)
                seen.add(code)
            if len(selected) >= original_target:
                break
    return selected, {
        "eligible_by_family": {
            family: sum(bool((r.get("candidate_family_screens") or {}).get(family)) for r in by_code.values())
            for family in ("PULLBACK", "MOMENTUM", "BREAKOUT", "VCP")
        },
        "eligible_total": len(proven),
        "protected_count": sum(str(r.get("code") or "").zfill(6) in seen for r in proven),
        "legacy_count": len(legacy_selected),
        "selected_count": len(selected),
    }
