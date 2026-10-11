"""KR candidate admission: preserve independently qualified setups before the pool cap.

This module does *candidate* screening, not a live order authorization.
A final KR style must still pass its existing owner, score, and BUY gates.
"""
from __future__ import annotations

import math


def _bar_date_key(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    raw = str(value).strip()
    if len(raw) >= 10 and raw[4] in {"-", "/"} and raw[7] in {"-", "/"}:
        return raw[:10].replace("-", "").replace("/", "")
    if len(raw) >= 8 and raw[:8].isdigit():
        return raw[:8]
    return ""


def completed_breakout_evidence(df, *, expected_as_of) -> dict:
    """Return auditable, completed-bar 55D breakout inputs, never a BUY pass."""
    screens = completed_daily_candidate_proofs(df, expected_as_of=expected_as_of)
    if not screens["BREAKOUT"]:
        return {}
    bars = df.tail(63)
    high20 = max(float(v) for v in bars["high"].iloc[-21:-1])
    pivot55 = max(float(v) for v in bars["high"].iloc[-56:-1])
    price = float(bars["close"].iloc[-1])
    volume = float(bars["volume"].iloc[-1])
    avgvol20 = sum(float(v) for v in bars["volume"].iloc[-21:-1]) / 20.0
    return {
        "source": "completed_daily_ohlcv",
        "as_of": _bar_date_key(expected_as_of),
        "close": price,
        "volume": volume,
        "average_volume20": avgvol20,
        "prior_high20": high20,
        "prior_high55": pivot55,
        "pivot55": pivot55,
        "volume_ratio20": volume / avgvol20,
    }


def completed_daily_candidate_proofs(df, *, expected_as_of=None) -> dict[str, bool]:
    """Conservative OHLCV screens for three KR entry families.

    Verified Minervini/VCP remains solely controlled by the existing
    derived-Minervini proof bridge, not the scanner's approximate VCP score.
    """
    empty = {"PULLBACK": False, "MOMENTUM": False, "BREAKOUT": False, "VCP": False}
    if df is None or len(df) < 63:
        return empty
    if expected_as_of is not None:
        expected_date = _bar_date_key(expected_as_of)
        date_column = next((key for key in ("date", "xymd", "datetime", "stck_bsop_date")
                            if key in df.columns), None)
        if date_column is not None:
            actual_date = _bar_date_key(df[date_column].iloc[-1])
        else:
            # KIS/FinanceDataReader may provide trading days only as the
            # DataFrame datetime index. Never accept a numeric RangeIndex.
            actual_date = _bar_date_key(df.index[-1])
        if not expected_date or actual_date != expected_date:
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
    """Evaluate *all* verified setups; preserve strongest cross-family signals.

    The Top50 is a capacity constraint, not a strategy quota. Rank each proof
    against its own family's peers, and explicitly report qualified setups
    that could not be admitted. No downstream BUY authorization is implied.
    """
    if target_size <= 0:
        raise ValueError("four_family_pool_target_nonpositive")
    by_code: dict[str, dict] = {}
    for row in prefilter_survivors:
        code = str(row.get("code") or "").zfill(6)
        if code != "000000":
            by_code[code] = row
    families = ("PULLBACK", "MOMENTUM", "BREAKOUT", "VCP")
    proven = [r for r in by_code.values()
              if any((r.get("candidate_family_screens") or {}).values())]

    def quality(row: dict) -> float:
        # Same upstream quality scale within a candidate stage, only the
        # *within-family percentile* competes across different strategies.
        for key in ("tech_score", "candidate_score", "score"):
            val = row.get(key)
            try:
                number = float(val)
            except (ValueError, TypeError):
                continue
            if math.isfinite(number) and number > 0:
                return number
        return 0.0

    peers = {
        family: sorted(quality(r) for r in proven
                       if (r.get("candidate_family_screens") or {}).get(family))
        for family in families
    }

    for row in proven:
        quality_by_family = {}
        for family in families:
            if not (row.get("candidate_family_screens") or {}).get(family):
                continue
            distribution = peers[family]
            q = quality(row)
            quality_by_family[family] = round(
                sum(v <= q for v in distribution) / len(distribution), 6
            )
        row["candidate_family_quality_percentiles"] = quality_by_family

    proven.sort(key=lambda r: (
        -max(r["candidate_family_quality_percentiles"].values()),
        -quality(r),
        str(r.get("code") or ""),
    ))
    selected = proven[:target_size]
    seen = {str(r.get("code") or "").zfill(6) for r in selected}
    capacity_rejected = proven[target_size:]
    for row in capacity_rejected:
        row.setdefault("reject_reasons", []).append("qualified_but_capacity_rejected")
    for row in legacy_selected:
        code = str(row.get("code") or "").zfill(6)
        if code not in seen and code in by_code and len(selected) < target_size:
            selected.append(row)
            seen.add(code)
    original_target = min(target_size, max(len(legacy_selected), len(proven)))
    if len(selected) < original_target:
        for row in sorted(by_code.values(), key=lambda r: (-quality(r), str(r.get("code") or ""))):
            code = str(row.get("code") or "").zfill(6)
            if code not in seen:
                selected.append(row)
                seen.add(code)
            if len(selected) >= original_target:
                break
    return selected, {
        "eligible_by_family": {
            family: sum(bool((r.get("candidate_family_screens") or {}).get(family))
                        for r in by_code.values())
            for family in families
        },
        "eligible_total": len(proven),
        "protected_count": len(proven) - len(capacity_rejected),
        "capacity_rejected_count": len(capacity_rejected),
        "capacity_rejected_sample": [
            str(r.get("code") or "") for r in capacity_rejected[:12]
        ],
        "legacy_count": len(legacy_selected),
        "selected_count": len(selected),
        "fairness_mode": "verified_family_peer_percentile_no_quotas",
    }


def apply_verified_family_final_arbitration(rows: list[dict]) -> list[dict]:
    """One chosen qualifying style per candidate, no Pullback-only score bonus.

    Stage-level family percentiles are comparable across independent setups.
    Keep half of the original general quality/risk score when ranking Final30;
    the other half reflects the strongest *verified* family. Existing entry
    checks and owner-specific exit contracts still apply downstream.
    """
    for row in rows:
        family_ranks = dict(row.get("candidate_family_quality_percentiles") or {})
        screens = dict(row.get("candidate_family_screens") or {})
        eligible = {family: float(value) for family, value in family_ranks.items()
                    if screens.get(family) is True and math.isfinite(float(value))}
        if not eligible:
            continue  # Legacy fallback still goes through unchanged risk gates.
        winner = max(sorted(eligible), key=lambda family: eligible[family])
        base = float(row.get("score_final") or row.get("final_score") or 0.0)
        fair_score = round(base * 0.5 + 100.0 * eligible[winner] * 0.5, 4)
        row["entry_style_selected"] = winner
        row["score_final"] = fair_score
        row["final_score"] = fair_score
        row["selected_family_quality_percentile"] = eligible[winner]
        meta = row.get("meta")
        if isinstance(meta, dict):
            meta["entry_style_selected"] = winner
            meta["candidate_family_quality_percentiles"] = family_ranks
            meta["candidate_family_screens"] = screens
            meta["selected_family_quality_percentile"] = eligible[winner]
    return rows
