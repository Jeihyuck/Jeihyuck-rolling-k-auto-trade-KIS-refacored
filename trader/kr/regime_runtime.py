"""Durable KR intraday regime state and breadth artifacts."""
from __future__ import annotations
from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any

from trader.kr.regime import MARKETS, normalize_kr_market

logger = logging.getLogger(__name__)
RUNTIME_STATE_PATH = Path("runtime/state/kr/kr_regime_runtime_state.json")
BREADTH_PATH = Path("artifacts/kr_market_breadth.json")
REQUIRED_BREADTH_FIELDS = {"market", "close", "ma20", "ma50", "return_1d", "return_5d"}


def atomic_json_write(path: str | Path, payload: dict[str, Any]) -> Path:
    target = Path(path); target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(target)
    return target


def load_runtime_state(trade_date: str, path: str | Path = RUNTIME_STATE_PATH, *, now: datetime | None = None, max_age_seconds: int = 900) -> dict[str, Any]:
    target = Path(path)
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {"trade_date": trade_date, "markets": {}}
    if str(data.get("trade_date")) != str(trade_date):
        return {"trade_date": trade_date, "markets": {}}
    if now and data.get("updated_at_kst"):
        try:
            updated = datetime.fromisoformat(str(data["updated_at_kst"]))
            if (now - updated).total_seconds() > max_age_seconds or (now - updated).total_seconds() < -60:
                logger.warning("[KR_REGIME][RUNTIME_STATE_STALE] updated_at=%s now=%s", updated, now)
                return {"trade_date": trade_date, "markets": {}}
        except (ValueError, TypeError):
            return {"trade_date": trade_date, "markets": {}}
    return data


def save_runtime_state(trade_date: str, now: datetime, markets: dict[str, dict[str, Any]], path: str | Path = RUNTIME_STATE_PATH) -> Path:
    return atomic_json_write(path, {"trade_date": str(trade_date), "updated_at_kst": now.isoformat(), "markets": markets})


def time_adjusted_turnover(*, accumulated_volume: float, avg_daily_volume: float, now: datetime,
                           expected_volume_until_now: float | None = None,
                           curve_fraction: float | None = None) -> tuple[float, str]:
    if expected_volume_until_now and expected_volume_until_now > 0:
        return accumulated_volume / expected_volume_until_now, "same_time_history"
    if not curve_fraction:
        minute = now.hour * 60 + now.minute
        progress = max(.01, min(1.0, (minute - 540) / 390.0))
        # U-shaped intraday curve approximation: more volume is expected near open.
        curve_fraction = min(1.0, .55 * (progress ** .55) + .45 * progress)
    expected = max(1.0, avg_daily_volume * curve_fraction)
    return accumulated_volume / expected, "intraday_curve_fallback"


def calculate_market_breadth(rows: list[dict[str, Any]], *, as_of: str, source: str, path: str | Path = BREADTH_PATH, min_sample: int = 30) -> dict[str, Any]:
    result: dict[str, Any] = {"as_of": as_of, "source": source, "markets": {}}
    for market in MARKETS:
        selected = [r for r in rows if normalize_kr_market(r.get("market") or r.get("market_code")) == market]
        missing = sorted({field for r in selected for field in REQUIRED_BREADTH_FIELDS if r.get(field) is None})
        if not selected or missing:
            logger.warning("[KR_REGIME][BREADTH_INPUT_BLOCKED] market=%s missing_fields=%s sample_size=%s", market, ",".join(missing or ["market_rows"]), len(selected))
            result["markets"][market] = {"sample_size": len(selected), "data_quality": "BLOCKED", "missing_fields": missing or ["market_rows"]}
            continue
        values = lambda key: [float(r[key]) for r in selected]
        import numpy as np
        close, ma20, ma50 = values("close"), values("ma20"), values("ma50")
        r1, r5 = values("return_1d"), values("return_5d")
        quality = "OK" if len(selected) >= min_sample and source != "final30_fallback" else "DEGRADED"
        result["markets"][market] = {
            "sample_size": len(selected), "data_quality": quality,
            "breadth_ma20": sum(c > m for c, m in zip(close, ma20)) / len(selected),
            "breadth_ma50": sum(c > m for c, m in zip(close, ma50)) / len(selected),
            "advance_ratio": sum(v > 0 for v in r1) / len(selected),
            "positive_return_ratio": sum(v > 0 for v in r1) / len(selected),
            "median_return_1d": float(np.median(r1)), "median_return_5d": float(np.median(r5)),
        }
        logger.info("[KR_REGIME][BREADTH] market=%s sample_size=%s ma20=%.3f ma50=%.3f advance=%.3f", market, len(selected), result["markets"][market]["breadth_ma20"], result["markets"][market]["breadth_ma50"], result["markets"][market]["advance_ratio"])
    overall = "BLOCKED" if all(v.get("data_quality") == "BLOCKED" for v in result["markets"].values()) else "DEGRADED" if any(v.get("data_quality") != "OK" for v in result["markets"].values()) else "OK"
    result["data_quality"] = overall
    logger.info("[KR_REGIME][BREADTH_SOURCE] source=%s quality=%s", source, overall)
    atomic_json_write(path, result)
    return result
