# -*- coding: utf-8 -*-
"""Immutable US STANDARD entry-to-exit contract.

Created at BUY time and carried unchanged through order/fill/position lifecycle.
TQQQ_INFINITE is intentionally excluded.
"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

US_ENTRY_EXIT_CONTRACT_VERSION = "us_entry_exit_contract_v2"

_PROVENANCE_FIELDS = (
    "entry_reason", "entry_style_selected", "entry_component", "entry_signal_type",
    "reasons", "filters_passed", "score_breakdown", "explanation_quality",
    "rank_final30", "score_final", "trend_score", "theme_cluster", "sector",
    "industry", "market_state", "market_regime", "rotation_regime",
)


def _dict(value: Any) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _merged(*sources: Any) -> dict:
    out: dict[str, Any] = {}
    for source in sources:
        data = _dict(source)
        meta = _dict(data.get("meta"))
        out.update({k: v for k, v in meta.items() if v is not None})
        out.update({k: v for k, v in data.items() if k != "meta" and v is not None})
    return out


def _sha(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def verify_us_entry_exit_contract(contract: Any) -> bool:
    data = _dict(contract)
    digest = str(data.pop("sha256", "") or "")
    return bool(
        digest
        and data.get("version") == US_ENTRY_EXIT_CONTRACT_VERSION
        and data.get("strategy_owner") == "US_STANDARD"
        and isinstance(data.get("management"), dict)
        and isinstance(data.get("entry_provenance"), dict)
        and digest == _sha(data)
    )


def us_entry_exit_contract_integrity_state(*sources: Any) -> str:
    """Return NONE, VALID, or INVALID for an explicitly claimed v2 contract."""
    claimed = False
    for source in sources:
        data = _dict(source)
        meta = _dict(data.get("meta"))
        for container in (data, meta):
            candidate = container.get("entry_exit_contract")
            version = str(container.get("entry_exit_contract_version") or "")
            digest = str(container.get("entry_exit_contract_sha256") or "")
            if candidate is not None or version == US_ENTRY_EXIT_CONTRACT_VERSION or digest:
                claimed = True
                parsed = _dict(candidate)
                if parsed and verify_us_entry_exit_contract(parsed):
                    if digest and digest != parsed.get("sha256"):
                        continue
                    return "VALID"
    return "INVALID" if claimed else "NONE"


def extract_us_entry_exit_contract(*sources: Any) -> dict:
    for source in sources:
        data = _dict(source)
        for candidate in (data.get("entry_exit_contract"), _dict(data.get("meta")).get("entry_exit_contract")):
            parsed = _dict(candidate)
            if verify_us_entry_exit_contract(parsed):
                return parsed
    return {}


def build_us_entry_exit_contract(*sources: Any) -> dict:
    merged = _merged(*sources)
    existing = extract_us_entry_exit_contract(*sources)
    if existing:
        return existing

    owner = str(merged.get("strategy_owner") or merged.get("sleeve_id") or "US_STANDARD").upper()
    symbol = str(merged.get("symbol") or "").upper()
    if owner == "TQQQ_INFINITE" or symbol == "TQQQ":
        return {}

    reasons = merged.get("reasons")
    if not isinstance(reasons, list):
        reasons = [] if reasons in (None, "") else [str(reasons)]
    entry_reason = str(
        merged.get("entry_reason")
        or merged.get("selected_reason")
        or (reasons[0] if reasons else "")
        or merged.get("entry_style_selected")
        or merged.get("entry_signal_type")
        or ""
    )
    provenance = {field: merged.get(field) for field in _PROVENANCE_FIELDS if merged.get(field) is not None}
    provenance["entry_reason"] = entry_reason
    provenance["reasons"] = reasons

    management = {
        "book": str(merged.get("book") or os.getenv("US_DEFAULT_ENTRY_BOOK", "SWING_BOOK")),
        "horizon": str(merged.get("horizon") or os.getenv("US_DEFAULT_ENTRY_HORIZON", "SWING_CARRY")),
        "exit_policy": str(merged.get("exit_policy") or os.getenv("US_DEFAULT_EXIT_POLICY", "US_SWING_DEFAULT")),
        "partial_exit_allowed": bool(merged.get("partial_exit_allowed", os.getenv("US_SELL_PARTIAL_ALLOWED", "0") == "1")),
        "min_hold_minutes": int(merged.get("min_hold_minutes") or os.getenv("US_SWING_MIN_HOLD_MINUTES", "390")),
        "swing": {
            "hard_stop": float(os.getenv("US_HARD_STOP_PCT", "0.08")),
            "soft_stop": float(os.getenv("US_SOFT_STOP_LOSS_PCT", "0.05")),
            "trailing_activation_profit": float(os.getenv("US_TRAILING_ACTIVATION_PROFIT_PCT", "0.03")),
            "trailing_stop": float(os.getenv("US_TRAILING_STOP_PCT", "0.05")),
            "profit_protect": float(os.getenv("US_PROFIT_PROTECT_PCT", "0.15")),
            "giveback": float(os.getenv("US_GIVEBACK_PCT", "0.33")),
            "profit_trailing_sell_ratio": float(os.getenv("US_PROFIT_TRAILING_SELL_RATIO", "0.50")),
            "soft_stop_sell_ratio": float(os.getenv("US_SOFT_STOP_SELL_RATIO", "0.50")),
            "trend_exit_min_hold_days": int(os.getenv("US_TREND_EXIT_MIN_HOLD_DAYS", "2")),
            "trend_trim_sell_ratio": float(os.getenv("US_TREND_TRIM_SELL_RATIO", "0.35")),
            "time_stop_days": int(os.getenv("US_TIME_STOP_DAYS", "20")),
            "time_stop_grace_days": int(os.getenv("US_TIME_STOP_GRACE_DAYS", "5")),
            "time_stop_min_profit_pct": float(os.getenv("US_TIME_STOP_MIN_PROFIT_PCT", "0.03")),
            "time_stop_first_sell_ratio": float(os.getenv("US_TIME_STOP_FIRST_SELL_RATIO", "0.50")),
            "soft_stop_confirm_ticks": int(os.getenv("US_SOFT_STOP_CONFIRM_TICKS", "2")),
            "persistent_soft_stop_ticks": int(os.getenv("US_PERSISTENT_SOFT_STOP_TICKS", "3")),
            "block_same_day_soft_exit": os.getenv("US_SWING_BLOCK_SAME_DAY_SOFT_EXIT", "1") in {"1", "true", "True", "yes"},
            "trend_score_warning_threshold": float(os.getenv("US_TREND_SCORE_WARNING_THRESHOLD", "0.45")),
            "trend_score_severe_threshold": float(os.getenv("US_TREND_SCORE_SEVERE_THRESHOLD", "0.35")),
        },
        "day": {
            "hard_stop": float(os.getenv("US_DAY_HARD_STOP_PCT", "0.03")),
            "profit_take": float(os.getenv("US_DAY_PROFIT_TAKE_PCT", "0.025")),
            "trailing_stop": float(os.getenv("US_DAY_TRAILING_STOP_PCT", "0.025")),
            "close_flatten_after_et": str(os.getenv("US_DAY_CLOSE_FLATTEN_AFTER_ET", "15:45")),
        },
        "profit_capture": {
            "enabled": os.getenv("US_PROFIT_CAPTURE_ENABLE", "1") not in {"0", "false", "False"},
            "runner_min_remain_pct": float(os.getenv("US_RUNNER_MIN_REMAIN_PCT", "0.40")),
            "stages": [
                {"flag": "tp1_done", "reason": "TAKE_PROFIT_TP1", "threshold_fraction": float(os.getenv("US_TP1_PCT", "0.03")), "sell_fraction": float(os.getenv("US_TP1_SELL_PCT", "0.25"))},
                {"flag": "tp2_done", "reason": "TAKE_PROFIT_TP2", "threshold_fraction": float(os.getenv("US_TP2_PCT", "0.05")), "sell_fraction": float(os.getenv("US_TP2_SELL_PCT", "0.25"))},
                {"flag": "tp3_done", "reason": "TAKE_PROFIT_TP3", "threshold_fraction": float(os.getenv("US_TP3_PCT", "0.08")), "sell_fraction": float(os.getenv("US_TP3_SELL_PCT", "0.20"))},
            ],
        },
    }
    payload = {
        "version": US_ENTRY_EXIT_CONTRACT_VERSION,
        "strategy_owner": "US_STANDARD",
        "sleeve_id": str(merged.get("sleeve_id") or "US_STANDARD"),
        "entry_strategy": str(merged.get("entry_strategy") or merged.get("strategy") or "us_pb1"),
        "entry_provenance": provenance,
        "management": management,
    }
    payload["sha256"] = _sha(payload)
    return payload


def contract_exit_config(position: dict) -> dict:
    contract = extract_us_entry_exit_contract(position, _dict(position).get("meta"))
    return dict((contract.get("management") or {}).get("swing") or {}) if contract else {}


def contract_day_config(position: dict) -> dict:
    contract = extract_us_entry_exit_contract(position, _dict(position).get("meta"))
    return dict((contract.get("management") or {}).get("day") or {}) if contract else {}


def contract_profit_capture(position: dict) -> dict:
    contract = extract_us_entry_exit_contract(position, _dict(position).get("meta"))
    return dict((contract.get("management") or {}).get("profit_capture") or {}) if contract else {}
