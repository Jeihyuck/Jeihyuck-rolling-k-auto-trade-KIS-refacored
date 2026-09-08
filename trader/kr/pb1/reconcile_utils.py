from __future__ import annotations

from typing import Any, Iterable


def extract_cooldown_source_details(ledger_rows: Iterable[dict[str, Any]] | None) -> dict[str, Any]:
    risk_off_exit_reasons = {"EXIT_RISK_OFF", "EXIT_SOFT_RISK_OFF", "BUG_RECOVERY_EXIT"}
    for row in ledger_rows or []:
        event_type = str((row or {}).get("event_type") or "").strip().upper()
        side = str((row or {}).get("side") or "").strip().upper()
        reasons = [str(item).strip().upper() for item in ((row or {}).get("reasons") or []) if str(item).strip()]
        payload = (row or {}).get("payload_json") if isinstance((row or {}).get("payload_json"), dict) else {}
        exit_reason = str(payload.get("exit_reason") or payload.get("reason") or (reasons[0] if reasons else "")).strip().upper()
        if not exit_reason:
            continue
        if side != "SELL" and not event_type.startswith("EXIT"):
            continue
        if exit_reason in risk_off_exit_reasons:
            return {
                "source": "risk_off_same_day_only",
                "recent_valid_exit_event": False,
                "recent_exit_reason": exit_reason,
                "evidence_count": 1,
            }
        return {
            "source": "completed_trade_cooldown",
            "recent_valid_exit_event": True,
            "recent_exit_reason": exit_reason,
            "evidence_count": 1,
        }
    return {
        "source": "none",
        "recent_valid_exit_event": False,
        "recent_exit_reason": None,
        "evidence_count": 0,
    }


def fill_reconcile_warn_needed(filled_price: float, avg_price_from_balance: float) -> bool:
    if filled_price <= 0 or avg_price_from_balance <= 0:
        return False
    return abs(filled_price - avg_price_from_balance) / avg_price_from_balance > 0.01
