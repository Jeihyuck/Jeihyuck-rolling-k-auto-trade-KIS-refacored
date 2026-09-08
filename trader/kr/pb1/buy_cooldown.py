from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def resolve_buy_cooldown_state(
    *,
    today: str,
    code: str,
    cooldown_until,
    holding_qty: int = 0,
    today_buy_exists: bool,
    today_fill_exists: bool,
    cooldown_source_events_count: int,
    last_fill_event_at,
    cooldown_source: str | None = None,
    recent_valid_exit_event: bool = False,
    recent_exit_reason: str | None = None,
) -> dict:
    cooldown_active = False
    stale_ignored = False
    final_cooldown_policy = "none"
    resolved_source = str(cooldown_source or "none")
    risk_off_same_day_only = str(recent_exit_reason or "").strip().upper() in {"EXIT_RISK_OFF", "EXIT_SOFT_RISK_OFF", "BUG_RECOVERY_EXIT"}

    cooldown_until_value = str(cooldown_until).strip() if cooldown_until is not None else ""
    if cooldown_until_value and cooldown_until_value >= today:
        if bool(today_buy_exists):
            cooldown_active = True
            final_cooldown_policy = "same_day_only"
            resolved_source = "same_day_duplicate_prevention"
        elif bool(recent_valid_exit_event) and not risk_off_same_day_only:
            cooldown_active = True
            final_cooldown_policy = "multi_day"
            if resolved_source == "none":
                resolved_source = "completed_trade_cooldown"
        elif int(holding_qty or 0) <= 0 and not bool(today_fill_exists):
            stale_ignored = True
            final_cooldown_policy = "stale_ignored"
            if resolved_source == "none":
                resolved_source = "stale_residue"
        elif bool(today_fill_exists) or risk_off_same_day_only:
            final_cooldown_policy = "same_day_only"
            resolved_source = "same_day_duplicate_prevention"
        else:
            cooldown_active = int(cooldown_source_events_count or 0) > 0 or bool(last_fill_event_at)
            final_cooldown_policy = "multi_day" if cooldown_active else "none"

    logger.info(
        "[PB1][BUYABLE_GATE][COOLDOWN_SRC] code=%s source=%s active=%s stale=%s today_buy_exists=%s today_fill_exists=%s last_fill_event_at=%s cooldown_until=%s",
        code,
        resolved_source,
        int(cooldown_active),
        int(stale_ignored),
        int(bool(today_buy_exists)),
        int(bool(today_fill_exists)),
        last_fill_event_at,
        cooldown_until_value or None,
    )
    if stale_ignored:
        logger.warning(
            "[PB1][BUYABLE_GATE][STALE_COOLDOWN] code=%s cooldown_until=%s ignored=1 reason=no_fill_evidence",
            code,
            cooldown_until_value or None,
        )

    return {
        "cooldown_active": cooldown_active,
        "stale_ignored": stale_ignored,
        "cooldown_source": resolved_source,
        "final_cooldown_policy": final_cooldown_policy,
    }
