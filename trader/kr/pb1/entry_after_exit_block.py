from __future__ import annotations


def should_block_entry_after_exit(*, enabled: bool, exit_summary_payload: dict | None) -> tuple[bool, dict]:
    if not enabled:
        return False, {"exit_submit_attempt_count": 0, "accepted_sell_count": 0}
    payload = dict(exit_summary_payload or {})
    submit_attempt_count = int(payload.get("submit_attempt_count") or 0)
    accepted_sell_count = int(payload.get("accepted_sell_count") or 0)
    metrics = {
        "exit_submit_attempt_count": submit_attempt_count,
        "accepted_sell_count": accepted_sell_count,
    }
    blocked = submit_attempt_count > 0 or accepted_sell_count > 0
    return blocked, metrics
