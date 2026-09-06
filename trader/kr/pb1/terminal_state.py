from __future__ import annotations


def resolve_terminal_state(*, status: str, notes: str | None = None, warning_counts: dict[str, int] | None = None) -> str:
    normalized_status = str(status or "UNKNOWN").strip().upper()
    normalized_notes = str(notes or "").strip().lower()
    counts = dict(warning_counts or {})
    warnings_total = sum(int(value) for value in counts.values())
    if normalized_status in {"FATAL_RUNTIME", "FATAL_POSTPROCESS"}:
        return "SESSION_END_FATAL"
    if normalized_status.startswith("SKIP"):
        return "SESSION_END_SKIPPED"
    if normalized_status in {"OK_DEGRADED", "DEGRADED_POSTPROCESS"} or counts.get("degraded_stage_count", 0) > 0:
        return "SESSION_END_OK_DEGRADED"
    if warnings_total > 0 or normalized_status in {"WARN_FAIL_OPEN", "OK_WITH_WARNINGS"} or "degraded" in normalized_notes:
        return "SESSION_END_OK_WITH_WARNINGS"
    return "SESSION_END_OK"
