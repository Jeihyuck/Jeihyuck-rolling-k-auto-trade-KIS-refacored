from __future__ import annotations


def _resolve_session_window_name(*, session_kind: str | None, raw_window_name: str | None) -> str:
    normalized_session = str(session_kind or "").strip().lower()
    normalized_window = str(raw_window_name or "").strip().lower()
    if normalized_session == "am" and normalized_window in {"am", "morning", "intraday", "day", "preopen", "session", "open"}:
        return "morning"
    if normalized_window in {"morning", "preopen", "close", "intraday"}:
        return "intraday"
    if normalized_window == "after":
        return "after"
    return "day"


def build_stage_label(*, session_kind: str | None, window: str | None = None, phase: str | None = None) -> str:
    """[2026-04-30] 올바른 stage 레이블 생성 (AM entry ≠ PB1-CLOSE).

    Rules:
        session_kind=am, phase=entry  → PB1-AM-ENTRY
        session_kind=afternoon/pm, phase=entry → PB1-AFTERNOON-ENTRY
        phase=exit or close           → PB1-CLOSE-EXIT
        fallback                      → PB1-{SESSION}-{PHASE}
    """
    sess = str(session_kind or "").strip().lower()
    ph = str(phase or window or "").strip().lower()
    if ph in {"exit", "close"}:
        return "PB1-CLOSE-EXIT"
    if ph == "entry":
        if sess == "am":
            return "PB1-AM-ENTRY"
        if sess in {"afternoon", "pm"}:
            return "PB1-AFTERNOON-ENTRY"
        return "PB1-ENTRY"
    if sess == "am":
        return "PB1-AM-ENTRY"
    if sess in {"afternoon", "pm"}:
        return "PB1-AFTERNOON-ENTRY"
    return "PB1-ENTRY"
