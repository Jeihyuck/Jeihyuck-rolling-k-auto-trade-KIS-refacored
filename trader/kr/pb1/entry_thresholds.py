from __future__ import annotations

from datetime import datetime


def is_intraday_threshold_window(*, phase: str, window_internal: str, now_kst: datetime, entry_window_end: str) -> bool:
    if phase not in {"prep", "entry", "pm_entry"}:
        return False
    if window_internal not in {"morning", "day"}:
        return False
    try:
        entry_end = datetime.strptime(entry_window_end, "%H:%M").time()
        return now_kst.time() <= entry_end
    except ValueError:
        return False


def resolve_entry_thresholds(
    *,
    phase: str,
    window_internal: str,
    now_kst: datetime,
    entry_window_end: str,
    effective_entry_filters: dict | None,
    defaults: dict,
) -> tuple[bool, dict]:
    intraday = is_intraday_threshold_window(
        phase=phase,
        window_internal=window_internal,
        now_kst=now_kst,
        entry_window_end=entry_window_end,
    )
    filters = dict(effective_entry_filters or {})
    volu_max = float(filters.get("volu_max_intraday" if intraday else "volu_max", defaults["volu_max_intraday"] if intraday else defaults["volu_max"]))
    thresholds = {
        "vol_contraction_max": float(filters.get("vol_max", defaults["vol_max"])),
        "volu_contraction_max": volu_max,
        "pullback_min": float(filters.get("pullback_min", defaults["pullback_min"])),
        "pullback_max": float(filters.get("pullback_max", defaults["pullback_max"])),
        "require_both_contractions": bool(filters.get("require_both_contractions", defaults["require_both_contractions"])),
    }
    return intraday, thresholds
