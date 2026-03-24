from __future__ import annotations


class BotStatePersistError(RuntimeError):
    pass


def evaluate_persist_guard(*, require_persist: bool, dirty_by_stat: bool) -> str:
    """Evaluate whether bot state persistence should hard-fail, warn, or skip.

    This small helper restores the public API expected by the persist-guard tests.
    """
    if not require_persist:
        return "skip"
    if dirty_by_stat:
        raise BotStatePersistError("BOT_STATE_PERSIST_GUARD_DIRTY_BY_STAT")
    return "warn"