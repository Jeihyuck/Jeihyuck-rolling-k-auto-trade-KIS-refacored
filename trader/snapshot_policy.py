from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class SnapshotDateValidation:
    requested_as_of: date
    actual_as_of: date
    ttl_days: int
    stale_days: int
    is_future: bool
    is_stale: bool
    is_exact: bool
    accepted: bool
    action: str


def validate_snapshot_date(
    requested_as_of: date,
    actual_as_of: date,
    ttl_days: int,
    allow_stale: bool = False,
) -> SnapshotDateValidation:
    """Single date-policy guard for snapshot loaders.

    Intent: keep future/stale handling deterministic across candidate/universe/derived flows.
    """
    ttl = max(0, int(ttl_days))
    stale_days = max(0, (requested_as_of - actual_as_of).days)
    is_future = actual_as_of > requested_as_of
    is_stale = stale_days > ttl
    is_exact = actual_as_of == requested_as_of

    if is_future:
        return SnapshotDateValidation(
            requested_as_of=requested_as_of,
            actual_as_of=actual_as_of,
            ttl_days=ttl,
            stale_days=stale_days,
            is_future=True,
            is_stale=False,
            is_exact=False,
            accepted=False,
            action="reject_future_snapshot",
        )

    if is_stale and not allow_stale:
        return SnapshotDateValidation(
            requested_as_of=requested_as_of,
            actual_as_of=actual_as_of,
            ttl_days=ttl,
            stale_days=stale_days,
            is_future=False,
            is_stale=True,
            is_exact=False,
            accepted=False,
            action="stale_snapshot",
        )

    action = "exact_snapshot" if is_exact else ("stale_snapshot" if is_stale else "ttl_valid_snapshot")
    return SnapshotDateValidation(
        requested_as_of=requested_as_of,
        actual_as_of=actual_as_of,
        ttl_days=ttl,
        stale_days=stale_days,
        is_future=False,
        is_stale=is_stale,
        is_exact=is_exact,
        accepted=True,
        action=action,
    )
