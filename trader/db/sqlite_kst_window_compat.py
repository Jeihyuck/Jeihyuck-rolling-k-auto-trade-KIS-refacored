"""SQLite compatibility for KR durable-order time windows.

Production PostgreSQL stores and compares timezone-aware timestamps correctly.
SQLite's ``CURRENT_TIMESTAMP`` is UTC-naive, while the KR repository APIs and
contract tests use KST wall-clock session windows.  Between 00:00 and 08:59 KST,
a newly created SQLite intent can therefore appear to belong to the previous
KST day.

Do not shift query expressions: some tests and repair fixtures explicitly store
KST wall-clock ``created_at`` values.  Instead, only for SQLite, normalize the
automatically-created durable intent timestamp to KST wall-clock time at intent
creation.  PostgreSQL/live behaviour is unchanged.
"""
from __future__ import annotations

import functools

import sqlalchemy as sa

from trader.time_utils import now_kst

_INSTALLED = False


def install_sqlite_kst_order_window_compat() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    from .repos import OrdersRepo

    if getattr(OrdersRepo, "_sqlite_kst_window_compat_installed", False):
        _INSTALLED = True
        return

    original_create = OrdersRepo.create_intent_idempotent

    @functools.wraps(original_create)
    def _create_intent_with_kst_created_at(self, *args, **kwargs):
        result = original_create(self, *args, **kwargs)
        if self.engine.dialect.name != "sqlite":
            return result

        try:
            order_id, created = result
        except Exception:
            return result
        if not created or not order_id:
            return result

        # SQLite DateTime does not preserve tz offsets.  Store the KST wall
        # clock explicitly so the existing KST session-window comparisons are
        # deterministic across UTC/KST midnight boundaries.
        created_at_kst = now_kst().replace(tzinfo=None)
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(self._schema.orders.c.order_id == order_id)
                .values(created_at=created_at_kst, updated_at=sa.func.now())
            )
        return result

    OrdersRepo.create_intent_idempotent = _create_intent_with_kst_created_at
    OrdersRepo._sqlite_kst_window_compat_installed = True
    _INSTALLED = True
