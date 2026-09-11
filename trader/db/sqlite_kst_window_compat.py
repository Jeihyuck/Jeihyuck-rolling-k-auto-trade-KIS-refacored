"""SQLite compatibility for KST order-window queries.

Production PostgreSQL stores/compares timezone-aware timestamps correctly.  The
SQLite test backend uses ``CURRENT_TIMESTAMP`` for ``orders.created_at``, which
is UTC-naive, while KR session windows are KST wall-clock datetimes.  Between
00:00 and 08:59 KST this can make a just-created durable order appear to belong
to the previous day.

Only SQLite is affected here.  Convert the order ``created_at`` expression from
UTC to KST before comparing it with KST session-window bounds.  PostgreSQL and
all live database behaviour remain unchanged.
"""
from __future__ import annotations

import sqlalchemy as sa

_INSTALLED = False


def install_sqlite_kst_order_window_compat() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    # Import lazily so this module can be installed by trader.db.__init__ after
    # engine/schema modules are available without affecting production dialects.
    from .repos import OrdersRepo

    if getattr(OrdersRepo, "_sqlite_kst_window_compat_installed", False):
        _INSTALLED = True
        return

    original = OrdersRepo._window_expr

    def _kst_window_expr(self, column):
        if self.engine.dialect.name == "sqlite":
            # SQLite CURRENT_TIMESTAMP is UTC.  KR order-window APIs are defined
            # in KST wall-clock time, so shift the stored UTC expression +09:00.
            return sa.func.datetime(column, "+9 hours")
        return original(self, column)

    OrdersRepo._window_expr = _kst_window_expr
    OrdersRepo._sqlite_kst_window_compat_installed = True
    _INSTALLED = True
