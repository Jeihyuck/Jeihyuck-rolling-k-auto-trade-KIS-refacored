from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.pb1_runner import _resolve_reconcile_only_followup


def test_reconcile_only_pending_keeps_waiting_while_open_orders_exist() -> None:
    skip_now, keep_pending = _resolve_reconcile_only_followup(
        enabled=True,
        pending=True,
        buy_orders=0,
        sell_orders=0,
        open_orders_count=2,
    )

    assert skip_now is True
    assert keep_pending is True


def test_reconcile_only_pending_clears_when_open_orders_gone() -> None:
    skip_now, keep_pending = _resolve_reconcile_only_followup(
        enabled=True,
        pending=True,
        buy_orders=0,
        sell_orders=0,
        open_orders_count=0,
    )

    assert skip_now is True
    assert keep_pending is False


def test_reconcile_only_arms_after_order_submission() -> None:
    skip_now, keep_pending = _resolve_reconcile_only_followup(
        enabled=True,
        pending=False,
        buy_orders=1,
        sell_orders=0,
        open_orders_count=0,
    )

    assert skip_now is False
    assert keep_pending is True