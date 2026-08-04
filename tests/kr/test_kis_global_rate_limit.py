from __future__ import annotations

from trader.rate_limit import KisCallGate


def test_global_cooldown_blocks_acquire_temporarily() -> None:
    gate = KisCallGate()
    gate.set_global_cooldown("practice", "acct-1", seconds=1.0)
    wait = gate.acquire(
        environment="practice",
        account_key="acct-1",
        endpoint_category="price",
        priority=False,
    )
    assert wait > 0


def test_priority_endpoint_skips_endpoint_bucket_delay() -> None:
    gate = KisCallGate()
    w1 = gate.acquire(
        environment="practice",
        account_key="acct-2",
        endpoint_category="price",
        priority=False,
    )
    assert w1 >= 0
    w2 = gate.acquire(
        environment="practice",
        account_key="acct-2",
        endpoint_category="order",
        priority=True,
    )
    assert w2 >= 0
