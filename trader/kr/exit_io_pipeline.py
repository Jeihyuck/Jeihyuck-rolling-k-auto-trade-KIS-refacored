"""Ordered, bounded OHLCV read-ahead for the KR protective SELL pass.

Only market *reads* run in the worker. Strategy evaluation, execution claims,
broker submissions, reconciliation, and all DB writes remain on the caller
thread in their original order. Read-ahead never waits for unrelated symbols
before evaluating the first holding.
"""
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from collections.abc import Callable, Iterator, Sequence
from typing import Any, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def ordered_exit_ohlcv(
    holdings: Sequence[T],
    *,
    needs_history: Callable[[T], bool],
    fetch: Callable[[T], R],
    empty: Callable[[], R],
    enabled: bool = True,
) -> Iterator[tuple[T, R]]:
    """Pipeline one NEXT holding read while evaluating the current one.

    A maximum of one provider call runs concurrently with the main (serial)
    SELL policy/order path. Existing holder ordering and broker safety gates
    remain unchanged. Worker errors propagate: no empty market data is
    fabricated for an otherwise history-dependent policy.
    """
    items = list(holdings)
    if not enabled or len(items) <= 1:
        for holding in items:
            yield holding, fetch(holding) if needs_history(holding) else empty()
        return

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kr-exit-ohlcv")
    future: Future[R] | None = None
    try:
        for i, holding in enumerate(items):
            if needs_history(holding):
                result = future.result() if future is not None else fetch(holding)
            else:
                result = empty()
            future = None
            # Launch exactly one independent market-data read for the next
            # eligible holding. Never submit a broker operation to the worker.
            if i + 1 < len(items) and needs_history(items[i + 1]):
                next_holding = items[i + 1]
                future = executor.submit(fetch, next_holding)
            yield holding, result
    finally:
        if future is not None:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
