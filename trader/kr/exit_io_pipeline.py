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

    # KIS deadlines are thread-local. A worker without this transfer would
    # silently bypass the single authoritative per-tick HTTP timeout.
    from trader.kis_wrapper import (
        get_kr_tick_deadline, set_kr_tick_deadline, clear_kr_tick_deadline,
    )
    shared_deadline = get_kr_tick_deadline()

    def _bounded_worker_read(holding: T) -> R:
        set_kr_tick_deadline(shared_deadline)
        try:
            return fetch(holding)
        finally:
            clear_kr_tick_deadline()

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
                future = executor.submit(_bounded_worker_read, next_holding)
            yield holding, result
    finally:
        if future is not None:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)


def ordered_bounded_entry_reads(
    holdings: Sequence[T],
    *,
    needs_history: Callable[[T], bool],
    fetch: Callable[[T], R],
    empty: Callable[[], R],
    max_workers: int = 3,
) -> Iterator[tuple[T, R]]:
    """Bounded, ordered final30 BUY data IO — NEVER evaluates or places orders.

    Submit at most max_workers reads ahead; consume them in the original
    strategy evaluation order. The authoritative KR tick deadline travels
    into every worker, so no quote call may outlive the caller budget.
    """
    from trader.kis_wrapper import (
        get_kr_tick_deadline, set_kr_tick_deadline, clear_kr_tick_deadline,
    )

    items = list(holdings)
    concurrency = max(1, min(4, int(max_workers)))
    if concurrency == 1 or len(items) <= 1:
        yield from ordered_exit_ohlcv(
            items, needs_history=needs_history, fetch=fetch,
            empty=empty, enabled=False,
        )
        return
    shared_deadline = get_kr_tick_deadline()

    def fetch_scoped(item: T) -> R:
        set_kr_tick_deadline(shared_deadline)
        try:
            return fetch(item)
        finally:
            clear_kr_tick_deadline()

    executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="kr-entry-ohlcv")
    pending: dict[int, Future[R]] = {}
    next_index = 0
    try:
        for i, item in enumerate(items):
            # Bounded lookahead: never scan/fetch more than N future symbols.
            while next_index < len(items) and next_index < i + concurrency:
                if needs_history(items[next_index]):
                    pending[next_index] = executor.submit(fetch_scoped, items[next_index])
                next_index += 1
            result = pending.pop(i).result() if i in pending else empty()
            yield item, result
    finally:
        for future in pending.values():
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
