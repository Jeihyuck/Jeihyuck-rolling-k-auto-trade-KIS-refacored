"""Read-only historical KIS evidence lane for the US Stage4 single writer.

The background thread may SELECT active claims/debts and GET broker orders/
balance. It can NEVER persist a fill, cancel/submit an order, release a claim,
publish READY, or own the Stage4 advisory writer lock. All state transitions
remain on the original writer, which rechecks the claim set and source age
immediately before publishing READY. Missing evidence fails closed.
"""
from __future__ import annotations

import copy
import hashlib
import logging
import threading
import time
from datetime import date
from typing import Any, Callable

from sqlalchemy import text

logger = logging.getLogger(__name__)


def active_claim_signature(claims: list[dict]) -> frozenset[tuple[str, str, str]]:
    if not isinstance(claims, list):
        raise RuntimeError("US_STAGE4_HISTORICAL_CLAIMS_UNVERIFIED")
    keys = set()
    for claim in claims:
        day = str(claim.get("trade_date") or "")[:10]
        client_key = str(claim.get("client_order_key") or "")
        attempt_id = str(claim.get("active_attempt_id") or "")
        if not (day and client_key and attempt_id):
            raise RuntimeError("US_STAGE4_HISTORICAL_CLAIM_IDENTITY_MISSING")
        date.fromisoformat(day)
        keys.add((day, client_key, attempt_id))
    return frozenset(keys)


def _debt_order_source_dates(debts: list[dict], *, scope: dict,
                             max_dates: int) -> set[str]:
    if not debts:
        return set()
    from trader.account_state import get_account_key
    from trader.us.db.repos import _get_engine_or_none
    raw_account = str(get_account_key(env="practice") or "")
    if hashlib.sha256(raw_account.encode()).hexdigest() != scope["account_id"]:
        raise RuntimeError("US_STAGE4_HISTORICAL_DEBT_ACCOUNT_MISMATCH")
    engine = _get_engine_or_none()
    if engine is None:
        raise RuntimeError("US_STAGE4_HISTORICAL_DEBT_DB_UNAVAILABLE")
    source_dates: set[str] = set()
    # Read-only bounded lookup of original TP orders. An epoch rollover must
    # not silently omit old protective obligations, or use a different account.
    with engine.connect() as conn:
        for debt in debts:
            if str(debt.get("account_id") or "") != raw_account:
                raise RuntimeError("US_STAGE4_HISTORICAL_DEBT_ACCOUNT_MISMATCH")
            life = str(debt.get("lifecycle_id") or "")
            epoch = str(debt.get("trading_epoch_id") or "")
            symbol = str(debt.get("symbol") or "").upper()
            if not life or not epoch or not symbol:
                raise RuntimeError("US_STAGE4_HISTORICAL_DEBT_IDENTITY_MISSING")
            dates = conn.execute(text("""
                SELECT DISTINCT o.trade_date
                  FROM us_orders o
                  JOIN us_execution_claims c
                    ON c.trading_epoch_id=o.trading_epoch_id
                   AND c.env=o.env AND c.trade_date=o.trade_date
                   AND c.lifecycle_id=:life AND c.account_id=:account
                   AND c.strategy_owner='US_STANDARD'
                   AND c.action=upper(o.meta->>'profit_capture_stage')
                 WHERE o.env='practice' AND o.trading_epoch_id=:epoch
                   AND o.symbol=:symbol AND o.side='SELL'
                   AND o.order_no IS NOT NULL
                 ORDER BY o.trade_date DESC
                 LIMIT :cap
            """), {
                "life": life, "account": scope["account_id"],
                "epoch": epoch, "symbol": symbol,
                "cap": max_dates + 1,
            }).scalars().all()
            source_dates.update(str(d)[:10] for d in dates)
            if len(source_dates) > max_dates:
                raise RuntimeError("US_STAGE4_HISTORICAL_SOURCE_DATES_OVER_BUDGET")
    return source_dates


class SnapshotOnlyProvider:
    """KIS GET-only snapshot facade. Never permits fallback network I/O."""

    _offline = False
    _tick_context = None

    def __init__(
        self, *, books: dict[str, dict], balance: dict | None,
        balance_at: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        ttl_sec: float = 15.0,
    ):
        self._books = copy.deepcopy(books)
        self._balance = copy.deepcopy(balance)
        self._balance_at = balance_at
        self._clock = clock
        self._ttl_sec = ttl_sec

    def _assert_fresh(self, observed_at: float) -> None:
        age = self._clock() - observed_at
        if age < 0 or age > self._ttl_sec:
            raise RuntimeError("US_STAGE4_HISTORICAL_BROKER_EVIDENCE_STALE")

    def get_today_orders(self, trade_date: str) -> list[dict] | None:
        row = self._books.get(str(trade_date)[:10])
        if row is None:
            return None
        self._assert_fresh(row["at"])
        return copy.deepcopy(row["orders"])

    def get_order_lookup_provenance(self, trade_date: str) -> dict:
        row = self._books.get(str(trade_date)[:10])
        if row is None:
            return {}
        self._assert_fresh(row["at"])
        return copy.deepcopy(row["provenance"])

    def get_fills_by_order_no(
        self, order_no: str, symbol: str, trade_date: str, **_kwargs,
    ) -> dict | None:
        rows = self.get_today_orders(trade_date)
        if rows is None:
            return None
        from trader.us.data_provider import USDataProvider
        return USDataProvider.get_fills_by_order_no(
            self, order_no=order_no, symbol=symbol,
            trade_date=trade_date, order_book_snapshot=rows,
        )

    def get_balance(self, force_refresh: bool = False) -> dict:
        if self._balance is None or self._balance_at is None:
            raise RuntimeError("US_STAGE4_HISTORICAL_BALANCE_NOT_PREFETCHED")
        self._assert_fresh(self._balance_at)
        return copy.deepcopy(self._balance)

    def _get_client(self):
        # Explicitly forbid the KIS fills fallback on the critical writer.
        raise RuntimeError("US_STAGE4_HISTORICAL_RAW_KIS_IO_FORBIDDEN")


class HistoricalReadOnlyReader:
    """One bounded GET lane, independently of the authoritative writer.

    At most one KIS book/balance operation per poll. The last successful
    fully paginated, account-scoped book can be used for at most ttl_sec.
    A background failure invalidates its evidence rather than extending TTL.
    """

    def __init__(
        self, *, scope: dict, generation: str,
        provider_factory: Callable[[], Any],
        load_claims: Callable[[], list[dict]] | None = None,
        load_debts: Callable[..., list[dict]] | None = None,
        load_debt_dates: Callable[..., set[str]] = _debt_order_source_dates,
        clock: Callable[[], float] = time.monotonic,
        poll_sec: float = 2.0, ttl_sec: float = 15.0,
        max_dates: int = 4,
    ):
        if (scope.get("env") != "practice"
                or not scope.get("account_id")
                or not scope.get("trading_epoch_id")
                or not generation):
            raise RuntimeError("US_STAGE4_HISTORICAL_READER_SCOPE_INVALID")
        self.scope = dict(scope)
        self.generation = str(generation)
        self.provider_factory = provider_factory
        if load_claims is None:
            from trader.us.db.repos import load_active_execution_claim_attempts
            load_claims = load_active_execution_claim_attempts
        if load_debts is None:
            from trader.us.protective_stop_obligation import load_pending_stop_obligations
            load_debts = load_pending_stop_obligations
        self.load_claims = load_claims
        self.load_debts = load_debts
        self.load_debt_dates = load_debt_dates
        self.clock = clock
        self.poll_sec = max(1.0, float(poll_sec))
        self.ttl_sec = max(1.0, min(float(ttl_sec), 20.0))
        self.max_dates = max(1, min(4, int(max_dates)))
        self._lock = threading.Lock()
        self._books: dict[str, dict] = {}
        self._balance: dict | None = None
        self._balance_at = 0.0
        self._last_error: str | None = "NOT_STARTED"
        self._cursor = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _poll_once(self, provider: Any) -> None:
        claims = self.load_claims()
        signatures = active_claim_signature(claims)
        debts = self.load_debts(env="practice")
        if not isinstance(debts, list):
            raise RuntimeError("US_STAGE4_HISTORICAL_DEBTS_UNVERIFIED")
        source_dates = {k[0] for k in signatures}
        source_dates.update(
            self.load_debt_dates(debts, scope=self.scope, max_dates=self.max_dates)
        )
        if len(source_dates) > self.max_dates:
            raise RuntimeError("US_STAGE4_HISTORICAL_SOURCE_DATES_OVER_BUDGET")
        tasks = [("book", day) for day in sorted(source_dates)]
        if debts:
            tasks.append(("balance", None))
        if not tasks:
            with self._lock:
                self._last_error = None
                self._books.clear()
                self._balance = None
            return
        kind, day = tasks[self._cursor % len(tasks)]
        self._cursor += 1
        if kind == "book":
            orders = provider.get_today_orders(trade_date=day)
            provenance = provider.get_order_lookup_provenance(day)
            if (not isinstance(orders, list)
                    or not isinstance(provenance, dict)
                    or provenance.get("verified") is not True
                    or provenance.get("source") != "kis_authenticated_order_book"
                    or provenance.get("market") != "US"
                    or provenance.get("env") != "practice"
                    or provenance.get("account_id") != self.scope["account_id"]
                    or str(provenance.get("query_trade_date") or "")[:10] != day
                    or not provenance.get("account_product_code")):
                raise RuntimeError("US_STAGE4_HISTORICAL_BROKER_SCOPE_UNVERIFIED")
            if any(not isinstance(row, dict) or
                   str(row.get("normalization_result") or "").lower() == "quarantined"
                   for row in orders):
                raise RuntimeError("US_STAGE4_HISTORICAL_ORDER_NORMALIZATION_INCOMPLETE")
            with self._lock:
                self._books[day] = {
                    "orders": copy.deepcopy(orders),
                    "provenance": copy.deepcopy(provenance),
                    "keys": frozenset(k for k in signatures if k[0] == day),
                    "at": self.clock(),
                }
                self._last_error = None
            logger.info(
                "[US_STAGE4][HISTORICAL_READONLY_BOOK] source_day=%s "
                "rows=%d age_ms=0 state=OK", day, len(orders)
            )
        else:
            balance = provider.get_balance(force_refresh=True)
            if (not isinstance(balance, dict)
                    or balance.get("balance_complete") is False
                    or balance.get("balance_authoritative") is False
                    or str(balance.get("balance_parse_status") or "OK").upper()
                    not in {"OK", "UNKNOWN"}):
                raise RuntimeError("US_STAGE4_HISTORICAL_BALANCE_UNVERIFIED")
            with self._lock:
                self._balance = copy.deepcopy(balance)
                self._balance_at = self.clock()
                self._last_error = None
            logger.info("[US_STAGE4][HISTORICAL_READONLY_BALANCE] state=OK")

    def _loop(self):
        try:
            provider = self.provider_factory()
            while not self._stop.is_set():
                try:
                    self._poll_once(provider)
                except Exception as exc:
                    with self._lock:
                        self._books.clear()
                        self._balance = None
                        self._last_error = type(exc).__name__
                    logger.warning(
                        "[US_STAGE4][HISTORICAL_READONLY_FAILED] type=%s "
                        "action=INVALIDATE_NO_READY_EXTENSION",
                        type(exc).__name__,
                    )
                self._stop.wait(self.poll_sec)
        except Exception as exc:
            with self._lock:
                self._books.clear()
                self._balance = None
                self._last_error = type(exc).__name__
            logger.exception(
                "[US_STAGE4][HISTORICAL_READONLY_WORKER_EXIT] type=%s",
                type(exc).__name__,
            )

    def start(self):
        if self._thread is not None:
            raise RuntimeError("US_STAGE4_HISTORICAL_READER_ALREADY_STARTED")
        self._thread = threading.Thread(
            target=self._loop, name="us-stage4-kis-readonly", daemon=True,
        )
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=0.5)

    def snapshot_for_claims(self, claims: list[dict]) -> dict:
        required = active_claim_signature(claims)
        if self._thread is not None and not self._thread.is_alive():
            raise RuntimeError("US_STAGE4_HISTORICAL_READER_DEAD")
        now = self.clock()
        with self._lock:
            if self._last_error is not None and required:
                raise RuntimeError("US_STAGE4_HISTORICAL_READONLY_UNAVAILABLE")
            for day in {k[0] for k in required}:
                book = self._books.get(day)
                if (book is None or now < book["at"]
                        or now - book["at"] > self.ttl_sec):
                    raise RuntimeError("US_STAGE4_HISTORICAL_BROKER_EVIDENCE_STALE")
                if book["keys"] != frozenset(k for k in required if k[0] == day):
                    raise RuntimeError("US_STAGE4_HISTORICAL_CLAIM_SET_CHANGED")
            books = {
                day: copy.deepcopy(book)
                for day, book in self._books.items()
                if 0 <= now - book["at"] <= self.ttl_sec
            }
            balance = (
                copy.deepcopy(self._balance)
                if self._balance is not None
                and 0 <= now - self._balance_at <= self.ttl_sec
                else None
            )
        return {
            "scope": dict(self.scope), "generation": self.generation,
            "active_keys": required, "books": books,
            "balance": balance,
            "balance_at": self._balance_at if balance is not None else None,
            "as_of_monotonic": now,
        }

    def assert_still_valid(self, snapshot: dict, claims: list[dict]) -> None:
        if snapshot["scope"] != self.scope or snapshot["generation"] != self.generation:
            raise RuntimeError("US_STAGE4_HISTORICAL_GENERATION_MISMATCH")
        if snapshot["active_keys"] != active_claim_signature(claims):
            raise RuntimeError("US_STAGE4_HISTORICAL_CLAIM_SET_CHANGED")
        current = self.snapshot_for_claims(claims)
        for day in {k[0] for k in snapshot["active_keys"]}:
            if (day not in current["books"]
                    or current["books"][day]["at"] != snapshot["books"][day]["at"]):
                raise RuntimeError("US_STAGE4_HISTORICAL_EVIDENCE_REPLACED")

    def provider_for(self, snapshot: dict) -> SnapshotOnlyProvider:
        if snapshot["scope"] != self.scope or snapshot["generation"] != self.generation:
            raise RuntimeError("US_STAGE4_HISTORICAL_GENERATION_MISMATCH")
        return SnapshotOnlyProvider(
            books=snapshot["books"], balance=snapshot.get("balance"),
            balance_at=snapshot.get("balance_at"),
            clock=self.clock, ttl_sec=self.ttl_sec,
        )
