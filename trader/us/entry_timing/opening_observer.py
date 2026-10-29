"""Read-only, separate-process US opening observer and durable minute-bar store.

Only actual KIS overseas-stock 1-minute OHLCV populates verified bars.
Irregular WS quote snapshots go in a separate table, NEVER fabricated bars.
No broker order/router imports, claims, or BUY/SELL permission.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from trader.us.entry_timing.opportunity import normalize_bars

LOG = logging.getLogger(__name__)
NY = ZoneInfo("America/New_York")


def store_path(*, env: str, account_scope: str, trade_date: str,
               base: str | Path = "runtime/private") -> Path:
    if env != "practice" or not account_scope or len(trade_date) != 10:
        raise ValueError("observer_requires_practice_account_scope_and_day")
    digest = hashlib.sha256((env + ":" + account_scope).encode()).hexdigest()[:20]
    root = Path(base)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root / ("us-entry-window-" + digest + "-" + trade_date + ".sqlite3")


def open_store(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    os.close(fd)
    db = sqlite3.connect(path, timeout=0.5)
    db.execute("PRAGMA busy_timeout=500")
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
    CREATE TABLE IF NOT EXISTS verified_bars (
        trade_date TEXT NOT NULL, symbol TEXT NOT NULL, owner TEXT NOT NULL,
        minute_et TEXT NOT NULL, source TEXT NOT NULL, row_json TEXT NOT NULL,
        observed_utc TEXT NOT NULL,
        PRIMARY KEY (trade_date,symbol,owner,minute_et,source)
    );
    CREATE TABLE IF NOT EXISTS quote_samples (
        trade_date TEXT NOT NULL, symbol TEXT NOT NULL, owner TEXT NOT NULL,
        received_epoch REAL NOT NULL, last REAL NOT NULL, source TEXT NOT NULL,
        generation TEXT NOT NULL, bid REAL, ask REAL,
        PRIMARY KEY (trade_date,symbol,owner,received_epoch,source,generation)
    );
    """)
    # Backward-compatible upgrade of an observer created by an earlier
    # candidate SHA. Never overwrite or invent historical quote prices.
    existing = {row[1] for row in db.execute("PRAGMA table_info(quote_samples)")}
    for column in ("bid", "ask"):
        if column not in existing:
            db.execute(f"ALTER TABLE quote_samples ADD COLUMN {column} REAL")
    db.commit()
    return db


def persist_bars(db: sqlite3.Connection, *, trade_date: str, symbol: str,
                 owner: str, native_rows: list[dict], now_et: datetime) -> int:
    """Idempotent native-bar persistence; no partial current minute."""
    if owner != "US_STANDARD" or symbol.upper() == "TQQQ":
        return 0
    verified = normalize_bars(native_rows, trade_date=trade_date, now_et=now_et)
    timestamp = datetime.now(timezone.utc).isoformat()
    changed = 0
    with db:
        for row in verified:
            prior = db.execute(
                "SELECT row_json FROM verified_bars WHERE trade_date=? AND symbol=? "
                "AND owner=? AND minute_et=? AND source='KIS_OVERSEAS_1M'",
                (trade_date, symbol.upper(), owner, row["time_et"]),
            ).fetchone()
            payload = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
            if prior and prior[0] == payload:
                continue
            db.execute(
                "INSERT INTO verified_bars VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(trade_date,symbol,owner,minute_et,source) "
                "DO UPDATE SET row_json=excluded.row_json, observed_utc=excluded.observed_utc",
                (trade_date, symbol.upper(), owner, row["time_et"],
                 "KIS_OVERSEAS_1M", payload, timestamp),
            )
            changed += 1
    return changed


def load_bars(db: sqlite3.Connection, *, trade_date: str, symbol: str,
              owner: str = "US_STANDARD", limit: int = 120) -> list[dict]:
    if owner != "US_STANDARD" or symbol.upper() == "TQQQ":
        return []
    rows = db.execute(
        "SELECT row_json FROM verified_bars WHERE trade_date=? AND symbol=? "
        "AND owner=? AND source='KIS_OVERSEAS_1M' ORDER BY minute_et DESC LIMIT ?",
        (trade_date, symbol.upper(), owner, max(1, min(240, limit))),
    ).fetchall()
    return [json.loads(row[0]) for row in reversed(rows)]


def persist_ws_sample(db: sqlite3.Connection, *, trade_date: str,
                      symbol: str, quote: dict, owner: str = "US_STANDARD") -> bool:
    """Audit-only WS quote. The reader must never call these values OHLCV."""
    if owner != "US_STANDARD" or symbol.upper() == "TQQQ":
        return False
    try:
        received = float(quote["received_at"])
        last = float(quote["last"])
        generation = str(quote["collector_generation"])
        if (not generation or received <= 0 or last <= 0 or
                not math.isfinite(last) or not math.isfinite(received) or
                abs(time.time() - received) > 15):
            return False
        bid_raw, ask_raw = quote.get("bid"), quote.get("ask")
        try:
            bid, ask = float(bid_raw), float(ask_raw)
            if not (math.isfinite(bid) and math.isfinite(ask)
                    and 0 < bid <= ask):
                bid, ask = None, None
        except (TypeError, ValueError, OverflowError):
            bid, ask = None, None
    except (TypeError, KeyError, ValueError, OverflowError):
        return False
    with db:
        db.execute(
            "INSERT OR IGNORE INTO quote_samples "
            "(trade_date,symbol,owner,received_epoch,last,source,generation,bid,ask) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (trade_date, symbol.upper(), owner, received, last,
             "KIS_WEBSOCKET_COLLECTOR", generation, bid, ask),
        )
    return True


def load_latest_ws_quote(db: sqlite3.Connection, *, trade_date: str,
                         symbol: str, now_epoch: float,
                         owner: str = "US_STANDARD") -> dict:
    if owner != "US_STANDARD" or symbol.upper() == "TQQQ":
        return {}
    row = db.execute(
        "SELECT received_epoch,last,generation,bid,ask FROM quote_samples "
        "WHERE trade_date=? AND symbol=? AND owner=? "
        "ORDER BY received_epoch DESC LIMIT 1",
        (trade_date, symbol.upper(), owner),
    ).fetchone()
    if row is None:
        return {}
    age = now_epoch - float(row[0])
    if age < -2 or age > 15:
        return {}
    # Bid and ask are not invented. The entry engine needs current KIS bid/ask.
    return {"last": float(row[1]), "age_sec": age,
            "bid": float(row[3]) if row[3] is not None else None,
            "ask": float(row[4]) if row[4] is not None else None,
            "received_at": float(row[0]),
            "source": "KIS_WEBSOCKET_COLLECTOR",
            "collector_generation": row[2]}


def load_quote_samples(db: sqlite3.Connection, *, trade_date: str,
                       symbol: str, owner: str = "US_STANDARD",
                       limit: int = 3000) -> list[dict]:
    """Return historical WS last/bid/ask as actually observed, never inferred."""
    if owner != "US_STANDARD" or symbol.upper() == "TQQQ":
        return []
    rows = db.execute(
        "SELECT received_epoch,last,bid,ask,generation FROM quote_samples "
        "WHERE trade_date=? AND symbol=? AND owner=? "
        "ORDER BY received_epoch DESC LIMIT ?",
        (trade_date, symbol.upper(), owner, max(1, min(10000, limit))),
    ).fetchall()
    return [
        {"received_at": float(row[0]), "last": float(row[1]),
         "bid": float(row[2]) if row[2] is not None else None,
         "ask": float(row[3]) if row[3] is not None else None,
         "collector_generation": row[4], "source": "KIS_WEBSOCKET_COLLECTOR"}
        for row in reversed(rows)
    ]


def standard_entry_pairs(watchlist: list[dict], limit: int = 30) -> list[tuple[str, str]]:
    """Canonicalize PREP exchanges without network and isolate strategy owner."""
    from trader.us.symbols import resolve_exchange
    aliases = {
        "NASDAQ": "NASDAQ", "NASD": "NASDAQ", "NAS": "NASDAQ",
        "NYSE": "NYSE", "NYS": "NYSE",
        "AMEX": "AMEX", "AMS": "AMEX",
    }
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for raw in watchlist or []:
        if not isinstance(raw, dict):
            continue
        symbol = str(raw.get("symbol") or "").upper().strip()
        meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
        owner = str(raw.get("strategy_owner") or meta.get("strategy_owner") or "US_STANDARD").upper()
        if not symbol or symbol == "TQQQ" or owner != "US_STANDARD" or symbol in seen:
            continue
        exchange_raw = str(raw.get("exchange") or "").upper().strip()
        if not exchange_raw:
            exchange_raw = str(resolve_exchange(symbol) or "").upper().strip()
        exchange = aliases.get(exchange_raw)
        if exchange is None:
            continue
        seen.add(symbol)
        pairs.append((symbol, exchange))
        if len(pairs) >= max(1, min(30, limit)):
            break
    return pairs


def run_observer(*, env: str, trade_date: str, scope: str,
                 parent_pid: int, watchlist: list[dict],
                 min_interval_sec: float = 10.0,
                 max_runtime_sec: float = 180.0 * 60.0) -> int:
    if env != "practice" or parent_pid < 2:
        return 2
    # The child cannot submit, cancel, or route orders. Existing KIS client
    # maintains its own practice-only guard; process permission also denies it.
    os.environ.update({
        "KIS_ENV": "practice", "DRY_RUN": "1", "ALLOW_REAL_ORDER": "0",
        "US_KIS_ORDER_ALLOWED": "0", "TRADING_REGION": "US",
    })
    pairs = standard_entry_pairs(watchlist)
    if not pairs:
        LOG.warning("[US_OPENING_OBSERVER][NO_WATCHLIST] action=NO_ORDER")
        return 2
    from trader.us.execution.kis_us_client import KisUSClient
    from trader.marketdata.us_ws_collector import get_quote, subscribe_many
    db = open_store(store_path(env=env, account_scope=scope, trade_date=trade_date))
    client = KisUSClient(env="practice", offline=False)
    ws_on = os.getenv("US_WS_COLLECTOR_ENABLED", "0") == "1"
    next_subscription_refresh = 0.0
    interval = max(6.0, float(min_interval_sec))
    start = time.monotonic()
    next_poll = 0.0
    pointer = 0
    fetch_enabled = os.getenv("US_OPENING_OBSERVER_BAR_FETCH_ENABLED", "1") == "1"
    try:
        while os.getppid() == parent_pid and time.monotonic() - start <= max_runtime_sec:
            now_et = datetime.now(NY)
            if now_et.date().isoformat() != trade_date or now_et.hour >= 16:
                break
            if (now_et.hour, now_et.minute) < (9, 30):
                time.sleep(2.0)
                continue
            if time.monotonic() < next_poll:
                time.sleep(min(1.0, next_poll - time.monotonic()))
                continue
            # Collector starts asynchronously; subscription warmup must
            # retry after readiness/reconnect, not fire once and disappear.
            if ws_on and time.monotonic() >= next_subscription_refresh:
                for _i in range(0, len(pairs), 16):
                    subscribe_many(pairs[_i:_i + 16], env=env, timeout_sec=0.3)
                next_subscription_refresh = time.monotonic() + 30.0
            symbol, exchange = pairs[pointer % len(pairs)]
            pointer += 1
            next_poll = time.monotonic() + interval
            try:
                quote = get_quote(symbol, exchange, env=env, timeout_sec=0.13)
                if quote:
                    persist_ws_sample(db, trade_date=trade_date,
                                      symbol=symbol, quote=quote)
            except (OSError, ValueError, RuntimeError) as exc:
                LOG.warning("[US_OPENING_OBSERVER][WS_MISS] symbol=%s error=%s",
                            symbol, type(exc).__name__)
            if not fetch_enabled:
                continue
            try:
                native = client.get_us_intraday_1m_bars(symbol, exchange, count=120)
                rows = persist_bars(db, trade_date=trade_date, symbol=symbol,
                                    owner="US_STANDARD", native_rows=native,
                                    now_et=datetime.now(NY))
                LOG.info("[US_OPENING_OBSERVER][BAR_UPDATE] symbol=%s new_rows=%d "
                         "source=KIS_OVERSEAS_1M action=NO_ORDER", symbol, rows)
            except Exception as exc:
                # Missing bars must remain WAIT. Never block an unrelated SELL.
                LOG.warning("[US_OPENING_OBSERVER][BAR_UNAVAILABLE] symbol=%s error=%s",
                            symbol, type(exc).__name__)
    finally:
        db.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["practice"], required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--parent-pid", type=int, required=True)
    parser.add_argument("--watchlist-file", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    with args.watchlist_file.open(encoding="utf-8") as fh:
        rows = json.load(fh)
    if not isinstance(rows, list):
        return 2
    return run_observer(
        env=args.env, trade_date=args.trade_date, scope=args.scope,
        parent_pid=args.parent_pid, watchlist=rows,
        min_interval_sec=float(os.getenv("US_OPENING_OBSERVER_INTERVAL_SEC", "10")),
    )


if __name__ == "__main__":
    sys.exit(main())
