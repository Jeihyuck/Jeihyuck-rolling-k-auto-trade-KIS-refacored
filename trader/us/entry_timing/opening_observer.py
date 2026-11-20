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
    CREATE TABLE IF NOT EXISTS bar_conflicts (
        trade_date TEXT NOT NULL, symbol TEXT NOT NULL, owner TEXT NOT NULL,
        minute_et TEXT NOT NULL, source TEXT NOT NULL,
        incoming_sha256 TEXT NOT NULL, original_row_json TEXT NOT NULL,
        incoming_row_json TEXT NOT NULL, observed_utc TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'UNRESOLVED',
        PRIMARY KEY (trade_date,symbol,owner,minute_et,source,incoming_sha256)
    );
    CREATE INDEX IF NOT EXISTS ix_us_minute_unresolved
    ON bar_conflicts(trade_date,symbol,owner,minute_et,status);
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
            if prior:
                if prior[0] == payload:
                    continue
                # Completed native minutes may be corrected or conflict. A
                # second observation is *not* automatically authoritative.
                # Preserve both payloads, quarantine only this minute, and
                # require independent native correction evidence to resolve.
                db.execute(
                    "INSERT OR IGNORE INTO bar_conflicts "
                    "(trade_date,symbol,owner,minute_et,source,incoming_sha256,"
                    "original_row_json,incoming_row_json,observed_utc,status) "
                    "VALUES (?,?,?,?,?,?,?,?,?,'UNRESOLVED')",
                    (trade_date, symbol.upper(), owner, row["time_et"],
                     "KIS_OVERSEAS_1M",
                     hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                     prior[0], payload, timestamp),
                )
                LOG.warning(
                    "[US_OPENING_OBSERVER][COMPLETED_BAR_CONFLICT] "
                    "symbol=%s minute=%s action=QUARANTINE_NO_REWRITE",
                    symbol.upper(), row["time_et"],
                )
                continue
            db.execute(
                "INSERT INTO verified_bars VALUES(?,?,?,?,?,?,?)",
                (trade_date, symbol.upper(), owner, row["time_et"],
                 "KIS_OVERSEAS_1M", payload, timestamp),
            )
            changed += 1
    return changed


def load_bars(db: sqlite3.Connection, *, trade_date: str, symbol: str,
              owner: str = "US_STANDARD", limit: int = 450) -> list[dict]:
    if owner != "US_STANDARD" or symbol.upper() == "TQQQ":
        return []
    rows = db.execute(
        "SELECT row_json FROM verified_bars WHERE trade_date=? AND symbol=? "
        "AND owner=? AND source='KIS_OVERSEAS_1M' "
        "AND NOT EXISTS (SELECT 1 FROM bar_conflicts c "
        "WHERE c.trade_date=verified_bars.trade_date "
        "AND c.symbol=verified_bars.symbol AND c.owner=verified_bars.owner "
        "AND c.minute_et=verified_bars.minute_et AND c.source=verified_bars.source "
        "AND c.status='UNRESOLVED') "
        "ORDER BY minute_et DESC LIMIT ?",
        (trade_date, symbol.upper(), owner, max(1, min(450, limit))),
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



def priority_manifest_path(*, env: str, scope: str, trade_date: str,
                           base: str | Path = "runtime/private") -> Path:
    """Same scoped day as the bar DB, different atomic read-only manifest."""
    return store_path(env=env, account_scope=scope, trade_date=trade_date,
                      base=base).with_suffix(".priority.json")


def publish_preminute_candidates(
    *, env: str, scope: str, trade_date: str, candidates: list[tuple],
    now_epoch: float | None = None, hot_limit: int = 4, warm_limit: int = 8,
    base: str | Path = "runtime/private",
) -> dict:
    """Publish a non-authorizing, pre-1m shortlist without KIS network I/O.

    Call before the actual 1m ENFORCE gate to avoid candidate starvation.
    Eligible scores are pre-minute facts, not an executable BUY. An atomic
    replace keeps observers from consuming truncated/part-written JSON.
    """
    import tempfile
    epoch = time.time() if now_epoch is None else float(now_epoch)
    if env != "practice" or not scope or not math.isfinite(epoch):
        raise ValueError("US_PRIORITY_SCOPE_INVALID")
    ranked = []
    seen = set()
    from trader.us.symbols import resolve_exchange
    for candidate in candidates:
        if not isinstance(candidate, (tuple, list)) or len(candidate) < 5:
            continue
        score, raw_symbol, raw_exchange, _price, meta = candidate[:5]
        sym = str(raw_symbol or "").strip().upper()
        meta = meta if isinstance(meta, dict) else {}
        owner = str(meta.get("strategy_owner") or "US_STANDARD").upper()
        if not sym or sym == "TQQQ" or owner != "US_STANDARD" or sym in seen:
            continue
        try:
            score_value = float(score)
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(score_value) or score_value <= 0:
            continue
        exchange = str(raw_exchange or resolve_exchange(sym) or "").upper()
        exchange = {"NASD": "NASDAQ", "NAS": "NASDAQ", "NYS": "NYSE",
                    "AMS": "AMEX"}.get(exchange, exchange)
        if exchange not in {"NASDAQ", "NYSE", "AMEX"}:
            continue
        seen.add(sym)
        ranked.append({"symbol": sym, "exchange": exchange, "score": score_value,
                       "position_state": str(meta.get("position_state") or "")})
    # First prioritize new candidates; retained/add BUY candidates are still
    # eligible for WARM collection and are not treated as global excludes.
    ranked.sort(key=lambda row: row["position_state"] == "HELD")
    hot = ranked[:max(0, min(30, int(hot_limit)))]
    warm = ranked[len(hot):len(hot) + max(0, min(30, int(warm_limit)))]
    payload = {
        "schema": "US_PREMINUTE_PRIORITY_V1", "env": env,
        "owner": "US_STANDARD", "trade_date": trade_date,
        "scope_sha256": hashlib.sha256((env + ":" + scope).encode()).hexdigest(),
        "created_epoch": epoch, "expires_epoch": epoch + 180.0,
        "generation": str(os.getpid()) + ":" + str(epoch),
        "hot": [{"symbol": x["symbol"], "exchange": x["exchange"]} for x in hot],
        "warm": [{"symbol": x["symbol"], "exchange": x["exchange"]} for x in warm],
        "is_order_authorization": False,
    }
    path = priority_manifest_path(env=env, scope=scope, trade_date=trade_date, base=base)
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=path.name + ".",
            suffix=".tmp", delete=False,
        ) as stream:
            temp_name = stream.name
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    finally:
        if temp_name is not None:
            try:
                Path(temp_name).unlink(missing_ok=True)
            except OSError:
                pass
    return {"hot": len(hot), "warm": len(warm), "generation": payload["generation"]}


def load_preminute_priority(
    *, env: str, scope: str, trade_date: str,
    pairs: list[tuple[str, str]], now_epoch: float | None = None,
    base: str | Path = "runtime/private",
) -> tuple[dict[str, list[tuple[str, str]]], str]:
    """Return validated HOT/WARM/COLD, or explicit MISSING/STALE/INVALID."""
    available = {sym: exch for sym, exch in pairs}
    fallback = {"HOT": [], "WARM": [], "COLD": list(pairs)}
    path = priority_manifest_path(env=env, scope=scope, trade_date=trade_date, base=base)
    try:
        if not path.is_file():
            return fallback, "MISSING"
        if path.stat().st_size > 65536:
            return fallback, "INVALID"
        payload = json.loads(path.read_text(encoding="utf-8"))
        epoch = time.time() if now_epoch is None else float(now_epoch)
        if (not isinstance(payload, dict)
                or payload.get("schema") != "US_PREMINUTE_PRIORITY_V1"
                or payload.get("owner") != "US_STANDARD"
                or payload.get("env") != env
                or payload.get("trade_date") != trade_date
                or payload.get("is_order_authorization") is not False
                or payload.get("scope_sha256") != hashlib.sha256(
                    (env + ":" + scope).encode()).hexdigest()):
            return fallback, "INVALID"
        created = float(payload["created_epoch"])
        expires = float(payload["expires_epoch"])
        if not all(map(math.isfinite, (created, expires, epoch))):
            return fallback, "INVALID"
        if created > epoch + 5 or expires > created + 180.0 + 1e-5:
            return fallback, "INVALID"
        if expires < epoch:
            return fallback, "STALE"
        lanes: dict[str, list[tuple[str, str]]] = {"HOT": [], "WARM": [], "COLD": []}
        selected = set()
        for lane, cap in (("HOT", 4), ("WARM", 8)):
            rows = payload.get(lane.lower())
            if not isinstance(rows, list) or len(rows) > cap:
                return fallback, "INVALID"
            for raw in rows:
                if not isinstance(raw, dict):
                    return fallback, "INVALID"
                sym = str(raw.get("symbol") or "").upper()
                exch = str(raw.get("exchange") or "").upper()
                if sym not in available or exch != available[sym] or sym in selected:
                    return fallback, "INVALID"
                selected.add(sym)
                lanes[lane].append((sym, exch))
        lanes["COLD"] = [p for p in pairs if p[0] not in selected]
        return lanes, "VALID"
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return fallback, "INVALID"


def next_priority_pair(
    lanes: dict[str, list[tuple[str, str]]],
    *, slot: int, offsets: dict[str, int],
) -> tuple[tuple[str, str], str] | None:
    """Fixed 4:1:1 request budget, with non-starving low-priority discovery."""
    schedule = ("HOT", "HOT", "HOT", "HOT", "WARM", "COLD")
    preferred = schedule[slot % len(schedule)]
    for tier in (preferred, "HOT", "WARM", "COLD"):
        lane = lanes.get(tier) or []
        if lane:
            pos = offsets.get(tier, 0)
            offsets[tier] = pos + 1
            return lane[pos % len(lane)], tier
    return None


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
    priority_offsets = {"HOT": 0, "WARM": 0, "COLD": 0}
    last_priority_state = None
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
            lanes, priority_state = load_preminute_priority(
                env=env, scope=scope, trade_date=trade_date,
                pairs=pairs, now_epoch=time.time(),
            )
            if pointer == 0 or priority_state != last_priority_state:
                LOG.info(
                    "[US_OPENING_OBSERVER][PRIORITY_SCOPE] state=%s hot=%d warm=%d cold=%d "
                    "source=PREMINUTE_NOT_ORDER_AUTHORITY",
                    priority_state, len(lanes["HOT"]), len(lanes["WARM"]),
                    len(lanes["COLD"]),
                )
            last_priority_state = priority_state
            picked = next_priority_pair(lanes, slot=pointer, offsets=priority_offsets)
            pointer += 1
            if picked is None:
                LOG.warning("[US_OPENING_OBSERVER][NO_COLLECTION_TARGET] action=NO_ORDER")
                next_poll = time.monotonic() + interval
                continue
            (symbol, exchange), tier = picked
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
