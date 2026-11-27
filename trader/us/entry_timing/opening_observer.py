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
    CREATE TABLE IF NOT EXISTS bar_repair_candidate (
        trade_date TEXT NOT NULL, symbol TEXT NOT NULL, owner TEXT NOT NULL,
        minute_et TEXT NOT NULL, candidate_json TEXT NOT NULL,
        first_epoch REAL NOT NULL, last_epoch REAL NOT NULL,
        response_ids_json TEXT NOT NULL,
        PRIMARY KEY (trade_date,symbol,owner,minute_et)
    );
    CREATE TABLE IF NOT EXISTS bar_conflict_resolution_history (
        trade_date TEXT NOT NULL, symbol TEXT NOT NULL, owner TEXT NOT NULL,
        minute_et TEXT NOT NULL, resolved_epoch REAL NOT NULL,
        original_row_json TEXT, accepted_row_json TEXT NOT NULL,
        response_ids_json TEXT NOT NULL,
        PRIMARY KEY (trade_date,symbol,owner,minute_et,resolved_epoch)
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


def _observe_conflict_candidate(db, *, key, payload, prior_payload,
                                observation_id, observed_epoch, timestamp):
    """Resolve only three separately received, spaced native HTTP observations.

    Replaying the same rows or local quote data can never increment proof.
    A different candidate resets the sequence. Original conflict and
    correction history remain durable.
    """
    prev = db.execute(
        "SELECT candidate_json,first_epoch,last_epoch,response_ids_json FROM bar_repair_candidate "
        "WHERE trade_date=? AND symbol=? AND owner=? AND minute_et=?", key).fetchone()
    if prev and prev[0] != payload:
        db.execute("DELETE FROM bar_repair_candidate WHERE trade_date=? AND symbol=? AND owner=? AND minute_et=?", key)
        prev = None
    if not observation_id or observed_epoch is None or not math.isfinite(observed_epoch):
        return False
    first, last, ids = (prev[1], prev[2], json.loads(prev[3])) if prev else (observed_epoch, observed_epoch, [])
    if observation_id in ids or (ids and observed_epoch-last < 60.0):
        return False
    ids.append(observation_id)
    last = observed_epoch
    db.execute("INSERT INTO bar_repair_candidate VALUES(?,?,?,?,?,?,?,?) "
               "ON CONFLICT(trade_date,symbol,owner,minute_et) DO UPDATE SET "
               "candidate_json=excluded.candidate_json,first_epoch=excluded.first_epoch,"
               "last_epoch=excluded.last_epoch,response_ids_json=excluded.response_ids_json",
               (*key, payload, first, last, json.dumps(ids)))
    if len(ids) < 3 or last-first < 120.0:
        return False
    db.execute("INSERT INTO bar_conflict_resolution_history VALUES(?,?,?,?,?,?,?,?)",
               (*key, last, prior_payload, payload, json.dumps(ids)))
    db.execute("INSERT INTO verified_bars VALUES(?,?,?,?,?,?,?) "
               "ON CONFLICT(trade_date,symbol,owner,minute_et,source) DO UPDATE SET "
               "row_json=excluded.row_json,observed_utc=excluded.observed_utc",
               (*key, "KIS_OVERSEAS_1M", payload, timestamp))
    db.execute("UPDATE bar_conflicts SET status='RESOLVED' WHERE trade_date=? AND symbol=? "
               "AND owner=? AND minute_et=? AND source='KIS_OVERSEAS_1M' AND status='UNRESOLVED'", key)
    db.execute("DELETE FROM bar_repair_candidate WHERE trade_date=? AND symbol=? AND owner=? AND minute_et=?", key)
    LOG.info("[US_OPENING_OBSERVER][CONFLICT_RESOLVED] symbol=%s minute=%s independent_reads=%d span_sec=%.1f",
             key[1], key[3], len(ids), last-first)
    return True


def persist_bars(db: sqlite3.Connection, *, trade_date: str, symbol: str,
                 owner: str, native_rows: list[dict], now_et: datetime,
                 observation_id: str | None = None, observed_epoch: float | None = None) -> int:
    """Idempotent native-bar persistence; no partial current minute."""
    if owner != "US_STANDARD" or symbol.upper() == "TQQQ":
        return 0
    verified = normalize_bars(native_rows, trade_date=trade_date, now_et=now_et)
    timestamp = datetime.now(timezone.utc).isoformat()
    # normalize_bars intentionally produces one bar per completed minute for
    # ordinary decisions. Before that deduplication can hide contradictory
    # native rows inside a SINGLE KIS response, compare the normalized source
    # rows independently and keep both versions as durable conflict evidence.
    _batch_versions: dict[str, dict[str, str]] = {}
    for raw in native_rows or []:
        for normalized in normalize_bars(
            [raw], trade_date=trade_date, now_et=now_et,
        ):
            payload = json.dumps(
                normalized, sort_keys=True, separators=(",", ":"), allow_nan=False,
            )
            minute = normalized["time_et"]
            _batch_versions.setdefault(minute, {})[
                hashlib.sha256(payload.encode("utf-8")).hexdigest()
            ] = payload
    _batch_conflicts = {
        minute: versions for minute, versions in _batch_versions.items()
        if len(versions) > 1
    }
    changed = 0
    with db:
        for minute, versions in _batch_conflicts.items():
            payloads = list(versions.values())
            original = payloads[0]
            for new_payload in payloads[1:]:
                db.execute(
                    "INSERT OR IGNORE INTO bar_conflicts "
                    "(trade_date,symbol,owner,minute_et,source,incoming_sha256,"
                    "original_row_json,incoming_row_json,observed_utc,status) "
                    "VALUES (?,?,?,?,?,?,?,?,?,'UNRESOLVED')",
                    (
                        trade_date, symbol.upper(), owner, minute,
                        "KIS_OVERSEAS_1M",
                        hashlib.sha256(new_payload.encode("utf-8")).hexdigest(),
                        original, new_payload, timestamp,
                    ),
                )
            # A formerly resolved variant may recur; reopen quarantine
            # without erasing the earlier resolution history.
            db.execute("UPDATE bar_conflicts SET status='UNRESOLVED' WHERE trade_date=? AND symbol=? "
                       "AND owner=? AND minute_et=? AND source='KIS_OVERSEAS_1M'",
                       (trade_date, symbol.upper(), owner, minute))
            db.execute("DELETE FROM bar_repair_candidate WHERE trade_date=? AND symbol=? AND owner=? AND minute_et=?",
                       (trade_date, symbol.upper(), owner, minute))
            LOG.warning(
                "[US_OPENING_OBSERVER][SAME_BATCH_NATIVE_BAR_CONFLICT] "
                "symbol=%s minute=%s variants=%d action=QUARANTINE_NO_REWRITE",
                symbol.upper(), minute, len(payloads),
            )
        for row in verified:
            if row["time_et"] in _batch_conflicts:
                # Never let an arbitrary response ordering select an authority
                # for the conflicted minute. Existing trusted rows are also
                # excluded by load_bars() via the durable conflict quarantine.
                continue
            prior = db.execute(
                "SELECT row_json FROM verified_bars WHERE trade_date=? AND symbol=? "
                "AND owner=? AND minute_et=? AND source='KIS_OVERSEAS_1M'",
                (trade_date, symbol.upper(), owner, row["time_et"]),
            ).fetchone()
            payload = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
            key = (trade_date, symbol.upper(), owner, row["time_et"])
            if prior and prior[0] != payload:
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
                db.execute("UPDATE bar_conflicts SET status='UNRESOLVED' WHERE trade_date=? AND symbol=? "
                           "AND owner=? AND minute_et=? AND source='KIS_OVERSEAS_1M' AND incoming_sha256=?",
                           (*key, hashlib.sha256(payload.encode("utf-8")).hexdigest()))
                LOG.warning(
                    "[US_OPENING_OBSERVER][COMPLETED_BAR_CONFLICT] "
                    "symbol=%s minute=%s action=QUARANTINE_NO_REWRITE",
                    symbol.upper(), row["time_et"],
                )
            unresolved = db.execute(
                "SELECT 1 FROM bar_conflicts WHERE trade_date=? AND symbol=? AND owner=? "
                "AND minute_et=? AND source='KIS_OVERSEAS_1M' AND status='UNRESOLVED' LIMIT 1", key
            ).fetchone()
            if unresolved:
                changed += int(_observe_conflict_candidate(
                    db, key=key, payload=payload, prior_payload=prior[0] if prior else None,
                    observation_id=observation_id, observed_epoch=observed_epoch, timestamp=timestamp))
                continue
            if prior:
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
    base: str | Path = "runtime/private", prep_contract: dict | None = None,
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
    hot = ranked[:max(0, min(4, int(hot_limit)))]
    warm = ranked[len(hot):len(hot) + max(0, min(8, int(warm_limit)))]
    payload = {
        "schema": "US_PREMINUTE_PRIORITY_V1", "env": env,
        "owner": "US_STANDARD", "trade_date": trade_date,
        "scope_sha256": hashlib.sha256((env + ":" + scope).encode()).hexdigest(),
        "created_epoch": epoch, "expires_epoch": epoch + 180.0,
        "generation": str(os.getpid()) + ":" + str(epoch),
        "hot": [{"symbol": x["symbol"], "exchange": x["exchange"]} for x in hot],
        "warm": [{"symbol": x["symbol"], "exchange": x["exchange"]} for x in warm],
        "cold": [{"symbol": x["symbol"], "exchange": x["exchange"]} for x in ranked[len(hot)+len(warm):]],
        "is_order_authorization": False,
    }
    path = priority_manifest_path(env=env, scope=scope, trade_date=trade_date, base=base)
    if prep_contract is not None:
        from datetime import date
        from trader.us.market_calendar import is_us_trading_day, regular_close_time_for_date
        day = date.fromisoformat(trade_date)
        close = datetime.combine(day, regular_close_time_for_date(day), tzinfo=NY).timestamp()
        if not is_us_trading_day(day) or epoch >= close:
            raise ValueError("PREP_COLLECTION_OUTSIDE_SESSION")
        payload.update(schema="US_PREP_COLLECTION_PRIORITY_V2", market="US",
                       kind="COLLECTION_PRIORITY", expires_epoch=close,
                       final30_digest=prep_contract["final30_hash"],
                       prep_contract_hash=_prep_collection_contract_hash(prep_contract),
                       source_code_sha=prep_contract["run_revision"],
                       selection_reason="PREP_FINAL30_EXISTING_RANK",
                       position_and_quantity_state="PROVISIONAL_UNVERIFIED")
        path = path.with_suffix(".prep.json")
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


def _prep_collection_contract_hash(contract: dict) -> str:
    return hashlib.sha256(json.dumps(contract, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def publish_prep_collection_priority(*, env, scope, trade_date, rows, contract,
                                     now_epoch=None, base="runtime/private"):
    """Existing PREP rank, never a new scoring or order decision."""
    from trader.us.prep_contract import _compute_final30_hash
    if (not isinstance(contract, dict) or contract.get("market") != "US"
            or contract.get("env") != env or contract.get("trade_date") != trade_date
            or contract.get("contract_ok") is not True
            or not contract.get("run_revision") or contract["run_revision"] == "unknown"
            or contract.get("final30_hash") != _compute_final30_hash({"final30_scored": rows})):
        raise ValueError("PREP_COLLECTION_CONTRACT_INVALID")
    candidates = []
    for row in sorted(rows, key=lambda x: int(x.get("rank_final30") or 999)):
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        candidates.append((row.get("score_final") or row.get("final_score") or row.get("score"),
                           row.get("symbol"), row.get("exchange"), None,
                           {**meta, "strategy_owner": row.get("strategy_owner") or meta.get("strategy_owner") or "US_STANDARD",
                            "position_state": "PROVISIONAL_UNKNOWN"}))
    return publish_preminute_candidates(env=env, scope=scope, trade_date=trade_date,
               candidates=candidates, now_epoch=now_epoch, base=base, prep_contract=contract)


def load_preminute_priority(
    *, env: str, scope: str, trade_date: str,
    pairs: list[tuple[str, str]], now_epoch: float | None = None,
    base: str | Path = "runtime/private", prep_only: bool = False,
    verified_prep_contract: dict | None = None,
) -> tuple[dict[str, list[tuple[str, str]]], str]:
    """Return validated HOT/WARM/COLD, or explicit MISSING/STALE/INVALID."""
    available = {sym: exch for sym, exch in pairs}
    fallback = {"HOT": [], "WARM": [], "COLD": []}
    path = priority_manifest_path(env=env, scope=scope, trade_date=trade_date, base=base)
    if prep_only:
        path = path.with_suffix(".prep.json")
    try:
        if not path.is_file():
            return fallback, "MISSING"
        if path.stat().st_size > 65536:
            return fallback, "INVALID"
        payload = json.loads(path.read_text(encoding="utf-8"))
        epoch = time.time() if now_epoch is None else float(now_epoch)
        if (not isinstance(payload, dict)
                or payload.get("schema") != ("US_PREP_COLLECTION_PRIORITY_V2" if prep_only else "US_PREMINUTE_PRIORITY_V1")
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
        if created > epoch + 5:
            return fallback, "INVALID"
        if prep_only:
            from datetime import date
            from trader.us.market_calendar import regular_close_time_for_date, is_us_trading_day
            day = date.fromisoformat(trade_date)
            close = datetime.combine(day, regular_close_time_for_date(day), tzinfo=NY).timestamp()
            c = verified_prep_contract or {}
            if (not is_us_trading_day(day) or expires != close
                    or payload.get("market") != "US" or payload.get("kind") != "COLLECTION_PRIORITY"
                    or c.get("contract_ok") is not True or c.get("env") != env
                    or c.get("trade_date") != trade_date or c.get("market") != "US"
                    or payload.get("final30_digest") != c.get("final30_hash")
                    or payload.get("source_code_sha") != c.get("run_revision")
                    or payload.get("prep_contract_hash") != _prep_collection_contract_hash(c)):
                return fallback, "INVALID"
        elif expires > created + 180.0 + 1e-5:
            return fallback, "INVALID"
        if expires < epoch:
            return fallback, "STALE"
        lanes: dict[str, list[tuple[str, str]]] = {"HOT": [], "WARM": [], "COLD": []}
        selected = set()
        for lane, cap in (("HOT", 4), ("WARM", 8), ("COLD", 30)):
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
        return lanes, "PREP_VALID" if prep_only else "VALID"
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return fallback, "INVALID"


def merge_collection_priorities(execution, prep):
    """Collection-only rank: eligible execution symbols first, then PREP.
    
    An empty execution shortlist is not a prohibition on observing verified
    PREP candidates. Both manifests were scope-checked before this merge.
    Never return a broker permission or admit a symbol outside those lists.
    """
    ranked = list(dict.fromkeys(
        execution["HOT"] + execution["WARM"] +
        prep["HOT"] + prep["WARM"] + execution["COLD"] + prep["COLD"]
    ))[:30]
    return {"HOT": ranked[:4], "WARM": ranked[4:12], "COLD": ranked[12:]}


def collection_priorities(*, env, scope, trade_date, pairs, now_epoch=None,
                          base="runtime/private", allow_prep_rebind=False):
    """Keep PREP observation independent from execution eligibility.

    A full-day observer may rebind to a newly verified same-day PREP
    contract after recovery replaces Final30. Rebind only after checking the
    account/epoch-scoped collection manifest; never trust a loose watchlist.
    """
    kw = dict(env=env, scope=scope, trade_date=trade_date, pairs=pairs,
              now_epoch=now_epoch, base=base)
    lanes, state = load_preminute_priority(**kw)
    try:
        from trader.us.path_contract import us_prep_contract_path, us_final30_scored_path
        from trader.us.prep_contract import _compute_final30_hash
        contract = json.loads(us_prep_contract_path(trade_date).read_text(encoding="utf-8"))
        payload = json.loads(us_final30_scored_path(trade_date).read_text(encoding="utf-8"))
        rows = payload.get("final30_scored") or []
        if (payload.get("trade_date") != trade_date or payload.get("env") != env
                or contract.get("final30_hash") != _compute_final30_hash({"final30_scored": rows})):
            return lanes, state
        verified_pairs = standard_entry_pairs(rows)
        changed = set(verified_pairs) != set(pairs)
        if changed and not allow_prep_rebind:
            return lanes, state
        verified_kw = {**kw, "pairs":verified_pairs}
        fallback, prep_state = load_preminute_priority(**verified_kw, prep_only=True,
                                                        verified_prep_contract=contract)
        if prep_state == "PREP_VALID":
            if changed:
                # Revalidate the execution list against the NEW universe;
                # old removed symbols cannot survive as collection targets.
                lanes, state = load_preminute_priority(**verified_kw)
            if state == "VALID":
                return merge_collection_priorities(lanes, fallback), "VALID"
            return fallback, prep_state
        if changed:
            return {"HOT":[],"WARM":[],"COLD":[]}, "PREP_REBIND_UNVERIFIED"
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        pass
    return lanes, state


def next_priority_pair(
    lanes: dict[str, list[tuple[str, str]]],
    *, slot: int, offsets: dict[str, int],
) -> tuple[tuple[str, str], str] | None:
    """V2 baseline 4:1:1; unused slots may serve HOT, never expand COLD budget."""
    if any(len(lanes.get(tier) or []) > cap for tier, cap in (("HOT", 4), ("WARM", 8), ("COLD", 30))):
        return None
    schedule = ("HOT", "HOT", "HOT", "HOT", "WARM", "COLD")
    preferred = schedule[slot % len(schedule)]
    tier = preferred if lanes.get(preferred) else "HOT"
    lane = lanes.get(tier) or []
    if not lane:
        return None
    pos = offsets.get(tier, 0)
    offsets[tier] = pos + 1
    return lane[pos % len(lane)], tier


def collect_hot_minutes(client, db, *, symbol, exchange, trade_date,
                        repair_due: bool, still_hot, max_pages: int = 4) -> dict:
    """Latest first, then bounded native historical pages only for a HOT symbol."""
    from datetime import timedelta
    if not still_hot():
        return {"requests": 0, "changed": 0}
    cursor = ""
    seen = set()
    requests = changed = 0
    started = time.monotonic()
    while requests < max(1, min(4, int(max_pages))) and still_hot():
        page = client.get_us_intraday_1m_page(symbol, exchange, before_key=cursor)
        requests += 1
        requested_at = page["requested_at"].astimezone(NY)
        if requested_at.date().isoformat() != trade_date:
            break
        changed += persist_bars(db, trade_date=trade_date, symbol=symbol,
                                owner="US_STANDARD", native_rows=page["rows"],
                                now_et=requested_at, observation_id=page["observation_id"],
                                observed_epoch=page["observed_epoch"])
        if not repair_due or time.monotonic() - started > 8.0:
            break
        stored = load_bars(db, trade_date=trade_date, symbol=symbol)
        present = {b["time_et"] for b in stored}
        targets = [row[0] for row in db.execute(
            "SELECT DISTINCT minute_et FROM bar_conflicts WHERE trade_date=? AND symbol=? "
            "AND owner='US_STANDARD' AND status='UNRESOLVED'", (trade_date, symbol.upper()))]
        opening = requested_at.replace(hour=9, minute=30, second=0, microsecond=0)
        elapsed = min(30, max(0, int((requested_at-opening).total_seconds() // 60)))
        targets.extend((opening + timedelta(minutes=i)).isoformat() for i in range(elapsed)
                       if (opening + timedelta(minutes=i)).isoformat() not in present)
        latest_boundary = requested_at.replace(second=0, microsecond=0)
        for i in range(1, 5):
            at = latest_boundary - timedelta(minutes=i)
            if at >= opening and at.isoformat() not in present:
                targets.append(at.isoformat())
        next_key = str(page.get("next_key") or "")
        # KEYB is derived from the actual preceding response, never a
        # fabricated anchor. Only continue if unresolved evidence is older.
        if (not targets or not next_key or next_key in seen
                or next_key[:8] != trade_date.replace("-", "")
                or (cursor and next_key >= cursor)
                or min(datetime.fromisoformat(t).strftime("%Y%m%d%H%M%S") for t in targets) > next_key):
            break
        seen.add(next_key)
        cursor = next_key
        LOG.info("[US_OPENING_OBSERVER][HISTORICAL_REPAIR] symbol=%s cursor=%s targets=%d",
                 symbol, cursor, len(targets))
    return {"requests": requests, "changed": changed}


def start_prep_observer(*, env: str, scope: str, trade_date: str, rows: list[dict],
                        now_et=None, base="runtime/private", log_base="runtime/logs"):
    """Launch the existing singleton READ-ONLY observer after verified PREP.

    No new broker client/order authority; the child owns its existing lock.
    AM/PM keep their existing recovery launch in case the child died.
    """
    from datetime import date
    from trader.us.market_calendar import is_us_trading_day, regular_close_time_for_date
    import subprocess
    import tempfile
    now_et = now_et or datetime.now(NY)
    day = date.fromisoformat(trade_date)
    if (env != "practice" or now_et.tzinfo is None
            or now_et.astimezone(NY).date() != day or not is_us_trading_day(day)
            or now_et.astimezone(NY).time() >= regular_close_time_for_date(day)):
        return None
    pairs = standard_entry_pairs(rows)
    if not pairs:
        return None
    key = hashlib.sha256((env + ":" + scope).encode()).hexdigest()[:20]
    root = Path(base)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    watchlist = root / ("us-opening-watchlist-" + key + "-" + trade_date + ".json")
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=root,
                                         delete=False) as handle:
            temp_name = handle.name
            json.dump([{"symbol": sym, "exchange": exch} for sym,exch in pairs], handle)
        os.replace(temp_name, watchlist)
    finally:
        if temp_name:
            Path(temp_name).unlink(missing_ok=True)
    log_dir = Path(log_base)
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / ("us-opening-observer-" + trade_date + "-" + key + ".log")
    flags = os.O_CREAT | os.O_APPEND | os.O_WRONLY | os.O_NOFOLLOW
    with os.fdopen(os.open(path, flags, 0o600), "a", encoding="utf-8") as stream:
        return subprocess.Popen(
            [sys.executable, "-m", "trader.us.entry_timing.opening_observer",
             "--env", env, "--scope", scope, "--trade-date", trade_date,
             "--parent-pid", str(os.getpid()), "--watchlist-file", str(watchlist),
             "--continuous"],
            env={**os.environ, "KIS_ENV":"practice", "DRY_RUN":"1",
                 "ALLOW_REAL_ORDER":"0", "US_KIS_ORDER_ALLOWED":"0",
                 "TRADING_REGION":"US", "US_ORDER_ARMED":"0",
                 "LIVE_TRADING_ENABLED":"0", "US_LIVE_TRADING_ENABLED":"0"},
            stdin=subprocess.DEVNULL, stdout=stream, stderr=stream,
            close_fds=True, start_new_session=True,
        )


def run_observer(*, env: str, trade_date: str, scope: str,
                 parent_pid: int, watchlist: list[dict],
                 min_interval_sec: float = 10.0,
                 max_runtime_sec: float = 180.0 * 60.0,
                 continuous: bool = False) -> int:
    if env != "practice" or (not continuous and parent_pid < 2):
        return 2
    # The child cannot submit, cancel, or route orders. Existing KIS client
    # maintains its own practice-only guard; process permission also denies it.
    os.environ.update({
        "KIS_ENV": "practice", "DRY_RUN": "1", "ALLOW_REAL_ORDER": "0",
        "US_KIS_ORDER_ALLOWED": "0", "TRADING_REGION": "US",
        "US_ORDER_ARMED": "0", "LIVE_TRADING_ENABLED": "0",
        "US_LIVE_TRADING_ENABLED": "0",
    })
    from trader.us.market_calendar import is_us_trading_day, regular_close_time_for_date
    from datetime import date
    day = date.fromisoformat(trade_date)
    if not is_us_trading_day(day):
        LOG.info("[US_OPENING_OBSERVER][HOLIDAY] date=%s action=NO_COLLECTION", trade_date)
        return 0
    close_time = regular_close_time_for_date(day)
    pairs = standard_entry_pairs(watchlist)
    if not pairs:
        LOG.warning("[US_OPENING_OBSERVER][NO_WATCHLIST] action=NO_ORDER")
        return 2
    from trader.us.execution.kis_us_client import KisUSClient
    from trader.marketdata.us_ws_collector import get_quote, subscribe_many, ensure_started
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
    next_repair_by_symbol = {}
    next_epoch_check = 0.0
    epoch_verified = not continuous
    fetch_enabled = os.getenv("US_OPENING_OBSERVER_BAR_FETCH_ENABLED", "1") == "1"
    try:
        while (continuous or os.getppid() == parent_pid) and time.monotonic() - start <= max_runtime_sec:
            now_et = datetime.now(NY)
            if now_et.date().isoformat() != trade_date or now_et.time() >= close_time:
                break
            if continuous and time.monotonic() >= next_epoch_check:
                next_epoch_check = time.monotonic() + 60.0
                try:
                    from trader.db.trading_epoch import active_trading_epoch_id
                    active = active_trading_epoch_id(env="practice", required=True)
                    epoch_verified = bool(active and scope.rsplit(":", 1)[-1] == str(active))
                    if not epoch_verified:
                        LOG.warning("[US_OPENING_OBSERVER][EPOCH_CHANGED] action=STOP_COLLECTION")
                        break
                except Exception as exc:
                    epoch_verified = False
                    LOG.warning("[US_OPENING_OBSERVER][EPOCH_UNVERIFIED] type=%s action=PAUSE_COLLECTION", type(exc).__name__)
            if not epoch_verified:
                time.sleep(2.0)
                continue
            if (now_et.hour, now_et.minute) < (9, 25):
                time.sleep(2.0)
                continue
            if time.monotonic() < next_poll:
                time.sleep(min(1.0, next_poll - time.monotonic()))
                continue
            # Collector starts asynchronously; subscription warmup must
            # retry after readiness/reconnect, not fire once and disappear.
            lanes, priority_state = collection_priorities(
                env=env, scope=scope, trade_date=trade_date,
                pairs=pairs, now_epoch=time.time(), allow_prep_rebind=continuous,
            )
            if ws_on and time.monotonic() >= next_subscription_refresh:
                try:
                    ensure_started(env=env, offline=False)
                except Exception as exc:
                    LOG.warning("[US_OPENING_OBSERVER][COLLECTOR_START_WARN] type=%s", type(exc).__name__)
                from trader.us.symbols import resolve_quote_exchange
                benchmark_pairs = [(sym, resolve_quote_exchange(sym)) for sym in ("SPY","QQQ","SMH")]
                # Benchmark subscriptions are independent of the entry shortlist.
                # Do not bulk-prewarm COLD ahead of held/exit-owner quotes.
                targets = list(dict.fromkeys(benchmark_pairs + lanes["HOT"] + lanes["WARM"]))
                for _i in range(0, len(targets), 16):
                    subscribe_many(targets[_i:_i + 16], env=env, timeout_sec=0.3)
                next_subscription_refresh = time.monotonic() + 30.0
            if (now_et.hour, now_et.minute) < (9, 30):
                time.sleep(2.0)
                continue
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
            quote = None
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
                def _still_hot():
                    current, state = collection_priorities(env=env, scope=scope,
                        trade_date=trade_date, pairs=pairs, now_epoch=time.time(),
                        allow_prep_rebind=continuous)
                    live = datetime.now(NY)
                    return (epoch_verified and live.date().isoformat() == trade_date
                            and live.time() < close_time and state in {"VALID", "PREP_VALID"}
                            and (symbol, exchange) in current[tier])
                repair_due = tier == "HOT" and time.monotonic() >= next_repair_by_symbol.get(symbol, 0.0)
                if repair_due:
                    next_repair_by_symbol[symbol] = time.monotonic() + 120.0
                result = collect_hot_minutes(client, db, symbol=symbol, exchange=exchange,
                    trade_date=trade_date, repair_due=repair_due, still_hot=_still_hot, max_pages=4 if tier == "HOT" else 1)
                LOG.info("[US_OPENING_OBSERVER][BAR_UPDATE] symbol=%s new_rows=%d requests=%d "
                         "source=KIS_OVERSEAS_1M action=NO_ORDER", symbol, result["changed"], result["requests"])
                from trader.marketdata.opening_range_intelligence import analyze_opening_range
                summary = analyze_opening_range(
                    market="US", trade_date=trade_date, now=datetime.now(NY),
                    bars=load_bars(db, trade_date=trade_date, symbol=symbol),
                    # Market-specific structural target must be provided by
                    # the actual strategy adapter before a ceiling is emitted.
                    quote=quote or {},
                )
                LOG.info("[US_OPENING_OBSERVER][OPENING_INTELLIGENCE] symbol=%s tier=%s %s",
                         symbol, tier, json.dumps(summary, sort_keys=True, allow_nan=False))
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
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    with args.watchlist_file.open(encoding="utf-8") as fh:
        rows = json.load(fh)
    if not isinstance(rows, list):
        return 2
    kwargs = dict(env=args.env, trade_date=args.trade_date, scope=args.scope,
                  parent_pid=args.parent_pid, watchlist=rows,
                  min_interval_sec=float(os.getenv("US_OPENING_OBSERVER_INTERVAL_SEC", "10")))
    if not args.continuous:
        return run_observer(**kwargs)
    # Linux/WSL process-held advisory lock, not a PID file. A second AM/PM
    # launcher exits without opening the DB or calling the broker. Never
    # unlink the lock file (which could allow separate locked inodes).
    import fcntl
    lock = store_path(env=args.env, account_scope=args.scope,
                      trade_date=args.trade_date).with_suffix(".observer.lock")
    fd = os.open(str(lock), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            LOG.info("[US_OPENING_OBSERVER][ALREADY_RUNNING] action=NO_COLLECTION")
            return 0
        return run_observer(**kwargs, continuous=True, max_runtime_sec=8*3600)
    finally:
        os.close(fd)


if __name__ == "__main__":
    sys.exit(main())
