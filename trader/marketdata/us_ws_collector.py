"""US read-only KIS WebSocket collector, isolated from trading tick processes.

This process has no broker order routes and communicates through short-lived
Unix domain socket requests. It must never run as a thread in a forked parent.
The collector is opt-in until deployed and measured in the real WSL environment.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

logger = logging.getLogger(__name__)
_MAX_MESSAGE_BYTES = 4096
_MAX_REPLY_BYTES = 8192
_LAST_START_REQUEST: subprocess.Popen | None = None


def enabled() -> bool:
    return str(os.getenv("US_WS_COLLECTOR_ENABLED", "0")).lower() in {"1", "yes", "true", "on"}


def _account_identity(env: str) -> str:
    from trader.account_state import get_account_key
    account = str(get_account_key(env=env) or "").strip()
    if not account:
        raise RuntimeError("cannot isolate WS collector without account identity")
    return hashlib.sha256((str(env).lower() + ":" + account).encode()).hexdigest()[:24]


def socket_path(env: str) -> Path:
    root = Path(__file__).resolve().parents[2] / "runtime" / "private"
    root.mkdir(parents=True, exist_ok=True)
    return root / ("us-ws-" + _account_identity(env) + ".sock")


def _send_request(env: str, payload: dict, *, timeout_sec: float) -> dict | None:
    path = socket_path(env)
    if not path.exists():
        return None
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(max(0.01, min(float(timeout_sec), 1.0)))
        conn.connect(str(path))
        payload = {**payload, "identity": _account_identity(env), "env": str(env)}
        data = json.dumps(payload, separators=(",", ":")).encode()
        if len(data) > _MAX_MESSAGE_BYTES:
            return None
        conn.sendall(data + b"\n")
        chunks = bytearray()
        while len(chunks) < _MAX_REPLY_BYTES:
            part = conn.recv(min(1024, _MAX_REPLY_BYTES - len(chunks)))
            if not part:
                break
            chunks.extend(part)
            if b"\n" in part:
                break
        if b"\n" not in chunks:
            return None
        row = json.loads(bytes(chunks).split(b"\n", 1)[0])
        return row if isinstance(row, dict) else None


def get_quote(symbol: str, exchange: str, *, env: str, max_age_sec: float = 15,
              timeout_sec: float = 0.18) -> dict | None:
    """Return a fresh collector quote, never an order or an old generation cache."""
    if not enabled():
        return None
    try:
        result = _send_request(env, {
            "cmd": "QUOTE", "symbol": str(symbol).upper(), "exchange": str(exchange).upper(),
            "max_age_sec": min(30.0, max(0.05, float(max_age_sec))),
        }, timeout_sec=timeout_sec)
        if not result or result.get("status") != "OK":
            return None
        quote = result.get("quote")
        if not isinstance(quote, dict):
            return None
        received = float(quote.get("received_at") or 0)
        if received <= 0 or time.time() - received > max_age_sec or received > time.time() + 1:
            return None
        if not result.get("generation") or float(quote.get("last") or 0) <= 0:
            return None
        return {**quote, "collector_generation": result["generation"],
                "source": "KIS_WEBSOCKET_COLLECTOR"}
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        logger.debug("[US_WS_COLLECTOR][FALLBACK] symbol=%s err=%s", symbol, exc)
        return None


def subscribe_many(pairs: list[tuple[str, str]], *, env: str,
                   timeout_sec: float = 0.5) -> bool:
    """Best-effort READ-ONLY subscription warmup, never a quote/order truth.

    Does not return prices. A failed, late or stale collector is handled by
    the unchanged fresh-quote and REST fallback in USDataProvider.
    """
    if not enabled() or not pairs:
        return False
    normalized = []
    seen = set()
    for symbol, exchange in pairs:
        pair = (str(symbol).strip().upper(), str(exchange).strip().upper())
        if not pair[0] or not pair[1] or len(pair[0]) > 16 or pair in seen:
            continue
        seen.add(pair)
        normalized.append({"symbol": pair[0], "exchange": pair[1]})
        if len(normalized) >= 16:
            break
    if not normalized:
        return False
    try:
        result = _send_request(
            env, {"cmd": "SUBSCRIBE_MANY", "pairs": normalized},
            timeout_sec=min(0.8, max(0.05, timeout_sec)),
        )
        ok = bool(
            result and result.get("status") == "OK"
            and int(result.get("subscribed") or 0) == len(normalized)
            and result.get("generation")
        )
        if not ok:
            logger.info(
                "[US_WS_COLLECTOR][SUBSCRIBE_MANY_MISS] requested=%d response=%s",
                len(normalized), str((result or {}).get("status") or "NO_REPLY"),
            )
        return ok
    except (OSError, ValueError, RuntimeError, TypeError) as exc:
        logger.info("[US_WS_COLLECTOR][SUBSCRIBE_MANY_FALLBACK] reason=%s", type(exc).__name__)
        return False


def _reply(request: dict, *, env: str, generation: str, service) -> dict:
    if request.get("identity") != _account_identity(env) or request.get("env") != env:
        return {"status": "DENIED"}
    if str(request.get("cmd") or "") == "HEALTH":
        return {"status": "OK", "generation": generation, "collector": "READ_ONLY"}
    if request.get("cmd") == "SUBSCRIBE_MANY":
        pairs = request.get("pairs")
        if not isinstance(pairs, list) or not 1 <= len(pairs) <= 16:
            return {"status": "BAD_REQUEST"}
        valid = []
        for item in pairs:
            if not isinstance(item, dict):
                return {"status": "BAD_REQUEST"}
            symbol = str(item.get("symbol") or "").strip().upper()
            exchange = str(item.get("exchange") or "").strip().upper()
            if not symbol or not exchange or len(symbol) > 16 or len(exchange) > 12:
                return {"status": "BAD_REQUEST"}
            valid.append((symbol, exchange))
        for symbol, exchange in valid:
            service.subscribe_us(symbol, exchange)
        return {"status": "OK", "generation": generation,
                "subscribed": len(valid), "collector": "READ_ONLY"}
    if request.get("cmd") != "QUOTE":
        return {"status": "BAD_REQUEST"}
    symbol = str(request.get("symbol") or "").strip().upper()
    exchange = str(request.get("exchange") or "").strip().upper()
    if not symbol or not exchange or len(symbol) > 32:
        return {"status": "BAD_REQUEST"}
    # Subscription happens only in this independent process, never in the tick.
    service.subscribe_us(symbol, exchange)
    max_age = max(0.05, min(float(request.get("max_age_sec") or 15), 30))
    quote = service.get_fresh_quote("US", symbol, max_age_sec=max_age)
    return {"status": "OK" if quote else "MISS", "generation": generation,
            "quote": quote, "reason": "" if quote else "NOT_RECEIVED_OR_STALE"}


def _handle(conn: socket.socket, *, env: str, generation: str, service) -> None:
    conn.settimeout(0.4)
    data = bytearray()
    while len(data) < _MAX_MESSAGE_BYTES:
        part = conn.recv(min(512, _MAX_MESSAGE_BYTES - len(data)))
        if not part:
            return
        data.extend(part)
        if b"\n" in part:
            break
    if b"\n" not in data:
        return
    try:
        request = json.loads(bytes(data).split(b"\n", 1)[0])
        reply = _reply(request, env=env, generation=generation, service=service)
    except (ValueError, TypeError, RuntimeError) as exc:
        reply = {"status": "ERROR", "reason": type(exc).__name__}
    message = json.dumps(reply, separators=(",", ":"), default=str).encode()
    if len(message) <= _MAX_REPLY_BYTES - 1:
        conn.sendall(message + b"\n")


def serve(*, env: str, parent_pid: int, service=None, max_seconds: float | None = None) -> int:
    import time as _time
    if service is None:
        from trader.marketdata.kis_ws_price import get_kis_ws_price_service
        service = get_kis_ws_price_service()
    path = socket_path(env)
    lock_path = path.with_suffix(".lock")
    start = _time.monotonic()
    generation = uuid.uuid4().hex
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0  # Existing account-scoped collector retains ownership.
        # Only the lock owner may replace a stale socket.
        path.unlink(missing_ok=True)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(path))
            os.chmod(path, 0o600)
            server.listen(16)
            server.settimeout(0.4)
            logger.info("[US_WS_COLLECTOR][START] generation=%s parent_pid=%d", generation, parent_pid)
            try:
                while True:
                    if parent_pid > 1 and os.getppid() != parent_pid:
                        return 0
                    if max_seconds is not None and _time.monotonic() - start >= max_seconds:
                        return 0
                    try:
                        conn, _ = server.accept()
                    except socket.timeout:
                        continue
                    with conn:
                        try:
                            _handle(conn, env=env, generation=generation, service=service)
                        except (OSError, ValueError) as exc:
                            logger.debug("[US_WS_COLLECTOR][CLIENT_ERROR] err=%s", exc)
            finally:
                path.unlink(missing_ok=True)


def ensure_started(*, env: str, offline: bool = False) -> subprocess.Popen | None:
    """Start collector from the session parent; do not fork its WS threads."""
    global _LAST_START_REQUEST
    if offline or not enabled():
        return None
    try:
        health = _send_request(env, {"cmd": "HEALTH"}, timeout_sec=0.15)
        if health and health.get("status") == "OK":
            return None
        if _LAST_START_REQUEST is not None:
            _previous_exit = _LAST_START_REQUEST.poll()
            if _previous_exit is None:
                logger.warning(
                    "[US_WS_COLLECTOR][HEALTH_UNAVAILABLE_CHILD_ALIVE] pid=%d "
                    "action=REST_FALLBACK_NO_DUPLICATE_SPAWN",
                    _LAST_START_REQUEST.pid,
                )
                return None
            logger.warning(
                "[US_WS_COLLECTOR][EARLY_EXIT] pid=%d exit_code=%s "
                "action=RESTART_WITH_NEW_GENERATION",
                _LAST_START_REQUEST.pid, _previous_exit,
            )
            _LAST_START_REQUEST = None
        process = subprocess.Popen(
            [sys.executable, "-m", "trader.marketdata.us_ws_collector",
             "--env", env, "--parent-pid", str(os.getpid())],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, close_fds=True, start_new_session=True,
        )
        _LAST_START_REQUEST = process
        logger.info("[US_WS_COLLECTOR][START_REQUEST] pid=%d", process.pid)
        return process
    except (OSError, RuntimeError) as exc:
        logger.warning("[US_WS_COLLECTOR][START_FAIL] err=%s action=REST_FALLBACK", exc)
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="practice", choices=["practice", "real"])
    parser.add_argument("--parent-pid", type=int, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    return serve(env=args.env, parent_pid=args.parent_pid)


if __name__ == "__main__":
    raise SystemExit(main())
