"""KR owner-scoped read-only Unix IPC, using the existing KIS WS connection.

No order permission, no second broker connection, no locally synthesized
minute bars. The socket's lifetime is tied to the existing WS owner flock
and a single connected generation. The practice-only switch is explicit;
do not enable until the two-process and WSL acceptance tests are complete.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import stat
import time
import uuid

_KST = timezone(timedelta(hours=9))
_MAX_REQUEST = 1024
_MAX_REPLY = 8192


def enabled() -> bool:
    return (str(os.getenv("KIS_ENV") or "practice").lower() == "practice"
            and os.getenv("KR_WS_SHARED_QUOTE_ENABLED") == "1")


def _scope(service) -> str:
    key, secret = service._credentials()
    if not key or not secret:
        raise ValueError("KR_WS_SHARED_CREDENTIALS_MISSING")
    # Scope mirrors the connection-owner lock. Never persist the key itself.
    env = str(os.getenv("KIS_ENV") or "practice").lower()
    return hashlib.sha256((env + "\0" + key).encode()).hexdigest()[:32]


def socket_path(service) -> Path:
    root = Path("/tmp") / f"krws-{os.getuid()}"
    root.mkdir(mode=0o700, exist_ok=True)
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise ValueError("KR_WS_SHARED_UNSAFE_DIRECTORY")
    path = root / (_scope(service) + ".sock")
    if len(os.fsencode(str(path))) >= 108:
        raise ValueError("KR_WS_SHARED_PATH_TOO_LONG")
    return path


def _symbol(value) -> str:
    s = str(value or "").removeprefix("A")
    return s if len(s) == 6 and s.isascii() and s.isdigit() and s != "000000" else ""


def _positive(value):
    if isinstance(value, bool):
        return None
    try:
        v = float(value)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _validated_quote(quote, *, symbol: str, max_age_sec: float,
                     now: float | None = None) -> dict | None:
    """Preserve source proof; reject wrong day/symbol, stale or future events."""
    if not isinstance(quote, dict):
        return None
    now = time.time() if now is None else now
    if (quote.get("source") != "KIS_WEBSOCKET"
            or quote.get("source_identity") != "KIS_WS_PARSED_SYMBOL"
            or quote.get("market") != "KR"
            or _symbol(quote.get("symbol")) != symbol
            or _symbol(quote.get("requested_symbol")) != symbol
            or quote.get("stale")):
        return None
    receipt = _positive(quote.get("received_at"))
    if receipt is None or not 0 <= now - receipt <= max_age_sec:
        return None
    # The remote path requires native day/time as well as receipt time.
    # A short, price-only frame is not a verified same-session observation.
    day = str(quote.get("stck_bsop_date") or "")
    hour = str(quote.get("stck_cntg_hour") or "")
    local_now = datetime.fromtimestamp(now, _KST)
    if day != local_now.strftime("%Y%m%d") or len(hour) != 6:
        return None
    try:
        exchange_time = datetime.strptime(day + hour, "%Y%m%d%H%M%S").replace(tzinfo=_KST)
    except ValueError:
        return None
    native_age = now - exchange_time.timestamp()
    # Exchange timestamps have second resolution; permit at most one second
    # of rounding, not a different day or fabricated local timestamp.
    if not -1 <= native_age <= max_age_sec + 1:
        return None
    last, bid, ask = (_positive(quote.get(k)) for k in ("last", "bid", "ask"))
    if last is None or bid is None or ask is None or ask < bid:
        return None
    fields = [k for k in ("last", "bid", "ask", "acml_vol", "wghn_avrg_stck_prc")
              if _positive(quote.get(k)) is not None]
    return {**quote, "age_sec": now - receipt,
            "shared_native_age_sec": native_age, "shared_valid_fields": fields}


async def start_owner_server(service):
    """Called only after the existing owner flock and WS connect succeed."""
    path = socket_path(service)
    generation = uuid.uuid4().hex
    scope = _scope(service)
    clients = set()

    async def handle(reader, writer):
        clients.add(writer)
        try:
            data = await asyncio.wait_for(reader.readline(), timeout=0.15)
            if not data or len(data) > _MAX_REQUEST:
                return
            req = json.loads(data)
            if not isinstance(req, dict) or req.get("scope") != scope or req.get("cmd") != "QUOTE":
                return
            symbol = _symbol(req.get("symbol"))
            age = _positive(req.get("max_age_sec"))
            if not symbol or age is None or age > 30:
                return
            if not service._connected.is_set() or not service.network_allowed("KR"):
                return
            # An absent symbol requests the same existing WS subscription,
            # never a REST request or a broker order. Existing owner priorities
            # are not replaced by an IPC client.
            service.subscribe_kr(symbol)
            q = _validated_quote(service.get_fresh_quote("KR", symbol, max_age_sec=age),
                                 symbol=symbol, max_age_sec=age)
            if not service._connected.is_set():
                return
            result = {"status": "OK" if q else "MISS", "generation": generation,
                      "scope": scope, "symbol": symbol, "owner_pid": os.getpid(), "quote": q}
            data = json.dumps(result, separators=(",", ":"), allow_nan=False).encode() + b"\n"
            if len(data) <= _MAX_REPLY:
                writer.write(data)
                await asyncio.wait_for(writer.drain(), timeout=0.15)
        except (OSError, ValueError, TypeError, asyncio.TimeoutError):
            pass
        finally:
            clients.discard(writer)
            writer.close()

    # Only the actual broker connection owner calls this function. The owner
    # flock must still be held during bind and cleanup, including re-connects.
    path.unlink(missing_ok=True)
    server = await asyncio.start_unix_server(handle, path=str(path), limit=_MAX_REQUEST)
    try:
        os.chmod(path, 0o600)
    except OSError:
        server.close()
        await server.wait_closed()
        path.unlink(missing_ok=True)
        raise
    return server, path, clients, generation


async def close_owner_server(handle):
    if handle is None:
        return
    server, path, clients, _ = handle
    server.close()
    for writer in list(clients):
        writer.close()
    await server.wait_closed()
    path.unlink(missing_ok=True)


def read_owner_quote(service, symbol: str, *, max_age_sec: float,
                     timeout_sec: float = 0.12) -> tuple[bool, dict | None]:
    """A bounded, synchronous local read. False means normal local/REST fallback.

    A MISS with a verified live owner avoids a second WS connection attempt.
    No remote data is retained in this client after the request.
    """
    if not enabled() or not service.network_allowed("KR"):
        return False, None
    symbol = _symbol(symbol)
    age = _positive(max_age_sec)
    timeout = _positive(timeout_sec)
    if not symbol or age is None or timeout is None:
        return False, None
    try:
        path = socket_path(service)
        scope = _scope(service)
        info = path.lstat()
        if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) & 0o077):
            return False, None
        deadline = time.monotonic() + min(timeout, 0.2)

        def remaining():
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("KR_WS_SHARED_DEADLINE")
            return left

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(remaining())
            conn.connect(str(path))
            data = json.dumps({"cmd": "QUOTE", "scope": scope, "symbol": symbol,
                               "max_age_sec": min(age, 30.0)}).encode() + b"\n"
            conn.settimeout(remaining())
            conn.sendall(data)
            reply = bytearray()
            while len(reply) < _MAX_REPLY and b"\n" not in reply:
                conn.settimeout(remaining())
                part = conn.recv(min(1024, _MAX_REPLY - len(reply)))
                if not part:
                    break
                reply.extend(part)
            if b"\n" not in reply:
                return False, None
        result = json.loads(bytes(reply).split(b"\n", 1)[0])
        if (not isinstance(result, dict) or result.get("scope") != scope
                or result.get("symbol") != symbol or not result.get("generation")
                or result.get("status") not in {"OK", "MISS"}):
            return False, None
        q = _validated_quote(result.get("quote"), symbol=symbol, max_age_sec=age)
        if q and result["status"] == "OK":
            q["shared_generation"] = result["generation"]
            q["shared_owner_pid"] = result.get("owner_pid")
            q["shared_transport"] = "KR_OWNER_UNIX_SOCKET"
            return True, q
        return True, None
    except (OSError, ValueError, TypeError, OverflowError):
        return False, None
