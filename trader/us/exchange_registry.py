"""Prepare the runtime US symbol/exchange registry before Prep price sync."""
from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from trader.us.symbols import (
    is_known_symbol,
    normalize_symbol,
    normalize_us_exchange,
    register_symbol,
    resolve_exchange,
)

logger = logging.getLogger(__name__)

EXCHANGE_KEYS = ("exchange", "exchange_code", "market", "market_code", "excd", "ovrs_excg_cd")


def _item_exchange(item: Mapping[str, Any]) -> tuple[str | None, str | None]:
    for key in EXCHANGE_KEYS:
        value = item.get(key)
        if value not in (None, ""):
            return str(value), key
    meta = item.get("meta")
    if isinstance(meta, Mapping):
        for key in EXCHANGE_KEYS:
            value = meta.get(key)
            if value not in (None, ""):
                return str(value), f"meta.{key}"
    return None, None


def prepare_exchange_registry(
    *,
    dynamic_universe_result: Mapping[str, Any],
    benchmark_symbols: Iterable[str],
    open_positions: Iterable[Mapping[str, Any] | str] = (),
    metadata_lookup: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Register exchanges from trusted inputs, returning structured failures.

    Resolution order is existing registry, source item metadata, optional project
    metadata lookup, and explicit benchmark/static mappings.  There is no guessed
    NASDAQ fallback.
    """
    targets: dict[str, tuple[str, Mapping[str, Any] | None]] = {}
    for raw in dynamic_universe_result.get("symbols", []) or []:
        if isinstance(raw, Mapping):
            symbol = normalize_symbol(str(raw.get("symbol") or ""))
            targets[symbol] = ("dynamic_universe", raw)
    for symbol in benchmark_symbols:
        targets[normalize_symbol(str(symbol))] = ("benchmark", None)
    for raw in open_positions:
        item = raw if isinstance(raw, Mapping) else None
        symbol = normalize_symbol(str(item.get("symbol") if item is not None else raw))
        targets[symbol] = ("open_position", item)

    registered: list[str] = []
    already_known: list[str] = []
    failures: list[dict[str, str]] = []
    for symbol, (source, item) in sorted(targets.items()):
        if is_known_symbol(symbol):
            already_known.append(symbol)
            continue
        raw_exchange, exchange_source = _item_exchange(item or {})
        if raw_exchange is None and metadata_lookup is not None:
            try:
                metadata = metadata_lookup(symbol)
                if isinstance(metadata, Mapping):
                    raw_exchange, exchange_source = _item_exchange(metadata)
                    exchange_source = f"metadata_lookup.{exchange_source}" if exchange_source else None
                elif metadata:
                    raw_exchange, exchange_source = str(metadata), "metadata_lookup"
            except Exception as exc:
                failures.append({"symbol": symbol, "source": source, "reason": f"metadata_lookup_failed: {exc}"})
                logger.warning("[US_EXCHANGE_REGISTRY][FAIL] symbol=%s reason=%s source=%s", symbol, failures[-1]["reason"], source)
                continue
        if raw_exchange is None:
            failures.append({"symbol": symbol, "source": source, "reason": "exchange_metadata_missing"})
            logger.warning("[US_EXCHANGE_REGISTRY][FAIL] symbol=%s reason=exchange_metadata_missing source=%s", symbol, source)
            continue
        try:
            exchange = normalize_us_exchange(raw_exchange)
            register_symbol(symbol, exchange)
            # Resolve once so preparation guarantees the same path used by sync.
            resolve_exchange(symbol)
            registered.append(symbol)
            logger.info("[US_EXCHANGE_REGISTRY][REGISTER] symbol=%s exchange=%s source=%s:%s", symbol, exchange, source, exchange_source)
        except ValueError as exc:
            failures.append({"symbol": symbol, "source": source, "reason": str(exc)})
            logger.warning("[US_EXCHANGE_REGISTRY][FAIL] symbol=%s reason=%s source=%s", symbol, exc, source)

    result = {
        "target_count": len(targets),
        "registered_count": len(registered),
        "already_known_count": len(already_known),
        "failed_count": len(failures),
        "registered_symbols": registered,
        "already_known_symbols": already_known,
        "failed_symbols": [row["symbol"] for row in failures],
        "failures": failures,
    }
    logger.info("[US_EXCHANGE_REGISTRY][PREPARE] targets=%d registered=%d already_known=%d failed=%d", len(targets), len(registered), len(already_known), len(failures))
    return result
