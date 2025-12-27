from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from trader.config import KST


def _collect_symbols(selected_by_market: Dict[str, Any] | None) -> set[str]:
    symbols: set[str] = set()
    if not isinstance(selected_by_market, dict):
        return symbols
    for rows in selected_by_market.values():
        if not rows:
            continue
        for row in rows:
            code = str(row.get("code") or row.get("pdno") or row.get("stock_code") or "").zfill(6)
            if code and code != "000000":
                symbols.add(code)
    return symbols


def build_market_data(
    selected_by_market: Dict[str, Any] | None = None,
    kis_client: Any | None = None,
    now_ts: str | None = None,
) -> Dict[str, Any]:
    """Build minimal market data snapshot for strategies."""

    now_ts = now_ts or datetime.now(KST).isoformat()
    prices: dict[str, Dict[str, Any]] = {}
    symbols = _collect_symbols(selected_by_market)
    for symbol in symbols:
        prices[symbol] = {}

    snapshot: Dict[str, Any] = {
        "as_of": now_ts,
        "prices": prices,
    }

    # Optionally hydrate last prices if client provided; safe no-op for now.
    if kis_client and hasattr(kis_client, "get_price_quote"):
        for symbol in symbols:
            try:
                quote = kis_client.get_price_quote(symbol)
                if isinstance(quote, dict):
                    last_price = quote.get("stck_prpr") or quote.get("prpr")
                    if last_price:
                        prices[symbol]["last_price"] = float(last_price)
            except Exception:
                # keep minimal snapshot even if fetch fails
                continue

    return snapshot
