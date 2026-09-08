from __future__ import annotations

from urllib.parse import urlparse


def endpoint_path(endpoint: str) -> str:
    parsed = urlparse(str(endpoint or ""))
    return (parsed.path or str(endpoint or "")).lower()


def endpoint_name(endpoint: str) -> str:
    path = endpoint_path(endpoint).rstrip("/")
    if not path:
        return "unknown"
    return path.split("/")[-1] or "unknown"


def is_order_endpoint(endpoint: str) -> bool:
    path = endpoint_path(endpoint)
    return any(
        token in path
        for token in (
            "/trading/order-cash",
            "/trading/order-rvsecncl",
            "/trading/order-resv",
            "/order-cash",
            "/order-rvsecncl",
            "/order/",
        )
    )


def is_data_endpoint(endpoint: str) -> bool:
    path = endpoint_path(endpoint)
    if "/oauth2/token" in path:
        return True
    return any(
        token in path
        for token in (
            "/quotations/",
            "inquire-price",
            "inquire-daily-itemchartprice",
            "inquire-asking-price-exp-ccn",
            "inquire-investor",
            "program-trade",
            "market-cap",
            "search-stock-info",
            "inquire-daily-ccld",
            "inquire-balance",
            "inquire-psbl-order",
        )
    )


def resolve_kis_http_caller_route(default: str = "live", *, env: dict[str, str] | None = None) -> str:
    source = env or {}
    raw = (source.get("KIS_HTTP_CALLER_ROUTE") or "").strip().lower()
    if raw:
        return raw
    mode = (source.get("MODE") or "").strip().lower()
    strategy_mode = (source.get("STRATEGY_MODE") or "").strip().upper()
    if mode == "prep":
        return "prep"
    if strategy_mode == "DIAG" and (
        source.get("PB1_DIAG_FULL_EXEC", "0").strip() == "1"
        or source.get("FORCE_RUN", "0").strip() == "1"
        or source.get("WATCHLIST_MODE", "0").strip() == "1"
    ):
        return "manual_test"
    return default


def kis_http_allowed(
    endpoint: str,
    strategy_mode: str,
    allow_data_http_in_diag: bool,
    caller_route: str | None = None,
    *,
    env: dict[str, str] | None = None,
) -> bool:
    normalized_mode = str(strategy_mode or "").strip().upper()
    route = (caller_route or resolve_kis_http_caller_route(default="live", env=env)).strip().lower() or "live"
    if normalized_mode == "DIAG":
        if is_order_endpoint(endpoint):
            return False
        if is_data_endpoint(endpoint):
            if route == "smoke":
                return False
            return bool(allow_data_http_in_diag)
    return True


def is_trading_endpoint(url: str) -> bool:
    return is_order_endpoint(url)


def kis_explicit_offline_mode(*, env: dict[str, str] | None = None) -> bool:
    source = env or {}
    if source.get("KIS_EXPLICIT_OFFLINE", "0").strip() == "1":
        return True
    if (source.get("DIAG_KIS_CALLS_ENABLED") or "").strip() == "0":
        return True
    return False


def kis_data_http_allowed_in_diag(*, env: dict[str, str] | None = None) -> bool:
    source = env or {}
    return source.get("ALLOW_KIS_DATA_HTTP_IN_DIAG", "0").strip() == "1"
