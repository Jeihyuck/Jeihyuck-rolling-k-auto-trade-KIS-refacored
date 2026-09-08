import os

from trader.kis_http_policy import (
    endpoint_name,
    endpoint_path,
    is_data_endpoint,
    is_order_endpoint,
    is_trading_endpoint,
    kis_data_http_allowed_in_diag,
    kis_explicit_offline_mode,
    kis_http_allowed,
    resolve_kis_http_caller_route,
)
from trader.kis_wrapper import (
    _endpoint_name,
    _endpoint_path,
    _resolve_kis_http_caller_route,
    is_data_endpoint as wrapper_is_data_endpoint,
    is_order_endpoint as wrapper_is_order_endpoint,
    is_trading_endpoint as wrapper_is_trading_endpoint,
    kis_data_http_allowed_in_diag as wrapper_kis_data_http_allowed_in_diag,
    kis_explicit_offline_mode as wrapper_kis_explicit_offline_mode,
    kis_http_allowed as wrapper_kis_http_allowed,
)


def test_kis_http_policy_utils_match_wrappers(monkeypatch):
    monkeypatch.setenv("MODE", "prep")
    monkeypatch.setenv("KIS_HTTP_CALLER_ROUTE", "")
    endpoint = "/uapi/domestic-stock/v1/trading/order-cash"
    data_endpoint = "/uapi/domestic-stock/v1/quotations/inquire-investor"

    assert _endpoint_path(endpoint) == endpoint_path(endpoint)
    assert _endpoint_name(endpoint) == endpoint_name(endpoint)
    assert wrapper_is_order_endpoint(endpoint) == is_order_endpoint(endpoint)
    assert wrapper_is_data_endpoint(data_endpoint) == is_data_endpoint(data_endpoint)
    assert wrapper_is_trading_endpoint(endpoint) == is_trading_endpoint(endpoint)
    assert wrapper_kis_explicit_offline_mode() == kis_explicit_offline_mode()
    assert wrapper_kis_data_http_allowed_in_diag() == kis_data_http_allowed_in_diag()
    assert _resolve_kis_http_caller_route("live") == resolve_kis_http_caller_route("live", env=os.environ)
    assert wrapper_kis_http_allowed(data_endpoint, "DIAG", True, caller_route="smoke") == kis_http_allowed(data_endpoint, "DIAG", True, caller_route="smoke")
