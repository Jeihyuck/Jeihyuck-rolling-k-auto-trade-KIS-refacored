from types import SimpleNamespace

import pytest

from scripts.kr_practice_readonly_auth import PracticeReadonlyGuard, token_diagnostic


class Response:
    is_redirect = False

    def __init__(self, status, body):
        self.status_code = status
        self.body = body

    def json(self):
        return self.body


def _request(method, path, base="https://openapivts.koreainvestment.com:29443"):
    return SimpleNamespace(method=method, url=base+path)


def test_generic_auth_error_not_retryable_and_diagnostics_exclude_secrets():
    data = {"error_code":"EGW00123","error_description":"private-secret", "appsecret":"private-secret"}
    info = token_diagnostic(Response(403, data))
    assert info == {"http_status":403,"broker_error_code":"EGW00123",
                    "state":"TOKEN_NOT_ISSUED","retryable":False}
    assert "private-secret" not in str(info)
    assert token_diagnostic(Response(403, {"error_code":"private-secret"}))["broker_error_code"] is None
    assert token_diagnostic(Response(200, {"access_token":"private-token"}))["state"] == "TOKEN_ISSUED"


def test_explicit_token_throttle_allows_one_retry_only_after_65_seconds():
    now = [100.]
    responses = iter([Response(403, {"error_code":"EGW00133"}), Response(200, {"access_token":"secret"})])
    guard = PracticeReadonlyGuard(lambda *a, **kw: next(responses), clock=lambda:now[0])
    request = _request("POST", "/oauth2/tokenP")
    guard.send(None, request)
    assert guard.retry_wait_seconds() == 65.
    now[0] = 165.
    guard.send(None, request)
    assert guard.counts["oauth_post"] == 2
    assert guard.retry_wait_seconds() is None
    with pytest.raises(RuntimeError, match="TOKEN_RETRY_NOT_AUTHORIZED"):
        guard.send(None, request)


def test_early_retry_and_unknown_auth_are_blocked():
    for body, now in [({"error_code":"EGW00133"}, 64.), ({"error_code":"EGW00123"}, 70.)]:
        clock = [0.]
        guard = PracticeReadonlyGuard(lambda *a, **kw: Response(403, body), clock=lambda:clock[0])
        req = _request("POST","/oauth2/tokenP")
        guard.send(None, req)
        clock[0] = now
        with pytest.raises(RuntimeError, match="TOKEN_RETRY_NOT_AUTHORIZED"):
            guard.send(None, req)
        assert guard.counts["oauth_post"] == 1


def test_only_readonly_practice_endpoints_allowed():
    sent = []
    def send(session, request, **kwargs):
        sent.append(kwargs)
        return Response(200, {})
    guard = PracticeReadonlyGuard(send)
    guard.send(None, _request("GET", "/uapi/domestic-stock/v1/trading/inquire-balance"))
    assert sent == [{"allow_redirects":False}]
    for method,path,base in [
        ("POST","/uapi/domestic-stock/v1/trading/order-cash","https://openapivts.koreainvestment.com:29443"),
        ("POST","/uapi/hashkey","https://openapivts.koreainvestment.com:29443"),
        ("GET","/uapi/domestic-stock/v1/trading/inquire-balance","https://openapi.koreainvestment.com:9443"),
        ("POST","/oauth2/tokenP?unexpected=1","https://openapivts.koreainvestment.com:29443"),
    ]:
        with pytest.raises(RuntimeError):
            guard.send(None, _request(method,path,base))
    assert len(sent) == 1
