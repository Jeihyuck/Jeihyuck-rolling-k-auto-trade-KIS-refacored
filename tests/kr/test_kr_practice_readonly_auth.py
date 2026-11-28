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


def test_transport_failure_is_reported_before_wrapper_retry_and_never_reissues():
    import requests
    for exc, state in [
        (requests.exceptions.SSLError("private-url"), "TLS_ERROR"),
        (requests.exceptions.ConnectTimeout("private-url"), "CONNECT_TIMEOUT"),
        (requests.exceptions.ReadTimeout("private-url"), "READ_TIMEOUT"),
        (requests.exceptions.ProxyError("private-url"), "PROXY_ERROR"),
        (requests.exceptions.ConnectionError("private-url"), "CONNECTION_ERROR"),
    ]:
        def send(*args, **kwargs):
            raise exc
        guard = PracticeReadonlyGuard(send)
        with pytest.raises(RuntimeError, match="TOKEN_TRANSPORT_OUTCOME_UNKNOWN_NO_RETRY"):
            guard.send(None, _request("POST", "/oauth2/tokenP"))
        assert guard.transport_failures == [{"scope":"OAUTH","state":state}]
        assert guard.token_responses == []
        assert guard.counts == {"oauth_post":1,"get":0,"blocked":0}
        assert guard.retry_wait_seconds() is None
        with pytest.raises(RuntimeError, match="TOKEN_RETRY_NOT_AUTHORIZED"):
            guard.send(None, _request("POST", "/oauth2/tokenP"))
        assert guard.blocked_reasons == ["TOKEN_RETRY_NOT_AUTHORIZED"]
        assert "private-url" not in str(guard.transport_failures)


def test_redirect_status_is_captured_but_redirect_is_never_followed():
    response = Response(302, {})
    response.is_redirect = True
    seen = []
    def send(session, request, **kwargs):
        seen.append(kwargs)
        return response
    guard = PracticeReadonlyGuard(send)
    with pytest.raises(RuntimeError, match="UNAPPROVED_HTTP_REDIRECT"):
        guard.send(None, _request("POST", "/oauth2/tokenP"))
    assert seen == [{"allow_redirects": False}]
    assert guard.token_responses[0]["http_status"] == 302
    assert guard.blocked_reasons == ["UNAPPROVED_HTTP_REDIRECT"]
    assert guard.retry_wait_seconds() is None


def test_requests_session_integration_captures_transport_failure(monkeypatch):
    import requests
    session = requests.Session()
    calls = []
    def fail(request, **kwargs):
        calls.append(request.method)
        raise requests.exceptions.ReadTimeout("sensitive")
    monkeypatch.setattr(session.get_adapter("https://"), "send", fail)
    guard = PracticeReadonlyGuard(requests.sessions.Session.send)
    monkeypatch.setattr(requests.sessions.Session, "send",
                        lambda self, request, **kwargs: guard.send(self, request, **kwargs))
    with pytest.raises(RuntimeError, match="TOKEN_TRANSPORT_OUTCOME_UNKNOWN_NO_RETRY"):
        session.post("https://openapivts.koreainvestment.com:29443/oauth2/tokenP",
                     json={"appsecret":"fake-test-secret"}, timeout=1)
    assert calls == ["POST"]
    assert guard.transport_failures == [{"scope":"OAUTH","state":"READ_TIMEOUT"}]


def test_missing_ack_native_candidates_never_authorize_recovery():
    from datetime import date
    from scripts.kr_historical_buy_native_evidence import missing_order_id_evidence
    day = date(2026,11,27)
    row = {"pdno":"012750","odno":"0000000123","ord_dt":"20261127",
           "sll_buy_dvsn_cd":"02","ord_qty":"19","ord_unpr":"101500",
           "rmn_qty":"0"}
    payload = {"rt_cd":"0","_sep29_complete":True,"output1":[row]}
    def evidence(data):
        return missing_order_id_evidence(data,day=day,code="012750",
                                         qty=19,limit_price=101500)
    result = evidence(payload)
    assert result["state"] == "MISSING_ORDER_ID_MATCH_REVIEW_REQUIRED"
    assert result["candidate_order_numbers"] == ["0000000123"]
    assert result["recovery_authorized"] is False
    result = evidence({**payload,"output1":[row,{**row,"odno":"0000000124"}]})
    assert result["candidate_count"] == 2
    assert result["recovery_authorized"] is False
    result = evidence({**payload,"output1":[]})
    assert result["state"] == "MISSING_ORDER_ID_NO_MATCH_NOT_ABSENCE_PROOF"
    assert result["recovery_authorized"] is False
    for data in ({**payload,"_sep29_complete":False},
                 {**payload,"output1":[row,row]},
                 {**payload,"output1":[{**row,"ord_qty":"bad"}]}):
        result = evidence(data)
        assert result["state"] == "MISSING_ORDER_ID_NATIVE_HISTORY_UNVERIFIED"
        assert result["recovery_authorized"] is False


def test_historical_native_day_budget_prioritizes_active_uncertain_claims():
    from datetime import date
    from scripts.kr_historical_buy_native_evidence import _evidence_target_priority

    def target(day, claim_id, exact_cycle):
        return {"claim": {"trade_date": day, "action_key": claim_id},
                "order": {"order_id": str(day), "position_cycle_id": "cycle"},
                "positions": [{"position_cycle_id": "cycle"}] if exact_cycle else []}

    audits = [target(date(2026, 9, d), None, False) for d in (1, 2, 3, 4)]
    unresolved = [
        target(date(2026, 10, 27), "old-claim", True),
        target(date(2026, 11, 27), "new-claim", False),
    ]
    ranked = sorted(audits + unresolved, key=_evidence_target_priority)
    assert {t["claim"]["action_key"] for t in ranked[:2]} == {"old-claim", "new-claim"}
    assert ranked[2]["claim"]["action_key"] is None


@pytest.mark.parametrize("cancelled,rejected", [(174, 0), (0, 174)])
def test_exact_original_zero_fill_terminal_evidence_does_not_authorize_retry(cancelled, rejected):
    from datetime import date
    from scripts.kr_historical_buy_native_evidence import exact_buy_evidence
    day = date(2026, 10, 27)
    row = {"pdno": "293490", "odno": "0000011280", "ord_dt": "20261027",
           "sll_buy_dvsn_cd": "02", "ord_qty": "174", "tot_ccld_qty": "0",
           "tot_ccld_amt": "0", "avg_prvs": "0", "rmn_qty": "0",
           "cncl_cfrm_qty": str(cancelled), "rjct_qty": str(rejected)}
    payload = {"rt_cd": "0", "_sep29_complete": True, "output1": [row]}
    def proof(data):
        return exact_buy_evidence(data, day=day, code="293490", odno="0000011280", qty=174)
    result = proof(payload)
    assert result["state"] == "ORIGINAL_ZERO_FILL_TERMINAL_PROVEN"
    assert result["recovery_authorized"] is False
    for changes in ({"rmn_qty": "174"}, {"sll_buy_dvsn_cd": "01"},
                    {"tot_ccld_amt": "100"}, {"cncl_cfrm_qty": "175"},
                    {"rjct_qty": "bad"}, {"ord_qty": "173"}):
        assert proof({**payload, "output1": [{**row, **changes}]})["state"] != result["state"]
    assert proof({**payload, "_stub": True})["state"] == "NATIVE_HISTORY_UNVERIFIED"
