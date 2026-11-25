"""Credential-safe diagnostics for the one-time KR practice readonly audit.

No production token changes, no order/cancel endpoint, no response bodies or
credentials in the report. A second token attempt is allowed only after a
positive native issuance-throttle response and >=65 seconds since it.
"""
from __future__ import annotations

import re
import time
from urllib.parse import urlsplit

_ALLOWED_GET = frozenset({
    "/uapi/domestic-stock/v1/trading/inquire-balance",
    "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
})
_OAUTH_CODES = frozenset({
    "invalid_client", "unauthorized_client", "invalid_request", "invalid_grant",
    "access_denied", "temporarily_unavailable", "server_error", "unsupported_grant_type",
})


def token_diagnostic(response):
    """Extract only bounded, known-shaped broker metadata, not arbitrary text."""
    status = int(response.status_code)
    try:
        data = response.json()
    except (ValueError, TypeError):
        data = None
    payload = data if isinstance(data, dict) else {}
    code = str(payload.get("error_code") or payload.get("msg_cd") or payload.get("error") or "").strip()
    safe_code = code if re.fullmatch(r"(?:EGW|OPS|APBK)[0-9]{5}", code) or code in _OAUTH_CODES else None
    success = 200 <= status < 300 and isinstance(payload.get("access_token"), str) and bool(payload["access_token"])
    # Retry ONLY on the explicit native throttle code, not generic auth
    # rejection, arbitrary response text, 5xx or network uncertainty.
    throttled = status in (401, 403, 429) and safe_code == "EGW00133" and not success
    return {"http_status": status, "broker_error_code": safe_code,
            "state": "TOKEN_ISSUED" if success else "TOKEN_ISSUANCE_THROTTLED" if throttled else "TOKEN_NOT_ISSUED",
            "retryable": throttled}


class PracticeReadonlyGuard:
    def __init__(self, send, *, clock=time.monotonic):
        self._send = send
        self._clock = clock
        self._last_token_start = None
        self.counts = {"oauth_post": 0, "get": 0, "blocked": 0}
        self.token_responses = []

    def _block(self, reason):
        self.counts["blocked"] += 1
        raise RuntimeError(reason)

    def send(self, session, request, **kwargs):
        try:
            url = urlsplit(request.url)
            valid = (url.scheme == "https" and url.hostname == "openapivts.koreainvestment.com"
                     and url.port == 29443 and not url.username and not url.password)
        except ValueError:
            valid = False
        if not valid:
            self._block("UNAPPROVED_HTTP_DESTINATION")
        method = request.method.upper()
        token = method == "POST" and url.path == "/oauth2/tokenP" and not url.query
        if method == "GET" and url.path in _ALLOWED_GET:
            self.counts["get"] += 1
        elif token:
            previous = self.counts["oauth_post"]
            now = self._clock()
            if previous and not (previous == 1 and self.token_responses
                    and self.token_responses[-1]["retryable"]
                    and self._last_token_start is not None
                    and now - self._last_token_start >= 65):
                self._block("TOKEN_RETRY_NOT_AUTHORIZED")
            self.counts["oauth_post"] += 1
            self._last_token_start = now
        else:
            self._block("UNAPPROVED_HTTP_METHOD_OR_PATH")
        kwargs["allow_redirects"] = False
        response = self._send(session, request, **kwargs)
        if response.is_redirect:
            self._block("UNAPPROVED_HTTP_REDIRECT")
        if token:
            self.token_responses.append(token_diagnostic(response))
        return response

    def retry_wait_seconds(self):
        if (self.counts["oauth_post"] != 1 or not self.token_responses
                or not self.token_responses[-1]["retryable"]
                or self.counts["blocked"] or self._last_token_start is None):
            return None
        return max(0., 65. - (self._clock() - self._last_token_start))
