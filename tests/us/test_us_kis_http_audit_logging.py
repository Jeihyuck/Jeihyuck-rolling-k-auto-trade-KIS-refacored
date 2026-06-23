from __future__ import annotations

import logging


def test_record_kis_http_call_is_info_not_error(caplog, tmp_path, monkeypatch):
    from trader.us.execution.kis_us_client import record_kis_http_call

    audit = tmp_path / "kis_http_audit.log"
    monkeypatch.setenv("KIS_HTTP_AUDIT_FILE", str(audit))

    with caplog.at_level(logging.INFO):
        record_kis_http_call("POST", "/oauth2/tokenP")

    marker = "[KIS_HTTP_CALL] method=POST path=/oauth2/tokenP"
    assert marker in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert marker in audit.read_text(encoding="utf-8")
