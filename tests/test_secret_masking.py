import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.kis_wrapper import sanitize_headers, sanitize_log_mapping


def test_sanitize_headers_masks_sensitive_kis_values():
    masked = sanitize_headers(
        {
            "authorization": "Bearer abcdef",
            "appkey": "app-key",
            "appsecret": "app-secret",
            "hashkey": "hash-value",
            "tr_id": "FHKST01010100",
        }
    )

    assert masked["authorization"] == "***"
    assert masked["appkey"] == "***"
    assert masked["appsecret"] == "***"
    assert masked["hashkey"] == "***"
    assert masked["tr_id"] == "FHKST01010100"


def test_sanitize_log_mapping_masks_token_and_secret_fields():
    masked = sanitize_log_mapping(
        {
            "access_token": "secret-token",
            "appkey": "key",
            "appsecret": "secret",
            "grant_type": "client_credentials",
        }
    )

    assert masked["access_token"] == "***"
    assert masked["appkey"] == "***"
    assert masked["appsecret"] == "***"
    assert masked["grant_type"] == "client_credentials"