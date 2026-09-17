"""Process-safe KIS practice REST rate governor.

KIS practice REST traffic is account-wide, while KisUSClient historically
throttled per endpoint and per client instance. Multiple USDataProvider /
KisUSClient instances can therefore burst different endpoints at the same
practice account. This module installs one narrow requests hook that only
serializes requests to the KIS VTS host. It is process-safe through a file
lock so separate US worker processes share the same request-start clock.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_KIS_VTS_HOST = "openapivts.koreainvestment.com"
_INSTALLED = False


def _enabled() -> bool:
    if os.getenv("PYTEST_CURRENT_TEST") and str(os.getenv("US_KIS_GLOBAL_RATE_GOVERNOR_TEST", "0")) != "1":
        return False
    return str(os.getenv("US_KIS_GLOBAL_RATE_GOVERNOR", "1")).strip().lower() not in {
        "0", "false", "no", "off"
    }


def _min_interval_sec() -> float:
    try:
        return max(1.0, float(os.getenv("US_KIS_PRACTICE_GLOBAL_MIN_INTERVAL_SEC", "1.05") or 1.05))
    except (TypeError, ValueError):
        return 1.05


def _state_paths() -> tuple[Path, Path]:
    root = Path(__file__).resolve().parents[2]
    private = root / "runtime" / "private"
    private.mkdir(parents=True, exist_ok=True)
    return private / "kis_us_practice_http.lock", private / "kis_us_practice_http.last"


def _is_kis_vts_url(url: str) -> bool:
    try:
        return (urlparse(str(url)).hostname or "").lower() == _KIS_VTS_HOST
    except Exception:
        return False


def _wait_for_global_slot(method: str, url: str) -> float:
    """Serialize request *starts* across processes; return waited seconds."""
    if not _enabled() or not _is_kis_vts_url(url):
        return 0.0
    try:
        import fcntl
    except ImportError:  # pragma: no cover - Windows does not run the US WSL trader.
        return 0.0

    lock_path, state_path = _state_paths()
    interval = _min_interval_sec()
    waited = 0.0
    with lock_path.open("a+", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            now = time.time()
            try:
                last = float(state_path.read_text(encoding="utf-8").strip() or 0.0)
            except Exception:
                last = 0.0
            elapsed = now - last
            if last > 0 and 0.0 <= elapsed < interval:
                waited = interval - elapsed
                time.sleep(waited)
            dispatch_at = time.time()
            tmp = state_path.with_suffix(".tmp")
            tmp.write_text(f"{dispatch_at:.9f}\n", encoding="utf-8")
            tmp.replace(state_path)
        finally:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    if waited >= 0.001:
        logger.info(
            "[US_KIS][GLOBAL_RATE_LIMIT] method=%s endpoint=%s wait_ms=%d interval_sec=%.3f",
            str(method).upper(), urlparse(str(url)).path, int(waited * 1000), interval,
        )
    return waited


def install_kis_http_governor() -> bool:
    """Install the VTS-only requests hook once per Python process."""
    global _INSTALLED
    if _INSTALLED or not _enabled():
        return _INSTALLED
    try:
        import requests
    except Exception:
        return False

    session_cls = requests.sessions.Session
    current = session_cls.request
    if getattr(current, "_nullim_kis_global_governor", False):
        _INSTALLED = True
        return True

    original = current

    def governed_request(self, method, url, *args, **kwargs):
        _wait_for_global_slot(str(method), str(url))
        return original(self, method, url, *args, **kwargs)

    governed_request._nullim_kis_global_governor = True  # type: ignore[attr-defined]
    governed_request._nullim_original_request = original  # type: ignore[attr-defined]
    session_cls.request = governed_request
    _INSTALLED = True
    logger.info(
        "[US_KIS][GLOBAL_RATE_GOVERNOR_INSTALLED] host=%s min_interval_sec=%.3f",
        _KIS_VTS_HOST, _min_interval_sec(),
    )
    return True
