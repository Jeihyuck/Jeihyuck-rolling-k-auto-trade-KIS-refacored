"""Trade API package.

Keep package import side-effect free.  In particular, importing ``trader.us``
must not import the legacy KR PB1 engine (and its KRX/pykrx providers).
"""

from __future__ import annotations

import os


def _kr_imports_disabled_for_us() -> bool:
    scope = str(os.getenv("MARKET_SCOPE") or os.getenv("TRADING_MARKET") or "").strip().lower()
    disabled = str(os.getenv("DISABLE_KR_IMPORTS_IN_US", "")).strip().lower() in {"1", "true", "yes", "on"}
    return scope == "us" and disabled


def install_legacy_pb1_runtime_guards() -> None:
    """Install legacy PB1 guards from a KR execution entry point only.

    This delayed import preserves the legacy KR runner behaviour while keeping
    lightweight US helpers such as ``python -m trader.us.session_lock`` fully
    independent from Korean credentials and providers.
    """
    if _kr_imports_disabled_for_us():
        return

    from trader.pb1_runtime_guards import _install_pb1_engine_runtime_guards

    _install_pb1_engine_runtime_guards()

    # Broker truth hardening is intentionally installed from the same KR-only
    # entry point.  It closes ACK->FILL->POSITION lifecycle gaps without
    # importing any Korean execution code into the US-only process path.
    from trader.kr.broker_truth_hardening import install_kr_broker_truth_runtime_guards

    install_kr_broker_truth_runtime_guards()
