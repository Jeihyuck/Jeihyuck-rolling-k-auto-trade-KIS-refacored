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


def _install_broker_truth_repo_engine_binding() -> None:
    """Ensure the KR post-tick broker-truth guard can reach the PB1 DB engine.

    PB1Engine owns repository objects, while the SQLAlchemy engine normally lives
    on those repositories rather than on PB1Engine itself.  The hardening guard
    intentionally accepts an engine-like PB1 object, so bind the repository
    engine onto the live instance immediately before the post-tick guard runs.
    This is KR-only and does not alter strategy or order decisions.
    """
    import trader.kr.broker_truth_hardening as broker_truth

    if getattr(broker_truth, "_repo_engine_binding_installed", False):
        return
    original = broker_truth._post_pb1_tick_reconcile

    def _with_repo_engine(engine_obj):
        if getattr(engine_obj, "engine", None) is None:
            for repo_name in ("orders_repo", "positions_repo", "fills_repo", "ledger_repo"):
                repo = getattr(engine_obj, repo_name, None)
                repo_engine = getattr(repo, "engine", None)
                if repo_engine is not None:
                    try:
                        setattr(engine_obj, "engine", repo_engine)
                    except Exception:
                        pass
                    break
        return original(engine_obj)

    broker_truth._post_pb1_tick_reconcile = _with_repo_engine
    broker_truth._repo_engine_binding_installed = True


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
    from trader.kr.broker_truth_review_fixes import install_review_feedback_guards

    _install_broker_truth_repo_engine_binding()
    install_kr_broker_truth_runtime_guards()
    install_review_feedback_guards()
