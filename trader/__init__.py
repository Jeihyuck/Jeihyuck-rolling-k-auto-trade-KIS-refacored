"""Trade API package.

Keep package import side-effect free.  In particular, importing ``trader.us``
must not import the legacy KR PB1 engine (and its KRX/pykrx providers).
"""

from __future__ import annotations

import os

_LEGACY_GUARDS_INSTALLING = False


def _kr_imports_disabled_for_us() -> bool:
    scope = str(os.getenv("MARKET_SCOPE") or os.getenv("TRADING_MARKET") or "").strip().lower()
    disabled = str(os.getenv("DISABLE_KR_IMPORTS_IN_US", "")).strip().lower() in {"1", "true", "yes", "on"}
    return scope == "us" and disabled


def _install_broker_truth_repo_engine_binding() -> None:
    """Ensure the KR post-tick broker-truth guard can reach the PB1 DB engine."""
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
    """Install legacy PB1 guards from a KR execution entry point only."""
    global _LEGACY_GUARDS_INSTALLING
    if _kr_imports_disabled_for_us() or _LEGACY_GUARDS_INSTALLING:
        return

    _LEGACY_GUARDS_INSTALLING = True
    try:
        from trader.pb1_runtime_guards import _install_pb1_engine_runtime_guards

        _install_pb1_engine_runtime_guards()

        from trader.kr.broker_truth_hardening import install_kr_broker_truth_runtime_guards
        from trader.kr.broker_truth_review_fixes import install_review_feedback_guards
        from trader.kr.broker_truth_sell_fixes import install_sell_fill_guard
        from trader.kr.broker_truth_final_review_fixes import install_final_review_guards
        from trader.kr.broker_truth_observability_compat import install_buy_observability_compat
        from trader.kr.broker_truth_historical_buy_retry import install_historical_buy_retry
        from trader.kr.broker_truth_historical_retry_safety import install_historical_retry_safety
        from trader.kr.broker_truth_cross_date_unowned_retry import install_cross_date_unowned_retry
        from trader.kr.broker_truth_pending_fill_fence import install_pending_fill_application_fence
        from trader.kr.runtime_integrity_20260915 import install_kr_20260915_runtime_integrity
        from trader.kr.runtime_integrity_20260917 import install_kr_20260917_runtime_integrity
        from trader.kr.runtime_integrity_20260928 import install_kr_20260928_runtime_integrity
        from trader.kr.runtime_integrity_20260929 import install_kr_20260929_runtime_integrity
        from trader.kr.runtime_integrity_20260929_review import install_kr_20260929_review_guards
        from trader.kr.runtime_integrity_20260929_log_review import install_kr_20260929_log_review_guards
        from trader.kr.runtime_integrity_20260929_side_proof import install_kr_20260929_side_proof_guard
        from trader.kr.runtime_integrity_20260930 import install_kr_20260930_runtime_integrity

        _install_broker_truth_repo_engine_binding()
        install_kr_broker_truth_runtime_guards()
        install_review_feedback_guards()
        install_sell_fill_guard()
        install_final_review_guards()
        install_kr_20260915_runtime_integrity()
        install_kr_20260917_runtime_integrity()
        install_buy_observability_compat()
        install_historical_buy_retry()
        install_historical_retry_safety()
        install_cross_date_unowned_retry()
        install_pending_fill_application_fence()
        install_kr_20260928_runtime_integrity()
        install_kr_20260929_runtime_integrity()
        # Final review fence must be outermost around daily-ccld so a diagnostic,
        # partial first page, or isolated continuation page can never overwrite a
        # previously authoritative complete negative-proof snapshot.
        install_kr_20260929_review_guards()
        # Full-session log review found the actual late-BUY failure one boundary
        # earlier, at hashkey, plus a date-object repository exception that made
        # PR147 force broker refresh every tick.  Install these compatibility
        # fences last so they protect the complete KR execution path.
        install_kr_20260929_log_review_guards()
        # Cross-day contract recovery requires explicit broker-side BUY evidence.
        # Missing/unknown side values must fail closed rather than be inferred.
        install_kr_20260929_side_proof_guard()
        # Sep-30 liveness guard intentionally installs after PR148: the existing
        # 20s pre-submit/post-engine safety fences remain authoritative while
        # PB1 BUY pretrade shares the canonical price snapshot and AM/PM receive
        # the expanded tick container.
        install_kr_20260930_runtime_integrity()

        import trader.reconcile_kis as _kr_reconcile
        import trader.pb1_runner as _kr_pb1_runner
        _kr_pb1_runner.reconcile_kis = _kr_reconcile.reconcile_kis
    finally:
        _LEGACY_GUARDS_INSTALLING = False
