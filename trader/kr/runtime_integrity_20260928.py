"""KR P0 runtime integrity guards for the 2026-09-28 broker-truth incident.

Implementation-only: this module does not alter PB1 entry/exit/sizing/TP policy
or KR Infinite ownership.  It hardens four runtime contracts:

* the session-start balance snapshot is one-shot inside the PB1 loop;
* configured balance-cache TTLs are actually enforced;
* unresolved broker activity never trusts a caller-supplied stale snapshot;
* the per-tick SIGALRM watchdog cannot be swallowed by ``except Exception``.

The KR package imports its legacy guards from two shapes: the sanctioned
``trade_session_runner`` calls them before importing ``pb1_runner``, while some
tests import ``pb1_runner`` directly and therefore call the installer from a
partially initialised module.  The installer below is deliberately re-entrant:
KIS/reconcile guards may be installed immediately, while PB1 function wrapping
is deferred until ``run_once`` and the watchdog function actually exist.
"""
from __future__ import annotations

import functools
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Any, Callable

logger = logging.getLogger(__name__)
_SAFE_INSTALLED = False
_PB1_INSTALLED = False
_INSTALLING = False


class _HardTickAbort(BaseException):
    """Internal SIGALRM escape hatch that broad ``except Exception`` cannot catch."""


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _balance_cache_ttl_sec() -> float:
    values = [
        _env_float("KIS_BALANCE_SNAPSHOT_TTL_SEC", 180.0),
        _env_float("KR_BALANCE_CACHE_MAX_AGE_SEC", 180.0),
    ]
    positive = [value for value in values if value > 0]
    return min(positive) if positive else 180.0


def _cache_age_sec(cache_at: Any) -> float | None:
    if cache_at is None or not hasattr(cache_at, "tzinfo"):
        return None
    try:
        now = datetime.now(cache_at.tzinfo) if cache_at.tzinfo is not None else datetime.now()
        return max(0.0, float((now - cache_at).total_seconds()))
    except Exception:
        return None


def _build_balance_cache_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(
        self,
        force: bool = False,
        *,
        return_source: bool = False,
        return_raw: bool = False,
    ):
        effective_force = bool(force)
        cache = getattr(self, "_balance_cache", None)
        cache_at = getattr(self, "_balance_cache_at", None)
        age_sec = _cache_age_sec(cache_at)
        ttl_sec = _balance_cache_ttl_sec()
        if not effective_force and cache is not None and age_sec is not None and age_sec > ttl_sec:
            logger.warning(
                "[KR_P0][BALANCE_CACHE_EXPIRED] age_sec=%.3f ttl_sec=%.3f action=force_refresh",
                age_sec,
                ttl_sec,
            )
            try:
                invalidate = getattr(self, "invalidate_balance_cache", None)
                if callable(invalidate):
                    invalidate(reason="kr_p0_balance_cache_ttl_expired")
                else:
                    setattr(self, "_balance_cache", None)
                    setattr(self, "_balance_cache_at", None)
            finally:
                effective_force = True
        return original(
            self,
            force=effective_force,
            return_source=return_source,
            return_raw=return_raw,
        )

    return guarded


def _default_unresolved_probe(engine: Any, env: str) -> bool:
    try:
        import trader.reconcile_kis as rk
        from trader.db.repos import OrdersRepo
        from trader.time_utils import now_kst

        return bool(
            rk._has_unresolved_broker_activity(
                engine=engine,
                orders_repo=OrdersRepo(engine),
                env=env,
                trade_date=now_kst().date(),
            )
        )
    except Exception as exc:
        # Broker-truth uncertainty must fail closed: a failed ledger lookup may
        # never make an old balance snapshot authoritative.
        logger.warning(
            "[KR_P0][UNRESOLVED_PROBE_FAIL] env=%s err_type=%s err=%s action=force_refresh",
            env,
            type(exc).__name__,
            exc,
        )
        return True


def _build_reconcile_guard(
    original: Callable[..., Any],
    *,
    unresolved_probe: Callable[[Any, str], bool] | None = None,
) -> Callable[..., Any]:
    probe = unresolved_probe or _default_unresolved_probe

    @functools.wraps(original)
    def guarded(*args: Any, **kwargs: Any):
        engine = kwargs.get("engine")
        kis = kwargs.get("kis")
        env = str(
            kwargs.get("env")
            or os.getenv("STRATEGY_ENV")
            or os.getenv("KIS_ENV")
            or "practice"
        ).lower()
        if engine is not None and kis is not None and probe(engine, env):
            if kwargs.get("balance_snapshot") is not None:
                logger.warning(
                    "[KR_P0][BROKER_TRUTH_REFRESH] env=%s reason=UNRESOLVED_BROKER_ACTIVITY "
                    "action=discard_injected_snapshot",
                    env,
                )
            try:
                invalidate = getattr(kis, "invalidate_balance_cache", None)
                if callable(invalidate):
                    invalidate(reason="kr_p0_unresolved_broker_activity")
            except Exception as exc:
                logger.warning(
                    "[KR_P0][BALANCE_INVALIDATE_FAIL] env=%s err_type=%s action=continue_force_path",
                    env,
                    type(exc).__name__,
                )
            # The canonical reconciler already calls get_balance_cached(force=True)
            # when no snapshot is supplied.  Do not duplicate broker I/O here.
            kwargs["balance_snapshot"] = None
        return original(*args, **kwargs)

    return guarded


def _build_run_once_precheck_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(*args: Any, **kwargs: Any):
        loop_mode = bool(kwargs.get("loop_mode", False))
        try:
            tick_index = int(os.getenv("PB1_LOOP_TICK_INDEX", "0") or 0)
        except Exception:
            tick_index = 0
        precheck_path = str(os.getenv("KR_BALANCE_PRECHECK_PATH") or "").strip()
        if loop_mode and tick_index > 1 and precheck_path:
            os.environ.pop("KR_BALANCE_PRECHECK_PATH", None)
            logger.warning(
                "[KR_P0][BALANCE_PRECHECK_RETIRED] tick=%s path=%s action=live_balance_requery",
                tick_index,
                precheck_path,
            )
        return original(*args, **kwargs)

    return guarded


def _build_hard_timeout_runner(timeout_error_cls: type[BaseException]) -> Callable[..., Any]:
    def hardened(*, timeout_sec: int | float, call: Callable[[], Any]):
        if float(timeout_sec) <= 0:
            last_stage = str(os.getenv("PB1_LAST_STAGE") or "unknown")
            raise timeout_error_cls(
                f"tick_hard_timeout timeout_sec={timeout_sec} last_stage={last_stage}"
            )

        def _alarm_handler(signum, frame):
            del signum, frame
            last_stage = str(os.getenv("PB1_LAST_STAGE") or "unknown")
            raise _HardTickAbort(
                f"tick_hard_timeout timeout_sec={timeout_sec} last_stage={last_stage}"
            )

        prev = signal.getsignal(signal.SIGALRM)
        try:
            signal.signal(signal.SIGALRM, _alarm_handler)
            signal.setitimer(signal.ITIMER_REAL, float(timeout_sec))
            try:
                return call()
            except _HardTickAbort as exc:
                # Translate only after the entire tick stack has unwound past all
                # broad ``except Exception`` fail-soft handlers.  The existing
                # outer PB1 loop can then keep its TickTimeoutError recovery path.
                raise timeout_error_cls(str(exc)) from None
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, prev)

    return hardened


def _install_safe_guards() -> None:
    global _SAFE_INSTALLED
    if _SAFE_INSTALLED:
        return

    from trader.kis_wrapper import KisAPI
    import trader.reconcile_kis as rk

    if not getattr(KisAPI, "_kr_p0_balance_ttl_installed", False):
        KisAPI.get_balance_cached = _build_balance_cache_guard(KisAPI.get_balance_cached)
        KisAPI._kr_p0_balance_ttl_installed = True

    if not getattr(rk, "_kr_p0_unresolved_refresh_installed", False):
        rk.reconcile_kis = _build_reconcile_guard(rk.reconcile_kis)
        rk._kr_p0_unresolved_refresh_installed = True

    _SAFE_INSTALLED = True


def _pb1_module_ready(module: Any) -> bool:
    return bool(
        module is not None
        and callable(getattr(module, "run_once", None))
        and callable(getattr(module, "_run_once_with_hard_timeout", None))
        and isinstance(getattr(module, "TickTimeoutError", None), type)
    )


def install_kr_20260928_runtime_integrity() -> None:
    """Install Sep-28 P0 guards without circular-importing ``pb1_runner``.

    The sanctioned KR session entrypoint calls legacy guard installation before
    importing pb1_runner.  In that case this function imports pb1_runner while
    ``_INSTALLING`` is set; the recursive call made at pb1_runner's module top is
    ignored, the module finishes defining its functions, and wrapping happens
    here.  Conversely, direct test imports arrive with a partially initialised
    pb1_runner already in ``sys.modules``; those calls install the safe KIS/
    reconcile layer and defer PB1 function wrapping instead of dereferencing
    missing attributes.
    """
    global _PB1_INSTALLED, _INSTALLING
    if _PB1_INSTALLED or _INSTALLING:
        return

    _INSTALLING = True
    try:
        _install_safe_guards()

        pb1_runner = sys.modules.get("trader.pb1_runner")
        if pb1_runner is not None and not _pb1_module_ready(pb1_runner):
            logger.info(
                "[KR_P0][20260928][DEFER_PB1_WRAP] reason=PARTIAL_PB1_IMPORT "
                "safe_guards=1 action=wait_for_sanctioned_kr_entrypoint"
            )
            return

        if pb1_runner is None:
            # Called from trade_session_runner (the production KR entrypoint).
            # Its import of pb1_runner recursively calls the legacy installer;
            # _INSTALLING above makes that recursive Sep-28 install a no-op.
            import trader.pb1_runner as pb1_runner

        if not _pb1_module_ready(pb1_runner):
            logger.warning(
                "[KR_P0][20260928][DEFER_PB1_WRAP] reason=PB1_NOT_READY_AFTER_IMPORT "
                "safe_guards=1"
            )
            return

        if not getattr(pb1_runner, "_kr_p0_precheck_one_shot_installed", False):
            pb1_runner.run_once = _build_run_once_precheck_guard(pb1_runner.run_once)
            pb1_runner._kr_p0_precheck_one_shot_installed = True

        if not getattr(pb1_runner, "_kr_p0_uncatchable_watchdog_installed", False):
            pb1_runner._run_once_with_hard_timeout = _build_hard_timeout_runner(
                pb1_runner.TickTimeoutError
            )
            pb1_runner._kr_p0_uncatchable_watchdog_installed = True

        # pb1_runner imports reconcile_kis by value.  Rebind after the safe
        # module wrapper exists so its first per-tick reconciliation cannot
        # bypass the unresolved-broker refresh contract.
        import trader.reconcile_kis as rk
        pb1_runner.reconcile_kis = rk.reconcile_kis

        _PB1_INSTALLED = True
        logger.info(
            "[KR_P0][20260928][INSTALLED] balance_ttl=1 precheck_one_shot=1 "
            "unresolved_force_refresh=1 watchdog_escape=1"
        )
    finally:
        _INSTALLING = False
