# -*- coding: utf-8 -*-
"""US Runner Dispatcher.

python -m trader.us.runner.dispatcher --mode <mode> --env practice
"""
from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)

MODE_ALIASES: dict[str, str] = {
    "trade-am": "session-am",
    "trade-pm": "session-afternoon",
    "trade-afternoon": "session-afternoon",
    "trade-close": "close",
}

MODES = (
    "prep",
    "tick",
    "session-am",
    "session-afternoon",
    "open",
    "mid",
    "close",
    "report",
    "all",
    "trade-am",
    "trade-pm",
    "trade-afternoon",
    "trade-close",
    "kis-diag",
)


def normalize_mode(mode: str) -> str:
    """mode alias를 실제 모드로 변환한다."""
    return MODE_ALIASES.get(mode, mode)


def dispatch(mode: str, env: str = "practice", offline: bool = False, force_now: str | None = None, max_ticks: int = 0) -> int:
    """모드에 따라 적절한 runner를 호출.

    Returns:
        0 (성공) or 1 (실패)
    """
    logger.info("[US_DISPATCHER][START] mode=%s env=%s offline=%s", mode, env, offline)

    mode = normalize_mode(mode)

    if mode == "all":
        results = []
        for m in ("prep", "open", "mid", "close", "report"):
            rc = dispatch(m, env=env, offline=offline, force_now=force_now)
            results.append(rc)
        return 0 if all(r == 0 for r in results) else 1

    if mode == "prep":
        from trader.us.runner.prep_runner import run_prep
        r = run_prep(env=env, offline=offline, force_now=force_now)
        return 0 if r.get("status") == "OK" else 1

    if mode == "open":
        from trader.us.runner.trade_open_runner import run_trade_open
        r = run_trade_open(env=env, offline=offline, force_now=force_now)
        return 0 if r.get("status") in ("OK", "SKIP") else 1

    if mode == "mid":
        from trader.us.runner.trade_mid_runner import run_trade_mid
        r = run_trade_mid(env=env, offline=offline)
        return 0 if r.get("status") in ("OK", "SKIP") else 1

    if mode == "close":
        from trader.us.runner.trade_close_runner import run_trade_close
        r = run_trade_close(env=env, offline=offline, force_now=force_now)
        return 0 if r.get("status") == "OK" else 1

    if mode == "report":
        from trader.us.runner.daily_report_runner import run_daily_report
        r = run_daily_report(env=env, offline=offline)
        return 0 if r.get("status") == "OK" else 1

    if mode == "session-am":
        from trader.us.runner.trade_session_runner import run_trade_session
        r = run_trade_session(session="am", env=env, offline=offline, force_now=force_now, max_ticks=max_ticks)
        return 0 if r.get("status") in ("OK", "OK_WITH_WARNINGS", "SKIP") else 1

    if mode == "session-afternoon":
        from trader.us.runner.trade_session_runner import run_trade_session
        r = run_trade_session(session="afternoon", env=env, offline=offline, force_now=force_now, max_ticks=max_ticks)
        return 0 if r.get("status") in ("OK", "OK_WITH_WARNINGS", "SKIP") else 1

    if mode == "tick":
        from trader.us.runner.trade_tick_runner import run_trade_tick
        r = run_trade_tick(session="manual", env=env, offline=offline, force_now=force_now)
        return 0 if r.get("status") in ("OK", "OK_WITH_WARNINGS", "SKIP") else 1

    if mode == "kis-diag":
        from trader.us.runner.kis_diag_runner import run_kis_diag
        r = run_kis_diag(env=env, offline=offline)
        return 0 if r.get("status") in ("OK", "PARTIAL", "SKIP") else 1

    logger.error("[US_DISPATCHER][ERROR] unknown mode=%s", mode)
    return 1


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="US Agent Dispatcher")
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None)
    parser.add_argument("--max-ticks", dest="max_ticks", type=int, default=0)
    args = parser.parse_args()

    rc = dispatch(args.mode, env=args.env, offline=args.offline, force_now=args.force_now, max_ticks=args.max_ticks)
    sys.exit(rc)


if __name__ == "__main__":
    main()
