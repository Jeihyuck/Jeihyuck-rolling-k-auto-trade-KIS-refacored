"""Independent read-only KIS KR completed-minute observer for standard PB1.

Usage (practice only, no broker orders):
python -m scripts.kr_opening_observer --once --final30-file PATH
python -m scripts.kr_opening_observer --loop --final30-file PATH

Must run as an independent WSL process; never called synchronously in the
PB1 BUY/SELL hot path. Not enabled by scheduler changes in this PR.
"""
from __future__ import annotations

import argparse
from datetime import datetime, time as dtime
import json
import logging
from pathlib import Path
import time

from trader.account_state import get_account_key
from trader.kr.opening_bar_contract import KST, save_opening_proof
from trader.runtime_paths import runtime_path
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def final30_codes(*, trade_date: str, filename: str | None = None) -> list[str]:
    file = Path(filename) if filename else runtime_path(
        "runtime", "kr", "watchlist", trade_date, "final30_scored.json"
    )
    payload = json.loads(file.read_text(encoding="utf-8"))
    if (not isinstance(payload, dict)
            or payload.get("trade_date") != trade_date
            or payload.get("market") != "KR"
            or payload.get("env") != "practice"):
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    rows = payload.get("rows") or []
    codes = sorted({
        str(r.get("code") or "").zfill(6)
        for r in rows if isinstance(r, dict) and str(r.get("code") or "").strip()
    })
    if len(codes) != len(rows) or not 1 <= len(codes) <= 30:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
    return codes


def run_once(*, kis, codes: list[str], observed_at: datetime, sleep_between: float = 0.8) -> dict:
    """No order methods are called; all I/O is read-only KIS and local proofs."""
    observed_at = observed_at.astimezone(KST)
    if not dtime(9, 0) <= observed_at.time() <= dtime(15, 5):
        return {"state": "OUTSIDE_SESSION", "completed": 0, "unknown": len(codes)}
    scope = get_account_key(env="practice")
    done, unknown = 0, 0
    for code in codes:
        if str(code).zfill(6) == "122630":
            # Separate owner: never collect KR_INFINITE by PB1 proof route.
            continue
        try:
            data = kis.get_intraday_candles_today(code, start_hhmm="090000")
            quote = kis.get_price_snapshot(code, market="J")
            if not isinstance(quote, dict):
                quote = {}
            raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
            vwap = quote.get("wghn_avrg_stck_prc") or raw.get("wghn_avrg_stck_prc")
            # Native weighted-average price is evidence only when its quote
            # timestamp/date can be checked. A cached/WS value with no
            # native clock MUST NOT be labeled fresh by assumption.
            from datetime import timedelta
            quote_day = str(quote.get("stck_bsop_date") or raw.get("stck_bsop_date") or "").replace("-", "")
            quote_clock = str(quote.get("stck_cntg_hour") or raw.get("stck_cntg_hour") or "").strip().zfill(6)
            verified_vwap = False
            if (quote_day == observed_at.strftime("%Y%m%d")
                    and len(quote_clock) == 6 and quote_clock.isdigit()):
                try:
                    quote_time = datetime.strptime(
                        quote_day + quote_clock, "%Y%m%d%H%M%S"
                    ).replace(tzinfo=KST)
                    age = (now_kst() - quote_time).total_seconds()
                    verified_vwap = -10 <= age <= 120
                except ValueError:
                    verified_vwap = False
            result = save_opening_proof(
                env="practice", account_scope=scope, code=code,
                now=now_kst(), raw_bars=data,
                vwap=vwap if verified_vwap else None,
                vwap_source="KIS_NATIVE_WEIGHTED_AVG" if verified_vwap else "UNKNOWN",
            )
            if result["bars"] > 0:
                done += 1
                logger.info(
                    "[KR][OPENING_OBSERVER] code=%s bars=%s verified_source=KIS "
                    "role=READ_ONLY no_broker_orders=1",
                    code, result["bars"],
                )
            else:
                unknown += 1
        except Exception as exc:
            unknown += 1
            logger.warning(
                "[KR][OPENING_OBSERVER][UNKNOWN] code=%s error_type=%s "
                "action=NO_TIMING_PROOF",
                code, type(exc).__name__,
            )
        # This independent collector is rate-limited and does not delay SELL.
        if sleep_between > 0:
            time.sleep(max(sleep_between, 0.0))
    return {"state": "COMPLETE", "completed": done, "unknown": unknown}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--final30-file")
    parser.add_argument("--date")
    parser.add_argument("--interval-sec", type=float, default=180.0)
    args = parser.parse_args()
    if args.once == args.loop:
        parser.error("select exactly one of --once or --loop")
    logging.basicConfig(level=logging.INFO)
    from trader.kis_wrapper import KisAPI
    kis = KisAPI(kis_env="practice")
    today = args.date or now_kst().date().isoformat()
    codes = final30_codes(trade_date=today, filename=args.final30_file)
    if args.once:
        print(json.dumps(run_once(kis=kis, codes=codes, observed_at=now_kst()), ensure_ascii=False))
        return
    while True:
        now = now_kst()
        if now.date().isoformat() != today or now.time() > dtime(15, 5):
            break
        if now.time() >= dtime(9, 0):
            result = run_once(kis=kis, codes=codes, observed_at=now)
            logger.info("[KR][OPENING_OBSERVER][PASS] completed=%s unknown=%s", result["completed"], result["unknown"])
        time.sleep(max(60.0, float(args.interval_sec)))


if __name__ == "__main__":
    main()
