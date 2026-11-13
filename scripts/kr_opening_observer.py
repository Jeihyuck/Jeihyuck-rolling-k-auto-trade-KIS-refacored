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
from trader.kr.opening_bar_contract import KST, save_opening_proof, inspect_completed_bar_coverage
from trader.runtime_paths import repo_root
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def final30_codes(*, trade_date: str, filename: str | None = None) -> list[str]:
    """Read the exact PREP canonical Final30 with its sibling dated contract.

    The canonical Final30 payload may be an array, or a dict with items/rows.
    The source file is not itself the authority for the trade date/market/env:
    `prep_contract.json` is. Do not infer scope from the file's directory.
    """
    # PREP writes the canonical mirrored file under the REPOSITORY runtime
    # directory. The generic ephemeral runtime_path() defaults to
    # /tmp/trader_runtime, which has no Final30 in actual WSL AM sessions.
    # Do not silently accept a different artifact root/date.
    file = (
        Path(filename) if filename else
        repo_root() / "runtime" / "kr" / "watchlist" / trade_date / "final30_scored.json"
    )
    # PREP's production canonical artifact is sometimes mirrored under an
    # extra trade-date directory. The 2026-11-10 AM archive proves:
    #   runtime/kr/watchlist/YYYY-MM-DD/final30_scored.json
    #   runtime/kr/watchlist/YYYY-MM-DD/YYYY-MM-DD/prep_contract.json
    # Search only these two same-day canonical locations, never stale latest
    # aliases. If both exist, reject conflicting date/as_of/contract hash.
    contract_paths = (
        file.with_name("prep_contract.json"),
        file.parent / trade_date / "prep_contract.json",
    )
    existing = [p for p in contract_paths if p.is_file()]
    if not existing:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    try:
        payload = json.loads(file.read_text(encoding="utf-8"))
        contracts = [json.loads(p.read_text(encoding="utf-8")) for p in existing]
        contract = contracts[0]
        for other in contracts[1:]:
            for field in ("trade_date", "market", "env", "contract_hash"):
                if (contract.get(field) is not None and other.get(field) is not None
                        and str(contract[field]) != str(other[field])):
                    raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_CONFLICTING_CONTRACTS")
            left_asof = str(contract.get("actual_as_of") or contract.get("as_of") or contract.get("expected_as_of") or "")[:10]
            right_asof = str(other.get("actual_as_of") or other.get("as_of") or other.get("expected_as_of") or "")[:10]
            if not left_asof or left_asof != right_asof:
                raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_CONFLICTING_CONTRACTS")
    except (ValueError, OSError, TypeError) as exc:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH") from exc
    if (
        not isinstance(contract, dict)
        or str(contract.get("trade_date") or "")[:10] != trade_date
        or str(contract.get("market") or "").upper() != "KR"
        or str(contract.get("env") or "").lower() != "practice"
        or not bool(contract.get("contract_ok"))
        or not bool(contract.get("trade_can_proceed"))
        or int(contract.get("final30_rows") or 0) != 30
    ):
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    contract_asof = str(
        contract.get("actual_as_of") or contract.get("as_of")
        or contract.get("expected_as_of") or ""
    )[:10]
    if not contract_asof:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    # PREP contract can have 'actual_as_of' instead of 'as_of'; do not reject
    # valid production artifacts or silently allow a mismatched scoring date.
    expected_asof = str(contract.get("expected_as_of") or "")[:10]
    if expected_asof and expected_asof != contract_asof:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
    if isinstance(payload, dict):
        for field, expected in (
            ("trade_date", trade_date),
            ("market", "KR"),
            ("env", "practice"),
            ("as_of", contract_asof),
            ("expected_as_of", contract_asof),
        ):
            if payload.get(field) is not None and str(payload[field])[:10] != expected:
                raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_SCOPE_MISMATCH")
        rows = (payload.get("rows") or payload.get("items")
                or payload.get("data") or payload.get("final30") or [])
    elif isinstance(payload, list):
        rows = payload
    else:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
    if not isinstance(rows, list) or len(rows) != 30:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
    codes = []
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
        code = str(row.get("code") or "").strip()
        if not code.isdigit() or len(code) > 6 or not code:
            raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
        codes.append(code.zfill(6))
    if len(set(codes)) != 30:
        raise RuntimeError("KR_OPENING_OBSERVER_FINAL30_BAD_UNIVERSE")
    return sorted(codes)

def run_once(*, kis, codes: list[str], observed_at: datetime, sleep_between: float = 0.8) -> dict:
    """No order methods are called; all I/O is read-only KIS and local proofs."""
    observed_at = observed_at.astimezone(KST)
    if not dtime(9, 0) <= observed_at.time() <= dtime(15, 5):
        return {"state": "OUTSIDE_SESSION", "completed": 0, "unknown": len(codes)}
    scope = get_account_key(env="practice")
    done, unknown, coverage_ok, missing_vwap = 0, 0, 0, 0
    symbol_scan_sec: list[float] = []
    for code in codes:
        symbol_started = time.monotonic()
        if str(code).zfill(6) == "122630":
            # Separate owner: never collect KR_INFINITE by PB1 proof route.
            continue
        try:
            # KIS time-itemchart returns bounded minute windows. Read both
            # the opening anchor and the latest anchor; a fixed 09:00 query
            # alone cannot prove a post-09:30 rebound. Never fill gaps with
            # synthetic open/high/low/close or an inferred business date.
            anchors = (
                ("092900", observed_at.strftime("%H%M%S"))
                if observed_at.time() >= dtime(9, 30)
                else (observed_at.strftime("%H%M%S"),)
            )
            data: list[dict] = []
            for anchor in dict.fromkeys(anchors):
                try:
                    rows = kis.get_intraday_candles_today(code, start_hhmm=anchor)
                    if isinstance(rows, list):
                        data.extend(row for row in rows if isinstance(row, dict))
                except Exception as minute_exc:
                    logger.warning(
                        "[KR][OPENING_OBSERVER][PARTIAL_MINUTE_FETCH] code=%s "
                        "anchor=%s error_type=%s",
                        code, anchor, type(minute_exc).__name__,
                    )
            if not data:
                raise RuntimeError("NO_KIS_NATIVE_INTRADAY_MINUTE_ROWS")
            quote = kis.get_price_snapshot(code, market="J")
            if not isinstance(quote, dict):
                quote = {}
            raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
            vwap = (
                quote["wghn_avrg_stck_prc"]
                if "wghn_avrg_stck_prc" in quote
                else raw.get("wghn_avrg_stck_prc")
            )  # Never silently replace an explicit invalid top-level source.
            quote_origin = str(quote.get("source") or raw.get("source") or "UNKNOWN")
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
            proof_now = now_kst()
            coverage = inspect_completed_bar_coverage(rows=data, now=proof_now)
            result = save_opening_proof(
                env="practice", account_scope=scope, code=code,
                now=proof_now, raw_bars=data,
                vwap=vwap if verified_vwap else None,
                vwap_source="KIS_NATIVE_WEIGHTED_AVG" if verified_vwap else "UNKNOWN",
            )
            if coverage["state"] == "COVERED":
                coverage_ok += 1
            if not verified_vwap:
                missing_vwap += 1
            logger.info(
                "[KR][OPENING_OBSERVER][COVERAGE] code=%s state=%s reason=%s "
                "opening_count=%s post_open_count=%s bar_age_sec=%s "
                "vwap_verified=%s vwap_origin=%s day_present=%s "
                "exchange_clock_present=%s read_only=1",
                code, coverage["state"], coverage["reason"],
                coverage["opening_count"], coverage["post_open_count"],
                coverage["latest_bar_age_sec"], int(verified_vwap),
                quote_origin, int(bool(quote_day)),
                int(bool(quote.get("stck_cntg_hour") or raw.get("stck_cntg_hour"))),
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
        finally:
            elapsed_for_symbol = time.monotonic() - symbol_started
            symbol_scan_sec.append(elapsed_for_symbol)
            logger.info(
                "[KR][OPENING_OBSERVER][SYMBOL_LATENCY] code=%s elapsed_sec=%.3f",
                code, elapsed_for_symbol,
            )
        # This independent collector is rate-limited and does not delay SELL.
        if sleep_between > 0:
            time.sleep(max(sleep_between, 0.0))
    ordered_latencies = sorted(symbol_scan_sec)
    p95 = (ordered_latencies[max(0, int((len(ordered_latencies) * 0.95 + 0.999999)) - 1)]
           if ordered_latencies else None)
    return {
        "state": "COMPLETE", "completed": done, "unknown": unknown,
        "coverage_ok": coverage_ok, "vwap_unverified": missing_vwap,
        "symbol_scan_p95_sec": p95,
        "symbol_scan_max_sec": ordered_latencies[-1] if ordered_latencies else None,
        "native_quote_requests_added": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--final30-file")
    parser.add_argument("--date")
    parser.add_argument("--interval-sec", type=float, default=120.0)
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
        if now.time() < dtime(9, 0):
            # An AM wrapper may start before the cash session: wake near 09:00
            # rather than sleeping the entire polling interval before the
            # first native opening-minute collection.
            time.sleep(2.0)
            continue
        cycle_started = time.monotonic()
        result = run_once(kis=kis, codes=codes, observed_at=now)
        elapsed = time.monotonic() - cycle_started
        logger.info(
            "[KR][OPENING_OBSERVER][PASS] completed=%s unknown=%s "
            "coverage_ok=%s vwap_unverified=%s symbol_p95_sec=%s "
            "symbol_max_sec=%s elapsed_sec=%.1f target_period_sec=%.1f",
            result["completed"], result["unknown"], result["coverage_ok"],
            result["vwap_unverified"], result["symbol_scan_p95_sec"],
            result["symbol_scan_max_sec"], elapsed,
            max(60.0, float(args.interval_sec)),
        )
        # Schedule from the *start* of the last 30-symbol scan, not from
        # its end. Otherwise every proof expires during the guaranteed
        # 'cycle work + 180sec sleep' blind spot while the BUY gate is ON.
        time.sleep(max(1.0, max(60.0, float(args.interval_sec)) - elapsed))


if __name__ == "__main__":
    main()
