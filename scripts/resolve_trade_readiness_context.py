from __future__ import annotations

import argparse
import datetime as dt

from trader.time_utils import resolve_trade_readiness_as_of


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve trade readiness as_of context.")
    parser.add_argument("--run-date", required=True, help="Run date in YYYY-MM-DD")
    parser.add_argument("--window", required=True, help="Market window label")
    parser.add_argument("--as-of", help="Requested as_of date in YYYY-MM-DD")
    parser.add_argument("--exchange", default="KRX", help="Exchange name")
    return parser.parse_args()


def _to_date(value: str | None) -> dt.date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    return dt.date.fromisoformat(raw)


def main() -> int:
    args = _parse_args()
    resolved = resolve_trade_readiness_as_of(
        run_date=dt.date.fromisoformat(args.run_date),
        market_window=(args.window or "").strip().lower(),
        exchange=(args.exchange or "KRX").strip().upper(),
        candidate_as_of=_to_date(args.as_of),
    )

    for key in ("run_date", "calendar_prev", "requested_as_of", "resolved_as_of"):
        value = resolved[key]
        print(f"{key.upper()}={value.isoformat()}")
    print(f"READINESS_REASON={resolved['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())