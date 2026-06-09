#!/usr/bin/env python3
from trader.us.market_calendar import is_us_trading_day, now_ny


def main() -> int:
    now = now_ny()
    if is_us_trading_day(now.date()):
        outputs = {
            "run_mode": "TRADE",
            "signal_only": "0",
            "order_allowed": "1",
            "kis_order_allowed": "1",
            "kis_data_allowed": "1",
            "non_trading_day": "0",
            "reason": "trading_day",
        }
    else:
        outputs = {
            "run_mode": "NON_TRADING_SIGNAL_ONLY",
            "signal_only": "1",
            "order_allowed": "0",
            "kis_order_allowed": "0",
            "kis_data_allowed": "0",
            "non_trading_day": "1",
            "reason": "non_trading_day_signal_only",
        }
    for key, value in outputs.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
