from trader.us.runner.daily_report_runner import _position_market_value_usd

def test_qty_times_current_px_fallback(): assert _position_market_value_usd({'qty':3,'current_px':12.5})==37.5
