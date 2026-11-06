"""Pure native HTTP TP quote fence regressions; no broker calls."""
from trader.us.execution.order_router import _tp_live_quote_threshold_proven

INTENT = {"limit_price": 103, "meta": {
    "broker_avg_price": 100, "tp_threshold_fraction": "0.03",
}}
NOW = 1793910000.0

def quote(last=104, age=0, quality="FRESH", source="KIS_LIVE"):
    return {"output": {"last": str(last)}, "_quote_quality": quality,
            "_quote_source": source, "_quote_asof_epoch": NOW - age}

def test_live_tp_threshold_holds():
    assert _tp_live_quote_threshold_proven(INTENT, quote(), now_epoch=NOW)[0]

def test_price_drops_below_tp_threshold():
    ok, reason = _tp_live_quote_threshold_proven(INTENT, quote(last=102), now_epoch=NOW)
    assert not ok and reason == "US_TP_LIVE_THRESHOLD_LOST"

def test_stale_or_missing_quote_never_submits():
    for value in (None, {}, quote(age=6), quote(quality="STALE"),
                  quote(source="KIS_STALE_CACHE"), quote(last="NaN"),
                  {"output": {"last": "104"}}):
        assert not _tp_live_quote_threshold_proven(INTENT, value, now_epoch=NOW)[0]

def test_order_limit_below_threshold_fails_even_if_market_high():
    intent = {**INTENT, "limit_price": 102}
    assert not _tp_live_quote_threshold_proven(intent, quote(last=110), now_epoch=NOW)[0]
