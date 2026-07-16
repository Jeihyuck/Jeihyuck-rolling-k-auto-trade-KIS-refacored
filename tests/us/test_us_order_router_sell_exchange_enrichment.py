from trader.us.execution.order_router import enrich_sell_exchange


def test_sell_exchange_empty_but_holding_exchange_exists():
    intent = {"symbol": "AAPL", "side": "SELL", "exchange": "", "current_holding": {"exchange": "NASDAQ"}}
    assert enrich_sell_exchange(intent) == "NASDAQ"
    assert intent["exchange"] == "NASDAQ"


def test_kis_raw_nys_normalizes_to_nyse():
    intent = {"symbol": "IBM", "side": "SELL", "exchange": "", "kis_balance_position": {"ovrs_excg_cd": "NYS"}}
    assert enrich_sell_exchange(intent) == "NYSE"
    assert intent["exchange"] == "NYSE"


def test_kis_raw_ams_normalizes_to_amex():
    intent = {"symbol": "ETF", "side": "SELL", "exchange": "", "kis_balance_position": {"ovrs_excg_cd": "AMS"}}
    assert enrich_sell_exchange(intent) == "AMEX"
    assert intent["exchange"] == "AMEX"


def test_kis_raw_nas_and_nasd_normalize_to_nasdaq():
    for raw in ("NAS", "NASD"):
        intent = {"symbol": "AAPL", "side": "SELL", "exchange": "", "kis_balance_position": {"ovrs_excg_cd": raw}}
        assert enrich_sell_exchange(intent) == "NASDAQ"
        assert intent["exchange"] == "NASDAQ"
