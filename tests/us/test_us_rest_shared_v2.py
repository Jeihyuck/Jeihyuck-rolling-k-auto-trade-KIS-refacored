import time
from trader.marketdata import us_rest_shared as shared

def test_native_price_cache_keeps_original_receipt_and_rejects_errors(tmp_path):
    path=tmp_path/"quote.json"
    at=time.time()
    row={"rt_cd":"0","output":{"last":"100"},"_quote_source":"KIS_LIVE",
         "_quote_quality":"FRESH","_quote_asof_epoch":at}
    shared._publish(path,row)
    read=shared._read(path)
    assert read["_quote_asof_epoch"]==at and read["_quote_source"]=="KIS_CACHE"
    shared._publish(path,{**row,"rt_cd":"1"})
    assert shared._read(path)["_quote_asof_epoch"]==at
    assert not shared._valid(row,at+6)

def test_shared_key_is_scoped_by_market_credential_exchange():
    assert shared._paths("a","ABC","NAS")!=shared._paths("b","ABC","NAS")
    assert shared._paths("a","ABC","NAS")!=shared._paths("a","ABC","NYS")
