import json
from trader.kr.sector_proxy_map import load_kr_sector_proxy_map, compute_sector_proxy_return
class P:
    returns = {("IDX",3):0.03,("ETF",3):0.02,("A",3):0.01,("B",3):0.03}

def test_load_override_and_priority(tmp_path, monkeypatch):
    path=tmp_path/'m.json'; path.write_text(json.dumps({"SEMICONDUCTOR":{"primary_index":"IDX","etf_proxies":["ETF"],"basket_symbols":["A","B"]}}))
    m=load_kr_sector_proxy_map(str(path)); assert m["SEMICONDUCTOR"]["primary_index"] == "IDX"
    monkeypatch.setenv("KR_SECTOR_PROXY_CONFIG_PATH", str(path))
    r=compute_sector_proxy_return(sector="SEMICONDUCTOR", provider=P(), trade_date="2026-07-09", lookbacks=(3,), index_context={"kospi_3d_return":0.01})
    assert r["source"] == "krx_index" and r["return_3d"] == 0.03 and r["vs_kospi_3d"] == 0.02

class PrivateReturnProvider:
    def _kr_return_from_daily(self, symbol, lookback):
        return {("A",3):0.03,("B",3):0.01}.get((symbol, lookback))

def test_private_kr_return_from_daily_fallback(monkeypatch, tmp_path):
    path=tmp_path/'m.json'; path.write_text(json.dumps({"FINANCIAL":{"basket_symbols":["A","B"]}}))
    monkeypatch.setenv("KR_SECTOR_PROXY_CONFIG_PATH", str(path))
    r=compute_sector_proxy_return(sector="FINANCIAL", provider=PrivateReturnProvider(), trade_date="2026-07-09", lookbacks=(3,), index_context={"kospi_3d_return":0.0})
    assert r["return_3d"] == 0.02
