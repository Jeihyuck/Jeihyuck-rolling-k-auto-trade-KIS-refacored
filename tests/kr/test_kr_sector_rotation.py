from trader.kr.sector_rotation import evaluate_kr_sector_rotation
class P:
    sector_strength={"FINANCIAL":{"vs_kospi_3d":0.02},"AUTO":{"vs_kospi_3d":0.01},"BIO_HEALTHCARE":{"vs_kospi_3d":-0.02}}

def test_kospi_value_leadership():
    r=evaluate_kr_sector_rotation(trade_date="2026-07-09", provider=P(), final30_rows=[], index_context={"kospi200_3d_return":0.01,"kosdaq150_3d_return":-0.01})
    assert r["rotation_regime"] == "KR_KOSPI_VALUE_LEAD"
    assert "FINANCIAL" in r["sector_leaders"]

class BasketProvider:
    returns = {
        ("105560", 3): 0.04, ("055550", 3): 0.03, ("086790", 3): 0.03, ("316140", 3): 0.02, ("032830", 3): 0.02,
        ("005930", 3): 0.00, ("000660", 3): 0.00, ("042700", 3): 0.00, ("000990", 3): 0.00, ("058470", 3): 0.00,
    }

def test_configured_basket_calculates_sector_leaders(monkeypatch):
    monkeypatch.setenv("KR_SECTOR_PROXY_CONFIG_PATH", "config/kr_sector_proxy_map.json")
    r=evaluate_kr_sector_rotation(trade_date="2026-07-09", provider=BasketProvider(), final30_rows=[], index_context={"kospi_3d_return":0.0,"kosdaq_3d_return":0.0,"kospi200_3d_return":0.01,"kosdaq150_3d_return":-0.01})
    assert "FINANCIAL" in r["sector_leaders"]
    assert r["sector_proxy_quality"]["FINANCIAL"] == "medium"
