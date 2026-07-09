from trader.kr.sector_rotation import evaluate_kr_sector_rotation
class P:
    sector_strength={"FINANCIAL":{"vs_kospi_3d":0.02},"AUTO":{"vs_kospi_3d":0.01},"BIO_HEALTHCARE":{"vs_kospi_3d":-0.02}}

def test_kospi_value_leadership():
    r=evaluate_kr_sector_rotation(trade_date="2026-07-09", provider=P(), final30_rows=[], index_context={"kospi200_3d_return":0.01,"kosdaq150_3d_return":-0.01})
    assert r["rotation_regime"] == "KR_KOSPI_VALUE_LEAD"
    assert "FINANCIAL" in r["sector_leaders"]
