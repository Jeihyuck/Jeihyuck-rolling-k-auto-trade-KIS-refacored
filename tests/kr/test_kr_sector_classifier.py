from trader.kr.sector_classifier import classify_kr_sector

def test_sector_keywords():
    assert classify_kr_sector({"name":"반도체 장비"})["sector_cluster"] == "SEMICONDUCTOR"
    assert classify_kr_sector({"industry":"바이오 제약"})["sector_cluster"] == "BIO_HEALTHCARE"
    assert classify_kr_sector({"theme":"2차전지 양극재"})["sector_cluster"] == "SECONDARY_BATTERY"
    assert classify_kr_sector({"industry":"은행 금융지주"})["sector_cluster"] == "FINANCIAL"
    assert classify_kr_sector({"name":"자동차 부품"})["sector_cluster"] == "AUTO"
    assert classify_kr_sector({"industry":"통신 유틸리티"})["is_defensive"] is True
    assert classify_kr_sector({"name":"알수없음"})["sector_cluster"] == "UNKNOWN"
