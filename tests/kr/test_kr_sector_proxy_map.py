from trader.kr.market_state_overlay import evaluate_kr_market_state
from trader.kr.sector_proxy_map import compute_sector_basket_return, load_sector_proxy_config


def test_basket_partial_returns_compute_average_3d():
    values = {"A": 0.03, "B": 0.00, "C": -0.015}
    result = compute_sector_basket_return("SEMICONDUCTOR", ["A", "B", "C", "D", "E"], 3, lambda s, lb: values.get(s))
    assert result["return_3d"] == 0.005
    assert result["valid_count"] == 3
    assert result["missing_count"] == 2
    assert result["source_quality"] == "medium"


def test_basket_all_missing_is_suspect():
    result = compute_sector_basket_return("SEMICONDUCTOR", ["A", "B", "C", "D", "E"], 3, lambda s, lb: None)
    assert result["return_3d"] is None
    assert result["source_quality"] == "suspect"


def test_suspect_only_sector_cannot_create_strong_risk_on():
    ctx = {"kospi_1d_return": 0.02, "kosdaq_1d_return": 0.02, "kospi200_1d_return": 0.02, "kosdaq150_1d_return": 0.02, "index_resolution": {}}
    overlay = evaluate_kr_market_state(ctx, sector_proxy_summary={"SEMICONDUCTOR": {"source_quality": "suspect"}})
    assert overlay["market_state"] != "KR_STRONG_RISK_ON"


def test_config_file_loads_sector_baskets():
    cfg = load_sector_proxy_config("config/kr_sector_proxy_map.json")
    assert "SEMICONDUCTOR" in cfg
    assert cfg["SEMICONDUCTOR"]["source"] == "basket"
