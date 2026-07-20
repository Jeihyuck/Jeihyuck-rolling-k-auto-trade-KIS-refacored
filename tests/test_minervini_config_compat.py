from trader.strategies.pb1_minervini_v2 import MinerviniConfig


def test_new_minervini_fields_have_legacy_contract():
    config = MinerviniConfig()
    rs = getattr(config, "rs_min_percentile", getattr(config, "rs_min", 70))
    assert float(rs) > 0
    assert float(getattr(config, "heavy_volume_mult", getattr(config, "heavy_vol_mult_10", 1))) > 0
