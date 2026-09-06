from trader.us.runner.daily_report_utils import fill_is_synthetic, positive_float


def test_daily_report_utils_positive_float():
    assert positive_float("3.5") == 3.5
    assert positive_float(0) is None
    assert positive_float("nope") is None


def test_daily_report_utils_fill_is_synthetic():
    assert fill_is_synthetic({"meta": {"synthetic": True}})
    assert fill_is_synthetic({"meta": '{"fill_evidence_type":"BALANCE_DELTA_SYNTHETIC"}'})
    assert not fill_is_synthetic({"meta": {"is_synthetic": False}})
