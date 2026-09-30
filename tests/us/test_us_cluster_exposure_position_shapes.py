import pytest

from trader.us.rotation import apply_cap_flags, compute_cluster_exposure


def test_db_position_shape_uses_qty_times_current_px_for_cluster_value():
    exposure = compute_cluster_exposure(
        [
            {"symbol": "NVDA", "qty": 10, "current_px": 227.75, "unrealized_pnl_usd": -20.45},
            {"symbol": "JNJ", "qty": 8, "current_px": 267.21, "unrealized_pnl_usd": -27.52},
        ],
        equity=10_000,
    )
    assert exposure["AI_SEMI"]["cluster_market_value"] == pytest.approx(2277.5)
    assert exposure["AI_SEMI"]["cluster_weight"] == pytest.approx(0.22775)
    assert exposure["HEALTHCARE"]["cluster_market_value"] == pytest.approx(2137.68)


def test_kis_market_value_remains_authoritative_when_present():
    exposure = compute_cluster_exposure(
        [{"symbol": "NVDA", "qty": 10, "current_price_usd": 227.75, "market_value_usd": 2300.0}],
        equity=10_000,
    )
    assert exposure["AI_SEMI"]["cluster_market_value"] == pytest.approx(2300.0)
    assert exposure["AI_SEMI"]["cluster_weight"] == pytest.approx(0.23)


def test_reconcile_and_db_shapes_produce_same_cap_decision():
    kis = [{"symbol": "NVDA", "qty": 10, "market_value_usd": 2500.0, "current_price_usd": 250.0}]
    db = [{"symbol": "NVDA", "qty": 10, "current_px": 250.0}]
    kis_flags = apply_cap_flags(compute_cluster_exposure(kis, equity=10_000), "RISK_OFF")
    db_flags = apply_cap_flags(compute_cluster_exposure(db, equity=10_000), "RISK_OFF")
    assert kis_flags["AI_SEMI"]["cluster_weight"] == pytest.approx(db_flags["AI_SEMI"]["cluster_weight"])
    assert kis_flags["AI_SEMI"]["over_cap"] is True
    assert db_flags["AI_SEMI"]["over_cap"] is True


def test_avg_cost_is_last_resort_when_live_price_is_unavailable():
    exposure = compute_cluster_exposure(
        [{"symbol": "MSFT", "qty": 4, "avg_cost": 498.835}],
    )
    assert exposure["MEGA_TECH"]["cluster_market_value"] == pytest.approx(1995.34)
