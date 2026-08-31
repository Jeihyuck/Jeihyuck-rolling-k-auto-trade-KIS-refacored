from trader.position_lifecycle import imported_runtime_state, lifecycle_is_authoritative


def test_oci_legacy_lifecycle_is_rejected_and_stale_trailing_is_not_copied():
    stale = {"position_cycle_id": "legacy-cycle-oci", "portfolio_epoch_id": "legacy-epoch-practice",
             "position_origin": "RECOVERY", "entry_date": "2026-05-27", "holding_bars": 68,
             "max_price": 397500, "last_trail_stop": 334107, "tp1_done": True}
    assert not lifecycle_is_authoritative(stale)
    current = imported_runtime_state(epoch_id="current-epoch", price=271660)
    assert current["position_cycle_id"] != stale["position_cycle_id"]
    assert current["entry_ts"] is None
    assert current["max_price"] == 271660
    assert current["last_trail_stop"] is None
    assert current["position_meta"]["holding_age_unknown"] is True
    assert current["position_meta"]["holding_bars"] == 0
    assert current["position_meta"]["trail_eligible"] is False


def test_samsung_legacy_invalid_stop_is_not_attached():
    stale = {"position_cycle_id": "legacy-cycle-samsung", "portfolio_epoch_id": "legacy-epoch-practice",
             "position_origin": "RECOVERY", "stop_price": 279465, "avg_buy_price": 265500}
    current = imported_runtime_state(epoch_id="current-epoch", price=265500)
    assert not lifecycle_is_authoritative(stale)
    assert "stop_price" not in current
    assert "initial_stop" not in current
