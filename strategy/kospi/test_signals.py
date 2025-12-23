from strategy.kospi import signals


def test_split_positions_unknown_fallback_to_target() -> None:
    positions = {
        "000001": {"qty": 5, "avg_price": 100.0, "market": None},
        "000002": {"qty": 3, "avg_price": 200.0, "market": "KOSDAQ"},
    }
    targets = {"000001": {"target_qty": 5}}
    kospi_positions, excluded = signals._split_positions_for_kospi(positions, targets)
    assert "000001" in kospi_positions
    assert "000002" not in kospi_positions
    assert "000002" in excluded


def test_split_positions_keeps_kosdaq_out_of_kospi() -> None:
    positions = {"000010": {"qty": 1, "avg_price": 50.0, "market": "KOSDAQ"}}
    kospi_positions, excluded = signals._split_positions_for_kospi(positions, {})
    assert kospi_positions == {}
    assert excluded == ["000010"]
