from __future__ import annotations

import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from trader.strategy_recovery import recover_lots_from_sources


def _temp_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="recovery_test_"))


def test_multi_sid_recovery_from_orders_map() -> None:
    tmp = _temp_dir()
    now = datetime.now()
    holdings = [{"pdno": "000001", "qty": 5, "avg_price": 1000.0}]
    orders_map = {
        "client-S3": {
            "order_id": "client-S3",
            "pdno": "000001",
            "sid": "S3",
            "side": "BUY",
            "qty": 2,
            "price": 1000.0,
            "ts": (now - timedelta(minutes=5)).isoformat(),
            "status": "submitted",
        },
        "client-S4": {
            "order_id": "client-S4",
            "pdno": "000001",
            "sid": "S4",
            "side": "BUY",
            "qty": 3,
            "price": 1000.0,
            "ts": (now - timedelta(minutes=1)).isoformat(),
            "status": "submitted",
        },
    }
    lots, diagnostics = recover_lots_from_sources(holdings, {"lots": []}, orders_map, [], [], tmp)
    assert len(lots) == 2, diagnostics
    assert sorted((lot["sid"], lot["remaining_qty"]) for lot in lots) == [("S3", 2), ("S4", 3)]
    shutil.rmtree(tmp, ignore_errors=True)


def test_ledger_based_recovery_when_orders_missing() -> None:
    tmp = _temp_dir()
    now = datetime.now()
    holdings = [{"pdno": "000002", "qty": 2, "avg_price": 500.0}]
    ledger_rows = [
        {"event": "FILL", "pdno": "000002", "sid": "S2", "side": "BUY", "qty": 2, "timestamp": now.isoformat()}
    ]
    lots, _ = recover_lots_from_sources(holdings, {"lots": []}, {}, ledger_rows, [], tmp)
    assert len(lots) == 1 and lots[0]["sid"] == "S2"
    shutil.rmtree(tmp, ignore_errors=True)


def test_manual_fallback_marks_safe_exit() -> None:
    tmp = _temp_dir()
    holdings = [{"pdno": "000099", "qty": 1, "avg_price": 1.0}]
    lots, _ = recover_lots_from_sources(holdings, {"lots": []}, {}, [], [], tmp)
    assert len(lots) == 1
    lot = lots[0]
    assert lot["sid"] in {"MANUAL", "UNKNOWN"}
    assert lot.get("meta", {}).get("safe_exit_required") is True
    shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    test_multi_sid_recovery_from_orders_map()
    test_ledger_based_recovery_when_orders_missing()
    test_manual_fallback_marks_safe_exit()
    print("recovery_self_test: OK")


if __name__ == "__main__":
    main()
