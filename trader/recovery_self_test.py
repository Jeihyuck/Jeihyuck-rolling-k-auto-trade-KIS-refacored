from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from trader.strategy_recovery import StrategyRecovery
from trader.io_atomic import append_jsonl


def _setup_temp_dirs() -> Path:
    return Path(tempfile.mkdtemp(prefix="recovery_test_"))


def _write_order(path: Path, pdno: str, sid: str, ts: datetime, qty: int = 1) -> None:
    append_jsonl(
        path,
        {
            "order_id": f"client-{sid}-{ts.timestamp()}",
            "pdno": pdno,
            "sid": sid,
            "side": "BUY",
            "qty": qty,
            "price": 1000,
            "ts": ts.isoformat(),
            "reason": "test",
        },
    )


def _write_fill(path: Path, pdno: str, sid: str, ts: datetime, qty: int = 1) -> None:
    append_jsonl(
        path,
        {
            "ts": ts.isoformat(),
            "order_id": f"client-{sid}-{ts.timestamp()}",
            "pdno": pdno,
            "sid": sid,
            "side": "BUY",
            "qty": qty,
            "price": 1000,
            "source": "test",
            "note": "fill",
        },
    )


def test_strategy_recovery_rules() -> None:
    tmp = _setup_temp_dirs()
    orders_path = tmp / "orders_map.jsonl"
    fills_dir = tmp / "fills"
    fills_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now()

    _write_order(orders_path, "000001", "S1", now - timedelta(days=40))
    _write_order(orders_path, "000001", "S3", now - timedelta(days=1), qty=3)
    fill_path = fills_dir / f"fills_{now.strftime('%Y%m%d')}.jsonl"
    _write_fill(fill_path, "000001", "S3", now - timedelta(hours=2), qty=2)

    os.environ["TRADER_STATE_DIR"] = str(tmp)
    recovery = StrategyRecovery(now_ts=now)
    recovery.orders_map = {}  # override to ensure fills dominate
    recovery.fills_rows = [
        {
            "ts": (now - timedelta(hours=2)).isoformat(),
            "pdno": "000001",
            "sid": "S3",
            "side": "BUY",
            "qty": 2,
            "price": 1000,
            "source": "test",
        }
    ]
    lots = recovery.recover("000001", 5, 1000.0, {"orders_map": str(orders_path), "fills_dir": str(fills_dir)})
    assert lots and lots[0]["sid"] == "S3", f"expected S3 from fills, got {lots}"

    recovery = StrategyRecovery(now_ts=now)
    recovery.fills_rows = []
    recovery.orders_map = {
        "client-S1": {
            "order_id": "client-S1",
            "pdno": "000002",
            "sid": "S1",
            "side": "BUY",
            "qty": 5,
            "ts": (now - timedelta(days=1)).isoformat(),
            "reason": "test",
        },
        "client-S2": {
            "order_id": "client-S2",
            "pdno": "000002",
            "sid": "S2",
            "side": "BUY",
            "qty": 4,
            "ts": (now - timedelta(days=2)).isoformat(),
            "reason": "test",
        },
    }
    lots = recovery.recover("000002", 3, 1000.0, {})
    assert lots[0]["sid"] == "S1", f"expected S1 from orders, got {lots}"

    recovery = StrategyRecovery(now_ts=now)
    recovery.fills_rows = []
    recovery.orders_map = {}
    lots = recovery.recover("000099", 1, 0.0, {})
    assert lots[0]["sid"] in {"MANUAL", "S1", "S2", "S3", "S4", "S5"}, "recovery should return a valid sid"

    shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    test_strategy_recovery_rules()
    print("recovery_self_test: OK")


if __name__ == "__main__":
    main()
