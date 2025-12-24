from __future__ import annotations

import json
import os
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from trader import state_store
from trader.io_atomic import append_jsonl
from trader.strategy_recovery import recover_sid_for_holding


def _setup_temp_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="recovery_test_"))


def _write_order(path: Path, pdno: str, sid: str, ts: datetime) -> None:
    append_jsonl(
        path,
        {
            "order_id": f"{pdno}-{sid}-{ts.timestamp()}",
            "pdno": pdno,
            "sid": sid,
            "side": "BUY",
            "qty": 1,
            "price": 1000,
            "ts": ts.isoformat(),
            "reason": "test",
        },
    )


def _write_fill(path: Path, pdno: str, sid: str, ts: datetime) -> None:
    append_jsonl(
        path,
        {
            "ts": ts.isoformat(),
            "order_id": f"{pdno}-{sid}-{ts.timestamp()}",
            "pdno": pdno,
            "sid": sid,
            "side": "BUY",
            "qty": 1,
            "price": 1000,
            "source": "test",
            "note": "fill",
        },
    )


def test_recovery_prefers_recent_fill() -> None:
    tmp = _setup_temp_dir()
    orders_path = tmp / "orders_map.jsonl"
    fills_dir = tmp / "fills"
    fills_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    _write_order(orders_path, "000001", "S1", now - timedelta(days=40))
    _write_order(orders_path, "000001", "S3", now - timedelta(days=1))
    _write_fill(fills_dir / f"fills_{now.strftime('%Y%m%d')}.jsonl", "000001", "S3", now - timedelta(hours=2))

    sid, conf, reasons = recover_sid_for_holding(
        "000001",
        10,
        1000.0,
        now,
        {"orders_map": orders_path, "fills_dir": fills_dir},
    )
    assert sid == "S3" and conf >= 0.8, f"expected S3 with confidence, got {sid} {conf} {reasons}"


def test_conflict_returns_manual() -> None:
    tmp = _setup_temp_dir()
    orders_path = tmp / "orders_map.jsonl"
    fills_dir = tmp / "fills"
    fills_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now()

    _write_order(orders_path, "000002", "S1", now - timedelta(hours=1))
    _write_fill(fills_dir / f"fills_{now.strftime('%Y%m%d')}.jsonl", "000002", "S3", now - timedelta(minutes=10))

    sid, conf, reasons = recover_sid_for_holding(
        "000002",
        5,
        0.0,
        now,
        {"orders_map": orders_path, "fills_dir": fills_dir},
    )
    assert sid == "MANUAL" or any("conflict" in r for r in reasons)


def test_no_evidence_is_manual() -> None:
    sid, conf, _ = recover_sid_for_holding("000003", 1, 0.0, datetime.now(), {})
    assert sid == "MANUAL"
    assert conf < 0.8


def test_legacy_migration_unknown_to_manual() -> None:
    tmp = _setup_temp_dir()
    bot_state_dir = tmp / "bot_state"
    bot_state_dir.mkdir(parents=True, exist_ok=True)
    legacy_path = bot_state_dir / "state.json"
    legacy_path.write_text(json.dumps({"version": 1, "lots": [{"pdno": "000010", "remaining_qty": 2, "strategy_id": "UNKNOWN"}]}))

    orig_cwd = Path.cwd()
    orig_state_path = state_store.STATE_PATH
    orig_state_dir = state_store.STATE_DIR
    orig_orders_map = state_store.ORDERS_MAP_PATH
    orig_log_dir = state_store.LOG_DIR
    orig_ensure_dirs = state_store.ensure_dirs
    try:
        tmp_state_dir = tmp / "trader" / "state"
        tmp_log_dir = tmp / "trader" / "logs"
        tmp_state_dir.mkdir(parents=True, exist_ok=True)
        tmp_log_dir.mkdir(parents=True, exist_ok=True)

        def _ensure_dirs() -> None:
            tmp_state_dir.mkdir(parents=True, exist_ok=True)
            (tmp / "trader" / "fills").mkdir(parents=True, exist_ok=True)
            tmp_log_dir.mkdir(parents=True, exist_ok=True)

        state_store.STATE_DIR = tmp_state_dir
        state_store.STATE_PATH = tmp_state_dir / "state.json"
        state_store.ORDERS_MAP_PATH = tmp_state_dir / "orders_map.jsonl"
        state_store.LOG_DIR = tmp_log_dir
        state_store.ensure_dirs = _ensure_dirs  # type: ignore[assignment]

        os.chdir(tmp)
        migrated = state_store.load_state()
        assert migrated["schema_version"] == 2
        assert migrated["lots"]
        assert migrated["lots"][0]["strategy_id"] in {"MANUAL", "S1", "S2", "S3", "S4", "S5"}
    finally:
        state_store.STATE_PATH = orig_state_path
        state_store.STATE_DIR = orig_state_dir
        state_store.ORDERS_MAP_PATH = orig_orders_map
        state_store.LOG_DIR = orig_log_dir
        state_store.ensure_dirs = orig_ensure_dirs  # type: ignore[assignment]
        os.chdir(orig_cwd)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    test_recovery_prefers_recent_fill()
    test_conflict_returns_manual()
    test_no_evidence_is_manual()
    test_legacy_migration_unknown_to_manual()
    print("recovery_self_test: OK")
