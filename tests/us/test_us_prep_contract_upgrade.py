from __future__ import annotations

import json

import pytest


def _base_contract(status: str, entry_can_proceed: int, trade_date: str = "2026-08-05") -> dict:
    return {
        "trade_date": trade_date,
        "status": status,
        "entry_can_proceed": entry_can_proceed,
        "exit_can_proceed": 1,
        "close_can_proceed": 1,
        "trade_can_proceed": 1,
        "trade_block_reason": "ok" if entry_can_proceed else "force_entry_block",
        "run_revision": "new-rev",
        "git_commit_sha": "new-rev",
        "warnings": [],
        "errors": [],
    }


def test_upgrade_from_defense_to_ok_allows_recovery_replace(monkeypatch, tmp_path):
    import trader.us.prep_contract as pc

    trade_date = "2026-08-05"
    contract = _base_contract("OK_WITH_WARNINGS", 1, trade_date=trade_date)
    existing = {
        "trade_date": trade_date,
        "status": "DEFENSE_CRASH_ENTRY_BLOCKED",
        "entry_can_proceed": 0,
        "run_revision": "old-rev",
        "git_commit_sha": "old-rev",
    }

    calls: list[dict] = []

    def _fake_pin(trade_date, revision, **kwargs):
        calls.append({"trade_date": trade_date, "revision": revision, **kwargs})
        if len(calls) == 1:
            raise RuntimeError("trade_day_revision_already_pinned")
        return {"trade_date": trade_date, "run_revision": revision}

    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract", lambda _td: existing)
    monkeypatch.setattr("trader.us.run_manifest.pin_run_revision", _fake_pin)
    monkeypatch.setattr("trader.us.db.repos.get_today_broker_progress_order_count", lambda trade_date=None: 0)
    monkeypatch.setattr(pc, "us_prep_contract_path", lambda td: tmp_path / td / "prep_contract.json")
    monkeypatch.setattr(pc, "us_signals_prep_contract_path", lambda: tmp_path / "signals" / "prep_contract.json")
    monkeypatch.setattr(pc, "us_signals_latest_prep_contract_path", lambda: tmp_path / "signals" / "latest_prep_contract.json")

    saved = pc.save_us_prep_contract(contract)
    assert saved["ok"] is True
    assert len(calls) == 2
    assert calls[1]["replace"] is True
    assert calls[1]["allow_recovery_upgrade"] is True
    assert calls[1]["replace_reason"] == "RECOVERY_UPGRADE_ALLOWED"
    assert calls[1]["replace_meta"]["old_status"] == "DEFENSE_CRASH_ENTRY_BLOCKED"
    assert calls[1]["replace_meta"]["new_status"] == "OK_WITH_WARNINGS"


def test_upgrade_blocked_when_order_already_progressed(monkeypatch, tmp_path):
    import trader.us.prep_contract as pc

    trade_date = "2026-08-05"
    contract = _base_contract("OK", 1, trade_date=trade_date)
    existing = {
        "trade_date": trade_date,
        "status": "DEFENSE_CRASH_ENTRY_BLOCKED",
        "entry_can_proceed": 0,
        "run_revision": "old-rev",
        "git_commit_sha": "old-rev",
    }

    def _fake_pin(_trade_date, _revision, **_kwargs):
        raise RuntimeError("trade_day_revision_already_pinned")

    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract", lambda _td: existing)
    monkeypatch.setattr("trader.us.run_manifest.pin_run_revision", _fake_pin)
    monkeypatch.setattr("trader.us.db.repos.get_today_broker_progress_order_count", lambda trade_date=None: 1)
    monkeypatch.setattr(pc, "us_prep_contract_path", lambda td: tmp_path / td / "prep_contract.json")
    monkeypatch.setattr(pc, "us_signals_prep_contract_path", lambda: tmp_path / "signals" / "prep_contract.json")
    monkeypatch.setattr(pc, "us_signals_latest_prep_contract_path", lambda: tmp_path / "signals" / "latest_prep_contract.json")

    with pytest.raises(RuntimeError, match="trade_day_revision_already_pinned"):
        pc.save_us_prep_contract(contract)


def test_recoverable_contract_does_not_pin_manifest(monkeypatch, tmp_path):
    import trader.us.prep_contract as pc

    trade_date = "2026-08-05"
    contract = _base_contract("DEFENSE_CRASH_ENTRY_BLOCKED", 0, trade_date=trade_date)
    contract["recoverable_contract"] = True

    calls = {"pin": 0}

    def _fake_pin(*_args, **_kwargs):
        calls["pin"] += 1
        return {}

    monkeypatch.setattr("trader.us.path_contract.load_us_prep_contract", lambda _td: {})
    monkeypatch.setattr("trader.us.run_manifest.pin_run_revision", _fake_pin)
    monkeypatch.setattr("trader.us.db.repos.get_today_broker_progress_order_count", lambda trade_date=None: 0)
    monkeypatch.setattr(pc, "us_prep_contract_path", lambda td: tmp_path / td / "prep_contract.json")
    monkeypatch.setattr(pc, "us_signals_prep_contract_path", lambda: tmp_path / "signals" / "prep_contract.json")
    monkeypatch.setattr(pc, "us_signals_latest_prep_contract_path", lambda: tmp_path / "signals" / "latest_prep_contract.json")

    saved = pc.save_us_prep_contract(contract)
    assert saved["ok"] is True
    assert calls["pin"] == 0

    runtime_payload = json.loads((tmp_path / trade_date / "prep_contract.json").read_text(encoding="utf-8"))
    assert runtime_payload["recoverable_contract"] is True


def test_defense_crash_contract_is_marked_recoverable():
    from trader.us.prep_contract import build_us_prep_contract
    from tests.us.test_us_prep_contract import _make_valid_inputs

    du, cp, wl, val, paths = _make_valid_inputs(30)
    wl["market_state_overlay"] = {
        "market_state": "DEFENSE_CRASH",
        "market_regime": "RISK_OFF",
        "allow_new_buy": False,
        "force_entry_block": True,
    }
    contract = build_us_prep_contract(
        trade_date="2026-08-05",
        env="practice",
        status="OK_WITH_WARNINGS",
        dynamic_universe_result=du,
        candidate_pool_result=cp,
        watchlist_result=wl,
        validation=val,
        paths=paths,
    )
    assert contract["status"] == "DEFENSE_CRASH_ENTRY_BLOCKED"
    assert contract["recoverable_contract"] is True