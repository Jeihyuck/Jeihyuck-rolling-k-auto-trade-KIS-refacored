"""No-secret guard tests for the one-off, practice-only US TP DB release."""
import json

import pytest

from scripts.us_historical_tp_oneoff_release_20261119 import (
    EXPECTED, BRANCH, REPO, PRACTICE_REST,
    validate_candidates, validate_event, seal_broker_order_gates,
)


def case(symbol, day, stage, qty):
    cycle = "original-cycle-" + symbol
    return {
        "trade_date": day, "symbol": symbol, "stage": stage,
        "stage_status": "NOT_TRIGGERED", "requested_qty": qty,
        "cumulative_filled_qty": 0, "trading_epoch_id": "active",
        "position_lifecycle_id": cycle,
        "client_order_key": f"US_PC_{day}_{symbol}_{cycle}_TAKE_PROFIT_{stage.upper()}",
        "raw_broker_order_no": "", "canonical_broker_order_no": "",
        "state": {"meta": {
            "last_status": "US_RECOVERY_SCOPE_UNVERIFIED",
            "last_stage": stage, "strategy_owner": "US_STANDARD",
        }},
    }


def rows():
    return [case(symbol, day, stage, qty) for (day, symbol, stage), qty in EXPECTED.items()]


def test_exact_four_apply_targets():
    validate_candidates(rows())


@pytest.mark.parametrize("transform", [
    lambda a: a[:3],
    lambda a: a + [case("GILD", "2026-10-29", "tp1", 5)],
    lambda a: a + [a[0].copy()],
    lambda a: [dict(a[0], requested_qty=2)] + a[1:],
    lambda a: [dict(a[0], symbol="TQQQ")] + a[1:],
    lambda a: [dict(a[0], stage_status="DONE")] + a[1:],
    lambda a: [dict(a[0], cumulative_filled_qty=1)] + a[1:],
    lambda a: [dict(a[0], raw_broker_order_no="0001")] + a[1:],
    lambda a: [dict(a[0], state={"meta": {"last_status": "OPEN", "last_stage": "tp3"}})] + a[1:],
])
def test_unexpected_local_state_never_applies(transform):
    with pytest.raises(RuntimeError):
        validate_candidates(transform(rows()))


def event():
    return {
        "action": "ready_for_review",
        "pull_request": {
            "draft": False,
            "head": {"ref": BRANCH, "repo": {"full_name": REPO}},
            "base": {"ref": "dual-agent"},
            "user": {"login": "Jeihyuck"},
        },
    }


def test_only_exact_owner_ready_event(monkeypatch, tmp_path):
    f = tmp_path / "event.json"
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("GITHUB_REPOSITORY", REPO)
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(f))
    monkeypatch.setenv("US_TP_ONEOFF_RELEASE", "AUTHORIZED_PR371_PRACTICE_REPAIR")
    f.write_text(json.dumps(event()))
    validate_event()
    for key, newval in [
        ("action", "synchronize"), ("action", "opened"),
    ]:
        wrong = event()
        wrong[key] = newval
        f.write_text(json.dumps(wrong))
        with pytest.raises(RuntimeError, match="SCOPE_NOT_AUTHORIZED"):
            validate_event()
    wrong = event()
    wrong["pull_request"]["head"]["repo"]["full_name"] = "untrusted/fork"
    f.write_text(json.dumps(wrong))
    with pytest.raises(RuntimeError, match="SCOPE_NOT_AUTHORIZED"):
        validate_event()
    wrong = event()
    wrong["pull_request"]["user"]["login"] = "unknown"
    f.write_text(json.dumps(wrong))
    with pytest.raises(RuntimeError, match="SCOPE_NOT_AUTHORIZED"):
        validate_event()


def test_kis_live_host_never_accepted(monkeypatch):
    monkeypatch.setenv("KIS_REST_URL", "https://openapi.koreainvestment.com:9443")
    with pytest.raises(RuntimeError, match="EXACT_PRACTICE"):
        seal_broker_order_gates()


def test_all_broker_order_gates_forced_off(monkeypatch):
    monkeypatch.setenv("KIS_REST_URL", PRACTICE_REST)
    monkeypatch.delenv("PGOPTIONS", raising=False)
    for flag in ("US_ORDER_ARMED", "US_LIVE_TRADING_ENABLED", "LIVE_TRADING_ENABLED"):
        monkeypatch.setenv(flag, "1")
    seal_broker_order_gates()
    for flag in ("US_ORDER_ARMED", "US_LIVE_TRADING_ENABLED", "LIVE_TRADING_ENABLED",
                 "US_KIS_ORDER_ALLOWED", "ALLOW_REAL_ORDER"):
        assert __import__("os").environ[flag] == "0"
    assert __import__("os").environ["DISABLE_REAL_TRADING"] == "1"


def test_readonly_session_cannot_be_used_to_claim_success(monkeypatch):
    monkeypatch.setenv("KIS_REST_URL", PRACTICE_REST)
    monkeypatch.setenv("PGOPTIONS", "-c default_transaction_read_only=on")
    with pytest.raises(RuntimeError, match="READ_ONLY_DATABASE"):
        seal_broker_order_gates()
