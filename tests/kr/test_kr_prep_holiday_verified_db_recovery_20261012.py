"""2026-10-12 KR holiday PREP and safe afternoon recovery regressions.

Purely read-only tests: no KIS orders and no changes to a real Supabase DB.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from trader import time_utils
from trader.kr.calendar import resolve_kr_expected_as_of
from trader.universe.build import _load_fallback_universe_from_db
from scripts import check_prep_duplicate_guard as duplicate_guard


AS_OF = "2026-10-08"


def _members(n: int = 195) -> list[dict]:
    return [
        {
            "code": f"{i:06d}",
            "market": "KOSPI" if i <= 100 else "KOSDAQ",
            "rank": i,
        }
        for i in range(1, n + 1)
    ]


def _snapshot(**changes) -> dict:
    result = {
        "status": "SUCCESS",
        "as_of": AS_OF,
        "provider": "krx_marketcap_asof",
        "actual_as_of": AS_OF,
        "run_id": "previous-verified-run",
        "members": _members(),
    }
    result.update(changes)
    return result


def test_oct12_prep_asof_skips_oct9_krx_holiday(monkeypatch):
    # Exercise the canonical resolver without external pykrx/HTTP calls.
    assert date(2026, 10, 9) in time_utils._load_krx_holidays()
    monkeypatch.setattr(
        time_utils,
        "is_krx_trading_day",
        lambda d: d.weekday() < 5 and d != date(2026, 10, 9),
    )
    assert resolve_kr_expected_as_of(date(2026, 10, 12)) == date(2026, 10, 8)


def test_verified_db_fallback_accepts_exact_previous_session(monkeypatch):
    monkeypatch.delenv("UNIVERSE_NO_FALLBACK", raising=False)
    assert len(
        _load_fallback_universe_from_db(
            repo=object(), env="practice", strategy="best_k_meta",
            as_of_date=AS_OF, snapshot=_snapshot(), require_exact_as_of=True,
        )
    ) == 195


def test_verified_db_fallback_rejects_stale_unverified_and_small(monkeypatch):
    monkeypatch.delenv("UNIVERSE_NO_FALLBACK", raising=False)
    for snapshot in (
        _snapshot(as_of="2026-10-07"),
        _snapshot(status="FAILED"),
        _snapshot(provider="emergency_seed"),
        _snapshot(provider="kis_marketcap_top"),
        _snapshot(actual_as_of="2026-10-07"),
        _snapshot(members=_members(20)),
        _snapshot(members=[*(_members(194)), {"code": "999999", "market": ""}]),
        _snapshot(members=[*(_members(194)), _members()[0]]),
    ):
        assert not _load_fallback_universe_from_db(
            repo=object(), env="practice", strategy="best_k_meta",
            as_of_date=AS_OF, snapshot=snapshot, require_exact_as_of=True,
        )


def test_duplicate_guard_requires_same_trade_date(monkeypatch):
    monkeypatch.setattr(duplicate_guard, "get_engine", lambda: object())

    class FakeLedger:
        def __init__(self, event):
            self.event = event

        def get_prep_event(self, **kwargs):
            return self.event

    payload = {
        "as_of": AS_OF,
        "trade_date": "2026-10-09",
        "status": "READY",
        "quality_ok": 1,
        "trade_can_proceed": 1,
        "final30_count": 30,
    }
    ledger = FakeLedger({"payload_json": payload})
    monkeypatch.setattr(duplicate_guard, "LedgerEventsRepo", lambda engine: ledger)
    # Same as-of from the previous *trade date* is NOT a valid duplicate.
    assert not duplicate_guard._same_trade_date_prep_ready(
        env="practice", as_of=AS_OF, trade_date="2026-10-12",
    )
    # A new PREP for Oct12 can be safely skipped on its own retry.
    payload["trade_date"] = "2026-10-12"
    assert duplicate_guard._same_trade_date_prep_ready(
        env="practice", as_of=AS_OF, trade_date="2026-10-12",
    )


def test_afternoon_entry_never_bypasses_failed_prep_provenance():
    root = Path(__file__).resolve().parents[2]
    workflow = (root / ".github/workflows/trade-afternoon.yml").read_text(encoding="utf-8")
    assert "get_prep_event(" in workflow
    assert "trade_date=today" in workflow
    assert "PREP_NOT_READY_FOR_TRADE_DATE" in workflow
    # Inspect only the new-BUY PM path. The existing exit-only Close path
    # deliberately accepts a 30-row snapshot even when PREP is not ready.
    pm_entry_block = workflow.split("      - name: Run PM entry loop", 1)[1].split(
        "      - name: Run PM exit-only loop", 1
    )[0]
    assert "|| steps.verify_final30.outputs.final30_rows == '30'" not in pm_entry_block
    assert "steps.verify_final30.outputs.prep_ready == '1'" in pm_entry_block
    assert "steps.verify_final30.outputs.prep_ready != '1'" in workflow
