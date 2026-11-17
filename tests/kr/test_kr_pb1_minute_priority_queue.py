"""Regression tests for read-only KR PB1 minute collection priorities."""
from __future__ import annotations

from datetime import datetime, timedelta
import json

import pytest

from trader.kr import entry_priority_queue as pq
from trader.kr.opening_bar_contract import KST


@pytest.fixture
def seed_prep(tmp_path, monkeypatch):
    monkeypatch.setattr(pq, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pq, "_current_sha", lambda: "a" * 40)
    date = "2026-11-17"
    root = tmp_path / "runtime" / "kr" / "watchlist" / date
    root.mkdir(parents=True)
    (root / "final30_scored.json").write_text(
        json.dumps({
            "trade_date": date, "market": "KR", "env": "practice",
            "as_of": "2026-11-16",
            "rows": [{"code": f"{i:06d}", "score": float(100 - i)}
                     for i in range(1, 31)],
        }), encoding="utf-8",
    )
    (root / "prep_contract.json").write_text(
        json.dumps({
            "trade_date": date, "market": "KR", "env": "practice",
            "contract_ok": True, "trade_can_proceed": True,
            "final30_rows": 30, "expected_as_of": "2026-11-16",
            "actual_as_of": "2026-11-16", "contract_hash": "test-prep-hash",
        }), encoding="utf-8",
    )
    return date, root


def moment(hour=9, minute=0):
    return datetime(2026, 11, 17, hour, minute, tzinfo=KST)


def test_no_minute_proof_is_needed_to_seed_preopen_collection(seed_prep):
    date, _ = seed_prep
    state = pq.collection_priorities(date=date, account_scope="practice:test",
                                      now=moment())
    assert state["state"] == "PREP_FALLBACK"
    assert state["hot"] == [f"{i:06d}" for i in range(1, 6)]
    assert set(state["hot"] + state["warm"] + state["cold"]) == {
        f"{i:06d}" for i in range(1, 31)
    }


def test_pb1_publisher_prioritizes_actual_candidates_without_restricting_universe(seed_prep):
    date, _ = seed_prep
    published = pq.publish_pb1_priority(
        date=date, account_scope="practice:test",
        eligible_codes=["000021", "000022", "000023"],
        held_codes=["000022"], stage="PB1-AM", now=moment(9, 5),
    )
    assert published["state"] == "PUBLISHED"
    snapshot = pq.collection_priorities(date=date, account_scope="practice:test",
                                        now=moment(9, 6))
    assert snapshot["state"] == "PB1_ACTIVE"
    assert snapshot["hot"] == ["000021", "000023"]
    assert "000022" in snapshot["cold"]
    assert len(set(snapshot["hot"] + snapshot["warm"] + snapshot["cold"])) == 30


def test_approved_add_and_other_scope_do_not_inherit_pb1_no_add_exclusion(seed_prep):
    date, _ = seed_prep
    pq.publish_pb1_priority(date=date, account_scope="practice:test",
                            eligible_codes=["000022"], held_codes=["000022"],
                            allow_add_to_existing=True, now=moment(9, 10))
    loaded = pq.collection_priorities(date=date, account_scope="practice:test",
                                      now=moment(9, 11))
    assert loaded["hot"] == ["000022"]
    separate = pq.collection_priorities(date=date, account_scope="different",
                                        now=moment(9, 11))
    assert separate["state"] == "PREP_FALLBACK"


def test_expired_tampered_or_cross_day_manifest_is_never_used(seed_prep):
    date, _ = seed_prep
    pq.publish_pb1_priority(date=date, account_scope="scope",
                            eligible_codes=["000025"], now=moment(9, 1))
    assert pq.collection_priorities(date=date, account_scope="scope",
                                    now=moment(9, 8))["state"] == "PREP_FALLBACK"
    pq.publish_pb1_priority(date=date, account_scope="scope",
                            eligible_codes=["000025"], now=moment(9, 10))
    p = pq.manifest_path(date, "scope")
    raw = json.loads(p.read_text(encoding="utf-8"))
    raw["hot"] = ["000001"]
    p.write_text(json.dumps(raw), encoding="utf-8")
    assert pq.collection_priorities(date=date, account_scope="scope",
                                    now=moment(9, 11))["state"] == "PREP_FALLBACK"
    assert pq.collection_priorities(date="2026-11-18", account_scope="scope",
                                    now=moment(9, 11) + timedelta(days=1)
                                    )["state"] == "INVALID_PREP"


def test_schedule_new_hot_promotion_is_immediate_and_warm_cold_not_starved():
    schedule = pq.CandidateSchedule(max_batch=5, hot_sec=45,
                                    warm_sec=60, cold_sec=120)
    state = {"hot": ["000001", "000002", "000003"],
             "warm": ["000004", "000005"],
             "cold": ["000006", "000007", "000008"]}
    first = schedule.select(state, moment(9, 0))
    assert "000001" in first and "000004" in first and "000006" in first
    promoted = {"hot": ["000008", "000001", "000002", "000003"],
                "warm": ["000004", "000005"],
                "cold": ["000006", "000007"]}
    next_batch = schedule.select(promoted, moment(9, 0) + timedelta(seconds=5))
    assert "000008" in next_batch
    observed = set(first + next_batch)
    for minute in range(1, 30):
        observed.update(schedule.select(promoted, moment(9, 0) +
                                        timedelta(minutes=minute)))
    assert set(promoted["hot"] + promoted["warm"] + promoted["cold"]) <= observed


def test_bad_prep_contract_never_seeds_collection(seed_prep):
    date, root = seed_prep
    (root / "prep_contract.json").write_text(
        json.dumps({"trade_date": date, "market": "US",
                    "env": "practice", "contract_ok": True,
                    "trade_can_proceed": True, "final30_rows": 30}),
        encoding="utf-8",
    )
    assert pq.collection_priorities(date=date, account_scope="scope",
                                    now=moment())["state"] == "INVALID_PREP"


def test_six_slot_hot_warm_cold_capacity_and_poll_gaps():
    """Deterministic ideal-cycle replay, NOT evidence of broker bar freshness."""
    state = {
        "hot": [f"{n:06d}" for n in range(1, 6)],
        "warm": [f"{n:06d}" for n in range(6, 11)],
        "cold": [f"{n:06d}" for n in range(11, 31)],
    }
    schedule = pq.CandidateSchedule(max_batch=6, hot_sec=45,
                                    warm_sec=180, cold_sec=600)
    current = moment(9, 0)
    before = schedule.diagnostics(state, current, interval_sec=45)
    assert before["tiers"]["hot"]["never_polled"] == 5
    assert before["tiers"]["cold"]["never_polled"] == 20
    assert before["overcommitted"] is True
    assert before["all_180s_possible_at_slot_capacity"] is False
    assert before["polls_not_proofs"] is True

    latest = {}
    gaps = {"hot": [], "warm": [], "cold": []}
    for tick in range(90):
        now = current + timedelta(seconds=45 * tick)
        selected = schedule.select(state, now)
        assert len(selected) <= 6
        assert len(selected) == len(set(selected))
        for tier, codes in state.items():
            for code in selected:
                if code not in codes:
                    continue
                if code in latest:
                    gaps[tier].append((now - latest[code]).total_seconds())
                latest[code] = now
    assert len(latest) == 30
    # HOT remains high priority; 2 WARM slots remove the old deterministic
    # 225s gap. COLD is best effort, not a promise of 180s broker freshness.
    assert max(gaps["hot"]) <= 90
    assert max(gaps["warm"]) <= 180
    assert max(gaps["cold"]) <= 720


def test_hot_overcrowding_reports_capacity_while_retaining_fail_closed_ttl():
    state = {
        "hot": [f"{n:06d}" for n in range(1, 17)],
        "warm": [f"{n:06d}" for n in range(17, 21)],
        "cold": [f"{n:06d}" for n in range(21, 31)],
    }
    schedule = pq.CandidateSchedule(max_batch=6, hot_sec=45,
                                    warm_sec=180, cold_sec=600)
    diag = schedule.diagnostics(state, moment(9, 0), interval_sec=45)
    assert diag["overcommitted"]
    # The scheduler prioritizes collection only: a late KIS bar still fails
    # the unchanged 180s entry gate, regardless of poll scheduling.
    assert schedule.select(state, moment(9, 0))
