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
    assert state["hot"] == []
    assert state["warm"] == []
    # A stale/missing ranking must not promote arbitrary Final30 into HOT.
    # Keep the universe eligible for low-frequency discovery only.
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
    assert snapshot["warm"] == []  # No unrelated Final30 symbols in WARM.
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


def test_only_one_actual_candidate_is_hot_and_no_other_symbol_is_warm(seed_prep):
    date, _ = seed_prep
    pq.publish_pb1_priority(date=date, account_scope="practice:test",
                            eligible_codes=["000021"], now=moment(9, 5))
    result = pq.collection_priorities(date=date, account_scope="practice:test",
                                      now=moment(9, 6))
    assert result["state"] == "PB1_ACTIVE"
    assert result["hot"] == ["000021"]
    assert result["warm"] == []
    assert len(result["cold"]) == 29
    assert "000021" not in result["cold"]


def test_preminute_candidate_promotion_recovers_from_missing_manifest(seed_prep):
    date, _ = seed_prep
    fallback = pq.collection_priorities(date=date, account_scope="practice:test",
                                        now=moment(9, 3))
    assert fallback["hot"] == []
    pq.publish_pb1_priority(date=date, account_scope="practice:test",
                            eligible_codes=["000021"], now=moment(9, 4))
    active = pq.collection_priorities(date=date, account_scope="practice:test",
                                      now=moment(9, 5))
    assert active["state"] == "PB1_ACTIVE"
    assert active["hot"] == ["000021"]


def test_preminute_filter_drops_held_open_filled_zero_qty_and_blocking_intent():
    """Collection HOT is only genuinely available, positively-sized PB1 setup."""
    from types import SimpleNamespace
    def cf(code, *, qty=11, setup=True, key=None):
        return SimpleNamespace(
            code=code, planned_qty=qty, setup_ok=setup,
            client_order_key=key,
        )

    candidates = [
        cf("000021"),                    # Sole genuinely buyable setup
        cf("000022"),                    # Already held, no ADD
        cf("000023"),                    # Pending broker BUY
        cf("000024"),                    # Same-day BUY fill
        cf("000025", qty=0),             # No executable quantity
        cf("000026", key="known-ACK"),   # Durable broker intent
        cf("000027", setup=False),      # Only score, no setup
        cf("000028", key="local-REJECTED"),
        cf("000021"),                    # Dedup without changing ranking
    ]
    eligible = pq.eligible_preminute_codes(
        candidates=candidates,
        held_codes={"000022"},
        blocking_buy_codes={"A000023"},
        filled_buy_codes={"000024"},
        blocking_client_order_keys={"known-ACK"},
    )
    assert eligible == ["000021", "000028"]
    # An explicitly rejected local intent does NOT imply a KIS order.
    # A future retry remains subject to the owner duplicate/order gate.


def test_preminute_filter_rejects_invalid_quantity_without_faking_cash_proof():
    from types import SimpleNamespace
    candidates = [
        SimpleNamespace(code="000021", planned_qty="not-an-int", setup_ok=True),
        SimpleNamespace(code="000022", planned_qty=-1, setup_ok=True),
        SimpleNamespace(code="000023", planned_qty=None, setup_ok=True),
        SimpleNamespace(code="000024", planned_qty=1, setup_ok=True),
    ]
    assert pq.eligible_preminute_codes(candidates=candidates) == ["000024"]


def test_actual_pb1_publisher_uses_existing_db_evidence_before_hot_queue():
    """Guard only a read-only source contract, NOT real BUY authorization."""
    from pathlib import Path
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    start = source.index("eligible_preminute_codes(")
    publish = source.index("_priority_status = publish_pb1_priority(", start)
    assert start < publish
    section = source[start:publish]
    for name in ("ok_after_risk", "held_codes", "open_buy_codes",
                 "today_buy_fill_by_code", "prior_intent_by_key"):
        assert name in source[max(0, start - 900):publish], name
    assert "eligible_codes=_priority_codes" in source[publish:publish + 500]
    assert "is_order_authorization" in Path(
        "trader/kr/entry_priority_queue.py"
    ).read_text(encoding="utf-8")


def test_20261119_missing_hot_manifest_is_loud_but_never_collects_final30(seed_prep):
    """Incident replay: yesterday the observer idled after PREP_READY."""
    date, _ = seed_prep
    snapshot = pq.collection_priorities(
        date=date, account_scope="practice:test", now=moment(9, 3)
    )
    assert snapshot["state"] == "PREP_FALLBACK"
    assert snapshot["reason"] == "MISSING_MANIFEST"
    assert len(snapshot["cold"]) == 30
    native_only = {**snapshot, "cold": []}
    assert pq.CandidateSchedule(max_batch=6).select(native_only, moment(9, 3)) == []


def test_20261119_actual_final_three_not_filled_to_hot_five(seed_prep):
    """Only the already shortlisted three get native 1m calls."""
    from types import SimpleNamespace

    date, _ = seed_prep
    final_shortlist = [
        SimpleNamespace(code=code, setup_ok=True, planned_qty=qty,
                        client_order_key=code + "-BUY")
        for code, qty in (("000021", 11), ("000023", 62), ("000029", 172))
    ]
    eligible = pq.eligible_preminute_codes(candidates=final_shortlist)
    assert eligible == ["000021", "000023", "000029"]
    published = pq.publish_pb1_priority(
        date=date, account_scope="practice:test", eligible_codes=eligible,
        now=moment(9, 5), stage="PB1-AM",
    )
    assert published["hot"] == eligible
    assert published["warm"] == []
    priorities = pq.collection_priorities(
        date=date, account_scope="practice:test", now=moment(9, 6),
    )
    assert priorities["state"] == "PB1_ACTIVE"
    selected = pq.CandidateSchedule(max_batch=6).select(
        {**priorities, "cold": []}, moment(9, 6),
    )
    assert set(selected) == set(eligible)
    assert len(selected) == 3


def test_20261119_final_shortlist_zero_stays_zero_and_never_fake_fills(seed_prep):
    date, _ = seed_prep
    published = pq.publish_pb1_priority(
        date=date, account_scope="practice:test", eligible_codes=[],
        now=moment(9, 4), stage="PB1-AM",
    )
    assert published["state"] == "PUBLISHED"
    snapshot = pq.collection_priorities(
        date=date, account_scope="practice:test", now=moment(9, 5),
    )
    assert snapshot["state"] == "PB1_ACTIVE"
    assert snapshot["hot"] == snapshot["warm"] == []
    assert len(snapshot["cold"]) == 30


def test_20261119_publisher_is_after_final_order_shortlist_not_wider_risk_pool():
    """PR367 incorrectly published from ok_after_risk instead of final orders."""
    from pathlib import Path

    src = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    top3 = src.index("orderable_candidates = orderable_candidates[:3]")
    kr_finalizer = src.index("orderable_candidates, final_invariants = _finalize_kr_order_candidates(")
    publish = src.index("_priority_status = publish_pb1_priority(")
    ready = src.index("orderable_codes = [c.code for c in orderable_candidates]")
    assert top3 < kr_finalizer < publish < ready
    publisher = src[kr_finalizer:ready]
    assert "candidates=orderable_candidates" in publisher
    assert "eligible_codes=_priority_codes" in publisher
    assert "[KR][ENTRY_TIMING][PRIORITY_SCOPE]" in publisher
    assert "_priority_phase in {\"entry\", \"pm_entry\"}" in publisher
    assert "not allow_add_to_existing" in publisher

    observer = Path("scripts/kr_opening_observer.py").read_text(encoding="utf-8")
    assert "[KR][OPENING_OBSERVER][NO_COLLECTION_TARGET]" in observer
    assert 'native_priority = {**priority, "cold": []}' in observer


def test_20261119_broken_manifest_reports_precise_reason(seed_prep):
    date, _ = seed_prep
    pq.publish_pb1_priority(
        date=date, account_scope="practice:test",
        eligible_codes=["000021"], now=moment(9, 1),
    )
    expired = pq.collection_priorities(
        date=date, account_scope="practice:test", now=moment(9, 8),
    )
    assert expired["state"] == "PREP_FALLBACK"
    assert expired["reason"] == "STALE_OR_UNTRUSTED_MANIFEST"
