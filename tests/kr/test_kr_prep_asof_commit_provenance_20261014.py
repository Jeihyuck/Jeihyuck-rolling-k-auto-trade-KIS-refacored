"""KR PREP: a correct date label alone must never authorize a malformed universe.

Offline; no broker calls, production database writes or public market data calls.
"""
from __future__ import annotations

from datetime import date

import pytest

from trader import prep_runner
from trader.universe.build import _kis_master_proof


AS_OF = "2026-10-13"


def members(count=150):
    return [
        {"code": f"{i:06d}", "market": "KOSPI" if i <= 75 else "KOSDAQ", "rank": i}
        for i in range(1, count + 1)
    ]


def snapshot(rows, *, actual_as_of=AS_OF, provider="kis_master_marketcap"):
    return {
        "status": "SUCCESS",
        "as_of": AS_OF,
        "actual_as_of": actual_as_of,
        "provider": provider,
        "run_id": "committed-kis-master-universe",
        "build_reason": _kis_master_proof(
            AS_OF, "2026-10-14", {"KOSPI": "a" * 64, "KOSDAQ": "b" * 64}
        ),
        "members": list(rows),
    }


class FakeRepo:
    def __init__(self, initial, committed):
        self.rows = list(initial)
        self.committed = committed

    def get_universe_members(self, **kwargs):
        assert kwargs["as_of_date"] == AS_OF
        assert kwargs["allow_fallback"] is False
        return list(self.rows)

    def get_latest_successful_universe_snapshot(self, **kwargs):
        assert kwargs["as_of_date"] == AS_OF
        return self.committed


def _install(monkeypatch, repo, build):
    monkeypatch.setattr(prep_runner, "UniverseRepo", lambda _engine: repo)
    monkeypatch.setattr(prep_runner, "build_universe", build)
    monkeypatch.setenv("UNIVERSE_BUILD_IF_MISSING", "1")
    monkeypatch.setenv("UNIVERSE_FORCE_REBUILD_ALWAYS", "0")
    monkeypatch.setenv("UNIVERSE_NO_FALLBACK", "1")


def test_same_count_but_different_members_does_not_reuse_stale_snapshot(monkeypatch):
    correct = members()
    wrong = [*correct[:-1], {"code": "987654", "market": "KOSDAQ", "rank": 150}]
    repo = FakeRepo(wrong, snapshot(correct))
    rebuild_called = []

    def rebuild(**kwargs):
        rebuild_called.append(kwargs)
        repo.rows = list(correct)
        repo.committed = snapshot(correct)
        return list(correct)

    _install(monkeypatch, repo, rebuild)
    result = prep_runner._ensure_universe(
        engine=object(), env="practice", strategy="best_k_meta",
        as_of=date(2026, 10, 13),
    )
    assert len(result) == 150
    assert len(rebuild_called) == 1


def test_duplicate_in_existing_same_day_snapshot_forces_verified_rebuild(monkeypatch):
    good = members()
    corrupt = [*good[:-1], good[0]]
    repo = FakeRepo(corrupt, snapshot(corrupt))
    calls = []

    def rebuild(**kwargs):
        calls.append(kwargs)
        repo.rows = list(good)
        repo.committed = snapshot(good)
        return list(good)

    _install(monkeypatch, repo, rebuild)
    result = prep_runner._ensure_universe(
        engine=object(), env="practice", strategy="best_k_meta", as_of=date(2026, 10, 13)
    )
    assert len(result) == 150
    assert calls == [{
        "as_of_date": AS_OF, "env": "practice", "strategy": "best_k_meta",
        "prep_recovery": True,
    }]


@pytest.mark.parametrize("actual_as_of,provider", [
    ("2026-10-12", "kis_master_marketcap"),
    ("2026-10-13", "kis_marketcap_top"),
])
def test_post_build_invalid_committed_provenance_fails_closed(monkeypatch, actual_as_of, provider):
    good = members()
    repo = FakeRepo([], snapshot(good, actual_as_of=actual_as_of, provider=provider))

    def rebuild(**kwargs):
        assert kwargs["prep_recovery"] is True
        repo.rows = list(good)
        return list(good)

    _install(monkeypatch, repo, rebuild)
    with pytest.raises(RuntimeError, match="KR_PREP_UNIVERSE_PROVENANCE_INVALID"):
        prep_runner._ensure_universe(
            engine=object(), env="practice", strategy="best_k_meta", as_of=date(2026, 10, 13)
        )


def test_existing_exact_dated_verified_snapshot_does_not_rebuild(monkeypatch):
    good = members()
    repo = FakeRepo(good, snapshot(good))
    _install(monkeypatch, repo, lambda **kw: pytest.fail("must reuse valid snapshot"))
    result = prep_runner._ensure_universe(
        engine=object(), env="practice", strategy="best_k_meta", as_of=date(2026, 10, 13)
    )
    assert len(result) == 150
