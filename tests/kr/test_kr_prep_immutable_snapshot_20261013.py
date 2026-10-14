"""2026-10-13 regression: 300-symbol dated universe and immutable PREP SUCCESS."""
from __future__ import annotations

import pytest

from trader.universe import build


def _rows(market, count=150, *, repeated=None):
    rows = []
    for i in range(count):
        code = repeated if repeated and i == 0 else f"{100000 + (0 if market == 'KOSPI' else 200000) + i:06d}"
        rows.append({
            "code": code, "market": market, "name": "SAMPLE",
            "market_cap": 100000000 - i * 100,
            "actual_as_of": "2026-10-12",
            "source_published_on": "2026-10-13",
            "source_sha256": "a" * 64,
        })
    return rows


def test_prep_kis_master_150_plus_150_preserves_candidate_policy():
    class Provider:
        def get_marketcap_top_with_meta(self, market, as_of_date, n):
            assert as_of_date == "2026-10-12"
            assert n == 150
            return _rows(market)
    result, reason = build._build_from_kis_master(Provider(), "2026-10-12")
    assert reason == "kis_master_marketcap"
    assert result["source"] == "kis_master_marketcap"
    assert result["params"]["actual_as_of"] == "2026-10-12"
    assert len(result["members"]) == 300
    assert len({x["code"] for x in result["members"]}) == 300
    assert len(result["payload"]["selected_by_market"]["KOSPI"]) == 150
    assert len(result["payload"]["selected_by_market"]["KOSDAQ"]) == 150


def test_prep_cross_market_identity_conflict_fails_closed():
    class Provider:
        def get_marketcap_top_with_meta(self, market, *_a):
            return _rows(market, repeated="005930")
    result, reason = build._build_from_kis_master(Provider(), "2026-10-12")
    assert result is None
    assert reason == "kis_master_cross_market_duplicate"


def test_prep_prior_success_is_not_reset_by_force_rebuild(monkeypatch):
    as_of = "2026-10-12"
    saved = [
        {"code": f"{i:06d}", "market": "KOSPI" if i < 60 else "KOSDAQ", "rank": i}
        for i in range(1, 151)
    ]
    class Repo:
        def __init__(self, _engine):
            pass
        def get_latest_successful_universe_snapshot(self, **_kwargs):
            return {
                "run_id": "previous-success", "as_of": as_of,
                "actual_as_of": as_of, "provider": "kis_master_marketcap",
                "status": "SUCCESS", "members": saved,
                "build_reason": build._kis_master_proof(
                    as_of, "2026-10-13", {"KOSPI": "a" * 64, "KOSDAQ": "b" * 64}
                ),
            }
        def start_universe_run(self, **_kw):
            raise AssertionError("must not change SUCCESS to RUNNING")
    monkeypatch.setattr(build, "make_engine", lambda: object())
    monkeypatch.setattr(build, "run_migrations", lambda _engine: None)
    monkeypatch.setattr(build, "UniverseRepo", Repo)
    monkeypatch.setenv("UNIVERSE_PREP_VERIFIED_DB_FALLBACK", "1")
    monkeypatch.delenv("UNIVERSE_NO_FALLBACK", raising=False)
    monkeypatch.setenv("UNIVERSE_FORCE_REBUILD_ALWAYS", "1")
    assert len(build.build_universe(as_of, "practice", "best_k_meta", prep_recovery=True)) == 150


def test_successful_kis_master_build_persists_two_zip_hashes(monkeypatch):
    """DB must retain dated file provenance after the input records are discarded."""
    as_of = "2026-10-12"
    digest_pi, digest_dq = "a" * 64, "b" * 64
    stored = []

    class Provider:
        def get_marketcap_top_with_meta(self, market, as_of_date, n):
            assert as_of_date == as_of and n == 150
            result = _rows(market)
            checksum = digest_pi if market == "KOSPI" else digest_dq
            return [{**row, "source_sha256": checksum} for row in result]

    class Repo:
        def __init__(self, _engine):
            pass

        def get_latest_successful_universe_snapshot(self, **_kwargs):
            return None

        def start_universe_run(self, **kwargs):
            assert kwargs["provider"] == "kis_master_marketcap"
            return "KIS-dated-run"

        def store_universe_snapshot(self, **kwargs):
            stored.append(kwargs)

        def cleanup_old_runs(self, **kwargs):
            pass

    monkeypatch.setattr(build, "make_engine", lambda: object())
    monkeypatch.setattr(build, "run_migrations", lambda *_a, **_kw: None)
    monkeypatch.setattr(build, "UniverseRepo", Repo)
    monkeypatch.setattr(build, "KISMasterMarketcapProvider", Provider)
    monkeypatch.setattr(build, "_write_universe_sanitize_report", lambda **kw: None)
    monkeypatch.setattr(build, "_write_universe_drop_diagnostics", lambda **kw: None)
    monkeypatch.setenv("UNIVERSE_VALIDATE_OHLCV", "0")
    monkeypatch.setenv("UNIVERSE_VALIDATE_WITH_KIS", "0")
    monkeypatch.setenv("UNIVERSE_NO_FALLBACK", "1")
    members = build.build_universe(as_of, "practice", "best_k_meta", prep_recovery=True)
    assert len(members) == 300
    assert len(stored) == 1
    proof = stored[0]["build_reason"]
    assert proof == build._kis_master_proof(
        as_of, "2026-10-13", {"KOSPI": digest_pi, "KOSDAQ": digest_dq}
    )
    assert build._kis_master_proof_matches(
        {"build_reason": proof}, as_of
    )
    assert stored[0]["actual_as_of"] == as_of
    assert stored[0]["provider"] == "kis_master_marketcap"
    assert len(stored[0]["members"]) == 300


def test_prep_rejects_undated_kis_provider_override():
    from unittest.mock import patch
    with patch.object(build, "make_engine", return_value=object()), patch.object(build, "run_migrations"):
        with pytest.raises(ValueError, match="verified KIS morning master"):
            build.build_universe(
                "2026-10-12", "practice", "best_k_meta",
                provider_override="kis_marketcap_top", prep_recovery=True,
            )
