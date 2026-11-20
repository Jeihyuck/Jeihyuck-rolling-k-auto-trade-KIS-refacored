"""2026-11-20 KR native VWAP persistence safety regression (offline only).

All times/quotes are fixtures; no KIS POST or operating DB access.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib

from trader.kr import opening_bar_contract as opening


KST = opening.KST
T0 = datetime(2026, 11, 20, 9, 36, tzinfo=KST)
CODE = "251970"
SCOPE = "practice-fixture"


def _provenance(now: datetime, *, native_age: int = 25, receipt_age: int = 5,
                code: str = CODE, scope: str = SCOPE, source: str = "KIS_REST_INQUIRE_PRICE"):
    return {
        "env": "practice",
        "account_scope_sha256": hashlib.sha256(scope.encode("utf-8")).hexdigest(),
        "code": code,
        "trade_date": now.date().isoformat(),
        "quote_source": source,
        "source_identity": "AUTHENTICATED_REST_REQUEST_BOUND",
        "native_at": (now - timedelta(seconds=native_age)).isoformat(),
        "received_at": (now - timedelta(seconds=receipt_age)).isoformat(),
        "verified_at": now.isoformat(),
    }


def _bars():
    return [{
        "date": "20261120", "time": "093000",
        "open": 10000, "high": 10010, "low": 9990,
        "close": 10000, "volume": 100,
    }]


def _save(t, *, vwap=None, provenance=None, claims=None, code=CODE, scope=SCOPE):
    return opening.save_opening_proof(
        env="practice", account_scope=scope, code=code, now=t,
        raw_bars=_bars(),
        vwap=vwap,
        vwap_source="KIS_NATIVE_WEIGHTED_AVG" if vwap is not None else "UNKNOWN",
        vwap_provenance=provenance, vwap_quote_claims=claims,
    )


def _load(t, *, code=CODE, scope=SCOPE):
    return opening.load_opening_proof(
        env="practice", account_scope=scope, code=code, now=t,
        max_age_seconds=None,
    )


def test_verified_vwap_survives_short_quote_dropout_without_refreshing_native_ttl(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path", lambda *parts: tmp_path.joinpath(*parts))
    attest = _provenance(T0)
    _save(T0, vwap=9990, provenance=attest)
    after = T0 + timedelta(seconds=30)
    _save(after, claims={})  # absent quote is not a false opposite identity
    saved = _load(after)
    assert saved["vwap"] == 9990
    assert saved["vwap_source"] == "KIS_NATIVE_WEIGHTED_AVG"
    assert saved["vwap_provenance"] == attest
    # An unrelated minute proof rewrite must not renew old VWAP.
    after_expiry = T0 + timedelta(seconds=121)
    _save(after_expiry, claims={})
    expired = _load(after_expiry)
    assert expired["vwap"] is None
    assert expired["vwap_source"] == "UNKNOWN"
    assert expired["vwap_provenance"] is None


def test_explicit_quote_scope_contradiction_rejects_cache_even_within_ttl(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path", lambda *parts: tmp_path.joinpath(*parts))
    _save(T0, vwap=9990, provenance=_provenance(T0))
    after = T0 + timedelta(seconds=20)
    _save(after, claims={"requested_symbol": "093320", "trade_date": "20261120"})
    assert _load(after)["vwap"] is None


def test_explicit_wrong_trading_day_rejects_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path", lambda *parts: tmp_path.joinpath(*parts))
    _save(T0, vwap=9990, provenance=_provenance(T0))
    after = T0 + timedelta(seconds=25)
    _save(after, claims={"trade_date": "20261119"})
    assert _load(after)["vwap_source"] == "UNKNOWN"


def test_untrusted_or_wrong_scope_vwap_attestation_is_not_accepted(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path", lambda *parts: tmp_path.joinpath(*parts))
    forged = _provenance(T0, scope="different-account")
    _save(T0, vwap=9990, provenance=forged)
    stored = _load(T0)
    assert stored["vwap"] is None
    assert stored["vwap_source"] == "UNKNOWN"
    stale = _provenance(T0, native_age=140)
    _save(T0 + timedelta(seconds=1), vwap=9990, provenance=stale)
    assert _load(T0 + timedelta(seconds=1))["vwap"] is None


def test_legacy_unattested_value_cannot_gain_quote_lifetime_through_bar_refresh(tmp_path, monkeypatch):
    monkeypatch.setattr(opening, "runtime_path", lambda *parts: tmp_path.joinpath(*parts))
    # Original API signature is supported for existing callers/tests, but a
    # missing native receipt cannot be promoted to a reusable cache attestation.
    _save(T0, vwap=9990)
    _save(T0 + timedelta(seconds=1), claims={})
    assert _load(T0 + timedelta(seconds=1))["vwap"] is None
