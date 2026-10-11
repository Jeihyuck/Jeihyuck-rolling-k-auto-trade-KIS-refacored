"""KIS-outage fallback must use verified exchange, never ticker-list position."""
from trader.universe import build


def _seed_rows(_path):
    return [
        {"code": "036930", "name": "주성엔지니어링", "market": None},
        {"code": "042700", "name": "한미반도체", "market": None},
        {"code": "039030", "name": "이오테크닉스", "market": None},
    ]


def test_verified_krx_market_allows_correct_emergency_fallback(monkeypatch):
    monkeypatch.setenv("UNIVERSE_EMERGENCY_VERIFY_KRX", "1")
    monkeypatch.setattr(build, "_load_seed_rows", _seed_rows)
    monkeypatch.setattr(build, "_load_krx_listing_market_map", lambda: {
        "036930": "KOSDAQ",
        "042700": "KOSPI",
        "039030": "KOSDAQ",
    })
    result = build._load_emergency_seed()
    assert result is not None
    assert result["params"]["market_source"] == "krx_listing_verified"
    assert [(m["code"], m["market"]) for m in result["members"]] == [
        ("036930", "KOSDAQ"),
        ("042700", "KOSPI"),
        ("039030", "KOSDAQ"),
    ]
    assert all(m["meta_json"]["market_verification"] == "krx_listing_verified" for m in result["members"])


def test_verified_krx_market_unavailable_fails_closed(monkeypatch):
    monkeypatch.setenv("UNIVERSE_EMERGENCY_VERIFY_KRX", "1")
    monkeypatch.setattr(build, "_load_seed_rows", _seed_rows)
    def fail():
        raise RuntimeError("exchange_feed_down")
    monkeypatch.setattr(build, "_load_krx_listing_market_map", fail)
    assert build._load_emergency_seed() is None


def test_partial_krx_market_coverage_is_not_guessable(monkeypatch):
    monkeypatch.setenv("UNIVERSE_EMERGENCY_VERIFY_KRX", "1")
    monkeypatch.setattr(build, "_load_seed_rows", _seed_rows)
    monkeypatch.setattr(build, "_load_krx_listing_market_map", lambda: {
        "036930": "KOSDAQ",
        "042700": "KOSPI",
    })
    assert build._load_emergency_seed() is None


def test_explicit_seed_market_conflict_with_krx_fails_closed(monkeypatch):
    monkeypatch.setenv("UNIVERSE_EMERGENCY_VERIFY_KRX", "1")
    monkeypatch.setattr(build, "_load_seed_rows", lambda _path: [
        {"code": "036930", "market": "KOSPI"},
        {"code": "042700", "market": None},
    ])
    monkeypatch.setattr(build, "_load_krx_listing_market_map", lambda: {
        "036930": "KOSDAQ",
        "042700": "KOSPI",
    })
    assert build._load_emergency_seed() is None


def test_normal_explicit_seed_never_calls_remote_market_lookup(monkeypatch):
    monkeypatch.setenv("UNIVERSE_EMERGENCY_VERIFY_KRX", "1")
    monkeypatch.setattr(build, "_load_seed_rows", lambda _path: [
        {"code": "036930", "market": "KOSDAQ"},
        {"code": "042700", "market": "KOSPI"},
    ])
    def remote_unexpected():
        raise AssertionError("extra listing fetch on already validated seed")
    monkeypatch.setattr(build, "_load_krx_listing_market_map", remote_unexpected)
    result = build._load_emergency_seed()
    assert result is not None
    assert result["params"]["market_source"] == "explicit_seed_csv"
