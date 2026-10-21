"""Exit resolver: avoid repeated account/epoch lookups, preserve owner isolation."""
from trader.us.pb1 import us_exit_position_resolver as resolver


def _position(symbol, epoch, owner="US_STANDARD"):
    return {
        "symbol": symbol, "exchange": "NASDAQ", "qty": 2,
        "entry_price": 100.0, "entry_price_source": "KIS",
        "current_price_usd": 100.0,
        "env": "practice", "account_id": "account-A",
        "trading_epoch_id": epoch, "strategy_owner": owner,
        "position_lifecycle_id": "life-" + symbol,
    }


def test_same_scope_queries_authority_only_once_per_tick(monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(
        repos, "load_us_position_risk_state_candidates_batch",
        lambda symbols, day: {name: [] for name in symbols},
    )
    counts = []
    def identity_ok(identity):
        counts.append(tuple(identity.get(k) for k in
                     ("env", "account_id", "trading_epoch_id")))
        return True
    monkeypatch.setattr(resolver, "_identity_is_current", identity_ok)

    positions = [_position("AAA", "epoch1"), _position("BBB", "epoch1"),
                 _position("CCC", "epoch2")]
    enriched, meta = resolver.enrich_us_positions_for_exit(
        positions, trade_date="2026-10-20", env="practice",
    )
    assert len(enriched) == 3
    assert meta["missing"] == 0
    assert len(counts) == 2
    assert counts == [
        ("practice", "account-A", "epoch1"),
        ("practice", "account-A", "epoch2"),
    ]
    # Not persisted across a later tick; current generation can change.
    resolver.enrich_us_positions_for_exit(
        positions, trade_date="2026-10-20", env="practice",
    )
    assert len(counts) == 4


def test_identity_cache_never_bypasses_missing_owner_or_lifecycle(monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(
        repos, "load_us_position_risk_state_candidates_batch",
        lambda symbols, day: {name: [] for name in symbols},
    )
    calls = []
    monkeypatch.setattr(
        resolver, "_identity_is_current",
        lambda identity: calls.append(identity) or True,
    )
    complete = _position("AAA", "epoch1")
    missing_owner = _position("BBB", "epoch1")
    missing_owner.pop("strategy_owner")
    complete2 = _position("CCC", "epoch1")
    enriched, _ = resolver.enrich_us_positions_for_exit(
        [complete, missing_owner, complete2],
        trade_date="2026-10-20", env="practice",
    )
    assert len(calls) == 1
    by_symbol = {p["symbol"]: p for p in enriched}
    assert by_symbol["AAA"]["position_lifecycle_view"]["identity_status"] == "IDENTIFIED"
    assert by_symbol["BBB"]["position_lifecycle_view"]["identity_status"] != "IDENTIFIED"
    assert by_symbol["CCC"]["position_lifecycle_view"]["identity_status"] == "IDENTIFIED"
