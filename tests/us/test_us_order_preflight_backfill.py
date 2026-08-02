from datetime import datetime
from zoneinfo import ZoneInfo

import pytest


@pytest.fixture(autouse=True)
def preflight_env(monkeypatch):
    monkeypatch.setenv("TRADING_REGION", "US")
    monkeypatch.setenv("US_AGENT_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_MAX_ORDER_USD", "10000")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "50000")
    monkeypatch.setenv("US_MAX_POSITIONS", "30")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "1")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {
        "effective_order_budget_usd": cash, "capital_usd_cap": max(cash, 1),
    })


def _intent(symbol, notional=100, *, key=None, **extra):
    intent = {
        "symbol": symbol, "exchange": "NASDAQ", "side": "BUY", "qty": 1,
        "limit_price": notional, "notional_usd": notional,
        "client_order_key": key or f"key-{symbol}", "trade_date": "2026-07-31",
        "position_state": "NOT_HELD", "position_action": "NEW_POSITION_BUY",
        "theme_cluster": "TECH", **extra,
    }
    intent["meta"] = {key: intent[key] for key in ("position_state", "position_action", "theme_cluster")}
    return intent


def _state(cash=10000, daily=0, count=0, keys=None):
    return {
        "available_cash_usd": cash, "daily_notional_usd": daily,
        "position_count": count, "portfolio_usd": 100000,
        "order_keys": set(keys or set()),
        "now": datetime(2026, 7, 31, 10, tzinfo=ZoneInfo("America/New_York")),
    }


def test_candidate_router_blocks_backfill_to_next_three(monkeypatch):
    from trader.us.execution.order_router import select_preflight_buy_candidates
    monkeypatch.setattr(
        "trader.us.db.repos.has_pending_order_for_symbol_side",
        lambda symbol, **kwargs: symbol == "AAPL",
    )
    intents = [
        _intent("AAPL"),
        _intent("MSFT", projected_weight=1.1),
        _intent("NVDA", key="duplicate"),
        _intent("AMZN"), _intent("META"), _intent("GOOGL"),
    ]
    accepted, rejected, diag = select_preflight_buy_candidates(
        intents, target_accept_count=3, projected_state=_state(keys={"duplicate"}),
        allowed_symbols={i["symbol"] for i in intents}, current_positions=[],
    )
    assert [i["symbol"] for i in accepted] == ["AMZN", "META", "GOOGL"]
    assert [r["reason"] for r in rejected] == [
        "pending_order_exists", "position_weight_exceeded", "duplicate_client_order_key",
    ]
    assert {key: diag[key] for key in ("global_stop_reason", "system_invariant_failure", "attempted", "accepted", "rejected", "candidate_pool_exhausted")} == {
        "global_stop_reason": "", "system_invariant_failure": "", "attempted": 6,
        "accepted": 3, "rejected": 3, "candidate_pool_exhausted": False,
    }


def test_projected_cash_is_consumed_and_later_candidate_can_backfill(monkeypatch):
    from trader.us.execution.order_router import select_preflight_buy_candidates
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: False)
    intents = [_intent("AAPL", 2000), _intent("MSFT", 4000), _intent("NVDA", 1500)]
    state = _state(cash=5000)
    accepted, rejected, diag = select_preflight_buy_candidates(
        intents, target_accept_count=3, projected_state=state,
        allowed_symbols={i["symbol"] for i in intents}, current_positions=[],
    )
    assert [i["symbol"] for i in accepted] == ["AAPL", "NVDA"]
    assert rejected[0]["reason"] in {"cash_below_buffer", "us_capital_budget_exceeded"}
    assert state["available_cash_usd"] == 1500
    assert state["daily_notional_usd"] == 3500
    assert diag["candidate_pool_exhausted"] is True


def test_expensive_candidate_does_not_globally_stop_cheaper_candidate():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    intents = [_intent("AAPL", 600), _intent("MSFT", 300)]
    accepted, rejected, _ = select_preflight_buy_candidates(
        intents, target_accept_count=1, projected_state=_state(cash=500),
        allowed_symbols={"AAPL", "MSFT"}, current_positions=[],
    )
    assert [item["symbol"] for item in accepted] == ["MSFT"]
    assert rejected[0]["scope"] == "CANDIDATE"


def test_committed_daily_buy_notional_deduplicates_statuses(monkeypatch):
    from trader.us.db.repos import load_today_committed_buy_notional
    rows = [
        {"side": "BUY", "status": "ACK", "client_order_key": "a", "committed_notional_usd": 600, "env": "practice"},
        {"side": "BUY", "status": "PENDING", "client_order_key": "a", "committed_notional_usd": 600, "env": "practice"},
        {"side": "BUY", "status": "DRY_RUN", "client_order_key": "b", "committed_notional_usd": 300, "env": "practice"},
        {"side": "SELL", "status": "ACK", "client_order_key": "c", "committed_notional_usd": 999, "env": "practice"},
    ]
    class Result:
        def mappings(self): return self
        def all(self): return rows
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args): return Result()
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: type("Engine", (), {"connect": lambda self: Connection()})())
    assert load_today_committed_buy_notional("2026-07-31") == 900


def test_filled_buy_remains_in_committed_daily_notional(monkeypatch):
    from trader.us.db.repos import load_today_committed_buy_notional

    rows = [{
        "side": "BUY", "status": "FILLED", "client_order_key": "filled-key",
        "committed_notional_usd": 600, "env": "practice",
    }]
    class Result:
        def mappings(self): return self
        def all(self): return rows
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args): return Result()
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: type("Engine", (), {"connect": lambda self: Connection()})())
    assert load_today_committed_buy_notional("2026-07-31") == 600


def test_strict_committed_notional_distinguishes_unavailable_from_authoritative_zero(monkeypatch):
    from trader.us.db.repos import load_today_committed_buy_notional_result
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: None)
    assert load_today_committed_buy_notional_result("2026-07-31").available is False

    class Result:
        def __init__(self, rows): self.rows = rows
        def mappings(self): return self
        def all(self): return self.rows
    class Connection:
        calls = 0
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args):
            self.calls += 1
            raise RuntimeError("authoritative query failed")
    connection = Connection()
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: type("Engine", (), {"connect": lambda self: connection})())
    result = load_today_committed_buy_notional_result("2026-07-31")
    assert result.available is False and connection.calls == 1

    class HealthyConnection(Connection):
        def execute(self, *args):
            self.calls += 1
            return Result([])
    healthy = HealthyConnection()
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: type("Engine", (), {"connect": lambda self: healthy})())
    result = load_today_committed_buy_notional_result("2026-07-31")
    assert result.available is True and result.notional_usd == 0 and healthy.calls == 1


@pytest.mark.parametrize(
    "row,error_fragment",
    [
        ({"side": "BUY", "status": "ACK", "client_order_key": "missing-env", "committed_notional_usd": 100}, "environment"),
        ({"side": "BUY", "status": "ACK", "client_order_key": "missing-value", "env": "practice"}, "notional"),
    ],
)
def test_strict_committed_notional_fails_closed_on_unrecoverable_legacy_risk_data(monkeypatch, row, error_fragment):
    from trader.us.db.repos import load_today_committed_buy_notional_result

    class Result:
        def mappings(self): return self
        def all(self): return [row]
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args): return Result()
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: type("Engine", (), {"connect": lambda self: Connection()})())
    result = load_today_committed_buy_notional_result("2026-07-31")
    assert result.available is False
    assert error_fragment in (result.error or "")


def test_oversized_daily_notional_candidate_backfills_smaller_candidates(monkeypatch):
    from trader.us.execution.order_router import select_preflight_buy_candidates
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "1000")
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: False)
    intents = [_intent("AAPL", 1200), _intent("MSFT", 100), _intent("NVDA", 100)]
    accepted, rejected, diag = select_preflight_buy_candidates(
        intents, target_accept_count=3, projected_state=_state(),
        allowed_symbols={i["symbol"] for i in intents}, current_positions=[],
    )
    assert [item["symbol"] for item in accepted] == ["MSFT", "NVDA"]
    assert rejected[0]["scope"] == "CANDIDATE"
    assert diag["attempted"] == 3


def test_exhausted_daily_limit_is_global_stop(monkeypatch):
    from trader.us.execution.order_router import select_preflight_buy_candidates
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "1000")
    intents = [_intent("AAPL", 100), _intent("MSFT", 100)]
    accepted, rejected, diag = select_preflight_buy_candidates(
        intents, target_accept_count=2, projected_state=_state(daily=1000),
        allowed_symbols={i["symbol"] for i in intents}, current_positions=[],
    )
    assert accepted == [] and rejected[0]["scope"] == "GLOBAL"
    assert diag["attempted"] == 1


def test_new_slots_do_not_block_add_to_existing():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    new = _intent("AAPL")
    add1 = _intent("MSFT", position_state="HELD", position_action="ADD_TO_EXISTING_BUY")
    add1["meta"].update(position_state="HELD", position_action="ADD_TO_EXISTING_BUY")
    add2 = _intent("NVDA", position_state="HELD", position_action="ADD_TO_EXISTING_BUY")
    add2["meta"].update(position_state="HELD", position_action="ADD_TO_EXISTING_BUY")
    accepted, rejected, _ = select_preflight_buy_candidates(
        [new, add1, add2], target_accept_count=3, projected_state=_state(count=30),
        allowed_symbols={"AAPL", "MSFT", "NVDA"},
        current_positions=[{"symbol": "MSFT"}, {"symbol": "NVDA"}],
        available_new_symbol_slots=0, max_new_symbol_buys=0, max_add_to_existing_buys=2,
    )
    assert [item["symbol"] for item in accepted] == ["MSFT", "NVDA"]
    assert rejected[0]["reason"] == "max_positions_reached_new_symbol"


def test_projected_cluster_cap_rejects_second_cluster_and_backfills():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    ai1 = _intent("AAPL", theme_cluster="AI_SOFTWARE")
    ai1["meta"]["theme_cluster"] = "AI_SOFTWARE"
    ai2 = _intent("MSFT", theme_cluster="AI_SOFTWARE")
    ai2["meta"]["theme_cluster"] = "AI_SOFTWARE"
    health = _intent("AMGN", theme_cluster="HEALTHCARE")
    health["meta"]["theme_cluster"] = "HEALTHCARE"
    state = _state()
    state["cluster_caps_usd"] = {"AI_SOFTWARE": 150, "HEALTHCARE": 500}
    accepted, rejected, _ = select_preflight_buy_candidates(
        [ai1, ai2, health], target_accept_count=2, projected_state=state,
        allowed_symbols={"AAPL", "MSFT", "AMGN"}, current_positions=[],
    )
    assert [item["symbol"] for item in accepted] == ["AAPL", "AMGN"]
    assert rejected[0]["reason"] == "projected_cluster_cap_exceeded"


def test_ai_combined_cap_rejects_cross_cluster_candidate_and_backfills():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    semi = _intent("NVDA", 100, theme_cluster="AI_SEMI"); semi["meta"]["theme_cluster"] = "AI_SEMI"
    software = _intent("MSFT", 100, theme_cluster="AI_SOFTWARE"); software["meta"]["theme_cluster"] = "AI_SOFTWARE"
    health = _intent("AMGN", 100, theme_cluster="HEALTHCARE"); health["meta"]["theme_cluster"] = "HEALTHCARE"
    state = _state(); state["ai_combined_cap_usd"] = 150
    accepted, rejected, _ = select_preflight_buy_candidates(
        [semi, software, health], target_accept_count=2, projected_state=state,
        allowed_symbols={"NVDA", "MSFT", "AMGN"}, current_positions=[],
    )
    assert [item["symbol"] for item in accepted] == ["NVDA", "AMGN"]
    assert rejected[0]["reason"] == "projected_ai_tech_combined_cap_exceeded"


def test_existing_cluster_exposure_is_counted_once():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    candidate = _intent("MSFT", 50, theme_cluster="AI_SOFTWARE")
    candidate["meta"]["theme_cluster"] = "AI_SOFTWARE"
    state = _state()
    state["cluster_exposure"] = {"AI_SOFTWARE": 100}
    state["cluster_exposure_start"] = {"AI_SOFTWARE": 100}
    accepted, rejected, _ = select_preflight_buy_candidates(
        [candidate], target_accept_count=1, projected_state=state,
        allowed_symbols={"MSFT"}, current_positions=[],
    )
    assert len(accepted) == 1 and rejected == []
    assert state["cluster_exposure_start"]["AI_SOFTWARE"] == 100
    assert state["cluster_exposure"]["AI_SOFTWARE"] == 150


def test_ai_cap_uses_portfolio_equity_not_available_cash():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    candidate = _intent("NVDA", 1000, theme_cluster="AI_SEMI")
    candidate["meta"]["theme_cluster"] = "AI_SEMI"
    state = _state(cash=10000)
    state["portfolio_usd"] = 100000
    state["cluster_exposure"] = {"AI_SOFTWARE": 20000}
    state["ai_combined_cap_usd"] = state["portfolio_usd"] * .35
    accepted, rejected, _ = select_preflight_buy_candidates(
        [candidate], target_accept_count=1, projected_state=state,
        allowed_symbols={"NVDA"}, current_positions=[],
    )
    assert len(accepted) == 1 and rejected == []
    assert state["ai_combined_cap_usd"] == 35000


def test_eval_amount_is_used_by_canonical_position_value_resolver():
    from trader.us.portfolio_cluster_guard import resolve_position_market_value_usd
    assert resolve_position_market_value_usd({"eval_amount_usd": 1234.5}) == 1234.5


def test_system_failure_after_accept_discards_all_prior_accepts():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    first = _intent("AAPL")
    mismatch = _intent("NVDA"); mismatch["meta"]["theme_cluster"] = "HEALTHCARE"
    third = _intent("MSFT")
    accepted, _rejected, diag = select_preflight_buy_candidates(
        [first, mismatch, third], target_accept_count=3, projected_state=_state(),
        allowed_symbols={"AAPL", "NVDA", "MSFT"}, current_positions=[],
    )
    assert accepted == []
    assert diag["system_invariant_failure"] == "ENTRY_METADATA_INVARIANT_FAIL"


def test_projected_cash_and_daily_notional_have_independent_start_values():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    state = _state(cash=10000, daily=2000)
    accepted, _, _ = select_preflight_buy_candidates(
        [_intent("AAPL", 500)], target_accept_count=1, projected_state=state,
        allowed_symbols={"AAPL"}, current_positions=[],
    )
    assert len(accepted) == 1
    assert state["available_cash_usd"] == 9500
    assert state["daily_notional_usd"] == 2500


def test_missing_classification_backfills_and_metadata_mismatch_is_system_failure():
    from trader.us.execution.order_router import select_preflight_buy_candidates
    missing = _intent("AAPL")
    missing["theme_cluster"] = missing["meta"]["theme_cluster"] = None
    good = _intent("MSFT")
    accepted, rejected, _ = select_preflight_buy_candidates(
        [missing, good], target_accept_count=1, projected_state=_state(),
        allowed_symbols={"AAPL", "MSFT"}, current_positions=[],
    )
    assert [item["symbol"] for item in accepted] == ["MSFT"]
    assert rejected[0]["reason"] == "classification_metadata_missing"

    mismatch = _intent("NVDA")
    mismatch["meta"]["theme_cluster"] = "HEALTHCARE"
    accepted, rejected, diag = select_preflight_buy_candidates(
        [mismatch, good], target_accept_count=1, projected_state=_state(),
        allowed_symbols={"NVDA", "MSFT"}, current_positions=[],
    )
    assert accepted == []
    assert rejected[0]["reason"] == "ENTRY_METADATA_INVARIANT_FAIL"
    assert diag["system_invariant_failure"] == "ENTRY_METADATA_INVARIANT_FAIL"


def test_actual_pb1_intents_pass_actual_preflight_with_projected_state(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution.order_router import select_preflight_buy_candidates
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", lambda **kwargs: False)
    monkeypatch.setattr(repos, "has_position", lambda symbol: False)
    monkeypatch.setattr(repos, "load_today_order_keys", lambda trade_date: set())
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.10")

    rows = [{
        "symbol": s, "exchange": "NASDAQ", "score": .9 - i * .01,
        "source_tags": ["final30"], "sector": "TECHNOLOGY", "industry": "SOFTWARE",
        "theme_cluster": "AI_SOFTWARE", "classification_source": "watchlist",
        "trend_score": .8, "score_final": .9 - i * .01, "rank_final30": i + 1,
        "market_state": "NORMAL", "market_regime": "RISK_ON",
    } for i, s in enumerate(("AAPL", "MSFT", "NVDA", "AMZN"))]
    class Provider:
        def get_current_price(self, symbol, exchange): return {"last": 100}
    intents = generate_entry_intents(
        None, Provider(), set(), 10000, 0, 10000,
        max_new_entries=4, watchlist_entries=rows, current_position_symbols=set(),
    )
    accepted, rejected, _ = select_preflight_buy_candidates(
        intents, target_accept_count=3, projected_state=_state(),
        allowed_symbols={row["symbol"] for row in rows}, current_positions=[],
    )
    assert len(accepted) == 3
    assert rejected == []
    for key in (
        "source_tags", "sector", "industry", "theme_cluster", "classification_source",
        "trend_score", "score_final", "rank_final30", "market_state", "market_regime",
        "position_state", "position_action",
    ):
        assert accepted[0][key] == accepted[0]["meta"][key]


def test_complete_canonical_snapshot_detects_cap_universe_and_order_key_changes():
    from trader.us.execution.order_router import (
        canonical_risk_snapshot_changed_fields,
        normalize_canonical_risk_snapshot,
    )

    state = _state(keys={"existing"})
    state.update({
        "cluster_exposure": {"AI_SOFTWARE": 100.0},
        "cluster_caps_usd": {"AI_SOFTWARE": 5000.0},
        "default_cluster_cap_usd": 4000.0,
        "ai_combined_cap_usd": 35000.0,
    })
    preflight = normalize_canonical_risk_snapshot(
        state, allowed_symbols={"AAPL", "MSFT"}, current_position_symbols={"AAPL"},
    )
    assert set(preflight) == {
        "available_cash_usd", "daily_notional_usd", "position_count",
        "portfolio_equity_usd", "order_keys", "cluster_exposure",
        "cluster_caps_usd", "default_cluster_cap_usd", "ai_combined_cap_usd",
        "now", "allowed_symbols", "current_position_symbols",
    }

    changed = dict(preflight)
    changed["ai_combined_cap_usd"] = 99.0
    assert canonical_risk_snapshot_changed_fields(preflight, changed) == ["ai_combined_cap_usd"]

    changed = dict(preflight)
    changed["allowed_symbols"] = ["AAPL"]
    changed["order_keys"] = ["different"]
    assert canonical_risk_snapshot_changed_fields(preflight, changed) == ["allowed_symbols", "order_keys"]


def test_fallback_selector_records_complete_accepted_snapshot():
    from trader.us.execution.order_router import select_preflight_buy_candidates

    state = _state()
    state.update({
        "cluster_exposure": {}, "cluster_caps_usd": {"TECH": 5000.0},
        "default_cluster_cap_usd": 5000.0, "ai_combined_cap_usd": 35000.0,
    })
    accepted, rejected, diagnostics = select_preflight_buy_candidates(
        [_intent("AAPL")], target_accept_count=1, projected_state=state,
        allowed_symbols={"AAPL"}, current_positions=[],
    )
    assert len(accepted) == 1 and rejected == []
    snapshot = diagnostics["accepted_states"]["key-AAPL"]
    assert snapshot["portfolio_equity_usd"] == 100000.0
    assert snapshot["ai_combined_cap_usd"] == 35000.0
    assert snapshot["cluster_caps_usd"] == {"TECH": 5000.0}
    assert snapshot["allowed_symbols"] == ["AAPL"]
    assert snapshot["current_position_symbols"] == []
