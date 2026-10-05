from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def test_sep30_legacy_tick_budget_upgrade_preserves_execution_tail(monkeypatch):
    from trader.us.runner.trade_tick_runner import budgeted_entry_timeout_sec

    class Ctx:
        def __init__(self, remaining: float):
            self.remaining = remaining

        def remaining_sec(self) -> float:
            return self.remaining

    monkeypatch.setenv("US_EXECUTION_TAIL_RESERVE_SEC", "70")

    # Sep-30 completed ticks reached entry with 0~51s left under the 240s
    # watchdog. The AM/afternoon legacy upgrade adds 120s without changing
    # the 70s broker/execution tail reserve or the 120s configured entry cap.
    assert budgeted_entry_timeout_sec(Ctx(120.0), 120.0) == 50.0
    assert budgeted_entry_timeout_sec(Ctx(171.0), 120.0) == 101.0


def test_wsl_am_afternoon_upgrade_only_complete_legacy_240_pair():
    joint_guard = 'if [[ "${US_TICK_TIMEOUT_SEC}" == "240" && "${US_TICK_TIMEOUT_MIN_SEC}" == "240" ]]; then'
    for path in (Path("scripts/wsl/run-us-am.sh"), Path("scripts/wsl/run-us-afternoon.sh")):
        text = path.read_text(encoding="utf-8")
        # Preserve the historical default token required by older deployment
        # contracts, then migrate only when both effective values are legacy.
        assert 'US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-240}"' in text
        assert 'US_TICK_TIMEOUT_MIN_SEC="${US_TICK_TIMEOUT_MIN_SEC:-240}"' in text
        assert joint_guard in text
        assert 'export US_TICK_TIMEOUT_SEC="360"' in text
        assert 'export US_TICK_TIMEOUT_MIN_SEC="360"' in text
        # Do not weaken the entry timeout or execution-tail policy in wrappers.
        assert 'US_ENTRY_EVAL_TIMEOUT_SEC="${US_ENTRY_EVAL_TIMEOUT_SEC:-120}"' in text

    close = Path("scripts/wsl/run-us-close.sh").read_text(encoding="utf-8")
    assert 'US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-240}"' in close
    assert 'US_TICK_TIMEOUT_MIN_SEC="${US_TICK_TIMEOUT_MIN_SEC:-240}"' in close
    assert joint_guard not in close


def test_cross_day_lifecycle_time_beats_same_day_position_row_created_at(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1 import us_exit_position_resolver as resolver
    from trader.us.pb1.us_exit_router import _min_hold_elapsed

    opened_at = "2026-09-22T14:00:00+00:00"
    identity = {
        "env": "practice",
        "account_id": "practice:test-account",
        "trading_epoch_id": "epoch-cross-day",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "life-cross-day",
    }
    monkeypatch.setattr(resolver, "_identity_is_current", lambda value: value == identity)
    monkeypatch.setattr(
        repos,
        "load_us_position_risk_state_candidates",
        lambda symbol, trade_date: [{
            "trade_date": "2026-09-22",
            "trading_epoch_id": identity["trading_epoch_id"],
            "state": {
                "lifecycle": {
                    "lifecycle_id": identity["position_lifecycle_id"],
                    "is_open": True,
                    "opened_trade_date": "2026-09-22",
                    "opened_at": opened_at,
                    "opened_at_source": "confirmed_buy_fill",
                    "holding_trade_days": 7,
                    **identity,
                },
                "high_watermark": 997.735,
            },
            **identity,
        }],
    )

    rows, meta = resolver.enrich_us_positions_for_exit(
        [
            {
                "symbol": "SAMPLE",
                "qty": 1,
                # Reconcile-skip DB rows already carry a resolved price/source;
                # this used to trigger the early return before lifecycle timing.
                "entry_price": 949.55,
                "entry_price_source": "us_positions_avg_cost",
                **identity,
                "avg_cost": 949.55,
                "current_px": 969.0483,
                # This is the persisted row creation/update lineage, not the BUY.
                "created_at": "2026-09-30T13:32:00+00:00",
                "min_hold_minutes": 390,
            }
        ],
        trade_date="2026-09-30",
    )

    assert meta["ok"] == 1
    position = rows[0]
    assert position["entry_price_source"] == "us_positions_avg_cost"
    assert position["opened_at"] == opened_at
    assert position["entry_time"] == opened_at
    assert position["entry_time_source"] == "confirmed_buy_fill"

    elapsed, held_minutes, required = _min_hold_elapsed(
        position,
        datetime(2026, 9, 30, 13, 36, tzinfo=timezone.utc),
    )
    assert required == 390
    assert held_minutes > 390
    assert elapsed is True


def test_swing_min_hold_never_uses_position_row_created_at():
    from trader.us.pb1.us_exit_router import _min_hold_elapsed

    elapsed, held_minutes, required = _min_hold_elapsed(
        {
            "symbol": "SAMPLE",
            "created_at": "2026-09-30T13:32:00+00:00",
            "min_hold_minutes": 30,
        },
        datetime(2026, 9, 30, 14, 32, tzinfo=timezone.utc),
    )

    assert (elapsed, held_minutes, required) == (False, 0, 30)


def test_reentry_lifecycle_does_not_inherit_prior_symbol_history(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1 import us_exit_position_resolver as resolver

    identity = {
        "env": "practice",
        "account_id": "practice:test-account",
        "trading_epoch_id": "epoch-current",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "lifecycle-b",
    }
    prior = {
        "lifecycle_id": "lifecycle-a",
        "env": "practice",
        "account_id": "practice:test-account",
        "trading_epoch_id": "epoch-current",
        "strategy_owner": "US_STANDARD",
        "is_open": False,
        "opened_at": "2026-09-01T14:00:00+00:00",
        "opened_at_source": "confirmed_buy_fill",
        "high_watermark": 250.0,
        "entry_policy": {
            "book": "OLD_BOOK",
            "entry_exit_contract_version": "old-version",
        },
    }
    monkeypatch.setattr(resolver, "_identity_is_current", lambda value: value == identity)
    monkeypatch.setattr(
        repos,
        "load_us_position_risk_state_candidates",
        lambda symbol, trade_date: [{
            "trade_date": trade_date,
            "trading_epoch_id": identity["trading_epoch_id"],
            "state": {"lifecycle": prior},
            **identity,
        }],
    )
    monkeypatch.setattr(
        repos,
        "load_us_position_history_candidates",
        lambda symbol, as_of=None: [{
                "symbol": "SAMPLE",
                "as_of": as_of,
                "trading_epoch_id": identity["trading_epoch_id"],
                "avg_cost": 10.0,
                "meta": {**{k: v for k, v in identity.items() if k != "position_lifecycle_id"},
                         "strategy_owner": "US_STANDARD",
                         "position_lifecycle_id": "lifecycle-a"},
            }],
    )
    monkeypatch.setattr(
        repos,
        "load_us_buy_fill_history_candidates",
        lambda symbol, trade_date=None: [{
                "symbol": "SAMPLE",
                "trade_date": trade_date,
                "trading_epoch_id": identity["trading_epoch_id"],
                "price_usd": 10.0,
                "meta": {**{k: v for k, v in identity.items() if k != "position_lifecycle_id"},
                         "strategy_owner": "US_STANDARD",
                         "position_lifecycle_id": "lifecycle-a"},
            }],
    )

    positions, meta = resolver.enrich_us_positions_for_exit(
        [{"symbol": "SAMPLE", "qty": 2, "current_price_usd": 12.0, **identity}],
        trade_date="2026-09-30",
        env="practice",
    )

    assert meta["missing"] == 1
    assert positions[0]["pnl_input_ok"] is False
    assert positions[0]["position_lifecycle_id"] == "lifecycle-b"
    assert "opened_at" not in positions[0]
    assert positions[0]["high_watermark"] != prior["high_watermark"]


def test_resolver_selects_exact_older_lifecycle_ahead_of_newer_symbol_row(monkeypatch):
    from trader.us.db import repos
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    from trader.us.pb1 import us_exit_position_resolver as resolver

    identity = {
        "env": "practice",
        "account_id": "practice:acct-a",
        "trading_epoch_id": "epoch-shared",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "life-a",
    }
    monkeypatch.setattr(resolver, "_identity_is_current", lambda _identity: True)
    contract_a = build_us_entry_exit_contract({
        "symbol": "SAMPLE", "strategy_owner": "US_STANDARD",
        "book": "BOOK_A", "horizon": "HORIZON_A", "exit_policy": "EXIT_A",
    })
    contract_b = build_us_entry_exit_contract({
        "symbol": "SAMPLE", "strategy_owner": "US_STANDARD",
        "book": "BOOK_B", "horizon": "HORIZON_B", "exit_policy": "EXIT_B",
    })
    life_a = {
        **{key: value for key, value in identity.items() if key != "position_lifecycle_id"},
        "lifecycle_id": "life-a",
        "is_open": True,
        "opened_at": "2026-09-01T14:00:00+00:00",
        "opened_trade_date": "2026-09-01",
        "opened_at_source": "confirmed_buy_fill",
        "high_watermark": 125.0,
        "entry_policy": {
            "entry_exit_contract": contract_a,
            "entry_exit_contract_sha256": contract_a["sha256"],
            "entry_exit_contract_version": contract_a["version"],
        },
    }
    life_b = {
        **{key: value for key, value in identity.items() if key != "position_lifecycle_id"},
        "lifecycle_id": "life-b",
        "is_open": True,
        "opened_at": "2026-09-29T14:00:00+00:00",
        "opened_trade_date": "2026-09-29",
        "opened_at_source": "confirmed_buy_fill",
        "high_watermark": 500.0,
        "entry_policy": {
            "entry_exit_contract": contract_b,
            "entry_exit_contract_sha256": contract_b["sha256"],
            "entry_exit_contract_version": contract_b["version"],
        },
    }
    monkeypatch.setattr(repos, "load_us_position_history_candidates", lambda *_a, **_kw: [
        {"symbol": "SAMPLE", "as_of": "2026-09-30", "trading_epoch_id": "epoch-shared",
         "avg_cost": 999.0, "meta": {**life_b, "position_lifecycle_id": "life-b"}},
        {"symbol": "SAMPLE", "as_of": "2026-09-02", "trading_epoch_id": "epoch-shared",
         "avg_cost": 100.0, "meta": {**life_a, "position_lifecycle_id": "life-a"}},
    ])
    monkeypatch.setattr(repos, "load_us_buy_fill_history_candidates", lambda *_a, **_kw: [
        {"symbol": "SAMPLE", "trade_date": "2026-09-29", "trading_epoch_id": "epoch-shared",
         "price_usd": 999.0, "meta": {**life_b, "position_lifecycle_id": "life-b"}},
        {"symbol": "SAMPLE", "trade_date": "2026-09-01", "trading_epoch_id": "epoch-shared",
         "price_usd": 100.0, "meta": {**life_a, "position_lifecycle_id": "life-a"}},
    ])
    monkeypatch.setattr(repos, "load_us_position_risk_state_candidates", lambda *_a, **_kw: [
        {"trade_date": "2026-09-29", "trading_epoch_id": "epoch-shared",
         "state": {"lifecycle": life_b}},
        {"trade_date": "2026-09-02", "trading_epoch_id": "epoch-shared",
         "state": {"lifecycle": life_a}},
    ])

    positions, _ = resolver.enrich_us_positions_for_exit(
        [{"symbol": "SAMPLE", "qty": 2, "current_price_usd": 110.0, **identity}],
        trade_date="2026-09-30",
        env="practice",
    )
    resolved = positions[0]
    assert resolved["entry_price"] == 100.0
    assert resolved["entry_price_source"] == "us_positions_avg_cost"
    assert resolved["opened_at"] == life_a["opened_at"]
    assert resolved["high_watermark"] == 125.0
    assert resolved["entry_exit_contract_sha256"] == contract_a["sha256"]
    assert resolved["entry_exit_contract_version"] == contract_a["version"]


def test_resolver_rejects_exact_lifecycle_history_missing_account_identity(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1 import us_exit_position_resolver as resolver

    identity = {
        "env": "practice",
        "account_id": "practice:acct-a",
        "trading_epoch_id": "epoch-a",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "life-a",
    }
    monkeypatch.setattr(resolver, "_identity_is_current", lambda _identity: True)
    row_without_account = {
        "symbol": "SAMPLE", "as_of": "2026-09-30", "trade_date": "2026-09-30",
        "trading_epoch_id": "epoch-a", "avg_cost": 100.0, "price_usd": 100.0,
        "meta": {"env": "practice", "trading_epoch_id": "epoch-a",
                 "strategy_owner": "US_STANDARD", "position_lifecycle_id": "life-a"},
    }
    monkeypatch.setattr(repos, "load_us_position_history_candidates", lambda *_a, **_kw: [row_without_account])
    monkeypatch.setattr(repos, "load_us_buy_fill_history_candidates", lambda *_a, **_kw: [row_without_account])
    monkeypatch.setattr(repos, "load_us_position_risk_state_candidates", lambda *_a, **_kw: [{
        "trade_date": "2026-09-30", "trading_epoch_id": "epoch-a",
        "state": {"lifecycle": {
            "lifecycle_id": "life-a", "env": "practice",
            "trading_epoch_id": "epoch-a", "strategy_owner": "US_STANDARD",
            "opened_at": "2026-09-01T14:00:00+00:00",
            "high_watermark": 125.0,
        }},
    }])

    positions, meta = resolver.enrich_us_positions_for_exit(
        [{"symbol": "SAMPLE", "qty": 2, "current_price_usd": 110.0, **identity}],
        trade_date="2026-09-30",
        env="practice",
    )
    assert meta["missing"] == 1
    assert positions[0]["pnl_input_ok"] is False
    assert "opened_at" not in positions[0]
    assert positions[0]["high_watermark"] != 125.0


def test_reconcile_skip_surfaces_old_epoch_position_without_rebinding(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1 import us_exit_position_resolver as resolver

    repos.reset_memory_stores()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_a, **_kw: "epoch-current")
    monkeypatch.setattr(resolver, "_identity_is_current", lambda _identity: False)
    repos._MEM_POSITIONS.append({
        "symbol": "SAMPLE", "as_of": "2026-09-30", "qty": 4, "avg_cost": 100.0,
        "current_px": 95.0, "trading_epoch_id": "epoch-previous",
        "env": "practice", "account_id": "practice:acct-a",
        "strategy_owner": "US_STANDARD", "position_lifecycle_id": "life-a",
        "meta": {
            "env": "practice", "account_id": "practice:acct-a",
            "trading_epoch_id": "epoch-previous", "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": "life-a", "orderable_qty": 4,
        },
    })

    loaded = repos.load_positions("2026-09-30", include_epoch_mismatches=True)
    assert len(loaded) == 1
    assert loaded[0]["sell_management_degraded"] is True
    assert loaded[0]["trading_epoch_id"] == "epoch-previous"
    enriched, summary = resolver.enrich_us_positions_for_exit(
        loaded, trade_date="2026-09-30", env="practice",
    )
    assert len(enriched) == 1 and enriched[0]["qty"] == 4
    assert enriched[0]["trading_epoch_id"] == "epoch-previous"
    assert enriched[0]["position_lifecycle_view"]["integrity"]["broker_held_position_hidden_by_epoch"]
    assert summary["integrity"]["broker_held_position_hidden_by_epoch"] == 1
