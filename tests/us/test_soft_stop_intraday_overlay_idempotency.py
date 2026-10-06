def test_soft_stop_repeat_is_blocked_for_general_pb1():
    from trader.us.pb1.us_exit_engine import soft_stop_repeat_allowed

    allowed, reason = soft_stop_repeat_allowed({
        "symbol": "TEST", "entry_price": 100.0, "current_price": 94.0,
        "soft_stop_triggered_today": True, "first_soft_stop_price": 94.0,
    })

    assert allowed is False
    assert reason == "SOFT_STOP_REPEAT_BLOCKED"


def test_soft_stop_repeat_allows_risk_escalation_and_tqqq_bypass():
    from trader.us.pb1.us_exit_engine import soft_stop_repeat_allowed

    allowed, reason = soft_stop_repeat_allowed({
        "symbol": "TEST", "entry_price": 100.0, "current_price": 91.0,
        "soft_stop_triggered_today": True, "first_soft_stop_price": 94.0,
    })
    assert allowed is True
    assert reason == "SOFT_STOP_REPEAT_ALLOWED_BY_RISK_ESCALATION"

    allowed, reason = soft_stop_repeat_allowed({
        "symbol": "TQQQ", "soft_stop_triggered_today": True,
    })
    assert allowed is True
    assert reason == "TQQQ_INFINITE_OVERLAY_BYPASS"


def test_confirmed_soft_stop_fill_state_reaches_repeat_gate(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_exit_engine import prepare_exit_position_snapshots, soft_stop_repeat_allowed
    from trader.us.runner.trade_tick_runner import _mark_soft_stop_stages_from_records

    store = {
        "trade_date": "2026-09-08", "symbol": "ABBV",
        "soft_stop_breach_count": 2,
        "state": {"lifecycle": {"lifecycle_id": "life-abbv", "is_open": True}},
    }

    def load_state(symbol, trade_date):
        return dict(store)

    def save_state(symbol, trade_date, state):
        store.clear()
        store.update(state)

    monkeypatch.setattr(repos, "load_us_position_risk_state", load_state)
    monkeypatch.setattr(repos, "save_us_position_risk_state", save_state)

    _mark_soft_stop_stages_from_records([{
        "symbol": "ABBV", "side": "SELL", "qty": 6, "price": 251.0,
        "order_no": "57", "client_order_key": "abbv-soft",
        "meta": {
            "exit_reason": "soft_stop_loss",
            "position_lifecycle_id": "life-abbv",
        },
    }], trade_date="2026-09-08", status="FILLED")

    soft_state = store["state"]["soft_stop_execution"]
    assert soft_state["soft_stop_triggered_today"] is True
    assert soft_state["soft_stop_partial_done"] is True
    assert soft_state["first_soft_stop_price"] == 251.0

    monkeypatch.setattr(
        repos,
        "update_us_soft_stop_risk_state",
        lambda **kwargs: {
            **store,
            "soft_stop_breach_count": 3,
        },
    )
    monkeypatch.setattr(
        "trader.us.position_lifecycle_state.update_us_position_high_watermark",
        lambda **kwargs: {"high_watermark": 264.91, "lifecycle_id": "life-abbv"},
    )

    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": 251.0}

    snapshots = prepare_exit_position_snapshots([{
        "symbol": "ABBV", "exchange": "NYSE", "qty": 7, "holding_qty": 7,
        "orderable_qty": 7, "entry_price": 264.91,
        "position_lifecycle_id": "life-abbv",
    }], Provider())
    assert snapshots[0]["soft_stop_triggered_today"] is True
    allowed, reason = soft_stop_repeat_allowed(snapshots[0])
    assert allowed is False
    assert reason == "SOFT_STOP_REPEAT_BLOCKED"

def _oct5_persistent_snapshot(*, symbol, entry_price, current_price, qty, first_fill_qty,
                              first_soft_stop_price, lifecycle_id, breach_count=3):
    return {
        "symbol": symbol,
        "exchange": "NYSE",
        "strategy_owner": "US_STANDARD",
        "qty": qty,
        "holding_qty": qty,
        "orderable_qty": qty,
        "entry_price": entry_price,
        "current_price": current_price,
        "current_price_usd": current_price,
        "resolved_current_price": current_price,
        "position_lifecycle_id": lifecycle_id,
        "soft_stop_breach_count": breach_count,
        "soft_stop_triggered_today": True,
        "soft_stop_partial_done": True,
        "first_soft_stop_price": first_soft_stop_price,
        "risk_state": {
            "trade_date": "2026-10-05",
            "soft_stop_breach_count": breach_count,
            "state": {
                "lifecycle": {
                    "lifecycle_id": lifecycle_id,
                    "is_open": True,
                    "opened_trade_date": "2026-09-25",
                    "opened_at": "2026-09-25T00:14:30+00:00",
                    "strategy_owner": "US_STANDARD",
                    "sleeve_id": "US_STANDARD",
                },
                "soft_stop_execution": {
                    "soft_stop_triggered_today": True,
                    "soft_stop_partial_done": True,
                    "first_soft_stop_filled_qty": first_fill_qty,
                    "first_soft_stop_price": first_soft_stop_price,
                    "first_soft_stop_at": "2026-10-05T14:10:24+00:00",
                    "client_order_key": f"soft-{symbol.lower()}",
                    "order_no": "1519",
                    "position_lifecycle_id": lifecycle_id,
                },
            },
        },
    }


def test_oct5_confirmed_persistent_soft_stop_full_exit_survives_general_repeat_gate(monkeypatch):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from trader.us.pb1 import us_exit_engine

    monkeypatch.setattr(
        us_exit_engine,
        "should_skip_exit_due_to_pending_sell",
        lambda *_args, **_kwargs: (False, None, None),
    )

    incidents = [
        _oct5_persistent_snapshot(
            symbol="JNJ",
            entry_price=270.65,
            current_price=253.75,
            qty=1,
            first_fill_qty=1,
            first_soft_stop_price=255.13,
            lifecycle_id="life-jnj-oct5",
            breach_count=67,
        ),
        _oct5_persistent_snapshot(
            symbol="MRK",
            entry_price=148.287,
            current_price=139.95,
            qty=6,
            first_fill_qty=6,
            first_soft_stop_price=139.66,
            lifecycle_id="life-mrk-oct5",
            breach_count=60,
        ),
    ]

    for snapshot in incidents:
        # These prices intentionally do not satisfy the old generic repeat
        # risk-escalation thresholds. The persistent contract itself must carry
        # the already-confirmed partial stop into the full-exit leg.
        allowed, reason = us_exit_engine.soft_stop_repeat_allowed(snapshot)
        assert allowed is False
        assert reason == "SOFT_STOP_REPEAT_BLOCKED"

        intents = us_exit_engine._evaluate_exit_intents_from_snapshots(
            [snapshot],
            now=datetime(2026, 10, 5, 14, 30, tzinfo=ZoneInfo("America/New_York")),
        )

        assert len(intents) == 1
        assert intents[0]["symbol"] == snapshot["symbol"]
        assert intents[0]["exit_type"] == "persistent_soft_stop_full_exit"
        assert intents[0]["qty"] == snapshot["orderable_qty"]
        assert intents[0]["position_lifecycle_id"] == snapshot["position_lifecycle_id"]


def test_persistent_soft_stop_requires_confirmed_fill_for_current_lifecycle():
    from trader.us.pb1.us_exit_engine import (
        _confirmed_soft_stop_partial_fill_for_current_lifecycle,
    )

    base = _oct5_persistent_snapshot(
        symbol="JNJ",
        entry_price=270.65,
        current_price=253.75,
        qty=1,
        first_fill_qty=1,
        first_soft_stop_price=255.13,
        lifecycle_id="life-current",
    )

    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(base)
    assert allowed is True
    assert reason == "PERSISTENT_SOFT_STOP_ESCALATION"

    no_fill = {
        **base,
        "risk_state": {
            "state": {
                "lifecycle": {"lifecycle_id": "life-current", "is_open": True},
                "soft_stop_execution": {
                    "soft_stop_triggered_today": True,
                    "soft_stop_partial_done": True,
                    "first_soft_stop_filled_qty": 0,
                    "position_lifecycle_id": "life-current",
                },
            },
        },
    }
    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(no_fill)
    assert allowed is False
    assert reason == "PERSISTENT_SOFT_STOP_PARTIAL_FILL_QTY_MISSING"

    stale_lifecycle = {
        **base,
        "risk_state": {
            "state": {
                "lifecycle": {"lifecycle_id": "life-current", "is_open": True},
                "soft_stop_execution": {
                    "soft_stop_triggered_today": True,
                    "soft_stop_partial_done": True,
                    "first_soft_stop_filled_qty": 1,
                    "position_lifecycle_id": "life-old",
                },
            },
        },
    }
    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(stale_lifecycle)
    assert allowed is False
    assert reason == "PERSISTENT_SOFT_STOP_LIFECYCLE_MISMATCH"

    missing_owner = dict(base)
    missing_owner.pop("strategy_owner", None)
    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(missing_owner)
    assert allowed is False
    assert reason == "PERSISTENT_SOFT_STOP_OWNER_UNVERIFIED"


def test_persistent_soft_stop_explicit_zero_orderable_qty_fails_closed():
    from trader.us.pb1.us_exit_engine import (
        _confirmed_soft_stop_partial_fill_for_current_lifecycle,
    )

    position = _oct5_persistent_snapshot(
        symbol="JNJ",
        entry_price=270.65,
        current_price=253.75,
        qty=1,
        first_fill_qty=1,
        first_soft_stop_price=255.13,
        lifecycle_id="life-jnj-zero-orderable",
        breach_count=67,
    )
    position["orderable_qty"] = 0

    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(position)
    assert allowed is False
    assert reason == "PERSISTENT_SOFT_STOP_NO_ORDERABLE_QTY"


def test_persistent_soft_stop_owner_isolation_keeps_tqqq_out():
    from trader.us.pb1.us_exit_engine import (
        _confirmed_soft_stop_partial_fill_for_current_lifecycle,
    )

    position = _oct5_persistent_snapshot(
        symbol="TQQQ",
        entry_price=100.0,
        current_price=94.0,
        qty=3,
        first_fill_qty=1,
        first_soft_stop_price=95.0,
        lifecycle_id="life-tqqq",
    )
    position["strategy_owner"] = "TQQQ_INFINITE"

    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(position)
    assert allowed is False
    assert reason == "PERSISTENT_SOFT_STOP_OWNER_ISOLATION"

def test_legacy_blank_soft_stop_lifecycle_fails_closed_without_broker_upgrade():
    from trader.us.pb1.us_exit_engine import (
        _confirmed_soft_stop_partial_fill_for_current_lifecycle,
    )

    incident = _oct5_persistent_snapshot(
        symbol="JNJ",
        entry_price=270.65,
        current_price=253.75,
        qty=1,
        first_fill_qty=1,
        first_soft_stop_price=255.13,
        lifecycle_id="life-jnj-oct5",
        breach_count=67,
    )
    incident["risk_state"]["state"]["soft_stop_execution"]["position_lifecycle_id"] = ""

    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(incident)
    assert allowed is False
    assert reason == "PERSISTENT_SOFT_STOP_LEGACY_LIFECYCLE_UNVERIFIED"


def test_oct5_legacy_blank_soft_stop_lifecycle_upgrades_from_broker_order(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_exit_engine import (
        _confirmed_soft_stop_partial_fill_for_current_lifecycle,
        _recover_legacy_soft_stop_execution_lifecycle,
    )

    incidents = [
        (
            _oct5_persistent_snapshot(
                symbol="JNJ",
                entry_price=270.65,
                current_price=253.75,
                qty=1,
                first_fill_qty=1,
                first_soft_stop_price=255.13,
                lifecycle_id="life-jnj-oct5",
                breach_count=67,
            ),
            "2026-10-05T23:04:28",
        ),
        (
            _oct5_persistent_snapshot(
                symbol="MRK",
                entry_price=148.287,
                current_price=139.95,
                qty=6,
                first_fill_qty=6,
                first_soft_stop_price=139.66,
                lifecycle_id="life-mrk-oct5",
                breach_count=60,
            ),
            "2026-10-05T23:19:14",
        ),
    ]
    saves = []

    def load_order_for_fill(*, order_no, client_order_key, symbol, trade_date):
        matching = next(item for item, _ in incidents if item["symbol"] == symbol)
        broker_time = next(ts for item, ts in incidents if item["symbol"] == symbol)
        soft_exec = matching["risk_state"]["state"]["soft_stop_execution"]
        return {
            "trade_date": trade_date,
            "symbol": symbol,
            "side": "SELL",
            "qty_filled": soft_exec["first_soft_stop_filled_qty"],
            "order_no": order_no,
            "client_order_key": client_order_key,
            "status": "FILLED",
            "meta": {
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "source_endpoint": "KIS_INQUIRE_CCNL",
                "order_timestamp": broker_time,
                "exit_reason": "soft_stop_loss",
            },
        }

    monkeypatch.setattr(repos, "load_us_order_for_fill", load_order_for_fill)
    def save_legacy_upgrade(symbol, trade_date, state, **kwargs):
        assert kwargs.get("require_durable") is True
        saves.append((symbol, trade_date, state))
        return True

    monkeypatch.setattr(
        repos,
        "save_us_position_risk_state",
        save_legacy_upgrade,
    )

    for incident, _broker_time in incidents:
        risk_state = incident["risk_state"]
        soft_exec = dict(risk_state["state"]["soft_stop_execution"])
        soft_exec["position_lifecycle_id"] = ""
        risk_state["state"]["soft_stop_execution"] = soft_exec

        upgraded = _recover_legacy_soft_stop_execution_lifecycle(
            incident,
            risk_state,
            soft_exec,
            incident["position_lifecycle_id"],
        )
        assert upgraded["position_lifecycle_id"] == incident["position_lifecycle_id"]
        assert upgraded["lifecycle_recovery_source"] == "BROKER_ORDER_TIMESTAMP"

        allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(incident)
        assert allowed is True
        assert reason == "PERSISTENT_SOFT_STOP_ESCALATION"

    assert {symbol for symbol, _, _ in saves} == {"JNJ", "MRK"}


def test_risk_state_require_durable_does_not_use_memory_fallback(monkeypatch):
    from trader.us.db import repos

    key = ("2026-10-05", "STRICTDURABLE")
    repos._MEM_RISK_STATE.pop(key, None)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)

    ok = repos.save_us_position_risk_state(
        "STRICTDURABLE",
        "2026-10-05",
        {"state": {"soft_stop_execution": {"position_lifecycle_id": "life-strict"}}},
        require_durable=True,
    )

    assert ok is False
    assert key not in repos._MEM_RISK_STATE


def test_legacy_soft_stop_upgrade_requires_durable_save(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_exit_engine import (
        _confirmed_soft_stop_partial_fill_for_current_lifecycle,
        _recover_legacy_soft_stop_execution_lifecycle,
    )

    incident = _oct5_persistent_snapshot(
        symbol="JNJ",
        entry_price=270.65,
        current_price=253.75,
        qty=1,
        first_fill_qty=1,
        first_soft_stop_price=255.13,
        lifecycle_id="life-jnj-oct5",
        breach_count=67,
    )
    soft_exec = incident["risk_state"]["state"]["soft_stop_execution"]
    soft_exec["position_lifecycle_id"] = ""

    monkeypatch.setattr(
        repos,
        "load_us_order_for_fill",
        lambda **kwargs: {
            "trade_date": "2026-10-05",
            "symbol": "JNJ",
            "side": "SELL",
            "qty_filled": 1,
            "status": "FILLED",
            "meta": {
                "strategy_owner": "US_STANDARD",
                "source_endpoint": "KIS_INQUIRE_CCNL",
                "order_timestamp": "2026-10-05T23:04:28",
            },
        },
    )
    monkeypatch.setattr(
        repos,
        "save_us_position_risk_state",
        lambda *args, **kwargs: False,
    )

    recovered = _recover_legacy_soft_stop_execution_lifecycle(
        incident,
        incident["risk_state"],
        dict(soft_exec),
        incident["position_lifecycle_id"],
    )
    assert recovered.get("position_lifecycle_id") in ("", None)

    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(incident)
    assert allowed is False
    assert reason == "PERSISTENT_SOFT_STOP_LEGACY_LIFECYCLE_UNVERIFIED"


def test_legacy_blank_soft_stop_same_day_ambiguous_open_time_fails_closed(monkeypatch):
    from trader.us.db import repos
    from trader.us.pb1.us_exit_engine import (
        _recover_legacy_soft_stop_execution_lifecycle,
    )

    incident = _oct5_persistent_snapshot(
        symbol="JNJ",
        entry_price=270.65,
        current_price=253.75,
        qty=1,
        first_fill_qty=1,
        first_soft_stop_price=255.13,
        lifecycle_id="life-jnj-reentry",
        breach_count=67,
    )
    lifecycle = incident["risk_state"]["state"]["lifecycle"]
    lifecycle["opened_trade_date"] = "2026-10-05"
    lifecycle["opened_at"] = "2026-10-05T23:10:00"  # legacy naive timestamp
    soft_exec = incident["risk_state"]["state"]["soft_stop_execution"]
    soft_exec["position_lifecycle_id"] = ""

    monkeypatch.setattr(
        repos,
        "load_us_order_for_fill",
        lambda **kwargs: {
            "trade_date": "2026-10-05",
            "symbol": "JNJ",
            "side": "SELL",
            "qty_filled": 1,
            "status": "FILLED",
            "meta": {
                "strategy_owner": "US_STANDARD",
                "source_endpoint": "KIS_INQUIRE_CCNL",
                "order_timestamp": "2026-10-05T23:04:28",
            },
        },
    )
    saves = []
    monkeypatch.setattr(
        repos,
        "save_us_position_risk_state",
        lambda *args, **kwargs: saves.append((args, kwargs)),
    )

    recovered = _recover_legacy_soft_stop_execution_lifecycle(
        incident,
        incident["risk_state"],
        dict(soft_exec),
        incident["position_lifecycle_id"],
    )
    assert recovered.get("position_lifecycle_id") in ("", None)
    assert saves == []


def test_soft_stop_fill_state_recovers_current_lifecycle_when_fill_meta_omits_id(monkeypatch):
    from trader.us.db import repos
    from trader.us.runner.trade_tick_runner import _mark_soft_stop_stages_from_records

    store = {
        "trade_date": "2026-10-05",
        "symbol": "JNJ",
        "soft_stop_breach_count": 7,
        "state": {
            "lifecycle": {
                "lifecycle_id": "life-jnj-oct5",
                "is_open": True,
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "opened_at": "2026-09-25T00:14:30+00:00",
            },
        },
    }

    monkeypatch.setattr(
        repos,
        "load_us_position_risk_state",
        lambda symbol, trade_date: dict(store),
    )

    def save_state(symbol, trade_date, state):
        store.clear()
        store.update(state)

    monkeypatch.setattr(repos, "save_us_position_risk_state", save_state)

    _mark_soft_stop_stages_from_records(
        [{
            "symbol": "JNJ",
            "side": "SELL",
            "qty": 1,
            "price": 255.13,
            "order_no": "0000001519",
            "client_order_key": "6485927d33957ca109a53853",
            "filled_at": "2026-10-06T16:00:00+00:00",
            "meta": {
                "exit_reason": "soft_stop_loss",
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "order_timestamp_utc": "2026-10-05T14:10:24+00:00",
            },
        }],
        trade_date="2026-10-05",
        status="FILLED",
    )

    soft_state = store["state"]["soft_stop_execution"]
    assert soft_state["soft_stop_partial_done"] is True
    assert soft_state["first_soft_stop_filled_qty"] == 1
    assert soft_state["position_lifecycle_id"] == "life-jnj-oct5"

def test_soft_stop_fill_state_prefers_broker_order_time_over_persisted_filled_at(monkeypatch):
    from trader.us.db import repos
    from trader.us.runner.trade_tick_runner import _mark_soft_stop_stages_from_records

    store = {
        "trade_date": "2026-10-06",
        "symbol": "JNJ",
        "soft_stop_breach_count": 0,
        "state": {
            "lifecycle": {
                "lifecycle_id": "life-jnj-reentry",
                "is_open": True,
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "opened_at": "2026-10-06T15:00:00+00:00",
            },
        },
    }

    monkeypatch.setattr(
        repos,
        "load_us_position_risk_state",
        lambda symbol, trade_date: dict(store),
    )
    saves = []
    monkeypatch.setattr(
        repos,
        "save_us_position_risk_state",
        lambda symbol, trade_date, state: saves.append(state),
    )

    _mark_soft_stop_stages_from_records(
        [{
            "symbol": "JNJ",
            "side": "SELL",
            "qty": 1,
            "price": 255.13,
            "order_no": "old-order",
            "client_order_key": "old-soft-stop",
            # DB persistence occurred after re-entry, but broker order time
            # proves the fill belongs to the prior lifecycle.
            "filled_at": "2026-10-06T16:00:00+00:00",
            "meta": {
                "exit_reason": "soft_stop_loss",
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "order_timestamp_utc": "2026-10-05T14:10:24+00:00",
            },
        }],
        trade_date="2026-10-06",
        status="FILLED",
    )

    assert saves == []
    assert "soft_stop_execution" not in store["state"]


def test_soft_stop_fill_state_does_not_use_observed_at_as_execution_time(monkeypatch):
    from trader.us.db import repos
    from trader.us.runner.trade_tick_runner import _mark_soft_stop_stages_from_records

    store = {
        "trade_date": "2026-10-06",
        "symbol": "JNJ",
        "soft_stop_breach_count": 0,
        "state": {
            "lifecycle": {
                "lifecycle_id": "life-jnj-reentry",
                "is_open": True,
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "opened_at": "2026-10-06T15:00:00+00:00",
            },
        },
    }

    monkeypatch.setattr(
        repos,
        "load_us_position_risk_state",
        lambda symbol, trade_date: dict(store),
    )
    saves = []
    monkeypatch.setattr(
        repos,
        "save_us_position_risk_state",
        lambda symbol, trade_date, state: saves.append(state),
    )

    _mark_soft_stop_stages_from_records(
        [{
            "symbol": "JNJ",
            "side": "SELL",
            "qty": 1,
            "price": 255.13,
            "order_no": "old-order",
            "client_order_key": "old-soft-stop",
            # Observation occurs after re-entry, but there is no actual
            # execution/order timestamp. This must not authorize rebinding.
            "observed_at": "2026-10-06T16:00:00+00:00",
            "meta": {
                "exit_reason": "soft_stop_loss",
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "observed_at": "2026-10-06T16:00:00+00:00",
            },
        }],
        trade_date="2026-10-06",
        status="FILLED",
    )

    assert saves == []
    assert "soft_stop_execution" not in store["state"]


def test_soft_stop_fill_state_does_not_rebind_stale_fill_to_reentry_lifecycle(monkeypatch):
    from trader.us.db import repos
    from trader.us.runner.trade_tick_runner import _mark_soft_stop_stages_from_records

    store = {
        "trade_date": "2026-10-06",
        "symbol": "JNJ",
        "soft_stop_breach_count": 0,
        "state": {
            "lifecycle": {
                "lifecycle_id": "life-jnj-reentry",
                "is_open": True,
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "opened_at": "2026-10-06T15:00:00+00:00",
            },
        },
    }

    monkeypatch.setattr(
        repos,
        "load_us_position_risk_state",
        lambda symbol, trade_date: dict(store),
    )
    saves = []
    monkeypatch.setattr(
        repos,
        "save_us_position_risk_state",
        lambda symbol, trade_date, state: saves.append(state),
    )

    _mark_soft_stop_stages_from_records(
        [{
            "symbol": "JNJ",
            "side": "SELL",
            "qty": 1,
            "price": 255.13,
            "order_no": "old-order",
            "client_order_key": "old-soft-stop",
            "filled_at": "2026-10-06T16:00:00+00:00",
            "meta": {
                "exit_reason": "soft_stop_loss",
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
                "order_timestamp_utc": "2026-10-05T14:10:24+00:00",
            },
        }],
        trade_date="2026-10-06",
        status="FILLED",
    )

    assert saves == []
    assert "soft_stop_execution" not in store["state"]

