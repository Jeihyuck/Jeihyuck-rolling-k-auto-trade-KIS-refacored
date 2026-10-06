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
            "soft_stop_breach_count": breach_count,
            "state": {
                "lifecycle": {
                    "lifecycle_id": lifecycle_id,
                    "is_open": True,
                    "opened_at": "2026-09-25T00:14:30+00:00",
                    "strategy_owner": "US_STANDARD",
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

def test_oct5_legacy_blank_soft_stop_lifecycle_recovers_only_within_current_open_lifecycle():
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
    assert allowed is True
    assert reason == "PERSISTENT_SOFT_STOP_ESCALATION_LEGACY_LIFECYCLE_RECOVERED"

    reentered = {
        **incident,
        "risk_state": {
            **incident["risk_state"],
            "state": {
                **incident["risk_state"]["state"],
                "lifecycle": {
                    **incident["risk_state"]["state"]["lifecycle"],
                    "lifecycle_id": "life-reentry",
                    "opened_at": "2026-10-06T15:00:00+00:00",
                    "is_open": True,
                },
            },
        },
        "position_lifecycle_id": "life-reentry",
    }
    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(reentered)
    assert allowed is False
    assert reason == "PERSISTENT_SOFT_STOP_LEGACY_LIFECYCLE_UNVERIFIED"


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
            "meta": {
                "exit_reason": "soft_stop_loss",
                "strategy_owner": "US_STANDARD",
                "sleeve_id": "US_STANDARD",
            },
        }],
        trade_date="2026-10-05",
        status="FILLED",
    )

    soft_state = store["state"]["soft_stop_execution"]
    assert soft_state["soft_stop_partial_done"] is True
    assert soft_state["first_soft_stop_filled_qty"] == 1
    assert soft_state["position_lifecycle_id"] == "life-jnj-oct5"

