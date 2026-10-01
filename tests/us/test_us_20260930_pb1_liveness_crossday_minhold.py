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
    monkeypatch.setattr(
        repos,
        "load_latest_us_position_risk_state",
        lambda symbol, trade_date: {
            "state": {
                "lifecycle": {
                    "lifecycle_id": "lite-cross-day",
                    "is_open": True,
                    "opened_trade_date": "2026-09-22",
                    "opened_at": opened_at,
                    "opened_at_source": "confirmed_buy_fill",
                    "holding_trade_days": 7,
                    "high_watermark": 997.735,
                }
            }
        },
    )

    rows, meta = resolver.enrich_us_positions_for_exit(
        [
            {
                "symbol": "LITE",
                "qty": 1,
                # Reconcile-skip DB rows already carry a resolved price/source;
                # this used to trigger the early return before lifecycle timing.
                "entry_price": 949.55,
                "entry_price_source": "us_positions_avg_cost",
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
