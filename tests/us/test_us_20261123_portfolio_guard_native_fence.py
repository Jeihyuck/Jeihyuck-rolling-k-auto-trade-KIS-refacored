"""PORT-04/05 native boundary authorization mock, no KIS POST or DB writes.

The ready-for-review gate remains unmet until a complete runtime
snapshot→HWM→reservation→router ACK/fill test exists.
"""
from datetime import datetime, timezone
from decimal import Decimal
from contextlib import contextmanager

import pytest

from trader.us.portfolio_profit_guard import policy_digest
from trader.us.portfolio_profit_guard_store import (
    assert_portfolio_guard_native_pre_http, PortfolioGuardUnavailable,
)
import hashlib
import hmac
from trader.us.portfolio_profit_guard import canonical

KEY = "offline-only-HMAC-" + "X" * 50
ACCOUNT = "f" * 64
EPOCH = "practice-test-trading-epoch"
PORT = "practice-test-portfolio-epoch"

def _policy():
    doc = {
        "purpose": "US_STANDARD_PORTFOLIO_GIVEBACK_SELL_V1",
        "mode": "ACTIVE", "env": "practice", "sleeve": "US_STANDARD",
        "account_hash": ACCOUNT, "trading_epoch_id": EPOCH,
        "portfolio_epoch_id": PORT, "policy_id": "synthetic-policy",
        "policy_version": "v1",
        "effective_at": "2026-11-23T14:00:00+00:00",
        "expires_at": "2026-11-24T14:00:00+00:00",
        "accounting_method": "NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1",
        "mark_source": "KIS_BID",
        "activation_profit_usd": "100", "quote_ttl_sec": "5",
        "max_snapshot_skew_sec": "3", "rearm_mode": "OPERATOR_SIGNED_NEW_EPISODE",
        "selection_method": "NATIVE_UNDERPERFORMER_FIRST",
        "reentry_mode": "SIGNED_RELEASE_AFTER_COOLDOWN",
        "reentry_min_cooldown_seconds": "3600",
        "reentry_min_recovery_fraction": "0.1",
        "min_order_notional_usd": "1",
        "max_order_notional_usd": "1000000",
        "stages": [{"giveback_fraction": "0.25", "cumulative_reduction_fraction": "0.20"}],
    }
    doc["hmac_sha256"] = hmac.new(KEY.encode(),canonical(doc),hashlib.sha256).hexdigest()
    return doc

NOW = datetime(2026, 11, 23, 15, 0, tzinfo=timezone.utc)
ACTION = "US_PPG_test-native-action"


def _intent():
    return {
        "symbol": "KO", "side": "SELL", "strategy_owner": "US_STANDARD",
        "exit_type": "portfolio_profit_guard", "qty": 2,
        "position_cycle_id": "native-cycle",
        "client_order_key": ACTION,
        "meta": {
            "portfolio_guard_action_key": ACTION,
            "portfolio_guard_policy_digest": policy_digest(_policy()),
            "portfolio_guard_policy_id": "synthetic-policy",
            "portfolio_guard_policy_version": "v1",
            "portfolio_guard_portfolio_epoch_id": PORT,
            "portfolio_guard_trading_epoch_id": EPOCH,
            "portfolio_guard_stage_no": 1,
            "portfolio_guard_episode_id": "episode-one",
            "portfolio_guard_quote_asof_utc": NOW.isoformat(),
            "portfolio_guard_verified_bid_usd": "100",
        },
    }


class _Rows:
    def __init__(self, rows):
        self.rows = rows
    def mappings(self):
        return self
    def all(self):
        return self.rows


class _Conn:
    def __init__(self, action):
        self.action = action
        self.reads = []
    def execute(self, statement, params):
        query = str(statement)
        self.reads.append(query)
        if "FROM public.us_portfolio_guard_policy" in query:
            return _Rows([{
                "policy_id": "synthetic-policy", "policy_version": "v1",
                "authorization_sha256": policy_digest(_policy()),
                "authorization_json": _policy(),
            }])
        if "FROM public.us_portfolio_guard_actions" in query:
            return _Rows([self.action])
        raise AssertionError("unexpected SQL: " + query[:90])


class _Engine:
    def __init__(self, conn):
        self.conn = conn
    @contextmanager
    def begin(self):
        yield self.conn


@pytest.fixture
def allow_native(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import recovery_identity
    from trader import account_state
    monkeypatch.setenv("US_PORTFOLIO_GUARD_ACTIVE", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_PORTFOLIO_GUARD_SIGNING_KEY", KEY)
    raw = "fixture-native-account"
    monkeypatch.setattr(account_state, "get_account_key", lambda **kw: raw)
    import hashlib
    assert hashlib.sha256(raw.encode()).hexdigest() != ACCOUNT
    # The fake account itself has an exact matching hash for the policy.
    actual_hash = hashlib.sha256(raw.encode()).hexdigest()
    doc = _policy()
    doc["account_hash"] = actual_hash
    from trader.us.portfolio_profit_guard import canonical
    import hmac
    doc["hmac_sha256"] = hmac.new(KEY.encode(), canonical(
        {k:v for k,v in doc.items() if k!="hmac_sha256"}
    ), hashlib.sha256).hexdigest()
    def sign_dynamic_policy(action):
        from trader.us.portfolio_profit_guard_store import _get_policy
        # The mock DB is constructed below with the operator-signed document.
        return doc
    action = {
        "symbol": "KO", "position_cycle_id": "native-cycle",
        "stage_no": 1, "episode_id": "episode-one",
        "target_qty": 2, "cumulative_verified_fill_qty": 0,
        "reserved_qty": 0, "state": "PLANNED",
        "manifest_sha256": policy_digest(doc),
    }
    conn = _Conn(action)
    base_execute = conn.execute
    def execute(query, params):
        if "FROM public.us_portfolio_guard_policy" in str(query):
            return _Rows([{
                "policy_id": "synthetic-policy", "policy_version": "v1",
                "authorization_sha256": policy_digest(doc),
                "authorization_json": doc,
            }])
        return base_execute(query, params)
    conn.execute = execute
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: _Engine(conn))
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_args, **_kw: EPOCH)
    monkeypatch.setattr(recovery_identity, "current_us_portfolio_epoch", lambda *_args, **_kw: PORT)
    intent = _intent()
    intent["meta"]["portfolio_guard_policy_digest"] = policy_digest(doc)
    return intent, conn, action, doc


def test_positive_native_policy_and_durable_action_authorization(allow_native):
    intent, conn, *_ = allow_native
    assert_portfolio_guard_native_pre_http(
        intent, now=NOW, actual_qty=2, actual_limit_price=Decimal("99.8"),
    )
    assert any("FOR UPDATE" in query for query in conn.reads)


@pytest.mark.parametrize("mutation", [
    lambda i, a: i["meta"].update({"portfolio_guard_episode_id": "other"}),
    lambda i, a: i["meta"].update({"portfolio_guard_quote_asof_utc": "2026-11-23T14:59:00+00:00"}),
    lambda i, a: i.update({"client_order_key": "other-key"}),
    lambda i, a: i.update({"strategy_owner": "TQQQ_INFINITE"}),
    lambda i, a: i.update({"position_cycle_id": "other-cycle"}),
    lambda i, a: a.update({"state": "SUBMIT_UNKNOWN"}),
    lambda i, a: a.update({"cumulative_verified_fill_qty": 1}),
])
def test_native_policy_action_quote_or_fence_mismatch_blocks(allow_native, mutation):
    intent, _, action, _ = allow_native
    mutation(intent, action)
    with pytest.raises(PortfolioGuardUnavailable):
        assert_portfolio_guard_native_pre_http(
            intent, now=NOW, actual_qty=2, actual_limit_price=Decimal("99.8"),
        )


def test_native_policy_hard_off_never_submits(allow_native, monkeypatch):
    intent, *_ = allow_native
    monkeypatch.setenv("US_PORTFOLIO_GUARD_ACTIVE", "0")
    with pytest.raises(PortfolioGuardUnavailable, match="NOT_OPERATOR_ACTIVE"):
        assert_portfolio_guard_native_pre_http(
            intent, now=NOW, actual_qty=2, actual_limit_price=Decimal("99.8"),
        )
