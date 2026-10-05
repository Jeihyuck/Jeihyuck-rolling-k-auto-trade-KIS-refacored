from datetime import datetime, timezone
from trader.us.db import repos
from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles


def setup_function(): repos.reset_memory_stores()

def test_lifecycle_carries_forward_and_closes_only_authoritative():
    now=datetime(2026,7,10,tzinfo=timezone.utc)
    identity={"env":"practice","account_id":"practice:test-account","trading_epoch_id":"epoch-a","strategy_owner":"US_STANDARD"}
    pos=[{"symbol":"SAMPLE","qty":10,"entry_price":100,"current_price_usd":105,**identity}]
    first=reconcile_us_position_lifecycles(positions=pos, trade_date="2026-07-10", now=now, authoritative=True, env="practice")["SAMPLE"]
    lid=first["lifecycle_id"]
    pos2=[{"symbol":"SAMPLE","qty":12,"entry_price":101,"current_price_usd":104,**identity}]
    second=reconcile_us_position_lifecycles(positions=pos2, trade_date="2026-07-11", now=datetime(2026,7,11,tzinfo=timezone.utc), authoritative=False, env="practice")["SAMPLE"]
    assert second["lifecycle_id"] == lid
    assert second["holding_trade_days"] == 2
    assert reconcile_us_position_lifecycles(positions=[], trade_date="2026-07-12", now=datetime(2026,7,12,tzinfo=timezone.utc), authoritative=False) == {}
    assert repos.load_latest_us_position_risk_state("SAMPLE","2026-07-12")["state"]["lifecycle"]["is_open"] is True
    closed=reconcile_us_position_lifecycles(positions=[], trade_date="2026-07-12", now=datetime(2026,7,12,tzinfo=timezone.utc), authoritative=True)["SAMPLE"]
    assert closed["is_open"] is False
    new=reconcile_us_position_lifecycles(positions=[{"symbol":"SAMPLE","qty":1,"entry_price":90,**identity}], trade_date="2026-07-13", now=datetime(2026,7,13,tzinfo=timezone.utc), authoritative=True, env="practice")["SAMPLE"]
    assert new["lifecycle_id"] != lid
    assert new["entry_policy"] == {}
    assert new["high_watermark"] == 90


def test_add_partial_liquidation_and_reentry_keep_distinct_frozen_contracts():
    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    from trader.us.position_lifecycle_state import update_us_position_high_watermark

    identity={"env":"practice","account_id":"practice:test-account","trading_epoch_id":"epoch-a","strategy_owner":"US_STANDARD"}
    contract_a=build_us_entry_exit_contract({
        "symbol":"SAMPLE","strategy_owner":"US_STANDARD","book":"BOOK_A",
        "horizon":"HORIZON_A","exit_policy":"EXIT_A",
    })
    contract_b=build_us_entry_exit_contract({
        "symbol":"SAMPLE","strategy_owner":"US_STANDARD","book":"BOOK_B",
        "horizon":"HORIZON_B","exit_policy":"EXIT_B",
    })
    first_fill={
        "symbol":"SAMPLE","side":"BUY","qty":10,"filled_at":"2026-07-10T14:00:00+00:00",
        "env":"practice","account_id":"practice:test-account",
        "trading_epoch_id":"epoch-a","strategy_owner":"US_STANDARD",
        "position_lifecycle_id":"life-a",
        "meta":{
            "strategy_owner":"US_STANDARD","position_lifecycle_id":"life-a",
            "entry_exit_contract":contract_a,
            "entry_exit_contract_sha256":contract_a["sha256"],
            "entry_exit_contract_version":contract_a["version"],
        },
    }
    first_positions=[{"symbol":"SAMPLE","qty":10,"entry_price":100,**identity}]
    first=reconcile_us_position_lifecycles(
        positions=first_positions,trade_date="2026-07-10",
        now=datetime(2026,7,10,tzinfo=timezone.utc),authoritative=True,
        fills=[first_fill],env="practice",
    )["SAMPLE"]
    assert first["lifecycle_id"]=="life-a"
    assert first["entry_policy"]["entry_exit_contract_sha256"]==contract_a["sha256"]
    update_us_position_high_watermark(
        symbol="SAMPLE",trade_date="2026-07-10",lifecycle_id="life-a",
        current_price=200,entry_price=100,now=datetime(2026,7,10,tzinfo=timezone.utc),
    )
    repos.mark_us_profit_capture_stage(
        "2026-07-10","SAMPLE","tp1",status="DONE",
        position_lifecycle_id="life-a",
        qty=2,filled_qty=2,evidence_type="KIS_EXECUTION_ACTUAL",
    )

    add_positions=[{
        "symbol":"SAMPLE","qty":12,"entry_price":101,
        "position_lifecycle_id":"life-a",**identity,
        "meta":{
            "entry_exit_contract":contract_b,
            "entry_exit_contract_sha256":contract_b["sha256"],
            "entry_exit_contract_version":contract_b["version"],
        },
    }]
    added=reconcile_us_position_lifecycles(
        positions=add_positions,trade_date="2026-07-11",
        now=datetime(2026,7,11,tzinfo=timezone.utc),authoritative=False,
        env="practice",
    )["SAMPLE"]
    assert added["lifecycle_id"]=="life-a"
    assert added["entry_policy"]["entry_exit_contract_sha256"]==contract_a["sha256"]
    partial_positions=[{"symbol":"SAMPLE","qty":4,"entry_price":100,**identity,"position_lifecycle_id":"life-a"}]
    partial=reconcile_us_position_lifecycles(
        positions=partial_positions,trade_date="2026-07-11",
        now=datetime(2026,7,11,tzinfo=timezone.utc),authoritative=False,
        env="practice",
    )["SAMPLE"]
    assert partial["lifecycle_id"]=="life-a"
    assert partial["entry_policy"]["entry_exit_contract_sha256"]==contract_a["sha256"]

    closed=reconcile_us_position_lifecycles(
        positions=[],trade_date="2026-07-12",
        now=datetime(2026,7,12,tzinfo=timezone.utc),authoritative=True,
        env="practice",
    )["SAMPLE"]
    assert closed["is_open"] is False

    reentry_identity={**identity,"position_lifecycle_id":"life-b"}
    second_fill={
        "symbol":"SAMPLE","side":"BUY","qty":2,"filled_at":"2026-07-13T14:00:00+00:00",
        "env":"practice","account_id":"practice:test-account",
        "trading_epoch_id":"epoch-a","strategy_owner":"US_STANDARD",
        "position_lifecycle_id":"life-b",
        "meta":{
            "strategy_owner":"US_STANDARD","position_lifecycle_id":"life-b",
            "entry_exit_contract":contract_b,
            "entry_exit_contract_sha256":contract_b["sha256"],
            "entry_exit_contract_version":contract_b["version"],
        },
    }
    reopened=reconcile_us_position_lifecycles(
        positions=[{"symbol":"SAMPLE","qty":2,"entry_price":95,"current_price_usd":96,**reentry_identity}],
        trade_date="2026-07-13",now=datetime(2026,7,13,tzinfo=timezone.utc),
        authoritative=True,fills=[second_fill],env="practice",
    )["SAMPLE"]
    assert reopened["lifecycle_id"]=="life-b"
    assert reopened["opened_at"]==second_fill["filled_at"]
    assert reopened["entry_policy"]["entry_exit_contract_sha256"]==contract_b["sha256"]
    assert reopened["high_watermark"]==96
    assert repos.load_us_profit_capture_state(
        "2026-07-13",["SAMPLE"],{"SAMPLE":"life-b"},
    )["SAMPLE"]["tp1_done"] is False


def test_authoritative_broker_position_binds_active_epoch_and_exact_fill_owner(monkeypatch):
    from trader.account_state import get_account_key

    account_id = get_account_key(env="practice")
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_args, **_kwargs: "epoch-current")
    fill = {
        "symbol": "SAMPLE", "side": "BUY", "qty": 2,
        "filled_at": "2026-07-10T14:00:00+00:00",
        "env": "practice", "account_id": account_id,
        "trading_epoch_id": "epoch-current", "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "life-current",
    }
    position = {"symbol": "SAMPLE", "qty": 2, "entry_price": 100}

    lifecycle = reconcile_us_position_lifecycles(
        positions=[position], trade_date="2026-07-10",
        now=datetime(2026, 7, 10, tzinfo=timezone.utc), authoritative=True,
        fills=[fill], env="practice",
    )["SAMPLE"]

    assert lifecycle["lifecycle_id"] == "life-current"
    assert lifecycle["trading_epoch_id"] == "epoch-current"
    assert lifecycle["strategy_owner"] == "US_STANDARD"
    assert position["lifecycle_identity_status"] == "IDENTIFIED"


def test_authoritative_broker_position_recovers_unique_open_lifecycle_identity(monkeypatch):
    from trader.account_state import get_account_key

    account_id = get_account_key(env="practice")
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_args, **_kwargs: "epoch-current")
    identity = {
        "env": "practice", "account_id": account_id,
        "trading_epoch_id": "epoch-current", "strategy_owner": "US_STANDARD",
    }
    first = reconcile_us_position_lifecycles(
        positions=[{"symbol": "SAMPLE", "qty": 2, "entry_price": 100, **identity}],
        trade_date="2026-07-10", now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        authoritative=True, env="practice",
    )["SAMPLE"]
    broker_position = {"symbol": "SAMPLE", "qty": 2, "entry_price": 100}

    recovered = reconcile_us_position_lifecycles(
        positions=[broker_position], trade_date="2026-07-11",
        now=datetime(2026, 7, 11, tzinfo=timezone.utc),
        authoritative=True, env="practice",
    )["SAMPLE"]

    assert recovered["lifecycle_id"] == first["lifecycle_id"]
    assert broker_position["strategy_owner"] == "US_STANDARD"
    assert broker_position["trading_epoch_id"] == "epoch-current"


def test_authoritative_broker_position_rejects_unscoped_or_cross_account_fill(monkeypatch):
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *_args, **_kwargs: "epoch-current")
    for fill_account in ("", "practice:another-account"):
        position = {"symbol": "SAMPLE", "qty": 2, "entry_price": 100}
        fill = {
            "symbol": "SAMPLE", "side": "BUY", "qty": 2,
            "filled_at": "2026-07-10T14:00:00+00:00",
            "env": "practice", "account_id": fill_account,
            "trading_epoch_id": "epoch-current", "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": "life-current",
        }

        result = reconcile_us_position_lifecycles(
            positions=[position], trade_date="2026-07-10",
            now=datetime(2026, 7, 10, tzinfo=timezone.utc), authoritative=True,
            fills=[fill], env="practice",
        )

        assert result == {}
        assert position["lifecycle_identity_status"] == "MISSING_OR_AMBIGUOUS"
