from trader.kr.entry_identity_proof import entry_style_identity_proof


def test_breakout_trigger_does_not_promote_momentum_to_breakout_owner():
    value = entry_style_identity_proof(
        selected_style="MOMENTUM", trigger_ok=True,
        decision_family="ENTRY_BREAKOUT_CONFIRMED", trigger_policy="BREAKOUT_CONFIRMED")
    assert value["state"] == "VERIFIED"
    assert value["selected_style"] == "ENTRY_MOMENTUM"
    assert value["trigger_event"] == "BREAKOUT_PRICE_CONFIRMED"
    assert value["contract_trade_horizon"] == "DAY_TRADE"
    assert value["contract_exit_policy_family"] == "INTRADAY_PROFIT_PROTECT"


def test_pullback_override_keeps_swing_owner():
    value = entry_style_identity_proof(
        selected_style="PULLBACK", trigger_ok=False,
        decision_family="ENTRY_PULLBACK_OVERRIDE", trigger_policy="PULLBACK_OVERRIDE")
    assert value["contract_trade_horizon"] == "SWING"
    assert value["contract_eod_action"] == "CARRY_IF_NO_EXIT_SIGNAL"


def test_unrecognized_style_never_infers_swing_or_day():
    proof = entry_style_identity_proof(
        selected_style="UNKNOWN", trigger_ok=True,
        decision_family="ENTRY_BREAKOUT_CONFIRMED", trigger_policy="BREAKOUT_CONFIRMED")
    assert proof["state"] == "UNKNOWN"
    assert "contract_trade_horizon" not in proof


def test_trend_swing_requires_selected_owner_practice_optin_and_independent_proof():
    from trader.kr.entry_identity_proof import select_practice_trend_horizon
    arguments = dict(selected_style="MOMENTUM", environment="practice",
                     enabled=True, volume_proof={"confirmation_state": "VERIFIED"},
                     price=120, ma20=110, ma50=100, rs_percentile=0.75)
    ok = select_practice_trend_horizon(**arguments)
    assert ok["state"] == "SWING_ELIGIBLE"
    assert ok["trade_horizon"] == "SWING"
    assert select_practice_trend_horizon(**{**arguments, "enabled": False})["state"] == "KEEP_EXISTING_DAY"
    assert select_practice_trend_horizon(**{**arguments, "environment": "real"})["reason"] == "practice_only"
    assert select_practice_trend_horizon(**{**arguments, "explicit_day": True})["reason"] == "explicit_day_trade"
    assert select_practice_trend_horizon(**{**arguments, "ma50": None})["state"] == "KEEP_EXISTING_DAY"
    assert select_practice_trend_horizon(**{**arguments, "rs_percentile": 0.4})["state"] == "KEEP_EXISTING_DAY"
    assert select_practice_trend_horizon(**{**arguments, "volume_proof": {}})["state"] == "KEEP_EXISTING_DAY"


def test_breakout_swing_requires_price_pivot_and_volume_proof():
    from trader.kr.entry_identity_proof import select_practice_trend_horizon
    kwargs = dict(selected_style="BREAKOUT", environment="practice", enabled=True,
                  volume_proof={"confirmation_state": "VERIFIED",
                                "breakout_volume_confirmed": True},
                  price=120, ma20=110, ma50=100, rs_percentile=80,
                  pivot=119, trigger_ok=True)
    assert select_practice_trend_horizon(**kwargs)["state"] == "SWING_ELIGIBLE"
    for overrides in ({"trigger_ok": False},
                      {"pivot": 121},
                      {"volume_proof": {"confirmation_state": "VERIFIED",
                                       "breakout_volume_confirmed": False}}):
        assert select_practice_trend_horizon(**{**kwargs, **overrides})["state"] == "KEEP_EXISTING_DAY"

def test_practice_trend_swing_default_is_on_for_new_paper_buys(monkeypatch):
    """October 29 incident: new MICO-like candidate must not fall back by default."""
    from trader.kr.entry_identity_proof import (
        kr_practice_trend_swing_enabled, select_practice_trend_horizon,
    )
    monkeypatch.delenv("PB1_KR_PRACTICE_TREND_SWING", raising=False)
    assert kr_practice_trend_swing_enabled(environment="practice") is True
    eligible = select_practice_trend_horizon(
        selected_style="ENTRY_MOMENTUM",
        environment="practice",
        enabled=kr_practice_trend_swing_enabled(environment="practice"),
        volume_proof={"confirmation_state": "VERIFIED"},
        price=26050,
        ma20=22686,
        ma50=20650,
        rs_percentile=0.88628762541806,
        trigger_ok=False,
        explicit_day=False,
    )
    assert eligible["state"] == "SWING_ELIGIBLE"
    assert eligible["trade_horizon"] == "SWING"

    # The selected swing style must generate the immutable SWING/Carry exit plan.
    from trader.trade_plan import build_entry_exit_plan
    plan = build_entry_exit_plan(
        code="059090", market="KOSDAQ",
        entry_style_selected="ENTRY_MOMENTUM_SWING",
        entry_reason="ENTRY_MOMENTUM",
        entry_price=26050, features={},
    ).to_dict()
    assert plan["trade_horizon"] == "SWING"
    assert plan["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert plan["eod_action"] == "CARRY_IF_NO_EXIT_SIGNAL"
    assert plan["force_eod_close"] is False


def test_practice_trend_swing_respects_explicit_operational_off(monkeypatch):
    from trader.kr.entry_identity_proof import kr_practice_trend_swing_enabled
    monkeypatch.setenv("PB1_KR_PRACTICE_TREND_SWING", "0")
    assert kr_practice_trend_swing_enabled(environment="practice") is False


def test_practice_trend_swing_never_opt_in_real_accounts(monkeypatch):
    from trader.kr.entry_identity_proof import kr_practice_trend_swing_enabled
    monkeypatch.delenv("PB1_KR_PRACTICE_TREND_SWING", raising=False)
    assert kr_practice_trend_swing_enabled(environment="real") is False
    monkeypatch.setenv("PB1_KR_PRACTICE_TREND_SWING", "1")
    assert kr_practice_trend_swing_enabled(environment="real") is False
    assert kr_practice_trend_swing_enabled(environment="practice") is True


def test_practice_trend_swing_still_honors_explicit_day_contract(monkeypatch):
    from trader.kr.entry_identity_proof import (
        kr_practice_trend_swing_enabled, select_practice_trend_horizon,
    )
    monkeypatch.delenv("PB1_KR_PRACTICE_TREND_SWING", raising=False)
    result = select_practice_trend_horizon(
        selected_style="ENTRY_MOMENTUM", environment="practice",
        enabled=kr_practice_trend_swing_enabled(environment="practice"),
        volume_proof={"confirmation_state": "VERIFIED"},
        price=26050, ma20=22686, ma50=20650, rs_percentile=88.6,
        explicit_day=True,
    )
    assert result["state"] == "KEEP_EXISTING_DAY"
    assert result["reason"] == "explicit_day_trade"


def test_practice_trend_swing_does_not_skip_evidence_requirements(monkeypatch):
    from trader.kr.entry_identity_proof import (
        kr_practice_trend_swing_enabled, select_practice_trend_horizon,
    )
    monkeypatch.delenv("PB1_KR_PRACTICE_TREND_SWING", raising=False)
    kwargs = dict(
        selected_style="ENTRY_MOMENTUM", environment="practice",
        enabled=kr_practice_trend_swing_enabled(environment="practice"),
        volume_proof={"confirmation_state": "VERIFIED"},
        price=26050, ma20=22686, ma50=20650, rs_percentile=88.6,
    )
    for mutation in [
        {"volume_proof": {"confirmation_state": "UNKNOWN"}},
        {"ma20": None},
        {"rs_percentile": 40},
        {"ma50": 29000},
    ]:
        assert select_practice_trend_horizon(**{**kwargs, **mutation})["state"] == "KEEP_EXISTING_DAY"
