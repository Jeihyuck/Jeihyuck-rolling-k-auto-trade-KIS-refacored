from dataclasses import replace
from datetime import date

from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.models import Action, OrderIntent, State, Status
from trader.kr.infinite.runner import run_once

DAY = date(2026, 8, 14)
REGIME = lambda: ("KR_NORMAL", "OK")


class FakeKIS:
    def __init__(self, *, qty=0, average=0, price=100, fill_qty=0):
        self.qty, self.average, self.price, self.fill_qty = qty, average, price, fill_qty
        self.orders, self.events, self.last_side = [], [], None

    def get_balance(self, force=True):
        holdings = [] if self.qty == 0 else [{"pdno": "122630", "hldg_qty": str(self.qty), "ord_psbl_qty": str(self.qty), "pchs_avg_pric": str(self.average)}]
        return {"output1": holdings, "output2": {"tot_evlu_amt": "4000000"}}

    def get_current_price(self, symbol): return self.price
    def get_orderable_cash(self, symbol, price): return 4_000_000, {}

    def buy_stock_limit(self, symbol, qty, price):
        self.events.append("submit"); self.orders.append(("BUY", qty)); self.last_side = "BUY"
        filled = min(qty, self.fill_qty)
        if filled:
            self.qty += filled; self.average = price
        return {"rt_cd": "0", "output": {"ODNO": "O1"}}

    def sell_stock(self, symbol, qty):
        self.events.append("submit"); self.orders.append(("SELL", qty)); self.last_side = "SELL"
        self.qty -= min(qty, self.fill_qty)
        return {"rt_cd": "0", "output": {"ODNO": "O1"}}

    def inquire_daily_ccld(self, **kwargs):
        return {"output1": [{"odno": "O1", "tot_ccld_qty": str(self.fill_qty), "avg_prvs": str(self.price)}]}


class FakeRepository:
    def __init__(self, state=None, intents=None):
        self.state, self.intents, self.events = state, list(intents or []), []

    def ensure_schema(self): self.events.append("schema")
    def load_state(self): self.events.append("load"); return self.state
    def intent_keys(self): return frozenset(item.idempotency_key for item in self.intents)
    def pending_intents(self): return [item for item in self.intents if item.status in {"INTENT_CREATED", "SUBMITTED", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING"}]
    def save_state(self, state): self.state = state

    def create_intent(self, state, decision, trade_date, market_state):
        self.events.append("intent")
        if decision.idempotency_key in self.intent_keys(): return False
        self.intents.append(OrderIntent(len(self.intents)+1, state.cycle_id, trade_date, decision.action.value,
                                        decision.idempotency_key, decision.qty, unit_sequence=state.units_used + 1))
        return True

    def mark_submitted(self, key, order_id):
        self.intents = [replace(item, broker_order_id=order_id, status="SUBMITTED") if item.idempotency_key == key else item for item in self.intents]

    def mark_rejected(self, key, reason):
        self.intents = [replace(item, status="REJECTED") if item.idempotency_key == key else item for item in self.intents]

    def persist_reconciliation(self, state, updates):
        self.state = state
        by_id = {intent.id: broker for intent, broker in updates}
        self.intents = [replace(item, status=by_id[item.id].status, filled_qty=by_id[item.id].filled_qty,
                                filled_notional_krw=by_id[item.id].filled_notional_krw)
                        if item.id in by_id else item for item in self.intents]


def config(live=False): return InfiniteConfig(enabled=True, live=live)

def active(**changes):
    base = State(cycle_id="KRINF-20260801-owned", cycle_start_date=date(2026, 8, 1),
                 allocated_capital_krw=1_200_000, unit_krw=30_000,
                 core_filled_notional=100_000, units_used=1, core_units_used=1,
                 last_buy_date=date(2026, 8, 10), last_buy_price=100, status=Status.ACTIVE)
    return replace(base, **changes)


def test_practice_and_real_live_submit_same_buy_path():
    for env, live in (("practice", False), ("real", True)):
        kis, repo = FakeKIS(fill_qty=300), FakeRepository()
        result = run_once(config=config(live), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env=env)
        assert result.decision.action == Action.BUY and result.submitted and len(kis.orders) == 1
        assert repo.events.index("intent") < kis.events.index("submit") if "submit" in repo.events else True
        assert repo.state.cycle_id.startswith("KRINF-20260814-") and repo.state.cycle_id != "NEW"


def test_real_live_gate_off_never_submits():
    kis, repo = FakeKIS(), FakeRepository()
    result = run_once(config=config(False), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="real")
    assert result.decision.reason == "KR_INF_LIVE_GATE_CLOSED" and not kis.orders


def test_intent_is_persisted_before_submit():
    kis, repo = FakeKIS(), FakeRepository()
    original = kis.buy_stock_limit
    def checked(*args):
        assert repo.intents
        return original(*args)
    kis.buy_stock_limit = checked
    run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="practice")


def test_accepted_unfilled_and_restart_do_not_consume_or_retry():
    kis, repo = FakeKIS(fill_qty=0), FakeRepository()
    first = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="practice")
    assert first.state.units_used == 0 and len(kis.orders) == 1
    second = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="practice")
    assert len(kis.orders) == 1 and second.decision.action == Action.WAIT


def test_confirmed_buy_updates_only_confirmed_fill():
    kis, repo = FakeKIS(fill_qty=100), FakeRepository()
    result = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="practice")
    assert result.state.units_used == 1 and result.state.core_units_used == 1
    assert result.state.core_filled_notional == 10_000 and result.state.last_buy_price == 100


def test_sell_acceptance_partial_and_final_completion():
    kis = FakeKIS(qty=150, average=100, price=110, fill_qty=110)
    repo = FakeRepository(active())
    partial = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="practice")
    assert partial.decision.action == Action.SELL_ALL and partial.state.status == Status.EXIT_PENDING and kis.qty == 40
    # A subsequent tick reconciles the pending sell and cannot buy.
    again = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="practice")
    assert again.decision.action == Action.WAIT and len(kis.orders) == 1
    kis.fill_qty = 150; kis.qty = 0
    done = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="practice")
    assert done.state.status == Status.COMPLETE and done.state.last_exit_date == DAY


def test_unowned_existing_position_freezes_without_order():
    kis, repo = FakeKIS(qty=10, average=100), FakeRepository()
    result = run_once(config=config(), kis=kis, repository=repo, regime_provider=REGIME, trade_date=DAY, kis_env="practice")
    assert result.decision.reason == "KR_INF_UNOWNED_EXISTING_POSITION" and not kis.orders
