"""Production orchestration for the isolated KR Infinite sleeve."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import logging, os, uuid
from typing import Any, Callable

from trader.db.repos import OrdersRepo, FillsRepo
from trader.kr.regime_runtime import load_runtime_state
from .config import InfiniteConfig
from .models import CycleStatus, MarketInput, SleeveState
from .order_router import CanonicalOrderRouter
from .reconcile import rebuild_from_fills
from .repository import SleeveRepository, sleeve_lock
from .strategy import decide

logger = logging.getLogger(__name__)


def run_isolated(callback: Callable[[], object]) -> object | None:
    try: return callback()
    except Exception:
        logger.exception("[KR_INF][ERROR] sleeve_failed; existing KR session continues")
        return None


def enabled() -> bool:
    return os.getenv("KR_INFINITE_ENABLED", "1").strip().lower() in {"1","true","yes","on"}


def exclude_owned_symbol(symbols: list[str], active: bool) -> list[str]:
    return [s for s in symbols if not (active and str(s).zfill(6) == "122630")]


def run_production_sleeve(*, engine, kis, trade_date: date, run_id: str,
                          balance_snapshot: dict, now: datetime) -> dict:
    """Run exactly once using objects already owned by the active PB1 session."""
    config=InfiniteConfig.from_env()
    if not config.enabled: return {"enabled":False,"decision":"SKIP","reason":"SLEEVE_DISABLED"}
    runtime=load_runtime_state(str(trade_date),now=now)
    regime=(runtime.get("markets") or {}).get("KOSPI") or {}
    quote=kis.get_price_quote("122630",diag_mode=False,attempts=1)
    price=Decimal(str(quote.get("stck_prpr") or quote.get("price") or quote.get("last") or 0))
    quote_at=now
    positions=_positions(balance_snapshot)
    broker=next((p for p in positions if str(p.get("pdno") or p.get("code") or "").zfill(6)=="122630"),{})
    broker_qty=int(float(broker.get("hldg_qty") or broker.get("qty") or 0))
    broker_avg=Decimal(str(broker.get("pchs_avg_pric") or broker.get("avg_buy_price") or 0))
    cash=_cash(balance_snapshot)
    with sleeve_lock(engine) as conn:
        if conn is None: return {"enabled":True,"decision":"BLOCK","reason":"LOCK_UNAVAILABLE"}
        repo=SleeveRepository(conn); repo.ensure_schema(); raw=repo.load_state()
        if raw is None:
            raw={"strategy_id":"kr_kodex_infinite_v2","symbol":"122630","book":"KR_INFINITE",
                 "cycle_id":str(uuid.uuid4()),"cycle_status":"READY","policy_version":config.policy_version,
                 "filled_quantity":0,"authoritative_buy_notional":Decimal(0),"authoritative_sell_notional":Decimal(0),
                 "authoritative_average_price":Decimal(0),"used_unit_fraction":Decimal(0),"last_buy_trade_date":None,
                 "pending_order_key":None,"current_regime_state":None,"current_regime_score":0,
                 "data_quality":"BLOCKED","metadata":{"cycle_start_date":str(trade_date)}}
            repo.save_state(raw,commit=True)
        orders=repo.load_attributed_orders(str(raw["cycle_id"])); fills=repo.load_attributed_fills(str(raw["cycle_id"]))
        attributed_qty=sum(int(f.get("qty") or 0)*(1 if str(f.get("side")).upper()=="BUY" else -1) for f in fills)
        open_orders=[o for o in orders if not repo.pending_is_terminal(o)]
        conflict=broker_qty != attributed_qty
        avg_db=Decimal(str(raw.get("authoritative_average_price") or 0))
        avg_mismatch=bool(broker_qty and avg_db and abs(broker_avg-avg_db) > max(Decimal("1"),avg_db*Decimal("0.001")))
        status=CycleStatus.OWNERSHIP_CONFLICT if conflict else CycleStatus(str(raw["cycle_status"]))
        state=SleeveState(str(raw["cycle_id"]),status,broker_qty,Decimal(str(raw["authoritative_buy_notional"])),
            Decimal(str(raw["authoritative_sell_notional"])),broker_avg or avg_db,raw.get("last_buy_trade_date"),
            (raw.get("metadata") or {}).get("last_sell_date"),raw.get("pending_order_key") or (open_orders[0]["client_order_key"] if open_orders else None),
            not (conflict or avg_mismatch))
        market=MarketInput(trade_date,price,quote_at,str(regime.get("state") or "UNKNOWN"),trade_date,
            _runtime_at(runtime,now),str(regime.get("data_quality") or "BLOCKED"),Decimal(str(regime.get("score") or 0)),
            cash,bool(os.getenv("KILL_SWITCH","0")=="1"),True,bool(regime.get("recovery_confirmed")),bool(regime.get("long_trend_broken")))
        decision=decide(config,state,market,now)
        result={"sent":False,"reason":decision.reason}
        if decision.action in {"BUY","SELL"}:
            router=CanonicalOrderRouter(OrdersRepo(engine),lambda **kw: kis.buy_stock_market("122630",kw["qty"]) if kw["side"]=="BUY" else kis.sell_stock_market("122630",kw["qty"]),config)
            result=router.route(decision,state,trade_date,env=str(kis.env),run_id=run_id,price=float(price),regime=market.regime_state)
        logger.info("[KR_INF][RUN] enabled=1 cycle_id=%s regime=%s state=%s decision=%s order_result=%s reconcile=%s",
            state.cycle_id,market.regime_state,state.status.value,decision.action,result,"OK" if state.reconciled else "BLOCKED")
        return {"enabled":True,"cycle_id":state.cycle_id,"regime":market.regime_state,"state":state.status.value,
                "decision":decision.action,"order_result":result,"reconciliation":"OK" if state.reconciled else "BLOCKED"}


def _positions(snapshot):
    if not isinstance(snapshot,dict): return []
    value=snapshot.get("output1") or snapshot.get("positions") or []
    return value if isinstance(value,list) else []


def _cash(snapshot) -> Decimal:
    rows=snapshot.get("output2") if isinstance(snapshot,dict) else None
    row=rows[0] if isinstance(rows,list) and rows else rows if isinstance(rows,dict) else {}
    return Decimal(str(row.get("ord_psbl_cash") or row.get("dnca_tot_amt") or 0).replace(",",""))


def _runtime_at(runtime, fallback):
    try: return datetime.fromisoformat(str(runtime["updated_at_kst"]))
    except Exception: return fallback
