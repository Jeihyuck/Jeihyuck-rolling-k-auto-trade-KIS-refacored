"""PR49 Korean market-state overlay: index breadth, sector rotation, account risk, caps."""
from __future__ import annotations
import logging, os
from typing import Any

from trader.kr.forbidden_products import is_forbidden_kr_product, BLOCK_REASON
from trader.kr.sector_classifier import classify_kr_sector
from trader.kr.regime import KRRegimeSnapshot, normalize_kr_market

logger = logging.getLogger(__name__)

def _f(k,d):
    try: return float(os.getenv(k, str(d)))
    except Exception: return d

def normalize_rs_percentile(value: Any) -> float:
    try:
        rs = float(value or 0.0)
    except Exception:
        return 0.0
    return rs / 100.0 if rs > 1.0 else rs

def _state_sector_cap(state: str) -> float:
    if state in {"KR_RISK_ON", "KR_STRONG_RISK_ON"}:
        return _f("KR_MAX_SECTOR_EXPOSURE_RISK_ON", 0.45)
    if state in {"KR_DEFENSE_RISK_OFF", "KR_DEFENSE_CRASH"}:
        return _f("KR_MAX_SECTOR_EXPOSURE_RISK_OFF", 0.20)
    return _f("KR_MAX_SECTOR_EXPOSURE_NORMAL", 0.35)

def _state_high_beta_cap(state: str) -> float:
    if state in {"KR_RISK_ON", "KR_STRONG_RISK_ON"}:
        return _f("KR_MAX_HIGH_BETA_EXPOSURE_RISK_ON", 0.55)
    if state == "KR_DEFENSE_CAUTION":
        return _f("KR_MAX_HIGH_BETA_EXPOSURE_CAUTION", 0.25)
    if state in {"KR_DEFENSE_RISK_OFF", "KR_DEFENSE_CRASH"}:
        return _f("KR_MAX_HIGH_BETA_EXPOSURE_RISK_OFF", 0.15)
    return _f("KR_MAX_HIGH_BETA_EXPOSURE_NORMAL", 0.45)

def calculate_kr_sector_exposure(*, positions: list[dict], candidate_orders: list[dict] | None, equity_krw: float) -> dict:
    exposure={}; high=unknown=0.0; eq=max(float(equity_krw or 0),1.0)
    for row in list(positions or []) + list(candidate_orders or []):
        side=str(row.get("side") or row.get("action") or "BUY").upper()
        if side == "SELL": continue
        cls=classify_kr_sector(row); sector=cls["sector_cluster"]
        val=float(row.get("market_value_krw") or row.get("notional") or row.get("order_value") or row.get("value") or 0)
        exposure[sector]=exposure.get(sector,0.0)+val/eq
        if cls["is_high_beta"]: high += val/eq
        if sector == "UNKNOWN": unknown += val/eq
    return {"sector_exposure_pct": exposure, "high_beta_exposure_pct": high, "unknown_exposure_pct": unknown}

def filter_kr_entry_intent(intent: dict, overlay: dict, *, positions: list[dict] | None=None) -> dict:
    side=str(intent.get("side") or intent.get("action") or "").upper()
    out=dict(intent)
    if side != "BUY": return out
    state = str(overlay.get("market_state") or "KR_NORMAL")
    cls=classify_kr_sector(out); cluster=cls["sector_cluster"]
    equity=float(overlay.get("portfolio_equity_krw") or out.get("portfolio_equity_krw") or 0.0)
    notional=float(out.get("notional") or out.get("order_value") or out.get("planned_value") or out.get("planned_cap") or 0.0)
    add_pct=(notional / equity) if equity > 0 and notional > 0 else 0.0
    sector_exposure=dict(overlay.get("sector_exposure_pct") or {})
    sector_after=float(sector_exposure.get(cluster) or 0.0) + add_pct
    high_beta_after=float(overlay.get("high_beta_exposure_pct") or 0.0) + (add_pct if cls["is_high_beta"] else 0.0)
    unknown_after=float(sector_exposure.get("UNKNOWN") or 0.0) + (add_pct if cluster == "UNKNOWN" else 0.0)
    gross_after=float(overlay.get("gross_exposure_pct") or 0.0) + add_pct
    reason = ""
    if overlay.get("data_quality") == "BLOCKED": reason="KR_REGIME_DATA_BLOCKED"
    elif str(out.get("market") or out.get("market_code") or "").upper() == "UNKNOWN": reason="KR_MARKET_UNKNOWN_ENTRY_BLOCK"
    elif is_forbidden_kr_product(row=out): reason=BLOCK_REASON
    elif overlay.get("force_entry_block"): reason="KR_DEFENSE_CRASH_ENTRY_BLOCK"
    elif gross_after >= _f("KR_MAX_GROSS_EXPOSURE_PCT",0.95): reason="KR_MAX_GROSS_EXPOSURE_BLOCK"
    else:
        # Add-to-existing/single-position cap. Loss averaging stays disallowed by default.
        code=str(out.get("code") or out.get("symbol") or "")
        existing_value=0.0; existing_loss=False
        for p in positions or []:
            if str(p.get("code") or p.get("symbol") or "") == code:
                existing_value += float(p.get("market_value_krw") or p.get("market_value") or p.get("total_cost") or 0.0)
                existing_loss = existing_loss or float(p.get("unrealized_pnl_pct") or p.get("return_pct") or 0.0) < 0
        if existing_loss:
            reason="KR_LOSS_AVERAGING_BLOCK"
        elif equity > 0 and (existing_value + notional) / equity > _f("KR_MAX_SINGLE_POSITION_PCT",0.10):
            reason="KR_SINGLE_POSITION_CAP_BLOCK"
        elif cluster == "UNKNOWN" and unknown_after > _f("KR_MAX_UNKNOWN_SECTOR_EXPOSURE",0.15):
            reason="KR_UNKNOWN_SECTOR_CAP_BLOCK"
        elif sector_after > _state_sector_cap(state):
            reason="KR_SECTOR_CAP_BLOCK"
        elif cls["is_high_beta"] and high_beta_after > _state_high_beta_cap(state):
            reason="KR_HIGH_BETA_CAP_BLOCK"
        elif state=="KR_DEFENSE_RISK_OFF" and cls["is_high_beta"]:
            reason="KR_DEFENSE_RISK_OFF_HIGH_BETA_BLOCK"
        elif state=="KR_DEFENSE_RISK_OFF" and cluster in {"BIO_HEALTHCARE","SECONDARY_BATTERY"}:
            reason="KR_DEFENSE_RISK_OFF_GROWTH_BLOCK"
        elif state=="KR_DEFENSE_CAUTION" and cls["is_high_beta"] and normalize_rs_percentile(out.get("rs_percentile") or out.get("rs_pctile")) < 0.85:
            reason="KR_DEFENSE_CAUTION_ENTRY_REDUCED"
    if not reason:
        return out
    out.update({"status":"BLOCKED","reason":reason,"blocked_reason":reason,"market_state":overlay.get("market_state")})
    if reason == BLOCK_REASON:
        logger.info("[KR_FORBIDDEN_PRODUCT][BLOCK] symbol=%s name=%s side=BUY reason=%s", out.get("code") or out.get("symbol"), out.get("name"), reason)
    logger.info("[KR_MARKET_STATE][ENTRY_BLOCK] symbol=%s name=%s cluster=%s reason=%s market_state=%s sector_exposure_pct=%.4f high_beta_exposure_pct=%.4f", out.get("code") or out.get("symbol"), out.get("name"), cluster, reason, overlay.get("market_state"), sector_after, high_beta_after)
    return out

def generate_kr_profit_capture_intents(positions: list[dict], overlay: dict) -> list[dict]:
    if os.getenv("KR_PROFIT_CAPTURE_ENABLE","1") == "0": return []
    result=[]
    levels=[("kr_tp3_done","KR_TAKE_PROFIT_TP3",_f("KR_TP3_PCT",0.08),_f("KR_TP3_SELL_PCT",0.20)),("kr_tp2_done","KR_TAKE_PROFIT_TP2",_f("KR_TP2_PCT",0.05),_f("KR_TP2_SELL_PCT",0.25)),("kr_tp1_done","KR_TAKE_PROFIT_TP1",_f("KR_TP1_PCT",0.03),_f("KR_TP1_SELL_PCT",0.25))]
    for p in positions or []:
        pnl=float(p.get("unrealized_pnl_pct") or p.get("return_pct") or 0); qty=int(p.get("orderable_qty") or p.get("qty") or 0); meta=p.get("meta") or p.get("position_meta") or {}
        for flag,reason,thr,sell_pct in levels:
            if pnl >= thr and not meta.get(flag) and qty>0:
                sell_qty=max(1,int(qty*sell_pct)); runner_min=int(qty*_f("KR_RUNNER_MIN_REMAIN_PCT",0.40))
                if overlay.get("market_state") not in {"KR_DEFENSE_RISK_OFF","KR_DEFENSE_CRASH"}: sell_qty=min(sell_qty, max(1, qty-runner_min))
                result.append({"side":"SELL","code":p.get("code") or p.get("symbol"),"qty":sell_qty,"reason":reason,"market_state":overlay.get("market_state")})
                logger.info("[KR_PROFIT_CAPTURE][%s] symbol=%s qty=%s pnl_pct=%.4f market_state=%s", reason.rsplit("_", 1)[-1], p.get("code") or p.get("symbol"), sell_qty, pnl, overlay.get("market_state"))
                break
    return result

def generate_kr_defense_trim_intents(positions: list[dict], snapshot: KRRegimeSnapshot, *, account_kill_switch: bool=False, existing_sell_symbols: set[str] | None=None) -> list[dict]:
    maxn=int(_f("KR_DEFENSE_MAX_TRIM_SYMBOLS_PER_TICK",3)); existing_sell_symbols=existing_sell_symbols or set(); out=[]
    for p in positions or []:
        code=str(p.get("code") or p.get("symbol") or "")
        if code in existing_sell_symbols: continue
        market=normalize_kr_market(p.get("market") or p.get("market_code"))
        local=snapshot.market_states.get(market)
        if not account_kill_switch and (market == "UNKNOWN" or local is None):
            logger.warning("[KR_DEFENSE][TRIM_DECISION] symbol=%s market=%s result=SKIP reason=unknown_position_market", code, market); continue
        if not account_kill_switch and local.data_quality == "BLOCKED":
            logger.info("[KR_DEFENSE][TRIM_DECISION] symbol=%s market=%s local_state=%s local_quality=%s result=SKIP reason=data_missing_is_not_crash_signal", code, market, local.state, local.data_quality); continue
        state = "KR_DEFENSE_CRASH" if account_kill_switch else local.state
        if state not in {"KR_DEFENSE_RISK_OFF","KR_DEFENSE_CRASH"}:
            logger.info("[KR_DEFENSE][TRIM_DECISION] symbol=%s market=%s local_state=%s local_quality=%s result=SKIP reason=local_market_not_defensive", code, market, state, local.data_quality if local else "ACCOUNT"); continue
        pct=_f("KR_DEFENSE_TRIM_PCT_CRASH",0.50) if state=="KR_DEFENSE_CRASH" else _f("KR_DEFENSE_TRIM_PCT_RISK_OFF",0.30)
        cls=classify_kr_sector(p); weak=float(p.get("unrealized_pnl_pct") or 0)<0 or cls["is_high_beta"] or state=="KR_DEFENSE_CRASH"
        if weak:
            qty=int(p.get("orderable_qty") or p.get("qty") or 0); sell_qty=max(1,int(qty*pct)) if qty>0 else 0
            if sell_qty>0:
                trim_qty = min(sell_qty, qty-1 if os.getenv("KR_DEFENSE_DO_NOT_FULL_LIQUIDATE_INTRADAY","1")!="0" and qty>1 else qty)
                reason = "KR_DEFENSE_CRASH_TRIM" if state=="KR_DEFENSE_CRASH" else "KR_DEFENSE_RISK_OFF_TRIM"
                out.append({"side":"SELL","code":code,"qty":trim_qty,"reason":reason,"market_state":state})
                logger.info("[KR_DEFENSE][TRIM_DECISION] symbol=%s market=%s local_state=%s local_quality=%s result=TRIM reason=%s qty=%s", code, market, state, local.data_quality if local else "ACCOUNT", reason, trim_qty)
        if len(out)>=maxn: break
    return out
