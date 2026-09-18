"""PR49 Korean market-state overlay: index breadth, sector rotation, account risk, caps."""
from __future__ import annotations
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib, json, logging, os
from typing import Any

from trader.kr.forbidden_products import is_forbidden_kr_product, BLOCK_REASON
from trader.kr.sector_classifier import classify_kr_sector
from trader.kr.regime import KRRegimeSnapshot, normalize_kr_market

logger = logging.getLogger(__name__)

KR_POLICY_MISSING_ADOPTION_VERSION = "kr_policy_missing_tp_adoption_v1"
KR_POLICY_MISSING_ADOPTION_SOURCE = "kr_policy_missing_profit_capture_adoption"


_ADOPTION_PRICE_QUANT = Decimal("0.000001")


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    raw = value
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = bytes(raw).decode("utf-8")
        except Exception:
            return {}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _canonical_adoption_price(value: Any) -> Decimal | None:
    try:
        out = Decimal(str(value)).quantize(_ADOPTION_PRICE_QUANT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return None
    return out if out.is_finite() else None


def _normalize_adoption_position(position: dict) -> dict:
    p = dict(position or {})
    for field in ("entry_exit_plan_json", "position_meta", "entry_meta_json"):
        p[field] = _json_object(p.get(field))
    plan = dict(p.get("entry_exit_plan_json") or {})
    for field in ("risk_plan", "profit_plan", "policy_adoption_contract"):
        plan[field] = _json_object(plan.get(field))
    contract = dict(plan.get("policy_adoption_contract") or {})
    for field in ("risk_plan", "profit_plan"):
        contract[field] = _json_object(contract.get(field))
    plan["policy_adoption_contract"] = contract
    p["entry_exit_plan_json"] = plan
    return p

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
    owner = str(out.get("owner_strategy") or out.get("strategy_owner") or out.get("sleeve_id") or ("KR_INFINITE" if str(out.get("code") or out.get("symbol") or "") == "122630" else "KR_STANDARD")).upper()
    if owner == "KR_INFINITE":
        out.setdefault("meta", {}).update({"owner_strategy": "KR_INFINITE", "overlay_bypass": True, "overlay_ignored_reason": "infinite_strategy_buy_dip"})
        logger.info("[KR_INF][OVERLAY_BYPASS] symbol=%s overlay=%s action=BUY_ALLOWED reason=infinite_strategy_buy_dip", out.get("code") or out.get("symbol"), overlay.get("market_state"))
        return out
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

def _kr_policy_missing_adoption_sha256(contract: dict) -> str:
    payload = {key: value for key, value in dict(contract or {}).items() if key != "sha256"}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def has_kr_policy_missing_adoption_claim(position: dict) -> bool:
    """Return whether a row claims this adoption, even if its proof is invalid."""
    p = _normalize_adoption_position(position)
    meta = p.get("position_meta") or {}
    plan = p.get("entry_exit_plan_json") or {}
    return bool(
        str(p.get("policy_version") or "") == KR_POLICY_MISSING_ADOPTION_VERSION
        or str(p.get("policy_source") or "") == KR_POLICY_MISSING_ADOPTION_SOURCE
        or str(plan.get("policy_version") or "") == KR_POLICY_MISSING_ADOPTION_VERSION
        or str(plan.get("policy_source") or "") == KR_POLICY_MISSING_ADOPTION_SOURCE
        or meta.get("policy_adopted") is True
        or str(meta.get("policy_adoption_version") or "") == KR_POLICY_MISSING_ADOPTION_VERSION
    )


def is_verified_kr_policy_missing_adoption(position: dict) -> bool:
    """Validate a durable adoption contract after real DB JSON/float round-trip."""
    p = _normalize_adoption_position(position)
    plan = dict(p.get("entry_exit_plan_json") or {})
    meta = dict(p.get("position_meta") or {})
    contract = dict(plan.get("policy_adoption_contract") or {})
    cycle_id = str(p.get("position_cycle_id") or "")
    epoch_id = str(p.get("portfolio_epoch_id") or "")
    digest = str(contract.get("sha256") or "")
    code = str(p.get("code") or p.get("symbol") or "").zfill(6)

    contract_price = _canonical_adoption_price(contract.get("entry_price"))
    row_price = _canonical_adoption_price(
        p.get("avg_buy_price") if p.get("avg_buy_price") not in (None, "")
        else p.get("avg") if p.get("avg") not in (None, "")
        else p.get("entry_price")
    )
    entry_price_matches = (
        contract_price is not None and row_price is not None and contract_price == row_price
    )

    ok = bool(
        cycle_id
        and epoch_id
        and str(p.get("policy_version") or "") == KR_POLICY_MISSING_ADOPTION_VERSION
        and str(p.get("policy_source") or "") == KR_POLICY_MISSING_ADOPTION_SOURCE
        and str(p.get("exit_policy_family") or "") == "SWING_STAGED_EXIT"
        and str(plan.get("policy_version") or "") == KR_POLICY_MISSING_ADOPTION_VERSION
        and str(plan.get("policy_source") or "") == KR_POLICY_MISSING_ADOPTION_SOURCE
        and str(plan.get("exit_policy_family") or "") == "SWING_STAGED_EXIT"
        and str(contract.get("version") or "") == KR_POLICY_MISSING_ADOPTION_VERSION
        and str(contract.get("code") or "").zfill(6) == code
        and str(contract.get("position_cycle_id") or "") == cycle_id
        and str(contract.get("portfolio_epoch_id") or "") == epoch_id
        and entry_price_matches
        and contract.get("risk_plan") == plan.get("risk_plan")
        and contract.get("profit_plan") == plan.get("profit_plan")
        and digest
        and digest == _kr_policy_missing_adoption_sha256(contract)
        and meta.get("policy_adopted") is True
        and str(meta.get("policy_adoption_version") or "") == KR_POLICY_MISSING_ADOPTION_VERSION
        and str(meta.get("policy_adoption_sha256") or "") == digest
    )
    if not ok:
        logger.error(
            "[KR_POLICY_ADOPTION][VERIFY_CORE] code=%s result=BLOCK cycle=%s epoch=%s "
            "contract_price=%s row_price=%s plan_type=%s meta_type=%s",
            code, cycle_id, epoch_id, contract_price, row_price,
            type((position or {}).get("entry_exit_plan_json")).__name__,
            type((position or {}).get("position_meta")).__name__,
        )
    return ok


def build_kr_policy_missing_adoption(position: dict, *, current_price: float | None = None) -> dict | None:
    """Build an explicit PB1 adoption contract for a profitable legacy holding.

    This is deliberately separate from the POLICY_MISSING exit router.  The
    caller must durably persist and read back this contract before any SELL is
    allowed.  KR Infinite/leveraged ownership is never eligible.
    """
    p = _normalize_adoption_position(position)
    meta = p.get("position_meta") or _json_object(p.get("meta")) or {}
    code = str(p.get("code") or p.get("symbol") or "").zfill(6)
    owner = str(p.get("owner_strategy") or meta.get("owner_strategy") or "KR_STANDARD").upper()
    cycle_id = str(p.get("position_cycle_id") or "")
    epoch_id = str(p.get("portfolio_epoch_id") or "")
    missing = any(str(v or "").upper() == "POLICY_MISSING" for v in (
        p.get("entry_thesis"), p.get("exit_policy_family"),
        (p.get("entry_exit_plan_json") or {}).get("exit_policy_family")
        if isinstance(p.get("entry_exit_plan_json"), dict) else None,
    ))
    if not missing or not cycle_id or not epoch_id or code == "122630" or "INFINITE" in owner:
        return None
    try:
        qty = int(p.get("orderable_qty") or p.get("qty") or 0)
        avg = float(p.get("avg_buy_price") or p.get("avg") or p.get("entry_price") or 0.0)
        mark = float(current_price or p.get("last_price") or p.get("current_price") or 0.0)
    except (TypeError, ValueError):
        return None
    if qty <= 0 or avg <= 0 or mark <= 0:
        return None
    return_fraction = (mark - avg) / avg
    tp1 = _f("KR_TP1_PCT", 0.03)
    if return_fraction < tp1:
        return None
    tp2, tp3 = _f("KR_TP2_PCT", 0.05), _f("KR_TP3_PCT", 0.08)
    stop_fraction = _f("KR_POLICY_MISSING_ADOPTION_STOP_PCT", 0.08)
    initial_stop = round(avg * (1.0 - stop_fraction), 2)
    plan = {
        "entry_thesis": "ADOPTED_LEGACY_HOLDING",
        "entry_style_selected": "LEGACY_POLICY_ADOPTION",
        "entry_reason": "POLICY_MISSING_PROFIT_CAPTURE_ADOPTION",
        "trade_horizon": "SWING",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "eod_action": "CARRY",
        "force_eod_close": False,
        "risk_plan": {"initial_stop": initial_stop, "risk_R": round(avg - initial_stop, 2)},
        "profit_plan": {
            "tp1": {"return_fraction": tp1, "sell_fraction": _f("KR_TP1_SELL_PCT", 0.25)},
            "tp2": {"return_fraction": tp2, "sell_fraction": _f("KR_TP2_SELL_PCT", 0.25)},
            "tp3": {"return_fraction": tp3, "sell_fraction": _f("KR_TP3_SELL_PCT", 0.20)},
        },
        "policy_source": KR_POLICY_MISSING_ADOPTION_SOURCE,
        "policy_version": KR_POLICY_MISSING_ADOPTION_VERSION,
    }
    adoption_contract = {
        "version": KR_POLICY_MISSING_ADOPTION_VERSION,
        "code": code,
        "position_cycle_id": cycle_id,
        "portfolio_epoch_id": epoch_id,
        "entry_price": avg,
        "risk_plan": plan["risk_plan"],
        "profit_plan": plan["profit_plan"],
    }
    adoption_contract["sha256"] = _kr_policy_missing_adoption_sha256(adoption_contract)
    plan["policy_adoption_contract"] = adoption_contract
    adopted_meta = {
        **meta,
        "book": "SWING_BOOK",
        "trade_horizon": "SWING_CARRY",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "policy_adopted": True,
        "policy_adopted_from": "POLICY_MISSING",
        "policy_adoption_version": KR_POLICY_MISSING_ADOPTION_VERSION,
        "policy_adoption_sha256": adoption_contract["sha256"],
        "policy_adoption_entry_price": avg,
        "policy_adoption_return_fraction": return_fraction,
        "entry_exit_plan": plan,
        "kr_tp1_done": bool(meta.get("kr_tp1_done")),
        "kr_tp2_done": bool(meta.get("kr_tp2_done")),
        "kr_tp3_done": bool(meta.get("kr_tp3_done")),
    }
    return {
        "plan": plan,
        "position_fields": {
            "entry_thesis": plan["entry_thesis"],
            "entry_style_selected": plan["entry_style_selected"],
            "entry_reason": plan["entry_reason"],
            "trade_horizon": plan["trade_horizon"],
            "exit_policy_family": plan["exit_policy_family"],
            "eod_action": plan["eod_action"],
            "force_eod_close": False,
            "initial_stop_price": initial_stop,
            "initial_risk_r": round(avg - initial_stop, 2),
            "entry_exit_plan_json": plan,
            "entry_meta_json": {
                "book": "SWING_BOOK",
                "trade_horizon": "SWING_CARRY",
                "exit_policy_family": "SWING_STAGED_EXIT",
                "entry_reason": plan["entry_reason"],
                "entry_style_selected": plan["entry_style_selected"],
                "policy_source": plan["policy_source"],
                "policy_version": plan["policy_version"],
            },
            "policy_source": plan["policy_source"],
            "policy_version": plan["policy_version"],
            "position_meta": adopted_meta,
        },
    }


def generate_kr_profit_capture_intents(positions: list[dict], overlay: dict) -> list[dict]:
    if os.getenv("KR_PROFIT_CAPTURE_ENABLE","1") == "0":
        return []
    result=[]
    default_levels=[
        ("kr_tp1_done","KR_TAKE_PROFIT_TP1",_f("KR_TP1_PCT",0.03),_f("KR_TP1_SELL_PCT",0.25)),
        ("kr_tp2_done","KR_TAKE_PROFIT_TP2",_f("KR_TP2_PCT",0.05),_f("KR_TP2_SELL_PCT",0.25)),
        ("kr_tp3_done","KR_TAKE_PROFIT_TP3",_f("KR_TP3_PCT",0.08),_f("KR_TP3_SELL_PCT",0.20)),
    ]
    for p in positions or []:
        code=str(p.get("code") or p.get("symbol") or "")
        raw_meta=p.get("meta") or p.get("position_meta") or {}
        if isinstance(raw_meta,str):
            try:
                raw_meta=json.loads(raw_meta)
            except Exception:
                raw_meta={}
        meta=raw_meta if isinstance(raw_meta,dict) else {}
        owner=str(p.get("owner_strategy") or meta.get("owner_strategy") or "KR_STANDARD").upper()
        if code.zfill(6) == "122630" or "INFINITE" in owner:
            continue

        raw_plan=p.get("entry_exit_plan_json") or meta.get("entry_exit_plan_json") or meta.get("entry_exit_plan")
        if isinstance(raw_plan,str):
            try:
                raw_plan=json.loads(raw_plan)
            except Exception:
                raw_plan={}
        plan=raw_plan if isinstance(raw_plan,dict) else {}

        adoption_verified=is_verified_kr_policy_missing_adoption(p)
        if plan and isinstance(plan.get("profit_plan"),dict) and "tp1_sell_pct" in plan["profit_plan"] and not adoption_verified:
            logger.info("[KR_PROFIT_CAPTURE][SKIP] symbol=%s reason=entry_exit_plan_authoritative",code)
            continue
        if has_kr_policy_missing_adoption_claim(p) and not adoption_verified:
            logger.warning("[KR_PROFIT_CAPTURE][BLOCK] symbol=%s reason=adoption_contract_unverified",code)
            continue

        levels=default_levels
        if adoption_verified:
            pp=plan.get("profit_plan") or {}
            adopted=[]
            for idx,(flag,reason,_,__) in enumerate(default_levels,start=1):
                stage=pp.get(f"tp{idx}") or {}
                if not isinstance(stage,dict):
                    adopted=[]
                    break
                adopted.append((flag,reason,float(stage.get("return_fraction") or 0.0),float(stage.get("sell_fraction") or 0.0)))
            if adopted:
                levels=adopted

        pnl=float(p.get("unrealized_pnl_pct") or p.get("return_pct") or 0)
        qty=int(p.get("orderable_qty") or p.get("qty") or 0)
        for index,(flag,reason,thr,sell_pct) in enumerate(levels):
            pending_flag=flag.replace("_done","_pending")
            prior_done=index == 0 or bool(meta.get(levels[index-1][0]))
            if not prior_done:
                break
            if pnl >= thr and not meta.get(flag) and not meta.get(pending_flag) and qty>0:
                sell_qty=max(1,int(qty*sell_pct))
                runner_min=int(qty*_f("KR_RUNNER_MIN_REMAIN_PCT",0.40))
                if overlay.get("market_state") not in {"KR_DEFENSE_RISK_OFF","KR_DEFENSE_CRASH"}:
                    sell_qty=min(sell_qty,max(1,qty-runner_min))
                result.append({
                    "side":"SELL","code":code,"qty":sell_qty,"reason":reason,
                    "profit_capture_stage":flag.replace("kr_","").replace("_done",""),
                    "market_state":overlay.get("market_state"),
                    "source_entry_contract_sha256":meta.get("entry_contract_sha256") or meta.get("policy_adoption_sha256"),
                    "exit_rule_source":"POLICY_ADOPTION_CONTRACT" if adoption_verified else "LEGACY_GLOBAL_TP",
                })
                logger.info(
                    "[KR_PROFIT_CAPTURE][%s] symbol=%s qty=%s pnl_pct=%.4f threshold=%.4f source=%s market_state=%s",
                    reason.rsplit("_",1)[-1],code,sell_qty,pnl,thr,
                    "POLICY_ADOPTION_CONTRACT" if adoption_verified else "LEGACY_GLOBAL_TP",
                    overlay.get("market_state"),
                )
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
