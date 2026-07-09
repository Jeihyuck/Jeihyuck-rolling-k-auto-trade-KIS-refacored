"""PR49 Korean market-state overlay: index breadth, sector rotation, account risk, caps."""
from __future__ import annotations
import logging, os
from typing import Any

from trader.kr.account_risk import evaluate_kr_account_risk
from trader.kr.forbidden_products import is_forbidden_kr_product, BLOCK_REASON
from trader.kr.sector_classifier import classify_kr_sector, summarize_final30_clusters, KR_HIGH_BETA_CLUSTERS, KR_DEFENSIVE_CLUSTERS
from trader.kr.sector_rotation import evaluate_kr_sector_rotation

logger = logging.getLogger(__name__)

STATES = ("KR_DEFENSE_CRASH","KR_DEFENSE_RISK_OFF","KR_DEFENSE_CAUTION","KR_NORMAL","KR_RISK_ON","KR_STRONG_RISK_ON")
MULT_ENV = {"KR_DEFENSE_CRASH":"KR_DEFENSE_CRASH_MULT","KR_DEFENSE_RISK_OFF":"KR_DEFENSE_RISK_OFF_MULT","KR_DEFENSE_CAUTION":"KR_DEFENSE_CAUTION_MULT","KR_NORMAL":"KR_NORMAL_MULT","KR_RISK_ON":"KR_RISK_ON_MULT","KR_STRONG_RISK_ON":"KR_STRONG_RISK_ON_MULT"}
MULT_DEFAULT = {"KR_DEFENSE_CRASH":0.0,"KR_DEFENSE_RISK_OFF":0.20,"KR_DEFENSE_CAUTION":0.50,"KR_NORMAL":1.0,"KR_RISK_ON":1.10,"KR_STRONG_RISK_ON":1.25}
TRAIL = {"KR_DEFENSE_CRASH":("kr_crash_tight","KR_TRAIL_CRASH_PCT",0.008),"KR_DEFENSE_RISK_OFF":("kr_risk_off_tight","KR_TRAIL_RISK_OFF_PCT",0.010),"KR_DEFENSE_CAUTION":("kr_caution","KR_TRAIL_CAUTION_PCT",0.015),"KR_NORMAL":("kr_normal","KR_TRAIL_NORMAL_PCT",0.020),"KR_RISK_ON":("kr_risk_on","KR_TRAIL_RISK_ON_PCT",0.025),"KR_STRONG_RISK_ON":("kr_strong_risk_on","KR_TRAIL_STRONG_RISK_ON_PCT",0.030)}

def _f(k,d):
    try: return float(os.getenv(k, str(d)))
    except Exception: return d

def _v(d,*keys):
    for k in keys:
        if d and d.get(k) is not None: return float(d[k])
    return None

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

def _pct_from_provider(provider, symbol, trade_date, lb):
    for name in ("get_index_return","get_return","return_pct"):
        fn=getattr(provider,name,None)
        if callable(fn):
            try: return float(fn(symbol=symbol, trade_date=trade_date, lookback=lb))
            except TypeError:
                try: return float(fn(symbol, trade_date, lb))
                except Exception: pass
            except Exception: pass
    data=getattr(provider,"returns",None) or getattr(provider,"data",None) or {}
    for key in ((symbol,lb),(symbol,f"return_{lb}d"),f"{symbol}_{lb}d"):
        if key in data: return float(data[key])
    return None

def _index_returns(trade_date, provider, ctx):
    ctx=dict(ctx or {}); warn=[]
    symbols={"kospi":os.getenv("KR_INDEX_KOSPI_SYMBOL","KOSPI"),"kosdaq":os.getenv("KR_INDEX_KOSDAQ_SYMBOL","KOSDAQ"),"kospi200":os.getenv("KR_INDEX_KOSPI200_PROXY","KOSPI200"),"kosdaq150":os.getenv("KR_INDEX_KOSDAQ150_PROXY","229200")}
    out={}
    for name,sym in symbols.items():
        for lb in (1,3):
            key=f"{name}_{lb}d_return"
            val=ctx.get(key)
            if val is None: val=_pct_from_provider(provider, sym, trade_date, lb)
            out[key]=val
            if val is None: warn.append(f"missing_{key}")
    out["kosdaq_vs_kospi_1d"] = None if out["kosdaq_1d_return"] is None or out["kospi_1d_return"] is None else out["kosdaq_1d_return"]-out["kospi_1d_return"]
    out["kosdaq150_vs_kospi200_1d"] = None if out["kosdaq150_1d_return"] is None or out["kospi200_1d_return"] is None else out["kosdaq150_1d_return"]-out["kospi200_1d_return"]
    out["kosdaq_vs_kospi_3d"] = None if out["kosdaq_3d_return"] is None or out["kospi_3d_return"] is None else out["kosdaq_3d_return"]-out["kospi_3d_return"]
    out["kosdaq150_vs_kospi200_3d"] = None if out["kosdaq150_3d_return"] is None or out["kospi200_3d_return"] is None else out["kosdaq150_3d_return"]-out["kospi200_3d_return"]
    quality = "ok" if not warn else ("usable" if len(warn) <= 3 else "degraded")
    return out, quality, warn

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

def _final30_quality(final30_rows, scanner_context):
    s=summarize_final30_clusters(final30_rows)
    sc=scanner_context or {}
    scanned=max(1,int(sc.get("scanned") or len(final30_rows or []) or 1))
    vol=float(sc.get("final30_vol_fail_ratio") if sc.get("final30_vol_fail_ratio") is not None else sc.get("vol_contraction_fail",0)/scanned)
    ma20=float(sc.get("final30_ma20_fail_ratio") if sc.get("final30_ma20_fail_ratio") is not None else sc.get("ma20_fail",0)/scanned)
    stress=bool(sc.get("final30_quality_stress")) or vol >= _f("PB1_KR_STRESS_VOL_FAIL_RATIO",0.70) or ma20 >= _f("PB1_KR_STRESS_MA20_FAIL_RATIO",0.40)
    return s | {"final30_vol_fail_ratio":vol,"final30_ma20_fail_ratio":ma20,"final30_quality_stress":stress,"final30_top10_avg_score": sc.get("final30_top10_avg_score"),"final30_avg_rs_percentile": sc.get("final30_avg_rs_percentile")}

def evaluate_kr_market_state(*, trade_date: str, provider, index_context: dict | None=None, sector_context: dict | None=None, prep_result: dict | None=None, final30_rows: list[dict] | None=None, scanner_context: dict | None=None, positions: list[dict] | None=None, account_snapshot: dict | None=None, now=None) -> dict:
    idx, data_quality, warnings = _index_returns(trade_date, provider, index_context)
    rotation = dict(sector_context or {}) if sector_context and "rotation_regime" in sector_context else evaluate_kr_sector_rotation(trade_date=trade_date, provider=provider, final30_rows=final30_rows, index_context=idx, positions=positions)
    fq=_final30_quality(final30_rows or [], scanner_context)
    acct=evaluate_kr_account_risk(account_snapshot)
    equity=float((account_snapshot or {}).get("portfolio_equity_krw") or 0)
    sector_exp=calculate_kr_sector_exposure(positions=positions or [], candidate_orders=None, equity_krw=equity) if equity>0 else {"sector_exposure_pct":{},"high_beta_exposure_pct":None,"unknown_exposure_pct":0}
    if (account_snapshot or {}).get("sector_exposure_pct"):
        sector_exp["sector_exposure_pct"] = dict((account_snapshot or {}).get("sector_exposure_pct") or {})
    if (account_snapshot or {}).get("high_beta_exposure_pct") is not None:
        sector_exp["high_beta_exposure_pct"] = float((account_snapshot or {}).get("high_beta_exposure_pct") or 0.0)
    gross=(account_snapshot or {}).get("gross_exposure_pct")
    if gross is None and equity>0: gross=float((account_snapshot or {}).get("invested_market_value_krw") or 0)/equity
    reasons=[]; state="KR_NORMAL"
    k1,q1,kp1,q1501 = (_v(idx,"kospi_1d_return"),_v(idx,"kosdaq_1d_return"),_v(idx,"kospi200_1d_return"),_v(idx,"kosdaq150_1d_return"))
    def any_le(vals): return any(v is not None and v <= t for v,t,n in vals)
    crash = any_le([(k1,-0.020,"kospi"),(q1,-0.030,"kosdaq"),(kp1,-0.020,"kospi200"),(q1501,-0.035,"kosdaq150")]) or (k1 is not None and q1 is not None and k1<=-0.015 and q1<=-0.020) or acct["account_loss_kill_switch_level"]=="KR_DEFENSE_CRASH"
    if crash: state="KR_DEFENSE_CRASH"; reasons.append("crash_threshold_or_account_kill")
    else:
        riskoff = any_le([(k1,-0.012,"kospi"),(q1,-0.018,"kosdaq"),(kp1,-0.012,"kospi200"),(q1501,-0.025,"kosdaq150")]) or (idx.get("kosdaq150_vs_kospi200_3d") is not None and idx["kosdaq150_vs_kospi200_3d"]<=-0.015) or rotation.get("rotation_regime") in {"KR_BROAD_RISK_OFF","KR_HIGH_BETA_OFF"} or fq["final30_vol_fail_ratio"]>=0.70 or fq["final30_ma20_fail_ratio"]>=0.50 or acct["account_loss_kill_switch_level"]=="KR_DEFENSE_RISK_OFF"
        if riskoff: state="KR_DEFENSE_RISK_OFF"; reasons.append("risk_off_threshold")
        else:
            caution = any_le([(k1,-0.007,"kospi"),(q1,-0.010,"kosdaq"),(kp1,-0.007,"kospi200"),(q1501,-0.015,"kosdaq150")]) or (idx.get("kosdaq150_vs_kospi200_1d") is not None and idx["kosdaq150_vs_kospi200_1d"]<=-0.010) or fq["final30_quality_stress"] or fq["sector_context_suspect"] or rotation.get("rotation_context_suspect") or acct["account_loss_kill_switch_level"]=="KR_DEFENSE_CAUTION"
            valid_proxy = any(q in {"high","medium"} for q in (rotation.get("sector_proxy_quality") or {}).values())
            risk_on_ok = data_quality in {"ok","usable"} and not fq["final30_quality_stress"] and not fq["sector_context_suspect"] and bool(rotation.get("sector_leaders")) and valid_proxy and not acct["account_loss_kill_switch_triggered"]
            if caution: state="KR_DEFENSE_CAUTION"; reasons.append("caution_threshold")
            elif risk_on_ok and ((k1 or 0)>0 or (q1 or 0)>0):
                strong = data_quality=="ok" and (idx.get("kospi_3d_return") or 0)>0 and (idx.get("kospi200_3d_return") or 0)>0 and ((idx.get("kosdaq_3d_return") or 0)>0 or rotation.get("kospi_value_leadership"))
                state="KR_STRONG_RISK_ON" if strong else "KR_RISK_ON"; reasons.append("risk_on_broad_and_sector_leadership")
    leaders=set(rotation.get("sector_leaders") or [])
    kosdaq_weak = (q1 is not None and q1 <= -0.010) or (q1501 is not None and q1501 <= -0.015)
    kospi_sector_strong = (kp1 is not None and kp1 > 0) and bool(leaders & {"FINANCIAL","AUTO","SEMICONDUCTOR","DEFENSIVE_CONSUMER","TELECOM_UTILITY"})
    if state == "KR_DEFENSE_RISK_OFF" and kosdaq_weak and kospi_sector_strong and not crash:
        state="KR_DEFENSE_CAUTION"; reasons.append("kosdaq_weak_but_kospi_leadership_no_full_block")
    if (account_snapshot or {}).get("gross_exposure_pct") is not None and float((account_snapshot or {}).get("gross_exposure_pct")) >= _f("KR_MAX_GROSS_EXPOSURE_PCT",0.95):
        reasons.append("gross_exposure_cap_blocks_new_buy")
    mult=_f(MULT_ENV[state], MULT_DEFAULT[state]); mode, env, d=TRAIL[state]; trail=_f(env,d)
    allow_new = state != "KR_DEFENSE_CRASH" and not ((gross is not None) and float(gross) >= _f("KR_MAX_GROSS_EXPOSURE_PCT",0.95))
    out={"market_state":state,"defense_regime":state.startswith("KR_DEFENSE"),"risk_on_regime":state in {"KR_RISK_ON","KR_STRONG_RISK_ON"},"market_state_reasons":reasons or ["normal"],"exposure_multiplier":mult,"effective_budget_before_overlay":None,"effective_budget_after_overlay":None,"allow_new_buy":allow_new,"allow_add_to_existing":state in {"KR_RISK_ON","KR_STRONG_RISK_ON"},"allow_growth_buy":state not in {"KR_DEFENSE_CRASH","KR_DEFENSE_RISK_OFF"},"allow_high_beta_buy":state in {"KR_RISK_ON","KR_STRONG_RISK_ON"},"allow_defensive_buy":state != "KR_DEFENSE_CRASH","allow_semiconductor_buy": state not in {"KR_DEFENSE_CRASH"} and (state not in {"KR_DEFENSE_RISK_OFF","KR_DEFENSE_CAUTION"} or "SEMICONDUCTOR" in leaders),"allow_bio_buy":state in {"KR_RISK_ON","KR_STRONG_RISK_ON"} and "BIO_HEALTHCARE" in leaders,"allow_secondary_battery_buy":state in {"KR_RISK_ON","KR_STRONG_RISK_ON"} and "SECONDARY_BATTERY" in leaders,"allow_financial_buy": state not in {"KR_DEFENSE_CRASH"} and (state not in {"KR_DEFENSE_RISK_OFF","KR_DEFENSE_CAUTION"} or "FINANCIAL" in leaders),"allow_auto_buy": state not in {"KR_DEFENSE_CRASH"} and (state not in {"KR_DEFENSE_RISK_OFF","KR_DEFENSE_CAUTION"} or "AUTO" in leaders),"force_entry_block":state=="KR_DEFENSE_CRASH","trim_required":state in {"KR_DEFENSE_CRASH","KR_DEFENSE_RISK_OFF"},"profit_capture_enabled":os.getenv("KR_PROFIT_CAPTURE_ENABLE","1")!="0","trailing_stop_mode":mode,"trailing_stop_pct":trail,"stop_tightening_level":state.lower(),"account_loss_kill_switch_triggered":acct["account_loss_kill_switch_triggered"],"account_loss_kill_switch_level":acct["account_loss_kill_switch_level"],"data_quality":data_quality,"data_quality_warnings":warnings+rotation.get("rotation_warnings",[]),"index_returns":idx,"relative_strength":{"kosdaq_vs_kospi_1d":idx.get("kosdaq_vs_kospi_1d"),"kosdaq150_vs_kospi200_3d":idx.get("kosdaq150_vs_kospi200_3d")},"sector_strength":rotation.get("sector_strength",{}),"sector_proxy_quality":rotation.get("sector_proxy_quality",{}),"sector_leaders":rotation.get("sector_leaders",[]),"sector_laggards":rotation.get("sector_laggards",[]),"final30_cluster_counts":fq["counts"],"final30_unknown_cluster_ratio":fq["unknown_ratio"],"final30_high_beta_ratio":fq["high_beta_ratio"],"final30_quality_stress":fq["final30_quality_stress"],"gross_exposure_pct":gross,"portfolio_equity_krw":equity,"sector_exposure_pct":sector_exp["sector_exposure_pct"],"high_beta_exposure_pct":sector_exp["high_beta_exposure_pct"],"forbidden_products":[BLOCK_REASON]}
    logger.info("[KR_MARKET_STATE][REGIME] market_state=%s defense_regime=%s risk_on_regime=%s reasons=%s kospi_1d=%s kosdaq_1d=%s kospi200_1d=%s kosdaq150_1d=%s rotation_regime=%s sector_leaders=%s exposure_multiplier=%.2f allow_new_buy=%s allow_add_to_existing=%s allow_high_beta_buy=%s force_entry_block=%s data_quality=%s data_quality_warnings=%s", state, out["defense_regime"], out["risk_on_regime"], out["market_state_reasons"], k1, q1, kp1, q1501, rotation.get("rotation_regime"), out["sector_leaders"], mult, out["allow_new_buy"], out["allow_add_to_existing"], out["allow_high_beta_buy"], out["force_entry_block"], data_quality, out["data_quality_warnings"])
    return out

def apply_kr_market_state_to_budget(effective_budget: float, overlay: dict, *, legacy_kr_stress_guard_triggered: bool=False) -> float:
    before=float(effective_budget or 0); after=before*float(overlay.get("exposure_multiplier") or 0)
    if legacy_kr_stress_guard_triggered: after=min(after, before*_f("PB1_KR_STRESS_TICK_BUDGET_PCT",0.15))
    overlay["effective_budget_before_overlay"]=before; overlay["effective_budget_after_overlay"]=after
    logger.info("[KR_MARKET_STATE][BUDGET] effective_budget_before_overlay=%.0f exposure_multiplier=%.2f effective_budget_after_overlay=%.0f legacy_stress_budget_applied=%s gross_exposure_pct=%s cash_krw=%s", before, overlay.get("exposure_multiplier"), after, int(legacy_kr_stress_guard_triggered), overlay.get("gross_exposure_pct"), None)
    return after

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
    if is_forbidden_kr_product(row=out): reason=BLOCK_REASON
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

def generate_kr_defense_trim_intents(positions: list[dict], overlay: dict, existing_sell_symbols: set[str] | None=None) -> list[dict]:
    state=overlay.get("market_state");
    if state not in {"KR_DEFENSE_RISK_OFF","KR_DEFENSE_CRASH"}: return []
    pct=_f("KR_DEFENSE_TRIM_PCT_CRASH",0.50) if state=="KR_DEFENSE_CRASH" else _f("KR_DEFENSE_TRIM_PCT_RISK_OFF",0.30)
    maxn=int(_f("KR_DEFENSE_MAX_TRIM_SYMBOLS_PER_TICK",3)); existing_sell_symbols=existing_sell_symbols or set(); out=[]
    for p in positions or []:
        code=str(p.get("code") or p.get("symbol") or "")
        if code in existing_sell_symbols: continue
        cls=classify_kr_sector(p); weak=float(p.get("unrealized_pnl_pct") or 0)<0 or cls["is_high_beta"] or state=="KR_DEFENSE_CRASH"
        if weak:
            qty=int(p.get("orderable_qty") or p.get("qty") or 0); sell_qty=max(1,int(qty*pct)) if qty>0 else 0
            if sell_qty>0:
                trim_qty = min(sell_qty, qty-1 if os.getenv("KR_DEFENSE_DO_NOT_FULL_LIQUIDATE_INTRADAY","1")!="0" and qty>1 else qty)
                reason = "KR_DEFENSE_CRASH_TRIM" if state=="KR_DEFENSE_CRASH" else "KR_DEFENSE_RISK_OFF_TRIM"
                out.append({"side":"SELL","code":code,"qty":trim_qty,"reason":reason,"market_state":state})
                logger.info("[KR_DEFENSE][TRIM] symbol=%s qty=%s reason=%s market_state=%s", code, trim_qty, reason, state)
        if len(out)>=maxn: break
    return out
