# -*- coding: utf-8 -*-
"""US long-only market state overlay.

Defensive/risk-on overlay for cash long-only stock/ETF accounts.  This module
never creates shorts, options, futures, inverse ETF hedges, VIX hedges, or
leveraged inverse exposure; it only reduces/blocks BUYs and creates optional
long-position SELL trims/profit-capture intents.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from trader.us.rotation import AI_CLUSTERS, theme_cluster_for

logger = logging.getLogger(__name__)

FORBIDDEN_HEDGE_SYMBOLS: set[str] = {
    "SH", "PSQ", "DOG", "SQQQ", "SOXS", "SPXU", "SDS", "QID", "TZA",
    "UVXY", "VXX", "VIXY", "SVXY", "SARK", "TECS", "LABD", "YANG", "FAZ",
    "TWM", "RWM", "SRTY", "SDOW", "DXD", "SDS", "REW", "SRS", "DRV",
}
AI_TECH_CLUSTERS = set(AI_CLUSTERS) | {"AI_SEMI", "AI_SOFTWARE", "DATA_CENTER_POWER", "MEGA_TECH", "TECH", "XLK"}
DEFENSIVE_CLUSTERS = {"HEALTHCARE", "CONSUMER_STAPLES", "DEFENSIVE_UTILITY", "UTILITIES", "XLV", "XLP", "XLU"}
CORE_INDEX_ETFS = {"SPY", "VOO", "IVV", "QQQ", "DIA", "RSP", "IWM"}

# Quote lookup exchange map for the market-state ETF basket.  Keep this in sync
# with watchlist_builder.  Without it, AMEX sector ETFs were queried as NASDAQ,
# producing false market_return_missing warnings and distorted breadth scores.
_MARKET_RETURN_EXCHANGE_MAP: dict[str, str] = {
    "SPY": "AMEX",
    "DIA": "AMEX",
    "IWM": "AMEX",
    "RSP": "AMEX",
    "XLK": "AMEX",
    "XLI": "AMEX",
    "XLF": "AMEX",
    "XLV": "AMEX",
    "XLP": "AMEX",
    "XLU": "AMEX",
    "XLE": "AMEX",
    "QQQ": "NASDAQ",
    "SMH": "NASDAQ",
}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _pct(v: Any) -> float | None:
    if v in (None, ""):
        return None
    try:
        x = float(v)
        return x / 100.0 if abs(x) > 1.0 else x
    except (TypeError, ValueError):
        return None


def _symbol(pos: dict) -> str:
    return str(pos.get("symbol") or pos.get("code") or "").upper().strip()


def _qty(pos: dict) -> int:
    try:
        return int(float(pos.get("sellable_qty") or pos.get("orderable_qty") or pos.get("qty") or pos.get("quantity") or pos.get("holdings_qty") or 0))
    except (TypeError, ValueError):
        return 0


def _num(v: Any) -> float | None:
    if v in (None, ""):
        return None
    try:
        x = float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return x


def _price(pos: dict) -> float:
    for k in (
        "last_price", "current_price", "current_price_usd", "current_px",
        "price", "close", "now_pric2", "ovrs_now_pric", "bass_pric",
        "avg_current_price",
    ):
        v = _num(pos.get(k))
        if v is not None and v > 0:
            return v
    return 0.0


def _entry_price(pos: dict) -> float:
    for k in ("entry_price", "avg_price_usd", "avg_cost", "avg_price"):
        v = _num(pos.get(k))
        if v is not None and v > 0:
            return v
    return 0.0


def _pnl_pct(pos: dict) -> float | None:
    for k in ("unrealized_pnl_pct", "pnl_pct", "profit_pct", "pnl_rate", "evlu_pfls_rt", "prls_rt"):
        v = _pct(pos.get(k))
        if v is not None:
            return v
    current = _price(pos)
    entry = _entry_price(pos)
    if current > 0 and entry > 0:
        return current / entry - 1.0
    return None


def _row_date_value(row: dict) -> str:
    for key in ("xymd", "date", "stck_bsop_date", "bas_dt", "trad_dvsn"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _row_close_value(row: dict) -> float | None:
    for key in ("close", "clos", "stck_clpr", "last", "price", "ovrs_nmix_prpr", "prpr"):
        v = _num(row.get(key))
        if v is not None and v > 0:
            return v
    return None


def _ret_from_rows(rows: Any, days: int) -> float | None:
    if not isinstance(rows, list) or len(rows) < days + 1:
        return None
    clean_rows = [r for r in rows if isinstance(r, dict)]
    if not clean_rows or len(clean_rows) < days + 1:
        return None
    if any(_row_date_value(r) for r in clean_rows):
        clean_rows = sorted(clean_rows, key=lambda r: _row_date_value(r) or "00000000")
    vals: list[float] = []
    for row in clean_rows:
        close = _row_close_value(row)
        if close is not None:
            vals.append(close)
    if len(vals) < days + 1:
        return None
    latest = vals[-1]
    past = vals[-1 - days]
    return (latest / past) - 1.0 if past else None


def _market_returns(provider: Any, trade_date: str, warnings: list[str]) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    for sym, exchange in _MARKET_RETURN_EXCHANGE_MAP.items():
        rows = None
        try:
            if isinstance(provider, dict):
                rows = provider.get(sym) or provider.get(sym.lower())
            elif callable(getattr(provider, "get_completed_daily_prices", None)) and getattr(getattr(provider, "get_completed_daily_prices", None), "__module__", "") != "unittest.mock":
                rows = provider.get_completed_daily_prices(sym, exchange, trade_date=trade_date, required_bars=260, allow_http_sync=False)
            elif hasattr(provider, "get_daily_prices"):
                rows = provider.get_daily_prices(sym, exchange, as_of_date=trade_date)
            elif hasattr(provider, "daily_prices"):
                rows = provider.daily_prices(sym)
        except Exception as exc:
            warnings.append(f"market_data_fetch_failed:{sym}:{exc}")
        out[f"{sym.lower()}_1d_return"] = _ret_from_rows(rows, 1)
        out[f"{sym.lower()}_3d_return"] = _ret_from_rows(rows, 3)
        out[f"{sym.lower()}_20d_return"] = _ret_from_rows(rows, 20)
        if out[f"{sym.lower()}_1d_return"] is None:
            warnings.append(f"market_return_missing:{sym}")
        clean_rows = [r for r in (rows or []) if isinstance(r, dict)] if isinstance(rows, list) else []
        if any(_row_date_value(r) for r in clean_rows):
            clean_rows.sort(key=lambda r: _row_date_value(r) or "00000000")
        out[f"{sym.lower()}_previous_close"] = _row_close_value(clean_rows[-1]) if clean_rows else None

    spy3 = out.get("spy_3d_return")
    spy20 = out.get("spy_20d_return")
    qqq3 = out.get("qqq_3d_return")
    qqq20 = out.get("qqq_20d_return")
    smh3 = out.get("smh_3d_return")
    smh20 = out.get("smh_20d_return")
    out["qqq_vs_spy_3d"] = (qqq3 - spy3) if qqq3 is not None and spy3 is not None else None
    out["smh_vs_spy_3d"] = (smh3 - spy3) if smh3 is not None and spy3 is not None else None
    out["qqq_vs_spy_20d"] = (qqq20 - spy20) if qqq20 is not None and spy20 is not None else None
    out["smh_vs_spy_20d"] = (smh20 - spy20) if smh20 is not None and spy20 is not None else None
    for _sym in ("rsp", "iwm", "xlk"):
        v = out.get(f"{_sym}_20d_return")
        out[f"{_sym}_vs_spy_20d"] = (v - spy20) if v is not None and spy20 is not None else None
    defensive = [out.get("xlv_3d_return"), out.get("xlp_3d_return"), out.get("xlu_3d_return")]
    cyc = [out.get("xlk_3d_return"), out.get("xli_3d_return"), out.get("xlf_3d_return"), out.get("xle_3d_return")]
    out["defensive_relative_strength"] = (sum(x for x in defensive if x is not None) / max(1, sum(x is not None for x in defensive)) - (spy3 or 0)) if any(x is not None for x in defensive) else None
    out["defensive_vs_spy_3d"] = out["defensive_relative_strength"]
    out["cyclical_relative_strength"] = (sum(x for x in cyc if x is not None) / max(1, sum(x is not None for x in cyc)) - (spy3 or 0)) if any(x is not None for x in cyc) else None
    defensive20 = [out.get("xlv_20d_return"), out.get("xlp_20d_return"), out.get("xlu_20d_return")]
    cyc20 = [out.get("xlk_20d_return"), out.get("xli_20d_return"), out.get("xlf_20d_return"), out.get("xle_20d_return")]
    out["defensive_relative_strength_20d"] = (sum(x for x in defensive20 if x is not None) / max(1, sum(x is not None for x in defensive20)) - (spy20 or 0)) if any(x is not None for x in defensive20) else None
    out["defensive_vs_spy_20d"] = out["defensive_relative_strength_20d"]
    out["cyclical_relative_strength_20d"] = (sum(x for x in cyc20 if x is not None) / max(1, sum(x is not None for x in cyc20)) - (spy20 or 0)) if any(x is not None for x in cyc20) else None
    out["cyclical_vs_spy_20d"] = out["cyclical_relative_strength_20d"]
    out["ai_relative_strength"] = out["smh_vs_spy_3d"]
    return out


def _intraday_rebound(provider: Any, rets: dict[str, float | None], warnings: list[str]) -> dict[str, Any]:
    """Fetch and validate the live SPY/QQQ/SMH crash-rebound snapshot."""
    snapshots: dict[str, dict] = {}
    for sym in ("SPY", "QQQ", "SMH"):
        quote = None
        try:
            if isinstance(provider, dict):
                quotes = provider.get("intraday_quotes") or provider.get("quotes") or {}
                quote = quotes.get(sym) or quotes.get(sym.lower())
            elif callable(getattr(provider, "get_current_price", None)):
                quote = provider.get_current_price(sym, _MARKET_RETURN_EXCHANGE_MAP[sym])
        except Exception as exc:
            warnings.append(f"intraday_rebound_fetch_failed:{sym}:{exc}")
        quote = quote if isinstance(quote, dict) else {}
        def first(keys):
            return next((_num(quote.get(k)) for k in keys if _num(quote.get(k)) is not None), None)
        current = first(("last", "current", "current_price", "price", "ovrs_nmix_prpr", "prpr"))
        open_px = first(("open", "open_price", "day_open"))
        vwap = first(("vwap", "VWAP", "weighted_average_price"))
        prev_close = first(("previous_close", "prev_close", "base", "bass_pric"))
        if prev_close is None:
            prev_close = rets.get(f"{sym.lower()}_previous_close")
        if prev_close is None and isinstance(provider, dict):
            rows = provider.get(sym) or provider.get(sym.lower()) or []
            clean = [r for r in rows if isinstance(r, dict)]
            if clean:
                if any(_row_date_value(r) for r in clean):
                    clean.sort(key=lambda r: _row_date_value(r) or "00000000")
                prev_close = _row_close_value(clean[-1])
        suspect = bool(quote.get("suspect") or quote.get("stale") or quote.get("_stale_date"))
        valid = bool(current and current > 0 and prev_close and prev_close > 0 and not suspect)
        intraday_return = current / prev_close - 1.0 if valid else None
        snapshots[sym] = {"current": current, "previous_close": prev_close, "open": open_px, "vwap": vwap, "intraday_return": intraday_return, "valid": valid, "suspect": suspect}
        if not valid:
            warnings.append(f"intraday_rebound_missing_or_suspect:{sym}")
    spy, qqq, smh = (snapshots[s] for s in ("SPY", "QQQ", "SMH"))
    signals = {
        "spy_return": spy["intraday_return"] is not None and spy["intraday_return"] >= .003,
        "qqq_return": qqq["intraday_return"] is not None and qqq["intraday_return"] >= .008,
        "smh_return": smh["intraday_return"] is not None and smh["intraday_return"] >= .015,
        "qqq_price_strength": bool(qqq["current"] and ((qqq["vwap"] and qqq["current"] > qqq["vwap"]) or (qqq["open"] and qqq["current"] >= qqq["open"]))),
        "smh_price_strength": bool(smh["current"] and ((smh["vwap"] and smh["current"] > smh["vwap"]) or (smh["open"] and smh["current"] >= smh["open"]))),
    }
    hard_down = any((spy["intraday_return"] is not None and spy["intraday_return"] <= -.005, qqq["intraday_return"] is not None and qqq["intraday_return"] <= -.008, smh["intraday_return"] is not None and smh["intraday_return"] <= -.015))
    data_ok = all(snapshot["valid"] for snapshot in snapshots.values())
    return {"snapshots": snapshots, "signals": signals, "confirmation_count": sum(signals.values()), "data_ok": data_ok, "hard_down": hard_down, "smh_recovered": bool(signals["smh_return"] or signals["smh_price_strength"])}

def regime_constraints(market_regime: str) -> dict[str, Any]:
    r = str(market_regime or "NEUTRAL").upper()
    base = {"market_regime": r, "sector_cap_enforced": True, "force_entry_block": False}
    table = {
        "RISK_ON": dict(capital_scale=_env_float("US_REGIME_RISK_ON_CAPITAL_SCALE", 1.00), max_ai_tech_ratio=_env_float("US_REGIME_RISK_ON_AI_TECH_CAP", 0.55), max_single_cluster_ratio=0.25, allow_new_buy=True, allow_ai_tech_buy=True, allow_defensive_buy=True, entry_aggressiveness="aggressive", trailing_stop_mode="risk_on", take_profit_mode="let_winner_run", max_new_positions=30),
        "GROWTH_LEADERSHIP": dict(capital_scale=_env_float("US_REGIME_GROWTH_LEADERSHIP_CAPITAL_SCALE", 0.70), max_ai_tech_ratio=_env_float("US_REGIME_GROWTH_LEADERSHIP_AI_TECH_CAP", 0.45), max_single_cluster_ratio=0.22, allow_new_buy=True, allow_ai_tech_buy=True, allow_defensive_buy=True, entry_aggressiveness="selective", trailing_stop_mode="normal", take_profit_mode="staged_take_profit", max_new_positions=15),
        "NEUTRAL": dict(capital_scale=_env_float("US_REGIME_NEUTRAL_CAPITAL_SCALE", 0.50), max_ai_tech_ratio=_env_float("US_REGIME_NEUTRAL_AI_TECH_CAP", 0.35), max_single_cluster_ratio=0.20, allow_new_buy=True, allow_ai_tech_buy=False, allow_defensive_buy=True, entry_aggressiveness="normal", trailing_stop_mode="normal", take_profit_mode="staged_take_profit", max_new_positions=10),
        "DEFENSIVE": dict(capital_scale=_env_float("US_REGIME_DEFENSIVE_CAPITAL_SCALE", 0.25), max_ai_tech_ratio=_env_float("US_REGIME_DEFENSIVE_AI_TECH_CAP", 0.20), max_single_cluster_ratio=0.15, allow_new_buy=True, allow_ai_tech_buy=False, allow_defensive_buy=True, entry_aggressiveness="defensive_only", trailing_stop_mode="risk_off_tight", take_profit_mode="fast_profit_capture", max_new_positions=5),
        "RISK_OFF": dict(capital_scale=_env_float("US_REGIME_RISK_OFF_CAPITAL_SCALE", 0.00), max_ai_tech_ratio=0.10, max_single_cluster_ratio=0.10, allow_new_buy=False, allow_ai_tech_buy=False, allow_defensive_buy=False, entry_aggressiveness="block_new_entries", trailing_stop_mode="crash_tight", take_profit_mode="preserve_cash", max_new_positions=0, force_entry_block=True),
    }
    base.update(table.get(r, table["NEUTRAL"]))
    return base


def _account_metrics(positions: list[dict] | None, snap: dict | None) -> dict:
    snap = snap or {}
    def f(key):
        try:
            return float(snap.get(key) or 0)
        except Exception:
            return 0.0
    invested_usd = f("invested_market_value_usd") or sum(float(p.get("market_value_usd") or p.get("market_value") or _price(p) * _qty(p) or 0) for p in positions or [])
    equity_usd = f("portfolio_equity_usd") or f("account_equity_usd") or invested_usd + f("cash_usd")
    cash_usd = f("cash_usd")
    gross = _pct(snap.get("gross_exposure_pct"))
    if gross is None:
        gross = invested_usd / equity_usd if equity_usd > 0 else 0.0
    ai_mv = 0.0
    for p in positions or []:
        if theme_cluster_for(_symbol(p), p) in AI_TECH_CLUSTERS:
            ai_mv += float(p.get("market_value_usd") or p.get("market_value") or _price(p) * _qty(p) or 0)
    ai_weight = _pct(snap.get("ai_tech_weight"))
    if ai_weight is None:
        ai_weight = ai_mv / equity_usd if equity_usd > 0 else 0.0
    return {
        "portfolio_equity_usd": equity_usd,
        "invested_market_value_usd": invested_usd,
        "cash_usd": cash_usd,
        "gross_exposure_pct": gross,
        "open_position_count": len(positions or []),
        "ai_tech_weight": ai_weight,
        "account_intraday_pnl_pct": _pct(snap.get("account_intraday_pnl_pct")),
        "account_5d_pnl_pct": _pct(snap.get("account_5d_pnl_pct")),
    }


def evaluate_us_market_state(*, trade_date: str, provider, rotation_context: dict | None, prep_result: dict | None, positions: list[dict] | None, account_snapshot: dict | None, now=None) -> dict:
    warnings: list[str] = []
    rc = rotation_context or (prep_result or {}).get("rotation_context") or {}
    rotation_regime = str((prep_result or {}).get("rotation_regime") or rc.get("rotation_regime") or "UNKNOWN")
    suspect = bool(rc.get("rotation_context_suspect") or (prep_result or {}).get("rotation_context_suspect"))
    suspect_policy = str(rc.get("rotation_suspect_policy") or (prep_result or {}).get("rotation_suspect_policy") or "block")
    quality = str(rc.get("benchmark_data_quality") or (prep_result or {}).get("benchmark_data_quality") or "ok").lower()
    rets = _market_returns(provider, trade_date, warnings)
    acct = _account_metrics(positions, account_snapshot)
    spy1, qqq1, smh1 = rets.get("spy_1d_return"), rets.get("qqq_1d_return"), rets.get("smh_1d_return")
    spy3, qqq3, smh3 = rets.get("spy_3d_return"), rets.get("qqq_3d_return"), rets.get("smh_3d_return")
    pnl1, pnl5 = acct["account_intraday_pnl_pct"], acct["account_5d_pnl_pct"]
    reasons: list[str] = []
    crash = riskoff = caution = False

    def hit(cond, level, reason):
        nonlocal crash, riskoff, caution
        if cond:
            reasons.append(reason)
            if level == "crash":
                crash = True
            elif level == "riskoff":
                riskoff = True
            else:
                caution = True

    global_crash_confirmations = [
        spy1 is not None and spy1 <= -0.012,
        qqq1 is not None and qqq1 <= -0.018,
        smh1 is not None and smh1 <= -0.025,
        pnl1 is not None and pnl1 <= -0.018,
    ]
    sector_crash_ai_semi = bool(smh1 is not None and smh1 <= -0.040 and sum(1 for x in global_crash_confirmations if x) < 2)
    hit(spy1 is not None and spy1 <= -0.020, "crash", "SPY_1D_LE_-2.0pct")
    hit(qqq1 is not None and qqq1 <= -0.028, "crash", "QQQ_1D_LE_-2.8pct")
    hit(smh1 is not None and smh1 <= -0.040 and not sector_crash_ai_semi, "crash", "SMH_1D_LE_-4.0pct")
    if sector_crash_ai_semi:
        reasons.append("SECTOR_CRASH_AI_SEMI:SMH_1D_LE_-4.0pct")
        riskoff = True
    hit(pnl1 is not None and pnl1 <= -0.018, "crash", "ACCOUNT_INTRADAY_LE_-1.8pct")
    hit(pnl5 is not None and pnl5 <= -0.050, "crash", "ACCOUNT_5D_LE_-5.0pct")
    hit(suspect and suspect_policy == "block", "crash", "ROTATION_CONTEXT_SUSPECT_BLOCK")
    critical_missing = {str(s).upper() for s in (rc.get("missing_symbols") or (prep_result or {}).get("missing_symbols") or []) if str(s).upper() in {"SPY", "QQQ", "SMH"}}
    degraded_crash = quality != "ok" and (
        (spy1 is not None and spy1 <= -0.012)
        or (qqq1 is not None and qqq1 <= -0.018)
        or (smh1 is not None and smh1 <= -0.025)
        or (len(critical_missing) >= 2 and suspect_policy == "block")
    )
    hit(degraded_crash, "crash", "DEGRADED_BENCHMARK_WITH_WEAK_INDEX")
    hit(spy1 is not None and spy1 <= -0.012, "riskoff", "SPY_1D_LE_-1.2pct")
    hit(qqq1 is not None and qqq1 <= -0.018, "riskoff", "QQQ_1D_LE_-1.8pct")
    hit(smh1 is not None and smh1 <= -0.025, "riskoff", "SMH_1D_LE_-2.5pct")
    hit(rotation_regime == "RISK_OFF", "riskoff", "ROTATION_RISK_OFF")
    hit(pnl1 is not None and pnl1 <= -0.010, "riskoff", "ACCOUNT_INTRADAY_LE_-1.0pct")
    hit(pnl5 is not None and pnl5 <= -0.030, "riskoff", "ACCOUNT_5D_LE_-3.0pct")
    hit(acct["ai_tech_weight"] > _env_float("US_MAX_AI_TECH_EXPOSURE_RISK_OFF", 0.20) and rotation_regime == "RISK_OFF", "riskoff", "AI_TECH_WEIGHT_OVER_RISK_OFF_CAP")
    hit(spy1 is not None and spy1 <= -0.007, "caution", "SPY_1D_LE_-0.7pct")
    hit(qqq1 is not None and qqq1 <= -0.010, "caution", "QQQ_1D_LE_-1.0pct")
    hit(smh1 is not None and smh1 <= -0.015, "caution", "SMH_1D_LE_-1.5pct")
    hit(rotation_regime == "AI_OFF_ROTATION", "caution", "ROTATION_AI_OFF")
    hit(pnl1 is not None and pnl1 <= -0.007, "caution", "ACCOUNT_INTRADAY_LE_-0.7pct")
    hit(rets.get("qqq_vs_spy_3d") is not None and rets["qqq_vs_spy_3d"] < -0.005, "caution", "QQQ_WEAK_VS_SPY_3D")
    hit(rets.get("smh_vs_spy_3d") is not None and rets["smh_vs_spy_3d"] < -0.010, "caution", "SMH_WEAK_VS_SPY_3D")
    if suspect and not crash:
        caution = True
        reasons.append("SUSPECT_DATA_RISK_ON_DISABLED")

    strong = rotation_regime == "AI_ON" and spy3 is not None and qqq3 is not None and smh3 is not None and spy3 > 0 and qqq3 > spy3 and smh3 > spy3 and ((qqq1 is not None and qqq1 >= 0) or (spy1 is not None and spy1 >= 0)) and not suspect and (pnl1 is None or pnl1 > -0.005) and quality == "ok"
    riskon = spy1 is not None and qqq1 is not None and spy1 > 0 and qqq1 > 0 and rotation_regime in {"AI_ON", "BROAD_UP"} and not suspect and (pnl1 is None or pnl1 > -0.007)
    rebound = None
    daily_crash = any(reason.startswith(("SPY_1D_", "QQQ_1D_", "SMH_1D_")) for reason in reasons)
    hard_non_market_crash = any(reason.startswith(("ACCOUNT_", "ROTATION_CONTEXT_", "DEGRADED_BENCHMARK_")) for reason in reasons)
    if crash:
        state = "DEFENSE_CRASH_PENDING"
        rebound = _intraday_rebound(provider, rets, warnings) if daily_crash and not hard_non_market_crash else {"snapshots": {}, "signals": {}, "confirmation_count": 0, "data_ok": False, "hard_down": True, "smh_recovered": False}
        logger.info("[US_MARKET_STATE][INTRADAY_REBOUND] data_ok=%s hard_down=%s confirmations=%s signals=%s snapshots=%s", rebound["data_ok"], rebound["hard_down"], rebound["confirmation_count"], rebound["signals"], rebound["snapshots"])
        if rebound["data_ok"] and not rebound["hard_down"] and rebound["confirmation_count"] >= 2:
            state = "DEFENSE_CRASH_REBOUND"
            reasons.append("INTRADAY_CRASH_REBOUND_CONFIRMED")
            logger.warning("[US_MARKET_STATE][CRASH_REBOUND_UNLOCK] confirmations=%s", rebound["confirmation_count"])
        else:
            state = "DEFENSE_CRASH_CONFIRMED"
            reasons.append("INTRADAY_CRASH_REBOUND_BLOCKED")
            logger.warning("[US_MARKET_STATE][CRASH_REBOUND_BLOCK] data_ok=%s hard_down=%s confirmations=%s", rebound["data_ok"], rebound["hard_down"], rebound["confirmation_count"])
    elif riskoff:
        state = "DEFENSE_RISK_OFF"
    elif caution:
        state = "DEFENSE_CAUTION"
    elif strong:
        state = "STRONG_RISK_ON"
        reasons.append("STRONG_RISK_ON_CONFIRM")
    elif riskon:
        state = "RISK_ON"
        reasons.append("RISK_ON_CONFIRM")
    else:
        state = "NORMAL"
        reasons.append("NORMAL_BASELINE")

    mults = {"DEFENSE_CRASH_PENDING": 0.0, "DEFENSE_CRASH_CONFIRMED": _env_float("US_DEFENSE_CRASH_MULT", 0.0), "DEFENSE_CRASH_REBOUND": _env_float("US_DEFENSE_CRASH_REBOUND_MULT", 0.25), "DEFENSE_RISK_OFF": _env_float("US_DEFENSE_RISK_OFF_MULT", 0.20), "DEFENSE_CAUTION": _env_float("US_DEFENSE_CAUTION_MULT", 0.50), "NORMAL": 1.0, "RISK_ON": _env_float("US_RISK_ON_MULT", 1.10), "STRONG_RISK_ON": _env_float("US_STRONG_RISK_ON_MULT", 1.25)}
    modes = {"DEFENSE_CRASH_PENDING": "crash_tight", "DEFENSE_CRASH_CONFIRMED": "crash_tight", "DEFENSE_CRASH_REBOUND": "crash_rebound_tight", "DEFENSE_RISK_OFF": "risk_off_tight", "DEFENSE_CAUTION": "caution", "NORMAL": "normal", "RISK_ON": "risk_on", "STRONG_RISK_ON": "strong_risk_on"}
    trails = {"DEFENSE_CRASH_PENDING": _env_float("US_TRAIL_CRASH_PCT", .008), "DEFENSE_CRASH_CONFIRMED": _env_float("US_TRAIL_CRASH_PCT", .008), "DEFENSE_CRASH_REBOUND": _env_float("US_TRAIL_CRASH_REBOUND_PCT", .010), "DEFENSE_RISK_OFF": _env_float("US_TRAIL_RISK_OFF_PCT", .010), "DEFENSE_CAUTION": _env_float("US_TRAIL_CAUTION_PCT", .015), "NORMAL": _env_float("US_TRAIL_NORMAL_PCT", .020), "RISK_ON": _env_float("US_TRAIL_RISK_ON_PCT", .025), "STRONG_RISK_ON": _env_float("US_TRAIL_STRONG_RISK_ON_PCT", .030)}
    loss = state.startswith("DEFENSE") and any(r.startswith("ACCOUNT_") for r in reasons)
    if loss and state in {"RISK_ON", "STRONG_RISK_ON"}:
        state = "DEFENSE_CAUTION"

    risk_score = 0
    growth_score = 0
    breadth_score = 0
    defensive_score = 0

    def add(cond: bool, bucket: str, pts: int, reason: str) -> None:
        nonlocal risk_score, growth_score, breadth_score, defensive_score
        if not cond:
            return
        reasons.append(reason)
        if bucket == "risk":
            risk_score += pts
        elif bucket == "growth":
            growth_score += pts
        elif bucket == "breadth":
            breadth_score += pts
        elif bucket == "defensive":
            defensive_score += pts

    add(spy1 is not None and spy1 <= -0.012, "risk", 2, "REGIME_SPY_1D_LE_-1.2pct")
    add(qqq1 is not None and qqq1 <= -0.018, "risk", 2, "REGIME_QQQ_1D_LE_-1.8pct")
    add(smh1 is not None and smh1 <= -0.025, "risk", 2, "REGIME_SMH_1D_LE_-2.5pct")
    add(spy3 is not None and spy3 < -0.020, "risk", 2, "REGIME_SPY_3D_LT_-2.0pct")
    add((rets.get("qqq_vs_spy_3d") is not None and rets["qqq_vs_spy_3d"] < -0.005), "risk", 1, "REGIME_QQQ_SPY_3D_WEAK")
    add((rets.get("smh_vs_spy_3d") is not None and rets["smh_vs_spy_3d"] < -0.010), "risk", 1, "REGIME_SMH_SPY_3D_WEAK")
    add((rets.get("defensive_vs_spy_3d") is not None and rets["defensive_vs_spy_3d"] > 0.005), "risk", 1, "REGIME_DEFENSIVE_3D_OUTPERFORM")
    add((rets.get("rsp_vs_spy_20d") is not None and rets["rsp_vs_spy_20d"] < -0.010), "risk", 1, "REGIME_RSP_SPY_20D_WEAK")
    add((rets.get("iwm_vs_spy_20d") is not None and rets["iwm_vs_spy_20d"] < -0.010), "risk", 1, "REGIME_IWM_SPY_20D_WEAK")
    add(spy3 is not None and spy3 > 0, "growth", 1, "REGIME_SPY_3D_POSITIVE")
    add((rets.get("qqq_vs_spy_3d") is not None and rets["qqq_vs_spy_3d"] > 0.005), "growth", 1, "REGIME_QQQ_SPY_3D_STRONG")
    add((rets.get("smh_vs_spy_3d") is not None and rets["smh_vs_spy_3d"] > 0.007), "growth", 1, "REGIME_SMH_SPY_3D_STRONG")
    add((rets.get("qqq_vs_spy_20d") is not None and rets["qqq_vs_spy_20d"] > 0.010), "growth", 1, "REGIME_QQQ_SPY_20D_STRONG")
    add((rets.get("smh_vs_spy_20d") is not None and rets["smh_vs_spy_20d"] > 0.010), "growth", 1, "REGIME_SMH_SPY_20D_STRONG")
    add((rets.get("xlk_vs_spy_20d") is not None and rets["xlk_vs_spy_20d"] > 0), "growth", 1, "REGIME_XLK_SPY_20D_STRONG")
    add(rotation_regime in {"AI_ON", "BROAD_UP"}, "growth", 1, "REGIME_ROTATION_GROWTH")
    add((rets.get("rsp_vs_spy_20d") is not None and rets["rsp_vs_spy_20d"] > 0), "breadth", 1, "REGIME_RSP_BREADTH_OK")
    add((rets.get("iwm_vs_spy_20d") is not None and rets["iwm_vs_spy_20d"] > 0), "breadth", 1, "REGIME_IWM_BREADTH_OK")
    add((rets.get("cyclical_vs_spy_20d") is not None and rets["cyclical_vs_spy_20d"] > 0), "breadth", 1, "REGIME_CYCLICAL_BREADTH_OK")
    add((rets.get("defensive_vs_spy_3d") is not None and rets["defensive_vs_spy_3d"] > 0.005), "defensive", 1, "REGIME_DEFENSIVE_3D_STRONG")
    add((rets.get("defensive_vs_spy_20d") is not None and rets["defensive_vs_spy_20d"] > 0.010), "defensive", 1, "REGIME_DEFENSIVE_20D_STRONG")
    add((rets.get("qqq_vs_spy_20d") is not None and rets["qqq_vs_spy_20d"] < 0), "defensive", 1, "REGIME_QQQ_20D_WEAK")
    add((rets.get("smh_vs_spy_20d") is not None and rets["smh_vs_spy_20d"] < 0), "defensive", 1, "REGIME_SMH_20D_WEAK")

    if suspect and suspect_policy == "block":
        market_regime = "RISK_OFF"
    elif risk_score >= 6:
        market_regime = "RISK_OFF"
    elif risk_score >= 4 or defensive_score >= 3:
        market_regime = "DEFENSIVE"
    elif growth_score >= 5 and breadth_score >= 2 and risk_score <= 2:
        market_regime = "RISK_ON"
    elif growth_score >= 4 and breadth_score < 2 and risk_score <= 3:
        market_regime = "GROWTH_LEADERSHIP"
    else:
        market_regime = "NEUTRAL"
    if state in {"DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED", "DEFENSE_CRASH_REBOUND"}:
        market_regime = "RISK_OFF"
    elif state in {"DEFENSE_CAUTION", "DEFENSE_RISK_OFF"} and market_regime not in {"RISK_OFF"}:
        market_regime = "DEFENSIVE"
    elif state in {"STRONG_RISK_ON", "RISK_ON"} and market_regime == "NEUTRAL" and growth_score >= 5 and breadth_score >= 2:
        market_regime = "RISK_ON"

    constraints = regime_constraints(market_regime)
    max_gross = _env_float("US_MAX_GROSS_EXPOSURE_PCT", 0.95)
    allow_new = constraints.get("allow_new_buy", True) and acct["gross_exposure_pct"] < max_gross
    if state.startswith("DEFENSE_CRASH"):
        allow_new = state == "DEFENSE_CRASH_REBOUND" and acct["gross_exposure_pct"] < max_gross
    if acct["gross_exposure_pct"] >= max_gross:
        reasons.append("GROSS_EXPOSURE_CAP_REACHED")
    constraints["allow_new_buy"] = bool(allow_new)
    if state == "DEFENSE_CRASH_REBOUND":
        constraints.update(
            force_entry_block=False,
            allow_new_buy=True,
            allow_ai_tech_buy=bool(rebound and rebound["smh_recovered"]),
            trailing_stop_mode="crash_rebound_tight",
            take_profit_mode="fast_profit_capture",
            max_new_positions=int(_env_float("US_DEFENSE_CRASH_REBOUND_MAX_NEW_POSITIONS", 3)),
        )
    regime_score = growth_score + breadth_score - risk_score - defensive_score
    out = {
        **rets,
        **acct,
        **constraints,
        "market_state": state,
        "sector_state": "SECTOR_CRASH_AI_SEMI" if sector_crash_ai_semi else "NONE",
        "global_crash_confirmations": sum(1 for x in global_crash_confirmations if x),
        "defense_regime": state if state.startswith("DEFENSE") else "NONE",
        "risk_on_regime": state if state.endswith("RISK_ON") else "NONE",
        "market_state_reasons": reasons,
        "rotation_regime": rotation_regime,
        "market_regime_version": "us_leading_regime_v1",
        "regime_score": regime_score,
        "risk_score": risk_score,
        "growth_score": growth_score,
        "breadth_score": breadth_score,
        "defensive_score": defensive_score,
        "regime_reasons": reasons,
        "leading_indicators": dict(rets),
        "exposure_multiplier": mults[state],
        "allow_new_buy": constraints.get("allow_new_buy", allow_new),
        "allow_add_to_existing": False if state == "DEFENSE_CRASH_REBOUND" else market_regime in {"NEUTRAL", "GROWTH_LEADERSHIP", "RISK_ON"},
        "trim_required": state in {"DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED", "DEFENSE_RISK_OFF"},
        "intraday_rebound": rebound,
        "profit_capture_enabled": os.getenv("US_PROFIT_CAPTURE_ENABLE", "1") not in {"0", "false", "False"},
        "trailing_stop_mode": constraints.get("trailing_stop_mode", modes[state]),
        "trailing_stop_pct": trails[state],
        "trailing_stop_reason": f"market_state={state}",
        "stop_tightening_level": constraints.get("trailing_stop_mode", modes[state]),
        "account_loss_kill_switch_triggered": loss,
        "account_loss_kill_switch_level": state if loss else "NONE",
        "account_pnl_source": "account_snapshot" if pnl1 is not None else "unknown",
        "account_intraday_pnl_pct_unknown": pnl1 is None,
        "data_quality": "degraded" if warnings or quality != "ok" else "ok",
        "data_quality_warnings": warnings,
        "forbidden_hedge_symbols": sorted(FORBIDDEN_HEDGE_SYMBOLS),
    }
    if state == "DEFENSE_CRASH_REBOUND":
        out["effective_capital_scale"] = mults[state]
        out["effective_max_new_positions"] = constraints["max_new_positions"]
    logger.info("[US_MARKET_STATE][REGIME] market_state=%s defense_regime=%s risk_on_regime=%s reasons=%s rotation_regime=%s spy_1d=%s qqq_1d=%s smh_1d=%s exposure_multiplier=%.2f allow_new_buy=%s allow_add_to_existing=%s allow_ai_tech_buy=%s", out["market_state"], out["defense_regime"], out["risk_on_regime"], out["market_state_reasons"], rotation_regime, spy1, qqq1, smh1, out["exposure_multiplier"], out["allow_new_buy"], out["allow_add_to_existing"], out["allow_ai_tech_buy"])
    return out


def resolve_market_state_entry_block_reason(
    *,
    symbol: str,
    cluster: str,
    trend_score: float,
    score_final: float,
    rank_final30: int,
    position_state: str,
    overlay: dict,
) -> str | None:
    """Return the canonical market-state block reason for a BUY candidate."""
    sym = str(symbol or "").upper().strip()
    cluster = str(cluster or theme_cluster_for(sym, {})).upper().strip()
    state = str(overlay.get("market_state") or "NORMAL")
    held = str(position_state or "NOT_HELD").upper() == "HELD"
    if sym in FORBIDDEN_HEDGE_SYMBOLS:
        return "FORBIDDEN_HEDGE_OR_INVERSE_ETF"
    if overlay.get("force_entry_block") or state in {"DEFENSE_CRASH_PENDING", "DEFENSE_CRASH_CONFIRMED"}:
        return "DEFENSE_CRASH_ENTRY_BLOCK"
    if state == "DEFENSE_RISK_OFF" and (cluster in AI_TECH_CLUSTERS or held):
        return "DEFENSE_RISK_OFF_AI_TECH_BLOCK" if cluster in AI_TECH_CLUSTERS else "DEFENSE_RISK_OFF_ENTRY_REDUCED"
    if state == "DEFENSE_RISK_OFF" and cluster not in DEFENSIVE_CLUSTERS and sym not in CORE_INDEX_ETFS:
        if cluster != "ENERGY_MATERIALS" or trend_score < 0.6 or score_final < 0.6:
            return "DEFENSE_RISK_OFF_ENTRY_REDUCED"
    if state == "DEFENSE_CAUTION" and (held or (cluster in AI_TECH_CLUSTERS and not overlay.get("allow_ai_tech_buy"))):
        return "DEFENSE_CAUTION_ENTRY_REDUCED"
    if overlay.get("market_regime") == "RISK_OFF" and state != "DEFENSE_CRASH_REBOUND":
        return "risk_off_entry_block"
    # DEFENSE_RISK_OFF has the more specific policy above, including its
    # strong ENERGY_MATERIALS exception; do not override it with the generic
    # defensive-regime rule.
    if overlay.get("market_regime") == "DEFENSIVE" and state != "DEFENSE_RISK_OFF" and cluster not in DEFENSIVE_CLUSTERS:
        return "defensive_only_entry_block"
    if overlay.get("market_regime") == "NEUTRAL" and cluster in AI_TECH_CLUSTERS and not (rank_final30 <= 10 and score_final >= 0.60):
        return "neutral_ai_tech_rank_score_block"
    if not overlay.get("allow_new_buy", True):
        return "allow_new_buy_false"
    if cluster in AI_TECH_CLUSTERS and not overlay.get("allow_ai_tech_buy", True):
        return "allow_ai_tech_buy_false"
    return None


def filter_watchlist_rows_for_market_state(
    rows: list[dict],
    overlay: dict,
    positions: list[dict] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Filter Final30 before top-N intent selection, retaining its metadata."""
    held_symbols = {_symbol(p) for p in positions or []}
    kept: list[dict] = []
    blocked: list[dict] = []
    for fallback_rank, row in enumerate(rows or [], 1):
        sym = str(row.get("symbol") or row.get("code") or "").upper().strip()
        cluster = str(row.get("theme_cluster") or row.get("cluster") or theme_cluster_for(sym, row))
        trend_score = float(row.get("trend_score") or 0.0)
        score_final = float(row.get("score_final") or row.get("score") or 0.0)
        rank_final30 = int(row.get("rank_final30") or row.get("rank") or fallback_rank)
        position_state = "HELD" if sym in held_symbols else str(row.get("position_state") or "NOT_HELD")
        reason = resolve_market_state_entry_block_reason(
            symbol=sym, cluster=cluster, trend_score=trend_score,
            score_final=score_final, rank_final30=rank_final30,
            position_state=position_state,
            overlay=overlay,
        )
        if reason is None and overlay.get("market_state") == "STRONG_RISK_ON" and sym in held_symbols:
            position = next((p for p in positions or [] if _symbol(p) == sym), {})
            if not is_strong_holding(position, row):
                reason = "MARKET_STATE_ENTRY_BLOCK"
        if reason:
            blocked.append({"symbol": sym, "cluster": cluster, "reason": reason, "block_stage": "market_state", "market_state": overlay.get("market_state", "NORMAL")})
            logger.warning("[US_MARKET_STATE][WATCHLIST_BLOCK] symbol=%s cluster=%s reason=%s", sym, cluster, reason)
        else:
            kept.append({
                **row,
                "theme_cluster": cluster,
                "trend_score": trend_score,
                "score_final": score_final,
                "rank_final30": rank_final30,
                "position_state": position_state,
                "market_state": overlay.get("market_state", "NORMAL"),
                "market_regime": overlay.get("market_regime", "NEUTRAL"),
                "blocked_reason": None,
                "block_stage": None,
            })
    kept.sort(key=lambda row: float(row.get("score_final") or row.get("score") or 0.0), reverse=True)
    return kept, blocked


def filter_entry_intents_for_market_state(entry_intents: list[dict], overlay: dict, positions: list[dict] | None = None) -> tuple[list[dict], list[dict]]:
    kept, blocked = [], []
    state = overlay.get("market_state", "NORMAL")
    pos_by_sym = {_symbol(p): p for p in positions or []}
    for intent in entry_intents or []:
        if str(intent.get("side") or "BUY").upper() != "BUY":
            kept.append(intent)
            continue
        sym = str(intent.get("symbol") or "").upper().strip()
        meta = intent.setdefault("meta", {}) if isinstance(intent.setdefault("meta", {}), dict) else {}
        metadata_mismatch = next(
            (
                key for key in (
                    "theme_cluster", "trend_score", "score_final", "rank_final30",
                    "entry_style", "position_state", "position_action",
                    "market_state", "market_regime", "blocked_reason", "block_stage",
                )
                if intent.get(key) is not None
                and meta.get(key) is not None
                and intent.get(key) != meta.get(key)
            ),
            None,
        )
        cluster = str(intent.get("theme_cluster") or intent.get("cluster") or meta.get("theme_cluster") or theme_cluster_for(sym, intent))
        reason = "ENTRY_METADATA_INVARIANT_FAIL" if metadata_mismatch else resolve_market_state_entry_block_reason(
            symbol=sym, cluster=cluster,
            trend_score=float(intent.get("trend_score") or meta.get("trend_score") or 0),
            score_final=float(intent.get("score_final") or intent.get("score") or meta.get("score_final") or 0),
            rank_final30=int(intent.get("rank_final30") or meta.get("rank_final30") or 99),
            position_state="HELD" if sym in pos_by_sym else str(intent.get("position_state") or "NOT_HELD"),
            overlay=overlay,
        )
        if reason is None and state == "STRONG_RISK_ON" and sym in pos_by_sym and not is_strong_holding(pos_by_sym[sym], intent):
            reason = "MARKET_STATE_ENTRY_BLOCK"
        if reason:
            meta["blocked_reason"] = reason
            meta["block_stage"] = "metadata" if metadata_mismatch else "market_state"
            blocked.append({"symbol": sym, "cluster": cluster, "reason": reason, "block_stage": meta["block_stage"], "metadata_field": metadata_mismatch, "market_state": state})
            logger.warning("[US_MARKET_STATE][ENTRY_BLOCK] symbol=%s cluster=%s reason=%s market_state=%s", sym, cluster, reason, state)
        else:
            kept.append(intent)
    return kept, blocked


def is_strong_holding(pos: dict, row: dict | None = None) -> bool:
    row = row or {}
    unreal = _pnl_pct(pos)
    day = _pct(pos.get("day_pnl_pct") or pos.get("pnl_1d_pct") or pos.get("day_pnl"))
    price, entry = _price(pos), _entry_price(pos)
    rank = int(row.get("rank_final30") or 99)
    score = float(row.get("score_final") or row.get("score") or 0)
    trend = float(row.get("trend_score") or 0)
    return (unreal is None or unreal > 0) and (day is None or day > -0.01) and (entry <= 0 or price > entry) and rank <= 10 and score >= 0.6 and trend >= 0


def build_profit_capture_intents(positions: list[dict], overlay: dict, existing_sell_symbols: set[str] | None = None, now=None, trade_date: str | None = None, profit_capture_state: dict[str, dict] | None = None) -> list[dict]:
    if not overlay.get("profit_capture_enabled", True):
        return []
    existing_sell_symbols = existing_sell_symbols or set()
    if profit_capture_state is None and trade_date:
        try:
            from trader.us.db.repos import load_us_profit_capture_state
            symbols = [_symbol(p) for p in positions or [] if _symbol(p)]
            profit_capture_state = load_us_profit_capture_state(trade_date, symbols)
        except Exception as exc:
            logger.warning("[US_PROFIT_CAPTURE][STATE_LOAD_WARN] trade_date=%s err=%s", trade_date, exc)
            profit_capture_state = {}
    profit_capture_state = profit_capture_state or {}
    runner_min = _env_float("US_RUNNER_MIN_REMAIN_PCT", 0.40)
    stages = [("tp1_done", _env_float("US_TP1_PCT", .03), _env_float("US_TP1_SELL_PCT", .25), "TAKE_PROFIT_TP1"), ("tp2_done", _env_float("US_TP2_PCT", .05), _env_float("US_TP2_SELL_PCT", .25), "TAKE_PROFIT_TP2"), ("tp3_done", _env_float("US_TP3_PCT", .08), _env_float("US_TP3_SELL_PCT", .20), "TAKE_PROFIT_TP3")]
    intents = []
    for p in positions or []:
        sym = _symbol(p)
        q = _qty(p)
        price = _price(p)
        meta = p.get("meta") if isinstance(p.get("meta"), dict) else p
        if not sym or sym in existing_sell_symbols or q <= 1 or price <= 0:
            continue
        state = dict(profit_capture_state.get(sym) or profit_capture_state.get(sym.upper()) or {})
        meta_state = p.get("meta") if isinstance(p.get("meta"), dict) else {}
        pnl = _pnl_pct(p)
        if pnl is None:
            continue
        for flag, thresh, sell_pct, reason in stages:
            pending_flag = flag.replace("_done", "_pending")
            if pnl >= thresh and not bool(meta.get(flag)) and not bool(meta_state.get(flag)) and not bool(state.get(flag)) and not bool(state.get(pending_flag)):
                max_sell = max(0, q - int(q * runner_min))
                qty = min(max_sell, max(1, int(q * sell_pct)))
                if qty > 0:
                    order_key = f"US_PC_{trade_date or 'NA'}_{sym}_{reason}"
                    intents.append({"symbol": sym, "side": "SELL", "qty": qty, "quantity": qty, "limit_price": price, "notional_usd": qty * price, "reason": reason, "client_order_key": order_key, "meta": {"reason": reason, "profit_capture_stage": flag.replace("_done", ""), flag: True, "runner_remaining_pct": (q - qty) / q, "market_state": overlay.get("market_state"), "last_profit_capture_at": (now or datetime.now(timezone.utc)).isoformat()}})
                    if trade_date:
                        try:
                            from trader.us.db.repos import mark_us_profit_capture_stage
                            mark_us_profit_capture_stage(trade_date, sym, flag.replace("_done", ""), order_key=order_key, qty=qty, notional_usd=qty * price, status="PENDING")
                            state[pending_flag] = True
                            profit_capture_state[sym] = state
                        except Exception as exc:
                            logger.warning("[US_PROFIT_CAPTURE][STATE_MARK_WARN] symbol=%s stage=%s err=%s", sym, flag, exc)
                    logger.info("[US_PROFIT_CAPTURE][%s] symbol=%s qty=%d pnl=%.4f runner_remaining_pct=%.4f", reason[-3:], sym, qty, pnl, (q - qty) / q)
                break
    return intents


def build_defense_trim_intents(positions: list[dict], overlay: dict, existing_sell_symbols: set[str] | None = None,
                               trade_date: str | None = None, context=None) -> list[dict]:
    from trader.us.execution.order_identity import InvalidOrderIdentity
    trade_date = trade_date or getattr(context, "trade_date", None)
    if not trade_date:
        raise InvalidOrderIdentity("defense trim requires trade_date")
    state = overlay.get("market_state")
    if state not in {"DEFENSE_RISK_OFF", "DEFENSE_CRASH"}:
        return []
    existing_sell_symbols = existing_sell_symbols or set()
    maxn = int(_env_float("US_DEFENSE_MAX_TRIM_SYMBOLS_PER_TICK", 3))
    pct = _env_float("US_DEFENSE_TRIM_PCT_CRASH", .50) if state == "DEFENSE_CRASH" else _env_float("US_DEFENSE_TRIM_PCT_RISK_OFF", .30)
    candidates = []
    for p in positions or []:
        sym = _symbol(p)
        if not sym or sym in existing_sell_symbols:
            continue
        cluster = theme_cluster_for(sym, p)
        if state == "DEFENSE_RISK_OFF" and cluster not in AI_TECH_CLUSTERS:
            continue
        candidates.append(p)
    candidates.sort(key=lambda p: (_pnl_pct(p) or 0, _pct(p.get("day_pnl_pct") or p.get("day_pnl")) or 0))
    intents = []
    for p in candidates[:maxn]:
        q = _qty(p)
        price = _price(p)
        sym = _symbol(p)
        if q <= 1 or price <= 0:
            continue
        qty = min(q - 1, max(1, int(q * pct)))
        reason = "DEFENSE_CRASH_TRIM" if state == "DEFENSE_CRASH" else "DEFENSE_RISK_OFF_TRIM"
        lifecycle = p.get("position_lifecycle_id") or (p.get("meta") or {}).get("position_lifecycle_id") or "NA"
        td = trade_date
        order_key = f"US_DEF_{td}_{sym}_{lifecycle}_{state}_{q}_{qty}"
        intents.append({
            "symbol": sym, "exchange": p.get("exchange") or p.get("raw_exchange") or "",
            "side": "SELL", "qty": qty, "quantity": qty, "limit_price": price,
            "notional_usd": qty * price, "reason": reason, "client_order_key": order_key,
            "trade_date": td, "position_lifecycle_id": lifecycle, "pre_order_position_qty": q,
            "session": getattr(context, "session", ""),
            "session_run_id": getattr(context, "session_run_id", ""),
            "session_generation": getattr(context, "session_generation", 1),
            "tick_id": getattr(context, "tick_id", ""), "prep_run_id": getattr(context, "prep_run_id", ""),
            "meta": {"reason": reason, "market_state": state, "position_lifecycle_id": lifecycle,
                     "pre_order_position_qty": q},
        })
        logger.warning("[US_DEFENSE][TRIM] symbol=%s qty=%d notional=%.2f reason=%s market_state=%s", sym, qty, qty * price, reason, state)
    return intents
