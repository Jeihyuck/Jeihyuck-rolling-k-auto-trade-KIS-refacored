from __future__ import annotations

import argparse
import logging
import os
from dataclasses import asdict
from datetime import date, datetime
from uuid import uuid4

from trader.core_utils import _round_to_tick
from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import FillsRepo, KrInfiniteCampaignsRepo, LedgerEventsRepo, OrdersRepo, RunsRepo
from trader.kis_wrapper import KisAPI
from trader.strategies.kr_infinite import CampaignState, KrInfiniteConfig, MarketState, evaluate_trade
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)
STRATEGY = "kr_infinite_leverage"
SID = 61
MODE = 1
STAGE = "KR-INFINITE"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _resolve_env(cli_env: str | None) -> str:
    raw = cli_env or os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice"
    env = raw.strip().lower()
    if env not in {"practice", "paper", "real"}:
        raise RuntimeError(f"invalid_env:{env}")
    return env


def _config() -> KrInfiniteConfig:
    return KrInfiniteConfig(
        tranches=_env_int("KR_INF_TRANCHES", 24),
        initial_tranches=_env_int("KR_INF_INITIAL_TRANCHES", 2),
        max_deployed_tranches=_env_int("KR_INF_MAX_DEPLOYED_TRANCHES", 18),
        add_step_pct=_env_float("KR_INF_ADD_STEP_PCT", 0.025),
        take_profit_pct=_env_float("KR_INF_TAKE_PROFIT_PCT", 0.045),
        take_profit_reduced_pct=_env_float("KR_INF_TAKE_PROFIT_REDUCED_PCT", 0.030),
        max_drawdown_pct=_env_float("KR_INF_MAX_DRAWDOWN_PCT", 0.18),
        max_campaign_days=_env_int("KR_INF_MAX_CAMPAIGN_DAYS", 90),
        max_buys_per_day=_env_int("KR_INF_MAX_BUYS_PER_DAY", 1),
        min_regime_score=_env_float("KR_INF_MIN_REGIME_SCORE", 0.0),
        defense_regime_score=_env_float("KR_INF_DEFENSE_REGIME_SCORE", -1.0),
        max_market_premium_pct=_env_float("KR_INF_MAX_MARKET_PREMIUM_PCT", 0.015),
    )


def _regime_score(kis: KisAPI, benchmark_code: str) -> tuple[float, dict]:
    candles = kis.get_daily_candles(benchmark_code, count=80)
    closes = [float(r.get("close") or 0.0) for r in candles if float(r.get("close") or 0.0) > 0]
    if len(closes) < 60:
        return -2.0, {"reason": "insufficient_benchmark_history", "rows": len(closes)}
    ma20 = sum(closes[-20:]) / 20.0
    ma60 = sum(closes[-60:]) / 60.0
    last = closes[-1]
    ret20 = (last / closes[-21]) - 1.0 if closes[-21] > 0 else 0.0
    score = 0.0
    score += 0.75 if last >= ma20 else -0.75
    score += 0.75 if ma20 >= ma60 else -0.75
    score += 0.50 if ret20 >= 0 else -0.50
    return score, {"last": last, "ma20": ma20, "ma60": ma60, "ret20": ret20}


def _campaign_from_fills(
    fills: list[dict],
    code: str,
    today: date,
    *,
    active_cycle: dict | None,
) -> tuple[CampaignState, bool]:
    rows = sorted(
        [r for r in fills if str(r.get("code") or "").zfill(6) == code],
        key=lambda r: r.get("filled_at") or datetime.min,
    )
    if not active_cycle:
        return CampaignState(
            code=code,
            cycle_id=f"{today.isoformat()}-{code}-new",
            started_on=today,
        ), False

    cycle_id = str(active_cycle.get("cycle_id") or f"{today.isoformat()}-{code}")
    started_on = active_cycle.get("started_on") or today
    if isinstance(started_on, str):
        started_on = date.fromisoformat(started_on[:10])
    qty = 0
    total_cost = 0.0
    last_fill_price = 0.0
    buys_today = 0
    deployed_tranches = 0
    buy_fill_seen = False

    for row in rows:
        raw = row.get("raw_json") or {}
        request_meta = raw.get("kr_inf") if isinstance(raw, dict) else None
        if not isinstance(request_meta, dict):
            request_meta = {}
        if str(request_meta.get("cycle_id") or raw.get("cycle_id") or "") != cycle_id:
            continue
        side = str(row.get("side") or "").upper()
        fill_qty = int(row.get("qty") or 0)
        price = float(row.get("price") or 0.0)
        filled_at = row.get("filled_at")
        fill_date = filled_at.date() if hasattr(filled_at, "date") else today
        if side == "BUY":
            buy_fill_seen = True
            qty += fill_qty
            total_cost += fill_qty * price
            last_fill_price = price
            deployed_tranches += int(request_meta.get("tranche_count") or raw.get("tranche_count") or 1)
            if fill_date == today:
                buys_today += 1
        elif side == "SELL" and qty > 0:
            sell_qty = min(qty, fill_qty)
            avg_before = total_cost / qty if qty else 0.0
            total_cost = max(0.0, total_cost - avg_before * sell_qty)
            qty -= sell_qty

    avg_price = total_cost / qty if qty > 0 else 0.0
    return CampaignState(
        code=code,
        cycle_id=cycle_id,
        started_on=started_on,
        qty=qty,
        avg_price=avg_price,
        invested_krw=total_cost,
        deployed_tranches=deployed_tranches,
        last_fill_price=last_fill_price,
        buys_today=buys_today,
    ), buy_fill_seen


def _fetch_strategy_fills(engine, env: str, code: str) -> list[dict]:
    schema = FillsRepo(engine)._schema
    orders = schema.orders
    fills = schema.fills
    stmt = (
        fills.select()
        .add_columns(orders.c.request_json.label("order_request_json"))
        .select_from(fills.join(orders, fills.c.kis_odno == orders.c.kis_odno))
        .where(
            (fills.c.env == env)
            & (orders.c.env == env)
            & (fills.c.code == code)
            & (orders.c.code == code)
            & (orders.c.strategy == STRATEGY)
        )
        .order_by(fills.c.filled_at.asc())
    )
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(stmt).mappings().all()]
    for row in rows:
        raw = dict(row.get("raw_json") or {})
        req = row.pop("order_request_json", None) or {}
        raw["kr_inf"] = {
            "cycle_id": req.get("cycle_id"),
            "tranche_count": req.get("tranche_count"),
        }
        row["raw_json"] = raw
    return rows


def _append_event(repo: LedgerEventsRepo, *, env: str, run_id: str, code: str, side: str | None, qty: int | None, price: float | None, ok: bool, reasons: list[str], payload: dict) -> None:
    repo.append_event(
        env=env,
        run_id=run_id,
        strategy=STRATEGY,
        event_type="KR_INF_DECISION" if side is None else "KR_INF_ORDER",
        ts=now_kst(),
        code=code,
        market="KOSPI",
        sid=SID,
        mode=MODE,
        side=side,
        qty=qty,
        price=price,
        ok=ok,
        reasons=reasons,
        stage=STAGE,
        payload_json=payload,
    )


def run_once(*, env: str, dry_run: bool) -> int:
    if not _env_bool("KR_INF_ENABLED", False):
        logger.info("[KR_INF][SKIP] reason=disabled")
        return 0

    code = str(os.getenv("KR_INF_SYMBOL") or "").strip().zfill(6)
    benchmark_code = str(os.getenv("KR_INF_BENCHMARK") or "").strip().zfill(6)
    campaign_budget = _env_float("KR_INF_CAMPAIGN_BUDGET_KRW", 0.0)
    if not code.strip("0") or not benchmark_code.strip("0") or campaign_budget <= 0:
        raise RuntimeError("KR_INF_SYMBOL, KR_INF_BENCHMARK and KR_INF_CAMPAIGN_BUDGET_KRW are required")

    cfg = _config()
    engine = make_engine()
    run_migrations(engine)
    runs = RunsRepo(engine)
    orders = OrdersRepo(engine)
    ledger = LedgerEventsRepo(engine)
    kis = KisAPI(env=env)
    run_id = runs.start_run(
        env=env,
        strategy=STRATEGY,
        run_window="intraday",
        phase="TRADE",
        event_name="kr_infinite_once",
        dry_run=dry_run,
        git_sha=os.getenv("GITHUB_SHA"),
        workflow=os.getenv("GITHUB_WORKFLOW"),
        workflow_run_id=os.getenv("GITHUB_RUN_ID"),
        workflow_attempt=_env_int("GITHUB_RUN_ATTEMPT", 0) or None,
        config_json={**asdict(cfg), "code": code, "benchmark_code": benchmark_code, "campaign_budget_krw": campaign_budget},
    )
    try:
        from trader.run_context import RunContext
        from trader.reconcile_kis import reconcile_today

        reconcile_today(
            engine=engine,
            kis=kis,
            ctx=RunContext.new(account_env=env, exec_mode="LIVE", strategy=STRATEGY, dry_run=dry_run),
        )
        campaigns = KrInfiniteCampaignsRepo(engine)
        active_cycle = campaigns.get_active(env=env, strategy=STRATEGY, code=code)
        fills = _fetch_strategy_fills(engine, env, code)
        state, buy_fill_seen = _campaign_from_fills(
            fills,
            code,
            now_kst().date(),
            active_cycle=active_cycle,
        )
        if active_cycle:
            campaigns.sync_from_fills(
                env=env,
                strategy=STRATEGY,
                code=code,
                cycle_id=state.cycle_id,
                started_on=state.started_on,
                deployed_tranches=state.deployed_tranches,
                last_fill_price=state.last_fill_price,
            )
            if state.qty <= 0 and buy_fill_seen:
                campaigns.close_cycle(
                    env=env,
                    strategy=STRATEGY,
                    code=code,
                    cycle_id=state.cycle_id,
                    closed_on=now_kst().date(),
                    reason="broker_reconciled_flat",
                )
                active_cycle = None
                state = CampaignState(
                    code=code,
                    cycle_id=f"{now_kst().date().isoformat()}-{code}-new",
                    started_on=now_kst().date(),
                )

        quote = kis.get_price_quote(code, diag_mode=False, attempts=1)
        price = float(quote.get("ask") or quote.get("last") or quote.get("prpr") or 0.0)
        if price <= 0:
            raise RuntimeError("current_price_unavailable")
        regime, regime_meta = _regime_score(kis, benchmark_code)
        nav_premium_pct = _env_float("KR_INF_NAV_PREMIUM_PCT", 0.0)
        market = MarketState(
            price=price,
            regime_score=regime,
            nav_premium_pct=nav_premium_pct,
            tradable=True,
            stale_price=False,
        )
        available_cash = float(kis.get_orderable_cash_krw(force=True))
        plan = evaluate_trade(
            state=state,
            market=market,
            campaign_budget_krw=campaign_budget,
            available_cash_krw=available_cash,
            today=now_kst().date(),
            config=cfg,
        )
        payload = {
            "state": asdict(state),
            "market": asdict(market),
            "plan": asdict(plan),
            "regime": regime_meta,
            "dry_run": dry_run,
        }
        logger.info("[KR_INF][DECISION] code=%s action=%s qty=%s tranches=%s reason=%s price=%.0f avg=%.0f deployed=%s/%s regime=%.2f", code, plan.action, plan.qty, plan.tranche_count, plan.reason, price, state.avg_price, state.deployed_tranches, cfg.max_deployed_tranches, regime)
        _append_event(ledger, env=env, run_id=run_id, code=code, side=None, qty=None, price=price, ok=True, reasons=[plan.reason], payload=payload)
        if plan.action == "HOLD":
            runs.finish_run(run_id, "OK", notes=plan.reason)
            return 0

        side = plan.action
        order_price = _round_to_tick(price * (1.002 if side == "BUY" else 0.998), mode="up" if side == "BUY" else "down")
        date_tag = now_kst().strftime("%Y%m%d")
        cycle_id = state.cycle_id if active_cycle else f"{date_tag}-{code}-{uuid4().hex[:8]}"
        if plan.action == "BUY" and not active_cycle:
            campaigns.start_cycle(
                env=env,
                strategy=STRATEGY,
                code=code,
                cycle_id=cycle_id,
                started_on=now_kst().date(),
            )
        key = f"{date_tag}|{STRATEGY}|{code}|{side}|cycle={cycle_id}|n={state.deployed_tranches}|reason={plan.reason}"
        order_id, created = orders.create_intent_idempotent(
            env=env,
            run_id=run_id,
            strategy=STRATEGY,
            sid=SID,
            mode=MODE,
            code=code,
            market="KOSPI",
            side=side,
            ord_type="LIMIT",
            qty=plan.qty,
            limit_price=order_price,
            stage=STAGE,
            client_order_key=key,
            request_json={**payload, "cycle_id": cycle_id, "tranche_count": plan.tranche_count},
            status="CREATED",
        )
        if not created:
            logger.warning("[KR_INF][ORDER][SKIP] reason=duplicate key=%s", key)
            runs.finish_run(run_id, "OK", notes="duplicate_order")
            return 0
        if dry_run:
            logger.info("[KR_INF][ORDER][DRY] side=%s code=%s qty=%s limit=%s key=%s", side, code, plan.qty, order_price, key)
            runs.finish_run(run_id, "OK", notes="dry_run_intent")
            return 0

        if side == "BUY":
            resp = kis.buy_stock_limit(code, plan.qty, order_price)
        else:
            resp = kis.sell_stock_limit(code, plan.qty, order_price)
        kis_odno = ((resp or {}).get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        ok = bool(isinstance(resp, dict) and str(resp.get("rt_cd")) == "0")
        if ok:
            orders.mark_submitted(env, key, kis_odno, resp)
            orders.mark_acked(env, kis_odno, resp)
            logger.info("[KR_INF][ORDER][ACK] side=%s code=%s qty=%s limit=%s odno=%s fill_assumed=0", side, code, plan.qty, order_price, kis_odno)
            _append_event(ledger, env=env, run_id=run_id, code=code, side=side, qty=plan.qty, price=order_price, ok=True, reasons=["broker_acked_not_filled", plan.reason], payload={"response": resp, "cycle_id": cycle_id, "tranche_count": plan.tranche_count})
            runs.finish_run(run_id, "OK", notes="broker_acked_not_filled")
            return 0

        orders.mark_error(env, key, resp if isinstance(resp, dict) else {"response": resp})
        logger.error("[KR_INF][ORDER][FAIL] side=%s code=%s qty=%s resp=%s", side, code, plan.qty, resp)
        runs.finish_run(run_id, "ERROR", notes="order_rejected")
        return 2
    except Exception as exc:
        logger.exception("[KR_INF][FAIL] %s", exc)
        runs.finish_run(run_id, "ERROR", notes=str(exc)[:500])
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="KR leveraged ETF risk-bounded infinite accumulation runner")
    parser.add_argument("--env", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    env = _resolve_env(args.env)
    dry_run = bool(args.dry_run or _env_bool("KR_INF_DRY_RUN", True))
    return run_once(env=env, dry_run=dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
