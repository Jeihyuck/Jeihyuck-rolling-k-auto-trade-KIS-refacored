from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List

import pandas as pd

from trader.config import (
    CAP_CAP,
    DAILY_CAPITAL,
    KOSDAQ_HARD_STOP_PCT,
    KOSPI_HARD_STOP_PCT,
    PB1_ENTRY_ENABLED,
    PB1_DAY_SL_R,
    PB1_DAY_TP_R,
    PB1_R_FLOOR_PCT,
    PB1_TIME_STOP_DAYS,
    PB1_REQUIRE_VOLUME,
)
from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo, UniverseRepo
from trader.kis_wrapper import KisAPI
from trader.strategies.pb1_pullback_close import choose_mode, compute_features, evaluate_setup
from trader.time_utils import now_kst
from trader.utils.env import env_bool
from trader.utils.ohlcv import normalize_ohlcv
from trader.window_router import WindowDecision, resolve_phase

logger = logging.getLogger(__name__)


@dataclass
class CandidateFeature:
    code: str
    market: str
    features: Dict[str, float]
    setup_ok: bool
    reasons: List[str]
    mode: int
    mode_reasons: List[str]
    client_order_key: str | None = None
    planned_qty: int = 0


@dataclass
class RunResult:
    status: str
    notes: str | None = None


class PB1Engine:
    STRATEGY_NAME = "pb1_pullback_close"
    UNIVERSE_STRATEGY = "best_k_meta"

    def __init__(
        self,
        *,
        universe_repo: UniverseRepo,
        orders_repo: OrdersRepo,
        fills_repo: FillsRepo,
        positions_repo: PositionsRepo,
        kis: KisAPI | None,
        window: WindowDecision,
        phase_override: str,
        dry_run: bool,
        env: str,
        run_id: str,
    ) -> None:
        self.universe_repo = universe_repo
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.kis = kis
        self.window = window
        self.phase = resolve_phase(window, phase_override)
        self.dry_run = dry_run
        self.env = env
        self.run_id = run_id
        self.require_volume = env_bool("PB1_REQUIRE_VOLUME", PB1_REQUIRE_VOLUME)
        self._today = now_kst().date().isoformat()
        self._universe_as_of = None

    def _client_order_key(self, code: str, mode: int, side: str, window_tag: str, stage: str) -> str:
        return f"{self._today}|{code}|sid=1|mode={mode}|{side}|{window_tag}|{stage}"

    def _log_setup(self, cf: CandidateFeature) -> None:
        prefix = "[PB1][SETUP-OK]" if cf.setup_ok else "[PB1][SETUP-BAD]"
        logger.info(
            "%s code=%s market=%s mode=%s reasons=%s features=%s",
            prefix,
            cf.code,
            cf.market,
            cf.mode,
            cf.reasons or ["n/a"],
            {k: cf.features.get(k) for k in ["close", "ma20", "ma50", "pullback_pct", "vol_contraction", "volu_contraction"]},
        )

    def _fetch_daily(self, code: str, count: int = 120) -> tuple[pd.DataFrame, Dict]:
        if not self.kis:
            return pd.DataFrame(), {"volume_missing": True, "source_cols": [], "mapped": {}}
        try:
            candles = self.kis.safe_get_daily_candles(code, count=count)
        except Exception:
            logger.exception("[PB1][DATA][FAIL] code=%s", code)
            return pd.DataFrame(), {"volume_missing": True, "source_cols": [], "mapped": {}}
        if not candles:
            return pd.DataFrame(), {"volume_missing": True, "source_cols": [], "mapped": {}}
        df = pd.DataFrame(candles).copy()
        if df.empty:
            return df, {"volume_missing": True, "source_cols": [], "mapped": {}}

        df_norm, meta = normalize_ohlcv(df)
        return df_norm, meta

    def _compute_candidates(self, members: Iterable[dict]) -> List[CandidateFeature]:
        candidates: List[CandidateFeature] = []
        for m in members:
            code = str(m.get("code") or "").zfill(6)
            market = m.get("market") or ""
            try:
                df, meta = self._fetch_daily(code, count=120)
                if df.empty:
                    cf = CandidateFeature(
                        code=code,
                        market=market,
                        features={"reasons": ["data_empty"]},
                        setup_ok=False,
                        reasons=["data_empty"],
                        mode=1,
                        mode_reasons=["default_day_mode"],
                    )
                    self._log_setup(cf)
                    candidates.append(cf)
                    continue
                features = compute_features(df)
                features["market"] = market
                features["volume_missing"] = bool(meta.get("volume_missing"))
                if features.get("volume_missing"):
                    features["volu_contraction"] = None
                ok, reasons = evaluate_setup(features, market, require_volume=self.require_volume)
                if features.get("volume_missing") and "volume_missing" not in reasons:
                    reasons.append("volume_missing")
                if ok:
                    reasons = []
                elif not reasons:
                    reasons = ["unspecified_fail"]
                mode, mode_reasons = choose_mode(features)
                cf = CandidateFeature(
                    code=code,
                    market=market,
                    features=features,
                    setup_ok=ok,
                    reasons=reasons,
                    mode=mode,
                    mode_reasons=mode_reasons,
                )
                self._log_setup(cf)
                candidates.append(cf)
            except Exception:
                logger.exception("[PB1][DAILY] fetch/normalize failed code=%s", code)
                continue
        return candidates

    def _size_positions(self, candidates: List[CandidateFeature]) -> List[CandidateFeature]:
        ok_list = [c for c in candidates if c.setup_ok]
        total = len(ok_list)
        if total <= 0:
            return candidates
        capital_per = DAILY_CAPITAL * CAP_CAP / total
        for cf in ok_list:
            close_px = cf.features.get("close") or 0
            qty = int(capital_per // close_px) if close_px > 0 else 0
            cf.planned_qty = max(qty, 0)
            cf.client_order_key = self._client_order_key(
                cf.code, cf.mode, "BUY", "close", "PB1"
            )
            if cf.planned_qty <= 0:
                cf.setup_ok = False
                cf.reasons.append("planned_qty_zero")
                self._log_setup(cf)
        return candidates

    def _mark_price(self, code: str) -> float | None:
        if self.kis:
            try:
                quote = self.kis.get_price_quote(code)
                if isinstance(quote, dict):
                    pr = quote.get("stck_prpr") or quote.get("prpr")
                    return float(pr) if pr is not None else None
            except Exception:
                logger.exception("[PB1][PRICE][FAIL] code=%s", code)
        return None

    def _fetch_marks(self, codes: Iterable[str], fallback: Dict[str, float]) -> Dict[str, float]:
        marks: Dict[str, float] = {}
        for code in codes:
            px = self._mark_price(code)
            if px is None:
                px = fallback.get(code)
            if px is not None:
                marks[code] = px
        return marks

    def _should_block_order(self, client_order_key: str) -> bool:
        if not client_order_key:
            return True
        return self.orders_repo.has_client_order_key(self.env, client_order_key)

    def _place_entry(self, cf: CandidateFeature) -> None:
        order_id = self.orders_repo.create_intent_idempotent(
            env=self.env,
            run_id=self.run_id,
            strategy=self.STRATEGY_NAME,
            sid=1,
            mode=cf.mode,
            code=cf.code,
            market=cf.market,
            side="BUY",
            ord_type="MARKET",
            qty=cf.planned_qty,
            limit_price=cf.features.get("close"),
            stage="PB1-CLOSE",
            client_order_key=cf.client_order_key or "",
            request_json={"features": cf.features, "reasons": cf.reasons},
        )
        if self.dry_run:
            logger.info("[PB1][ENTRY-DRY] code=%s qty=%s key=%s order_id=%s", cf.code, cf.planned_qty, cf.client_order_key, order_id)
            return
        if not self.kis:
            logger.warning("[PB1][ENTRY][SKIP] KIS missing code=%s", cf.code)
            return
        resp = None
        kis_odno = None
        try:
            resp = self.kis.buy_stock_market(cf.code, cf.planned_qty)
            kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        except Exception:
            logger.exception("[PB1][ENTRY][FAIL] code=%s", cf.code)
        self.orders_repo.mark_submitted(self.env, cf.client_order_key or "", kis_odno, resp if isinstance(resp, dict) else {"resp": resp})
        if resp and isinstance(resp, dict) and resp.get("rt_cd") == "0":
            self.orders_repo.mark_acked(self.env, kis_odno, resp)
            filled_at = now_kst()
            self.fills_repo.upsert_fill(
                env=self.env,
                run_id=self.run_id,
                order_id=order_id,
                kis_odno=kis_odno,
                trade_id=None,
                code=cf.code,
                market=cf.market,
                side="BUY",
                qty=cf.planned_qty,
                price=cf.features.get("close") or 0.0,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
                raw_json=resp,
            )
            self.positions_repo.apply_fill(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=cf.mode,
                code=cf.code,
                market=cf.market,
                side="BUY",
                qty=cf.planned_qty,
                price=cf.features.get("close") or 0.0,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
            )
        else:
            self.orders_repo.mark_error(self.env, cf.client_order_key or "", resp if isinstance(resp, dict) else {"resp": resp})

    def _plan_exit_event(self, pos: Dict, features: Dict[str, float], window_tag: str) -> None:
        avg = pos.get("avg_buy_price")
        if not avg:
            return
        code = pos.get("code")
        market = pos.get("market")
        mode = pos.get("mode")
        if pos.get("sid") != 1:
            return
        qty = pos.get("qty") or 0
        if qty <= 0:
            return
        mark = self._mark_price(code) or features.get("close") or avg
        ret_pct = ((mark - avg) / avg) * 100 if avg else 0.0
        client_key = self._client_order_key(code, mode, "SELL", window_tag, "exit")
        if self._should_block_order(client_key):
            logger.info("[PB1][EXIT-SKIP] code=%s mode=%s reason=dup key=%s", code, mode, client_key)
            return

        if mode == 1:
            atr_pct = ((features.get("atr14") or 0.0) / avg) * 100
            r_pct = max(PB1_R_FLOOR_PCT, atr_pct)
            take_profit = PB1_DAY_TP_R * r_pct
            stop_loss = PB1_DAY_SL_R * r_pct
            if window_tag != "morning":
                return
            stage = "DAY-EXIT"
            reasons: list[str] = []
            if ret_pct >= take_profit:
                reasons.append("take_profit")
            if ret_pct <= -stop_loss:
                reasons.append("stop_loss")
            if not reasons:
                reasons.append("time_exit")
        else:
            hard_stop = KOSDAQ_HARD_STOP_PCT if market == "KOSDAQ" else KOSPI_HARD_STOP_PCT
            if ret_pct <= -hard_stop:
                stage = "HARD-STOP"
                if window_tag not in {"morning", "close"}:
                    return
            else:
                if window_tag != "close":
                    return
                close_px = features.get("close")
                ma20 = features.get("ma20")
                holding_days = pos.get("holding_days") or 0
                if holding_days >= PB1_TIME_STOP_DAYS:
                    stage = "TIME-STOP"
                elif close_px is not None and ma20 is not None and close_px < ma20:
                    stage = "MA20-TRAIL"
                else:
                    return
            reasons = ["pb1_exit"]

        order_id = self.orders_repo.create_intent_idempotent(
            env=self.env,
            run_id=self.run_id,
            strategy=self.STRATEGY_NAME,
            sid=1,
            mode=mode,
            code=code,
            market=market,
            side="SELL",
            ord_type="MARKET",
            qty=qty,
            limit_price=mark,
            stage=stage,
            client_order_key=client_key,
            request_json={"reasons": reasons, "ret_pct": ret_pct},
        )
        if self.dry_run:
            logger.info("[PB1][EXIT-DRY] code=%s qty=%s key=%s order_id=%s", code, qty, client_key, order_id)
            return
        if not self.kis:
            logger.warning("[PB1][EXIT][SKIP] kis missing code=%s", code)
            return
        resp = None
        kis_odno = None
        try:
            resp = self.kis.sell_stock_market(code, qty)
            kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        except Exception:
            logger.exception("[PB1][EXIT][FAIL] code=%s", code)
        self.orders_repo.mark_submitted(self.env, client_key, kis_odno, resp if isinstance(resp, dict) else {"resp": resp})
        if resp and isinstance(resp, dict) and resp.get("rt_cd") == "0":
            self.orders_repo.mark_acked(self.env, kis_odno, resp)
            filled_at = now_kst()
            self.fills_repo.upsert_fill(
                env=self.env,
                run_id=self.run_id,
                order_id=order_id,
                kis_odno=kis_odno,
                trade_id=None,
                code=code,
                market=market,
                side="SELL",
                qty=qty,
                price=mark,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
                raw_json=resp,
            )
            self.positions_repo.apply_fill(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                market=market,
                side="SELL",
                qty=qty,
                price=mark,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
            )
        else:
            self.orders_repo.mark_error(self.env, client_key, resp if isinstance(resp, dict) else {"resp": resp})

    def _positions_with_meta(self, positions: Iterable[Dict]) -> List[Dict]:
        enriched: List[Dict] = []
        for state in positions:
            if state.get("sid") != 1:
                continue
            enriched.append(
                {
                    "code": state.get("code"),
                    "sid": state.get("sid"),
                    "mode": state.get("mode"),
                    "qty": state.get("qty") or 0,
                    "avg_buy_price": state.get("avg_buy_price"),
                    "market": state.get("market"),
                    "holding_days": state.get("holding_days") or 0,
                    "first_buy_ts": state.get("first_buy_ts"),
                    "total_cost": state.get("total_cost") or 0.0,
                    "realized_pnl": state.get("realized_pnl") or 0.0,
                }
            )
        return enriched

    def _load_universe(self) -> list[dict]:
        today = now_kst().date().isoformat()
        members = self.universe_repo.get_universe_members(self.env, self.UNIVERSE_STRATEGY, today)
        self._universe_as_of = today if members else None
        if not members:
            members = self.universe_repo.get_latest_universe_members(self.env, self.UNIVERSE_STRATEGY)
            if members:
                self._universe_as_of = members[0].get("as_of_date")
        return members

    def _pnl_snapshot(self, positions: List[Dict]) -> Dict[str, float]:
        fallback: Dict[str, float] = {p["code"]: p.get("avg_buy_price") or 0.0 for p in positions}
        marks = self._fetch_marks([p["code"] for p in positions], fallback)
        totals = {"market_value": 0.0, "cost": 0.0, "unrealized": 0.0, "realized": 0.0}
        for pos in positions:
            qty = pos.get("qty") or 0
            mark = marks.get(pos["code"], pos.get("avg_buy_price") or 0.0)
            market_value = float(mark) * qty
            cost = float(pos.get("total_cost") or 0.0)
            totals["market_value"] += market_value
            totals["cost"] += cost
            totals["realized"] += float(pos.get("realized_pnl") or 0.0)
            totals["unrealized"] += market_value - cost
        portfolio_return_pct = 0.0
        if totals["cost"] > 0:
            portfolio_return_pct = (totals["market_value"] - totals["cost"] + totals["realized"]) / totals["cost"] * 100
        logger.info(
            "[PNL][SNAPSHOT] universe_as_of=%s market_value=%.2f cost=%.2f unrealized=%.2f realized=%.2f return_pct=%.2f",
            self._universe_as_of or "none",
            totals["market_value"],
            totals["cost"],
            totals["unrealized"],
            totals["realized"],
            portfolio_return_pct,
        )
        return totals

    def run(self) -> RunResult:
        entry_allowed = PB1_ENTRY_ENABLED and env_bool("PB1_ENTRY_ENABLED", PB1_ENTRY_ENABLED)
        if not entry_allowed:
            logger.warning("[PB1][ENTRY_DISABLED] PB1_ENTRY_ENABLED=%s -> skip new entries", entry_allowed)
        logger.info("[PB1][RUN] window=%s phase=%s dry_run=%s env=%s", self.window.name, self.phase, self.dry_run, self.env)

        members = self._load_universe()
        if not members:
            note = "universe_empty"
            logger.warning("[PB1][UNIVERSE][EMPTY] env=%s strategy=%s", self.env, self.UNIVERSE_STRATEGY)
            return RunResult(status="SKIPPED", notes=note)

        positions = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
        if self.phase in {"verify", "exit"}:
            holdings = []
            if self.kis:
                try:
                    holdings = self.kis.get_positions()
                except Exception:
                    logger.exception("[PB1][HOLDINGS][FAIL]")
            if not positions and holdings:
                bootstrapped = self.positions_repo.bootstrap_from_kis_holdings(
                    env=self.env,
                    strategy=self.STRATEGY_NAME,
                    sid=1,
                    mode=1,
                    holdings=holdings,
                )
                logger.info("[PB1][BOOTSTRAP] positions_inserted=%s", bootstrapped)
                positions = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
            open_orders = self.orders_repo.get_open_orders(self.env)
            if open_orders:
                logger.info("[PB1][ORDERS][OPEN] count=%s", len(open_orders))
            self._pnl_snapshot(self._positions_with_meta(positions))
            if self.phase == "verify":
                return RunResult(status="OK", notes="verify_only")

        code_market = {m.get("code"): m.get("market") for m in members}
        marks_fallback: Dict[str, float] = {}
        if self.phase in {"prep", "entry"}:
            candidates = self._compute_candidates(members)
            candidates = self._size_positions(candidates)
            if self.phase == "entry" and self.window.name == "afternoon":
                for cf in candidates:
                    if not cf.setup_ok or self._should_block_order(cf.client_order_key or ""):
                        continue
                    if not entry_allowed:
                        continue
                    self._place_entry(cf)
        if self.phase in {"exit", "verify"}:
            pos_list = self._positions_with_meta(positions)
            for pos in pos_list:
                df, _ = self._fetch_daily(pos["code"], count=120)
                if df.empty:
                    continue
                features = compute_features(df)
                features["market"] = pos.get("market") or code_market.get(pos["code"], "")
                marks_fallback[pos["code"]] = features.get("close") or pos.get("avg_buy_price") or 0.0
                self._plan_exit_event(pos, features, "morning" if self.window.name == "morning" else "close")
        if self.phase == "entry" and self.window.name == "afternoon":
            pos_list = self._positions_with_meta(positions)
            for pos in pos_list:
                df, _ = self._fetch_daily(pos["code"], count=120)
                if df.empty:
                    continue
                features = compute_features(df)
                features["market"] = pos.get("market") or code_market.get(pos["code"], "")
                marks_fallback[pos["code"]] = features.get("close") or pos.get("avg_buy_price") or 0.0
                self._plan_exit_event(pos, features, "close")
        self._pnl_snapshot(self._positions_with_meta(self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)))
        return RunResult(status="OK", notes=self._universe_as_of or "ok")
