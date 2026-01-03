from __future__ import annotations

import logging
import json
from dataclasses import dataclass
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance
from trader.config import (
    CAP_CAP,
    DAILY_CAPITAL,
    KOSDAQ_HARD_STOP_PCT,
    KOSPI_HARD_STOP_PCT,
    LEDGER_BASE_DIR,
    LEDGER_LOOKBACK_DAYS,
    PB1_ENTRY_ENABLED,
    PB1_DAY_SL_R,
    PB1_DAY_TP_R,
    PB1_R_FLOOR_PCT,
    PB1_TIME_STOP_DAYS,
    PB1_REQUIRE_VOLUME,
)
from trader.utils.env import env_bool
from trader.kis_wrapper import KisAPI
from trader.ledger.event_types import new_error, new_exit_intent, new_order_intent, new_fill, new_order_ack, new_unfilled, new_shadow_check
from trader.ledger.store import LedgerStore
from trader.strategies.pb1_pullback_close import choose_mode, compute_features, evaluate_setup
from trader.utils.ohlcv import normalize_ohlcv
from trader.time_utils import now_kst
from trader.window_router import WindowDecision, resolve_phase
from trader.botstate_sync import persist_run_files

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


class PB1Engine:
    def __init__(
        self,
        *,
        kis: KisAPI | None,
        worktree_dir: Path,
        window: WindowDecision,
        phase_override: str,
        dry_run: bool,
        env: str,
        run_id: str,
        order_mode: str = "live",
        diag_level: int = 1,
        shadow_skip_reason: str | None = None,
    ) -> None:
        self.kis = kis
        self.worktree_dir = worktree_dir
        self.window = window
        self.phase = resolve_phase(window, phase_override)
        self.dry_run = dry_run
        self.env = env
        self.run_id = run_id
        self.order_mode = order_mode
        self.diag_level = diag_level
        base_dir = LEDGER_BASE_DIR
        if not Path(base_dir).is_absolute():
            base_dir = worktree_dir / base_dir
        self.ledger = LedgerStore(Path(base_dir), env=env, run_id=run_id)
        self.worktree_dir = worktree_dir
        self._today = now_kst().date().isoformat()
        self.require_volume = env_bool("PB1_REQUIRE_VOLUME", PB1_REQUIRE_VOLUME)
        self.diag_counters: Dict[str, object] = {
            "universe_size": 0,
            "candidates": 0,
            "entry_orders": 0,
            "exit_orders": 0,
            "preflight_ok": 0,
            "preflight_fail": 0,
            "fail_reasons": [],
        }
        self.shadow_skip_reason = shadow_skip_reason
        self.executor = self._build_executor()

    def _build_executor(self):
        if self.order_mode == "shadow":
            return ShadowExecutor(
                kis=self.kis,
                ledger=self.ledger,
                env=self.env,
                run_id=self.run_id,
                worktree_dir=self.worktree_dir,
                diag_counters=self.diag_counters,
                shadow_skip_reason=self.shadow_skip_reason,
            )
        if self.order_mode in {"dry_run", "intent_only"}:
            return DryRunExecutor(
                ledger=self.ledger,
                env=self.env,
                run_id=self.run_id,
                diag_counters=self.diag_counters,
            )
        return LiveExecutor(
            kis=self.kis,
            ledger=self.ledger,
            env=self.env,
            run_id=self.run_id,
            worktree_dir=self.worktree_dir,
            diag_counters=self.diag_counters,
        )

    def _client_order_key(self, code: str, mode: int, side: str, stage: str, window_tag: str) -> str:
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

    def _build_universe(self) -> Dict[str, List[Dict]]:
        """
        Build selection universe for PB1 without triggering any legacy order flows.
        run_rebalance() in best_k_meta_strategy is selection-only and returns weights.
        """
        cache_path = self.worktree_dir / "bot_state" / "trader_state" / "universe_cache.json"
        today_str = now_kst().date().isoformat()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        selected_by_market: Dict[str, List[Dict]] = {}
        cache_loaded = False
        try:
            rebalance_payload = run_rebalance(str(now_kst().date()), return_by_market=True)
            selected_by_market = rebalance_payload.get("selected_by_market") or {}
            if selected_by_market:
                try:
                    with open(cache_path, "w", encoding="utf-8") as f:
                        json.dump({"date": today_str, "selected_by_market": selected_by_market, "source": "pykrx"}, f, ensure_ascii=False, indent=2)
                    logger.info("[PB1][UNIVERSE][CACHE-SAVE] path=%s", cache_path)
                except Exception:
                    logger.exception("[PB1][UNIVERSE][CACHE-SAVE][FAIL]")
            else:
                raise ValueError("empty_universe")
        except Exception:
            logger.exception("[PB1][UNIVERSE][FAIL]")
            if cache_path.exists():
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        cached = json.load(f)
                        selected_by_market = cached.get("selected_by_market") or {}
                        cache_loaded = True
                        logger.warning("[PB1][UNIVERSE][FALLBACK]=cache path=%s source=%s", cache_path, cached.get("source"))
                except Exception:
                    logger.exception("[PB1][UNIVERSE][CACHE-LOAD][FAIL]")
        self.diag_counters["universe_size"] = sum(len(v or []) for v in (selected_by_market or {}).values())
        if not selected_by_market:
            raise RuntimeError("universe_build_failed")
        if cache_loaded:
            self.diag_counters["fail_reasons"].append("pykrx_universe_empty")
        return selected_by_market

    def _code_market_map(self, selected_by_market: Dict[str, List[Dict]]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for market, rows in (selected_by_market or {}).items():
            for row in rows or []:
                code = str(row.get("code") or row.get("pdno") or "").zfill(6)
                mapping[code] = market
        return mapping

    def _compute_candidates(self, selected_by_market: Dict[str, List[Dict]]) -> List[CandidateFeature]:
        candidates: List[CandidateFeature] = []
        for market, rows in (selected_by_market or {}).items():
            for row in rows or []:
                code = str(row.get("code") or row.get("pdno") or "").zfill(6)
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
        self.diag_counters["candidates"] = len(candidates)
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
                cf.code, cf.mode, "BUY", "PB1", "close"
            )
            if cf.planned_qty <= 0:
                cf.setup_ok = False
                cf.reasons.append("planned_qty_zero")
                self._log_setup(cf)
        return candidates

    def _should_block_order(self, client_order_key: str) -> bool:
        if not client_order_key:
            return True
        return self.ledger.has_client_order_key(client_order_key)

    def _append_intent(self, cf: CandidateFeature) -> None:
        event = new_order_intent(
            code=cf.code,
            market=cf.market,
            sid=1,
            mode=cf.mode,
            env=self.env,
            run_id=self.run_id,
            side="BUY",
            qty=cf.planned_qty,
            price=cf.features.get("close"),
            client_order_key=cf.client_order_key,
            ok=self.order_mode == "live",
            reasons=["dry_run"] if self.order_mode == "dry_run" else ["shadow_mode"] if self.order_mode == "shadow" else ["intent_only"] if self.order_mode == "intent_only" else ["pb1_close_entry"],
            stage="PB1-CLOSE",
            payload={"order_mode": self.order_mode},
        )
        path = self.ledger.append_event("orders_intent", event)
        self.diag_counters["entry_orders"] = int(self.diag_counters.get("entry_orders", 0)) + 1
        logger.info("[PB1][ENTRY-INTENT] code=%s mode=%s qty=%s key=%s path=%s", cf.code, cf.mode, cf.planned_qty, cf.client_order_key, path)

    def _place_entry(self, cf: CandidateFeature) -> List[Path]:
        paths: List[Path] = []
        self._append_intent(cf)
        paths.append(self.ledger._run_file("orders_intent"))
        exec_paths, _ok, _reason = self.executor.submit_order(
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            qty=cf.planned_qty,
            price=cf.features.get("close"),
            client_order_key=cf.client_order_key,
            stage="PB1-CLOSE",
            side="BUY",
        )
        paths.extend(exec_paths)
        return paths

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

    def _plan_exit_event(self, pos: Dict, features: Dict[str, float], window_tag: str) -> None:
        avg = pos.get("avg_buy_price")
        if not avg:
            return
        code = pos.get("code")
        market = pos.get("market")
        mode = pos.get("mode")
        if pos.get("sid") != 1:
            return
        qty = pos.get("total_qty") or 0
        if qty <= 0:
            return
        mark = self._mark_price(code) or features.get("close")
        if not mark:
            return
        ret_pct = ((mark - avg) / avg) * 100
        client_key = self._client_order_key(code, mode, "SELL", window_tag, "exit")
        if self._should_block_order(client_key):
            logger.info("[PB1][EXIT-SKIP] code=%s mode=%s reason=dup key=%s", code, mode, client_key)
            return

        if mode == 1:
            atr_pct = ((features.get("atr14") or 0.0) / avg) * 100
            r_pct = max(PB1_R_FLOOR_PCT, atr_pct)
            take_profit = PB1_DAY_TP_R * r_pct
            stop_loss = PB1_DAY_SL_R * r_pct
            trigger_tp = ret_pct >= take_profit
            trigger_sl = ret_pct <= -stop_loss
            if window_tag != "morning":
                return
            stage = "DAY-EXIT"
            reasons = ["take_profit"] if trigger_tp else []
            if trigger_sl:
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
        event = new_exit_intent(
            code=code,
            market=market,
            sid=1,
            mode=mode,
            env=self.env,
            run_id=self.run_id,
            side="SELL",
            qty=qty,
            price=mark,
            client_order_key=client_key,
            ok=self.order_mode == "live",
            reasons=["dry_run"] if self.order_mode == "dry_run" else ["shadow_mode"] if self.order_mode == "shadow" else reasons,
            stage=stage,
            payload={"order_mode": self.order_mode},
        )
        path = self.ledger.append_event("exits_intent", event)
        logger.info("[PB1][EXIT-INTENT] code=%s mode=%s stage=%s ret_pct=%.2f key=%s path=%s", code, mode, stage, ret_pct, client_key, path)
        self.diag_counters["exit_orders"] = int(self.diag_counters.get("exit_orders", 0)) + 1
        self.executor.submit_exit(
            code=code,
            market=market,
            mode=mode,
            qty=qty,
            price=mark,
            client_order_key=client_key,
            stage=stage,
            side="SELL",
        )

    def _positions_with_meta(self, positions: Dict[Tuple[str, int, int], Dict]) -> List[Dict]:
        enriched: List[Dict] = []
        for (code, sid, mode), state in positions.items():
            if sid != 1:
                continue
            enriched.append(
                {
                    "code": code,
                    "sid": sid,
                    "mode": mode,
                    "total_qty": state.get("total_qty") or 0,
                    "avg_buy_price": state.get("avg_buy_price"),
                    "market": state.get("market"),
                    "holding_days": state.get("holding_days") or 0,
                    "first_buy_ts": state.get("first_buy_ts"),
                }
            )
        return enriched

    def _is_diagnostic_guard_enabled(self) -> bool:
        skip_env = os.getenv("PB1_SKIP_UNIVERSE_IN_DIAG") == "1"
        return bool(
            self.order_mode == "dry_run"
            and (
                (self.window and self.window.name == "diagnostic")
                or (skip_env)
                or (self.phase == "verify")
            )
        )

    def _run_verify_only(self, positions: Dict[Tuple[str, int, int], Dict], touched: List[Path]) -> List[Path]:
        logger.info(
            "[PB1][DIAG-MODE] universe/rebalance skipped window=%s phase=%s dry_run=%s",
            self.window.name,
            self.phase,
            self.dry_run,
        )
        marks = self._fetch_marks([p["code"] for p in self._positions_with_meta(positions)], {})
        snapshot = self.ledger.generate_pnl_snapshot(positions, marks=marks)
        logger.info(
            "[PNL][SNAPSHOT] portfolio_return_pct=%.2f%% unrealized=%.2f realized=%.2f",
            snapshot["totals"]["portfolio_return_pct"],
            snapshot["totals"]["unrealized"],
            snapshot["totals"]["realized"],
        )
        snap_path = self.ledger.write_snapshot(snapshot, self.run_id)
        touched.append(snap_path)
        touched = self._emit_diag_summary(touched)
        return touched

    def run(self) -> List[Path]:
        entry_allowed = PB1_ENTRY_ENABLED and env_bool("PB1_ENTRY_ENABLED", PB1_ENTRY_ENABLED)
        if not entry_allowed:
            logger.warning("[PB1][ENTRY_DISABLED] PB1_ENTRY_ENABLED=%s -> skip new entries", entry_allowed)
        logger.info("[PB1][RUN] window=%s phase=%s dry_run=%s", self.window.name, self.phase, self.dry_run)
        run_files = self.ledger.open_run_files()
        touched: List[Path] = list(run_files.values())
        logger.info("[LEDGER][APPEND] kind=touch path=%s", run_files)
        persist_run_files(self.worktree_dir, touched, message=f"pb1 touch run_id={self.run_id}")
        positions = self.ledger.rebuild_positions_average_cost(lookback_days=LEDGER_LOOKBACK_DAYS)
        if self._is_diagnostic_guard_enabled():
            return self._run_verify_only(positions, touched)
        try:
            selected = self._build_universe()
        except Exception as exc:
            logger.exception("[PB1][UNIVERSE][ERROR]")
            err = new_error(
                code="000000",
                market="",
                sid=1,
                mode=0,
                env=self.env,
                run_id=self.run_id,
                reasons=["universe_build_failed", str(exc)],
            )
            touched.append(self.ledger.append_event("errors", err))
            self.diag_counters["fail_reasons"].append("universe_build_failed")
            return touched
        code_market = self._code_market_map(selected)
        marks_fallback: Dict[str, float] = {}
        if self.phase in {"prep", "entry"}:
            candidates = self._compute_candidates(selected)
            candidates = self._size_positions(candidates)
            if self.phase == "entry" and self.window.name == "afternoon":
                code_hint = candidates[0].code if candidates else next(iter(code_market), "005930")
                price_hint = candidates[0].features.get("close") if candidates else None
                orderable_cash = 0
                cash_meta: Dict[str, object] = {"source": "none", "raw_fields": {}, "clamp_applied": False}
                if self.kis:
                    orderable_cash, cash_meta = self.kis.get_orderable_cash(code_hint, price_hint)
                if orderable_cash < 0:
                    orderable_cash = 0
                logger.info(
                    "[PB1][CASH][ORDERABLE] value=%s source=%s clamp=%s raw_fields=%s",
                    orderable_cash,
                    cash_meta.get("source"),
                    cash_meta.get("clamp_applied"),
                    {
                        k: (cash_meta.get("raw_fields") or {}).get(k)
                        for k in ("ord_psbl_cash", "ord_psbl_amt", "nrcvb_buy_amt", "dnca_tot_amt")
                    },
                )
                cash_block_logged = False
                for cf in candidates:
                    if not cf.setup_ok:
                        continue
                    if self._should_block_order(cf.client_order_key):
                        continue
                    if orderable_cash <= 0:
                        if not cash_block_logged:
                            logger.info(
                                "[PB1][CASH-BLOCK] orderable=%s meta=%s reason=insufficient_cash",
                                orderable_cash,
                                {
                                    "source": cash_meta.get("source"),
                                    "raw_fields": {
                                        k: (cash_meta.get("raw_fields") or {}).get(k)
                                        for k in ("ord_psbl_cash", "ord_psbl_amt", "nrcvb_buy_amt", "dnca_tot_amt")
                                    },
                                    "clamp_applied": cash_meta.get("clamp_applied"),
                                },
                            )
                            cash_block_logged = True
                        continue
                    if not entry_allowed:
                        continue
                    paths = self._place_entry(cf)
                    touched.extend(paths)
        elif self.phase in {"exit", "verify"}:
            pos_list = self._positions_with_meta(positions)
            for pos in pos_list:
                df, _ = self._fetch_daily(pos["code"], count=120)
                if df.empty:
                    err = new_error(
                        code=pos["code"],
                        market=pos.get("market") or "",
                        sid=1,
                        mode=pos["mode"],
                        env=self.env,
                        run_id=self.run_id,
                        reasons=["daily_data_missing"],
                    )
                    self.ledger.append_event("errors", err)
                    continue
                features = compute_features(df)
                features["market"] = pos.get("market") or code_market.get(pos["code"], "")
                marks_fallback[pos["code"]] = features.get("close")
                if self.phase == "exit":
                    self._plan_exit_event(pos, features, "morning" if self.window.name == "morning" else "close")
        if self.phase == "entry" and self.window.name == "afternoon":
            pos_list = self._positions_with_meta(positions)
            for pos in pos_list:
                df, _ = self._fetch_daily(pos["code"], count=120)
                if df.empty:
                    continue
                features = compute_features(df)
                features["market"] = pos.get("market") or code_market.get(pos["code"], "")
                marks_fallback[pos["code"]] = features.get("close")
                self._plan_exit_event(pos, features, "close")
        marks = self._fetch_marks([p["code"] for p in self._positions_with_meta(positions)], marks_fallback)
        snapshot = self.ledger.generate_pnl_snapshot(positions, marks=marks)
        logger.info(
            "[PNL][SNAPSHOT] portfolio_return_pct=%.2f%% unrealized=%.2f realized=%.2f",
            snapshot["totals"]["portfolio_return_pct"],
            snapshot["totals"]["unrealized"],
            snapshot["totals"]["realized"],
        )
        snap_path = self.ledger.write_snapshot(snapshot, self.run_id)
        touched.append(snap_path)
        touched = self._emit_diag_summary(touched)
        return touched

    def _emit_diag_summary(self, touched: List[Path]) -> List[Path]:
        if self.diag_level <= 0:
            return touched
        fail_reasons = list(self.diag_counters.get("fail_reasons") or [])
        if not fail_reasons:
            fail_reasons = ["none"]
        summary = {
            "level": self.diag_level,
            "order_mode": self.order_mode,
            "universe": int(self.diag_counters.get("universe_size", 0)),
            "candidates": int(self.diag_counters.get("candidates", 0)),
            "entry_intents": int(self.diag_counters.get("entry_orders", 0)),
            "exit_intents": int(self.diag_counters.get("exit_orders", 0)),
            "preflight_ok": int(self.diag_counters.get("preflight_ok", 0)),
            "preflight_fail": int(self.diag_counters.get("preflight_fail", 0)),
            "fail_reasons": fail_reasons,
            "ts": now_kst().isoformat(),
        }
        logger.info(
            "[PB1][DIAG-SUMMARY] level=%s mode=%s universe=%s candidates=%s entry_intents=%s exit_intents=%s preflight_ok=%s preflight_fail=%s reasons=%s",
            summary["level"],
            summary["order_mode"],
            summary["universe"],
            summary["candidates"],
            summary["entry_intents"],
            summary["exit_intents"],
            summary["preflight_ok"],
            summary["preflight_fail"],
            fail_reasons,
        )
        path = self.ledger.base_dir / "reports" / self._today / "diag_summary.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        touched.append(path)
        return touched


class OrderExecutor:
    def __init__(self, *, ledger: LedgerStore, env: str, run_id: str, diag_counters: Dict[str, object]) -> None:
        self.ledger = ledger
        self.env = env
        self.run_id = run_id
        self.diag_counters = diag_counters

    def _bump_preflight(self, ok: bool, reason: str | None) -> None:
        key = "preflight_ok" if ok else "preflight_fail"
        self.diag_counters[key] = int(self.diag_counters.get(key, 0)) + 1
        if not ok and reason:
            reasons = self.diag_counters.setdefault("fail_reasons", [])
            if reason not in reasons:
                reasons.append(reason)

    def submit_order(
        self,
        *,
        code: str,
        market: str,
        mode: int,
        qty: int,
        price: float | None,
        client_order_key: str,
        stage: str,
        side: str,
    ) -> tuple[List[Path], bool, str | None]:
        raise NotImplementedError

    def submit_exit(
        self,
        *,
        code: str,
        market: str,
        mode: int,
        qty: int,
        price: float | None,
        client_order_key: str,
        stage: str,
        side: str,
    ) -> tuple[List[Path], bool, str | None]:
        raise NotImplementedError


class DryRunExecutor(OrderExecutor):
    def __init__(self, *, ledger: LedgerStore, env: str, run_id: str, diag_counters: Dict[str, object]) -> None:
        super().__init__(ledger=ledger, env=env, run_id=run_id, diag_counters=diag_counters)

    def submit_order(self, **kwargs) -> tuple[List[Path], bool, str | None]:
        self._bump_preflight(True, None)
        return [], True, None

    def submit_exit(self, **kwargs) -> tuple[List[Path], bool, str | None]:
        self._bump_preflight(True, None)
        return [], True, None


class ShadowExecutor(OrderExecutor):
    def __init__(
        self,
        *,
        kis: KisAPI | None,
        ledger: LedgerStore,
        env: str,
        run_id: str,
        worktree_dir: Path,
        diag_counters: Dict[str, object],
        shadow_skip_reason: str | None = None,
    ) -> None:
        super().__init__(ledger=ledger, env=env, run_id=run_id, diag_counters=diag_counters)
        self.kis = kis
        self.worktree_dir = worktree_dir
        self.shadow_skip_reason = shadow_skip_reason

    def _shadow_ack(
        self,
        *,
        code: str,
        market: str,
        mode: int,
        qty: int,
        price: float | None,
        client_order_key: str,
        stage: str,
        side: str,
        ok: bool,
        reason: str | None,
    ) -> Path:
        ack = new_order_ack(
            code=code,
            market=market,
            sid=1,
            mode=mode,
            env=self.env,
            run_id=self.run_id,
            side=side,
            qty=qty,
            price=price,
            odno="",
            client_order_key=client_order_key,
            ok=ok,
            reasons=[] if ok else [reason or "shadow_fail"],
            stage=stage,
            payload={
                "mode": "shadow",
                "preflight_ok": ok,
                "reason": reason,
            },
        )
        return self.ledger.append_event("orders_ack", ack)

    def submit_order(self, **kwargs) -> tuple[List[Path], bool, str | None]:
        code = kwargs.get("code")
        qty = kwargs.get("qty")
        price = kwargs.get("price")
        market = kwargs.get("market")
        mode = kwargs.get("mode")
        client_order_key = kwargs.get("client_order_key")
        stage = kwargs.get("stage")
        side = kwargs.get("side", "BUY")
        paths: List[Path] = []
        if self.shadow_skip_reason:
            ok = False
            reason = self.shadow_skip_reason
            payload: Dict[str, object] = {"skipped": True, "reason": reason}
        elif not self.kis:
            ok = False
            reason = "kis_missing"
            payload = {"skipped": True, "reason": reason}
        else:
            try:
                result = self.kis.check_orderable(code=code, qty=qty, price=price, side=side, order_type="market")
            except Exception as exc:
                result = {"ok": False, "reason": f"exception:{exc.__class__.__name__}", "error": str(exc)}
            ok = bool(result.get("ok"))
            reason = str(result.get("reason") or ("ok" if ok else "shadow_check_failed"))
            payload = {"result": result}
        self._bump_preflight(ok, reason)
        paths.append(
            self._shadow_ack(
                code=code,
                market=market,
                mode=mode,
                qty=qty,
                price=price,
                client_order_key=client_order_key,
                stage=stage,
                side=side,
                ok=ok,
                reason=reason,
            )
        )
        check_event = new_shadow_check(
            code=code,
            market=market,
            sid=1,
            mode=mode,
            env=self.env,
            run_id=self.run_id,
            side=side,
            qty=qty,
            price=price,
            client_order_key=client_order_key,
            ok=ok,
            reasons=[] if ok else [reason],
            stage=stage,
            payload=payload,
        )
        paths.append(self.ledger.append_event("orders_shadow_check", check_event))
        return paths, ok, reason

    def submit_exit(self, **kwargs) -> tuple[List[Path], bool, str | None]:
        return self.submit_order(**kwargs)


class LiveExecutor(OrderExecutor):
    def __init__(
        self,
        *,
        kis: KisAPI | None,
        ledger: LedgerStore,
        env: str,
        run_id: str,
        worktree_dir: Path,
        diag_counters: Dict[str, object],
    ) -> None:
        super().__init__(ledger=ledger, env=env, run_id=run_id, diag_counters=diag_counters)
        self.kis = kis
        self.worktree_dir = worktree_dir

    def _handle_order(
        self,
        *,
        code: str,
        market: str,
        mode: int,
        qty: int,
        price: float | None,
        client_order_key: str,
        stage: str,
        side: str,
    ) -> tuple[List[Path], bool, str | None]:
        paths: List[Path] = []
        if not self.kis:
            err = new_unfilled(
                code=code,
                market=market,
                sid=1,
                mode=mode,
                env=self.env,
                run_id=self.run_id,
                side=side,
                qty=qty,
                price=price,
                client_order_key=client_order_key,
                reasons=["kis_missing"],
                stage=stage,
            )
            paths.append(self.ledger.append_event("errors", err))
            self._bump_preflight(False, "kis_missing")
            return paths, False, "kis_missing"
        resp = None
        try:
            if side == "BUY":
                resp = self.kis.buy_stock_market(code, qty)
            else:
                resp = self.kis.sell_stock_market(code, qty)
        except Exception:
            logger.exception("[PB1][ORDER][FAIL] code=%s side=%s", code, side)
        odno = ""
        if isinstance(resp, dict):
            odno = (resp.get("output") or {}).get("ODNO") or ""
        ok = bool(resp and resp.get("rt_cd") == "0")
        reason = None
        if not ok:
            if isinstance(resp, dict):
                reason = resp.get("msg1") or resp.get("msg_cd") or "order_failed"
            else:
                reason = "order_failed"
        ack = new_order_ack(
            code=code,
            market=market,
            sid=1,
            mode=mode,
            env=self.env,
            run_id=self.run_id,
            side=side,
            qty=qty,
            price=price,
            odno=odno,
            client_order_key=client_order_key,
            ok=ok,
            reasons=[] if ok else [reason],
            stage=stage,
        )
        paths.append(self.ledger.append_event("orders_ack", ack))
        if ok:
            fill = new_fill(
                code=code,
                market=market,
                sid=1,
                mode=mode,
                env=self.env,
                run_id=self.run_id,
                side=side,
                qty=qty,
                price=price,
                odno=odno,
                client_order_key=client_order_key,
                stage=stage,
            )
            fill_path = self.ledger.append_event("fills", fill)
            paths.append(fill_path)
            persist_run_files(self.worktree_dir, [fill_path], message=f"pb1 fill {self.run_id}")
        else:
            unfilled = new_unfilled(
                code=code,
                market=market,
                sid=1,
                mode=mode,
                env=self.env,
                run_id=self.run_id,
                side=side,
                qty=qty,
                price=price,
                client_order_key=client_order_key,
                reasons=[reason] if reason else ["order_failed"],
                stage=stage,
            )
            paths.append(self.ledger.append_event("errors", unfilled))
        self._bump_preflight(ok, reason or None)
        return paths, ok, reason

    def submit_order(self, **kwargs) -> tuple[List[Path], bool, str | None]:
        return self._handle_order(**kwargs)

    def submit_exit(self, **kwargs) -> tuple[List[Path], bool, str | None]:
        return self._handle_order(**kwargs)

    def run(self) -> List[Path]:
        entry_allowed = PB1_ENTRY_ENABLED and env_bool("PB1_ENTRY_ENABLED", PB1_ENTRY_ENABLED)
        if not entry_allowed:
            logger.warning("[PB1][ENTRY_DISABLED] PB1_ENTRY_ENABLED=%s -> skip new entries", entry_allowed)
        logger.info("[PB1][RUN] window=%s phase=%s dry_run=%s", self.window.name, self.phase, self.dry_run)
        run_files = self.ledger.open_run_files()
        touched: List[Path] = list(run_files.values())
        logger.info("[LEDGER][APPEND] kind=touch path=%s", run_files)
        persist_run_files(self.worktree_dir, touched, message=f"pb1 touch run_id={self.run_id}")
        positions = self.ledger.rebuild_positions_average_cost(lookback_days=LEDGER_LOOKBACK_DAYS)
        if self._is_diagnostic_guard_enabled():
            return self._run_verify_only(positions, touched)
        try:
            selected = self._build_universe()
        except Exception as exc:
            logger.exception("[PB1][UNIVERSE][ERROR]")
            err = new_error(
                code="000000",
                market="",
                sid=1,
                mode=0,
                env=self.env,
                run_id=self.run_id,
                reasons=["universe_build_failed", str(exc)],
            )
            touched.append(self.ledger.append_event("errors", err))
            self.diag_counters["fail_reasons"].append("universe_build_failed")
            return touched
        code_market = self._code_market_map(selected)
        marks_fallback: Dict[str, float] = {}
        if self.phase in {"prep", "entry"}:
            candidates = self._compute_candidates(selected)
            candidates = self._size_positions(candidates)
            if self.phase == "entry" and self.window.name == "afternoon":
                code_hint = candidates[0].code if candidates else next(iter(code_market), "005930")
                price_hint = candidates[0].features.get("close") if candidates else None
                orderable_cash = 0
                cash_meta: Dict[str, object] = {"source": "none", "raw_fields": {}, "clamp_applied": False}
                if self.kis:
                    orderable_cash, cash_meta = self.kis.get_orderable_cash(code_hint, price_hint)
                if orderable_cash < 0:
                    orderable_cash = 0
                logger.info(
                    "[PB1][CASH][ORDERABLE] value=%s source=%s clamp=%s raw_fields=%s",
                    orderable_cash,
                    cash_meta.get("source"),
                    cash_meta.get("clamp_applied"),
                    {
                        k: (cash_meta.get("raw_fields") or {}).get(k)
                        for k in ("ord_psbl_cash", "ord_psbl_amt", "nrcvb_buy_amt", "dnca_tot_amt")
                    },
                )
                cash_block_logged = False
                for cf in candidates:
                    if not cf.setup_ok:
                        continue
                    if self._should_block_order(cf.client_order_key):
                        continue
                    if orderable_cash <= 0:
                        if not cash_block_logged:
                            logger.info(
                                "[PB1][CASH-BLOCK] orderable=%s meta=%s reason=insufficient_cash",
                                orderable_cash,
                                {
                                    "source": cash_meta.get("source"),
                                    "raw_fields": {
                                        k: (cash_meta.get("raw_fields") or {}).get(k)
                                        for k in ("ord_psbl_cash", "ord_psbl_amt", "nrcvb_buy_amt", "dnca_tot_amt")
                                    },
                                    "clamp_applied": cash_meta.get("clamp_applied"),
                                },
                            )
                            cash_block_logged = True
                        continue
                    if not entry_allowed:
                        continue
                    paths = self._place_entry(cf)
                    touched.extend(paths)
        elif self.phase in {"exit", "verify"}:
            pos_list = self._positions_with_meta(positions)
            for pos in pos_list:
                df, _ = self._fetch_daily(pos["code"], count=120)
                if df.empty:
                    err = new_error(
                        code=pos["code"],
                        market=pos.get("market") or "",
                        sid=1,
                        mode=pos["mode"],
                        env=self.env,
                        run_id=self.run_id,
                        reasons=["daily_data_missing"],
                    )
                    self.ledger.append_event("errors", err)
                    continue
                features = compute_features(df)
                features["market"] = pos.get("market") or code_market.get(pos["code"], "")
                marks_fallback[pos["code"]] = features.get("close")
                if self.phase == "exit":
                    self._plan_exit_event(pos, features, "morning" if self.window.name == "morning" else "close")
        if self.phase == "entry" and self.window.name == "afternoon":
            pos_list = self._positions_with_meta(positions)
            for pos in pos_list:
                df, _ = self._fetch_daily(pos["code"], count=120)
                if df.empty:
                    continue
                features = compute_features(df)
                features["market"] = pos.get("market") or code_market.get(pos["code"], "")
                marks_fallback[pos["code"]] = features.get("close")
                self._plan_exit_event(pos, features, "close")
        marks = self._fetch_marks([p["code"] for p in self._positions_with_meta(positions)], marks_fallback)
        snapshot = self.ledger.generate_pnl_snapshot(positions, marks=marks)
        logger.info(
            "[PNL][SNAPSHOT] portfolio_return_pct=%.2f%% unrealized=%.2f realized=%.2f",
            snapshot["totals"]["portfolio_return_pct"],
            snapshot["totals"]["unrealized"],
            snapshot["totals"]["realized"],
        )
        snap_path = self.ledger.write_snapshot(snapshot, self.run_id)
        touched.append(snap_path)
        return touched
