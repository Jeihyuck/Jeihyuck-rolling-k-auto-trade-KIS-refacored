from __future__ import annotations

import logging
from dataclasses import dataclass
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
)
from trader.utils.env import env_bool
from trader.kis_wrapper import KisAPI
from trader.ledger.event_types import new_error, new_exit_intent, new_order_intent, new_fill, new_order_ack, new_unfilled
from trader.ledger.store import LedgerStore
from trader.strategies.pb1_pullback_close import choose_mode, compute_features, evaluate_setup
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
    ) -> None:
        self.kis = kis
        self.worktree_dir = worktree_dir
        self.window = window
        self.phase = resolve_phase(window, phase_override)
        self.dry_run = dry_run
        self.env = env
        self.run_id = run_id
        base_dir = LEDGER_BASE_DIR
        if not Path(base_dir).is_absolute():
            base_dir = worktree_dir / base_dir
        self.ledger = LedgerStore(Path(base_dir), env=env, run_id=run_id)
        self.worktree_dir = worktree_dir
        self._today = now_kst().date().isoformat()

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

    def _fetch_daily(self, code: str, count: int = 120) -> pd.DataFrame:
        if not self.kis:
            return pd.DataFrame()
        try:
            candles = self.kis.safe_get_daily_candles(code, count=count)
        except Exception:
            logger.exception("[PB1][DATA][FAIL] code=%s", code)
            return pd.DataFrame()
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(candles).copy()
        if df.empty:
            return df

        df.columns = [str(c).strip().lower() for c in df.columns]
        rename_map = {
            "stck_clpr": "close",
            "stck_hgpr": "high",
            "stck_lwpr": "low",
            "stck_trqu": "volume",
            "stck_bsop_date": "date",
            "거래량": "volume",
            "acml_vol": "volume",
            "acc_vol": "volume",
            "vol": "volume",
            "volume(주)": "volume",
            "volume ": "volume",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        if "volume" not in df.columns:
            logger.warning("[PB1][DAILY] missing volume code=%s cols=%s -> fill 0", code, list(df.columns))
            df["volume"] = 0.0

        for col in ["close", "high", "low", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return df

    def _build_universe(self) -> Dict[str, List[Dict]]:
        """
        Build selection universe for PB1 without triggering any legacy order flows.
        run_rebalance() in best_k_meta_strategy is selection-only and returns weights.
        """
        try:
            rebalance_payload = run_rebalance(str(now_kst().date()), return_by_market=True)
            return rebalance_payload.get("selected_by_market") or {}
        except Exception:
            logger.exception("[PB1][UNIVERSE][FAIL]")
            return {}

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
                    df = self._fetch_daily(code, count=120)
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
                    ok, reasons = evaluate_setup(features, market)
                    if not reasons:
                        reasons = ["missing_reason"]
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
            env="paper" if self.dry_run else self.kis.env if self.kis else "paper",
            run_id=self.run_id,
            side="BUY",
            qty=cf.planned_qty,
            price=cf.features.get("close"),
            client_order_key=cf.client_order_key,
            ok=not self.dry_run,
            reasons=["dry_run"] if self.dry_run else ["pb1_close_entry"],
            stage="PB1-CLOSE",
        )
        path = self.ledger.append_event("orders_intent", event)
        logger.info("[PB1][ENTRY-INTENT] code=%s mode=%s qty=%s key=%s path=%s", cf.code, cf.mode, cf.planned_qty, cf.client_order_key, path)

    def _place_entry(self, cf: CandidateFeature) -> List[Path]:
        paths: List[Path] = []
        self._append_intent(cf)
        paths.append(self.ledger._run_file("orders_intent"))
        if self.dry_run:
            return paths
        if not self.kis:
            err = new_unfilled(
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
                reasons=["kis_missing"],
                stage="PB1-CLOSE",
            )
            paths.append(self.ledger.append_event("errors", err))
            return paths
        resp = None
        try:
            resp = self.kis.buy_stock_market(cf.code, cf.planned_qty)
        except Exception:
            logger.exception("[PB1][ENTRY][FAIL] code=%s", cf.code)
        odno = ""
        if isinstance(resp, dict):
            odno = (resp.get("output") or {}).get("ODNO") or ""
        ack = new_order_ack(
            code=cf.code,
            market=cf.market,
            sid=1,
            mode=cf.mode,
            env=self.env,
            run_id=self.run_id,
            side="BUY",
            qty=cf.planned_qty,
            price=cf.features.get("close"),
            odno=odno,
            client_order_key=cf.client_order_key,
            ok=bool(resp and resp.get("rt_cd") == "0"),
            reasons=[] if resp and resp.get("rt_cd") == "0" else [resp.get("msg1", "order_failed")] if isinstance(resp, dict) else ["order_failed"],
            stage="PB1-CLOSE",
        )
        paths.append(self.ledger.append_event("orders_ack", ack))
        if resp and resp.get("rt_cd") == "0":
            fill_price = cf.features.get("close")
            fill = new_fill(
                code=cf.code,
                market=cf.market,
                sid=1,
                mode=cf.mode,
                env=self.env,
                run_id=self.run_id,
                side="BUY",
                qty=cf.planned_qty,
                price=fill_price,
                odno=odno,
                client_order_key=cf.client_order_key,
                stage="PB1-CLOSE",
            )
            paths.append(self.ledger.append_event("fills", fill))
            persist_run_files(self.worktree_dir, [paths[-1]], message=f"pb1 fill {self.run_id}")
        else:
            unfilled = new_unfilled(
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
                reasons=ack.reasons if ack.reasons else ["order_failed"],
                stage="PB1-CLOSE",
            )
            paths.append(self.ledger.append_event("errors", unfilled))
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
            ok=not self.dry_run,
            reasons=["dry_run"] if self.dry_run else reasons,
            stage=stage,
        )
        path = self.ledger.append_event("exits_intent", event)
        logger.info("[PB1][EXIT-INTENT] code=%s mode=%s stage=%s ret_pct=%.2f key=%s path=%s", code, mode, stage, ret_pct, client_key, path)
        # execute sell when allowed
        if self.dry_run:
            return
        if not self.kis:
            logger.warning("[PB1][EXIT][SKIP] kis missing code=%s", code)
            return
        resp = None
        try:
            resp = self.kis.sell_stock_market(code, qty)
        except Exception:
            logger.exception("[PB1][EXIT][FAIL] code=%s", code)
        odno = ""
        if isinstance(resp, dict):
            odno = (resp.get("output") or {}).get("ODNO") or ""
        ack = new_order_ack(
            code=code,
            market=market,
            sid=1,
            mode=mode,
            env=self.env,
            run_id=self.run_id,
            side="SELL",
            qty=qty,
            price=mark,
            odno=odno,
            client_order_key=client_key,
            ok=bool(resp and resp.get("rt_cd") == "0"),
            reasons=[] if resp and resp.get("rt_cd") == "0" else [resp.get("msg1", "order_failed")] if isinstance(resp, dict) else ["order_failed"],
            stage=stage,
        )
        self.ledger.append_event("orders_ack", ack)
        if resp and resp.get("rt_cd") == "0":
            fill = new_fill(
                code=code,
                market=market,
                sid=1,
                mode=mode,
                env=self.env,
                run_id=self.run_id,
                side="SELL",
                qty=qty,
                price=mark,
                odno=odno,
                client_order_key=client_key,
                stage=stage,
            )
            fill_path = self.ledger.append_event("fills", fill)
            persist_run_files(self.worktree_dir, [fill_path], message=f"pb1 fill {self.run_id}")
        else:
            err = new_unfilled(
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
                reasons=ack.reasons if ack.reasons else ["order_failed"],
                stage=stage,
            )
            self.ledger.append_event("errors", err)

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
        selected = self._build_universe()
        code_market = self._code_market_map(selected)
        marks_fallback: Dict[str, float] = {}
        if self.phase in {"prep", "entry"}:
            candidates = self._compute_candidates(selected)
            candidates = self._size_positions(candidates)
            if self.phase == "entry" and self.window.name == "afternoon":
                for cf in candidates:
                    if not cf.setup_ok:
                        continue
                    if self._should_block_order(cf.client_order_key):
                        continue
                    if not entry_allowed:
                        continue
                    paths = self._place_entry(cf)
                    touched.extend(paths)
        elif self.phase in {"exit", "verify"}:
            pos_list = self._positions_with_meta(positions)
            for pos in pos_list:
                df = self._fetch_daily(pos["code"], count=120)
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
                df = self._fetch_daily(pos["code"], count=120)
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
