# -*- coding: utf-8 -*-
"""거래 메인 루프.

기존 trader.py의 설정/유틸을 분리하고, 메인 진입점만 남겨 전략 추가가
쉬운 구조로 변경했다.
"""
from __future__ import annotations

import time
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, List, Tuple, TYPE_CHECKING


from .config import (
    DAILY_CAPITAL,
    FORCE_SELL_PASSES_CLOSE,
    FORCE_SELL_PASSES_CUTOFF,
    KST,
    RATE_SLEEP_SEC,
    SELL_ALL_BALANCES_AT_CUTOFF,
    SELL_FORCE_TIME,
    SLIPPAGE_ENTER_GUARD_PCT,
    USE_PULLBACK_ENTRY,
    _cfg,
    logger,
)
from .core import *  # noqa: F401,F403 - 전략 유틸 전체 노출로 확장성 확보

if TYPE_CHECKING:
    # core 쪽에 구현돼 있는 헬퍼들을 타입체커에게만 명시적으로 알려준다.
    from .core import (
        _this_iso_week_key,
        _get_effective_ord_cash,
        _to_float,
        _to_int,
        _weight_to_qty,
        _classify_champion_grade,
        _update_market_regime,
        _notional_to_qty,
        _fetch_balances,
        _init_position_state_from_balance,
        _sell_once,
        _adaptive_exit,
        _compute_daily_entry_context,
        _compute_intraday_entry_context,
        _safe_get_price,
        _round_to_tick,
        _init_position_state,
        _detect_pullback_reversal,
        _has_bullish_trend_structure,
    )


def main():
    kis = KisAPI()

    rebalance_date = get_rebalance_anchor_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date} (anchor={REBALANCE_ANCHOR}, ref={WEEKLY_ANCHOR_REF})")
    logger.info(
        f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
        f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}"
    )
    logger.info(f"[💰 DAILY_CAPITAL] {DAILY_CAPITAL:,}원")
    logger.info(f"[🛡️ SLIPPAGE_ENTER_GUARD_PCT] {SLIPPAGE_ENTER_GUARD_PCT:.2f}%")

    # 상태 복구
    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # === [NEW] 주간 리밸런싱 강제/중복 방지 ===
    targets: List[Dict[str, Any]] = []
    if REBALANCE_ANCHOR == "weekly":
        if should_weekly_rebalance_now():
            targets = fetch_rebalancing_targets(rebalance_date)
            # 중복 실행 방지를 위해 즉시 스탬프(필요 시 FORCE로 재실행 가능)
            stamp_weekly_done()
            logger.info(f"[REBALANCE] 이번 주 리밸런싱 실행 기록 저장({_this_iso_week_key()})")
        else:
            logger.info("[REBALANCE] 이번 주 이미 실행됨 → 신규 리밸런싱 생략 (보유 관리만)")
    else:
        # today/monthly 등 다른 앵커 모드는 기존 방식으로 바로 호출
        targets = fetch_rebalancing_targets(rebalance_date)

    # === [NEW] 예산 가드: 예수금이 0/부족이면 신규 매수만 스킵 ===
    effective_cash = _get_effective_ord_cash(kis)
    if effective_cash <= 0:
        can_buy = False
        logger.warning("[BUDGET] 유효 예산 0 → 신규 매수 스킵(보유 관리만 수행)")
    else:
        can_buy = True
    logger.info(
        f"[BUDGET] today effective cash = {effective_cash:,} KRW "
        f"(env DAILY_CAPITAL={DAILY_CAPITAL:,})"
    )

    # 리밸런싱 대상 후처리: qty 없고 weight만 있으면 DAILY_CAPITAL로 수량 계산
    processed_targets: Dict[str, Any] = {}
    for t in targets:
        code = t.get("stock_code") or t.get("code")
        if not code:
            continue
        name = t.get("name") or t.get("종목명")
        k_best = t.get("best_k") or t.get("K") or t.get("k")
        target_price = _to_float(t.get("목표가") or t.get("target_price"))
        qty = _to_int(t.get("매수수량") or t.get("qty"), 0)
        weight = t.get("weight")
        strategy = t.get("strategy") or "전월 rolling K 최적화"
        avg_return_pct = _to_float(t.get("avg_return_pct") or t.get("수익률(%)"), 0.0)
        win_rate_pct = _to_float(t.get("win_rate_pct") or t.get("승률(%)"), 0.0)
        mdd_pct = _to_float(t.get("mdd_pct") or t.get("MDD(%)"), 0.0)
        trades = _to_int(t.get("trades"), 0)
        sharpe_m = _to_float(t.get("sharpe_m"), 0.0)
        cumret_pct = _to_float(t.get("cumulative_return_pct") or t.get("수익률(%)"), 0.0)

        if qty <= 0 and weight is not None:
            ref_px = _to_float(t.get("close")) or _to_float(t.get("prev_close"))
            try:
                qty = _weight_to_qty(kis, code, float(weight), DAILY_CAPITAL, ref_price=ref_px)
            except Exception as e:
                logger.warning("[REBALANCE] weight→qty 변환 실패 %s: %s", code, e)
                qty = 0

        processed_targets[code] = {
            "code": code,
            "name": name,
            "best_k": k_best,
            "target_price": target_price,
            "qty": qty,
            "strategy": strategy,
            "avg_return_pct": avg_return_pct,
            "win_rate_pct": win_rate_pct,
            "mdd_pct": mdd_pct,
            "trades": trades,
            "sharpe_m": sharpe_m,
            "cumulative_return_pct": cumret_pct,
            "prev_open": t.get("prev_open"),
            "prev_high": t.get("prev_high"),
            "prev_low": t.get("prev_low"),
            "prev_close": t.get("prev_close"),
            "prev_volume": t.get("prev_volume"),
        }

    filtered_targets: Dict[str, Any] = {}
    for code, info in processed_targets.items():
        trades = _to_int(info.get("trades"), 0)
        win_rate = _to_float(info.get("win_rate_pct"), 0.0)
        mdd = abs(_to_float(info.get("mdd_pct"), 0.0) or 0.0)
        sharpe = _to_float(info.get("sharpe_m"), 0.0)

        if (
            trades < CHAMPION_MIN_TRADES
            or win_rate < CHAMPION_MIN_WINRATE
            or mdd > CHAMPION_MAX_MDD
            or sharpe < CHAMPION_MIN_SHARPE
        ):
            logger.info(
                f"[CHAMPION_FILTER_SKIP] {code}: trades={trades}, win={win_rate:.1f}%, mdd={mdd:.1f}%, sharpe={sharpe:.2f}"
            )
            continue

        filtered_targets[code] = info

    processed_targets = filtered_targets

    # 챔피언 등급화 (A/B/C) → 실제 매수 후보는 A급만 사용
    graded_targets: Dict[str, Any] = {}
    grade_counts = {"A": 0, "B": 0, "C": 0}
    for code, info in processed_targets.items():
        grade = _classify_champion_grade(info)
        info["champion_grade"] = grade
        graded_targets[code] = info
        grade_counts[grade] = grade_counts.get(grade, 0) + 1

    logger.info(
        "[CHAMPION-GRADE] A:%d / B:%d / C:%d (A/B급 실제 매수)",
        grade_counts.get("A", 0),
        grade_counts.get("B", 0),
        grade_counts.get("C", 0),
    )

    # 🔽 여기 필터를 A → A/B 로
    processed_targets = {
        k: v
        for k, v in graded_targets.items()
        if v.get("champion_grade") in ("A", "B")
    }
    # === [챔피언 & 레짐 상세 로그] ===
    try:
        if len(processed_targets) > 0:
            log_champion_and_regime("rebalance_api", processed_targets)
    except Exception as e:
        logger.warning(f"[CHAMPION_LOG] 챔피언/레짐 로그 생성 실패: {e}")

    # 현재 레짐 기반 자본 스케일링 & 챔피언 선택
    selected_targets: Dict[str, Any] = {}
    regime = _update_market_regime(kis)
    pct_change = regime.get("pct_change") or 0.0
    mode = regime.get("mode") or "neutral"
    stage = regime.get("bear_stage") or 0
    regime_key = regime.get("key")
    R20 = regime.get("R20")
    D1 = regime.get("D1")

    REGIME_CAP_TABLE = {
        ("bull", 0): 1.0,
        ("neutral", 0): 0.8,
        ("bear", 0): 0.7,
        ("bear", 1): 0.5,
        ("bear", 2): 0.3,
    }

    REGIME_WEIGHTS = {
        ("bull", 0): [0.22, 0.20, 0.18, 0.16, 0.14, 0.10],
        ("neutral", 0): [0.20, 0.18, 0.16, 0.14, 0.12, 0.10, 0.10],
        ("bear", 0): [0.18, 0.16, 0.14, 0.12, 0.10],
        ("bear", 1): [0.16, 0.14, 0.12],
        ("bear", 2): [0.14, 0.12, 0.10],
    }

    REGIME_MAX_ACTIVE = {
        ("bull", 0): 6,
        ("neutral", 0): 5,
        ("bear", 0): 4,
        ("bear", 1): 3,
        ("bear", 2): 2,
    }

    REG_PARTIAL_S1 = float(_cfg("REG_PARTIAL_S1") or "0.3")
    REG_PARTIAL_S2 = float(_cfg("REG_PARTIAL_S2") or "0.3")
    TRAIL_PCT_BULL = float(_cfg("TRAIL_PCT_BULL") or "0.025")
    TRAIL_PCT_BEAR = float(_cfg("TRAIL_PCT_BEAR") or "0.012")
    TP_PROFIT_PCT_BULL = float(_cfg("TP_PROFIT_PCT_BULL") or "3.5")

    cap_scale = REGIME_CAP_TABLE.get(regime.get("key"), 0.8)
    ord_cash = _get_effective_ord_cash(kis)
    capital_base = min(ord_cash, int(CAP_CAP * DAILY_CAPITAL))
    capital_active = int(min(capital_base * cap_scale, DAILY_CAPITAL))
    logger.info(
        f"[REGIME-CAP] mode={mode} stage={stage} R20={R20 if R20 is not None else 'N/A'} "
        f"D1={D1 if D1 is not None else 'N/A'} "
        f"ord_cash(effective)={ord_cash:,} base={capital_base:,} active={capital_active:,} "
        f"scale={cap_scale:.2f}"
    )

    # 레짐별 최대 보유 종목 수
    n_active = REGIME_MAX_ACTIVE.get(regime_key, REGIME_MAX_ACTIVE.get(("neutral", 0), 3))

    scored: List[Tuple[str, float, bool]] = []

    for code, info in processed_targets.items():
        score = _to_float(info.get("composite_score"), 0.0) or 0.0

        # 단기 모멘텀 강세 여부 (is_strong_momentum)로 버킷 구분
        try:
            strong = is_strong_momentum(kis, code)
        except Exception as e:
            logger.warning("[REBALANCE] 모멘텀 판별 실패 %s: %s", code, e)
            strong = False

        scored.append((code, score, strong))

    # 모멘텀 strong 버킷 우선, 그 다음 나머지 중에서 점수 순으로 채우기
    strong_bucket = [x for x in scored if x[2]]
    weak_bucket = [x for x in scored if not x[2]]

    strong_bucket.sort(key=lambda x: x[1], reverse=True)
    weak_bucket.sort(key=lambda x: x[1], reverse=True)

    picked: List[str] = []

    # 모멘텀 강 버킷을 우선 사용하되, 전체 보유 종목 수는 레짐별 n_active로 제한
    for code, score, _ in strong_bucket:
        if len(picked) >= n_active:
            break
        picked.append(code)

    for code, score, _ in weak_bucket:
        if len(picked) >= n_active:
            break
        picked.append(code)

    # === [NEW] 레짐별 챔피언 비중 & Target Notional 계산 ===
    regime_weights = REGIME_WEIGHTS.get(regime_key, REGIME_WEIGHTS.get(("neutral", 0), [1.0]))
    # 선택된 종목 수만큼 비중 슬라이스
    weights_for_picked: List[float] = list(regime_weights[: len(picked)])

    for idx, code in enumerate(picked):
        if code not in processed_targets:
            continue
        w = weights_for_picked[idx] if idx < len(weights_for_picked) else 0.0
        t = processed_targets[code]
        t["regime_weight"] = float(w)
        t["capital_active"] = int(capital_active)
        target_notional = int(round(capital_active * w))
        t["target_notional"] = target_notional

        ref_px = _to_float(t.get("close")) or _to_float(t.get("prev_close"))
        planned_qty = _notional_to_qty(kis, code, target_notional, ref_price=ref_px)
        t["qty"] = int(planned_qty)
        t["매수수량"] = int(planned_qty)
        processed_targets[code] = t

    for code in picked:
        if code in processed_targets:
            selected_targets[code] = processed_targets[code]

    logger.info(
        "[REGIME-CHAMPIONS] mode=%s stage=%s n_active=%s picked=%s capital_active=%s",
        mode,
        stage,
        n_active,
        picked,
        f"{capital_active:,}",
    )

    logger.info(
        "[REBALANCE] 레짐=%s pct=%.2f%%, 후보 %d개 중 상위 %d종목만 선택: %s",
        mode,
        pct_change,
        len(processed_targets),
        len(selected_targets),
        ",".join(selected_targets.keys()),
    )

    code_to_target: Dict[str, Any] = selected_targets

    # 눌림목 스캔용 코스닥 시총 상위 리스트 (챔피언과 별도로 관리)
    pullback_watch: Dict[str, Dict[str, Any]] = {}
    if USE_PULLBACK_ENTRY:
        try:
            pb_weight = max(0.0, min(PULLBACK_UNIT_WEIGHT, 1.0))
            base_notional = int(round(capital_active * pb_weight))
            pb_df = get_kosdaq_top_n(date_str=rebalance_date, n=PULLBACK_TOPN)
            for _, row in pb_df.iterrows():
                code = str(row.get("Code") or row.get("code") or "").zfill(6)
                if not code:
                    continue
                pullback_watch[code] = {
                    "code": code,
                    "name": row.get("Name") or row.get("name"),
                    "notional": base_notional,
                }
            logger.info(
                f"[PULLBACK-WATCH] 코스닥 시총 Top{PULLBACK_TOPN} {len(pullback_watch)}종목 스캔 준비"
            )
        except Exception as e:
            logger.warning(f"[PULLBACK-WATCH-FAIL] 시총 상위 로드 실패: {e}")

    loop_sleep_sec = 2.5  # 메인 루프 대기 시간(초)

    try:
        while True:
            # === 코스닥 레짐 업데이트 ===
            regime = _update_market_regime(kis)
            pct_txt = f"{regime.get('pct_change'):.2f}%" if regime.get("pct_change") is not None else "N/A"
            logger.info(f"[REGIME] mode={regime['mode']} stage={regime['bear_stage']} pct={pct_txt}")

            # 장 상태
            now_dt_kst = datetime.now(KST)
            is_open = kis.is_market_open()
            now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")

            if not is_open:
                if not ALLOW_WHEN_CLOSED:
                    logger.info("[CLOSED] 장 종료 → 10초 대기 후 재확인")
                    time.sleep(10)
                    continue
                else:
                    logger.warning("[CLOSED-DATA] 장 종료지만 환경설정 허용 → 시세 조회 후 진행")

            if kis.should_cooldown(now_dt_kst):
                logger.warning("[COOLDOWN] 2초간 대기 (API 제한 보호)")
                time.sleep(2)

            # 잔고 가져오기
            balances = _fetch_balances(kis)
            holding = {}
            for bal in balances:
                code = bal.get("code")
                qty = int(bal.get("qty", 0))
                if qty <= 0:
                    continue
                price = float(bal.get("avg_price", 0.0))
                holding[code] = {
                    "qty": qty,
                    "buy_price": price,
                    "bear_s1_done": False,
                    "bear_s2_done": False,
                }
                _init_position_state_from_balance(kis, holding, code, price, qty)

            # 잔고 기준으로 보유종목 매도 가능 수량 맵 생성
            ord_psbl_map = {bal.get("code"): int(bal.get("sell_psbl_qty", 0)) for bal in balances}

            logger.info(
                f"[STATUS] holdings={holding} traded_today={list(traded.keys())} ord_psbl={ord_psbl_map}"
            )

            # 커트오프 타임 도달 시 강제매도 루틴
            if now_dt_kst.time() >= SELL_FORCE_TIME and SELL_ALL_BALANCES_AT_CUTOFF:
                logger.info("[⏰ 커트오프 도달: 전량매도 루틴 실행]")
                pass_count = FORCE_SELL_PASSES_CUTOFF
                if now_dt_kst.time() >= dtime(hour=15, minute=0):
                    pass_count = FORCE_SELL_PASSES_CLOSE
                for code, qty in ord_psbl_map.items():
                    if qty <= 0:
                        continue
                    exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
                    log_trade(
                        {
                            "datetime": now_str,
                            "code": code,
                            "name": None,
                            "qty": int(qty),
                            "K": None,
                            "target_price": None,
                            "strategy": "강제매도",
                            "side": "SELL",
                            "price": exec_px,
                            "amount": int((exec_px or 0)) * int(qty),
                            "result": result,
                            "reason": "커트오프 강제매도",
                        }
                    )
                    time.sleep(RATE_SLEEP_SEC)
                for _ in range(pass_count - 1):
                    logger.info(
                        f"[커트오프 추가패스] {pass_count}회 중 남은 패스 실행 (잔고변동 감지용)"
                    )
                    time.sleep(loop_sleep_sec)
                    continue
                logger.info("[⏰ 커트오프 종료] 루프 종료")
                break

            # === (1) 잔여 물량 대상 스탑/리밸런스 관리 ===
            for code in list(holding.keys()):
                # 신규 진입 금지 모드
                if code not in code_to_target:
                    continue

                # --- 1a) 강제 레짐별 축소 로직 ---
                sellable_qty = ord_psbl_map.get(code, 0)
                if sellable_qty <= 0:
                    continue

                regime_key = regime.get("key")
                mode = regime.get("mode")
                if regime_key and regime_key[0] == "bear":
                    if regime["bear_stage"] >= 1 and not holding[code].get("bear_s1_done"):
                        cut_qty = max(1, int(holding[code]["qty"] * REG_PARTIAL_S1))
                        logger.info(
                            f"[REGIME-REDUCE-S1] {code} 약세1단계 {REG_PARTIAL_S1 * 100:.0f}% 축소 → {cut_qty}"
                        )
                        exec_px, result = _sell_once(kis, code, cut_qty, prefer_market=True)
                        holding[code]["qty"] -= int(cut_qty)
                        holding[code]["bear_s1_done"] = True
                        log_trade(
                            {
                                "datetime": now_str,
                                "code": code,
                                "name": None,
                                "qty": int(cut_qty),
                                "K": holding[code].get("k_value"),
                                "target_price": holding[code].get("target_price_src"),
                                "strategy": "레짐축소",  # 신규 전략 구분을 위해 strategy 필드 활용
                                "side": "SELL",
                                "price": exec_px,
                                "amount": int((exec_px or 0)) * int(cut_qty),
                                "result": result,
                                "reason": "시장약세 1단계 축소",
                            }
                        )
                        save_state(holding, traded)
                        time.sleep(RATE_SLEEP_SEC)

                    if regime["bear_stage"] >= 2 and not holding[code].get("bear_s2_done"):
                        cut_qty = max(1, int(holding[code]["qty"] * REG_PARTIAL_S2))
                        logger.info(
                            f"[REGIME-REDUCE-S2] {code} 약세2단계 {REG_PARTIAL_S2 * 100:.0f}% 축소 → {cut_qty}"
                        )
                        exec_px, result = _sell_once(kis, code, cut_qty, prefer_market=True)
                        holding[code]["qty"] -= int(cut_qty)
                        holding[code]["bear_s2_done"] = True
                        log_trade(
                            {
                                "datetime": now_str,
                                "code": code,
                                "name": None,
                                "qty": int(cut_qty),
                                "K": holding[code].get("k_value"),
                                "target_price": holding[code].get("target_price_src"),
                                "strategy": "레짐축소",
                                "side": "SELL",
                                "price": exec_px,
                                "amount": int((exec_px or 0)) * int(cut_qty),
                                "result": result,
                                "reason": "시장약세 2단계 축소",
                            }
                        )
                        save_state(holding, traded)
                        time.sleep(RATE_SLEEP_SEC)

                # --- 1b) TP/SL/트레일링, VWAP 가드 ---
                _adaptive_exit(
                    kis,
                    holding,
                    traded,
                    code,
                    ord_psbl_map,
                    regime,
                    now_dt_kst,
                    now_str,
                    R20,
                    can_buy,
                    PARTIAL1,
                    PARTIAL2,
                    TRAIL_PCT_BULL,
                    TRAIL_PCT_BEAR,
                    TP_PROFIT_PCT_BULL,
                    DEFAULT_PROFIT_PCT,
                    DEFAULT_LOSS_PCT,
                    ATR_STOP,
                    FAST_STOP,
                )

            # === (2) 신규 진입 로직 (챔피언) ===
            for code, info in code_to_target.items():
                if not can_buy:
                    continue

                if code in traded:
                    continue

                if code in holding:
                    continue

                target_qty = int(info.get("qty", 0))
                if target_qty <= 0:
                    logger.info(f"[REBALANCE] {code}: target_qty=0 → 스킵")
                    continue

                target_price = info.get("target_price")
                k_value = info.get("best_k")
                strategy = info.get("strategy")
                weight = _to_float(info.get("weight") or 0.0)

                planned_notional = int(_to_float(info.get("target_notional") or 0.0) or 0)
                logger.info(
                    f"[TARGET] {code} qty={target_qty} tgt_px={target_price} notional={planned_notional} K={k_value}"
                )

                # [중복 진입 방지] 이미 주문된 종목인지 확인
                if code in traded:
                    logger.info(f"[SKIP] {code}: 이미 금일 거래됨")
                    continue

                # === GOOD/BAD 타점 평가 ===
                daily_ctx = _compute_daily_entry_context(kis, code, PULLBACK_LOOKBACK)
                intra_ctx = _compute_intraday_entry_context(kis, code, fast=MOM_FAST, slow=MOM_SLOW)

                if is_bad_entry(daily_ctx, intra_ctx):
                    logger.info(f"[ENTRY-SKIP] {code}: BAD 타점 감지 → 이번 루프 매수 스킵")
                    continue

                if not is_good_entry(daily_ctx, intra_ctx):
                    logger.info(
                        f"[ENTRY-SKIP] {code}: GOOD 타점 미충족 → 다음 루프에서 재확인"
                    )
                    continue

                logger.info(f"[ENTRY-GOOD] {code}: GOOD 타점 확인 → 매수 시도")

                # === VWAP 가드(슬리피지 방어) ===
                try:
                    guard_passed = vwap_guard(kis, code, SLIPPAGE_ENTER_GUARD_PCT)
                except Exception as e:
                    logger.warning(f"[VWAP_GUARD_FAIL] {code}: VWAP 가드 오류 → 진입 보류 ({e})")
                    continue

                if not guard_passed:
                    logger.info(f"[VWAP_GUARD] {code}: 슬리피지 위험 → 매수 스킵")
                    continue

                current_price = _safe_get_price(kis, code)
                if not current_price or current_price <= 0:
                    logger.warning(f"[PRICE_FAIL] {code}: 현재가 조회 실패 → 스킵")
                    continue

                qty = target_qty
                trade_ctx = {
                    "datetime": now_str,
                    "code": code,
                    "name": info.get("name"),
                    "qty": int(qty),
                    "K": k_value,
                    "target_price": target_price,
                    "strategy": strategy,
                    "side": "BUY",
                }

                limit_px, mo_px = compute_entry_target(kis, info)
                if limit_px is None and mo_px is None:
                    logger.warning(f"[TARGET-PRICE] {code}: limit/mo 가격 산출 실패 → 스킵")
                    continue

                if limit_px and abs(limit_px - current_price) / current_price * 100 > SLIPPAGE_LIMIT_PCT:
                    logger.info(
                        f"[SLIPPAGE_LIMIT] {code}: 호가乖離 {abs(limit_px - current_price) / current_price * 100:.2f}% → 스킵"
                    )
                    continue

                logger.info(
                    f"[BUY-TRY] {code}: qty={qty} limit={limit_px} mo={mo_px} target={target_price} k={k_value}"
                )

                result = place_buy_with_fallback(kis, code, qty, limit_px or _round_to_tick(current_price))
                traded[code] = {
                    "buy_time": now_str,
                    "qty": int(qty),
                    "price": float(current_price),
                }

                _init_position_state(
                    kis,
                    holding,
                    code,
                    float(current_price),
                    int(qty),
                    k_value,
                    target_price,
                )

                log_trade(
                    {
                        **trade_ctx,
                        "price": float(current_price),
                        "amount": int(float(current_price) * int(qty)),
                        "result": result,
                    }
                )
                save_state(holding, traded)
                time.sleep(RATE_SLEEP_SEC)

            # ====== 눌림목 전용 매수 (챔피언과 독립적으로 Top-N 시총 리스트 스캔) ======
            if USE_PULLBACK_ENTRY and is_open:
                if pullback_watch:
                    logger.info(f"[PULLBACK-SCAN] {len(pullback_watch)}종목 검사")

                for code, info in list(pullback_watch.items()):
                    if code in traded or code in holding:
                        continue  # 챔피언 루프와 별도로만 처리

                    base_notional = int(info.get("notional") or 0)
                    if base_notional <= 0:
                        logger.info(f"[PULLBACK-SKIP] {code}: 예산 0")
                        continue

                    try:
                        pullback_ok, trigger_price = _detect_pullback_reversal(
                            kis,
                            code,
                            lookback=PULLBACK_LOOKBACK,
                            pullback_days=PULLBACK_DAYS,
                            reversal_buffer_pct=PULLBACK_REVERSAL_BUFFER_PCT,
                        )
                    except Exception as e:
                        logger.warning(f"[PULLBACK-FAIL] {code}: 스캔 실패 {e}")
                        continue

                    if not pullback_ok:
                        continue

                    qty = _notional_to_qty(kis, code, base_notional)
                    if qty <= 0:
                        logger.info(f"[PULLBACK-SKIP] {code}: 수량 산출 0")
                        continue

                    current_price = _safe_get_price(kis, code)
                    if not current_price:
                        logger.warning(f"[PULLBACK-PRICE] {code}: 현재가 조회 실패")
                        continue

                    if trigger_price and current_price < trigger_price * 0.98:
                        logger.info(
                            f"[PULLBACK-DELAY] {code}: 가격이 트리거 대비 2% 이상 하락 → 대기 (cur={current_price}, trigger={trigger_price})"
                        )
                        continue

                    result = place_buy_with_fallback(
                        kis,
                        code,
                        int(qty),
                        _round_to_tick(trigger_price or current_price),
                    )

                    try:
                        _init_position_state(
                            kis,
                            holding,
                            code,
                            float(current_price),
                            int(qty),
                            None,
                            trigger_price,
                        )
                    except Exception as e:
                        logger.warning(f"[PULLBACK-INIT-FAIL] {code}: {e}")

                    traded[code] = {
                        "buy_time": now_str,
                        "qty": int(qty),
                        "price": float(current_price),
                    }
                    logger.info(
                        f"[✅ 눌림목 매수] {code}, qty={qty}, price={current_price}, trigger={trigger_price}, result={result}"
                    )

                    log_trade(
                        {
                            "datetime": now_str,
                            "code": code,
                            "name": info.get("name"),
                            "qty": int(qty),
                            "K": None,
                            "target_price": trigger_price,
                            "strategy": f"코스닥 Top{PULLBACK_TOPN} 눌림목",
                            "side": "BUY",
                            "price": float(current_price),
                            "amount": int(float(current_price) * int(qty)),
                            "result": result,
                        }
                    )
                    save_state(holding, traded)
                    time.sleep(RATE_SLEEP_SEC)

            # ====== (A) 비타겟 보유분도 장중 능동관리 ======
            if is_open:
                for code in list(holding.keys()):
                    if code in code_to_target:
                        continue  # 위 루프에서 이미 처리

                    # 약세 단계 축소(비타겟)
                    if regime["mode"] == "bear":
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here > 0:
                            if (
                                regime["bear_stage"] >= 1
                                and not holding[code].get("bear_s1_done")
                            ):
                                cut_qty = max(
                                    1, int(holding[code]["qty"] * REG_PARTIAL_S1)
                                )
                                logger.info(
                                    f"[REGIME-REDUCE-S1/비타겟] {code} 약세1단계 {REG_PARTIAL_S1 * 100:.0f}% 축소 → {cut_qty}"
                                )
                                exec_px, result = _sell_once(
                                    kis, code, cut_qty, prefer_market=True
                                )
                                holding[code]["qty"] -= int(cut_qty)
                                holding[code]["bear_s1_done"] = True
                                log_trade(
                                    {
                                        "datetime": now_str,
                                        "code": code,
                                        "name": None,
                                        "qty": int(cut_qty),
                                        "K": holding[code].get("k_value"),
                                        "target_price": holding[code].get(
                                            "target_price_src"
                                        ),
                                        "strategy": "기존보유 능동관리",
                                        "side": "SELL",
                                        "price": exec_px,
                                        "amount": int((exec_px or 0))
                                        * int(cut_qty),
                                        "result": result,
                                        "reason": "시장약세 1단계 축소(비타겟)",
                                    }
                                )
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                            if (
                                regime["bear_stage"] >= 2
                                and not holding[code].get("bear_s2_done")
                            ):
                                cut_qty = max(
                                    1, int(holding[code]["qty"] * REG_PARTIAL_S2)
                                )
                                logger.info(
                                    f"[REGIME-REDUCE-S2/비타겟] {code} 약세2단계 {REG_PARTIAL_S2 * 100:.0f}% 축소 → {cut_qty}"
                                )
                                exec_px, result = _sell_once(
                                    kis, code, cut_qty, prefer_market=True
                                )
                                holding[code]["qty"] -= int(cut_qty)
                                holding[code]["bear_s2_done"] = True
                                log_trade(
                                    {
                                        "datetime": now_str,
                                        "code": code,
                                        "name": None,
                                        "qty": int(cut_qty),
                                        "K": holding[code].get("k_value"),
                                        "target_price": holding[code].get(
                                            "target_price_src"
                                        ),
                                        "strategy": "기존보유 능동관리",
                                        "side": "SELL",
                                        "price": exec_px,
                                        "amount": int((exec_px or 0))
                                        * int(cut_qty),
                                        "result": result,
                                        "reason": "시장약세 2단계 축소(타겟)",
                                    }
                                )
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                    try:
                        momentum_intact, trend_ctx = _has_bullish_trend_structure(kis, code)
                    except NetTemporaryError:
                        logger.warning(
                            f"[20D_TREND_TEMP_SKIP] {code}: 네트워크 일시 실패 → 이번 루프 스킵"
                        )
                        continue
                    except DataEmptyError:
                        logger.warning(
                            f"[DATA_EMPTY] {code}: 0캔들 → 다음 루프에서 재확인"
                        )
                        continue
                    except DataShortError:
                        logger.error(
                            f"[DATA_SHORT] {code}: 21개 미만 → 이번 루프 판단 스킵"
                        )
                        continue

                    if momentum_intact:
                        logger.info(
                            (
                                f"[모멘텀 보유] {code}: 5/10/20 정배열 & 20일선 상승 & 종가>20일선 유지 "
                                f"(close={trend_ctx.get('last_close'):.2f}, ma5={trend_ctx.get('ma5'):.2f}, "
                                f"ma10={trend_ctx.get('ma10'):.2f}, ma20={trend_ctx.get('ma20'):.2f}→{trend_ctx.get('ma20_prev'):.2f})"
                            )
                        )
                        continue

            # --- 장중 커트오프(KST): 14:40 도달 시 "전량매도 없이" 리포트 생성 후 정상 종료 ---
            if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
                logger.info(
                    f"[⏰ 커트오프] {SELL_FORCE_TIME.strftime('%H:%M')} 도달: 전량 매도 없이 리포트 생성 후 종료"
                )

                save_state(holding, traded)

                try:
                    _report = ceo_report(datetime.now(KST), period="daily")
                    logger.info(
                        f"[📄 CEO Report 생성 완료] title={_report.get('title')}"
                    )
                except Exception as e:
                    logger.error(f"[CEO Report 생성 실패] {e}")

                logger.info("[✅ 커트오프 완료: 루프 정상 종료]")
                break

            save_state(holding, traded)
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")
    except Exception as e:
        logger.exception(f"[FATAL] 메인 루프 예외 발생: {e}")


if __name__ == "__main__":
    main()
