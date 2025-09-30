import logging
import requests
from .kis_wrapper import KisAPI
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import time
import os
import random
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

# ====== 시간대(KST) 및 설정 ======
KST = ZoneInfo("Asia/Seoul")

# 장중 강제 전량매도 커트오프 (KST 기준)
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "11:15").strip()

# 커트오프/장마감 시 보유 전 종목(계좌 잔고 전체) 포함 여부 (기본 True)
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "true").lower() == "true"

# API 호출 간 최소 휴지시간(초)
RATE_SLEEP_SEC = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))

# 커트오프/장마감 매도 시 패스(회차) 수
FORCE_SELL_PASSES_CUTOFF = int(os.getenv("FORCE_SELL_PASSES_CUTOFF", "3"))
FORCE_SELL_PASSES_CLOSE = int(os.getenv("FORCE_SELL_PASSES_CLOSE", "5"))

# Rebalance fetch / fallback settings
REBALANCE_TIMEOUT = int(os.getenv("REBALANCE_TIMEOUT", "60"))
REBALANCE_FETCH_RETRIES = int(os.getenv("REBALANCE_FETCH_RETRIES", "3"))
REBALANCE_OUT_DIR = os.getenv("REBALANCE_OUT_DIR", "rebalance_results")
REBALANCE_USE_CACHE_FALLBACK = os.getenv("REBALANCE_USE_CACHE_FALLBACK", "1") == "1"
ALLOW_FORCE_SELL_ON_REBALANCE_FAIL = os.getenv("ALLOW_FORCE_SELL_ON_REBALANCE_FAIL", "0") == "1"

# 기본 안전 제어
ORDER_THROTTLE_SEC = float(os.getenv("ORDER_THROTTLE_SEC", "0.3"))
MIN_QTY_PER_TICKET = int(os.getenv("MIN_QTY_PER_TICKET", "1"))

# 내부 상수
LOG_DAY_FMT = "%Y-%m-%d"


def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식 오류 → 기본값 15:15 적용: {hhmm}")
        return dtime(hour=15, minute=15)


SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)


def get_month_first_date():
    today = datetime.now(KST)
    month_first = today.replace(day=1)
    return month_first.strftime("%Y-%m-%d")


def log_trade(trade: dict):
    today = datetime.now(KST).strftime(LOG_DAY_FMT)
    logfile = LOG_DIR / f"trades_{today}.json"
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(json.dumps(trade, ensure_ascii=False) + "\n")


def save_state(holding, traded):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"holding": holding, "traded": traded}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.exception(f"[STATE_SAVE_FAIL] {e}")


def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
                return state.get("holding", {}), state.get("traded", {})
        except Exception as e:
            logger.exception(f"[STATE_LOAD_FAIL] {e}")
    return {}, {}


# ----- 공용 재시도 래퍼 -----
def _with_retry(func, *args, max_retries=5, base_delay=0.6, **kwargs):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            sleep_sec = base_delay * (1.6 ** (attempt - 1)) + random.uniform(0, 0.25)
            logger.error(f"[재시도 {attempt}/{max_retries}] {func.__name__} 실패: {e} → {sleep_sec:.2f}s 대기 후 재시도")
            time.sleep(sleep_sec)
    raise last_err


def _safe_get_price(kis: KisAPI, code: str):
    """현재가 조회 실패해도 매도는 진행할 수 있도록 None을 허용."""
    try:
        price = _with_retry(kis.get_current_price, code)
        # 가격가드: 0.0 / 음수 / 비정상은 None 처리
        if price is None or (isinstance(price, (int, float)) and price <= 0):
            logger.warning(f"[PRICE_GUARD] {code} 현재가 무효값({price})")
            return None
        # 숫자형 문자열도 허용
        try:
            return float(price)
        except Exception:
            return price
    except Exception as e:
        logger.warning(f"[현재가 조회 실패: 계속 진행] {code} err={e}")
        return None


def _to_int(val, default=0):
    try:
        return int(float(val))
    except Exception:
        return default


def _to_float(val, default=None):
    try:
        return float(val)
    except Exception:
        return default


# Balance normalization helper
def _normalize_balances(raw: Any) -> List[Dict[str, Any]]:
    """KIS 응답 포맷(여러가지)을 list[dict] 로 정규화 반환."""
    try:
        if raw is None:
            return []
        # 문자열이면 JSON 파싱 시도
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                raw = parsed
            except Exception:
                # 알 수 없는 문자열 포맷
                logger.warning("[BALANCE_NORMALIZE_WARN] response is raw string and cannot parse JSON")
                return []

        # dict 형태인 경우
        if isinstance(raw, dict):
            # KIS wrapper에서 반환한 dict 형식에 output1 키가 있으면 사용
            if 'output1' in raw and isinstance(raw['output1'], list):
                return raw['output1']
            # 때때로 실제 리스트 자체가 'output' 또는 'output1' 내부에 있는 케이스
            if 'output' in raw and isinstance(raw['output'], list):
                return raw['output']
            # single balance dict
            if all(k in raw for k in ('pdno', 'hldg_qty')):
                return [raw]
            # dict 내에 'output1'이 아닌 다른 래핑이 있을 수 있어 안전하게 빈리스트 반환
            logger.warning("[BALANCE_NORMALIZE_WARN] unexpected balance format: dict without output1")
            return []

        # 이미 리스트면 그대로 반환(리스트 내부 원소 체크는 호출자 책임)
        if isinstance(raw, list):
            return raw

        # 기타 타입
        logger.warning(f"[BALANCE_NORMALIZE_WARN] unexpected balance format: {type(raw)}")
        return []
    except Exception as e:
        logger.exception(f"[BALANCE_NORMALIZE_FAIL] {e}")
        return []


# 통합 잔고 조회
def _fetch_balances(kis: KisAPI) -> List[Dict[str, Any]]:
    """잔고 조회 및 정규화. KisAPI의 get_balance_all 또는 get_balance 사용."""
    try:
        if hasattr(kis, "get_balance_all"):
            raw = _with_retry(kis.get_balance_all)
        else:
            raw = _with_retry(kis.get_balance)
        return _normalize_balances(raw)
    except Exception as e:
        logger.error(f"[잔고조회 오류]{e}")
        return []


def _sell_once(kis: KisAPI, code: str, qty: int, prefer_market=True):
    cur_price = _safe_get_price(kis, code)

    try:
        if prefer_market and hasattr(kis, "sell_stock_market"):
            result = _with_retry(kis.sell_stock_market, code, qty)
        else:
            result = _with_retry(kis.sell_stock, code, qty)
    except Exception as e:
        logger.warning(f"[매도 재시도: 토큰 갱신 후 1회] {code} qty={qty} err={e}")
        try:
            if hasattr(kis, "refresh_token"):
                kis.refresh_token()
        except Exception:
            pass
        if prefer_market and hasattr(kis, "sell_stock_market"):
            result = _with_retry(kis.sell_stock_market, code, qty)
        else:
            result = _with_retry(kis.sell_stock, code, qty)

    logger.info(f"[매도호출] {code}, qty={qty}, price(log)={cur_price}, result={result}")
    return cur_price, result


def _force_sell_pass(kis: KisAPI, targets_codes: set, reason: str, prefer_market=True):
    if not targets_codes:
        return set()

    targets_codes = {c for c in targets_codes if c}

    balances = _fetch_balances(kis)
    qty_map = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in balances}
    sellable_map = {b.get("pdno"): _to_int(b.get("ord_psbl_qty", 0)) for b in balances}

    remaining = set()

    for code in list(targets_codes):
        qty = qty_map.get(code, 0)
        sellable = sellable_map.get(code, 0)

        if qty <= 0:
            logger.info(f"[스킵] {code}: 실제 잔고 수량 0")
            continue

        if sellable <= 0:
            logger.info(f"[스킵] {code}: 매도가능수량=0 (대기/체결중/락) → 이번 패스 보류")
            remaining.add(code)
            continue

        try:
            sell_qty = min(qty, sellable) if sellable > 0 else qty
            cur_price, result = _sell_once(kis, code, sell_qty, prefer_market=prefer_market)
            log_trade({
                "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "name": None,
                "qty": sell_qty,
                "K": None,
                "target_price": None,
                "strategy": "강제전량매도",
                "side": "SELL",
                "price": cur_price if cur_price is not None else 0,
                "amount": (_to_int(cur_price, 0) * int(sell_qty)) if cur_price is not None else 0,
                "result": result,
                "reason": reason
            })
        finally:
            time.sleep(RATE_SLEEP_SEC)

    balances_after = _fetch_balances(kis)
    after_qty_map = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in balances_after}

    for code in targets_codes:
        if after_qty_map.get(code, 0) > 0:
            remaining.add(code)

    return remaining


def _force_sell_all(kis: KisAPI, holding: dict, reason: str, passes: int, include_all_balances: bool, prefer_market=True):
    target_codes = set([c for c in holding.keys() if c])

    if include_all_balances:
        try:
            balances = _fetch_balances(kis)
            for b in balances:
                code = b.get("pdno")
                if code and _to_int(b.get("hldg_qty", 0)) > 0:
                    target_codes.add(code)
        except Exception as e:
            logger.error(f"[잔고조회 오류: 전체포함 불가] {e}")

    if not target_codes:
        logger.info("[강제전량매도] 대상 종목 없음")
        return

    logger.info(f"[⚠️ 강제전량매도] 사유: {reason} / 대상 종목수: {len(target_codes)} / 전체잔고포함={include_all_balances}")

    remaining = target_codes
    for p in range(1, max(1, passes) + 1):
        logger.info(f"[강제전량매도 PASS {p}/{passes}] 대상 {len(remaining)}종목 시도")
        remaining = _force_sell_pass(kis, remaining, reason=reason, prefer_market=prefer_market)
        if not remaining:
            logger.info("[강제전량매도] 모든 종목 매도 완료")
            break

    if remaining:
        logger.error(f"[강제전량매도] 미매도 잔여 {len(remaining)}종목: {sorted(list(remaining))}")

    for code in list(holding.keys()):
        holding.pop(code, None)
    save_state(holding, {})


# ===== New: rebalance fetch with retries and cache fallback =====

def fetch_rebalancing_targets(date: str) -> List[dict]:
    """
    안전한 리밸런스 시그널 fetch.
    - retries + backoff
    - timeout configurable
    - cache fallback to REBALANCE_OUT_DIR/rebalance_{date}.json if enabled
    """
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}?force_order=true"
    session = requests.Session()
    attempt = 0
    while attempt < REBALANCE_FETCH_RETRIES:
        attempt += 1
        try:
            resp = session.post(REBALANCE_API_URL, timeout=REBALANCE_TIMEOUT)
            logger.info(f"[🛰️ 리밸런싱 API 응답(시도{attempt})]: status={resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                # 우선 selected 또는 signals 또는 selected_stocks 키를 확인
                targets = data.get('selected') or data.get('selected_stocks') or data.get('signals')
                if targets is None:
                    logger.warning("[REBALANCE_PARSE_WARN] 응답에 selected/signals 키 없음, 전체 JSON 반환 시도")
                    # 가능하면 전체 JSON이 list일 때만 사용
                    if isinstance(data, list):
                        targets = data
                    else:
                        targets = []
                logger.info(f"[🎯 리밸런싱 시그널]: {targets}")
                return targets or []
            else:
                logger.error(f"[REBALANCE_FETCH_FAIL] status_code={resp.status_code} text={resp.text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"[REBALANCE_FETCH_FAIL] 리밸런싱 API 호출 실패(시도{attempt}): {e}")
        # exponential backoff before next attempt
        backoff = 0.5 * (2 ** (attempt - 1))
        time.sleep(backoff)

    # all retries failed -> fallback to cache if enabled
    cache_fp = Path(REBALANCE_OUT_DIR) / f"rebalance_{date}.json"
    if REBALANCE_USE_CACHE_FALLBACK and cache_fp.exists():
        try:
            with open(cache_fp, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            logger.warning(f"[REBALANCE_CACHE_FALLBACK] 캐시 사용: {cache_fp}")
            # cached expected to be a list of signals or results_list (selected entries)
            return cached
        except Exception as e:
            logger.exception(f"[REBALANCE_CACHE_FAIL] 캐시 파싱 실패: {e}")

    # 최종 실패 처리: 기본 동작은 강제전량매도하지 않고 빈 리스트 반환
    logger.error("[REBALANCE_FETCH_FAIL] 모든 시도 실패 및 캐시 없음")
    return []


# ===== New: compute daily target (trader-side) =====

def compute_daily_target(kis: KisAPI, signal: dict) -> Optional[int]:
    """트레이더가 당일(최신) 목표가를 계산한다.

    우선 적절한 OHLC를 KIS에서 직접 가져오려 시도하고, 실패하면
    signal의 base_close/base_high/base_low에 의존한다.
    """
    code = signal.get('stock_code') or signal.get('code')
    best_k = signal.get('best_k') or signal.get('K') or signal.get('k')
    if code is None or best_k is None:
        logger.warning(f"[TARGET_CALC_SKIP] code 또는 best_k 누락: {code}, {best_k}")
        return None

    # 우선 KIS에서 전일 OHLC 가져오기 시도 (KisAPI가 제공하면 사용)
    ohlc = None
    try:
        if hasattr(kis, 'get_prev_day_ohlc'):
            ohlc = _with_retry(kis.get_prev_day_ohlc, code)
            # Expecting dict with keys close/high/low OR numeric values
    except Exception:
        logger.debug(f"[TARGET_CALC] kis.get_prev_day_ohlc 실패, 시그널의 base_* 사용 예정: {code}")

    if not ohlc:
        # fallback to signal's provided base values
        ohlc = {
            'close': signal.get('base_close') or signal.get('last_close') or signal.get('종가'),
            'high': signal.get('base_high') or signal.get('baseHigh') or signal.get('고가'),
            'low': signal.get('base_low') or signal.get('baseLow') or signal.get('저가'),
        }

    try:
        close = float(ohlc.get('close'))
        high = float(ohlc.get('high'))
        low = float(ohlc.get('low'))
    except Exception:
        logger.warning(f"[TARGET_CALC_FAIL] OHLC 값 부족/변환불가 for {code}: {ohlc}")
        return None

    try:
        kf = float(best_k)
    except Exception:
        logger.warning(f"[TARGET_CALC_FAIL] best_k 변환불가 for {code}: {best_k}")
        return None

    target = close + kf * (high - low)
    target_int = int(round(target))
    logger.info(f"[TARGET_CALC] {code} close={close}, high={high}, low={low}, k={kf} -> target={target_int}")
    return target_int


def fetch_and_prepare_targets(kis: KisAPI, rebalance_date: str) -> Dict[str, dict]:
    """리밸런서에서 시그널을 받아 트레이더용 target_price를 계산 후 반환하는 헬퍼.

    반환값은 코드->signal dict 매핑이며 각 dict에는 'computed_target_price' 키가 추가된다.
    """
    raw_targets = fetch_rebalancing_targets(rebalance_date)
    code_to_target: Dict[str, dict] = {}

    if not raw_targets:
        logger.warning("[PREPARE_TARGETS] 리밸런싱 시그널 없음")
        return code_to_target

    for sig in raw_targets:
        code = sig.get('stock_code') or sig.get('code')
        if not code:
            logger.warning(f"[PREPARE_TARGETS] 시그널 코드 누락: {sig}")
            continue
        try:
            sig = dict(sig)  # copy
            # compute daily target (트레이더 주도)
            computed = compute_daily_target(kis, sig)
            if computed is not None:
                sig['computed_target_price'] = computed
            else:
                sig['computed_target_price'] = None
            code_to_target[code] = sig
            # throttle between per-symbol KIS calls if compute_daily_target used KIS API
            time.sleep(RATE_SLEEP_SEC)
        except Exception as e:
            logger.exception(f"[PREPARE_TARGETS_FAIL] {code}: {e}")
            continue

    logger.info(f"[PREPARE_TARGETS] Prepared {len(code_to_target)} targets")
    return code_to_target


# ===== main loop =====

def main(force_sell_mode: bool = False):
    kis = KisAPI()
    rebalance_date = get_month_first_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date}")
    logger.info(f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
                f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}")

    # ======== 상태 복구 ========
    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # ======== 리밸런싱 대상 종목 추출 및 준비(트레이더가 목표가 계산) ========
    code_to_target = fetch_and_prepare_targets(kis, rebalance_date)

    if not code_to_target and force_sell_mode:
        logger.info("[FORCE_SELL_MODE] 즉시 강제전량매도 실행")
        _force_sell_all(
            kis=kis,
            holding=holding,
            reason="force-sell-mode",
            passes=FORCE_SELL_PASSES_CUTOFF,
            include_all_balances=True,
            prefer_market=True
        )
        return

    # 기본 매도조건(익절/손절)
    sell_conditions = {
        'profit_pct': 3.0,   # +3% 이상 익절
        'loss_pct':  -2.0    # -2% 이하 손절
    }

    loop_sleep_sec = 3

    try:
        while True:
            is_open = kis.is_market_open()
            now_dt_kst = datetime.now(KST)
            now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[⏰ 장상태] {'OPEN' if is_open else 'CLOSED'} / KST={now_str}")

            # ====== 잔고 동기화 ======
            ord_psbl_map = {}
            try:
                balances = _fetch_balances(kis)
                logger.info(f"[보유잔고 API 결과 종목수] {len(balances)}개")
                for stock in balances:
                    logger.info(
                        f"  [잔고] 종목: {stock.get('prdt_name')}, 코드: {stock.get('pdno')}, "
                        f"보유수량: {stock.get('hldg_qty')}, 매도가능: {stock.get('ord_psbl_qty')}"
                    )
                current_holding = {b['pdno']: _to_int(b.get('hldg_qty', 0)) for b in balances if _to_int(b.get('hldg_qty', 0)) > 0}
                ord_psbl_map = {b['pdno']: _to_int(b.get('ord_psbl_qty', 0)) for b in balances}
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)
            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # ====== 매수/매도(전략) LOOP ======
            for code, target in list(code_to_target.items()):
                # 입력 방어
                qty = _to_int(target.get("매수수량") or target.get("qty") or target.get('매수수량_권장'), 0)
                if qty <= 0:
                    # If no explicit qty provided, derive from capital rules
                    # Conservative fallback: use 1 share minimum
                    qty = max(MIN_QTY_PER_TICKET, 1)

                k_value = (target.get("best_k") or target.get("K") or target.get("k"))
                # 트레이더 계산 목표가 우선
                target_price = _to_float(target.get('computed_target_price'))
                # fallback: signal에서 내려준 목표가 (권장하지 않음)
                if target_price is None:
                    target_price = _to_float(target.get("목표가") or target.get("target_price") or target.get('best_k_price'))

                strategy = target.get("strategy") or target.get('strategy_name') or "전월 rolling K 최적화"
                name = target.get("name") or target.get("종목명")

                if target_price is None:
                    logger.warning(f"[SKIP] {code}: target_price 누락 (computed 및 signal 모두 없음)")
                    continue

                try:
                    current_price = _safe_get_price(kis, code)
                    logger.info(f"[📈 현재가] {code}: {current_price}")

                    trade_common = {
                        "datetime": now_str,
                        "code": code,
                        "name": name,
                        "qty": qty,
                        "K": k_value,
                        "target_price": target_price,
                        "strategy": strategy,
                    }

                    # --- 매수 ---
                    if is_open and code not in holding and code not in traded:
                        if current_price is not None and current_price >= float(target_price):
                            result = _with_retry(kis.buy_stock, code, qty)
                            holding[code] = {
                                'qty': int(qty),
                                'buy_price': float(current_price),
                                'trade_common': trade_common
                            }
                            traded[code] = {"buy_time": now_str, "qty": int(qty), "price": float(current_price)}
                            logger.info(f"[✅ 매수주문] {code}, qty={qty}, price={current_price}, result={result}")
                            log_trade({**trade_common, "side": "BUY", "price": current_price,
                                       "amount": int(current_price) * int(qty), "result": result})
                            save_state(holding, traded)
                            time.sleep(ORDER_THROTTLE_SEC)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 미매수")
                            continue

                    # --- 익절/손절 매도 ---
                    if is_open and code in holding:
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here <= 0:
                            logger.info(f"[SKIP] {code}: 매도가능수량=0 (대기/체결중/락) → 매도 보류")
                        else:
                            buy_info = holding[code]
                            buy_price = _to_float(buy_info.get('buy_price'))
                            bqty = _to_int(buy_info.get('qty'), 0)

                            if bqty <= 0 or buy_price is None or current_price is None:
                                logger.warning(f"[매도조건 판정불가] {code} qty={bqty}, buy_price={buy_price}, cur={current_price}")
                            else:
                                profit_pct = ((current_price - buy_price) / buy_price) * 100
                                if profit_pct >= sell_conditions['profit_pct'] or profit_pct <= sell_conditions['loss_pct']:
                                    sell_qty = min(bqty, sellable_here)
                                    cur_price, result = _sell_once(kis, code, sell_qty, prefer_market=True)
                                    logger.info(f"[✅ 매도주문] {code}, qty={sell_qty}, result={result}, 수익률: {profit_pct:.2f}%")
                                    log_trade({**trade_common, "side": "SELL", "price": cur_price,
                                               "amount": (int(cur_price) * int(sell_qty)) if cur_price else 0,
                                               "result": result,
                                               "reason": f"매도조건 (수익률: {profit_pct:.2f}%)"})
                                    holding.pop(code, None)
                                    traded.pop(code, None)
                                    save_state(holding, traded)
                                    time.sleep(RATE_SLEEP_SEC)

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # --- 장중 커트오프(KST) 강제 전량매도 ---
            if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
                _force_sell_all(
                    kis=kis,
                    holding=holding,
                    reason=f"장중 강제전량매도(커트오프 {SELL_FORCE_TIME.strftime('%H:%M')} KST)",
                    passes=FORCE_SELL_PASSES_CUTOFF,
                    include_all_balances=SELL_ALL_BALANCES_AT_CUTOFF,
                    prefer_market=True
                )

            # --- 장마감 전량매도(더블 세이프) ---
            if not is_open:
                _force_sell_all(
                    kis=kis,
                    holding=holding,
                    reason="장마감 전 강제전량매도",
                    passes=FORCE_SELL_PASSES_CLOSE,
                    include_all_balances=True,   # 장마감 시에는 무조건 전체 잔고 대상
                    prefer_market=True
                )
                logger.info("[✅ 장마감, 루프 종료]")
                break

            save_state(holding, traded)
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")


if __name__ == "__main__":
    # allow CLI flag for force-sell-only mode
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--force-sell", action="store_true", help="즉시 강제전량매도 후 종료")
    args = p.parse_args()
    try:
        main(force_sell_mode=args.force_sell)
    except Exception:
        logger.exception("[MAIN_FAIL]")
