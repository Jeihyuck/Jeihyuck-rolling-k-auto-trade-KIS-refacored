import logging
import requests
from .kis_wrapper import KisAPI
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import time
import os
import random
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

# ====== 시간대(KST) 및 설정 ======
KST = ZoneInfo("Asia/Seoul")

# 장중 강제 전량매도 커트오프 (KST 기준)
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "15:20").strip()

# 커트오프/장마감 시 보유 전 종목(계좌 잔고 전체) 포함 여부 (기본 True)
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "true").lower() == "true"

# API 호출 간 최소 휴지시간(초)
RATE_SLEEP_SEC = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))

# 커트오프/장마감 매도 시 패스(회차) 수
FORCE_SELL_PASSES_CUTOFF = int(os.getenv("FORCE_SELL_PASSES_CUTOFF", "2"))
FORCE_SELL_PASSES_CLOSE  = int(os.getenv("FORCE_SELL_PASSES_CLOSE",  "4"))

# ====== 실전형 매도/진입 파라미터 ======
PARTIAL1 = float(os.getenv("PARTIAL1", "0.5"))        # 목표가1 도달 시 매도 비중
PARTIAL2 = float(os.getenv("PARTIAL2", "0.3"))        # 목표가2 도달 시 매도 비중
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "0.02"))      # 고점대비 -2% 청산
FAST_STOP = float(os.getenv("FAST_STOP", "0.01"))       # 진입 5분내 -1%
ATR_STOP = float(os.getenv("ATR_STOP", "1.5"))          # ATR 1.5배 손절(절대값)
TIME_STOP_HHMM = os.getenv("TIME_STOP_HHMM", "13:00")   # 시간 손절 기준

# (기존 단일 임계치 대비) 백테/실전 괴리 축소를 위한 기본값 조정
DEFAULT_PROFIT_PCT = float(os.getenv("DEFAULT_PROFIT_PCT", "3.0"))   # 백업용
DEFAULT_LOSS_PCT   = float(os.getenv("DEFAULT_LOSS_PCT",   "-2.0"))  # 백업용


def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식 오류 → 기본값 15:20 적용: {hhmm}")
        return dtime(hour=15, minute=20)


SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)
TIME_STOP_TIME = _parse_hhmm(TIME_STOP_HHMM)


def get_month_first_date():
    today = datetime.now(KST)
    month_first = today.replace(day=1)
    return month_first.strftime("%Y-%m-%d")


def fetch_rebalancing_targets(date):
    """
    /rebalance/run/{date}?force_order=true 호출 결과에서
    selected 또는 selected_stocks 키를 우선 사용.
    (가능하면 각 항목에 weight, k_best, target_price 포함)
    """
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}?force_order=true"
    response = requests.post(REBALANCE_API_URL)
    logger.info(f"[🛰️ 리밸런싱 API 전체 응답]: {response.text}")
    if response.status_code == 200:
        data = response.json()
        logger.info(f"[🎯 리밸런싱 종목]: {data.get('selected') or data.get('selected_stocks')}")
        return data.get("selected") or data.get("selected_stocks") or []
    else:
        raise Exception(f"리밸런싱 API 호출 실패: {response.text}")


def log_trade(trade: dict):
    today = datetime.now(KST).strftime("%Y-%m-%d")
    logfile = LOG_DIR / f"trades_{today}.json"
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(json.dumps(trade, ensure_ascii=False) + "\n")


def save_state(holding, traded):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"holding": holding, "traded": traded}, f, ensure_ascii=False, indent=2)


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
            return state.get("holding", {}), state.get("traded", {})
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


# ====== ATR/보조 ======
def _get_atr(kis: KisAPI, code: str, window: int = 14) -> Optional[float]:
    if hasattr(kis, "get_atr"):
        try:
            return kis.get_atr(code, window=window)  # type: ignore
        except Exception as e:
            logger.warning(f"[ATR_FAIL] {code}: {e}")
            return None
    return None


def _init_position_state(holding: Dict[str, Any], code: str, entry_price: float, qty: int, k_value: Any, target_price: Optional[float]):
    """보유 상태에 실전형 필드를 세팅(분할/트레일/ATR/시간손절)."""
    atr = _get_atr(KisAPI(), code)  # 별도 API 호출로 약간 비용 발생
    rng_eff = (atr * 1.5) if (atr and atr > 0) else max(1.0, entry_price * 0.01)
    t1 = entry_price + 0.5 * rng_eff
    t2 = entry_price + 1.0 * rng_eff
    holding[code] = {
        'qty': int(qty),
        'buy_price': float(entry_price),
        'entry_time': datetime.now(KST).isoformat(),
        'high': float(entry_price),
        'tp1': float(t1),
        'tp2': float(t2),
        'sold_p1': False,
        'sold_p2': False,
        'trail_pct': TRAIL_PCT,
        'atr': float(atr) if atr else None,
        'stop_abs': float(entry_price - ATR_STOP * atr) if atr else float(entry_price * (1 - FAST_STOP)),
        'k_value': k_value,
        'target_price_src': float(target_price) if target_price is not None else None,
    }


# ----- 1회 매도 시도 -----
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


# ====== FIX: 잔고 표준화 반환 (항상 list[dict]) ======
def _fetch_balances(kis: KisAPI):
    """
    항상 포지션 리스트(list[dict])를 반환하도록 표준화.
    - kis.get_balance() 가 dict({"cash": int, "positions": list[dict]}) → positions 리스트만 추출
    - kis.get_balance_all() 이 리스트를 준다면 그대로 반환
    """
    if hasattr(kis, "get_balance_all"):
        res = _with_retry(kis.get_balance_all)
    else:
        res = _with_retry(kis.get_balance)

    if isinstance(res, dict):
        positions = res.get("positions") or []
        if not isinstance(positions, list):
            logger.error(f"[BAL_STD_FAIL] positions 타입 이상: {type(positions)}")
            return []
        return positions
    elif isinstance(res, list):
        return res
    else:
        logger.error(f"[BAL_STD_FAIL] 지원하지 않는 반환 타입: {type(res)}")
        return []


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


# ====== 실전형 청산 로직 ======
def _adaptive_exit(kis: KisAPI, code: str, pos: Dict[str, Any]) -> Optional[str]:
    """분할매도/트레일/ATR/시간 손절을 종합 적용. 실행 시 매도 주문을 내리고 이유를 반환."""
    now = datetime.now(KST)
    try:
        cur = _safe_get_price(kis, code)
        if cur is None:
            return None
    except Exception:
        return None

    # 상태 갱신
    pos['high'] = max(float(pos.get('high', cur)), float(cur))
    qty = _to_int(pos.get('qty'), 0)
    if qty <= 0:
        return None

    # 1) 진입 5분 내 급락 손절
    try:
        ent = datetime.fromisoformat(pos.get('entry_time')).replace(tzinfo=KST)
    except Exception:
        ent = now
    if now - ent <= timedelta(minutes=5) and cur <= float(pos['buy_price']) * (1 - FAST_STOP):
        _sell_once(kis, code, qty, prefer_market=True)
        return "FAST_STOP"

    # 2) ATR 손절(절대값)
    stop_abs = pos.get('stop_abs')
    if stop_abs is not None and cur <= float(stop_abs):
        _sell_once(kis, code, qty, prefer_market=True)
        return "ATR_STOP"

    # 3) 목표가 분할
    if (not pos.get('sold_p1')) and cur >= float(pos.get('tp1', 9e18)):
        sell_qty = max(1, int(qty * PARTIAL1))
        _sell_once(kis, code, sell_qty, prefer_market=True)
        pos['qty'] = qty - sell_qty
        pos['sold_p1'] = True
        return "TP1"
    if (not pos.get('sold_p2')) and cur >= float(pos.get('tp2', 9e18)):
        sell_qty = max(1, int(qty * PARTIAL2))
        _sell_once(kis, code, sell_qty, prefer_market=True)
        pos['qty'] = qty - sell_qty
        pos['sold_p2'] = True
        return "TP2"

    # 4) 트레일링 스탑(고점대비 하락)
    trail_line = float(pos['high']) * (1 - float(pos.get('trail_pct', TRAIL_PCT)))
    if cur <= trail_line:
        _sell_once(kis, code, qty, prefer_market=True)
        return "TRAIL"

    # 5) 시간 손절 (예: 13:00까지 수익전환 없으면 청산)
    if now.time() >= TIME_STOP_TIME:
        buy_px = float(pos.get('buy_price'))
        if cur < buy_px:  # 손실 지속 시만 적용(보수적)
            _sell_once(kis, code, qty, prefer_market=True)
            return "TIME_STOP"

    # 6) 장 후반 강제 청산은 루프 말미에서 처리
    return None


def main():
    kis = KisAPI()
    rebalance_date = get_month_first_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date}")
    logger.info(f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
                f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}")

    # ======== 상태 복구 ========
    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # ======== 리밸런싱 대상 종목 추출 ========
    targets = fetch_rebalancing_targets(rebalance_date)
    code_to_target: Dict[str, Any] = {}
    for target in targets:
        code = target.get("stock_code") or target.get("code")
        if code:
            code_to_target[code] = target

    loop_sleep_sec = 2.5

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
            for code, target in code_to_target.items():
                qty = _to_int(target.get("매수수량") or target.get("qty"), 0)
                if qty <= 0:
                    logger.info(f"[SKIP] {code}: 매수수량 없음/0")
                    continue

                k_value = (target.get("best_k") or target.get("K") or target.get("k"))
                target_price = _to_float(target.get("목표가") or target.get("target_price"))
                strategy = target.get("strategy") or "전월 rolling K 최적화"
                name = target.get("name") or target.get("종목명")

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

                    # --- 매수 --- (돌파 진입)
                    if is_open and code not in holding and code not in traded:
                        if target_price is not None:
                            enter_cond = (current_price is not None and current_price >= float(target_price))
                        else:
                            enter_cond = False
                        if enter_cond:
                            result = _with_retry(kis.buy_stock, code, qty)
                            _init_position_state(holding, code, float(current_price), int(qty), k_value, target_price)
                            traded[code] = {"buy_time": now_str, "qty": int(qty), "price": float(current_price)}
                            logger.info(f"[✅ 매수주문] {code}, qty={qty}, price={current_price}, result={result}")
                            log_trade({**trade_common, "side": "BUY", "price": current_price,
                                       "amount": int(current_price) * int(qty), "result": result})
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 미매수")
                            continue

                    # --- 실전형 청산 ---
                    if is_open and code in holding:
                        # 매도가능 0이면 보류(중복주문 방지)
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here <= 0:
                            logger.info(f"[SKIP] {code}: 매도가능수량=0 (대기/체결중/락) → 매도 보류")
                        else:
                            reason = _adaptive_exit(kis, code, holding[code])
                            if reason:
                                # 포지션 수량은 _adaptive_exit 내부에서 차감
                                log_trade({**trade_common, "side": "SELL", "price": _safe_get_price(kis, code),
                                           "amount": 0, "result": reason, "reason": reason})
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # --- 장중 커트오프(KST) 강제 전량매도 (마지막 안전장치) ---
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
    main()
