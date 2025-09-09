# FILE: `trader/trader.py`

from __future__ import annotations
import logging
import requests
from .kis_wrapper import KisAPI, append_fill
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import time
import os
import random
from typing import Optional, Dict, Any, Tuple
import csv

# === RK-Max v3+ 최소 패치: 스냅샷·오버레이·킬타임 ===
from .rebalance_engine import load_latest_snapshot  # Top10 스냅샷 병합
from .overlay import decide_carry_over              # 스윙 오버레이

# RK-Max 유틸(가능하면 사용, 없으면 graceful fallback)
try:
    from .rkmax_utils import blend_k, recent_features
except Exception:
    # 배포 초기에 rkmax_utils가 없을 수 있으므로 더미 함수로 안전가동
    def blend_k(k_month: float, day: int, atr20: Optional[float], atr60: Optional[float]) -> float:
        return float(k_month) if k_month is not None else 0.5

    def recent_features(kis, code: str) -> Dict[str, Optional[float]]:
        return {"atr20": None, "atr60": None}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

# ====== 시간대(KST) 및 설정 ======
KST = ZoneInfo("Asia/Seoul")

# 장중 강제 전량매도 커트오프 (KST 기준) — 기본 14:30으로 변경(RK-Max 권장)
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "14:30").strip()
# 루프 종료 킬 타임 (KST 기준) — 14:35 권장
ACTION_KILL_TIME_STR = os.getenv("ACTION_KILL_TIME", "14:35").strip()
# 커트오프/장마감 시 보유 전 종목(계좌 잔고 전체) 포함 여부 (기본 True)
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "true").lower() == "true"
# API 호출 간 최소 휴지시간(초)
RATE_SLEEP_SEC = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))
# 커트오프/장마감 매도 시 패스(회차) 수
FORCE_SELL_PASSES_CUTOFF = int(os.getenv("FORCE_SELL_PASSES_CUTOFF", "2"))
FORCE_SELL_PASSES_CLOSE = int(os.getenv("FORCE_SELL_PASSES_CLOSE", "4"))

# ====== 실전형 매도/진입 파라미터 ======
PARTIAL1 = float(os.getenv("PARTIAL1", "0.5"))   # 목표가1 도달 시 매도 비중
PARTIAL2 = float(os.getenv("PARTIAL2", "0.3"))   # 목표가2 도달 시 매도 비중
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "0.02"))  # 고점대비 -2% 청산
FAST_STOP = float(os.getenv("FAST_STOP", "0.01"))  # 진입 5분내 -1%
ATR_STOP = float(os.getenv("ATR_STOP", "1.5"))     # ATR 1.5배 손절(절대값)
TIME_STOP_HHMM = os.getenv("TIME_STOP_HHMM", "13:00")  # 시간 손절 기준

# (기존 단일 임계치 대비) 백테/실전 괴리 축소를 위한 기본값 조정
DEFAULT_PROFIT_PCT = float(os.getenv("DEFAULT_PROFIT_PCT", "3.0"))  # 백업용
DEFAULT_LOSS_PCT = float(os.getenv("DEFAULT_LOSS_PCT", "-2.0"))     # 백업용

# ====== RK-Max 보강 파라미터 ======
DAILY_CAPITAL = int(os.getenv("DAILY_CAPITAL", "3000000"))            # 일일 총 집행 금액(원)
SLIPPAGE_LIMIT_PCT = float(os.getenv("SLIPPAGE_LIMIT_PCT", "0.15"))   # 슬리피지 로깅 임계(정보용)
# 신규: 진입 슬리피지 가드(목표가 대비 불리 체결 한도)
SLIPPAGE_ENTER_GUARD_PCT = float(os.getenv("SLIPPAGE_ENTER_GUARD_PCT", "1.5"))
# (선택) 단일종목 비중 가드
W_MAX_ONE = float(os.getenv("W_MAX_ONE", "0.25"))
W_MIN_ONE = float(os.getenv("W_MIN_ONE", "0.03"))

# 리밸런싱 기준일 앵커: "first"(월초·기본) / "today"(당일)
REBALANCE_ANCHOR = os.getenv("REBALANCE_ANCHOR", "first").lower().strip()


def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] 시간 형식 오류 → 기본값 적용: {hhmm}")
        return dtime(hour=15, minute=20)


SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)
TIME_STOP_TIME = _parse_hhmm(TIME_STOP_HHMM)
KILL_TIME = _parse_hhmm(ACTION_KILL_TIME_STR)


def get_rebalance_anchor_date():
    """리밸런싱 기준일을 환경변수로 제어.
    - REBALANCE_ANCHOR=first → 해당 월 1일(기본)
    - REBALANCE_ANCHOR=today → 오늘 날짜
    """
    today = datetime.now(KST).date()
    if REBALANCE_ANCHOR == "today":
        return today.strftime("%Y-%m-%d")
    # default: first of month
    return today.replace(day=1).strftime("%Y-%m-%d")


def fetch_rebalancing_targets(date):
    """ /rebalance/run/{date}?force_order=true 호출 결과에서 selected 또는 selected_stocks 키를 우선 사용.
    (가능하면 각 항목에 weight, k_best, target_price 포함)
    """
    # 127.0.0.1로 고정 (GitHub Actions에서 localhost 해석 문제 예방)
    REBALANCE_API_URL = f"http://127.0.0.1:8000/rebalance/run/{date}?force_order=true"
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


def _init_position_state_from_balance(holding: Dict[str, Any], code: str, avg_price: float, qty: int):
    """계좌에 이미 들고 있던 종목에 대해 능동관리 상태를 부트스트랩.
    FAST_STOP 오작동 방지를 위해 entry_time을 10분 전으로 설정."""
    if qty <= 0 or code in holding:
        return
    atr = _get_atr(KisAPI(), code)
    rng_eff = (atr * 1.5) if (atr and atr > 0) else max(1.0, avg_price * 0.01)
    t1 = avg_price + 0.5 * rng_eff
    t2 = avg_price + 1.0 * rng_eff

    holding[code] = {
        'qty': int(qty),
        'buy_price': float(avg_price),
        'entry_time': (datetime.now(KST) - timedelta(minutes=10)).isoformat(),  # fast stop 회피
        'high': float(avg_price),
        'tp1': float(t1),
        'tp2': float(t2),
        'sold_p1': False,
        'sold_p2': False,
        'trail_pct': TRAIL_PCT,
        'atr': float(atr) if atr else None,
        'stop_abs': float(avg_price - ATR_STOP * atr) if atr else float(avg_price * (1 - FAST_STOP)),
        'k_value': None,
        'target_price_src': None,
    }


# ----- 1회 매도 시도 -----
def _sell_once(kis: KisAPI, code: str, qty: int, prefer_market=True) -> Tuple[Optional[float], Any]:
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
    """항상 포지션 리스트(list[dict])를 반환하도록 표준화."""
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

    # === RK-Max v3+ 추가: 스윙 오버레이 캐리오버 선별 ===
    try:
        carry_cnt = 0
        for code in list(target_codes):
            pos = holding.get(code)
            if not pos:
                continue
            try:
                cur_px = _safe_get_price(kis, code)
                dec = decide_carry_over(
                    hit_tp1=bool(pos.get('sold_p1', False)),
                    close=float(cur_px or pos.get('buy_price') or 0.0),
                    day_high=float(pos.get('high') or 0.0),
                    atr=float(pos.get('atr') or 0.0),
                    close_ge_ma20=False,   # 지표 미존재 시 보수적 False
                    close_ge_vwap=False,
                    volume_rank_pct=int(pos.get('volume_rank_pct', 50)),
                    had_cutoff=True,
                    carry_days=int(pos.get('carry_days', 0)),
                    carry_max_days=int(os.getenv('CARRY_MAX_DAYS', '3')),
                )
                if dec.carry_over:
                    pos['carry_over'] = True
                    pos['carry_days'] = int(pos.get('carry_days', 0)) + 1
                    target_codes.discard(code)
                    carry_cnt += 1
                    logger.info(f"[CARRY-OVER] {code} {dec.reason} carry_frac={dec.carry_frac}")
            except Exception as e:
                logger.warning(f"[CARRY-OVER-ERR] {code} {e}")
        if carry_cnt:
            save_state(holding, {})
            logger.info(f"[CARRY-OVER] 강제매도 대상에서 제외된 종목수: {carry_cnt}")
    except Exception as e:
        logger.warning(f"[CARRY-OVER-BLOCK-ERR] {e}")

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
def _adaptive_exit(kis: KisAPI, code: str, pos: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], Optional[Any], Optional[int]]:
    """분할매도/트레일/ATR/시간 손절을 종합 적용.
    실행 시 매도 주문을 내리고 (reason, exec_price, result, sell_qty) 반환."""
    now = datetime.now(KST)
    try:
        cur = _safe_get_price(kis, code)
        if cur is None:
            return None, None, None, None
    except Exception:
        return None, None, None, None

    # 상태 갱신
    pos['high'] = max(float(pos.get('high', cur)), float(cur))
    qty = _to_int(pos.get('qty'), 0)
    if qty <= 0:
        return None, None, None, None

    # 1) 진입 5분 내 급락 손절
    try:
        ent = datetime.fromisoformat(pos.get('entry_time')).replace(tzinfo=KST)
    except Exception:
        ent = now
    if now - ent <= timedelta(minutes=5) and cur <= float(pos['buy_price']) * (1 - FAST_STOP):
        exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
        return "FAST_STOP", exec_px, result, qty

    # 2) ATR 손절(절대값)
    stop_abs = pos.get('stop_abs')
    if stop_abs is not None and cur <= float(stop_abs):
        exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
        return "ATR_STOP", exec_px, result, qty

    # 3) 목표가 분할
    if (not pos.get('sold_p1')) and cur >= float(pos.get('tp1', 9e18)):
        sell_qty = max(1, int(qty * PARTIAL1))
        exec_px, result = _sell_once(kis, code, sell_qty, prefer_market=True)
        pos['qty'] = qty - sell_qty
        pos['sold_p1'] = True
        return "TP1", exec_px, result, sell_qty

    if (not pos.get('sold_p2')) and cur >= float(pos.get('tp2', 9e18)):
        sell_qty = max(1, int(qty * PARTIAL2))
        exec_px, result = _sell_once(kis, code, sell_qty, prefer_market=True)
        pos['qty'] = qty - sell_qty
        pos['sold_p2'] = True
        return "TP2", exec_px, result, sell_qty

    # 4) 트레일링 스탑(고점대비 하락)
    trail_line = float(pos['high']) * (1 - float(pos.get('trail_pct', TRAIL_PCT)))
    if cur <= trail_line:
        exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
        return "TRAIL", exec_px, result, qty

    # 5) 시간 손절 (예: 13:00까지 수익전환 없으면 청산)
    if now.time() >= TIME_STOP_TIME:
        buy_px = float(pos.get('buy_price'))
        if cur < buy_px:  # 손실 지속 시만 적용(보수적)
            exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
            return "TIME_STOP", exec_px, result, qty

    # 6) 장 후반 강제 청산은 루프 말미에서 처리
    return None, None, None, None


# ====== 보조: fills CSV에 name 채워넣기 또는 보완 기록 함수 ======
def ensure_fill_has_name(odno: str, code: str, name: str, qty: int = 0, price: float = 0.0):
    """오늘의 fills CSV를 열어 ODNO 일치 레코드가 있으면 name 컬럼을 채움.
    없으면 append_fill()로 보조 기록을 남김."""
    try:
        fills_dir = Path("fills")
        fills_dir.mkdir(exist_ok=True)
        today_path = fills_dir / f"fills_{datetime.now().strftime('%Y%m%d')}.csv"

        updated = False
        if today_path.exists():
            # 읽기
            with open(today_path, "r", encoding="utf-8", newline="") as f:
                reader = list(csv.reader(f))
            if reader:
                header = reader[0]
                # 안전하게 인덱스 찾기
                try:
                    idx_odno = header.index("ODNO")
                    idx_code = header.index("code")
                    idx_name = header.index("name")
                except ValueError:
                    idx_odno = None
                    idx_code = None
                    idx_name = None

                if idx_odno is not None and idx_name is not None and idx_code is not None:
                    for i in range(1, len(reader)):
                        row = reader[i]
                        # 보호: 행 길이가 짧으면 패스
                        if len(row) <= max(idx_odno, idx_code, idx_name):
                            continue
                        if (row[idx_odno] == str(odno) or (not row[idx_odno] and str(odno) == "")) and row[idx_code] == str(code):
                            # 채워넣기 (비어있을 때만)
                            if not row[idx_name]:
                                row[idx_name] = name or ""
                                reader[i] = row
                                updated = True
                                logger.info(f"[FILL_NAME_UPDATE] ODNO={odno} code={code} name={name}")
                                break

        if updated:
            # 덮어쓰기
            with open(today_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(reader)
            return

        # 찾지 못하면 append_fill로 보조 기록 남김 (중복 가능성 존재)
        append_fill("BUY", code, name or "", qty, price or 0.0, odno or "", note="ensure_fill_added_by_trader")
    except Exception as e:
        logger.warning(f"[ENSURE_FILL_FAIL] odno={odno} code={code} ex={e}")


# ====== RK-Max: 목표가 계산 & 지정가→시장가 Fallback ======
def compute_entry_target(
    kis: KisAPI,
    code: str,
    k_month: Optional[float],
    given_target: Optional[float] = None
) -> Tuple[int, Optional[float]]:
    """월간 K(k_month)에 최근 변동성(ATR20/60)을 블렌딩해 진입 타깃 가격을 계산.
    - API가 목표가를 이미 제공했더라도(given_target), '월말 보정 K'로 **조정**해 사용.
    - API 목표가가 없으면: 오늘 시가 + K_use * (전일 고-저)
      ※ 전일 범위를 얻을 수 없으면 백업 규칙: 현재가 * (1 + DEFAULT_PROFIT_PCT/100)
    반환: (target_price:int, k_use:Optional[float])
    """

    # --- 1) 최근 특성 / 전일 고저 / 오늘시가 확보 ---
    feats = {}
    try:
        feats = recent_features(kis, code) or {}
    except Exception:
        feats = {}
    atr20 = feats.get("atr20")
    atr60 = feats.get("atr60")

    today_open = None
    prev_high = None
    prev_low = None
    try:
        if hasattr(kis, "get_today_open"):
            today_open = _to_float(_with_retry(kis.get_today_open, code))
        if hasattr(kis, "get_prev_high_low"):
            prev = _with_retry(kis.get_prev_high_low, code)  # { "high":..., "low":... } 가정
            if isinstance(prev, dict):
                prev_high = _to_float(prev.get("high"))
                prev_low = _to_float(prev.get("low"))
    except Exception:
        pass

    # --- 2) K 블렌딩 (월말 보정 K) ---
    day = datetime.now(KST).day
    try:
        k_use = blend_k(float(k_month) if k_month is not None else 0.5, day, atr20, atr60)
    except Exception:
        k_use = float(k_month) if k_month is not None else 0.5
    baseline_k = float(k_month) if k_month is not None else 0.5

    # --- 3) API 목표가가 있을 때도 보정(delta) 적용 ---
    if given_target is not None:
        try:
            base = float(given_target)
            if prev_high is not None and prev_low is not None:
                rng = max(1.0, float(prev_high) - float(prev_low))
                delta = (float(k_use) - float(baseline_k)) * rng
                adjusted = int(round(base + delta))
                logger.info(
                    "[TARGET/adjust] %s base=%s baseline_k=%.3f k_use=%.3f rng=%s -> target=%s",
                    code, base, baseline_k, k_use, rng, adjusted
                )
                return adjusted, k_use
            # 전일 범위를 못 구하면 보정 없이 그대로 사용(안전)
            tgt = int(round(base))
            logger.info(
                "[TARGET/adjust-skip] %s base=%s (no prev range) -> target=%s (k_use=%.3f)",
                code, base, tgt, k_use
            )
            return tgt, k_use
        except Exception:
            logger.warning("[TARGET/adjust-fail] %s given_target=%s -> fallback compute", code, given_target)
            # 아래 일반 계산으로 폴백

    # --- 4) (API 목표가가 없을 때) 표준 계산 ---
    if today_open is not None and prev_high is not None and prev_low is not None:
        rng = max(1.0, float(prev_high) - float(prev_low))
        target = int(round(float(today_open) + float(k_use) * rng))
        logger.info("[TARGET] %s K_use=%.3f open=%s range=%s -> target=%s", code, k_use, today_open, rng, target)
        return target, k_use

    # --- 5) 백업 규칙: 현재가 기반 ---
    cur = _safe_get_price(kis, code)
    if cur is not None and cur > 0:
        target = int(round(float(cur) * (1.0 + DEFAULT_PROFIT_PCT / 100.0)))
        logger.info("[TARGET/backup] %s cur=%s -> target=%s (%.2f%%)", code, cur, target, DEFAULT_PROFIT_PCT)
        return target, k_use

    # --- 6) 최후의 보루: 적당한 고정값(매우 보수적) ---
    logger.warning("[TARGET/fallback-last] %s: 모든 소스 실패 → 고정치 사용", code)
    return int(0), k_use


def place_buy_with_fallback(kis: KisAPI, code: str, qty: int, limit_price: int) -> Dict[str, Any]:
    """지정가 주문(가능시) → 3초 대기 → 미체결이면 시장가 전환. 결과 dict 반환."""
    result_limit = None

    # 1) 지정가 가능 시 우선 시도
    try:
        if hasattr(kis, "buy_stock_limit") and limit_price and limit_price > 0:
            result_limit = _with_retry(kis.buy_stock_limit, code, qty, int(limit_price))
            logger.info("[BUY-LIMIT] %s qty=%s limit=%s -> %s", code, qty, limit_price, result_limit)
            time.sleep(3.0)
            # 1-1) 체결 확인 가능할 때만 Fallback 판단
            if hasattr(kis, "check_filled"):
                try:
                    filled = bool(_with_retry(kis.check_filled, result_limit))
                except Exception:
                    filled = False
                if filled:
                    return result_limit
        else:
            logger.info("[BUY-LIMIT] API 미지원 또는 limit_price 무효 → 시장가로 진행")
    except Exception as e:
        logger.warning("[BUY-LIMIT-FAIL] %s qty=%s limit=%s err=%s", code, qty, limit_price, e)

    # 2) 시장가 전환
    try:
        if hasattr(kis, "buy_stock_market"):
            result_mkt = _with_retry(kis.buy_stock_market, code, qty)
        else:
            # 후방호환: buy_stock가 시장가로 동작하는 래퍼
            result_mkt = _with_retry(kis.buy_stock, code, qty)
        logger.info("[BUY-MKT] %s qty=%s (from limit=%s) -> %s", code, qty, limit_price, result_mkt)
        return result_mkt
    except Exception as e:
        logger.error("[BUY-MKT-FAIL] %s qty=%s err=%s", code, qty, e)
        raise


def _weight_to_qty(kis: KisAPI, code: str, weight: float, daily_capital: int) -> int:
    """weight와 일일 집행금으로 수량을 산출 (현재가 기반)."""
    weight = max(0.0, float(weight))
    alloc = int(round(daily_capital * weight))
    price = _safe_get_price(kis, code) or 0
    if price <= 0:
        return 0
    return max(0, int(alloc // int(price)))


def main():
    kis = KisAPI()

    rebalance_date = get_rebalance_anchor_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date} (anchor={REBALANCE_ANCHOR})")
    logger.info(
        f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
        f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}"
    )
    logger.info(f"[💰 DAILY_CAPITAL] {DAILY_CAPITAL:,}원")
    logger.info(f"[🛡️ SLIPPAGE_ENTER_GUARD_PCT] {SLIPPAGE_ENTER_GUARD_PCT:.2f}%")

    # ======== 상태 복구 ========
    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # ======== 리밸런싱 대상 종목 추출 ========
    targets = fetch_rebalancing_targets(rebalance_date)  # API 반환 dict 목록

    # === RK-Max v3+ 추가: 08:50/12:00 스냅샷 병합 (core 우선)
    try:
        snap = load_latest_snapshot(datetime.now(KST))
        if snap and isinstance(snap, dict):
            uni = snap.get('universe') or {}
            core_list = uni.get('core') or []
            added = 0
            for it in core_list:
                code = (it.get('code') if isinstance(it, dict) else None)
                if not code:
                    continue
                exists = any(((t.get('stock_code') == code) or (t.get('code') == code)) for t in targets)
                if not exists:
                    targets.append({"code": code, "weight": it.get('weight', 0.1), "strategy": "Top10Core"})
                    added += 1
            if added:
                logger.info(f"[UNIVERSE SNAPSHOT] core 병합: +{added}개")
    except Exception as e:
        logger.warning(f"[UNIVERSE SNAPSHOT LOAD FAIL] {e}")

    # 후처리: qty 없고 weight만 있으면 DAILY_CAPITAL로 수량 계산
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

        if qty <= 0 and weight is not None:
            try:
                qty = _weight_to_qty(kis, code, float(weight), DAILY_CAPITAL)
                logger.info(f"[ALLOC->QTY] {code} weight={weight} → qty={qty}")
            except Exception:
                qty = 0

        processed_targets[code] = {
            "code": code,
            "name": name,
            "best_k": k_best,
            "target_price": target_price,
            "qty": qty,
            "strategy": strategy,
        }
    code_to_target: Dict[str, Any] = processed_targets

    loop_sleep_sec = 2.5

    try:
        while True:
            is_open = kis.is_market_open()
            now_dt_kst = datetime.now(KST)
            now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[⏰ 장상태] {'OPEN' if is_open else 'CLOSED'} / KST={now_str}")

            # === RK-Max v3+ 추가: 14:35 킬 게이트 ===
            if now_dt_kst.time() >= KILL_TIME:
                save_state(holding, traded)
                logger.info("[KILL] ACTION_KILL_TIME 도달 → 안전 종료")
                break

            # ====== 잔고 동기화 & 보유분 능동관리 부트스트랩 ======
            ord_psbl_map: Dict[str, int] = {}
            name_map: Dict[str, str] = {}
            try:
                balances = _fetch_balances(kis)
                logger.info(f"[보유잔고 API 결과 종목수] {len(balances)}개")
                for stock in balances:
                    code_b = stock.get('pdno')
                    name_b = stock.get('prdt_name')
                    name_map[code_b] = name_b
                    logger.info(
                        f" [잔고] 종목: {name_b}, 코드: {code_b}, "
                        f"보유수량: {stock.get('hldg_qty')}, 매도가능: {stock.get('ord_psbl_qty')}"
                    )

                current_holding = {b['pdno']: _to_int(b.get('hldg_qty', 0)) for b in balances if _to_int(b.get('hldg_qty', 0)) > 0}
                ord_psbl_map = {b['pdno']: _to_int(b.get('ord_psbl_qty', 0)) for b in balances}

                # 신규 보유분을 능동관리 대상으로 자동 초기화 (A)
                for b in balances:
                    code_b = b.get('pdno')
                    qty_b = _to_int(b.get('hldg_qty', 0))
                    if qty_b > 0 and code_b and code_b not in holding:
                        avg_b = _to_float(b.get('pchs_avg_pric') or b.get('avg_price') or 0.0, 0.0)
                        if avg_b and avg_b > 0:
                            _init_position_state_from_balance(holding, code_b, avg_b, qty_b)

                # 실제 잔고에서 사라진 보유항목은 정리
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)

            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # ====== 매수/매도(전략) LOOP — 오늘의 타겟 ======
            for code, target in code_to_target.items():
                qty = _to_int(target.get("매수수량") or target.get("qty"), 0)
                if qty <= 0:
                    logger.info(f"[SKIP] {code}: 매수수량 없음/0")
                    continue

                # 입력 K 값
                k_value = (target.get("best_k") or target.get("K") or target.get("k"))
                k_value_float = None if k_value is None else _to_float(k_value)

                # 목표가(있으면 사용, 없으면 K 블렌딩으로 계산) — 단, 주어진 목표가도 보정 적용
                raw_target_price = _to_float(target.get("목표가") or target.get("target_price"))
                eff_target_price, k_used = compute_entry_target(
                    kis, code, k_month=k_value_float, given_target=raw_target_price
                )

                strategy = target.get("strategy") or "전월 rolling K 최적화"
                name = target.get("name") or target.get("종목명") or name_map.get(code)

                try:
                    current_price = _safe_get_price(kis, code)
                    logger.info(f"[📈 현재가] {code}: {current_price}")

                    trade_common_buy = {
                        "datetime": now_str,
                        "code": code,
                        "name": name,
                        "qty": qty,
                        "K": k_value if k_value is not None else k_used,
                        "target_price": eff_target_price,
                        "strategy": strategy,
                    }

                    # --- 매수 --- (돌파 진입 + 슬리피지 가드)
                    if is_open and code not in holding and code not in traded:
                        enter_cond = (
                            current_price is not None and
                            eff_target_price is not None and
                            int(current_price) >= int(eff_target_price)
                        )

                        if enter_cond:
                            # 진입 슬리피지 가드
                            guard_ok = True
                            if eff_target_price and eff_target_price > 0 and current_price is not None:
                                slip_pct = ((float(current_price) - float(eff_target_price)) / float(eff_target_price)) * 100.0
                                if slip_pct > SLIPPAGE_ENTER_GUARD_PCT:
                                    guard_ok = False
                                    logger.info(
                                        f"[ENTER-GUARD] {code} 진입슬리피지 {slip_pct:.2f}% > "
                                        f"{SLIPPAGE_ENTER_GUARD_PCT:.2f}% → 진입 스킵"
                                    )

                            if not guard_ok:
                                continue

                            result = place_buy_with_fallback(kis, code, qty, limit_price=int(eff_target_price))

                            # 성공 여부 판별 후 fills에 name 채우기 시도
                            try:
                                if isinstance(result, dict) and result.get("rt_cd") == "0":
                                    out = result.get("output") or {}
                                    odno = out.get("ODNO") or out.get("ord_no") or out.get("order_no") or ""
                                    ensure_fill_has_name(odno=odno, code=code, name=name or "", qty=qty, price=current_price or 0.0)
                            except Exception as e:
                                logger.warning(f"[BUY_FILL_NAME_FAIL] code={code} ex={e}")

                            _init_position_state(holding, code, float(current_price), int(qty),
                                                 (k_value if k_value is not None else k_used), eff_target_price)
                            traded[code] = {"buy_time": now_str, "qty": int(qty), "price": float(current_price)}
                            logger.info(f"[✅ 매수주문] {code}, qty={qty}, price={current_price}, result={result}")

                            log_trade({
                                **trade_common_buy,
                                "side": "BUY",
                                "price": current_price,
                                "amount": int(current_price) * int(qty),
                                "result": result
                            })
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({eff_target_price}), 미매수")
                            continue

                    # --- 실전형 청산 (타겟 보유포지션) ---
                    if is_open and code in holding:
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here <= 0:
                            logger.info(f"[SKIP] {code}: 매도가능수량=0 (대기/체결중/락) → 매도 보류")
                        else:
                            reason, exec_price, result, sold_qty = _adaptive_exit(kis, code, holding[code])
                            if reason:
                                trade_common_sell = {
                                    "datetime": now_str,
                                    "code": code,
                                    "name": name,
                                    "qty": int(sold_qty or 0),
                                    "K": k_value if k_value is not None else k_used,
                                    "target_price": eff_target_price,
                                    "strategy": strategy,
                                }
                                log_trade({
                                    **trade_common_sell,
                                    "side": "SELL",
                                    "price": exec_price,
                                    "amount": int(exec_price or 0) * int(sold_qty or 0),
                                    "result": result,
                                    "reason": reason
                                })
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # ====== (A) 비타겟 보유분도 장중 능동관리 ======
            if is_open:
                for code in list(holding.keys()):
                    if code in code_to_target:
                        continue  # 위 루프에서 이미 처리
                    sellable_here = ord_psbl_map.get(code, 0)
                    if sellable_here <= 0:
                        logger.info(f"[SKIP-기존보유] {code}: 매도가능수량=0 (대기/체결중/락)")
                        continue
                    name = name_map.get(code)
                    reason, exec_price, result, sold_qty = _adaptive_exit(kis, code, holding[code])
                    if reason:
                        trade_common = {
                            "datetime": now_str,
                            "code": code,
                            "name": name,
                            "qty": int(sold_qty or 0),
                            "K": holding[code].get("k_value"),
                            "target_price": holding[code].get("target_price_src"),
                            "strategy": "기존보유 능동관리",
                        }
                        log_trade({
                            **trade_common,
                            "side": "SELL",
                            "price": exec_price,
                            "amount": int(exec_price or 0) * int(sold_qty or 0),
                            "result": result,
                            "reason": reason
                        })
                        save_state(holding, traded)
                        time.sleep(RATE_SLEEP_SEC)

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
                    include_all_balances=True,  # 장마감 시에는 무조건 전체 잔고 대상
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
