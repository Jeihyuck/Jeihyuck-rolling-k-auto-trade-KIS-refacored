# simulate_with_k_and_get_metrics.py (신규/필수 전체본)
"""
rolling_k 자동매매 시스템에서 K값별 과거 구간 시뮬레이션 후 성과 메트릭(수익률, 승률, MDD, 거래수 등) 반환
- best_k_meta_strategy.py가 K최적화, 월/분기/연간 rolling, 실전 TopN 종목선정 등에서 계속 호출
- 단일 함수: simulate_with_k_and_get_metrics(code, k_value, price_data:list)
"""

def simulate_with_k_and_get_metrics(
    code: str,
    k_value: float,
    price_data: list[dict],
    fee_rate: float = 0.0015,
) -> dict:
    """단일 K값 기준 월(또는 분기/연간) price_data를 변동성돌파 전략으로 시뮬레이션 후 성과 반환
    - price_data: [{date,open,high,low,close}, ...] (최근순)
    - fee_rate: 거래 수수료, 실전 0.15% 수준(필요시 변경)
    반환 dict 예시:
      { avg_return_pct, win_rate_pct, mdd_pct, trades, cumulative_return_pct, avg_holding_days }
    """
    n = len(price_data)
    if n < 3:
        return {
            "avg_return_pct": 0.0, "win_rate_pct": 0.0, "mdd_pct": 0.0,
            "trades": 0, "cumulative_return_pct": 0.0, "avg_holding_days": 0.0
        }

    rets = []
    wins = 0
    losses = 0
    equities = [1.0]
    holding_days = []
    last_trade_idx = None

    for i in range(1, n):
        today = price_data[i]
        yesterday = price_data[i - 1]
        open_px = float(today["open"])
        high_px = float(yesterday["high"])
        low_px = float(yesterday["low"])
        close_px = float(today["close"])
        rng = high_px - low_px
        target = open_px + k_value * rng

        if today["high"] > target:  # 돌파 발생
            # 진입 후 당일 종가에 매도
            buy_px = target * (1 + fee_rate)
            sell_px = close_px * (1 - fee_rate)
            ret = (sell_px / buy_px - 1) * 100
            rets.append(ret)
            equities.append(equities[-1] * (1 + ret / 100))
            if ret > 0:
                wins += 1
            else:
                losses += 1
            if last_trade_idx is not None:
                holding_days.append(i - last_trade_idx)
            last_trade_idx = i
        else:
            equities.append(equities[-1])

    trades = len(rets)
    avg_ret = sum(rets) / trades if trades else 0.0
    win_rate = (wins / trades * 100) if trades else 0.0
    mdd = 0.0
    max_eq = equities[0]
    for eq in equities:
        if eq > max_eq:
            max_eq = eq
        mdd = max(mdd, (max_eq - eq) / max_eq * 100)
    cumulative_return = (equities[-1] - 1) * 100
    avg_holding_days = sum(holding_days) / len(holding_days) if holding_days else 0.0

    return {
        "avg_return_pct": round(avg_ret, 2),
        "win_rate_pct": round(win_rate, 2),
        "mdd_pct": round(mdd, 2),
        "trades": trades,
        "cumulative_return_pct": round(cumulative_return, 2),
        "avg_holding_days": round(avg_holding_days, 1),
    }

def get_best_k_meta(year_metrics: list, quarter_metrics: list, month_metrics: list) -> float:
    """
    연/분기/월 k별 시뮬레이션 메트릭을 받아, 메타점수(Sharpe 가중합)로 best_k를 찾음
    """
    scores: dict = {}

    def _update(metrics: list, weight: float):
        for m in metrics:
            k = round(float(m["k"]), 2)
            scores[k] = scores.get(k, 0.0) + float(m.get("sharpe", 0.0)) * weight

    _update(year_metrics, 1.0)
    _update(quarter_metrics, 1.5)
    _update(month_metrics, 2.0)

    if not scores:
        return 0.5
    best_k, _ = max(scores.items(), key=lambda x: x[1])
    return round(float(best_k), 2)


def assign_weights(selected: list) -> list:
    """
    승률/수익률 우대, MDD 패널티 반영. 합은 1.0으로 정규화.
    입력 항목 예: {code, win_rate_pct, avg_return_pct, mdd_pct, ...}
    """
    if not selected:
        return []
    raw = []
    for it in selected:
        try:
            win = float(it.get("win_rate_pct", 0.0)) / 100.0
            ret = float(it.get("avg_return_pct", 0.0)) / 100.0
            mdd = abs(float(it.get("mdd_pct", 0.0))) / 100.0
        except Exception:
            win, ret, mdd = 0.5, 0.1, 0.1
        score = (0.6 * win + 0.6 * ret) / max(0.05, (0.4 * mdd))
        raw.append(max(0.0, score))

    s = sum(raw) or 1.0
    ws = [r / s for r in raw]

    out = []
    for it, w in zip(selected, ws):
        obj = dict(it)
        obj["weight"] = round(float(w), 6)
        out.append(obj)
    return out


def _enforce_min_weight_for_forced(items: list, min_w: float = 0.01) -> list:
    """
    forced_include=True 항목은 weight 하한(min_w) 보장. 합계 1 유지.
    """
    if not items:
        return items
    forced_idx = [i for i, it in enumerate(items) if it.get("forced_include")]
    if not forced_idx:
        return items

    weights = [float(it.get("weight", 0.0)) for it in items]
    uplift = [max(0.0, min_w - weights[i]) for i in forced_idx]
    delta_up = sum(uplift)
    if delta_up <= 0:
        return items

    non_idx = [i for i in range(len(items)) if i not in forced_idx]
    non_sum = sum(weights[i] for i in non_idx)
    if non_sum <= 0:
        new_w = 1.0 / len(items)
        for it in items:
            it["weight"] = round(new_w, 6)
        return items

    for i, up in zip(forced_idx, uplift):
        weights[i] += up
    for i in non_idx:
        w = weights[i]
        dec = delta_up * (w / non_sum)
        weights[i] = max(0.0, w - dec)

    s = sum(weights) or 1.0
    weights = [w / s for w in weights]
    for it, w in zip(items, weights):
        it["weight"] = round(float(w), 6)
    return items

