import logging

logger = logging.getLogger(__name__)

def simulate_with_k_and_get_metrics(stock_code, k, price_data):
    logger.debug(f"simulate start: code={stock_code}, k={k}, data_len={len(price_data)}")
    equity_curve = [1.0]
    max_drawdown = 0.0
    wins = 0
    trades = 0
    holding_days = []

    returns = []
    peak = equity_curve[0]

    for idx, day in enumerate(price_data):
        logger.debug(f"[{stock_code}] day[{idx}] open={day['open']} high={day['high']} low={day['low']}")
        try:
            target_price = day["open"] + (day["high"] - day["low"]) * k
            if day["high"] > target_price:
                buy_price = target_price
                sell_price = day["close"]
                holding_days.append(1)
                r = (sell_price - buy_price) / buy_price
                returns.append(r)
                trades += 1
                if r > 0:
                    wins += 1
                equity_curve.append(equity_curve[-1] * (1 + r))
                peak = max(peak, equity_curve[-1])
                drawdown = (peak - equity_curve[-1]) / peak
                max_drawdown = max(max_drawdown, drawdown)
        except Exception as e:
            logger.exception(f"[{stock_code}] simulate error at day[{idx}]: {e}")
            continue

    avg_return_pct = round((sum(returns) / trades) * 100, 2) if trades > 0 else 0.0
    win_rate_pct = round((wins / trades) * 100, 2) if trades > 0 else 0.0
    mdd_pct = round(max_drawdown * 100, 2)
    cumulative_return_pct = round((equity_curve[-1] - 1) * 100, 2)
    avg_holding_days = round(sum(holding_days) / len(holding_days), 2) if holding_days else 0

    return {
        "stock_code": stock_code,
        "k": k,
        "avg_return_pct": avg_return_pct,
        "win_rate_pct": win_rate_pct,
        "mdd_pct": mdd_pct,
        "cumulative_return_pct": cumulative_return_pct,
        "trades": trades,
        "avg_holding_days": avg_holding_days
    }

