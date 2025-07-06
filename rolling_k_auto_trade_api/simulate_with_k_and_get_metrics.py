def simulate_with_k_and_get_metrics(stock_code, k, price_data):
    returns = []
    peak = -float("inf")
    max_drawdown = 0
    wins = 0
    trades = 0

    for day in price_data:
        try:
            target_price = day["open"] + (day["high"] - day["low"]) * k
            if day["high"] > target_price:
                buy_price = target_price
                sell_price = day["close"]
                r = (sell_price - buy_price) / buy_price
                returns.append(r)
                trades += 1
                if r > 0:
                    wins += 1
                peak = max(peak, sell_price)
                drawdown = (peak - sell_price) / peak if peak > 0 else 0
                max_drawdown = max(max_drawdown, drawdown)
        except:
            continue

    avg_return_pct = round(sum(returns) / trades * 100, 2) if trades > 0 else 0.0
    win_rate_pct = round((wins / trades) * 100, 2) if trades > 0 else 0.0
    mdd_pct = round(max_drawdown * 100, 2)

    return {
        "stock_code": stock_code,
        "k": k,
        "avg_return_pct": avg_return_pct,
        "win_rate_pct": win_rate_pct,
        "mdd_pct": mdd_pct,
    }
