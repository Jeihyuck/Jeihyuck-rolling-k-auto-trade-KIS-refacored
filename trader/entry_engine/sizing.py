from __future__ import annotations


def calculate_position_size(
    *,
    capital: float,
    entry_price: float,
    atr: float,
    risk_per_trade: float = 0.01,
) -> dict[str, float]:
    risk_amount = float(capital) * float(risk_per_trade)
    stop_price = float(entry_price) - (float(atr) * 2.0)
    risk_per_share = float(entry_price) - stop_price
    shares = int(risk_amount / risk_per_share) if risk_per_share > 0 else 0
    return {
        "position_size": float(shares * float(entry_price)),
        "shares": float(shares),
        "stop_price": float(stop_price),
        "risk_amount": float(risk_amount),
        "atr": float(atr),
        "entry_price": float(entry_price),
    }
