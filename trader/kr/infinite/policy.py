from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal, ROUND_DOWN
import hashlib, json
from .config import InfiniteConfig
from .risk_adapter import REGIME_POLICY


def policy_checksum(config: InfiniteConfig) -> str:
    payload = asdict(config) | {"regimes": REGIME_POLICY}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def integer_buy_quantity(price: Decimal, fraction: Decimal, remaining: Decimal,
                         cash: Decimal, config: InfiniteConfig) -> int:
    effective = price * (Decimal(1) + config.buy_fee_rate + config.slippage_rate)
    budget = min(config.unit_krw * fraction, remaining, cash)
    return int((budget / effective).to_integral_value(rounding=ROUND_DOWN)) if effective > 0 else 0


def net_liquidation_return(price: Decimal, state_qty: int, buy_notional: Decimal,
                           config: InfiniteConfig) -> Decimal:
    if buy_notional <= 0: return Decimal("-1")
    proceeds = price * state_qty * (Decimal(1) - config.sell_fee_rate - config.sell_tax_rate - config.slippage_rate)
    return (proceeds - buy_notional) / buy_notional
