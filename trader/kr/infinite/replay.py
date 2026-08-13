"""Point-in-time daily replay for adjusted 122630 OHLCV CSV data."""
from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import mean


@dataclass(frozen=True)
class ReplayCosts:
    buy_fee: float = .00015
    sell_fee: float = .00015
    sell_tax: float = .0018
    slippage: float = .001


def replay(rows: list[dict], *, capital: float = 15_000_000, unit: float = 375_000,
           target: float = .08, costs: ReplayCosts = ReplayCosts()) -> dict:
    """Signals use only prior closes; orders fill at the following adjusted open."""
    cash, qty, cost_basis, gross_cost, cycles = capital, 0, 0.0, 0.0, []
    equity, max_units, max_invested, buy_dates = [], 0.0, 0.0, set()
    for i in range(50, len(rows)):
        row, history = rows[i], rows[:i]
        close_hist = [float(x["close"]) for x in history[-50:]]
        px_open, px_close = float(row["open"]), float(row["close"])
        if min(px_open, px_close) <= 0: continue
        ma20, ma50 = mean(close_hist[-20:]), mean(close_hist)
        # Historical regime proxy is recomputed as-of i-1, never backfilled from today's regime.
        allow = close_hist[-1] >= ma20 and ma20 >= ma50
        sell_px = px_open * (1 - costs.slippage)
        if qty and sell_px >= (cost_basis / qty) * (1 + target):
            gross = qty * sell_px; cash += gross * (1 - costs.sell_fee - costs.sell_tax)
            gross_cost += gross * (costs.sell_fee + costs.sell_tax); cycles.append(1); qty = 0; cost_basis = 0
        elif allow and str(row["date"]) not in buy_dates and cost_basis < capital:
            buy_px = px_open * (1 + costs.slippage)
            amount = min(unit, capital - cost_basis, cash)
            n = int(amount / (buy_px * (1 + costs.buy_fee)))
            if n:
                paid = n * buy_px * (1 + costs.buy_fee); qty += n; cost_basis += paid; cash -= paid
                gross_cost += n * buy_px * costs.buy_fee; buy_dates.add(str(row["date"]))
        max_units = max(max_units, cost_basis / unit); max_invested = max(max_invested, cost_basis)
        equity.append((str(row["date"]), cash + qty * px_close))
    if not equity: raise ValueError("insufficient replay rows")
    values = [v for _, v in equity]; peak, mdd = values[0], 0.0
    for value in values: peak = max(peak, value); mdd = min(mdd, value / peak - 1)
    years = max(1 / 252, len(values) / 252); total = values[-1] / capital - 1
    return {"data_start": rows[0]["date"], "data_end": rows[-1]["date"], "rows": len(rows),
        "total_return": total, "cagr": (values[-1] / capital) ** (1 / years) - 1, "mdd": mdd,
        "calmar": ((values[-1] / capital) ** (1 / years) - 1) / abs(mdd) if mdd else None,
        "cycle_success_rate": 1.0 if cycles else None, "completed_cycles": len(cycles),
        "max_used_units": max_units, "max_invested_notional": max_invested,
        "capital_exhausted_early": max_invested >= capital, "costs_krw": gross_cost,
        "method": "adjusted OHLC; prior-close signals; next adjusted open; integer shares; distributions embedded in adjusted prices"}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(); p.add_argument("csv"); p.add_argument("--output", default="artifacts/kr_infinite/replay_summary.json")
    args = p.parse_args(argv)
    with open(args.csv, newline="", encoding="utf-8") as f: rows = list(csv.DictReader(f))
    result = replay(rows); out = Path(args.output); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"); return 0


if __name__ == "__main__": raise SystemExit(main())
