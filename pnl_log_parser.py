# pnl_log_parser.py
import re
import json
import csv
from pathlib import Path

def parse_pnl_log(log_path):
    result = []
    p_pnl = re.compile(r'\[P&L\] (\w+) SELL (\d+)@([\d.]+) / BUY=([\d.]+) → PnL=([\d.-]+)% \(₩([\d,.-]+)\)')
    p_trigger = re.compile(r'\[SELL-TRIGGER\] (\w+) REASON=([A-Z_]+) qty=(\d+) price=([\d.]+)')
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            m = p_pnl.search(line)
            if m:
                code, qty, sell_px, buy_px, pnl_pct, profit = m.groups()
                result.append({
                    "code": code, "qty": int(qty), "sell_px": float(sell_px),
                    "buy_px": float(buy_px), "pnl_pct": float(pnl_pct),
                    "profit": float(str(profit).replace(',', ''))
                })
    return result

def write_to_csv(data, out_path):
    if not data:
        print("No data to write.")
        return
    keys = data[0].keys()
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(data)

if __name__ == "__main__":
    logs = list(Path("logs").glob("trades_*.json"))
    for logf in logs:
        items = parse_pnl_log(logf)
        write_to_csv(items, logf.with_suffix('.csv'))
        print(f"{logf}: {len(items)} rows exported.")
