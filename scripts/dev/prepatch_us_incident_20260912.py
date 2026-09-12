from pathlib import Path


runner = Path("trader/us/runner/trade_tick_runner.py")
text = runner.read_text(encoding="utf-8")
old = '''    def query_order(**identity):
        detail = provider.get_fills_by_order_no(
            order_no=str(identity.get("order_no") or ""),
            symbol=symbol,
            trade_date=trade_date,
        )
        return detail or {}
'''
new = '''    def query_order(**identity):
        client_key = str(identity.get("client_order_key") or "")
        key_trade_date = next(
            (token for token in client_key.split(":")
             if len(token) == 10 and token[4:5] == "-" and token[7:8] == "-"),
            "",
        )
        lookup_trade_date = key_trade_date or trade_date
        detail = provider.get_fills_by_order_no(
            order_no=str(identity.get("order_no") or ""),
            symbol=symbol,
            trade_date=lookup_trade_date,
        )
        return detail or {}
'''
if old not in text and new not in text:
    raise SystemExit("TQQQ callback query anchor missing")
if old in text:
    runner.write_text(text.replace(old, new, 1), encoding="utf-8")

# Make the main one-shot patcher's replace helper accept a change already made
# by this prepatch step.
patcher = Path("scripts/dev/apply_us_incident_fix_20260912.py")
source = patcher.read_text(encoding="utf-8")
old_helper = '''    if old not in text:
        raise SystemExit(f"expected patch anchor missing: {path}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")
'''
new_helper = '''    if old not in text:
        if new in text:
            return
        raise SystemExit(f"expected patch anchor missing: {path}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")
'''
if old_helper not in source:
    raise SystemExit("replace_once helper anchor missing")
patcher.write_text(source.replace(old_helper, new_helper, 1), encoding="utf-8")

Path(__file__).unlink()
