from pathlib import Path

p = Path("tests/test_kr_close_phase_forced.py")
text = p.read_text(encoding="utf-8")
old = '''    calls = {"n": 0}\n    mod = types.SimpleNamespace(main=lambda: calls.__setitem__("n", calls["n"] + 1) or 0)\n    monkeypatch.setitem(sys.modules, "trader.pb1_runner", mod)\n'''
new = '''    calls = {"n": 0}\n\n    def fake_success_main():\n        calls["n"] += 1\n        result_path = runner.Path(runner.os.environ["PB1_SESSION_RESULT_PATH"])\n        result_path.parent.mkdir(parents=True, exist_ok=True)\n        result_path.write_text(\n            runner.json.dumps({"status": "OK", "entry_status": "DONE", "sell_orders_ack": 0}),\n            encoding="utf-8",\n        )\n        return 0\n\n    mod = types.SimpleNamespace(main=fake_success_main)\n    monkeypatch.setitem(sys.modules, "trader.pb1_runner", mod)\n'''
if text.count(old) != 1:
    raise RuntimeError(f"close success fixture marker count={text.count(old)}")
p.write_text(text.replace(old, new, 1), encoding="utf-8")
print("PR127 close success fixture aligned with durable PB1 result contract")
