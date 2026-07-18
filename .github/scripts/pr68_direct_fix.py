from __future__ import annotations

from pathlib import Path


def replace_exact(path: str, old: str, new: str, *, expected: int = 1) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != expected:
        raise RuntimeError(f"{path}: expected {expected} matches, found {count}: {old[:80]!r}")
    target.write_text(text.replace(old, new), encoding="utf-8")


def main() -> None:
    replace_exact(
        "trader/us/runner/trade_close_runner.py",
        '''            try:\n                from trader.us.execution.reconcile import reconcile_positions\n                try:\n                    reconcile_result = reconcile_positions(provider=provider, trade_date=trade_date)\n                except TypeError:\n                    reconcile_result = reconcile_positions(provider=provider)\n                logger.info("[US_TRADE_CLOSE][RECONCILE] status=%s", reconcile_result.get("status"))\n            except Exception as exc:\n                logger.error("[US_TRADE_CLOSE][ERROR] reconcile failed: %s", exc)\n                reconcile_result = {\n                    "status": "TEMP_ERROR",\n                    "balance_fetch_status": "FAILED",\n                    "authoritative_positions": False,\n                    "preserve_previous_positions": True,\n                    "error": str(exc),\n                    "positions": [],\n                    "position_count": 0,\n                }\n''',
        '''            try:\n                from trader.us.execution.reconcile import reconcile_positions\n                reconcile_result = reconcile_positions(provider=provider, trade_date=trade_date)\n                logger.info("[US_TRADE_CLOSE][RECONCILE] status=%s", reconcile_result.get("status"))\n            except TypeError as exc:\n                logger.error("[US_TRADE_CLOSE][CONTRACT_ERROR] reconcile TypeError: %s", exc)\n                reconcile_result = {\n                    "status": "CONTRACT_ERROR",\n                    "reason": "reconcile_internal_type_error",\n                    "balance_fetch_status": "FAILED",\n                    "authoritative_positions": False,\n                    "preserve_previous_positions": True,\n                    "error": str(exc),\n                    "positions": [],\n                    "position_count": 0,\n                }\n            except Exception as exc:\n                logger.error("[US_TRADE_CLOSE][ERROR] reconcile failed: %s", exc)\n                reconcile_result = {\n                    "status": "TEMP_ERROR",\n                    "balance_fetch_status": "FAILED",\n                    "authoritative_positions": False,\n                    "preserve_previous_positions": True,\n                    "error": str(exc),\n                    "positions": [],\n                    "position_count": 0,\n                }\n''',
    )
    replace_exact(
        "trader/us/runner/trade_close_runner.py",
        '''                try:\n                    save_position_snapshot(positions, trade_date=trade_date, balance_fetch_status="OK",\n                                           balance_parse_status="OK", authoritative_positions=True,\n                                           preserve_previous_positions=False, close_source="kis_final_balance")\n                except TypeError:\n                    # Compatibility for injected test/adapter callables; repository\n                    # implementation always receives authoritative flags above.\n                    save_position_snapshot(positions)\n                logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", len(positions))\n''',
        '''                save_position_snapshot(positions, trade_date=trade_date, balance_fetch_status="OK",\n                                       balance_parse_status="OK", authoritative_positions=True,\n                                       preserve_previous_positions=False, close_source="kis_final_balance")\n                logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", len(positions))\n''',
    )

    replace_exact(
        "trader/us/execution/reconcile.py",
        '''    if not trade_date:\n        from datetime import date\n        trade_date = date.today().isoformat()\n''',
        '''    if not str(trade_date or "").strip():\n        from trader.us.market_calendar import now_ny\n        trade_date = now_ny().strftime("%Y-%m-%d")\n        logger.warning("[US_RECONCILE][TRADE_DATE_FALLBACK] source=ny_clock trade_date=%s", trade_date)\n''',
        expected=2,
    )
    replace_exact(
        "trader/us/execution/reconcile.py",
        '''        try:\n            from trader.us.db.repos import save_position_snapshot\n            try:\n                saved = save_position_snapshot(positions, trade_date=trade_date)\n            except TypeError:\n                saved = save_position_snapshot(positions)\n            logger.info(\n''',
        '''        try:\n            from trader.us.db.repos import save_position_snapshot\n            saved = save_position_snapshot(\n                positions,\n                trade_date=trade_date,\n                balance_fetch_status="OK",\n                balance_parse_status="OK",\n                authoritative_positions=True,\n                preserve_previous_positions=False,\n                close_source="kis_reconcile_balance",\n            )\n            logger.info(\n''',
    )
    replace_exact(
        "trader/us/execution/reconcile.py",
        '''            if fills_resp and isinstance(fills_resp, dict):\n                if fills_resp.get("filled_qty", 0) > 0:\n''',
        '''            if fills_resp and isinstance(fills_resp, dict):\n                fill_contract_status = str(fills_resp.get("status") or "OK").upper()\n                if fill_contract_status != "OK":\n                    logger.error(\n                        "[US_RECONCILE][ACK_RECONCILE][FILL_CONTRACT_ERROR] symbol=%s status=%s",\n                        symbol, fill_contract_status,\n                    )\n                    failed_count += 1\n                    unresolved_count += 1\n                    symbols_by_status["unresolved"].append(symbol)\n                    continue\n                if fills_resp.get("filled_qty", 0) > 0:\n''',
    )
    replace_exact(
        "trader/us/execution/reconcile.py",
        '''                if not isinstance(mark_result, dict) or mark_result.get("status") == "OK":\n''',
        '''                if isinstance(mark_result, dict) and mark_result.get("status") == "OK":\n''',
        expected=3,
    )
    replace_exact(
        "trader/us/execution/reconcile.py",
        '''                else:\n                    unresolved_count += 1\n                    symbols_by_status["unresolved"].append(symbol)\n''',
        '''                else:\n                    failed_count += 1\n                    unresolved_count += 1\n                    symbols_by_status["unresolved"].append(symbol)\n''',
        expected=3,
    )
    replace_exact(
        "trader/us/execution/reconcile.py",
        '''                logger.error(\n                    "[US_RECONCILE][ACK_RECONCILE][ERROR] mark_order_filled failed symbol=%s: %s",\n                    symbol, exc,\n                )\n            continue\n''',
        '''                logger.error(\n                    "[US_RECONCILE][ACK_RECONCILE][ERROR] mark_order_filled failed symbol=%s: %s",\n                    symbol, exc,\n                )\n                failed_count += 1\n                unresolved_count += 1\n                symbols_by_status["unresolved"].append(symbol)\n            continue\n''',
    )
    replace_exact(
        "trader/us/execution/reconcile.py",
        '''            except Exception as exc:\n                logger.error("[US_RECONCILE][ACK_RECONCILE][ERROR] balance_delta_confirm failed symbol=%s: %s", symbol, exc)\n            continue\n''',
        '''            except Exception as exc:\n                logger.error("[US_RECONCILE][ACK_RECONCILE][ERROR] balance_delta_confirm failed symbol=%s: %s", symbol, exc)\n                failed_count += 1\n                unresolved_count += 1\n                symbols_by_status["unresolved"].append(symbol)\n            continue\n''',
    )
    replace_exact(
        "trader/us/execution/reconcile.py",
        '''                    logger.error(\n                        "[US_RECONCILE][ACK_RECONCILE][ERROR] balance_reconcile_buy failed symbol=%s: %s",\n                        symbol, exc,\n                    )\n                continue\n''',
        '''                    logger.error(\n                        "[US_RECONCILE][ACK_RECONCILE][ERROR] balance_reconcile_buy failed symbol=%s: %s",\n                        symbol, exc,\n                    )\n                    failed_count += 1\n                    unresolved_count += 1\n                    symbols_by_status["unresolved"].append(symbol)\n                continue\n''',
    )
    replace_exact(
        "trader/us/execution/reconcile.py",
        '''        "status": "OK" if unresolved_count == 0 else "WARN",\n''',
        '''        "status": "ERROR" if failed_count > 0 else ("OK" if unresolved_count == 0 else "WARN"),\n''',
    )

    replace_exact(
        "trader/us/execution/order_router.py",
        '''    def _persist_with_trade_date(func, payload):\n        try:\n            return func(payload, trade_date=trade_date)\n        except TypeError:\n            # Compatibility for older injected test adapters; production repo\n            # functions accept and receive the explicit US trade_date above.\n            return func(payload)\n''',
        '''    def _accepts_trade_date(func) -> bool:\n        import inspect\n        try:\n            params = inspect.signature(func).parameters.values()\n        except (TypeError, ValueError):\n            return True\n        return any(\n            param.name == "trade_date" or param.kind == inspect.Parameter.VAR_KEYWORD\n            for param in params\n        )\n\n    def _persist_with_trade_date(func, payload):\n        if _accepts_trade_date(func):\n            return func(payload, trade_date=trade_date)\n        return func(payload)\n''',
    )
    replace_exact(
        "trader/us/execution/order_router.py",
        '''    try:\n        db_keys = load_today_order_keys(trade_date=trade_date)\n    except TypeError:\n        db_keys = load_today_order_keys()\n    except Exception:\n        db_keys = set()\n''',
        '''    try:\n        db_keys = (\n            load_today_order_keys(trade_date=trade_date)\n            if _accepts_trade_date(load_today_order_keys)\n            else load_today_order_keys()\n        )\n    except Exception:\n        db_keys = set()\n''',
    )

    test_path = Path("tests/us/test_us_reconcile_contract_hardening.py")
    test_path.write_text(
        '''from __future__ import annotations\n\n\ndef _pending_sell_order():\n    return {\n        "symbol": "AMD",\n        "side": "SELL",\n        "order_no": "O1",\n        "client_order_key": "CK1",\n        "qty_requested": 7,\n        "pre_order_position_qty": 7,\n        "limit_price": 100,\n    }\n\n\ndef test_ack_reconcile_non_dict_mark_result_is_failure(monkeypatch):\n    from trader.us.execution import reconcile\n\n    monkeypatch.setattr(\n        "trader.us.db.repos.load_pending_ack_orders",\n        lambda trade_date, env="practice": [_pending_sell_order()],\n    )\n    monkeypatch.setattr(\n        "trader.us.db.repos.mark_order_filled_by_reconcile",\n        lambda **kwargs: None,\n    )\n\n    class Provider:\n        def get_balance(self, force_refresh=False):\n            return {"positions": []}\n\n        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):\n            return {\n                "status": "OK",\n                "filled_qty": 7,\n                "avg_price": 100,\n                "symbol": "AMD",\n                "side": "SELL",\n                "order_no": "O1",\n            }\n\n    result = reconcile.reconcile_ack_orders_with_balance(\n        provider=Provider(), trade_date="2026-07-17"\n    )\n\n    assert result["status"] == "ERROR"\n    assert result["confirmed_count"] == 0\n    assert result["failed_count"] == 1\n    assert result["unresolved_count"] == 1\n\n\ndef test_ack_reconcile_rejects_regressed_provider_snapshot(monkeypatch):\n    from trader.us.execution import reconcile\n\n    monkeypatch.setattr(\n        "trader.us.db.repos.load_pending_ack_orders",\n        lambda trade_date, env="practice": [_pending_sell_order()],\n    )\n    calls = []\n    monkeypatch.setattr(\n        "trader.us.db.repos.mark_order_filled_by_reconcile",\n        lambda **kwargs: calls.append(kwargs) or {"status": "OK"},\n    )\n\n    class Provider:\n        def get_balance(self, force_refresh=False):\n            return {"positions": []}\n\n        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):\n            return {\n                "status": "EVIDENCE_QUANTITY_REGRESSION",\n                "filled_qty": 3,\n                "avg_price": 100,\n                "symbol": "AMD",\n                "side": "SELL",\n                "order_no": "O1",\n            }\n\n    result = reconcile.reconcile_ack_orders_with_balance(\n        provider=Provider(), trade_date="2026-07-17"\n    )\n\n    assert result["status"] == "ERROR"\n    assert result["confirmed_count"] == 0\n    assert result["failed_count"] == 1\n    assert calls == []\n\n\ndef test_close_does_not_retry_reconcile_without_trade_date(tmp_path, monkeypatch):\n    monkeypatch.chdir(tmp_path)\n    calls = []\n\n    class Provider:\n        def get_balance(self, force_refresh=False):\n            return {"positions": [], "balance_parse_status": "OK"}\n\n    def broken_reconcile(provider, *, trade_date):\n        calls.append(trade_date)\n        raise TypeError("internal reconcile bug")\n\n    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())\n    monkeypatch.setattr(\n        "trader.us.execution.fills.get_fills_today",\n        lambda **kwargs: {"status": "OK", "fills": []},\n    )\n    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", broken_reconcile)\n    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *args, **kwargs: True)\n    monkeypatch.setattr(\n        "trader.us.execution.reconcile.classify_ack_orders_with_final_balance",\n        lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0},\n    )\n    monkeypatch.setattr(\n        "trader.us.runner.daily_report_runner.run_daily_report",\n        lambda **kwargs: {"status": "OK", "report": {"report_consistency": "OK"}},\n    )\n\n    from trader.us.runner.trade_close_runner import run_trade_close\n\n    result = run_trade_close(\n        env="practice", offline=False, force_now="2026-07-17T16:05:00-04:00"\n    )\n\n    assert calls == ["2026-07-17"]\n    assert result["status"] == "ERROR"\n    assert result["reconcile_status"] == "CONTRACT_ERROR"\n''',
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
