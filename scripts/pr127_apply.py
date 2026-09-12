from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if new in text:
        return
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected exactly one match, got {count}: {old[:100]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


def replace_in_section(path: str, start_marker: str, end_marker: str, replacements: list[tuple[str, str, int]]) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    section = text[start:end]
    for old, new, expected in replacements:
        count = section.count(old)
        if count != expected:
            raise RuntimeError(f"{path}: section replacement expected {expected}, got {count}: {old!r}")
        section = section.replace(old, new)
    p.write_text(text[:start] + section + text[end:], encoding="utf-8")


# ---------------------------------------------------------------------------
# 1) US quote provenance: stale KIS/DB fallback must never look fresh.
# ---------------------------------------------------------------------------
replace_once(
    "trader/us/execution/kis_us_client.py",
    '''        if cached and now_ts - cached[0] <= ttl:\n            logger.debug("[US_KIS][CACHE_HIT] endpoint=GET_price symbol=%s ttl=%.1f", symbol, ttl)\n            return cached[1]\n        try:\n            data = self._get(tr["path"], headers=headers, params=params)\n            self._response_cache[cache_key] = (time.time(), data)\n            self._temporary_error_streak[cache_key] = 0\n            return data\n        except KisUSTemporaryError:\n            self._temporary_error_streak[cache_key] = self._temporary_error_streak.get(cache_key, 0) + 1\n            if cached and self._temporary_error_streak[cache_key] >= 2:\n                self.stats["stale_price_fallback_count"] += 1\n                logger.warning("[US_KIS][STALE_PRICE_FALLBACK] endpoint=GET_price symbol=%s streak=%d", symbol, self._temporary_error_streak[cache_key])\n                return cached[1]\n            raise\n''',
    '''        if cached and now_ts - cached[0] <= ttl:\n            age_sec = max(0.0, now_ts - float(cached[0]))\n            logger.debug("[US_KIS][CACHE_HIT] endpoint=GET_price symbol=%s ttl=%.1f", symbol, ttl)\n            cached_payload = dict(cached[1]) if isinstance(cached[1], dict) else cached[1]\n            if isinstance(cached_payload, dict):\n                cached_payload.update({\n                    "_quote_quality": "FRESH_CACHE",\n                    "_quote_source": "KIS_CACHE",\n                    "_quote_asof_epoch": float(cached[0]),\n                    "_quote_age_sec": age_sec,\n                })\n            return cached_payload\n        try:\n            fetched_at = time.time()\n            data = self._get(tr["path"], headers=headers, params=params)\n            if isinstance(data, dict):\n                data = dict(data)\n                data.update({\n                    "_quote_quality": "FRESH",\n                    "_quote_source": "KIS_LIVE",\n                    "_quote_asof_epoch": fetched_at,\n                    "_quote_age_sec": 0.0,\n                })\n            self._response_cache[cache_key] = (fetched_at, data)\n            self._temporary_error_streak[cache_key] = 0\n            return data\n        except KisUSTemporaryError:\n            self._temporary_error_streak[cache_key] = self._temporary_error_streak.get(cache_key, 0) + 1\n            if cached and self._temporary_error_streak[cache_key] >= 2:\n                self.stats["stale_price_fallback_count"] += 1\n                age_sec = max(0.0, time.time() - float(cached[0]))\n                logger.warning("[US_KIS][STALE_PRICE_FALLBACK] endpoint=GET_price symbol=%s streak=%d age_sec=%.3f", symbol, self._temporary_error_streak[cache_key], age_sec)\n                stale_payload = dict(cached[1]) if isinstance(cached[1], dict) else cached[1]\n                if isinstance(stale_payload, dict):\n                    stale_payload.update({\n                        "_quote_quality": "STALE",\n                        "_quote_source": "KIS_STALE_CACHE",\n                        "_quote_asof_epoch": float(cached[0]),\n                        "_quote_age_sec": age_sec,\n                    })\n                return stale_payload\n            raise\n''',
)

replace_once(
    "trader/us/data_provider.py",
    '''                    "symbol": symbol,\n                    "_stale_date": row[0].strftime("%Y-%m-%d"),\n                }\n''',
    '''                    "symbol": symbol,\n                    "_stale_date": row[0].strftime("%Y-%m-%d"),\n                    "stale": True,\n                    "quality": "stale",\n                    "source": "DB_STALE",\n                    "asof": row[0].strftime("%Y-%m-%d"),\n                }\n''',
)

replace_once(
    "trader/us/data_provider.py",
    '''            data = {\n                "last": output.get("last", "0"),\n                "open": output.get("open", "0"),\n                "high": output.get("high", "0"),\n                "low": output.get("low", "0"),\n                "tvol": output.get("tvol", "0"),\n                "symbol": symbol,\n            }\n''',
    '''            quote_quality = str(result.get("_quote_quality") or "FRESH").upper()\n            data = {\n                "last": output.get("last", "0"),\n                "open": output.get("open", "0"),\n                "high": output.get("high", "0"),\n                "low": output.get("low", "0"),\n                "tvol": output.get("tvol", "0"),\n                "symbol": symbol,\n                "stale": quote_quality in {"STALE", "DEGRADED", "SUSPECT"},\n                "quality": quote_quality.lower(),\n                "source": str(result.get("_quote_source") or "KIS_LIVE"),\n                "asof_epoch": result.get("_quote_asof_epoch"),\n                "age_sec": result.get("_quote_age_sec"),\n            }\n''',
)

replace_once(
    "trader/us/pb1/us_exit_engine.py",
    '''                if isinstance(price_data, dict):\n                    current_price = float(price_data.get("last") or price_data.get("price") or 0)\n                else:\n                    current_price = float(price_data or 0)\n''',
    '''                if isinstance(price_data, dict):\n                    quote_quality = str(price_data.get("quality") or "").lower()\n                    quote_stale = bool(\n                        price_data.get("stale")\n                        or price_data.get("suspect")\n                        or price_data.get("_stale_date")\n                        or quote_quality in {"stale", "suspect", "degraded"}\n                    )\n                    if quote_stale:\n                        logger.warning(\n                            "[US_EXIT][QUOTE_STALE_BLOCK] symbol=%s exchange=%s source=%s quality=%s asof=%s action=skip_exit_state_mutation",\n                            symbol, exchange, price_data.get("source") or "unknown",\n                            quote_quality or "unknown", price_data.get("asof") or price_data.get("_stale_date") or price_data.get("asof_epoch"),\n                        )\n                        continue\n                    current_price = float(price_data.get("last") or price_data.get("price") or 0)\n                else:\n                    current_price = float(price_data or 0)\n''',
)


# ---------------------------------------------------------------------------
# 2) Preserve already-routed SELL truth when fills become degraded.
# ---------------------------------------------------------------------------
replace_once(
    "trader/us/runner/trade_tick_runner.py",
    '''    sell_notional_routed = float(exit_route_result.get("sell_notional_routed", 0.0) or 0.0)\n    exit_routed_before_entry = 1\n\n    # ── ENTRY 평가''',
    '''    sell_notional_routed = float(exit_route_result.get("sell_notional_routed", 0.0) or 0.0)\n    exit_routed_before_entry = 1\n    routed_sent = sum(1 for order in orders if str(order.get("status") or "").upper() in {"ACK", "DRY_RUN", "SIGNAL_ONLY", "SUBMITTED", "SENT"})\n    routed_ack = sum(1 for order in orders if str(order.get("status") or "").upper() in {"ACK", "FILLED"})\n    routed_rejected = sum(1 for order in orders if str(order.get("status") or "").upper() in {"REJECT", "REJECTED"})\n    routed_blocked = sum(1 for order in orders if str(order.get("status") or "").upper() in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"})\n\n    # ── ENTRY 평가''',
)

replace_in_section(
    "trader/us/runner/trade_tick_runner.py",
    "    # fills contract/temp error is degraded: block duplicate-sensitive BUYs, keep session alive.\n",
    "    elif not daily_notional_available:\n",
    [
        ('"orders": [],', '"orders": list(orders),', 2),
        ('"ack": 0,', '"ack": routed_ack,', 2),
        ('"blocked": 0,', '"blocked": routed_blocked,', 2),
        ('"orders_sent": 0,', '"orders_sent": routed_sent,\n            "orders_ack": routed_ack,\n            "orders_rejected": routed_rejected,\n            "sell_notional_routed": sell_notional_routed,\n            "exit_intents": len(exit_intents),', 2),
    ],
)


# ---------------------------------------------------------------------------
# 3) TQQQ unresolved cancel must escalate visibly; never guess terminal truth.
# ---------------------------------------------------------------------------
replace_once(
    "trader/us/infinite/integration.py",
    "import logging\nimport math\nimport uuid\n",
    "import logging\nimport math\nimport os\nimport uuid\n",
)

replace_once(
    "trader/us/infinite/integration.py",
    '''    result = {"expired": len(expired), "cancel_requested": 0, "terminal": 0, "pending": 0}\n''',
    '''    result = {"expired": len(expired), "cancel_requested": 0, "terminal": 0, "pending": 0, "escalated": 0}\n''',
)

replace_once(
    "trader/us/infinite/integration.py",
    '''        # A prior cancel request is never replayed blindly. The next tick\n        # re-queries broker truth and keeps BUY fenced until terminal evidence.\n        if metadata.get("tqqq_ttl_cancel_requested_at"):\n            result["pending"] += 1\n            continue\n''',
    '''        # A prior cancel request is never replayed blindly. The next tick\n        # re-queries broker truth and keeps BUY fenced until terminal evidence.\n        # If broker truth remains unresolved for too long, surface a durable RED/manual\n        # reconcile requirement instead of silently starving BUY forever.  This does\n        # not guess FILLED/CANCELLED and therefore preserves fail-closed safety.\n        cancel_requested_at = metadata.get("tqqq_ttl_cancel_requested_at")\n        if cancel_requested_at:\n            unresolved_age_sec = 0.0\n            try:\n                requested_dt = datetime.fromisoformat(str(cancel_requested_at).replace("Z", "+00:00"))\n                if requested_dt.tzinfo is None:\n                    requested_dt = requested_dt.replace(tzinfo=timezone.utc)\n                unresolved_age_sec = max(0.0, (now.astimezone(timezone.utc) - requested_dt.astimezone(timezone.utc)).total_seconds())\n            except Exception:\n                unresolved_age_sec = 0.0\n            escalate_after_sec = max(\n                int(ttl_seconds),\n                int(os.getenv("US_TQQQ_TTL_UNRESOLVED_ESCALATE_SEC", "900") or 900),\n            )\n            if unresolved_age_sec >= escalate_after_sec:\n                result["escalated"] += 1\n                if not metadata.get("tqqq_ttl_unresolved_escalated_at"):\n                    logger.error(\n                        "[TQQQ_INF][TTL_RECONCILE][ESCALATED] order_no=%s key=%s unresolved_age_sec=%.1f action=manual_reconcile_required buy_fence=keep",\n                        order_no, client_order_key, unresolved_age_sec,\n                    )\n                    if hasattr(repository, "mark_ttl_unresolved_escalated"):\n                        repository.mark_ttl_unresolved_escalated(\n                            order, escalated_at=now, unresolved_age_sec=unresolved_age_sec,\n                        )\n            result["pending"] += 1\n            continue\n''',
)

replace_once(
    "trader/us/infinite/repository.py",
    '''    def apply_ttl_terminal_observation(self, order: dict, observation: dict) -> dict:\n''',
    '''    def mark_ttl_unresolved_escalated(self, order: dict, *, escalated_at: datetime, unresolved_age_sec: float) -> None:\n        """Persist a manual-reconcile RED marker without changing broker/order status."""\n        key = str(order.get("client_order_key") or "")\n        trade_date = str(order.get("trade_date") or "")\n        if not key or not trade_date:\n            raise ValueError("TTL unresolved escalation requires original order identity")\n        patch = json.dumps({\n            "tqqq_ttl_unresolved_escalated_at": escalated_at.isoformat(),\n            "tqqq_ttl_unresolved_age_sec": float(unresolved_age_sec),\n            "manual_reconcile_required": True,\n            "manual_reconcile_reason": "TQQQ_TTL_UNRESOLVED_AFTER_CANCEL",\n        }, default=str)\n        with self.engine.begin() as conn:\n            conn.execute(text("""\n                UPDATE us_orders\n                SET meta=COALESCE(meta, '{}'::jsonb) || CAST(:patch AS jsonb), updated_at=NOW()\n                WHERE trade_date=:trade_date AND client_order_key=:key\n            """), {"patch": patch, "trade_date": trade_date, "key": key})\n\n    def apply_ttl_terminal_observation(self, order: dict, observation: dict) -> dict:\n''',
)


# ---------------------------------------------------------------------------
# 4) Make timeout contract explicit and reuse one fresh SELL balance snapshot/tick.
# ---------------------------------------------------------------------------
replace_once(
    "trader/us/execution/tick_context.py",
    '''    balance_snapshot: dict = field(default_factory=dict)\n    positions_by_symbol: dict[str, dict] = field(default_factory=dict)\n''',
    '''    balance_snapshot: dict = field(default_factory=dict)\n    sell_balance_snapshot: dict | None = None\n    positions_by_symbol: dict[str, dict] = field(default_factory=dict)\n''',
)

replace_once(
    "trader/us/execution/order_router.py",
    '''def _get_broker_position(kis_client: Any, symbol: str) -> dict | None:\n    method = _client_method(kis_client, "get_balance")\n    if method is not None:\n        bal = method(force_refresh=True)\n    else:\n        raw_method = _client_method(kis_client, "get_us_balance")\n        if raw_method is None:\n            return None\n        from trader.us.data_provider import normalize_us_balance\n        bal = normalize_us_balance(raw_method(force_refresh=True))\n    for pos in bal.get("positions", []) if isinstance(bal, dict) else []:\n        if str(pos.get("symbol") or "").upper().strip() == str(symbol or "").upper().strip():\n            return pos\n    return {}\n''',
    '''def _get_broker_position(kis_client: Any, symbol: str, context: Any | None = None) -> dict | None:\n    # The first SELL in a tick performs the required fresh broker balance read.\n    # Remaining SELLs reuse that same immutable pre-routing snapshot, avoiding\n    # N symbols x NASD/NYSE/AMEX sequential balance sweeps while preserving the\n    # fresh pre-submit broker-truth contract.\n    bal = getattr(context, "sell_balance_snapshot", None) if context is not None else None\n    if not isinstance(bal, dict):\n        method = _client_method(kis_client, "get_balance")\n        if method is not None:\n            bal = method(force_refresh=True)\n        else:\n            raw_method = _client_method(kis_client, "get_us_balance")\n            if raw_method is None:\n                return None\n            from trader.us.data_provider import normalize_us_balance\n            bal = normalize_us_balance(raw_method(force_refresh=True))\n        if context is not None and isinstance(bal, dict):\n            context.sell_balance_snapshot = bal\n            if hasattr(context, "count"):\n                context.count("sell_balance_fresh_snapshots")\n    for pos in bal.get("positions", []) if isinstance(bal, dict) else []:\n        if str(pos.get("symbol") or "").upper().strip() == str(symbol or "").upper().strip():\n            return pos\n    return {}\n''',
)

replace_once(
    "trader/us/execution/order_router.py",
    '''        broker_pos = _get_broker_position(kis_client, symbol)\n''',
    '''        broker_pos = _get_broker_position(kis_client, symbol, context=context)\n''',
)

replace_once(
    "trader/us/runner/trade_session_runner.py",
    '''    tick_timeout_sec = int(os.getenv("US_TICK_TIMEOUT_SEC", "270"))\n    if interval_sec >= 300 and tick_timeout_sec < 240:\n        logger.warning(\n            "[US_SESSION][CONFIG][TICK_TIMEOUT_RAISED] interval_sec=%d requested_timeout_sec=%d effective_timeout_sec=240",\n            interval_sec, tick_timeout_sec,\n        )\n        tick_timeout_sec = 240\n''',
    '''    requested_tick_timeout_sec = int(os.getenv("US_TICK_TIMEOUT_SEC", "240"))\n    tick_timeout_min_sec = int(os.getenv("US_TICK_TIMEOUT_MIN_SEC", "240"))\n    tick_timeout_sec = max(requested_tick_timeout_sec, tick_timeout_min_sec) if interval_sec >= 300 else requested_tick_timeout_sec\n    logger.info(\n        "[US_SESSION][CONFIG][TICK_TIMEOUT_CONTRACT] interval_sec=%d requested_timeout_sec=%d minimum_timeout_sec=%d effective_timeout_sec=%d",\n        interval_sec, requested_tick_timeout_sec, tick_timeout_min_sec, tick_timeout_sec,\n    )\n''',
)

for path in [
    "scripts/wsl/run-us-am.sh",
    "scripts/wsl/run-us-afternoon.sh",
    "scripts/wsl/run-us-prep.sh",
    "scripts/wsl/run-us-close.sh",
]:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    old = 'export US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-150}"'
    new = 'export US_TICK_TIMEOUT_SEC="${US_TICK_TIMEOUT_SEC:-240}"\nexport US_TICK_TIMEOUT_MIN_SEC="${US_TICK_TIMEOUT_MIN_SEC:-240}"'
    if new not in text:
        if text.count(old) != 1:
            raise RuntimeError(f"{path}: timeout default marker mismatch")
        p.write_text(text.replace(old, new, 1), encoding="utf-8")

for path in [".github/workflows/us-trade-am.yml", ".github/workflows/us-trade-afternoon.yml"]:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    old = '      US_TICK_TIMEOUT_SEC: "150"'
    new = '      US_TICK_TIMEOUT_SEC: "240"\n      US_TICK_TIMEOUT_MIN_SEC: "240"'
    if new not in text:
        if text.count(old) != 1:
            raise RuntimeError(f"{path}: workflow timeout marker mismatch")
        p.write_text(text.replace(old, new, 1), encoding="utf-8")

replace_once(
    ".env.example",
    "US_TICK_TIMEOUT_SEC=150\n",
    "US_TICK_TIMEOUT_SEC=240\nUS_TICK_TIMEOUT_MIN_SEC=240\n",
)


# ---------------------------------------------------------------------------
# 5) KR close: execute the normal PB1 EXIT phase, not a distinct 'close' phase.
# Liquidation remains independently controlled; this does not force liquidation.
# ---------------------------------------------------------------------------
replace_once(
    "trader/kr/runner/trade_session_runner.py",
    '''        os.environ["FORCE_PB1_PHASE"] = "close"\n''',
    '''        os.environ["FORCE_PB1_PHASE"] = "exit"\n''',
)

replace_once(
    "trader/kr/runner/trade_session_runner.py",
    '''        logger.info("[KR_CLOSE][PHASE] phase=close entry_enabled=0 exit_enabled=1 close_enabled=1 close_liquidation_enabled=0")\n''',
    '''        logger.info("[KR_CLOSE][PHASE] phase=close engine_phase=exit entry_enabled=0 exit_enabled=1 close_enabled=1 close_liquidation_enabled=0")\n''',
)

p = Path("scripts/wsl/run-kr-close.sh")
text = p.read_text(encoding="utf-8")
if "FORCE_PB1_PHASE=exit" not in text:
    if "FORCE_PB1_PHASE=close" not in text:
        raise RuntimeError("run-kr-close.sh: FORCE_PB1_PHASE marker missing")
    p.write_text(text.replace("FORCE_PB1_PHASE=close", "FORCE_PB1_PHASE=exit"), encoding="utf-8")

p = Path("tests/test_kr_close_phase_forced.py")
text = p.read_text(encoding="utf-8")
text = text.replace('assert runner.os.environ["FORCE_PB1_PHASE"] == "close"', 'assert runner.os.environ["FORCE_PB1_PHASE"] == "exit"')
p.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Regression coverage for all five residual defects.
# ---------------------------------------------------------------------------
Path("tests/us/test_us_pr127_residual_integrity.py").write_text(r'''from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path


def test_stale_quote_provenance_propagates_from_data_provider():
    from trader.us.data_provider import USDataProvider

    class Client:
        def get_us_price(self, symbol, exchange):
            return {
                "output": {"last": "100", "open": "99", "high": "101", "low": "98", "tvol": "10"},
                "_quote_quality": "STALE",
                "_quote_source": "KIS_STALE_CACHE",
                "_quote_asof_epoch": 123.0,
                "_quote_age_sec": 42.0,
            }

    provider = USDataProvider(offline=False)
    provider._client = Client()
    quote = provider.get_current_price("AAPL", "NASDAQ")
    assert quote["stale"] is True
    assert quote["quality"] == "stale"
    assert quote["source"] == "KIS_STALE_CACHE"
    assert quote["age_sec"] == 42.0


def test_pb1_exit_snapshot_rejects_stale_quote_before_state_mutation():
    from trader.us.pb1.us_exit_engine import prepare_exit_position_snapshots

    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": "110", "stale": True, "quality": "stale", "source": "DB_STALE", "_stale_date": "2026-09-11"}

    snapshots = prepare_exit_position_snapshots(
        [{"symbol": "AAPL", "qty": 10, "avg_price_usd": 100, "exchange": "NASDAQ"}],
        Provider(),
        now=datetime(2026, 9, 12, tzinfo=timezone.utc),
    )
    assert snapshots == []


def test_sell_balance_snapshot_is_fetched_once_per_tick():
    from trader.us.execution.order_router import _get_broker_position
    from trader.us.execution.tick_context import TickExecutionContext

    class Client:
        def __init__(self):
            self.calls = 0
        def get_balance(self, force_refresh=False):
            self.calls += 1
            assert force_refresh is True
            return {"positions": [
                {"symbol": "AAPL", "qty": 10, "orderable_qty": 10},
                {"symbol": "MRVL", "qty": 15, "orderable_qty": 15},
            ]}

    ctx = TickExecutionContext("2026-09-11", "am", "run", 1, "tick")
    client = Client()
    assert _get_broker_position(client, "AAPL", context=ctx)["qty"] == 10
    assert _get_broker_position(client, "MRVL", context=ctx)["qty"] == 15
    assert client.calls == 1
    assert ctx.counters["sell_balance_fresh_snapshots"] == 1


def test_tqqq_unresolved_cancel_escalates_without_guessing_terminal(monkeypatch):
    from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl

    now = datetime(2026, 9, 12, 2, 0, tzinfo=timezone.utc)
    order = {
        "trade_date": "2026-09-10",
        "order_no": "123",
        "client_order_key": "TQQQ_INF_V3:cycle:2026-09-10:BUY:1",
        "symbol": "TQQQ", "side": "BUY", "status": "ACK", "qty": 3,
        "meta": {"tqqq_ttl_cancel_requested_at": (now - timedelta(hours=1)).isoformat()},
    }

    class Repo:
        def __init__(self):
            self.escalations = []
        def load_expired_open_buy_orders(self, **kwargs):
            return [order]
        def mark_ttl_unresolved_escalated(self, order, **kwargs):
            self.escalations.append(kwargs)

    repo = Repo()
    monkeypatch.setenv("US_TQQQ_TTL_UNRESOLVED_ESCALATE_SEC", "900")
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=now, ttl_seconds=120,
        cancel_order=lambda **kwargs: (_ for _ in ()).throw(AssertionError("cancel must not replay")),
        query_order=lambda **kwargs: {"order_no": "123", "symbol": "TQQQ", "side": "BUY", "status": "OPEN"},
    )
    assert result["pending"] == 1
    assert result["terminal"] == 0
    assert result["escalated"] == 1
    assert len(repo.escalations) == 1


def test_degraded_fills_return_keeps_already_routed_order_truth_in_source():
    source = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    start = source.index("# fills contract/temp error is degraded")
    end = source.index("elif not daily_notional_available", start)
    section = source[start:end]
    assert section.count('"orders": list(orders)') == 2
    assert section.count('"orders_sent": routed_sent') == 2
    assert section.count('"orders_ack": routed_ack') == 2
    assert section.count('"sell_notional_routed": sell_notional_routed') == 2


def test_timeout_contract_is_explicit_240_seconds():
    source = Path("trader/us/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert "US_TICK_TIMEOUT_MIN_SEC" in source
    assert "TICK_TIMEOUT_CONTRACT" in source
    assert "TICK_TIMEOUT_RAISED" not in source
    for path in [
        "scripts/wsl/run-us-am.sh", "scripts/wsl/run-us-afternoon.sh",
        "scripts/wsl/run-us-prep.sh", "scripts/wsl/run-us-close.sh",
    ]:
        text = Path(path).read_text(encoding="utf-8")
        assert 'US_TICK_TIMEOUT_SEC:-240' in text
        assert 'US_TICK_TIMEOUT_MIN_SEC:-240' in text
''', encoding="utf-8")

Path("tests/kr/test_kr_pr127_close_contract.py").write_text(r'''from pathlib import Path


def test_wsl_close_uses_exit_engine_phase_without_forcing_liquidation():
    text = Path("scripts/wsl/run-kr-close.sh").read_text(encoding="utf-8")
    assert "FORCE_PB1_PHASE=exit" in text
    assert 'PB1_CLOSE_LIQUIDATION_ENABLED="${PB1_CLOSE_LIQUIDATION_ENABLED:-0}"' in text


def test_kr_close_runner_uses_exit_phase_and_keeps_entry_disabled():
    text = Path("trader/kr/runner/trade_session_runner.py").read_text(encoding="utf-8")
    start = text.index('if session == "close":')
    section = text[start:start + 1800]
    assert 'os.environ["FORCE_PB1_PHASE"] = "exit"' in section
    assert 'os.environ["PB1_ENTRY_ENABLED"] = "0"' in section
    assert 'os.environ["PB1_EXIT_ENABLED"] = "1"' in section
    assert 'os.environ["PB1_CLOSE_LIQUIDATION_ENABLED"] = os.getenv("PB1_CLOSE_LIQUIDATION_ENABLED", "0")' in section
''', encoding="utf-8")

print("PR127 patch applied")
