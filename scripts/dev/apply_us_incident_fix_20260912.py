from pathlib import Path
import re
import textwrap


def dedent(text: str) -> str:
    return textwrap.dedent(text).lstrip("\n")


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"expected patch anchor missing: {path}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# 1) TQQQ broker-truth-first TTL reconciliation.
p = Path("trader/us/infinite/integration.py")
text = p.read_text(encoding="utf-8")
pattern = r"def reconcile_tqqq_open_buy_ttl\(.*?\n(?=def owns_symbol)"
new_func = dedent('''
def reconcile_tqqq_open_buy_ttl(*, repository: Any, now: datetime, ttl_seconds: int,
                                cancel_order: Callable[..., dict],
                                query_order: Callable[..., dict]) -> dict[str, int]:
    """Reconcile elapsed Infinite BUYs from broker truth before attempting cancel.

    A stale DB row is not proof that a broker order is still open. Query the
    original order first, terminalize only broker-confirmed terminal evidence,
    and isolate cancel/query failures to the one stale BUY. Unresolved evidence
    remains pending/fail-closed for another BUY, but must not raise through the
    whole TQQQ sleeve.
    """
    expired = repository.load_expired_open_buy_orders(
        now=now, ttl_seconds=ttl_seconds, symbol="TQQQ"
    )
    result = {"expired": len(expired), "cancel_requested": 0, "terminal": 0, "pending": 0}

    def terminal_observation(order: dict, observation: dict | None) -> bool:
        if not isinstance(observation, dict):
            return False
        order_no = str(order.get("order_no") or "")
        observed_order_no = str(observation.get("order_no") or order_no)
        observed_symbol = str(observation.get("symbol") or "TQQQ").upper()
        observed_side = str(observation.get("side") or "BUY").upper()
        status = str(observation.get("status") or "").upper().replace("CANCELED", "CANCELLED")
        return bool(
            observed_order_no == order_no
            and observed_symbol == "TQQQ"
            and observed_side == "BUY"
            and status in _TQQQ_TTL_TERMINAL_STATUSES
        )

    for order in expired:
        order_no = str(order.get("order_no") or "")
        client_order_key = str(order.get("client_order_key") or "")
        if not order_no or not client_order_key:
            result["pending"] += 1
            continue
        identity = {
            "order_no": order_no,
            "client_order_key": client_order_key,
            "symbol": "TQQQ",
            "side": "BUY",
        }
        metadata = order.get("meta") if isinstance(order.get("meta"), dict) else {}

        try:
            observation = query_order(**identity) or {}
        except Exception as exc:
            logger.warning(
                "[TQQQ_INF][TTL_RECONCILE][QUERY_WARN] order_no=%s key=%s error=%s action=keep_pending",
                order_no, client_order_key, exc,
            )
            result["pending"] += 1
            continue

        if terminal_observation(order, observation):
            repository.apply_ttl_terminal_observation(order, observation)
            result["terminal"] += 1
            continue

        # A prior cancel request is never replayed blindly. The next tick
        # re-queries broker truth and keeps BUY fenced until terminal evidence.
        if metadata.get("tqqq_ttl_cancel_requested_at"):
            result["pending"] += 1
            continue

        try:
            cancel_result = cancel_order(**identity) or {}
        except Exception as exc:
            error_text = str(exc)
            logger.warning(
                "[TQQQ_INF][TTL_RECONCILE][CANCEL_WARN] order_no=%s key=%s error=%s action=requery_original_order",
                order_no, client_order_key, error_text,
            )
            # KIS paper frequently returns "원주문번호가 존재하지 않습니다"
            # for an already-terminal prior-day order. That message is not
            # itself fill/cancel evidence; re-query the original trade-date
            # order history and terminalize only if that query proves it.
            try:
                retry_observation = query_order(**identity) or {}
            except Exception as retry_exc:
                logger.warning(
                    "[TQQQ_INF][TTL_RECONCILE][REQUERY_WARN] order_no=%s key=%s error=%s action=keep_pending",
                    order_no, client_order_key, retry_exc,
                )
                result["pending"] += 1
                continue
            if terminal_observation(order, retry_observation):
                repository.apply_ttl_terminal_observation(order, retry_observation)
                result["terminal"] += 1
            else:
                result["pending"] += 1
            continue

        repository.mark_ttl_cancel_requested(
            order, requested_at=now, cancel_result=cancel_result
        )
        result["cancel_requested"] += 1
        # Cancel ACK != CANCELLED. Leave pending until a later broker query
        # proves the terminal state.
        result["pending"] += 1
    return result


''')
text2, count = re.subn(pattern, new_func, text, count=1, flags=re.S)
if count != 1:
    raise SystemExit("TQQQ TTL function patch anchor missing")
old_ttl_call = dedent('''
        if cancel_order is not None and query_order is not None:
            reconcile_tqqq_open_buy_ttl(
                repository=repository, now=datetime.now(timezone.utc),
                ttl_seconds=config.open_buy_ttl_seconds,
                cancel_order=cancel_order, query_order=query_order,
            )
        raw = next((p for p in positions if str(p.get("symbol") or p.get("code") or "").upper() == config.symbol), None)
''')
# dedent removed the required function indentation; restore it for this nested block.
old_ttl_call = textwrap.indent(old_ttl_call, "        ")
new_ttl_call = textwrap.indent(dedent('''
if cancel_order is not None and query_order is not None:
    try:
        ttl_result = reconcile_tqqq_open_buy_ttl(
            repository=repository, now=datetime.now(timezone.utc),
            ttl_seconds=config.open_buy_ttl_seconds,
            cancel_order=cancel_order, query_order=query_order,
        )
        logger.info("[TQQQ_INF][TTL_RECONCILE] result=%s", ttl_result)
    except Exception as ttl_exc:
        # Reconciliation failure may keep another BUY fail-closed, but it must
        # not suppress position/exit evaluation for the dedicated sleeve.
        logger.warning(
            "[TQQQ_INF][TTL_RECONCILE][WARN] error=%s action=continue_sleeve_with_pending_buy_fence",
            ttl_exc,
        )
raw = next((p for p in positions if str(p.get("symbol") or p.get("code") or "").upper() == config.symbol), None)
'''), "        ")
if old_ttl_call not in text2:
    raise SystemExit("TQQQ run_sleeve TTL call anchor missing")
text2 = text2.replace(old_ttl_call, new_ttl_call, 1)
p.write_text(text2, encoding="utf-8")

replace_once(
    "trader/us/infinite/repository.py",
    '            evidence_type="TQQQ_TTL_BROKER_REQUERY",\n',
    '            evidence_type=str(observation.get("evidence_type") or "TQQQ_TTL_BROKER_REQUERY"),\n',
)

# 2) Query stale TQQQ orders using the original date carried by the client key.
replace_once(
    "trader/us/runner/trade_tick_runner.py",
    dedent('''
    def query_order(**identity):
        detail = provider.get_fills_by_order_no(
            order_no=str(identity.get("order_no") or ""),
            symbol=symbol,
            trade_date=trade_date,
        )
        return detail or {}
'''),
    dedent('''
    def query_order(**identity):
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
'''),
)

# 3) Audit truth: missing gate evidence is NA, never fabricated False.
runner = Path("trader/us/runner/trade_tick_runner.py")
rtext = runner.read_text(encoding="utf-8")
latency_anchor = dedent('''
def calculate_latency_accounting(total_ms: float, stage_metrics: dict[str, float]) -> tuple[float, float]:
    """Account only non-nested stage timers and keep the remainder bounded."""
    total = max(0.0, float(total_ms))
    accounted = min(total, sum(max(0.0, float(stage_metrics.get(key, 0.0)))
                               for key in _ACCOUNTED_TOP_LEVEL_STAGES))
    return round(accounted, 3), round(total - accounted, 3)
''')
latency_replacement = latency_anchor + dedent('''


def _audit_gate_value(intent: dict, *keys: str):
    """Return 1/0 only for explicit evidence; missing audit fields stay NA."""
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    for source in (intent, meta):
        for key in keys:
            if key in source and source.get(key) is not None:
                return int(bool(source.get(key)))
    return "NA"
''')
if latency_anchor not in rtext:
    raise SystemExit("audit helper anchor missing")
rtext = rtext.replace(latency_anchor, latency_replacement, 1)
old_audit = dedent('''
                    int(bool(intent.get("setup_ok") or intent.get("setup_passed"))),
                    int(bool(intent.get("risk_ok") or intent.get("risk_passed"))),
                    int(bool(intent.get("sizing_ok") or intent.get("sizing_passed"))),
                    int(bool(intent.get("buyable_ok") or intent.get("buyable_passed"))),
''')
old_audit = textwrap.indent(old_audit, "                    ")
# The first line is already indented by textwrap; normalize to the exact source block.
old_audit = (
    '                    int(bool(intent.get("setup_ok") or intent.get("setup_passed"))),\n'
    '                    int(bool(intent.get("risk_ok") or intent.get("risk_passed"))),\n'
    '                    int(bool(intent.get("sizing_ok") or intent.get("sizing_passed"))),\n'
    '                    int(bool(intent.get("buyable_ok") or intent.get("buyable_passed"))),\n'
)
new_audit = (
    '                    _audit_gate_value(intent, "setup_ok", "setup_passed"),\n'
    '                    _audit_gate_value(intent, "risk_ok", "risk_passed"),\n'
    '                    _audit_gate_value(intent, "sizing_ok", "sizing_passed"),\n'
    '                    _audit_gate_value(intent, "buyable_ok", "buyable_passed"),\n'
)
if old_audit not in rtext:
    raise SystemExit("WHY_BUY audit anchor missing")
rtext = rtext.replace(old_audit, new_audit, 1)
runner.write_text(rtext, encoding="utf-8")

# 4) TP router guard: every real TP SELL is revalidated against the fresh KIS
# balance already fetched by the SELL hard guard.
router = Path("trader/us/execution/order_router.py")
otext = router.read_text(encoding="utf-8")
helper_anchor = "def route_order(\n"
helper = dedent('''
def _validate_take_profit_with_fresh_broker_position(
    intent: dict, broker_position: dict | None, *, now: Any | None = None,
) -> dict:
    """Revalidate a TAKE_PROFIT SELL against one fresh KIS balance snapshot."""
    from datetime import datetime, timezone
    from trader.us.profit_capture import authoritative_broker_avg, as_decimal, calc_return_rate

    meta = dict(intent.get("meta") or {})
    symbol = str(intent.get("symbol") or "").upper().strip()
    reason = str(intent.get("reason") or meta.get("reason") or "")
    if not reason.startswith("TAKE_PROFIT"):
        return {"ok": True, "reason": "not_take_profit"}
    if not broker_position:
        return {"ok": False, "reason": "fresh_broker_position_unavailable"}

    now_value = now if isinstance(now, datetime) else datetime.now(timezone.utc)
    if now_value.tzinfo is None:
        now_value = now_value.replace(tzinfo=timezone.utc)
    now_utc = now_value.astimezone(timezone.utc)
    broker_avg_raw = next(
        (broker_position.get(key) for key in (
            "broker_avg_price", "avg_price_usd", "avg_price", "avg_cost", "entry_price", "pchs_avg_pric"
        ) if broker_position.get(key) not in (None, "", 0, "0")),
        None,
    )
    fresh_qty = int(broker_position.get("qty") or broker_position.get("holding_qty") or 0)
    fresh_orderable = broker_position.get("orderable_qty")
    if fresh_orderable is None:
        fresh_orderable = broker_position.get("sellable_qty")
    if fresh_orderable is None:
        fresh_orderable = fresh_qty
    fresh_position = {
        **meta,
        **broker_position,
        "symbol": symbol,
        "qty": fresh_qty,
        "orderable_qty": int(fresh_orderable or 0),
        "position_lifecycle_id": (
            intent.get("position_lifecycle_id") or meta.get("position_lifecycle_id")
        ),
        "broker_avg_price": broker_avg_raw,
        "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_currency": "USD",
        "broker_avg_price_asof": now_utc.isoformat(),
        "balance_source": "kis_balance_authoritative",
        "authoritative_positions": True,
    }
    try:
        broker_avg, provenance = authoritative_broker_avg(fresh_position, now=now_utc)
        executable = as_decimal(intent.get("limit_price"), name="executable_price")
        threshold = as_decimal(meta.get("tp_threshold_fraction"), name="tp_threshold")
        actual_return = calc_return_rate(executable, broker_avg)
    except ValueError as exc:
        return {"ok": False, "reason": str(exc)}
    if actual_return <= 0:
        return {"ok": False, "reason": "non_positive_return", "return_rate": float(actual_return)}
    if actual_return < threshold:
        return {"ok": False, "reason": "threshold_not_met_after_refresh", "return_rate": float(actual_return)}

    meta.update(provenance)
    meta.update({
        "broker_avg_price": float(broker_avg),
        "return_rate_at_submit": float(actual_return),
        "take_profit_guard_source": "fresh_kis_balance",
        "take_profit_guard_refreshed": True,
    })
    intent["meta"] = meta
    intent["broker_avg_price"] = float(broker_avg)
    intent["broker_avg_price_source"] = provenance.get("broker_avg_price_source")
    intent["broker_avg_price_asof"] = provenance.get("broker_avg_price_asof")
    return {
        "ok": True,
        "reason": "ok",
        "broker_avg_price": float(broker_avg),
        "return_rate": float(actual_return),
        "provenance": provenance,
    }


''')
if helper_anchor not in otext:
    raise SystemExit("route_order helper anchor missing")
otext = otext.replace(helper_anchor, helper + helper_anchor, 1)
early_guard = dedent('''
    if side == "SELL" and str(intent.get("reason") or meta.get("reason") or "").startswith("TAKE_PROFIT"):
        from trader.us.profit_capture import authoritative_broker_avg, as_decimal, calc_return_rate
        try:
            broker_avg, _ = authoritative_broker_avg({**meta, "qty": qty, "orderable_qty": intent.get("available_qty", qty), "position_lifecycle_id": intent.get("position_lifecycle_id") or meta.get("position_lifecycle_id")})
            executable = as_decimal(intent.get("limit_price"), name="executable_price")
            threshold = as_decimal(meta.get("tp_threshold_fraction"), name="tp_threshold")
            actual_return = calc_return_rate(executable, broker_avg)
            if actual_return <= 0 or actual_return < threshold:
                raise ValueError("threshold_not_met" if actual_return > 0 else "non_positive_return")
        except ValueError as exc:
            logger.warning("[US_PROFIT_CAPTURE][PRE_SUBMIT_GUARD] symbol=%s decision=BLOCK reason=%s", symbol_upper, exc)
            return {"status": "BLOCKED", "reason": "take_profit_pre_submit_guard_failed", "guard_reason": str(exc), "broker_submit": False, "intent": intent}
''')
# restore the four-space function indentation removed by dedent
early_guard = textwrap.indent(early_guard, "    ")
if early_guard not in otext:
    raise SystemExit("old TP pre-submit guard anchor missing")
otext = otext.replace(early_guard, "", 1)
broker_anchor = (
    '    if side == "SELL":\n'
    '        broker_pos = _get_broker_position(kis_client, symbol)\n'
    '        broker_holding_qty = int(broker_pos.get("qty") or broker_pos.get("holding_qty") or 0) if broker_pos else 0\n'
    '        broker_orderable_qty = int(broker_pos.get("orderable_qty") or 0) if broker_pos else 0\n'
)
broker_replacement = broker_anchor + dedent('''
        if str(intent.get("reason") or (intent.get("meta") or {}).get("reason") or "").startswith("TAKE_PROFIT"):
            tp_guard = _validate_take_profit_with_fresh_broker_position(intent, broker_pos, now=now)
            if not tp_guard.get("ok"):
                guard_reason = str(tp_guard.get("reason") or "take_profit_guard_failed")
                if order_key:
                    mark_order_intent_blocked(order_key, reason=f"take_profit_pre_submit_guard_failed:{guard_reason}")
                logger.warning(
                    "[US_PROFIT_CAPTURE][PRE_SUBMIT_GUARD] symbol=%s decision=BLOCK reason=%s source=fresh_kis_balance",
                    symbol_upper, guard_reason,
                )
                return {
                    "status": "BLOCKED",
                    "reason": "take_profit_pre_submit_guard_failed",
                    "guard_reason": guard_reason,
                    "broker_submit": False,
                    "intent": intent,
                    "broker_position": broker_pos,
                }
            logger.info(
                "[US_PROFIT_CAPTURE][PRE_SUBMIT_GUARD] symbol=%s decision=ALLOW source=fresh_kis_balance avg=%s return_rate=%s",
                symbol_upper, tp_guard.get("broker_avg_price"), tp_guard.get("return_rate"),
            )
''')
# dedent above stripped all leading whitespace; restore nested SELL indentation.
broker_extra = textwrap.indent(dedent('''
if str(intent.get("reason") or (intent.get("meta") or {}).get("reason") or "").startswith("TAKE_PROFIT"):
    tp_guard = _validate_take_profit_with_fresh_broker_position(intent, broker_pos, now=now)
    if not tp_guard.get("ok"):
        guard_reason = str(tp_guard.get("reason") or "take_profit_guard_failed")
        if order_key:
            mark_order_intent_blocked(order_key, reason=f"take_profit_pre_submit_guard_failed:{guard_reason}")
        logger.warning(
            "[US_PROFIT_CAPTURE][PRE_SUBMIT_GUARD] symbol=%s decision=BLOCK reason=%s source=fresh_kis_balance",
            symbol_upper, guard_reason,
        )
        return {
            "status": "BLOCKED",
            "reason": "take_profit_pre_submit_guard_failed",
            "guard_reason": guard_reason,
            "broker_submit": False,
            "intent": intent,
            "broker_position": broker_pos,
        }
    logger.info(
        "[US_PROFIT_CAPTURE][PRE_SUBMIT_GUARD] symbol=%s decision=ALLOW source=fresh_kis_balance avg=%s return_rate=%s",
        symbol_upper, tp_guard.get("broker_avg_price"), tp_guard.get("return_rate"),
    )
'''), "        ")
broker_replacement = broker_anchor + broker_extra
if broker_anchor not in otext:
    raise SystemExit("SELL broker hard-guard anchor missing")
otext = otext.replace(broker_anchor, broker_replacement, 1)
router.write_text(otext, encoding="utf-8")

# Regression tests for the live 9/10 and 9/11 incidents.
test = dedent(r'''
from datetime import datetime, timezone

import pytest

from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl
from trader.us.runner.trade_tick_runner import _audit_gate_value, _build_tqqq_ttl_callbacks
from trader.us.execution.order_router import _validate_take_profit_with_fresh_broker_position


NOW = datetime(2026, 9, 11, 15, 0, tzinfo=timezone.utc)


class Repo:
    def __init__(self, order):
        self.order = order
        self.cancelled = []
        self.terminal = []

    def load_expired_open_buy_orders(self, **_kwargs):
        return [self.order]

    def mark_ttl_cancel_requested(self, order, **kwargs):
        self.cancelled.append((order, kwargs))
        order.setdefault("meta", {})["tqqq_ttl_cancel_requested_at"] = kwargs["requested_at"].isoformat()

    def apply_ttl_terminal_observation(self, order, observation):
        self.terminal.append((order, observation))
        order["status"] = observation["status"]
        return {"status": "OK"}


def stale_order():
    return {
        "trade_date": "2026-09-10",
        "client_order_key": "TQQQ_INF_V3:cycle-live:2026-09-10:BUY",
        "order_no": "old-18",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": 3,
        "status": "OPEN",
        "meta": {},
    }


def test_tqqq_terminal_broker_truth_is_applied_before_cancel():
    repo = Repo(stale_order())
    cancel_calls = []
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **kw: cancel_calls.append(kw) or {"status": "ACK"},
        query_order=lambda **_kw: {
            "order_no": "old-18", "symbol": "TQQQ", "side": "BUY",
            "status": "FILLED", "filled_qty": 3,
        },
    )
    assert result["terminal"] == 1
    assert cancel_calls == []
    assert repo.order["status"] == "FILLED"


def test_tqqq_original_order_not_found_isolated_and_requeried():
    repo = Repo(stale_order())
    queries = iter([
        {"status": "OPEN", "order_no": "old-18", "symbol": "TQQQ", "side": "BUY"},
        {"status": "EXPIRED", "order_no": "old-18", "symbol": "TQQQ", "side": "BUY", "filled_qty": 0},
    ])

    def cancel(**_kw):
        raise RuntimeError("모의투자 원주문번호가 존재하지 않습니다")

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=cancel, query_order=lambda **_kw: next(queries),
    )
    assert result["terminal"] == 1
    assert repo.order["status"] == "EXPIRED"


def test_tqqq_unresolved_cancel_error_keeps_pending_without_exception():
    repo = Repo(stale_order())

    def cancel(**_kw):
        raise RuntimeError("모의투자 원주문번호가 존재하지 않습니다")

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=cancel, query_order=lambda **_kw: {},
    )
    assert result["pending"] == 1
    assert result["terminal"] == 0
    assert repo.order["status"] == "OPEN"


def test_tqqq_callback_uses_original_trade_date_from_client_key():
    seen = []

    class Provider:
        def get_fills_by_order_no(self, **kwargs):
            seen.append(kwargs)
            return {}

    class Client:
        def cancel_us_order(self, **_kwargs):
            return {"status": "ACK"}

    _cancel, query = _build_tqqq_ttl_callbacks(
        Provider(), Client(), trade_date="2026-09-11", symbol="TQQQ"
    )
    query(
        order_no="old-18",
        client_order_key="TQQQ_INF_V3:cycle-live:2026-09-10:BUY",
        symbol="TQQQ", side="BUY",
    )
    assert seen[0]["trade_date"] == "2026-09-10"


@pytest.mark.parametrize(
    "symbol,avg_price,executable,expected_min",
    [
        ("AAPL", 318.93, 332.4501, 0.04),
        ("MRVL", 230.49, 237.665, 0.03),
    ],
)
def test_take_profit_refresh_uses_fresh_kis_avg_and_allows_valid_tp(symbol, avg_price, executable, expected_min):
    intent = {
        "symbol": symbol,
        "side": "SELL",
        "qty": 2,
        "limit_price": executable,
        "reason": "TAKE_PROFIT_TP1",
        "position_lifecycle_id": f"lc-{symbol}",
        "meta": {
            "reason": "TAKE_PROFIT_TP1",
            "profit_capture_stage": "tp1",
            "tp_threshold_fraction": 0.03,
            "position_lifecycle_id": f"lc-{symbol}",
            "broker_avg_price": avg_price,
            "broker_avg_price_source": "fallback_avg_cost",
            "broker_avg_price_currency": "USD",
            "broker_avg_price_asof": "2026-09-10T14:00:00+00:00",
        },
    }
    broker = {
        "symbol": symbol,
        "qty": 10,
        "orderable_qty": 10,
        "avg_price_usd": avg_price,
        "balance_source": "kis_balance_authoritative",
    }
    result = _validate_take_profit_with_fresh_broker_position(intent, broker, now=NOW)
    assert result["ok"] is True
    assert result["return_rate"] >= expected_min
    assert intent["meta"]["take_profit_guard_source"] == "fresh_kis_balance"
    assert intent["meta"]["broker_avg_price_source"] == "kis_pchs_avg_pric"


def test_take_profit_refresh_reblocks_when_threshold_no_longer_met():
    intent = {
        "symbol": "MRVL", "side": "SELL", "qty": 3,
        "limit_price": 237.665, "reason": "TAKE_PROFIT_TP1",
        "position_lifecycle_id": "lc-mrvl",
        "meta": {"reason": "TAKE_PROFIT_TP1", "tp_threshold_fraction": 0.03, "position_lifecycle_id": "lc-mrvl"},
    }
    broker = {"symbol": "MRVL", "qty": 15, "orderable_qty": 15, "avg_price_usd": 236.50}
    result = _validate_take_profit_with_fresh_broker_position(intent, broker, now=NOW)
    assert result["ok"] is False
    assert result["reason"] == "threshold_not_met_after_refresh"


def test_take_profit_refresh_fails_closed_without_broker_position():
    intent = {
        "symbol": "AAPL", "side": "SELL", "qty": 2,
        "limit_price": 332.4501, "reason": "TAKE_PROFIT_TP1",
        "position_lifecycle_id": "lc-aapl",
        "meta": {"reason": "TAKE_PROFIT_TP1", "tp_threshold_fraction": 0.03, "position_lifecycle_id": "lc-aapl"},
    }
    result = _validate_take_profit_with_fresh_broker_position(intent, {}, now=NOW)
    assert result == {"ok": False, "reason": "fresh_broker_position_unavailable"}


def test_buy_audit_missing_is_na_not_false_zero():
    assert _audit_gate_value({}, "setup_ok", "setup_passed") == "NA"
    assert _audit_gate_value({"risk_ok": False}, "risk_ok", "risk_passed") == 0
    assert _audit_gate_value({"meta": {"sizing_ok": True}}, "sizing_ok", "sizing_passed") == 1
''')
Path("tests/us/test_us_20260910_20260911_incident_fixes.py").write_text(test, encoding="utf-8")

# Temporary bootstrap artifacts must not remain in the PR.
Path(".github/workflows/pr-us-20260910-11-bootstrap.yml").unlink(missing_ok=True)
Path("scripts/dev/apply_us_incident_fix_20260912.py").unlink(missing_ok=True)
