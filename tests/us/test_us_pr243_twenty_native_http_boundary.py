"""PR243 native KIS HTTP boundary endurance; NEVER connects to real KIS.

20 independent US_STANDARD protective SELL intents pass through the actual
durable route_order and KisUSClient._post path. Only requests.post and the
account/risk/DB test fixtures are substituted. This is a controlled CI
regression, explicitly NOT 20 WSL practice live ticks.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import statistics
import time

from trader.us.execution.tick_context import TickExecutionContext
from trader.us.execution.order_router import route_order, _semantic_action_identity
from trader.us.runner.trade_tick_runner import route_exit_orders_immediately
from tests.us.test_us_route_order_claim_boundary import (
    _route_fixture, _native_kis_client, _identity, _intent,
)


def _p95_nearest_rank(samples: list[float]) -> float:
    assert samples
    return sorted(samples)[math.ceil(len(samples) * .95)-1]


def test_pr243_twenty_native_sell_http_and_restart_fence(monkeypatch, tmp_path):
    from trader.us.execution import order_router
    from trader.us.execution.kis_us_client import KisUSClient
    import requests

    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "1")
    monkeypatch.setenv("DRY_RUN", "0")
    # All broker HTTP is intercepted locally; no DNS or network I/O is made.
    _, claim_repo, _broker = _route_fixture(monkeypatch)
    monkeypatch.setattr(
        order_router, "_semantic_action_identity",
        lambda intent, *, account_env: replace(
            _identity(str(intent["trade_date"]), str(intent.get("semantic_action") or "ENTRY")),
            lifecycle_id=str(intent.get("position_lifecycle_id") or intent["symbol"]),
            action_instance=str(intent.get("client_order_key") or intent["symbol"]),
        ),
    )
    events=[]
    client = _native_kis_client(monkeypatch, events)
    client._base_url = "https://openapivts.koreainvestment.com:29443"
    # Native SELL fresh KIS balance logic stays active, but its input is a
    # fixed account-scoped mocked broker snapshot, never actual HTTP. Route
    # selection cannot silently fall back to an unavailable live account.
    broker_snapshot = {"positions": [
        {"symbol":f"SIM_PROTECT_{idx:02d}", "exchange":"NASDAQ",
         "qty":1, "holding_qty":1, "orderable_qty":1,
         "currency":"USD", "avg_price":10.0}
        for idx in range(20)
    ]}
    monkeypatch.setattr(client,"get_us_balance",lambda force_refresh=True: broker_snapshot)
    monkeypatch.setattr(
        "trader.us.data_provider.normalize_us_balance",
        lambda snapshot: snapshot,
    )
    def deny_unmocked_http(*_args,**_kwargs):
        raise AssertionError("No real KIS GET permitted in mocked-HTTP endurance")
    monkeypatch.setattr(requests,"get",deny_unmocked_http)

    def fake_http(url, headers, json, timeout):
        assert url.startswith("https://openapivts.koreainvestment.com:29443/")
        assert "/trading/order" in url
        assert timeout > 0
        # This is a stubbed HTTP response, never a real broker ACK/fill.
        events.append("INTERCEPTED_POST")
        class Response:
            def raise_for_status(self): pass
            def json(self): return {"rt_cd":"0", "order_no":f"STUB_{len(events)}"}
        return Response()

    monkeypatch.setattr(requests, "post", fake_http)
    measured=[]
    attempts=[]
    for ix in range(20):
        symbol=f"SIM_PROTECT_{ix:02d}"
        intent=_intent(
            "2026-10-20",side="SELL",
            client_order_key=f"pr243-stub-sell-{ix}",
            action=f"HARD_STOP_{ix}",
        )
        intent.update({
            "symbol":symbol,
            "position_lifecycle_id":f"sim-life-{ix}",
            "strategy_owner":"US_STANDARD",
            "reason":"HARD_STOP",
        })
        ready=time.monotonic()
        context=TickExecutionContext(
            "2026-10-20","am","pr243-20-boundaries",1,f"sim-tick-{ix}",
            deadline=time.monotonic()+30,
        )
        context._sell_selected_ready_by_symbol={symbol:ready}
        context._sell_signal_ready_by_symbol={symbol:ready}
        context._sell_selected_trace_id_by_symbol={symbol:f"signal-{ix}"}
        result=route_exit_orders_immediately(
            [intent], buy_daily_notional=0,position_count=1,
            effective_budget=20000.0,
            signal_only=False,kis_order_allowed=True,
            current_position_symbols={symbol},
            context=context,kis_client=client,
        )
        assert result["orders"][0]["status"]=="ACK", result
        routed=result["orders"][0]["intent"]
        attempt=str(routed["submit_attempt_id"])
        metrics=context.metrics["sell_signal_to_broker_boundary_by_attempt_ms"]
        assert len(metrics)==1 and attempt in metrics
        assert isinstance(metrics[attempt],float) and math.isfinite(metrics[attempt])
        assert metrics[attempt]>=0
        assert claim_repo.health()["unresolved_execution_actions"]==ix+1
        measured.append(metrics[attempt])
        attempts.append(attempt)

    # A fresh claim repo demonstrates that ACK-only remains fenced after
    # restart. An ACK is intentionally NEVER promoted to fill in this test.
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db.execution_claim_schema import us_execution_claims
    restarted=DurableExecutionClaimRepo(claim_repo.engine,us_execution_claims)
    assert restarted.health()["unresolved_execution_actions"]==20
    assert len(set(attempts))==20
    assert events.count("HTTP")==20
    assert events.count("INTERCEPTED_POST")==20
    simulated_p95=_p95_nearest_rank(measured)
    # Harness-only fast-path regression, not deployment 60-second acceptance.
    assert simulated_p95<2000, (simulated_p95,measured)
    report={
        "sample_origin":"GITHUB_HOSTED_STUBBED_HTTP_NOT_WSL_KIS",
        "eligible_protective_sell_attempts":20,
        "stubbed_http_post_count":events.count("INTERCEPTED_POST"),
        "native_boundary_samples":len(measured),
        "p50_ms":round(statistics.median(measured),3),
        "p95_ms":round(simulated_p95,3),
        "max_ms":round(max(measured),3),
        "broker_real_orders":0,
        "broker_real_fills":0,
        "restart_ack_only_fenced":20,
        "production_p95_60s_verified":False,
    }
    output_dir=Path(os.environ.get("US243_BENCH_REPORT_DIR",str(tmp_path)))
    output_dir.mkdir(parents=True,exist_ok=True)
    out=output_dir/"pr243-stubbed-native-boundary-20.json"
    out.write_text(json.dumps(report,sort_keys=True),encoding="utf-8")
    print("PR243_STUBBED_NATIVE_BOUNDARY_REPORT",json.dumps(report,sort_keys=True))
