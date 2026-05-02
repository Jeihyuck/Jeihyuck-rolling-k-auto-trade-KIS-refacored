# -*- coding: utf-8 -*-
"""Tests for Multi-Agent classes."""
import os
import pytest


# ---------------------------------------------------------------------------
# ExecutionAgent
# ---------------------------------------------------------------------------
class TestExecutionAgent:
    def test_execute_empty_intents(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "1")
        monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
        monkeypatch.setenv("ALLOW_REAL_ORDER", "0")
        from trader.us.agents.execution_agent import ExecutionAgent
        agent = ExecutionAgent()
        results = agent.execute_intents([])
        assert results == []

    def test_execute_intent_dry_run(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "1")
        monkeypatch.setenv("US_AGENT_ENABLED", "1")
        monkeypatch.setenv("TRADING_REGION", "US")
        monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
        monkeypatch.setenv("ALLOW_REAL_ORDER", "0")
        monkeypatch.setenv("US_MAX_ORDER_USD", "5000")
        monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "20000")
        monkeypatch.setenv("US_MAX_POSITION_COUNT", "10")
        monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.5")
        monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "100")
        from trader.us.agents.execution_agent import ExecutionAgent
        from trader.us.execution.order_router import clear_sent_order_keys
        clear_sent_order_keys()
        agent = ExecutionAgent()
        intent = {
            "symbol": "AAPL",
            "exchange": "NASD",
            "side": "BUY",
            "qty": 1,
            "notional_usd": 200.0,
            "client_order_key": "test-exec-agent-001",
        }
        results = agent.execute_intents([intent], available_cash_usd=5000.0)
        assert len(results) == 1
        assert results[0]["status"] == "DRY_RUN"


# ---------------------------------------------------------------------------
# RiskAgent
# ---------------------------------------------------------------------------
class TestRiskAgent:
    def _set_env(self, monkeypatch):
        monkeypatch.setenv("US_AGENT_ENABLED", "1")
        monkeypatch.setenv("TRADING_REGION", "US")
        monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
        monkeypatch.setenv("ALLOW_REAL_ORDER", "0")
        monkeypatch.setenv("US_MAX_ORDER_USD", "5000")
        monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "20000")
        monkeypatch.setenv("US_MAX_POSITION_COUNT", "10")
        monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.5")
        monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "100")

    def test_check_pass(self, monkeypatch):
        self._set_env(monkeypatch)
        from trader.us.agents.risk_agent import RiskAgent
        agent = RiskAgent()
        intent = {
            "symbol": "AAPL",
            "exchange": "NASD",
            "side": "BUY",
            "qty": 1,
            "notional_usd": 200.0,
            "client_order_key": "risk-agent-001",
        }
        result = agent.check(intent, total_portfolio_usd=5000.0, available_cash_usd=4000.0)
        assert result["status"] == "PASS"

    def test_check_block_cash(self, monkeypatch):
        self._set_env(monkeypatch)
        monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "9999")
        from trader.us.agents.risk_agent import RiskAgent
        agent = RiskAgent()
        intent = {
            "symbol": "AAPL",
            "exchange": "NASD",
            "side": "BUY",
            "qty": 1,
            "notional_usd": 200.0,
            "client_order_key": "risk-agent-002",
        }
        result = agent.check(intent, total_portfolio_usd=5000.0, available_cash_usd=100.0)
        assert result["status"] == "BLOCK"

    def test_check_all(self, monkeypatch):
        self._set_env(monkeypatch)
        from trader.us.agents.risk_agent import RiskAgent
        agent = RiskAgent()
        intents = [
            {
                "symbol": "AAPL",
                "exchange": "NASD",
                "side": "BUY",
                "qty": 1,
                "notional_usd": 100.0,
                "client_order_key": f"check-all-{i}",
            }
            for i in range(3)
        ]
        results = agent.check_all(intents, total_portfolio_usd=5000.0, available_cash_usd=4000.0)
        assert len(results) == 3


# ---------------------------------------------------------------------------
# FailureTriageAgent
# ---------------------------------------------------------------------------
class TestFailureTriageAgent:
    def test_triage_rate_limit(self):
        from trader.us.agents.failure_triage_agent import FailureTriageAgent
        agent = FailureTriageAgent()
        result = agent.triage("rate limit exceeded ErrCd:EGW00201")
        assert result["label"] == "RATE_LIMIT"
        assert "raw_type" in result

    def test_triage_unknown(self):
        from trader.us.agents.failure_triage_agent import FailureTriageAgent
        agent = FailureTriageAgent()
        result = agent.triage("some random text with no known pattern")
        assert result["label"] == "UNKNOWN"

    def test_triage_bulk(self):
        from trader.us.agents.failure_triage_agent import FailureTriageAgent
        agent = FailureTriageAgent()
        logs = ["rate limit exceeded ErrCd:EGW00201", "401 unauthorized token expired", "unknown error xyz"]
        results = agent.triage_bulk(logs)
        assert len(results) == 3
        assert results[0]["label"] == "RATE_LIMIT"
        assert results[1]["label"] == "AUTH_FAIL"


# ---------------------------------------------------------------------------
# PatchPlannerAgent
# ---------------------------------------------------------------------------
class TestPatchPlannerAgent:
    def test_plan_rate_limit(self):
        from trader.us.agents.patch_planner_agent import PatchPlannerAgent
        agent = PatchPlannerAgent()
        plan = agent.plan({"label": "RATE_LIMIT"})
        assert plan["failure_type"] == "RATE_LIMIT"
        assert plan["safe_to_apply"] is True
        assert "patch_id" in plan
        assert len(plan["files_to_change"]) > 0

    def test_plan_auth_fail_unsafe(self):
        from trader.us.agents.patch_planner_agent import PatchPlannerAgent
        agent = PatchPlannerAgent()
        plan = agent.plan({"label": "AUTH_FAIL"})
        assert plan["safe_to_apply"] is False

    def test_plan_unknown(self):
        from trader.us.agents.patch_planner_agent import PatchPlannerAgent
        agent = PatchPlannerAgent()
        plan = agent.plan({"label": "UNKNOWN"})
        assert plan["failure_type"] == "UNKNOWN"

    def test_plan_bulk(self):
        from trader.us.agents.patch_planner_agent import PatchPlannerAgent
        agent = PatchPlannerAgent()
        results = agent.plan_bulk([
            {"label": "RATE_LIMIT"},
            {"label": "UNKNOWN"},
        ])
        assert len(results) == 2


# ---------------------------------------------------------------------------
# ReportAgent
# ---------------------------------------------------------------------------
class TestReportAgent:
    def test_empty_report(self):
        from trader.us.agents.report_agent import ReportAgent
        agent = ReportAgent()
        report = agent.generate()
        assert "report_date" in report
        assert report["total_intents"] == 0
        assert len(report["next_actions"]) > 0

    def test_report_with_data(self):
        from trader.us.agents.report_agent import ReportAgent
        agent = ReportAgent()
        intents = [{"symbol": "AAPL"}]
        exec_results = [{"status": "DRY_RUN"}]
        risk_results = [{"status": "PASS"}]
        report = agent.generate(
            intents=intents,
            execution_results=exec_results,
            risk_results=risk_results,
        )
        assert report["total_intents"] == 1
        assert report["risk_pass"] == 1
        assert report["exec_ack"] == 1

    def test_report_with_failures(self):
        from trader.us.agents.report_agent import ReportAgent
        agent = ReportAgent()
        risk_results = [{"status": "BLOCK"}]
        report = agent.generate(risk_results=risk_results)
        assert report["risk_block"] == 1
        assert any("RISK" in a for a in report["next_actions"])


# ---------------------------------------------------------------------------
# ArchitectAgent (already created, smoke test)
# ---------------------------------------------------------------------------
class TestArchitectAgentSmoke:
    def test_import(self):
        from trader.us.agents.architect_agent import ArchitectAgent
        agent = ArchitectAgent()
        assert agent.name == "architect_agent"

    def test_inspect_no_violations(self):
        from trader.us.agents.architect_agent import ArchitectAgent
        agent = ArchitectAgent()
        result = agent.inspect()
        assert "status" in result
        assert "us_files_checked" in result


# ---------------------------------------------------------------------------
# StrategyAgent (already created, smoke test)
# ---------------------------------------------------------------------------
class TestStrategyAgentSmoke:
    def test_import(self):
        from trader.us.agents.strategy_agent import StrategyAgent
        agent = StrategyAgent()
        assert agent.name == "strategy_agent"

    def test_run_offline(self):
        from trader.us.agents.strategy_agent import StrategyAgent
        agent = StrategyAgent()
        result = agent.run(offline=True)
        assert "status" in result
        assert "intents" in result
        assert isinstance(result["intents"], list)
