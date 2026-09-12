"""Macro market monitoring agent.

This package is intentionally read-only with respect to trading.  It collects
market data, evaluates deterministic macro rules, and produces alerts/reports.
"""

from .rules import Decision, MarketSnapshot, evaluate

__all__ = ["Decision", "MarketSnapshot", "evaluate"]
