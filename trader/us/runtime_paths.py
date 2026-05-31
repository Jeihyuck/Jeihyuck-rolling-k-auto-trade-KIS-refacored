# -*- coding: utf-8 -*-
"""US Runtime Paths — 미국장 전용 저장 경로 표준화.

한국장 runtime/watchlist, signals, bot_state 경로 사용 금지.
미국장은 반드시 아래 경로만 사용한다.
"""
from __future__ import annotations

from pathlib import Path


# ──────────────────────────────────────────────────────────────────────────────
# Root 기준 (프로젝트 루트 = trader/us/runtime_paths.py 의 3단계 상위)
# ──────────────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _root(*parts: str) -> Path:
    return _PROJECT_ROOT.joinpath(*parts)


# ──────────────────────────────────────────────────────────────────────────────
# runtime/us 경로
# ──────────────────────────────────────────────────────────────────────────────

def us_runtime_watchlist_dir(trade_date: str) -> Path:
    return _root("runtime", "us", "watchlist", trade_date)


def us_dynamic_universe_path(trade_date: str) -> Path:
    return us_runtime_watchlist_dir(trade_date) / "dynamic_universe.json"


def us_candidate_pool_path(trade_date: str) -> Path:
    return us_runtime_watchlist_dir(trade_date) / "candidate_pool.json"


def us_top50_scored_path(trade_date: str) -> Path:
    return us_runtime_watchlist_dir(trade_date) / "top50_scored.json"


def us_final30_scored_path(trade_date: str) -> Path:
    return us_runtime_watchlist_dir(trade_date) / "final30_scored.json"


def us_prep_contract_path(trade_date: str) -> Path:
    return us_runtime_watchlist_dir(trade_date) / "prep_contract.json"


# ──────────────────────────────────────────────────────────────────────────────
# signals/us 경로 (latest symlink용)
# ──────────────────────────────────────────────────────────────────────────────

def us_signals_dir() -> Path:
    return _root("signals", "us")


def us_signals_dynamic_universe_path() -> Path:
    return us_signals_dir() / "dynamic_universe.json"


def us_signals_candidate_pool_path() -> Path:
    return us_signals_dir() / "candidate_pool.json"


def us_signals_top50_scored_path() -> Path:
    return us_signals_dir() / "top50_scored.json"


def us_signals_final30_scored_path() -> Path:
    return us_signals_dir() / "final30_scored.json"


def us_signals_prep_contract_path() -> Path:
    return us_signals_dir() / "prep_contract.json"


def us_signals_latest_dynamic_universe_path() -> Path:
    return us_signals_dir() / "latest_dynamic_universe.json"


def us_signals_latest_candidate_pool_path() -> Path:
    return us_signals_dir() / "latest_candidate_pool.json"


def us_signals_latest_top50_scored_path() -> Path:
    return us_signals_dir() / "latest_top50_scored.json"


def us_signals_latest_final30_scored_path() -> Path:
    return us_signals_dir() / "latest_final30_scored.json"


def us_signals_latest_prep_contract_path() -> Path:
    return us_signals_dir() / "latest_prep_contract.json"


# ──────────────────────────────────────────────────────────────────────────────
# bot_state/us 경로
# ──────────────────────────────────────────────────────────────────────────────

def us_bot_state_final30_path(env: str, trade_date: str) -> Path:
    return _root("bot_state", "us", "trader_ledger", "final30", env, trade_date, "final30_scored.json")


def us_bot_state_candidate_pool_path(env: str, trade_date: str) -> Path:
    return _root("bot_state", "us", "trader_ledger", "candidate_pool", env, trade_date, "candidate_pool.json")


# ──────────────────────────────────────────────────────────────────────────────
# reports/us_prep 경로
# ──────────────────────────────────────────────────────────────────────────────

def us_prep_report_dir(trade_date: str) -> Path:
    return _root("reports", "us_prep", trade_date)


def us_prep_summary_json_path(trade_date: str) -> Path:
    return us_prep_report_dir(trade_date) / "us_prep_summary.json"


def us_prep_summary_md_path(trade_date: str) -> Path:
    return us_prep_report_dir(trade_date) / "us_prep_summary.md"


def us_prep_latest_summary_json_path() -> Path:
    return _root("reports", "us_prep", "latest_us_prep_summary.json")


def us_prep_latest_summary_md_path() -> Path:
    return _root("reports", "us_prep", "latest_us_prep_summary.md")


# ──────────────────────────────────────────────────────────────────────────────
# 전체 경로 dict (prep_contract에서 사용)
# ──────────────────────────────────────────────────────────────────────────────

def get_us_prep_paths(trade_date: str) -> dict[str, str]:
    return {
        "dynamic_universe": str(us_dynamic_universe_path(trade_date)),
        "candidate_pool": str(us_candidate_pool_path(trade_date)),
        "top50_scored": str(us_top50_scored_path(trade_date)),
        "final30_scored": str(us_final30_scored_path(trade_date)),
        "prep_contract": str(us_prep_contract_path(trade_date)),
        "signals_final30_scored": str(us_signals_final30_scored_path()),
        "latest_final30_scored": str(us_signals_latest_final30_scored_path()),
        "latest_prep_contract": str(us_signals_latest_prep_contract_path()),
    }
