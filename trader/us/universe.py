# -*- coding: utf-8 -*-
"""미국주식 Universe 관리.

config/us_universe.yaml에서 ticker 목록을 로드하고
symbols.py registry에 등록한다.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_LOADED: bool = False
_UNIVERSE: dict[str, list[str]] = {}
_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "us_universe.yaml"
INVALID_SYMBOL_LITERALS = {"TRUE", "FALSE", "NONE", "NULL", "YES", "NO", "OFF"}
_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")

def validate_symbol(symbol: Any, *, path: str | Path = _CONFIG_PATH) -> str:
    if not isinstance(symbol, str):
        raise ValueError(f"[US_UNIVERSE_CONFIG][INVALID_SYMBOL] value={symbol!r} type={type(symbol).__name__} path={path}")
    normalized = symbol.strip().upper()
    if normalized in INVALID_SYMBOL_LITERALS or not _SYMBOL_RE.fullmatch(normalized):
        raise ValueError(f"[US_UNIVERSE_CONFIG][INVALID_SYMBOL] value={symbol!r} type=str path={path}")
    return normalized


def _load_yaml() -> dict[str, list[str]]:
    try:
        import yaml  # type: ignore
        with open(_CONFIG_PATH, "r") as f:
            data = yaml.safe_load(f) or {}
        return {k: [validate_symbol(v) for v in vs] for k, vs in data.items() if isinstance(vs, list)}
    except FileNotFoundError:
        logger.warning("[US_UNIVERSE][WARN] config not found: %s", _CONFIG_PATH)
        return {}
    except Exception as exc:
        logger.error("[US_UNIVERSE][ERROR] load failed: %s", exc)
        return {}


def load_universe(force: bool = False) -> dict[str, list[str]]:
    """universe yaml을 로드하고 symbols registry에 등록한다.

    Returns:
        {category: [ticker, ...]} 구조
    """
    global _LOADED, _UNIVERSE
    if _LOADED and not force:
        return _UNIVERSE

    raw = _load_yaml()
    _UNIVERSE = raw

    # YAML에는 거래소 근거가 없으므로 여기서 미등록 종목을 추정하지 않는다.
    # 정적 master 또는 Prep의 원천 메타데이터 준비 단계에서만 등록한다.

    _LOADED = True
    logger.info("[US_UNIVERSE][OK] loaded %d categories %d tickers",
                len(raw), sum(len(v) for v in raw.values()))
    return _UNIVERSE


def get_all_tickers() -> list[str]:
    """전체 ticker 목록 반환."""
    universe = load_universe()
    seen: set[str] = set()
    result: list[str] = []
    for tickers in universe.values():
        for t in tickers:
            if t not in seen:
                seen.add(t)
                result.append(t)
    return result


def get_tickers_by_category(category: str) -> list[str]:
    """특정 카테고리의 ticker 목록."""
    universe = load_universe()
    return list(universe.get(category, []))


def get_categories() -> list[str]:
    """카테고리 목록."""
    universe = load_universe()
    return list(universe.keys())


def is_in_universe(ticker: str) -> bool:
    """해당 ticker가 universe에 있는지 확인."""
    return ticker.upper() in {t.upper() for t in get_all_tickers()}
