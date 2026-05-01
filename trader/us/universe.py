# -*- coding: utf-8 -*-
"""미국주식 Universe 관리.

config/us_universe.yaml에서 ticker 목록을 로드하고
symbols.py registry에 등록한다.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_LOADED: bool = False
_UNIVERSE: dict[str, list[str]] = {}
_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "us_universe.yaml"


def _load_yaml() -> dict[str, list[str]]:
    try:
        import yaml  # type: ignore
        with open(_CONFIG_PATH, "r") as f:
            data = yaml.safe_load(f) or {}
        return {k: [str(v) for v in vs] for k, vs in data.items() if isinstance(vs, list)}
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

    # symbols 레지스트리에 등록
    from trader.us.symbols import register_symbol, US_EXCHANGE_REGISTRY, _SYMBOL_EXCHANGE_MAP

    # us_universe.yaml에는 exchange 정보 없음 → symbols.py의 known map 우선
    # 미등록 symbol은 NASDAQ 기본으로 등록
    for category, tickers in raw.items():
        for ticker in tickers:
            try:
                from trader.us.symbols import is_known_symbol
                if not is_known_symbol(ticker):
                    register_symbol(ticker, "NASDAQ")  # 기본값; 개선 가능
            except Exception as exc:
                logger.warning("[US_UNIVERSE][SKIP] ticker=%s error=%s", ticker, exc)

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
