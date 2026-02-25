# -*- coding: utf-8 -*-
"""Minervini Buyable Decision — 상세 사유 추적
종목별로 buyable 판정 이유를 명확히 기록합니다.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class BuyableDecision:
    """Buyable 판정 결과
    
    Attributes:
        ok: True이면 매수 가능
        reasons: 판정 이유 코드 리스트 (예: ["PRICE_BELOW_PIVOT", "VCP_PIVOT_TOO_FAR"])
        metrics: 판정에 사용된 메트릭 (예: pivot, last, rs_rank 등)
        metadata: 추가 정보 (선택사항)
    """
    ok: bool
    reasons: list[str] = field(default_factory=list)
    metrics: dict[str, float | str | Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    
    def add_reason(self, reason: str) -> None:
        """사유 추가 (중복 방지)"""
        if reason not in self.reasons:
            self.reasons.append(reason)
    
    def to_dict(self) -> dict:
        """dict로 변환 (JSON 직렬화용)"""
        return {
            "ok": self.ok,
            "reasons": self.reasons,
            "metrics": self.metrics,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_bool(cls, ok: bool, reason: str = "") -> BuyableDecision:
        """기존 bool 기반 코드와 호환성 유지"""
        reasons = [reason] if reason else []
        return cls(ok=ok, reasons=reasons)
