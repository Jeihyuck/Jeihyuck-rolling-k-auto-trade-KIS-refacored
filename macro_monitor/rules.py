from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class MarketSnapshot:
    us10y: float
    us10y_prev: float | None
    brent: float
    brent_change_pct: float | None
    sp500: float
    sp500_change_pct: float | None
    sp500_drawdown_pct: float
    nasdaq: float
    vix: float
    us10y_below_470_days: int = 0

    @property
    def us10y_change_bp(self) -> float | None:
        if self.us10y_prev is None:
            return None
        return (self.us10y - self.us10y_prev) * 100.0


@dataclass(frozen=True)
class Decision:
    regime: str
    severity: int
    signals: tuple[str, ...]
    action: str
    reasons: tuple[str, ...] = field(default_factory=tuple)


REGIME_ORDER = {
    "BUY_STRONG": 1,
    "BUY_1": 1,
    "BUY_READY": 1,
    "HOLD": 1,
    "CAUTION": 2,
    "RISK_OFF": 3,
    "RED": 4,
}


def evaluate(s: MarketSnapshot) -> Decision:
    signals: list[str] = []
    reasons: list[str] = []

    if s.us10y_change_bp is not None and s.us10y_change_bp >= 15:
        signals.append("RATE_SHOCK")
        reasons.append(f"미국 10년물이 전일 대비 {s.us10y_change_bp:.1f}bp 급등")
    if s.brent >= 110 or (s.brent_change_pct is not None and s.brent_change_pct >= 5):
        signals.append("OIL_SHOCK")
        reasons.append(f"Brent ${s.brent:.2f}, 공급충격/인플레 위험")
    if (
        s.sp500_change_pct is not None
        and s.sp500_change_pct < 0
        and s.us10y_change_bp is not None
        and s.us10y_change_bp >= 5
        and s.brent_change_pct is not None
        and s.brent_change_pct >= 2
    ):
        signals.append("STAGFLATION_TAPE")
        reasons.append("주가 하락과 장기금리·유가 상승이 동시에 발생")
    if s.sp500_drawdown_pct <= -15:
        signals.append("DRAWDOWN_15")
    elif s.sp500_drawdown_pct <= -10:
        signals.append("DRAWDOWN_10")
    elif s.sp500_drawdown_pct <= -8:
        signals.append("DRAWDOWN_8")

    # Risk has priority over apparent cheapness. A -10% equity drawdown while
    # yields/oil are still breaking higher is not treated as a buy signal.
    if s.us10y >= 5.15:
        return Decision(
            regime="RED",
            severity=4,
            signals=tuple(signals + ["US10Y_GT_515"]),
            action="357870 파킹 유지. S&P500·Nasdaq·반도체 신규매수 중단.",
            reasons=tuple(reasons + [f"미국 10년물 {s.us10y:.3f}% >= 5.15%"]),
        )

    if s.us10y >= 5.00 or s.brent >= 110:
        return Decision(
            regime="RISK_OFF",
            severity=3,
            signals=tuple(signals + (["US10Y_GT_500"] if s.us10y >= 5 else []) + (["BRENT_GT_110"] if s.brent >= 110 else [])),
            action="357870 유지. 신규 주식매수 보류. 금리/유가 하락 전환 확인.",
            reasons=tuple(reasons + [f"US10Y {s.us10y:.3f}%, Brent ${s.brent:.2f}"]),
        )

    if s.sp500_drawdown_pct <= -10 and s.us10y <= 4.70 and s.brent < 105:
        return Decision(
            regime="BUY_STRONG",
            severity=1,
            signals=tuple(signals + ["PRICE_AND_YIELD_BUY"]),
            action="379800(KODEX 미국S&P500) 1,000~1,500만원 1차 매수 검토. Nasdaq은 다음 단계.",
            reasons=tuple(reasons + [f"S&P500 고점 대비 {s.sp500_drawdown_pct:.1f}% + US10Y {s.us10y:.3f}%"]),
        )

    if s.sp500_drawdown_pct <= -10 and s.us10y <= 4.90 and s.brent < 110:
        return Decision(
            regime="BUY_1",
            severity=1,
            signals=tuple(signals + ["DRAWDOWN_BUY"]),
            action="379800(KODEX 미국S&P500) 약 1,000만원 1차 분할매수 검토.",
            reasons=tuple(reasons + [f"S&P500 {s.sp500_drawdown_pct:.1f}% 조정, US10Y는 {s.us10y:.3f}%"]),
        )

    if s.us10y <= 4.70 and s.us10y_below_470_days >= 2 and s.brent < 100:
        return Decision(
            regime="BUY_READY",
            severity=1,
            signals=tuple(signals + ["YIELD_NORMALIZED"]),
            action="장기금리 정상화 확인. 379800 S&P500 1,000~1,500만원 단계적 진입 검토.",
            reasons=tuple(reasons + [f"US10Y <=4.70%가 {s.us10y_below_470_days}거래일 지속, Brent ${s.brent:.2f}"]),
        )

    if s.us10y >= 4.85 or s.brent >= 100:
        return Decision(
            regime="CAUTION",
            severity=2,
            signals=tuple(signals + ["HIGH_DISCOUNT_RATE"]),
            action="357870 파킹 유지. 예정일 매수 금지; 10년물/유가 정상화 또는 충분한 조정을 기다림.",
            reasons=tuple(reasons + [f"US10Y {s.us10y:.3f}%, Brent ${s.brent:.2f}"]),
        )

    return Decision(
        regime="HOLD",
        severity=1,
        signals=tuple(signals),
        action="357870 중심 대기. S&P500은 조건부 소액 분할만 검토.",
        reasons=tuple(reasons + [f"US10Y {s.us10y:.3f}%, Brent ${s.brent:.2f}"]),
    )
