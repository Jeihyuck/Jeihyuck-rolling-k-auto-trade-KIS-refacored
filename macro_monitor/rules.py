from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class Decision:
    risk_level: str
    signal: str
    action: str
    reasons: list[str]
    subject: str
    should_email: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fmt(value: float | None, digits: int = 2, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}{suffix}"


def evaluate(snapshot: dict[str, Any], previous: dict[str, Any] | None = None, *, daily_digest: bool = False, force_email: bool = False) -> Decision:
    previous = previous or {}
    us10 = snapshot.get("us10y_live")
    brent = snapshot.get("brent")
    drawdown = snapshot.get("sp500_drawdown_pct")
    delta_bp = snapshot.get("us10y_change_bp")
    errors = snapshot.get("errors") or []
    reasons: list[str] = []

    if us10 is None:
        risk = "RED"
        signal = "DATA_DEGRADED"
        action = "10년물 실시간 데이터가 없어 신규 주식 매수를 보류하고 357870 파킹을 유지. 데이터 복구 후 재평가."
        reasons.append("US10Y live data unavailable")
    else:
        if us10 >= 5.15 or (brent is not None and brent >= 115):
            risk = "RED"
        elif us10 >= 5.00 or (brent is not None and brent >= 105):
            risk = "ORANGE"
        elif us10 >= 4.85 or (brent is not None and brent >= 100):
            risk = "YELLOW"
        else:
            risk = "GREEN"

        if drawdown is not None and drawdown <= -10 and us10 <= 4.70 and (brent is None or brent < 100):
            signal = "BUY_STRONG"
            action = "1차 강한 매수 검토: 357870 일부 매도 후 KODEX 미국S&P500(379800) 1,500만원 + KODEX 미국나스닥100(379810) 500만원 검토. 자동주문 금지."
            reasons.append("S&P500 drawdown <= -10% while US10Y <= 4.70%")
        elif drawdown is not None and drawdown <= -10 and us10 <= 4.90:
            signal = "BUY_DIP"
            action = "가격조정 1차 매수 검토: KODEX 미국S&P500(379800) 1,000만원 검토. 나스닥/반도체는 금리 추가 안정 확인 전 보류. 자동주문 금지."
            reasons.append("S&P500 drawdown <= -10% and US10Y <= 4.90%")
        elif us10 <= 4.70 and (brent is None or brent < 100):
            signal = "BUY_READY"
            action = "금리 정상화 신호: KODEX 미국S&P500(379800) 1,000만~1,500만원 1차 매수 검토. 나머지는 357870 유지. 자동주문 금지."
            reasons.append("US10Y <= 4.70% and Brent < $100")
        elif us10 >= 5.00 or (brent is not None and brent >= 105):
            signal = "RISK_OFF"
            action = "신규 주식 ETF 매수 중단. TIGER CD금리투자KIS(357870) 파킹 유지. 379800/379810/381180 신규매수 보류."
            reasons.append("US10Y >= 5.00% or Brent >= $105")
        else:
            signal = "HOLD_CD"
            action = "357870 파킹 유지. 금리 4.70% 이하 안정 또는 S&P500 -10% 조정 + 금리안정을 기다림."
            reasons.append("No risk-off or buy trigger confirmed")

    if us10 is not None:
        reasons.append(f"US10Y={us10:.3f}%")
    if delta_bp is not None:
        reasons.append(f"US10Y daily change={delta_bp:+.1f}bp")
    if brent is not None:
        reasons.append(f"Brent=${brent:.2f}")
    if drawdown is not None:
        reasons.append(f"S&P500 drawdown={drawdown:.2f}%")
    if errors:
        reasons.append(f"data warnings={len(errors)}")

    prior_decision = previous.get("decision") or previous
    prior_signal = prior_decision.get("signal")
    prior_risk = prior_decision.get("risk_level")
    state_changed = signal != prior_signal or risk != prior_risk
    rate_shock = delta_bp is not None and abs(delta_bp) >= 15.0
    critical = signal in {"RISK_OFF", "BUY_STRONG", "BUY_DIP", "DATA_DEGRADED"}
    should_email = bool(force_email or daily_digest or state_changed or rate_shock or critical)

    subject = (
        f"[MACRO][{risk}][{signal}] "
        f"US10Y {_fmt(us10, 3, '%')} | Brent ${_fmt(brent, 2)} | S&P DD {_fmt(drawdown, 1, '%')}"
    )
    return Decision(
        risk_level=risk,
        signal=signal,
        action=action,
        reasons=reasons,
        subject=subject,
        should_email=should_email,
    )
