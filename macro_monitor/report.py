from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from .market_data import Headline
from .rules import Decision, MarketSnapshot


KST = ZoneInfo("Asia/Seoul")


def _fmt_change(value: float | None, suffix: str = "%") -> str:
    if value is None:
        return "n/a"
    return f"{value:+.2f}{suffix}"


def build_subject(decision: Decision, snapshot: MarketSnapshot) -> str:
    return (
        f"[MACRO][{decision.regime}] "
        f"US10Y {snapshot.us10y:.3f}% | Brent ${snapshot.brent:.1f} | "
        f"S&P DD {snapshot.sp500_drawdown_pct:.1f}%"
    )


def build_report(
    decision: Decision,
    snapshot: MarketSnapshot,
    headlines: list[Headline],
    *,
    collected_at: str,
) -> str:
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    lines = [
        "# Macro Market Monitor",
        "",
        f"- 생성: {now_kst}",
        f"- 데이터 수집(UTC): {collected_at}",
        f"- 판정: **{decision.regime}** (severity {decision.severity}/4)",
        "",
        "## 핵심 시장 지표",
        "",
        "| 지표 | 현재 | 변화/상태 |",
        "|---|---:|---:|",
        f"| 미국 10년물 | **{snapshot.us10y:.3f}%** | {_fmt_change(snapshot.us10y_change_bp, 'bp')} |",
        f"| Brent | **${snapshot.brent:.2f}** | {_fmt_change(snapshot.brent_change_pct)} |",
        f"| S&P500 | **{snapshot.sp500:,.2f}** | {_fmt_change(snapshot.sp500_change_pct)} |",
        f"| S&P500 1년 고점 대비 | **{snapshot.sp500_drawdown_pct:.2f}%** | - |",
        f"| Nasdaq | **{snapshot.nasdaq:,.2f}** | - |",
        f"| VIX | **{snapshot.vix:.2f}** | - |",
        f"| 10Y ≤4.70% 연속 거래일 | **{snapshot.us10y_below_470_days}일** | - |",
        "",
        "## 투자 판정",
        "",
        f"**{decision.action}**",
        "",
    ]

    if decision.signals:
        lines.extend(["### Trigger", "", *[f"- `{x}`" for x in decision.signals], ""])
    if decision.reasons:
        lines.extend(["### 근거", "", *[f"- {x}" for x in decision.reasons], ""])

    lines.extend(
        [
            "## 현재 연금 실행 원칙",
            "",
            "- 기존 4% 저축은행 예금 1.8억원: 유지",
            "- 기존 TDF 2개 총 3,000만원: 유지, 추가매수는 별도 판단",
            "- 신규 대기자금: TIGER CD금리투자KIS(357870) 중심 파킹",
            "- 신규 주식 진입 순서: S&P500 → Nasdaq100 → 반도체",
            "- 이 모니터는 **주문을 실행하지 않고 알림만 발송**",
            "",
            "## 주요 뉴스 헤드라인 (맥락 참고용)",
            "",
        ]
    )
    if headlines:
        for headline in headlines[:8]:
            lines.append(f"- [{headline.title}]({headline.link})")
    else:
        lines.append("- 뉴스 RSS 수집 실패/없음 (숫자 판정에는 영향 없음)")

    lines.extend(
        [
            "",
            "## Rulebook",
            "",
            "- US10Y ≥5.15%: RED",
            "- US10Y ≥5.00% 또는 Brent ≥$110: RISK_OFF",
            "- US10Y 4.85~5.00% 또는 Brent ≥$100: CAUTION",
            "- S&P -10% 이상 + US10Y ≤4.90%: BUY_1 (단, 위험조건 우선)",
            "- S&P -10% 이상 + US10Y ≤4.70% + Brent <$105: BUY_STRONG",
            "- US10Y ≤4.70% 2거래일 이상 + Brent <$100: BUY_READY",
            "",
            "---",
            "본 리포트는 거시시장 모니터링용이며 자동매매 신호가 아닙니다.",
        ]
    )
    return "\n".join(lines) + "\n"
