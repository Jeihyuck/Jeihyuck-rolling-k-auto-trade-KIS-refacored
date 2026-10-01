#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from macro_monitor.market_data import collect_market_bundle  # noqa: E402
from macro_monitor.report import build_report, build_subject  # noqa: E402
from macro_monitor.rules import MarketSnapshot, evaluate  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
IMPORTANT_SIGNALS = {
    "RATE_SHOCK",
    "OIL_SHOCK",
    "STAGFLATION_TAPE",
    "US10Y_GT_515",
    "US10Y_GT_500",
    "BRENT_GT_110",
    "PRICE_AND_YIELD_BUY",
    "DRAWDOWN_BUY",
    "YIELD_NORMALIZED",
}


def _load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _snapshot(bundle: dict) -> MarketSnapshot:
    q = bundle["quotes"]
    return MarketSnapshot(
        us10y=q["us10y"].value,
        us10y_prev=q["us10y"].previous_close,
        brent=q["brent"].value,
        brent_change_pct=q["brent"].change_pct,
        sp500=q["sp500"].value,
        sp500_change_pct=q["sp500"].change_pct,
        sp500_drawdown_pct=float(bundle["sp500_drawdown_pct"]),
        nasdaq=q["nasdaq"].value,
        vix=q["vix"].value,
        us10y_below_470_days=int(bundle["us10y_below_470_days"]),
    )


def _should_notify(previous: dict, regime: str, signals: tuple[str, ...], daily_summary: bool) -> tuple[bool, str]:
    if daily_summary:
        return True, "daily_summary"
    if not previous:
        return True, "initial_state"
    if previous.get("regime") != regime:
        return True, f"regime_change:{previous.get('regime')}->{regime}"
    old_signals = set(previous.get("signals") or [])
    new_important = (set(signals) - old_signals) & IMPORTANT_SIGNALS
    if new_important:
        return True, "new_signal:" + ",".join(sorted(new_important))
    return False, "unchanged"


def _send_mail(subject: str, body: str, report_path: Path) -> int:
    sender = ROOT / "scripts" / "notify" / "send_mail_attachment.py"
    cmd = [
        sys.executable,
        str(sender),
        "--subject",
        subject,
        "--body",
        body,
        "--attach",
        str(report_path),
    ]
    result = subprocess.run(cmd, cwd=ROOT, check=False)
    return int(result.returncode)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only macro market monitor and Naver-mail alert agent")
    parser.add_argument("--state-file", default="runtime/macro_monitor/state.json")
    parser.add_argument("--report-dir", default="reports/macro_monitor")
    parser.add_argument("--send-mail", action="store_true")
    parser.add_argument("--daily-summary", action="store_true")
    parser.add_argument("--force-mail", action="store_true")
    args = parser.parse_args(argv)

    state_path = ROOT / args.state_file
    report_dir = ROOT / args.report_dir
    previous = _load_state(state_path)

    bundle = collect_market_bundle()
    snapshot = _snapshot(bundle)
    decision = evaluate(snapshot)
    should_notify, notify_reason = _should_notify(
        previous,
        decision.regime,
        decision.signals,
        args.daily_summary,
    )
    if args.force_mail:
        should_notify, notify_reason = True, "forced"

    now = datetime.now(KST)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{now:%Y-%m-%d_%H%M}_macro_monitor.md"
    report = build_report(
        decision,
        snapshot,
        bundle["headlines"],
        collected_at=bundle["collected_at"],
    )
    report_path.write_text(report, encoding="utf-8")

    print(
        "[MACRO_MONITOR] "
        f"regime={decision.regime} us10y={snapshot.us10y:.3f} "
        f"brent={snapshot.brent:.2f} sp_dd={snapshot.sp500_drawdown_pct:.2f} "
        f"notify={should_notify} reason={notify_reason} report={report_path.relative_to(ROOT)}"
    )

    mail_rc = 0
    if args.send_mail and should_notify:
        subject = build_subject(decision, snapshot)
        body = (
            f"Macro Monitor 판정: {decision.regime}\n"
            f"US10Y {snapshot.us10y:.3f}% / Brent ${snapshot.brent:.2f} / "
            f"S&P drawdown {snapshot.sp500_drawdown_pct:.1f}%\n\n"
            f"행동: {decision.action}\n"
            f"알림 사유: {notify_reason}\n\n"
            "상세 분석은 첨부 Markdown 리포트를 확인하세요."
        )
        mail_rc = _send_mail(subject, body, report_path)
        if mail_rc != 0:
            print(f"[MACRO_MONITOR][MAIL_ERROR] rc={mail_rc}", file=sys.stderr)

    _save_state(
        state_path,
        {
            "regime": decision.regime,
            "severity": decision.severity,
            "signals": list(decision.signals),
            "us10y": snapshot.us10y,
            "brent": snapshot.brent,
            "sp500_drawdown_pct": snapshot.sp500_drawdown_pct,
            "last_run_kst": now.isoformat(),
            "last_notification_reason": notify_reason if should_notify else previous.get("last_notification_reason"),
        },
    )
    return mail_rc


if __name__ == "__main__":
    raise SystemExit(main())
