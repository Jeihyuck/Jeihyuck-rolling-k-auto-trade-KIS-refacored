"""US 2026-11-09 Close/mail trade-date regression (no live SMTP)."""
from __future__ import annotations

import runpy
import subprocess
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
NY = ZoneInfo("America/New_York")
KST = ZoneInfo("Asia/Seoul")
resolver = runpy.run_path(str(ROOT / "scripts/wsl/resolve-us-mail-trade-date.py"))
latest_completed = resolver["latest_completed_us_trade_date"]


class CloseMailDateContract(unittest.TestCase):
    def test_est_november_9_close_and_afternoon(self):
        self.assertEqual(
            datetime(2026, 11, 9, 16, 5, tzinfo=NY).astimezone(KST).isoformat(),
            "2026-11-10T06:05:00+09:00",
        )
        self.assertEqual(latest_completed(
            datetime(2026, 11, 9, 17, 0, tzinfo=NY)), "2026-11-09")
        self.assertEqual(latest_completed(
            datetime(2026, 11, 9, 10, 0, tzinfo=NY)), "2026-11-06")

    def test_edt_and_friday_to_saturday_mail(self):
        self.assertEqual(
            datetime(2026, 7, 9, 16, 5, tzinfo=NY).astimezone(KST).isoformat(),
            "2026-07-10T05:05:00+09:00",
        )
        self.assertEqual(latest_completed(
            datetime(2026, 11, 7, 9, 0, tzinfo=NY)), "2026-11-06")

    def test_holiday_and_early_close(self):
        self.assertEqual(latest_completed(
            datetime(2026, 11, 26, 18, 0, tzinfo=NY)), "2026-11-25")
        self.assertEqual(latest_completed(
            datetime(2026, 11, 27, 13, 5, tzinfo=NY)), "2026-11-27")
        self.assertEqual(latest_completed(
            datetime(2026, 11, 27, 12, 0, tzinfo=NY)), "2026-11-25")

    def test_script_does_not_fallback_to_previous_close_file(self):
        mail = (ROOT / "scripts/wsl/send-market-log-mail.sh").read_text()
        self.assertIn("resolve-us-mail-trade-date.py", mail)
        self.assertNotIn("resolve_latest_completed_us_trade_date", mail)
        self.assertIn("collect_session_purpose \"$purpose\"", mail)
        self.assertIn("UNKNOWN_AFTER_SEND_FENCED", mail)
        self.assertIn("break  # Never issue a second SMTP attempt", mail)
        for path in ("scripts/wsl/send-market-log-mail.sh",):
            result = subprocess.run(
                ["bash", "-n", str(ROOT / path)], capture_output=True, text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_close_dispatcher_status_contract(self):
        from trader.us.runner.dispatcher import dispatch
        from trader.us.runner import trade_close_runner
        for status, expected in (
            ("OK", 0), ("OK_WITH_WARNINGS", 0),
            ("ERROR", 1), ("SKIP", 1),
        ):
            # GitHub PR CI sets GITHUB_EVENT_NAME=pull_request. This test
            # checks the normal scheduler Close path, not the PR execution
            # safety guard; do not weaken the production dispatcher guard.
            with self.subTest(status=status), patch.dict(
                "os.environ", {"GITHUB_EVENT_NAME": "schedule"},
            ), patch.object(
                trade_close_runner, "run_trade_close", return_value={"status": status},
            ):
                self.assertEqual(
                    dispatch("close", env="practice", offline=True), expected
                )

    def test_close_pr_event_requires_explicit_offline_smoke_scope(self):
        from trader.us.runner.dispatcher import dispatch
        from trader.us.runner import trade_close_runner
        with patch.dict("os.environ", {
            "GITHUB_EVENT_NAME": "pull_request", "ALLOW_REAL_ORDER": "0",
        }), patch.object(
            trade_close_runner, "run_trade_close", return_value={"status": "OK"},
        ) as run_close:
            # A PR event without forced scope must never execute a Close.
            self.assertEqual(dispatch("close", env="practice", offline=True), 1)
            run_close.assert_not_called()
            # Explicit offline smoke context is permitted by the existing
            # gate and exercises the Close status under PR CI.
            self.assertEqual(dispatch(
                "close", env="practice", offline=True,
                force_now="2026-11-09T17:00:00-05:00",
            ), 0)
            run_close.assert_called_once()

    def test_windows_scheduler_uses_one_consistent_us_dst_profile(self):
        # Baselines are KST EDT wall-clock triggers. Every phase, including
        # Close, mail and health, gets +1h during EST; prewarm tasks already
        # carry explicit season labels and stay on their separate clocks.
        baseline = {
            "PB1 US Prep WSL": "21:30",
            "PB1 US Prep Recovery WSL": "22:10",
            "PB1 US AM Preflight WSL": "22:20",
            "PB1 US AM WSL": "22:30",
            "PB1 US Afternoon WSL": "02:00",
            "PB1 US Close WSL": "05:05",
            "PB1 US Mail WSL": "06:00",
            "PB1 US Health WSL": "06:10",
        }
        sources = {
            name: (ROOT / "scripts/windows" / name).read_text()
            for name in ("update-nullim-scheduler.ps1", "verify-scheduler.ps1")
        }
        for source in sources.values():
            self.assertIn('FindSystemTimeZoneById("Eastern Standard Time")', source)
            self.assertIn('IsDaylightSavingTime($nyLocalNow)', source)
            self.assertIn('Get-UsKstTime', source)
            for task, edt_clock in baseline.items():
                self.assertIn(f'"{task}"', source)
                self.assertIn(f'(Get-UsKstTime "{edt_clock}")', source)
        self.assertIn('EST Close 16:00 NY => 06:00 KST', sources["update-nullim-scheduler.ps1"])

    def test_us_scheduler_est_and_edt_phase_clock_matrix(self):
        from datetime import timedelta
        baselines = {
            "Prep": ("21:30", "22:30"),
            "Recovery": ("22:10", "23:10"),
            "AM Preflight": ("22:20", "23:20"),
            "AM": ("22:30", "23:30"),
            "Afternoon": ("02:00", "03:00"),
            "Close": ("05:05", "06:05"),
            "Mail": ("06:00", "07:00"),
            "Health": ("06:10", "07:10"),
        }
        for season_time, expected_hour_offset in (
            (datetime(2026, 7, 10, tzinfo=NY), 0),
            (datetime(2026, 11, 10, tzinfo=NY), 1),
        ):
            actual_offset = 0 if season_time.dst() else 1
            self.assertEqual(actual_offset, expected_hour_offset)
            for phase, (edt, est) in baselines.items():
                hh, mm = map(int, edt.split(":"))
                actual = (datetime(2026, 1, 1, hh, mm) + timedelta(
                    hours=actual_offset)).strftime("%H:%M")
                self.assertEqual(
                    actual, edt if actual_offset == 0 else est, phase,
                )


if __name__ == "__main__":
    unittest.main()
