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
            with self.subTest(status=status), patch.object(
                trade_close_runner, "run_trade_close", return_value={"status": status},
            ):
                self.assertEqual(dispatch(
                    "close", env="practice", offline=True), expected)

    def test_windows_installer_cannot_schedule_close_before_est_end(self):
        for name in ("update-nullim-scheduler.ps1", "verify-scheduler.ps1"):
            source = (ROOT / "scripts/windows" / name).read_text()
            self.assertIn('"PB1 US Close WSL"', source)
            self.assertIn('06:05', source)
            self.assertNotIn('"PB1 US Close WSL";Days=$usMorningDays;Time="05:05"', source)


if __name__ == "__main__":
    unittest.main()
