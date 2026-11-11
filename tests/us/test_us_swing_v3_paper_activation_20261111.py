"""Scheduled practice profile grants new swing v3, preserving frozen owners."""
import json
import os
import subprocess
import sys
import unittest


class ScheduledSwingV3Test(unittest.TestCase):
    def invoke(self, **extra):
        env = {"PATH": os.environ["PATH"], "PYTHONPATH": os.getcwd(), **extra}
        script = r'''source scripts/wsl/us-swing-v3-paper-defaults.sh >&2
python - <<'INNER'
import json, os
from trader.us.entry_exit_contract import build_us_entry_exit_contract
new = {"symbol":"AMGN", "strategy_owner":"US_STANDARD", "book":"SWING_BOOK", "horizon":"SWING_CARRY", "position_action":"NEW_POSITION_BUY"}
old = build_us_entry_exit_contract({**new, "position_action":"ADD_TO_EXISTING_BUY"})
print(json.dumps({"mode":os.getenv("US_SWING_EXIT_MODE"), "approved":os.getenv("US_SWING_V3_DEPLOY_APPROVED"), "new":build_us_entry_exit_contract(new)["version"], "old":old["version"], "frozen":build_us_entry_exit_contract({**new, "entry_exit_contract":old})["version"], "tqqq":build_us_entry_exit_contract({"symbol":"TQQQ","strategy_owner":"TQQQ_INFINITE"})}))
INNER'''
        run = subprocess.run(["bash", "-c", script], env=env, text=True, capture_output=True, check=True)
        return json.loads(run.stdout)

    def test_practice_upgrades_shadow_and_preserves_existing_contract(self):
        out = self.invoke(KIS_ENV="practice", STRATEGY_ENV="practice", US_SWING_EXIT_MODE="V3_SHADOW")
        self.assertEqual(out["mode"], "V3_ACTIVE_PRACTICE")
        self.assertEqual(out["new"], "us_entry_exit_contract_v3")
        self.assertEqual(out["frozen"], "us_entry_exit_contract_v2")
        self.assertEqual(out["tqqq"], {})

    def test_explicit_rollback_and_real_account_do_not_activate(self):
        rollback = self.invoke(KIS_ENV="practice", US_SWING_V3_ACTIVATION="OFF")
        self.assertEqual(rollback["new"], "us_entry_exit_contract_v2")
        real = self.invoke(KIS_ENV="real")
        self.assertEqual(real["new"], "us_entry_exit_contract_v2")
        self.assertIsNone(real["approved"])


if __name__ == "__main__":
    unittest.main()
