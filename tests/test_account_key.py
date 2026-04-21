import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.account_state import get_account_key, get_masked_account_key


def test_get_account_key_normalizes_digits_only():
    key = get_account_key(env="practice", cano="50-160136", product_code="01")

    assert key == "practice:50160136:01"


def test_get_masked_account_key_hides_cano_prefix():
    masked = get_masked_account_key(env="practice", cano="50160136", product_code="01")

    assert masked == "practice:***0136:01"