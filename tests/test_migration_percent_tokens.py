from pathlib import Path
import re


def test_raise_notice_percent_tokens_are_driver_safe():
    for path in Path("migrations").glob("*.sql"):
        for notice in re.findall(r"RAISE\s+NOTICE\s+'([^']*)'", path.read_text(), flags=re.I):
            assert not re.search(r"(?<!%)%(?!%)", notice), path
