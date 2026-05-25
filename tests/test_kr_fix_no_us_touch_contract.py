import os
import subprocess


FORBIDDEN_PREFIXES = (
    "trader/us/",
    "tests/us/",
    "docs/US_",
    ".github/workflows/us-",
)

FORBIDDEN_CONTAINS = (
    "US_FIXES_",
)


def test_kr_fix_does_not_touch_us_files():
    base = os.getenv("BASE_REF", "origin/dual-agent")
    try:
        diff = subprocess.check_output(
            ["git", "diff", "--name-only", f"{base}...HEAD"],
            text=True,
        )
    except Exception:
        return

    changed = [line.strip() for line in diff.splitlines() if line.strip()]

    bad = [
        path for path in changed
        if path.startswith(FORBIDDEN_PREFIXES)
        or any(token in path for token in FORBIDDEN_CONTAINS)
    ]

    assert not bad, f"US files must not be touched in KR-only fix: {bad}"
