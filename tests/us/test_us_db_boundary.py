# -*- coding: utf-8 -*-
"""US/KR DB 경계 테스트.

US 코드가 KR 테이블에 직접 접근하지 않는지 검사.
"""
import pathlib
import re

US_CODE_DIR = pathlib.Path("trader/us")

# KR 전용 테이블명 (us_ 접두사 없는)
KR_TABLE_PATTERNS = [
    r"INSERT\s+INTO\s+(?!us_)(orders|positions|fills|signals|runs|ledger_events|price_daily|pb1_watchlist)",
    r"SELECT\s+.*\s+FROM\s+(?!us_)(orders|positions|fills|signals|runs|ledger_events|price_daily|pb1_watchlist)",
    r"UPDATE\s+(?!us_)(orders|positions|fills|signals|runs|ledger_events)",
    r"DELETE\s+FROM\s+(?!us_)(orders|positions|fills|signals|runs|ledger_events)",
]


def iter_us_py_files():
    return sorted(US_CODE_DIR.rglob("*.py"))


def test_us_code_does_not_import_kis_wrapper():
    """US 코드가 KR KisAPI를 직접 import하지 않아야 한다."""
    for path in iter_us_py_files():
        src = path.read_text(encoding="utf-8")
        if "from trader.kis_wrapper" in src or "import trader.kis_wrapper" in src:
            raise AssertionError(
                f"{path}: KR kis_wrapper 직접 import 금지. us_kis_client.py 등 US 전용 클라이언트를 사용하라."
            )


def test_us_code_does_not_access_kr_tables_directly():
    """US 코드가 KR 테이블(us_ 접두사 없는)에 직접 SQL INSERT/SELECT하지 않아야 한다."""
    violations = []
    for path in iter_us_py_files():
        src = path.read_text(encoding="utf-8")
        for pat in KR_TABLE_PATTERNS:
            matches = re.findall(pat, src, flags=re.IGNORECASE)
            if matches:
                violations.append(f"{path}: pattern={pat!r} matches={matches}")
    assert not violations, "US 코드가 KR 테이블에 직접 접근:\n" + "\n".join(violations)


def test_us_db_repos_only_uses_us_prefix():
    """repos.py INSERT INTO 구문이 모두 us_ 테이블을 대상으로 한다."""
    repos = pathlib.Path("trader/us/db/repos.py")
    src = repos.read_text(encoding="utf-8")
    inserts = re.findall(r"INSERT\s+INTO\s+(\w+)", src, re.IGNORECASE)
    for tbl in inserts:
        assert tbl.startswith("us_"), f"repos.py INSERT INTO {tbl!r}: us_ 아닌 테이블"
