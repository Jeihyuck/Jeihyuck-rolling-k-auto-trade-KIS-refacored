"""Contract test: US files must not import from KR modules.

이 테스트는 `trader/us/` 하위 파이썬 파일이 아래 한국장 모듈을 
직접 import하지 않음을 정적으로 검증한다.

금지된 KR import:
  - trader.db.repos
  - trader.pb1_engine
  - trader.kis_wrapper
  - trader.prep_runner
  - trader.trade_am_runner
  - trader.trade_afternoon_runner
  - trader.trade_close_runner
  - settings  (최상위 settings.py — KR 설정)
"""

from __future__ import annotations

import ast
import pathlib
import textwrap
from typing import Iterator

import pytest


# ---------------------------------------------------------------------------
# Forbidden KR import patterns
# ---------------------------------------------------------------------------

FORBIDDEN_KR_IMPORTS = [
    "trader.db.repos",
    "trader.pb1_engine",
    "trader.kis_wrapper",
    "trader.prep_runner",
    "trader.trade_am_runner",
    "trader.trade_afternoon_runner",
    "trader.trade_close_runner",
]

# Top-level `import settings` or `from settings import ...` (KR settings.py)
# Note: `trader.us.config` is allowed. Only the bare `settings` module is forbidden.
FORBIDDEN_BARE_IMPORTS = ["settings"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

US_TRADER_ROOT = pathlib.Path(__file__).parent.parent.parent / "trader" / "us"


def _iter_python_files() -> Iterator[pathlib.Path]:
    yield from US_TRADER_ROOT.rglob("*.py")


def _extract_imports(source: str) -> list[str]:
    """Return list of fully-qualified import names from Python source."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return imports


# ---------------------------------------------------------------------------
# Parametrized tests
# ---------------------------------------------------------------------------

def _collect_violations() -> list[tuple[str, str, str]]:
    """Returns list of (filepath, import_name, violation_reason)."""
    violations: list[tuple[str, str, str]] = []
    for py_file in _iter_python_files():
        try:
            source = py_file.read_text(encoding="utf-8")
        except Exception:
            continue
        imports = _extract_imports(source)
        for imp in imports:
            for forbidden in FORBIDDEN_KR_IMPORTS:
                if imp == forbidden or imp.startswith(forbidden + "."):
                    violations.append((str(py_file), imp, f"KR module: {forbidden}"))
            for bare in FORBIDDEN_BARE_IMPORTS:
                if imp == bare:
                    violations.append((str(py_file), imp, f"bare KR settings: {bare}"))
    return violations


@pytest.mark.parametrize("filepath,import_name,reason", _collect_violations())
def test_no_kr_import(filepath: str, import_name: str, reason: str):
    """US 파일이 KR 모듈을 import하면 이 테스트가 실패한다."""
    relative = pathlib.Path(filepath).relative_to(US_TRADER_ROOT.parent.parent)
    pytest.fail(
        f"US file '{relative}' imports forbidden KR module '{import_name}' ({reason})"
    )


def test_us_trader_dir_exists():
    """trader/us/ 디렉토리가 존재해야 한다."""
    assert US_TRADER_ROOT.is_dir(), f"trader/us/ not found at {US_TRADER_ROOT}"


def test_at_least_one_us_file_checked():
    """최소 1개 이상의 US Python 파일이 검사되었는지 확인."""
    files = list(_iter_python_files())
    assert len(files) > 0, "No Python files found under trader/us/"
