# -*- coding: utf-8 -*-
"""repos.py SQLAlchemy 마이그레이션 검증 테스트.

- psycopg2 직접 임포트 없음
- sqlalchemy.text 사용
- CAST(:meta AS jsonb) 패턴 사용
"""
import importlib
import inspect
import pytest


def test_repos_module_imports_without_error():
    """repos 모듈이 오류 없이 임포트되어야 한다."""
    import trader.us.db.repos as repos  # noqa: F401
    assert repos is not None


def test_repos_no_psycopg2_direct_import():
    """repos.py 소스에 psycopg2 직접 임포트가 없어야 한다."""
    import trader.us.db.repos as repos
    src_file = inspect.getfile(repos)
    with open(src_file, encoding="utf-8") as f:
        source = f.read()
    assert "import psycopg2" not in source, (
        "repos.py must not directly import psycopg2"
    )
    assert "from psycopg2" not in source, (
        "repos.py must not import from psycopg2"
    )


def test_repos_uses_sqlalchemy_text():
    """repos.py 소스에 sqlalchemy text 임포트가 있어야 한다."""
    import trader.us.db.repos as repos
    src_file = inspect.getfile(repos)
    with open(src_file, encoding="utf-8") as f:
        source = f.read()
    assert "from sqlalchemy import text" in source, (
        "repos.py must import text from sqlalchemy"
    )


def test_repos_uses_cast_jsonb():
    """repos.py 소스에 CAST(:meta AS jsonb) 패턴이 있어야 한다."""
    import trader.us.db.repos as repos
    src_file = inspect.getfile(repos)
    with open(src_file, encoding="utf-8") as f:
        source = f.read()
    assert "CAST(:meta AS jsonb)" in source, (
        "repos.py must use CAST(:meta AS jsonb) for jsonb columns"
    )


def test_repos_sqlalchemy_available_flag():
    """_SQLALCHEMY_AVAILABLE 플래그가 True여야 한다 (sqlalchemy 설치 환경)."""
    import trader.us.db.repos as repos
    assert repos._SQLALCHEMY_AVAILABLE is True, (
        "sqlalchemy must be available in test environment"
    )


def test_repos_text_symbol_is_sqlalchemy():
    """repos 모듈의 text가 sqlalchemy.text여야 한다."""
    import trader.us.db.repos as repos
    from sqlalchemy import text as sa_text
    assert repos.text is sa_text, (
        "repos.text must be sqlalchemy.text"
    )


@pytest.mark.parametrize("fn_name", [
    "save_us_watchlist",
    "save_order_intent",
    "save_position_snapshot",
    "save_fills",
    "save_order_ack",
    "save_order_reject",
    "save_reconcile_log",
])
def test_repos_functions_exist(fn_name):
    """필수 CRUD 함수가 존재해야 한다."""
    import trader.us.db.repos as repos
    assert hasattr(repos, fn_name), f"repos.{fn_name} must exist"
