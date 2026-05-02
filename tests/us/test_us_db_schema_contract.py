# -*- coding: utf-8 -*-
"""DB schema contract test.

repos.py가 migration 0038 컬럼과 일치하는지 검사.
"""
import pathlib

REPOS_PATH = pathlib.Path("trader/us/db/repos.py")
MIGRATION_PATH = pathlib.Path("migrations/0038_us_agent_tables.sql")


def _read(path):
    return path.read_text(encoding="utf-8")


def test_migration_exists():
    assert MIGRATION_PATH.exists()


def test_repos_uses_qty_requested():
    src = _read(REPOS_PATH)
    assert "qty_requested" in src, "repos.py는 us_orders.qty_requested를 사용해야 한다"


def test_repos_does_not_use_forbidden_order_columns():
    src = _read(REPOS_PATH)
    # us_orders insert에 존재하지 않는 컬럼 금지 (주석/문자열은 제외)
    import re
    # INSERT/UPDATE INTO us_orders(...) 구문 내에 order_type, raw_response가 없는지 확인
    # SQL 문자열 내에서만 금지 (주석 행은 제외)
    non_comment_lines = "\n".join(
        ln for ln in src.splitlines() if not ln.strip().startswith("#")
    )
    # SQL 컬럼 리스트 컨텍스트에서의 등장 여부 (따옴표 없는 식별자로)
    for col in ["order_type", "raw_response"]:
        # column name in SQL: followed by space/comma/newline/paren
        pattern = rf'\b{col}\b\s*[,\)\s=]'
        if re.search(pattern, non_comment_lines):
            raise AssertionError(f"repos.py에 금지 SQL 컬럼 {col!r}이 있다")


def test_repos_uses_price_usd_for_fills():
    src = _read(REPOS_PATH)
    assert "price_usd" in src, "repos.py는 us_fills.price_usd를 사용해야 한다"


def test_repos_does_not_use_fill_price_usd():
    src = _read(REPOS_PATH)
    # 주석 제외한 non-comment 라인에서만 검사
    non_comment = "\n".join(
        ln for ln in src.splitlines() if not ln.strip().startswith("#")
    )
    assert "fill_price_usd" not in non_comment, "repos.py에 금지 컬럼 fill_price_usd가 있다 (주석 제외)"


def test_repos_does_not_use_filled_at_str():
    src = _read(REPOS_PATH)
    non_comment = "\n".join(
        ln for ln in src.splitlines() if not ln.strip().startswith("#")
    )
    assert "filled_at_str" not in non_comment, "repos.py에 금지 컬럼 filled_at_str가 있다 (주석 제외)"


def test_repos_uses_as_of_avg_cost_current_px():
    src = _read(REPOS_PATH)
    assert "as_of" in src, "repos.py는 us_positions.as_of를 사용해야 한다"
    assert "avg_cost" in src, "repos.py는 us_positions.avg_cost를 사용해야 한다"
    assert "current_px" in src, "repos.py는 us_positions.current_px를 사용해야 한다"


def test_repos_does_not_use_forbidden_position_columns():
    src = _read(REPOS_PATH)
    # us_positions INSERT에는 avg_price_usd, current_price_usd 컬럼이 없다.
    # 단, us_orders에는 avg_price_usd가 유효하므로,
    # us_positions INSERT 구문 내에만 포함되지 않으면 OK.
    # INSERT INTO us_positions(...) 블록에서만 검사
    import re
    pos_insert = re.search(
        r"INSERT\s+INTO\s+us_positions\s*\(([^)]+)\)",
        src,
        re.DOTALL | re.IGNORECASE,
    )
    if pos_insert:
        col_block = pos_insert.group(1)
        assert "avg_price_usd" not in col_block, "us_positions INSERT에 금지 컬럼 avg_price_usd가 있다"
        assert "current_price_usd" not in col_block, "us_positions INSERT에 금지 컬럼 current_price_usd가 있다"


def test_repos_does_not_use_position_status_column():
    src = _read(REPOS_PATH)
    # us_positions에는 status 컬럼이 없음
    # "status='OPEN'" 또는 "status = 'OPEN'" 같은 패턴 금지
    assert "status = 'OPEN'" not in src
    assert "status='OPEN'" not in src


def test_reconcile_log_uses_message_not_detail():
    src = _read(REPOS_PATH)
    # us_reconcile_logs은 message, meta를 사용 (position_count 컬럼 없음)
    assert "\"position_count\"" not in src or "meta" in src


def test_migration_has_us_prefix_tables_only():
    sql = _read(MIGRATION_PATH)
    import re
    tables = re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", sql)
    for t in tables:
        assert t.startswith("us_"), f"migration에 us_* 아닌 테이블 {t!r}이 있다"
