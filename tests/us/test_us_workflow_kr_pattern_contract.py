# -*- coding: utf-8 -*-
"""미국장 workflow가 한국장 운영 패턴을 따르는지 검증.

Codex 지시서 섹션 17 기준.
"""
import pathlib
import re

WORKFLOW_DIR = pathlib.Path(".github/workflows")

US_WORKFLOWS = [
    "us-trade-prep.yml",
    "us-trade-am.yml",
    "us-trade-afternoon.yml",
    "us-trade-close.yml",
    "us-agent.yml",
]


def _read(name: str) -> str:
    return (WORKFLOW_DIR / name).read_text(encoding="utf-8")


def test_checkout_path_repo():
    """1. 각 workflow에 'path: repo'가 있다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        assert "path: repo" in content, f"{name}: checkout path: repo 없음"


def test_pythonpath_workspace_repo():
    """2. 각 workflow에 PYTHONPATH: .../repo가 있다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        assert "PYTHONPATH: ${{ github.workspace }}/repo" in content, (
            f"{name}: PYTHONPATH: ${{{{ github.workspace }}}}/repo 없음"
        )


def test_pbcore_db_url_in_job_env():
    """3. 각 workflow job env에 PBCORE_DB_URL secret이 있다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        assert "PBCORE_DB_URL: ${{ secrets.PBCORE_DB_URL }}" in content, (
            f"{name}: PBCORE_DB_URL: ${{{{ secrets.PBCORE_DB_URL }}}} 없음"
        )


def test_database_url_in_job_env():
    """4. 각 workflow job env에 DATABASE_URL이 있다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        assert "DATABASE_URL: ${{ secrets.PBCORE_DB_URL }}" in content, (
            f"{name}: DATABASE_URL: ${{{{ secrets.PBCORE_DB_URL }}}} 없음"
        )


def test_kis_secrets_in_job_env():
    """5. 각 workflow에 KIS_APP_KEY/KIS_APP_SECRET/KIS_REST_URL/CANO/ACNT_PRDT_CD가 있다."""
    required = [
        "KIS_APP_KEY: ${{ secrets.KIS_APP_KEY }}",
        "KIS_APP_SECRET: ${{ secrets.KIS_APP_SECRET }}",
        "KIS_REST_URL: ${{ secrets.KIS_REST_URL }}",
        "CANO: ${{ secrets.CANO }}",
        "ACNT_PRDT_CD: ${{ secrets.ACNT_PRDT_CD }}",
    ]
    for name in US_WORKFLOWS:
        content = _read(name)
        for req in required:
            assert req in content, f"{name}: {req} 없음"


def test_alias_env_in_job_env():
    """6. 각 workflow에 APP_KEY/APP_SECRET/API_BASE_URL alias가 있다."""
    aliases = [
        "APP_KEY: ${{ secrets.KIS_APP_KEY }}",
        "APP_SECRET: ${{ secrets.KIS_APP_SECRET }}",
        "API_BASE_URL: ${{ secrets.KIS_REST_URL }}",
    ]
    for name in US_WORKFLOWS:
        content = _read(name)
        for alias in aliases:
            assert alias in content, f"{name}: alias {alias} 없음"


def test_db_migrate_working_dir_repo():
    """7. DB migrate step은 working-directory: repo를 사용한다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        # DB migrate가 있는지 + working-directory: repo와 함께
        if "trader.db.migrate" not in content:
            continue
        # migrate 주변에 working-directory: repo가 있어야 함
        # (같은 step 블록에 포함)
        idx = content.find("trader.db.migrate")
        # 앞 500자에서 working-directory: repo를 찾는다
        context = content[max(0, idx - 500):idx]
        assert "working-directory: repo" in context, (
            f"{name}: DB migrate step에 working-directory: repo 없음"
        )


def test_db_migrate_tee_log():
    """8. DB migrate step은 artifacts/${US_LOG_FILE}에 tee한다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        if "trader.db.migrate" not in content:
            continue
        idx = content.find("trader.db.migrate")
        context = content[max(0, idx - 200):idx + 200]
        assert "tee" in context and "artifacts" in context, (
            f"{name}: DB migrate step에 tee -a artifacts/... 없음"
        )


def test_artifact_upload_path_repo_artifacts():
    """9. artifact upload path에 repo/artifacts/가 있다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        assert "repo/artifacts/" in content, f"{name}: artifact path에 repo/artifacts/ 없음"


def test_if_no_files_found_warn():
    """10. if-no-files-found가 warn이다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        assert "if-no-files-found: warn" in content, (
            f"{name}: if-no-files-found: warn 없음 (ignore나 error가 아닌 warn이어야 함)"
        )


def test_no_tmp_log_only_upload():
    """11. /tmp/*.log만 업로드하는 구조가 아니다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        # /tmp/*.log 단독 업로드 패턴 금지
        assert not re.search(r"path:\s*/tmp/\*\.log\s*$", content, re.MULTILINE), (
            f"{name}: /tmp/*.log 단독 업로드 금지"
        )


def test_no_kr_trade_tick_call():
    """12. 미국장 workflow에서 python -m trader.trade_tick 호출 없어야 한다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        assert "trader.trade_tick" not in content, (
            f"{name}: 한국장 trader.trade_tick 호출 금지"
        )


def test_no_final30_kr_check():
    """13. 미국장 workflow에서 final30_scored 한국장 검증 없어야 한다."""
    for name in US_WORKFLOWS:
        content = _read(name)
        assert "final30_scored" not in content, (
            f"{name}: 한국장 final30_scored 검증 호출 금지"
        )


def test_us_trade_am_calls_session_runner_am():
    """14. us-trade-am은 trade_session_runner --session am을 호출한다."""
    content = _read("us-trade-am.yml")
    assert "trade_session_runner" in content, "us-trade-am.yml: trade_session_runner 없음"
    assert "--session am" in content, "us-trade-am.yml: --session am 없음"


def test_us_trade_afternoon_calls_session_runner_afternoon():
    """15. us-trade-afternoon은 trade_session_runner --session afternoon을 호출한다."""
    content = _read("us-trade-afternoon.yml")
    assert "trade_session_runner" in content, "us-trade-afternoon.yml: trade_session_runner 없음"
    assert "--session afternoon" in content, "us-trade-afternoon.yml: --session afternoon 없음"


def test_us_trade_prep_calls_dispatcher_prep():
    """16. us-trade-prep은 dispatcher --mode prep을 호출한다."""
    content = _read("us-trade-prep.yml")
    assert "trader.us.runner.dispatcher" in content, "us-trade-prep.yml: dispatcher 없음"
    assert "--mode prep" in content, "us-trade-prep.yml: --mode prep 없음"


def test_us_trade_close_calls_dispatcher_trade_close():
    """17. us-trade-close는 dispatcher --mode trade-close를 호출한다."""
    content = _read("us-trade-close.yml")
    assert "trader.us.runner.dispatcher" in content, "us-trade-close.yml: dispatcher 없음"
    assert "--mode trade-close" in content, "us-trade-close.yml: --mode trade-close 없음"
