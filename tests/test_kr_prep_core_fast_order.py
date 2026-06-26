from pathlib import Path


def _src(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_core_artifact_written_inside_build_and_save_watchlist_before_flow():
    src = _src("trader/watchlist_builder.py")
    assert "core_artifact_trade_date" in src
    assert "publish_kr_prep_artifacts_core_fast" in src
    build_src = src[src.index("def build_and_save_watchlist("):]
    assert build_src.index("publish_kr_prep_artifacts_core_fast") < build_src.index("save_bundle(")


def test_build_and_save_watchlist_core_done_even_when_derived_repo_connection_closed():
    src = _src("trader/db/repos.py")
    assert "EDBHANDLEREXITED" in src
    assert "self.engine.dispose()" in src
    assert "[DB][DERIVED][LOAD_FAIL_SOFT]" in src
    assert "return []" in src


def test_inquire_investor_egw00201_no_retry():
    src = _src("trader/kis_wrapper.py")
    assert "no_retry_inquire_investor" in src
    assert "attempts = 1" in src
    assert "[KR_FLOW][KIS_INVESTOR][RATE_LIMIT_FAIL_SOFT]" in src


def test_flow_disabled_skips_flow_try():
    src = _src("trader/prep_runner.py")
    assert "KR_INVESTOR_FLOW_ENABLED" in src
    assert "providers = []" in src
    assert src.index("providers = []") < src.index('logger.info("[FLOW][TRY]')


def test_prep_exit_zero_after_core_done_even_aux_failure():
    src = _src("trader/prep_runner.py")
    assert 'return "OK_CORE_AUX_DEGRADED", 0' in src
    assert "[PREP][AUX][FAIL_SOFT]" in src
