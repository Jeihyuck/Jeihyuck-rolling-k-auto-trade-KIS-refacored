from trader.prep_runner import compute_prep_core_status


def test_prep_core_ok_not_saved_when_quality_fails(monkeypatch):
    saved = []

    def fake_save_job_checkpoint(*args, **kwargs):
        saved.append((args, kwargs))

    monkeypatch.setattr("trader.prep_runner.save_job_checkpoint", fake_save_job_checkpoint)
    result = compute_prep_core_status(
        scored_contract={"contract_ok": 1, "rows": 30},
        db_roundtrip_ok=True,
        final30_file_contract_ok=True,
        final30_quality_ok=False,
    )
    assert result["core_ok"] == 0
    assert result["status"] in {"FAIL_CORE_QUALITY", "FAIL_CORE_CONTRACT"}
    assert saved == []


def test_prep_core_ok_saved_only_when_all_core_checks_pass():
    result = compute_prep_core_status(
        scored_contract={"contract_ok": 1, "rows": 30},
        db_roundtrip_ok=True,
        final30_file_contract_ok=True,
        final30_quality_ok=True,
    )
    assert result["core_ok"] == 1
    assert result["status"] == "PREP_CORE_OK"


def test_prep_core_ok_not_saved_when_db_roundtrip_fails():
    result = compute_prep_core_status(
        scored_contract={"contract_ok": 1, "rows": 30},
        db_roundtrip_ok=False,
        final30_file_contract_ok=True,
        final30_quality_ok=True,
    )
    assert result["core_ok"] == 0
    assert "db_roundtrip_not_ok" in result["reasons"]


def test_prep_core_ok_not_saved_when_file_mirror_fails():
    result = compute_prep_core_status(
        scored_contract={"contract_ok": 1, "rows": 30},
        db_roundtrip_ok=True,
        final30_file_contract_ok=False,
        final30_quality_ok=True,
    )
    assert result["core_ok"] == 0
    assert "file_mirror_not_ok" in result["reasons"]
