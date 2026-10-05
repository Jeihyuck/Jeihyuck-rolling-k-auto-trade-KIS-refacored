from __future__ import annotations

import ast
import json
from pathlib import Path


def test_incident_manifests_disclose_inputs_and_reference_continuous_component_tests():
    root = Path(__file__).resolve().parents[2]
    fixture_root = root / "tests" / "us" / "fixtures" / "runtime_integrity"
    fixture_names = (
        "incident_20260929_prep_tqqq.json.fixture",
        "incident_20260930_liveness_minhold.json.fixture",
        "incident_20261001_tqqq_sector_cap.json.fixture",
    )
    test_sources = {
        path.stem.removesuffix(".py"): path.read_text(encoding="utf-8")
        for path in (root / "tests" / "us").glob("test_us_*.py")
    }

    for fixture_name in fixture_names:
        fixture = json.loads((fixture_root / fixture_name).read_text(encoding="utf-8"))
        assert fixture["incident_date"]
        assert fixture["code_sha"]
        assert isinstance(fixture["settings"], dict)
        assert "run_id" in fixture["prep"]
        assert "status" in fixture["prep"]
        assert "initial_balance" in fixture
        assert "initial_positions" in fixture
        assert "broker_responses" in fixture
        assert fixture["clock"]["timestamp"]
        assert fixture["clock"]["timezone"]
        assert "prices" in fixture
        assert fixture["synthesized_missing_inputs"]
        for test_id in fixture["production_path_test_ids"]:
            assert any(test_id in source for source in test_sources.values()), test_id


def test_legacy_kr_runtime_integrity_install_order_is_explicit_and_stable():
    root = Path(__file__).resolve().parents[2]
    source = (root / "trader" / "__init__.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    installer = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "install_legacy_pb1_runtime_guards"
    )
    calls = [
        node.func.id
        for node in ast.walk(installer)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    expected_order = [
        "install_kr_20260915_runtime_integrity",
        "install_kr_20260917_runtime_integrity",
        "install_kr_20260928_runtime_integrity",
        "install_kr_20260929_runtime_integrity",
        "install_kr_20260929_review_guards",
        "install_kr_20260929_log_review_guards",
        "install_kr_20260929_side_proof_guard",
        "install_kr_20260930_runtime_integrity",
        "install_kr_20260930_freshness_review",
    ]

    indexes = [calls.index(name) for name in expected_order]
    assert indexes == sorted(indexes)

    kr_runner = (
        root / "trader" / "kr" / "runner" / "trade_session_runner.py"
    ).read_text(encoding="utf-8")
    us_runner = (
        root / "trader" / "us" / "runner" / "trade_session_runner.py"
    ).read_text(encoding="utf-8")
    assert "install_legacy_pb1_runtime_guards()" in kr_runner
    assert "install_legacy_pb1_runtime_guards()" not in us_runner
    assert not list((root / "trader" / "us").rglob("runtime_integrity_*.py"))
