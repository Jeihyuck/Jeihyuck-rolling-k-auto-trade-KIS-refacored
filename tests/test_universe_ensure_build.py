import importlib
import json
import sys
from hashlib import sha1
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))


def _reload_modules():
    import trader.runtime_store as runtime_store
    import trader.universe.build as universe_build
    import trader.universe.lkg_store as lkg_store

    importlib.reload(lkg_store)
    importlib.reload(runtime_store)
    importlib.reload(universe_build)
    return runtime_store, universe_build, lkg_store


def test_ensure_universe_builds_and_saves(tmp_path, monkeypatch):
    runtime_root = tmp_path / "runtime"
    monkeypatch.setenv("TRADER_RUNTIME_DIR", str(runtime_root))
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("CANO", "12345678")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    runtime_store, universe_build, lkg_store = _reload_modules()
    monkeypatch.setattr(runtime_store, "EMERGENCY_UNIVERSE_BUILD", True)
    monkeypatch.setattr(runtime_store, "FORCE_UNIVERSE_REBUILD", False)
    monkeypatch.setattr(lkg_store, "UNIVERSE_NAMESPACE_MODE", "ACCOUNT_ENV")

    members = [{"code": "000001"}]
    monkeypatch.setattr(universe_build, "build_universe", lambda **_kwargs: members)

    store = runtime_store.RuntimeStore(runtime_root)
    meta = store.ensure_universe(as_of="2025-01-02", env="practice", strategy="best_k_meta")

    today_path = runtime_root / "runtime" / "universe" / "2025-01-02.json"
    assert meta.get("ok") is True
    assert today_path.exists()
    payload = json.loads(today_path.read_text(encoding="utf-8"))
    assert payload["members"] == members

    lkg_path = lkg_store.lkg_path("practice", "best_k_meta")
    assert lkg_path.exists()
    lkg_payload = json.loads(lkg_path.read_text(encoding="utf-8"))
    assert lkg_payload["members"] == members


def test_ensure_universe_empty_members_keeps_missing(tmp_path, monkeypatch):
    runtime_root = tmp_path / "runtime"
    monkeypatch.setenv("TRADER_RUNTIME_DIR", str(runtime_root))
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("CANO", "12345678")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    runtime_store, universe_build, lkg_store = _reload_modules()
    monkeypatch.setattr(runtime_store, "EMERGENCY_UNIVERSE_BUILD", True)
    monkeypatch.setattr(runtime_store, "FORCE_UNIVERSE_REBUILD", False)
    monkeypatch.setattr(lkg_store, "UNIVERSE_NAMESPACE_MODE", "ACCOUNT_ENV")

    monkeypatch.setattr(universe_build, "build_universe", lambda **_kwargs: [])

    store = runtime_store.RuntimeStore(runtime_root)
    meta = store.ensure_universe(as_of="2025-01-03", env="practice", strategy="best_k_meta")

    today_path = runtime_root / "runtime" / "universe" / "2025-01-03.json"
    assert meta.get("ok") is False
    assert not today_path.exists()


def test_universe_namespace_is_stable(monkeypatch):
    monkeypatch.setenv("TRADER_RUNTIME_DIR", "/tmp/runtime")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("CANO", "12345678")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")

    _, _, lkg_store = _reload_modules()
    monkeypatch.setattr(lkg_store, "UNIVERSE_NAMESPACE_MODE", "ACCOUNT_ENV")

    path_first = lkg_store.lkg_path("practice", "best_k_meta")
    path_second = lkg_store.lkg_path("practice", "best_k_meta")

    raw = "practice:12345678:01"
    expected = sha1(raw.encode("utf-8")).hexdigest()[:10]
    assert expected in str(path_first)
    assert path_first == path_second
