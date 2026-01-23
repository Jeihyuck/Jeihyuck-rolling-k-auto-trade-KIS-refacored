import importlib
import json
import sys
import types
from datetime import date

def _install_sa_stub() -> None:
    if "sqlalchemy" in sys.modules:
        return
    sa = types.ModuleType("sqlalchemy")

    class _DummyResult:
        def scalar(self):
            return None

        def scalars(self):
            return self

        def mappings(self):
            return self

        def all(self):
            return []

        def first(self):
            return None

        def __iter__(self):
            return iter([])

    class _DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args, **kwargs):
            return _DummyResult()

    class _DummyEngine:
        def __init__(self, url):
            self.url = url

        def begin(self):
            return _DummyConn()

    class _DummyStatement:
        def values(self, *args, **kwargs):
            return self

        def where(self, *args, **kwargs):
            return self

        def returning(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def limit(self, *args, **kwargs):
            return self

    sa.Engine = _DummyEngine
    sa.create_engine = lambda url, **kwargs: _DummyEngine(url)
    sa.text = lambda sql: sql
    sa.MetaData = lambda *args, **kwargs: object()
    sa.Column = lambda *args, **kwargs: object()
    sa.Table = lambda *args, **kwargs: object()
    sa.Connection = object
    scalar_type = lambda *args, **kwargs: object()
    sa.String = sa.Integer = sa.Float = sa.JSON = sa.Boolean = sa.DateTime = sa.Text = scalar_type
    sa.ForeignKey = lambda *args, **kwargs: None
    sa.func = types.SimpleNamespace(now=lambda: None)
    sa.insert = lambda *args, **kwargs: _DummyStatement()
    sa.update = lambda *args, **kwargs: _DummyStatement()
    sa.select = lambda *args, **kwargs: _DummyStatement()
    sa.and_ = lambda *args, **kwargs: None
    sa.UniqueConstraint = lambda *args, **kwargs: None

    sa_engine = types.ModuleType("sqlalchemy.engine")
    sa_engine.url = types.SimpleNamespace(make_url=lambda url: types.SimpleNamespace(database=url))
    sa_engine.Engine = _DummyEngine
    sys.modules["sqlalchemy.engine"] = sa_engine
    sys.modules["sqlalchemy.engine.url"] = sa_engine.url
    sys.modules["sqlalchemy"] = sa


_install_sa_stub()

from trader.db.engine import make_engine
from trader.db.repos import UniverseRepo


def test_lkg_used_when_krx_jsondecode(monkeypatch, tmp_path):
    lkg_root = tmp_path / "lkg"
    monkeypatch.setenv("PBCORE_DB_URL", "postgresql+psycopg://user:pass@localhost:5432/pbcore")
    monkeypatch.setenv("UNIVERSE_LKG_ROOT", str(lkg_root))
    monkeypatch.setenv("UNIVERSE_ENABLE_LKG", "1")
    monkeypatch.setenv("UNIVERSE_VALIDATE_OHLCV", "0")
    monkeypatch.setenv("UNIVERSE_VALIDATE_KIS", "0")

    from trader.universe import lkg_store

    importlib.reload(lkg_store)
    from trader.universe import build as build_module

    importlib.reload(build_module)

    stored: dict = {}

    class FakeUniverseRepo:
        def __init__(self, engine):
            self.engine = engine

        def store_universe_snapshot(self, *, env, strategy, as_of_date, provider, members, reason=None):
            stored[(env, strategy)] = {
                "universe": {
                    "run_id": "fake-id",
                    "env": env,
                    "strategy": strategy,
                    "as_of": as_of_date,
                    "provider": provider,
                    "reason": reason,
                },
                "members": list(members),
            }
            return "fake-id"

        def cleanup_old_runs(self, *, retain_days=30):
            return None

    build_module.UniverseRepo = FakeUniverseRepo
    build_module.make_engine = lambda *args, **kwargs: None
    build_module.run_migrations = lambda *args, **kwargs: None

    lkg_payload = {
        "payload": {
            "selected": ["000001", "000002"],
            "selected_by_market": {
                "KOSPI": [{"code": "000001", "rank": 1, "name": "foo"}],
                "KOSDAQ": [{"code": "000002", "rank": 1, "name": "bar"}],
            },
        },
        "members": [
            {"code": "000001", "market": "KOSPI", "rank": 1, "meta_json": {}},
            {"code": "000002", "market": "KOSDAQ", "rank": 1, "meta_json": {}},
        ],
        "params": {"as_of": "2024-01-02"},
    }
    lkg_store.save_lkg("practice", "best_k_meta", lkg_payload)

    def fail_fetch(*_, **__):
        raise json.JSONDecodeError("bad", "{}", 0)

    monkeypatch.setattr(build_module, "fetch_with_rollback", fail_fetch)

    build_module.build_universe(as_of_date=date(2024, 1, 3).isoformat(), env="practice", strategy="best_k_meta")

    latest = stored.get(("practice", "best_k_meta"))
    assert latest is not None
    universe_row = latest["universe"]
    members = latest["members"]
    assert universe_row.get("provider") == "fallback:lkg"
    assert len(members) == 2
