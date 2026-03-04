from trader.universe.capabilities import providers_for_env
from trader.universe import build


def test_universe_provider_chain_includes_emergency_seed():
    chain = providers_for_env("practice")
    assert chain == ["kis_marketcap_top", "seed_static", "emergency_seed"]


def test_universe_default_provider_is_kis_only():
    assert build.DEFAULT_PROVIDER == "kis_marketcap_top"
    assert build.TARGETS["KOSPI"] + build.TARGETS["KOSDAQ"] == 300
