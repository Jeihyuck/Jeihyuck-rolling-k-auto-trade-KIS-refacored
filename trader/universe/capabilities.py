from __future__ import annotations

ALLOWED_PROVIDER_CHAIN = {
    "practice": ["krx_marketcap_top", "lkg", "sqlite_cache", "seed_static"],
    "real": ["kis_marketcap_top", "krx_marketcap_top", "lkg", "sqlite_cache", "seed_static"],
}


def providers_for_env(env: str) -> list[str]:
    normalized = (env or "practice").lower()
    return list(ALLOWED_PROVIDER_CHAIN.get(normalized, ALLOWED_PROVIDER_CHAIN["practice"]))
