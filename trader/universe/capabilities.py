from __future__ import annotations

ALLOWED_PROVIDER_CHAIN = {
    "practice": ["fdr_kospi100_kosdaq100", "fdr_marketcap_top", "seed_static"],
    "real": ["fdr_kospi100_kosdaq100", "fdr_marketcap_top", "kis_marketcap_top", "seed_static"],
}


def providers_for_env(env: str) -> list[str]:
    normalized = (env or "practice").lower()
    return list(ALLOWED_PROVIDER_CHAIN.get(normalized, ALLOWED_PROVIDER_CHAIN["practice"]))
